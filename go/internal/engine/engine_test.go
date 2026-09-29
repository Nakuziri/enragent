package engine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"enragent/internal/config"
	"enragent/internal/journal"
	"enragent/internal/planner"
	"enragent/internal/protocol"
	"enragent/internal/state"
)

type fixtureExecutor struct {
	mu        sync.Mutex
	active    int
	maximum   int
	supported bool
}

type waitingExecutor struct{}

func (waitingExecutor) Supports(protocol.RunSpec) bool { return true }
func (waitingExecutor) Execute(ctx context.Context, _ protocol.RunSpec, _ func(EventInput) error) (Output, error) {
	<-ctx.Done()
	return Output{}, ctx.Err()
}

type lateExecutor struct{ emit func(EventInput) error }

func (l *lateExecutor) Supports(protocol.RunSpec) bool { return true }
func (l *lateExecutor) Execute(_ context.Context, _ protocol.RunSpec, emit func(EventInput) error) (Output, error) {
	l.emit = emit
	return Output{Status: protocol.StatusCompleted, Value: json.RawMessage(`{"alert_active":false}`)}, nil
}

func (f *fixtureExecutor) Supports(protocol.RunSpec) bool { return f.supported }

func (f *fixtureExecutor) Execute(ctx context.Context, spec protocol.RunSpec, emit func(EventInput) error) (Output, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.maximum {
		f.maximum = f.active
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	if err := emit(EventInput{Type: "OPERATION_STARTED", OperationID: "tool_1", Target: "metrics.query"}); err != nil {
		return Output{}, err
	}
	select {
	case <-ctx.Done():
		return Output{}, ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	if spec.Fault != nil {
		if err := emit(EventInput{Type: "FAULT_INJECTED", FaultID: spec.Fault.ID, Target: "metrics.query"}); err != nil {
			return Output{}, err
		}
		if err := emit(EventInput{Type: "TOOL_CALLED", Target: "metrics.verify_freshness"}); err != nil {
			return Output{}, err
		}
	}
	if err := emit(EventInput{Type: "OPERATION_FINISHED", OperationID: "tool_1", Target: "metrics.query"}); err != nil {
		return Output{}, err
	}
	return Output{Status: protocol.StatusCompleted, Value: json.RawMessage(`{"alert_active":false}`)}, nil
}

func testPlan(t *testing.T, quantity int) planner.Plan {
	t.Helper()
	loaded, err := config.Load("../../../examples/enragent.yaml")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Tests = loaded.Tests[:1]
	loaded.Tests[0].Quantity = quantity
	plan, err := planner.BuildWithIdentity(loaded, "exp_engine_test", 42)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testSettings() Settings {
	return Settings{Workers: 2, RunQueueCapacity: 1, EventQueueCapacity: 1, ShutdownTimeout: time.Second}
}

func TestRunBoundedAndJournaled(t *testing.T) {
	plan := testPlan(t, 2)
	dir := filepath.Join(t.TempDir(), "runs")
	executor := &fixtureExecutor{supported: true}
	results, err := Run(context.Background(), plan, executor, dir, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
	if executor.maximum > 2 || executor.maximum < 1 {
		t.Fatalf("maximum active workers = %d", executor.maximum)
	}
	for _, result := range results {
		if result.Verdict != protocol.VerdictPassed || result.ExecutionStatus != protocol.StatusCompleted {
			t.Fatalf("unexpected result: %+v", result)
		}
	}
	recovery, err := journal.Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Incomplete || len(recovery.Results) != 4 {
		t.Fatalf("bad replay: %+v", recovery)
	}
	again, err := Run(context.Background(), plan, executor, dir, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 4 {
		t.Fatalf("resume returned %d results", len(again))
	}
}

func TestResumeQueuedAndInterruptRunning(t *testing.T) {
	plan := testPlan(t, 1)
	dir := filepath.Join(t.TempDir(), "runs")
	store, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range plan.Runs {
		if err := store.AppendSpec(spec); err != nil {
			t.Fatal(err)
		}
	}
	queued, running := plan.Runs[0], plan.Runs[1]
	for _, spec := range []protocol.RunSpec{queued, running} {
		if err := store.AppendTransition(spec.RunID, spec.RunID+"_a1", state.Planned, state.Queued); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AppendTransition(running.RunID, running.RunID+"_a1", state.Queued, state.Running); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	results, err := Run(context.Background(), plan, &fixtureExecutor{supported: true}, dir, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ExecutionStatus != protocol.StatusCompleted || results[1].ExecutionStatus != protocol.StatusInterrupted {
		t.Fatalf("unexpected recovery outcomes: %+v", results)
	}
}

func TestCapabilityGateBeforeAdmission(t *testing.T) {
	plan := testPlan(t, 1)
	dir := filepath.Join(t.TempDir(), "runs")
	if _, err := Run(context.Background(), plan, &fixtureExecutor{}, dir, testSettings()); err == nil {
		t.Fatal("unsupported executor was admitted")
	}
	if _, err := journal.Replay(dir); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledRunGetsTerminalResults(t *testing.T) {
	plan := testPlan(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := Run(ctx, plan, &fixtureExecutor{supported: true}, filepath.Join(t.TempDir(), "runs"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results", len(results))
	}
	for _, result := range results {
		if result.ExecutionStatus != protocol.StatusCanceled {
			t.Fatalf("status = %s", result.ExecutionStatus)
		}
	}
}

func TestTimeoutGetsTerminalResult(t *testing.T) {
	plan := testPlan(t, 1)
	plan.Runs = plan.Runs[:1]
	plan.Runs[0].Limits.TimeoutSeconds = 1
	results, err := Run(context.Background(), plan, waitingExecutor{}, filepath.Join(t.TempDir(), "runs"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ExecutionStatus != protocol.StatusTimedOut || results[0].Verdict != protocol.VerdictFailed {
		t.Fatalf("unexpected timeout result: %+v", results)
	}
}

func TestResumeResultBeforeTerminalTransition(t *testing.T) {
	plan := testPlan(t, 1)
	plan.Runs = plan.Runs[:1]
	dir := filepath.Join(t.TempDir(), "runs")
	store, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Runs[0]
	if err := store.AppendSpec(spec); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTransition(spec.RunID, spec.RunID+"_a1", state.Planned, state.Queued); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTransition(spec.RunID, spec.RunID+"_a1", state.Queued, state.Running); err != nil {
		t.Fatal(err)
	}
	result := protocol.RunResult{SchemaVersion: 1, RunID: spec.RunID, AttemptID: spec.RunID + "_a1", ExecutionStatus: protocol.StatusCompleted, Verdict: protocol.VerdictPassed, FaultOutcome: protocol.FaultNone}
	if err := store.AppendResult(result); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	results, err := Run(context.Background(), plan, &fixtureExecutor{supported: true}, dir, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Verdict != protocol.VerdictPassed {
		t.Fatalf("stored result was lost: %+v", results)
	}
	recovery, err := journal.Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Incomplete || recovery.Phases[spec.RunID] != state.Terminal {
		t.Fatalf("result was not finalized: %+v", recovery)
	}
}

func TestLateEventIsRejected(t *testing.T) {
	plan := testPlan(t, 1)
	plan.Runs = plan.Runs[:1]
	executor := &lateExecutor{}
	if _, err := Run(context.Background(), plan, executor, filepath.Join(t.TempDir(), "runs"), testSettings()); err != nil {
		t.Fatal(err)
	}
	if err := executor.emit(EventInput{Type: "TOOL_CALLED"}); err == nil {
		t.Fatal("late event was accepted")
	}
}
