package journal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"enragent/internal/protocol"
	"enragent/internal/state"
)

func TestReplayAndEventDeduplication(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := protocol.RunSpec{SchemaVersion: 1, RunID: "run_test", ExperimentID: "exp"}
	if err := store.AppendSpec(spec); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTransition(spec.RunID, "attempt", state.Planned, state.Queued); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTransition(spec.RunID, "attempt", state.Queued, state.Running); err != nil {
		t.Fatal(err)
	}
	event := protocol.RunEvent{SchemaVersion: 1, RunID: spec.RunID, ExperimentID: "exp", AttemptID: "attempt", EventID: "event_1", Producer: "test", ProducerSequence: 1, Timestamp: time.Now(), Type: "RUN_STARTED"}
	if err := store.AppendEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEvent(event); err != nil {
		t.Fatal(err)
	}
	result := protocol.RunResult{SchemaVersion: 1, RunID: spec.RunID, AttemptID: "attempt", ExecutionStatus: protocol.StatusCompleted}
	if err := store.AppendResult(result); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTransition(spec.RunID, "attempt", state.Running, state.Terminal); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovery, err := Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Incomplete || recovery.Phases[spec.RunID] != state.Terminal || len(recovery.Results) != 1 {
		t.Fatalf("bad replay: %+v", recovery)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.AppendEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	records, incomplete, err := readRecords(filepath.Join(dir, "events", spec.RunID+".jsonl"))
	if err != nil || incomplete || len(records) != 2 {
		t.Fatalf("dedupe failed: count %d, incomplete %v, error %v", len(records), incomplete, err)
	}
}

func TestTruncatedTailIsIncomplete(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendSpec(protocol.RunSpec{RunID: "run_test"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, "runs.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{\"record\":"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	recovery, err := Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.Incomplete {
		t.Fatal("truncated tail was not detected")
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("incomplete journal was reopened for writes")
	}
}

func TestSequenceGapIsIncomplete(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := protocol.RunSpec{SchemaVersion: 1, RunID: "run_gap", ExperimentID: "exp"}
	if err := store.AppendSpec(spec); err != nil {
		t.Fatal(err)
	}
	event := protocol.RunEvent{SchemaVersion: 1, ExperimentID: "exp", RunID: spec.RunID, AttemptID: "attempt", EventID: "event_2", Producer: "test", ProducerSequence: 2, Type: "TOOL_CALLED"}
	if err := store.AppendEvent(event); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovery, err := Replay(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.Incomplete {
		t.Fatal("sequence gap was not detected")
	}
}
