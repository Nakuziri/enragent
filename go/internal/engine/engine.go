package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"enragent/internal/journal"
	"enragent/internal/planner"
	"enragent/internal/protocol"
	"enragent/internal/state"
)

type Settings struct {
	Workers            int
	RunQueueCapacity   int
	EventQueueCapacity int
	ShutdownTimeout    time.Duration
}

func SettingsFromEnv() (Settings, error) {
	settings := Settings{}
	var err error
	if settings.Workers, err = positiveEnv("ENRAGENT_WORKERS", 4); err != nil {
		return Settings{}, err
	}
	if settings.RunQueueCapacity, err = positiveEnv("ENRAGENT_RUN_QUEUE_CAPACITY", 32); err != nil {
		return Settings{}, err
	}
	if settings.EventQueueCapacity, err = positiveEnv("ENRAGENT_EVENT_QUEUE_CAPACITY", 256); err != nil {
		return Settings{}, err
	}
	value := os.Getenv("ENRAGENT_SHUTDOWN_TIMEOUT")
	if value == "" {
		settings.ShutdownTimeout = 30 * time.Second
	} else {
		settings.ShutdownTimeout, err = time.ParseDuration(value)
		if err != nil || settings.ShutdownTimeout <= 0 {
			return Settings{}, fmt.Errorf("ENRAGENT_SHUTDOWN_TIMEOUT must be a positive duration")
		}
	}
	return settings, nil
}

func positiveEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil || number <= 0 || number > 1024 {
		return 0, fmt.Errorf("%s must be an integer from 1 to 1024", name)
	}
	return number, nil
}

type EventInput struct {
	Type        string
	FaultID     string
	OperationID string
	Target      string
}

type Output struct {
	Status protocol.ExecutionStatus
	Value  json.RawMessage
}

type Executor interface {
	Supports(protocol.RunSpec) bool
	Execute(context.Context, protocol.RunSpec, func(EventInput) error) (Output, error)
}

type writeRequest struct {
	event  *protocol.RunEvent
	result *protocol.RunResult
	ack    chan error
}

func Run(parent context.Context, plan planner.Plan, executor Executor, dir string, settings Settings) ([]protocol.RunResult, error) {
	if settings.Workers < 1 || settings.RunQueueCapacity < 1 || settings.EventQueueCapacity < 1 || settings.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("workers, queue capacities, and shutdown timeout must be positive")
	}
	for _, spec := range plan.Runs {
		if !executor.Supports(spec) {
			return nil, fmt.Errorf("preset %q requires an unavailable execution hook", spec.PresetID)
		}
	}
	recovery, err := journal.Replay(dir)
	if err != nil {
		return nil, err
	}
	if recovery.Incomplete {
		return nil, fmt.Errorf("journal has incomplete evidence")
	}
	planned := make(map[string]protocol.RunSpec, len(plan.Runs))
	for _, spec := range plan.Runs {
		planned[spec.RunID] = spec
	}
	for runID, old := range recovery.Specs {
		current, ok := planned[runID]
		if !ok {
			return nil, fmt.Errorf("journal contains run %s outside this plan", runID)
		}
		before, _ := json.Marshal(old)
		after, _ := json.Marshal(current)
		if !bytes.Equal(before, after) {
			return nil, fmt.Errorf("run %s differs from the journaled plan", runID)
		}
	}
	store, err := journal.Open(dir)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	for _, spec := range plan.Runs {
		if _, exists := recovery.Specs[spec.RunID]; exists {
			continue
		}
		if err := store.AppendSpec(spec); err != nil {
			return nil, fmt.Errorf("admitting %s: %w", spec.RunID, err)
		}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	jobs := make(chan protocol.RunSpec, settings.RunQueueCapacity)
	writes := make(chan writeRequest, settings.EventQueueCapacity)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for request := range writes {
			var writeErr error
			if request.event != nil {
				writeErr = store.AppendEvent(*request.event)
			} else {
				writeErr = store.AppendResult(*request.result)
			}
			request.ack <- writeErr
		}
	}()
	write := func(event *protocol.RunEvent, result *protocol.RunResult) error {
		ack := make(chan error, 1)
		writes <- writeRequest{event: event, result: result, ack: ack}
		return <-ack
	}
	attempts := make(map[string]*state.Attempt, len(plan.Runs))
	for _, spec := range plan.Runs {
		attemptID := spec.RunID + "_a1"
		if recovered := recovery.AttemptIDs[spec.RunID]; recovered != "" {
			attemptID = recovered
		}
		attempt := state.New(spec, attemptID)
		if phase, exists := recovery.Phases[spec.RunID]; exists {
			attempt.Phase = phase
		}
		attempts[spec.RunID] = attempt
	}
	transition := func(attempt *state.Attempt, next state.Phase) error {
		previous := attempt.Phase
		if err := attempt.Transition(next); err != nil {
			return err
		}
		if err := store.AppendTransition(attempt.Spec.RunID, attempt.AttemptID, previous, next); err != nil {
			attempt.Phase = previous
			return err
		}
		return nil
	}
	results := make(map[string]protocol.RunResult, len(plan.Runs))
	for runID, result := range recovery.Results {
		results[runID] = result
	}
	var mu sync.Mutex
	var firstErr error
	recordError := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	finish := func(attempt *state.Attempt, status protocol.ExecutionStatus, output json.RawMessage, durationMS int64, cause error) {
		result := attempt.Finish(status, output, durationMS, cause)
		if err := write(nil, &result); err != nil {
			recordError(fmt.Errorf("persisting result %s: %w", attempt.Spec.RunID, err))
			return
		}
		if err := transition(attempt, state.Terminal); err != nil {
			recordError(fmt.Errorf("terminal transition %s: %w", attempt.Spec.RunID, err))
			return
		}
		mu.Lock()
		results[attempt.Spec.RunID] = result
		mu.Unlock()
	}
	var workers sync.WaitGroup
	for i := 0; i < settings.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for spec := range jobs {
				attempt := attempts[spec.RunID]
				if ctx.Err() != nil {
					finish(attempt, protocol.StatusCanceled, nil, 0, ctx.Err())
					continue
				}
				if err := transition(attempt, state.Running); err != nil {
					recordError(err)
					continue
				}
				runCtx, stop := context.WithTimeout(ctx, time.Duration(spec.Limits.TimeoutSeconds)*time.Second)
				started := time.Now()
				var eventMu sync.Mutex
				var sequence uint64
				var emitErr error
				var emitterClosed bool
				emit := func(input EventInput) error {
					eventMu.Lock()
					defer eventMu.Unlock()
					if emitterClosed {
						return fmt.Errorf("attempt event stream is closed")
					}
					if emitErr != nil {
						return emitErr
					}
					sequence++
					event := protocol.RunEvent{SchemaVersion: protocol.SchemaVersion, ExperimentID: spec.ExperimentID,
						RunID: spec.RunID, AttemptID: attempt.AttemptID, EventID: fmt.Sprintf("%s_e%d", attempt.AttemptID, sequence),
						Producer: "executor", ProducerSequence: sequence, Timestamp: time.Now().UTC(), Type: input.Type,
						FaultID: input.FaultID, OperationID: input.OperationID, Target: input.Target}
					if err := attempt.Apply(event); err != nil {
						emitErr = err
						return err
					}
					if err := write(&event, nil); err != nil {
						emitErr = err
						return err
					}
					return nil
				}
				output, execErr := executor.Execute(runCtx, spec, emit)
				eventMu.Lock()
				emitterClosed = true
				currentEmitErr := emitErr
				eventMu.Unlock()
				duration := time.Since(started).Milliseconds()
				status := output.Status
				switch {
				case currentEmitErr != nil:
					status = protocol.StatusInfrastructureError
					execErr = currentEmitErr
				case errors.Is(runCtx.Err(), context.DeadlineExceeded):
					status = protocol.StatusTimedOut
				case runCtx.Err() != nil:
					status = protocol.StatusCanceled
				case execErr != nil:
					status = protocol.StatusInfrastructureError
				case status == "":
					status = protocol.StatusCompleted
				}
				stop()
				finish(attempt, status, output.Value, duration, execErr)
			}
		}()
	}
	admissionStopped := false
	for _, spec := range plan.Runs {
		attempt := attempts[spec.RunID]
		if attempt.Phase == state.Terminal {
			continue
		}
		if attempt.Phase == state.Running {
			if _, hasResult := recovery.Results[spec.RunID]; hasResult {
				if err := transition(attempt, state.Terminal); err != nil {
					recordError(err)
				}
			} else {
				finish(attempt, protocol.StatusInterrupted, nil, 0, fmt.Errorf("attempt interrupted by control-plane restart"))
			}
			continue
		}
		if admissionStopped {
			finish(attempt, protocol.StatusCanceled, nil, 0, ctx.Err())
			continue
		}
		if attempt.Phase == state.Planned {
			if err := transition(attempt, state.Queued); err != nil {
				recordError(err)
				admissionStopped = true
				finish(attempt, protocol.StatusInfrastructureError, nil, 0, err)
				continue
			}
		}
		select {
		case jobs <- spec:
		case <-ctx.Done():
			admissionStopped = true
			finish(attempt, protocol.StatusCanceled, nil, 0, ctx.Err())
		}
	}
	close(jobs)
	workersDone := make(chan struct{})
	go func() { workers.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
	case <-ctx.Done():
		deadline := time.NewTimer(settings.ShutdownTimeout)
		select {
		case <-workersDone:
		case <-deadline.C:
			recordError(fmt.Errorf("workers exceeded shutdown deadline %s", settings.ShutdownTimeout))
			<-workersDone
		}
		deadline.Stop()
	}
	close(writes)
	<-writerDone
	mu.Lock()
	err = firstErr
	mu.Unlock()
	ordered := make([]protocol.RunResult, 0, len(results))
	for _, spec := range plan.Runs {
		if result, ok := results[spec.RunID]; ok {
			ordered = append(ordered, result)
		}
	}
	return ordered, err
}
