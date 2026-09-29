package state

import (
	"encoding/json"
	"testing"
	"time"

	"enragent/internal/protocol"
)

func TestAttemptTransitionsAndOperations(t *testing.T) {
	spec := protocol.RunSpec{SchemaVersion: 1, ExperimentID: "exp", RunID: "run_test", Mode: protocol.ModeChaos, Fault: &protocol.Fault{ID: "fault"}}
	attempt := New(spec, "attempt")
	if err := attempt.Transition(Running); err == nil {
		t.Fatal("planned -> running should fail")
	}
	if err := attempt.Transition(Queued); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Transition(Running); err != nil {
		t.Fatal(err)
	}
	events := []protocol.RunEvent{
		{SchemaVersion: 1, ExperimentID: "exp", RunID: "run_test", AttemptID: "attempt", EventID: "e1", Producer: "test", ProducerSequence: 1, Timestamp: time.Now(), Type: "OPERATION_STARTED", OperationID: "op"},
		{SchemaVersion: 1, ExperimentID: "exp", RunID: "run_test", AttemptID: "attempt", EventID: "e2", Producer: "test", ProducerSequence: 2, Timestamp: time.Now(), Type: "FAULT_INJECTED", FaultID: "fault"},
	}
	for _, event := range events {
		if err := attempt.Apply(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := attempt.Apply(events[1]); err != nil {
		t.Fatalf("duplicate event should be ignored: %v", err)
	}
	result := attempt.Finish(protocol.StatusCompleted, json.RawMessage(`{"ok":true}`), 20, nil)
	if result.ExecutionStatus != protocol.StatusInfrastructureError || len(result.OpenOperations) != 1 {
		t.Fatalf("open operation was not preserved: %+v", result)
	}
	if err := attempt.Transition(Terminal); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Transition(Running); err == nil {
		t.Fatal("terminal transition should fail")
	}
}

func TestChecksNeedTaskOracle(t *testing.T) {
	minimum := 1
	events := []protocol.RunEvent{{Type: "FAULT_INJECTED"}, {Type: "TOOL_CALLED", Target: "fresh"}}
	checks := []protocol.Check{{ID: "injection", Type: "event.count", Event: &protocol.EventSelector{Type: "FAULT_INJECTED"}, Min: &minimum}}
	assessment := Evaluate(checks, events, json.RawMessage(`{"ok":true}`), 5)
	if assessment.Verdict != protocol.VerdictNeedsReview {
		t.Fatalf("got %s, want needs_review", assessment.Verdict)
	}
	checks = append(checks, protocol.Check{ID: "success", Purpose: "success", Type: "output.field_equals", Path: "/ok", Value: json.RawMessage(`true`)})
	checks = append(checks, protocol.Check{ID: "recovery", Purpose: "detection", Type: "event.after", First: &protocol.EventSelector{Type: "FAULT_INJECTED"}, Then: &protocol.EventSelector{Type: "TOOL_CALLED", Target: "fresh"}})
	assessment = Evaluate(checks, events, json.RawMessage(`{"ok":true}`), 5)
	if assessment.Verdict != protocol.VerdictPassed || assessment.TaskSuccess == nil || !*assessment.TaskSuccess {
		t.Fatalf("got %+v, want task pass", assessment)
	}
	checks[2].Then.Target = "missing"
	assessment = Evaluate(checks, events, json.RawMessage(`{"ok":true}`), 5)
	if assessment.Verdict != protocol.VerdictPassed || assessment.TaskSuccess == nil || !*assessment.TaskSuccess {
		t.Fatalf("detection failure changed task verdict: %+v", assessment)
	}
	if assessment.FaultDetected == nil || *assessment.FaultDetected {
		t.Fatalf("detection should be false: %+v", assessment)
	}
}
