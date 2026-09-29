package state

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"enragent/internal/protocol"
)

type Phase string

const (
	Planned  Phase = "planned"
	Queued   Phase = "queued"
	Running  Phase = "running"
	Terminal Phase = "terminal"
)

type Attempt struct {
	Spec       protocol.RunSpec
	AttemptID  string
	Phase      Phase
	Events     []protocol.RunEvent
	operations map[string]bool
	seen       map[string]bool
	sequence   map[string]uint64
	injections int
}

func New(spec protocol.RunSpec, attemptID string) *Attempt {
	return &Attempt{Spec: spec, AttemptID: attemptID, Phase: Planned,
		operations: make(map[string]bool), seen: make(map[string]bool), sequence: make(map[string]uint64)}
}

func (a *Attempt) Transition(next Phase) error {
	valid := (a.Phase == Planned && (next == Queued || next == Terminal)) ||
		(a.Phase == Queued && (next == Running || next == Terminal)) ||
		(a.Phase == Running && next == Terminal)
	if !valid {
		return fmt.Errorf("invalid attempt transition %s -> %s", a.Phase, next)
	}
	a.Phase = next
	return nil
}

func (a *Attempt) Apply(event protocol.RunEvent) error {
	if a.Phase != Running {
		return fmt.Errorf("event %q arrived while attempt is %s", event.EventID, a.Phase)
	}
	if event.SchemaVersion != protocol.SchemaVersion || event.ExperimentID != a.Spec.ExperimentID || event.RunID != a.Spec.RunID || event.AttemptID != a.AttemptID {
		return fmt.Errorf("event %q has mismatched attempt identity", event.EventID)
	}
	if event.EventID == "" || event.Producer == "" || event.ProducerSequence == 0 || event.Type == "" {
		return fmt.Errorf("event identity, producer sequence, and type are required")
	}
	if a.seen[event.EventID] {
		return nil
	}
	if event.ProducerSequence != a.sequence[event.Producer]+1 {
		return fmt.Errorf("event %q has producer sequence gap", event.EventID)
	}
	switch event.Type {
	case "OPERATION_STARTED":
		if event.OperationID == "" || a.operations[event.OperationID] {
			return fmt.Errorf("invalid operation start %q", event.OperationID)
		}
		a.operations[event.OperationID] = true
	case "OPERATION_FINISHED":
		if event.OperationID == "" || !a.operations[event.OperationID] {
			return fmt.Errorf("operation %q was not active", event.OperationID)
		}
		delete(a.operations, event.OperationID)
	case "FAULT_INJECTED":
		if a.Spec.Mode != protocol.ModeChaos || a.Spec.Fault == nil || event.FaultID != a.Spec.Fault.ID {
			return fmt.Errorf("fault injection does not match run spec")
		}
		a.injections++
	}
	a.seen[event.EventID] = true
	a.sequence[event.Producer] = event.ProducerSequence
	a.Events = append(a.Events, event)
	return nil
}

func (a *Attempt) Finish(status protocol.ExecutionStatus, output json.RawMessage, durationMS int64, cause error) protocol.RunResult {
	result := protocol.RunResult{
		SchemaVersion: protocol.SchemaVersion, ExperimentID: a.Spec.ExperimentID,
		RunID: a.Spec.RunID, AttemptID: a.AttemptID, ExecutionStatus: status,
		DurationMS: durationMS, Output: output,
	}
	if cause != nil {
		result.Error = cause.Error()
	}
	for op := range a.operations {
		result.OpenOperations = append(result.OpenOperations, op)
	}
	slices.Sort(result.OpenOperations)
	if len(result.OpenOperations) > 0 && status == protocol.StatusCompleted {
		result.ExecutionStatus = protocol.StatusInfrastructureError
		result.Error = "attempt ended with active operations"
		status = protocol.StatusInfrastructureError
	}
	if a.Spec.Mode == protocol.ModeBaseline {
		result.FaultOutcome = protocol.FaultNone
	} else if status == protocol.StatusInfrastructureError || status == protocol.StatusInterrupted {
		result.FaultOutcome = protocol.FaultUnknown
	} else if a.injections > 0 {
		result.FaultOutcome = protocol.FaultInjected
	} else {
		result.FaultOutcome = protocol.FaultNotInjected
	}
	if status == protocol.StatusInfrastructureError || status == protocol.StatusInterrupted || status == protocol.StatusCanceled || result.FaultOutcome == protocol.FaultNotInjected {
		result.Verdict = protocol.VerdictInconclusive
		return result
	}
	if status == protocol.StatusAgentError || status == protocol.StatusTimedOut {
		failed := false
		result.TaskSuccess = &failed
		assessment := Evaluate(a.Spec.Checks, a.Events, output, durationMS)
		result.Checks = assessment.Checks
		result.FaultDetected = assessment.FaultDetected
		result.Recovered = assessment.Recovered
		result.Verdict = protocol.VerdictFailed
		return result
	}
	if status != protocol.StatusCompleted {
		result.Verdict = protocol.VerdictInconclusive
		return result
	}
	assessment := Evaluate(a.Spec.Checks, a.Events, output, durationMS)
	result.Checks = assessment.Checks
	result.Verdict = assessment.Verdict
	result.TaskSuccess = assessment.TaskSuccess
	result.FaultDetected = assessment.FaultDetected
	result.Recovered = assessment.Recovered
	return result
}

type Assessment struct {
	Checks        []protocol.CheckResult
	Verdict       protocol.Verdict
	TaskSuccess   *bool
	FaultDetected *bool
	Recovered     *bool
}

func Evaluate(checks []protocol.Check, events []protocol.RunEvent, output json.RawMessage, durationMS int64) Assessment {
	if len(checks) == 0 {
		return Assessment{Verdict: protocol.VerdictNeedsReview}
	}
	assessment := Assessment{Checks: make([]protocol.CheckResult, 0, len(checks)), Verdict: protocol.VerdictNeedsReview}
	groups := map[string][]bool{}
	for _, check := range checks {
		passed, reason := evaluateOne(check, events, output, durationMS)
		status := "passed"
		if !passed {
			status = "failed"
		}
		assessment.Checks = append(assessment.Checks, protocol.CheckResult{ID: check.ID, Purpose: check.Purpose, Status: status, Reason: reason})
		groups[check.Purpose] = append(groups[check.Purpose], passed)
	}
	assessment.TaskSuccess = allChecks(groups["success"])
	assessment.FaultDetected = allChecks(groups["detection"])
	assessment.Recovered = allChecks(groups["recovery"])
	if assessment.TaskSuccess != nil {
		if *assessment.TaskSuccess {
			assessment.Verdict = protocol.VerdictPassed
		} else {
			assessment.Verdict = protocol.VerdictFailed
		}
	}
	return assessment
}

func allChecks(values []bool) *bool {
	if len(values) == 0 {
		return nil
	}
	passed := true
	for _, value := range values {
		if !value {
			passed = false
		}
	}
	return &passed
}

func evaluateOne(check protocol.Check, events []protocol.RunEvent, output json.RawMessage, durationMS int64) (bool, string) {
	switch check.Type {
	case "output.field_equals":
		var value any
		if err := json.Unmarshal(output, &value); err != nil {
			return false, "output is not valid JSON"
		}
		actual, ok := jsonPointer(value, check.Path)
		if !ok {
			return false, "output path is missing"
		}
		var expected any
		if err := json.Unmarshal(check.Value, &expected); err != nil {
			return false, "expected value is not valid JSON"
		}
		return reflect.DeepEqual(actual, expected), ""
	case "event.count":
		count := 0
		for _, event := range events {
			if check.Event != nil && matches(*check.Event, event) {
				count++
			}
		}
		if check.Min != nil && count < *check.Min {
			return false, fmt.Sprintf("count %d is below %d", count, *check.Min)
		}
		if check.Max != nil && count > *check.Max {
			return false, fmt.Sprintf("count %d exceeds %d", count, *check.Max)
		}
		return true, ""
	case "event.after":
		firstSeen := false
		for _, event := range events {
			if firstSeen && check.Then != nil && matches(*check.Then, event) {
				return true, ""
			}
			if check.First != nil && matches(*check.First, event) {
				firstSeen = true
			}
		}
		return false, "required event order was not observed"
	case "duration.max":
		if durationMS > int64(check.MaxMS) {
			return false, fmt.Sprintf("duration %dms exceeds %dms", durationMS, check.MaxMS)
		}
		return true, ""
	default:
		return false, "unsupported check"
	}
}

func matches(selector protocol.EventSelector, event protocol.RunEvent) bool {
	return selector.Type == event.Type && (selector.Target == "" || selector.Target == event.Target) &&
		(selector.FaultID == "" || selector.FaultID == event.FaultID)
}

func jsonPointer(value any, path string) (any, bool) {
	if !strings.HasPrefix(path, "/") {
		return nil, false
	}
	for _, token := range strings.Split(path[1:], "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[token]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(current) {
				return nil, false
			}
			value = current[index]
		default:
			return nil, false
		}
	}
	return value, true
}
