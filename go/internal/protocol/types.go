package protocol

import (
	"encoding/json"
	"time"
)

const SchemaVersion = 1

type Mode string

const (
	ModeBaseline Mode = "baseline"
	ModeChaos    Mode = "chaos"
)

type ExecutionStatus string

const (
	StatusCompleted           ExecutionStatus = "completed"
	StatusAgentError          ExecutionStatus = "agent_error"
	StatusTimedOut            ExecutionStatus = "timed_out"
	StatusCanceled            ExecutionStatus = "canceled"
	StatusInterrupted         ExecutionStatus = "interrupted"
	StatusInfrastructureError ExecutionStatus = "infrastructure_error"
)

type Verdict string

const (
	VerdictPassed       Verdict = "passed"
	VerdictFailed       Verdict = "failed"
	VerdictNeedsReview  Verdict = "needs_review"
	VerdictInconclusive Verdict = "inconclusive"
)

type FaultOutcome string

const (
	FaultNone        FaultOutcome = "none"
	FaultInjected    FaultOutcome = "injected"
	FaultNotInjected FaultOutcome = "not_injected"
	FaultUnknown     FaultOutcome = "unknown"
)

type Agent struct {
	Adapter     string `json:"adapter"`
	Entrypoint  string `json:"entrypoint"`
	ProjectRoot string `json:"project_root"`
	Python      string `json:"python,omitempty"`
}

type Task struct {
	ID    string          `json:"id"`
	Input json.RawMessage `json:"input"`
}

type Target struct {
	Tool      string `json:"tool,omitempty" yaml:"tool"`
	Retriever string `json:"retriever,omitempty" yaml:"retriever"`
	Node      string `json:"node,omitempty" yaml:"node"`
	MemoryKey string `json:"memory_key,omitempty" yaml:"memory_key"`
	Route     string `json:"route,omitempty" yaml:"route"`
}

type Trigger struct {
	Call int `json:"call"`
}

type FaultParams struct {
	Fixture        string          `json:"fixture,omitempty"`
	FixtureSHA256  string          `json:"fixture_sha256,omitempty"`
	TimestampField string          `json:"timestamp_field,omitempty"`
	DurationMS     int             `json:"duration_ms,omitempty"`
	StatusCode     int             `json:"status_code,omitempty"`
	Message        string          `json:"message,omitempty"`
	Fields         []string        `json:"fields,omitempty"`
	DocumentID     string          `json:"document_id,omitempty"`
	Key            string          `json:"key,omitempty"`
	Value          json.RawMessage `json:"value,omitempty"`
}

type Fault struct {
	ID      string      `json:"id"`
	Kind    string      `json:"kind"`
	Target  Target      `json:"target"`
	Trigger Trigger     `json:"trigger"`
	Params  FaultParams `json:"params"`
}

type EventSelector struct {
	Type    string `json:"type" yaml:"type"`
	Target  string `json:"target,omitempty" yaml:"target"`
	FaultID string `json:"fault_id,omitempty" yaml:"fault_id"`
}

type Check struct {
	ID      string          `json:"id"`
	Mode    string          `json:"mode"`
	Purpose string          `json:"purpose,omitempty"`
	Type    string          `json:"type"`
	Path    string          `json:"path,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`
	Event   *EventSelector  `json:"event,omitempty"`
	First   *EventSelector  `json:"first,omitempty"`
	Then    *EventSelector  `json:"then,omitempty"`
	Min     *int            `json:"min,omitempty"`
	Max     *int            `json:"max,omitempty"`
	MaxMS   int             `json:"max_ms,omitempty"`
}

type Limits struct {
	TimeoutSeconds int `json:"timeout_seconds"`
}

type RunSpec struct {
	SchemaVersion int     `json:"schema_version"`
	ExperimentID  string  `json:"experiment_id"`
	RunID         string  `json:"run_id"`
	PairID        string  `json:"pair_id"`
	PresetID      string  `json:"preset_id"`
	Repetition    int     `json:"repetition"`
	Mode          Mode    `json:"mode"`
	Seed          int64   `json:"seed"`
	Agent         Agent   `json:"agent"`
	Task          Task    `json:"task"`
	Fault         *Fault  `json:"fault,omitempty"`
	Checks        []Check `json:"checks"`
	Limits        Limits  `json:"limits"`
}

type RunEvent struct {
	SchemaVersion    int       `json:"schema_version"`
	ExperimentID     string    `json:"experiment_id"`
	RunID            string    `json:"run_id"`
	AttemptID        string    `json:"attempt_id"`
	EventID          string    `json:"event_id"`
	Producer         string    `json:"producer"`
	ProducerSequence uint64    `json:"producer_sequence"`
	Timestamp        time.Time `json:"timestamp"`
	Type             string    `json:"type"`
	FaultID          string    `json:"fault_id,omitempty"`
	OperationID      string    `json:"operation_id,omitempty"`
	Target           string    `json:"target,omitempty"`
}

type CheckResult struct {
	ID      string `json:"id"`
	Purpose string `json:"purpose,omitempty"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}

type RunResult struct {
	SchemaVersion   int             `json:"schema_version"`
	ExperimentID    string          `json:"experiment_id"`
	RunID           string          `json:"run_id"`
	AttemptID       string          `json:"attempt_id"`
	ExecutionStatus ExecutionStatus `json:"execution_status"`
	Verdict         Verdict         `json:"verdict"`
	FaultOutcome    FaultOutcome    `json:"fault_outcome"`
	TaskSuccess     *bool           `json:"task_success,omitempty"`
	FaultDetected   *bool           `json:"fault_detected,omitempty"`
	Recovered       *bool           `json:"recovered,omitempty"`
	DurationMS      int64           `json:"duration_ms"`
	Output          json.RawMessage `json:"output,omitempty"`
	Checks          []CheckResult   `json:"checks,omitempty"`
	OpenOperations  []string        `json:"open_operations,omitempty"`
	Error           string          `json:"error,omitempty"`
}
