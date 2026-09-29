package state

import (
	"fmt"
	"sort"

	"enragent/internal/protocol"
)

type ModeSummary struct {
	Planned          int `json:"planned"`
	Terminal         int `json:"terminal"`
	TaskSuccessKnown int `json:"task_success_known"`
	TaskSuccess      int `json:"task_success"`
	Injected         int `json:"injected"`
	NotInjected      int `json:"not_injected"`
	DetectionKnown   int `json:"detection_known"`
	Detected         int `json:"detected"`
	RecoveryKnown    int `json:"recovery_known"`
	Recovered        int `json:"recovered"`
	SilentFailures   int `json:"silent_failures"`
}

type TestSummary struct {
	PresetID         string      `json:"preset_id"`
	Complete         bool        `json:"complete"`
	EvidenceComplete bool        `json:"evidence_complete"`
	Baseline         ModeSummary `json:"baseline"`
	Chaos            ModeSummary `json:"chaos"`
}

type ExperimentSummary struct {
	Complete         bool          `json:"complete"`
	EvidenceComplete bool          `json:"evidence_complete"`
	Planned          int           `json:"planned"`
	Terminal         int           `json:"terminal"`
	Tests            []TestSummary `json:"tests"`
}

func Summarize(specs []protocol.RunSpec, results []protocol.RunResult) (ExperimentSummary, error) {
	byRun := make(map[string]protocol.RunResult, len(results))
	for _, result := range results {
		if _, exists := byRun[result.RunID]; exists {
			return ExperimentSummary{}, fmt.Errorf("duplicate result for %s", result.RunID)
		}
		byRun[result.RunID] = result
	}
	byTest := map[string]*TestSummary{}
	summary := ExperimentSummary{Planned: len(specs), Complete: true, EvidenceComplete: true}
	seen := map[string]bool{}
	for _, spec := range specs {
		if seen[spec.RunID] {
			return ExperimentSummary{}, fmt.Errorf("duplicate run spec %s", spec.RunID)
		}
		seen[spec.RunID] = true
		test := byTest[spec.PresetID]
		if test == nil {
			test = &TestSummary{PresetID: spec.PresetID, Complete: true, EvidenceComplete: true}
			byTest[spec.PresetID] = test
		}
		mode := &test.Chaos
		if spec.Mode == protocol.ModeBaseline {
			mode = &test.Baseline
		}
		mode.Planned++
		result, exists := byRun[spec.RunID]
		if !exists {
			test.Complete = false
			test.EvidenceComplete = false
			summary.Complete = false
			summary.EvidenceComplete = false
			continue
		}
		mode.Terminal++
		summary.Terminal++
		if result.ExecutionStatus == protocol.StatusInfrastructureError || result.ExecutionStatus == protocol.StatusInterrupted || result.ExecutionStatus == protocol.StatusCanceled || result.FaultOutcome == protocol.FaultUnknown {
			test.EvidenceComplete = false
			summary.EvidenceComplete = false
		}
		if result.TaskSuccess != nil {
			mode.TaskSuccessKnown++
			if *result.TaskSuccess {
				mode.TaskSuccess++
			}
		}
		if result.FaultOutcome == protocol.FaultInjected {
			mode.Injected++
		}
		if result.FaultOutcome == protocol.FaultNotInjected {
			mode.NotInjected++
		}
		if result.FaultDetected != nil {
			mode.DetectionKnown++
			if *result.FaultDetected {
				mode.Detected++
			}
		}
		if result.Recovered != nil {
			mode.RecoveryKnown++
			if *result.Recovered {
				mode.Recovered++
			}
		}
		if result.FaultOutcome == protocol.FaultInjected && result.TaskSuccess != nil && !*result.TaskSuccess && result.FaultDetected != nil && !*result.FaultDetected {
			mode.SilentFailures++
		}
	}
	for runID := range byRun {
		if !seen[runID] {
			return ExperimentSummary{}, fmt.Errorf("result for unknown run %s", runID)
		}
	}
	for _, test := range byTest {
		summary.Tests = append(summary.Tests, *test)
	}
	sort.Slice(summary.Tests, func(i, j int) bool { return summary.Tests[i].PresetID < summary.Tests[j].PresetID })
	return summary, nil
}
