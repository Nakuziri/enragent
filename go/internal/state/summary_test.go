package state

import (
	"testing"

	"enragent/internal/protocol"
)

func TestSummaryDerivedFromResults(t *testing.T) {
	falseValue, trueValue := false, true
	specs := []protocol.RunSpec{
		{RunID: "baseline", PresetID: "case", Mode: protocol.ModeBaseline},
		{RunID: "chaos", PresetID: "case", Mode: protocol.ModeChaos},
	}
	partial, err := Summarize(specs, []protocol.RunResult{{RunID: "baseline", ExecutionStatus: protocol.StatusCompleted, TaskSuccess: &trueValue, FaultOutcome: protocol.FaultNone}})
	if err != nil {
		t.Fatal(err)
	}
	if partial.Complete || partial.Terminal != 1 {
		t.Fatalf("unexpected partial summary: %+v", partial)
	}
	results := []protocol.RunResult{
		{RunID: "baseline", ExecutionStatus: protocol.StatusCompleted, TaskSuccess: &trueValue, FaultOutcome: protocol.FaultNone},
		{RunID: "chaos", ExecutionStatus: protocol.StatusCompleted, TaskSuccess: &falseValue, FaultDetected: &falseValue, FaultOutcome: protocol.FaultInjected},
	}
	complete, err := Summarize(specs, results)
	if err != nil {
		t.Fatal(err)
	}
	if !complete.Complete || !complete.EvidenceComplete || complete.Tests[0].Chaos.SilentFailures != 1 {
		t.Fatalf("unexpected complete summary: %+v", complete)
	}
}
