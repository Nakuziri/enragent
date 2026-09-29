package planner

import (
	"testing"

	"enragent/internal/config"
	"enragent/internal/protocol"
)

func TestPairExpansion(t *testing.T) {
	loaded, err := config.Load("../../../examples/enragent.yaml")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildWithIdentity(loaded, "exp_test", 81237)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Runs) != 28 {
		t.Fatalf("got %d runs, want 28", len(plan.Runs))
	}
	if len(plan.Readiness) != 13 {
		t.Fatalf("got %d readiness entries", len(plan.Readiness))
	}
	for index := 0; index < len(plan.Runs); index += 2 {
		baseline, chaos := plan.Runs[index], plan.Runs[index+1]
		if baseline.Mode != protocol.ModeBaseline || chaos.Mode != protocol.ModeChaos {
			t.Fatalf("pair %d has wrong modes", index/2)
		}
		if baseline.PairID != chaos.PairID || baseline.Seed != chaos.Seed || baseline.Task.ID != chaos.Task.ID {
			t.Fatalf("pair %d is not comparable", index/2)
		}
		if baseline.Fault != nil || chaos.Fault == nil {
			t.Fatalf("pair %d has wrong fault placement", index/2)
		}
	}
	again, err := BuildWithIdentity(loaded, "exp_test", 81237)
	if err != nil {
		t.Fatal(err)
	}
	for index := range plan.Runs {
		if plan.Runs[index].RunID != again.Runs[index].RunID || plan.Runs[index].Seed != again.Runs[index].Seed {
			t.Fatal("plan was not deterministic")
		}
	}
}

func TestMaxRuns(t *testing.T) {
	loaded, err := config.Load("../../../examples/enragent.yaml")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Tests[0].Quantity = MaxRuns
	if _, err := BuildWithIdentity(loaded, "exp_test", 1); err == nil {
		t.Fatal("expected run limit error")
	}
}
