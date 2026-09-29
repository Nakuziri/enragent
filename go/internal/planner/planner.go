package planner

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"

	"enragent/internal/catalog"
	"enragent/internal/config"
	"enragent/internal/protocol"
)

const MaxRuns = 1000

type PresetReadiness struct {
	PresetID          string `json:"preset_id"`
	FaultKind         string `json:"fault_kind"`
	RequiredHook      string `json:"required_hook"`
	AdapterCompatible bool   `json:"adapter_compatible"`
	Runnable          bool   `json:"runnable"`
}

type Plan struct {
	SchemaVersion int                `json:"schema_version"`
	ExperimentID  string             `json:"experiment_id"`
	RootSeed      int64              `json:"root_seed"`
	ConfigPath    string             `json:"config_path"`
	Runs          []protocol.RunSpec `json:"runs"`
	Readiness     []PresetReadiness  `json:"readiness"`
}

func Build(loaded config.Loaded) (Plan, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return Plan{}, err
	}
	experimentID := "exp_" + hex.EncodeToString(entropy[:])
	seed := int64(binary.BigEndian.Uint64(entropy[:8]))
	if loaded.Seed != nil {
		seed = *loaded.Seed
	}
	return BuildWithIdentity(loaded, experimentID, seed)
}

func BuildWithIdentity(loaded config.Loaded, experimentID string, rootSeed int64) (Plan, error) {
	if experimentID == "" {
		return Plan{}, fmt.Errorf("experiment ID is required")
	}
	total := 0
	for _, test := range loaded.Tests {
		total += 2 * test.Quantity
	}
	if total > MaxRuns {
		return Plan{}, fmt.Errorf("experiment expands to %d runs; maximum is %d", total, MaxRuns)
	}
	plan := Plan{SchemaVersion: protocol.SchemaVersion, ExperimentID: experimentID, RootSeed: rootSeed, ConfigPath: loaded.Path}
	for _, test := range loaded.Tests {
		definition, _ := catalog.Lookup(test.Preset.Fault.Kind)
		plan.Readiness = append(plan.Readiness, PresetReadiness{PresetID: test.Preset.ID, FaultKind: definition.Kind, RequiredHook: definition.RequiredHook, AdapterCompatible: catalog.Compatible(definition.Kind, loaded.Agent.Adapter), Runnable: false})
		for index := 1; index <= test.Quantity; index++ {
			pairID := stableID("pair", experimentID, test.Preset.ID, fmt.Sprint(index))
			pairSeed := stableSeed(rootSeed, test.Preset.ID, index)
			for _, mode := range []protocol.Mode{protocol.ModeBaseline, protocol.ModeChaos} {
				checks := make([]protocol.Check, 0, len(test.Preset.Checks))
				for _, check := range test.Preset.Checks {
					if check.Mode == "both" || check.Mode == string(mode) {
						checks = append(checks, check)
					}
				}
				spec := protocol.RunSpec{
					SchemaVersion: protocol.SchemaVersion, ExperimentID: experimentID,
					RunID: stableID("run", pairID, string(mode)), PairID: pairID,
					PresetID: test.Preset.ID, Repetition: index, Mode: mode, Seed: pairSeed,
					Agent: loaded.Agent, Task: test.Preset.Task, Checks: slices.Clone(checks),
					Limits: test.Preset.Limits,
				}
				if mode == protocol.ModeChaos {
					fault := test.Preset.Fault
					fault.Params.FixtureSHA256 = test.Preset.FixtureSHA256
					spec.Fault = &fault
				}
				plan.Runs = append(plan.Runs, spec)
			}
		}
	}
	return plan, nil
}

func stableSeed(root int64, preset string, index int) int64 {
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], uint64(root))
	hash := sha256.Sum256([]byte(fmt.Sprintf("%x:%s:%d", seed, preset, index)))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		hash.Write([]byte(part))
		hash.Write([]byte{0})
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil)[:12])
}
