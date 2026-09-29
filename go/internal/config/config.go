package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"enragent/internal/catalog"
	"enragent/internal/protocol"
	"gopkg.in/yaml.v3"
)

const maxYAMLBytes = 1 << 20

type TestRef struct {
	Preset   string `yaml:"preset"`
	Quantity *int   `yaml:"quantity"`
}

type rootYAML struct {
	SchemaVersion int       `yaml:"schema_version"`
	Seed          *int64    `yaml:"seed"`
	Agent         agentYAML `yaml:"agent"`
	Tests         []TestRef `yaml:"tests"`
}

type agentYAML struct {
	Adapter     string `yaml:"adapter"`
	Entrypoint  string `yaml:"entrypoint"`
	ProjectRoot string `yaml:"project_root"`
	Python      string `yaml:"python"`
}

type taskYAML struct {
	ID    string    `yaml:"id"`
	Input yaml.Node `yaml:"input"`
}

type faultYAML struct {
	ID      string          `yaml:"id"`
	Kind    string          `yaml:"kind"`
	Target  protocol.Target `yaml:"target"`
	Trigger struct {
		Call *int `yaml:"call"`
	} `yaml:"trigger"`
	Params struct {
		Fixture        string    `yaml:"fixture"`
		TimestampField string    `yaml:"timestamp_field"`
		DurationMS     int       `yaml:"duration_ms"`
		StatusCode     int       `yaml:"status_code"`
		Message        string    `yaml:"message"`
		Fields         []string  `yaml:"fields"`
		DocumentID     string    `yaml:"document_id"`
		Key            string    `yaml:"key"`
		Value          yaml.Node `yaml:"value"`
	} `yaml:"params"`
}

type checkYAML struct {
	ID      string                 `yaml:"id"`
	Mode    string                 `yaml:"mode"`
	Purpose string                 `yaml:"purpose"`
	Type    string                 `yaml:"type"`
	Path    string                 `yaml:"path"`
	Value   yaml.Node              `yaml:"value"`
	Event   protocol.EventSelector `yaml:"event"`
	First   protocol.EventSelector `yaml:"first"`
	Then    protocol.EventSelector `yaml:"then"`
	Min     *int                   `yaml:"min"`
	Max     *int                   `yaml:"max"`
	MaxMS   int                    `yaml:"max_ms"`
}

type presetYAML struct {
	SchemaVersion int         `yaml:"schema_version"`
	ID            string      `yaml:"id"`
	Task          taskYAML    `yaml:"task"`
	Fault         faultYAML   `yaml:"fault"`
	Checks        []checkYAML `yaml:"checks"`
	Limits        struct {
		TimeoutSeconds int `yaml:"timeout_seconds"`
	} `yaml:"limits"`
}

type Preset struct {
	ID            string
	Path          string
	Task          protocol.Task
	Fault         protocol.Fault
	Checks        []protocol.Check
	Limits        protocol.Limits
	FixtureSHA256 string
}

type Test struct {
	Preset   Preset
	Quantity int
}

type Loaded struct {
	Path  string
	Agent protocol.Agent
	Seed  *int64
	Tests []Test
}

func Discover(path, cwd string) (string, error) {
	if path != "" {
		return filepath.Abs(path)
	}
	dir, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, "enragent.yaml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("enragent.yaml not found from %s to repository root", cwd)
}

func Load(path string) (Loaded, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return Loaded{}, err
	}
	var raw rootYAML
	if err := decodeFile(absolute, &raw); err != nil {
		return Loaded{}, err
	}
	if raw.SchemaVersion != protocol.SchemaVersion {
		return Loaded{}, fmt.Errorf("%s: unsupported schema_version %d", absolute, raw.SchemaVersion)
	}
	if raw.Agent.Adapter == "" || raw.Agent.Entrypoint == "" {
		return Loaded{}, fmt.Errorf("agent.adapter and agent.entrypoint are required")
	}
	if raw.Agent.Adapter != "python.raw" && raw.Agent.Adapter != "python.langchain" && raw.Agent.Adapter != "python.langgraph" {
		return Loaded{}, fmt.Errorf("unknown agent adapter %q", raw.Agent.Adapter)
	}
	if len(raw.Tests) == 0 {
		return Loaded{}, fmt.Errorf("tests must contain at least one preset")
	}
	root := filepath.Dir(absolute)
	projectRoot := resolve(root, raw.Agent.ProjectRoot)
	if raw.Agent.ProjectRoot == "" {
		projectRoot = root
	}
	if info, err := os.Stat(projectRoot); err != nil || !info.IsDir() {
		return Loaded{}, fmt.Errorf("agent.project_root must be an existing directory: %s", projectRoot)
	}
	agent := protocol.Agent{Adapter: raw.Agent.Adapter, Entrypoint: raw.Agent.Entrypoint, ProjectRoot: projectRoot}
	if raw.Agent.Python != "" {
		agent.Python = resolve(root, raw.Agent.Python)
	}
	loaded := Loaded{Path: absolute, Agent: agent, Seed: raw.Seed}
	seen := map[string]bool{}
	for _, ref := range raw.Tests {
		if ref.Preset == "" {
			return Loaded{}, fmt.Errorf("tests[].preset is required")
		}
		quantity := 1
		if ref.Quantity != nil {
			quantity = *ref.Quantity
		}
		if quantity < 1 || quantity > 100 {
			return Loaded{}, fmt.Errorf("%s: quantity must be between 1 and 100", ref.Preset)
		}
		preset, err := loadPreset(resolve(root, ref.Preset))
		if err != nil {
			return Loaded{}, err
		}
		if seen[preset.ID] {
			return Loaded{}, fmt.Errorf("duplicate preset id %q", preset.ID)
		}
		seen[preset.ID] = true
		loaded.Tests = append(loaded.Tests, Test{Preset: preset, Quantity: quantity})
	}
	return loaded, nil
}

func loadPreset(path string) (Preset, error) {
	var raw presetYAML
	if err := decodeFile(path, &raw); err != nil {
		return Preset{}, err
	}
	if raw.SchemaVersion != protocol.SchemaVersion {
		return Preset{}, fmt.Errorf("%s: unsupported schema_version %d", path, raw.SchemaVersion)
	}
	if raw.ID == "" || raw.Task.ID == "" {
		return Preset{}, fmt.Errorf("%s: id and task.id are required", path)
	}
	input, err := jsonValue(raw.Task.Input)
	if err != nil {
		return Preset{}, fmt.Errorf("%s task.input: %w", path, err)
	}
	if len(input) == 0 {
		return Preset{}, fmt.Errorf("%s: task.input is required", path)
	}
	value, err := jsonValue(raw.Fault.Params.Value)
	if err != nil {
		return Preset{}, fmt.Errorf("%s fault.params.value: %w", path, err)
	}
	call := 1
	if raw.Fault.Trigger.Call != nil {
		call = *raw.Fault.Trigger.Call
	}
	fault := protocol.Fault{
		ID: raw.Fault.ID, Kind: raw.Fault.Kind, Target: raw.Fault.Target,
		Trigger: protocol.Trigger{Call: call},
		Params: protocol.FaultParams{
			Fixture: raw.Fault.Params.Fixture, TimestampField: raw.Fault.Params.TimestampField, DurationMS: raw.Fault.Params.DurationMS,
			StatusCode: raw.Fault.Params.StatusCode, Message: raw.Fault.Params.Message,
			Fields: raw.Fault.Params.Fields, DocumentID: raw.Fault.Params.DocumentID,
			Key: raw.Fault.Params.Key, Value: value,
		},
	}
	if err := catalog.Validate(fault); err != nil {
		return Preset{}, fmt.Errorf("%s: %w", path, err)
	}
	preset := Preset{ID: raw.ID, Path: path, Task: protocol.Task{ID: raw.Task.ID, Input: input}, Fault: fault}
	if fault.Params.Fixture != "" {
		fixture := resolve(filepath.Dir(path), fault.Params.Fixture)
		contents, err := os.ReadFile(fixture)
		if err != nil {
			return Preset{}, fmt.Errorf("%s fixture: %w", path, err)
		}
		if len(contents) > maxYAMLBytes {
			return Preset{}, fmt.Errorf("%s fixture exceeds 1 MiB", fixture)
		}
		var decoded any
		if err := json.Unmarshal(contents, &decoded); err != nil {
			return Preset{}, fmt.Errorf("%s fixture: %w", fixture, err)
		}
		if fault.Kind == "tool.stale_response" && !hasJSONPointer(decoded, fault.Params.TimestampField) {
			return Preset{}, fmt.Errorf("%s fixture lacks timestamp_field %q", fixture, fault.Params.TimestampField)
		}
		hash := sha256.Sum256(contents)
		preset.FixtureSHA256 = hex.EncodeToString(hash[:])
		preset.Fault.Params.Fixture = fixture
	}
	ids := map[string]bool{}
	for _, c := range raw.Checks {
		checkValue, err := jsonValue(c.Value)
		if err != nil {
			return Preset{}, fmt.Errorf("%s check %q: %w", path, c.ID, err)
		}
		check := protocol.Check{ID: c.ID, Mode: c.Mode, Purpose: c.Purpose, Type: c.Type, Path: c.Path, Value: checkValue, Min: c.Min, Max: c.Max, MaxMS: c.MaxMS}
		if c.Event.Type != "" {
			check.Event = &c.Event
		}
		if c.First.Type != "" {
			check.First = &c.First
		}
		if c.Then.Type != "" {
			check.Then = &c.Then
		}
		if err := validateCheck(check); err != nil {
			return Preset{}, fmt.Errorf("%s: %w", path, err)
		}
		if ids[check.ID] {
			return Preset{}, fmt.Errorf("%s: duplicate check id %q", path, check.ID)
		}
		ids[check.ID] = true
		preset.Checks = append(preset.Checks, check)
	}
	timeout := raw.Limits.TimeoutSeconds
	if timeout == 0 {
		timeout = 30
	}
	if timeout < 1 || timeout > 3600 {
		return Preset{}, fmt.Errorf("%s: limits.timeout_seconds must be between 1 and 3600", path)
	}
	preset.Limits = protocol.Limits{TimeoutSeconds: timeout}
	return preset, nil
}

func validateCheck(c protocol.Check) error {
	if c.ID == "" {
		return fmt.Errorf("check id is required")
	}
	if c.Mode != "baseline" && c.Mode != "chaos" && c.Mode != "both" {
		return fmt.Errorf("check %q mode must be baseline, chaos, or both", c.ID)
	}
	if c.Purpose != "" && c.Purpose != "success" && c.Purpose != "detection" && c.Purpose != "recovery" && c.Purpose != "injection" {
		return fmt.Errorf("check %q has unknown purpose %q", c.ID, c.Purpose)
	}
	switch c.Type {
	case "output.field_equals":
		if !strings.HasPrefix(c.Path, "/") || len(c.Value) == 0 {
			return fmt.Errorf("check %q needs JSON pointer path and value", c.ID)
		}
		if c.Event != nil || c.First != nil || c.Then != nil || c.Min != nil || c.Max != nil || c.MaxMS != 0 {
			return fmt.Errorf("check %q has fields unused by output.field_equals", c.ID)
		}
	case "event.count":
		if c.Event == nil || (c.Min == nil && c.Max == nil) {
			return fmt.Errorf("check %q needs event.type and min or max", c.ID)
		}
		if c.Min != nil && *c.Min < 0 {
			return fmt.Errorf("check %q min must be nonnegative", c.ID)
		}
		if c.Max != nil && *c.Max < 0 {
			return fmt.Errorf("check %q max must be nonnegative", c.ID)
		}
		if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
			return fmt.Errorf("check %q min exceeds max", c.ID)
		}
		if c.Path != "" || len(c.Value) != 0 || c.First != nil || c.Then != nil || c.MaxMS != 0 {
			return fmt.Errorf("check %q has fields unused by event.count", c.ID)
		}
	case "event.after":
		if c.First == nil || c.Then == nil {
			return fmt.Errorf("check %q needs first.type and then.type", c.ID)
		}
		if c.Path != "" || len(c.Value) != 0 || c.Event != nil || c.Min != nil || c.Max != nil || c.MaxMS != 0 {
			return fmt.Errorf("check %q has fields unused by event.after", c.ID)
		}
	case "duration.max":
		if c.MaxMS < 1 {
			return fmt.Errorf("check %q max_ms must be positive", c.ID)
		}
		if c.Path != "" || len(c.Value) != 0 || c.Event != nil || c.First != nil || c.Then != nil || c.Min != nil || c.Max != nil {
			return fmt.Errorf("check %q has fields unused by duration.max", c.ID)
		}
	default:
		return fmt.Errorf("check %q has unknown type %q", c.ID, c.Type)
	}
	return nil
}

func decodeFile(path string, out any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxYAMLBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxYAMLBytes {
		return fmt.Errorf("%s exceeds 1 MiB", path)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return fmt.Errorf("%s: multiple YAML documents are not supported", path)
	}
	return nil
}

func jsonValue(node yaml.Node) (json.RawMessage, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	var value any
	if err := node.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func resolve(base, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(base, path))
}

func hasJSONPointer(value any, pointer string) bool {
	if !strings.HasPrefix(pointer, "/") {
		return false
	}
	for _, token := range strings.Split(pointer[1:], "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		value, ok = object[token]
		if !ok {
			return false
		}
	}
	return value != nil
}
