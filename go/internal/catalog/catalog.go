package catalog

import (
	"fmt"
	"slices"

	"enragent/internal/protocol"
)

type Definition struct {
	Kind         string `json:"kind"`
	Category     string `json:"category"`
	RequiredHook string `json:"required_hook"`
}

var definitions = []Definition{
	{"tool.stale_response", "tool", "tool.result.replace"},
	{"tool.error", "tool", "tool.result.error"},
	{"tool.delay", "tool", "tool.result.delay"},
	{"tool.partial_response", "tool", "tool.result.patch"},
	{"tool.contradictory_response", "tool", "tool.result.replace"},
	{"retrieval.drop_document", "retrieval", "retrieval.result.patch"},
	{"retrieval.insert_document", "retrieval", "retrieval.result.patch"},
	{"state.patch", "state", "graph.state.patch"},
	{"memory.replace", "memory", "memory.read.replace"},
	{"flow.skip_node", "flow", "graph.node.skip"},
	{"network.timeout", "network", "proxy.request.timeout"},
	{"network.http_status", "network", "proxy.response.replace"},
	{"network.reset", "network", "proxy.connection.reset"},
}

func List() []Definition { return slices.Clone(definitions) }

func Lookup(kind string) (Definition, bool) {
	for _, definition := range definitions {
		if definition.Kind == kind {
			return definition, true
		}
	}
	return Definition{}, false
}

func Compatible(kind, adapter string) bool {
	definition, ok := Lookup(kind)
	if !ok {
		return false
	}
	switch definition.Category {
	case "state", "flow":
		return adapter == "python.langgraph"
	case "memory":
		return adapter == "python.langgraph" || adapter == "python.langchain"
	default:
		return adapter == "python.raw" || adapter == "python.langchain" || adapter == "python.langgraph"
	}
}

func Validate(f protocol.Fault) error {
	definition, ok := Lookup(f.Kind)
	if !ok {
		return fmt.Errorf("unknown fault kind %q", f.Kind)
	}
	if f.ID == "" {
		return fmt.Errorf("fault id is required")
	}
	if f.Trigger.Call < 1 {
		return fmt.Errorf("fault %q trigger.call must be positive", f.ID)
	}
	if err := validateTarget(definition.Category, f.Target); err != nil {
		return fmt.Errorf("fault %q: %w", f.ID, err)
	}
	if err := validateParams(f.Kind, f.Params); err != nil {
		return fmt.Errorf("fault %q: %w", f.ID, err)
	}
	return nil
}

func validateTarget(category string, t protocol.Target) error {
	fields := map[string]string{
		"tool": t.Tool, "retrieval": t.Retriever, "state": t.Node,
		"memory": t.MemoryKey, "flow": t.Node, "network": t.Route,
	}
	if fields[category] == "" {
		return fmt.Errorf("%s target is required", category)
	}
	count := 0
	for _, value := range []string{t.Tool, t.Retriever, t.Node, t.MemoryKey, t.Route} {
		if value != "" {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("target must name exactly one boundary")
	}
	return nil
}

func validateParams(kind string, p protocol.FaultParams) error {
	set := make(map[string]bool)
	if p.Fixture != "" {
		set["fixture"] = true
	}
	if p.TimestampField != "" {
		set["timestamp_field"] = true
	}
	if p.DurationMS != 0 {
		set["duration_ms"] = true
	}
	if p.StatusCode != 0 {
		set["status_code"] = true
	}
	if p.Message != "" {
		set["message"] = true
	}
	if len(p.Fields) != 0 {
		set["fields"] = true
	}
	if p.DocumentID != "" {
		set["document_id"] = true
	}
	if p.Key != "" {
		set["key"] = true
	}
	if len(p.Value) != 0 {
		set["value"] = true
	}
	var required []string
	switch kind {
	case "tool.stale_response":
		required = []string{"fixture", "timestamp_field"}
	case "tool.contradictory_response", "retrieval.insert_document":
		required = []string{"fixture"}
	case "tool.error":
		required = []string{"message"}
	case "tool.delay", "network.timeout":
		required = []string{"duration_ms"}
	case "tool.partial_response":
		required = []string{"fields"}
	case "retrieval.drop_document":
		required = []string{"document_id"}
	case "state.patch":
		required = []string{"key", "value"}
	case "memory.replace":
		required = []string{"value"}
	case "network.http_status":
		required = []string{"status_code"}
	case "flow.skip_node", "network.reset":
	default:
		return fmt.Errorf("unknown fault kind %q", kind)
	}
	for _, key := range required {
		if !set[key] {
			return fmt.Errorf("params.%s is required", key)
		}
	}
	if len(set) != len(required) {
		return fmt.Errorf("params contain fields unused by %s", kind)
	}
	if p.DurationMS < 0 || p.DurationMS > 300_000 {
		return fmt.Errorf("params.duration_ms must be between 1 and 300000")
	}
	if p.StatusCode != 0 && (p.StatusCode < 400 || p.StatusCode > 599) {
		return fmt.Errorf("params.status_code must be a 4xx or 5xx status")
	}
	return nil
}
