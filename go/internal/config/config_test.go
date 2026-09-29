package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExampleCatalogLoads(t *testing.T) {
	loaded, err := Load("../../../examples/enragent.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Tests) != 13 {
		t.Fatalf("got %d presets, want 13", len(loaded.Tests))
	}
	if loaded.Tests[0].Quantity != 2 {
		t.Fatalf("stale preset quantity = %d", loaded.Tests[0].Quantity)
	}
	if loaded.Tests[0].Preset.FixtureSHA256 == "" {
		t.Fatal("fixture hash missing")
	}
}

func TestStrictYAML(t *testing.T) {
	for _, test := range []struct{ name, config, want string }{
		{"unknown key", "schema_version: 1\nagent: {adapter: python.raw, entrypoint: sample:run}\ntests: [{preset: preset.yaml}]\nunexpected: true\n", "field unexpected not found"},
		{"duplicate key", "schema_version: 1\nschema_version: 1\n", "already defined"},
		{"bad quantity", "schema_version: 1\nagent: {adapter: python.raw, entrypoint: sample:run}\ntests: [{preset: preset.yaml, quantity: 0}]\n", "quantity must be"},
		{"multiple documents", "schema_version: 1\n---\nschema_version: 1\n", "multiple YAML documents"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "enragent.yaml")
			if err := os.WriteFile(path, []byte(test.config), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDiscoverNearest(t *testing.T) {
	dir := t.TempDir()
	child := filepath.Join(dir, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "enragent.yaml")
	if err := os.WriteFile(path, []byte("schema_version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	selected, err := Discover("", child)
	if err != nil {
		t.Fatal(err)
	}
	if selected != path {
		t.Fatalf("selected %s, want %s", selected, path)
	}
}
