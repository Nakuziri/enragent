package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"enragent/internal/catalog"
	"enragent/internal/config"
	"enragent/internal/planner"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "enragent:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "presets":
		if len(args) != 2 || args[1] != "list" {
			return usage()
		}
		for _, definition := range catalog.List() {
			fmt.Printf("%-30s %-10s %s\n", definition.Kind, definition.Category, definition.RequiredHook)
		}
		return nil
	case "validate", "plan":
		flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		flags.SetOutput(os.Stderr)
		asJSON := flags.Bool("json", false, "write the complete plan as JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() > 1 {
			return usage()
		}
		path := ""
		if flags.NArg() == 1 {
			path = flags.Arg(0)
		}
		selected, err := config.Discover(path, ".")
		if err != nil {
			return err
		}
		loaded, err := config.Load(selected)
		if err != nil {
			return err
		}
		plan, err := planner.Build(loaded)
		if err != nil {
			return err
		}
		if *asJSON {
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(plan)
		}
		fmt.Printf("%s: %d presets, %d paired repetitions, %d runs\n", selected, len(loaded.Tests), len(plan.Runs)/2, len(plan.Runs))
		for _, readiness := range plan.Readiness {
			status := "runtime pending"
			if !readiness.AdapterCompatible {
				status = "adapter incompatible"
			}
			fmt.Printf("  %-28s %-30s requires %s (%s)\n", readiness.PresetID, readiness.FaultKind, readiness.RequiredHook, status)
		}
		if args[0] == "plan" {
			fmt.Printf("experiment_id=%s seed=%d\n", plan.ExperimentID, plan.RootSeed)
		}
		return nil
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage: enragent validate [--json] [config-path] | plan [--json] [config-path] | presets list")
}
