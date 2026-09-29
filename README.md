# Enragent

Enragent is a local chaos-testing framework for AI agents. It plans paired baseline and faulted runs, records whether a requested fault actually fired, and keeps execution outcomes separate from task verdicts. This repository currently contains the Go control-plane foundation. Agent execution and fault hooks are the next integration layer.

## Repository layout

| Path | Purpose |
| --- | --- |
| `go/cmd/enragent` | CLI for validation, planning, and catalog inspection |
| `go/internal/config`, `catalog`, `planner` | Strict YAML loading, fault definitions, paired run expansion |
| `go/internal/state`, `engine`, `journal` | Attempt state machine, bounded workers, durable local records |
| `go/internal/protocol` | Versioned JSON types for runs, events, and results |
| `examples/enragent.yaml` | Suite selecting the 13 starter scenario presets |
| `examples/presets`, `examples/fixtures` | Scenario definitions and sample fault payloads |

## Commands

From `go/`:

```bash
go run ./cmd/enragent validate ../examples/enragent.yaml
go run ./cmd/enragent plan --json ../examples/enragent.yaml
go run ./cmd/enragent presets list
go test ./...
```

`validate` checks structure, fault parameters, fixtures, quantity, and plan limits. `plan` also emits an experiment ID and seed. Both commands can omit the path when an `enragent.yaml` is in the current directory or an ancestor up to the repository root. These commands do not invoke an agent. The scheduler and event ingress are exercised by deterministic Go integration tests; a public `run` command will follow the Python adapter and durable event acknowledgment protocol.

## YAML contract, version 1

An experiment file selects an agent and scenario preset files:

```yaml
schema_version: 1
seed: 81237
agent:
  adapter: python.raw
  entrypoint: monitor_agent:run
  project_root: .
tests:
  - preset: presets/tool-stale-response.yaml
    quantity: 3
```

Each preset contains a task, one fault, assertions, and an optional timeout:

```yaml
schema_version: 1
id: cpu_stale_metrics
task:
  id: cpu_triage
  input: {question: "Is the CPU alert still active?"}
fault:
  id: stale_metrics
  kind: tool.stale_response
  target: {tool: metrics.query}
  trigger: {call: 2}
  params: {fixture: ../fixtures/stale-metrics.json, timestamp_field: /observed_at}
checks:
  - id: task_success
    mode: both
    purpose: success
    type: output.field_equals
    path: /alert_active
    value: false
  - id: fresh_verification
    mode: chaos
    purpose: detection
    type: event.after
    first: {type: FAULT_INJECTED, fault_id: stale_metrics}
    then: {type: TOOL_CALLED, target: metrics.verify_freshness}
limits: {timeout_seconds: 30}
```

`quantity: N` creates N baseline/chaos pairs, or `2N` logical runs. A pair uses the same task and seed; only the chaos run carries the fault. Quantity defaults to 1 and is limited to 100 per preset, with a 1,000-run experiment limit. A new experiment ID is generated for each plan. Supplying `seed` makes pair seeds reproducible across plans.

Paths in `agent` are relative to `enragent.yaml`; fixture paths are relative to their preset file. A stale-response preset must name a `timestamp_field` JSON Pointer that exists in its fixture. The planner records a SHA-256 hash of each fixture. Unknown YAML keys, unsupported schema versions, duplicate preset IDs, invalid parameters, missing fixtures, and extra YAML documents fail validation. YAML contains data and registered fault names, never executable expressions.

Checks support `output.field_equals` with a JSON Pointer, `event.count` with `min` and/or `max`, `event.after` with `first` and `then` selectors, and `duration.max` with `max_ms`. Each check has `mode: baseline`, `chaos`, or `both`. Use `purpose: success` for a task oracle; other purposes are `detection`, `recovery`, and `injection`. Without a success oracle, an otherwise successful attempt remains `needs_review`. A chaos attempt whose fault never fires is `not_injected` and inconclusive.

## Preset catalog

| Category | Fault kinds | Runtime hook required |
| --- | --- | --- |
| Tool | `tool.stale_response`, `tool.error`, `tool.delay`, `tool.partial_response`, `tool.contradictory_response` | Wrapped tool result |
| Retrieval | `retrieval.drop_document`, `retrieval.insert_document` | Wrapped retriever result |
| State | `state.patch` | Mutable graph state boundary |
| Memory | `memory.replace` | Mutable memory read boundary |
| Flow | `flow.skip_node` | Controllable graph node boundary |
| Network | `network.timeout`, `network.http_status`, `network.reset` | Opt-in Go proxy route |

All 13 kinds have typed validation, sample YAML, and planning support. Their runtime hooks are pending; `validate` and `plan` report the required hook and whether the selected adapter is compatible. `python.raw` can eventually support explicit tool and retrieval wrappers; state and flow mutation require `python.langgraph`, while memory mutation requires a framework adapter. Most samples assert only that injection occurs and need a task-specific success oracle before they can yield a passing agent verdict. Stale and contradictory evidence are scenarios at a tool or context boundary, not separate mutation mechanisms.

## Execution model

The scheduler owns a shared bounded `RunSpec` channel and its closure. Workers consume from it until admission stops and the queue drains. A separate bounded ingress channel sends events and terminal results to one journal writer; producers wait for sync acknowledgment. The attempt lifecycle is `planned -> queued -> running -> terminal`. Cancellation and timeouts produce terminal results when the executor honors its context. An interrupted attempt is never silently rerun; explicit retry will use a new attempt ID. Test-case and experiment summaries derive completion and evidence quality from their attempt records, with separate counts for task success, detection, recovery, and silent failure.

The journal keeps `runs.jsonl` plus per-run event files under `.enragent/runs/<experiment-id>` when the CLI execution path is added. Records have CRC32C checks and are synced before acknowledgment. Recovery requeues queued work, marks formerly running attempts interrupted, and refuses incomplete or corrupt evidence. The test-case and experiment completion status is derived from their attempts. Run data is retained until explicitly removed.

Worker count, run queue capacity, event queue capacity, and shutdown deadline default to 4, 32, 256, and 30 seconds. Override with `ENRAGENT_WORKERS`, `ENRAGENT_RUN_QUEUE_CAPACITY`, `ENRAGENT_EVENT_QUEUE_CAPACITY`, and `ENRAGENT_SHUTDOWN_TIMEOUT` (Go duration syntax).

## Next integration

Connect one fresh Python child process per attempt, beginning with `python.raw` and a wrapped stale-response tool. Define versioned `START`, `EVENT`, `ACK`, `RESULT`, and `CANCEL` envelopes; persist Python events in an outbox until the Go journal acknowledges them. Then expose `enragent run`, report baseline-versus-chaos metrics, and gate CI on explicit experiment thresholds.
