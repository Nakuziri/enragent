# Enragent codebase rules

## Product contract

- Enragent tests agent resilience through paired baseline and faulted experiments. A fault requested in YAML is not counted as injected until complete event evidence confirms it.
- The first layer is a local foreground Go control plane. Python adapters will own agent-aware hooks; the Go proxy will own opt-in network faults. Do not claim runtime support for a catalog kind until its hook and integration tests exist.
- Keep execution status, task verdict, and fault outcome separate. A clean process exit does not imply task success. Missing task oracles yield `needs_review`.
- One chaos RunSpec carries one fault. Paired runs share task, limits, and seed. A retry creates a new attempt ID and process; interrupted attempts are not retried automatically.

## Go boundaries

- `config` decodes and validates authoring YAML. `catalog` describes fault kinds and required hooks. `planner` creates canonical protocol values. `state` owns legal attempt transitions and check evaluation. `engine` owns channel admission, worker lifetimes, and cancellation. `journal` is the durable source of truth.
- Use versioned, typed Go structs for config, protocol, fault parameters, checks, and statuses. Confine opaque JSON to agent input, fixture values, and output assertions. Reject unknown YAML fields and unsupported schema versions.
- The scheduler alone sends to and closes the shared run channel. Close it only after admission stops. Close the event ingress channel only after all event producers finish. An empty channel is not a completion signal.
- Persist a RunSpec before queue admission and persist state transitions. Sync event and result records before acknowledging them. Surface storage failures; do not silently discard fault evidence or terminal results.
- Track active operations by ID. Reject invalid transitions and event sequence gaps. Derive test-case and experiment completion from attempt records rather than maintaining competing mutable status fields.
- Honor context cancellation and per-run deadlines in every executor. Keep fixtures and external state isolated between baseline and chaos attempts.

## Working practices

- Update `README.md` and the Enragent Docmost space when YAML contracts, supported hooks, or phase ordering changes. Treat examples as validation fixtures and keep their support status accurate.
- Add focused tests for state transitions, quantity expansion, worker bounds, cancellation, event acknowledgment, and recovery when changing those paths. Run `go test ./...`, `go test -race ./...`, `go vet ./...`, and example YAML validation before merging.
- Keep event payloads to identifiers and metadata by default. Do not journal credentials or raw sensitive tool responses. Restrict local run-data permissions and retain data until explicit cleanup.
- Keep dependencies small and pinned. Use Go standard libraries for concurrency, process supervision, JSON, and local persistence where practical.
