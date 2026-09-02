# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 5 adds a remote runner protocol with persistent registered runners and whole-run leasing. Direct execution remains available, while `forge-server` can queue runs, persist run and job state in PostgreSQL, and hand off an entire pipeline run to a registered remote runner over a runner-authenticated HTTP protocol.

## Current capabilities

- Strict `forge.yaml` parsing and validation
- Deterministic dependency ordering and cycle rejection
- Bounded dependency-aware local job parallelism
- Deterministic admission and completion-driven scheduling
- Cancellation with live, race-safe stdout and stderr
- Failed-job propagation while independent jobs continue
- Optional Docker jobs through the official Moby Go SDK
- One container per Docker job with live, demultiplexed output
- Repository workspace mounting and reliable container cleanup
- Durable PostgreSQL run history and pipeline-definition snapshots
- FIFO dispatch with live job-state persistence; one local run or one remote run per runner
- Remote runner registration, heartbeat, lease acquisition, and completion reporting
- Persistent runner inventory with `GET /v1/runners`
- Local HTTP API and CLI commands for submit, list, inspect, cancel, and runner listing
- Restart recovery for queued, completed, and interrupted runs
- Stable summaries and process exit codes

## Architecture

```text
HTTP API → control plane → PostgreSQL → queued run
    │
    └── remote runner protocol → registered runner → leased pipeline payload → executor
```

Parsing, graph compilation, runtime state, and command execution remain separate so the control plane never re-interprets raw YAML. The control plane owns queueing and run lifecycle, while a persistent remote runner owns a leased whole-run execution. See [docs/architecture.md](docs/architecture.md) for the component boundaries.

## Pipeline example

```yaml
version: 1

jobs:
  build:
    steps:
      - run: go build ./...

  test:
    needs:
      - build
    image: golang:1.27
    steps:
      - run: go test ./...
```

The complete schema is documented in [docs/pipeline-format.md](docs/pipeline-format.md).

## Quick start

Go 1.27 or newer is required. Docker jobs additionally require a reachable Docker Engine; ForgeCI honors the standard Docker client environment variables and negotiates the daemon API version.

```bash
go build -o build/forge ./cmd/forge
./build/forge run
```

To run a different file:

```bash
./build/forge run --file forge.example.yaml
./build/forge run --jobs 3 --file forge.example.yaml
```

Direct mode does not require PostgreSQL or `forge-server`. For persistent server-backed runs and the runner protocol, see [docs/control-plane.md](docs/control-plane.md).

To run the remote runner protocol locally:

```bash
go build -o build/forge-server ./cmd/forge-server
go build -o build/forge-runner ./cmd/forge-runner
./build/forge-server \
  --listen 127.0.0.1:8080 \
  --runner-listen 127.0.0.1:9090 \
  --workspace "$(pwd)" \
  --execution-mode remote \
  --database-url 'postgres://postgres:forgeci@127.0.0.1:5432/forgeci?sslmode=disable' \
  --runner-token-file /path/to/runner-token

./build/forge-runner \
  --server http://127.0.0.1:9090 \
  --workspace "$(pwd)"
```

Set `FORGECI_RUNNER_TOKEN` for the runner. Plain HTTP is accepted only on a loopback runner listener; non-loopback listeners require `--runner-tls-cert` and `--runner-tls-key`, and runners can trust a private CA with `--ca-cert`.

ForgeCI executes commands from the directory where it was invoked, even when `--file` names a file in another directory.

## CLI

```text
forge --help
forge run --help
forge run
forge run --file <path>
forge run --jobs <N>
forge submit --file <path> --jobs <N>
forge runs --limit <N>
forge inspect <run-id>
forge cancel <run-id>
```

Exit code `0` means success, `1` means at least one job failed or was blocked, and `2` means a CLI, configuration, compilation, or interruption error.

`--jobs` must be greater than zero and defaults to `1`, preserving sequential Milestone 1 behavior.

## What Milestone 5 demonstrates

This milestone separates the control plane, which chooses and records the active pipeline run, from remote runner processes that register, renew leases, execute a full pipeline payload, and report completion back to the server. Runner inventory and lease state survive process restarts, local mode remains available, and the remote runner path participates in the same durable run lifecycle.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Local commands run with the invoking user's privileges. Docker execution adds process/filesystem isolation but bind-mounts the repository read-write, and access to the Docker daemon is itself privileged. It is not a sandbox for hostile repositories.

The HTTP API remains loopback-only for the control-plane surface, and the runner listener uses a bearer token for authentication. Remote workers are persistent and registered, but this milestone still has no strong secret isolation, multi-tenant isolation, persistent logs, artifacts, cache, SCM-trigger integration, deployment system, web UI, or production security model. It is not yet a production-safe runner.

## Roadmap

Source synchronization, isolated per-run workspaces, distributed control-plane coordination, per-runner credentials, and broader platform features remain deferred beyond this milestone.
