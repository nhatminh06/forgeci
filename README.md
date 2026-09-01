# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 4 adds a small persistent local control plane. Direct execution remains available, while `forge-server` can queue runs, persist run and job state in PostgreSQL, and expose submission, inspection, listing, and cancellation over a localhost HTTP API.

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
- FIFO, single-active-run dispatch with live job-state persistence
- Local HTTP API and CLI commands for submit, list, inspect, and cancel
- Restart recovery for queued, completed, and interrupted runs
- Stable summaries and process exit codes

## Architecture

```text
HTTP → run manager → PostgreSQL → dispatcher → DAG scheduler → local or Docker executor
```

Parsing, graph compilation, runtime state, and command execution are separate so commands never need to reinterpret raw YAML. See [docs/architecture.md](docs/architecture.md) for the component boundaries.

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

Direct mode does not require PostgreSQL or `forge-server`. For persistent server-backed runs, see [docs/control-plane.md](docs/control-plane.md).

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

## What Milestone 4 demonstrates

This milestone separates the control plane, which chooses and records the active pipeline run, from the scheduler, which decides which jobs inside that run are ready. Run definitions and live state survive process restarts; direct mode remains database-independent.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Local commands run with the invoking user's privileges. Docker execution adds process/filesystem isolation but bind-mounts the repository read-write, and access to the Docker daemon is itself privileged. It is not a sandbox for hostile repositories.

The HTTP API is intentionally unauthenticated, plaintext, and loopback-only. ForgeCI has no secret isolation, multi-tenant isolation, remote workers, persistent logs, artifacts, cache, SCM-trigger integration, deployment system, web UI, or authentication. It is not yet a production-safe runner.

## Roadmap

Remote runners and distributed systems remain intentionally deferred.
