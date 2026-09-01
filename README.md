# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 2 implements a bounded parallel local scheduler. ForgeCI reads a repository-owned YAML file, validates it, compiles dependencies into a directed acyclic graph (DAG), and runs ready jobs on the current machine. Execution remains local; job concurrency is selected at runtime.

## Current capabilities

- Strict `forge.yaml` parsing and validation
- Deterministic dependency ordering and cycle rejection
- Bounded dependency-aware local job parallelism
- Deterministic admission and completion-driven scheduling
- Cancellation with live, race-safe stdout and stderr
- Failed-job propagation while independent jobs continue
- Stable summaries and process exit codes

## Architecture

```text
forge.yaml → parser → validation → DAG compiler → bounded scheduler → local executors
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
    steps:
      - run: go test ./...
```

The complete schema is documented in [docs/pipeline-format.md](docs/pipeline-format.md).

## Quick start

Go 1.27 or newer is required.

```bash
go build -o build/forge ./cmd/forge
./build/forge run
```

To run a different file:

```bash
./build/forge run --file forge.example.yaml
./build/forge run --jobs 3 --file forge.example.yaml
```

ForgeCI executes commands from the directory where it was invoked, even when `--file` names a file in another directory.

## CLI

```text
forge --help
forge run --help
forge run
forge run --file <path>
forge run --jobs <N>
```

Exit code `0` means success, `1` means at least one job failed or was blocked, and `2` means a CLI, configuration, compilation, or interruption error.

`--jobs` must be greater than zero and defaults to `1`, preserving sequential Milestone 1 behavior.

## What Milestone 2 demonstrates

This milestone demonstrates the mechanics of a bounded scheduler: one owner for runtime state, deterministic ready-job admission, worker completion events, dependency gating, failure propagation, cancellation, and race-safe live output. Steps inside each job remain sequential. The example pipeline lets ForgeCI locally execute ForgeCI's own checks and build; this is preparation, not full self-hosting.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Commands run with the privileges of the user invoking ForgeCI. There is no sandbox, container isolation, secret isolation, multi-tenant isolation, or remote execution isolation.

Milestone 2 has no process isolation, remote workers, persistent database, artifacts, cache, SCM-trigger integration, deployment system, web UI, or authentication. It is not yet a production-safe runner.

## Roadmap

The next logical milestone is a Docker job executor that adds per-job container isolation while preserving the scheduler and DAG semantics. Remote runners and distributed systems remain intentionally deferred.
