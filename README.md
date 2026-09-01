# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 3 adds per-job Docker execution to the bounded parallel scheduler. Jobs without an `image` remain local; jobs with an `image` run all their steps sequentially in one container.

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
- Stable summaries and process exit codes

## Architecture

```text
forge.yaml → parser → validation → DAG compiler → bounded scheduler → local or Docker job executor
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

## What Milestone 3 demonstrates

This milestone preserves the scheduler's single runtime-state owner while routing each admitted job to one environment. A Docker job inspects or pulls its image, creates and starts one labeled container, runs every step with `/bin/sh -c` in `/workspace`, and force-removes the container on success, failure, internal error, or cancellation.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Local commands run with the invoking user's privileges. Docker execution adds process/filesystem isolation but bind-mounts the repository read-write, and access to the Docker daemon is itself privileged. It is not a sandbox for hostile repositories.

ForgeCI has no secret isolation, multi-tenant isolation, remote workers, persistent database, artifacts, cache, SCM-trigger integration, deployment system, web UI, or authentication. It is not yet a production-safe runner.

## Roadmap

Remote runners and distributed systems remain intentionally deferred.
