# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 1 implements only the local pipeline engine. It reads a repository-owned YAML file, validates it, compiles dependencies into a directed acyclic graph (DAG), and runs jobs on the current machine. Current execution is local and sequential.

## Current capabilities

- Strict `forge.yaml` parsing and validation
- Deterministic dependency ordering and cycle rejection
- Sequential local shell execution with live stdout and stderr
- Failed-job propagation while independent jobs continue
- Stable summaries and process exit codes

## Architecture

```text
forge.yaml → parser → validation → DAG compiler → runner → local executor
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
```

ForgeCI executes commands from the directory where it was invoked, even when `--file` names a file in another directory.

## CLI

```text
forge --help
forge run --help
forge run
forge run --file <path>
```

Exit code `0` means success, `1` means at least one job failed or was blocked, and `2` means a CLI, configuration, compilation, or interruption error.

## What Milestone 1 demonstrates

This milestone demonstrates the mechanics beneath a CI pipeline: strict configuration boundaries, deterministic topological ordering, explicit job states, shell exit-code handling, and dependency failure propagation. The example pipeline lets ForgeCI locally execute ForgeCI's own formatting, tests, and build; this is preparation, not full self-hosting.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Commands run with the privileges of the user invoking ForgeCI. There is no sandbox, container isolation, secret isolation, multi-tenant isolation, or remote execution isolation.

Milestone 1 has no parallel scheduling, remote workers, persistent database, artifacts, cache, SCM integration, deployment system, web UI, or authentication. It is not yet a production-safe runner.

## Roadmap

The next logical milestone is a bounded parallel local scheduler that preserves dependency correctness, cancellation, failure propagation, and deterministic observable behavior. Later platform capabilities remain intentionally deferred.
