# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 6 adds deterministic source snapshots and isolated run workspaces. Every server-backed submission captures the exact source tree into an immutable content-addressed archive; local servers and empty remote runners materialize and verify that snapshot before execution. Direct `forge run` intentionally remains a live-workspace command.

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
- Deterministic source manifests with logical and archive SHA-256 identities
- Immutable filesystem snapshot storage with deduplication and PostgreSQL metadata
- Authenticated, lease-owned snapshot streaming and runner-side integrity verification
- Isolated per-run local and Docker workspaces with marker-protected cleanup
- FIFO dispatch with live job-state persistence; one local run or one remote run per runner
- Remote runner registration, heartbeat, lease acquisition, and completion reporting
- Persistent runner inventory with `GET /v1/runners`
- Local HTTP API and CLI commands for submit, list, inspect, cancel, and runner listing
- Restart recovery for queued, completed, and interrupted runs
- Stable summaries and process exit codes

## Architecture

```text
HTTP API → control plane → PostgreSQL + snapshot CAS → queued run
    │
    └── runner protocol → lease → download + verify → isolated workspace → executor
```

Parsing, graph compilation, runtime state, and command execution remain separate so the control plane never re-interprets raw YAML. The control plane owns queueing and run lifecycle, while a persistent remote runner owns a leased whole-run execution. See [docs/architecture.md](docs/architecture.md) for the component boundaries.

Snapshot format, safety, and reproducibility rules are documented in [docs/source-snapshots.md](docs/source-snapshots.md).

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
  --snapshot-dir /var/lib/forgeci/snapshots \
  --execution-mode remote \
  --database-url 'postgres://postgres:forgeci@127.0.0.1:5432/forgeci?sslmode=disable' \
  --runner-token-file /path/to/runner-token

./build/forge-runner \
  --server http://127.0.0.1:9090 \
  --workspace-root /var/lib/forgeci/runner-workspaces
```

Set `FORGECI_RUNNER_TOKEN` for the runner. Plain HTTP is accepted only on a loopback runner listener; non-loopback listeners require `--runner-tls-cert` and `--runner-tls-key`, and runners can trust a private CA with `--ca-cert`.

Direct `forge run` executes from the directory where it was invoked. Server-backed runs execute a captured source snapshot in a unique workspace, even if the original source changes after submission.

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

## What Milestone 6 demonstrates

Pipeline identity and source identity are independent. The stored YAML digest identifies the execution definition, while the canonical source digest identifies files, directories, symlinks, modes, and file content. A second blob digest protects the exact compressed transport. Runners verify both before executing.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Local commands run with the invoking user's privileges. Docker execution adds process/filesystem isolation but bind-mounts the repository read-write, and access to the Docker daemon is itself privileged. It is not a sandbox for hostile repositories.

The HTTP API remains loopback-only for the control-plane surface, and the runner listener uses one shared bearer token. Source capture is not filesystem-atomic. `.git` is excluded; Git commit identity, SCM checkout, custom ignore rules, xattrs, and ACLs are not supported. Snapshot isolation does not provide hostile multi-tenant sandboxing: workspaces are writable, Docker daemon access is privileged, and secret isolation is absent.

## Roadmap

Durable artifacts, caching, cross-runner job scheduling, retries, persistent logs, per-runner credentials, Kubernetes, multi-tenancy/RBAC, and high availability remain deferred.
