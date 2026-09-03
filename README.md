# ForgeCI

ForgeCI is a self-hosted CI/CD platform built from first principles to explore how pipeline engines, schedulers, runners, build systems, artifact stores, software-supply-chain controls, and deployment systems work internally.

Milestone 8 adds job-level remote scheduling. The control plane durably owns DAG readiness and leases individual jobs across a runner fleet; immutable source snapshots and explicit artifacts make placement independent of runner filesystem state.

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
- Named file and directory artifacts with independent logical and blob SHA-256 identities
- Immutable artifact CAS, transactional PostgreSQL metadata, retention, and reference-aware GC
- Authenticated lease-owned artifact transfer and verified downstream materialization
- Isolated per-run local and Docker workspaces with marker-protected cleanup
- Deterministic ready-job dispatch with durable dependency and job-state persistence
- Independent runner and per-run concurrency limits with Docker capability matching
- Remote runner registration, multi-job heartbeat, job lease acquisition, and completion reporting
- Persistent runner inventory with `GET /v1/runners`
- Local HTTP API and CLI commands for runs, runners, and artifact listing/download
- Restart recovery for queued, completed, and interrupted runs
- Stable summaries and process exit codes

## Architecture

```text
HTTP API → control-plane DAG scheduler → PostgreSQL + snapshot CAS + artifact CAS
    │
    └── protocol v2 → ready job lease → fresh workspace → local/Docker executor
```

Parsing, graph compilation, runtime state, and command execution remain separate. In remote mode the control plane owns the durable DAG and readiness; each runner owns only its concurrently active leased jobs. See [docs/architecture.md](docs/architecture.md) and [docs/job-scheduling.md](docs/job-scheduling.md).

Snapshot rules are documented in [docs/source-snapshots.md](docs/source-snapshots.md); artifact lifecycle, integrity, retention, and security are in [docs/artifacts.md](docs/artifacts.md).

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
  --artifact-dir /var/lib/forgeci/artifacts \
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
forge artifacts <run-id>
forge artifact download <run-id> <job> <name> --output <path>
```

Exit code `0` means success, `1` means at least one job failed or was blocked, and `2` means a CLI, configuration, compilation, or interruption error.

`--jobs` must be greater than zero and defaults to `1`, preserving sequential Milestone 1 behavior.

## What Milestone 8 demonstrates

Jobs from one run can execute across three or more runners, including local and Docker executors, without sharing a mutable workspace. Durable edges govern readiness, job leases provide fencing, and explicit artifacts are the only upstream-output transfer mechanism. Runner capacity and run capacity are enforced independently by PostgreSQL claim transactions.

## Current limitations and security model

ForgeCI assumes a trusted local repository, pipeline definition, and execution environment. Local commands run with the invoking user's privileges. Docker execution adds process/filesystem isolation but bind-mounts the repository read-write, and access to the Docker daemon is itself privileged. It is not a sandbox for hostile repositories.

The HTTP API remains loopback-only for the control-plane surface, and the runner listener uses one shared bearer token. Source capture is not filesystem-atomic. `.git` is excluded; Git commit identity, SCM checkout, custom ignore rules, xattrs, and ACLs are not supported. Snapshot isolation does not provide hostile multi-tenant sandboxing: workspaces are writable, Docker daemon access is privileged, and secret isolation is absent.

## Roadmap

Build caching, automatic retries or reassignment, labels/selectors, GPUs, persistent logs, per-runner credentials, Kubernetes/autoscaling, multi-tenancy/RBAC, and high availability remain deferred.
