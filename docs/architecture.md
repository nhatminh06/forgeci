# ForgeCI architecture

```text
HTTP API
    │
    ▼
run manager + durable DAG scheduler
    │
    ▼
PostgreSQL store + filesystem snapshot CAS + filesystem artifact CAS
    │
    ▼
ready-job claim transactions
    │
    ▼
persisted dependency edges + job state
    │
    ▼
combined capacity checks
    │
    ├── job lease ──► runner A local executor
    ├── job lease ──► runner B Docker executor
    └── ready job ──► waits for runner/run capacity
    │
    ▼
completion events and state updates
```

Before creating a run, the control plane captures a canonical source manifest, writes a deterministic `tar-gzip-v1` blob to temporary storage, verifies it, and publishes it atomically by source digest. One transaction upserts snapshot metadata and inserts the run and jobs. The pipeline YAML digest and source digest remain separate identities.

Remote leases include snapshot identity, blob identity, format, size, and entry metadata. The owning runner downloads through its authenticated live lease, verifies the blob while streaming to a temporary file, extracts without following archive-controlled symlinks, recomputes the logical manifest, and only then executes. Local server mode uses the same materialization invariant.

The `pipeline` package compiles validated configuration into graph nodes containing sorted dependency and dependent lists. Kahn's algorithm creates a topological order, using lexicographic job names whenever multiple nodes are ready. If not every node can be ordered, compilation reports the jobs involved in a dependency cycle.

Direct mode retains the in-process `runner` event loop. Remote mode persists dependency edges and lets PostgreSQL transactions select globally ready jobs deterministically. Claims enforce the registered runner's capacity, the run's global parallel limit, dependency readiness, and Docker capability before assigning a fenced job lease.

A `PENDING` job is ready only when every dependency is `PASSED`. A failed or blocked dependency makes the job `BLOCKED`; dependencies that are still pending or running leave it waiting. Independent work remains eligible after another branch fails.

Each admitted worker executes exactly one job. Jobs may overlap, but steps inside a job remain sequential. Workers report `PASSED`, `FAILED`, `CANCELED`, or internal startup failure to the state-owning event loop.

The `executor` owns the job-level routing boundary. In server-backed mode, local jobs use the isolated materialized source directory and Docker bind-mounts that directory read-write at `/workspace`. Direct mode continues to use the invocation directory.

On cancellation, admission stops first, running commands receive the canceled context, and the scheduler waits for their completion. Running and never-started jobs become `CANCELED`; completed terminal states remain unchanged. The CLI retains exit code `2` for interruption.

The scheduler knows only job results and never Docker lifecycle details. Its single `--jobs` capacity applies across local and Docker work. Cancellation stops admission; the Docker executor explicitly kills an active container before cleanup.

Direct `forge run` still enters the parser/compiler/scheduler path directly and imports no runtime database requirement.

Each remote workspace is keyed by run, job, and lease, uses restrictive permissions, and carries an ownership marker outside the extracted source directory. Cleanup verifies root containment, path shape, and marker identity before recursive deletion. Startup removes only stale marked workspaces and preserves unrelated directories.

Successful producer steps are followed by deterministic artifact capture, blob upload, and one transactional metadata-set commit before `PASSED`. A consumer downloads only explicitly declared upstream artifacts through durable storage, verifies blob and logical identity, and atomically materializes them before steps. Snapshot and artifact stores remain semantically separate.

## Current distributed flow

forge submits a pipeline and immutable source snapshot. forge-server persists
the DAG, job states, leases, artifact/cache metadata, and chunked logs in
PostgreSQL. Runners restore source, declared artifacts and caches, execute
local or Docker jobs, upload logs, publish artifacts/caches, and complete the
lease. Source, artifact, and cache CAS stores are distinct.

M11 uses GitHub Actions only as bootstrap infrastructure. The harness starts
PostgreSQL, forge-server, and two production runners; ForgeCI owns the project
CI DAG. See self-hosting.md.

## Intentionally absent

ForgeCI has no native Git/SCM checkout, automatic retry or reassignment, runner
labels/selectors, GPU scheduling, Kubernetes/autoscaling, secrets, RBAC, or
high-availability control plane. Docker execution remains unsuitable as
hostile-code isolation.
