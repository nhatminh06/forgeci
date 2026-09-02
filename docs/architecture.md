# Milestone 6 architecture

```text
HTTP API
    │
    ▼
run manager
    │
    ▼
PostgreSQL store + filesystem snapshot CAS
    │
    ▼
single-run FIFO dispatcher
    │
    ▼
stored pipeline YAML + verified source snapshot → parser → validation → DAG
    │
    ▼
bounded scheduler
    │
    ├── ready job ──► local job executor
    ├── ready job ──► Docker job executor
    └── ready job ──► waits for capacity
    │
    ▼
completion events and state updates
```

Before creating a run, the control plane captures a canonical source manifest, writes a deterministic `tar-gzip-v1` blob to temporary storage, verifies it, and publishes it atomically by source digest. One transaction upserts snapshot metadata and inserts the run and jobs. The pipeline YAML digest and source digest remain separate identities.

Remote leases include snapshot identity, blob identity, format, size, and entry metadata. The owning runner downloads through its authenticated live lease, verifies the blob while streaming to a temporary file, extracts without following archive-controlled symlinks, recomputes the logical manifest, and only then executes. Local server mode uses the same materialization invariant.

The `pipeline` package compiles validated configuration into graph nodes containing sorted dependency and dependent lists. Kahn's algorithm creates a topological order, using lexicographic job names whenever multiple nodes are ready. If not every node can be ordered, compilation reports the jobs involved in a dependency cycle.

The `runner` event loop is the sole owner of runtime state. It scans `graph.Order` to admit ready jobs deterministically, never admits more than `--jobs`, and processes completion events from workers. Completion timing and live logs may vary, but admission decisions and the final summary order remain deterministic.

A `PENDING` job is ready only when every dependency is `PASSED`. A failed or blocked dependency makes the job `BLOCKED`; dependencies that are still pending or running leave it waiting. Independent work remains eligible after another branch fails.

Each admitted worker executes exactly one job. Jobs may overlap, but steps inside a job remain sequential. Workers report `PASSED`, `FAILED`, `CANCELED`, or internal startup failure to the state-owning event loop.

The `executor` owns the job-level routing boundary. In server-backed mode, local jobs use the isolated materialized source directory and Docker bind-mounts that directory read-write at `/workspace`. Direct mode continues to use the invocation directory.

On cancellation, admission stops first, running commands receive the canceled context, and the scheduler waits for their completion. Running and never-started jobs become `CANCELED`; completed terminal states remain unchanged. The CLI retains exit code `2` for interruption.

The scheduler knows only job results and never Docker lifecycle details. Its single `--jobs` capacity applies across local and Docker work. Cancellation stops admission; the Docker executor explicitly kills an active container before cleanup.

Direct `forge run` still enters the parser/compiler/scheduler path directly and imports no runtime database requirement.

Each runner workspace is keyed by run and lease, uses restrictive permissions, and carries an ownership marker outside the extracted source directory. Cleanup verifies root containment, path shape, and marker identity before recursive deletion. Startup removes only stale marked workspaces and preserves unrelated directories.

## Intentionally absent

Milestone 6 has no artifact or cache services, Git/SCM checkout, cross-runner jobs, retries, persistent logs, Kubernetes, secrets, RBAC, or distributed control-plane coordination. Docker execution remains unsuitable as hostile-code isolation.
