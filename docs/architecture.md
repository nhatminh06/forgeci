# Milestone 5 architecture

```text
HTTP API
    │
    ▼
run manager
    │
    ▼
PostgreSQL store
    │
    ▼
single-run FIFO dispatcher
    │
    ▼
stored pipeline snapshot → parser → validation → DAG
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

The control plane atomically stores each run and all of its jobs before execution. It retains the exact pipeline YAML bytes and their SHA-256 digest. The dispatcher reconstructs the DAG from those stored bytes rather than rereading the submitted path.

Milestone 5 adds a runner protocol layer. Registered runners announce capabilities, send heartbeats, request a lease, and then execute a whole pipeline payload that has already been assigned to them. The control plane still decides which pipeline run is active, but a remote runner is now the execution owner for the leased run. A narrow observer reports live job-state transitions to the manager without introducing SQL into the runner.

The `pipeline` package compiles validated configuration into graph nodes containing sorted dependency and dependent lists. Kahn's algorithm creates a topological order, using lexicographic job names whenever multiple nodes are ready. If not every node can be ordered, compilation reports the jobs involved in a dependency cycle.

The `runner` event loop is the sole owner of runtime state. It scans `graph.Order` to admit ready jobs deterministically, never admits more than `--jobs`, and processes completion events from workers. Completion timing and live logs may vary, but admission decisions and the final summary order remain deterministic.

A `PENDING` job is ready only when every dependency is `PASSED`. A failed or blocked dependency makes the job `BLOCKED`; dependencies that are still pending or running leave it waiting. Independent work remains eligible after another branch fails.

Each admitted worker executes exactly one job. Jobs may overlap, but steps inside a job remain sequential. Workers report `PASSED`, `FAILED`, `CANCELED`, or internal startup failure to the state-owning event loop.

The `executor` owns the job-level routing boundary. Local jobs use `/bin/sh -c` in the invocation directory. Docker jobs use the official Moby client through a narrow internal interface: inspect/pull image, create and start one ordinary labeled container, exec every step sequentially, and force-remove it with an independent bounded cleanup context. `/workspace` is the container working directory and a read-write bind mount of the invocation directory. Docker attach streams are demultiplexed before reaching the shared synchronized writers.

On cancellation, admission stops first, running commands receive the canceled context, and the scheduler waits for their completion. Running and never-started jobs become `CANCELED`; completed terminal states remain unchanged. The CLI retains exit code `2` for interruption.

The scheduler knows only job results and never Docker lifecycle details. Its single `--jobs` capacity applies across local and Docker work. Cancellation stops admission; the Docker executor explicitly kills an active container before cleanup.

Direct `forge run` still enters the parser/compiler/scheduler path directly and imports no runtime database requirement.

## Intentionally absent

Milestone 5 has no multi-run runner scheduling, artifact distribution, persistent log streaming, source snapshot replication, artifact or cache services, SCM triggers, deployment orchestration, authentication beyond a shared runner bearer token, or distributed control-plane coordination. Docker execution remains unsuitable as hostile-code isolation.
