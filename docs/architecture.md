# Milestone 2 architecture

```text
forge.yaml
    │
    ▼
parser
    │
    ▼
validation
    │
    ▼
compiler
    │
    ▼
DAG
    │
    ▼
bounded scheduler
    │
    ├── ready job ──► local executor
    ├── ready job ──► local executor
    └── ready job ──► waits for capacity
    │
    ▼
completion events and state updates
```

The `config` package strictly decodes YAML into the Milestone 1 schema and validates job names, steps, and dependency references. Unknown fields are rejected.

The `pipeline` package compiles validated configuration into graph nodes containing sorted dependency and dependent lists. Kahn's algorithm creates a topological order, using lexicographic job names whenever multiple nodes are ready. If not every node can be ordered, compilation reports the jobs involved in a dependency cycle.

The `runner` event loop is the sole owner of runtime state. It scans `graph.Order` to admit ready jobs deterministically, never admits more than `--jobs`, and processes completion events from workers. Completion timing and live logs may vary, but admission decisions and the final summary order remain deterministic.

A `PENDING` job is ready only when every dependency is `PASSED`. A failed or blocked dependency makes the job `BLOCKED`; dependencies that are still pending or running leave it waiting. Independent work remains eligible after another branch fails.

Each admitted worker executes exactly one job. Jobs may overlap, but steps inside a job remain sequential. Workers report `PASSED`, `FAILED`, `CANCELED`, or internal startup failure to the state-owning event loop.

The `executor` runs each trusted command through `/bin/sh -c` in the invocation directory. A shared synchronized writer preserves live stdout and stderr without concurrent writer races.

On cancellation, admission stops first, running commands receive the canceled context, and the scheduler waits for their completion. Running and never-started jobs become `CANCELED`; completed terminal states remain unchanged. The CLI retains exit code `2` for interruption.

## Intentionally absent

Milestone 2 has no remote workers, Docker executor, persistent database, artifacts, cache, SCM integrations, deployments, or control plane.
