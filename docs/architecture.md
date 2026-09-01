# Milestone 1 architecture

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
runner
    │
    ▼
local executor
```

The `config` package strictly decodes YAML into the Milestone 1 schema and validates job names, steps, and dependency references. Unknown fields are rejected.

The `pipeline` package compiles validated configuration into graph nodes containing sorted dependency and dependent lists. Kahn's algorithm creates a topological order, using lexicographic job names whenever multiple nodes are ready. If not every node can be ordered, compilation reports the jobs involved in a dependency cycle.

The `runner` owns runtime state. It walks the compiled order once, moves eligible jobs through `PENDING`, `RUNNING`, and `PASSED` or `FAILED`, and marks jobs with unsuccessful dependencies `BLOCKED`. Independent work remains eligible after another branch fails.

The `executor` runs each trusted command through `/bin/sh -c` in the invocation directory. It connects child stdout and stderr directly to ForgeCI's streams and reports process exit status without treating ordinary command failure as a panic or configuration error.

The `cli` joins these stages, handles the pipeline filename and signals, prints deterministic output, and maps results to stable process exit codes.

## Intentionally absent

Milestone 1 has no remote workers, Docker executor, concurrent scheduler, persistent database, artifacts, cache, SCM integrations, deployments, or control plane. Those systems are deferred until the local execution semantics are established.
