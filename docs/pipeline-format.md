# Pipeline format

Milestone 3 extends version 1 with an optional per-job `image`. The default file is `forge.yaml`; `forge run --file <path>` selects another file. Concurrency remains a CLI concern.

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

## Schema

`version` is required and must be the integer `1`.

`jobs` is required and must be a non-empty mapping. Job names are identifiers matching `[A-Za-z0-9][A-Za-z0-9_-]*`.

Each job has optional `needs` and `image` fields and a required, non-empty `steps` list. `image` must be a non-empty string with no leading, trailing, embedded whitespace, or control characters. Docker performs full image-reference validation. Every dependency must name another job, cannot name the job itself, and cannot be repeated. The dependency graph must be acyclic.

Each step has exactly one supported field, `run`, whose value must be a non-empty string. Unknown fields and malformed structural types are rejected.

Local, Docker, and mixed jobs use the same DAG:

```yaml
jobs:
  local:
    steps:
      - run: make test
  build:
    image: golang:1.27
    steps:
      - run: go build -o output ./...
  verify:
    needs: [build]
    steps:
      - run: test -f output
```

## Execution behavior

Jobs may declare named outputs with `artifacts.upload` (`name`, `path`) and inputs with `artifacts.download` (`from`, `name`, `into`). A producer must be a direct `needs` dependency and declare the requested name. Upload follows successful steps and precedes `PASSED`; verified restore precedes consumer steps. See [artifacts.md](artifacts.md).

`forge run` defaults to one active job. `forge run --jobs N` permits at most `N` ready jobs to overlap. Ready jobs are admitted in deterministic graph order; their completion and live output order may vary. Steps within each job always run sequentially in declaration order through `/bin/sh -c`.

Jobs without `image` execute locally from the invocation directory. Jobs with `image` execute in one container per job; all steps share its state and run in order through `/bin/sh -c` from `/workspace`. The invocation directory is bind-mounted read-write at `/workspace`. Standard output and standard error stream live and separately.

A non-zero step stops the remaining steps in that job and marks it `FAILED`. Direct and transitive dependents become `BLOCKED`; independent jobs still run. Cancellation stops new admission, cancels active commands, waits for admitted workers, and marks affected jobs `CANCELED`. Final summaries remain in deterministic graph order.

The process exits `0` for a successful pipeline, `1` for a pipeline with failed or blocked jobs, and `2` for invalid CLI usage (including an invalid `--jobs` limit), invalid configuration, graph compilation or process-start errors, or interruption.

## Security model

The pipeline and repository must be trusted. Docker jobs cannot configure privileged mode, host networking/PID/IPC, devices, added capabilities, or a Docker socket mount, but the repository is writable from the container and Docker daemon access is privileged. Containers use the runner host UID/GID so outputs remain publishable and cleanable; default networking remains enabled. ForgeCI does not add seccomp/AppArmor policy management, secret isolation, resource quotas, or hostile multi-tenant isolation. This is not safe isolation for arbitrary untrusted code.
