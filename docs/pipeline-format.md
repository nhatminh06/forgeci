# Pipeline format

Milestone 2 reads the same version 1 YAML format. The default is `forge.yaml`; `forge run --file <path>` selects another file. Concurrency is a CLI concern and does not add pipeline fields.

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

## Schema

`version` is required and must be the integer `1`.

`jobs` is required and must be a non-empty mapping. Job names are identifiers matching `[A-Za-z0-9][A-Za-z0-9_-]*`.

Each job has an optional `needs` list and a required, non-empty `steps` list. Every dependency must name another job, cannot name the job itself, and cannot be repeated. The dependency graph must be acyclic.

Each step has exactly one supported field, `run`, whose value must be a non-empty string. Unknown fields and malformed structural types are rejected.

## Execution behavior

`forge run` defaults to one active job. `forge run --jobs N` permits at most `N` ready jobs to overlap. Ready jobs are admitted in deterministic graph order; their completion and live output order may vary. Steps within each job always run sequentially in declaration order through `/bin/sh -c`.

Commands run from the directory where ForgeCI was invoked. The location selected by `--file` does not change the command working directory. Standard output and standard error are shown while each command runs.

A non-zero step stops the remaining steps in that job and marks it `FAILED`. Direct and transitive dependents become `BLOCKED`; independent jobs still run. Cancellation stops new admission, cancels active commands, waits for admitted workers, and marks affected jobs `CANCELED`. Final summaries remain in deterministic graph order.

The process exits `0` for a successful pipeline, `1` for a pipeline with failed or blocked jobs, and `2` for invalid CLI usage (including an invalid `--jobs` limit), invalid configuration, graph compilation or process-start errors, or interruption.

## Security model

The pipeline and repository must be trusted. Commands have the same host privileges as the user running ForgeCI. Milestone 1 provides no sandbox, container isolation, secret isolation, multi-tenant isolation, or remote execution isolation.

## Future work

Container isolation is a future milestone and is not part of this format.
