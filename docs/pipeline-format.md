# Pipeline format

Milestone 1 reads one local YAML file. The default is `forge.yaml`; `forge run --file <path>` selects another file.

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

Jobs run sequentially in a deterministic topological order. When multiple jobs are eligible, job name lexicographic order breaks the tie. Steps within a job run in declaration order through `/bin/sh -c`.

Commands run from the directory where ForgeCI was invoked. The location selected by `--file` does not change the command working directory. Standard output and standard error are shown while each command runs.

A non-zero step stops the remaining steps in that job and marks it `FAILED`. Direct and transitive dependents become `BLOCKED`; independent jobs still run. The final summary uses the explicit states `PASSED`, `FAILED`, and `BLOCKED`.

The process exits `0` for a successful pipeline, `1` for a pipeline with failed or blocked jobs, and `2` for invalid CLI usage, invalid configuration, graph compilation errors, or interruption.

## Security model

The pipeline and repository must be trusted. Commands have the same host privileges as the user running ForgeCI. Milestone 1 provides no sandbox, container isolation, secret isolation, multi-tenant isolation, or remote execution isolation.

## Future work

Parallel scheduling and isolated execution are future milestones and are not part of this format.
