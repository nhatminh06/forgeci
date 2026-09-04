# Persistent control plane

Every `forge submit` is reproducible with respect to both its pipeline definition and source tree. The server reads and validates the requested YAML, captures the configured workspace, publishes an immutable snapshot blob, then transactionally stores snapshot metadata, the run, its jobs, and dependency edges. A run is never queued when capture or publication fails.

## Start PostgreSQL and the server

```bash
docker run --rm -d --name forgeci-postgres \
  -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci \
  -p 127.0.0.1:5432:5432 postgres:17-alpine

go build -o build/forge-server ./cmd/forge-server
./build/forge-server \
  --listen 127.0.0.1:8080 \
  --workspace "$(pwd)" \
  --snapshot-dir /var/lib/forgeci/snapshots \
  --artifact-dir /var/lib/forgeci/artifacts \
  --database-url 'postgres://postgres:forgeci@127.0.0.1:5432/forgeci?sslmode=disable'
```

The snapshot directory is required and must be outside the source workspace. Defaults limit capture to 100,000 entries, 1 GiB of logical file data, and a 512 MiB compressed archive; `--snapshot-max-entries`, `--snapshot-max-logical-bytes`, and `--snapshot-max-archive-bytes` configure these server-wide bounds.

## Persistence and identities

`pipeline_sha256` identifies the exact stored YAML. `source_snapshot_sha256` identifies the canonical logical source manifest. `source_snapshots` stores that source digest, the exact archive blob digest, format, sizes, entry count, and creation time. Archives live only in the filesystem content-addressed store, never PostgreSQL. Historical pre-Milestone-6 runs may have a null source reference; every new server-backed run must have one.

```bash
./build/forge submit --server http://127.0.0.1:8080 --file forge.yaml
./build/forge inspect <run-id> --server http://127.0.0.1:8080
```

Inspection prints both pipeline and source digests. Local execution materializes the immutable snapshot below the server snapshot store, executes local and Docker jobs there, and removes the workspace at every terminal outcome. Editing or deleting source after submission cannot affect that run.

Direct `forge run` uses the live invocation workspace and needs neither PostgreSQL nor server storage. Artifact-enabled direct runs use an ephemeral local CAS deleted at process exit; server-backed artifacts persist in the configured artifact CAS and PostgreSQL.

## Cache, logs, and automation

Pipelines restore and save explicit cache keys. Job stdout and stderr are
persisted as durable PostgreSQL chunks and retrieved with forge logs; forge
wait polls a submitted run to its terminal result. The self-hosting harness
uses this production control plane with PostgreSQL and two remote runners.

## Recovery and limitations

Snapshot and artifact blobs and metadata survive server restart. In remote mode, startup preserves queued jobs but conservatively marks uncertain running job leases `ABORTED`, blocks their descendants, and leaves independent pending jobs schedulable. The server never guesses that an old worker stopped, and it does not automatically retry or reassign lost work.

Startup and GC remove stale temporary data after conservative grace periods.
Capture observes exact bytes read but is not a filesystem-atomic point-in-time
operation; obvious mutation during capture fails. `.git` is excluded, while
untracked files and other source content are included. There are no custom
ignores, Git commit identities, SCM checkout, xattrs, or ACL preservation.
See [artifacts.md](artifacts.md), [job-scheduling.md](job-scheduling.md), and
[self-hosting.md](self-hosting.md).
