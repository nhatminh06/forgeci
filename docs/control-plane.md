# Persistent control plane and remote runners

Milestone 5 adds a single-user control plane backed by PostgreSQL with a remote-runner protocol layered on top. The HTTP API accepts runs through a loopback-only server and persists each run and its jobs atomically. Registered remote runners authenticate using a bearer token, heartbeat, acquire a whole-run lease, and execute the leased pipeline payload in their local workspace. Local mode retains one active pipeline; remote mode permits one active run per runner.

## Start PostgreSQL and the server

```bash
docker run --rm -d --name forgeci-postgres \
  -e POSTGRES_PASSWORD=forgeci \
  -e POSTGRES_DB=forgeci \
  -p 127.0.0.1:5432:5432 postgres:17-alpine

go build -o build/forge-server ./cmd/forge-server
./build/forge-server \
  --listen 127.0.0.1:8080 \
  --workspace "$(pwd)" \
  --database-url 'postgres://postgres:forgeci@127.0.0.1:5432/forgeci?sslmode=disable'
```

`--database-url` takes precedence over `DATABASE_URL`. The server rejects non-loopback listen addresses because this milestone has no authentication or TLS.

## API and CLI

```bash
curl http://127.0.0.1:8080/healthz
./build/forge submit --server http://127.0.0.1:8080 --file forge.example.yaml --jobs 3
./build/forge runs --server http://127.0.0.1:8080 --limit 10
./build/forge runners --server http://127.0.0.1:8080
./build/forge inspect <run-id> --server http://127.0.0.1:8080
./build/forge cancel <run-id> --server http://127.0.0.1:8080
```

The API surface includes `GET /healthz`, `POST /v1/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `POST /v1/runs/{id}/cancel`, and `GET /v1/runners`. The runner protocol is exposed on the separate runner listener, with bearer-authenticated endpoints for registration, heartbeats, lease requests, job events, and completion. JSON requests reject unknown fields and multiple documents.

## Persistence model

Runs use UUID identifiers and statuses `QUEUED`, `RUNNING`, `PASSED`, `FAILED`, `CANCELED`, `ERROR`, and `ABORTED`. Job rows use `PENDING`, `RUNNING`, `PASSED`, `FAILED`, `BLOCKED`, `CANCELED`, and `ABORTED`.

Run creation stores the pipeline path, exact YAML bytes, SHA-256 digest, canonical workspace, parallelism, timestamps, cancellation request, and safe error summary. Every job is inserted in the same transaction with its name, image, status, timestamps, and error summary. The embedded version-1 migration is recorded in `schema_migrations` and applied transactionally.

The YAML snapshot prevents a queued run from changing when its pipeline file is edited. Source files in the workspace are not snapshotted and may still change while a run waits.

## Queue, cancellation, and recovery

Runs are claimed FIFO by creation time and UUID tie-breaker using a transaction and row lock. Local mode has one active pipeline. Remote mode leases one run to each available compatible runner and records the runner, lease ID, generation, expiration, and effective parallelism.

Queued cancellation atomically cancels the run and all pending jobs. Running cancellation records the request and cancels the active scheduler context. Canceling a terminal run returns a conflict. Runner leases may also be renewed by heartbeat; stale or expired leases are rejected and marked ABORTED.

On startup, stale `RUNNING` runs and unfinished jobs become `ABORTED`; queued work remains eligible for dispatch; terminal history remains unchanged. Graceful shutdown stops HTTP acceptance and cancels active execution. Logs remain process-local and are not durable.

## Security and limitations

This is trusted, single-user infrastructure. The main API remains loopback-only and the runner protocol requires a shared bearer token. It must not be exposed to a LAN, the Internet, or shared untrusted users. One configured workspace is accepted; submitted paths must be relative, remain within it after symlink resolution, and pass strict pipeline validation.

Docker jobs retain the writable workspace mount and other Milestone 3 limitations. There are no source snapshots, persistent logs, artifacts, cache, SCM triggers, multi-tenancy, RBAC, UI, per-runner certificates, token rotation API, or high-availability coordination.
