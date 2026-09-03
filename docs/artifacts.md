# Durable job artifacts

Artifacts are explicit named outputs from a job to its direct downstream jobs. A source snapshot freezes submission input; an artifact publishes output created during execution.

```yaml
version: 1
jobs:
  build:
    steps:
      - run: mkdir -p dist && printf 'hello\n' > dist/app
    artifacts:
      upload:
        - name: application
          path: dist/app
  test:
    needs: [build]
    artifacts:
      download:
        - from: build
          name: application
          into: inputs
    steps:
      - run: test -f inputs/app
```

Each name contains one exact file or directory. Names match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. Paths and destinations are normalized relative paths. Absolute paths, traversal, control bytes, duplicates, conflicting final destinations, undeclared producers, and producers absent from `needs` are rejected before execution. Globs, multiple paths per name, conditional publication, and pipeline-configured retention are not implemented.

## Publication and identity

After every producer step passes, ForgeCI captures and verifies the complete declared set before uploading it. Missing roots, root symlinks, escaping symlinks, FIFOs, sockets, devices, and limit violations fail the job. Blob, transport, filesystem, and database failures instead make the run `ERROR`.

The deterministic `artifact-tar-gzip-v1` archive has one root: the upload path basename. A canonical manifest records sorted paths, node types, permission modes, file sizes and SHA-256 values, and relative symlink targets. Host paths, owner IDs/names, and timestamps are excluded. `content_sha256` hashes the logical manifest; `blob_sha256` hashes exact compressed bytes. Equivalent trees therefore share both identities and one immutable CAS blob.

All blobs are present before one PostgreSQL transaction inserts the complete `(run, producer job, name)` set. Exact commit replay is a no-op; conflicting replay is rejected. PostgreSQL rejects `PASSED` for a job whose declared set is absent, so committed metadata precedes the durable passed state.

## Restore and security

Downloads occur after a consumer becomes running and before its first step. Even in the currently shared run workspace, a runner fetches the committed CAS object through the control plane. It streams into a temporary file, verifies size and blob SHA-256, extracts into a temporary directory with entry and byte limits, rebuilds the canonical manifest, verifies the logical digest, and renames the single root into `<workspace>/<into>/<root-name>`.

Extraction rejects absolute, non-canonical, or traversing paths, hard links, special nodes, unsafe symlinks, and symlink parents. An existing final root is never overwritten. Temporary capture, upload, download, and extraction data is removed after success, failure, or cancellation.

Runner routes require the bearer token plus exact runner ID, run ID, lease ID, generation, unexpired lease, and running job ownership. Downloads require a passed producer in the same run. A token alone cannot fetch another run's object, and stale leases cannot upload, commit, or download.

## Storage, retention, and GC

`forge-server --artifact-dir` selects a filesystem CAS separate from source snapshots. `--artifact-retention` defaults to 168 hours. Artifacts have no expiry while a run is active; terminal transition sets expiry from `finished_at`. Outputs from later-failed, canceled, error, or aborted runs remain available until then.

GC marks expired metadata unavailable and returns `410 Gone` for its download. A blob is deleted only when no live artifact references it. Recent unreferenced uploads receive `--artifact-orphan-grace`; `--artifact-gc-interval` controls sweeps. Stale transfer temporaries use the same conservative grace.

```bash
forge artifacts <run-id> --server http://127.0.0.1:8080
forge artifact download <run-id> build application \
  --output application.tar.gz --server http://127.0.0.1:8080
```

The API routes are `GET /v1/runs/{id}/artifacts` and `GET /v1/runs/{id}/artifacts/{job}/{name}`. Historical downloads return raw archives and are not automatically extracted.

Direct `forge run` uses the same capture, verification, and restore semantics in a temporary local CAS, without PostgreSQL or a server. The CAS is discarded at command exit. Server-backed artifacts survive runner and server restarts.

All jobs in a run are still pinned to one runner and share one isolated run workspace. Explicit artifacts prove durable output transfer but do not prohibit incidental communication through that workspace. Artifacts are immutable named outputs, not a build cache: there are no cache keys, lookup policy, mutable replacement, or eviction-based reuse.
