# Remote runners, source transfer, and run leases

In remote mode, `forge-server` queues a whole pipeline for one registered runner. The runner root starts empty; repository pre-provisioning is neither required nor used. A lease contains stored pipeline YAML plus source digest, archive blob digest, archive format, compressed and logical sizes, and entry count.

```bash
mkdir -p /var/lib/forgeci/runner-workspaces
FORGECI_RUNNER_TOKEN=... ./build/forge-runner \
  --server http://127.0.0.1:9090 \
  --workspace-root /var/lib/forgeci/runner-workspaces \
  --state-dir /var/lib/forgeci/runner-state \
  --name runner-1
```

`--workspace` remains a deprecated alias for `--workspace-root`. The workspace root is canonicalized, created with restrictive permissions, and must not contain or be contained by the runner state directory.

Runner-side bounds default to 512 MiB compressed, 1 GiB extracted, and 100,000 entries. They can be reduced with `--snapshot-max-archive-bytes`, `--snapshot-max-logical-bytes`, and `--snapshot-max-entries`.

## Download and verification

The runner requests `GET /v1/runner/leases/{lease-id}/source` with its runner ID, run ID, and generation. The shared bearer token is necessary but insufficient: the server also requires the exact current owner, live run, matching lease/generation, and unexpired deadline. There is no generic public digest endpoint.

Archive bytes stream to a temporary file while the runner enforces the expected size, its own maximum, and the blob SHA-256. A mismatch prevents extraction and reports `ERROR`. Extraction rejects absolute, empty, control-byte, non-canonical, and traversal paths; hard links and special entries are unsupported. Regular files are created only through verified directory parents. Validated relative symlinks are created last, preventing an archive symlink from redirecting earlier writes. Entry and extracted-byte limits bound decompression.

After extraction the runner rebuilds the canonical source manifest. Execution begins only when its logical digest equals the leased source digest.

## Isolated workspaces and cleanup

Each lease executes below:

```text
<workspace-root>/runs/<run-id>/<lease-id>/
  .forgeci-workspace.json
  source/
```

Local jobs run in `source/`; Docker jobs mount it read-write at `/workspace`. Passed, failed, canceled, error, shutdown, and lease-loss paths remove temporary downloads and the isolated workspace. Deletion fails closed unless the path is beneath the configured root, matches the run/lease shape, and its ownership marker matches. On restart, the runner removes stale marked workspaces left by a crash and preserves unmarked user directories.

## Lease and security model

Heartbeats renew only the exact current lease. Expired, replaced, wrong-runner, and stale ownership tuples cannot download source, report events, or complete a run. Runner loss remains conservative: the run becomes `ABORTED` and is not automatically reassigned.

The shared bearer token is still not per-runner cryptographic identity. Pipelines and sources are trusted; workspaces are writable; Docker daemon access is privileged; and ForgeCI provides no hostile multi-tenant sandbox, secret isolation, artifact transfer, cache, job-level leases, retries, or cross-runner job scheduling.
