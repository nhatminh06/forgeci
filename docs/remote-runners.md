# Remote runners, source transfer, and job leases

In remote mode, `forge-server` schedules ready jobs across registered runners. The runner root starts empty; repository pre-provisioning is neither required nor used. A lease contains exactly one compiled job plus source digest, archive blob digest, archive format, compressed and logical sizes, and entry count.

```bash
mkdir -p /var/lib/forgeci/runner-workspaces
FORGECI_RUNNER_TOKEN=... ./build/forge-runner \
  --server http://127.0.0.1:9090 \
  --workspace-root /var/lib/forgeci/runner-workspaces \
  --state-dir /var/lib/forgeci/runner-state \
  --name runner-1
```

`--workspace` remains a deprecated alias for `--workspace-root`. The workspace root is canonicalized, created with restrictive permissions, and must not contain or be contained by the runner state directory.

Runner-side snapshot and artifact bounds default to 512 MiB compressed, 1 GiB extracted, and 100,000 entries. Artifact bounds use the corresponding `--artifact-max-*` flags.

## Download and verification

The runner requests `GET /v1/runner/leases/{lease-id}/source` with its runner ID, run ID, job name, and generation. The shared bearer token is necessary but insufficient: the server also requires the exact current job owner, matching lease/generation, and unexpired deadline. There is no generic public digest endpoint.

Archive bytes stream to a temporary file while the runner enforces the expected size, its own maximum, and the blob SHA-256. A mismatch prevents extraction and reports `ERROR`. Extraction rejects absolute, empty, control-byte, non-canonical, and traversal paths; hard links and special entries are unsupported. Regular files are created only through verified directory parents. Validated relative symlinks are created last, preventing an archive symlink from redirecting earlier writes. Entry and extracted-byte limits bound decompression.

After extraction the runner rebuilds the canonical source manifest. Execution begins only when its logical digest equals the leased source digest.

## Isolated workspaces and cleanup

Each job lease executes below:

```text
<workspace-root>/jobs/<run-id>/<job-name>/<lease-id>/
  .forgeci-workspace.json
  source/
```

Local jobs run in `source/`; Docker jobs mount it read-write at `/workspace`. Each job starts from the immutable source plus only its declared artifact downloads. Passed, failed, canceled, error, shutdown, and lease-loss paths remove temporary downloads and the isolated workspace. Deletion fails closed unless the path is beneath the configured root, matches the run/job/lease shape, and its ownership marker matches. On restart, the runner removes stale marked workspaces left by a crash and preserves unmarked user directories.

## Lease and security model

One heartbeat reports every active job lease. Each result independently renews or cancels only its exact lease. Expired, wrong-runner, wrong-job, and stale ownership tuples cannot download source, transfer artifacts, or complete a job. Runner loss conservatively marks only uncertain running jobs `ABORTED`, blocks their descendants, permits independent work to continue, and never automatically reassigns them.

The shared bearer token is still not per-runner cryptographic identity. Artifact routes additionally require an exact live consumer/producer job lease and durable dependency edge. Pipelines and sources are trusted; workspaces are writable; Docker daemon access is privileged; and ForgeCI provides no hostile multi-tenant sandbox, secret isolation, build cache, retries, or automatic reassignment.
