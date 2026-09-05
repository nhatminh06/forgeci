# Native GitHub SCM

Register repositories through the production API:

```bash
forge repo add github owner/repo --pipeline forge.yaml
forge repo list
forge repo remove <repository-id>
```

## GitHub App setup

Create a GitHub App with **Metadata: read**, **Contents: read**, and
**Checks: read/write** repository permissions. Subscribe it to `push` and
`pull_request` events and send webhooks to `/v1/hooks/github`. Configure the
server with owner-only regular secret files:

```bash
forge-server \
  --github-app-id 12345 \
  --github-private-key-file /run/secrets/github-app.pem \
  --github-webhook-secret-file /run/secrets/github-webhook \
  --github-api-base-url https://api.github.com \
  --github-clone-base-url https://github.com \
  --scm-worker-concurrency 2 \
  --scm-worker-lease 2m \
  # normal database, snapshot, artifact, and listener flags
```

Webhook bodies are capped and authenticated with HMAC-SHA256 before parsing.
Only enabled registered repository identities are accepted. Payload clone URLs
are ignored: the remote is constructed from the operator clone base and the
normalized `owner/repo` identity.

## Exact source and credentials

Workers fetch the event ref and detach at the event's exact commit SHA. Pull
requests use `refs/pull/<number>/head` and must resolve to the expected SHA.
The pipeline must remain inside the checkout after symlink resolution. GitHub
installation tokens live only in the bounded in-memory cache and subprocess
environment. `GIT_ASKPASS` is a temporary owner-executable helper; tokens never
enter Git argv, remotes, configuration, persisted diagnostics, or logs. Git
stdout and stderr share a live 4 KiB capture budget. Checkouts and helpers are
removed on success, error, and cancellation.

## Durable processing

The webhook transaction creates one `scm_delivery`. PostgreSQL claims use
`FOR UPDATE SKIP LOCKED`, a unique lease token, and an expiry. Claims increment
the attempt count once; stale owners cannot complete, retry, or overwrite a
reclaimed delivery. Permanent source/configuration failures stop immediately.
Transient transport, token, and database failures retry with exponential delay
from one second to one minute, for at most five attempts.

Run creation, snapshot association, jobs, dependencies, and `scm_run_trigger`
are one transaction. Thus one delivery creates at most one run even if a server
stops at that boundary. Expired processing leases are reclaimed after restart.

## GitHub Checks

Each SCM run has a deterministic Check `external_id` equal to its ForgeCI run
ID and the stable name `ForgeCI`.

| ForgeCI | GitHub status | GitHub conclusion |
| --- | --- | --- |
| `QUEUED` | `queued` | — |
| `RUNNING` | `in_progress` | — |
| `PASSED` | `completed` | `success` |
| `FAILED`, `ERROR` | `completed` | `failure` |
| `CANCELED`, `ABORTED` | `completed` | `cancelled` |

If a create response is lost, the bounded reconciler lists Check Runs for the
commit and adopts the matching `external_id` before posting. Desired and last
synced states, retry timing, errors, and Check ID are durable, so terminal state
is repaired after API outages or restart.

## Pull-request supersession

A newer accepted revision transactionally ignores older pending or processing
deliveries for the same repository and PR. Queued older runs are canceled and
running ones receive the normal cancellation request. Cleared lease tokens stop
late workers from changing superseded state. A `closed` event starts no run and
cancels older active work.

## Limitations

Repository code and pipelines remain trusted. ForgeCI does not sandbox local
commands, and Docker daemon access is privileged. Check adoption scans at most
three pages of 100 same-name Checks per commit. Job retry/reassignment and
general-purpose secret injection are outside this milestone.
