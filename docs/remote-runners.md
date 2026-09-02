# Remote runners and run leases

In remote mode, `forge-server` persists queued runs but never executes them locally. An outbound-only `forge-runner` registers a stable UUID, reports OS, architecture, Docker availability, and capacity, then long-polls for compatible work.

ForgeCI leases an entire pipeline run to one runner. This preserves shared-workspace semantics between jobs. Jobs inside the run may execute concurrently but never move between runners. Effective concurrency is the smaller of the run limit and runner capacity.

An assignment atomically records the run ID, runner ID, random lease UUID, generation, and expiration. Heartbeats renew only that exact current lease. Events and completion present the same ownership tuple. Expired, replaced, wrong-runner, and stale messages cannot alter durable state. Expiration marks the runner OFFLINE, the run ABORTED, and unfinished jobs ABORTED; uncertain work is never automatically reassigned.

Cancellation is persisted and returned on heartbeat. The runner cancels its scheduler context, terminates local or Docker work, reports CANCELED, and releases the lease. If it disappears first, expiration conservatively produces ABORTED.

Runner endpoints use a shared bearer token from `FORGECI_RUNNER_TOKEN` or the server's `--runner-token-file`. Tokens are not stored or logged. Plain HTTP is loopback-only. Non-loopback listeners require user-provided TLS certificate and key; runners can use `--ca-cert` for a private CA.

The restricted runner state directory contains its stable UUID. Restarting with the same directory reuses the same database identity. One runner holds at most one run because its configured workspace is shared.

The server sends the stored pipeline YAML snapshot, not repository contents. Runner workspaces must already contain the intended source. ForgeCI does not yet verify or synchronize source revisions, create isolated run workspaces, transfer artifacts, persist logs, retry runner-loss failures, or distribute individual jobs across runners.
