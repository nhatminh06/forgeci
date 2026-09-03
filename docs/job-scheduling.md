# Job-level remote scheduling

Milestone 8 moves DAG readiness and concurrency enforcement into the control plane. A remote runner leases one ready job, not a whole pipeline. Independent jobs from one run can therefore execute on different runners while preserving durable dependency ordering.

The scheduler considers `PENDING` jobs in run creation order, run ID, then job name. A job is ready only when every persisted `job_dependencies` predecessor is `PASSED`. The claim transaction locks the runner and candidate job, uses `SKIP LOCKED`, checks Docker capability, and enforces both the runner's `max_parallel` active jobs and the run's `max_parallel` active jobs.

Each lease identity is `(run, job, runner, lease ID, generation, expiration)`. Source, artifact, heartbeat, and completion operations require that exact unexpired tuple. A heartbeat carries all active leases and receives validity, cancellation, and renewed expiration independently for each job.

Every job receives a fresh `<workspace-root>/jobs/<run>/<job>/<lease>` directory. The runner downloads and verifies the run's immutable source snapshot, restores only explicitly declared artifacts, executes one local or Docker job, publishes declared outputs, and reports one terminal result. Mutable producer workspace state is never copied to a consumer.

`FAILED` and `ERROR` jobs durably block descendants, while independent branches continue. Lease loss or conservative server recovery marks an uncertain running job `ABORTED`; it is never reassigned because retry semantics do not yet exist. A run becomes terminal only after no job remains pending or running. Final precedence is `ERROR`, `ABORTED`, cancellation, `FAILED`/`BLOCKED`, then `PASSED`.

Protocol v1 runners are rejected. Historical run-level lease columns remain for migration compatibility, but new remote execution uses only protocol v2 job leases.
