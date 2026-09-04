# Durable job logs

ForgeCI stores job output in PostgreSQL `job_log_chunks`. Local execution and
remote runners use the same durable store and preserve one monotonically
increasing sequence per job across stdout and stderr. Writers split output at
64 KiB. Remote runners batch at most eight chunks and 1 MiB per request, and
retain up to 1 MiB of unacknowledged data; writes apply backpressure at that
limit.

Remote upload is a single ordered stream per job. A batch is peeked from the
queue, sent, and acknowledged only after a successful response. Connection,
timeout, temporary network, and HTTP 5xx failures use bounded exponential
backoff. Identical replay is safe; conflicting or out-of-order replay is
rejected transactionally. A runner drains accepted output and verifies upload
success before reporting `PASSED`. Failed, canceled, and runner-lost jobs
retain whatever output was durably acknowledged before termination.

`forge logs RUN --job JOB` retrieves bounded pages using the sequence cursor.
`forge logs RUN --job JOB --follow` uses bounded cursor polling: durable data
returns immediately, an active job waits briefly for new data, and a terminal
job returns remaining chunks before exiting. The cursor is the reconnect
boundary; PostgreSQL, rather than process memory, is the source of truth, so
server restarts do not erase acknowledged logs.

Logs are retained according to the current database lifecycle; this milestone
does not add retention quotas or garbage collection. Direct `forge run` uses
the local durable logging path but has no server-backed restart/reconnect
surface. ForgeCI does not redact arbitrary process output: user processes may
intentionally print secrets, so bearer tokens, authorization headers, and
database credentials must never be written to job output or logs.
