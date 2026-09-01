CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_runs (
    id uuid PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('QUEUED','RUNNING','PASSED','FAILED','CANCELED','ERROR','ABORTED')),
    pipeline_file text NOT NULL,
    pipeline_yaml bytea NOT NULL,
    pipeline_sha256 char(64) NOT NULL,
    workspace text NOT NULL,
    max_parallel integer NOT NULL CHECK (max_parallel > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    cancel_requested_at timestamptz,
    error_message text
);

CREATE TABLE job_runs (
    run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    job_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','RUNNING','PASSED','FAILED','BLOCKED','CANCELED','ABORTED')),
    image text,
    started_at timestamptz,
    finished_at timestamptz,
    error_message text,
    PRIMARY KEY (run_id, job_name)
);

CREATE INDEX pipeline_runs_queue_idx ON pipeline_runs(status, created_at, id);
CREATE INDEX job_runs_run_idx ON job_runs(run_id);
