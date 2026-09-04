CREATE TABLE IF NOT EXISTS job_log_chunks (
    run_id uuid NOT NULL,
    job_name text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    stream text NOT NULL CHECK (stream IN ('stdout', 'stderr')),
    payload bytea NOT NULL CHECK (octet_length(payload) > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, job_name, sequence),
    FOREIGN KEY (run_id, job_name) REFERENCES job_runs(run_id, job_name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS job_log_chunks_cursor ON job_log_chunks(run_id, job_name, sequence);
