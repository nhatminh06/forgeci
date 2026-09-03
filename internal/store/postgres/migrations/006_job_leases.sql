ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check
    CHECK (status IN ('PENDING','RUNNING','PASSED','FAILED','ERROR','BLOCKED','CANCELED','ABORTED'));

ALTER TABLE job_runs
    ADD COLUMN runner_id uuid REFERENCES runners(id) ON DELETE SET NULL,
    ADD COLUMN lease_id uuid,
    ADD COLUMN lease_generation integer NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    ADD COLUMN lease_expires_at timestamptz;

ALTER TABLE job_runs ADD CONSTRAINT job_lease_consistent CHECK (
    (status <> 'RUNNING' AND lease_id IS NULL AND lease_expires_at IS NULL)
    OR
    (status = 'RUNNING' AND (
        (runner_id IS NULL AND lease_id IS NULL AND lease_generation = 0 AND lease_expires_at IS NULL)
        OR
        (runner_id IS NOT NULL AND lease_id IS NOT NULL AND lease_generation > 0 AND lease_expires_at IS NOT NULL)
    ))
);

CREATE TABLE job_dependencies (
    run_id uuid NOT NULL,
    job_name text NOT NULL,
    depends_on_job text NOT NULL,
    PRIMARY KEY (run_id, job_name, depends_on_job),
    FOREIGN KEY (run_id, job_name) REFERENCES job_runs(run_id, job_name) ON DELETE CASCADE,
    FOREIGN KEY (run_id, depends_on_job) REFERENCES job_runs(run_id, job_name) ON DELETE CASCADE,
    CHECK (job_name <> depends_on_job)
);

CREATE INDEX job_dependencies_upstream_idx
    ON job_dependencies(run_id, depends_on_job, job_name);
CREATE INDEX job_runs_ready_idx ON job_runs(status, run_id, job_name);
CREATE INDEX job_runs_active_runner_idx
    ON job_runs(runner_id, lease_expires_at) WHERE status = 'RUNNING';
