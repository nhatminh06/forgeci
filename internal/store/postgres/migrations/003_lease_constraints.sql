ALTER TABLE runners
    ADD CONSTRAINT runners_current_run_fk
    FOREIGN KEY (current_run_id) REFERENCES pipeline_runs(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX runners_one_owner_per_run
    ON runners(current_run_id) WHERE current_run_id IS NOT NULL;

ALTER TABLE pipeline_runs
    ADD CONSTRAINT lease_generation_nonnegative CHECK (lease_generation >= 0),
    ADD CONSTRAINT remote_lease_consistent CHECK (
        (runner_id IS NULL AND lease_id IS NULL AND lease_expires_at IS NULL)
        OR
        (runner_id IS NOT NULL AND lease_id IS NOT NULL AND lease_expires_at IS NOT NULL
            AND effective_parallel IS NOT NULL AND effective_parallel > 0 AND lease_generation > 0)
    );
