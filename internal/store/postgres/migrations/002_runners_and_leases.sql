-- Add runners table
CREATE TABLE runners (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    protocol_version integer NOT NULL CHECK (protocol_version > 0),
    os text NOT NULL,
    arch text NOT NULL,
    docker_available boolean NOT NULL DEFAULT false,
    max_parallel integer NOT NULL CHECK (max_parallel > 0),
    status text NOT NULL CHECK (status IN ('ONLINE','OFFLINE')) DEFAULT 'ONLINE',
    registered_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    current_run_id uuid,
    UNIQUE(id)
);

CREATE INDEX runners_status_idx ON runners(status);
CREATE INDEX runners_last_seen_idx ON runners(last_seen_at);

-- Add lease fields to pipeline_runs
ALTER TABLE pipeline_runs
    ADD COLUMN runner_id uuid REFERENCES runners(id) ON DELETE SET NULL,
    ADD COLUMN lease_id uuid,
    ADD COLUMN lease_generation integer DEFAULT 0,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN effective_parallel integer;

-- Update existing runs to be compatible with new schema
UPDATE pipeline_runs SET effective_parallel = max_parallel WHERE effective_parallel IS NULL;

-- Add constraint for effective_parallel
ALTER TABLE pipeline_runs
    ADD CONSTRAINT effective_parallel_valid CHECK (effective_parallel IS NULL OR effective_parallel > 0);

CREATE INDEX pipeline_runs_runner_idx ON pipeline_runs(runner_id);
CREATE INDEX pipeline_runs_lease_expires_idx ON pipeline_runs(lease_expires_at);
