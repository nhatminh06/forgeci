CREATE TABLE scm_repositories (
    id uuid PRIMARY KEY,
    provider text NOT NULL,
    full_name text NOT NULL,
    normalized_full_name text NOT NULL,
    pipeline_path text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (provider, normalized_full_name)
);

CREATE TABLE scm_deliveries (
    id uuid PRIMARY KEY,
    provider text NOT NULL,
    delivery_id text NOT NULL,
    repository_id uuid NOT NULL REFERENCES scm_repositories(id) ON DELETE RESTRICT,
    event_type text NOT NULL,
    action text NOT NULL,
    installation_id text,
    commit_sha text,
    ref text,
    pull_request_number integer,
    pull_request_head_ref text,
    pull_request_base_ref text,
    payload_sha256 char(64) NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','PROCESSING','PROCESSED','FAILED','IGNORED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz,
    last_error text,
    received_at timestamptz NOT NULL,
    processed_at timestamptz,
    UNIQUE (provider, delivery_id)
);
CREATE INDEX scm_deliveries_claim_idx ON scm_deliveries(status, next_attempt_at, received_at);

CREATE TABLE scm_run_triggers (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES scm_deliveries(id) ON DELETE RESTRICT,
    repository_id uuid NOT NULL REFERENCES scm_repositories(id) ON DELETE RESTRICT,
    run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
    provider text NOT NULL,
    commit_sha text NOT NULL,
    ref text,
    pull_request_number integer,
    installation_id text,
    check_run_id text,
    check_state text,
    last_check_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (delivery_id),
    UNIQUE (run_id)
);
