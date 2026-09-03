CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    producer_job text NOT NULL,
    name text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    root_name text NOT NULL CHECK (root_name <> '' AND root_name !~ '[/\\]'),
    root_kind text NOT NULL CHECK (root_kind IN ('file','directory')),
    artifact_content_sha256 char(64) NOT NULL CHECK (artifact_content_sha256 ~ '^[0-9a-f]{64}$'),
    blob_sha256 char(64) NOT NULL CHECK (blob_sha256 ~ '^[0-9a-f]{64}$'),
    format text NOT NULL CHECK (format = 'artifact-tar-gzip-v1'),
    archive_size_bytes bigint NOT NULL CHECK (archive_size_bytes >= 0),
    logical_size_bytes bigint NOT NULL CHECK (logical_size_bytes >= 0),
    entry_count integer NOT NULL CHECK (entry_count > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    deleted_at timestamptz,
    UNIQUE (run_id, producer_job, name),
    FOREIGN KEY (run_id, producer_job) REFERENCES job_runs(run_id, job_name) ON DELETE CASCADE,
    CHECK (deleted_at IS NULL OR expires_at IS NOT NULL)
);

CREATE INDEX artifacts_run_idx ON artifacts(run_id, producer_job, name);
CREATE INDEX artifacts_blob_live_idx ON artifacts(blob_sha256) WHERE deleted_at IS NULL;
CREATE INDEX artifacts_expiry_idx ON artifacts(expires_at) WHERE deleted_at IS NULL;
