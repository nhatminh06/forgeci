CREATE TABLE source_snapshots (
    source_digest char(64) PRIMARY KEY CHECK (source_digest ~ '^[0-9a-f]{64}$'),
    blob_digest char(64) NOT NULL CHECK (blob_digest ~ '^[0-9a-f]{64}$'),
    format text NOT NULL CHECK (format IN ('tar-gzip-v1')),
    archive_size_bytes bigint NOT NULL CHECK (archive_size_bytes >= 0),
    logical_size_bytes bigint NOT NULL CHECK (logical_size_bytes >= 0),
    entry_count integer NOT NULL CHECK (entry_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE pipeline_runs
    ADD COLUMN source_snapshot_sha256 char(64)
    REFERENCES source_snapshots(source_digest);

CREATE INDEX pipeline_runs_source_snapshot_idx
    ON pipeline_runs(source_snapshot_sha256);
