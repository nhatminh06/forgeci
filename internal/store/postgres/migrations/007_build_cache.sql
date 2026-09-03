CREATE TABLE cache_entries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace text NOT NULL,
    cache_key text NOT NULL CHECK (cache_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    root_name text NOT NULL CHECK (root_name <> '' AND root_name !~ '[/\\\\]'),
    root_kind text NOT NULL CHECK (root_kind IN ('file','directory')),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    blob_sha256 text NOT NULL CHECK (blob_sha256 ~ '^[0-9a-f]{64}$'),
    format text NOT NULL CHECK (format='cache-tar-gzip-v1'),
    archive_size_bytes bigint NOT NULL CHECK (archive_size_bytes >= 0),
    logical_size_bytes bigint NOT NULL CHECK (logical_size_bytes >= 0),
    entry_count integer NOT NULL CHECK (entry_count > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    last_accessed_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    deleted_at timestamptz
);
CREATE UNIQUE INDEX cache_entries_active_key ON cache_entries(workspace, cache_key) WHERE deleted_at IS NULL;
CREATE INDEX cache_entries_expiry ON cache_entries(expires_at) WHERE deleted_at IS NULL;
CREATE INDEX cache_entries_lru ON cache_entries(last_accessed_at, created_at, id) WHERE deleted_at IS NULL;
CREATE INDEX cache_entries_blob ON cache_entries(blob_sha256) WHERE deleted_at IS NULL;
