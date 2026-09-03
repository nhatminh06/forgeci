package postgres

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nhatminh06/forgeci/internal/store"
)

func (s *Store) SetCacheRetention(value time.Duration) { s.cacheRetention = value }
func (s *Store) SetCacheMaxBytes(value int64)          { s.cacheMaxBytes = value }

func (s *Store) LookupCache(ctx context.Context, workspace, key string, now time.Time) (*store.CacheMetadata, error) {
	now = now.UTC()
	var m store.CacheMetadata
	err := s.pool.QueryRow(ctx, `UPDATE cache_entries SET last_accessed_at=$3,expires_at=$3+$4::interval
		WHERE workspace=$1 AND cache_key=$2 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at>$3)
		RETURNING workspace,cache_key,root_name,root_kind,content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count,created_at,last_accessed_at,expires_at,deleted_at`, workspace, key, now, s.cacheRetention.String()).Scan(
		&m.Workspace, &m.Key, &m.RootName, &m.RootKind, &m.ContentSHA256, &m.BlobSHA256, &m.Format, &m.ArchiveSizeBytes, &m.LogicalSizeBytes, &m.EntryCount, &m.CreatedAt, &m.LastAccessedAt, &m.ExpiresAt, &m.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Store) CommitCache(ctx context.Context, m store.CacheMetadata) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var existing store.CacheMetadata
	err = tx.QueryRow(ctx, `SELECT workspace,cache_key,root_name,root_kind,content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count,created_at,last_accessed_at,expires_at,deleted_at FROM cache_entries WHERE workspace=$1 AND cache_key=$2 AND deleted_at IS NULL FOR UPDATE`, m.Workspace, m.Key).Scan(
		&existing.Workspace, &existing.Key, &existing.RootName, &existing.RootKind, &existing.ContentSHA256, &existing.BlobSHA256, &existing.Format, &existing.ArchiveSizeBytes, &existing.LogicalSizeBytes, &existing.EntryCount, &existing.CreatedAt, &existing.LastAccessedAt, &existing.ExpiresAt, &existing.DeletedAt)
	if err == nil {
		if sameCacheMetadata(existing, m) {
			return tx.Commit(ctx)
		}
		return store.ErrConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	expires := m.ExpiresAt
	if expires == nil && s.cacheRetention > 0 {
		v := time.Now().UTC().Add(s.cacheRetention)
		expires = &v
	}
	_, err = tx.Exec(ctx, `INSERT INTO cache_entries(workspace,cache_key,root_name,root_kind,content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count,created_at,last_accessed_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11,now()),COALESCE($11,now()),$12)`, m.Workspace, m.Key, m.RootName, m.RootKind, m.ContentSHA256, m.BlobSHA256, m.Format, m.ArchiveSizeBytes, m.LogicalSizeBytes, m.EntryCount, m.CreatedAt, expires)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func sameCacheMetadata(a, b store.CacheMetadata) bool {
	return a.Workspace == b.Workspace && a.Key == b.Key && a.RootName == b.RootName && a.RootKind == b.RootKind && a.ContentSHA256 == b.ContentSHA256 && a.BlobSHA256 == b.BlobSHA256 && a.Format == b.Format && a.ArchiveSizeBytes == b.ArchiveSizeBytes && a.LogicalSizeBytes == b.LogicalSizeBytes && a.EntryCount == b.EntryCount
}

func (s *Store) ListCache(ctx context.Context, workspace string, limit int) ([]store.CacheMetadata, error) {
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT workspace,cache_key,root_name,root_kind,content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count,created_at,last_accessed_at,expires_at,deleted_at FROM cache_entries WHERE workspace=$1 AND deleted_at IS NULL AND (expires_at IS NULL OR expires_at>now()) ORDER BY cache_key LIMIT $2`, workspace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.CacheMetadata
	for rows.Next() {
		var m store.CacheMetadata
		if err := rows.Scan(&m.Workspace, &m.Key, &m.RootName, &m.RootKind, &m.ContentSHA256, &m.BlobSHA256, &m.Format, &m.ArchiveSizeBytes, &m.LogicalSizeBytes, &m.EntryCount, &m.CreatedAt, &m.LastAccessedAt, &m.ExpiresAt, &m.DeletedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCache(ctx context.Context, workspace, key string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE cache_entries SET deleted_at=now() WHERE workspace=$1 AND cache_key=$2 AND deleted_at IS NULL`, workspace, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) ExpireCache(ctx context.Context, now time.Time, maxBytes int64) ([]string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE cache_entries SET deleted_at=$1 WHERE deleted_at IS NULL AND expires_at IS NOT NULL AND expires_at<=$1`, now.UTC()); err != nil {
		return nil, err
	}
	if maxBytes > 0 {
		rows, err := tx.Query(ctx, `SELECT blob_sha256,max(archive_size_bytes) FROM cache_entries WHERE deleted_at IS NULL GROUP BY blob_sha256`)
		if err != nil {
			return nil, err
		}
		var total int64
		for rows.Next() {
			var digest string
			var size int64
			if err := rows.Scan(&digest, &size); err != nil {
				rows.Close()
				return nil, err
			}
			total += size
		}
		rows.Close()
		if total > maxBytes {
			rows, err = tx.Query(ctx, `SELECT id,blob_sha256,archive_size_bytes FROM cache_entries WHERE deleted_at IS NULL ORDER BY last_accessed_at,created_at,id`)
			if err != nil {
				return nil, err
			}
			seen := map[string]struct{}{}
			for rows.Next() && total > maxBytes {
				var id, digest string
				var size int64
				if err := rows.Scan(&id, &digest, &size); err != nil {
					rows.Close()
					return nil, err
				}
				if _, ok := seen[digest]; ok {
					continue
				}
				seen[digest] = struct{}{}
				if _, err = tx.Exec(ctx, `UPDATE cache_entries SET deleted_at=$2 WHERE blob_sha256=$1 AND deleted_at IS NULL`, digest, now.UTC()); err != nil {
					rows.Close()
					return nil, err
				}
				total -= size
			}
			rows.Close()
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.unreferencedCacheBlobs(ctx), nil
}

func (s *Store) unreferencedCacheBlobs(ctx context.Context) []string {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT e.blob_sha256 FROM cache_entries e WHERE e.deleted_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM cache_entries live WHERE live.blob_sha256=e.blob_sha256 AND live.deleted_at IS NULL)`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if rows.Scan(&d) == nil {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Store) LiveCacheBlobs(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT blob_sha256 FROM cache_entries WHERE deleted_at IS NULL AND (expires_at IS NULL OR expires_at>now())`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out[d] = struct{}{}
	}
	return out, rows.Err()
}
