package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

const maxJobLogChunk = 64 << 10

func (s *Store) AppendJobLog(ctx context.Context, c store.JobLogChunk) error {
	if c.RunID == "" || c.JobName == "" || c.Sequence < 1 || len(c.Payload) == 0 || len(c.Payload) > maxJobLogChunk || (c.Stream != store.JobLogStdout && c.Stream != store.JobLogStderr) {
		return fmt.Errorf("invalid job log chunk")
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var jobExists int
	if err = tx.QueryRow(ctx, `SELECT 1 FROM job_runs WHERE run_id=$1 AND job_name=$2 FOR UPDATE`, c.RunID, c.JobName).Scan(&jobExists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}
	var stream store.JobLogStream
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT stream,payload FROM job_log_chunks WHERE run_id=$1 AND job_name=$2 AND sequence=$3`, c.RunID, c.JobName, c.Sequence).Scan(&stream, &payload)
	if err == nil {
		if stream != c.Stream || string(payload) != string(c.Payload) {
			return store.ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var max int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM job_log_chunks WHERE run_id=$1 AND job_name=$2`, c.RunID, c.JobName).Scan(&max); err != nil {
		return err
	}
	if c.Sequence != max+1 {
		return store.ErrConflict
	}
	if _, err = tx.Exec(ctx, `INSERT INTO job_log_chunks(run_id,job_name,sequence,stream,payload,created_at) VALUES($1,$2,$3,$4,$5,COALESCE($6,now()))`, c.RunID, c.JobName, c.Sequence, c.Stream, c.Payload, c.CreatedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListJobLogs(ctx context.Context, runID, jobName string, after int64, limit int) ([]store.JobLogChunk, error) {
	if limit < 1 {
		limit = 256
	}
	if limit > 1000 {
		limit = 1000
	}
	var exists int
	if err := s.pool.QueryRow(ctx, `SELECT 1 FROM job_runs WHERE run_id=$1 AND job_name=$2`, runID, jobName).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT run_id::text,job_name,sequence,stream,created_at,payload FROM job_log_chunks WHERE run_id=$1 AND job_name=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`, runID, jobName, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.JobLogChunk
	for rows.Next() {
		var c store.JobLogChunk
		if err := rows.Scan(&c.RunID, &c.JobName, &c.Sequence, &c.Stream, &c.CreatedAt, &c.Payload); err != nil {
			return nil, err
		}
		c.CreatedAt = c.CreatedAt.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}
