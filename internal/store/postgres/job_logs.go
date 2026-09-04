package postgres

import (
	"context"
	"fmt"

	"github.com/nhatminh06/forgeci/internal/store"
)

const maxJobLogChunk = 64 << 10

func (s *Store) AppendJobLog(ctx context.Context, c store.JobLogChunk) error {
	if c.RunID == "" || c.JobName == "" || c.Sequence < 1 || len(c.Payload) == 0 || len(c.Payload) > maxJobLogChunk || (c.Stream != store.JobLogStdout && c.Stream != store.JobLogStderr) {
		return fmt.Errorf("invalid job log chunk")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO job_log_chunks(run_id,job_name,sequence,stream,payload,created_at) VALUES($1,$2,$3,$4,$5,COALESCE($6,now())) ON CONFLICT (run_id,job_name,sequence) DO NOTHING`, c.RunID, c.JobName, c.Sequence, c.Stream, c.Payload, c.CreatedAt)
	return err
}

func (s *Store) ListJobLogs(ctx context.Context, runID, jobName string, after int64) ([]store.JobLogChunk, error) {
	rows, err := s.pool.Query(ctx, `SELECT run_id::text,job_name,sequence,stream,created_at,payload FROM job_log_chunks WHERE run_id=$1 AND job_name=$2 AND sequence>$3 ORDER BY sequence`, runID, jobName, after)
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
