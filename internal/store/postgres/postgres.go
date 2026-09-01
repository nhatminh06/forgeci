package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nhatminh06/forgeci/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	config.MaxConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=1)`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	if !exists {
		sql, err := migrations.ReadFile("migrations/001_initial.sql")
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration 1: %w", err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES(1)`); err != nil {
			return fmt.Errorf("record migration 1: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func (s *Store) CreateRun(ctx context.Context, in store.CreateRun) (*store.Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_runs(id,status,pipeline_file,pipeline_yaml,pipeline_sha256,workspace,max_parallel) VALUES($1,'QUEUED',$2,$3,$4,$5,$6)`, in.ID, in.PipelineFile, in.PipelineYAML, in.PipelineSHA256, in.Workspace, in.MaxParallel)
	if err != nil {
		return nil, fmt.Errorf("insert run: %w", err)
	}
	for _, job := range in.Jobs {
		if _, err = tx.Exec(ctx, `INSERT INTO job_runs(run_id,job_name,status,image) VALUES($1,$2,'PENDING',$3)`, in.ID, job.Name, job.Image); err != nil {
			return nil, fmt.Errorf("insert job %q: %w", job.Name, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit run: %w", err)
	}
	return s.GetRun(ctx, in.ID)
}

const runColumns = `id::text,status,pipeline_file,pipeline_yaml,pipeline_sha256,workspace,max_parallel,created_at,started_at,finished_at,cancel_requested_at,error_message`

func scanRun(row pgx.Row) (*store.Run, error) {
	r := &store.Run{}
	err := row.Scan(&r.ID, &r.Status, &r.PipelineFile, &r.PipelineYAML, &r.PipelineSHA256, &r.Workspace, &r.MaxParallel, &r.CreatedAt, &r.StartedAt, &r.FinishedAt, &r.CancelRequestedAt, &r.ErrorMessage)
	r.CreatedAt = r.CreatedAt.UTC()
	r.StartedAt = utcTime(r.StartedAt)
	r.FinishedAt = utcTime(r.FinishedAt)
	r.CancelRequestedAt = utcTime(r.CancelRequestedAt)
	return r, err
}

func utcTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func (s *Store) GetRun(ctx context.Context, id string) (*store.Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM pipeline_runs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT job_name,status,image,started_at,finished_at,error_message FROM job_runs WHERE run_id=$1 ORDER BY job_name`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var j store.Job
		if err := rows.Scan(&j.Name, &j.Status, &j.Image, &j.StartedAt, &j.FinishedAt, &j.ErrorMessage); err != nil {
			return nil, err
		}
		j.StartedAt = utcTime(j.StartedAt)
		j.FinishedAt = utcTime(j.FinishedAt)
		r.Jobs = append(r.Jobs, j)
	}
	return r, rows.Err()
}

func (s *Store) ListRuns(ctx context.Context, limit int) ([]store.Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM pipeline_runs ORDER BY created_at DESC,id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *Store) ClaimNextQueuedRun(ctx context.Context) (*store.Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id string
	err = tx.QueryRow(ctx, `SELECT id::text FROM pipeline_runs WHERE status='QUEUED' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs SET status='RUNNING',started_at=now() WHERE id=$1 AND status='QUEUED'`, id)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return nil, fmt.Errorf("claim queued run: %w", err)
		}
		return nil, store.ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetRun(ctx, id)
}

func (s *Store) FinishRun(ctx context.Context, id string, status store.RunStatus, message *string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE pipeline_runs SET status=$2,finished_at=now(),error_message=$3 WHERE id=$1 AND status='RUNNING'`, id, status, message)
	if err == nil && tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return err
}
func (s *Store) UpdateJob(ctx context.Context, runID, name string, status store.JobStatus, message *string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs SET status=$3,started_at=CASE WHEN $3='RUNNING' THEN COALESCE(started_at,now()) ELSE started_at END,finished_at=CASE WHEN $3 IN ('PASSED','FAILED','BLOCKED','CANCELED','ABORTED') THEN now() ELSE finished_at END,error_message=$4 WHERE run_id=$1 AND job_name=$2`, runID, name, status, message)
	if err == nil && tag.RowsAffected() != 1 {
		return store.ErrNotFound
	}
	return err
}
func (s *Store) CancelQueued(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs SET status='CANCELED',cancel_requested_at=now(),finished_at=now() WHERE id=$1 AND status='QUEUED'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE job_runs SET status='CANCELED',finished_at=now() WHERE run_id=$1 AND status='PENDING'`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) RequestCancel(ctx context.Context, id string) (store.RunStatus, error) {
	var status store.RunStatus
	err := s.pool.QueryRow(ctx, `UPDATE pipeline_runs SET cancel_requested_at=COALESCE(cancel_requested_at,now()) WHERE id=$1 AND status='RUNNING' RETURNING status`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		r, e := s.GetRun(ctx, id)
		if e != nil {
			return "", e
		}
		return r.Status, store.ErrConflict
	}
	return status, err
}
func (s *Store) RecoverInterrupted(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `UPDATE pipeline_runs SET status='ABORTED',finished_at=now(),error_message='control plane restarted before run completed' WHERE status='RUNNING' RETURNING id::text`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	sort.Strings(ids)
	for _, id := range ids {
		if _, err = tx.Exec(ctx, `UPDATE job_runs SET status='ABORTED',finished_at=now(),error_message='control plane restarted before run completed' WHERE run_id=$1 AND status IN ('PENDING','RUNNING')`, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
