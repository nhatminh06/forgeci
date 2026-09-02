package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
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

	for version := 1; version <= 2; version++ {
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("read migration version %d: %w", version, err)
		}
		if !exists {
			filename := fmt.Sprintf("migrations/%03d_initial.sql", version)
			if version == 2 {
				filename = "migrations/002_runners_and_leases.sql"
			}
			sql, err := migrations.ReadFile(filename)
			if err != nil {
				return fmt.Errorf("read migration %d: %w", version, err)
			}
			if _, err = tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("apply migration %d: %w", version, err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, version); err != nil {
				return fmt.Errorf("record migration %d: %w", version, err)
			}
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

const runColumns = `id::text,status,pipeline_file,pipeline_yaml,pipeline_sha256,workspace,max_parallel,created_at,started_at,finished_at,cancel_requested_at,error_message,runner_id::text,lease_id::text,lease_generation,lease_expires_at,effective_parallel`

func scanRun(row pgx.Row) (*store.Run, error) {
	r := &store.Run{}
	err := row.Scan(&r.ID, &r.Status, &r.PipelineFile, &r.PipelineYAML, &r.PipelineSHA256, &r.Workspace, &r.MaxParallel, &r.CreatedAt, &r.StartedAt, &r.FinishedAt, &r.CancelRequestedAt, &r.ErrorMessage, &r.RunnerID, &r.LeaseID, &r.LeaseGeneration, &r.LeaseExpiresAt, &r.EffectiveParallel)
	r.CreatedAt = r.CreatedAt.UTC()
	r.StartedAt = utcTime(r.StartedAt)
	r.FinishedAt = utcTime(r.FinishedAt)
	r.CancelRequestedAt = utcTime(r.CancelRequestedAt)
	r.LeaseExpiresAt = utcTime(r.LeaseExpiresAt)
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

// Runner management

func (s *Store) RegisterRunner(ctx context.Context, r store.Runner) (*store.Runner, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO runners(id,name,protocol_version,os,arch,docker_available,max_parallel,status,registered_at,last_seen_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,now(),now())
		ON CONFLICT(id) DO UPDATE SET
			name=EXCLUDED.name,
			protocol_version=EXCLUDED.protocol_version,
			os=EXCLUDED.os,
			arch=EXCLUDED.arch,
			docker_available=EXCLUDED.docker_available,
			max_parallel=EXCLUDED.max_parallel,
			status='ONLINE',
			last_seen_at=now()`,
		r.ID, r.Name, r.ProtocolVersion, r.OS, r.Arch, r.DockerAvailable, r.MaxParallel, store.RunnerOnline)
	if err != nil {
		return nil, fmt.Errorf("register runner: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return s.GetRunner(ctx, r.ID)
}

func (s *Store) GetRunner(ctx context.Context, id string) (*store.Runner, error) {
	r := &store.Runner{}
	err := s.pool.QueryRow(ctx, `SELECT id::text,name,protocol_version,os,arch,docker_available,max_parallel,status,registered_at,last_seen_at,current_run_id::text
		FROM runners WHERE id=$1`, id).Scan(
		&r.ID, &r.Name, &r.ProtocolVersion, &r.OS, &r.Arch, &r.DockerAvailable, &r.MaxParallel, &r.Status, &r.RegisteredAt, &r.LastSeenAt, &r.CurrentRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.RegisteredAt = r.RegisteredAt.UTC()
	r.LastSeenAt = r.LastSeenAt.UTC()
	return r, nil
}

func (s *Store) ListRunners(ctx context.Context) ([]store.Runner, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text,name,protocol_version,os,arch,docker_available,max_parallel,status,registered_at,last_seen_at,current_run_id::text
		FROM runners ORDER BY name, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runners []store.Runner
	for rows.Next() {
		r := store.Runner{}
		if err := rows.Scan(&r.ID, &r.Name, &r.ProtocolVersion, &r.OS, &r.Arch, &r.DockerAvailable, &r.MaxParallel, &r.Status, &r.RegisteredAt, &r.LastSeenAt, &r.CurrentRunID); err != nil {
			return nil, err
		}
		r.RegisteredAt = r.RegisteredAt.UTC()
		r.LastSeenAt = r.LastSeenAt.UTC()
		runners = append(runners, r)
	}
	return runners, rows.Err()
}

func (s *Store) UpdateRunnerLiveness(ctx context.Context, runnerID string, lastSeenAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE runners SET last_seen_at=$2, status='ONLINE' WHERE id=$1`,
		runnerID, lastSeenAt.UTC())
	if err == nil && tag.RowsAffected() != 1 {
		return store.ErrNotFound
	}
	return err
}

// Lease management

func (s *Store) LeaseRun(ctx context.Context, runnerID, _ string) (*store.Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Verify runner exists
	var runnerDockerAvail, maxParallel int
	err = tx.QueryRow(ctx, `SELECT COALESCE(docker_available::int, 0), max_parallel FROM runners WHERE id=$1`, runnerID).Scan(&runnerDockerAvail, &maxParallel)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Check if runner already has an active run
	var hasActiveRun bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runners WHERE id=$1 AND current_run_id IS NOT NULL)`, runnerID).Scan(&hasActiveRun)
	if err != nil {
		return nil, err
	}
	if hasActiveRun {
		return nil, nil // Runner already has active run
	}

	// Find oldest compatible QUEUED run
	// For now, in Milestone 5, lease entire run to one runner
	// Compatible means: if run has Docker jobs, runner must have Docker available
	var runID string
	err = tx.QueryRow(ctx, `
		SELECT p.id FROM pipeline_runs p
		WHERE p.status='QUEUED'
		AND ($1::int = 1 OR NOT EXISTS (
			SELECT 1 FROM job_runs j WHERE j.run_id=p.id AND j.image IS NOT NULL
		))
		ORDER BY p.created_at,p.id
		LIMIT 1
		FOR UPDATE OF p SKIP LOCKED
	`, runnerDockerAvail).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // No compatible run available
	}
	if err != nil {
		return nil, err
	}

	// Generate lease ID
	leaseID := uuid.New().String()

	// Get run's max_parallel
	var runMaxParallel int
	err = tx.QueryRow(ctx, `SELECT max_parallel FROM pipeline_runs WHERE id=$1`, runID).Scan(&runMaxParallel)
	if err != nil {
		return nil, err
	}

	effectiveParallel := runMaxParallel
	if maxParallel < runMaxParallel {
		effectiveParallel = maxParallel
	}

	ttl := 30 * time.Second
	leaseExpiresAt := time.Now().UTC().Add(ttl)

	// Atomically assign lease
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs
		SET status='RUNNING', started_at=now(), runner_id=$2, lease_id=$3, lease_generation=lease_generation+1, lease_expires_at=$4, effective_parallel=$5
		WHERE id=$1 AND status='QUEUED'`, runID, runnerID, leaseID, leaseExpiresAt, effectiveParallel)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, store.ErrConflict
	}

	// Assign run to runner
	_, err = tx.Exec(ctx, `UPDATE runners SET current_run_id=$2 WHERE id=$1`, runnerID, runID)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return s.GetRun(ctx, runID)
}

func (s *Store) RenewLease(ctx context.Context, runID, runnerID, leaseID string, expectedGeneration int, expiresAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE pipeline_runs
		SET lease_expires_at=$5
		WHERE id=$1 AND runner_id=$2 AND lease_id=$3 AND lease_generation=$4 AND status='RUNNING' AND lease_expires_at > now()`,
		runID, runnerID, leaseID, expectedGeneration, expiresAt.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) ReportJobEvent(ctx context.Context, runID, runnerID, leaseID string, generation int, jobName string, status store.JobStatus) error {
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs j SET status=$6,
		started_at=CASE WHEN $6='RUNNING' THEN COALESCE(j.started_at,now()) ELSE j.started_at END,
		finished_at=CASE WHEN $6 IN ('PASSED','FAILED','BLOCKED','CANCELED') THEN COALESCE(j.finished_at,now()) ELSE j.finished_at END
		FROM pipeline_runs p
		WHERE j.run_id=p.id AND p.id=$1 AND p.runner_id=$2 AND p.lease_id=$3 AND p.lease_generation=$4
		AND p.status='RUNNING' AND p.lease_expires_at > now() AND j.job_name=$5
		AND (j.status=$6 OR (j.status='PENDING' AND $6 IN ('RUNNING','BLOCKED','CANCELED')) OR (j.status='RUNNING' AND $6 IN ('PASSED','FAILED','CANCELED')))`,
		runID, runnerID, leaseID, generation, jobName, status)
	if err == nil && tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return err
}

func (s *Store) CompleteRun(ctx context.Context, runID, runnerID, leaseID string, expectedGeneration int, status store.RunStatus, message *string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Verify lease before accepting completion
	var leaseValid bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pipeline_runs
		WHERE id=$1 AND runner_id=$2 AND lease_id=$3 AND lease_generation=$4 AND status='RUNNING' AND lease_expires_at > now()
	)`, runID, runnerID, leaseID, expectedGeneration).Scan(&leaseValid)
	if err != nil {
		return err
	}
	if !leaseValid {
		return store.ErrConflict
	}

	// Update run status
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs
		SET status=$2, finished_at=now(), error_message=$3, lease_id=NULL, runner_id=NULL, lease_expires_at=NULL, effective_parallel=NULL
		WHERE id=$1 AND status='RUNNING'`, runID, status, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}

	// Clear runner's current run
	_, err = tx.Exec(ctx, `UPDATE runners SET current_run_id=NULL WHERE id=$1`, runnerID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) ExpireLeases(ctx context.Context, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Find expired runs
	rows, err := tx.Query(ctx, `SELECT id::text FROM pipeline_runs
		WHERE status='RUNNING' AND lease_expires_at < $1 AND runner_id IS NOT NULL
		FOR UPDATE`, now.UTC())
	if err != nil {
		return err
	}

	var expiredRunIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		expiredRunIDs = append(expiredRunIDs, id)
	}
	rows.Close()

	for _, runID := range expiredRunIDs {
		// Mark run as ABORTED
		_, err := tx.Exec(ctx, `UPDATE pipeline_runs
			SET status='ABORTED', finished_at=now(), error_message='runner lease expired', lease_id=NULL, runner_id=NULL, lease_expires_at=NULL, effective_parallel=NULL
			WHERE id=$1`, runID)
		if err != nil {
			return err
		}

		// Mark unfinished jobs as ABORTED
		_, err = tx.Exec(ctx, `UPDATE job_runs
			SET status='ABORTED', finished_at=now(), error_message='runner lease expired'
			WHERE run_id=$1 AND status IN ('PENDING', 'RUNNING')`, runID)
		if err != nil {
			return err
		}
	}

	// Mark runners with expired leases as OFFLINE
	_, err = tx.Exec(ctx, `UPDATE runners
		SET status='OFFLINE', current_run_id=NULL
		WHERE current_run_id IS NOT NULL AND (
			SELECT status FROM pipeline_runs WHERE id=current_run_id
		) = 'ABORTED'`)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
