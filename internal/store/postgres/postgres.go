package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	artifactpkg "github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	pool              *pgxpool.Pool
	artifactRetention time.Duration
}

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
	s := &Store{pool: pool, artifactRetention: 168 * time.Hour}
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

func (s *Store) Close()                                   { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error           { return s.pool.Ping(ctx) }
func (s *Store) SetArtifactRetention(value time.Duration) { s.artifactRetention = value }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}

	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for version := 1; version <= len(files); version++ {
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("read migration version %d: %w", version, err)
		}
		if !exists {
			filename := fmt.Sprintf("migrations/%03d_", version)
			for _, file := range files {
				if len(file.Name()) >= 4 && file.Name()[:4] == fmt.Sprintf("%03d_", version) {
					filename = "migrations/" + file.Name()
					break
				}
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
	if in.Snapshot == nil {
		return nil, fmt.Errorf("source snapshot is required for new runs")
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_snapshots(source_digest,blob_digest,format,archive_size_bytes,logical_size_bytes,entry_count,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(source_digest) DO NOTHING`, in.Snapshot.SourceDigest, in.Snapshot.BlobDigest, in.Snapshot.Format, in.Snapshot.ArchiveSizeBytes, in.Snapshot.LogicalSizeBytes, in.Snapshot.EntryCount, in.Snapshot.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert source snapshot: %w", err)
	}
	var blob, format string
	var archive, logical int64
	var entries int
	if err = tx.QueryRow(ctx, `SELECT blob_digest,format,archive_size_bytes,logical_size_bytes,entry_count FROM source_snapshots WHERE source_digest=$1`, in.Snapshot.SourceDigest).Scan(&blob, &format, &archive, &logical, &entries); err != nil {
		return nil, err
	}
	if blob != in.Snapshot.BlobDigest || format != in.Snapshot.Format || archive != in.Snapshot.ArchiveSizeBytes || logical != in.Snapshot.LogicalSizeBytes || entries != in.Snapshot.EntryCount {
		return nil, fmt.Errorf("conflicting source snapshot metadata")
	}
	_, err = tx.Exec(ctx, `INSERT INTO pipeline_runs(id,status,pipeline_file,pipeline_yaml,pipeline_sha256,workspace,max_parallel,source_snapshot_sha256) VALUES($1,'QUEUED',$2,$3,$4,$5,$6,$7)`, in.ID, in.PipelineFile, in.PipelineYAML, in.PipelineSHA256, in.Workspace, in.MaxParallel, in.Snapshot.SourceDigest)
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

const runColumns = `p.id::text,p.status,p.pipeline_file,p.pipeline_yaml,p.pipeline_sha256,p.source_snapshot_sha256,COALESCE(s.blob_digest,''),COALESCE(s.format,''),COALESCE(s.archive_size_bytes,0),COALESCE(s.logical_size_bytes,0),COALESCE(s.entry_count,0),p.workspace,p.max_parallel,p.created_at,p.started_at,p.finished_at,p.cancel_requested_at,p.error_message,p.runner_id::text,p.lease_id::text,p.lease_generation,p.lease_expires_at,p.effective_parallel`

func scanRun(row pgx.Row) (*store.Run, error) {
	r := &store.Run{}
	err := row.Scan(&r.ID, &r.Status, &r.PipelineFile, &r.PipelineYAML, &r.PipelineSHA256, &r.SourceSnapshotSHA256, &r.SnapshotBlobSHA256, &r.SnapshotFormat, &r.SnapshotArchiveSize, &r.SnapshotLogicalSize, &r.SnapshotEntryCount, &r.Workspace, &r.MaxParallel, &r.CreatedAt, &r.StartedAt, &r.FinishedAt, &r.CancelRequestedAt, &r.ErrorMessage, &r.RunnerID, &r.LeaseID, &r.LeaseGeneration, &r.LeaseExpiresAt, &r.EffectiveParallel)
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
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM pipeline_runs p LEFT JOIN source_snapshots s ON s.source_digest=p.source_snapshot_sha256 WHERE p.id=$1`, id))
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
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM pipeline_runs p LEFT JOIN source_snapshots s ON s.source_digest=p.source_snapshot_sha256 ORDER BY p.created_at DESC,p.id DESC LIMIT $1`, limit)
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs SET status=$2,finished_at=now(),error_message=$3 WHERE id=$1 AND status='RUNNING'`, id, status, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, id, s.artifactRetention.String()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) UpdateJob(ctx context.Context, runID, name string, status store.JobStatus, message *string) error {
	if status == store.JobPassed {
		ok, err := s.artifactSetCommitted(ctx, runID, name)
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrConflict
		}
	}
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs SET status=$3,started_at=CASE WHEN $3='RUNNING' THEN COALESCE(started_at,now()) ELSE started_at END,finished_at=CASE WHEN $3 IN ('PASSED','FAILED','BLOCKED','CANCELED','ABORTED') THEN now() ELSE finished_at END,error_message=$4 WHERE run_id=$1 AND job_name=$2`, runID, name, status, message)
	if err == nil && tag.RowsAffected() != 1 {
		return store.ErrNotFound
	}
	return err
}
func (s *Store) artifactSetCommitted(ctx context.Context, runID, name string) (bool, error) {
	var data []byte
	if err := s.pool.QueryRow(ctx, `SELECT pipeline_yaml FROM pipeline_runs WHERE id=$1`, runID).Scan(&data); errors.Is(err, pgx.ErrNoRows) {
		return false, store.ErrNotFound
	} else if err != nil {
		return false, err
	}
	cfg, err := config.ParseBytes(data, "stored pipeline snapshot")
	if err != nil {
		return false, err
	}
	job, ok := cfg.Jobs[name]
	if !ok {
		return false, store.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT name FROM artifacts WHERE run_id=$1 AND producer_job=$2 AND deleted_at IS NULL ORDER BY name`, runID, name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return false, err
		}
		actual = append(actual, value)
	}
	expected := make([]string, len(job.Artifacts.Upload))
	for i, item := range job.Artifacts.Upload {
		expected[i] = item.Name
	}
	sort.Strings(expected)
	return slices.Equal(expected, actual), rows.Err()
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
		if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, id, s.artifactRetention.String()); err != nil {
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
	if status == store.JobPassed {
		ok, err := s.artifactSetCommitted(ctx, runID, jobName)
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrConflict
		}
	}
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
	if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, runID, s.artifactRetention.String()); err != nil {
		return err
	}

	// Clear runner's current run
	_, err = tx.Exec(ctx, `UPDATE runners SET current_run_id=NULL WHERE id=$1`, runnerID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) CommitArtifacts(ctx context.Context, owner store.ArtifactOwnership, items []store.Artifact) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var pipelineYAML []byte
	var jobStatus store.JobStatus
	var valid bool
	query := `SELECT p.pipeline_yaml,j.status,($2='' OR (p.runner_id::text=$2 AND p.lease_id::text=$3 AND p.lease_generation=$4 AND p.lease_expires_at>now())) FROM pipeline_runs p JOIN job_runs j ON j.run_id=p.id AND j.job_name=$5 WHERE p.id=$1 AND p.status='RUNNING' FOR UPDATE OF p,j`
	if err = tx.QueryRow(ctx, query, owner.RunID, owner.RunnerID, owner.LeaseID, owner.Generation, owner.JobName).Scan(&pipelineYAML, &jobStatus, &valid); errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	if !valid || jobStatus != store.JobRunning {
		return store.ErrConflict
	}
	cfg, err := config.ParseBytes(pipelineYAML, "stored pipeline snapshot")
	if err != nil {
		return err
	}
	job, ok := cfg.Jobs[owner.JobName]
	if !ok {
		return store.ErrNotFound
	}
	expected := make(map[string]string, len(job.Artifacts.Upload))
	for _, u := range job.Artifacts.Upload {
		expected[u.Name] = path.Base(u.Path)
	}
	if len(items) != len(expected) {
		return store.ErrConflict
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		root, ok := expected[item.Name]
		if !ok {
			return store.ErrConflict
		}
		if _, ok := seen[item.Name]; ok {
			return store.ErrConflict
		}
		seen[item.Name] = struct{}{}
		if item.ProducerJob != owner.JobName || item.RunID != "" && item.RunID != owner.RunID || !artifactpkg.ValidDigest(item.ContentSHA256) || !artifactpkg.ValidDigest(item.BlobSHA256) || item.Format != artifactpkg.Format || item.RootName != root || (item.RootKind != "file" && item.RootKind != "directory") || item.ArchiveSizeBytes < 0 || item.LogicalSizeBytes < 0 || item.EntryCount < 1 {
			return store.ErrConflict
		}
	}
	rows, err := tx.Query(ctx, `SELECT producer_job,name,root_name,root_kind,artifact_content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count FROM artifacts WHERE run_id=$1 AND producer_job=$2 ORDER BY name`, owner.RunID, owner.JobName)
	if err != nil {
		return err
	}
	var existing []store.Artifact
	for rows.Next() {
		var a store.Artifact
		if err := rows.Scan(&a.ProducerJob, &a.Name, &a.RootName, &a.RootKind, &a.ContentSHA256, &a.BlobSHA256, &a.Format, &a.ArchiveSizeBytes, &a.LogicalSizeBytes, &a.EntryCount); err != nil {
			rows.Close()
			return err
		}
		existing = append(existing, a)
	}
	rows.Close()
	if len(existing) > 0 {
		if !sameArtifactSet(existing, items) {
			return store.ErrConflict
		}
		return tx.Commit(ctx)
	}
	for _, item := range items {
		_, err = tx.Exec(ctx, `INSERT INTO artifacts(id,run_id,producer_job,name,root_name,root_kind,artifact_content_sha256,blob_sha256,format,archive_size_bytes,logical_size_bytes,entry_count,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, uuid.NewString(), owner.RunID, owner.JobName, item.Name, item.RootName, item.RootKind, item.ContentSHA256, item.BlobSHA256, item.Format, item.ArchiveSizeBytes, item.LogicalSizeBytes, item.EntryCount, item.CreatedAt)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func sameArtifactSet(a, b []store.Artifact) bool {
	if len(a) != len(b) {
		return false
	}
	byName := make(map[string]store.Artifact, len(b))
	for _, x := range b {
		byName[x.Name] = x
	}
	for _, x := range a {
		y, ok := byName[x.Name]
		if !ok || x.RootName != y.RootName || x.RootKind != y.RootKind || x.ContentSHA256 != y.ContentSHA256 || x.BlobSHA256 != y.BlobSHA256 || x.Format != y.Format || x.ArchiveSizeBytes != y.ArchiveSizeBytes || x.LogicalSizeBytes != y.LogicalSizeBytes || x.EntryCount != y.EntryCount {
			return false
		}
	}
	return true
}

const artifactColumns = `a.id::text,a.run_id::text,a.producer_job,a.name,a.root_name,a.root_kind,a.artifact_content_sha256,a.blob_sha256,a.format,a.archive_size_bytes,a.logical_size_bytes,a.entry_count,a.created_at,a.expires_at,a.deleted_at,(a.deleted_at IS NULL)`

func scanArtifact(row pgx.Row) (*store.Artifact, error) {
	var a store.Artifact
	err := row.Scan(&a.ID, &a.RunID, &a.ProducerJob, &a.Name, &a.RootName, &a.RootKind, &a.ContentSHA256, &a.BlobSHA256, &a.Format, &a.ArchiveSizeBytes, &a.LogicalSizeBytes, &a.EntryCount, &a.CreatedAt, &a.ExpiresAt, &a.DeletedAt, &a.Available)
	if err != nil {
		return nil, err
	}
	a.CreatedAt = a.CreatedAt.UTC()
	a.ExpiresAt = utcTime(a.ExpiresAt)
	a.DeletedAt = utcTime(a.DeletedAt)
	return &a, nil
}
func (s *Store) GetArtifactForLease(ctx context.Context, owner store.ArtifactOwnership, producer, name string) (*store.Artifact, error) {
	a, err := scanArtifact(s.pool.QueryRow(ctx, `SELECT `+artifactColumns+` FROM artifacts a JOIN pipeline_runs p ON p.id=a.run_id JOIN job_runs j ON j.run_id=a.run_id AND j.job_name=a.producer_job WHERE a.run_id=$1 AND a.producer_job=$6 AND a.name=$7 AND a.deleted_at IS NULL AND p.status='RUNNING' AND p.runner_id::text=$2 AND p.lease_id::text=$3 AND p.lease_generation=$4 AND p.lease_expires_at>now() AND j.status='PASSED' AND EXISTS(SELECT 1 FROM job_runs c WHERE c.run_id=p.id AND c.job_name=$5 AND c.status='RUNNING')`, owner.RunID, owner.RunnerID, owner.LeaseID, owner.Generation, owner.JobName, producer, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return a, err
}
func (s *Store) ListArtifacts(ctx context.Context, runID string) ([]store.Artifact, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+artifactColumns+` FROM artifacts a WHERE a.run_id=$1 ORDER BY a.producer_job,a.name`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []store.Artifact{}
	for rows.Next() {
		a, err := scanArtifact(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var exists bool
	if len(result) == 0 {
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_runs WHERE id=$1)`, runID).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, store.ErrNotFound
		}
	}
	return result, nil
}
func (s *Store) GetArtifact(ctx context.Context, runID, job, name string) (*store.Artifact, error) {
	a, err := scanArtifact(s.pool.QueryRow(ctx, `SELECT `+artifactColumns+` FROM artifacts a WHERE a.run_id=$1 AND a.producer_job=$2 AND a.name=$3`, runID, job, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return a, err
}
func (s *Store) SetArtifactExpiry(ctx context.Context, runID string, when time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE artifacts SET expires_at=$2 WHERE run_id=$1 AND expires_at IS NULL`, runID, when.UTC())
	return err
}
func (s *Store) ExpireArtifacts(ctx context.Context, now time.Time) ([]string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `UPDATE artifacts SET deleted_at=$1 WHERE deleted_at IS NULL AND expires_at IS NOT NULL AND expires_at<=$1 RETURNING blob_sha256`, now.UTC())
	if err != nil {
		return nil, err
	}
	candidates := map[string]struct{}{}
	for rows.Next() {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			rows.Close()
			return nil, err
		}
		candidates[strings.TrimSpace(digest)] = struct{}{}
	}
	rows.Close()
	var removable []string
	for digest := range candidates {
		var live bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifacts WHERE blob_sha256=$1 AND deleted_at IS NULL)`, digest).Scan(&live); err != nil {
			return nil, err
		}
		if !live {
			removable = append(removable, digest)
		}
	}
	sort.Strings(removable)
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return removable, nil
}

func (s *Store) LiveArtifactBlobs(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT blob_sha256 FROM artifacts WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]struct{}{}
	for rows.Next() {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			return nil, err
		}
		result[strings.TrimSpace(digest)] = struct{}{}
	}
	return result, rows.Err()
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
		if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, runID, s.artifactRetention.String()); err != nil {
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
