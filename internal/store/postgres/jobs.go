package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/store"
)

const jobLeaseTTL = 30 * time.Second

func (s *Store) LeaseJob(ctx context.Context, runnerID string) (*store.JobLease, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var docker bool
	var capacity, protocol int
	var status store.RunnerStatus
	if err = tx.QueryRow(ctx, `SELECT docker_available,max_parallel,protocol_version,status FROM runners WHERE id=$1 FOR UPDATE`, runnerID).Scan(&docker, &capacity, &protocol, &status); errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	if protocol != 2 || status != store.RunnerOnline {
		return nil, store.ErrConflict
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE runner_id=$1 AND status='RUNNING'`, runnerID).Scan(&active); err != nil {
		return nil, err
	}
	if active >= capacity {
		return nil, nil
	}
	var runID, jobName string
	var pipelineYAML []byte
	var source, blob, format string
	var archive, logical int64
	var entries int
	err = tx.QueryRow(ctx, `SELECT p.id::text,j.job_name,p.pipeline_yaml,s.source_digest,s.blob_digest,s.format,s.archive_size_bytes,s.logical_size_bytes,s.entry_count
		FROM pipeline_runs p JOIN job_runs j ON j.run_id=p.id JOIN source_snapshots s ON s.source_digest=p.source_snapshot_sha256
		WHERE p.status IN ('QUEUED','RUNNING') AND p.cancel_requested_at IS NULL AND j.status='PENDING'
		AND ($1 OR j.image IS NULL)
		AND NOT EXISTS (SELECT 1 FROM job_dependencies d JOIN job_runs u ON u.run_id=d.run_id AND u.job_name=d.depends_on_job WHERE d.run_id=j.run_id AND d.job_name=j.job_name AND u.status<>'PASSED')
		AND (SELECT count(*) FROM job_runs a WHERE a.run_id=p.id AND a.status='RUNNING') < p.max_parallel
		ORDER BY p.created_at,p.id,j.job_name LIMIT 1 FOR UPDATE OF p,j SKIP LOCKED`, docker).Scan(&runID, &jobName, &pipelineYAML, &source, &blob, &format, &archive, &logical, &entries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cfg, err := config.ParseBytes(pipelineYAML, "stored pipeline snapshot")
	if err != nil {
		return nil, err
	}
	job, ok := cfg.Jobs[jobName]
	if !ok {
		return nil, fmt.Errorf("leased job missing from pipeline")
	}
	leaseID := uuid.NewString()
	expires := time.Now().UTC().Add(jobLeaseTTL)
	tag, err := tx.Exec(ctx, `UPDATE job_runs SET status='RUNNING',runner_id=$3,lease_id=$4,lease_generation=lease_generation+1,lease_expires_at=$5,started_at=COALESCE(started_at,now()),finished_at=NULL,error_message=NULL WHERE run_id=$1 AND job_name=$2 AND status='PENDING'`, runID, jobName, runnerID, leaseID, expires)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return nil, err
		}
		return nil, store.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE pipeline_runs SET status='RUNNING',started_at=COALESCE(started_at,now()) WHERE id=$1 AND status='QUEUED'`, runID); err != nil {
		return nil, err
	}
	var generation int
	if err = tx.QueryRow(ctx, `SELECT lease_generation FROM job_runs WHERE run_id=$1 AND job_name=$2`, runID, jobName).Scan(&generation); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	def := store.JobDefinition{Image: job.Image}
	for _, step := range job.Steps {
		def.Steps = append(def.Steps, step.Run)
	}
	for _, u := range job.Artifacts.Upload {
		def.Uploads = append(def.Uploads, store.ArtifactDeclaration{Name: u.Name, Path: u.Path})
	}
	for _, d := range job.Artifacts.Download {
		def.Downloads = append(def.Downloads, store.ArtifactDownload{From: d.From, Name: d.Name, Into: d.Into})
	}
	return &store.JobLease{RunID: runID, JobName: jobName, RunnerID: runnerID, LeaseID: leaseID, Generation: generation, ExpiresAt: expires, Job: def, Snapshot: store.SourceSnapshot{SourceDigest: source, BlobDigest: blob, Format: format, ArchiveSizeBytes: archive, LogicalSizeBytes: logical, EntryCount: entries}}, nil
}

func (s *Store) HeartbeatJobLeases(ctx context.Context, runnerID string, leases []store.LeaseHeartbeat, now time.Time) ([]store.LeaseHeartbeatResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE runners SET last_seen_at=$2,status='ONLINE' WHERE id=$1`, runnerID, now.UTC())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, store.ErrNotFound
	}
	results := make([]store.LeaseHeartbeatResult, len(leases))
	for i, l := range leases {
		result := store.LeaseHeartbeatResult{RunID: l.RunID, JobName: l.JobName, LeaseID: l.LeaseID, Generation: l.Generation}
		var cancel bool
		expires := now.UTC().Add(jobLeaseTTL)
		err = tx.QueryRow(ctx, `UPDATE job_runs j SET lease_expires_at=$6 FROM pipeline_runs p WHERE j.run_id=p.id AND j.run_id=$1 AND j.job_name=$2 AND j.runner_id=$3 AND j.lease_id=$4 AND j.lease_generation=$5 AND j.status='RUNNING' AND j.lease_expires_at>$7 RETURNING p.cancel_requested_at IS NOT NULL`, l.RunID, l.JobName, runnerID, l.LeaseID, l.Generation, expires, now.UTC()).Scan(&cancel)
		if err == nil {
			result.Valid = true
			result.CancelRequested = cancel
			result.ExpiresAt = &expires
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		results[i] = result
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) CompleteJob(ctx context.Context, owner store.ArtifactOwnership, status store.JobStatus, message *string) error {
	if status != store.JobPassed && status != store.JobFailed && status != store.JobError && status != store.JobCanceled {
		return store.ErrConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var yaml []byte
	err = tx.QueryRow(ctx, `SELECT p.pipeline_yaml FROM job_runs j JOIN pipeline_runs p ON p.id=j.run_id WHERE j.run_id=$1 AND j.job_name=$2 AND j.runner_id=$3 AND j.lease_id=$4 AND j.lease_generation=$5 AND j.status='RUNNING' AND j.lease_expires_at>now() FOR UPDATE OF p,j`, owner.RunID, owner.JobName, owner.RunnerID, owner.LeaseID, owner.Generation).Scan(&yaml)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if status == store.JobPassed {
		ok, err := artifactSetCommittedTx(ctx, tx, yaml, owner.RunID, owner.JobName)
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrConflict
		}
	}
	_, err = tx.Exec(ctx, `UPDATE job_runs SET status=$3,finished_at=now(),error_message=$4,lease_id=NULL,lease_expires_at=NULL WHERE run_id=$1 AND job_name=$2`, owner.RunID, owner.JobName, status, message)
	if err != nil {
		return err
	}
	if status != store.JobPassed {
		if err = blockDependents(ctx, tx, owner.RunID); err != nil {
			return err
		}
	}
	if err = finalizeRun(ctx, tx, owner.RunID, s.artifactRetention); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func artifactSetCommittedTx(ctx context.Context, tx pgx.Tx, yaml []byte, runID, jobName string) (bool, error) {
	cfg, err := config.ParseBytes(yaml, "stored pipeline snapshot")
	if err != nil {
		return false, err
	}
	job, ok := cfg.Jobs[jobName]
	if !ok {
		return false, store.ErrNotFound
	}
	rows, err := tx.Query(ctx, `SELECT name FROM artifacts WHERE run_id=$1 AND producer_job=$2 AND deleted_at IS NULL ORDER BY name`, runID, jobName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
		actual = append(actual, n)
	}
	expected := make([]string, len(job.Artifacts.Upload))
	for i, u := range job.Artifacts.Upload {
		expected[i] = u.Name
	}
	sort.Strings(expected)
	if len(expected) != len(actual) {
		return false, nil
	}
	for i := range expected {
		if expected[i] != actual[i] {
			return false, nil
		}
	}
	return true, rows.Err()
}

func blockDependents(ctx context.Context, tx pgx.Tx, runID string) error {
	for {
		tag, err := tx.Exec(ctx, `UPDATE job_runs j SET status='BLOCKED',finished_at=now(),error_message='upstream dependency did not pass' WHERE j.run_id=$1 AND j.status='PENDING' AND EXISTS(SELECT 1 FROM job_dependencies d JOIN job_runs u ON u.run_id=d.run_id AND u.job_name=d.depends_on_job WHERE d.run_id=j.run_id AND d.job_name=j.job_name AND u.status IN ('FAILED','ERROR','BLOCKED','CANCELED','ABORTED'))`, runID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

func finalizeRun(ctx context.Context, tx pgx.Tx, runID string, retention time.Duration) error {
	var unfinished int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE run_id=$1 AND status IN ('PENDING','RUNNING')`, runID).Scan(&unfinished); err != nil {
		return err
	}
	if unfinished > 0 {
		return nil
	}
	var status store.RunStatus
	var cancel bool
	var hasError, hasAbort, hasFailure bool
	if err := tx.QueryRow(ctx, `SELECT p.cancel_requested_at IS NOT NULL,bool_or(j.status='ERROR'),bool_or(j.status='ABORTED'),bool_or(j.status IN ('FAILED','BLOCKED')) FROM pipeline_runs p JOIN job_runs j ON j.run_id=p.id WHERE p.id=$1 GROUP BY p.cancel_requested_at`, runID).Scan(&cancel, &hasError, &hasAbort, &hasFailure); err != nil {
		return err
	}
	switch {
	case hasError:
		status = store.RunError
	case hasAbort:
		status = store.RunAborted
	case cancel:
		status = store.RunCanceled
	case hasFailure:
		status = store.RunFailed
	default:
		status = store.RunPassed
	}
	tag, err := tx.Exec(ctx, `UPDATE pipeline_runs SET status=$2,finished_at=now(),error_message=CASE WHEN $2='ERROR' THEN 'job infrastructure failure' WHEN $2='ABORTED' THEN 'job execution ownership lost' ELSE error_message END WHERE id=$1 AND status IN ('QUEUED','RUNNING')`, runID, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, runID, retention.String())
	return err
}

func (s *Store) ExpireJobLeases(ctx context.Context, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `UPDATE job_runs SET status='ABORTED',finished_at=$1,error_message='job lease expired',lease_id=NULL,lease_expires_at=NULL WHERE status='RUNNING' AND lease_expires_at<=$1 RETURNING run_id::text`, now.UTC())
	if err != nil {
		return err
	}
	runs := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		runs[id] = struct{}{}
	}
	rows.Close()
	for id := range runs {
		if err = blockDependents(ctx, tx, id); err != nil {
			return err
		}
		if err = finalizeRun(ctx, tx, id, s.artifactRetention); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE runners SET status='OFFLINE' WHERE last_seen_at<$1`, now.UTC().Add(-jobLeaseTTL))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) RecoverRemoteJobs(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	legacy, err := tx.Query(ctx, `UPDATE pipeline_runs SET status='ABORTED',finished_at=now(),error_message='control plane restarted during legacy run lease',runner_id=NULL,lease_id=NULL,lease_expires_at=NULL,effective_parallel=NULL WHERE status='RUNNING' AND runner_id IS NOT NULL RETURNING id::text`)
	if err != nil {
		return err
	}
	var legacyIDs []string
	for legacy.Next() {
		var id string
		if err = legacy.Scan(&id); err != nil {
			return err
		}
		legacyIDs = append(legacyIDs, id)
	}
	legacy.Close()
	for _, id := range legacyIDs {
		if _, err = tx.Exec(ctx, `UPDATE job_runs SET status='ABORTED',finished_at=now(),error_message='legacy run lease ownership lost' WHERE run_id=$1 AND status IN ('PENDING','RUNNING')`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE artifacts SET expires_at=(SELECT finished_at+$2::interval FROM pipeline_runs WHERE id=$1) WHERE run_id=$1 AND expires_at IS NULL`, id, s.artifactRetention.String()); err != nil {
			return err
		}
	}
	rows, err := tx.Query(ctx, `UPDATE job_runs SET status='ABORTED',finished_at=now(),error_message='control plane restarted during job execution',lease_id=NULL,lease_expires_at=NULL WHERE status='RUNNING' RETURNING run_id::text`)
	if err != nil {
		return err
	}
	runs := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		runs[id] = struct{}{}
	}
	rows.Close()
	for id := range runs {
		if err = blockDependents(ctx, tx, id); err != nil {
			return err
		}
		if err = finalizeRun(ctx, tx, id, s.artifactRetention); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
