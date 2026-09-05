package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

const scmTriggerColumns = `id::text,delivery_id::text,repository_id::text,run_id::text,provider,commit_sha,COALESCE(ref,''),pull_request_number,COALESCE(installation_id,''),check_run_id,check_state,last_check_error,external_id,desired_check_status,desired_check_conclusion,last_check_conclusion,COALESCE(check_claim_token::text,''),COALESCE(check_claimed_by,''),check_claim_expires_at,next_check_attempt_at,check_attempt_count,created_at,updated_at`

func scanSCMTrigger(row pgx.Row) (*scm.RunTrigger, error) {
	var out scm.RunTrigger
	if err := row.Scan(&out.ID, &out.DeliveryID, &out.RepositoryID, &out.RunID, &out.Provider, &out.CommitSHA, &out.Ref, &out.PullRequestNumber, &out.InstallationID, &out.CheckRunID, &out.CheckState, &out.LastCheckError, &out.ExternalID, &out.DesiredCheckStatus, &out.DesiredCheckConclusion, &out.LastCheckConclusion, &out.CheckClaimToken, &out.CheckClaimedBy, &out.CheckClaimExpiresAt, &out.NextCheckAttemptAt, &out.CheckAttemptCount, &out.CreatedAt, &out.UpdatedAt); err != nil {
		return nil, err
	}
	out.CreatedAt, out.UpdatedAt = out.CreatedAt.UTC(), out.UpdatedAt.UTC()
	out.CheckClaimExpiresAt = utcTime(out.CheckClaimExpiresAt)
	out.NextCheckAttemptAt = utcTime(out.NextCheckAttemptAt)
	return &out, nil
}

func (s *Store) CreateSCMRunTrigger(ctx context.Context, in scm.RunTrigger) (*scm.RunTrigger, error) {
	if in.DeliveryID == "" || in.RepositoryID == "" || in.RunID == "" || in.Provider == "" || in.CommitSHA == "" {
		return nil, errors.New("invalid SCM run trigger")
	}
	if in.ID == "" {
		in.ID = uuid.NewString()
	}
	if in.ExternalID == "" {
		in.ExternalID = in.RunID
	}
	if in.DesiredCheckStatus == "" {
		in.DesiredCheckStatus = "queued"
	}
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, `INSERT INTO scm_run_triggers(id,delivery_id,repository_id,run_id,provider,commit_sha,ref,pull_request_number,installation_id,check_run_id,check_state,last_check_error,external_id,desired_check_status,desired_check_conclusion)
		VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,NULLIF($9,''),$10,$11,$12,$13,$14,$15) RETURNING `+scmTriggerColumns, in.ID, in.DeliveryID, in.RepositoryID, in.RunID, in.Provider, in.CommitSHA, in.Ref, in.PullRequestNumber, in.InstallationID, in.CheckRunID, in.CheckState, in.LastCheckError, in.ExternalID, in.DesiredCheckStatus, in.DesiredCheckConclusion))
	if err == nil {
		return out, nil
	}
	if !isUniqueViolation(err) {
		return nil, err
	}
	out, replayErr := s.GetSCMRunTriggerByDelivery(ctx, in.DeliveryID)
	if replayErr != nil {
		return nil, replayErr
	}
	if out.RunID != in.RunID {
		return nil, store.ErrConflict
	}
	return out, nil
}

// CreateSCMRun persists a normal run and its delivery association in one transaction.
func (s *Store) CreateSCMRun(ctx context.Context, claimToken string, runIn store.CreateRun, triggerIn scm.RunTrigger) (*store.Run, *scm.RunTrigger, error) {
	if claimToken == "" || triggerIn.DeliveryID == "" || runIn.ID == "" || triggerIn.RunID != runIn.ID {
		return nil, nil, errors.New("invalid SCM run")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)
	var owned bool
	if err := tx.QueryRow(ctx, `SELECT true FROM scm_deliveries WHERE id=$1 AND status='PROCESSING' AND claim_token=$2 AND claim_expires_at>now() FOR UPDATE`, triggerIn.DeliveryID, claimToken).Scan(&owned); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, store.ErrConflict
	} else if err != nil {
		return nil, nil, err
	}
	if err := createRun(ctx, tx, runIn); err != nil {
		return nil, nil, err
	}
	if triggerIn.ID == "" {
		triggerIn.ID = uuid.NewString()
	}
	if triggerIn.ExternalID == "" {
		triggerIn.ExternalID = triggerIn.RunID
	}
	if triggerIn.DesiredCheckStatus == "" {
		triggerIn.DesiredCheckStatus = "queued"
	}
	trigger, err := scanSCMTrigger(tx.QueryRow(ctx, `INSERT INTO scm_run_triggers(id,delivery_id,repository_id,run_id,provider,commit_sha,ref,pull_request_number,installation_id,check_run_id,check_state,last_check_error,external_id,desired_check_status,desired_check_conclusion)
		VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,NULLIF($9,''),$10,$11,$12,$13,$14,$15) RETURNING `+scmTriggerColumns,
		triggerIn.ID, triggerIn.DeliveryID, triggerIn.RepositoryID, triggerIn.RunID, triggerIn.Provider, triggerIn.CommitSHA, triggerIn.Ref, triggerIn.PullRequestNumber, triggerIn.InstallationID, triggerIn.CheckRunID, triggerIn.CheckState, triggerIn.LastCheckError, triggerIn.ExternalID, triggerIn.DesiredCheckStatus, triggerIn.DesiredCheckConclusion))
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	run, err := s.GetRun(ctx, runIn.ID)
	if err != nil {
		return nil, nil, err
	}
	return run, trigger, nil
}

func (s *Store) GetSCMRunTrigger(ctx context.Context, id string) (*scm.RunTrigger, error) {
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, `SELECT `+scmTriggerColumns+` FROM scm_run_triggers WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}

func (s *Store) GetSCMRunTriggerByDelivery(ctx context.Context, deliveryID string) (*scm.RunTrigger, error) {
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, `SELECT `+scmTriggerColumns+` FROM scm_run_triggers WHERE delivery_id=$1`, deliveryID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}

func (s *Store) GetSCMRunTriggerByRunID(ctx context.Context, runID string) (*scm.RunTrigger, error) {
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, `SELECT `+scmTriggerColumns+` FROM scm_run_triggers WHERE run_id=$1`, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}
