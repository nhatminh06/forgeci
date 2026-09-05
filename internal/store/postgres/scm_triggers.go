package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

const scmTriggerColumns = `id::text,delivery_id::text,repository_id::text,run_id::text,provider,commit_sha,COALESCE(ref,''),pull_request_number,COALESCE(installation_id,''),check_run_id,check_state,last_check_error,created_at,updated_at`

func scanSCMTrigger(row pgx.Row) (*scm.RunTrigger, error) {
	var out scm.RunTrigger
	if err := row.Scan(&out.ID, &out.DeliveryID, &out.RepositoryID, &out.RunID, &out.Provider, &out.CommitSHA, &out.Ref, &out.PullRequestNumber, &out.InstallationID, &out.CheckRunID, &out.CheckState, &out.LastCheckError, &out.CreatedAt, &out.UpdatedAt); err != nil {
		return nil, err
	}
	out.CreatedAt, out.UpdatedAt = out.CreatedAt.UTC(), out.UpdatedAt.UTC()
	return &out, nil
}

func (s *Store) CreateSCMRunTrigger(ctx context.Context, in scm.RunTrigger) (*scm.RunTrigger, error) {
	if in.DeliveryID == "" || in.RepositoryID == "" || in.RunID == "" || in.Provider == "" || in.CommitSHA == "" {
		return nil, errors.New("invalid SCM run trigger")
	}
	if in.ID == "" {
		in.ID = uuid.NewString()
	}
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, `INSERT INTO scm_run_triggers(id,delivery_id,repository_id,run_id,provider,commit_sha,ref,pull_request_number,installation_id,check_run_id,check_state,last_check_error)
		VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,NULLIF($9,''),$10,$11,$12) RETURNING `+scmTriggerColumns, in.ID, in.DeliveryID, in.RepositoryID, in.RunID, in.Provider, in.CommitSHA, in.Ref, in.PullRequestNumber, in.InstallationID, in.CheckRunID, in.CheckState, in.LastCheckError))
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
