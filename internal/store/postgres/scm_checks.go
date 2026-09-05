package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

func (s *Store) ClaimSCMCheck(ctx context.Context, worker string, now time.Time, lease time.Duration) (*scm.RunTrigger, error) {
	if worker == "" || lease <= 0 {
		return nil, fmt.Errorf("invalid SCM check claim")
	}
	_, err := s.pool.Exec(ctx, `UPDATE scm_run_triggers t SET
		desired_check_status=CASE p.status WHEN 'QUEUED' THEN 'queued' WHEN 'RUNNING' THEN 'in_progress' ELSE 'completed' END,
		desired_check_conclusion=CASE p.status WHEN 'PASSED' THEN 'success' WHEN 'FAILED' THEN 'failure' WHEN 'ERROR' THEN 'failure' WHEN 'CANCELED' THEN 'cancelled' WHEN 'ABORTED' THEN 'cancelled' ELSE NULL END,
		updated_at=CASE WHEN desired_check_status IS DISTINCT FROM CASE p.status WHEN 'QUEUED' THEN 'queued' WHEN 'RUNNING' THEN 'in_progress' ELSE 'completed' END
		 OR desired_check_conclusion IS DISTINCT FROM CASE p.status WHEN 'PASSED' THEN 'success' WHEN 'FAILED' THEN 'failure' WHEN 'ERROR' THEN 'failure' WHEN 'CANCELED' THEN 'cancelled' WHEN 'ABORTED' THEN 'cancelled' ELSE NULL END THEN now() ELSE updated_at END
		FROM pipeline_runs p WHERE p.id=t.run_id`)
	if err != nil {
		return nil, err
	}
	token := uuid.NewString()
	query := `UPDATE scm_run_triggers SET check_claim_token=$2,check_claimed_by=$3,check_claim_expires_at=$4,check_attempt_count=check_attempt_count+1
		WHERE id=(SELECT id FROM scm_run_triggers
			WHERE (check_claim_expires_at IS NULL OR check_claim_expires_at <= $1)
			  AND (next_check_attempt_at IS NULL OR next_check_attempt_at <= $1)
			  AND (check_run_id IS NULL OR check_state IS DISTINCT FROM desired_check_status OR last_check_conclusion IS DISTINCT FROM desired_check_conclusion)
			ORDER BY updated_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING ` + scmTriggerColumns
	out, err := scanSCMTrigger(s.pool.QueryRow(ctx, query, now.UTC(), token, worker, now.UTC().Add(lease)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return out, err
}

func (s *Store) CompleteSCMCheck(ctx context.Context, id, token, checkRunID, status string, conclusion *string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE scm_run_triggers SET check_run_id=$3,check_state=$4,last_check_conclusion=$5,last_check_error=NULL,
		check_claim_token=NULL,check_claimed_by=NULL,check_claim_expires_at=NULL,next_check_attempt_at=NULL,updated_at=now()
		WHERE id=$1 AND check_claim_token=$2 AND check_claim_expires_at>now()`, id, token, checkRunID, status, conclusion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) FailSCMCheck(ctx context.Context, id, token string, retryAt time.Time, message string) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE scm_run_triggers SET last_check_error=$3,next_check_attempt_at=$4,
		check_claim_token=NULL,check_claimed_by=NULL,check_claim_expires_at=NULL,updated_at=now()
		WHERE id=$1 AND check_claim_token=$2 AND check_claim_expires_at>now()`, id, token, message, retryAt.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}
