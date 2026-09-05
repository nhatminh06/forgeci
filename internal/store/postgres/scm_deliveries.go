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

const scmDeliveryColumns = `id::text,provider,delivery_id,repository_id::text,event_type,action,COALESCE(installation_id,''),COALESCE(commit_sha,''),COALESCE(ref,''),payload_sha256,pull_request_number,COALESCE(pull_request_head_ref,''),COALESCE(pull_request_base_ref,''),status,attempt_count,next_attempt_at,last_error,COALESCE(claim_token::text,''),COALESCE(claimed_by,''),claim_expires_at,received_at,processed_at`

func scanSCMDelivery(row pgx.Row) (*scm.Delivery, error) {
	var out scm.Delivery
	if err := row.Scan(&out.ID, &out.Provider, &out.DeliveryID, &out.RepositoryID, &out.EventType, &out.Action, &out.InstallationID, &out.CommitSHA, &out.Ref, &out.PayloadSHA256, &out.PullRequestNumber, &out.PullRequestHeadRef, &out.PullRequestBaseRef, &out.Status, &out.AttemptCount, &out.NextAttemptAt, &out.LastError, &out.ClaimToken, &out.ClaimedBy, &out.ClaimExpiresAt, &out.ReceivedAt, &out.ProcessedAt); err != nil {
		return nil, err
	}
	out.ReceivedAt, out.ProcessedAt = out.ReceivedAt.UTC(), out.ProcessedAt.UTC()
	out.NextAttemptAt = utcTime(out.NextAttemptAt)
	out.ClaimExpiresAt = utcTime(out.ClaimExpiresAt)
	return &out, nil
}

func (s *Store) ClaimSCMDelivery(ctx context.Context, worker string, now time.Time, lease time.Duration) (*scm.Delivery, error) {
	if worker == "" || len(worker) > 256 || lease <= 0 {
		return nil, fmt.Errorf("invalid SCM delivery claim")
	}
	token := uuid.NewString()
	expires := now.UTC().Add(lease)
	query := `UPDATE scm_deliveries SET status='PROCESSING',attempt_count=attempt_count+1,
		claim_token=$2,claimed_by=$3,claim_expires_at=$4,next_attempt_at=NULL
	WHERE id=(SELECT id FROM scm_deliveries
		WHERE (status='PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= $1))
		   OR (status='FAILED' AND next_attempt_at IS NOT NULL AND next_attempt_at <= $1)
		   OR (status='PROCESSING' AND claim_expires_at <= $1)
		ORDER BY received_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
	RETURNING ` + scmDeliveryColumns
	out, err := scanSCMDelivery(s.pool.QueryRow(ctx, query, now.UTC(), token, worker, expires))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return out, err
}

func (s *Store) RenewSCMDeliveryClaim(ctx context.Context, id, token string, now time.Time, lease time.Duration) error {
	if id == "" || token == "" || lease <= 0 {
		return fmt.Errorf("invalid SCM delivery claim renewal")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE scm_deliveries SET claim_expires_at=$4
		WHERE id=$1 AND status='PROCESSING' AND claim_token=$2 AND claim_expires_at>$3`, id, token, now.UTC(), now.UTC().Add(lease))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) CompleteSCMDelivery(ctx context.Context, id, token string, status scm.DeliveryStatus) error {
	if status != scm.DeliveryProcessed && status != scm.DeliveryIgnored {
		return fmt.Errorf("invalid completed SCM delivery status")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE scm_deliveries SET status=$3,processed_at=now(),
		claim_token=NULL,claimed_by=NULL,claim_expires_at=NULL,next_attempt_at=NULL,last_error=NULL
		WHERE id=$1 AND status='PROCESSING' AND claim_token=$2 AND claim_expires_at>now()`, id, token, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) FailSCMDelivery(ctx context.Context, id, token string, retryAt *time.Time, message string) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE scm_deliveries SET status='FAILED',last_error=$3,next_attempt_at=$4,
		claim_token=NULL,claimed_by=NULL,claim_expires_at=NULL
		WHERE id=$1 AND status='PROCESSING' AND claim_token=$2 AND claim_expires_at>now()`, id, token, message, utcTime(retryAt))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) CreateSCMDelivery(ctx context.Context, in scm.Delivery) (*scm.Delivery, error) {
	if in.Provider == "" || in.DeliveryID == "" || len(in.DeliveryID) > 256 || len(in.PayloadSHA256) != 64 || in.RepositoryID == "" || in.EventType == "" || in.Status == "" {
		return nil, fmt.Errorf("invalid SCM delivery")
	}
	if in.ID == "" {
		in.ID = uuid.NewString()
	}
	if in.ReceivedAt.IsZero() {
		in.ReceivedAt = time.Now().UTC()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	out, err := scanSCMDelivery(tx.QueryRow(ctx, `INSERT INTO scm_deliveries(id,provider,delivery_id,repository_id,event_type,action,installation_id,commit_sha,ref,pull_request_number,pull_request_head_ref,pull_request_base_ref,payload_sha256,status,attempt_count,next_attempt_at,last_error,received_at,processed_at)
		VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),$10,NULLIF($11,''),NULLIF($12,''),$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT(provider,delivery_id) DO NOTHING RETURNING `+scmDeliveryColumns, in.ID, in.Provider, in.DeliveryID, in.RepositoryID, in.EventType, in.Action, in.InstallationID, in.CommitSHA, in.Ref, in.PullRequestNumber, in.PullRequestHeadRef, in.PullRequestBaseRef, in.PayloadSHA256, in.Status, in.AttemptCount, in.NextAttemptAt, in.LastError, in.ReceivedAt, in.ProcessedAt))
	if err == nil {
		if in.EventType == string(scm.EventPullRequest) && in.PullRequestNumber != nil && (in.Status == scm.DeliveryPending || in.Action == "closed") {
			_, err = tx.Exec(ctx, `UPDATE scm_deliveries SET status='IGNORED',processed_at=now(),last_error='superseded by newer pull request delivery',claim_token=NULL,claimed_by=NULL,claim_expires_at=NULL,next_attempt_at=NULL
				WHERE repository_id=$1 AND event_type='pull_request' AND pull_request_number=$2 AND id<>$3
				  AND status IN ('PENDING','PROCESSING','FAILED') AND ($4='closed' OR commit_sha IS DISTINCT FROM $5)`, in.RepositoryID, in.PullRequestNumber, in.ID, in.Action, in.CommitSHA)
			if err != nil {
				return nil, err
			}
			_, err = tx.Exec(ctx, `UPDATE pipeline_runs p SET
				status=CASE WHEN p.status='QUEUED' THEN 'CANCELED' ELSE p.status END,
				finished_at=CASE WHEN p.status='QUEUED' THEN now() ELSE p.finished_at END,
				cancel_requested_at=CASE WHEN p.status='RUNNING' THEN now() ELSE p.cancel_requested_at END
				FROM scm_run_triggers t WHERE t.run_id=p.id AND t.repository_id=$1 AND t.pull_request_number=$2
				  AND p.status IN ('QUEUED','RUNNING') AND ($3='closed' OR t.commit_sha IS DISTINCT FROM $4)`, in.RepositoryID, in.PullRequestNumber, in.Action, in.CommitSHA)
			if err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err := tx.Rollback(ctx); err != nil {
		return nil, err
	}
	out, err = s.GetSCMDeliveryByProviderDeliveryID(ctx, scm.Provider(in.Provider), in.DeliveryID)
	if err != nil {
		return nil, err
	}
	if out.PayloadSHA256 != in.PayloadSHA256 {
		return nil, store.ErrConflict
	}
	return out, nil
}

func (s *Store) GetSCMDelivery(ctx context.Context, id string) (*scm.Delivery, error) {
	out, err := scanSCMDelivery(s.pool.QueryRow(ctx, `SELECT `+scmDeliveryColumns+` FROM scm_deliveries WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}

func (s *Store) GetSCMDeliveryByProviderDeliveryID(ctx context.Context, provider scm.Provider, deliveryID string) (*scm.Delivery, error) {
	out, err := scanSCMDelivery(s.pool.QueryRow(ctx, `SELECT `+scmDeliveryColumns+` FROM scm_deliveries WHERE provider=$1 AND delivery_id=$2`, provider, deliveryID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}
