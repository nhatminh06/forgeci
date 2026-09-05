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

const scmDeliveryColumns = `id::text,provider,delivery_id,repository_id::text,event_type,action,COALESCE(installation_id,''),COALESCE(commit_sha,''),COALESCE(ref,''),payload_sha256,pull_request_number,COALESCE(pull_request_head_ref,''),COALESCE(pull_request_base_ref,''),status,attempt_count,next_attempt_at,last_error,received_at,processed_at`

func scanSCMDelivery(row pgx.Row) (*scm.Delivery, error) {
	var out scm.Delivery
	if err := row.Scan(&out.ID, &out.Provider, &out.DeliveryID, &out.RepositoryID, &out.EventType, &out.Action, &out.InstallationID, &out.CommitSHA, &out.Ref, &out.PayloadSHA256, &out.PullRequestNumber, &out.PullRequestHeadRef, &out.PullRequestBaseRef, &out.Status, &out.AttemptCount, &out.NextAttemptAt, &out.LastError, &out.ReceivedAt, &out.ProcessedAt); err != nil {
		return nil, err
	}
	out.ReceivedAt, out.ProcessedAt = out.ReceivedAt.UTC(), out.ProcessedAt.UTC()
	out.NextAttemptAt = utcTime(out.NextAttemptAt)
	return &out, nil
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
	out, err := scanSCMDelivery(s.pool.QueryRow(ctx, `INSERT INTO scm_deliveries(id,provider,delivery_id,repository_id,event_type,action,installation_id,commit_sha,ref,pull_request_number,pull_request_head_ref,pull_request_base_ref,payload_sha256,status,attempt_count,next_attempt_at,last_error,received_at,processed_at)
		VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),$10,NULLIF($11,''),NULLIF($12,''),$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT(provider,delivery_id) DO NOTHING RETURNING `+scmDeliveryColumns, in.ID, in.Provider, in.DeliveryID, in.RepositoryID, in.EventType, in.Action, in.InstallationID, in.CommitSHA, in.Ref, in.PullRequestNumber, in.PullRequestHeadRef, in.PullRequestBaseRef, in.PayloadSHA256, in.Status, in.AttemptCount, in.NextAttemptAt, in.LastError, in.ReceivedAt, in.ProcessedAt))
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
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
