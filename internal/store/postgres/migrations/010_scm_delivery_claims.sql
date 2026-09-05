ALTER TABLE scm_deliveries
    ADD COLUMN claim_token uuid,
    ADD COLUMN claimed_by text,
    ADD COLUMN claim_expires_at timestamptz;

CREATE INDEX scm_deliveries_lease_idx
    ON scm_deliveries(status, claim_expires_at, next_attempt_at, received_at);
