ALTER TABLE scm_run_triggers
    ADD COLUMN external_id text,
    ADD COLUMN desired_check_status text NOT NULL DEFAULT 'queued',
    ADD COLUMN desired_check_conclusion text,
    ADD COLUMN last_check_conclusion text,
    ADD COLUMN check_claim_token uuid,
    ADD COLUMN check_claimed_by text,
    ADD COLUMN check_claim_expires_at timestamptz,
    ADD COLUMN next_check_attempt_at timestamptz,
    ADD COLUMN check_attempt_count integer NOT NULL DEFAULT 0;

UPDATE scm_run_triggers SET external_id=run_id::text WHERE external_id IS NULL;
ALTER TABLE scm_run_triggers ALTER COLUMN external_id SET NOT NULL;

CREATE INDEX scm_run_triggers_reconcile_idx
    ON scm_run_triggers(check_claim_expires_at, next_check_attempt_at, updated_at);
