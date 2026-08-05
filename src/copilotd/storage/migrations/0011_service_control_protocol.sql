ALTER TABLE service_admission_fences
ADD COLUMN protocol_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE service_admission_fences
ADD COLUMN handoff_token_hash TEXT;

ALTER TABLE service_admission_fences
ADD COLUMN rollback_state TEXT NOT NULL DEFAULT 'none';

ALTER TABLE service_admission_fences
ADD COLUMN rollback_attempts INTEGER NOT NULL DEFAULT 0;

UPDATE service_admission_fences
SET rollback_state = CASE
    WHEN state = 'released' THEN 'complete'
    ELSE rollback_state
END;

UPDATE service_admission_fences
SET handoff_token_hash = ''
WHERE protocol_version = 1 AND handoff_token_hash IS NULL;

UPDATE service_admission_fences
SET expected_process_started_at = 0
WHERE protocol_version = 1 AND expected_process_started_at IS NULL;
