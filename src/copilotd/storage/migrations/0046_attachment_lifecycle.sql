ALTER TABLE attachment_manifests
ADD COLUMN source_channel_id TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN source_message_id TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN recovery_prompt TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN recovery_idempotency_key TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN recovery_origin TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN error_code TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN error_detail TEXT;

ALTER TABLE attachment_manifests
ADD COLUMN updated_at REAL NOT NULL DEFAULT 0;

UPDATE attachment_manifests
SET updated_at = created_at
WHERE updated_at = 0;

CREATE INDEX attachment_manifests_lifecycle_idx
ON attachment_manifests(state, retention_until, updated_at);
