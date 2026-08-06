CREATE TABLE config_reload_claims (
    sdk_session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    claimed_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    state TEXT NOT NULL,
    config_version INTEGER,
    error_code TEXT,
    created_at REAL NOT NULL,
    settled_at REAL,
    PRIMARY KEY (sdk_session_id, idempotency_key)
);

CREATE INDEX config_reload_claims_state_idx
ON config_reload_claims(sdk_session_id, state);

ALTER TABLE agent_loop_projections
ADD COLUMN source_event_id TEXT;

ALTER TABLE session_bindings
ADD COLUMN desired_project_config_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE session_bindings
ADD COLUMN pending_project_config_version INTEGER;

ALTER TABLE session_bindings
ADD COLUMN runtime_project_config_version INTEGER;

UPDATE session_bindings
SET desired_project_config_version = CASE
        WHEN desired_session_config_hash IS NULL
        THEN desired_session_config_version
        ELSE 1
    END,
    pending_project_config_version = CASE
        WHEN pending_session_config_hash IS NULL
        THEN pending_session_config_version
        ELSE NULL
    END,
    runtime_project_config_version = CASE
        WHEN runtime_session_config_hash IS NULL
        THEN runtime_session_config_version
        ELSE NULL
    END,
    desired_session_config_version = CASE
        WHEN desired_session_config_hash IS NULL THEN 1
        ELSE desired_session_config_version
    END,
    pending_session_config_version = CASE
        WHEN pending_session_config_hash IS NULL THEN NULL
        ELSE pending_session_config_version
    END,
    runtime_session_config_version = CASE
        WHEN runtime_session_config_hash IS NULL THEN NULL
        ELSE runtime_session_config_version
    END;
