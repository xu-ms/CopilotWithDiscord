ALTER TABLE session_bindings
ADD COLUMN mode_reconciliation_state TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE session_bindings
ADD COLUMN mode_drift INTEGER NOT NULL DEFAULT 0;

ALTER TABLE session_bindings
ADD COLUMN model_confirmation_mask TEXT NOT NULL DEFAULT '[]';

ALTER TABLE session_bindings
ADD COLUMN model_reconciliation_state TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE session_bindings
ADD COLUMN model_drift INTEGER NOT NULL DEFAULT 0;

ALTER TABLE session_bindings
ADD COLUMN managed_settings_state TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE session_bindings
ADD COLUMN managed_permissions_blocked INTEGER NOT NULL DEFAULT 0;

CREATE TABLE model_config_observations (
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    model_id TEXT,
    reasoning_effort TEXT,
    reasoning_summary TEXT,
    context_tier TEXT,
    known_fields TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, event_id)
);

CREATE INDEX model_config_observations_generation_idx
ON model_config_observations(sdk_session_id, runtime_generation, observed_at);

CREATE TABLE extension_runtime_projections (
    sdk_session_id TEXT NOT NULL,
    extension_kind TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    state TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    source_event_id TEXT,
    observed_at REAL NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sdk_session_id, extension_kind)
);

CREATE TABLE mcp_server_projections (
    sdk_session_id TEXT NOT NULL,
    server_name TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    transport TEXT,
    state TEXT NOT NULL,
    error_code TEXT,
    detail_json TEXT NOT NULL,
    source_event_id TEXT,
    observed_at REAL NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sdk_session_id, server_name)
);
