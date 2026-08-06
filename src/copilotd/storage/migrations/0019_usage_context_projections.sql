CREATE TABLE context_projections (
    sdk_session_id TEXT PRIMARY KEY,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    source_event_id TEXT,
    payload_json TEXT NOT NULL,
    observed_at REAL NOT NULL,
    reconciled_at REAL,
    stale INTEGER NOT NULL DEFAULT 0,
    stale_reason TEXT
);

CREATE TABLE usage_projections (
    sdk_session_id TEXT PRIMARY KEY,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    source_event_id TEXT,
    payload_json TEXT NOT NULL,
    observed_at REAL NOT NULL,
    reconciled_at REAL,
    stale INTEGER NOT NULL DEFAULT 0,
    stale_reason TEXT
);

CREATE TABLE session_limit_projections (
    sdk_session_id TEXT PRIMARY KEY,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    max_ai_credits REAL,
    used_ai_credits REAL,
    payload_json TEXT NOT NULL,
    source_event_id TEXT,
    observed_at REAL NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX usage_samples_session_idx
ON usage_samples(session_id, observed_at);
