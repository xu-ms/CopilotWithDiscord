CREATE TABLE hook_audit_events (
    audit_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    hook_name TEXT NOT NULL,
    hook_invocation_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    tool_name TEXT,
    tool_call_id TEXT,
    correlation_id TEXT,
    classification TEXT,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_at REAL NOT NULL,
    UNIQUE (
        sdk_session_id, runtime_generation, hook_name,
        hook_invocation_id, phase
    )
);

CREATE INDEX hook_audit_session_idx
ON hook_audit_events(sdk_session_id, runtime_generation, observed_at);

CREATE TABLE permission_audit_events (
    audit_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    request_id TEXT,
    permission_kind TEXT NOT NULL,
    managed_settings INTEGER NOT NULL,
    managed_approval_required INTEGER NOT NULL,
    decision TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    observed_at REAL NOT NULL
);

CREATE INDEX permission_audit_session_idx
ON permission_audit_events(sdk_session_id, runtime_generation, observed_at);

CREATE TABLE agent_loop_projections (
    sdk_session_id TEXT PRIMARY KEY,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    state TEXT NOT NULL,
    stop_reason TEXT,
    source_hook_audit_id TEXT REFERENCES hook_audit_events(audit_id),
    observed_at REAL NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE session_error_projections (
    sdk_session_id TEXT PRIMARY KEY,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    classification TEXT NOT NULL,
    recoverable INTEGER,
    correlation_id TEXT,
    source_hook_audit_id TEXT REFERENCES hook_audit_events(audit_id),
    observed_at REAL NOT NULL,
    stale INTEGER NOT NULL DEFAULT 0
);
