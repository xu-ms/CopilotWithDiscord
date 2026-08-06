ALTER TABLE session_bindings ADD COLUMN remote_steerable INTEGER;
ALTER TABLE session_bindings ADD COLUMN remote_observed_at REAL;
ALTER TABLE session_bindings ADD COLUMN remote_snapshot_json TEXT;
ALTER TABLE session_bindings ADD COLUMN remote_observed_sdk_timestamp REAL;

CREATE TABLE runtime_remote_transitions (
    transition_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    previous_mode TEXT NOT NULL,
    target_mode TEXT NOT NULL,
    state TEXT NOT NULL,
    url TEXT,
    auth_json TEXT NOT NULL,
    repository_json TEXT NOT NULL,
    snapshot_json TEXT,
    event_id TEXT,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX runtime_remote_transitions_unsettled_idx
ON runtime_remote_transitions(sdk_session_id, state, created_at);
