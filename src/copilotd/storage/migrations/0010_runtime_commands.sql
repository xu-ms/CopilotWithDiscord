CREATE TABLE runtime_command_refreshes (
    sdk_session_id TEXT PRIMARY KEY,
    manifest_generation INTEGER NOT NULL DEFAULT 0,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_event_id TEXT,
    error_code TEXT,
    refreshed_at REAL
);

CREATE TABLE runtime_command_manifest (
    sdk_session_id TEXT NOT NULL,
    command_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    description TEXT NOT NULL,
    aliases_json TEXT NOT NULL,
    allow_during_agent_execution INTEGER NOT NULL,
    experimental INTEGER NOT NULL,
    schedulable INTEGER NOT NULL,
    input_schema_json TEXT,
    manifest_generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    last_seen_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, command_name)
);

CREATE INDEX runtime_command_manifest_available_idx
ON runtime_command_manifest(sdk_session_id, state, kind, command_name);

CREATE TABLE runtime_command_invocations (
    invocation_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    command_name TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    result_kind TEXT,
    result_json TEXT,
    state TEXT NOT NULL,
    agent_submission_id TEXT REFERENCES submissions(submission_id),
    selection_token TEXT UNIQUE,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX runtime_command_invocations_session_idx
ON runtime_command_invocations(sdk_session_id, command_name, state, created_at);
