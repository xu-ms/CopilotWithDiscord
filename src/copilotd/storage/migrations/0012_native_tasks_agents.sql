ALTER TABLE session_bindings ADD COLUMN agent_observed_sdk_timestamp REAL;

CREATE TABLE runtime_task_actions (
    action_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    task_id TEXT,
    action TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX runtime_task_actions_session_idx
ON runtime_task_actions(sdk_session_id, task_id, action, state, created_at);

CREATE TABLE runtime_agent_manifest (
    sdk_session_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT,
    user_invocable INTEGER,
    metadata_json TEXT NOT NULL,
    manifest_generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    last_seen_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, agent_name)
);

CREATE INDEX runtime_agent_manifest_available_idx
ON runtime_agent_manifest(sdk_session_id, state, agent_name);

CREATE TABLE runtime_agent_transitions (
    transition_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    previous_agent TEXT NOT NULL,
    target_agent TEXT NOT NULL,
    state TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX runtime_agent_transitions_unsettled_idx
ON runtime_agent_transitions(sdk_session_id, state, created_at);
