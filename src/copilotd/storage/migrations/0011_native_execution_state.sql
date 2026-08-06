CREATE TABLE ephemeral_queries (
    query_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    question_hash TEXT NOT NULL,
    history_count_before INTEGER,
    history_count_after INTEGER,
    sdk_receive_seq_before INTEGER,
    sdk_receive_seq_after INTEGER,
    answer_hash TEXT,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE TABLE compaction_runs (
    compaction_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    focus_hash TEXT,
    event_cursor_before TEXT,
    sdk_receive_seq_before INTEGER NOT NULL,
    context_before_json TEXT,
    result_json TEXT,
    context_after_json TEXT,
    start_event_id TEXT,
    start_sdk_receive_seq INTEGER,
    completion_event_id TEXT,
    completion_sdk_receive_seq INTEGER,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX compaction_runs_unsettled_idx
ON compaction_runs(sdk_session_id, state, created_at);

CREATE TABLE fleet_runs (
    fleet_run_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES session_operations(operation_id),
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions(submission_id),
    prompt_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX fleet_runs_session_idx
ON fleet_runs(sdk_session_id, state, created_at);
