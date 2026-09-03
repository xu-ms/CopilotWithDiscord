CREATE TABLE tool_render_state (
    sdk_session_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id) ON DELETE CASCADE,
    segment_index INTEGER,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    sanitized_command TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_summary TEXT,
    failure_summary TEXT,
    started_seq INTEGER NOT NULL,
    updated_seq INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, turn_key, tool_call_id)
);

CREATE INDEX tool_render_state_current_idx
ON tool_render_state(sdk_session_id, turn_key, state, updated_seq);

ALTER TABLE render_outbox ADD COLUMN last_error TEXT;
