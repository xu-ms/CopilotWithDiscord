CREATE TABLE turn_render_state (
    sdk_session_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    submission_id TEXT REFERENCES submissions(submission_id) ON DELETE CASCADE,
    segment_index INTEGER,
    state TEXT NOT NULL DEFAULT 'running',
    answer_payload TEXT,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, turn_key)
);

CREATE INDEX turn_render_state_submission_idx
ON turn_render_state(sdk_session_id, submission_id, segment_index);

UPDATE render_outbox
SET state = 'superseded', updated_at = strftime('%s', 'now')
WHERE state IN ('pending', 'sending', 'blocked')
  AND (
    lane IN ('diff', 'taskdeck')
    OR json_extract(payload, '$.type') IN (
        'diff', 'taskdeck', 'tool_output_artifact'
    )
  );
