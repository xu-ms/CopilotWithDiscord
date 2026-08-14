ALTER TABLE submissions ADD COLUMN discord_source_channel_id TEXT;
ALTER TABLE submissions ADD COLUMN discord_source_message_id TEXT;

CREATE TABLE submission_reactions (
    submission_id TEXT PRIMARY KEY REFERENCES submissions(submission_id) ON DELETE CASCADE,
    sdk_session_id TEXT NOT NULL,
    source_channel_id TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    desired_state TEXT NOT NULL,
    resume_state TEXT,
    delivered_state TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    delivered_revision INTEGER NOT NULL DEFAULT 0,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX submission_reactions_delivery_idx
ON submission_reactions(sdk_session_id, desired_state, delivered_state, updated_at);
