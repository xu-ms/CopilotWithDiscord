CREATE TABLE IF NOT EXISTS render_attachment_checkpoints (
    session_id TEXT NOT NULL,
    render_message_id TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    first_discord_message_id TEXT,
    next_batch_index INTEGER NOT NULL DEFAULT 0,
    finalized INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, render_message_id, agent_id)
);

CREATE TABLE IF NOT EXISTS render_attachment_batches (
    session_id TEXT NOT NULL,
    render_message_id TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    batch_index INTEGER NOT NULL,
    discord_message_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    attachment_count INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, render_message_id, agent_id, batch_index),
    UNIQUE (idempotency_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS render_attachment_batches_idempotency_idx
ON render_attachment_batches(idempotency_key);
