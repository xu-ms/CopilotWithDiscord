CREATE TABLE render_batch_intents (
    session_id TEXT NOT NULL,
    render_message_id TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    batch_index INTEGER NOT NULL,
    nonce TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    discord_message_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, render_message_id, agent_id, batch_index),
    UNIQUE (nonce)
);

CREATE INDEX render_batch_intents_reconcile_idx
ON render_batch_intents(state, updated_at);
