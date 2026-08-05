CREATE TABLE render_streams (
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    finalized INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, message_id)
);
