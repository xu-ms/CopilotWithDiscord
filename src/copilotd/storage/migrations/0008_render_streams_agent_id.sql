-- Compatibility patch: rebuild render_streams with agent_id in the primary key.
CREATE TABLE render_streams__new (
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    finalized INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, message_id, agent_id)
);

INSERT INTO render_streams__new (
    session_id, message_id, agent_id, content, finalized, updated_at
)
SELECT session_id, message_id, '', content, finalized, updated_at
FROM render_streams;

DROP TABLE render_streams;
ALTER TABLE render_streams__new RENAME TO render_streams;
