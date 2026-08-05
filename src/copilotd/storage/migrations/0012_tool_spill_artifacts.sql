CREATE TABLE tool_spill_artifacts (
    session_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    local_path TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    sha256 TEXT,
    finalized INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, tool_call_id)
);
