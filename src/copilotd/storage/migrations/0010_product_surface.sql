CREATE TABLE session_ui_metadata (
    session_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL UNIQUE,
    parent_channel_id TEXT,
    display_name TEXT,
    native_name_state TEXT NOT NULL DEFAULT 'unsupported',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE session_projection_snapshots (
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    observed_at REAL NOT NULL,
    PRIMARY KEY (session_id, kind)
);

CREATE TABLE render_parent_diagnostics (
    idempotency_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    parent_channel_id TEXT,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    discord_message_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE pinned_message_provenance (
    discord_message_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    guild_id TEXT,
    author_id TEXT,
    jump_url TEXT,
    attachment_manifest_id TEXT,
    attachments_json TEXT NOT NULL DEFAULT '[]',
    pinned_at REAL NOT NULL
);

CREATE TABLE tool_output_streams (
    session_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    spilled INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (session_id, tool_call_id)
);
