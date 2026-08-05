CREATE TABLE taskdeck_panel_state (
    sdk_session_id TEXT PRIMARY KEY,
    panel_id TEXT NOT NULL,
    selected_card_token TEXT,
    page INTEGER NOT NULL DEFAULT 0,
    expanded INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);
