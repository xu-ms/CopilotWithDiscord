CREATE TABLE service_restart_intents (
    intent_id TEXT PRIMARY KEY,
    restart_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    sdk_session_id TEXT,
    target_id TEXT,
    state TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX service_restart_intents_restart_idx
ON service_restart_intents(restart_id, state, kind);
