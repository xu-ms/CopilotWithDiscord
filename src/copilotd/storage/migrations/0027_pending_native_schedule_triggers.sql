CREATE TABLE pending_runtime_schedule_triggers (
    sdk_session_id TEXT NOT NULL,
    runtime_schedule_id TEXT NOT NULL,
    user_event_id TEXT NOT NULL,
    observed_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, runtime_schedule_id)
);
