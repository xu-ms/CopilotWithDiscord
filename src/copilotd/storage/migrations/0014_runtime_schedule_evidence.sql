ALTER TABLE runtime_schedules ADD COLUMN recurring INTEGER;
ALTER TABLE runtime_schedules ADD COLUMN schedule_kind TEXT;
ALTER TABLE runtime_schedules ADD COLUMN display_prompt TEXT;
ALTER TABLE runtime_schedules ADD COLUMN prompt_hash TEXT;
ALTER TABLE runtime_schedules ADD COLUMN snapshot_id TEXT;
ALTER TABLE runtime_schedules ADD COLUMN observed_at REAL;
ALTER TABLE runtime_schedules ADD COLUMN terminal_at REAL;
ALTER TABLE runtime_schedules ADD COLUMN invocation_id TEXT
    REFERENCES runtime_command_invocations(invocation_id);

CREATE TABLE runtime_schedule_actions (
    action_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    operation_id TEXT REFERENCES session_operations(operation_id),
    invocation_id TEXT REFERENCES runtime_command_invocations(invocation_id),
    runtime_schedule_id TEXT,
    builtin_name TEXT NOT NULL,
    action TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    baseline_json TEXT,
    state TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    settled_at REAL
);

CREATE INDEX runtime_schedule_actions_session_idx
ON runtime_schedule_actions(sdk_session_id, runtime_schedule_id, action, state, created_at);
