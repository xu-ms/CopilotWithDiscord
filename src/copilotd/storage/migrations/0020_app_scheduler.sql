ALTER TABLE schedules ADD COLUMN channel_id TEXT;
ALTER TABLE schedules ADD COLUMN name TEXT;
ALTER TABLE schedules ADD COLUMN created_by TEXT;
ALTER TABLE schedules ADD COLUMN normalized_expression TEXT;
ALTER TABLE schedules ADD COLUMN next_run_at_utc REAL;
ALTER TABLE schedules ADD COLUMN last_planned_at_utc REAL;
ALTER TABLE schedules ADD COLUMN planner_owner TEXT;
ALTER TABLE schedules ADD COLUMN planner_lease_expires_at REAL;
ALTER TABLE schedules ADD COLUMN planner_fence_token INTEGER NOT NULL DEFAULT 0;
ALTER TABLE schedules ADD COLUMN misfire_grace_seconds REAL;
ALTER TABLE schedules ADD COLUMN catch_up_limit INTEGER NOT NULL DEFAULT 1;
ALTER TABLE schedules ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE schedules ADD COLUMN deleted_at REAL;

UPDATE schedules
SET normalized_expression = CASE
        WHEN instr(expression, ':') > 0
        THEN trim(substr(expression, instr(expression, ':') + 1))
        ELSE trim(expression)
    END,
    next_run_at_utc = CASE
        WHEN state = 'enabled'
        THEN COALESCE(updated_at, created_at, 0)
        ELSE next_run_at_utc
    END;

CREATE INDEX schedules_due_idx
ON schedules(state, next_run_at_utc, planner_lease_expires_at);

ALTER TABLE schedule_runs ADD COLUMN retry_at REAL;
ALTER TABLE schedule_runs ADD COLUMN error_category TEXT;
ALTER TABLE schedule_runs ADD COLUMN error_code TEXT;
ALTER TABLE schedule_runs ADD COLUMN error_detail TEXT;
ALTER TABLE schedule_runs ADD COLUMN target_started_at REAL;
ALTER TABLE schedule_runs ADD COLUMN queued_at REAL;
ALTER TABLE schedule_runs ADD COLUMN accepted_at REAL;
ALTER TABLE schedule_runs ADD COLUMN waiting_at REAL;
ALTER TABLE schedule_runs ADD COLUMN render_intent_id TEXT REFERENCES render_outbox(id);
ALTER TABLE schedule_runs ADD COLUMN result_project_id TEXT REFERENCES projects(id);
ALTER TABLE schedule_runs ADD COLUMN result_submission_id TEXT REFERENCES submissions(submission_id);
ALTER TABLE schedule_runs ADD COLUMN last_progress_at REAL;
ALTER TABLE schedule_runs ADD COLUMN terminal_at REAL;
ALTER TABLE schedule_runs ADD COLUMN cancelled_at REAL;
ALTER TABLE schedule_runs ADD COLUMN temporary_attachment INTEGER NOT NULL DEFAULT 0;
ALTER TABLE schedule_runs ADD COLUMN target_released_at REAL;

CREATE INDEX schedule_runs_retry_idx
ON schedule_runs(status, retry_at, lease_expires_at, planned_at_utc);

CREATE TABLE scheduler_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner_id TEXT,
    worker_state TEXT NOT NULL,
    recovery_completed_at REAL,
    last_tick_at REAL,
    last_clock_utc REAL,
    last_wake_at REAL,
    paused_reason TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);

INSERT INTO scheduler_state(singleton, worker_state, updated_at)
VALUES (1, 'stopped', 0);

CREATE TABLE schedule_run_attempts (
    run_id TEXT NOT NULL REFERENCES schedule_runs(run_id),
    attempt INTEGER NOT NULL,
    fence_token INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    error_category TEXT,
    error_code TEXT,
    started_at REAL NOT NULL,
    settled_at REAL,
    PRIMARY KEY (run_id, attempt)
);

CREATE TABLE scheduler_events (
    event_id TEXT PRIMARY KEY,
    schedule_id TEXT REFERENCES schedules(id),
    run_id TEXT REFERENCES schedule_runs(run_id),
    event_type TEXT NOT NULL,
    owner_id TEXT,
    fence_token INTEGER,
    detail TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX scheduler_events_run_idx
ON scheduler_events(run_id, created_at);
