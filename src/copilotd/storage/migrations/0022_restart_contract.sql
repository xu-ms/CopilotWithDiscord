CREATE TABLE restart_intents (
    restart_id TEXT PRIMARY KEY,
    requested_by TEXT NOT NULL,
    force INTEGER NOT NULL,
    state TEXT NOT NULL,
    blockers_json TEXT NOT NULL,
    affected_runs_json TEXT NOT NULL,
    requested_at REAL NOT NULL,
    completed_at REAL
);

CREATE TABLE scheduler_render_intents (
    run_id TEXT PRIMARY KEY REFERENCES schedule_runs(run_id),
    render_outbox_id TEXT NOT NULL UNIQUE REFERENCES render_outbox(id),
    terminal_status TEXT NOT NULL,
    completion_basis TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE worktree_recovery_runs (
    recovery_id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    completed_at REAL,
    examined_intents INTEGER NOT NULL DEFAULT 0,
    recovered_intents INTEGER NOT NULL DEFAULT 0,
    orphaned_intents INTEGER NOT NULL DEFAULT 0,
    detail TEXT NOT NULL
);
