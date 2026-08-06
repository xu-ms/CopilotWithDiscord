ALTER TABLE projects ADD COLUMN parent_project_id TEXT REFERENCES projects(id);
ALTER TABLE projects ADD COLUMN project_kind TEXT NOT NULL DEFAULT 'binding';
ALTER TABLE projects ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';
ALTER TABLE channel_settings ADD COLUMN timezone TEXT;

ALTER TABLE session_creation_intents ADD COLUMN project_snapshot_json TEXT;
ALTER TABLE session_creation_intents ADD COLUMN session_config_snapshot_json TEXT;
ALTER TABLE session_creation_intents ADD COLUMN worktree_intent_id TEXT;

ALTER TABLE session_bindings ADD COLUMN project_snapshot_json TEXT;
ALTER TABLE session_bindings ADD COLUMN session_config_snapshot_json TEXT;

CREATE TABLE project_config_revisions (
    project_id TEXT NOT NULL REFERENCES projects(id),
    config_version INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (project_id, config_version)
);

CREATE TABLE worktree_intents (
    intent_id TEXT PRIMARY KEY,
    parent_project_id TEXT NOT NULL REFERENCES projects(id),
    source_session_id TEXT,
    name TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    history_mode TEXT NOT NULL,
    target_path TEXT NOT NULL UNIQUE,
    project_id TEXT REFERENCES projects(id),
    thread_id TEXT,
    sdk_session_id TEXT,
    created_branch INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    error_code TEXT,
    error_detail TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (parent_project_id, name)
);

CREATE TABLE project_worktrees (
    worktree_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE REFERENCES worktree_intents(intent_id),
    parent_project_id TEXT NOT NULL REFERENCES projects(id),
    project_id TEXT NOT NULL UNIQUE REFERENCES projects(id),
    name TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    branch_name TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    history_mode TEXT NOT NULL,
    thread_id TEXT,
    sdk_session_id TEXT,
    created_branch INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    closed_at REAL,
    UNIQUE (parent_project_id, name)
);

CREATE INDEX project_worktrees_state_idx
ON project_worktrees(parent_project_id, state, name);

CREATE TABLE worktree_events (
    event_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES worktree_intents(intent_id),
    state TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX worktree_events_intent_idx
ON worktree_events(intent_id, created_at);
