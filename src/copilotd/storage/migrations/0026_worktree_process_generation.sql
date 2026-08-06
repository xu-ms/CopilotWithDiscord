ALTER TABLE worktree_intents ADD COLUMN git_create_process_generation INTEGER;
ALTER TABLE worktree_intents ADD COLUMN git_create_retry_at REAL;

CREATE TABLE worktree_process_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    process_owner_id TEXT,
    process_generation INTEGER NOT NULL DEFAULT 0,
    started_at REAL
);

INSERT INTO worktree_process_state(singleton, process_generation)
VALUES (1, 0);
