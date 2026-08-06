ALTER TABLE worktree_intents ADD COLUMN git_child_pid INTEGER;
ALTER TABLE worktree_intents ADD COLUMN git_child_token TEXT;
ALTER TABLE worktree_intents ADD COLUMN git_child_process_generation INTEGER;
ALTER TABLE worktree_intents ADD COLUMN git_child_started_at REAL;
