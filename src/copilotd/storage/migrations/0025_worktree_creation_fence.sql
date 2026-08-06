ALTER TABLE worktree_intents ADD COLUMN git_create_holder TEXT;
ALTER TABLE worktree_intents ADD COLUMN git_create_fence_token INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worktree_intents ADD COLUMN git_create_lease_expires_at REAL;
