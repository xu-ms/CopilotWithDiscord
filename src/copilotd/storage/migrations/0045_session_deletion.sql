ALTER TABLE session_bindings
ADD COLUMN delete_cleanup_state TEXT NOT NULL DEFAULT 'not_started';

ALTER TABLE session_bindings
ADD COLUMN delete_cleanup_error TEXT;

ALTER TABLE session_bindings
ADD COLUMN deleted_at REAL;
