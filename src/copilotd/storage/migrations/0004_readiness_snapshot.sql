ALTER TABLE session_bindings ADD COLUMN native_queue_count INTEGER;
ALTER TABLE session_bindings ADD COLUMN native_steering_count INTEGER;
ALTER TABLE session_bindings ADD COLUMN queue_observed_at REAL;
