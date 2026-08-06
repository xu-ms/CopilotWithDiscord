ALTER TABLE service_admission_fences
ADD COLUMN expected_process_started_at REAL;

ALTER TABLE service_admission_fences
ADD COLUMN producer_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE service_admission_fences
ADD COLUMN acknowledged_producer_count INTEGER;

ALTER TABLE service_admission_fences
ADD COLUMN baseline_journal_id INTEGER NOT NULL DEFAULT 0;

ALTER TABLE service_admission_fences
ADD COLUMN acknowledged_journal_id INTEGER;

ALTER TABLE service_admission_fences
ADD COLUMN force_prepared_at REAL;

ALTER TABLE service_admission_fences
ADD COLUMN owner_handoff_at REAL;

UPDATE service_admission_fences
SET acknowledged_producer_count = COALESCE(
        acknowledged_producer_count,
        producer_count
    ),
    owner_handoff_at = CASE
        WHEN state = 'committed' THEN COALESCE(
            owner_handoff_at,
            committed_at,
            requested_at
        )
        ELSE owner_handoff_at
    END,
    acknowledged_journal_id = COALESCE(
        acknowledged_journal_id,
        baseline_journal_id
    ),
    violation_count = violation_count + 1,
    detail = '{"reason":"legacy_fence_epoch_unknown"}'
WHERE state IN ('acknowledged', 'committed')
  AND (
    acknowledged_producer_count IS NULL
    OR acknowledged_journal_id IS NULL
  );

DROP INDEX service_admission_fences_active_idx;

CREATE UNIQUE INDEX service_admission_fences_active_idx
ON service_admission_fences((1))
WHERE state IN (
    'requested', 'acknowledged', 'violated', 'prepared', 'committed'
);

DROP INDEX message_queue_schedule_nonterminal_idx;

CREATE UNIQUE INDEX message_queue_schedule_nonterminal_idx
ON message_queue(schedule_run_id)
WHERE schedule_run_id IS NOT NULL
  AND state NOT IN (
    'cancelled', 'submitted', 'submitted_unknown', 'failed'
  );
