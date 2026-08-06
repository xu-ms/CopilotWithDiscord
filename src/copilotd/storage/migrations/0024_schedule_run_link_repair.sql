UPDATE message_queue
SET schedule_run_id = NULL
WHERE schedule_run_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM schedule_runs r
      WHERE r.run_id = message_queue.schedule_run_id
        AND r.result_submission_id = message_queue.id
  );

UPDATE submissions
SET schedule_run_id = NULL
WHERE schedule_run_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM schedule_runs r
      WHERE r.run_id = submissions.schedule_run_id
        AND r.result_submission_id = submissions.submission_id
  );

UPDATE message_queue
SET schedule_run_id = (
    SELECT r.run_id FROM schedule_runs r
    WHERE r.result_submission_id = message_queue.id
)
WHERE EXISTS (
    SELECT 1 FROM schedule_runs r
    WHERE r.result_submission_id = message_queue.id
);

UPDATE submissions
SET schedule_run_id = (
    SELECT r.run_id FROM schedule_runs r
    WHERE r.result_submission_id = submissions.submission_id
)
WHERE EXISTS (
    SELECT 1 FROM schedule_runs r
    WHERE r.result_submission_id = submissions.submission_id
);
