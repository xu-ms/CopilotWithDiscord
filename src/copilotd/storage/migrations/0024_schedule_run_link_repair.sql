UPDATE schedule_runs AS run
SET result_submission_id = (
    SELECT submission_id
    FROM submissions
    WHERE schedule_run_id = run.run_id
    LIMIT 1
)
WHERE result_submission_id IS NULL
  AND (
      SELECT COUNT(*)
      FROM submissions
      WHERE schedule_run_id = run.run_id
  ) = 1;

UPDATE schedule_runs AS run
SET result_submission_id = (
    SELECT queue.id
    FROM message_queue AS queue
    JOIN submissions AS submission ON submission.submission_id = queue.id
    WHERE queue.schedule_run_id = run.run_id
    LIMIT 1
)
WHERE result_submission_id IS NULL
  AND (
      SELECT COUNT(*)
      FROM message_queue AS queue
      JOIN submissions AS submission ON submission.submission_id = queue.id
      WHERE queue.schedule_run_id = run.run_id
  ) = 1
  AND NOT EXISTS (
      SELECT 1
      FROM submissions
      WHERE schedule_run_id = run.run_id
        AND submission_id != (
            SELECT queue.id
            FROM message_queue AS queue
            JOIN submissions AS submission ON submission.submission_id = queue.id
            WHERE queue.schedule_run_id = run.run_id
            LIMIT 1
        )
  );

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
