UPDATE service_admission_fences
SET expected_process_started_at = NULL
WHERE protocol_version = 1
  AND expected_process_started_at = 0;
