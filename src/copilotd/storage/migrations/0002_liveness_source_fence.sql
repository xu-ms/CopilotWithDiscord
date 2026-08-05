UPDATE liveness_leases
SET state = 'orphaned',
    released_at = COALESCE(released_at, refreshed_at)
WHERE state = 'active'
  AND rowid NOT IN (
      SELECT MAX(rowid)
      FROM liveness_leases
      WHERE state = 'active'
      GROUP BY sdk_session_id, kind, source_id
  );

CREATE UNIQUE INDEX liveness_active_source_idx
ON liveness_leases(sdk_session_id, kind, source_id)
WHERE state = 'active';
