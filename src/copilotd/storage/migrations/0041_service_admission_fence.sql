CREATE TABLE service_admission_fences (
    fence_id TEXT PRIMARY KEY,
    expected_pid INTEGER NOT NULL,
    expected_generation TEXT NOT NULL,
    state TEXT NOT NULL,
    requested_at REAL NOT NULL,
    acknowledged_at REAL,
    committed_at REAL,
    released_at REAL,
    ingress_depth INTEGER,
    violation_count INTEGER NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX service_admission_fences_active_idx
ON service_admission_fences((1))
WHERE state IN ('requested', 'acknowledged', 'violated', 'committed');
