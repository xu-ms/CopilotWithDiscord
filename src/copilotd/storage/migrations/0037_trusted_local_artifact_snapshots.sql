CREATE TABLE trusted_local_artifact_snapshots (
    session_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    snapshot_path TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    observed_at REAL NOT NULL,
    PRIMARY KEY (session_id, source_path)
);

CREATE UNIQUE INDEX trusted_local_artifact_snapshot_path_idx
ON trusted_local_artifact_snapshots(snapshot_path);
