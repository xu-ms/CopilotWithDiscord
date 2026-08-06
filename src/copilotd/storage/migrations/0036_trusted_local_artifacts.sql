CREATE TABLE trusted_local_artifacts (
    session_id TEXT NOT NULL,
    path TEXT NOT NULL,
    observed_at REAL NOT NULL,
    PRIMARY KEY (session_id, path)
);
