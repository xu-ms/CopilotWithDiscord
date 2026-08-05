ALTER TABLE background_observations
ADD COLUMN submission_id TEXT REFERENCES submissions(submission_id);

ALTER TABLE task_card_projections
ADD COLUMN submission_id TEXT REFERENCES submissions(submission_id);

ALTER TABLE capabilities
ADD COLUMN evidence_status TEXT NOT NULL DEFAULT 'explicit';

CREATE TABLE submission_task_links (
    sdk_session_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id),
    objective_id TEXT,
    state TEXT NOT NULL,
    terminal_evidence TEXT,
    correlation_basis TEXT NOT NULL,
    linked_at REAL NOT NULL,
    last_progress_at REAL NOT NULL,
    terminal_at REAL,
    PRIMARY KEY (sdk_session_id, task_id)
);

CREATE INDEX submission_task_links_submission_idx
ON submission_task_links(submission_id, state, terminal_at);
