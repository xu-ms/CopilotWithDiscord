ALTER TABLE capabilities RENAME TO capabilities_v1;

CREATE TABLE capabilities (
    runtime_version TEXT NOT NULL,
    sdk_version TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    ping_protocol_version INTEGER NOT NULL,
    capability TEXT NOT NULL,
    supported INTEGER NOT NULL,
    evidence_kind TEXT NOT NULL,
    probe_detail TEXT NOT NULL,
    fixture_path TEXT NOT NULL,
    fixture_sha256 TEXT NOT NULL,
    generated_event_count INTEGER NOT NULL,
    event_types_sha256 TEXT NOT NULL,
    source TEXT NOT NULL,
    probed_at REAL NOT NULL,
    PRIMARY KEY (runtime_version, sdk_version, protocol_version, capability)
);

INSERT INTO capabilities(
    runtime_version, sdk_version, protocol_version, ping_protocol_version,
    capability, supported, evidence_kind, probe_detail, fixture_path,
    fixture_sha256, generated_event_count, event_types_sha256, source, probed_at
)
SELECT runtime_version, sdk_version, 0, 0, capability, supported,
       'legacy-unverified', probe_detail, '', '', 0, '', 'legacy', probed_at
FROM capabilities_v1;

DROP TABLE capabilities_v1;

ALTER TABLE event_journal ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE event_journal ADD COLUMN thread_id TEXT;
ALTER TABLE event_journal ADD COLUMN sdk_timestamp REAL;
ALTER TABLE event_journal ADD COLUMN task_id TEXT;
ALTER TABLE event_journal ADD COLUMN tool_call_id TEXT;
ALTER TABLE event_journal ADD COLUMN correlation_id TEXT;

ALTER TABLE submissions ADD COLUMN correlation_id TEXT;
ALTER TABLE submissions ADD COLUMN attachment_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN send_started_at REAL;
ALTER TABLE submissions ADD COLUMN accepted_at REAL;
ALTER TABLE submissions ADD COLUMN observed_at REAL;
ALTER TABLE submissions ADD COLUMN observed_interaction_id TEXT;
ALTER TABLE submissions ADD COLUMN objective_status TEXT;
ALTER TABLE submissions ADD COLUMN task_complete_event_id TEXT;
ALTER TABLE submissions ADD COLUMN abort_event_id TEXT;
ALTER TABLE submissions ADD COLUMN terminal_at REAL;
ALTER TABLE submissions ADD COLUMN continuation_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE model_turns ADD COLUMN sdk_session_id TEXT;
ALTER TABLE model_turns ADD COLUMN segment_index INTEGER;
ALTER TABLE model_turns ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE model_turns ADD COLUMN last_event_id TEXT;

CREATE TABLE submission_segments (
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id),
    segment_index INTEGER NOT NULL,
    user_event_id TEXT NOT NULL UNIQUE,
    interaction_id TEXT,
    is_continuation INTEGER NOT NULL,
    state TEXT NOT NULL,
    observed_at REAL NOT NULL,
    idle_at REAL,
    PRIMARY KEY (submission_id, segment_index)
);

CREATE TABLE autopilot_objectives (
    sdk_session_id TEXT NOT NULL,
    objective_id TEXT NOT NULL,
    submission_id TEXT REFERENCES submissions(submission_id),
    status TEXT,
    operation TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    updated_at REAL NOT NULL,
    deleted_at REAL,
    PRIMARY KEY (sdk_session_id, objective_id)
);

CREATE TABLE startup_recovery_runs (
    run_id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    completed_at REAL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL
);

ALTER TABLE reconciliation_state ADD COLUMN snapshot_id TEXT;
ALTER TABLE reconciliation_state ADD COLUMN last_positive_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE reconciliation_state ADD COLUMN uncertainty_reason TEXT;

CREATE TABLE snapshot_observations (
    snapshot_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    requested_epoch INTEGER NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    query_start_sdk_receive_seq INTEGER NOT NULL,
    query_end_sdk_receive_seq INTEGER NOT NULL,
    positive_evidence INTEGER NOT NULL,
    negative_applied INTEGER NOT NULL,
    observed_at REAL NOT NULL
);

CREATE INDEX snapshot_observations_topic_idx
ON snapshot_observations(sdk_session_id, topic, requested_epoch);

CREATE TABLE native_queue_items (
    sdk_session_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    agent_mode TEXT,
    display_text TEXT,
    state TEXT NOT NULL,
    last_snapshot_id TEXT NOT NULL,
    last_seen_epoch INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, item_id)
);

ALTER TABLE session_bindings ADD COLUMN event_cursor_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_bindings ADD COLUMN event_predecessor_id TEXT;

CREATE TABLE execution_health (
    sdk_session_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    last_progress_at REAL NOT NULL,
    suspect_since REAL,
    last_ping_at REAL,
    detail TEXT NOT NULL,
    updated_at REAL NOT NULL
);
