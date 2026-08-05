CREATE TABLE global_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE channel_settings (
    channel_id TEXT PRIMARY KEY,
    layout TEXT NOT NULL DEFAULT 'text',
    mention_required INTEGER NOT NULL DEFAULT 0,
    config_version INTEGER NOT NULL DEFAULT 1,
    updated_at REAL NOT NULL
);

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    root_path TEXT NOT NULL,
    cwd TEXT NOT NULL,
    config_version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX projects_one_active_per_channel
ON projects(channel_id) WHERE state = 'active';

CREATE TABLE project_env (
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE mcp_servers (
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    transport TEXT NOT NULL,
    config_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE skill_dirs (
    project_id TEXT NOT NULL REFERENCES projects(id),
    path TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, path)
);

CREATE TABLE plugin_dirs (
    project_id TEXT NOT NULL REFERENCES projects(id),
    path TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, path)
);

CREATE TABLE custom_agents (
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    prompt TEXT NOT NULL,
    tools_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE session_creation_intents (
    creation_token TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    project_source TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    cwd_snapshot TEXT NOT NULL,
    sdk_session_id TEXT NOT NULL UNIQUE,
    thread_id TEXT,
    starter_message_id TEXT,
    attachment_manifest_id TEXT,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (source_kind, source_id)
);

CREATE TABLE session_bindings (
    thread_id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id),
    project_source TEXT NOT NULL,
    cwd_snapshot TEXT NOT NULL,
    sdk_session_id TEXT NOT NULL UNIQUE,
    binding_intent TEXT NOT NULL DEFAULT 'active',
    attachment_state TEXT NOT NULL DEFAULT 'absent',
    attachment_reason TEXT,
    permission_posture TEXT NOT NULL DEFAULT 'unverified',
    permission_verified_at REAL,
    desired_mode TEXT NOT NULL DEFAULT 'interactive',
    pending_mode TEXT,
    pending_mode_transition_id TEXT,
    runtime_mode TEXT NOT NULL DEFAULT 'unknown',
    desired_model_config TEXT NOT NULL DEFAULT '{}',
    pending_model_config TEXT,
    pending_model_transition_id TEXT,
    runtime_model_config TEXT,
    desired_agent TEXT NOT NULL DEFAULT 'default',
    pending_agent TEXT,
    pending_agent_transition_id TEXT,
    runtime_agent TEXT NOT NULL DEFAULT 'unknown',
    pending_remote_target TEXT,
    pending_remote_transition_id TEXT,
    runtime_remote_mode TEXT NOT NULL DEFAULT 'unknown',
    remote_url TEXT,
    desired_session_config_version INTEGER NOT NULL DEFAULT 1,
    pending_session_config_version INTEGER,
    runtime_session_config_version INTEGER,
    runtime_processing INTEGER,
    runtime_has_active_work INTEGER,
    runtime_abortable INTEGER,
    activity_observed_at REAL,
    runtime_generation INTEGER NOT NULL DEFAULT 0,
    owner_fence_token INTEGER,
    event_cursor TEXT,
    cursor_status TEXT,
    last_inbox_seq INTEGER NOT NULL DEFAULT 0,
    last_sdk_receive_seq INTEGER,
    last_event_at REAL,
    row_version INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX session_bindings_intent_idx
ON session_bindings(binding_intent, attachment_state);

CREATE TABLE session_owner_leases (
    sdk_session_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    fence_token INTEGER NOT NULL,
    acquired_at REAL NOT NULL,
    renewed_at REAL NOT NULL,
    expires_at REAL NOT NULL
);

CREATE TABLE session_operations (
    operation_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    kind TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    result_ref TEXT,
    error_code TEXT,
    started_at REAL,
    settled_at REAL,
    created_at REAL NOT NULL,
    UNIQUE (sdk_session_id, idempotency_key)
);

CREATE INDEX session_operations_pending_idx
ON session_operations(sdk_session_id, state);

CREATE TABLE reconciliation_state (
    sdk_session_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    requested_epoch INTEGER NOT NULL DEFAULT 0,
    applied_epoch INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'idle',
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    query_start_sdk_receive_seq INTEGER,
    query_end_sdk_receive_seq INTEGER,
    observed_at REAL,
    PRIMARY KEY (sdk_session_id, topic)
);

CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id),
    thread_id TEXT REFERENCES session_bindings(thread_id),
    kind TEXT NOT NULL,
    expression TEXT NOT NULL,
    timezone TEXT NOT NULL,
    payload TEXT NOT NULL,
    target_snapshot TEXT NOT NULL,
    misfire_policy TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'enabled',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE schedule_runs (
    run_id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    planned_key TEXT NOT NULL,
    planned_at_utc REAL NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    fence_token INTEGER,
    attempt INTEGER NOT NULL DEFAULT 0,
    claimed_at REAL,
    creation_intent_id TEXT,
    session_create_started_at REAL,
    send_started_at REAL,
    accepted_message_id TEXT,
    terminal_turn_id TEXT,
    completion_basis TEXT,
    result_thread_id TEXT,
    result_session_id TEXT,
    dispatch_key TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (schedule_id, planned_key)
);

CREATE INDEX schedule_runs_claim_idx
ON schedule_runs(status, planned_at_utc, lease_expires_at);

CREATE TABLE attachment_manifests (
    id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    session_id TEXT,
    state TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    retention_until REAL
);

CREATE TABLE attachment_items (
    manifest_id TEXT NOT NULL REFERENCES attachment_manifests(id),
    item_index INTEGER NOT NULL,
    discord_attachment_id TEXT,
    original_name TEXT NOT NULL,
    mime_type TEXT,
    byte_size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    local_path TEXT NOT NULL,
    sdk_attachment_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (manifest_id, item_index)
);

CREATE TABLE submissions (
    submission_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    source_operation_id TEXT REFERENCES session_operations(operation_id),
    parent_submission_id TEXT REFERENCES submissions(submission_id),
    discord_message_id TEXT,
    schedule_run_id TEXT UNIQUE REFERENCES schedule_runs(run_id),
    runtime_schedule_id TEXT,
    attachment_manifest_id TEXT REFERENCES attachment_manifests(id),
    prompt_hash TEXT,
    requested_mode TEXT,
    requested_model_config TEXT,
    requested_agent TEXT,
    requested_session_config_version INTEGER,
    requested_delivery TEXT,
    observed_delivery TEXT,
    state TEXT NOT NULL,
    accepted_message_id TEXT,
    native_queue_item_id TEXT,
    observed_user_event_id TEXT,
    observed_origin_hint TEXT,
    correlation_basis TEXT,
    autopilot_objective_id TEXT,
    task_completion_outcome TEXT,
    completion_basis TEXT,
    created_at REAL NOT NULL,
    idle_at REAL
);

CREATE INDEX submissions_active_idx
ON submissions(sdk_session_id, state);

CREATE TABLE model_turns (
    sdk_turn_id TEXT PRIMARY KEY,
    submission_id TEXT REFERENCES submissions(submission_id),
    agent_id TEXT,
    interaction_id TEXT,
    state TEXT NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL
);

CREATE TABLE message_queue (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES session_bindings(thread_id),
    discord_message_id TEXT,
    schedule_run_id TEXT REFERENCES schedule_runs(run_id),
    prompt TEXT NOT NULL,
    attachment_manifest_id TEXT REFERENCES attachment_manifests(id),
    requested_mode_snapshot TEXT NOT NULL,
    requested_model_config_snapshot TEXT NOT NULL,
    requested_agent_snapshot TEXT,
    requested_session_config_version INTEGER NOT NULL,
    position INTEGER NOT NULL,
    state TEXT NOT NULL,
    replaces_id TEXT REFERENCES message_queue(id),
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX message_queue_position_idx
ON message_queue(thread_id, position);

CREATE UNIQUE INDEX message_queue_schedule_nonterminal_idx
ON message_queue(schedule_run_id)
WHERE schedule_run_id IS NOT NULL
  AND state NOT IN ('cancelled', 'submitted', 'failed');

CREATE TABLE background_observations (
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    source_event_id TEXT NOT NULL,
    task_id TEXT,
    task_type TEXT,
    agent_id TEXT,
    observed_state TEXT NOT NULL,
    terminal_evidence TEXT,
    last_progress_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, runtime_generation, source_event_id)
);

CREATE TABLE task_card_projections (
    sdk_session_id TEXT NOT NULL,
    panel_id TEXT NOT NULL,
    card_token TEXT NOT NULL,
    card_key TEXT NOT NULL,
    task_id TEXT,
    agent_id TEXT,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_summary TEXT,
    detail_artifact TEXT,
    first_seen_at REAL NOT NULL,
    terminal_at REAL,
    revision INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (sdk_session_id, panel_id, card_key),
    UNIQUE (card_token)
);

CREATE TABLE liveness_leases (
    sdk_session_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    state TEXT NOT NULL,
    acquired_at REAL NOT NULL,
    refreshed_at REAL NOT NULL,
    released_at REAL,
    PRIMARY KEY (sdk_session_id, lease_id)
);

CREATE INDEX liveness_active_idx
ON liveness_leases(sdk_session_id, state, runtime_generation, owner_fence_token);

CREATE TABLE event_journal (
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sdk_session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    inbox_seq INTEGER NOT NULL,
    source TEXT NOT NULL,
    sdk_receive_seq INTEGER,
    event_id TEXT,
    internal_event_id TEXT,
    ephemeral INTEGER,
    persistence_class TEXT NOT NULL,
    raw_type TEXT NOT NULL,
    parent_id TEXT,
    agent_id TEXT,
    message_id TEXT,
    turn_id TEXT,
    interaction_id TEXT,
    request_id TEXT,
    reducer_hash TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    received_at REAL NOT NULL,
    UNIQUE (sdk_session_id, generation, inbox_seq)
);

CREATE UNIQUE INDEX event_journal_sdk_event_idx
ON event_journal(sdk_session_id, event_id)
WHERE event_id IS NOT NULL;

CREATE UNIQUE INDEX event_journal_internal_event_idx
ON event_journal(sdk_session_id, internal_event_id)
WHERE internal_event_id IS NOT NULL;

CREATE TABLE render_outbox (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    logical_seq INTEGER NOT NULL,
    lane TEXT NOT NULL,
    coalesce_key TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX render_outbox_ready_idx
ON render_outbox(state, next_attempt_at, session_id, logical_seq);

CREATE TABLE render_messages (
    session_id TEXT NOT NULL,
    logical_key TEXT NOT NULL,
    discord_message_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    finalized INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, logical_key)
);

CREATE TABLE pending_interactions (
    interaction_id TEXT PRIMARY KEY,
    protocol_request_id TEXT,
    sdk_session_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    response_plane TEXT NOT NULL,
    expires_at REAL NOT NULL,
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE protocol_requests (
    sdk_session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    requested_type TEXT NOT NULL,
    requested_event_id TEXT NOT NULL,
    completed_event_id TEXT,
    state TEXT NOT NULL,
    PRIMARY KEY (sdk_session_id, generation, request_id)
);

CREATE TABLE usage_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    turn_id TEXT,
    model TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    nano_aiu INTEGER,
    premium_requests REAL,
    observed_at REAL NOT NULL
);

CREATE TABLE runtime_schedules (
    sdk_session_id TEXT NOT NULL,
    runtime_schedule_id TEXT NOT NULL,
    builtin_name TEXT NOT NULL,
    invocation_input TEXT NOT NULL,
    recurrence TEXT,
    next_run_at REAL,
    state TEXT NOT NULL,
    last_event_id TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, runtime_schedule_id)
);

CREATE TABLE capabilities (
    runtime_version TEXT NOT NULL,
    sdk_version TEXT NOT NULL,
    capability TEXT NOT NULL,
    supported INTEGER NOT NULL,
    probe_detail TEXT NOT NULL,
    probed_at REAL NOT NULL,
    PRIMARY KEY (runtime_version, sdk_version, capability)
);

CREATE TABLE runtime_incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    runtime_generation INTEGER NOT NULL,
    session_id TEXT,
    kind TEXT NOT NULL,
    stderr_tail TEXT,
    last_inbox_seq INTEGER,
    last_sdk_receive_seq INTEGER,
    detail TEXT NOT NULL
);
