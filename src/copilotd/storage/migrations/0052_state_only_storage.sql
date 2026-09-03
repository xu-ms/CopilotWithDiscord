PRAGMA secure_delete = ON;

CREATE TABLE state_only_cleanup (
    cleanup_key TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),
    completed_at REAL,
    updated_at REAL NOT NULL
);
INSERT INTO state_only_cleanup(cleanup_key, state, updated_at)
VALUES ('legacy_content_artifacts', 'pending', strftime('%s', 'now'));

CREATE TABLE state_only_cleanup_artifacts (
    artifact_id INTEGER PRIMARY KEY,
    managed_path TEXT,
    path_sha256 TEXT,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'removed', 'ignored_unmanaged')),
    removed_at REAL
);
CREATE UNIQUE INDEX state_only_cleanup_artifact_path_idx
ON state_only_cleanup_artifacts(managed_path)
WHERE managed_path IS NOT NULL;
INSERT OR IGNORE INTO state_only_cleanup_artifacts(managed_path)
SELECT local_path FROM tool_spill_artifacts WHERE local_path IS NOT NULL;
INSERT OR IGNORE INTO state_only_cleanup_artifacts(managed_path)
SELECT snapshot_path
FROM trusted_local_artifact_snapshots
WHERE snapshot_path IS NOT NULL;
INSERT OR IGNORE INTO state_only_cleanup_artifacts(managed_path)
SELECT local_path FROM attachment_items WHERE local_path IS NOT NULL;
INSERT OR IGNORE INTO state_only_cleanup_artifacts(managed_path)
SELECT local_path
FROM attachment_inline_variants
WHERE local_path IS NOT NULL;

UPDATE attachment_manifests
SET state = CASE
        WHEN state IN ('preparing', 'ready')
             AND source_channel_id IS NOT NULL
             AND source_message_id IS NOT NULL
             AND recovery_idempotency_key IS NOT NULL
             AND recovery_origin = 'discord_message'
        THEN 'preparing'
        WHEN state IN ('preparing', 'ready') THEN 'failed'
        ELSE state
    END,
    total_bytes = 0,
    retention_until = NULL,
    error_code = CASE
        WHEN state IN ('preparing', 'ready')
             AND source_channel_id IS NOT NULL
             AND source_message_id IS NOT NULL
             AND recovery_idempotency_key IS NOT NULL
             AND recovery_origin = 'discord_message'
        THEN 'source_refetch_required'
        WHEN state IN ('preparing', 'ready') THEN 'content_unavailable'
        ELSE error_code
    END,
    error_detail = NULL
WHERE EXISTS (
    SELECT 1 FROM attachment_items
    WHERE attachment_items.manifest_id = attachment_manifests.id
);
DELETE FROM attachment_inline_variants;
DELETE FROM attachment_items;

ALTER TABLE native_queue_items ADD COLUMN display_text_hash TEXT;
UPDATE native_queue_items
SET display_text = NULL,
    display_text_hash = NULL;

ALTER TABLE runtime_schedule_actions ADD COLUMN baseline_hash TEXT;
ALTER TABLE runtime_schedule_actions ADD COLUMN result_hash TEXT;
UPDATE runtime_schedule_actions
SET baseline_json = NULL,
    result_json = NULL,
    baseline_hash = NULL,
    result_hash = NULL;

ALTER TABLE attachment_manifests ADD COLUMN recovery_prompt_hash TEXT;
UPDATE attachment_manifests
SET recovery_prompt = NULL,
    recovery_prompt_hash = NULL;

ALTER TABLE event_journal ADD COLUMN payload_sha256 TEXT;
UPDATE event_journal
SET payload_sha256 = reducer_hash,
    raw_payload = '{"payload_state":"discarded","schema":1}';

ALTER TABLE render_outbox ADD COLUMN content_key TEXT;
ALTER TABLE render_outbox ADD COLUMN content_hash TEXT;
ALTER TABLE render_outbox ADD COLUMN render_kind TEXT;
ALTER TABLE render_outbox ADD COLUMN finalized INTEGER NOT NULL DEFAULT 0;
ALTER TABLE render_outbox ADD COLUMN source_submission_id TEXT;
ALTER TABLE render_outbox ADD COLUMN source_channel_id TEXT;
ALTER TABLE render_outbox ADD COLUMN source_message_id TEXT;
ALTER TABLE render_outbox ADD COLUMN tool_call_id TEXT;
ALTER TABLE render_outbox ADD COLUMN error_code TEXT;
ALTER TABLE render_outbox ADD COLUMN reaction_state TEXT;
ALTER TABLE render_outbox ADD COLUMN previous_reaction_state TEXT;
UPDATE render_outbox
SET render_kind = COALESCE(json_extract(payload, '$.type'), lane),
    finalized = COALESCE(json_extract(payload, '$.finalized'), 0),
    source_submission_id = json_extract(payload, '$.submission_id'),
    source_channel_id = json_extract(payload, '$.source_channel_id'),
    source_message_id = json_extract(payload, '$.source_message_id'),
    tool_call_id = json_extract(payload, '$.tool_call_id'),
    reaction_state = json_extract(payload, '$.state'),
    payload = '{"content_state":"unavailable","schema":1}',
    state = CASE
        WHEN state IN ('pending', 'sending', 'blocked')
             AND (
                 lane IN ('diff', 'taskdeck')
                 OR json_extract(payload, '$.type') IN (
                     'diff', 'taskdeck', 'tool_output_artifact'
                 )
             )
        THEN 'superseded'
        WHEN state IN ('pending', 'sending', 'blocked')
             AND lane NOT IN ('reaction', 'admission_reaction')
        THEN 'content_unavailable'
        ELSE state
    END,
    last_error = CASE
        WHEN state IN ('pending', 'sending', 'blocked')
             AND lane NOT IN ('reaction', 'admission_reaction')
             AND lane NOT IN ('diff', 'taskdeck')
             AND COALESCE(json_extract(payload, '$.type'), '') NOT IN (
                 'diff', 'taskdeck', 'tool_output_artifact'
             )
        THEN 'content_unavailable'
        ELSE NULL
    END,
    error_code = CASE
        WHEN state IN ('pending', 'sending', 'blocked')
             AND lane NOT IN ('reaction', 'admission_reaction')
             AND lane NOT IN ('diff', 'taskdeck')
             AND COALESCE(json_extract(payload, '$.type'), '') NOT IN (
                 'diff', 'taskdeck', 'tool_output_artifact'
             )
        THEN 'content_unavailable'
        ELSE NULL
    END;

INSERT OR IGNORE INTO render_outbox(
    id, session_id, logical_seq, lane, coalesce_key,
    idempotency_key, payload, state, attempts,
    next_attempt_at, created_at, updated_at,
    content_key, content_hash, render_kind, finalized,
    source_submission_id, error_code
)
SELECT
    'm52-content-unavailable:' || id,
    session_id,
    logical_seq,
    'status',
    COALESCE(coalesce_key, id),
    idempotency_key || ':content-unavailable:migration-52',
    json_object(
        'schema', 1,
        'render_kind', 'content_unavailable',
        'finalized', 1,
        'source_outbox_id', id,
        'submission_id', source_submission_id
    ),
    'pending',
    0,
    strftime('%s', 'now'),
    strftime('%s', 'now'),
    strftime('%s', 'now'),
    NULL,
    NULL,
    'content_unavailable',
    1,
    source_submission_id,
    'content_unavailable'
FROM render_outbox
WHERE state = 'content_unavailable'
  AND error_code = 'content_unavailable'
  AND lane NOT IN ('reaction', 'admission_reaction');

ALTER TABLE message_queue ADD COLUMN prompt_content_key TEXT;
ALTER TABLE message_queue ADD COLUMN prompt_hash TEXT;
UPDATE message_queue
SET prompt_hash = COALESCE(
        (SELECT submissions.prompt_hash
         FROM submissions
         WHERE submissions.submission_id = message_queue.id),
        prompt_hash
    ),
    prompt = '',
    state = CASE
        WHEN state IN (
            'local_queued', 'blocked_config_unknown', 'blocked_remote_transition',
            'blocked_mode_drift', 'blocked_model_drift', 'blocked_agent_drift',
            'blocked_session_config_drift'
        ) THEN 'content_unavailable'
        ELSE state
    END;
UPDATE submissions
SET state = 'rejected',
    completion_basis = 'content_unavailable',
    terminal_at = COALESCE(terminal_at, strftime('%s', 'now'))
WHERE submission_id IN (
    SELECT id FROM message_queue WHERE state = 'content_unavailable'
);
UPDATE liveness_leases
SET state = 'released',
    released_at = COALESCE(released_at, strftime('%s', 'now')),
    refreshed_at = strftime('%s', 'now')
WHERE kind = 'submission'
  AND source_id IN (
      SELECT id FROM message_queue WHERE state = 'content_unavailable'
  )
  AND state = 'active';
UPDATE submission_reactions
SET desired_state = 'failed',
    resume_state = 'content_unavailable',
    terminal = 1,
    revision = revision + 1,
    last_error = 'content_unavailable',
    updated_at = strftime('%s', 'now')
WHERE submission_id IN (
    SELECT id FROM message_queue WHERE state = 'content_unavailable'
);

ALTER TABLE pending_interactions ADD COLUMN content_key TEXT;
ALTER TABLE pending_interactions ADD COLUMN request_hash TEXT;
ALTER TABLE pending_interactions ADD COLUMN response_hash TEXT;
UPDATE pending_interactions
SET request_hash = NULL,
    payload = '{}',
    response = NULL,
    form_schema = NULL,
    state = CASE WHEN state = 'pending' THEN 'content_unavailable' ELSE state END;

ALTER TABLE protocol_requests ADD COLUMN requested_hash TEXT;
ALTER TABLE protocol_requests ADD COLUMN completed_hash TEXT;
UPDATE protocol_requests
SET requested_payload = NULL,
    completed_payload = NULL,
    response_payload = NULL,
    response_state = CASE
        WHEN response_state IN ('pending', 'responding')
        THEN 'content_unavailable'
        ELSE response_state
    END;

ALTER TABLE schedules ADD COLUMN source_channel_id TEXT;
ALTER TABLE schedules ADD COLUMN source_message_id TEXT;
ALTER TABLE schedules ADD COLUMN prompt_hash TEXT;
ALTER TABLE schedules ADD COLUMN target_snapshot_hash TEXT;
UPDATE schedules
SET payload = '{}',
    target_snapshot = CASE
        WHEN json_valid(target_snapshot)
        THEN json_remove(target_snapshot, '$.prompt', '$.text', '$.content')
        ELSE '{}'
    END,
    state = CASE
        WHEN state IN ('enabled', 'disabled') THEN 'needs_recreate'
        ELSE state
    END;
UPDATE schedule_runs
SET error_detail = NULL;

UPDATE runtime_schedules
SET invocation_input = '',
    display_prompt = NULL;

UPDATE turn_render_state SET answer_payload = NULL;
UPDATE tool_render_state
SET tool_name = '',
    sanitized_command = '',
    progress_summary = NULL,
    failure_summary = NULL;

ALTER TABLE turn_render_state RENAME TO turn_render_state_content_v51;
CREATE TABLE turn_render_state (
    sdk_session_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    submission_id TEXT REFERENCES submissions(submission_id) ON DELETE CASCADE,
    segment_index INTEGER,
    state TEXT NOT NULL DEFAULT 'running',
    runtime_generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, turn_key)
);
INSERT INTO turn_render_state(
    sdk_session_id, turn_key, submission_id, segment_index, state,
    runtime_generation, owner_fence_token, created_at, updated_at
)
SELECT sdk_session_id, turn_key, submission_id, segment_index, state,
       runtime_generation, owner_fence_token, created_at, updated_at
FROM turn_render_state_content_v51;
DROP TABLE turn_render_state_content_v51;
CREATE INDEX turn_render_state_submission_idx
ON turn_render_state(sdk_session_id, submission_id, segment_index);

ALTER TABLE tool_render_state RENAME TO tool_render_state_content_v51;
CREATE TABLE tool_render_state (
    sdk_session_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id) ON DELETE CASCADE,
    segment_index INTEGER,
    tool_call_id TEXT NOT NULL,
    state TEXT NOT NULL,
    started_seq INTEGER NOT NULL,
    updated_seq INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, turn_key, tool_call_id)
);
INSERT INTO tool_render_state(
    sdk_session_id, turn_key, submission_id, segment_index, tool_call_id,
    state, started_seq, updated_seq, created_at, updated_at
)
SELECT sdk_session_id, turn_key, submission_id, segment_index, tool_call_id,
       state, started_seq, updated_seq, created_at, updated_at
FROM tool_render_state_content_v51;
DROP TABLE tool_render_state_content_v51;
CREATE INDEX tool_render_state_current_idx
ON tool_render_state(sdk_session_id, turn_key, state, updated_seq);

UPDATE task_card_projections
SET title = '',
    progress_summary = NULL,
    detail_artifact = NULL,
    dependencies_json = '[]',
    artifact_links_json = '[]';
UPDATE session_projection_snapshots SET payload = '{}';
UPDATE context_projections SET payload_json = '{}', stale_reason = NULL;
UPDATE usage_projections SET payload_json = '{}', stale_reason = NULL;
UPDATE session_limit_projections SET payload_json = '{}';
UPDATE extension_runtime_projections SET detail_json = '{}';
UPDATE mcp_server_projections SET detail_json = '{}';

UPDATE runtime_incidents SET stderr_tail = NULL, detail = '{}';
UPDATE session_operations SET result_ref = NULL;
UPDATE capabilities SET probe_detail = '{}';
UPDATE execution_health SET detail = '{}';
UPDATE startup_recovery_runs SET detail = '{}';
UPDATE scheduler_events SET detail = '{}';
UPDATE service_restart_intents SET detail = '{}';
UPDATE worktree_recovery_runs SET detail = '{}';
UPDATE worktree_events SET detail = '{}';
UPDATE worktree_intents SET error_detail = NULL;
UPDATE service_admission_fences SET detail = '{}';

UPDATE hook_audit_events SET payload_json = '{}';
UPDATE runtime_command_invocations SET result_json = NULL;
ALTER TABLE runtime_command_invocations ADD COLUMN result_hash TEXT;
UPDATE runtime_task_actions SET result_json = NULL;
UPDATE runtime_agent_transitions SET result_json = NULL;
UPDATE runtime_remote_transitions SET snapshot_json = NULL;
UPDATE compaction_runs
SET context_before_json = NULL,
    result_json = NULL,
    context_after_json = NULL;
UPDATE fleet_runs SET result_json = NULL;

UPDATE session_ui_metadata SET display_name = NULL;
UPDATE session_bindings SET delete_cleanup_error = NULL;
UPDATE render_parent_diagnostics SET reason = 'render_unavailable';
UPDATE pinned_message_provenance SET attachments_json = '[]';
UPDATE attachment_manifests SET error_detail = NULL;

DROP TABLE IF EXISTS render_streams;
DROP TABLE IF EXISTS tool_output_streams;
DROP TABLE IF EXISTS tool_spill_artifacts;
DROP TABLE IF EXISTS trusted_local_artifact_snapshots;
DROP TABLE IF EXISTS trusted_local_artifacts;
DROP TABLE IF EXISTS tool_activity_projections;

CREATE TRIGGER state_only_event_journal_insert
BEFORE INSERT ON event_journal
WHEN COALESCE(json_extract(NEW.raw_payload, '$.payload_state'), '') != 'discarded'
BEGIN
    SELECT RAISE(ABORT, 'state_only:event_journal');
END;
CREATE TRIGGER state_only_event_journal_update
BEFORE UPDATE OF raw_payload ON event_journal
WHEN COALESCE(json_extract(NEW.raw_payload, '$.payload_state'), '') != 'discarded'
BEGIN
    SELECT RAISE(ABORT, 'state_only:event_journal');
END;

CREATE TRIGGER state_only_render_outbox_insert
BEFORE INSERT ON render_outbox
WHEN json_valid(NEW.payload) = 0
  OR json_type(NEW.payload, '$.content') IS NOT NULL
  OR json_type(NEW.payload, '$.embeds') IS NOT NULL
  OR json_type(NEW.payload, '$.attachments') IS NOT NULL
  OR json_type(NEW.payload, '$.command') IS NOT NULL
  OR json_type(NEW.payload, '$.tool') IS NOT NULL
  OR json_type(NEW.payload, '$.interaction') IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:render_outbox');
END;
CREATE TRIGGER state_only_render_outbox_update
BEFORE UPDATE OF payload ON render_outbox
WHEN json_valid(NEW.payload) = 0
  OR json_type(NEW.payload, '$.content') IS NOT NULL
  OR json_type(NEW.payload, '$.embeds') IS NOT NULL
  OR json_type(NEW.payload, '$.attachments') IS NOT NULL
  OR json_type(NEW.payload, '$.command') IS NOT NULL
  OR json_type(NEW.payload, '$.tool') IS NOT NULL
  OR json_type(NEW.payload, '$.interaction') IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:render_outbox');
END;

CREATE TRIGGER state_only_message_queue_insert
BEFORE INSERT ON message_queue
WHEN NEW.prompt != ''
BEGIN
    SELECT RAISE(ABORT, 'state_only:message_queue');
END;
CREATE TRIGGER state_only_message_queue_update
BEFORE UPDATE OF prompt ON message_queue
WHEN NEW.prompt != ''
BEGIN
    SELECT RAISE(ABORT, 'state_only:message_queue');
END;

CREATE TRIGGER state_only_native_queue_insert
BEFORE INSERT ON native_queue_items
WHEN NEW.display_text IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:native_queue_items');
END;
CREATE TRIGGER state_only_native_queue_update
BEFORE UPDATE OF display_text ON native_queue_items
WHEN NEW.display_text IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:native_queue_items');
END;

CREATE TRIGGER state_only_interactions_insert
BEFORE INSERT ON pending_interactions
WHEN NEW.payload != '{}' OR NEW.response IS NOT NULL OR NEW.form_schema IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:pending_interactions');
END;
CREATE TRIGGER state_only_interactions_update
BEFORE UPDATE OF payload, response, form_schema ON pending_interactions
WHEN NEW.payload != '{}' OR NEW.response IS NOT NULL OR NEW.form_schema IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:pending_interactions');
END;

CREATE TRIGGER state_only_protocol_insert
BEFORE INSERT ON protocol_requests
WHEN NEW.requested_payload IS NOT NULL
  OR NEW.completed_payload IS NOT NULL
  OR NEW.response_payload IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:protocol_requests');
END;
CREATE TRIGGER state_only_protocol_update
BEFORE UPDATE OF requested_payload, completed_payload, response_payload
ON protocol_requests
WHEN NEW.requested_payload IS NOT NULL
  OR NEW.completed_payload IS NOT NULL
  OR NEW.response_payload IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:protocol_requests');
END;

CREATE TRIGGER state_only_schedules_insert
BEFORE INSERT ON schedules
WHEN NEW.payload != '{}'
  OR json_type(NEW.target_snapshot, '$.prompt') IS NOT NULL
  OR json_type(NEW.target_snapshot, '$.text') IS NOT NULL
  OR json_type(NEW.target_snapshot, '$.content') IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:schedules');
END;
CREATE TRIGGER state_only_schedules_update
BEFORE UPDATE OF payload, target_snapshot ON schedules
WHEN NEW.payload != '{}'
  OR json_type(NEW.target_snapshot, '$.prompt') IS NOT NULL
  OR json_type(NEW.target_snapshot, '$.text') IS NOT NULL
  OR json_type(NEW.target_snapshot, '$.content') IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:schedules');
END;

CREATE TRIGGER state_only_runtime_schedules_insert
BEFORE INSERT ON runtime_schedules
WHEN NEW.invocation_input != '' OR NEW.display_prompt IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_schedules');
END;
CREATE TRIGGER state_only_runtime_schedules_update
BEFORE UPDATE OF invocation_input, display_prompt ON runtime_schedules
WHEN NEW.invocation_input != '' OR NEW.display_prompt IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_schedules');
END;

CREATE TRIGGER state_only_schedule_actions_insert
BEFORE INSERT ON runtime_schedule_actions
WHEN NEW.baseline_json IS NOT NULL OR NEW.result_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_schedule_actions');
END;
CREATE TRIGGER state_only_schedule_actions_update
BEFORE UPDATE OF baseline_json, result_json ON runtime_schedule_actions
WHEN NEW.baseline_json IS NOT NULL OR NEW.result_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_schedule_actions');
END;

CREATE TRIGGER state_only_task_cards_insert
BEFORE INSERT ON task_card_projections
WHEN NEW.title != '' OR NEW.progress_summary IS NOT NULL
  OR NEW.detail_artifact IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:task_card_projections');
END;
CREATE TRIGGER state_only_task_cards_update
BEFORE UPDATE OF title, progress_summary, detail_artifact ON task_card_projections
WHEN NEW.title != '' OR NEW.progress_summary IS NOT NULL
  OR NEW.detail_artifact IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:task_card_projections');
END;

CREATE TRIGGER state_only_attachments_insert
BEFORE INSERT ON attachment_items
WHEN NEW.original_name != ''
BEGIN
    SELECT RAISE(ABORT, 'state_only:attachment_items');
END;
CREATE TRIGGER state_only_attachments_update
BEFORE UPDATE OF original_name ON attachment_items
WHEN NEW.original_name != ''
BEGIN
    SELECT RAISE(ABORT, 'state_only:attachment_items');
END;

CREATE TRIGGER state_only_attachment_manifests_insert
BEFORE INSERT ON attachment_manifests
WHEN NEW.recovery_prompt IS NOT NULL OR NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:attachment_manifests');
END;
CREATE TRIGGER state_only_attachment_manifests_update
BEFORE UPDATE OF recovery_prompt, error_detail ON attachment_manifests
WHEN NEW.recovery_prompt IS NOT NULL OR NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:attachment_manifests');
END;

CREATE TRIGGER state_only_session_ui_insert
BEFORE INSERT ON session_ui_metadata
WHEN NEW.display_name IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:session_ui_metadata');
END;
CREATE TRIGGER state_only_session_ui_update
BEFORE UPDATE OF display_name ON session_ui_metadata
WHEN NEW.display_name IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:session_ui_metadata');
END;

CREATE TRIGGER state_only_operation_results_insert
BEFORE INSERT ON session_operations
WHEN NEW.result_ref IS NOT NULL AND NEW.result_ref NOT LIKE 'vc:%'
BEGIN
    SELECT RAISE(ABORT, 'state_only:session_operations');
END;
CREATE TRIGGER state_only_operation_results_update
BEFORE UPDATE OF result_ref ON session_operations
WHEN NEW.result_ref IS NOT NULL AND NEW.result_ref NOT LIKE 'vc:%'
BEGIN
    SELECT RAISE(ABORT, 'state_only:session_operations');
END;

CREATE TRIGGER state_only_runtime_command_results_insert
BEFORE INSERT ON runtime_command_invocations
WHEN NEW.result_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_command_invocations');
END;
CREATE TRIGGER state_only_runtime_command_results_update
BEFORE UPDATE OF result_json ON runtime_command_invocations
WHEN NEW.result_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_command_invocations');
END;

CREATE TRIGGER state_only_runtime_incidents_insert
BEFORE INSERT ON runtime_incidents
WHEN NEW.stderr_tail IS NOT NULL
  OR json_valid(NEW.detail) = 0
  OR instr(lower(NEW.detail), '"prompt"') > 0
  OR instr(lower(NEW.detail), '"content"') > 0
  OR instr(lower(NEW.detail), '"message"') > 0
  OR instr(lower(NEW.detail), '"command"') > 0
  OR instr(lower(NEW.detail), '"output"') > 0
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_incidents');
END;
CREATE TRIGGER state_only_runtime_incidents_update
BEFORE UPDATE OF stderr_tail, detail ON runtime_incidents
WHEN NEW.stderr_tail IS NOT NULL
  OR json_valid(NEW.detail) = 0
  OR instr(lower(NEW.detail), '"prompt"') > 0
  OR instr(lower(NEW.detail), '"content"') > 0
  OR instr(lower(NEW.detail), '"message"') > 0
  OR instr(lower(NEW.detail), '"command"') > 0
  OR instr(lower(NEW.detail), '"output"') > 0
BEGIN
    SELECT RAISE(ABORT, 'state_only:runtime_incidents');
END;

CREATE TRIGGER state_only_schedule_errors_insert
BEFORE INSERT ON schedule_runs
WHEN NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:schedule_runs');
END;
CREATE TRIGGER state_only_schedule_errors_update
BEFORE UPDATE OF error_detail ON schedule_runs
WHEN NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:schedule_runs');
END;

CREATE TRIGGER state_only_worktree_errors_insert
BEFORE INSERT ON worktree_intents
WHEN NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:worktree_intents');
END;
CREATE TRIGGER state_only_worktree_errors_update
BEFORE UPDATE OF error_detail ON worktree_intents
WHEN NEW.error_detail IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'state_only:worktree_intents');
END;
