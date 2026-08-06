ALTER TABLE protocol_requests RENAME TO protocol_requests_v1;

CREATE TABLE protocol_requests (
    sdk_session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    requested_type TEXT,
    requested_event_id TEXT,
    requested_payload TEXT,
    requested_at REAL,
    completed_type TEXT,
    completed_event_id TEXT,
    completed_payload TEXT,
    completed_at REAL,
    wire_state TEXT NOT NULL,
    response_plane TEXT NOT NULL,
    response_state TEXT NOT NULL,
    response_payload TEXT,
    response_attempt_id TEXT,
    responded_at REAL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (sdk_session_id, generation, request_id),
    CHECK (requested_event_id IS NOT NULL OR completed_event_id IS NOT NULL)
);

INSERT INTO protocol_requests(
    sdk_session_id, generation, request_id, requested_type,
    requested_event_id, completed_event_id, wire_state,
    response_plane, response_state, updated_at
)
SELECT sdk_session_id, generation, request_id, requested_type,
       requested_event_id, completed_event_id,
       CASE WHEN completed_event_id IS NULL THEN 'requested' ELSE 'paired' END,
       CASE requested_type
           WHEN 'session_limits_exhausted.requested' THEN 'app_rpc'
           WHEN 'sampling.requested' THEN 'app_rpc'
           WHEN 'mcp.headers_refresh_required' THEN 'app_rpc'
           WHEN 'permission.requested' THEN 'sdk_handler'
           WHEN 'external_tool.requested' THEN 'sdk_handler'
           WHEN 'elicitation.requested' THEN 'sdk_handler'
           WHEN 'mcp.oauth_required' THEN 'sdk_handler'
           WHEN 'user_input.requested' THEN 'direct_handler'
           WHEN 'exit_plan_mode.requested' THEN 'direct_handler'
           WHEN 'auto_mode_switch.requested' THEN 'direct_handler'
           ELSE 'journal'
       END,
       CASE
           WHEN requested_type IN (
               'session_limits_exhausted.requested',
               'sampling.requested',
               'mcp.headers_refresh_required'
           ) AND completed_event_id IS NULL THEN 'pending'
           WHEN requested_type IN (
               'session_limits_exhausted.requested',
               'sampling.requested',
               'mcp.headers_refresh_required'
           ) THEN 'completed'
           WHEN requested_type IN (
               'permission.requested',
               'external_tool.requested',
               'elicitation.requested',
               'mcp.oauth_required',
               'user_input.requested',
               'exit_plan_mode.requested',
               'auto_mode_switch.requested'
           ) THEN 'delegated'
           ELSE 'not_applicable'
       END,
       0
FROM protocol_requests_v1;

DROP TABLE protocol_requests_v1;

CREATE INDEX protocol_requests_pending_idx
ON protocol_requests(sdk_session_id, generation, response_plane, response_state);

CREATE TABLE protocol_response_attempts (
    attempt_id TEXT PRIMARY KEY,
    sdk_session_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    owner_fence_token INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    response_plane TEXT NOT NULL,
    response_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    error_code TEXT,
    started_at REAL NOT NULL,
    settled_at REAL,
    UNIQUE (sdk_session_id, generation, request_id, response_plane),
    FOREIGN KEY (sdk_session_id, generation, request_id)
        REFERENCES protocol_requests(sdk_session_id, generation, request_id)
);

ALTER TABLE pending_interactions
ADD COLUMN form_schema TEXT;

ALTER TABLE pending_interactions
ADD COLUMN response_attempt_id TEXT;

ALTER TABLE pending_interactions
ADD COLUMN sensitive_response INTEGER NOT NULL DEFAULT 0;
