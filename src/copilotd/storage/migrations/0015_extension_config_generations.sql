CREATE TABLE project_extension_config_generations (
    scope_key TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    project_id TEXT REFERENCES projects(id),
    project_source TEXT NOT NULL,
    cwd_snapshot TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (scope_key, version)
);

CREATE INDEX project_extension_config_project_idx
ON project_extension_config_generations(project_id, version);

CREATE INDEX project_extension_config_hash_idx
ON project_extension_config_generations(scope_key, config_hash);

CREATE TABLE project_extension_env_refs (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    name TEXT NOT NULL,
    source_env TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, name),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

CREATE TABLE project_extension_mcp_servers (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    name TEXT NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('stdio', 'http')),
    config_json TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, name),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

CREATE TABLE project_extension_skill_dirs (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, path),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

CREATE TABLE project_extension_disabled_skills (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, name),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

CREATE TABLE project_extension_plugin_dirs (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, path),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

CREATE TABLE project_extension_custom_agents (
    scope_key TEXT NOT NULL,
    config_version INTEGER NOT NULL,
    name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    PRIMARY KEY (scope_key, config_version, name),
    FOREIGN KEY (scope_key, config_version)
        REFERENCES project_extension_config_generations(scope_key, version)
);

ALTER TABLE session_creation_intents
ADD COLUMN desired_session_config_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE session_creation_intents
ADD COLUMN desired_session_config_hash TEXT;

ALTER TABLE session_bindings
ADD COLUMN desired_session_config_hash TEXT;

ALTER TABLE session_bindings
ADD COLUMN pending_session_config_hash TEXT;

ALTER TABLE session_bindings
ADD COLUMN pending_session_config_transition_id TEXT;

ALTER TABLE session_bindings
ADD COLUMN runtime_session_config_hash TEXT;

ALTER TABLE session_bindings
ADD COLUMN session_config_state TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE session_bindings
ADD COLUMN session_config_drift INTEGER NOT NULL DEFAULT 0;
