from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, TypeAlias
from urllib.parse import urlparse

from copilotd.core.projects import ProjectSnapshot, ProjectSource
from copilotd.storage.database import Database

_CONFIG_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
_REASONING_EFFORTS = {"low", "medium", "high", "xhigh", "max"}
DEFAULT_EXTENSION_CONFIG_PATH = Path(".copilotd/extensions.json")
MAX_EXTENSION_CONFIG_BYTES = 1024 * 1024


class ExtensionConfigError(ValueError):
    pass


class ExtensionConfigConflict(ExtensionConfigError):
    pass


class MissingEnvironmentReference(ExtensionConfigError):
    pass


class ConfigReloadState(StrEnum):
    CLAIMED = "claimed"
    STARTED = "started"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ConfigReloadClaim:
    sdk_session_id: str
    idempotency_key: str
    config_hash: str
    claimed_generation: int
    owner_fence_token: int
    state: ConfigReloadState
    config_version: int | None
    error_code: str | None


@dataclass(frozen=True, slots=True)
class EnvironmentReference:
    name: str
    source_env: str

    def __post_init__(self) -> None:
        _validate_name(self.name, "environment reference")
        if not self.source_env or "=" in self.source_env or "\x00" in self.source_env:
            raise ExtensionConfigError(f"invalid source environment name: {self.source_env!r}")


@dataclass(frozen=True, slots=True)
class EnvironmentBinding:
    name: str
    reference: str

    def __post_init__(self) -> None:
        if not self.name or "=" in self.name or "\x00" in self.name:
            raise ExtensionConfigError(f"invalid environment binding name: {self.name!r}")
        _validate_name(self.reference, "environment reference")


@dataclass(frozen=True, slots=True)
class HeaderBinding:
    name: str
    reference: str

    def __post_init__(self) -> None:
        if (
            not self.name
            or ":" in self.name
            or "\r" in self.name
            or "\n" in self.name
            or "\x00" in self.name
        ):
            raise ExtensionConfigError(f"invalid HTTP header name: {self.name!r}")
        _validate_name(self.reference, "environment reference")


@dataclass(frozen=True, slots=True)
class McpStdioServer:
    name: str
    command: str
    args: tuple[str, ...] = ()
    environment: tuple[EnvironmentBinding, ...] = ()
    working_directory: str | None = None
    tools: tuple[str, ...] = ("*",)
    timeout_ms: int | None = None
    transport: Literal["stdio"] = "stdio"

    def __post_init__(self) -> None:
        _validate_name(self.name, "MCP server")
        if not self.command or "\x00" in self.command:
            raise ExtensionConfigError("stdio MCP command must be non-empty")
        if self.timeout_ms is not None and self.timeout_ms <= 0:
            raise ExtensionConfigError("MCP timeout must be positive")
        _require_unique((item.name for item in self.environment), "MCP environment binding")
        _require_unique(self.tools, "MCP tool")


@dataclass(frozen=True, slots=True)
class McpHttpServer:
    name: str
    url: str
    headers: tuple[HeaderBinding, ...] = ()
    tools: tuple[str, ...] = ("*",)
    timeout_ms: int | None = None
    transport: Literal["http"] = "http"

    def __post_init__(self) -> None:
        _validate_name(self.name, "MCP server")
        parsed = urlparse(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ExtensionConfigError(f"invalid HTTP MCP URL: {self.url!r}")
        if parsed.username is not None or parsed.password is not None:
            raise ExtensionConfigError("MCP URLs must not embed credentials")
        if self.timeout_ms is not None and self.timeout_ms <= 0:
            raise ExtensionConfigError("MCP timeout must be positive")
        _require_unique((item.name.lower() for item in self.headers), "MCP header")
        _require_unique(self.tools, "MCP tool")


McpServer: TypeAlias = McpStdioServer | McpHttpServer


@dataclass(frozen=True, slots=True)
class CustomAgent:
    name: str
    prompt: str
    description: str = ""
    display_name: str | None = None
    tools: tuple[str, ...] | None = None
    skills: tuple[str, ...] = ()
    mcp_server_names: tuple[str, ...] = ()
    infer: bool | None = None
    model: str | None = None
    reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        _validate_name(self.name, "custom agent")
        if not self.prompt.strip():
            raise ExtensionConfigError("custom agent prompt must be non-empty")
        if self.reasoning_effort not in _REASONING_EFFORTS | {None}:
            raise ExtensionConfigError(
                f"unsupported custom-agent reasoning effort: {self.reasoning_effort}"
            )
        if self.tools is not None:
            _require_unique(self.tools, "custom-agent tool")
        _require_unique(self.skills, "custom-agent skill")
        _require_unique(self.mcp_server_names, "custom-agent MCP server")


@dataclass(frozen=True, slots=True)
class ProjectExtensionConfig:
    environment_references: tuple[EnvironmentReference, ...] = ()
    mcp_servers: tuple[McpServer, ...] = ()
    skill_directories: tuple[str, ...] = ()
    disabled_skills: tuple[str, ...] = ()
    plugin_directories: tuple[str, ...] = ()
    custom_agents: tuple[CustomAgent, ...] = ()

    def __post_init__(self) -> None:
        _require_unique(
            (item.name for item in self.environment_references),
            "environment reference",
        )
        _require_unique((item.name for item in self.mcp_servers), "MCP server")
        _require_unique(self.skill_directories, "skill directory")
        _require_unique(self.disabled_skills, "disabled skill")
        _require_unique(self.plugin_directories, "plugin directory")
        _require_unique((item.name for item in self.custom_agents), "custom agent")
        references = {item.name for item in self.environment_references}
        server_names = {item.name for item in self.mcp_servers}
        for server in self.mcp_servers:
            bindings = server.environment if isinstance(server, McpStdioServer) else server.headers
            missing = sorted(
                item.reference for item in bindings if item.reference not in references
            )
            if missing:
                raise ExtensionConfigError(
                    f"MCP server {server.name} references unknown environment values: "
                    + ", ".join(missing)
                )
        for agent in self.custom_agents:
            missing_servers = sorted(set(agent.mcp_server_names) - server_names)
            if missing_servers:
                raise ExtensionConfigError(
                    f"custom agent {agent.name} references unknown MCP servers: "
                    + ", ".join(missing_servers)
                )

    def normalized(self, cwd: Path) -> ProjectExtensionConfig:
        resolved_cwd = cwd.expanduser().resolve()
        return replace(
            self,
            skill_directories=tuple(
                str(_normalize_directory(value, resolved_cwd)) for value in self.skill_directories
            ),
            plugin_directories=tuple(
                str(_normalize_directory(value, resolved_cwd)) for value in self.plugin_directories
            ),
            mcp_servers=tuple(
                replace(
                    server,
                    working_directory=str(
                        _normalize_directory(server.working_directory, resolved_cwd)
                    ),
                )
                if isinstance(server, McpStdioServer) and server.working_directory is not None
                else server
                for server in self.mcp_servers
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment_references": [
                {"name": item.name, "source_env": item.source_env}
                for item in self.environment_references
            ],
            "mcp_servers": [_mcp_to_dict(item) for item in self.mcp_servers],
            "skill_directories": list(self.skill_directories),
            "disabled_skills": list(self.disabled_skills),
            "plugin_directories": list(self.plugin_directories),
            "custom_agents": [_agent_to_dict(item) for item in self.custom_agents],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ProjectExtensionConfig:
        try:
            _reject_unknown_keys(
                payload,
                {
                    "environment_references",
                    "mcp_servers",
                    "skill_directories",
                    "disabled_skills",
                    "plugin_directories",
                    "custom_agents",
                },
                "extension config",
            )
            environment_items = _mapping_list(payload.get("environment_references", []))
            for item in environment_items:
                _reject_unknown_keys(
                    item,
                    {"name", "source_env"},
                    "environment reference",
                )
            environment_references = tuple(
                EnvironmentReference(
                    name=str(item["name"]),
                    source_env=str(item["source_env"]),
                )
                for item in environment_items
            )
            mcp_servers = tuple(
                _mcp_from_dict(item) for item in _mapping_list(payload.get("mcp_servers", []))
            )
            custom_agents = tuple(
                _agent_from_dict(item) for item in _mapping_list(payload.get("custom_agents", []))
            )
            return cls(
                environment_references=environment_references,
                mcp_servers=mcp_servers,
                skill_directories=_string_tuple(payload.get("skill_directories", [])),
                disabled_skills=_string_tuple(payload.get("disabled_skills", [])),
                plugin_directories=_string_tuple(payload.get("plugin_directories", [])),
                custom_agents=custom_agents,
            )
        except (KeyError, TypeError, ValueError) as error:
            if isinstance(error, ExtensionConfigError):
                raise
            raise ExtensionConfigError("invalid extension configuration payload") from error

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ExtensionConfigSnapshot:
    scope_key: str
    version: int
    project_id: str | None
    project_source: str
    cwd_snapshot: Path
    config_hash: str
    config: ProjectExtensionConfig

    def sdk_session_options(
        self,
        environ: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        values = os.environ if environ is None else environ
        resolved_references = {
            item.name: _resolve_environment_value(item, values)
            for item in self.config.environment_references
        }
        mcp_servers = {
            server.name: _mcp_sdk_config(server, resolved_references)
            for server in self.config.mcp_servers
        }
        custom_agents: list[dict[str, Any]] = []
        for agent in self.config.custom_agents:
            payload: dict[str, Any] = {
                "name": agent.name,
                "prompt": agent.prompt,
            }
            if agent.display_name is not None:
                payload["display_name"] = agent.display_name
            if agent.description:
                payload["description"] = agent.description
            if agent.tools is not None:
                payload["tools"] = list(agent.tools)
            if agent.skills:
                payload["skills"] = list(agent.skills)
            if agent.mcp_server_names:
                payload["mcp_servers"] = {
                    name: mcp_servers[name] for name in agent.mcp_server_names
                }
            if agent.infer is not None:
                payload["infer"] = agent.infer
            if agent.model is not None:
                payload["model"] = agent.model
            if agent.reasoning_effort is not None:
                payload["reasoning_effort"] = agent.reasoning_effort
            custom_agents.append(payload)
        return {
            "enable_config_discovery": False,
            "custom_agents_local_only": True,
            "enable_file_hooks": False,
            "enable_skills": bool(self.config.skill_directories or self.config.disabled_skills),
            "skill_directories": list(self.config.skill_directories),
            "disabled_skills": list(self.config.disabled_skills),
            "plugin_directories": list(self.config.plugin_directories),
            "mcp_servers": mcp_servers,
            "mcp_oauth_token_storage": "in-memory",
            "custom_agents": custom_agents,
        }

    def dynamic_headers(
        self,
        server_name: str,
        environ: Mapping[str, str] | None = None,
    ) -> dict[str, str] | None:
        server = next(
            (
                item
                for item in self.config.mcp_servers
                if item.name == server_name and isinstance(item, McpHttpServer)
            ),
            None,
        )
        if server is None or not server.headers:
            return None
        values = os.environ if environ is None else environ
        references = {
            item.name: _resolve_environment_value(item, values)
            for item in self.config.environment_references
        }
        return {item.name: references[item.reference] for item in server.headers}


class ExtensionConfigFileSource:
    """Loads one strict, secret-free project config from the project snapshot."""

    def __init__(
        self,
        relative_path: Path = DEFAULT_EXTENSION_CONFIG_PATH,
        *,
        max_bytes: int = MAX_EXTENSION_CONFIG_BYTES,
    ) -> None:
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ExtensionConfigError("extension config path must stay within the project")
        if max_bytes < 1:
            raise ExtensionConfigError("extension config size limit must be positive")
        self.relative_path = relative_path
        self.max_bytes = max_bytes

    def path_for(self, project: ProjectSnapshot) -> Path:
        return (project.cwd / self.relative_path).resolve()

    async def load(self, project: ProjectSnapshot) -> ProjectExtensionConfig:
        return await asyncio.to_thread(self._load_sync, project)

    def _load_sync(self, project: ProjectSnapshot) -> ProjectExtensionConfig:
        path = project.cwd / self.relative_path
        if path.is_symlink():
            raise ExtensionConfigError("extension config file cannot be a symlink")
        project_root = project.cwd.resolve()
        resolved = path.resolve()
        if not resolved.is_relative_to(project_root):
            raise ExtensionConfigError("extension config resolves outside the project")
        if not path.exists():
            return ProjectExtensionConfig()
        stat = path.stat()
        if not path.is_file():
            raise ExtensionConfigError(f"extension config is not a file: {path}")
        if stat.st_size > self.max_bytes:
            raise ExtensionConfigError(f"extension config exceeds {self.max_bytes} bytes")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ExtensionConfigError(f"extension config could not be read: {path}") from error
        if not isinstance(payload, Mapping):
            raise ExtensionConfigError("extension config root must be an object")
        return ProjectExtensionConfig.from_dict(payload)


class ExtensionConfigRepository:
    """Publishes immutable project extension generations and resolves session pins."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def publish(
        self,
        project: ProjectSnapshot,
        config: ProjectExtensionConfig,
        *,
        expected_current_version: int | None = None,
        now: float | None = None,
    ) -> ExtensionConfigSnapshot:
        normalized = config.normalized(project.cwd)
        encoded = normalized.canonical_json()
        config_hash = hashlib.sha256(encoded.encode()).hexdigest()
        scope_key = extension_scope_key(project.source.value, project.project_id)
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            version = await _publish_config_in_transaction(
                connection,
                project=project,
                config=normalized,
                config_hash=config_hash,
                encoded=encoded,
                expected_current_version=expected_current_version,
                timestamp=timestamp,
            )
        return ExtensionConfigSnapshot(
            scope_key=scope_key,
            version=version,
            project_id=project.project_id,
            project_source=project.source.value,
            cwd_snapshot=project.cwd,
            config_hash=config_hash,
            config=normalized,
        )

    async def latest(self, project: ProjectSnapshot) -> ExtensionConfigSnapshot:
        scope_key = extension_scope_key(project.source.value, project.project_id)
        row = await self._database.fetchone(
            """
            SELECT * FROM project_extension_config_generations
            WHERE scope_key = ? ORDER BY version DESC LIMIT 1
            """,
            (scope_key,),
        )
        if row is None:
            return await self.publish(project, ProjectExtensionConfig())
        return _snapshot_from_row(row)

    async def ingest(
        self,
        project: ProjectSnapshot,
        source: ExtensionConfigFileSource,
        *,
        expected_current_version: int | None = None,
    ) -> ExtensionConfigSnapshot:
        config = await source.load(project)
        return await self.publish(
            project,
            config,
            expected_current_version=expected_current_version,
        )

    async def for_session(
        self,
        *,
        project_source: str,
        project_id: str | None,
        cwd_snapshot: Path,
        version: int,
    ) -> ExtensionConfigSnapshot:
        scope_key = extension_scope_key(project_source, project_id)
        row = await self._database.fetchone(
            """
            SELECT * FROM project_extension_config_generations
            WHERE scope_key = ? AND version = ?
            """,
            (scope_key, version),
        )
        if row is None and version == 1:
            source = (
                ProjectSource.EXPLICIT
                if project_source == ProjectSource.EXPLICIT.value
                else ProjectSource.IMPLICIT_HOME
            )
            project = ProjectSnapshot(
                project_id=project_id,
                channel_id="session-config-bootstrap",
                source=source,
                root_path=cwd_snapshot,
                cwd=cwd_snapshot,
                config_version=1,
            )
            return await self.publish(project, ProjectExtensionConfig())
        if row is None:
            raise ExtensionConfigError(
                f"extension config generation {scope_key}@{version} does not exist"
            )
        snapshot = _snapshot_from_row(row)
        if snapshot.cwd_snapshot != cwd_snapshot:
            raise ExtensionConfigError(
                "session cwd does not match its immutable extension config generation"
            )
        return snapshot


class ConfigReloadClaimStore:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def find(
        self,
        *,
        sdk_session_id: str,
        idempotency_key: str,
        config_hash: str,
    ) -> ConfigReloadClaim | None:
        row = await self._database.fetchone(
            """
            SELECT * FROM config_reload_claims
            WHERE sdk_session_id = ? AND idempotency_key = ?
            """,
            (sdk_session_id, idempotency_key),
        )
        if row is None:
            return None
        claim = _row_to_reload_claim(row)
        if claim.config_hash != config_hash:
            raise ExtensionConfigConflict(
                "config reload idempotency key was reused with a different config hash"
            )
        return claim

    async def claim(
        self,
        *,
        sdk_session_id: str,
        idempotency_key: str,
        config_hash: str,
        runtime_generation: int,
        owner_fence_token: int,
        now: float | None = None,
    ) -> tuple[ConfigReloadClaim, bool]:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM config_reload_claims
                WHERE sdk_session_id = ? AND idempotency_key = ?
                """,
                (sdk_session_id, idempotency_key),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                await connection.execute(
                    """
                    INSERT INTO config_reload_claims(
                        sdk_session_id, idempotency_key, config_hash,
                        claimed_generation, owner_fence_token, state, created_at
                    ) VALUES (?, ?, ?, ?, ?, 'claimed', ?)
                    """,
                    (
                        sdk_session_id,
                        idempotency_key,
                        config_hash,
                        runtime_generation,
                        owner_fence_token,
                        timestamp,
                    ),
                )
                row = await _fetch_reload_claim(
                    connection,
                    sdk_session_id,
                    idempotency_key,
                )
                return _row_to_reload_claim(row), True
        claim = _row_to_reload_claim(row)
        if claim.config_hash != config_hash:
            raise ExtensionConfigConflict(
                "config reload idempotency key was reused with a different config hash"
            )
        return claim, False

    async def claim_and_publish(
        self,
        *,
        sdk_session_id: str,
        idempotency_key: str,
        project: ProjectSnapshot,
        config: ProjectExtensionConfig,
        owner_id: str,
        runtime_generation: int,
        owner_fence_token: int,
        transition_id: str,
        minimum_headroom_seconds: float,
        expected_current_version: int | None = None,
        now: float | None = None,
    ) -> tuple[ConfigReloadClaim, ExtensionConfigSnapshot, bool]:
        timestamp = time.time() if now is None else now
        normalized = config.normalized(project.cwd)
        encoded = normalized.canonical_json()
        config_hash = hashlib.sha256(encoded.encode()).hexdigest()
        created = False
        async with self._database.transaction() as connection:
            owner_cursor = await connection.execute(
                """
                SELECT 1 FROM session_owner_leases
                WHERE sdk_session_id = ? AND owner_id = ? AND fence_token = ?
                  AND expires_at - ? >= ?
                """,
                (
                    sdk_session_id,
                    owner_id,
                    owner_fence_token,
                    timestamp,
                    minimum_headroom_seconds,
                ),
            )
            owner = await owner_cursor.fetchone()
            await owner_cursor.close()
            if owner is None:
                raise ExtensionConfigConflict("owner fence is not current for config publication")
            binding_cursor = await connection.execute(
                """
                SELECT desired_session_config_version,
                       desired_session_config_hash,
                       pending_session_config_version,
                       pending_session_config_hash,
                       pending_session_config_transition_id,
                       runtime_session_config_version,
                       runtime_session_config_hash,
                       session_config_state
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND attachment_state = 'attached'
                  AND project_source = ? AND project_id IS ?
                  AND cwd_snapshot = ?
                """,
                (
                    sdk_session_id,
                    runtime_generation,
                    owner_fence_token,
                    project.source.value,
                    project.project_id,
                    str(project.cwd),
                ),
            )
            binding = await binding_cursor.fetchone()
            await binding_cursor.close()
            if binding is None:
                raise ExtensionConfigConflict(
                    "session binding fence is not current for config publication"
                )
            cursor = await connection.execute(
                """
                SELECT * FROM config_reload_claims
                WHERE sdk_session_id = ? AND idempotency_key = ?
                """,
                (sdk_session_id, idempotency_key),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is not None and str(row["config_hash"]) != config_hash:
                raise ExtensionConfigConflict(
                    "config reload idempotency key was reused with a different config hash"
                )
            if row is None or (
                row["state"] == ConfigReloadState.CLAIMED.value and row["config_version"] is None
            ):
                version = await _publish_config_in_transaction(
                    connection,
                    project=project,
                    config=normalized,
                    config_hash=config_hash,
                    encoded=encoded,
                    expected_current_version=expected_current_version,
                    timestamp=timestamp,
                )
                if row is None:
                    await connection.execute(
                        """
                        INSERT INTO config_reload_claims(
                            sdk_session_id, idempotency_key, config_hash,
                            claimed_generation, owner_fence_token, state,
                            config_version, created_at
                        ) VALUES (?, ?, ?, ?, ?, 'started', ?, ?)
                        """,
                        (
                            sdk_session_id,
                            idempotency_key,
                            config_hash,
                            runtime_generation,
                            owner_fence_token,
                            version,
                            timestamp,
                        ),
                    )
                    created = True
                else:
                    await connection.execute(
                        """
                        UPDATE config_reload_claims
                        SET state = 'started', config_version = ?,
                            claimed_generation = ?, owner_fence_token = ?,
                            error_code = NULL, settled_at = NULL
                        WHERE sdk_session_id = ? AND idempotency_key = ?
                          AND state = 'claimed' AND config_version IS NULL
                        """,
                        (
                            version,
                            runtime_generation,
                            owner_fence_token,
                            sdk_session_id,
                            idempotency_key,
                        ),
                    )
                row = await _fetch_reload_claim(
                    connection,
                    sdk_session_id,
                    idempotency_key,
                )
            version = None if row["config_version"] is None else int(row["config_version"])
            if version is None:
                raise ExtensionConfigConflict("config reload claim has no deterministic generation")
            generation_cursor = await connection.execute(
                """
                SELECT config_hash FROM project_extension_config_generations
                WHERE scope_key = ? AND version = ?
                """,
                (
                    extension_scope_key(project.source.value, project.project_id),
                    version,
                ),
            )
            generation = await generation_cursor.fetchone()
            await generation_cursor.close()
            if generation is None or str(generation["config_hash"]) != config_hash:
                raise ExtensionConfigConflict(
                    "config reload claim generation does not match its hash"
                )
            if row["state"] in {
                ConfigReloadState.CLAIMED.value,
                ConfigReloadState.STARTED.value,
                ConfigReloadState.UNKNOWN.value,
            }:
                await connection.execute(
                    """
                    UPDATE config_reload_claims
                    SET state = 'started', claimed_generation = ?,
                        owner_fence_token = ?, error_code = NULL,
                        settled_at = NULL
                    WHERE sdk_session_id = ? AND idempotency_key = ?
                      AND config_hash = ?
                    """,
                    (
                        runtime_generation,
                        owner_fence_token,
                        sdk_session_id,
                        idempotency_key,
                        config_hash,
                    ),
                )
                row = await _fetch_reload_claim(
                    connection,
                    sdk_session_id,
                    idempotency_key,
                )
            if row["state"] in {
                ConfigReloadState.STARTED.value,
                ConfigReloadState.UNKNOWN.value,
            }:
                pending_version = binding["pending_session_config_version"]
                pending_matches = (
                    pending_version is not None
                    and int(pending_version) == version
                    and binding["pending_session_config_hash"] == config_hash
                    and binding["pending_session_config_transition_id"] == transition_id
                )
                already_synced = (
                    pending_version is None
                    and int(binding["desired_session_config_version"]) == version
                    and binding["desired_session_config_hash"] == config_hash
                    and binding["runtime_session_config_version"] == version
                    and binding["runtime_session_config_hash"] == config_hash
                    and binding["session_config_state"] == "synced"
                )
                if pending_version is not None and not pending_matches:
                    raise ExtensionConfigConflict(
                        "another extension config transition is already pending"
                    )
                if not already_synced:
                    update = await connection.execute(
                        """
                        UPDATE session_bindings
                        SET pending_session_config_version = ?,
                            pending_session_config_hash = ?,
                            pending_session_config_transition_id = ?,
                            session_config_state = 'pending',
                            session_config_drift = 0,
                            updated_at = ?, row_version = row_version + 1
                        WHERE sdk_session_id = ? AND runtime_generation = ?
                          AND owner_fence_token = ? AND attachment_state = 'attached'
                          AND (
                            pending_session_config_version IS NULL
                            OR (
                              pending_session_config_version = ?
                              AND pending_session_config_hash = ?
                              AND pending_session_config_transition_id = ?
                            )
                          )
                        """,
                        (
                            version,
                            config_hash,
                            transition_id,
                            timestamp,
                            sdk_session_id,
                            runtime_generation,
                            owner_fence_token,
                            version,
                            config_hash,
                            transition_id,
                        ),
                    )
                    if update.rowcount != 1:
                        await update.close()
                        raise ExtensionConfigConflict(
                            "session binding changed during config publication"
                        )
                    await update.close()
        claim = _row_to_reload_claim(row)
        return (
            claim,
            ExtensionConfigSnapshot(
                scope_key=extension_scope_key(
                    project.source.value,
                    project.project_id,
                ),
                version=version,
                project_id=project.project_id,
                project_source=project.source.value,
                cwd_snapshot=project.cwd,
                config_hash=config_hash,
                config=normalized,
            ),
            created,
        )

    async def transition(
        self,
        claim: ConfigReloadClaim,
        state: ConfigReloadState,
        *,
        config_version: int | None = None,
        error_code: str | None = None,
        now: float | None = None,
    ) -> ConfigReloadClaim:
        allowed = {
            ConfigReloadState.CLAIMED: {
                ConfigReloadState.STARTED,
                ConfigReloadState.CONFIRMED,
                ConfigReloadState.REJECTED,
                ConfigReloadState.UNKNOWN,
            },
            ConfigReloadState.STARTED: {
                ConfigReloadState.CONFIRMED,
                ConfigReloadState.REJECTED,
                ConfigReloadState.UNKNOWN,
            },
            ConfigReloadState.UNKNOWN: {
                ConfigReloadState.CONFIRMED,
            },
        }
        if state not in allowed.get(claim.state, set()):
            raise ExtensionConfigConflict(
                f"invalid config reload transition {claim.state} -> {state}"
            )
        timestamp = time.time() if now is None else now
        settled_at = (
            timestamp
            if state
            in {
                ConfigReloadState.CONFIRMED,
                ConfigReloadState.REJECTED,
                ConfigReloadState.UNKNOWN,
            }
            else None
        )
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE config_reload_claims
                SET state = ?, config_version = COALESCE(?, config_version),
                    error_code = ?, settled_at = ?
                WHERE sdk_session_id = ? AND idempotency_key = ?
                  AND config_hash = ? AND state = ?
                  AND claimed_generation = ? AND owner_fence_token = ?
                  AND EXISTS (
                    SELECT 1 FROM session_owner_leases
                    WHERE session_owner_leases.sdk_session_id =
                          config_reload_claims.sdk_session_id
                      AND session_owner_leases.fence_token =
                          config_reload_claims.owner_fence_token
                      AND session_owner_leases.expires_at > ?
                  )
                """,
                (
                    state.value,
                    config_version,
                    error_code,
                    settled_at,
                    claim.sdk_session_id,
                    claim.idempotency_key,
                    claim.config_hash,
                    claim.state.value,
                    claim.claimed_generation,
                    claim.owner_fence_token,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise ExtensionConfigConflict("config reload claim changed concurrently")
            await cursor.close()
            row = await _fetch_reload_claim(
                connection,
                claim.sdk_session_id,
                claim.idempotency_key,
            )
        return _row_to_reload_claim(row)


async def _fetch_reload_claim(
    connection: Any,
    sdk_session_id: str,
    idempotency_key: str,
) -> Any:
    cursor = await connection.execute(
        """
        SELECT * FROM config_reload_claims
        WHERE sdk_session_id = ? AND idempotency_key = ?
        """,
        (sdk_session_id, idempotency_key),
    )
    row = await cursor.fetchone()
    await cursor.close()
    if row is None:
        raise ExtensionConfigConflict("config reload claim disappeared")
    return row


def _row_to_reload_claim(row: Any) -> ConfigReloadClaim:
    return ConfigReloadClaim(
        sdk_session_id=str(row["sdk_session_id"]),
        idempotency_key=str(row["idempotency_key"]),
        config_hash=str(row["config_hash"]),
        claimed_generation=int(row["claimed_generation"]),
        owner_fence_token=int(row["owner_fence_token"]),
        state=ConfigReloadState(str(row["state"])),
        config_version=(None if row["config_version"] is None else int(row["config_version"])),
        error_code=(None if row["error_code"] is None else str(row["error_code"])),
    )


def extension_scope_key(project_source: str, project_id: str | None) -> str:
    if project_source == ProjectSource.EXPLICIT.value:
        if project_id is None:
            raise ExtensionConfigError("explicit project config requires a project id")
        return f"project:{project_id}"
    if project_id is not None:
        raise ExtensionConfigError("implicit project config cannot carry a project id")
    return "implicit-home"


def _snapshot_from_row(row: Any) -> ExtensionConfigSnapshot:
    try:
        payload = json.loads(str(row["config_json"]))
        config = ProjectExtensionConfig.from_dict(payload)
    except json.JSONDecodeError as error:
        raise ExtensionConfigError("persisted extension configuration is invalid JSON") from error
    config_hash = config.digest()
    if config_hash != str(row["config_hash"]):
        raise ExtensionConfigError("persisted extension configuration hash mismatch")
    return ExtensionConfigSnapshot(
        scope_key=str(row["scope_key"]),
        version=int(row["version"]),
        project_id=row["project_id"],
        project_source=str(row["project_source"]),
        cwd_snapshot=Path(str(row["cwd_snapshot"])),
        config_hash=config_hash,
        config=config,
    )


async def _publish_config_in_transaction(
    connection: Any,
    *,
    project: ProjectSnapshot,
    config: ProjectExtensionConfig,
    config_hash: str,
    encoded: str,
    expected_current_version: int | None,
    timestamp: float,
) -> int:
    scope_key = extension_scope_key(project.source.value, project.project_id)
    cursor = await connection.execute(
        """
        SELECT version, config_hash
        FROM project_extension_config_generations
        WHERE scope_key = ?
        ORDER BY version DESC LIMIT 1
        """,
        (scope_key,),
    )
    current = await cursor.fetchone()
    await cursor.close()
    current_version = 0 if current is None else int(current["version"])
    if expected_current_version is not None and current_version != expected_current_version:
        raise ExtensionConfigConflict(
            f"extension config changed: expected {expected_current_version}, "
            f"found {current_version}"
        )
    if current is not None and str(current["config_hash"]) == config_hash:
        return current_version
    version = current_version + 1
    await connection.execute(
        """
        INSERT INTO project_extension_config_generations(
            scope_key, version, project_id, project_source,
            cwd_snapshot, config_hash, config_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scope_key,
            version,
            project.project_id,
            project.source.value,
            str(project.cwd),
            config_hash,
            encoded,
            timestamp,
        ),
    )
    await _insert_config_children(
        connection,
        scope_key=scope_key,
        version=version,
        config=config,
    )
    return version


async def _insert_config_children(
    connection: Any,
    *,
    scope_key: str,
    version: int,
    config: ProjectExtensionConfig,
) -> None:
    for item in config.environment_references:
        await connection.execute(
            """
            INSERT INTO project_extension_env_refs(
                scope_key, config_version, name, source_env
            ) VALUES (?, ?, ?, ?)
            """,
            (scope_key, version, item.name, item.source_env),
        )
    for server in config.mcp_servers:
        await connection.execute(
            """
            INSERT INTO project_extension_mcp_servers(
                scope_key, config_version, name, transport, config_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                scope_key,
                version,
                server.name,
                server.transport,
                json.dumps(_mcp_to_dict(server), sort_keys=True),
            ),
        )
    for path in config.skill_directories:
        await connection.execute(
            """
            INSERT INTO project_extension_skill_dirs(
                scope_key, config_version, path
            ) VALUES (?, ?, ?)
            """,
            (scope_key, version, path),
        )
    for name in config.disabled_skills:
        await connection.execute(
            """
            INSERT INTO project_extension_disabled_skills(
                scope_key, config_version, name
            ) VALUES (?, ?, ?)
            """,
            (scope_key, version, name),
        )
    for path in config.plugin_directories:
        await connection.execute(
            """
            INSERT INTO project_extension_plugin_dirs(
                scope_key, config_version, path
            ) VALUES (?, ?, ?)
            """,
            (scope_key, version, path),
        )
    for agent in config.custom_agents:
        await connection.execute(
            """
            INSERT INTO project_extension_custom_agents(
                scope_key, config_version, name, config_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                scope_key,
                version,
                agent.name,
                json.dumps(_agent_to_dict(agent), sort_keys=True),
            ),
        )


def _mcp_sdk_config(
    server: McpServer,
    references: Mapping[str, str],
) -> dict[str, Any]:
    if isinstance(server, McpStdioServer):
        result: dict[str, Any] = {
            "type": "stdio",
            "command": server.command,
            "args": list(server.args),
            "tools": list(server.tools),
        }
        if server.environment:
            result["env"] = {item.name: references[item.reference] for item in server.environment}
        if server.working_directory is not None:
            result["working_directory"] = server.working_directory
    else:
        result = {
            "type": "http",
            "url": server.url,
            "tools": list(server.tools),
        }
        if server.headers:
            result["headers"] = {item.name: references[item.reference] for item in server.headers}
    if server.timeout_ms is not None:
        result["timeout"] = server.timeout_ms
    return result


def _resolve_environment_value(
    reference: EnvironmentReference,
    environ: Mapping[str, str],
) -> str:
    value = environ.get(reference.source_env)
    if value is None:
        raise MissingEnvironmentReference(
            f"required environment variable is not set: {reference.source_env}"
        )
    return value


def _mcp_to_dict(server: McpServer) -> dict[str, Any]:
    if isinstance(server, McpStdioServer):
        return {
            "name": server.name,
            "transport": server.transport,
            "command": server.command,
            "args": list(server.args),
            "environment": [
                {"name": item.name, "reference": item.reference} for item in server.environment
            ],
            "working_directory": server.working_directory,
            "tools": list(server.tools),
            "timeout_ms": server.timeout_ms,
        }
    return {
        "name": server.name,
        "transport": server.transport,
        "url": server.url,
        "headers": [{"name": item.name, "reference": item.reference} for item in server.headers],
        "tools": list(server.tools),
        "timeout_ms": server.timeout_ms,
    }


def _mcp_from_dict(payload: Mapping[str, Any]) -> McpServer:
    transport = str(payload["transport"])
    if transport == "stdio":
        _reject_unknown_keys(
            payload,
            {
                "name",
                "transport",
                "command",
                "args",
                "environment",
                "working_directory",
                "tools",
                "timeout_ms",
            },
            "stdio MCP server",
        )
        environment = _mapping_list(payload.get("environment", []))
        for item in environment:
            _reject_unknown_keys(
                item,
                {"name", "reference"},
                "MCP environment binding",
            )
        return McpStdioServer(
            name=str(payload["name"]),
            command=str(payload["command"]),
            args=_string_tuple(payload.get("args", [])),
            environment=tuple(
                EnvironmentBinding(
                    name=str(item["name"]),
                    reference=str(item["reference"]),
                )
                for item in environment
            ),
            working_directory=(
                None
                if payload.get("working_directory") is None
                else str(payload["working_directory"])
            ),
            tools=_string_tuple(payload.get("tools", ["*"])),
            timeout_ms=_optional_int(payload.get("timeout_ms")),
        )
    if transport == "http":
        _reject_unknown_keys(
            payload,
            {
                "name",
                "transport",
                "url",
                "headers",
                "tools",
                "timeout_ms",
            },
            "HTTP MCP server",
        )
        headers = _mapping_list(payload.get("headers", []))
        for item in headers:
            _reject_unknown_keys(
                item,
                {"name", "reference"},
                "MCP header binding",
            )
        return McpHttpServer(
            name=str(payload["name"]),
            url=str(payload["url"]),
            headers=tuple(
                HeaderBinding(
                    name=str(item["name"]),
                    reference=str(item["reference"]),
                )
                for item in headers
            ),
            tools=_string_tuple(payload.get("tools", ["*"])),
            timeout_ms=_optional_int(payload.get("timeout_ms")),
        )
    raise ExtensionConfigError(f"unsupported MCP transport: {transport}")


def _agent_to_dict(agent: CustomAgent) -> dict[str, Any]:
    return {
        "name": agent.name,
        "display_name": agent.display_name,
        "description": agent.description,
        "prompt": agent.prompt,
        "tools": None if agent.tools is None else list(agent.tools),
        "skills": list(agent.skills),
        "mcp_server_names": list(agent.mcp_server_names),
        "infer": agent.infer,
        "model": agent.model,
        "reasoning_effort": agent.reasoning_effort,
    }


def _agent_from_dict(payload: Mapping[str, Any]) -> CustomAgent:
    _reject_unknown_keys(
        payload,
        {
            "name",
            "display_name",
            "description",
            "prompt",
            "tools",
            "skills",
            "mcp_server_names",
            "infer",
            "model",
            "reasoning_effort",
        },
        "custom agent",
    )
    tools = payload.get("tools")
    return CustomAgent(
        name=str(payload["name"]),
        display_name=(
            None if payload.get("display_name") is None else str(payload["display_name"])
        ),
        description=str(payload.get("description", "")),
        prompt=str(payload["prompt"]),
        tools=None if tools is None else _string_tuple(tools),
        skills=_string_tuple(payload.get("skills", [])),
        mcp_server_names=_string_tuple(payload.get("mcp_server_names", [])),
        infer=None if payload.get("infer") is None else bool(payload["infer"]),
        model=None if payload.get("model") is None else str(payload["model"]),
        reasoning_effort=(
            None if payload.get("reasoning_effort") is None else str(payload["reasoning_effort"])
        ),
    )


def _normalize_directory(value: str, cwd: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = cwd / candidate
    resolved = candidate.resolve()
    if not resolved.exists() or not resolved.is_dir():
        raise ExtensionConfigError(f"extension directory does not exist: {resolved}")
    return resolved


def _validate_name(value: str, kind: str) -> None:
    if _CONFIG_NAME.fullmatch(value) is None:
        raise ExtensionConfigError(f"invalid {kind} name: {value!r}")


def _require_unique(values: Any, kind: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for raw_value in values:
        value = str(raw_value)
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        raise ExtensionConfigError(f"duplicate {kind} values: {', '.join(sorted(duplicates))}")


def _mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise ExtensionConfigError("expected a list of objects")
    if not all(isinstance(item, Mapping) for item in value):
        raise ExtensionConfigError("expected a list of objects")
    return list(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not all(isinstance(item, str) for item in value):
        raise ExtensionConfigError("expected a list of strings")
    return tuple(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ExtensionConfigError("expected an integer")
    return int(value)


def _reject_unknown_keys(
    payload: Mapping[str, Any],
    allowed: set[str],
    context: str,
) -> None:
    unknown = sorted(str(key) for key in set(payload) - allowed)
    if unknown:
        raise ExtensionConfigError(f"{context} contains unknown keys: {', '.join(unknown)}")
