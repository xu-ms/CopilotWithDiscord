from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from aiosqlite import Connection, Row

from copilotd.core.commands import (
    CDConflictError,
    CDInputError,
    CDPathError,
    CDProjectError,
)
from copilotd.core.schedule_time import load_timezone
from copilotd.storage.database import Database


class ProjectSource(StrEnum):
    EXPLICIT = "explicit"
    IMPLICIT_HOME = "implicit-home"


@dataclass(frozen=True, slots=True)
class ProjectSnapshot:
    project_id: str | None
    channel_id: str
    source: ProjectSource
    root_path: Path
    cwd: Path
    config_version: int
    timezone: str = "UTC"


@dataclass(frozen=True, slots=True)
class McpServerSnapshot:
    name: str
    transport: Literal["stdio", "http"]
    command: str | None
    url: str | None
    args: tuple[str, ...]
    headers: tuple[tuple[str, str], ...]
    env_refs: tuple[str, ...]
    enabled: bool
    version: int


@dataclass(frozen=True, slots=True)
class DirectorySnapshot:
    path: Path
    enabled: bool


@dataclass(frozen=True, slots=True)
class CustomAgentSnapshot:
    name: str
    description: str
    prompt: str
    tools: tuple[str, ...]
    enabled: bool


@dataclass(frozen=True, slots=True)
class ProjectConfigSnapshot:
    project_id: str | None
    source: ProjectSource
    cwd: Path
    timezone: str
    config_version: int
    variables: tuple[tuple[str, str], ...] = ()
    mcp_servers: tuple[McpServerSnapshot, ...] = ()
    skill_dirs: tuple[DirectorySnapshot, ...] = ()
    plugin_dirs: tuple[DirectorySnapshot, ...] = ()
    custom_agents: tuple[CustomAgentSnapshot, ...] = ()

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ProjectConfigSnapshot:
        return cls(
            project_id=(None if payload.get("project_id") is None else str(payload["project_id"])),
            source=ProjectSource(str(payload["source"])),
            cwd=Path(str(payload["cwd"])),
            timezone=str(payload.get("timezone", "UTC")),
            config_version=int(payload.get("config_version", 1)),
            variables=tuple(
                (str(item["name"]), str(item["value"])) for item in payload.get("variables", [])
            ),
            mcp_servers=tuple(
                McpServerSnapshot(
                    name=str(item["name"]),
                    transport=str(item["transport"]),
                    command=(None if item.get("command") is None else str(item["command"])),
                    url=None if item.get("url") is None else str(item["url"]),
                    args=tuple(str(value) for value in item.get("args", [])),
                    headers=tuple(
                        sorted(
                            (str(name), str(value))
                            for name, value in dict(item.get("headers", {})).items()
                        )
                    ),
                    env_refs=tuple(str(value) for value in item.get("env_refs", [])),
                    enabled=bool(item.get("enabled", True)),
                    version=int(item.get("version", 1)),
                )
                for item in payload.get("mcp_servers", [])
            ),
            skill_dirs=tuple(
                DirectorySnapshot(
                    path=Path(str(item["path"])),
                    enabled=bool(item.get("enabled", True)),
                )
                for item in payload.get("skill_dirs", [])
            ),
            plugin_dirs=tuple(
                DirectorySnapshot(
                    path=Path(str(item["path"])),
                    enabled=bool(item.get("enabled", True)),
                )
                for item in payload.get("plugin_dirs", [])
            ),
            custom_agents=tuple(
                CustomAgentSnapshot(
                    name=str(item["name"]),
                    description=str(item["description"]),
                    prompt=str(item["prompt"]),
                    tools=tuple(str(value) for value in item.get("tools", [])),
                    enabled=bool(item.get("enabled", True)),
                )
                for item in payload.get("custom_agents", [])
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "source": self.source.value,
            "cwd": str(self.cwd),
            "timezone": self.timezone,
            "config_version": self.config_version,
            "variables": [{"name": name, "value": value} for name, value in self.variables],
            "mcp_servers": [
                {
                    "name": item.name,
                    "transport": item.transport,
                    "command": item.command,
                    "url": item.url,
                    "args": list(item.args),
                    "headers": dict(item.headers),
                    "env_refs": list(item.env_refs),
                    "enabled": item.enabled,
                    "version": item.version,
                }
                for item in self.mcp_servers
            ],
            "skill_dirs": [
                {"path": str(item.path), "enabled": item.enabled} for item in self.skill_dirs
            ],
            "plugin_dirs": [
                {"path": str(item.path), "enabled": item.enabled} for item in self.plugin_dirs
            ],
            "custom_agents": [
                {
                    "name": item.name,
                    "description": item.description,
                    "prompt": item.prompt,
                    "tools": list(item.tools),
                    "enabled": item.enabled,
                }
                for item in self.custom_agents
            ],
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def snapshot_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ProjectSessionConfigSnapshot:
    project_id: str | None
    source: ProjectSource
    root_path: Path
    cwd: Path
    project_config_version: int
    channel_config_version: int
    layout: str
    mention_required: bool
    session_options: dict[str, Any]

    def project_payload(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "project_config_version": self.project_config_version,
            "session_options": self.session_options,
        }

    def channel_payload(self) -> dict[str, Any]:
        return {
            "channel_config_version": self.channel_config_version,
            "layout": self.layout,
            "mention_required": self.mention_required,
        }


@dataclass(frozen=True, slots=True)
class ProjectEnvEntry:
    project_id: str
    channel_id: str
    name: str
    value: str
    project_config_version: int


@dataclass(frozen=True, slots=True)
class ProjectMcpServerEntry:
    project_id: str
    channel_id: str
    name: str
    transport: str
    config: dict[str, Any]
    enabled: bool
    project_config_version: int
    server_version: int


@dataclass(frozen=True, slots=True)
class ProjectDirectoryEntry:
    project_id: str
    channel_id: str
    path: Path
    enabled: bool
    project_config_version: int


@dataclass(frozen=True, slots=True)
class ProjectCustomAgentEntry:
    project_id: str
    channel_id: str
    name: str
    description: str
    prompt: str
    tools: tuple[str, ...]
    enabled: bool
    project_config_version: int


class ProjectPathError(CDPathError):
    pass


class ProjectBindingError(CDProjectError):
    pass


class ProjectConflictError(CDConflictError):
    pass


class ProjectValidationError(CDInputError):
    pass


_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TOOL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


class ProjectConfigError(ProjectValidationError):
    code = "CD-INPUT-001"


ConfigMutation = Callable[[Connection], Awaitable[None]]
_VARIABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONFIG_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class ProjectRegistry:
    """Resolves explicit channel projects before the immutable HOME fallback."""

    def __init__(self, database: Database, *, resolved_home: Path) -> None:
        self._database = database
        self._configured_home = resolved_home
        self._resolved_home: Path | None = None

    @property
    def resolved_home(self) -> Path:
        if self._resolved_home is None:
            raise RuntimeError("project registry has not been initialized")
        return self._resolved_home

    async def initialize(self) -> None:
        resolved_home = await asyncio.to_thread(_validate_directory, self._configured_home)
        now = time.time()
        row = await self._database.fetchone(
            "SELECT value FROM global_config WHERE key = 'resolved_home'"
        )
        if row is None:
            await self._database.execute(
                """
                INSERT INTO global_config(key, value, updated_at)
                VALUES ('resolved_home', ?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (str(resolved_home), now),
            )
            row = await self._database.fetchone(
                "SELECT value FROM global_config WHERE key = 'resolved_home'"
            )
        if row is None:
            raise RuntimeError("resolved HOME could not be persisted")
        persisted = await asyncio.to_thread(_validate_directory, Path(row["value"]))
        if persisted != resolved_home:
            raise ProjectPathError(
                "resolved HOME differs from the value persisted by this installation"
            )
        self._resolved_home = resolved_home

    async def resolve(self, channel_id: str) -> ProjectSnapshot:
        self._require_initialized()
        row = await self._database.fetchone(
            """
            SELECT * FROM projects
            WHERE channel_id = ? AND state = 'active'
            """,
            (channel_id,),
        )
        if row is None:
            timezone = await self.channel_timezone(channel_id)
            return ProjectSnapshot(
                project_id=None,
                channel_id=channel_id,
                source=ProjectSource.IMPLICIT_HOME,
                root_path=self.resolved_home,
                cwd=self.resolved_home,
                config_version=1,
                timezone=timezone,
            )
        return ProjectSnapshot(
            project_id=row["id"],
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=Path(row["root_path"]),
            cwd=Path(row["cwd"]),
            config_version=row["config_version"],
            timezone=str(row["timezone"]),
        )

    async def bind(self, channel_id: str, path: Path) -> ProjectSnapshot:
        self._require_initialized()
        resolved = await asyncio.to_thread(_validate_directory, path)
        now = time.time()
        project_id = str(uuid.uuid4())
        timezone = await self.channel_timezone(channel_id)
        async with self._database.transaction() as connection:
            await _require_no_active_worktree_intents(connection, channel_id)
            cursor = await connection.execute(
                """
                SELECT COALESCE(MAX(config_version), 0)
                FROM projects WHERE channel_id = ?
                """,
                (channel_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            config_version = int(row[0]) + 1
            await connection.execute(
                """
                UPDATE projects SET state = 'retired', updated_at = ?
                WHERE channel_id = ? AND state = 'active'
                """,
                (now, channel_id),
            )
            await connection.execute(
                """
                INSERT INTO projects(
                    id, channel_id, root_path, cwd, config_version,
                    state, timezone, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
                """,
                (
                    project_id,
                    channel_id,
                    str(resolved),
                    str(resolved),
                    config_version,
                    timezone,
                    now,
                    now,
                ),
            )
            snapshot = ProjectConfigSnapshot(
                project_id=project_id,
                source=ProjectSource.EXPLICIT,
                cwd=resolved,
                timezone=timezone,
                config_version=config_version,
            )
            await _insert_config_revision(connection, snapshot, now=now)
        return ProjectSnapshot(
            project_id=project_id,
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=resolved,
            cwd=resolved,
            config_version=config_version,
            timezone=timezone,
        )

    async def unbind(self, channel_id: str) -> ProjectSnapshot:
        self._require_initialized()
        now = time.time()
        async with self._database.transaction() as connection:
            await _require_no_active_worktree_intents(connection, channel_id)
            await connection.execute(
                """
                UPDATE projects SET state = 'retired', updated_at = ?
                WHERE channel_id = ? AND state = 'active'
                """,
                (now, channel_id),
            )
        return await self.resolve(channel_id)

    async def set_layout(self, channel_id: str, layout: str) -> None:
        if layout not in {"text", "forum"}:
            raise ValueError(f"unsupported channel layout: {layout}")
        await self._upsert_channel_setting(channel_id, layout=layout)

    async def set_mention_required(self, channel_id: str, required: bool) -> None:
        await self._upsert_channel_setting(channel_id, mention_required=required)

    async def set_channel_timezone(self, channel_id: str, timezone: str) -> None:
        zone = load_timezone(timezone)
        now = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO channel_settings(channel_id, timezone, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    timezone = excluded.timezone,
                    config_version = channel_settings.config_version + 1,
                    updated_at = excluded.updated_at
                """,
                (channel_id, zone.key, now),
            )
            await connection.execute(
                """
                UPDATE projects
                SET timezone = ?, config_version = config_version + 1,
                    updated_at = ?
                WHERE channel_id = ? AND state = 'active'
                """,
                (zone.key, now, channel_id),
            )
            project = await _fetchone(
                connection,
                """
                SELECT * FROM projects
                WHERE channel_id = ? AND state = 'active'
                """,
                (channel_id,),
            )
            if project is not None:
                snapshot = await _config_snapshot_from_connection(connection, project)
                await _insert_config_revision(connection, snapshot, now=now)

    async def channel_timezone(self, channel_id: str) -> str:
        row = await self._database.fetchone(
            "SELECT timezone FROM channel_settings WHERE channel_id = ?",
            (channel_id,),
        )
        if row is not None and row["timezone"] is not None:
            return str(row["timezone"])
        global_row = await self._database.fetchone(
            "SELECT value FROM global_config WHERE key = 'default_timezone'"
        )
        return "UTC" if global_row is None else load_timezone(str(global_row["value"])).key

    async def channel_settings(self, channel_id: str) -> tuple[str, bool, int]:
        row = await self._database.fetchone(
            "SELECT * FROM channel_settings WHERE channel_id = ?",
            (channel_id,),
        )
        if row is None:
            return "text", False, 1
        return row["layout"], bool(row["mention_required"]), row["config_version"]

    async def session_config_snapshot(
        self,
        channel_id: str,
    ) -> ProjectSessionConfigSnapshot:
        project = await self.resolve(channel_id)
        layout, mention_required, channel_version = await self.channel_settings(channel_id)
        if project.project_id is None:
            return ProjectSessionConfigSnapshot(
                project_id=None,
                source=project.source,
                root_path=project.root_path,
                cwd=project.cwd,
                project_config_version=project.config_version,
                channel_config_version=channel_version,
                layout=layout,
                mention_required=mention_required,
                session_options={},
            )

        env_entries = await self.list_project_env(channel_id, reveal=True)
        mcp_entries = await self.list_mcp_servers(channel_id, reveal=True)
        skill_entries = await self.list_skill_dirs(channel_id)
        plugin_entries = await self.list_plugin_dirs(channel_id)
        agent_entries = await self.list_custom_agents(channel_id)
        latest_project = await self.resolve(channel_id)
        latest_layout, latest_mention, latest_channel_version = await self.channel_settings(
            channel_id
        )
        if (
            latest_project.project_id != project.project_id
            or latest_project.config_version != project.config_version
            or latest_channel_version != channel_version
            or latest_layout != layout
            or latest_mention != mention_required
        ):
            raise ProjectConflictError(
                "project configuration changed while creating a session snapshot"
            )

        project_env = {entry.name: entry.value for entry in env_entries}
        referenced_env: set[str] = set()
        mcp_servers: dict[str, dict[str, Any]] = {}
        for entry in mcp_entries:
            if not entry.enabled:
                continue
            server, references = await asyncio.to_thread(
                _sdk_mcp_server_config,
                entry,
                project_env,
                project.root_path,
            )
            mcp_servers[entry.name] = server
            referenced_env.update(references)
        unapplied_env = sorted(set(project_env) - referenced_env)
        if unapplied_env:
            raise ProjectValidationError(
                "project environment variables cannot be applied by this SDK unless "
                "referenced by an enabled stdio MCP server: " + ", ".join(unapplied_env)
            )

        skill_directories = [
            str(await asyncio.to_thread(_validate_directory, entry.path))
            for entry in skill_entries
            if entry.enabled
        ]
        plugin_directories = [
            str(await asyncio.to_thread(_validate_directory, entry.path))
            for entry in plugin_entries
            if entry.enabled
        ]
        custom_agents = [
            {
                "name": entry.name,
                "display_name": entry.name,
                "description": entry.description,
                "prompt": entry.prompt,
                "tools": list(entry.tools),
            }
            for entry in agent_entries
            if entry.enabled
        ]
        options: dict[str, Any] = {}
        if mcp_servers:
            options["mcp_servers"] = mcp_servers
        if skill_directories:
            options["enable_skills"] = True
            options["skill_directories"] = skill_directories
        if plugin_directories:
            options["plugin_directories"] = plugin_directories
        if custom_agents:
            options["custom_agents"] = custom_agents
        return ProjectSessionConfigSnapshot(
            project_id=project.project_id,
            source=project.source,
            root_path=project.root_path,
            cwd=project.cwd,
            project_config_version=project.config_version,
            channel_config_version=channel_version,
            layout=layout,
            mention_required=mention_required,
            session_options=options,
        )

    async def list_project_env(
        self,
        channel_id: str,
        *,
        reveal: bool = False,
    ) -> list[ProjectEnvEntry]:
        project = await self._require_explicit_project(channel_id)
        rows = await self._database.fetchall(
            """
            SELECT name, value FROM project_env
            WHERE project_id = ?
            ORDER BY name
            """,
            (project.project_id,),
        )
        return [
            ProjectEnvEntry(
                project_id=project.project_id,
                channel_id=channel_id,
                name=str(row["name"]),
                value=str(row["value"]) if reveal else "[redacted]",
                project_config_version=project.config_version,
            )
            for row in rows
        ]

    async def set_project_env(
        self,
        channel_id: str,
        name: str,
        value: str,
        *,
        expected_version: int | None = None,
    ) -> ProjectEnvEntry:
        project = await self._require_explicit_project(channel_id)
        env_name = _validate_env_name(name)
        env_value = str(value)
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            await connection.execute(
                """
                INSERT INTO project_env(project_id, name, value)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id, name) DO UPDATE SET value = excluded.value
                """,
                (project.project_id, env_name, env_value),
            )
        return ProjectEnvEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            name=env_name,
            value=env_value,
            project_config_version=new_version,
        )

    async def remove_project_env(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool:
        project = await self._require_explicit_project(channel_id)
        env_name = _validate_env_name(name)
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                DELETE FROM project_env
                WHERE project_id = ? AND name = ?
                """,
                (project.project_id, env_name),
            )
            removed = cursor.rowcount > 0
            await cursor.close()
            if not removed:
                return False
            await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
        return True

    async def list_mcp_servers(
        self,
        channel_id: str,
        *,
        reveal: bool = False,
    ) -> list[ProjectMcpServerEntry]:
        project = await self._require_explicit_project(channel_id)
        rows = await self._database.fetchall(
            """
            SELECT name, transport, config_json, enabled, version
            FROM mcp_servers
            WHERE project_id = ?
            ORDER BY name
            """,
            (project.project_id,),
        )
        entries: list[ProjectMcpServerEntry] = []
        for row in rows:
            config = json.loads(str(row["config_json"]))
            entries.append(
                ProjectMcpServerEntry(
                    project_id=project.project_id,
                    channel_id=channel_id,
                    name=str(row["name"]),
                    transport=str(row["transport"]),
                    config=config if reveal else _redact_mcp_config(config),
                    enabled=bool(row["enabled"]),
                    project_config_version=project.config_version,
                    server_version=int(row["version"]),
                )
            )
        return entries

    async def set_mcp_server(
        self,
        channel_id: str,
        *,
        name: str,
        transport: str,
        config: Mapping[str, Any],
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> ProjectMcpServerEntry:
        project = await self._require_explicit_project(channel_id)
        server_name = _validate_resource_name(name, kind="MCP server name")
        server_transport = _validate_mcp_transport(transport)
        normalized = _normalize_mcp_config(server_transport, config)
        serialized = json.dumps(normalized, sort_keys=True)
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            row = await connection.execute(
                "SELECT version FROM mcp_servers WHERE project_id = ? AND name = ?",
                (project.project_id, server_name),
            )
            current = await row.fetchone()
            await row.close()
            server_version = 1 if current is None else int(current["version"]) + 1
            await connection.execute(
                """
                INSERT INTO mcp_servers(project_id, name, transport, config_json, enabled, version)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id, name) DO UPDATE SET
                    transport = excluded.transport,
                    config_json = excluded.config_json,
                    enabled = excluded.enabled,
                    version = excluded.version
                """,
                (
                    project.project_id,
                    server_name,
                    server_transport,
                    serialized,
                    int(enabled),
                    server_version,
                ),
            )
        return ProjectMcpServerEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            name=server_name,
            transport=server_transport,
            config=normalized,
            enabled=enabled,
            project_config_version=new_version,
            server_version=server_version,
        )

    async def _toggle_mcp_server_for_channel(
        self,
        channel_id: str,
        *,
        name: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectMcpServerEntry:
        project = await self._require_explicit_project(channel_id)
        server_name = _validate_resource_name(name, kind="MCP server name")
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            row = await connection.execute(
                "SELECT transport, config_json, version FROM mcp_servers "
                "WHERE project_id = ? AND name = ?",
                (project.project_id, server_name),
            )
            current = await row.fetchone()
            await row.close()
            if current is None:
                raise ProjectValidationError(f"MCP server not found: {server_name}")
            server_version = int(current["version"]) + 1
            await connection.execute(
                """
                UPDATE mcp_servers
                SET enabled = ?, version = ?
                WHERE project_id = ? AND name = ?
                """,
                (int(enabled), server_version, project.project_id, server_name),
            )
        config = json.loads(str(current["config_json"]))
        return ProjectMcpServerEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            name=server_name,
            transport=str(current["transport"]),
            config=config,
            enabled=enabled,
            project_config_version=new_version,
            server_version=server_version,
        )

    async def _remove_mcp_server_for_channel(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool:
        project = await self._require_explicit_project(channel_id)
        server_name = _validate_resource_name(name, kind="MCP server name")
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                "DELETE FROM mcp_servers WHERE project_id = ? AND name = ?",
                (project.project_id, server_name),
            )
            removed = cursor.rowcount > 0
            await cursor.close()
            if not removed:
                return False
            await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
        return True

    async def list_skill_dirs(self, channel_id: str) -> list[ProjectDirectoryEntry]:
        return await self._list_directory_configs(channel_id, table="skill_dirs")

    async def set_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        return await self._set_directory_config(
            channel_id,
            table="skill_dirs",
            path=path,
            enabled=enabled,
            expected_version=expected_version,
        )

    async def toggle_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        return await self._toggle_directory_config(
            channel_id,
            table="skill_dirs",
            path=path,
            enabled=enabled,
            expected_version=expected_version,
        )

    async def remove_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        expected_version: int | None = None,
    ) -> bool:
        return await self._remove_directory_config(
            channel_id,
            table="skill_dirs",
            path=path,
            expected_version=expected_version,
        )

    async def list_plugin_dirs(self, channel_id: str) -> list[ProjectDirectoryEntry]:
        return await self._list_directory_configs(channel_id, table="plugin_dirs")

    async def set_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        return await self._set_directory_config(
            channel_id,
            table="plugin_dirs",
            path=path,
            enabled=enabled,
            expected_version=expected_version,
        )

    async def toggle_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        return await self._toggle_directory_config(
            channel_id,
            table="plugin_dirs",
            path=path,
            enabled=enabled,
            expected_version=expected_version,
        )

    async def remove_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        expected_version: int | None = None,
    ) -> bool:
        return await self._remove_directory_config(
            channel_id,
            table="plugin_dirs",
            path=path,
            expected_version=expected_version,
        )

    async def list_custom_agents(self, channel_id: str) -> list[ProjectCustomAgentEntry]:
        project = await self._require_explicit_project(channel_id)
        rows = await self._database.fetchall(
            """
            SELECT name, description, prompt, tools_json, enabled
            FROM custom_agents
            WHERE project_id = ?
            ORDER BY name
            """,
            (project.project_id,),
        )
        return [
            ProjectCustomAgentEntry(
                project_id=project.project_id,
                channel_id=channel_id,
                name=str(row["name"]),
                description=str(row["description"]),
                prompt=str(row["prompt"]),
                tools=tuple(json.loads(str(row["tools_json"]))),
                enabled=bool(row["enabled"]),
                project_config_version=project.config_version,
            )
            for row in rows
        ]

    async def set_custom_agent(
        self,
        channel_id: str,
        *,
        name: str,
        description: str,
        prompt: str,
        tools: Sequence[str],
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> ProjectCustomAgentEntry:
        project = await self._require_explicit_project(channel_id)
        agent_name = _validate_resource_name(name, kind="custom agent name")
        agent_description = _validate_text(description, kind="custom agent description")
        agent_prompt = _validate_text(prompt, kind="custom agent prompt")
        agent_tools = tuple(
            _validate_tool_name(tool)
            for tool in _validate_string_list(
                tools,
                kind="custom agent tools",
            )
        )
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            await connection.execute(
                """
                INSERT INTO custom_agents(
                    project_id, name, description, prompt, tools_json, enabled
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id, name) DO UPDATE SET
                    description = excluded.description,
                    prompt = excluded.prompt,
                    tools_json = excluded.tools_json,
                    enabled = excluded.enabled
                """,
                (
                    project.project_id,
                    agent_name,
                    agent_description,
                    agent_prompt,
                    json.dumps(agent_tools, sort_keys=True),
                    int(enabled),
                ),
            )
        return ProjectCustomAgentEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            name=agent_name,
            description=agent_description,
            prompt=agent_prompt,
            tools=agent_tools,
            enabled=enabled,
            project_config_version=new_version,
        )

    async def _toggle_custom_agent_for_channel(
        self,
        channel_id: str,
        *,
        name: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectCustomAgentEntry:
        project = await self._require_explicit_project(channel_id)
        agent_name = _validate_resource_name(name, kind="custom agent name")
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            row = await connection.execute(
                """
                SELECT description, prompt, tools_json, enabled
                FROM custom_agents
                WHERE project_id = ? AND name = ?
                """,
                (project.project_id, agent_name),
            )
            current = await row.fetchone()
            await row.close()
            if current is None:
                raise ProjectValidationError(f"custom agent not found: {agent_name}")
            await connection.execute(
                """
                UPDATE custom_agents SET enabled = ?
                WHERE project_id = ? AND name = ?
                """,
                (int(enabled), project.project_id, agent_name),
            )
        return ProjectCustomAgentEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            name=agent_name,
            description=str(current["description"]),
            prompt=str(current["prompt"]),
            tools=tuple(json.loads(str(current["tools_json"]))),
            enabled=enabled,
            project_config_version=new_version,
        )

    async def _remove_custom_agent_for_channel(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool:
        project = await self._require_explicit_project(channel_id)
        agent_name = _validate_resource_name(name, kind="custom agent name")
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                "DELETE FROM custom_agents WHERE project_id = ? AND name = ?",
                (project.project_id, agent_name),
            )
            removed = cursor.rowcount > 0
            await cursor.close()
            if not removed:
                return False
            await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
        return True

    async def _require_explicit_project(self, channel_id: str) -> ProjectSnapshot:
        project = await self.resolve(channel_id)
        if project.project_id is None:
            raise ProjectBindingError("explicit project binding is required")
        return project

    async def _project_mutation(
        self,
        channel_id: str,
        *,
        expected_version: int | None = None,
    ) -> int:
        project = await self._require_explicit_project(channel_id)
        async with self._database.transaction() as connection:
            return await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )

    async def _advance_project_config_version(
        self,
        connection: Any,
        project_id: str,
        *,
        expected_version: int | None = None,
    ) -> int:
        cursor = await connection.execute(
            "SELECT config_version FROM projects WHERE id = ?",
            (project_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            raise ProjectBindingError("explicit project binding no longer exists")
        current_version = int(row[0])
        if expected_version is not None and expected_version != current_version:
            raise ProjectConflictError(
                "project config version changed: "
                f"expected {expected_version}, found {current_version}"
            )
        new_version = current_version + 1
        update = await connection.execute(
            """
            UPDATE projects
            SET config_version = ?, updated_at = ?
            WHERE id = ? AND config_version = ?
            """,
            (new_version, time.time(), project_id, current_version),
        )
        if update.rowcount != 1:
            await update.close()
            raise ProjectConflictError("project config changed concurrently")
        await update.close()
        return new_version

    async def _upsert_channel_setting(
        self,
        channel_id: str,
        *,
        layout: str | None = None,
        mention_required: bool | None = None,
        timezone: str | None = None,
    ) -> None:
        now = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO channel_settings(channel_id, updated_at)
                VALUES (?, ?)
                ON CONFLICT(channel_id) DO NOTHING
                """,
                (channel_id, now),
            )
            updates: list[str] = []
            values: list[object] = []
            if layout is not None:
                updates.append("layout = ?")
                values.append(layout)
            if mention_required is not None:
                updates.append("mention_required = ?")
                values.append(int(mention_required))
            if timezone is not None:
                updates.append("timezone = ?")
                values.append(timezone)
            updates.extend(("config_version = config_version + 1", "updated_at = ?"))
            values.extend((now, channel_id))
            await connection.execute(
                f"UPDATE channel_settings SET {', '.join(updates)} WHERE channel_id = ?",
                values,
            )

    async def _list_directory_configs(
        self,
        channel_id: str,
        *,
        table: str,
    ) -> list[ProjectDirectoryEntry]:
        project = await self._require_explicit_project(channel_id)
        rows = await self._database.fetchall(
            f"""
            SELECT path, enabled
            FROM {table}
            WHERE project_id = ?
            ORDER BY path
            """,
            (project.project_id,),
        )
        return [
            ProjectDirectoryEntry(
                project_id=project.project_id,
                channel_id=channel_id,
                path=Path(str(row["path"])),
                enabled=bool(row["enabled"]),
                project_config_version=project.config_version,
            )
            for row in rows
        ]

    async def _set_directory_config(
        self,
        channel_id: str,
        *,
        table: str,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        project = await self._require_explicit_project(channel_id)
        resolved = await asyncio.to_thread(_validate_directory, Path(path))
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            await connection.execute(
                f"""
                INSERT INTO {table}(project_id, path, enabled)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id, path) DO UPDATE SET enabled = excluded.enabled
                """,
                (project.project_id, str(resolved), int(enabled)),
            )
        return ProjectDirectoryEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            path=resolved,
            enabled=enabled,
            project_config_version=new_version,
        )

    async def _toggle_directory_config(
        self,
        channel_id: str,
        *,
        table: str,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> ProjectDirectoryEntry:
        project = await self._require_explicit_project(channel_id)
        resolved = await asyncio.to_thread(_directory_key, Path(path))
        async with self._database.transaction() as connection:
            new_version = await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
            row = await connection.execute(
                f"SELECT enabled FROM {table} WHERE project_id = ? AND path = ?",
                (project.project_id, str(resolved)),
            )
            current = await row.fetchone()
            await row.close()
            if current is None:
                raise ProjectValidationError(f"{table[:-1]} not found: {resolved}")
            await connection.execute(
                f"UPDATE {table} SET enabled = ? WHERE project_id = ? AND path = ?",
                (int(enabled), project.project_id, str(resolved)),
            )
        return ProjectDirectoryEntry(
            project_id=project.project_id,
            channel_id=channel_id,
            path=resolved,
            enabled=enabled,
            project_config_version=new_version,
        )

    async def _remove_directory_config(
        self,
        channel_id: str,
        *,
        table: str,
        path: str,
        expected_version: int | None = None,
    ) -> bool:
        project = await self._require_explicit_project(channel_id)
        resolved = await asyncio.to_thread(_directory_key, Path(path))
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                f"DELETE FROM {table} WHERE project_id = ? AND path = ?",
                (project.project_id, str(resolved)),
            )
            removed = cursor.rowcount > 0
            await cursor.close()
            if not removed:
                return False
            await self._advance_project_config_version(
                connection,
                project.project_id,
                expected_version=expected_version,
            )
        return True

    async def config_snapshot(
        self,
        project: ProjectSnapshot,
    ) -> ProjectConfigSnapshot:
        if project.project_id is None:
            return ProjectConfigSnapshot(
                project_id=None,
                source=ProjectSource.IMPLICIT_HOME,
                cwd=project.cwd,
                timezone=project.timezone,
                config_version=project.config_version,
            )
        async with self._database.transaction() as connection:
            row = await _fetchone(
                connection,
                "SELECT * FROM projects WHERE id = ?",
                (project.project_id,),
            )
            if row is None:
                raise ProjectConfigError(f"project does not exist: {project.project_id}")
            return await _config_snapshot_from_connection(connection, row)

    async def config_snapshot_by_id(self, project_id: str) -> ProjectConfigSnapshot:
        async with self._database.transaction() as connection:
            row = await _fetchone(
                connection,
                "SELECT * FROM projects WHERE id = ?",
                (project_id,),
            )
            if row is None:
                raise ProjectConfigError(f"project does not exist: {project_id}")
            return await _config_snapshot_from_connection(connection, row)

    async def project_by_id(self, project_id: str) -> ProjectSnapshot:
        row = await self._database.fetchone(
            "SELECT * FROM projects WHERE id = ?",
            (project_id,),
        )
        if row is None:
            raise ProjectConfigError(f"project does not exist: {project_id}")
        return ProjectSnapshot(
            project_id=project_id,
            channel_id=str(row["channel_id"]),
            source=ProjectSource.EXPLICIT,
            root_path=Path(str(row["root_path"])),
            cwd=Path(str(row["cwd"])),
            config_version=int(row["config_version"]),
            timezone=str(row["timezone"]),
        )

    async def register_worktree_project(
        self,
        *,
        parent_project_id: str,
        project_id: str,
        path: Path,
        now: float | None = None,
    ) -> ProjectSnapshot:
        timestamp = time.time() if now is None else now
        resolved = await asyncio.to_thread(_validate_directory, path)
        async with self._database.transaction() as connection:
            existing = await _fetchone(
                connection,
                "SELECT * FROM projects WHERE id = ?",
                (project_id,),
            )
            if existing is not None:
                if (
                    str(existing["parent_project_id"]) != parent_project_id
                    or Path(str(existing["cwd"])) != resolved
                ):
                    raise ProjectConfigError("worktree project id conflicts with existing project")
                return ProjectSnapshot(
                    project_id=project_id,
                    channel_id=str(existing["channel_id"]),
                    source=ProjectSource.EXPLICIT,
                    root_path=Path(str(existing["root_path"])),
                    cwd=Path(str(existing["cwd"])),
                    config_version=int(existing["config_version"]),
                    timezone=str(existing["timezone"]),
                )
            parent = await _fetchone(
                connection,
                """
                SELECT * FROM projects
                WHERE id = ? AND state IN ('active', 'worktree')
                """,
                (parent_project_id,),
            )
            if parent is None:
                raise ProjectConfigError("parent project is not active")
            await connection.execute(
                """
                INSERT INTO projects(
                    id, channel_id, root_path, cwd, config_version, state,
                    parent_project_id, project_kind, timezone, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, 'worktree', ?, 'worktree', ?, ?, ?)
                """,
                (
                    project_id,
                    parent["channel_id"],
                    str(resolved),
                    str(resolved),
                    parent_project_id,
                    parent["timezone"],
                    timestamp,
                    timestamp,
                ),
            )
            for table, columns in (
                ("project_env", "name, value"),
                ("mcp_servers", "name, transport, config_json, enabled, version"),
                ("skill_dirs", "path, enabled"),
                ("plugin_dirs", "path, enabled"),
                (
                    "custom_agents",
                    "name, description, prompt, tools_json, enabled",
                ),
            ):
                await connection.execute(
                    f"""
                    INSERT INTO {table}(project_id, {columns})
                    SELECT ?, {columns} FROM {table} WHERE project_id = ?
                    """,
                    (project_id, parent_project_id),
                )
            created = await _fetchone(
                connection,
                "SELECT * FROM projects WHERE id = ?",
                (project_id,),
            )
            assert created is not None
            config = await _config_snapshot_from_connection(connection, created)
            await _insert_config_revision(connection, config, now=timestamp)
        return await self.project_by_id(project_id)

    async def retire_project(self, project_id: str, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        await self._database.execute(
            """
            UPDATE projects SET state = 'retired', updated_at = ?
            WHERE id = ? AND state != 'retired'
            """,
            (timestamp, project_id),
        )

    async def set_variable(self, project_id: str, name: str, value: str) -> int:
        if not _VARIABLE_NAME.fullmatch(name):
            raise ProjectConfigError(f"invalid environment variable name: {name!r}")

        async def mutate(connection: Connection) -> None:
            await connection.execute(
                """
                INSERT INTO project_env(project_id, name, value)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id, name) DO UPDATE SET value = excluded.value
                """,
                (project_id, name, value),
            )

        return await self._mutate_config(project_id, mutate)

    async def list_variables(
        self,
        project_id: str,
        *,
        reveal: bool = False,
    ) -> list[tuple[str, str]]:
        rows = await self._database.fetchall(
            "SELECT name, value FROM project_env WHERE project_id = ? ORDER BY name",
            (project_id,),
        )
        return [(str(row["name"]), str(row["value"]) if reveal else "********") for row in rows]

    async def remove_variable(self, project_id: str, name: str) -> int:
        async def mutate(connection: Connection) -> None:
            mcp_rows = await _fetchall(
                connection,
                "SELECT name, config_json FROM mcp_servers WHERE project_id = ?",
                (project_id,),
            )
            references = [
                str(row["name"])
                for row in mcp_rows
                if name in json.loads(str(row["config_json"])).get("env_refs", [])
            ]
            if references:
                raise ProjectConfigError(
                    f"project variable {name!r} is referenced by MCP servers: "
                    + ", ".join(sorted(references))
                )
            cursor = await connection.execute(
                "DELETE FROM project_env WHERE project_id = ? AND name = ?",
                (project_id, name),
            )
            deleted = cursor.rowcount
            await cursor.close()
            if deleted != 1:
                raise ProjectConfigError(f"project variable does not exist: {name}")

        return await self._mutate_config(project_id, mutate)

    async def add_mcp_server(
        self,
        project_id: str,
        *,
        name: str,
        transport: Literal["stdio", "http"],
        command: str | None = None,
        url: str | None = None,
        args: tuple[str, ...] = (),
        headers: dict[str, str] | None = None,
        env_refs: tuple[str, ...] = (),
    ) -> int:
        _validate_config_name(name, "MCP server")
        if transport == "stdio":
            if not command or url is not None:
                raise ProjectConfigError("stdio MCP requires command and forbids url")
        elif transport == "http":
            if not url or command is not None or args:
                raise ProjectConfigError("http MCP requires url and forbids command/args")
        else:
            raise ProjectConfigError(f"unsupported MCP transport: {transport}")
        for env_name in env_refs:
            if not _VARIABLE_NAME.fullmatch(env_name):
                raise ProjectConfigError(f"invalid MCP project-env reference: {env_name!r}")
        config = {
            "command": command,
            "url": url,
            "args": list(args),
            "headers": dict(sorted((headers or {}).items())),
            "env_refs": list(env_refs),
        }

        async def mutate(connection: Connection) -> None:
            if env_refs:
                placeholders = ", ".join("?" for _ in env_refs)
                rows = await _fetchall(
                    connection,
                    f"""
                    SELECT name FROM project_env
                    WHERE project_id = ? AND name IN ({placeholders})
                    """,
                    (project_id, *env_refs),
                )
                existing = {str(row["name"]) for row in rows}
                missing = sorted(set(env_refs) - existing)
                if missing:
                    raise ProjectConfigError(
                        "MCP project-env references do not exist: " + ", ".join(missing)
                    )
            await connection.execute(
                """
                INSERT INTO mcp_servers(
                    project_id, name, transport, config_json, enabled, version
                ) VALUES (?, ?, ?, ?, 1, 1)
                ON CONFLICT(project_id, name) DO UPDATE SET
                    transport = excluded.transport,
                    config_json = excluded.config_json,
                    enabled = 1,
                    version = mcp_servers.version + 1
                """,
                (project_id, name, transport, _canonical_json(config)),
            )

        return await self._mutate_config(project_id, mutate)

    async def toggle_mcp_server(
        self,
        project_id: str,
        name: str,
        enabled: bool,
        *,
        expected_version: int | None = None,
    ) -> int | ProjectMcpServerEntry:
        project = await self._database.fetchone(
            "SELECT 1 FROM projects WHERE id = ? AND state IN ('active', 'worktree')",
            (project_id,),
        )
        if project is None:
            return await self._toggle_mcp_server_for_channel(
                project_id,
                name=name,
                enabled=enabled,
                expected_version=expected_version,
            )

        async def mutate(connection: Connection) -> None:
            await _toggle_named_config(
                connection,
                table="mcp_servers",
                project_id=project_id,
                key_column="name",
                key=name,
                enabled=enabled,
            )
            await connection.execute(
                """
                UPDATE mcp_servers SET version = version + 1
                WHERE project_id = ? AND name = ?
                """,
                (project_id, name),
            )

        return await self._mutate_config(project_id, mutate)

    async def remove_mcp_server(
        self,
        project_id: str,
        name: str,
        *,
        expected_version: int | None = None,
    ) -> int | bool:
        project = await self._database.fetchone(
            "SELECT 1 FROM projects WHERE id = ? AND state IN ('active', 'worktree')",
            (project_id,),
        )
        if project is None:
            return await self._remove_mcp_server_for_channel(
                project_id,
                name=name,
                expected_version=expected_version,
            )
        return await self._remove_named_config(
            project_id,
            table="mcp_servers",
            key_column="name",
            key=name,
        )

    async def add_directory(
        self,
        project_id: str,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
    ) -> int:
        resolved = await asyncio.to_thread(_validate_directory, path)
        table = "skill_dirs" if kind == "skill" else "plugin_dirs"

        async def mutate(connection: Connection) -> None:
            await connection.execute(
                f"""
                INSERT INTO {table}(project_id, path, enabled)
                VALUES (?, ?, 1)
                ON CONFLICT(project_id, path) DO UPDATE SET enabled = 1
                """,
                (project_id, str(resolved)),
            )

        return await self._mutate_config(project_id, mutate)

    async def toggle_directory(
        self,
        project_id: str,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
        enabled: bool,
    ) -> int:
        resolved = await asyncio.to_thread(_resolve_path, path)
        table = "skill_dirs" if kind == "skill" else "plugin_dirs"

        async def mutate(connection: Connection) -> None:
            await _toggle_named_config(
                connection,
                table=table,
                project_id=project_id,
                key_column="path",
                key=str(resolved),
                enabled=enabled,
            )

        return await self._mutate_config(project_id, mutate)

    async def remove_directory(
        self,
        project_id: str,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
    ) -> int:
        table = "skill_dirs" if kind == "skill" else "plugin_dirs"
        resolved = await asyncio.to_thread(_resolve_path, path)
        return await self._remove_named_config(
            project_id,
            table=table,
            key_column="path",
            key=str(resolved),
        )

    async def add_custom_agent(
        self,
        project_id: str,
        *,
        name: str,
        description: str,
        prompt: str,
        tools: tuple[str, ...],
    ) -> int:
        _validate_config_name(name, "custom agent")
        if not description.strip() or not prompt.strip():
            raise ProjectConfigError("custom agent description and prompt are required")
        if any(not _CONFIG_NAME.fullmatch(tool) for tool in tools):
            raise ProjectConfigError("custom agent tools must use typed tool names")

        async def mutate(connection: Connection) -> None:
            await connection.execute(
                """
                INSERT INTO custom_agents(
                    project_id, name, description, prompt, tools_json, enabled
                ) VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(project_id, name) DO UPDATE SET
                    description = excluded.description,
                    prompt = excluded.prompt,
                    tools_json = excluded.tools_json,
                    enabled = 1
                """,
                (project_id, name, description, prompt, _canonical_json(list(tools))),
            )

        return await self._mutate_config(project_id, mutate)

    async def toggle_custom_agent(
        self,
        project_id: str,
        name: str,
        enabled: bool,
        *,
        expected_version: int | None = None,
    ) -> int | ProjectCustomAgentEntry:
        project = await self._database.fetchone(
            "SELECT 1 FROM projects WHERE id = ? AND state IN ('active', 'worktree')",
            (project_id,),
        )
        if project is None:
            return await self._toggle_custom_agent_for_channel(
                project_id,
                name=name,
                enabled=enabled,
                expected_version=expected_version,
            )

        async def mutate(connection: Connection) -> None:
            await _toggle_named_config(
                connection,
                table="custom_agents",
                project_id=project_id,
                key_column="name",
                key=name,
                enabled=enabled,
            )

        return await self._mutate_config(project_id, mutate)

    async def remove_custom_agent(
        self,
        project_id: str,
        name: str,
        *,
        expected_version: int | None = None,
    ) -> int | bool:
        project = await self._database.fetchone(
            "SELECT 1 FROM projects WHERE id = ? AND state IN ('active', 'worktree')",
            (project_id,),
        )
        if project is None:
            return await self._remove_custom_agent_for_channel(
                project_id,
                name=name,
                expected_version=expected_version,
            )
        return await self._remove_named_config(
            project_id,
            table="custom_agents",
            key_column="name",
            key=name,
        )

    async def _remove_named_config(
        self,
        project_id: str,
        *,
        table: str,
        key_column: str,
        key: str,
    ) -> int:
        if table not in {"mcp_servers", "skill_dirs", "plugin_dirs", "custom_agents"}:
            raise ValueError(f"unsupported project config table: {table}")
        if key_column not in {"name", "path"}:
            raise ValueError(f"unsupported project config key: {key_column}")

        async def mutate(connection: Connection) -> None:
            cursor = await connection.execute(
                f"DELETE FROM {table} WHERE project_id = ? AND {key_column} = ?",
                (project_id, key),
            )
            deleted = cursor.rowcount
            await cursor.close()
            if deleted != 1:
                raise ProjectConfigError(f"project config entry does not exist: {key}")

        return await self._mutate_config(project_id, mutate)

    async def _mutate_config(
        self,
        project_id: str,
        mutation: ConfigMutation,
        *,
        now: float | None = None,
    ) -> int:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            project = await _fetchone(
                connection,
                """
                SELECT * FROM projects
                WHERE id = ? AND state IN ('active', 'worktree')
                """,
                (project_id,),
            )
            if project is None:
                raise ProjectConfigError("project configuration requires an explicit project")
            await mutation(connection)
            await connection.execute(
                """
                UPDATE projects
                SET config_version = config_version + 1, updated_at = ?
                WHERE id = ?
                """,
                (timestamp, project_id),
            )
            updated = await _fetchone(
                connection,
                "SELECT * FROM projects WHERE id = ?",
                (project_id,),
            )
            assert updated is not None
            snapshot = await _config_snapshot_from_connection(connection, updated)
            await _insert_config_revision(connection, snapshot, now=timestamp)
            return snapshot.config_version

    def _require_initialized(self) -> None:
        if self._resolved_home is None:
            raise RuntimeError("project registry has not been initialized")


def _validate_directory(path: Path) -> Path:
    resolved = _directory_key(path)
    if not resolved.exists():
        raise ProjectPathError(f"project path does not exist: {resolved}")
    if not resolved.is_dir():
        raise ProjectPathError(f"project path is not a directory: {resolved}")
    return resolved


def _directory_key(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def _validate_env_name(name: str) -> str:
    normalized = name.strip()
    if not _ENV_NAME.fullmatch(normalized):
        raise ProjectValidationError(f"invalid environment variable name: {name}")
    return normalized


def _validate_resource_name(name: str, *, kind: str) -> str:
    normalized = name.strip()
    if not _RESOURCE_NAME.fullmatch(normalized):
        raise ProjectValidationError(f"invalid {kind}: {name}")
    return normalized


def _validate_tool_name(name: str) -> str:
    normalized = name.strip()
    if not _TOOL_NAME.fullmatch(normalized):
        raise ProjectValidationError(f"invalid tool name: {name}")
    return normalized


def _validate_text(value: str, *, kind: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ProjectValidationError(f"{kind} cannot be empty")
    return value


def _validate_mcp_transport(transport: str) -> str:
    normalized = transport.strip()
    if normalized not in {"stdio", "http"}:
        raise ProjectValidationError(f"unsupported MCP transport: {transport}")
    return normalized


def _normalize_mcp_config(transport: str, config: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(config, Mapping):
        raise ProjectValidationError("MCP config must be a mapping")
    normalized = _jsonable_copy(dict(config))
    if transport == "stdio":
        command = normalized.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ProjectValidationError("stdio MCP config requires a command")
    else:
        url = normalized.get("url")
        if not isinstance(url, str) or not url.strip():
            raise ProjectValidationError("http MCP config requires a url")
    if "args" in normalized:
        normalized["args"] = _validate_string_list(normalized["args"], kind="MCP args")
    if "headers" in normalized:
        normalized["headers"] = _validate_string_mapping(
            normalized["headers"],
            kind="MCP headers",
            key_validator=_validate_header_name,
        )
    if "env" in normalized:
        normalized["env"] = _validate_string_mapping(
            normalized["env"],
            kind="MCP env",
            key_validator=_validate_env_name,
        )
    if "project_env_refs" in normalized:
        normalized["project_env_refs"] = [
            _validate_env_name(item)
            for item in _validate_string_list(
                normalized["project_env_refs"],
                kind="project_env_refs",
            )
        ]
    if "cwd" in normalized and isinstance(normalized["cwd"], str):
        normalized["cwd"] = str(Path(normalized["cwd"]).expanduser())
    json.dumps(normalized, sort_keys=True)
    return normalized


def _validate_header_name(name: str) -> str:
    normalized = name.strip()
    if not _HEADER_NAME.fullmatch(normalized):
        raise ProjectValidationError(f"invalid header name: {name}")
    return normalized


def _validate_string_list(values: Any, *, kind: str) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise ProjectValidationError(f"{kind} must be a list of strings")
    if isinstance(values, Mapping):
        raise ProjectValidationError(f"{kind} must be a list of strings")
    result: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ProjectValidationError(f"{kind} must contain strings")
        result.append(value)
    return result


def _validate_string_mapping(
    values: Any,
    *,
    kind: str,
    key_validator: Callable[[str], str],
) -> dict[str, str]:
    if not isinstance(values, Mapping):
        raise ProjectValidationError(f"{kind} must be a mapping")
    result: dict[str, str] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ProjectValidationError(f"{kind} must map strings to strings")
        result[key_validator(key)] = value
    return result


def _jsonable_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable_copy(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable_copy(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ProjectValidationError(f"unsupported MCP config value: {type(value).__name__}")


def _redact_mcp_config(config: Mapping[str, Any]) -> dict[str, Any]:
    redacted = _jsonable_copy(config)
    if isinstance(redacted, dict):
        if "headers" in redacted and isinstance(redacted["headers"], dict):
            redacted["headers"] = {key: "[redacted]" for key in redacted["headers"]}
        if "env" in redacted and isinstance(redacted["env"], dict):
            redacted["env"] = {key: "[redacted]" for key in redacted["env"]}
    return redacted


def _sdk_mcp_server_config(
    entry: ProjectMcpServerEntry,
    project_env: Mapping[str, str],
    project_root: Path,
) -> tuple[dict[str, Any], set[str]]:
    config = _jsonable_copy(entry.config)
    common = {"tools", "timeout"}
    allowed = (
        common | {"command", "args", "env", "project_env_refs", "cwd"}
        if entry.transport == "stdio"
        else common | {"url", "headers"}
    )
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ProjectValidationError(
            f"MCP server {entry.name} has unsupported SDK fields: {', '.join(unknown)}"
        )
    result: dict[str, Any] = {
        "type": entry.transport,
    }
    references: set[str] = set()
    if entry.transport == "stdio":
        result["command"] = str(config["command"])
        if "args" in config:
            result["args"] = _validate_string_list(config["args"], kind="MCP args")
        environment = dict(config.get("env", {}))
        for name in config.get("project_env_refs", []):
            if name not in project_env:
                raise ProjectValidationError(
                    f"MCP server {entry.name} references missing project variable {name}"
                )
            environment[name] = project_env[name]
            references.add(name)
        if environment:
            result["env"] = environment
        if "cwd" in config:
            cwd = Path(str(config["cwd"]))
            if not cwd.is_absolute():
                cwd = project_root / cwd
            result["working_directory"] = str(_validate_directory(cwd))
    else:
        result["url"] = str(config["url"])
        if "headers" in config:
            result["headers"] = dict(config["headers"])
    if "tools" in config:
        result["tools"] = _validate_string_list(config["tools"], kind="MCP tools")
    if "timeout" in config:
        timeout = config["timeout"]
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
            raise ProjectValidationError("MCP timeout must be a positive integer")
        result["timeout"] = timeout
    return result, references


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve()


async def _config_snapshot_from_connection(
    connection: Connection,
    project: Row,
) -> ProjectConfigSnapshot:
    project_id = str(project["id"])
    variables = await _fetchall(
        connection,
        "SELECT name, value FROM project_env WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    mcp_rows = await _fetchall(
        connection,
        "SELECT * FROM mcp_servers WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    skill_rows = await _fetchall(
        connection,
        "SELECT path, enabled FROM skill_dirs WHERE project_id = ? ORDER BY path",
        (project_id,),
    )
    plugin_rows = await _fetchall(
        connection,
        "SELECT path, enabled FROM plugin_dirs WHERE project_id = ? ORDER BY path",
        (project_id,),
    )
    agent_rows = await _fetchall(
        connection,
        "SELECT * FROM custom_agents WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    mcp_servers: list[McpServerSnapshot] = []
    for row in mcp_rows:
        config = json.loads(str(row["config_json"]))
        headers = config.get("headers")
        mcp_servers.append(
            McpServerSnapshot(
                name=str(row["name"]),
                transport=str(row["transport"]),
                command=(None if config.get("command") is None else str(config["command"])),
                url=None if config.get("url") is None else str(config["url"]),
                args=tuple(str(item) for item in config.get("args", [])),
                headers=tuple(
                    sorted(
                        (str(name), str(value))
                        for name, value in (headers.items() if isinstance(headers, dict) else ())
                    )
                ),
                env_refs=tuple(str(item) for item in config.get("env_refs", [])),
                enabled=bool(row["enabled"]),
                version=int(row["version"]),
            )
        )
    return ProjectConfigSnapshot(
        project_id=project_id,
        source=ProjectSource.EXPLICIT,
        cwd=Path(str(project["cwd"])),
        timezone=str(project["timezone"]),
        config_version=int(project["config_version"]),
        variables=tuple((str(row["name"]), str(row["value"])) for row in variables),
        mcp_servers=tuple(mcp_servers),
        skill_dirs=tuple(
            DirectorySnapshot(path=Path(str(row["path"])), enabled=bool(row["enabled"]))
            for row in skill_rows
        ),
        plugin_dirs=tuple(
            DirectorySnapshot(path=Path(str(row["path"])), enabled=bool(row["enabled"]))
            for row in plugin_rows
        ),
        custom_agents=tuple(
            CustomAgentSnapshot(
                name=str(row["name"]),
                description=str(row["description"]),
                prompt=str(row["prompt"]),
                tools=tuple(str(item) for item in json.loads(str(row["tools_json"]))),
                enabled=bool(row["enabled"]),
            )
            for row in agent_rows
        ),
    )


async def _insert_config_revision(
    connection: Connection,
    snapshot: ProjectConfigSnapshot,
    *,
    now: float,
) -> None:
    if snapshot.project_id is None:
        return
    await connection.execute(
        """
        INSERT INTO project_config_revisions(
            project_id, config_version, snapshot_json, snapshot_hash, created_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(project_id, config_version) DO NOTHING
        """,
        (
            snapshot.project_id,
            snapshot.config_version,
            snapshot.canonical_json(),
            snapshot.snapshot_hash,
            now,
        ),
    )


async def _toggle_named_config(
    connection: Connection,
    *,
    table: str,
    project_id: str,
    key_column: str,
    key: str,
    enabled: bool,
) -> None:
    if table not in {"mcp_servers", "skill_dirs", "plugin_dirs", "custom_agents"}:
        raise ValueError(f"unsupported project config table: {table}")
    if key_column not in {"name", "path"}:
        raise ValueError(f"unsupported project config key: {key_column}")
    cursor = await connection.execute(
        f"""
        UPDATE {table} SET enabled = ?
        WHERE project_id = ? AND {key_column} = ?
        """,
        (int(enabled), project_id, key),
    )
    changed = cursor.rowcount
    await cursor.close()
    if changed != 1:
        raise ProjectConfigError(f"project config entry does not exist: {key}")


def _validate_config_name(name: str, kind: str) -> None:
    if not _CONFIG_NAME.fullmatch(name):
        raise ProjectConfigError(f"invalid {kind} name: {name!r}")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def _fetchone(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> Row | None:
    cursor = await connection.execute(statement, parameters)
    row = await cursor.fetchone()
    await cursor.close()
    return row


async def _fetchall(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> list[Row]:
    cursor = await connection.execute(statement, parameters)
    rows = list(await cursor.fetchall())
    await cursor.close()
    return rows


async def _require_no_active_worktree_intents(
    connection: Connection,
    channel_id: str,
) -> None:
    row = await _fetchone(
        connection,
        """
        SELECT i.intent_id, i.state
        FROM projects p
        JOIN worktree_intents i ON i.parent_project_id = p.id
        WHERE p.channel_id = ? AND p.state = 'active'
          AND i.state NOT IN ('closed', 'failed', 'compensated')
        LIMIT 1
        """,
        (channel_id,),
    )
    if row is not None:
        raise ProjectConfigError(
            f"project has nonterminal worktree intent {row['intent_id']} ({row['state']})"
        )
