from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from copilotd.core.commands import (
    CDConflictError,
    CDInputError,
    CDPathError,
    CDProjectError,
)
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
            return ProjectSnapshot(
                project_id=None,
                channel_id=channel_id,
                source=ProjectSource.IMPLICIT_HOME,
                root_path=self.resolved_home,
                cwd=self.resolved_home,
                config_version=1,
            )
        return ProjectSnapshot(
            project_id=row["id"],
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=Path(row["root_path"]),
            cwd=Path(row["cwd"]),
            config_version=row["config_version"],
        )

    async def bind(self, channel_id: str, path: Path) -> ProjectSnapshot:
        self._require_initialized()
        resolved = await asyncio.to_thread(_validate_directory, path)
        now = time.time()
        project_id = str(uuid.uuid4())
        async with self._database.transaction() as connection:
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
                    state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    project_id,
                    channel_id,
                    str(resolved),
                    str(resolved),
                    config_version,
                    now,
                    now,
                ),
            )
        return ProjectSnapshot(
            project_id=project_id,
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=resolved,
            cwd=resolved,
            config_version=config_version,
        )

    async def unbind(self, channel_id: str) -> ProjectSnapshot:
        self._require_initialized()
        now = time.time()
        await self._database.execute(
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

    async def channel_settings(self, channel_id: str) -> tuple[str, bool, int]:
        row = await self._database.fetchone(
            "SELECT * FROM channel_settings WHERE channel_id = ?",
            (channel_id,),
        )
        if row is None:
            return "text", False, 1
        return row["layout"], bool(row["mention_required"]), row["config_version"]

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

    async def toggle_mcp_server(
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

    async def remove_mcp_server(
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

    async def toggle_custom_agent(
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

    async def remove_custom_agent(
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
