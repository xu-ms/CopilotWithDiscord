from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from copilotd.core.projects import (
    ProjectConfigError,
    ProjectConfigSnapshot,
    ProjectRegistry,
    ProjectSnapshot,
)
from copilotd.core.scheduler import (
    MisfirePolicy,
    ScheduleDefinition,
    ScheduleKind,
    SchedulerRepository,
    SchedulerStatus,
    ScheduleRun,
)
from copilotd.core.worktrees import (
    WorktreeHistoryMode,
    WorktreeManager,
    WorktreeProjection,
)
from copilotd.storage.database import Database


class DiscordParentType(StrEnum):
    TEXT = "text"
    FORUM = "forum"


class LifecycleCommandError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ScheduleDetailProjection:
    definition: ScheduleDefinition
    runs: tuple[ScheduleRun, ...]


@dataclass(frozen=True, slots=True)
class ProjectInfoProjection:
    project: ProjectSnapshot
    config: ProjectConfigSnapshot
    layout: str
    mention_required: bool
    resident_sessions: tuple[dict[str, Any], ...]
    worktrees: tuple[dict[str, Any], ...]
    schedules: tuple[dict[str, Any], ...]


class SchedulerCommandService:
    def __init__(
        self,
        database: Database,
        projects: ProjectRegistry,
        repository: SchedulerRepository,
    ) -> None:
        self._database = database
        self._projects = projects
        self._repository = repository

    async def create_message(
        self,
        *,
        thread_id: str,
        expression: str,
        text: str,
        timezone: str | None,
        created_by: str,
        channel_id: str | None = None,
        name: str | None = None,
        misfire_policy: MisfirePolicy = MisfirePolicy.LATEST,
        now: float | None = None,
    ) -> ScheduleDefinition:
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM session_bindings
                WHERE thread_id = ? AND binding_intent IN ('active', 'closed')
                """,
                (thread_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                raise LifecycleCommandError(
                    "message schedules require an existing session thread",
                    code="CD-SCHEDULE-TARGET-001",
                )
            project_timezone = timezone
            if project_timezone is None:
                if row["project_id"] is None:
                    snapshot_channel = (
                        str(json.loads(row["project_snapshot_json"])["channel_id"])
                        if row["project_snapshot_json"]
                        else channel_id
                    )
                    if snapshot_channel is None:
                        raise LifecycleCommandError(
                            "legacy implicit-home sessions require timezone or channel_id",
                            code="CD-SCHEDULE-TZ-002",
                        )
                    project_timezone = await self._projects.channel_timezone(snapshot_channel)
                else:
                    project_timezone = (
                        await self._projects.project_by_id(str(row["project_id"]))
                    ).timezone
            target = {
                "thread_id": str(row["thread_id"]),
                "sdk_session_id": str(row["sdk_session_id"]),
                "project_id": row["project_id"],
                "project_source": str(row["project_source"]),
                "cwd_snapshot": str(row["cwd_snapshot"]),
                "project_snapshot": (
                    None
                    if row["project_snapshot_json"] is None
                    else json.loads(str(row["project_snapshot_json"]))
                ),
                "session_config": (
                    None
                    if row["session_config_snapshot_json"] is None
                    else json.loads(str(row["session_config_snapshot_json"]))
                ),
                "execution_config": {
                    "mode": str(row["desired_mode"]),
                    "model_config": json.loads(str(row["desired_model_config"])),
                    "agent": str(row["desired_agent"]),
                    "session_config_version": int(row["desired_project_config_version"]),
                },
            }
            return await self._repository.create(
                kind=ScheduleKind.MESSAGE,
                expression=expression,
                timezone=str(project_timezone),
                payload={"text": text},
                target_snapshot=target,
                project_id=row["project_id"],
                thread_id=thread_id,
                channel_id=(
                    channel_id
                    if target["project_snapshot"] is None
                    else str(target["project_snapshot"]["channel_id"])
                ),
                name=name,
                created_by=created_by,
                misfire_policy=misfire_policy,
                now=now,
                connection=connection,
            )

    async def create_new_session(
        self,
        *,
        channel_id: str,
        expression: str,
        text: str,
        timezone: str | None,
        created_by: str,
        name: str | None = None,
        thread_name: str = "Scheduled Copilot session",
        misfire_policy: MisfirePolicy = MisfirePolicy.LATEST,
        now: float | None = None,
    ) -> ScheduleDefinition:
        project = await self._projects.resolve(channel_id)
        config = await self._projects.config_snapshot(project)
        target = {
            "project": _project_dict(project),
            "project_config": config.to_dict(),
            "execution_config": {
                "mode": "interactive",
                "model_config": {},
                "agent": "default",
                "session_config_version": config.config_version,
            },
        }
        return await self._repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression=expression,
            timezone=timezone or project.timezone,
            payload={"text": text, "thread_name": thread_name},
            target_snapshot=target,
            project_id=project.project_id,
            channel_id=channel_id,
            name=name,
            created_by=created_by,
            misfire_policy=misfire_policy,
            now=now,
        )

    async def list(
        self,
        *,
        project_id: str | None = None,
        thread_id: str | None = None,
        channel_id: str | None = None,
    ) -> list[ScheduleDefinition]:
        return await self._repository.list(
            project_id=project_id,
            thread_id=thread_id,
            channel_id=channel_id,
        )

    async def show(self, schedule_id: str) -> ScheduleDetailProjection:
        return ScheduleDetailProjection(
            definition=await self._repository.require(schedule_id),
            runs=tuple(await self._repository.list_runs(schedule_id)),
        )

    async def toggle(self, schedule_id: str, *, enabled: bool) -> ScheduleDefinition:
        return await self._repository.toggle(schedule_id, enabled=enabled)

    async def delete(self, schedule_id: str) -> None:
        await self._repository.delete(schedule_id)

    async def run_now(self, schedule_id: str) -> ScheduleRun:
        return await self._repository.run_now(schedule_id)

    async def status(self) -> SchedulerStatus:
        return await self._repository.status()


class ProjectLifecycleService:
    def __init__(
        self,
        database: Database,
        projects: ProjectRegistry,
    ) -> None:
        self._database = database
        self._projects = projects

    async def set_layout(
        self,
        channel_id: str,
        *,
        layout: Literal["text", "forum"],
        parent_type: DiscordParentType,
    ) -> None:
        if layout != parent_type.value:
            raise LifecycleCommandError(
                f"layout {layout!r} is incompatible with Discord parent type {parent_type.value!r}",
                code="CD-PROJECT-LAYOUT-001",
            )
        await self._projects.set_layout(channel_id, layout)

    async def set_mention_required(self, channel_id: str, required: bool) -> None:
        await self._projects.set_mention_required(channel_id, required)

    async def set_timezone(self, channel_id: str, timezone: str) -> None:
        await self._projects.set_channel_timezone(channel_id, timezone)

    async def variable_set(self, project_id: str | None, name: str, value: str) -> int:
        return await self._projects.set_variable(
            _require_explicit(project_id),
            name,
            value,
        )

    async def variable_list(
        self,
        project_id: str | None,
        *,
        reveal: bool = False,
    ) -> list[tuple[str, str]]:
        return await self._projects.list_variables(
            _require_explicit(project_id),
            reveal=reveal,
        )

    async def variable_remove(self, project_id: str | None, name: str) -> int:
        return await self._projects.remove_variable(_require_explicit(project_id), name)

    async def mcp_add(
        self,
        project_id: str | None,
        *,
        name: str,
        transport: Literal["stdio", "http"],
        command: str | None = None,
        url: str | None = None,
        args: tuple[str, ...] = (),
        headers: dict[str, str] | None = None,
        env_refs: tuple[str, ...] = (),
    ) -> int:
        return await self._projects.add_mcp_server(
            _require_explicit(project_id),
            name=name,
            transport=transport,
            command=command,
            url=url,
            args=args,
            headers=headers,
            env_refs=env_refs,
        )

    async def mcp_list(self, project_id: str | None) -> tuple[Any, ...]:
        snapshot = await self._projects.config_snapshot_by_id(_require_explicit(project_id))
        return snapshot.mcp_servers

    async def mcp_toggle(
        self,
        project_id: str | None,
        *,
        name: str,
        enabled: bool,
    ) -> int:
        return await self._projects.toggle_mcp_server(
            _require_explicit(project_id),
            name,
            enabled,
        )

    async def mcp_remove(self, project_id: str | None, *, name: str) -> int:
        return await self._projects.remove_mcp_server(
            _require_explicit(project_id),
            name,
        )

    async def directory_add(
        self,
        project_id: str | None,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
    ) -> int:
        return await self._projects.add_directory(
            _require_explicit(project_id),
            kind=kind,
            path=path,
        )

    async def directory_list(
        self,
        project_id: str | None,
        *,
        kind: Literal["skill", "plugin"],
    ) -> tuple[Any, ...]:
        snapshot = await self._projects.config_snapshot_by_id(_require_explicit(project_id))
        return snapshot.skill_dirs if kind == "skill" else snapshot.plugin_dirs

    async def directory_toggle(
        self,
        project_id: str | None,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
        enabled: bool,
    ) -> int:
        return await self._projects.toggle_directory(
            _require_explicit(project_id),
            kind=kind,
            path=path,
            enabled=enabled,
        )

    async def directory_remove(
        self,
        project_id: str | None,
        *,
        kind: Literal["skill", "plugin"],
        path: Path,
    ) -> int:
        return await self._projects.remove_directory(
            _require_explicit(project_id),
            kind=kind,
            path=path,
        )

    async def agent_add(
        self,
        project_id: str | None,
        *,
        name: str,
        description: str,
        prompt: str,
        tools: tuple[str, ...],
    ) -> int:
        return await self._projects.add_custom_agent(
            _require_explicit(project_id),
            name=name,
            description=description,
            prompt=prompt,
            tools=tools,
        )

    async def agent_list(self, project_id: str | None) -> tuple[Any, ...]:
        snapshot = await self._projects.config_snapshot_by_id(_require_explicit(project_id))
        return snapshot.custom_agents

    async def agent_toggle(
        self,
        project_id: str | None,
        *,
        name: str,
        enabled: bool,
    ) -> int:
        return await self._projects.toggle_custom_agent(
            _require_explicit(project_id),
            name,
            enabled,
        )

    async def agent_remove(self, project_id: str | None, *, name: str) -> int:
        return await self._projects.remove_custom_agent(
            _require_explicit(project_id),
            name,
        )

    async def info(self, channel_id: str) -> ProjectInfoProjection:
        project = await self._projects.resolve(channel_id)
        config = await self._projects.config_snapshot(project)
        layout, mention, _version = await self._projects.channel_settings(channel_id)
        if project.project_id is None:
            session_rows: list[Any] = []
            worktree_rows: list[Any] = []
            schedule_rows: list[Any] = []
        else:
            session_rows = await self._database.fetchall(
                """
                SELECT thread_id, sdk_session_id, binding_intent, attachment_state,
                       cwd_snapshot
                FROM session_bindings WHERE project_id = ? ORDER BY created_at
                """,
                (project.project_id,),
            )
            worktree_rows = await self._database.fetchall(
                """
                SELECT name, branch_name, path, state
                FROM project_worktrees WHERE parent_project_id = ?
                ORDER BY created_at
                """,
                (project.project_id,),
            )
            schedule_rows = await self._database.fetchall(
                """
                SELECT id, kind, state, next_run_at_utc
                FROM schedules WHERE project_id = ? AND state != 'deleted'
                ORDER BY created_at
                """,
                (project.project_id,),
            )
        return ProjectInfoProjection(
            project=project,
            config=config,
            layout=layout,
            mention_required=mention,
            resident_sessions=tuple(dict(row) for row in session_rows),
            worktrees=tuple(dict(row) for row in worktree_rows),
            schedules=tuple(dict(row) for row in schedule_rows),
        )


class WorktreeCommandService:
    def __init__(self, manager: WorktreeManager) -> None:
        self._manager = manager

    @property
    def history_fork_available(self) -> bool:
        return self._manager.history_fork_available

    async def create(
        self,
        *,
        project_id: str | None,
        name: str,
        base_ref: str = "HEAD",
        history: Literal["none", "fork"] = "none",
        source_session_id: str | None = None,
    ) -> WorktreeProjection:
        return await self._manager.create(
            parent_project_id=_require_explicit(project_id),
            name=name,
            base_ref=base_ref,
            history_mode=WorktreeHistoryMode(history),
            source_session_id=source_session_id,
        )

    async def list(self, project_id: str | None) -> list[WorktreeProjection]:
        return await self._manager.list(parent_project_id=_require_explicit(project_id))

    async def close(
        self,
        project_id: str | None,
        *,
        name: str,
    ) -> WorktreeProjection:
        return await self._manager.close(
            name,
            parent_project_id=_require_explicit(project_id),
        )


def _require_explicit(project_id: str | None) -> str:
    if project_id is None:
        raise ProjectConfigError("this command requires an explicit project binding")
    return project_id


def _project_dict(project: ProjectSnapshot) -> dict[str, Any]:
    return {
        "project_id": project.project_id,
        "channel_id": project.channel_id,
        "source": project.source.value,
        "root_path": str(project.root_path),
        "cwd": str(project.cwd),
        "config_version": project.config_version,
        "timezone": project.timezone,
    }
