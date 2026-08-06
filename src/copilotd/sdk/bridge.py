from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, cast

from copilot import CopilotClient, RuntimeConnection
from copilot.generated.rpc import (
    AgentSelectRequest,
    CommandsInvokeRequest,
    CommandsListRequest,
    EventLogReadRequest,
    FleetStartRequest,
    HistoryCompactRequest,
    MetadataContextInfoRequest,
    ModeSetRequest,
    PermissionsAllowAllMode,
    PermissionsSetAAllSource,
    PermissionsSetAllowAllRequest,
    PermissionsSetApproveAllRequest,
    RemoteEnableRequest,
    RemoteSessionMode,
    ScheduleStopRequest,
    SessionMode,
    SessionsCheckInUseRequest,
    TasksCancelRequest,
    TasksGetProgressRequest,
    TasksPromoteToBackgroundRequest,
    TasksRemoveRequest,
    TasksSendMessageRequest,
    UIEphemeralQueryRequest,
)
from copilot.session import CopilotSession, PermissionHandler
from copilot.session_events import SessionEvent

from copilotd.config import Settings
from copilotd.core.session_config import SessionLaunchOptions
from copilotd.sdk.native import (
    NativeCommandDefinition,
    NativeCommandResult,
    parse_command_result,
)


class PermissionPostureError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PermissionPosture:
    enabled: bool
    mode: str | None


@dataclass(frozen=True, slots=True)
class EventLogBatch:
    cursor: str
    cursor_status: str
    events: tuple[SessionEvent, ...]
    has_more: bool
    filtered_ephemeral: int


class CopilotBridge:
    """Small typed facade over the official SDK and its generated session RPC."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: CopilotClient | None = None

    @property
    def client(self) -> CopilotClient:
        if self._client is None:
            raise RuntimeError("Copilot bridge is not started")
        return self._client

    async def start(self) -> None:
        if self._client is not None:
            return
        if self._settings.runtime_uri:
            connection = RuntimeConnection.for_uri(
                self._settings.runtime_uri,
                connection_token=(
                    None
                    if self._settings.runtime_connection_token is None
                    else self._settings.runtime_connection_token.get_secret_value()
                ),
            )
        else:
            connection = RuntimeConnection.for_stdio(args=("--yolo",))
        client = CopilotClient(
            connection=connection,
            log_level=cast(
                Literal["none", "error", "warning", "info", "debug", "all"],
                self._settings.sdk_log_level,
            ),
            session_idle_timeout_seconds=0,
            enable_remote_sessions=True,
        )
        try:
            await client.start()
        except BaseException:
            await client.stop()
            raise
        self._client = client

    async def stop(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            try:
                async with asyncio.timeout(self._settings.sdk_shutdown_timeout_seconds):
                    await client.stop()
            except TimeoutError:
                async with asyncio.timeout(self._settings.sdk_shutdown_timeout_seconds):
                    await client.force_stop()
        finally:
            if self._client is client:
                self._client = None

    async def force_stop(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            async with asyncio.timeout(self._settings.sdk_shutdown_timeout_seconds):
                await client.force_stop()
        finally:
            if self._client is client:
                self._client = None

    async def __aenter__(self) -> CopilotBridge:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def create_session(
        self,
        *,
        session_id: str,
        working_directory: str,
        on_event: Callable[[SessionEvent], None],
        on_user_input_request: Callable[..., Any] | None = None,
        on_exit_plan_mode_request: Callable[..., Any] | None = None,
        on_auto_mode_switch_request: Callable[..., Any] | None = None,
        session_config: dict[str, Any] | None = None,
        launch_options: SessionLaunchOptions | None = None,
    ) -> CopilotSession:
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
        launch_kwargs.update(_session_config_kwargs(session_config))
        return await self.client.create_session(
            session_id=session_id,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            on_event=on_event,
            on_permission_request=PermissionHandler.approve_all,
            on_user_input_request=on_user_input_request,
            on_exit_plan_mode_request=on_exit_plan_mode_request,
            on_auto_mode_switch_request=on_auto_mode_switch_request,
            **launch_kwargs,
        )

    async def resume_session(
        self,
        *,
        session_id: str,
        working_directory: str,
        on_event: Callable[[SessionEvent], None],
        continue_pending_work: bool = True,
        on_user_input_request: Callable[..., Any] | None = None,
        on_exit_plan_mode_request: Callable[..., Any] | None = None,
        on_auto_mode_switch_request: Callable[..., Any] | None = None,
        session_config: dict[str, Any] | None = None,
        launch_options: SessionLaunchOptions | None = None,
    ) -> CopilotSession:
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
        launch_kwargs.update(_session_config_kwargs(session_config))
        return await self.client.resume_session(
            session_id,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            continue_pending_work=continue_pending_work,
            on_event=on_event,
            on_permission_request=PermissionHandler.approve_all,
            on_user_input_request=on_user_input_request,
            on_exit_plan_mode_request=on_exit_plan_mode_request,
            on_auto_mode_switch_request=on_auto_mode_switch_request,
            **launch_kwargs,
        )

    async def ensure_allow_all(self, session: CopilotSession) -> PermissionPosture:
        state = await session.rpc.permissions.get_allow_all(timeout=10)
        if not state.enabled or state.mode != PermissionsAllowAllMode.ON:
            await session.rpc.permissions.set_allow_all(
                PermissionsSetAllowAllRequest(
                    enabled=True,
                    mode=PermissionsAllowAllMode.ON,
                    source=PermissionsSetAAllSource.RPC,
                ),
                timeout=10,
            )
            await session.rpc.permissions.set_approve_all(
                PermissionsSetApproveAllRequest(
                    enabled=True,
                    source=PermissionsSetAAllSource.RPC,
                ),
                timeout=10,
            )
            state = await session.rpc.permissions.get_allow_all(timeout=10)

        posture = PermissionPosture(
            enabled=state.enabled,
            mode=None if state.mode is None else state.mode.value,
        )
        if not posture.enabled or posture.mode != PermissionsAllowAllMode.ON.value:
            raise PermissionPostureError(
                f"full allow-all was not confirmed: enabled={posture.enabled}, mode={posture.mode}"
            )
        return posture

    async def get_mode(self, session: CopilotSession) -> str:
        return (await session.rpc.mode.get(timeout=10)).value

    async def set_mode(self, session: CopilotSession, mode: str) -> None:
        await session.rpc.mode.set(
            ModeSetRequest(mode=SessionMode(mode)),
            timeout=10,
        )

    async def list_models(self) -> list[dict[str, Any]]:
        return [model.to_dict() for model in await self.client.list_models()]

    async def set_model(
        self,
        session: CopilotSession,
        *,
        model: str,
        reasoning_effort: str | None,
        context_tier: str | None,
        reasoning_summary: str | None = None,
    ) -> None:
        await session.set_model(
            model,
            reasoning_effort=reasoning_effort,
            reasoning_summary=reasoning_summary,
            context_tier=cast(Any, context_tier),
        )

    async def get_current_model(self, session: CopilotSession) -> dict[str, Any]:
        current = await session.rpc.model.get_current(timeout=10)
        return cast(dict[str, Any], current.to_dict())

    async def get_context(self, session: CopilotSession) -> dict[str, Any] | None:
        result = await session.rpc.metadata.context_info(
            MetadataContextInfoRequest(
                output_token_limit=0,
                prompt_token_limit=0,
                selected_model=None,
            ),
            timeout=10,
        )
        return None if result.context_info is None else result.context_info.to_dict()

    async def get_usage(self, session: CopilotSession) -> dict[str, Any]:
        metrics = await session.rpc.usage.get_metrics(timeout=10)
        return cast(dict[str, Any], metrics.to_dict())

    async def get_readiness(self, session: CopilotSession) -> dict[str, Any]:
        processing, activity, pending = await asyncio.gather(
            session.rpc.metadata.is_processing(timeout=10),
            session.rpc.metadata.activity(timeout=10),
            session.rpc.queue.pending_items(timeout=10),
        )
        return {
            "processing": processing.processing,
            "hasActiveWork": activity.has_active_work,
            "abortable": activity.abortable,
            "pendingItems": [item.to_dict() for item in pending.items],
            "steeringMessages": list(pending.steering_messages),
        }

    async def get_tasks(self, session: CopilotSession) -> list[dict[str, Any]]:
        await self.refresh_tasks(session)
        return await self.list_tasks(session)

    async def refresh_tasks(self, session: CopilotSession) -> None:
        await session.rpc.tasks.refresh(timeout=10)

    async def list_tasks(self, session: CopilotSession) -> list[dict[str, Any]]:
        tasks = await session.rpc.tasks.list(timeout=10)
        return [cast(dict[str, Any], task.to_dict()) for task in tasks.tasks]

    async def get_task_progress(
        self,
        session: CopilotSession,
        task_id: str,
    ) -> dict[str, Any] | None:
        result = await session.rpc.tasks.get_progress(
            TasksGetProgressRequest(id=task_id),
            timeout=10,
        )
        return None if result.progress is None else cast(dict[str, Any], result.progress.to_dict())

    async def send_task_message(
        self,
        session: CopilotSession,
        task_id: str,
        message: str,
    ) -> dict[str, Any]:
        result = await session.rpc.tasks.send_message(
            TasksSendMessageRequest(id=task_id, message=message),
            timeout=10,
        )
        return cast(dict[str, Any], result.to_dict())

    async def get_current_promotable_task(
        self,
        session: CopilotSession,
    ) -> dict[str, Any] | None:
        result = await session.rpc.tasks.get_current_promotable(timeout=10)
        return None if result.task is None else cast(dict[str, Any], result.task.to_dict())

    async def promote_task(self, session: CopilotSession, task_id: str) -> bool:
        result = await session.rpc.tasks.promote_to_background(
            TasksPromoteToBackgroundRequest(id=task_id),
            timeout=10,
        )
        return result.promoted

    async def cancel_task(self, session: CopilotSession, task_id: str) -> bool:
        result = await session.rpc.tasks.cancel(
            TasksCancelRequest(id=task_id),
            timeout=10,
        )
        return result.cancelled

    async def remove_task(self, session: CopilotSession, task_id: str) -> bool:
        result = await session.rpc.tasks.remove(
            TasksRemoveRequest(id=task_id),
            timeout=10,
        )
        return result.removed

    async def wait_for_tasks(
        self,
        session: CopilotSession,
        *,
        wait_seconds: float,
    ) -> None:
        await session.rpc.tasks.wait_for_pending(timeout=wait_seconds)

    async def get_native_schedules(self, session: CopilotSession) -> list[dict[str, Any]]:
        schedules = await session.rpc.schedule.list(timeout=10)
        return [cast(dict[str, Any], entry.to_dict()) for entry in schedules.entries]

    async def stop_native_schedule(
        self,
        session: CopilotSession,
        schedule_id: int,
    ) -> dict[str, Any] | None:
        result = await session.rpc.schedule.stop(
            ScheduleStopRequest(id=schedule_id),
            timeout=10,
        )
        return None if result.entry is None else cast(dict[str, Any], result.entry.to_dict())

    async def get_remote_state(self, session: CopilotSession) -> dict[str, Any]:
        snapshot = cast(
            dict[str, Any],
            (await session.rpc.metadata.snapshot(timeout=10)).to_dict(),
        )
        return {
            "is_remote_session": bool(snapshot.get("isRemote")),
            "metadata": snapshot,
        }

    async def get_current_agent(self, session: CopilotSession) -> str:
        current = await session.rpc.agent.get_current(timeout=10)
        if current.agent is None:
            return "default"
        payload = cast(dict[str, Any], current.agent.to_dict())
        return str(payload.get("name") or payload.get("displayName") or "default")

    async def list_agents(self, session: CopilotSession) -> list[dict[str, Any]]:
        result = await session.rpc.agent.list(timeout=10)
        return [_safe_agent_info(agent.to_dict()) for agent in result.agents]

    async def get_current_agent_info(
        self,
        session: CopilotSession,
    ) -> dict[str, Any] | None:
        result = await session.rpc.agent.get_current(timeout=10)
        return None if result.agent is None else _safe_agent_info(result.agent.to_dict())

    async def select_agent(
        self,
        session: CopilotSession,
        name: str,
    ) -> dict[str, Any]:
        result = await session.rpc.agent.select(
            AgentSelectRequest(name=name),
            timeout=10,
        )
        return _safe_agent_info(result.agent.to_dict())

    async def deselect_agent(self, session: CopilotSession) -> None:
        await session.rpc.agent.deselect(timeout=10)

    async def list_commands(
        self,
        session: CopilotSession,
        *,
        include_builtins: bool,
    ) -> tuple[NativeCommandDefinition, ...]:
        result = await session.rpc.commands.list(
            CommandsListRequest(
                include_builtins=include_builtins,
                include_client_commands=False,
                include_skills=False,
            ),
            timeout=10,
        )
        return tuple(NativeCommandDefinition.from_sdk(command) for command in result.commands)

    async def invoke_command(
        self,
        session: CopilotSession,
        *,
        name: str,
        input_text: str | None,
    ) -> NativeCommandResult:
        result = await session.rpc.commands.invoke(
            CommandsInvokeRequest(name=name, input=input_text),
            timeout=10,
        )
        return parse_command_result(result)

    async def ephemeral_query(
        self,
        session: CopilotSession,
        question: str,
    ) -> str:
        result = await session.rpc.ui.ephemeral_query(
            UIEphemeralQueryRequest(question=question),
            timeout=30,
        )
        return result.answer

    async def compact_history(
        self,
        session: CopilotSession,
        *,
        focus: str | None,
    ) -> dict[str, Any]:
        result = await session.rpc.history.compact(
            HistoryCompactRequest(custom_instructions=focus),
            timeout=30,
        )
        return cast(dict[str, Any], result.to_dict())

    async def start_fleet(
        self,
        session: CopilotSession,
        prompt: str,
        *,
        timeout_seconds: float = 120,
    ) -> bool:
        result = await session.rpc.fleet.start(
            FleetStartRequest(prompt=prompt),
            timeout=timeout_seconds,
        )
        return result.started

    async def get_session_auth(self, session: CopilotSession) -> dict[str, Any]:
        result = await session.rpc.git_hub_auth.get_status(timeout=10)
        return cast(dict[str, Any], result.to_dict())

    async def enable_remote(
        self,
        session: CopilotSession,
        mode: Literal["on", "export"],
    ) -> dict[str, Any]:
        result = await session.rpc.remote.enable(
            RemoteEnableRequest(mode=RemoteSessionMode(mode)),
            timeout=30,
        )
        return cast(dict[str, Any], result.to_dict())

    async def disable_remote(self, session: CopilotSession) -> None:
        await session.rpc.remote.disable(timeout=30)

    async def tail_event_log(self, session: CopilotSession) -> str:
        return (await session.rpc.event_log.tail(timeout=10)).cursor

    async def read_event_log(
        self,
        session: CopilotSession,
        *,
        cursor: str | None,
        max_events: int = 500,
        wait_ms: int = 0,
        include_ephemeral: bool = False,
    ) -> EventLogBatch:
        if include_ephemeral:
            raise ValueError("durable recovery never requests ephemeral event replay")
        result = await session.rpc.event_log.read(
            EventLogReadRequest(
                cursor=cursor,
                max=max_events,
                wait_ms=wait_ms,
            ),
            timeout=max(10, wait_ms / 1000 + 5),
        )
        durable = tuple(event for event in result.events if event.ephemeral is not True)
        return EventLogBatch(
            cursor=result.cursor,
            cursor_status=result.cursor_status.value,
            events=durable,
            has_more=result.has_more,
            filtered_ephemeral=len(result.events) - len(durable),
        )

    async def check_session_in_use(self, session_id: str) -> bool:
        result = await self.client.rpc.sessions.check_in_use(
            SessionsCheckInUseRequest(session_ids=[session_id]),
            timeout=10,
        )
        return session_id in result.in_use

    async def transport_ping(self) -> dict[str, object]:
        result = await self.client.ping("copilotd-stall-monitor")
        return {
            "status": "ok",
            "protocol_version": result.protocol_version,
        }

    async def runtime_identity(self) -> dict[str, Any]:
        status = await self.client.get_status()
        ping = await self.client.ping("copilotd")
        auth = await self.client.get_auth_status()
        return {
            "runtime_version": status.version,
            "protocol_version": status.protocol_version,
            "ping_protocol_version": ping.protocol_version,
            "authenticated": auth.isAuthenticated,
            "auth_type": auth.authType,
            "auth_host": auth.host,
        }


def _session_config_kwargs(session_config: dict[str, Any] | None) -> dict[str, Any]:
    options = session_config or {}
    return {
        key: options[key]
        for key in (
            "mcp_servers",
            "custom_agents",
            "enable_skills",
            "skill_directories",
            "plugin_directories",
        )
        if key in options and options[key] is not None
    }


def _safe_agent_info(payload: dict[str, Any]) -> dict[str, Any]:
    safe_fields = (
        "description",
        "displayName",
        "id",
        "model",
        "name",
        "skills",
        "source",
        "tools",
        "userInvocable",
    )
    return {field: payload[field] for field in safe_fields if field in payload}
