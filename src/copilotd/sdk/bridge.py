from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, cast

from copilot import CopilotClient, RuntimeConnection
from copilot.generated.rpc import (
    EventLogReadRequest,
    MetadataContextInfoRequest,
    ModeSetRequest,
    PermissionsAllowAllMode,
    PermissionsSetAAllSource,
    PermissionsSetAllowAllRequest,
    PermissionsSetApproveAllRequest,
    SessionMode,
    SessionsCheckInUseRequest,
)
from copilot.session import CopilotSession, PermissionHandler
from copilot.session_events import SessionEvent

from copilotd.config import Settings
from copilotd.core.session_config import SessionLaunchOptions


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
                async with asyncio.timeout(
                    self._settings.sdk_shutdown_timeout_seconds
                ):
                    await client.stop()
            except TimeoutError:
                async with asyncio.timeout(
                    self._settings.sdk_shutdown_timeout_seconds
                ):
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
        launch_options: SessionLaunchOptions | None = None,
    ) -> CopilotSession:
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
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
        launch_options: SessionLaunchOptions | None = None,
    ) -> CopilotSession:
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
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
    ) -> None:
        await session.set_model(
            model,
            reasoning_effort=reasoning_effort,
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
        await session.rpc.tasks.refresh(timeout=10)
        tasks = await session.rpc.tasks.list(timeout=10)
        return [cast(dict[str, Any], task.to_dict()) for task in tasks.tasks]

    async def get_native_schedules(self, session: CopilotSession) -> list[dict[str, Any]]:
        schedules = await session.rpc.schedule.list(timeout=10)
        return [cast(dict[str, Any], entry.to_dict()) for entry in schedules.entries]

    async def get_remote_state(self, session: CopilotSession) -> dict[str, Any]:
        snapshot = cast(dict[str, Any], (await session.rpc.metadata.snapshot(timeout=10)).to_dict())
        return {
            "mode": "unknown" if snapshot.get("isRemote") else "off",
            "url": snapshot.get("remoteUrl"),
            "metadata": snapshot,
        }

    async def get_current_agent(self, session: CopilotSession) -> str:
        current = await session.rpc.agent.get_current(timeout=10)
        if current.agent is None:
            return "default"
        payload = cast(dict[str, Any], current.agent.to_dict())
        return str(payload.get("name") or payload.get("displayName") or "default")

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
