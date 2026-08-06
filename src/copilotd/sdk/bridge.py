from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from copilot import CopilotClient, RuntimeConnection
from copilot.generated.rpc import (
    EventLogReadRequest,
    HandlePendingToolCallRequest,
    MCPHeadersHandlePendingHeadersRefreshRequest,
    MCPHeadersHandlePendingHeadersRefreshRequestKind,
    MCPHeadersHandlePendingHeadersRefreshRequestRequest,
    MetadataContextInfoRequest,
    ModeSetRequest,
    PermissionDecisionApproveOnce,
    PermissionDecisionUserNotAvailable,
    PermissionsAllowAllMode,
    PermissionsSetAAllSource,
    PermissionsSetAllowAllRequest,
    PermissionsSetApproveAllRequest,
    SessionMode,
    SessionsCheckInUseRequest,
    UIHandlePendingSamplingRequest,
    UIHandlePendingSessionLimitsExhaustedRequest,
    UISessionLimitsExhaustedResponse,
    UISessionLimitsExhaustedResponseAction,
)
from copilot.session import CopilotSession
from copilot.session_events import SessionEvent

from copilotd.config import Settings


class PermissionPostureError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PermissionPosture:
    enabled: bool
    mode: str | None
    approve_all_confirmed: bool


@dataclass(frozen=True, slots=True)
class EventLogBatch:
    cursor: str
    cursor_status: str
    events: tuple[SessionEvent, ...]
    has_more: bool
    filtered_ephemeral: int


PermissionAuditCallback = Callable[[dict[str, Any]], Awaitable[None]]


class ManagedAwarePermissionHandler:
    """Approve ordinary yolo requests once and deterministically block managed ones."""

    def __init__(self, audit: PermissionAuditCallback | None = None) -> None:
        self._audit = audit
        self._managed_permissions_blocked = False
        self._managed_lock = threading.Lock()

    def set_managed_permissions_blocked(self, blocked: bool) -> None:
        with self._managed_lock:
            self._managed_permissions_blocked = blocked

    @property
    def managed_permissions_blocked(self) -> bool:
        with self._managed_lock:
            return self._managed_permissions_blocked

    async def __call__(
        self,
        request: Any,
        invocation: Mapping[str, Any],
    ) -> Any:
        request_payload = (
            request.to_dict()
            if hasattr(request, "to_dict")
            else {"kind": getattr(request, "kind", "unknown")}
        )
        with self._managed_lock:
            runtime_managed_block = self._managed_permissions_blocked
        managed_settings = bool(invocation.get("managed_settings_enabled")) or runtime_managed_block
        managed_request = getattr(request, "managed_approval_required", False) is True
        if managed_settings or managed_request:
            decision = PermissionDecisionUserNotAvailable()
            decision_name = "user-not-available"
        else:
            decision = PermissionDecisionApproveOnce()
            decision_name = "approve-once"
        if self._audit is not None:
            encoded = json.dumps(
                request_payload,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            await self._audit(
                {
                    "request_id": invocation.get("request_id"),
                    "permission_kind": str(
                        request_payload.get("kind") or getattr(request, "kind", "unknown")
                    ),
                    "managed_settings": managed_settings,
                    "managed_approval_required": managed_request,
                    "decision": decision_name,
                    "request_hash": hashlib.sha256(encoded.encode()).hexdigest(),
                }
            )
        return decision


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
        github_token = self._settings.github_token
        client = CopilotClient(
            connection=connection,
            log_level=cast(
                Literal["none", "error", "warning", "info", "debug", "all"],
                self._settings.sdk_log_level,
            ),
            github_token=(None if github_token is None else github_token.get_secret_value()),
            use_logged_in_user=not self._settings.sdk_no_auto_login,
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
        on_elicitation_request: Callable[..., Any] | None = None,
        on_mcp_auth_request: Callable[..., Any] | None = None,
        permission_handler: Callable[..., Any] | None = None,
        hooks: Mapping[str, Any] | None = None,
        session_options: Mapping[str, Any] | None = None,
    ) -> CopilotSession:
        options = _validated_session_options(session_options)
        managed = self._managed_session_options()
        return await self.client.create_session(
            **options,
            **managed,
            session_id=session_id,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            on_event=on_event,
            on_permission_request=permission_handler or ManagedAwarePermissionHandler(),
            on_user_input_request=on_user_input_request,
            on_exit_plan_mode_request=on_exit_plan_mode_request,
            on_auto_mode_switch_request=on_auto_mode_switch_request,
            on_elicitation_request=on_elicitation_request,
            on_mcp_auth_request=on_mcp_auth_request,
            hooks=None if hooks is None else dict(hooks),
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
        on_elicitation_request: Callable[..., Any] | None = None,
        on_mcp_auth_request: Callable[..., Any] | None = None,
        permission_handler: Callable[..., Any] | None = None,
        hooks: Mapping[str, Any] | None = None,
        session_options: Mapping[str, Any] | None = None,
    ) -> CopilotSession:
        options = _validated_session_options(session_options)
        managed = self._managed_session_options()
        return await self.client.resume_session(
            session_id,
            **options,
            **managed,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            continue_pending_work=continue_pending_work,
            on_event=on_event,
            on_permission_request=permission_handler or ManagedAwarePermissionHandler(),
            on_user_input_request=on_user_input_request,
            on_exit_plan_mode_request=on_exit_plan_mode_request,
            on_auto_mode_switch_request=on_auto_mode_switch_request,
            on_elicitation_request=on_elicitation_request,
            on_mcp_auth_request=on_mcp_auth_request,
            hooks=None if hooks is None else dict(hooks),
        )

    async def ensure_allow_all(self, session: CopilotSession) -> PermissionPosture:
        state = await session.rpc.permissions.get_allow_all(timeout=10)
        if not state.enabled or state.mode != PermissionsAllowAllMode.ON:
            changed = await session.rpc.permissions.set_allow_all(
                PermissionsSetAllowAllRequest(
                    enabled=True,
                    mode=PermissionsAllowAllMode.ON,
                    source=PermissionsSetAAllSource.RPC,
                ),
                timeout=10,
            )
            if getattr(changed, "success", True) is not True:
                raise PermissionPostureError("allow-all mutation was not accepted")
        approve_all = await session.rpc.permissions.set_approve_all(
            PermissionsSetApproveAllRequest(
                enabled=True,
                source=PermissionsSetAAllSource.RPC,
            ),
            timeout=10,
        )
        if getattr(approve_all, "success", False) is not True:
            raise PermissionPostureError("approve-all mutation was not accepted")
        state = await session.rpc.permissions.get_allow_all(timeout=10)

        posture = PermissionPosture(
            enabled=state.enabled,
            mode=None if state.mode is None else state.mode.value,
            approve_all_confirmed=True,
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
        reasoning_summary: str | None,
        context_tier: str | None,
    ) -> None:
        await session.set_model(
            model,
            reasoning_effort=reasoning_effort,
            reasoning_summary=cast(Any, reasoning_summary),
            context_tier=cast(Any, context_tier),
        )

    async def respond_session_limits(
        self,
        session: CopilotSession,
        request_id: str,
    ) -> bool:
        result = await session.rpc.ui.handle_pending_session_limits_exhausted(
            UIHandlePendingSessionLimitsExhaustedRequest(
                request_id=request_id,
                response=UISessionLimitsExhaustedResponse(
                    action=UISessionLimitsExhaustedResponseAction.CANCEL,
                ),
            ),
            timeout=10,
        )
        return bool(result.success)

    async def respond_sampling(
        self,
        session: CopilotSession,
        request_id: str,
        response: dict[str, Any] | None,
    ) -> bool:
        result = await session.rpc.ui.handle_pending_sampling(
            UIHandlePendingSamplingRequest(
                request_id=request_id,
                response=response,
            ),
            timeout=10,
        )
        return bool(result.success)

    async def respond_mcp_headers(
        self,
        session: CopilotSession,
        request_id: str,
        headers: dict[str, str] | None,
    ) -> bool:
        result = await session.rpc.mcp.headers.handle_pending_headers_refresh_request(
            MCPHeadersHandlePendingHeadersRefreshRequestRequest(
                request_id=request_id,
                result=MCPHeadersHandlePendingHeadersRefreshRequest(
                    kind=(
                        MCPHeadersHandlePendingHeadersRefreshRequestKind.HEADERS
                        if headers
                        else MCPHeadersHandlePendingHeadersRefreshRequestKind.NONE
                    ),
                    headers=headers or None,
                ),
            ),
            timeout=10,
        )
        return bool(result.success)

    async def respond_external_tool(
        self,
        session: CopilotSession,
        request_id: str,
        *,
        result: str | None = None,
        error: str | None = None,
    ) -> bool:
        response = await session.rpc.tools.handle_pending_tool_call(
            HandlePendingToolCallRequest(
                request_id=request_id,
                result=result,
                error=error,
            ),
            timeout=10,
        )
        return bool(response.success)

    async def get_mcp_servers(self, session: CopilotSession) -> dict[str, Any]:
        return cast(dict[str, Any], (await session.rpc.mcp.list(timeout=10)).to_dict())

    async def get_skills(self, session: CopilotSession) -> dict[str, Any]:
        return cast(dict[str, Any], (await session.rpc.skills.list(timeout=10)).to_dict())

    async def get_agents(self, session: CopilotSession) -> dict[str, Any]:
        return cast(dict[str, Any], (await session.rpc.agent.list(timeout=10)).to_dict())

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

    def _managed_session_options(self) -> dict[str, Any]:
        token = self._settings.github_token
        if token is None:
            return {
                "enable_managed_settings": False,
                "github_token": None,
            }
        return {
            "enable_managed_settings": True,
            "github_token": token.get_secret_value(),
        }


_FORCED_SESSION_OPTIONS = {
    "session_id",
    "working_directory",
    "streaming",
    "include_sub_agent_streaming_events",
    "manage_schedule_enabled",
    "on_event",
    "on_permission_request",
    "on_user_input_request",
    "on_exit_plan_mode_request",
    "on_auto_mode_switch_request",
    "on_elicitation_request",
    "on_mcp_auth_request",
    "hooks",
    "enable_managed_settings",
    "github_token",
    "continue_pending_work",
}


def _validated_session_options(
    value: Mapping[str, Any] | None,
) -> dict[str, Any]:
    options = {} if value is None else dict(value)
    forbidden = sorted(set(options).intersection(_FORCED_SESSION_OPTIONS))
    if forbidden:
        raise ValueError(
            "session options cannot override runtime-owned fields: " + ", ".join(forbidden)
        )
    return options
