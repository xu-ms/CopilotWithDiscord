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
    AgentSelectRequest,
    CommandsInvokeRequest,
    CommandsListRequest,
    EventLogReadRequest,
    FleetStartRequest,
    HistoryCompactRequest,
    MCPHeadersHandlePendingHeadersRefreshRequest,
    MCPHeadersHandlePendingHeadersRefreshRequestKind,
    MCPHeadersHandlePendingHeadersRefreshRequestRequest,
    MCPListToolsRequest,
    MetadataContextInfoRequest,
    ModeSetRequest,
    PermissionDecisionApproveOnce,
    PermissionDecisionUserNotAvailable,
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
    TasksStartAgentRequest,
    UIEphemeralQueryRequest,
    UIHandlePendingSamplingRequest,
    UIHandlePendingSessionLimitsExhaustedRequest,
    UISessionLimitsExhaustedResponse,
    UISessionLimitsExhaustedResponseAction,
)
from copilot.session import CopilotSession
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
    approve_all_confirmed: bool


@dataclass(frozen=True, slots=True)
class EventLogBatch:
    cursor: str
    cursor_status: str
    events: tuple[SessionEvent, ...]
    has_more: bool
    filtered_ephemeral: int


PermissionAuditCallback = Callable[[dict[str, Any]], Awaitable[None]]
ApprovalValidator = Callable[[], Awaitable[bool]]

BRIDGE_ACCEPTANCE_LANES: dict[str, tuple[str, ...]] = {
    "start": ("broad", "native", "extensions", "scheduler-worktree"),
    "stop": ("broad", "native", "extensions", "scheduler-worktree"),
    "force_stop": ("sidecar",),
    "create_session": ("broad", "native", "extensions"),
    "resume_session": ("broad", "scheduler-worktree"),
    "delete_session": ("broad", "native", "extensions", "scheduler-worktree"),
    "session_exists": ("broad", "native", "extensions", "scheduler-worktree"),
    "list_sessions": ("broad", "extensions", "sidecar"),
    "send": ("broad", "native", "extensions", "scheduler-worktree"),
    "abort": ("broad", "native"),
    "disconnect": ("broad", "native", "extensions", "scheduler-worktree"),
    "get_events": ("broad", "native", "sidecar"),
    "ensure_allow_all": ("broad", "native", "extensions", "scheduler-worktree"),
    "set_allow_all": ("extensions",),
    "set_approve_all": ("extensions",),
    "get_mode": ("broad",),
    "set_mode": ("broad",),
    "list_models": ("broad", "native", "extensions", "scheduler-worktree"),
    "healthcheck": ("broad",),
    "set_model": ("native",),
    "respond_session_limits": ("extensions",),
    "respond_sampling": ("extensions",),
    "respond_mcp_headers": ("extensions",),
    "get_mcp_servers": ("broad", "extensions"),
    "list_mcp_tools": ("extensions",),
    "get_plugins": ("extensions",),
    "get_skills": ("broad", "extensions"),
    "get_agents": ("broad", "extensions"),
    "get_current_model": ("broad", "native"),
    "get_context": ("broad",),
    "read_plan": ("broad",),
    "get_usage": ("broad",),
    "get_readiness": ("broad", "scheduler-worktree"),
    "clear_native_queue": ("broad",),
    "get_tasks": ("broad",),
    "refresh_tasks": ("broad", "native"),
    "list_tasks": ("broad", "native"),
    "get_task_progress": ("native",),
    "send_task_message": ("native",),
    "get_current_promotable_task": ("native",),
    "promote_task": ("native",),
    "cancel_task": ("native",),
    "remove_task": ("native",),
    "wait_for_tasks": ("native",),
    "start_agent_task": ("native",),
    "get_native_schedules": ("broad", "native", "scheduler-worktree", "sidecar"),
    "stop_native_schedule": ("broad", "native"),
    "get_remote_state": ("broad", "native", "scheduler-worktree"),
    "get_current_agent": ("broad", "native"),
    "list_agents": ("native",),
    "get_current_agent_info": ("broad", "native"),
    "select_agent": ("native",),
    "deselect_agent": ("native",),
    "list_commands": ("broad", "native"),
    "invoke_command": ("broad", "native", "sidecar"),
    "ephemeral_query": ("native",),
    "compact_history": ("native",),
    "start_fleet": ("native",),
    "get_session_auth": ("broad", "native"),
    "enable_remote": ("native",),
    "disable_remote": ("native", "scheduler-worktree"),
    "tail_event_log": ("broad",),
    "read_event_log": ("broad", "native"),
    "check_session_in_use": ("broad", "scheduler-worktree"),
    "transport_ping": ("broad",),
    "runtime_identity": ("broad", "native", "extensions", "scheduler-worktree"),
    "managed_settings_available": ("deterministic",),
}


class ManagedAwarePermissionHandler:
    """Approve ordinary yolo requests once and deterministically block managed ones."""

    def __init__(
        self,
        audit: PermissionAuditCallback | None = None,
        approval_validator: ApprovalValidator | None = None,
    ) -> None:
        self._audit = audit
        self._approval_validator = approval_validator
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
        decision_name = (
            "user-not-available" if managed_settings or managed_request else "approve-once"
        )
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
        if managed_settings or managed_request:
            return PermissionDecisionUserNotAvailable()
        validator = self._approval_validator
        owner_valid = validator is not None and await validator()
        with self._managed_lock:
            runtime_managed_block = self._managed_permissions_blocked
        if runtime_managed_block or not owner_valid:
            if self._audit is not None:
                await self._audit(
                    {
                        "request_id": invocation.get("request_id"),
                        "permission_kind": str(
                            request_payload.get("kind") or getattr(request, "kind", "unknown")
                        ),
                        "managed_settings": runtime_managed_block,
                        "managed_approval_required": managed_request,
                        "decision": "user-not-available-after-fence-check",
                        "request_hash": hashlib.sha256(encoded.encode()).hexdigest(),
                    }
                )
            return PermissionDecisionUserNotAvailable()
        return PermissionDecisionApproveOnce()


class CopilotBridge:
    """Small typed facade over the official SDK and its generated session RPC."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: CopilotClient | None = None

    def _require_client(self) -> CopilotClient:
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
        auth_options: dict[str, Any] = {}
        if github_token is not None:
            auth_options["github_token"] = github_token.get_secret_value()
        client = CopilotClient(
            connection=connection,
            log_level=cast(
                Literal["none", "error", "warning", "info", "debug", "all"],
                self._settings.sdk_log_level,
            ),
            use_logged_in_user=(github_token is None or not self._settings.sdk_no_auto_login),
            session_idle_timeout_seconds=0,
            enable_remote_sessions=False,
            **auth_options,
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
        on_elicitation_request: Callable[..., Any] | None = None,
        on_mcp_auth_request: Callable[..., Any] | None = None,
        permission_handler: Callable[..., Any] | None = None,
        hooks: Mapping[str, Any] | None = None,
        session_options: Mapping[str, Any] | None = None,
    ) -> CopilotSession:
        options = _validated_session_options(session_options)
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
        launch_kwargs.update(_session_config_kwargs(session_config))
        options.update(launch_kwargs)
        managed = self._managed_session_options()
        if permission_handler is None:
            raise PermissionPostureError("explicit fence-validating permission handler is required")
        return await self._require_client().create_session(
            **options,
            **managed,
            session_id=session_id,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            remote_session=RemoteSessionMode.OFF,
            on_event=on_event,
            on_permission_request=permission_handler,
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
        session_config: dict[str, Any] | None = None,
        launch_options: SessionLaunchOptions | None = None,
        on_elicitation_request: Callable[..., Any] | None = None,
        on_mcp_auth_request: Callable[..., Any] | None = None,
        permission_handler: Callable[..., Any] | None = None,
        hooks: Mapping[str, Any] | None = None,
        session_options: Mapping[str, Any] | None = None,
    ) -> CopilotSession:
        options = _validated_session_options(session_options)
        launch_kwargs = {} if launch_options is None else launch_options.sdk_kwargs()
        launch_kwargs.update(_session_config_kwargs(session_config))
        options.update(launch_kwargs)
        managed = self._managed_session_options()
        if permission_handler is None:
            raise PermissionPostureError("explicit fence-validating permission handler is required")
        return await self._require_client().resume_session(
            session_id,
            **options,
            **managed,
            working_directory=working_directory,
            streaming=True,
            include_sub_agent_streaming_events=True,
            manage_schedule_enabled=False,
            remote_session=RemoteSessionMode.OFF,
            continue_pending_work=continue_pending_work,
            on_event=on_event,
            on_permission_request=permission_handler,
            on_user_input_request=on_user_input_request,
            on_exit_plan_mode_request=on_exit_plan_mode_request,
            on_auto_mode_switch_request=on_auto_mode_switch_request,
            on_elicitation_request=on_elicitation_request,
            on_mcp_auth_request=on_mcp_auth_request,
            hooks=None if hooks is None else dict(hooks),
        )

    async def delete_session(self, session_id: str) -> None:
        """Permanently delete one persisted SDK session by its stable ID."""
        await self._require_client().delete_session(session_id)

    async def session_exists(self, session_id: str) -> bool:
        """Authoritatively reconcile whether a persisted SDK session still exists."""
        return await self._require_client().get_session_metadata(session_id) is not None

    async def list_sessions(self) -> tuple[str, ...]:
        sessions = await self._require_client().list_sessions()
        return tuple(str(getattr(item, "session_id", getattr(item, "id", ""))) for item in sessions)

    async def send(
        self,
        session: CopilotSession,
        prompt: str,
        *,
        attachments: list[Any] | None = None,
        mode: Literal["enqueue", "immediate"] | None = None,
        agent_mode: Literal["interactive", "plan", "autopilot", "shell"] | None = None,
        request_headers: dict[str, str] | None = None,
        display_prompt: str | None = None,
    ) -> str:
        return await session.send(
            prompt,
            attachments=attachments,
            mode=mode,
            agent_mode=agent_mode,
            request_headers=request_headers,
            display_prompt=display_prompt,
        )

    async def abort(self, session: CopilotSession) -> None:
        await session.abort()

    async def disconnect(self, session: CopilotSession) -> None:
        await session.disconnect()

    async def get_events(self, session: CopilotSession) -> tuple[SessionEvent, ...]:
        return tuple(await session.get_events())

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

    async def set_allow_all(
        self,
        session: CopilotSession,
        *,
        enabled: bool,
        mode: str,
    ) -> None:
        result = await session.rpc.permissions.set_allow_all(
            PermissionsSetAllowAllRequest(
                enabled=enabled,
                mode=PermissionsAllowAllMode(mode),
                source=PermissionsSetAAllSource.RPC,
            ),
            timeout=10,
        )
        if getattr(result, "success", True) is not True:
            raise PermissionPostureError("allow-all mutation was not accepted")

    async def set_approve_all(
        self,
        session: CopilotSession,
        *,
        enabled: bool,
    ) -> None:
        result = await session.rpc.permissions.set_approve_all(
            PermissionsSetApproveAllRequest(
                enabled=enabled,
                source=PermissionsSetAAllSource.RPC,
            ),
            timeout=10,
        )
        if getattr(result, "success", False) is not True:
            raise PermissionPostureError("approve-all mutation was not accepted")

    async def get_mode(self, session: CopilotSession) -> str:
        return (await session.rpc.mode.get(timeout=10)).value

    async def set_mode(self, session: CopilotSession, mode: str) -> None:
        await session.rpc.mode.set(
            ModeSetRequest(mode=SessionMode(mode)),
            timeout=10,
        )

    async def list_models(self) -> list[dict[str, Any]]:
        return [model.to_dict() for model in await self._require_client().list_models()]

    async def healthcheck(self) -> None:
        await self._require_client().ping("copilotd-heartbeat")

    async def set_model(
        self,
        session: CopilotSession,
        *,
        model: str,
        reasoning_effort: str | None = None,
        reasoning_summary: str | None = None,
        context_tier: str | None = None,
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

    async def get_mcp_servers(self, session: CopilotSession) -> dict[str, Any]:
        return cast(dict[str, Any], (await session.rpc.mcp.list(timeout=10)).to_dict())

    async def list_mcp_tools(
        self,
        session: CopilotSession,
        *,
        server_name: str,
    ) -> list[dict[str, Any]]:
        result = await session.rpc.mcp.list_tools(
            MCPListToolsRequest(server_name=server_name),
            timeout=10,
        )
        return [cast(dict[str, Any], tool.to_dict()) for tool in result.tools]

    async def get_plugins(self, session: CopilotSession) -> dict[str, Any]:
        return cast(dict[str, Any], (await session.rpc.plugins.list(timeout=10)).to_dict())

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

    async def read_plan(self, session: CopilotSession) -> dict[str, Any]:
        result = await session.rpc.plan.read(timeout=10)
        return cast(dict[str, Any], result.to_dict())

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

    async def clear_native_queue(self, session: CopilotSession) -> None:
        await session.rpc.queue.clear(timeout=10)

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

    async def start_agent_task(
        self,
        session: CopilotSession,
        *,
        agent_type: str,
        name: str,
        description: str,
        prompt: str,
    ) -> str:
        result = await session.rpc.tasks.start_agent(
            TasksStartAgentRequest(
                agent_type=agent_type,
                name=name,
                description=description,
                prompt=prompt,
            ),
            timeout=10,
        )
        return result.agent_id

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
        timeout_seconds: float = 180,
    ) -> dict[str, Any]:
        result = await session.rpc.history.compact(
            HistoryCompactRequest(custom_instructions=focus),
            timeout=timeout_seconds,
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
        result = await self._require_client().rpc.sessions.check_in_use(
            SessionsCheckInUseRequest(session_ids=[session_id]),
            timeout=10,
        )
        return session_id in result.in_use

    async def transport_ping(self, message: str = "copilotd-stall-monitor") -> dict[str, object]:
        result = await self._require_client().ping(message)
        return {
            "status": "ok",
            "protocol_version": result.protocol_version,
            "message": result.message,
        }

    async def runtime_identity(self) -> dict[str, Any]:
        client = self._require_client()
        status = await client.get_status()
        ping = await client.ping("copilotd")
        auth = await client.get_auth_status()
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
            return {}
        return {
            "enable_managed_settings": True,
            "github_token": token.get_secret_value(),
        }

    def managed_settings_available(self) -> bool:
        return self._settings.github_token is not None


_FORCED_SESSION_OPTIONS = {
    "session_id",
    "working_directory",
    "streaming",
    "include_sub_agent_streaming_events",
    "manage_schedule_enabled",
    "remote_session",
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
