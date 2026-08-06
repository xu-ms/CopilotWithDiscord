from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import Any, Literal, Protocol, TypeVar, cast

from copilotd.core.attachments import (
    sdk_send_frame_size,
    sdk_trace_context,
)
from copilotd.core.bindings import (
    TYPED_CLOSED_ATTACHMENT_REASONS,
    AttachmentState,
    BindingConflict,
    BindingIntent,
    PermissionPosture,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.commands import (
    CDCapabilityError,
    CDInputError,
    CDSessionStateError,
    ModelReasoningSummaryAdapter,
    TaskActionAdapter,
)
from copilotd.core.extensions import (
    ConfigReloadClaim,
    ConfigReloadClaimStore,
    ConfigReloadState,
    ExtensionConfigConflict,
    ExtensionConfigRepository,
    ExtensionConfigSnapshot,
    ProjectExtensionConfig,
    extension_scope_key,
)
from copilotd.core.hooks import HookSessionContext, SessionHookAudit
from copilotd.core.inbox import ReducerInbox, SdkEventIngress
from copilotd.core.interactions import (
    InteractionGateway,
    InteractionKind,
    InteractionScope,
)
from copilotd.core.mailbox import (
    CommandMailbox,
    MailboxNotAccepting,
    OperationAmbiguous,
    OperationDeferred,
    OperationRejected,
    OperationState,
    OperationStore,
)
from copilotd.core.native import (
    TERMINAL_TASK_STATES,
    NativeCapabilityError,
    NativeManifestController,
    NativeRemoteMode,
    NativeTaskAction,
    RemotePreflightController,
    TaskDeckAdapter,
    json_payload,
    stable_hash,
)
from copilotd.core.projects import ProjectSnapshot, ProjectSource
from copilotd.core.protocol import ProtocolResponseRepository
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.session_config import SessionLaunchOptions
from copilotd.core.task_registry import TaskRegistry
from copilotd.sdk.bridge import (
    EventLogBatch,
    ManagedAwarePermissionHandler,
    PermissionPostureError,
)
from copilotd.sdk.capabilities import CapabilityManifest
from copilotd.sdk.native import NativeCommandResultKind
from copilotd.storage.database import Database
from copilotd.storage.leases import (
    MUTATION_HEADROOM_SECONDS,
    OWNER_LEASE_RENEW_SECONDS,
    FenceLost,
    OwnerConflict,
    OwnerLease,
    OwnerLeaseStore,
)

AgentMode = Literal["interactive", "plan", "autopilot", "shell"]
DeliveryMode = Literal["enqueue", "immediate"]
AttachmentResolver = Callable[..., Awaitable[list[Any]]]
OAuthAuthorizer = Callable[
    [Mapping[str, Any]],
    Awaitable[Mapping[str, Any]],
]
T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


class SessionHandle(Protocol):
    session_id: str

    async def send(
        self,
        prompt: str,
        *,
        attachments: list[Any] | None = None,
        mode: DeliveryMode | None = None,
        agent_mode: AgentMode | None = None,
    ) -> str: ...

    async def abort(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def get_events(self) -> list[Any]: ...


class RuntimeBridge(Protocol):
    def managed_settings_available(self) -> bool: ...

    async def create_session(
        self,
        *,
        session_id: str,
        working_directory: str,
        on_event: Any,
        on_user_input_request: Any,
        on_exit_plan_mode_request: Any,
        on_auto_mode_switch_request: Any,
        session_config: dict[str, Any] | None = None,
        launch_options: SessionLaunchOptions | None = None,
        on_elicitation_request: Any,
        on_mcp_auth_request: Any,
        permission_handler: Any,
        hooks: Mapping[str, Any],
        session_options: Mapping[str, Any],
    ) -> SessionHandle: ...

    async def resume_session(
        self,
        *,
        session_id: str,
        working_directory: str,
        on_event: Any,
        continue_pending_work: bool,
        on_user_input_request: Any,
        on_exit_plan_mode_request: Any,
        on_auto_mode_switch_request: Any,
        session_config: dict[str, Any] | None = None,
        launch_options: SessionLaunchOptions | None = None,
        on_elicitation_request: Any,
        on_mcp_auth_request: Any,
        permission_handler: Any,
        hooks: Mapping[str, Any],
        session_options: Mapping[str, Any],
    ) -> SessionHandle: ...

    async def delete_session(self, session_id: str) -> None: ...

    async def session_exists(self, session_id: str) -> bool: ...

    async def ensure_allow_all(self, session: SessionHandle) -> Any: ...

    async def get_mode(self, session: SessionHandle) -> str: ...

    async def set_mode(self, session: SessionHandle, mode: str) -> None: ...

    async def list_models(self) -> list[dict[str, Any]]: ...

    async def set_model(
        self,
        session: SessionHandle,
        *,
        model: str,
        reasoning_effort: str | None,
        reasoning_summary: str | None,
        context_tier: str | None,
    ) -> None: ...

    async def get_current_model(self, session: SessionHandle) -> dict[str, Any]: ...

    async def respond_session_limits(
        self,
        session: SessionHandle,
        request_id: str,
    ) -> bool: ...

    async def respond_sampling(
        self,
        session: SessionHandle,
        request_id: str,
        response: dict[str, Any] | None,
    ) -> bool: ...

    async def respond_mcp_headers(
        self,
        session: SessionHandle,
        request_id: str,
        headers: dict[str, str] | None,
    ) -> bool: ...

    async def get_context(self, session: SessionHandle) -> dict[str, Any] | None: ...

    async def get_usage(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_readiness(self, session: SessionHandle) -> dict[str, Any]: ...

    async def clear_native_queue(self, session: SessionHandle) -> None: ...

    async def get_tasks(self, session: SessionHandle) -> list[dict[str, Any]]: ...

    async def check_session_in_use(self, session_id: str) -> bool: ...

    async def get_native_schedules(
        self,
        session: SessionHandle,
    ) -> list[dict[str, Any]]: ...

    async def get_remote_state(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_current_agent(self, session: SessionHandle) -> str: ...

    async def list_agents(self, session: SessionHandle) -> list[dict[str, Any]]: ...

    async def get_current_agent_info(
        self,
        session: SessionHandle,
    ) -> dict[str, Any] | None: ...

    async def select_agent(
        self,
        session: SessionHandle,
        name: str,
    ) -> dict[str, Any]: ...

    async def deselect_agent(self, session: SessionHandle) -> None: ...

    async def list_commands(
        self,
        session: SessionHandle,
        *,
        include_builtins: bool,
    ) -> tuple[Any, ...]: ...

    async def invoke_command(
        self,
        session: SessionHandle,
        *,
        name: str,
        input_text: str | None,
    ) -> Any: ...

    async def ephemeral_query(self, session: SessionHandle, question: str) -> str: ...

    async def compact_history(
        self,
        session: SessionHandle,
        *,
        focus: str | None,
    ) -> dict[str, Any]: ...

    async def start_fleet(
        self,
        session: SessionHandle,
        prompt: str,
        *,
        timeout_seconds: float = 120,
    ) -> bool: ...

    async def refresh_tasks(self, session: SessionHandle) -> None: ...

    async def list_tasks(self, session: SessionHandle) -> list[dict[str, Any]]: ...

    async def get_task_progress(
        self,
        session: SessionHandle,
        task_id: str,
    ) -> dict[str, Any] | None: ...

    async def send_task_message(
        self,
        session: SessionHandle,
        task_id: str,
        message: str,
    ) -> dict[str, Any]: ...

    async def get_current_promotable_task(
        self,
        session: SessionHandle,
    ) -> dict[str, Any] | None: ...

    async def promote_task(self, session: SessionHandle, task_id: str) -> bool: ...

    async def cancel_task(self, session: SessionHandle, task_id: str) -> bool: ...

    async def remove_task(self, session: SessionHandle, task_id: str) -> bool: ...

    async def wait_for_tasks(
        self,
        session: SessionHandle,
        *,
        wait_seconds: float,
    ) -> None: ...

    async def stop_native_schedule(
        self,
        session: SessionHandle,
        schedule_id: int,
    ) -> dict[str, Any] | None: ...

    async def get_session_auth(self, session: SessionHandle) -> dict[str, Any]: ...

    async def enable_remote(
        self,
        session: SessionHandle,
        mode: Literal["on", "export"],
    ) -> dict[str, Any]: ...

    async def disable_remote(self, session: SessionHandle) -> None: ...
    async def get_mcp_servers(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_skills(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_agents(self, session: SessionHandle) -> dict[str, Any]: ...
    async def tail_event_log(self, session: SessionHandle) -> str: ...

    async def read_event_log(
        self,
        session: SessionHandle,
        *,
        cursor: str | None,
        max_events: int = 500,
        wait_ms: int = 0,
        include_ephemeral: bool = False,
    ) -> EventLogBatch: ...


class RuntimeState(StrEnum):
    DETACHED = "detached"
    ATTACHING = "attaching"
    READY = "ready"
    DEGRADED = "degraded"
    RECOVERY_UNKNOWN = "recovery_unknown"
    FENCED = "fenced"
    CLOSING = "closing"
    CLOSED = "closed"
    TERMINAL = "terminal"


class SessionAttachUnknown(RuntimeError):
    pass


class SessionAttachRejected(RuntimeError):
    pass


class SessionOwnerConflict(RuntimeError):
    pass


class SessionNotReady(RuntimeError):
    pass


class SubmissionClaimDeferred(OperationDeferred):
    pass


class ClosedSessionRequiresReactivation(SessionNotReady):
    pass


class DetachBlocked(RuntimeError):
    def __init__(self, blockers: list[str]) -> None:
        self.blockers = blockers
        super().__init__("session is not detach-safe: " + ", ".join(blockers))


class SessionRuntime:
    """Owns one long-lived SDK handle and every app-side session worker."""

    def __init__(
        self,
        *,
        database: Database,
        bridge: RuntimeBridge,
        bindings: SessionBindingRepository,
        owner_leases: OwnerLeaseStore,
        owner_id: str,
        binding: SessionBinding,
        ingress_capacity: int = 4096,
        reducer_batch_size: int = 64,
        owner_renew_seconds: float = OWNER_LEASE_RENEW_SECONDS,
        queue_poll_seconds: float = 1,
        attachment_resolver: AttachmentResolver | None = None,
        interaction_timeout_seconds: float = 24 * 60 * 60,
        sdk_operation_timeout_seconds: float = 30,
        abort_evidence_timeout_seconds: float = 15,
        shutdown_timeout_seconds: float = 5,
        capabilities: CapabilityManifest | None = None,
        task_registry: TaskRegistry | None = None,
        send_frame_max_bytes: int = 7 * 1024 * 1024,
        model_summary_adapter: ModelReasoningSummaryAdapter | None = None,
        task_action_adapter: TaskActionAdapter | None = None,
        extension_configs: ExtensionConfigRepository | None = None,
        oauth_authorizer: OAuthAuthorizer | None = None,
    ) -> None:
        self._database = database
        self._bridge = bridge
        self._bindings = bindings
        self._owner_leases = owner_leases
        self._owner_id = owner_id
        self.binding = binding
        self._ingress_capacity = ingress_capacity
        self._reducer_batch_size = reducer_batch_size
        self._owner_renew_seconds = owner_renew_seconds
        self._queue_poll_seconds = queue_poll_seconds
        self._attachment_resolver = attachment_resolver
        self._interaction_timeout_seconds = interaction_timeout_seconds
        self._sdk_operation_timeout_seconds = sdk_operation_timeout_seconds
        self._abort_evidence_timeout_seconds = abort_evidence_timeout_seconds
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._capabilities = capabilities
        self._send_frame_max_bytes = send_frame_max_bytes
        self._model_summary_adapter = model_summary_adapter
        self._task_action_adapter = task_action_adapter
        self._native_manifest = NativeManifestController(database, capabilities)
        self._taskdeck = TaskDeckAdapter(database)
        self._remote_preflight = RemotePreflightController(bridge)
        self._extension_configs = extension_configs
        self._oauth_authorizer = oauth_authorizer
        self.state = RuntimeState.DETACHED
        self._lease: OwnerLease | None = None
        self._handle: SessionHandle | None = None
        self._inbox: ReducerInbox | None = None
        self._ingress: SdkEventIngress | None = None
        self._reducer: EventReducerWorker | None = None
        self._mailbox: CommandMailbox | None = None
        self._tasks = task_registry or TaskRegistry()
        self._renewal_stop = asyncio.Event()
        self._renewal_task: asyncio.Task[None] | None = None
        self._overflow_task: asyncio.Task[None] | None = None
        self._queue_task: asyncio.Task[None] | None = None
        self._queue_stop = asyncio.Event()
        self._task_reconcile_requested = asyncio.Event()
        self._task_reconcile_stop = asyncio.Event()
        self._task_reconcile_task: asyncio.Task[None] | None = None
        self._snapshot_topics: set[str] = set()
        self._snapshot_query_lock = asyncio.Lock()
        self._native_schedule_lock = asyncio.Lock()
        self._permission_reconcile_requested = asyncio.Event()
        self._permission_reconcile_stop = asyncio.Event()
        self._permission_reconcile_task: asyncio.Task[None] | None = None
        self._permission_reconcile_epoch = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue_dispatch_lock = asyncio.Lock()
        self._volatile_attachments: dict[str, list[Any] | None] = {}
        self._interaction_gateway: InteractionGateway | None = None
        self._hook_audit: SessionHookAudit | None = None
        self._permission_handler: ManagedAwarePermissionHandler | None = None
        self._extension_snapshot: ExtensionConfigSnapshot | None = None
        self._protocol_responses = ProtocolResponseRepository(database)
        self._deferred_protocol_events: list[Any] = []
        self._protocol_tasks: set[asyncio.Task[Any]] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._admission_lock = asyncio.Lock()
        self._active_send_admissions = 0
        self._send_admissions_drained = asyncio.Event()
        self._send_admissions_drained.set()
        self._accepting_sends = False
        self._service_quiesced = False
        self._service_quiesce_violations = 0
        self._service_quiesce_violation_callback: Callable[[str], None] | None = None
        self._service_producers_stopped = False
        self._handle_terminal = False
        self._shutdown_finalize_task: asyncio.Task[None] | None = None

    @property
    def handle(self) -> SessionHandle | None:
        return None if self._handle_terminal else self._handle

    @property
    def inbox(self) -> ReducerInbox | None:
        return self._inbox

    async def attach_create(self) -> None:
        await self._attach_guarded(
            create=True,
            continue_pending_work=False,
        )

    async def attach_resume(
        self,
        *,
        reactivate: bool = False,
        continue_pending_work: bool = False,
    ) -> None:
        current = await self._bindings.by_thread(self.binding.thread_id)
        if current is None:
            raise SessionNotReady("session binding disappeared before attach")
        self.binding = current
        if self.binding.binding_intent == BindingIntent.CLOSED:
            scheduler_attachment = self.binding.attachment_reason == "scheduler_run"
            if not reactivate and not scheduler_attachment:
                raise ClosedSessionRequiresReactivation(
                    "closed session requires an explicit resume"
                )
            if reactivate:
                self.binding = await self._bindings.activate(self.binding)
        await self._attach_guarded(
            create=False,
            continue_pending_work=continue_pending_work,
        )

    async def send(
        self,
        prompt: str,
        *,
        idempotency_key: str,
        attachments: list[Any] | None = None,
        attachment_manifest_id: str | None = None,
        mode: DeliveryMode = "enqueue",
        agent_mode: AgentMode | None = None,
        origin: str = "app_message",
    ) -> str:
        if attachments and attachment_manifest_id is None and mode != "immediate":
            raise SessionNotReady("attachments require a durable manifest")
        submission_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self.binding.sdk_session_id}:submission:{idempotency_key}",
            )
        )
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
        async with self._admission_lock:
            if not self._accepting_sends:
                raise SessionNotReady("session is not accepting new messages")
            snapshot = await self._database.fetchone(
                """
                SELECT q.prompt, q.attachment_manifest_id,
                       q.requested_mode_snapshot,
                       q.requested_model_config_snapshot,
                       q.requested_agent_snapshot,
                       q.requested_session_config_version,
                       q.dispatch_attempt,
                       s.prompt_hash, s.requested_delivery, s.origin,
                       s.attachment_count
                FROM message_queue q
                JOIN submissions s ON s.submission_id = q.id
                WHERE q.id = ? AND q.thread_id = ?
                """,
                (submission_id, self.binding.thread_id),
            )
            if snapshot is None:
                await self._assert_dispatchable()
                effective_agent_mode = agent_mode or self.binding.desired_mode
                if effective_agent_mode not in {
                    "interactive",
                    "plan",
                    "autopilot",
                    "shell",
                }:
                    raise SessionNotReady(f"unsupported message mode: {effective_agent_mode}")
                model_row = await self._database.fetchone(
                    """
                    SELECT desired_model_config, desired_agent,
                           desired_project_config_version
                    FROM session_bindings WHERE thread_id = ?
                    """,
                    (self.binding.thread_id,),
                )
                if model_row is None:
                    raise SessionNotReady("session execution configuration is unavailable")
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.submission.queued",
                        "data": {
                            "submission_id": submission_id,
                            "thread_id": self.binding.thread_id,
                            "origin": origin,
                            "prompt": prompt,
                            "prompt_hash": prompt_hash,
                            "correlation_id": idempotency_key,
                            "attachment_manifest_id": attachment_manifest_id,
                            "attachment_count": len(attachments or []),
                            "requested_mode": effective_agent_mode,
                            "requested_model_config": json.loads(model_row["desired_model_config"]),
                            "requested_agent": model_row["desired_agent"],
                            "requested_session_config_version": model_row[
                                "desired_project_config_version"
                            ],
                            "requested_delivery": mode,
                            "created_at": time.time(),
                        },
                    },
                    internal_event_id=f"submission:{submission_id}:queued",
                )
                snapshot = await self._database.fetchone(
                    """
                    SELECT q.prompt, q.attachment_manifest_id,
                           q.requested_mode_snapshot,
                           q.requested_model_config_snapshot,
                           q.requested_agent_snapshot,
                           q.requested_session_config_version,
                           q.dispatch_attempt,
                           s.prompt_hash, s.requested_delivery, s.origin,
                           s.attachment_count
                    FROM message_queue q
                    JOIN submissions s ON s.submission_id = q.id
                    WHERE q.id = ? AND q.thread_id = ?
                    """,
                    (submission_id, self.binding.thread_id),
                )
            if snapshot is None:
                raise SessionNotReady("submission admission was rejected by lifecycle fencing")
            if (
                str(snapshot["prompt_hash"]) != prompt_hash
                or snapshot["attachment_manifest_id"] != attachment_manifest_id
                or (
                    agent_mode is not None
                    and str(snapshot["requested_mode_snapshot"]) != agent_mode
                )
                or str(snapshot["requested_delivery"]) != mode
                or str(snapshot["origin"]) != origin
                or (
                    attachments is not None
                    and len(attachments) != int(snapshot["attachment_count"])
                )
            ):
                raise ValueError("idempotency key was reused with a different immutable submission")
            self._volatile_attachments.setdefault(submission_id, attachments)
            self._active_send_admissions += 1
            self._send_admissions_drained.clear()

        try:
            if mode == "immediate":
                async with self._queue_dispatch_lock:
                    current = await self._database.fetchone(
                        """
                        SELECT s.state, s.accepted_message_id, q.dispatch_attempt
                        FROM submissions s
                        JOIN message_queue q ON q.id = s.submission_id
                        WHERE s.submission_id = ?
                        """,
                        (submission_id,),
                    )
                    if current is None:
                        raise SessionNotReady("immediate submission disappeared")
                    if current["accepted_message_id"] is not None:
                        return str(current["accepted_message_id"])
                    if current["state"] in {"submitted_unknown", "outcome_unknown"}:
                        raise OperationAmbiguous("immediate submission outcome is already unknown")
                    if current["state"] != "local_queued":
                        raise OperationRejected(f"immediate submission is {current['state']}")
                    dispatch_attempt = int(current["dispatch_attempt"])
                    operation_key = (
                        idempotency_key
                        if dispatch_attempt == 0
                        else f"{idempotency_key}:{dispatch_attempt}"
                    )
                try:
                    return await self._dispatch_submission(
                        submission_id=submission_id,
                        idempotency_key=operation_key,
                        prompt=str(snapshot["prompt"]),
                        prompt_hash=str(snapshot["prompt_hash"]),
                        attachment_manifest_id=snapshot["attachment_manifest_id"],
                        attachments=attachments,
                        mode=mode,
                        agent_mode=cast(
                            AgentMode,
                            str(snapshot["requested_mode_snapshot"]),
                        ),
                        dispatch_attempt=dispatch_attempt,
                        requested_model_config=json.loads(
                            str(snapshot["requested_model_config_snapshot"])
                        ),
                        requested_agent=str(snapshot["requested_agent_snapshot"]),
                        requested_session_config_version=int(
                            snapshot["requested_session_config_version"]
                        ),
                        requested_attachment_count=int(snapshot["attachment_count"]),
                        requested_origin=str(snapshot["origin"]),
                        allow_mode_override=agent_mode is not None,
                    )
                except (MailboxNotAccepting, SessionNotReady):
                    if self._accepting_sends and self.state == RuntimeState.READY:
                        raise
                    deferred = await self._database.fetchone(
                        """
                        SELECT 1
                        FROM submissions s
                        JOIN message_queue q ON q.id = s.submission_id
                        WHERE s.submission_id = ?
                          AND s.state = 'local_queued'
                          AND s.accepted_message_id IS NULL
                          AND s.observed_user_event_id IS NULL
                          AND q.state = 'local_queued'
                          AND q.dispatch_attempt = ?
                        """,
                        (submission_id, dispatch_attempt),
                    )
                    if deferred is not None:
                        return submission_id
                    raise
            dispatched = await self._dispatch_next_queued()
            if dispatched is not None and dispatched[0] == submission_id:
                return dispatched[1]
            accepted = await self._database.fetchone(
                "SELECT accepted_message_id FROM submissions WHERE submission_id = ?",
                (submission_id,),
            )
            if accepted is not None and accepted["accepted_message_id"] is not None:
                return str(accepted["accepted_message_id"])
            return submission_id

        finally:
            async with self._admission_lock:
                self._active_send_admissions -= 1
                if self._active_send_admissions == 0:
                    self._send_admissions_drained.set()

    async def _dispatch_next_queued(self) -> tuple[str, str] | None:
        async with self._queue_dispatch_lock:
            if self.state != RuntimeState.READY or self._service_quiesced:
                return None
            try:
                readiness = await self._refresh_readiness()
            except Exception:
                return None
            if (
                readiness["processing"]
                or readiness["hasActiveWork"]
                or readiness["pendingItems"]
                or readiness["steeringMessages"]
            ):
                return None
            unsettled = await self._database.fetchone(
                """
                SELECT COUNT(*) FROM submissions
                WHERE sdk_session_id = ?
                  AND state IN (
                    'submitting', 'submitted', 'submitted_unknown',
                    'observed_active', 'continuation_expected'
                  )
                """,
                (self.binding.sdk_session_id,),
            )
            if unsettled is not None and int(unsettled[0]) > 0:
                return None
            row = await self._database.fetchone(
                """
                SELECT q.*, s.prompt_hash, s.requested_delivery,
                       s.attachment_count, s.origin
                FROM message_queue AS q
                JOIN submissions AS s ON s.submission_id = q.id
                WHERE q.thread_id = ? AND q.state = 'local_queued'
                ORDER BY q.position LIMIT 1
                """,
                (self.binding.thread_id,),
            )
            if row is None:
                return None
            if row["requested_mode_snapshot"] != self.binding.runtime_mode:
                await self._block_queue_item(row["id"], "blocked_mode_drift")
                return None
            runtime_model = await self._database.fetchone(
                """
                SELECT runtime_model_config, desired_agent, runtime_agent,
                       desired_project_config_version,
                       runtime_project_config_version
                FROM session_bindings WHERE thread_id = ?
                """,
                (self.binding.thread_id,),
            )
            requested_model = json.loads(row["requested_model_config_snapshot"])
            observed_model = (
                None
                if runtime_model is None or runtime_model["runtime_model_config"] is None
                else json.loads(runtime_model["runtime_model_config"])
            )
            if requested_model and not _model_config_matches(
                requested_model,
                observed_model,
            ):
                await self._block_queue_item(row["id"], "blocked_model_drift")
                return None
            config_row = await self._database.fetchone(
                """
                SELECT permission_posture, pending_mode, pending_model_config,
                       desired_agent, pending_agent, runtime_agent,
                       desired_project_config_version, pending_project_config_version,
                       runtime_project_config_version, pending_remote_transition_id,
                       runtime_remote_mode
                FROM session_bindings WHERE thread_id = ?
                """,
                (self.binding.thread_id,),
            )
            if config_row is None:
                await self._block_queue_item(row["id"], "blocked_config_unknown")
                return None
            if (
                config_row["permission_posture"] != "verified_allow_all"
                or config_row["pending_mode"] is not None
                or config_row["pending_model_config"] is not None
                or config_row["pending_agent"] is not None
                or config_row["pending_project_config_version"] is not None
            ):
                await self._block_queue_item(row["id"], "blocked_config_unknown")
                return None
            if config_row["pending_remote_transition_id"] is not None:
                await self._block_queue_item(row["id"], "blocked_remote_transition")
                return None
            remote_evidenced = self._capabilities is not None and self._capabilities.supports(
                "remote"
            )
            if remote_evidenced and config_row["runtime_remote_mode"] == "unknown":
                await self._block_queue_item(row["id"], "blocked_remote_transition")
                return None
            agent_evidenced = self._capabilities is not None and self._capabilities.supports(
                "selected_agent"
            )
            runtime_agent = str(config_row["runtime_agent"])
            observed_agent = (
                str(config_row["desired_agent"])
                if runtime_agent == "unknown" and not agent_evidenced
                else runtime_agent
            )
            if row["requested_agent_snapshot"] != observed_agent:
                await self._block_queue_item(row["id"], "blocked_agent_drift")
                return None
            requested_config_version = int(row["requested_session_config_version"])
            desired_config_version = int(config_row["desired_project_config_version"])
            observed_config_version = (
                desired_config_version
                if config_row["runtime_project_config_version"] is None
                else int(config_row["runtime_project_config_version"])
            )
            if (
                requested_config_version != desired_config_version
                or requested_config_version != observed_config_version
            ):
                await self._block_queue_item(
                    row["id"],
                    "blocked_session_config_drift",
                )
                return None
            attachments = self._volatile_attachments.get(row["id"])
            manifest_id = row["attachment_manifest_id"]
            if manifest_id is not None:
                if self._attachment_resolver is None:
                    await self._block_queue_item(
                        str(row["id"]),
                        "blocked_attachment_unavailable",
                    )
                    raise SessionNotReady(
                        "attachment manifest exists but no integrity resolver is configured"
                    )
            elif attachments:
                await self._block_queue_item(
                    str(row["id"]),
                    "blocked_attachment_manifest_missing",
                )
                raise SessionNotReady("attachments require a durable manifest")
            message_id = await self._dispatch_submission(
                submission_id=str(row["id"]),
                idempotency_key=(f"queue:{row['id']}:{int(row['dispatch_attempt'])}"),
                prompt=str(row["prompt"]),
                prompt_hash=str(row["prompt_hash"]),
                attachment_manifest_id=(None if manifest_id is None else str(manifest_id)),
                attachments=attachments,
                mode=cast(DeliveryMode, str(row["requested_delivery"])),
                agent_mode=cast(AgentMode, str(row["requested_mode_snapshot"])),
                dispatch_attempt=int(row["dispatch_attempt"]),
                requested_model_config=requested_model,
                requested_agent=str(row["requested_agent_snapshot"]),
                requested_session_config_version=int(row["requested_session_config_version"]),
                requested_attachment_count=int(row["attachment_count"]),
                requested_origin=str(row["origin"]),
                allow_mode_override=False,
            )
            return str(row["id"]), message_id

    async def _dispatch_submission(
        self,
        *,
        submission_id: str,
        idempotency_key: str,
        prompt: str,
        prompt_hash: str,
        attachment_manifest_id: str | None,
        attachments: list[Any] | None,
        mode: DeliveryMode,
        agent_mode: AgentMode,
        dispatch_attempt: int,
        requested_model_config: dict[str, Any],
        requested_agent: str,
        requested_session_config_version: int,
        requested_attachment_count: int,
        requested_origin: str,
        allow_mode_override: bool,
    ) -> str:
        inbox = self._require_inbox()
        if attachment_manifest_id is not None:
            if self._attachment_resolver is None:
                raise SessionNotReady(
                    "attachment manifest exists but no integrity resolver is configured"
                )
            attachments = await self._resolve_attachments_for_send(
                attachment_manifest_id,
                prompt=prompt,
                mode=mode,
                agent_mode=agent_mode,
            )
        elif attachments and mode != "immediate":
            raise SessionNotReady("attachments require a durable manifest")
        if len(attachments or []) != requested_attachment_count:
            raise SessionNotReady(
                "durable attachment manifest count does not match submission snapshot"
            )

        async def requeue_fence_deferred() -> None:
            requeued = await self._requeue_pre_send_without_reducer(
                submission_id,
                operation_idempotency_key=f"send:{idempotency_key}",
                dispatch_attempt=dispatch_attempt,
            )
            if not requeued:
                raise OperationAmbiguous(
                    f"submission {submission_id} changed before pre-send requeue"
                )

        async def dispatch() -> str:
            claim = await self._claim_submission(
                submission_id,
                operation_idempotency_key=f"send:{idempotency_key}",
                dispatch_attempt=dispatch_attempt,
            )
            if claim == "deferred":
                raise SubmissionClaimDeferred(
                    f"submission {submission_id} was deferred by restart draining"
                )
            if claim != "claimed":
                raise OperationRejected(f"submission {submission_id} was cancelled before dispatch")
            try:
                await self._assert_claimed_dispatchable(
                    require_quiet=mode != "immediate",
                    requested_mode=agent_mode,
                    requested_model_config=requested_model_config,
                    requested_agent=requested_agent,
                    requested_session_config_version=(requested_session_config_version),
                    requested_origin=requested_origin,
                    enforce_mode_snapshot=not allow_mode_override,
                )
            except Exception as error:
                owner_current = await self._is_current_owner()
                if self.state == RuntimeState.FENCED or not owner_current:
                    self.state = RuntimeState.FENCED
                    if self._mailbox is not None:
                        self._mailbox.freeze()
                    requeued = await self._requeue_pre_send_without_reducer(
                        submission_id,
                        operation_idempotency_key=f"send:{idempotency_key}",
                        dispatch_attempt=dispatch_attempt,
                    )
                    if not requeued:
                        raise
                else:
                    await inbox.commit_internal(
                        {
                            "type": "copilotd.submission.pre_send_deferred",
                            "data": {
                                "submission_id": submission_id,
                                "dispatch_attempt": dispatch_attempt,
                                "error_type": type(error).__name__,
                            },
                        },
                        internal_event_id=(
                            f"submission:{submission_id}:pre-send-deferred:{dispatch_attempt}"
                        ),
                    )
                raise SubmissionClaimDeferred(
                    f"submission {submission_id} lost readiness before SDK send"
                ) from error
            frame_size = await asyncio.to_thread(
                sdk_send_frame_size,
                session_id=self.binding.sdk_session_id,
                prompt=prompt,
                attachments=attachments,
                mode=mode,
                agent_mode=agent_mode,
                trace_context=sdk_trace_context(),
            )
            if frame_size > self._send_frame_max_bytes:
                raise OperationRejected(
                    "complete serialized session.send frame exceeds runtime limit"
                )
            message_id = await self._sdk_call(
                self._require_handle().send(
                    prompt,
                    attachments=attachments,
                    mode=mode,
                    agent_mode=agent_mode,
                )
            )
            await inbox.commit_internal(
                {
                    "type": "copilotd.submission.accepted",
                    "data": {
                        "submission_id": submission_id,
                        "message_id": message_id,
                    },
                },
                internal_event_id=f"submission:{submission_id}:accepted",
            )
            self._volatile_attachments.pop(submission_id, None)
            return str(message_id)

        try:
            message_id = await self._require_mailbox().submit(
                kind="send",
                idempotency_key=f"send:{idempotency_key}",
                input_payload={
                    "prompt_hash": prompt_hash,
                    "attachment_manifest_id": attachment_manifest_id,
                    "mode": mode,
                    "agent_mode": agent_mode,
                },
                operation=dispatch,
                defer_on_fence_loss=True,
                on_fence_deferred=requeue_fence_deferred,
            )
        except SubmissionClaimDeferred:
            raise
        except OperationDeferred as error:
            requeued = await self._requeue_pre_send_without_reducer(
                submission_id,
                operation_idempotency_key=f"send:{idempotency_key}",
                dispatch_attempt=dispatch_attempt,
            )
            if not requeued:
                raise OperationAmbiguous(
                    f"submission {submission_id} could not be requeued after "
                    "a deterministic pre-send fence failure"
                ) from error
            raise SubmissionClaimDeferred(
                f"submission {submission_id} was requeued before SDK send"
            ) from error
        except OperationRejected:
            self._volatile_attachments.pop(submission_id, None)
            await inbox.commit_internal(
                {
                    "type": "copilotd.submission.rejected",
                    "data": {"submission_id": submission_id},
                },
                internal_event_id=f"submission:{submission_id}:rejected",
            )
            raise
        except OperationAmbiguous:
            await inbox.commit_internal(
                {
                    "type": "copilotd.submission.acceptance_unknown",
                    "data": {"submission_id": submission_id},
                },
                internal_event_id=f"submission:{submission_id}:acceptance-unknown",
            )
            raise

        await inbox.commit_internal(
            {
                "type": "copilotd.submission.accepted",
                "data": {
                    "submission_id": submission_id,
                    "message_id": message_id,
                },
            },
            internal_event_id=f"submission:{submission_id}:accepted",
        )
        self._volatile_attachments.pop(submission_id, None)
        return str(message_id)

    async def _resolve_attachments_for_send(
        self,
        manifest_id: str,
        *,
        prompt: str,
        mode: DeliveryMode,
        agent_mode: AgentMode,
    ) -> list[Any]:
        resolver = self._attachment_resolver
        if resolver is None:
            raise SessionNotReady(
                "attachment manifest exists but no integrity resolver is configured"
            )
        parameters = inspect.signature(resolver).parameters.values()
        supports_context = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters
        )
        if not supports_context:
            names = {parameter.name for parameter in parameters}
            supports_context = {"session_id", "prompt", "mode", "agent_mode"} <= names
        if not supports_context:
            return await resolver(manifest_id)
        return await resolver(
            manifest_id,
            session_id=self.binding.sdk_session_id,
            prompt=prompt,
            mode=mode,
            agent_mode=agent_mode,
        )

    async def _claim_submission(
        self,
        submission_id: str,
        *,
        operation_idempotency_key: str,
        dispatch_attempt: int,
    ) -> Literal["claimed", "deferred", "cancelled"]:
        operation = await self._database.fetchone(
            """
            SELECT operation_id FROM session_operations
            WHERE sdk_session_id = ? AND idempotency_key = ?
            """,
            (self.binding.sdk_session_id, operation_idempotency_key),
        )
        if operation is None:
            raise RuntimeError("submission claim has no durable operation intent")
        operation_id = str(operation["operation_id"])
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.submission.submitting",
                "data": {
                    "submission_id": submission_id,
                    "operation_id": operation_id,
                    "dispatch_attempt": dispatch_attempt,
                },
            },
            internal_event_id=f"submission:{submission_id}:submitting:{operation_id}",
        )
        claimed = await self._database.fetchone(
            """
            SELECT s.state AS submission_state, s.source_operation_id,
                   q.state AS queue_state, q.dispatch_attempt
            FROM submissions AS s
            JOIN message_queue AS q ON q.id = s.submission_id
            WHERE s.submission_id = ? AND s.sdk_session_id = ?
            """,
            (submission_id, self.binding.sdk_session_id),
        )
        if (
            claimed is not None
            and claimed["submission_state"] == "submitting"
            and claimed["queue_state"] == "submitting"
            and claimed["source_operation_id"] == operation_id
        ):
            return "claimed"
        if (
            claimed is not None
            and claimed["submission_state"] == "local_queued"
            and claimed["queue_state"] == "local_queued"
            and int(claimed["dispatch_attempt"]) > dispatch_attempt
        ):
            return "deferred"
        return "cancelled"

    async def _block_queue_item(self, submission_id: str, state: str) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.queue.blocked",
                "data": {"submission_id": submission_id, "state": state},
            },
            internal_event_id=f"queue:{submission_id}:{state}",
        )

    async def _requeue_pre_send_without_reducer(
        self,
        submission_id: str,
        *,
        operation_idempotency_key: str,
        dispatch_attempt: int,
    ) -> bool:
        now = time.time()
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT s.state AS submission_state, s.source_operation_id,
                       s.accepted_message_id, s.observed_user_event_id,
                       q.state AS queue_state, q.dispatch_attempt,
                       o.operation_id
                FROM submissions s
                JOIN message_queue q ON q.id = s.submission_id
                JOIN session_operations o
                  ON o.sdk_session_id = s.sdk_session_id
                 AND o.idempotency_key = ?
                WHERE s.submission_id = ? AND s.sdk_session_id = ?
                """,
                (
                    operation_idempotency_key,
                    submission_id,
                    self.binding.sdk_session_id,
                ),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                return False
            observed_attempt = int(row["dispatch_attempt"])
            if (
                observed_attempt > dispatch_attempt
                and row["submission_state"] == "local_queued"
                and row["queue_state"] == "local_queued"
            ):
                return True
            if (
                observed_attempt != dispatch_attempt
                or row["accepted_message_id"] is not None
                or row["observed_user_event_id"] is not None
            ):
                return False
            operation_id = str(row["operation_id"])
            states = (str(row["submission_state"]), str(row["queue_state"]))
            if states == ("local_queued", "local_queued"):
                source_matches = row["source_operation_id"] is None
            else:
                source_matches = (
                    states
                    in {
                        ("submitting", "submitting"),
                        ("submitted_unknown", "submitted_unknown"),
                    }
                    and row["source_operation_id"] == operation_id
                )
            if not source_matches:
                return False
            updated_submission = await connection.execute(
                """
                UPDATE submissions
                SET state = 'local_queued', source_operation_id = NULL,
                    send_started_at = NULL, terminal_at = NULL
                WHERE submission_id = ?
                  AND state = ?
                  AND accepted_message_id IS NULL
                  AND observed_user_event_id IS NULL
                """,
                (submission_id, row["submission_state"]),
            )
            requeued = updated_submission.rowcount == 1
            await updated_submission.close()
            if not requeued:
                return False
            updated_queue = await connection.execute(
                """
                UPDATE message_queue
                SET state = 'local_queued',
                    dispatch_attempt = dispatch_attempt + 1,
                    updated_at = ?
                WHERE id = ? AND state = ? AND dispatch_attempt = ?
                """,
                (now, submission_id, row["queue_state"], dispatch_attempt),
            )
            queue_requeued = updated_queue.rowcount == 1
            await updated_queue.close()
            if not queue_requeued:
                raise RuntimeError(
                    f"submission {submission_id} queue changed during pre-send requeue"
                )
            await connection.execute(
                """
                UPDATE schedule_runs
                SET status = 'submitting', send_started_at = NULL,
                    last_progress_at = ?, updated_at = ?
                WHERE result_submission_id = ?
                  AND status = 'submitting'
                  AND accepted_message_id IS NULL
                """,
                (now, now, submission_id),
            )
            return True

    async def _refresh_readiness(
        self,
        *,
        allow_attaching: bool = False,
        only_if_pending: bool = False,
    ) -> dict[str, Any] | None:
        await self._assert_owned_handle(allow_attaching=allow_attaching)
        async with self._snapshot_query_lock:
            if only_if_pending:
                rows = await self._database.fetchall(
                    """
                    SELECT requested_epoch, applied_epoch
                    FROM reconciliation_state
                    WHERE sdk_session_id = ? AND topic IN ('activity', 'queue')
                    """,
                    (self.binding.sdk_session_id,),
                )
                if rows and all(
                    int(row["requested_epoch"]) <= int(row["applied_epoch"]) for row in rows
                ):
                    return None
            activity_epoch = await self._request_snapshot("activity")
            queue_epoch = await self._request_snapshot("queue")
            inbox = self._require_inbox()
            query_start = inbox.last_sdk_receive_seq
            try:
                snapshot = await self._bridge.get_readiness(self._require_handle())
            except Exception as error:
                query_end = inbox.last_sdk_receive_seq
                await self._commit_snapshot_failure(
                    "activity",
                    activity_epoch,
                    query_start,
                    query_end,
                    error,
                )
                await self._commit_snapshot_failure(
                    "queue",
                    queue_epoch,
                    query_start,
                    query_end,
                    error,
                )
                raise
            query_end = inbox.last_sdk_receive_seq
            observed = {
                "processing": bool(snapshot.get("processing")),
                "hasActiveWork": bool(snapshot.get("hasActiveWork")),
                "abortable": bool(snapshot.get("abortable")),
                "pendingItems": list(snapshot.get("pendingItems") or []),
                "steeringMessages": list(snapshot.get("steeringMessages") or []),
            }
            await self._commit_snapshot(
                "activity",
                activity_epoch,
                query_start,
                query_end,
                {
                    "processing": observed["processing"],
                    "has_active_work": observed["hasActiveWork"],
                    "abortable": observed["abortable"],
                },
            )
            await self._commit_snapshot(
                "queue",
                queue_epoch,
                query_start,
                query_end,
                {
                    "items": observed["pendingItems"],
                    "steering_messages": observed["steeringMessages"],
                },
            )
            return observed

    async def reload_extension_config(
        self,
        *,
        idempotency_key: str,
        config: ProjectExtensionConfig | None = None,
        expected_project_config_version: int | None = None,
    ) -> ExtensionConfigSnapshot:
        repository = self._extension_configs
        if repository is None:
            raise SessionNotReady("extension configuration repository is unavailable")
        project = ProjectSnapshot(
            project_id=self.binding.project_id,
            channel_id=f"session:{self.binding.thread_id}",
            source=(
                ProjectSource.EXPLICIT
                if self.binding.project_id is not None
                else ProjectSource.IMPLICIT_HOME
            ),
            root_path=self.binding.cwd_snapshot,
            cwd=self.binding.cwd_snapshot,
            config_version=1,
        )
        existing_snapshot = await repository.latest(project) if config is None else None
        normalized_config = (
            existing_snapshot.config
            if existing_snapshot is not None
            else config.normalized(project.cwd)
            if config is not None
            else ProjectExtensionConfig()
        )
        config_hash = normalized_config.digest()
        preflight = ExtensionConfigSnapshot(
            scope_key=extension_scope_key(
                self.binding.project_source,
                self.binding.project_id,
            ),
            version=0,
            project_id=self.binding.project_id,
            project_source=self.binding.project_source,
            cwd_snapshot=self.binding.cwd_snapshot,
            config_hash=config_hash,
            config=normalized_config,
        )
        preflight.sdk_session_options()
        transition_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (f"copilotd:{self.binding.sdk_session_id}:config:{idempotency_key}"),
            )
        )
        lease = self._lease
        if lease is None:
            raise SessionNotReady("config reload requires an active owner lease")
        claim_store = ConfigReloadClaimStore(self._database)
        existing_claim = await claim_store.find(
            sdk_session_id=self.binding.sdk_session_id,
            idempotency_key=idempotency_key,
            config_hash=config_hash,
        )
        if existing_claim is None:
            await self._assert_dispatchable()
            blockers = await self.detach_blockers()
            if blockers:
                raise DetachBlocked(blockers)
        elif self.state != RuntimeState.READY:
            raise SessionNotReady(
                f"config reload recovery requires a ready runtime, found {self.state}"
            )
        claim, snapshot, created = await claim_store.claim_and_publish(
            sdk_session_id=self.binding.sdk_session_id,
            idempotency_key=idempotency_key,
            project=project,
            config=normalized_config,
            owner_id=lease.owner_id,
            runtime_generation=self.binding.runtime_generation,
            owner_fence_token=self._require_fence_token(),
            transition_id=transition_id,
            minimum_headroom_seconds=MUTATION_HEADROOM_SECONDS,
            expected_current_version=expected_project_config_version,
        )
        current = await self._bindings.by_thread(self.binding.thread_id)
        if current is None:
            raise SessionNotReady("session binding disappeared during config publication")
        self.binding = current
        if not created:
            replayed = await self._replay_config_reload_claim(
                claim,
                repository,
                claim_store,
            )
            if replayed is not None:
                return replayed
        if (
            snapshot.version == self.binding.desired_session_config_version
            and snapshot.config_hash == self.binding.desired_session_config_hash
            and snapshot.version == self.binding.runtime_session_config_version
            and snapshot.config_hash == self.binding.runtime_session_config_hash
        ):
            await claim_store.transition(
                claim,
                ConfigReloadState.CONFIRMED,
                config_version=snapshot.version,
            )
            return snapshot
        inbox = self._require_inbox()
        await inbox.commit_internal(
            {
                "type": "copilotd.config.pending",
                "data": {
                    "version": snapshot.version,
                    "config_hash": snapshot.config_hash,
                    "transition_id": transition_id,
                },
            },
            internal_event_id=f"config:{transition_id}:pending",
        )
        current = await self._bindings.by_thread(self.binding.thread_id)
        if current is None:
            raise SessionNotReady("session binding disappeared before config reattach")
        self.binding = current
        async with self._lifecycle_lock:
            await self._assert_owned_handle()
            async with self._admission_lock:
                self._accepting_sends = False
            if self._mailbox is not None:
                self._mailbox.freeze()
            try:
                await self._sdk_call(self._require_handle().disconnect())
            except Exception as error:
                await claim_store.transition(
                    claim,
                    ConfigReloadState.UNKNOWN,
                    config_version=snapshot.version,
                    error_code=type(error).__name__,
                )
                await inbox.commit_internal(
                    {
                        "type": "copilotd.config.unknown",
                        "data": {
                            "transition_id": transition_id,
                            "error_type": type(error).__name__,
                        },
                    },
                    internal_event_id=f"config:{transition_id}:unknown",
                )
                latest = await self._bindings.by_thread(self.binding.thread_id)
                if latest is not None:
                    self.binding = await self._bindings.mark_recovery_unknown(latest)
                self.state = RuntimeState.RECOVERY_UNKNOWN
                await self._stop_components(release_owner=True)
                raise SessionAttachUnknown(
                    "config reattach disconnect outcome is unknown"
                ) from error
            await inbox.join()
            await self._stop_components(release_owner=False)
            self.state = RuntimeState.DETACHED
        try:
            await self._attach(
                create=False,
                continue_pending_work=True,
                reuse_owner=True,
                target_config_version=snapshot.version,
            )
        except BaseException as error:
            await claim_store.transition(
                claim,
                ConfigReloadState.UNKNOWN,
                config_version=snapshot.version,
                error_code=type(error).__name__,
            )
            await self._cleanup_failed_attach(error)
            raise
        current = await self._bindings.by_thread(self.binding.thread_id)
        if current is None:
            raise SessionNotReady("session binding disappeared after config reattach")
        self.binding = current
        if (
            current.runtime_session_config_version != snapshot.version
            or current.runtime_session_config_hash != snapshot.config_hash
            or current.session_config_state != "synced"
        ):
            self.state = RuntimeState.DEGRADED
            await claim_store.transition(
                claim,
                ConfigReloadState.UNKNOWN,
                config_version=snapshot.version,
                error_code="ConfigReadbackMismatch",
            )
            raise SessionAttachUnknown("config reattach could not be durably confirmed")
        await claim_store.transition(
            claim,
            ConfigReloadState.CONFIRMED,
            config_version=snapshot.version,
        )
        return snapshot

    async def _replay_config_reload_claim(
        self,
        claim: ConfigReloadClaim,
        repository: ExtensionConfigRepository,
        claim_store: ConfigReloadClaimStore,
    ) -> ExtensionConfigSnapshot | None:
        if claim.state == ConfigReloadState.CONFIRMED and claim.config_version is not None:
            snapshot = await repository.for_session(
                project_source=self.binding.project_source,
                project_id=self.binding.project_id,
                cwd_snapshot=self.binding.cwd_snapshot,
                version=claim.config_version,
            )
            if snapshot.config_hash != claim.config_hash:
                raise ExtensionConfigConflict(
                    "confirmed config reload claim no longer matches its generation"
                )
            return snapshot
        if claim.state in {
            ConfigReloadState.CLAIMED,
            ConfigReloadState.STARTED,
            ConfigReloadState.UNKNOWN,
        }:
            current = await self._bindings.by_thread(self.binding.thread_id)
            if current is not None:
                config_version = (
                    claim.config_version
                    if claim.config_version is not None
                    else current.desired_session_config_version
                )
                durable_match = (
                    current.session_config_state == "synced"
                    and current.desired_session_config_version == config_version
                    and current.runtime_session_config_version == config_version
                    and current.desired_session_config_hash == claim.config_hash
                    and current.runtime_session_config_hash == claim.config_hash
                )
                if durable_match:
                    snapshot = await repository.for_session(
                        project_source=current.project_source,
                        project_id=current.project_id,
                        cwd_snapshot=current.cwd_snapshot,
                        version=config_version,
                    )
                    if snapshot.config_hash != claim.config_hash:
                        raise ExtensionConfigConflict(
                            "durable config readback does not match reload claim"
                        )
                    await claim_store.transition(
                        claim,
                        ConfigReloadState.CONFIRMED,
                        config_version=config_version,
                    )
                    return snapshot
                can_continue = (
                    claim.state == ConfigReloadState.STARTED
                    and current.attachment_state == AttachmentState.ATTACHED
                    and current.owner_fence_token == self.binding.owner_fence_token
                    and (
                        (
                            current.pending_session_config_version == claim.config_version
                            and current.pending_session_config_hash == claim.config_hash
                            and current.session_config_state == "pending"
                        )
                        or (
                            current.pending_session_config_version is None
                            and current.session_config_state == "synced"
                        )
                    )
                )
                if can_continue:
                    return None
        if claim.state == ConfigReloadState.REJECTED:
            raise OperationRejected(f"config reload was rejected: {claim.error_code or 'unknown'}")
        raise OperationAmbiguous(f"config reload outcome is {claim.state}: {claim.idempotency_key}")

    async def _request_snapshot(self, topic: str) -> int:
        request_id = str(uuid.uuid4())
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.snapshot.requested",
                "data": {"topic": topic},
            },
            source="snapshot",
            internal_event_id=f"snapshot-request:{topic}:{request_id}",
        )
        row = await self._database.fetchone(
            """
            SELECT requested_epoch FROM reconciliation_state
            WHERE sdk_session_id = ? AND topic = ?
            """,
            (self.binding.sdk_session_id, topic),
        )
        if row is None:
            raise RuntimeError(f"snapshot request was not persisted for {topic}")
        return int(row["requested_epoch"])

    async def _commit_snapshot(
        self,
        topic: str,
        epoch: int,
        query_start: int,
        query_end: int,
        payload: dict[str, Any],
    ) -> None:
        snapshot_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"copilotd:{self.binding.sdk_session_id}:snapshot:"
                    f"{self.binding.runtime_generation}:{topic}:{epoch}:"
                    f"{query_start}:{query_end}"
                ),
            )
        )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.snapshot.observed",
                "data": {
                    "topic": topic,
                    "epoch": epoch,
                    "snapshot_id": snapshot_id,
                    "query_start_sdk_receive_seq": query_start,
                    "query_end_sdk_receive_seq": query_end,
                    "payload": payload,
                    "observed_at": time.time(),
                },
            },
            source="snapshot",
            internal_event_id=f"snapshot:{snapshot_id}",
        )

    async def _commit_snapshot_failure(
        self,
        topic: str,
        epoch: int,
        query_start: int,
        query_end: int,
        error: Exception,
    ) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.snapshot.failed",
                "data": {
                    "topic": topic,
                    "epoch": epoch,
                    "query_start_sdk_receive_seq": query_start,
                    "query_end_sdk_receive_seq": query_end,
                    "error_type": type(error).__name__,
                    "observed_at": time.time(),
                },
            },
            source="snapshot",
            internal_event_id=(
                f"snapshot:{self.binding.runtime_generation}:{topic}:{epoch}:failed"
            ),
        )

    async def _queue_pump(self) -> None:
        while not self._queue_stop.is_set():
            pending = await self._database.fetchone(
                """
                SELECT 1 FROM message_queue
                WHERE thread_id = ? AND state = 'local_queued' LIMIT 1
                """,
                (self.binding.thread_id,),
            )
            if pending is not None:
                try:
                    await self._dispatch_next_queued()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await self._require_inbox().commit_internal(
                        {
                            "type": "copilotd.queue.pump_failed",
                            "data": {"error_type": type(error).__name__},
                        },
                        internal_event_id=(
                            f"queue-pump:{self.binding.runtime_generation}:"
                            f"{type(error).__name__}:{time.time_ns()}"
                        ),
                    )
            try:
                await asyncio.wait_for(
                    self._queue_stop.wait(),
                    timeout=self._queue_poll_seconds,
                )
            except TimeoutError:
                pass

    async def _task_reconcile_loop(self) -> None:
        while not self._task_reconcile_stop.is_set():
            await self._task_reconcile_requested.wait()
            if self._task_reconcile_stop.is_set():
                return
            self._task_reconcile_requested.clear()
            if not await self._task_reconcile_owner_is_current():
                return
            await self._require_inbox().join()
            if not await self._task_reconcile_owner_is_current():
                return
            topics = set(self._snapshot_topics)
            self._snapshot_topics.clear()
            force_tasks = not topics
            if not topics:
                topics.add("tasks")
            if "tasks" in topics:
                await self._query_snapshot_topic(
                    "tasks",
                    require_current_owner=True,
                    only_if_pending=not force_tasks,
                )
                if not await self._task_reconcile_owner_is_current():
                    return
                topics.remove("tasks")
            if {"activity", "queue"}.intersection(topics):
                try:
                    await self._refresh_readiness(only_if_pending=True)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if not await self._task_reconcile_owner_is_current():
                        return
                if not await self._task_reconcile_owner_is_current():
                    return
                topics.difference_update({"activity", "queue"})
            for topic in sorted(topics):
                await self._query_snapshot_topic(
                    topic,
                    require_current_owner=True,
                    only_if_pending=True,
                )
                if not await self._task_reconcile_owner_is_current():
                    return

    async def _task_reconcile_owner_is_current(self) -> bool:
        if self.state != RuntimeState.READY:
            return False
        if await self._is_current_owner():
            return True
        self.state = RuntimeState.FENCED
        if self._mailbox is not None:
            self._mailbox.freeze()
        return False

    async def _query_snapshot_topic(
        self,
        topic: str,
        *,
        require_current_owner: bool = False,
        only_if_pending: bool = False,
    ) -> None:
        async with self._snapshot_query_lock:
            if require_current_owner and not await self._task_reconcile_owner_is_current():
                return
            state = await self._database.fetchone(
                """
                SELECT requested_epoch, applied_epoch
                FROM reconciliation_state
                WHERE sdk_session_id = ? AND topic = ?
                """,
                (self.binding.sdk_session_id, topic),
            )
            if (
                only_if_pending
                and state is not None
                and int(state["requested_epoch"]) <= int(state["applied_epoch"])
            ):
                return
            if state is None or int(state["requested_epoch"]) <= int(state["applied_epoch"]):
                epoch = await self._request_snapshot(topic)
            else:
                epoch = int(state["requested_epoch"])
            inbox = self._require_inbox()
            query_start = inbox.last_sdk_receive_seq
            try:
                if topic == "tasks":
                    payload = {"tasks": await self._bridge.get_tasks(self._require_handle())}
                elif topic == "commands":
                    commands = self._native_manifest.validate(
                        await self._bridge.list_commands(
                            self._require_handle(),
                            include_builtins=True,
                        )
                    )
                    payload = {
                        "commands": [command.to_dict() for command in commands],
                        "manifest_generation": epoch,
                    }
                elif topic == "agents":
                    agents, current = await asyncio.gather(
                        self._bridge.list_agents(self._require_handle()),
                        self._bridge.get_current_agent_info(self._require_handle()),
                    )
                    payload = {
                        "agents": agents,
                        "current": current,
                        "manifest_generation": epoch,
                    }
                elif topic == "schedules":
                    payload = {
                        "schedules": await self._bridge.get_native_schedules(self._require_handle())
                    }
                elif topic == "remote":
                    snapshot = await self._bridge.get_remote_state(self._require_handle())
                    remote = await self._database.fetchone(
                        """
                        SELECT runtime_remote_mode, remote_url, remote_steerable
                        FROM session_bindings WHERE sdk_session_id = ?
                        """,
                        (self.binding.sdk_session_id,),
                    )
                    mode = "unknown" if remote is None else str(remote["runtime_remote_mode"])
                    payload = {
                        "mode": mode,
                        "url": None if remote is None else remote["remote_url"],
                        "steerable": (
                            False
                            if mode == "off"
                            else None
                            if remote is None or remote["remote_steerable"] is None
                            else bool(remote["remote_steerable"])
                        ),
                        "metadata": snapshot,
                    }
                elif topic == "extensions":
                    skills, agents = await asyncio.gather(
                        self._bridge.get_skills(self._require_handle()),
                        self._bridge.get_agents(self._require_handle()),
                    )
                    payload = {"skills": skills, "agents": agents}
                elif topic == "mcp":
                    payload = await self._bridge.get_mcp_servers(self._require_handle())
                else:
                    raise ValueError(f"unsupported snapshot topic: {topic}")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if require_current_owner and not await self._task_reconcile_owner_is_current():
                    return
                await self._commit_snapshot_failure(
                    topic,
                    epoch,
                    query_start,
                    inbox.last_sdk_receive_seq,
                    error,
                )
                return
            if require_current_owner and not await self._task_reconcile_owner_is_current():
                return
            await self._commit_snapshot(
                topic,
                epoch,
                query_start,
                inbox.last_sdk_receive_seq,
                payload,
            )
            latest = await self._database.fetchone(
                """
                SELECT requested_epoch, applied_epoch
                FROM reconciliation_state
                WHERE sdk_session_id = ? AND topic = ?
                """,
                (self.binding.sdk_session_id, topic),
            )
            if latest is not None and int(latest["applied_epoch"]) < int(latest["requested_epoch"]):
                self._snapshot_topics.add(topic)
                self._task_reconcile_requested.set()

    def _enqueue_snapshot_request(self, topic: str, trigger_id: str) -> None:
        inbox = self._inbox
        if inbox is None:
            return
        accepted = inbox.submit_internal(
            {
                "type": "copilotd.snapshot.requested",
                "data": {"topic": topic},
            },
            source="snapshot",
            internal_event_id=f"snapshot-request:{topic}:{trigger_id}",
        )
        if accepted:
            self._snapshot_topics.add(topic)
            self._task_reconcile_requested.set()

    def _supported_snapshot_topics(self) -> set[str]:
        topics = {"activity", "queue"}
        if self._capabilities is None:
            topics.add("tasks")
        else:
            if self._capabilities.supports("tasks_list"):
                topics.add("tasks")
            if self._capabilities.supports("commands_list"):
                topics.add("commands")
            if self._capabilities.supports("agents_list") and self._capabilities.supports(
                "agents_current"
            ):
                topics.add("agents")
            if self._capabilities.supports("schedules_list"):
                topics.add("schedules")
            if self._capabilities.supports("remote_status"):
                topics.add("remote")
            if self._capabilities.supports("session_extension_config"):
                topics.update({"extensions", "mcp"})
        return topics

    async def _prime_readiness(self) -> None:
        await self._refresh_readiness(allow_attaching=True)
        pending_topics = self._supported_snapshot_topics() - {"activity", "queue"}
        for _ in range(8):
            for topic in sorted(pending_topics):
                await self._query_snapshot_topic(topic)
            await self._require_inbox().join()
            rows = await self._database.fetchall(
                """
                SELECT topic FROM reconciliation_state
                WHERE sdk_session_id = ?
                  AND topic IN ('agents', 'commands', 'remote',
                                'schedules', 'tasks')
                  AND (
                      requested_epoch != applied_epoch
                      OR status != 'idle'
                  )
                """,
                (self.binding.sdk_session_id,),
            )
            pending_topics = {
                str(row["topic"])
                for row in rows
                if str(row["topic"]) in self._supported_snapshot_topics()
            }
            if not pending_topics:
                break
        blockers = await self._readiness_blockers(require_quiet=False)
        if blockers:
            self.state = RuntimeState.DEGRADED
            raise SessionNotReady("session readiness reconciliation failed: " + ", ".join(blockers))

    async def _readiness_blockers(self, *, require_quiet: bool) -> list[str]:
        blockers: list[str] = []
        inbox = self._require_inbox()
        binding = await self._database.fetchone(
            "SELECT * FROM session_bindings WHERE sdk_session_id = ?",
            (self.binding.sdk_session_id,),
        )
        if binding is None:
            return ["binding_missing"]
        if int(binding["last_inbox_seq"]) < inbox.last_reserved_inbox_seq:
            blockers.append("reducer_not_caught_up")

        expected_topics = self._supported_snapshot_topics()
        rows = await self._database.fetchall(
            """
            SELECT topic, requested_epoch, applied_epoch, status
            FROM reconciliation_state WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        snapshots = {str(row["topic"]): row for row in rows}
        for topic in sorted(expected_topics):
            row = snapshots.get(topic)
            if (
                row is None
                or int(row["requested_epoch"]) < 1
                or int(row["applied_epoch"]) != int(row["requested_epoch"])
                or row["status"] != "idle"
            ):
                blockers.append(f"snapshot_{topic}_stale")

        if self._capabilities is not None:
            if self._capabilities.supports("agents_current"):
                if binding["pending_agent"] is not None:
                    blockers.append("agent_transition_pending")
                if binding["runtime_agent"] == "unknown":
                    blockers.append("runtime_agent_unknown")
                elif binding["runtime_agent"] != binding["desired_agent"]:
                    blockers.append("runtime_agent_drift")
            if binding["pending_project_config_version"] is not None:
                blockers.append("session_config_transition_pending")
            if binding["runtime_project_config_version"] is None:
                blockers.append("runtime_session_config_unknown")
            elif int(binding["runtime_project_config_version"]) != int(
                binding["desired_project_config_version"]
            ):
                blockers.append("runtime_session_config_drift")
            if self._capabilities.supports("remote_status"):
                if binding["pending_remote_transition_id"] is not None:
                    blockers.append("remote_transition_pending")
                if binding["runtime_remote_mode"] == "unknown":
                    blockers.append("runtime_remote_unknown")

            if self._capabilities.supports("tasks_list"):
                unknown_tasks = await self._database.fetchone(
                    """
                    SELECT COUNT(*) FROM background_observations
                    WHERE sdk_session_id = ? AND observed_state = 'unknown'
                    """,
                    (self.binding.sdk_session_id,),
                )
                if unknown_tasks is not None and int(unknown_tasks[0]) > 0:
                    blockers.append(f"background_tasks_unknown:{int(unknown_tasks[0])}")
            if self._capabilities.supports("schedules_list"):
                unknown_schedules = await self._database.fetchone(
                    """
                    SELECT COUNT(*) FROM runtime_schedules
                    WHERE sdk_session_id = ? AND state = 'unknown'
                    """,
                    (self.binding.sdk_session_id,),
                )
                if unknown_schedules is not None and int(unknown_schedules[0]) > 0:
                    blockers.append(f"runtime_schedules_unknown:{int(unknown_schedules[0])}")
            if self._capabilities.supports("history_compact"):
                unresolved_compactions = await self._database.fetchone(
                    """
                    SELECT COUNT(*) FROM compaction_runs
                    WHERE sdk_session_id = ?
                      AND state IN ('pending', 'started', 'unknown')
                    """,
                    (self.binding.sdk_session_id,),
                )
                if unresolved_compactions is not None and int(unresolved_compactions[0]) > 0:
                    blockers.append(f"compaction_outcome_unknown:{int(unresolved_compactions[0])}")
        if require_quiet:
            if binding["runtime_processing"]:
                blockers.append("runtime_processing")
            if binding["runtime_has_active_work"]:
                blockers.append("runtime_active_work")
            if int(binding["native_queue_count"] or 0) > 0:
                blockers.append(f"native_queue:{int(binding['native_queue_count'])}")
            if int(binding["native_steering_count"] or 0) > 0:
                blockers.append(f"native_steering:{int(binding['native_steering_count'])}")
            if self._capabilities is None or self._capabilities.supports("tasks_list"):
                active_tasks = await self._database.fetchone(
                    """
                    SELECT COUNT(*) FROM background_observations
                    WHERE sdk_session_id = ?
                      AND observed_state IN ('running', 'idle', 'unknown')
                    """,
                    (self.binding.sdk_session_id,),
                )
                if active_tasks is not None and int(active_tasks[0]) > 0:
                    blockers.append(f"background_tasks_active:{int(active_tasks[0])}")
            if (
                self._capabilities is not None
                and self._capabilities.supports("remote_status")
                and (
                    binding["runtime_remote_mode"] in {"on", "unknown"}
                    or (
                        binding["runtime_remote_mode"] == "export"
                        and not self._capabilities.supports("remote_export_detach_safe")
                    )
                )
            ):
                blockers.append(f"remote_mode:{binding['runtime_remote_mode']}")
        return blockers

    async def _permission_reconcile_loop(self) -> None:
        while not self._permission_reconcile_stop.is_set():
            requested = asyncio.create_task(self._permission_reconcile_requested.wait())
            stopped = asyncio.create_task(self._permission_reconcile_stop.wait())
            done, pending = await asyncio.wait(
                {requested, stopped},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if stopped in done and self._permission_reconcile_stop.is_set():
                return
            self._permission_reconcile_requested.clear()
            if self.state != RuntimeState.READY:
                continue
            await self._require_inbox().join()
            if self.state != RuntimeState.READY:
                continue
            self._permission_reconcile_epoch += 1
            epoch = self._permission_reconcile_epoch
            posture = PermissionPosture.VERIFIED_ALLOW_ALL
            error_type: str | None = None
            managed = await self._bindings.by_thread(self.binding.thread_id)
            if managed is not None and managed.managed_permissions_blocked:
                posture = PermissionPosture.PLATFORM_BLOCKED
                error_type = "ManagedPermissionsBlocked"
            else:
                try:
                    await self._assert_owned_handle()
                    await self._sdk_call(self._bridge.ensure_allow_all(self._require_handle()))
                except asyncio.CancelledError:
                    raise
                except PermissionPostureError as error:
                    posture = PermissionPosture.PLATFORM_BLOCKED
                    error_type = type(error).__name__
                except Exception as error:
                    posture = PermissionPosture.UNKNOWN
                    error_type = type(error).__name__
            if self.state != RuntimeState.READY or not await self._is_current_owner():
                continue
            latest = await self._bindings.by_thread(self.binding.thread_id)
            if (
                latest is None
                or latest.runtime_generation != self.binding.runtime_generation
                or latest.owner_fence_token != self.binding.owner_fence_token
                or latest.attachment_state != AttachmentState.ATTACHED
            ):
                continue
            try:
                if posture == PermissionPosture.VERIFIED_ALLOW_ALL:
                    self.binding = await self._bindings.mark_permissions_verified(latest)
                else:
                    self.binding = await self._bindings.invalidate_permissions(
                        latest,
                        posture=posture,
                    )
            except BindingConflict:
                continue
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.permissions.reconciled",
                    "data": {
                        "epoch": epoch,
                        "posture": posture.value,
                        "error_type": error_type,
                    },
                },
                source="snapshot",
                internal_event_id=(f"permissions:{self.binding.runtime_generation}:{epoch}"),
            )

    def _on_sdk_event_accepted(self, event: Any) -> None:
        event_type = getattr(event, "type", None)
        raw_type = getattr(event_type, "value", event_type)
        loop = self._loop
        raw_event_id = getattr(event, "id", None)
        event_id = (
            str(raw_event_id)
            if raw_event_id is not None
            else f"missing-sdk-id:{self.binding.runtime_generation}:{time.time_ns()}"
        )
        if raw_type == "session.shutdown":
            self._handle_terminal = True
            self._accepting_sends = False
            if self.state != RuntimeState.CLOSING:
                self.state = RuntimeState.TERMINAL
            if loop is not None and (
                self._shutdown_finalize_task is None or self._shutdown_finalize_task.done()
            ):
                loop.call_soon_threadsafe(self._schedule_shutdown_finalization)
        topics: set[str] = set()
        if raw_type == "session.background_tasks_changed":
            topics.update({"activity", "queue", "tasks"})
        if raw_type == "pending_messages.modified":
            topics.update({"activity", "queue"})
        if raw_type == "session.remote_steerable_changed":
            topics.add("remote")
        if raw_type in {"commands.changed", "capabilities.changed"}:
            topics.add("commands")
        if raw_type in {
            "session.custom_agents_updated",
            "subagent.selected",
            "subagent.deselected",
        }:
            topics.add("agents")
        if raw_type in {
            "session.schedule_created",
            "session.schedule_cancelled",
            "session.schedule_rearmed",
        }:
            topics.add("schedules")
        if raw_type == "session.idle":
            topics.update({"activity", "queue", "tasks"})
        if raw_type in {
            "session.tools_updated",
            "session.skills_loaded",
            "session.custom_agents_updated",
            "session.extensions_loaded",
        }:
            topics.add("extensions")
        if raw_type in {
            "session.mcp_servers_loaded",
            "session.mcp_server_status_changed",
            "mcp.tools.list_changed",
            "mcp.resources.list_changed",
            "mcp.prompts.list_changed",
        }:
            topics.add("mcp")
        if loop is not None:
            for topic in topics.intersection(self._supported_snapshot_topics()):
                loop.call_soon_threadsafe(
                    self._enqueue_snapshot_request,
                    topic,
                    event_id,
                )
        if raw_type == "session.permissions_changed" and loop is not None:
            loop.call_soon_threadsafe(self._permission_reconcile_requested.set)
        if raw_type in {
            "session.managed_settings_resolved",
            "session.managed_settings_enforced",
        }:
            payload = event.to_dict()
            data = payload.get("data", {})
            blocked = raw_type.endswith("_enforced") or (
                isinstance(data, dict)
                and (bool(data.get("bypassPermissionsDisabled")) or bool(data.get("failClosed")))
            )
            handler = self._permission_handler
            if handler is not None:
                handler.set_managed_permissions_blocked(blocked)
            if loop is not None:
                loop.call_soon_threadsafe(self._permission_reconcile_requested.set)
        if (
            raw_type
            in {
                "session_limits_exhausted.requested",
                "sampling.requested",
                "mcp.headers_refresh_required",
            }
            and loop is not None
        ):
            loop.call_soon_threadsafe(self._schedule_protocol_response, event)

    def _schedule_shutdown_finalization(self) -> None:
        if self.state == RuntimeState.CLOSING:
            return
        self._shutdown_finalize_task = self._tasks.create(
            self._finalize_handle_shutdown(),
            name=f"session-shutdown:{self.binding.sdk_session_id}",
            source="session-shutdown",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )

    async def _finalize_handle_shutdown(self) -> None:
        async with self._lifecycle_lock:
            inbox = self._inbox
            if inbox is not None:
                await inbox.join()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if current is None:
                await self._stop_components(release_owner=True)
                return
            if self._inbox is not None and self._reducer is not None:
                await self._force_active_unknown()
                await self._inbox.join()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if (
                current is not None
                and current.runtime_generation == self.binding.runtime_generation
                and current.owner_fence_token == self.binding.owner_fence_token
                and current.attachment_state
                in {
                    AttachmentState.CREATING,
                    AttachmentState.RESUMING,
                    AttachmentState.ATTACHED,
                    AttachmentState.DISCONNECTING,
                    AttachmentState.TERMINAL,
                }
            ):
                self.binding = await self._bindings.mark_recovery_unknown(current)
            await self._stop_components(release_owner=True)
            self.state = RuntimeState.RECOVERY_UNKNOWN

    def _schedule_protocol_response(self, event: Any) -> None:
        if self._handle is None:
            self._deferred_protocol_events.append(event)
            return
        raw_type = str(getattr(getattr(event, "type", None), "value", "unknown"))
        task = self._tasks.create(
            self._respond_protocol_request(event),
            name=(
                f"protocol-response:{self.binding.sdk_session_id}:"
                f"{raw_type}:{getattr(event, 'id', 'unknown')}"
            ),
            source="protocol-response",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )
        self._protocol_tasks.add(task)
        task.add_done_callback(self._protocol_tasks.discard)

    def _flush_deferred_protocol_responses(self) -> None:
        deferred = self._deferred_protocol_events
        self._deferred_protocol_events = []
        for event in deferred:
            self._schedule_protocol_response(event)

    async def _respond_protocol_request(self, event: Any) -> None:
        inbox = self._require_inbox()
        await inbox.join()
        raw_type = str(getattr(getattr(event, "type", None), "value", "unknown"))
        raw_payload = event.to_dict()
        data = raw_payload.get("data", {})
        if not isinstance(data, dict) or data.get("requestId") is None:
            return
        request_id = str(data["requestId"])
        capability = {
            "session_limits_exhausted.requested": "protocol_session_limits_response",
            "sampling.requested": "protocol_sampling_response",
            "mcp.headers_refresh_required": "protocol_mcp_headers_response",
        }.get(raw_type)
        if capability is None:
            return
        if self._capabilities is not None and not self._capabilities.supports(capability):
            await self._protocol_responses.mark_unsupported(
                sdk_session_id=self.binding.sdk_session_id,
                generation=self.binding.runtime_generation,
                request_id=request_id,
                reason=f"capability gate failed: {capability}",
            )
            return

        safe_response: dict[str, Any]
        headers: dict[str, str] | None = None
        if raw_type == "session_limits_exhausted.requested":
            safe_response = {"action": "cancel"}
        elif raw_type == "sampling.requested":
            safe_response = {"response": None}
        else:
            snapshot = self._extension_snapshot
            server_name = str(data.get("serverName") or "")
            headers = None if snapshot is None else snapshot.dynamic_headers(server_name)
            safe_response = {
                "kind": "headers" if headers else "none",
                "header_names": sorted(headers or {}),
            }
        claim = await self._protocol_responses.claim(
            sdk_session_id=self.binding.sdk_session_id,
            generation=self.binding.runtime_generation,
            owner_fence_token=self._require_fence_token(),
            request_id=request_id,
            response_payload=safe_response,
        )
        if claim is None:
            return
        try:
            await self._assert_owned_handle(allow_attaching=True)
            handle = self._require_handle()
            if raw_type == "session_limits_exhausted.requested":
                accepted = await self._sdk_call(
                    self._bridge.respond_session_limits(handle, request_id)
                )
            elif raw_type == "sampling.requested":
                accepted = await self._sdk_call(
                    self._bridge.respond_sampling(handle, request_id, None)
                )
            else:
                accepted = await self._sdk_call(
                    self._bridge.respond_mcp_headers(handle, request_id, headers)
                )
        except asyncio.CancelledError:
            await asyncio.shield(
                self._protocol_responses.settle(
                    claim,
                    state="unknown",
                    error_code="cancelled",
                )
            )
            raise
        except Exception as error:
            await self._protocol_responses.settle(
                claim,
                state="unknown",
                error_code=type(error).__name__,
            )
        else:
            await self._protocol_responses.settle(
                claim,
                state="confirmed" if accepted else "rejected",
                error_code=None if accepted else "already_resolved_or_expired",
            )

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[str], None],
        on_loss: Callable[[str], None],
    ) -> None:
        async with self._admission_lock:
            self._service_quiesced = True
            self._service_quiesce_violations = 0
            self._service_quiesce_violation_callback = on_violation
            self._accepting_sends = False
            if self._inbox is not None:
                self._inbox.set_quiesce_observers(
                    self._record_service_quiesce_producer,
                    on_loss,
                )
                overflow = self._inbox.overflow
                if overflow is not None and overflow.lost_count:
                    on_loss("pre_quiesce_inbox_overflow")
        if self._mailbox is not None:
            await self._mailbox.pause_admission()
            await self._mailbox.wait_idle()
        async with self._queue_dispatch_lock:
            pass
        await self._stop_service_quiesce_producers()
        await self.drain_service_quiesce()

    async def end_service_quiesce(self) -> None:
        if self._inbox is not None:
            self._inbox.set_quiesce_observers(None, None)
        await self._restart_service_quiesce_producers()
        if self._mailbox is not None:
            await self._mailbox.resume_admission()
        async with self._admission_lock:
            self._service_quiesced = False
            self._service_quiesce_violations = 0
            self._service_quiesce_violation_callback = None
            if self.state == RuntimeState.READY:
                self._accepting_sends = True

    async def drain_service_quiesce(self) -> None:
        if self._inbox is not None:
            await self._inbox.join()

    def service_quiesce_metrics(self) -> tuple[int, int]:
        depth = 0 if self._inbox is None else self._inbox.size
        return depth, self._service_quiesce_violations

    def _record_service_quiesce_producer(self, source: str) -> None:
        self._service_quiesce_violations += 1
        callback = self._service_quiesce_violation_callback
        if callback is not None:
            callback(source)

    async def _stop_service_quiesce_producers(self) -> None:
        if self._service_producers_stopped:
            return
        self._queue_stop.set()
        self._task_reconcile_stop.set()
        self._task_reconcile_requested.set()
        self._permission_reconcile_stop.set()
        self._permission_reconcile_requested.set()
        self._renewal_stop.set()
        tasks = (
            self._queue_task,
            self._task_reconcile_task,
            self._permission_reconcile_task,
            self._renewal_task,
        )
        for task in tasks:
            if task is None:
                continue
            task.cancel()
        active = [task for task in tasks if task is not None]
        try:
            if active:
                async with asyncio.timeout(self._shutdown_timeout_seconds):
                    await asyncio.gather(*active, return_exceptions=True)
        finally:
            self._queue_task = None
            self._task_reconcile_task = None
            self._permission_reconcile_task = None
            self._renewal_task = None
            self._service_producers_stopped = True

    async def _restart_service_quiesce_producers(self) -> None:
        if not self._service_producers_stopped:
            return
        if self.state == RuntimeState.READY:
            self._start_runtime_producers()
        elif self.state == RuntimeState.DEGRADED and self._lease is not None:
            self._renewal_stop.clear()
            self._renewal_task = self._tasks.create(
                self._renew_owner(),
                name=f"owner-renew:{self.binding.sdk_session_id}",
            )
        self._service_producers_stopped = False

    async def set_mode(
        self,
        mode: Literal["interactive", "plan", "autopilot"],
        *,
        idempotency_key: str,
    ) -> str:
        await self._assert_dispatchable()
        blockers = await self.detach_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        transition_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self.binding.sdk_session_id}:mode:{idempotency_key}",
            )
        )
        inbox = self._require_inbox()
        await inbox.commit_internal(
            {
                "type": "copilotd.mode.pending",
                "data": {"mode": mode, "transition_id": transition_id},
            },
            internal_event_id=f"mode:{transition_id}:pending",
        )

        async def dispatch() -> str:
            await self._assert_owned_handle()
            handle = self._require_handle()
            await self._bridge.set_mode(handle, mode)
            observed = await self._bridge.get_mode(handle)
            if observed != mode:
                raise RuntimeError(f"mode reconciliation returned {observed}; expected {mode}")
            return observed

        try:
            observed = await self._require_mailbox().submit(
                kind="mode",
                idempotency_key=f"mode:{idempotency_key}",
                input_payload={"mode": mode},
                operation=dispatch,
            )
        except OperationRejected:
            await inbox.commit_internal(
                {
                    "type": "copilotd.mode.rejected",
                    "data": {"mode": mode, "transition_id": transition_id},
                },
                internal_event_id=f"mode:{transition_id}:rejected",
            )
            raise
        except OperationAmbiguous:
            await inbox.commit_internal(
                {
                    "type": "copilotd.mode.unknown",
                    "data": {"mode": mode, "transition_id": transition_id},
                },
                internal_event_id=f"mode:{transition_id}:unknown",
            )
            raise

        await inbox.commit_internal(
            {
                "type": "copilotd.mode.confirmed",
                "data": {"mode": observed, "transition_id": transition_id},
            },
            internal_event_id=f"mode:{transition_id}:confirmed",
        )
        binding = await self._bindings.by_thread(self.binding.thread_id)
        if binding is None:
            raise SessionNotReady("session binding disappeared after mode change")
        self.binding = binding
        return str(observed)

    async def set_model(
        self,
        model: str,
        *,
        reasoning_effort: str | None,
        context_tier: str | None,
        reasoning_summary: str | None = None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        await self._assert_dispatchable()
        blockers = await self.detach_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        available = await self._bridge.list_models()
        selected = next((item for item in available if item.get("id") == model), None)
        if selected is None:
            raise ValueError(f"unknown Copilot model: {model}")
        policy = selected.get("policy") or {}
        if policy.get("state") == "disabled":
            raise ValueError(f"Copilot model is disabled by policy: {model}")
        supported_efforts = selected.get("supportedReasoningEfforts")
        if (
            reasoning_effort is not None
            and supported_efforts is not None
            and reasoning_effort not in supported_efforts
        ):
            raise ValueError(
                f"{model} does not support reasoning effort {reasoning_effort}; "
                f"choose one of {', '.join(supported_efforts)}"
            )
        if context_tier not in {None, "default", "long_context"}:
            raise ValueError(f"unsupported context tier: {context_tier}")
        if reasoning_summary is not None:
            reasoning_summary = reasoning_summary.strip()
            if not reasoning_summary:
                raise CDInputError("reasoning summary cannot be empty")
            if reasoning_summary not in {"none", "concise", "detailed"}:
                raise CDInputError(f"unsupported reasoning summary: {reasoning_summary}")
            adapter_supported = self._model_summary_adapter is not None and (
                self._model_summary_adapter.supports_reasoning_summary(model)
            )
            capability_supported = self._capabilities is not None and (
                self._capabilities.supports("reasoning_summary_readback")
            )
            if not adapter_supported and not capability_supported:
                raise CDCapabilityError(
                    f"{model} does not expose confirmed reasoning-summary readback"
                )

        target: dict[str, Any] = {"modelId": model}
        confirmation_mask = ["modelId"]
        if reasoning_effort is not None:
            target["reasoningEffort"] = reasoning_effort
            confirmation_mask.append("reasoningEffort")
        if reasoning_summary is not None:
            target["reasoningSummary"] = reasoning_summary
            confirmation_mask.append("reasoningSummary")
        if context_tier is not None:
            target["contextTier"] = context_tier
            confirmation_mask.append("contextTier")
        target["confirmationMask"] = confirmation_mask
        transition_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self.binding.sdk_session_id}:model:{idempotency_key}",
            )
        )
        inbox = self._require_inbox()
        await inbox.commit_internal(
            {
                "type": "copilotd.model.pending",
                "data": {"config": target, "transition_id": transition_id},
            },
            internal_event_id=f"model:{transition_id}:pending",
        )

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            handle = self._require_handle()
            await self._sdk_call(
                self._bridge.set_model(
                    handle,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    reasoning_summary=reasoning_summary,
                    context_tier=context_tier,
                )
            )
            observed = await self._sdk_call(self._bridge.get_current_model(handle))
            if reasoning_summary is not None and self._model_summary_adapter is not None:
                readback = await self._model_summary_adapter.read_current_model(
                    session_id=self.binding.sdk_session_id
                )
                if readback is not None:
                    observed = {**observed, **dict(readback)}
            observed["knownFields"] = sorted(
                key
                for key in (
                    "modelId",
                    "reasoningEffort",
                    "reasoningSummary",
                    "contextTier",
                )
                if key in observed
            )
            if not _model_config_matches(target, observed):
                raise RuntimeError("model configuration could not be fully confirmed")
            return {key: observed.get(key) for key in confirmation_mask}

        try:
            observed = await self._require_mailbox().submit(
                kind="model",
                idempotency_key=f"model:{idempotency_key}",
                input_payload=target,
                operation=dispatch,
            )
        except OperationRejected:
            await inbox.commit_internal(
                {
                    "type": "copilotd.model.rejected",
                    "data": {"transition_id": transition_id},
                },
                internal_event_id=f"model:{transition_id}:rejected",
            )
            raise
        except OperationAmbiguous:
            await inbox.commit_internal(
                {
                    "type": "copilotd.model.unknown",
                    "data": {"transition_id": transition_id},
                },
                internal_event_id=f"model:{transition_id}:unknown",
            )
            raise

        await inbox.commit_internal(
            {
                "type": "copilotd.model.confirmed",
                "data": {
                    "config": target,
                    "observed": observed,
                    "transition_id": transition_id,
                },
            },
            internal_event_id=f"model:{transition_id}:confirmed",
        )
        return observed

    async def context_snapshot(self) -> dict[str, Any] | None:
        await self._assert_owned_handle()
        capability_available = self._capabilities is None or self._capabilities.supports(
            "context_info"
        )
        error: Exception | None = None
        if capability_available:
            try:
                payload = await self._sdk_call(self._bridge.get_context(self._require_handle()))
                if payload is not None:
                    observed_at = time.time()
                    await self._require_inbox().commit_internal(
                        {
                            "type": "copilotd.context.observed",
                            "data": {
                                "payload": payload,
                                "observed_at": observed_at,
                            },
                        },
                        source="snapshot",
                        internal_event_id=(
                            f"context:{self.binding.runtime_generation}:{time.time_ns()}"
                        ),
                    )
                    return payload
            except Exception as caught:
                error = caught
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.context.failed",
                        "data": {"error_type": type(caught).__name__},
                    },
                    source="snapshot",
                    internal_event_id=(
                        f"context:{self.binding.runtime_generation}:failed:{time.time_ns()}"
                    ),
                )
        stale = await self._read_projection("context_projections")
        if stale is not None:
            return stale
        if error is not None:
            raise error
        return None

    async def usage_snapshot(self) -> dict[str, Any]:
        await self._assert_owned_handle()
        capability_available = self._capabilities is None or self._capabilities.supports("usage")
        error: Exception | None = None
        if capability_available:
            try:
                payload = await self._sdk_call(self._bridge.get_usage(self._require_handle()))
                observed_at = time.time()
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.usage.observed",
                        "data": {
                            "payload": payload,
                            "observed_at": observed_at,
                        },
                    },
                    source="snapshot",
                    internal_event_id=(f"usage:{self.binding.runtime_generation}:{time.time_ns()}"),
                )
                return payload
            except Exception as caught:
                error = caught
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.usage.failed",
                        "data": {"error_type": type(caught).__name__},
                    },
                    source="snapshot",
                    internal_event_id=(
                        f"usage:{self.binding.runtime_generation}:failed:{time.time_ns()}"
                    ),
                )
        stale = await self._read_projection("usage_projections")
        if stale is not None:
            return stale
        if error is not None:
            raise error
        return {"_stale": True, "_unavailable": True}

    async def _read_projection(self, table: str) -> dict[str, Any] | None:
        if table not in {"context_projections", "usage_projections"}:
            raise ValueError(f"unsupported projection table: {table}")
        row = await self._database.fetchone(
            f"""
            SELECT payload_json, observed_at, stale, stale_reason
            FROM {table} WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            payload = {"value": payload}
        return {
            **payload,
            "_stale": True,
            "_observedAt": float(row["observed_at"]),
            "_staleReason": row["stale_reason"] or "live reconciliation unavailable",
            "_error": row["stale_reason"] or "live reconciliation unavailable",
        }

    async def readiness_snapshot(self) -> dict[str, Any]:
        return await self._refresh_readiness()

    async def steer(self, text: str, *, idempotency_key: str) -> str:
        normalized = text.strip()
        if not normalized:
            raise CDInputError("steer text cannot be empty")
        readiness = await self._refresh_readiness()
        if not (readiness["processing"] or readiness["hasActiveWork"] or readiness["abortable"]):
            raise CDSessionStateError("steering requires an observed active Copilot turn")
        return await self.send(
            normalized,
            idempotency_key=idempotency_key,
            mode="immediate",
            origin="steer",
        )

    async def _projection_snapshot(
        self,
        kind: Literal["context", "usage"],
        operation: Awaitable[dict[str, Any] | None],
        *,
        allow_none: bool,
    ) -> dict[str, Any] | None:
        observed_at = time.time()
        error: Exception | None = None
        try:
            snapshot = await self._sdk_call(operation)
        except Exception as caught:
            error = caught
            snapshot = None
        if snapshot is not None:
            value = dict(snapshot)
            await self._database.execute(
                """
                INSERT INTO session_projection_snapshots(
                    session_id, kind, payload, observed_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, kind) DO UPDATE SET
                    payload = excluded.payload,
                    observed_at = excluded.observed_at
                """,
                (
                    self.binding.sdk_session_id,
                    kind,
                    json.dumps(value, ensure_ascii=False, sort_keys=True),
                    observed_at,
                ),
            )
            return value
        row = await self._database.fetchone(
            """
            SELECT payload, observed_at FROM session_projection_snapshots
            WHERE session_id = ? AND kind = ?
            """,
            (self.binding.sdk_session_id, kind),
        )
        if row is not None:
            value = json.loads(str(row["payload"]))
            value["_stale"] = True
            value["_observed_at"] = float(row["observed_at"])
            if error is not None:
                value["_error"] = type(error).__name__
            return value
        if error is not None:
            raise error
        return None if allow_none else {}

    async def refresh_native_commands(self) -> tuple[dict[str, Any], ...]:
        self._require_capability("commands_list")
        await self._assert_owned_handle()
        await self._query_snapshot_topic("commands")
        await self._require_inbox().join()
        return await self._native_manifest.available_builtins(self.binding.sdk_session_id)

    async def invoke_native_command(
        self,
        command_name: str,
        input_text: str | None,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        result = await self._invoke_command_operation(
            command_name,
            input_text,
            idempotency_key=idempotency_key,
            require_manifest=True,
            require_quiet=True,
        )
        if result["kind"] != NativeCommandResultKind.AGENT_PROMPT.value:
            return result
        prompt = result.get("prompt")
        if not isinstance(prompt, str) or not prompt:
            raise NativeCapabilityError("runtime returned agent-prompt without a generated prompt")
        invocation_id = self._native_id("command", idempotency_key)
        submission_key = f"native-command:{invocation_id}:agent-prompt"
        await self.send(
            prompt,
            idempotency_key=submission_key,
            agent_mode=cast(AgentMode | None, result.get("mode")),
            origin=f"builtin:{command_name}",
        )
        submission_id = self._native_id("submission", submission_key)
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.native_command.settled",
                "data": {
                    "invocation_id": invocation_id,
                    "state": "confirmed",
                    "result_kind": result["kind"],
                    "result": result,
                    "agent_submission_id": submission_id,
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"native-command:{invocation_id}:agent-submission",
        )
        return {**result, "agent_submission_id": submission_id}

    async def continue_native_command(
        self,
        selection_token: str,
        selection: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_capability("commands_result_select_subcommand")
        row = await self._database.fetchone(
            """
            SELECT command_name, result_json
            FROM runtime_command_invocations
            WHERE sdk_session_id = ? AND selection_token = ?
              AND state = 'confirmed' AND result_kind = 'select-subcommand'
            """,
            (self.binding.sdk_session_id, selection_token),
        )
        if row is None or row["result_json"] is None:
            raise NativeCapabilityError("native command selection is stale or invalid")
        previous = json.loads(row["result_json"])
        options = {
            str(option["name"])
            for option in previous.get("options", [])
            if isinstance(option, dict) and option.get("name") is not None
        }
        if selection not in options:
            raise ValueError("selection is not one of the runtime-provided options")
        command = previous.get("command")
        if not isinstance(command, str) or not command:
            raise NativeCapabilityError("runtime selection result omitted its command")
        await self._native_manifest.require_builtin(
            self.binding.sdk_session_id,
            str(row["command_name"]),
        )
        if command != str(row["command_name"]):
            await self._native_manifest.require_builtin(
                self.binding.sdk_session_id,
                command,
            )
        return await self._invoke_command_operation(
            command,
            selection,
            idempotency_key=idempotency_key,
            require_manifest=True,
            require_quiet=True,
        )

    async def _invoke_command_operation(
        self,
        command_name: str,
        input_text: str | None,
        *,
        idempotency_key: str,
        require_manifest: bool,
        require_quiet: bool,
    ) -> dict[str, Any]:
        self._require_capability("commands_invoke")
        await self._assert_dispatchable()
        if require_quiet:
            blockers = await self.operational_blockers()
            if blockers:
                raise DetachBlocked(blockers)
        manifest_entry = (
            await self._native_manifest.require_builtin(
                self.binding.sdk_session_id,
                command_name,
            )
            if require_manifest
            else None
        )
        invocation_id = self._native_id("command", idempotency_key)
        operation_key = f"native-command:{idempotency_key}"
        input_hash = stable_hash(input_text) or hashlib.sha256(b"").hexdigest()

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            if manifest_entry is not None:
                current_entry = await self._native_manifest.require_builtin(
                    self.binding.sdk_session_id,
                    command_name,
                )
                if current_entry["manifest_generation"] != manifest_entry["manifest_generation"]:
                    raise OperationRejected("native command manifest changed before invocation")
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.native_command.pending",
                    "data": {
                        "invocation_id": invocation_id,
                        "operation_id": operation_id,
                        "command_name": command_name,
                        "input_hash": input_hash,
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"native-command:{invocation_id}:pending",
            )
            result = await self._sdk_call(
                self._bridge.invoke_command(
                    self._require_handle(),
                    name=command_name,
                    input_text=input_text,
                )
            )
            payload = cast(dict[str, Any], result.to_dict())
            result_kind = str(payload["kind"])
            normalized_kind = result_kind.replace("-", "_")
            exact_result_capability = (
                f"builtin_{command_name.replace('-', '_')}_result_{normalized_kind}"
            )
            if (
                self._capabilities is not None
                and exact_result_capability in self._capabilities.capabilities
            ):
                self._require_capability(exact_result_capability)
            else:
                self._require_capability(f"commands_result_{normalized_kind}")
            selection_token = (
                self._native_id("selection", invocation_id)
                if payload["kind"] == NativeCommandResultKind.SELECT_SUBCOMMAND.value
                else None
            )
            if selection_token is not None:
                payload["selection_token"] = selection_token
            return payload

        try:
            payload = cast(
                dict[str, Any],
                await self._require_mailbox().submit(
                    kind="native-command",
                    idempotency_key=operation_key,
                    input_payload={
                        "command_name": command_name,
                        "input_hash": input_hash,
                    },
                    operation=dispatch,
                ),
            )
        except OperationRejected:
            await self._settle_native_command_failure(
                invocation_id,
                state="rejected",
            )
            raise
        except OperationAmbiguous:
            await self._settle_native_command_failure(
                invocation_id,
                state="unknown",
            )
            raise
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.native_command.settled",
                "data": {
                    "invocation_id": invocation_id,
                    "state": "confirmed",
                    "result_kind": payload["kind"],
                    "result": payload,
                    "selection_token": payload.get("selection_token"),
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"native-command:{invocation_id}:confirmed",
        )
        return payload

    async def _settle_native_command_failure(
        self,
        invocation_id: str,
        *,
        state: Literal["rejected", "unknown"],
    ) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.native_command.settled",
                "data": {
                    "invocation_id": invocation_id,
                    "state": state,
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"native-command:{invocation_id}:{state}",
        )

    async def ask_ephemeral(
        self,
        question: str,
        *,
        idempotency_key: str,
    ) -> str:
        self._require_capability("ephemeral_query")
        if not question.strip():
            raise ValueError("question cannot be empty")
        blockers = await self.operational_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        query_id = self._native_id("ephemeral-query", idempotency_key)
        operation_key = f"ephemeral-query:{idempotency_key}"
        question_hash = stable_hash(question)

        async def dispatch() -> str:
            await self._assert_owned_handle()
            handle = self._require_handle()
            history_before = len(await handle.get_events())
            receive_before = self._require_inbox().last_sdk_receive_seq
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.ephemeral_query.pending",
                    "data": {
                        "query_id": query_id,
                        "operation_id": operation_id,
                        "question_hash": question_hash,
                        "history_count_before": history_before,
                        "sdk_receive_seq_before": receive_before,
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"ephemeral-query:{query_id}:pending",
            )
            answer = await self._sdk_call(self._bridge.ephemeral_query(handle, question))
            await self._require_inbox().join()
            history_after = len(await handle.get_events())
            receive_after = self._require_inbox().last_sdk_receive_seq
            tool_event = await self._database.fetchone(
                """
                SELECT 1 FROM event_journal
                WHERE sdk_session_id = ?
                  AND sdk_receive_seq > ? AND sdk_receive_seq <= ?
                  AND raw_type LIKE 'tool.%'
                LIMIT 1
                """,
                (
                    self.binding.sdk_session_id,
                    receive_before,
                    receive_after,
                ),
            )
            if history_after != history_before or tool_event is not None:
                raise NativeCapabilityError(
                    "ephemeral query violated its no-tools/no-history contract"
                )
            return {
                "answer": answer,
                "answer_hash": stable_hash(answer),
                "history_count_after": history_after,
                "sdk_receive_seq_after": receive_after,
            }

        try:
            result = cast(
                dict[str, Any],
                await self._require_mailbox().submit(
                    kind="ephemeral-query",
                    idempotency_key=operation_key,
                    input_payload={"question_hash": question_hash},
                    operation=dispatch,
                    result_persistence=lambda value: {
                        "answer_hash": value["answer_hash"],
                        "confirmed": True,
                    },
                ),
            )
            if "answer" not in result:
                raise NativeCapabilityError(
                    "ephemeral query already completed; its answer was not retained"
                )
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.ephemeral_query.settled",
                    "data": {
                        "query_id": query_id,
                        "history_count_after": result["history_count_after"],
                        "sdk_receive_seq_after": result["sdk_receive_seq_after"],
                        "answer_hash": result["answer_hash"],
                        "state": "confirmed",
                        "settled_at": time.time(),
                    },
                },
                internal_event_id=f"ephemeral-query:{query_id}:confirmed",
            )
            return str(result["answer"])
        except OperationRejected:
            state = "rejected"
            raise
        except OperationAmbiguous:
            state = "unknown"
            raise
        finally:
            if "state" in locals():
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.ephemeral_query.settled",
                        "data": {
                            "query_id": query_id,
                            "state": state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=f"ephemeral-query:{query_id}:{state}",
                )

    async def compact(
        self,
        focus: str | None,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_capability("history_compact")
        blockers = await self.operational_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        compaction_id = self._native_id("compaction", idempotency_key)
        operation_key = f"compact:{idempotency_key}"
        unresolved = await self._database.fetchone(
            """
            SELECT compaction_id FROM compaction_runs
            WHERE sdk_session_id = ?
              AND state IN ('pending', 'started', 'unknown')
            ORDER BY created_at DESC LIMIT 1
            """,
            (self.binding.sdk_session_id,),
        )
        if unresolved is not None and unresolved["compaction_id"] != compaction_id:
            raise SessionNotReady(
                "a prior compaction is unresolved; reconcile it before compacting again"
            )

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            handle = self._require_handle()
            operation_id = await self._operation_id(operation_key)
            cursor = await self._database.fetchone(
                "SELECT event_cursor FROM session_bindings WHERE sdk_session_id = ?",
                (self.binding.sdk_session_id,),
            )
            context_before = await self._bridge.get_context(handle)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.compaction.pending",
                    "data": {
                        "compaction_id": compaction_id,
                        "operation_id": operation_id,
                        "focus_hash": stable_hash(focus),
                        "event_cursor_before": (None if cursor is None else cursor["event_cursor"]),
                        "sdk_receive_seq_before": (self._require_inbox().last_sdk_receive_seq),
                        "context_before": context_before,
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"compaction:{compaction_id}:pending",
            )
            result = await self._sdk_call(self._bridge.compact_history(handle, focus=focus))
            if not result.get("success"):
                raise OperationRejected(str(result.get("error") or "runtime rejected compaction"))
            await self._require_inbox().join()
            await self._recover_event_log(handle, initialize=False)
            await self._require_inbox().join()
            context_after = await self._bridge.get_context(handle)
            return {
                "compaction_id": compaction_id,
                "result": result,
                "context": context_after,
            }

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            compacted = cast(
                dict[str, Any],
                await self._require_mailbox().submit(
                    kind="compact",
                    idempotency_key=operation_key,
                    input_payload={"focus_hash": stable_hash(focus)},
                    operation=dispatch,
                ),
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.compaction.settled",
                        "data": {
                            "compaction_id": compaction_id,
                            "state": failure_state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=(f"compaction:{compaction_id}:{failure_state}"),
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.compaction.settled",
                "data": {
                    "compaction_id": compaction_id,
                    "result": compacted["result"],
                    "context_after": compacted["context"],
                    "state": "confirmed",
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"compaction:{compaction_id}:confirmed",
        )
        return compacted

    async def reconcile_compaction(self, compaction_id: str) -> str:
        self._require_capability("history_compact")
        await self._assert_owned_handle()
        await self._recover_event_log(self._require_handle(), initialize=False)
        await self._require_inbox().join()
        await self._reconcile_unresolved_compactions(basis="explicit_event_log_reconcile")
        row = await self._database.fetchone(
            """
            SELECT state FROM compaction_runs
            WHERE compaction_id = ? AND sdk_session_id = ?
            """,
            (compaction_id, self.binding.sdk_session_id),
        )
        if row is None:
            raise ValueError("compaction intent does not exist")
        return str(row["state"])

    async def _reconcile_unresolved_compactions(self, *, basis: str) -> None:
        rows = await self._database.fetchall(
            """
            SELECT compaction_id FROM compaction_runs
            WHERE sdk_session_id = ? AND state IN ('pending', 'started')
            ORDER BY created_at
            """,
            (self.binding.sdk_session_id,),
        )
        for row in rows:
            compaction_id = str(row["compaction_id"])
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.compaction.settled",
                    "data": {
                        "compaction_id": compaction_id,
                        "state": "unknown",
                        "result": {
                            "basis": basis,
                            "completion_event_observed": False,
                        },
                        "settled_at": time.time(),
                    },
                },
                internal_event_id=(f"compaction:{compaction_id}:no-completion:{basis}"),
            )

    async def start_fleet(
        self,
        prompt: str,
        *,
        idempotency_key: str,
    ) -> dict[str, str]:
        self._require_capability("fleet_start")
        if not prompt.strip():
            raise ValueError("Fleet prompt cannot be empty")
        blockers = await self.operational_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        fleet_run_id = self._native_id("fleet", idempotency_key)
        submission_id = self._native_id(
            "submission",
            f"fleet:{idempotency_key}",
        )
        operation_key = f"fleet:{idempotency_key}"
        config = await self._database.fetchone(
            """
            SELECT runtime_mode, runtime_agent, runtime_project_config_version
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if config is None:
            raise SessionNotReady("Fleet execution configuration is unavailable")

        async def dispatch() -> dict[str, str]:
            await self._assert_owned_handle()
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.fleet.pending",
                    "data": {
                        "fleet_run_id": fleet_run_id,
                        "submission_id": submission_id,
                        "operation_id": operation_id,
                        "prompt_hash": stable_hash(prompt),
                        "requested_mode": config["runtime_mode"],
                        "requested_agent": config["runtime_agent"],
                        "requested_session_config_version": config[
                            "runtime_project_config_version"
                        ],
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"fleet:{fleet_run_id}:pending",
            )
            if not await self._sdk_call(
                self._bridge.start_fleet(self._require_handle(), prompt),
                timeout_seconds=120,
            ):
                raise OperationRejected("runtime did not start Fleet")
            result = {
                "fleet_run_id": fleet_run_id,
                "submission_id": submission_id,
            }
            return result

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            result = cast(
                dict[str, str],
                await self._require_mailbox().submit(
                    kind="fleet",
                    idempotency_key=operation_key,
                    input_payload={"prompt_hash": stable_hash(prompt)},
                    operation=dispatch,
                ),
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.fleet.settled",
                        "data": {
                            "fleet_run_id": fleet_run_id,
                            "submission_id": submission_id,
                            "state": failure_state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=f"fleet:{fleet_run_id}:{failure_state}",
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.fleet.settled",
                "data": {
                    **result,
                    "state": "confirmed",
                    "result": {"started": True},
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"fleet:{fleet_run_id}:confirmed",
        )
        return result

    async def task_action(
        self,
        action: NativeTaskAction | str,
        *,
        task_id: str | None = None,
        message: str | None = None,
        wait_seconds: float = 30,
        idempotency_key: str,
    ) -> dict[str, Any]:
        selected = NativeTaskAction(action)
        capability = {
            NativeTaskAction.LIST: "tasks_list",
            NativeTaskAction.SHOW: "tasks_progress",
            NativeTaskAction.PROGRESS: "tasks_progress",
            NativeTaskAction.MESSAGE: "tasks_message",
            NativeTaskAction.PROMOTE: "tasks_promote",
            NativeTaskAction.CANCEL: "tasks_cancel",
            NativeTaskAction.ALL: "tasks_cancel",
            NativeTaskAction.REMOVE: "tasks_remove",
            NativeTaskAction.WAIT: "tasks_wait",
        }[selected]
        self._require_capability(capability)
        await self._assert_dispatchable()
        if (
            selected
            in {
                NativeTaskAction.SHOW,
                NativeTaskAction.PROGRESS,
                NativeTaskAction.MESSAGE,
                NativeTaskAction.CANCEL,
                NativeTaskAction.REMOVE,
            }
            and not task_id
        ):
            raise ValueError(f"task id is required for {selected.value}")
        if selected == NativeTaskAction.MESSAGE and not (message or "").strip():
            raise ValueError("task message cannot be empty")
        if wait_seconds <= 0:
            raise ValueError("task wait timeout must be positive")
        action_id = self._native_id("task-action", idempotency_key)
        operation_key = f"task:{idempotency_key}"
        input_payload = {
            "action": selected.value,
            "task_id": task_id,
            "message_hash": stable_hash(message),
            "wait_seconds": wait_seconds if selected == NativeTaskAction.WAIT else None,
        }

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            handle = self._require_handle()
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.task_action.pending",
                    "data": {
                        "action_id": action_id,
                        "operation_id": operation_id,
                        "task_id": task_id,
                        "action": selected.value,
                        "input_hash": stable_hash(json_payload(input_payload)),
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"task-action:{action_id}:pending",
            )
            await self._sdk_call(self._bridge.refresh_tasks(handle))
            tasks = await self._sdk_call(self._bridge.list_tasks(handle))
            action_result: Any
            if selected == NativeTaskAction.LIST:
                action_result = {"listed": len(tasks)}
            elif selected in {NativeTaskAction.SHOW, NativeTaskAction.PROGRESS}:
                task = _task_by_id(tasks, cast(str, task_id))
                progress = await self._sdk_call(
                    self._bridge.get_task_progress(
                        handle,
                        cast(str, task_id),
                    )
                )
                action_result = {"task": task, "progress": progress}
            elif selected == NativeTaskAction.MESSAGE:
                action_result = await self._sdk_call(
                    self._bridge.send_task_message(
                        handle,
                        cast(str, task_id),
                        cast(str, message),
                    )
                )
                if not action_result.get("sent"):
                    raise OperationRejected(
                        str(action_result.get("error") or "task message was rejected")
                    )
            elif selected == NativeTaskAction.PROMOTE:
                effective_id = task_id
                if effective_id is None:
                    current = await self._sdk_call(self._bridge.get_current_promotable_task(handle))
                    if current is None:
                        raise OperationRejected(
                            "runtime has no current task eligible for promotion"
                        )
                    effective_id = str(current["id"])
                _task_by_id(tasks, effective_id)
                if not await self._sdk_call(self._bridge.promote_task(handle, effective_id)):
                    raise OperationRejected("runtime rejected task promotion")
                action_result = {"promoted": True, "task_id": effective_id}
            elif selected == NativeTaskAction.CANCEL:
                _task_by_id(tasks, cast(str, task_id))
                if not await self._sdk_call(self._bridge.cancel_task(handle, cast(str, task_id))):
                    raise OperationRejected("runtime rejected task cancellation")
                action_result = {"cancelled": [task_id]}
            elif selected == NativeTaskAction.ALL:
                cancellable = [
                    str(task["id"])
                    for task in tasks
                    if str(task.get("status", "")).lower() not in TERMINAL_TASK_STATES
                ]
                cancelled: list[str] = []
                rejected: list[str] = []
                for candidate in cancellable:
                    if await self._sdk_call(self._bridge.cancel_task(handle, candidate)):
                        cancelled.append(candidate)
                    else:
                        rejected.append(candidate)
                if rejected and not cancelled:
                    raise OperationRejected(
                        "runtime rejected cancellation for: " + ", ".join(rejected)
                    )
                action_result = {
                    "cancelled": cancelled,
                    "rejected": rejected,
                    "partial": bool(cancelled and rejected),
                }
            elif selected == NativeTaskAction.REMOVE:
                task = _task_by_id(tasks, cast(str, task_id))
                if str(task.get("status", "")).lower() not in TERMINAL_TASK_STATES:
                    raise OperationRejected("only terminal tasks can be removed")
                if not await self._sdk_call(self._bridge.remove_task(handle, cast(str, task_id))):
                    raise OperationRejected("runtime rejected task removal")
                action_result = {"removed": True, "task_id": task_id}
            else:
                await self._sdk_call(
                    self._bridge.wait_for_tasks(
                        handle,
                        wait_seconds=wait_seconds,
                    ),
                    timeout_seconds=wait_seconds + 10,
                )
                action_result = {"waited": True}
            await self._sdk_call(self._bridge.refresh_tasks(handle))
            tasks = await self._sdk_call(self._bridge.list_tasks(handle))
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.tasks.snapshot",
                    "data": {
                        "tasks": tasks,
                        "observed_at": time.time(),
                    },
                },
                source="snapshot",
                internal_event_id=f"task-action:{action_id}:snapshot",
            )
            if selected == NativeTaskAction.SHOW and task_id is not None:
                card = await self._taskdeck.task(
                    self.binding.sdk_session_id,
                    task_id,
                )
                if card is not None:
                    panel_id = str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"copilotd:{self.binding.sdk_session_id}:taskdeck",
                        )
                    )[:16]
                    await self._require_inbox().commit_internal(
                        {
                            "type": "copilotd.taskdeck.view_changed",
                            "data": {
                                "panel_id": panel_id,
                                "selected_card_token": card.card_token,
                                "page": 0,
                                "expanded": True,
                            },
                        },
                        internal_event_id=f"task-action:{action_id}:focus",
                    )
            return {"action": selected.value, "result": action_result, "tasks": tasks}

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            result = cast(
                dict[str, Any],
                await self._require_mailbox().submit(
                    kind=f"task-{selected.value}",
                    idempotency_key=operation_key,
                    input_payload=input_payload,
                    operation=dispatch,
                ),
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.task_action.settled",
                        "data": {
                            "action_id": action_id,
                            "state": failure_state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=f"task-action:{action_id}:{failure_state}",
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.task_action.settled",
                "data": {
                    "action_id": action_id,
                    "state": "confirmed",
                    "result": result["result"],
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"task-action:{action_id}:confirmed",
        )
        cards = await self._taskdeck.cards(self.binding.sdk_session_id)
        result["taskdeck"] = [
            {
                "card_token": card.card_token,
                "task_id": card.task_id,
                "agent_id": card.agent_id,
                "kind": card.kind,
                "title": card.title,
                "state": card.state,
                "progress_summary": card.progress_summary,
                "terminal_at": card.terminal_at,
            }
            for card in cards
        ]
        return result

    async def list_agents(self) -> dict[str, Any]:
        self._require_capability("agents_list")
        self._require_capability("agents_current")
        await self._assert_owned_handle()
        await self._query_snapshot_topic("agents")
        await self._require_inbox().join()
        refresh = await self._database.fetchone(
            """
            SELECT applied_epoch FROM reconciliation_state
            WHERE sdk_session_id = ? AND topic = 'agents'
            """,
            (self.binding.sdk_session_id,),
        )
        rows = await self._database.fetchall(
            """
            SELECT agent_name, agent_id, display_name, description, source,
                   user_invocable, metadata_json, manifest_generation
            FROM runtime_agent_manifest
            WHERE sdk_session_id = ? AND state = 'available'
            ORDER BY agent_name
            """,
            (self.binding.sdk_session_id,),
        )
        current = await self._database.fetchone(
            "SELECT runtime_agent FROM session_bindings WHERE sdk_session_id = ?",
            (self.binding.sdk_session_id,),
        )
        return {
            "generation": 0 if refresh is None else int(refresh["applied_epoch"]),
            "agents": [
                {
                    **dict(row),
                    "metadata": json.loads(row["metadata_json"]),
                }
                for row in rows
            ],
            "current": "unknown" if current is None else str(current["runtime_agent"]),
        }

    async def current_agent(self) -> dict[str, Any]:
        listing = await self.list_agents()
        current = next(
            (agent for agent in listing["agents"] if agent["agent_name"] == listing["current"]),
            None,
        )
        return {
            "generation": listing["generation"],
            "name": listing["current"],
            "agent": current,
        }

    async def select_agent(
        self,
        name: str,
        *,
        idempotency_key: str,
    ) -> str:
        self._require_capability("agents_select")
        if not name.strip():
            raise ValueError("agent name cannot be empty")
        listing = await self.list_agents()
        candidate = next(
            (agent for agent in listing["agents"] if agent["agent_name"] == name),
            None,
        )
        if candidate is None:
            raise ValueError(f"runtime agent is unavailable: {name}")
        if candidate["user_invocable"] == 0:
            raise NativeCapabilityError(f"runtime agent is not user-invocable: {name}")
        return await self._change_agent(
            name,
            idempotency_key=idempotency_key,
        )

    async def deselect_agent(self, *, idempotency_key: str) -> str:
        self._require_capability("agents_deselect")
        return await self._change_agent(
            "default",
            idempotency_key=idempotency_key,
        )

    async def _change_agent(
        self,
        target: str,
        *,
        idempotency_key: str,
    ) -> str:
        blockers = await self.operational_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        transition_id = self._native_id("agent-transition", idempotency_key)
        operation_key = f"agent:{idempotency_key}"
        state = await self._database.fetchone(
            "SELECT runtime_agent FROM session_bindings WHERE sdk_session_id = ?",
            (self.binding.sdk_session_id,),
        )
        if state is None:
            raise SessionNotReady("selected-agent projection is unavailable")
        previous = str(state["runtime_agent"])

        async def dispatch() -> str:
            await self._assert_owned_handle()
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.agent_transition.pending",
                    "data": {
                        "transition_id": transition_id,
                        "operation_id": operation_id,
                        "previous_agent": previous,
                        "target_agent": target,
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"agent-transition:{transition_id}:pending",
            )
            claim = await self._database.fetchone(
                """
                SELECT runtime_agent, pending_agent, pending_agent_transition_id
                FROM session_bindings WHERE sdk_session_id = ?
                """,
                (self.binding.sdk_session_id,),
            )
            if (
                claim is None
                or claim["runtime_agent"] != previous
                or claim["pending_agent"] != target
                or claim["pending_agent_transition_id"] != transition_id
            ):
                raise OperationRejected(
                    "selected-agent transition lost its durable admission claim"
                )
            handle = self._require_handle()
            if target == "default":
                await self._sdk_call(self._bridge.deselect_agent(handle))
            else:
                await self._sdk_call(self._bridge.select_agent(handle, target))
            observed = await self._sdk_call(self._bridge.get_current_agent(handle))
            if observed != target:
                raise RuntimeError(f"agent reconciliation returned {observed}; expected {target}")
            return observed

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            observed = await self._require_mailbox().submit(
                kind="agent",
                idempotency_key=operation_key,
                input_payload={"target": target},
                operation=dispatch,
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.agent_transition.settled",
                        "data": {
                            "transition_id": transition_id,
                            "target_agent": target,
                            "state": failure_state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=(f"agent-transition:{transition_id}:{failure_state}"),
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.agent_transition.settled",
                "data": {
                    "transition_id": transition_id,
                    "target_agent": target,
                    "state": "confirmed",
                    "result": {"agent": observed},
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"agent-transition:{transition_id}:confirmed",
        )
        latest = await self._bindings.by_thread(self.binding.thread_id)
        if latest is not None:
            self.binding = latest
        return str(observed)

    async def runtime_schedules(
        self,
        *,
        kind: Literal["after", "every"] | None = None,
    ) -> list[dict[str, Any]]:
        self._require_capability("schedules_list")
        await self._assert_owned_handle()
        await self._query_snapshot_topic("schedules")
        await self._require_inbox().join()
        parameters: tuple[Any, ...] = (self.binding.sdk_session_id,)
        filter_sql = ""
        if kind is not None:
            filter_sql = " AND schedule_kind = ?"
            parameters += (kind,)
        rows = await self._database.fetchall(
            f"""
            SELECT * FROM runtime_schedules
            WHERE sdk_session_id = ?{filter_sql}
            ORDER BY CASE WHEN state IN ('active', 'unknown') THEN 0 ELSE 1 END,
                     next_run_at, runtime_schedule_id
            """,
            parameters,
        )
        return [dict(row) for row in rows]

    async def create_runtime_schedule(
        self,
        kind: Literal["after", "every"],
        expression: str,
        prompt: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_capability(f"builtin_{kind}")
        self._require_capability("schedules_list")
        if not expression.strip() or not prompt.strip():
            raise ValueError("schedule expression and prompt are required")
        action_id = self._native_id(
            "schedule-action",
            f"{kind}:{idempotency_key}",
        )
        invocation_key = f"schedule:{kind}:{idempotency_key}"
        invocation_id = self._native_id("command", invocation_key)
        invocation_input = f"{expression.strip()} {prompt.strip()}"
        input_hash = stable_hash(f"{kind}:{invocation_input}")

        async with self._native_schedule_lock:
            existing = await self._database.fetchone(
                """
                SELECT state, runtime_schedule_id, input_hash, baseline_json,
                       builtin_name
                FROM runtime_schedule_actions
                WHERE action_id = ? AND sdk_session_id = ?
                """,
                (action_id, self.binding.sdk_session_id),
            )
            if existing is not None and existing["input_hash"] != input_hash:
                raise ValueError("schedule idempotency key was reused with different input")
            if existing is not None and existing["builtin_name"] != kind:
                raise ValueError("schedule idempotency key was reused for another kind")
            if existing is None:
                before = {
                    str(item["runtime_schedule_id"])
                    for item in await self.runtime_schedules()
                    if item["state"] in {"active", "unknown"}
                }
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.schedule_action.pending",
                        "data": {
                            "action_id": action_id,
                            "builtin_name": kind,
                            "action": "create",
                            "input_hash": input_hash,
                            "baseline_ids": sorted(before),
                            "created_at": time.time(),
                        },
                    },
                    internal_event_id=f"schedule-action:{action_id}:pending",
                )
            else:
                before = set(json.loads(existing["baseline_json"] or "[]"))
                if existing["runtime_schedule_id"] is not None:
                    schedules = await self.runtime_schedules(kind=kind)
                    confirmed = next(
                        (
                            item
                            for item in schedules
                            if str(item["runtime_schedule_id"])
                            == str(existing["runtime_schedule_id"])
                        ),
                        None,
                    )
                    if confirmed is not None:
                        return confirmed

            async def find_created() -> list[dict[str, Any]]:
                deadline = asyncio.get_running_loop().time() + 10
                while True:
                    schedules = await self.runtime_schedules(kind=kind)
                    created = [
                        item
                        for item in schedules
                        if str(item["runtime_schedule_id"]) not in before
                        and item["state"] == "active"
                    ]
                    if created or asyncio.get_running_loop().time() >= deadline:
                        return created
                    await asyncio.sleep(0.25)

            if existing is not None:
                created = await find_created()
                if len(created) == 1:
                    await self._confirm_schedule_create(
                        action_id,
                        invocation_id,
                        kind,
                        created[0],
                    )
                    return created[0]
                if len(created) > 1:
                    raise NativeCapabilityError(f"{kind} schedule reconciliation is ambiguous")

            try:
                result = await self._invoke_command_operation(
                    kind,
                    invocation_input,
                    idempotency_key=invocation_key,
                    require_manifest=True,
                    require_quiet=True,
                )
            except OperationAmbiguous:
                created = await find_created()
                if len(created) == 1:
                    await self._confirm_schedule_create(
                        action_id,
                        invocation_id,
                        kind,
                        created[0],
                    )
                    return created[0]
                await self._settle_schedule_create_unknown(action_id)
                raise
            if result["kind"] != NativeCommandResultKind.COMPLETED.value:
                await self._settle_schedule_create_unknown(action_id)
                raise NativeCapabilityError(f"{kind} did not complete schedule creation directly")
            created = await find_created()
            if len(created) != 1:
                await self._settle_schedule_create_unknown(action_id)
                raise NativeCapabilityError(
                    f"{kind} invocation did not produce one identifiable runtime schedule"
                )
            await self._confirm_schedule_create(
                action_id,
                invocation_id,
                kind,
                created[0],
            )
            return created[0]

    async def _confirm_schedule_create(
        self,
        action_id: str,
        invocation_id: str,
        kind: Literal["after", "every"],
        schedule: dict[str, Any],
    ) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.schedule_action.settled",
                "data": {
                    "action_id": action_id,
                    "runtime_schedule_id": schedule["runtime_schedule_id"],
                    "invocation_id": invocation_id,
                    "action": "create",
                    "state": "confirmed",
                    "result": {
                        "kind": kind,
                        "runtime_schedule_id": schedule["runtime_schedule_id"],
                    },
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"schedule-action:{action_id}:confirmed",
        )

    async def _settle_schedule_create_unknown(self, action_id: str) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.schedule_action.settled",
                "data": {
                    "action_id": action_id,
                    "action": "create",
                    "state": "unknown",
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"schedule-action:{action_id}:unknown",
        )

    async def cancel_runtime_schedule(
        self,
        kind: Literal["after", "every"],
        schedule_id: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_capability("schedules_stop")
        schedules = await self.runtime_schedules(kind=kind)
        schedule = next(
            (item for item in schedules if str(item["runtime_schedule_id"]) == schedule_id),
            None,
        )
        if schedule is None:
            raise ValueError(f"{kind} schedule does not exist: {schedule_id}")
        if schedule["state"] not in {"active", "unknown"}:
            return schedule
        action_id = self._native_id(
            "schedule-action",
            f"{kind}:{idempotency_key}",
        )
        operation_key = f"schedule-stop:{idempotency_key}"

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            operation_id = await self._operation_id(operation_key)
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.schedule_action.pending",
                    "data": {
                        "action_id": action_id,
                        "operation_id": operation_id,
                        "runtime_schedule_id": schedule_id,
                        "builtin_name": kind,
                        "action": "cancel",
                        "input_hash": stable_hash(schedule_id),
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"schedule-action:{action_id}:pending",
            )
            stopped = await self._sdk_call(
                self._bridge.stop_native_schedule(
                    self._require_handle(),
                    int(schedule_id),
                )
            )
            if stopped is None:
                raise OperationRejected(
                    f"runtime did not confirm cancellation of schedule {schedule_id}"
                )
            if str(stopped.get("id")) != schedule_id:
                raise RuntimeError("schedule.stop returned a different runtime schedule")
            remaining = await self._sdk_call(
                self._bridge.get_native_schedules(self._require_handle())
            )
            if any(str(item.get("id")) == schedule_id for item in remaining):
                raise RuntimeError("schedule remained active after schedule.stop")
            return stopped

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            stopped = await self._require_mailbox().submit(
                kind="schedule-stop",
                idempotency_key=operation_key,
                input_payload={"kind": kind, "schedule_id": schedule_id},
                operation=dispatch,
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.schedule_action.settled",
                        "data": {
                            "action_id": action_id,
                            "runtime_schedule_id": schedule_id,
                            "action": "cancel",
                            "state": failure_state,
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=(f"schedule-action:{action_id}:{failure_state}"),
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.schedule_action.settled",
                "data": {
                    "action_id": action_id,
                    "runtime_schedule_id": schedule_id,
                    "action": "cancel",
                    "state": "confirmed",
                    "result": stopped,
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"schedule-action:{action_id}:confirmed",
        )
        await self._query_snapshot_topic("schedules")
        await self._require_inbox().join()
        row = await self._database.fetchone(
            """
            SELECT * FROM runtime_schedules
            WHERE sdk_session_id = ? AND runtime_schedule_id = ?
            """,
            (self.binding.sdk_session_id, schedule_id),
        )
        if row is None:
            raise NativeCapabilityError("cancelled schedule projection disappeared")
        return dict(row)

    async def remote_status(self) -> dict[str, Any]:
        self._require_capability("remote_status")
        await self._assert_owned_handle()
        prerequisites = await self._remote_preflight.status(
            self._require_handle(),
            str(self.binding.cwd_snapshot),
        )
        await self._query_snapshot_topic("remote")
        await self._require_inbox().join()
        row = await self._database.fetchone(
            """
            SELECT runtime_remote_mode, remote_url, remote_steerable,
                   remote_observed_at, pending_remote_target,
                   pending_remote_transition_id, remote_snapshot_json
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if row is None:
            raise SessionNotReady("remote session projection is unavailable")
        return {
            "mode": str(row["runtime_remote_mode"]),
            "url": row["remote_url"],
            "steerable": (
                None if row["remote_steerable"] is None else bool(row["remote_steerable"])
            ),
            "observed_at": row["remote_observed_at"],
            "pending_target": row["pending_remote_target"],
            "pending_transition_id": row["pending_remote_transition_id"],
            "snapshot": (
                {}
                if row["remote_snapshot_json"] is None
                else json.loads(row["remote_snapshot_json"])
            ),
            "auth": {
                "authenticated": prerequisites.authenticated,
                "type": prerequisites.auth_type,
                "host": prerequisites.auth_host,
            },
            "repository": {
                "root": prerequisites.repository_root,
                "host": prerequisites.repository_host,
                "has_origin": prerequisites.has_origin,
            },
        }

    async def set_remote(
        self,
        mode: NativeRemoteMode | Literal["off", "export", "on"],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        target = NativeRemoteMode(mode)
        if target == NativeRemoteMode.UNKNOWN:
            raise ValueError("remote mode cannot be set to unknown")
        self._require_capability(
            "remote_disable" if target == NativeRemoteMode.OFF else "remote_enable"
        )
        await self._assert_owned_handle()
        prerequisites = await self._remote_preflight.status(
            self._require_handle(),
            str(self.binding.cwd_snapshot),
        )
        current_row = await self._database.fetchone(
            """
            SELECT runtime_remote_mode, pending_remote_transition_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if current_row is None:
            raise SessionNotReady("remote session projection is unavailable")
        current = str(current_row["runtime_remote_mode"])
        if (
            current_row["pending_remote_transition_id"] is not None
            and target != NativeRemoteMode.OFF
        ):
            raise SessionNotReady("remote transition is already pending")
        if current == target.value:
            return await self.remote_status()
        if target in {NativeRemoteMode.ON, NativeRemoteMode.EXPORT}:
            if current != NativeRemoteMode.OFF.value:
                raise ValueError(f"remote {target.value} requires confirmed off state first")
            blockers = await self.operational_blockers()
        else:
            blockers = await self.runtime_drained_blockers()
        if blockers:
            raise DetachBlocked(blockers)
        transition_id = self._native_id("remote-transition", idempotency_key)
        operation_key = f"remote:{idempotency_key}"

        async def dispatch() -> dict[str, Any]:
            await self._assert_owned_handle()
            effective_prerequisites = (
                prerequisites
                if target == NativeRemoteMode.OFF
                else await self._remote_preflight.inspect(
                    self._require_handle(),
                    str(self.binding.cwd_snapshot),
                )
            )
            operation_id = await self._operation_id(operation_key)
            preflight = effective_prerequisites.to_dict()
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.remote_transition.pending",
                    "data": {
                        "transition_id": transition_id,
                        "operation_id": operation_id,
                        "previous_mode": current,
                        "target_mode": target.value,
                        "auth": {
                            "authenticated": preflight["authenticated"],
                            "auth_type": preflight["auth_type"],
                            "auth_host": preflight["auth_host"],
                        },
                        "repository": {
                            "root": preflight["repository_root"],
                            "host": preflight["repository_host"],
                            "has_origin": preflight["has_origin"],
                        },
                        "snapshot": preflight["snapshot"],
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"remote-transition:{transition_id}:pending",
            )
            claim = await self._database.fetchone(
                """
                SELECT runtime_remote_mode, pending_remote_target,
                       pending_remote_transition_id
                FROM session_bindings WHERE sdk_session_id = ?
                """,
                (self.binding.sdk_session_id,),
            )
            if (
                claim is None
                or claim["runtime_remote_mode"] != current
                or claim["pending_remote_target"] != target.value
                or claim["pending_remote_transition_id"] != transition_id
            ):
                raise OperationRejected("remote transition lost its durable admission claim")
            handle = self._require_handle()
            url: str | None = None
            if target == NativeRemoteMode.OFF:
                await self._sdk_call(self._bridge.disable_remote(handle))
            else:
                result = await self._sdk_call(
                    self._bridge.enable_remote(
                        handle,
                        cast(Literal["on", "export"], target.value),
                    )
                )
                steerable = bool(result.get("remoteSteerable"))
                if steerable != (target == NativeRemoteMode.ON):
                    raise RuntimeError(
                        "remote enable result contradicted the requested steerability"
                    )
                url = None if result.get("url") is None else str(result.get("url"))
            snapshot = await self._bridge.get_remote_state(handle)
            return {"mode": target.value, "url": url, "snapshot": snapshot}

        failure_state: Literal["rejected", "unknown"] | None = None
        try:
            transition = cast(
                dict[str, Any],
                await self._require_mailbox().submit(
                    kind="remote",
                    idempotency_key=operation_key,
                    input_payload={"target": target.value},
                    operation=dispatch,
                ),
            )
        except OperationRejected:
            failure_state = "rejected"
            raise
        except OperationAmbiguous:
            failure_state = "unknown"
            raise
        finally:
            if failure_state is not None:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.remote_transition.settled",
                        "data": {
                            "transition_id": transition_id,
                            "target_mode": target.value,
                            "state": failure_state,
                            "snapshot": {},
                            "settled_at": time.time(),
                        },
                    },
                    internal_event_id=(f"remote-transition:{transition_id}:{failure_state}"),
                )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.remote_transition.settled",
                "data": {
                    "transition_id": transition_id,
                    "target_mode": target.value,
                    "state": "confirmed",
                    "url": transition.get("url"),
                    "snapshot": transition["snapshot"],
                    "settled_at": time.time(),
                },
            },
            internal_event_id=f"remote-transition:{transition_id}:confirmed",
        )
        latest = await self._bindings.by_thread(self.binding.thread_id)
        if latest is not None:
            self.binding = latest
        return await self.remote_status()

    async def _handle_user_input_request(
        self,
        request: dict[str, Any],
        _context: dict[str, str],
    ) -> dict[str, Any]:
        result = await self._request_interaction("user_input", request)
        return cast(dict[str, Any], result)

    async def _handle_exit_plan_mode_request(
        self,
        request: dict[str, Any],
        _context: dict[str, str],
    ) -> dict[str, Any]:
        result = await self._request_interaction("exit_plan_mode", request)
        return cast(dict[str, Any], result)

    async def _handle_auto_mode_switch_request(
        self,
        request: dict[str, Any],
        _context: dict[str, str],
    ) -> str:
        result = await self._request_interaction("auto_mode_switch", request)
        return cast(str, result)

    async def _handle_elicitation_request(
        self,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        result = await self._request_interaction(
            "elicitation",
            context,
            response_plane="sdk_handler",
        )
        return cast(dict[str, Any], result)

    async def _handle_mcp_auth_request(
        self,
        request: dict[str, Any],
        _context: dict[str, str],
    ) -> dict[str, Any]:
        authorizer = self._oauth_authorizer
        automatic_response = None if authorizer is None else authorizer(request)
        result = await self._request_interaction(
            "mcp_oauth",
            request,
            protocol_request_id=(
                None if request.get("requestId") is None else str(request["requestId"])
            ),
            response_plane="sdk_handler",
            automatic_response=automatic_response,
        )
        return cast(dict[str, Any], result)

    async def _request_interaction(
        self,
        kind: InteractionKind,
        request: dict[str, Any],
        *,
        protocol_request_id: str | None = None,
        response_plane: str = "direct_handler",
        automatic_response: Awaitable[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any] | str:
        await self._assert_owned_handle()
        return await self._require_interaction_gateway().request(
            kind,
            request,
            protocol_request_id=protocol_request_id,
            response_plane=response_plane,
            automatic_response=automatic_response,
        )

    async def respond_interaction(
        self,
        interaction_id: str,
        *,
        selection: int | None = None,
        freeform: str | None = None,
        form_content: Mapping[str, Any] | None = None,
        action: Literal["decline", "cancel"] | None = None,
        secure_response: Mapping[str, Any] | None = None,
    ) -> Literal["resolved", "expired", "invalid"]:
        return await self._require_interaction_gateway().respond(
            interaction_id,
            selection=selection,
            freeform=freeform,
            form_content=form_content,
            action=action,
            secure_response=secure_response,
        )

    async def cancel_pending_interactions(self, *, reason: str) -> int:
        gateway = self._interaction_gateway
        if gateway is None:
            return 0
        return await gateway.cancel_pending(reason=reason)

    async def queue_items(self) -> list[dict[str, Any]]:
        rows = await self._database.fetchall(
            """
            SELECT q.id, q.prompt, q.position, q.state, q.created_at,
                   q.schedule_run_id, q.replaces_id, s.origin,
                   s.runtime_schedule_id
            FROM message_queue AS q
            JOIN submissions AS s ON s.submission_id = q.id
            WHERE q.thread_id = ?
              AND q.state NOT IN ('cancelled', 'submitted', 'failed')
            ORDER BY q.position
            """,
            (self.binding.thread_id,),
        )
        return [dict(row) for row in rows]

    async def dispatch_queued_once(self) -> tuple[str, str] | None:
        return await self._dispatch_next_queued()

    async def cancel_queue_item(self, submission_id: str) -> bool:
        async with self._queue_dispatch_lock:
            return bool(
                await self._cancel_queued(
                    "id = ?",
                    (submission_id,),
                )
            )

    async def clear_queue(self) -> int:
        async with self._queue_dispatch_lock:
            return await self._cancel_queued("1 = 1", ())

    async def resubmit_queue_item(
        self,
        submission_id: str,
        *,
        idempotency_key: str,
    ) -> str:
        allowed = {
            "blocked_mode_drift",
            "blocked_model_drift",
            "blocked_agent_drift",
            "blocked_session_config_drift",
        }
        return await self._replace_queue_item(
            submission_id,
            prompt=None,
            idempotency_key=idempotency_key,
            allowed_states=allowed,
        )

    async def update_queue_item(
        self,
        submission_id: str,
        *,
        prompt: str,
        idempotency_key: str,
    ) -> str:
        if not prompt.strip():
            raise ValueError("queue prompt cannot be empty")
        allowed = {
            "local_queued",
            "blocked_config_unknown",
            "blocked_remote_transition",
            "blocked_mode_drift",
            "blocked_model_drift",
            "blocked_agent_drift",
            "blocked_session_config_drift",
        }
        return await self._replace_queue_item(
            submission_id,
            prompt=prompt,
            idempotency_key=idempotency_key,
            allowed_states=allowed,
        )

    async def _replace_queue_item(
        self,
        submission_id: str,
        *,
        prompt: str | None,
        idempotency_key: str,
        allowed_states: set[str],
    ) -> str:
        async with self._queue_dispatch_lock:
            await self._assert_dispatchable()
            await self._assert_resubmit_write_fence()
            row = await self._database.fetchone(
                """
                SELECT q.*, s.prompt_hash, s.origin
                FROM message_queue q
                JOIN submissions s ON s.submission_id = q.id
                WHERE q.id = ? AND q.thread_id = ?
                """,
                (submission_id, self.binding.thread_id),
            )
            if row is None or str(row["state"]) not in allowed_states:
                raise ValueError("queue item is not replaceable in its current state")
            binding = await self._database.fetchone(
                """
                SELECT desired_mode, desired_model_config, desired_agent,
                       desired_project_config_version
                FROM session_bindings WHERE thread_id = ?
                """,
                (self.binding.thread_id,),
            )
            if binding is None:
                raise SessionNotReady("session execution configuration is unavailable")
            replacement_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:{self.binding.sdk_session_id}:queue-replace:"
                    f"{submission_id}:{idempotency_key}",
                )
            )
            replacement_prompt = str(row["prompt"]) if prompt is None else prompt
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.queue.replaced",
                    "data": {
                        "old_submission_id": submission_id,
                        "new_submission_id": replacement_id,
                        "prompt": replacement_prompt,
                        "prompt_hash": hashlib.sha256(replacement_prompt.encode()).hexdigest(),
                        "allowed_states": sorted(allowed_states),
                        "requested_mode": str(binding["desired_mode"]),
                        "requested_model_config": json.loads(str(binding["desired_model_config"])),
                        "requested_agent": str(binding["desired_agent"]),
                        "requested_session_config_version": int(
                            binding["desired_project_config_version"]
                        ),
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"queue:{replacement_id}:replaced",
            )
            replacement = await self._database.fetchone(
                "SELECT id FROM message_queue WHERE id = ?",
                (replacement_id,),
            )
            if replacement is None:
                await self._assert_resubmit_write_fence()
                raise RuntimeError("queue replacement was not persisted")
            return replacement_id

    async def _assert_resubmit_write_fence(self) -> None:
        lease = self._lease
        binding_fence = self.binding.owner_fence_token
        if (
            lease is None
            or binding_fence is None
            or lease.sdk_session_id != self.binding.sdk_session_id
        ):
            raise FenceLost("queue resubmission has no current owner fence")
        current = await self._database.fetchone(
            """
            SELECT 1
            FROM session_owner_leases AS owner
            JOIN session_bindings AS binding
              ON binding.sdk_session_id = owner.sdk_session_id
            WHERE owner.sdk_session_id = ?
              AND owner.owner_id = ?
              AND owner.fence_token = ?
              AND owner.expires_at >= ?
              AND binding.thread_id = ?
              AND binding.runtime_generation = ?
              AND binding.owner_fence_token = ?
              AND binding.owner_fence_token = owner.fence_token
              AND binding.binding_intent = 'active'
              AND binding.attachment_state = 'attached'
            """,
            (
                self.binding.sdk_session_id,
                lease.owner_id,
                lease.fence_token,
                time.time() + MUTATION_HEADROOM_SECONDS,
                self.binding.thread_id,
                self.binding.runtime_generation,
                binding_fence,
            ),
        )
        if current is None:
            raise FenceLost(
                "owner fence, runtime generation, or mutation headroom "
                "changed before queue resubmission"
            )

    async def update_taskdeck_view(
        self,
        *,
        panel_id: str,
        expected_revision: int,
        action: Literal["select", "toggle", "previous", "next"],
        card_token: str | None,
        message_id: str,
        interaction_id: str,
    ) -> Literal["updated", "stale", "invalid"]:
        mapping = await self._database.fetchone(
            """
            SELECT discord_message_id FROM render_messages
            WHERE session_id = ? AND logical_key = 'taskdeck'
            """,
            (self.binding.sdk_session_id,),
        )
        if mapping is None or str(mapping["discord_message_id"]) != message_id:
            return "invalid"
        cards = await self._database.fetchall(
            """
            SELECT card_token, revision FROM task_card_projections
            WHERE sdk_session_id = ? AND panel_id = ?
            ORDER BY
              CASE WHEN terminal_at IS NULL THEN 0 ELSE 1 END,
              first_seen_at, card_key
            """,
            (self.binding.sdk_session_id, panel_id),
        )
        if not cards:
            return "invalid"
        revision = sum(int(card["revision"]) for card in cards)
        if revision != expected_revision:
            return "stale"
        state = await self._database.fetchone(
            """
            SELECT selected_card_token, page, expanded
            FROM taskdeck_panel_state WHERE sdk_session_id = ? AND panel_id = ?
            """,
            (self.binding.sdk_session_id, panel_id),
        )
        tokens = [str(card["card_token"]) for card in cards]
        selected = (
            tokens[0]
            if state is None or state["selected_card_token"] not in tokens
            else str(state["selected_card_token"])
        )
        page_count = max(1, (len(tokens) + 7) // 8)
        page = 0 if state is None else min(max(int(state["page"]), 0), page_count - 1)
        expanded = bool(state is not None and state["expanded"])
        if action == "select":
            if card_token not in tokens:
                return "invalid"
            selected = str(card_token)
            page = tokens.index(selected) // 8
            expanded = False
        elif action == "toggle":
            expanded = not expanded
        elif action == "previous":
            page = max(0, page - 1)
            selected = tokens[page * 8]
            expanded = False
        elif action == "next":
            page = min(page_count - 1, page + 1)
            selected = tokens[page * 8]
            expanded = False
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.taskdeck.view_changed",
                "data": {
                    "panel_id": panel_id,
                    "selected_card_token": selected,
                    "page": page,
                    "expanded": expanded,
                },
            },
            internal_event_id=f"taskdeck-view:{interaction_id}",
        )
        return "updated"

    async def perform_taskdeck_action(
        self,
        *,
        panel_id: str,
        card_token: str,
        expected_revision: int,
        action: Literal["cancel", "promote", "message", "remove", "download"],
        message_id: str,
        interaction_id: str,
        message: str | None = None,
    ) -> dict[str, Any]:
        mapping = await self._database.fetchone(
            """
            SELECT discord_message_id FROM render_messages
            WHERE session_id = ? AND logical_key = 'taskdeck'
            """,
            (self.binding.sdk_session_id,),
        )
        if mapping is None or str(mapping["discord_message_id"]) != message_id:
            return {"status": "invalid"}
        cards = await self._database.fetchall(
            """
            SELECT card_token, task_id, agent_id, kind, state, can_promote,
                   detail_artifact, revision
            FROM task_card_projections
            WHERE sdk_session_id = ? AND panel_id = ?
            ORDER BY first_seen_at, card_key
            """,
            (self.binding.sdk_session_id, panel_id),
        )
        if not cards:
            return {"status": "invalid"}
        revision = sum(int(card["revision"]) for card in cards)
        if revision != expected_revision:
            await self.refresh_taskdeck(interaction_id=f"{interaction_id}:stale")
            return {"status": "stale"}
        card = next(
            (candidate for candidate in cards if str(candidate["card_token"]) == card_token),
            None,
        )
        if card is None:
            return {"status": "invalid"}
        state = str(card["state"])
        task_id = None if card["task_id"] is None else str(card["task_id"])
        if action == "download":
            artifact = card["detail_artifact"]
            if artifact is None:
                return {"status": "invalid"}
            return {
                "status": "download",
                "filename": f"task-{card_token}-detail.md",
                "content": str(artifact),
            }
        if self._task_action_adapter is None:
            raise CDCapabilityError("native task actions are not available")
        if task_id is None:
            raise CDCapabilityError("this TaskDeck card has no addressable task ID")
        operation_key = f"task-action:{interaction_id}:{action}:{task_id}"

        async def dispatch() -> Mapping[str, Any]:
            await self._assert_owned_handle()
            if action == "cancel":
                return await self._task_action_adapter.cancel_task(
                    session_id=self.binding.sdk_session_id,
                    task_id=task_id,
                )
            if action == "promote":
                return await self._task_action_adapter.promote_task(
                    session_id=self.binding.sdk_session_id,
                    task_id=task_id,
                )
            if action == "message":
                return await self._task_action_adapter.message_task(
                    session_id=self.binding.sdk_session_id,
                    task_id=task_id,
                    message=normalized_message,
                )
            return await self._task_action_adapter.remove_task(
                session_id=self.binding.sdk_session_id,
                task_id=task_id,
            )

        normalized_message = ""
        if action == "cancel":
            if state not in {"running", "idle"}:
                raise CDSessionStateError("only running or idle tasks can be cancelled")
        elif action == "promote":
            if state not in {"running", "idle"} or not bool(card["can_promote"]):
                raise CDSessionStateError("this task cannot be promoted")
        elif action == "message":
            if str(card["kind"]) != "agent" or state not in {"running", "idle"}:
                raise CDSessionStateError("only active agent tasks accept messages")
            normalized_message = "" if message is None else message.strip()
            if not normalized_message:
                raise CDInputError("task message cannot be empty")
        else:
            if state not in {"completed", "failed", "cancelled"}:
                raise CDSessionStateError("only terminal tasks can be removed")
        if not await self._is_current_owner():
            raise OperationAmbiguous(f"owner fence lost before task action {operation_key}")
        result = await self._require_mailbox().submit(
            kind=f"task_{action}",
            idempotency_key=operation_key,
            input_payload={
                "action": action,
                "task_id": task_id,
                "message": normalized_message or None,
            },
            operation=dispatch,
        )
        if action == "remove":
            await self._database.execute(
                """
                DELETE FROM task_card_projections
                WHERE sdk_session_id = ? AND panel_id = ? AND card_token = ?
                """,
                (self.binding.sdk_session_id, panel_id, card_token),
            )
        await self.refresh_taskdeck(interaction_id=interaction_id)
        return {"status": "updated", "result": dict(result or {})}

    async def refresh_taskdeck(self, *, interaction_id: str) -> None:
        state = await self._database.fetchone(
            """
            SELECT panel_id, selected_card_token, page, expanded
            FROM taskdeck_panel_state WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if state is None:
            cards = await self._database.fetchone(
                """
                SELECT panel_id, card_token FROM task_card_projections
                WHERE sdk_session_id = ?
                ORDER BY first_seen_at, card_key LIMIT 1
                """,
                (self.binding.sdk_session_id,),
            )
            if cards is None:
                return
            data = {
                "panel_id": str(cards["panel_id"]),
                "selected_card_token": str(cards["card_token"]),
                "page": 0,
                "expanded": False,
            }
        else:
            data = {
                "panel_id": str(state["panel_id"]),
                "selected_card_token": state["selected_card_token"],
                "page": int(state["page"]),
                "expanded": bool(state["expanded"]),
            }
        await self._require_inbox().commit_internal(
            {"type": "copilotd.taskdeck.view_changed", "data": data},
            internal_event_id=f"taskdeck-refresh:{interaction_id}",
        )

    async def _cancel_queued(
        self,
        predicate: str,
        parameters: tuple[Any, ...],
    ) -> int:
        await self._assert_owned_handle()
        now = time.time()
        cancellable = (
            "local_queued",
            "blocked_config_unknown",
            "blocked_remote_transition",
            "blocked_mode_drift",
            "blocked_model_drift",
            "blocked_agent_drift",
            "blocked_session_config_drift",
            "blocked_attachment_unavailable",
            "blocked_attachment_manifest_missing",
        )
        placeholders = ", ".join("?" for _ in cancellable)
        rows = await self._database.fetchall(
            f"""
            SELECT id FROM message_queue
            WHERE thread_id = ? AND {predicate}
              AND state IN ({placeholders})
            """,
            (self.binding.thread_id, *parameters, *cancellable),
        )
        submission_ids = [str(row["id"]) for row in rows]
        if not submission_ids:
            return 0
        cancellation_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self.binding.sdk_session_id}:cancel:{','.join(submission_ids)}",
            )
        )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.submission.cancel_queued",
                "data": {
                    "submission_ids": submission_ids,
                    "cancellable_states": list(cancellable),
                    "cancelled_at": now,
                },
            },
            internal_event_id=f"submissions:{cancellation_id}:cancelled",
        )
        for submission_id in submission_ids:
            self._volatile_attachments.pop(submission_id, None)
        ids = ", ".join("?" for _ in submission_ids)
        cancelled = await self._database.fetchone(
            f"""
            SELECT COUNT(*) FROM submissions
            WHERE submission_id IN ({ids}) AND state = 'cancelled'
            """,
            tuple(submission_ids),
        )
        return 0 if cancelled is None else int(cancelled[0])

    async def abort(self, *, idempotency_key: str) -> None:
        await self._assert_owned_handle()
        operation_key = f"abort:{idempotency_key}"
        existing = await self._database.fetchone(
            """
            SELECT state FROM session_operations
            WHERE sdk_session_id = ? AND idempotency_key = ? AND kind = 'abort'
            """,
            (self.binding.sdk_session_id, operation_key),
        )
        candidate = await self._database.fetchone(
            """
            SELECT submission_id, state, abort_event_id FROM submissions
            WHERE sdk_session_id = ?
              AND state IN (
                'submitted', 'observed_active', 'continuation_expected',
                'observed_aborted'
              )
            ORDER BY COALESCE(observed_at, created_at) DESC, created_at DESC
            LIMIT 1
            """,
            (self.binding.sdk_session_id,),
        )
        if existing is None:
            snapshot = await self._refresh_readiness()
            if snapshot is None or not bool(snapshot.get("abortable")):
                raise SessionNotReady("fresh runtime activity does not report abortable work")
            if not bool(snapshot.get("processing") or snapshot.get("hasActiveWork")):
                raise SessionNotReady("fresh runtime activity does not report current work")
            if candidate is None or str(candidate["state"]) == "observed_aborted":
                raise SessionNotReady("no current submission correlates with the abortable work")
        if candidate is None:
            raise SessionNotReady("abort evidence has no correlated submission")
        submission_id = str(candidate["submission_id"])
        if (
            existing is not None
            and existing["state"] == "confirmed"
            and candidate["abort_event_id"] is not None
            and candidate["state"] == "observed_aborted"
        ):
            return
        await self.cancel_pending_interactions(reason="Cancelled by session abort.")
        mailbox = self._require_mailbox()

        async def dispatch() -> None:
            await self._assert_owned_handle()
            await self._sdk_call(self._require_handle().abort())

        await mailbox.submit(
            kind="abort",
            idempotency_key=operation_key,
            input_payload={"submission_id": submission_id},
            operation=dispatch,
        )
        deadline = asyncio.get_running_loop().time() + self._abort_evidence_timeout_seconds
        while True:
            evidence = await self._database.fetchone(
                """
                SELECT state, abort_event_id FROM submissions
                WHERE submission_id = ? AND sdk_session_id = ?
                """,
                (submission_id, self.binding.sdk_session_id),
            )
            if (
                evidence is not None
                and evidence["abort_event_id"] is not None
                and evidence["state"] == "observed_aborted"
            ):
                return
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise OperationAmbiguous(
                    "abort RPC returned but correlated abort + idle(aborted=true) "
                    "evidence was not observed"
                )
            await asyncio.sleep(min(0.05, remaining))

    async def close(
        self,
        *,
        idempotency_key: str,
        force: bool = False,
    ) -> None:
        async with self._lifecycle_lock:
            await self._assert_owned_handle()
            mailbox = self._require_mailbox()
            async with self._admission_lock:
                self._accepting_sends = False
            if not force:
                await self._send_admissions_drained.wait()
            try:
                if not force:
                    await mailbox.freeze_and_drain()
                blockers = await self.detach_blockers()
            except BaseException:
                async with self._admission_lock:
                    self._accepting_sends = True
                if not force:
                    mailbox.thaw()
                raise
            if blockers and not force:
                async with self._admission_lock:
                    self._accepting_sends = True
                mailbox.thaw()
                raise DetachBlocked(blockers)
            if force:
                await self._force_close_teardown(idempotency_key=idempotency_key)
            if force:
                await mailbox.freeze_and_drain()

            inbox = self._require_inbox()
            await inbox.join()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if current is None:
                raise SessionNotReady("session binding disappeared before close")
            self.binding = await self._bindings.begin_close(current)
            self.state = RuntimeState.CLOSING

            async def disconnect() -> None:
                await self._assert_owned_handle(allow_closing=True)
                await self._sdk_call(self._require_handle().disconnect())

            succeeded = False
            try:
                await mailbox.submit(
                    kind="close",
                    idempotency_key=f"close:{idempotency_key}",
                    input_payload={"force": force},
                    operation=disconnect,
                    allow_when_frozen=True,
                )
                succeeded = True
            finally:
                latest = await self._bindings.by_thread(self.binding.thread_id)
                if latest is not None:
                    self.binding = await self._bindings.finish_disconnect(
                        latest,
                        succeeded=succeeded,
                    )
                await self._stop_components(release_owner=True)
                self.state = RuntimeState.CLOSED if succeeded else RuntimeState.RECOVERY_UNKNOWN

    async def _force_close_teardown(self, *, idempotency_key: str) -> None:
        unknown: set[str] = set()
        await self.clear_queue()
        await self.cancel_pending_interactions(reason="Cancelled by forced session close.")
        try:
            async with asyncio.timeout(15):
                readiness: dict[str, Any] | None = None
                try:
                    readiness = await self._refresh_readiness()
                except Exception:
                    unknown.update({"activity", "native_queue"})

                if readiness is not None and bool(readiness.get("abortable")):
                    try:
                        await self.abort(idempotency_key=f"{idempotency_key}:force")
                    except (OperationAmbiguous, OperationRejected, SessionNotReady):
                        unknown.add("abort")

                if self._supports_capability("native_queue_snapshot") and hasattr(
                    self._bridge,
                    "clear_native_queue",
                ):
                    try:
                        await self._force_mailbox_call(
                            kind="force-native-queue-clear",
                            idempotency_key=f"force-native-queue:{idempotency_key}",
                            input_payload={},
                            operation=lambda: self._sdk_call(
                                self._bridge.clear_native_queue(self._require_handle())
                            ),
                        )
                        queue = await self._refresh_readiness()
                        if (
                            queue is None
                            or queue.get("pendingItems")
                            or queue.get("steeringMessages")
                        ):
                            unknown.add("native_queue")
                    except Exception:
                        unknown.add("native_queue")
                else:
                    unknown.add("native_queue")

                if self._supports_capability("tasks_list") and self._supports_capability(
                    "tasks_cancel"
                ):
                    try:
                        tasks = await self._sdk_call(self._bridge.get_tasks(self._require_handle()))
                    except Exception:
                        unknown.add("tasks")
                    else:
                        for task in tasks:
                            state = str(task.get("status", "")).lower()
                            if state in TERMINAL_TASK_STATES:
                                continue
                            task_id = task.get("id")
                            if task_id is None:
                                unknown.add("tasks")
                                continue
                            try:
                                cancelled = await self._force_mailbox_call(
                                    kind="force-task-cancel",
                                    idempotency_key=(f"force-task:{idempotency_key}:{task_id}"),
                                    input_payload={"task_id": str(task_id)},
                                    operation=lambda task_id=str(task_id): self._sdk_call(
                                        self._bridge.cancel_task(
                                            self._require_handle(),
                                            task_id,
                                        )
                                    ),
                                )
                                if not cancelled:
                                    unknown.add("tasks")
                            except Exception:
                                unknown.add("tasks")
                        try:
                            remaining = await self._sdk_call(
                                self._bridge.get_tasks(self._require_handle())
                            )
                            if any(
                                str(task.get("status", "")).lower() not in TERMINAL_TASK_STATES
                                for task in remaining
                            ):
                                unknown.add("tasks")
                            await self._require_inbox().commit_internal(
                                {
                                    "type": "copilotd.tasks.snapshot",
                                    "data": {
                                        "tasks": remaining,
                                        "observed_at": time.time(),
                                    },
                                },
                                source="snapshot",
                                internal_event_id=(f"force-close:{idempotency_key}:tasks:snapshot"),
                            )
                        except Exception:
                            unknown.add("tasks")
                else:
                    unknown.add("tasks")

                if self._supports_capability("schedules_list") and self._supports_capability(
                    "schedules_stop"
                ):
                    try:
                        schedules = await self._sdk_call(
                            self._bridge.get_native_schedules(self._require_handle())
                        )
                    except Exception:
                        unknown.add("schedules")
                    else:
                        for schedule in schedules:
                            schedule_id = schedule.get("id")
                            if schedule_id is None:
                                unknown.add("schedules")
                                continue
                            try:
                                stopped = await self._force_mailbox_call(
                                    kind="force-schedule-stop",
                                    idempotency_key=(
                                        f"force-schedule:{idempotency_key}:{schedule_id}"
                                    ),
                                    input_payload={"schedule_id": int(schedule_id)},
                                    operation=lambda schedule_id=int(schedule_id): self._sdk_call(
                                        self._bridge.stop_native_schedule(
                                            self._require_handle(),
                                            schedule_id,
                                        )
                                    ),
                                )
                                if stopped is None:
                                    unknown.add("schedules")
                            except Exception:
                                unknown.add("schedules")
                        try:
                            remaining_schedules = await self._sdk_call(
                                self._bridge.get_native_schedules(self._require_handle())
                            )
                            if remaining_schedules:
                                unknown.add("schedules")
                            await self._query_snapshot_topic("schedules")
                        except Exception:
                            unknown.add("schedules")
                else:
                    unknown.add("schedules")

                if self._supports_capability("remote_disable"):
                    try:
                        await self._force_mailbox_call(
                            kind="force-remote-disable",
                            idempotency_key=f"force-remote:{idempotency_key}",
                            input_payload={"target": "off"},
                            operation=lambda: self._sdk_call(
                                self._bridge.disable_remote(self._require_handle())
                            ),
                        )
                        snapshot = await self._sdk_call(
                            self._bridge.get_remote_state(self._require_handle())
                        )
                        await self._require_inbox().commit_internal(
                            {
                                "type": "copilotd.remote.observed",
                                "data": {
                                    "mode": "off",
                                    "steerable": False,
                                    "snapshot": snapshot,
                                    "clear_pending": True,
                                    "observed_at": time.time(),
                                },
                            },
                            internal_event_id=(f"force-close:{idempotency_key}:remote:off"),
                        )
                    except Exception:
                        unknown.add("remote")
                else:
                    unknown.add("remote")
        except TimeoutError:
            unknown.update(
                {
                    "activity",
                    "abort",
                    "native_queue",
                    "tasks",
                    "schedules",
                    "remote",
                }
            )

        if self._inbox is not None and self._reducer is not None:
            if unknown:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.force_teardown.unknown",
                        "data": {
                            "unknown": sorted(unknown),
                            "observed_at": time.time(),
                        },
                    },
                    internal_event_id=(
                        f"force-close:{self._native_id('teardown', idempotency_key)}:unknown"
                    ),
                )
            await self._force_active_unknown()
            await self._require_inbox().join()

    async def _force_mailbox_call(
        self,
        *,
        kind: str,
        idempotency_key: str,
        input_payload: dict[str, Any],
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        return await self._require_mailbox().submit(
            kind=kind,
            idempotency_key=idempotency_key,
            input_payload=input_payload,
            operation=operation,
        )

    def _supports_capability(self, capability: str) -> bool:
        return self._capabilities is None or self._capabilities.supports(capability)

    async def detach_blockers(self) -> list[str]:
        refresh_error: Exception | None = None
        if self.state == RuntimeState.READY:
            try:
                await self._refresh_all_snapshots()
            except Exception as error:
                refresh_error = error
        binding = await self._bindings.by_thread(self.binding.thread_id)
        if binding is None:
            return ["binding_missing"]
        blockers: list[str] = []
        if refresh_error is not None:
            blockers.append(f"snapshot_refresh_failed:{type(refresh_error).__name__}")
        active = await self._database.fetchone(
            """
            SELECT COUNT(*) FROM liveness_leases
            WHERE sdk_session_id = ? AND state = 'active'
              AND runtime_generation = ? AND owner_fence_token = ?
            """,
            (
                binding.sdk_session_id,
                binding.runtime_generation,
                binding.owner_fence_token,
            ),
        )
        if active[0]:
            blockers.append(f"active_liveness:{active[0]}")
        queued = await self._database.fetchone(
            """
            SELECT COUNT(*) FROM message_queue
            WHERE thread_id = ? AND state NOT IN (
              'cancelled', 'submitted', 'submitted_unknown', 'failed'
            )
            """,
            (binding.thread_id,),
        )
        if queued[0]:
            blockers.append(f"local_queue:{queued[0]}")
        interactions = await self._database.fetchone(
            """
            SELECT COUNT(*) FROM pending_interactions
            WHERE sdk_session_id = ? AND state = 'pending'
            """,
            (binding.sdk_session_id,),
        )
        if interactions[0]:
            blockers.append(f"pending_interactions:{interactions[0]}")
        schedules = await self._database.fetchone(
            """
            SELECT COUNT(*) FROM runtime_schedules
            WHERE sdk_session_id = ? AND state IN ('active', 'unknown')
            """,
            (binding.sdk_session_id,),
        )
        if schedules[0]:
            blockers.append(f"runtime_schedules:{schedules[0]}")
        for blocker in await self._readiness_blockers(require_quiet=True):
            if blocker not in blockers:
                blockers.append(blocker)
        return blockers

    async def operational_blockers(self) -> list[str]:
        return [
            blocker
            for blocker in await self.detach_blockers()
            if not blocker.startswith("runtime_schedules:")
        ]

    async def runtime_drained_blockers(self) -> list[str]:
        return [
            blocker
            for blocker in await self.operational_blockers()
            if not blocker.startswith("remote_mode:")
            and blocker
            not in {
                "remote_transition_pending",
                "runtime_remote_unknown",
            }
        ]

    def _require_capability(self, capability: str) -> None:
        if self._capabilities is not None and not self._capabilities.supports(capability):
            raise NativeCapabilityError(f"runtime capability is not verified: {capability}")

    async def _operation_id(self, idempotency_key: str) -> str:
        row = await self._database.fetchone(
            """
            SELECT operation_id FROM session_operations
            WHERE sdk_session_id = ? AND idempotency_key = ?
            """,
            (self.binding.sdk_session_id, idempotency_key),
        )
        if row is None:
            raise RuntimeError(f"native operation has no durable envelope: {idempotency_key}")
        return str(row["operation_id"])

    def _native_id(self, kind: str, key: str) -> str:
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self.binding.sdk_session_id}:{kind}:{key}",
            )
        )

    async def _refresh_all_snapshots(self) -> None:
        # Flush callback-scheduled snapshot requests before selecting the latest epochs.
        await asyncio.sleep(0)
        await self._require_inbox().join()
        topics = self._supported_snapshot_topics()
        if "tasks" in topics:
            await self._query_snapshot_topic("tasks")
        await self._refresh_readiness()
        for topic in sorted(topics - {"activity", "queue", "tasks"}):
            await self._query_snapshot_topic(topic)
        await self._require_inbox().join()

    async def shutdown(self, *, emergency: bool = False) -> None:
        """Stop app workers without claiming that an in-flight SDK operation succeeded."""
        async with self._lifecycle_lock:
            if self.state == RuntimeState.CLOSED:
                return
            fenced_shutdown = self.state == RuntimeState.FENCED
            emergency = emergency or fenced_shutdown
            async with self._admission_lock:
                self._accepting_sends = False
            if self._mailbox is not None:
                self._mailbox.freeze()
                if not emergency:
                    await self._mailbox.drain(timeout_seconds=self._shutdown_timeout_seconds)
                    if self._inbox is not None:
                        await self._inbox.join()
            if not emergency and self._inbox is not None and self._reducer is not None:
                await self._force_active_unknown()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if (
                not fenced_shutdown
                and current is not None
                and current.runtime_generation == self.binding.runtime_generation
                and current.owner_fence_token == self.binding.owner_fence_token
                and current.attachment_state
                in {
                    AttachmentState.CREATING,
                    AttachmentState.RESUMING,
                    AttachmentState.ATTACHED,
                    AttachmentState.DISCONNECTING,
                }
            ):
                self.binding = await self._bindings.mark_recovery_unknown(current)
                self.state = RuntimeState.RECOVERY_UNKNOWN
            await self._stop_components(
                release_owner=True,
                emergency=emergency,
            )
            if self.state not in {RuntimeState.RECOVERY_UNKNOWN, RuntimeState.FENCED}:
                self.state = RuntimeState.DETACHED

    async def _reconcile_agent_after_attach(
        self,
        handle: SessionHandle,
    ) -> None:
        observed_agent = await self._sdk_call(self._bridge.get_current_agent(handle))
        row = await self._database.fetchone(
            """
            SELECT desired_agent, pending_agent, pending_agent_transition_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if row is None:
            raise SessionNotReady("selected-agent state disappeared during attach")
        transition_id = row["pending_agent_transition_id"]
        target = row["pending_agent"]
        transition = (
            None
            if transition_id is None
            else await self._database.fetchone(
                """
                SELECT transition_id, previous_agent, target_agent
                FROM runtime_agent_transitions
                WHERE transition_id = ? AND sdk_session_id = ?
                  AND state IN ('pending', 'unknown')
                """,
                (str(transition_id), self.binding.sdk_session_id),
            )
        )
        if transition is None:
            transition = await self._database.fetchone(
                """
                SELECT transition_id, previous_agent, target_agent
                FROM runtime_agent_transitions
                WHERE sdk_session_id = ? AND state IN ('pending', 'unknown')
                ORDER BY created_at DESC LIMIT 1
                """,
                (self.binding.sdk_session_id,),
            )
        if transition is None and (transition_id is None or target is None):
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.agent.observed",
                    "data": {
                        "agent": observed_agent,
                        "observed_at": time.time(),
                    },
                },
                internal_event_id=(
                    f"agent:{self.binding.runtime_generation}:initial:"
                    f"{hashlib.sha256(observed_agent.encode()).hexdigest()[:16]}"
                ),
            )
            return

        if transition is not None:
            transition_id = str(transition["transition_id"])
        previous = (
            str(row["desired_agent"]) if transition is None else str(transition["previous_agent"])
        )
        expected_target = str(target) if transition is None else str(transition["target_agent"])
        if observed_agent == expected_target:
            state = "confirmed"
            basis = "observed_target_after_resume"
            clear_pending = False
        elif observed_agent == previous:
            state = "rejected"
            basis = "observed_previous_after_resume"
            clear_pending = False
        else:
            state = "unknown"
            basis = "observed_third_state_after_resume"
            clear_pending = True
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.agent_transition.settled",
                "data": {
                    "transition_id": str(transition_id),
                    "target_agent": expected_target,
                    "observed_agent": observed_agent,
                    "clear_pending": clear_pending,
                    "state": state,
                    "result": {
                        "basis": basis,
                        "observed_agent": observed_agent,
                    },
                    "settled_at": time.time(),
                },
            },
            internal_event_id=(f"agent-transition:{transition_id}:attach-reconcile:{state}"),
        )

    async def _reconcile_remote_after_attach(
        self,
        handle: SessionHandle,
        *,
        create: bool,
    ) -> None:
        if self._capabilities is None:
            return
        row = await self._database.fetchone(
            """
            SELECT runtime_remote_mode, pending_remote_transition_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        if row is None:
            raise SessionNotReady("remote state disappeared during attach")
        pending_transition_id = row["pending_remote_transition_id"]
        remote_disable_supported = self._capabilities is not None and self._capabilities.supports(
            "remote_disable"
        )
        export_detach_safe = self._capabilities is not None and self._capabilities.supports(
            "remote_export_detach_safe"
        )
        unsafe_persisted_mode = row["runtime_remote_mode"] == "on" or (
            row["runtime_remote_mode"] == "export" and not export_detach_safe
        )
        uncertain_persisted_state = (
            pending_transition_id is not None
            or row["runtime_remote_mode"] == "unknown"
            or unsafe_persisted_mode
        )
        if not create and uncertain_persisted_state and not remote_disable_supported:
            raise SessionNotReady(
                "unsafe persisted remote exposure cannot be disabled by this runtime"
            )
        should_force_off = not create and remote_disable_supported and uncertain_persisted_state
        if should_force_off:

            async def disable_uncertain_remote() -> None:
                await self._assert_owned_handle(allow_attaching=True)
                await self._sdk_call(self._bridge.disable_remote(handle))

            await self._require_mailbox().submit(
                kind="remote-reconcile-off",
                idempotency_key=(
                    "remote-reconcile-off:"
                    f"{self.binding.runtime_generation}:"
                    f"{pending_transition_id or 'unknown'}"
                ),
                input_payload={
                    "target": "off",
                    "basis": "resume_pending_or_unknown",
                },
                operation=disable_uncertain_remote,
            )
        if create or should_force_off:
            basis = "fresh_create" if create else "resume_forced_off_pending_or_unknown"
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.remote.observed",
                    "data": {
                        "mode": "off",
                        "steerable": False,
                        "snapshot": {"basis": basis},
                        "clear_pending": should_force_off,
                        "abandoned_transition_id": (
                            None if pending_transition_id is None else str(pending_transition_id)
                        ),
                        "observed_at": time.time(),
                    },
                },
                internal_event_id=(f"remote:{self.binding.runtime_generation}:{basis}"),
            )

    async def _attach(
        self,
        *,
        create: bool,
        continue_pending_work: bool,
        reuse_owner: bool = False,
        target_config_version: int | None = None,
    ) -> None:
        config_version = (
            target_config_version
            or self.binding.pending_session_config_version
            or self.binding.desired_session_config_version
        )
        snapshot = await self._load_extension_snapshot(config_version)
        extension_session_options = snapshot.sdk_session_options()
        expected_hash = (
            self.binding.pending_session_config_hash
            if self.binding.pending_session_config_version == config_version
            else self.binding.desired_session_config_hash
        )
        if expected_hash is not None and expected_hash != snapshot.config_hash:
            raise SessionNotReady(
                "extension config generation does not match the session binding hash"
            )
        self._extension_snapshot = snapshot
        async with self._lifecycle_lock:
            if self.state != RuntimeState.DETACHED:
                raise SessionNotReady(f"runtime cannot attach from state {self.state}")
            if self.binding.config_snapshot_state != "verified":
                raise SessionNotReady(
                    "legacy session has no verified project configuration snapshot"
                )
            launch_options = SessionLaunchOptions.from_json(
                self.binding.session_config_snapshot_json
            )
            self.state = RuntimeState.ATTACHING
            if reuse_owner:
                if create or self._lease is None:
                    raise SessionNotReady("same-owner reattach requires an active owner lease")
                if not await self._owner_leases.is_current(self._lease):
                    self.state = RuntimeState.FENCED
                    raise FenceLost(f"owner fence lost for session {self.binding.sdk_session_id}")
                self.binding = await self._bindings.begin_reattach(
                    thread_id=self.binding.thread_id,
                    lease=self._lease,
                )
            else:
                try:
                    self._lease = await self._acquire_owner_for_attachment()
                except BaseException:
                    cleanup = asyncio.create_task(
                        self._release_unassigned_owner_lease(),
                        name=f"owner-acquire-cleanup:{self.binding.sdk_session_id}",
                    )
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await cleanup
                    self.state = RuntimeState.DETACHED
                    raise
            try:
                if not reuse_owner:
                    self.binding = await self._bindings.begin_attachment(
                        thread_id=self.binding.thread_id,
                        lease=self._lease,
                        state=(AttachmentState.CREATING if create else AttachmentState.RESUMING),
                    )
                if (
                    not create
                    and self._capabilities is not None
                    and self._capabilities.supports("sessions_check_in_use")
                ):
                    try:
                        in_use = await self._bridge.check_session_in_use(
                            self.binding.sdk_session_id
                        )
                    except Exception as error:
                        self.binding = await self._bindings.mark_attach_unknown(self.binding)
                        lease = self._lease
                        self._lease = None
                        if lease is not None:
                            await self._owner_leases.release(lease)
                        self.state = RuntimeState.RECOVERY_UNKNOWN
                        raise SessionAttachUnknown(
                            "runtime in-use probe failed before resume"
                        ) from error
                    if in_use:
                        self.binding = await self._bindings.mark_owner_conflict(self.binding)
                        lease = self._lease
                        self._lease = None
                        if lease is not None:
                            await self._owner_leases.release(lease)
                        self.state = RuntimeState.DETACHED
                        raise SessionOwnerConflict(
                            f"session {self.binding.sdk_session_id} is held by another process"
                        )
                self._start_components()
            except BaseException:
                await self._shield_attachment_cleanup(mark_unknown=True)
                raise

            try:
                raw_options = self.binding.session_config_snapshot.get(
                    "session_options",
                    {},
                )
                if not isinstance(raw_options, dict):
                    raise SessionNotReady("session configuration snapshot is invalid")
                session_options = dict(raw_options)
                if create:
                    handle = await self._sdk_call(
                        self._bridge.create_session(
                            session_id=self.binding.sdk_session_id,
                            working_directory=str(self.binding.cwd_snapshot),
                            on_event=self._ingress,
                            on_user_input_request=self._handle_user_input_request,
                            on_exit_plan_mode_request=self._handle_exit_plan_mode_request,
                            on_auto_mode_switch_request=self._handle_auto_mode_switch_request,
                            on_elicitation_request=self._handle_elicitation_request,
                            on_mcp_auth_request=self._handle_mcp_auth_request,
                            permission_handler=self._require_permission_handler(),
                            hooks=self._require_hook_audit().handlers(),
                            session_config=session_options or None,
                            launch_options=launch_options,
                            session_options=extension_session_options,
                        ),
                        capture_result=lambda attached: setattr(
                            self,
                            "_handle",
                            attached,
                        ),
                    )
                else:
                    handle = await self._sdk_call(
                        self._bridge.resume_session(
                            session_id=self.binding.sdk_session_id,
                            working_directory=str(self.binding.cwd_snapshot),
                            on_event=self._ingress,
                            continue_pending_work=continue_pending_work,
                            on_user_input_request=self._handle_user_input_request,
                            on_exit_plan_mode_request=self._handle_exit_plan_mode_request,
                            on_auto_mode_switch_request=self._handle_auto_mode_switch_request,
                            on_elicitation_request=self._handle_elicitation_request,
                            on_mcp_auth_request=self._handle_mcp_auth_request,
                            permission_handler=self._require_permission_handler(),
                            hooks=self._require_hook_audit().handlers(),
                            session_config=session_options or None,
                            launch_options=launch_options,
                            session_options=extension_session_options,
                        ),
                        capture_result=lambda attached: setattr(
                            self,
                            "_handle",
                            attached,
                        ),
                    )
                if handle.session_id != self.binding.sdk_session_id:
                    raise RuntimeError("SDK returned a different session ID")
            except BaseException as error:
                if self._handle is None:
                    await self._shield_attachment_cleanup(mark_unknown=True)
                if isinstance(error, asyncio.CancelledError):
                    raise
                raise SessionAttachUnknown(
                    f"session {self.binding.sdk_session_id} attachment is unknown"
                ) from error

            self._handle = handle
            self._handle_terminal = False
            self._flush_deferred_protocol_responses()
            try:
                await self._recover_event_log(handle, initialize=create)
                if not create:
                    await self._reconcile_unresolved_compactions(basis="attach_event_log_reconcile")
            except Exception as error:
                self.binding = await self._bindings.mark_attach_unknown(self.binding)
                self.state = RuntimeState.RECOVERY_UNKNOWN
                raise SessionAttachUnknown("event-log recovery failed during attach") from error
            try:
                if self._require_permission_handler().managed_permissions_blocked:
                    raise PermissionPostureError("managed settings block permission bypass")
                await self._sdk_call(self._bridge.ensure_allow_all(handle))
            except PermissionPostureError:
                self.binding = await self._bindings.mark_attached_blocked(
                    self.binding,
                    posture=PermissionPosture.PLATFORM_BLOCKED,
                )
                self.state = RuntimeState.DEGRADED
                raise
            except Exception:
                self.binding = await self._bindings.mark_attached_blocked(
                    self.binding,
                    posture=PermissionPosture.UNKNOWN,
                )
                self.state = RuntimeState.DEGRADED
                raise

            self.binding = await self._bindings.mark_attached(self.binding)
            self._mailbox = CommandMailbox(
                store=OperationStore(self._database, self._require_inbox()),
                sdk_session_id=self.binding.sdk_session_id,
                runtime_generation=self.binding.runtime_generation,
                owner_fence_token=self._require_fence_token(),
                fence_validator=self._is_mutation_safe_owner,
                task_registry=self._tasks,
            )
            self._mailbox.start()
            try:
                observed_mode = await self._sdk_call(self._bridge.get_mode(handle))
            except Exception:
                self.state = RuntimeState.DEGRADED
                raise
            if observed_mode not in {"interactive", "plan", "autopilot"}:
                self.state = RuntimeState.DEGRADED
                raise SessionNotReady(f"runtime returned unsupported mode: {observed_mode}")
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.mode.observed",
                    "data": {"mode": observed_mode},
                },
                internal_event_id=(
                    f"mode:{self.binding.runtime_generation}:initial:{observed_mode}"
                ),
            )
            model_row = await self._database.fetchone(
                """
                SELECT desired_model_config
                FROM session_bindings WHERE thread_id = ?
                """,
                (self.binding.thread_id,),
            )
            desired_model = {} if model_row is None else json.loads(model_row[0])
            get_current_model = getattr(self._bridge, "get_current_model", None)
            if callable(get_current_model):
                try:
                    observed_model = await self._sdk_call(get_current_model(handle))
                except Exception:
                    if desired_model:
                        self.state = RuntimeState.DEGRADED
                        raise
                else:
                    observed_hash = hashlib.sha256(
                        json.dumps(observed_model, sort_keys=True).encode()
                    ).hexdigest()[:16]
                    await self._require_inbox().commit_internal(
                        {
                            "type": "copilotd.model.observed",
                            "data": {"observed": observed_model},
                        },
                        internal_event_id=(
                            f"model:{self.binding.runtime_generation}:initial:{observed_hash}"
                        ),
                    )
            elif desired_model:
                self.state = RuntimeState.DEGRADED
                raise SessionNotReady("runtime model reconciliation is unavailable")
            if self._capabilities is not None and self._capabilities.supports("agents_current"):
                await self._reconcile_agent_after_attach(handle)
            config_row = await self._database.fetchone(
                """
                SELECT desired_project_config_version
                FROM session_bindings WHERE thread_id = ?
                """,
                (self.binding.thread_id,),
            )
            if config_row is None:
                raise SessionNotReady("session configuration disappeared during attach")
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.project_config.observed",
                    "data": {"version": int(config_row["desired_project_config_version"])},
                },
                internal_event_id=(
                    f"project-config:{self.binding.runtime_generation}:"
                    f"{int(config_row['desired_project_config_version'])}"
                ),
            )
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.config.observed",
                    "data": {
                        "version": snapshot.version,
                        "config_hash": snapshot.config_hash,
                    },
                },
                internal_event_id=(
                    f"config:{self.binding.runtime_generation}:"
                    f"{snapshot.version}:{snapshot.config_hash[:16]}"
                ),
            )
            await self._reconcile_remote_after_attach(handle, create=create)
            await self._prime_readiness()
            observed_binding = await self._bindings.by_thread(self.binding.thread_id)
            if observed_binding is None:
                raise SessionNotReady("session binding disappeared during mode reconciliation")
            self.binding = observed_binding
            self.state = RuntimeState.READY
            async with self._admission_lock:
                self._accepting_sends = True
            self._start_runtime_producers()

    async def _attach_guarded(
        self,
        *,
        create: bool,
        continue_pending_work: bool,
    ) -> None:
        try:
            await self._attach(
                create=create,
                continue_pending_work=continue_pending_work,
            )
        except asyncio.CancelledError:
            if (
                self._handle is not None or self._lease is not None or self._inbox is not None
            ) and self.state not in {
                RuntimeState.DETACHED,
                RuntimeState.RECOVERY_UNKNOWN,
            }:
                await self._shield_cancelled_attachment_cleanup()
            raise
        except BaseException as error:
            await self._cleanup_failed_attach(error)
            raise

    async def _shield_cancelled_attachment_cleanup(self) -> None:
        cleanup = asyncio.create_task(
            self._cleanup_cancelled_attachment(),
            name=f"attach-cancel-cleanup:{self.binding.sdk_session_id}",
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
        except BaseException:
            self.state = RuntimeState.RECOVERY_UNKNOWN
            _LOGGER.exception(
                "cancelled attachment cleanup failed for %s",
                self.binding.sdk_session_id,
            )

    async def _cleanup_cancelled_attachment(self) -> None:
        disconnected = False
        durable_state_set = False
        lease = self._lease
        fence_token = lease.fence_token if lease is not None else self.binding.owner_fence_token
        try:
            if self._handle is not None:
                try:
                    await self._sdk_call(self._handle.disconnect())
                except BaseException:
                    disconnected = False
                else:
                    disconnected = True
            current = await self._bindings.by_thread(self.binding.thread_id)
            if (
                current is not None
                and current.sdk_session_id == self.binding.sdk_session_id
                and fence_token is not None
                and current.owner_fence_token == fence_token
            ):
                if disconnected and current.attachment_state in {
                    AttachmentState.CREATING,
                    AttachmentState.RESUMING,
                    AttachmentState.ATTACHED,
                }:
                    self.binding = await self._bindings.reset_cancelled_attachment(current)
                    self.state = RuntimeState.DETACHED
                    durable_state_set = True
                elif current.attachment_state in {
                    AttachmentState.CREATING,
                    AttachmentState.RESUMING,
                    AttachmentState.ATTACHED,
                    AttachmentState.DISCONNECTING,
                }:
                    self.binding = await self._bindings.mark_recovery_unknown(current)
                    self.state = RuntimeState.RECOVERY_UNKNOWN
                    durable_state_set = True
        except BaseException:
            self.state = RuntimeState.RECOVERY_UNKNOWN
            try:
                async with self._database.transaction() as connection:
                    cursor = await connection.execute(
                        """
                        UPDATE session_bindings
                        SET attachment_state = 'recovery_unknown',
                            attachment_reason = 'attach_cancel_cleanup_failed',
                            permission_posture = 'unknown',
                            permission_verified_at = NULL,
                            updated_at = ?, row_version = row_version + 1
                        WHERE thread_id = ? AND sdk_session_id = ?
                          AND owner_fence_token = ?
                          AND attachment_state IN (
                            'creating', 'resuming', 'attached', 'disconnecting'
                          )
                        """,
                        (
                            time.time(),
                            self.binding.thread_id,
                            self.binding.sdk_session_id,
                            fence_token,
                        ),
                    )
                    durable_state_set = cursor.rowcount == 1
                    await cursor.close()
            except BaseException:
                _LOGGER.exception(
                    "could not mark cancelled attachment unknown for %s",
                    self.binding.sdk_session_id,
                )
            _LOGGER.exception(
                "attachment cancellation reconciliation failed for %s",
                self.binding.sdk_session_id,
            )
        finally:
            if not durable_state_set:
                self.state = RuntimeState.RECOVERY_UNKNOWN
            try:
                await self._stop_components(release_owner=True)
            except BaseException:
                self.state = RuntimeState.RECOVERY_UNKNOWN
                _LOGGER.exception(
                    "attachment cancellation component release failed for %s",
                    self.binding.sdk_session_id,
                )
                if lease is not None:
                    try:
                        await self._owner_leases.release(lease)
                    except BaseException:
                        _LOGGER.exception(
                            "attachment cancellation owner release failed for %s",
                            self.binding.sdk_session_id,
                        )
        if disconnected and durable_state_set and (self.state != RuntimeState.RECOVERY_UNKNOWN):
            self.state = RuntimeState.DETACHED

    async def _shield_attachment_cleanup(self, *, mark_unknown: bool) -> None:
        cleanup = asyncio.create_task(
            self._cleanup_attachment_failure(mark_unknown=mark_unknown),
            name=f"attach-cleanup:{self.binding.sdk_session_id}",
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup

    async def _cleanup_attachment_failure(self, *, mark_unknown: bool) -> None:
        lease = self._lease
        if mark_unknown:
            self.state = RuntimeState.RECOVERY_UNKNOWN
        try:
            if self._inbox is not None or self._lease is not None:
                await self._stop_components(release_owner=True)
        except BaseException:
            self.state = RuntimeState.RECOVERY_UNKNOWN
            _LOGGER.exception(
                "attachment failure component cleanup failed for %s",
                self.binding.sdk_session_id,
            )
            if lease is not None:
                try:
                    await self._owner_leases.release(lease)
                except BaseException:
                    _LOGGER.exception(
                        "attachment failure owner release failed for %s",
                        self.binding.sdk_session_id,
                    )
        if mark_unknown:
            try:
                fence_token = (
                    lease.fence_token if lease is not None else self.binding.owner_fence_token
                )
                current = await self._bindings.by_thread(self.binding.thread_id)
                if (
                    current is not None
                    and current.sdk_session_id == self.binding.sdk_session_id
                    and fence_token is not None
                    and current.owner_fence_token == fence_token
                    and current.attachment_state
                    in {AttachmentState.CREATING, AttachmentState.RESUMING}
                ):
                    self.binding = await self._bindings.mark_attach_unknown(current)
            except BaseException:
                _LOGGER.exception(
                    "attachment failure recovery marking failed for %s",
                    self.binding.sdk_session_id,
                )
        elif self.state != RuntimeState.RECOVERY_UNKNOWN:
            self.state = RuntimeState.DETACHED

    async def _acquire_owner_for_attachment(self) -> OwnerLease:
        error: OwnerConflict | None = None
        for attempt in range(5):
            try:
                return await self._owner_leases.acquire(
                    self.binding.sdk_session_id,
                    self._owner_id,
                )
            except OwnerConflict as conflict:
                error = conflict
                if attempt == 4:
                    break
                await asyncio.sleep(0.1 * (attempt + 1))
        assert error is not None
        raise error

    async def _release_unassigned_owner_lease(self) -> None:
        current = await self._owner_leases.current(self.binding.sdk_session_id)
        if current is None or current.owner_id != self._owner_id:
            return
        try:
            await self._owner_leases.release(current)
        except FenceLost:
            return

    def _start_components(self) -> None:
        self._loop = asyncio.get_running_loop()
        fence_token = self._require_fence_token()
        self._inbox = ReducerInbox(
            sdk_session_id=self.binding.sdk_session_id,
            generation=self.binding.runtime_generation,
            fence_token=fence_token,
            capacity=self._ingress_capacity,
            thread_id=self.binding.thread_id,
        )
        self._interaction_gateway = InteractionGateway(
            database=self._database,
            inbox=self._inbox,
            scope=InteractionScope(
                sdk_session_id=self.binding.sdk_session_id,
                runtime_generation=self.binding.runtime_generation,
                owner_fence_token=fence_token,
                thread_id=self.binding.thread_id,
            ),
            timeout_seconds=self._interaction_timeout_seconds,
        )
        snapshot = self._extension_snapshot
        if snapshot is None:
            raise SessionNotReady("extension config snapshot is unavailable")
        self._hook_audit = SessionHookAudit(
            inbox=self._inbox,
            context=HookSessionContext(
                sdk_session_id=self.binding.sdk_session_id,
                runtime_generation=self.binding.runtime_generation,
                owner_fence_token=fence_token,
                thread_id=self.binding.thread_id,
                project_id=self.binding.project_id,
                project_source=self.binding.project_source,
                cwd_snapshot=str(self.binding.cwd_snapshot),
                config_version=snapshot.version,
                config_hash=snapshot.config_hash,
            ),
        )
        self._permission_handler = ManagedAwarePermissionHandler(
            self._audit_permission,
            self._is_mutation_safe_owner,
        )
        self._permission_handler.set_managed_permissions_blocked(
            self.binding.managed_permissions_blocked
        )
        self._ingress = SdkEventIngress(
            self._inbox,
            on_event_accepted=self._on_sdk_event_accepted,
        )

        async def validate(generation: int, token: int) -> bool:
            return (
                generation == self.binding.runtime_generation
                and token == fence_token
                and await self._is_current_owner()
            )

        self._reducer = EventReducerWorker(
            inbox=self._inbox,
            reducer=JournalReducer(
                self._database,
                require_binding_fence=True,
            ),
            batch_size=self._reducer_batch_size,
            fence_validator=validate,
            task_registry=self._tasks,
        )
        self._reducer.start()
        self._overflow_task = self._tasks.create(
            self._overflow_supervisor(),
            name=f"ingress-overflow:{self.binding.sdk_session_id}",
            source="ingress-overflow",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )
        self._renewal_stop.clear()
        self._renewal_task = self._tasks.create(
            self._renew_owner(),
            name=f"owner-renew:{self.binding.sdk_session_id}",
            source="owner-renewal",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )

    async def _recover_event_log(
        self,
        handle: SessionHandle,
        *,
        initialize: bool,
    ) -> None:
        if self._capabilities is None or not self._capabilities.supports("event_log"):
            return
        cursor_state = await self._database.fetchone(
            """
            SELECT event_cursor, event_cursor_epoch, event_predecessor_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        cursor = None if cursor_state is None else cursor_state["event_cursor"]
        cursor_epoch = 0 if cursor_state is None else int(cursor_state["event_cursor_epoch"])
        predecessor_id = None if cursor_state is None else cursor_state["event_predecessor_id"]
        if initialize:
            tail = await self._bridge.tail_event_log(handle)
            await self._advance_event_cursor(
                tail,
                cursor_status="tail",
                cursor_epoch=cursor_epoch,
                last_event_id=predecessor_id,
            )
            return

        seen_ids: set[str] = set()
        if predecessor_id is not None:
            seen_ids.add(str(predecessor_id))
        for _ in range(1000):
            previous_cursor = cursor
            batch = await self._bridge.read_event_log(
                handle,
                cursor=None if cursor is None else str(cursor),
                max_events=500,
                wait_ms=0,
                include_ephemeral=False,
            )
            if batch.cursor_status == "expired":
                cursor_epoch += 1
                await self._record_recovery_incident(
                    "event_cursor_expired_rebase",
                    {
                        "previous_cursor": cursor,
                        "rebased_cursor": batch.cursor,
                        "cursor_epoch": cursor_epoch,
                    },
                )
            for recovered in batch.events:
                parent_id = (
                    None
                    if getattr(recovered, "parent_id", None) is None
                    else str(recovered.parent_id)
                )
                if parent_id is not None and parent_id not in seen_ids:
                    known = await self._database.fetchone(
                        """
                        SELECT 1 FROM event_journal
                        WHERE sdk_session_id = ? AND event_id = ?
                        """,
                        (self.binding.sdk_session_id, parent_id),
                    )
                    if known is None:
                        await self._record_recovery_incident(
                            "event_predecessor_gap",
                            {
                                "event_id": str(recovered.id),
                                "parent_id": parent_id,
                                "classification": "filtered_or_retained_gap",
                                "cursor_epoch": cursor_epoch,
                            },
                        )
                await self._require_inbox().commit_recovered_sdk(recovered)
                seen_ids.add(str(recovered.id))
                predecessor_id = str(recovered.id)
            cursor = batch.cursor
            await self._advance_event_cursor(
                cursor,
                cursor_status=batch.cursor_status,
                cursor_epoch=cursor_epoch,
                last_event_id=predecessor_id,
            )
            if not batch.has_more:
                return
            if cursor == previous_cursor:
                await self._record_recovery_incident(
                    "event_cursor_stalled",
                    {"cursor": cursor, "cursor_epoch": cursor_epoch},
                )
                raise RuntimeError("event-log cursor did not advance while has_more was true")
        raise RuntimeError("event-log recovery exceeded the bounded page limit")

    async def _advance_event_cursor(
        self,
        cursor: str,
        *,
        cursor_status: str,
        cursor_epoch: int,
        last_event_id: str | None,
    ) -> None:
        cursor_hash = hashlib.sha256(cursor.encode()).hexdigest()[:16]
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.event_cursor.advanced",
                "data": {
                    "cursor": cursor,
                    "cursor_status": cursor_status,
                    "cursor_epoch": cursor_epoch,
                    "last_event_id": last_event_id,
                },
            },
            source="snapshot",
            internal_event_id=(
                f"event-cursor:{self.binding.runtime_generation}:{cursor_epoch}:{cursor_hash}"
            ),
        )

    async def _record_recovery_incident(
        self,
        kind: str,
        detail: dict[str, Any],
    ) -> None:
        incident_id = str(uuid.uuid4())
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.recovery.incident",
                "data": {
                    "kind": kind,
                    "detail": detail,
                    "observed_at": time.time(),
                },
            },
            source="snapshot",
            internal_event_id=f"recovery-incident:{incident_id}",
        )

    async def _overflow_supervisor(self) -> None:
        inbox = self._require_inbox()
        await inbox.overflow_event.wait()
        incident = inbox.overflow
        if incident is None:
            return
        try:
            async with self._lifecycle_lock:
                self.state = RuntimeState.RECOVERY_UNKNOWN
                async with self._admission_lock:
                    self._accepting_sends = False
                if self._mailbox is not None:
                    self._mailbox.freeze()
                inbox.close_sdk()
                await inbox.join()
                await self._record_recovery_incident(
                    "ingress_overflow",
                    {
                        "first_lost_inbox_seq": incident.first_lost_inbox_seq,
                        "first_lost_sdk_receive_seq": (incident.first_lost_sdk_receive_seq),
                        "lost_count": incident.lost_count,
                    },
                )
                await self._force_active_unknown()
                handle = self._require_handle()
                await self._recover_event_log(handle, initialize=False)
                await self._disconnect_for_recovery(handle, reason="overflow")
                current = await self._bindings.by_thread(self.binding.thread_id)
                if (
                    current is not None
                    and current.runtime_generation == self.binding.runtime_generation
                    and current.owner_fence_token == self.binding.owner_fence_token
                    and current.attachment_state == AttachmentState.ATTACHED
                ):
                    self.binding = await self._bindings.mark_recovery_unknown(current)
                self._overflow_task = None
                await self._stop_components(release_owner=True)
                self.state = RuntimeState.DETACHED
            await self.attach_resume()
        except BaseException as error:
            self.state = RuntimeState.RECOVERY_UNKNOWN
            await self._database.execute(
                """
                INSERT INTO runtime_incidents(
                    timestamp, runtime_generation, session_id, kind, detail
                ) VALUES (?, ?, ?, 'overflow_recovery_failed', ?)
                """,
                (
                    time.time(),
                    self.binding.runtime_generation,
                    self.binding.sdk_session_id,
                    json.dumps(
                        {"error_type": type(error).__name__, "message": str(error)},
                        sort_keys=True,
                    ),
                ),
            )
            raise

    async def _disconnect_for_recovery(
        self,
        handle: SessionHandle,
        *,
        reason: str,
    ) -> None:
        if self._mailbox is not None:
            await self._mailbox.stop(timeout_seconds=self._shutdown_timeout_seconds)
            self._mailbox = None
        store = OperationStore(self._database, self._require_inbox())
        record, created = await store.begin(
            sdk_session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
            owner_fence_token=self._require_fence_token(),
            kind="recovery_disconnect",
            idempotency_key=(f"{reason}:{self.binding.runtime_generation}:disconnect"),
            input_payload={},
        )
        if not created:
            if record.state.value == "confirmed":
                return
            raise RuntimeError("prior overflow disconnect has an uncertain outcome")
        record = await store.transition(record, state=OperationState.STARTED)
        if not await self._is_mutation_safe_owner():
            await store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code="owner_lease_headroom",
            )
            raise FenceLost("insufficient owner lease headroom for overflow disconnect")
        try:
            await self._sdk_call(handle.disconnect())
        except Exception as error:
            await store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code=type(error).__name__,
            )
            raise
        await store.transition(record, state=OperationState.CONFIRMED)

    async def _cleanup_failed_attach(self, original_error: BaseException) -> None:
        if (
            self._inbox is None
            and self._reducer is None
            and self._lease is None
            and self._handle is None
        ):
            if self.state == RuntimeState.ATTACHING:
                self.state = RuntimeState.DETACHED
            return
        errors: list[Exception] = []
        async with self._lifecycle_lock:
            async with self._admission_lock:
                self._accepting_sends = False
            if self._mailbox is not None:
                self._mailbox.freeze()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if (
                current is not None
                and current.runtime_generation == self.binding.runtime_generation
                and current.owner_fence_token == self.binding.owner_fence_token
                and current.attachment_state
                in {
                    AttachmentState.CREATING,
                    AttachmentState.RESUMING,
                    AttachmentState.ATTACHED,
                    AttachmentState.DISCONNECTING,
                }
            ):
                try:
                    self.binding = await self._bindings.mark_recovery_unknown(current)
                except Exception as error:
                    errors.append(error)
            handle = self._handle
            lease = self._lease
            if (
                handle is not None
                and lease is not None
                and await self._is_current_owner()
                and not await self._is_mutation_safe_owner()
            ):
                try:
                    self._lease = await self._owner_leases.renew(lease)
                except Exception as error:
                    errors.append(error)
            if (
                handle is not None
                and self._inbox is not None
                and self._reducer is not None
                and await self._is_current_owner()
            ):
                try:
                    await self._disconnect_for_recovery(
                        handle,
                        reason="attach-failed",
                    )
                except Exception as error:
                    errors.append(error)
            try:
                await self._stop_components(release_owner=True)
            except Exception as error:
                errors.append(error)
            self.state = RuntimeState.RECOVERY_UNKNOWN
        if errors:
            raise SessionAttachUnknown(
                f"{type(original_error).__name__} was followed by cleanup failure"
            ) from ExceptionGroup("attachment cleanup failed", errors)

    def _start_runtime_producers(self) -> None:
        self._queue_stop.clear()
        self._queue_task = self._tasks.create(
            self._queue_pump(),
            name=f"queue-pump:{self.binding.sdk_session_id}",
            source="queue-pump",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )
        self._task_reconcile_stop.clear()
        if self._snapshot_topics:
            self._task_reconcile_requested.set()
        else:
            self._task_reconcile_requested.clear()
        self._task_reconcile_task = self._tasks.create(
            self._task_reconcile_loop(),
            name=f"task-reconcile:{self.binding.sdk_session_id}",
            source="snapshot-reconciler",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )
        self._permission_reconcile_stop.clear()
        self._permission_reconcile_task = self._tasks.create(
            self._permission_reconcile_loop(),
            name=f"permission-reconcile:{self.binding.sdk_session_id}",
            source="permission-reconciler",
            session_id=self.binding.sdk_session_id,
            runtime_generation=self.binding.runtime_generation,
        )
        if self._renewal_task is None:
            self._renewal_stop.clear()
            self._renewal_task = self._tasks.create(
                self._renew_owner(),
                name=f"owner-renew:{self.binding.sdk_session_id}",
                source="owner-renewal",
                session_id=self.binding.sdk_session_id,
                runtime_generation=self.binding.runtime_generation,
            )

    async def _renew_owner(self) -> None:
        while not self._renewal_stop.is_set():
            try:
                await asyncio.wait_for(
                    self._renewal_stop.wait(),
                    timeout=self._owner_renew_seconds,
                )
            except TimeoutError:
                pass
            if self._renewal_stop.is_set():
                return
            lease = self._lease
            if lease is None:
                return
            try:
                self._lease = await self._owner_leases.renew(lease)
            except Exception:
                self.state = RuntimeState.FENCED
                if self._mailbox is not None:
                    self._mailbox.freeze()
                if self._inbox is not None:
                    self._inbox.close_sdk()
                raise

    async def _assert_dispatchable(self) -> None:
        if self.state != RuntimeState.READY:
            raise SessionNotReady(f"session runtime is {self.state}")
        draining = await self._database.fetchone(
            "SELECT value FROM global_config WHERE key = 'restart_draining'"
        )
        if draining is not None and draining["value"] == "1":
            raise SessionNotReady("copilotD is draining for restart")
        await self._assert_owned_handle()
        handler = self._permission_handler
        if handler is not None and handler.managed_permissions_blocked:
            raise SessionNotReady("managed permissions are platform-blocked")
        binding = await self._bindings.by_thread(self.binding.thread_id)
        if binding is None:
            raise SessionNotReady("session binding no longer exists")
        if binding.binding_intent != BindingIntent.ACTIVE and not (
            binding.binding_intent == BindingIntent.CLOSED
            and binding.attachment_reason in TYPED_CLOSED_ATTACHMENT_REASONS
        ):
            raise SessionNotReady("session binding is closed without a typed dispatch exemption")
        if binding.project_id is not None:
            project = await self._database.fetchone(
                "SELECT state FROM projects WHERE id = ?",
                (binding.project_id,),
            )
            if project is None or project["state"] == "closing":
                raise SessionNotReady("session project is closing")
        self.binding = binding
        if binding.attachment_state != AttachmentState.ATTACHED:
            raise SessionNotReady(f"session attachment is {binding.attachment_state}")
        if binding.managed_permissions_blocked:
            raise SessionNotReady("managed permissions are platform-blocked")
        if binding.permission_posture != PermissionPosture.VERIFIED_ALLOW_ALL:
            raise SessionNotReady(f"permission posture is {binding.permission_posture}")
        if binding.pending_mode is not None:
            raise SessionNotReady(f"mode transition is pending: {binding.pending_mode}")
        if binding.runtime_mode != binding.desired_mode:
            raise SessionNotReady(
                f"mode drift: desired={binding.desired_mode}, runtime={binding.runtime_mode}"
            )
        if binding.pending_session_config_version is not None:
            raise SessionNotReady(
                "session extension config transition is pending: "
                f"{binding.pending_session_config_version}"
            )
        if (
            binding.runtime_session_config_version is None
            or binding.runtime_session_config_hash is None
        ):
            raise SessionNotReady("runtime_session_config_unknown")
        if (
            binding.runtime_session_config_version != binding.desired_session_config_version
            or binding.runtime_session_config_hash != binding.desired_session_config_hash
            or binding.session_config_drift
        ):
            raise SessionNotReady("runtime extension configuration drifted")
        model_state = await self._database.fetchone(
            """
            SELECT desired_model_config, pending_model_config, runtime_model_config
            FROM session_bindings
            WHERE thread_id = ?
            """,
            (binding.thread_id,),
        )
        if model_state is None:
            raise SessionNotReady("session model state is unavailable")
        if model_state["pending_model_config"] is not None:
            raise SessionNotReady("model transition is pending")
        desired_model = json.loads(model_state["desired_model_config"])
        runtime_model = (
            None
            if model_state["runtime_model_config"] is None
            else json.loads(model_state["runtime_model_config"])
        )
        if desired_model and runtime_model is None:
            raise SessionNotReady("runtime model state is unknown")
        if desired_model and not _model_config_matches(desired_model, runtime_model):
            raise SessionNotReady("runtime model configuration drifted")
        blockers = await self._readiness_blockers(require_quiet=False)
        if blockers:
            raise SessionNotReady("session readiness is blocked: " + ", ".join(blockers))

    async def _assert_claimed_dispatchable(
        self,
        *,
        require_quiet: bool,
        requested_mode: AgentMode,
        requested_model_config: dict[str, Any],
        requested_agent: str,
        requested_session_config_version: int,
        requested_origin: str,
        enforce_mode_snapshot: bool,
    ) -> None:
        await self._assert_dispatchable()
        readiness = await self._refresh_readiness()
        if require_quiet and (
            readiness["processing"]
            or readiness["hasActiveWork"]
            or readiness["pendingItems"]
            or readiness["steeringMessages"]
        ):
            raise SessionNotReady("runtime became active after queue claim")
        row = await self._database.fetchone(
            """
            SELECT runtime_mode, runtime_model_config, desired_agent, runtime_agent,
                   desired_project_config_version, runtime_project_config_version
            FROM session_bindings WHERE thread_id = ?
            """,
            (self.binding.thread_id,),
        )
        if row is None:
            raise SessionNotReady("claimed session configuration disappeared")
        if enforce_mode_snapshot and str(row["runtime_mode"]) != requested_mode:
            raise SessionNotReady("claimed queue mode snapshot drifted")
        runtime_model = (
            None
            if row["runtime_model_config"] is None
            else json.loads(str(row["runtime_model_config"]))
        )
        if requested_model_config:
            if runtime_model is None or not _model_config_matches(
                requested_model_config,
                runtime_model,
            ):
                raise SessionNotReady("claimed queue model snapshot drifted")
        runtime_agent = str(row["runtime_agent"])
        agent_evidenced = self._capabilities is not None and self._capabilities.supports(
            "selected_agent"
        )
        observed_agent = (
            str(row["desired_agent"])
            if runtime_agent == "unknown" and not agent_evidenced
            else runtime_agent
        )
        if requested_agent != observed_agent:
            raise SessionNotReady("claimed queue agent snapshot drifted")
        desired_config_version = int(row["desired_project_config_version"])
        observed_config_version = (
            desired_config_version
            if row["runtime_project_config_version"] is None
            else int(row["runtime_project_config_version"])
        )
        if (
            requested_session_config_version != desired_config_version
            or requested_session_config_version != observed_config_version
        ):
            raise SessionNotReady("claimed queue session-config snapshot drifted")
        await self._assert_pre_send_fence(requested_origin=requested_origin)

    async def _assert_pre_send_fence(self, *, requested_origin: str) -> None:
        lease = self._lease
        if self.state != RuntimeState.READY or self._handle is None:
            raise SessionNotReady(f"session runtime is {self.state}")
        if (
            lease is None
            or lease.sdk_session_id != self.binding.sdk_session_id
            or lease.owner_id != self._owner_id
            or lease.fence_token != self.binding.owner_fence_token
        ):
            self.state = RuntimeState.FENCED
            if self._mailbox is not None:
                self._mailbox.freeze()
            raise FenceLost(f"owner fence lost for session {self.binding.sdk_session_id}")
        now = time.time()
        typed_attachment_reason = (
            "scheduler_run"
            if requested_origin == "app_schedule"
            else (
                "recovery_cleanup"
                if requested_origin in {"recovery", "recovery_cleanup"}
                else "__not_typed__"
            )
        )
        current = await self._database.fetchone(
            """
            SELECT 1
            FROM session_bindings b
            JOIN session_owner_leases l
              ON l.sdk_session_id = b.sdk_session_id
             AND l.fence_token = b.owner_fence_token
            WHERE b.thread_id = ? AND b.sdk_session_id = ?
              AND b.runtime_generation = ?
              AND b.owner_fence_token = ?
              AND b.attachment_state = 'attached'
              AND (
                  b.binding_intent = 'active'
                  OR (
                      b.binding_intent = 'closed'
                      AND b.attachment_reason = ?
                  )
              )
              AND l.owner_id = ?
              AND l.expires_at - ? >= ?
            """,
            (
                self.binding.thread_id,
                self.binding.sdk_session_id,
                self.binding.runtime_generation,
                self.binding.owner_fence_token,
                typed_attachment_reason,
                self._owner_id,
                now,
                MUTATION_HEADROOM_SECONDS,
            ),
        )
        if current is None:
            if not await self._is_current_owner():
                self.state = RuntimeState.FENCED
                if self._mailbox is not None:
                    self._mailbox.freeze()
                raise FenceLost(f"owner fence lost for session {self.binding.sdk_session_id}")
            raise FenceLost(
                f"owner lease lacks mutation headroom for {self.binding.sdk_session_id}"
            )

    async def _assert_owned_handle(
        self,
        *,
        allow_closing: bool = False,
        allow_attaching: bool = False,
    ) -> None:
        if self._service_quiesced and not allow_closing:
            raise SessionNotReady("session admission is quiesced for service restart")
        allowed = {RuntimeState.READY}
        if allow_closing:
            allowed.add(RuntimeState.CLOSING)
        if allow_attaching:
            allowed.add(RuntimeState.ATTACHING)
        if self.state not in allowed:
            raise SessionNotReady(f"session runtime is {self.state}")
        if self._handle is None or not await self._is_current_owner():
            self.state = RuntimeState.FENCED
            if self._mailbox is not None:
                self._mailbox.freeze()
            raise FenceLost(f"owner fence lost for session {self.binding.sdk_session_id}")

    async def _is_current_owner(self) -> bool:
        lease = self._lease
        return lease is not None and await self._owner_leases.is_current(lease)

    async def _is_mutation_safe_owner(self) -> bool:
        lease = self._lease
        return lease is not None and await self._owner_leases.has_mutation_headroom(
            lease,
            minimum_seconds=MUTATION_HEADROOM_SECONDS,
        )

    async def _force_active_unknown(self) -> None:
        receipt_id = str(uuid.uuid4())
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.submission.active_unknown",
                "data": {
                    "reason": "runtime_shutdown_or_recovery",
                    "observed_at": time.time(),
                },
            },
            internal_event_id=f"submissions:{receipt_id}:active-unknown",
        )

    async def _sdk_call(
        self,
        operation: Awaitable[T],
        *,
        timeout_seconds: float | None = None,
        capture_result: Callable[[T], None] | None = None,
    ) -> T:
        async with asyncio.timeout(
            self._sdk_operation_timeout_seconds if timeout_seconds is None else timeout_seconds
        ):
            result = await operation
        if capture_result is not None:
            capture_result(result)
        lease = self._lease
        if lease is not None:
            latest = await self._bindings.by_thread(self.binding.thread_id)
            owns_generation = (
                latest is not None
                and latest.runtime_generation == self.binding.runtime_generation
                and latest.owner_fence_token == self.binding.owner_fence_token
            )
            if not owns_generation or not await self._is_mutation_safe_owner():
                if self._mailbox is not None:
                    self._mailbox.freeze()
                raise FenceLost("owner generation, fence, or lease headroom was lost after SDK RPC")
        return result

    async def _cancel_component_task(
        self,
        task: asyncio.Task[Any] | None,
    ) -> None:
        if task is None:
            return
        task.cancel()
        try:
            async with asyncio.timeout(self._shutdown_timeout_seconds):
                await asyncio.gather(task, return_exceptions=True)
        except TimeoutError:
            task.add_done_callback(_consume_task_result)

    async def _stop_components(
        self,
        *,
        release_owner: bool,
        emergency: bool = False,
    ) -> None:
        self._accepting_sends = False
        errors: list[Exception] = []
        protocol_tasks = list(self._protocol_tasks)
        self._protocol_tasks.clear()
        for task in protocol_tasks:
            if task is not asyncio.current_task():
                await self._cancel_component_task(task)
        overflow_task = self._overflow_task
        self._overflow_task = None
        if overflow_task is not None and overflow_task is not asyncio.current_task():
            await self._cancel_component_task(overflow_task)
        self._queue_stop.set()
        if self._queue_task is not None:
            await self._cancel_component_task(self._queue_task)
            self._queue_task = None
        self._task_reconcile_stop.set()
        self._task_reconcile_requested.set()
        if self._task_reconcile_task is not None:
            await self._cancel_component_task(self._task_reconcile_task)
            self._task_reconcile_task = None
        self._permission_reconcile_stop.set()
        self._permission_reconcile_requested.set()
        if self._permission_reconcile_task is not None:
            await self._cancel_component_task(self._permission_reconcile_task)
            self._permission_reconcile_task = None
        if self._mailbox is not None:
            try:
                if emergency:
                    await self._mailbox.emergency_stop(
                        timeout_seconds=self._shutdown_timeout_seconds
                    )
                else:
                    await self._mailbox.stop(timeout_seconds=self._shutdown_timeout_seconds)
            except Exception as error:
                errors.append(error)
            self._mailbox = None
        self._renewal_stop.set()
        if self._renewal_task is not None:
            await self._cancel_component_task(self._renewal_task)
            self._renewal_task = None
        if self._reducer is not None:
            reducer = self._reducer
            try:
                if emergency:
                    await reducer.emergency_stop(timeout_seconds=self._shutdown_timeout_seconds)
                else:
                    await reducer.stop(timeout_seconds=self._shutdown_timeout_seconds)
            except Exception as error:
                if self.state != RuntimeState.FENCED:
                    errors.append(error)
            else:
                if reducer.failure is not None and self.state != RuntimeState.FENCED:
                    errors.append(reducer.failure)
            self._reducer = None
        self._inbox = None
        self._ingress = None
        self._interaction_gateway = None
        self._hook_audit = None
        self._permission_handler = None
        self._loop = None
        self._handle = None
        self._handle_terminal = True
        self._deferred_protocol_events = []
        lease = self._lease
        if release_owner:
            self._lease = None
        if release_owner and lease is not None:
            try:
                await self._owner_leases.release(lease)
            except FenceLost as error:
                if not emergency:
                    errors.append(error)
            except Exception as error:
                if not (self.state == RuntimeState.FENCED and isinstance(error, FenceLost)):
                    errors.append(error)
        if errors:
            raise ExceptionGroup("session component shutdown failed", errors)

    def _require_handle(self) -> SessionHandle:
        if self._handle is None or self._handle_terminal:
            raise SessionNotReady("session handle is not attached")
        return self._handle

    def _require_inbox(self) -> ReducerInbox:
        if self._inbox is None:
            raise SessionNotReady("session reducer inbox is not running")
        return self._inbox

    def _require_mailbox(self) -> CommandMailbox:
        if self._mailbox is None:
            raise SessionNotReady("session command mailbox is not running")
        return self._mailbox

    def _require_interaction_gateway(self) -> InteractionGateway:
        if self._interaction_gateway is None:
            raise SessionNotReady("interaction gateway is not available")
        return self._interaction_gateway

    def _require_hook_audit(self) -> SessionHookAudit:
        if self._hook_audit is None:
            raise SessionNotReady("session hooks are not available")
        return self._hook_audit

    def _require_permission_handler(self) -> ManagedAwarePermissionHandler:
        if self._permission_handler is None:
            raise SessionNotReady("permission handler is not available")
        return self._permission_handler

    def _require_fence_token(self) -> int:
        if self.binding.owner_fence_token is None:
            raise SessionNotReady("session binding has no owner fence")
        return self.binding.owner_fence_token

    async def _load_extension_snapshot(
        self,
        version: int,
    ) -> ExtensionConfigSnapshot:
        project_source = self.binding.project_source
        project_id = self.binding.project_id
        if project_source == "explicit" and project_id is None:
            project_source = "implicit-home"
        if self._extension_configs is not None:
            return await self._extension_configs.for_session(
                project_source=project_source,
                project_id=project_id,
                cwd_snapshot=self.binding.cwd_snapshot,
                version=version,
            )
        config = ProjectExtensionConfig()
        return ExtensionConfigSnapshot(
            scope_key=extension_scope_key(
                project_source,
                project_id,
            ),
            version=version,
            project_id=project_id,
            project_source=project_source,
            cwd_snapshot=self.binding.cwd_snapshot,
            config_hash=config.digest(),
            config=config,
        )

    async def _audit_permission(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        audit_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"copilotd:{self.binding.sdk_session_id}:"
                    f"{self.binding.runtime_generation}:permission:"
                    f"{payload.get('request_id') or hashlib.sha256(encoded.encode()).hexdigest()}"
                ),
            )
        )
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.permission.audit",
                "data": {
                    "audit_id": audit_id,
                    **payload,
                    "observed_at": time.time(),
                },
            },
            internal_event_id=f"permission:{audit_id}",
        )


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _task_by_id(tasks: list[dict[str, Any]], task_id: str) -> dict[str, Any]:
    task = next((item for item in tasks if str(item.get("id")) == task_id), None)
    if task is None:
        raise ValueError(f"runtime task does not exist: {task_id}")
    return task


def _model_config_matches(
    desired: dict[str, Any],
    runtime: dict[str, Any] | None,
) -> bool:
    if runtime is None:
        return False
    explicit_mask = desired.get("confirmationMask")
    mask = (
        [str(item) for item in explicit_mask]
        if isinstance(explicit_mask, list)
        else [
            key
            for key in (
                "modelId",
                "reasoningEffort",
                "reasoningSummary",
                "contextTier",
            )
            if key in desired
        ]
    )
    known_fields = set(runtime.get("knownFields", runtime))
    return all(key in known_fields and runtime.get(key) == desired.get(key) for key in mask)
