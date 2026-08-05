from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any, Literal, Protocol, TypeVar, cast

from copilotd.core.bindings import (
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
from copilotd.core.inbox import ReducerInbox, SdkEventIngress
from copilotd.core.interactions import interaction_target_mode
from copilotd.core.mailbox import (
    CommandMailbox,
    OperationAmbiguous,
    OperationRejected,
    OperationStore,
)
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.task_registry import TaskRegistry
from copilotd.sdk.bridge import PermissionPostureError
from copilotd.storage.database import Database
from copilotd.storage.leases import FenceLost, OwnerLease, OwnerLeaseStore

AgentMode = Literal["interactive", "plan", "autopilot", "shell"]
DeliveryMode = Literal["enqueue", "immediate"]
AttachmentResolver = Callable[[str], Awaitable[list[Any]]]
T = TypeVar("T")


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


class RuntimeBridge(Protocol):
    async def create_session(
        self,
        *,
        session_id: str,
        working_directory: str,
        on_event: Any,
        on_user_input_request: Any,
        on_exit_plan_mode_request: Any,
        on_auto_mode_switch_request: Any,
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
    ) -> SessionHandle: ...

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
        context_tier: str | None,
        reasoning_summary: str | None = None,
    ) -> None: ...

    async def get_current_model(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_context(self, session: SessionHandle) -> dict[str, Any] | None: ...

    async def get_usage(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_readiness(self, session: SessionHandle) -> dict[str, Any]: ...

    async def get_tasks(self, session: SessionHandle) -> list[dict[str, Any]]: ...


class RuntimeState(StrEnum):
    DETACHED = "detached"
    ATTACHING = "attaching"
    READY = "ready"
    DEGRADED = "degraded"
    RECOVERY_UNKNOWN = "recovery_unknown"
    FENCED = "fenced"
    CLOSING = "closing"
    CLOSED = "closed"


class SessionAttachUnknown(RuntimeError):
    pass


class SessionNotReady(RuntimeError):
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
        owner_renew_seconds: float = 20,
        queue_poll_seconds: float = 1,
        attachment_resolver: AttachmentResolver | None = None,
        interaction_timeout_seconds: float = 24 * 60 * 60,
        sdk_operation_timeout_seconds: float = 30,
        shutdown_timeout_seconds: float = 5,
        model_summary_adapter: ModelReasoningSummaryAdapter | None = None,
        task_action_adapter: TaskActionAdapter | None = None,
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
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._model_summary_adapter = model_summary_adapter
        self._task_action_adapter = task_action_adapter

        self.state = RuntimeState.DETACHED
        self._lease: OwnerLease | None = None
        self._handle: SessionHandle | None = None
        self._inbox: ReducerInbox | None = None
        self._ingress: SdkEventIngress | None = None
        self._reducer: EventReducerWorker | None = None
        self._mailbox: CommandMailbox | None = None
        self._tasks = TaskRegistry()
        self._renewal_stop = asyncio.Event()
        self._renewal_task: asyncio.Task[None] | None = None
        self._queue_task: asyncio.Task[None] | None = None
        self._queue_stop = asyncio.Event()
        self._task_reconcile_requested = asyncio.Event()
        self._task_reconcile_stop = asyncio.Event()
        self._task_reconcile_task: asyncio.Task[None] | None = None
        self._task_snapshot_epoch = 0
        self._permission_reconcile_requested = asyncio.Event()
        self._permission_reconcile_stop = asyncio.Event()
        self._permission_reconcile_task: asyncio.Task[None] | None = None
        self._permission_reconcile_epoch = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue_dispatch_lock = asyncio.Lock()
        self._readiness_epoch = 0
        self._volatile_attachments: dict[str, list[Any] | None] = {}
        self._interaction_futures: dict[str, asyncio.Future[dict[str, Any] | str]] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._admission_lock = asyncio.Lock()
        self._accepting_sends = False

    @property
    def handle(self) -> SessionHandle | None:
        return self._handle

    @property
    def inbox(self) -> ReducerInbox | None:
        return self._inbox

    async def attach_create(self) -> None:
        await self._attach(create=True, continue_pending_work=False)

    async def attach_resume(
        self,
        *,
        reactivate: bool = False,
        continue_pending_work: bool = False,
    ) -> None:
        if reactivate and self.binding.binding_intent == BindingIntent.CLOSED:
            self.binding = await self._bindings.activate(self.binding)
        await self._attach(
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
        async with self._admission_lock:
            if not self._accepting_sends:
                raise SessionNotReady("session is not accepting new messages")
            await self._assert_dispatchable()
            effective_agent_mode = agent_mode or self.binding.desired_mode
            if effective_agent_mode not in {"interactive", "plan", "autopilot", "shell"}:
                raise SessionNotReady(f"unsupported message mode: {effective_agent_mode}")
            submission_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:{self.binding.sdk_session_id}:submission:{idempotency_key}",
                )
            )
            prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
            model_row = await self._database.fetchone(
                """
                SELECT desired_model_config, desired_agent, desired_session_config_version
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
                        "attachment_manifest_id": attachment_manifest_id,
                        "requested_mode": effective_agent_mode,
                        "requested_model_config": json.loads(model_row["desired_model_config"]),
                        "requested_agent": model_row["desired_agent"],
                        "requested_session_config_version": model_row[
                            "desired_session_config_version"
                        ],
                        "requested_delivery": mode,
                        "created_at": time.time(),
                    },
                },
                internal_event_id=f"submission:{submission_id}:queued",
            )
            self._volatile_attachments[submission_id] = attachments

        if mode == "immediate":
            return await self._dispatch_submission(
                submission_id=submission_id,
                idempotency_key=idempotency_key,
                prompt=prompt,
                prompt_hash=prompt_hash,
                attachment_manifest_id=attachment_manifest_id,
                attachments=attachments,
                mode=mode,
                agent_mode=effective_agent_mode,
            )
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

    async def _dispatch_next_queued(self) -> tuple[str, str] | None:
        async with self._queue_dispatch_lock:
            if self.state != RuntimeState.READY:
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
                SELECT q.*, s.prompt_hash, s.requested_delivery
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
                       desired_session_config_version,
                       runtime_session_config_version
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
            requested_agent = row["requested_agent_snapshot"]
            observed_agent = None if runtime_model is None else runtime_model["runtime_agent"]
            if observed_agent in {None, "unknown"} and runtime_model is not None:
                observed_agent = runtime_model["desired_agent"]
            if requested_agent is not None and str(observed_agent or "unknown") != str(
                requested_agent
            ):
                await self._block_queue_item(row["id"], "blocked_agent_drift")
                return None
            observed_config_version = (
                None if runtime_model is None else runtime_model["runtime_session_config_version"]
            )
            if observed_config_version is None and runtime_model is not None:
                observed_config_version = runtime_model["desired_session_config_version"]
            if observed_config_version is None or int(observed_config_version) != int(
                row["requested_session_config_version"]
            ):
                await self._block_queue_item(
                    row["id"],
                    "blocked_session_config_drift",
                )
                return None
            attachments = self._volatile_attachments.get(row["id"])
            manifest_id = row["attachment_manifest_id"]
            if (
                attachments is None
                and manifest_id is not None
                and self._attachment_resolver is not None
            ):
                attachments = await self._attachment_resolver(str(manifest_id))
            message_id = await self._dispatch_submission(
                submission_id=str(row["id"]),
                idempotency_key=f"queue:{row['id']}",
                prompt=str(row["prompt"]),
                prompt_hash=str(row["prompt_hash"]),
                attachment_manifest_id=(None if manifest_id is None else str(manifest_id)),
                attachments=attachments,
                mode=cast(DeliveryMode, str(row["requested_delivery"])),
                agent_mode=cast(AgentMode, str(row["requested_mode_snapshot"])),
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
    ) -> str:
        inbox = self._require_inbox()

        async def dispatch() -> str:
            claimed = await self._claim_submission(
                submission_id,
                operation_idempotency_key=f"send:{idempotency_key}",
            )
            if not claimed:
                raise OperationRejected(f"submission {submission_id} was cancelled before dispatch")
            await inbox.commit_internal(
                {
                    "type": "copilotd.submission.submitting",
                    "data": {
                        "submission_id": submission_id,
                        "idempotency_key": idempotency_key,
                    },
                },
                internal_event_id=f"submission:{submission_id}:submitting",
            )
            await self._assert_dispatchable()
            return await self._sdk_call(
                self._require_handle().send(
                    prompt,
                    attachments=attachments,
                    mode=mode,
                    agent_mode=agent_mode,
                )
            )

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
            )
        except OperationRejected:
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

    async def _claim_submission(
        self,
        submission_id: str,
        *,
        operation_idempotency_key: str,
    ) -> bool:
        now = time.time()
        async with self._database.transaction() as connection:
            operation_cursor = await connection.execute(
                """
                SELECT operation_id FROM session_operations
                WHERE sdk_session_id = ? AND idempotency_key = ?
                """,
                (self.binding.sdk_session_id, operation_idempotency_key),
            )
            operation = await operation_cursor.fetchone()
            await operation_cursor.close()
            cursor = await connection.execute(
                """
                UPDATE submissions
                SET state = 'submitting', source_operation_id = ?
                WHERE submission_id = ? AND sdk_session_id = ?
                  AND state = 'local_queued'
                """,
                (
                    None if operation is None else str(operation["operation_id"]),
                    submission_id,
                    self.binding.sdk_session_id,
                ),
            )
            claimed = cursor.rowcount == 1
            await cursor.close()
            if not claimed:
                return False
            queue_cursor = await connection.execute(
                """
                UPDATE message_queue SET state = 'submitting', updated_at = ?
                WHERE id = ? AND thread_id = ? AND state = 'local_queued'
                """,
                (now, submission_id, self.binding.thread_id),
            )
            queue_claimed = queue_cursor.rowcount == 1
            await queue_cursor.close()
            if not queue_claimed:
                raise RuntimeError(
                    f"submission queue state diverged while claiming {submission_id}"
                )
        return True

    async def _block_queue_item(self, submission_id: str, state: str) -> None:
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.queue.blocked",
                "data": {"submission_id": submission_id, "state": state},
            },
            internal_event_id=f"queue:{submission_id}:{state}",
        )

    async def _refresh_readiness(self) -> dict[str, Any]:
        await self._assert_owned_handle()
        self._readiness_epoch += 1
        epoch = self._readiness_epoch
        try:
            snapshot = await self._bridge.get_readiness(self._require_handle())
        except Exception:
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.readiness.failed",
                    "data": {"epoch": epoch},
                },
                source="snapshot",
                internal_event_id=f"readiness:{self.binding.runtime_generation}:{epoch}:failed",
            )
            raise
        observed = {
            "processing": bool(snapshot.get("processing")),
            "hasActiveWork": bool(snapshot.get("hasActiveWork")),
            "abortable": bool(snapshot.get("abortable")),
            "pendingItems": list(snapshot.get("pendingItems") or []),
            "steeringMessages": list(snapshot.get("steeringMessages") or []),
        }
        await self._require_inbox().commit_internal(
            {
                "type": "copilotd.readiness.observed",
                "data": {
                    "epoch": epoch,
                    "processing": observed["processing"],
                    "has_active_work": observed["hasActiveWork"],
                    "abortable": observed["abortable"],
                    "native_queue_count": len(observed["pendingItems"]),
                    "native_steering_count": len(observed["steeringMessages"]),
                    "observed_at": time.time(),
                },
            },
            source="snapshot",
            internal_event_id=f"readiness:{self.binding.runtime_generation}:{epoch}",
        )
        return observed

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
            requested = asyncio.create_task(self._task_reconcile_requested.wait())
            stopped = asyncio.create_task(self._task_reconcile_stop.wait())
            done, pending = await asyncio.wait(
                {requested, stopped},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if stopped in done and self._task_reconcile_stop.is_set():
                return
            self._task_reconcile_requested.clear()
            self._task_snapshot_epoch += 1
            epoch = self._task_snapshot_epoch
            try:
                tasks = await self._bridge.get_tasks(self._require_handle())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._require_inbox().commit_internal(
                    {
                        "type": "copilotd.tasks.snapshot_failed",
                        "data": {
                            "epoch": epoch,
                            "error_type": type(error).__name__,
                        },
                    },
                    source="snapshot",
                    internal_event_id=(f"tasks:{self.binding.runtime_generation}:{epoch}:failed"),
                )
                continue
            await self._require_inbox().commit_internal(
                {
                    "type": "copilotd.tasks.snapshot",
                    "data": {
                        "epoch": epoch,
                        "tasks": tasks,
                        "observed_at": time.time(),
                    },
                },
                source="snapshot",
                internal_event_id=f"tasks:{self.binding.runtime_generation}:{epoch}",
            )

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
        if raw_type == "session.background_tasks_changed" and loop is not None:
            loop.call_soon_threadsafe(self._task_reconcile_requested.set)
        if raw_type == "session.permissions_changed" and loop is not None:
            loop.call_soon_threadsafe(self._permission_reconcile_requested.set)

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
            if (
                self._model_summary_adapter is None
                or not self._model_summary_adapter.supports_reasoning_summary(model)
            ):
                raise CDCapabilityError(
                    f"{model} does not expose confirmed reasoning-summary readback"
                )

        target = {
            "modelId": model,
            "reasoningEffort": reasoning_effort,
            "contextTier": context_tier,
        }
        if reasoning_summary is not None:
            target["reasoningSummary"] = reasoning_summary
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
            if reasoning_summary is None:
                await self._sdk_call(
                    self._bridge.set_model(
                        handle,
                        model=model,
                        reasoning_effort=reasoning_effort,
                        context_tier=context_tier,
                    )
                )
            else:
                await self._sdk_call(
                    self._bridge.set_model(
                        handle,
                        model=model,
                        reasoning_effort=reasoning_effort,
                        context_tier=context_tier,
                        reasoning_summary=reasoning_summary,
                    )
                )
            observed = await self._sdk_call(self._bridge.get_current_model(handle))
            if reasoning_summary is not None and self._model_summary_adapter is not None:
                readback = await self._model_summary_adapter.read_current_model(
                    session_id=self.binding.sdk_session_id
                )
                if readback is not None:
                    observed = {**observed, **dict(readback)}
            if observed.get("modelId") != model:
                raise RuntimeError(
                    f"model reconciliation returned {observed.get('modelId')}; expected {model}"
                )
            if reasoning_effort is not None and observed.get("reasoningEffort") != reasoning_effort:
                raise RuntimeError("model reasoning effort could not be confirmed")
            observed_tier = observed.get("contextTier")
            if context_tier is not None and observed_tier != context_tier:
                raise RuntimeError("model context tier could not be confirmed")
            if (
                reasoning_summary is not None
                and observed.get("reasoningSummary") != reasoning_summary
            ):
                raise RuntimeError("model reasoning summary could not be confirmed")
            return observed

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
        return await self._projection_snapshot(
            "context",
            self._bridge.get_context(self._require_handle()),
            allow_none=True,
        )

    async def usage_snapshot(self) -> dict[str, Any]:
        await self._assert_owned_handle()
        snapshot = await self._projection_snapshot(
            "usage",
            self._bridge.get_usage(self._require_handle()),
            allow_none=False,
        )
        return {} if snapshot is None else snapshot

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

    async def _request_interaction(
        self,
        kind: Literal["user_input", "exit_plan_mode", "auto_mode_switch"],
        request: dict[str, Any],
    ) -> dict[str, Any] | str:
        await self._assert_owned_handle()
        interaction_id = str(uuid.uuid4())
        now = time.time()
        expires_at = now + self._interaction_timeout_seconds
        payload: dict[str, Any] = {
            "interaction_id": interaction_id,
            "thread_id": self.binding.thread_id,
            "kind": kind,
            "state": "pending",
            "expires_at": expires_at,
        }
        payload.update(request)
        if kind == "exit_plan_mode":
            payload["choices"] = list(request.get("actions", []))
        if kind == "auto_mode_switch":
            payload["choices"] = ["yes", "yes_always", "no"]
            retry_after = request.get("retryAfterSeconds")
            suffix = "" if retry_after is None else f" Retry after {retry_after} seconds."
            payload["question"] = (
                f"Copilot reached an eligible rate limit. Switch to Auto mode?{suffix}"
            )
        future: asyncio.Future[dict[str, Any] | str] = asyncio.get_running_loop().create_future()
        self._interaction_futures[interaction_id] = future
        await self._require_inbox().commit_internal(
            {"type": "copilotd.interaction.requested", "data": payload},
            internal_event_id=f"interaction:{interaction_id}:requested",
        )
        try:
            return await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self._interaction_timeout_seconds,
            )
        except TimeoutError:
            fallback = self._interaction_fallback(kind)
            _claimed, settled_response = await self._settle_interaction(
                interaction_id,
                response=fallback,
                display_response="Request timed out.",
                state="expired",
            )
            return settled_response
        except asyncio.CancelledError:
            fallback = self._interaction_fallback(kind)
            await asyncio.shield(
                self._settle_interaction(
                    interaction_id,
                    response=fallback,
                    display_response="Request cancelled because the handler stopped.",
                    state="expired",
                )
            )
            raise
        finally:
            self._interaction_futures.pop(interaction_id, None)

    async def respond_interaction(
        self,
        interaction_id: str,
        *,
        selection: int | None = None,
        freeform: str | None = None,
    ) -> Literal["resolved", "expired", "invalid"]:
        row = await self._database.fetchone(
            """
            SELECT kind, expires_at, state, payload, runtime_generation,
                   owner_fence_token
            FROM pending_interactions
            WHERE interaction_id = ? AND sdk_session_id = ?
            """,
            (interaction_id, self.binding.sdk_session_id),
        )
        if row is None:
            return "invalid"
        if (
            row["state"] != "pending"
            or float(row["expires_at"]) <= time.time()
            or int(row["runtime_generation"]) != self.binding.runtime_generation
            or int(row["owner_fence_token"]) != self.binding.owner_fence_token
        ):
            return "expired"
        future = self._interaction_futures.get(interaction_id)
        if future is None or future.done():
            return "expired"
        payload = json.loads(str(row["payload"]))
        kind = str(row["kind"])
        response: dict[str, Any] | str
        display_response: str
        if freeform is not None:
            answer = freeform.strip()
            choices = [str(choice) for choice in payload.get("choices", [])]
            choice_fallback = len(choices) > 25 and answer in choices
            if (
                kind != "user_input"
                or not answer
                or (not payload.get("allowFreeform") and not choice_fallback)
            ):
                return "invalid"
            response = {
                "answer": answer,
                "wasFreeform": not choice_fallback,
            }
            display_response = answer
        else:
            choices = payload.get("choices", [])
            if not isinstance(selection, int) or not 0 <= selection < len(choices):
                return "invalid"
            answer = str(choices[selection])
            if kind == "user_input":
                response = {"answer": answer, "wasFreeform": False}
            elif kind == "exit_plan_mode":
                if answer not in payload.get("actions", []):
                    return "invalid"
                response = {"approved": True, "selectedAction": answer}
            elif kind == "auto_mode_switch":
                if answer not in {"yes", "yes_always", "no"}:
                    return "invalid"
                response = answer
            else:
                return "invalid"
            display_response = answer
        claimed, _settled_response = await self._settle_interaction(
            interaction_id,
            response=response,
            display_response=display_response,
            state="resolved",
        )
        return "resolved" if claimed else "expired"

    async def _settle_interaction(
        self,
        interaction_id: str,
        *,
        response: dict[str, Any] | str,
        display_response: str,
        state: Literal["resolved", "expired"],
    ) -> tuple[bool, dict[str, Any] | str]:
        future = self._interaction_futures.get(interaction_id)
        encoded_response = json.dumps(response, ensure_ascii=False, sort_keys=True)
        target_mode = interaction_target_mode(response) if state == "resolved" else None
        now = time.time()
        expiry_predicate = "AND expires_at > ?" if state == "resolved" else ""
        parameters: tuple[Any, ...] = (
            state,
            encoded_response,
            target_mode,
            now,
            interaction_id,
            self.binding.sdk_session_id,
            self.binding.runtime_generation,
            self.binding.owner_fence_token,
        )
        if state == "resolved":
            parameters = (*parameters, now)
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                f"""
                UPDATE pending_interactions
                SET state = ?, response = ?, target_mode = ?, updated_at = ?
                WHERE interaction_id = ? AND sdk_session_id = ?
                  AND runtime_generation = ? AND owner_fence_token = ?
                  AND state = 'pending' {expiry_predicate}
                """,
                parameters,
            )
            claimed = cursor.rowcount == 1
            await cursor.close()
            if claimed:
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'released', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id = ? AND lease_id = ?
                      AND runtime_generation = ? AND owner_fence_token = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        now,
                        self.binding.sdk_session_id,
                        f"interaction:{interaction_id}",
                        self.binding.runtime_generation,
                        self.binding.owner_fence_token,
                    ),
                )
            row_cursor = await connection.execute(
                """
                SELECT kind, response FROM pending_interactions
                WHERE interaction_id = ? AND sdk_session_id = ?
                """,
                (interaction_id, self.binding.sdk_session_id),
            )
            row = await row_cursor.fetchone()
            await row_cursor.close()
        settled_response = response
        kind = "interaction"
        if row is not None:
            kind = str(row["kind"])
            if row["response"] is not None:
                settled_response = cast(
                    dict[str, Any] | str,
                    json.loads(str(row["response"])),
                )
        if claimed:
            try:
                await self._require_inbox().commit_internal(
                    {
                        "type": f"copilotd.interaction.{state}",
                        "data": {
                            "interaction_id": interaction_id,
                            "kind": kind,
                            "state": state,
                            "response": response,
                            "display_response": display_response,
                            "target_mode": target_mode,
                        },
                    },
                    internal_event_id=f"interaction:{interaction_id}:{state}",
                )
            finally:
                if future is not None and not future.done():
                    future.set_result(response)
        elif row is not None and row["response"] is not None:
            if future is not None and not future.done():
                future.set_result(settled_response)
        return claimed, settled_response

    async def _interaction_kind(self, interaction_id: str) -> str:
        row = await self._database.fetchone(
            "SELECT kind FROM pending_interactions WHERE interaction_id = ?",
            (interaction_id,),
        )
        return "interaction" if row is None else str(row["kind"])

    @staticmethod
    def _interaction_fallback(kind: str) -> dict[str, Any] | str:
        if kind == "user_input":
            return {
                "answer": "No response was provided before the request expired.",
                "wasFreeform": True,
            }
        if kind == "exit_plan_mode":
            return {"approved": False}
        return "no"

    async def cancel_pending_interactions(self, *, reason: str) -> int:
        pending = [
            (interaction_id, future)
            for interaction_id, future in self._interaction_futures.items()
            if not future.done()
        ]
        settled = 0
        for interaction_id, _future in pending:
            kind = await self._interaction_kind(interaction_id)
            claimed, _response = await self._settle_interaction(
                interaction_id,
                response=self._interaction_fallback(kind),
                display_response=reason,
                state="expired",
            )
            settled += int(claimed)
        return settled

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
        await self._assert_owned_handle()
        allowed_states = {
            "blocked_mode_drift",
            "blocked_model_drift",
            "blocked_agent_drift",
            "blocked_session_config_drift",
        }
        now = time.time()
        replacement_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"copilotd:{self.binding.sdk_session_id}:queue-resubmit:"
                    f"{submission_id}:{idempotency_key}"
                ),
            )
        )
        async with self._queue_dispatch_lock:
            async with self._database.transaction() as connection:
                cursor = await connection.execute(
                    """
                    SELECT q.*, s.origin, s.runtime_schedule_id
                    FROM message_queue AS q
                    JOIN submissions AS s ON s.submission_id = q.id
                    WHERE q.id = ? AND q.thread_id = ?
                    """,
                    (submission_id, self.binding.thread_id),
                )
                source = await cursor.fetchone()
                await cursor.close()
                if source is None:
                    raise CDInputError(f"queue item not found: {submission_id}")
                if str(source["state"]) not in allowed_states:
                    raise CDSessionStateError(
                        "only configuration-drift queue items can be resubmitted"
                    )
                cursor = await connection.execute(
                    """
                    SELECT desired_mode, pending_mode, runtime_mode,
                           desired_model_config, pending_model_config,
                           runtime_model_config, desired_agent, pending_agent,
                           runtime_agent, desired_session_config_version,
                           pending_session_config_version,
                           runtime_session_config_version
                    FROM session_bindings WHERE thread_id = ?
                    """,
                    (self.binding.thread_id,),
                )
                config = await cursor.fetchone()
                await cursor.close()
                if config is None:
                    raise CDSessionStateError("session configuration is unavailable")
                if any(
                    config[key] is not None
                    for key in (
                        "pending_mode",
                        "pending_model_config",
                        "pending_agent",
                        "pending_session_config_version",
                    )
                ):
                    raise CDSessionStateError("session configuration is still transitioning")
                runtime_mode = str(config["runtime_mode"])
                if runtime_mode == "unknown":
                    raise CDSessionStateError("runtime mode is not confirmed")
                model_config = config["runtime_model_config"]
                if model_config is None:
                    model_config = config["desired_model_config"]
                runtime_agent = str(config["runtime_agent"])
                if runtime_agent == "unknown":
                    runtime_agent = str(config["desired_agent"])
                session_config_version = (
                    config["runtime_session_config_version"]
                    if config["runtime_session_config_version"] is not None
                    else config["desired_session_config_version"]
                )
                position_cursor = await connection.execute(
                    """
                    SELECT COALESCE(MAX(position), 0) + 1
                    FROM message_queue WHERE thread_id = ?
                    """,
                    (self.binding.thread_id,),
                )
                position = int((await position_cursor.fetchone())[0])
                await position_cursor.close()
                schedule_run_id = source["schedule_run_id"]
                await connection.execute(
                    """
                    UPDATE message_queue
                    SET state = 'cancelled', updated_at = ?
                    WHERE id = ?
                    """,
                    (now, submission_id),
                )
                await connection.execute(
                    """
                    UPDATE submissions
                    SET state = 'cancelled', schedule_run_id = NULL
                    WHERE submission_id = ?
                    """,
                    (submission_id,),
                )
                await connection.execute(
                    """
                    INSERT INTO submissions(
                        submission_id, sdk_session_id, origin, parent_submission_id,
                        schedule_run_id, runtime_schedule_id, attachment_manifest_id,
                        prompt_hash, requested_mode, requested_model_config,
                        requested_agent, requested_session_config_version,
                        requested_delivery, state, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'enqueue',
                              'local_queued', ?)
                    """,
                    (
                        replacement_id,
                        self.binding.sdk_session_id,
                        str(source["origin"]),
                        submission_id,
                        schedule_run_id,
                        source["runtime_schedule_id"],
                        source["attachment_manifest_id"],
                        hashlib.sha256(str(source["prompt"]).encode()).hexdigest(),
                        runtime_mode,
                        str(model_config),
                        runtime_agent,
                        int(session_config_version),
                        now,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO message_queue(
                        id, thread_id, discord_message_id, schedule_run_id, prompt,
                        attachment_manifest_id, requested_mode_snapshot,
                        requested_model_config_snapshot, requested_agent_snapshot,
                        requested_session_config_version, position, state,
                        replaces_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued',
                              ?, ?, ?)
                    """,
                    (
                        replacement_id,
                        self.binding.thread_id,
                        source["discord_message_id"],
                        schedule_run_id,
                        str(source["prompt"]),
                        source["attachment_manifest_id"],
                        runtime_mode,
                        str(model_config),
                        runtime_agent,
                        int(session_config_version),
                        position,
                        submission_id,
                        now,
                        now,
                    ),
                )
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'released', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id = ? AND kind = 'submission'
                      AND source_id = ? AND state = 'active'
                    """,
                    (now, now, self.binding.sdk_session_id, submission_id),
                )
                await connection.execute(
                    """
                    INSERT INTO liveness_leases(
                        sdk_session_id, lease_id, kind, source_id,
                        runtime_generation, owner_fence_token, state,
                        acquired_at, refreshed_at
                    ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        self.binding.sdk_session_id,
                        f"submission:{replacement_id}",
                        replacement_id,
                        self.binding.runtime_generation,
                        self.binding.owner_fence_token,
                        now,
                        now,
                    ),
                )
        return replacement_id

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
        if action == "cancel":
            if state not in {"running", "idle"}:
                raise CDSessionStateError("only running or idle tasks can be cancelled")
            result = await self._task_action_adapter.cancel_task(
                session_id=self.binding.sdk_session_id,
                task_id=task_id,
            )
        elif action == "promote":
            if state not in {"running", "idle"} or not bool(card["can_promote"]):
                raise CDSessionStateError("this task cannot be promoted")
            result = await self._task_action_adapter.promote_task(
                session_id=self.binding.sdk_session_id,
                task_id=task_id,
            )
        elif action == "message":
            if str(card["kind"]) != "agent" or state not in {"running", "idle"}:
                raise CDSessionStateError("only active agent tasks accept messages")
            normalized = "" if message is None else message.strip()
            if not normalized:
                raise CDInputError("task message cannot be empty")
            result = await self._task_action_adapter.message_task(
                session_id=self.binding.sdk_session_id,
                task_id=task_id,
                message=normalized,
            )
        else:
            if state not in {"completed", "failed", "cancelled"}:
                raise CDSessionStateError("only terminal tasks can be removed")
            result = await self._task_action_adapter.remove_task(
                session_id=self.binding.sdk_session_id,
                task_id=task_id,
            )
            await self._database.execute(
                """
                DELETE FROM task_card_projections
                WHERE sdk_session_id = ? AND panel_id = ? AND card_token = ?
                """,
                (self.binding.sdk_session_id, panel_id, card_token),
            )
        await self.refresh_taskdeck(interaction_id=interaction_id)
        return {"status": "updated", "result": dict(result)}

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
        )
        placeholders = ", ".join("?" for _ in cancellable)
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                f"""
                SELECT id, schedule_run_id FROM message_queue
                WHERE thread_id = ? AND {predicate}
                  AND state IN ({placeholders})
                """,
                (self.binding.thread_id, *parameters, *cancellable),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            submission_ids = [str(row["id"]) for row in rows]
            schedule_run_ids = [
                str(row["schedule_run_id"]) for row in rows if row["schedule_run_id"] is not None
            ]
            if not submission_ids:
                return 0
            ids = ", ".join("?" for _ in submission_ids)
            await connection.execute(
                f"""
                UPDATE message_queue SET state = 'cancelled', updated_at = ?
                WHERE id IN ({ids})
                """,
                (now, *submission_ids),
            )
            await connection.execute(
                f"""
                UPDATE submissions SET state = 'cancelled'
                WHERE submission_id IN ({ids}) AND state = 'local_queued'
                """,
                tuple(submission_ids),
            )
            await connection.execute(
                f"""
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND kind = 'submission'
                  AND source_id IN ({ids}) AND state = 'active'
                """,
                (now, now, self.binding.sdk_session_id, *submission_ids),
            )
            if schedule_run_ids:
                run_ids = ", ".join("?" for _ in schedule_run_ids)
                await connection.execute(
                    f"""
                    UPDATE schedule_runs
                    SET status = 'cancelled', updated_at = ?
                    WHERE run_id IN ({run_ids}) AND status = 'queued_local'
                    """,
                    (now, *schedule_run_ids),
                )
        for submission_id in submission_ids:
            self._volatile_attachments.pop(submission_id, None)
        return len(submission_ids)

    async def abort(self, *, idempotency_key: str) -> None:
        await self._assert_owned_handle()
        await self.cancel_pending_interactions(reason="Cancelled by session abort.")
        mailbox = self._require_mailbox()

        async def dispatch() -> None:
            await self._assert_owned_handle()
            await self._sdk_call(self._require_handle().abort())

        await mailbox.submit(
            kind="abort",
            idempotency_key=f"abort:{idempotency_key}",
            input_payload={},
            operation=dispatch,
        )

    async def close(
        self,
        *,
        idempotency_key: str,
        force: bool = False,
    ) -> None:
        async with self._lifecycle_lock:
            await self._assert_owned_handle()
            async with self._admission_lock:
                self._accepting_sends = False
                try:
                    blockers = await self.detach_blockers()
                except BaseException:
                    self._accepting_sends = True
                    raise
                if blockers and not force:
                    self._accepting_sends = True
                    raise DetachBlocked(blockers)
            if force and blockers:
                await self.clear_queue()
                await self.cancel_pending_interactions(reason="Cancelled by forced session close.")
                await self._force_active_unknown()
                try:
                    await self.abort(idempotency_key=f"{idempotency_key}:force")
                except (OperationAmbiguous, OperationRejected):
                    pass

            inbox = self._require_inbox()
            await inbox.join()
            current = await self._bindings.by_thread(self.binding.thread_id)
            if current is None:
                raise SessionNotReady("session binding disappeared before close")
            self.binding = await self._bindings.begin_close(current)
            self.state = RuntimeState.CLOSING
            mailbox = self._require_mailbox()

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

    async def detach_blockers(self) -> list[str]:
        binding = await self._bindings.by_thread(self.binding.thread_id)
        if binding is None:
            return ["binding_missing"]
        blockers: list[str] = []
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
            WHERE thread_id = ? AND state NOT IN ('cancelled', 'submitted', 'failed')
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
        return blockers

    async def shutdown(self) -> None:
        """Stop app workers without claiming that an in-flight SDK operation succeeded."""
        async with self._lifecycle_lock:
            if self.state == RuntimeState.CLOSED:
                return
            async with self._admission_lock:
                self._accepting_sends = False
            if self._mailbox is not None:
                self._mailbox.freeze()
            await self._force_active_unknown()
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
                self.binding = await self._bindings.mark_recovery_unknown(current)
                self.state = RuntimeState.RECOVERY_UNKNOWN
            await self._stop_components(release_owner=True)
            if self.state not in {RuntimeState.RECOVERY_UNKNOWN, RuntimeState.FENCED}:
                self.state = RuntimeState.DETACHED

    async def _attach(self, *, create: bool, continue_pending_work: bool) -> None:
        async with self._lifecycle_lock:
            if self.state != RuntimeState.DETACHED:
                raise SessionNotReady(f"runtime cannot attach from state {self.state}")
            self.state = RuntimeState.ATTACHING
            self._lease = await self._owner_leases.acquire(
                self.binding.sdk_session_id,
                self._owner_id,
            )
            self.binding = await self._bindings.begin_attachment(
                thread_id=self.binding.thread_id,
                lease=self._lease,
                state=AttachmentState.CREATING if create else AttachmentState.RESUMING,
            )
            self._start_components()

            try:
                if create:
                    handle = await self._sdk_call(
                        self._bridge.create_session(
                            session_id=self.binding.sdk_session_id,
                            working_directory=str(self.binding.cwd_snapshot),
                            on_event=self._ingress,
                            on_user_input_request=self._handle_user_input_request,
                            on_exit_plan_mode_request=self._handle_exit_plan_mode_request,
                            on_auto_mode_switch_request=self._handle_auto_mode_switch_request,
                        )
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
                        )
                    )
                if handle.session_id != self.binding.sdk_session_id:
                    raise RuntimeError("SDK returned a different session ID")
            except Exception as error:
                self.binding = await self._bindings.mark_attach_unknown(self.binding)
                self.state = RuntimeState.RECOVERY_UNKNOWN
                raise SessionAttachUnknown(
                    f"session {self.binding.sdk_session_id} attachment is unknown"
                ) from error

            self._handle = handle
            try:
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
            observed_binding = await self._bindings.by_thread(self.binding.thread_id)
            if observed_binding is None:
                raise SessionNotReady("session binding disappeared during mode reconciliation")
            self.binding = observed_binding
            self._mailbox = CommandMailbox(
                store=OperationStore(self._database),
                sdk_session_id=self.binding.sdk_session_id,
                runtime_generation=self.binding.runtime_generation,
                owner_fence_token=self._require_fence_token(),
                fence_validator=self._is_current_owner,
            )
            self._mailbox.start()
            self.state = RuntimeState.READY
            async with self._admission_lock:
                self._accepting_sends = True
            self._queue_stop.clear()
            self._queue_task = self._tasks.create(
                self._queue_pump(),
                name=f"queue-pump:{self.binding.sdk_session_id}",
            )
            self._task_reconcile_stop.clear()
            self._task_reconcile_requested.set()
            self._task_reconcile_task = self._tasks.create(
                self._task_reconcile_loop(),
                name=f"task-reconcile:{self.binding.sdk_session_id}",
            )
            self._permission_reconcile_stop.clear()
            self._permission_reconcile_task = self._tasks.create(
                self._permission_reconcile_loop(),
                name=f"permission-reconcile:{self.binding.sdk_session_id}",
            )

    def _start_components(self) -> None:
        self._loop = asyncio.get_running_loop()
        fence_token = self._require_fence_token()
        self._inbox = ReducerInbox(
            sdk_session_id=self.binding.sdk_session_id,
            generation=self.binding.runtime_generation,
            fence_token=fence_token,
            capacity=self._ingress_capacity,
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
            reducer=JournalReducer(self._database),
            batch_size=self._reducer_batch_size,
            fence_validator=validate,
        )
        self._reducer.start()
        self._renewal_stop.clear()
        self._renewal_task = self._tasks.create(
            self._renew_owner(),
            name=f"owner-renew:{self.binding.sdk_session_id}",
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
        await self._assert_owned_handle()
        binding = await self._bindings.by_thread(self.binding.thread_id)
        if binding is None:
            raise SessionNotReady("session binding no longer exists")
        self.binding = binding
        if binding.attachment_state != AttachmentState.ATTACHED:
            raise SessionNotReady(f"session attachment is {binding.attachment_state}")
        if binding.permission_posture != PermissionPosture.VERIFIED_ALLOW_ALL:
            raise SessionNotReady(f"permission posture is {binding.permission_posture}")
        if binding.pending_mode is not None:
            raise SessionNotReady(f"mode transition is pending: {binding.pending_mode}")
        if binding.runtime_mode != binding.desired_mode:
            raise SessionNotReady(
                f"mode drift: desired={binding.desired_mode}, runtime={binding.runtime_mode}"
            )
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

    async def _assert_owned_handle(self, *, allow_closing: bool = False) -> None:
        allowed = {RuntimeState.READY}
        if allow_closing:
            allowed.add(RuntimeState.CLOSING)
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

    async def _force_active_unknown(self) -> None:
        now = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'outcome_unknown'
                WHERE sdk_session_id = ?
                  AND state NOT IN (
                    'rejected', 'semantic_complete', 'semantic_blocked',
                    'observed_aborted', 'outcome_unknown'
                  )
                """,
                (self.binding.sdk_session_id,),
            )
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'orphaned', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND state = 'active'
                  AND runtime_generation = ? AND owner_fence_token = ?
                """,
                (
                    now,
                    now,
                    self.binding.sdk_session_id,
                    self.binding.runtime_generation,
                    self.binding.owner_fence_token,
                ),
            )

    async def _sdk_call(self, operation: Awaitable[T]) -> T:
        async with asyncio.timeout(self._sdk_operation_timeout_seconds):
            return await operation

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

    async def _stop_components(self, *, release_owner: bool) -> None:
        self._accepting_sends = False
        errors: list[Exception] = []
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
                await self._mailbox.stop(timeout_seconds=self._shutdown_timeout_seconds)
            except Exception as error:
                errors.append(error)
            self._mailbox = None
        self._renewal_stop.set()
        if self._renewal_task is not None:
            await self._cancel_component_task(self._renewal_task)
            self._renewal_task = None
        if self._reducer is not None:
            try:
                await self._reducer.stop(timeout_seconds=self._shutdown_timeout_seconds)
            except Exception as error:
                errors.append(error)
            self._reducer = None
        self._inbox = None
        self._ingress = None
        self._loop = None
        self._handle = None
        lease = self._lease
        self._lease = None
        if release_owner and lease is not None:
            try:
                await self._owner_leases.release(lease)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("session component shutdown failed", errors)

    def _require_handle(self) -> SessionHandle:
        if self._handle is None:
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

    def _require_fence_token(self) -> int:
        if self.binding.owner_fence_token is None:
            raise SessionNotReady("session binding has no owner fence")
        return self.binding.owner_fence_token


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _model_config_matches(
    desired: dict[str, Any],
    runtime: dict[str, Any] | None,
) -> bool:
    if runtime is None:
        return False
    return all(value is None or runtime.get(key) == value for key, value in desired.items())
