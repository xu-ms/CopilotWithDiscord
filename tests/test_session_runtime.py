import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from copilot.generated.session_events import SessionMode
from copilot.session_events import (
    AssistantMessageDeltaData,
    PendingMessagesModifiedData,
    SessionBackgroundTasksChangedData,
    SessionEvent,
    SessionEventType,
    SessionIdleData,
    SessionModeChangedData,
    SessionPermissionsChangedData,
    UserMessageData,
)

from copilotd.config import Settings
from copilotd.core.attachments import AttachmentError
from copilotd.core.bindings import (
    AttachmentState,
    BindingConflict,
    BindingIntent,
    PermissionPosture,
    SessionBindingRepository,
)
from copilotd.core.commands import CDSessionStateError
from copilotd.core.mailbox import OperationAmbiguous, OperationRejected
from copilotd.core.session_runtime import (
    DetachBlocked,
    RuntimeState,
    SessionAttachUnknown,
    SessionNotReady,
    SessionOwnerConflict,
    SessionRuntime,
    SubmissionClaimDeferred,
)
from copilotd.sdk.bridge import EventLogBatch, PermissionPostureError
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.sdk.native import NativeCommandDefinition
from copilotd.storage.database import Database
from copilotd.storage.leases import FenceLost, OwnerConflict, OwnerLeaseStore


class FakeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.message_id = str(uuid4())
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.abort_calls = 0
        self.disconnect_calls = 0
        self.events: list[Any] = []

    async def send(self, prompt: str, **kwargs: Any) -> str:
        self.sent.append((prompt, kwargs))
        self.message_id = str(uuid4())
        return self.message_id

    async def abort(self) -> None:
        self.abort_calls += 1

    async def disconnect(self) -> None:
        self.disconnect_calls += 1

    async def get_events(self) -> list[Any]:
        return list(self.events)


class FakeBridge:
    def __init__(self, session_id: str, *, fail_resume: bool = False) -> None:
        self.handle = FakeHandle(session_id)
        self.fail_resume = fail_resume
        self.create_calls = 0
        self.resume_calls = 0
        self.create_kwargs: dict[str, Any] = {}
        self.resume_kwargs: dict[str, Any] = {}
        self.ingress: Any = None
        self.mode = "interactive"
        self.mode_set_calls: list[str] = []
        self.model = {
            "modelId": "gpt-test",
            "reasoningEffort": None,
            "contextTier": None,
        }
        self.model_set_calls: list[dict[str, Any]] = []
        self.processing = False
        self.has_active_work = False
        self.pending_items: list[dict[str, Any]] = []
        self.steering_messages: list[str] = []
        self.tasks: list[dict[str, Any]] = []
        self.task_snapshot_calls = 0
        self.readiness_snapshot_calls = 0
        self.allow_all_calls = 0
        self.permission_error: Exception | None = None
        self.in_use = False
        self.tail_cursor = "tail-0"
        self.event_log_batches: list[EventLogBatch] = []
        self.event_log_reads = 0

    async def create_session(self, **kwargs: Any) -> FakeHandle:
        self.create_calls += 1
        self.create_kwargs = kwargs
        self.ingress = kwargs["on_event"]
        self.ingress(_message_delta())
        return self.handle

    async def resume_session(self, session_id: str, **kwargs: Any) -> FakeHandle:
        self.resume_calls += 1
        self.resume_kwargs = kwargs
        self.ingress = kwargs["on_event"]
        if self.fail_resume:
            raise ConnectionError("resume transport lost")
        assert session_id == self.handle.session_id
        return self.handle

    async def ensure_allow_all(self, _session: FakeHandle) -> object:
        self.allow_all_calls += 1
        if self.permission_error is not None:
            raise self.permission_error
        return object()

    async def get_mode(self, _session: FakeHandle) -> str:
        return self.mode

    async def set_mode(self, _session: FakeHandle, mode: str) -> None:
        self.mode = mode
        self.mode_set_calls.append(mode)

    async def list_models(self) -> list[dict[str, Any]]:
        return [
            {
                "id": "gpt-test",
                "name": "GPT Test",
                "capabilities": {"supports": {"reasoningEffort": True}},
                "supportedReasoningEfforts": ["low", "high"],
            }
        ]

    async def set_model(
        self,
        _session: FakeHandle,
        *,
        model: str,
        reasoning_effort: str | None,
        context_tier: str | None,
        reasoning_summary: str | None = None,
    ) -> None:
        self.model = {
            "modelId": model,
            "reasoningEffort": reasoning_effort,
            "contextTier": context_tier,
        }
        if reasoning_summary is not None:
            self.model["reasoningSummary"] = reasoning_summary
        self.model_set_calls.append(self.model)

    async def get_current_model(self, _session: FakeHandle) -> dict[str, Any]:
        return self.model

    async def get_context(self, _session: FakeHandle) -> dict[str, Any]:
        return {"totalTokens": 10, "limit": 100}

    async def get_usage(self, _session: FakeHandle) -> dict[str, Any]:
        return {"totalUserRequests": 2}

    async def get_readiness(self, _session: FakeHandle) -> dict[str, Any]:
        self.readiness_snapshot_calls += 1
        return {
            "processing": self.processing,
            "hasActiveWork": self.has_active_work,
            "abortable": self.processing,
            "pendingItems": self.pending_items,
            "steeringMessages": self.steering_messages,
        }

    async def get_tasks(self, _session: FakeHandle) -> list[dict[str, Any]]:
        self.task_snapshot_calls += 1
        return self.tasks

    async def check_session_in_use(self, _session_id: str) -> bool:
        return self.in_use

    async def tail_event_log(self, _session: FakeHandle) -> str:
        return self.tail_cursor

    async def read_event_log(
        self,
        _session: FakeHandle,
        *,
        cursor: str | None,
        max_events: int = 500,
        wait_ms: int = 0,
        include_ephemeral: bool = False,
    ) -> EventLogBatch:
        del cursor, max_events, wait_ms
        assert not include_ephemeral
        self.event_log_reads += 1
        if self.event_log_batches:
            return self.event_log_batches.pop(0)
        return EventLogBatch(
            cursor=f"cursor-{self.event_log_reads}",
            cursor_status="ok",
            events=(),
            has_more=False,
            filtered_ephemeral=0,
        )

    async def get_native_schedules(self, _session: FakeHandle) -> list[dict[str, Any]]:
        return []

    async def get_remote_state(self, _session: FakeHandle) -> dict[str, Any]:
        return {"mode": "off", "url": None}

    async def disable_remote(self, _session: FakeHandle) -> None:
        return None

    async def get_current_agent(self, _session: FakeHandle) -> str:
        return "default"

    async def list_agents(self, _session: FakeHandle) -> list[dict[str, Any]]:
        return []

    async def get_current_agent_info(
        self,
        _session: FakeHandle,
    ) -> dict[str, Any] | None:
        return None

    async def list_commands(
        self,
        _session: FakeHandle,
        *,
        include_builtins: bool,
    ) -> tuple[NativeCommandDefinition, ...]:
        assert include_builtins
        return ()


class FakeSummaryAdapter:
    def supports_reasoning_summary(self, model_id: str) -> bool:
        return model_id == "gpt-test"

    async def read_current_model(self, *, session_id: str) -> dict[str, Any]:
        del session_id
        return {"reasoningSummary": "concise"}


class FakeTaskActionAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []

    async def list_tasks(self, *, session_id: str) -> list[dict[str, Any]]:
        del session_id
        return []

    async def show_task(self, *, session_id: str, task_id: str) -> dict[str, Any]:
        del session_id
        return {"id": task_id}

    async def cancel_task(self, *, session_id: str, task_id: str) -> dict[str, Any]:
        del session_id
        self.calls.append(("cancel", task_id, None))
        return {"cancelled": task_id}

    async def promote_task(self, *, session_id: str, task_id: str) -> dict[str, Any]:
        del session_id
        self.calls.append(("promote", task_id, None))
        return {"promoted": task_id}

    async def message_task(
        self,
        *,
        session_id: str,
        task_id: str,
        message: str,
    ) -> dict[str, Any]:
        del session_id
        self.calls.append(("message", task_id, message))
        return {"messaged": task_id}

    async def remove_task(self, *, session_id: str, task_id: str) -> dict[str, Any]:
        del session_id
        self.calls.append(("remove", task_id, None))
        return {"removed": task_id}


def _event(
    data: Any,
    event_type: SessionEventType,
    *,
    event_id: UUID | None = None,
) -> SessionEvent:
    return SessionEvent(
        data=data,
        id=event_id or uuid4(),
        timestamp=datetime.now(UTC),
        type=event_type,
    )


def _message_delta() -> SessionEvent:
    return _event(
        AssistantMessageDeltaData(delta_content="early", message_id="message-early"),
        SessionEventType.ASSISTANT_MESSAGE_DELTA,
    )


@pytest.mark.asyncio
async def test_legacy_unverified_binding_fails_before_sdk_attach(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "legacy-binding.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-legacy",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET config_snapshot_state = 'legacy_unverified'
            WHERE thread_id = 'thread-legacy'
            """
        )
        binding = await bindings.by_thread("thread-legacy")
        assert binding is not None
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-legacy",
            binding=binding,
        )

        with pytest.raises(SessionNotReady, match="no verified project"):
            await runtime.attach_create()

        assert bridge.create_calls == 0
        assert runtime.state == RuntimeState.DETACHED


@pytest.mark.asyncio
async def test_runtime_preregisters_ingress_stays_alive_after_idle_and_closes_cleanly(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "runtime.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)

        async def resolve_attachments(_manifest_id: str) -> list[Any]:
            return []

        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            owner_renew_seconds=30,
            attachment_resolver=resolve_attachments,
        )

        await runtime.attach_create()
        assert runtime.state == RuntimeState.READY
        assert bridge.create_kwargs["session_id"] == session_id
        assert bridge.create_kwargs["working_directory"] == str(binding.cwd_snapshot)
        assert bridge.create_kwargs["on_event"] is bridge.ingress

        attached = await bindings.by_thread("thread-1")
        assert attached is not None
        assert attached.attachment_state == AttachmentState.ATTACHED
        assert attached.permission_posture == PermissionPosture.VERIFIED_ALLOW_ALL

        manifest_id = str(uuid4())
        await database.execute(
            """
            INSERT INTO attachment_manifests(
                id, source_kind, source_id, session_id, state, total_bytes, created_at
            ) VALUES (?, 'test', 'test-source', ?, 'ready', 0, 0)
            """,
            (manifest_id, session_id),
        )
        message_id = await runtime.send(
            "hello",
            idempotency_key="discord-message-1",
            agent_mode="interactive",
            attachment_manifest_id=manifest_id,
        )
        bridge.ingress(
            _event(
                UserMessageData(content="hello"),
                SessionEventType.USER_MESSAGE,
                event_id=UUID(message_id),
            )
        )
        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        assert runtime.inbox is not None
        await runtime.inbox.join()
        await runtime._refresh_all_snapshots()

        submission = await database.fetchone(
            """
            SELECT state, accepted_message_id, correlation_basis, attachment_manifest_id
            FROM submissions
            """
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE kind = 'submission'"
        )
        early_event = await database.fetchone(
            "SELECT raw_type FROM event_journal WHERE raw_type = 'assistant.message_delta'"
        )

        assert bridge.handle.disconnect_calls == 0
        assert runtime.state == RuntimeState.READY
        assert dict(submission) == {
            "state": "semantic_complete",
            "accepted_message_id": message_id,
            "correlation_basis": "single_candidate_facts",
            "attachment_manifest_id": manifest_id,
        }
        assert lease["state"] == "released"
        assert early_event["raw_type"] == "assistant.message_delta"

        await runtime.close(idempotency_key="close-1")
        closed = await bindings.by_thread("thread-1")
        owner = await OwnerLeaseStore(database).current(session_id)

    assert runtime.state == RuntimeState.CLOSED
    assert bridge.handle.disconnect_calls == 1
    assert closed is not None
    assert closed.binding_intent == BindingIntent.CLOSED
    assert closed.attachment_state == AttachmentState.ABSENT
    assert owner is not None
    assert owner.expires_at <= datetime.now(UTC).timestamp()


@pytest.mark.asyncio
async def test_immediate_delivery_steers_active_turn_while_queue_waits_for_quiet(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "immediate-steering.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        bridge.processing = True
        bridge.has_active_work = True

        message_id = await runtime.send(
            "steer active turn",
            idempotency_key="active-steering",
            mode="immediate",
        )
        queued_id = await runtime.send(
            "wait for quiet",
            idempotency_key="quiet-queue",
            mode="enqueue",
        )
        queued = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (queued_id,),
        )
        await runtime.shutdown()

    assert message_id == bridge.handle.message_id
    assert [item[0] for item in bridge.handle.sent] == ["steer active turn"]
    assert queued["state"] == "local_queued"


@pytest.mark.asyncio
async def test_restart_drain_losing_queue_claim_stays_durable_and_retries(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "drain-claim.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.processing = True
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        submission_id = await runtime.send(
            "queued across drain",
            idempotency_key="drain-message",
        )
        await database.execute(
            """
            INSERT INTO global_config(key, value, updated_at)
            VALUES ('restart_draining', '1', 1)
            """
        )
        bridge.processing = False

        with pytest.raises(SubmissionClaimDeferred):
            await runtime.dispatch_queued_once()
        deferred = await database.fetchone(
            """
            SELECT q.state AS queue_state, q.dispatch_attempt,
                   s.state AS submission_state
            FROM message_queue q JOIN submissions s ON s.submission_id = q.id
            WHERE q.id = ?
            """,
            (submission_id,),
        )
        unknown = await database.fetchone(
            """
            SELECT COUNT(*) FROM event_journal
            WHERE raw_type = 'copilotd.submission.acceptance_unknown'
            """
        )
        await database.execute(
            """
            UPDATE global_config SET value = '0', updated_at = 2
            WHERE key = 'restart_draining'
            """
        )
        dispatched = await runtime.dispatch_queued_once()

        assert dispatched is not None and dispatched[0] == submission_id
        assert dict(deferred) == {
            "queue_state": "local_queued",
            "dispatch_attempt": 1,
            "submission_state": "local_queued",
        }
        assert unknown[0] == 0
        assert len(bridge.handle.sent) == 1
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_deferred_immediate_send_retries_with_new_dispatch_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "immediate-retry.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        original_claim = runtime._claim_submission
        claim_started = asyncio.Event()
        allow_claim = asyncio.Event()

        async def delayed_claim(
            submission_id: str,
            *,
            operation_idempotency_key: str,
            dispatch_attempt: int,
        ) -> str:
            claim_started.set()
            await allow_claim.wait()
            return await original_claim(
                submission_id,
                operation_idempotency_key=operation_idempotency_key,
                dispatch_attempt=dispatch_attempt,
            )

        monkeypatch.setattr(runtime, "_claim_submission", delayed_claim)
        first = asyncio.create_task(
            runtime.send(
                "immediate retry",
                idempotency_key="immediate-retry",
                mode="immediate",
            )
        )
        await claim_started.wait()
        await database.execute(
            """
            INSERT INTO global_config(key, value, updated_at)
            VALUES ('restart_draining', '1', 1)
            """
        )
        allow_claim.set()
        with pytest.raises(SubmissionClaimDeferred):
            await first
        await database.execute(
            """
            UPDATE global_config SET value = '0', updated_at = 2
            WHERE key = 'restart_draining'
            """
        )
        with pytest.raises(ValueError, match="different immutable submission"):
            await runtime.send(
                "different prompt",
                idempotency_key="immediate-retry",
                mode="immediate",
            )

        pump_task = asyncio.create_task(runtime.dispatch_queued_once())
        retry_task = asyncio.create_task(
            runtime.send(
                "immediate retry",
                idempotency_key="immediate-retry",
                mode="immediate",
            )
        )
        _pump_result, message_id = await asyncio.gather(pump_task, retry_task)
        operations = await database.fetchall(
            """
            SELECT idempotency_key, state FROM session_operations
            WHERE kind = 'send'
            ORDER BY idempotency_key
            """
        )

        assert message_id == bridge.handle.message_id
        operation_states = {str(row["idempotency_key"]): str(row["state"]) for row in operations}
        assert operation_states["send:immediate-retry"] == "rejected"
        assert list(operation_states.values()).count("confirmed") == 1
        assert len(bridge.handle.sent) == 1
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_post_claim_configuration_drift_defers_before_sdk_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "post-claim-drift.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        original_check = runtime._assert_claimed_dispatchable
        claimed = asyncio.Event()
        continue_check = asyncio.Event()

        async def delayed_check(**kwargs: Any) -> None:
            claimed.set()
            await continue_check.wait()
            await original_check(**kwargs)

        monkeypatch.setattr(runtime, "_assert_claimed_dispatchable", delayed_check)
        first = asyncio.create_task(
            runtime.send(
                "post-claim drift",
                idempotency_key="post-claim-drift",
                mode="immediate",
            )
        )
        await claimed.wait()
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_mode = 'plan', runtime_mode = 'plan'
            WHERE thread_id = 'thread-1'
            """
        )
        continue_check.set()
        with pytest.raises(SubmissionClaimDeferred):
            await first
        queued = await database.fetchone(
            """
            SELECT state, dispatch_attempt FROM message_queue
            WHERE thread_id = 'thread-1'
            """
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_mode = 'interactive', runtime_mode = 'interactive'
            WHERE thread_id = 'thread-1'
            """
        )
        message_id = await runtime.send(
            "post-claim drift",
            idempotency_key="post-claim-drift",
            mode="immediate",
        )

        assert dict(queued) == {
            "state": "local_queued",
            "dispatch_attempt": 1,
        }
        assert message_id == bridge.handle.message_id
        assert len(bridge.handle.sent) == 1
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_owner_expiry_during_final_readiness_requeues_before_sdk_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "final-owner-expiry.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        original_readiness = bridge.get_readiness
        readiness_started = asyncio.Event()
        continue_readiness = asyncio.Event()

        async def delayed_readiness(session: FakeHandle) -> dict[str, Any]:
            readiness_started.set()
            await continue_readiness.wait()
            return await original_readiness(session)

        monkeypatch.setattr(bridge, "get_readiness", delayed_readiness)
        send = asyncio.create_task(
            runtime.send(
                "owner expires",
                idempotency_key="owner-expires",
                mode="immediate",
            )
        )
        await readiness_started.wait()
        await database.execute(
            """
            UPDATE session_owner_leases SET expires_at = 0
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        continue_readiness.set()

        with pytest.raises(SubmissionClaimDeferred):
            await asyncio.wait_for(send, timeout=2)
        queue = await database.fetchone(
            """
            SELECT state, dispatch_attempt FROM message_queue
            WHERE thread_id = 'thread-1'
            """
        )
        unknown = await database.fetchone(
            """
            SELECT COUNT(*) FROM event_journal
            WHERE raw_type = 'copilotd.submission.acceptance_unknown'
            """
        )
        await asyncio.wait_for(runtime.shutdown(), timeout=2)

    assert runtime.state == RuntimeState.FENCED
    assert dict(queue) == {"state": "local_queued", "dispatch_attempt": 1}
    assert unknown[0] == 0
    assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_closed_intent_race_is_rejected_immediately_before_sdk_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "closed-final-dispatch.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        final_check_entered = asyncio.Event()
        continue_final_check = asyncio.Event()
        original_final_check = runtime._assert_pre_send_fence

        async def delayed_final_check(*, requested_origin: str) -> None:
            final_check_entered.set()
            await continue_final_check.wait()
            await original_final_check(requested_origin=requested_origin)

        monkeypatch.setattr(runtime, "_assert_pre_send_fence", delayed_final_check)
        send = asyncio.create_task(
            runtime.send(
                "must not cross close",
                idempotency_key="closed-final-race",
                mode="immediate",
            )
        )
        await final_check_entered.wait()
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_reason = NULL
            WHERE thread_id = 'thread-1'
            """
        )
        continue_final_check.set()

        with pytest.raises(SubmissionClaimDeferred):
            await send
        queue = await database.fetchone("SELECT state, dispatch_attempt FROM message_queue")
        await runtime.shutdown()

    assert dict(queue) == {"state": "local_queued", "dispatch_attempt": 1}
    assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_closed_intent_race_is_rejected_during_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "closed-attach-race.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        leases = OwnerLeaseStore(database)
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=leases,
            owner_id="process-1",
            binding=binding,
        )
        lease_acquired = asyncio.Event()
        continue_attachment = asyncio.Event()
        original_acquire = leases.acquire

        async def delayed_acquire(*args: Any, **kwargs: Any) -> object:
            lease = await original_acquire(*args, **kwargs)
            lease_acquired.set()
            await continue_attachment.wait()
            return lease

        monkeypatch.setattr(leases, "acquire", delayed_acquire)
        attach = asyncio.create_task(runtime.attach_resume())
        await lease_acquired.wait()
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_reason = NULL
            WHERE thread_id = 'thread-1'
            """
        )
        continue_attachment.set()

        with pytest.raises(BindingConflict, match="explicit scheduler or recovery"):
            await attach

    assert bridge.resume_calls == 0
    assert runtime.handle is None


@pytest.mark.asyncio
async def test_mailbox_fence_failure_before_claim_requeues_without_unknown(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "mailbox-fence-requeue.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            owner_renew_seconds=3_600,
            queue_poll_seconds=3_600,
        )
        await runtime.attach_create()
        await database.execute(
            """
            UPDATE session_owner_leases SET expires_at = ?
            WHERE sdk_session_id = ?
            """,
            (time.time() + 10, session_id),
        )

        with pytest.raises(SubmissionClaimDeferred):
            await runtime.send(
                "fenced before claim",
                idempotency_key="pre-claim-fence",
                mode="immediate",
            )
        queue = await database.fetchone("SELECT state, dispatch_attempt FROM message_queue")
        operation = await database.fetchone(
            """
            SELECT state, error_code FROM session_operations
            WHERE idempotency_key = 'send:pre-claim-fence'
            """
        )
        unknown = await database.fetchone(
            """
            SELECT COUNT(*) FROM event_journal
            WHERE raw_type = 'copilotd.submission.acceptance_unknown'
            """
        )
        await runtime.shutdown()

    assert dict(queue) == {"state": "local_queued", "dispatch_attempt": 1}
    assert dict(operation) == {
        "state": "rejected",
        "error_code": "owner_fence_lost_before_start",
    }
    assert unknown[0] == 0
    assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_non_manifest_attachments_are_rejected_before_admission(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "attachment-admission.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
        )
        await runtime.attach_create()

        with pytest.raises(SessionNotReady, match="durable manifest"):
            await runtime.send(
                "attachment",
                idempotency_key="attachment-without-manifest",
                attachments=[object()],
            )
        queue = await database.fetchone("SELECT COUNT(*) FROM message_queue")
        await runtime.shutdown()

    assert queue[0] == 0


@pytest.mark.asyncio
async def test_emergency_shutdown_does_not_wait_on_dead_reducer(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "dead-reducer-shutdown.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            shutdown_timeout_seconds=0.05,
        )
        await runtime.attach_create()
        assert runtime._reducer is not None
        await runtime._reducer.stop()

        await asyncio.wait_for(runtime.shutdown(emergency=True), timeout=1)
        recovered = await bindings.by_thread("thread-1")

    assert runtime.state == RuntimeState.RECOVERY_UNKNOWN
    assert recovered is not None
    assert recovered.attachment_state == AttachmentState.RECOVERY_UNKNOWN


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_before_response", [True, False])
async def test_user_message_and_send_response_orderings_keep_one_submission(
    tmp_path: Path,
    callback_before_response: bool,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / f"send-order-{callback_before_response}.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-send-order",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-send-order",
            binding=binding,
        )
        await runtime.attach_create()
        accepted_id: str
        if callback_before_response:
            accepted_id = str(uuid4())
            original_send = bridge.handle.send

            async def callback_first_send(prompt: str, **kwargs: Any) -> str:
                if prompt != "first prompt":
                    return await original_send(prompt, **kwargs)
                bridge.handle.sent.append((prompt, kwargs))
                bridge.ingress(
                    _event(
                        UserMessageData(content=prompt),
                        SessionEventType.USER_MESSAGE,
                        event_id=UUID(accepted_id),
                    )
                )
                assert runtime.inbox is not None
                await runtime.inbox.join()
                return accepted_id

            bridge.handle.send = callback_first_send  # type: ignore[method-assign]

        returned_id = await runtime.send(
            "first prompt",
            idempotency_key="first-prompt",
        )
        if not callback_before_response:
            accepted_id = returned_id
            bridge.ingress(
                _event(
                    UserMessageData(content="first prompt"),
                    SessionEventType.USER_MESSAGE,
                    event_id=UUID(accepted_id),
                )
            )
        assert runtime.inbox is not None
        await runtime.inbox.join()
        rows = await database.fetchall(
            """
            SELECT origin, state, accepted_message_id, observed_user_event_id
            FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )

        assert returned_id == accepted_id
        assert [dict(row) for row in rows] == [
            {
                "origin": "app_message",
                "state": "observed_active",
                "accepted_message_id": accepted_id,
                "observed_user_event_id": accepted_id,
            }
        ]

        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        await runtime.inbox.join()
        await runtime._refresh_all_snapshots()
        await runtime.send("second prompt", idempotency_key="second-prompt")
        assert [prompt for prompt, _ in bridge.handle.sent] == [
            "first prompt",
            "second prompt",
        ]
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_external_same_prompt_callback_before_acceptance_is_reclassified(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    external_event_id = str(uuid4())
    accepted_id = str(uuid4())
    async with Database(tmp_path / "external-before-acceptance.sqlite3") as database:
        await CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-external-before-acceptance",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-external-before-acceptance",
            binding=binding,
        )
        await runtime.attach_create()

        async def external_callback_first(prompt: str, **kwargs: Any) -> str:
            bridge.handle.sent.append((prompt, kwargs))
            bridge.ingress(
                _event(
                    UserMessageData(content=prompt),
                    SessionEventType.USER_MESSAGE,
                    event_id=UUID(external_event_id),
                )
            )
            assert runtime.inbox is not None
            await runtime.inbox.join()
            return accepted_id

        bridge.handle.send = external_callback_first  # type: ignore[method-assign]
        assert await runtime.send("same prompt", idempotency_key="same-prompt") == accepted_id
        provisional = await database.fetchall(
            """
            SELECT origin, state, accepted_message_id, observed_user_event_id,
                   correlation_basis
            FROM submissions WHERE sdk_session_id = ? ORDER BY origin
            """,
            (session_id,),
        )

        assert [dict(row) for row in provisional] == [
            {
                "origin": "app_message",
                "state": "submitted",
                "accepted_message_id": accepted_id,
                "observed_user_event_id": None,
                "correlation_basis": None,
            },
            {
                "origin": "runtime_observed",
                "state": "observed_active",
                "accepted_message_id": None,
                "observed_user_event_id": external_event_id,
                "correlation_basis": "acceptance_id_mismatch_runtime_observed",
            },
        ]

        bridge.ingress(
            _event(
                UserMessageData(content="same prompt"),
                SessionEventType.USER_MESSAGE,
                event_id=UUID(accepted_id),
            )
        )
        assert runtime.inbox is not None
        await runtime.inbox.join()
        reconciled = await database.fetchall(
            """
            SELECT origin, state, accepted_message_id, observed_user_event_id
            FROM submissions WHERE sdk_session_id = ? ORDER BY origin
            """,
            (session_id,),
        )
        leases = await database.fetchall(
            """
            SELECT source_id, state FROM liveness_leases
            WHERE sdk_session_id = ? AND kind = 'submission'
            ORDER BY source_id
            """,
            (session_id,),
        )
        await runtime.shutdown()

    assert len(reconciled) == 2
    assert [dict(row) for row in reconciled] == [
        {
            "origin": "app_message",
            "state": "observed_active",
            "accepted_message_id": accepted_id,
            "observed_user_event_id": accepted_id,
        },
        {
            "origin": "runtime_observed",
            "state": "observed_active",
            "accepted_message_id": None,
            "observed_user_event_id": external_event_id,
        },
    ]
    assert len(leases) == 2
    assert all(row["state"] == "active" for row in leases)


@pytest.mark.asyncio
async def test_attachment_frame_resolver_runs_immediately_before_send(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    calls: list[dict[str, Any]] = []

    async def resolver(manifest_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        calls.append({"manifest_id": manifest_id, **kwargs})
        return [
            {
                "type": "file",
                "path": "/tmp/fallback.bin",
                "displayName": "fallback.bin",
            }
        ]

    async with Database(tmp_path / "frame-resolver.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-frame",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-frame",
            binding=binding,
            attachment_resolver=resolver,
        )
        await runtime.attach_create()
        await database.execute(
            """
            INSERT INTO attachment_manifests(
                id, source_kind, source_id, session_id,
                state, total_bytes, created_at
            ) VALUES ('manifest-frame', 'test', 'frame', ?, 'ready', 0, 0)
            """,
            (session_id,),
        )

        await runtime.send(
            "长提示" * 100,
            idempotency_key="frame-send",
            attachments=[{"type": "blob", "data": "old"}],
            attachment_manifest_id="manifest-frame",
            mode="immediate",
            agent_mode="plan",
        )

        assert calls == [
            {
                "manifest_id": "manifest-frame",
                "session_id": session_id,
                "prompt": "长提示" * 100,
                "mode": "immediate",
                "agent_mode": "plan",
            }
        ]
        assert bridge.handle.sent[-1][1]["attachments"][0]["type"] == "file"
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_direct_attachments_cannot_bypass_complete_frame_limit(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "direct-frame-limit.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-direct-frame",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-direct-frame",
            binding=binding,
            send_frame_max_bytes=256,
        )
        await runtime.attach_create()

        with pytest.raises(OperationRejected, match=r"serialized session\.send"):
            await runtime.send(
                "x" * 1000,
                idempotency_key="oversized-direct-frame",
                attachments=[{"type": "blob", "data": "x" * 1000}],
                mode="immediate",
            )

        assert bridge.handle.sent == []
        assert runtime._volatile_attachments == {}
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE sdk_session_id = ?",
            (session_id,),
        )
        assert submission["state"] == "rejected"
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_runtime_routes_user_input_through_durable_interaction(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "interaction.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-interaction",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-interaction",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()
        handler = bridge.create_kwargs["on_user_input_request"]
        assert callable(handler)
        assert callable(bridge.create_kwargs["on_exit_plan_mode_request"])
        assert callable(bridge.create_kwargs["on_auto_mode_switch_request"])

        response_task = asyncio.create_task(
            handler(
                {
                    "question": "Choose one",
                    "choices": ["first", "second"],
                    "allowFreeform": True,
                },
                {},
            )
        )
        for _ in range(20):
            pending = await database.fetchone(
                "SELECT interaction_id FROM pending_interactions WHERE state = 'pending'"
            )
            if pending is not None:
                break
            await asyncio.sleep(0)
        assert pending is not None
        interaction_id = str(pending["interaction_id"])

        assert await runtime.respond_interaction(interaction_id, selection=1) == "resolved"
        assert await response_task == {"answer": "second", "wasFreeform": False}
        assert await runtime.respond_interaction(interaction_id, selection=0) == "expired"

        settled = await database.fetchone(
            "SELECT state, response FROM pending_interactions WHERE interaction_id = ?",
            (interaction_id,),
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE lease_id = ?",
            (f"interaction:{interaction_id}",),
        )
        render_rows = await database.fetchall(
            """
            SELECT lane, coalesce_key, payload FROM render_outbox
            WHERE coalesce_key = ? ORDER BY logical_seq
            """,
            (f"interaction:{interaction_id}",),
        )
        assert settled is not None and settled["state"] == "resolved"
        assert '"answer": "second"' in str(settled["response"])
        assert lease is not None and lease["state"] == "released"
        assert [row["lane"] for row in render_rows] == ["interaction", "interaction"]
        assert all(row["coalesce_key"] == f"interaction:{interaction_id}" for row in render_rows)

        assert await runtime.set_mode("plan", idempotency_key="enter-plan") == "plan"
        plan_task = asyncio.create_task(
            bridge.create_kwargs["on_exit_plan_mode_request"](
                {
                    "summary": "Plan is ready",
                    "actions": ["interactive", "autopilot"],
                    "recommendedAction": "interactive",
                },
                {},
            )
        )
        for _ in range(20):
            plan_row = await database.fetchone(
                """
                SELECT interaction_id FROM pending_interactions
                WHERE kind = 'exit_plan_mode' AND state = 'pending'
                """
            )
            if plan_row is not None:
                break
            await asyncio.sleep(0)
        assert plan_row is not None
        assert (
            await runtime.respond_interaction(
                str(plan_row["interaction_id"]),
                selection=0,
            )
            == "resolved"
        )
        assert await plan_task == {
            "approved": True,
            "selectedAction": "interactive",
        }
        bridge.ingress(
            _event(
                SessionModeChangedData(
                    new_mode=SessionMode.INTERACTIVE,
                    previous_mode=SessionMode.PLAN,
                ),
                SessionEventType.SESSION_MODE_CHANGED,
            )
        )
        assert runtime.inbox is not None
        await runtime.inbox.join()
        mode_state = await database.fetchone(
            """
            SELECT desired_mode, runtime_mode FROM session_bindings
            WHERE thread_id = 'thread-interaction'
            """
        )
        assert dict(mode_state) == {
            "desired_mode": "interactive",
            "runtime_mode": "interactive",
        }
        bridge.ingress(
            _event(
                SessionModeChangedData(
                    new_mode=SessionMode.AUTOPILOT,
                    previous_mode=SessionMode.INTERACTIVE,
                ),
                SessionEventType.SESSION_MODE_CHANGED,
            )
        )
        await runtime.inbox.join()
        unrelated_mode_state = await database.fetchone(
            """
            SELECT desired_mode, runtime_mode FROM session_bindings
            WHERE thread_id = 'thread-interaction'
            """
        )
        consumed = await database.fetchone(
            """
            SELECT target_mode, consumed_at FROM pending_interactions
            WHERE interaction_id = ?
            """,
            (str(plan_row["interaction_id"]),),
        )
        assert dict(unrelated_mode_state) == {
            "desired_mode": "interactive",
            "runtime_mode": "autopilot",
        }
        assert consumed is not None and consumed["target_mode"] == "interactive"
        assert consumed["consumed_at"] is not None
        await runtime.close(idempotency_key="close-interaction")


@pytest.mark.asyncio
async def test_runtime_times_out_plan_interaction_with_typed_decline(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "interaction-timeout.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-interaction-timeout",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-interaction-timeout",
            binding=binding,
            owner_renew_seconds=30,
            interaction_timeout_seconds=0.01,
        )
        await runtime.attach_create()
        result = await bridge.create_kwargs["on_exit_plan_mode_request"](
            {
                "summary": "Ready to implement",
                "actions": ["interactive", "autopilot"],
                "recommendedAction": "interactive",
            },
            {},
        )
        assert result == {"approved": False}
        interaction = await database.fetchone("SELECT state FROM pending_interactions")
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE kind = 'interaction'"
        )
        assert interaction is not None and interaction["state"] == "expired"
        assert lease is not None and lease["state"] == "released"
        await runtime.close(idempotency_key="close-interaction-timeout")


@pytest.mark.asyncio
async def test_interaction_timeout_returns_response_that_already_won_durable_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "interaction-race.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-interaction-race",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-interaction-race",
            binding=binding,
            owner_renew_seconds=30,
            interaction_timeout_seconds=0.05,
        )
        await runtime.attach_create()
        handler_task = asyncio.create_task(
            bridge.create_kwargs["on_user_input_request"](
                {
                    "question": "Choose",
                    "choices": ["winner"],
                    "allowFreeform": False,
                },
                {},
            )
        )
        for _ in range(20):
            pending = await database.fetchone(
                "SELECT interaction_id FROM pending_interactions WHERE state = 'pending'"
            )
            if pending is not None:
                break
            await asyncio.sleep(0)
        assert pending is not None
        assert runtime.inbox is not None
        original_commit = runtime.inbox.commit_internal
        response_claimed = asyncio.Event()
        allow_render_event = asyncio.Event()

        async def delayed_commit(
            payload: Any,
            *,
            source: str = "internal",
            internal_event_id: str | None = None,
        ) -> None:
            if payload.get("type") == "copilotd.interaction.resolved":
                response_claimed.set()
                await allow_render_event.wait()
            await original_commit(
                payload,
                source=source,
                internal_event_id=internal_event_id,
            )

        monkeypatch.setattr(runtime.inbox, "commit_internal", delayed_commit)
        response_task = asyncio.create_task(
            runtime.respond_interaction(str(pending["interaction_id"]), selection=0)
        )
        await response_claimed.wait()

        assert await handler_task == {"answer": "winner", "wasFreeform": False}
        allow_render_event.set()
        assert await response_task == "resolved"
        settled = await database.fetchone(
            "SELECT state, response FROM pending_interactions WHERE interaction_id = ?",
            (str(pending["interaction_id"]),),
        )
        assert settled is not None and settled["state"] == "resolved"
        assert '"answer": "winner"' in str(settled["response"])
        await runtime.close(idempotency_key="close-interaction-race")


@pytest.mark.asyncio
async def test_background_task_snapshot_marks_disappearance_unknown_then_terminal(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    task_id = "task-1"
    async with Database(tmp_path / "task-snapshot.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-task-snapshot",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.tasks = [
            {
                "id": task_id,
                "type": "agent",
                "status": "running",
                "description": "Background worker",
                "latestResponse": "Working",
            }
        ]
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-task-snapshot",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()

        for _ in range(50):
            observation = await database.fetchone(
                """
                SELECT observed_state FROM background_observations
                WHERE task_id = ?
                """,
                (task_id,),
            )
            if observation is not None:
                break
            await asyncio.sleep(0.01)
        assert observation is not None and observation["observed_state"] == "running"

        bridge.tasks = []
        bridge.ingress(
            _event(
                SessionBackgroundTasksChangedData(),
                SessionEventType.SESSION_BACKGROUND_TASKS_CHANGED,
            )
        )
        for _ in range(50):
            observation = await database.fetchone(
                "SELECT observed_state FROM background_observations WHERE task_id = ?",
                (task_id,),
            )
            if observation is not None and observation["observed_state"] == "unknown":
                break
            await asyncio.sleep(0.01)
        card = await database.fetchone(
            "SELECT state FROM task_card_projections WHERE task_id = ?",
            (task_id,),
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE source_id = ?",
            (f"task:{task_id}",),
        )
        assert observation is not None and observation["observed_state"] == "unknown"
        assert card is not None and card["state"] == "unknown"
        assert lease is not None and lease["state"] == "active"

        bridge.tasks = [
            {
                "id": task_id,
                "type": "agent",
                "status": "completed",
                "description": "Background worker",
                "result": "Done",
            }
        ]
        bridge.ingress(
            _event(
                SessionBackgroundTasksChangedData(),
                SessionEventType.SESSION_BACKGROUND_TASKS_CHANGED,
            )
        )
        for _ in range(50):
            observation = await database.fetchone(
                """
                SELECT observed_state, terminal_evidence
                FROM background_observations WHERE task_id = ?
                """,
                (task_id,),
            )
            if observation is not None and observation["terminal_evidence"]:
                break
            await asyncio.sleep(0.01)
        card = await database.fetchone(
            "SELECT state FROM task_card_projections WHERE task_id = ?",
            (task_id,),
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE source_id = ?",
            (f"task:{task_id}",),
        )
        assert observation is not None
        assert dict(observation) == {
            "observed_state": "completed",
            "terminal_evidence": "task_snapshot",
        }
        assert card is not None and card["state"] == "completed"
        assert lease is not None and lease["state"] == "released"
        await runtime.close(idempotency_key="close-task-snapshot")


@pytest.mark.asyncio
async def test_pending_messages_modified_triggers_durable_queue_snapshot(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "pending-trigger.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-pending-trigger",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-pending-trigger",
            binding=binding,
        )
        await runtime.attach_create()
        for _ in range(50):
            initial = await database.fetchone(
                """
                SELECT applied_epoch FROM reconciliation_state
                WHERE sdk_session_id = ? AND topic = 'queue'
                """,
                (session_id,),
            )
            if initial is not None and int(initial["applied_epoch"]) > 0:
                break
            await asyncio.sleep(0.01)
        initial_calls = bridge.readiness_snapshot_calls
        bridge.pending_items = [
            {
                "id": "native-item-1",
                "agentMode": "interactive",
                "displayText": "queued remotely",
            }
        ]
        bridge.ingress(
            _event(
                PendingMessagesModifiedData(),
                SessionEventType.PENDING_MESSAGES_MODIFIED,
            )
        )

        for _ in range(50):
            snapshot = await database.fetchone(
                """
                SELECT requested_epoch, applied_epoch
                FROM reconciliation_state
                WHERE sdk_session_id = ? AND topic = 'queue'
                """,
                (session_id,),
            )
            queue_item = await database.fetchone(
                """
                SELECT state FROM native_queue_items
                WHERE sdk_session_id = ? AND item_id = 'native-item-1'
                """,
                (session_id,),
            )
            if (
                snapshot is not None
                and int(snapshot["applied_epoch"]) > int(initial["applied_epoch"])
                and queue_item is not None
            ):
                break
            await asyncio.sleep(0.01)

        assert bridge.readiness_snapshot_calls > initial_calls
        assert snapshot["applied_epoch"] == snapshot["requested_epoch"]
        assert queue_item["state"] == "present"
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_close_freezes_send_admission_before_checking_detach_blockers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "close-admission.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-close-admission",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-close-admission",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()
        assert runtime.inbox is not None
        original_commit = runtime.inbox.commit_internal
        admission_started = asyncio.Event()
        allow_admission = asyncio.Event()

        async def delayed_commit(
            payload: Any,
            *,
            source: str = "internal",
            internal_event_id: str | None = None,
        ) -> None:
            if payload.get("type") == "copilotd.submission.queued":
                admission_started.set()
                await allow_admission.wait()
            await original_commit(
                payload,
                source=source,
                internal_event_id=internal_event_id,
            )

        monkeypatch.setattr(runtime.inbox, "commit_internal", delayed_commit)
        send_task = asyncio.create_task(runtime.send("racing send", idempotency_key="racing-send"))
        await admission_started.wait()
        close_task = asyncio.create_task(runtime.close(idempotency_key="racing-close"))
        await asyncio.sleep(0)
        assert not close_task.done()

        allow_admission.set()
        message_id = await send_task
        with pytest.raises(DetachBlocked):
            await close_task
        assert runtime.state == RuntimeState.READY

        bridge.ingress(
            _event(
                UserMessageData(content="racing send"),
                SessionEventType.USER_MESSAGE,
                event_id=UUID(message_id),
            )
        )
        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        await runtime.inbox.join()
        await runtime.close(idempotency_key="close-after-race")


@pytest.mark.asyncio
async def test_shutdown_cancels_hung_sdk_send_with_unknown_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "shutdown-hung-send.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-hung-send",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-hung-send",
            binding=binding,
            owner_renew_seconds=30,
            sdk_operation_timeout_seconds=60,
            shutdown_timeout_seconds=0.01,
        )
        await runtime.attach_create()
        send_started = asyncio.Event()
        never = asyncio.Event()

        async def hung_send(_prompt: str, **_kwargs: Any) -> str:
            send_started.set()
            await never.wait()
            return "unreachable"

        monkeypatch.setattr(bridge.handle, "send", hung_send)
        send_task = asyncio.create_task(
            runtime.send(
                "hang",
                idempotency_key="hung-send",
                mode="immediate",
            )
        )
        await send_started.wait()

        await asyncio.wait_for(runtime.shutdown(), timeout=0.5)
        with pytest.raises(OperationAmbiguous):
            await send_task
        operation = await database.fetchone(
            "SELECT state FROM session_operations WHERE idempotency_key = 'send:hung-send'"
        )

    assert runtime.state == RuntimeState.RECOVERY_UNKNOWN
    assert operation is not None and operation["state"] == "unknown"


@pytest.mark.asyncio
async def test_shutdown_drains_successful_send_acceptance_before_unknown_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "shutdown-accepted-send.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-shutdown-accepted",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-shutdown-accepted",
            binding=binding,
            owner_renew_seconds=30,
            shutdown_timeout_seconds=2,
        )
        await runtime.attach_create()
        send_started = asyncio.Event()
        allow_send_result = asyncio.Event()

        async def delayed_success(_prompt: str, **_kwargs: Any) -> str:
            send_started.set()
            await allow_send_result.wait()
            return "accepted-during-shutdown"

        monkeypatch.setattr(bridge.handle, "send", delayed_success)
        send_task = asyncio.create_task(
            runtime.send(
                "finish before shutdown",
                idempotency_key="shutdown-accepted",
                mode="immediate",
            )
        )
        await send_started.wait()
        shutdown_task = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0.05)
        before_release = await database.fetchone(
            "SELECT state, accepted_message_id FROM submissions"
        )
        assert not shutdown_task.done()
        assert dict(before_release) == {
            "state": "submitting",
            "accepted_message_id": None,
        }

        allow_send_result.set()
        message_id, _ = await asyncio.gather(send_task, shutdown_task)
        persisted = await database.fetchone("SELECT state, accepted_message_id FROM submissions")
        operation = await database.fetchone(
            """
            SELECT state FROM session_operations
            WHERE idempotency_key = 'send:shutdown-accepted'
            """
        )

    assert message_id == "accepted-during-shutdown"
    assert dict(persisted) == {
        "state": "outcome_unknown",
        "accepted_message_id": "accepted-during-shutdown",
    }
    assert operation["state"] == "confirmed"


@pytest.mark.asyncio
async def test_shutdown_after_admission_returns_durable_deferred_handoff_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "shutdown-handoff-gap.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-shutdown-gap",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-shutdown-gap",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()
        admitted = asyncio.Event()
        continue_handoff = asyncio.Event()
        original_dispatch = runtime._dispatch_submission

        async def delayed_handoff(**kwargs: Any) -> str:
            admitted.set()
            await continue_handoff.wait()
            return await original_dispatch(**kwargs)

        monkeypatch.setattr(runtime, "_dispatch_submission", delayed_handoff)
        send_task = asyncio.create_task(
            runtime.send(
                "durable across shutdown",
                idempotency_key="shutdown-handoff-gap",
                mode="immediate",
            )
        )
        await admitted.wait()

        await runtime.shutdown()
        continue_handoff.set()
        receipt = await send_task
        durable = await database.fetchone(
            """
            SELECT s.submission_id, s.state AS submission_state,
                   q.state AS queue_state
            FROM submissions s JOIN message_queue q ON q.id = s.submission_id
            """
        )
        operations = await database.fetchone(
            "SELECT COUNT(*) FROM session_operations WHERE kind = 'send'"
        )

    assert receipt == durable["submission_id"]
    assert dict(durable) == {
        "submission_id": receipt,
        "submission_state": "local_queued",
        "queue_state": "local_queued",
    }
    assert operations[0] == 0
    assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_graceful_shutdown_preserves_local_queued_submissions(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "shutdown-queued.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-shutdown-queued",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.processing = True
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-shutdown-queued",
            binding=binding,
        )
        await runtime.attach_create()
        submission_id = await runtime.send(
            "stay queued",
            idempotency_key="shutdown-queued",
        )

        await runtime.shutdown()
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = ?",
            (submission_id,),
        )
        queued = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (submission_id,),
        )

    assert submission["state"] == "local_queued"
    assert queued["state"] == "local_queued"


@pytest.mark.asyncio
async def test_forced_close_cancels_submission_before_atomic_dispatch_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "force-close-claim.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-force-close-claim",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-force-close-claim",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()
        original_claim = runtime._claim_submission
        claim_started = asyncio.Event()
        allow_claim = asyncio.Event()

        async def delayed_claim(
            submission_id: str,
            *,
            operation_idempotency_key: str,
            dispatch_attempt: int,
        ) -> str:
            claim_started.set()
            await allow_claim.wait()
            return await original_claim(
                submission_id,
                operation_idempotency_key=operation_idempotency_key,
                dispatch_attempt=dispatch_attempt,
            )

        monkeypatch.setattr(runtime, "_claim_submission", delayed_claim)
        send_task = asyncio.create_task(
            runtime.send(
                "must be cancelled",
                idempotency_key="force-close-race",
                mode="immediate",
            )
        )
        await claim_started.wait()
        close_task = asyncio.create_task(
            runtime.close(idempotency_key="force-close-race", force=True)
        )
        try:
            for _ in range(50):
                queue_row = await database.fetchone(
                    "SELECT state FROM message_queue WHERE thread_id = ?",
                    ("thread-force-close-claim",),
                )
                if queue_row is not None and queue_row["state"] == "cancelled":
                    break
                await asyncio.sleep(0.005)
            assert queue_row is not None and queue_row["state"] == "cancelled"
        finally:
            allow_claim.set()
        with pytest.raises(OperationRejected, match="cancelled before dispatch"):
            await send_task
        await close_task
        assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_model_switch_timeout_marks_operation_unknown_and_releases_mailbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "model-timeout.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-model-timeout",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-model-timeout",
            binding=binding,
            owner_renew_seconds=30,
            sdk_operation_timeout_seconds=0.01,
        )
        await runtime.attach_create()

        async def hung_set_model(
            _session: FakeHandle,
            *,
            model: str,
            reasoning_effort: str | None,
            context_tier: str | None,
        ) -> None:
            del model, reasoning_effort, context_tier
            await asyncio.Event().wait()

        monkeypatch.setattr(bridge, "set_model", hung_set_model)
        with pytest.raises(OperationAmbiguous):
            await runtime.set_model(
                "gpt-test",
                reasoning_effort="low",
                context_tier=None,
                idempotency_key="hung-model",
            )
        operation = await database.fetchone(
            "SELECT state FROM session_operations WHERE idempotency_key = 'model:hung-model'"
        )
        assert operation is not None and operation["state"] == "unknown"
        await runtime.close(idempotency_key="close-model-timeout")


@pytest.mark.asyncio
async def test_resume_failure_keeps_original_mapping_and_never_falls_back_to_create(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "resume.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id, fail_resume=True)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
        )

        with pytest.raises(SessionAttachUnknown):
            await runtime.attach_resume()

        recovered = await bindings.by_thread("thread-1")
        assert recovered is not None
        assert recovered.sdk_session_id == session_id
        assert recovered.attachment_state == AttachmentState.RECOVERY_UNKNOWN
        assert bridge.resume_calls == 1
        assert bridge.create_calls == 0
        assert runtime.inbox is None
        assert runtime.handle is None
        assert runtime._tasks.active_count == 0
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_resume_honors_runtime_in_use_probe_before_attaching(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "owner-conflict.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-owner-conflict",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.in_use = True
        leases = OwnerLeaseStore(database)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=leases,
            owner_id="new-process",
            binding=binding,
            capabilities=CapabilityRegistry(
                Settings(_env_file=None, data_dir=tmp_path)
            ).load_checked(),
        )

        with pytest.raises(SessionOwnerConflict):
            await runtime.attach_resume()

        observed = await bindings.by_thread("thread-owner-conflict")
        lease = await leases.current(session_id)

    assert observed is not None
    assert observed.attachment_state == AttachmentState.OWNER_CONFLICT
    assert bridge.resume_calls == 0
    assert runtime.inbox is None
    assert lease is not None and lease.expires_at <= lease.renewed_at


@pytest.mark.asyncio
async def test_post_attach_reconciliation_failure_cleans_handle_lease_and_tasks(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "post-attach-cleanup.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-post-attach-cleanup",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.permission_error = PermissionPostureError("managed policy")
        leases = OwnerLeaseStore(database)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=leases,
            owner_id="process-post-attach-cleanup",
            binding=binding,
        )

        with pytest.raises(PermissionPostureError):
            await runtime.attach_create()

        observed = await bindings.by_thread("thread-post-attach-cleanup")
        lease = await leases.current(session_id)

    assert observed is not None
    assert observed.attachment_state == AttachmentState.RECOVERY_UNKNOWN
    assert runtime.state == RuntimeState.RECOVERY_UNKNOWN
    assert runtime.handle is None
    assert runtime.inbox is None
    assert runtime._tasks.active_count == 0
    assert bridge.handle.disconnect_calls == 1
    assert lease is not None and lease.expires_at <= lease.renewed_at


@pytest.mark.asyncio
async def test_owner_lease_acquisition_failure_restores_detached_retryable_state(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "lease-acquire-failure.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-lease-acquire-failure",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        leases = OwnerLeaseStore(database)
        await leases.acquire(session_id, "existing-owner")
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=leases,
            owner_id="contending-owner",
            binding=binding,
        )

        with pytest.raises(OwnerConflict):
            await runtime.attach_resume()

    assert runtime.state == RuntimeState.DETACHED
    assert runtime.handle is None
    assert runtime.inbox is None


@pytest.mark.asyncio
async def test_unsupported_optional_capabilities_do_not_create_unknown_gates(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "optional-capabilities.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-optional-capabilities",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        manifest = CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).load_checked()
        capabilities = dict(manifest.capabilities)
        for name in (
            "agents_current",
            "agents_list",
            "commands_list",
            "native_schedule",
            "remote",
            "remote_status",
            "selected_agent",
            "schedules_list",
            "task_snapshot",
            "tasks_list",
        ):
            capabilities[name] = replace(capabilities[name], supported=False)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-optional-capabilities",
            binding=binding,
            capabilities=replace(manifest, capabilities=capabilities),
        )

        await runtime.attach_create()
        await runtime._assert_dispatchable()
        await runtime.shutdown()

    assert runtime.state == RuntimeState.RECOVERY_UNKNOWN


@pytest.mark.asyncio
async def test_resume_backfills_durable_event_log_rebases_expired_cursor_and_records_gap(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    recovered_event = SessionEvent(
        data=AssistantMessageDeltaData(
            delta_content="recovered",
            message_id="recovered-message",
        ),
        id=uuid4(),
        parent_id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.ASSISTANT_MESSAGE_DELTA,
        ephemeral=False,
    )
    async with Database(tmp_path / "event-backfill.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-event-backfill",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET event_cursor = 'expired-cursor', event_cursor_epoch = 2
            WHERE thread_id = 'thread-event-backfill'
            """
        )
        binding = await bindings.by_thread("thread-event-backfill")
        assert binding is not None
        bridge = FakeBridge(session_id)
        bridge.event_log_batches = [
            EventLogBatch(
                cursor="rebased-cursor",
                cursor_status="expired",
                events=(recovered_event,),
                has_more=False,
                filtered_ephemeral=0,
            )
        ]
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-event-backfill",
            binding=binding,
            capabilities=CapabilityRegistry(
                Settings(_env_file=None, data_dir=tmp_path)
            ).load_checked(),
        )

        await runtime.attach_resume()
        cursor = await database.fetchone(
            """
            SELECT event_cursor, cursor_status, event_cursor_epoch,
                   event_predecessor_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        journal = await database.fetchone(
            "SELECT raw_type FROM event_journal WHERE event_id = ?",
            (str(recovered_event.id),),
        )
        incidents = await database.fetchall(
            """
            SELECT kind FROM runtime_incidents
            WHERE session_id = ? ORDER BY id
            """,
            (session_id,),
        )
        await runtime.shutdown()

    assert dict(cursor) == {
        "event_cursor": "rebased-cursor",
        "cursor_status": "expired",
        "event_cursor_epoch": 3,
        "event_predecessor_id": str(recovered_event.id),
    }
    assert journal["raw_type"] == "assistant.message_delta"
    assert [row["kind"] for row in incidents] == [
        "event_cursor_expired_rebase",
        "event_predecessor_gap",
    ]


@pytest.mark.asyncio
async def test_ingress_overflow_freezes_backfills_and_replaces_generation(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    recovered = _message_delta()
    async with Database(tmp_path / "overflow-recovery.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-overflow",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.event_log_batches = [
            EventLogBatch(
                cursor="overflow-backfilled",
                cursor_status="ok",
                events=(recovered,),
                has_more=False,
                filtered_ephemeral=0,
            ),
            EventLogBatch(
                cursor="replacement-caught-up",
                cursor_status="ok",
                events=(),
                has_more=False,
                filtered_ephemeral=0,
            ),
        ]
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-overflow",
            binding=binding,
            capabilities=CapabilityRegistry(
                Settings(_env_file=None, data_dir=tmp_path)
            ).load_checked(),
            ingress_capacity=8,
        )
        await runtime.attach_create()
        for _ in range(100):
            snapshots = await database.fetchone(
                """
                SELECT COUNT(*) FROM reconciliation_state
                WHERE sdk_session_id = ? AND applied_epoch = requested_epoch
                  AND applied_epoch > 0
                """,
                (session_id,),
            )
            if snapshots is not None and int(snapshots[0]) >= 5:
                break
            await asyncio.sleep(0.01)

        for index in range(20):
            bridge.ingress(
                _event(
                    AssistantMessageDeltaData(
                        delta_content=f"burst-{index}",
                        message_id=f"burst-message-{index}",
                    ),
                    SessionEventType.ASSISTANT_MESSAGE_DELTA,
                )
            )

        for _ in range(300):
            observed = await bindings.by_thread("thread-overflow")
            if (
                observed is not None
                and observed.runtime_generation >= 2
                and runtime.state == RuntimeState.READY
                and bridge.resume_calls >= 1
            ):
                break
            await asyncio.sleep(0.01)
        incidents = await database.fetchall(
            """
            SELECT kind FROM runtime_incidents
            WHERE session_id = ? ORDER BY id
            """,
            (session_id,),
        )
        cursor = await database.fetchone(
            "SELECT event_cursor FROM session_bindings WHERE sdk_session_id = ?",
            (session_id,),
        )
        await runtime.shutdown()

    assert observed is not None and observed.runtime_generation >= 2
    assert bridge.resume_calls == 1
    assert bridge.handle.disconnect_calls == 1
    assert "ingress_overflow" in {row["kind"] for row in incidents}
    assert cursor["event_cursor"] == "replacement-caught-up"


@pytest.mark.asyncio
async def test_attachment_manifest_never_falls_back_to_attachment_free_send(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "attachment-gate.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-attachment-gate",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-attachment-gate",
            binding=binding,
        )
        await runtime.attach_create()
        await database.execute(
            """
            INSERT INTO attachment_manifests(
                id, source_kind, source_id, session_id, state,
                total_bytes, created_at
            ) VALUES ('manifest-1', 'test', 'source', ?, 'ready', 0, 1)
            """,
            (session_id,),
        )

        with pytest.raises(SessionNotReady, match="no integrity resolver"):
            await runtime.send(
                "must not lose attachment",
                idempotency_key="attachment-gate",
                attachment_manifest_id="manifest-1",
            )
        queue = await database.fetchone(
            "SELECT state FROM message_queue WHERE thread_id = 'thread-attachment-gate'"
        )
        await runtime.shutdown()

    assert bridge.handle.sent == []
    assert queue["state"] == "blocked_attachment_unavailable"


@pytest.mark.asyncio
async def test_attachment_integrity_failure_blocks_sdk_dispatch(tmp_path: Path) -> None:
    session_id = str(uuid4())
    resolver_calls = 0

    async def reject_corrupt(_manifest_id: str) -> list[Any]:
        nonlocal resolver_calls
        resolver_calls += 1
        raise AttachmentError("attachment integrity check failed")

    async with Database(tmp_path / "attachment-integrity.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-attachment-integrity",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-attachment-integrity",
            binding=binding,
            attachment_resolver=reject_corrupt,
        )
        await runtime.attach_create()
        await database.execute(
            """
            INSERT INTO attachment_manifests(
                id, source_kind, source_id, session_id, state,
                total_bytes, created_at
            ) VALUES ('manifest-corrupt', 'test', 'source', ?, 'ready', 1, 1)
            """,
            (session_id,),
        )

        with pytest.raises(AttachmentError):
            await runtime.send(
                "must verify attachment",
                idempotency_key="attachment-integrity",
                attachment_manifest_id="manifest-corrupt",
            )
        await runtime.shutdown()

    assert resolver_calls == 1
    assert bridge.handle.sent == []


@pytest.mark.asyncio
async def test_capability_runtime_unknown_states_gate_dispatch(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "unknown-gates.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-unknown-gates",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-unknown-gates",
            binding=binding,
            capabilities=CapabilityRegistry(
                Settings(_env_file=None, data_dir=tmp_path)
            ).load_checked(),
        )
        await runtime.attach_create()

        await database.execute(
            "UPDATE session_bindings SET runtime_agent = 'unknown' WHERE sdk_session_id = ?",
            (session_id,),
        )
        with pytest.raises(SessionNotReady, match="runtime_agent_unknown"):
            await runtime._assert_dispatchable()
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_agent = 'default', runtime_session_config_version = NULL
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        with pytest.raises(SessionNotReady, match="runtime_session_config_unknown"):
            await runtime._assert_dispatchable()
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_session_config_version = desired_session_config_version,
                runtime_remote_mode = 'unknown'
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        with pytest.raises(SessionNotReady, match="runtime_remote_unknown"):
            await runtime._assert_dispatchable()
        await database.execute(
            "UPDATE session_bindings SET runtime_remote_mode = 'off' WHERE sdk_session_id = ?",
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, state, updated_at
            ) VALUES (?, 'unknown-schedule', 'after', 'work', 'unknown', 1)
            """,
            (session_id,),
        )
        with pytest.raises(SessionNotReady, match="runtime_schedules_unknown"):
            await runtime._assert_dispatchable()
        await database.execute(
            "DELETE FROM runtime_schedules WHERE sdk_session_id = ?",
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO background_observations(
                sdk_session_id, runtime_generation, source_event_id,
                task_id, observed_state, last_progress_at
            ) VALUES (?, 1, 'task:unknown', 'task-unknown', 'unknown', 1)
            """,
            (session_id,),
        )
        with pytest.raises(SessionNotReady, match="background_tasks_unknown"):
            await runtime._assert_dispatchable()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_requested_snapshot_epoch_blocks_dispatch_until_applied(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "snapshot-gate.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-snapshot-gate",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-snapshot-gate",
            binding=binding,
        )
        await runtime.attach_create()
        assert runtime.inbox is not None
        await runtime.inbox.commit_internal(
            {
                "type": "copilotd.snapshot.requested",
                "data": {"topic": "tasks"},
            },
            source="snapshot",
            internal_event_id="snapshot-request:test-stale-task",
        )

        with pytest.raises(SessionNotReady, match="snapshot_tasks_stale"):
            await runtime._assert_dispatchable()
        await runtime._query_snapshot_topic("tasks")
        await runtime._assert_dispatchable()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_queue_blocks_agent_and_session_config_drift(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "queue-drift.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-queue-drift",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-queue-drift",
            binding=binding,
        )
        await runtime.attach_create()

        bridge.processing = True
        first = await runtime.send("agent snapshot", idempotency_key="agent-drift")
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_agent = 'custom-agent'
            WHERE thread_id = 'thread-queue-drift'
            """
        )
        bridge.processing = False
        assert await runtime._dispatch_next_queued() is None
        first_row = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (first,),
        )

        bridge.processing = True
        second = await runtime.send("config snapshot", idempotency_key="config-drift")
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_session_config_version = desired_session_config_version + 1
            WHERE thread_id = 'thread-queue-drift'
            """
        )
        bridge.processing = False
        assert await runtime._dispatch_next_queued() is None
        second_row = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (second,),
        )

        assert first_row["state"] == "blocked_agent_drift"
        assert second_row["state"] == "blocked_session_config_drift"
        assert bridge.handle.sent == []
        replacement = await runtime.resubmit_queue_item(
            first,
            idempotency_key="retry-agent-drift",
        )
        replacement_row = await database.fetchone(
            """
            SELECT state, replaces_id, requested_agent_snapshot
            FROM message_queue WHERE id = ?
            """,
            (replacement,),
        )
        original_row = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (first,),
        )
        assert original_row["state"] == "cancelled"
        assert dict(replacement_row) == {
            "state": "local_queued",
            "replaces_id": first,
            "requested_agent_snapshot": "custom-agent",
        }
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_resubmit_queue_item_atomically_rejects_headroom_and_takeover_races(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "resubmit-fence.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-resubmit-fence",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        owner_leases = OwnerLeaseStore(database)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=owner_leases,
            owner_id="process-original",
            binding=binding,
        )
        await runtime.attach_create()
        bridge.processing = True
        original_id = await runtime.send(
            "retry after drift",
            idempotency_key="resubmit-fence-source",
        )
        await runtime._block_queue_item(original_id, "blocked_mode_drift")

        await database.execute(
            """
            UPDATE session_owner_leases
            SET expires_at = ?
            WHERE sdk_session_id = ?
            """,
            (time.time() + 5, session_id),
        )
        with pytest.raises(FenceLost, match="mutation headroom"):
            await runtime.resubmit_queue_item(
                original_id,
                idempotency_key="insufficient-headroom",
            )
        assert (
            await database.fetchone(
                "SELECT 1 FROM submissions WHERE parent_submission_id = ?",
                (original_id,),
            )
            is None
        )

        assert runtime._lease is not None
        runtime._lease = await owner_leases.renew(runtime._lease)
        initial_check_done = asyncio.Event()
        allow_transaction = asyncio.Event()
        original_assert_owned_handle = runtime._assert_owned_handle

        async def pause_after_initial_owner_check(
            *,
            allow_closing: bool = False,
            allow_attaching: bool = False,
        ) -> None:
            await original_assert_owned_handle(
                allow_closing=allow_closing,
                allow_attaching=allow_attaching,
            )
            initial_check_done.set()
            await allow_transaction.wait()

        runtime._assert_owned_handle = pause_after_initial_owner_check  # type: ignore[method-assign]
        resubmit = asyncio.create_task(
            runtime.resubmit_queue_item(
                original_id,
                idempotency_key="takeover-race",
            )
        )
        await initial_check_done.wait()
        await database.execute(
            """
            UPDATE session_owner_leases
            SET expires_at = 0
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        takeover = await owner_leases.acquire(session_id, "process-takeover")
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_generation = runtime_generation + 1,
                owner_fence_token = ?
            WHERE sdk_session_id = ?
            """,
            (takeover.fence_token, session_id),
        )
        allow_transaction.set()

        with pytest.raises(FenceLost, match="changed before queue resubmission"):
            await resubmit
        original = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = ?",
            (original_id,),
        )
        replacements = await database.fetchall(
            """
            SELECT submission_id FROM submissions
            WHERE parent_submission_id = ?
            """,
            (original_id,),
        )
        replacement_leases = await database.fetchall(
            """
            SELECT source_id FROM liveness_leases
            WHERE sdk_session_id = ? AND source_id != ?
            """,
            (session_id, original_id),
        )

        assert original["state"] == "blocked_mode_drift"
        assert replacements == []
        assert replacement_leases == []
        runtime._assert_owned_handle = original_assert_owned_handle  # type: ignore[method-assign]
        with pytest.raises(ExceptionGroup, match="session component shutdown failed"):
            await runtime._stop_components(release_owner=False)


@pytest.mark.asyncio
async def test_permission_change_event_reconciles_allow_all_before_next_send(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "permission.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
        )
        await runtime.attach_create()
        bridge.ingress(
            _event(
                SessionPermissionsChangedData(
                    allow_all_permissions=False,
                    previous_allow_all_permissions=True,
                ),
                SessionEventType.SESSION_PERMISSIONS_CHANGED,
            )
        )
        assert runtime.inbox is not None
        await runtime.inbox.join()
        for _ in range(50):
            reconciled = await bindings.by_thread("thread-1")
            if (
                bridge.allow_all_calls >= 2
                and reconciled is not None
                and reconciled.permission_posture == PermissionPosture.VERIFIED_ALLOW_ALL
            ):
                break
            await asyncio.sleep(0.005)
        assert reconciled is not None
        assert reconciled.permission_posture == PermissionPosture.VERIFIED_ALLOW_ALL

        message_id = await runtime.send(
            "dispatch after reconciliation",
            idempotency_key="reconciled-message",
        )
        bridge.ingress(
            _event(
                UserMessageData(content="dispatch after reconciliation"),
                SessionEventType.USER_MESSAGE,
                event_id=UUID(message_id),
            )
        )
        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        await runtime.inbox.join()
        assert len(bridge.handle.sent) == 1
        await runtime.close(idempotency_key="close-after-reconciliation")


@pytest.mark.asyncio
async def test_permission_reconciliation_persists_platform_blocked_posture(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "permission-blocked.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-permission-blocked",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-permission-blocked",
            binding=binding,
        )
        await runtime.attach_create()
        bridge.permission_error = PermissionPostureError("managed policy blocked allow-all")
        bridge.ingress(
            _event(
                SessionPermissionsChangedData(
                    allow_all_permissions=False,
                    previous_allow_all_permissions=True,
                ),
                SessionEventType.SESSION_PERMISSIONS_CHANGED,
            )
        )
        assert runtime.inbox is not None
        for _ in range(50):
            blocked = await bindings.by_thread("thread-permission-blocked")
            if (
                blocked is not None
                and blocked.permission_posture == PermissionPosture.PLATFORM_BLOCKED
            ):
                break
            await asyncio.sleep(0.005)
        assert blocked is not None
        assert blocked.permission_posture == PermissionPosture.PLATFORM_BLOCKED
        with pytest.raises(SessionNotReady, match="permission posture"):
            await runtime.send("blocked", idempotency_key="blocked")
        await runtime.close(idempotency_key="close-permission-blocked")


@pytest.mark.asyncio
async def test_bare_mode_changes_do_not_send_a_prompt(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "mode.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
        )
        await runtime.attach_create()

        assert await runtime.set_mode("autopilot", idempotency_key="autopilot-on") == "autopilot"
        updated = await bindings.by_thread("thread-1")

        assert bridge.mode_set_calls == ["autopilot"]
        assert bridge.handle.sent == []
        assert updated is not None
        assert updated.desired_mode == "autopilot"
        assert updated.runtime_mode == "autopilot"
        assert updated.pending_mode is None
        await runtime.close(idempotency_key="mode-close")


@pytest.mark.asyncio
async def test_model_change_is_validated_confirmed_and_persisted(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "model.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-model",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-model",
            binding=binding,
        )
        await runtime.attach_create()

        observed = await runtime.set_model(
            "gpt-test",
            reasoning_effort="high",
            context_tier="long_context",
            idempotency_key="model-change-1",
        )
        row = await database.fetchone(
            """
            SELECT desired_model_config, pending_model_config, runtime_model_config
            FROM session_bindings WHERE thread_id = 'thread-model'
            """
        )

        assert observed == {
            "modelId": "gpt-test",
            "reasoningEffort": "high",
            "contextTier": "long_context",
        }
        assert row["pending_model_config"] is None
        assert '"modelId": "gpt-test"' in row["desired_model_config"]
        assert row["runtime_model_config"] == row["desired_model_config"]
        assert await runtime.context_snapshot() == {"totalTokens": 10, "limit": 100}
        assert await runtime.usage_snapshot() == {"totalUserRequests": 2}

        with pytest.raises(ValueError, match="does not support"):
            await runtime.set_model(
                "gpt-test",
                reasoning_effort="xhigh",
                context_tier=None,
                idempotency_key="model-change-2",
            )
        assert len(bridge.model_set_calls) == 1
        await runtime.close(idempotency_key="close-model")


@pytest.mark.asyncio
async def test_reasoning_summary_uses_capability_adapter_and_readback(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "model-summary.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-model-summary",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-model-summary",
            binding=binding,
            model_summary_adapter=FakeSummaryAdapter(),
        )
        await runtime.attach_create()

        observed = await runtime.set_model(
            "gpt-test",
            reasoning_effort="high",
            reasoning_summary="concise",
            context_tier="default",
            idempotency_key="summary-change",
        )
        row = await database.fetchone(
            """
            SELECT desired_model_config, runtime_model_config
            FROM session_bindings WHERE thread_id = 'thread-model-summary'
            """
        )

        assert observed["reasoningSummary"] == "concise"
        assert '"reasoningSummary": "concise"' in row["desired_model_config"]
        assert row["runtime_model_config"] == row["desired_model_config"]
        await runtime.close(idempotency_key="close-model-summary")


@pytest.mark.asyncio
async def test_context_and_usage_return_durable_stale_snapshots(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "projection-fallback.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-projection",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-projection",
            binding=binding,
        )
        await runtime.attach_create()
        assert await runtime.context_snapshot() == {"totalTokens": 10, "limit": 100}
        assert await runtime.usage_snapshot() == {"totalUserRequests": 2}

        async def fail_context(_session: FakeHandle) -> dict[str, Any]:
            raise ConnectionError("context unavailable")

        async def fail_usage(_session: FakeHandle) -> dict[str, Any]:
            raise ConnectionError("usage unavailable")

        bridge.get_context = fail_context
        bridge.get_usage = fail_usage
        stale_context = await runtime.context_snapshot()
        stale_usage = await runtime.usage_snapshot()

        assert stale_context is not None
        assert stale_context["totalTokens"] == 10
        assert stale_context["_stale"] is True
        assert stale_context["_error"] == "ConnectionError"
        assert stale_usage["totalUserRequests"] == 2
        assert stale_usage["_stale"] is True
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_steer_requires_observed_active_work(tmp_path: Path) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "steer.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-steer",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-steer",
            binding=binding,
        )
        await runtime.attach_create()

        with pytest.raises(CDSessionStateError, match="observed active"):
            await runtime.steer("too early", idempotency_key="inactive")

        bridge.processing = True
        await runtime.steer("correct course", idempotency_key="active")

        assert bridge.handle.sent[-1] == (
            "correct course",
            {
                "attachments": None,
                "mode": "immediate",
                "agent_mode": "interactive",
            },
        )
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_taskdeck_actions_are_state_gated_and_adapter_backed(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "task-actions.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-task-actions",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        actions = FakeTaskActionAdapter()
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-task-actions",
            binding=binding,
            task_action_adapter=actions,
        )
        await runtime.attach_create()
        await database.execute(
            """
            INSERT INTO render_messages(
                session_id, logical_key, discord_message_id, content_hash, finalized
            ) VALUES (?, 'taskdeck', 'message-taskdeck', 'hash', 0)
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO task_card_projections(
                sdk_session_id, panel_id, card_token, card_key, task_id, agent_id,
                kind, title, state, progress_summary, detail_artifact,
                dependencies_json, artifact_links_json, can_promote,
                first_seen_at, revision, last_progress_at
            ) VALUES (?, 'panel', 'card', 'task:1', 'task-1', 'agent-1',
                      'agent', 'Worker', 'running', 'working', 'full detail',
                      '[]', '[]', 1, 1, 1, 1)
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO taskdeck_panel_state(
                sdk_session_id, panel_id, selected_card_token,
                page, expanded, updated_at
            ) VALUES (?, 'panel', 'card', 0, 0, 1)
            """,
            (session_id,),
        )

        stale = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=0,
            action="cancel",
            message_id="message-taskdeck",
            interaction_id="stale",
        )
        promoted = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=1,
            action="promote",
            message_id="message-taskdeck",
            interaction_id="promote",
        )
        promoted_duplicate = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=1,
            action="promote",
            message_id="message-taskdeck",
            interaction_id="promote",
        )
        messaged = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=1,
            action="message",
            message_id="message-taskdeck",
            interaction_id="message",
            message="status please",
        )
        downloaded = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=1,
            action="download",
            message_id="message-taskdeck",
            interaction_id="download",
        )
        await database.execute(
            """
            UPDATE task_card_projections
            SET state = 'completed', terminal_at = 2
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        removed = await runtime.perform_taskdeck_action(
            panel_id="panel",
            card_token="card",
            expected_revision=1,
            action="remove",
            message_id="message-taskdeck",
            interaction_id="remove",
        )

        assert stale["status"] == "stale"
        assert promoted["status"] == "updated"
        assert promoted_duplicate["status"] == "updated"
        assert messaged["status"] == "updated"
        assert downloaded["status"] == "download"
        assert downloaded["content"] == "full detail"
        assert removed["status"] == "updated"
        assert actions.calls == [
            ("promote", "task-1", None),
            ("message", "task-1", "status please"),
            ("remove", "task-1", None),
        ]
        assert (
            await database.fetchone(
                "SELECT 1 FROM task_card_projections WHERE sdk_session_id = ?",
                (session_id,),
            )
            is None
        )
        await database.execute(
            """
            INSERT INTO task_card_projections(
                sdk_session_id, panel_id, card_token, card_key, task_id,
                kind, title, state, dependencies_json, artifact_links_json,
                can_promote, first_seen_at, revision, last_progress_at
            ) VALUES (?, 'panel', 'card-fenced', 'task:2', 'task-2',
                      'background', 'Fenced worker', 'running', '[]', '[]',
                      0, 2, 1, 2)
            """,
            (session_id,),
        )
        await database.execute(
            """
            UPDATE session_owner_leases SET expires_at = 0
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        with pytest.raises(OperationAmbiguous, match="owner fence lost"):
            await runtime.perform_taskdeck_action(
                panel_id="panel",
                card_token="card-fenced",
                expected_revision=1,
                action="cancel",
                message_id="message-taskdeck",
                interaction_id="fenced-cancel",
            )
        assert all(call[0] != "cancel" for call in actions.calls)
        await database.execute(
            """
            UPDATE session_owner_leases
            SET expires_at = CAST(strftime('%s', 'now') AS REAL) + 60
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_busy_runtime_keeps_durable_fifo_and_dispatches_only_the_head(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "durable-queue.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-queue",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        bridge.processing = True
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-queue",
            binding=binding,
            queue_poll_seconds=60,
        )
        await runtime.attach_create()

        queued_id = await runtime.send(
            "first queued prompt",
            idempotency_key="queued-1",
        )
        queued = await database.fetchone(
            "SELECT id, prompt, state FROM message_queue WHERE thread_id = 'thread-queue'"
        )
        assert bridge.handle.sent == []
        assert dict(queued) == {
            "id": queued_id,
            "prompt": "first queued prompt",
            "state": "local_queued",
        }

        bridge.processing = False
        dispatched = await runtime._dispatch_next_queued()
        assert dispatched is not None
        assert dispatched[0] == queued_id
        assert [item[0] for item in bridge.handle.sent] == ["first queued prompt"]
        accepted_id = dispatched[1]

        second_id = await runtime.send(
            "second queued prompt",
            idempotency_key="queued-2",
        )
        assert second_id != accepted_id
        assert [item[0] for item in bridge.handle.sent] == ["first queued prompt"]

        bridge.ingress(
            _event(
                UserMessageData(content="first queued prompt"),
                SessionEventType.USER_MESSAGE,
                event_id=uuid4(),
            )
        )
        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        assert runtime.inbox is not None
        await runtime.inbox.join()
        correlation = await database.fetchone(
            """
            SELECT correlation_basis FROM submissions
            WHERE accepted_message_id = ?
            """,
            (accepted_id,),
        )
        assert correlation["correlation_basis"] == "single_candidate_facts"

        second_dispatch = await runtime._dispatch_next_queued()
        assert second_dispatch is not None
        assert second_dispatch[0] == second_id
        assert [item[0] for item in bridge.handle.sent] == [
            "first queued prompt",
            "second queued prompt",
        ]
        readiness = await database.fetchone(
            """
            SELECT runtime_processing, runtime_has_active_work,
                   native_queue_count, native_steering_count, queue_observed_at
            FROM session_bindings WHERE thread_id = 'thread-queue'
            """
        )
        assert dict(readiness) == {
            "runtime_processing": 0,
            "runtime_has_active_work": 0,
            "native_queue_count": 0,
            "native_steering_count": 0,
            "queue_observed_at": pytest.approx(readiness["queue_observed_at"]),
        }

        second_accepted_id = second_dispatch[1]
        bridge.ingress(
            _event(
                UserMessageData(content="second queued prompt"),
                SessionEventType.USER_MESSAGE,
                event_id=UUID(second_accepted_id),
            )
        )
        bridge.ingress(_event(SessionIdleData(), SessionEventType.SESSION_IDLE))
        await runtime.inbox.join()
        await runtime.close(idempotency_key="close-queue")
