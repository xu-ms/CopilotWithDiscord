import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from copilot.generated.session_events import SessionMode
from copilot.session_events import (
    AssistantMessageDeltaData,
    SessionBackgroundTasksChangedData,
    SessionEvent,
    SessionEventType,
    SessionIdleData,
    SessionModeChangedData,
    SessionPermissionsChangedData,
    UserMessageData,
)

from copilotd.core.bindings import (
    AttachmentState,
    BindingIntent,
    PermissionPosture,
    SessionBindingRepository,
)
from copilotd.core.mailbox import OperationAmbiguous, OperationRejected
from copilotd.core.session_runtime import (
    DetachBlocked,
    RuntimeState,
    SessionAttachUnknown,
    SessionNotReady,
    SessionRuntime,
)
from copilotd.core.sessions import SessionRegistry
from copilotd.sdk.bridge import PermissionPostureError
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerConflict, OwnerLeaseStore


class FakeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.message_id = str(uuid4())
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.abort_calls = 0
        self.disconnect_calls = 0

    async def send(self, prompt: str, **kwargs: Any) -> str:
        self.sent.append((prompt, kwargs))
        self.message_id = str(uuid4())
        return self.message_id

    async def abort(self) -> None:
        self.abort_calls += 1

    async def disconnect(self) -> None:
        self.disconnect_calls += 1


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
        self.allow_all_calls = 0
        self.permission_error: Exception | None = None

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
    ) -> None:
        self.model = {
            "modelId": model,
            "reasoningEffort": reasoning_effort,
            "contextTier": context_tier,
        }
        self.model_set_calls.append(self.model)

    async def get_current_model(self, _session: FakeHandle) -> dict[str, Any]:
        return self.model

    async def get_context(self, _session: FakeHandle) -> dict[str, Any]:
        return {"totalTokens": 10, "limit": 100}

    async def get_usage(self, _session: FakeHandle) -> dict[str, Any]:
        return {"totalUserRequests": 2}

    async def get_readiness(self, _session: FakeHandle) -> dict[str, Any]:
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
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-1",
            binding=binding,
            owner_renew_seconds=30,
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
            "state": "loop_idle",
            "accepted_message_id": message_id,
            "correlation_basis": "accepted_event_id",
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

        assert (
            await runtime.respond_interaction(interaction_id, selection=1)
            == "resolved"
        )
        assert await response_task == {"answer": "second", "wasFreeform": False}
        assert (
            await runtime.respond_interaction(interaction_id, selection=0)
            == "expired"
        )

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
        assert all(
            row["coalesce_key"] == f"interaction:{interaction_id}"
            for row in render_rows
        )

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
        interaction = await database.fetchone(
            "SELECT state FROM pending_interactions"
        )
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
        send_task = asyncio.create_task(
            runtime.send("racing send", idempotency_key="racing-send")
        )
        await admission_started.wait()
        close_task = asyncio.create_task(
            runtime.close(idempotency_key="racing-close")
        )
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
        ) -> bool:
            claim_started.set()
            await allow_claim.wait()
            return await original_claim(
                submission_id,
                operation_idempotency_key=operation_idempotency_key,
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
        for _ in range(50):
            queue_row = await database.fetchone(
                "SELECT state FROM message_queue WHERE thread_id = ?",
                ("thread-force-close-claim",),
            )
            if queue_row is not None and queue_row["state"] == "cancelled":
                break
            await asyncio.sleep(0.005)
        assert queue_row is not None and queue_row["state"] == "cancelled"

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
        await runtime.shutdown()


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
                and reconciled.permission_posture
                == PermissionPosture.VERIFIED_ALLOW_ALL
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
        await database.execute(
            """
            UPDATE liveness_leases
            SET state = 'orphaned',
                runtime_generation = runtime_generation - 1,
                owner_fence_token = owner_fence_token - 1,
                released_at = refreshed_at
            WHERE kind = 'submission' AND source_id = ?
            """,
            (queued_id,),
        )

        bridge.processing = False
        dispatched = await runtime._dispatch_next_queued()
        assert dispatched is not None
        assert dispatched[0] == queued_id
        assert [item[0] for item in bridge.handle.sent] == ["first queued prompt"]
        reactivated = await database.fetchone(
            """
            SELECT state, runtime_generation, owner_fence_token
            FROM liveness_leases
            WHERE kind = 'submission' AND source_id = ? AND state = 'active'
            """,
            (queued_id,),
        )
        assert dict(reactivated) == {
            "state": "active",
            "runtime_generation": runtime.binding.runtime_generation,
            "owner_fence_token": runtime.binding.owner_fence_token,
        }
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
        assert correlation["correlation_basis"] == "single_unambiguous_candidate"

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


@pytest.mark.asyncio
async def test_owner_acquisition_conflict_retries_and_resets_attaching_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        store = OwnerLeaseStore(database)
        calls = 0

        async def conflict(*_args: Any, **_kwargs: Any) -> None:
            nonlocal calls
            calls += 1
            raise OwnerConflict("old owner still active")

        async def no_wait(_delay: float) -> None:
            return

        monkeypatch.setattr(store, "acquire", conflict)
        monkeypatch.setattr(
            "copilotd.core.session_runtime.asyncio.sleep",
            no_wait,
        )
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=store,
            owner_id="replacement-owner",
            binding=binding,
        )

        with pytest.raises(OwnerConflict):
            await runtime.attach_resume()

        assert calls == 5
        assert runtime.state == RuntimeState.DETACHED
        assert runtime.handle is None
        assert runtime.inbox is None


@pytest.mark.asyncio
async def test_owner_acquisition_retry_can_attach_after_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "owner-handoff-retry.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-owner-handoff-retry",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        store = OwnerLeaseStore(database)
        acquire = store.acquire
        calls = 0

        async def transient_conflict(*args: Any, **kwargs: Any):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise OwnerConflict("handoff not committed yet")
            return await acquire(*args, **kwargs)

        async def no_wait(_delay: float) -> None:
            return

        monkeypatch.setattr(store, "acquire", transient_conflict)
        monkeypatch.setattr(
            "copilotd.core.session_runtime.asyncio.sleep",
            no_wait,
        )
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=store,
            owner_id="replacement-owner",
            binding=binding,
        )

        await runtime.attach_resume()

        assert calls == 3
        assert runtime.state == RuntimeState.READY
        await runtime.close(idempotency_key="close-owner-handoff-retry")


@pytest.mark.asyncio
async def test_attach_cancellation_during_owner_acquire_resets_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "attach-cancel-owner.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-attach-cancel-owner",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        store = OwnerLeaseStore(database)
        started = asyncio.Event()
        never = asyncio.Event()

        async def blocked_acquire(*_args: Any, **_kwargs: Any):
            started.set()
            await never.wait()

        monkeypatch.setattr(store, "acquire", blocked_acquire)
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=store,
            owner_id="cancelled-owner",
            binding=binding,
        )
        attach = asyncio.create_task(runtime.attach_resume())
        await started.wait()
        attach.cancel()
        with pytest.raises(asyncio.CancelledError):
            await attach

        assert runtime.state == RuntimeState.DETACHED
        assert await store.current(session_id) is None


@pytest.mark.asyncio
async def test_attach_cancellation_during_resume_releases_owner_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "attach-cancel-resume.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-attach-cancel-resume",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        started = asyncio.Event()
        never = asyncio.Event()

        async def blocked_resume(**_kwargs: Any):
            started.set()
            await never.wait()

        monkeypatch.setattr(bridge, "resume_session", blocked_resume)
        store = OwnerLeaseStore(database)
        runtime = SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=store,
            owner_id="cancelled-resume-owner",
            binding=binding,
        )
        attach = asyncio.create_task(runtime.attach_resume())
        await started.wait()
        attach.cancel()
        with pytest.raises(asyncio.CancelledError):
            await attach

        recovered = await bindings.by_thread(binding.thread_id)
        owner = await store.current(session_id)
        assert runtime.state == RuntimeState.RECOVERY_UNKNOWN
        assert recovered is not None
        assert recovered.attachment_state == AttachmentState.RECOVERY_UNKNOWN
        assert owner is not None and owner.expires_at <= time.time()
        assert runtime.inbox is None


@pytest.mark.asyncio
async def test_runtime_reactivation_is_single_flight_for_concurrent_messages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "single-flight-resume.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-single-flight",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        bridge = FakeBridge(session_id)
        entered = asyncio.Event()
        release = asyncio.Event()
        resume_calls = 0
        resume = bridge.resume_session

        async def delayed_resume(**kwargs: Any):
            nonlocal resume_calls
            resume_calls += 1
            entered.set()
            await release.wait()
            return await resume(**kwargs)

        monkeypatch.setattr(bridge, "resume_session", delayed_resume)

        def runtime_factory(current_binding):
            return SessionRuntime(
                database=database,
                bridge=bridge,
                bindings=bindings,
                owner_leases=OwnerLeaseStore(database),
                owner_id="single-flight-owner",
                binding=current_binding,
            )

        registry = SessionRegistry(bindings, runtime_factory)
        first = asyncio.create_task(registry.ensure_attached(binding))
        await entered.wait()
        second = asyncio.create_task(registry.ensure_attached(binding))
        await asyncio.sleep(0)
        assert not second.done()
        release.set()
        first_runtime, second_runtime = await asyncio.gather(first, second)

        assert first_runtime is second_runtime
        assert first_runtime.state == RuntimeState.READY
        assert resume_calls == 1
        await first_runtime.close(idempotency_key="close-single-flight")


@pytest.mark.asyncio
async def test_service_quiesce_stops_all_internal_inbox_producers(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "quiesce-producers.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-quiesce-producers",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-quiesce-producers",
            binding=binding,
            queue_poll_seconds=60,
        )
        await runtime.attach_create()
        producers: list[str] = []
        losses: list[str] = []
        assert runtime.inbox is not None
        with runtime.inbox._lock:
            runtime.inbox._record_overflow_locked(1, 1)

        await runtime.begin_service_quiesce(
            producers.append,
            losses.append,
        )

        assert runtime._queue_task is None
        assert runtime._task_reconcile_task is None
        assert runtime._permission_reconcile_task is None
        assert runtime._renewal_task is None
        with pytest.raises(SessionNotReady):
            await runtime.send(
                "blocked",
                idempotency_key="blocked-during-quiesce",
            )
        assert runtime.service_quiesce_metrics() == (0, 0)
        assert losses == ["pre_quiesce_inbox_overflow"]

        await runtime.end_service_quiesce()
        assert runtime._queue_task is not None
        assert runtime._task_reconcile_task is not None
        assert runtime._permission_reconcile_task is not None
        assert runtime._renewal_task is not None
        await runtime.close(idempotency_key="close-quiesce-producers")


@pytest.mark.asyncio
async def test_aborted_quiesce_restores_degraded_owner_renewal(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "quiesce-degraded.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-quiesce-degraded",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        runtime = SessionRuntime(
            database=database,
            bridge=FakeBridge(session_id),
            bindings=bindings,
            owner_leases=OwnerLeaseStore(database),
            owner_id="process-quiesce-degraded",
            binding=binding,
            owner_renew_seconds=30,
        )
        await runtime.attach_create()
        runtime.state = RuntimeState.DEGRADED

        await runtime.begin_service_quiesce(
            lambda _source: None,
            lambda _source: None,
        )
        assert runtime._renewal_task is None
        await runtime.end_service_quiesce()

        assert runtime._renewal_task is not None
        assert runtime._queue_task is None
        assert runtime._task_reconcile_task is None
        assert runtime._permission_reconcile_task is None
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_task_reconcile_does_not_call_runtime_after_owner_fence_loss(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "fenced-reconcile.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-fenced-reconcile",
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
            owner_id="process-fenced-reconcile",
            binding=binding,
        )
        await runtime.attach_create()
        for _ in range(100):
            if bridge.task_snapshot_calls:
                break
            await asyncio.sleep(0.005)
        initial_calls = bridge.task_snapshot_calls
        assert initial_calls >= 1

        runtime.state = RuntimeState.FENCED
        runtime._task_reconcile_requested.set()
        assert runtime._task_reconcile_task is not None
        await asyncio.wait_for(runtime._task_reconcile_task, timeout=1)

        assert bridge.task_snapshot_calls == initial_calls
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_task_reconcile_rpc_failure_fences_if_owner_was_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = str(uuid4())
    async with Database(tmp_path / "fenced-reconcile-error.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-fenced-reconcile-error",
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
            owner_id="process-fenced-reconcile-error",
            binding=binding,
        )
        await runtime.attach_create()
        for _ in range(100):
            if bridge.task_snapshot_calls:
                break
            await asyncio.sleep(0.005)

        async def lose_owner_then_fail(_handle: FakeHandle):
            await database.execute(
                """
                UPDATE session_owner_leases SET expires_at = 0
                WHERE sdk_session_id = ?
                """,
                (session_id,),
            )
            raise RuntimeError("transport failed after owner loss")

        monkeypatch.setattr(bridge, "get_tasks", lose_owner_then_fail)
        runtime._task_reconcile_requested.set()
        assert runtime._task_reconcile_task is not None
        await asyncio.wait_for(runtime._task_reconcile_task, timeout=1)

        assert runtime.state == RuntimeState.FENCED
        failed_snapshot = await database.fetchone(
            """
            SELECT COUNT(*) FROM event_journal
            WHERE raw_type = 'copilotd.tasks.snapshot_failed'
            """
        )
        assert failed_snapshot[0] == 0
        await runtime.shutdown()
