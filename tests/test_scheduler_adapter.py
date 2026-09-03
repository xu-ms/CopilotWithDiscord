import asyncio
import hashlib
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.inbox import ReducerInbox
from copilotd.core.recovery import StartupRecoveryInventory
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.scheduler import ScheduleKind, SchedulerRepository, SchedulerWorker
from copilotd.core.scheduler_adapter import ApplicationSchedulerAdapter
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.sessions import SessionRegistry
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


class FakeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.disconnect_calls = 0
        self.send_calls = 0

    async def send(self, _prompt: str, **_kwargs: Any) -> str:
        self.send_calls += 1
        return "message"

    async def abort(self) -> None:
        pass

    async def disconnect(self) -> None:
        self.disconnect_calls += 1


class FakeBridge:
    def __init__(self) -> None:
        self.handle = FakeHandle("session-1")

    async def create_session(self, **_kwargs: Any) -> FakeHandle:
        return self.handle

    async def resume_session(self, **_kwargs: Any) -> FakeHandle:
        return self.handle

    async def ensure_allow_all(self, _session: FakeHandle) -> object:
        return object()

    async def send(
        self,
        session: FakeHandle,
        prompt: str,
        **kwargs: Any,
    ) -> str:
        return await session.send(prompt, **kwargs)

    async def disconnect(self, session: FakeHandle) -> None:
        await session.disconnect()

    async def get_mode(self, _session: FakeHandle) -> str:
        return "interactive"

    async def get_readiness(self, _session: FakeHandle) -> dict[str, Any]:
        return {
            "processing": False,
            "hasActiveWork": False,
            "abortable": False,
            "pendingItems": [],
            "steeringMessages": [],
        }

    async def get_tasks(self, _session: FakeHandle) -> list[dict[str, Any]]:
        return []


class FakeScheduledCreation:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def create_from_source(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(
            binding=SimpleNamespace(
                project_id=None,
                thread_id="created-thread",
                sdk_session_id=kwargs["preallocated_session_id"],
            )
        )


@pytest.mark.asyncio
async def test_closed_message_target_temporarily_attaches_and_returns_to_closed_absent(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "temporary-attach.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="explicit",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent'
            WHERE thread_id = 'thread-1'
            """
        )
        binding = await bindings.by_thread("thread-1")
        assert binding is not None
        bridge = FakeBridge()

        def runtime_factory(item: SessionBinding) -> SessionRuntime:
            return SessionRuntime(
                database=database,
                bridge=bridge,
                bindings=bindings,
                owner_leases=OwnerLeaseStore(database),
                owner_id="owner",
                binding=item,
            )

        sessions = SessionRegistry(bindings, runtime_factory)
        adapter = ApplicationSchedulerAdapter(
            database,
            bindings,
            sessions,
            None,
        )
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={
                "thread_id": "thread-1",
                "sdk_session_id": "session-1",
            },
            thread_id="thread-1",
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="temporary")

        target = await adapter.prepare_message_target(definition, run)
        attached = await bindings.by_thread("thread-1")
        await adapter.release_temporary_target(target, run)
        detached = await bindings.by_thread("thread-1")

        assert target.temporary_attachment
        assert attached is not None and attached.attachment_reason == "scheduler_run"
        assert attached.attachment_state.value == "attached"
        assert detached is not None
        assert detached.binding_intent.value == "closed"
        assert detached.attachment_state.value == "absent"
        assert detached.attachment_reason is None
        assert bridge.handle.disconnect_calls == 1
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_scheduled_custom_thread_name_survives_restart_and_executes_exactly(
    tmp_path: Path,
) -> None:
    path = tmp_path / "scheduled-thread-name.sqlite3"
    async with Database(path) as database:
        definition = await SchedulerRepository(database).create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "scheduled prompt", "thread_name": "Exact scheduled title"},
            target_snapshot={
                "project": {
                    "project_id": None,
                    "channel_id": "channel-1",
                    "source": "implicit-home",
                    "root_path": str(tmp_path),
                    "cwd": str(tmp_path),
                    "config_version": 1,
                    "timezone": "UTC",
                },
                "project_config": {
                    "project_id": None,
                    "source": "implicit-home",
                    "cwd": str(tmp_path),
                    "timezone": "UTC",
                    "config_version": 1,
                },
                "execution_config": {},
            },
            now=1,
            source_channel_id="source-channel",
            source_message_id="source-message",
        )
        schedule_id = definition.id

    async with Database(path) as restarted:
        repository = SchedulerRepository(
            restarted,
            prompt_resolver=lambda _channel, _message: asyncio.sleep(
                0,
                result="scheduled prompt",
            ),
        )
        await repository.recover(now=2)
        definition = await repository.require(schedule_id)
        run = await repository.run_now(schedule_id, now=2, manual_id="restart")
        await restarted.execute(
            "UPDATE schedule_runs SET result_session_id = 'scheduled-session' WHERE run_id = ?",
            (run.run_id,),
        )
        run = await repository.get_run(run.run_id)
        creation = FakeScheduledCreation()
        adapter = ApplicationSchedulerAdapter(
            restarted,
            SessionBindingRepository(restarted),
            SimpleNamespace(),  # type: ignore[arg-type]
            creation,  # type: ignore[arg-type]
        )

        target = await adapter.prepare_new_session_target(definition, run)

    assert definition.thread_name == "Exact scheduled title"
    assert target.thread_id == "created-thread"
    assert creation.calls[0]["thread_name"] == "Exact scheduled title"


@pytest.mark.asyncio
async def test_late_shutdown_event_preserves_explicit_closed_absent_binding(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "late-shutdown.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                binding_intent, attachment_state, runtime_generation,
                owner_fence_token, created_at, updated_at
            ) VALUES ('thread-1', 'explicit', '/tmp', 'session-1',
                      'closed', 'absent', 1, 7, 0, 0)
            """
        )
        inbox = ReducerInbox(
            sdk_session_id="session-1",
            generation=1,
            fence_token=7,
            capacity=16,
        )
        worker = EventReducerWorker(
            inbox=inbox,
            reducer=JournalReducer(database),
            batch_size=4,
        )
        worker.start()
        await inbox.commit_internal(
            {"type": "session.shutdown", "data": {"shutdownType": "routine"}},
            internal_event_id="late-shutdown",
        )
        binding = await database.fetchone(
            """
            SELECT binding_intent, attachment_state
            FROM session_bindings WHERE thread_id = 'thread-1'
            """
        )
        await worker.stop()

    assert dict(binding) == {
        "binding_intent": "closed",
        "attachment_state": "absent",
    }


@pytest.mark.asyncio
async def test_closed_scheduler_queue_reattaches_and_redrives_after_startup_lease_expiry(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "closed-queue-recovery.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-1",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="explicit",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent'
            WHERE thread_id = 'thread-1'
            """
        )
        lease_expiry = time.time() + 4
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-1', 'killed-owner', 1, 0, 0, ?)
            """,
            (lease_expiry,),
        )
        bridge = FakeBridge()

        def runtime_factory(item: SessionBinding) -> SessionRuntime:
            return SessionRuntime(
                database=database,
                bridge=bridge,
                bindings=bindings,
                owner_leases=OwnerLeaseStore(database),
                owner_id="restarted-owner",
                binding=item,
            )

        sessions = SessionRegistry(bindings, runtime_factory)
        repository = SchedulerRepository(database)
        await repository.recover()
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "recover me"},
            target_snapshot={
                "thread_id": "thread-1",
                "sdk_session_id": "session-1",
            },
            thread_id="thread-1",
        )
        run = await repository.run_now(definition.id, manual_id="crashed")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                prompt_hash, requested_mode, requested_model_config,
                requested_agent, requested_session_config_version,
                requested_delivery, attachment_count, state, created_at
            ) VALUES ('queued', 'session-1', 'app_schedule', ?, 'hash',
                      'interactive', '{}', 'default', 1, 'enqueue', 0,
                      'local_queued', 1)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, schedule_run_id, prompt,
                requested_mode_snapshot, requested_model_config_snapshot,
                requested_agent_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES ('queued', 'thread-1', ?, '', 'interactive',
                      '{}', 'default', 1, 1, 'local_queued', 1, 1)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', result_submission_id = 'queued',
                result_thread_id = 'thread-1', result_session_id = 'session-1',
                temporary_attachment = 1
            WHERE run_id = ?
            """,
            (run.run_id,),
        )
        failures = await sessions.eager_resume()
        adapter = ApplicationSchedulerAdapter(database, bindings, sessions, None)
        worker = SchedulerWorker(repository, adapter, owner_id="restarted-scheduler")

        await worker.tick()
        before_expiry = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = 'queued'"
        )
        await asyncio.sleep(max(0, lease_expiry - time.time()) + 0.05)
        await worker.tick()
        await asyncio.sleep(0)
        await worker.tick()
        recovered = await repository.get_run(run.run_id)
        queue = await database.fetchone("SELECT state FROM message_queue WHERE id = 'queued'")
        binding = await bindings.by_thread("thread-1")
        await sessions.shutdown()

    assert "thread-1" in failures
    assert before_expiry["state"] == "local_queued"
    assert queue["state"] == "content_unavailable"
    assert recovered.status.value == "failed"
    assert recovered.error_code == "content_unavailable"
    assert recovered.accepted_message_id is None
    assert bridge.handle.send_calls == 0
    assert binding is not None and binding.attachment_reason == "scheduler_run"


@pytest.mark.asyncio
async def test_source_backed_schedule_queue_rehydrates_and_dispatches_after_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-backed-queue-restart.sqlite3"
    prompt = "recover source-backed schedule"
    prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
    async with Database(path) as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="source-thread",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="explicit",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent'
            WHERE thread_id = 'source-thread'
            """
        )
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": prompt},
            target_snapshot={
                "thread_id": "source-thread",
                "sdk_session_id": "session-1",
            },
            thread_id="source-thread",
            source_channel_id="source-channel",
            source_message_id="source-message",
        )
        run = await repository.run_now(definition.id, manual_id="source-restart")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                prompt_hash, requested_mode, requested_model_config,
                requested_agent, requested_session_config_version,
                requested_delivery, attachment_count, state, created_at
            ) VALUES (
                'source-queued', 'session-1', 'app_schedule', ?,
                ?, 'interactive', '{}', 'default', 1, 'enqueue', 0,
                'local_queued', 1
            )
            """,
            (run.run_id, prompt_hash),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, schedule_run_id, prompt, prompt_content_key,
                prompt_hash, requested_mode_snapshot,
                requested_model_config_snapshot, requested_agent_snapshot,
                requested_session_config_version, position, state,
                created_at, updated_at
            ) VALUES (
                'source-queued', 'source-thread', ?, '', 'vc:missing',
                ?, 'interactive', '{}', 'default', 1, 1,
                'local_queued', 1, 1
            )
            """,
            (run.run_id, prompt_hash),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', result_submission_id = 'source-queued',
                result_thread_id = 'source-thread',
                result_session_id = 'session-1', temporary_attachment = 1
            WHERE run_id = ?
            """,
            (run.run_id,),
        )

    resolved: list[tuple[str, str]] = []

    async def resolve_prompt(channel_id: str, message_id: str) -> str:
        resolved.append((channel_id, message_id))
        return prompt

    async with Database(path) as restarted:
        inventory = await StartupRecoveryInventory(restarted).run()
        queued_before = await restarted.fetchone(
            "SELECT state FROM message_queue WHERE id = 'source-queued'"
        )
        repository = SchedulerRepository(restarted, prompt_resolver=resolve_prompt)
        recovery = await repository.recover()
        bindings = SessionBindingRepository(restarted)
        bridge = FakeBridge()

        def runtime_factory(item: SessionBinding) -> SessionRuntime:
            return SessionRuntime(
                database=restarted,
                bridge=bridge,
                bindings=bindings,
                owner_leases=OwnerLeaseStore(restarted),
                owner_id="restarted-source-owner",
                binding=item,
            )

        sessions = SessionRegistry(bindings, runtime_factory)
        worker = SchedulerWorker(
            repository,
            ApplicationSchedulerAdapter(restarted, bindings, sessions, None),
            owner_id="restarted-source-scheduler",
        )
        await worker.tick()
        queue_after = await restarted.fetchone(
            "SELECT state FROM message_queue WHERE id = 'source-queued'"
        )
        await sessions.shutdown()

    assert inventory.unknown_submissions == 0
    assert queued_before["state"] == "local_queued"
    assert recovery["rehydrated_queued_prompts"] == 1
    assert resolved
    assert queue_after["state"] == "submitted"
    assert bridge.handle.send_calls == 1
