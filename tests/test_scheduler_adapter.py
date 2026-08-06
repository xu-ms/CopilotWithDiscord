import asyncio
import time
from pathlib import Path
from typing import Any

import pytest

from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.inbox import ReducerInbox
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
            ) VALUES ('queued', 'thread-1', ?, 'recover me', 'interactive',
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
    assert queue["state"] == "submitted"
    assert recovered.accepted_message_id == "message"
    assert bridge.handle.send_calls == 1
    assert binding is not None and binding.attachment_reason == "scheduler_run"
