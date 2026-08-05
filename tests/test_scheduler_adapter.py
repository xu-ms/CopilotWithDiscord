from pathlib import Path
from typing import Any

import pytest

from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.inbox import ReducerInbox
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.scheduler import ScheduleKind, SchedulerRepository
from copilotd.core.scheduler_adapter import ApplicationSchedulerAdapter
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.sessions import SessionRegistry
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


class FakeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.disconnect_calls = 0

    async def send(self, _prompt: str, **_kwargs: Any) -> str:
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
