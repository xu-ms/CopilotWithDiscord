import asyncio
import os
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.core.sessions import SessionRegistry
from copilotd.ops.control import ServiceControlWorker
from copilotd.ops.service import SqliteRestartCoordinator
from copilotd.storage.database import Database


class FakeSessions:
    def __init__(self) -> None:
        self.begun = 0
        self.ended = 0
        self.depth = 0
        self.violations = 0
        self.on_violation: Callable[[], None] | None = None

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[], None],
    ) -> None:
        self.begun += 1
        self.on_violation = on_violation

    async def end_service_quiesce(self) -> None:
        self.ended += 1

    def service_quiesce_metrics(self) -> tuple[int, int]:
        return self.depth, self.violations


class FakeRuntime:
    def __init__(self, thread_id: str) -> None:
        self.binding = SimpleNamespace(thread_id=thread_id)
        self.begun = 0
        self.ended = 0

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[], None],
    ) -> None:
        del on_violation
        self.begun += 1

    async def end_service_quiesce(self) -> None:
        self.ended += 1

    def service_quiesce_metrics(self) -> tuple[int, int]:
        return 0, 0


@pytest.mark.asyncio
async def test_service_control_acknowledges_drain_and_detects_late_ingress(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "copilotd.sqlite3"
    sessions = FakeSessions()
    generation = "generation-1"
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            now=time.time(),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                poll_seconds=0.005,
            ).run()
        )
        try:
            acknowledged = await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            assert sessions.begun == 1
            assert acknowledged.ingress_depth == 0

            sessions.violations = 1
            assert sessions.on_violation is not None
            await asyncio.to_thread(sessions.on_violation)
            for _ in range(100):
                row = await database.fetchone(
                    """
                    SELECT state FROM service_admission_fences
                    WHERE fence_id = ?
                    """,
                    (fence.fence_id,),
                )
                if row is not None and row["state"] == "violated":
                    break
                await asyncio.sleep(0.005)
            assert row is not None and row["state"] == "violated"

            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            for _ in range(100):
                if sessions.ended:
                    break
                await asyncio.sleep(0.005)
            assert sessions.ended == 1
        finally:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_registry_quiesce_waits_for_active_create_and_blocks_new_runtime() -> None:
    registry = SessionRegistry(
        SimpleNamespace(),
        lambda binding: FakeRuntime(binding.thread_id),
    )
    runtime = FakeRuntime("thread-1")
    violations = 0

    def violated() -> None:
        nonlocal violations
        violations += 1

    async with registry.creation_admission():
        quiesce = asyncio.create_task(
            registry.begin_service_quiesce(violated)
        )
        await asyncio.sleep(0)
        assert not quiesce.done()
        registry.register(runtime)
    await quiesce

    assert runtime.begun == 1
    with pytest.raises(RuntimeError, match="admission is quiesced"):
        registry.register(FakeRuntime("thread-2"))
    assert violations == 1
    assert registry.service_quiesce_metrics() == (0, 1)
    await registry.end_service_quiesce()
    registry.register(FakeRuntime("thread-2"))
