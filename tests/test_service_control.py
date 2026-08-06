import asyncio
import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.core.sessions import SessionRegistry
from copilotd.ops.control import (
    ServiceControlWorker,
    fence_marker_paths,
    fence_pending_marker_directory,
)
from copilotd.ops.service import (
    RestartBlocked,
    ServiceError,
    SqliteRestartCoordinator,
)
from copilotd.storage.database import Database


class FakeSessions:
    def __init__(self) -> None:
        self.begun = 0
        self.ended = 0
        self.depth = 0
        self.violations = 0
        self.on_violation: Callable[[str], None] | None = None
        self.produce_during_begin = False
        self.produce_during_first_drain = False
        self.produce_during_first_metrics = False
        self.produce_during_every_metrics = False
        self.drain_calls = 0
        self.metrics_calls = 0
        self.begin_gate: asyncio.Event | None = None
        self.end_gate: asyncio.Event | None = None
        self.ignore_end_cancellation = False
        self.end_error: Exception | None = None

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[str], None],
        on_loss: Callable[[str], None],
    ) -> None:
        del on_loss
        self.begun += 1
        self.on_violation = on_violation
        if self.produce_during_begin:
            on_violation("internal_snapshot")
        if self.begin_gate is not None:
            await self.begin_gate.wait()

    async def end_service_quiesce(self) -> None:
        self.ended += 1
        if self.end_error is not None:
            raise self.end_error
        if self.end_gate is not None:
            try:
                await self.end_gate.wait()
            except asyncio.CancelledError:
                if not self.ignore_end_cancellation:
                    raise
                await self.end_gate.wait()

    async def drain_service_quiesce(self) -> None:
        self.drain_calls += 1
        if (
            self.produce_during_first_drain
            and self.drain_calls == 1
            and self.on_violation is not None
        ):
            self.on_violation("producer_between_epoch_and_drain")
        if self.depth:
            await asyncio.Event().wait()

    def service_quiesce_metrics(self) -> tuple[int, int]:
        self.metrics_calls += 1
        if (
            self.produce_during_first_metrics
            and self.metrics_calls == 1
            and self.on_violation is not None
        ):
            self.on_violation("producer_during_depth_snapshot")
        if self.produce_during_every_metrics and self.on_violation is not None:
            self.on_violation("producer_during_every_depth_snapshot")
        return self.depth, self.violations


class FakeRuntime:
    def __init__(self, thread_id: str) -> None:
        self.binding = SimpleNamespace(thread_id=thread_id)
        self.begun = 0
        self.ended = 0

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[str], None],
        on_loss: Callable[[str], None],
    ) -> None:
        del on_violation, on_loss
        self.begun += 1

    async def end_service_quiesce(self) -> None:
        self.ended += 1

    async def drain_service_quiesce(self) -> None:
        return

    def service_quiesce_metrics(self) -> tuple[int, int]:
        return 0, 0


@pytest.mark.asyncio
async def test_second_restart_rejects_existing_active_fence_cleanly(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "active-fence.sqlite3"
    async with Database(database_path):
        pass
    coordinator = SqliteRestartCoordinator(database_path)
    now = time.time()
    coordinator.request_quiesce(
        expected_pid=123,
        expected_generation="generation-1",
        expected_process_started_at=now - 1,
        now=now,
    )

    with pytest.raises(ServiceError, match="already active"):
        coordinator.request_quiesce(
            expected_pid=123,
            expected_generation="generation-1",
            expected_process_started_at=now - 1,
            now=now + 1,
        )


@pytest.mark.asyncio
async def test_durable_loss_marker_blocks_acknowledged_fence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "loss-marker.sqlite3"
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        now = time.time()
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation="generation-1",
            expected_process_started_at=now - 1,
            now=now,
        )
        coordinator.acknowledge_quiesce(fence, now=now)
        worker = ServiceControlWorker(
            database,
            FakeSessions(),
            process_generation="generation-1",
            process_started_at=now - 1,
        )
        worker._active_fence_id = fence.fence_id
        worker._record_loss_sync("inbox_overflow")

        with pytest.raises(
            RestartBlocked,
            match="loss_or_accounting_failure",
        ):
            coordinator.assert_quiesced(fence)

        coordinator.release_quiesce(
            fence,
            now=now + 1,
            reason="test_complete",
        )
        assert not any(path.exists() for path in fence_marker_paths(database_path, fence.fence_id))


@pytest.mark.asyncio
async def test_sqlite_accounting_lock_fails_fast_and_persists_marker(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "accounting-lock.sqlite3"
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        now = time.time()
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation="generation-1",
            expected_process_started_at=now - 1,
            now=now,
        )
        worker = ServiceControlWorker(
            database,
            FakeSessions(),
            process_generation="generation-1",
            process_started_at=now - 1,
        )
        worker._active_fence_id = fence.fence_id
        lock = sqlite3.connect(database_path, timeout=0)
        lock.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        try:
            with pytest.raises(RuntimeError, match="durably record"):
                worker._record_producer_sync("sdk")
        finally:
            lock.rollback()
            lock.close()
        assert time.monotonic() - started < 0.2
        _, accounting_marker = fence_marker_paths(
            database_path,
            fence.fence_id,
        )
        assert accounting_marker.is_file()
        pending = fence_pending_marker_directory(
            database_path,
            fence.fence_id,
        )
        assert any(pending.iterdir())
        coordinator.release_quiesce(
            fence,
            now=time.time(),
            reason="test_complete",
        )
        assert not pending.exists()


@pytest.mark.asyncio
async def test_pending_producer_marker_blocks_commit_after_ack_race(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pending-producer.sqlite3"
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        now = time.time()
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation="generation-1",
            expected_process_started_at=now - 1,
            now=now,
        )
        coordinator.acknowledge_quiesce(fence, now=now)
        worker = ServiceControlWorker(
            database,
            FakeSessions(),
            process_generation="generation-1",
            process_started_at=now - 1,
        )
        worker._active_fence_id = fence.fence_id
        worker._create_pending_marker(
            fence_id=fence.fence_id,
            source="producer_pre_accounting",
        )

        with pytest.raises(
            RestartBlocked,
            match="loss_or_accounting_failure",
        ):
            coordinator.commit_quiesce(fence, now=now + 1)

        coordinator.release_quiesce(
            fence,
            now=now + 1,
            reason="test_complete",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["prepared", "committed"])
async def test_irreversible_fence_self_terminates_live_worker(
    tmp_path: Path,
    state: str,
) -> None:
    database_path = tmp_path / f"self-terminate-{state}.sqlite3"
    terminated: list[int] = []
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        now = time.time()
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation="generation-1",
            expected_process_started_at=now - 1,
            now=now,
        )
        coordinator.acknowledge_quiesce(fence, now=now)
        await database.execute(
            """
            UPDATE service_admission_fences SET state = ?
            WHERE fence_id = ?
            """,
            (state, fence.fence_id),
        )
        worker = ServiceControlWorker(
            database,
            FakeSessions(),
            process_generation="generation-1",
            process_started_at=now - 1,
            terminate_process=terminated.append,
        )
        worker._active_fence_id = fence.fence_id

        await worker._poll_once()

    assert terminated == [75]


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
            expected_process_started_at=time.time() - 1,
            now=time.time(),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=fence.expected_process_started_at,
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
            row = await database.fetchone(
                """
                SELECT producer_count, acknowledged_producer_count
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert tuple(row) == (0, 0)

            sessions.violations = 1
            assert sessions.on_violation is not None
            await asyncio.to_thread(sessions.on_violation, "sdk")
            for _ in range(100):
                row = await database.fetchone(
                    """
                    SELECT state, violation_count
                    FROM service_admission_fences
                    WHERE fence_id = ?
                    """,
                    (fence.fence_id,),
                )
                if row is not None and row["state"] in {
                    "violated",
                    "released",
                }:
                    break
                await asyncio.sleep(0.005)
            assert row is not None
            assert row["state"] in {"violated", "released"}
            assert row["violation_count"] == 1

            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            await asyncio.to_thread(
                sessions.on_violation,
                "late_after_release",
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
async def test_requested_producers_are_counted_before_atomic_ack(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "producer-count.sqlite3"
    sessions = FakeSessions()
    sessions.produce_during_begin = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=started_at,
                poll_seconds=0.005,
            ).run()
        )
        try:
            await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            row = await database.fetchone(
                """
                SELECT state, producer_count, acknowledged_producer_count,
                       violation_count
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert tuple(row) == ("acknowledged", 1, 1, 0)
        finally:
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_requested_pre_observer_journal_event_is_in_ack_epoch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal-epoch.sqlite3"
    sessions = FakeSessions()
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        await database.execute(
            """
            INSERT INTO event_journal(
                sdk_session_id, generation, inbox_seq, source,
                persistence_class, raw_type, reducer_hash,
                raw_payload, received_at
            ) VALUES ('session-1', 1, 1, 'sdk',
                      'durable', 'assistant.message', 'hash', '{}', ?)
            """,
            (time.time(),),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=started_at,
                poll_seconds=0.005,
            ).run()
        )
        try:
            await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            row = await database.fetchone(
                """
                SELECT baseline_journal_id, acknowledged_journal_id
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert tuple(row) == (0, 1)
        finally:
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_atomic_ack_retries_callback_in_final_ack_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "ack-race.sqlite3"
    sessions = FakeSessions()
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        original_fetchone = database.fetchone
        injected = False

        async def racing_fetchone(
            sql: str,
            parameters: tuple[object, ...] = (),
        ):
            nonlocal injected
            row = await original_fetchone(sql, parameters)
            if (
                "SELECT producer_count" in sql
                and not injected
                and sessions.on_violation is not None
            ):
                injected = True
                await asyncio.to_thread(
                    sessions.on_violation,
                    "sdk_final_ack_window",
                )
            return row

        monkeypatch.setattr(database, "fetchone", racing_fetchone)
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=started_at,
                poll_seconds=0.005,
            ).run()
        )
        try:
            await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            row = await original_fetchone(
                """
                SELECT state, producer_count, acknowledged_producer_count
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert injected is True
            assert tuple(row) == ("acknowledged", 1, 1)
        finally:
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_ack_retries_producer_accounted_before_reservation_finishes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "ack-reservation-race.sqlite3"
    sessions = FakeSessions()
    sessions.produce_during_first_drain = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=started_at,
                poll_seconds=0.005,
            ).run()
        )
        try:
            await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            row = await database.fetchone(
                """
                SELECT state, producer_count, acknowledged_producer_count
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert sessions.drain_calls >= 2
            assert tuple(row) == ("acknowledged", 1, 1)
        finally:
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_ack_snapshots_producer_count_before_depth_and_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "ack-count-before-depth.sqlite3"
    sessions = FakeSessions()
    sessions.produce_during_first_metrics = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        control = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
            poll_seconds=0.005,
        )
        producer_snapshots = 0
        original_producer_count = control._producer_count

        async def counted_producer_snapshot(fence_id: str) -> int:
            nonlocal producer_snapshots
            producer_snapshots += 1
            return await original_producer_count(fence_id)

        monkeypatch.setattr(
            control,
            "_producer_count",
            counted_producer_snapshot,
        )
        worker = asyncio.create_task(control.run())
        try:
            await asyncio.to_thread(
                coordinator.wait_for_quiesce,
                fence,
                timeout_seconds=1,
            )
            row = await database.fetchone(
                """
                SELECT state, producer_count, acknowledged_producer_count
                FROM service_admission_fences WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            assert producer_snapshots >= 4
            assert sessions.drain_calls >= 2
            assert tuple(row) == ("acknowledged", 1, 1)
        finally:
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="test_complete",
            )
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_pre_ack_violation_rolls_back_instead_of_retrying_forever(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "permanent-pre-ack-violation.sqlite3"
    sessions = FakeSessions()
    sessions.violations = 1
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        control = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
            poll_seconds=0.001,
        )

        await asyncio.wait_for(control._poll_once(), timeout=1)

        row = await database.fetchone(
            """
            SELECT state, rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("released", "complete", 1)
        assert sessions.ended == 1
        assert control._active_fence_id is None


@pytest.mark.asyncio
async def test_unstable_pre_ack_epoch_honors_quiesce_deadline(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "unstable-pre-ack-epoch.sqlite3"
    sessions = FakeSessions()
    sessions.produce_during_every_metrics = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        control = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
            poll_seconds=0.001,
            quiesce_timeout_seconds=0.1,
        )

        await asyncio.wait_for(control._poll_once(), timeout=1)

        row = await database.fetchone(
            """
            SELECT state, rollback_state, rollback_attempts, detail
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row)[:3] == ("released", "complete", 1)
        assert "rollback_complete" in str(row["detail"])
        assert sessions.metrics_calls > 1


@pytest.mark.asyncio
async def test_disappearing_pending_marker_directory_does_not_kill_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "pending-marker-directory-race.sqlite3"
    sessions = FakeSessions()
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        pending = fence_pending_marker_directory(
            database_path,
            fence.fence_id,
        )
        pending.mkdir()
        original_iterdir = Path.iterdir

        def disappearing_iterdir(path: Path):
            if path == pending:
                pending.rmdir()
                raise FileNotFoundError(path)
            return original_iterdir(path)

        monkeypatch.setattr(Path, "iterdir", disappearing_iterdir)
        control = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
        )

        await control._poll_once()

        row = await database.fetchone(
            "SELECT state FROM service_admission_fences WHERE fence_id = ?",
            (fence.fence_id,),
        )
        assert row["state"] == "acknowledged"
        coordinator.release_quiesce(
            fence,
            now=time.time(),
            reason="test_complete",
        )
        await control._poll_once()
        assert sessions.ended == 1


@pytest.mark.asyncio
async def test_released_fence_cancels_hung_quiesce_and_rolls_back(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "hung-quiesce.sqlite3"
    sessions = FakeSessions()
    sessions.begin_gate = asyncio.Event()
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        worker = asyncio.create_task(
            ServiceControlWorker(
                database,
                sessions,
                process_generation=generation,
                process_started_at=started_at,
                poll_seconds=0.005,
                quiesce_timeout_seconds=5,
            ).run()
        )
        try:
            for _ in range(100):
                if sessions.begun:
                    break
                await asyncio.sleep(0.005)
            assert sessions.begun == 1
            coordinator.release_quiesce(
                fence,
                now=time.time(),
                reason="manager_cancelled_restart",
            )
            for _ in range(100):
                if sessions.ended:
                    break
                await asyncio.sleep(0.005)
            assert sessions.ended == 1
            assert worker.done() is False
        finally:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.asyncio
async def test_rollback_timeout_is_persisted_and_retried(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rollback-retry.sqlite3"
    sessions = FakeSessions()
    sessions.end_gate = asyncio.Event()
    sessions.ignore_end_cancellation = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        coordinator.acknowledge_quiesce(fence, now=time.time())
        coordinator.release_quiesce(
            fence,
            now=time.time(),
            reason="test_rollback",
        )
        worker = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
            rollback_timeout_seconds=0.01,
            terminate_process=lambda _code: None,
        )
        worker._active_fence_id = fence.fence_id

        await worker._poll_once()
        row = await database.fetchone(
            """
            SELECT rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("pending", 1)
        assert worker._active_fence_id == fence.fence_id

        sessions.end_gate.set()
        await worker._poll_once()
        row = await database.fetchone(
            """
            SELECT rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("complete", 2)
        assert worker._active_fence_id is None


@pytest.mark.asyncio
async def test_rollback_exception_remains_pending_for_retry(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rollback-error.sqlite3"
    sessions = FakeSessions()
    sessions.end_error = RuntimeError("mailbox resume failed")
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        coordinator.acknowledge_quiesce(fence, now=time.time())
        coordinator.release_quiesce(
            fence,
            now=time.time(),
            reason="test_rollback_error",
        )
        worker = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
        )
        worker._active_fence_id = fence.fence_id

        await worker._poll_once()

        row = await database.fetchone(
            """
            SELECT rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("pending", 1)
        assert worker._active_fence_id == fence.fence_id


@pytest.mark.asyncio
async def test_violated_fence_rollback_timeout_is_released_and_retried(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "violated-rollback-retry.sqlite3"
    sessions = FakeSessions()
    sessions.end_gate = asyncio.Event()
    sessions.ignore_end_cancellation = True
    generation = "generation-1"
    started_at = time.time() - 1
    async with Database(database_path) as database:
        coordinator = SqliteRestartCoordinator(database_path)
        fence = coordinator.request_quiesce(
            expected_pid=os.getpid(),
            expected_generation=generation,
            expected_process_started_at=started_at,
            now=time.time(),
        )
        await database.execute(
            """
            UPDATE service_admission_fences
            SET state = 'violated', rollback_state = 'pending',
                violation_count = 1
            WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        worker = ServiceControlWorker(
            database,
            sessions,
            process_generation=generation,
            process_started_at=started_at,
            rollback_timeout_seconds=0.01,
        )
        worker._active_fence_id = fence.fence_id

        await worker._poll_once()
        row = await database.fetchone(
            """
            SELECT state, rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("released", "pending", 1)
        assert worker._active_fence_id == fence.fence_id

        sessions.end_gate.set()
        await worker._poll_once()
        row = await database.fetchone(
            """
            SELECT state, rollback_state, rollback_attempts
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        )
        assert tuple(row) == ("released", "complete", 2)
        assert worker._active_fence_id is None


@pytest.mark.asyncio
async def test_registry_quiesce_waits_for_active_create_and_blocks_new_runtime() -> None:
    registry = SessionRegistry(
        SimpleNamespace(),
        lambda binding: FakeRuntime(binding.thread_id),
    )
    runtime = FakeRuntime("thread-1")
    violations = 0

    def violated(source: str) -> None:
        nonlocal violations
        assert source == "runtime_registration"
        violations += 1

    async with registry.creation_admission():
        quiesce = asyncio.create_task(
            registry.begin_service_quiesce(
                violated,
                lambda _source: None,
            )
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
