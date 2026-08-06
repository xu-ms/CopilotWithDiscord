import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from copilotd.core.inbox import ReducerInbox
from copilotd.core.mailbox import (
    CommandMailbox,
    OperationAmbiguous,
    OperationDeferred,
    OperationRejected,
    OperationStore,
)
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.storage.database import Database


def _start_mailbox(
    database: Database,
    validate: Callable[[], Awaitable[bool]],
) -> tuple[CommandMailbox, EventReducerWorker]:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=5,
        capacity=128,
    )
    reducer = EventReducerWorker(
        inbox=inbox,
        reducer=JournalReducer(database),
        batch_size=16,
    )
    reducer.start()
    mailbox = CommandMailbox(
        store=OperationStore(database, inbox),
        sdk_session_id="session-1",
        runtime_generation=1,
        owner_fence_token=5,
        fence_validator=validate,
    )
    mailbox.start()
    return mailbox, reducer


@pytest.mark.asyncio
async def test_mailbox_serializes_and_deduplicates_operations(tmp_path: Path) -> None:
    async with Database(tmp_path / "mailbox.sqlite3") as database:
        active = 0
        peak_active = 0
        calls = 0

        async def validate() -> bool:
            return True

        mailbox, reducer = _start_mailbox(database, validate)

        async def operation(value: str) -> str:
            nonlocal active, peak_active, calls
            active += 1
            calls += 1
            peak_active = max(peak_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return value

        first, duplicate, second = await asyncio.gather(
            mailbox.submit(
                kind="send",
                idempotency_key="message-1",
                input_payload={"prompt": "one"},
                operation=lambda: operation("accepted-1"),
            ),
            mailbox.submit(
                kind="send",
                idempotency_key="message-1",
                input_payload={"prompt": "one"},
                operation=lambda: operation("must-not-run"),
            ),
            mailbox.submit(
                kind="send",
                idempotency_key="message-2",
                input_payload={"prompt": "two"},
                operation=lambda: operation("accepted-2"),
            ),
        )
        replay = await mailbox.submit(
            kind="send",
            idempotency_key="message-1",
            input_payload={"prompt": "one"},
            operation=lambda: operation("must-not-run"),
        )
        await mailbox.stop()
        await reducer.stop()
        rows = await database.fetchall(
            "SELECT idempotency_key, state, result_ref FROM session_operations "
            "ORDER BY idempotency_key"
        )

    assert (first, duplicate, second, replay) == (
        "accepted-1",
        "accepted-1",
        "accepted-2",
        "accepted-1",
    )
    assert calls == 2
    assert peak_active == 1
    assert [row["state"] for row in rows] == ["confirmed", "confirmed"]


@pytest.mark.asyncio
async def test_mailbox_never_replays_rejected_or_ambiguous_operation(tmp_path: Path) -> None:
    async with Database(tmp_path / "unknown.sqlite3") as database:
        calls = 0

        async def validate() -> bool:
            return True

        mailbox, reducer = _start_mailbox(database, validate)

        async def fail_unknown() -> None:
            nonlocal calls
            calls += 1
            raise ConnectionError("transport lost")

        async def reject() -> None:
            raise OperationRejected("server rejected input")

        with pytest.raises(OperationAmbiguous, match="outcome is unknown"):
            await mailbox.submit(
                kind="send",
                idempotency_key="unknown",
                input_payload={"prompt": "one"},
                operation=fail_unknown,
            )
        with pytest.raises(OperationAmbiguous, match="automatic replay is forbidden"):
            await mailbox.submit(
                kind="send",
                idempotency_key="unknown",
                input_payload={"prompt": "one"},
                operation=fail_unknown,
            )
        with pytest.raises(OperationRejected, match="server rejected"):
            await mailbox.submit(
                kind="mode",
                idempotency_key="rejected",
                input_payload={"mode": "plan"},
                operation=reject,
            )
        await mailbox.stop()
        await reducer.stop()
        states = await database.fetchall(
            "SELECT idempotency_key, state FROM session_operations ORDER BY idempotency_key"
        )

    assert calls == 1
    assert [dict(row) for row in states] == [
        {"idempotency_key": "rejected", "state": "rejected"},
        {"idempotency_key": "unknown", "state": "unknown"},
    ]


@pytest.mark.asyncio
async def test_mailbox_checks_fence_again_immediately_before_dispatch(tmp_path: Path) -> None:
    async with Database(tmp_path / "fence.sqlite3") as database:
        checks = 0
        called = False

        async def validate() -> bool:
            nonlocal checks
            checks += 1
            return checks == 1

        async def operation() -> None:
            nonlocal called
            called = True

        mailbox, reducer = _start_mailbox(database, validate)

        with pytest.raises(OperationAmbiguous, match="owner fence lost"):
            await mailbox.submit(
                kind="abort",
                idempotency_key="abort-1",
                input_payload={},
                operation=operation,
            )
        await mailbox.stop()
        await reducer.stop()
        row = await database.fetchone(
            "SELECT state, error_code FROM session_operations WHERE idempotency_key = 'abort-1'"
        )

    assert not called
    assert dict(row) == {
        "state": "unknown",
        "error_code": "owner_fence_lost_before_dispatch",
    }


@pytest.mark.asyncio
async def test_mailbox_stop_cancels_hung_operation_and_marks_it_unknown(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "mailbox-stop.sqlite3") as database:
        started = asyncio.Event()
        never = asyncio.Event()

        async def validate() -> bool:
            return True

        async def hang() -> None:
            started.set()
            await never.wait()

        mailbox, reducer = _start_mailbox(database, validate)
        submission = asyncio.create_task(
            mailbox.submit(
                kind="send",
                idempotency_key="hung",
                input_payload={"prompt": "hang"},
                operation=hang,
            )
        )
        await started.wait()

        await mailbox.stop(timeout_seconds=0.01)
        with pytest.raises(OperationAmbiguous, match="interrupted"):
            await submission
        await reducer.stop()
        row = await database.fetchone(
            "SELECT state, error_code FROM session_operations WHERE idempotency_key = 'hung'"
        )

    assert row is not None and row["state"] == "unknown"
    assert row["error_code"] in {"mailbox_cancelled", "mailbox_stopped"}


@pytest.mark.asyncio
async def test_inflight_idempotency_rejects_different_kind_or_input(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "inflight-input.sqlite3") as database:
        started = asyncio.Event()
        release = asyncio.Event()

        async def validate() -> bool:
            return True

        async def operation() -> str:
            started.set()
            await release.wait()
            return "first"

        mailbox, reducer = _start_mailbox(database, validate)
        first = asyncio.create_task(
            mailbox.submit(
                kind="agent",
                idempotency_key="same-key",
                input_payload={"target": "reviewer"},
                operation=operation,
            )
        )
        await started.wait()

        with pytest.raises(ValueError, match="different input"):
            await mailbox.submit(
                kind="remote",
                idempotency_key="same-key",
                input_payload={"target": "off"},
                operation=operation,
            )

        release.set()
        assert await first == "first"
        await mailbox.stop()
        await reducer.stop()


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cache_raw_ephemeral_result(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "ephemeral-cache.sqlite3") as database:
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def validate() -> bool:
            return True

        async def operation() -> str:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return "private answer"

        mailbox, reducer = _start_mailbox(database, validate)
        waiter = asyncio.create_task(
            mailbox.submit(
                kind="ephemeral-query",
                idempotency_key="ask",
                input_payload={"question_hash": "hash"},
                operation=operation,
                result_persistence=lambda value: {"answer_hash": f"hash:{len(value)}"},
            )
        )
        await started.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        await mailbox.drain()

        replay = await mailbox.submit(
            kind="ephemeral-query",
            idempotency_key="ask",
            input_payload={"question_hash": "hash"},
            operation=operation,
        )

        assert replay == {"answer_hash": "hash:14"}
        assert calls == 1
        assert "ask" not in mailbox._futures
        await mailbox.stop()
        await reducer.stop()


@pytest.mark.asyncio
async def test_frozen_mailbox_drains_native_work_before_close_operation(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "freeze-drain.sqlite3") as database:
        started = asyncio.Event()
        release = asyncio.Event()

        async def validate() -> bool:
            return True

        async def native_operation() -> str:
            started.set()
            await release.wait()
            return "native-complete"

        mailbox, reducer = _start_mailbox(database, validate)
        native = asyncio.create_task(
            mailbox.submit(
                kind="native-command",
                idempotency_key="native",
                input_payload={},
                operation=native_operation,
            )
        )
        await started.wait()
        draining = asyncio.create_task(mailbox.freeze_and_drain())
        await asyncio.sleep(0)
        assert not draining.done()
        with pytest.raises(RuntimeError, match="not accepting"):
            await mailbox.submit(
                kind="remote",
                idempotency_key="late",
                input_payload={},
                operation=native_operation,
            )
        release.set()
        assert await native == "native-complete"
        await draining

        closed = await mailbox.submit(
            kind="close",
            idempotency_key="close",
            input_payload={},
            operation=lambda: asyncio.sleep(0, result="closed"),
            allow_when_frozen=True,
        )

        assert closed == "closed"
        await mailbox.stop()
        await reducer.stop()


@pytest.mark.asyncio
async def test_mailbox_rechecks_fence_after_rpc_before_confirming(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "post-rpc-fence.sqlite3") as database:
        checks = 0

        async def validate() -> bool:
            nonlocal checks
            checks += 1
            return checks < 3

        async def operation() -> str:
            return "must-not-confirm"

        mailbox, reducer = _start_mailbox(database, validate)

        with pytest.raises(OperationAmbiguous, match="lost after"):
            await mailbox.submit(
                kind="fleet",
                idempotency_key="long-rpc",
                input_payload={"prompt_hash": "hash"},
                operation=operation,
            )

        row = await database.fetchone(
            """
            SELECT state, result_ref, error_code
            FROM session_operations WHERE idempotency_key = 'long-rpc'
            """
        )
        assert dict(row) == {
            "state": "unknown",
            "result_ref": None,
            "error_code": "owner_fence_lost_after_dispatch",
        }
        await mailbox.stop()
        await reducer.stop()


@pytest.mark.asyncio
async def test_emergency_stop_defers_unstarted_operation_and_runs_durable_requeue(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "mailbox-emergency.sqlite3") as database:
        started = asyncio.Event()
        never = asyncio.Event()
        requeued = asyncio.Event()
        pending = asyncio.Event()
        second_called = False

        async def validate() -> bool:
            return True

        async def hang() -> None:
            started.set()
            await never.wait()

        async def must_not_start() -> None:
            nonlocal second_called
            second_called = True

        async def durable_requeue() -> None:
            requeued.set()

        mailbox, reducer = _start_mailbox(database, validate)
        original_begin = mailbox._store.begin

        async def begin_and_signal(**kwargs: Any) -> Any:
            result = await original_begin(**kwargs)
            if kwargs["idempotency_key"] == "unstarted":
                pending.set()
            return result

        mailbox._store.begin = begin_and_signal  # type: ignore[method-assign]
        first = asyncio.create_task(
            mailbox.submit(
                kind="send",
                idempotency_key="started",
                input_payload={"prompt": "started"},
                operation=hang,
            )
        )
        await started.wait()
        second = asyncio.create_task(
            mailbox.submit(
                kind="send",
                idempotency_key="unstarted",
                input_payload={"prompt": "queued"},
                operation=must_not_start,
                defer_on_fence_loss=True,
                on_fence_deferred=durable_requeue,
            )
        )
        await pending.wait()

        await mailbox.emergency_stop()
        with pytest.raises(OperationAmbiguous):
            await first
        with pytest.raises(OperationDeferred, match="deferred"):
            await second
        rows = await database.fetchall(
            """
            SELECT idempotency_key, state, error_code
            FROM session_operations
            WHERE idempotency_key IN ('started', 'unstarted')
            ORDER BY idempotency_key
            """
        )
        await reducer.stop()

    assert requeued.is_set()
    assert not second_called
    assert [dict(row) for row in rows] == [
        {
            "idempotency_key": "started",
            "state": "unknown",
            "error_code": "mailbox_cancelled",
        },
        {
            "idempotency_key": "unstarted",
            "state": "rejected",
            "error_code": "mailbox_emergency_deferred",
        },
    ]
