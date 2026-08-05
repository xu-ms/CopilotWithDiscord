import asyncio
from pathlib import Path

import pytest

from copilotd.core.mailbox import (
    CommandMailbox,
    OperationAmbiguous,
    OperationRejected,
    OperationStore,
)
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_mailbox_serializes_and_deduplicates_operations(tmp_path: Path) -> None:
    async with Database(tmp_path / "mailbox.sqlite3") as database:
        active = 0
        peak_active = 0
        calls = 0

        async def validate() -> bool:
            return True

        mailbox = CommandMailbox(
            store=OperationStore(database),
            sdk_session_id="session-1",
            runtime_generation=1,
            owner_fence_token=5,
            fence_validator=validate,
        )
        mailbox.start()

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

        mailbox = CommandMailbox(
            store=OperationStore(database),
            sdk_session_id="session-1",
            runtime_generation=1,
            owner_fence_token=5,
            fence_validator=validate,
        )
        mailbox.start()

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

        mailbox = CommandMailbox(
            store=OperationStore(database),
            sdk_session_id="session-1",
            runtime_generation=1,
            owner_fence_token=5,
            fence_validator=validate,
        )
        mailbox.start()

        with pytest.raises(OperationAmbiguous, match="owner fence lost"):
            await mailbox.submit(
                kind="abort",
                idempotency_key="abort-1",
                input_payload={},
                operation=operation,
            )
        await mailbox.stop()
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

        mailbox = CommandMailbox(
            store=OperationStore(database),
            sdk_session_id="session-1",
            runtime_generation=1,
            owner_fence_token=5,
            fence_validator=validate,
        )
        mailbox.start()
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
        row = await database.fetchone(
            "SELECT state, error_code FROM session_operations WHERE idempotency_key = 'hung'"
        )

    assert row is not None and row["state"] == "unknown"
    assert row["error_code"] in {"mailbox_cancelled", "mailbox_stopped"}
