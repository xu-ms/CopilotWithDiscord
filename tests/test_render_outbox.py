import json
from pathlib import Path
from typing import Any

import pytest

from copilotd.render.outbox import (
    RenderOutboxDispatcher,
    RenderRateLimited,
    RenderTransientError,
)
from copilotd.storage.database import Database


class FakeTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any], str]] = []
        self.edited: list[tuple[str, str, dict[str, Any]]] = []
        self.failures: list[Exception] = []

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        if self.failures:
            raise self.failures.pop(0)
        self.sent.append((session_id, lane, payload, idempotency_key))
        return f"discord-{len(self.sent)}"

    async def edit(
        self,
        *,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
    ) -> None:
        if self.failures:
            raise self.failures.pop(0)
        self.edited.append((message_id, lane, payload))


async def _insert_outbox(
    database: Database,
    *,
    item_id: str,
    sequence: int,
    payload: dict[str, Any],
    coalesce_key: str | None = "assistant:message-1",
    lane: str = "assistant_stream",
) -> None:
    await database.execute(
        """
        INSERT INTO render_outbox(
            id, session_id, logical_seq, lane, coalesce_key,
            idempotency_key, payload, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, 'session-1', ?, ?, ?, ?, ?,
                  'pending', 0, 0, 0, 0)
        """,
        (
            item_id,
            sequence,
            lane,
            coalesce_key,
            f"idempotency:{item_id}",
            json.dumps(payload),
        ),
    )


@pytest.mark.asyncio
async def test_outbox_sends_then_edits_one_coalesced_message(tmp_path: Path) -> None:
    async with Database(tmp_path / "outbox.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="one",
            sequence=1,
            payload={"text": "first", "finalized": False},
        )
        transport = FakeTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once(now=99) == 1
        await _insert_outbox(
            database,
            item_id="two",
            sequence=2,
            payload={"text": "final", "finalized": True},
        )

        delivered = 1 + await dispatcher.dispatch_once(now=100)
        messages = await database.fetchall("SELECT * FROM render_messages")
        states = await database.fetchall(
            "SELECT state, attempts FROM render_outbox ORDER BY logical_seq"
        )

    assert delivered == 2
    assert len(transport.sent) == 1
    assert transport.edited == [
        (
            "discord-1",
            "assistant_stream",
            {"text": "final", "finalized": True},
        )
    ]
    assert len(messages) == 1
    assert messages[0]["discord_message_id"] == "discord-1"
    assert messages[0]["finalized"] == 1
    assert [dict(row) for row in states] == [
        {"state": "sent", "attempts": 1},
        {"state": "sent", "attempts": 1},
    ]


@pytest.mark.asyncio
async def test_rate_limit_uses_exact_retry_after_and_does_not_lose_item(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "rate.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="one",
            sequence=1,
            payload={"text": "hello"},
        )
        transport = FakeTransport()
        transport.failures.append(RenderRateLimited(7.5))
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(now=100) == 0
        pending = await database.fetchone(
            "SELECT state, attempts, next_attempt_at FROM render_outbox"
        )
        assert dict(pending) == {
            "state": "pending",
            "attempts": 1,
            "next_attempt_at": 107.5,
        }

        assert await dispatcher.dispatch_once(now=107) == 0
        assert await dispatcher.dispatch_once(now=107.5) == 1

    assert len(transport.sent) == 1


@pytest.mark.asyncio
async def test_drain_waits_for_final_rate_limit_retry(tmp_path: Path) -> None:
    async with Database(tmp_path / "drain-rate-limit.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="final",
            sequence=1,
            payload={"content": "final", "finalized": True},
        )
        transport = FakeTransport()
        transport.failures.append(RenderRateLimited(0.02))
        dispatcher = RenderOutboxDispatcher(database, transport)

        delivered = await dispatcher.drain(deadline_seconds=0.5)
        state = await database.fetchone(
            "SELECT state, attempts FROM render_outbox WHERE id = 'final'"
        )

    assert delivered == 1
    assert dict(state) == {"state": "sent", "attempts": 2}
    assert len(transport.sent) == 1


@pytest.mark.asyncio
async def test_transient_failure_blocks_after_three_attempts(tmp_path: Path) -> None:
    async with Database(tmp_path / "transient.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="one",
            sequence=1,
            payload={"text": "hello"},
        )
        transport = FakeTransport()
        transport.failures.extend(
            [
                RenderTransientError("one"),
                RenderTransientError("two"),
                RenderTransientError("three"),
            ]
        )
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(now=100) == 0
        assert await dispatcher.dispatch_once(now=101) == 0
        assert await dispatcher.dispatch_once(now=103) == 0
        state = await database.fetchone("SELECT state, attempts FROM render_outbox")

    assert dict(state) == {"state": "blocked", "attempts": 3}


@pytest.mark.asyncio
async def test_taskdeck_coalesces_pending_updates_and_final_bypasses_cadence(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "taskdeck-outbox.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="one",
            sequence=1,
            lane="taskdeck",
            coalesce_key="taskdeck",
            payload={"content": "first", "finalized": False},
        )
        transport = FakeTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once(now=100) == 1

        await _insert_outbox(
            database,
            item_id="two",
            sequence=2,
            lane="taskdeck",
            coalesce_key="taskdeck",
            payload={"content": "intermediate", "finalized": False},
        )
        assert await dispatcher.dispatch_once(now=101) == 0

        await _insert_outbox(
            database,
            item_id="three",
            sequence=3,
            lane="taskdeck",
            coalesce_key="taskdeck",
            payload={"content": "terminal", "finalized": True},
        )
        assert await dispatcher.dispatch_once(now=102) == 1
        states = await database.fetchall("SELECT state FROM render_outbox ORDER BY logical_seq")

    assert [row["state"] for row in states] == ["sent", "superseded", "sent"]
    assert transport.edited[-1] == (
        "discord-1",
        "taskdeck",
        {"content": "terminal", "finalized": True},
    )
