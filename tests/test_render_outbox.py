import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from copilotd.render.outbox import (
    RenderOutboxDispatcher,
    RenderPermanentError,
    RenderRateLimited,
    RenderTransientError,
)
from copilotd.storage.database import Database


class FakeTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any], str]] = []
        self.edited: list[tuple[str, str, dict[str, Any]]] = []
        self.edit_idempotency_keys: list[str | None] = []
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
        session_id: str | None = None,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> None:
        del session_id
        if self.failures:
            raise self.failures.pop(0)
        self.edited.append((message_id, lane, payload))
        self.edit_idempotency_keys.append(idempotency_key)


class HungTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        del session_id, lane, payload, idempotency_key
        self.started.set()
        await self.release.wait()
        return "unexpected"


class BlockingSuccessTransport(FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        self.sent.append((session_id, lane, payload, idempotency_key))
        self.started.set()
        await self.release.wait()
        return "discord-stable"


class BlockingFailureTransport(FakeTransport):
    def __init__(self, failure: Exception) -> None:
        super().__init__()
        self.failure = failure
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.failed = False

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        if not self.failed:
            self.failed = True
            self.started.set()
            await self.release.wait()
            raise self.failure
        return await super().send(
            session_id=session_id,
            lane=lane,
            payload=payload,
            idempotency_key=idempotency_key,
        )


class PausingClaimDispatcher(RenderOutboxDispatcher):
    def __init__(self, database: Database, transport: FakeTransport) -> None:
        super().__init__(database, transport)
        self.claim_committed = asyncio.Event()
        self.release_claim = asyncio.Event()

    async def _claim(
        self,
        *,
        limit: int,
        now: float,
        claimed_ids: list[str],
    ) -> list[Any]:
        items = await super()._claim(
            limit=limit,
            now=now,
            claimed_ids=claimed_ids,
        )
        self.claim_committed.set()
        await self.release_claim.wait()
        return items


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
async def test_retryable_head_prevents_later_claimed_message_overtake(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "ordered-retry.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="first",
            sequence=1,
            payload={"content": "first", "finalized": True},
            coalesce_key="first",
            lane="status",
        )
        await _insert_outbox(
            database,
            item_id="second",
            sequence=2,
            payload={"content": "second", "finalized": True},
            coalesce_key="second",
            lane="status",
        )
        transport = FakeTransport()
        transport.failures.append(RenderRateLimited(1))
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(limit=2, now=100) == 0
        states = await database.fetchall("SELECT id, state FROM render_outbox ORDER BY logical_seq")
        assert [dict(row) for row in states] == [
            {"id": "first", "state": "pending"},
            {"id": "second", "state": "pending"},
        ]
        assert transport.sent == []

        assert await dispatcher.dispatch_once(limit=2, now=100.5) == 0
        assert await dispatcher.dispatch_once(limit=2, now=101) == 1
        assert await dispatcher.dispatch_once(limit=2, now=101) == 1

    assert [entry[2]["content"] for entry in transport.sent] == [
        "first",
        "second",
    ]


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
async def test_cancelled_dispatch_restores_all_claims_to_pending(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "cancelled-claim.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="hung",
            sequence=1,
            payload={"content": "hung", "finalized": True},
        )
        await _insert_outbox(
            database,
            item_id="also-claimed",
            sequence=2,
            payload={"content": "second", "finalized": True},
            coalesce_key="assistant:message-2",
        )
        transport = HungTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        task = asyncio.create_task(dispatcher.dispatch_once(limit=2))
        await transport.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        states = await database.fetchall("SELECT id, state FROM render_outbox ORDER BY logical_seq")

    assert [dict(row) for row in states] == [
        {"id": "hung", "state": "pending"},
        {"id": "also-claimed", "state": "pending"},
    ]


@pytest.mark.asyncio
async def test_cancellation_after_claim_commit_restores_known_ids(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "claim-gap.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="claim-gap",
            sequence=1,
            payload={"content": "claim gap", "finalized": True},
        )
        dispatcher = PausingClaimDispatcher(database, FakeTransport())
        task = asyncio.create_task(dispatcher.dispatch_once())
        await dispatcher.claim_committed.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = await database.fetchone("SELECT state FROM render_outbox WHERE id = 'claim-gap'")

    assert row["state"] == "pending"


@pytest.mark.asyncio
async def test_stable_outbox_update_during_send_requeues_same_row(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "stable-update.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="stable",
            sequence=1,
            payload={"content": "old", "finalized": False},
            coalesce_key="tool-spill:one",
            lane="artifact",
        )
        transport = BlockingSuccessTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        first = asyncio.create_task(dispatcher.dispatch_once())
        await transport.started.wait()
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2,
                payload = ?,
                payload_revision = payload_revision + 1,
                updated_at = 2
            WHERE id = 'stable' AND state = 'sending'
            """,
            (json.dumps({"content": "new", "finalized": True}),),
        )
        transport.release.set()
        assert await first == 1
        pending = await database.fetchone("SELECT state FROM render_outbox WHERE id = 'stable'")
        assert pending["state"] == "pending"

        assert await dispatcher.dispatch_once() == 1
        final = await database.fetchone(
            """
            SELECT state, payload_revision, COUNT(*) OVER () AS row_count
            FROM render_outbox
            """
        )

    assert transport.edited[-1][2] == {"content": "new", "finalized": True}
    assert transport.sent[0][3] != transport.edit_idempotency_keys[-1]
    assert dict(final) == {"state": "sent", "payload_revision": 2, "row_count": 1}


@pytest.mark.parametrize(
    "failure",
    [
        RenderPermanentError("stale permanent failure"),
        RenderRateLimited(30),
    ],
)
@pytest.mark.asyncio
async def test_stale_failure_requeues_newer_payload_revision(
    tmp_path: Path,
    failure: Exception,
) -> None:
    async with Database(tmp_path / "stale-failure.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="stable",
            sequence=1,
            payload={"content": "progress", "finalized": False},
            coalesce_key="tool-spill:one",
            lane="artifact",
        )
        transport = BlockingFailureTransport(failure)
        dispatcher = RenderOutboxDispatcher(database, transport)
        first = asyncio.create_task(dispatcher.dispatch_once(now=100))
        await transport.started.wait()
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2,
                payload = ?,
                payload_revision = payload_revision + 1,
                updated_at = 101
            WHERE id = 'stable' AND state = 'sending'
            """,
            (json.dumps({"content": "final", "finalized": True}),),
        )
        transport.release.set()

        assert await first == 0
        pending = await database.fetchone(
            """
            SELECT state, payload_revision, next_attempt_at
            FROM render_outbox WHERE id = 'stable'
            """
        )
        assert dict(pending) == {
            "state": "pending",
            "payload_revision": 2,
            "next_attempt_at": 0,
        }
        assert await dispatcher.dispatch_once(now=101) == 1
        final = await database.fetchone(
            "SELECT state, payload_revision FROM render_outbox WHERE id = 'stable'"
        )

    assert dict(final) == {"state": "sent", "payload_revision": 2}
    assert transport.sent[-1][2] == {"content": "final", "finalized": True}


@pytest.mark.asyncio
async def test_final_spill_survives_retry_then_is_collected_after_delivery(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "tool-spill.txt"
    content = b"durable retry payload"
    artifact.write_bytes(content)
    async with Database(tmp_path / "spill-delivery.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO tool_spill_artifacts(
                session_id, tool_call_id, local_path, byte_size, sha256,
                finalized, retention_until, updated_at
            ) VALUES ('session-1', 'tool-1', ?, ?, ?, 1, 999, 0)
            """,
            (
                str(artifact),
                len(content),
                hashlib.sha256(content).hexdigest(),
            ),
        )
        await _insert_outbox(
            database,
            item_id="spill-final",
            sequence=1,
            lane="artifact",
            coalesce_key="tool-spill:tool-1",
            payload={
                "content": "tool spill",
                "finalized": True,
                "attachments": [{"path": str(artifact)}],
            },
        )
        transport = FakeTransport()
        transport.failures.append(RenderTransientError("retry"))
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(now=100) == 0
        assert artifact.is_file()
        assert await database.fetchone(
            "SELECT 1 FROM tool_spill_artifacts WHERE tool_call_id = 'tool-1'"
        )

        assert await dispatcher.dispatch_once(now=101) == 1
        row = await database.fetchone(
            "SELECT 1 FROM tool_spill_artifacts WHERE tool_call_id = 'tool-1'"
        )

    assert not artifact.exists()
    assert row is None


@pytest.mark.asyncio
async def test_dispatch_periodically_collects_abandoned_nonfinal_spill(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "abandoned.txt"
    content = b"abandoned"
    artifact.write_bytes(content)
    async with Database(tmp_path / "periodic-spill-gc.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO tool_spill_artifacts(
                session_id, tool_call_id, local_path, byte_size,
                sha256, finalized, retention_until, updated_at
            ) VALUES ('abandoned-session', 'abandoned-tool', ?, ?, NULL, 0, 50, 0)
            """,
            (str(artifact), len(content)),
        )
        dispatcher = RenderOutboxDispatcher(
            database,
            FakeTransport(),
            spill_gc_interval_seconds=60,
        )

        assert await dispatcher.dispatch_once(now=100) == 0
        row = await database.fetchone(
            "SELECT 1 FROM tool_spill_artifacts WHERE tool_call_id = 'abandoned-tool'"
        )

    assert not artifact.exists()
    assert row is None


@pytest.mark.asyncio
async def test_drain_deadline_bounds_hung_delivery_and_restores_claim(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "hung-drain.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="hung",
            sequence=1,
            payload={"content": "hung", "finalized": True},
        )
        transport = HungTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)

        delivered = await dispatcher.drain(deadline_seconds=0.05)
        row = await database.fetchone("SELECT state FROM render_outbox WHERE id = 'hung'")

    assert delivered == 0
    assert row["state"] == "pending"


@pytest.mark.asyncio
async def test_drain_deadline_bounds_pending_fetch(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "hung-pending-fetch.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="rate-limited",
            sequence=1,
            payload={"content": "retry", "finalized": True},
        )
        transport = FakeTransport()
        transport.failures.append(RenderRateLimited(10))
        dispatcher = RenderOutboxDispatcher(database, transport)
        original_fetchone = database.fetchone

        async def blocked_fetchone(sql: str, parameters: Any = ()) -> Any:
            if "MIN(next_attempt_at)" in sql:
                await asyncio.Event().wait()
            return await original_fetchone(sql, parameters)

        database.fetchone = blocked_fetchone
        started = asyncio.get_running_loop().time()
        delivered = await dispatcher.drain(deadline_seconds=0.05)
        elapsed = asyncio.get_running_loop().time() - started
        database.fetchone = original_fetchone
        row = await database.fetchone("SELECT state FROM render_outbox WHERE id = 'rate-limited'")

    assert delivered == 0
    assert elapsed < 0.2
    assert row["state"] == "pending"


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
async def test_legacy_taskdeck_rows_are_suppressed_without_transport(
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
        assert await dispatcher.dispatch_once(now=100) == 0

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
        assert await dispatcher.dispatch_once(now=102) == 0
        states = await database.fetchall("SELECT state FROM render_outbox ORDER BY logical_seq")

    assert [row["state"] for row in states] == [
        "superseded",
        "superseded",
        "superseded",
    ]
    assert transport.sent == []
    assert transport.edited == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lane", "payload_type"),
    [
        ("artifact", "tool_output_artifact"),
        ("diff", "diff"),
        ("taskdeck", "taskdeck"),
    ],
)
async def test_legacy_internal_tool_rows_never_reach_transport(
    tmp_path: Path,
    lane: str,
    payload_type: str,
) -> None:
    async with Database(tmp_path / f"internal-{lane}.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id=f"internal-{lane}",
            sequence=1,
            lane=lane,
            coalesce_key=f"internal-{lane}",
            payload={
                "type": payload_type,
                "content": "raw detailedContent and tool logs",
                "attachments": [{"filename": "tool-output.txt", "content": "secret"}],
                "finalized": True,
            },
        )
        transport = FakeTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once() == 0
        row = await database.fetchone(
            "SELECT state FROM render_outbox WHERE id = ?",
            (f"internal-{lane}",),
        )

    assert row["state"] == "superseded"
    assert transport.sent == []
    assert transport.edited == []
