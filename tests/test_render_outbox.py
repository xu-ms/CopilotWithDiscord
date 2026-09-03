import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.core.volatile_content import VolatileContentStore, opaque_content_key
from copilotd.discord_http_limiter import (
    DiscordHttpRateLimiterClosed,
    DiscordHttpRedirectBlocked,
    DiscordHttpRouteCapacityExceeded,
)
from copilotd.discord_requests import DiscordBackpressure
from copilotd.render.outbox import (
    RenderOutboxDispatcher,
    RenderPermanentError,
    RenderRateLimited,
    RenderTransientError,
)
from copilotd.storage.database import Database
from copilotd.storage.state_only import render_payload_receipt


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


def _stage(
    database: Database,
    payload: dict[str, Any],
) -> tuple[str, str, str, str, int]:
    ref = database.content_store.put(payload)
    return (
        render_payload_receipt(payload, ref),
        ref.key,
        ref.sha256,
        str(payload.get("type", "render")),
        int(bool(payload.get("finalized"))),
    )


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


class PausingRecordFailureDispatcher(RenderOutboxDispatcher):
    def __init__(self, database: Database, transport: FakeTransport) -> None:
        super().__init__(database, transport)
        self.record_started = asyncio.Event()
        self.release_record = asyncio.Event()

    async def _record_render_failure(
        self,
        item: Any,
        error: Exception,
        *,
        now: float,
    ) -> bool:
        self.record_started.set()
        await self.release_record.wait()
        return await super()._record_render_failure(item, error, now=now)


async def _insert_outbox(
    database: Database,
    *,
    item_id: str,
    sequence: int,
    payload: dict[str, Any],
    coalesce_key: str | None = "assistant:message-1",
    lane: str = "assistant_stream",
) -> None:
    receipt, key, digest, kind, finalized = _stage(database, payload)
    await database.execute(
        """
        INSERT INTO render_outbox(
            id, session_id, logical_seq, lane, coalesce_key,
            idempotency_key, payload, state, attempts,
            next_attempt_at, created_at, updated_at,
            content_key, content_hash, render_kind, finalized
        ) VALUES (?, 'session-1', ?, ?, ?, ?, ?,
                 'pending', 0, 0, 0, 0, ?, ?, ?, ?)
        """,
        (
            item_id,
            sequence,
            lane,
            coalesce_key,
            f"idempotency:{item_id}",
            receipt,
            key,
            digest,
            kind if kind != "render" else lane,
            finalized,
        ),
    )


async def _insert_submission_reaction(
    database: Database,
    *,
    submission_id: str = "submission-1",
) -> None:
    await database.execute(
        """
        INSERT INTO submissions(
            submission_id, sdk_session_id, origin, state, created_at
        ) VALUES (?, 'session-1', 'app_message', 'observed_active', 1)
        """,
        (submission_id,),
    )
    await database.execute(
        """
        INSERT INTO submission_reactions(
            submission_id, sdk_session_id, source_channel_id, source_message_id,
            desired_state, revision, delivered_revision, runtime_generation,
            owner_fence_token, terminal, created_at, updated_at
        ) VALUES (?, 'session-1', 'channel-1', 'message-1', 'accepted',
                  1, 0, 1, 1, 0, 1, 1)
        """,
        (submission_id,),
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
        assert database.content_store.item_count == 0

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
async def test_repeated_finalized_renders_reclaim_bounded_volatile_content(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "bounded-outbox.sqlite3") as database:
        database.content_store = VolatileContentStore(max_items=2, max_bytes=1024 * 1024)
        transport = FakeTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)

        for index in range(8):
            await _insert_outbox(
                database,
                item_id=f"final-{index}",
                sequence=index + 1,
                payload={
                    "type": "assistant.message",
                    "content": f"answer {index}",
                    "message_id": f"message-{index}",
                    "finalized": True,
                },
                coalesce_key=f"assistant:message-{index}",
                lane="assistant_final",
            )
            assert await dispatcher.dispatch_once(now=100 + index) == 1
            assert database.content_store.item_count == 0

    assert len(transport.sent) == 8


@pytest.mark.asyncio
async def test_final_delivery_reclaims_assistant_stream_and_tool_display_content(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "render-source-cleanup.sqlite3") as database:
        assistant_key = opaque_content_key(
            "assistant-stream",
            "session-1",
            "assistant-message",
            "",
        )
        database.content_store.put(
            {"content": "answer", "finalized": True},
            key=assistant_key,
        )
        await _insert_outbox(
            database,
            item_id="assistant-final",
            sequence=1,
            payload={
                "type": "assistant.message",
                "content": "answer",
                "message_id": "assistant-message",
                "finalized": True,
            },
            coalesce_key="assistant:assistant-message",
            lane="assistant_final",
        )
        dispatcher = RenderOutboxDispatcher(database, FakeTransport())
        assert await dispatcher.dispatch_once(now=100) == 1
        assert database.content_store.get(assistant_key) is None

        await _insert_submission_reaction(database, submission_id="tool-submission")
        await database.execute(
            """
            INSERT INTO turn_render_state(
                sdk_session_id, turn_key, submission_id, segment_index, state,
                runtime_generation, owner_fence_token, created_at, updated_at
            ) VALUES ('session-1', 'turn-tool', 'tool-submission', 0, 'running',
                      1, 1, 1, 1)
            """
        )
        await database.execute(
            """
            INSERT INTO tool_render_state(
                sdk_session_id, turn_key, submission_id, segment_index,
                tool_call_id, state, started_seq, updated_seq, created_at, updated_at
            ) VALUES ('session-1', 'turn-tool', 'tool-submission', 0,
                      'tool-1', 'succeeded', 1, 1, 1, 1)
            """
        )
        tool_key = opaque_content_key("tool-display", "session-1", "turn-tool", "tool-1")
        database.content_store.put({"tool_name": "tool"}, key=tool_key)
        await _insert_outbox(
            database,
            item_id="tool-final",
            sequence=2,
            payload={
                "type": "tool_card",
                "content": "done",
                "turn_render_key": "turn-tool",
                "finalized": True,
            },
            coalesce_key="tool-card:turn-tool",
            lane="tool",
        )
        assert await dispatcher.dispatch_once(now=101) == 1
        assert database.content_store.get(tool_key) is None


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
            coalesce_key="artifact:one",
            lane="artifact",
        )
        transport = BlockingSuccessTransport()
        dispatcher = RenderOutboxDispatcher(database, transport)
        first = asyncio.create_task(dispatcher.dispatch_once())
        await transport.started.wait()
        staged = _stage(database, {"content": "new", "finalized": True})
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2,
                payload = ?, content_key = ?, content_hash = ?,
                render_kind = ?, finalized = ?,
                payload_revision = payload_revision + 1,
                updated_at = 2
            WHERE id = 'stable' AND state = 'sending'
            """,
            staged,
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
            coalesce_key="artifact:one",
            lane="artifact",
        )
        transport = BlockingFailureTransport(failure)
        dispatcher = RenderOutboxDispatcher(database, transport)
        first = asyncio.create_task(dispatcher.dispatch_once(now=100))
        await transport.started.wait()
        staged = _stage(database, {"content": "final", "finalized": True})
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2,
                payload = ?, content_key = ?, content_hash = ?,
                render_kind = ?, finalized = ?,
                payload_revision = payload_revision + 1,
                updated_at = 101
            WHERE id = 'stable' AND state = 'sending'
            """,
            staged,
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
async def test_new_revision_between_block_and_failure_record_is_not_terminalized(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "failure-record-race.sqlite3") as database:
        await _insert_submission_reaction(database)
        await _insert_outbox(
            database,
            item_id="stable",
            sequence=1,
            payload={
                "content": "old",
                "submission_id": "submission-1",
                "finalized": False,
            },
        )
        transport = BlockingFailureTransport(RenderPermanentError("stale failure"))
        dispatcher = PausingRecordFailureDispatcher(database, transport)
        first = asyncio.create_task(dispatcher.dispatch_once(now=100))
        await transport.started.wait()
        transport.release.set()
        await dispatcher.record_started.wait()
        staged = _stage(
            database,
            {
                "content": "new",
                "submission_id": "submission-1",
                "finalized": True,
            },
        )
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2, payload = ?, content_key = ?, content_hash = ?,
                render_kind = ?, finalized = ?,
                payload_revision = payload_revision + 1,
                state = 'pending', updated_at = 101
            WHERE id = 'stable' AND state = 'blocked'
            """,
            staged,
        )
        dispatcher.release_record.set()
        assert await first == 0
        stale = await database.fetchone(
            """
            SELECT state, payload_revision, last_error
            FROM render_outbox WHERE id = 'stable'
            """
        )
        reaction = await database.fetchone(
            """
            SELECT desired_state, terminal, last_error
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        assert await dispatcher.dispatch_once(now=101) == 1

    assert dict(stale) == {
        "state": "pending",
        "payload_revision": 2,
        "last_error": None,
    }
    assert dict(reaction) == {
        "desired_state": "accepted",
        "terminal": 0,
        "last_error": None,
    }
    assert transport.sent[-1][2]["content"] == "new"


@pytest.mark.asyncio
async def test_render_failure_without_submission_id_does_not_fail_latest_reaction(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "failure-no-submission.sqlite3") as database:
        await _insert_submission_reaction(database, submission_id="unrelated")
        await _insert_outbox(
            database,
            item_id="unscoped-render",
            sequence=1,
            payload={"content": "unscoped", "finalized": True},
        )
        transport = FakeTransport()
        transport.failures.append(RenderPermanentError("invalid payload"))
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once(now=100) == 1
        reaction = await database.fetchone(
            """
            SELECT desired_state, terminal, last_error
            FROM submission_reactions WHERE submission_id = 'unrelated'
            """
        )

    assert dict(reaction) == {
        "desired_state": "accepted",
        "terminal": 0,
        "last_error": None,
    }


@pytest.mark.asyncio
async def test_newer_same_family_revision_recovers_only_its_render_failure(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "newer-render-recovers.sqlite3") as database:
        await _insert_submission_reaction(database)
        await _insert_outbox(
            database,
            item_id="stable",
            sequence=1,
            payload={
                "content": "intermediate",
                "submission_id": "submission-1",
                "finalized": False,
            },
            coalesce_key="answer:submission-1",
        )
        transport = FakeTransport()
        transport.failures.append(RenderPermanentError("intermediate rejected"))
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(now=100) == 1
        failed = await database.fetchone(
            """
            SELECT desired_state, resume_state, terminal
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        failure_state = json.loads(str(failed["resume_state"]))
        assert failure_state["family"] == "idempotency:stable"
        assert failure_state["payload_revision"] == 1
        assert (failed["desired_state"], failed["terminal"]) == ("failed", 1)

        await database.execute("UPDATE render_outbox SET state = 'sent' WHERE lane = 'reaction'")
        await _insert_outbox(
            database,
            item_id="other-family",
            sequence=2,
            payload={
                "content": "other family",
                "submission_id": "submission-1",
                "finalized": True,
            },
            coalesce_key="other:submission-1",
        )
        await database.execute(
            "UPDATE render_outbox SET payload_revision = 2 WHERE id = 'other-family'"
        )
        assert await dispatcher.dispatch_once(now=101) == 1
        still_failed = await database.fetchone(
            """
            SELECT desired_state, resume_state, terminal
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        assert dict(still_failed) == dict(failed)

        staged = _stage(
            database,
            {
                "content": "final answer",
                "submission_id": "submission-1",
                "finalized": True,
            },
        )
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 3, payload = ?, content_key = ?, content_hash = ?,
                render_kind = ?, finalized = ?, payload_revision = 2,
                state = 'pending', last_error = NULL, updated_at = 102
            WHERE id = 'stable'
            """,
            staged,
        )
        assert await dispatcher.dispatch_once(now=102) == 1
        recovered = await database.fetchone(
            """
            SELECT desired_state, resume_state, terminal, last_error
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        recovered_outbox = await database.fetchone(
            "SELECT payload FROM render_outbox WHERE lane = 'reaction'"
        )
        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', terminal_at = 103,
                discord_source_channel_id = 'channel-1',
                discord_source_message_id = 'message-1'
            WHERE submission_id = 'submission-1'
            """
        )
        semantic_complete = AdaptedEvent(
            sdk_session_id="session-1",
            generation=1,
            fence_token=1,
            inbox_seq=10,
            source="internal",
            raw_type="copilotd.test.reconcile",
            raw_payload={"type": "copilotd.test.reconcile", "data": {}},
            reducer_hash="semantic-complete-after-render-recovery",
            persistence_class="internal",
            received_at=103,
            event_id=None,
            internal_event_id="semantic-complete-after-render-recovery",
        )
        assert await JournalReducer(database).persist([semantic_complete]) == 1
        succeeded = await database.fetchone(
            """
            SELECT desired_state, terminal
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )

    assert dict(recovered) == {
        "desired_state": "accepted",
        "resume_state": None,
        "terminal": 0,
        "last_error": None,
    }
    assert json.loads(str(recovered_outbox["payload"]))["state"] == "accepted"
    assert dict(succeeded) == {"desired_state": "succeeded", "terminal": 1}


@pytest.mark.parametrize(
    ("submission_state", "expected_reaction"),
    [
        ("semantic_complete", "succeeded"),
        ("semantic_blocked", "failed"),
    ],
)
@pytest.mark.asyncio
async def test_newer_render_success_restores_authoritative_terminal_reaction(
    tmp_path: Path,
    submission_state: str,
    expected_reaction: str,
) -> None:
    async with Database(tmp_path / f"terminal-restore-{submission_state}.sqlite3") as database:
        await _insert_submission_reaction(database)
        await _insert_outbox(
            database,
            item_id="stable",
            sequence=1,
            payload={
                "content": "intermediate",
                "submission_id": "submission-1",
                "finalized": False,
            },
            coalesce_key="answer:submission-1",
        )
        transport = FakeTransport()
        transport.failures.append(RenderPermanentError("intermediate rejected"))
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once(now=100) == 1
        await database.execute("UPDATE render_outbox SET state = 'sent' WHERE lane = 'reaction'")
        await database.execute(
            """
            UPDATE submissions
            SET state = ?, terminal_at = 101
            WHERE submission_id = 'submission-1'
            """,
            (submission_state,),
        )
        staged = _stage(
            database,
            {
                "content": "final answer",
                "submission_id": "submission-1",
                "finalized": True,
            },
        )
        await database.execute(
            """
            UPDATE render_outbox
            SET logical_seq = 2, payload = ?, content_key = ?, content_hash = ?,
                render_kind = ?, finalized = ?, payload_revision = 2,
                state = 'pending', last_error = NULL, updated_at = 101
            WHERE id = 'stable'
            """,
            staged,
        )

        assert await dispatcher.dispatch_once(now=101) == 1
        reaction = await database.fetchone(
            """
            SELECT desired_state, resume_state, revision, terminal, last_error
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        reaction_outbox = await database.fetchone(
            "SELECT payload, state FROM render_outbox WHERE lane = 'reaction'"
        )

    assert dict(reaction) == {
        "desired_state": expected_reaction,
        "resume_state": None,
        "revision": 3,
        "terminal": 1,
        "last_error": None,
    }
    assert reaction_outbox["state"] == "pending"
    assert json.loads(str(reaction_outbox["payload"]))["state"] == expected_reaction


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
        assert await dispatcher.dispatch_once(now=103) == 1
        state = await database.fetchone("SELECT state, attempts, last_error FROM render_outbox")

    assert state["state"] == "blocked"
    assert state["attempts"] == 3
    assert state["last_error"] == "rendertransienterror"
    assert transport.sent[-1][2]["type"] == "render.failure"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        DiscordHttpRedirectBlocked(),
        DiscordHttpRouteCapacityExceeded(),
        DiscordHttpRateLimiterClosed(),
        DiscordBackpressure("semantic coordinator full"),
    ],
)
async def test_discord_capacity_failures_remain_retryable_in_render_dispatcher(
    tmp_path: Path,
    error: Exception,
) -> None:
    async with Database(tmp_path / f"capacity-{type(error).__name__}.sqlite3") as database:
        await _insert_outbox(
            database,
            item_id="capacity",
            sequence=1,
            payload={"type": "assistant.message", "content": "hello"},
        )
        transport = FakeTransport()
        transport.failures.append(error)
        dispatcher = RenderOutboxDispatcher(database, transport)

        assert await dispatcher.dispatch_once(now=100) == 0
        row = await database.fetchone(
            "SELECT state, attempts, next_attempt_at FROM render_outbox WHERE id = 'capacity'"
        )

    assert dict(row) == {
        "state": "pending",
        "attempts": 1,
        "next_attempt_at": 101,
    }


@pytest.mark.asyncio
async def test_missing_content_surface_retries_durably_across_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "missing-content-restart.sqlite3"
    async with Database(path) as database:
        await database.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at,
                content_key, content_hash, render_kind, finalized,
                source_submission_id
            ) VALUES (
                'missing-original', 'session-1', 1, 'assistant_final',
                'assistant:missing', 'render:missing',
                '{"schema":1,"render_kind":"assistant.message","finalized":true}',
                'pending', 0, 0, 0, 0,
                'vc:missing-after-restart', 'missing-hash',
                'assistant.message', 1, 'submission-missing'
            )
            """
        )
        first_transport = FakeTransport()
        dispatcher = RenderOutboxDispatcher(database, first_transport)
        assert await dispatcher.dispatch_once(now=100) == 0
        rows = await database.fetchall(
            """
            SELECT id, state, render_kind, content_key, error_code
            FROM render_outbox ORDER BY created_at, id
            """
        )
        assert len(rows) == 2
        original = next(row for row in rows if row["id"] == "missing-original")
        fallback = next(row for row in rows if row["id"] != "missing-original")
        assert dict(original) == {
            "id": "missing-original",
            "state": "content_unavailable",
            "render_kind": "assistant.message",
            "content_key": None,
            "error_code": "content_unavailable",
        }
        assert fallback["state"] == "pending"
        assert fallback["render_kind"] == "content_unavailable"
        assert fallback["content_key"] is None
        assert first_transport.sent == []

    async with Database(path) as restarted:
        transport = FakeTransport()
        transport.failures.append(RenderTransientError("temporary Discord failure"))
        dispatcher = RenderOutboxDispatcher(restarted, transport)
        assert await dispatcher.dispatch_once(now=101) == 0
        retry = await restarted.fetchone(
            """
            SELECT state, attempts, next_attempt_at
            FROM render_outbox WHERE render_kind = 'content_unavailable'
            """
        )
        assert dict(retry) == {
            "state": "pending",
            "attempts": 1,
            "next_attempt_at": 102,
        }
        assert await dispatcher.dispatch_once(now=102) == 1
        delivered = await restarted.fetchone(
            """
            SELECT state, attempts FROM render_outbox
            WHERE render_kind = 'content_unavailable'
            """
        )
        persisted = "\n".join(
            str(row["payload"])
            for row in await restarted.fetchall("SELECT payload FROM render_outbox ORDER BY id")
        )

    assert dict(delivered) == {"state": "sent", "attempts": 2}
    assert len(transport.sent) == 1
    assert transport.sent[0][2]["type"] == "render.content_unavailable"
    assert "Response content unavailable" in transport.sent[0][2]["content"]
    assert "Response content unavailable" not in persisted
