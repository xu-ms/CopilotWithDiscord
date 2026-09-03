from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from copilotd.config import Settings
from copilotd.discord_app import (
    CopilotDiscordBot,
    DiscordRenderBatch,
    DiscordRenderPlan,
    RenderPermanentError,
    _discord_render_plan,
    _render_batch_hash,
)
from copilotd.render.tables import TableAsset
from copilotd.storage.database import Database


class FakeMessage:
    def __init__(self, message_id: int, *, nonce: str | None = None) -> None:
        self.id = message_id
        self.nonce = nonce
        self.deleted = False
        self.edits: list[dict[str, Any]] = []

    async def edit(self, **kwargs: Any) -> None:
        self.edits.append(kwargs)

    async def delete(self) -> None:
        self.deleted = True


class FakeThread:
    def __init__(self) -> None:
        self.messages: dict[int, FakeMessage] = {}
        self.send_calls = 0
        self.send_payloads: list[dict[str, Any]] = []

    async def send(self, **kwargs: Any) -> FakeMessage:
        self.send_calls += 1
        self.send_payloads.append(kwargs)
        message = FakeMessage(
            100 + self.send_calls,
            nonce=None if kwargs.get("nonce") is None else str(kwargs["nonce"]),
        )
        self.messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int) -> FakeMessage:
        return self.messages[message_id]

    async def history(self, **_kwargs: Any) -> Any:
        for message in reversed(self.messages.values()):
            yield message


@pytest.mark.asyncio
async def test_render_batches_checkpoint_each_message_and_retry_idempotently(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    plan = DiscordRenderPlan(
        (
            DiscordRenderBatch("first"),
            DiscordRenderBatch(
                "second",
                (
                    TableAsset(
                        filename="detail.txt",
                        media_type="text/plain",
                        content=b"detail",
                    ),
                ),
            ),
        )
    )
    async with Database(tmp_path / "delivery.sqlite3") as database:
        bot.database = database
        first = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-1",
            payload={"finalized": True},
            plan=plan,
            delivery_id="event:one",
        )
        retried = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-1",
            payload={"finalized": True},
            plan=plan,
            delivery_id="event:one",
        )
        rows = await database.fetchall(
            """
            SELECT batch_index, discord_message_id, attachment_count
            FROM render_attachment_batches
            ORDER BY batch_index
            """
        )

    assert first == retried == "101"
    assert thread.send_calls == 2
    assert [dict(row) for row in rows] == [
        {"batch_index": 0, "discord_message_id": "101", "attachment_count": 0},
        {"batch_index": 1, "discord_message_id": "102", "attachment_count": 1},
    ]


@pytest.mark.asyncio
async def test_growing_nonfinal_response_updates_all_batches_in_one_family(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    first_content = "".join(f"first-{index:03d} " for index in range(300))
    second_content = first_content + "".join(f"second-{index:03d} " for index in range(300))
    first_plan = await _discord_render_plan(
        {
            "type": "assistant.message_delta",
            "content": first_content,
            "finalized": False,
        }
    )
    second_plan = await _discord_render_plan(
        {
            "type": "assistant.message_delta",
            "content": second_content,
            "finalized": False,
        }
    )
    assert len(second_plan.batches) > len(first_plan.batches) > 1

    async with Database(tmp_path / "growing-stream.sqlite3") as database:
        bot.database = database
        first_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-stream",
            payload={"finalized": False},
            plan=first_plan,
            delivery_id="render:assistant:payload:1:1111111111111111",
        )
        first_message = await thread.fetch_message(int(first_id))
        await bot._deliver_render_plan(
            thread=thread,
            session_id="session-stream",
            payload={"finalized": False},
            plan=second_plan,
            delivery_id="render:assistant:payload:2:2222222222222222",
            first_message=first_message,
        )
        current = await database.fetchall(
            """
            SELECT batch_index, discord_message_id FROM render_attachment_batches
            WHERE session_id = 'session-stream'
              AND render_message_id = 'render:assistant:payload:2:2222222222222222'
            ORDER BY batch_index
            """
        )

    assert [row["discord_message_id"] for row in current] == [
        str(101 + index) for index in range(len(second_plan.batches))
    ]
    for index, batch in enumerate(second_plan.batches[: len(first_plan.batches)]):
        assert thread.messages[101 + index].edits[-1]["content"] == batch.content
    assert [payload["content"] for payload in thread.send_payloads[len(first_plan.batches) :]] == [
        batch.content for batch in second_plan.batches[len(first_plan.batches) :]
    ]


@pytest.mark.asyncio
async def test_post_send_crash_reconciles_nonce_without_duplicate(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    plan = DiscordRenderPlan((DiscordRenderBatch("crash-safe"),))
    crashed = False

    async def crash_after_send(_index: int, _message_id: str) -> None:
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("simulated process crash")

    async with Database(tmp_path / "post-send-crash.sqlite3") as database:
        bot.database = database
        bot._after_render_send_hook = crash_after_send
        with pytest.raises(RuntimeError, match="simulated process crash"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-crash",
                payload={"finalized": True},
                plan=plan,
                delivery_id="event:crash",
            )
        prepared = await database.fetchone(
            """
            SELECT state, discord_message_id FROM render_batch_intents
            WHERE session_id = 'session-crash'
            """
        )
        assert dict(prepared) == {
            "state": "prepared",
            "discord_message_id": None,
        }
        for offset in range(250):
            message_id = 1000 + offset
            thread.messages[message_id] = FakeMessage(message_id)

        bot._after_render_send_hook = None
        reconciled = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-crash",
            payload={"finalized": True},
            plan=plan,
            delivery_id="event:crash",
        )
        checkpoint = await database.fetchone(
            """
            SELECT state, discord_message_id FROM render_batch_intents
            WHERE session_id = 'session-crash'
            """
        )

    assert reconciled == "101"
    assert thread.send_calls == 1
    assert dict(checkpoint) == {
        "state": "sent",
        "discord_message_id": "101",
    }


@pytest.mark.asyncio
async def test_legacy_delivered_batch_backfills_missing_intent_without_duplicate(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    plan = DiscordRenderPlan((DiscordRenderBatch("legacy delivery"),))

    async with Database(tmp_path / "legacy-delivery.sqlite3") as database:
        bot.database = database
        first = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-legacy-delivery",
            payload={"finalized": True},
            plan=plan,
            delivery_id="event:legacy-delivery",
        )
        await database.execute(
            """
            DELETE FROM render_batch_intents
            WHERE session_id = 'session-legacy-delivery'
            """
        )
        retried = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-legacy-delivery",
            payload={"finalized": True},
            plan=plan,
            delivery_id="event:legacy-delivery",
        )
        intent = await database.fetchone(
            """
            SELECT state, discord_message_id, payload_hash
            FROM render_batch_intents
            WHERE session_id = 'session-legacy-delivery'
            """
        )

    assert first == retried == "101"
    assert thread.send_calls == 1
    assert dict(intent) == {
        "state": "sent",
        "discord_message_id": "101",
        "payload_hash": _render_batch_hash(plan.batches[0]),
    }


@pytest.mark.asyncio
async def test_delivered_plan_cannot_silently_drop_a_persisted_batch(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    original = DiscordRenderPlan(
        (
            DiscordRenderBatch("first"),
            DiscordRenderBatch("second"),
        )
    )

    async with Database(tmp_path / "delivery-count.sqlite3") as database:
        bot.database = database
        await bot._deliver_render_plan(
            thread=thread,
            session_id="session-delivery-count",
            payload={"finalized": True},
            plan=original,
            delivery_id="event:delivery-count",
        )
        with pytest.raises(RenderPermanentError, match="render batch count changed"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-delivery-count",
                payload={"finalized": True},
                plan=DiscordRenderPlan((DiscordRenderBatch("first"),)),
                delivery_id="event:delivery-count",
            )

    assert thread.send_calls == 2


@pytest.mark.asyncio
async def test_prepared_intent_recovers_across_renderer_upgrade_without_duplicate(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    plain_plan = DiscordRenderPlan((DiscordRenderBatch("plain response"),))
    rich_plan = DiscordRenderPlan(
        (
            DiscordRenderBatch(
                "",
                embeds=(
                    {
                        "title": "✨ Copilot response",
                        "description": "rich response",
                        "color": 0x5865F2,
                    },
                ),
            ),
        )
    )

    async def crash_after_send(_index: int, _message_id: str) -> None:
        raise RuntimeError("simulated renderer-upgrade crash")

    async with Database(tmp_path / "renderer-upgrade.sqlite3") as database:
        bot.database = database
        bot._after_render_send_hook = crash_after_send
        with pytest.raises(RuntimeError, match="simulated renderer-upgrade crash"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-renderer-upgrade",
                payload={"finalized": True},
                plan=plain_plan,
                delivery_id="event:renderer-upgrade",
            )

        bot._after_render_send_hook = None
        recovered = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-renderer-upgrade",
            payload={"finalized": True},
            plan=rich_plan,
            delivery_id="event:renderer-upgrade",
        )
        intent = await database.fetchone(
            """
            SELECT state, discord_message_id, payload_hash
            FROM render_batch_intents
            WHERE session_id = 'session-renderer-upgrade'
            """
        )
        with pytest.raises(RenderPermanentError, match="render batch intent changed"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-renderer-upgrade",
                payload={"finalized": True},
                plan=DiscordRenderPlan((DiscordRenderBatch("unexpected rewrite"),)),
                delivery_id="event:renderer-upgrade",
            )

    assert recovered == "101"
    assert thread.send_calls == 1
    assert thread.messages[101].edits[-1]["embeds"][0].title == "✨ Copilot response"
    assert dict(intent) == {
        "state": "sent",
        "discord_message_id": "101",
        "payload_hash": _render_batch_hash(rich_plan.batches[0]),
    }


@pytest.mark.asyncio
async def test_new_payload_revision_recovers_prior_family_message(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    async with Database(tmp_path / "delivery-family.sqlite3") as database:
        bot.database = database
        first_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-family",
            payload={"finalized": False},
            plan=DiscordRenderPlan((DiscordRenderBatch("progress"),)),
            delivery_id="artifact:family:payload:1:1111111111111111",
        )
        recovered_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-family",
            payload={"finalized": True},
            plan=DiscordRenderPlan((DiscordRenderBatch("final"),)),
            delivery_id="artifact:family:payload:2:2222222222222222",
        )
        family_rows = await database.fetchall(
            """
            SELECT delivery_family, discord_message_id
            FROM render_batch_intents
            WHERE session_id = 'session-family'
            ORDER BY render_message_id
            """
        )

    assert first_id == recovered_id == "101"
    assert thread.send_calls == 1
    assert thread.messages[101].edits[-1]["content"] == "final"
    assert [dict(row) for row in family_rows] == [
        {"delivery_family": "artifact:family", "discord_message_id": "101"},
    ]


@pytest.mark.asyncio
async def test_new_revision_skips_unsent_intent_to_recover_older_family_batches(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    initial = DiscordRenderPlan((DiscordRenderBatch("first-old"), DiscordRenderBatch("second-old")))
    updated = DiscordRenderPlan((DiscordRenderBatch("first-new"), DiscordRenderBatch("second-new")))
    async with Database(tmp_path / "delivery-family-unsent.sqlite3") as database:
        bot.database = database
        await bot._deliver_render_plan(
            thread=thread,
            session_id="session-family-unsent",
            payload={"finalized": False},
            plan=initial,
            delivery_id="artifact:family:payload:1:1111111111111111",
        )
        await database.execute(
            """
            INSERT INTO render_batch_intents(
                session_id, render_message_id, agent_id, batch_index,
                nonce, payload_hash, state, discord_message_id,
                delivery_family, created_at, updated_at
            ) VALUES (
                'session-family-unsent',
                'artifact:family:payload:2:2222222222222222',
                '', 1, 'unsent-nonce', 'unsent-hash', 'prepared', NULL,
                'artifact:family', 9999999999, 9999999999
            )
            """
        )
        recovered_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-family-unsent",
            payload={"finalized": True},
            plan=updated,
            delivery_id="artifact:family:payload:3:3333333333333333",
        )

    assert recovered_id == "101"
    assert thread.send_calls == 2
    assert thread.messages[101].edits[-1]["content"] == "first-new"
    assert thread.messages[102].edits[-1]["content"] == "second-new"


@pytest.mark.asyncio
async def test_new_revision_recovers_prior_post_send_crash_without_duplicate(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()

    async def crash_after_send(_index: int, _message_id: str) -> None:
        raise RuntimeError("simulated family crash")

    async with Database(tmp_path / "delivery-family-crash.sqlite3") as database:
        bot.database = database
        bot._after_render_send_hook = crash_after_send
        with pytest.raises(RuntimeError, match="simulated family crash"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-family-crash",
                payload={"finalized": False},
                plan=DiscordRenderPlan((DiscordRenderBatch("progress"),)),
                delivery_id="artifact:crash:payload:1:1111111111111111",
            )

        bot._after_render_send_hook = None
        recovered_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-family-crash",
            payload={"finalized": True},
            plan=DiscordRenderPlan((DiscordRenderBatch("final"),)),
            delivery_id="artifact:crash:payload:2:2222222222222222",
        )

    assert recovered_id == "101"
    assert thread.send_calls == 1
    assert thread.messages[101].edits[-1]["content"] == "final"


@pytest.mark.asyncio
async def test_edit_followup_crash_reconciles_without_orphan_duplicate(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    initial_plan = DiscordRenderPlan((DiscordRenderBatch("initial"),))
    edited_plan = DiscordRenderPlan(
        (
            DiscordRenderBatch("edited"),
            DiscordRenderBatch("follow-up"),
        )
    )

    async def crash_on_followup(index: int, _message_id: str) -> None:
        if index == 1:
            raise RuntimeError("crash after edit follow-up")

    async with Database(tmp_path / "edit-crash.sqlite3") as database:
        bot.database = database
        first_id = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-edit",
            payload={"finalized": False},
            plan=initial_plan,
            delivery_id="event:initial",
        )
        first_message = thread.messages[int(first_id)]
        bot._after_render_send_hook = crash_on_followup
        with pytest.raises(RuntimeError, match="crash after edit follow-up"):
            await bot._deliver_render_plan(
                thread=thread,
                session_id="session-edit",
                payload={"finalized": True},
                plan=edited_plan,
                delivery_id="event:edit",
                first_message=first_message,
            )

        bot._after_render_send_hook = None
        await bot._deliver_render_plan(
            thread=thread,
            session_id="session-edit",
            payload={"finalized": True},
            plan=edited_plan,
            delivery_id="event:edit",
            first_message=first_message,
        )
        edit_batches = await database.fetchall(
            """
            SELECT batch_index, discord_message_id
            FROM render_attachment_batches
            WHERE render_message_id = 'event:edit'
            ORDER BY batch_index
            """
        )

    assert thread.send_calls == 2
    assert [dict(row) for row in edit_batches] == [
        {"batch_index": 0, "discord_message_id": "101"},
        {"batch_index": 1, "discord_message_id": "102"},
    ]


class BlockingDispatcher:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.drained = False

    async def dispatch_once(self) -> int:
        self.active += 1
        self.entered.set()
        await self.release.wait()
        self.active -= 1
        return 0

    async def drain(self) -> int:
        assert self.active == 0
        self.drained = True
        return 0


@pytest.mark.asyncio
async def test_render_consumer_stops_before_shutdown_drain(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    dispatcher = BlockingDispatcher()
    bot.dispatcher = dispatcher
    bot._render_task = asyncio.create_task(bot._render_loop())
    await dispatcher.entered.wait()

    stopping = asyncio.create_task(bot._stop_render_consumer())
    await asyncio.sleep(0)
    assert not stopping.done()
    dispatcher.release.set()
    await stopping
    await dispatcher.drain()

    assert dispatcher.drained is True


@pytest.mark.asyncio
async def test_edit_prunes_obsolete_followup_batches(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    first = FakeMessage(100)
    spill_one = FakeMessage(101)
    spill_two = FakeMessage(102)
    thread.messages = {100: first, 101: spill_one, 102: spill_two}
    async with Database(tmp_path / "prune.sqlite3") as database:
        bot.database = database
        await database.execute(
            """
            INSERT INTO render_attachment_checkpoints(
                session_id, render_message_id, agent_id,
                first_discord_message_id, next_batch_index, finalized, updated_at
            ) VALUES ('session-1', 'old-delivery', '', '100', 3, 1, 0)
            """
        )
        for index, message_id in enumerate(("100", "101", "102")):
            await database.execute(
                """
                INSERT INTO render_attachment_batches(
                    session_id, render_message_id, agent_id, batch_index,
                    discord_message_id, idempotency_key, attachment_count,
                    created_at, updated_at
                ) VALUES ('session-1', 'old-delivery', '', ?, ?, ?, 0, 0, 0)
                """,
                (index, message_id, f"old:{index}"),
            )

        await bot._prune_previous_render_batches(
            thread=thread,
            session_id="session-1",
            first_message_id="100",
            current_delivery_id="new-delivery",
        )
        old_batches = await database.fetchone("SELECT COUNT(*) FROM render_attachment_batches")
        old_checkpoints = await database.fetchone(
            "SELECT COUNT(*) FROM render_attachment_checkpoints"
        )

    assert first.deleted is False
    assert spill_one.deleted is True
    assert spill_two.deleted is True
    assert old_batches[0] == 0
    assert old_checkpoints[0] == 0


@pytest.mark.asyncio
async def test_checkpoint_only_restart_prunes_obsolete_followup_batches(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    thread = FakeThread()
    first = FakeMessage(100)
    obsolete = FakeMessage(101)
    thread.messages = {100: first, 101: obsolete}
    async with Database(tmp_path / "checkpoint-prune.sqlite3") as database:
        bot.database = database
        async with database.transaction() as connection:
            await connection.executemany(
                """
                INSERT INTO render_attachment_checkpoints(
                    session_id, render_message_id, agent_id,
                    first_discord_message_id, next_batch_index, finalized, updated_at
                ) VALUES ('session-1', ?, '', '100', ?, 1, 0)
                """,
                (("old-delivery", 2), ("new-delivery", 1)),
            )
            await connection.executemany(
                """
                INSERT INTO render_attachment_batches(
                    session_id, render_message_id, agent_id, batch_index,
                    discord_message_id, idempotency_key, attachment_count,
                    created_at, updated_at
                ) VALUES ('session-1', ?, '', ?, ?, ?, 0, 0, 0)
                """,
                (
                    ("old-delivery", 0, "100", "old:0"),
                    ("old-delivery", 1, "101", "old:1"),
                    ("new-delivery", 0, "100", "new:0"),
                ),
            )

        recovered = await bot._deliver_render_plan(
            thread=thread,
            session_id="session-1",
            payload={"finalized": True},
            plan=DiscordRenderPlan((DiscordRenderBatch("recovered"),)),
            delivery_id="new-delivery",
        )
        old_checkpoint = await database.fetchone(
            """
            SELECT 1 FROM render_attachment_checkpoints
            WHERE render_message_id = 'old-delivery'
            """
        )
        current_checkpoint = await database.fetchone(
            """
            SELECT first_discord_message_id
            FROM render_attachment_checkpoints
            WHERE render_message_id = 'new-delivery'
            """
        )

    assert recovered == "100"
    assert first.deleted is False
    assert obsolete.deleted is True
    assert old_checkpoint is None
    assert current_checkpoint["first_discord_message_id"] == "100"
