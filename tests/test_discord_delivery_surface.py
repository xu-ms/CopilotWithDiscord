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

    async def send(self, **kwargs: Any) -> FakeMessage:
        self.send_calls += 1
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
