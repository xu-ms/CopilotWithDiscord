from __future__ import annotations

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
    def __init__(self, message_id: int) -> None:
        self.id = message_id
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

    async def send(self, **_kwargs: Any) -> FakeMessage:
        self.send_calls += 1
        message = FakeMessage(100 + self.send_calls)
        self.messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int) -> FakeMessage:
        return self.messages[message_id]


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
