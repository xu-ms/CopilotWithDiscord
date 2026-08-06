from __future__ import annotations

import sqlite3
import time
from importlib import resources
from pathlib import Path

import pytest

from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_compatibility_patches_create_render_stream_key_and_attachment_tables(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "schema.sqlite3") as database:
        await database.apply_compatibility_patches()
        render_columns = await database.fetchall("PRAGMA table_info(render_streams)")
        render_tables = await database.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name LIKE 'render_attachment_%' ORDER BY name"
        )
        index = await database.fetchone(
            "SELECT name FROM sqlite_master WHERE type = 'index' "
            "AND name = 'render_attachment_batches_idempotency_idx'"
        )
        batch_index = await database.fetchall(
            "PRAGMA index_info(render_attachment_batches_idempotency_idx)"
        )

    assert [row["name"] for row in render_columns] == [
        "session_id",
        "message_id",
        "agent_id",
        "content",
        "finalized",
        "updated_at",
    ]
    assert [row["name"] for row in render_columns if int(row["pk"]) > 0] == [
        "session_id",
        "message_id",
        "agent_id",
    ]
    assert [row["name"] for row in render_tables] == [
        "render_attachment_batches",
        "render_attachment_checkpoints",
    ]
    assert index is not None
    assert [row["name"] for row in batch_index] == ["idempotency_key"]


@pytest.mark.asyncio
async def test_render_stream_compatibility_patch_preserves_existing_rows(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        "CREATE TABLE schema_migrations ("
        "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL)"
    )
    migration_root = resources.files("copilotd.storage.migrations")
    for migration in sorted(
        item
        for item in migration_root.iterdir()
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) <= 7
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        version = int(migration.name.partition("_")[0])
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (version, migration.name, time.time()),
        )
    connection.execute(
        """
        INSERT INTO render_streams(session_id, message_id, content, finalized, updated_at)
        VALUES ('session-1', 'message-1', 'hello', 1, 123.0)
        """
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        await database.apply_compatibility_patches()
        row = await database.fetchone(
            "SELECT session_id, message_id, agent_id, content, finalized, updated_at "
            "FROM render_streams"
        )
        schema_rows = await database.fetchall(
            "SELECT version FROM schema_migrations ORDER BY version"
        )

    assert dict(row) == {
        "session_id": "session-1",
        "message_id": "message-1",
        "agent_id": "",
        "content": "hello",
        "finalized": 1,
        "updated_at": 123.0,
    }
    assert [item["version"] for item in schema_rows] == [
        *range(1, 15),
        *range(20, 29),
        *range(30, 38),
    ]
