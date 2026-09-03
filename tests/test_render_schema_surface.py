from __future__ import annotations

import sqlite3
import time
from importlib import resources
from pathlib import Path

import pytest

from copilotd.ops.contracts import EXPECTED_MIGRATION_VERSIONS
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_compatibility_patches_keep_render_streams_in_memory_only(
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

    assert render_columns == []
    assert [row["name"] for row in render_tables] == [
        "render_attachment_batches",
        "render_attachment_checkpoints",
    ]
    assert index is not None
    assert [row["name"] for row in batch_index] == ["idempotency_key"]


@pytest.mark.asyncio
async def test_state_only_migration_deletes_legacy_render_stream_rows(
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
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'render_streams'"
        )
        schema_rows = await database.fetchall(
            "SELECT version FROM schema_migrations ORDER BY version"
        )

    assert row is None
    assert [item["version"] for item in schema_rows] == list(EXPECTED_MIGRATION_VERSIONS)
