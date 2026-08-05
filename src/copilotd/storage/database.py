from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from importlib import resources
from pathlib import Path
from typing import Any

import aiosqlite

_CORE_MIGRATION_VERSION = 14


class Database:
    """Single-process async SQLite connection with serialized transactions."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._connection: aiosqlite.Connection | None = None
        self._transaction_lock = asyncio.Lock()
        self._transaction_owner: asyncio.Task[Any] | None = None

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("database is not open")
        return self._connection

    async def open(self) -> None:
        if self._connection is not None:
            return
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(str(self.path), isolation_level=None)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA foreign_keys = ON")
        await self._connection.execute("PRAGMA busy_timeout = 5000")
        await self._connection.execute("PRAGMA synchronous = NORMAL")
        if str(self.path) != ":memory:":
            await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._ensure_migration_table()
        await self.migrate()
        await self.apply_compatibility_patches()

    async def close(self) -> None:
        if self._connection is None:
            return
        async with self._serialized_connection():
            if self._connection is not None:
                await self._connection.close()
                self._connection = None

    async def __aenter__(self) -> Database:
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _ensure_migration_table(self) -> None:
        await self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at REAL NOT NULL
            )
            """
        )

    async def migrate(self) -> None:
        migration_root = resources.files("copilotd.storage.migrations")
        migration_files = sorted(
            item for item in migration_root.iterdir() if item.name.endswith(".sql")
        )
        applied_rows = await self.fetchall("SELECT version FROM schema_migrations")
        applied = {int(row["version"]) for row in applied_rows}

        for migration in migration_files:
            version_text, _, _ = migration.name.partition("_")
            version = int(version_text)
            if version > _CORE_MIGRATION_VERSION:
                continue
            if version in applied:
                continue
            sql = migration.read_text(encoding="utf-8")
            async with self.transaction() as connection:
                for statement in _split_sql_statements(sql):
                    await connection.execute(statement)
                await connection.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (version, migration.name, time.time()),
                )

    async def apply_compatibility_patches(self) -> None:
        await self._ensure_render_streams_agent_schema()
        await self._ensure_render_attachment_delivery_schema()
        await self._ensure_task_surface_columns()
        await self._ensure_review_hardening_columns()

    async def _ensure_render_streams_agent_schema(self) -> None:
        if not await self._table_exists("render_streams"):
            return
        columns = await self.fetchall("PRAGMA table_info(render_streams)")
        column_names = [str(row["name"]) for row in columns]
        pk_columns = [
            str(row["name"])
            for row in sorted(columns, key=lambda row: int(row["pk"]))
            if int(row["pk"]) > 0
        ]
        if "agent_id" in column_names and pk_columns == [
            "session_id",
            "message_id",
            "agent_id",
        ]:
            return
        migration = resources.files("copilotd.storage.migrations").joinpath(
            "0008_render_streams_agent_id.sql"
        )
        sql = migration.read_text(encoding="utf-8")
        async with self.transaction() as connection:
            for statement in _split_sql_statements(sql):
                await connection.execute(statement)

    async def _ensure_render_attachment_delivery_schema(self) -> None:
        tables = await self.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name LIKE 'render_attachment_%'"
        )
        table_names = {str(row["name"]) for row in tables}
        if {
            "render_attachment_checkpoints",
            "render_attachment_batches",
        } <= table_names and await self._index_exists("render_attachment_batches_idempotency_idx"):
            return
        migration = resources.files("copilotd.storage.migrations").joinpath(
            "0009_render_attachment_delivery.sql"
        )
        sql = migration.read_text(encoding="utf-8")
        async with self.transaction() as connection:
            for statement in _split_sql_statements(sql):
                await connection.execute(statement)

    async def _ensure_task_surface_columns(self) -> None:
        if not await self._table_exists("task_card_projections"):
            return
        columns = await self.fetchall("PRAGMA table_info(task_card_projections)")
        existing = {str(row["name"]) for row in columns}
        additions = {
            "dependencies_json": "TEXT NOT NULL DEFAULT '[]'",
            "artifact_links_json": "TEXT NOT NULL DEFAULT '[]'",
            "can_promote": "INTEGER NOT NULL DEFAULT 0",
            "last_progress_at": "REAL",
        }
        async with self.transaction() as connection:
            for name, declaration in additions.items():
                if name not in existing:
                    await connection.execute(
                        f"ALTER TABLE task_card_projections ADD COLUMN {name} {declaration}"
                    )

    async def _ensure_review_hardening_columns(self) -> None:
        additions = {
            "tool_output_streams": {
                "artifact_emitted": "INTEGER NOT NULL DEFAULT 0",
                "finalized": "INTEGER NOT NULL DEFAULT 0",
            },
            "tool_spill_artifacts": {
                "retention_until": "REAL NOT NULL DEFAULT 0",
                "delivery_confirmed_at": "REAL",
            },
            "render_outbox": {
                "payload_revision": "INTEGER NOT NULL DEFAULT 1",
            },
            "session_creation_intents": {
                "project_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "channel_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "layout": "TEXT NOT NULL DEFAULT 'text'",
                "project_config_version": "INTEGER NOT NULL DEFAULT 1",
                "channel_config_version": "INTEGER NOT NULL DEFAULT 1",
                "config_snapshot_state": ("TEXT NOT NULL DEFAULT 'legacy_unverified'"),
            },
            "session_bindings": {
                "session_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "channel_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "config_snapshot_state": ("TEXT NOT NULL DEFAULT 'legacy_unverified'"),
            },
        }
        for table, columns in additions.items():
            if not await self._table_exists(table):
                continue
            rows = await self.fetchall(f"PRAGMA table_info({table})")
            existing = {str(row["name"]) for row in rows}
            async with self.transaction() as connection:
                for name, declaration in columns.items():
                    if name not in existing:
                        await connection.execute(
                            f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                        )
        if await self._table_exists("session_bindings"):
            await self.execute(
                """
                UPDATE session_bindings
                SET session_config_snapshot = '{"session_options":{}}',
                    channel_config_snapshot =
                        '{"channel_config_version":1,"layout":"text",'
                        || '"mention_required":false}',
                    config_snapshot_state = 'verified'
                WHERE config_snapshot_state = 'legacy_unverified'
                """
            )
        if await self._table_exists("tool_spill_artifacts"):
            await self.execute(
                """
                UPDATE tool_spill_artifacts
                SET retention_until = updated_at + 604800
                WHERE retention_until = 0
                """
            )
        if await self._table_exists("session_creation_intents"):
            await self.execute(
                """
                UPDATE session_creation_intents
                SET project_config_snapshot =
                        '{"project_config_version":1,"session_options":{}}',
                    channel_config_snapshot =
                        '{"channel_config_version":1,"layout":"text",'
                        || '"mention_required":false}',
                    layout = 'text',
                    project_config_version = 1,
                    channel_config_version = 1,
                    config_snapshot_state = 'verified'
                WHERE config_snapshot_state = 'legacy_unverified'
                  AND state NOT IN ('creating', 'unknown')
                """
            )

    async def _table_exists(self, name: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        )
        return row is not None

    async def _index_exists(self, name: str) -> bool:
        row = await self.fetchone(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        )
        return row is not None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._transaction_lock:
            connection = self.connection
            self._transaction_owner = asyncio.current_task()
            try:
                await connection.execute("BEGIN IMMEDIATE")
                try:
                    yield connection
                except BaseException:
                    await connection.rollback()
                    raise
                else:
                    await connection.commit()
            finally:
                self._transaction_owner = None

    async def execute(self, sql: str, parameters: Iterable[Any] = ()) -> None:
        async with self._serialized_connection():
            await self.connection.execute(sql, tuple(parameters))

    async def fetchone(self, sql: str, parameters: Iterable[Any] = ()) -> aiosqlite.Row | None:
        async with self._serialized_connection():
            async with self.connection.execute(sql, tuple(parameters)) as cursor:
                return await cursor.fetchone()

    async def fetchall(self, sql: str, parameters: Iterable[Any] = ()) -> list[aiosqlite.Row]:
        async with self._serialized_connection():
            async with self.connection.execute(sql, tuple(parameters)) as cursor:
                return list(await cursor.fetchall())

    @asynccontextmanager
    async def _serialized_connection(self) -> AsyncIterator[None]:
        if self._transaction_owner is asyncio.current_task():
            yield
            return
        async with self._transaction_lock:
            yield


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        raise ValueError("incomplete SQL statement in migration")
    return statements
