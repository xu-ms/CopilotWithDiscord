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

    async def execute_count(self, sql: str, parameters: Iterable[Any] = ()) -> int:
        async with self._serialized_connection():
            cursor = await self.connection.execute(sql, tuple(parameters))
            count = cursor.rowcount
            await cursor.close()
            return count

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
