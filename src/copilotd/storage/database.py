from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from importlib import resources
from pathlib import Path
from typing import Any

import aiosqlite

from copilotd.core.volatile_content import CommittedCancellation, VolatileContentStore

_CORE_MIGRATION_VERSION = 53
_LEGACY_DISCORD_MIGRATION_NAMES = {
    48: "0048_tool_activity_projections.sql",
    49: "0049_quiet_discord_surfaces.sql",
    50: "0050_discord_reaction_feedback.sql",
}
_CURRENT_DISCORD_MIGRATION_NAMES = {
    48: "0048_discord_reaction_delivery.sql",
    49: "0049_single_turn_card.sql",
    50: "0050_message_queue_discord_channel.sql",
}
_LEGACY_MIGRATION_REMAPPINGS = (
    (
        8,
        "0008_render_streams_agent_id.sql",
        30,
        "0030_render_streams_agent_id.sql",
        frozenset({"render_streams"}),
    ),
    (
        9,
        "0009_render_attachment_delivery.sql",
        31,
        "0031_render_attachment_delivery.sql",
        frozenset(
            {
                "render_attachment_batches",
                "render_attachment_checkpoints",
            }
        ),
    ),
    (
        10,
        "0010_product_surface.sql",
        32,
        "0032_product_surface.sql",
        frozenset(
            {
                "pinned_message_provenance",
                "render_parent_diagnostics",
                "session_projection_snapshots",
                "session_ui_metadata",
                "tool_output_streams",
            }
        ),
    ),
    (
        11,
        "0011_review_hardening.sql",
        33,
        "0033_review_hardening.sql",
        frozenset({"render_batch_intents"}),
    ),
    (
        12,
        "0012_tool_spill_artifacts.sql",
        34,
        "0034_tool_spill_artifacts.sql",
        frozenset({"tool_spill_artifacts"}),
    ),
    (
        13,
        "0013_attachment_inline_variants.sql",
        35,
        "0035_attachment_inline_variants.sql",
        frozenset({"attachment_inline_variants"}),
    ),
    (
        14,
        "0014_trusted_local_artifacts.sql",
        36,
        "0036_trusted_local_artifacts.sql",
        frozenset({"trusted_local_artifacts"}),
    ),
    (
        15,
        "0015_trusted_local_artifact_snapshots.sql",
        37,
        "0037_trusted_local_artifact_snapshots.sql",
        frozenset({"trusted_local_artifact_snapshots"}),
    ),
)


async def _settle_database_operation(
    operation_awaitable: Any,
    *,
    operation: str,
) -> asyncio.CancelledError | None:
    task = asyncio.create_task(operation_awaitable, name=f"sqlite-{operation}")
    caller_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                caller_cancellation = caller_cancellation or error
                continue
            if task.done():
                break
            raise
        except BaseException:
            break
    task.result()
    return caller_cancellation


class Database:
    """Single-process async SQLite connection with serialized transactions."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._connection: aiosqlite.Connection | None = None
        self._transaction_lock = asyncio.Lock()
        self._transaction_owner: asyncio.Task[Any] | None = None
        self.render_delivery_lock = asyncio.Lock()
        self.content_store = VolatileContentStore()

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("database is not open")
        return self._connection

    async def open(self, *, secure_erase: bool = False) -> None:
        if self._connection is not None:
            cleanup_maintenance_ran = await self._retry_state_only_cleanup()
            if secure_erase and not cleanup_maintenance_ran:
                await self.secure_maintenance()
            return
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._connection = await aiosqlite.connect(str(self.path), isolation_level=None)
            self._connection.row_factory = aiosqlite.Row
            await self._connection.execute("PRAGMA foreign_keys = ON")
            await self._connection.execute("PRAGMA busy_timeout = 5000")
            await self._connection.execute("PRAGMA synchronous = NORMAL")
            await self._connection.execute("PRAGMA secure_delete = ON")
            if str(self.path) != ":memory:":
                await self._connection.execute("PRAGMA journal_mode = WAL")
            await self._ensure_migration_table()
            await self._remap_legacy_migration_identities()
            await self._normalize_legacy_discord_migration_collisions()
            await self.migrate()
            cleanup_maintenance_ran = await self._retry_state_only_cleanup()
            await self.apply_compatibility_patches()
            if secure_erase and not cleanup_maintenance_ran:
                try:
                    await self.secure_maintenance()
                except Exception as error:
                    raise RuntimeError("secure erase failed") from error
        except BaseException:
            if self._connection is not None:
                await self._connection.close()
                self._connection = None
            raise

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

    async def _remap_legacy_migration_identities(self) -> None:
        async with self.transaction() as connection:
            cursor = await connection.execute(
                "SELECT version, name, applied_at FROM schema_migrations"
            )
            rows = await cursor.fetchall()
            await cursor.close()
            applied = {
                int(row["version"]): (str(row["name"]), float(row["applied_at"])) for row in rows
            }
            legacy_rows = [
                remapping
                for remapping in _LEGACY_MIGRATION_REMAPPINGS
                if applied.get(remapping[0], (None, None))[0] == remapping[1]
            ]
            if not legacy_rows:
                return

            cursor = await connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            tables = {str(row["name"]) for row in await cursor.fetchall()}
            await cursor.close()
            for (
                legacy_version,
                legacy_name,
                current_version,
                current_name,
                required_tables,
            ) in legacy_rows:
                if not required_tables <= tables:
                    missing = ", ".join(sorted(required_tables - tables))
                    raise RuntimeError(
                        f"legacy migration {legacy_name} is recorded but its schema "
                        f"is missing: {missing}"
                    )
                current = applied.get(current_version)
                if current is not None and current[0] != current_name:
                    raise RuntimeError(
                        f"migration version {current_version} is already recorded as "
                        f"{current[0]}, not {current_name}"
                    )
                if current is None:
                    applied_at = applied[legacy_version][1]
                    await connection.execute(
                        """
                        INSERT INTO schema_migrations(version, name, applied_at)
                        VALUES (?, ?, ?)
                        """,
                        (current_version, current_name, applied_at),
                    )
                    applied[current_version] = (current_name, applied_at)
                await connection.execute(
                    "DELETE FROM schema_migrations WHERE version = ? AND name = ?",
                    (legacy_version, legacy_name),
                )
                applied.pop(legacy_version)

    async def _normalize_legacy_discord_migration_collisions(self) -> None:
        rows = await self.fetchall(
            """
            SELECT version, name FROM schema_migrations
            WHERE version BETWEEN 48 AND 50
            """
        )
        recorded = {int(row["version"]): str(row["name"]) for row in rows}
        legacy_versions = {
            version
            for version, legacy_name in _LEGACY_DISCORD_MIGRATION_NAMES.items()
            if recorded.get(version) == legacy_name
        }
        if not legacy_versions:
            return
        if 49 in legacy_versions and recorded.get(48) != _LEGACY_DISCORD_MIGRATION_NAMES[48]:
            raise RuntimeError(
                "legacy migration 0049_quiet_discord_surfaces.sql requires the "
                "recorded 0048_tool_activity_projections.sql schema"
            )
        if 50 in legacy_versions and (
            recorded.get(48) != _LEGACY_DISCORD_MIGRATION_NAMES[48]
            or recorded.get(49) != _LEGACY_DISCORD_MIGRATION_NAMES[49]
        ):
            raise RuntimeError(
                "legacy migration 0050_discord_reaction_feedback.sql requires the "
                "recorded legacy 0048/0049 migration sequence"
            )

        async with self.transaction() as connection:
            await _require_table_columns(
                connection,
                "tool_activity_projections",
                {
                    "sdk_session_id",
                    "tool_call_id",
                    "submission_id",
                    "activity_key",
                    "tool_name",
                    "state",
                    "error_summary",
                    "first_seen_at",
                    "last_seen_at",
                    "last_event_id",
                    "revision",
                },
                migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[48],
            )
            await _require_indexes(
                connection,
                {
                    "idx_event_journal_tool_call",
                    "idx_render_outbox_session_sequence",
                    "idx_tool_activity_submission_state",
                    "idx_tool_activity_key_state",
                },
                migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[48],
            )
            if 50 in legacy_versions:
                await _require_table_columns(
                    connection,
                    "message_queue",
                    {
                        "discord_feedback_reaction",
                        "discord_feedback_status",
                        "discord_feedback_attempts",
                        "discord_feedback_next_attempt_at",
                        "discord_feedback_updated_at",
                        "discord_channel_id",
                    },
                    migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[50],
                )
                await _require_table_columns(
                    connection,
                    "render_outbox",
                    {"source_received_at"},
                    migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[50],
                )
                await _require_table_columns(
                    connection,
                    "discord_admission_feedback",
                    {
                        "feedback_id",
                        "channel_id",
                        "message_id",
                        "desired_reaction",
                        "status",
                        "attempts",
                        "next_attempt_at",
                        "created_at",
                        "updated_at",
                    },
                    migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[50],
                )
                await _require_indexes(
                    connection,
                    {
                        "message_queue_discord_feedback_idx",
                        "render_outbox_feedback_window_idx",
                        "discord_admission_feedback_ready_idx",
                    },
                    migration_name=_LEGACY_DISCORD_MIGRATION_NAMES[50],
                )

            await _ensure_current_discord_schema(
                connection,
                through_version=max(legacy_versions),
            )
            if 50 in legacy_versions:
                await self._translate_legacy_discord_reactions(connection)
            for version in sorted(legacy_versions):
                await connection.execute(
                    """
                    UPDATE schema_migrations SET name = ?
                    WHERE version = ? AND name = ?
                    """,
                    (
                        _CURRENT_DISCORD_MIGRATION_NAMES[version],
                        version,
                        _LEGACY_DISCORD_MIGRATION_NAMES[version],
                    ),
                )

    async def _translate_legacy_discord_reactions(
        self,
        connection: aiosqlite.Connection,
    ) -> None:
        queue_cursor = await connection.execute(
            """
            SELECT 'message_queue' AS legacy_source, q.id AS legacy_id,
                   q.discord_channel_id AS channel_id,
                   q.discord_message_id AS message_id,
                   q.discord_feedback_reaction AS emoji,
                   q.discord_feedback_status AS legacy_status,
                   q.discord_feedback_attempts AS attempts,
                   q.discord_feedback_next_attempt_at AS next_attempt_at,
                   q.created_at,
                   COALESCE(q.discord_feedback_updated_at, q.updated_at) AS updated_at,
                   b.sdk_session_id
            FROM message_queue q
            LEFT JOIN session_bindings b ON b.thread_id = q.thread_id
            WHERE q.discord_feedback_status IN ('pending', 'retry')
              AND q.discord_feedback_reaction IS NOT NULL
              AND q.discord_channel_id IS NOT NULL
              AND q.discord_message_id IS NOT NULL
            """
        )
        queue_rows = [dict(row) for row in await queue_cursor.fetchall()]
        await queue_cursor.close()
        admission_cursor = await connection.execute(
            """
            SELECT 'discord_admission_feedback' AS legacy_source,
                   feedback_id AS legacy_id, channel_id, message_id,
                   desired_reaction AS emoji, status AS legacy_status,
                   attempts, next_attempt_at, created_at, updated_at,
                   NULL AS sdk_session_id
            FROM discord_admission_feedback
            WHERE status IN ('pending', 'retry')
            """
        )
        admission_rows = [dict(row) for row in await admission_cursor.fetchall()]
        await admission_cursor.close()
        candidates = sorted(
            [*queue_rows, *admission_rows],
            key=lambda row: (float(row["updated_at"]), str(row["legacy_id"])),
            reverse=True,
        )
        for row in candidates:
            channel_id = str(row["channel_id"])
            message_id = str(row["message_id"])
            key = f"admission-reaction:{channel_id}:{message_id}"
            reaction_state = {
                "👀": "accepted",
                "🧠": "reasoning",
                "🛠️": "action",
                "❓": "unresolved",
                "✅": "succeeded",
                "❌": "failed",
            }.get(str(row["emoji"]), "accepted")
            payload = json.dumps(
                {
                    "type": "admission_reaction",
                    "source_channel_id": channel_id,
                    "source_message_id": message_id,
                    "state": reaction_state,
                    "emoji": str(row["emoji"]),
                    "finalized": False,
                    "legacy_source": str(row["legacy_source"]),
                    "legacy_id": str(row["legacy_id"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            await connection.execute(
                """
                INSERT OR IGNORE INTO render_outbox(
                    id, session_id, logical_seq, lane, coalesce_key,
                    idempotency_key, payload, state, attempts,
                    next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 0, 'admission_reaction', ?, ?, ?, 'pending',
                          ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
                    row["sdk_session_id"] or f"admission:{channel_id}:{message_id}",
                    key,
                    key,
                    payload,
                    int(row["attempts"]),
                    float(row["next_attempt_at"]),
                    float(row["created_at"]),
                    float(row["updated_at"]),
                ),
            )
            if row["legacy_source"] == "message_queue":
                await connection.execute(
                    """
                    UPDATE message_queue
                    SET discord_feedback_status = 'migrated',
                        discord_feedback_updated_at = ?
                    WHERE id = ?
                      AND discord_feedback_status IN ('pending', 'retry')
                    """,
                    (float(row["updated_at"]), str(row["legacy_id"])),
                )
            else:
                await connection.execute(
                    """
                    UPDATE discord_admission_feedback
                    SET status = 'migrated'
                    WHERE feedback_id = ? AND status IN ('pending', 'retry')
                    """,
                    (str(row["legacy_id"]),),
                )

    async def migrate(self) -> None:
        migration_root = resources.files("copilotd.storage.migrations")
        migration_files = sorted(
            item for item in migration_root.iterdir() if item.name.endswith(".sql")
        )
        migrations_by_version = {
            int(migration.name.partition("_")[0]): migration for migration in migration_files
        }
        applied_rows = await self.fetchall("SELECT version, name FROM schema_migrations")
        applied = {int(row["version"]): str(row["name"]) for row in applied_rows}
        for version, name in applied.items():
            migration = migrations_by_version.get(version)
            if (
                migration is not None
                and version <= _CORE_MIGRATION_VERSION
                and name != migration.name
            ):
                raise RuntimeError(
                    f"migration version {version} is already recorded as {name}, "
                    f"not {migration.name}"
                )

        for migration in migration_files:
            version_text, _, _ = migration.name.partition("_")
            version = int(version_text)
            if version > _CORE_MIGRATION_VERSION:
                continue
            if version in applied:
                continue
            if version == 52:
                await self._prepare_state_only_migration()
            sql = migration.read_text(encoding="utf-8")
            async with self.transaction() as connection:
                for statement in _split_sql_statements(sql):
                    await connection.execute(statement)
                await connection.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (version, migration.name, time.time()),
                )

    async def _prepare_state_only_migration(self) -> None:
        additions = {
            "task_card_projections": {
                "dependencies_json": "TEXT NOT NULL DEFAULT '[]'",
                "artifact_links_json": "TEXT NOT NULL DEFAULT '[]'",
                "can_promote": "INTEGER NOT NULL DEFAULT 0",
                "last_progress_at": "REAL",
            },
            "session_creation_intents": {
                "project_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "channel_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "layout": "TEXT NOT NULL DEFAULT 'text'",
                "project_config_version": "INTEGER NOT NULL DEFAULT 1",
                "channel_config_version": "INTEGER NOT NULL DEFAULT 1",
                "config_snapshot_state": "TEXT NOT NULL DEFAULT 'legacy_unverified'",
            },
            "session_bindings": {
                "session_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "channel_config_snapshot": "TEXT NOT NULL DEFAULT '{}'",
                "config_snapshot_state": "TEXT NOT NULL DEFAULT 'legacy_unverified'",
            },
        }
        async with self.transaction() as connection:
            for table, declarations in additions.items():
                columns = await _connection_column_names(connection, table)
                for name, declaration in declarations.items():
                    if name not in columns:
                        await connection.execute(
                            f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                        )

    async def _retry_state_only_cleanup(self) -> bool:
        if not await self._table_exists("state_only_cleanup"):
            return False
        cleanup = await self.fetchone(
            """
            SELECT state FROM state_only_cleanup
            WHERE cleanup_key = 'legacy_content_artifacts'
            """
        )
        if cleanup is None or str(cleanup["state"]) == "complete":
            return False

        root = None if str(self.path) == ":memory:" else self.path.parent.resolve(strict=False)
        discovered = (
            () if root is None else await asyncio.to_thread(_managed_legacy_artifact_files, root)
        )
        if discovered:
            async with self.transaction() as connection:
                await connection.executemany(
                    """
                    INSERT OR IGNORE INTO state_only_cleanup_artifacts(managed_path)
                    VALUES (?)
                    """,
                    ((str(path),) for path in discovered),
                )
        rows = await self.fetchall(
            """
            SELECT artifact_id, managed_path
            FROM state_only_cleanup_artifacts
            WHERE state = 'pending'
            ORDER BY artifact_id
            """
        )
        for row in rows:
            raw_path = str(row["managed_path"])
            path_digest = hashlib.sha256(raw_path.encode()).hexdigest()
            try:
                removed = (
                    False
                    if root is None
                    else await asyncio.to_thread(
                        _remove_managed_legacy_artifact,
                        root,
                        raw_path,
                    )
                )
            except OSError as error:
                raise RuntimeError(
                    "legacy content artifact could not be securely removed"
                ) from error
            if not removed:
                await self.execute(
                    """
                    UPDATE state_only_cleanup_artifacts
                    SET managed_path = NULL, path_sha256 = ?,
                        state = 'ignored_unmanaged', removed_at = ?
                    WHERE artifact_id = ? AND state = 'pending'
                    """,
                    (path_digest, time.time(), int(row["artifact_id"])),
                )
                continue
            await self.execute(
                """
                UPDATE state_only_cleanup_artifacts
                SET managed_path = NULL, path_sha256 = ?,
                    state = 'removed', removed_at = ?
                WHERE artifact_id = ? AND state = 'pending'
                """,
                (path_digest, time.time(), int(row["artifact_id"])),
            )

        try:
            await self.secure_maintenance()
        except Exception as error:
            raise RuntimeError("secure erase failed after state-only migration") from error
        completed_at = time.time()
        await self.execute(
            """
            UPDATE state_only_cleanup
            SET state = 'complete', completed_at = ?, updated_at = ?
            WHERE cleanup_key = 'legacy_content_artifacts' AND state = 'pending'
            """,
            (completed_at, completed_at),
        )
        try:
            await self._checkpoint_cleanup_marker()
        except Exception:
            await self.execute(
                """
                UPDATE state_only_cleanup
                SET state = 'pending', completed_at = NULL, updated_at = ?
                WHERE cleanup_key = 'legacy_content_artifacts'
                """,
                (time.time(),),
            )
            raise
        return True

    async def _checkpoint_cleanup_marker(self) -> None:
        if str(self.path) == ":memory:":
            return
        async with self._serialized_connection():
            await self._checkpoint_truncate()

    async def apply_compatibility_patches(self) -> None:
        await self._ensure_render_attachment_delivery_schema()
        await self._ensure_review_hardening_columns()

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
            "0031_render_attachment_delivery.sql"
        )
        sql = migration.read_text(encoding="utf-8")
        async with self.transaction() as connection:
            for statement in _split_sql_statements(sql):
                await connection.execute(statement)

    async def _ensure_review_hardening_columns(self) -> None:
        additions = {
            "render_outbox": {
                "payload_revision": "INTEGER NOT NULL DEFAULT 1",
            },
            "render_batch_intents": {
                "delivery_family": "TEXT NOT NULL DEFAULT ''",
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
                SET config_snapshot_state = 'verified'
                WHERE config_snapshot_state = 'legacy_unverified'
                  AND json_valid(session_config_snapshot)
                  AND json_valid(channel_config_snapshot)
                  AND (
                    session_config_snapshot != '{}'
                    OR channel_config_snapshot != '{}'
                    OR session_config_snapshot_json IS NOT NULL
                  )
                """
            )
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
        if await self._table_exists("render_batch_intents"):
            await self.execute(
                """
                UPDATE render_batch_intents
                SET delivery_family = render_message_id
                WHERE delivery_family = ''
                """
            )
            await self.execute(
                """
                CREATE INDEX IF NOT EXISTS render_batch_intents_family_idx
                ON render_batch_intents(
                    session_id, delivery_family, agent_id, batch_index, updated_at
                )
                """
            )
        if await self._table_exists("session_creation_intents"):
            await self.execute(
                """
                UPDATE session_creation_intents
                SET config_snapshot_state = 'verified'
                WHERE config_snapshot_state = 'legacy_unverified'
                  AND json_valid(project_config_snapshot)
                  AND json_valid(channel_config_snapshot)
                  AND (
                    project_config_snapshot != '{}'
                    OR channel_config_snapshot != '{}'
                    OR session_config_snapshot_json IS NOT NULL
                  )
                """
            )
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
                except BaseException as error:
                    rollback_cancellation = await _settle_database_operation(
                        connection.rollback(),
                        operation="rollback",
                    )
                    if rollback_cancellation is not None and not isinstance(
                        error,
                        asyncio.CancelledError,
                    ):
                        raise rollback_cancellation from error
                    raise
                else:
                    try:
                        commit_cancellation = await _settle_database_operation(
                            connection.commit(),
                            operation="commit",
                        )
                    except BaseException:
                        await _settle_database_operation(
                            connection.rollback(),
                            operation="rollback",
                        )
                        raise
                    if commit_cancellation is not None:
                        raise CommittedCancellation(
                            "transaction committed after caller cancellation"
                        ) from commit_cancellation
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

    async def secure_maintenance(self) -> None:
        """Checkpoint and erase free pages after state-only migration cleanup."""
        if str(self.path) == ":memory:":
            return
        if self._transaction_owner is asyncio.current_task():
            raise RuntimeError("secure maintenance cannot run inside a transaction")
        async with self._serialized_connection():
            try:
                cursor = await self.connection.execute("PRAGMA secure_delete = ON")
                secure_delete = await cursor.fetchone()
                await cursor.close()
                if secure_delete is None or int(secure_delete[0]) != 1:
                    raise RuntimeError("SQLite did not enable secure_delete")
                await self._checkpoint_truncate()
                cursor = await self.connection.execute("VACUUM")
                await cursor.close()
                await self._checkpoint_truncate()
            except Exception as error:
                if isinstance(error, RuntimeError):
                    raise
                raise RuntimeError("SQLite secure maintenance failed") from error

    async def _checkpoint_truncate(self) -> None:
        cursor = await self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = await cursor.fetchone()
        await cursor.close()
        if result is None or int(result[0]) != 0:
            raise RuntimeError("SQLite WAL checkpoint remained busy")

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


async def _ensure_current_discord_schema(
    connection: aiosqlite.Connection,
    *,
    through_version: int,
) -> None:
    columns = await _connection_column_names(connection, "submissions")
    for name in ("discord_source_channel_id", "discord_source_message_id"):
        if name not in columns:
            await connection.execute(f"ALTER TABLE submissions ADD COLUMN {name} TEXT")
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS submission_reactions (
            submission_id TEXT PRIMARY KEY
                REFERENCES submissions(submission_id) ON DELETE CASCADE,
            sdk_session_id TEXT NOT NULL,
            source_channel_id TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            desired_state TEXT NOT NULL,
            resume_state TEXT,
            delivered_state TEXT,
            revision INTEGER NOT NULL DEFAULT 1,
            delivered_revision INTEGER NOT NULL DEFAULT 0,
            runtime_generation INTEGER NOT NULL,
            owner_fence_token INTEGER NOT NULL,
            terminal INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    await _require_table_columns(
        connection,
        "submission_reactions",
        {
            "submission_id",
            "sdk_session_id",
            "source_channel_id",
            "source_message_id",
            "desired_state",
            "resume_state",
            "delivered_state",
            "revision",
            "delivered_revision",
            "runtime_generation",
            "owner_fence_token",
            "terminal",
            "last_error",
            "created_at",
            "updated_at",
        },
        migration_name=_CURRENT_DISCORD_MIGRATION_NAMES[48],
    )
    await connection.execute(
        """
        CREATE INDEX IF NOT EXISTS submission_reactions_delivery_idx
        ON submission_reactions(
            sdk_session_id, desired_state, delivered_state, updated_at
        )
        """
    )
    if through_version < 49:
        return

    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS turn_render_state (
            sdk_session_id TEXT NOT NULL,
            turn_key TEXT NOT NULL,
            submission_id TEXT
                REFERENCES submissions(submission_id) ON DELETE CASCADE,
            segment_index INTEGER,
            state TEXT NOT NULL DEFAULT 'running',
            answer_payload TEXT,
            runtime_generation INTEGER NOT NULL,
            owner_fence_token INTEGER NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (sdk_session_id, turn_key)
        )
        """
    )
    await _require_table_columns(
        connection,
        "turn_render_state",
        {
            "sdk_session_id",
            "turn_key",
            "submission_id",
            "segment_index",
            "state",
            "answer_payload",
            "runtime_generation",
            "owner_fence_token",
            "created_at",
            "updated_at",
        },
        migration_name=_CURRENT_DISCORD_MIGRATION_NAMES[49],
    )
    await connection.execute(
        """
        CREATE INDEX IF NOT EXISTS turn_render_state_submission_idx
        ON turn_render_state(sdk_session_id, submission_id, segment_index)
        """
    )
    await connection.execute(
        """
        UPDATE render_outbox
        SET state = 'superseded', updated_at = strftime('%s', 'now')
        WHERE state IN ('pending', 'sending', 'blocked')
          AND (
            lane IN ('diff', 'taskdeck')
            OR json_extract(payload, '$.type') IN (
                'diff', 'taskdeck', 'tool_output_artifact'
            )
          )
        """
    )
    if through_version < 50:
        return

    columns = await _connection_column_names(connection, "message_queue")
    if "discord_channel_id" not in columns:
        await connection.execute("ALTER TABLE message_queue ADD COLUMN discord_channel_id TEXT")


def _managed_legacy_artifact_files(root: Path) -> tuple[Path, ...]:
    sessions_root = root / "sessions"
    if not sessions_root.is_dir() or sessions_root.is_symlink():
        return ()
    discovered: list[Path] = []
    session_directories = tuple(sessions_root.iterdir())

    def raise_walk_error(error: OSError) -> None:
        raise error

    for session_directory in session_directories:
        if session_directory.is_symlink() or not session_directory.is_dir():
            continue
        artifacts = session_directory / "artifacts"
        if artifacts.is_symlink() or not artifacts.is_dir():
            continue
        for directory, child_directories, filenames in os.walk(
            artifacts,
            followlinks=False,
            onerror=raise_walk_error,
        ):
            directory_path = Path(directory)
            child_directories[:] = [
                name for name in child_directories if not (directory_path / name).is_symlink()
            ]
            discovered.extend(directory_path / name for name in filenames)
    return tuple(sorted(discovered))


def _is_managed_legacy_artifact(root: Path, candidate: Path) -> bool:
    try:
        relative = candidate.relative_to(root / "sessions")
    except ValueError:
        return False
    return len(relative.parts) >= 3 and relative.parts[1] in {
        "artifacts",
        "attachments",
    }


def _remove_managed_legacy_artifact(root: Path, raw_path: str) -> bool:
    candidate = Path(raw_path)
    target = candidate if candidate.is_absolute() else root / candidate
    if not _is_managed_legacy_artifact(root, target.resolve(strict=False)):
        return False
    target.unlink(missing_ok=True)
    return True


async def _require_table_columns(
    connection: aiosqlite.Connection,
    table: str,
    required: set[str],
    *,
    migration_name: str,
) -> None:
    columns = await _connection_column_names(connection, table)
    if not columns:
        raise RuntimeError(
            f"legacy migration {migration_name} is recorded but its schema is missing table {table}"
        )
    missing = required - columns
    if missing:
        raise RuntimeError(
            f"legacy migration {migration_name} is recorded but {table} is "
            f"missing columns: {', '.join(sorted(missing))}"
        )


async def _connection_column_names(
    connection: aiosqlite.Connection,
    table: str,
) -> set[str]:
    cursor = await connection.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    await cursor.close()
    return {str(row["name"]) for row in rows}


async def _require_indexes(
    connection: aiosqlite.Connection,
    required: set[str],
    *,
    migration_name: str,
) -> None:
    cursor = await connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    indexes = {str(row["name"]) for row in await cursor.fetchall()}
    await cursor.close()
    missing = required - indexes
    if missing:
        raise RuntimeError(
            f"legacy migration {migration_name} is recorded but its schema is "
            f"missing indexes: {', '.join(sorted(missing))}"
        )


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
