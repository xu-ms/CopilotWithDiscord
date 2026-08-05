import asyncio
from pathlib import Path

import pytest

from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_initial_migration_creates_full_schema(tmp_path: Path) -> None:
    database_path = tmp_path / "copilotd.sqlite3"
    expected_tables = {
        "attachment_items",
        "attachment_manifests",
        "background_observations",
        "capabilities",
        "channel_settings",
        "custom_agents",
        "event_journal",
        "global_config",
        "liveness_leases",
        "mcp_servers",
        "message_queue",
        "model_turns",
        "pending_interactions",
        "plugin_dirs",
        "project_env",
        "projects",
        "protocol_requests",
        "reconciliation_state",
        "render_messages",
        "render_outbox",
        "render_streams",
        "runtime_incidents",
        "runtime_schedules",
        "schedule_runs",
        "schedules",
        "schema_migrations",
        "service_admission_fences",
        "service_restart_intents",
        "session_bindings",
        "session_creation_intents",
        "session_operations",
        "session_owner_leases",
        "skill_dirs",
        "submissions",
        "task_card_projections",
        "taskdeck_panel_state",
        "usage_samples",
    }

    async with Database(database_path) as database:
        tables = await database.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
        migration = await database.fetchone(
            "SELECT version, name FROM schema_migrations WHERE version = 1"
        )
        foreign_keys = await database.fetchone("PRAGMA foreign_keys")
        journal_mode = await database.fetchone("PRAGMA journal_mode")

    assert {row["name"] for row in tables} == expected_tables
    assert dict(migration) == {"version": 1, "name": "0001_initial.sql"}
    assert foreign_keys[0] == 1
    assert journal_mode[0] == "wal"


@pytest.mark.asyncio
async def test_migrations_are_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "copilotd.sqlite3"

    async with Database(database_path):
        pass
    async with Database(database_path) as database:
        rows = await database.fetchall("SELECT version FROM schema_migrations")

    assert [row["version"] for row in rows] == [1, 2, 3, 4, 5, 6, 7, 8, 9]


@pytest.mark.asyncio
async def test_helper_operations_cannot_join_another_coroutines_transaction(
    tmp_path: Path,
) -> None:
    transaction_started = asyncio.Event()
    allow_rollback = asyncio.Event()

    async with Database(tmp_path / "serialized.sqlite3") as database:
        await database.execute("CREATE TABLE transaction_probe(name TEXT PRIMARY KEY)")

        async def rollback_transaction() -> None:
            async with database.transaction() as connection:
                await connection.execute("INSERT INTO transaction_probe VALUES ('inside')")
                transaction_started.set()
                await allow_rollback.wait()
                raise RuntimeError("rollback")

        transaction_task = asyncio.create_task(rollback_transaction())
        await transaction_started.wait()
        outside_write = asyncio.create_task(
            database.execute("INSERT INTO transaction_probe VALUES ('outside')")
        )
        await asyncio.sleep(0)
        assert not outside_write.done()

        allow_rollback.set()
        with pytest.raises(RuntimeError, match="rollback"):
            await transaction_task
        await outside_write
        rows = await database.fetchall("SELECT name FROM transaction_probe ORDER BY name")

    assert [row["name"] for row in rows] == ["outside"]
