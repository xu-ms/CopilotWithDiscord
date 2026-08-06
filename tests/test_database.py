import asyncio
import sqlite3
import time
from importlib import resources
from pathlib import Path

import pytest

from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_initial_migration_creates_full_schema(tmp_path: Path) -> None:
    database_path = tmp_path / "copilotd.sqlite3"
    expected_tables = {
        "attachment_items",
        "attachment_manifests",
        "autopilot_objectives",
        "background_observations",
        "capabilities",
        "channel_settings",
        "custom_agents",
        "event_journal",
        "execution_health",
        "extension_runtime_projections",
        "global_config",
        "hook_audit_events",
        "liveness_leases",
        "mcp_servers",
        "mcp_server_projections",
        "message_queue",
        "model_config_observations",
        "model_turns",
        "native_queue_items",
        "pending_interactions",
        "permission_audit_events",
        "plugin_dirs",
        "project_env",
        "project_extension_config_generations",
        "project_extension_custom_agents",
        "project_extension_disabled_skills",
        "project_extension_env_refs",
        "project_extension_mcp_servers",
        "project_extension_plugin_dirs",
        "project_extension_skill_dirs",
        "projects",
        "protocol_requests",
        "protocol_response_attempts",
        "reconciliation_state",
        "render_messages",
        "render_outbox",
        "render_streams",
        "runtime_incidents",
        "runtime_schedules",
        "schedule_runs",
        "schedules",
        "schema_migrations",
        "session_bindings",
        "session_creation_intents",
        "session_error_projections",
        "session_limit_projections",
        "session_operations",
        "session_owner_leases",
        "skill_dirs",
        "startup_recovery_runs",
        "snapshot_observations",
        "submissions",
        "submission_segments",
        "submission_task_links",
        "task_card_projections",
        "taskdeck_panel_state",
        "agent_loop_projections",
        "context_projections",
        "usage_samples",
        "usage_projections",
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

    assert [row["version"] for row in rows] == [*range(1, 10), *range(15, 20)]


@pytest.mark.asyncio
async def test_foundation_migration_upgrades_existing_v7_database(tmp_path: Path) -> None:
    database_path = tmp_path / "upgrade-v7.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at REAL NOT NULL
        )
        """
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
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        versions = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")
        capability_columns = await database.fetchall("PRAGMA table_info(capabilities)")
        event_columns = await database.fetchall("PRAGMA table_info(event_journal)")
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in versions] == [*range(1, 10), *range(15, 20)]
    assert "protocol_version" in {row["name"] for row in capability_columns}
    assert {
        "schema_version",
        "sdk_timestamp",
        "task_id",
        "tool_call_id",
        "correlation_id",
    } <= {row["name"] for row in event_columns}
    assert {
        "execution_health",
        "snapshot_observations",
        "startup_recovery_runs",
        "submission_segments",
        "submission_task_links",
    } <= {row["name"] for row in tables}


@pytest.mark.asyncio
async def test_protocol_migration_preserves_legacy_response_planes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "protocol-v9.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at REAL NOT NULL
        )
        """
    )
    migration_root = resources.files("copilotd.storage.migrations")
    for migration in sorted(
        item
        for item in migration_root.iterdir()
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) <= 9
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        version = int(migration.name.partition("_")[0])
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (version, migration.name, time.time()),
        )
    connection.executemany(
        """
        INSERT INTO protocol_requests(
            sdk_session_id, generation, request_id, requested_type,
            requested_event_id, completed_event_id, state
        ) VALUES ('session-1', 1, ?, ?, ?, ?, ?)
        """,
        [
            (
                "limit-pending",
                "session_limits_exhausted.requested",
                "event-limit-pending",
                None,
                "requested",
            ),
            (
                "sampling-completed",
                "sampling.requested",
                "event-sampling-requested",
                "event-sampling-completed",
                "completed",
            ),
            (
                "oauth-pending",
                "mcp.oauth_required",
                "event-oauth-pending",
                None,
                "requested",
            ),
        ],
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        rows = await database.fetchall(
            """
            SELECT request_id, response_plane, response_state
            FROM protocol_requests ORDER BY request_id
            """
        )

    assert [dict(row) for row in rows] == [
        {
            "request_id": "limit-pending",
            "response_plane": "app_rpc",
            "response_state": "pending",
        },
        {
            "request_id": "oauth-pending",
            "response_plane": "sdk_handler",
            "response_state": "delegated",
        },
        {
            "request_id": "sampling-completed",
            "response_plane": "app_rpc",
            "response_state": "completed",
        },
    ]


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
