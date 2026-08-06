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
        "global_config",
        "liveness_leases",
        "mcp_servers",
        "message_queue",
        "model_turns",
        "native_queue_items",
        "pending_interactions",
        "pending_runtime_schedule_triggers",
        "plugin_dirs",
        "project_env",
        "project_config_revisions",
        "project_worktrees",
        "projects",
        "protocol_requests",
        "reconciliation_state",
        "render_messages",
        "render_outbox",
        "render_streams",
        "runtime_incidents",
        "runtime_schedules",
        "schedule_run_attempts",
        "schedule_runs",
        "schedules",
        "scheduler_events",
        "scheduler_render_intents",
        "scheduler_state",
        "schema_migrations",
        "session_bindings",
        "session_creation_intents",
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
        "usage_samples",
        "restart_intents",
        "worktree_events",
        "worktree_intents",
        "worktree_process_state",
        "worktree_recovery_runs",
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

    assert [row["version"] for row in rows] == [
        *range(1, 10),
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
    ]


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
        worktree_columns = await database.fetchall("PRAGMA table_info(worktree_intents)")
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in versions] == [
        *range(1, 10),
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
    ]
    assert "protocol_version" in {row["name"] for row in capability_columns}
    assert {
        "schema_version",
        "sdk_timestamp",
        "task_id",
        "tool_call_id",
        "correlation_id",
    } <= {row["name"] for row in event_columns}
    assert {
        "git_create_holder",
        "git_create_fence_token",
        "git_create_lease_expires_at",
        "git_create_process_generation",
        "git_create_retry_at",
    } <= {row["name"] for row in worktree_columns}
    assert {
        "execution_health",
        "snapshot_observations",
        "startup_recovery_runs",
        "submission_segments",
        "submission_task_links",
    } <= {row["name"] for row in tables}


@pytest.mark.asyncio
async def test_v24_repairs_links_for_existing_v23_database(tmp_path: Path) -> None:
    database_path = tmp_path / "upgrade-v23.sqlite3"
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
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) <= 23
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        version = int(migration.name.partition("_")[0])
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (version, migration.name, time.time()),
        )
    connection.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            created_at, updated_at
        ) VALUES ('thread-1', 'implicit-home', '/tmp', 'session-1', 1, 1)
        """
    )
    connection.execute(
        """
        INSERT INTO schedules(
            id, thread_id, kind, expression, timezone, payload,
            target_snapshot, misfire_policy, state, created_at, updated_at
        ) VALUES ('schedule-1', 'thread-1', 'message', 'cron:0 9 * * *',
                  'UTC', '{}', '{}', 'latest', 'enabled', 1, 1)
        """
    )
    connection.execute(
        """
        INSERT INTO schedule_runs(
            run_id, schedule_id, planned_key, planned_at_utc, status,
            created_at, updated_at
        ) VALUES ('run-1', 'schedule-1', 'manual:1', 1, 'submitting', 1, 1)
        """
    )
    connection.execute(
        """
        INSERT INTO submissions(
            submission_id, sdk_session_id, origin, schedule_run_id, state, created_at
        ) VALUES ('old', 'session-1', 'app_schedule', 'run-1', 'cancelled', 1)
        """
    )
    connection.execute(
        """
        INSERT INTO submissions(
            submission_id, sdk_session_id, origin, parent_submission_id,
            state, created_at
        ) VALUES ('new', 'session-1', 'app_schedule', 'old', 'local_queued', 2)
        """
    )
    connection.execute(
        """
        INSERT INTO message_queue(
            id, thread_id, schedule_run_id, prompt,
            requested_mode_snapshot, requested_model_config_snapshot,
            requested_session_config_version, position, state, created_at, updated_at
        ) VALUES ('old', 'thread-1', 'run-1', 'old', 'interactive', '{}',
                  1, 1, 'cancelled', 1, 1)
        """
    )
    connection.execute(
        """
        INSERT INTO message_queue(
            id, thread_id, prompt, requested_mode_snapshot,
            requested_model_config_snapshot, requested_session_config_version,
            position, state, replaces_id, created_at, updated_at
        ) VALUES ('new', 'thread-1', 'new', 'interactive', '{}',
                  1, 2, 'local_queued', 'old', 2, 2)
        """
    )
    connection.execute(
        """
        UPDATE schedule_runs SET result_submission_id = 'new'
        WHERE run_id = 'run-1'
        """
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        queue = await database.fetchall(
            """
            SELECT id, schedule_run_id, dispatch_attempt
            FROM message_queue ORDER BY id
            """
        )
        submissions = await database.fetchall(
            """
            SELECT submission_id, schedule_run_id
            FROM submissions ORDER BY submission_id
            """
        )

    assert [dict(row) for row in queue] == [
        {"id": "new", "schedule_run_id": "run-1", "dispatch_attempt": 0},
        {"id": "old", "schedule_run_id": None, "dispatch_attempt": 0},
    ]
    assert [dict(row) for row in submissions] == [
        {"submission_id": "new", "schedule_run_id": "run-1"},
        {"submission_id": "old", "schedule_run_id": None},
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
