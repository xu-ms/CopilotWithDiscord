import asyncio
import shutil
import sqlite3
import time
from importlib import resources
from pathlib import Path

import pytest

from copilotd.storage.database import Database

EXPECTED_MIGRATION_VERSIONS = [*range(1, 10), *range(30, 38)]


def _create_migration_fixture(path: Path, *, through_version: int) -> None:
    connection = sqlite3.connect(path)
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
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) <= through_version
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        version = int(migration.name.partition("_")[0])
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (version, migration.name, time.time()),
        )
    connection.commit()
    connection.close()


def _create_legacy_discord_v9_fixture(path: Path) -> None:
    _create_migration_fixture(path, through_version=7)
    connection = sqlite3.connect(path)
    migration_root = resources.files("copilotd.storage.migrations")
    for legacy_version, legacy_name, current_name in (
        (
            8,
            "0008_render_streams_agent_id.sql",
            "0030_render_streams_agent_id.sql",
        ),
        (
            9,
            "0009_render_attachment_delivery.sql",
            "0031_render_attachment_delivery.sql",
        ),
    ):
        migration = migration_root.joinpath(current_name)
        connection.executescript(migration.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (legacy_version, legacy_name, time.time()),
        )
    connection.executemany(
        """
        INSERT INTO render_streams(
            session_id, message_id, agent_id, content, finalized, updated_at
        ) VALUES ('legacy-session', 'shared-message', ?, ?, 1, ?)
        """,
        (
            ("agent-a", "content-a", 8.0),
            ("agent-b", "content-b", 9.0),
        ),
    )
    connection.execute(
        """
        INSERT INTO render_attachment_checkpoints(
            session_id, render_message_id, agent_id,
            first_discord_message_id, next_batch_index, finalized, updated_at
        ) VALUES (
            'legacy-session', 'shared-message', 'agent-b',
            'discord-message', 2, 1, 9
        )
        """
    )
    connection.commit()
    connection.close()


@pytest.mark.asyncio
async def test_initial_migration_creates_full_schema(tmp_path: Path) -> None:
    database_path = tmp_path / "copilotd.sqlite3"
    expected_tables = {
        "attachment_items",
        "attachment_inline_variants",
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
        "plugin_dirs",
        "project_env",
        "projects",
        "protocol_requests",
        "reconciliation_state",
        "render_messages",
        "render_outbox",
        "render_streams",
        "render_attachment_batches",
        "render_attachment_checkpoints",
        "render_batch_intents",
        "render_parent_diagnostics",
        "runtime_incidents",
        "runtime_schedules",
        "schedule_runs",
        "schedules",
        "schema_migrations",
        "session_bindings",
        "session_creation_intents",
        "session_operations",
        "session_owner_leases",
        "session_projection_snapshots",
        "session_ui_metadata",
        "skill_dirs",
        "startup_recovery_runs",
        "snapshot_observations",
        "submissions",
        "submission_segments",
        "submission_task_links",
        "task_card_projections",
        "taskdeck_panel_state",
        "pinned_message_provenance",
        "tool_output_streams",
        "tool_spill_artifacts",
        "trusted_local_artifacts",
        "trusted_local_artifact_snapshots",
        "usage_samples",
    }

    async with Database(database_path) as database:
        tables = await database.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
        migration = await database.fetchone(
            "SELECT version, name FROM schema_migrations WHERE version = 1"
        )
        outbox_columns = await database.fetchall("PRAGMA table_info(render_outbox)")
        spill_columns = await database.fetchall("PRAGMA table_info(tool_spill_artifacts)")
        foreign_keys = await database.fetchone("PRAGMA foreign_keys")
        journal_mode = await database.fetchone("PRAGMA journal_mode")

    assert {row["name"] for row in tables} == expected_tables
    assert dict(migration) == {"version": 1, "name": "0001_initial.sql"}
    assert "payload_revision" in {row["name"] for row in outbox_columns}
    assert {"retention_until", "delivery_confirmed_at"} <= {row["name"] for row in spill_columns}
    assert foreign_keys[0] == 1
    assert journal_mode[0] == "wal"


@pytest.mark.asyncio
async def test_migrations_are_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "copilotd.sqlite3"

    async with Database(database_path):
        pass
    async with Database(database_path) as database:
        rows = await database.fetchall("SELECT version FROM schema_migrations")

    assert [row["version"] for row in rows] == EXPECTED_MIGRATION_VERSIONS


@pytest.mark.asyncio
async def test_foundation_migration_upgrades_existing_v7_database(tmp_path: Path) -> None:
    fixture_path = tmp_path / "copilotd-v7-fixture.sqlite3"
    database_path = tmp_path / "upgrade-v7.sqlite3"
    _create_migration_fixture(fixture_path, through_version=7)
    shutil.copy2(fixture_path, database_path)

    async with Database(database_path) as database:
        versions = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")
        capability_columns = await database.fetchall("PRAGMA table_info(capabilities)")
        event_columns = await database.fetchall("PRAGMA table_info(event_journal)")
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in versions] == EXPECTED_MIGRATION_VERSIONS
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
async def test_discord_migrations_upgrade_copied_foundation_v9_database(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "copilotd-foundation-v9-fixture.sqlite3"
    database_path = tmp_path / "upgrade-foundation-v9.sqlite3"
    _create_migration_fixture(fixture_path, through_version=9)
    connection = sqlite3.connect(fixture_path)
    connection.execute(
        """
        INSERT INTO render_streams(
            session_id, message_id, content, finalized, updated_at
        ) VALUES ('session-v9', 'message-v9', 'preserved', 1, 9)
        """
    )
    connection.commit()
    connection.close()
    shutil.copy2(fixture_path, database_path)

    async with Database(database_path) as database:
        versions = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")
        render_stream = await database.fetchone(
            """
            SELECT session_id, message_id, agent_id, content, finalized, updated_at
            FROM render_streams
            WHERE session_id = 'session-v9' AND message_id = 'message-v9'
            """
        )
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in versions] == EXPECTED_MIGRATION_VERSIONS
    assert dict(render_stream) == {
        "session_id": "session-v9",
        "message_id": "message-v9",
        "agent_id": "",
        "content": "preserved",
        "finalized": 1,
        "updated_at": 9.0,
    }
    assert {
        "execution_health",
        "snapshot_observations",
        "submission_task_links",
        "render_attachment_batches",
        "session_ui_metadata",
        "trusted_local_artifact_snapshots",
    } <= {row["name"] for row in tables}


@pytest.mark.asyncio
async def test_migrations_remap_copied_legacy_discord_v9_database(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "copilotd-legacy-discord-v9-fixture.sqlite3"
    database_path = tmp_path / "upgrade-legacy-discord-v9.sqlite3"
    _create_legacy_discord_v9_fixture(fixture_path)
    shutil.copy2(fixture_path, database_path)

    async with Database(database_path) as database:
        migrations = await database.fetchall(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        )
        streams = await database.fetchall(
            """
            SELECT session_id, message_id, agent_id, content, finalized, updated_at
            FROM render_streams
            WHERE session_id = 'legacy-session' AND message_id = 'shared-message'
            ORDER BY agent_id
            """
        )
        checkpoint = await database.fetchone(
            """
            SELECT agent_id, first_discord_message_id, next_batch_index, finalized
            FROM render_attachment_checkpoints
            WHERE session_id = 'legacy-session'
              AND render_message_id = 'shared-message'
            """
        )
        capability_columns = await database.fetchall("PRAGMA table_info(capabilities)")
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in migrations] == EXPECTED_MIGRATION_VERSIONS
    migration_names = {int(row["version"]): str(row["name"]) for row in migrations}
    assert migration_names[8] == "0008_durable_foundation.sql"
    assert migration_names[9] == "0009_review_invariants.sql"
    assert migration_names[30] == "0030_render_streams_agent_id.sql"
    assert migration_names[31] == "0031_render_attachment_delivery.sql"
    assert [dict(row) for row in streams] == [
        {
            "session_id": "legacy-session",
            "message_id": "shared-message",
            "agent_id": "agent-a",
            "content": "content-a",
            "finalized": 1,
            "updated_at": 8.0,
        },
        {
            "session_id": "legacy-session",
            "message_id": "shared-message",
            "agent_id": "agent-b",
            "content": "content-b",
            "finalized": 1,
            "updated_at": 9.0,
        },
    ]
    assert dict(checkpoint) == {
        "agent_id": "agent-b",
        "first_discord_message_id": "discord-message",
        "next_batch_index": 2,
        "finalized": 1,
    }
    assert "protocol_version" in {row["name"] for row in capability_columns}
    assert {
        "execution_health",
        "snapshot_observations",
        "submission_task_links",
        "trusted_local_artifact_snapshots",
    } <= {row["name"] for row in tables}


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
