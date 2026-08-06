import asyncio
import hashlib
import shutil
import sqlite3
import time
from importlib import resources
from pathlib import Path

import pytest

from copilotd.storage.database import Database

EXPECTED_MIGRATION_VERSIONS = [*range(1, 38), *range(40, 47)]


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


@pytest.mark.parametrize(
    ("name", "expected_sha256"),
    [
        (
            "0015_extension_config_generations.sql",
            "7b4a2d5c4d43e9dc3ce6a07e79313257170b483e601afbf4f129253e16944adc",
        ),
        (
            "0017_hook_permission_audit.sql",
            "60a2c024d8e336de03185159ae53bc8c2b47b571a03e8f19042f40816661818f",
        ),
    ],
)
def test_applied_protocol_migrations_remain_byte_immutable(
    name: str,
    expected_sha256: str,
) -> None:
    content = resources.files("copilotd.storage.migrations").joinpath(name).read_bytes()

    assert hashlib.sha256(content).hexdigest() == expected_sha256


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
        "config_reload_claims",
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
        "pending_runtime_schedule_triggers",
        "plugin_dirs",
        "project_env",
        "project_config_revisions",
        "project_worktrees",
        "permission_audit_events",
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
        "render_attachment_batches",
        "render_attachment_checkpoints",
        "render_batch_intents",
        "render_parent_diagnostics",
        "runtime_incidents",
        "runtime_agent_manifest",
        "runtime_agent_transitions",
        "runtime_command_invocations",
        "runtime_command_manifest",
        "runtime_command_refreshes",
        "runtime_remote_transitions",
        "runtime_schedule_actions",
        "runtime_schedules",
        "runtime_task_actions",
        "schedule_run_attempts",
        "schedule_runs",
        "schedules",
        "scheduler_events",
        "scheduler_render_intents",
        "scheduler_state",
        "schema_migrations",
        "service_admission_fences",
        "service_restart_intents",
        "session_bindings",
        "session_creation_intents",
        "session_error_projections",
        "session_limit_projections",
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
        "compaction_runs",
        "ephemeral_queries",
        "fleet_runs",
        "task_card_projections",
        "taskdeck_panel_state",
        "pinned_message_provenance",
        "tool_output_streams",
        "tool_spill_artifacts",
        "trusted_local_artifacts",
        "trusted_local_artifact_snapshots",
        "usage_samples",
        "restart_intents",
        "worktree_events",
        "worktree_intents",
        "worktree_process_state",
        "worktree_recovery_runs",
        "agent_loop_projections",
        "context_projections",
        "usage_projections",
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
        agent_columns = await database.fetchall("PRAGMA table_info(agent_loop_projections)")
        binding_columns = await database.fetchall("PRAGMA table_info(session_bindings)")

    assert {row["name"] for row in tables} == expected_tables
    assert dict(migration) == {"version": 1, "name": "0001_initial.sql"}
    assert "payload_revision" in {row["name"] for row in outbox_columns}
    assert {"retention_until", "delivery_confirmed_at"} <= {row["name"] for row in spill_columns}
    assert foreign_keys[0] == 1
    assert journal_mode[0] == "wal"
    assert "source_event_id" in {row["name"] for row in agent_columns}
    assert {
        "delete_cleanup_state",
        "delete_cleanup_error",
        "deleted_at",
    } <= {row["name"] for row in binding_columns}


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
        worktree_columns = await database.fetchall("PRAGMA table_info(worktree_intents)")
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
        "git_create_holder",
        "git_create_fence_token",
        "git_create_lease_expires_at",
        "git_create_process_generation",
        "git_create_retry_at",
    } <= {row["name"] for row in worktree_columns}
    assert {
        "compaction_runs",
        "execution_health",
        "runtime_command_manifest",
        "runtime_remote_transitions",
        "runtime_task_actions",
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
async def test_v20_backfills_existing_enabled_schedule_planning_fields(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "upgrade-v9-schedule.sqlite3"
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
    connection.execute(
        """
        INSERT INTO schedules(
            id, kind, expression, timezone, payload, target_snapshot,
            misfire_policy, state, created_at, updated_at
        ) VALUES ('legacy-schedule', 'new_session', 'cron:0 9 * * *',
                  'UTC', '{}', '{}', 'latest', 'enabled', 10, 20)
        """
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        schedule = await database.fetchone(
            """
            SELECT normalized_expression, next_run_at_utc
            FROM schedules WHERE id = 'legacy-schedule'
            """
        )

    assert dict(schedule) == {
        "normalized_expression": "0 9 * * *",
        "next_run_at_utc": 20.0,
    }


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
async def test_v24_backfills_unambiguous_legacy_run_link_before_repair(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "upgrade-v23-unambiguous.sqlite3"
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
        ) VALUES ('thread-1', 'implicit-home', '/legacy', 'session-1', 1, 1)
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
        ) VALUES ('legacy-submission', 'session-1', 'app_schedule',
                  'run-1', 'local_queued', 1)
        """
    )
    connection.execute(
        """
        INSERT INTO message_queue(
            id, thread_id, schedule_run_id, prompt,
            requested_mode_snapshot, requested_model_config_snapshot,
            requested_session_config_version, position, state, created_at, updated_at
        ) VALUES ('legacy-submission', 'thread-1', 'run-1', 'legacy',
                  'interactive', '{}', 1, 1, 'local_queued', 1, 1)
        """
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        run = await database.fetchone(
            """
            SELECT result_submission_id FROM schedule_runs
            WHERE run_id = 'run-1'
            """
        )
        queue = await database.fetchone(
            """
            SELECT schedule_run_id FROM message_queue
            WHERE id = 'legacy-submission'
            """
        )
        submission = await database.fetchone(
            """
            SELECT schedule_run_id FROM submissions
            WHERE submission_id = 'legacy-submission'
            """
        )

    assert run["result_submission_id"] == "legacy-submission"
    assert queue["schedule_run_id"] == "run-1"
    assert submission["schedule_run_id"] == "run-1"


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
async def test_protocol_compatibility_upgrades_exact_6d00930_schema(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "upgrade-6d00930.sqlite3"
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
    original_versions = {*range(1, 10), *range(15, 20)}
    for migration in sorted(
        item
        for item in migration_root.iterdir()
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) in original_versions
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        version = int(migration.name.partition("_")[0])
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, ?)",
            (version, migration.name, time.time()),
        )
    pre_tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    pre_agent_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(agent_loop_projections)")
    }
    connection.commit()
    connection.close()

    assert "config_reload_claims" not in pre_tables
    assert "source_event_id" not in pre_agent_columns

    async with Database(database_path) as database:
        versions = await database.fetchall(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        )
        tables = {
            row["name"]
            for row in await database.fetchall(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        agent_columns = {
            row["name"]
            for row in await database.fetchall("PRAGMA table_info(agent_loop_projections)")
        }

    assert [row["version"] for row in versions] == EXPECTED_MIGRATION_VERSIONS
    assert {row["version"]: row["name"] for row in versions}[29] == (
        "0029_protocol_compatibility.sql"
    )
    assert "config_reload_claims" in tables
    assert "source_event_id" in agent_columns


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
