import asyncio
import hashlib
import shutil
import sqlite3
import time
from importlib import resources
from pathlib import Path
from typing import Any

import pytest

from copilotd.render.outbox import RenderOutboxDispatcher
from copilotd.storage.database import CommittedCancellation, Database

EXPECTED_MIGRATION_VERSIONS = [*range(1, 38), *range(40, 54)]


class _MigrationReactionTransport:
    def __init__(self) -> None:
        self.reactions: list[dict[str, Any]] = []

    async def send(self, **_kwargs: Any) -> str:
        raise AssertionError("legacy reactions must not use message delivery")

    async def edit(self, **_kwargs: Any) -> None:
        raise AssertionError("legacy reactions must not use message delivery")

    async def reaction(self, *, payload: dict[str, Any], **_kwargs: Any) -> None:
        self.reactions.append(payload)


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


def _create_legacy_discord_v50_fixture(path: Path) -> None:
    _create_migration_fixture(path, through_version=47)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE tool_activity_projections (
            sdk_session_id TEXT NOT NULL,
            tool_call_id TEXT NOT NULL,
            submission_id TEXT,
            activity_key TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            state TEXT NOT NULL,
            error_summary TEXT,
            first_seen_at REAL NOT NULL,
            last_seen_at REAL NOT NULL,
            last_event_id TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (sdk_session_id, tool_call_id)
        );
        CREATE INDEX idx_tool_activity_submission_state
        ON tool_activity_projections(
            sdk_session_id, submission_id, state, last_seen_at
        );
        CREATE INDEX idx_tool_activity_key_state
        ON tool_activity_projections(
            sdk_session_id, activity_key, state, last_seen_at
        );
        CREATE INDEX idx_event_journal_tool_call
        ON event_journal(sdk_session_id, tool_call_id, journal_id)
        WHERE tool_call_id IS NOT NULL;
        CREATE INDEX idx_render_outbox_session_sequence
        ON render_outbox(session_id, logical_seq);

        ALTER TABLE message_queue ADD COLUMN discord_feedback_reaction TEXT;
        ALTER TABLE message_queue ADD COLUMN discord_feedback_status TEXT;
        ALTER TABLE message_queue
            ADD COLUMN discord_feedback_attempts INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE message_queue
            ADD COLUMN discord_feedback_next_attempt_at REAL NOT NULL DEFAULT 0;
        ALTER TABLE message_queue ADD COLUMN discord_feedback_updated_at REAL;
        ALTER TABLE message_queue ADD COLUMN discord_channel_id TEXT;
        ALTER TABLE render_outbox ADD COLUMN source_received_at REAL;

        CREATE TABLE discord_admission_feedback (
            feedback_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            desired_reaction TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(channel_id, message_id)
        );
        CREATE INDEX message_queue_discord_feedback_idx
        ON message_queue(
            discord_feedback_status, discord_feedback_next_attempt_at
        )
        WHERE discord_message_id IS NOT NULL;
        CREATE INDEX render_outbox_feedback_window_idx
        ON render_outbox(session_id, state, source_received_at, created_at);
        CREATE INDEX discord_admission_feedback_ready_idx
        ON discord_admission_feedback(status, next_attempt_at);
        """
    )
    connection.executemany(
        "INSERT INTO schema_migrations VALUES (?, ?, ?)",
        [
            (48, "0048_tool_activity_projections.sql", 48),
            (49, "0049_quiet_discord_surfaces.sql", 49),
            (50, "0050_discord_reaction_feedback.sql", 50),
        ],
    )
    connection.execute(
        """
        INSERT INTO tool_activity_projections(
            sdk_session_id, tool_call_id, submission_id, activity_key,
            tool_name, state, first_seen_at, last_seen_at, last_event_id
        ) VALUES (
            'legacy-session', 'legacy-tool', NULL, 'activity:current',
            'legacy-shell', 'completed', 1, 2, 'legacy-complete'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO discord_admission_feedback(
            feedback_id, channel_id, message_id, desired_reaction,
            status, created_at, updated_at
        ) VALUES ('feedback-1', 'channel-1', 'message-1', '✅', 'sent', 1, 2)
        """
    )
    connection.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            created_at, updated_at
        ) VALUES (
            'legacy-feedback-thread', 'home', '/workspace',
            'legacy-feedback-session', 1, 1
        )
        """
    )
    connection.executemany(
        """
        INSERT INTO message_queue(
            id, thread_id, discord_message_id, prompt,
            requested_mode_snapshot, requested_model_config_snapshot,
            requested_session_config_version, position, state,
            created_at, updated_at, discord_feedback_reaction,
            discord_feedback_status, discord_feedback_attempts,
            discord_feedback_next_attempt_at, discord_feedback_updated_at,
            discord_channel_id
        ) VALUES (?, 'legacy-feedback-thread', ?, 'legacy', 'interactive', '{}', 1, ?,
                  'local_queued', 1, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ("queue-pending", "message-q1", 1, 11, "👀", "pending", 2, 12, 11, "channel-q"),
            ("queue-retry", "message-q2", 2, 21, "❌", "retry", 3, 22, 21, "channel-q"),
            (
                "queue-unavailable",
                "message-q3",
                3,
                31,
                "❓",
                "unavailable",
                4,
                32,
                31,
                "channel-q",
            ),
        ),
    )
    connection.executemany(
        """
        INSERT INTO discord_admission_feedback(
            feedback_id, channel_id, message_id, desired_reaction,
            status, attempts, next_attempt_at, created_at, updated_at
        ) VALUES (?, 'channel-a', ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            ("feedback-pending", "message-a1", "🧠", "pending", 5, 42, 41),
            ("feedback-retry", "message-a2", "🛠️", "retry", 6, 52, 51),
            ("feedback-unavailable", "message-a3", "❓", "unavailable", 7, 62, 61),
            ("feedback-applied", "message-a4", "✅", "applied", 1, 0, 71),
        ),
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
        "state_only_cleanup",
        "state_only_cleanup_artifacts",
        "snapshot_observations",
        "submissions",
        "submission_segments",
        "submission_reactions",
        "submission_task_links",
        "compaction_runs",
        "ephemeral_queries",
        "fleet_runs",
        "pinned_message_provenance",
        "tool_render_state",
        "turn_render_state",
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
        tool_render_columns = await database.fetchall("PRAGMA table_info(tool_render_state)")
        foreign_keys = await database.fetchone("PRAGMA foreign_keys")
        journal_mode = await database.fetchone("PRAGMA journal_mode")
        agent_columns = await database.fetchall("PRAGMA table_info(agent_loop_projections)")
        binding_columns = await database.fetchall("PRAGMA table_info(session_bindings)")

    assert {row["name"] for row in tables} == expected_tables
    assert dict(migration) == {"version": 1, "name": "0001_initial.sql"}
    assert "payload_revision" in {row["name"] for row in outbox_columns}
    assert "last_error" in {row["name"] for row in outbox_columns}
    assert {
        "turn_key",
        "submission_id",
        "segment_index",
        "tool_call_id",
        "state",
        "started_seq",
        "updated_seq",
    } <= {row["name"] for row in tool_render_columns}
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
async def test_state_only_migration_purges_filename_paths_and_preserves_refetch_source(
    tmp_path: Path,
) -> None:
    sentinel = "LEGACY-PRIVATE-FILENAME-4d7a.txt"
    database_path = tmp_path / "legacy-attachment.sqlite3"
    _create_migration_fixture(database_path, through_version=51)
    managed_path = (
        tmp_path
        / "sessions"
        / "legacy-session"
        / "attachments"
        / "legacy-manifest"
        / f"000-{sentinel}"
    )
    managed_path.parent.mkdir(parents=True)
    managed_path.write_bytes(b"legacy attachment")
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        INSERT INTO attachment_manifests(
            id, source_kind, source_id, session_id, state, total_bytes,
            created_at, source_channel_id, source_message_id, recovery_prompt,
            recovery_idempotency_key, recovery_origin, updated_at
        ) VALUES (
            'legacy-manifest', 'discord-message', 'legacy-source',
            'legacy-session', 'ready', 17, 1, 'channel-1', 'message-1',
            'legacy prompt', 'discord-message:message-1', 'discord_message', 1
        )
        """
    )
    connection.execute(
        """
        INSERT INTO attachment_items(
            manifest_id, item_index, discord_attachment_id, original_name,
            mime_type, byte_size, sha256, local_path, sdk_attachment_kind, state
        ) VALUES (
            'legacy-manifest', 0, '1', ?, 'text/plain', 17, ?,
            ?, 'file', 'ready'
        )
        """,
        (
            sentinel,
            hashlib.sha256(b"legacy attachment").hexdigest(),
            str(managed_path),
        ),
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        manifest = await database.fetchone(
            """
            SELECT state, total_bytes, source_channel_id, source_message_id,
                   recovery_idempotency_key, recovery_origin, error_code
            FROM attachment_manifests WHERE id = 'legacy-manifest'
            """
        )
        item_count = await database.fetchone(
            "SELECT COUNT(*) FROM attachment_items WHERE manifest_id = 'legacy-manifest'"
        )

    assert dict(manifest) == {
        "state": "preparing",
        "total_bytes": 0,
        "source_channel_id": "channel-1",
        "source_message_id": "message-1",
        "recovery_idempotency_key": "discord-message:message-1",
        "recovery_origin": "discord_message",
        "error_code": "source_refetch_required",
    }
    assert item_count[0] == 0
    assert not managed_path.exists()
    assert sentinel.encode() not in database_path.read_bytes()


@pytest.mark.asyncio
async def test_discord_render_families_upgrade_v50_without_losing_outbox(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "upgrade-v50.sqlite3"
    _create_migration_fixture(database_path, through_version=50)
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        INSERT INTO render_outbox(
            id, session_id, logical_seq, lane, coalesce_key,
            idempotency_key, payload, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (
            'render-before-v51', 'session-1', 1, 'assistant_stream',
            'assistant:message-1', 'render-before-v51', '{"content":"preserved"}',
            'pending', 0, 0, 0, 0
        )
        """
    )
    connection.commit()
    connection.close()

    async with Database(database_path) as database:
        outbox = await database.fetchone(
            """
            SELECT payload, state, last_error FROM render_outbox
            WHERE id = 'render-before-v51'
            """
        )
        table = await database.fetchone(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'tool_render_state'
            """
        )
        migration = await database.fetchone("SELECT name FROM schema_migrations WHERE version = 51")

    assert dict(outbox) == {
        "payload": '{"content_state":"unavailable","schema":1}',
        "state": "content_unavailable",
        "last_error": "content_unavailable",
    }
    assert table["name"] == "tool_render_state"
    assert migration["name"] == "0051_discord_render_families.sql"


@pytest.mark.asyncio
async def test_legacy_discord_48_49_50_collision_normalizes_through_51(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "legacy-discord-v50.sqlite3"
    _create_legacy_discord_v50_fixture(database_path)

    async with Database(database_path) as database:
        migrations = await database.fetchall(
            """
            SELECT version, name FROM schema_migrations
            WHERE version BETWEEN 48 AND 53 ORDER BY version
            """
        )
        tables = {
            str(row["name"])
            for row in await database.fetchall(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        submission_columns = {
            str(row["name"]) for row in await database.fetchall("PRAGMA table_info(submissions)")
        }
        queue_columns = {
            str(row["name"]) for row in await database.fetchall("PRAGMA table_info(message_queue)")
        }
        outbox_columns = {
            str(row["name"]) for row in await database.fetchall("PRAGMA table_info(render_outbox)")
        }
        legacy_feedback = await database.fetchone(
            """
            SELECT desired_reaction, status FROM discord_admission_feedback
            WHERE feedback_id = 'feedback-1'
            """
        )
        migrated_reactions = await database.fetchall(
            """
            SELECT session_id, source_message_id, payload, attempts,
                   next_attempt_at, state
            FROM render_outbox
            WHERE lane = 'admission_reaction'
            ORDER BY source_message_id
            """
        )
        queue_feedback = await database.fetchall(
            """
            SELECT id, discord_feedback_status
            FROM message_queue ORDER BY position
            """
        )
        admission_feedback = await database.fetchall(
            """
            SELECT feedback_id, status
            FROM discord_admission_feedback ORDER BY feedback_id
            """
        )
        transport = _MigrationReactionTransport()
        assert (
            await RenderOutboxDispatcher(database, transport).dispatch_once(
                now=100,
                limit=10,
            )
            == 4
        )
        delivered_reactions = await database.fetchall(
            """
            SELECT state FROM render_outbox
            WHERE lane = 'admission_reaction' ORDER BY id
            """
        )

    assert [tuple(row) for row in migrations] == [
        (48, "0048_discord_reaction_delivery.sql"),
        (49, "0049_single_turn_card.sql"),
        (50, "0050_message_queue_discord_channel.sql"),
        (51, "0051_discord_render_families.sql"),
        (52, "0052_state_only_storage.sql"),
        (53, "0053_schedule_thread_name.sql"),
    ]
    assert {
        "submission_reactions",
        "turn_render_state",
        "tool_render_state",
        "discord_admission_feedback",
    } <= tables
    assert {
        "discord_source_channel_id",
        "discord_source_message_id",
    } <= submission_columns
    assert {
        "discord_channel_id",
        "discord_feedback_reaction",
        "discord_feedback_status",
    } <= queue_columns
    assert {"source_received_at", "last_error"} <= outbox_columns
    assert "tool_activity_projections" not in tables
    assert dict(legacy_feedback) == {"desired_reaction": "✅", "status": "sent"}
    assert [
        (
            row["session_id"],
            row["source_message_id"],
            row["attempts"],
            row["next_attempt_at"],
            row["state"],
        )
        for row in migrated_reactions
    ] == [
        ("admission:channel-a:message-a1", "message-a1", 5, 42, "pending"),
        ("admission:channel-a:message-a2", "message-a2", 6, 52, "pending"),
        ("legacy-feedback-session", "message-q1", 2, 12, "pending"),
        ("legacy-feedback-session", "message-q2", 3, 22, "pending"),
    ]
    assert [item["emoji"] for item in transport.reactions] == ["🧠", "🛠️", "👀", "❌"]
    assert [tuple(row) for row in queue_feedback] == [
        ("queue-pending", "migrated"),
        ("queue-retry", "migrated"),
        ("queue-unavailable", "unavailable"),
    ]
    assert [tuple(row) for row in admission_feedback] == [
        ("feedback-1", "sent"),
        ("feedback-applied", "applied"),
        ("feedback-pending", "migrated"),
        ("feedback-retry", "migrated"),
        ("feedback-unavailable", "unavailable"),
    ]
    assert {payload["emoji"] for payload in transport.reactions} == {"👀", "❌", "🛠️", "🧠"}
    assert [row["state"] for row in delivered_reactions] == ["sent"] * 4


@pytest.mark.asyncio
async def test_arbitrary_migration_collision_is_rejected(tmp_path: Path) -> None:
    database_path = tmp_path / "unknown-collision.sqlite3"
    _create_migration_fixture(database_path, through_version=47)
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        INSERT INTO schema_migrations(version, name, applied_at)
        VALUES (48, '0048_unrelated_local_schema.sql', 48)
        """
    )
    connection.commit()
    connection.close()

    database = Database(database_path)
    with pytest.raises(
        RuntimeError,
        match=r"migration version 48 is already recorded as 0048_unrelated_local_schema\.sql",
    ):
        await database.open()
    await database.close()


@pytest.mark.asyncio
async def test_legacy_migration_identity_requires_matching_schema(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy-collision-missing-schema.sqlite3"
    _create_migration_fixture(database_path, through_version=47)
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        INSERT INTO schema_migrations(version, name, applied_at)
        VALUES (48, '0048_tool_activity_projections.sql', 48)
        """
    )
    connection.commit()
    connection.close()

    database = Database(database_path)
    with pytest.raises(
        RuntimeError,
        match=r"0048_tool_activity_projections\.sql.*missing table",
    ):
        await database.open()
    await database.close()


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
        tables = await database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")

    assert [row["version"] for row in versions] == EXPECTED_MIGRATION_VERSIONS
    assert "render_streams" not in {row["name"] for row in tables}
    assert {
        "execution_health",
        "snapshot_observations",
        "submission_task_links",
        "render_attachment_batches",
        "session_ui_metadata",
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
    } <= {row["name"] for row in tables}
    assert "render_streams" not in {row["name"] for row in tables}


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
            "response_state": "content_unavailable",
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
async def test_background_liveness_kind_migrates_from_release_0046(tmp_path: Path) -> None:
    database_path = tmp_path / "upgrade-background-kind.sqlite3"
    async with Database(database_path) as database:
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES (
                'session-background', 'background:task:1', 'background',
                'task:1', 1, 1, 'active', 1, 1
            )
            """
        )
        await database.execute("DELETE FROM schema_migrations WHERE version = 47")

    async with Database(database_path) as database:
        lease = await database.fetchone(
            """
            SELECT kind FROM liveness_leases
            WHERE sdk_session_id = 'session-background'
            """
        )
        versions = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")

    assert lease["kind"] == "observed_background"
    assert [row["version"] for row in versions] == EXPECTED_MIGRATION_VERSIONS


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


@pytest.mark.asyncio
async def test_cancellation_during_commit_reports_committed_coupled_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commit_started = asyncio.Event()
    allow_commit = asyncio.Event()
    async with Database(tmp_path / "cancel-commit.sqlite3") as database:
        await database.execute("CREATE TABLE coupled(ref TEXT PRIMARY KEY)")
        real_commit = database.connection.commit

        async def delayed_commit() -> None:
            commit_started.set()
            await allow_commit.wait()
            await real_commit()

        monkeypatch.setattr(database.connection, "commit", delayed_commit)

        async def coupled_write() -> None:
            with database.content_store.transaction():
                async with database.transaction() as connection:
                    reference = database.content_store.put("live", key="vc:coupled")
                    await connection.execute(
                        "INSERT INTO coupled(ref) VALUES (?)",
                        (reference.key,),
                    )

        write = asyncio.create_task(coupled_write())
        await commit_started.wait()
        write.cancel()
        await asyncio.sleep(0)
        assert not write.done()
        allow_commit.set()
        with pytest.raises(CommittedCancellation):
            await write

        row = await database.fetchone("SELECT ref FROM coupled")
        assert row["ref"] == "vc:coupled"
        assert database.content_store.require("vc:coupled") == "live"


@pytest.mark.asyncio
async def test_cancellation_waits_for_rollback_and_restores_coupled_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body_started = asyncio.Event()
    rollback_started = asyncio.Event()
    allow_rollback = asyncio.Event()
    never = asyncio.Event()
    async with Database(tmp_path / "cancel-rollback.sqlite3") as database:
        await database.execute("CREATE TABLE coupled(ref TEXT PRIMARY KEY)")
        real_rollback = database.connection.rollback

        async def delayed_rollback() -> None:
            rollback_started.set()
            await allow_rollback.wait()
            await real_rollback()

        monkeypatch.setattr(database.connection, "rollback", delayed_rollback)

        async def coupled_write() -> None:
            with database.content_store.transaction():
                async with database.transaction() as connection:
                    reference = database.content_store.put("live", key="vc:coupled")
                    await connection.execute(
                        "INSERT INTO coupled(ref) VALUES (?)",
                        (reference.key,),
                    )
                    body_started.set()
                    await never.wait()

        write = asyncio.create_task(coupled_write())
        await body_started.wait()
        write.cancel()
        await rollback_started.wait()
        await asyncio.sleep(0)
        assert not write.done()
        allow_rollback.set()
        with pytest.raises(asyncio.CancelledError):
            await write

        row = await database.fetchone("SELECT ref FROM coupled")
        assert row is None
        assert database.content_store.get("vc:coupled") is None


@pytest.mark.asyncio
async def test_commit_failure_rolls_back_coupled_volatile_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Database(tmp_path / "failed-commit.sqlite3") as database:
        await database.execute("CREATE TABLE coupled(ref TEXT PRIMARY KEY)")

        async def failed_commit() -> None:
            raise sqlite3.OperationalError("injected commit failure")

        monkeypatch.setattr(database.connection, "commit", failed_commit)

        with pytest.raises(sqlite3.OperationalError, match="injected commit failure"):
            with database.content_store.transaction():
                async with database.transaction() as connection:
                    reference = database.content_store.put("live", key="vc:coupled")
                    await connection.execute(
                        "INSERT INTO coupled(ref) VALUES (?)",
                        (reference.key,),
                    )

        row = await database.fetchone("SELECT ref FROM coupled")
        assert row is None
        assert database.content_store.get("vc:coupled") is None


@pytest.mark.asyncio
async def test_secure_maintenance_runs_only_for_new_migration_or_explicit_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Path] = []
    original = Database.secure_maintenance

    async def tracked(database: Database) -> None:
        calls.append(database.path)
        await original(database)

    monkeypatch.setattr(Database, "secure_maintenance", tracked)
    path = tmp_path / "automatic-secure-maintenance.sqlite3"

    database = Database(path)
    await database.open()
    await database.close()
    assert calls == [path]

    database = Database(path)
    await database.open()
    assert calls == [path]
    await database.open(secure_erase=True)
    assert calls == [path, path]
    await database.close()


@pytest.mark.asyncio
async def test_automatic_secure_maintenance_failure_aborts_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "secure-maintenance-failure.sqlite3")

    async def fail() -> None:
        raise OSError("checkpoint failed")

    monkeypatch.setattr(database, "secure_maintenance", fail)

    with pytest.raises(
        RuntimeError,
        match="secure erase failed after state-only migration",
    ):
        await database.open()
    with pytest.raises(RuntimeError, match="database is not open"):
        _ = database.connection
