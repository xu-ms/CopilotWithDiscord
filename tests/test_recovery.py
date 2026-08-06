import json
from pathlib import Path

import pytest

from copilotd.core.recovery import StartupRecoveryInventory
from copilotd.core.scheduler import ScheduleKind, SchedulerRepository
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


@pytest.mark.asyncio
async def test_startup_inventory_settles_only_expired_owner_work_before_ready(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "startup-recovery.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 1, 1, 1)
            """
        )
        await OwnerLeaseStore(database, ttl_seconds=60).acquire(
            "session-1",
            "dead-owner",
            now=100,
        )
        await database.execute(
            """
            INSERT INTO session_operations(
                operation_id, sdk_session_id, runtime_generation,
                owner_fence_token, kind, idempotency_key, input_hash,
                state, created_at
            ) VALUES ('operation-1', 'session-1', 1, 1, 'send',
                      'send-1', 'hash', 'started', 101)
            """
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'observed_active', 101)
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'submission:1', 'submission',
                      'submission-1', 1, 1, 'active', 101, 101)
            """
        )
        await database.execute(
            """
            INSERT INTO session_creation_intents(
                creation_token, source_kind, source_id, project_source,
                cwd_snapshot, sdk_session_id, state, created_at, updated_at
            ) VALUES ('token-1', 'message', 'message-1', 'implicit-home',
                      '/tmp', 'new-session', 'creating', 101, 101)
            """
        )

        report = await StartupRecoveryInventory(database).run(now=200)
        binding = await database.fetchone(
            "SELECT attachment_state, permission_posture FROM session_bindings"
        )
        operation = await database.fetchone(
            "SELECT state, error_code FROM session_operations"
        )
        submission = await database.fetchone("SELECT state FROM submissions")
        liveness = await database.fetchone("SELECT state FROM liveness_leases")
        creation = await database.fetchone(
            "SELECT state FROM session_creation_intents"
        )
        run = await database.fetchone(
            "SELECT status, detail FROM startup_recovery_runs WHERE run_id = ?",
            (report.run_id,),
        )

    assert report.expired_owner_sessions == 1
    assert report.unknown_operations == 1
    assert report.unknown_submissions == 1
    assert report.orphaned_liveness == 1
    assert report.unknown_creation_intents == 1
    assert dict(binding) == {
        "attachment_state": "recovery_unknown",
        "permission_posture": "unknown",
    }
    assert dict(operation) == {
        "state": "unknown",
        "error_code": "startup_recovery",
    }
    assert submission["state"] == "outcome_unknown"
    assert liveness["state"] == "orphaned"
    assert creation["state"] == "unknown"
    assert run["status"] == "completed"
    assert json.loads(run["detail"])["run_id"] == report.run_id


@pytest.mark.asyncio
async def test_startup_recovery_waits_for_new_session_run_owner_lease(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "startup-schedule-lease.sqlite3") as database:
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "recover"},
            target_snapshot={},
            now=0,
        )
        scheduled_run = await repository.run_now(
            definition.id,
            now=1,
            manual_id="crash",
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', target_started_at = 2,
                session_create_started_at = 2,
                result_session_id = 'scheduled-session',
                lease_owner = 'old-worker', lease_expires_at = 300,
                attempt = 1, fence_token = 1
            WHERE run_id = ?
            """,
            (scheduled_run.run_id,),
        )
        await database.execute(
            """
            INSERT INTO session_creation_intents(
                creation_token, source_kind, source_id, project_source,
                cwd_snapshot, sdk_session_id, state, created_at, updated_at
            ) VALUES ('scheduled-token', 'schedule', ?, 'implicit-home',
                      ?, 'scheduled-session', 'creating', 2, 2)
            """,
            (scheduled_run.run_id, str(tmp_path)),
        )

        live_report = await StartupRecoveryInventory(database).run(now=200)
        live_run = await repository.get_run(scheduled_run.run_id)
        live_intent = await database.fetchone(
            """
            SELECT state FROM session_creation_intents
            WHERE creation_token = 'scheduled-token'
            """
        )
        stale_report = await StartupRecoveryInventory(database).run(now=301)
        stale_run = await repository.get_run(scheduled_run.run_id)
        stale_intent = await database.fetchone(
            """
            SELECT state FROM session_creation_intents
            WHERE creation_token = 'scheduled-token'
            """
        )

    assert live_report.target_unknown_runs == 0
    assert live_report.unknown_creation_intents == 0
    assert live_run.status.value == "submitting"
    assert live_intent["state"] == "creating"
    assert stale_report.target_unknown_runs == 1
    assert stale_report.unknown_creation_intents == 1
    assert stale_run.status.value == "retry_wait"
    assert stale_intent["state"] == "unknown"


@pytest.mark.asyncio
async def test_startup_recovery_preserves_acceptance_evidence_for_backfill(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "startup-acceptance.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 1, 1, 1)
            """
        )
        await OwnerLeaseStore(database, ttl_seconds=60).acquire(
            "session-1",
            "dead-owner",
            now=100,
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state,
                accepted_message_id, accepted_at, created_at
            ) VALUES (
                'submission-accepted', 'session-1', 'app_message',
                'submitted', '7e543dd8-4483-4c1a-ab7f-bf99dbad6d4c',
                105, 101
            )
            """
        )

        report = await StartupRecoveryInventory(database).run(now=200)
        submission = await database.fetchone(
            """
            SELECT state, accepted_message_id, accepted_at
            FROM submissions WHERE submission_id = 'submission-accepted'
            """
        )

    assert report.unknown_submissions == 1
    assert dict(submission) == {
        "state": "submitted_unknown",
        "accepted_message_id": "7e543dd8-4483-4c1a-ab7f-bf99dbad6d4c",
        "accepted_at": 105,
    }
