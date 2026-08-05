from pathlib import Path

import pytest

from copilotd.core.supervisor import ExecutionStallMonitor
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_stall_monitor_marks_only_active_execution_suspect_and_never_guesses(
    tmp_path: Path,
) -> None:
    pings = 0

    async def ping() -> dict[str, object]:
        nonlocal pings
        pings += 1
        return {"status": "ok", "protocol_version": 3}

    async with Database(tmp_path / "stall.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_remote_mode, last_event_at,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 'on', 100, 100, 100)
            """
        )
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, next_run_at, state, updated_at
            ) VALUES ('session-1', 'future-schedule', 'after',
                      'tomorrow', 10000, 'active', 100)
            """
        )
        monitor = ExecutionStallMonitor(
            database,
            ping,
            stall_seconds=600,
            interval_seconds=60,
        )

        assert await monitor.check(now=1000) == []
        assert pings == 0

        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'observed_active', 100)
            """
        )
        suspects = await monitor.check(now=1000)
        health = await database.fetchone(
            "SELECT state, suspect_since, last_ping_at FROM execution_health"
        )
        incident = await database.fetchone(
            "SELECT kind FROM runtime_incidents WHERE session_id = 'session-1'"
        )
        binding = await database.fetchone(
            "SELECT attachment_state FROM session_bindings"
        )
        submission = await database.fetchone("SELECT state FROM submissions")

        assert len(suspects) == 1
        assert suspects[0].silent_seconds == 900
        assert pings == 1
        assert dict(health) == {
            "state": "suspect",
            "suspect_since": 1000,
            "last_ping_at": 1000,
        }
        assert incident["kind"] == "active_execution_stall_suspect"
        assert binding["attachment_state"] == "attached"
        assert submission["state"] == "observed_active"

        await database.execute(
            """
            UPDATE session_bindings
            SET last_event_at = 950, updated_at = 950
            WHERE sdk_session_id = 'session-1'
            """
        )
        assert await monitor.check(now=1000) == []
        health = await database.fetchone(
            "SELECT state, suspect_since FROM execution_health"
        )

    assert dict(health) == {"state": "healthy", "suspect_since": None}
    assert pings == 1


@pytest.mark.asyncio
async def test_stall_monitor_persists_transient_ping_failure_and_keeps_running(
    tmp_path: Path,
) -> None:
    calls = 0

    async def failing_ping() -> dict[str, object]:
        nonlocal calls
        calls += 1
        raise ConnectionError("runtime reconnecting")

    async with Database(tmp_path / "stall-ping-failure.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, last_event_at, created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 100, 100, 100)
            """
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'observed_active', 100)
            """
        )
        monitor = ExecutionStallMonitor(
            database,
            failing_ping,
            stall_seconds=600,
            interval_seconds=60,
        )

        first = await monitor.check(now=1000)
        second = await monitor.check(now=1061)
        health = await database.fetchone(
            "SELECT state, detail FROM execution_health WHERE sdk_session_id = 'session-1'"
        )

    assert calls == 2
    assert first[0].ping["status"] == "error"
    assert second[0].ping["status"] == "error"
    assert health["state"] == "suspect"
    assert "ConnectionError" in health["detail"]
