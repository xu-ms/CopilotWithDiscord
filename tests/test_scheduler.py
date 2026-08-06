import asyncio
from pathlib import Path

import pytest

from copilotd.core.scheduler import (
    DeterministicSchedulerAdapter,
    MisfirePolicy,
    ScheduleConflict,
    ScheduleDefinition,
    ScheduledTarget,
    ScheduleKind,
    SchedulerDispatchError,
    SchedulerErrorCategory,
    SchedulerNotRecovered,
    SchedulerRepository,
    ScheduleRun,
    ScheduleRunState,
    SchedulerWorker,
)
from copilotd.ops.heartbeat import HeartbeatWriter
from copilotd.storage.database import Database


class FakeClock:
    def __init__(self, now: float) -> None:
        self.value = now

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self.value += seconds


async def _insert_binding(
    database: Database,
    *,
    thread_id: str = "thread-1",
    session_id: str = "session-1",
    project_id: str | None = None,
) -> None:
    await database.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_id, project_source, cwd_snapshot, sdk_session_id,
            attachment_state, permission_posture, runtime_mode,
            runtime_agent, runtime_project_config_version,
            runtime_generation, owner_fence_token, created_at, updated_at
        ) VALUES (?, ?, 'explicit', '/tmp', ?, 'attached', 'verified_allow_all',
                  'interactive', 'default', 1, 1, 1, 0, 0)
        """,
        (thread_id, project_id, session_id),
    )
    await database.execute(
        """
        INSERT INTO session_owner_leases(
            sdk_session_id, owner_id, fence_token,
            acquired_at, renewed_at, expires_at
        ) VALUES (?, 'test-owner', 1, 0, 0, 100000)
        """,
        (session_id,),
    )


def _target(thread_id: str = "thread-1", session_id: str = "session-1") -> dict:
    return {
        "thread_id": thread_id,
        "sdk_session_id": session_id,
        "execution_config": {
            "mode": "interactive",
            "model_config": {},
            "agent": "default",
            "session_config_version": 1,
        },
    }


@pytest.mark.asyncio
async def test_concurrent_ticks_claim_one_run_and_enqueue_once(tmp_path: Path) -> None:
    async with Database(tmp_path / "duplicate-tick.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        clock = FakeClock(60)
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )
        first = SchedulerWorker(repository, adapter, owner_id="worker-a", clock=clock)
        second = SchedulerWorker(repository, adapter, owner_id="worker-b", clock=clock)

        await asyncio.gather(first.tick(), second.tick())
        await asyncio.gather(first.tick(), second.tick())

        runs = await repository.list_runs(definition.id)
        queue = await database.fetchall("SELECT * FROM message_queue")
        refreshed = await repository.require(definition.id)

    assert len(runs) == 1
    assert runs[0].status == ScheduleRunState.SUBMITTING
    assert runs[0].fence_token == 1
    assert len(queue) == 1
    assert queue[0]["schedule_run_id"] == runs[0].run_id
    assert refreshed.next_run_at_utc == 120
    assert refreshed.planner_fence_token == 1
    assert adapter.prepare_calls == [runs[0].run_id]


@pytest.mark.asyncio
async def test_worker_refuses_start_and_tick_before_recovery(tmp_path: Path) -> None:
    async with Database(tmp_path / "not-recovered.sqlite3") as database:
        repository = SchedulerRepository(database)
        worker = SchedulerWorker(
            repository,
            DeterministicSchedulerAdapter(),
            owner_id="worker",
            clock=FakeClock(0),
        )
        with pytest.raises(SchedulerNotRecovered):
            await worker.start()
        with pytest.raises(SchedulerNotRecovered):
            await worker.tick()


@pytest.mark.asyncio
async def test_sleep_wake_catches_up_only_latest_interval_and_keeps_session_attached(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "sleep-wake.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        clock = FakeClock(60)
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )
        worker = SchedulerWorker(repository, adapter, owner_id="worker", clock=clock)
        await worker.tick()
        clock.advance(29 * 60)
        await worker.tick()

        runs = list(reversed(await repository.list_runs(definition.id)))
        binding = await database.fetchone(
            "SELECT attachment_state FROM session_bindings WHERE thread_id = 'thread-1'"
        )

    assert [run.planned_at_utc for run in runs] == [60, 1_800]
    assert binding["attachment_state"] == "attached"


@pytest.mark.asyncio
async def test_backward_clock_jump_never_replans_same_instant(tmp_path: Path) -> None:
    async with Database(tmp_path / "clock-jump.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        clock = FakeClock(60)
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )
        worker = SchedulerWorker(repository, adapter, owner_id="worker", clock=clock)
        await worker.tick()
        clock.value = 30
        await worker.tick()
        clock.value = 60
        await worker.tick()
        runs = await repository.list_runs(definition.id)
        jumps = await database.fetchall(
            "SELECT event_type FROM scheduler_events WHERE event_type = 'clock_jump_backward'"
        )

    assert len(runs) == 1
    assert len(jumps) == 1


@pytest.mark.asyncio
async def test_skip_misfire_policy_advances_without_catchup(tmp_path: Path) -> None:
    async with Database(tmp_path / "misfire.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            misfire_policy=MisfirePolicy.SKIP,
            misfire_grace_seconds=5,
            now=0,
        )
        skipped = await repository.plan_due("planner", now=70)
        advanced = await repository.require(definition.id)
        due = await repository.plan_due("planner", now=120)
        current_definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "current"},
            target_snapshot={},
            misfire_policy=MisfirePolicy.SKIP,
            misfire_grace_seconds=5,
            now=0,
        )
        current = await repository.plan_due("planner", now=120)

    assert skipped == []
    assert advanced.next_run_at_utc == 120
    assert len(due) == 1
    assert due[0].planned_at_utc == 120
    current_run = next(run for run in current if run.schedule_id == current_definition.id)
    assert current_run.planned_at_utc == 120


@pytest.mark.asyncio
async def test_cron_planning_at_subminute_time_keeps_current_due_minute(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "subminute-cron.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:* * * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )

        planned = await repository.plan_due("planner", now=90)
        advanced = await repository.require(definition.id)

    assert [run.planned_at_utc for run in planned] == [60]
    assert advanced.next_run_at_utc == 120


@pytest.mark.asyncio
async def test_fractional_interval_planning_keeps_exact_float_boundary(
    tmp_path: Path,
) -> None:
    boundary = 3 * 1.2
    async with Database(tmp_path / "fractional-boundary.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="interval:1.2s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )

        planned = await repository.plan_due("planner", now=boundary)
        advanced = await repository.require(definition.id)

    assert [run.planned_at_utc for run in planned] == [boundary]
    assert advanced.next_run_at_utc == 4.8


@pytest.mark.asyncio
async def test_expired_run_claim_reassigns_monotonic_fence_without_new_attempt(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "run-fence.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )
        await repository.plan_due("planner", now=60)
        first = await repository.claim_next("worker-a", now=60, lease_seconds=10)
        second = await repository.claim_next("worker-b", now=71, lease_seconds=10)

    assert first is not None and second is not None
    assert first.run_id == second.run_id
    assert first.attempt == second.attempt == 1
    assert second.fence_token == first.fence_token + 1
    assert definition.id == first.schedule_id


@pytest.mark.asyncio
async def test_stale_dispatch_worker_cannot_terminalize_reclaimed_run(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "terminal-fence.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="interval:60s",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )
        await repository.plan_due("planner", now=60)
        first = await repository.claim_next("worker-a", now=60, lease_seconds=10)
        assert first is not None
        first = await repository.mark_target_started(
            first,
            "worker-a",
            new_session=True,
            now=60,
        )
        second = await repository.claim_next("worker-b", now=71, lease_seconds=10)
        assert second is not None
        failure = SchedulerDispatchError(
            "terminal failure",
            category=SchedulerErrorCategory.TARGET,
            code="terminal_failure",
        )

        with pytest.raises(ScheduleConflict, match="terminal finalization"):
            await repository.retry_or_fail(
                first,
                "worker-a",
                failure,
                now=72,
            )
        reclaimed = await repository.get_run(first.run_id)
        stale_render = await database.fetchone(
            "SELECT 1 FROM scheduler_render_intents WHERE run_id = ?",
            (first.run_id,),
        )
        final_state = await repository.retry_or_fail(
            second,
            "worker-b",
            failure,
            now=73,
        )

    assert reclaimed.status == ScheduleRunState.CLAIMED
    assert reclaimed.lease_owner == "worker-b"
    assert reclaimed.fence_token == second.fence_token
    assert stale_render is None
    assert final_state == ScheduleRunState.FAILED


@pytest.mark.asyncio
async def test_pre_dispatch_retry_uses_five_delays_then_fails_with_render_intent(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "retry.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        clock = FakeClock(60)
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )

        async def unavailable(_run: object) -> None:
            raise SchedulerDispatchError(
                "runtime unavailable before queue insertion",
                category=SchedulerErrorCategory.RUNTIME,
                code="runtime_unavailable",
                retryable=True,
            )

        adapter.on_prepare = unavailable
        worker = SchedulerWorker(repository, adapter, owner_id="worker", clock=clock)
        expected_retry = [65, 95, 215, 815, 2_615]
        for expected in expected_retry:
            await worker.tick()
            run = (await repository.list_runs(definition.id))[0]
            assert run.status == ScheduleRunState.RETRY_WAIT
            assert run.retry_at == expected
            clock.value = expected
        await worker.tick()
        run = (await repository.list_runs(definition.id))[0]
        render = await database.fetchone(
            "SELECT * FROM scheduler_render_intents WHERE run_id = ?",
            (run.run_id,),
        )

    assert run.attempt == 6
    assert run.status == ScheduleRunState.FAILED
    assert run.render_intent_id is not None
    assert render["terminal_status"] == "failed"


@pytest.mark.asyncio
async def test_confirmed_config_gate_failure_is_blocked_not_retried(tmp_path: Path) -> None:
    async with Database(tmp_path / "blocked.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        adapter = DeterministicSchedulerAdapter()

        async def blocked(_run: ScheduleRun) -> None:
            raise SchedulerDispatchError(
                "mode drift",
                category=SchedulerErrorCategory.CONFIG,
                code="mode_drift",
                blocked=True,
            )

        adapter.on_prepare = blocked
        worker = SchedulerWorker(
            repository,
            adapter,
            owner_id="worker",
            clock=FakeClock(60),
        )
        await worker.tick()
        run = (await repository.list_runs(definition.id))[0]

    assert run.status == ScheduleRunState.BLOCKED
    assert run.attempt == 1
    assert run.render_intent_id is not None


@pytest.mark.asyncio
async def test_semantic_completion_persists_final_render_before_terminal_projection(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "semantic.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )
        worker = SchedulerWorker(
            repository,
            adapter,
            owner_id="worker",
            clock=FakeClock(60),
        )
        await worker.tick()
        run = (await repository.list_runs(definition.id))[0]
        await database.execute(
            """
            UPDATE submissions
            SET state = 'submitted', accepted_message_id = 'accepted-1'
            WHERE submission_id = ?
            """,
            (run.result_submission_id,),
        )
        await repository.reconcile_submissions(now=60.5)
        accepted = await repository.get_run(run.run_id)
        await database.execute(
            "UPDATE submissions SET state = 'observed_active' WHERE submission_id = ?",
            (run.result_submission_id,),
        )
        await repository.reconcile_submissions(now=60.75)
        waiting = await repository.get_run(run.run_id)
        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', completion_basis = 'loop_idle',
                terminal_at = 61
            WHERE submission_id = ?
            """,
            (run.result_submission_id,),
        )
        await repository.reconcile_submissions(now=61)
        terminal = await repository.get_run(run.run_id)
        outbox = await database.fetchone(
            "SELECT state, payload FROM render_outbox WHERE id = ?",
            (terminal.render_intent_id,),
        )

    assert terminal.status == ScheduleRunState.SEMANTIC_COMPLETE
    assert accepted.status == ScheduleRunState.ACCEPTED
    assert waiting.status == ScheduleRunState.WAITING
    assert terminal.completion_basis == "loop_idle"
    assert outbox is not None and outbox["state"] == "pending"


@pytest.mark.asyncio
async def test_force_restart_marks_accepted_run_outcome_unknown(tmp_path: Path) -> None:
    async with Database(tmp_path / "restart.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )
        run = await repository.run_now(definition.id, now=10, manual_id="one")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                state, accepted_message_id, created_at
            ) VALUES ('submission-1', 'session-1', 'app_schedule', ?,
                      'submitted', 'accepted-1', 10)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'accepted', result_session_id = 'session-1',
                result_submission_id = 'submission-1', send_started_at = 15,
                accepted_message_id = 'accepted-1'
            WHERE run_id = ?
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'submission:submission-1', 'submission',
                      'submission-1', 1, 1, 'active', 10, 10)
            """
        )
        status = await repository.status(now=20)
        restart_id = await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=20,
        )
        recovered = await repository.get_run(run.run_id)
        restart = await database.fetchone(
            "SELECT affected_runs_json FROM restart_intents WHERE restart_id = ?",
            (restart_id,),
        )
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = 'submission-1'"
        )
        lease = await database.fetchone(
            """
            SELECT state FROM liveness_leases
            WHERE lease_id = 'submission:submission-1'
            """
        )

    assert recovered.status == ScheduleRunState.OUTCOME_UNKNOWN
    assert f"schedule_run:{run.run_id}:accepted" in status.restart_blockers
    assert "liveness:session-1:submission:submission-1" in status.restart_blockers
    assert recovered.render_intent_id is not None
    assert run.run_id in restart["affected_runs_json"]
    assert submission["state"] == "outcome_unknown"
    assert lease["state"] == "orphaned"


@pytest.mark.asyncio
async def test_restart_draining_fences_admission_and_recovery_reopens_it(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "restart-drain.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            channel_id="channel-1",
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="active")
        await database.execute(
            "UPDATE schedule_runs SET status = 'waiting' WHERE run_id = ?",
            (run.run_id,),
        )
        with pytest.raises(ScheduleConflict, match="restart blocked"):
            await repository.prepare_restart(
                requested_by="operator",
                force=False,
                now=2,
            )
        not_draining = await database.fetchone(
            "SELECT value FROM global_config WHERE key = 'restart_draining'"
        )
        await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=3,
        )
        with pytest.raises(ScheduleConflict, match="draining"):
            await repository.create(
                kind=ScheduleKind.NEW_SESSION,
                expression="cron:30 9 * * *",
                timezone="UTC",
                payload={"text": "late"},
                target_snapshot={},
                channel_id="channel-1",
                now=3,
            )
        await repository.recover(now=4)
        reopened = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:30 9 * * *",
            timezone="UTC",
            payload={"text": "after restart"},
            target_snapshot={},
            channel_id="channel-1",
            now=4,
        )

    assert not_draining["value"] == "0"
    assert reopened.state.value == "enabled"


@pytest.mark.asyncio
async def test_forced_restart_terminalizes_local_queue_with_schedule_run(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "restart-queue.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        adapter = DeterministicSchedulerAdapter(
            {definition.id: ScheduledTarget(None, "thread-1", "session-1")}
        )
        worker = SchedulerWorker(
            repository,
            adapter,
            owner_id="worker",
            clock=FakeClock(60),
        )
        await worker.tick()
        run = (await repository.list_runs(definition.id))[0]

        await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=61,
        )
        terminal = await repository.get_run(run.run_id)
        queue = await database.fetchone(
            "SELECT state FROM message_queue WHERE schedule_run_id = ?",
            (run.run_id,),
        )
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = ?",
            (run.result_submission_id,),
        )

    assert terminal.status == ScheduleRunState.DISPATCH_UNKNOWN
    assert queue["state"] == "cancelled"
    assert submission["state"] == "cancelled"


@pytest.mark.asyncio
async def test_forced_restart_uses_observed_user_event_as_dispatch_evidence(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "restart-observed.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "observed"},
            target_snapshot={},
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="observed")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                state, observed_user_event_id, created_at
            ) VALUES ('observed-restart', 'session-1', 'app_schedule', ?,
                      'observed_active', 'user-event-1', 1)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', result_submission_id = 'observed-restart'
            WHERE run_id = ?
            """,
            (run.run_id,),
        )

        await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=2,
        )
        terminal = await repository.get_run(run.run_id)
        submission = await database.fetchone(
            """
            SELECT state FROM submissions
            WHERE submission_id = 'observed-restart'
            """
        )

    assert terminal.status == ScheduleRunState.OUTCOME_UNKNOWN
    assert submission["state"] == "outcome_unknown"


@pytest.mark.asyncio
async def test_forced_restart_rechecks_acceptance_inside_finalization_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Database(tmp_path / "restart-evidence-race.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "race"},
            target_snapshot={},
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="race")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                state, created_at
            ) VALUES ('race-submission', 'session-1', 'app_schedule', ?,
                      'submitting', 1)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', result_submission_id = 'race-submission'
            WHERE run_id = ?
            """,
            (run.run_id,),
        )
        original_finalize = repository.finalize

        async def finalize_after_observation(*args: object, **kwargs: object) -> object:
            await database.execute(
                """
                UPDATE submissions SET observed_user_event_id = 'user-event-race'
                WHERE submission_id = 'race-submission'
                """
            )
            return await original_finalize(*args, **kwargs)

        monkeypatch.setattr(repository, "finalize", finalize_after_observation)
        await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=2,
        )
        terminal = await repository.get_run(run.run_id)

    assert terminal.status == ScheduleRunState.OUTCOME_UNKNOWN


@pytest.mark.asyncio
async def test_stale_tick_cannot_reopen_draining_scheduler(tmp_path: Path) -> None:
    async with Database(tmp_path / "draining-monotonic.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        await repository.mark_tick("worker", now=1)
        await repository.prepare_restart(
            requested_by="operator",
            force=True,
            now=2,
        )
        await repository.mark_tick("stale-worker", now=3)
        state = await database.fetchone(
            "SELECT worker_state, owner_id FROM scheduler_state WHERE singleton = 1"
        )

    assert dict(state) == {
        "worker_state": "draining",
        "owner_id": "worker",
    }


@pytest.mark.asyncio
async def test_nonterminal_creation_intent_blocks_restart(tmp_path: Path) -> None:
    async with Database(tmp_path / "creation-restart-blocker.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        await database.execute(
            """
            INSERT INTO session_creation_intents(
                creation_token, source_kind, source_id, project_source,
                cwd_snapshot, sdk_session_id, state, created_at, updated_at
            ) VALUES ('creation-1', 'schedule', 'run-1', 'implicit-home',
                      '/tmp', 'session-1', 'reserved', 1, 1)
            """
        )

        blockers = await repository.restart_blockers()
        with pytest.raises(ScheduleConflict, match="creation_intent"):
            await repository.prepare_restart(
                requested_by="operator",
                force=False,
                now=2,
            )

    assert "creation_intent:creation-1:reserved" in blockers


@pytest.mark.asyncio
async def test_claim_next_checks_draining_inside_claim_transaction(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "claim-draining.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "pending"},
            target_snapshot={},
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="pending")
        await repository.prepare_restart(
            requested_by="operator",
            force=False,
            now=2,
        )

        claimed = await repository.claim_next("stale-worker", now=3)
        unchanged = await repository.get_run(run.run_id)

    assert claimed is None
    assert unchanged.status == ScheduleRunState.PENDING


@pytest.mark.asyncio
async def test_manual_runs_use_distinct_manual_keys(tmp_path: Path) -> None:
    async with Database(tmp_path / "manual.sqlite3") as database:
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )
        first = await repository.run_now(definition.id, now=1, manual_id="first")
        second = await repository.run_now(definition.id, now=1, manual_id="second")

    assert first.planned_key == "manual:first"
    assert second.planned_key == "manual:second"
    assert first.run_id != second.run_id
    assert first.status == second.status == ScheduleRunState.PENDING


@pytest.mark.asyncio
async def test_heartbeat_projects_scheduler_health_and_restart_protection(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "heartbeat.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="heartbeat")
        await database.execute(
            "UPDATE schedule_runs SET status = 'waiting' WHERE run_id = ?",
            (run.run_id,),
        )
        snapshot = await HeartbeatWriter(
            database,
            tmp_path / "heartbeat.json",
        ).snapshot()

    assert snapshot.app_scheduler_state == "recovered"
    assert snapshot.enabled_app_schedules == 1
    assert snapshot.active_app_schedule_runs == 1
    assert snapshot.unknown_app_schedule_runs == 0
    assert snapshot.protected_work


@pytest.mark.asyncio
async def test_implicit_home_schedule_listing_is_channel_scoped(tmp_path: Path) -> None:
    async with Database(tmp_path / "channel-scope.sqlite3") as database:
        repository = SchedulerRepository(database)
        first = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "one"},
            target_snapshot={},
            channel_id="channel-1",
            now=0,
        )
        await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 10 * * *",
            timezone="UTC",
            payload={"text": "two"},
            target_snapshot={},
            channel_id="channel-2",
            now=0,
        )
        visible = await repository.list(channel_id="channel-1")

    assert [item.id for item in visible] == [first.id]


@pytest.mark.asyncio
async def test_recovery_classifies_every_dispatch_boundary_without_reenqueue(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "boundaries.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="cron:* * * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        queued = await repository.run_now(definition.id, now=1, manual_id="queued")
        dispatching = await repository.run_now(
            definition.id,
            now=2,
            manual_id="dispatching",
        )
        targeting = await repository.run_now(
            definition.id,
            now=3,
            manual_id="targeting",
        )
        known_target = await repository.run_now(
            definition.id,
            now=3.5,
            manual_id="known-target",
        )
        intent_target = await repository.run_now(
            definition.id,
            now=3.75,
            manual_id="intent-target",
        )
        stale = await repository.run_now(definition.id, now=4, manual_id="stale")
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'claimed', lease_owner = 'dead', lease_expires_at = 5,
                fence_token = 1, attempt = 1
            WHERE run_id IN (?, ?)
            """,
            (queued.run_id, stale.run_id),
        )
        await database.execute(
            "UPDATE schedule_runs SET lease_expires_at = 500 WHERE run_id = ?",
            (stale.run_id,),
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                state, created_at
            ) VALUES ('queued-submission', 'session-1', 'app_schedule', ?,
                      'local_queued', 1)
            """,
            (queued.run_id,),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, schedule_run_id, prompt,
                requested_mode_snapshot, requested_model_config_snapshot,
                requested_agent_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES ('queued-submission', 'thread-1', ?, 'scheduled',
                      'interactive', '{}', 'default', 1, 1, 'local_queued', 1, 1)
            """,
            (queued.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', send_started_at = 6,
                lease_owner = 'dead', lease_expires_at = 5,
                fence_token = 1, attempt = 1
            WHERE run_id = ?
            """,
            (dispatching.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', session_create_started_at = 6,
                result_session_id = 'preallocated',
                lease_owner = 'dead', lease_expires_at = 5,
                fence_token = 1, attempt = 1
            WHERE run_id = ?
            """,
            (targeting.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', session_create_started_at = 6,
                result_session_id = 'known-session',
                result_thread_id = 'known-thread',
                lease_owner = 'dead', lease_expires_at = 5,
                fence_token = 1, attempt = 1
            WHERE run_id = ?
            """,
            (known_target.run_id,),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', session_create_started_at = 6,
                result_session_id = 'intent-session',
                lease_owner = 'dead', lease_expires_at = 5,
                fence_token = 1, attempt = 1
            WHERE run_id = ?
            """,
            (intent_target.run_id,),
        )
        await database.execute(
            """
            INSERT INTO session_creation_intents(
                creation_token, source_kind, source_id, project_source,
                cwd_snapshot, sdk_session_id, thread_id, state,
                created_at, updated_at
            ) VALUES ('intent-token', 'schedule', ?, 'implicit-home',
                      '/tmp', 'intent-session', 'intent-thread', 'attached', 6, 6)
            """,
            (intent_target.run_id,),
        )

        counts = await repository.recover(now=10)
        states = {run.run_id: run.status for run in await repository.list_runs(definition.id)}
        queue_count = await database.fetchone(
            "SELECT COUNT(*) FROM message_queue WHERE schedule_run_id = ?",
            (queued.run_id,),
        )
        target_render = await database.fetchone(
            """
            SELECT o.session_id
            FROM schedule_runs r
            JOIN render_outbox o ON o.id = r.render_intent_id
            WHERE r.run_id = ?
            """,
            (targeting.run_id,),
        )
        reconciled = await database.fetchone(
            "SELECT result_thread_id FROM schedule_runs WHERE run_id = ?",
            (intent_target.run_id,),
        )

    assert counts == {
        "queued": 1,
        "dispatch_unknown": 1,
        "reconciled_target": 1,
        "target_unknown": 1,
        "known_target_retry": 1,
        "retry_wait": 0,
        "outcome_unknown": 0,
    }
    assert states[queued.run_id] == ScheduleRunState.SUBMITTING
    assert states[dispatching.run_id] == ScheduleRunState.DISPATCH_UNKNOWN
    assert states[targeting.run_id] == ScheduleRunState.RETRY_WAIT
    assert states[known_target.run_id] == ScheduleRunState.RETRY_WAIT
    assert states[intent_target.run_id] == ScheduleRunState.RETRY_WAIT
    assert states[stale.run_id] == ScheduleRunState.CLAIMED
    assert queue_count[0] == 1
    assert target_render is None
    assert reconciled["result_thread_id"] == "intent-thread"


@pytest.mark.asyncio
async def test_recovered_new_session_waits_for_lease_and_reconciles_retryably(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "new-session-reconcile.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={"execution_config": {}},
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="recovery")
        target = ScheduledTarget(
            project_id=None,
            thread_id="recovered-thread",
            sdk_session_id="recovered-session",
        )
        await _insert_binding(
            database,
            thread_id=target.thread_id,
            session_id=target.sdk_session_id,
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', target_started_at = 1,
                session_create_started_at = 1,
                result_session_id = ?, lease_owner = 'dead-worker',
                lease_expires_at = 100, attempt = 1, fence_token = 1
            WHERE run_id = ?
            """,
            (target.sdk_session_id, run.run_id),
        )

        class RecoveringAdapter(DeterministicSchedulerAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.fail_reconcile = True
                self.reconcile_calls: list[str] = []

            async def prepare_new_session_target(
                self,
                _definition: ScheduleDefinition,
                _run: ScheduleRun,
            ) -> ScheduledTarget:
                raise AssertionError("recovery must not create a second target")

            async def reconcile_target(
                self,
                _definition: ScheduleDefinition,
                recovered_run: ScheduleRun,
            ) -> ScheduledTarget | None:
                self.reconcile_calls.append(recovered_run.run_id)
                if self.fail_reconcile:
                    raise SchedulerDispatchError(
                        "stale target owner still holds its lease",
                        category=SchedulerErrorCategory.RUNTIME,
                        code="stale_target_owner",
                        retryable=True,
                    )
                return target

        adapter = RecoveringAdapter()
        clock = FakeClock(50)
        worker = SchedulerWorker(repository, adapter, owner_id="recovery", clock=clock)

        assert await worker.tick() == 0
        assert adapter.reconcile_calls == []
        clock.advance(51)
        assert await worker.tick() == 1
        retrying = await repository.get_run(run.run_id)
        assert retrying.status == ScheduleRunState.RETRY_WAIT
        assert retrying.error_code == "stale_target_owner"

        adapter.fail_reconcile = False
        clock.advance(30)
        assert await worker.tick() == 1
        recovered = await repository.get_run(run.run_id)
        queue = await database.fetchone(
            "SELECT schedule_run_id FROM message_queue WHERE schedule_run_id = ?",
            (run.run_id,),
        )

    assert adapter.reconcile_calls == [run.run_id, run.run_id]
    assert recovered.status == ScheduleRunState.SUBMITTING
    assert queue["schedule_run_id"] == run.run_id


@pytest.mark.asyncio
async def test_new_session_target_and_queue_are_not_duplicated_after_notification_crash(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "new-session-idempotent.sqlite3") as database:
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={
                "execution_config": {
                    "mode": "interactive",
                    "model_config": {},
                    "agent": "default",
                    "session_config_version": 1,
                }
            },
            now=0,
        )

        class CrashAfterQueueAdapter(DeterministicSchedulerAdapter):
            async def prepare_new_session_target(
                self,
                item: ScheduleDefinition,
                run: ScheduleRun,
            ) -> ScheduledTarget:
                target = await super().prepare_new_session_target(item, run)
                existing = await database.fetchone(
                    "SELECT 1 FROM session_bindings WHERE thread_id = ?",
                    (target.thread_id,),
                )
                if existing is None:
                    await _insert_binding(
                        database,
                        thread_id=target.thread_id,
                        session_id=target.sdk_session_id,
                    )
                return target

            async def queue_ready(self, target: ScheduledTarget, run_id: str) -> None:
                await super().queue_ready(target, run_id)
                raise ConnectionError("notification response lost")

        adapter = CrashAfterQueueAdapter()
        worker = SchedulerWorker(
            repository,
            adapter,
            owner_id="worker",
            clock=FakeClock(60),
        )
        await worker.tick()
        await worker.tick()
        runs = await repository.list_runs(definition.id)
        queue = await database.fetchall("SELECT id, schedule_run_id FROM message_queue")

    assert len(adapter.new_session_targets) == 1
    assert len(adapter.prepare_calls) == 1
    assert len(runs) == 1
    assert len(queue) == 1
    assert queue[0]["schedule_run_id"] == runs[0].run_id


@pytest.mark.asyncio
async def test_temporary_detach_failure_does_not_block_other_candidates(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "detach-isolation.sqlite3") as database:
        await _insert_binding(database, thread_id="thread-1", session_id="session-1")
        await _insert_binding(database, thread_id="thread-2", session_id="session-2")
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        runs: list[ScheduleRun] = []
        for index in (1, 2):
            definition = await repository.create(
                kind=ScheduleKind.MESSAGE,
                expression="cron:0 9 * * *",
                timezone="UTC",
                payload={"text": "scheduled"},
                target_snapshot={},
                thread_id=f"thread-{index}",
                now=0,
            )
            run = await repository.run_now(
                definition.id,
                now=index,
                manual_id=str(index),
            )
            await database.execute(
                """
                UPDATE schedule_runs
                SET result_thread_id = ?, result_session_id = ?,
                    temporary_attachment = 1
                WHERE run_id = ?
                """,
                (f"thread-{index}", f"session-{index}", run.run_id),
            )
            await repository.finalize(
                run.run_id,
                ScheduleRunState.SEMANTIC_COMPLETE,
                completion_basis="loop_idle",
                now=10 + index,
            )
            runs.append(await repository.get_run(run.run_id))

        class IsolatedFailureAdapter(DeterministicSchedulerAdapter):
            async def release_temporary_target(
                self,
                target: ScheduledTarget,
                run: ScheduleRun,
            ) -> None:
                if target.thread_id == "thread-1":
                    raise RuntimeError("detach blocked")
                await super().release_temporary_target(target, run)

        adapter = IsolatedFailureAdapter()
        worker = SchedulerWorker(
            repository,
            adapter,
            owner_id="worker",
            clock=FakeClock(20),
        )
        await worker._release_temporary_targets()
        states = await database.fetchall(
            """
            SELECT run_id, target_released_at FROM schedule_runs
            WHERE run_id IN (?, ?) ORDER BY run_id
            """,
            (runs[0].run_id, runs[1].run_id),
        )
        incident = await database.fetchone(
            """
            SELECT COUNT(*) FROM scheduler_events
            WHERE event_type = 'temporary_target_release_failed'
            """
        )

    released = {row["run_id"]: row["target_released_at"] for row in states}
    assert released[runs[0].run_id] is None
    assert released[runs[1].run_id] is not None
    assert incident[0] == 1


@pytest.mark.asyncio
async def test_close_race_before_enqueue_retries_without_stranding_queue(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "enqueue-close-race.sqlite3") as database:
        await _insert_binding(database)
        repository = SchedulerRepository(database)
        await repository.recover(now=0)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:1970-01-01T00:01:00Z",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot=_target(),
            thread_id="thread-1",
            now=0,
        )
        await repository.plan_due("planner", now=60)
        claimed = await repository.claim_next("worker", now=60)
        assert claimed is not None
        started = await repository.mark_target_started(
            claimed,
            "worker",
            new_session=False,
            now=60,
        )
        target = ScheduledTarget(None, "thread-1", "session-1")
        started = await repository.record_target(
            started,
            "worker",
            target,
            now=60,
        )
        await database.execute(
            """
            UPDATE session_bindings SET attachment_state = 'absent'
            WHERE thread_id = 'thread-1'
            """
        )
        await database.execute(
            "DELETE FROM session_owner_leases WHERE sdk_session_id = 'session-1'"
        )

        with pytest.raises(SchedulerDispatchError) as failure:
            await repository.enqueue(
                definition,
                started,
                "worker",
                target,
                now=61,
            )
        state = await repository.retry_or_fail(
            started,
            "worker",
            failure.value,
            now=61,
        )
        queued = await database.fetchone(
            "SELECT COUNT(*) FROM message_queue WHERE schedule_run_id = ?",
            (started.run_id,),
        )

    assert state == ScheduleRunState.RETRY_WAIT
    assert queued[0] == 0
