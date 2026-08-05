from pathlib import Path

import pytest

from copilotd.core.inbox import ReducerInbox
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.scheduler import ScheduleKind, SchedulerRepository
from copilotd.storage.database import Database


async def _start_reducer(
    database: Database,
) -> tuple[ReducerInbox, EventReducerWorker]:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=128,
    )
    reducer = EventReducerWorker(
        inbox=inbox,
        reducer=JournalReducer(database),
        batch_size=16,
    )
    reducer.start()
    return inbox, reducer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old_state", "replacement_prompt"),
    [
        ("blocked_mode_drift", "old prompt"),
        ("local_queued", "updated prompt"),
    ],
)
async def test_schedule_queue_replacement_transfers_nonterminal_slot_atomically(
    tmp_path: Path,
    old_state: str,
    replacement_prompt: str,
) -> None:
    async with Database(tmp_path / "queue-resubmit.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, permission_posture, runtime_mode, runtime_agent,
                runtime_session_config_version, runtime_generation,
                owner_fence_token, created_at, updated_at
            ) VALUES ('thread-1', 'explicit', '/tmp', 'session-1', 'attached',
                      'verified_allow_all', 'interactive', 'default', 1, 1, 7, 0, 0)
            """
        )
        repository = SchedulerRepository(database)
        definition = await repository.create(
            kind=ScheduleKind.MESSAGE,
            expression="at:2030-01-01T00:00:00Z",
            timezone="UTC",
            payload={"text": "old prompt"},
            target_snapshot={},
            thread_id="thread-1",
            now=0,
        )
        run = await repository.run_now(definition.id, now=1, manual_id="resubmit")
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, schedule_run_id,
                prompt_hash, requested_delivery, attachment_count,
                state, created_at
            ) VALUES ('old', 'session-1', 'app_schedule', ?, 'hash',
                      'enqueue', 0, 'local_queued', 1)
            """,
            (run.run_id,),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, schedule_run_id, prompt,
                requested_mode_snapshot, requested_model_config_snapshot,
                requested_agent_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES ('old', 'thread-1', ?, 'old prompt', 'plan', '{}',
                      'default', 1, 1, ?, 1, 1)
            """,
            (run.run_id, old_state),
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET status = 'submitting', result_submission_id = 'old',
                result_thread_id = 'thread-1', result_session_id = 'session-1'
            WHERE run_id = ?
            """,
            (run.run_id,),
        )
        inbox, reducer = await _start_reducer(database)

        await inbox.commit_internal(
            {
                "type": "copilotd.queue.replaced",
                "data": {
                    "old_submission_id": "old",
                    "new_submission_id": "new",
                    "prompt": replacement_prompt,
                    "prompt_hash": "hash",
                    "allowed_states": [old_state],
                    "requested_mode": "interactive",
                    "requested_model_config": {},
                    "requested_agent": "default",
                    "requested_session_config_version": 1,
                    "created_at": 2,
                },
            },
            internal_event_id="queue:new:replaced",
        )
        rows = await database.fetchall(
            """
            SELECT id, schedule_run_id, prompt, state, replaces_id
            FROM message_queue ORDER BY position
            """
        )
        updated_run = await database.fetchone(
            "SELECT status, result_submission_id FROM schedule_runs WHERE run_id = ?",
            (run.run_id,),
        )

        await inbox.commit_internal(
            {
                "type": "copilotd.submission.cancel_queued",
                "data": {
                    "submission_ids": ["new"],
                    "cancellable_states": ["local_queued"],
                    "cancelled_at": 3,
                },
            },
            internal_event_id="submissions:new:cancelled",
        )
        cancelled_run = await database.fetchone(
            """
            SELECT status, render_intent_id
            FROM schedule_runs WHERE run_id = ?
            """,
            (run.run_id,),
        )
        render = await database.fetchone(
            "SELECT terminal_status FROM scheduler_render_intents WHERE run_id = ?",
            (run.run_id,),
        )
        await reducer.stop()

    assert [dict(row) for row in rows] == [
        {
            "id": "old",
            "schedule_run_id": run.run_id,
            "prompt": "old prompt",
            "state": "cancelled",
            "replaces_id": None,
        },
        {
            "id": "new",
            "schedule_run_id": run.run_id,
            "prompt": replacement_prompt,
            "state": "local_queued",
            "replaces_id": "old",
        },
    ]
    assert dict(updated_run) == {
        "status": "submitting",
        "result_submission_id": "new",
    }
    assert cancelled_run["status"] == "cancelled"
    assert cancelled_run["render_intent_id"] is not None
    assert render["terminal_status"] == "cancelled"
