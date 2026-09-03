import asyncio
import hashlib
import json
import threading
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from copilot.session_events import (
    AssistantMessageData,
    AssistantMessageDeltaData,
    SessionEvent,
    SessionEventType,
    SubagentCompletedData,
    SubagentStartedData,
)

from copilotd.config import Settings
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.event_adapter import EventAdapter
from copilotd.core.event_inventory import (
    EVENT_DISPOSITIONS,
    MAIN_BRANCH_ONLY_DISPOSITIONS,
    disposition_for,
)
from copilotd.core.inbox import ReducerInbox, SdkEventIngress
from copilotd.core.models import AdaptedEvent, InboxEnvelope
from copilotd.core.reducer import (
    EventReducerWorker,
    JournalReducer,
    RenderPlanner,
)
from copilotd.core.scheduler import SchedulerRepository, ScheduleRunState
from copilotd.core.volatile_content import CommittedCancellation, opaque_content_key
from copilotd.ops.heartbeat import HeartbeatWriter
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.storage.database import Database


def _message_delta(event_id: UUID | None = None, *, content: str = "hello") -> SessionEvent:
    return SessionEvent(
        data=AssistantMessageDeltaData(delta_content=content, message_id="message-1"),
        id=event_id or uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.ASSISTANT_MESSAGE_DELTA,
    )


def _message_final(content: str) -> SessionEvent:
    return SessionEvent(
        data=AssistantMessageData(
            content=content,
            message_id="message-1",
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.ASSISTANT_MESSAGE,
    )


def _adapted(
    raw_type: str,
    data: dict[str, Any],
    inbox_seq: int,
    *,
    source: str = "sdk",
    session_id: str = "session-projection",
    generation: int = 1,
    fence_token: int = 7,
) -> AdaptedEvent:
    event_id = str(uuid4()) if source == "sdk" else None
    return AdaptedEvent(
        sdk_session_id=session_id,
        generation=generation,
        fence_token=fence_token,
        inbox_seq=inbox_seq,
        source=source,
        raw_type=raw_type,
        raw_payload={"type": raw_type, "data": data},
        reducer_hash=f"hash-{inbox_seq}",
        persistence_class="durable" if source == "sdk" else "internal",
        received_at=100 + inbox_seq,
        event_id=event_id,
        internal_event_id=None if source == "sdk" else f"{raw_type}:{inbox_seq}",
        turn_id=None if data.get("turnId") is None else str(data["turnId"]),
        interaction_id=(None if data.get("interactionId") is None else str(data["interactionId"])),
    )


async def _insert_projection_binding(database: Database, session_id: str) -> None:
    await database.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            attachment_state, permission_posture,
            runtime_generation, owner_fence_token, created_at, updated_at
        ) VALUES ('thread-projection', 'home', '/tmp', ?,
                  'attached', 'verified_allow_all', 1, 7, 1, 1)
        """,
        (session_id,),
    )
    await database.execute(
        """
        INSERT INTO session_owner_leases(
            sdk_session_id, owner_id, fence_token,
            acquired_at, renewed_at, expires_at
        ) VALUES (?, 'test-owner', 7, 1, 1, 9999999999)
        """,
        (session_id,),
    )


@pytest.mark.asyncio
async def test_shutdown_settlement_is_reversed_by_later_linked_resume(
    tmp_path: Path,
) -> None:
    session_id = "session-shutdown-order"
    async with Database(tmp_path / "shutdown-order.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        reducer = JournalReducer(database, require_binding_fence=True)
        shutdown = _adapted(
            "session.shutdown",
            {"shutdownType": "routine"},
            1,
            session_id=session_id,
        )
        resume = replace(
            _adapted(
                "session.resume",
                {"eventCount": 1},
                2,
                session_id=session_id,
            ),
            parent_id=shutdown.event_id,
        )

        await reducer.persist([shutdown])
        interrupted = await database.fetchone(
            """
            SELECT attachment_state FROM session_bindings
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        await reducer.persist([resume])
        settled = await database.fetchone(
            """
            SELECT attachment_state, permission_posture FROM session_bindings
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        shutdown_render = await database.fetchone(
            """
            SELECT state, payload, payload_revision FROM render_outbox
            WHERE idempotency_key = ?
            """,
            (f"event:{session_id}:{shutdown.event_id}:status",),
        )

    assert interrupted["attachment_state"] == "recovery_unknown"
    assert dict(settled) == {
        "attachment_state": "attached",
        "permission_posture": "verified_allow_all",
    }
    assert shutdown_render["state"] == "pending"
    assert shutdown_render["payload_revision"] == 2
    assert json.loads(str(shutdown_render["payload"]))["render_kind"] == "session.resume"


@pytest.mark.parametrize("shutdown_render_state", ["pending", "sending", "blocked", "sent"])
@pytest.mark.asyncio
async def test_linked_resume_recovers_only_its_submission_shutdown_failure(
    tmp_path: Path,
    shutdown_render_state: str,
) -> None:
    session_id = "session-submission-shutdown"
    async with Database(tmp_path / "submission-shutdown.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        async with database.transaction() as connection:
            await connection.executemany(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, state,
                    discord_source_channel_id, discord_source_message_id,
                    created_at, observed_at, terminal_at
                ) VALUES (?, ?, 'app_message', ?, 'channel-1', ?, 1, 1, ?)
                """,
                (
                    ("active-submission", session_id, "observed_active", "message-active", None),
                    ("other-submission", session_id, "semantic_blocked", "message-other", 2),
                ),
            )
            await connection.executemany(
                """
                INSERT INTO submission_reactions(
                    submission_id, sdk_session_id, source_channel_id, source_message_id,
                    desired_state, delivered_state, revision, delivered_revision,
                    runtime_generation, owner_fence_token, terminal, created_at, updated_at
                ) VALUES (?, ?, 'channel-1', ?, ?, ?, 1, 1, 1, 7, ?, 1, 1)
                """,
                (
                    (
                        "active-submission",
                        session_id,
                        "message-active",
                        "action",
                        "action",
                        0,
                    ),
                    (
                        "other-submission",
                        session_id,
                        "message-other",
                        "failed",
                        "failed",
                        1,
                    ),
                ),
            )
        await database.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at
            ) VALUES (
                'other-failure', ?, 0, 'status', 'failure:turn:other-submission',
                'render-family:other-failure',
                '{"schema":1,"render_kind":"model.call_failure",'
                || '"submission_id":"other-submission","finalized":true}',
                'pending', 0, 0, 1, 1
            )
            """,
            (session_id,),
        )
        reducer = JournalReducer(database, require_binding_fence=True)
        shutdown = _adapted(
            "session.shutdown",
            {"shutdownType": "routine"},
            1,
            session_id=session_id,
        )
        resume = replace(
            _adapted(
                "session.resume",
                {"eventCount": 1},
                2,
                session_id=session_id,
            ),
            parent_id=shutdown.event_id,
        )

        assert await reducer.persist([shutdown]) == 1
        shutdown_failure = await database.fetchone(
            """
            SELECT id, state, payload, content_key, content_hash FROM render_outbox
            WHERE idempotency_key = ?
            """,
            (f"render-family:{session_id}:failure:turn:active-submission",),
        )
        failed_reaction = await database.fetchone(
            """
            SELECT desired_state, resume_state, terminal
            FROM submission_reactions WHERE submission_id = 'active-submission'
            """
        )
        await database.execute(
            """
            UPDATE render_outbox
            SET state = ?, last_error = CASE
                    WHEN ? = 'blocked' THEN 'render_delivery_error'
                    ELSE last_error
                END
            WHERE id = ?
            """,
            (shutdown_render_state, shutdown_render_state, shutdown_failure["id"]),
        )
        assert await reducer.persist([resume]) == 1
        settled_failure = await database.fetchone(
            """
            SELECT state, payload, content_key, content_hash,
                   payload_revision, last_error
            FROM render_outbox WHERE id = ?
            """,
            (shutdown_failure["id"],),
        )
        other_failure = await database.fetchone(
            "SELECT state FROM render_outbox WHERE id = 'other-failure'"
        )
        reactions = await database.fetchall(
            """
            SELECT submission_id, desired_state, resume_state, terminal
            FROM submission_reactions ORDER BY submission_id
            """
        )
        reaction_outbox = await database.fetchone(
            """
            SELECT payload FROM render_outbox
            WHERE idempotency_key = 'reaction:active-submission'
            """
        )

    shutdown_payload = json.loads(str(shutdown_failure["payload"]))
    shutdown_resume_state = json.loads(str(failed_reaction["resume_state"]))
    assert shutdown_failure["state"] == "pending"
    assert shutdown_payload["shutdown_event_id"] == shutdown.event_id
    assert shutdown_resume_state["event_id"] == shutdown.event_id
    assert settled_failure["state"] == (
        "sending" if shutdown_render_state == "sending" else "pending"
    )
    assert settled_failure["payload_revision"] == 2
    assert settled_failure["last_error"] is None
    recovered_payload = database.content_store.require(
        str(settled_failure["content_key"]),
        expected_hash=str(settled_failure["content_hash"]),
    )
    assert recovered_payload["type"] == "session.resume"
    assert recovered_payload["status"]["title"] == "Session resumed"
    assert recovered_payload["shutdown_event_id"] == shutdown.event_id
    assert recovered_payload["shutdown_generation"] == 1
    assert recovered_payload["finalized"] is True
    assert dict(other_failure) == {"state": "pending"}
    assert [dict(row) for row in reactions] == [
        {
            "submission_id": "active-submission",
            "desired_state": "action",
            "resume_state": None,
            "terminal": 0,
        },
        {
            "submission_id": "other-submission",
            "desired_state": "failed",
            "resume_state": None,
            "terminal": 1,
        },
    ]
    assert json.loads(str(reaction_outbox["payload"]))["state"] == "action"


@pytest.mark.parametrize("event_type", list(SessionEventType))
def test_every_sdk_1_0_8_generated_event_has_an_explicit_disposition(
    event_type: SessionEventType,
) -> None:
    disposition = disposition_for(event_type.value)

    assert event_type.value in EVENT_DISPOSITIONS
    assert disposition.state in {"audit", "fallback", "journal", "reconcile", "reduce"}
    assert disposition.render in {"content", "gated", "none", "status"}
    assert disposition.liveness in {
        "correlate",
        "diagnostic",
        "interaction",
        "none",
        "snapshot",
    }
    assert disposition.rationale


def test_event_inventory_tracks_main_only_events_without_claiming_them_for_sdk_1_0_8() -> None:
    assert len(EVENT_DISPOSITIONS) == 114
    assert set(MAIN_BRANCH_ONLY_DISPOSITIONS) == {
        "factory.run_updated",
        "session.context_cleared",
    }
    assert not set(MAIN_BRANCH_ONLY_DISPOSITIONS).intersection(EVENT_DISPOSITIONS)


@pytest.mark.asyncio
async def test_inbox_reserves_capacity_before_cross_thread_scheduling() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=32,
    )
    received: list[int] = []

    async def consume() -> None:
        for _ in range(20):
            envelope = await inbox.get()
            received.append(envelope.inbox_seq)
            inbox.acknowledge(envelope)

    consumer = asyncio.create_task(consume())
    thread = threading.Thread(target=lambda: [_submit(inbox) for _ in range(20)])
    thread.start()
    thread.join()
    await consumer

    assert received == list(range(1, 21))
    assert inbox.size == 0
    assert inbox.overflow is None


@pytest.mark.asyncio
async def test_sdk_ingress_serializes_acceptance_callbacks_with_reservations() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=4,
    )
    first = _message_delta(content="first")
    second = _message_delta(content="second")
    first_reserved = threading.Event()
    release_first = threading.Event()
    original_submit = inbox.submit_sdk

    def delayed_submit(event: SessionEvent) -> bool:
        accepted = original_submit(event)
        if event is first:
            first_reserved.set()
            assert release_first.wait(timeout=1)
        return accepted

    inbox.submit_sdk = delayed_submit  # type: ignore[method-assign]
    accepted: list[SessionEvent] = []
    ingress = SdkEventIngress(inbox, on_event_accepted=accepted.append)
    first_thread = threading.Thread(target=ingress, args=(first,))
    second_thread = threading.Thread(target=ingress, args=(second,))

    first_thread.start()
    assert first_reserved.wait(timeout=1)
    second_thread.start()
    release_first.set()
    first_thread.join()
    second_thread.join()

    assert accepted == [first, second]
    for _ in range(2):
        envelope = await inbox.get()
        inbox.acknowledge(envelope)


@pytest.mark.asyncio
async def test_inbox_reports_real_oldest_outstanding_lag() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=2,
    )
    assert inbox.submit_sdk(_message_delta())
    await asyncio.sleep(0.01)

    assert inbox.size == 1
    assert inbox.lag_ms >= 5
    assert inbox.last_received_at is not None
    envelope = await inbox.get()
    inbox.acknowledge(envelope)
    assert inbox.lag_ms == 0


@pytest.mark.asyncio
async def test_quiesce_observer_covers_sdk_and_internal_producers() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=4,
    )
    observed: list[str] = []
    inbox.set_producer_observer(observed.append)

    assert inbox.submit_sdk(_message_delta())
    internal = asyncio.create_task(
        inbox.commit_internal(
            {"type": "copilotd.snapshot"},
            source="snapshot",
            internal_event_id="snapshot:quiesce-observer",
        )
    )
    for _ in range(2):
        envelope = await inbox.get()
        inbox.acknowledge(envelope)
    await internal

    assert observed == ["sdk", "snapshot"]


@pytest.mark.asyncio
async def test_observer_change_and_reservation_share_one_barrier() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=2,
    )
    observer_entered = threading.Event()
    release_observer = threading.Event()
    observed: list[str] = []

    def observer(source: str) -> None:
        observed.append(source)
        observer_entered.set()
        release_observer.wait(timeout=1)

    inbox.set_quiesce_observers(observer, None)
    producer = threading.Thread(target=lambda: _submit(inbox))
    producer.start()
    assert observer_entered.wait(timeout=1)
    setter = threading.Thread(target=lambda: inbox.set_quiesce_observers(None, None))
    setter.start()
    setter.join(timeout=0.02)
    assert setter.is_alive()
    release_observer.set()
    producer.join(timeout=1)
    setter.join(timeout=1)

    assert observed == ["sdk"]
    envelope = await inbox.get()
    inbox.acknowledge(envelope)


@pytest.mark.asyncio
async def test_quiesce_loss_observer_marks_overflow() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=1,
    )
    producers: list[str] = []
    losses: list[str] = []
    inbox.set_quiesce_observers(producers.append, losses.append)

    assert inbox.submit_sdk(_message_delta())
    assert not inbox.submit_sdk(_message_delta())

    assert producers == ["sdk", "sdk"]
    assert losses == ["inbox_overflow"]
    envelope = await inbox.get()
    inbox.acknowledge(envelope)


def _submit(inbox: ReducerInbox) -> None:
    assert inbox.submit_sdk(_message_delta())


@pytest.mark.asyncio
async def test_internal_receipts_require_caller_owned_ids() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=2,
    )

    with pytest.raises(ValueError, match="explicit internal_event_id"):
        inbox.submit_internal({"type": "copilotd.test"})
    with pytest.raises(ValueError, match="explicit internal_event_id"):
        await inbox.commit_internal({"type": "copilotd.test"})


@pytest.mark.asyncio
async def test_inbox_overflow_is_explicit_and_keeps_first_lost_sequence() -> None:
    inbox = ReducerInbox(
        sdk_session_id="session-1",
        generation=1,
        fence_token=7,
        capacity=1,
    )

    assert inbox.submit_sdk(_message_delta())
    assert not inbox.submit_sdk(_message_delta())
    assert not inbox.submit_sdk(_message_delta())

    incident = inbox.overflow
    assert incident is not None
    assert incident.first_lost_inbox_seq == 2
    assert incident.first_lost_sdk_receive_seq == 2
    assert incident.lost_count == 2

    envelope = await inbox.get()
    inbox.acknowledge(envelope)


def test_adapter_carries_complete_internal_envelope_fields() -> None:
    timestamp = datetime(2026, 8, 5, 12, 30, tzinfo=UTC)
    event = SessionEvent(
        data=SimpleNamespace(
            to_dict=lambda: {
                "messageId": "message-1",
                "turnId": "turn-1",
                "interactionId": "interaction-1",
                "taskId": "task-1",
                "toolCallId": "tool-1",
                "requestId": "request-1",
                "correlationId": "correlation-1",
            }
        ),  # type: ignore[arg-type]
        id=uuid4(),
        timestamp=timestamp,
        type=SessionEventType.UNKNOWN,
        agent_id="agent-1",
    )
    adapted = EventAdapter().adapt(
        InboxEnvelope(
            sdk_session_id="session-1",
            generation=2,
            fence_token=9,
            inbox_seq=4,
            source="sdk",
            payload=event,
            received_at=123,
            thread_id="thread-1",
            sdk_receive_seq=3,
        )
    )

    assert adapted.schema_version == 1
    assert adapted.thread_id == "thread-1"
    assert adapted.sdk_timestamp == timestamp.timestamp()
    assert adapted.message_id == "message-1"
    assert adapted.turn_id == "turn-1"
    assert adapted.interaction_id == "interaction-1"
    assert adapted.task_id == "task-1"
    assert adapted.tool_call_id == "tool-1"
    assert adapted.request_id == "request-1"
    assert adapted.correlation_id == "correlation-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_id", "expected_kind"),
    [(None, "missing_event_id"), ("not-a-uuid", "invalid_event_id")],
)
async def test_invalid_sdk_event_ids_become_durable_incidents(
    tmp_path: Path,
    event_id: Any,
    expected_kind: str,
) -> None:
    async with Database(tmp_path / f"{expected_kind}.sqlite3") as database:
        now = datetime.now(UTC).timestamp()
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                runtime_generation, owner_fence_token, created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1', 1, 7, ?, ?)
            """,
            (now, now),
        )
        inbox = ReducerInbox(
            sdk_session_id="session-1",
            generation=1,
            fence_token=7,
            capacity=4,
            thread_id="thread-1",
        )
        reducer = JournalReducer(database)
        worker = EventReducerWorker(inbox=inbox, reducer=reducer, batch_size=4)
        worker.start()
        invalid = SessionEvent(
            data=AssistantMessageDeltaData(delta_content="lost", message_id="message-1"),
            id=event_id,  # type: ignore[arg-type]
            timestamp=datetime.now(UTC),
            type=SessionEventType.ASSISTANT_MESSAGE_DELTA,
        )
        assert inbox.submit_sdk(invalid)
        await inbox.join()
        await worker.stop()
        incidents = await database.fetchall(
            "SELECT kind, last_inbox_seq, detail FROM runtime_incidents"
        )
        sdk_rows = await database.fetchall(
            "SELECT event_id FROM event_journal WHERE source = 'sdk'"
        )

    assert len(incidents) == 1
    assert incidents[0]["kind"] == expected_kind
    assert incidents[0]["last_inbox_seq"] == 1
    assert json.loads(incidents[0]["detail"])["reason"].startswith("sha256:")
    assert sdk_rows == []


@pytest.mark.asyncio
async def test_reducer_atomically_journals_deduplicates_and_creates_outbox(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "core.sqlite3") as database:
        now = datetime.now(UTC).timestamp()
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                runtime_generation, owner_fence_token, created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1', 1, 7, ?, ?)
            """,
            (now, now),
        )
        inbox = ReducerInbox(
            sdk_session_id="session-1",
            generation=1,
            fence_token=7,
            capacity=8,
        )

        async def validate(generation: int, fence_token: int) -> bool:
            return generation == 1 and fence_token == 7

        reducer = JournalReducer(database)
        worker = EventReducerWorker(
            inbox=inbox,
            reducer=reducer,
            batch_size=8,
            fence_validator=validate,
        )
        worker.start()
        event_id = uuid4()
        assert inbox.submit_sdk(_message_delta(event_id))
        await inbox.join()
        await worker.stop()

        journal = await database.fetchall(
            "SELECT raw_type, event_id FROM event_journal ORDER BY inbox_seq"
        )
        outbox = await database.fetchall(
            "SELECT lane, coalesce_key, state FROM render_outbox ORDER BY logical_seq"
        )
        binding = await database.fetchone(
            "SELECT last_inbox_seq, last_sdk_receive_seq FROM session_bindings "
            "WHERE thread_id = 'thread-1'"
        )

        duplicate = InboxEnvelope(
            sdk_session_id="session-1",
            generation=2,
            fence_token=8,
            inbox_seq=1,
            source="sdk",
            payload=_message_delta(event_id),
            received_at=now + 1,
            sdk_receive_seq=1,
        )
        inserted = await reducer.persist([EventAdapter().adapt(duplicate)])

    assert [row["raw_type"] for row in journal] == [
        "assistant.message_delta",
        "copilotd.reducer.stop",
    ]
    assert journal[0]["event_id"] == str(event_id)
    assert [dict(row) for row in outbox] == [
        {
            "lane": "assistant_stream",
            "coalesce_key": "assistant:message-1",
            "state": "pending",
        }
    ]
    assert dict(binding) == {"last_inbox_seq": 2, "last_sdk_receive_seq": 1}
    assert inserted == 0


@pytest.mark.asyncio
async def test_journal_and_outbox_rollback_together(tmp_path: Path) -> None:
    class FailingPlanner:
        def __init__(self) -> None:
            self.fail_once = True
            self.delegate = RenderPlanner()

        def plan(
            self,
            event: AdaptedEvent,
            *,
            payload_override: dict[str, Any] | None = None,
        ) -> list[Any]:
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("render planning failed")
            return self.delegate.plan(event, payload_override=payload_override)

    async with Database(tmp_path / "atomic.sqlite3") as database:
        reducer = JournalReducer(database, planner=FailingPlanner())
        envelope = InboxEnvelope(
            sdk_session_id="session-1",
            generation=1,
            fence_token=1,
            inbox_seq=1,
            source="sdk",
            payload=_message_delta(),
            received_at=datetime.now(UTC).timestamp(),
            sdk_receive_seq=1,
        )

        with pytest.raises(RuntimeError, match="render planning failed"):
            await reducer.persist([EventAdapter().adapt(envelope)])

        count = await database.fetchone("SELECT COUNT(*) FROM event_journal")
        stream_key = opaque_content_key(
            "assistant-stream",
            "session-1",
            "message-1",
            "",
        )
        assert count[0] == 0
        assert database.content_store.get(stream_key) is None

        assert await reducer.persist([EventAdapter().adapt(envelope)]) == 1
        stream = database.content_store.require(stream_key)
        row = await database.fetchone("SELECT content_key, content_hash FROM render_outbox")
        payload = database.content_store.require(
            str(row["content_key"]),
            expected_hash=str(row["content_hash"]),
        )

    assert stream == {"content": "hello", "finalized": False}
    assert payload["content"] == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_succeeds", [True, False])
async def test_reducer_cancellation_waits_for_coupled_commit_or_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    commit_succeeds: bool,
) -> None:
    async with Database(tmp_path / f"cancel-atomic-{commit_succeeds}.sqlite3") as database:
        reducer = JournalReducer(database)
        envelope = InboxEnvelope(
            sdk_session_id="session-cancel",
            generation=1,
            fence_token=1,
            inbox_seq=1,
            source="sdk",
            payload=_message_delta(content="commit-delay"),
            received_at=datetime.now(UTC).timestamp(),
            sdk_receive_seq=1,
        )
        event = EventAdapter().adapt(envelope)
        commit_started = asyncio.Event()
        finish_commit = asyncio.Event()
        original_commit = database.connection.commit

        async def delayed_commit() -> None:
            commit_started.set()
            await finish_commit.wait()
            if not commit_succeeds:
                raise RuntimeError("injected commit failure")
            await original_commit()

        monkeypatch.setattr(database.connection, "commit", delayed_commit)
        persistence = asyncio.create_task(reducer.persist([event]))
        await commit_started.wait()
        persistence.cancel()
        await asyncio.sleep(0)
        assert not persistence.done()

        finish_commit.set()
        expected_error = CommittedCancellation if commit_succeeds else RuntimeError
        with pytest.raises(expected_error):
            await persistence

        journal = await database.fetchone("SELECT COUNT(*) FROM event_journal")
        outbox = await database.fetchone("SELECT COUNT(*) FROM render_outbox")
        stream_key = opaque_content_key(
            "assistant-stream",
            "session-cancel",
            "message-1",
            "",
        )

    expected = int(commit_succeeds)
    assert journal[0] == expected
    assert outbox[0] == expected
    assert (database.content_store.get(stream_key) is not None) is commit_succeeds


@pytest.mark.asyncio
async def test_reducer_coalesces_full_stream_content_in_durable_message_family(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "stream.sqlite3") as database:
        adapter = EventAdapter()
        events = [
            _message_delta(content="hel"),
            _message_delta(content="lo"),
            _message_final("hello!"),
        ]
        adapted = [
            adapter.adapt(
                InboxEnvelope(
                    sdk_session_id="session-1",
                    generation=1,
                    fence_token=1,
                    inbox_seq=index,
                    source="sdk",
                    payload=event,
                    received_at=100 + index,
                    sdk_receive_seq=index,
                )
            )
            for index, event in enumerate(events, start=1)
        ]

        assert await JournalReducer(database).persist(adapted) == 3
        rows = await database.fetchall(
            """
            SELECT content_key, content_hash, payload_revision
            FROM render_outbox ORDER BY logical_seq
            """
        )
        stream = database.content_store.require(
            opaque_content_key("assistant-stream", "session-1", "message-1", "")
        )

    payloads = [
        database.content_store.require(
            str(row["content_key"]),
            expected_hash=str(row["content_hash"]),
        )
        for row in rows
    ]
    assert [payload["content"] for payload in payloads] == ["hello!"]
    assert [payload["finalized"] for payload in payloads] == [True]
    assert rows[0]["payload_revision"] == 3
    assert stream == {"content": "hello!", "finalized": True}


@pytest.mark.asyncio
async def test_reducer_materializes_status_and_keeps_usage_in_footer_projection(
    tmp_path: Path,
) -> None:
    adapter = EventAdapter()
    source_events = [
        {
            "type": "session.warning",
            "data": {"message": "Context is nearly full."},
        },
        {
            "type": "session.usage_info",
            "data": {
                "tokenUsage": {
                    "inputTokens": 12,
                    "outputTokens": 7,
                    "totalTokens": 19,
                },
                "premiumRequests": 1,
            },
        },
        {
            "type": "assistant.reasoning",
            "data": {"content": "private chain of thought"},
        },
        {
            "type": "session.workspace_file_changed",
            "data": {"operation": "modified", "path": "src/copilotd/app.py"},
        },
        {
            "type": "session.task_complete",
            "data": {"outcome": "blocked"},
        },
        {
            "type": "session.task_complete",
            "data": {"success": True},
        },
    ]
    adapted = [
        adapter.adapt(
            InboxEnvelope(
                sdk_session_id="session-render-status",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload=event,
                received_at=100 + index,
                internal_event_id=f"status-{index}",
            )
        )
        for index, event in enumerate(source_events, start=1)
    ]
    async with Database(tmp_path / "status-render.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 6
        rows = await database.fetchall(
            """
            SELECT lane, content_key, content_hash
            FROM render_outbox ORDER BY logical_seq
            """
        )
        usage = await database.fetchone(
            """
            SELECT input_tokens, output_tokens, premium_requests
            FROM usage_samples WHERE session_id = 'session-render-status'
            """
        )

    payloads = [
        (
            row["lane"],
            database.content_store.require(
                str(row["content_key"]),
                expected_hash=str(row["content_hash"]),
            ),
        )
        for row in rows
    ]
    assert len(payloads) == 4
    assert payloads[0][0] == "status"
    assert "Context is nearly full." in payloads[0][1]["content"]
    assert payloads[0][1]["status"] == {
        "title": "Copilot warning",
        "detail": "Context is nearly full.",
        "event_type": "session.warning",
    }
    assert dict(usage) == {
        "input_tokens": 12,
        "output_tokens": 7,
        "premium_requests": 1.0,
    }
    assert payloads[1][0] == "status"
    assert "`modified` `src/copilotd/app.py`" in payloads[1][1]["content"]
    assert payloads[2][0] == "status"
    assert payloads[2][1]["status"]["outcome"] == "blocked"
    assert payloads[3][0] == "status"
    assert payloads[3][1]["status"]["outcome"] == "completed"


@pytest.mark.asyncio
async def test_reasoning_and_transport_only_deltas_are_suppressed(
    tmp_path: Path,
) -> None:
    planner = RenderPlanner()
    reasoning = _adapted(
        "assistant.reasoning_delta",
        {"content": "private step-by-step chain"},
        1,
    )
    streaming = _adapted(
        "assistant.streaming_delta",
        {"content": "transport-only bytes"},
        2,
    )

    assert planner.plan(reasoning) == []
    assert planner.plan(streaming) == []

    async with Database(tmp_path / "reasoning-render.sqlite3") as database:
        assert await JournalReducer(database).persist([reasoning, streaming]) == 2
        rows = await database.fetchall(
            "SELECT lane, coalesce_key, payload FROM render_outbox ORDER BY logical_seq"
        )

    assert rows == []


@pytest.mark.asyncio
async def test_correlated_idle_keeps_metrics_durable_without_footer_message(
    tmp_path: Path,
) -> None:
    session_id = "session-footer"
    async with Database(tmp_path / "footer.sqlite3") as database:
        await SessionBindingRepository(database).create(
            thread_id="thread-footer",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
            now=90,
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_model_config = '{"modelId":"gpt-footer"}'
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, requested_mode,
                requested_delivery, state, created_at
            ) VALUES ('submission-footer', ?, 'app_message', 'interactive',
                      'enqueue', 'observed_active', 100)
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO usage_samples(
                session_id, model, input_tokens, output_tokens,
                premium_requests, observed_at
            ) VALUES (?, 'gpt-footer', 12, 7, 1.5, 119)
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO session_projection_snapshots(
                session_id, kind, payload, observed_at
            ) VALUES (?, 'context', '{"totalTokens":19,"limit":100}', 119)
            """,
            (session_id,),
        )
        event = EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id=session_id,
                generation=0,
                fence_token=0,
                inbox_seq=1,
                source="internal",
                payload={"type": "session.idle", "data": {}},
                received_at=120,
                internal_event_id="idle-footer",
            )
        )
        assert await JournalReducer(database).persist([event]) == 1
        row = await database.fetchone(
            "SELECT lane, payload FROM render_outbox WHERE session_id = ?",
            (session_id,),
        )

    assert row is None


@pytest.mark.asyncio
async def test_wire_requested_and_completed_events_pair_by_request_id(
    tmp_path: Path,
) -> None:
    events = [
        AdaptedEvent(
            sdk_session_id="session-protocol",
            generation=1,
            fence_token=7,
            inbox_seq=1,
            source="sdk",
            raw_type="user_input.requested",
            raw_payload={"data": {"requestId": "request-1"}},
            reducer_hash="requested",
            persistence_class="durable",
            received_at=100,
            event_id="event-requested",
        ),
        AdaptedEvent(
            sdk_session_id="session-protocol",
            generation=1,
            fence_token=7,
            inbox_seq=2,
            source="sdk",
            raw_type="user_input.completed",
            raw_payload={"data": {"requestId": "request-1"}},
            reducer_hash="completed",
            persistence_class="durable",
            received_at=101,
            event_id="event-completed",
        ),
    ]
    async with Database(tmp_path / "protocol-pair.sqlite3") as database:
        assert await JournalReducer(database).persist(events) == 2
        row = await database.fetchone(
            """
            SELECT requested_type, requested_event_id, completed_event_id,
                   wire_state, response_state
            FROM protocol_requests WHERE request_id = 'request-1'
            """
        )

    assert dict(row) == {
        "requested_type": "user_input.requested",
        "requested_event_id": "event-requested",
        "completed_event_id": "event-completed",
        "wire_state": "paired",
        "response_state": "delegated",
    }


@pytest.mark.asyncio
async def test_external_mode_change_marks_drift_without_overwriting_desired(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "mode-drift.sqlite3") as database:
        await _insert_projection_binding(database, "session-mode-drift")
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _adapted(
                    "session.mode_changed",
                    {"newMode": "autopilot", "previousMode": "interactive"},
                    1,
                    session_id="session-mode-drift",
                )
            ]
        )
        drifted = await database.fetchone(
            """
            SELECT desired_mode, runtime_mode, pending_mode,
                   mode_reconciliation_state, mode_drift
            FROM session_bindings WHERE sdk_session_id = 'session-mode-drift'
            """
        )
        await reducer.persist(
            [
                _adapted(
                    "copilotd.mode.pending",
                    {"mode": "autopilot", "transition_id": "mode-transition"},
                    2,
                    source="internal",
                    session_id="session-mode-drift",
                ),
                _adapted(
                    "copilotd.mode.observed",
                    {"mode": "autopilot", "transition_id": "mode-transition"},
                    3,
                    source="internal",
                    session_id="session-mode-drift",
                ),
            ]
        )
        reconciled = await database.fetchone(
            """
            SELECT desired_mode, runtime_mode, pending_mode,
                   mode_reconciliation_state, mode_drift
            FROM session_bindings WHERE sdk_session_id = 'session-mode-drift'
            """
        )

    assert dict(drifted) == {
        "desired_mode": "interactive",
        "runtime_mode": "autopilot",
        "pending_mode": None,
        "mode_reconciliation_state": "drift",
        "mode_drift": 1,
    }
    assert dict(reconciled) == {
        "desired_mode": "autopilot",
        "runtime_mode": "autopilot",
        "pending_mode": None,
        "mode_reconciliation_state": "synced",
        "mode_drift": 0,
    }


@pytest.mark.asyncio
async def test_model_event_uses_per_field_mask_and_preserves_external_drift(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "model-fields.sqlite3") as database:
        await _insert_projection_binding(database, "session-model-fields")
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_model_config = ?,
                runtime_model_config = ?,
                model_confirmation_mask = ?
            WHERE sdk_session_id = 'session-model-fields'
            """,
            (
                json.dumps(
                    {
                        "modelId": "old-model",
                        "confirmationMask": ["modelId"],
                    }
                ),
                json.dumps(
                    {
                        "modelId": "old-model",
                        "knownFields": ["modelId"],
                    }
                ),
                json.dumps(["modelId"]),
            ),
        )
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _adapted(
                    "session.model_change",
                    {
                        "newModel": "new-model",
                        "reasoningSummary": "concise",
                    },
                    1,
                    session_id="session-model-fields",
                )
            ]
        )
        drifted = await database.fetchone(
            """
            SELECT desired_model_config, runtime_model_config,
                   model_reconciliation_state, model_drift
            FROM session_bindings WHERE sdk_session_id = 'session-model-fields'
            """
        )
        observation = await database.fetchone(
            """
            SELECT model_id, reasoning_summary, known_fields, source
            FROM model_config_observations
            """
        )

    assert json.loads(drifted["desired_model_config"])["modelId"] == "old-model"
    assert json.loads(drifted["runtime_model_config"]) == {
        "knownFields": ["modelId", "reasoningSummary"],
        "modelId": "new-model",
        "reasoningSummary": "concise",
    }
    assert drifted["model_reconciliation_state"] == "drift"
    assert drifted["model_drift"] == 1
    assert dict(observation) == {
        "model_id": "new-model",
        "reasoning_summary": "concise",
        "known_fields": '["modelId", "reasoningSummary"]',
        "source": "session.model_change",
    }


@pytest.mark.asyncio
async def test_rejected_model_change_uses_semantic_mask_for_synced_state(
    tmp_path: Path,
) -> None:
    desired = {
        "modelId": "model-a",
        "confirmationMask": ["modelId"],
    }
    runtime = {
        "modelId": "model-a",
        "reasoningEffort": "high",
        "knownFields": ["modelId", "reasoningEffort"],
    }
    async with Database(tmp_path / "model-rejected.sqlite3") as database:
        await _insert_projection_binding(database, "session-model-rejected")
        await database.execute(
            """
            UPDATE session_bindings
            SET desired_model_config = ?,
                runtime_model_config = ?,
                pending_model_config = ?,
                pending_model_transition_id = 'transition-rejected',
                model_reconciliation_state = 'pending'
            WHERE sdk_session_id = 'session-model-rejected'
            """,
            (
                json.dumps(desired),
                json.dumps(runtime),
                json.dumps(
                    {
                        "modelId": "model-b",
                        "confirmationMask": ["modelId"],
                    }
                ),
            ),
        )
        await JournalReducer(database).persist(
            [
                _adapted(
                    "copilotd.model.rejected",
                    {"transition_id": "transition-rejected"},
                    1,
                    source="internal",
                    session_id="session-model-rejected",
                )
            ]
        )
        row = await database.fetchone(
            """
            SELECT pending_model_config, pending_model_transition_id,
                   model_reconciliation_state, model_drift
            FROM session_bindings
            WHERE sdk_session_id = 'session-model-rejected'
            """
        )

    assert dict(row) == {
        "pending_model_config": None,
        "pending_model_transition_id": None,
        "model_reconciliation_state": "synced",
        "model_drift": 0,
    }


@pytest.mark.asyncio
async def test_stale_rejections_do_not_overwrite_newer_pending_transitions(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "stale-rejections.sqlite3") as database:
        await _insert_projection_binding(database, "session-stale-rejections")
        await database.execute(
            """
            UPDATE session_bindings
            SET pending_mode = 'autopilot',
                pending_mode_transition_id = 'mode-new',
                mode_reconciliation_state = 'pending',
                pending_model_config = '{"modelId":"model-new"}',
                pending_model_transition_id = 'model-new',
                model_reconciliation_state = 'pending',
                pending_session_config_version = 2,
                pending_session_config_hash = 'config-new',
                pending_session_config_transition_id = 'config-new',
                session_config_state = 'pending'
            WHERE sdk_session_id = 'session-stale-rejections'
            """
        )
        await JournalReducer(database).persist(
            [
                _adapted(
                    "copilotd.mode.rejected",
                    {"transition_id": "mode-old"},
                    1,
                    source="internal",
                    session_id="session-stale-rejections",
                ),
                _adapted(
                    "copilotd.model.rejected",
                    {"transition_id": "model-old"},
                    2,
                    source="internal",
                    session_id="session-stale-rejections",
                ),
                _adapted(
                    "copilotd.config.rejected",
                    {"transition_id": "config-old"},
                    3,
                    source="internal",
                    session_id="session-stale-rejections",
                ),
            ]
        )
        row = await database.fetchone(
            """
            SELECT pending_mode, pending_mode_transition_id,
                   mode_reconciliation_state,
                   pending_model_config, pending_model_transition_id,
                   model_reconciliation_state,
                   pending_session_config_version,
                   pending_session_config_hash,
                   pending_session_config_transition_id,
                   session_config_state
            FROM session_bindings
            WHERE sdk_session_id = 'session-stale-rejections'
            """
        )

    assert dict(row) == {
        "pending_mode": "autopilot",
        "pending_mode_transition_id": "mode-new",
        "mode_reconciliation_state": "pending",
        "pending_model_config": '{"modelId":"model-new"}',
        "pending_model_transition_id": "model-new",
        "model_reconciliation_state": "pending",
        "pending_session_config_version": 2,
        "pending_session_config_hash": "config-new",
        "pending_session_config_transition_id": "config-new",
        "session_config_state": "pending",
    }


@pytest.mark.asyncio
async def test_managed_settings_events_drive_durable_permission_block_state(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "managed-settings.sqlite3") as database:
        await _insert_projection_binding(database, "session-managed-settings")
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _adapted(
                    "session.managed_settings_resolved",
                    {
                        "bypassPermissionsDisabled": True,
                        "failClosed": False,
                        "managedKeys": ["bypassPermissions"],
                    },
                    1,
                    session_id="session-managed-settings",
                )
            ]
        )
        blocked = await database.fetchone(
            """
            SELECT managed_settings_state, managed_permissions_blocked,
                   permission_posture
            FROM session_bindings
            WHERE sdk_session_id = 'session-managed-settings'
            """
        )
        await reducer.persist(
            [
                _adapted(
                    "session.managed_settings_resolved",
                    {
                        "bypassPermissionsDisabled": False,
                        "failClosed": False,
                        "managedKeys": [],
                    },
                    2,
                    session_id="session-managed-settings",
                )
            ]
        )
        unblocked = await database.fetchone(
            """
            SELECT managed_settings_state, managed_permissions_blocked,
                   permission_posture
            FROM session_bindings
            WHERE sdk_session_id = 'session-managed-settings'
            """
        )

    assert dict(blocked) == {
        "managed_settings_state": "resolved",
        "managed_permissions_blocked": 1,
        "permission_posture": "platform_blocked",
    }
    assert dict(unblocked) == {
        "managed_settings_state": "resolved",
        "managed_permissions_blocked": 0,
        "permission_posture": "unverified",
    }


@pytest.mark.asyncio
async def test_session_idle_is_the_supported_agent_loop_observation(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "agent-loop-idle.sqlite3") as database:
        await _insert_projection_binding(database, "session-agent-loop")
        await JournalReducer(database).persist(
            [
                _adapted(
                    "session.idle",
                    {"aborted": False},
                    1,
                    session_id="session-agent-loop",
                )
            ]
        )
        row = await database.fetchone(
            """
            SELECT state, stop_reason, source_hook_audit_id,
                   source_event_id, stale
            FROM agent_loop_projections
            WHERE sdk_session_id = 'session-agent-loop'
            """
        )

    assert row["state"] == "idle"
    assert row["stop_reason"] == "session_idle"
    assert row["source_hook_audit_id"] is None
    assert row["source_event_id"] is not None
    assert row["stale"] == 0


@pytest.mark.asyncio
async def test_subagent_lifecycle_stays_durable_without_discord_render(tmp_path: Path) -> None:
    started = SessionEvent(
        data=SubagentStartedData(
            agent_description="Investigate the parser",
            agent_display_name="Parser investigator",
            agent_name="parser-agent",
            tool_call_id="tool-agent-1",
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.SUBAGENT_STARTED,
        agent_id="agent-1",
    )
    detail = SessionEvent(
        data=AssistantMessageDeltaData(
            delta_content="Found the parsing edge case.",
            message_id="agent-message-1",
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.ASSISTANT_MESSAGE_DELTA,
        agent_id="agent-1",
    )
    completed = SessionEvent(
        data=SubagentCompletedData(
            agent_display_name="Parser investigator",
            agent_name="parser-agent",
            tool_call_id="tool-agent-1",
            total_tokens=42,
            total_tool_calls=2,
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.SUBAGENT_COMPLETED,
        agent_id="agent-1",
    )
    adapter = EventAdapter()
    adapted = [
        adapter.adapt(
            InboxEnvelope(
                sdk_session_id="session-subagent",
                generation=1,
                fence_token=9,
                inbox_seq=index,
                source="sdk",
                payload=event,
                received_at=100 + index,
                sdk_receive_seq=index,
            )
        )
        for index, event in enumerate((started, detail, completed), start=1)
    ]

    async with Database(tmp_path / "subagent-lifecycle.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 3
        outbox = await database.fetchall(
            "SELECT lane, coalesce_key, payload FROM render_outbox ORDER BY logical_seq"
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE sdk_session_id = 'session-subagent'"
        )
        journal = await database.fetchone(
            "SELECT COUNT(*) FROM event_journal WHERE sdk_session_id = 'session-subagent'"
        )

    assert outbox == []
    assert lease["state"] == "released"
    assert journal[0] == 3


@pytest.mark.asyncio
async def test_user_message_correlation_retains_facts_and_creates_runtime_observed_rows(
    tmp_path: Path,
) -> None:
    session_id = "session-correlation"
    prompt = "same prompt"
    async with Database(tmp_path / "correlation.sqlite3") as database:
        await CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        await _insert_projection_binding(database, session_id)
        queued = _adapted(
            "copilotd.submission.queued",
            {
                "submission_id": "submission-app",
                "thread_id": "thread-projection",
                "origin": "app_message",
                "prompt": prompt,
                "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                "requested_mode": "interactive",
                "requested_delivery": "enqueue",
                "attachment_count": 3,
                "discord_source_channel_id": "channel-1",
                "discord_source_message_id": "message-1",
            },
            1,
            source="internal",
            session_id=session_id,
        )
        accepted = _adapted(
            "copilotd.submission.accepted",
            {"submission_id": "submission-app", "message_id": "accepted-not-event-id"},
            2,
            source="internal",
            session_id=session_id,
        )
        observed = _adapted(
            "user.message",
            {
                "content": prompt,
                "agentMode": "interactive",
                "delivery": "queued",
                "attachments": [{}, {}, {}],
            },
            3,
            session_id=session_id,
        )
        external = _adapted(
            "user.message",
            {
                "content": "remote message",
                "agentMode": "interactive",
                "delivery": "idle",
            },
            4,
            session_id=session_id,
        )
        assert await JournalReducer(database).persist([queued, accepted, observed, external]) == 4
        submissions = await database.fetchall(
            """
            SELECT origin, state, observed_user_event_id, correlation_basis
            FROM submissions WHERE sdk_session_id = ? ORDER BY origin
            """,
            (session_id,),
        )
        segments = await database.fetchall(
            """
            SELECT submission_id, segment_index, user_event_id
            FROM submission_segments ORDER BY observed_at
            """
        )
        reaction = await database.fetchone(
            """
            SELECT desired_state, terminal FROM submission_reactions
            WHERE submission_id = 'submission-app'
            """
        )
        incident = await database.fetchone(
            """
            SELECT kind FROM runtime_incidents
            WHERE session_id = ? AND kind = 'accepted_user_event_id_mapping_mismatch'
            """,
            (session_id,),
        )

    assert [dict(row) for row in submissions] == [
        {
            "origin": "app_message",
            "state": "observed_active",
            "observed_user_event_id": observed.event_id,
            "correlation_basis": "single_candidate_facts_after_acceptance_id_mismatch",
        },
        {
            "origin": "runtime_observed",
            "state": "observed_active",
            "observed_user_event_id": external.event_id,
            "correlation_basis": "runtime_observed",
        },
    ]
    assert [row["segment_index"] for row in segments] == [1, 1]
    assert [row["user_event_id"] for row in segments] == [
        observed.event_id,
        external.event_id,
    ]
    assert dict(reaction) == {"desired_state": "reasoning", "terminal": 0}
    assert incident["kind"] == "accepted_user_event_id_mapping_mismatch"


@pytest.mark.asyncio
async def test_late_user_correlation_recovers_submission_reaction_and_schedule(
    tmp_path: Path,
) -> None:
    session_id = "session-late-correlation"
    prompt = "scheduled prompt"
    async with Database(tmp_path / "late-correlation.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        await database.execute(
            """
            INSERT INTO schedules(
                id, thread_id, kind, expression, timezone, payload,
                target_snapshot, misfire_policy, state, created_at, updated_at
            ) VALUES (
                'schedule-late', 'thread-projection', 'message', 'cron:0 9 * * *',
                'UTC', '{}', '{}', 'latest', 'enabled', 1, 1
            )
            """
        )
        await database.execute(
            """
            INSERT INTO schedule_runs(
                run_id, schedule_id, planned_key, planned_at_utc, status,
                created_at, updated_at
            ) VALUES ('run-late', 'schedule-late', 'manual:1', 1, 'accepted', 1, 1)
            """
        )
        reducer = JournalReducer(database)
        queued = _adapted(
            "copilotd.submission.queued",
            {
                "submission_id": "submission-late",
                "thread_id": "thread-projection",
                "origin": "app_schedule",
                "prompt": prompt,
                "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                "requested_mode": "interactive",
                "attachment_count": 0,
                "discord_source_channel_id": "channel-late",
                "discord_source_message_id": "message-late",
            },
            1,
            source="internal",
            session_id=session_id,
        )
        accepted = _adapted(
            "copilotd.submission.accepted",
            {"submission_id": "submission-late", "message_id": "accepted-late"},
            2,
            source="internal",
            session_id=session_id,
        )
        assert await reducer.persist([queued, accepted]) == 2
        await database.execute(
            """
            UPDATE message_queue SET schedule_run_id = 'run-late'
            WHERE id = 'submission-late'
            """
        )
        await database.execute(
            """
            UPDATE schedule_runs
            SET result_submission_id = 'submission-late',
                result_session_id = ?, accepted_message_id = 'accepted-late'
            WHERE run_id = 'run-late'
            """,
            (session_id,),
        )
        assert (
            await reducer.persist(
                [_adapted("session.idle", {"aborted": False}, 3, session_id=session_id)]
            )
            == 1
        )
        await database.execute(
            """
            UPDATE render_outbox SET state = 'sending'
            WHERE id = (
                SELECT render_intent_id FROM schedule_runs WHERE run_id = 'run-late'
            )
            """
        )
        initial_render = await database.fetchone(
            """
            SELECT payload_revision FROM render_outbox
            WHERE id = (
                SELECT render_intent_id FROM schedule_runs WHERE run_id = 'run-late'
            )
            """
        )
        assert (
            await reducer.persist(
                [
                    _adapted(
                        "user.message",
                        {"content": prompt, "agentMode": "interactive"},
                        4,
                        session_id=session_id,
                    )
                ]
            )
            == 1
        )
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = 'submission-late'"
        )
        reaction = await database.fetchone(
            """
            SELECT desired_state, terminal FROM submission_reactions
            WHERE submission_id = 'submission-late'
            """
        )
        lease = await database.fetchone(
            """
            SELECT state FROM liveness_leases
            WHERE sdk_session_id = ? AND lease_id = 'submission:submission-late'
            """,
            (session_id,),
        )
        schedule = await database.fetchone(
            """
            SELECT status, completion_basis, error_code, terminal_at
            FROM schedule_runs WHERE run_id = 'run-late'
            """
        )
        schedule_render = await database.fetchone(
            """
            SELECT state, payload, payload_revision FROM render_outbox
            WHERE id = (
                SELECT render_intent_id FROM schedule_runs WHERE run_id = 'run-late'
            )
            """
        )
        await SchedulerRepository(database).finalize(
            "run-late",
            ScheduleRunState.SEMANTIC_COMPLETE,
            completion_basis="loop_idle",
            now=10,
        )
        terminal_schedule = await database.fetchone(
            "SELECT status FROM schedule_runs WHERE run_id = 'run-late'"
        )
        terminal_render = await database.fetchone(
            """
            SELECT state, payload, payload_revision FROM render_outbox
            WHERE id = (
                SELECT render_intent_id FROM schedule_runs WHERE run_id = 'run-late'
            )
            """
        )

    assert submission["state"] == "observed_active"
    assert dict(reaction) == {"desired_state": "reasoning", "terminal": 0}
    assert lease["state"] == "active"
    assert dict(schedule) == {
        "status": "accepted",
        "completion_basis": None,
        "error_code": None,
        "terminal_at": None,
    }
    assert schedule_render["state"] == "sending"
    assert schedule_render["payload_revision"] == initial_render["payload_revision"] + 1
    payload = json.loads(str(schedule_render["payload"]))
    assert payload["finalized"] is False
    assert payload["schedule_run"]["status"] == "accepted"
    assert terminal_schedule["status"] == "semantic_complete"
    assert terminal_render["state"] == "sending"
    assert terminal_render["payload_revision"] == schedule_render["payload_revision"] + 1
    terminal_payload = json.loads(str(terminal_render["payload"]))
    assert terminal_payload["finalized"] is True
    assert terminal_payload["schedule_run"]["status"] == "semantic_complete"


@pytest.mark.asyncio
async def test_idle_settles_uniquely_correlated_acceptance_without_blocking_local_queue(
    tmp_path: Path,
) -> None:
    session_id = "session-acceptance-mismatch-idle"
    prompt = "first prompt"
    accepted_id = str(uuid4())
    async with Database(tmp_path / "acceptance-mismatch-idle.sqlite3") as database:
        await CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-first",
                    "thread_id": "thread-projection",
                    "origin": "app_message",
                    "prompt": prompt,
                    "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                    "requested_mode": "interactive",
                    "requested_delivery": "enqueue",
                    "attachment_count": 0,
                    "created_at": 101,
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {
                    "submission_id": "submission-first",
                    "message_id": accepted_id,
                },
                2,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "user.message",
                {"content": prompt, "agentMode": "interactive"},
                3,
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-second",
                    "thread_id": "thread-projection",
                    "origin": "app_message",
                    "prompt": "second prompt",
                    "prompt_hash": hashlib.sha256(b"second prompt").hexdigest(),
                    "requested_mode": "interactive",
                    "requested_delivery": "enqueue",
                    "attachment_count": 0,
                    "created_at": 104,
                },
                4,
                source="internal",
                session_id=session_id,
            ),
            _adapted("session.idle", {"aborted": False}, 5, session_id=session_id),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submissions = await database.fetchall(
            """
            SELECT submission_id, origin, state, completion_basis
            FROM submissions WHERE sdk_session_id = ?
            ORDER BY submission_id
            """,
            (session_id,),
        )
        queue = await database.fetchall("SELECT id, state FROM message_queue ORDER BY id")
        leases = await database.fetchall(
            """
            SELECT source_id, state FROM liveness_leases
            WHERE sdk_session_id = ? AND kind = 'submission'
            ORDER BY source_id
            """,
            (session_id,),
        )
        incident = await database.fetchone(
            """
            SELECT kind FROM runtime_incidents
            WHERE session_id = ? AND kind = 'accepted_user_event_id_mapping_mismatch'
            """,
            (session_id,),
        )

    app_submissions = {
        row["submission_id"]: dict(row) for row in submissions if row["origin"] == "app_message"
    }
    runtime_submissions = [dict(row) for row in submissions if row["origin"] == "runtime_observed"]
    assert app_submissions == {
        "submission-first": {
            "submission_id": "submission-first",
            "origin": "app_message",
            "state": "loop_idle",
            "completion_basis": None,
        },
        "submission-second": {
            "submission_id": "submission-second",
            "origin": "app_message",
            "state": "local_queued",
            "completion_basis": None,
        },
    }
    assert runtime_submissions == []
    assert [dict(row) for row in queue] == [
        {"id": "submission-first", "state": "submitted"},
        {"id": "submission-second", "state": "local_queued"},
    ]
    assert [dict(row) for row in leases if row["source_id"].startswith("submission-")] == [
        {"source_id": "submission-first", "state": "active"},
        {"source_id": "submission-second", "state": "active"},
    ]
    assert incident["kind"] == "accepted_user_event_id_mapping_mismatch"


@pytest.mark.asyncio
async def test_exact_runtime_schedule_event_bypasses_app_correlation_and_settles_one_shot(
    tmp_path: Path,
) -> None:
    session_id = "session-native-schedule"
    prompt = "same scheduled prompt"
    async with Database(tmp_path / "native-schedule-correlation.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, recurrence, next_run_at, state, updated_at
            ) VALUES (?, 'native-once', 'after', '', NULL, 100,
                      'active', 1)
            """,
            (session_id,),
        )
        queued = _adapted(
            "copilotd.submission.queued",
            {
                "submission_id": "submission-app",
                "thread_id": "thread-projection",
                "origin": "app_message",
                "prompt": prompt,
                "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                "requested_mode": "interactive",
                "requested_delivery": "enqueue",
                "attachment_count": 0,
            },
            1,
            source="internal",
            session_id=session_id,
        )
        accepted = _adapted(
            "copilotd.submission.accepted",
            {"submission_id": "submission-app", "message_id": "accepted-app"},
            2,
            source="internal",
            session_id=session_id,
        )
        native = _adapted(
            "user.message",
            {"content": prompt, "agentMode": "interactive", "delivery": "queued"},
            3,
            session_id=session_id,
        )
        native = replace(
            native,
            raw_payload={
                "type": "user.message",
                "runtimeScheduleId": "native-once",
                "data": native.raw_payload["data"],
            },
        )
        snapshot_requested = _adapted(
            "copilotd.snapshot.requested",
            {"topic": "schedules"},
            4,
            source="internal",
            session_id=session_id,
        )
        snapshot_omission = _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "schedules",
                "epoch": 1,
                "snapshot_id": "schedule-snapshot-1",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {"schedules": []},
                "observed_at": 105,
            },
            5,
            source="snapshot",
            session_id=session_id,
        )

        assert (
            await JournalReducer(database).persist(
                [
                    queued,
                    accepted,
                    native,
                    snapshot_requested,
                    snapshot_omission,
                ]
            )
            == 5
        )
        app_submission = await database.fetchone(
            """
            SELECT state, observed_user_event_id
            FROM submissions WHERE submission_id = 'submission-app'
            """
        )
        runtime_submission = await database.fetchone(
            """
            SELECT origin, runtime_schedule_id, observed_user_event_id
            FROM submissions
            WHERE sdk_session_id = ? AND origin = 'runtime_observed'
            """,
            (session_id,),
        )
        native_schedule = await database.fetchone(
            """
            SELECT state, next_run_at FROM runtime_schedules
            WHERE sdk_session_id = ? AND runtime_schedule_id = 'native-once'
            """,
            (session_id,),
        )
        close_blockers = await database.fetchone(
            """
            SELECT COUNT(*) FROM runtime_schedules
            WHERE sdk_session_id = ? AND state IN ('active', 'unknown')
            """,
            (session_id,),
        )

    assert dict(app_submission) == {
        "state": "submitted",
        "observed_user_event_id": None,
    }
    assert dict(runtime_submission) == {
        "origin": "runtime_observed",
        "runtime_schedule_id": "native-once",
        "observed_user_event_id": native.event_id,
    }
    assert dict(native_schedule) == {
        "state": "triggered",
        "next_run_at": None,
    }
    assert close_blockers[0] == 0


@pytest.mark.asyncio
async def test_native_one_shot_trigger_before_inventory_reconciles_when_row_arrives(
    tmp_path: Path,
) -> None:
    session_id = "session-trigger-before-inventory"
    async with Database(tmp_path / "trigger-before-inventory.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        native = _adapted(
            "user.message",
            {"content": "scheduled early", "agentMode": "interactive"},
            1,
            session_id=session_id,
        )
        native = replace(
            native,
            raw_payload={
                "type": "user.message",
                "runtimeScheduleId": "native-before-inventory",
                "data": native.raw_payload["data"],
            },
        )
        reducer = JournalReducer(database)
        assert await reducer.persist([native]) == 1
        pending = await database.fetchone(
            """
            SELECT user_event_id FROM pending_runtime_schedule_triggers
            WHERE sdk_session_id = ? AND runtime_schedule_id =
                  'native-before-inventory'
            """,
            (session_id,),
        )
        requested = _adapted(
            "copilotd.snapshot.requested",
            {"topic": "schedules"},
            2,
            source="internal",
            session_id=session_id,
        )
        observed = _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "schedules",
                "epoch": 1,
                "snapshot_id": "native-before-snapshot",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {
                    "schedules": [
                        {
                            "id": "native-before-inventory",
                            "builtinName": "after",
                            "input": "once",
                            "recurrence": None,
                            "nextRunAt": 100,
                            "state": "active",
                        }
                    ]
                },
                "observed_at": 105,
            },
            3,
            source="snapshot",
            session_id=session_id,
        )

        assert await reducer.persist([requested, observed]) == 2
        schedule = await database.fetchone(
            """
            SELECT state, next_run_at FROM runtime_schedules
            WHERE sdk_session_id = ? AND runtime_schedule_id =
                  'native-before-inventory'
            """,
            (session_id,),
        )
        remaining = await database.fetchone(
            """
            SELECT COUNT(*) FROM pending_runtime_schedule_triggers
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )

    assert pending["user_event_id"] == native.event_id
    assert dict(schedule) == {"state": "triggered", "next_run_at": None}
    assert remaining[0] == 0


@pytest.mark.asyncio
async def test_ambiguous_duplicate_prompt_does_not_pollute_app_submissions(
    tmp_path: Path,
) -> None:
    session_id = "session-ambiguous"
    prompt = "duplicate"
    async with Database(tmp_path / "ambiguous.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        events: list[AdaptedEvent] = []
        for index, submission_id in enumerate(("submission-a", "submission-b"), start=1):
            events.extend(
                [
                    _adapted(
                        "copilotd.submission.queued",
                        {
                            "submission_id": submission_id,
                            "thread_id": "thread-projection",
                            "origin": "app_message",
                            "prompt": prompt,
                            "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                            "requested_mode": "interactive",
                            "requested_delivery": "enqueue",
                        },
                        index * 2 - 1,
                        source="internal",
                        session_id=session_id,
                    ),
                    _adapted(
                        "copilotd.submission.accepted",
                        {
                            "submission_id": submission_id,
                            "message_id": f"accepted-{index}",
                        },
                        index * 2,
                        source="internal",
                        session_id=session_id,
                    ),
                ]
            )
        observed = _adapted(
            "user.message",
            {"content": prompt, "agentMode": "interactive"},
            5,
            session_id=session_id,
        )
        assert await JournalReducer(database).persist([*events, observed]) == 5
        app_states = await database.fetchall(
            """
            SELECT state FROM submissions
            WHERE sdk_session_id = ? AND origin = 'app_message' ORDER BY submission_id
            """,
            (session_id,),
        )
        runtime_row = await database.fetchone(
            """
            SELECT state FROM submissions
            WHERE sdk_session_id = ? AND origin = 'runtime_observed'
            """,
            (session_id,),
        )
        incident = await database.fetchone(
            """
            SELECT kind FROM runtime_incidents
            WHERE session_id = ? ORDER BY id DESC LIMIT 1
            """,
            (session_id,),
        )

    assert [row["state"] for row in app_states] == ["submitted", "submitted"]
    assert runtime_row["state"] == "observed_active"
    assert incident["kind"] == "user_message_correlation_ambiguous"


@pytest.mark.asyncio
async def test_overflow_unknown_replay_reuses_retained_acceptance_facts(
    tmp_path: Path,
) -> None:
    session_id = "session-overflow-replay"
    prompt = "accepted before overflow"
    accepted_id = str(uuid4())
    async with Database(tmp_path / "overflow-replay.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-overflow",
                    "thread_id": "thread-projection",
                    "origin": "app_message",
                    "prompt": prompt,
                    "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                    "requested_mode": "interactive",
                    "requested_delivery": "enqueue",
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {
                    "submission_id": "submission-overflow",
                    "message_id": accepted_id,
                },
                2,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.active_unknown",
                {"observed_at": 103},
                3,
                source="internal",
                session_id=session_id,
            ),
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=1,
                fence_token=7,
                inbox_seq=4,
                source="sdk",
                raw_type="user.message",
                raw_payload={
                    "type": "user.message",
                    "data": {"content": prompt, "agentMode": "interactive"},
                },
                reducer_hash="overflow-replayed-user",
                persistence_class="durable",
                received_at=104,
                event_id=accepted_id,
            ),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submissions = await database.fetchall(
            """
            SELECT origin, state, accepted_message_id, observed_user_event_id,
                   correlation_basis
            FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )

    assert [dict(row) for row in submissions] == [
        {
            "origin": "app_message",
            "state": "observed_active",
            "accepted_message_id": accepted_id,
            "observed_user_event_id": accepted_id,
            "correlation_basis": "single_candidate_facts",
        }
    ]


@pytest.mark.asyncio
async def test_acceptance_receipts_are_monotonic_across_unknown_and_duplicates(
    tmp_path: Path,
) -> None:
    session_id = "session-acceptance-monotonic"
    message_id = str(uuid4())
    conflicting_id = str(uuid4())
    async with Database(tmp_path / "acceptance-monotonic.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-1",
                    "thread_id": "thread-projection",
                    "prompt": "hello",
                    "prompt_hash": hashlib.sha256(b"hello").hexdigest(),
                    "requested_mode": "interactive",
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.acceptance_unknown",
                {"submission_id": "submission-1"},
                2,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {"submission_id": "submission-1", "message_id": message_id},
                3,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.acceptance_unknown",
                {"submission_id": "submission-1"},
                4,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {"submission_id": "submission-1", "message_id": message_id},
                5,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {"submission_id": "submission-1", "message_id": conflicting_id},
                6,
                source="internal",
                session_id=session_id,
            ),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submission = await database.fetchone(
            """
            SELECT state, accepted_message_id, accepted_at, terminal_at
            FROM submissions WHERE submission_id = 'submission-1'
            """
        )
        queue = await database.fetchone("SELECT state FROM message_queue WHERE id = 'submission-1'")
        lease = await database.fetchone(
            """
            SELECT state FROM liveness_leases
            WHERE sdk_session_id = ? AND source_id = 'submission-1'
            """,
            (session_id,),
        )
        incident = await database.fetchone(
            """
            SELECT kind, detail FROM runtime_incidents
            WHERE session_id = ? AND kind = 'submission_acceptance_conflict'
            """,
            (session_id,),
        )

    assert dict(submission) == {
        "state": "submitted",
        "accepted_message_id": message_id,
        "accepted_at": 103,
        "terminal_at": None,
    }
    assert queue["state"] == "submitted"
    assert lease["state"] == "active"
    assert incident["kind"] == "submission_acceptance_conflict"
    assert conflicting_id in incident["detail"]


@pytest.mark.asyncio
async def test_explicit_rejection_moves_early_callback_to_runtime_observed_submission(
    tmp_path: Path,
) -> None:
    session_id = "session-rejected-early-callback"
    prompt = "callback before rejection"
    user_event_id = str(uuid4())
    async with Database(tmp_path / "rejected-early-callback.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-app",
                    "thread_id": "thread-projection",
                    "origin": "app_message",
                    "prompt": prompt,
                    "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
                    "requested_mode": "interactive",
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.operation.pending",
                {
                    "operation_id": "operation-send",
                    "runtime_generation": 1,
                    "owner_fence_token": 7,
                    "kind": "send",
                    "idempotency_key": "send:test",
                    "input_hash": "hash",
                    "created_at": 102,
                },
                2,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.submitting",
                {
                    "submission_id": "submission-app",
                    "operation_id": "operation-send",
                },
                3,
                source="internal",
                session_id=session_id,
            ),
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=1,
                fence_token=7,
                inbox_seq=4,
                source="sdk",
                raw_type="user.message",
                raw_payload={
                    "type": "user.message",
                    "data": {"content": prompt, "agentMode": "interactive"},
                },
                reducer_hash="early-user-message",
                persistence_class="durable",
                received_at=104,
                event_id=user_event_id,
            ),
            _adapted(
                "copilotd.submission.rejected",
                {"submission_id": "submission-app"},
                5,
                source="internal",
                session_id=session_id,
            ),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submissions = await database.fetchall(
            """
            SELECT submission_id, origin, state, accepted_message_id,
                   accepted_at, observed_user_event_id, correlation_basis
            FROM submissions WHERE sdk_session_id = ? ORDER BY origin
            """,
            (session_id,),
        )
        segment = await database.fetchone(
            "SELECT submission_id FROM submission_segments WHERE user_event_id = ?",
            (user_event_id,),
        )
        lease = await database.fetchone(
            """
            SELECT source_id, state FROM liveness_leases
            WHERE sdk_session_id = ? AND kind = 'submission'
            """,
            (session_id,),
        )
        queue = await database.fetchone(
            "SELECT state FROM message_queue WHERE id = 'submission-app'"
        )

    assert [dict(row) for row in submissions] == [
        {
            "submission_id": "submission-app",
            "origin": "app_message",
            "state": "rejected",
            "accepted_message_id": None,
            "accepted_at": None,
            "observed_user_event_id": None,
            "correlation_basis": None,
        },
        {
            "submission_id": segment["submission_id"],
            "origin": "runtime_observed",
            "state": "observed_active",
            "accepted_message_id": None,
            "accepted_at": None,
            "observed_user_event_id": user_event_id,
            "correlation_basis": "rejected_send_runtime_observed",
        },
    ]
    assert lease["source_id"] == segment["submission_id"]
    assert lease["state"] == "active"
    assert queue["state"] == "rejected"


@pytest.mark.asyncio
async def test_definitive_acceptance_and_rejection_conflicts_are_monotonic(
    tmp_path: Path,
) -> None:
    session_id = "session-receipt-conflicts"
    accepted_id = str(uuid4())
    rejected_then_accepted_id = str(uuid4())
    async with Database(tmp_path / "receipt-conflicts.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "accepted-first",
                    "thread_id": "thread-projection",
                    "prompt": "accepted first",
                    "prompt_hash": hashlib.sha256(b"accepted first").hexdigest(),
                    "requested_mode": "interactive",
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {"submission_id": "accepted-first", "message_id": accepted_id},
                2,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.rejected",
                {"submission_id": "accepted-first"},
                3,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.rejected",
                {"submission_id": "accepted-first"},
                4,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "rejected-first",
                    "thread_id": "thread-projection",
                    "prompt": "rejected first",
                    "prompt_hash": hashlib.sha256(b"rejected first").hexdigest(),
                    "requested_mode": "interactive",
                },
                5,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.rejected",
                {"submission_id": "rejected-first"},
                6,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {
                    "submission_id": "rejected-first",
                    "message_id": rejected_then_accepted_id,
                },
                7,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {
                    "submission_id": "rejected-first",
                    "message_id": rejected_then_accepted_id,
                },
                8,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.acceptance_unknown",
                {"submission_id": "rejected-first"},
                9,
                source="internal",
                session_id=session_id,
            ),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submissions = await database.fetchall(
            """
            SELECT submission_id, state, accepted_message_id
            FROM submissions WHERE sdk_session_id = ? ORDER BY submission_id
            """,
            (session_id,),
        )
        queue = await database.fetchall("SELECT id, state FROM message_queue ORDER BY id")
        incidents = await database.fetchall(
            """
            SELECT kind, COUNT(*) AS count FROM runtime_incidents
            WHERE session_id = ?
            GROUP BY kind ORDER BY kind
            """,
            (session_id,),
        )

    assert [dict(row) for row in submissions] == [
        {
            "submission_id": "accepted-first",
            "state": "submitted",
            "accepted_message_id": accepted_id,
        },
        {
            "submission_id": "rejected-first",
            "state": "rejected",
            "accepted_message_id": None,
        },
    ]
    assert [dict(row) for row in queue] == [
        {"id": "accepted-first", "state": "submitted"},
        {"id": "rejected-first", "state": "rejected"},
    ]
    assert [dict(row) for row in incidents] == [
        {"kind": "submission_acceptance_after_rejection", "count": 1},
        {"kind": "submission_rejection_after_acceptance", "count": 1},
    ]


@pytest.mark.asyncio
async def test_unknown_acceptance_mapping_capability_is_not_treated_as_supported(
    tmp_path: Path,
) -> None:
    session_id = "session-unknown-mapping"
    accepted_id = str(uuid4())
    async with Database(tmp_path / "unknown-mapping.sqlite3") as database:
        await CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        await database.execute(
            """
            UPDATE capabilities
            SET supported = -1, evidence_status = 'unknown'
            WHERE capability = 'accepted_user_event_id_mapping'
            """
        )
        await _insert_projection_binding(database, session_id)
        events = [
            _adapted(
                "copilotd.submission.queued",
                {
                    "submission_id": "submission-app",
                    "thread_id": "thread-projection",
                    "prompt": "different prompt",
                    "prompt_hash": hashlib.sha256(b"different prompt").hexdigest(),
                    "requested_mode": "interactive",
                },
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.submission.accepted",
                {"submission_id": "submission-app", "message_id": accepted_id},
                2,
                source="internal",
                session_id=session_id,
            ),
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=1,
                fence_token=7,
                inbox_seq=3,
                source="sdk",
                raw_type="user.message",
                raw_payload={
                    "type": "user.message",
                    "data": {"content": "actual runtime message"},
                },
                reducer_hash="unknown-mapping-user",
                persistence_class="durable",
                received_at=103,
                event_id=accepted_id,
            ),
        ]
        assert await JournalReducer(database).persist(events) == len(events)
        submissions = await database.fetchall(
            """
            SELECT origin, state FROM submissions
            WHERE sdk_session_id = ? ORDER BY origin
            """,
            (session_id,),
        )

    assert [dict(row) for row in submissions] == [
        {"origin": "app_message", "state": "submitted"},
        {"origin": "runtime_observed", "state": "observed_active"},
    ]


@pytest.mark.asyncio
async def test_model_turn_projection_and_interactive_idle_are_semantically_terminal(
    tmp_path: Path,
) -> None:
    session_id = "session-turns"
    events = [
        _adapted(
            "user.message",
            {
                "content": "runtime root",
                "agentMode": "interactive",
                "interactionId": "interaction-1",
            },
            1,
            session_id=session_id,
        ),
        _adapted(
            "assistant.turn_start",
            {"turnId": "turn-1", "interactionId": "interaction-1"},
            2,
            session_id=session_id,
        ),
        _adapted(
            "assistant.turn_retry",
            {"turnId": "turn-1", "reason": "rate_limit"},
            3,
            session_id=session_id,
        ),
        _adapted(
            "assistant.turn_end",
            {"turnId": "turn-1"},
            4,
            session_id=session_id,
        ),
        _adapted("session.idle", {"aborted": False}, 5, session_id=session_id),
    ]
    async with Database(tmp_path / "turns.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        reducer = JournalReducer(database)
        assert await reducer.persist(events) == len(events)
        snapshots = [
            _adapted(
                "copilotd.snapshot.requested",
                {"topic": "tasks"},
                6,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.observed",
                {
                    "topic": "tasks",
                    "epoch": 1,
                    "snapshot_id": "turns-tasks",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": 0,
                    "observed_at": 107,
                    "payload": {"tasks": []},
                },
                7,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.requested",
                {"topic": "activity"},
                8,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.observed",
                {
                    "topic": "activity",
                    "epoch": 1,
                    "snapshot_id": "turns-activity",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": 0,
                    "observed_at": 109,
                    "payload": {
                        "processing": False,
                        "has_active_work": False,
                        "abortable": False,
                    },
                },
                9,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.requested",
                {"topic": "queue"},
                10,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.observed",
                {
                    "topic": "queue",
                    "epoch": 1,
                    "snapshot_id": "turns-queue",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": 0,
                    "observed_at": 111,
                    "payload": {"items": [], "steering_messages": []},
                },
                11,
                source="internal",
                session_id=session_id,
            ),
        ]
        assert await reducer.persist(snapshots) == len(snapshots)
        turn = await database.fetchone(
            """
            SELECT submission_id, state, retry_count, started_at, ended_at
            FROM model_turns WHERE sdk_turn_id = 'turn-1'
            """
        )
        submission = await database.fetchone(
            """
            SELECT state, completion_basis, terminal_at FROM submissions
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        segment = await database.fetchone("SELECT state, idle_at FROM submission_segments")
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE sdk_session_id = ?",
            (session_id,),
        )

    assert turn["submission_id"] is not None
    assert turn["state"] == "observed_end"
    assert turn["retry_count"] == 1
    assert turn["ended_at"] >= turn["started_at"]
    assert dict(submission) == {
        "state": "semantic_complete",
        "completion_basis": "loop_idle",
        "terminal_at": 111,
    }
    assert dict(segment) == {"state": "semantic_complete", "idle_at": 105}
    assert lease["state"] == "released"


@pytest.mark.asyncio
async def test_autopilot_continue_reopens_then_blocked_idle_settles(
    tmp_path: Path,
) -> None:
    session_id = "session-autopilot"
    events = [
        _adapted(
            "user.message",
            {"content": "autopilot root", "agentMode": "autopilot"},
            1,
            session_id=session_id,
        ),
        _adapted(
            "session.autopilot_objective_changed",
            {"operation": "create", "id": 42, "status": "active"},
            2,
            session_id=session_id,
        ),
        _adapted(
            "session.task_complete",
            {"outcome": "continue", "objectiveId": "42"},
            3,
            session_id=session_id,
        ),
        _adapted("session.idle", {"aborted": False}, 4, session_id=session_id),
        _adapted(
            "user.message",
            {
                "content": "autopilot continuation",
                "agentMode": "autopilot",
                "isAutopilotContinuation": True,
            },
            5,
            session_id=session_id,
        ),
        _adapted(
            "session.task_complete",
            {"outcome": "blocked", "objectiveId": "42"},
            6,
            session_id=session_id,
        ),
        _adapted("session.idle", {"aborted": False}, 7, session_id=session_id),
    ]
    async with Database(tmp_path / "autopilot.sqlite3") as database:
        assert await JournalReducer(database).persist(events) == len(events)
        submission = await database.fetchone(
            """
            SELECT submission_id, state, task_completion_outcome, completion_basis,
                   autopilot_objective_id, continuation_count
            FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        segments = await database.fetchall(
            """
            SELECT segment_index, is_continuation, state
            FROM submission_segments ORDER BY segment_index
            """
        )
        objective = await database.fetchone(
            """
            SELECT objective_id, status, submission_id
            FROM autopilot_objectives WHERE sdk_session_id = ?
            """,
            (session_id,),
        )

    assert dict(submission) == {
        "submission_id": submission["submission_id"],
        "state": "semantic_blocked",
        "task_completion_outcome": "blocked",
        "completion_basis": "task_complete_blocked",
        "autopilot_objective_id": "42",
        "continuation_count": 1,
    }
    assert [dict(row) for row in segments] == [
        {"segment_index": 1, "is_continuation": 0, "state": "continuation_expected"},
        {"segment_index": 2, "is_continuation": 1, "state": "semantic_blocked"},
    ]
    assert objective["objective_id"] == "42"
    assert objective["status"] == "active"
    assert objective["submission_id"] == submission["submission_id"]


@pytest.mark.asyncio
async def test_abort_projection_waits_for_aborted_idle_and_closes_turn(
    tmp_path: Path,
) -> None:
    session_id = "session-abort"
    events = [
        _adapted(
            "user.message",
            {"content": "runtime root", "agentMode": "interactive"},
            1,
            session_id=session_id,
        ),
        _adapted(
            "assistant.turn_start",
            {"turnId": "turn-abort"},
            2,
            session_id=session_id,
        ),
        _adapted("abort", {"reason": "user"}, 3, session_id=session_id),
        _adapted("session.idle", {"aborted": True}, 4, session_id=session_id),
    ]
    async with Database(tmp_path / "abort.sqlite3") as database:
        assert await JournalReducer(database).persist(events) == len(events)
        submission = await database.fetchone(
            """
            SELECT state, abort_event_id, completion_basis
            FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        turn = await database.fetchone(
            "SELECT state, ended_at FROM model_turns WHERE sdk_turn_id = 'turn-abort'"
        )

    assert submission["state"] == "observed_aborted"
    assert submission["abort_event_id"] == events[2].event_id
    assert submission["completion_basis"] == "session_idle_aborted"
    assert turn["state"] == "aborted"
    assert turn["ended_at"] == 103


@pytest.mark.asyncio
async def test_reducer_background_lease_is_counted_by_heartbeat(tmp_path: Path) -> None:
    session_id = "session-background-heartbeat"
    async with Database(tmp_path / "background-heartbeat.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-background-heartbeat",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET attachment_state = 'attached', runtime_generation = 2,
                owner_fence_token = 8
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at, released_at
            ) VALUES (
                ?, 'background:task:task-heartbeat', 'background',
                'task:task-heartbeat', 1, 7, 'orphaned', 100, 150, 150
            )
            """,
            (session_id,),
        )
        events = [
            _adapted(
                "copilotd.snapshot.requested",
                {"topic": "tasks"},
                1,
                source="internal",
                session_id=session_id,
                generation=2,
                fence_token=8,
            ),
            _adapted(
                "copilotd.snapshot.observed",
                {
                    "topic": "tasks",
                    "epoch": 1,
                    "snapshot_id": "background-heartbeat-snapshot",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": 0,
                    "payload": {
                        "tasks": [
                            {
                                "id": "task-heartbeat",
                                "status": "running",
                                "type": "agent",
                                "description": "Long-running worker",
                            }
                        ]
                    },
                },
                2,
                source="internal",
                session_id=session_id,
                generation=2,
                fence_token=8,
            ),
        ]
        assert await JournalReducer(database).persist(events) == 2
        writer = HeartbeatWriter(
            database,
            tmp_path / "heartbeat.json",
            resume_provider=lambda: None,
        )
        snapshot = await writer.snapshot(now=200)
        lease = await database.fetchone(
            """
            SELECT kind, source_id, runtime_generation, owner_fence_token,
                   state, acquired_at, released_at
            FROM liveness_leases
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )

    assert {
        key: lease[key]
        for key in (
            "kind",
            "source_id",
            "runtime_generation",
            "owner_fence_token",
            "state",
            "released_at",
        )
    } == {
        "kind": "observed_background",
        "source_id": "task:task-heartbeat",
        "runtime_generation": 2,
        "owner_fence_token": 8,
        "state": "active",
        "released_at": None,
    }
    assert lease["acquired_at"] > 150
    assert snapshot.observed_background_tasks == 1


@pytest.mark.asyncio
async def test_snapshot_epochs_suppress_stale_negative_and_keep_terminal_monotonic(
    tmp_path: Path,
) -> None:
    session_id = "session-snapshot-order"
    events = [
        _adapted(
            "copilotd.snapshot.requested",
            {"topic": "tasks"},
            1,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "tasks",
                "epoch": 1,
                "snapshot_id": "snapshot-1",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {
                    "tasks": [
                        {
                            "id": "task-1",
                            "status": "running",
                            "type": "agent",
                            "description": "Worker",
                        }
                    ]
                },
            },
            2,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.requested",
            {"topic": "tasks"},
            3,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.requested",
            {"topic": "tasks"},
            4,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "tasks",
                "epoch": 2,
                "snapshot_id": "snapshot-stale-empty",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {"tasks": []},
            },
            5,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "tasks",
                "epoch": 3,
                "snapshot_id": "snapshot-terminal",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {
                    "tasks": [
                        {
                            "id": "task-1",
                            "status": "completed",
                            "type": "agent",
                            "description": "Worker",
                        }
                    ]
                },
            },
            6,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.requested",
            {"topic": "tasks"},
            7,
            source="internal",
            session_id=session_id,
        ),
        _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "tasks",
                "epoch": 4,
                "snapshot_id": "snapshot-late-running",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "payload": {
                    "tasks": [
                        {
                            "id": "task-1",
                            "status": "running",
                            "type": "agent",
                            "description": "Worker",
                        }
                    ]
                },
            },
            8,
            source="internal",
            session_id=session_id,
        ),
    ]
    async with Database(tmp_path / "snapshot-order.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        assert await JournalReducer(database).persist(events) == len(events)
        observation = await database.fetchone(
            """
            SELECT observed_state, terminal_evidence
            FROM background_observations WHERE task_id = 'task-1'
            """
        )
        reconciliation = await database.fetchone(
            """
            SELECT requested_epoch, applied_epoch, status
            FROM reconciliation_state
            WHERE sdk_session_id = ? AND topic = 'tasks'
            """,
            (session_id,),
        )
        stale = await database.fetchone(
            """
            SELECT negative_applied FROM snapshot_observations
            WHERE snapshot_id = 'snapshot-stale-empty'
            """
        )

    assert dict(observation) == {
        "observed_state": "completed",
        "terminal_evidence": "task_snapshot",
    }
    assert dict(reconciliation) == {
        "requested_epoch": 4,
        "applied_epoch": 4,
        "status": "idle",
    }
    assert stale["negative_applied"] == 0


@pytest.mark.asyncio
async def test_linked_task_terminal_waits_for_late_activity_and_queue_quiet_snapshots(
    tmp_path: Path,
) -> None:
    session_id = "session-linked-task"

    def requested(topic: str, seq: int) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.requested",
            {"topic": topic},
            seq,
            source="internal",
            session_id=session_id,
        )

    def observed(
        topic: str,
        seq: int,
        epoch: int,
        payload: dict[str, Any],
    ) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": topic,
                "epoch": epoch,
                "snapshot_id": f"{topic}-{epoch}-{seq}",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "observed_at": 100 + seq,
                "payload": payload,
            },
            seq,
            source="internal",
            session_id=session_id,
        )

    async with Database(tmp_path / "linked-task-quiet.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        reducer = JournalReducer(database)
        assert (
            await reducer.persist(
                [
                    _adapted(
                        "user.message",
                        {"content": "start worker", "agentMode": "interactive"},
                        1,
                        session_id=session_id,
                    ),
                    requested("tasks", 2),
                    observed(
                        "tasks",
                        3,
                        1,
                        {
                            "tasks": [
                                {
                                    "id": "task-1",
                                    "status": "running",
                                    "type": "agent",
                                    "description": "Worker",
                                }
                            ]
                        },
                    ),
                    _adapted(
                        "session.idle",
                        {"aborted": False},
                        4,
                        session_id=session_id,
                    ),
                ]
            )
            == 4
        )
        initial = await database.fetchone(
            """
            SELECT state FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        link = await database.fetchone(
            """
            SELECT submission_id, state, correlation_basis
            FROM submission_task_links WHERE task_id = 'task-1'
            """
        )
        assert initial["state"] == "loop_idle"
        assert link["state"] == "running"
        assert link["correlation_basis"] == "single_active_submission"

        assert (
            await reducer.persist(
                [
                    requested("tasks", 5),
                    observed(
                        "tasks",
                        6,
                        2,
                        {
                            "tasks": [
                                {
                                    "id": "task-1",
                                    "status": "completed",
                                    "type": "agent",
                                    "description": "Worker",
                                }
                            ]
                        },
                    ),
                ]
            )
            == 2
        )
        after_task = await database.fetchone(
            "SELECT state FROM submissions WHERE sdk_session_id = ?",
            (session_id,),
        )
        assert after_task["state"] == "loop_idle"

        assert (
            await reducer.persist(
                [
                    requested("activity", 7),
                    observed(
                        "activity",
                        8,
                        1,
                        {
                            "processing": False,
                            "has_active_work": False,
                            "abortable": False,
                        },
                    ),
                ]
            )
            == 2
        )
        after_activity = await database.fetchone(
            "SELECT state FROM submissions WHERE sdk_session_id = ?",
            (session_id,),
        )
        assert after_activity["state"] == "loop_idle"

        assert (
            await reducer.persist(
                [
                    requested("queue", 9),
                    observed(
                        "queue",
                        10,
                        1,
                        {"items": [], "steering_messages": []},
                    ),
                ]
            )
            == 2
        )
        terminal = await database.fetchone(
            """
            SELECT state, completion_basis FROM submissions
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        segment = await database.fetchone(
            "SELECT state FROM submission_segments WHERE submission_id = ?",
            (link["submission_id"],),
        )
        leases = await database.fetchall(
            """
            SELECT kind, state FROM liveness_leases
            WHERE sdk_session_id = ? ORDER BY kind
            """,
            (session_id,),
        )

    assert dict(terminal) == {
        "state": "semantic_complete",
        "completion_basis": "tasks_terminal_quiet",
    }
    assert segment["state"] == "semantic_complete"
    assert [dict(row) for row in leases] == [
        {"kind": "observed_background", "state": "released"},
        {"kind": "submission", "state": "released"},
    ]


@pytest.mark.asyncio
async def test_task_discovered_after_quiet_completion_reopens_latest_submission(
    tmp_path: Path,
) -> None:
    session_id = "session-truly-late-task"

    def requested(topic: str, seq: int) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.requested",
            {"topic": topic},
            seq,
            source="internal",
            session_id=session_id,
        )

    def observed(
        topic: str,
        seq: int,
        epoch: int,
        payload: dict[str, Any],
    ) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": topic,
                "epoch": epoch,
                "snapshot_id": f"late-{topic}-{epoch}-{seq}",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "observed_at": 100 + seq,
                "payload": payload,
            },
            seq,
            source="internal",
            session_id=session_id,
        )

    async with Database(tmp_path / "truly-late-task.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        reducer = JournalReducer(database)
        initial = [
            _adapted(
                "user.message",
                {"content": "maybe background", "agentMode": "interactive"},
                1,
                session_id=session_id,
            ),
            _adapted(
                "session.idle",
                {"aborted": False},
                2,
                session_id=session_id,
            ),
            requested("tasks", 3),
            observed("tasks", 4, 1, {"tasks": []}),
            requested("activity", 5),
            observed(
                "activity",
                6,
                1,
                {
                    "processing": False,
                    "has_active_work": False,
                    "abortable": False,
                },
            ),
            requested("queue", 7),
            observed("queue", 8, 1, {"items": [], "steering_messages": []}),
        ]
        assert await reducer.persist(initial) == len(initial)
        completed = await database.fetchone(
            "SELECT state, completion_basis FROM submissions WHERE sdk_session_id = ?",
            (session_id,),
        )
        assert dict(completed) == {
            "state": "semantic_complete",
            "completion_basis": "loop_idle",
        }

        late_running = [
            requested("tasks", 9),
            observed(
                "tasks",
                10,
                2,
                {
                    "tasks": [
                        {
                            "id": "late-task",
                            "status": "running",
                            "type": "agent",
                            "description": "Late worker",
                        }
                    ]
                },
            ),
        ]
        assert await reducer.persist(late_running) == len(late_running)
        reopened = await database.fetchone(
            "SELECT state, completion_basis, terminal_at FROM submissions"
        )
        link = await database.fetchone(
            """
            SELECT state, correlation_basis FROM submission_task_links
            WHERE task_id = 'late-task'
            """
        )
        incident = await database.fetchone(
            """
            SELECT kind FROM runtime_incidents
            WHERE kind = 'late_task_reopened_submission'
            """
        )

        assert dict(reopened) == {
            "state": "loop_idle",
            "completion_basis": None,
            "terminal_at": None,
        }
        assert dict(link) == {
            "state": "running",
            "correlation_basis": "late_task_after_idle",
        }
        assert incident["kind"] == "late_task_reopened_submission"

        terminal = [
            requested("tasks", 11),
            observed(
                "tasks",
                12,
                3,
                {
                    "tasks": [
                        {
                            "id": "late-task",
                            "status": "completed",
                            "type": "agent",
                            "description": "Late worker",
                        }
                    ]
                },
            ),
            requested("activity", 13),
            observed(
                "activity",
                14,
                2,
                {
                    "processing": False,
                    "has_active_work": False,
                    "abortable": False,
                },
            ),
            requested("queue", 15),
            observed("queue", 16, 2, {"items": [], "steering_messages": []}),
        ]
        assert await reducer.persist(terminal) == len(terminal)
        settled = await database.fetchone("SELECT state, completion_basis FROM submissions")

    assert dict(settled) == {
        "state": "semantic_complete",
        "completion_basis": "tasks_terminal_quiet",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("link_field", "expected_basis"),
    [
        ("submissionId", "explicit_submission_id"),
        ("objectiveId", "objective_id"),
    ],
)
async def test_explicit_late_task_links_reopen_and_reacquire_submission(
    tmp_path: Path,
    link_field: str,
    expected_basis: str,
) -> None:
    session_id = f"session-explicit-task-{link_field}"

    def requested(topic: str, seq: int) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.requested",
            {"topic": topic},
            seq,
            source="internal",
            session_id=session_id,
        )

    def observed(
        topic: str,
        seq: int,
        epoch: int,
        payload: dict[str, Any],
    ) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": topic,
                "epoch": epoch,
                "snapshot_id": f"explicit-{link_field}-{topic}-{epoch}",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "observed_at": 100 + seq,
                "payload": payload,
            },
            seq,
            source="internal",
            session_id=session_id,
        )

    async with Database(tmp_path / f"explicit-task-{link_field}.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        reducer = JournalReducer(database)
        initial = [
            _adapted(
                "user.message",
                {"content": "explicit late task", "agentMode": "interactive"},
                1,
                session_id=session_id,
            ),
            _adapted(
                "session.idle",
                {"aborted": False},
                2,
                session_id=session_id,
            ),
            requested("tasks", 3),
            observed("tasks", 4, 1, {"tasks": []}),
            requested("activity", 5),
            observed(
                "activity",
                6,
                1,
                {
                    "processing": False,
                    "has_active_work": False,
                    "abortable": False,
                },
            ),
            requested("queue", 7),
            observed("queue", 8, 1, {"items": [], "steering_messages": []}),
        ]
        assert await reducer.persist(initial) == len(initial)
        submission = await database.fetchone(
            "SELECT submission_id, state FROM submissions WHERE sdk_session_id = ?",
            (session_id,),
        )
        assert submission["state"] == "semantic_complete"
        objective_id = "objective-explicit"
        if link_field == "objectiveId":
            await database.execute(
                """
                UPDATE submissions SET autopilot_objective_id = ?
                WHERE submission_id = ?
                """,
                (objective_id, submission["submission_id"]),
            )
        link_value = (
            str(submission["submission_id"]) if link_field == "submissionId" else objective_id
        )
        running_task = {
            "id": "explicit-task",
            "status": "running",
            "type": "agent",
            "description": "Explicit worker",
            link_field: link_value,
        }
        assert (
            await reducer.persist(
                [
                    requested("tasks", 9),
                    observed("tasks", 10, 2, {"tasks": [running_task]}),
                ]
            )
            == 2
        )
        reopened = await database.fetchone(
            "SELECT state, terminal_at FROM submissions WHERE submission_id = ?",
            (submission["submission_id"],),
        )
        link = await database.fetchone(
            """
            SELECT submission_id, state, correlation_basis
            FROM submission_task_links WHERE task_id = 'explicit-task'
            """
        )
        lease = await database.fetchone(
            """
            SELECT state FROM liveness_leases
            WHERE sdk_session_id = ? AND kind = 'submission'
              AND source_id = ?
            """,
            (session_id, submission["submission_id"]),
        )
        assert dict(reopened) == {"state": "loop_idle", "terminal_at": None}
        assert dict(link) == {
            "submission_id": submission["submission_id"],
            "state": "running",
            "correlation_basis": expected_basis,
        }
        assert lease["state"] == "active"

        completed_task = {**running_task, "status": "completed"}
        terminal = [
            requested("tasks", 11),
            observed("tasks", 12, 3, {"tasks": [completed_task]}),
            requested("activity", 13),
            observed(
                "activity",
                14,
                2,
                {
                    "processing": False,
                    "has_active_work": False,
                    "abortable": False,
                },
            ),
            requested("queue", 15),
            observed("queue", 16, 2, {"items": [], "steering_messages": []}),
        ]
        assert await reducer.persist(terminal) == len(terminal)
        settled = await database.fetchone(
            """
            SELECT state, completion_basis FROM submissions
            WHERE submission_id = ?
            """,
            (submission["submission_id"],),
        )

    assert dict(settled) == {
        "state": "semantic_complete",
        "completion_basis": "tasks_terminal_quiet",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("link_field", ["submissionId", "objectiveId"])
async def test_stale_explicit_task_snapshot_cannot_reopen_completed_submission(
    tmp_path: Path,
    link_field: str,
) -> None:
    session_id = f"session-stale-explicit-{link_field}"
    submission_id = "completed-submission"
    objective_id = "completed-objective"
    async with Database(tmp_path / f"stale-explicit-{link_field}.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, requested_mode,
                state, completion_basis, autopilot_objective_id,
                created_at, observed_at, idle_at, terminal_at
            ) VALUES (
                ?, ?, 'runtime_observed', 'interactive',
                'semantic_complete', 'loop_idle', ?,
                100, 100, 110, 200
            )
            """,
            (
                submission_id,
                session_id,
                objective_id if link_field == "objectiveId" else None,
            ),
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at, released_at
            ) VALUES (?, ?, 'submission', ?, 1, 7, 'released', 100, 200, 200)
            """,
            (
                session_id,
                f"submission:{submission_id}",
                submission_id,
            ),
        )
        link_value = submission_id if link_field == "submissionId" else objective_id
        reducer = JournalReducer(database)
        events = [
            _adapted(
                "copilotd.snapshot.requested",
                {"topic": "tasks"},
                1,
                source="internal",
                session_id=session_id,
            ),
            _adapted(
                "copilotd.snapshot.observed",
                {
                    "topic": "tasks",
                    "epoch": 1,
                    "snapshot_id": f"stale-explicit-{link_field}",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": 0,
                    "observed_at": 150,
                    "payload": {
                        "tasks": [
                            {
                                "id": "stale-task",
                                "status": "running",
                                "type": "agent",
                                link_field: link_value,
                            }
                        ]
                    },
                },
                2,
                source="internal",
                session_id=session_id,
            ),
        ]
        assert await reducer.persist(events) == len(events)
        submission = await database.fetchone(
            """
            SELECT state, completion_basis, terminal_at
            FROM submissions WHERE submission_id = ?
            """,
            (submission_id,),
        )
        link = await database.fetchone(
            "SELECT 1 FROM submission_task_links WHERE task_id = 'stale-task'"
        )
        lease = await database.fetchone(
            """
            SELECT state FROM liveness_leases
            WHERE sdk_session_id = ? AND kind = 'submission'
              AND source_id = ?
            """,
            (session_id, submission_id),
        )
        incident = await database.fetchone(
            """
            SELECT kind FROM runtime_incidents
            WHERE kind = 'stale_task_evidence_ignored_for_completed_submission'
            """
        )

    assert dict(submission) == {
        "state": "semantic_complete",
        "completion_basis": "loop_idle",
        "terminal_at": 200,
    }
    assert link is None
    assert lease["state"] == "released"
    assert incident["kind"] == "stale_task_evidence_ignored_for_completed_submission"


@pytest.mark.asyncio
async def test_repeated_terminal_task_snapshots_are_idempotent_across_resume(
    tmp_path: Path,
) -> None:
    session_id = "session-terminal-repeat"
    submission_id = "submission-terminal-repeat"

    def requested(seq: int, *, generation: int, fence_token: int) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.requested",
            {"topic": "tasks"},
            seq,
            source="internal",
            session_id=session_id,
            generation=generation,
            fence_token=fence_token,
        )

    def terminal_snapshot(
        seq: int,
        epoch: int,
        observed_at: float,
        *,
        generation: int,
        fence_token: int,
    ) -> AdaptedEvent:
        return _adapted(
            "copilotd.snapshot.observed",
            {
                "topic": "tasks",
                "epoch": epoch,
                "snapshot_id": f"terminal-repeat-{generation}-{epoch}",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "observed_at": observed_at,
                "payload": {
                    "tasks": [
                        {
                            "id": "task-terminal",
                            "status": "completed",
                            "type": "agent",
                            "submissionId": submission_id,
                        }
                    ]
                },
            },
            seq,
            source="internal",
            session_id=session_id,
            generation=generation,
            fence_token=fence_token,
        )

    async with Database(tmp_path / "terminal-repeat.sqlite3") as database:
        await _insert_projection_binding(database, session_id)
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, requested_mode,
                state, completion_basis, created_at, observed_at,
                idle_at, terminal_at
            ) VALUES (
                ?, ?, 'runtime_observed', 'interactive',
                'semantic_complete', 'tasks_terminal_quiet',
                100, 100, 150, 200
            )
            """,
            (submission_id, session_id),
        )
        await database.execute(
            """
            INSERT INTO submission_segments(
                submission_id, segment_index, user_event_id,
                is_continuation, state, observed_at, idle_at
            ) VALUES (?, 1, 'terminal-repeat-user', 0,
                      'semantic_complete', 100, 150)
            """,
            (submission_id,),
        )
        await database.execute(
            """
            INSERT INTO submission_task_links(
                sdk_session_id, task_id, submission_id, state,
                terminal_evidence, correlation_basis, linked_at,
                last_progress_at, terminal_at
            ) VALUES (
                ?, 'task-terminal', ?, 'completed', 'task_snapshot',
                'explicit_submission_id', 120, 180, 180
            )
            """,
            (session_id, submission_id),
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at, released_at
            ) VALUES (?, ?, 'submission', ?, 1, 7, 'released', 100, 200, 200)
            """,
            (
                session_id,
                f"submission:{submission_id}",
                submission_id,
            ),
        )
        reducer = JournalReducer(database)
        first_repeat = [
            requested(1, generation=1, fence_token=7),
            terminal_snapshot(
                2,
                1,
                210,
                generation=1,
                fence_token=7,
            ),
        ]
        assert await reducer.persist(first_repeat) == len(first_repeat)
        before_resume = await database.fetchone(
            "SELECT state, terminal_at FROM submissions WHERE submission_id = ?",
            (submission_id,),
        )
        lease_before = await database.fetchone(
            """
            SELECT state, runtime_generation, owner_fence_token
            FROM liveness_leases WHERE kind = 'submission' AND source_id = ?
            """,
            (submission_id,),
        )

        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_generation = 2, owner_fence_token = 8,
                last_inbox_seq = 0, last_sdk_receive_seq = NULL
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        resumed_repeat = [
            requested(3, generation=2, fence_token=8),
            terminal_snapshot(
                4,
                2,
                220,
                generation=2,
                fence_token=8,
            ),
        ]
        assert await reducer.persist(resumed_repeat) == len(resumed_repeat)
        after_resume = await database.fetchone(
            "SELECT state, completion_basis, terminal_at FROM submissions"
        )
        link = await database.fetchone(
            """
            SELECT state, terminal_at FROM submission_task_links
            WHERE task_id = 'task-terminal'
            """
        )
        lease_after = await database.fetchone(
            """
            SELECT state, runtime_generation, owner_fence_token
            FROM liveness_leases WHERE kind = 'submission' AND source_id = ?
            """,
            (submission_id,),
        )
        reopen_incidents = await database.fetchone(
            """
            SELECT COUNT(*) FROM runtime_incidents
            WHERE kind = 'late_task_reopened_submission'
            """
        )

    assert dict(before_resume) == {
        "state": "semantic_complete",
        "terminal_at": 200,
    }
    assert dict(lease_before) == {
        "state": "released",
        "runtime_generation": 1,
        "owner_fence_token": 7,
    }
    assert dict(after_resume) == {
        "state": "semantic_complete",
        "completion_basis": "tasks_terminal_quiet",
        "terminal_at": 200,
    }
    assert dict(link) == {"state": "completed", "terminal_at": 180}
    assert dict(lease_after) == {
        "state": "released",
        "runtime_generation": 1,
        "owner_fence_token": 7,
    }
    assert reopen_incidents[0] == 0
