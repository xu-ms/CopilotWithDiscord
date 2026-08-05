import asyncio
import json
import threading
from datetime import UTC, datetime
from pathlib import Path
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

from copilotd.core.event_adapter import EventAdapter
from copilotd.core.inbox import ReducerInbox
from copilotd.core.models import AdaptedEvent, InboxEnvelope
from copilotd.core.reducer import EventReducerWorker, JournalReducer
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
    setter = threading.Thread(
        target=lambda: inbox.set_quiesce_observers(None, None)
    )
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
        def plan(
            self,
            _event: object,
            *,
            payload_override: object = None,
        ) -> list[object]:
            del payload_override
            raise RuntimeError("render planning failed")

    async with Database(tmp_path / "atomic.sqlite3") as database:
        reducer = JournalReducer(database, planner=FailingPlanner())  # type: ignore[arg-type]
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

    assert count[0] == 0


@pytest.mark.asyncio
async def test_reducer_materializes_full_stream_content_for_each_outbox_edit(
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
        rows = await database.fetchall("SELECT payload FROM render_outbox ORDER BY logical_seq")
        stream = await database.fetchone(
            "SELECT content, finalized FROM render_streams WHERE message_id = 'message-1'"
        )

    payloads = [json.loads(row["payload"]) for row in rows]
    assert [payload["content"] for payload in payloads] == ["hel", "hello", "hello!"]
    assert [payload["finalized"] for payload in payloads] == [False, False, True]
    assert dict(stream) == {"content": "hello!", "finalized": 1}


@pytest.mark.asyncio
async def test_reducer_materializes_nonempty_status_and_usage_payloads(
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
        assert await JournalReducer(database).persist(adapted) == 4
        rows = await database.fetchall(
            "SELECT lane, payload FROM render_outbox ORDER BY logical_seq"
        )

    payloads = [(row["lane"], json.loads(row["payload"])) for row in rows]
    assert payloads[0][0] == "status"
    assert "Context is nearly full." in payloads[0][1]["content"]
    assert payloads[1][0] == "usage"
    assert "Input tokens: `12`" in payloads[1][1]["content"]
    assert "Total tokens: `19`" in payloads[1][1]["content"]
    assert payloads[2][0] == "status"
    assert "raw chain-of-thought is hidden" in payloads[2][1]["content"]
    assert "private chain of thought" not in payloads[2][1]["content"]
    assert payloads[3][0] == "status"
    assert "`modified` `src/copilotd/app.py`" in payloads[3][1]["content"]


@pytest.mark.asyncio
async def test_long_tool_results_are_preserved_as_artifacts_at_8000_boundary(
    tmp_path: Path,
) -> None:
    outputs = [
        ("short", "x" * 7999, "content", True),
        ("fallback", "y" * 8000, "content", True),
        ("detailed", "z" * 12000, "detailedContent", True),
        ("failure", "e" * 8000, "error", False),
    ]
    adapted = []
    for index, (tool_id, content, source, success) in enumerate(outputs, start=1):
        result: dict[str, str] | None = None
        error: dict[str, str] | None = None
        if success:
            result = {source: content}
        else:
            error = {"message": content}
        adapted.append(
            EventAdapter().adapt(
                InboxEnvelope(
                    sdk_session_id="session-tool-artifact",
                    generation=1,
                    fence_token=1,
                    inbox_seq=index,
                    source="internal",
                    payload={
                        "type": "tool.execution_complete",
                        "data": {
                            "toolCallId": tool_id,
                            "success": success,
                            "result": result,
                            "error": error,
                        },
                    },
                    received_at=100 + index,
                    internal_event_id=f"tool-{tool_id}",
                )
            )
        )

    async with Database(tmp_path / "tool-artifact.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 4
        rows = await database.fetchall(
            """
            SELECT payload FROM render_outbox
            WHERE lane = 'artifact' ORDER BY logical_seq
            """
        )

    payloads = [json.loads(row["payload"]) for row in rows]
    assert len(payloads) == 3
    assert "Runtime fallback content may be truncated." in payloads[0]["content"]
    assert payloads[0]["attachments"][0]["content"] == "y" * 8000
    assert "12,000" in payloads[1]["content"]
    assert "truncated" not in payloads[1]["content"]
    assert payloads[1]["attachments"][0]["content"] == "z" * 12000
    assert "**Tool failed**" in payloads[2]["content"]
    assert payloads[2]["attachments"][0]["content"] == "e" * 8000


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
            SELECT requested_type, requested_event_id, completed_event_id, state
            FROM protocol_requests WHERE request_id = 'request-1'
            """
        )

    assert dict(row) == {
        "requested_type": "user_input.requested",
        "requested_event_id": "event-requested",
        "completed_event_id": "event-completed",
        "state": "completed",
    }


@pytest.mark.asyncio
async def test_subagent_output_stays_in_one_collapsed_taskdeck(tmp_path: Path) -> None:
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
                sdk_session_id="session-taskdeck",
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

    async with Database(tmp_path / "taskdeck.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 3
        cards = await database.fetchall(
            "SELECT kind, title, state, progress_summary FROM task_card_projections"
        )
        outbox = await database.fetchall(
            "SELECT lane, coalesce_key, payload FROM render_outbox ORDER BY logical_seq"
        )
        lease = await database.fetchone(
            "SELECT state FROM liveness_leases WHERE sdk_session_id = 'session-taskdeck'"
        )

    assert [dict(card) for card in cards] == [
        {
            "kind": "agent",
            "title": "Parser investigator",
            "state": "completed",
            "progress_summary": "42 tokens, 2 tool calls",
        }
    ]
    assert {row["lane"] for row in outbox} == {"taskdeck"}
    assert {row["coalesce_key"] for row in outbox} == {"taskdeck"}
    final_payload = json.loads(outbox[-1]["payload"])
    assert "**TaskDeck**" in final_payload["content"]
    assert "✅ **Parser investigator**" in final_payload["content"]
    assert final_payload["finalized"] is True
    assert final_payload["taskdeck"]["expanded"] is False
    assert final_payload["taskdeck"]["options"][0]["label"] == "Parser investigator"
    assert lease["state"] == "released"


@pytest.mark.asyncio
async def test_taskdeck_view_change_expands_the_selected_card_in_place(
    tmp_path: Path,
) -> None:
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
    adapter = EventAdapter()
    adapted_started = adapter.adapt(
        InboxEnvelope(
            sdk_session_id="session-taskdeck-view",
            generation=1,
            fence_token=9,
            inbox_seq=1,
            source="sdk",
            payload=started,
            received_at=100,
            sdk_receive_seq=1,
        )
    )

    async with Database(tmp_path / "taskdeck-view.sqlite3") as database:
        reducer = JournalReducer(database)
        assert await reducer.persist([adapted_started]) == 1
        initial_row = await database.fetchone(
            "SELECT payload FROM render_outbox ORDER BY logical_seq DESC LIMIT 1"
        )
        initial = json.loads(initial_row["payload"])
        metadata = initial["taskdeck"]
        changed = adapter.adapt(
            InboxEnvelope(
                sdk_session_id="session-taskdeck-view",
                generation=1,
                fence_token=9,
                inbox_seq=2,
                source="internal",
                payload={
                    "type": "copilotd.taskdeck.view_changed",
                    "data": {
                        "panel_id": metadata["panel_id"],
                        "selected_card_token": metadata["selected_card_token"],
                        "page": 0,
                        "expanded": True,
                    },
                },
                received_at=101,
                internal_event_id="taskdeck-view:interaction-1",
            )
        )
        assert await reducer.persist([changed]) == 1
        expanded_row = await database.fetchone(
            "SELECT payload FROM render_outbox ORDER BY logical_seq DESC LIMIT 1"
        )

    expanded = json.loads(expanded_row["payload"])
    assert expanded["taskdeck"]["expanded"] is True
    assert "**Parser investigator details**" in expanded["content"]
    assert "Investigate the parser" in expanded["content"]
