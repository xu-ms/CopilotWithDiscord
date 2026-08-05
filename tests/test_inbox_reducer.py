import asyncio
import hashlib
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
from PIL import Image

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.event_adapter import EventAdapter
from copilotd.core.inbox import ReducerInbox
from copilotd.core.models import AdaptedEvent, InboxEnvelope
from copilotd.core.reducer import (
    EventReducerWorker,
    JournalReducer,
    _diff_render_payload,
)
from copilotd.discord_app import _discord_render_plan
from copilotd.storage.database import Database


def _read_text_file(path: Path) -> str:
    return path.read_text()


def _sha256_file(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


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
async def test_sdk_workspace_event_authorizes_matching_local_image_path(
    tmp_path: Path,
) -> None:
    cwd = tmp_path / "workspace"
    image_path = cwd / "artifacts" / "chart.png"
    image_path.parent.mkdir(parents=True)
    Image.new("RGB", (4, 4), "green").save(image_path)
    original_bytes = image_path.read_bytes()
    events = [
        AdaptedEvent(
            sdk_session_id="session-trusted-image",
            generation=1,
            fence_token=1,
            inbox_seq=1,
            source="internal",
            raw_type="session.workspace_file_changed",
            raw_payload={
                "data": {
                    "operation": "created",
                    "path": "artifacts/unverified.png",
                }
            },
            reducer_hash="unverified-workspace-image",
            persistence_class="internal",
            received_at=1,
            internal_event_id="unverified-workspace-image",
        ),
        AdaptedEvent(
            sdk_session_id="session-trusted-image",
            generation=1,
            fence_token=1,
            inbox_seq=2,
            source="sdk",
            raw_type="session.workspace_file_changed",
            raw_payload={
                "data": {
                    "operation": "created",
                    "path": "artifacts/chart.png",
                }
            },
            reducer_hash="workspace-image",
            persistence_class="durable",
            received_at=2,
            event_id="workspace-image",
        ),
        AdaptedEvent(
            sdk_session_id="session-trusted-image",
            generation=1,
            fence_token=1,
            inbox_seq=3,
            source="sdk",
            raw_type="assistant.message",
            raw_payload={
                "data": {
                    "messageId": "trusted-image-message",
                    "content": "![chart](artifacts/chart.png)",
                }
            },
            reducer_hash="assistant-image",
            persistence_class="durable",
            received_at=3,
            event_id="assistant-image",
            message_id="trusted-image-message",
        ),
    ]
    async with Database(tmp_path / "trusted-image.sqlite3") as database:
        await SessionBindingRepository(database).create(
            thread_id="thread-trusted-image",
            sdk_session_id="session-trusted-image",
            cwd_snapshot=cwd,
            project_source="explicit",
        )
        assert await JournalReducer(database).persist(events) == 3
        row = await database.fetchone(
            """
            SELECT payload FROM render_outbox
            WHERE lane = 'assistant_final'
            """
        )

    payload = json.loads(row["payload"])
    assert payload["trusted_local_images"] is True
    assert payload["trusted_local_image_paths"] == ["artifacts/chart.png"]
    artifacts = payload["trusted_local_image_artifacts"]
    assert len(artifacts) == 1
    snapshot_path = Path(artifacts[0]["snapshot_path"])
    assert await asyncio.to_thread(snapshot_path.read_bytes) == original_bytes
    assert artifacts[0]["byte_size"] == len(original_bytes)
    assert artifacts[0]["sha256"] == hashlib.sha256(original_bytes).hexdigest()

    Image.new("RGB", (4, 4), "red").save(image_path)
    first_render = await _discord_render_plan(payload, allowed_roots=(cwd,))
    second_render = await _discord_render_plan(payload, allowed_roots=(cwd,))
    assert [asset.content for batch in first_render.batches for asset in batch.assets] == [
        original_bytes
    ]
    assert [asset.content for batch in second_render.batches for asset in batch.assets] == [
        original_bytes
    ]


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
async def test_partial_tool_spill_and_fenced_diff_are_attached_losslessly(
    tmp_path: Path,
) -> None:
    events = [
        {
            "type": "tool.execution_progress",
            "data": {
                "toolCallId": "partial-short",
                "output": "a" * (64 * 1024 - 1),
            },
        },
        {
            "type": "tool.execution_progress",
            "data": {
                "toolCallId": "partial-spill",
                "output": "b" * (64 * 1024),
            },
        },
        {
            "type": "tool.execution_complete",
            "data": {
                "toolCallId": "diff-fence",
                "toolName": "git diff",
                "success": True,
                "result": {"patch": "+```python\n+print('safe')\n+```"},
            },
        },
    ]
    adapted = [
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-tool-spill",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload=event,
                received_at=100 + index,
                internal_event_id=f"spill-{index}",
            )
        )
        for index, event in enumerate(events, start=1)
    ]
    async with Database(tmp_path / "tool-spill.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 3
        artifact_rows = await database.fetchall(
            "SELECT payload FROM render_outbox WHERE lane = 'artifact'"
        )
        diff_row = await database.fetchone("SELECT payload FROM render_outbox WHERE lane = 'diff'")
        spill = await database.fetchone(
            """
            SELECT content, spilled FROM tool_output_streams
            WHERE tool_call_id = 'partial-spill'
            """
        )

    assert len(artifact_rows) == 1
    artifact = json.loads(artifact_rows[0]["payload"])
    spill_attachment = artifact["attachments"][0]
    assert "content" not in spill_attachment
    assert await asyncio.to_thread(
        _read_text_file,
        Path(spill_attachment["path"]),
    ) == "b" * (64 * 1024)
    assert artifact["verbatim"] is True
    assert spill["content"] == ""
    assert spill["spilled"] == 1
    diff = json.loads(diff_row["payload"])
    assert diff["content"].endswith("attached as `changes-diff-fence.diff`.")
    assert diff["attachments"][0]["content"] == "+```python\n+print('safe')\n+```"


def test_structured_diff_enforces_render_byte_cap() -> None:
    patch = "x" * (8 * 1024 * 1024 + 1)
    event = AdaptedEvent(
        sdk_session_id="session-large-diff",
        generation=1,
        fence_token=1,
        inbox_seq=1,
        source="internal",
        raw_type="tool.execution_complete",
        raw_payload={
            "data": {
                "toolCallId": "large-diff",
                "success": True,
                "result": {"patch": patch},
            }
        },
        reducer_hash="large-diff",
        persistence_class="internal",
        received_at=1,
        internal_event_id="large-diff",
    )

    payload = _diff_render_payload(event)

    assert payload is not None
    assert payload["oversized"] is True
    assert payload["byte_count"] == len(patch)
    assert payload["attachments"] == []
    assert len(payload["content"]) < 300


def test_diff_named_tool_cannot_trigger_implicit_local_or_content_diff() -> None:
    event = AdaptedEvent(
        sdk_session_id="session-untrusted-diff",
        generation=1,
        fence_token=1,
        inbox_seq=1,
        source="internal",
        raw_type="tool.execution_complete",
        raw_payload={
            "data": {
                "toolCallId": "untrusted-diff",
                "toolName": "diff secrets helper",
                "success": True,
                "result": {"detailedContent": "not a structured diff"},
            }
        },
        reducer_hash="untrusted-diff",
        persistence_class="internal",
        received_at=1,
        internal_event_id="untrusted-diff",
    )

    assert _diff_render_payload(event) is None


@pytest.mark.asyncio
async def test_structured_diff_suppresses_duplicate_generic_tool_artifact(
    tmp_path: Path,
) -> None:
    patch = "+" + ("diff-line\n" * 1000)
    event = EventAdapter().adapt(
        InboxEnvelope(
            sdk_session_id="session-diff-dedup",
            generation=1,
            fence_token=1,
            inbox_seq=1,
            source="internal",
            payload={
                "type": "tool.execution_complete",
                "data": {
                    "toolCallId": "diff-dedup",
                    "toolName": "git diff",
                    "success": True,
                    "result": {
                        "patch": patch,
                        "detailedContent": patch,
                    },
                },
            },
            received_at=1,
            internal_event_id="diff-dedup",
        )
    )
    async with Database(tmp_path / "diff-dedup.sqlite3") as database:
        assert await JournalReducer(database).persist([event]) == 1
        lanes = await database.fetchall("SELECT lane FROM render_outbox ORDER BY lane")

    assert [row["lane"] for row in lanes] == ["diff", "taskdeck"]


@pytest.mark.asyncio
async def test_tool_spill_uses_durable_cumulative_stream(tmp_path: Path) -> None:
    chunks = ("a" * (70 * 1024), "b" * (10 * 1024))
    adapted = [
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-cumulative-tool",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload={
                    "type": "tool.execution_progress",
                    "data": {
                        "toolCallId": "cumulative-tool",
                        "outputDelta": chunk,
                    },
                },
                received_at=100 + index,
                internal_event_id=f"cumulative-{index}",
            )
        )
        for index, chunk in enumerate(chunks, start=1)
    ]
    adapted.append(
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-cumulative-tool",
                generation=1,
                fence_token=1,
                inbox_seq=3,
                source="internal",
                payload={
                    "type": "tool.execution_progress",
                    "data": {
                        "toolCallId": "cumulative-tool",
                        "progressMessage": "human status only",
                    },
                },
                received_at=103,
                internal_event_id="cumulative-status",
            )
        )
    )
    adapted.append(
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-cumulative-tool",
                generation=1,
                fence_token=1,
                inbox_seq=4,
                source="internal",
                payload={
                    "type": "tool.execution_complete",
                    "data": {
                        "toolCallId": "cumulative-tool",
                        "success": True,
                        "result": {"detailedContent": "FINAL-RESULT"},
                    },
                },
                received_at=104,
                internal_event_id="cumulative-complete",
            )
        )
    )
    adapted.append(
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-cumulative-tool",
                generation=1,
                fence_token=1,
                inbox_seq=5,
                source="internal",
                payload={
                    "type": "tool.execution_progress",
                    "data": {
                        "toolCallId": "cumulative-tool",
                        "outputDelta": "LATE-PROGRESS",
                    },
                },
                received_at=105,
                internal_event_id="cumulative-late-progress",
            )
        )
    )
    async with Database(tmp_path / "cumulative-tool.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 5
        rows = await database.fetchall(
            """
            SELECT coalesce_key, payload FROM render_outbox
            WHERE lane = 'artifact' ORDER BY logical_seq
            """
        )
        stream = await database.fetchone(
            """
            SELECT content, spilled, artifact_emitted, finalized
            FROM tool_output_streams
            WHERE session_id = 'session-cumulative-tool'
              AND tool_call_id = 'cumulative-tool'
            """
        )

    assert len(rows) == 1
    payloads = [json.loads(row["payload"]) for row in rows]
    assert {row["coalesce_key"] for row in rows} == {"tool-spill:cumulative-tool"}
    attachments = [payload["attachments"][0] for payload in payloads]
    assert all("content" not in attachment for attachment in attachments)
    assert len({attachment["path"] for attachment in attachments}) == 1
    final_content = await asyncio.to_thread(
        _read_text_file,
        Path(attachments[-1]["path"]),
    )
    assert final_content.startswith("".join(chunks))
    assert final_content.endswith("FINAL-RESULT")
    assert "LATE-PROGRESS" not in final_content
    assert [payload["finalized"] for payload in payloads] == [True]
    assert payloads[0]["tool_source"] == "durable-stream+detailedContent"
    assert stream["content"] == ""
    assert "human status only" not in final_content
    assert stream["spilled"] == 1
    assert stream["artifact_emitted"] == 1
    assert stream["finalized"] == 1


@pytest.mark.asyncio
async def test_stable_spill_revision_preserves_pending_retry_window(
    tmp_path: Path,
) -> None:
    def progress(index: int, chunk: str) -> AdaptedEvent:
        return EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-spill-retry",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload={
                    "type": "tool.execution_progress",
                    "data": {
                        "toolCallId": "spill-retry",
                        "outputDelta": chunk,
                    },
                },
                received_at=100 + index,
                internal_event_id=f"spill-retry-{index}",
            )
        )

    async with Database(tmp_path / "spill-retry.sqlite3") as database:
        reducer = JournalReducer(database)
        assert await reducer.persist([progress(1, "a" * (70 * 1024))]) == 1
        await database.execute(
            """
            UPDATE render_outbox
            SET next_attempt_at = 9999999999, attempts = 1
            WHERE coalesce_key = 'tool-spill:spill-retry'
            """
        )
        assert await reducer.persist([progress(2, "b")]) == 1
        row = await database.fetchone(
            """
            SELECT next_attempt_at, attempts, payload_revision FROM render_outbox
            WHERE coalesce_key = 'tool-spill:spill-retry'
            """
        )

    assert dict(row) == {
        "next_attempt_at": 9999999999,
        "attempts": 1,
        "payload_revision": 2,
    }


@pytest.mark.asyncio
async def test_canonical_tool_completion_does_not_duplicate_spilled_stream(
    tmp_path: Path,
) -> None:
    canonical = "canonical-" * (70 * 1024 // len("canonical-") + 1)
    events = [
        {
            "type": "tool.execution_progress",
            "data": {
                "toolCallId": "canonical-spill",
                "outputDelta": canonical,
            },
        },
        {
            "type": "tool.execution_complete",
            "data": {
                "toolCallId": "canonical-spill",
                "success": True,
                "result": {"detailedContent": canonical},
            },
        },
    ]
    adapted = [
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-canonical-spill",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload=event,
                received_at=100 + index,
                internal_event_id=f"canonical-spill-{index}",
            )
        )
        for index, event in enumerate(events, start=1)
    ]

    async with Database(tmp_path / "canonical-spill.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 2
        artifact = await database.fetchone(
            """
            SELECT local_path, byte_size, sha256 FROM tool_spill_artifacts
            WHERE session_id = 'session-canonical-spill'
              AND tool_call_id = 'canonical-spill'
            """
        )

    artifact_path = Path(str(artifact["local_path"]))
    content = await asyncio.to_thread(artifact_path.read_text, encoding="utf-8")
    assert content == canonical
    assert artifact["byte_size"] == len(canonical.encode())
    assert artifact["sha256"] == hashlib.sha256(canonical.encode()).hexdigest()


@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_completion_only_100mib_spills_without_large_outbox_payload(
    tmp_path: Path,
) -> None:
    completion = "q" * (100 * 1024 * 1024)
    event = EventAdapter().adapt(
        InboxEnvelope(
            sdk_session_id="session-completion-only",
            generation=1,
            fence_token=1,
            inbox_seq=1,
            source="internal",
            payload={
                "type": "tool.execution_complete",
                "data": {
                    "toolCallId": "completion-only",
                    "success": True,
                    "result": {"detailedContent": completion},
                },
            },
            received_at=100,
            internal_event_id="completion-only",
        )
    )
    database_path = tmp_path / "completion-only.sqlite3"
    async with Database(database_path) as database:
        assert await JournalReducer(database).persist([event]) == 1
        artifact = await database.fetchone(
            """
            SELECT local_path, byte_size, sha256, finalized
            FROM tool_spill_artifacts
            WHERE session_id = 'session-completion-only'
              AND tool_call_id = 'completion-only'
            """
        )
        outbox = await database.fetchone(
            """
            SELECT LENGTH(payload) AS payload_bytes
            FROM render_outbox WHERE lane = 'artifact'
            """
        )
        stream = await database.fetchone(
            """
            SELECT content, spilled, finalized FROM tool_output_streams
            WHERE session_id = 'session-completion-only'
            """
        )

    artifact_path = Path(str(artifact["local_path"]))
    artifact_size = await asyncio.to_thread(lambda: artifact_path.stat().st_size)
    artifact_digest = await asyncio.to_thread(_sha256_file, artifact_path)
    assert artifact_size == len(completion)
    assert artifact["byte_size"] == len(completion)
    assert artifact["sha256"] == artifact_digest
    assert artifact["finalized"] == 1
    assert outbox["payload_bytes"] < 200_000
    assert dict(stream) == {"content": "", "spilled": 1, "finalized": 1}
    assert database_path.stat().st_size < 160 * 1024 * 1024


@pytest.mark.asyncio
async def test_tool_spill_preserves_full_structured_completion_error(
    tmp_path: Path,
) -> None:
    events = [
        {
            "type": "tool.execution_progress",
            "data": {
                "toolCallId": "error-spill",
                "outputDelta": "x" * (70 * 1024),
            },
        },
        {
            "type": "tool.execution_complete",
            "data": {
                "toolCallId": "error-spill",
                "success": False,
                "error": {
                    "message": "ENOENT",
                    "path": "/tmp/missing",
                    "errno": 2,
                },
            },
        },
    ]
    adapted = [
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-error-spill",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload=event,
                received_at=100 + index,
                internal_event_id=f"error-spill-{index}",
            )
        )
        for index, event in enumerate(events, start=1)
    ]
    async with Database(tmp_path / "error-spill.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 2
        rows = await database.fetchall(
            """
            SELECT payload FROM render_outbox
            WHERE lane = 'artifact' ORDER BY logical_seq
            """
        )

    attachment = json.loads(rows[-1]["payload"])["attachments"][0]
    final_content = await asyncio.to_thread(
        _read_text_file,
        Path(attachment["path"]),
    )
    assert final_content.startswith("x" * 100)
    assert '"message": "ENOENT"' in final_content
    assert '"path": "/tmp/missing"' in final_content
    assert '"errno": 2' in final_content


@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_tool_spill_100x1mib_has_linear_database_growth(
    tmp_path: Path,
) -> None:
    chunk = "z" * (1024 * 1024)
    adapted = [
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-stress-spill",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="internal",
                payload={
                    "type": "tool.execution_progress",
                    "data": {
                        "toolCallId": "stress-spill",
                        "outputDelta": chunk,
                    },
                },
                received_at=100 + index,
                internal_event_id=f"stress-spill-{index}",
            )
        )
        for index in range(1, 101)
    ]
    adapted.append(
        EventAdapter().adapt(
            InboxEnvelope(
                sdk_session_id="session-stress-spill",
                generation=1,
                fence_token=1,
                inbox_seq=101,
                source="internal",
                payload={
                    "type": "tool.execution_complete",
                    "data": {
                        "toolCallId": "stress-spill",
                        "success": True,
                        "result": {"detailedContent": "STRESS-DONE"},
                    },
                },
                received_at=201,
                internal_event_id="stress-spill-complete",
            )
        )
    )
    database_path = tmp_path / "stress-spill.sqlite3"
    async with Database(database_path) as database:
        assert await JournalReducer(database).persist(adapted) == 101
        artifact = await database.fetchone(
            """
            SELECT local_path, byte_size, sha256, finalized
            FROM tool_spill_artifacts
            WHERE session_id = 'session-stress-spill'
              AND tool_call_id = 'stress-spill'
            """
        )
        outbox = await database.fetchone(
            """
            SELECT COUNT(*) AS count,
                   COUNT(DISTINCT coalesce_key) AS coalesce_count,
                   SUM(LENGTH(payload)) AS payload_bytes
            FROM render_outbox WHERE lane = 'artifact'
            """
        )
        stream = await database.fetchone(
            """
            SELECT content, finalized FROM tool_output_streams
            WHERE session_id = 'session-stress-spill'
            """
        )

    artifact_path = Path(artifact["local_path"])
    artifact_digest = await asyncio.to_thread(_sha256_file, artifact_path)
    assert artifact["finalized"] == 1
    assert artifact["sha256"] == artifact_digest
    assert artifact["byte_size"] >= 100 * 1024 * 1024
    assert outbox["count"] == 1
    assert outbox["coalesce_count"] == 1
    assert outbox["payload_bytes"] < 200_000
    assert stream["content"] == ""
    assert stream["finalized"] == 1
    assert database_path.stat().st_size < 300 * 1024 * 1024


@pytest.mark.asyncio
async def test_correlated_idle_emits_model_token_context_duration_footer(
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
            "SELECT lane, payload FROM render_outbox WHERE lane = 'footer'"
        )

    assert row["lane"] == "footer"
    payload = json.loads(row["payload"])
    assert payload["model"] == "gpt-footer"
    assert payload["input_tokens"] == 12
    assert payload["output_tokens"] == 7
    assert payload["credits"] == 1.5
    assert payload["context"] == "19/100"
    assert payload["duration_seconds"] == 20


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
async def test_agent_deltas_assemble_before_taskdeck_projection(tmp_path: Path) -> None:
    started = SessionEvent(
        data=SubagentStartedData(
            agent_description="Stream a result",
            agent_display_name="Streaming agent",
            agent_name="stream-agent",
            tool_call_id="tool-agent-stream",
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.SUBAGENT_STARTED,
        agent_id="agent-stream",
    )
    deltas = [
        SessionEvent(
            data=AssistantMessageDeltaData(
                delta_content=content,
                message_id="agent-stream-message",
            ),
            id=uuid4(),
            timestamp=datetime.now(UTC),
            type=SessionEventType.ASSISTANT_MESSAGE_DELTA,
            agent_id="agent-stream",
        )
        for content in ("hel", "lo")
    ]
    adapter = EventAdapter()
    adapted = [
        adapter.adapt(
            InboxEnvelope(
                sdk_session_id="session-agent-stream",
                generation=1,
                fence_token=1,
                inbox_seq=index,
                source="sdk",
                payload=event,
                received_at=100 + index,
                sdk_receive_seq=index,
            )
        )
        for index, event in enumerate((started, *deltas), start=1)
    ]

    async with Database(tmp_path / "agent-stream.sqlite3") as database:
        assert await JournalReducer(database).persist(adapted) == 3
        stream = await database.fetchone(
            """
            SELECT content, finalized FROM render_streams
            WHERE session_id = 'session-agent-stream'
              AND message_id = 'agent-stream-message'
              AND agent_id = 'agent-stream'
            """
        )
        card = await database.fetchone(
            """
            SELECT progress_summary FROM task_card_projections
            WHERE sdk_session_id = 'session-agent-stream'
            """
        )

    assert dict(stream) == {"content": "hello", "finalized": 0}
    assert card["progress_summary"] == "hello"


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
