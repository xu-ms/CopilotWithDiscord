import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.discord_app import _discord_render_plan
from copilotd.render.outbox import RenderOutboxDispatcher
from copilotd.storage.database import Database


def _event(
    kind: str,
    data: dict[str, Any],
    sequence: int,
    *,
    source: str = "internal",
    event_id: str | None = None,
    message_id: str | None = None,
    turn_id: str | None = None,
    agent_id: str | None = None,
    task_id: str | None = None,
    tool_call_id: str | None = None,
    interaction_id: str | None = None,
) -> AdaptedEvent:
    return AdaptedEvent(
        sdk_session_id="turn-session",
        generation=1,
        fence_token=7,
        inbox_seq=sequence,
        source=source,
        raw_type=kind,
        raw_payload={"type": kind, "data": data},
        reducer_hash=f"turn-{sequence}",
        persistence_class="durable" if source == "sdk" else "internal",
        received_at=100 + sequence,
        event_id=event_id,
        internal_event_id=None if source == "sdk" else f"turn:{kind}:{sequence}",
        message_id=message_id,
        turn_id=turn_id,
        agent_id=agent_id,
        task_id=task_id,
        tool_call_id=tool_call_id,
        interaction_id=interaction_id,
    )


async def _binding(database: Database, root: Path) -> None:
    await database.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            attachment_state, permission_posture, runtime_generation,
            owner_fence_token, created_at, updated_at
        ) VALUES (
            'turn-thread', 'home', ?, 'turn-session', 'attached',
            'verified_allow_all', 1, 7, 1, 1
        )
        """,
        (str(root),),
    )


def _queued(sequence: int = 1) -> AdaptedEvent:
    return _event(
        "copilotd.submission.queued",
        {
            "submission_id": "submission-1",
            "thread_id": "turn-thread",
            "origin": "app_message",
            "prompt": "build it",
            "prompt_hash": hashlib.sha256(b"build it").hexdigest(),
            "requested_mode": "interactive",
            "requested_model_config": {},
            "requested_agent": "default",
            "requested_session_config_version": 1,
            "requested_delivery": "enqueue",
            "attachment_count": 0,
            "created_at": 100,
        },
        sequence,
    )


class _Transport:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[tuple[str, dict[str, Any]]] = []

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        del session_id, lane, idempotency_key
        self.sent.append(payload)
        return "discord-turn-card"

    async def edit(
        self,
        *,
        session_id: str,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None:
        del session_id, lane, idempotency_key
        self.edited.append((message_id, payload))

    async def reaction(self, **_kwargs: Any) -> None:
        return None


@pytest.mark.asyncio
async def test_multi_tool_stream_and_replay_use_one_durable_turn_card(tmp_path: Path) -> None:
    transport = _Transport()
    interaction_id = "interaction-1"
    async with Database(tmp_path / "single-turn.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        assert await reducer.persist([_queued()]) == 1
        initial = await database.fetchone(
            """
            SELECT idempotency_key, coalesce_key, payload, payload_revision
            FROM render_outbox WHERE lane = 'assistant_stream'
            """
        )
        assert initial is not None
        initial_payload = json.loads(str(initial["payload"]))
        assert initial_payload["content"] == "Copilot is working…"
        assert initial["idempotency_key"] == ("turn-render:turn-session:turn:submission-1")
        assert initial["coalesce_key"] == "turn:submission-1"

        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once() == 1
        assert len(transport.sent) == 1

        events = [
            _event(
                "copilotd.submission.accepted",
                {"submission_id": "submission-1", "message_id": "runtime-user"},
                2,
            ),
            _event(
                "user.message",
                {
                    "content": "build it",
                    "agentMode": "interactive",
                    "attachments": [],
                    "interactionId": interaction_id,
                },
                3,
                source="sdk",
                event_id="runtime-user",
                interaction_id=interaction_id,
            ),
            _event(
                "assistant.turn_start",
                {"turnId": "turn-1", "interactionId": interaction_id},
                4,
                source="sdk",
                event_id="turn-start",
                turn_id="turn-1",
                interaction_id=interaction_id,
            ),
            _event(
                "assistant.intent",
                {"intent": "Inspecting the repository."},
                5,
                source="sdk",
                event_id="intent",
                turn_id="turn-1",
                interaction_id=interaction_id,
            ),
            _event(
                "tool.execution_start",
                {"toolCallId": "tool-1", "toolName": "private-shell"},
                6,
                source="sdk",
                event_id="tool-start",
                turn_id="turn-1",
                tool_call_id="tool-1",
                interaction_id=interaction_id,
            ),
            _event(
                "tool.execution_progress",
                {
                    "toolCallId": "tool-1",
                    "progressMessage": "Running tests.",
                    "outputDelta": "secret-log-" * 7000,
                },
                7,
                source="sdk",
                event_id="tool-progress",
                turn_id="turn-1",
                tool_call_id="tool-1",
                interaction_id=interaction_id,
            ),
            _event(
                "assistant.message",
                {
                    "messageId": "answer-progress",
                    "content": "I inspected the implementation.",
                    "interactionId": interaction_id,
                },
                8,
                source="sdk",
                event_id="answer-progress",
                message_id="answer-progress",
                turn_id="turn-1",
                interaction_id=interaction_id,
            ),
            _event(
                "tool.execution_complete",
                {
                    "toolCallId": "tool-1",
                    "toolName": "private-shell",
                    "success": True,
                    "result": {"detailedContent": "raw-detail-" * 9000},
                },
                9,
                source="sdk",
                event_id="tool-complete",
                turn_id="turn-1",
                tool_call_id="tool-1",
                interaction_id=interaction_id,
            ),
        ]
        assert await reducer.persist(events) == len(events)
        rows = await database.fetchall(
            """
            SELECT lane, payload FROM render_outbox
            WHERE session_id = 'turn-session'
            """
        )
        assert len(rows) == 1
        progress_payload = json.loads(str(rows[0]["payload"]))
        assert progress_payload["status"] == {
            "title": "Tool completed",
            "detail": "`private-shell`",
            "event_type": "turn.tool_complete",
        }
        assert await dispatcher.dispatch_once() == 1
        assert transport.edited[-1][1]["status"] == progress_payload["status"]
        spill = await database.fetchone(
            """
            SELECT local_path, finalized FROM tool_spill_artifacts
            WHERE session_id = 'turn-session' AND tool_call_id = 'tool-1'
            """
        )
        assert spill is not None
        assert await asyncio.to_thread(Path(str(spill["local_path"])).exists)

        final = _event(
            "assistant.message",
            {
                "messageId": "answer-1",
                "content": "The complete final answer.",
                "interactionId": interaction_id,
            },
            10,
            source="sdk",
            event_id="answer-final",
            message_id="answer-1",
            turn_id="turn-1",
            interaction_id=interaction_id,
        )
        assert await reducer.persist([final]) == 1
        assert await dispatcher.dispatch_once() == 1
        assert transport.edited[-1][0] == "discord-turn-card"
        assert transport.edited[-1][1]["content"] == (
            "I inspected the implementation.\n\nThe complete final answer."
        )

        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', terminal_at = 109
            WHERE submission_id = 'submission-1'
            """
        )
        assert (
            await reducer.persist(
                [_event("copilotd.snapshot.requested", {"topic": "activity"}, 11)]
            )
            == 1
        )
        assert await RenderOutboxDispatcher(database, transport).dispatch_once() == 1
        mapping = await database.fetchone(
            """
            SELECT discord_message_id, finalized FROM render_messages
            WHERE session_id = 'turn-session' AND logical_key = 'turn:submission-1'
            """
        )
        journal_tools = await database.fetchone(
            """
            SELECT COUNT(*) FROM event_journal
            WHERE sdk_session_id = 'turn-session'
              AND raw_type LIKE 'tool.execution_%'
            """
        )

    delivered = [*transport.sent, *(payload for _, payload in transport.edited)]
    encoded = json.dumps(delivered)
    assert len(transport.sent) == 1
    assert all(message_id == "discord-turn-card" for message_id, _ in transport.edited)
    assert transport.edited[-1][1]["finalized"] is True
    assert "private-shell" in encoded
    assert "secret-log" not in encoded
    assert "raw-detail" not in encoded
    assert "tool_output_artifact" not in encoded
    assert dict(mapping) == {"discord_message_id": "discord-turn-card", "finalized": 1}
    assert journal_tools[0] == 3


@pytest.mark.asyncio
async def test_autopilot_continuation_card_uses_current_interaction_transcript(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "continuation-turn.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        assert await reducer.persist([_queued()]) == 1
        await database.execute(
            """
            UPDATE submissions SET requested_mode = 'autopilot'
            WHERE submission_id = 'submission-1'
            """
        )
        assert (
            await reducer.persist(
                [
                    _event(
                        "copilotd.submission.accepted",
                        {"submission_id": "submission-1", "message_id": "runtime-root"},
                        2,
                    ),
                    _event(
                        "user.message",
                        {
                            "content": "build it",
                            "agentMode": "autopilot",
                            "interactionId": "interaction-root",
                        },
                        3,
                        source="sdk",
                        event_id="runtime-root",
                        interaction_id="interaction-root",
                    ),
                    _event(
                        "assistant.message",
                        {
                            "messageId": "root-answer",
                            "content": "Root interaction answer.",
                            "interactionId": "interaction-root",
                        },
                        4,
                        source="sdk",
                        event_id="root-answer",
                        message_id="root-answer",
                        interaction_id="interaction-root",
                    ),
                    _event(
                        "session.task_complete",
                        {"outcome": "continue"},
                        5,
                        source="sdk",
                        event_id="continue",
                    ),
                    _event(
                        "session.idle",
                        {"aborted": False},
                        6,
                        source="sdk",
                        event_id="root-idle",
                    ),
                    _event(
                        "user.message",
                        {
                            "content": "continue",
                            "agentMode": "autopilot",
                            "isAutopilotContinuation": True,
                            "interactionId": "interaction-continuation",
                        },
                        7,
                        source="sdk",
                        event_id="runtime-continuation",
                        interaction_id="interaction-continuation",
                    ),
                    _event(
                        "assistant.message",
                        {
                            "messageId": "continuation-answer",
                            "content": "Continuation interaction answer.",
                            "interactionId": "interaction-continuation",
                        },
                        8,
                        source="sdk",
                        event_id="continuation-answer",
                        message_id="continuation-answer",
                        interaction_id="interaction-continuation",
                    ),
                ]
            )
            == 7
        )
        continuation = await database.fetchone(
            """
            SELECT payload FROM render_outbox
            WHERE session_id = 'turn-session'
              AND coalesce_key = 'turn:submission-1:segment:2'
            """
        )

    payload = json.loads(str(continuation["payload"]))
    assert payload["content"] == "Continuation interaction answer."
    assert "Root interaction answer." not in payload["content"]


@pytest.mark.asyncio
async def test_failed_tool_turn_edits_same_card_with_sanitized_error(tmp_path: Path) -> None:
    transport = _Transport()
    async with Database(tmp_path / "failed-turn.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        dispatcher = RenderOutboxDispatcher(database, transport)
        await dispatcher.dispatch_once()
        await reducer.persist(
            [
                _event(
                    "tool.execution_complete",
                    {
                        "toolCallId": "tool-failed",
                        "toolName": "dangerous-command",
                        "success": False,
                        "error": {
                            "message": "secret failure",
                            "stack": "Traceback: private details",
                        },
                    },
                    2,
                    tool_call_id="tool-failed",
                ),
                _event(
                    "session.error",
                    {
                        "message": "Traceback: private details",
                        "stack": "secret stack",
                    },
                    3,
                ),
            ]
        )
        await dispatcher.dispatch_once()

    assert len(transport.sent) == 1
    assert len(transport.edited) == 1
    message_id, payload = transport.edited[0]
    assert message_id == "discord-turn-card"
    assert payload["type"] == "turn_error"
    assert payload["finalized"] is True
    assert "secret" not in json.dumps(payload)
    assert "Traceback" not in json.dumps(payload)


@pytest.mark.asyncio
async def test_subagent_background_progress_never_creates_taskdeck_message(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "background-turn.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        await reducer.persist(
            [
                _event(
                    "subagent.started",
                    {
                        "toolCallId": "subagent-tool",
                        "agentDisplayName": "Private investigator",
                    },
                    2,
                    agent_id="agent-1",
                    task_id="background-1",
                ),
                _event(
                    "assistant.message_delta",
                    {
                        "messageId": "agent-message",
                        "deltaContent": "private subagent output",
                    },
                    3,
                    source="sdk",
                    event_id="agent-delta",
                    message_id="agent-message",
                    agent_id="agent-1",
                ),
                _event(
                    "subagent.completed",
                    {
                        "toolCallId": "subagent-tool",
                        "agentDisplayName": "Private investigator",
                    },
                    4,
                    agent_id="agent-1",
                    task_id="background-1",
                ),
            ]
        )
        rows = await database.fetchall(
            "SELECT lane, payload FROM render_outbox WHERE session_id = 'turn-session'"
        )
        cards = await database.fetchall(
            "SELECT kind, state FROM task_card_projections WHERE sdk_session_id = 'turn-session'"
        )

    assert len(rows) == 1
    payload = json.loads(str(rows[0]["payload"]))
    assert rows[0]["lane"] == "assistant_stream"
    assert payload["content"] == "Copilot is working…"
    assert payload["status"] == {
        "title": "Subagent completed",
        "detail": "`Private investigator`",
        "event_type": "turn.subagent_completed",
    }
    assert "private subagent output" not in json.dumps(payload)
    assert cards


@pytest.mark.asyncio
async def test_final_turn_card_keeps_explicitly_linked_image_artifact(
    tmp_path: Path,
) -> None:
    image_path = tmp_path / "requested-result.png"
    Image.new("RGB", (4, 4), "blue").save(image_path)
    async with Database(tmp_path / "turn-image.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _queued(),
                _event(
                    "session.workspace_file_changed",
                    {"operation": "created", "path": image_path.name},
                    2,
                    source="sdk",
                    event_id="workspace-image",
                ),
                _event(
                    "assistant.message",
                    {
                        "messageId": "image-answer",
                        "content": f"Requested image: ![result]({image_path.name})",
                    },
                    3,
                    source="sdk",
                    event_id="image-answer",
                    message_id="image-answer",
                ),
            ]
        )
        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', terminal_at = 104
            WHERE submission_id = 'submission-1'
            """
        )
        await reducer.persist([_event("copilotd.snapshot.requested", {"topic": "activity"}, 4)])
        row = await database.fetchone(
            "SELECT payload FROM render_outbox WHERE session_id = 'turn-session'"
        )

    payload = json.loads(str(row["payload"]))
    plan = await _discord_render_plan(payload, allowed_roots=(tmp_path,))
    assets = [asset for batch in plan.batches for asset in batch.assets]
    assert payload["turn_render_key"] == "turn:submission-1"
    assert [asset.filename for asset in assets] == ["requested-result.png"]
    assert assets[0].content == image_path.read_bytes()
