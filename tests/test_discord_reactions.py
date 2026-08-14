import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.render.outbox import (
    RenderOutboxDispatcher,
    RenderPermanentError,
    recover_reaction_outbox,
)
from copilotd.storage.database import Database


def _event(
    kind: str,
    data: dict[str, Any],
    sequence: int,
    *,
    source: str = "internal",
    event_id: str | None = None,
    interaction_id: str | None = None,
    turn_id: str | None = None,
    tool_call_id: str | None = None,
) -> AdaptedEvent:
    return AdaptedEvent(
        sdk_session_id="reaction-session",
        generation=1,
        fence_token=7,
        inbox_seq=sequence,
        source=source,
        raw_type=kind,
        raw_payload={"type": kind, "data": data},
        reducer_hash=f"reaction-{sequence}",
        persistence_class="internal" if source == "internal" else "durable",
        received_at=100 + sequence,
        event_id=event_id,
        internal_event_id=None if source == "sdk" else f"reaction:{kind}:{sequence}",
        interaction_id=interaction_id,
        turn_id=turn_id,
        tool_call_id=tool_call_id,
    )


async def _binding(database: Database) -> None:
    await database.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            attachment_state, permission_posture, runtime_generation,
            owner_fence_token, created_at, updated_at
        ) VALUES (
            'reaction-thread', 'home', '/workspace', 'reaction-session',
            'attached', 'verified_allow_all', 1, 7, 1, 1
        )
        """
    )
    await database.execute(
        """
        INSERT INTO session_owner_leases(
            sdk_session_id, owner_id, fence_token,
            acquired_at, renewed_at, expires_at
        ) VALUES ('reaction-session', 'reaction-owner', 7, 1, 1, 9999999999)
        """
    )


def _queued(sequence: int = 1) -> AdaptedEvent:
    return _event(
        "copilotd.submission.queued",
        {
            "submission_id": "submission-1",
            "thread_id": "reaction-thread",
            "origin": "app_message",
            "prompt": "hello",
            "prompt_hash": hashlib.sha256(b"hello").hexdigest(),
            "requested_mode": "interactive",
            "requested_model_config": {},
            "requested_agent": "default",
            "requested_session_config_version": 1,
            "requested_delivery": "enqueue",
            "attachment_count": 0,
            "discord_source_channel_id": "100",
            "discord_source_message_id": "200",
            "created_at": 100,
        },
        sequence,
    )


def _queued_submission(
    submission_id: str,
    source_message_id: str,
    prompt: str,
    sequence: int,
) -> AdaptedEvent:
    event = _queued(sequence)
    payload = dict(event.raw_payload)
    data = dict(payload["data"])
    data.update(
        submission_id=submission_id,
        prompt=prompt,
        prompt_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        discord_source_message_id=source_message_id,
    )
    payload["data"] = data
    return replace(
        event,
        raw_payload=payload,
        reducer_hash=f"queued-{submission_id}",
        internal_event_id=f"submission:{submission_id}:queued",
    )


async def _state(database: Database) -> dict[str, Any]:
    row = await database.fetchone(
        """
        SELECT desired_state, resume_state, delivered_state, revision,
               delivered_revision, terminal, runtime_generation,
               owner_fence_token, last_error
        FROM submission_reactions WHERE submission_id = 'submission-1'
        """
    )
    assert row is not None
    return dict(row)


@pytest.mark.asyncio
async def test_reducer_tracks_complete_monotonic_reaction_chain_and_deduplicates(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "reaction-chain.sqlite3") as database:
        await _binding(database)
        reducer = JournalReducer(database)
        assert await reducer.persist([_queued()]) == 1
        assert (await _state(database))["desired_state"] == "accepted"

        assert (
            await reducer.persist(
                [
                    _event(
                        "copilotd.submission.accepted",
                        {"submission_id": "submission-1", "message_id": "runtime-user-1"},
                        2,
                    ),
                    _event(
                        "user.message",
                        {"content": "hello", "agentMode": "interactive", "attachments": []},
                        3,
                        source="sdk",
                        event_id="runtime-user-1",
                    ),
                ]
            )
            == 2
        )
        assert (await _state(database))["desired_state"] == "reasoning"

        assert (
            await reducer.persist(
                [
                    _event(
                        "tool.execution_progress",
                        {"toolCallId": "tool-1", "progressMessage": "working"},
                        4,
                        source="sdk",
                        event_id="tool-progress-1",
                        tool_call_id="tool-1",
                    )
                ]
            )
            == 1
        )
        action = await _state(database)
        assert (action["desired_state"], action["revision"]) == ("action", 3)

        assert (
            await reducer.persist(
                [
                    _event(
                        "tool.execution_progress",
                        {"toolCallId": "tool-1", "progressMessage": "still working"},
                        5,
                        source="sdk",
                        event_id="tool-progress-2",
                        tool_call_id="tool-1",
                    )
                ]
            )
            == 1
        )
        assert (await _state(database))["revision"] == 3

        request = {
            "interaction_id": "interaction-1",
            "thread_id": "reaction-thread",
            "kind": "user_input",
            "expires_at": 1000,
            "question": "continue?",
        }
        assert (
            await reducer.persist(
                [
                    _event(
                        "copilotd.interaction.requested",
                        request,
                        6,
                        interaction_id="interaction-1",
                    )
                ]
            )
            == 1
        )
        unresolved = await _state(database)
        assert unresolved["desired_state"] == "unresolved"
        assert unresolved["resume_state"] == "action"

        assert (
            await reducer.persist(
                [
                    _event(
                        "copilotd.interaction.resolved",
                        {
                            "interaction_id": "interaction-1",
                            "state": "resolved",
                            "response": {"selection": 0},
                        },
                        7,
                        interaction_id="interaction-1",
                    )
                ]
            )
            == 1
        )
        assert (await _state(database))["desired_state"] == "action"

        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', terminal_at = 108,
                completion_basis = 'test_reliable_terminal'
            WHERE submission_id = 'submission-1'
            """
        )
        assert (
            await reducer.persist([_event("copilotd.snapshot.requested", {"topic": "activity"}, 8)])
            == 1
        )
        terminal = await _state(database)
        outbox = await database.fetchone(
            """
            SELECT payload, payload_revision, state
            FROM render_outbox WHERE lane = 'reaction'
            """
        )

    assert terminal["desired_state"] == "succeeded"
    assert terminal["terminal"] == 1
    assert terminal["revision"] == 6
    payload = json.loads(str(outbox["payload"]))
    assert payload["source_channel_id"] == "100"
    assert payload["source_message_id"] == "200"
    assert payload["emoji"] == "✅"
    assert payload["finalized"] is True


@pytest.mark.asyncio
async def test_rejected_submission_gets_failed_reaction_without_parsing_correlation_key(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "reaction-rejected.sqlite3") as database:
        await _binding(database)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        await reducer.persist(
            [_event("copilotd.submission.rejected", {"submission_id": "submission-1"}, 2)]
        )
        reaction = await _state(database)
        submission = await database.fetchone(
            """
            SELECT discord_source_channel_id, discord_source_message_id, state
            FROM submissions WHERE submission_id = 'submission-1'
            """
        )

    assert reaction["desired_state"] == "failed"
    assert reaction["terminal"] == 1
    assert dict(submission) == {
        "discord_source_channel_id": "100",
        "discord_source_message_id": "200",
        "state": "rejected",
    }


@pytest.mark.asyncio
async def test_three_fifo_messages_keep_independent_reaction_targets(tmp_path: Path) -> None:
    async with Database(tmp_path / "reaction-fifo.sqlite3") as database:
        await _binding(database)
        reducer = JournalReducer(database)
        queued = [
            _queued_submission("submission-1", "201", "one", 1),
            _queued_submission("submission-2", "202", "two", 2),
            _queued_submission("submission-3", "203", "three", 3),
        ]
        assert await reducer.persist(queued) == 3
        initial = await database.fetchall(
            """
            SELECT submission_id, source_message_id, desired_state
            FROM submission_reactions ORDER BY source_message_id
            """
        )
        assert [tuple(row) for row in initial] == [
            ("submission-1", "201", "accepted"),
            ("submission-2", "202", "accepted"),
            ("submission-3", "203", "accepted"),
        ]

        await reducer.persist(
            [
                _event(
                    "copilotd.submission.accepted",
                    {"submission_id": "submission-1", "message_id": "runtime-one"},
                    4,
                ),
                _event(
                    "user.message",
                    {"content": "one", "agentMode": "interactive", "attachments": []},
                    5,
                    source="sdk",
                    event_id="runtime-one",
                ),
                _event(
                    "tool.execution_progress",
                    {"toolCallId": "tool-one", "progressMessage": "working"},
                    6,
                    source="sdk",
                    event_id="tool-one-progress",
                    tool_call_id="tool-one",
                ),
            ]
        )
        after_first = await database.fetchall(
            """
            SELECT submission_id, desired_state
            FROM submission_reactions ORDER BY source_message_id
            """
        )
        assert [tuple(row) for row in after_first] == [
            ("submission-1", "action"),
            ("submission-2", "accepted"),
            ("submission-3", "accepted"),
        ]

        await database.execute(
            """
            UPDATE submissions
            SET state = 'semantic_complete', terminal_at = 107
            WHERE submission_id = 'submission-1'
            """
        )
        await reducer.persist(
            [
                _event("copilotd.snapshot.requested", {"topic": "activity"}, 7),
                _event(
                    "copilotd.submission.accepted",
                    {"submission_id": "submission-2", "message_id": "runtime-two"},
                    8,
                ),
                _event(
                    "user.message",
                    {"content": "two", "agentMode": "interactive", "attachments": []},
                    9,
                    source="sdk",
                    event_id="runtime-two",
                ),
            ]
        )
        after_second = await database.fetchall(
            """
            SELECT submission_id, source_message_id, desired_state
            FROM submission_reactions ORDER BY source_message_id
            """
        )

    assert [tuple(row) for row in after_second] == [
        ("submission-1", "201", "succeeded"),
        ("submission-2", "202", "reasoning"),
        ("submission-3", "203", "accepted"),
    ]


class _ReactionTransport:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.reactions: list[tuple[str, dict[str, Any], str]] = []

    async def send(self, **_kwargs: Any) -> str:
        return "unused"

    async def edit(self, **_kwargs: Any) -> None:
        return None

    async def reaction(
        self,
        *,
        session_id: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None:
        if self.error is not None:
            raise self.error
        self.reactions.append((session_id, payload, idempotency_key))


@pytest.mark.asyncio
async def test_reaction_delivery_is_durable_idempotent_and_fenced(tmp_path: Path) -> None:
    path = tmp_path / "reaction-delivery.sqlite3"
    transport = _ReactionTransport()
    async with Database(path) as database:
        await _binding(database)
        await JournalReducer(database).persist([_queued()])
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once() == 1
        delivered = await _state(database)
        assert delivered["delivered_state"] == "accepted"
        assert delivered["delivered_revision"] == 1
        assert await dispatcher.dispatch_once() == 0

        await database.execute(
            """
            UPDATE submission_reactions
            SET desired_state = 'reasoning', revision = 2,
                runtime_generation = 1, owner_fence_token = 7
            WHERE submission_id = 'submission-1'
            """
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_generation = 2, owner_fence_token = 8
            WHERE sdk_session_id = 'reaction-session'
            """
        )
        await database.execute(
            """
            UPDATE session_owner_leases
            SET fence_token = 8
            WHERE sdk_session_id = 'reaction-session'
            """
        )
        assert await recover_reaction_outbox(database) == 1
        recovered = await database.fetchone(
            "SELECT payload FROM render_outbox WHERE lane = 'reaction'"
        )
        recovered_payload = json.loads(str(recovered["payload"]))
        assert (recovered_payload["generation"], recovered_payload["fence_token"]) == (2, 8)
        assert await dispatcher.dispatch_once() == 1
        latest = await _state(database)

    assert [call[1]["state"] for call in transport.reactions] == ["accepted", "reasoning"]
    assert (latest["runtime_generation"], latest["owner_fence_token"]) == (2, 8)
    assert latest["delivered_state"] == "reasoning"


@pytest.mark.asyncio
async def test_reaction_permission_failure_is_diagnostic_not_submission_failure(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "reaction-permission.sqlite3") as database:
        await _binding(database)
        await JournalReducer(database).persist([_queued()])
        dispatcher = RenderOutboxDispatcher(
            database,
            _ReactionTransport(RenderPermanentError("missing Add Reactions")),
        )
        assert await dispatcher.dispatch_once() == 0
        reaction = await _state(database)
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = 'submission-1'"
        )
        outbox = await database.fetchone("SELECT state FROM render_outbox WHERE lane = 'reaction'")

    assert "missing Add Reactions" in reaction["last_error"]
    assert submission["state"] == "local_queued"
    assert outbox["state"] == "blocked"
