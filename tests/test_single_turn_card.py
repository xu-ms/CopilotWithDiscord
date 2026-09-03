import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.core.volatile_content import opaque_content_key
from copilotd.discord_app import _discord_render_plan
from copilotd.render.outbox import RenderOutboxDispatcher, RenderPermanentError
from copilotd.render.sanitizer import redact_sensitive_text, sanitize_tool_command
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
    tool_call_id: str | None = None,
    interaction_id: str | None = None,
) -> AdaptedEvent:
    return AdaptedEvent(
        sdk_session_id="render-session",
        generation=1,
        fence_token=7,
        inbox_seq=sequence,
        source=source,
        raw_type=kind,
        raw_payload={"type": kind, "data": data},
        reducer_hash=f"render-{sequence}",
        persistence_class="durable" if source == "sdk" else "internal",
        received_at=100 + sequence,
        event_id=event_id,
        internal_event_id=None if source == "sdk" else f"render:{kind}:{sequence}",
        message_id=message_id,
        turn_id=turn_id,
        agent_id=agent_id,
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
            'render-thread', 'home', ?, 'render-session', 'attached',
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
            "thread_id": "render-thread",
            "origin": "app_message",
            "prompt": "build it",
            "prompt_hash": hashlib.sha256(b"build it").hexdigest(),
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


class _Transport:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.edited: list[tuple[str, str, dict[str, Any]]] = []

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        del session_id, idempotency_key
        self.sent.append((lane, payload))
        return f"discord-{len(self.sent)}"

    async def edit(
        self,
        *,
        session_id: str,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None:
        del session_id, idempotency_key
        self.edited.append((message_id, lane, payload))

    async def reaction(self, **_kwargs: Any) -> None:
        return None


class _PermanentOnceTransport(_Transport):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False
        self.reactions: list[dict[str, Any]] = []

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        if not self.failed:
            self.failed = True
            raise RenderPermanentError("asset " + "password" + "=" + "do-not-leak")
        return await super().send(
            session_id=session_id,
            lane=lane,
            payload=payload,
            idempotency_key=idempotency_key,
        )

    async def reaction(self, **kwargs: Any) -> None:
        self.reactions.append(kwargs["payload"])


@pytest.mark.parametrize(
    ("value", "secret"),
    [
        ("TOKEN=token-value command", "token-value"),
        ("command --password hunter2", "hunter2"),
        ("Authorization" + ": Bearer " + "header-value", "header-value"),
        ("https://" + "user" + ":" + "credential" + "@example.test", "credential"),
        (
            "eyJabcdefghijk.abcdefghijkl.abcdefghijkl",
            "eyJabcdefghijk.abcdefghijkl.abcdefghijkl",
        ),
    ],
)
def test_shared_redactor_removes_common_credentials(value: str, secret: str) -> None:
    sanitized = redact_sensitive_text(value)

    assert secret not in sanitized
    assert "[REDACTED]" in sanitized


def test_tool_command_sanitizer_never_serializes_structured_arguments() -> None:
    sanitized = sanitize_tool_command(
        {
            "arguments": {
                "command": '{"operation":"deploy","token":"raw-token"}',
                "env": {"TOKEN": "raw-env"},
            }
        }
    )

    assert sanitized == "(structured command omitted)"


@pytest.mark.parametrize(
    "command",
    [
        "deploy --token secret-value",
        "deploy --token=secret-value",
        "deploy --auth-token secret-value",
        "deploy --refresh-token=secret-value",
        "deploy --id-token secret-value",
        "deploy --client-secret secret-value",
        "deploy --client_secret=secret-value",
        "deploy --oauth-token secret-value",
        "deploy --access-key=secret-value",
        "deploy --github-oauth-token-file secret-value",
    ],
)
def test_shared_redactor_removes_exact_sensitive_token_options(command: str) -> None:
    sanitized = redact_sensitive_text(command)

    assert "secret-value" not in sanitized
    assert "[REDACTED]" in sanitized


def test_shared_redactor_preserves_benign_max_tokens_option() -> None:
    assert redact_sensitive_text("run --max-tokens 4096") == "run --max-tokens 4096"
    assert redact_sensitive_text("run --token-count 12") == "run --token-count 12"


@pytest.mark.asyncio
async def test_queue_and_reasoning_do_not_create_tool_or_working_card(tmp_path: Path) -> None:
    async with Database(tmp_path / "no-placeholder.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        queued_rows = await database.fetchall(
            "SELECT lane, payload FROM render_outbox WHERE lane != 'reaction'"
        )
        await reducer.persist(
            [
                _event(
                    "assistant.reasoning",
                    {"summary": "Inspecting."},
                    2,
                    source="sdk",
                    event_id="reasoning",
                )
            ]
        )
        rows = await database.fetchall("SELECT lane, payload FROM render_outbox")

    assert queued_rows == []
    assert all(row["lane"] != "tool" for row in rows)
    assert all(row["lane"] == "reaction" for row in rows)
    assert "Copilot is working…" not in json.dumps([dict(row) for row in rows])


@pytest.mark.asyncio
async def test_sequential_and_overlapping_tools_share_one_sanitized_card(
    tmp_path: Path,
) -> None:
    secret_values = (
        "top-secret",
        "hunter2",
        "bearer-value",
        "env-value",
        "progress-secret",
    )
    command = (
        "TOKEN=top-secret curl -H 'Authorization: Bearer bearer-value' "
        "--password hunter2 https://example.test?token=env-value"
    )
    transport = _Transport()
    async with Database(tmp_path / "tool-card.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        dispatcher = RenderOutboxDispatcher(database, transport)

        events = [
            _event(
                "tool.execution_start",
                {
                    "toolCallId": "tool-1",
                    "toolName": "shell",
                    "arguments": {
                        "command": command,
                        "password": "argument-password",
                    },
                    "headers": {"Authorization": "Bearer raw-header"},
                    "env": {"API_TOKEN": "raw-env"},
                },
                2,
                source="sdk",
                event_id="tool-1-start",
                tool_call_id="tool-1",
            ),
            _event(
                "tool.execution_progress",
                {
                    "toolCallId": "tool-1",
                    "progressMessage": "Checking TOKEN=progress-secret",
                    "outputDelta": "raw progress stdout",
                },
                3,
                source="sdk",
                event_id="tool-1-progress",
                tool_call_id="tool-1",
            ),
            _event(
                "tool.execution_start",
                {
                    "toolCallId": "tool-2",
                    "toolName": "tests",
                    "arguments": {"command": "pytest -q"},
                },
                4,
                source="sdk",
                event_id="tool-2-start",
                tool_call_id="tool-2",
            ),
            _event(
                "tool.execution_complete",
                {
                    "toolCallId": "tool-2",
                    "toolName": "tests",
                    "success": True,
                    "result": {"content": "raw stdout must stay internal"},
                },
                5,
                source="sdk",
                event_id="tool-2-complete",
                tool_call_id="tool-2",
            ),
            _event(
                "tool.execution_complete",
                {
                    "toolCallId": "tool-1",
                    "toolName": "shell",
                    "success": False,
                    "error": {
                        "message": "password=hunter2",
                        "stack": "raw stack must stay internal",
                    },
                },
                6,
                source="sdk",
                event_id="tool-1-complete",
                tool_call_id="tool-1",
            ),
        ]
        observed: list[dict[str, Any]] = []
        for event in events:
            await reducer.persist([event])
            row = await database.fetchone(
                "SELECT content_key, content_hash FROM render_outbox WHERE lane = 'tool'"
            )
            observed.append(
                database.content_store.require(
                    str(row["content_key"]),
                    expected_hash=str(row["content_hash"]),
                )
            )
            await dispatcher.dispatch_once()
        cards = await database.fetchall(
            """
            SELECT tool_call_id, state
            FROM tool_render_state ORDER BY tool_call_id
            """
        )
        outbox_count = await database.fetchone(
            "SELECT COUNT(*) AS count FROM render_outbox WHERE lane = 'tool'"
        )
        mapping_count = await database.fetchone(
            "SELECT COUNT(*) AS count FROM render_messages WHERE logical_key LIKE 'tool-card:%'"
        )

    assert observed[0]["tool"]["name"] == "shell"
    assert observed[0]["finalized"] is False
    assert observed[1]["tool"]["progress"] == "Checking TOKEN=[REDACTED]"
    assert observed[2]["tool"]["name"] == "tests"
    assert observed[3]["tool"]["name"] == "shell"
    assert observed[3]["tool"]["latest_state"] == "succeeded"
    assert observed[4]["tool"]["state"] == "failed"
    assert observed[4]["finalized"] is True
    encoded = json.dumps([observed, [dict(row) for row in cards]])
    assert all(secret not in encoded for secret in secret_values)
    assert "raw-header" not in encoded
    assert "raw-env" not in encoded
    assert "raw stdout" not in encoded
    assert "raw progress stdout" not in encoded
    assert "raw stack" not in encoded
    assert "argument-password" not in encoded
    assert all(key not in encoded for key in ('"arguments"', '"headers"', '"env"', '"result"'))
    assert "[REDACTED]" in encoded
    assert outbox_count["count"] == 1
    assert mapping_count["count"] == 1
    assert len(transport.sent) == 1
    assert {message_id for message_id, _, _ in transport.edited} == {"discord-1"}


@pytest.mark.asyncio
async def test_successful_view_of_session_png_attaches_image_to_tool_card(
    tmp_path: Path,
) -> None:
    session_state = tmp_path / "session-state"
    image = session_state / "render-session" / "files" / "report.png"
    image.parent.mkdir(parents=True)
    image.write_bytes(b"png fixture")
    async with Database(tmp_path / "view-image.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database, session_state_dir=session_state)
        await reducer.persist([_queued()])
        await reducer.persist(
            [
                _event(
                    "tool.execution_start",
                    {
                        "toolCallId": "view-1",
                        "toolName": "view",
                        "arguments": {"path": str(image)},
                    },
                    2,
                    source="sdk",
                    event_id="view-start",
                    tool_call_id="view-1",
                ),
                _event(
                    "tool.execution_complete",
                    {
                        "toolCallId": "view-1",
                        "toolName": "view",
                        "success": True,
                    },
                    3,
                    source="sdk",
                    event_id="view-complete",
                    tool_call_id="view-1",
                ),
            ]
        )
        row = await database.fetchone(
            "SELECT content_key, content_hash FROM render_outbox WHERE lane = 'tool'"
        )
        payload = database.content_store.require(
            str(row["content_key"]),
            expected_hash=str(row["content_hash"]),
        )
        plan = await _discord_render_plan(payload)

    assert payload["attachments"][0]["path"] == str(image)
    assert [asset.filename for batch in plan.batches for asset in batch.assets] == [
        "report.png"
    ]


@pytest.mark.asyncio
async def test_view_image_outside_session_artifacts_is_not_attached(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "private.png"
    outside.write_bytes(b"not exposed")
    async with Database(tmp_path / "view-image-boundary.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(
            database,
            session_state_dir=tmp_path / "session-state",
        )
        await reducer.persist([_queued()])
        await reducer.persist(
            [
                _event(
                    "tool.execution_start",
                    {
                        "toolCallId": "view-1",
                        "toolName": "view",
                        "arguments": {"path": str(outside)},
                    },
                    2,
                    source="sdk",
                    event_id="view-start",
                    tool_call_id="view-1",
                ),
                _event(
                    "tool.execution_complete",
                    {
                        "toolCallId": "view-1",
                        "toolName": "view",
                        "success": True,
                    },
                    3,
                    source="sdk",
                    event_id="view-complete",
                    tool_call_id="view-1",
                ),
            ]
        )
        row = await database.fetchone(
            "SELECT content_key, content_hash FROM render_outbox WHERE lane = 'tool'"
        )
        payload = database.content_store.require(
            str(row["content_key"]),
            expected_hash=str(row["content_hash"]),
        )

    assert "attachments" not in payload


@pytest.mark.asyncio
async def test_sequential_tools_reopen_and_refinalize_the_same_card(
    tmp_path: Path,
) -> None:
    transport = _Transport()
    async with Database(tmp_path / "sequential-tool-card.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        dispatcher = RenderOutboxDispatcher(database, transport)
        events = [
            _event(
                "tool.execution_start",
                {
                    "toolCallId": "tool-1",
                    "toolName": "shell",
                    "arguments": {"command": "echo first"},
                },
                2,
                source="sdk",
                event_id="tool-1-start",
                tool_call_id="tool-1",
            ),
            _event(
                "tool.execution_complete",
                {"toolCallId": "tool-1", "toolName": "shell", "success": True},
                3,
                source="sdk",
                event_id="tool-1-complete",
                tool_call_id="tool-1",
            ),
            _event(
                "tool.execution_start",
                {
                    "toolCallId": "tool-2",
                    "toolName": "tests",
                    "arguments": {"command": "pytest focused"},
                },
                4,
                source="sdk",
                event_id="tool-2-start",
                tool_call_id="tool-2",
            ),
            _event(
                "tool.execution_complete",
                {"toolCallId": "tool-2", "toolName": "tests", "success": True},
                5,
                source="sdk",
                event_id="tool-2-complete",
                tool_call_id="tool-2",
            ),
        ]
        for event in events:
            await reducer.persist([event])
            await dispatcher.dispatch_once()

    assert len(transport.sent) == 1
    assert [payload["finalized"] for _, _, payload in transport.edited] == [
        True,
        False,
        True,
    ]
    reopened = transport.edited[1][2]
    assert reopened["tool"]["name"] == "tests"
    assert reopened["tool"]["command"] == "pytest focused"
    assert reopened["tool"]["state"] == "running"
    assert {message_id for message_id, _, _ in transport.edited} == {"discord-1"}


@pytest.mark.asyncio
async def test_assistant_message_ids_keep_distinct_lossless_families(tmp_path: Path) -> None:
    transport = _Transport()
    async with Database(tmp_path / "assistant-families.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist([_queued()])
        dispatcher = RenderOutboxDispatcher(database, transport)
        events = [
            _event(
                "assistant.message_delta",
                {"messageId": "message-1", "deltaContent": "First accumulated answer."},
                2,
                source="sdk",
                event_id="m1-delta",
                message_id="message-1",
            ),
            _event(
                "assistant.message",
                {"messageId": "message-1", "content": ""},
                3,
                source="sdk",
                event_id="m1-final",
                message_id="message-1",
            ),
            _event(
                "assistant.message_delta",
                {"messageId": "message-2", "deltaContent": "Second "},
                4,
                source="sdk",
                event_id="m2-delta-1",
                message_id="message-2",
            ),
            _event(
                "assistant.message_delta",
                {"messageId": "message-2", "deltaContent": "message body."},
                5,
                source="sdk",
                event_id="m2-delta-2",
                message_id="message-2",
            ),
            _event(
                "assistant.message",
                {"messageId": "message-2", "content": "Done."},
                6,
                source="sdk",
                event_id="m2-final",
                message_id="message-2",
            ),
        ]
        for event in events:
            await reducer.persist([event])
            await dispatcher.dispatch_once()
        streams = [
            (
                message_id,
                database.content_store.get(
                    opaque_content_key(
                        "assistant-stream",
                        "render-session",
                        message_id,
                        "",
                    )
                ),
            )
            for message_id in ("message-1", "message-2")
        ]
        outbox = await database.fetchall(
            """
            SELECT coalesce_key, finalized FROM render_outbox
            WHERE lane IN ('assistant_stream', 'assistant_final')
            ORDER BY coalesce_key
            """
        )
        mappings = await database.fetchall(
            """
            SELECT logical_key, discord_message_id, finalized FROM render_messages
            WHERE logical_key LIKE 'assistant:%' ORDER BY logical_key
            """
        )

    assert streams == [
        ("message-1", None),
        ("message-2", None),
    ]
    assert [row["coalesce_key"] for row in outbox] == [
        "assistant:message-1",
        "assistant:message-2",
    ]
    assert all(row["finalized"] for row in outbox)
    assert [tuple(row) for row in mappings] == [
        ("assistant:message-1", "discord-1", 1),
        ("assistant:message-2", "discord-2", 1),
    ]
    assert [payload["content"] for _, payload in transport.sent] == [
        "First accumulated answer.",
        "Second ",
    ]
    assert any(payload["content"] == "Second message body." for _, _, payload in transport.edited)


@pytest.mark.asyncio
async def test_empty_assistant_message_without_deltas_is_a_no_output_completion(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "empty-message.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _queued(),
                _event(
                    "assistant.message",
                    {"messageId": "empty-message", "content": ""},
                    2,
                    source="sdk",
                    event_id="empty-final",
                    message_id="empty-message",
                ),
            ]
        )
        assistant_rows = await database.fetchall(
            """
            SELECT payload FROM render_outbox
            WHERE lane IN ('assistant_stream', 'assistant_final')
            """
        )
        stream = database.content_store.get(
            opaque_content_key(
                "assistant-stream",
                "render-session",
                "empty-message",
                "",
            )
        )

    assert assistant_rows == []
    assert stream is None


@pytest.mark.asyncio
async def test_long_nonfinal_discord_text_updates_all_batches_cumulatively() -> None:
    content = "".join(f"{index:04d}:" + ("x" * 80) + "\n" for index in range(70))

    plan = await _discord_render_plan(
        {
            "type": "assistant.message_delta",
            "content": content,
            "finalized": False,
        }
    )

    assert len(plan.batches) > 2
    assert all(len(batch.content) <= 1850 for batch in plan.batches)
    assert all(batch.embeds == () and batch.assets == () for batch in plan.batches)
    assert "".join(batch.content for batch in plan.batches) == content


@pytest.mark.asyncio
async def test_final_plan_blocks_and_attachments_survive_durable_round_trip() -> None:
    payload = {
        "type": "assistant.message",
        "content": "\n\n".join(
            (
                "First block " + "a" * 1100,
                "Second block " + "b" * 1100,
                "Final block.",
            )
        ),
        "attachments": [
            {
                "filename": "plan.txt",
                "media_type": "text/plain",
                "content": "durable plan attachment",
            }
        ],
        "finalized": True,
    }

    before = await _discord_render_plan(payload)
    after = await _discord_render_plan(json.loads(json.dumps(payload)))

    def materialize(plan: Any) -> list[tuple[str, list[tuple[str, bytes]]]]:
        return [
            (
                batch.content,
                [(asset.filename, asset.content) for asset in batch.assets],
            )
            for batch in plan.batches
        ]

    assert materialize(after) == materialize(before)
    rendered = "\n".join(batch.content for batch in after.batches)
    assert "First block" in rendered
    assert "Second block" in rendered
    assert any(asset.filename == "plan.txt" for batch in after.batches for asset in batch.assets)


@pytest.mark.asyncio
async def test_failure_events_render_immediately_without_fabricated_success(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "failure-surface.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _queued(),
                _event(
                    "copilotd.submission.accepted",
                    {"submission_id": "submission-1", "message_id": "runtime-user"},
                    2,
                ),
            ]
        )
        await reducer.persist(
            [
                _event(
                    "model.call_failure",
                    {"message": "Model unavailable; token=private-token"},
                    3,
                    source="sdk",
                    event_id="model-failure",
                )
            ]
        )
        status = await database.fetchone(
            "SELECT content_key, content_hash FROM render_outbox WHERE lane = 'status'"
        )
        reaction = await database.fetchone(
            """
            SELECT desired_state, terminal FROM submission_reactions
            WHERE submission_id = 'submission-1'
            """
        )
        all_payloads = await database.fetchall(
            "SELECT content_key, content_hash FROM render_outbox"
        )

    payload = database.content_store.require(
        str(status["content_key"]),
        expected_hash=str(status["content_hash"]),
    )
    encoded = json.dumps(
        [
            database.content_store.require(
                str(row["content_key"]),
                expected_hash=str(row["content_hash"]),
            )
            for row in all_payloads
        ]
    )
    assert payload["type"] == "model.call_failure"
    assert payload["status"]["event_type"] == "model.call_failure"
    assert payload["finalized"] is True
    assert "[REDACTED]" in encoded
    assert "private-token" not in encoded
    assert "Copilot completed the request." not in encoded
    assert tuple(reaction) == ("failed", 1)


@pytest.mark.asyncio
async def test_permanent_render_failure_surfaces_once_and_sets_failed_reaction(
    tmp_path: Path,
) -> None:
    transport = _PermanentOnceTransport()
    async with Database(tmp_path / "render-fallback.sqlite3") as database:
        await _binding(database, tmp_path)
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _queued(),
                _event(
                    "assistant.message",
                    {"messageId": "message-1", "content": "Original response."},
                    2,
                    source="sdk",
                    event_id="message-1-final",
                    message_id="message-1",
                ),
            ]
        )
        dispatcher = RenderOutboxDispatcher(database, transport)
        assert await dispatcher.dispatch_once() == 1
        assert await dispatcher.dispatch_once() == 1
        outbox = await database.fetchone(
            """
            SELECT state, last_error FROM render_outbox
            WHERE lane IN ('assistant_stream', 'assistant_final')
            """
        )
        reaction = await database.fetchone(
            """
            SELECT desired_state, delivered_state, resume_state, last_error
            FROM submission_reactions WHERE submission_id = 'submission-1'
            """
        )
        await reducer.persist(
            [
                _event(
                    "assistant.reasoning",
                    {"summary": "Retrying after render failure."},
                    3,
                    source="sdk",
                    event_id="reasoning-after-render-failure",
                )
            ]
        )
        after_reasoning = await database.fetchone(
            """
            SELECT desired_state, resume_state FROM submission_reactions
            WHERE submission_id = 'submission-1'
            """
        )

    assert outbox["state"] == "blocked"
    assert "do-not-leak" not in outbox["last_error"]
    assert [payload["type"] for _, payload in transport.sent] == ["render.failure"]
    assert "Original response." not in json.dumps(transport.sent)
    assert (reaction["desired_state"], reaction["delivered_state"]) == ("failed", "failed")
    assert json.loads(str(reaction["resume_state"]))["kind"] == "render_failed"
    assert reaction["last_error"] is None
    assert outbox["last_error"] == "renderpermanenterror"
    assert [payload["state"] for payload in transport.reactions] == ["failed"]
    assert after_reasoning["desired_state"] == "failed"
    assert json.loads(str(after_reasoning["resume_state"]))["kind"] == "render_failed"
