from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from importlib import resources
from pathlib import Path
from typing import Any

import pytest

import copilotd.storage.database as database_module
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.extensions import (
    CustomAgent,
    ExtensionConfigRepository,
    McpStdioServer,
    ProjectExtensionConfig,
)
from copilotd.core.models import AdaptedEvent
from copilotd.core.projects import ProjectRegistry
from copilotd.core.recovery import StartupRecoveryInventory
from copilotd.core.reducer import JournalReducer
from copilotd.core.scheduler import ScheduleKind, SchedulerRepository
from copilotd.core.volatile_content import (
    VolatileContentCapacityError,
    VolatileContentStore,
    opaque_content_key,
)
from copilotd.render.outbox import (
    RenderOutboxDispatcher,
    queue_admission_reaction,
)
from copilotd.storage.database import Database

_CONFIG_SENTINEL = "CONFIG-SENTINEL-7f4c"


class _Transport:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []
        self.reactions: list[dict[str, Any]] = []

    async def send(self, *, payload: dict[str, Any], **_kwargs: Any) -> str:
        self.sent.append(payload)
        return f"discord-{len(self.sent)}"

    async def edit(self, *, payload: dict[str, Any], **_kwargs: Any) -> None:
        self.edited.append(payload)

    async def reaction(self, *, payload: dict[str, Any], **_kwargs: Any) -> None:
        self.reactions.append(payload)


def _event(
    raw_type: str,
    data: dict[str, Any],
    seq: int,
    *,
    message_id: str | None = None,
    tool_call_id: str | None = None,
    source: str = "sdk",
) -> AdaptedEvent:
    return AdaptedEvent(
        sdk_session_id="sentinel-session",
        generation=1,
        fence_token=7,
        inbox_seq=seq,
        source=source,
        raw_type=raw_type,
        raw_payload={"type": raw_type, "data": data},
        reducer_hash=f"{seq:064x}",
        persistence_class="durable",
        received_at=float(seq),
        event_id=f"event-{seq}",
        message_id=message_id,
        tool_call_id=tool_call_id,
    )


async def _binding(database: Database, root: Path) -> None:
    await SessionBindingRepository(database).create(
        thread_id="sentinel-thread",
        sdk_session_id="sentinel-session",
        cwd_snapshot=root,
        project_source="explicit",
        session_config_snapshot={
            "custom_agents": [{"prompt": _CONFIG_SENTINEL}],
        },
        channel_config_snapshot={
            "layout": "forum",
            "configuration": _CONFIG_SENTINEL,
        },
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET attachment_state = 'attached', permission_posture = 'verified_allow_all',
            runtime_mode = 'interactive', runtime_agent = 'default',
            runtime_generation = 1, owner_fence_token = 7
        WHERE sdk_session_id = 'sentinel-session'
        """
    )


async def _write_all_sentinels(
    database: Database,
    store: VolatileContentStore,
    root: Path,
) -> tuple[str, ...]:
    sentinels = (
        "USER-PROMPT-5c7e",
        "ASSISTANT-DELTA-38a1",
        "ASSISTANT-FINAL-c04b",
        "REASONING-91bd",
        "TOOL-COMMAND-e1ad",
        "TOOL-OUTPUT-8cef",
        "TOOL-ERROR-266e",
        "INTERACTION-QUESTION-a9d3",
        "INTERACTION-RESPONSE-a5f7",
        "PROTOCOL-PAYLOAD-e48f",
        "SCHEDULER-PROMPT-1ae3",
        "RENDER-PAYLOAD-2f3c",
        "NATIVE-QUEUE-DISPLAY-4c2a",
        "SCHEDULE-BASELINE-b6f1",
        "SCHEDULE-RESULT-57de",
    )
    projects = ProjectRegistry(database, resolved_home=root)
    await projects.initialize()
    project = await projects.bind("config-channel", root)
    await projects.set_layout("config-channel", "forum")
    await projects.set_mcp_server(
        "config-channel",
        name="config-mcp",
        transport="stdio",
        config={"command": _CONFIG_SENTINEL},
    )
    await projects.set_custom_agent(
        "config-channel",
        name="config-agent",
        description=_CONFIG_SENTINEL,
        prompt=_CONFIG_SENTINEL,
        tools=("read",),
    )
    await ExtensionConfigRepository(database).publish(
        project,
        ProjectExtensionConfig(
            mcp_servers=(
                McpStdioServer(
                    name="extension-mcp",
                    command=_CONFIG_SENTINEL,
                ),
            ),
            custom_agents=(
                CustomAgent(
                    name="extension-agent",
                    description=_CONFIG_SENTINEL,
                    prompt=_CONFIG_SENTINEL,
                    tools=("read",),
                ),
            ),
        ),
    )
    await _binding(database, root)
    reducer = JournalReducer(database, content_store=store)
    queued = _event(
        "copilotd.submission.queued",
        {
            "submission_id": "submission-1",
            "thread_id": "sentinel-thread",
            "prompt": sentinels[0],
            "prompt_hash": "0" * 64,
            "requested_mode": "interactive",
            "requested_model_config": {},
            "requested_agent": "default",
            "requested_session_config_version": 1,
            "requested_delivery": "enqueue",
            "discord_source_channel_id": "channel-1",
            "discord_source_message_id": "source-1",
        },
        1,
    )
    queued = AdaptedEvent(
        **{name: getattr(queued, name) for name in queued.__dataclass_fields__ if name != "source"},
        source="internal",
    )
    events = [
        queued,
        _event(
            "assistant.message_delta",
            {
                "messageId": "message-1",
                "deltaContent": sentinels[1] + sentinels[11],
            },
            2,
            message_id="message-1",
        ),
        _event(
            "assistant.reasoning",
            {"summary": sentinels[3]},
            3,
        ),
        _event(
            "tool.execution_start",
            {
                "toolCallId": "tool-1",
                "toolName": "shell",
                "submission_id": "submission-1",
                "arguments": {"command": sentinels[4]},
            },
            4,
            tool_call_id="tool-1",
        ),
        _event(
            "tool.execution_progress",
            {
                "toolCallId": "tool-1",
                "submission_id": "submission-1",
                "outputDelta": sentinels[5],
            },
            5,
            tool_call_id="tool-1",
        ),
        _event(
            "tool.execution_complete",
            {
                "toolCallId": "tool-1",
                "submission_id": "submission-1",
                "success": False,
                "error": {"message": sentinels[6]},
            },
            6,
            tool_call_id="tool-1",
        ),
        _event(
            "assistant.message",
            {"messageId": "message-1", "content": sentinels[2]},
            7,
            message_id="message-1",
        ),
        _event(
            "copilotd.interaction.requested",
            {
                "interaction_id": "interaction-1",
                "thread_id": "sentinel-thread",
                "kind": "user_input",
                "question": sentinels[7],
                "state": "pending",
                "response_plane": "direct_handler",
                "expires_at": time.time() + 600,
            },
            8,
        ),
        _event(
            "copilotd.interaction.resolved",
            {
                "interaction_id": "interaction-1",
                "thread_id": "sentinel-thread",
                "kind": "user_input",
                "state": "resolved",
                "response": {"answer": sentinels[8]},
                "display_response": sentinels[8],
            },
            9,
        ),
        _event(
            "sampling.requested",
            {
                "requestId": "protocol-1",
                "prompt": sentinels[9],
            },
            10,
        ),
        _event(
            "copilotd.snapshot.requested",
            {"topic": "queue"},
            11,
            source="internal",
        ),
        _event(
            "copilotd.snapshot.observed",
            {
                "topic": "queue",
                "epoch": 1,
                "snapshot_id": "sentinel-queue-snapshot",
                "query_start_sdk_receive_seq": 0,
                "query_end_sdk_receive_seq": 0,
                "observed_at": 12,
                "payload": {
                    "items": [
                        {
                            "id": "native-queue-sentinel",
                            "agentMode": "interactive",
                            "displayText": sentinels[12],
                        }
                    ],
                    "steering_messages": [],
                },
            },
            12,
            source="internal",
        ),
        _event(
            "copilotd.schedule_action.pending",
            {
                "action_id": "schedule-action-sentinel",
                "builtin_name": "after",
                "action": "create",
                "input_hash": "3" * 64,
                "baseline_ids": [sentinels[13]],
            },
            13,
            source="internal",
        ),
        _event(
            "copilotd.schedule_action.settled",
            {
                "action_id": "schedule-action-sentinel",
                "builtin_name": "after",
                "action": "create",
                "state": "rejected",
                "result": {"message": sentinels[14]},
            },
            14,
            source="internal",
        ),
    ]
    assert await reducer.persist(events) == len(events)
    await SchedulerRepository(database, content_store=store).create(
        kind=ScheduleKind.MESSAGE,
        expression="at:2030-01-01T00:00:00Z",
        timezone="UTC",
        payload={"text": sentinels[10]},
        target_snapshot={"thread_id": "sentinel-thread"},
        thread_id="sentinel-thread",
        now=1,
    )
    return sentinels


@pytest.mark.asyncio
async def test_sentinel_content_never_reaches_sqlite_or_wal(tmp_path: Path) -> None:
    database_path = tmp_path / "state-only.sqlite3"
    store = VolatileContentStore()
    transport = _Transport()
    database = Database(database_path)
    await database.open()
    sentinels = await _write_all_sentinels(database, store, tmp_path)
    dispatcher = RenderOutboxDispatcher(
        database,
        transport,
        content_store=store,
    )
    await dispatcher.drain(deadline_seconds=1)
    assert any(sentinels[1] in str(payload) for payload in transport.sent + transport.edited)
    assert any(sentinels[4] in str(payload) for payload in transport.sent + transport.edited)

    rows = await database.fetchall("SELECT raw_payload FROM event_journal")
    assert all('"payload_state":"discarded"' in str(row["raw_payload"]) for row in rows)
    assert (await database.fetchone("SELECT COUNT(*) FROM message_queue WHERE prompt != ''"))[
        0
    ] == 0
    assert (
        await database.fetchone(
            """
            SELECT COUNT(*) FROM pending_interactions
            WHERE payload != '{}' OR response IS NOT NULL OR form_schema IS NOT NULL
            """
        )
    )[0] == 0
    native_queue = await database.fetchone(
        """
        SELECT display_text, display_text_hash
        FROM native_queue_items WHERE item_id = 'native-queue-sentinel'
        """
    )
    schedule_action = await database.fetchone(
        """
        SELECT baseline_json, result_json, baseline_hash, result_hash
        FROM runtime_schedule_actions WHERE action_id = 'schedule-action-sentinel'
        """
    )
    assert native_queue is not None
    assert native_queue["display_text"] is None
    assert native_queue["display_text_hash"] is not None
    assert schedule_action is not None
    assert schedule_action["baseline_json"] is None
    assert schedule_action["result_json"] is None
    assert schedule_action["baseline_hash"] is not None
    assert schedule_action["result_hash"] is not None
    await database.close()

    persisted = database_path.read_bytes()
    wal_path = database_path.with_name(database_path.name + "-wal")
    wal = wal_path.read_bytes() if wal_path.exists() else b""
    for sentinel in sentinels:
        encoded = sentinel.encode()
        assert encoded not in persisted
        assert encoded not in wal

    database = Database(database_path)
    await database.open()
    projects = ProjectRegistry(database, resolved_home=tmp_path)
    await projects.initialize()
    project = await projects.resolve("config-channel")
    agents = await projects.list_custom_agents("config-channel")
    servers = await projects.list_mcp_servers("config-channel")
    extension = await ExtensionConfigRepository(database).latest(project)
    binding = await SessionBindingRepository(database).by_thread("sentinel-thread")
    settings = await projects.channel_settings("config-channel")
    await database.close()

    assert agents[0].description == _CONFIG_SENTINEL
    assert agents[0].prompt == _CONFIG_SENTINEL
    assert servers[0].config["command"] == _CONFIG_SENTINEL
    assert extension.config.custom_agents[0].prompt == _CONFIG_SENTINEL
    assert extension.config.mcp_servers[0].command == _CONFIG_SENTINEL
    assert binding is not None
    assert binding.session_config_snapshot["custom_agents"][0]["prompt"] == _CONFIG_SENTINEL
    assert binding.channel_config_snapshot["configuration"] == _CONFIG_SENTINEL
    assert settings[:2] == ("forum", False)
    assert _CONFIG_SENTINEL.encode() in database_path.read_bytes()


@pytest.mark.asyncio
async def test_state_only_write_guards_reject_content_columns(tmp_path: Path) -> None:
    async with Database(tmp_path / "guards.sqlite3") as database:
        await _binding(database, tmp_path)
        store = VolatileContentStore()
        await JournalReducer(database, content_store=store).persist(
            [
                AdaptedEvent(
                    sdk_session_id="sentinel-session",
                    generation=1,
                    fence_token=7,
                    inbox_seq=1,
                    source="internal",
                    raw_type="copilotd.submission.queued",
                    raw_payload={
                        "type": "copilotd.submission.queued",
                        "data": {
                            "submission_id": "guard-submission",
                            "thread_id": "sentinel-thread",
                            "prompt": "volatile",
                            "prompt_hash": "1" * 64,
                            "discord_source_channel_id": "channel",
                            "discord_source_message_id": "message",
                        },
                    },
                    reducer_hash="2" * 64,
                    persistence_class="internal",
                    received_at=1,
                    internal_event_id="guard-queued",
                )
            ]
        )
        with pytest.raises(sqlite3.IntegrityError, match="state_only:message_queue"):
            await database.execute(
                "UPDATE message_queue SET prompt = 'forbidden' WHERE id = 'guard-submission'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="state_only:render_outbox"):
            await database.execute(
                """
                UPDATE render_outbox SET payload = '{"content":"forbidden"}'
                WHERE id = (SELECT id FROM render_outbox LIMIT 1)
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="state_only:event_journal"):
            await database.execute(
                """
                UPDATE event_journal SET raw_payload = '{"content":"forbidden"}'
                WHERE journal_id = 1
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="state_only:native_queue_items"):
            await database.execute(
                """
                INSERT INTO native_queue_items(
                    sdk_session_id, item_id, display_text, state,
                    last_snapshot_id, last_seen_epoch, updated_at
                ) VALUES ('sentinel-session', 'forbidden-native', 'forbidden',
                          'present', 'snapshot', 1, 1)
                """
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="state_only:runtime_schedule_actions",
        ):
            await database.execute(
                """
                INSERT INTO runtime_schedule_actions(
                    action_id, sdk_session_id, builtin_name, action, input_hash,
                    baseline_json, state, created_at
                ) VALUES ('forbidden-action', 'sentinel-session', 'after', 'create',
                          'hash', '["forbidden"]', 'pending', 1)
                """
            )


@pytest.mark.asyncio
async def test_state_only_schema_has_no_content_stream_or_tool_artifact_tables(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "schema-denylist.sqlite3") as database:
        tables = {
            str(row["name"])
            for row in await database.fetchall(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert {
            "render_streams",
            "tool_output_streams",
            "tool_spill_artifacts",
            "trusted_local_artifacts",
            "trusted_local_artifact_snapshots",
            "tool_activity_projections",
        }.isdisjoint(tables)
        tool_columns = {
            str(row["name"])
            for row in await database.fetchall("PRAGMA table_info(tool_render_state)")
        }
        assert {
            "tool_name",
            "sanitized_command",
            "progress_summary",
            "failure_summary",
        }.isdisjoint(tool_columns)
        turn_columns = {
            str(row["name"])
            for row in await database.fetchall("PRAGMA table_info(turn_render_state)")
        }
        assert "answer_payload" not in turn_columns
        trigger_count = await database.fetchone(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'trigger' AND name LIKE 'state_only_%'
            """
        )
        assert trigger_count is not None and int(trigger_count[0]) >= 30


@pytest.mark.asyncio
async def test_restart_truthfully_expires_missing_volatile_content(tmp_path: Path) -> None:
    database_path = tmp_path / "restart.sqlite3"
    first_store = VolatileContentStore()
    database = Database(database_path)
    await database.open()
    await _write_all_sentinels(database, first_store, tmp_path)
    await JournalReducer(database, content_store=first_store).persist(
        [
            _event(
                "copilotd.interaction.requested",
                {
                    "interaction_id": "interaction-pending",
                    "thread_id": "sentinel-thread",
                    "kind": "user_input",
                    "question": "pending volatile question",
                    "state": "pending",
                    "response_plane": "direct_handler",
                    "expires_at": time.time() + 600,
                },
                15,
            )
        ]
    )
    await database.close()

    second_store = VolatileContentStore()
    database = Database(database_path)
    await database.open()
    await StartupRecoveryInventory(database, content_store=second_store).run(now=time.time())
    queue = await database.fetchone("SELECT state FROM message_queue WHERE id = 'submission-1'")
    interaction = await database.fetchone(
        "SELECT state FROM pending_interactions WHERE interaction_id = 'interaction-pending'"
    )
    assert queue is not None and queue["state"] == "content_unavailable"
    assert interaction is not None and interaction["state"] == "content_unavailable"

    transport = _Transport()
    dispatcher = RenderOutboxDispatcher(
        database,
        transport,
        content_store=second_store,
    )
    await dispatcher.drain(deadline_seconds=1)
    unavailable = await database.fetchone(
        "SELECT COUNT(*) FROM render_outbox WHERE state = 'content_unavailable'"
    )
    assert unavailable is not None and int(unavailable[0]) > 0
    assert any(
        payload.get("type") == "render.content_unavailable"
        for payload in transport.sent + transport.edited
    )
    reaction = await database.fetchone(
        "SELECT desired_state FROM submission_reactions WHERE submission_id = 'submission-1'"
    )
    assert reaction is not None and reaction["desired_state"] == "failed"
    await database.close()


@pytest.mark.asyncio
async def test_replayed_sdk_event_rehydrates_volatile_render_without_db_content(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sdk-replay.sqlite3"
    event = _event(
        "assistant.message",
        {"messageId": "replayed-message", "content": "SDK-REPLAY-CONTENT-7d2a"},
        1,
        message_id="replayed-message",
    )
    database = Database(path)
    await database.open()
    assert await JournalReducer(database).persist([event]) == 1
    await database.close()

    database = Database(path)
    await database.open()
    assert await JournalReducer(database).persist([event]) == 0
    transport = _Transport()
    assert await RenderOutboxDispatcher(database, transport).dispatch_once(now=time.time()) == 1
    assert any(payload.get("content") == "SDK-REPLAY-CONTENT-7d2a" for payload in transport.sent)
    await database.close()
    assert b"SDK-REPLAY-CONTENT-7d2a" not in path.read_bytes()


@pytest.mark.asyncio
@pytest.mark.parametrize("fallback_state", ["pending", "sending", "blocked", "sent"])
async def test_sdk_rehydration_atomically_supersedes_every_fallback_state(
    tmp_path: Path,
    fallback_state: str,
) -> None:
    path = tmp_path / f"rehydrate-{fallback_state}.sqlite3"
    event = _event(
        "assistant.message_delta",
        {"messageId": "recovered-message", "deltaContent": "partial"},
        1,
        message_id="recovered-message",
    )
    recovered_event = _event(
        "assistant.message",
        {"messageId": "recovered-message", "content": "recovered content"},
        2,
        message_id="recovered-message",
    )
    database = Database(path)
    await database.open()
    assert await JournalReducer(database).persist([event]) == 1
    await database.close()

    database = Database(path)
    await database.open()
    transport = _Transport()
    dispatcher = RenderOutboxDispatcher(database, transport)
    dispatch_at = time.time() + 1
    assert await dispatcher.dispatch_once(now=dispatch_at) == 0
    stale_fallback = None
    if fallback_state == "sending":
        claimed_ids: list[str] = []
        claimed = await dispatcher._claim(
            limit=1,
            now=dispatch_at + 1,
            claimed_ids=claimed_ids,
        )
        assert len(claimed) == 1
        stale_fallback = claimed[0]
    else:
        await database.execute(
            """
            UPDATE render_outbox SET state = ?
            WHERE render_kind = 'content_unavailable'
            """,
            (fallback_state,),
        )

    assert await JournalReducer(database).persist([recovered_event]) == 1
    rows = await database.fetchall(
        """
        SELECT id, render_kind, state, content_key
        FROM render_outbox ORDER BY render_kind
        """
    )

    source = next(row for row in rows if row["render_kind"] == "assistant.message")
    fallback = next(row for row in rows if row["render_kind"] == "content_unavailable")
    assert (source["state"], source["content_key"]) == (
        "pending",
        opaque_content_key("render-outbox", str(source["id"])),
    )
    assert (fallback["state"], fallback["content_key"]) == ("superseded", None)
    if stale_fallback is not None:
        assert await dispatcher.dispatch_once(now=dispatch_at + 2) == 1
        assert await dispatcher._deliver(
            stale_fallback,
            now=dispatch_at + 3,
            live_clock=False,
        ) == (False, False)
        assert transport.edited == []
        assert [payload["content"] for payload in transport.sent] == ["recovered content"]
    await database.close()


@pytest.mark.asyncio
async def test_rehydrated_source_compensates_sent_fallback_before_future_dispatch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rehydrate-ordering.sqlite3"
    event = _event(
        "assistant.message",
        {"messageId": "ordered-message", "content": "RECOVERED-SDK-CONTENT-2d71"},
        1,
        message_id="ordered-message",
    )
    database = Database(path)
    await database.open()
    await _binding(database, tmp_path)
    await database.execute(
        """
        INSERT INTO submissions(
            submission_id, sdk_session_id, origin, prompt_hash, state,
            discord_source_channel_id, discord_source_message_id,
            terminal_at, created_at
        ) VALUES ('recovered-submission', 'sentinel-session', 'app_message',
                  'hash', 'semantic_complete', 'source-channel',
                  'source-message', 2, 1)
        """
    )
    await database.execute(
        """
        INSERT INTO submission_reactions(
            submission_id, sdk_session_id, source_channel_id, source_message_id,
            desired_state, revision, delivered_revision, runtime_generation,
            owner_fence_token, terminal, created_at, updated_at
        ) VALUES ('recovered-submission', 'sentinel-session', 'source-channel',
                  'source-message', 'succeeded', 1, 0, 1, 7, 1, 1, 1)
        """
    )
    assert await JournalReducer(database).persist([event]) == 1
    await database.execute(
        """
        UPDATE render_outbox
        SET source_submission_id = 'recovered-submission'
        WHERE render_kind = 'assistant.message'
        """
    )
    await database.close()

    transport = _Transport()
    database = Database(path)
    await database.open()
    dispatcher = RenderOutboxDispatcher(database, transport)
    dispatch_at = time.time() + 1
    assert await dispatcher.dispatch_once(now=dispatch_at) == 0
    assert await dispatcher.dispatch_once(now=dispatch_at + 1) == 2
    await database.close()
    assert [payload["type"] for payload in transport.sent] == ["render.content_unavailable"]
    assert [payload["state"] for payload in transport.reactions] == ["failed"]

    database = Database(path)
    await database.open()
    assert await JournalReducer(database).persist([event]) == 0
    compensated = await database.fetchone(
        """
        SELECT
          (SELECT state FROM render_outbox
           WHERE render_kind = 'assistant.message') AS source_state,
          (SELECT state FROM render_outbox
           WHERE render_kind = 'content_unavailable') AS fallback_state,
          (SELECT desired_state FROM submission_reactions
           WHERE submission_id = 'recovered-submission') AS reaction_state,
          (SELECT resume_state FROM submission_reactions
           WHERE submission_id = 'recovered-submission') AS resume_state
        """
    )
    dispatcher = RenderOutboxDispatcher(database, transport)
    assert await dispatcher.dispatch_once(now=dispatch_at + 2) == 2
    assert await dispatcher.dispatch_once(now=dispatch_at + 3) == 0
    await database.close()

    assert dict(compensated) == {
        "source_state": "pending",
        "fallback_state": "superseded",
        "reaction_state": "succeeded",
        "resume_state": None,
    }
    assert [payload["content"] for payload in transport.edited] == ["RECOVERED-SDK-CONTENT-2d71"]
    assert [payload["state"] for payload in transport.reactions] == [
        "failed",
        "succeeded",
    ]
    assert (
        sum(
            payload.get("type") == "render.content_unavailable"
            for payload in transport.sent + transport.edited
        )
        == 1
    )


@pytest.mark.asyncio
async def test_replay_of_sent_render_does_not_repopulate_tiny_content_store(
    tmp_path: Path,
) -> None:
    event = _event(
        "assistant.message",
        {"messageId": "sent-message", "content": "already delivered"},
        1,
        message_id="sent-message",
    )
    async with Database(tmp_path / "sent-replay-capacity.sqlite3") as database:
        database.content_store = VolatileContentStore(max_items=2, max_bytes=4096)
        reducer = JournalReducer(database)
        assert await reducer.persist([event]) == 1
        assert (
            await RenderOutboxDispatcher(database, _Transport()).dispatch_once(now=time.time() + 1)
            == 1
        )
        assert database.content_store.item_count == 0

        for _ in range(64):
            assert await reducer.persist([event]) == 0
            assert database.content_store.item_count == 0

        row = await database.fetchone(
            "SELECT state, content_key FROM render_outbox WHERE render_kind = 'assistant.message'"
        )

    assert dict(row) == {"state": "sent", "content_key": None}


def test_volatile_store_capacity_failure_preserves_existing_entries() -> None:
    store = VolatileContentStore(max_items=2, max_bytes=8)
    first = store.put("1111")
    second = store.put("2222")
    with pytest.raises(
        VolatileContentCapacityError,
        match="content_capacity_exceeded",
    ):
        store.put("3333")
    assert store.require(first.key) == "1111"
    assert store.require(second.key) == "2222"
    assert store.item_count == 2
    assert store.byte_count == 8

    with pytest.raises(VolatileContentCapacityError):
        store.put("11111", key=first.key)
    assert store.require(first.key) == "1111"
    assert store.require(second.key) == "2222"

    replacement = store.put("11", key=first.key)
    assert replacement.key == first.key
    assert store.require(first.key) == "11"
    assert store.require(second.key) == "2222"
    assert store.byte_count == 6


def test_volatile_store_nested_transactions_restore_exact_prior_values() -> None:
    store = VolatileContentStore(max_items=4, max_bytes=100)
    store.put("before", key="existing")

    with pytest.raises(RuntimeError, match="outer rollback"):
        with store.transaction():
            store.put("outer", key="existing")
            with pytest.raises(RuntimeError, match="inner rollback"):
                with store.transaction():
                    store.put("inner", key="existing")
                    raise RuntimeError("inner rollback")
            assert store.require("existing") == "outer"
            with store.transaction():
                store.put("committed inner", key="new")
            raise RuntimeError("outer rollback")

    assert store.require("existing") == "before"
    assert store.get("new") is None
    assert store.item_count == 1
    assert store.byte_count == len("before")


@pytest.mark.asyncio
async def test_repeated_admission_updates_use_one_volatile_key(tmp_path: Path) -> None:
    store = VolatileContentStore(max_items=1, max_bytes=4_096)
    async with Database(tmp_path / "admission-capacity.sqlite3") as database:
        for index in range(4_097):
            await queue_admission_reaction(
                database,
                source_channel_id="channel",
                source_message_id="message",
                state="accepted",
                emoji="👀",
                now=float(index),
                content_store=store,
            )
        row = await database.fetchone(
            """
            SELECT COUNT(*) AS row_count, payload_revision
            FROM render_outbox WHERE lane = 'admission_reaction'
            """
        )

    assert store.item_count == 1
    assert row is not None
    assert dict(row) == {"row_count": 1, "payload_revision": 1}


@pytest.mark.asyncio
async def test_streaming_updates_remain_bounded_to_family_keys(tmp_path: Path) -> None:
    store = VolatileContentStore(max_items=2, max_bytes=1_000_000)
    async with Database(tmp_path / "stream-capacity.sqlite3") as database:
        reducer = JournalReducer(database, content_store=store)
        events = [
            _event(
                "assistant.message_delta",
                {"messageId": "bounded-message", "deltaContent": "x"},
                index,
                message_id="bounded-message",
            )
            for index in range(1, 4_098)
        ]
        assert await reducer.persist(events) == 4_097
        row = await database.fetchone(
            """
            SELECT content_key, content_hash, payload_revision
            FROM render_outbox WHERE coalesce_key = 'assistant:bounded-message'
            """
        )

    assert store.item_count == 2
    assert row is not None and row["payload_revision"] == 4_097
    payload = store.require(
        str(row["content_key"]),
        expected_hash=str(row["content_hash"]),
    )
    assert payload["content"] == "x" * 4_097


def _create_v51_fixture(path: Path, sentinels: tuple[str, ...]) -> None:
    legacy_artifact = (
        path.parent / "sessions" / "legacy-session" / "artifacts" / "legacy-tool-output.txt"
    )
    legacy_artifact.parent.mkdir(parents=True, exist_ok=True)
    legacy_artifact.write_text(sentinels[3], encoding="utf-8")
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE schema_migrations(
            version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL
        )
        """
    )
    migration_root = resources.files("copilotd.storage.migrations")
    for migration in sorted(
        item
        for item in migration_root.iterdir()
        if item.name.endswith(".sql") and int(item.name.partition("_")[0]) <= 51
    ):
        connection.executescript(migration.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO schema_migrations VALUES (?, ?, 0)",
            (int(migration.name.partition("_")[0]), migration.name),
        )
    connection.executescript(
        """
        ALTER TABLE task_card_projections
            ADD COLUMN dependencies_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE task_card_projections
            ADD COLUMN artifact_links_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE task_card_projections
            ADD COLUMN can_promote INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE task_card_projections ADD COLUMN last_progress_at REAL;
        ALTER TABLE session_creation_intents
            ADD COLUMN project_config_snapshot TEXT NOT NULL DEFAULT '{}';
        ALTER TABLE session_creation_intents
            ADD COLUMN channel_config_snapshot TEXT NOT NULL DEFAULT '{}';
        ALTER TABLE session_creation_intents
            ADD COLUMN layout TEXT NOT NULL DEFAULT 'text';
        ALTER TABLE session_creation_intents
            ADD COLUMN project_config_version INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE session_creation_intents
            ADD COLUMN channel_config_version INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE session_creation_intents
            ADD COLUMN config_snapshot_state TEXT NOT NULL DEFAULT 'legacy_unverified';
        ALTER TABLE session_bindings
            ADD COLUMN session_config_snapshot TEXT NOT NULL DEFAULT '{}';
        ALTER TABLE session_bindings
            ADD COLUMN channel_config_snapshot TEXT NOT NULL DEFAULT '{}';
        ALTER TABLE session_bindings
            ADD COLUMN config_snapshot_state TEXT NOT NULL DEFAULT 'legacy_unverified';
        """
    )
    connection.execute(
        """
        INSERT INTO session_bindings(
            thread_id, project_source, cwd_snapshot, sdk_session_id,
            attachment_state, binding_intent, created_at, updated_at
        ) VALUES ('legacy-thread', 'explicit', '/repo', 'legacy-session',
                  'closed', 'closed', 1, 2)
        """
    )
    connection.execute(
        """
        INSERT INTO submissions(
            submission_id, sdk_session_id, origin, prompt_hash, state, created_at
        ) VALUES ('legacy-submission', 'legacy-session', 'app_message', 'hash',
                  'semantic_complete', 3)
        """
    )
    connection.execute(
        """
        INSERT INTO message_queue(
            id, thread_id, prompt, requested_mode_snapshot,
            requested_model_config_snapshot, requested_session_config_version,
            position, state, created_at, updated_at
        ) VALUES ('legacy-submission', 'legacy-thread', ?, 'interactive', '{}',
                  1, 1, 'submitted', 3, 4)
        """,
        (sentinels[0],),
    )
    connection.execute(
        """
        INSERT INTO event_journal(
            sdk_session_id, generation, inbox_seq, source, persistence_class,
            raw_type, reducer_hash, raw_payload, received_at
        ) VALUES ('legacy-session', 1, 1, 'sdk', 'durable',
                  'assistant.message', 'legacy-hash', ?, 4)
        """,
        (f'{{"data":{{"content":"{sentinels[1]}"}}}}',),
    )
    connection.execute(
        """
        INSERT INTO render_outbox(
            id, session_id, logical_seq, lane, idempotency_key, payload,
            state, next_attempt_at, created_at, updated_at
        ) VALUES ('legacy-render', 'legacy-session', 1, 'assistant_final',
                  'legacy-render', ?, 'sent', 0, 4, 4)
        """,
        (f'{{"content":"{sentinels[2]}","finalized":true}}',),
    )
    connection.execute(
        """
        INSERT INTO submission_reactions(
            submission_id, sdk_session_id, source_channel_id, source_message_id,
            desired_state, revision, runtime_generation, owner_fence_token,
            terminal, created_at, updated_at
        ) VALUES ('legacy-submission', 'legacy-session', 'channel', 'message',
                  'succeeded', 1, 1, 1, 1, 4, 4)
        """
    )
    connection.execute(
        """
        INSERT INTO render_streams(
            session_id, message_id, agent_id, content, finalized, updated_at
        ) VALUES ('legacy-session', 'legacy-message', '', ?, 1, 4)
        """,
        (sentinels[3],),
    )
    connection.execute(
        """
        INSERT INTO tool_spill_artifacts(
            session_id, tool_call_id, local_path, byte_size,
            sha256, finalized, updated_at
        ) VALUES ('legacy-session', 'legacy-tool', ?, ?, NULL, 1, 4)
        """,
        (str(legacy_artifact), len(sentinels[3].encode())),
    )
    connection.execute(
        """
        INSERT INTO native_queue_items(
            sdk_session_id, item_id, agent_mode, display_text, state,
            last_snapshot_id, last_seen_epoch, updated_at
        ) VALUES ('legacy-session', 'legacy-native-item', 'interactive', ?,
                  'present', 'legacy-snapshot', 1, 4)
        """,
        (sentinels[4],),
    )
    connection.execute(
        """
        INSERT INTO runtime_schedule_actions(
            action_id, sdk_session_id, builtin_name, action, input_hash,
            baseline_json, state, result_json, created_at, settled_at
        ) VALUES ('legacy-schedule-action', 'legacy-session', 'after', 'create',
                  'legacy-input-hash', ?, 'confirmed', ?, 4, 5)
        """,
        (f'["{sentinels[5]}"]', f'{{"message":"{sentinels[6]}"}}'),
    )
    connection.execute(
        """
        INSERT INTO projects(
            id, channel_id, root_path, cwd, config_version, state,
            created_at, updated_at
        ) VALUES ('legacy-project', 'legacy-config-channel', ?, ?, 1, 'active', 1, 1)
        """,
        (str(path.parent), str(path.parent)),
    )
    connection.execute(
        """
        INSERT INTO custom_agents(
            project_id, name, description, prompt, tools_json, enabled
        ) VALUES ('legacy-project', 'legacy-agent', ?, ?, '["read"]', 1)
        """,
        (_CONFIG_SENTINEL, _CONFIG_SENTINEL),
    )
    connection.execute(
        """
        INSERT INTO mcp_servers(
            project_id, name, transport, config_json, enabled, version
        ) VALUES ('legacy-project', 'legacy-mcp', 'stdio', ?, 1, 1)
        """,
        (json.dumps({"command": _CONFIG_SENTINEL}, sort_keys=True),),
    )
    connection.execute(
        """
        UPDATE session_bindings
        SET session_config_snapshot = ?,
            channel_config_snapshot = ?,
            config_snapshot_state = 'legacy_unverified'
        WHERE sdk_session_id = 'legacy-session'
        """,
        (
            json.dumps({"custom_agent_prompt": _CONFIG_SENTINEL}),
            json.dumps({"layout": "forum", "configuration": _CONFIG_SENTINEL}),
        ),
    )
    connection.execute(
        """
        INSERT INTO runtime_command_manifest(
            sdk_session_id, command_name, kind, description, aliases_json,
            allow_during_agent_execution, experimental, schedulable,
            input_schema_json, manifest_generation, state, last_seen_at
        ) VALUES ('legacy-session', 'legacy-command', 'builtin', ?, '[]',
                  0, 0, 1, '{"type":"object"}', 1, 'available', 1)
        """,
        (_CONFIG_SENTINEL,),
    )
    connection.execute(
        """
        INSERT INTO runtime_agent_manifest(
            sdk_session_id, agent_name, agent_id, display_name, description,
            source, user_invocable, metadata_json, manifest_generation,
            state, last_seen_at
        ) VALUES ('legacy-session', 'legacy-runtime-agent', 'legacy-agent-id',
                  ?, ?, 'custom', 1, ?, 1, 'available', 1)
        """,
        (
            _CONFIG_SENTINEL,
            _CONFIG_SENTINEL,
            json.dumps({"description": _CONFIG_SENTINEL}, sort_keys=True),
        ),
    )
    extension_config = ProjectExtensionConfig(
        mcp_servers=(McpStdioServer(name="legacy-extension-mcp", command=_CONFIG_SENTINEL),),
        custom_agents=(
            CustomAgent(
                name="legacy-extension-agent",
                description=_CONFIG_SENTINEL,
                prompt=_CONFIG_SENTINEL,
            ),
        ),
    ).normalized(path.parent)
    extension_json = extension_config.canonical_json()
    extension_payload = json.loads(extension_json)
    connection.execute(
        """
        INSERT INTO project_extension_config_generations(
            scope_key, version, project_id, project_source, cwd_snapshot,
            config_hash, config_json, created_at
        ) VALUES ('project:legacy-project', 1, 'legacy-project', 'explicit', ?,
                  ?, ?, 1)
        """,
        (
            str(path.parent),
            hashlib.sha256(extension_json.encode()).hexdigest(),
            extension_json,
        ),
    )
    connection.execute(
        """
        INSERT INTO project_extension_mcp_servers(
            scope_key, config_version, name, transport, config_json
        ) VALUES ('project:legacy-project', 1, 'legacy-extension-mcp', 'stdio', ?)
        """,
        (json.dumps(extension_payload["mcp_servers"][0], sort_keys=True),),
    )
    connection.execute(
        """
        INSERT INTO project_extension_custom_agents(
            scope_key, config_version, name, config_json
        ) VALUES ('project:legacy-project', 1, 'legacy-extension-agent', ?)
        """,
        (json.dumps(extension_payload["custom_agents"][0], sort_keys=True),),
    )
    connection.commit()
    connection.close()


@pytest.mark.asyncio
async def test_v51_migration_purges_content_and_preserves_lifecycle(
    tmp_path: Path,
) -> None:
    sentinels = tuple(f"LEGACY-SENTINEL-{index}-6f8a" for index in range(7))
    path = tmp_path / "legacy.sqlite3"
    _create_v51_fixture(path, sentinels)

    database = Database(path)
    await database.open()
    binding = await database.fetchone(
        "SELECT binding_intent, attachment_state FROM session_bindings"
    )
    submission = await database.fetchone(
        "SELECT state FROM submissions WHERE submission_id = 'legacy-submission'"
    )
    reaction = await database.fetchone(
        "SELECT desired_state FROM submission_reactions WHERE submission_id = 'legacy-submission'"
    )
    native_queue = await database.fetchone(
        """
        SELECT display_text, display_text_hash
        FROM native_queue_items WHERE item_id = 'legacy-native-item'
        """
    )
    schedule_action = await database.fetchone(
        """
        SELECT baseline_json, result_json, baseline_hash, result_hash
        FROM runtime_schedule_actions WHERE action_id = 'legacy-schedule-action'
        """
    )
    assert dict(binding) == {
        "binding_intent": "closed",
        "attachment_state": "closed",
    }
    assert submission is not None and submission["state"] == "semantic_complete"
    assert reaction is not None and reaction["desired_state"] == "succeeded"
    assert native_queue is not None and dict(native_queue) == {
        "display_text": None,
        "display_text_hash": None,
    }
    assert schedule_action is not None and dict(schedule_action) == {
        "baseline_json": None,
        "result_json": None,
        "baseline_hash": None,
        "result_hash": None,
    }
    assert (
        await database.fetchone(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'render_streams'"
        )
        is None
    )
    assert not (
        tmp_path / "sessions" / "legacy-session" / "artifacts" / "legacy-tool-output.txt"
    ).exists()
    await database.close()

    database = Database(path)
    await database.open()
    assert (await database.fetchone("SELECT MAX(version) FROM schema_migrations"))[0] == 53
    projects = ProjectRegistry(database, resolved_home=tmp_path)
    await projects.initialize()
    agents = await projects.list_custom_agents("legacy-config-channel")
    servers = await projects.list_mcp_servers("legacy-config-channel")
    extension = await ExtensionConfigRepository(database).for_session(
        project_source="explicit",
        project_id="legacy-project",
        cwd_snapshot=tmp_path,
        version=1,
    )
    binding = await database.fetchone(
        """
        SELECT session_config_snapshot, channel_config_snapshot, config_snapshot_state
        FROM session_bindings WHERE sdk_session_id = 'legacy-session'
        """
    )
    runtime_config = await database.fetchone(
        """
        SELECT
          (SELECT description FROM runtime_command_manifest
           WHERE command_name = 'legacy-command') AS command_description,
          (SELECT description FROM runtime_agent_manifest
           WHERE agent_name = 'legacy-runtime-agent') AS agent_description,
          (SELECT metadata_json FROM runtime_agent_manifest
           WHERE agent_name = 'legacy-runtime-agent') AS agent_metadata,
          (SELECT config_json FROM project_extension_mcp_servers
           WHERE name = 'legacy-extension-mcp') AS extension_mcp,
          (SELECT config_json FROM project_extension_custom_agents
           WHERE name = 'legacy-extension-agent') AS extension_agent
        """
    )
    await database.close()
    content = path.read_bytes()
    wal_path = path.with_name(path.name + "-wal")
    wal = wal_path.read_bytes() if wal_path.exists() else b""
    for sentinel in sentinels:
        assert sentinel.encode() not in content
        assert sentinel.encode() not in wal
    assert agents[0].prompt == _CONFIG_SENTINEL
    assert servers[0].config["command"] == _CONFIG_SENTINEL
    assert extension.config.custom_agents[0].prompt == _CONFIG_SENTINEL
    assert extension.config.mcp_servers[0].command == _CONFIG_SENTINEL
    assert runtime_config is not None
    assert runtime_config["command_description"] == _CONFIG_SENTINEL
    assert runtime_config["agent_description"] == _CONFIG_SENTINEL
    assert _CONFIG_SENTINEL in runtime_config["agent_metadata"]
    assert _CONFIG_SENTINEL in runtime_config["extension_mcp"]
    assert _CONFIG_SENTINEL in runtime_config["extension_agent"]
    assert binding is not None
    assert binding["config_snapshot_state"] == "verified"
    assert json.loads(binding["session_config_snapshot"])["custom_agent_prompt"] == (
        _CONFIG_SENTINEL
    )
    assert _CONFIG_SENTINEL.encode() in content


@pytest.mark.asyncio
async def test_v52_migrates_pending_render_to_retryable_content_unavailable_surface(
    tmp_path: Path,
) -> None:
    sentinels = tuple(f"MIGRATION-PENDING-{index}-8bd1" for index in range(7))
    path = tmp_path / "legacy-pending-render.sqlite3"
    _create_v51_fixture(path, sentinels)
    connection = sqlite3.connect(path)
    connection.execute("UPDATE render_outbox SET state = 'pending' WHERE id = 'legacy-render'")
    connection.commit()
    connection.close()

    async with Database(path) as database:
        original = await database.fetchone(
            """
            SELECT state, error_code, content_key
            FROM render_outbox WHERE id = 'legacy-render'
            """
        )
        fallback = await database.fetchone(
            """
            SELECT state, render_kind, content_key, error_code, payload
            FROM render_outbox
            WHERE idempotency_key = 'legacy-render:content-unavailable:migration-52'
            """
        )

    assert dict(original) == {
        "state": "content_unavailable",
        "error_code": "content_unavailable",
        "content_key": None,
    }
    assert fallback is not None
    assert dict(fallback) == {
        "state": "pending",
        "render_kind": "content_unavailable",
        "content_key": None,
        "error_code": "content_unavailable",
        "payload": (
            '{"schema":1,"render_kind":"content_unavailable","finalized":1,'
            '"source_outbox_id":"legacy-render","submission_id":null}'
        ),
    }
    assert all(sentinel not in str(fallback["payload"]) for sentinel in sentinels)


@pytest.mark.asyncio
async def test_state_only_cleanup_retries_failed_secure_maintenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinels = tuple(f"CLEANUP-RETRY-SENTINEL-{index}-5a2d" for index in range(7))
    path = tmp_path / "cleanup-retry.sqlite3"
    _create_v51_fixture(path, sentinels)
    database = Database(path)
    attempts = 0
    original = database.secure_maintenance

    async def fail_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("injected maintenance failure")
        await original()

    monkeypatch.setattr(database, "secure_maintenance", fail_once)
    with pytest.raises(
        RuntimeError,
        match="secure erase failed after state-only migration",
    ):
        await database.open()

    database = Database(path)
    await database.open()
    cleanup = await database.fetchone(
        """
        SELECT state, completed_at FROM state_only_cleanup
        WHERE cleanup_key = 'legacy_content_artifacts'
        """
    )
    artifacts = await database.fetchall(
        """
        SELECT managed_path, path_sha256, state
        FROM state_only_cleanup_artifacts
        """
    )
    await database.close()

    assert cleanup is not None and cleanup["state"] == "complete"
    assert cleanup["completed_at"] is not None
    assert [row["state"] for row in artifacts] == ["removed"]
    assert artifacts[0]["managed_path"] is None
    assert artifacts[0]["path_sha256"] is not None
    assert not (
        tmp_path / "sessions" / "legacy-session" / "artifacts" / "legacy-tool-output.txt"
    ).exists()
    persisted = path.read_bytes()
    wal_path = path.with_name(path.name + "-wal")
    wal = wal_path.read_bytes() if wal_path.exists() else b""
    for sentinel in sentinels:
        assert sentinel.encode() not in persisted
        assert sentinel.encode() not in wal


@pytest.mark.asyncio
async def test_state_only_cleanup_discovers_orphans_without_touching_attachments_or_outside(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orphan-cleanup.sqlite3"
    sentinels = tuple(f"ORPHAN-CLEANUP-{index}-91c2" for index in range(7))
    _create_v51_fixture(path, sentinels)
    orphan_image = (
        tmp_path / "sessions" / "orphan-session" / "artifacts" / "local-images" / "1f2e3d4c"
    )
    orphan_spill = tmp_path / "sessions" / "orphan-session" / "artifacts" / "tools" / "spill.txt"
    attachment = tmp_path / "sessions" / "orphan-session" / "attachments" / "keep.txt"
    outside = tmp_path / "outside-managed-artifacts.txt"
    for candidate, content in (
        (orphan_image, "old image"),
        (orphan_spill, "old tool output"),
        (attachment, "attachment must remain"),
        (outside, "outside must remain"),
    ):
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_text(content, encoding="utf-8")
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO tool_spill_artifacts(
            session_id, tool_call_id, local_path, byte_size,
            sha256, finalized, updated_at
        ) VALUES ('legacy-session', 'outside-tool', ?, 1, NULL, 1, 4)
        """,
        (str(outside),),
    )
    connection.commit()
    connection.close()

    async with Database(path) as database:
        cleanup_rows = await database.fetchall(
            """
            SELECT state, COUNT(*) AS count
            FROM state_only_cleanup_artifacts
            GROUP BY state ORDER BY state
            """
        )

    assert not orphan_image.exists()
    assert not orphan_spill.exists()
    assert attachment.read_text(encoding="utf-8") == "attachment must remain"
    assert outside.read_text(encoding="utf-8") == "outside must remain"
    assert {str(row["state"]): int(row["count"]) for row in cleanup_rows} == {
        "ignored_unmanaged": 1,
        "removed": 3,
    }


@pytest.mark.asyncio
async def test_state_only_migration_sql_failure_keeps_legacy_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinels = tuple(f"MIGRATION-ROLLBACK-SENTINEL-{index}-13ac" for index in range(7))
    path = tmp_path / "migration-rollback.sqlite3"
    _create_v51_fixture(path, sentinels)
    artifact = tmp_path / "sessions" / "legacy-session" / "artifacts" / "legacy-tool-output.txt"
    original = database_module._split_sql_statements

    def inject_failure(sql: str) -> list[str]:
        statements = original(sql)
        if "CREATE TABLE state_only_cleanup" in sql:
            statements.append("INSERT INTO missing_state_only_table VALUES (1)")
        return statements

    monkeypatch.setattr(database_module, "_split_sql_statements", inject_failure)
    with pytest.raises(sqlite3.OperationalError, match="missing_state_only_table"):
        await Database(path).open()

    connection = sqlite3.connect(path)
    version = connection.execute("SELECT 1 FROM schema_migrations WHERE version = 52").fetchone()
    spill = connection.execute("SELECT local_path FROM tool_spill_artifacts").fetchone()
    cleanup_table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'state_only_cleanup'
        """
    ).fetchone()
    connection.close()

    assert artifact.exists()
    assert artifact.read_text(encoding="utf-8") == sentinels[3]
    assert version is None
    assert spill == (str(artifact),)
    assert cleanup_table is None
