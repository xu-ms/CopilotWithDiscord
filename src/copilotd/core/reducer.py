from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar

from copilotd.core.event_adapter import EventAdapter, InvalidSdkEvent
from copilotd.core.inbox import ReducerInbox
from copilotd.core.interactions import interaction_target_mode
from copilotd.core.models import AdaptedEvent, InboxEnvelope, RenderIntent
from copilotd.core.task_registry import TaskRegistry
from copilotd.storage.database import Database

FenceValidator = Callable[[int, int], Awaitable[bool]]


class RenderPlanner:
    _STREAM_TYPES: ClassVar[set[str]] = {
        "assistant.message_delta",
        "assistant.streaming_delta",
    }
    _FINAL_TYPES: ClassVar[set[str]] = {
        "assistant.message",
        "session.error",
        "session.warning",
        "session.info",
        "session.shutdown",
    }
    _TASK_TYPES: ClassVar[set[str]] = {
        "copilotd.tasks.snapshot",
        "subagent.started",
        "subagent.completed",
        "subagent.failed",
        "tool.execution_start",
        "tool.execution_progress",
        "tool.execution_complete",
    }
    _TASK_VIEW_TYPES: ClassVar[set[str]] = {"copilotd.taskdeck.view_changed"}
    _INTERACTION_TYPES: ClassVar[set[str]] = {
        "copilotd.interaction.requested",
        "copilotd.interaction.resolved",
        "copilotd.interaction.expired",
    }
    _USAGE_TYPES: ClassVar[set[str]] = {
        "assistant.usage",
        "session.usage_checkpoint",
        "session.usage_info",
    }
    _STATUS_TYPES: ClassVar[set[str]] = {
        "abort",
        "assistant.intent",
        "assistant.reasoning",
        "assistant.turn_retry",
        "copilotd.permissions.reconciled",
        "model.call_failure",
        "session.autopilot_objective_changed",
        "session.compaction_start",
        "session.compaction_complete",
        "session.context_cleared",
        "session.snapshot_rewind",
        "session.task_complete",
        "session.truncation",
        "session.workspace_file_changed",
    }

    def plan(
        self,
        event: AdaptedEvent,
        *,
        payload_override: dict[str, Any] | None = None,
    ) -> list[RenderIntent]:
        if payload_override is not None and payload_override.get("suppress"):
            return []
        agent_scoped_content = event.agent_id is not None and event.raw_type in (
            self._STREAM_TYPES | {"assistant.message"}
        )
        if agent_scoped_content:
            lane = "taskdeck"
            finalized = False
            coalesce_key = "taskdeck"
        elif event.raw_type in self._STREAM_TYPES:
            lane = "assistant_stream"
            finalized = False
            coalesce_key = f"assistant:{event.message_id or event.turn_id or 'main'}"
        elif event.raw_type in self._FINAL_TYPES:
            lane = "assistant_final" if event.raw_type == "assistant.message" else "status"
            finalized = True
            coalesce_key = (
                f"assistant:{event.message_id or event.turn_id or 'main'}"
                if event.raw_type == "assistant.message"
                else None
            )
        elif event.raw_type in self._TASK_TYPES or event.raw_type in self._TASK_VIEW_TYPES:
            lane = "taskdeck"
            finalized = event.raw_type.endswith(("completed", "failed", "complete"))
            coalesce_key = "taskdeck"
        elif event.raw_type in self._USAGE_TYPES:
            lane = "usage"
            finalized = event.raw_type != "assistant.usage"
            coalesce_key = "usage"
        elif event.raw_type in self._INTERACTION_TYPES:
            data = event.raw_payload.get("data", event.raw_payload)
            interaction_id = (
                data.get("interaction_id", event.internal_event_id)
                if isinstance(data, dict)
                else event.internal_event_id
            )
            lane = "interaction"
            finalized = event.raw_type != "copilotd.interaction.requested"
            coalesce_key = f"interaction:{interaction_id}"
        elif event.raw_type in self._STATUS_TYPES:
            lane = "status"
            finalized = event.raw_type not in {
                "assistant.intent",
                "session.compaction_start",
            }
            coalesce_key = (
                "intent"
                if event.raw_type in {"assistant.intent", "assistant.reasoning"}
                else "compaction"
                if event.raw_type.startswith("session.compaction_")
                else "autopilot-objective"
                if event.raw_type == "session.autopilot_objective_changed"
                else "workspace"
                if event.raw_type == "session.workspace_file_changed"
                else "permissions"
                if event.raw_type == "copilotd.permissions.reconciled"
                else None
            )
        else:
            return []

        event_key = event.event_id or event.internal_event_id or str(event.inbox_seq)
        idempotency_key = f"event:{event.sdk_session_id}:{event_key}:{lane}"
        intents = [
            RenderIntent(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key)),
                session_id=event.sdk_session_id,
                logical_seq=event.inbox_seq,
                lane=lane,
                coalesce_key=coalesce_key,
                idempotency_key=idempotency_key,
                payload=payload_override
                or {
                    "type": event.raw_type,
                    "event": event.raw_payload,
                    "finalized": finalized,
                },
                finalized=finalized,
            )
        ]
        artifact = _tool_output_artifact(event)
        if artifact is not None:
            artifact_key = f"{idempotency_key}:artifact"
            intents.append(
                RenderIntent(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, artifact_key)),
                    session_id=event.sdk_session_id,
                    logical_seq=event.inbox_seq,
                    lane="artifact",
                    coalesce_key=None,
                    idempotency_key=artifact_key,
                    payload=artifact,
                    finalized=True,
                )
            )
        return intents


class JournalReducer:
    """Atomically persists event journal rows and their render intents."""

    def __init__(self, database: Database, planner: RenderPlanner | None = None) -> None:
        self._database = database
        self._planner = planner or RenderPlanner()

    async def persist(self, events: list[AdaptedEvent]) -> int:
        inserted = 0
        now = time.time()
        async with self._database.transaction() as connection:
            for event in events:
                cursor = await connection.execute(
                    """
                    INSERT INTO event_journal(
                        sdk_session_id, generation, inbox_seq, source, schema_version,
                        thread_id, sdk_timestamp,
                        sdk_receive_seq, event_id, internal_event_id, ephemeral,
                        persistence_class, raw_type, parent_id, agent_id,
                        message_id, turn_id, interaction_id, task_id, tool_call_id,
                        request_id, correlation_id,
                        reducer_hash, raw_payload, received_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        event.sdk_session_id,
                        event.generation,
                        event.inbox_seq,
                        event.source,
                        event.schema_version,
                        event.thread_id,
                        event.sdk_timestamp,
                        event.sdk_receive_seq,
                        event.event_id,
                        event.internal_event_id,
                        event.ephemeral,
                        event.persistence_class,
                        event.raw_type,
                        event.parent_id,
                        event.agent_id,
                        event.message_id,
                        event.turn_id,
                        event.interaction_id,
                        event.task_id,
                        event.tool_call_id,
                        event.request_id,
                        event.correlation_id,
                        event.reducer_hash,
                        json.dumps(event.raw_payload, ensure_ascii=False, sort_keys=True),
                        event.received_at,
                    ),
                )
                was_inserted = cursor.rowcount == 1
                await cursor.close()
                if not was_inserted:
                    await connection.execute(
                        """
                        UPDATE session_bindings
                        SET last_inbox_seq = MAX(last_inbox_seq, ?),
                            last_sdk_receive_seq = CASE
                                WHEN ? IS NULL THEN last_sdk_receive_seq
                                ELSE MAX(COALESCE(last_sdk_receive_seq, 0), ?)
                            END,
                            last_event_at = MAX(COALESCE(last_event_at, 0), ?),
                            updated_at = ?, row_version = row_version + 1
                        WHERE sdk_session_id = ? AND runtime_generation = ?
                          AND owner_fence_token = ?
                        """,
                        (
                            event.inbox_seq,
                            event.sdk_receive_seq,
                            event.sdk_receive_seq,
                            event.received_at,
                            now,
                            event.sdk_session_id,
                            event.generation,
                            event.fence_token,
                        ),
                    )
                    continue
                inserted += 1
                await self._apply_domain_state(connection, event, now=now)
                render_payload = await self._materialize_render_payload(
                    connection,
                    event,
                    now=now,
                )
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET last_inbox_seq = MAX(last_inbox_seq, ?),
                        last_sdk_receive_seq = CASE
                            WHEN ? IS NULL THEN last_sdk_receive_seq
                            ELSE MAX(COALESCE(last_sdk_receive_seq, 0), ?)
                        END,
                        last_event_at = MAX(COALESCE(last_event_at, 0), ?),
                        updated_at = ?,
                        row_version = row_version + 1
                    WHERE sdk_session_id = ?
                      AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        event.inbox_seq,
                        event.sdk_receive_seq,
                        event.sdk_receive_seq,
                        event.received_at,
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                    ),
                )
                for intent in self._planner.plan(event, payload_override=render_payload):
                    await connection.execute(
                        """
                        INSERT INTO render_outbox(
                            id, session_id, logical_seq, lane, coalesce_key,
                            idempotency_key, payload, state, attempts,
                            next_attempt_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                        ON CONFLICT(idempotency_key) DO NOTHING
                        """,
                        (
                            intent.id,
                            intent.session_id,
                            intent.logical_seq,
                            intent.lane,
                            intent.coalesce_key,
                            intent.idempotency_key,
                            json.dumps(intent.payload, ensure_ascii=False, sort_keys=True),
                            now,
                            now,
                            now,
                        ),
                    )
        return inserted

    async def persist_incident(
        self,
        envelope: InboxEnvelope,
        error: InvalidSdkEvent,
    ) -> None:
        event = envelope.payload
        raw_type = getattr(getattr(event, "type", None), "value", None)
        detail = {
            "reason": error.detail,
            "raw_type": raw_type,
            "raw_event_id": repr(getattr(event, "id", None)),
            "raw_parent_id": repr(getattr(event, "parent_id", None)),
        }
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO runtime_incidents(
                    timestamp, runtime_generation, session_id, kind,
                    last_inbox_seq, last_sdk_receive_seq, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.received_at,
                    envelope.generation,
                    envelope.sdk_session_id,
                    error.kind,
                    envelope.inbox_seq,
                    envelope.sdk_receive_seq,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                ),
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET last_inbox_seq = MAX(last_inbox_seq, ?),
                    last_sdk_receive_seq = CASE
                        WHEN ? IS NULL THEN last_sdk_receive_seq
                        ELSE MAX(COALESCE(last_sdk_receive_seq, 0), ?)
                    END,
                    last_event_at = MAX(COALESCE(last_event_at, 0), ?),
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    envelope.inbox_seq,
                    envelope.sdk_receive_seq,
                    envelope.sdk_receive_seq,
                    envelope.received_at,
                    time.time(),
                    envelope.sdk_session_id,
                    envelope.generation,
                    envelope.fence_token,
                ),
            )

    async def _materialize_render_payload(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> dict[str, Any] | None:
        if event.raw_type in RenderPlanner._INTERACTION_TYPES:
            data = event.raw_payload.get("data", event.raw_payload)
            if not isinstance(data, dict):
                return None
            interaction = dict(data)
            kind = str(interaction.get("kind", "interaction"))
            state = str(interaction.get("state", "pending"))
            prompt = str(
                interaction.get("question")
                or interaction.get("summary")
                or "Copilot is waiting for input."
            )
            if state == "pending":
                content = f"**Copilot needs input** · `{kind}`\n{_bounded_text(prompt, 1600)}"
            else:
                content = (
                    f"**Copilot input {state}** · `{kind}`\n"
                    f"{_bounded_text(str(interaction.get('display_response', '')), 1600)}"
                ).rstrip()
            return {
                "type": "interaction",
                "content": content,
                "finalized": state != "pending",
                "interaction": interaction,
            }
        if event.raw_type in RenderPlanner._USAGE_TYPES:
            data = event.raw_payload.get("data", event.raw_payload)
            values = data if isinstance(data, dict) else {}
            lines = _usage_summary_lines(values)
            return {
                "type": event.raw_type,
                "content": "**Copilot usage**\n" + "\n".join(lines),
                "finalized": event.raw_type != "assistant.usage",
            }
        if (
            event.raw_type in RenderPlanner._STATUS_TYPES
            or event.raw_type in RenderPlanner._FINAL_TYPES - {"assistant.message"}
        ):
            data = event.raw_payload.get("data", event.raw_payload)
            values = data if isinstance(data, dict) else {}
            title, fallback = _status_title(event.raw_type)
            detail = _status_detail(event.raw_type, values, fallback=fallback)
            return {
                "type": event.raw_type,
                "content": f"**{title}**\n{_bounded_text(detail, 1600)}",
                "finalized": event.raw_type
                not in {"assistant.intent", "session.compaction_start"},
            }
        if (
            _is_task_projection_event(event)
            or event.raw_type in RenderPlanner._TASK_VIEW_TYPES
        ):
            rows = await connection.execute(
                """
                SELECT panel_id, card_token, card_key, kind, title, state,
                       progress_summary, revision
                FROM task_card_projections
                WHERE sdk_session_id = ?
                ORDER BY
                  CASE WHEN terminal_at IS NULL THEN 0 ELSE 1 END,
                  first_seen_at, card_key
                """,
                (event.sdk_session_id,),
            )
            cards = [dict(row) for row in await rows.fetchall()]
            await rows.close()
            if not cards:
                if event.raw_type == "copilotd.tasks.snapshot":
                    return {"suppress": True}
                return {
                    "type": "taskdeck",
                    "content": "**TaskDeck**\nNo observed tasks.",
                    "cards": [],
                    "finalized": True,
                    "taskdeck": None,
                }
            panel_id = str(cards[0]["panel_id"])
            deck_revision = sum(int(card["revision"]) for card in cards)
            state_cursor = await connection.execute(
                """
                SELECT selected_card_token, page, expanded
                FROM taskdeck_panel_state WHERE sdk_session_id = ?
                """,
                (event.sdk_session_id,),
            )
            state_row = await state_cursor.fetchone()
            await state_cursor.close()
            page_count = max(1, (len(cards) + 7) // 8)
            page = (
                0
                if state_row is None
                else min(max(int(state_row["page"]), 0), page_count - 1)
            )
            visible = cards[page * 8 : (page + 1) * 8]
            selected = None if state_row is None else state_row["selected_card_token"]
            if selected not in {card["card_token"] for card in cards}:
                selected = visible[0]["card_token"]
            selected_card = next(
                (card for card in cards if card["card_token"] == selected),
                visible[0],
            )
            expanded = bool(state_row is not None and state_row["expanded"])
            terminal_states = {"completed", "failed", "cancelled"}
            finalized = all(str(card["state"]) in terminal_states for card in cards)
            if (
                finalized
                and event.raw_type != "copilotd.taskdeck.view_changed"
                and str(selected_card["state"]) in terminal_states
            ):
                expanded = False
            await connection.execute(
                """
                INSERT INTO taskdeck_panel_state(
                    sdk_session_id, panel_id, selected_card_token,
                    page, expanded, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    panel_id = excluded.panel_id,
                    selected_card_token = excluded.selected_card_token,
                    page = excluded.page,
                    expanded = excluded.expanded,
                    updated_at = excluded.updated_at
                """,
                (
                    event.sdk_session_id,
                    panel_id,
                    selected,
                    page,
                    int(expanded),
                    now,
                ),
            )
            lines = [f"**TaskDeck** — {len(cards)} item(s)"]
            for card in visible:
                state = str(card["state"])
                icon = _task_state_icon(state)
                title = _bounded_text(str(card["title"]), 120)
                lines.append(f"{icon} **{title}** · `{card['kind']}` · `{state}`")
            if expanded and selected_card["progress_summary"]:
                detail = _bounded_text(str(selected_card["progress_summary"]), 900)
                lines.extend(("", f"**{selected_card['title']} details**", detail))
            if page_count > 1:
                lines.append(f"\nPage {page + 1}/{page_count}")
            return {
                "type": "taskdeck",
                "content": "\n".join(lines),
                "cards": cards,
                "finalized": finalized,
                "taskdeck": {
                    "panel_id": panel_id,
                    "revision": deck_revision,
                    "page": page,
                    "page_count": page_count,
                    "selected_card_token": selected,
                    "expanded": expanded,
                    "options": [
                        {
                            "label": _bounded_text(str(card["title"]), 90),
                            "value": str(card["card_token"]),
                            "state": str(card["state"]),
                        }
                        for card in visible
                    ],
                },
            }

        if event.raw_type not in {"assistant.message_delta", "assistant.message"}:
            return None
        data = event.raw_payload.get("data", {})
        if not isinstance(data, dict) or event.message_id is None:
            return None
        if event.raw_type == "assistant.message_delta":
            delta = str(data.get("deltaContent", ""))
            await connection.execute(
                """
                INSERT INTO render_streams(
                    session_id, message_id, content, finalized, updated_at
                ) VALUES (?, ?, ?, 0, ?)
                ON CONFLICT(session_id, message_id) DO UPDATE SET
                    content = CASE
                        WHEN render_streams.finalized = 1 THEN render_streams.content
                        ELSE render_streams.content || excluded.content
                    END,
                    updated_at = excluded.updated_at
                """,
                (event.sdk_session_id, event.message_id, delta, now),
            )
        else:
            content = str(data.get("content", ""))
            await connection.execute(
                """
                INSERT INTO render_streams(
                    session_id, message_id, content, finalized, updated_at
                ) VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(session_id, message_id) DO UPDATE SET
                    content = excluded.content,
                    finalized = 1,
                    updated_at = excluded.updated_at
                """,
                (event.sdk_session_id, event.message_id, content, now),
            )
        cursor = await connection.execute(
            """
            SELECT content, finalized FROM render_streams
            WHERE session_id = ? AND message_id = ?
            """,
            (event.sdk_session_id, event.message_id),
        )
        stream = await cursor.fetchone()
        await cursor.close()
        return {
            "type": event.raw_type,
            "content": stream["content"],
            "message_id": event.message_id,
            "agent_id": event.agent_id,
            "finalized": bool(stream["finalized"]),
        }

    async def _apply_domain_state(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> None:
        data = event.raw_payload.get("data", event.raw_payload)
        if not isinstance(data, dict):
            return
        request_id = data.get("requestId")
        if request_id is not None and event.event_id is not None:
            if event.raw_type.endswith(".requested"):
                await connection.execute(
                    """
                    INSERT INTO protocol_requests(
                        sdk_session_id, generation, request_id, requested_type,
                        requested_event_id, state
                    ) VALUES (?, ?, ?, ?, ?, 'requested')
                    ON CONFLICT(sdk_session_id, generation, request_id) DO NOTHING
                    """,
                    (
                        event.sdk_session_id,
                        event.generation,
                        str(request_id),
                        event.raw_type,
                        event.event_id,
                    ),
                )
            elif event.raw_type.endswith(".completed"):
                await connection.execute(
                    """
                    UPDATE protocol_requests
                    SET completed_event_id = ?, state = 'completed'
                    WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
                    """,
                    (
                        event.event_id,
                        event.sdk_session_id,
                        event.generation,
                        str(request_id),
                    ),
                )
        if event.raw_type == "copilotd.operation.pending":
            await connection.execute(
                """
                INSERT INTO session_operations(
                    operation_id, sdk_session_id, runtime_generation,
                    owner_fence_token, kind, idempotency_key, input_hash,
                    state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(sdk_session_id, idempotency_key) DO NOTHING
                """,
                (
                    str(data["operation_id"]),
                    event.sdk_session_id,
                    int(data["runtime_generation"]),
                    int(data["owner_fence_token"]),
                    str(data["kind"]),
                    str(data["idempotency_key"]),
                    str(data["input_hash"]),
                    float(data["created_at"]),
                ),
            )
            cursor = await connection.execute(
                """
                SELECT operation_id, kind, input_hash FROM session_operations
                WHERE sdk_session_id = ? AND idempotency_key = ?
                """,
                (event.sdk_session_id, str(data["idempotency_key"])),
            )
            operation = await cursor.fetchone()
            await cursor.close()
            if (
                operation is None
                or operation["operation_id"] != str(data["operation_id"])
                or operation["kind"] != str(data["kind"])
                or operation["input_hash"] != str(data["input_hash"])
            ):
                raise ValueError("idempotency key was reused with different operation input")
        elif event.raw_type == "copilotd.operation.transition":
            to_state = str(data["to_state"])
            transitioned_at = float(data["transitioned_at"])
            cursor = await connection.execute(
                """
                UPDATE session_operations
                SET state = ?, result_ref = ?, error_code = ?,
                    started_at = CASE WHEN ? = 'started' THEN ? ELSE started_at END,
                    settled_at = CASE
                        WHEN ? IN ('confirmed', 'rejected', 'unknown')
                        THEN ? ELSE settled_at
                    END
                WHERE operation_id = ? AND sdk_session_id = ? AND state = ?
                """,
                (
                    to_state,
                    data.get("result_ref"),
                    data.get("error_code"),
                    to_state,
                    transitioned_at,
                    to_state,
                    transitioned_at,
                    str(data["operation_id"]),
                    event.sdk_session_id,
                    str(data["from_state"]),
                ),
            )
            changed = cursor.rowcount
            await cursor.close()
            if changed != 1:
                raise RuntimeError(
                    f"operation state changed concurrently: {data['operation_id']}"
                )
        elif event.raw_type == "copilotd.operation.unsettled_unknown":
            await connection.execute(
                """
                UPDATE session_operations
                SET state = 'unknown', error_code = ?, settled_at = ?
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND state IN ('pending', 'started')
                """,
                (
                    str(data["error_code"]),
                    float(data["settled_at"]),
                    event.sdk_session_id,
                    int(data["runtime_generation"]),
                    int(data["owner_fence_token"]),
                ),
            )
        elif event.raw_type == "copilotd.snapshot.requested":
            topic = str(data["topic"])
            await connection.execute(
                """
                INSERT INTO reconciliation_state(
                    sdk_session_id, topic, requested_epoch, applied_epoch,
                    status, runtime_generation, owner_fence_token
                ) VALUES (?, ?, 1, 0, 'requested', ?, ?)
                ON CONFLICT(sdk_session_id, topic) DO UPDATE SET
                    requested_epoch = reconciliation_state.requested_epoch + 1,
                    status = 'requested',
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    uncertainty_reason = NULL
                """,
                (
                    event.sdk_session_id,
                    topic,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.snapshot.observed":
            await self._apply_snapshot_observed(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type == "copilotd.snapshot.failed":
            await connection.execute(
                """
                UPDATE reconciliation_state
                SET status = 'failed', uncertainty_reason = ?,
                    query_start_sdk_receive_seq = ?,
                    query_end_sdk_receive_seq = ?, observed_at = ?
                WHERE sdk_session_id = ? AND topic = ?
                  AND requested_epoch <= ?
                """,
                (
                    str(data.get("error_type", "snapshot_failed")),
                    int(data.get("query_start_sdk_receive_seq", 0)),
                    int(data.get("query_end_sdk_receive_seq", 0)),
                    float(data.get("observed_at", now)),
                    event.sdk_session_id,
                    str(data["topic"]),
                    int(data["epoch"]),
                ),
            )
        elif event.raw_type == "copilotd.event_cursor.advanced":
            await connection.execute(
                """
                UPDATE session_bindings
                SET event_cursor = ?, cursor_status = ?,
                    event_cursor_epoch = ?,
                    event_predecessor_id = COALESCE(?, event_predecessor_id),
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(data["cursor"]),
                    str(data["cursor_status"]),
                    int(data["cursor_epoch"]),
                    data.get("last_event_id"),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.recovery.incident":
            await connection.execute(
                """
                INSERT INTO runtime_incidents(
                    timestamp, runtime_generation, session_id, kind,
                    last_inbox_seq, last_sdk_receive_seq, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    float(data.get("observed_at", event.received_at)),
                    event.generation,
                    event.sdk_session_id,
                    str(data["kind"]),
                    event.inbox_seq,
                    event.sdk_receive_seq,
                    json.dumps(
                        data.get("detail", {}),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                ),
            )
        elif event.raw_type == "copilotd.tasks.snapshot":
            await self._apply_task_snapshot(connection, event, data=data, now=now)
        elif _is_task_projection_event(event):
            await self._update_task_projection(connection, event, data=data, now=now)
        if event.raw_type == "copilotd.interaction.requested":
            interaction_id = str(data["interaction_id"])
            await connection.execute(
                """
                INSERT INTO pending_interactions(
                    interaction_id, sdk_session_id, runtime_generation,
                    owner_fence_token, thread_id, kind, response_plane,
                    expires_at, state, payload, response, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'direct_handler', ?, 'pending', ?, NULL, ?, ?)
                ON CONFLICT(interaction_id) DO NOTHING
                """,
                (
                    interaction_id,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    str(data["thread_id"]),
                    str(data["kind"]),
                    float(data["expires_at"]),
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                ) VALUES (?, ?, 'interaction', ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                    state = 'active',
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    refreshed_at = excluded.refreshed_at,
                    released_at = NULL
                """,
                (
                    event.sdk_session_id,
                    f"interaction:{interaction_id}",
                    interaction_id,
                    event.generation,
                    event.fence_token,
                    now,
                    now,
                ),
            )
        elif event.raw_type in {
            "copilotd.interaction.resolved",
            "copilotd.interaction.expired",
        }:
            interaction_id = str(data["interaction_id"])
            state = str(data["state"])
            target_mode = data.get("target_mode") or interaction_target_mode(
                data.get("response")
            )
            await connection.execute(
                """
                UPDATE pending_interactions
                SET state = ?, response = ?, target_mode = ?, updated_at = ?
                WHERE interaction_id = ? AND sdk_session_id = ?
                  AND runtime_generation = ? AND owner_fence_token = ?
                  AND state = 'pending'
                """,
                (
                    state,
                    json.dumps(data.get("response"), ensure_ascii=False, sort_keys=True),
                    target_mode,
                    now,
                    interaction_id,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND lease_id = ?
                  AND runtime_generation = ? AND owner_fence_token = ?
                  AND state = 'active'
                """,
                (
                    now,
                    now,
                    event.sdk_session_id,
                    f"interaction:{interaction_id}",
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.submission.queued":
            submission_id = str(data["submission_id"])
            await connection.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, attachment_manifest_id, prompt_hash,
                    requested_mode, requested_delivery, correlation_id, attachment_count,
                    state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued', ?)
                ON CONFLICT(submission_id) DO NOTHING
                """,
                (
                    submission_id,
                    event.sdk_session_id,
                    str(data.get("origin", "app_message")),
                    data.get("attachment_manifest_id"),
                    data.get("prompt_hash"),
                    data.get("requested_mode"),
                    data.get("requested_delivery", "enqueue"),
                    data.get("correlation_id"),
                    int(data.get("attachment_count", 0)),
                    float(data.get("created_at", event.received_at)),
                ),
            )

            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    event.sdk_session_id,
                    f"submission:{submission_id}",
                    submission_id,
                    event.generation,
                    event.fence_token,
                    event.received_at,
                    event.received_at,
                ),
            )
            position_cursor = await connection.execute(
                """
                SELECT COALESCE(MAX(position), 0) + 1
                FROM message_queue WHERE thread_id = ?
                """,
                (str(data["thread_id"]),),
            )
            position_row = await position_cursor.fetchone()
            await position_cursor.close()
            await connection.execute(
                """
                INSERT INTO message_queue(
                    id, thread_id, discord_message_id, prompt,
                    attachment_manifest_id, requested_mode_snapshot,
                    requested_model_config_snapshot, requested_agent_snapshot,
                    requested_session_config_version, position, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued', ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    submission_id,
                    str(data["thread_id"]),
                    data.get("discord_message_id"),
                    str(data.get("prompt", "")),
                    data.get("attachment_manifest_id"),
                    str(data.get("requested_mode", "interactive")),
                    json.dumps(data.get("requested_model_config", {}), sort_keys=True),
                    data.get("requested_agent", "default"),
                    int(data.get("requested_session_config_version", 1)),
                    int(position_row[0]),
                    float(data.get("created_at", event.received_at)),
                    float(data.get("created_at", event.received_at)),
                ),
            )
        elif event.raw_type == "copilotd.submission.submitting":
            cursor = await connection.execute(
                """
                UPDATE submissions
                SET state = 'submitting', source_operation_id = ?,
                    send_started_at = ?
                WHERE submission_id = ? AND sdk_session_id = ?
                  AND state = 'local_queued'
                """,
                (
                    str(data["operation_id"]),
                    event.received_at,
                    str(data["submission_id"]),
                    event.sdk_session_id,
                ),
            )
            submission_claimed = cursor.rowcount
            await cursor.close()
            if submission_claimed == 0:
                return
            cursor = await connection.execute(
                """
                UPDATE message_queue SET state = 'submitting', updated_at = ?
                WHERE id = ? AND state = 'local_queued'
                """,
                (now, str(data["submission_id"])),
            )
            queue_claimed = cursor.rowcount
            await cursor.close()
            if queue_claimed != 1:
                raise RuntimeError(
                    f"submission queue state diverged while claiming {data['submission_id']}"
                )
        elif event.raw_type == "copilotd.submission.cancel_queued":
            submission_ids = [str(item) for item in data.get("submission_ids", [])]
            cancellable = [str(item) for item in data.get("cancellable_states", [])]
            if not submission_ids or not cancellable:
                return
            ids = ", ".join("?" for _ in submission_ids)
            states = ", ".join("?" for _ in cancellable)
            await connection.execute(
                f"""
                UPDATE message_queue SET state = 'cancelled', updated_at = ?
                WHERE id IN ({ids}) AND state IN ({states})
                """,
                (
                    float(data.get("cancelled_at", now)),
                    *submission_ids,
                    *cancellable,
                ),
            )
            await connection.execute(
                f"""
                UPDATE submissions SET state = 'cancelled'
                WHERE submission_id IN ({ids}) AND state = 'local_queued'
                """,
                tuple(submission_ids),
            )
            await connection.execute(
                f"""
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND kind = 'submission'
                  AND source_id IN ({ids}) AND state = 'active'
                """,
                (now, now, event.sdk_session_id, *submission_ids),
            )
        elif event.raw_type == "copilotd.submission.active_unknown":
            observed_at = float(data.get("observed_at", now))
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'outcome_unknown'
                WHERE sdk_session_id = ?
                  AND state IN (
                    'submitting', 'submitted', 'submitted_unknown',
                    'observed_active', 'loop_idle', 'continuation_expected'
                  )
                """,
                (event.sdk_session_id,),
            )
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'orphaned', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND state = 'active'
                  AND runtime_generation = ? AND owner_fence_token = ?
                """,
                (
                    observed_at,
                    observed_at,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.submission.accepted":
            submission_id = str(data["submission_id"])
            message_id = str(data["message_id"])
            cursor = await connection.execute(
                """
                SELECT state, accepted_message_id, observed_user_event_id
                FROM submissions
                WHERE submission_id = ? AND sdk_session_id = ?
                """,
                (submission_id, event.sdk_session_id),
            )
            accepted = await cursor.fetchone()
            await cursor.close()
            if accepted is None:
                return
            if accepted["state"] == "rejected":
                await self._record_runtime_incident_once(
                    connection,
                    event,
                    kind="submission_acceptance_after_rejection",
                    detail={
                        "submission_id": submission_id,
                        "message_id": message_id,
                    },
                )
                return
            if (
                accepted["accepted_message_id"] is None
                and accepted["observed_user_event_id"] is not None
                and str(accepted["observed_user_event_id"]) != message_id
                and await self._capability_evidenced(
                    connection,
                    "accepted_user_event_id_mapping",
                )
            ):
                await self._split_observed_submission(
                    connection,
                    event,
                    submission_id=submission_id,
                    app_state="submitted",
                    runtime_correlation_basis=(
                        "acceptance_id_mismatch_runtime_observed"
                    ),
                    incident_kind="provisional_user_event_acceptance_mismatch",
                    now=now,
                )
            if (
                accepted["accepted_message_id"] is not None
                and str(accepted["accepted_message_id"]) != message_id
            ):
                await self._record_runtime_incident_once(
                    connection,
                    event,
                    kind="submission_acceptance_conflict",
                    detail={
                        "submission_id": submission_id,
                        "retained_message_id": str(accepted["accepted_message_id"]),
                        "conflicting_message_id": message_id,
                    },
                )
                return
            await connection.execute(
                """
                UPDATE submissions
                SET state = CASE
                        WHEN state IN (
                            'local_queued', 'submitting', 'submitted_unknown'
                        )
                        THEN 'submitted'
                        ELSE state
                    END,
                    accepted_message_id = COALESCE(accepted_message_id, ?),
                    accepted_at = COALESCE(accepted_at, ?),
                    terminal_at = CASE
                        WHEN state = 'submitted_unknown' THEN NULL
                        ELSE terminal_at
                    END
                WHERE submission_id = ? AND state IN (
                    'local_queued', 'submitting', 'submitted',
                    'submitted_unknown', 'observed_active', 'loop_idle',
                    'continuation_expected', 'semantic_complete',
                    'semantic_blocked', 'observed_aborted'
                )
                """,
                (
                    message_id,
                    event.received_at,
                    submission_id,
                ),
            )
            await connection.execute(
                """
                UPDATE message_queue SET state = 'submitted', updated_at = ?
                WHERE id = ? AND state IN (
                    'local_queued', 'submitting', 'submitted_unknown'
                )
                """,
                (now, submission_id),
            )
            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                )
                SELECT sdk_session_id, 'submission:' || submission_id,
                       'submission', submission_id, ?, ?, 'active', ?, ?
                FROM submissions
                WHERE submission_id = ? AND state = 'submitted'
                ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                    state = 'active',
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    refreshed_at = excluded.refreshed_at,
                    released_at = NULL
                """,
                (
                    event.generation,
                    event.fence_token,
                    now,
                    now,
                    submission_id,
                ),
            )
        elif event.raw_type in {
            "copilotd.submission.rejected",
            "copilotd.submission.acceptance_unknown",
        }:
            state = (
                "rejected"
                if event.raw_type == "copilotd.submission.rejected"
                else "submitted_unknown"
            )
            submission_id = str(data["submission_id"])
            if event.raw_type == "copilotd.submission.acceptance_unknown":
                await connection.execute(
                    """
                    UPDATE submissions
                    SET state = CASE
                            WHEN state = 'rejected'
                            THEN state
                            WHEN observed_user_event_id IS NOT NULL
                              OR accepted_message_id IS NOT NULL
                            THEN state
                            ELSE 'submitted_unknown'
                        END,
                        terminal_at = CASE
                            WHEN state = 'rejected'
                            THEN terminal_at
                            WHEN observed_user_event_id IS NOT NULL
                              OR accepted_message_id IS NOT NULL
                            THEN terminal_at
                            ELSE ?
                        END
                    WHERE submission_id = ?
                    """,
                    (event.received_at, submission_id),
                )
                await connection.execute(
                    """
                    UPDATE message_queue
                    SET state = CASE
                            WHEN EXISTS (
                                SELECT 1 FROM submissions
                                WHERE submission_id = ?
                                  AND (
                                      observed_user_event_id IS NOT NULL
                                      OR accepted_message_id IS NOT NULL
                                  )
                            ) THEN 'submitted'
                            ELSE 'submitted_unknown'
                        END,
                        updated_at = ?
                    WHERE id = ? AND state IN ('local_queued', 'submitting')
                    """,
                    (submission_id, now, submission_id),
                )
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'released', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id = ? AND kind = 'submission'
                      AND source_id = ? AND state = 'active'
                      AND NOT EXISTS (
                          SELECT 1 FROM submissions
                          WHERE submission_id = ?
                            AND (
                                observed_user_event_id IS NOT NULL
                                OR accepted_message_id IS NOT NULL
                            )
                      )
                    """,
                    (
                        now,
                        now,
                        event.sdk_session_id,
                        submission_id,
                        submission_id,
                    ),
                )
            else:
                cursor = await connection.execute(
                    """
                    SELECT state, accepted_message_id, observed_user_event_id
                    FROM submissions
                    WHERE submission_id = ? AND sdk_session_id = ?
                    """,
                    (submission_id, event.sdk_session_id),
                )
                rejected = await cursor.fetchone()
                await cursor.close()
                if rejected is None or rejected["state"] == "rejected":
                    return
                if rejected["accepted_message_id"] is not None:
                    await self._record_runtime_incident_once(
                        connection,
                        event,
                        kind="submission_rejection_after_acceptance",
                        detail={
                            "submission_id": submission_id,
                            "accepted_message_id": str(
                                rejected["accepted_message_id"]
                            ),
                        },
                    )
                    await connection.execute(
                        """
                        UPDATE message_queue
                        SET state = 'submitted', updated_at = ?
                        WHERE id = ? AND state IN (
                            'local_queued', 'submitting', 'submitted_unknown'
                        )
                        """,
                        (now, submission_id),
                    )
                    return
                if rejected["observed_user_event_id"] is not None:
                    await self._split_observed_submission(
                        connection,
                        event,
                        submission_id=submission_id,
                        app_state="rejected",
                        runtime_correlation_basis="rejected_send_runtime_observed",
                        incident_kind="rejected_send_observed_as_runtime_message",
                        now=now,
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE submissions SET state = ?, terminal_at = ?
                        WHERE submission_id = ?
                        """,
                        (state, event.received_at, submission_id),
                    )
                await connection.execute(
                    """
                    UPDATE message_queue SET state = ?, updated_at = ?
                    WHERE id = ? AND state IN (
                        'local_queued', 'submitting', 'submitted',
                        'submitted_unknown'
                    )
                    """,
                    (state, now, submission_id),
                )
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'released', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id = ? AND kind = 'submission'
                      AND source_id = ? AND state = 'active'
                    """,
                    (now, now, event.sdk_session_id, submission_id),
                )
        elif event.raw_type == "copilotd.queue.blocked":
            await connection.execute(
                """
                UPDATE message_queue SET state = ?, updated_at = ?
                WHERE id = ? AND state = 'local_queued'
                """,
                (str(data["state"]), now, str(data["submission_id"])),
            )
        elif event.raw_type == "copilotd.taskdeck.view_changed":
            await connection.execute(
                """
                INSERT INTO taskdeck_panel_state(
                    sdk_session_id, panel_id, selected_card_token,
                    page, expanded, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    selected_card_token = excluded.selected_card_token,
                    page = excluded.page,
                    expanded = excluded.expanded,
                    updated_at = excluded.updated_at
                """,
                (
                    event.sdk_session_id,
                    str(data["panel_id"]),
                    data.get("selected_card_token"),
                    int(data.get("page", 0)),
                    int(bool(data.get("expanded"))),
                    now,
                ),
            )
        elif event.raw_type == "copilotd.readiness.observed":
            observed_at = float(data.get("observed_at", now))
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_processing = ?, runtime_has_active_work = ?,
                    runtime_abortable = ?, activity_observed_at = ?,
                    native_queue_count = ?, native_steering_count = ?,
                    queue_observed_at = ?, updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ?
                """,
                (
                    int(bool(data.get("processing"))),
                    int(bool(data.get("has_active_work"))),
                    int(bool(data.get("abortable"))),
                    observed_at,
                    int(data.get("native_queue_count", 0)),
                    int(data.get("native_steering_count", 0)),
                    observed_at,
                    now,
                    event.sdk_session_id,
                ),
            )
            for topic in ("activity", "queue"):
                await connection.execute(
                    """
                    INSERT INTO reconciliation_state(
                        sdk_session_id, topic, requested_epoch, applied_epoch,
                        status, runtime_generation, owner_fence_token, observed_at
                    ) VALUES (?, ?, ?, ?, 'idle', ?, ?, ?)
                    ON CONFLICT(sdk_session_id, topic) DO UPDATE SET
                        requested_epoch = excluded.requested_epoch,
                        applied_epoch = excluded.applied_epoch,
                        status = 'idle',
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        observed_at = excluded.observed_at
                    """,
                    (
                        event.sdk_session_id,
                        topic,
                        int(data["epoch"]),
                        int(data["epoch"]),
                        event.generation,
                        event.fence_token,
                        observed_at,
                    ),
                )
        elif event.raw_type == "copilotd.readiness.failed":
            for topic in ("activity", "queue"):
                await connection.execute(
                    """
                    INSERT INTO reconciliation_state(
                        sdk_session_id, topic, requested_epoch, applied_epoch,
                        status, runtime_generation, owner_fence_token, observed_at
                    ) VALUES (?, ?, ?, 0, 'failed', ?, ?, ?)
                    ON CONFLICT(sdk_session_id, topic) DO UPDATE SET
                        requested_epoch = excluded.requested_epoch,
                        status = 'failed',
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        observed_at = excluded.observed_at
                    """,
                    (
                        event.sdk_session_id,
                        topic,
                        int(data["epoch"]),
                        event.generation,
                        event.fence_token,
                        now,
                    ),
                )
        elif event.raw_type == "user.message":
            await self._apply_user_message(connection, event, data=data, now=now)
        elif event.raw_type in {
            "assistant.turn_start",
            "assistant.turn_retry",
            "assistant.turn_end",
        }:
            await self._apply_model_turn(connection, event, now=now)
        elif event.raw_type == "session.task_complete":
            await self._apply_task_complete(connection, event, data=data, now=now)
        elif event.raw_type == "session.autopilot_objective_changed":
            await self._apply_autopilot_objective(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type == "abort":
            await self._apply_abort(connection, event, now=now)
        elif event.raw_type == "session.idle":
            await self._apply_session_idle(connection, event, data=data, now=now)
        elif event.raw_type == "session.permissions_changed":
            await connection.execute(
                """
                UPDATE session_bindings
                SET permission_posture = 'unverified',
                    permission_verified_at = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.mode.pending":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_mode = ?, pending_mode_transition_id = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND pending_mode IS NULL
                """,
                (
                    str(data["mode"]),
                    str(data["transition_id"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type in {"copilotd.mode.confirmed", "copilotd.mode.observed"}:
            mode = str(data["mode"])
            transition_id = data.get("transition_id")
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_mode = CASE
                        WHEN pending_mode = ?
                          AND (? IS NULL OR pending_mode_transition_id = ?)
                        THEN ?
                        ELSE desired_mode
                    END,
                    runtime_mode = ?,
                    pending_mode = CASE
                        WHEN pending_mode = ?
                          AND (? IS NULL OR pending_mode_transition_id = ?)
                        THEN NULL
                        ELSE pending_mode
                    END,
                    pending_mode_transition_id = CASE
                        WHEN pending_mode = ?
                          AND (? IS NULL OR pending_mode_transition_id = ?)
                        THEN NULL
                        ELSE pending_mode_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    mode,
                    transition_id,
                    transition_id,
                    mode,
                    mode,
                    mode,
                    transition_id,
                    transition_id,
                    mode,
                    transition_id,
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.mode.unknown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_mode = 'unknown', updated_at = ?,
                    row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.mode.rejected":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_mode = CASE
                        WHEN pending_mode_transition_id = ? THEN NULL ELSE pending_mode
                    END,
                    pending_mode_transition_id = CASE
                        WHEN pending_mode_transition_id = ? THEN NULL
                        ELSE pending_mode_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(data["transition_id"]),
                    str(data["transition_id"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.pending":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_model_config = ?, pending_model_transition_id = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND pending_model_config IS NULL
                """,
                (
                    json.dumps(data["config"], sort_keys=True),
                    str(data["transition_id"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.observed":
            observed = data["observed"]
            cursor = await connection.execute(
                """
                SELECT desired_model_config, pending_model_config,
                       pending_model_transition_id
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
            current = await cursor.fetchone()
            await cursor.close()
            pending = (
                None
                if current is None or current["pending_model_config"] is None
                else json.loads(current["pending_model_config"])
            )
            pending_confirmed = pending is not None and _model_config_matches(
                pending,
                observed,
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_model_config = CASE WHEN ? THEN ? ELSE desired_model_config END,
                    runtime_model_config = ?,
                    pending_model_config = CASE
                        WHEN ? THEN NULL ELSE pending_model_config
                    END,
                    pending_model_transition_id = CASE
                        WHEN ? THEN NULL ELSE pending_model_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    pending_confirmed,
                    json.dumps(pending, sort_keys=True),
                    json.dumps(observed, sort_keys=True),
                    pending_confirmed,
                    pending_confirmed,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.confirmed":
            transition_id = str(data["transition_id"])
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_model_config = CASE
                        WHEN pending_model_transition_id = ? THEN ? ELSE desired_model_config
                    END,
                    runtime_model_config = ?,
                    pending_model_config = CASE
                        WHEN pending_model_transition_id = ? THEN NULL
                        ELSE pending_model_config
                    END,
                    pending_model_transition_id = CASE
                        WHEN pending_model_transition_id = ? THEN NULL
                        ELSE pending_model_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    transition_id,
                    json.dumps(data["config"], sort_keys=True),
                    json.dumps(data["observed"], sort_keys=True),
                    transition_id,
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.unknown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_model_config = NULL, updated_at = ?,
                    row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.rejected":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_model_config = CASE
                        WHEN pending_model_transition_id = ? THEN NULL
                        ELSE pending_model_config
                    END,
                    pending_model_transition_id = CASE
                        WHEN pending_model_transition_id = ? THEN NULL
                        ELSE pending_model_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(data["transition_id"]),
                    str(data["transition_id"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.agent.observed":
            agent = str(data.get("agent") or "default")
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_agent = ?,
                    desired_agent = CASE
                        WHEN pending_agent = ? THEN ? ELSE desired_agent
                    END,
                    pending_agent = CASE
                        WHEN pending_agent = ? THEN NULL ELSE pending_agent
                    END,
                    pending_agent_transition_id = CASE
                        WHEN pending_agent = ? THEN NULL
                        ELSE pending_agent_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    agent,
                    agent,
                    agent,
                    agent,
                    agent,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.config.observed":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_session_config_version = ?,
                    pending_session_config_version = CASE
                        WHEN pending_session_config_version = ? THEN NULL
                        ELSE pending_session_config_version
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    int(data["version"]),
                    int(data["version"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "session.mode_changed":
            mode = data.get("mode") or data.get("newMode")
            if mode in {"interactive", "plan", "autopilot"}:
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET desired_mode = CASE
                            WHEN pending_mode = ? THEN ?
                            WHEN EXISTS (
                                SELECT 1 FROM pending_interactions AS interaction
                                WHERE interaction.sdk_session_id =
                                      session_bindings.sdk_session_id
                                  AND interaction.runtime_generation =
                                      session_bindings.runtime_generation
                                  AND interaction.owner_fence_token =
                                      session_bindings.owner_fence_token
                                  AND interaction.kind = 'exit_plan_mode'
                                  AND interaction.state = 'resolved'
                                  AND interaction.target_mode = ?
                                  AND interaction.consumed_at IS NULL
                                  AND interaction.updated_at >= ?
                            ) THEN ?
                            ELSE desired_mode
                        END,
                        runtime_mode = ?,
                        pending_mode = CASE WHEN pending_mode = ? THEN NULL ELSE pending_mode END,
                        pending_mode_transition_id = CASE
                            WHEN pending_mode = ? THEN NULL ELSE pending_mode_transition_id
                        END,
                        updated_at = ?, row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        mode,
                        mode,
                        mode,
                        now - 60,
                        mode,
                        mode,
                        mode,
                        mode,
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                    ),
                )
                await connection.execute(
                   """
                   UPDATE pending_interactions
                   SET consumed_at = ?
                   WHERE interaction_id = (
                       SELECT interaction_id FROM pending_interactions
                       WHERE sdk_session_id = ? AND runtime_generation = ?
                         AND owner_fence_token = ?
                         AND kind = 'exit_plan_mode' AND state = 'resolved'
                         AND target_mode = ? AND consumed_at IS NULL
                         AND updated_at >= ?
                       ORDER BY updated_at DESC
                       LIMIT 1
                   )
                   """,
                   (
                       now,
                       event.sdk_session_id,
                       event.generation,
                       event.fence_token,
                       mode,
                       now - 60,
                   ),
                )
        elif event.raw_type == "session.shutdown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET attachment_state = 'terminal',
                    permission_posture = 'unknown',
                    permission_verified_at = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )

    async def _apply_snapshot_observed(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        topic = str(data["topic"])
        epoch = int(data["epoch"])
        snapshot_id = str(data["snapshot_id"])
        query_start = int(data.get("query_start_sdk_receive_seq", 0))
        query_end = int(data.get("query_end_sdk_receive_seq", query_start))
        observed_at = float(data.get("observed_at", now))
        payload = data.get("payload")
        values = payload if isinstance(payload, dict) else {}

        cursor = await connection.execute(
            """
            SELECT requested_epoch, applied_epoch
            FROM reconciliation_state
            WHERE sdk_session_id = ? AND topic = ?
            """,
            (event.sdk_session_id, topic),
        )
        reconciliation = await cursor.fetchone()
        await cursor.close()
        if reconciliation is None:
            await self._record_runtime_incident(
                connection,
                event,
                kind="snapshot_without_request",
                detail={"topic": topic, "epoch": epoch, "snapshot_id": snapshot_id},
            )
            return
        cursor = await connection.execute(
            """
            SELECT COALESCE(last_sdk_receive_seq, 0)
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (event.sdk_session_id,),
        )
        binding = await cursor.fetchone()
        await cursor.close()
        reducer_watermark = 0 if binding is None else int(binding[0])
        caught_up = reducer_watermark >= query_end
        fresh = epoch == int(reconciliation["requested_epoch"])
        positive = _snapshot_has_positive_evidence(topic, values)
        negative_applied = fresh and caught_up and not positive
        may_merge = positive or negative_applied

        if topic == "activity" and may_merge:
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_processing = ?, runtime_has_active_work = ?,
                    runtime_abortable = ?, activity_observed_at = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    int(bool(values.get("processing"))),
                    int(bool(values.get("has_active_work"))),
                    int(bool(values.get("abortable"))),
                    observed_at,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif topic == "queue" and may_merge:
            items = [item for item in values.get("items", []) if isinstance(item, dict)]
            steering = [
                item for item in values.get("steering_messages", []) if isinstance(item, str)
            ]
            await connection.execute(
                """
                UPDATE session_bindings
                SET native_queue_count = ?, native_steering_count = ?,
                    queue_observed_at = ?, updated_at = ?,
                    row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    len(items),
                    len(steering),
                    observed_at,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
            seen_ids: list[str] = []
            for index, item in enumerate(items):
                item_id = str(item.get("id") or item.get("itemId") or "").strip()
                if not item_id:
                    item_id = f"opaque:{snapshot_id}:{index}"
                seen_ids.append(item_id)
                await connection.execute(
                    """
                    INSERT INTO native_queue_items(
                        sdk_session_id, item_id, agent_mode, display_text,
                        state, last_snapshot_id, last_seen_epoch, updated_at
                    ) VALUES (?, ?, ?, ?, 'present', ?, ?, ?)
                    ON CONFLICT(sdk_session_id, item_id) DO UPDATE SET
                        agent_mode = excluded.agent_mode,
                        display_text = excluded.display_text,
                        state = 'present',
                        last_snapshot_id = excluded.last_snapshot_id,
                        last_seen_epoch = excluded.last_seen_epoch,
                        updated_at = excluded.updated_at
                    """,
                    (
                        event.sdk_session_id,
                        item_id,
                        item.get("agentMode"),
                        item.get("displayText"),
                        snapshot_id,
                        epoch,
                        observed_at,
                    ),
                )
            if fresh and caught_up:
                if seen_ids:
                    placeholders = ", ".join("?" for _ in seen_ids)
                    await connection.execute(
                        f"""
                        UPDATE native_queue_items
                        SET state = 'absent', updated_at = ?
                        WHERE sdk_session_id = ? AND state = 'present'
                          AND item_id NOT IN ({placeholders})
                        """,
                        (observed_at, event.sdk_session_id, *seen_ids),
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE native_queue_items
                        SET state = 'absent', updated_at = ?
                        WHERE sdk_session_id = ? AND state = 'present'
                        """,
                        (observed_at, event.sdk_session_id),
                    )
        elif topic == "tasks" and (positive or (fresh and caught_up)):
            await self._apply_task_snapshot(
                connection,
                event,
                data={
                    "tasks": values.get("tasks", []),
                    "observed_at": observed_at,
                },
                now=now,
                allow_negative=fresh and caught_up,
            )
        elif topic == "remote" and may_merge:
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_remote_mode = ?, remote_url = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(values.get("mode", "unknown")),
                    values.get("url"),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif topic == "schedules" and (positive or (fresh and caught_up)):
            schedules = [
                item for item in values.get("schedules", []) if isinstance(item, dict)
            ]
            seen_schedule_ids: list[str] = []
            for item in schedules:
                schedule_id = str(item.get("id") or item.get("scheduleId") or "").strip()
                if not schedule_id:
                    continue
                seen_schedule_ids.append(schedule_id)
                state = str(item.get("state") or item.get("status") or "active").lower()
                await connection.execute(
                    """
                    INSERT INTO runtime_schedules(
                        sdk_session_id, runtime_schedule_id, builtin_name,
                        invocation_input, recurrence, next_run_at, state,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sdk_session_id, runtime_schedule_id) DO UPDATE SET
                        recurrence = excluded.recurrence,
                        next_run_at = excluded.next_run_at,
                        state = CASE
                            WHEN runtime_schedules.state IN (
                                'cancelled', 'triggered', 'failed'
                            ) THEN runtime_schedules.state
                            ELSE excluded.state
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        event.sdk_session_id,
                        schedule_id,
                        str(item.get("builtinName") or item.get("kind") or "unknown"),
                        str(item.get("input") or ""),
                        item.get("recurrence"),
                        item.get("nextRunAt"),
                        state,
                        observed_at,
                    ),
                )
            if fresh and caught_up:
                if seen_schedule_ids:
                    placeholders = ", ".join("?" for _ in seen_schedule_ids)
                    await connection.execute(
                        f"""
                        UPDATE runtime_schedules
                        SET state = 'unknown', updated_at = ?
                        WHERE sdk_session_id = ? AND state = 'active'
                          AND runtime_schedule_id NOT IN ({placeholders})
                        """,
                        (observed_at, event.sdk_session_id, *seen_schedule_ids),
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE runtime_schedules
                        SET state = 'unknown', updated_at = ?
                        WHERE sdk_session_id = ? AND state = 'active'
                        """,
                        (observed_at, event.sdk_session_id),
                    )

        await connection.execute(
            """
            INSERT INTO snapshot_observations(
                snapshot_id, sdk_session_id, topic, requested_epoch,
                runtime_generation, owner_fence_token,
                query_start_sdk_receive_seq, query_end_sdk_receive_seq,
                positive_evidence, negative_applied, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id) DO NOTHING
            """,
            (
                snapshot_id,
                event.sdk_session_id,
                topic,
                epoch,
                event.generation,
                event.fence_token,
                query_start,
                query_end,
                int(positive),
                int(negative_applied),
                observed_at,
            ),
        )
        applied = fresh and caught_up
        await connection.execute(
            """
            UPDATE reconciliation_state
            SET applied_epoch = CASE WHEN ? THEN ? ELSE applied_epoch END,
                status = CASE WHEN ? THEN 'idle' ELSE 'requested' END,
                query_start_sdk_receive_seq = ?,
                query_end_sdk_receive_seq = ?,
                observed_at = ?,
                snapshot_id = CASE
                    WHEN ? OR ? THEN ? ELSE snapshot_id
                END,
                last_positive_epoch = CASE
                    WHEN ? THEN MAX(last_positive_epoch, ?)
                    ELSE last_positive_epoch
                END,
                uncertainty_reason = CASE
                    WHEN ? THEN NULL
                    WHEN ? THEN 'reducer_not_caught_up'
                    ELSE uncertainty_reason
                END
            WHERE sdk_session_id = ? AND topic = ?
            """,
            (
                applied,
                epoch,
                applied,
                query_start,
                query_end,
                observed_at,
                applied,
                positive,
                snapshot_id,
                positive,
                epoch,
                applied,
                fresh and not caught_up,
                event.sdk_session_id,
                topic,
            ),
        )
        if applied and topic in {"activity", "queue", "tasks"}:
            await self._settle_linked_loop_idle_submissions(
                connection,
                event,
                now=now,
            )

    async def _split_observed_submission(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        submission_id: str,
        app_state: str,
        runtime_correlation_basis: str,
        incident_kind: str,
        now: float,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT observed_user_event_id FROM submissions
            WHERE submission_id = ? AND sdk_session_id = ?
            """,
            (submission_id, event.sdk_session_id),
        )
        observed = await cursor.fetchone()
        await cursor.close()
        if observed is None or observed["observed_user_event_id"] is None:
            return
        user_event_id = str(observed["observed_user_event_id"])
        runtime_submission_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{event.sdk_session_id}:runtime-observed:{user_event_id}",
            )
        )
        await connection.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, runtime_schedule_id,
                prompt_hash, requested_mode, requested_delivery, observed_delivery,
                state, accepted_message_id, accepted_at,
                observed_user_event_id, observed_origin_hint,
                correlation_basis, autopilot_objective_id,
                task_completion_outcome, completion_basis, created_at, idle_at,
                observed_at, observed_interaction_id, objective_status,
                task_complete_event_id, abort_event_id, terminal_at,
                continuation_count
            )
            SELECT ?, sdk_session_id, 'runtime_observed', runtime_schedule_id,
                   prompt_hash, requested_mode, requested_delivery, observed_delivery,
                   state, accepted_message_id, accepted_at,
                   observed_user_event_id, observed_origin_hint,
                   ?, autopilot_objective_id,
                   task_completion_outcome, completion_basis,
                   COALESCE(observed_at, created_at), idle_at,
                   observed_at, observed_interaction_id, objective_status,
                   task_complete_event_id, abort_event_id, terminal_at,
                   continuation_count
            FROM submissions WHERE submission_id = ?
            ON CONFLICT(submission_id) DO NOTHING
            """,
            (
                runtime_submission_id,
                runtime_correlation_basis,
                submission_id,
            ),
        )
        for table in (
            "submission_segments",
            "model_turns",
            "submission_task_links",
            "background_observations",
            "task_card_projections",
            "autopilot_objectives",
        ):
            await connection.execute(
                f"UPDATE {table} SET submission_id = ? WHERE submission_id = ?",
                (runtime_submission_id, submission_id),
            )
        await connection.execute(
            """
            UPDATE liveness_leases
            SET lease_id = ?, source_id = ?
            WHERE sdk_session_id = ? AND kind = 'submission' AND source_id = ?
            """,
            (
                f"submission:{runtime_submission_id}",
                runtime_submission_id,
                event.sdk_session_id,
                submission_id,
            ),
        )
        await connection.execute(
            """
            UPDATE submissions
            SET state = ?, accepted_message_id = NULL,
                accepted_at = NULL, observed_user_event_id = NULL,
                observed_origin_hint = NULL, observed_delivery = NULL,
                observed_at = NULL, observed_interaction_id = NULL,
                correlation_basis = NULL, autopilot_objective_id = NULL,
                task_completion_outcome = NULL, completion_basis = NULL,
                objective_status = NULL, task_complete_event_id = NULL,
                abort_event_id = NULL, idle_at = NULL,
                terminal_at = CASE WHEN ? = 'rejected' THEN ? ELSE NULL END,
                continuation_count = 0
            WHERE submission_id = ?
            """,
            (
                app_state,
                app_state,
                event.received_at,
                submission_id,
            ),
        )
        await self._record_runtime_incident_once(
            connection,
            event,
            kind=incident_kind,
            detail={
                "app_submission_id": submission_id,
                "runtime_submission_id": runtime_submission_id,
                "user_event_id": user_event_id,
            },
        )

    async def _apply_user_message(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.event_id is None:
            raise RuntimeError("validated SDK user.message is missing its event ID")
        interaction_id = event.interaction_id or _value(data, "interactionId")
        observed_mode = _value(data, "agentMode")
        delivery = _value(data, "delivery")
        continuation = bool(data.get("isAutopilotContinuation"))

        if continuation:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND requested_mode = 'autopilot'
                  AND state IN ('observed_active', 'loop_idle', 'continuation_expected')
                ORDER BY observed_at DESC, created_at DESC
                """,
                (event.sdk_session_id,),
            )
            continuation_candidates = await cursor.fetchall()
            await cursor.close()
            if len(continuation_candidates) == 1:
                await self._observe_submission(
                    connection,
                    event,
                    submission_id=str(continuation_candidates[0]["submission_id"]),
                    correlation_basis="autopilot_continuation",
                    interaction_id=interaction_id,
                    delivery=delivery,
                    origin_hint="autopilot_continuation",
                    continuation=True,
                    now=now,
                )
                return

        candidates: list[Any] = []
        correlation_basis = ""
        exact_mapping = await self._capability_evidenced(
            connection,
            "accepted_user_event_id_mapping",
        )
        if exact_mapping:
            cursor = await connection.execute(
                """
                SELECT * FROM submissions
                WHERE sdk_session_id = ? AND accepted_message_id = ?
                  AND observed_user_event_id IS NULL
                  AND state IN ('submitted', 'submitted_unknown', 'outcome_unknown')
                """,
                (event.sdk_session_id, event.event_id),
            )
            candidates = list(await cursor.fetchall())
            await cursor.close()
            correlation_basis = "accepted_event_id_fixture"

        if not candidates and interaction_id is not None:
            cursor = await connection.execute(
                """
                SELECT * FROM submissions
                WHERE sdk_session_id = ? AND correlation_id = ?
                  AND observed_user_event_id IS NULL
                  AND state IN (
                      'submitting', 'submitted', 'submitted_unknown', 'outcome_unknown'
                  )
                """,
                (event.sdk_session_id, interaction_id),
            )
            candidates = list(await cursor.fetchall())
            await cursor.close()
            correlation_basis = "interaction_id"

        if not candidates:
            cursor = await connection.execute(
                """
                SELECT * FROM submissions
                WHERE sdk_session_id = ?
                  AND state IN (
                      'submitting', 'submitted', 'submitted_unknown', 'outcome_unknown'
                  )
                  AND observed_user_event_id IS NULL
                  AND (
                      (state = 'submitting'
                       AND send_started_at IS NOT NULL
                       AND send_started_at <= ?)
                      OR
                      (state IN ('submitted', 'submitted_unknown', 'outcome_unknown')
                       AND accepted_at IS NOT NULL
                       AND accepted_at <= ?)
                  )
                ORDER BY COALESCE(accepted_at, send_started_at), created_at
                """,
                (
                    event.sdk_session_id,
                    event.received_at,
                    event.received_at,
                ),
            )
            raw_candidates = list(await cursor.fetchall())
            await cursor.close()
            content = data.get("content")
            content_hash = (
                hashlib.sha256(str(content).encode()).hexdigest()
                if isinstance(content, str)
                else None
            )
            raw_attachments = data.get("attachments")
            attachment_count = (
                len(raw_attachments) if isinstance(raw_attachments, list) else None
            )
            candidates = [
                candidate
                for candidate in raw_candidates
                if (
                    content_hash is not None
                    and candidate["prompt_hash"] == content_hash
                    and (
                        not exact_mapping
                        or candidate["accepted_message_id"] is None
                        or str(candidate["accepted_message_id"]) == event.event_id
                    )
                    and (
                        observed_mode is None
                        or candidate["requested_mode"] is None
                        or candidate["requested_mode"] == observed_mode
                    )
                    and (
                        attachment_count is None
                        or int(candidate["attachment_count"]) == attachment_count
                    )
                )
            ]
            correlation_basis = "single_candidate_facts"

        if len(candidates) == 1:
            await self._observe_submission(
                connection,
                event,
                submission_id=str(candidates[0]["submission_id"]),
                correlation_basis=correlation_basis,
                interaction_id=interaction_id,
                delivery=delivery,
                origin_hint=None,
                continuation=False,
                now=now,
            )
            return

        if candidates:
            await self._record_runtime_incident(
                connection,
                event,
                kind="user_message_correlation_ambiguous",
                detail={
                    "candidate_count": len(candidates),
                    "candidate_ids": [str(item["submission_id"]) for item in candidates],
                },
            )
        await self._create_runtime_observed_submission(
            connection,
            event,
            data=data,
            interaction_id=interaction_id,
            observed_mode=observed_mode,
            delivery=delivery,
            continuation=continuation,
            now=now,
        )

    async def _observe_submission(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        submission_id: str,
        correlation_basis: str,
        interaction_id: str | None,
        delivery: str | None,
        origin_hint: str | None,
        continuation: bool,
        now: float,
    ) -> None:
        await connection.execute(
            """
            UPDATE submissions
            SET state = 'observed_active',
                observed_user_event_id = COALESCE(observed_user_event_id, ?),
                observed_at = COALESCE(observed_at, ?),
                observed_interaction_id = COALESCE(observed_interaction_id, ?),
                observed_delivery = COALESCE(?, observed_delivery),
                observed_origin_hint = COALESCE(?, observed_origin_hint),
                correlation_basis = ?,
                continuation_count = continuation_count + ?,
                terminal_at = NULL
            WHERE submission_id = ? AND sdk_session_id = ?
            """,
            (
                event.event_id,
                event.received_at,
                interaction_id,
                delivery,
                origin_hint,
                correlation_basis,
                int(continuation),
                submission_id,
                event.sdk_session_id,
            ),
        )
        cursor = await connection.execute(
            """
            SELECT COALESCE(MAX(segment_index), 0) + 1
            FROM submission_segments WHERE submission_id = ?
            """,
            (submission_id,),
        )
        segment = await cursor.fetchone()
        await cursor.close()
        await connection.execute(
            """
            INSERT INTO submission_segments(
                submission_id, segment_index, user_event_id, interaction_id,
                is_continuation, state, observed_at
            ) VALUES (?, ?, ?, ?, ?, 'observed_active', ?)
            ON CONFLICT(user_event_id) DO NOTHING
            """,
            (
                submission_id,
                int(segment[0]),
                event.event_id,
                interaction_id,
                int(continuation),
                event.received_at,
            ),
        )
        await connection.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
            ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                state = 'active',
                runtime_generation = excluded.runtime_generation,
                owner_fence_token = excluded.owner_fence_token,
                refreshed_at = excluded.refreshed_at,
                released_at = NULL
            """,
            (
                event.sdk_session_id,
                f"submission:{submission_id}",
                submission_id,
                event.generation,
                event.fence_token,
                now,
                now,
            ),
        )

    async def _create_runtime_observed_submission(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        interaction_id: str | None,
        observed_mode: str | None,
        delivery: str | None,
        continuation: bool,
        now: float,
    ) -> None:
        if event.event_id is None:
            return
        submission_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{event.sdk_session_id}:runtime-observed:{event.event_id}",
            )
        )
        content = str(data.get("content", ""))
        attachments = data.get("attachments")
        runtime_schedule_id = _value(data, "runtimeScheduleId")
        parent_task_id = _value(data, "parentAgentTaskId")
        origin_hint = (
            "autopilot_continuation"
            if continuation
            else "runtime_schedule"
            if runtime_schedule_id is not None
            else "background_task"
            if parent_task_id is not None
            else None
        )
        await connection.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, runtime_schedule_id,
                prompt_hash, requested_mode, requested_delivery, observed_delivery,
                state, observed_user_event_id, observed_origin_hint,
                correlation_basis, observed_interaction_id, attachment_count,
                created_at, observed_at, continuation_count
            ) VALUES (
                ?, ?, 'runtime_observed', ?, ?, ?, ?, ?,
                'observed_active', ?, ?, 'runtime_observed', ?, ?, ?, ?, ?
            )
            ON CONFLICT(submission_id) DO NOTHING
            """,
            (
                submission_id,
                event.sdk_session_id,
                runtime_schedule_id,
                hashlib.sha256(content.encode()).hexdigest(),
                observed_mode,
                delivery,
                delivery,
                event.event_id,
                origin_hint,
                interaction_id,
                len(attachments) if isinstance(attachments, list) else 0,
                event.received_at,
                event.received_at,
                int(continuation),
            ),
        )
        await self._observe_submission(
            connection,
            event,
            submission_id=submission_id,
            correlation_basis="runtime_observed",
            interaction_id=interaction_id,
            delivery=delivery,
            origin_hint=origin_hint,
            continuation=False,
            now=now,
        )

    async def _apply_model_turn(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> None:
        if event.turn_id is None:
            await self._record_runtime_incident(
                connection,
                event,
                kind="model_turn_missing_id",
                detail={"raw_type": event.raw_type},
            )
            return
        submission_id: str | None = None
        if event.interaction_id is not None:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND observed_interaction_id = ?
                  AND state IN ('observed_active', 'continuation_expected')
                """,
                (event.sdk_session_id, event.interaction_id),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            if len(rows) == 1:
                submission_id = str(rows[0]["submission_id"])
        if submission_id is None:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND state = 'observed_active'
                ORDER BY observed_at DESC, created_at DESC
                """,
                (event.sdk_session_id,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            if len(rows) == 1:
                submission_id = str(rows[0]["submission_id"])
        segment_index: int | None = None
        if submission_id is not None:
            cursor = await connection.execute(
                "SELECT MAX(segment_index) FROM submission_segments WHERE submission_id = ?",
                (submission_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            segment_index = None if row is None or row[0] is None else int(row[0])

        if event.raw_type == "assistant.turn_start":
            await connection.execute(
                """
                INSERT INTO model_turns(
                    sdk_turn_id, sdk_session_id, submission_id, segment_index,
                    agent_id, interaction_id, state, started_at, last_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, 'observed_start', ?, ?)
                ON CONFLICT(sdk_turn_id) DO UPDATE SET
                    submission_id = COALESCE(model_turns.submission_id,
                                             excluded.submission_id),
                    segment_index = COALESCE(model_turns.segment_index,
                                             excluded.segment_index),
                    agent_id = COALESCE(model_turns.agent_id, excluded.agent_id),
                    interaction_id = COALESCE(model_turns.interaction_id,
                                               excluded.interaction_id),
                    state = CASE
                        WHEN model_turns.ended_at IS NULL THEN 'observed_start'
                        ELSE model_turns.state
                    END,
                    last_event_id = excluded.last_event_id
                """,
                (
                    event.turn_id,
                    event.sdk_session_id,
                    submission_id,
                    segment_index,
                    event.agent_id,
                    event.interaction_id,
                    event.received_at,
                    event.event_id,
                ),
            )
        elif event.raw_type == "assistant.turn_retry":
            await connection.execute(
                """
                INSERT INTO model_turns(
                    sdk_turn_id, sdk_session_id, submission_id, segment_index,
                    agent_id, interaction_id, state, started_at,
                    retry_count, last_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, 'retrying', ?, 1, ?)
                ON CONFLICT(sdk_turn_id) DO UPDATE SET
                    state = CASE
                        WHEN model_turns.ended_at IS NULL THEN 'retrying'
                        ELSE model_turns.state
                    END,
                    retry_count = CASE
                        WHEN model_turns.ended_at IS NULL
                        THEN model_turns.retry_count + 1
                        ELSE model_turns.retry_count
                    END,
                    last_event_id = excluded.last_event_id
                """,
                (
                    event.turn_id,
                    event.sdk_session_id,
                    submission_id,
                    segment_index,
                    event.agent_id,
                    event.interaction_id,
                    event.received_at,
                    event.event_id,
                ),
            )
        else:
            await connection.execute(
                """
                INSERT INTO model_turns(
                    sdk_turn_id, sdk_session_id, submission_id, segment_index,
                    agent_id, interaction_id, state, started_at, ended_at,
                    last_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, 'observed_end', ?, ?, ?)
                ON CONFLICT(sdk_turn_id) DO UPDATE SET
                    state = CASE
                        WHEN model_turns.state = 'aborted' THEN model_turns.state
                        ELSE 'observed_end'
                    END,
                    ended_at = COALESCE(model_turns.ended_at, excluded.ended_at),
                    last_event_id = excluded.last_event_id
                """,
                (
                    event.turn_id,
                    event.sdk_session_id,
                    submission_id,
                    segment_index,
                    event.agent_id,
                    event.interaction_id,
                    event.received_at,
                    event.received_at,
                    event.event_id,
                ),
            )

    async def _apply_task_complete(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        raw_outcome = data.get("outcome")
        outcome = (
            str(raw_outcome)
            if raw_outcome in {"completed", "continue", "blocked"}
            else "completed"
            if raw_outcome is None and data.get("success") is True
            else None
        )
        objective_id = _value(data, "objectiveId")
        if objective_id is not None:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND autopilot_objective_id = ?
                  AND state IN ('observed_active', 'loop_idle', 'continuation_expected')
                """,
                (event.sdk_session_id, objective_id),
            )
        else:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND requested_mode = 'autopilot'
                  AND state IN ('observed_active', 'loop_idle', 'continuation_expected')
                ORDER BY observed_at DESC, created_at DESC
                """,
                (event.sdk_session_id,),
            )
        candidates = await cursor.fetchall()
        await cursor.close()
        if len(candidates) != 1:
            await self._record_runtime_incident(
                connection,
                event,
                kind="task_complete_correlation_ambiguous",
                detail={
                    "objective_id": objective_id,
                    "candidate_count": len(candidates),
                    "outcome": outcome,
                },
            )
            return
        submission_id = str(candidates[0]["submission_id"])
        await connection.execute(
            """
            UPDATE submissions
            SET task_completion_outcome = COALESCE(?, task_completion_outcome),
                task_complete_event_id = ?,
                state = CASE
                    WHEN ? = 'continue' THEN 'continuation_expected'
                    ELSE state
                END
            WHERE submission_id = ?
            """,
            (outcome, event.event_id, outcome, submission_id),
        )
        if outcome == "continue":
            await connection.execute(
                """
                UPDATE submission_segments
                SET state = 'continuation_expected'
                WHERE submission_id = ? AND state = 'observed_active'
                """,
                (submission_id,),
            )

    async def _apply_autopilot_objective(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        objective = data.get("objective")
        values = objective if isinstance(objective, dict) else data
        objective_id = _value(values, "id")
        operation = _value(data, "operation") or _value(values, "operation") or "update"
        status = _value(values, "status")
        if objective_id is None or event.event_id is None:
            await self._record_runtime_incident(
                connection,
                event,
                kind="autopilot_objective_missing_id",
                detail={"operation": operation, "status": status},
            )
            return
        cursor = await connection.execute(
            """
            SELECT submission_id FROM autopilot_objectives
            WHERE sdk_session_id = ? AND objective_id = ?
            """,
            (event.sdk_session_id, objective_id),
        )
        existing = await cursor.fetchone()
        await cursor.close()
        submission_id = None if existing is None else existing["submission_id"]
        if submission_id is None and operation != "delete":
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND requested_mode = 'autopilot'
                  AND state IN ('observed_active', 'loop_idle', 'continuation_expected')
                ORDER BY observed_at DESC, created_at DESC
                """,
                (event.sdk_session_id,),
            )
            candidates = await cursor.fetchall()
            await cursor.close()
            if len(candidates) == 1:
                submission_id = str(candidates[0]["submission_id"])
        await connection.execute(
            """
            INSERT INTO autopilot_objectives(
                sdk_session_id, objective_id, submission_id, status,
                operation, last_event_id, updated_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sdk_session_id, objective_id) DO UPDATE SET
                submission_id = COALESCE(autopilot_objectives.submission_id,
                                         excluded.submission_id),
                status = excluded.status,
                operation = excluded.operation,
                last_event_id = excluded.last_event_id,
                updated_at = excluded.updated_at,
                deleted_at = excluded.deleted_at
            """,
            (
                event.sdk_session_id,
                objective_id,
                submission_id,
                None if operation == "delete" else status,
                operation,
                event.event_id,
                now,
                now if operation == "delete" else None,
            ),
        )
        if submission_id is not None:
            await connection.execute(
                """
                UPDATE submissions
                SET autopilot_objective_id = ?,
                    objective_status = ?
                WHERE submission_id = ?
                """,
                (
                    objective_id,
                    None if operation == "delete" else status,
                    submission_id,
                ),
            )

    async def _apply_abort(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> None:
        await connection.execute(
            """
            UPDATE submissions
            SET abort_event_id = ?
            WHERE sdk_session_id = ?
              AND state IN ('submitted', 'observed_active', 'continuation_expected')
            """,
            (event.event_id, event.sdk_session_id),
        )
        await connection.execute(
            """
            UPDATE model_turns
            SET state = 'aborted', ended_at = COALESCE(ended_at, ?),
                last_event_id = ?
            WHERE sdk_session_id = ?
              AND state IN ('observed_start', 'retrying')
            """,
            (event.received_at, event.event_id, event.sdk_session_id),
        )

    async def _apply_session_idle(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT submission_id, requested_mode, task_completion_outcome,
                   objective_status
            FROM submissions
            WHERE sdk_session_id = ?
              AND state IN ('observed_active', 'continuation_expected')
            ORDER BY observed_at, created_at
            """,
            (event.sdk_session_id,),
        )
        candidates = await cursor.fetchall()
        await cursor.close()
        if not candidates:
            return
        aborted = bool(data.get("aborted"))
        for candidate in candidates:
            submission_id = str(candidate["submission_id"])
            mode = candidate["requested_mode"]
            outcome = candidate["task_completion_outcome"]
            objective_status = candidate["objective_status"]
            state = "loop_idle"
            completion_basis: str | None = None
            terminal = False
            if aborted:
                state = "observed_aborted"
                completion_basis = "session_idle_aborted"
                terminal = True
            elif outcome == "continue":
                state = "continuation_expected"
                completion_basis = "task_complete_continue"
            elif outcome == "blocked" or objective_status in {"paused", "cap_reached"}:
                state = "semantic_blocked"
                completion_basis = (
                    "task_complete_blocked"
                    if outcome == "blocked"
                    else f"objective_{objective_status}"
                )
                terminal = True
            elif mode == "autopilot":
                # Completion waits for post-idle task/activity/queue snapshots.
                pass
            elif mode in {None, "interactive", "plan", "shell"}:
                # A late task can appear after idle; snapshots own terminalization.
                pass

            await connection.execute(
                """
                UPDATE submissions
                SET state = ?, idle_at = ?, completion_basis = ?,
                    terminal_at = CASE WHEN ? THEN ? ELSE terminal_at END
                WHERE submission_id = ?
                """,
                (
                    state,
                    event.received_at,
                    completion_basis,
                    terminal,
                    event.received_at,
                    submission_id,
                ),
            )
            await connection.execute(
                """
                UPDATE submission_segments
                SET state = ?, idle_at = ?
                WHERE submission_id = ?
                  AND segment_index = (
                      SELECT MAX(segment_index) FROM submission_segments
                      WHERE submission_id = ?
                  )
                  AND state IN ('observed_active', 'continuation_expected')
                """,
                (state, event.received_at, submission_id, submission_id),
            )
            if terminal:
                await self._release_submission_liveness(
                    connection,
                    event,
                    submission_id=submission_id,
                    now=now,
                )

    async def _release_submission_liveness(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        submission_id: str,
        now: float,
    ) -> None:
        await connection.execute(
            """
            UPDATE liveness_leases
            SET state = 'released', refreshed_at = ?, released_at = ?
            WHERE sdk_session_id = ? AND kind = 'submission'
              AND source_id = ? AND state = 'active'
              AND runtime_generation = ? AND owner_fence_token = ?
            """,
            (
                now,
                now,
                event.sdk_session_id,
                submission_id,
                event.generation,
                event.fence_token,
            ),
        )

    async def _record_runtime_incident(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        kind: str,
        detail: dict[str, Any],
    ) -> None:
        await connection.execute(
            """
            INSERT INTO runtime_incidents(
                timestamp, runtime_generation, session_id, kind,
                last_inbox_seq, last_sdk_receive_seq, detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.received_at,
                event.generation,
                event.sdk_session_id,
                kind,
                event.inbox_seq,
                event.sdk_receive_seq,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
            ),
        )

    async def _record_runtime_incident_once(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        kind: str,
        detail: dict[str, Any],
    ) -> None:
        encoded = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        cursor = await connection.execute(
            """
            SELECT 1 FROM runtime_incidents
            WHERE session_id = ? AND kind = ? AND detail = ?
            LIMIT 1
            """,
            (event.sdk_session_id, kind, encoded),
        )
        existing = await cursor.fetchone()
        await cursor.close()
        if existing is not None:
            return
        await connection.execute(
            """
            INSERT INTO runtime_incidents(
                timestamp, runtime_generation, session_id, kind,
                last_inbox_seq, last_sdk_receive_seq, detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.received_at,
                event.generation,
                event.sdk_session_id,
                kind,
                event.inbox_seq,
                event.sdk_receive_seq,
                encoded,
            ),
        )

    async def _capability_evidenced(
        self,
        connection: Any,
        capability: str,
    ) -> bool:
        cursor = await connection.execute(
            """
            SELECT supported, evidence_status FROM capabilities
            WHERE capability = ? AND protocol_version > 0
            ORDER BY probed_at DESC LIMIT 1
            """,
            (capability,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return (
            row is not None
            and int(row["supported"]) == 1
            and row["evidence_status"] != "unknown"
        )


    async def _update_task_projection(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        facts = _task_projection_facts(event, data)
        if facts is None:
            return
        card_key, kind, title, state, progress, task_id, agent_id = facts
        if event.agent_id is not None and event.raw_type.startswith("assistant."):
            existing_cursor = await connection.execute(
                """
                SELECT card_key, kind FROM task_card_projections
                WHERE sdk_session_id = ? AND agent_id = ?
                ORDER BY first_seen_at LIMIT 1
                """,
                (event.sdk_session_id, event.agent_id),
            )
            existing = await existing_cursor.fetchone()
            await existing_cursor.close()
            if existing is not None:
                card_key = str(existing["card_key"])
                kind = str(existing["kind"])
                title = ""
        panel_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{event.sdk_session_id}:taskdeck",
            )
        )[:16]
        card_token = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{event.sdk_session_id}:taskdeck:{card_key}",
            )
        )[:16]
        terminal_at = now if state in {"completed", "failed", "cancelled"} else None
        await connection.execute(
            """
            INSERT INTO task_card_projections(
                sdk_session_id, panel_id, card_token, card_key, task_id, agent_id,
                kind, title, state, progress_summary, first_seen_at, terminal_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sdk_session_id, panel_id, card_key) DO UPDATE SET
                task_id = COALESCE(excluded.task_id, task_card_projections.task_id),
                agent_id = COALESCE(excluded.agent_id, task_card_projections.agent_id),
                kind = excluded.kind,
                title = CASE
                    WHEN excluded.title = '' THEN task_card_projections.title
                    ELSE excluded.title
                END,
                state = CASE
                    WHEN task_card_projections.terminal_at IS NOT NULL
                    THEN task_card_projections.state
                    ELSE excluded.state
                END,
                progress_summary = CASE
                    WHEN task_card_projections.terminal_at IS NOT NULL
                    THEN task_card_projections.progress_summary
                    ELSE COALESCE(excluded.progress_summary,
                                  task_card_projections.progress_summary)
                END,
                terminal_at = COALESCE(task_card_projections.terminal_at,
                                       excluded.terminal_at),
                revision = task_card_projections.revision + 1
            """,
            (
                event.sdk_session_id,
                panel_id,
                card_token,
                card_key,
                task_id,
                agent_id,
                kind,
                title,
                state,
                progress,
                now,
                terminal_at,
            ),
        )
        if event.raw_type == "subagent.started":
            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                ) VALUES (?, ?, 'background', ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                    state = 'active', refreshed_at = excluded.refreshed_at,
                    released_at = NULL
                """,
                (
                    event.sdk_session_id,
                    f"background:{card_key}",
                    card_key,
                    event.generation,
                    event.fence_token,
                    now,
                    now,
                ),
            )
        elif event.raw_type in {"subagent.completed", "subagent.failed"}:
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND lease_id = ?
                  AND runtime_generation = ? AND owner_fence_token = ?
                  AND state = 'active'
                """,
                (
                    now,
                    now,
                    event.sdk_session_id,
                    f"background:{card_key}",
                    event.generation,
                    event.fence_token,
                ),
            )

    async def _reopen_submission_for_task(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        submission_id: str,
        task_id: str,
        correlation_basis: str,
        evidence_time: float,
    ) -> bool:
        cursor = await connection.execute(
            """
            SELECT state, terminal_at FROM submissions
            WHERE submission_id = ? AND sdk_session_id = ?
            """,
            (submission_id, event.sdk_session_id),
        )
        submission = await cursor.fetchone()
        await cursor.close()
        if submission is None:
            return False
        if submission["state"] != "semantic_complete":
            return True
        if (
            submission["terminal_at"] is None
            or float(submission["terminal_at"]) > evidence_time
        ):
            await self._record_runtime_incident_once(
                connection,
                event,
                kind="stale_task_evidence_ignored_for_completed_submission",
                detail={
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "correlation_basis": correlation_basis,
                    "evidence_time": evidence_time,
                    "terminal_at": submission["terminal_at"],
                },
            )
            return False
        cursor = await connection.execute(
            """
            UPDATE submissions
            SET state = 'loop_idle', completion_basis = NULL, terminal_at = NULL
            WHERE submission_id = ? AND sdk_session_id = ?
              AND state = 'semantic_complete' AND terminal_at <= ?
            """,
            (submission_id, event.sdk_session_id, evidence_time),
        )
        reopened = cursor.rowcount == 1
        await cursor.close()
        if not reopened:
            return False
        await connection.execute(
            """
            UPDATE submission_segments
            SET state = 'loop_idle'
            WHERE submission_id = ?
              AND segment_index = (
                  SELECT MAX(segment_index) FROM submission_segments
                  WHERE submission_id = ?
              )
              AND state = 'semantic_complete'
            """,
            (submission_id, submission_id),
        )
        await connection.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
            ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                state = 'active',
                runtime_generation = excluded.runtime_generation,
                owner_fence_token = excluded.owner_fence_token,
                refreshed_at = excluded.refreshed_at,
                released_at = NULL
            """,
            (
                event.sdk_session_id,
                f"submission:{submission_id}",
                submission_id,
                event.generation,
                event.fence_token,
                evidence_time,
                evidence_time,
            ),
        )
        await self._record_runtime_incident_once(
            connection,
            event,
            kind="late_task_reopened_submission",
            detail={
                "task_id": task_id,
                "submission_id": submission_id,
                "correlation_basis": correlation_basis,
            },
        )
        return True

    async def _resolve_task_submission(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        task: dict[str, Any],
        task_id: str,
        evidence_time: float,
    ) -> tuple[str | None, str | None, str | None]:
        cursor = await connection.execute(
            """
            SELECT submission_id, correlation_basis, objective_id,
                   state, terminal_at
            FROM submission_task_links
            WHERE sdk_session_id = ? AND task_id = ?
            """,
            (event.sdk_session_id, task_id),
        )
        existing = await cursor.fetchone()
        await cursor.close()
        if existing is not None:
            submission_id = str(existing["submission_id"])
            correlation_basis = str(existing["correlation_basis"])
            if existing["terminal_at"] is not None:
                incoming_state = str(task.get("status", "")).lower()
                if (
                    incoming_state not in {"completed", "failed", "cancelled"}
                    and evidence_time > float(existing["terminal_at"])
                ):
                    await self._record_runtime_incident_once(
                        connection,
                        event,
                        kind="terminal_task_nonterminal_reappearance_ignored",
                        detail={
                            "task_id": task_id,
                            "submission_id": submission_id,
                            "terminal_state": str(existing["state"]),
                            "incoming_state": incoming_state,
                            "terminal_at": existing["terminal_at"],
                            "evidence_time": evidence_time,
                        },
                    )
            elif not await self._reopen_submission_for_task(
                connection,
                event,
                submission_id=submission_id,
                task_id=task_id,
                correlation_basis=correlation_basis,
                evidence_time=evidence_time,
            ):
                return None, None, (
                    None
                    if existing["objective_id"] is None
                    else str(existing["objective_id"])
                )
            return (
                submission_id,
                correlation_basis,
                (
                    None
                    if existing["objective_id"] is None
                    else str(existing["objective_id"])
                ),
            )

        explicit_submission_id = _value(task, "submissionId") or _value(
            task,
            "submission_id",
        )
        objective_id = _value(task, "objectiveId") or _value(task, "objective_id")
        if explicit_submission_id is not None:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND submission_id = ?
                """,
                (event.sdk_session_id, explicit_submission_id),
            )
            explicit = await cursor.fetchone()
            await cursor.close()
            if explicit is None:
                await self._record_runtime_incident(
                    connection,
                    event,
                    kind="task_submission_reference_missing",
                    detail={
                        "task_id": task_id,
                        "submission_id": explicit_submission_id,
                    },
                )
                return None, None, objective_id
            if not await self._reopen_submission_for_task(
                connection,
                event,
                submission_id=explicit_submission_id,
                task_id=task_id,
                correlation_basis="explicit_submission_id",
                evidence_time=evidence_time,
            ):
                return None, None, objective_id
            return explicit_submission_id, "explicit_submission_id", objective_id

        if objective_id is not None:
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND autopilot_objective_id = ?
                """,
                (event.sdk_session_id, objective_id),
            )
            objective_candidates = await cursor.fetchall()
            await cursor.close()
            if len(objective_candidates) == 1:
                submission_id = str(objective_candidates[0]["submission_id"])
                if not await self._reopen_submission_for_task(
                    connection,
                    event,
                    submission_id=submission_id,
                    task_id=task_id,
                    correlation_basis="objective_id",
                    evidence_time=evidence_time,
                ):
                    return None, None, objective_id
                return (
                    submission_id,
                    "objective_id",
                    objective_id,
                )
            if objective_candidates:
                await self._record_runtime_incident(
                    connection,
                    event,
                    kind="task_objective_correlation_ambiguous",
                    detail={
                        "task_id": task_id,
                        "objective_id": objective_id,
                        "candidate_count": len(objective_candidates),
                    },
                )
                return None, None, objective_id

        cursor = await connection.execute(
            """
            SELECT submission_id FROM submissions
            WHERE sdk_session_id = ?
              AND state IN ('observed_active', 'loop_idle', 'continuation_expected')
              AND observed_at IS NOT NULL AND observed_at <= ?
            ORDER BY observed_at DESC, created_at DESC
            """,
            (event.sdk_session_id, evidence_time),
        )
        candidates = await cursor.fetchall()
        await cursor.close()
        if len(candidates) == 1:
            submission_id = str(candidates[0]["submission_id"])
            if not await self._reopen_submission_for_task(
                connection,
                event,
                submission_id=submission_id,
                task_id=task_id,
                correlation_basis="single_active_submission",
                evidence_time=evidence_time,
            ):
                return None, None, None
            return submission_id, "single_active_submission", None
        if not candidates:
            cursor = await connection.execute(
                """
                SELECT submission_id, state, completion_basis, terminal_at
                FROM submissions
                WHERE sdk_session_id = ?
                ORDER BY created_at DESC, submission_id DESC
                LIMIT 1
                """,
                (event.sdk_session_id,),
            )
            latest = await cursor.fetchone()
            await cursor.close()
            if (
                latest is not None
                and latest["state"] == "semantic_complete"
                and latest["completion_basis"]
                in {"loop_idle", "task_complete_final_idle", "tasks_terminal_quiet"}
                and latest["terminal_at"] is not None
                and float(latest["terminal_at"]) <= evidence_time
            ):
                submission_id = str(latest["submission_id"])
                if not await self._reopen_submission_for_task(
                    connection,
                    event,
                    submission_id=submission_id,
                    task_id=task_id,
                    correlation_basis="late_task_after_idle",
                    evidence_time=evidence_time,
                ):
                    return None, None, None
                return submission_id, "late_task_after_idle", None
        if candidates:
            await self._record_runtime_incident(
                connection,
                event,
                kind="task_submission_correlation_ambiguous",
                detail={
                    "task_id": task_id,
                    "candidate_count": len(candidates),
                },
            )
        return None, None, objective_id

    async def _settle_linked_loop_idle_submissions(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT submission_id, idle_at, requested_mode,
                   task_completion_outcome, objective_status
            FROM submissions
            WHERE sdk_session_id = ? AND state = 'loop_idle'
            """,
            (event.sdk_session_id,),
        )
        candidates = await cursor.fetchall()
        await cursor.close()
        for candidate in candidates:
            submission_id = str(candidate["submission_id"])
            cursor = await connection.execute(
                """
                SELECT state, terminal_at FROM submission_task_links
                WHERE sdk_session_id = ? AND submission_id = ?
                """,
                (event.sdk_session_id, submission_id),
            )
            links = await cursor.fetchall()
            await cursor.close()
            if links and any(
                link["state"] not in {"completed", "failed", "cancelled"}
                or link["terminal_at"] is None
                for link in links
            ):
                continue
            cutoff = float(candidate["idle_at"] or 0)
            if links:
                cutoff = max(
                    cutoff,
                    max(float(link["terminal_at"]) for link in links),
                )
            cursor = await connection.execute(
                """
                SELECT topic, requested_epoch, applied_epoch, status, observed_at
                FROM reconciliation_state
                WHERE sdk_session_id = ?
                  AND topic IN ('activity', 'queue', 'tasks')
                """,
                (event.sdk_session_id,),
            )
            snapshots = {str(row["topic"]): row for row in await cursor.fetchall()}
            await cursor.close()
            if any(
                topic not in snapshots
                or snapshots[topic]["status"] != "idle"
                or int(snapshots[topic]["requested_epoch"])
                != int(snapshots[topic]["applied_epoch"])
                or snapshots[topic]["observed_at"] is None
                or float(snapshots[topic]["observed_at"]) < cutoff
                for topic in ("activity", "queue", "tasks")
            ):
                continue
            settled_at = max(
                float(snapshots[topic]["observed_at"])
                for topic in ("activity", "queue", "tasks")
            )
            cursor = await connection.execute(
                """
                SELECT runtime_processing, runtime_has_active_work,
                       native_queue_count, native_steering_count
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
            binding = await cursor.fetchone()
            await cursor.close()
            if binding is None or any(
                (
                    bool(binding["runtime_processing"]),
                    bool(binding["runtime_has_active_work"]),
                    int(binding["native_queue_count"] or 0) > 0,
                    int(binding["native_steering_count"] or 0) > 0,
                )
            ):
                continue
            failed = any(link["state"] in {"failed", "cancelled"} for link in links)
            if failed:
                state = "semantic_blocked"
                basis = "tasks_terminal_failure_quiet"
            elif links:
                state = "semantic_complete"
                basis = "tasks_terminal_quiet"
            elif candidate["requested_mode"] == "autopilot":
                if candidate["task_completion_outcome"] != "completed":
                    continue
                state = "semantic_complete"
                basis = "task_complete_final_idle"
            else:
                state = "semantic_complete"
                basis = "loop_idle"
            cursor = await connection.execute(
                """
                UPDATE submissions
                SET state = ?, completion_basis = ?, terminal_at = ?
                WHERE submission_id = ? AND state = 'loop_idle'
                """,
                (state, basis, settled_at, submission_id),
            )
            changed = cursor.rowcount
            await cursor.close()
            if changed == 1:
                await connection.execute(
                    """
                    UPDATE submission_segments
                    SET state = ?
                    WHERE submission_id = ?
                      AND segment_index = (
                          SELECT MAX(segment_index) FROM submission_segments
                          WHERE submission_id = ?
                      )
                      AND state = 'loop_idle'
                    """,
                    (state, submission_id, submission_id),
                )
                await self._release_submission_liveness(
                    connection,
                    event,
                    submission_id=submission_id,
                    now=settled_at,
                )

    async def _apply_task_snapshot(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
        allow_negative: bool = True,
    ) -> None:
        raw_tasks = data.get("tasks", [])
        tasks = [task for task in raw_tasks if isinstance(task, dict)]
        evidence_time = float(data.get("observed_at", now))
        seen: set[str] = set()
        terminal_states = {"completed", "failed", "cancelled"}
        panel_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{event.sdk_session_id}:taskdeck",
            )
        )[:16]
        for task in tasks:
            task_id = str(task.get("id", "")).strip()
            state = str(task.get("status", "")).lower()
            if not task_id or state not in {
                "running",
                "idle",
                "completed",
                "failed",
                "cancelled",
            }:
                continue
            seen.add(task_id)
            kind = str(task.get("type", "background"))
            title = str(
                task.get("description")
                or task.get("command")
                or task.get("agentType")
                or f"Background task {task_id[:12]}"
            )
            progress = next(
                (
                    str(task[key])
                    for key in ("error", "result", "latestResponse", "recentOutput")
                    if task.get(key)
                ),
                None,
            )
            card_key = f"task:{task_id}"
            submission_id, correlation_basis, objective_id = (
                await self._resolve_task_submission(
                    connection,
                    event,
                    task=task,
                    task_id=task_id,
                    evidence_time=evidence_time,
                )
            )
            card_token = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:{event.sdk_session_id}:taskdeck:{card_key}",
                )
            )[:16]
            terminal_at = evidence_time if state in terminal_states else None
            await connection.execute(
                """
                INSERT INTO task_card_projections(
                    sdk_session_id, panel_id, card_token, card_key, task_id,
                    submission_id, kind, title, state, progress_summary,
                    first_seen_at, terminal_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sdk_session_id, panel_id, card_key) DO UPDATE SET
                    submission_id = COALESCE(
                        task_card_projections.submission_id,
                        excluded.submission_id
                    ),
                    kind = excluded.kind,
                    title = excluded.title,
                    state = CASE
                        WHEN task_card_projections.terminal_at IS NOT NULL
                        THEN task_card_projections.state
                        ELSE excluded.state
                    END,
                    progress_summary = CASE
                        WHEN task_card_projections.terminal_at IS NOT NULL
                        THEN task_card_projections.progress_summary
                        ELSE COALESCE(
                            excluded.progress_summary,
                            task_card_projections.progress_summary
                        )
                    END,
                    terminal_at = COALESCE(
                        task_card_projections.terminal_at,
                        excluded.terminal_at
                    ),
                    revision = task_card_projections.revision + 1
                """,
                (
                    event.sdk_session_id,
                    panel_id,
                    card_token,
                    card_key,
                    task_id,
                    submission_id,
                    kind,
                    title,
                    state,
                    progress,
                    evidence_time,
                    terminal_at,
                ),
            )
            await connection.execute(
                """
                INSERT INTO background_observations(
                    sdk_session_id, runtime_generation, source_event_id,
                    task_id, task_type, submission_id, observed_state,
                    terminal_evidence, last_progress_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    sdk_session_id, runtime_generation, source_event_id
                ) DO UPDATE SET
                    observed_state = CASE
                        WHEN background_observations.terminal_evidence IS NOT NULL
                        THEN background_observations.observed_state
                        ELSE excluded.observed_state
                    END,
                    terminal_evidence = COALESCE(
                        background_observations.terminal_evidence,
                        excluded.terminal_evidence
                    ),
                    submission_id = COALESCE(
                        background_observations.submission_id,
                        excluded.submission_id
                    ),
                    last_progress_at = excluded.last_progress_at
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    card_key,
                    task_id,
                    kind,
                    submission_id,
                    state,
                    "task_snapshot" if state in terminal_states else None,
                    evidence_time,
                ),
            )
            if submission_id is not None and correlation_basis is not None:
                await connection.execute(
                    """
                    INSERT INTO submission_task_links(
                        sdk_session_id, task_id, submission_id, objective_id,
                        state, terminal_evidence, correlation_basis,
                        linked_at, last_progress_at, terminal_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sdk_session_id, task_id) DO UPDATE SET
                        submission_id = submission_task_links.submission_id,
                        objective_id = COALESCE(
                            submission_task_links.objective_id,
                            excluded.objective_id
                        ),
                        state = CASE
                            WHEN submission_task_links.terminal_at IS NOT NULL
                            THEN submission_task_links.state
                            ELSE excluded.state
                        END,
                        terminal_evidence = COALESCE(
                            submission_task_links.terminal_evidence,
                            excluded.terminal_evidence
                        ),
                        last_progress_at = excluded.last_progress_at,
                        terminal_at = COALESCE(
                            submission_task_links.terminal_at,
                            excluded.terminal_at
                        )
                    """,
                    (
                        event.sdk_session_id,
                        task_id,
                        submission_id,
                        objective_id,
                        state,
                        "task_snapshot" if state in terminal_states else None,
                        correlation_basis,
                        evidence_time,
                        evidence_time,
                        terminal_at,
                    ),
                )
            lease_id = f"background:{card_key}"
            if state in terminal_states:
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'released', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id = ? AND lease_id = ?
                      AND runtime_generation = ? AND owner_fence_token = ?
                      AND state = 'active'
                    """,
                    (
                        now,
                        now,
                        event.sdk_session_id,
                        lease_id,
                        event.generation,
                        event.fence_token,
                    ),
                )
            else:
                await connection.execute(
                    """
                    INSERT INTO liveness_leases(
                        sdk_session_id, lease_id, kind, source_id,
                        runtime_generation, owner_fence_token, state,
                        acquired_at, refreshed_at
                    ) VALUES (?, ?, 'background', ?, ?, ?, 'active', ?, ?)
                    ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                        state = CASE
                            WHEN liveness_leases.state = 'released'
                            THEN 'released'
                            ELSE 'active'
                        END,
                        refreshed_at = excluded.refreshed_at,
                        released_at = CASE
                            WHEN liveness_leases.state = 'released'
                            THEN liveness_leases.released_at
                            ELSE NULL
                        END
                    """,
                    (
                        event.sdk_session_id,
                        lease_id,
                        card_key,
                        event.generation,
                        event.fence_token,
                        now,
                        now,
                    ),
                )

        if not allow_negative:
            return
        cursor = await connection.execute(
            """
            SELECT source_event_id, task_id FROM background_observations
            WHERE sdk_session_id = ? AND runtime_generation = ?
              AND terminal_evidence IS NULL
              AND observed_state IN ('running', 'idle')
            """,
            (event.sdk_session_id, event.generation),
        )
        existing = await cursor.fetchall()
        await cursor.close()
        for observation in existing:
            task_id = str(observation["task_id"] or "")
            if task_id in seen:
                continue
            card_key = str(observation["source_event_id"])
            await connection.execute(
                """
                UPDATE background_observations
                SET observed_state = 'unknown', last_progress_at = ?
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND source_event_id = ? AND terminal_evidence IS NULL
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    card_key,
                ),
            )
            await connection.execute(
                """
                UPDATE task_card_projections
                SET state = 'unknown',
                    progress_summary = 'Task disappeared without terminal evidence.',
                    revision = revision + 1
                WHERE sdk_session_id = ? AND panel_id = ? AND card_key = ?
                  AND terminal_at IS NULL
                """,
                (event.sdk_session_id, panel_id, card_key),
            )
            await connection.execute(
                """
                UPDATE submission_task_links
                SET state = 'unknown', last_progress_at = ?
                WHERE sdk_session_id = ? AND task_id = ? AND terminal_at IS NULL
                """,
                (
                    evidence_time,
                    event.sdk_session_id,
                    task_id,
                ),
            )


class EventReducerWorker:
    def __init__(
        self,
        *,
        inbox: ReducerInbox,
        reducer: JournalReducer,
        batch_size: int,
        fence_validator: FenceValidator | None = None,
        task_registry: TaskRegistry | None = None,
    ) -> None:
        self._inbox = inbox
        self._reducer = reducer
        self._adapter = EventAdapter()
        self._batch_size = batch_size
        self._fence_validator = fence_validator
        self._task_registry = task_registry or TaskRegistry()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("reducer worker already started")
        self._task = self._task_registry.create(
            self._run(),
            name=f"reducer:{self._inbox.sdk_session_id}",
            source="event-reducer",
            session_id=self._inbox.sdk_session_id,
            runtime_generation=self._inbox.generation,
        )

    async def stop(self, *, timeout_seconds: float = 5) -> None:
        if self._task is None:
            return
        self._stopping = True
        self._inbox.close_sdk()
        worker = self._task
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._inbox.commit_internal(
                    {"type": "copilotd.reducer.stop"},
                    internal_event_id=f"stop:{uuid.uuid4()}",
                )
                await worker
        except TimeoutError:
            worker.cancel()
            try:
                async with asyncio.timeout(timeout_seconds):
                    await asyncio.gather(worker, return_exceptions=True)
            except TimeoutError:
                worker.add_done_callback(_consume_task_result)
        finally:
            self._task = None
            self._inbox.close()

    async def _run(self) -> None:
        while True:
            first = await self._inbox.get()
            batch = [first]
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._inbox.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if self._fence_validator is not None:
                valid = await self._fence_validator(
                    first.generation,
                    first.fence_token,
                )
                if not valid:
                    error = RuntimeError("reducer owner fence is no longer current")
                    for envelope in batch:
                        self._inbox.acknowledge(envelope, error=error)
                    raise error

            events: list[AdaptedEvent] = []
            valid_envelopes: list[InboxEnvelope] = []
            for envelope in batch:
                try:
                    events.append(self._adapter.adapt(envelope))
                except InvalidSdkEvent as error:
                    await self._reducer.persist_incident(envelope, error)
                    self._inbox.acknowledge(envelope)
                else:
                    valid_envelopes.append(envelope)
            try:
                if events:
                    await self._reducer.persist(events)
            except BaseException as error:
                for envelope in valid_envelopes:
                    self._inbox.acknowledge(envelope, error=error)
                raise
            should_stop = any(event.raw_type == "copilotd.reducer.stop" for event in events)
            for envelope in valid_envelopes:
                self._inbox.acknowledge(envelope)
            if should_stop and self._stopping:
                return


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _snapshot_has_positive_evidence(topic: str, values: dict[str, Any]) -> bool:
    if topic == "activity":
        return any(
            bool(values.get(key))
            for key in ("processing", "has_active_work", "abortable")
        )
    if topic == "queue":
        return bool(values.get("items") or values.get("steering_messages"))
    if topic == "tasks":
        return bool(values.get("tasks"))
    if topic == "remote":
        return str(values.get("mode", "unknown")) != "off"
    if topic == "schedules":
        return bool(values.get("schedules"))
    return bool(values)


def _value(data: Any, key: str) -> str | None:
    if not isinstance(data, dict):
        return None
    value = data.get(key)
    return None if value is None else str(value)


def _is_task_projection_event(event: AdaptedEvent) -> bool:
    return event.raw_type in RenderPlanner._TASK_TYPES or (
        event.agent_id is not None
        and event.raw_type
        in {
            "assistant.message_start",
            "assistant.message_delta",
            "assistant.message",
            "assistant.streaming_delta",
        }
    )


def _task_projection_facts(
    event: AdaptedEvent,
    data: dict[str, Any],
) -> tuple[str, str, str, str, str | None, str | None, str | None] | None:
    raw_type = event.raw_type
    tool_call_id = _value(data, "toolCallId")
    task_id = _value(data, "taskId")
    if raw_type.startswith("subagent."):
        card_id = tool_call_id or event.agent_id or event.event_id
        if card_id is None:
            return None
        title = (
            _value(data, "agentDisplayName")
            or _value(data, "agentName")
            or "Copilot subagent"
        )
        if raw_type == "subagent.started":
            return (
                f"agent:{card_id}",
                "agent",
                title,
                "running",
                _value(data, "agentDescription"),
                task_id,
                event.agent_id,
            )
        if raw_type == "subagent.completed":
            summary = _metric_summary(data)
            return (
                f"agent:{card_id}",
                "agent",
                title,
                "completed",
                summary or "Subagent completed.",
                task_id,
                event.agent_id,
            )
        if raw_type == "subagent.failed":
            return (
                f"agent:{card_id}",
                "agent",
                title,
                "failed",
                _value(data, "error") or "Subagent failed.",
                task_id,
                event.agent_id,
            )
        return None

    if raw_type.startswith("tool.execution_"):
        if tool_call_id is None:
            return None
        card_key = f"tool:{tool_call_id}"
        if raw_type == "tool.execution_start":
            title = _value(data, "toolName") or "Copilot tool"
            return card_key, "tool", title, "running", "Started.", task_id, event.agent_id
        if raw_type == "tool.execution_progress":
            return (
                card_key,
                "tool",
                "",
                "running",
                _value(data, "progressMessage"),
                task_id,
                event.agent_id,
            )
        success = bool(data.get("success"))
        error = data.get("error")
        summary = "Completed successfully." if success else _bounded_text(str(error), 300)
        return (
            card_key,
            "tool",
            "",
            "completed" if success else "failed",
            summary,
            task_id,
            event.agent_id,
        )

    if event.agent_id is not None and raw_type.startswith("assistant."):
        content = _value(data, "deltaContent") or _value(data, "content")
        return (
            f"agent:{event.agent_id}",
            "orphan",
            f"Agent {event.agent_id[:12]}",
            "running",
            content,
            task_id,
            event.agent_id,
        )
    return None


def _metric_summary(data: dict[str, Any]) -> str:
    values: list[str] = []
    if data.get("totalTokens") is not None:
        values.append(f"{int(data['totalTokens']):,} tokens")
    if data.get("totalToolCalls") is not None:
        values.append(f"{int(data['totalToolCalls']):,} tool calls")
    return ", ".join(values)


def _usage_summary_lines(data: dict[str, Any]) -> list[str]:
    labels = {
        "inputTokens": "Input tokens",
        "outputTokens": "Output tokens",
        "cacheReadTokens": "Cache-read tokens",
        "cacheWriteTokens": "Cache-write tokens",
        "totalTokens": "Total tokens",
        "premiumRequests": "Premium requests",
        "aiCredits": "AI credits",
        "nanoAiu": "nano AIU",
        "currentTokens": "Context tokens",
        "tokenLimit": "Context limit",
    }
    lines: list[str] = []
    for key, label in labels.items():
        value = _find_nested_value(data, key)
        if value is not None:
            lines.append(f"- {label}: `{value}`")
    return lines or ["Usage snapshot updated; this runtime did not expose numeric fields."]


def _find_nested_value(value: Any, key: str) -> Any:
    if not isinstance(value, dict):
        return None
    if key in value and isinstance(value[key], (str, int, float)):
        return value[key]
    for nested in value.values():
        found = _find_nested_value(nested, key)
        if found is not None:
            return found
    return None


def _status_title(raw_type: str) -> tuple[str, str]:
    values = {
        "abort": ("Copilot aborted", "The active agent loop was aborted."),
        "assistant.intent": ("Copilot is working", "Copilot updated its current intent."),
        "assistant.reasoning": (
            "Reasoning complete",
            "Copilot completed internal reasoning; raw chain-of-thought is hidden.",
        ),
        "assistant.turn_retry": ("Copilot retrying", "The model turn is being retried."),
        "copilotd.permissions.reconciled": (
            "Permission posture",
            "Copilot allow-all permission posture was reconciled.",
        ),
        "model.call_failure": ("Model call failed", "The current model call failed."),
        "session.autopilot_objective_changed": (
            "Autopilot objective",
            "The Autopilot objective changed.",
        ),
        "session.compaction_start": (
            "Compacting context",
            "Copilot started compacting the session context.",
        ),
        "session.compaction_complete": (
            "Context compacted",
            "Copilot finished compacting the session context.",
        ),
        "session.context_cleared": (
            "Context cleared",
            "Copilot cleared the active session context.",
        ),
        "session.error": ("Copilot error", "The Copilot session reported an error."),
        "session.warning": ("Copilot warning", "The Copilot session reported a warning."),
        "session.info": ("Copilot info", "The Copilot session reported an update."),
        "session.snapshot_rewind": (
            "Session rewound",
            "Copilot rewound the session to an earlier snapshot.",
        ),
        "session.shutdown": ("Copilot session ended", "The Copilot session shut down."),
        "session.task_complete": (
            "Task evaluation",
            "Copilot evaluated the current task.",
        ),
        "session.truncation": (
            "Context truncated",
            "Copilot truncated session context to stay within model limits.",
        ),
        "session.workspace_file_changed": (
            "Workspace changed",
            "Copilot changed a workspace file.",
        ),
    }
    return values.get(raw_type, ("Copilot status", raw_type))


def _status_detail(raw_type: str, data: dict[str, Any], *, fallback: str) -> str:
    if raw_type == "assistant.reasoning":
        return fallback
    if raw_type == "copilotd.permissions.reconciled":
        posture = str(data.get("posture", "unknown"))
        error_type = data.get("error_type")
        suffix = "" if error_type is None else f" (`{error_type}`)"
        return f"Posture: `{posture}`{suffix}"
    if raw_type == "session.workspace_file_changed":
        operation = str(data.get("operation", "changed"))
        path = str(data.get("path", "unknown path"))
        return f"`{operation}` `{path}`"
    if raw_type == "session.task_complete":
        outcome = data.get("outcome")
        if outcome is not None:
            return f"Outcome: `{outcome}`"
    if raw_type == "session.autopilot_objective_changed":
        objective = data.get("objective")
        if isinstance(objective, dict):
            status = objective.get("status")
            if status is not None:
                return f"Status: `{status}`"
    return next(
        (
            str(data[key])
            for key in (
                "message",
                "summary",
                "error",
                "reason",
                "intent",
                "shutdownType",
            )
            if data.get(key)
        ),
        fallback,
    )


def _tool_output_artifact(event: AdaptedEvent) -> dict[str, Any] | None:
    if event.raw_type != "tool.execution_complete":
        return None
    data = event.raw_payload.get("data", event.raw_payload)
    if not isinstance(data, dict):
        return None
    success = bool(data.get("success"))
    text: str | None = None
    source = "error"
    if success:
        result = data.get("result")
        if isinstance(result, dict):
            detailed = result.get("detailedContent")
            if isinstance(detailed, str) and detailed:
                text, source = detailed, "detailedContent"
            elif result.get("contents"):
                text, source = _structured_tool_text(result["contents"]), "contents"
            elif result.get("structuredContent") is not None:
                text = json.dumps(
                    result["structuredContent"],
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                source = "structuredContent"
            elif isinstance(result.get("content"), str):
                text, source = str(result["content"]), "content"
    else:
        error = data.get("error")
        if isinstance(error, dict):
            text = str(error.get("message", ""))
        elif error is not None:
            text = str(error)
    if text is None or len(text) < 8000:
        return None
    tool_call_id = str(data.get("toolCallId", "tool"))
    filename = (
        f"tool-output-{tool_call_id[:12]}.txt"
        if success
        else f"tool-error-{tool_call_id[:12]}.txt"
    )
    line_count = text.count("\n") + 1
    caveat = (
        " Runtime fallback content may be truncated."
        if source == "content"
        else ""
    )
    status = "completed" if success else "failed"
    return {
        "type": "tool_output_artifact",
        "content": (
            f"**Tool {status}** — `{len(text):,}` characters / "
            f"`{line_count:,}` lines attached as `{filename}`.{caveat}"
        ),
        "finalized": True,
        "attachments": [
            {
                "filename": filename,
                "media_type": "text/plain",
                "content": text,
            }
        ],
    }


def _structured_tool_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        pieces = [_structured_tool_text(item) for item in value]
        return "\n".join(piece for piece in pieces if piece)
    if isinstance(value, dict):
        for key in ("text", "content", "value"):
            if isinstance(value.get(key), str):
                return str(value[key])
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    return str(value)


def _task_state_icon(state: str) -> str:
    return {
        "running": "▶",
        "completed": "✅",
        "failed": "❌",
        "cancelled": "⏹",
        "unknown": "❔",
    }.get(state, "•")


def _bounded_text(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    return normalized if len(normalized) <= limit else normalized[: limit - 1] + "…"


def _model_config_matches(
    requested: dict[str, Any],
    observed: dict[str, Any],
) -> bool:
    return all(value is None or observed.get(key) == value for key, value in requested.items())
