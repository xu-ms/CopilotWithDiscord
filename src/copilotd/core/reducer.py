from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, ClassVar

from copilotd.core.event_adapter import EventAdapter
from copilotd.core.inbox import ReducerInbox
from copilotd.core.interactions import interaction_target_mode
from copilotd.core.models import AdaptedEvent, RenderIntent
from copilotd.storage.database import Database

FenceValidator = Callable[[int, int], Awaitable[bool]]
_DIFF_OUTPUT_LIMIT = 8 * 1024 * 1024
_TOOL_SPILL_RETENTION_SECONDS = 7 * 24 * 60 * 60
_TRUSTED_LOCAL_IMAGE_SUFFIXES = {
    ".bmp",
    ".gif",
    ".jpeg",
    ".jpg",
    ".png",
    ".tiff",
    ".webp",
}


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
    _FOOTER_TYPES: ClassVar[set[str]] = {"session.idle"}
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
        artifact_override: dict[str, Any] | None = None,
        suppress_default_artifact: bool = False,
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
        elif event.raw_type in self._FOOTER_TYPES:
            lane = "footer"
            finalized = True
            coalesce_key = f"footer:{event.turn_id or event.event_id or event.inbox_seq}"
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
        diff = _diff_render_payload(event)
        artifact = (
            artifact_override
            if artifact_override is not None or suppress_default_artifact
            else _tool_output_artifact(event)
        )
        if (
            diff is not None
            and event.raw_type == "tool.execution_complete"
            and artifact_override is None
        ):
            artifact = None
        if artifact is not None:
            stable_outbox_key = artifact.get("stable_outbox_key")
            artifact_key = (
                f"artifact:{event.sdk_session_id}:{stable_outbox_key}"
                if stable_outbox_key is not None
                else f"{idempotency_key}:artifact"
            )
            artifact_finalized = bool(artifact.get("finalized", True))
            intents.append(
                RenderIntent(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, artifact_key)),
                    session_id=event.sdk_session_id,
                    logical_seq=event.inbox_seq,
                    lane="artifact",
                    coalesce_key=(
                        None
                        if artifact.get("coalesce_key") is None
                        else str(artifact["coalesce_key"])
                    ),
                    idempotency_key=artifact_key,
                    payload=artifact,
                    finalized=artifact_finalized,
                )
            )
        if diff is not None:
            diff_key = f"{idempotency_key}:diff"
            intents.append(
                RenderIntent(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, diff_key)),
                    session_id=event.sdk_session_id,
                    logical_seq=event.inbox_seq,
                    lane="diff",
                    coalesce_key=None,
                    idempotency_key=diff_key,
                    payload=diff,
                    finalized=True,
                )
            )
        return intents


class JournalReducer:
    """Atomically persists event journal rows and their render intents."""

    def __init__(
        self,
        database: Database,
        planner: RenderPlanner | None = None,
        *,
        artifact_root: Path | None = None,
    ) -> None:
        self._database = database
        self._planner = planner or RenderPlanner()
        self._artifact_root = (
            database.path.parent / "sessions" if artifact_root is None else artifact_root
        )

    async def persist(self, events: list[AdaptedEvent]) -> int:
        inserted = 0
        now = time.time()
        async with self._database.transaction() as connection:
            for event in events:
                cursor = await connection.execute(
                    """
                    INSERT INTO event_journal(
                        sdk_session_id, generation, inbox_seq, source,
                        sdk_receive_seq, event_id, internal_event_id, ephemeral,
                        persistence_class, raw_type, parent_id, agent_id,
                        message_id, turn_id, interaction_id, request_id,
                        reducer_hash, raw_payload, received_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        event.sdk_session_id,
                        event.generation,
                        event.inbox_seq,
                        event.source,
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
                        event.request_id,
                        event.reducer_hash,
                        json.dumps(event.raw_payload, ensure_ascii=False, sort_keys=True),
                        event.received_at,
                    ),
                )
                was_inserted = cursor.rowcount == 1
                await cursor.close()
                if not was_inserted:
                    continue
                inserted += 1
                await self._apply_domain_state(connection, event, now=now)
                render_payload = await self._materialize_render_payload(
                    connection,
                    event,
                    now=now,
                )
                suppress_default_artifact, artifact_override = await self._cumulative_tool_artifact(
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
                planner_parameters = inspect.signature(self._planner.plan).parameters
                planner_kwargs: dict[str, Any] = {
                    "payload_override": render_payload,
                }
                if "artifact_override" in planner_parameters:
                    planner_kwargs.update(
                        artifact_override=artifact_override,
                        suppress_default_artifact=suppress_default_artifact,
                    )
                for intent in self._planner.plan(event, **planner_kwargs):
                    serialized_payload = json.dumps(
                        intent.payload,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if intent.payload.get("stable_outbox_key") is not None:
                        await connection.execute(
                            """
                            INSERT INTO render_outbox(
                                id, session_id, logical_seq, lane, coalesce_key,
                                idempotency_key, payload, state, attempts,
                                next_attempt_at, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                            ON CONFLICT(idempotency_key) DO UPDATE SET
                                logical_seq = excluded.logical_seq,
                                coalesce_key = excluded.coalesce_key,
                                payload = excluded.payload,
                                payload_revision = render_outbox.payload_revision + 1,
                                state = CASE
                                    WHEN render_outbox.state = 'sending'
                                    THEN 'sending'
                                    ELSE 'pending'
                                END,
                                next_attempt_at = CASE
                                    WHEN render_outbox.state IN ('pending', 'sending')
                                    THEN MAX(
                                        render_outbox.next_attempt_at,
                                        excluded.next_attempt_at
                                    )
                                    ELSE excluded.next_attempt_at
                                END,
                                updated_at = excluded.updated_at
                            """,
                            (
                                intent.id,
                                intent.session_id,
                                intent.logical_seq,
                                intent.lane,
                                intent.coalesce_key,
                                intent.idempotency_key,
                                serialized_payload,
                                now,
                                now,
                                now,
                            ),
                        )
                        continue
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
                            serialized_payload,
                            now,
                            now,
                            now,
                        ),
                    )
        return inserted

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
        if event.raw_type in RenderPlanner._FOOTER_TYPES:
            submission_cursor = await connection.execute(
                """
                SELECT submission_id, created_at, idle_at
                FROM submissions
                WHERE sdk_session_id = ? AND idle_at IS NOT NULL
                ORDER BY idle_at DESC LIMIT 1
                """,
                (event.sdk_session_id,),
            )
            submission = await submission_cursor.fetchone()
            await submission_cursor.close()
            binding_cursor = await connection.execute(
                """
                SELECT runtime_model_config, desired_model_config
                FROM session_bindings WHERE sdk_session_id = ?
                """,
                (event.sdk_session_id,),
            )
            binding = await binding_cursor.fetchone()
            await binding_cursor.close()
            usage_cursor = await connection.execute(
                """
                SELECT model, input_tokens, output_tokens, nano_aiu,
                       premium_requests, observed_at
                FROM usage_samples WHERE session_id = ?
                ORDER BY observed_at DESC, id DESC LIMIT 1
                """,
                (event.sdk_session_id,),
            )
            usage = await usage_cursor.fetchone()
            await usage_cursor.close()
            context_cursor = await connection.execute(
                """
                SELECT payload, observed_at FROM session_projection_snapshots
                WHERE session_id = ? AND kind = 'context'
                """,
                (event.sdk_session_id,),
            )
            context = await context_cursor.fetchone()
            await context_cursor.close()
            background_cursor = await connection.execute(
                """
                SELECT COUNT(*) FROM liveness_leases
                WHERE sdk_session_id = ? AND kind = 'background' AND state = 'active'
                """,
                (event.sdk_session_id,),
            )
            background = int((await background_cursor.fetchone())[0])
            await background_cursor.close()
            model_config: dict[str, Any] = {}
            if binding is not None:
                encoded = binding["runtime_model_config"] or binding["desired_model_config"]
                if encoded:
                    model_config = json.loads(str(encoded))
            model = (
                (None if usage is None else usage["model"])
                or model_config.get("modelId")
                or "unknown"
            )
            input_tokens = 0 if usage is None else int(usage["input_tokens"] or 0)
            output_tokens = 0 if usage is None else int(usage["output_tokens"] or 0)
            credits = 0.0 if usage is None else float(usage["premium_requests"] or 0)
            context_text = "unknown"
            if context is not None:
                context_payload = json.loads(str(context["payload"]))
                current = _find_nested_value(context_payload, "totalTokens")
                limit = _find_nested_value(context_payload, "limit")
                if current is not None or limit is not None:
                    context_text = f"{current or 0}/{limit or 0}"
            duration = 0.0
            submission_id = None
            if submission is not None:
                submission_id = str(submission["submission_id"])
                duration = max(
                    0.0,
                    float(submission["idle_at"]) - float(submission["created_at"]),
                )
            lines = [
                (
                    f"Model `{model}` · tokens `{input_tokens:,}` in / "
                    f"`{output_tokens:,}` out · credits `{credits:g}`"
                ),
                f"Context `{context_text}` · duration `{_format_elapsed(duration)}`",
            ]
            if background:
                lines.append("Background work was observed; this footer is a point-in-time status.")
            return {
                "type": "idle_footer",
                "content": "\n".join(lines),
                "submission_id": submission_id,
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "credits": credits,
                "context": context_text,
                "duration_seconds": duration,
                "background_observed": bool(background),
                "finalized": True,
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
                "finalized": event.raw_type not in {"assistant.intent", "session.compaction_start"},
            }
        if _is_task_projection_event(event) or event.raw_type in RenderPlanner._TASK_VIEW_TYPES:
            rows = await connection.execute(
                """
                SELECT panel_id, card_token, card_key, task_id, agent_id,
                       kind, title, state, progress_summary, detail_artifact,
                       first_seen_at, terminal_at, dependencies_json,
                       artifact_links_json, can_promote, last_progress_at,
                       revision
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
            for card in cards:
                card["dependencies"] = json.loads(str(card["dependencies_json"]))
                card["artifact_links"] = json.loads(str(card["artifact_links_json"]))
                card["can_promote"] = bool(card["can_promote"])
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
            page = 0 if state_row is None else min(max(int(state_row["page"]), 0), page_count - 1)
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
                ended_at = card["terminal_at"] or now
                elapsed = _format_elapsed(max(0.0, float(ended_at) - float(card["first_seen_at"])))
                lines.append(f"{icon} **{title}** · `{card['kind']}` · `{state}` · `{elapsed}`")
            attachments: list[dict[str, Any]] = []
            if expanded:
                lines.extend(("", f"**{selected_card['title']} details**"))
                detail = selected_card["progress_summary"]
                artifact = selected_card["detail_artifact"]
                if artifact:
                    filename = f"task-{selected_card['card_token']}-detail.md"
                    lines.append(
                        f"Large detail attached as `{filename}`; use Download for a fresh copy."
                    )
                    attachments.append(
                        {
                            "filename": filename,
                            "media_type": "text/markdown",
                            "content": str(artifact),
                        }
                    )
                elif detail:
                    lines.append(_bounded_text(str(detail), 700))
                dependencies = selected_card["dependencies"]
                if dependencies:
                    lines.append(
                        "**Dependencies:** "
                        + ", ".join(
                            f"`{_bounded_text(str(value), 80)}`" for value in dependencies[:10]
                        )
                    )
                artifact_links = selected_card["artifact_links"]
                if artifact_links:
                    lines.append(
                        "**Artifacts:** "
                        + " · ".join(_bounded_text(str(value), 160) for value in artifact_links[:8])
                    )
            if page_count > 1:
                lines.append(f"\nPage {page + 1}/{page_count}")
            selected_state = str(selected_card["state"])
            actions: list[str] = []
            if selected_card["task_id"] is not None and selected_state in {"running", "idle"}:
                actions.append("cancel")
                if selected_card["can_promote"]:
                    actions.append("promote")
                if str(selected_card["kind"]) == "agent":
                    actions.append("message")
            if selected_card["task_id"] is not None and selected_state in {
                "completed",
                "failed",
                "cancelled",
            }:
                actions.append("remove")
            if selected_card["detail_artifact"]:
                actions.append("download")
            return {
                "type": "taskdeck",
                "content": "\n".join(lines),
                "cards": cards,
                "attachments": attachments,
                "finalized": finalized,
                "taskdeck": {
                    "panel_id": panel_id,
                    "revision": deck_revision,
                    "page": page,
                    "page_count": page_count,
                    "selected_card_token": selected,
                    "expanded": expanded,
                    "actions": actions,
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
        if event.message_id is None:
            return None
        cursor = await connection.execute(
            """
            SELECT content, finalized FROM render_streams
            WHERE session_id = ? AND message_id = ? AND agent_id = ?
            """,
            (event.sdk_session_id, event.message_id, event.agent_id or ""),
        )
        stream = await cursor.fetchone()
        await cursor.close()
        trusted_cursor = await connection.execute(
            """
            SELECT trusted.path, snapshots.snapshot_path,
                   snapshots.byte_size, snapshots.sha256
            FROM trusted_local_artifacts AS trusted
            JOIN trusted_local_artifact_snapshots AS snapshots
              ON snapshots.session_id = trusted.session_id
             AND snapshots.source_path = trusted.path
            WHERE trusted.session_id = ?
            ORDER BY trusted.observed_at, trusted.path
            """,
            (event.sdk_session_id,),
        )
        trusted_rows = await trusted_cursor.fetchall()
        await trusted_cursor.close()
        payload = {
            "type": event.raw_type,
            "content": stream["content"],
            "message_id": event.message_id,
            "agent_id": event.agent_id,
            "finalized": bool(stream["finalized"]),
        }
        if trusted_rows:
            payload["trusted_local_images"] = True
            payload["trusted_local_image_paths"] = [str(row["path"]) for row in trusted_rows]
            payload["trusted_local_image_artifacts"] = [
                {
                    "source_path": str(row["path"]),
                    "snapshot_path": str(row["snapshot_path"]),
                    "byte_size": int(row["byte_size"]),
                    "sha256": str(row["sha256"]),
                }
                for row in trusted_rows
            ]
        return payload

    async def _accumulate_render_stream(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> str:
        agent_id = event.agent_id or ""
        if event.raw_type == "assistant.message_delta":
            delta = str(data.get("deltaContent", ""))
            await connection.execute(
                """
                INSERT INTO render_streams(
                    session_id, message_id, agent_id, content, finalized, updated_at
                ) VALUES (?, ?, ?, ?, 0, ?)
                ON CONFLICT(session_id, message_id, agent_id) DO UPDATE SET
                    content = CASE
                        WHEN render_streams.finalized = 1 THEN render_streams.content
                        ELSE render_streams.content || excluded.content
                    END,
                    updated_at = excluded.updated_at
                """,
                (event.sdk_session_id, event.message_id, agent_id, delta, now),
            )
        else:
            content = str(data.get("content", ""))
            await connection.execute(
                """
                INSERT INTO render_streams(
                    session_id, message_id, agent_id, content, finalized, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(session_id, message_id, agent_id) DO UPDATE SET
                    content = excluded.content,
                    finalized = 1,
                    updated_at = excluded.updated_at
                """,
                (event.sdk_session_id, event.message_id, agent_id, content, now),
            )
        cursor = await connection.execute(
            """
            SELECT content FROM render_streams
            WHERE session_id = ? AND message_id = ? AND agent_id = ?
            """,
            (event.sdk_session_id, event.message_id, agent_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return "" if row is None else str(row["content"])

    async def _append_tool_output(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        tool_call_id = _value(data, "toolCallId")
        partial = _tool_partial_text(data)
        if tool_call_id is None or partial is None:
            return
        cursor = await connection.execute(
            """
            SELECT content, finalized FROM tool_output_streams
            WHERE session_id = ? AND tool_call_id = ?
            """,
            (event.sdk_session_id, tool_call_id),
        )
        stream = await cursor.fetchone()
        await cursor.close()
        if stream is not None and bool(stream["finalized"]):
            return
        cursor = await connection.execute(
            """
            SELECT local_path, byte_size FROM tool_spill_artifacts
            WHERE session_id = ? AND tool_call_id = ?
            """,
            (event.sdk_session_id, tool_call_id),
        )
        artifact = await cursor.fetchone()
        await cursor.close()
        encoded = partial.encode("utf-8")
        if artifact is not None:
            path = Path(str(artifact["local_path"]))
            byte_size = await asyncio.to_thread(
                _append_spill_bytes,
                path,
                int(artifact["byte_size"]),
                encoded,
            )
            await connection.execute(
                """
                UPDATE tool_spill_artifacts
                SET byte_size = ?, sha256 = NULL,
                    retention_until = MAX(retention_until, ?),
                    updated_at = ?
                WHERE session_id = ? AND tool_call_id = ? AND finalized = 0
                """,
                (
                    byte_size,
                    now + _TOOL_SPILL_RETENTION_SECONDS,
                    now,
                    event.sdk_session_id,
                    tool_call_id,
                ),
            )
            await connection.execute(
                """
                UPDATE tool_output_streams
                SET content = '', spilled = 1, updated_at = ?
                WHERE session_id = ? AND tool_call_id = ? AND finalized = 0
                """,
                (now, event.sdk_session_id, tool_call_id),
            )
            return
        existing = "" if stream is None else str(stream["content"])
        combined = existing.encode("utf-8") + encoded
        if len(combined) < 64 * 1024:
            await connection.execute(
                """
                INSERT INTO tool_output_streams(
                    session_id, tool_call_id, content, spilled, updated_at
                ) VALUES (?, ?, ?, 0, ?)
                ON CONFLICT(session_id, tool_call_id) DO UPDATE SET
                    content = excluded.content,
                    updated_at = excluded.updated_at
                WHERE tool_output_streams.finalized = 0
                """,
                (event.sdk_session_id, tool_call_id, combined.decode(), now),
            )
            return
        path = _tool_spill_path(
            self._artifact_root,
            event.sdk_session_id,
            tool_call_id,
        )
        await asyncio.to_thread(_write_spill_bytes, path, combined)
        await connection.execute(
            """
            INSERT INTO tool_spill_artifacts(
                session_id, tool_call_id, local_path, byte_size,
                sha256, finalized, retention_until, updated_at
            ) VALUES (?, ?, ?, ?, NULL, 0, ?, ?)
            ON CONFLICT(session_id, tool_call_id) DO UPDATE SET
                local_path = excluded.local_path,
                byte_size = excluded.byte_size,
                sha256 = NULL,
                finalized = 0,
                retention_until = MAX(
                    tool_spill_artifacts.retention_until,
                    excluded.retention_until
                ),
                updated_at = excluded.updated_at
            """,
            (
                event.sdk_session_id,
                tool_call_id,
                str(path),
                len(combined),
                now + _TOOL_SPILL_RETENTION_SECONDS,
                now,
            ),
        )
        await connection.execute(
            """
            INSERT INTO tool_output_streams(
                session_id, tool_call_id, content, spilled,
                artifact_emitted, finalized, updated_at
            ) VALUES (?, ?, '', 1, 0, 0, ?)
            ON CONFLICT(session_id, tool_call_id) DO UPDATE SET
                content = '',
                spilled = 1,
                updated_at = excluded.updated_at
            WHERE tool_output_streams.finalized = 0
            """,
            (event.sdk_session_id, tool_call_id, now),
        )

    async def _cumulative_tool_artifact(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> tuple[bool, dict[str, Any] | None]:
        if event.raw_type not in {
            "tool.execution_progress",
            "tool.execution_complete",
        }:
            return False, None
        data = event.raw_payload.get("data", event.raw_payload)
        if not isinstance(data, dict):
            return False, None
        tool_call_id = _value(data, "toolCallId")
        if tool_call_id is None:
            return False, None
        cursor = await connection.execute(
            """
            SELECT content, spilled, artifact_emitted, finalized
            FROM tool_output_streams
            WHERE session_id = ? AND tool_call_id = ?
            """,
            (event.sdk_session_id, tool_call_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            if event.raw_type != "tool.execution_complete":
                return False, None
            await connection.execute(
                """
                INSERT INTO tool_output_streams(
                    session_id, tool_call_id, content, spilled,
                    artifact_emitted, finalized, updated_at
                ) VALUES (?, ?, '', 0, 0, 0, ?)
                """,
                (event.sdk_session_id, tool_call_id, now),
            )
            cursor = await connection.execute(
                """
                SELECT content, spilled, artifact_emitted, finalized
                FROM tool_output_streams
                WHERE session_id = ? AND tool_call_id = ?
                """,
                (event.sdk_session_id, tool_call_id),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                raise RuntimeError("completion stream initialization failed")
        if event.raw_type == "tool.execution_progress" and bool(row["finalized"]):
            return True, None
        if event.raw_type == "tool.execution_complete" and bool(row["finalized"]):
            return True, None
        if event.raw_type == "tool.execution_progress" and _tool_partial_text(data) is None:
            return bool(row["spilled"]), None
        cursor = await connection.execute(
            """
            SELECT local_path, byte_size, sha256, finalized
            FROM tool_spill_artifacts
            WHERE session_id = ? AND tool_call_id = ?
            """,
            (event.sdk_session_id, tool_call_id),
        )
        artifact = await cursor.fetchone()
        await cursor.close()
        source = "durable-stream"
        verbatim = True
        completion_bytes = b""
        completion_already_written = False
        if event.raw_type == "tool.execution_complete":
            completion, completion_source, completion_verbatim = _tool_display_text(
                data,
                success=bool(data.get("success")),
            )
            if completion:
                source = f"durable-stream+{completion_source}"
                verbatim = completion_verbatim
                canonical_bytes = completion.encode()
                if artifact is None:
                    stream_bytes = str(row["content"]).encode()
                    completion_matches_stream = stream_bytes == canonical_bytes
                else:
                    stream_bytes = b""
                    completion_matches_stream = await asyncio.to_thread(
                        _spill_matches_bytes,
                        Path(str(artifact["local_path"])),
                        int(artifact["byte_size"]),
                        canonical_bytes,
                    )
                if not completion_matches_stream:
                    completion_bytes = (
                        canonical_bytes
                        if artifact is None and not stream_bytes
                        else (
                            f"\n\n--- completion payload ({completion_source}) ---\n{completion}"
                        ).encode()
                    )
        if artifact is None and event.raw_type == "tool.execution_complete":
            accumulated_bytes = str(row["content"]).encode()
            combined = (
                completion_bytes if not accumulated_bytes else accumulated_bytes + completion_bytes
            )
            if len(combined) >= 64 * 1024:
                path = _tool_spill_path(
                    self._artifact_root,
                    event.sdk_session_id,
                    tool_call_id,
                )
                await asyncio.to_thread(_write_spill_bytes, path, combined)
                digest = await asyncio.to_thread(_spill_sha256, path)
                await connection.execute(
                    """
                    INSERT INTO tool_spill_artifacts(
                        session_id, tool_call_id, local_path, byte_size,
                        sha256, finalized, retention_until, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        event.sdk_session_id,
                        tool_call_id,
                        str(path),
                        len(combined),
                        digest,
                        now + _TOOL_SPILL_RETENTION_SECONDS,
                        now,
                    ),
                )
                artifact = {
                    "local_path": str(path),
                    "byte_size": len(combined),
                    "sha256": digest,
                    "finalized": 1,
                }
                completion_already_written = True
            else:
                await connection.execute(
                    """
                    UPDATE tool_output_streams
                    SET finalized = 1, updated_at = ?
                    WHERE session_id = ? AND tool_call_id = ?
                    """,
                    (now, event.sdk_session_id, tool_call_id),
                )
                return False, None
        if artifact is None:
            return False, None
        path = Path(str(artifact["local_path"]))
        byte_size = int(artifact["byte_size"])
        digest = None if artifact["sha256"] is None else str(artifact["sha256"])
        if event.raw_type == "tool.execution_complete":
            if completion_bytes and not completion_already_written:
                byte_size = await asyncio.to_thread(
                    _append_spill_bytes,
                    path,
                    byte_size,
                    completion_bytes,
                )
            digest = await asyncio.to_thread(_spill_sha256, path)
            await connection.execute(
                """
                UPDATE tool_output_streams
                SET content = '', spilled = 1, finalized = 1, updated_at = ?
                WHERE session_id = ? AND tool_call_id = ?
                """,
                (now, event.sdk_session_id, tool_call_id),
            )
            await connection.execute(
                """
                UPDATE tool_spill_artifacts
                SET byte_size = ?, sha256 = ?, finalized = 1,
                    retention_until = MAX(retention_until, ?),
                    updated_at = ?
                WHERE session_id = ? AND tool_call_id = ?
                """,
                (
                    byte_size,
                    digest,
                    now + _TOOL_SPILL_RETENTION_SECONDS,
                    now,
                    event.sdk_session_id,
                    tool_call_id,
                ),
            )
        if not bool(row["artifact_emitted"]):
            await connection.execute(
                """
                UPDATE tool_output_streams
                SET artifact_emitted = 1, spilled = 1, updated_at = ?
                WHERE session_id = ? AND tool_call_id = ?
                  AND artifact_emitted = 0
                """,
                (now, event.sdk_session_id, tool_call_id),
            )
        filename = f"tool-partial-{tool_call_id[:12]}.txt"
        finalized = event.raw_type == "tool.execution_complete"
        return True, {
            "type": "tool_output_artifact",
            "content": (
                f"**Tool partial spill** — `{byte_size:,}` bytes; "
                f"verbatim durable stream attached as `{filename}`."
            ),
            "finalized": finalized,
            "coalesce_key": f"tool-spill:{tool_call_id}",
            "stable_outbox_key": f"tool-spill:{tool_call_id}",
            "tool_source": source,
            "verbatim": verbatim,
            "attachments": [
                {
                    "filename": filename,
                    "media_type": "text/plain",
                    "path": str(path),
                    "byte_size": byte_size,
                    "sha256": digest,
                }
            ],
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
        if event.raw_type == "session.workspace_file_changed" and event.source == "sdk":
            path = data.get("path")
            if isinstance(path, str) and path.strip():
                operation = str(data.get("operation", "modified")).lower()
                if operation in {"delete", "deleted", "remove", "removed"}:
                    await connection.execute(
                        """
                        DELETE FROM trusted_local_artifacts
                        WHERE session_id = ? AND path = ?
                        """,
                        (event.sdk_session_id, path),
                    )
                    await connection.execute(
                        """
                        DELETE FROM trusted_local_artifact_snapshots
                        WHERE session_id = ? AND source_path = ?
                        """,
                        (event.sdk_session_id, path),
                    )
                else:
                    await connection.execute(
                        """
                        INSERT INTO trusted_local_artifacts(
                            session_id, path, observed_at
                        ) VALUES (?, ?, ?)
                        ON CONFLICT(session_id, path) DO UPDATE SET
                            observed_at = excluded.observed_at
                        """,
                        (event.sdk_session_id, path, event.received_at),
                    )
                    binding_cursor = await connection.execute(
                        """
                        SELECT cwd_snapshot FROM session_bindings
                        WHERE sdk_session_id = ?
                        """,
                        (event.sdk_session_id,),
                    )
                    binding = await binding_cursor.fetchone()
                    await binding_cursor.close()
                    snapshot = None
                    if binding is not None:
                        try:
                            snapshot = await asyncio.to_thread(
                                _snapshot_trusted_local_artifact,
                                self._artifact_root,
                                Path(str(binding["cwd_snapshot"])),
                                event.sdk_session_id,
                                path,
                            )
                        except OSError:
                            snapshot = None
                    if snapshot is None:
                        await connection.execute(
                            """
                            DELETE FROM trusted_local_artifact_snapshots
                            WHERE session_id = ? AND source_path = ?
                            """,
                            (event.sdk_session_id, path),
                        )
                    else:
                        snapshot_path, byte_size, digest = snapshot
                        await connection.execute(
                            """
                            INSERT INTO trusted_local_artifact_snapshots(
                                session_id, source_path, snapshot_path,
                                byte_size, sha256, observed_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(session_id, source_path) DO UPDATE SET
                                snapshot_path = excluded.snapshot_path,
                                byte_size = excluded.byte_size,
                                sha256 = excluded.sha256,
                                observed_at = excluded.observed_at
                            """,
                            (
                                event.sdk_session_id,
                                path,
                                str(snapshot_path),
                                byte_size,
                                digest,
                                event.received_at,
                            ),
                        )
        stream_content: str | None = None
        if (
            event.raw_type in {"assistant.message_delta", "assistant.message"}
            and event.message_id is not None
        ):
            stream_content = await self._accumulate_render_stream(
                connection,
                event,
                data=data,
                now=now,
            )
        if event.raw_type in RenderPlanner._USAGE_TYPES:
            await connection.execute(
                """
                INSERT INTO session_projection_snapshots(
                    session_id, kind, payload, observed_at
                ) VALUES (?, 'usage', ?, ?)
                ON CONFLICT(session_id, kind) DO UPDATE SET
                    payload = excluded.payload,
                    observed_at = excluded.observed_at
                """,
                (
                    event.sdk_session_id,
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    event.received_at,
                ),
            )
            await connection.execute(
                """
                INSERT INTO usage_samples(
                    session_id, turn_id, model, input_tokens, output_tokens,
                    cache_read_tokens, cache_write_tokens, nano_aiu,
                    premium_requests, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.sdk_session_id,
                    event.turn_id,
                    _find_nested_value(data, "model") or _find_nested_value(data, "currentModel"),
                    _integer_or_none(_find_nested_value(data, "inputTokens")),
                    _integer_or_none(_find_nested_value(data, "outputTokens")),
                    _integer_or_none(_find_nested_value(data, "cacheReadTokens")),
                    _integer_or_none(_find_nested_value(data, "cacheWriteTokens")),
                    _integer_or_none(
                        _find_nested_value(data, "nanoAiu")
                        or _find_nested_value(data, "totalNanoAiu")
                    ),
                    _float_or_none(
                        _find_nested_value(data, "premiumRequests")
                        or _find_nested_value(data, "totalPremiumRequestCost")
                    ),
                    event.received_at,
                ),
            )
        if event.raw_type.startswith("tool.execution_"):
            await self._append_tool_output(
                connection,
                event,
                data=data,
                now=now,
            )
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
        if event.raw_type == "copilotd.tasks.snapshot":
            await self._apply_task_snapshot(connection, event, data=data, now=now)
        elif _is_task_projection_event(event):
            projection_data = data
            if event.agent_id is not None and stream_content is not None:
                projection_data = {
                    **data,
                    "content": stream_content,
                    "deltaContent": stream_content,
                }
            await self._update_task_projection(
                connection,
                event,
                data=projection_data,
                now=now,
            )
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
            target_mode = data.get("target_mode") or interaction_target_mode(data.get("response"))
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
                    requested_mode, requested_delivery, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'local_queued', ?)
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
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'submitting', source_operation_id = ?
                WHERE submission_id = ? AND state = 'local_queued'
                """,
                (data.get("operation_id"), str(data["submission_id"])),
            )
            await connection.execute(
                """
                UPDATE message_queue SET state = 'submitting', updated_at = ?
                WHERE id = ? AND state = 'local_queued'
                """,
                (now, str(data["submission_id"])),
            )
        elif event.raw_type == "copilotd.submission.accepted":
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'submitted', accepted_message_id = ?
                WHERE submission_id = ? AND state IN ('local_queued', 'submitting')
                """,
                (str(data["message_id"]), str(data["submission_id"])),
            )
            await connection.execute(
                """
                UPDATE message_queue SET state = 'submitted', updated_at = ?
                WHERE id = ? AND state IN ('local_queued', 'submitting')
                """,
                (now, str(data["submission_id"])),
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
            await connection.execute(
                "UPDATE submissions SET state = ? WHERE submission_id = ?",
                (state, submission_id),
            )
            await connection.execute(
                """
                UPDATE message_queue SET state = ?, updated_at = ?
                WHERE id = ? AND state IN ('local_queued', 'submitting')
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
            cursor = await connection.execute(
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND accepted_message_id = ?
                  AND observed_user_event_id IS NULL
                """,
                (event.sdk_session_id, event.event_id),
            )
            candidates = await cursor.fetchall()
            await cursor.close()
            correlation_basis = "accepted_event_id"
            if not candidates:
                cursor = await connection.execute(
                    """
                    SELECT submission_id FROM submissions
                    WHERE sdk_session_id = ? AND state = 'submitted'
                      AND observed_user_event_id IS NULL
                    ORDER BY created_at
                    """,
                    (event.sdk_session_id,),
                )
                candidates = await cursor.fetchall()
                await cursor.close()
                correlation_basis = "single_unambiguous_candidate"
            if len(candidates) == 1:
                await connection.execute(
                    """
                    UPDATE submissions
                    SET state = 'observed_active', observed_user_event_id = ?,
                        correlation_basis = ?
                    WHERE submission_id = ?
                    """,
                    (
                        event.event_id,
                        correlation_basis,
                        candidates[0]["submission_id"],
                    ),
                )
        elif event.raw_type == "session.idle":
            cursor = await connection.execute(
                """
                SELECT submission_id, requested_mode FROM submissions
                WHERE sdk_session_id = ?
                  AND state = 'observed_active'
                ORDER BY created_at
                """,
                (event.sdk_session_id,),
            )
            candidates = await cursor.fetchall()
            await cursor.close()
            if len(candidates) == 1:
                candidate = candidates[0]
                await connection.execute(
                    """
                    UPDATE submissions
                    SET state = 'loop_idle', idle_at = ?
                    WHERE submission_id = ?
                    """,
                    (event.received_at, candidate["submission_id"]),
                )
                if candidate["requested_mode"] in {None, "interactive", "plan"}:
                    await connection.execute(
                        """
                        UPDATE liveness_leases
                        SET state = 'released', refreshed_at = ?, released_at = ?
                        WHERE sdk_session_id = ? AND kind = 'submission'
                          AND source_id = ? AND state = 'active'
                        """,
                        (
                            now,
                            now,
                            event.sdk_session_id,
                            candidate["submission_id"],
                        ),
                    )
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
        dependencies = _string_list(
            data.get("dependencies") or data.get("dependsOn") or data.get("blockedBy")
        )
        artifact_links = _artifact_links(
            data.get("artifacts") or data.get("artifactLinks") or data.get("links")
        )
        can_promote = bool(
            data.get("canPromoteToBackground") or data.get("can_promote_to_background")
        )
        detail_artifact = progress if progress is not None and len(progress) > 900 else None
        if detail_artifact is not None:
            progress = _bounded_text(progress, 500)
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
                kind, title, state, progress_summary, detail_artifact,
                dependencies_json, artifact_links_json, can_promote,
                first_seen_at, terminal_at, last_progress_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                detail_artifact = COALESCE(
                    excluded.detail_artifact,
                    task_card_projections.detail_artifact
                ),
                dependencies_json = CASE
                    WHEN excluded.dependencies_json = '[]'
                    THEN task_card_projections.dependencies_json
                    ELSE excluded.dependencies_json
                END,
                artifact_links_json = CASE
                    WHEN excluded.artifact_links_json = '[]'
                    THEN task_card_projections.artifact_links_json
                    ELSE excluded.artifact_links_json
                END,
                can_promote = MAX(task_card_projections.can_promote, excluded.can_promote),
                terminal_at = COALESCE(task_card_projections.terminal_at,
                                       excluded.terminal_at),
                last_progress_at = excluded.last_progress_at,
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
                detail_artifact,
                json.dumps(dependencies, ensure_ascii=False),
                json.dumps(artifact_links, ensure_ascii=False),
                int(can_promote),
                now,
                terminal_at,
                now,
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

    async def _apply_task_snapshot(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        raw_tasks = data.get("tasks", [])
        tasks = [task for task in raw_tasks if isinstance(task, dict)]
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
            dependencies = _string_list(
                task.get("dependencies") or task.get("dependsOn") or task.get("blockedBy")
            )
            artifact_links = _artifact_links(
                task.get("artifacts") or task.get("artifactLinks") or task.get("links")
            )
            can_promote = bool(
                task.get("canPromoteToBackground") or task.get("can_promote_to_background")
            )
            detail_artifact = progress if progress is not None and len(progress) > 900 else None
            if detail_artifact is not None:
                progress = _bounded_text(progress, 500)
            card_key = f"task:{task_id}"
            card_token = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:{event.sdk_session_id}:taskdeck:{card_key}",
                )
            )[:16]
            terminal_at = now if state in terminal_states else None
            await connection.execute(
                """
                INSERT INTO task_card_projections(
                    sdk_session_id, panel_id, card_token, card_key, task_id,
                    kind, title, state, progress_summary, detail_artifact,
                    dependencies_json, artifact_links_json, can_promote,
                    first_seen_at, terminal_at, last_progress_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sdk_session_id, panel_id, card_key) DO UPDATE SET
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
                    detail_artifact = COALESCE(
                        excluded.detail_artifact,
                        task_card_projections.detail_artifact
                    ),
                    dependencies_json = excluded.dependencies_json,
                    artifact_links_json = excluded.artifact_links_json,
                    can_promote = excluded.can_promote,
                    terminal_at = COALESCE(
                        task_card_projections.terminal_at,
                        excluded.terminal_at
                    ),
                    last_progress_at = excluded.last_progress_at,
                    revision = task_card_projections.revision + 1
                """,
                (
                    event.sdk_session_id,
                    panel_id,
                    card_token,
                    card_key,
                    task_id,
                    kind,
                    title,
                    state,
                    progress,
                    detail_artifact,
                    json.dumps(dependencies, ensure_ascii=False),
                    json.dumps(artifact_links, ensure_ascii=False),
                    int(can_promote),
                    now,
                    terminal_at,
                    now,
                ),
            )
            await connection.execute(
                """
                INSERT INTO background_observations(
                    sdk_session_id, runtime_generation, source_event_id,
                    task_id, task_type, observed_state, terminal_evidence,
                    last_progress_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
                    last_progress_at = excluded.last_progress_at
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    card_key,
                    task_id,
                    kind,
                    state,
                    "task_snapshot" if state in terminal_states else None,
                    now,
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


class EventReducerWorker:
    def __init__(
        self,
        *,
        inbox: ReducerInbox,
        reducer: JournalReducer,
        batch_size: int,
        fence_validator: FenceValidator | None = None,
    ) -> None:
        self._inbox = inbox
        self._reducer = reducer
        self._adapter = EventAdapter()
        self._batch_size = batch_size
        self._fence_validator = fence_validator
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("reducer worker already started")
        self._task = asyncio.create_task(self._run(), name=f"reducer:{self._inbox.sdk_session_id}")

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

            events = [self._adapter.adapt(envelope) for envelope in batch]
            try:
                await self._reducer.persist(events)
            except BaseException as error:
                for envelope in batch:
                    self._inbox.acknowledge(envelope, error=error)
                raise
            should_stop = any(event.raw_type == "copilotd.reducer.stop" for event in events)
            for envelope in batch:
                self._inbox.acknowledge(envelope)
            if should_stop and self._stopping:
                return


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


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
        title = _value(data, "agentDisplayName") or _value(data, "agentName") or "Copilot subagent"
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
        if success:
            display, source, verbatim = _tool_display_text(data, success=True)
            if display and len(display) < 8000:
                summary = (
                    f"Completed successfully · "
                    f"{'verbatim' if verbatim else 'runtime fallback'} `{source}` · "
                    f"{_bounded_text(display, 220)}"
                )
            else:
                summary = "Completed successfully."
        else:
            summary = _bounded_text(str(error), 300)
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


def _integer_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
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
        if any(key in data for key in ("encryptedContent", "opaque", "chainOfThought")):
            return fallback
        for key in ("summary", "intent"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return _bounded_text(value, 500)
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
    if event.raw_type not in {"tool.execution_progress", "tool.execution_complete"}:
        return None
    data = event.raw_payload.get("data", event.raw_payload)
    if not isinstance(data, dict):
        return None
    if event.raw_type == "tool.execution_progress":
        text = _tool_partial_text(data)
        source = "progress"
        success: bool | None = None
        threshold = 64 * 1024
        verbatim = True
    else:
        success = bool(data.get("success"))
        text, source, verbatim = _tool_display_text(data, success=success)
        threshold = 8000
    if text is None or len(text) < threshold:
        return None
    tool_call_id = str(data.get("toolCallId", "tool"))
    prefix = "tool-partial" if success is None else "tool-output" if success else "tool-error"
    filename = f"{prefix}-{tool_call_id[:12]}.txt"
    line_count = text.count("\n") + 1
    caveat = " Runtime fallback content may be truncated." if not verbatim else ""
    status = "partial spill" if success is None else "completed" if success else "failed"
    return {
        "type": "tool_output_artifact",
        "content": (
            f"**Tool {status}** — `{len(text):,}` characters / `{line_count:,}` lines; "
            f"{'verbatim' if verbatim else 'runtime fallback'} `{source}` attached as "
            f"`{filename}`.{caveat}"
        ),
        "finalized": True,
        "tool_source": source,
        "verbatim": verbatim,
        "attachments": [
            {
                "filename": filename,
                "media_type": "text/plain",
                "content": text,
            }
        ],
    }


def _tool_display_text(
    data: dict[str, Any],
    *,
    success: bool,
) -> tuple[str | None, str, bool]:
    if success:
        result = data.get("result")
        if isinstance(result, dict):
            detailed = result.get("detailedContent")
            if isinstance(detailed, str) and detailed:
                return detailed, "detailedContent", True
            if result.get("contents"):
                return _structured_tool_text(result["contents"]), "contents", True
            if result.get("structuredContent") is not None:
                return (
                    json.dumps(
                        result["structuredContent"],
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ),
                    "structuredContent",
                    True,
                )
            if isinstance(result.get("content"), str):
                return str(result["content"]), "content", False
        return None, "missing", False
    error = data.get("error")
    if isinstance(error, dict):
        if set(error) == {"message"}:
            return str(error["message"]), "error", True
        return (
            json.dumps(error, ensure_ascii=False, indent=2, sort_keys=True),
            "error",
            True,
        )
    if error is not None:
        return str(error), "error", True
    return None, "error", True


def _tool_partial_text(data: dict[str, Any]) -> str | None:
    for key in (
        "output",
        "outputDelta",
        "partialOutput",
        "progressOutput",
    ):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _diff_render_payload(event: AdaptedEvent) -> dict[str, Any] | None:
    if event.raw_type != "tool.execution_complete":
        return None
    data = event.raw_payload.get("data", event.raw_payload)
    if not isinstance(data, dict) or not bool(data.get("success")):
        return None
    result = data.get("result")
    if not isinstance(result, dict):
        result = {}
    patch: str | None = None
    source = "structured"
    for key in ("diff", "patch", "unifiedDiff"):
        value = result.get(key)
        if isinstance(value, str) and value:
            patch = value
            break
    structured = result.get("structuredContent")
    if patch is None and isinstance(structured, dict):
        for key in ("diff", "patch", "unifiedDiff"):
            value = structured.get(key)
            if isinstance(value, str) and value:
                patch = value
                break
    if patch is None:
        return None
    encoded_patch = patch.encode("utf-8", errors="replace")
    if len(encoded_patch) > _DIFF_OUTPUT_LIMIT:
        return {
            "type": "diff",
            "content": (
                "**Code changes**\nStructured diff exceeds the 8 MiB render safety "
                "limit; exact source remains in the durable event journal."
            ),
            "source": source,
            "oversized": True,
            "byte_count": len(encoded_patch),
            "finalized": True,
            "attachments": [],
        }
    patch = encoded_patch.decode("utf-8")
    tool_call_id = str(data.get("toolCallId", "diff"))
    if len(patch) <= 1600 and "```" not in patch:
        content = f"**Code changes** · `{source}`\n```diff\n{patch}\n```"
        attachments: list[dict[str, str]] = []
    else:
        filename = f"changes-{tool_call_id[:12]}.diff"
        content = f"**Code changes** · `{source}` attached as `{filename}`."
        attachments = [
            {
                "filename": filename,
                "media_type": "text/x-diff",
                "content": patch,
            }
        ]
    return {
        "type": "diff",
        "content": content,
        "source": source,
        "finalized": True,
        "attachments": attachments,
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


def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    minutes, remainder = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {remainder:02d}s"
    return f"{remainder}s"


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        values = value.values()
    elif isinstance(value, list | tuple | set):
        values = value
    else:
        values = (value,)
    result: list[str] = []
    for item in values:
        if isinstance(item, dict):
            text = next(
                (
                    str(item[key])
                    for key in ("id", "taskId", "name", "title")
                    if item.get(key) is not None
                ),
                "",
            )
        else:
            text = str(item)
        normalized = " ".join(text.split())
        if normalized and normalized not in result:
            result.append(normalized)
    return result[:25]


def _artifact_links(value: Any) -> list[str]:
    if value is None:
        return []
    values = value if isinstance(value, list | tuple) else (value,)
    result: list[str] = []
    for item in values:
        if isinstance(item, dict):
            label = next(
                (str(item[key]) for key in ("name", "title", "path", "url") if item.get(key)),
                "",
            )
            url = item.get("url")
            text = f"[{label}]({url})" if label and url else label
        else:
            text = str(item)
        if text and text not in result:
            result.append(text)
    return result[:25]


def _bounded_text(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    return normalized if len(normalized) <= limit else normalized[: limit - 1] + "…"


def _model_config_matches(
    requested: dict[str, Any],
    observed: dict[str, Any],
) -> bool:
    return all(value is None or observed.get(key) == value for key, value in requested.items())


def _tool_spill_path(root: Path, session_id: str, tool_call_id: str) -> Path:
    session_token = uuid.uuid5(uuid.NAMESPACE_URL, f"session:{session_id}").hex[:16]
    tool_token = uuid.uuid5(uuid.NAMESPACE_URL, f"tool:{tool_call_id}").hex[:16]
    return root / session_token / "artifacts" / f"tool-{tool_token}.txt"


def _snapshot_trusted_local_artifact(
    root: Path,
    cwd: Path,
    session_id: str,
    source_path: str,
) -> tuple[Path, int, str] | None:
    resolved_cwd = cwd.resolve(strict=False)
    candidate = Path(source_path)
    source = (
        candidate.resolve(strict=False)
        if candidate.is_absolute()
        else (resolved_cwd / candidate).resolve(strict=False)
    )
    try:
        source.relative_to(resolved_cwd)
    except ValueError:
        return None
    if source.suffix.lower() not in _TRUSTED_LOCAL_IMAGE_SUFFIXES or not source.is_file():
        return None

    session_token = uuid.uuid5(uuid.NAMESPACE_URL, f"session:{session_id}").hex[:16]
    source_token = uuid.uuid5(uuid.NAMESPACE_URL, f"local-image:{source_path}").hex[:16]
    snapshot_root = root / session_token / "artifacts" / "local-images"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    temporary = snapshot_root / f".{source_token}.{uuid.uuid4().hex}.tmp"
    digest = hashlib.sha256()
    byte_size = 0
    try:
        with source.open("rb") as input_file, temporary.open("xb") as output_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                output_file.write(chunk)
                digest.update(chunk)
                byte_size += len(chunk)
            output_file.flush()
            os.fsync(output_file.fileno())
        digest_text = digest.hexdigest()
        suffix = source.suffix.lower()
        snapshot_path = snapshot_root / f"{source_token}-{digest_text}{suffix}"
        if snapshot_path.exists():
            temporary.unlink(missing_ok=True)
        else:
            os.replace(temporary, snapshot_path)
        return snapshot_path, byte_size, digest_text
    finally:
        temporary.unlink(missing_ok=True)


def _write_spill_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _append_spill_bytes(path: Path, expected_size: int, content: bytes) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "r+b" if path.exists() else "w+b"
    with path.open(mode) as file:
        file.seek(0, os.SEEK_END)
        current_size = file.tell()
        if current_size < expected_size:
            raise RuntimeError(
                f"tool spill artifact is truncated: {current_size} < {expected_size}"
            )
        if current_size > expected_size:
            file.truncate(expected_size)
        file.seek(expected_size)
        file.write(content)
        file.flush()
        os.fsync(file.fileno())
    return expected_size + len(content)


def _spill_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _spill_matches_bytes(path: Path, byte_size: int, expected: bytes) -> bool:
    if byte_size != len(expected):
        return False
    expected_digest = hashlib.sha256(expected).digest()
    actual_digest = hashlib.sha256()
    consumed = 0
    with path.open("rb") as file:
        while consumed < byte_size:
            chunk = file.read(min(1024 * 1024, byte_size - consumed))
            if not chunk:
                return False
            actual_digest.update(chunk)
            consumed += len(chunk)
        if file.read(1):
            return False
    return actual_digest.digest() == expected_digest
