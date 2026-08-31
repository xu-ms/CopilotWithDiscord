from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
import stat
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, BinaryIO, ClassVar, cast

from aiosqlite import Connection, Row

from copilotd.core.event_adapter import EventAdapter, InvalidSdkEvent
from copilotd.core.inbox import ReducerInbox
from copilotd.core.interactions import interaction_target_mode
from copilotd.core.models import AdaptedEvent, InboxEnvelope, RenderIntent
from copilotd.core.native import stable_hash, timestamp_seconds
from copilotd.core.protocol import apply_protocol_event
from copilotd.core.task_registry import TaskRegistry
from copilotd.storage.database import Database

FenceValidator = Callable[[int, int], Awaitable[bool]]
_DIFF_OUTPUT_LIMIT = 8 * 1024 * 1024
_DIFF_HUNK_PATTERN = re.compile(r"^@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@")
_TOOL_SPILL_RETENTION_SECONDS = 7 * 24 * 60 * 60
REACTION_EMOJI = {
    "accepted": "👀",
    "reasoning": "🧠",
    "action": "🛠️",
    "unresolved": "❓",
    "succeeded": "✅",
    "failed": "❌",
}
_REACTION_TERMINAL_STATES = {"succeeded", "failed"}
_SUBMISSION_SUCCESS_STATES = {"semantic_complete"}
_SUBMISSION_FAILURE_STATES = {
    "cancelled",
    "observed_aborted",
    "outcome_unknown",
    "rejected",
    "semantic_blocked",
    "submitted_unknown",
}
_TURN_PROGRESS_TYPES = {
    "abort",
    "assistant.intent",
    "assistant.message_delta",
    "assistant.reasoning",
    "assistant.reasoning_delta",
    "assistant.turn_end",
    "assistant.turn_retry",
    "assistant.turn_start",
    "copilotd.interaction.expired",
    "copilotd.interaction.resolved",
    "copilotd.submission.accepted",
    "copilotd.submission.queued",
    "copilotd.tasks.snapshot",
    "model.call_failure",
    "session.error",
    "session.idle",
    "session.info",
    "session.shutdown",
    "session.task_complete",
    "session.warning",
    "session.workspace_file_changed",
    "subagent.completed",
    "subagent.failed",
    "subagent.started",
    "tool.execution_complete",
    "tool.execution_progress",
    "tool.execution_start",
    "user.message",
}
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
        "assistant.reasoning_delta",
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
        if event.raw_type == "assistant.streaming_delta":
            return []
        turn_key = (
            None
            if payload_override is None or payload_override.get("turn_render_key") is None
            else str(payload_override["turn_render_key"])
        )
        if turn_key is not None:
            turn_payload = {
                **payload_override,
                "stable_outbox_key": turn_key,
            }
            finalized = bool(turn_payload.get("finalized"))
            lane = "assistant_final" if finalized else "assistant_stream"
            idempotency_key = f"turn-render:{event.sdk_session_id}:{turn_key}"
            return [
                RenderIntent(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key)),
                    session_id=event.sdk_session_id,
                    logical_seq=event.inbox_seq,
                    lane=lane,
                    coalesce_key=turn_key,
                    idempotency_key=idempotency_key,
                    payload=turn_payload,
                    finalized=finalized,
                )
            ]
        if event.agent_id is not None and event.raw_type in (
            self._STREAM_TYPES | {"assistant.message"}
        ):
            return []
        if event.raw_type in self._TASK_TYPES or event.raw_type in self._TASK_VIEW_TYPES:
            return []
        if event.raw_type in self._FOOTER_TYPES:
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
        elif event.raw_type in self._USAGE_TYPES:
            # Usage samples still feed the durable projection and the turn footer.
            # Rendering each sample separately duplicates the final summary and
            # interrupts the response text with a transient status message.
            return []
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
                "assistant.reasoning_delta",
                "session.compaction_start",
            }
            coalesce_key = (
                "intent"
                if event.raw_type
                in {"assistant.intent", "assistant.reasoning_delta", "assistant.reasoning"}
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
        return [
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


class JournalReducer:
    """Atomically persists event journal rows and their render intents."""

    def __init__(
        self,
        database: Database,
        planner: RenderPlanner | None = None,
        *,
        artifact_root: Path | None = None,
        managed_session_state_root: Path | None = None,
        require_binding_fence: bool = False,
    ) -> None:
        self._database = database
        self._planner = planner or RenderPlanner()
        self._artifact_root = (
            database.path.parent / "sessions" if artifact_root is None else artifact_root
        )
        self._managed_session_state_root = managed_session_state_root
        self._require_binding_fence = require_binding_fence

    async def persist(self, events: list[AdaptedEvent]) -> int:
        inserted = 0
        now = time.time()
        async with self._database.transaction() as connection:
            for event in events:
                if self._require_binding_fence:
                    cursor = await connection.execute(
                        """
                        SELECT 1 FROM session_bindings
                        WHERE sdk_session_id = ? AND runtime_generation = ?
                          AND owner_fence_token = ?
                        """,
                        (
                            event.sdk_session_id,
                            event.generation,
                            event.fence_token,
                        ),
                    )
                    owns_generation = await cursor.fetchone()
                    await cursor.close()
                    if owns_generation is None:
                        continue
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
                superseded_shutdown = await self._is_superseded_shutdown(
                    connection,
                    event,
                )
                render_payload: dict[str, Any] | None = None
                suppress_default_artifact = False
                artifact_override: dict[str, Any] | None = None
                if not superseded_shutdown:
                    await self._apply_domain_state(connection, event, now=now)
                    await self._apply_reaction_state(connection, event, now=now)
                    render_payload = await self._materialize_render_payload(
                        connection,
                        event,
                        now=now,
                    )
                    (
                        suppress_default_artifact,
                        artifact_override,
                    ) = await self._cumulative_tool_artifact(
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
                if superseded_shutdown:
                    continue
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
                        cursor = await connection.execute(
                            """
                            SELECT payload FROM render_outbox
                            WHERE idempotency_key = ?
                            """,
                            (intent.idempotency_key,),
                        )
                        existing_outbox = await cursor.fetchone()
                        await cursor.close()
                        if existing_outbox is not None:
                            existing_payload = json.loads(str(existing_outbox["payload"]))
                            if (
                                bool(existing_payload.get("finalized"))
                                and not bool(intent.payload.get("finalized"))
                            ) or existing_payload == intent.payload:
                                continue
                        await connection.execute(
                            """
                            INSERT INTO render_outbox(
                                id, session_id, logical_seq, lane, coalesce_key,
                                idempotency_key, payload, state, attempts,
                                next_attempt_at, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                            ON CONFLICT(idempotency_key) DO UPDATE SET
                                logical_seq = excluded.logical_seq,
                                lane = excluded.lane,
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

    async def _is_superseded_shutdown(
        self,
        connection: Any,
        event: AdaptedEvent,
    ) -> bool:
        if event.raw_type != "session.shutdown" or event.event_id is None:
            return False
        cursor = await connection.execute(
            """
            SELECT 1
            FROM event_journal
            WHERE sdk_session_id = ? AND generation = ?
              AND raw_type = 'session.resume' AND parent_id = ?
            LIMIT 1
            """,
            (
                event.sdk_session_id,
                event.generation,
                event.event_id,
            ),
        )
        resumed_after_shutdown = await cursor.fetchone()
        await cursor.close()
        return resumed_after_shutdown is not None

    async def _apply_reaction_state(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> None:
        data = event.raw_payload.get("data", event.raw_payload)
        values = data if isinstance(data, dict) else {}
        if event.raw_type == "copilotd.submission.queued":
            await self._set_submission_reaction(
                connection,
                event,
                str(values["submission_id"]),
                "accepted",
                now=now,
            )

        cursor = await connection.execute(
            """
            SELECT r.submission_id, s.state
            FROM submission_reactions r
            JOIN submissions s ON s.submission_id = r.submission_id
            WHERE r.sdk_session_id = ? AND r.terminal = 0
              AND s.state IN (
                'cancelled', 'observed_aborted', 'outcome_unknown', 'rejected',
                'semantic_blocked', 'semantic_complete', 'submitted_unknown'
              )
            """,
            (event.sdk_session_id,),
        )
        terminal_rows = await cursor.fetchall()
        await cursor.close()
        for row in terminal_rows:
            await self._set_submission_reaction(
                connection,
                event,
                str(row["submission_id"]),
                ("succeeded" if str(row["state"]) in _SUBMISSION_SUCCESS_STATES else "failed"),
                now=now,
            )

        if event.raw_type == "abort":
            state = "failed"
        elif event.raw_type == "copilotd.interaction.requested":
            state = "unresolved"
        elif event.raw_type in {
            "copilotd.interaction.resolved",
            "copilotd.interaction.expired",
        }:
            state = "resume"
        elif event.raw_type in {
            "subagent.started",
            "subagent.completed",
            "subagent.failed",
            "tool.execution_start",
            "tool.execution_progress",
            "tool.execution_complete",
        }:
            state = "action"
        elif event.raw_type in {
            "assistant.intent",
            "assistant.message_delta",
            "assistant.reasoning",
            "assistant.reasoning_delta",
            "assistant.turn_retry",
            "assistant.turn_start",
            "user.message",
        }:
            state = "reasoning"
        else:
            return

        submission_id: str | None = None
        if event.raw_type == "user.message" and event.event_id is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND observed_user_event_id = ?
                """,
                (event.sdk_session_id, event.event_id),
            )
            if row is not None:
                submission_id = str(row["submission_id"])
        if submission_id is None and event.turn_id is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id FROM model_turns
                WHERE sdk_session_id = ? AND sdk_turn_id = ?
                """,
                (event.sdk_session_id, event.turn_id),
            )
            if row is not None and row["submission_id"] is not None:
                submission_id = str(row["submission_id"])
        if submission_id is None:
            row = await _fetchone_row(
                connection,
                """
                SELECT s.submission_id
                FROM submissions s
                JOIN submission_reactions r
                  ON r.submission_id = s.submission_id
                WHERE s.sdk_session_id = ?
                  AND s.state IN (
                    'observed_active', 'loop_idle', 'continuation_expected',
                    'submitted'
                  )
                  AND r.terminal = 0
                ORDER BY
                  CASE WHEN s.observed_at IS NULL THEN 1 ELSE 0 END,
                  s.observed_at DESC, s.created_at
                LIMIT 1
                """,
                (event.sdk_session_id,),
            )
            if row is not None:
                submission_id = str(row["submission_id"])
        if submission_id is None:
            return
        if state == "resume":
            row = await _fetchone_row(
                connection,
                "SELECT resume_state FROM submission_reactions WHERE submission_id = ?",
                (submission_id,),
            )
            state = (
                "reasoning"
                if row is None or row["resume_state"] not in {"reasoning", "action"}
                else str(row["resume_state"])
            )
        await self._set_submission_reaction(
            connection,
            event,
            submission_id,
            state,
            now=now,
        )

    async def _set_submission_reaction(
        self,
        connection: Any,
        event: AdaptedEvent,
        submission_id: str,
        state: str,
        *,
        now: float,
    ) -> None:
        if state not in REACTION_EMOJI:
            raise ValueError(f"unknown submission reaction state: {state}")
        submission = await _fetchone_row(
            connection,
            """
            SELECT sdk_session_id, discord_source_channel_id,
                   discord_source_message_id
            FROM submissions
            WHERE submission_id = ? AND sdk_session_id = ?
            """,
            (submission_id, event.sdk_session_id),
        )
        if (
            submission is None
            or submission["discord_source_channel_id"] is None
            or submission["discord_source_message_id"] is None
        ):
            return
        existing = await _fetchone_row(
            connection,
            """
            SELECT desired_state, resume_state, revision, terminal
            FROM submission_reactions WHERE submission_id = ?
            """,
            (submission_id,),
        )
        if existing is not None:
            existing_state = str(existing["desired_state"])
            if bool(existing["terminal"]):
                return
            if existing_state == state:
                return
            revision = int(existing["revision"]) + 1
            resume_state = (
                existing_state
                if state == "unresolved" and existing_state in {"reasoning", "action"}
                else existing["resume_state"]
            )
        else:
            revision = 1
            resume_state = None
        terminal = state in _REACTION_TERMINAL_STATES
        source_channel_id = str(submission["discord_source_channel_id"])
        source_message_id = str(submission["discord_source_message_id"])
        await connection.execute(
            """
            INSERT INTO submission_reactions(
                submission_id, sdk_session_id, source_channel_id,
                source_message_id, desired_state, resume_state, revision,
                runtime_generation, owner_fence_token, terminal,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(submission_id) DO UPDATE SET
                desired_state = excluded.desired_state,
                resume_state = excluded.resume_state,
                revision = excluded.revision,
                runtime_generation = excluded.runtime_generation,
                owner_fence_token = excluded.owner_fence_token,
                terminal = MAX(submission_reactions.terminal, excluded.terminal),
                last_error = NULL,
                updated_at = excluded.updated_at
            """,
            (
                submission_id,
                event.sdk_session_id,
                source_channel_id,
                source_message_id,
                state,
                resume_state,
                revision,
                event.generation,
                event.fence_token,
                int(terminal),
                now,
                now,
            ),
        )
        payload = json.dumps(
            {
                "type": "reaction_state",
                "submission_id": submission_id,
                "source_channel_id": source_channel_id,
                "source_message_id": source_message_id,
                "state": state,
                "emoji": REACTION_EMOJI[state],
                "reaction_revision": revision,
                "generation": event.generation,
                "fence_token": event.fence_token,
                "finalized": terminal,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        outbox_key = f"reaction:{submission_id}"
        outbox_id = str(uuid.uuid5(uuid.NAMESPACE_URL, outbox_key))
        await connection.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, 'reaction', ?, ?, ?, 'pending', 0, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                logical_seq = excluded.logical_seq,
                payload = excluded.payload,
                payload_revision = render_outbox.payload_revision + 1,
                state = CASE
                    WHEN render_outbox.state = 'sending' THEN 'sending'
                    ELSE 'pending'
                END,
                next_attempt_at = MIN(render_outbox.next_attempt_at,
                                      excluded.next_attempt_at),
                updated_at = excluded.updated_at
            """,
            (
                outbox_id,
                event.sdk_session_id,
                event.inbox_seq,
                outbox_key,
                outbox_key,
                payload,
                now,
                now,
                now,
            ),
        )

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
            if self._require_binding_fence:
                cursor = await connection.execute(
                    """
                    SELECT 1 FROM session_bindings
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        envelope.sdk_session_id,
                        envelope.generation,
                        envelope.fence_token,
                    ),
                )
                owns_generation = await cursor.fetchone()
                await cursor.close()
                if owns_generation is None:
                    return
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

    async def _turn_render_context(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> dict[str, Any] | None:
        data = event.raw_payload.get("data", event.raw_payload)
        values = data if isinstance(data, dict) else {}
        submission_id: str | None = None
        segment_index: int | None = None

        direct_submission = values.get("submission_id")
        if direct_submission is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ? AND submission_id = ?
                """,
                (event.sdk_session_id, str(direct_submission)),
            )
            if row is not None:
                submission_id = str(row["submission_id"])

        if submission_id is None and event.raw_type == "user.message" and event.event_id:
            row = await _fetchone_row(
                connection,
                """
                SELECT s.submission_id, g.segment_index
                FROM submissions s
                LEFT JOIN submission_segments g
                  ON g.submission_id = s.submission_id
                 AND g.user_event_id = ?
                WHERE s.sdk_session_id = ? AND s.observed_user_event_id = ?
                """,
                (event.event_id, event.sdk_session_id, event.event_id),
            )
            if row is not None:
                submission_id = str(row["submission_id"])
                segment_index = None if row["segment_index"] is None else int(row["segment_index"])

        if submission_id is None and event.turn_id is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id, segment_index
                FROM model_turns
                WHERE sdk_session_id = ? AND sdk_turn_id = ?
                  AND submission_id IS NOT NULL
                """,
                (event.sdk_session_id, event.turn_id),
            )
            if row is not None:
                submission_id = str(row["submission_id"])
                segment_index = None if row["segment_index"] is None else int(row["segment_index"])

        if submission_id is None and event.task_id is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id FROM submission_task_links
                WHERE sdk_session_id = ? AND task_id = ?
                  AND submission_id IS NOT NULL
                ORDER BY linked_at DESC LIMIT 1
                """,
                (event.sdk_session_id, event.task_id),
            )
            if row is not None:
                submission_id = str(row["submission_id"])

        if submission_id is None and event.interaction_id is not None:
            row = await _fetchone_row(
                connection,
                """
                SELECT submission_id, segment_index FROM model_turns
                WHERE sdk_session_id = ? AND interaction_id = ?
                  AND submission_id IS NOT NULL
                ORDER BY started_at DESC LIMIT 1
                """,
                (event.sdk_session_id, event.interaction_id),
            )
            if row is not None:
                submission_id = str(row["submission_id"])
                segment_index = None if row["segment_index"] is None else int(row["segment_index"])

        if submission_id is None:
            rows = await _fetchall_rows(
                connection,
                """
                SELECT s.submission_id
                FROM submissions s
                JOIN turn_render_state r
                  ON r.sdk_session_id = s.sdk_session_id
                 AND r.submission_id = s.submission_id
                WHERE s.sdk_session_id = ?
                  AND r.state IN ('running', 'answer_ready')
                  AND s.state IN (
                    'local_queued', 'submitting', 'submitted',
                    'observed_active', 'loop_idle', 'continuation_expected',
                    'semantic_complete', 'semantic_blocked', 'observed_aborted',
                    'outcome_unknown', 'rejected', 'submitted_unknown', 'cancelled'
                  )
                ORDER BY
                  CASE WHEN s.observed_at IS NULL THEN 1 ELSE 0 END,
                  s.observed_at DESC, r.updated_at DESC, s.created_at
                LIMIT 1
                """,
                (event.sdk_session_id,),
            )
            if rows:
                submission_id = str(rows[0]["submission_id"])

        if submission_id is None:
            rows = await _fetchall_rows(
                connection,
                """
                SELECT submission_id FROM submissions
                WHERE sdk_session_id = ?
                  AND state IN (
                    'submitted', 'observed_active', 'loop_idle',
                    'continuation_expected'
                  )
                ORDER BY
                  CASE WHEN observed_at IS NULL THEN 1 ELSE 0 END,
                  observed_at DESC, created_at
                LIMIT 2
                """,
                (event.sdk_session_id,),
            )
            if len(rows) == 1:
                submission_id = str(rows[0]["submission_id"])

        if submission_id is None:
            return None
        if segment_index is None:
            row = await _fetchone_row(
                connection,
                """
                SELECT MAX(segment_index) AS segment_index
                FROM submission_segments WHERE submission_id = ?
                """,
                (submission_id,),
            )
            if row is not None and row["segment_index"] is not None:
                segment_index = int(row["segment_index"])
        turn_key = (
            f"turn:{submission_id}"
            if segment_index in {None, 1}
            else f"turn:{submission_id}:segment:{segment_index}"
        )
        submission = await _fetchone_row(
            connection,
            """
            SELECT state FROM submissions
            WHERE sdk_session_id = ? AND submission_id = ?
            """,
            (event.sdk_session_id, submission_id),
        )
        if submission is None:
            return None
        await connection.execute(
            """
            INSERT INTO turn_render_state(
                sdk_session_id, turn_key, submission_id, segment_index,
                state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?)
            ON CONFLICT(sdk_session_id, turn_key) DO UPDATE SET
                submission_id = excluded.submission_id,
                segment_index = COALESCE(
                    excluded.segment_index,
                    turn_render_state.segment_index
                ),
                runtime_generation = excluded.runtime_generation,
                owner_fence_token = excluded.owner_fence_token,
                updated_at = excluded.updated_at
            """,
            (
                event.sdk_session_id,
                turn_key,
                submission_id,
                segment_index,
                event.generation,
                event.fence_token,
                now,
                now,
            ),
        )
        render = await _fetchone_row(
            connection,
            """
            SELECT state, answer_payload
            FROM turn_render_state
            WHERE sdk_session_id = ? AND turn_key = ?
            """,
            (event.sdk_session_id, turn_key),
        )
        assert render is not None
        return {
            "turn_key": turn_key,
            "submission_id": submission_id,
            "segment_index": segment_index,
            "submission_state": str(submission["state"]),
            "render_state": str(render["state"]),
            "answer_payload": render["answer_payload"],
        }

    async def _turn_progress_payload(
        self,
        connection: Any,
        event: AdaptedEvent,
        context: dict[str, Any],
        *,
        now: float,
    ) -> dict[str, Any] | None:
        turn_key = str(context["turn_key"])
        submission_state = str(context["submission_state"])
        if submission_state in _SUBMISSION_FAILURE_STATES or event.raw_type in {
            "abort",
            "session.error",
            "session.shutdown",
        }:
            await connection.execute(
                """
                UPDATE turn_render_state
                SET state = 'failed', updated_at = ?
                WHERE sdk_session_id = ? AND turn_key = ?
                """,
                (now, event.sdk_session_id, turn_key),
            )
            return {
                "type": "turn_error",
                "content": (
                    "**Copilot could not complete this request.**\n"
                    "Please try again or refine the request."
                ),
                "status": {
                    "title": "Copilot request failed",
                    "detail": "The request did not complete successfully.",
                    "event_type": "turn.failed",
                },
                "turn_render_key": turn_key,
                "submission_id": context["submission_id"],
                "finalized": True,
            }
        if event.raw_type == "assistant.message":
            return None
        if submission_state in _SUBMISSION_SUCCESS_STATES:
            answer_payload = context.get("answer_payload")
            if answer_payload is None:
                payload: dict[str, Any] = {
                    "type": "turn_complete",
                    "content": "Copilot completed the request.",
                }
            else:
                payload = json.loads(str(answer_payload))
            payload.update(
                turn_render_key=turn_key,
                submission_id=context["submission_id"],
                finalized=True,
            )
            await connection.execute(
                """
                UPDATE turn_render_state
                SET state = 'final', updated_at = ?
                WHERE sdk_session_id = ? AND turn_key = ?
                """,
                (now, event.sdk_session_id, turn_key),
            )
            return payload
        if event.raw_type == "copilotd.interaction.requested":
            return None
        if event.raw_type not in _TURN_PROGRESS_TYPES:
            return {"suppress": True}
        if context["render_state"] in {"final", "failed"}:
            return {"suppress": True}
        return {
            "type": "turn_progress",
            "content": "Copilot is working…",
            "status": {
                "title": "Copilot is working",
                "detail": "Processing your request.",
                "event_type": "turn.running",
            },
            "turn_render_key": turn_key,
            "submission_id": context["submission_id"],
            "finalized": False,
        }

    async def _materialize_render_payload(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        now: float,
    ) -> dict[str, Any] | None:
        if event.agent_id is not None and event.raw_type in {
            "assistant.message",
            "assistant.message_delta",
        }:
            return {"suppress": True}
        turn_context = await self._turn_render_context(
            connection,
            event,
            now=now,
        )
        if turn_context is not None:
            progress = await self._turn_progress_payload(
                connection,
                event,
                turn_context,
                now=now,
            )
            if progress is not None:
                return progress
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
            payload = {
                "type": "interaction",
                "content": content,
                "finalized": state != "pending",
                "interaction": interaction,
            }
            if turn_context is not None:
                payload.update(
                    turn_render_key=turn_context["turn_key"],
                    submission_id=turn_context["submission_id"],
                    finalized=False,
                )
            return payload
        if event.raw_type in RenderPlanner._USAGE_TYPES:
            data = event.raw_payload.get("data", event.raw_payload)
            values = data if isinstance(data, dict) else {}
            metrics = _usage_summary_values(values)
            lines = _usage_summary_lines(metrics)
            return {
                "type": event.raw_type,
                "content": "**Copilot usage**\n" + "\n".join(lines),
                "usage": metrics,
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
                WHERE sdk_session_id = ?
                  AND kind = 'observed_background' AND state = 'active'
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
            status = {
                "title": title,
                "detail": _bounded_text(detail, 1600),
                "event_type": event.raw_type,
            }
            if event.raw_type == "session.task_complete":
                outcome = values.get("outcome")
                if outcome is None and values.get("success") is True:
                    outcome = "completed"
                if outcome is not None:
                    status["outcome"] = str(outcome)
            return {
                "type": event.raw_type,
                "content": f"**{title}**\n{_bounded_text(detail, 1600)}",
                "status": status,
                "finalized": event.raw_type
                not in {
                    "assistant.intent",
                    "assistant.reasoning_delta",
                    "session.compaction_start",
                },
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
                ended_at = card["terminal_at"] or now
                card["elapsed"] = _format_elapsed(
                    max(0.0, float(ended_at) - float(card["first_seen_at"]))
                )
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
            attachments: list[dict[str, Any]] = []
            if expanded:
                artifact = selected_card["detail_artifact"]
                if artifact:
                    filename = f"task-{selected_card['card_token']}-detail.md"
                    attachments.append(
                        {
                            "filename": filename,
                            "media_type": "text/markdown",
                            "content": str(artifact),
                        }
                    )
            if page_count > 1:
                lines.append(f"Page {page + 1}/{page_count}")
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
            LEFT JOIN trusted_local_artifact_snapshots AS snapshots
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
            snapshot_rows = [row for row in trusted_rows if row["snapshot_path"] is not None]
            payload["trusted_local_images"] = True
            payload["trusted_local_image_paths"] = [str(row["path"]) for row in trusted_rows]
            payload["trusted_local_image_artifacts"] = [
                {
                    "source_path": str(row["path"]),
                    "snapshot_path": str(row["snapshot_path"]),
                    "byte_size": int(row["byte_size"]),
                    "sha256": str(row["sha256"]),
                }
                for row in snapshot_rows
            ]
        if turn_context is not None:
            answer_payload = {**payload, "finalized": True}
            terminal = str(turn_context["submission_state"]) in _SUBMISSION_SUCCESS_STATES
            await connection.execute(
                """
                UPDATE turn_render_state
                SET state = ?, answer_payload = ?, updated_at = ?
                WHERE sdk_session_id = ? AND turn_key = ?
                """,
                (
                    "final" if terminal else "answer_ready",
                    json.dumps(answer_payload, ensure_ascii=False, sort_keys=True),
                    now,
                    event.sdk_session_id,
                    turn_context["turn_key"],
                ),
            )
            payload.update(
                turn_render_key=turn_context["turn_key"],
                submission_id=turn_context["submission_id"],
                finalized=terminal,
            )
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
            "status": (
                ("completed" if bool(data.get("success")) else "failed")
                if finalized
                else "partial spill"
            ),
            "byte_count": byte_size,
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
                                self._managed_session_state_root,
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
        await apply_protocol_event(connection, event, data, now=now)
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
                raise RuntimeError(f"operation state changed concurrently: {data['operation_id']}")
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
        elif event.raw_type.startswith("copilotd.native_command."):
            await self._apply_native_command_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type.startswith("copilotd.ephemeral_query."):
            await self._apply_ephemeral_query_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type.startswith("copilotd.compaction."):
            await self._apply_compaction_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type.startswith("copilotd.fleet."):
            await self._apply_fleet_event(connection, event, data=data, now=now)
        elif event.raw_type.startswith("copilotd.task_action."):
            await self._apply_task_action_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type.startswith("copilotd.agent_transition."):
            await self._apply_agent_transition_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type.startswith("copilotd.remote_transition."):
            await self._apply_remote_transition_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type == "copilotd.remote.observed":
            abandoned_transition_id = data.get("abandoned_transition_id")
            if abandoned_transition_id is not None:
                await connection.execute(
                    """
                    UPDATE runtime_remote_transitions
                    SET state = 'unknown',
                        snapshot_json = ?,
                        settled_at = ?
                    WHERE transition_id = ? AND sdk_session_id = ?
                      AND state IN ('pending', 'unknown')
                    """,
                    (
                        _json_or_none(
                            {
                                "basis": "forced_off_during_attach",
                                "original_outcome": "unknown",
                            }
                        ),
                        float(data.get("observed_at", now)),
                        str(abandoned_transition_id),
                        event.sdk_session_id,
                    ),
                )
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_remote_mode = ?, remote_url = ?,
                    remote_steerable = ?, remote_observed_at = ?,
                    remote_snapshot_json = ?,
                    remote_observed_sdk_timestamp = MAX(
                        COALESCE(remote_observed_sdk_timestamp, -1), ?
                    ),
                    pending_remote_target = CASE
                        WHEN ? THEN NULL ELSE pending_remote_target
                    END,
                    pending_remote_transition_id = CASE
                        WHEN ? THEN NULL ELSE pending_remote_transition_id
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(data["mode"]),
                    data.get("url"),
                    (None if data.get("steerable") is None else int(bool(data["steerable"]))),
                    float(data.get("observed_at", now)),
                    _json_or_none(data.get("snapshot", {})),
                    float(data.get("observed_at", now)),
                    int(bool(data.get("clear_pending"))),
                    int(bool(data.get("clear_pending"))),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type.startswith("copilotd.schedule_action."):
            await self._apply_schedule_action_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type in {
            "session.schedule_created",
            "session.schedule_cancelled",
            "session.schedule_rearmed",
        }:
            await self._apply_runtime_schedule_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type == "session.remote_steerable_changed":
            await self._apply_remote_event(connection, event, data=data, now=now)
        elif event.raw_type in {"subagent.selected", "subagent.deselected"}:
            await self._apply_selected_agent_event(
                connection,
                event,
                data=data,
                now=now,
            )
        elif event.raw_type in {
            "session.compaction_start",
            "session.compaction_complete",
        }:
            await self._apply_compaction_sdk_event(
                connection,
                event,
                data=data,
                now=now,
            )
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
        elif event.raw_type == "copilotd.hook.audit":
            await connection.execute(
                """
                INSERT INTO hook_audit_events(
                    audit_id, sdk_session_id, runtime_generation,
                    owner_fence_token, hook_name, hook_invocation_id,
                    phase, tool_name, tool_call_id, correlation_id,
                    classification, payload_hash, payload_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(audit_id) DO NOTHING
                """,
                (
                    str(data["audit_id"]),
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    str(data["hook_name"]),
                    str(data["hook_invocation_id"]),
                    str(data["phase"]),
                    data.get("tool_name"),
                    data.get("tool_call_id"),
                    data.get("correlation_id"),
                    data.get("classification"),
                    str(data["payload_hash"]),
                    json.dumps(data.get("payload", {}), sort_keys=True),
                    float(data.get("observed_at", now)),
                ),
            )
            if data["hook_name"] == "agent_stop":
                payload = data.get("payload", {})
                await connection.execute(
                    """
                    INSERT INTO agent_loop_projections(
                        sdk_session_id, runtime_generation, owner_fence_token,
                        state, stop_reason, source_hook_audit_id, observed_at, stale
                    ) VALUES (?, ?, ?, 'stopped', ?, ?, ?, 0)
                    ON CONFLICT(sdk_session_id) DO UPDATE SET
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        state = excluded.state,
                        stop_reason = excluded.stop_reason,
                        source_hook_audit_id = excluded.source_hook_audit_id,
                        observed_at = excluded.observed_at,
                        stale = 0
                    """,
                    (
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                        (payload.get("stop_reason") if isinstance(payload, dict) else None),
                        str(data["audit_id"]),
                        float(data.get("observed_at", now)),
                    ),
                )
            elif data["hook_name"] == "error_occurred":
                payload = data.get("payload", {})
                await connection.execute(
                    """
                    INSERT INTO session_error_projections(
                        sdk_session_id, runtime_generation, owner_fence_token,
                        classification, recoverable, correlation_id,
                        source_hook_audit_id, observed_at, stale
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                    ON CONFLICT(sdk_session_id) DO UPDATE SET
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        classification = excluded.classification,
                        recoverable = excluded.recoverable,
                        correlation_id = excluded.correlation_id,
                        source_hook_audit_id = excluded.source_hook_audit_id,
                        observed_at = excluded.observed_at,
                        stale = 0
                    """,
                    (
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                        str(data.get("classification") or "unknown"),
                        (
                            None
                            if not isinstance(payload, dict) or payload.get("recoverable") is None
                            else int(bool(payload["recoverable"]))
                        ),
                        data.get("correlation_id"),
                        str(data["audit_id"]),
                        float(data.get("observed_at", now)),
                    ),
                )
        elif event.raw_type == "copilotd.permission.audit":
            await connection.execute(
                """
                INSERT INTO permission_audit_events(
                    audit_id, sdk_session_id, runtime_generation,
                    owner_fence_token, request_id, permission_kind,
                    managed_settings, managed_approval_required,
                    decision, request_hash, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(audit_id) DO NOTHING
                """,
                (
                    str(data["audit_id"]),
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    data.get("request_id"),
                    str(data["permission_kind"]),
                    int(bool(data.get("managed_settings"))),
                    int(bool(data.get("managed_approval_required"))),
                    str(data["decision"]),
                    str(data["request_hash"]),
                    float(data.get("observed_at", now)),
                ),
            )
            if data["decision"] == "user-not-available":
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET permission_posture = 'platform_blocked',
                        permission_verified_at = NULL,
                        managed_settings_state = 'enforced',
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
        elif event.raw_type in {"hook.start", "hook.progress", "hook.end"}:
            invocation_id = str(
                data.get("hookInvocationId") or data.get("hook_invocation_id") or event.event_id
            )
            audit_id = str(event.event_id or f"{invocation_id}:{event.raw_type}")
            payload_json = json.dumps(data, ensure_ascii=False, sort_keys=True)
            await connection.execute(
                """
                INSERT INTO hook_audit_events(
                    audit_id, sdk_session_id, runtime_generation,
                    owner_fence_token, hook_name, hook_invocation_id,
                    phase, tool_name, tool_call_id, correlation_id,
                    classification, payload_hash, payload_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(audit_id) DO NOTHING
                """,
                (
                    audit_id,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    str(data.get("hookType") or data.get("hook_type") or "unknown"),
                    invocation_id,
                    event.raw_type.rsplit(".", 1)[-1],
                    data.get("toolName") or data.get("tool_name"),
                    event.tool_call_id,
                    event.correlation_id,
                    (str(data.get("status")) if data.get("status") is not None else None),
                    hashlib.sha256(payload_json.encode()).hexdigest(),
                    payload_json,
                    event.received_at,
                ),
            )
        if event.raw_type == "copilotd.interaction.requested":
            interaction_id = str(data["interaction_id"])
            await connection.execute(
                """
                INSERT INTO pending_interactions(
                    interaction_id, protocol_request_id, sdk_session_id, runtime_generation,
                    owner_fence_token, thread_id, kind, response_plane,
                    expires_at, state, payload, response, form_schema,
                    sensitive_response, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(interaction_id) DO NOTHING
                """,
                (
                    interaction_id,
                    data.get("protocol_request_id"),
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    str(data["thread_id"]),
                    str(data["kind"]),
                    str(data.get("response_plane", "direct_handler")),
                    float(data["expires_at"]),
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    (
                        None
                        if data.get("form_schema") is None
                        else json.dumps(
                            data["form_schema"],
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    ),
                    int(bool(data.get("sensitive_response"))),
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
            origin = str(data.get("origin", "app_message"))
            typed_attachment_reason = (
                "scheduler_run"
                if origin == "app_schedule"
                else (
                    "recovery_cleanup"
                    if origin in {"recovery", "recovery_cleanup"}
                    else "__not_typed__"
                )
            )
            admission = await _fetchone_row(
                connection,
                """
                SELECT 1
                FROM session_bindings b
                LEFT JOIN projects p ON p.id = b.project_id
                WHERE b.thread_id = ? AND b.sdk_session_id = ?
                  AND (
                      b.binding_intent = 'active'
                      OR (
                          b.binding_intent = 'closed'
                          AND b.attachment_reason = ?
                      )
                  )
                  AND b.attachment_state = 'attached'
                  AND b.runtime_generation = ?
                  AND b.owner_fence_token = ?
                  AND (
                      p.id IS NULL
                      OR p.state != 'closing'
                         AND NOT (
                             p.project_kind = 'worktree'
                             AND p.state = 'retired'
                         )
                  )
                """,
                (
                    str(data["thread_id"]),
                    event.sdk_session_id,
                    typed_attachment_reason,
                    event.generation,
                    event.fence_token,
                ),
            )
            if admission is None:
                return
            await connection.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, attachment_manifest_id, prompt_hash,
                    requested_mode, requested_delivery, correlation_id, attachment_count,
                    discord_source_channel_id, discord_source_message_id,
                    state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued', ?)
                ON CONFLICT(submission_id) DO NOTHING
                """,
                (
                    submission_id,
                    event.sdk_session_id,
                    origin,
                    data.get("attachment_manifest_id"),
                    data.get("prompt_hash"),
                    data.get("requested_mode"),
                    data.get("requested_delivery", "enqueue"),
                    data.get("correlation_id"),
                    int(data.get("attachment_count", 0)),
                    data.get("discord_source_channel_id"),
                    data.get("discord_source_message_id"),
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
            draining = await _fetchone_row(
                connection,
                "SELECT value FROM global_config WHERE key = 'restart_draining'",
                (),
            )
            ready = await _fetchone_row(
                connection,
                """
                SELECT 1
                FROM session_bindings b
                JOIN session_owner_leases l
                  ON l.sdk_session_id = b.sdk_session_id
                 AND l.fence_token = b.owner_fence_token
                WHERE b.sdk_session_id = ?
                  AND b.runtime_generation = ?
                  AND b.owner_fence_token = ?
                  AND b.binding_intent IN ('active', 'closed')
                  AND b.attachment_state = 'attached'
                  AND b.permission_posture = 'verified_allow_all'
                  AND l.expires_at > ?
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    now,
                ),
            )
            if (draining is not None and draining["value"] == "1") or ready is None:
                await connection.execute(
                    """
                    UPDATE message_queue
                    SET dispatch_attempt = dispatch_attempt + 1, updated_at = ?
                    WHERE id = ? AND state = 'local_queued'
                      AND dispatch_attempt = ?
                    """,
                    (
                        now,
                        str(data["submission_id"]),
                        int(data.get("dispatch_attempt", 0)),
                    ),
                )
                return
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
            await connection.execute(
                """
                UPDATE schedule_runs
                SET status = 'submitting', send_started_at = COALESCE(send_started_at, ?),
                    last_progress_at = ?, updated_at = ?
                WHERE result_submission_id = ?
                  AND status IN ('claimed', 'submitting')
                """,
                (
                    event.received_at,
                    now,
                    now,
                    str(data["submission_id"]),
                ),
            )
        elif event.raw_type == "copilotd.submission.cancel_queued":
            submission_ids = [str(item) for item in data.get("submission_ids", [])]
            cancellable = [str(item) for item in data.get("cancellable_states", [])]
            if not submission_ids or not cancellable:
                return
            ids = ", ".join("?" for _ in submission_ids)
            states = ", ".join("?" for _ in cancellable)
            schedule_rows = await _fetchall_rows(
                connection,
                f"""
                SELECT DISTINCT schedule_run_id FROM message_queue
                WHERE id IN ({ids}) AND schedule_run_id IS NOT NULL
                  AND state IN ({states})
                """,
                (*submission_ids, *cancellable),
            )
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
            for schedule_row in schedule_rows:
                await _finalize_schedule_run_from_reducer(
                    connection,
                    run_id=str(schedule_row["schedule_run_id"]),
                    status="cancelled",
                    completion_basis="queue_cancelled",
                    error_code=None,
                    now=now,
                )
        elif event.raw_type == "copilotd.submission.pre_send_deferred":
            submission_id = str(data["submission_id"])
            cursor = await connection.execute(
                """
                UPDATE submissions
                SET state = 'local_queued', source_operation_id = NULL,
                    send_started_at = NULL, terminal_at = NULL
                WHERE submission_id = ? AND state = 'submitting'
                  AND accepted_message_id IS NULL
                  AND observed_user_event_id IS NULL
                RETURNING submission_id
                """,
                (submission_id,),
            )
            deferred = await cursor.fetchone()
            await cursor.close()
            if deferred is None:
                return
            await connection.execute(
                """
                UPDATE message_queue
                SET state = 'local_queued',
                    dispatch_attempt = dispatch_attempt + 1,
                    updated_at = ?
                WHERE id = ? AND state = 'submitting'
                """,
                (now, submission_id),
            )
            await connection.execute(
                """
                UPDATE schedule_runs
                SET status = 'submitting', send_started_at = NULL,
                    last_progress_at = ?, updated_at = ?
                WHERE result_submission_id = ?
                  AND status = 'submitting'
                  AND accepted_message_id IS NULL
                """,
                (now, now, submission_id),
            )
        elif event.raw_type == "copilotd.queue.replaced":
            old_id = str(data["old_submission_id"])
            new_id = str(data["new_submission_id"])
            allowed = [str(state) for state in data.get("allowed_states", [])]
            if not allowed:
                return
            placeholders = ", ".join("?" for _ in allowed)
            old = await _fetchone_row(
                connection,
                f"""
                SELECT q.*, s.sdk_session_id, s.origin, s.attachment_count,
                       s.requested_delivery, s.discord_source_channel_id,
                       s.discord_source_message_id
                FROM message_queue q
                JOIN submissions s ON s.submission_id = q.id
                WHERE q.id = ? AND q.state IN ({placeholders})
                """,
                (old_id, *allowed),
            )
            if old is None:
                existing = await _fetchone_row(
                    connection,
                    "SELECT id FROM message_queue WHERE id = ?",
                    (new_id,),
                )
                if existing is not None:
                    return
                raise RuntimeError(f"queue item cannot be replaced: {old_id}")
            await connection.execute(
                """
                UPDATE message_queue
                SET state = 'cancelled', schedule_run_id = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, old_id),
            )
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'cancelled', schedule_run_id = NULL
                WHERE submission_id = ?
                """,
                (old_id,),
            )
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND kind = 'submission'
                  AND source_id = ? AND state = 'active'
                """,
                (now, now, event.sdk_session_id, old_id),
            )
            position = await _fetchone_row(
                connection,
                """
                SELECT COALESCE(MAX(position), 0) + 1 AS next_position
                FROM message_queue WHERE thread_id = ?
                """,
                (str(old["thread_id"]),),
            )
            assert position is not None
            schedule_run_id = old["schedule_run_id"]
            await connection.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, parent_submission_id,
                    schedule_run_id, attachment_manifest_id, prompt_hash, requested_mode,
                    requested_model_config, requested_agent,
                    requested_session_config_version, requested_delivery,
                    correlation_id, attachment_count, discord_source_channel_id,
                    discord_source_message_id, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          'local_queued', ?)
                ON CONFLICT(submission_id) DO NOTHING
                """,
                (
                    new_id,
                    old["sdk_session_id"],
                    old["origin"],
                    old_id,
                    schedule_run_id,
                    old["attachment_manifest_id"],
                    str(data["prompt_hash"]),
                    str(data["requested_mode"]),
                    json.dumps(data.get("requested_model_config", {}), sort_keys=True),
                    str(data["requested_agent"]),
                    int(data["requested_session_config_version"]),
                    old["requested_delivery"],
                    f"queue-replacement:{old_id}",
                    int(old["attachment_count"]),
                    old["discord_source_channel_id"],
                    old["discord_source_message_id"],
                    float(data.get("created_at", now)),
                ),
            )
            await connection.execute(
                """
                INSERT INTO message_queue(
                    id, thread_id, discord_message_id, schedule_run_id, prompt,
                    attachment_manifest_id, requested_mode_snapshot,
                    requested_model_config_snapshot, requested_agent_snapshot,
                    requested_session_config_version, position, state, replaces_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued', ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    new_id,
                    old["thread_id"],
                    old["discord_message_id"],
                    schedule_run_id,
                    str(data["prompt"]),
                    old["attachment_manifest_id"],
                    str(data["requested_mode"]),
                    json.dumps(data.get("requested_model_config", {}), sort_keys=True),
                    str(data["requested_agent"]),
                    int(data["requested_session_config_version"]),
                    int(position["next_position"]),
                    old_id,
                    float(data.get("created_at", now)),
                    float(data.get("created_at", now)),
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
                    f"submission:{new_id}",
                    new_id,
                    event.generation,
                    event.fence_token,
                    now,
                    now,
                ),
            )
            if schedule_run_id is not None:
                await connection.execute(
                    """
                    UPDATE schedule_runs
                    SET status = 'submitting', result_submission_id = ?,
                        last_progress_at = ?, updated_at = ?
                    WHERE run_id = ? AND status NOT IN (
                        'semantic_complete', 'failed', 'outcome_unknown',
                        'cancelled', 'target_unknown', 'dispatch_unknown'
                    )
                    """,
                    (new_id, now, now, schedule_run_id),
                )
        elif event.raw_type == "copilotd.submission.active_unknown":
            observed_at = float(data.get("observed_at", now))
            transitioned_cursor = await connection.execute(
                """
                UPDATE submissions
                SET state = 'outcome_unknown'
                WHERE sdk_session_id = ?
                  AND state IN (
                    'submitting', 'submitted', 'submitted_unknown',
                    'observed_active', 'loop_idle', 'continuation_expected'
                  )
                RETURNING submission_id
                """,
                (event.sdk_session_id,),
            )
            transitioned_rows = await transitioned_cursor.fetchall()
            await transitioned_cursor.close()
            transitioned_ids = [str(row["submission_id"]) for row in transitioned_rows]
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
            run_rows: list[Row] = []
            if transitioned_ids:
                placeholders = ", ".join("?" for _ in transitioned_ids)
                run_rows = await _fetchall_rows(
                    connection,
                    f"""
                    SELECT r.run_id FROM schedule_runs r
                    WHERE r.result_submission_id IN ({placeholders})
                      AND r.status IN ('submitting', 'accepted', 'waiting')
                    """,
                    tuple(transitioned_ids),
                )
            for run_row in run_rows:
                await _finalize_schedule_run_from_reducer(
                    connection,
                    run_id=str(run_row["run_id"]),
                    status="outcome_unknown",
                    completion_basis=None,
                    error_code="runtime_active_unknown",
                    now=now,
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
                    runtime_correlation_basis=("acceptance_id_mismatch_runtime_observed"),
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
                UPDATE schedule_runs
                SET status = 'accepted',
                    accepted_message_id = COALESCE(accepted_message_id, ?),
                    accepted_at = COALESCE(accepted_at, ?),
                    last_progress_at = ?, updated_at = ?
                WHERE result_submission_id = ?
                  AND status IN ('claimed', 'submitting', 'accepted')
                """,
                (message_id, event.received_at, now, now, submission_id),
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
                run_row = await _fetchone_row(
                    connection,
                    """
                    SELECT r.run_id
                    FROM schedule_runs r
                    JOIN submissions s ON s.submission_id = r.result_submission_id
                    WHERE r.result_submission_id = ?
                      AND r.accepted_message_id IS NULL
                      AND s.accepted_message_id IS NULL
                      AND s.observed_user_event_id IS NULL
                    """,
                    (submission_id,),
                )
                if run_row is not None:
                    await _finalize_schedule_run_from_reducer(
                        connection,
                        run_id=str(run_row["run_id"]),
                        status="dispatch_unknown",
                        completion_basis=None,
                        error_code="send_acceptance_unknown",
                        now=now,
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE schedule_runs
                        SET status = 'waiting',
                            waiting_at = COALESCE(waiting_at, ?),
                            last_progress_at = ?, updated_at = ?
                        WHERE result_submission_id = ?
                          AND status IN ('submitting', 'accepted', 'waiting')
                          AND EXISTS (
                              SELECT 1 FROM submissions
                              WHERE submission_id = ?
                                AND (
                                    accepted_message_id IS NOT NULL
                                    OR observed_user_event_id IS NOT NULL
                                )
                          )
                        """,
                        (now, now, now, submission_id, submission_id),
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
                            "accepted_message_id": str(rejected["accepted_message_id"]),
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
                run_row = await _fetchone_row(
                    connection,
                    "SELECT run_id FROM schedule_runs WHERE result_submission_id = ?",
                    (submission_id,),
                )
                if run_row is not None:
                    await _finalize_schedule_run_from_reducer(
                        connection,
                        run_id=str(run_row["run_id"]),
                        status="failed",
                        completion_basis=None,
                        error_code="submission_rejected",
                        now=now,
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
        elif event.raw_type == "copilotd.force_teardown.unknown":
            unknown = {str(item) for item in data.get("unknown", [])}
            observed_at = float(data.get("observed_at", now))
            if "tasks" in unknown:
                await connection.execute(
                    """
                    UPDATE task_card_projections
                    SET state = 'unknown', progress_summary = ?,
                        last_progress_at = ?, revision = revision + 1
                    WHERE sdk_session_id = ?
                      AND state IN ('running', 'idle', 'unknown')
                    """,
                    (
                        "Forced teardown could not prove task termination.",
                        observed_at,
                        event.sdk_session_id,
                    ),
                )
            if "schedules" in unknown:
                await connection.execute(
                    """
                    UPDATE runtime_schedules
                    SET state = 'unknown', updated_at = ?
                    WHERE sdk_session_id = ? AND state IN ('active', 'unknown')
                    """,
                    (observed_at, event.sdk_session_id),
                )
            if "native_queue" in unknown:
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET native_queue_count = NULL, native_steering_count = NULL,
                        queue_observed_at = ?, updated_at = ?,
                        row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        observed_at,
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                    ),
                )
            if "activity" in unknown or "abort" in unknown:
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET runtime_processing = NULL, runtime_has_active_work = NULL,
                        runtime_abortable = NULL, activity_observed_at = ?,
                        updated_at = ?, row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        observed_at,
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                    ),
                )
            if "remote" in unknown:
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET runtime_remote_mode = 'unknown', remote_steerable = NULL,
                        remote_observed_at = ?, updated_at = ?,
                        row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        observed_at,
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
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
        elif event.raw_type in {
            "assistant.usage",
            "session.usage_info",
            "session.usage_checkpoint",
            "copilotd.usage.observed",
        }:
            payload = data.get("payload") if event.raw_type == "copilotd.usage.observed" else data
            values = payload if isinstance(payload, dict) else {}
            observed_at = float(data.get("observed_at", event.received_at))
            await connection.execute(
                """
                INSERT INTO usage_projections(
                    sdk_session_id, runtime_generation, owner_fence_token,
                    source_type, source_event_id, payload_json,
                    observed_at, reconciled_at, stale, stale_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    source_type = excluded.source_type,
                    source_event_id = excluded.source_event_id,
                    payload_json = excluded.payload_json,
                    observed_at = excluded.observed_at,
                    reconciled_at = excluded.reconciled_at,
                    stale = 0,
                    stale_reason = NULL
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    event.raw_type,
                    event.event_id or event.internal_event_id,
                    json.dumps(values, ensure_ascii=False, sort_keys=True),
                    observed_at,
                    observed_at if event.source != "sdk" else None,
                ),
            )
            if event.raw_type == "assistant.usage":
                await connection.execute(
                    """
                    INSERT INTO usage_samples(
                        session_id, turn_id, model, input_tokens,
                        output_tokens, cache_read_tokens, cache_write_tokens,
                        nano_aiu, premium_requests, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.sdk_session_id,
                        event.turn_id,
                        _find_nested_value(values, "model"),
                        _find_nested_value(values, "inputTokens"),
                        _find_nested_value(values, "outputTokens"),
                        _find_nested_value(values, "cacheReadTokens"),
                        _find_nested_value(values, "cacheWriteTokens"),
                        _find_nested_value(values, "nanoAiu"),
                        _find_nested_value(values, "premiumRequests"),
                        observed_at,
                    ),
                )
        elif event.raw_type == "copilotd.usage.failed":
            await connection.execute(
                """
                UPDATE usage_projections
                SET stale = 1, stale_reason = ?, reconciled_at = ?
                WHERE sdk_session_id = ?
                """,
                (
                    str(data.get("error_type") or "usage_reconciliation_failed"),
                    now,
                    event.sdk_session_id,
                ),
            )
        elif event.raw_type in {
            "session.context_changed",
            "copilotd.context.observed",
        }:
            payload = data.get("payload") if event.raw_type == "copilotd.context.observed" else data
            values = payload if isinstance(payload, dict) else {}
            observed_at = float(data.get("observed_at", event.received_at))
            await connection.execute(
                """
                INSERT INTO context_projections(
                    sdk_session_id, runtime_generation, owner_fence_token,
                    source_type, source_event_id, payload_json,
                    observed_at, reconciled_at, stale, stale_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    source_type = excluded.source_type,
                    source_event_id = excluded.source_event_id,
                    payload_json = excluded.payload_json,
                    observed_at = excluded.observed_at,
                    reconciled_at = excluded.reconciled_at,
                    stale = 0,
                    stale_reason = NULL
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    event.raw_type,
                    event.event_id or event.internal_event_id,
                    json.dumps(values, ensure_ascii=False, sort_keys=True),
                    observed_at,
                    observed_at if event.source != "sdk" else None,
                ),
            )
        elif event.raw_type == "copilotd.context.failed":
            await connection.execute(
                """
                UPDATE context_projections
                SET stale = 1, stale_reason = ?, reconciled_at = ?
                WHERE sdk_session_id = ?
                """,
                (
                    str(data.get("error_type") or "context_reconciliation_failed"),
                    now,
                    event.sdk_session_id,
                ),
            )
        elif event.raw_type in {
            "session.session_limits_changed",
            "session_limits_exhausted.requested",
        }:
            await connection.execute(
                """
                INSERT INTO session_limit_projections(
                    sdk_session_id, runtime_generation, owner_fence_token,
                    max_ai_credits, used_ai_credits, payload_json,
                    source_event_id, observed_at, stale
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    max_ai_credits = excluded.max_ai_credits,
                    used_ai_credits = excluded.used_ai_credits,
                    payload_json = excluded.payload_json,
                    source_event_id = excluded.source_event_id,
                    observed_at = excluded.observed_at,
                    stale = 0
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    _find_nested_value(data, "maxAiCredits"),
                    _find_nested_value(data, "usedAiCredits"),
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    event.event_id,
                    event.received_at,
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
        elif event.raw_type in {
            "session.managed_settings_resolved",
            "session.managed_settings_enforced",
        }:
            enforced = event.raw_type.endswith("_enforced")
            managed_blocked = (
                enforced
                or bool(data.get("bypassPermissionsDisabled"))
                or bool(data.get("failClosed"))
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET managed_settings_state = ?,
                    managed_permissions_blocked = ?,
                    permission_posture = CASE
                        WHEN ? THEN 'platform_blocked'
                        WHEN permission_posture = 'platform_blocked'
                        THEN 'unverified'
                        ELSE permission_posture
                    END,
                    permission_verified_at = CASE
                        WHEN ? OR permission_posture = 'platform_blocked'
                        THEN NULL ELSE permission_verified_at
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    "enforced" if enforced else "resolved",
                    int(managed_blocked),
                    managed_blocked,
                    managed_blocked,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type in {
            "commands.changed",
            "capabilities.changed",
            "session.tools_updated",
            "session.skills_loaded",
            "session.custom_agents_updated",
            "session.extensions_loaded",
            "session.extensions.attachments_pushed",
            "mcp.tools.list_changed",
            "mcp.resources.list_changed",
            "mcp.prompts.list_changed",
        }:
            extension_kind = {
                "commands.changed": "commands",
                "capabilities.changed": "capabilities",
                "session.tools_updated": "tools",
                "session.skills_loaded": "skills",
                "session.custom_agents_updated": "custom_agents",
                "session.extensions_loaded": "extensions",
                "session.extensions.attachments_pushed": "extension_attachments",
                "mcp.tools.list_changed": "mcp_tools",
                "mcp.resources.list_changed": "mcp_resources",
                "mcp.prompts.list_changed": "mcp_prompts",
            }[event.raw_type]
            await connection.execute(
                """
                INSERT INTO extension_runtime_projections(
                    sdk_session_id, extension_kind, runtime_generation,
                    owner_fence_token, state, detail_json,
                    source_event_id, observed_at, stale
                ) VALUES (?, ?, ?, ?, 'changed', ?, ?, ?, 1)
                ON CONFLICT(sdk_session_id, extension_kind) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    state = excluded.state,
                    detail_json = excluded.detail_json,
                    source_event_id = excluded.source_event_id,
                    observed_at = excluded.observed_at,
                    stale = 1
                """,
                (
                    event.sdk_session_id,
                    extension_kind,
                    event.generation,
                    event.fence_token,
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    event.event_id,
                    now,
                ),
            )
        elif event.raw_type == "session.mcp_servers_loaded":
            await connection.execute(
                """
                INSERT INTO extension_runtime_projections(
                    sdk_session_id, extension_kind, runtime_generation,
                    owner_fence_token, state, detail_json,
                    source_event_id, observed_at, stale
                ) VALUES (?, 'mcp_servers', ?, ?, 'loaded', ?, ?, ?, 0)
                ON CONFLICT(sdk_session_id, extension_kind) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    state = excluded.state,
                    detail_json = excluded.detail_json,
                    source_event_id = excluded.source_event_id,
                    observed_at = excluded.observed_at,
                    stale = 0
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    event.event_id,
                    now,
                ),
            )
        elif event.raw_type == "session.mcp_server_status_changed":
            server_name = str(data.get("serverName") or data.get("name") or "unknown")
            await connection.execute(
                """
                INSERT INTO mcp_server_projections(
                    sdk_session_id, server_name, runtime_generation,
                    owner_fence_token, transport, state, error_code,
                    detail_json, source_event_id, observed_at, stale
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(sdk_session_id, server_name) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    transport = excluded.transport,
                    state = excluded.state,
                    error_code = excluded.error_code,
                    detail_json = excluded.detail_json,
                    source_event_id = excluded.source_event_id,
                    observed_at = excluded.observed_at,
                    stale = 0
                """,
                (
                    event.sdk_session_id,
                    server_name,
                    event.generation,
                    event.fence_token,
                    data.get("transport"),
                    str(data.get("status") or data.get("state") or "unknown"),
                    data.get("errorCode") or data.get("error_code"),
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    event.event_id,
                    now,
                ),
            )
        elif event.raw_type == "copilotd.mode.pending":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_mode = ?, pending_mode_transition_id = ?,
                    mode_reconciliation_state = 'pending', mode_drift = 0,
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
            cursor = await connection.execute(
                """
                SELECT desired_mode, pending_mode, pending_mode_transition_id
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (event.sdk_session_id, event.generation, event.fence_token),
            )
            current = await cursor.fetchone()
            await cursor.close()
            if current is None:
                return
            pending_matches = current["pending_mode"] == mode and (
                transition_id is None or current["pending_mode_transition_id"] == transition_id
            )
            desired_mode = mode if pending_matches else str(current["desired_mode"])
            drift = desired_mode != mode
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_mode = ?,
                    runtime_mode = ?,
                    pending_mode = CASE WHEN ? THEN NULL ELSE pending_mode END,
                    pending_mode_transition_id = CASE
                        WHEN ? THEN NULL ELSE pending_mode_transition_id END,
                    mode_reconciliation_state = ?,
                    mode_drift = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    desired_mode,
                    mode,
                    pending_matches,
                    pending_matches,
                    "drift" if drift else "synced",
                    int(drift),
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
                SET runtime_mode = 'unknown',
                    mode_reconciliation_state = 'unknown',
                    mode_drift = 0, updated_at = ?,
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
            transition_id = str(data["transition_id"])
            cursor = await connection.execute(
                """
                SELECT pending_mode_transition_id FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (event.sdk_session_id, event.generation, event.fence_token),
            )
            current = await cursor.fetchone()
            await cursor.close()
            if current is None or current["pending_mode_transition_id"] != transition_id:
                return
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
                    mode_reconciliation_state = CASE
                        WHEN runtime_mode = desired_mode THEN 'synced' ELSE 'drift'
                    END,
                    mode_drift = CASE
                        WHEN runtime_mode = desired_mode THEN 0 ELSE 1
                    END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    transition_id,
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.pending":
            config = cast(dict[str, Any], data["config"])
            confirmation_mask = _model_confirmation_mask(config)
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_model_config = ?, pending_model_transition_id = ?,
                    model_confirmation_mask = ?,
                    model_reconciliation_state = 'pending', model_drift = 0,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND pending_model_config IS NULL
                """,
                (
                    json.dumps(config, sort_keys=True),
                    str(data["transition_id"]),
                    json.dumps(confirmation_mask),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.model.observed":
            await _apply_model_observation(
                connection,
                event,
                observed=cast(dict[str, Any], data["observed"]),
                source="snapshot",
                observation_id=event.internal_event_id or f"snapshot:{event.inbox_seq}",
                now=now,
            )
        elif event.raw_type == "copilotd.model.confirmed":
            await _apply_model_observation(
                connection,
                event,
                observed=cast(dict[str, Any], data["observed"]),
                source="confirmed",
                observation_id=event.internal_event_id or f"confirmed:{event.inbox_seq}",
                now=now,
                transition_id=str(data["transition_id"]),
            )
        elif event.raw_type == "copilotd.model.unknown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_model_config = NULL,
                    model_reconciliation_state = 'unknown',
                    model_drift = 0, updated_at = ?,
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
            cursor = await connection.execute(
                """
                SELECT desired_model_config, runtime_model_config,
                       pending_model_transition_id
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (event.sdk_session_id, event.generation, event.fence_token),
            )
            current = await cursor.fetchone()
            await cursor.close()
            transition_id = str(data["transition_id"])
            if current is None or current["pending_model_transition_id"] != transition_id:
                return
            runtime_model = (
                None
                if current["runtime_model_config"] is None
                else json.loads(str(current["runtime_model_config"]))
            )
            desired_model = json.loads(str(current["desired_model_config"]))
            model_synced = _model_config_matches(desired_model, runtime_model)
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
                    model_reconciliation_state = ?,
                    model_drift = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    transition_id,
                    transition_id,
                    ("unknown" if runtime_model is None else "synced" if model_synced else "drift"),
                    int(runtime_model is not None and not model_synced),
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
                    agent_observed_sdk_timestamp = MAX(
                        COALESCE(agent_observed_sdk_timestamp, -1), ?
                    ),
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
                    float(data.get("observed_at", now)),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.project_config.observed":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_project_config_version = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    int(data["version"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.config.pending":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_session_config_version = ?,
                    pending_session_config_hash = ?,
                    pending_session_config_transition_id = ?,
                    session_config_state = 'pending',
                    session_config_drift = 0,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_session_config_version IS NULL
                """,
                (
                    int(data["version"]),
                    str(data["config_hash"]),
                    str(data["transition_id"]),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.config.observed":
            observed_version = int(data["version"])
            observed_hash = str(data["config_hash"])
            cursor = await connection.execute(
                """
                SELECT desired_session_config_version,
                       desired_session_config_hash,
                       pending_session_config_version,
                       pending_session_config_hash
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (event.sdk_session_id, event.generation, event.fence_token),
            )
            current = await cursor.fetchone()
            await cursor.close()
            if current is None:
                return
            pending_matches = (
                current["pending_session_config_version"] == observed_version
                and current["pending_session_config_hash"] == observed_hash
            )
            desired_version = (
                observed_version
                if pending_matches
                else int(current["desired_session_config_version"])
            )
            desired_hash = (
                observed_hash
                if pending_matches
                or (
                    desired_version == observed_version
                    and current["desired_session_config_hash"] is None
                )
                else current["desired_session_config_hash"]
            )
            drift = desired_version != observed_version or (
                desired_hash is not None and str(desired_hash) != observed_hash
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_session_config_version = ?,
                    desired_session_config_hash = ?,
                    runtime_session_config_version = ?,
                    runtime_session_config_hash = ?,
                    pending_session_config_version = CASE
                        WHEN ? THEN NULL ELSE pending_session_config_version END,
                    pending_session_config_hash = CASE
                        WHEN ? THEN NULL ELSE pending_session_config_hash END,
                    pending_session_config_transition_id = CASE
                        WHEN ? THEN NULL
                        ELSE pending_session_config_transition_id END,
                    session_config_state = ?,
                    session_config_drift = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    desired_version,
                    desired_hash,
                    observed_version,
                    observed_hash,
                    pending_matches,
                    pending_matches,
                    pending_matches,
                    "drift" if drift else "synced",
                    int(drift),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "copilotd.config.unknown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_session_config_version = NULL,
                    runtime_session_config_hash = NULL,
                    session_config_state = 'unknown',
                    session_config_drift = 0,
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
        elif event.raw_type == "copilotd.config.rejected":
            transition_id = str(data["transition_id"])
            cursor = await connection.execute(
                """
                SELECT pending_session_config_transition_id
                FROM session_bindings
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (event.sdk_session_id, event.generation, event.fence_token),
            )
            current = await cursor.fetchone()
            await cursor.close()
            if current is None or current["pending_session_config_transition_id"] != transition_id:
                return
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_session_config_version = CASE
                        WHEN pending_session_config_transition_id = ?
                        THEN NULL ELSE pending_session_config_version END,
                    pending_session_config_hash = CASE
                        WHEN pending_session_config_transition_id = ?
                        THEN NULL ELSE pending_session_config_hash END,
                    pending_session_config_transition_id = CASE
                        WHEN pending_session_config_transition_id = ?
                        THEN NULL ELSE pending_session_config_transition_id END,
                    session_config_state = CASE
                        WHEN runtime_session_config_version =
                             desired_session_config_version
                          AND runtime_session_config_hash =
                              desired_session_config_hash
                        THEN 'synced' ELSE 'drift' END,
                    session_config_drift = CASE
                        WHEN runtime_session_config_version =
                             desired_session_config_version
                          AND runtime_session_config_hash =
                              desired_session_config_hash
                        THEN 0 ELSE 1 END,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    transition_id,
                    transition_id,
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif event.raw_type == "session.model_change":
            observed: dict[str, Any] = {"modelId": data.get("newModel")}
            for source, target in (
                ("reasoningEffort", "reasoningEffort"),
                ("reasoningSummary", "reasoningSummary"),
                ("contextTier", "contextTier"),
            ):
                if source in data:
                    observed[target] = data[source]
            observed["knownFields"] = sorted(observed)
            await _apply_model_observation(
                connection,
                event,
                observed=observed,
                source="session.model_change",
                observation_id=event.event_id or f"model-change:{event.inbox_seq}",
                now=now,
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
                    UPDATE session_bindings
                    SET mode_reconciliation_state = CASE
                            WHEN pending_mode IS NOT NULL THEN 'pending'
                            WHEN runtime_mode = desired_mode THEN 'synced'
                            ELSE 'drift'
                        END,
                        mode_drift = CASE
                            WHEN pending_mode IS NULL
                              AND runtime_mode != desired_mode THEN 1 ELSE 0
                        END
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
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
                  AND binding_intent != 'closed'
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
        crossed_scalar_event = topic in {"agents", "commands", "remote"} and (
            query_end > query_start or reducer_watermark > query_end
        )
        fresh = epoch == int(reconciliation["requested_epoch"]) and not crossed_scalar_event
        positive = _snapshot_has_positive_evidence(topic, values)
        negative_applied = fresh and caught_up and not positive
        may_merge = not crossed_scalar_event and (positive or negative_applied)

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
        elif topic == "commands" and may_merge:
            commands = [item for item in values.get("commands", []) if isinstance(item, dict)]
            manifest_generation = int(values.get("manifest_generation", epoch))
            seen_commands: list[str] = []
            for item in commands:
                name = str(item.get("name", "")).strip()
                kind = str(item.get("kind", "")).strip()
                if not name or kind != "builtin":
                    continue
                seen_commands.append(name)
                await connection.execute(
                    """
                    INSERT INTO runtime_command_manifest(
                        sdk_session_id, command_name, kind, description,
                        aliases_json, allow_during_agent_execution,
                        experimental, schedulable, input_schema_json,
                        manifest_generation, state, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'available', ?)
                    ON CONFLICT(sdk_session_id, command_name) DO UPDATE SET
                        kind = excluded.kind,
                        description = excluded.description,
                        aliases_json = excluded.aliases_json,
                        allow_during_agent_execution =
                            excluded.allow_during_agent_execution,
                        experimental = excluded.experimental,
                        schedulable = excluded.schedulable,
                        input_schema_json = excluded.input_schema_json,
                        manifest_generation = excluded.manifest_generation,
                        state = 'available',
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        event.sdk_session_id,
                        name,
                        kind,
                        str(item.get("description", "")),
                        json.dumps(
                            item.get("aliases", []),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        int(bool(item.get("allow_during_agent_execution"))),
                        int(bool(item.get("experimental"))),
                        int(bool(item.get("schedulable"))),
                        (
                            None
                            if item.get("input") is None
                            else json.dumps(
                                item["input"],
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                        ),
                        manifest_generation,
                        observed_at,
                    ),
                )
            if fresh and caught_up:
                if seen_commands:
                    placeholders = ", ".join("?" for _ in seen_commands)
                    await connection.execute(
                        f"""
                        UPDATE runtime_command_manifest
                        SET state = 'unavailable',
                            manifest_generation = ?,
                            last_seen_at = ?
                        WHERE sdk_session_id = ?
                          AND command_name NOT IN ({placeholders})
                        """,
                        (
                            manifest_generation,
                            observed_at,
                            event.sdk_session_id,
                            *seen_commands,
                        ),
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE runtime_command_manifest
                        SET state = 'unavailable',
                            manifest_generation = ?,
                            last_seen_at = ?
                        WHERE sdk_session_id = ?
                        """,
                        (manifest_generation, observed_at, event.sdk_session_id),
                    )
            await connection.execute(
                """
                INSERT INTO runtime_command_refreshes(
                    sdk_session_id, manifest_generation, runtime_generation,
                    owner_fence_token, status, source_event_id, refreshed_at
                ) VALUES (?, ?, ?, ?, 'ready', ?, ?)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    manifest_generation = excluded.manifest_generation,
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    status = 'ready',
                    source_event_id = excluded.source_event_id,
                    error_code = NULL,
                    refreshed_at = excluded.refreshed_at
                """,
                (
                    event.sdk_session_id,
                    manifest_generation,
                    event.generation,
                    event.fence_token,
                    values.get("source_event_id"),
                    observed_at,
                ),
            )
        elif topic == "agents" and may_merge:
            agents = [item for item in values.get("agents", []) if isinstance(item, dict)]
            current = values.get("current")
            manifest_generation = int(values.get("manifest_generation", epoch))
            seen_agents: list[str] = []
            for item in agents:
                name = str(item.get("name", "")).strip()
                agent_id = str(item.get("id", "")).strip()
                if not name or not agent_id:
                    continue
                seen_agents.append(name)
                await connection.execute(
                    """
                    INSERT INTO runtime_agent_manifest(
                        sdk_session_id, agent_name, agent_id, display_name,
                        description, source, user_invocable, metadata_json,
                        manifest_generation, state, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'available', ?)
                    ON CONFLICT(sdk_session_id, agent_name) DO UPDATE SET
                        agent_id = excluded.agent_id,
                        display_name = excluded.display_name,
                        description = excluded.description,
                        source = excluded.source,
                        user_invocable = excluded.user_invocable,
                        metadata_json = excluded.metadata_json,
                        manifest_generation = excluded.manifest_generation,
                        state = 'available',
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        event.sdk_session_id,
                        name,
                        agent_id,
                        str(item.get("displayName") or name),
                        str(item.get("description") or ""),
                        item.get("source"),
                        (
                            None
                            if item.get("userInvocable") is None
                            else int(bool(item.get("userInvocable")))
                        ),
                        json.dumps(
                            _safe_agent_metadata(item),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        manifest_generation,
                        observed_at,
                    ),
                )
            if fresh and caught_up:
                if seen_agents:
                    placeholders = ", ".join("?" for _ in seen_agents)
                    await connection.execute(
                        f"""
                        UPDATE runtime_agent_manifest
                        SET state = 'unavailable',
                            manifest_generation = ?,
                            last_seen_at = ?
                        WHERE sdk_session_id = ?
                          AND agent_name NOT IN ({placeholders})
                        """,
                        (
                            manifest_generation,
                            observed_at,
                            event.sdk_session_id,
                            *seen_agents,
                        ),
                    )
                else:
                    await connection.execute(
                        """
                        UPDATE runtime_agent_manifest
                        SET state = 'unavailable',
                            manifest_generation = ?,
                            last_seen_at = ?
                        WHERE sdk_session_id = ?
                        """,
                        (manifest_generation, observed_at, event.sdk_session_id),
                    )
            current_name = (
                "default"
                if not isinstance(current, dict)
                else str(current.get("name") or current.get("displayName") or "default")
            )
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
                    agent_observed_sdk_timestamp = MAX(
                        COALESCE(agent_observed_sdk_timestamp, -1), ?
                    ),
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    current_name,
                    current_name,
                    current_name,
                    current_name,
                    current_name,
                    observed_at,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif topic == "remote" and may_merge:
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_remote_mode = ?, remote_url = ?,
                    remote_steerable = ?, remote_observed_at = ?,
                    remote_snapshot_json = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                """,
                (
                    str(values.get("mode", "unknown")),
                    values.get("url"),
                    (
                        None
                        if values.get("steerable") is None
                        else int(bool(values.get("steerable")))
                    ),
                    observed_at,
                    json.dumps(
                        values.get("metadata", {}),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
        elif topic == "extensions" and (positive or (fresh and caught_up)):
            for extension_kind in ("skills", "agents"):
                detail = values.get(extension_kind, {})
                await connection.execute(
                    """
                    INSERT INTO extension_runtime_projections(
                        sdk_session_id, extension_kind, runtime_generation,
                        owner_fence_token, state, detail_json,
                        source_event_id, observed_at, stale
                    ) VALUES (?, ?, ?, ?, 'observed', ?, ?, ?, 0)
                    ON CONFLICT(sdk_session_id, extension_kind) DO UPDATE SET
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        state = excluded.state,
                        detail_json = excluded.detail_json,
                        source_event_id = excluded.source_event_id,
                        observed_at = excluded.observed_at,
                        stale = 0
                    """,
                    (
                        event.sdk_session_id,
                        extension_kind,
                        event.generation,
                        event.fence_token,
                        json.dumps(detail, ensure_ascii=False, sort_keys=True),
                        event.internal_event_id,
                        observed_at,
                    ),
                )
        elif topic == "mcp" and (positive or (fresh and caught_up)):
            await connection.execute(
                """
                INSERT INTO extension_runtime_projections(
                    sdk_session_id, extension_kind, runtime_generation,
                    owner_fence_token, state, detail_json,
                    source_event_id, observed_at, stale
                ) VALUES (?, 'mcp_servers', ?, ?, 'observed', ?, ?, ?, 0)
                ON CONFLICT(sdk_session_id, extension_kind) DO UPDATE SET
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    state = excluded.state,
                    detail_json = excluded.detail_json,
                    source_event_id = excluded.source_event_id,
                    observed_at = excluded.observed_at,
                    stale = 0
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    json.dumps(values, ensure_ascii=False, sort_keys=True),
                    event.internal_event_id,
                    observed_at,
                ),
            )
        elif topic == "schedules" and (positive or (fresh and caught_up)):
            schedules = [item for item in values.get("schedules", []) if isinstance(item, dict)]
            seen_schedule_ids: list[str] = []
            for item in schedules:
                schedule_id = str(item.get("id") or item.get("scheduleId") or "").strip()
                if not schedule_id:
                    continue
                seen_schedule_ids.append(schedule_id)
                state = str(item.get("state") or item.get("status") or "active").lower()
                recurring = bool(item.get("recurring"))
                schedule_kind = "every" if recurring else "after"
                prompt = str(item.get("prompt") or "")
                await connection.execute(
                    """
                    INSERT INTO runtime_schedules(
                        sdk_session_id, runtime_schedule_id, builtin_name,
                        invocation_input, recurrence, next_run_at, state,
                        updated_at, recurring, schedule_kind, display_prompt,
                        prompt_hash, snapshot_id, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sdk_session_id, runtime_schedule_id) DO UPDATE SET
                        builtin_name = excluded.builtin_name,
                        invocation_input = CASE
                            WHEN runtime_schedules.invocation_input = ''
                            THEN excluded.invocation_input
                            ELSE runtime_schedules.invocation_input
                        END,
                        recurrence = excluded.recurrence,
                        next_run_at = excluded.next_run_at,
                        state = CASE
                            WHEN runtime_schedules.state IN (
                                'cancelled', 'triggered', 'failed'
                            ) THEN runtime_schedules.state
                            ELSE excluded.state
                        END,
                        recurring = excluded.recurring,
                        schedule_kind = excluded.schedule_kind,
                        display_prompt = excluded.display_prompt,
                        prompt_hash = excluded.prompt_hash,
                        snapshot_id = excluded.snapshot_id,
                        observed_at = excluded.observed_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        event.sdk_session_id,
                        schedule_id,
                        schedule_kind,
                        prompt,
                        (
                            item.get("cron")
                            or item.get("intervalMs")
                            or item.get("at")
                            or item.get("recurrence")
                        ),
                        timestamp_seconds(item.get("nextRunAt")),
                        state,
                        observed_at,
                        int(recurring),
                        schedule_kind,
                        item.get("displayPrompt"),
                        stable_hash(prompt),
                        snapshot_id,
                        observed_at,
                    ),
                )
                await connection.execute(
                    """
                    UPDATE runtime_schedules
                    SET state = 'triggered', next_run_at = NULL,
                        terminal_at = COALESCE(terminal_at, ?), updated_at = ?
                    WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                      AND (recurrence IS NULL OR TRIM(recurrence) = '')
                      AND EXISTS (
                          SELECT 1 FROM pending_runtime_schedule_triggers p
                          WHERE p.sdk_session_id =
                                runtime_schedules.sdk_session_id
                            AND p.runtime_schedule_id =
                                runtime_schedules.runtime_schedule_id
                      )
                    """,
                    (observed_at, observed_at, event.sdk_session_id, schedule_id),
                )
                await connection.execute(
                    """
                    DELETE FROM pending_runtime_schedule_triggers
                    WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                    """,
                    (event.sdk_session_id, schedule_id),
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

    async def _apply_native_command_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "copilotd.native_command.pending":
            await connection.execute(
                """
                INSERT INTO runtime_command_invocations(
                    invocation_id, sdk_session_id, operation_id, command_name,
                    input_hash, state, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(invocation_id) DO NOTHING
                """,
                (
                    str(data["invocation_id"]),
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    str(data["command_name"]),
                    str(data["input_hash"]),
                    float(data.get("created_at", now)),
                ),
            )
            return
        await connection.execute(
            """
            UPDATE runtime_command_invocations
            SET result_kind = ?, result_json = ?, state = ?,
                agent_submission_id = ?, selection_token = ?, settled_at = ?
            WHERE invocation_id = ? AND sdk_session_id = ?
            """,
            (
                data.get("result_kind"),
                _json_or_none(data.get("result")),
                str(data["state"]),
                data.get("agent_submission_id"),
                data.get("selection_token"),
                float(data.get("settled_at", now)),
                str(data["invocation_id"]),
                event.sdk_session_id,
            ),
        )

    async def _apply_ephemeral_query_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "copilotd.ephemeral_query.pending":
            await connection.execute(
                """
                INSERT INTO ephemeral_queries(
                    query_id, sdk_session_id, operation_id, question_hash,
                    history_count_before, sdk_receive_seq_before,
                    state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(query_id) DO NOTHING
                """,
                (
                    str(data["query_id"]),
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    str(data["question_hash"]),
                    data.get("history_count_before"),
                    data.get("sdk_receive_seq_before"),
                    float(data.get("created_at", now)),
                ),
            )
            return
        await connection.execute(
            """
            UPDATE ephemeral_queries
            SET history_count_after = ?, sdk_receive_seq_after = ?,
                answer_hash = ?, state = ?, settled_at = ?
            WHERE query_id = ? AND sdk_session_id = ?
            """,
            (
                data.get("history_count_after"),
                data.get("sdk_receive_seq_after"),
                data.get("answer_hash"),
                str(data["state"]),
                float(data.get("settled_at", now)),
                str(data["query_id"]),
                event.sdk_session_id,
            ),
        )

    async def _apply_compaction_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "copilotd.compaction.pending":
            await connection.execute(
                """
                INSERT INTO compaction_runs(
                    compaction_id, sdk_session_id, operation_id, focus_hash,
                    event_cursor_before, sdk_receive_seq_before,
                    context_before_json, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(compaction_id) DO NOTHING
                """,
                (
                    str(data["compaction_id"]),
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    data.get("focus_hash"),
                    data.get("event_cursor_before"),
                    int(data["sdk_receive_seq_before"]),
                    _json_or_none(data.get("context_before")),
                    float(data.get("created_at", now)),
                ),
            )
            return
        await connection.execute(
            """
            UPDATE compaction_runs
            SET result_json = CASE
                    WHEN state IN ('confirmed', 'rejected') THEN result_json
                    ELSE COALESCE(?, result_json)
                END,
                context_after_json = CASE
                    WHEN state IN ('confirmed', 'rejected') THEN context_after_json
                    ELSE COALESCE(?, context_after_json)
                END,
                completion_event_id = CASE
                    WHEN state IN ('confirmed', 'rejected') THEN completion_event_id
                    ELSE COALESCE(?, completion_event_id)
                END,
                state = CASE
                    WHEN state IN ('confirmed', 'rejected') THEN state
                    ELSE ?
                END,
                settled_at = CASE
                    WHEN state IN ('confirmed', 'rejected') THEN settled_at
                    ELSE ?
                END
            WHERE compaction_id = ? AND sdk_session_id = ?
            """,
            (
                _json_or_none(data.get("result")),
                _json_or_none(data.get("context_after")),
                data.get("completion_event_id"),
                str(data["state"]),
                float(data.get("settled_at", now)),
                str(data["compaction_id"]),
                event.sdk_session_id,
            ),
        )

    async def _apply_compaction_sdk_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "session.compaction_start":
            if event.sdk_receive_seq is None:
                return
            await connection.execute(
                """
                UPDATE compaction_runs
                SET state = 'started', start_event_id = ?,
                    start_sdk_receive_seq = ?
                WHERE compaction_id = (
                    SELECT compaction_id FROM compaction_runs
                    WHERE sdk_session_id = ?
                      AND state IN ('pending', 'unknown')
                      AND sdk_receive_seq_before < ?
                    ORDER BY created_at DESC LIMIT 1
                )
                """,
                (
                    event.event_id,
                    event.sdk_receive_seq,
                    event.sdk_session_id,
                    event.sdk_receive_seq,
                ),
            )
            return
        if event.sdk_receive_seq is None:
            return
        success = bool(data.get("success"))
        await connection.execute(
            """
            UPDATE compaction_runs
            SET result_json = ?, completion_event_id = ?,
                completion_sdk_receive_seq = ?, state = ?, settled_at = ?
            WHERE compaction_id = (
                SELECT compaction_id FROM compaction_runs
                WHERE sdk_session_id = ?
                  AND state IN ('pending', 'started', 'unknown')
                  AND sdk_receive_seq_before < ?
                  AND (
                      start_sdk_receive_seq IS NULL
                      OR start_sdk_receive_seq <= ?
                  )
                ORDER BY CASE WHEN start_event_id IS NULL THEN 1 ELSE 0 END,
                         created_at DESC LIMIT 1
            )
            """,
            (
                _json_or_none(data),
                event.event_id,
                event.sdk_receive_seq,
                "confirmed" if success else "rejected",
                now,
                event.sdk_session_id,
                event.sdk_receive_seq,
                event.sdk_receive_seq,
            ),
        )

    async def _apply_fleet_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        submission_id = str(data["submission_id"])
        fleet_run_id = str(data["fleet_run_id"])
        if event.raw_type == "copilotd.fleet.pending":
            await connection.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, source_operation_id,
                    prompt_hash, requested_mode, requested_agent,
                    requested_session_config_version, requested_delivery,
                    state, correlation_id, created_at
                ) VALUES (?, ?, 'fleet', ?, ?, ?, ?, ?, 'fleet', 'submitting', ?, ?)
                ON CONFLICT(submission_id) DO NOTHING
                """,
                (
                    submission_id,
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    str(data["prompt_hash"]),
                    data.get("requested_mode"),
                    data.get("requested_agent"),
                    data.get("requested_session_config_version"),
                    fleet_run_id,
                    float(data.get("created_at", now)),
                ),
            )
            await connection.execute(
                """
                INSERT INTO fleet_runs(
                    fleet_run_id, sdk_session_id, operation_id, submission_id,
                    prompt_hash, state, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(fleet_run_id) DO NOTHING
                """,
                (
                    fleet_run_id,
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    submission_id,
                    str(data["prompt_hash"]),
                    float(data.get("created_at", now)),
                ),
            )
            return
        state = str(data["state"])
        await connection.execute(
            """
            UPDATE fleet_runs SET state = ?, result_json = ?, settled_at = ?
            WHERE fleet_run_id = ? AND sdk_session_id = ?
            """,
            (
                state,
                _json_or_none(data.get("result")),
                float(data.get("settled_at", now)),
                fleet_run_id,
                event.sdk_session_id,
            ),
        )
        submission_state = {
            "confirmed": "observed_active",
            "rejected": "rejected",
            "unknown": "outcome_unknown",
        }.get(state, state)
        await connection.execute(
            """
            UPDATE submissions SET state = ?,
                terminal_at = CASE WHEN ? IN ('rejected', 'outcome_unknown')
                                   THEN ? ELSE terminal_at END
            WHERE submission_id = ? AND sdk_session_id = ?
            """,
            (
                submission_state,
                submission_state,
                float(data.get("settled_at", now)),
                submission_id,
                event.sdk_session_id,
            ),
        )
        if state == "confirmed":
            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                    state = 'active', refreshed_at = excluded.refreshed_at,
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

    async def _apply_task_action_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "copilotd.task_action.pending":
            await connection.execute(
                """
                INSERT INTO runtime_task_actions(
                    action_id, sdk_session_id, operation_id, task_id,
                    action, input_hash, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(action_id) DO NOTHING
                """,
                (
                    str(data["action_id"]),
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    data.get("task_id"),
                    str(data["action"]),
                    str(data["input_hash"]),
                    float(data.get("created_at", now)),
                ),
            )
            return
        await connection.execute(
            """
            UPDATE runtime_task_actions
            SET state = ?, result_json = ?, settled_at = ?
            WHERE action_id = ? AND sdk_session_id = ?
            """,
            (
                str(data["state"]),
                _json_or_none(data.get("result")),
                float(data.get("settled_at", now)),
                str(data["action_id"]),
                event.sdk_session_id,
            ),
        )

    async def _apply_agent_transition_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        transition_id = str(data["transition_id"])
        if event.raw_type == "copilotd.agent_transition.pending":
            await connection.execute(
                """
                INSERT INTO runtime_agent_transitions(
                    transition_id, sdk_session_id, operation_id,
                    previous_agent, target_agent, state, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(transition_id) DO NOTHING
                """,
                (
                    transition_id,
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    str(data["previous_agent"]),
                    str(data["target_agent"]),
                    float(data.get("created_at", now)),
                ),
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_agent = ?, pending_agent_transition_id = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND pending_agent IS NULL
                """,
                (
                    str(data["target_agent"]),
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                ),
            )
            return
        state = str(data["state"])
        await connection.execute(
            """
            UPDATE runtime_agent_transitions
            SET state = ?, result_json = ?, settled_at = ?
            WHERE transition_id = ? AND sdk_session_id = ?
            """,
            (
                state,
                _json_or_none(data.get("result")),
                float(data.get("settled_at", now)),
                transition_id,
                event.sdk_session_id,
            ),
        )
        target = str(data["target_agent"])
        if state == "confirmed":
            await connection.execute(
                """
                UPDATE session_bindings
                SET desired_agent = ?, runtime_agent = ?,
                    pending_agent = NULL, pending_agent_transition_id = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_agent_transition_id = ?
                """,
                (
                    target,
                    target,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    transition_id,
                ),
            )
        elif state == "unknown":
            if data.get("clear_pending"):
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET runtime_agent = ?,
                        pending_agent = NULL,
                        pending_agent_transition_id = NULL,
                        updated_at = ?, row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                      AND pending_agent_transition_id = ?
                    """,
                    (
                        str(data.get("observed_agent") or "unknown"),
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                        transition_id,
                    ),
                )
            else:
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET runtime_agent = 'unknown',
                        updated_at = ?, row_version = row_version + 1
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                      AND pending_agent_transition_id = ?
                    """,
                    (
                        now,
                        event.sdk_session_id,
                        event.generation,
                        event.fence_token,
                        transition_id,
                    ),
                )
        elif state == "rejected":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_agent = COALESCE(?, runtime_agent),
                    pending_agent = NULL, pending_agent_transition_id = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_agent_transition_id = ?
                """,
                (
                    data.get("observed_agent"),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    transition_id,
                ),
            )

    async def _apply_remote_transition_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        transition_id = str(data["transition_id"])
        if event.raw_type == "copilotd.remote_transition.pending":
            await connection.execute(
                """
                INSERT INTO runtime_remote_transitions(
                    transition_id, sdk_session_id, operation_id,
                    previous_mode, target_mode, state, auth_json,
                    repository_json, snapshot_json, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                ON CONFLICT(transition_id) DO NOTHING
                """,
                (
                    transition_id,
                    event.sdk_session_id,
                    str(data["operation_id"]),
                    str(data["previous_mode"]),
                    str(data["target_mode"]),
                    _json_or_none(data["auth"]),
                    _json_or_none(data["repository"]),
                    _json_or_none(data.get("snapshot", {})),
                    float(data.get("created_at", now)),
                ),
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_remote_target = ?,
                    pending_remote_transition_id = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND (
                      pending_remote_transition_id IS NULL
                      OR (
                          ? = 'off'
                          AND runtime_remote_mode = 'unknown'
                      )
                  )
                """,
                (
                    str(data["target_mode"]),
                    transition_id,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    str(data["target_mode"]),
                ),
            )
            return
        state = str(data["state"])
        snapshot = data.get("snapshot", {})
        await connection.execute(
            """
            UPDATE runtime_remote_transitions
            SET state = ?, url = ?, snapshot_json = ?,
                event_id = COALESCE(?, event_id), settled_at = ?
            WHERE transition_id = ? AND sdk_session_id = ?
            """,
            (
                state,
                data.get("url"),
                _json_or_none(snapshot),
                data.get("event_id"),
                float(data.get("settled_at", now)),
                transition_id,
                event.sdk_session_id,
            ),
        )
        target = str(data["target_mode"])
        if state == "confirmed":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_remote_mode = ?, remote_url = ?,
                    remote_steerable = ?, remote_observed_at = ?,
                    remote_snapshot_json = ?,
                    pending_remote_target = NULL,
                    pending_remote_transition_id = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_remote_transition_id = ?
                """,
                (
                    target,
                    data.get("url"),
                    int(target == "on"),
                    float(data.get("settled_at", now)),
                    _json_or_none(snapshot),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    transition_id,
                ),
            )
        elif state == "unknown":
            await connection.execute(
                """
                UPDATE session_bindings
                SET runtime_remote_mode = 'unknown',
                    remote_steerable = NULL, remote_observed_at = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_remote_transition_id = ?
                """,
                (
                    float(data.get("settled_at", now)),
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    transition_id,
                ),
            )
        elif state == "rejected":
            await connection.execute(
                """
                UPDATE session_bindings
                SET pending_remote_target = NULL,
                    pending_remote_transition_id = NULL,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND pending_remote_transition_id = ?
                """,
                (
                    now,
                    event.sdk_session_id,
                    event.generation,
                    event.fence_token,
                    transition_id,
                ),
            )

    async def _apply_schedule_action_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.raw_type == "copilotd.schedule_action.pending":
            await connection.execute(
                """
                INSERT INTO runtime_schedule_actions(
                    action_id, sdk_session_id, operation_id, invocation_id,
                    runtime_schedule_id, builtin_name, action, input_hash,
                    baseline_json, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(action_id) DO NOTHING
                """,
                (
                    str(data["action_id"]),
                    event.sdk_session_id,
                    data.get("operation_id"),
                    data.get("invocation_id"),
                    data.get("runtime_schedule_id"),
                    str(data["builtin_name"]),
                    str(data["action"]),
                    str(data["input_hash"]),
                    _json_or_none(data.get("baseline_ids")),
                    float(data.get("created_at", now)),
                ),
            )
            return
        await connection.execute(
            """
            UPDATE runtime_schedule_actions
            SET runtime_schedule_id = COALESCE(?, runtime_schedule_id),
                invocation_id = COALESCE(?, invocation_id),
                state = ?, result_json = ?, settled_at = ?
            WHERE action_id = ? AND sdk_session_id = ?
            """,
            (
                data.get("runtime_schedule_id"),
                data.get("invocation_id"),
                str(data["state"]),
                _json_or_none(data.get("result")),
                float(data.get("settled_at", now)),
                str(data["action_id"]),
                event.sdk_session_id,
            ),
        )
        if str(data["state"]) == "confirmed" and data.get("runtime_schedule_id") is not None:
            action = str(data.get("action") or "")
            await connection.execute(
                """
                UPDATE runtime_schedules
                SET invocation_id = COALESCE(?, invocation_id),
                    state = CASE
                        WHEN ? = 'cancel' AND state != 'triggered'
                        THEN 'cancelled'
                        ELSE state
                    END,
                    terminal_at = CASE
                        WHEN ? = 'cancel' AND state != 'triggered'
                        THEN COALESCE(terminal_at, ?)
                        ELSE terminal_at
                    END,
                    updated_at = ?
                WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                """,
                (
                    data.get("invocation_id"),
                    action,
                    action,
                    float(data.get("settled_at", now)),
                    now,
                    event.sdk_session_id,
                    str(data["runtime_schedule_id"]),
                ),
            )

    async def _apply_runtime_schedule_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        schedule_id = str(data.get("id") or data.get("scheduleId") or "").strip()
        if not schedule_id:
            return
        if event.raw_type == "session.schedule_cancelled":
            await connection.execute(
                """
                UPDATE runtime_schedules
                SET state = CASE WHEN state = 'triggered' THEN state ELSE 'cancelled' END,
                    last_event_id = ?, terminal_at = COALESCE(terminal_at, ?),
                    observed_at = ?, updated_at = ?
                WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                """,
                (
                    event.event_id,
                    now,
                    now,
                    now,
                    event.sdk_session_id,
                    schedule_id,
                ),
            )
            return
        if event.raw_type == "session.schedule_rearmed":
            await connection.execute(
                """
                UPDATE runtime_schedules
                SET state = CASE
                        WHEN state IN ('cancelled', 'triggered') THEN state
                        ELSE 'active'
                    END,
                    next_run_at = ?, last_event_id = ?,
                    observed_at = ?, updated_at = ?
                WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                """,
                (
                    timestamp_seconds(data.get("nextRunAt") or data.get("next_run_at")),
                    event.event_id,
                    now,
                    now,
                    event.sdk_session_id,
                    schedule_id,
                ),
            )
            return
        recurring = bool(
            data.get("recurring")
            or data.get("cron")
            or data.get("interval")
            or data.get("selfPaced")
        )
        schedule_kind = "every" if recurring else "after"
        prompt = str(data.get("prompt") or "")
        recurrence = data.get("cron") or data.get("interval") or data.get("at") or data.get("tz")
        await connection.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, recurrence, state, last_event_id,
                updated_at, recurring, schedule_kind, display_prompt,
                prompt_hash, observed_at
            ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sdk_session_id, runtime_schedule_id) DO UPDATE SET
                builtin_name = excluded.builtin_name,
                invocation_input = excluded.invocation_input,
                recurrence = excluded.recurrence,
                state = CASE
                    WHEN runtime_schedules.state IN ('cancelled', 'triggered')
                    THEN runtime_schedules.state
                    ELSE 'active'
                END,
                last_event_id = excluded.last_event_id,
                recurring = excluded.recurring,
                schedule_kind = excluded.schedule_kind,
                display_prompt = excluded.display_prompt,
                prompt_hash = excluded.prompt_hash,
                observed_at = excluded.observed_at,
                updated_at = excluded.updated_at
            """,
            (
                event.sdk_session_id,
                schedule_id,
                schedule_kind,
                prompt,
                None if recurrence is None else str(recurrence),
                event.event_id,
                now,
                int(recurring),
                schedule_kind,
                data.get("displayPrompt") or data.get("display_prompt"),
                stable_hash(prompt),
                now,
            ),
        )

    async def _apply_remote_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.sdk_timestamp is None:
            return
        steerable = bool(
            data.get("remoteSteerable")
            if "remoteSteerable" in data
            else data.get("remote_steerable")
        )
        cursor = await connection.execute(
            """
            SELECT pending_remote_target, pending_remote_transition_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (event.sdk_session_id,),
        )
        binding = await cursor.fetchone()
        await cursor.close()
        pending_target = None if binding is None else binding["pending_remote_target"]
        mode = (
            "on"
            if steerable
            else str(pending_target)
            if pending_target in {"off", "export"}
            else "unknown"
        )
        cursor = await connection.execute(
            """
            UPDATE session_bindings
            SET runtime_remote_mode = ?, remote_steerable = ?,
                remote_observed_at = ?,
                remote_observed_sdk_timestamp = ?,
                updated_at = ?,
                row_version = row_version + 1
            WHERE sdk_session_id = ? AND runtime_generation = ?
              AND owner_fence_token = ?
              AND COALESCE(remote_observed_sdk_timestamp, -1) <= ?
            """,
            (
                mode,
                int(steerable),
                now,
                event.sdk_timestamp,
                now,
                event.sdk_session_id,
                event.generation,
                event.fence_token,
                event.sdk_timestamp,
            ),
        )
        applied = cursor.rowcount == 1
        await cursor.close()
        if not applied:
            return
        if binding is not None and binding["pending_remote_transition_id"] is not None:
            await connection.execute(
                """
                UPDATE runtime_remote_transitions
                SET event_id = ?
                WHERE transition_id = ? AND sdk_session_id = ?
                """,
                (
                    event.event_id,
                    str(binding["pending_remote_transition_id"]),
                    event.sdk_session_id,
                ),
            )

    async def _apply_selected_agent_event(
        self,
        connection: Any,
        event: AdaptedEvent,
        *,
        data: dict[str, Any],
        now: float,
    ) -> None:
        if event.agent_id is not None or event.sdk_timestamp is None:
            return
        agent = (
            "default"
            if event.raw_type == "subagent.deselected"
            else str(data.get("agentName") or data.get("agent_name") or "default")
        )
        cursor = await connection.execute(
            """
            SELECT pending_agent, pending_agent_transition_id
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (event.sdk_session_id,),
        )
        pending = await cursor.fetchone()
        await cursor.close()
        cursor = await connection.execute(
            """
            UPDATE session_bindings
            SET runtime_agent = ?,
                desired_agent = CASE WHEN pending_agent = ? THEN ? ELSE desired_agent END,
                pending_agent = CASE WHEN pending_agent = ? THEN NULL ELSE pending_agent END,
                pending_agent_transition_id = CASE
                    WHEN pending_agent = ? THEN NULL ELSE pending_agent_transition_id
                END,
                agent_observed_sdk_timestamp = ?,
                updated_at = ?, row_version = row_version + 1
            WHERE sdk_session_id = ? AND runtime_generation = ?
              AND owner_fence_token = ?
              AND COALESCE(agent_observed_sdk_timestamp, -1) <= ?
            """,
            (
                agent,
                agent,
                agent,
                agent,
                agent,
                event.sdk_timestamp,
                now,
                event.sdk_session_id,
                event.generation,
                event.fence_token,
                event.sdk_timestamp,
            ),
        )
        applied = cursor.rowcount == 1
        await cursor.close()
        if (
            applied
            and pending is not None
            and pending["pending_agent_transition_id"] is not None
            and pending["pending_agent"] == agent
        ):
            await connection.execute(
                """
                UPDATE runtime_agent_transitions
                SET state = 'confirmed',
                    result_json = ?,
                    settled_at = ?
                WHERE transition_id = ? AND sdk_session_id = ?
                  AND state IN ('pending', 'unknown')
                """,
                (
                    _json_or_none(
                        {
                            "basis": "sdk_selected_event",
                            "event_id": event.event_id,
                            "observed_agent": agent,
                        }
                    ),
                    now,
                    str(pending["pending_agent_transition_id"]),
                    event.sdk_session_id,
                ),
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
        runtime_schedule_id = _runtime_schedule_id(event, data)

        if runtime_schedule_id is not None:
            await connection.execute(
                """
                UPDATE runtime_schedules
                SET state = 'triggered', next_run_at = NULL,
                    terminal_at = COALESCE(terminal_at, ?), updated_at = ?
                WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                  AND state IN ('active', 'unknown')
                  AND (recurrence IS NULL OR TRIM(recurrence) = '')
                """,
                (now, now, event.sdk_session_id, runtime_schedule_id),
            )
            await connection.execute(
                """
                INSERT INTO pending_runtime_schedule_triggers(
                    sdk_session_id, runtime_schedule_id,
                    user_event_id, observed_at
                )
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM runtime_schedules
                    WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                )
                ON CONFLICT(sdk_session_id, runtime_schedule_id) DO UPDATE SET
                    user_event_id = excluded.user_event_id,
                    observed_at = excluded.observed_at
                """,
                (
                    event.sdk_session_id,
                    runtime_schedule_id,
                    event.event_id,
                    event.received_at,
                    event.sdk_session_id,
                    runtime_schedule_id,
                ),
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
            return

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
            attachment_count = len(raw_attachments) if isinstance(raw_attachments, list) else None
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
                completion_basis = NULL,
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
        await connection.execute(
            """
            UPDATE message_queue
            SET state = 'submitted', updated_at = ?
            WHERE id = ? AND state = 'outcome_unknown'
            """,
            (now, submission_id),
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
        runtime_schedule_id = _runtime_schedule_id(event, data)
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
        if runtime_schedule_id is not None:
            await connection.execute(
                """
                UPDATE runtime_schedules
                SET state = 'triggered', last_event_id = ?,
                    terminal_at = COALESCE(terminal_at, ?),
                    observed_at = ?, updated_at = ?
                WHERE sdk_session_id = ? AND runtime_schedule_id = ?
                  AND state IN ('active', 'unknown')
                  AND (
                      recurring = 0
                      OR schedule_kind = 'after'
                  )
                """,
                (
                    event.event_id,
                    event.received_at,
                    event.received_at,
                    now,
                    event.sdk_session_id,
                    runtime_schedule_id,
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
        aborted = bool(data.get("aborted"))
        await connection.execute(
            """
            INSERT INTO agent_loop_projections(
                sdk_session_id, runtime_generation, owner_fence_token,
                state, stop_reason, source_hook_audit_id, source_event_id,
                observed_at, stale
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0)
            ON CONFLICT(sdk_session_id) DO UPDATE SET
                runtime_generation = excluded.runtime_generation,
                owner_fence_token = excluded.owner_fence_token,
                state = excluded.state,
                stop_reason = excluded.stop_reason,
                source_hook_audit_id = NULL,
                source_event_id = excluded.source_event_id,
                observed_at = excluded.observed_at,
                stale = 0
            """,
            (
                event.sdk_session_id,
                event.generation,
                event.fence_token,
                "aborted" if aborted else "idle",
                "session_idle_aborted" if aborted else "session_idle",
                event.event_id,
                event.received_at,
            ),
        )
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

        orphan_cursor = await connection.execute(
            """
            SELECT submissions.submission_id, message_queue.schedule_run_id
            FROM submissions
            JOIN message_queue
              ON message_queue.id = submissions.submission_id
            WHERE submissions.sdk_session_id = ?
              AND submissions.observed_user_event_id IS NULL
              AND submissions.created_at <= ?
              AND submissions.state IN (
                  'submitting', 'submitted', 'submitted_unknown'
              )
            ORDER BY submissions.created_at
            """,
            (event.sdk_session_id, event.received_at),
        )
        uncorrelated = await orphan_cursor.fetchall()
        await orphan_cursor.close()
        for submission in uncorrelated:
            submission_id = str(submission["submission_id"])
            await connection.execute(
                """
                UPDATE submissions
                SET state = 'outcome_unknown',
                    completion_basis = 'session_idle_without_user_correlation',
                    terminal_at = COALESCE(terminal_at, ?)
                WHERE submission_id = ?
                  AND state IN ('submitting', 'submitted', 'submitted_unknown')
                """,
                (event.received_at, submission_id),
            )
            await connection.execute(
                """
                UPDATE message_queue
                SET state = 'outcome_unknown', updated_at = ?
                WHERE id = ?
                  AND state IN ('submitting', 'submitted', 'submitted_unknown')
                """,
                (now, submission_id),
            )
            await self._release_submission_liveness(
                connection,
                event,
                submission_id=submission_id,
                now=now,
            )
            schedule_run_id = submission["schedule_run_id"]
            if schedule_run_id is not None:
                await _finalize_schedule_run_from_reducer(
                    connection,
                    run_id=str(schedule_run_id),
                    status="outcome_unknown",
                    completion_basis=None,
                    error_code="session_idle_without_user_correlation",
                    now=now,
                )
            await self._record_runtime_incident_once(
                connection,
                event,
                kind="submission_unobserved_at_session_idle",
                detail={"submission_id": submission_id},
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
            row is not None and int(row["supported"]) == 1 and row["evidence_status"] != "unknown"
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
        if not title:
            title_cursor = await connection.execute(
                """
                SELECT title FROM task_card_projections
                WHERE sdk_session_id = ? AND panel_id = ? AND card_key = ?
                """,
                (event.sdk_session_id, panel_id, card_key),
            )
            title_row = await title_cursor.fetchone()
            await title_cursor.close()
            existing_title = None if title_row is None else str(title_row["title"]).strip()
            title = existing_title or _task_projection_default_title(kind)
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
                ) VALUES (?, ?, 'observed_background', ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                    kind = excluded.kind,
                    source_id = excluded.source_id,
                    runtime_generation = excluded.runtime_generation,
                    owner_fence_token = excluded.owner_fence_token,
                    state = CASE
                        WHEN liveness_leases.runtime_generation =
                             excluded.runtime_generation
                         AND liveness_leases.owner_fence_token =
                             excluded.owner_fence_token
                         AND liveness_leases.state = 'released'
                        THEN 'released'
                        ELSE 'active'
                    END,
                    acquired_at = CASE
                        WHEN liveness_leases.runtime_generation =
                             excluded.runtime_generation
                         AND liveness_leases.owner_fence_token =
                             excluded.owner_fence_token
                        THEN liveness_leases.acquired_at
                        ELSE excluded.acquired_at
                    END,
                    refreshed_at = excluded.refreshed_at,
                    released_at = CASE
                        WHEN liveness_leases.runtime_generation =
                             excluded.runtime_generation
                         AND liveness_leases.owner_fence_token =
                             excluded.owner_fence_token
                         AND liveness_leases.state = 'released'
                        THEN liveness_leases.released_at
                        ELSE NULL
                    END
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
        if submission["terminal_at"] is None or float(submission["terminal_at"]) > evidence_time:
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
                if incoming_state not in {
                    "completed",
                    "failed",
                    "cancelled",
                } and evidence_time > float(existing["terminal_at"]):
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
                return (
                    None,
                    None,
                    (None if existing["objective_id"] is None else str(existing["objective_id"])),
                )
            return (
                submission_id,
                correlation_basis,
                (None if existing["objective_id"] is None else str(existing["objective_id"])),
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
                float(snapshots[topic]["observed_at"]) for topic in ("activity", "queue", "tasks")
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
            submission_id, correlation_basis, objective_id = await self._resolve_task_submission(
                connection,
                event,
                task=task,
                task_id=task_id,
                evidence_time=evidence_time,
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
                    detail_artifact,
                    dependencies_json, artifact_links_json, can_promote,
                    first_seen_at, terminal_at, last_progress_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    submission_id,
                    kind,
                    title,
                    state,
                    progress,
                    detail_artifact,
                    json.dumps(dependencies, ensure_ascii=False),
                    json.dumps(artifact_links, ensure_ascii=False),
                    int(can_promote),
                    evidence_time,
                    terminal_at,
                    now,
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
                    ) VALUES (?, ?, 'observed_background', ?, ?, ?, 'active', ?, ?)
                    ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                        kind = excluded.kind,
                        source_id = excluded.source_id,
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        state = CASE
                            WHEN liveness_leases.runtime_generation =
                                 excluded.runtime_generation
                             AND liveness_leases.owner_fence_token =
                                 excluded.owner_fence_token
                             AND liveness_leases.state = 'released'
                            THEN 'released'
                            ELSE 'active'
                        END,
                        acquired_at = CASE
                            WHEN liveness_leases.runtime_generation =
                                 excluded.runtime_generation
                             AND liveness_leases.owner_fence_token =
                                 excluded.owner_fence_token
                            THEN liveness_leases.acquired_at
                            ELSE excluded.acquired_at
                        END,
                        refreshed_at = excluded.refreshed_at,
                        released_at = CASE
                            WHEN liveness_leases.runtime_generation =
                                 excluded.runtime_generation
                             AND liveness_leases.owner_fence_token =
                                 excluded.owner_fence_token
                             AND liveness_leases.state = 'released'
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
            SELECT source_event_id, task_id, task_type FROM background_observations
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
            if str(observation["task_type"] or "").lower() == "shell":
                await connection.execute(
                    """
                    UPDATE background_observations
                    SET observed_state = 'completed',
                        terminal_evidence = 'task_snapshot_absent',
                        last_progress_at = ?
                    WHERE sdk_session_id = ? AND runtime_generation = ?
                      AND source_event_id = ? AND terminal_evidence IS NULL
                    """,
                    (
                        evidence_time,
                        event.sdk_session_id,
                        event.generation,
                        card_key,
                    ),
                )
                await connection.execute(
                    """
                    UPDATE task_card_projections
                    SET state = 'completed',
                        progress_summary = 'Shell task exited.',
                        terminal_at = ?,
                        last_progress_at = ?,
                        revision = revision + 1
                    WHERE sdk_session_id = ? AND panel_id = ? AND card_key = ?
                      AND terminal_at IS NULL
                    """,
                    (
                        evidence_time,
                        evidence_time,
                        event.sdk_session_id,
                        panel_id,
                        card_key,
                    ),
                )
                await connection.execute(
                    """
                    UPDATE submission_task_links
                    SET state = 'completed',
                        terminal_evidence = 'task_snapshot_absent',
                        last_progress_at = ?, terminal_at = ?
                    WHERE sdk_session_id = ? AND task_id = ? AND terminal_at IS NULL
                    """,
                    (
                        evidence_time,
                        evidence_time,
                        event.sdk_session_id,
                        task_id,
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
                        evidence_time,
                        evidence_time,
                        event.sdk_session_id,
                        f"background:{card_key}",
                        event.generation,
                        event.fence_token,
                    ),
                )
                continue
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
        self._failure: Exception | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def failure(self) -> Exception | None:
        return self._failure

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("reducer worker already started")
        self._failure = None
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
        if worker.done():
            await asyncio.gather(worker, return_exceptions=True)
            self._task = None
            self._inbox.close()
            return
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._inbox.commit_internal(
                    {"type": "copilotd.reducer.stop"},
                    internal_event_id=f"stop:{uuid.uuid4()}",
                )
                await worker
        except asyncio.CancelledError:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            raise
        except TimeoutError:
            worker.cancel()
            try:
                async with asyncio.timeout(timeout_seconds):
                    await asyncio.gather(worker, return_exceptions=True)
            except TimeoutError:
                worker.add_done_callback(_consume_task_result)
        except BaseException:
            if worker.done():
                _consume_task_result(worker)
            else:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            raise
        finally:
            self._task = None
            self._inbox.close()

    async def emergency_stop(self, *, timeout_seconds: float = 5) -> None:
        if self._task is None:
            self._inbox.close()
            return
        self._stopping = True
        self._inbox.close_sdk()
        worker = self._task
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
        try:
            await self._reduce_loop()
        except BaseException as error:
            if isinstance(error, Exception):
                self._failure = error
            self._inbox.fail_pending(error)
            raise

    async def _reduce_loop(self) -> None:
        while True:
            first = await self._inbox.get()
            batch = [first]
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._inbox.get_nowait())
                except asyncio.QueueEmpty:
                    break
            unacknowledged = list(batch)

            def acknowledge(
                envelope: InboxEnvelope,
                *,
                error: BaseException | None = None,
                pending: list[InboxEnvelope] = unacknowledged,
            ) -> None:
                self._inbox.acknowledge(envelope, error=error)
                pending.remove(envelope)

            try:
                if self._fence_validator is not None:
                    valid = await self._fence_validator(
                        first.generation,
                        first.fence_token,
                    )
                    if not valid:
                        raise RuntimeError("reducer owner fence is no longer current")

                events: list[AdaptedEvent] = []
                valid_envelopes: list[InboxEnvelope] = []
                for envelope in batch:
                    try:
                        events.append(self._adapter.adapt(envelope))
                    except InvalidSdkEvent as error:
                        await self._reducer.persist_incident(envelope, error)
                        acknowledge(envelope)
                    else:
                        valid_envelopes.append(envelope)
                if events:
                    await self._reducer.persist(events)
            except BaseException as error:
                for envelope in list(unacknowledged):
                    acknowledge(envelope, error=error)
                raise
            should_stop = any(event.raw_type == "copilotd.reducer.stop" for event in events)
            for envelope in valid_envelopes:
                acknowledge(envelope)
            if should_stop and self._stopping:
                return


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _snapshot_has_positive_evidence(topic: str, values: dict[str, Any]) -> bool:
    if topic == "activity":
        return any(bool(values.get(key)) for key in ("processing", "has_active_work", "abortable"))
    if topic == "queue":
        return bool(values.get("items") or values.get("steering_messages"))
    if topic == "tasks":
        return bool(values.get("tasks"))
    if topic == "remote":
        return str(values.get("mode", "unknown")) != "off"
    if topic == "schedules":
        return bool(values.get("schedules"))
    if topic in {"extensions", "mcp"}:
        return bool(values)
    return bool(values)


def _json_or_none(value: Any) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False, sort_keys=True)


def _safe_agent_metadata(value: dict[str, Any]) -> dict[str, Any]:
    safe_fields = {
        "description",
        "displayName",
        "id",
        "model",
        "name",
        "skills",
        "source",
        "tools",
        "userInvocable",
    }
    return {key: item for key, item in value.items() if key in safe_fields}


def _value(data: Any, key: str) -> str | None:
    if not isinstance(data, dict):
        return None
    value = data.get(key)
    return None if value is None else str(value)


def _runtime_schedule_id(
    event: AdaptedEvent,
    data: dict[str, Any],
) -> str | None:
    return _value(data, "runtimeScheduleId") or _value(
        event.raw_payload,
        "runtimeScheduleId",
    )


def _is_task_projection_event(event: AdaptedEvent) -> bool:
    return event.raw_type in RenderPlanner._TASK_TYPES or (
        event.agent_id is not None
        and event.raw_type
        in {
            "assistant.message_start",
            "assistant.message_delta",
            "assistant.message",
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


def _task_projection_default_title(kind: str) -> str:
    return {
        "agent": "Copilot subagent",
        "orphan": "Copilot agent",
        "tool": "Copilot tool",
    }.get(kind, "Copilot task")


def _metric_summary(data: dict[str, Any]) -> str:
    values: list[str] = []
    if data.get("totalTokens") is not None:
        values.append(f"{int(data['totalTokens']):,} tokens")
    if data.get("totalToolCalls") is not None:
        values.append(f"{int(data['totalToolCalls']):,} tool calls")
    return ", ".join(values)


def _usage_summary_values(data: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "inputTokens",
        "outputTokens",
        "cacheReadTokens",
        "cacheWriteTokens",
        "totalTokens",
        "premiumRequests",
        "aiCredits",
        "nanoAiu",
        "currentTokens",
        "tokenLimit",
    )
    return {key: value for key in keys if (value := _find_nested_value(data, key)) is not None}


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
        "assistant.reasoning_delta": (
            "Copilot is thinking",
            "Copilot is reasoning; raw chain-of-thought is hidden.",
        ),
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
    if raw_type == "assistant.reasoning_delta":
        return fallback
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
        "status": status,
        "character_count": len(text),
        "line_count": line_count,
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
            "stats": {},
            "finalized": True,
            "attachments": [],
        }
    patch = encoded_patch.decode("utf-8")
    stats = _diff_stats(patch)
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
        "byte_count": len(encoded_patch),
        "stats": stats,
        "finalized": True,
        "attachments": attachments,
    }


def _diff_stats(patch: str) -> dict[str, int]:
    lines = patch.splitlines()
    files = sum(line.startswith("diff --git ") for line in lines)
    has_hunks = any(_DIFF_HUNK_PATTERN.match(line) for line in lines)
    fallback_files = 0
    in_hunk = False
    old_remaining = 0
    new_remaining = 0
    additions = 0
    deletions = 0
    for index, line in enumerate(lines):
        hunk = _DIFF_HUNK_PATTERN.match(line)
        if hunk is not None:
            in_hunk = True
            old_remaining = int(hunk.group(1) or 1)
            new_remaining = int(hunk.group(2) or 1)
            continue
        if in_hunk:
            if line.startswith("\\"):
                continue
            if line.startswith("+"):
                additions += 1
                new_remaining = max(0, new_remaining - 1)
            elif line.startswith("-"):
                deletions += 1
                old_remaining = max(0, old_remaining - 1)
            else:
                old_remaining = max(0, old_remaining - 1)
                new_remaining = max(0, new_remaining - 1)
            if old_remaining == 0 and new_remaining == 0:
                in_hunk = False
            continue
        if (
            files == 0
            and has_hunks
            and line.startswith("+++ ")
            and index > 0
            and lines[index - 1].startswith("--- ")
            and index + 1 < len(lines)
            and _DIFF_HUNK_PATTERN.match(lines[index + 1])
        ):
            fallback_files += 1
        if has_hunks:
            continue
        if line.startswith("+++ "):
            fallback_files += 1
            continue
        if line.startswith("--- "):
            continue
        additions += int(line.startswith("+"))
        deletions += int(line.startswith("-"))
    if files == 0:
        files = fallback_files
    stats = {
        "additions": additions,
        "deletions": deletions,
    }
    if files > 0:
        stats["files"] = files
    return stats


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


async def _finalize_schedule_run_from_reducer(
    connection: Connection,
    *,
    run_id: str,
    status: str,
    completion_basis: str | None,
    error_code: str | None,
    now: float,
) -> None:
    row = await _fetchone_row(
        connection,
        """
        SELECT r.*, s.thread_id AS schedule_thread_id,
               s.channel_id AS schedule_channel_id,
               EXISTS (
                   SELECT 1 FROM session_bindings b
                   WHERE b.sdk_session_id = r.result_session_id
               ) AS result_session_bound
        FROM schedule_runs r
        JOIN schedules s ON s.id = r.schedule_id
        WHERE r.run_id = ?
        """,
        (run_id,),
    )
    if row is None or row["render_intent_id"] is not None:
        return
    render_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:schedule-run:{run_id}:final-render"))
    if row["result_session_id"] is not None and bool(row["result_session_bound"]):
        session_id = str(row["result_session_id"])
    elif row["result_thread_id"] is not None or row["schedule_thread_id"] is not None:
        session_id = f"thread:{row['result_thread_id'] or row['schedule_thread_id']}"
    elif row["schedule_channel_id"] is not None:
        session_id = f"channel:{row['schedule_channel_id']}"
    else:
        session_id = "ops:scheduler"
    logical = await _fetchone_row(
        connection,
        """
        SELECT COALESCE(MAX(logical_seq), 0) + 1 AS logical_seq
        FROM render_outbox WHERE session_id = ?
        """,
        (session_id,),
    )
    assert logical is not None
    content = f"Scheduled run `{run_id}` is `{status}`."
    if completion_basis:
        content += f" Completion basis: `{completion_basis}`."
    if error_code:
        content += f" Error: `{error_code}`."
    payload = json.dumps(
        {
            "content": content,
            "finalized": True,
            "schedule_run": {
                "run_id": run_id,
                "schedule_id": str(row["schedule_id"]),
                "status": status,
                "completion_basis": completion_basis,
                "error_code": error_code,
            },
            "render_destination": session_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    await connection.execute(
        """
        INSERT INTO render_outbox(
            id, session_id, logical_seq, lane, coalesce_key,
            idempotency_key, payload, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, 'schedule', ?, ?, ?, 'pending', 0, ?, ?, ?)
        ON CONFLICT(idempotency_key) DO NOTHING
        """,
        (
            render_id,
            session_id,
            int(logical["logical_seq"]),
            f"schedule-run:{run_id}",
            f"schedule-run:{run_id}:final",
            payload,
            now,
            now,
            now,
        ),
    )
    await connection.execute(
        """
        INSERT INTO scheduler_render_intents(
            run_id, render_outbox_id, terminal_status, completion_basis, created_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO NOTHING
        """,
        (run_id, render_id, status, completion_basis, now),
    )
    await connection.execute(
        """
        UPDATE schedule_runs
        SET status = ?, completion_basis = ?, error_code = ?,
            render_intent_id = ?, lease_owner = NULL, lease_expires_at = NULL,
            terminal_at = COALESCE(terminal_at, ?),
            cancelled_at = CASE WHEN ? = 'cancelled' THEN ? ELSE cancelled_at END,
            last_progress_at = ?, updated_at = ?
        WHERE run_id = ?
        """,
        (
            status,
            completion_basis,
            error_code,
            render_id,
            now,
            status,
            now,
            now,
            now,
            run_id,
        ),
    )


async def _fetchone_row(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> Row | None:
    cursor = await connection.execute(statement, parameters)
    row = await cursor.fetchone()
    await cursor.close()
    return row


async def _fetchall_rows(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> list[Row]:
    cursor = await connection.execute(statement, parameters)
    rows = list(await cursor.fetchall())
    await cursor.close()
    return rows


async def _apply_model_observation(
    connection: Any,
    event: AdaptedEvent,
    *,
    observed: dict[str, Any],
    source: str,
    observation_id: str,
    now: float,
    transition_id: str | None = None,
) -> None:
    normalized = _normalize_model_observation(observed)
    cursor = await connection.execute(
        """
        SELECT desired_model_config, pending_model_config,
               pending_model_transition_id, runtime_model_config
        FROM session_bindings
        WHERE sdk_session_id = ? AND runtime_generation = ?
          AND owner_fence_token = ?
        """,
        (event.sdk_session_id, event.generation, event.fence_token),
    )
    current = await cursor.fetchone()
    await cursor.close()
    if current is None:
        return
    previous_runtime = (
        {}
        if current["runtime_model_config"] is None
        else json.loads(str(current["runtime_model_config"]))
    )
    runtime = {
        key: value
        for key, value in previous_runtime.items()
        if key in {"modelId", "reasoningEffort", "reasoningSummary", "contextTier"}
    }
    runtime.update(
        {
            key: value
            for key, value in normalized.items()
            if key in {"modelId", "reasoningEffort", "reasoningSummary", "contextTier"}
        }
    )
    known_fields = set(previous_runtime.get("knownFields", []))
    known_fields.update(normalized.get("knownFields", []))
    runtime["knownFields"] = sorted(known_fields)
    pending = (
        None
        if current["pending_model_config"] is None
        else json.loads(str(current["pending_model_config"]))
    )
    pending_transition_matches = pending is not None and (
        transition_id is None or current["pending_model_transition_id"] == transition_id
    )
    pending_confirmed = pending_transition_matches and _model_config_matches(pending, runtime)
    desired = json.loads(str(current["desired_model_config"]))
    if pending_confirmed:
        desired = pending
    drift = bool(desired) and not _model_config_matches(desired, runtime)
    reconciliation_state = (
        "pending"
        if pending is not None and not pending_confirmed
        else "drift"
        if drift
        else "synced"
    )
    await connection.execute(
        """
        UPDATE session_bindings
        SET desired_model_config = ?,
            runtime_model_config = ?,
            pending_model_config = CASE WHEN ? THEN NULL ELSE pending_model_config END,
            pending_model_transition_id = CASE
                WHEN ? THEN NULL ELSE pending_model_transition_id END,
            model_confirmation_mask = ?,
            model_reconciliation_state = ?,
            model_drift = ?,
            updated_at = ?, row_version = row_version + 1
        WHERE sdk_session_id = ? AND runtime_generation = ?
          AND owner_fence_token = ?
        """,
        (
            json.dumps(desired, sort_keys=True),
            json.dumps(runtime, sort_keys=True),
            pending_confirmed,
            pending_confirmed,
            json.dumps(_model_confirmation_mask(desired)),
            reconciliation_state,
            int(drift),
            now,
            event.sdk_session_id,
            event.generation,
            event.fence_token,
        ),
    )
    await connection.execute(
        """
        INSERT INTO model_config_observations(
            sdk_session_id, runtime_generation, event_id, model_id,
            reasoning_effort, reasoning_summary, context_tier,
            known_fields, source, observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sdk_session_id, event_id) DO NOTHING
        """,
        (
            event.sdk_session_id,
            event.generation,
            observation_id,
            runtime.get("modelId"),
            runtime.get("reasoningEffort"),
            runtime.get("reasoningSummary"),
            runtime.get("contextTier"),
            json.dumps(sorted(known_fields)),
            source,
            now,
        ),
    )


def _normalize_model_observation(value: dict[str, Any]) -> dict[str, Any]:
    aliases = {
        "modelId": ("modelId", "newModel", "model"),
        "reasoningEffort": ("reasoningEffort",),
        "reasoningSummary": ("reasoningSummary",),
        "contextTier": ("contextTier",),
    }
    normalized: dict[str, Any] = {}
    known_fields: set[str] = set()
    explicit_known = value.get("knownFields")
    if isinstance(explicit_known, list):
        known_fields.update(str(item) for item in explicit_known)
    for target, sources in aliases.items():
        for source in sources:
            if source in value:
                normalized[target] = value[source]
                known_fields.add(target)
                break
    normalized["knownFields"] = sorted(known_fields)
    return normalized


def _model_confirmation_mask(config: dict[str, Any]) -> list[str]:
    explicit = config.get("confirmationMask")
    if isinstance(explicit, list):
        return sorted(
            {
                str(item)
                for item in explicit
                if str(item) in {"modelId", "reasoningEffort", "reasoningSummary", "contextTier"}
            }
        )
    return sorted(
        key
        for key in ("modelId", "reasoningEffort", "reasoningSummary", "contextTier")
        if key in config
    )


def _model_config_matches(
    requested: dict[str, Any],
    observed: dict[str, Any] | None,
) -> bool:
    if observed is None:
        return False
    known_fields = set(observed.get("knownFields", observed))
    return all(
        key in known_fields and observed.get(key) == requested.get(key)
        for key in _model_confirmation_mask(requested)
    )


def _tool_spill_path(root: Path, session_id: str, tool_call_id: str) -> Path:
    session_token = uuid.uuid5(uuid.NAMESPACE_URL, f"session:{session_id}").hex[:16]
    tool_token = uuid.uuid5(uuid.NAMESPACE_URL, f"tool:{tool_call_id}").hex[:16]
    return root / session_token / "artifacts" / f"tool-{tool_token}.txt"


def _snapshot_trusted_local_artifact(
    root: Path,
    cwd: Path,
    managed_session_state_root: Path | None,
    session_id: str,
    source_path: str,
) -> tuple[Path, int, str] | None:
    resolved_cwd = cwd.resolve(strict=False)
    candidate = Path(source_path)
    source_candidate = candidate if candidate.is_absolute() else resolved_cwd / candidate
    resolved_session_state_root = (
        None
        if managed_session_state_root is None
        else managed_session_state_root.resolve(strict=False)
    )
    managed_files = _managed_session_files_root(managed_session_state_root, session_id)
    if source_candidate.suffix.lower() not in _TRUSTED_LOCAL_IMAGE_SUFFIXES:
        return None
    try:
        source_file = _open_trusted_local_artifact(
            source_candidate,
            cwd=resolved_cwd,
            managed_session_state_root=resolved_session_state_root,
            managed_files=managed_files,
        )
    except OSError:
        return None

    session_token = uuid.uuid5(uuid.NAMESPACE_URL, f"session:{session_id}").hex[:16]
    source_token = uuid.uuid5(uuid.NAMESPACE_URL, f"local-image:{source_path}").hex[:16]
    snapshot_root = root / session_token / "artifacts" / "local-images"
    with source_file as input_file:
        snapshot_root.mkdir(parents=True, exist_ok=True)
        temporary = snapshot_root / f".{source_token}.{uuid.uuid4().hex}.tmp"
        digest = hashlib.sha256()
        byte_size = 0
        try:
            with temporary.open("xb") as output_file:
                for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                    output_file.write(chunk)
                    digest.update(chunk)
                    byte_size += len(chunk)
                output_file.flush()
                os.fsync(output_file.fileno())
            digest_text = digest.hexdigest()
            suffix = source_candidate.suffix.lower()
            snapshot_path = snapshot_root / f"{source_token}-{digest_text}{suffix}"
            if snapshot_path.exists():
                temporary.unlink(missing_ok=True)
            else:
                os.replace(temporary, snapshot_path)
            return snapshot_path, byte_size, digest_text
        finally:
            temporary.unlink(missing_ok=True)


def _managed_session_files_root(root: Path | None, session_id: str) -> Path | None:
    if root is None or Path(session_id).name != session_id or session_id in {".", ".."}:
        return None
    resolved_root = root.resolve(strict=False)
    session_root = resolved_root / session_id
    files_root = session_root / "files"
    if session_root.is_symlink() or files_root.is_symlink():
        return None
    return files_root.resolve(strict=False)


def _open_trusted_local_artifact(
    source: Path,
    *,
    cwd: Path,
    managed_session_state_root: Path | None,
    managed_files: Path | None,
) -> BinaryIO:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(source, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise OSError("trusted local image is not a regular file")
        resolved = source.resolve(strict=True)
        current = resolved.stat()
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise OSError("trusted local image changed while opening")
        if managed_session_state_root is not None and _path_is_within(
            resolved,
            managed_session_state_root,
        ):
            if managed_files is None or not _path_is_within(resolved, managed_files):
                raise OSError("managed local image belongs to another session")
        elif not _path_is_within(resolved, cwd):
            raise OSError("trusted local image is outside approved roots")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


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
