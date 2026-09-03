from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from copilotd.core.volatile_content import (
    VolatileContentStore,
    opaque_content_key,
)
from copilotd.discord_http_limiter import (
    DiscordHttpRateLimiterClosed,
    DiscordHttpRedirectBlocked,
    DiscordHttpRouteCapacityExceeded,
)
from copilotd.discord_requests import DiscordRequestError
from copilotd.storage.database import Database
from copilotd.storage.state_only import (
    fixed_error_code,
    payload_sha256,
    render_payload_receipt,
)

_REACTION_EMOJIS = {
    "accepted": "👀",
    "reasoning": "🧠",
    "action": "🛠️",
    "unresolved": "❓",
    "succeeded": "✅",
    "failed": "❌",
}
_REACTION_STATES_BY_EMOJI = {emoji: state for state, emoji in _REACTION_EMOJIS.items()}
_SUBMISSION_FAILURE_STATES = {
    "cancelled",
    "observed_aborted",
    "outcome_unknown",
    "rejected",
    "semantic_blocked",
    "submitted_unknown",
}


class RenderTransport(Protocol):
    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str: ...

    async def edit(
        self,
        *,
        session_id: str,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None: ...

    async def reaction(
        self,
        *,
        session_id: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None: ...


class RenderDeliveryError(RuntimeError):
    pass


class RenderRateLimited(RenderDeliveryError):
    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f"Discord rate limited render for {retry_after} seconds")


class RenderTransientError(RenderDeliveryError):
    pass


class RenderPermanentError(RenderDeliveryError):
    pass


@dataclass(frozen=True, slots=True)
class OutboxItem:
    id: str
    session_id: str
    logical_seq: int
    lane: str
    coalesce_key: str | None
    idempotency_key: str
    payload: dict[str, Any] | None
    metadata: dict[str, Any]
    content_key: str | None
    content_hash: str | None
    payload_revision: int
    attempts: int


class RenderOutboxDispatcher:
    def __init__(
        self,
        database: Database,
        transport: RenderTransport,
        *,
        claim_timeout_seconds: float = 60,
        max_transient_attempts: int = 3,
        content_store: VolatileContentStore | None = None,
    ) -> None:
        self._database = database
        self._transport = transport
        self._claim_timeout_seconds = claim_timeout_seconds
        self._max_transient_attempts = max_transient_attempts
        self._content_store = content_store or database.content_store
        self._dispatch_lock = asyncio.Lock()
        self._restore_tasks: set[asyncio.Task[None]] = set()

    async def dispatch_once(
        self,
        *,
        limit: int = 20,
        now: float | None = None,
        _deadline: float | None = None,
    ) -> int:
        async with self._dispatch_lock:
            live_clock = now is None
            timestamp = time.time() if live_clock else now
            claimed_ids: list[str] = []
            try:
                items = await self._claim(
                    limit=limit,
                    now=timestamp,
                    claimed_ids=claimed_ids,
                )
            except asyncio.CancelledError:
                await self._restore_claim_ids(
                    claimed_ids,
                    deadline=_deadline,
                )
                raise
            except Exception:
                await self._restore_claim_ids(
                    claimed_ids,
                    deadline=_deadline,
                )
                raise
            delivered = 0
            for index, item in enumerate(items):
                delivery_now = time.time() if live_clock else timestamp
                try:
                    was_delivered, stop_for_retry = await self._deliver(
                        item,
                        now=delivery_now,
                        live_clock=live_clock,
                    )
                    if item.content_key is not None:
                        await self._delete_content_if_unreferenced(item.content_key)
                    if was_delivered:
                        delivered += 1
                        await self._delete_finalized_source_content(item)
                    if stop_for_retry:
                        await self._restore_claims(
                            items[index + 1 :],
                            deadline=_deadline,
                        )
                        break
                except asyncio.CancelledError:
                    await self._restore_claims(
                        items[index:],
                        deadline=_deadline,
                    )
                    raise
                except Exception:
                    await self._restore_claims(
                        items[index:],
                        deadline=_deadline,
                    )
                    raise
            return delivered

    async def drain(
        self,
        *,
        deadline_seconds: float = 30,
        limit: int = 50,
    ) -> int:
        if deadline_seconds <= 0:
            raise ValueError("drain deadline must be positive")
        deadline = time.monotonic() + deadline_seconds
        restore_reserve = min(0.05, deadline_seconds * 0.2)
        delivered = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return delivered
            dispatch_budget = remaining - min(restore_reserve, remaining / 2)
            if dispatch_budget <= 0:
                return delivered
            try:
                async with asyncio.timeout(dispatch_budget):
                    count = await self.dispatch_once(
                        limit=limit,
                        _deadline=deadline,
                    )
                    pending = await self._database.fetchone(
                        """
                        SELECT COUNT(*) AS count,
                               MIN(next_attempt_at) AS next_attempt_at
                        FROM render_outbox
                        WHERE state IN ('pending', 'sending')
                        """
                    )
            except TimeoutError:
                return delivered
            delivered += count
            if pending is None or int(pending["count"]) == 0:
                return delivered
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return delivered
            next_attempt = pending["next_attempt_at"]
            eligibility_wait = (
                0.01 if next_attempt is None else max(0.01, float(next_attempt) - time.time())
            )
            await asyncio.sleep(min(eligibility_wait, 0.25, remaining))

    async def _restore_claims(
        self,
        items: list[OutboxItem],
        *,
        deadline: float | None = None,
    ) -> None:
        await self._restore_claim_ids(
            [item.id for item in items],
            deadline=deadline,
        )

    async def _restore_claim_ids(
        self,
        identifiers: list[str],
        *,
        deadline: float | None = None,
    ) -> None:
        if not identifiers:
            return
        placeholders = ", ".join("?" for _ in identifiers)
        timestamp = time.time()

        async def restore() -> None:
            await self._database.execute(
                f"""
                UPDATE render_outbox
                SET state = 'pending', next_attempt_at = MIN(next_attempt_at, ?),
                    updated_at = ?
                WHERE id IN ({placeholders}) AND state = 'sending'
                """,
                (timestamp, timestamp, *identifiers),
            )

        task = asyncio.create_task(restore())
        self._restore_tasks.add(task)
        task.add_done_callback(self._restore_task_done)
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if remaining == 0:
            return
        try:
            if remaining is None:
                await asyncio.shield(task)
            else:
                async with asyncio.timeout(remaining):
                    await asyncio.shield(task)
        except TimeoutError:
            return
        except asyncio.CancelledError:
            if deadline is None:
                await task
            raise

    def _restore_task_done(self, task: asyncio.Task[None]) -> None:
        self._restore_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _delete_content_if_unreferenced(self, key: str) -> None:
        referenced = await self._database.fetchone(
            """
            SELECT 1 FROM render_outbox
            WHERE content_key = ?
              AND state IN ('pending', 'sending', 'blocked')
            LIMIT 1
            """,
            (key,),
        )
        if referenced is None:
            self._content_store.delete(key)

    async def _delete_finalized_source_content(self, item: OutboxItem) -> None:
        payload = item.payload
        if not isinstance(payload, dict) or not bool(payload.get("finalized")):
            return
        if payload.get("type") == "assistant.message" and payload.get("message_id") is not None:
            self._content_store.delete(
                opaque_content_key(
                    "assistant-stream",
                    item.session_id,
                    payload["message_id"],
                    payload.get("agent_id") or "",
                )
            )
        if payload.get("type") == "tool_card" and payload.get("turn_render_key") is not None:
            rows = await self._database.fetchall(
                """
                SELECT tool_call_id FROM tool_render_state
                WHERE sdk_session_id = ? AND turn_key = ?
                """,
                (item.session_id, str(payload["turn_render_key"])),
            )
            for row in rows:
                self._content_store.delete(
                    opaque_content_key(
                        "tool-display",
                        item.session_id,
                        payload["turn_render_key"],
                        row["tool_call_id"],
                    )
                )
        interaction = payload.get("interaction")
        if isinstance(interaction, dict) and interaction.get("interaction_id") is not None:
            for scope in ("interaction-request", "interaction-response"):
                self._content_store.delete(
                    opaque_content_key(
                        scope,
                        item.session_id,
                        interaction["interaction_id"],
                    )
                )

    async def _claim(
        self,
        *,
        limit: int,
        now: float,
        claimed_ids: list[str],
    ) -> list[OutboxItem]:
        retired_keys: list[str] = []
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'pending', updated_at = ?
                WHERE state = 'sending' AND updated_at <= ?
                """,
                (now, now - self._claim_timeout_seconds),
            )
            cursor = await connection.execute(
                """
                SELECT content_key FROM render_outbox AS older
                WHERE older.state = 'pending'
                  AND older.content_key IS NOT NULL
                  AND older.coalesce_key IS NOT NULL
                  AND EXISTS (
                    SELECT 1 FROM render_outbox AS newer
                    WHERE newer.session_id = older.session_id
                      AND newer.coalesce_key = older.coalesce_key
                      AND newer.state = 'pending'
                      AND newer.logical_seq > older.logical_seq
                  )
                """
            )
            retired_keys.extend(str(row["content_key"]) for row in await cursor.fetchall())
            await cursor.close()
            await connection.execute(
                """
                UPDATE render_outbox AS older
                SET state = 'superseded', content_key = NULL, updated_at = ?
                WHERE older.state = 'pending'
                  AND older.coalesce_key IS NOT NULL
                  AND EXISTS (
                    SELECT 1 FROM render_outbox AS newer
                    WHERE newer.session_id = older.session_id
                      AND newer.coalesce_key = older.coalesce_key
                      AND newer.state = 'pending'
                      AND newer.logical_seq > older.logical_seq
                  )
                """,
                (now,),
            )
            cursor = await connection.execute(
                """
                SELECT candidate.* FROM render_outbox AS candidate
                WHERE candidate.state = 'pending'
                  AND candidate.next_attempt_at <= ?
                  AND NOT EXISTS (
                    SELECT 1 FROM render_outbox AS earlier
                    WHERE earlier.session_id = candidate.session_id
                      AND earlier.state IN ('pending', 'sending')
                      AND (earlier.lane IN ('reaction', 'admission_reaction'))
                          = (candidate.lane IN ('reaction', 'admission_reaction'))
                      AND (
                        earlier.logical_seq < candidate.logical_seq
                        OR (
                            earlier.logical_seq = candidate.logical_seq
                            AND earlier.created_at < candidate.created_at
                        )
                      )
                  )
                ORDER BY candidate.session_id,
                         CASE
                             WHEN candidate.lane IN ('reaction', 'admission_reaction')
                             THEN 1 ELSE 0
                         END,
                         candidate.logical_seq,
                         candidate.created_at
                LIMIT ?
                """,
                (now, limit),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            items: list[OutboxItem] = []
            for row in rows:
                update = await connection.execute(
                    """
                    UPDATE render_outbox
                    SET state = 'sending', attempts = attempts + 1, updated_at = ?
                    WHERE id = ? AND state = 'pending'
                    """,
                    (now, row["id"]),
                )
                claimed = update.rowcount == 1
                await update.close()
                if claimed:
                    claimed_ids.append(str(row["id"]))
                    metadata = json.loads(str(row["payload"]))
                    for column, key in (
                        ("source_submission_id", "submission_id"),
                        ("source_channel_id", "source_channel_id"),
                        ("source_message_id", "source_message_id"),
                        ("tool_call_id", "tool_call_id"),
                        ("render_kind", "render_kind"),
                        ("finalized", "finalized"),
                        ("reaction_state", "state"),
                        ("previous_reaction_state", "previous_reaction_state"),
                    ):
                        if metadata.get(key) is None and row[column] is not None:
                            metadata[key] = row[column]
                    content_key = row["content_key"] or metadata.get("content_key")
                    content_hash = row["content_hash"] or metadata.get("content_sha256")
                    payload = self._content_store.get(
                        None if content_key is None else str(content_key),
                        expected_hash=None if content_hash is None else str(content_hash),
                    )
                    items.append(
                        OutboxItem(
                            id=row["id"],
                            session_id=row["session_id"],
                            logical_seq=row["logical_seq"],
                            lane=row["lane"],
                            coalesce_key=row["coalesce_key"],
                            idempotency_key=row["idempotency_key"],
                            payload=payload if isinstance(payload, dict) else None,
                            metadata=metadata,
                            content_key=None if content_key is None else str(content_key),
                            content_hash=None if content_hash is None else str(content_hash),
                            payload_revision=int(row["payload_revision"]),
                            attempts=row["attempts"] + 1,
                        )
                    )
        for key in retired_keys:
            await self._delete_content_if_unreferenced(key)
        return items

    async def _deliver(
        self,
        item: OutboxItem,
        *,
        now: float,
        live_clock: bool,
    ) -> tuple[bool, bool]:
        async with self._database.render_delivery_lock:
            try:
                return await self._deliver_locked(
                    item,
                    now=now,
                    live_clock=live_clock,
                )
            except (
                DiscordRequestError,
                DiscordHttpRateLimiterClosed,
                DiscordHttpRedirectBlocked,
                DiscordHttpRouteCapacityExceeded,
            ):
                retry_now = time.time() if live_clock else now
                if item.attempts >= self._max_transient_attempts:
                    blocked = await self._block(item, now=retry_now)
                    return False, not blocked
                await self._retry(
                    item,
                    next_attempt_at=retry_now + min(2 ** (item.attempts - 1), 30),
                    now=retry_now,
                )
                return False, True

    async def _deliver_locked(
        self,
        item: OutboxItem,
        *,
        now: float,
        live_clock: bool,
    ) -> tuple[bool, bool]:
        current = await self._database.fetchone(
            "SELECT state FROM render_outbox WHERE id = ?",
            (item.id,),
        )
        if current is None or str(current["state"]) != "sending":
            return False, False
        if item.payload is None and item.lane in {"reaction", "admission_reaction"}:
            item = _with_reconstructed_reaction(item)
        if item.payload is None and item.metadata.get("render_kind") == "content_unavailable":
            item = _with_reconstructed_content_unavailable(item)
        if item.payload is None:
            await self._mark_content_unavailable(item, now=now)
            return False, False
        if item.lane == "reaction":
            return await self._deliver_reaction(
                item,
                now=now,
                live_clock=live_clock,
            )
        if item.lane == "admission_reaction":
            return await self._deliver_admission_reaction(
                item,
                now=now,
                live_clock=live_clock,
            )
        logical_key = item.coalesce_key or item.id
        payload_hash = _payload_hash(item.payload)
        transport_idempotency_key = (
            f"{item.idempotency_key}:payload:{item.payload_revision}:{payload_hash[:16]}"
        )
        mapping = await self._database.fetchone(
            """
            SELECT * FROM render_messages
            WHERE session_id = ? AND logical_key = ?
            """,
            (item.session_id, logical_key),
        )
        finalized = bool(item.payload.get("finalized"))
        try:
            if mapping is not None and mapping["content_hash"] == payload_hash:
                message_id = mapping["discord_message_id"]
            elif mapping is None:
                message_id = await self._transport.send(
                    session_id=item.session_id,
                    lane=item.lane,
                    payload=item.payload,
                    idempotency_key=transport_idempotency_key,
                )
            else:
                message_id = mapping["discord_message_id"]
                edit = self._transport.edit
                if "session_id" in inspect.signature(edit).parameters:
                    await edit(
                        session_id=item.session_id,
                        message_id=message_id,
                        lane=item.lane,
                        payload=item.payload,
                        idempotency_key=transport_idempotency_key,
                    )
                else:
                    await edit(
                        message_id=message_id,
                        lane=item.lane,
                        payload=item.payload,
                    )
        except RenderRateLimited as error:
            retry_now = time.time() if live_clock else now
            await self._retry(
                item,
                next_attempt_at=retry_now + error.retry_after,
                now=retry_now,
            )
            return False, True
        except RenderTransientError as error:
            retry_now = time.time() if live_clock else now
            if item.attempts >= self._max_transient_attempts:
                blocked = await self._block(item, now=retry_now)
                if blocked:
                    recorded = await self._record_render_failure(
                        item,
                        error,
                        now=retry_now,
                    )
                    if not recorded:
                        return False, True
                    surfaced = await self._attempt_render_failure_surface(
                        item,
                        mapping=mapping,
                    )
                    return surfaced, False
                return False, not blocked
            else:
                await self._retry(
                    item,
                    next_attempt_at=retry_now + min(2 ** (item.attempts - 1), 30),
                    now=retry_now,
                )
                return False, True
        except RenderPermanentError as error:
            failure_now = time.time() if live_clock else now
            blocked = await self._block(
                item,
                now=failure_now,
            )
            if not blocked:
                return False, True
            recorded = await self._record_render_failure(item, error, now=failure_now)
            if not recorded:
                return False, True
            surfaced = await self._attempt_render_failure_surface(
                item,
                mapping=mapping,
            )
            return surfaced, False

        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO render_messages(
                    session_id, logical_key, discord_message_id, content_hash, finalized
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(session_id, logical_key) DO UPDATE SET
                    discord_message_id = excluded.discord_message_id,
                    content_hash = excluded.content_hash,
                    finalized = MAX(render_messages.finalized, excluded.finalized)
                """,
                (
                    item.session_id,
                    logical_key,
                    message_id,
                    payload_hash,
                    int(finalized),
                ),
            )
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'sent', content_key = NULL,
                    last_error = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (now, item.id, item.payload_revision),
            )
            payload_changed = cursor.rowcount == 0
            await cursor.close()
            if not payload_changed:
                await self._restore_newer_render_revision(
                    connection,
                    item,
                    now=now,
                )
            if payload_changed:
                cursor = await connection.execute(
                    """
                    UPDATE render_outbox
                    SET state = 'pending',
                        next_attempt_at = MIN(next_attempt_at, ?),
                        updated_at = ?
                    WHERE id = ? AND state = 'sending'
                      AND payload_revision > ?
                    """,
                    (time.time(), now, item.id, item.payload_revision),
                )
                if cursor.rowcount != 1:
                    await cursor.close()
                    raise RuntimeError(f"render outbox claim was lost: {item.id}")
                await cursor.close()
        return True, False

    async def _mark_content_unavailable(self, item: OutboxItem, *, now: float) -> None:
        submission_id = item.metadata.get("submission_id")
        surface_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:content-unavailable:{item.id}:{item.payload_revision}",
            )
        )
        surface_key = f"{item.idempotency_key}:content-unavailable:{item.payload_revision}"
        surface_receipt = json.dumps(
            {
                "schema": 1,
                "render_kind": "content_unavailable",
                "finalized": True,
                "source_outbox_id": item.id,
                "submission_id": submission_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._content_store.transaction():
            async with self._database.transaction() as connection:
                current_cursor = await connection.execute(
                    """
                    SELECT 1 FROM render_outbox
                    WHERE id = ? AND state = 'sending' AND payload_revision = ?
                    """,
                    (item.id, item.payload_revision),
                )
                current = await current_cursor.fetchone()
                await current_cursor.close()
                if current is None:
                    return
                await connection.execute(
                    """
                    INSERT INTO render_outbox(
                        id, session_id, logical_seq, lane, coalesce_key,
                        idempotency_key, payload, state, attempts,
                        next_attempt_at, created_at, updated_at,
                        content_key, content_hash, render_kind, finalized,
                        source_submission_id, error_code
                    ) VALUES (?, ?, ?, 'status', ?, ?, ?, 'pending', 0, ?, ?, ?,
                              NULL, NULL, 'content_unavailable', 1, ?,
                              'content_unavailable')
                    ON CONFLICT(idempotency_key) DO NOTHING
                    """,
                    (
                        surface_id,
                        item.session_id,
                        item.logical_seq,
                        item.coalesce_key or item.id,
                        surface_key,
                        surface_receipt,
                        now,
                        now,
                        now,
                        None if submission_id is None else str(submission_id),
                    ),
                )
                await connection.execute(
                    """
                    UPDATE render_outbox
                    SET state = 'content_unavailable',
                        content_key = NULL,
                        last_error = 'content_unavailable',
                        error_code = 'content_unavailable',
                        updated_at = ?
                    WHERE id = ? AND state = 'sending' AND payload_revision = ?
                    """,
                    (now, item.id, item.payload_revision),
                )
                if submission_id is not None:
                    row_cursor = await connection.execute(
                        """
                        SELECT * FROM submission_reactions
                        WHERE submission_id = ? AND sdk_session_id = ?
                        """,
                        (str(submission_id), item.session_id),
                    )
                    row = await row_cursor.fetchone()
                    await row_cursor.close()
                    if row is not None:
                        revision = int(row["revision"]) + (
                            0
                            if str(row["desired_state"]) == "failed"
                            and str(row["resume_state"] or "") == "content_unavailable"
                            else 1
                        )
                        await connection.execute(
                            """
                            UPDATE submission_reactions
                            SET desired_state = 'failed',
                                resume_state = 'content_unavailable',
                                terminal = 1, revision = ?,
                                last_error = 'content_unavailable', updated_at = ?
                            WHERE submission_id = ?
                            """,
                            (revision, now, str(submission_id)),
                        )
                        await self._queue_submission_reaction(
                            connection,
                            row,
                            state="failed",
                            revision=revision,
                            terminal=True,
                            logical_seq=item.logical_seq,
                            now=now,
                        )
        if item.content_key is not None:
            await self._delete_content_if_unreferenced(item.content_key)

    async def _restore_newer_render_revision(
        self,
        connection: Any,
        item: OutboxItem,
        *,
        now: float,
    ) -> None:
        requested_submission = item.payload.get("submission_id")
        if requested_submission is None:
            return
        cursor = await connection.execute(
            """
            SELECT r.*, s.state AS submission_state,
                   s.terminal_at AS submission_terminal_at
            FROM submission_reactions r
            JOIN submissions s ON s.submission_id = r.submission_id
            WHERE r.sdk_session_id = ? AND r.submission_id = ?
              AND r.desired_state = 'failed' AND r.terminal = 1
            LIMIT 1
            """,
            (item.session_id, str(requested_submission)),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return
        encoded_state = row["resume_state"]
        failure_state = _render_failure_state(encoded_state)
        if (
            failure_state is None
            or failure_state.get("outbox_id") != item.id
            or failure_state.get("family") != item.idempotency_key
            or failure_state.get("submission_id") != str(requested_submission)
            or item.payload_revision <= int(failure_state.get("payload_revision", -1))
        ):
            return
        previous_state = failure_state.get("previous_state")
        if previous_state not in _REACTION_EMOJIS:
            return
        submission_state = str(row["submission_state"])
        if submission_state == "semantic_complete":
            restored_state = "succeeded"
            restored_terminal = True
            restored_resume_state = None
            restored_last_error = None
        elif submission_state in _SUBMISSION_FAILURE_STATES:
            restored_state = "failed"
            restored_terminal = True
            restored_resume_state = None
            restored_last_error = failure_state.get("previous_last_error")
        else:
            if row["submission_terminal_at"] is not None or bool(
                failure_state.get("previous_terminal")
            ):
                return
            restored_state = str(previous_state)
            restored_terminal = False
            restored_resume_state = failure_state.get("previous_resume_state")
            restored_last_error = failure_state.get("previous_last_error")
        revision = int(row["revision"]) + 1
        cursor = await connection.execute(
            """
            UPDATE submission_reactions
            SET desired_state = ?, resume_state = ?, revision = ?, terminal = ?,
                last_error = ?, updated_at = ?
            WHERE submission_id = ? AND desired_state = 'failed'
              AND terminal = 1 AND resume_state = ?
            """,
            (
                restored_state,
                restored_resume_state,
                revision,
                int(restored_terminal),
                restored_last_error,
                now,
                row["submission_id"],
                encoded_state,
            ),
        )
        restored = cursor.rowcount == 1
        await cursor.close()
        if not restored:
            return
        await self._queue_submission_reaction(
            connection,
            row,
            state=restored_state,
            revision=revision,
            terminal=restored_terminal,
            logical_seq=item.logical_seq,
            now=now,
        )

    async def _record_render_failure(
        self,
        item: OutboxItem,
        error: Exception,
        *,
        now: float,
    ) -> bool:
        error_code = fixed_error_code(error) or "render_delivery_error"
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE render_outbox SET last_error = ?, updated_at = ?
                WHERE id = ? AND state = 'blocked' AND payload_revision = ?
                """,
                (
                    error_code,
                    now,
                    item.id,
                    item.payload_revision,
                ),
            )
            recorded = cursor.rowcount == 1
            await cursor.close()
            if not recorded:
                return False
            await self._mark_submission_render_failed(
                connection,
                item,
                summary=error_code,
                now=now,
            )
        return True

    async def _attempt_render_failure_surface(
        self,
        item: OutboxItem,
        *,
        mapping: Any,
    ) -> bool:
        payload = {
            "type": "render.failure",
            "content": (
                "**Copilot could not render this response.**\n"
                "Discord rejected the live response payload. "
                "Code: `CD-RENDER-DELIVERY`."
            ),
            "status": {
                "title": "Response rendering failed",
                "detail": ("Code: CD-RENDER-DELIVERY"),
                "event_type": "render.failure",
            },
            "submission_id": item.payload.get("submission_id"),
            "finalized": True,
        }
        key = f"{item.idempotency_key}:render-failure"
        try:
            if mapping is None:
                message_id = await self._transport.send(
                    session_id=item.session_id,
                    lane="status",
                    payload=payload,
                    idempotency_key=key,
                )
            else:
                message_id = str(mapping["discord_message_id"])
                edit = self._transport.edit
                if "session_id" in inspect.signature(edit).parameters:
                    await edit(
                        session_id=item.session_id,
                        message_id=message_id,
                        lane="status",
                        payload=payload,
                        idempotency_key=key,
                    )
                else:
                    await edit(
                        message_id=message_id,
                        lane="status",
                        payload=payload,
                    )
        except (RenderDeliveryError, OSError, TimeoutError):
            return False
        await self._database.execute(
            """
            INSERT INTO render_messages(
                session_id, logical_key, discord_message_id, content_hash, finalized
            ) VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(session_id, logical_key) DO UPDATE SET
                discord_message_id = excluded.discord_message_id,
                content_hash = excluded.content_hash,
                finalized = 1
            """,
            (
                item.session_id,
                item.coalesce_key or item.id,
                message_id,
                _payload_hash(payload),
            ),
        )
        return True

    async def _mark_submission_render_failed(
        self,
        connection: Any,
        item: OutboxItem,
        *,
        summary: str,
        now: float,
    ) -> None:
        requested_submission = item.payload.get("submission_id")
        if requested_submission is None:
            return
        cursor = await connection.execute(
            """
            SELECT r.* FROM submission_reactions r
            WHERE r.sdk_session_id = ? AND r.submission_id = ?
            LIMIT 1
            """,
            (item.session_id, str(requested_submission)),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return
        existing_failure = _render_failure_state(row["resume_state"])
        if str(row["desired_state"]) == "failed" and existing_failure is None:
            return
        if (
            existing_failure is not None
            and existing_failure.get("outbox_id") == item.id
            and existing_failure.get("family") == item.idempotency_key
        ):
            previous_state = str(existing_failure["previous_state"])
            previous_resume_state = existing_failure.get("previous_resume_state")
            previous_terminal = bool(existing_failure.get("previous_terminal"))
            previous_last_error = existing_failure.get("previous_last_error")
        else:
            previous_state = str(row["desired_state"])
            previous_resume_state = row["resume_state"]
            previous_terminal = bool(row["terminal"])
            previous_last_error = row["last_error"]
        revision = int(row["revision"])
        if str(row["desired_state"]) != "failed":
            revision += 1
        resume_state = _encode_render_failure_state(
            item,
            submission_id=str(row["submission_id"]),
            previous_state=previous_state,
            previous_resume_state=previous_resume_state,
            previous_terminal=previous_terminal,
            previous_last_error=previous_last_error,
        )
        await connection.execute(
            """
            UPDATE submission_reactions
            SET desired_state = 'failed', terminal = 1, revision = ?,
                resume_state = ?, last_error = ?, updated_at = ?
            WHERE submission_id = ?
            """,
            (
                revision,
                resume_state,
                summary,
                now,
                row["submission_id"],
            ),
        )
        await self._queue_submission_reaction(
            connection,
            row,
            state="failed",
            revision=revision,
            terminal=True,
            logical_seq=item.logical_seq,
            now=now,
        )

    async def _queue_submission_reaction(
        self,
        connection: Any,
        row: Any,
        *,
        state: str,
        revision: int,
        terminal: bool,
        logical_seq: int,
        now: float,
    ) -> None:
        render_payload = {
            "type": "reaction_state",
            "submission_id": str(row["submission_id"]),
            "source_channel_id": str(row["source_channel_id"]),
            "source_message_id": str(row["source_message_id"]),
            "state": state,
            "emoji": _REACTION_EMOJIS[state],
            "reaction_revision": revision,
            "generation": int(row["runtime_generation"]),
            "fence_token": int(row["owner_fence_token"]),
            "finalized": terminal,
        }
        key = f"reaction:{row['submission_id']}"
        outbox_id = str(uuid.uuid5(uuid.NAMESPACE_URL, key))
        content_key = opaque_content_key("render-outbox", outbox_id)
        incoming_hash = payload_sha256(render_payload)
        cursor = await connection.execute(
            """
            SELECT content_hash FROM render_outbox
            WHERE idempotency_key = ?
            """,
            (key,),
        )
        existing = await cursor.fetchone()
        await cursor.close()
        if existing is not None:
            if (
                str(existing["content_hash"]) == incoming_hash
                and self._content_store.get(
                    content_key,
                    expected_hash=incoming_hash,
                )
                is not None
            ):
                return
        ref = self._content_store.put(
            render_payload,
            key=content_key,
        )
        payload = render_payload_receipt(render_payload, ref)
        await connection.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at,
                content_key, content_hash, render_kind, finalized,
                source_submission_id, source_channel_id, source_message_id,
                reaction_state
            ) VALUES (?, ?, ?, 'reaction', ?, ?, ?, 'pending', 0, ?, ?, ?,
                     ?, ?, 'reaction_state', ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                logical_seq = excluded.logical_seq,
                payload = excluded.payload,
                content_key = excluded.content_key,
                content_hash = excluded.content_hash,
                render_kind = excluded.render_kind,
                finalized = excluded.finalized,
                source_submission_id = excluded.source_submission_id,
                source_channel_id = excluded.source_channel_id,
                source_message_id = excluded.source_message_id,
                reaction_state = excluded.reaction_state,
                payload_revision = render_outbox.payload_revision + 1,
                state = CASE WHEN render_outbox.state = 'sending'
                             THEN 'sending' ELSE 'pending' END,
                next_attempt_at = excluded.next_attempt_at,
                updated_at = excluded.updated_at
            """,
            (
                outbox_id,
                str(row["sdk_session_id"]),
                logical_seq,
                key,
                key,
                payload,
                now,
                now,
                now,
                ref.key,
                ref.sha256,
                int(terminal),
                str(row["submission_id"]),
                str(row["source_channel_id"]),
                str(row["source_message_id"]),
                state,
            ),
        )

    async def _deliver_admission_reaction(
        self,
        item: OutboxItem,
        *,
        now: float,
        live_clock: bool,
    ) -> tuple[bool, bool]:
        payload = item.payload
        transport_key = (
            f"{item.idempotency_key}:payload:{item.payload_revision}:{_payload_hash(payload)[:16]}"
        )
        try:
            await self._transport.reaction(
                session_id=item.session_id,
                payload=payload,
                idempotency_key=transport_key,
            )
        except RenderRateLimited as error:
            retry_now = time.time() if live_clock else now
            await self._retry(
                item,
                next_attempt_at=retry_now + error.retry_after,
                now=retry_now,
            )
            return False, True
        except RenderTransientError:
            retry_now = time.time() if live_clock else now
            if item.attempts >= self._max_transient_attempts:
                blocked = await self._block(item, now=retry_now)
                return False, not blocked
            await self._retry(
                item,
                next_attempt_at=retry_now + min(2 ** (item.attempts - 1), 30),
                now=retry_now,
            )
            return False, True
        except RenderPermanentError as error:
            failure_now = time.time() if live_clock else now
            await self._database.execute(
                """
                UPDATE render_outbox
                SET last_error = ?, updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (
                    fixed_error_code(error),
                    failure_now,
                    item.id,
                    item.payload_revision,
                ),
            )
            blocked = await self._block(item, now=failure_now)
            return False, not blocked
        await self._finish_admission_reaction_claim(item, now=now)
        return True, False

    async def _finish_admission_reaction_claim(
        self,
        item: OutboxItem,
        *,
        now: float,
    ) -> None:
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'sent', content_key = NULL,
                    last_error = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (now, item.id, item.payload_revision),
            )
            finished = cursor.rowcount == 1
            await cursor.close()
            if finished:
                return
            cursor = await connection.execute(
                """
                SELECT payload, payload_revision
                FROM render_outbox
                WHERE id = ? AND state = 'sending' AND payload_revision > ?
                """,
                (item.id, item.payload_revision),
            )
            newer = await cursor.fetchone()
            await cursor.close()
            if newer is None:
                raise RuntimeError(f"admission reaction outbox claim was lost: {item.id}")
            newer_payload = json.loads(str(newer["payload"]))
            next_state = "superseded" if newer_payload.get("superseded") else "pending"
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = ?,
                    content_key = CASE WHEN ? = 'superseded' THEN NULL
                                       ELSE content_key END,
                    next_attempt_at = MIN(next_attempt_at, ?),
                    updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (
                    next_state,
                    next_state,
                    now,
                    now,
                    item.id,
                    int(newer["payload_revision"]),
                ),
            )
            transitioned = cursor.rowcount == 1
            await cursor.close()
            if not transitioned:
                raise RuntimeError(f"admission reaction outbox claim was lost: {item.id}")

    async def _deliver_reaction(
        self,
        item: OutboxItem,
        *,
        now: float,
        live_clock: bool,
    ) -> tuple[bool, bool]:
        payload = item.payload
        current = await self._database.fetchone(
            """
            SELECT r.desired_state, r.revision, r.delivered_state, r.terminal,
                   r.runtime_generation,
                   r.owner_fence_token, b.runtime_generation AS binding_generation,
                   b.owner_fence_token AS binding_fence,
                   l.fence_token AS lease_fence, l.expires_at AS lease_expires_at
            FROM submission_reactions r
            LEFT JOIN session_bindings b ON b.sdk_session_id = r.sdk_session_id
            LEFT JOIN session_owner_leases l
              ON l.sdk_session_id = b.sdk_session_id
             AND l.fence_token = b.owner_fence_token
            WHERE r.submission_id = ? AND r.sdk_session_id = ?
            """,
            (payload.get("submission_id"), item.session_id),
        )
        desired_is_current = (
            current is not None
            and str(current["desired_state"]) == str(payload.get("state"))
            and int(current["revision"]) == int(payload.get("reaction_revision", -1))
        )
        owner_is_current = (
            current is not None
            and current["lease_fence"] is not None
            and current["lease_expires_at"] is not None
            and float(current["lease_expires_at"]) > time.time()
        )
        terminal_is_current = (
            desired_is_current
            and bool(current["terminal"])
            and bool(payload.get("finalized"))
            and int(current["runtime_generation"]) == int(payload.get("generation", -1))
            and int(current["owner_fence_token"]) == int(payload.get("fence_token", -1))
        )
        nonterminal_is_current = (
            desired_is_current
            and owner_is_current
            and not bool(current["terminal"])
            and not bool(payload.get("finalized"))
            and int(current["runtime_generation"]) == int(payload.get("generation", -1))
            and int(current["owner_fence_token"]) == int(payload.get("fence_token", -1))
            and int(current["binding_generation"]) == int(payload.get("generation", -1))
            and int(current["binding_fence"]) == int(payload.get("fence_token", -1))
        )
        is_current = terminal_is_current or nonterminal_is_current
        if not is_current:
            if desired_is_current and not bool(current["terminal"]) and not owner_is_current:
                retry_now = time.time() if live_clock else now
                await self._retry(
                    item,
                    next_attempt_at=retry_now + 1,
                    now=retry_now,
                )
                return False, True
            await recover_reaction_outbox(
                self._database,
                content_store=self._content_store,
            )
            await self._finish_reaction_claim(item, now=now, delivered=False)
            return False, False
        transport_key = (
            f"{item.idempotency_key}:payload:{item.payload_revision}:{_payload_hash(payload)[:16]}"
        )
        transport_payload = dict(payload)
        previous_state = current["delivered_state"]
        previous_emoji = (
            None if previous_state is None else _REACTION_EMOJIS.get(str(previous_state))
        )
        if previous_emoji is not None and previous_emoji != payload.get("emoji"):
            transport_payload["previous_emoji"] = previous_emoji
        try:
            await self._transport.reaction(
                session_id=item.session_id,
                payload=transport_payload,
                idempotency_key=transport_key,
            )
        except RenderRateLimited as error:
            retry_now = time.time() if live_clock else now
            await self._retry(
                item,
                next_attempt_at=retry_now + error.retry_after,
                now=retry_now,
            )
            return False, True
        except RenderTransientError:
            retry_now = time.time() if live_clock else now
            if item.attempts >= self._max_transient_attempts:
                blocked = await self._block(item, now=retry_now)
                return False, not blocked
            await self._retry(
                item,
                next_attempt_at=retry_now + min(2 ** (item.attempts - 1), 30),
                now=retry_now,
            )
            return False, True
        except RenderPermanentError as error:
            await self._database.execute(
                """
                UPDATE submission_reactions
                SET last_error = ?, updated_at = ?
                WHERE submission_id = ? AND revision = ?
                """,
                (
                    fixed_error_code(error),
                    time.time() if live_clock else now,
                    payload.get("submission_id"),
                    payload.get("reaction_revision"),
                ),
            )
            blocked = await self._block(
                item,
                now=time.time() if live_clock else now,
            )
            return False, not blocked
        await self._finish_reaction_claim(item, now=now, delivered=True)
        return True, False

    async def _finish_reaction_claim(
        self,
        item: OutboxItem,
        *,
        now: float,
        delivered: bool,
    ) -> None:
        payload = item.payload
        async with self._database.transaction() as connection:
            if delivered:
                await connection.execute(
                    """
                    UPDATE submission_reactions
                    SET delivered_state = ?, delivered_revision = ?,
                        last_error = NULL, updated_at = ?
                    WHERE submission_id = ? AND desired_state = ?
                      AND revision = ? AND runtime_generation = ?
                      AND owner_fence_token = ?
                    """,
                    (
                        payload["state"],
                        int(payload["reaction_revision"]),
                        now,
                        payload["submission_id"],
                        payload["state"],
                        int(payload["reaction_revision"]),
                        int(payload["generation"]),
                        int(payload["fence_token"]),
                    ),
                )
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'sent', content_key = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (now, item.id, item.payload_revision),
            )
            finished = cursor.rowcount == 1
            await cursor.close()
            if finished:
                return
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'pending', next_attempt_at = MIN(next_attempt_at, ?),
                    updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision > ?
                """,
                (now, now, item.id, item.payload_revision),
            )
            restored = cursor.rowcount == 1
            await cursor.close()
            if not restored:
                raise RuntimeError(f"reaction outbox claim was lost: {item.id}")

    async def _retry(
        self,
        item: OutboxItem,
        *,
        next_attempt_at: float,
        now: float,
    ) -> bool:
        return await self._transition_failed_claim(
            item,
            state="pending",
            next_attempt_at=next_attempt_at,
            now=now,
        )

    async def _block(self, item: OutboxItem, *, now: float) -> bool:
        return await self._transition_failed_claim(
            item,
            state="blocked",
            next_attempt_at=None,
            now=now,
        )

    async def _transition_failed_claim(
        self,
        item: OutboxItem,
        *,
        state: str,
        next_attempt_at: float | None,
        now: float,
    ) -> bool:
        async with self._database.transaction() as connection:
            if next_attempt_at is None:
                cursor = await connection.execute(
                    """
                    UPDATE render_outbox
                    SET state = ?, updated_at = ?
                    WHERE id = ? AND state = 'sending'
                      AND payload_revision = ?
                    """,
                    (state, now, item.id, item.payload_revision),
                )
            else:
                cursor = await connection.execute(
                    """
                    UPDATE render_outbox
                    SET state = ?, next_attempt_at = ?, updated_at = ?
                    WHERE id = ? AND state = 'sending'
                      AND payload_revision = ?
                    """,
                    (
                        state,
                        next_attempt_at,
                        now,
                        item.id,
                        item.payload_revision,
                    ),
                )
            transitioned = cursor.rowcount == 1
            await cursor.close()
            if transitioned:
                return True
            cursor = await connection.execute(
                """
                UPDATE render_outbox
                SET state = CASE
                        WHEN lane = 'admission_reaction'
                          AND COALESCE(json_extract(payload, '$.superseded'), 0) = 1
                        THEN 'superseded'
                        ELSE 'pending'
                    END,
                    next_attempt_at = MIN(next_attempt_at, ?),
                    updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND payload_revision > ?
                """,
                (now, now, item.id, item.payload_revision),
            )
            requeued = cursor.rowcount == 1
            await cursor.close()
            if not requeued:
                raise RuntimeError(f"render outbox claim was lost: {item.id}")
            return False


def _encode_render_failure_state(
    item: OutboxItem,
    *,
    submission_id: str,
    previous_state: str,
    previous_resume_state: Any,
    previous_terminal: bool,
    previous_last_error: Any,
) -> str:
    return json.dumps(
        {
            "kind": "render_failed",
            "outbox_id": item.id,
            "family": item.idempotency_key,
            "payload_revision": item.payload_revision,
            "submission_id": submission_id,
            "previous_state": previous_state,
            "previous_resume_state": previous_resume_state,
            "previous_terminal": previous_terminal,
            "previous_last_error": previous_last_error,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _render_failure_state(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or not value.startswith("{"):
        return None
    try:
        state = json.loads(value)
    except ValueError:
        return None
    if not isinstance(state, dict) or state.get("kind") != "render_failed":
        return None
    return state


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _with_reconstructed_reaction(item: OutboxItem) -> OutboxItem:
    state = str(item.metadata.get("state", "accepted"))
    if state not in _REACTION_EMOJIS:
        state = "accepted"
    payload = {
        "type": ("admission_reaction" if item.lane == "admission_reaction" else "reaction_state"),
        "submission_id": item.metadata.get("submission_id"),
        "source_channel_id": item.metadata.get("source_channel_id"),
        "source_message_id": item.metadata.get("source_message_id"),
        "state": state,
        "emoji": _REACTION_EMOJIS[state],
        "reaction_revision": item.metadata.get("reaction_revision", 1),
        "generation": item.metadata.get("generation", 0),
        "fence_token": item.metadata.get("fence_token", 0),
        "finalized": bool(item.metadata.get("finalized")),
    }
    previous_state = item.metadata.get("previous_reaction_state")
    if previous_state is not None:
        previous_emoji = _REACTION_EMOJIS.get(str(previous_state))
        if previous_emoji is not None and previous_emoji != payload["emoji"]:
            payload["previous_emoji"] = previous_emoji
    return OutboxItem(
        id=item.id,
        session_id=item.session_id,
        logical_seq=item.logical_seq,
        lane=item.lane,
        coalesce_key=item.coalesce_key,
        idempotency_key=item.idempotency_key,
        payload=payload,
        metadata=item.metadata,
        content_key=item.content_key,
        content_hash=item.content_hash,
        payload_revision=item.payload_revision,
        attempts=item.attempts,
    )


def _with_reconstructed_content_unavailable(item: OutboxItem) -> OutboxItem:
    return OutboxItem(
        id=item.id,
        session_id=item.session_id,
        logical_seq=item.logical_seq,
        lane=item.lane,
        coalesce_key=item.coalesce_key,
        idempotency_key=item.idempotency_key,
        payload=_fixed_content_unavailable_payload(item.metadata.get("submission_id")),
        metadata=item.metadata,
        content_key=None,
        content_hash=None,
        payload_revision=item.payload_revision,
        attempts=item.attempts,
    )


def _fixed_content_unavailable_payload(submission_id: Any) -> dict[str, Any]:
    return {
        "type": "render.content_unavailable",
        "content": (
            "**Response content unavailable**\n"
            "The process restarted before volatile content could be delivered. "
            "Code: `CD-CONTENT-UNAVAILABLE`."
        ),
        "status": {
            "title": "Response content unavailable",
            "detail": "Code: CD-CONTENT-UNAVAILABLE",
            "event_type": "render.content_unavailable",
        },
        "submission_id": submission_id,
        "finalized": True,
    }


def admission_reaction_key(source_channel_id: str, source_message_id: str) -> str:
    return f"admission-reaction:{source_channel_id}:{source_message_id}"


async def queue_admission_reaction(
    database: Database,
    *,
    source_channel_id: str,
    source_message_id: str,
    state: str,
    emoji: str,
    session_id: str | None = None,
    previous_emoji: str | None = None,
    now: float | None = None,
    content_store: VolatileContentStore | None = None,
) -> None:
    timestamp = time.time() if now is None else now
    key = admission_reaction_key(source_channel_id, source_message_id)
    outbox_id = str(uuid.uuid5(uuid.NAMESPACE_URL, key))
    payload: dict[str, Any] = {
        "type": "admission_reaction",
        "source_channel_id": source_channel_id,
        "source_message_id": source_message_id,
        "state": state,
        "emoji": emoji,
        "finalized": state == "failed",
    }
    store = content_store or database.content_store
    with store.transaction():
        async with database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT lane, payload, state, content_hash, finalized,
                       reaction_state
                FROM render_outbox WHERE idempotency_key = ?
                """,
                (key,),
            )
            existing = await cursor.fetchone()
            await cursor.close()
            if existing is not None:
                existing_payload = json.loads(str(existing["payload"]))
                if str(existing["lane"]) != "admission_reaction" or bool(
                    existing_payload.get("superseded")
                ):
                    return
                if bool(existing["finalized"]) and str(existing["reaction_state"]) != state:
                    return
            previous_state = (
                None if previous_emoji is None else _REACTION_STATES_BY_EMOJI.get(previous_emoji)
            )
            if (
                previous_state is None
                and existing is not None
                and existing["reaction_state"] is not None
                and str(existing["reaction_state"]) != state
            ):
                previous_state = str(existing["reaction_state"])
                previous_emoji = _REACTION_EMOJIS.get(previous_state)
            if previous_emoji is not None and previous_emoji != emoji:
                payload["previous_emoji"] = previous_emoji
            incoming_hash = payload_sha256(payload)
            if (
                existing is not None
                and str(existing["content_hash"]) == incoming_hash
                and str(existing["state"]) not in {"blocked", "failed"}
            ):
                return
            ref = store.put(
                payload,
                key=opaque_content_key("render-outbox", outbox_id),
            )
            serialized = render_payload_receipt(payload, ref)
            await connection.execute(
                """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at,
                content_key, content_hash, render_kind, finalized,
                source_channel_id, source_message_id, reaction_state,
                previous_reaction_state
            ) VALUES (?, ?, 0, 'admission_reaction', ?, ?, ?, 'pending', 0, ?, ?, ?,
                     ?, ?, 'admission_reaction', ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                session_id = COALESCE(?, render_outbox.session_id),
                payload = excluded.payload,
                content_key = excluded.content_key,
                content_hash = excluded.content_hash,
                render_kind = excluded.render_kind,
                finalized = excluded.finalized,
                source_channel_id = excluded.source_channel_id,
                source_message_id = excluded.source_message_id,
                reaction_state = excluded.reaction_state,
                previous_reaction_state = COALESCE(
                    excluded.previous_reaction_state,
                    CASE
                        WHEN render_outbox.reaction_state != excluded.reaction_state
                        THEN render_outbox.reaction_state
                        ELSE render_outbox.previous_reaction_state
                    END
                ),
                payload_revision = render_outbox.payload_revision + 1,
                state = CASE
                    WHEN render_outbox.state = 'sending' THEN 'sending'
                    ELSE 'pending'
                END,
                next_attempt_at = MIN(
                    render_outbox.next_attempt_at,
                    excluded.next_attempt_at
                ),
                last_error = NULL,
                updated_at = excluded.updated_at
            WHERE render_outbox.lane = 'admission_reaction'
              AND COALESCE(json_extract(render_outbox.payload, '$.superseded'), 0) = 0
              AND (
                  render_outbox.content_hash != excluded.content_hash
                  OR render_outbox.state IN ('blocked', 'failed')
              )
            """,
                (
                    outbox_id,
                    session_id or f"admission:{source_channel_id}:{source_message_id}",
                    key,
                    key,
                    serialized,
                    timestamp,
                    timestamp,
                    timestamp,
                    ref.key,
                    ref.sha256,
                    int(state == "failed"),
                    source_channel_id,
                    source_message_id,
                    state,
                    previous_state,
                    session_id,
                ),
            )


async def supersede_admission_reaction(
    connection: Any,
    *,
    session_id: str,
    logical_seq: int,
    source_channel_id: str,
    source_message_id: str,
    now: float,
    content_store: VolatileContentStore,
) -> None:
    cursor = await connection.execute(
        """
        SELECT content_key FROM render_outbox
        WHERE lane = 'admission_reaction'
          AND state IN ('pending', 'blocked', 'sent')
          AND content_key IS NOT NULL
          AND source_channel_id = ?
          AND source_message_id = ?
          AND COALESCE(json_extract(payload, '$.superseded'), 0) = 0
        """,
        (source_channel_id, source_message_id),
    )
    retired_keys = [str(row["content_key"]) for row in await cursor.fetchall()]
    await cursor.close()
    await connection.execute(
        """
        UPDATE render_outbox
        SET session_id = ?,
            logical_seq = ?,
            payload = json_set(
                payload,
                '$.superseded', 1,
                '$.submission_owned', 1
            ),
            payload_revision = payload_revision + 1,
            state = CASE
                WHEN state = 'sending' THEN 'sending'
                ELSE 'superseded'
            END,
            content_key = CASE
                WHEN state = 'sending' THEN content_key
                ELSE NULL
            END,
            next_attempt_at = MIN(next_attempt_at, ?),
            updated_at = ?
        WHERE lane = 'admission_reaction'
          AND state IN ('pending', 'sending', 'blocked', 'sent')
          AND source_channel_id = ?
          AND source_message_id = ?
          AND COALESCE(json_extract(payload, '$.superseded'), 0) = 0
        """,
        (
            session_id,
            logical_seq,
            now,
            now,
            source_channel_id,
            source_message_id,
        ),
    )
    for key in retired_keys:
        content_store.delete(key)


async def recover_reaction_outbox(
    database: Database,
    *,
    now: float | None = None,
    content_store: VolatileContentStore | None = None,
) -> int:
    timestamp = time.time() if now is None else now
    store = content_store or database.content_store
    rows = await database.fetchall(
        """
        SELECT r.*, b.runtime_generation AS binding_generation,
               b.owner_fence_token AS binding_fence,
               l.expires_at AS lease_expires_at
        FROM submission_reactions r
        LEFT JOIN session_bindings b ON b.sdk_session_id = r.sdk_session_id
        LEFT JOIN session_owner_leases l
          ON l.sdk_session_id = b.sdk_session_id
         AND l.fence_token = b.owner_fence_token
        WHERE (
            r.delivered_state IS NULL
            OR r.delivered_state != r.desired_state
            OR r.delivered_revision != r.revision
          )
          AND (
            r.terminal = 1
            OR (
              b.owner_fence_token IS NOT NULL
              AND l.expires_at > ?
            )
          )
        """,
        (timestamp,),
    )
    async with database.transaction() as connection:
        for row in rows:
            terminal = bool(row["terminal"])
            generation = int(row["runtime_generation"] if terminal else row["binding_generation"])
            fence_token = int(row["owner_fence_token"] if terminal else row["binding_fence"])
            submission_id = str(row["submission_id"])
            state = str(row["desired_state"])
            await connection.execute(
                """
                UPDATE submission_reactions
                SET runtime_generation = ?, owner_fence_token = ?, updated_at = ?
                WHERE submission_id = ?
                """,
                (generation, fence_token, timestamp, submission_id),
            )
            render_payload = {
                "type": "reaction_state",
                "submission_id": submission_id,
                "source_channel_id": str(row["source_channel_id"]),
                "source_message_id": str(row["source_message_id"]),
                "state": state,
                "emoji": _REACTION_EMOJIS[state],
                "reaction_revision": int(row["revision"]),
                "generation": generation,
                "fence_token": fence_token,
                "finalized": terminal,
            }
            key = f"reaction:{submission_id}"
            outbox_id = str(uuid.uuid5(uuid.NAMESPACE_URL, key))
            content_key = opaque_content_key("render-outbox", outbox_id)
            incoming_hash = payload_sha256(render_payload)
            cursor = await connection.execute(
                """
                SELECT content_hash FROM render_outbox
                WHERE idempotency_key = ?
                """,
                (key,),
            )
            existing = await cursor.fetchone()
            await cursor.close()
            if (
                existing is not None
                and str(existing["content_hash"]) == incoming_hash
                and store.get(content_key, expected_hash=incoming_hash) is not None
            ):
                continue
            ref = store.put(
                render_payload,
                key=content_key,
            )
            payload = render_payload_receipt(render_payload, ref)
            await connection.execute(
                """
                INSERT INTO render_outbox(
                    id, session_id, logical_seq, lane, coalesce_key,
                    idempotency_key, payload, state, attempts,
                    next_attempt_at, created_at, updated_at,
                    content_key, content_hash, render_kind, finalized,
                    source_submission_id, source_channel_id, source_message_id,
                    reaction_state
                ) VALUES (?, ?, 0, 'reaction', ?, ?, ?, 'pending', 0, ?, ?, ?,
                         ?, ?, 'reaction_state', ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    payload = excluded.payload,
                    content_key = excluded.content_key,
                    content_hash = excluded.content_hash,
                    render_kind = excluded.render_kind,
                    finalized = excluded.finalized,
                    source_submission_id = excluded.source_submission_id,
                    source_channel_id = excluded.source_channel_id,
                    source_message_id = excluded.source_message_id,
                    reaction_state = excluded.reaction_state,
                    payload_revision = render_outbox.payload_revision + 1,
                    state = CASE
                        WHEN render_outbox.state = 'sending' THEN 'sending'
                        ELSE 'pending'
                    END,
                    next_attempt_at = excluded.next_attempt_at,
                    updated_at = excluded.updated_at
                """,
                (
                    outbox_id,
                    str(row["sdk_session_id"]),
                    key,
                    key,
                    payload,
                    timestamp,
                    timestamp,
                    timestamp,
                    ref.key,
                    ref.sha256,
                    int(terminal),
                    submission_id,
                    str(row["source_channel_id"]),
                    str(row["source_message_id"]),
                    state,
                ),
            )
    return len(rows)
