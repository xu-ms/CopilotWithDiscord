from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from copilotd.core.spill_artifacts import (
    confirm_and_collect_tool_spills,
    garbage_collect_tool_spills,
)
from copilotd.storage.database import Database


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
    payload: dict[str, Any]
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
        spill_gc_interval_seconds: float = 60 * 60,
    ) -> None:
        self._database = database
        self._transport = transport
        self._claim_timeout_seconds = claim_timeout_seconds
        self._max_transient_attempts = max_transient_attempts
        self._spill_gc_interval_seconds = spill_gc_interval_seconds
        self._next_spill_gc = 0.0
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
            monotonic_now = time.monotonic()
            if monotonic_now >= self._next_spill_gc:
                await garbage_collect_tool_spills(self._database, now=timestamp)
                self._next_spill_gc = monotonic_now + self._spill_gc_interval_seconds
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
                    if was_delivered:
                        delivered += 1
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

    async def _claim(
        self,
        *,
        limit: int,
        now: float,
        claimed_ids: list[str],
    ) -> list[OutboxItem]:
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE render_outbox
                SET state = 'pending', updated_at = ?
                WHERE state = 'sending' AND updated_at <= ?
                """,
                (now, now - self._claim_timeout_seconds),
            )
            await connection.execute(
                """
                UPDATE render_outbox AS older
                SET state = 'superseded', updated_at = ?
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
                      AND (
                        earlier.logical_seq < candidate.logical_seq
                        OR (
                            earlier.logical_seq = candidate.logical_seq
                            AND earlier.created_at < candidate.created_at
                        )
                      )
                  )
                ORDER BY candidate.session_id,
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
                    items.append(
                        OutboxItem(
                            id=row["id"],
                            session_id=row["session_id"],
                            logical_seq=row["logical_seq"],
                            lane=row["lane"],
                            coalesce_key=row["coalesce_key"],
                            idempotency_key=row["idempotency_key"],
                            payload=json.loads(row["payload"]),
                            payload_revision=int(row["payload_revision"]),
                            attempts=row["attempts"] + 1,
                        )
                    )
            return items

    async def _deliver(
        self,
        item: OutboxItem,
        *,
        now: float,
        live_clock: bool,
    ) -> tuple[bool, bool]:
        if item.lane == "reaction":
            return await self._deliver_reaction(
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
        except RenderTransientError:
            retry_now = time.time() if live_clock else now
            if item.attempts >= self._max_transient_attempts:
                blocked = await self._block(item, now=retry_now)
                return False, not blocked
            else:
                await self._retry(
                    item,
                    next_attempt_at=retry_now + min(2 ** (item.attempts - 1), 30),
                    now=retry_now,
                )
                return False, True
        except RenderPermanentError:
            blocked = await self._block(
                item,
                now=time.time() if live_clock else now,
            )
            return False, not blocked

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
                SET state = 'sent', updated_at = ?
                WHERE id = ? AND state = 'sending' AND payload_revision = ?
                """,
                (now, item.id, item.payload_revision),
            )
            payload_changed = cursor.rowcount == 0
            await cursor.close()
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
        if finalized and not payload_changed:
            spill_paths = [
                str(attachment["path"])
                for attachment in item.payload.get("attachments", [])
                if isinstance(attachment, dict) and isinstance(attachment.get("path"), str)
            ]
            await confirm_and_collect_tool_spills(
                self._database,
                spill_paths,
                session_id=item.session_id,
            )
        return True, False

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
            SELECT r.desired_state, r.revision, r.runtime_generation,
                   r.owner_fence_token, b.runtime_generation AS binding_generation,
                   b.owner_fence_token AS binding_fence,
                   l.fence_token AS lease_fence, l.expires_at AS lease_expires_at
            FROM submission_reactions r
            JOIN session_bindings b ON b.sdk_session_id = r.sdk_session_id
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
        is_current = (
            desired_is_current
            and owner_is_current
            and int(current["runtime_generation"]) == int(payload.get("generation", -1))
            and int(current["owner_fence_token"]) == int(payload.get("fence_token", -1))
            and int(current["binding_generation"]) == int(payload.get("generation", -1))
            and int(current["binding_fence"]) == int(payload.get("fence_token", -1))
        )
        if not is_current:
            if desired_is_current and not owner_is_current:
                retry_now = time.time() if live_clock else now
                await self._retry(
                    item,
                    next_attempt_at=retry_now + 1,
                    now=retry_now,
                )
                return False, True
            await recover_reaction_outbox(self._database)
            await self._finish_reaction_claim(item, now=now, delivered=False)
            return False, False
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
            await self._database.execute(
                """
                UPDATE submission_reactions
                SET last_error = ?, updated_at = ?
                WHERE submission_id = ? AND revision = ?
                """,
                (
                    f"{type(error).__name__}: {error}",
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
                SET state = 'sent', updated_at = ?
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
                SET state = 'pending',
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


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def recover_reaction_outbox(database: Database, *, now: float | None = None) -> int:
    timestamp = time.time() if now is None else now
    rows = await database.fetchall(
        """
        SELECT r.*, b.runtime_generation AS binding_generation,
               b.owner_fence_token AS binding_fence
        FROM submission_reactions r
        JOIN session_bindings b ON b.sdk_session_id = r.sdk_session_id
        JOIN session_owner_leases l
          ON l.sdk_session_id = b.sdk_session_id
         AND l.fence_token = b.owner_fence_token
        WHERE b.owner_fence_token IS NOT NULL
          AND l.expires_at > ?
          AND (
            r.delivered_state IS NULL
            OR r.delivered_state != r.desired_state
            OR r.delivered_revision != r.revision
          )
        """,
        (timestamp,),
    )
    emojis = {
        "accepted": "👀",
        "reasoning": "🧠",
        "action": "🛠️",
        "unresolved": "❓",
        "succeeded": "✅",
        "failed": "❌",
    }
    async with database.transaction() as connection:
        for row in rows:
            generation = int(row["binding_generation"])
            fence_token = int(row["binding_fence"])
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
            payload = json.dumps(
                {
                    "type": "reaction_state",
                    "submission_id": submission_id,
                    "source_channel_id": str(row["source_channel_id"]),
                    "source_message_id": str(row["source_message_id"]),
                    "state": state,
                    "emoji": emojis[state],
                    "reaction_revision": int(row["revision"]),
                    "generation": generation,
                    "fence_token": fence_token,
                    "finalized": bool(row["terminal"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            key = f"reaction:{submission_id}"
            await connection.execute(
                """
                INSERT INTO render_outbox(
                    id, session_id, logical_seq, lane, coalesce_key,
                    idempotency_key, payload, state, attempts,
                    next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 0, 'reaction', ?, ?, ?, 'pending', 0, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    payload = excluded.payload,
                    payload_revision = render_outbox.payload_revision + 1,
                    state = CASE
                        WHEN render_outbox.state = 'sending' THEN 'sending'
                        ELSE 'pending'
                    END,
                    next_attempt_at = excluded.next_attempt_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
                    str(row["sdk_session_id"]),
                    key,
                    key,
                    payload,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )
    return len(rows)
