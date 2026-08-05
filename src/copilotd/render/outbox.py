from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Protocol

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
        message_id: str,
        lane: str,
        payload: dict[str, Any],
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
    attempts: int


class RenderOutboxDispatcher:
    def __init__(
        self,
        database: Database,
        transport: RenderTransport,
        *,
        claim_timeout_seconds: float = 60,
        max_transient_attempts: int = 3,
    ) -> None:
        self._database = database
        self._transport = transport
        self._claim_timeout_seconds = claim_timeout_seconds
        self._max_transient_attempts = max_transient_attempts
        self._last_delivery: dict[tuple[str, str], float] = {}

    async def dispatch_once(
        self,
        *,
        limit: int = 20,
        now: float | None = None,
    ) -> int:
        timestamp = time.time() if now is None else now
        items = await self._claim(limit=limit, now=timestamp)
        delivered = 0
        for item in items:
            if await self._deliver(item, now=timestamp):
                delivered += 1
        return delivered

    async def _claim(self, *, limit: int, now: float) -> list[OutboxItem]:
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
                SELECT * FROM render_outbox
                WHERE state = 'pending' AND next_attempt_at <= ?
                ORDER BY session_id, logical_seq, created_at
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
                    items.append(
                        OutboxItem(
                            id=row["id"],
                            session_id=row["session_id"],
                            logical_seq=row["logical_seq"],
                            lane=row["lane"],
                            coalesce_key=row["coalesce_key"],
                            idempotency_key=row["idempotency_key"],
                            payload=json.loads(row["payload"]),
                            attempts=row["attempts"] + 1,
                        )
                    )
            return items

    async def _deliver(self, item: OutboxItem, *, now: float) -> bool:
        logical_key = item.coalesce_key or item.id
        payload_hash = _payload_hash(item.payload)
        mapping = await self._database.fetchone(
            """
            SELECT * FROM render_messages
            WHERE session_id = ? AND logical_key = ?
            """,
            (item.session_id, logical_key),
        )
        finalized = bool(item.payload.get("finalized"))
        cadence = 4.0 if item.lane == "taskdeck" else 1.0 if item.lane == "assistant_stream" else 0
        delivery_key = (item.session_id, item.lane)
        last_delivery = self._last_delivery.get(delivery_key)
        elapsed = float("inf") if last_delivery is None else time.monotonic() - last_delivery
        if cadence and not finalized and elapsed < cadence:
            await self._retry(
                item,
                next_attempt_at=time.time() + cadence - elapsed,
                now=now,
            )
            return False
        try:
            if mapping is not None and mapping["content_hash"] == payload_hash:
                message_id = mapping["discord_message_id"]
            elif mapping is None:
                message_id = await self._transport.send(
                    session_id=item.session_id,
                    lane=item.lane,
                    payload=item.payload,
                    idempotency_key=item.idempotency_key,
                )
            else:
                message_id = mapping["discord_message_id"]
                await self._transport.edit(
                    message_id=message_id,
                    lane=item.lane,
                    payload=item.payload,
                )
        except RenderRateLimited as error:
            await self._retry(item, next_attempt_at=now + error.retry_after, now=now)
            return False
        except RenderTransientError:
            if item.attempts >= self._max_transient_attempts:
                await self._block(item, now=now)
            else:
                await self._retry(
                    item,
                    next_attempt_at=now + min(2 ** (item.attempts - 1), 30),
                    now=now,
                )
            return False
        except RenderPermanentError:
            await self._block(item, now=now)
            return False

        self._last_delivery[delivery_key] = time.monotonic()
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
                WHERE id = ? AND state = 'sending'
                """,
                (now, item.id),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise RuntimeError(f"render outbox claim was lost: {item.id}")
            await cursor.close()
        return True

    async def _retry(
        self,
        item: OutboxItem,
        *,
        next_attempt_at: float,
        now: float,
    ) -> None:
        await self._database.execute(
            """
            UPDATE render_outbox
            SET state = 'pending', next_attempt_at = ?, updated_at = ?
            WHERE id = ? AND state = 'sending'
            """,
            (next_attempt_at, now, item.id),
        )

    async def _block(self, item: OutboxItem, *, now: float) -> None:
        await self._database.execute(
            """
            UPDATE render_outbox
            SET state = 'blocked', updated_at = ?
            WHERE id = ? AND state = 'sending'
            """,
            (now, item.id),
        )


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
