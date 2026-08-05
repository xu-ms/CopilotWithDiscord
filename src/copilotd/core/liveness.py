from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from enum import StrEnum

from aiosqlite import Row

from copilotd.storage.database import Database


class LivenessKind(StrEnum):
    SUBMISSION = "submission"
    BACKGROUND = "observed_background"
    INTERACTION = "interaction"


@dataclass(frozen=True, slots=True)
class LivenessLease:
    sdk_session_id: str
    lease_id: str
    kind: LivenessKind
    source_id: str
    runtime_generation: int
    owner_fence_token: int
    acquired_at: float
    refreshed_at: float


class LivenessController:
    """Persists evidence-based liveness without an idle TTL."""

    def __init__(
        self,
        database: Database,
        *,
        sdk_session_id: str,
        runtime_generation: int,
        owner_fence_token: int,
    ) -> None:
        self._database = database
        self.sdk_session_id = sdk_session_id
        self.runtime_generation = runtime_generation
        self.owner_fence_token = owner_fence_token

    async def orphan_previous_generations(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'orphaned', released_at = ?
                WHERE sdk_session_id = ? AND state = 'active'
                  AND (
                    runtime_generation != ?
                    OR owner_fence_token != ?
                  )
                """,
                (
                    timestamp,
                    self.sdk_session_id,
                    self.runtime_generation,
                    self.owner_fence_token,
                ),
            )
            count = cursor.rowcount
            await cursor.close()
        return count

    async def acquire(
        self,
        kind: LivenessKind,
        source_id: str,
        *,
        now: float | None = None,
    ) -> LivenessLease:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM liveness_leases
                WHERE sdk_session_id = ? AND kind = ? AND source_id = ? AND state = 'active'
                """,
                (self.sdk_session_id, kind.value, source_id),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is not None:
                if (
                    row["runtime_generation"] != self.runtime_generation
                    or row["owner_fence_token"] != self.owner_fence_token
                ):
                    raise RuntimeError("active liveness source belongs to a stale generation")
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET refreshed_at = ?
                    WHERE sdk_session_id = ? AND lease_id = ?
                    """,
                    (timestamp, self.sdk_session_id, row["lease_id"]),
                )
                return _row_to_lease(row, refreshed_at=timestamp)

            lease = LivenessLease(
                sdk_session_id=self.sdk_session_id,
                lease_id=str(uuid.uuid4()),
                kind=kind,
                source_id=source_id,
                runtime_generation=self.runtime_generation,
                owner_fence_token=self.owner_fence_token,
                acquired_at=timestamp,
                refreshed_at=timestamp,
            )
            await connection.execute(
                """
                INSERT INTO liveness_leases(
                    sdk_session_id, lease_id, kind, source_id,
                    runtime_generation, owner_fence_token, state,
                    acquired_at, refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    lease.sdk_session_id,
                    lease.lease_id,
                    lease.kind.value,
                    lease.source_id,
                    lease.runtime_generation,
                    lease.owner_fence_token,
                    lease.acquired_at,
                    lease.refreshed_at,
                ),
            )
            return lease

    async def refresh(
        self,
        lease: LivenessLease,
        *,
        now: float | None = None,
    ) -> LivenessLease:
        self._validate_generation(lease)
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE liveness_leases
                SET refreshed_at = ?
                WHERE sdk_session_id = ? AND lease_id = ? AND state = 'active'
                  AND runtime_generation = ? AND owner_fence_token = ?
                """,
                (
                    timestamp,
                    lease.sdk_session_id,
                    lease.lease_id,
                    lease.runtime_generation,
                    lease.owner_fence_token,
                ),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise RuntimeError(f"liveness lease is no longer active: {lease.lease_id}")
            await cursor.close()
        return LivenessLease(
            sdk_session_id=lease.sdk_session_id,
            lease_id=lease.lease_id,
            kind=lease.kind,
            source_id=lease.source_id,
            runtime_generation=lease.runtime_generation,
            owner_fence_token=lease.owner_fence_token,
            acquired_at=lease.acquired_at,
            refreshed_at=timestamp,
        )

    async def release(self, lease: LivenessLease, *, now: float | None = None) -> None:
        self._validate_generation(lease)
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND lease_id = ? AND state = 'active'
                  AND runtime_generation = ? AND owner_fence_token = ?
                """,
                (
                    timestamp,
                    timestamp,
                    lease.sdk_session_id,
                    lease.lease_id,
                    lease.runtime_generation,
                    lease.owner_fence_token,
                ),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise RuntimeError(f"liveness lease is no longer active: {lease.lease_id}")
            await cursor.close()

    async def active(self) -> list[LivenessLease]:
        rows = await self._database.fetchall(
            """
            SELECT * FROM liveness_leases
            WHERE sdk_session_id = ? AND state = 'active'
              AND runtime_generation = ? AND owner_fence_token = ?
            ORDER BY acquired_at, lease_id
            """,
            (
                self.sdk_session_id,
                self.runtime_generation,
                self.owner_fence_token,
            ),
        )
        return [_row_to_lease(row) for row in rows]

    def _validate_generation(self, lease: LivenessLease) -> None:
        if (
            lease.sdk_session_id != self.sdk_session_id
            or lease.runtime_generation != self.runtime_generation
            or lease.owner_fence_token != self.owner_fence_token
        ):
            raise RuntimeError("liveness lease belongs to a stale generation")


def _row_to_lease(row: Row, *, refreshed_at: float | None = None) -> LivenessLease:
    return LivenessLease(
        sdk_session_id=row["sdk_session_id"],
        lease_id=row["lease_id"],
        kind=LivenessKind(row["kind"]),
        source_id=row["source_id"],
        runtime_generation=row["runtime_generation"],
        owner_fence_token=row["owner_fence_token"],
        acquired_at=row["acquired_at"],
        refreshed_at=row["refreshed_at"] if refreshed_at is None else refreshed_at,
    )
