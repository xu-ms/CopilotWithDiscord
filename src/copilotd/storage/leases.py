from __future__ import annotations

import time
from dataclasses import dataclass

from aiosqlite import Connection, Row

from copilotd.storage.database import Database

OWNER_LEASE_TTL_SECONDS = 60.0
OWNER_LEASE_RENEW_SECONDS = 15.0
MUTATION_HEADROOM_SECONDS = 40.0
RENEWAL_JITTER_MARGIN_SECONDS = 5.0


class OwnerConflict(RuntimeError):
    pass


class FenceLost(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OwnerLease:
    sdk_session_id: str
    owner_id: str
    fence_token: int
    acquired_at: float
    renewed_at: float
    expires_at: float


class OwnerLeaseStore:
    """Cross-process lease with a monotonically increasing fencing token."""

    def __init__(
        self,
        database: Database,
        ttl_seconds: float = OWNER_LEASE_TTL_SECONDS,
    ) -> None:
        if ttl_seconds < MUTATION_HEADROOM_SECONDS + RENEWAL_JITTER_MARGIN_SECONDS:
            raise ValueError("owner lease TTL must exceed mutation headroom by the jitter margin")
        self._database = database
        self._ttl_seconds = ttl_seconds

    async def acquire(
        self,
        sdk_session_id: str,
        owner_id: str,
        *,
        now: float | None = None,
    ) -> OwnerLease:
        timestamp = time.time() if now is None else now
        expires_at = timestamp + self._ttl_seconds
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT * FROM session_owner_leases WHERE sdk_session_id = ?",
                (sdk_session_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                lease = OwnerLease(
                    sdk_session_id=sdk_session_id,
                    owner_id=owner_id,
                    fence_token=1,
                    acquired_at=timestamp,
                    renewed_at=timestamp,
                    expires_at=expires_at,
                )
                await connection.execute(
                    """
                    INSERT INTO session_owner_leases(
                        sdk_session_id, owner_id, fence_token,
                        acquired_at, renewed_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lease.sdk_session_id,
                        lease.owner_id,
                        lease.fence_token,
                        lease.acquired_at,
                        lease.renewed_at,
                        lease.expires_at,
                    ),
                )
                return lease

            current = _row_to_lease(row)
            if current.owner_id != owner_id and current.expires_at > timestamp:
                raise OwnerConflict(
                    f"session {sdk_session_id} is owned by {current.owner_id} "
                    f"until {current.expires_at}"
                )

            if current.owner_id == owner_id and current.expires_at > timestamp:
                fence_token = current.fence_token
                acquired_at = current.acquired_at
            else:
                fence_token = current.fence_token + 1
                acquired_at = timestamp

            lease = OwnerLease(
                sdk_session_id=sdk_session_id,
                owner_id=owner_id,
                fence_token=fence_token,
                acquired_at=acquired_at,
                renewed_at=timestamp,
                expires_at=expires_at,
            )
            await connection.execute(
                """
                UPDATE session_owner_leases
                SET owner_id = ?, fence_token = ?, acquired_at = ?,
                    renewed_at = ?, expires_at = ?
                WHERE sdk_session_id = ?
                """,
                (
                    lease.owner_id,
                    lease.fence_token,
                    lease.acquired_at,
                    lease.renewed_at,
                    lease.expires_at,
                    lease.sdk_session_id,
                ),
            )
            if fence_token != current.fence_token:
                await _orphan_previous_owners(
                    connection,
                    sdk_session_id=sdk_session_id,
                    current_fence_token=fence_token,
                    timestamp=timestamp,
                    error_code="owner_fence_takeover",
                )
            return lease

    async def renew(
        self,
        lease: OwnerLease,
        *,
        now: float | None = None,
    ) -> OwnerLease:
        timestamp = time.time() if now is None else now
        expires_at = timestamp + self._ttl_seconds
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE session_owner_leases
                SET renewed_at = ?, expires_at = ?
                WHERE sdk_session_id = ? AND owner_id = ? AND fence_token = ?
                  AND expires_at > ?
                """,
                (
                    timestamp,
                    expires_at,
                    lease.sdk_session_id,
                    lease.owner_id,
                    lease.fence_token,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                raise FenceLost(f"owner fence lost for session {lease.sdk_session_id}")
        return OwnerLease(
            sdk_session_id=lease.sdk_session_id,
            owner_id=lease.owner_id,
            fence_token=lease.fence_token,
            acquired_at=lease.acquired_at,
            renewed_at=timestamp,
            expires_at=expires_at,
        )

    async def release(self, lease: OwnerLease, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE session_owner_leases
                SET renewed_at = ?, expires_at = ?
                WHERE sdk_session_id = ? AND owner_id = ? AND fence_token = ?
                  AND expires_at > ?
                """,
                (
                    timestamp,
                    timestamp,
                    lease.sdk_session_id,
                    lease.owner_id,
                    lease.fence_token,
                    timestamp,
                ),
            )
            if cursor.rowcount != 1:
                raise FenceLost(f"owner fence lost for session {lease.sdk_session_id}")

    async def current(self, sdk_session_id: str) -> OwnerLease | None:
        row = await self._database.fetchone(
            "SELECT * FROM session_owner_leases WHERE sdk_session_id = ?",
            (sdk_session_id,),
        )
        return None if row is None else _row_to_lease(row)

    async def is_current(self, lease: OwnerLease, *, now: float | None = None) -> bool:
        timestamp = time.time() if now is None else now
        current = await self.current(lease.sdk_session_id)
        return (
            current is not None
            and current.owner_id == lease.owner_id
            and current.fence_token == lease.fence_token
            and current.expires_at > timestamp
        )

    async def has_mutation_headroom(
        self,
        lease: OwnerLease,
        *,
        minimum_seconds: float = MUTATION_HEADROOM_SECONDS,
        now: float | None = None,
    ) -> bool:
        timestamp = time.time() if now is None else now
        current = await self.current(lease.sdk_session_id)
        return (
            current is not None
            and current.owner_id == lease.owner_id
            and current.fence_token == lease.fence_token
            and current.expires_at - timestamp >= minimum_seconds
        )


async def _orphan_previous_owners(
    connection: Connection,
    *,
    sdk_session_id: str,
    current_fence_token: int,
    timestamp: float,
    error_code: str,
) -> None:
    await connection.execute(
        """
        UPDATE session_operations
        SET state = 'unknown', error_code = ?, settled_at = ?
        WHERE sdk_session_id = ? AND owner_fence_token != ?
          AND state IN ('pending', 'started')
        """,
        (error_code, timestamp, sdk_session_id, current_fence_token),
    )
    await connection.execute(
        """
        UPDATE submissions
        SET state = CASE
                WHEN state IN ('submitting', 'submitted') THEN 'submitted_unknown'
                ELSE 'outcome_unknown'
            END,
            terminal_at = COALESCE(terminal_at, ?)
        WHERE sdk_session_id = ?
          AND state IN (
              'submitting', 'submitted', 'submitted_unknown',
              'observed_active', 'loop_idle', 'continuation_expected'
          )
        """,
        (timestamp, sdk_session_id),
    )
    await connection.execute(
        """
        UPDATE message_queue
        SET state = 'submitted_unknown', updated_at = ?
        WHERE id IN (
            SELECT submission_id FROM submissions
            WHERE sdk_session_id = ? AND state = 'submitted_unknown'
        ) AND state = 'submitting'
        """,
        (timestamp, sdk_session_id),
    )
    await connection.execute(
        """
        UPDATE background_observations
        SET observed_state = 'unknown', last_progress_at = ?
        WHERE sdk_session_id = ? AND terminal_evidence IS NULL
          AND observed_state IN ('running', 'idle')
        """,
        (timestamp, sdk_session_id),
    )
    await connection.execute(
        """
        UPDATE pending_interactions
        SET state = 'expired', updated_at = ?
        WHERE sdk_session_id = ? AND state = 'pending'
          AND owner_fence_token != ?
        """,
        (timestamp, sdk_session_id, current_fence_token),
    )
    await connection.execute(
        """
        UPDATE runtime_schedules
        SET state = 'unknown', updated_at = ?
        WHERE sdk_session_id = ? AND state = 'active'
        """,
        (timestamp, sdk_session_id),
    )
    await connection.execute(
        """
        UPDATE liveness_leases
        SET state = 'orphaned', refreshed_at = ?, released_at = ?
        WHERE sdk_session_id = ? AND owner_fence_token != ? AND state = 'active'
        """,
        (timestamp, timestamp, sdk_session_id, current_fence_token),
    )


def _row_to_lease(row: Row) -> OwnerLease:
    return OwnerLease(
        sdk_session_id=row["sdk_session_id"],
        owner_id=row["owner_id"],
        fence_token=row["fence_token"],
        acquired_at=row["acquired_at"],
        renewed_at=row["renewed_at"],
        expires_at=row["expires_at"],
    )
