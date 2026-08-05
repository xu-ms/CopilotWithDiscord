from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass

from aiosqlite import Connection

from copilotd.storage.database import Database


@dataclass(frozen=True, slots=True)
class RecoveryInventoryReport:
    run_id: str
    expired_owner_sessions: int
    unknown_operations: int
    unknown_submissions: int
    unknown_background_observations: int
    expired_interactions: int
    orphaned_liveness: int
    unknown_runtime_schedules: int
    unknown_creation_intents: int
    dispatch_unknown_runs: int
    target_unknown_runs: int
    retry_wait_runs: int
    outcome_unknown_runs: int


class StartupRecoveryInventory:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def run(self, *, now: float | None = None) -> RecoveryInventoryReport:
        timestamp = time.time() if now is None else now
        run_id = str(uuid.uuid4())
        counts: dict[str, int] = {}
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO startup_recovery_runs(
                    run_id, started_at, status, detail
                ) VALUES (?, ?, 'running', '{}')
                """,
                (run_id, timestamp),
            )
            cursor = await connection.execute(
                """
                SELECT sdk_session_id FROM session_owner_leases
                WHERE expires_at <= ?
                """,
                (timestamp,),
            )
            expired = [str(row["sdk_session_id"]) for row in await cursor.fetchall()]
            await cursor.close()
            counts["expired_owner_sessions"] = len(expired)

            counts["unknown_operations"] = await _update_count(
                connection,
                """
                UPDATE session_operations
                SET state = 'unknown', error_code = 'startup_recovery',
                    settled_at = COALESCE(settled_at, ?)
                WHERE state IN ('pending', 'started')
                  AND NOT EXISTS (
                      SELECT 1 FROM session_owner_leases AS owner
                      WHERE owner.sdk_session_id = session_operations.sdk_session_id
                        AND owner.fence_token = session_operations.owner_fence_token
                        AND owner.expires_at > ?
                  )
                """,
                (timestamp, timestamp),
            )
            if expired:
                placeholders = ", ".join("?" for _ in expired)
                counts["unknown_submissions"] = await _update_count(
                    connection,
                    f"""
                    UPDATE submissions
                    SET state = CASE
                            WHEN state IN ('submitting', 'submitted')
                            THEN 'submitted_unknown'
                            ELSE 'outcome_unknown'
                        END,
                        terminal_at = COALESCE(terminal_at, ?)
                    WHERE sdk_session_id IN ({placeholders})
                      AND state IN (
                          'submitting', 'submitted', 'submitted_unknown',
                          'observed_active', 'loop_idle', 'continuation_expected'
                      )
                    """,
                    (timestamp, *expired),
                )
                counts["unknown_background_observations"] = await _update_count(
                    connection,
                    f"""
                    UPDATE background_observations
                    SET observed_state = 'unknown', last_progress_at = ?
                    WHERE sdk_session_id IN ({placeholders})
                      AND terminal_evidence IS NULL
                      AND observed_state IN ('running', 'idle')
                    """,
                    (timestamp, *expired),
                )
                counts["expired_interactions"] = await _update_count(
                    connection,
                    f"""
                    UPDATE pending_interactions
                    SET state = 'expired', updated_at = ?
                    WHERE sdk_session_id IN ({placeholders}) AND state = 'pending'
                    """,
                    (timestamp, *expired),
                )
                counts["orphaned_liveness"] = await _update_count(
                    connection,
                    f"""
                    UPDATE liveness_leases
                    SET state = 'orphaned', refreshed_at = ?, released_at = ?
                    WHERE sdk_session_id IN ({placeholders}) AND state = 'active'
                    """,
                    (timestamp, timestamp, *expired),
                )
                counts["unknown_runtime_schedules"] = await _update_count(
                    connection,
                    f"""
                    UPDATE runtime_schedules
                    SET state = 'unknown', updated_at = ?
                    WHERE sdk_session_id IN ({placeholders}) AND state = 'active'
                    """,
                    (timestamp, *expired),
                )
                await connection.execute(
                    f"""
                    UPDATE session_bindings
                    SET attachment_state = 'recovery_unknown',
                        permission_posture = 'unknown',
                        updated_at = ?, row_version = row_version + 1
                    WHERE sdk_session_id IN ({placeholders})
                      AND attachment_state IN (
                          'creating', 'resuming', 'attached', 'disconnecting'
                      )
                    """,
                    (timestamp, *expired),
                )
            else:
                for key in (
                    "unknown_submissions",
                    "unknown_background_observations",
                    "expired_interactions",
                    "orphaned_liveness",
                    "unknown_runtime_schedules",
                ):
                    counts[key] = 0

            counts["unknown_creation_intents"] = await _update_count(
                connection,
                """
                UPDATE session_creation_intents
                SET state = 'unknown', updated_at = ?
                WHERE state = 'creating'
                """,
                (timestamp,),
            )
            counts["target_unknown_runs"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'target_unknown', updated_at = ?
                WHERE status IN ('claimed', 'dispatching', 'submitting')
                  AND session_create_started_at IS NOT NULL
                  AND result_thread_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                  )
                """,
                (timestamp,),
            )
            counts["dispatch_unknown_runs"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'dispatch_unknown', updated_at = ?
                WHERE status IN ('dispatching', 'submitting')
                  AND send_started_at IS NOT NULL
                  AND accepted_message_id IS NULL
                """,
                (timestamp,),
            )
            counts["retry_wait_runs"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'retry_wait', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE status IN ('claimed', 'dispatching', 'submitting')
                  AND send_started_at IS NULL
                  AND session_create_started_at IS NULL
                  AND COALESCE(lease_expires_at, 0) <= ?
                """,
                (timestamp, timestamp),
            )
            counts["outcome_unknown_runs"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'outcome_unknown', updated_at = ?
                WHERE status IN ('accepted', 'waiting')
                  AND EXISTS (
                      SELECT 1 FROM submissions
                      WHERE submission_id = schedule_runs.result_submission_id
                        AND state IN ('submitted_unknown', 'outcome_unknown')
                  )
                """,
                (timestamp,),
            )
            report = RecoveryInventoryReport(run_id=run_id, **counts)
            await connection.execute(
                """
                UPDATE startup_recovery_runs
                SET completed_at = ?, status = 'completed', detail = ?
                WHERE run_id = ?
                """,
                (
                    timestamp,
                    json.dumps(asdict(report), sort_keys=True),
                    run_id,
                ),
            )
        return report


async def _update_count(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> int:
    cursor = await connection.execute(statement, parameters)
    count = cursor.rowcount
    await cursor.close()
    return count
