from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from copilotd.storage.database import Database

TransportPing = Callable[[], Awaitable[dict[str, object]]]


@dataclass(frozen=True, slots=True)
class SuspectExecution:
    sdk_session_id: str
    last_progress_at: float
    silent_seconds: float
    ping: dict[str, object]


class ExecutionStallMonitor:
    def __init__(
        self,
        database: Database,
        transport_ping: TransportPing,
        *,
        stall_seconds: float = 10 * 60,
        interval_seconds: float = 60,
    ) -> None:
        self._database = database
        self._transport_ping = transport_ping
        self._stall_seconds = stall_seconds
        self._interval_seconds = interval_seconds

    async def run(self) -> None:
        while True:
            await self.check()
            await asyncio.sleep(self._interval_seconds)

    async def check(self, *, now: float | None = None) -> list[SuspectExecution]:
        timestamp = time.time() if now is None else now
        rows = await self._database.fetchall(
            """
            SELECT binding.sdk_session_id,
                   MAX(
                       COALESCE(binding.last_event_at, 0),
                       COALESCE(binding.activity_observed_at, 0),
                       COALESCE((
                           SELECT MAX(observed_at) FROM reconciliation_state AS snapshot
                           WHERE snapshot.sdk_session_id = binding.sdk_session_id
                       ), 0),
                       COALESCE((
                           SELECT MAX(last_progress_at)
                           FROM background_observations AS background
                           WHERE background.sdk_session_id = binding.sdk_session_id
                       ), 0),
                       COALESCE((
                           SELECT MAX(created_at) FROM submissions AS submission
                           WHERE submission.sdk_session_id = binding.sdk_session_id
                       ), 0)
                   ) AS last_progress_at
            FROM session_bindings AS binding
            WHERE binding.attachment_state = 'attached'
              AND (
                  EXISTS (
                      SELECT 1 FROM submissions AS submission
                      WHERE submission.sdk_session_id = binding.sdk_session_id
                        AND submission.state IN (
                            'submitting', 'submitted', 'observed_active',
                            'loop_idle', 'continuation_expected'
                        )
                  )
                  OR EXISTS (
                      SELECT 1 FROM background_observations AS background
                      WHERE background.sdk_session_id = binding.sdk_session_id
                        AND background.observed_state IN ('running', 'idle')
                  )
                  OR EXISTS (
                      SELECT 1 FROM pending_interactions AS interaction
                      WHERE interaction.sdk_session_id = binding.sdk_session_id
                        AND interaction.state = 'pending'
                  )
              )
            GROUP BY binding.sdk_session_id
            """
        )
        active_ids = {str(row["sdk_session_id"]) for row in rows}
        if active_ids:
            placeholders = ", ".join("?" for _ in active_ids)
            await self._database.execute(
                f"""
                UPDATE execution_health
                SET state = 'healthy', suspect_since = NULL
                WHERE sdk_session_id NOT IN ({placeholders})
                """,
                tuple(sorted(active_ids)),
            )
        else:
            await self._database.execute(
                "UPDATE execution_health SET state = 'healthy', suspect_since = NULL"
            )

        suspects: list[SuspectExecution] = []
        for row in rows:
            session_id = str(row["sdk_session_id"])
            last_progress = float(row["last_progress_at"])
            silent_seconds = max(0.0, timestamp - last_progress)
            if last_progress <= 0 or silent_seconds < self._stall_seconds:
                await self._mark_healthy(session_id, last_progress, timestamp)
                continue
            current = await self._database.fetchone(
                "SELECT state, last_ping_at FROM execution_health WHERE sdk_session_id = ?",
                (session_id,),
            )
            should_ping = (
                current is None
                or current["state"] != "suspect"
                or current["last_ping_at"] is None
                or timestamp - float(current["last_ping_at"]) >= self._interval_seconds
            )
            if should_ping:
                try:
                    ping = await self._transport_ping()
                except Exception as error:
                    ping = {
                        "status": "error",
                        "error_type": type(error).__name__,
                        "message": str(error),
                    }
            else:
                ping = {"status": "not_repeated"}
            newly_suspect = current is None or current["state"] != "suspect"
            await self._database.execute(
                """
                INSERT INTO execution_health(
                    sdk_session_id, state, last_progress_at, suspect_since,
                    last_ping_at, detail, updated_at
                ) VALUES (?, 'suspect', ?, ?, ?, ?, ?)
                ON CONFLICT(sdk_session_id) DO UPDATE SET
                    state = 'suspect',
                    last_progress_at = excluded.last_progress_at,
                    suspect_since = COALESCE(
                        execution_health.suspect_since,
                        excluded.suspect_since
                    ),
                    last_ping_at = CASE
                        WHEN ? THEN excluded.last_ping_at
                        ELSE execution_health.last_ping_at
                    END,
                    detail = excluded.detail,
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    last_progress,
                    timestamp,
                    timestamp if should_ping else None,
                    json.dumps(ping, sort_keys=True),
                    timestamp,
                    should_ping,
                ),
            )
            if newly_suspect:
                await self._database.execute(
                    """
                    INSERT INTO runtime_incidents(
                        timestamp, runtime_generation, session_id, kind, detail
                    )
                    SELECT ?, runtime_generation, sdk_session_id,
                           'active_execution_stall_suspect', ?
                    FROM session_bindings WHERE sdk_session_id = ?
                    """,
                    (
                        timestamp,
                        json.dumps(
                            {
                                "last_progress_at": last_progress,
                                "silent_seconds": silent_seconds,
                                "ping": ping,
                            },
                            sort_keys=True,
                        ),
                        session_id,
                    ),
                )
            suspects.append(
                SuspectExecution(
                    sdk_session_id=session_id,
                    last_progress_at=last_progress,
                    silent_seconds=silent_seconds,
                    ping=ping,
                )
            )
        return suspects

    async def _mark_healthy(
        self,
        session_id: str,
        last_progress_at: float,
        now: float,
    ) -> None:
        await self._database.execute(
            """
            INSERT INTO execution_health(
                sdk_session_id, state, last_progress_at, detail, updated_at
            ) VALUES (?, 'healthy', ?, '{}', ?)
            ON CONFLICT(sdk_session_id) DO UPDATE SET
                state = 'healthy',
                last_progress_at = excluded.last_progress_at,
                suspect_since = NULL,
                detail = '{}',
                updated_at = excluded.updated_at
            """,
            (session_id, last_progress_at, now),
        )
