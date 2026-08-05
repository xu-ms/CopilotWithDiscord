from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from collections.abc import Callable
from typing import Protocol

from copilotd.storage.database import Database


class QuiesceSessions(Protocol):
    async def begin_service_quiesce(
        self,
        on_violation: Callable[[], None],
    ) -> None: ...

    async def end_service_quiesce(self) -> None: ...

    def service_quiesce_metrics(self) -> tuple[int, int]: ...


class ServiceControlWorker:
    """Acknowledges durable restart fences only after ingress is drained."""

    def __init__(
        self,
        database: Database,
        sessions: QuiesceSessions,
        *,
        process_generation: str,
        poll_seconds: float = 0.05,
    ) -> None:
        self._database = database
        self._sessions = sessions
        self._pid = os.getpid()
        self._process_generation = process_generation
        self._poll_seconds = poll_seconds
        self._active_fence_id: str | None = None
        self._acknowledged_violations = 0

    async def run(self) -> None:
        try:
            while True:
                await self._poll_once()
                await asyncio.sleep(self._poll_seconds)
        finally:
            if self._active_fence_id is not None:
                row = await self._database.fetchone(
                    "SELECT state FROM service_admission_fences WHERE fence_id = ?",
                    (self._active_fence_id,),
                )
                if row is None or row["state"] != "committed":
                    await self._sessions.end_service_quiesce()

    async def _poll_once(self) -> None:
        if self._active_fence_id is None:
            row = await self._database.fetchone(
                """
                SELECT * FROM service_admission_fences
                WHERE state = 'requested'
                ORDER BY requested_at
                LIMIT 1
                """
            )
            if row is None:
                return
            if (
                int(row["expected_pid"]) != self._pid
                or str(row["expected_generation"]) != self._process_generation
            ):
                await self._database.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'released', released_at = ?,
                        detail = ?
                    WHERE fence_id = ? AND state = 'requested'
                    """,
                    (
                        time.time(),
                        json.dumps(
                            {"reason": "process_identity_mismatch"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        row["fence_id"],
                    ),
                )
                return
            self._active_fence_id = str(row["fence_id"])
            await self._sessions.begin_service_quiesce(
                self._record_violation_sync,
            )
            await self._acknowledge_when_stable()
            return

        row = await self._database.fetchone(
            "SELECT state FROM service_admission_fences WHERE fence_id = ?",
            (self._active_fence_id,),
        )
        if row is None or row["state"] == "released":
            await self._sessions.end_service_quiesce()
            self._active_fence_id = None
            return
        if row["state"] == "acknowledged":
            depth, violations = self._sessions.service_quiesce_metrics()
            if depth or violations != self._acknowledged_violations:
                await self._database.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'violated', ingress_depth = ?,
                        violation_count = ?, detail = ?
                    WHERE fence_id = ? AND state = 'acknowledged'
                    """,
                    (
                        depth,
                        violations,
                        json.dumps(
                            {"reason": "post_ack_ingress"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        self._active_fence_id,
                    ),
                )

    async def _acknowledge_when_stable(self) -> None:
        stable_samples = 0
        last_violations = -1
        while stable_samples < 2:
            depth, violations = self._sessions.service_quiesce_metrics()
            if depth == 0 and violations == last_violations:
                stable_samples += 1
            else:
                stable_samples = 0
            last_violations = violations
            if depth:
                await asyncio.sleep(self._poll_seconds)
                continue
            await asyncio.sleep(self._poll_seconds)
        self._acknowledged_violations = last_violations
        await self._database.execute(
            """
            UPDATE service_admission_fences
            SET state = 'acknowledged', acknowledged_at = ?,
                ingress_depth = 0, violation_count = ?, detail = ?
            WHERE fence_id = ? AND state = 'requested'
              AND expected_pid = ? AND expected_generation = ?
            """,
            (
                time.time(),
                last_violations,
                json.dumps(
                    {"drained": True},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                self._active_fence_id,
                self._pid,
                self._process_generation,
            ),
        )

    def _record_violation_sync(self) -> None:
        if self._active_fence_id is None:
            return
        try:
            connection = sqlite3.connect(str(self._database.path), timeout=0.25)
            try:
                connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'violated',
                        violation_count = violation_count + 1,
                        detail = ?
                    WHERE fence_id = ? AND state = 'acknowledged'
                    """,
                    (
                        json.dumps(
                            {"reason": "post_ack_ingress"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        self._active_fence_id,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
        except sqlite3.Error:
            # The async poll path observes the in-memory violation count too.
            return
