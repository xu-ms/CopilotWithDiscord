from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from copilotd.ops.contracts import SERVICE_CONTROL_PROTOCOL_VERSION
from copilotd.storage.database import Database


def fence_marker_paths(
    database_path: Path,
    fence_id: str,
) -> tuple[Path, Path]:
    prefix = database_path.with_name(f".{database_path.name}.{fence_id}")
    return (
        prefix.with_name(prefix.name + ".loss"),
        prefix.with_name(prefix.name + ".accounting-failure"),
    )


def fence_pending_marker_directory(
    database_path: Path,
    fence_id: str,
) -> Path:
    return database_path.with_name(f".{database_path.name}.{fence_id}.pending")


class QuiesceSessions(Protocol):
    async def begin_service_quiesce(
        self,
        on_producer: Callable[[str], None],
        on_loss: Callable[[str], None],
    ) -> None: ...

    async def drain_service_quiesce(self) -> None: ...

    async def end_service_quiesce(self) -> None: ...

    def service_quiesce_metrics(self) -> tuple[int, int]: ...


class ServiceControlWorker:
    """Runs the process half of the durable restart transaction."""

    def __init__(
        self,
        database: Database,
        sessions: QuiesceSessions,
        *,
        process_generation: str,
        process_started_at: float,
        handoff_token: str = "",
        poll_seconds: float = 0.05,
        quiesce_timeout_seconds: float = 15,
        rollback_timeout_seconds: float = 2,
        terminate_process: Callable[[int], None] = os._exit,
    ) -> None:
        self._database = database
        self._sessions = sessions
        self._pid = os.getpid()
        self._process_generation = process_generation
        self._process_started_at = process_started_at
        self._handoff_token_hash = hashlib.sha256(handoff_token.encode()).hexdigest()
        self._poll_seconds = poll_seconds
        self._quiesce_timeout_seconds = quiesce_timeout_seconds
        self._rollback_timeout_seconds = rollback_timeout_seconds
        self._terminate_process = terminate_process
        self._active_fence_id: str | None = None

    async def run(self) -> None:
        try:
            while True:
                await self._poll_once()
                await asyncio.sleep(self._poll_seconds)
        finally:
            if self._active_fence_id is not None:
                state = await self._fence_state(self._active_fence_id)
                if state not in {"prepared", "committed"}:
                    await self._rollback_quiesce(self._active_fence_id)

    async def _poll_once(self) -> None:
        if self._active_fence_id is None:
            row = await self._database.fetchone(
                """
                SELECT * FROM service_admission_fences
                WHERE state = 'requested' AND protocol_version = ?
                ORDER BY requested_at
                LIMIT 1
                """,
                (SERVICE_CONTROL_PROTOCOL_VERSION,),
            )
            if row is None:
                return
            if (
                int(row["expected_pid"]) != self._pid
                or str(row["expected_generation"]) != self._process_generation
                or abs(float(row["expected_process_started_at"]) - self._process_started_at) > 5
                or str(row["handoff_token_hash"]) != self._handoff_token_hash
            ):
                await self._release_mismatched_fence(row)
                return
            self._active_fence_id = str(row["fence_id"])
            await self._begin_and_acknowledge(row)
            return

        state = await self._fence_state(self._active_fence_id)
        if state in {"prepared", "committed"}:
            self._terminate_process(75)
            return
        if state is None or state in {"released", "violated"}:
            await self._rollback_quiesce(self._active_fence_id)
            return
        if state == "acknowledged":
            depth, _ = self._sessions.service_quiesce_metrics()
            if depth:
                self._record_producer_sync("post_ack_depth")

    async def _begin_and_acknowledge(self, row: Any) -> None:
        fence_id = str(row["fence_id"])
        deadline = time.monotonic() + self._quiesce_timeout_seconds
        begin = asyncio.create_task(
            self._sessions.begin_service_quiesce(
                self._record_producer_sync,
                self._record_loss_sync,
            ),
            name=f"service-quiesce:{fence_id}",
        )
        if not await self._await_or_abort(begin, fence_id, deadline):
            return
        while True:
            if not await self._retry_requested_fence(fence_id, deadline):
                return
            producer_before = await self._producer_count(fence_id)
            depth_before, violations_before = self._sessions.service_quiesce_metrics()
            if violations_before:
                await self._mark_quiesce_failed(
                    fence_id,
                    "post_quiesce_producer",
                )
                await self._rollback_quiesce(fence_id)
                return
            journal_before = await self._journal_id()
            markers_before = self._failure_marker_epoch(fence_id)
            if markers_before:
                await self._mark_quiesce_failed(
                    fence_id,
                    "durable_loss_or_accounting_failure",
                )
                await self._rollback_quiesce(fence_id)
                return
            drain = asyncio.create_task(
                self._sessions.drain_service_quiesce(),
                name=f"service-final-drain:{fence_id}",
            )
            if not await self._await_or_abort(drain, fence_id, deadline):
                return
            producer_after = await self._producer_count(fence_id)
            depth_after, violations_after = self._sessions.service_quiesce_metrics()
            if depth_after or violations_after:
                if depth_after:
                    self._record_producer_sync("drain_depth")
                await self._mark_quiesce_failed(
                    fence_id,
                    "post_quiesce_activity",
                )
                await self._rollback_quiesce(fence_id)
                return
            journal_after = await self._journal_id()
            markers_after = self._failure_marker_epoch(fence_id)
            if markers_after:
                await self._mark_quiesce_failed(
                    fence_id,
                    "durable_loss_or_accounting_failure",
                )
                await self._rollback_quiesce(fence_id)
                return
            if (
                depth_before != 0
                or producer_before != producer_after
                or journal_before != journal_after
                or markers_before != markers_after
            ):
                if not await self._retry_requested_fence(fence_id, deadline):
                    return
                continue
            async with self._database.transaction() as connection:
                cursor = await connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'acknowledged', acknowledged_at = ?,
                        ingress_depth = 0,
                        acknowledged_producer_count = ?,
                        acknowledged_journal_id = ?,
                        detail = ?
                    WHERE fence_id = ? AND state = 'requested'
                      AND expected_pid = ? AND expected_generation = ?
                      AND ABS(expected_process_started_at - ?) <= 5
                      AND protocol_version = ?
                      AND handoff_token_hash = ?
                      AND producer_count = ?
                      AND violation_count = 0
                      AND (SELECT COALESCE(MAX(journal_id), 0)
                           FROM event_journal) = ?
                    """,
                    (
                        time.time(),
                        producer_before,
                        journal_before,
                        json.dumps(
                            {"drained": True},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        fence_id,
                        self._pid,
                        self._process_generation,
                        self._process_started_at,
                        SERVICE_CONTROL_PROTOCOL_VERSION,
                        self._handoff_token_hash,
                        producer_before,
                        journal_before,
                    ),
                )
                changed = cursor.rowcount == 1
                await cursor.close()
            if changed:
                return
            if not await self._retry_requested_fence(fence_id, deadline):
                return

    async def _retry_requested_fence(
        self,
        fence_id: str,
        deadline: float,
    ) -> bool:
        state = await self._fence_state(fence_id)
        if state in {"prepared", "committed"}:
            self._terminate_process(75)
            return False
        if state != "requested":
            await self._rollback_quiesce(fence_id)
            return False
        if time.monotonic() < deadline:
            return True
        await self._mark_quiesce_failed(fence_id, "quiesce_timeout")
        await self._rollback_quiesce(fence_id)
        return False

    async def _await_or_abort(
        self,
        task: asyncio.Task[None],
        fence_id: str,
        deadline: float,
    ) -> bool:
        try:
            while not task.done():
                state = await self._fence_state(fence_id)
                if state in {"prepared", "committed"}:
                    await self._cancel_bounded(task)
                    self._terminate_process(75)
                    return False
                if state != "requested":
                    await self._cancel_bounded(task)
                    await self._rollback_quiesce(fence_id)
                    return False
                if time.monotonic() >= deadline:
                    await self._cancel_bounded(task)
                    await self._mark_quiesce_failed(
                        fence_id,
                        "quiesce_timeout",
                    )
                    await self._rollback_quiesce(fence_id)
                    return False
                await asyncio.sleep(self._poll_seconds)
            error = task.exception()
            if error is not None:
                await self._mark_quiesce_failed(
                    fence_id,
                    f"{type(error).__name__}: {error}",
                )
                await self._rollback_quiesce(fence_id)
                return False
            return True
        except asyncio.CancelledError:
            await self._cancel_bounded(task)
            await self._rollback_quiesce(fence_id)
            raise

    async def _cancel_bounded(self, task: asyncio.Task[None]) -> None:
        task.cancel()
        done, _ = await asyncio.wait(
            {task},
            timeout=max(1.0, self._poll_seconds * 4),
        )
        if task not in done:
            task.add_done_callback(_consume_task_result)

    async def _rollback_quiesce(self, fence_id: str) -> None:
        await self._database.execute(
            """
            UPDATE service_admission_fences
            SET state = 'released', released_at = ?,
                rollback_state = 'pending', detail = ?
            WHERE fence_id = ? AND state = 'violated'
            """,
            (
                time.time(),
                json.dumps(
                    {"reason": "violated_fence_rollback"},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                fence_id,
            ),
        )
        rollback = asyncio.create_task(
            self._sessions.end_service_quiesce(),
            name=f"service-quiesce-rollback:{fence_id}",
        )
        done, _ = await asyncio.wait(
            {rollback},
            timeout=self._rollback_timeout_seconds,
        )
        completed = rollback in done and not rollback.cancelled() and rollback.exception() is None
        if not completed:
            if rollback in done and not rollback.cancelled():
                rollback.exception()
            await self._cancel_bounded(rollback)
        await self._database.execute(
            """
            UPDATE service_admission_fences
            SET rollback_state = ?,
                rollback_attempts = rollback_attempts + 1,
                detail = ?
            WHERE fence_id = ? AND state = 'released'
            """,
            (
                "complete" if completed else "pending",
                json.dumps(
                    {"reason": ("rollback_complete" if completed else "rollback_timeout_retrying")},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                fence_id,
            ),
        )
        if completed and self._active_fence_id == fence_id:
            self._active_fence_id = None

    async def _mark_quiesce_failed(self, fence_id: str, reason: str) -> None:
        await self._database.execute(
            """
            UPDATE service_admission_fences
            SET state = 'violated', violation_count = violation_count + 1,
                rollback_state = 'pending', detail = ?
            WHERE fence_id = ? AND state = 'requested'
            """,
            (
                json.dumps(
                    {"reason": reason},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                fence_id,
            ),
        )

    async def _release_mismatched_fence(self, row: Any) -> None:
        await self._database.execute(
            """
            UPDATE service_admission_fences
            SET state = 'released', released_at = ?, detail = ?
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

    async def _fence_state(self, fence_id: str) -> str | None:
        row = await self._database.fetchone(
            "SELECT state FROM service_admission_fences WHERE fence_id = ?",
            (fence_id,),
        )
        return None if row is None else str(row["state"])

    async def _producer_count(self, fence_id: str) -> int:
        row = await self._database.fetchone(
            """
            SELECT producer_count FROM service_admission_fences
            WHERE fence_id = ?
            """,
            (fence_id,),
        )
        if row is None:
            raise RuntimeError("service admission fence disappeared")
        return int(row["producer_count"])

    async def _journal_id(self) -> int:
        row = await self._database.fetchone(
            """
            SELECT COALESCE(MAX(journal_id), 0) AS journal_id
            FROM event_journal
            """
        )
        if row is None:
            raise RuntimeError("event journal high-water mark is unavailable")
        return int(row["journal_id"])

    def _record_producer_sync(self, source: str) -> None:
        if self._active_fence_id is None:
            return
        fence_id = self._active_fence_id
        pending = self._create_pending_marker(
            fence_id=fence_id,
            source=source,
        )
        accounted = False
        try:
            connection = sqlite3.connect(str(self._database.path), timeout=0)
            try:
                connection.row_factory = sqlite3.Row
                cursor = connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET producer_count = producer_count + 1,
                        violation_count = violation_count + CASE
                          WHEN state IN (
                            'acknowledged', 'violated', 'prepared', 'committed'
                          )
                          THEN 1 ELSE 0 END,
                        state = CASE
                          WHEN state = 'acknowledged' THEN 'violated'
                          ELSE state END,
                        detail = ?
                    WHERE fence_id = ?
                      AND state IN ('requested', 'acknowledged',
                                    'violated', 'prepared', 'committed')
                    """,
                    (
                        json.dumps(
                            {"producer": source},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        fence_id,
                    ),
                )
                connection.commit()
                if cursor.rowcount != 1:
                    row = connection.execute(
                        """
                        SELECT state FROM service_admission_fences
                        WHERE fence_id = ?
                        """,
                        (fence_id,),
                    ).fetchone()
                    if row is None or row["state"] == "released":
                        accounted = True
                        return
                    raise RuntimeError("active service admission fence disappeared")
                accounted = True
            finally:
                connection.close()
        except sqlite3.Error as error:
            self._write_marker(
                fence_id=fence_id,
                kind="accounting-failure",
                source=source,
            )
            raise RuntimeError("could not durably record post-quiesce producer") from error
        finally:
            if accounted:
                pending.unlink(missing_ok=True)
                try:
                    pending.parent.rmdir()
                except OSError:
                    pass

    def _record_loss_sync(self, source: str) -> None:
        if self._active_fence_id is None:
            return
        self._write_marker(
            fence_id=self._active_fence_id,
            kind="loss",
            source=source,
        )

    def _has_failure_marker(self, fence_id: str) -> bool:
        return bool(self._failure_marker_epoch(fence_id))

    def _failure_marker_epoch(
        self,
        fence_id: str,
    ) -> tuple[tuple[str, int, int], ...]:
        paths = list(fence_marker_paths(self._database.path, fence_id))
        pending = fence_pending_marker_directory(
            self._database.path,
            fence_id,
        )
        if pending.is_dir():
            try:
                paths.extend(pending.iterdir())
            except FileNotFoundError:
                pass
        epoch: list[tuple[str, int, int]] = []
        for path in paths:
            try:
                stat_result = path.stat()
            except FileNotFoundError:
                continue
            if stat_result.st_size > 0:
                epoch.append(
                    (
                        path.name,
                        stat_result.st_size,
                        stat_result.st_mtime_ns,
                    )
                )
        return tuple(sorted(epoch))

    def _create_pending_marker(
        self,
        *,
        fence_id: str,
        source: str,
    ) -> Path:
        directory = fence_pending_marker_directory(
            self._database.path,
            fence_id,
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = directory / uuid.uuid4().hex
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            os.write(descriptor, f"{source}\n".encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return path

    def _write_marker(
        self,
        *,
        fence_id: str,
        kind: str,
        source: str,
    ) -> None:
        loss_path, accounting_path = fence_marker_paths(
            self._database.path,
            fence_id,
        )
        path = loss_path if kind == "loss" else accounting_path
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.write(descriptor, f"{source}\n".encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()
