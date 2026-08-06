from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from aiosqlite import Connection, Row

from copilotd.core.schedule_time import ParsedSchedule, parse_schedule, planned_key
from copilotd.storage.database import Database

SCHEDULE_LEASE_SECONDS = 60.0
SCHEDULE_RENEW_SECONDS = 20.0
SCHEDULE_RETRY_DELAYS = (5.0, 30.0, 120.0, 600.0, 1_800.0)
SCHEDULE_MAX_ATTEMPTS = 6


class ScheduleKind(StrEnum):
    MESSAGE = "message"
    NEW_SESSION = "new_session"


class ScheduleState(StrEnum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    DELETED = "deleted"


class MisfirePolicy(StrEnum):
    LATEST = "latest"
    SKIP = "skip"


class ScheduleRunState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    SUBMITTING = "submitting"
    ACCEPTED = "accepted"
    WAITING = "waiting"
    SEMANTIC_COMPLETE = "semantic_complete"
    BLOCKED = "blocked"
    FAILED = "failed"
    OUTCOME_UNKNOWN = "outcome_unknown"
    CANCELLED = "cancelled"
    TARGET_UNKNOWN = "target_unknown"
    DISPATCH_UNKNOWN = "dispatch_unknown"
    RETRY_WAIT = "retry_wait"

    @property
    def terminal(self) -> bool:
        return self in {
            self.SEMANTIC_COMPLETE,
            self.BLOCKED,
            self.FAILED,
            self.OUTCOME_UNKNOWN,
            self.CANCELLED,
            self.TARGET_UNKNOWN,
            self.DISPATCH_UNKNOWN,
        }


class SchedulerErrorCategory(StrEnum):
    INPUT = "input"
    CAPABILITY = "capability"
    TARGET = "target"
    CONFIG = "config"
    ATTACHMENT = "attachment"
    REMOTE = "remote"
    RUNTIME = "runtime"
    DISCORD = "discord"
    STORAGE = "storage"
    INTERNAL = "internal"


class SchedulerError(RuntimeError):
    code = "CD-SCHEDULE-001"


class ScheduleNotFound(SchedulerError):
    code = "CD-SCHEDULE-NOT-FOUND"


class ScheduleConflict(SchedulerError):
    code = "CD-SCHEDULE-CONFLICT"


class SchedulerNotRecovered(SchedulerError):
    code = "CD-SCHEDULE-RECOVERY"


class SchedulerDispatchError(SchedulerError):
    def __init__(
        self,
        message: str,
        *,
        category: SchedulerErrorCategory,
        code: str,
        retryable: bool = False,
        target_unknown: bool = False,
        dispatch_unknown: bool = False,
        blocked: bool = False,
    ) -> None:
        self.category = category
        self.code = code
        self.retryable = retryable
        self.target_unknown = target_unknown
        self.dispatch_unknown = dispatch_unknown
        self.blocked = blocked
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ScheduleDefinition:
    id: str
    project_id: str | None
    thread_id: str | None
    channel_id: str | None
    kind: ScheduleKind
    expression: str
    normalized_expression: str
    timezone: str
    payload: dict[str, Any]
    target_snapshot: dict[str, Any]
    misfire_policy: MisfirePolicy
    state: ScheduleState
    next_run_at_utc: float | None
    planner_fence_token: int
    created_at: float
    updated_at: float
    name: str | None = None
    created_by: str | None = None


@dataclass(frozen=True, slots=True)
class ScheduleRun:
    run_id: str
    schedule_id: str
    planned_key: str
    planned_at_utc: float
    status: ScheduleRunState
    attempt: int
    fence_token: int
    lease_owner: str | None
    lease_expires_at: float | None
    creation_intent_id: str | None
    session_create_started_at: float | None
    send_started_at: float | None
    accepted_message_id: str | None
    completion_basis: str | None
    result_project_id: str | None
    result_thread_id: str | None
    result_session_id: str | None
    result_submission_id: str | None
    render_intent_id: str | None
    retry_at: float | None
    error_category: str | None
    error_code: str | None
    error_detail: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class ScheduledTarget:
    project_id: str | None
    thread_id: str
    sdk_session_id: str
    temporary_attachment: bool = False


@dataclass(frozen=True, slots=True)
class SchedulerStatus:
    worker_state: str
    owner_id: str | None
    recovery_completed_at: float | None
    last_tick_at: float | None
    last_clock_utc: float | None
    paused_reason: str | None
    enabled_definitions: int
    due_definitions: int
    pending_runs: int
    claimed_runs: int
    waiting_runs: int
    unknown_runs: int
    restart_blockers: tuple[str, ...]


class SchedulerTargetAdapter(Protocol):
    async def prepare_message_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget: ...

    async def prepare_new_session_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget: ...

    async def reconcile_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget | None: ...

    async def queue_ready(self, target: ScheduledTarget, run_id: str) -> None: ...

    async def release_temporary_target(
        self,
        target: ScheduledTarget,
        run: ScheduleRun,
    ) -> None: ...


class Clock(Protocol):
    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SchedulerRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(
        self,
        *,
        kind: ScheduleKind,
        expression: str,
        timezone: str,
        payload: dict[str, Any],
        target_snapshot: dict[str, Any],
        project_id: str | None = None,
        thread_id: str | None = None,
        channel_id: str | None = None,
        name: str | None = None,
        created_by: str | None = None,
        misfire_policy: MisfirePolicy = MisfirePolicy.LATEST,
        misfire_grace_seconds: float | None = None,
        now: float | None = None,
        schedule_id: str | None = None,
        connection: Connection | None = None,
    ) -> ScheduleDefinition:
        timestamp = time.time() if now is None else now
        parsed = parse_schedule(expression, timezone, anchor_utc=timestamp)
        next_run = parsed.next_after(timestamp)
        identifier = str(uuid.uuid4()) if schedule_id is None else schedule_id

        async def insert(active_connection: Connection) -> None:
            await _require_scheduler_admission(
                active_connection,
                project_id=project_id,
            )
            await active_connection.execute(
                """
                INSERT INTO schedules(
                    id, project_id, thread_id, channel_id, kind, expression,
                    normalized_expression, timezone, payload, target_snapshot,
                    misfire_policy, state, next_run_at_utc, name, created_by,
                    misfire_grace_seconds, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'enabled',
                          ?, ?, ?, ?, ?, ?)
                """,
                (
                    identifier,
                    project_id,
                    thread_id,
                    channel_id,
                    kind.value,
                    parsed.source,
                    parsed.normalized,
                    parsed.timezone,
                    _canonical_json(payload),
                    _canonical_json(target_snapshot),
                    misfire_policy.value,
                    next_run,
                    name,
                    created_by,
                    misfire_grace_seconds,
                    timestamp,
                    timestamp,
                ),
            )

        if connection is None:
            async with self._database.transaction() as active_connection:
                await insert(active_connection)
        else:
            await insert(connection)
        return await self.require(identifier)

    async def require(self, schedule_id: str) -> ScheduleDefinition:
        row = await self._database.fetchone("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
        if row is None:
            raise ScheduleNotFound(f"schedule does not exist: {schedule_id}")
        return _row_to_definition(row)

    async def get_run(self, run_id: str) -> ScheduleRun:
        row = await self._database.fetchone(
            "SELECT * FROM schedule_runs WHERE run_id = ?",
            (run_id,),
        )
        if row is None:
            raise ScheduleNotFound(f"schedule run does not exist: {run_id}")
        return _row_to_run(row)

    async def list(
        self,
        *,
        project_id: str | None = None,
        thread_id: str | None = None,
        channel_id: str | None = None,
        include_deleted: bool = False,
    ) -> list[ScheduleDefinition]:
        filters: list[str] = []
        parameters: list[object] = []
        if project_id is not None:
            filters.append("project_id = ?")
            parameters.append(project_id)
        if thread_id is not None:
            filters.append("thread_id = ?")
            parameters.append(thread_id)
        if channel_id is not None:
            filters.append("channel_id = ?")
            parameters.append(channel_id)
        if not include_deleted:
            filters.append("state != 'deleted'")
        where = "" if not filters else "WHERE " + " AND ".join(filters)
        rows = await self._database.fetchall(
            f"SELECT * FROM schedules {where} ORDER BY created_at, id",
            parameters,
        )
        return [_row_to_definition(row) for row in rows]

    async def list_runs(self, schedule_id: str, *, limit: int = 50) -> list[ScheduleRun]:
        rows = await self._database.fetchall(
            """
            SELECT * FROM schedule_runs
            WHERE schedule_id = ?
            ORDER BY planned_at_utc DESC, created_at DESC
            LIMIT ?
            """,
            (schedule_id, limit),
        )
        return [_row_to_run(row) for row in rows]

    async def toggle(
        self,
        schedule_id: str,
        *,
        enabled: bool,
        now: float | None = None,
    ) -> ScheduleDefinition:
        timestamp = time.time() if now is None else now
        target = ScheduleState.ENABLED if enabled else ScheduleState.DISABLED
        changed = await self._database.execute_count(
            """
            UPDATE schedules
            SET state = ?, version = version + 1, updated_at = ?
            WHERE id = ? AND state != 'deleted'
            """,
            (target.value, timestamp, schedule_id),
        )
        if changed != 1:
            raise ScheduleNotFound(f"active schedule does not exist: {schedule_id}")
        return await self.require(schedule_id)

    async def delete(self, schedule_id: str, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            row = await _fetchone(
                connection,
                "SELECT state FROM schedules WHERE id = ?",
                (schedule_id,),
            )
            if row is None:
                raise ScheduleNotFound(f"schedule does not exist: {schedule_id}")
            if row["state"] == ScheduleState.DELETED.value:
                return
            active = await _fetchone(
                connection,
                """
                SELECT run_id, status FROM schedule_runs
                WHERE schedule_id = ? AND status NOT IN (
                    'semantic_complete', 'blocked', 'failed', 'outcome_unknown',
                    'cancelled', 'target_unknown', 'dispatch_unknown'
                )
                LIMIT 1
                """,
                (schedule_id,),
            )
            if active is not None:
                raise ScheduleConflict(
                    f"schedule has nonterminal run {active['run_id']} ({active['status']})"
                )
            await connection.execute(
                """
                UPDATE schedules
                SET state = 'deleted', deleted_at = ?, planner_owner = NULL,
                    planner_lease_expires_at = NULL, version = version + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (timestamp, timestamp, schedule_id),
            )

    async def run_now(
        self,
        schedule_id: str,
        *,
        now: float | None = None,
        manual_id: str | None = None,
    ) -> ScheduleRun:
        timestamp = time.time() if now is None else now
        definition = await self.require(schedule_id)
        if definition.state == ScheduleState.DELETED:
            raise ScheduleConflict("deleted schedules cannot be run")
        key = f"manual:{manual_id or uuid.uuid4()}"
        run_id = _run_id(schedule_id, key)
        async with self._database.transaction() as connection:
            scheduler = await _fetchone(
                connection,
                "SELECT worker_state FROM scheduler_state WHERE singleton = 1",
                (),
            )
            if scheduler is not None and scheduler["worker_state"] == "draining":
                raise ScheduleConflict("scheduler is draining for restart")
            current = await _fetchone(
                connection,
                "SELECT state, project_id FROM schedules WHERE id = ?",
                (schedule_id,),
            )
            if current is None or current["state"] == ScheduleState.DELETED.value:
                raise ScheduleConflict("deleted schedules cannot be run")
            await _require_scheduler_admission(
                connection,
                project_id=current["project_id"],
            )
            await connection.execute(
                """
                INSERT INTO schedule_runs(
                    run_id, schedule_id, planned_key, planned_at_utc, status,
                    attempt, fence_token, last_progress_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?)
                ON CONFLICT(schedule_id, planned_key) DO NOTHING
                """,
                (
                    run_id,
                    schedule_id,
                    key,
                    timestamp,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )
        return await self.get_run(run_id)

    async def plan_due(
        self,
        owner_id: str,
        *,
        now: float,
        limit: int = 100,
        lease_seconds: float = SCHEDULE_LEASE_SECONDS,
    ) -> list[ScheduleRun]:
        state = await self._database.fetchone(
            "SELECT worker_state FROM scheduler_state WHERE singleton = 1"
        )
        if state is not None and state["worker_state"] == "draining":
            return []
        rows = await self._database.fetchall(
            """
            SELECT id FROM schedules
            WHERE state = 'enabled' AND next_run_at_utc IS NOT NULL
              AND next_run_at_utc <= ?
              AND (
                  planner_lease_expires_at IS NULL
                  OR planner_lease_expires_at <= ?
                  OR planner_owner = ?
              )
            ORDER BY next_run_at_utc, id
            LIMIT ?
            """,
            (now, now, owner_id, limit),
        )
        planned: list[ScheduleRun] = []
        for candidate in rows:
            run = await self._plan_one(
                str(candidate["id"]),
                owner_id,
                now=now,
                lease_seconds=lease_seconds,
            )
            if run is not None:
                planned.append(run)
        return planned

    async def _plan_one(
        self,
        schedule_id: str,
        owner_id: str,
        *,
        now: float,
        lease_seconds: float,
    ) -> ScheduleRun | None:
        async with self._database.transaction() as connection:
            scheduler = await _fetchone(
                connection,
                "SELECT worker_state FROM scheduler_state WHERE singleton = 1",
                (),
            )
            if scheduler is not None and scheduler["worker_state"] == "draining":
                return None
            update = await connection.execute(
                """
                UPDATE schedules
                SET planner_owner = ?, planner_lease_expires_at = ?,
                    planner_fence_token = planner_fence_token + 1,
                    updated_at = ?
                WHERE id = ? AND state = 'enabled'
                  AND next_run_at_utc IS NOT NULL AND next_run_at_utc <= ?
                  AND (
                      planner_lease_expires_at IS NULL
                      OR planner_lease_expires_at <= ?
                      OR planner_owner = ?
                  )
                """,
                (owner_id, now + lease_seconds, now, schedule_id, now, now, owner_id),
            )
            claimed = update.rowcount == 1
            await update.close()
            if not claimed:
                return None
            row = await _fetchone(
                connection,
                "SELECT * FROM schedules WHERE id = ?",
                (schedule_id,),
            )
            if row is None:
                return None
            definition = _row_to_definition(row)
            assert definition.next_run_at_utc is not None
            parsed = _parse_definition(definition)
            latest_due = parsed.latest_due(definition.next_run_at_utc, now)
            grace = row["misfire_grace_seconds"]
            grace_seconds = 0.0 if grace is None else float(grace)
            skip_misfire = (
                definition.misfire_policy == MisfirePolicy.SKIP
                and latest_due is not None
                and latest_due < now - grace_seconds
            )
            run: ScheduleRun | None = None
            if latest_due is not None and not skip_misfire:
                key = planned_key(latest_due)
                run_id = _run_id(schedule_id, key)
                await connection.execute(
                    """
                    INSERT INTO schedule_runs(
                        run_id, schedule_id, planned_key, planned_at_utc, status,
                        attempt, fence_token, last_progress_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?)
                    ON CONFLICT(schedule_id, planned_key) DO NOTHING
                    """,
                    (run_id, schedule_id, key, latest_due, now, now, now),
                )
                run_row = await _fetchone(
                    connection,
                    "SELECT * FROM schedule_runs WHERE schedule_id = ? AND planned_key = ?",
                    (schedule_id, key),
                )
                if run_row is not None:
                    run = _row_to_run(run_row)
            next_run = parsed.next_after(now)
            await connection.execute(
                """
                UPDATE schedules
                SET next_run_at_utc = ?, last_planned_at_utc = ?,
                    state = CASE WHEN ? IS NULL THEN 'disabled' ELSE state END,
                    planner_owner = NULL, planner_lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ? AND planner_owner = ?
                """,
                (next_run, latest_due, next_run, now, schedule_id, owner_id),
            )
            await _record_scheduler_event(
                connection,
                event_type="definition_planned" if run is not None else "definition_advanced",
                schedule_id=schedule_id,
                run_id=None if run is None else run.run_id,
                owner_id=owner_id,
                fence_token=definition.planner_fence_token,
                detail={
                    "latest_due": latest_due,
                    "next_run": next_run,
                    "misfire_skipped": skip_misfire,
                },
                now=now,
            )
            return run

    async def claim_next(
        self,
        owner_id: str,
        *,
        now: float,
        lease_seconds: float = SCHEDULE_LEASE_SECONDS,
    ) -> ScheduleRun | None:
        async with self._database.transaction() as connection:
            scheduler = await _fetchone(
                connection,
                "SELECT worker_state FROM scheduler_state WHERE singleton = 1",
                (),
            )
            if scheduler is not None and scheduler["worker_state"] == "draining":
                return None
            row = await _fetchone(
                connection,
                """
                SELECT * FROM schedule_runs
                WHERE (
                    status = 'pending'
                    OR (status = 'retry_wait' AND COALESCE(retry_at, 0) <= ?)
                    OR (
                        status = 'claimed'
                        AND COALESCE(lease_expires_at, 0) <= ?
                        AND target_started_at IS NULL
                        AND send_started_at IS NULL
                    )
                    OR (
                        status = 'submitting'
                        AND COALESCE(lease_expires_at, 0) <= ?
                        AND send_started_at IS NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM message_queue q
                            WHERE q.schedule_run_id = schedule_runs.run_id
                              AND q.state NOT IN ('cancelled', 'failed')
                        )
                    )
                )
                ORDER BY planned_at_utc, created_at, run_id
                LIMIT 1
                """,
                (now, now, now),
            )
            if row is None:
                return None
            old_state = ScheduleRunState(str(row["status"]))
            attempt = int(row["attempt"])
            if old_state in {
                ScheduleRunState.PENDING,
                ScheduleRunState.RETRY_WAIT,
                ScheduleRunState.SUBMITTING,
            }:
                attempt += 1
            if attempt > SCHEDULE_MAX_ATTEMPTS:
                await self._finalize_in_transaction(
                    connection,
                    str(row["run_id"]),
                    ScheduleRunState.FAILED,
                    completion_basis=None,
                    error_category=SchedulerErrorCategory.RUNTIME.value,
                    error_code="retry_exhausted",
                    detail="scheduler exhausted all pre-dispatch attempts",
                    now=now,
                )
                return None
            update = await connection.execute(
                """
                UPDATE schedule_runs
                SET status = 'claimed', lease_owner = ?, lease_expires_at = ?,
                    fence_token = fence_token + 1, attempt = ?,
                    claimed_at = COALESCE(claimed_at, ?), retry_at = NULL,
                    last_progress_at = ?, updated_at = ?
                WHERE run_id = ? AND status = ?
                """,
                (
                    owner_id,
                    now + lease_seconds,
                    attempt,
                    now,
                    now,
                    now,
                    row["run_id"],
                    old_state.value,
                ),
            )
            claimed = update.rowcount == 1
            await update.close()
            if not claimed:
                return None
            claimed_row = await _fetchone(
                connection,
                "SELECT * FROM schedule_runs WHERE run_id = ?",
                (row["run_id"],),
            )
            assert claimed_row is not None
            run = _row_to_run(claimed_row)
            await connection.execute(
                """
                INSERT INTO schedule_run_attempts(
                    run_id, attempt, fence_token, owner_id, state, started_at
                ) VALUES (?, ?, ?, ?, 'claimed', ?)
                ON CONFLICT(run_id, attempt) DO UPDATE SET
                    fence_token = excluded.fence_token,
                    owner_id = excluded.owner_id,
                    state = excluded.state,
                    started_at = excluded.started_at,
                    settled_at = NULL
                """,
                (run.run_id, run.attempt, run.fence_token, owner_id, now),
            )
            return run

    async def renew_run(
        self,
        run_id: str,
        owner_id: str,
        fence_token: int,
        *,
        now: float,
        lease_seconds: float = SCHEDULE_LEASE_SECONDS,
    ) -> bool:
        changed = await self._database.execute_count(
            """
            UPDATE schedule_runs
            SET lease_expires_at = ?, last_progress_at = ?, updated_at = ?
            WHERE run_id = ? AND lease_owner = ? AND fence_token = ?
              AND status IN ('claimed', 'submitting')
            """,
            (now + lease_seconds, now, now, run_id, owner_id, fence_token),
        )
        return changed == 1

    async def mark_target_started(
        self,
        run: ScheduleRun,
        owner_id: str,
        *,
        new_session: bool,
        now: float,
    ) -> ScheduleRun:
        session_id = (
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:schedule-run:{run.run_id}:session",
                )
            )
            if new_session
            else run.result_session_id
        )
        changed = await self._database.execute_count(
            """
            UPDATE schedule_runs
            SET status = 'submitting', target_started_at = COALESCE(target_started_at, ?),
                session_create_started_at = CASE
                    WHEN ? THEN COALESCE(session_create_started_at, ?)
                    ELSE session_create_started_at
                END,
                creation_intent_id = CASE
                    WHEN ? THEN COALESCE(creation_intent_id, ?)
                    ELSE creation_intent_id
                END,
                result_session_id = COALESCE(result_session_id, ?),
                last_progress_at = ?, updated_at = ?
            WHERE run_id = ? AND lease_owner = ? AND fence_token = ?
              AND status = 'claimed'
            """,
            (
                now,
                int(new_session),
                now,
                int(new_session),
                f"schedule:{run.run_id}",
                session_id,
                now,
                now,
                run.run_id,
                owner_id,
                run.fence_token,
            ),
        )
        if changed != 1:
            raise ScheduleConflict("schedule run fence was lost before target preparation")
        return await self.get_run(run.run_id)

    async def record_target(
        self,
        run: ScheduleRun,
        owner_id: str,
        target: ScheduledTarget,
        *,
        now: float,
    ) -> ScheduleRun:
        changed = await self._database.execute_count(
            """
            UPDATE schedule_runs
            SET result_project_id = COALESCE(result_project_id, ?),
                result_thread_id = COALESCE(result_thread_id, ?),
                result_session_id = COALESCE(result_session_id, ?),
                temporary_attachment = MAX(temporary_attachment, ?),
                last_progress_at = ?, updated_at = ?
            WHERE run_id = ? AND lease_owner = ? AND fence_token = ?
              AND status IN ('claimed', 'submitting')
              AND (result_thread_id IS NULL OR result_thread_id = ?)
              AND (result_session_id IS NULL OR result_session_id = ?)
            """,
            (
                target.project_id,
                target.thread_id,
                target.sdk_session_id,
                int(target.temporary_attachment),
                now,
                now,
                run.run_id,
                owner_id,
                run.fence_token,
                target.thread_id,
                target.sdk_session_id,
            ),
        )
        if changed != 1:
            raise ScheduleConflict("schedule target conflicts with durable run target")
        return await self.get_run(run.run_id)

    async def enqueue(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
        owner_id: str,
        target: ScheduledTarget,
        *,
        now: float,
    ) -> str:
        submission_id = _submission_id(run.run_id)
        snapshot = definition.target_snapshot
        execution = snapshot.get("execution_config")
        if not isinstance(execution, dict):
            execution = {}
        requested_mode = str(execution.get("mode", "interactive"))
        requested_model = execution.get("model_config")
        if not isinstance(requested_model, dict):
            requested_model = {}
        requested_agent = str(execution.get("agent", "default"))
        requested_config_version = int(execution.get("session_config_version", 1))
        prompt = str(definition.payload.get("text", ""))
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()

        async with self._database.transaction() as connection:
            scheduler = await _fetchone(
                connection,
                "SELECT worker_state FROM scheduler_state WHERE singleton = 1",
                (),
            )
            if scheduler is not None and scheduler["worker_state"] == "draining":
                raise ScheduleConflict("scheduler is draining for restart")
            current = await _fetchone(
                connection,
                "SELECT * FROM schedule_runs WHERE run_id = ?",
                (run.run_id,),
            )
            if current is None:
                raise ScheduleNotFound(f"schedule run does not exist: {run.run_id}")
            existing = await _fetchone(
                connection,
                """
                SELECT id FROM message_queue
                WHERE schedule_run_id = ? AND state NOT IN ('cancelled', 'submitted', 'failed')
                """,
                (run.run_id,),
            )
            if existing is not None:
                await connection.execute(
                    """
                    UPDATE schedule_runs
                    SET status = 'submitting', result_submission_id = ?,
                        lease_owner = NULL, lease_expires_at = NULL,
                        last_progress_at = ?, updated_at = ?
                    WHERE run_id = ?
                    """,
                    (existing["id"], now, now, run.run_id),
                )
                return str(existing["id"])
            if (
                current["lease_owner"] != owner_id
                or int(current["fence_token"] or 0) != run.fence_token
                or current["status"] not in {"claimed", "submitting"}
            ):
                raise ScheduleConflict("schedule run fence was lost before queue insertion")
            binding = await _fetchone(
                connection,
                """
                SELECT b.runtime_generation, b.owner_fence_token,
                       b.binding_intent, b.attachment_state,
                       b.attachment_reason,
                       b.permission_posture, p.state AS project_state,
                       p.project_kind,
                       EXISTS (
                           SELECT 1 FROM session_owner_leases l
                           WHERE l.sdk_session_id = b.sdk_session_id
                             AND l.fence_token = b.owner_fence_token
                             AND l.expires_at > ?
                       ) AS owner_current
                FROM session_bindings b
                LEFT JOIN projects p ON p.id = b.project_id
                WHERE b.thread_id = ? AND b.sdk_session_id = ?
                """,
                (now, target.thread_id, target.sdk_session_id),
            )
            if (
                binding is None
                or (
                    binding["binding_intent"] != "active"
                    and not (
                        binding["binding_intent"] == "closed"
                        and binding["attachment_reason"] == "scheduler_run"
                    )
                )
                or binding["attachment_state"] != "attached"
                or binding["permission_posture"] != "verified_allow_all"
                or not bool(binding["owner_current"])
                or binding["project_state"] == "closing"
                or (binding["project_kind"] == "worktree" and binding["project_state"] == "retired")
            ):
                raise SchedulerDispatchError(
                    "scheduled target lost attached runtime ownership before enqueue",
                    category=SchedulerErrorCategory.TARGET,
                    code="target_not_ready_at_enqueue",
                    retryable=True,
                )
            position_row = await _fetchone(
                connection,
                "SELECT COALESCE(MAX(position), 0) + 1 AS position "
                "FROM message_queue WHERE thread_id = ?",
                (target.thread_id,),
            )
            assert position_row is not None
            await connection.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin, schedule_run_id,
                    prompt_hash, requested_mode, requested_model_config,
                    requested_agent, requested_session_config_version,
                    requested_delivery, correlation_id, attachment_count,
                    state, created_at
                ) VALUES (?, ?, 'app_schedule', ?, ?, ?, ?, ?, ?, 'enqueue', ?, 0,
                          'local_queued', ?)
                """,
                (
                    submission_id,
                    target.sdk_session_id,
                    run.run_id,
                    prompt_hash,
                    requested_mode,
                    _canonical_json(requested_model),
                    requested_agent,
                    requested_config_version,
                    f"schedule:{run.run_id}",
                    now,
                ),
            )
            await connection.execute(
                """
                INSERT INTO message_queue(
                    id, thread_id, schedule_run_id, prompt,
                    requested_mode_snapshot, requested_model_config_snapshot,
                    requested_agent_snapshot, requested_session_config_version,
                    position, state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'local_queued', ?, ?)
                """,
                (
                    submission_id,
                    target.thread_id,
                    run.run_id,
                    prompt,
                    requested_mode,
                    _canonical_json(requested_model),
                    requested_agent,
                    requested_config_version,
                    int(position_row["position"]),
                    now,
                    now,
                ),
            )
            if binding["owner_fence_token"] is not None:
                await connection.execute(
                    """
                    INSERT INTO liveness_leases(
                        sdk_session_id, lease_id, kind, source_id,
                        runtime_generation, owner_fence_token, state,
                        acquired_at, refreshed_at
                    ) VALUES (?, ?, 'submission', ?, ?, ?, 'active', ?, ?)
                    ON CONFLICT(sdk_session_id, lease_id) DO UPDATE SET
                        runtime_generation = excluded.runtime_generation,
                        owner_fence_token = excluded.owner_fence_token,
                        state = 'active', refreshed_at = excluded.refreshed_at,
                        released_at = NULL
                    """,
                    (
                        target.sdk_session_id,
                        f"submission:{submission_id}",
                        submission_id,
                        int(binding["runtime_generation"]),
                        int(binding["owner_fence_token"]),
                        now,
                        now,
                    ),
                )
            await connection.execute(
                """
                UPDATE schedule_runs
                SET status = 'submitting', queued_at = COALESCE(queued_at, ?),
                    result_project_id = COALESCE(result_project_id, ?),
                    result_thread_id = COALESCE(result_thread_id, ?),
                    result_session_id = COALESCE(result_session_id, ?),
                    result_submission_id = ?, dispatch_key = COALESCE(dispatch_key, ?),
                    lease_owner = NULL, lease_expires_at = NULL,
                    last_progress_at = ?, updated_at = ?
                WHERE run_id = ? AND lease_owner = ? AND fence_token = ?
                """,
                (
                    now,
                    target.project_id,
                    target.thread_id,
                    target.sdk_session_id,
                    submission_id,
                    f"schedule:{run.run_id}",
                    now,
                    now,
                    run.run_id,
                    owner_id,
                    run.fence_token,
                ),
            )
            await connection.execute(
                """
                UPDATE schedule_run_attempts
                SET state = 'queued', settled_at = ?
                WHERE run_id = ? AND attempt = ?
                """,
                (now, run.run_id, run.attempt),
            )
            await _record_scheduler_event(
                connection,
                event_type="run_queued",
                schedule_id=definition.id,
                run_id=run.run_id,
                owner_id=owner_id,
                fence_token=run.fence_token,
                detail={"submission_id": submission_id, "thread_id": target.thread_id},
                now=now,
            )
        return submission_id

    async def retry_or_fail(
        self,
        run: ScheduleRun,
        owner_id: str,
        error: SchedulerDispatchError,
        *,
        now: float,
    ) -> ScheduleRunState:
        if error.target_unknown:
            state = ScheduleRunState.TARGET_UNKNOWN
        elif error.dispatch_unknown:
            state = ScheduleRunState.DISPATCH_UNKNOWN
        elif error.blocked:
            state = ScheduleRunState.BLOCKED
        elif not error.retryable or run.attempt >= SCHEDULE_MAX_ATTEMPTS:
            state = ScheduleRunState.FAILED
        else:
            state = ScheduleRunState.RETRY_WAIT
        if state.terminal:
            await self.finalize(
                run.run_id,
                state,
                completion_basis=None,
                error_category=error.category.value,
                error_code=error.code,
                detail=str(error),
                now=now,
                expected_owner_id=owner_id,
                expected_fence_token=run.fence_token,
            )
            return state
        delay = SCHEDULE_RETRY_DELAYS[min(run.attempt - 1, len(SCHEDULE_RETRY_DELAYS) - 1)]
        changed = await self._database.execute_count(
            """
            UPDATE schedule_runs
            SET status = 'retry_wait', retry_at = ?,
                error_category = ?, error_code = ?, error_detail = ?,
                lease_owner = NULL, lease_expires_at = NULL,
                last_progress_at = ?, updated_at = ?
            WHERE run_id = ? AND lease_owner = ? AND fence_token = ?
              AND send_started_at IS NULL
            """,
            (
                now + delay,
                error.category.value,
                error.code,
                str(error),
                now,
                now,
                run.run_id,
                owner_id,
                run.fence_token,
            ),
        )
        if changed != 1:
            raise ScheduleConflict("run cannot retry after its dispatch boundary")
        await self._database.execute(
            """
            UPDATE schedule_run_attempts
            SET state = 'retry_wait', error_category = ?, error_code = ?, settled_at = ?
            WHERE run_id = ? AND attempt = ?
            """,
            (error.category.value, error.code, now, run.run_id, run.attempt),
        )
        return state

    async def finalize(
        self,
        run_id: str,
        status: ScheduleRunState,
        *,
        completion_basis: str | None,
        error_category: str | None = None,
        error_code: str | None = None,
        detail: str | None = None,
        now: float | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> ScheduleRun:
        if not status.terminal:
            raise ValueError(f"schedule final state must be terminal: {status}")
        if (expected_owner_id is None) != (expected_fence_token is None):
            raise ValueError("expected owner and fence must be provided together")
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            await self._finalize_in_transaction(
                connection,
                run_id,
                status,
                completion_basis=completion_basis,
                error_category=error_category,
                error_code=error_code,
                detail=detail,
                now=timestamp,
                expected_owner_id=expected_owner_id,
                expected_fence_token=expected_fence_token,
            )
        return await self.get_run(run_id)

    async def _finalize_in_transaction(
        self,
        connection: Connection,
        run_id: str,
        status: ScheduleRunState,
        *,
        completion_basis: str | None,
        error_category: str | None,
        error_code: str | None,
        detail: str | None,
        now: float,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> None:
        row = await _fetchone(
            connection,
            """
            SELECT r.*, s.kind AS schedule_kind,
                   s.thread_id AS schedule_thread_id,
                   s.channel_id AS schedule_channel_id,
                   EXISTS (
                       SELECT 1 FROM session_bindings b
                       WHERE b.sdk_session_id = r.result_session_id
                   ) AS result_session_bound,
                   EXISTS (
                       SELECT 1 FROM submissions sub
                       WHERE sub.submission_id = r.result_submission_id
                         AND (
                             sub.accepted_message_id IS NOT NULL
                             OR sub.observed_user_event_id IS NOT NULL
                         )
                   ) AS dispatch_observed
            FROM schedule_runs AS r
            JOIN schedules AS s ON s.id = r.schedule_id
            WHERE r.run_id = ?
            """,
            (run_id,),
        )
        if row is None:
            raise ScheduleNotFound(f"schedule run does not exist: {run_id}")
        if expected_owner_id is not None and (
            row["lease_owner"] != expected_owner_id
            or int(row["fence_token"] or 0) != expected_fence_token
        ):
            raise ScheduleConflict("schedule run fence was lost before terminal finalization")
        if (
            error_code == "forced_restart"
            and status == ScheduleRunState.DISPATCH_UNKNOWN
            and (bool(row["dispatch_observed"]) or row["accepted_message_id"] is not None)
        ):
            status = ScheduleRunState.OUTCOME_UNKNOWN
        current = ScheduleRunState(str(row["status"]))
        if current.terminal and row["render_intent_id"] is not None:
            return
        render_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:schedule-run:{run_id}:final-render")
        )
        render_session = _schedule_render_destination(row)
        logical_row = await _fetchone(
            connection,
            """
            SELECT COALESCE(MAX(logical_seq), 0) + 1 AS logical_seq
            FROM render_outbox WHERE session_id = ?
            """,
            (render_session,),
        )
        assert logical_row is not None
        content = _terminal_content(
            run_id,
            status,
            completion_basis=completion_basis,
            error_code=error_code,
            detail=detail,
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
                render_session,
                int(logical_row["logical_seq"]),
                f"schedule-run:{run_id}",
                f"schedule-run:{run_id}:final",
                _canonical_json(
                    {
                        "content": content,
                        "finalized": True,
                        "schedule_run": {
                            "run_id": run_id,
                            "schedule_id": str(row["schedule_id"]),
                            "status": status.value,
                            "completion_basis": completion_basis,
                            "error_code": error_code,
                        },
                        "render_destination": render_session,
                    }
                ),
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
            (run_id, render_id, status.value, completion_basis, now),
        )
        await connection.execute(
            """
            UPDATE schedule_runs
            SET status = ?, completion_basis = ?, error_category = ?,
                error_code = ?, error_detail = ?, render_intent_id = ?,
                lease_owner = NULL, lease_expires_at = NULL,
                terminal_at = COALESCE(terminal_at, ?),
                cancelled_at = CASE WHEN ? = 'cancelled' THEN ? ELSE cancelled_at END,
                last_progress_at = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (
                status.value,
                completion_basis,
                error_category,
                error_code,
                detail,
                render_id,
                now,
                status.value,
                now,
                now,
                now,
                run_id,
            ),
        )
        if error_code == "forced_restart":
            submission_state = (
                "outcome_unknown"
                if row["send_started_at"] is not None
                or row["accepted_message_id"] is not None
                or bool(row["dispatch_observed"])
                else "cancelled"
            )
            queue_state = (
                "submitted_unknown"
                if row["send_started_at"] is not None or bool(row["dispatch_observed"])
                else "cancelled"
            )
            await connection.execute(
                """
                UPDATE message_queue SET state = ?, updated_at = ?
                WHERE schedule_run_id = ?
                  AND state NOT IN ('submitted', 'failed', 'cancelled')
                """,
                (queue_state, now, run_id),
            )
            await connection.execute(
                """
                UPDATE submissions SET state = ?, terminal_at = COALESCE(terminal_at, ?)
                WHERE submission_id = ?
                  AND state IN (
                      'local_queued', 'submitting', 'submitted',
                      'submitted_unknown', 'observed_active', 'loop_idle',
                      'continuation_expected'
                  )
                """,
                (
                    submission_state,
                    now,
                    row["result_submission_id"],
                ),
            )
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'orphaned', refreshed_at = ?, released_at = ?
                WHERE kind = 'submission' AND source_id = ? AND state = 'active'
                """,
                (now, now, row["result_submission_id"]),
            )
        await connection.execute(
            """
            UPDATE schedule_run_attempts
            SET state = ?, error_category = ?, error_code = ?,
                settled_at = COALESCE(settled_at, ?)
            WHERE run_id = ? AND attempt = ?
            """,
            (
                status.value,
                error_category,
                error_code,
                now,
                run_id,
                int(row["attempt"]),
            ),
        )
        await _record_scheduler_event(
            connection,
            event_type="run_terminal",
            schedule_id=str(row["schedule_id"]),
            run_id=run_id,
            owner_id=None if row["lease_owner"] is None else str(row["lease_owner"]),
            fence_token=int(row["fence_token"] or 0),
            detail={
                "status": status.value,
                "completion_basis": completion_basis,
                "render_intent_id": render_id,
                "error_code": error_code,
            },
            now=now,
        )

    async def reconcile_submissions(self, *, now: float) -> int:
        rows = await self._database.fetchall(
            """
            SELECT r.run_id, r.status AS run_status,
                   s.state AS submission_state, s.completion_basis,
                   s.accepted_message_id, s.terminal_at
            FROM schedule_runs AS r
            JOIN submissions AS s ON s.submission_id = r.result_submission_id
            WHERE r.status IN ('submitting', 'accepted', 'waiting')
            ORDER BY r.created_at
            """
        )
        changed = 0
        for row in rows:
            submission_state = str(row["submission_state"])
            run_id = str(row["run_id"])
            if submission_state == "semantic_complete":
                await self.finalize(
                    run_id,
                    ScheduleRunState.SEMANTIC_COMPLETE,
                    completion_basis=str(row["completion_basis"] or "semantic_complete"),
                    now=now,
                )
                changed += 1
            elif submission_state in {"semantic_blocked", "observed_aborted"}:
                await self.finalize(
                    run_id,
                    ScheduleRunState.BLOCKED,
                    completion_basis=str(row["completion_basis"] or submission_state),
                    error_category=SchedulerErrorCategory.RUNTIME.value,
                    error_code=submission_state,
                    now=now,
                )
                changed += 1
            elif submission_state in {"rejected"}:
                await self.finalize(
                    run_id,
                    ScheduleRunState.FAILED,
                    completion_basis=None,
                    error_category=SchedulerErrorCategory.RUNTIME.value,
                    error_code="submission_rejected",
                    now=now,
                )
                changed += 1
            elif submission_state in {"submitted_unknown", "outcome_unknown"}:
                await self.finalize(
                    run_id,
                    ScheduleRunState.OUTCOME_UNKNOWN,
                    completion_basis=None,
                    error_category=SchedulerErrorCategory.RUNTIME.value,
                    error_code=submission_state,
                    now=now,
                )
                changed += 1
            elif submission_state in {
                "submitted",
                "observed_active",
                "loop_idle",
                "continuation_expected",
            }:
                status = (
                    ScheduleRunState.ACCEPTED
                    if submission_state == "submitted"
                    else ScheduleRunState.WAITING
                )
                update_count = await self._database.execute_count(
                    """
                    UPDATE schedule_runs
                    SET status = ?, accepted_message_id = COALESCE(
                            accepted_message_id, ?
                        ),
                        accepted_at = CASE
                            WHEN ? = 'accepted' THEN COALESCE(accepted_at, ?)
                            ELSE accepted_at
                        END,
                        waiting_at = CASE
                            WHEN ? = 'waiting' THEN COALESCE(waiting_at, ?)
                            ELSE waiting_at
                        END,
                        last_progress_at = ?, updated_at = ?
                    WHERE run_id = ? AND status IN ('submitting', 'accepted', 'waiting')
                    """,
                    (
                        status.value,
                        row["accepted_message_id"],
                        status.value,
                        now,
                        status.value,
                        now,
                        now,
                        now,
                        run_id,
                    ),
                )
                changed += int(update_count == 1)
        return changed

    async def release_candidates(self) -> list[tuple[ScheduleRun, ScheduledTarget]]:
        rows = await self._database.fetchall(
            """
            SELECT * FROM schedule_runs
            WHERE temporary_attachment = 1 AND target_released_at IS NULL
              AND render_intent_id IS NOT NULL
              AND status IN (
                  'semantic_complete', 'blocked', 'failed', 'cancelled'
              )
              AND result_thread_id IS NOT NULL
              AND result_session_id IS NOT NULL
            ORDER BY terminal_at, run_id
            """
        )
        return [
            (
                _row_to_run(row),
                ScheduledTarget(
                    project_id=(
                        None if row["result_project_id"] is None else str(row["result_project_id"])
                    ),
                    thread_id=str(row["result_thread_id"]),
                    sdk_session_id=str(row["result_session_id"]),
                    temporary_attachment=True,
                ),
            )
            for row in rows
        ]

    async def mark_target_released(self, run_id: str, *, now: float) -> None:
        await self._database.execute(
            """
            UPDATE schedule_runs
            SET target_released_at = COALESCE(target_released_at, ?), updated_at = ?
            WHERE run_id = ? AND temporary_attachment = 1
              AND render_intent_id IS NOT NULL
            """,
            (now, now, run_id),
        )

    async def record_target_release_failure(
        self,
        run_id: str,
        *,
        error: Exception,
        now: float,
    ) -> None:
        run = await self.get_run(run_id)
        await self._database.execute(
            """
            INSERT INTO scheduler_events(
                event_id, schedule_id, run_id, event_type,
                detail, created_at
            ) VALUES (?, ?, ?, 'temporary_target_release_failed', ?, ?)
            """,
            (
                str(uuid.uuid4()),
                run.schedule_id,
                run_id,
                _canonical_json(
                    {
                        "error_type": type(error).__name__,
                        "message": str(error)[:500],
                    }
                ),
                now,
            ),
        )

    async def recover(self, *, now: float | None = None) -> dict[str, int]:
        now = time.time() if now is None else now
        counts: dict[str, int] = {}
        async with self._database.transaction() as connection:
            legacy_definitions = await _fetchall(
                connection,
                """
                SELECT id, expression, normalized_expression, timezone, state,
                       next_run_at_utc, last_planned_at_utc, created_at
                FROM schedules
                WHERE state != 'deleted'
                  AND (
                      normalized_expression IS NULL
                      OR next_run_at_utc IS NULL AND state = 'enabled'
                      OR instr(normalized_expression, ':') > 0
                  )
                """,
                (),
            )
            normalized = 0
            for row in legacy_definitions:
                anchor = (
                    float(row["last_planned_at_utc"])
                    if row["last_planned_at_utc"] is not None
                    else float(row["created_at"])
                )
                parsed = parse_schedule(
                    str(row["expression"]),
                    str(row["timezone"]),
                    anchor_utc=anchor,
                )
                next_run = row["next_run_at_utc"]
                if row["state"] == ScheduleState.ENABLED.value and next_run is None:
                    next_run = parsed.next_after(anchor)
                await connection.execute(
                    """
                    UPDATE schedules
                    SET expression = ?, normalized_expression = ?, timezone = ?,
                        next_run_at_utc = ?,
                        state = CASE WHEN ? IS NULL THEN 'disabled' ELSE state END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        parsed.source,
                        parsed.normalized,
                        parsed.timezone,
                        next_run,
                        next_run,
                        now,
                        row["id"],
                    ),
                )
                normalized += 1
            if normalized:
                counts["normalized_definitions"] = normalized
            counts["queued"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'submitting', lease_owner = NULL, lease_expires_at = NULL,
                    result_submission_id = COALESCE(
                        result_submission_id,
                        (
                            SELECT queue.id FROM message_queue AS queue
                            WHERE queue.schedule_run_id = schedule_runs.run_id
                              AND queue.state NOT IN ('cancelled', 'failed')
                            ORDER BY queue.created_at DESC LIMIT 1
                        )
                    ),
                    result_thread_id = COALESCE(
                        result_thread_id,
                        (
                            SELECT queue.thread_id FROM message_queue AS queue
                            WHERE queue.schedule_run_id = schedule_runs.run_id
                              AND queue.state NOT IN ('cancelled', 'failed')
                            ORDER BY queue.created_at DESC LIMIT 1
                        )
                    ),
                    result_session_id = COALESCE(
                        result_session_id,
                        (
                            SELECT submission.sdk_session_id
                            FROM message_queue AS queue
                            JOIN submissions AS submission
                              ON submission.submission_id = queue.id
                            WHERE queue.schedule_run_id = schedule_runs.run_id
                              AND queue.state NOT IN ('cancelled', 'failed')
                            ORDER BY queue.created_at DESC LIMIT 1
                        )
                    ),
                    updated_at = ?, last_progress_at = ?
                WHERE status IN ('claimed', 'submitting', 'retry_wait')
                  AND COALESCE(lease_expires_at, 0) <= ?
                  AND EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                        AND state NOT IN ('cancelled', 'failed')
                  )
                """,
                (now, now, now),
            )
            counts["dispatch_unknown"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'dispatch_unknown', lease_owner = NULL,
                    lease_expires_at = NULL, error_category = 'runtime',
                    error_code = 'startup_send_boundary', terminal_at = ?,
                    updated_at = ?
                WHERE status IN ('claimed', 'submitting')
                  AND COALESCE(lease_expires_at, 0) <= ?
                  AND send_started_at IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM submissions
                      WHERE schedule_run_id = schedule_runs.run_id
                        AND (
                            accepted_message_id IS NOT NULL
                            OR observed_user_event_id IS NOT NULL
                        )
                  )
                """,
                (now, now, now),
            )
            counts["reconciled_target"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'retry_wait', retry_at = ?,
                    result_thread_id = (
                        SELECT i.thread_id FROM session_creation_intents i
                        WHERE i.source_kind = 'schedule'
                          AND i.source_id = schedule_runs.run_id
                    ),
                    result_project_id = COALESCE(
                        result_project_id,
                        (
                            SELECT i.project_id FROM session_creation_intents i
                            WHERE i.source_kind = 'schedule'
                              AND i.source_id = schedule_runs.run_id
                        )
                    ),
                    lease_owner = NULL, lease_expires_at = NULL,
                    error_category = NULL, error_code = 'resume_creation_intent',
                    error_detail = NULL, updated_at = ?, last_progress_at = ?
                WHERE status IN ('claimed', 'submitting', 'target_unknown')
                  AND (
                      status = 'target_unknown'
                      OR COALESCE(lease_expires_at, 0) <= ?
                  )
                  AND session_create_started_at IS NOT NULL
                  AND result_thread_id IS NULL
                  AND send_started_at IS NULL
                  AND render_intent_id IS NULL
                  AND EXISTS (
                      SELECT 1 FROM session_creation_intents i
                      WHERE i.source_kind = 'schedule'
                        AND i.source_id = schedule_runs.run_id
                        AND i.thread_id IS NOT NULL
                        AND i.sdk_session_id = schedule_runs.result_session_id
                        AND i.state IN (
                            'thread_created', 'creating', 'attached', 'unknown'
                        )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                  )
                """,
                (now, now, now, now),
            )
            counts["target_unknown"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'retry_wait', retry_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, error_category = NULL,
                    error_code = 'resume_unreconciled_target',
                    error_detail = NULL, updated_at = ?, last_progress_at = ?
                WHERE status IN ('claimed', 'submitting')
                  AND COALESCE(lease_expires_at, 0) <= ?
                  AND session_create_started_at IS NOT NULL
                  AND result_thread_id IS NULL
                  AND send_started_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                  )
                """,
                (now, now, now, now),
            )
            counts["known_target_retry"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'retry_wait', retry_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, error_category = NULL,
                    error_code = 'resume_known_target', error_detail = NULL,
                    updated_at = ?, last_progress_at = ?
                WHERE status IN ('claimed', 'submitting')
                  AND COALESCE(lease_expires_at, 0) <= ?
                  AND session_create_started_at IS NOT NULL
                  AND result_thread_id IS NOT NULL
                  AND result_session_id IS NOT NULL
                  AND send_started_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                  )
                """,
                (now, now, now, now),
            )
            counts["retry_wait"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'retry_wait', lease_owner = NULL,
                    lease_expires_at = NULL, retry_at = ?, updated_at = ?
                WHERE status IN ('claimed', 'submitting')
                  AND COALESCE(lease_expires_at, 0) <= ?
                  AND send_started_at IS NULL
                  AND session_create_started_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM message_queue
                      WHERE schedule_run_id = schedule_runs.run_id
                  )
                """,
                (now, now, now),
            )
            counts["outcome_unknown"] = await _update_count(
                connection,
                """
                UPDATE schedule_runs
                SET status = 'outcome_unknown', error_category = 'runtime',
                    error_code = 'startup_submission_unknown',
                    terminal_at = ?, updated_at = ?
                WHERE status IN ('accepted', 'waiting')
                  AND EXISTS (
                      SELECT 1 FROM submissions
                      WHERE submission_id = schedule_runs.result_submission_id
                        AND state IN ('submitted_unknown', 'outcome_unknown')
                  )
                """,
                (now, now),
            )
            await connection.execute(
                """
                UPDATE scheduler_state
                SET recovery_completed_at = ?, worker_state = 'recovered',
                    paused_reason = NULL, updated_at = ?
                WHERE singleton = 1
                """,
                (now, now),
            )
            await connection.execute(
                """
                UPDATE global_config SET value = '0', updated_at = ?
                WHERE key = 'restart_draining'
                """,
                (now,),
            )
            await connection.execute(
                """
                UPDATE restart_intents
                SET state = 'completed', completed_at = COALESCE(completed_at, ?)
                WHERE state = 'prepared'
                """,
                (now,),
            )
        for _state, status in (
            ("dispatch_unknown", ScheduleRunState.DISPATCH_UNKNOWN),
            ("target_unknown", ScheduleRunState.TARGET_UNKNOWN),
            ("outcome_unknown", ScheduleRunState.OUTCOME_UNKNOWN),
        ):
            rows = await self._database.fetchall(
                """
                SELECT run_id FROM schedule_runs
                WHERE status = ? AND render_intent_id IS NULL
                """,
                (status.value,),
            )
            for row in rows:
                await self.finalize(
                    str(row["run_id"]),
                    status,
                    completion_basis=None,
                    error_category=(
                        SchedulerErrorCategory.TARGET.value
                        if status == ScheduleRunState.TARGET_UNKNOWN
                        else SchedulerErrorCategory.RUNTIME.value
                    ),
                    error_code=f"startup_{status.value}",
                    now=now,
                )
        return counts

    async def closed_queued_runs_for_redrive(self) -> list[ScheduleRun]:
        rows = await self._database.fetchall(
            """
            SELECT DISTINCT run.*
            FROM schedule_runs AS run
            JOIN message_queue AS queue
              ON queue.schedule_run_id = run.run_id
            JOIN submissions AS submission
              ON submission.submission_id = queue.id
            JOIN session_bindings AS binding
              ON binding.thread_id = queue.thread_id
             AND binding.sdk_session_id = submission.sdk_session_id
            WHERE run.status = 'submitting'
              AND run.send_started_at IS NULL
              AND queue.state = 'local_queued'
              AND submission.state = 'local_queued'
              AND submission.origin = 'app_schedule'
              AND binding.binding_intent = 'closed'
            ORDER BY run.created_at, run.run_id
            """
        )
        return [_row_to_run(row) for row in rows]

    async def status(self, *, now: float | None = None) -> SchedulerStatus:
        timestamp = time.time() if now is None else now
        state = await self._database.fetchone("SELECT * FROM scheduler_state WHERE singleton = 1")
        counts = await self._database.fetchone(
            """
            SELECT
              (SELECT COUNT(*) FROM schedules WHERE state = 'enabled')
                AS enabled_definitions,
              (SELECT COUNT(*) FROM schedules
               WHERE state = 'enabled' AND next_run_at_utc <= ?) AS due_definitions,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN ('pending', 'retry_wait')) AS pending_runs,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN ('claimed', 'submitting')) AS claimed_runs,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN ('accepted', 'waiting')) AS waiting_runs,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN (
                   'target_unknown', 'dispatch_unknown', 'outcome_unknown'
               )) AS unknown_runs
            """,
            (timestamp,),
        )
        blockers = await self.restart_blockers()
        assert state is not None and counts is not None
        return SchedulerStatus(
            worker_state=str(state["worker_state"]),
            owner_id=None if state["owner_id"] is None else str(state["owner_id"]),
            recovery_completed_at=(
                None
                if state["recovery_completed_at"] is None
                else float(state["recovery_completed_at"])
            ),
            last_tick_at=(None if state["last_tick_at"] is None else float(state["last_tick_at"])),
            last_clock_utc=(
                None if state["last_clock_utc"] is None else float(state["last_clock_utc"])
            ),
            paused_reason=(None if state["paused_reason"] is None else str(state["paused_reason"])),
            enabled_definitions=int(counts["enabled_definitions"]),
            due_definitions=int(counts["due_definitions"]),
            pending_runs=int(counts["pending_runs"]),
            claimed_runs=int(counts["claimed_runs"]),
            waiting_runs=int(counts["waiting_runs"]),
            unknown_runs=int(counts["unknown_runs"]),
            restart_blockers=tuple(blockers),
        )

    async def restart_blockers(self) -> list[str]:
        rows = await self._database.fetchall(
            """
            SELECT run_id, status FROM schedule_runs
            WHERE status IN ('claimed', 'submitting', 'accepted', 'waiting')
            ORDER BY created_at, run_id
            """
        )
        blockers = [f"schedule_run:{row['run_id']}:{row['status']}" for row in rows]
        liveness = await self._database.fetchall(
            """
            SELECT sdk_session_id, kind, source_id FROM liveness_leases
            WHERE state = 'active' ORDER BY sdk_session_id, lease_id
            """
        )
        blockers.extend(
            f"liveness:{row['sdk_session_id']}:{row['kind']}:{row['source_id']}" for row in liveness
        )
        remote = await self._database.fetchall(
            """
            SELECT sdk_session_id, runtime_remote_mode FROM session_bindings
            WHERE runtime_remote_mode IN ('on', 'unknown')
            ORDER BY sdk_session_id
            """
        )
        blockers.extend(
            f"remote:{row['sdk_session_id']}:{row['runtime_remote_mode']}" for row in remote
        )
        native = await self._database.fetchall(
            """
            SELECT sdk_session_id, runtime_schedule_id, state
            FROM runtime_schedules WHERE state IN ('active', 'unknown')
            ORDER BY sdk_session_id, runtime_schedule_id
            """
        )
        blockers.extend(
            f"native_schedule:{row['sdk_session_id']}:{row['runtime_schedule_id']}:{row['state']}"
            for row in native
        )
        interactions = await self._database.fetchall(
            """
            SELECT interaction_id, sdk_session_id FROM pending_interactions
            WHERE state = 'pending' ORDER BY interaction_id
            """
        )
        blockers.extend(
            f"interaction:{row['sdk_session_id']}:{row['interaction_id']}" for row in interactions
        )
        creations = await self._database.fetchall(
            """
            SELECT creation_token, state FROM session_creation_intents
            WHERE state NOT IN ('attached', 'failed')
            ORDER BY created_at, creation_token
            """
        )
        blockers.extend(
            f"creation_intent:{row['creation_token']}:{row['state']}" for row in creations
        )
        worktrees = await self._database.fetchall(
            """
            SELECT intent_id, state FROM worktree_intents
            WHERE state NOT IN ('ready', 'closed', 'failed', 'compensated')
            ORDER BY created_at, intent_id
            """
        )
        blockers.extend(f"worktree_intent:{row['intent_id']}:{row['state']}" for row in worktrees)
        return blockers

    async def prepare_restart(
        self,
        *,
        requested_by: str,
        force: bool,
        now: float | None = None,
    ) -> str:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO global_config(key, value, updated_at)
                VALUES ('restart_draining', '1', ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = '1', updated_at = excluded.updated_at
                """,
                (timestamp,),
            )
            await connection.execute(
                """
                UPDATE scheduler_state
                SET worker_state = 'draining', paused_reason = 'restart_prepared',
                    updated_at = ?
                WHERE singleton = 1
                """,
                (timestamp,),
            )
        blockers = await self.restart_blockers()
        if blockers and not force:
            async with self._database.transaction() as connection:
                await connection.execute(
                    """
                    UPDATE global_config SET value = '0', updated_at = ?
                    WHERE key = 'restart_draining'
                    """,
                    (timestamp,),
                )
                await connection.execute(
                    """
                    UPDATE scheduler_state
                    SET worker_state = 'running', paused_reason = NULL, updated_at = ?
                    WHERE singleton = 1
                    """,
                    (timestamp,),
                )
            raise ScheduleConflict("restart blocked by " + ", ".join(blockers))
        restart_id = str(uuid.uuid4())
        affected: list[str] = []
        if force:
            rows = await self._database.fetchall(
                """
                SELECT r.run_id, r.status,
                       EXISTS (
                           SELECT 1 FROM submissions s
                           WHERE s.submission_id = r.result_submission_id
                             AND (
                                 s.accepted_message_id IS NOT NULL
                                 OR s.observed_user_event_id IS NOT NULL
                             )
                       ) AS dispatch_observed
                FROM schedule_runs r
                WHERE r.status IN ('claimed', 'submitting', 'accepted', 'waiting')
                """
            )
            for row in rows:
                run_id = str(row["run_id"])
                current = str(row["status"])
                target = (
                    ScheduleRunState.OUTCOME_UNKNOWN
                    if current in {"accepted", "waiting"} or bool(row["dispatch_observed"])
                    else ScheduleRunState.DISPATCH_UNKNOWN
                )
                await self.finalize(
                    run_id,
                    target,
                    completion_basis=None,
                    error_category=SchedulerErrorCategory.RUNTIME.value,
                    error_code="forced_restart",
                    detail="forced restart crossed an unresolved schedule run boundary",
                    now=timestamp,
                )
                affected.append(run_id)
            async with self._database.transaction() as connection:
                await connection.execute(
                    """
                    UPDATE submissions
                    SET state = CASE
                            WHEN state IN ('local_queued', 'submitting', 'submitted')
                            THEN 'submitted_unknown'
                            ELSE 'outcome_unknown'
                        END,
                        terminal_at = COALESCE(terminal_at, ?)
                    WHERE state IN (
                        'submitting', 'submitted', 'submitted_unknown',
                        'observed_active', 'loop_idle', 'continuation_expected'
                    )
                    """,
                    (timestamp,),
                )
                await connection.execute(
                    """
                    UPDATE runtime_schedules
                    SET state = 'unknown', updated_at = ?
                    WHERE state = 'active'
                    """,
                    (timestamp,),
                )
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET runtime_remote_mode = 'unknown',
                        attachment_state = CASE
                            WHEN attachment_state = 'attached'
                            THEN 'recovery_unknown'
                            ELSE attachment_state
                        END,
                        permission_posture = 'unknown',
                        updated_at = ?, row_version = row_version + 1
                    WHERE runtime_remote_mode IN ('on', 'export', 'unknown')
                       OR attachment_state = 'attached'
                    """,
                    (timestamp,),
                )
                await connection.execute(
                    """
                    UPDATE liveness_leases
                    SET state = 'orphaned', refreshed_at = ?, released_at = ?
                    WHERE state = 'active'
                    """,
                    (timestamp, timestamp),
                )
        await self._database.execute(
            """
            INSERT INTO restart_intents(
                restart_id, requested_by, force, state, blockers_json,
                affected_runs_json, requested_at
            ) VALUES (?, ?, ?, 'prepared', ?, ?, ?)
            """,
            (
                restart_id,
                requested_by,
                int(force),
                _canonical_json(blockers),
                _canonical_json(affected),
                timestamp,
            ),
        )
        return restart_id

    async def mark_tick(
        self,
        owner_id: str,
        *,
        now: float,
        worker_state: str = "running",
        paused_reason: str | None = None,
    ) -> None:
        previous = await self._database.fetchone(
            "SELECT last_clock_utc FROM scheduler_state WHERE singleton = 1"
        )
        previous_clock = (
            None
            if previous is None or previous["last_clock_utc"] is None
            else float(previous["last_clock_utc"])
        )
        await self._database.execute(
            """
            UPDATE scheduler_state
            SET owner_id = ?, worker_state = ?, last_tick_at = ?,
                last_clock_utc = MAX(COALESCE(last_clock_utc, ?), ?),
                last_wake_at = CASE
                    WHEN last_clock_utc IS NOT NULL AND ? > last_clock_utc + 60
                    THEN ?
                    ELSE last_wake_at
                END,
                paused_reason = ?, updated_at = ?
            WHERE singleton = 1 AND worker_state != 'draining'
            """,
            (
                owner_id,
                worker_state,
                now,
                now,
                now,
                now,
                now,
                paused_reason,
                now,
            ),
        )
        if previous_clock is not None and now < previous_clock:
            await self._database.execute(
                """
                INSERT INTO scheduler_events(
                    event_id, event_type, owner_id, detail, created_at
                ) VALUES (?, 'clock_jump_backward', ?, ?, ?)
                ON CONFLICT(event_id) DO NOTHING
                """,
                (
                    str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"copilotd:scheduler-clock-jump:{owner_id}:{previous_clock}:{now}",
                        )
                    ),
                    owner_id,
                    _canonical_json({"previous": previous_clock, "observed": now}),
                    now,
                ),
            )


class SchedulerWorker:
    def __init__(
        self,
        repository: SchedulerRepository,
        adapter: SchedulerTargetAdapter,
        *,
        owner_id: str,
        clock: Clock | None = None,
        poll_seconds: float = 1.0,
        lease_seconds: float = SCHEDULE_LEASE_SECONDS,
        renew_seconds: float = SCHEDULE_RENEW_SECONDS,
    ) -> None:
        self._repository = repository
        self._adapter = adapter
        self._owner_id = owner_id
        self._clock = SystemClock() if clock is None else clock
        self._poll_seconds = poll_seconds
        self._lease_seconds = lease_seconds
        self._renew_seconds = renew_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        status = await self._repository.status(now=self._clock.now())
        if status.recovery_completed_at is None:
            raise SchedulerNotRecovered("scheduler cannot start before startup recovery")
        if self._task is not None:
            raise RuntimeError("scheduler worker is already started")
        self._stop.clear()
        self._task = asyncio.create_task(
            self.run(),
            name=f"scheduler:{self._owner_id}",
        )

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._repository.mark_tick(
            self._owner_id,
            now=self._clock.now(),
            worker_state="stopped",
        )

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._repository.mark_tick(
                    self._owner_id,
                    now=self._clock.now(),
                    worker_state="degraded",
                    paused_reason=f"{type(error).__name__}: {error}",
                )
            await self._clock.sleep(self._poll_seconds)

    async def tick(self, *, dispatch_limit: int = 100) -> int:
        now = self._clock.now()
        status = await self._repository.status(now=now)
        if status.recovery_completed_at is None:
            raise SchedulerNotRecovered("scheduler tick is blocked until recovery completes")
        if status.worker_state == "draining":
            return 0
        await self._repository.mark_tick(self._owner_id, now=now)
        await self._repository.plan_due(self._owner_id, now=now, limit=dispatch_limit)
        await self._repository.reconcile_submissions(now=now)
        await self._redrive_closed_queues()
        await self._release_temporary_targets()
        dispatched = 0
        while dispatched < dispatch_limit:
            run = await self._repository.claim_next(
                self._owner_id,
                now=self._clock.now(),
                lease_seconds=self._lease_seconds,
            )
            if run is None:
                break
            await self._dispatch(run)
            dispatched += 1
        await self._release_temporary_targets()
        return dispatched

    async def _redrive_closed_queues(self) -> None:
        for run in await self._repository.closed_queued_runs_for_redrive():
            try:
                definition = await self._repository.require(run.schedule_id)
                target = await self._adapter.reconcile_target(definition, run)
                if target is None and definition.kind == ScheduleKind.MESSAGE:
                    target = await self._adapter.prepare_message_target(definition, run)
                if target is not None:
                    await self._adapter.queue_ready(target, run.run_id)
            except Exception:
                # The durable local queue remains authoritative. A later tick retries
                # after stale startup owner leases or runtime attachment failures clear.
                continue

    async def _dispatch(self, run: ScheduleRun) -> None:
        definition = await self._repository.require(run.schedule_id)
        recovering_new_session_target = (
            definition.kind == ScheduleKind.NEW_SESSION
            and run.session_create_started_at is not None
        )
        run = await self._repository.mark_target_started(
            run,
            self._owner_id,
            new_session=definition.kind == ScheduleKind.NEW_SESSION,
            now=self._clock.now(),
        )
        renewal = asyncio.create_task(
            self._renew_lease(run),
            name=f"schedule-renew:{run.run_id}",
        )
        queued = False
        try:
            if definition.kind == ScheduleKind.MESSAGE:
                target = await self._adapter.prepare_message_target(definition, run)
            elif recovering_new_session_target:
                target = await self._adapter.reconcile_target(definition, run)
                if target is None:
                    raise SchedulerDispatchError(
                        "new-session target reconciliation has not completed",
                        category=SchedulerErrorCategory.TARGET,
                        code="new_session_target_reconcile_pending",
                        retryable=True,
                    )
            else:
                target = await self._adapter.prepare_new_session_target(definition, run)
            run = await self._repository.record_target(
                run,
                self._owner_id,
                target,
                now=self._clock.now(),
            )
            await self._repository.enqueue(
                definition,
                run,
                self._owner_id,
                target,
                now=self._clock.now(),
            )
            queued = True
            await self._adapter.queue_ready(target, run.run_id)
        except SchedulerDispatchError as error:
            if queued:
                return
            await self._repository.retry_or_fail(
                run,
                self._owner_id,
                error,
                now=self._clock.now(),
            )
        except Exception as error:
            if queued:
                return
            wrapped = SchedulerDispatchError(
                str(error),
                category=SchedulerErrorCategory.INTERNAL,
                code=type(error).__name__,
                retryable=True,
            )
            await self._repository.retry_or_fail(
                run,
                self._owner_id,
                wrapped,
                now=self._clock.now(),
            )
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)

    async def _renew_lease(self, run: ScheduleRun) -> None:
        while True:
            await asyncio.sleep(self._renew_seconds)
            renewed = await self._repository.renew_run(
                run.run_id,
                self._owner_id,
                run.fence_token,
                now=self._clock.now(),
                lease_seconds=self._lease_seconds,
            )
            if not renewed:
                return

    async def _release_temporary_targets(self) -> None:
        for run, target in await self._repository.release_candidates():
            try:
                await self._adapter.release_temporary_target(target, run)
            except Exception as error:
                await self._repository.record_target_release_failure(
                    run.run_id,
                    error=error,
                    now=self._clock.now(),
                )
                continue
            await self._repository.mark_target_released(
                run.run_id,
                now=self._clock.now(),
            )


class DeterministicSchedulerAdapter:
    """In-memory adapter used by deterministic tests and local contract probes."""

    def __init__(
        self,
        message_targets: dict[str, ScheduledTarget] | None = None,
    ) -> None:
        self.message_targets = {} if message_targets is None else message_targets
        self.new_session_targets: dict[str, ScheduledTarget] = {}
        self.prepare_calls: list[str] = []
        self.queue_notifications: list[tuple[str, str]] = []
        self.release_calls: list[str] = []
        self.on_prepare: Callable[[ScheduleRun], Awaitable[None]] | None = None

    async def prepare_message_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget:
        self.prepare_calls.append(run.run_id)
        if self.on_prepare is not None:
            await self.on_prepare(run)
        target = self.message_targets.get(definition.id)
        if target is None:
            snapshot = definition.target_snapshot
            target = ScheduledTarget(
                project_id=definition.project_id,
                thread_id=str(snapshot["thread_id"]),
                sdk_session_id=str(snapshot["sdk_session_id"]),
            )
        return target

    async def prepare_new_session_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget:
        self.prepare_calls.append(run.run_id)
        if self.on_prepare is not None:
            await self.on_prepare(run)
        existing = self.new_session_targets.get(run.run_id)
        if existing is not None:
            return existing
        target = ScheduledTarget(
            project_id=definition.project_id,
            thread_id=f"thread-{run.run_id}",
            sdk_session_id=str(run.result_session_id),
        )
        self.new_session_targets[run.run_id] = target
        return target

    async def reconcile_target(
        self,
        _definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget | None:
        return self.new_session_targets.get(run.run_id)

    async def queue_ready(self, target: ScheduledTarget, run_id: str) -> None:
        self.queue_notifications.append((target.thread_id, run_id))

    async def release_temporary_target(
        self,
        _target: ScheduledTarget,
        run: ScheduleRun,
    ) -> None:
        self.release_calls.append(run.run_id)


def _parse_definition(definition: ScheduleDefinition) -> ParsedSchedule:
    return parse_schedule(
        definition.expression,
        definition.timezone,
        anchor_utc=definition.created_at,
    )


def _row_to_definition(row: Row) -> ScheduleDefinition:
    return ScheduleDefinition(
        id=str(row["id"]),
        project_id=None if row["project_id"] is None else str(row["project_id"]),
        thread_id=None if row["thread_id"] is None else str(row["thread_id"]),
        channel_id=None if row["channel_id"] is None else str(row["channel_id"]),
        kind=ScheduleKind(str(row["kind"])),
        expression=str(row["expression"]),
        normalized_expression=str(row["normalized_expression"] or row["expression"]),
        timezone=str(row["timezone"]),
        payload=json.loads(str(row["payload"])),
        target_snapshot=json.loads(str(row["target_snapshot"])),
        misfire_policy=MisfirePolicy(str(row["misfire_policy"])),
        state=ScheduleState(str(row["state"])),
        next_run_at_utc=(None if row["next_run_at_utc"] is None else float(row["next_run_at_utc"])),
        planner_fence_token=int(row["planner_fence_token"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        name=None if row["name"] is None else str(row["name"]),
        created_by=None if row["created_by"] is None else str(row["created_by"]),
    )


def _row_to_run(row: Row) -> ScheduleRun:
    return ScheduleRun(
        run_id=str(row["run_id"]),
        schedule_id=str(row["schedule_id"]),
        planned_key=str(row["planned_key"]),
        planned_at_utc=float(row["planned_at_utc"]),
        status=ScheduleRunState(str(row["status"])),
        attempt=int(row["attempt"]),
        fence_token=int(row["fence_token"] or 0),
        lease_owner=None if row["lease_owner"] is None else str(row["lease_owner"]),
        lease_expires_at=(
            None if row["lease_expires_at"] is None else float(row["lease_expires_at"])
        ),
        creation_intent_id=(
            None if row["creation_intent_id"] is None else str(row["creation_intent_id"])
        ),
        session_create_started_at=(
            None
            if row["session_create_started_at"] is None
            else float(row["session_create_started_at"])
        ),
        send_started_at=(None if row["send_started_at"] is None else float(row["send_started_at"])),
        accepted_message_id=(
            None if row["accepted_message_id"] is None else str(row["accepted_message_id"])
        ),
        completion_basis=(
            None if row["completion_basis"] is None else str(row["completion_basis"])
        ),
        result_project_id=(
            None if row["result_project_id"] is None else str(row["result_project_id"])
        ),
        result_thread_id=(
            None if row["result_thread_id"] is None else str(row["result_thread_id"])
        ),
        result_session_id=(
            None if row["result_session_id"] is None else str(row["result_session_id"])
        ),
        result_submission_id=(
            None if row["result_submission_id"] is None else str(row["result_submission_id"])
        ),
        render_intent_id=(
            None if row["render_intent_id"] is None else str(row["render_intent_id"])
        ),
        retry_at=None if row["retry_at"] is None else float(row["retry_at"]),
        error_category=(None if row["error_category"] is None else str(row["error_category"])),
        error_code=None if row["error_code"] is None else str(row["error_code"]),
        error_detail=(None if row["error_detail"] is None else str(row["error_detail"])),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
    )


def _run_id(schedule_id: str, key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:schedule:{schedule_id}:{key}"))


def _submission_id(run_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:schedule-run:{run_id}:submission"))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _terminal_content(
    run_id: str,
    status: ScheduleRunState,
    *,
    completion_basis: str | None,
    error_code: str | None,
    detail: str | None,
) -> str:
    suffix = f" ({completion_basis})" if completion_basis else ""
    content = f"Scheduled run `{run_id}` is `{status.value}`{suffix}."
    if error_code:
        content += f" Error: `{error_code}`."
    if detail:
        content += f" {detail}"
    return content


def _schedule_render_destination(row: Row) -> str:
    if row["result_session_id"] is not None and bool(row["result_session_bound"]):
        return str(row["result_session_id"])
    thread_id = row["result_thread_id"] or row["schedule_thread_id"]
    if thread_id is not None:
        return f"thread:{thread_id}"
    if row["schedule_channel_id"] is not None:
        return f"channel:{row['schedule_channel_id']}"
    return "ops:scheduler"


async def _fetchone(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> Row | None:
    cursor = await connection.execute(statement, parameters)
    row = await cursor.fetchone()
    await cursor.close()
    return row


async def _fetchall(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> list[Row]:
    cursor = await connection.execute(statement, parameters)
    rows = list(await cursor.fetchall())
    await cursor.close()
    return rows


async def _update_count(
    connection: Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> int:
    cursor = await connection.execute(statement, parameters)
    count = cursor.rowcount
    await cursor.close()
    return count


async def _record_scheduler_event(
    connection: Connection,
    *,
    event_type: str,
    schedule_id: str | None,
    run_id: str | None,
    owner_id: str | None,
    fence_token: int | None,
    detail: dict[str, Any],
    now: float,
) -> None:
    identity = _canonical_json(
        {
            "event_type": event_type,
            "schedule_id": schedule_id,
            "run_id": run_id,
            "owner_id": owner_id,
            "fence_token": fence_token,
            "detail": detail,
        }
    )
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:scheduler-event:{identity}"))
    await connection.execute(
        """
        INSERT INTO scheduler_events(
            event_id, schedule_id, run_id, event_type,
            owner_id, fence_token, detail, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO NOTHING
        """,
        (
            event_id,
            schedule_id,
            run_id,
            event_type,
            owner_id,
            fence_token,
            _canonical_json(detail),
            now,
        ),
    )


async def _require_scheduler_admission(
    connection: Connection,
    *,
    project_id: str | None,
) -> None:
    scheduler = await _fetchone(
        connection,
        "SELECT worker_state FROM scheduler_state WHERE singleton = 1",
        (),
    )
    if scheduler is not None and scheduler["worker_state"] == "draining":
        raise ScheduleConflict("scheduler is draining for restart")
    if project_id is None:
        return
    project = await _fetchone(
        connection,
        "SELECT state, project_kind FROM projects WHERE id = ?",
        (project_id,),
    )
    if project is None:
        raise ScheduleConflict("schedule project does not exist")
    if project["state"] == "closing" or (
        project["project_kind"] == "worktree" and project["state"] == "retired"
    ):
        raise ScheduleConflict("schedule project is closing or closed")
