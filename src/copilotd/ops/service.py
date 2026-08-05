from __future__ import annotations

import getpass
import hashlib
import json
import os
import plistlib
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import SecretStr

from copilotd.config import Settings
from copilotd.ops.contracts import (
    FORCE_RESTART_DRAIN_SECONDS,
    RESTART_STORM_LIMIT,
    RESTART_STORM_WINDOW_SECONDS,
    SERVICE_CONTROL_PROTOCOL_VERSION,
    SERVICE_STATE_SCHEMA_VERSION,
    SERVICE_STATUS_SCHEMA_VERSION,
    WATCHDOG_INTERVAL_SECONDS,
)
from copilotd.ops.control import fence_marker_paths
from copilotd.ops.heartbeat import HeartbeatSnapshot, heartbeat_age_seconds, read_heartbeat
from copilotd.ops.wake import ResumeTimestampProvider, resume_timestamp_provider

Topology = Literal["bundled-runtime", "sidecar"]
EffectiveState = Literal["running", "loaded", "stopped", "missing", "unknown"]

_MAC_RUNTIME_LABEL = "com.github.copilotd.runtime"
_MAC_BOT_LABEL = "com.github.copilotd.bot"
_MAC_WATCHDOG_LABEL = "com.github.copilotd.watchdog"
_WINDOWS_RUNTIME_TASK = "copilotD Runtime"
_WINDOWS_BOT_TASK = "copilotD Bot"
_WINDOWS_WATCHDOG_TASK = "copilotD Watchdog"
_TASK_NAMESPACE = "http://schemas.microsoft.com/windows/2004/02/mit/task"

_INFLIGHT_SUBMISSION_STATES = {
    "submitting",
    "submitted",
    "submitted_unknown",
    "observed_active",
    "continuation_expected",
}
_TERMINAL_SCHEDULE_RUN_STATES = {
    "cancelled",
    "completed",
    "failed",
    "outcome_unknown",
    "target_unknown",
    "dispatch_unknown",
}


class ServiceError(RuntimeError):
    pass


class ServiceVerificationError(ServiceError):
    pass


class RestartBlocked(ServiceError):
    def __init__(self, blockers: Sequence[str]) -> None:
        self.blockers = tuple(blockers)
        super().__init__("restart is not detach-safe: " + ", ".join(self.blockers))


class ForcePreparationUncertain(ServiceError):
    pass


@dataclass(frozen=True, slots=True)
class CommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(self, command: Sequence[str], *, check: bool = False) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(self, command: Sequence[str], *, check: bool = False) -> CommandResult:
        try:
            completed = subprocess.run(
                list(command),
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as error:
            result = CommandResult(
                command=tuple(command),
                returncode=127,
                stdout="",
                stderr=str(error),
            )
        else:
            result = CommandResult(
                command=tuple(command),
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no output"
            raise ServiceError(
                f"command failed ({result.returncode}): {' '.join(result.command)}: {detail}"
            )
        return result


@dataclass(frozen=True, slots=True)
class ServiceUnitStatus:
    name: str
    manager_id: str
    definition_path: str
    installed_definition: bool
    effective_state: EffectiveState
    pid: int | None
    process_started_at: float | None
    definition_matches: bool
    expected_definition_hash: str
    effective_definition_hash: str | None
    detail: str | None = None

    @property
    def loaded(self) -> bool:
        return self.effective_state not in {"missing", "unknown"}


@dataclass(frozen=True, slots=True)
class LeaseMetrics:
    active_submissions: int = 0
    observed_background_tasks: int = 0
    pending_interactions: int = 0
    total: int = 0


@dataclass(frozen=True, slots=True)
class QueueMetrics:
    ingress_queue_depth: int = 0
    max_reducer_lag_ms: int = 0
    local_pending: int = 0
    render_pending: int = 0
    last_callback_at: str | None = None
    last_reducer_progress_at: str | None = None


@dataclass(frozen=True, slots=True)
class ExposureMetrics:
    remote_steerable_or_unknown_sessions: int = 0
    active_or_unknown_native_schedules: int = 0


@dataclass(frozen=True, slots=True)
class ServiceStatus:
    schema_version: int
    platform: str
    topology: Topology
    installed: bool
    effective_state: str
    ready: bool
    bot_loaded: bool
    watchdog_loaded: bool
    runtime_loaded: bool
    pid: int | None
    process_generation: str | None
    process_started_at: str | None
    manager_process_started_at: float | None
    process_identity_matches: bool | None
    service_control_protocol: int | None
    heartbeat_age_seconds: float | None
    heartbeat_written_at: str | None
    heartbeat_fresh: bool
    heartbeat_frozen: bool
    heartbeat_error: str | None
    gateway_state: str | None
    runtime_state: str | None
    protected_work: bool | None
    active_leases: LeaseMetrics
    queue: QueueMetrics
    exposure: ExposureMetrics
    units: tuple[ServiceUnitStatus, ...]
    definition_drift: tuple[str, ...]
    last_resume_at: str | None
    wake_suppression_until: str | None


@dataclass(frozen=True, slots=True)
class InstallReceipt:
    installed_at: float
    topology: Topology
    previous_pid: int | None
    previous_generation: str | None
    previous_process_started_at: str | None
    expected_units: tuple[str, ...]
    definition_hashes: dict[str, str]


@dataclass(frozen=True, slots=True)
class QuiesceFence:
    fence_id: str
    expected_pid: int
    expected_generation: str
    expected_process_started_at: float
    protocol_version: int
    handoff_token_hash: str
    requested_at: float
    acknowledged_at: float | None = None
    ingress_depth: int | None = None
    violation_count: int = 0


@dataclass(frozen=True, slots=True)
class RestartSafetySnapshot:
    captured_at: float
    active_leases: LeaseMetrics
    local_pending: int
    pending_operations: int
    remote_sessions: int
    native_schedules: int
    native_trigger_windows: int
    blockers: tuple[str, ...]
    ingress_depth: int = 0


@dataclass(frozen=True, slots=True)
class ForceRestartOutcome:
    submissions_unknown: int
    operations_unknown: int
    interactions_cancelled: int
    remote_unknown: int
    native_schedules_unknown: int
    native_triggers_unknown: int
    leases_orphaned: int
    bounded: bool
    detail: str
    intents_recorded: int = 0


@dataclass(frozen=True, slots=True)
class RestartReceipt:
    requested_at: float
    force: bool
    previous_pid: int
    previous_generation: str
    previous_process_started_at: str
    safety_snapshot: RestartSafetySnapshot
    force_outcome: ForceRestartOutcome | None
    admission_fence_id: str


class RestartCoordinator(Protocol):
    def request_quiesce(
        self,
        *,
        expected_pid: int,
        expected_generation: str,
        expected_process_started_at: float,
        handoff_token: str = "",
        now: float,
    ) -> QuiesceFence: ...

    def wait_for_quiesce(
        self,
        fence: QuiesceFence,
        *,
        timeout_seconds: float,
    ) -> QuiesceFence: ...

    def snapshot_under_fence(
        self,
        fence: QuiesceFence,
        *,
        now: float,
    ) -> RestartSafetySnapshot: ...

    def assert_quiesced(self, fence: QuiesceFence) -> None: ...

    def commit_quiesce(self, fence: QuiesceFence, *, now: float) -> None: ...

    def commit_irreversible_quiesce(
        self,
        fence: QuiesceFence,
        *,
        now: float,
        reason: str,
    ) -> None: ...

    def is_irreversible(self, fence: QuiesceFence) -> bool: ...

    def release_quiesce(
        self,
        fence: QuiesceFence,
        *,
        now: float,
        reason: str,
    ) -> None: ...

    def snapshot(self, *, now: float) -> RestartSafetySnapshot: ...

    def prepare_force(
        self,
        fence: QuiesceFence,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> ForceRestartOutcome: ...

    def prepare_replay_restart(
        self,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> bool: ...


class SqliteRestartCoordinator:
    """Durably fences ambiguous work before the OS process is replaced."""

    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path

    def request_quiesce(
        self,
        *,
        expected_pid: int,
        expected_generation: str,
        expected_process_started_at: float,
        handoff_token: str = "",
        now: float,
    ) -> QuiesceFence:
        fence = QuiesceFence(
            fence_id=str(uuid.uuid4()),
            expected_pid=expected_pid,
            expected_generation=expected_generation,
            expected_process_started_at=expected_process_started_at,
            protocol_version=SERVICE_CONTROL_PROTOCOL_VERSION,
            handoff_token_hash=_token_hash(handoff_token),
            requested_at=now,
        )
        for marker in fence_marker_paths(self._database_path, fence.fence_id):
            marker.unlink(missing_ok=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                """
                SELECT fence_id, state, expected_pid, expected_generation,
                       expected_process_started_at, requested_at
                FROM service_admission_fences
                WHERE state IN (
                  'requested', 'acknowledged', 'violated',
                  'prepared', 'committed'
                )
                LIMIT 1
                """
            ).fetchone()
            if active is not None:
                active_state = str(active["state"])
                if active_state in {"prepared", "committed"}:
                    raise ServiceError(
                        "an irreversible restart fence requires replacement "
                        f"adoption: {active['fence_id']}"
                    )
                stale = (
                    int(active["expected_pid"]) != expected_pid
                    or str(active["expected_generation"]) != expected_generation
                    or not _timestamps_match(
                        active["expected_process_started_at"],
                        expected_process_started_at,
                    )
                    or now - float(active["requested_at"]) > 60
                )
                if not stale:
                    raise ServiceError(
                        f"restart admission fence is already active: {active['fence_id']}"
                    )
                connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'released', released_at = ?,
                        rollback_state = 'pending', detail = ?
                    WHERE fence_id = ?
                    """,
                    (
                        now,
                        json.dumps(
                            {"reason": "superseded_stale_fence"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        active["fence_id"],
                    ),
                )
            connection.execute(
                """
                INSERT INTO service_admission_fences(
                    fence_id, expected_pid, expected_generation,
                    expected_process_started_at, state, requested_at,
                    baseline_journal_id, protocol_version,
                    handoff_token_hash, rollback_state, detail
                ) VALUES (
                    ?, ?, ?, ?, 'requested', ?,
                    (SELECT COALESCE(MAX(journal_id), 0)
                     FROM event_journal),
                    ?, ?, 'none', ?
                )
                """,
                (
                    fence.fence_id,
                    fence.expected_pid,
                    fence.expected_generation,
                    fence.expected_process_started_at,
                    fence.requested_at,
                    fence.protocol_version,
                    fence.handoff_token_hash,
                    json.dumps(
                        {"reason": "service_restart"},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            connection.commit()
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(f"could not request restart quiesce: {error}") from error
        finally:
            connection.close()
        return fence

    def recover_for_replacement(
        self,
        *,
        replacement_pid: int,
        replacement_generation: str,
        replacement_process_started_at: float,
        manager_handoff_token: str | None,
        replacement_is_managed: bool,
        old_process_identity_alive: bool,
        now: float,
    ) -> str:
        row = self._active_fence_row()
        if row is None:
            return "none"
        if not replacement_is_managed:
            raise ServiceError(
                "replacement process is not the effective OS-managed bot"
            )
        if old_process_identity_alive:
            raise ServiceError(
                "refusing replacement adoption while old process is alive"
            )
        if (
            int(row["protocol_version"]) >= SERVICE_CONTROL_PROTOCOL_VERSION
            and (
                manager_handoff_token is None
                or _token_hash(manager_handoff_token)
                != str(row["handoff_token_hash"])
            )
        ):
            raise ServiceError("replacement manager handoff token is invalid")
        same_process = (
            int(row["expected_pid"]) == replacement_pid
            and str(row["expected_generation"]) == replacement_generation
            and _timestamps_match(
                row["expected_process_started_at"],
                replacement_process_started_at,
            )
        )
        if same_process:
            raise ServiceError(
                f"service fence still belongs to this process: {row['fence_id']}"
            )
        state = str(row["state"])
        if state == "prepared":
            fence = self._row_to_fence(row)
            self.commit_irreversible_quiesce(
                fence,
                now=now,
                reason="replacement_completed_irreversible_handoff",
            )
            state = "committed"
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if state == "committed":
                committed = connection.execute(
                    """
                    SELECT producer_count, acknowledged_producer_count,
                           acknowledged_journal_id, violation_count,
                           (SELECT COALESCE(MAX(journal_id), 0)
                            FROM event_journal) AS current_journal_id
                    FROM service_admission_fences
                    WHERE fence_id = ? AND state = 'committed'
                    """,
                    (row["fence_id"],),
                ).fetchone()
                if committed is None:
                    raise ServiceError(
                        "committed service fence disappeared during adoption"
                    )
                acknowledged_producer_count = committed[
                    "acknowledged_producer_count"
                ]
                acknowledged_journal_id = committed[
                    "acknowledged_journal_id"
                ]
                if (
                    acknowledged_producer_count is None
                    or acknowledged_journal_id is None
                    or int(committed["producer_count"])
                    != int(acknowledged_producer_count)
                    or int(committed["current_journal_id"])
                    != int(acknowledged_journal_id)
                    or int(committed["violation_count"]) > 0
                    or self._has_fence_failure_marker(str(row["fence_id"]))
                ):
                    self._mark_post_commit_producer_unknown(
                        connection,
                        fence_id=str(row["fence_id"]),
                        now=now,
                        kind="post_commit_producer_violation",
                        reason="producer_after_owner_handoff",
                    )
                cursor = connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'released', released_at = ?,
                        rollback_state = 'complete', detail = ?
                    WHERE fence_id = ? AND state = 'committed'
                      AND owner_handoff_at IS NOT NULL
                    """,
                    (
                        now,
                        json.dumps(
                            {
                                "reason": "replacement_generation_adopted",
                                "replacement_pid": replacement_pid,
                                "replacement_generation":
                                    replacement_generation,
                                "replacement_process_started_at":
                                    replacement_process_started_at,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        row["fence_id"],
                    ),
                )
            else:
                self._mark_post_commit_producer_unknown(
                    connection,
                    fence_id=str(row["fence_id"]),
                    now=now,
                    kind="replacement_recovered_crash",
                    reason="process_lost_before_restart_commit",
                )
                self._apply_owner_handoff_rows(
                    connection,
                    now=now,
                    reason="replacement_adopted_reversible_crash",
                )
                cursor = connection.execute(
                    """
                    UPDATE service_admission_fences
                    SET state = 'released', released_at = ?,
                        owner_handoff_at = ?,
                        rollback_state = 'complete', detail = ?
                    WHERE fence_id = ?
                      AND state IN ('requested', 'acknowledged', 'violated')
                    """,
                    (
                        now,
                        now,
                        json.dumps(
                            {
                                "reason": "replacement_aborted_reversible_fence",
                                "replacement_pid": replacement_pid,
                                "replacement_generation":
                                    replacement_generation,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        row["fence_id"],
                    ),
                )
            if cursor.rowcount != 1:
                raise ServiceError(
                    "replacement could not adopt the service restart fence"
                )
            connection.commit()
        finally:
            connection.close()
        self._cleanup_fence_markers(str(row["fence_id"]))
        return state

    def recovery_fence(self) -> QuiesceFence | None:
        row = self._active_fence_row()
        return None if row is None else self._row_to_fence(row)

    def handoff_stopped_legacy_worker(
        self,
        *,
        force: bool,
        now: float,
    ) -> ForceRestartOutcome | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if force:
                self._mark_post_commit_producer_unknown(
                    connection,
                    fence_id=f"legacy-worker:{uuid.uuid4()}",
                    now=now,
                    kind="legacy_worker_replacement",
                    reason="legacy_worker_stopped_for_protocol_upgrade",
                )
            self._apply_owner_handoff_rows(
                connection,
                now=now,
                reason="legacy_worker_protocol_handoff",
            )
            connection.commit()
        finally:
            connection.close()
        if not force:
            return None
        return ForceRestartOutcome(
            submissions_unknown=0,
            operations_unknown=0,
            interactions_cancelled=0,
            remote_unknown=0,
            native_schedules_unknown=0,
            native_triggers_unknown=0,
            leases_orphaned=0,
            bounded=True,
            detail="legacy worker stopped before conservative ambiguity recovery",
            intents_recorded=1,
        )

    @staticmethod
    def _apply_owner_handoff_rows(
        connection: sqlite3.Connection,
        *,
        now: float,
        reason: str,
    ) -> None:
        connection.execute(
            """
            UPDATE session_owner_leases
            SET renewed_at = ?, expires_at = ?
            WHERE EXISTS (
              SELECT 1 FROM session_bindings AS b
              WHERE b.sdk_session_id = session_owner_leases.sdk_session_id
                AND b.owner_fence_token = session_owner_leases.fence_token
                AND b.attachment_state IN (
                  'creating', 'resuming', 'attached',
                  'disconnecting', 'recovery_unknown'
                )
            )
            """,
            (now, now),
        )
        connection.execute(
            """
            UPDATE session_bindings
            SET attachment_state = 'recovery_unknown',
                attachment_reason = ?,
                permission_posture = 'unknown',
                permission_verified_at = NULL,
                updated_at = ?,
                row_version = row_version + 1
            WHERE attachment_state IN (
              'creating', 'resuming', 'attached', 'disconnecting'
            )
            """,
            (reason, now),
        )
        connection.execute(
            """
            UPDATE session_operations
            SET state = 'unknown', error_code = ?, settled_at = ?
            WHERE state IN ('pending', 'started')
            """,
            (reason, now),
        )

    def _mark_post_commit_producer_unknown(
        self,
        connection: sqlite3.Connection,
        *,
        fence_id: str,
        now: float,
        kind: str,
        reason: str,
    ) -> None:
        inflight = tuple(sorted(_INFLIGHT_SUBMISSION_STATES))
        placeholders = ",".join("?" for _ in inflight)
        connection.execute(
            f"""
            UPDATE message_queue
            SET state = 'submitted_unknown', updated_at = ?
            WHERE state NOT IN (
              'cancelled', 'submitted', 'submitted_unknown', 'failed'
            )
              AND id IN (
                SELECT submission_id FROM submissions
                WHERE state IN ({placeholders})
              )
            """,
            (now, *inflight),
        )
        connection.execute(
            f"""
            UPDATE submissions
            SET state = 'outcome_unknown'
            WHERE state IN ({placeholders})
            """,
            inflight,
        )
        connection.execute(
            """
            UPDATE session_operations
            SET state = 'unknown',
                error_code = 'post_commit_producer_violation',
                settled_at = ?
            WHERE state IN ('pending', 'started')
            """,
            (now,),
        )
        connection.execute(
            """
            UPDATE pending_interactions
            SET state = 'expired', updated_at = ?,
                response = COALESCE(
                  response,
                  'Cancelled after restart admission violation.'
                )
            WHERE state = 'pending'
            """,
            (now,),
        )
        connection.execute(
            """
            UPDATE session_bindings
            SET runtime_remote_mode = 'unknown',
                updated_at = ?, row_version = row_version + 1
            WHERE runtime_remote_mode = 'on'
               OR pending_remote_transition_id IS NOT NULL
            """,
            (now,),
        )
        connection.execute(
            """
            UPDATE runtime_schedules
            SET state = 'unknown', updated_at = ?
            WHERE state = 'active'
            """,
            (now,),
        )
        terminal = tuple(sorted(_TERMINAL_SCHEDULE_RUN_STATES))
        terminal_placeholders = ",".join("?" for _ in terminal)
        connection.execute(
            f"""
            UPDATE schedule_runs
            SET status = 'outcome_unknown', updated_at = ?
            WHERE status NOT IN ({terminal_placeholders})
              AND (
                claimed_at IS NOT NULL
                OR session_create_started_at IS NOT NULL
                OR send_started_at IS NOT NULL
              )
            """,
            (now, *terminal),
        )
        connection.execute(
            """
            UPDATE liveness_leases
            SET state = 'orphaned', refreshed_at = ?, released_at = ?
            WHERE state = 'active'
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO service_restart_intents(
                intent_id, restart_id, kind, state, outcome,
                detail, created_at, updated_at
            ) VALUES (?, ?, ?, 'recorded', 'unknown', ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                fence_id,
                kind,
                json.dumps(
                    {"reason": reason},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                now,
                now,
            ),
        )

    def acknowledge_quiesce(
        self,
        fence: QuiesceFence,
        *,
        ingress_depth: int = 0,
        violation_count: int = 0,
        now: float | None = None,
    ) -> None:
        timestamp = time.time() if now is None else now
        connection = self._connect()
        try:
            cursor = connection.execute(
                """
                UPDATE service_admission_fences
                SET state = 'acknowledged', acknowledged_at = ?,
                    ingress_depth = ?, violation_count = ?,
                    acknowledged_producer_count = producer_count,
                    acknowledged_journal_id = (
                      SELECT COALESCE(MAX(journal_id), 0)
                      FROM event_journal
                    )
                WHERE fence_id = ? AND state = 'requested'
                  AND expected_pid = ? AND expected_generation = ?
                  AND ABS(expected_process_started_at - ?) <= 5
                  AND protocol_version = ?
                  AND handoff_token_hash = ?
                """,
                (
                    timestamp,
                    ingress_depth,
                    violation_count,
                    fence.fence_id,
                    fence.expected_pid,
                    fence.expected_generation,
                    fence.expected_process_started_at,
                    fence.protocol_version,
                    fence.handoff_token_hash,
                ),
            )
            if cursor.rowcount != 1:
                raise ServiceError("restart admission fence could not be acknowledged")
            connection.commit()
        finally:
            connection.close()

    def wait_for_quiesce(
        self,
        fence: QuiesceFence,
        *,
        timeout_seconds: float,
    ) -> QuiesceFence:
        deadline = time.monotonic() + timeout_seconds
        while True:
            row = self._fence_row(fence.fence_id)
            if row is None:
                raise ServiceError("restart admission fence disappeared")
            state = str(row["state"])
            if state == "acknowledged":
                return QuiesceFence(
                    fence_id=fence.fence_id,
                    expected_pid=fence.expected_pid,
                    expected_generation=fence.expected_generation,
                    expected_process_started_at=fence.expected_process_started_at,
                    protocol_version=fence.protocol_version,
                    handoff_token_hash=fence.handoff_token_hash,
                    requested_at=fence.requested_at,
                    acknowledged_at=float(row["acknowledged_at"]),
                    ingress_depth=int(row["ingress_depth"]),
                    violation_count=int(row["violation_count"]),
                )
            if state in {"violated", "released"}:
                raise RestartBlocked([f"admission_fence_{state}"])
            if time.monotonic() >= deadline:
                raise RestartBlocked(["admission_quiesce_timeout"])
            time.sleep(0.02)

    def snapshot_under_fence(
        self,
        fence: QuiesceFence,
        *,
        now: float,
    ) -> RestartSafetySnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._require_fence_state(
                connection,
                fence,
                allowed_states={"acknowledged"},
            )
            snapshot = self._snapshot_connection(
                connection,
                now=now,
                ingress_depth=int(row["ingress_depth"]),
            )
            self._require_fence_state(
                connection,
                fence,
                allowed_states={"acknowledged"},
            )
            connection.commit()
            return snapshot
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(
                f"could not capture fenced restart snapshot: {error}"
            ) from error
        finally:
            connection.close()

    def assert_quiesced(self, fence: QuiesceFence) -> None:
        connection = self._connect()
        try:
            self._require_fence_state(
                connection,
                fence,
                allowed_states={"acknowledged", "prepared"},
            )
        finally:
            connection.close()

    def commit_quiesce(self, fence: QuiesceFence, *, now: float) -> None:
        self._commit_owner_handoff(
            fence,
            now=now,
            allow_changed_producers=False,
            reason="restart_owner_handoff",
        )

    def commit_irreversible_quiesce(
        self,
        fence: QuiesceFence,
        *,
        now: float,
        reason: str,
    ) -> None:
        self._commit_owner_handoff(
            fence,
            now=now,
            allow_changed_producers=True,
            reason=reason,
        )

    def is_irreversible(self, fence: QuiesceFence) -> bool:
        row = self._fence_row(fence.fence_id)
        return row is not None and str(row["state"]) in {
            "prepared",
            "committed",
        }

    def _commit_owner_handoff(
        self,
        fence: QuiesceFence,
        *,
        now: float,
        allow_changed_producers: bool,
        reason: str,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if allow_changed_producers:
                row = connection.execute(
                    """
                    SELECT * FROM service_admission_fences
                    WHERE fence_id = ? AND state = 'prepared'
                      AND expected_pid = ?
                      AND expected_generation = ?
                      AND (
                        ABS(expected_process_started_at - ?) <= 5
                        OR (
                          protocol_version = 1
                          AND expected_process_started_at IS NULL
                        )
                      )
                      AND protocol_version = ?
                      AND (
                        handoff_token_hash = ?
                        OR (
                          protocol_version = 1
                          AND handoff_token_hash IS NULL
                        )
                      )
                    """,
                    (
                        fence.fence_id,
                        fence.expected_pid,
                        fence.expected_generation,
                        fence.expected_process_started_at,
                        fence.protocol_version,
                        fence.handoff_token_hash,
                    ),
                ).fetchone()
                if row is None:
                    raise RestartBlocked(
                        ["irreversible_admission_fence_missing"]
                    )
            else:
                self._require_fence_state(
                    connection,
                    fence,
                    allowed_states={"acknowledged", "prepared"},
                )
            producer_clause = (
                ""
                if allow_changed_producers
                else """
                  AND producer_count = acknowledged_producer_count
                  AND acknowledged_journal_id = (
                    SELECT COALESCE(MAX(journal_id), 0)
                    FROM event_journal
                  )
                  AND violation_count = 0
                """
            )
            cursor = connection.execute(
                f"""
                UPDATE service_admission_fences
                SET state = 'committed', committed_at = ?,
                    owner_handoff_at = ?, detail = ?
                WHERE fence_id = ?
                  AND state IN ('acknowledged', 'prepared')
                  {producer_clause}
                """,
                (
                    now,
                    now,
                    json.dumps(
                        {"reason": reason},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    fence.fence_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RestartBlocked(["owner_handoff_fence_changed"])
            self._apply_owner_handoff_rows(
                connection,
                now=now,
                reason=reason,
            )
            connection.commit()
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(
                f"could not commit restart owner handoff: {error}"
            ) from error
        finally:
            connection.close()

    def release_quiesce(
        self,
        fence: QuiesceFence,
        *,
        now: float,
        reason: str,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE service_admission_fences
                SET state = 'released', released_at = ?,
                    rollback_state = 'pending', detail = ?
                WHERE fence_id = ?
                  AND state IN ('requested', 'acknowledged', 'violated')
                """,
                (
                    now,
                    json.dumps(
                        {"reason": reason},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    fence.fence_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        self._cleanup_fence_markers(fence.fence_id)

    def snapshot(self, *, now: float) -> RestartSafetySnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            snapshot = self._snapshot_connection(
                connection,
                now=now,
                ingress_depth=0,
            )
            connection.commit()
            return snapshot
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(f"could not capture durable restart snapshot: {error}") from error
        finally:
            connection.close()

    def prepare_force(
        self,
        fence: QuiesceFence,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> ForceRestartOutcome:
        connection = self._connect()
        counts: dict[str, int] = {}
        transition_id = f"service-force-restart:{uuid.uuid4()}"
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._require_fence_state(
                connection,
                fence,
                allowed_states={"acknowledged"},
            )
            intent_targets = self._restart_intent_targets(connection)
            connection.executemany(
                """
                INSERT INTO service_restart_intents(
                    intent_id, restart_id, kind, sdk_session_id, target_id,
                    state, outcome, detail, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'requested', 'unknown', ?, ?, ?)
                """,
                [
                    (
                        str(uuid.uuid4()),
                        transition_id,
                        kind,
                        sdk_session_id,
                        target_id,
                        json.dumps(
                            {"reason": "service_force_restart"},
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        snapshot.captured_at,
                        snapshot.captured_at,
                    )
                    for kind, sdk_session_id, target_id in intent_targets
                ],
            )
            counts["intents"] = len(intent_targets)
            inflight_placeholders = ",".join(
                "?" for _ in _INFLIGHT_SUBMISSION_STATES
            )
            connection.execute(
                f"""
                UPDATE message_queue
                SET state = 'submitted_unknown', updated_at = ?
                WHERE state NOT IN (
                  'cancelled', 'submitted', 'submitted_unknown', 'failed'
                )
                  AND id IN (
                    SELECT submission_id FROM submissions
                    WHERE state IN ({inflight_placeholders})
                  )
                """,
                (
                    snapshot.captured_at,
                    *sorted(_INFLIGHT_SUBMISSION_STATES),
                ),
            )
            counts["submissions"] = self._update(
                connection,
                f"""
                UPDATE submissions
                SET state = 'outcome_unknown'
                WHERE state IN ({inflight_placeholders})
                """,
                tuple(sorted(_INFLIGHT_SUBMISSION_STATES)),
            )
            counts["operations"] = self._update(
                connection,
                """
                UPDATE session_operations
                SET state = 'unknown',
                    error_code = 'service_force_restart',
                    settled_at = ?
                WHERE state IN ('pending', 'started')
                """,
                (snapshot.captured_at,),
            )
            counts["interactions"] = self._update(
                connection,
                """
                UPDATE pending_interactions
                SET state = 'expired',
                    response = COALESCE(response, 'Cancelled by forced service restart.'),
                    updated_at = ?
                WHERE state = 'pending'
                """,
                (snapshot.captured_at,),
            )
            counts["remote"] = self._update(
                connection,
                """
                UPDATE session_bindings
                SET runtime_remote_mode = 'unknown',
                    pending_remote_target = 'off',
                    pending_remote_transition_id = ?,
                    updated_at = ?,
                    row_version = row_version + 1
                WHERE runtime_remote_mode IN ('on', 'unknown')
                   OR pending_remote_transition_id IS NOT NULL
                """,
                (transition_id, snapshot.captured_at),
            )
            counts["schedules"] = self._update(
                connection,
                """
                UPDATE runtime_schedules
                SET state = 'unknown', updated_at = ?
                WHERE state IN ('active', 'unknown')
                """,
                (snapshot.captured_at,),
            )
            terminal_placeholders = ",".join("?" for _ in _TERMINAL_SCHEDULE_RUN_STATES)
            counts["triggers"] = self._update(
                connection,
                f"""
                UPDATE schedule_runs
                SET status = 'outcome_unknown', updated_at = ?
                WHERE status NOT IN ({terminal_placeholders})
                  AND (
                    claimed_at IS NOT NULL
                    OR session_create_started_at IS NOT NULL
                    OR send_started_at IS NOT NULL
                  )
                """,
                (snapshot.captured_at, *sorted(_TERMINAL_SCHEDULE_RUN_STATES)),
            )
            counts["leases"] = self._update(
                connection,
                """
                UPDATE liveness_leases
                SET state = 'orphaned',
                    refreshed_at = ?,
                    released_at = ?
                WHERE state = 'active'
                  AND EXISTS (
                    SELECT 1 FROM session_bindings AS b
                    WHERE b.sdk_session_id = liveness_leases.sdk_session_id
                      AND b.runtime_generation = liveness_leases.runtime_generation
                      AND b.owner_fence_token = liveness_leases.owner_fence_token
                  )
                  AND (
                    kind != 'submission'
                    OR EXISTS (
                      SELECT 1 FROM submissions AS s
                      WHERE s.submission_id = liveness_leases.source_id
                        AND s.state = 'outcome_unknown'
                    )
                  )
                """,
                (snapshot.captured_at, snapshot.captured_at),
            )
            cursor = connection.execute(
                """
                UPDATE service_admission_fences
                SET state = 'prepared', force_prepared_at = ?, detail = ?
                WHERE fence_id = ? AND state = 'acknowledged'
                  AND producer_count = acknowledged_producer_count
                  AND acknowledged_journal_id = (
                    SELECT COALESCE(MAX(journal_id), 0)
                    FROM event_journal
                  )
                  AND violation_count = 0
                """,
                (
                    snapshot.captured_at,
                    json.dumps(
                        {"reason": "force_outcomes_prepared"},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    fence.fence_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RestartBlocked(["admission_fence_changed_during_prepare"])
            connection.commit()
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(f"could not durably prepare forced restart: {error}") from error
        finally:
            connection.close()
        bounded = time.time() <= deadline
        return ForceRestartOutcome(
            submissions_unknown=counts["submissions"],
            operations_unknown=counts["operations"],
            interactions_cancelled=counts["interactions"],
            remote_unknown=counts["remote"],
            native_schedules_unknown=counts["schedules"],
            native_triggers_unknown=counts["triggers"],
            leases_orphaned=counts["leases"],
            bounded=bounded,
            detail=(
                "durable disable/stop/drain intents recorded"
                if bounded
                else "durable outcomes recorded after the drain deadline"
            ),
            intents_recorded=counts["intents"],
        )

    def prepare_replay_restart(
        self,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> bool:
        if time.time() > deadline:
            return False
        connection = self._connect()
        restart_id = f"service-replay-restart:{uuid.uuid4()}"
        try:
            connection.execute("BEGIN IMMEDIATE")
            sessions = connection.execute(
                """
                SELECT sdk_session_id
                FROM session_bindings
                WHERE attachment_state = 'attached'
                ORDER BY sdk_session_id
                """
            ).fetchall()
            connection.executemany(
                """
                INSERT INTO service_restart_intents(
                    intent_id, restart_id, kind, sdk_session_id, target_id,
                    state, outcome, detail, created_at, updated_at
                ) VALUES (?, ?, 'checkpoint_replay', ?, ?, 'requested',
                          'replay_required', ?, ?, ?)
                """,
                [
                    (
                        str(uuid.uuid4()),
                        restart_id,
                        str(row["sdk_session_id"]),
                        str(row["sdk_session_id"]),
                        json.dumps(
                            {
                                "captured_at": snapshot.captured_at,
                                "reason": "watchdog_stale_with_verified_replay",
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        snapshot.captured_at,
                        snapshot.captured_at,
                    )
                    for row in sessions
                ],
            )
            connection.commit()
        except sqlite3.Error as error:
            connection.rollback()
            raise ServiceError(f"could not checkpoint replay restart: {error}") from error
        finally:
            connection.close()
        return time.time() <= deadline

    def _snapshot_connection(
        self,
        connection: sqlite3.Connection,
        *,
        now: float,
        ingress_depth: int,
    ) -> RestartSafetySnapshot:
        lease_rows = connection.execute(
            """
            SELECT l.kind, COUNT(*) AS count
            FROM liveness_leases AS l
            JOIN session_bindings AS b USING (sdk_session_id)
            WHERE l.state = 'active'
              AND l.runtime_generation = b.runtime_generation
              AND l.owner_fence_token = b.owner_fence_token
            GROUP BY l.kind
            """
        ).fetchall()
        lease_counts = {str(row["kind"]): int(row["count"]) for row in lease_rows}
        leases = LeaseMetrics(
            active_submissions=lease_counts.get("submission", 0),
            observed_background_tasks=lease_counts.get("observed_background", 0),
            pending_interactions=lease_counts.get("interaction", 0),
            total=sum(lease_counts.values()),
        )
        local_pending = self._count(
            connection,
            """
            SELECT COUNT(*) FROM message_queue
            WHERE state NOT IN (
              'cancelled', 'submitted', 'submitted_unknown', 'failed'
            )
            """,
        )
        pending_operations = self._count(
            connection,
            """
            SELECT COUNT(*)
            FROM session_operations AS o
            JOIN session_bindings AS b USING (sdk_session_id)
            WHERE o.state IN ('pending', 'started')
              AND o.runtime_generation = b.runtime_generation
              AND o.owner_fence_token = b.owner_fence_token
            """,
        )
        remote_sessions = self._count(
            connection,
            """
            SELECT COUNT(*) FROM session_bindings
            WHERE attachment_state = 'attached'
              AND (
                runtime_remote_mode IN ('on', 'unknown')
                OR pending_remote_transition_id IS NOT NULL
              )
            """,
        )
        native_schedules = self._count(
            connection,
            """
            SELECT COUNT(*) FROM runtime_schedules
            WHERE state IN ('active', 'unknown')
            """,
        )
        terminal_placeholders = ",".join(
            "?" for _ in _TERMINAL_SCHEDULE_RUN_STATES
        )
        native_trigger_windows = self._count(
            connection,
            f"""
            SELECT COUNT(*) FROM schedule_runs
            WHERE status NOT IN ({terminal_placeholders})
              AND (
                claimed_at IS NOT NULL
                OR session_create_started_at IS NOT NULL
                OR send_started_at IS NOT NULL
              )
            """,
            tuple(sorted(_TERMINAL_SCHEDULE_RUN_STATES)),
        )
        blockers = _restart_blockers(
            leases=leases,
            local_pending=local_pending,
            pending_operations=pending_operations,
            remote_sessions=remote_sessions,
            native_schedules=native_schedules,
            native_trigger_windows=native_trigger_windows,
            ingress_depth=ingress_depth,
        )
        return RestartSafetySnapshot(
            captured_at=now,
            active_leases=leases,
            local_pending=local_pending,
            pending_operations=pending_operations,
            remote_sessions=remote_sessions,
            native_schedules=native_schedules,
            native_trigger_windows=native_trigger_windows,
            blockers=tuple(blockers),
            ingress_depth=ingress_depth,
        )

    def _fence_row(self, fence_id: str) -> sqlite3.Row | None:
        connection = self._connect()
        try:
            return connection.execute(
                "SELECT * FROM service_admission_fences WHERE fence_id = ?",
                (fence_id,),
            ).fetchone()
        finally:
            connection.close()

    def _active_fence_row(self) -> sqlite3.Row | None:
        connection = self._connect()
        try:
            return connection.execute(
                """
                SELECT * FROM service_admission_fences
                WHERE state IN (
                  'requested', 'acknowledged', 'violated',
                  'prepared', 'committed'
                )
                LIMIT 1
                """
            ).fetchone()
        finally:
            connection.close()

    @staticmethod
    def _row_to_fence(row: sqlite3.Row) -> QuiesceFence:
        return QuiesceFence(
            fence_id=str(row["fence_id"]),
            expected_pid=int(row["expected_pid"]),
            expected_generation=str(row["expected_generation"]),
            expected_process_started_at=float(
                row["expected_process_started_at"] or 0
            ),
            protocol_version=int(row["protocol_version"]),
            handoff_token_hash=str(row["handoff_token_hash"] or ""),
            requested_at=float(row["requested_at"]),
            acknowledged_at=(
                None
                if row["acknowledged_at"] is None
                else float(row["acknowledged_at"])
            ),
            ingress_depth=(
                None
                if row["ingress_depth"] is None
                else int(row["ingress_depth"])
            ),
            violation_count=int(row["violation_count"]),
        )

    @staticmethod
    def _require_fence_state(
        connection: sqlite3.Connection,
        fence: QuiesceFence,
        *,
        allowed_states: set[str],
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT * FROM service_admission_fences
            WHERE fence_id = ? AND expected_pid = ?
              AND expected_generation = ?
              AND ABS(expected_process_started_at - ?) <= 5
              AND protocol_version = ?
              AND handoff_token_hash = ?
            """,
            (
                fence.fence_id,
                fence.expected_pid,
                fence.expected_generation,
                fence.expected_process_started_at,
                fence.protocol_version,
                fence.handoff_token_hash,
            ),
        ).fetchone()
        if row is None or str(row["state"]) not in allowed_states:
            state = "missing" if row is None else str(row["state"])
            raise RestartBlocked([f"admission_fence_{state}"])
        if int(row["ingress_depth"] or 0) != 0:
            raise RestartBlocked([f"ingress_queue:{row['ingress_depth']}"])
        if (
            int(row["producer_count"])
            != (
                -1
                if row["acknowledged_producer_count"] is None
                else int(row["acknowledged_producer_count"])
            )
            or int(row["violation_count"]) != 0
        ):
            raise RestartBlocked(["admission_fence_producer_changed"])
        if SqliteRestartCoordinator._markers_exist(
            connection,
            fence,
        ):
            raise RestartBlocked(
                ["admission_fence_loss_or_accounting_failure"]
            )
        journal = connection.execute(
            "SELECT COALESCE(MAX(journal_id), 0) FROM event_journal"
        ).fetchone()
        if (
            row["acknowledged_journal_id"] is None
            or journal is None
            or int(row["acknowledged_journal_id"]) != int(journal[0])
        ):
            raise RestartBlocked(["admission_fence_journal_changed"])
        return row

    @staticmethod
    def _markers_exist(
        connection: sqlite3.Connection,
        fence: QuiesceFence,
    ) -> bool:
        database_path_row = connection.execute(
            "PRAGMA database_list"
        ).fetchone()
        if database_path_row is None:
            return True
        database_path = Path(str(database_path_row[2]))
        return any(
            marker.exists() and marker.stat().st_size > 0
            for marker in fence_marker_paths(database_path, fence.fence_id)
        )

    def _has_fence_failure_marker(self, fence_id: str) -> bool:
        return any(
            marker.exists() and marker.stat().st_size > 0
            for marker in fence_marker_paths(self._database_path, fence_id)
        )

    def _cleanup_fence_markers(self, fence_id: str) -> None:
        for marker in fence_marker_paths(self._database_path, fence_id):
            marker.unlink(missing_ok=True)

    def _connect(self) -> sqlite3.Connection:
        if not self._database_path.is_file():
            raise ServiceError(f"durable database is missing: {self._database_path}")
        connection = sqlite3.connect(self._database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _count(
        connection: sqlite3.Connection,
        query: str,
        parameters: Sequence[object] = (),
    ) -> int:
        row = connection.execute(query, tuple(parameters)).fetchone()
        return 0 if row is None else int(row[0])

    @staticmethod
    def _update(
        connection: sqlite3.Connection,
        query: str,
        parameters: Sequence[object] = (),
    ) -> int:
        cursor = connection.execute(query, tuple(parameters))
        return cursor.rowcount

    @staticmethod
    def _restart_intent_targets(
        connection: sqlite3.Connection,
    ) -> list[tuple[str, str | None, str | None]]:
        targets: list[tuple[str, str | None, str | None]] = []
        targets.extend(
            ("disable_remote", str(row["sdk_session_id"]), str(row["thread_id"]))
            for row in connection.execute(
                """
                SELECT sdk_session_id, thread_id
                FROM session_bindings
                WHERE runtime_remote_mode IN ('on', 'unknown')
                   OR pending_remote_transition_id IS NOT NULL
                """
            ).fetchall()
        )
        targets.extend(
            (
                "stop_native_schedule",
                str(row["sdk_session_id"]),
                str(row["runtime_schedule_id"]),
            )
            for row in connection.execute(
                """
                SELECT sdk_session_id, runtime_schedule_id
                FROM runtime_schedules
                WHERE state IN ('active', 'unknown')
                """
            ).fetchall()
        )
        targets.extend(
            (
                "drain_session",
                str(row["sdk_session_id"]),
                str(row["sdk_session_id"]),
            )
            for row in connection.execute(
                """
                SELECT DISTINCT l.sdk_session_id
                FROM liveness_leases AS l
                JOIN session_bindings AS b USING (sdk_session_id)
                WHERE l.state = 'active'
                  AND l.runtime_generation = b.runtime_generation
                  AND l.owner_fence_token = b.owner_fence_token
                """
            ).fetchall()
        )
        terminal_placeholders = ",".join("?" for _ in _TERMINAL_SCHEDULE_RUN_STATES)
        targets.extend(
            (
                "stop_native_trigger",
                None if row["sdk_session_id"] is None else str(row["sdk_session_id"]),
                str(row["run_id"]),
            )
            for row in connection.execute(
                f"""
                SELECT run_id, result_session_id AS sdk_session_id
                FROM schedule_runs
                WHERE status NOT IN ({terminal_placeholders})
                  AND (
                    claimed_at IS NOT NULL
                    OR session_create_started_at IS NOT NULL
                    OR send_started_at IS NOT NULL
                  )
                """,
                tuple(sorted(_TERMINAL_SCHEDULE_RUN_STATES)),
            ).fetchall()
        )
        return targets


class Notifier(Protocol):
    def notify(self, title: str, message: str) -> None: ...


class PlatformNotifier:
    def __init__(self, platform_name: str, runner: CommandRunner) -> None:
        self._platform = platform_name
        self._runner = runner

    def notify(self, title: str, message: str) -> None:
        try:
            if self._platform == "darwin":
                script = (
                    f"display notification {_applescript_string(message)} "
                    f"with title {_applescript_string(title)}"
                )
                self._runner.run(["osascript", "-e", script], check=False)
            elif self._platform == "win32":
                script = _windows_toast_script(title, message)
                self._runner.run(
                    [
                        "powershell.exe",
                        "-NoProfile",
                        "-NonInteractive",
                        "-Command",
                        script,
                    ],
                    check=False,
                )
                return
        except (OSError, ServiceError):
            return


@dataclass(frozen=True, slots=True)
class StormDecision:
    suppress: bool
    count: int
    reason: str


class RestartStormStore:
    """A lock-protected, fail-closed restart history retained across uninstall."""

    def __init__(
        self,
        path: Path,
        *,
        window_seconds: float = RESTART_STORM_WINDOW_SECONDS,
        limit: int = RESTART_STORM_LIMIT,
        max_gap_seconds: float = WATCHDOG_INTERVAL_SECONDS * 1.5,
    ) -> None:
        self._path = path
        self._lock_path = path.with_suffix(path.suffix + ".lock")
        self._window_seconds = window_seconds
        self._limit = limit
        self._max_gap_seconds = max_gap_seconds

    def check_and_record(self, now: float) -> StormDecision:
        descriptor = self._acquire_lock()
        try:
            try:
                payload = self._read()
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
                return StormDecision(True, self._limit, f"corrupt restart state: {error}")
            recent = sorted(
                float(value)
                for value in payload.get("restarts", [])
                if -5 <= now - float(value) <= self._window_seconds
            )
            restarts: list[float] = []
            for value in recent:
                if restarts and value - restarts[-1] > self._max_gap_seconds:
                    restarts = []
                restarts.append(value)
            if restarts and now - restarts[-1] > self._max_gap_seconds:
                restarts = []
            if len(restarts) >= self._limit:
                return StormDecision(True, len(restarts), "restart threshold reached")
            restarts.append(now)
            _atomic_write_text(
                self._path,
                json.dumps(
                    {
                        "schema_version": 1,
                        "restarts": restarts,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
                private=True,
            )
            return StormDecision(False, len(restarts), "restart recorded")
        finally:
            os.close(descriptor)
            self._lock_path.unlink(missing_ok=True)

    def _read(self) -> dict[str, Any]:
        if not self._path.exists():
            return {"schema_version": 1, "restarts": []}
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != 1 or not isinstance(payload.get("restarts"), list):
            raise ValueError("unsupported watchdog state schema")
        return payload

    def _acquire_lock(self) -> int:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        deadline = time.monotonic() + 2
        while True:
            try:
                return os.open(
                    self._lock_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                try:
                    stale = time.time() - self._lock_path.stat().st_mtime > 30
                except FileNotFoundError:
                    continue
                if stale:
                    self._lock_path.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    raise ServiceError("watchdog restart-state lock is busy") from None
                time.sleep(0.02)


class ServiceManager:
    def __init__(
        self,
        settings: Settings,
        *,
        entrypoint: Path | None = None,
        working_directory: Path | None = None,
        platform: str | None = None,
        launch_agents_dir: Path | None = None,
        topology: Topology | None = None,
        runtime_argv: Sequence[str] | None = None,
        command_runner: CommandRunner | None = None,
        restart_coordinator: RestartCoordinator | None = None,
        resume_provider: ResumeTimestampProvider | None = None,
        process_start_provider: Callable[[int], float | None] | None = None,
        notifier: Notifier | None = None,
        uid: int | None = None,
        windows_user_id: str | None = None,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.platform = sys.platform if platform is None else platform
        persisted = _read_json_optional(settings.service_state_path)
        persisted_working_directory = persisted.get("working_directory") if persisted else None
        persisted_runtime_argv = persisted.get("runtime_argv") if persisted else None
        self.entrypoint = (
            (
                Path(entrypoint)
                if entrypoint is not None
                else Path(sys.argv[0])
            )
            .expanduser()
            .resolve()
        )
        self.working_directory = (
            (
                Path(working_directory)
                if working_directory is not None
                else Path(persisted_working_directory)
                if persisted_working_directory
                else settings.resolved_home
            )
            .expanduser()
            .resolve()
        )
        self.launch_agents_dir = (
            settings.resolved_home / "Library" / "LaunchAgents"
            if launch_agents_dir is None
            else launch_agents_dir.expanduser().resolve()
        )
        self.topology = _resolve_topology(settings, topology, persisted)
        selected_runtime_argv = runtime_argv or persisted_runtime_argv
        self._runtime_argv_configured = selected_runtime_argv is not None
        self.runtime_argv = tuple(
            str(value)
            for value in (selected_runtime_argv or (str(self.entrypoint), "runtime", "--headless"))
        )
        self._runner = command_runner or SubprocessCommandRunner()
        self._handoff_token = (
            settings.service_handoff_token.get_secret_value()
            if settings.service_handoff_token is not None
            else uuid.uuid4().hex
        )
        self._coordinator = restart_coordinator or SqliteRestartCoordinator(settings.database_path)
        self._resume_provider = resume_provider or resume_timestamp_provider(self.platform)
        self._process_start_provider = process_start_provider
        self._notifier = notifier or PlatformNotifier(self.platform, self._runner)
        self._uid = os.getuid() if uid is None and hasattr(os, "getuid") else (uid or 0)
        self._windows_user_id = windows_user_id or _current_windows_user()
        self._now = now
        self._sleep = sleep
        self._storm_store = RestartStormStore(settings.watchdog_state_path)

    @property
    def expected_unit_names(self) -> tuple[str, ...]:
        names = ["bot", "watchdog"]
        if self.topology == "sidecar":
            names.insert(0, "runtime")
        return tuple(names)

    def macos_plists(self) -> dict[str, bytes]:
        environment = self._service_environment()
        base = {
            "WorkingDirectory": str(self.working_directory),
            "EnvironmentVariables": environment,
            "ThrottleInterval": 30,
            "LowPriorityBackgroundIO": False,
        }
        definitions: dict[str, dict[str, Any]] = {}
        if self.topology == "sidecar":
            definitions[_MAC_RUNTIME_LABEL] = {
                **base,
                "Label": _MAC_RUNTIME_LABEL,
                "ProgramArguments": [
                    str(self.entrypoint),
                    "service",
                    "runtime",
                ],
                "RunAtLoad": True,
                "KeepAlive": True,
                "StandardOutPath": str(self.settings.log_paths["boot"]),
                "StandardErrorPath": str(self.settings.log_paths["boot"]),
            }
        definitions[_MAC_BOT_LABEL] = {
            **base,
            "Label": _MAC_BOT_LABEL,
            "ProgramArguments": [str(self.entrypoint), "run", "--foreground"],
            "RunAtLoad": True,
            "KeepAlive": True,
            "StandardOutPath": str(self.settings.log_paths["boot"]),
            "StandardErrorPath": str(self.settings.log_paths["boot"]),
        }
        definitions[_MAC_WATCHDOG_LABEL] = {
            **base,
            "Label": _MAC_WATCHDOG_LABEL,
            "ProgramArguments": [str(self.entrypoint), "service", "watchdog"],
            "RunAtLoad": True,
            "StartInterval": WATCHDOG_INTERVAL_SECONDS,
            "StandardOutPath": str(self.settings.log_paths["watchdog"]),
            "StandardErrorPath": str(self.settings.log_paths["watchdog"]),
        }
        return {
            f"{label}.plist": plistlib.dumps(definition, sort_keys=True)
            for label, definition in definitions.items()
        }

    def windows_task_xml(self) -> dict[str, str]:
        runner = self.settings.data_dir / "runtime" / "copilotd-service.ps1"
        tasks: dict[str, str] = {}
        if self.topology == "sidecar":
            tasks[_WINDOWS_RUNTIME_TASK] = _windows_task_xml(
                command="powershell.exe",
                arguments=(
                    f'-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{runner}" runtime'
                ),
                working_directory=str(self.working_directory),
                user_id=self._windows_user_id,
                watchdog=False,
            )
        tasks[_WINDOWS_BOT_TASK] = _windows_task_xml(
            command="powershell.exe",
            arguments=(f'-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{runner}" run'),
            working_directory=str(self.working_directory),
            user_id=self._windows_user_id,
            watchdog=False,
        )
        tasks[_WINDOWS_WATCHDOG_TASK] = _windows_task_xml(
            command="powershell.exe",
            arguments=(
                f'-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{runner}" watchdog'
            ),
            working_directory=str(self.working_directory),
            user_id=self._windows_user_id,
            watchdog=True,
        )
        return tasks

    def windows_runner(self) -> str:
        self._assert_runtime_argv_has_no_secrets()
        environment_lines = [
            f"$env:{key} = '{_powershell_quote(value)}'"
            for key, value in sorted(self._service_environment().items())
        ]
        secret_path = _powershell_quote(str(self.settings.service_secrets_path))
        entrypoint = _powershell_quote(str(self.entrypoint))
        working_directory = _powershell_quote(str(self.working_directory))
        boot_log = _powershell_quote(str(self.settings.log_paths["boot"]))
        watchdog_log = _powershell_quote(str(self.settings.log_paths["watchdog"]))
        runtime_command = " ".join(
            f"'{_powershell_quote(argument)}'" for argument in self.runtime_argv
        )
        return "\n".join(
            [
                "param([Parameter(Mandatory=$true)][ValidateSet('run','runtime','watchdog')]"
                "[string]$Action)",
                "$ErrorActionPreference = 'Stop'",
                "$ProgressPreference = 'SilentlyContinue'",
                *environment_lines,
                f"$secretPath = '{secret_path}'",
                "if (-not (Test-Path -LiteralPath $secretPath -PathType Leaf)) {",
                '  throw "copilotD service secret file is missing: $secretPath"',
                "}",
                "$secrets = Get-Content -LiteralPath $secretPath -Raw -Encoding UTF8 "
                "| ConvertFrom-Json",
                "if ($null -ne $secrets.discord_token) { "
                "$env:COPILOTD_DISCORD_TOKEN = [string]$secrets.discord_token }",
                "if ($null -ne $secrets.runtime_connection_token) { "
                "$env:COPILOTD_RUNTIME_CONNECTION_TOKEN = "
                "[string]$secrets.runtime_connection_token; "
                "$env:COPILOT_CONNECTION_TOKEN = "
                "[string]$secrets.runtime_connection_token }",
                f"Set-Location -LiteralPath '{working_directory}'",
                "if ($Action -eq 'run') {",
                f"  & '{entrypoint}' run --foreground 1>> '{boot_log}' 2>&1",
                "} elseif ($Action -eq 'watchdog') {",
                f"  & '{entrypoint}' service watchdog 1>> '{watchdog_log}' 2>&1",
                "} elseif ($Action -eq 'runtime') {",
                f"  & {runtime_command} 1>> '{boot_log}' 2>&1",
                "} else {",
                '  throw "Unknown copilotD service action: $Action"',
                "}",
                "exit $LASTEXITCODE",
                "",
            ]
        )

    def windows_installer(self) -> str:
        task_paths = self._windows_task_paths()
        task_map = ",\n".join(
            "  " + f"'{_powershell_quote(name)}' = " + f"'{_powershell_quote(str(path))}'"
            for name, path in task_paths.items()
        )
        secret_path = _powershell_quote(str(self.settings.service_secrets_path))
        runner_path = _powershell_quote(
            str(self.settings.data_dir / "runtime" / "copilotd-service.ps1")
        )
        return "\n".join(
            [
                "param([Parameter(Mandatory=$true)]"
                "[ValidateSet('Install','Status','Restart','RestartRuntime',"
                "'StopBot','StartBot','Uninstall')]"
                "[string]$Action)",
                "$ErrorActionPreference = 'Stop'",
                "$identity = [Security.Principal.WindowsIdentity]::GetCurrent()",
                "if ($identity.IsSystem) { "
                "throw 'copilotD must be installed by the signed-in user' }",
                "$taskFiles = [ordered]@{",
                task_map,
                "}",
                "$knownTaskNames = @("
                "'copilotD Runtime','copilotD Bot','copilotD Watchdog')",
                f"$secretPath = '{secret_path}'",
                f"$runnerPath = '{runner_path}'",
                "function Test-CopilotDAction("
                "[string]$Line, [string]$ActionName) {",
                "  $runnerIndex = $Line.IndexOf($runnerPath, "
                "[StringComparison]::OrdinalIgnoreCase)",
                "  if ($runnerIndex -lt 0) { return $false }",
                "  $tail = $Line.Substring($runnerIndex + $runnerPath.Length)",
                "  $pattern = '(?i)(?:^|\\s)' + "
                "[regex]::Escape($ActionName) + '(?:\\s|$)'",
                "  return [regex]::IsMatch($tail, $pattern)",
                "}",
                "function Get-CopilotDHostProcesses([string[]]$TaskNames) {",
                "  $actions = @($TaskNames | ForEach-Object {",
                "    if ($_ -eq 'copilotD Bot') { 'run' }",
                "    elseif ($_ -eq 'copilotD Runtime') { 'runtime' }",
                "    else { 'watchdog' }",
                "  })",
                "  return @(Get-CimInstance Win32_Process | Where-Object {",
                "    $line = [string]$_.CommandLine",
                "    $matches = $false",
                "    foreach ($actionName in $actions) {",
                "      if (Test-CopilotDAction $line $actionName) { "
                "$matches = $true; break }",
                "    }",
                "    $matches",
                "  })",
                "}",
                "function Get-CopilotDProcessTreeIds([object[]]$Roots) {",
                "  $all = @(Get-CimInstance Win32_Process)",
                "  $pending = [Collections.Generic.Queue[int]]::new()",
                "  $ids = [Collections.Generic.HashSet[int]]::new()",
                "  foreach ($root in $Roots) { "
                "$pending.Enqueue([int]$root.ProcessId) }",
                "  while ($pending.Count -gt 0) {",
                "    $id = $pending.Dequeue()",
                "    if (-not $ids.Add($id)) { continue }",
                "    foreach ($child in $all | Where-Object { "
                "$_.ParentProcessId -eq $id }) {",
                "      $pending.Enqueue([int]$child.ProcessId)",
                "    }",
                "  }",
                "  return @($ids)",
                "}",
                "function Stop-CopilotDTasks([string[]]$TaskNames) {",
                "  foreach ($taskName in $TaskNames) {",
                "    Stop-ScheduledTask -TaskName $taskName "
                "-ErrorAction SilentlyContinue",
                "  }",
                "  $hosts = @(Get-CopilotDHostProcesses $TaskNames)",
                "  $tracked = @(Get-CopilotDProcessTreeIds $hosts)",
                "  foreach ($hostProcess in $hosts) {",
                "    & taskkill.exe /PID $hostProcess.ProcessId /T /F | Out-Null",
                "    if ($LASTEXITCODE -ne 0 -and "
                "$null -ne (Get-Process -Id $hostProcess.ProcessId "
                "-ErrorAction SilentlyContinue)) {",
                "      throw \"Could not stop copilotD process tree "
                "$($hostProcess.ProcessId)\"",
                "    }",
                "  }",
                "  $deadline = [DateTime]::UtcNow.AddSeconds(15)",
                "  do {",
                "    $remaining = @($tracked | Where-Object { "
                "$null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue) })",
                "    $remainingHosts = @(Get-CopilotDHostProcesses $TaskNames)",
                "    if ($remaining.Count -eq 0 -and "
                "$remainingHosts.Count -eq 0) { return }",
                "    Start-Sleep -Milliseconds 200",
                "  } while ([DateTime]::UtcNow -lt $deadline)",
                "  throw 'copilotD process tree did not exit before task unregister'",
                "}",
                "function Get-CopilotDStatus {",
                "  $rows = @()",
                "  foreach ($taskName in $taskFiles.Keys) {",
                "    try {",
                "      $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop",
                "      $xml = Export-ScheduledTask -TaskName $taskName -ErrorAction Stop",
                "      $actionName = if ($taskName -eq 'copilotD Bot') { 'run' } "
                "elseif ($taskName -eq 'copilotD Runtime') { 'runtime' } else { $null }",
                "      $pid = $null",
                "      $processStartedAt = $null",
                "      if ($null -ne $actionName) {",
                "        $hostProcess = Get-CimInstance Win32_Process | Where-Object { "
                "$line = [string]$_.CommandLine; "
                "Test-CopilotDAction $line $actionName "
                "} | Select-Object -First 1",
                "        if ($null -ne $hostProcess) {",
                "          $process = Get-CimInstance Win32_Process | Where-Object { "
                "$_.ParentProcessId -eq $hostProcess.ProcessId } | Select-Object -First 1",
                "          if ($null -ne $process) {",
                "            $pid = [int]$process.ProcessId",
                "            $processStartedAt = "
                "$process.CreationDate.ToUniversalTime().ToString('o')",
                "          }",
                "        }",
                "      }",
                "      $rows += [ordered]@{ "
                "name=$taskName; state=[string]$task.State; pid=$pid; "
                "process_started_at=$processStartedAt; xml=$xml; "
                "current_user_sid=$identity.User.Value }",
                "    } catch {",
                "      $rows += [ordered]@{ "
                "name=$taskName; state='Missing'; pid=$null; "
                "process_started_at=$null; xml=$null; "
                "current_user_sid=$identity.User.Value }",
                "    }",
                "  }",
                "  return ,$rows",
                "}",
                "if ($Action -eq 'Install') {",
                "  if (-not (Test-Path -LiteralPath $secretPath -PathType Leaf)) { "
                'throw "Missing service secret file: $secretPath" }',
                "  $sid = $identity.User.Value",
                "  & icacls.exe $secretPath '/inheritance:r' "
                '"/grant:r" "*$($sid):(R,W)" | Out-Null',
                "  if ($LASTEXITCODE -ne 0) { throw 'Could not restrict service secret ACL' }",
                "  Stop-CopilotDTasks $knownTaskNames",
                "  foreach ($taskName in $knownTaskNames) {",
                "    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false "
                "-ErrorAction SilentlyContinue",
                "  }",
                "  foreach ($taskName in $taskFiles.Keys) {",
                "    $xml = Get-Content -LiteralPath $taskFiles[$taskName] -Raw -Encoding UTF8",
                "    Register-ScheduledTask -TaskName $taskName -Xml $xml "
                "-Force -ErrorAction Stop | Out-Null",
                "    Start-ScheduledTask -TaskName $taskName -ErrorAction Stop",
                "  }",
                "  $status = Get-CopilotDStatus",
                "  if (($status | Where-Object { $_.state -eq 'Missing' }).Count -ne 0) { "
                "throw 'One or more copilotD tasks were not registered' }",
                "  $status | ConvertTo-Json -Depth 8 -Compress",
                "} elseif ($Action -eq 'Status') {",
                "  (Get-CopilotDStatus) | ConvertTo-Json -Depth 8 -Compress",
                "} elseif ($Action -eq 'Restart') {",
                "  Stop-CopilotDTasks @('copilotD Bot')",
                "  Start-ScheduledTask -TaskName 'copilotD Bot' -ErrorAction Stop",
                "} elseif ($Action -eq 'RestartRuntime') {",
                "  Stop-CopilotDTasks @('copilotD Runtime')",
                "  Start-ScheduledTask -TaskName 'copilotD Runtime' "
                "-ErrorAction Stop",
                "} elseif ($Action -eq 'StopBot') {",
                "  Stop-CopilotDTasks @('copilotD Bot')",
                "} elseif ($Action -eq 'StartBot') {",
                "  Start-ScheduledTask -TaskName 'copilotD Bot' -ErrorAction Stop",
                "} elseif ($Action -eq 'Uninstall') {",
                "  Stop-CopilotDTasks $knownTaskNames",
                "  foreach ($taskName in $knownTaskNames) {",
                "    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false "
                "-ErrorAction SilentlyContinue",
                "  }",
                "}",
                "",
            ]
        )

    def validate_windows_powershell(self) -> dict[str, tuple[str, ...]]:
        return {
            "runner": _powershell_contract_errors(
                self.windows_runner(),
                required=(
                    "ConvertFrom-Json",
                    "COPILOTD_DISCORD_TOKEN",
                    "Set-Location -LiteralPath",
                    "boot.log",
                    "watchdog.log",
                ),
            ),
            "installer": _powershell_contract_errors(
                self.windows_installer(),
                required=(
                    "Unregister-ScheduledTask",
                    "Register-ScheduledTask",
                    "Export-ScheduledTask",
                    "Start-ScheduledTask",
                    "Stop-ScheduledTask",
                    "Get-CimInstance Win32_Process",
                    "taskkill.exe",
                    "Stop-CopilotDTasks",
                    "icacls.exe",
                ),
            ),
        }

    def validate_windows_task_xml(self) -> dict[str, tuple[str, ...]]:
        return {
            task: _windows_task_contract_errors(xml)
            for task, xml in self.windows_task_xml().items()
        }

    def install(self) -> InstallReceipt:
        if self.platform not in {"darwin", "win32"}:
            raise ServiceError(f"service installation is unsupported on {self.platform}")
        if self.settings.discord_token is None:
            raise ServiceError("COPILOTD_DISCORD_TOKEN is required for service install")
        if self.topology == "sidecar":
            missing = []
            if not self._runtime_argv_configured:
                missing.append("runtime argv")
            if self.settings.runtime_uri is None:
                missing.append("COPILOTD_RUNTIME_URI")
            if self.settings.runtime_connection_token is None:
                missing.append("COPILOTD_RUNTIME_CONNECTION_TOKEN")
            if missing:
                raise ServiceError(
                    "sidecar topology is missing: " + ", ".join(missing)
                )
        self.settings.ensure_directories()
        self._assert_runtime_argv_has_no_secrets()
        self.settings.write_service_secrets(
            service_handoff_token=SecretStr(self._handoff_token)
        )
        previous = _read_heartbeat_optional(self.settings.heartbeat_path)
        installed_at = self._now()
        if self.platform == "darwin":
            hashes = self._install_macos()
        else:
            hashes = self._install_windows()
        self._persist_service_state(installed_at, hashes)
        return InstallReceipt(
            installed_at=installed_at,
            topology=self.topology,
            previous_pid=None if previous is None else previous.pid,
            previous_generation=(None if previous is None else previous.process_generation),
            previous_process_started_at=(
                None if previous is None else previous.process_started_at
            ),
            expected_units=self.expected_unit_names,
            definition_hashes=hashes,
        )

    def verify_post_install(
        self,
        receipt: InstallReceipt,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceStatus:
        timeout = (
            self.settings.setup_verify_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        deadline = time.monotonic() + timeout
        last_status: ServiceStatus | None = None
        while True:
            status = self.status()
            last_status = status
            current_process_started_at = _optional_timestamp(
                status.process_started_at
            )
            previous_process_started_at = _optional_timestamp(
                receipt.previous_process_started_at
            )
            fresh_process = (
                status.pid is not None
                and status.process_generation is not None
                and current_process_started_at is not None
                and (
                    receipt.previous_generation is None
                    or (
                        status.process_generation
                        != receipt.previous_generation
                        and (
                            previous_process_started_at is None
                            or current_process_started_at
                            > previous_process_started_at
                        )
                    )
                )
            )
            written_after_install = False
            if status.heartbeat_written_at is not None:
                written_after_install = (
                    _parse_rfc3339(status.heartbeat_written_at) >= receipt.installed_at - 1
                )
            if status.ready and fresh_process and written_after_install:
                self._record_verified_identity(status)
                return status
            if time.monotonic() >= deadline:
                detail = status_dict(last_status) if last_status is not None else {}
                raise ServiceVerificationError(
                    "service installed but no fresh ready heartbeat matched the effective "
                    f"PID/generation within {timeout:g} seconds: "
                    f"{json.dumps(detail, sort_keys=True, default=str)}"
                )
            self._sleep(0.25)

    def uninstall(self) -> None:
        if self.platform == "darwin":
            domain = self._mac_domain
            for label in self._all_mac_labels:
                self._runner.run(
                    ["launchctl", "bootout", f"{domain}/{label}"],
                    check=False,
                )
                (self.launch_agents_dir / f"{label}.plist").unlink(missing_ok=True)
        elif self.platform == "win32":
            installer = self.settings.data_dir / "runtime" / "install-service.ps1"
            if not installer.exists():
                raise ServiceError(
                    "refusing unsafe Windows uninstall without install-service.ps1"
                )
            self._runner.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(installer),
                    "-Action",
                    "Uninstall",
                ],
                check=True,
            )
        else:
            raise ServiceError(f"service uninstallation is unsupported on {self.platform}")
        self._mark_uninstalled()

    def status(self) -> ServiceStatus:
        heartbeat, heartbeat_error = self._heartbeat_status()
        age = None if heartbeat is None else heartbeat_age_seconds(heartbeat, now=self._now())
        heartbeat_fresh = age is not None and age <= self.settings.heartbeat_stale_seconds
        if self.platform == "darwin":
            units = self._macos_unit_statuses()
        elif self.platform == "win32":
            units = self._windows_unit_statuses()
        else:
            units = ()

        unit_by_name = {unit.name: unit for unit in units}
        bot = unit_by_name.get("bot")
        watchdog = unit_by_name.get("watchdog")
        runtime = unit_by_name.get("runtime")
        leases, queues, exposure = self._durable_metrics()
        if heartbeat is not None:
            queues = QueueMetrics(
                ingress_queue_depth=heartbeat.ingress_queue_depth,
                max_reducer_lag_ms=heartbeat.max_reducer_lag_ms,
                local_pending=queues.local_pending,
                render_pending=queues.render_pending,
                last_callback_at=heartbeat.last_callback_at,
                last_reducer_progress_at=heartbeat.last_reducer_progress_at,
            )

        definition_drift = tuple(
            unit.name for unit in units if unit.installed_definition and not unit.definition_matches
        )
        expected_units = [unit_by_name.get(name) for name in self.expected_unit_names]
        installed = bool(expected_units) and all(
            unit is not None and unit.installed_definition and unit.effective_state != "missing"
            for unit in expected_units
        )
        process_identity_matches: bool | None
        heartbeat_process_started_at = (
            None
            if heartbeat is None
            else _optional_timestamp(heartbeat.process_started_at)
        )
        if (
            heartbeat is None
            or bot is None
            or bot.pid is None
            or bot.process_started_at is None
            or heartbeat_process_started_at is None
        ):
            process_identity_matches = None
        else:
            process_identity_matches = (
                heartbeat.pid == bot.pid
                and _timestamps_match(
                    heartbeat_process_started_at,
                    bot.process_started_at,
                )
            )
        units_effective = all(
            unit is not None
            and (
                unit.effective_state == "running"
                if unit.name in {"bot", "runtime"}
                else unit.effective_state in {"running", "loaded", "stopped"}
            )
            for unit in expected_units
        )
        ready = (
            installed
            and units_effective
            and not definition_drift
            and heartbeat_fresh
            and heartbeat is not None
            and heartbeat.gateway_state == "ready"
            and heartbeat.runtime_state == "ready"
            and process_identity_matches is True
        )
        effective_state = "ready" if ready else "not-installed" if not installed else "degraded"
        durable_protected = bool(
            leases.total
            or exposure.remote_steerable_or_unknown_sessions
            or exposure.active_or_unknown_native_schedules
        )
        return ServiceStatus(
            schema_version=SERVICE_STATUS_SCHEMA_VERSION,
            platform=self.platform,
            topology=self.topology,
            installed=installed,
            effective_state=effective_state,
            ready=ready,
            bot_loaded=bot is not None and bot.loaded,
            watchdog_loaded=watchdog is not None and watchdog.loaded,
            runtime_loaded=(
                self.topology == "bundled-runtime" or (runtime is not None and runtime.loaded)
            ),
            pid=None if heartbeat is None else heartbeat.pid,
            process_generation=(None if heartbeat is None else heartbeat.process_generation),
            process_started_at=(
                None if heartbeat is None else heartbeat.process_started_at
            ),
            manager_process_started_at=(
                None if bot is None else bot.process_started_at
            ),
            process_identity_matches=process_identity_matches,
            service_control_protocol=(
                None
                if heartbeat is None
                else heartbeat.service_control_protocol
            ),
            heartbeat_age_seconds=age,
            heartbeat_written_at=(None if heartbeat is None else heartbeat.written_at),
            heartbeat_fresh=heartbeat_fresh,
            heartbeat_frozen=(False if heartbeat is None else heartbeat.heartbeat_frozen),
            heartbeat_error=heartbeat_error,
            gateway_state=None if heartbeat is None else heartbeat.gateway_state,
            runtime_state=None if heartbeat is None else heartbeat.runtime_state,
            protected_work=(
                (heartbeat.protected_work or durable_protected)
                if heartbeat is not None
                else durable_protected
            ),
            active_leases=leases,
            queue=queues,
            exposure=exposure,
            units=units,
            definition_drift=definition_drift,
            last_resume_at=None if heartbeat is None else heartbeat.last_resume_at,
            wake_suppression_until=(
                None if heartbeat is None else heartbeat.wake_suppression_until
            ),
        )

    def restart(
        self,
        *,
        force: bool = False,
        restart_runtime: bool = False,
    ) -> RestartReceipt:
        requested_at = self._now()
        status = self.status()
        if status.heartbeat_error is not None or status.pid is None:
            raise RestartBlocked([f"heartbeat_unavailable:{status.heartbeat_error or 'missing'}"])
        if status.process_generation is None:
            raise RestartBlocked(["process_generation_missing"])
        expected_process_started_at = _optional_timestamp(
            status.process_started_at
        )
        if expected_process_started_at is None:
            raise RestartBlocked(["process_start_identity_missing"])
        if status.process_identity_matches is not True:
            raise RestartBlocked(["os_pid_heartbeat_mismatch"])
        if (
            status.service_control_protocol is None
            or status.service_control_protocol
            < SERVICE_CONTROL_PROTOCOL_VERSION
        ):
            return self._replace_legacy_worker(
                status,
                force=force,
                restart_runtime=restart_runtime,
            )
        if not status.heartbeat_fresh:
            raise RestartBlocked(["heartbeat_stale"])
        deadline = time.monotonic() + self.settings.restart_drain_timeout_seconds
        fence = self._coordinator.request_quiesce(
            expected_pid=status.pid,
            expected_generation=status.process_generation,
            expected_process_started_at=expected_process_started_at,
            handoff_token=self._handoff_token,
            now=requested_at,
        )
        committed = False
        irreversible = False
        force_attempted = False
        try:
            fence = self._coordinator.wait_for_quiesce(
                fence,
                timeout_seconds=_remaining_seconds(deadline),
            )
            snapshot = self._coordinator.snapshot_under_fence(
                fence,
                now=self._now(),
            )
            if self._now() - snapshot.captured_at > 2:
                raise RestartBlocked(["durable_snapshot_stale"])
            self._revalidate_restart_identity(status, fence)
            self._coordinator.assert_quiesced(fence)
            if snapshot.blockers and not force:
                raise RestartBlocked(snapshot.blockers)
            force_outcome = None
            if force:
                force_attempted = True
                force_outcome = self._prepare_force_bounded(
                    fence,
                    snapshot,
                    timeout_seconds=_remaining_seconds(deadline),
                )
                irreversible = True
            self._coordinator.assert_quiesced(fence)
            self._revalidate_restart_identity(status, fence)
            self._coordinator.commit_quiesce(fence, now=self._now())
            committed = True
            if restart_runtime:
                if self.topology != "sidecar":
                    raise ServiceError(
                        "runtime restart requires sidecar topology"
                    )
                self._restart_runtime()
            self._restart_bot()
            return RestartReceipt(
                requested_at=requested_at,
                force=force,
                previous_pid=status.pid,
                previous_generation=status.process_generation,
                previous_process_started_at=status.process_started_at,
                safety_snapshot=snapshot,
                force_outcome=force_outcome,
                admission_fence_id=fence.fence_id,
            )
        except BaseException as error:
            if force_attempted and isinstance(
                error,
                ForcePreparationUncertain,
            ):
                self._terminate_bot_fail_closed()
                raise
            irreversible = (
                irreversible
                or self._coordinator.is_irreversible(fence)
            )
            if irreversible:
                try:
                    if not committed:
                        self._coordinator.commit_irreversible_quiesce(
                            fence,
                            now=self._now(),
                            reason="irreversible_restart_failure",
                        )
                finally:
                    self._terminate_bot_fail_closed()
            else:
                self._coordinator.release_quiesce(
                    fence,
                    now=self._now(),
                    reason="restart_aborted",
                )
            raise

    def _replace_legacy_worker(
        self,
        status: ServiceStatus,
        *,
        force: bool,
        restart_runtime: bool = False,
    ) -> RestartReceipt:
        assert status.pid is not None
        assert status.process_generation is not None
        assert status.process_started_at is not None
        requested_at = self._now()
        snapshot = self._coordinator.snapshot(now=requested_at)
        if not force and (
            not status.heartbeat_fresh
            or status.protected_work is not False
            or status.queue.ingress_queue_depth != 0
        ):
            raise RestartBlocked(
                ["legacy_worker_heartbeat_not_detach_safe"]
            )
        if snapshot.blockers and not force:
            raise RestartBlocked(snapshot.blockers)
        process_started_at = _parse_rfc3339(status.process_started_at)
        self._stop_bot_for_legacy_upgrade()
        deadline = time.monotonic() + self.settings.restart_drain_timeout_seconds
        while self.process_identity_alive(
            pid=status.pid,
            process_started_at=process_started_at,
        ):
            if time.monotonic() >= deadline:
                raise ServiceError(
                    "legacy service worker did not exit before handoff"
                )
            self._sleep(0.05)
        force_outcome = self._coordinator.handoff_stopped_legacy_worker(
            force=force,
            now=self._now(),
        )
        self.settings.write_service_secrets(
            service_handoff_token=SecretStr(self._handoff_token)
        )
        if restart_runtime:
            if self.topology != "sidecar":
                raise ServiceError(
                    "legacy runtime restart requires sidecar topology"
                )
            self._restart_runtime()
        self._start_bot_after_legacy_upgrade()
        return RestartReceipt(
            requested_at=requested_at,
            force=force,
            previous_pid=status.pid,
            previous_generation=status.process_generation,
            previous_process_started_at=status.process_started_at,
            safety_snapshot=snapshot,
            force_outcome=force_outcome,
            admission_fence_id="legacy-worker-protocol-upgrade",
        )

    def _revalidate_restart_identity(
        self,
        initial: ServiceStatus,
        fence: QuiesceFence,
    ) -> None:
        current = self.status()
        if (
            current.pid != initial.pid
            or current.process_generation != initial.process_generation
            or current.process_identity_matches is not True
            or current.pid != fence.expected_pid
            or current.process_generation != fence.expected_generation
            or not _timestamps_match(
                current.process_started_at,
                fence.expected_process_started_at,
            )
        ):
            raise RestartBlocked(["managed_process_identity_changed"])

    def run_runtime(self) -> None:
        if self.topology != "sidecar":
            raise ServiceError("independent runtime is unavailable in bundled topology")
        if self.settings.runtime_connection_token is None:
            raise ServiceError("COPILOTD_RUNTIME_CONNECTION_TOKEN is required for sidecar topology")
        environment = {
            **os.environ,
            "COPILOT_CONNECTION_TOKEN": (self.settings.runtime_connection_token.get_secret_value()),
        }
        try:
            os.execvpe(
                self.runtime_argv[0],
                list(self.runtime_argv),
                environment,
            )
        except OSError as error:
            raise ServiceError(
                f"could not start sidecar runtime {self.runtime_argv[0]}: {error}"
            ) from error

    def _prepare_force_bounded(
        self,
        fence: QuiesceFence,
        snapshot: RestartSafetySnapshot,
        *,
        timeout_seconds: float,
    ) -> ForceRestartOutcome:
        deadline = time.time() + timeout_seconds
        outcomes: queue.Queue[ForceRestartOutcome | BaseException] = queue.Queue(maxsize=1)

        def prepare() -> None:
            try:
                outcomes.put(
                    self._coordinator.prepare_force(
                        fence,
                        snapshot,
                        deadline=deadline,
                    )
                )
            except BaseException as error:
                outcomes.put(error)

        worker = threading.Thread(
            target=prepare,
            name="copilotd-force-restart-coordinator",
            daemon=True,
        )
        worker.start()
        try:
            result = outcomes.get(timeout=timeout_seconds)
        except queue.Empty:
            raise ForcePreparationUncertain(
                "force preparation timed out; process terminated fail-closed"
            ) from None
        if isinstance(result, BaseException):
            raise result
        if time.time() > deadline or not result.bounded:
            raise ForcePreparationUncertain(
                "force preparation exceeded its deadline; "
                "process terminated fail-closed"
            )
        return result

    def verify_restart(
        self,
        receipt: RestartReceipt,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceStatus:
        install_receipt = InstallReceipt(
            installed_at=receipt.requested_at,
            topology=self.topology,
            previous_pid=receipt.previous_pid,
            previous_generation=receipt.previous_generation,
            previous_process_started_at=receipt.previous_process_started_at,
            expected_units=self.expected_unit_names,
            definition_hashes={},
        )
        return self.verify_post_install(
            install_receipt,
            timeout_seconds=timeout_seconds,
        )

    def watchdog(self, *, now: float | None = None) -> str:
        current = self._now() if now is None else now
        try:
            last_resume = self._resume_provider()
        except (OSError, RuntimeError, ValueError):
            last_resume = None
        if (
            last_resume is not None
            and 0 <= current - last_resume <= self.settings.resume_suppression_seconds
        ):
            return "recent-wake"

        managed_bot = self._managed_bot_unit()
        startup_grace = self._startup_grace_active(current)
        try:
            snapshot = read_heartbeat(self.settings.heartbeat_path)
            age = heartbeat_age_seconds(snapshot, now=current)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            if (
                managed_bot is not None
                and managed_bot.effective_state == "running"
                and startup_grace
            ):
                return "startup-grace"
            self._write_alert(
                "watchdog_heartbeat_invalid",
                "watchdog failed closed because heartbeat is missing or malformed",
                error=str(error),
            )
            return "heartbeat-invalid"
        if (
            managed_bot is None
            or managed_bot.effective_state != "running"
            or managed_bot.pid is None
        ):
            self._write_alert(
                "watchdog_manager_state_invalid",
                "watchdog failed closed because the OS manager has no running bot",
            )
            return "manager-not-running"
        if managed_bot.pid != snapshot.pid:
            if startup_grace:
                return "startup-grace"
            self._write_alert(
                "watchdog_replacement_starting",
                "stale heartbeat belongs to a replaced process",
                heartbeat_pid=snapshot.pid,
                managed_pid=managed_bot.pid,
                process_generation=snapshot.process_generation,
            )
            return "replacement-starting"
        heartbeat_process_started_at = _optional_timestamp(
            snapshot.process_started_at
        )
        if (
            heartbeat_process_started_at is None
            or managed_bot.process_started_at is None
            or not _timestamps_match(
                heartbeat_process_started_at,
                managed_bot.process_started_at,
            )
        ):
            if startup_grace:
                return "startup-grace"
            return "replacement-starting"
        if not self._heartbeat_generation_matches_verified(
            snapshot,
            managed_bot,
            age=age,
        ):
            if startup_grace:
                return "startup-grace"
            self._write_alert(
                "watchdog_generation_mismatch",
                "heartbeat generation does not match the verified managed process",
                heartbeat_pid=snapshot.pid,
                process_generation=snapshot.process_generation,
            )
            return "replacement-starting"
        if age > self.settings.heartbeat_stale_seconds and startup_grace:
            return "startup-grace"

        gateway_down_for = _gateway_down_seconds(snapshot, current)
        gateway_restart = (
            snapshot.gateway_state == "down"
            and gateway_down_for is not None
            and gateway_down_for >= self.settings.gateway_down_restart_seconds
        )
        if self.topology == "sidecar" and snapshot.runtime_state == "down":
            try:
                receipt = self.restart(force=True, restart_runtime=True)
            except (RestartBlocked, ServiceError) as error:
                self._write_alert(
                    "watchdog_runtime_loss_quiesce_failed",
                    "runtime sidecar loss could not safely fence bot ingress",
                    error=str(error),
                )
                return "runtime-loss-quiesce-failed"
            self._write_alert(
                "watchdog_runtime_loss",
                "runtime sidecar loss fenced and restarted the bot",
                force_outcome=(
                    None
                    if receipt.force_outcome is None
                    else asdict(receipt.force_outcome)
                ),
            )
            return "runtime-loss-restarted"
        if age <= self.settings.heartbeat_stale_seconds and not gateway_restart:
            return "healthy"
        try:
            durable = self._coordinator.snapshot(now=current)
        except ServiceError as error:
            self._write_alert(
                "watchdog_durable_state_invalid",
                "watchdog failed closed because durable restart state is unavailable",
                error=str(error),
            )
            return "durable-state-invalid"
        if snapshot.protected_work or durable.blockers:
            if snapshot.durable_replay_capable and self.topology == "sidecar":
                if self._coordinator.prepare_replay_restart(
                    durable,
                    deadline=time.time() + FORCE_RESTART_DRAIN_SECONDS,
                ):
                    return self._watchdog_restart(
                        current,
                        allow_replay_blockers=True,
                    )
            event = (
                "watchdog_gateway_down_protected" if gateway_restart else "watchdog_stale_protected"
            )
            self._write_alert(
                event,
                "protected work prevents an unsafe automatic restart",
                heartbeat_age_seconds=age,
                gateway_down_seconds=gateway_down_for,
                durable_blockers=list(durable.blockers),
            )
            return "protected-gateway-down" if gateway_restart else "protected-no-restart"
        return self._watchdog_restart(current)

    def _watchdog_restart(
        self,
        now: float,
        *,
        allow_replay_blockers: bool = False,
    ) -> str:
        initial = self.status()
        if (
            initial.pid is None
            or initial.process_generation is None
            or initial.process_identity_matches is not True
        ):
            return "replacement-starting"
        if (
            initial.service_control_protocol is None
            or initial.service_control_protocol
            < SERVICE_CONTROL_PROTOCOL_VERSION
        ):
            if (
                not initial.heartbeat_fresh
                or initial.protected_work is not False
                or initial.queue.ingress_queue_depth != 0
            ):
                self._write_alert(
                    "watchdog_legacy_worker_not_detach_safe",
                    "legacy worker requires manual force upgrade",
                    heartbeat_fresh=initial.heartbeat_fresh,
                    protected_work=initial.protected_work,
                    ingress_queue_depth=initial.queue.ingress_queue_depth,
                )
                return "legacy-worker-manual-upgrade-required"
            try:
                self._replace_legacy_worker(initial, force=False)
            except RestartBlocked:
                return "protected-no-restart"
            except ServiceError as error:
                self._write_alert(
                    "watchdog_legacy_worker_upgrade_failed",
                    "legacy worker could not be replaced safely",
                    error=str(error),
                )
                return "legacy-worker-upgrade-failed"
            return "restarted"
        fence: QuiesceFence | None = None
        committed = False
        try:
            expected_process_started_at = _optional_timestamp(
                initial.process_started_at
            )
            if expected_process_started_at is None:
                return "replacement-starting"
            fence = self._coordinator.request_quiesce(
                expected_pid=initial.pid,
                expected_generation=initial.process_generation,
                expected_process_started_at=expected_process_started_at,
                handoff_token=self._handoff_token,
                now=now,
            )
            fence = self._coordinator.wait_for_quiesce(
                fence,
                timeout_seconds=self.settings.restart_drain_timeout_seconds,
            )
            durable = self._coordinator.snapshot_under_fence(
                fence,
                now=self._now(),
            )
            if durable.blockers and not allow_replay_blockers:
                self._write_alert(
                    "watchdog_restart_blocked_after_quiesce",
                    "durable work appeared while the watchdog quiesced ingress",
                    durable_blockers=list(durable.blockers),
                )
                return "protected-no-restart"
            self._revalidate_restart_identity(initial, fence)
            self._coordinator.assert_quiesced(fence)
            decision = self._storm_store.check_and_record(now)
            if decision.suppress:
                self._write_alert(
                    "watchdog_restart_storm",
                    "watchdog restart suppressed after repeated attempts",
                    restart_count=decision.count,
                    reason=decision.reason,
                )
                self._notifier.notify(
                    "copilotD restart storm",
                    "Automatic restarts were suppressed; inspect alerts.log.",
                )
                return "restart-storm"
            self._coordinator.commit_quiesce(fence, now=self._now())
            committed = True
            self._restart_bot()
            return "restarted"
        except (RestartBlocked, ServiceError) as error:
            if committed:
                self._terminate_bot_fail_closed()
            self._write_alert(
                "watchdog_quiesce_failed",
                "watchdog restart failed closed during admission quiesce",
                error=str(error),
            )
            return (
                "restart-failed-closed"
                if committed
                else "quiesce-failed"
            )
        finally:
            if not committed and fence is not None:
                self._coordinator.release_quiesce(
                    fence,
                    now=self._now(),
                    reason="watchdog_restart_aborted",
                )

    def _install_macos(self) -> dict[str, str]:
        self.launch_agents_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        definitions = self.macos_plists()
        hashes: dict[str, str] = {}
        for filename, content in definitions.items():
            path = self.launch_agents_dir / filename
            _atomic_write_bytes(path, content, private=False)
            self._runner.run(["plutil", "-lint", str(path)], check=True)
            hashes[filename] = _sha256_bytes(content)
        watchdog_path = self.launch_agents_dir / f"{_MAC_WATCHDOG_LABEL}.plist"
        interval = self._runner.run(
            [
                "plutil",
                "-extract",
                "StartInterval",
                "raw",
                "-o",
                "-",
                str(watchdog_path),
            ],
            check=True,
        ).stdout.strip()
        if interval != str(WATCHDOG_INTERVAL_SECONDS):
            raise ServiceVerificationError(
                f"watchdog plist interval is {interval}, expected {WATCHDOG_INTERVAL_SECONDS}"
            )
        domain = self._mac_domain
        for label in self._all_mac_labels:
            self._runner.run(
                ["launchctl", "bootout", f"{domain}/{label}"],
                check=False,
            )
            if label not in self._mac_labels:
                (self.launch_agents_dir / f"{label}.plist").unlink(
                    missing_ok=True
                )
        for label in self._mac_labels:
            path = self.launch_agents_dir / f"{label}.plist"
            self._runner.run(
                ["launchctl", "bootstrap", domain, str(path)],
                check=True,
            )
            self._runner.run(
                ["launchctl", "enable", f"{domain}/{label}"],
                check=True,
            )
            self._runner.run(
                ["launchctl", "kickstart", f"{domain}/{label}"],
                check=True,
            )
        self._verify_macos_effective()
        return hashes

    def _install_windows(self) -> dict[str, str]:
        runtime_dir = self.settings.data_dir / "runtime"
        runner_path = runtime_dir / "copilotd-service.ps1"
        installer_path = runtime_dir / "install-service.ps1"
        runner_script = self.windows_runner()
        installer_script = self.windows_installer()
        validation = self.validate_windows_powershell()
        task_validation = self.validate_windows_task_xml()
        failures = [f"{name}: {', '.join(errors)}" for name, errors in validation.items() if errors]
        failures.extend(
            f"{name}: {', '.join(errors)}"
            for name, errors in task_validation.items()
            if errors
        )
        if failures:
            raise ServiceVerificationError(
                "generated PowerShell failed static validation: " + "; ".join(failures)
            )
        _atomic_write_bytes(
            runner_path,
            runner_script.encode("utf-8-sig"),
            private=True,
        )
        _atomic_write_bytes(
            installer_path,
            installer_script.encode("utf-8-sig"),
            private=True,
        )
        hashes: dict[str, str] = {}
        for task, xml in self.windows_task_xml().items():
            path = self._windows_task_paths()[task]
            _atomic_write_text(path, xml, private=True)
            hashes[path.name] = _sha256_text(xml)
        self._runner.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(installer_path),
                "-Action",
                "Install",
            ],
            check=True,
        )
        units = self._windows_unit_statuses()
        bad = [unit.name for unit in units if not unit.loaded or not unit.definition_matches]
        if bad:
            raise ServiceVerificationError(
                "Windows effective task verification failed: " + ", ".join(bad)
            )
        return hashes

    def _verify_macos_effective(self) -> None:
        deadline = time.monotonic() + 10
        while True:
            units = self._macos_unit_statuses()
            bad = [
                unit.name
                for unit in units
                if not unit.loaded
                or not unit.definition_matches
                or (
                    unit.name in {"bot", "runtime"}
                    and (unit.effective_state != "running" or unit.pid is None)
                )
            ]
            if not bad:
                return
            if time.monotonic() >= deadline:
                raise ServiceVerificationError(
                    "macOS effective LaunchAgent verification failed: " + ", ".join(bad)
                )
            self._sleep(0.1)

    def _macos_unit_statuses(self) -> tuple[ServiceUnitStatus, ...]:
        definitions = self.macos_plists()
        statuses: list[ServiceUnitStatus] = []
        label_to_name = {
            _MAC_RUNTIME_LABEL: "runtime",
            _MAC_BOT_LABEL: "bot",
            _MAC_WATCHDOG_LABEL: "watchdog",
        }
        for label in self._mac_labels:
            name = label_to_name[label]
            filename = f"{label}.plist"
            expected = definitions[filename]
            path = self.launch_agents_dir / filename
            disk_matches = path.is_file() and path.read_bytes() == expected
            result = self._runner.run(
                ["launchctl", "print", f"{self._mac_domain}/{label}"],
                check=False,
            )
            if result.returncode != 0:
                state: EffectiveState = "missing"
                pid = None
                process_started_at = None
                process_started_at = None
                effective_matches = False
                effective_hash = None
                detail = result.stderr.strip() or result.stdout.strip() or None
            else:
                state = _parse_launchctl_state(result.stdout)
                pid = _parse_launchctl_pid(result.stdout)
                process_started_at = (
                    None
                    if pid is None
                    else self._process_started_at(pid)
                )
                expected_plist = plistlib.loads(expected)
                effective_matches = _launchctl_definition_matches(
                    result.stdout,
                    expected_plist,
                )
                effective_hash = _sha256_text(_normalize_manager_output(result.stdout))
                detail = None
            statuses.append(
                ServiceUnitStatus(
                    name=name,
                    manager_id=label,
                    definition_path=str(path),
                    installed_definition=path.is_file(),
                    effective_state=state,
                    pid=pid,
                    process_started_at=process_started_at,
                    definition_matches=disk_matches and effective_matches,
                    expected_definition_hash=_sha256_bytes(expected),
                    effective_definition_hash=effective_hash,
                    detail=detail,
                )
            )
        return tuple(statuses)

    def _windows_unit_statuses(self) -> tuple[ServiceUnitStatus, ...]:
        expected_xml = self.windows_task_xml()
        task_paths = self._windows_task_paths()
        installer = self.settings.data_dir / "runtime" / "install-service.ps1"
        effective: dict[str, dict[str, Any]] = {}
        error: str | None = None
        if installer.exists():
            result = self._runner.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(installer),
                    "-Action",
                    "Status",
                ],
                check=False,
            )
            if result.returncode == 0:
                try:
                    payload = json.loads(result.stdout or "[]")
                    rows = payload if isinstance(payload, list) else [payload]
                    effective = {str(row["name"]): row for row in rows}
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as parse_error:
                    error = f"could not parse Task Scheduler status: {parse_error}"
            else:
                error = result.stderr.strip() or result.stdout.strip() or "status failed"
        else:
            error = f"installer script missing: {installer}"

        name_map = {
            _WINDOWS_RUNTIME_TASK: "runtime",
            _WINDOWS_BOT_TASK: "bot",
            _WINDOWS_WATCHDOG_TASK: "watchdog",
        }
        statuses: list[ServiceUnitStatus] = []
        for task in self._windows_task_names:
            name = name_map[task]
            path = task_paths[task]
            expected = expected_xml[task]
            disk_matches = False
            if path.is_file():
                try:
                    disk_matches = _windows_xml_contract(path.read_text(encoding="utf-8")) == (
                        _windows_xml_contract(expected)
                    )
                except (OSError, ET.ParseError, ValueError):
                    disk_matches = False
            row = effective.get(task)
            exported = None if row is None else row.get("xml")
            if row is None or str(row.get("state", "")).lower() == "missing":
                state: EffectiveState = "missing"
                pid = None
                process_started_at = None
                effective_matches = False
                effective_hash = None
            else:
                state = _normalize_windows_task_state(str(row.get("state", "")))
                pid_value = row.get("pid")
                pid = None if pid_value is None else int(pid_value)
                process_started_at = _optional_timestamp(
                    row.get("process_started_at")
                )
                try:
                    effective_matches = isinstance(
                        exported,
                        str,
                    ) and _windows_contract_matches(
                        expected,
                        exported,
                        current_user_sid=(
                            None
                            if row.get("current_user_sid") is None
                            else str(row["current_user_sid"])
                        ),
                    )
                except (ET.ParseError, ValueError):
                    effective_matches = False
                effective_hash = (
                    _sha256_text(_normalize_manager_output(exported))
                    if isinstance(exported, str)
                    else None
                )
            statuses.append(
                ServiceUnitStatus(
                    name=name,
                    manager_id=task,
                    definition_path=str(path),
                    installed_definition=path.is_file(),
                    effective_state=state,
                    pid=pid,
                    process_started_at=process_started_at,
                    definition_matches=disk_matches and effective_matches,
                    expected_definition_hash=_sha256_text(expected),
                    effective_definition_hash=effective_hash,
                    detail=error,
                )
            )
        return tuple(statuses)

    def _durable_metrics(
        self,
    ) -> tuple[LeaseMetrics, QueueMetrics, ExposureMetrics]:
        if not self.settings.database_path.is_file():
            return LeaseMetrics(), QueueMetrics(), ExposureMetrics()
        try:
            connection = sqlite3.connect(self.settings.database_path, timeout=2)
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT l.kind, COUNT(*) AS count
                FROM liveness_leases AS l
                JOIN session_bindings AS b USING (sdk_session_id)
                WHERE l.state = 'active'
                  AND l.runtime_generation = b.runtime_generation
                  AND l.owner_fence_token = b.owner_fence_token
                GROUP BY l.kind
                """
            ).fetchall()
            counts = {str(row["kind"]): int(row["count"]) for row in rows}
            leases = LeaseMetrics(
                active_submissions=counts.get("submission", 0),
                observed_background_tasks=counts.get("observed_background", 0),
                pending_interactions=counts.get("interaction", 0),
                total=sum(counts.values()),
            )
            local_pending = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM message_queue
                    WHERE state NOT IN (
                      'cancelled', 'submitted', 'submitted_unknown', 'failed'
                    )
                    """
                ).fetchone()[0]
            )
            render_pending = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM render_outbox
                    WHERE state IN ('pending', 'retry')
                    """
                ).fetchone()[0]
            )
            remote = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM session_bindings
                    WHERE attachment_state = 'attached'
                      AND runtime_remote_mode IN ('on', 'unknown')
                    """
                ).fetchone()[0]
            )
            schedules = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM runtime_schedules
                    WHERE state IN ('active', 'unknown')
                    """
                ).fetchone()[0]
            )
        except sqlite3.Error:
            return LeaseMetrics(), QueueMetrics(), ExposureMetrics()
        finally:
            if "connection" in locals():
                connection.close()
        return (
            leases,
            QueueMetrics(local_pending=local_pending, render_pending=render_pending),
            ExposureMetrics(
                remote_steerable_or_unknown_sessions=remote,
                active_or_unknown_native_schedules=schedules,
            ),
        )

    def _restart_bot(self) -> None:
        self._update_service_state(
            {
                "last_restart_requested_at": self._now(),
            }
        )
        if self.platform == "darwin":
            self._runner.run(
                [
                    "launchctl",
                    "kickstart",
                    "-k",
                    f"{self._mac_domain}/{_MAC_BOT_LABEL}",
                ],
                check=True,
            )
        elif self.platform == "win32":
            installer = self.settings.data_dir / "runtime" / "install-service.ps1"
            self._runner.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(installer),
                    "-Action",
                    "Restart",
                ],
                check=True,
            )
        else:
            raise ServiceError(f"service restart is unsupported on {self.platform}")

    def _restart_runtime(self) -> None:
        if self.platform == "darwin":
            self._runner.run(
                [
                    "launchctl",
                    "kickstart",
                    "-k",
                    f"{self._mac_domain}/{_MAC_RUNTIME_LABEL}",
                ],
                check=True,
            )
            return
        if self.platform == "win32":
            installer = self.settings.data_dir / "runtime" / "install-service.ps1"
            self._runner.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(installer),
                    "-Action",
                    "RestartRuntime",
                ],
                check=True,
            )
            return
        raise ServiceError(
            f"runtime restart is unsupported on {self.platform}"
        )

    def _terminate_bot_fail_closed(self) -> None:
        if self.platform == "darwin":
            self._runner.run(
                [
                    "launchctl",
                    "kill",
                    "SIGTERM",
                    f"{self._mac_domain}/{_MAC_BOT_LABEL}",
                ],
                check=False,
            )

    def _stop_bot_for_legacy_upgrade(self) -> None:
        if self.platform == "darwin":
            self._runner.run(
                [
                    "launchctl",
                    "bootout",
                    f"{self._mac_domain}/{_MAC_BOT_LABEL}",
                ],
                check=True,
            )
            return
        if self.platform == "win32":
            self._run_windows_installer_action("StopBot", check=True)
            return
        raise ServiceError(
            f"legacy worker stop is unsupported on {self.platform}"
        )

    def _start_bot_after_legacy_upgrade(self) -> None:
        if self.platform == "darwin":
            path = self.launch_agents_dir / f"{_MAC_BOT_LABEL}.plist"
            self._runner.run(
                ["launchctl", "bootstrap", self._mac_domain, str(path)],
                check=True,
            )
            self._runner.run(
                [
                    "launchctl",
                    "enable",
                    f"{self._mac_domain}/{_MAC_BOT_LABEL}",
                ],
                check=True,
            )
            self._runner.run(
                [
                    "launchctl",
                    "kickstart",
                    f"{self._mac_domain}/{_MAC_BOT_LABEL}",
                ],
                check=True,
            )
            return
        if self.platform == "win32":
            self._runner.run(
                [
                    "schtasks.exe",
                    "/Run",
                    "/TN",
                    _WINDOWS_BOT_TASK,
                ],
                check=True,
            )
            return
        raise ServiceError(
            f"legacy worker start is unsupported on {self.platform}"
        )

    def _run_windows_installer_action(
        self,
        action: str,
        *,
        check: bool,
    ) -> None:
        installer = self.settings.data_dir / "runtime" / "install-service.ps1"
        self._runner.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(installer),
                "-Action",
                action,
            ],
            check=check,
        )

    def _service_environment(self) -> dict[str, str]:
        environment = {
            "HOME": str(self.settings.resolved_home),
            "PATH": os.environ.get("PATH", ""),
            "PYTHONUNBUFFERED": "1",
            "COPILOTD_MANAGED_SERVICE": "1",
            "COPILOTD_DATA_DIR": str(self.settings.data_dir),
            "COPILOTD_CACHE_DIR": str(self.settings.cache_dir),
            "COPILOTD_LOG_DIR": str(self.settings.log_dir),
            "COPILOTD_RESOLVED_HOME": str(self.settings.resolved_home),
            "COPILOTD_LOG_LEVEL": self.settings.log_level,
            "COPILOTD_SDK_LOG_LEVEL": self.settings.sdk_log_level,
            "COPILOTD_SERVICE_SECRETS": str(self.settings.service_secrets_path),
            "COPILOTD_MENTION_REQUIRED": str(self.settings.mention_required).lower(),
        }
        if self.settings.discord_guild_id is not None:
            environment["COPILOTD_DISCORD_GUILD_ID"] = str(self.settings.discord_guild_id)
        if self.settings.runtime_uri is not None:
            environment["COPILOTD_RUNTIME_URI"] = self.settings.runtime_uri
        return environment

    def _process_started_at(self, pid: int) -> float | None:
        if self._process_start_provider is not None:
            return self._process_start_provider(pid)
        if self.platform == "win32":
            script = (
                "$process = Get-CimInstance Win32_Process "
                f"-Filter \"ProcessId = {pid}\" "
                "-ErrorAction Stop; "
                "if ($null -ne $process) { "
                "$process.CreationDate.ToUniversalTime().ToString('o') }"
            )
            result = self._runner.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    script,
                ],
                check=False,
            )
            if result.returncode != 0:
                raise ServiceError(
                    "could not query Windows process start identity"
                )
            return _optional_timestamp(result.stdout.strip())
        if self.platform != "darwin":
            raise ServiceError(
                f"process start identity is unsupported on {self.platform}"
            )
        result = self._runner.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        try:
            parsed = datetime.strptime(
                " ".join(result.stdout.split()),
                "%a %b %d %H:%M:%S %Y",
            )
        except ValueError:
            return None
        return parsed.astimezone().timestamp()

    def _assert_runtime_argv_has_no_secrets(self) -> None:
        serialized = "\0".join(self.runtime_argv)
        secrets = (
            self.settings.discord_token,
            self.settings.runtime_connection_token,
        )
        if any(
            secret is not None
            and secret.get_secret_value()
            and secret.get_secret_value() in serialized
            for secret in secrets
        ):
            raise ServiceError("runtime argv must not contain Discord or runtime connection tokens")

    def _heartbeat_status(self) -> tuple[HeartbeatSnapshot | None, str | None]:
        try:
            return read_heartbeat(self.settings.heartbeat_path), None
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            return None, f"{type(error).__name__}: {error}"

    def _persist_service_state(
        self,
        installed_at: float,
        definition_hashes: dict[str, str],
    ) -> None:
        _atomic_write_text(
            self.settings.service_state_path,
            json.dumps(
                {
                    "schema_version": SERVICE_STATE_SCHEMA_VERSION,
                    "installed": True,
                    "installed_at": installed_at,
                    "platform": self.platform,
                    "topology": self.topology,
                    "entrypoint": str(self.entrypoint),
                    "working_directory": str(self.working_directory),
                    "runtime_argv": list(self.runtime_argv),
                    "definition_hashes": definition_hashes,
                    "settings": {
                        "discord_guild_id": self.settings.discord_guild_id,
                        "mention_required": self.settings.mention_required,
                        "log_level": self.settings.log_level,
                        "sdk_log_level": self.settings.sdk_log_level,
                        "runtime_uri": self.settings.runtime_uri,
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            private=True,
        )

    def _record_verified_identity(self, status: ServiceStatus) -> None:
        if (
            status.pid is None
            or status.process_generation is None
            or status.process_started_at is None
        ):
            raise ServiceVerificationError("ready service has no process identity")
        self._update_service_state(
            {
                "last_verified_pid": status.pid,
                "last_verified_generation": status.process_generation,
                "last_verified_process_started_at": status.process_started_at,
                "last_verified_at": self._now(),
            }
        )

    def _update_service_state(self, updates: dict[str, object]) -> None:
        state = _read_json_optional(self.settings.service_state_path) or {
            "schema_version": SERVICE_STATE_SCHEMA_VERSION,
        }
        state.update(updates)
        _atomic_write_text(
            self.settings.service_state_path,
            json.dumps(
                state,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            private=True,
        )

    def _managed_bot_unit(self) -> ServiceUnitStatus | None:
        units = (
            self._macos_unit_statuses()
            if self.platform == "darwin"
            else self._windows_unit_statuses()
            if self.platform == "win32"
            else ()
        )
        return next((unit for unit in units if unit.name == "bot"), None)

    def replacement_is_managed(
        self,
        *,
        pid: int,
        process_started_at: float,
    ) -> bool:
        unit = self._managed_bot_unit()
        return (
            unit is not None
            and unit.effective_state == "running"
            and unit.pid == pid
            and _timestamps_match(
                unit.process_started_at,
                process_started_at,
            )
        )

    def process_identity_alive(
        self,
        *,
        pid: int,
        process_started_at: float,
    ) -> bool:
        current = self._process_started_at(pid)
        return _timestamps_match(current, process_started_at)

    def _startup_grace_active(self, now: float) -> bool:
        state = _read_json_optional(self.settings.service_state_path) or {}
        candidates = [
            float(value)
            for key in ("installed_at", "last_restart_requested_at")
            if (value := state.get(key)) is not None
        ]
        return bool(candidates) and (
            0 <= now - max(candidates) <= self.settings.service_startup_grace_seconds
        )

    def _heartbeat_generation_matches_verified(
        self,
        snapshot: HeartbeatSnapshot,
        managed_bot: ServiceUnitStatus,
        *,
        age: float,
    ) -> bool:
        state = _read_json_optional(self.settings.service_state_path) or {}
        verified_pid = state.get("last_verified_pid")
        verified_generation = state.get("last_verified_generation")
        verified_started_at = _optional_timestamp(
            state.get("last_verified_process_started_at")
        )
        heartbeat_started_at = _optional_timestamp(snapshot.process_started_at)
        exact = (
            verified_pid is not None
            and verified_generation is not None
            and verified_started_at is not None
            and int(verified_pid) == snapshot.pid
            and str(verified_generation) == snapshot.process_generation
            and _timestamps_match(
                verified_started_at,
                heartbeat_started_at,
            )
        )
        if exact:
            return True
        replacement_is_ready = (
            age <= self.settings.heartbeat_stale_seconds
            and snapshot.gateway_state == "ready"
            and snapshot.runtime_state == "ready"
            and heartbeat_started_at is not None
            and managed_bot.process_started_at is not None
            and _timestamps_match(
                heartbeat_started_at,
                managed_bot.process_started_at,
            )
            and (
                verified_started_at is None
                or heartbeat_started_at > verified_started_at
            )
        )
        if not replacement_is_ready:
            return False
        self._update_service_state(
            {
                "last_verified_pid": snapshot.pid,
                "last_verified_generation": snapshot.process_generation,
                "last_verified_process_started_at": snapshot.process_started_at,
                "last_verified_at": self._now(),
                "replacement_adopted_at": self._now(),
            }
        )
        return True

    def _mark_uninstalled(self) -> None:
        state = _read_json_optional(self.settings.service_state_path) or {}
        state.update(
            {
                "schema_version": SERVICE_STATE_SCHEMA_VERSION,
                "installed": False,
                "uninstalled_at": self._now(),
                "topology": self.topology,
                "entrypoint": str(self.entrypoint),
                "working_directory": str(self.working_directory),
                "runtime_argv": list(self.runtime_argv),
            }
        )
        _atomic_write_text(
            self.settings.service_state_path,
            json.dumps(
                state,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            private=True,
        )

    def _write_alert(self, event: str, message: str, **detail: object) -> None:
        self.settings.log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        payload = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "level": "warning",
            "event": event,
            "message": message,
            "platform": self.platform,
            **detail,
        }
        line = (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        path = self.settings.log_paths["alerts"]
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, line.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if os.name == "posix":
            path.chmod(0o600)

    @property
    def _mac_domain(self) -> str:
        return f"gui/{self._uid}"

    @property
    def _mac_labels(self) -> tuple[str, ...]:
        labels = [_MAC_BOT_LABEL, _MAC_WATCHDOG_LABEL]
        if self.topology == "sidecar":
            labels.insert(0, _MAC_RUNTIME_LABEL)
        return tuple(labels)

    @property
    def _all_mac_labels(self) -> tuple[str, ...]:
        return (
            _MAC_RUNTIME_LABEL,
            _MAC_BOT_LABEL,
            _MAC_WATCHDOG_LABEL,
        )

    @property
    def _windows_task_names(self) -> tuple[str, ...]:
        names = [_WINDOWS_BOT_TASK, _WINDOWS_WATCHDOG_TASK]
        if self.topology == "sidecar":
            names.insert(0, _WINDOWS_RUNTIME_TASK)
        return tuple(names)

    def _windows_task_paths(self) -> dict[str, Path]:
        directory = self.settings.data_dir / "runtime" / "tasks"
        return {
            task: directory / f"{task.replace(' ', '-')}.xml" for task in self._windows_task_names
        }


def _windows_task_xml(
    *,
    command: str,
    arguments: str,
    working_directory: str,
    user_id: str,
    watchdog: bool,
) -> str:
    ET.register_namespace("", _TASK_NAMESPACE)
    task = ET.Element(f"{{{_TASK_NAMESPACE}}}Task", {"version": "1.4"})
    registration = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}RegistrationInfo")
    ET.SubElement(registration, f"{{{_TASK_NAMESPACE}}}Author").text = "copilotD"
    triggers = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}Triggers")
    logon = ET.SubElement(triggers, f"{{{_TASK_NAMESPACE}}}LogonTrigger")
    ET.SubElement(logon, f"{{{_TASK_NAMESPACE}}}Enabled").text = "true"
    if watchdog:
        repetition = ET.SubElement(logon, f"{{{_TASK_NAMESPACE}}}Repetition")
        ET.SubElement(repetition, f"{{{_TASK_NAMESPACE}}}Interval").text = "PT5M"
        ET.SubElement(repetition, f"{{{_TASK_NAMESPACE}}}StopAtDurationEnd").text = "false"
    ET.SubElement(logon, f"{{{_TASK_NAMESPACE}}}UserId").text = user_id
    if watchdog:
        registration_trigger = ET.SubElement(
            triggers,
            f"{{{_TASK_NAMESPACE}}}RegistrationTrigger",
        )
        ET.SubElement(
            registration_trigger,
            f"{{{_TASK_NAMESPACE}}}Enabled",
        ).text = "true"
        registration_repetition = ET.SubElement(
            registration_trigger,
            f"{{{_TASK_NAMESPACE}}}Repetition",
        )
        ET.SubElement(
            registration_repetition,
            f"{{{_TASK_NAMESPACE}}}Interval",
        ).text = "PT5M"
        ET.SubElement(
            registration_repetition,
            f"{{{_TASK_NAMESPACE}}}StopAtDurationEnd",
        ).text = "false"

    principals = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}Principals")
    principal = ET.SubElement(
        principals,
        f"{{{_TASK_NAMESPACE}}}Principal",
        {"id": "Author"},
    )
    ET.SubElement(principal, f"{{{_TASK_NAMESPACE}}}UserId").text = user_id
    ET.SubElement(principal, f"{{{_TASK_NAMESPACE}}}LogonType").text = "InteractiveToken"
    ET.SubElement(principal, f"{{{_TASK_NAMESPACE}}}RunLevel").text = "LeastPrivilege"

    settings = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}Settings")
    values = {
        "MultipleInstancesPolicy": "IgnoreNew",
        "DisallowStartIfOnBatteries": "false",
        "StopIfGoingOnBatteries": "false",
        "AllowHardTerminate": "true",
        "StartWhenAvailable": "true" if watchdog else "false",
        "RunOnlyIfNetworkAvailable": "false",
        "WakeToRun": "false",
        "ExecutionTimeLimit": "PT0S",
        "Enabled": "true",
    }
    for name, value in values.items():
        ET.SubElement(settings, f"{{{_TASK_NAMESPACE}}}{name}").text = value
    restart = ET.SubElement(settings, f"{{{_TASK_NAMESPACE}}}RestartOnFailure")
    ET.SubElement(restart, f"{{{_TASK_NAMESPACE}}}Interval").text = "PT1M"
    ET.SubElement(restart, f"{{{_TASK_NAMESPACE}}}Count").text = "255"

    actions = ET.SubElement(
        task,
        f"{{{_TASK_NAMESPACE}}}Actions",
        {"Context": "Author"},
    )
    execute = ET.SubElement(actions, f"{{{_TASK_NAMESPACE}}}Exec")
    ET.SubElement(execute, f"{{{_TASK_NAMESPACE}}}Command").text = command
    ET.SubElement(execute, f"{{{_TASK_NAMESPACE}}}Arguments").text = arguments
    ET.SubElement(
        execute,
        f"{{{_TASK_NAMESPACE}}}WorkingDirectory",
    ).text = working_directory
    return ET.tostring(task, encoding="unicode", xml_declaration=True)


def _windows_task_contract_errors(xml: str) -> tuple[str, ...]:
    errors: list[str] = []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as error:
        return (f"invalid XML: {error}",)
    if root.tag != f"{{{_TASK_NAMESPACE}}}Task":
        errors.append("Task Scheduler namespace is invalid")
    namespace = {"t": _TASK_NAMESPACE}
    logon = root.find(".//t:LogonTrigger", namespace)
    if logon is None:
        errors.append("LogonTrigger is missing")
    else:
        errors.extend(
            _ordered_child_errors(
                logon,
                (
                    "Enabled",
                    "Repetition",
                    "StartBoundary",
                    "EndBoundary",
                    "UserId",
                    "Delay",
                ),
                context="LogonTrigger",
            )
        )
    registration = root.find(".//t:RegistrationTrigger", namespace)
    if registration is not None:
        errors.extend(
            _ordered_child_errors(
                registration,
                (
                    "Enabled",
                    "Repetition",
                    "StartBoundary",
                    "EndBoundary",
                    "Delay",
                ),
                context="RegistrationTrigger",
            )
        )
    interval = root.findtext(
        ".//t:Settings/t:RestartOnFailure/t:Interval",
        namespaces=namespace,
    )
    interval_seconds = _task_duration_seconds(interval)
    if interval_seconds is None or interval_seconds < 60:
        errors.append("RestartOnFailure Interval must be at least PT1M")
    count = root.findtext(
        ".//t:Settings/t:RestartOnFailure/t:Count",
        namespaces=namespace,
    )
    try:
        count_value = int(count or "")
    except ValueError:
        errors.append("RestartOnFailure Count must be an unsigned byte")
    else:
        if not 1 <= count_value <= 255:
            errors.append("RestartOnFailure Count must be between 1 and 255")
    return tuple(errors)


def _ordered_child_errors(
    element: ET.Element,
    expected_order: tuple[str, ...],
    *,
    context: str,
) -> list[str]:
    ranks = {name: index for index, name in enumerate(expected_order)}
    children = [child.tag.rsplit("}", 1)[-1] for child in element]
    known = [name for name in children if name in ranks]
    if known != sorted(known, key=ranks.__getitem__):
        return [f"{context} child order is schema-invalid: {children}"]
    return []


def _task_duration_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.fullmatch(
        r"PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?",
        value,
    )
    if match is None:
        return None
    return (
        int(match.group("hours") or 0) * 3600
        + int(match.group("minutes") or 0) * 60
        + int(match.group("seconds") or 0)
    )


def _windows_xml_contract(xml: str) -> dict[str, str | None]:
    root = ET.fromstring(xml)
    namespace = {"t": _TASK_NAMESPACE}

    def text(path: str) -> str | None:
        return root.findtext(path, namespaces=namespace)

    return {
        "task_version": root.attrib.get("version"),
        "logon_enabled": text(".//t:LogonTrigger/t:Enabled"),
        "logon_user": text(".//t:LogonTrigger/t:UserId"),
        "repetition_interval": text(".//t:LogonTrigger/t:Repetition/t:Interval"),
        "repetition_stop": text(        ".//t:LogonTrigger/t:Repetition/t:StopAtDurationEnd"
        ),
        "registration_interval": text(
        ".//t:RegistrationTrigger/t:Repetition/t:Interval"),
        "multiple_instances": text(".//t:Settings/t:MultipleInstancesPolicy"),
        "disallow_battery": text(".//t:Settings/t:DisallowStartIfOnBatteries"),
        "stop_on_battery": text(".//t:Settings/t:StopIfGoingOnBatteries"),
        "allow_hard_terminate": text(".//t:Settings/t:AllowHardTerminate"),
        "start_when_available": text(".//t:Settings/t:StartWhenAvailable"),
        "network_required": text(".//t:Settings/t:RunOnlyIfNetworkAvailable"),
        "wake_to_run": text(".//t:Settings/t:WakeToRun"),
        "execution_limit": text(".//t:Settings/t:ExecutionTimeLimit"),
        "enabled": text(".//t:Settings/t:Enabled"),
        "restart_interval": text(".//t:Settings/t:RestartOnFailure/t:Interval"),
        "restart_count": text(".//t:Settings/t:RestartOnFailure/t:Count"),
        "principal_user": text(".//t:Principals/t:Principal/t:UserId"),
        "principal_logon": text(".//t:Principals/t:Principal/t:LogonType"),
        "principal_run_level": text(".//t:Principals/t:Principal/t:RunLevel"),
        "command": text(".//t:Actions/t:Exec/t:Command"),
        "arguments": text(".//t:Actions/t:Exec/t:Arguments"),
        "working_directory": text(".//t:Actions/t:Exec/t:WorkingDirectory"),
    }


def _windows_contract_matches(
    expected_xml: str,
    effective_xml: str,
    *,
    current_user_sid: str | None,
) -> bool:
    expected = _windows_xml_contract(expected_xml)
    effective = _windows_xml_contract(effective_xml)
    user_fields = {"logon_user", "principal_user"}
    if any(expected[key] != effective[key] for key in expected.keys() - user_fields):
        return False
    effective_logon = effective["logon_user"]
    effective_principal = effective["principal_user"]
    if effective_logon != effective_principal or effective_logon is None:
        return False
    return effective_logon in {
        expected["logon_user"],
        expected["principal_user"],
        current_user_sid,
    }


def _restart_blockers(
    *,
    leases: LeaseMetrics,
    local_pending: int,
    pending_operations: int,
    remote_sessions: int,
    native_schedules: int,
    native_trigger_windows: int,
    ingress_depth: int,
) -> list[str]:
    blockers: list[str] = []
    if ingress_depth:
        blockers.append(f"ingress_queue:{ingress_depth}")
    if leases.total:
        blockers.append(f"active_liveness:{leases.total}")
    if local_pending:
        blockers.append(f"local_queue:{local_pending}")
    if pending_operations:
        blockers.append(f"pending_operations:{pending_operations}")
    if remote_sessions:
        blockers.append(f"remote_exposure:{remote_sessions}")
    if native_schedules:
        blockers.append(f"runtime_schedules:{native_schedules}")
    if native_trigger_windows:
        blockers.append(f"native_trigger_windows:{native_trigger_windows}")
    return blockers


def _remaining_seconds(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RestartBlocked(["restart_deadline_exceeded"])
    return remaining


def _resolve_topology(
    settings: Settings,
    explicit: Topology | None,
    persisted: dict[str, Any] | None,
) -> Topology:
    if explicit is not None:
        return explicit
    if persisted and persisted.get("topology") in {"bundled-runtime", "sidecar"}:
        return persisted["topology"]
    capabilities = _read_json_optional(settings.capability_path)
    if capabilities and capabilities.get("topology") == "sidecar":
        return "sidecar"
    return "bundled-runtime"


def _read_heartbeat_optional(path: Path) -> HeartbeatSnapshot | None:
    try:
        return read_heartbeat(path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_launchctl_state(output: str) -> EffectiveState:
    match = re.search(r"^\s*state\s*=\s*(\S+)", output, flags=re.MULTILINE)
    if match is None:
        return "loaded"
    state = match.group(1).lower()
    if state == "running":
        return "running"
    if state in {"waiting", "exited", "spawn scheduled"}:
        return "stopped"
    return "loaded"


def _parse_launchctl_pid(output: str) -> int | None:
    match = re.search(r"^\s*pid\s*=\s*(\d+)", output, flags=re.MULTILINE)
    return None if match is None else int(match.group(1))


def _launchctl_definition_matches(
    output: str,
    expected: dict[str, Any],
) -> bool:
    arguments = [str(value) for value in expected["ProgramArguments"]]
    if not all(argument in output for argument in arguments):
        return False
    if "ProcessType" in expected or re.search(
        r"process type\s*=\s*(?:Background|Interactive)",
        output,
        flags=re.IGNORECASE,
    ):
        return False
    interval = expected.get("StartInterval")
    if (
        interval is not None
        and re.search(
            rf"run interval\s*=\s*{int(interval)}(?:\s+seconds)?",
            output,
        )
        is None
    ):
        return False
    return True


def _normalize_windows_task_state(value: str) -> EffectiveState:
    state = value.strip().lower()
    if state == "running":
        return "running"
    if state in {"ready", "queued"}:
        return "loaded"
    if state in {"disabled"}:
        return "stopped"
    if state in {"missing", ""}:
        return "missing"
    return "unknown"


def _gateway_down_seconds(
    snapshot: HeartbeatSnapshot,
    now: float,
) -> float | None:
    if snapshot.gateway_down_since is None:
        return None
    return max(0.0, now - _parse_rfc3339(snapshot.gateway_down_since))


def _parse_rfc3339(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _optional_timestamp(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return _parse_rfc3339(str(value))
    except ValueError:
        return None


def _timestamps_match(
    left: object,
    right: object,
    *,
    tolerance_seconds: float = 5,
) -> bool:
    left_timestamp = _optional_timestamp(left)
    right_timestamp = _optional_timestamp(right)
    return (
        left_timestamp is not None
        and right_timestamp is not None
        and abs(left_timestamp - right_timestamp) <= tolerance_seconds
    )


def _normalize_manager_output(output: str) -> str:
    return "\n".join(line.rstrip() for line in output.replace("\r\n", "\n").splitlines()).strip()


def _atomic_write_text(path: Path, content: str, *, private: bool) -> None:
    _atomic_write_bytes(path, content.encode("utf-8"), private=private)


def _atomic_write_bytes(path: Path, content: bytes, *, private: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700 if private else 0o755)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600 if private else 0o644,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name == "posix":
            path.chmod(0o600 if private else 0o644)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_text(content: str) -> str:
    return _sha256_bytes(content.encode("utf-8"))


def _token_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _powershell_quote(value: str) -> str:
    return value.replace("'", "''")


def _powershell_contract_errors(
    script: str,
    *,
    required: Sequence[str],
) -> tuple[str, ...]:
    errors: list[str] = []
    if "\x00" in script:
        errors.append("contains NUL")
    for value in required:
        if value not in script:
            errors.append(f"missing {value}")
    if not _powershell_delimiters_balanced(script):
        errors.append("unbalanced delimiters")
    try:
        script.encode("utf-8")
    except UnicodeEncodeError:
        errors.append("is not UTF-8 encodable")
    return tuple(errors)


def _powershell_delimiters_balanced(script: str) -> bool:
    pairs = {"(": ")", "{": "}", "[": "]"}
    closing = set(pairs.values())
    stack: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(script):
        character = script[index]
        if quote is not None:
            if character == quote:
                if quote == "'" and index + 1 < len(script) and script[index + 1] == "'":
                    index += 2
                    continue
                quote = None
            elif character == "`":
                index += 2
                continue
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
        elif character in pairs:
            stack.append(pairs[character])
        elif character in closing:
            if not stack or stack.pop() != character:
                return False
        index += 1
    return quote is None and not stack


def _applescript_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _windows_toast_script(title: str, message: str) -> str:
    safe_title = _powershell_quote(title)
    safe_message = _powershell_quote(message)
    return (
        "[Windows.UI.Notifications.ToastNotificationManager, "
        "Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; "
        "$xml = [Windows.UI.Notifications.ToastNotificationManager]::"
        "GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02); "
        f"$xml.GetElementsByTagName('text')[0].AppendChild($xml.CreateTextNode('{safe_title}')) "
        "> $null; "
        f"$xml.GetElementsByTagName('text')[1].AppendChild($xml.CreateTextNode('{safe_message}')) "
        "> $null; "
        "$toast = [Windows.UI.Notifications.ToastNotification]::new($xml); "
        "[Windows.UI.Notifications.ToastNotificationManager]::"
        "CreateToastNotifier('copilotD').Show($toast)"
    )


def _current_windows_user() -> str:
    domain = os.environ.get("USERDOMAIN")
    user = os.environ.get("USERNAME") or getpass.getuser()
    return f"{domain}\\{user}" if domain else user


def status_dict(status: ServiceStatus) -> dict[str, Any]:
    return asdict(status)
