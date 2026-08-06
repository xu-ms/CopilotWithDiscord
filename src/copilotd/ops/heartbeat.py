from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from copilotd.ops.contracts import (
    GATEWAY_DOWN_RESTART_SECONDS,
    HEARTBEAT_SCHEMA_VERSION,
    HEARTBEAT_WRITE_INTERVAL_SECONDS,
    RESUME_SUPPRESSION_SECONDS,
    SERVICE_CONTROL_PROTOCOL_VERSION,
)
from copilotd.ops.wake import ResumeTimestampProvider, resume_timestamp_provider
from copilotd.storage.database import Database

GatewayState = Literal["ready", "reconnecting", "down"]
RuntimeState = Literal["ready", "reconnecting", "down"]


@dataclass(frozen=True, slots=True)
class HeartbeatSnapshot:
    schema_version: int
    pid: int
    process_generation: str
    written_at: str
    gateway_state: GatewayState
    gateway_down_since: str | None
    runtime_state: RuntimeState
    attached_sessions: int
    active_submissions: int
    observed_background_tasks: int
    active_or_unknown_native_schedules: int
    remote_steerable_or_unknown_sessions: int
    pending_interactions: int
    ingress_queue_depth: int
    max_reducer_lag_ms: int
    last_callback_at: str | None
    last_reducer_progress_at: str | None
    durable_replay_capable: bool
    suspect_executions: int = 0
    app_scheduler_state: str = "stopped"
    enabled_app_schedules: int = 0
    active_app_schedule_runs: int = 0
    unknown_app_schedule_runs: int = 0
    scheduler_last_tick_at: str | None = None
    heartbeat_frozen: bool = False
    frozen_reason: str | None = None
    process_started_at: str | None = None
    last_resume_at: str | None = None
    wake_suppression_until: str | None = None
    service_control_protocol: int = 1

    @property
    def protected_work(self) -> bool:
        return any(
            (
                self.active_submissions,
                self.observed_background_tasks,
                self.active_or_unknown_native_schedules,
                self.remote_steerable_or_unknown_sessions,
                self.pending_interactions,
                self.active_app_schedule_runs,
                self.unknown_app_schedule_runs,
                self.ingress_queue_depth,
            )
        )


class HeartbeatWriter:
    def __init__(
        self,
        database: Database,
        path: Path,
        *,
        interval_seconds: float = HEARTBEAT_WRITE_INTERVAL_SECONDS,
        gateway_down_seconds: float = GATEWAY_DOWN_RESTART_SECONDS,
        resume_suppression_seconds: float = RESUME_SUPPRESSION_SECONDS,
        durable_replay_capable: bool = False,
        process_generation: str | None = None,
        metrics_provider: Callable[[], tuple[int, int, float | None]] | None = None,
        resume_provider: ResumeTimestampProvider | None = None,
    ) -> None:
        self._database = database
        self._path = path
        self._interval_seconds = interval_seconds
        self._gateway_down_seconds = gateway_down_seconds
        self._resume_suppression_seconds = resume_suppression_seconds
        self._durable_replay_capable = durable_replay_capable
        self._process_generation = process_generation or str(uuid.uuid4())
        self._process_started_at = time.time()
        self._metrics_provider = metrics_provider
        self._resume_provider = resume_provider or resume_timestamp_provider(sys.platform)
        self._freeze_announced = False
        self.gateway_state: GatewayState = "down"
        self.gateway_down_since: float | None = time.time()
        self.runtime_state: RuntimeState = "down"

    def set_gateway(self, state: GatewayState) -> None:
        if state == "ready":
            self.gateway_down_since = None
            self._freeze_announced = False
        elif self.gateway_state == "ready" or self.gateway_down_since is None:
            self.gateway_down_since = time.time()
        self.gateway_state = state

    @property
    def durable_replay_capable(self) -> bool:
        return self._durable_replay_capable

    @durable_replay_capable.setter
    def durable_replay_capable(self, value: bool) -> None:
        self._durable_replay_capable = value

    @property
    def process_generation(self) -> str:
        return self._process_generation

    @property
    def process_started_at(self) -> float:
        return self._process_started_at

    async def run(self) -> None:
        while True:
            current = time.time()
            try:
                last_resume_at = await asyncio.to_thread(self._resume_provider)
            except (OSError, RuntimeError, ValueError):
                last_resume_at = None
            snapshot = await self.snapshot(now=current, last_resume_at=last_resume_at)
            gateway_down_for = (
                0.0 if self.gateway_down_since is None else current - self.gateway_down_since
            )
            should_freeze = (
                snapshot.gateway_state == "down"
                and gateway_down_for >= self._gateway_down_seconds
                and not snapshot.protected_work
            )
            if should_freeze and not self._freeze_announced:
                snapshot = replace(
                    snapshot,
                    heartbeat_frozen=True,
                    frozen_reason="gateway_down_unprotected",
                )
                await asyncio.to_thread(_atomic_json_write, self._path, asdict(snapshot))
                self._freeze_announced = True
            elif not should_freeze:
                self._freeze_announced = False
                await asyncio.to_thread(_atomic_json_write, self._path, asdict(snapshot))
            await asyncio.sleep(self._interval_seconds)

    async def snapshot(
        self,
        *,
        now: float | None = None,
        last_resume_at: float | None = None,
    ) -> HeartbeatSnapshot:
        current = time.time() if now is None else now
        counts = await self._database.fetchone(
            """
            SELECT
              (SELECT COUNT(*) FROM session_bindings
               WHERE attachment_state = 'attached') AS attached_sessions,
              (SELECT COUNT(*) FROM liveness_leases
               JOIN session_bindings USING (sdk_session_id)
               WHERE kind = 'submission' AND state = 'active'
                 AND liveness_leases.runtime_generation =
                     session_bindings.runtime_generation
                 AND liveness_leases.owner_fence_token =
                     session_bindings.owner_fence_token) AS active_submissions,
              (SELECT COUNT(*) FROM liveness_leases
               JOIN session_bindings USING (sdk_session_id)
               WHERE kind = 'observed_background' AND state = 'active'
                 AND liveness_leases.runtime_generation =
                     session_bindings.runtime_generation
                 AND liveness_leases.owner_fence_token =
                     session_bindings.owner_fence_token) AS background_tasks,
              (SELECT COUNT(*) FROM runtime_schedules
               WHERE state IN ('active', 'unknown')) AS native_schedules,
              (SELECT COUNT(*) FROM session_bindings
               WHERE attachment_state = 'attached'
                 AND runtime_remote_mode IN ('on', 'unknown')) AS remote_sessions,
              (SELECT COUNT(*) FROM pending_interactions
               JOIN session_bindings USING (sdk_session_id)
               WHERE pending_interactions.state = 'pending'
                 AND pending_interactions.runtime_generation =
                     session_bindings.runtime_generation
                 AND pending_interactions.owner_fence_token =
                     session_bindings.owner_fence_token) AS pending_interactions,
              (SELECT COUNT(*) FROM execution_health
               WHERE state = 'suspect') AS suspect_executions,
              (SELECT COUNT(*) FROM schedules
               WHERE state = 'enabled') AS enabled_app_schedules,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN (
                  'claimed', 'submitting', 'accepted', 'waiting'
               )) AS active_app_schedule_runs,
              (SELECT COUNT(*) FROM schedule_runs
               WHERE status IN (
                  'target_unknown', 'dispatch_unknown', 'outcome_unknown'
               )) AS unknown_app_schedule_runs
            """
        )
        latest = await self._database.fetchone(
            """
            SELECT MAX(last_event_at) AS last_reducer_progress_at
            FROM session_bindings
            WHERE attachment_state = 'attached'
            """
        )
        scheduler = await self._database.fetchone(
            """
            SELECT worker_state, last_tick_at
            FROM scheduler_state WHERE singleton = 1
            """
        )
        ingress_depth = 0
        reducer_lag_ms = 0
        last_callback_at: float | None = None
        if self._metrics_provider is not None:
            ingress_depth, reducer_lag_ms, last_callback_at = self._metrics_provider()
        effective_gateway = self.gateway_state
        if (
            effective_gateway != "ready"
            and self.gateway_down_since is not None
            and current - self.gateway_down_since >= self._gateway_down_seconds
        ):
            effective_gateway = "down"
        wake_suppression_until = (
            None if last_resume_at is None else last_resume_at + self._resume_suppression_seconds
        )
        return HeartbeatSnapshot(
            schema_version=HEARTBEAT_SCHEMA_VERSION,
            pid=os.getpid(),
            process_generation=self._process_generation,
            written_at=_rfc3339(current),
            gateway_state=effective_gateway,
            gateway_down_since=(
                None if self.gateway_down_since is None else _rfc3339(self.gateway_down_since)
            ),
            runtime_state=self.runtime_state,
            attached_sessions=int(counts["attached_sessions"]),
            active_submissions=int(counts["active_submissions"]),
            observed_background_tasks=int(counts["background_tasks"]),
            active_or_unknown_native_schedules=int(counts["native_schedules"]),
            remote_steerable_or_unknown_sessions=int(counts["remote_sessions"]),
            pending_interactions=int(counts["pending_interactions"]),
            ingress_queue_depth=ingress_depth,
            max_reducer_lag_ms=reducer_lag_ms,
            last_callback_at=_optional_rfc3339(last_callback_at),
            last_reducer_progress_at=_optional_rfc3339(latest["last_reducer_progress_at"]),
            durable_replay_capable=self._durable_replay_capable,
            suspect_executions=int(counts["suspect_executions"]),
            app_scheduler_state=(
                "stopped" if scheduler is None else str(scheduler["worker_state"])
            ),
            enabled_app_schedules=int(counts["enabled_app_schedules"]),
            active_app_schedule_runs=int(counts["active_app_schedule_runs"]),
            unknown_app_schedule_runs=int(counts["unknown_app_schedule_runs"]),
            scheduler_last_tick_at=(
                None if scheduler is None else _optional_rfc3339(scheduler["last_tick_at"])
            ),
            process_started_at=_rfc3339(self._process_started_at),
            last_resume_at=_optional_rfc3339(last_resume_at),
            wake_suppression_until=_optional_rfc3339(wake_suppression_until),
            service_control_protocol=SERVICE_CONTROL_PROTOCOL_VERSION,
        )


def read_heartbeat(path: Path) -> HeartbeatSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    snapshot = HeartbeatSnapshot(**payload)
    if snapshot.schema_version != HEARTBEAT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported heartbeat schema {snapshot.schema_version}; "
            f"expected {HEARTBEAT_SCHEMA_VERSION}"
        )
    if snapshot.pid <= 0 or not snapshot.process_generation:
        raise ValueError("heartbeat process identity is invalid")
    heartbeat_age_seconds(snapshot)
    return snapshot


def heartbeat_age_seconds(snapshot: HeartbeatSnapshot, *, now: float | None = None) -> float:
    current = time.time() if now is None else now
    written = datetime.fromisoformat(snapshot.written_at.replace("Z", "+00:00")).timestamp()
    return max(0.0, current - written)


def _optional_rfc3339(value: object) -> str | None:
    return None if value is None else _rfc3339(float(value))


def _rfc3339(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")


def _atomic_json_write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
        if os.name == "posix":
            path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)
