from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

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
            )
        )


class HeartbeatWriter:
    def __init__(
        self,
        database: Database,
        path: Path,
        *,
        interval_seconds: float = 30,
        durable_replay_capable: bool = False,
    ) -> None:
        self._database = database
        self._path = path
        self._interval_seconds = interval_seconds
        self._durable_replay_capable = durable_replay_capable
        self._process_generation = str(uuid.uuid4())
        self.gateway_state: GatewayState = "down"
        self.gateway_down_since: float | None = time.time()
        self.runtime_state: RuntimeState = "down"

    def set_gateway(self, state: GatewayState) -> None:
        if state == "ready":
            self.gateway_down_since = None
        elif self.gateway_state == "ready" or self.gateway_down_since is None:
            self.gateway_down_since = time.time()
        self.gateway_state = state

    @property
    def durable_replay_capable(self) -> bool:
        return self._durable_replay_capable

    @durable_replay_capable.setter
    def durable_replay_capable(self, value: bool) -> None:
        self._durable_replay_capable = value

    async def run(self) -> None:
        while True:
            snapshot = await self.snapshot()
            gateway_down_for = (
                0.0 if self.gateway_down_since is None else time.time() - self.gateway_down_since
            )
            if not (
                self.gateway_state == "down"
                and gateway_down_for >= 600
                and not snapshot.protected_work
            ):
                await asyncio.to_thread(_atomic_json_write, self._path, asdict(snapshot))
            await asyncio.sleep(self._interval_seconds)

    async def snapshot(self) -> HeartbeatSnapshot:
        counts = await self._database.fetchone(
            """
            SELECT
              (SELECT COUNT(*) FROM session_bindings
               WHERE attachment_state = 'attached') AS attached_sessions,
              (SELECT COUNT(*) FROM liveness_leases
               WHERE kind = 'submission' AND state = 'active') AS active_submissions,
              (SELECT COUNT(*) FROM liveness_leases
               WHERE kind = 'background' AND state = 'active') AS background_tasks,
              (SELECT COUNT(*) FROM runtime_schedules
               WHERE state IN ('active', 'unknown')) AS native_schedules,
              (SELECT COUNT(*) FROM session_bindings
               WHERE runtime_remote_mode IN ('on', 'unknown')) AS remote_sessions,
              (SELECT COUNT(*) FROM pending_interactions
               WHERE state = 'pending') AS pending_interactions,
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
            SELECT MAX(last_event_at) AS last_callback_at,
                   MAX(updated_at) AS last_reducer_progress_at
            FROM session_bindings
            """
        )
        scheduler = await self._database.fetchone(
            """
            SELECT worker_state, last_tick_at
            FROM scheduler_state WHERE singleton = 1
            """
        )
        return HeartbeatSnapshot(
            schema_version=1,
            pid=os.getpid(),
            process_generation=self._process_generation,
            written_at=_rfc3339(time.time()),
            gateway_state=self.gateway_state,
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
            ingress_queue_depth=0,
            max_reducer_lag_ms=0,
            last_callback_at=_optional_rfc3339(latest["last_callback_at"]),
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
        )


def read_heartbeat(path: Path) -> HeartbeatSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return HeartbeatSnapshot(**payload)


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
    finally:
        temporary.unlink(missing_ok=True)
