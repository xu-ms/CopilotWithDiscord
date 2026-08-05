from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from importlib.metadata import version
from pathlib import Path
from typing import Any

from copilot import CopilotClient, RuntimeConnection
from copilot.generated.rpc import (
    CommandsInvokeRequest,
    CommandsListRequest,
    EventLogReadRequest,
    MetadataContextInfoRequest,
    ModeSetRequest,
    SessionMode,
)
from copilot.session import CopilotSession
from copilot.session_events import SessionEvent, SessionEventType
from pydantic import SecretStr

from copilotd.config import Settings
from copilotd.core.task_registry import TaskRegistry
from copilotd.sdk.bridge import CopilotBridge
from copilotd.sdk.capabilities import (
    MAIN_BRANCH_ONLY_EVENTS,
    PINNED_GENERATED_EVENT_COUNT,
    PINNED_GENERATED_EVENT_SHA256,
    CapabilityRegistry,
)


@dataclass(frozen=True, slots=True)
class CapabilityResult:
    supported: bool
    detail: Any


class EventRecorder:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []
        self.queue: asyncio.Queue[SessionEvent] = asyncio.Queue()

    def callback(self, event: SessionEvent) -> None:
        payload = event.to_dict()
        with self._lock:
            self._events.append(payload)
        self._loop.call_soon_threadsafe(self.queue.put_nowait, event)

    @property
    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events)

    def drain(self) -> None:
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()

    async def wait_for(self, event_type: SessionEventType, wait_seconds: float) -> SessionEvent:
        async with asyncio.timeout(wait_seconds):
            while True:
                event = await self.queue.get()
                self.queue.task_done()
                if event.type == event_type:
                    return event


class SdkProbe:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def static_matrix(self) -> dict[str, Any]:
        matrix = CapabilityRegistry(self._settings).load_checked().to_dict()
        matrix["python"] = os.sys.version.split()[0]
        matrix["sdk_version"] = matrix["identity"]["sdk_version"]
        matrix["event_count"] = matrix["generated_events"]["count"]
        matrix["event_types"] = sorted(item.value for item in SessionEventType)
        matrix["main_branch_only_events"] = matrix["generated_events"]["main_branch_only"]
        return matrix

    async def run_live(
        self,
        *,
        prompt: str,
        wait_seconds: float,
        keep_session: bool,
        probe_native_schedule: bool,
        probe_sidecar: bool,
    ) -> dict[str, Any]:
        self._settings.ensure_directories()
        matrix = self.static_matrix()
        matrix["generated_at"] = datetime.now(UTC).isoformat()
        matrix["live"] = {}
        live: dict[str, Any] = matrix["live"]
        session_id = f"copilotd-probe-{uuid.uuid4()}"
        live["session_id"] = session_id

        with tempfile.TemporaryDirectory(prefix="copilotd-sdk-probe-") as workspace:
            process = await asyncio.create_subprocess_exec(
                "git",
                "init",
                "--quiet",
                cwd=workspace,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
            if process.returncode != 0:
                raise RuntimeError(f"git init failed: {stderr.decode(errors='replace').strip()}")
            bridge = CopilotBridge(self._settings)
            bridge_started = False
            session: CopilotSession | None = None
            resumed: CopilotSession | None = None
            all_events: list[dict[str, Any]] = []
            try:
                await bridge.start()
                bridge_started = True
                live["runtime"] = await bridge.runtime_identity()
                live["sessions_check_in_use"] = await self._probe_call(
                    "sessions.check_in_use",
                    lambda: bridge.check_session_in_use(session_id),
                    transform=lambda in_use: {"in_use": in_use},
                )
                live["transport_frames"] = await self._probe_transport_frames(bridge)
                live["models"] = await self._probe_call(
                    "models",
                    bridge.client.list_models,
                    transform=lambda models: [model.to_dict() for model in models],
                )

                recorder = EventRecorder(asyncio.get_running_loop())
                session = await bridge.create_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=recorder.callback,
                )
                live["actual_session_id"] = session.session_id
                live["session_id_matches"] = session.session_id == session_id
                live["permission_posture"] = asdict(await bridge.ensure_allow_all(session))

                live["mode_initial"] = await self._probe_call(
                    "mode.get", session.rpc.mode.get, transform=lambda mode: mode.value
                )
                live["mode_autopilot"] = await self._probe_mode_round_trip(session)
                live["model_current"] = await self._probe_call(
                    "model.get_current",
                    session.rpc.model.get_current,
                    transform=_to_jsonable,
                )
                live["activity"] = await self._probe_call(
                    "metadata.activity", session.rpc.metadata.activity, transform=_to_jsonable
                )
                live["processing"] = await self._probe_call(
                    "metadata.is_processing",
                    session.rpc.metadata.is_processing,
                    transform=_to_jsonable,
                )
                live["tasks"] = await self._probe_call(
                    "tasks.refresh",
                    session.rpc.tasks.refresh,
                    transform=_to_jsonable,
                )
                live["task_list"] = await self._probe_call(
                    "tasks.list",
                    session.rpc.tasks.list,
                    transform=_to_jsonable,
                )
                live["agents"] = await self._probe_call(
                    "agent.list",
                    session.rpc.agent.list,
                    transform=_to_jsonable,
                )
                live["agent_current"] = await self._probe_call(
                    "agent.get_current",
                    session.rpc.agent.get_current,
                    transform=_to_jsonable,
                )
                live["queue"] = await self._probe_call(
                    "queue.pending_items",
                    session.rpc.queue.pending_items,
                    transform=_to_jsonable,
                )
                live["schedule"] = await self._probe_call(
                    "schedule.list", session.rpc.schedule.list, transform=_to_jsonable
                )
                live["commands"] = await self._probe_call(
                    "commands.list",
                    lambda: session.rpc.commands.list(
                        CommandsListRequest(
                            include_builtins=True,
                            include_client_commands=False,
                            include_skills=False,
                        ),
                        timeout=10,
                    ),
                    transform=_to_jsonable,
                )

                recorder.drain()
                accepted_message_id = await session.send(prompt, agent_mode="interactive")
                live["accepted_message_id"] = accepted_message_id
                await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
                live["first_generation_event_count"] = len(recorder.events)

                recorder.drain()
                followup_message_id = await session.send(
                    "Reply with exactly COPILOTD_STREAM_STILL_ALIVE and do not use tools.",
                    agent_mode="interactive",
                )
                live["followup_message_id"] = followup_message_id
                await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
                live["callback_survived_idle"] = (
                    len(recorder.events) > live["first_generation_event_count"]
                )
                live["metadata_snapshot"] = await self._probe_call(
                    "metadata.snapshot",
                    session.rpc.metadata.snapshot,
                    transform=_to_jsonable,
                )
                live["usage_metrics"] = await self._probe_call(
                    "usage.get_metrics",
                    session.rpc.usage.get_metrics,
                    transform=_to_jsonable,
                )
                live["context_info"] = await self._probe_call(
                    "metadata.context_info",
                    lambda: session.rpc.metadata.context_info(
                        MetadataContextInfoRequest(
                            output_token_limit=0,
                            prompt_token_limit=0,
                            selected_model=None,
                        ),
                        timeout=10,
                    ),
                    transform=_to_jsonable,
                )
                live["plan_read"] = await self._probe_call(
                    "plan.read",
                    session.rpc.plan.read,
                    transform=_to_jsonable,
                )
                live["event_log_tail"] = await self._probe_call(
                    "event_log.tail",
                    session.rpc.event_log.tail,
                    transform=_to_jsonable,
                )
                live["event_log_read"] = await self._probe_call(
                    "event_log.read",
                    lambda: session.rpc.event_log.read(
                        EventLogReadRequest(max=100, wait_ms=0),
                        timeout=10,
                    ),
                    transform=_summarize_event_log,
                )
                if probe_native_schedule:
                    live["native_schedule_direct"] = await self._probe_native_schedule(
                        session,
                        recorder,
                        wait_seconds=wait_seconds,
                    )
                if probe_sidecar:
                    live["sidecar_replay"] = await self._probe_sidecar_replay(
                        wait_seconds=wait_seconds
                    )
                all_events.extend(recorder.events)
                live["history_before_disconnect"] = len(await session.get_events())
                await session.disconnect()
                session = None

                resume_recorder = EventRecorder(asyncio.get_running_loop())
                resumed = await bridge.resume_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=resume_recorder.callback,
                    continue_pending_work=True,
                )
                live["resume_session_id_matches"] = resumed.session_id == session_id
                await bridge.ensure_allow_all(resumed)
                history = await resumed.get_events()
                live["history_after_resume"] = len(history)
                live["durable_history_recovered"] = len(history) > 0
                all_events.extend(resume_recorder.events)
                await resumed.disconnect()
                resumed = None
            finally:
                if resumed is not None:
                    await resumed.disconnect()
                if session is not None:
                    await session.disconnect()
                if bridge_started and not keep_session:
                    try:
                        await bridge.client.delete_session(session_id)
                        live["session_deleted"] = True
                    except Exception as error:
                        live["session_deleted"] = False
                        live["delete_error"] = _error_detail(error)
                if bridge_started:
                    await bridge.stop()

        fixture_path = self._write_fixture(session_id, all_events)
        fixture_sha256 = _sha256_file(fixture_path)
        live["fixture_path"] = str(fixture_path)
        live["fixture_sha256"] = fixture_sha256
        live["recorded_event_count"] = len(all_events)
        accepted_ids = {
            str(item)
            for item in (
                live.get("accepted_message_id"),
                live.get("followup_message_id"),
            )
            if item is not None
        }
        user_event_ids = {
            str(event.get("id"))
            for event in all_events
            if event.get("type") == SessionEventType.USER_MESSAGE.value
        }
        live["accepted_user_event_id_mapping"] = bool(accepted_ids) and accepted_ids.issubset(
            user_event_ids
        )
        matrix = self._live_matrix(live, fixture_path, fixture_sha256)
        matrix["live"] = live
        self._write_matrix(matrix)
        return matrix

    def _live_matrix(
        self,
        live: dict[str, Any],
        fixture_path: Path,
        fixture_sha256: str,
    ) -> dict[str, Any]:
        runtime = live["runtime"]
        native_schedule = live.get("native_schedule_direct")
        sidecar = live.get("sidecar_replay")
        commands = live.get("commands")
        event_log_read = live.get("event_log_read")
        event_log_tail = live.get("event_log_tail")
        capabilities = {
            "accepted_user_event_id_mapping": _evidence(
                live.get("accepted_user_event_id_mapping") is True,
                "live-callback-probe",
                {
                    "accepted_message_id": live.get("accepted_message_id"),
                    "followup_message_id": live.get("followup_message_id"),
                    "matched": live.get("accepted_user_event_id_mapping"),
                },
            ),
            "activity_snapshot": _evidence(
                _supported(live.get("activity")) and _supported(live.get("processing")),
                "live-rpc-probe",
                {"activity": live.get("activity"), "processing": live.get("processing")},
            ),
            "builtin_commands": _evidence(
                _supported(commands) and _supported(native_schedule),
                "live-command-probe",
                {"list": commands, "disposable_invoke": native_schedule},
            ),
            "context_info": _evidence(
                _supported(live.get("context_info")),
                "live-rpc-probe",
                live.get("context_info"),
            ),
            "detached_continuation": _evidence(
                _supported(sidecar),
                "live-sidecar-probe",
                sidecar or {"reason": "sidecar probe not requested"},
            ),
            "event_log": _evidence(
                _supported(event_log_read) and _supported(event_log_tail),
                "live-rpc-probe",
                {"read": event_log_read, "tail": event_log_tail},
            ),
            "model_config": _evidence(
                False,
                "unprobed",
                "live probe did not mutate model configuration",
            ),
            "models": _evidence(
                _supported(live.get("models")),
                "live-rpc-probe",
                live.get("models"),
            ),
            "native_queue_snapshot": _evidence(
                _supported(live.get("queue")),
                "live-rpc-probe",
                live.get("queue"),
            ),
            "native_schedule": _evidence(
                _supported(native_schedule),
                "live-command-probe",
                native_schedule or {"reason": "native schedule probe not requested"},
            ),
            "permission_allow_all": _evidence(
                bool((live.get("permission_posture") or {}).get("enabled"))
                and (live.get("permission_posture") or {}).get("mode") == "on",
                "live-rpc-probe",
                live.get("permission_posture"),
            ),
            "persistent_history": _evidence(
                live.get("durable_history_recovered") is True,
                "live-resume-probe",
                {
                    "before_disconnect": live.get("history_before_disconnect"),
                    "after_resume": live.get("history_after_resume"),
                },
            ),
            "pre_registered_on_event": _evidence(
                live.get("session_id_matches") is True
                and live.get("resume_session_id_matches") is True
                and live.get("callback_survived_idle") is True,
                "live-callback-probe",
                {
                    "create_id_matches": live.get("session_id_matches"),
                    "resume_id_matches": live.get("resume_session_id_matches"),
                    "callback_survived_idle": live.get("callback_survived_idle"),
                },
            ),
            "reasoning_summary_readback": _evidence(
                False,
                "unprobed",
                "no durable reasoning-summary readback was exercised",
            ),
            "remote": _evidence(
                False,
                "unprobed",
                "remote enable/disable was not exercised",
            ),
            "selected_agent": _evidence(
                _supported(live.get("agents")) and _supported(live.get("agent_current")),
                "live-rpc-probe",
                {"list": live.get("agents"), "current": live.get("agent_current")},
            ),
            "session_mode": _evidence(
                _supported(live.get("mode_initial"))
                and _supported(live.get("mode_autopilot")),
                "live-rpc-probe",
                {
                    "initial": live.get("mode_initial"),
                    "round_trip": live.get("mode_autopilot"),
                },
            ),
            "sessions_check_in_use": _evidence(
                _supported(live.get("sessions_check_in_use")),
                "live-rpc-probe",
                live.get("sessions_check_in_use"),
            ),
            "task_snapshot": _evidence(
                _supported(live.get("tasks")) and _supported(live.get("task_list")),
                "live-rpc-probe",
                {"refresh": live.get("tasks"), "list": live.get("task_list")},
            ),
            "usage": _evidence(
                _supported(live.get("usage_metrics")),
                "live-rpc-probe",
                live.get("usage_metrics"),
            ),
        }
        return {
            "schema_version": 1,
            "source": "live-probe",
            "generated_at": datetime.now(UTC).isoformat(),
            "identity": {
                "sdk_version": version("github-copilot-sdk"),
                "runtime_version": runtime["runtime_version"],
                "protocol_version": runtime["protocol_version"],
                "ping_protocol_version": runtime["ping_protocol_version"],
            },
            "generated_events": {
                "count": PINNED_GENERATED_EVENT_COUNT,
                "sha256": PINNED_GENERATED_EVENT_SHA256,
                "main_branch_only": list(MAIN_BRANCH_ONLY_EVENTS),
            },
            "capabilities": capabilities,
            "fixture": {
                "path": str(fixture_path),
                "sha256": fixture_sha256,
            },
        }

    async def _probe_mode_round_trip(self, session: CopilotSession) -> CapabilityResult:
        try:
            await session.rpc.mode.set(
                ModeSetRequest(mode=SessionMode.AUTOPILOT),
                timeout=10,
            )
            autopilot = await session.rpc.mode.get(timeout=10)
            await session.rpc.mode.set(
                ModeSetRequest(mode=SessionMode.INTERACTIVE),
                timeout=10,
            )
            interactive = await session.rpc.mode.get(timeout=10)
            return CapabilityResult(
                autopilot == SessionMode.AUTOPILOT and interactive == SessionMode.INTERACTIVE,
                {
                    "autopilot": autopilot.value,
                    "restored": interactive.value,
                    "prompt_sent": False,
                },
            )
        except Exception as error:
            return CapabilityResult(False, _error_detail(error))

    async def _probe_transport_frames(self, bridge: CopilotBridge) -> dict[str, Any]:
        results: dict[str, Any] = {}
        for size_mib in (1, 5, 10):
            payload = "x" * (size_mib * 1024 * 1024)
            started_at = time.perf_counter()
            try:
                response = await bridge.client.ping(payload)
            except Exception as error:
                results[str(size_mib)] = CapabilityResult(False, _error_detail(error))
                continue
            expected_size = len(payload) + len("pong: ")
            results[str(size_mib)] = CapabilityResult(
                len(response.message) == expected_size,
                {
                    "request_bytes": len(payload),
                    "response_bytes": len(response.message),
                    "elapsed_seconds": round(time.perf_counter() - started_at, 3),
                },
            )
        return results

    async def _probe_native_schedule(
        self,
        session: CopilotSession,
        recorder: EventRecorder,
        *,
        wait_seconds: float,
    ) -> CapabilityResult:
        recorder.drain()
        event_start = len(recorder.events)
        try:
            result = await session.rpc.commands.invoke(
                CommandsInvokeRequest(
                    name="after",
                    input=("10s Reply with exactly COPILOTD_AFTER_OK and do not use tools."),
                ),
                timeout=10,
            )
            invocation = _to_jsonable(result)
            if invocation.get("kind") not in {"completed", "text"}:
                return CapabilityResult(
                    False,
                    {
                        "reason": "builtin did not complete scheduling directly",
                        "invocation": invocation,
                    },
                )
            await recorder.wait_for(
                SessionEventType.SESSION_SCHEDULE_CREATED,
                min(wait_seconds, 30),
            )
            before_trigger = _to_jsonable(await session.rpc.schedule.list(timeout=10))
            await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
            after_trigger = _to_jsonable(await session.rpc.schedule.list(timeout=10))
        except Exception as error:
            return CapabilityResult(False, _error_detail(error))

        observed = recorder.events[event_start:]
        observed_types = [event["type"] for event in observed]
        triggered = (
            SessionEventType.USER_MESSAGE.value in observed_types
            and SessionEventType.SESSION_IDLE.value in observed_types
        )
        return CapabilityResult(
            triggered,
            {
                "invocation": invocation,
                "before_trigger": before_trigger,
                "after_trigger": after_trigger,
                "observed_event_types": observed_types,
            },
        )

    async def _probe_sidecar_replay(self, *, wait_seconds: float) -> CapabilityResult:
        sidecar: asyncio.subprocess.Process | None = None
        stdout_task: asyncio.Task[None] | None = None
        stderr_task: asyncio.Task[None] | None = None
        stdout_tail: deque[str] = deque(maxlen=100)
        stderr_tail: deque[str] = deque(maxlen=100)
        bridge: CopilotBridge | None = None
        resumed_bridge: CopilotBridge | None = None
        session: CopilotSession | None = None
        resumed: CopilotSession | None = None
        session_id = f"copilotd-sidecar-{uuid.uuid4()}"
        detail: dict[str, Any] = {"session_id": session_id}
        probe_tasks = TaskRegistry()

        try:
            runtime_path = _resolve_runtime_path()
            connection_token = uuid.uuid4().hex
            environment = dict(os.environ)
            environment["COPILOT_CONNECTION_TOKEN"] = connection_token
            sidecar = await asyncio.create_subprocess_exec(
                runtime_path,
                "--yolo",
                "--headless",
                "--no-auto-update",
                "--log-level",
                self._settings.sdk_log_level,
                "--remote",
                "--port",
                "0",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=environment,
            )
            port = await _read_sidecar_port(sidecar, stdout_tail)
            detail["port"] = port
            detail["runtime_path"] = runtime_path
            assert sidecar.stdout is not None
            assert sidecar.stderr is not None
            stdout_task = probe_tasks.create(
                _drain_stream(sidecar.stdout, stdout_tail),
                name="sdk-probe-sidecar-stdout",
                source="sdk-probe",
            )
            stderr_task = probe_tasks.create(
                _drain_stream(sidecar.stderr, stderr_tail),
                name="sdk-probe-sidecar-stderr",
                source="sdk-probe",
            )

            uri = f"127.0.0.1:{port}"
            sidecar_settings = self._settings.model_copy(
                update={
                    "runtime_uri": uri,
                    "runtime_connection_token": SecretStr(connection_token),
                }
            )
            bridge = CopilotBridge(sidecar_settings)
            await bridge.start()
            recorder = EventRecorder(asyncio.get_running_loop())
            with tempfile.TemporaryDirectory(prefix="copilotd-sidecar-session-") as workspace:
                process = await asyncio.create_subprocess_exec(
                    "git",
                    "init",
                    "--quiet",
                    cwd=workspace,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, git_stderr = await process.communicate()
                if process.returncode != 0:
                    raise RuntimeError(
                        f"git init failed: {git_stderr.decode(errors='replace').strip()}"
                    )
                session = await bridge.create_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=recorder.callback,
                )
                detail["actual_session_id"] = session.session_id
                detail["sessions_before_disconnect"] = [
                    item.session_id for item in await bridge.client.list_sessions()
                ]
                await bridge.ensure_allow_all(session)
                invocation = await session.rpc.commands.invoke(
                    CommandsInvokeRequest(
                        name="after",
                        input=("10s Reply with exactly COPILOTD_SIDECAR_OK and do not use tools."),
                    ),
                    timeout=10,
                )
                detail["invocation"] = _to_jsonable(invocation)
                await recorder.wait_for(
                    SessionEventType.SESSION_SCHEDULE_CREATED,
                    min(wait_seconds, 30),
                )
                detail["schedule_before_disconnect"] = _to_jsonable(
                    await session.rpc.schedule.list(timeout=10)
                )
                session = None
                await bridge.force_stop()
                bridge = None
                detached_at = datetime.now(UTC)
                detail["detached_at"] = detached_at.isoformat()

                await asyncio.sleep(13)
                reconnect_started_at = datetime.now(UTC)
                detail["reconnect_started_at"] = reconnect_started_at.isoformat()
                detail["process_survived_disconnect"] = sidecar.returncode is None
                if sidecar.returncode is not None:
                    raise RuntimeError(
                        f"sidecar exited while detached with code {sidecar.returncode}"
                    )

                resumed_bridge = CopilotBridge(sidecar_settings)
                await resumed_bridge.start()
                detail["sessions_after_reconnect"] = [
                    item.session_id for item in await resumed_bridge.client.list_sessions()
                ]
                if session_id not in detail["sessions_after_reconnect"]:
                    raise RuntimeError(
                        "sidecar did not retain the session after the client transport closed"
                    )
                resume_recorder = EventRecorder(asyncio.get_running_loop())
                resumed = await resumed_bridge.resume_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=resume_recorder.callback,
                    continue_pending_work=True,
                )
                history = await resumed.get_events()
                detail["history_event_count"] = len(history)
                scheduled_user_events = [
                    event
                    for event in history
                    if event.type == SessionEventType.USER_MESSAGE
                    and str(event.data.to_dict().get("source", "")).startswith("schedule-")
                ]
                assistant_events = [
                    event
                    for event in history
                    if event.type == SessionEventType.ASSISTANT_MESSAGE
                    and event.timestamp <= reconnect_started_at
                ]
                detail["scheduled_user_events_before_reconnect"] = len(
                    [
                        event
                        for event in scheduled_user_events
                        if event.timestamp <= reconnect_started_at
                    ]
                )
                detail["assistant_events_before_reconnect"] = len(assistant_events)
                detail["schedule_after_reconnect"] = _to_jsonable(
                    await resumed.rpc.schedule.list(timeout=10)
                )
                await resumed.disconnect()
                resumed = None
                await resumed_bridge.client.delete_session(session_id)
                await resumed_bridge.stop()
                resumed_bridge = None
        except Exception as error:
            detail["error"] = _error_detail(error)
        finally:
            if resumed is not None:
                await resumed.disconnect()
            if resumed_bridge is not None:
                try:
                    await resumed_bridge.client.delete_session(session_id)
                except Exception as error:
                    detail["cleanup_delete_error"] = _error_detail(error)
                await resumed_bridge.stop()
            if session is not None:
                await session.disconnect()
            if bridge is not None:
                try:
                    await bridge.client.delete_session(session_id)
                except Exception as error:
                    detail["cleanup_delete_error"] = _error_detail(error)
                await bridge.stop()
            if sidecar is not None and sidecar.returncode is None:
                sidecar.terminate()
                try:
                    await asyncio.wait_for(sidecar.wait(), timeout=5)
                except TimeoutError:
                    sidecar.kill()
                    await sidecar.wait()
            for task in (stdout_task, stderr_task):
                if task is not None:
                    await task
            detail["stdout_tail"] = list(stdout_tail)
            detail["stderr_tail"] = list(stderr_tail)

        supported = (
            detail.get("process_survived_disconnect") is True
            and detail.get("scheduled_user_events_before_reconnect", 0) > 0
            and detail.get("assistant_events_before_reconnect", 0) > 0
        )
        return CapabilityResult(supported, detail)

    async def _probe_call(
        self,
        _name: str,
        operation: Any,
        *,
        transform: Any = None,
    ) -> CapabilityResult:
        try:
            result = await operation()
        except Exception as error:
            return CapabilityResult(False, _error_detail(error))
        formatter = _to_jsonable if transform is None else transform
        return CapabilityResult(True, formatter(result))

    def _write_fixture(
        self,
        session_id: str,
        events: list[dict[str, Any]],
    ) -> Path:
        fixture_dir = self._settings.capability_path.parent
        fixture_dir.mkdir(parents=True, exist_ok=True)
        fixture_path = fixture_dir / f"{session_id}.events.jsonl"
        with fixture_path.open("w", encoding="utf-8") as handle:
            for sequence, event in enumerate(events, start=1):
                handle.write(
                    json.dumps(
                        {"sdk_receive_seq": sequence, "event": event},
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                handle.write("\n")
        return fixture_path

    def _write_matrix(self, matrix: dict[str, Any]) -> None:
        target = self._settings.capability_path
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(
            json.dumps(matrix, default=_to_jsonable, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(target)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, CapabilityResult):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    return value


def _error_detail(error: Exception) -> dict[str, str]:
    return {
        "type": type(error).__name__,
        "message": str(error),
    }


def _summarize_event_log(value: Any) -> dict[str, Any]:
    payload = _to_jsonable(value)
    events = payload.get("events", [])
    return {
        "event_count": len(events),
        "event_types": [event.get("type") for event in events],
        "cursor": payload.get("cursor"),
        "has_more": payload.get("hasMore"),
        "cursor_expired": payload.get("cursorExpired"),
    }


def _supported(value: Any) -> bool:
    if isinstance(value, CapabilityResult):
        return value.supported
    if isinstance(value, dict):
        return value.get("supported") is True
    return False


def _evidence(supported: bool, evidence_kind: str, detail: Any) -> dict[str, Any]:
    return {
        "supported": supported,
        "evidence_kind": evidence_kind,
        "detail": _to_jsonable(detail),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_runtime_path() -> str:
    connection = RuntimeConnection.for_tcp()
    CopilotClient(connection=connection)
    if connection.path is None:
        raise RuntimeError("SDK did not resolve a bundled runtime path")
    return connection.path


async def _read_sidecar_port(
    process: asyncio.subprocess.Process,
    stdout_tail: deque[str],
) -> int:
    if process.stdout is None:
        raise RuntimeError("sidecar stdout is unavailable")
    async with asyncio.timeout(10):
        while line := await process.stdout.readline():
            text = line.decode(errors="replace").rstrip()
            stdout_tail.append(text)
            match = re.search(r"listening on port (\d+)", text, re.IGNORECASE)
            if match:
                return int(match.group(1))
    raise RuntimeError("sidecar exited before announcing a port")


async def _drain_stream(
    stream: asyncio.StreamReader,
    tail: deque[str],
) -> None:
    while line := await stream.readline():
        tail.append(line.decode(errors="replace").rstrip())
