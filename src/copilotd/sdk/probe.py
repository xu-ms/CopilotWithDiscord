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
from copilot.session import CopilotSession
from copilot.session_events import SessionEvent, SessionEventType
from pydantic import SecretStr

from copilotd.config import Settings
from copilotd.core.task_registry import TaskRegistry
from copilotd.sdk.bridge import (
    BRIDGE_ACCEPTANCE_LANES,
    CopilotBridge,
    ManagedAwarePermissionHandler,
)
from copilotd.sdk.capabilities import (
    CAPABILITY_SCHEMA_VERSION,
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


async def _disposable_owner_is_valid() -> bool:
    return True


def _disposable_permission_handler() -> ManagedAwarePermissionHandler:
    return ManagedAwarePermissionHandler(approval_validator=_disposable_owner_is_valid)


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
        matrix["bridge_acceptance_lanes"] = {
            operation: list(lanes) for operation, lanes in sorted(BRIDGE_ACCEPTANCE_LANES.items())
        }
        return matrix

    async def run_live(
        self,
        *,
        prompt: str,
        wait_seconds: float,
        keep_session: bool,
        probe_native_schedule: bool,
        probe_sidecar: bool,
        expected_response: str | None = None,
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
                live["healthcheck"] = await self._probe_call(
                    "bridge.healthcheck",
                    bridge.healthcheck,
                    transform=lambda _result: {"completed": True},
                )
                live["transport_ping"] = await self._probe_call(
                    "bridge.transport_ping",
                    bridge.transport_ping,
                    transform=lambda result: result,
                )
                live["sessions_check_in_use"] = await self._probe_call(
                    "sessions.check_in_use",
                    lambda: bridge.check_session_in_use(session_id),
                    transform=lambda in_use: {"in_use": in_use},
                )
                live["transport_frames"] = await self._probe_transport_frames(bridge)
                live["models"] = await self._probe_call(
                    "models",
                    bridge.list_models,
                    transform=lambda models: models,
                )

                recorder = EventRecorder(asyncio.get_running_loop())
                session = await bridge.create_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=recorder.callback,
                    permission_handler=_disposable_permission_handler(),
                )
                live["actual_session_id"] = session.session_id
                live["session_id_matches"] = session.session_id == session_id
                live["session_exists_immediately_after_create"] = await bridge.session_exists(
                    session_id
                )
                live["permission_posture"] = asdict(await bridge.ensure_allow_all(session))

                live["mode_initial"] = await self._probe_call(
                    "bridge.get_mode",
                    lambda: bridge.get_mode(session),
                    transform=lambda mode: mode,
                )
                live["mode_autopilot"] = await self._probe_mode_round_trip(bridge, session)
                live["model_current"] = await self._probe_call(
                    "bridge.get_current_model",
                    lambda: bridge.get_current_model(session),
                    transform=lambda result: result,
                )
                live["activity"] = await self._probe_call(
                    "bridge.get_readiness.activity",
                    lambda: bridge.get_readiness(session),
                    transform=lambda result: {
                        "hasActiveWork": result["hasActiveWork"],
                        "abortable": result["abortable"],
                    },
                )
                live["processing"] = await self._probe_call(
                    "bridge.get_readiness.processing",
                    lambda: bridge.get_readiness(session),
                    transform=lambda result: {"processing": result["processing"]},
                )
                live["tasks"] = await self._probe_call(
                    "bridge.refresh_tasks",
                    lambda: bridge.refresh_tasks(session),
                    transform=lambda _result: {"completed": True},
                )
                live["task_list"] = await self._probe_call(
                    "bridge.list_tasks",
                    lambda: bridge.list_tasks(session),
                    transform=lambda tasks: {"tasks": tasks},
                )
                live["task_snapshot"] = await self._probe_call(
                    "bridge.get_tasks",
                    lambda: bridge.get_tasks(session),
                    transform=lambda tasks: {"tasks": tasks},
                )
                live["agents"] = await self._probe_call(
                    "bridge.get_agents",
                    lambda: bridge.get_agents(session),
                    transform=lambda result: result,
                )
                live["agent_current"] = await self._probe_call(
                    "bridge.get_current_agent_info",
                    lambda: bridge.get_current_agent_info(session),
                    transform=lambda result: result,
                )
                live["agent_current_name"] = await self._probe_call(
                    "bridge.get_current_agent",
                    lambda: bridge.get_current_agent(session),
                    transform=lambda result: {"name": result},
                )
                live["queue"] = await self._probe_call(
                    "bridge.get_readiness.queue",
                    lambda: bridge.get_readiness(session),
                    transform=lambda result: {
                        "items": result["pendingItems"],
                        "steeringMessages": result["steeringMessages"],
                    },
                )
                live["schedule"] = await self._probe_call(
                    "bridge.get_native_schedules",
                    lambda: bridge.get_native_schedules(session),
                    transform=lambda entries: {"entries": entries},
                )
                live["commands"] = await self._probe_call(
                    "bridge.list_commands",
                    lambda: bridge.list_commands(session, include_builtins=True),
                    transform=lambda commands: [command.to_dict() for command in commands],
                )
                live["mcp_servers"] = await self._probe_call(
                    "bridge.get_mcp_servers",
                    lambda: bridge.get_mcp_servers(session),
                    transform=lambda result: result,
                )
                live["skills"] = await self._probe_call(
                    "bridge.get_skills",
                    lambda: bridge.get_skills(session),
                    transform=lambda result: result,
                )
                live["session_auth"] = await self._probe_call(
                    "bridge.get_session_auth",
                    lambda: bridge.get_session_auth(session),
                    transform=lambda result: result,
                )

                recorder.drain()
                accepted_message_id = await bridge.send(
                    session,
                    prompt,
                    agent_mode="interactive",
                )
                live["accepted_message_id"] = accepted_message_id
                await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
                live["session_exists_after_activity"] = await bridge.session_exists(session_id)
                live["first_generation_event_count"] = len(recorder.events)
                if expected_response is not None:
                    live["expected_response"] = expected_response
                    live["response_matched"] = _response_matches(
                        recorder.events,
                        expected_response,
                    )
                    if not live["response_matched"]:
                        raise RuntimeError(
                            f"live probe assistant response did not match {expected_response!r}"
                        )

                recorder.drain()
                followup_message_id = await bridge.send(
                    session,
                    "Reply with exactly COPILOTD_STREAM_STILL_ALIVE and do not use tools.",
                    agent_mode="interactive",
                )
                live["followup_message_id"] = followup_message_id
                await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
                live["callback_survived_idle"] = (
                    len(recorder.events) > live["first_generation_event_count"]
                )
                live["abort_round_trip"] = await self._probe_abort_round_trip(
                    bridge,
                    session,
                    recorder,
                    wait_seconds=wait_seconds,
                )
                _require_supported_probe(
                    live["abort_round_trip"],
                    "abort and post-abort recovery acceptance",
                )
                live["native_queue_clear"] = await self._probe_call(
                    "bridge.clear_native_queue",
                    lambda: bridge.clear_native_queue(session),
                    transform=lambda _result: {"completed": True},
                )
                live["metadata_snapshot"] = await self._probe_call(
                    "bridge.get_remote_state",
                    lambda: bridge.get_remote_state(session),
                    transform=lambda result: result,
                )
                live["usage_metrics"] = await self._probe_call(
                    "bridge.get_usage",
                    lambda: bridge.get_usage(session),
                    transform=lambda result: result,
                )
                live["context_info"] = await self._probe_call(
                    "bridge.get_context",
                    lambda: bridge.get_context(session),
                    transform=lambda result: result,
                )
                live["plan_read"] = await self._probe_call(
                    "bridge.read_plan",
                    lambda: bridge.read_plan(session),
                    transform=lambda result: result,
                )
                live["event_log_tail"] = await self._probe_call(
                    "bridge.tail_event_log",
                    lambda: bridge.tail_event_log(session),
                    transform=lambda cursor: {"cursor_present": bool(cursor)},
                )
                live["event_log_read"] = await self._probe_call(
                    "bridge.read_event_log",
                    lambda: bridge.read_event_log(session, cursor=None, max_events=100),
                    transform=lambda batch: {
                        "cursor_status": batch.cursor_status,
                        "event_count": len(batch.events),
                        "has_more": batch.has_more,
                        "filtered_ephemeral": batch.filtered_ephemeral,
                    },
                )
                if probe_native_schedule:
                    live["native_schedule_direct"] = await self._probe_native_schedule(
                        bridge,
                        session,
                        recorder,
                        wait_seconds=wait_seconds,
                    )
                if probe_sidecar:
                    live["sidecar_replay"] = await self._probe_sidecar_replay(
                        wait_seconds=wait_seconds
                    )
                all_events.extend(recorder.events)
                live["history_before_disconnect"] = len(await bridge.get_events(session))
                await bridge.disconnect(session)
                session = None

                resume_recorder = EventRecorder(asyncio.get_running_loop())
                resumed = await bridge.resume_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=resume_recorder.callback,
                    continue_pending_work=True,
                    permission_handler=_disposable_permission_handler(),
                )
                live["resume_session_id_matches"] = resumed.session_id == session_id
                await bridge.ensure_allow_all(resumed)
                history = await bridge.get_events(resumed)
                live["history_after_resume"] = len(history)
                live["durable_history_recovered"] = len(history) > 0
                all_events.extend(resume_recorder.events)
                await bridge.disconnect(resumed)
                resumed = None
            finally:
                await self._cleanup_live_resources(
                    bridge,
                    session_id=session_id,
                    active_sessions=(("resumed", resumed), ("session", session)),
                    bridge_started=bridge_started,
                    keep_session=keep_session,
                    live=live,
                )

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
        lifecycle_deletion_skipped = live.get("session_deletion_skipped") is True
        lifecycle_core_supported = (
            live.get("session_id_matches") is True
            and live.get("session_exists_immediately_after_create") is True
            and live.get("session_exists_after_activity") is True
            and live.get("resume_session_id_matches") is True
        )
        lifecycle_supported = (
            None
            if lifecycle_core_supported and lifecycle_deletion_skipped
            else lifecycle_core_supported and live.get("session_deleted") is True
        )
        capabilities = {
            "abort_recovery": _evidence(
                (
                    None
                    if "abort_round_trip" not in live
                    else _supported(live.get("abort_round_trip"))
                ),
                ("unprobed" if "abort_round_trip" not in live else "live-abort-recovery-probe"),
                live.get("abort_round_trip"),
            ),
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
                (
                    None
                    if native_schedule is None
                    else _supported(commands) and _supported(native_schedule)
                ),
                "unprobed" if native_schedule is None else "live-command-probe",
                {"list": commands, "disposable_invoke": native_schedule},
            ),
            "context_info": _evidence(
                _supported(live.get("context_info")),
                "live-rpc-probe",
                live.get("context_info"),
            ),
            "detached_continuation": _evidence(
                None if sidecar is None else _supported(sidecar),
                "unprobed" if sidecar is None else "live-sidecar-probe",
                sidecar or {"reason": "sidecar probe not requested"},
            ),
            "event_log": _evidence(
                _supported(event_log_read) and _supported(event_log_tail),
                "live-rpc-probe",
                {"read": event_log_read, "tail": event_log_tail},
            ),
            "hook_agent_stop": _evidence(
                None,
                "unprobed",
                "SDK 1.0.8 does not expose a verified AgentStop callback",
            ),
            "hook_user_prompt_transformed": _evidence(
                None,
                "unprobed",
                "SDK 1.0.8 does not expose a verified transformed-prompt callback",
            ),
            "config_reattach": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for same-fence reattach acceptance",
            ),
            "managed_permission_handler": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for permission-handler acceptance",
            ),
            "mcp_http": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for disposable HTTP MCP acceptance",
            ),
            "mcp_stdio": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for disposable stdio MCP acceptance",
            ),
            "model_config": _evidence(
                None,
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
                None if native_schedule is None else _supported(native_schedule),
                "unprobed" if native_schedule is None else "live-command-probe",
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
            "session_lifecycle_wrappers": _evidence(
                lifecycle_supported,
                "unprobed" if lifecycle_supported is None else "live-bridge-wrapper-probe",
                {
                    "create_id_matches": live.get("session_id_matches"),
                    "exists_immediately_after_create": live.get(
                        "session_exists_immediately_after_create"
                    ),
                    "exists_after_activity": live.get("session_exists_after_activity"),
                    "resume_id_matches": live.get("resume_session_id_matches"),
                    "deleted_and_absent": live.get("session_deleted"),
                    "deletion_skipped": lifecycle_deletion_skipped,
                },
            ),
            "protocol_elicitation": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for MCP elicitation acceptance",
            ),
            "protocol_external_tool": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for external-tool acceptance",
            ),
            "protocol_mcp_headers_response": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for MCP header response RPC gate",
            ),
            "protocol_mcp_oauth": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for MCP OAuth acceptance",
            ),
            "protocol_sampling_response": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for sampling response RPC gate",
            ),
            "protocol_session_limits_response": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for session-limit response RPC gate",
            ),
            "reasoning_summary_readback": _evidence(
                None,
                "unprobed",
                "no durable reasoning-summary readback was exercised",
            ),
            "remote": _evidence(
                None,
                "unprobed",
                "remote enable/disable was not exercised",
            ),
            "selected_agent": _evidence(
                _supported(live.get("agents")) and _supported(live.get("agent_current")),
                "live-rpc-probe",
                {"list": live.get("agents"), "current": live.get("agent_current")},
            ),
            "session_extension_config": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for create/resume config acceptance",
            ),
            "session_hooks": _evidence(
                None,
                "unprobed",
                "run sdk-probe --live-extensions for callback hook acceptance",
            ),
            "session_mode": _evidence(
                _supported(live.get("mode_initial")) and _supported(live.get("mode_autopilot")),
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
        command_list_supported = None if "commands" not in live else _supported(commands)
        agent_list_supported = None if "agents" not in live else _supported(live.get("agents"))
        agent_current_supported = (
            None if "agent_current" not in live else _supported(live.get("agent_current"))
        )
        task_list_supported = (
            None
            if "tasks" not in live or "task_list" not in live
            else _supported(live.get("tasks")) and _supported(live.get("task_list"))
        )
        schedule_list_supported = (
            None if "schedule" not in live else _supported(live.get("schedule"))
        )
        remote_status_supported = (
            None if "metadata_snapshot" not in live else _supported(live.get("metadata_snapshot"))
        )
        capabilities.update(
            {
                "commands_list": _evidence(
                    command_list_supported,
                    ("unprobed" if command_list_supported is None else "live-rpc-probe"),
                    commands,
                ),
                "agents_list": _evidence(
                    agent_list_supported,
                    "unprobed" if agent_list_supported is None else "live-rpc-probe",
                    live.get("agents"),
                ),
                "agents_current": _evidence(
                    agent_current_supported,
                    ("unprobed" if agent_current_supported is None else "live-rpc-probe"),
                    live.get("agent_current"),
                ),
                "tasks_list": _evidence(
                    task_list_supported,
                    "unprobed" if task_list_supported is None else "live-rpc-probe",
                    {
                        "refresh": live.get("tasks"),
                        "list": live.get("task_list"),
                    },
                ),
                "schedules_list": _evidence(
                    schedule_list_supported,
                    ("unprobed" if schedule_list_supported is None else "live-rpc-probe"),
                    live.get("schedule"),
                ),
                "remote_status": _evidence(
                    remote_status_supported,
                    ("unprobed" if remote_status_supported is None else "live-rpc-probe"),
                    live.get("metadata_snapshot"),
                ),
            }
        )
        if "model_config_round_trip" in live:
            capabilities["model_config"] = _evidence(
                _supported(live.get("model_config_round_trip")),
                "live-model-mutation-probe",
                live.get("model_config_round_trip"),
            )
        elif "model_current" in live and not _supported(live.get("model_current")):
            capabilities["model_config"] = _evidence(
                None,
                "live-model-readback-unavailable",
                {
                    "reason": (
                        "read-only model.getCurrent failed; mutation support was not exercised"
                    ),
                    "readback": live.get("model_current"),
                },
            )
        if command_list_supported is False:
            for name in (
                "builtin_after",
                "builtin_after_result_completed",
                "builtin_every",
                "builtin_every_result_completed",
                "builtin_research",
                "builtin_research_result_agent_prompt",
                "builtin_review",
                "builtin_review_result_agent_prompt",
                "builtin_rubber_duck",
                "builtin_rubber_duck_result_agent_prompt",
                "builtin_security_review",
                "builtin_security_review_result_agent_prompt",
                "commands_invoke",
                "commands_result_agent_prompt",
                "commands_result_completed",
                "commands_result_select_subcommand",
                "commands_result_text",
            ):
                capabilities[name] = _evidence(
                    False,
                    "live-prerequisite-failed",
                    {"prerequisite": "commands_list"},
                )
        if agent_list_supported is False or agent_current_supported is False:
            for name in ("agents_select", "agents_deselect"):
                capabilities[name] = _evidence(
                    False,
                    "live-prerequisite-failed",
                    {"prerequisite": "agents_list/current"},
                )
        if task_list_supported is False:
            for name in (
                "tasks_cancel",
                "tasks_message",
                "tasks_progress",
                "tasks_promote",
                "tasks_remove",
                "tasks_wait",
            ):
                capabilities[name] = _evidence(
                    False,
                    "live-prerequisite-failed",
                    {"prerequisite": "tasks_list"},
                )
        if schedule_list_supported is False:
            capabilities["schedules_stop"] = _evidence(
                False,
                "live-prerequisite-failed",
                {"prerequisite": "schedules_list"},
            )
        if native_schedule is not None and command_list_supported is not False:
            invocation = (
                native_schedule.detail.get("invocation")
                if isinstance(native_schedule.detail, dict)
                else None
            )
            invocation_kind = invocation.get("kind") if isinstance(invocation, dict) else None
            capabilities["commands_invoke"] = _evidence(
                invocation_kind is not None,
                "live-command-probe",
                {"result_kind": invocation_kind},
            )
            for kind in ("agent-prompt", "completed", "select-subcommand", "text"):
                observed = invocation_kind == kind
                capabilities[f"commands_result_{kind.replace('-', '_')}"] = _evidence(
                    True if observed else None,
                    "live-command-probe" if observed else "unprobed",
                    {"observed_result_kind": invocation_kind},
                )
            capabilities["builtin_after"] = _evidence(
                _supported(native_schedule),
                "live-command-probe",
                native_schedule,
            )
            capabilities["builtin_after_result_completed"] = _evidence(
                invocation_kind == "completed",
                "live-command-probe",
                {"observed_result_kind": invocation_kind},
            )
        for size_mib in (1, 5, 10):
            frame = (live.get("transport_frames") or {}).get(str(size_mib))
            capabilities[f"transport_frame_{size_mib}mib"] = _evidence(
                _supported(frame),
                "live-transport-frame-probe",
                frame,
            )
        checked_capabilities = CapabilityRegistry(self._settings).load_checked().capabilities
        for name in checked_capabilities:
            capabilities.setdefault(
                name,
                _evidence(
                    None,
                    "unprobed",
                    {"reason": "this live probe did not exercise the exact capability"},
                ),
            )
        return {
            "schema_version": CAPABILITY_SCHEMA_VERSION,
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
            "bridge_acceptance_lanes": {
                operation: list(lanes)
                for operation, lanes in sorted(BRIDGE_ACCEPTANCE_LANES.items())
            },
            "capabilities": capabilities,
            "fixture": {
                "path": str(fixture_path),
                "sha256": fixture_sha256,
            },
        }

    async def _probe_mode_round_trip(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
    ) -> CapabilityResult:
        initial: str | None = None
        autopilot: str | None = None
        restored: str | None = None
        operation_error: Exception | None = None
        restore_error: Exception | None = None
        try:
            initial = await bridge.get_mode(session)
            await bridge.set_mode(session, "autopilot")
            autopilot = await bridge.get_mode(session)
        except Exception as error:
            operation_error = error
        finally:
            if initial is not None:
                try:
                    await bridge.set_mode(session, initial)
                    restored = await bridge.get_mode(session)
                except Exception as error:
                    restore_error = error
        if operation_error is not None or restore_error is not None:
            detail: dict[str, Any] = {
                "autopilot": autopilot,
                "restored": restored,
                "prompt_sent": False,
            }
            if operation_error is not None:
                detail["error"] = _error_detail(operation_error)
            if restore_error is not None:
                detail["restore_error"] = _error_detail(restore_error)
            return CapabilityResult(False, detail)
        return CapabilityResult(
            autopilot == "autopilot" and restored == initial,
            {
                "autopilot": autopilot,
                "restored": restored,
                "prompt_sent": False,
            },
        )

    async def _cleanup_live_resources(
        self,
        bridge: CopilotBridge,
        *,
        session_id: str,
        active_sessions: tuple[tuple[str, CopilotSession | None], ...],
        bridge_started: bool,
        keep_session: bool,
        live: dict[str, Any],
    ) -> None:
        cleanup_errors: list[Exception] = []
        for label, active_session in active_sessions:
            if active_session is None:
                continue
            try:
                await bridge.disconnect(active_session)
            except Exception as error:
                cleanup_errors.append(error)
                live[f"{label}_disconnect_error"] = _error_detail(error)
        if bridge_started and keep_session:
            live["session_deletion_skipped"] = True
            live["session_deleted"] = None
        if bridge_started and not keep_session:
            delete_error: Exception | None = None
            try:
                await bridge.delete_session(session_id)
            except Exception as error:
                delete_error = error
                live["delete_error"] = _error_detail(error)
            try:
                session_deleted = not await bridge.session_exists(session_id)
            except Exception as error:
                cleanup_errors.append(error)
                live["delete_reconcile_error"] = _error_detail(error)
                session_deleted = False
            else:
                live["session_absence_confirmed"] = session_deleted
                if not session_deleted:
                    cleanup_errors.append(
                        delete_error
                        or RuntimeError("disposable session still exists after delete response")
                    )
            live["session_deleted"] = session_deleted
        if bridge_started:
            try:
                await bridge.stop()
            except Exception as error:
                cleanup_errors.append(error)
                live["bridge_stop_error"] = _error_detail(error)
        if cleanup_errors:
            live["cleanup_errors"] = [_error_detail(error) for error in cleanup_errors]
            names = ", ".join(type(error).__name__ for error in cleanup_errors)
            raise RuntimeError(f"live probe cleanup failed: {names}")

    async def _probe_abort_round_trip(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: EventRecorder,
        *,
        wait_seconds: float,
    ) -> CapabilityResult:
        recorder.drain()
        event_start = len(recorder.events)
        abort_message_id: str | None = None
        try:
            abort_message_id = await bridge.send(
                session,
                "Use the shell tool to run `sleep 60`; do not reply until it finishes.",
                agent_mode="interactive",
            )
            await recorder.wait_for(
                SessionEventType.ASSISTANT_TURN_START,
                min(wait_seconds, 30),
            )
            await bridge.abort(session)
            await recorder.wait_for(SessionEventType.ABORT, min(wait_seconds, 30))
            idle = await recorder.wait_for(
                SessionEventType.SESSION_IDLE,
                min(wait_seconds, 30),
            )
            recovery_text = "COPILOTD_ABORT_RECOVERY_OK"
            recovery_message_id = await bridge.send(
                session,
                f"Reply with exactly {recovery_text} and do not use tools.",
                agent_mode="interactive",
            )
            await recorder.wait_for(SessionEventType.SESSION_IDLE, wait_seconds)
        except Exception as error:
            detail: dict[str, Any] = _error_detail(error)
            try:
                await bridge.abort(session)
                await recorder.wait_for(
                    SessionEventType.SESSION_IDLE,
                    min(wait_seconds, 30),
                )
            except Exception as cleanup_error:
                detail["cleanup_error"] = _error_detail(cleanup_error)
            return CapabilityResult(False, detail)

        observed = recorder.events[event_start:]
        idle_payload = idle.data.to_dict()
        recovered = _response_matches(observed, recovery_text)
        return CapabilityResult(
            idle_payload.get("aborted") is True and recovered,
            {
                "abort_message_id_present": bool(abort_message_id),
                "idle_aborted": idle_payload.get("aborted"),
                "recovery_message_id_present": bool(recovery_message_id),
                "recovered_after_abort": recovered,
            },
        )

    async def _probe_transport_frames(self, bridge: CopilotBridge) -> dict[str, Any]:
        results: dict[str, Any] = {}
        for size_mib in (1, 5, 10):
            payload = "x" * (size_mib * 1024 * 1024)
            started_at = time.perf_counter()
            try:
                response = await bridge.transport_ping(payload)
            except Exception as error:
                results[str(size_mib)] = CapabilityResult(False, _error_detail(error))
                continue
            response_message = str(response["message"])
            expected_size = len(payload) + len("pong: ")
            results[str(size_mib)] = CapabilityResult(
                len(response_message) == expected_size,
                {
                    "request_bytes": len(payload),
                    "response_bytes": len(response_message),
                    "elapsed_seconds": round(time.perf_counter() - started_at, 3),
                },
            )
        return results

    async def _probe_native_schedule(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: EventRecorder,
        *,
        wait_seconds: float,
    ) -> CapabilityResult:
        recorder.drain()
        event_start = len(recorder.events)
        initial: dict[str, Any] = {"entries": []}
        created: dict[str, Any] = {"entries": []}
        invocation: dict[str, Any] | None = None
        operation_error: Exception | None = None
        stopped: list[dict[str, Any]] = []
        new_entries: list[dict[str, Any]] = []
        try:
            initial = {"entries": await bridge.get_native_schedules(session)}
        except Exception as error:
            return CapabilityResult(False, _error_detail(error))
        try:
            result = await bridge.invoke_command(
                session,
                name="after",
                input_text="30m Reply with exactly COPILOTD_AFTER_OK and do not use tools.",
            )
            invocation = result.to_dict()
            await recorder.wait_for(
                SessionEventType.SESSION_SCHEDULE_CREATED,
                min(wait_seconds, 30),
            )
        except Exception as error:
            operation_error = error

        cleanup_errors: list[Exception] = []
        try:
            created = {"entries": await bridge.get_native_schedules(session)}
            initial_ids = {int(item["id"]) for item in initial["entries"]}
            new_entries = [
                item for item in created["entries"] if int(item["id"]) not in initial_ids
            ]
            for entry in new_entries:
                schedule_id = int(entry["id"])
                try:
                    stop_result = await bridge.stop_native_schedule(
                        session,
                        schedule_id=schedule_id,
                    )
                    if stop_result is None:
                        raise RuntimeError(f"schedule {schedule_id} stop returned no stopped entry")
                    stopped_id = int(stop_result.get("id", stop_result.get("schedule_id", -1)))
                    if stopped_id != schedule_id:
                        raise RuntimeError(
                            f"schedule stop returned id {stopped_id}, expected {schedule_id}"
                        )
                    stopped.append(stop_result)
                except Exception as error:
                    cleanup_errors.append(error)
            remaining = {"entries": await bridge.get_native_schedules(session)}
            remaining_ids = {int(item["id"]) for item in remaining["entries"]}
            leaked_ids = sorted({int(item["id"]) for item in new_entries} & remaining_ids)
            if leaked_ids:
                cleanup_errors.append(
                    RuntimeError(f"runtime schedules remain after cleanup: {leaked_ids}")
                )
        except Exception as error:
            cleanup_errors.append(error)
            remaining = {"error": _error_detail(error)}
        if cleanup_errors:
            names = ", ".join(type(error).__name__ for error in cleanup_errors)
            raise RuntimeError(f"native schedule cleanup failed: {names}")

        observed = recorder.events[event_start:]
        observed_types = [event["type"] for event in observed]
        supported = (
            operation_error is None
            and invocation is not None
            and invocation.get("kind") == "completed"
            and len(new_entries) == 1
            and len(stopped) == 1
        )
        detail: dict[str, Any] = {
            "invocation": invocation,
            "initial": initial,
            "created": created,
            "stopped": stopped,
            "remaining": remaining,
            "observed_event_types": observed_types,
        }
        if operation_error is not None:
            detail["operation_error"] = _error_detail(operation_error)
        return CapabilityResult(
            supported,
            detail,
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
        detail: dict[str, Any] = {"session_id_preallocated": True}
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
            detail["port_assigned"] = port > 0
            detail["runtime_source"] = "bundled"
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
                    permission_handler=_disposable_permission_handler(),
                )
                detail["actual_session_id_matches"] = session.session_id == session_id
                sessions_before_disconnect = set(await bridge.list_sessions())
                detail["sessions_before_disconnect_count"] = len(sessions_before_disconnect)
                detail["target_listed_before_disconnect"] = session_id in sessions_before_disconnect
                await bridge.ensure_allow_all(session)
                invocation = await bridge.invoke_command(
                    session,
                    name="after",
                    input_text=("10s Reply with exactly COPILOTD_SIDECAR_OK and do not use tools."),
                )
                detail["invocation"] = invocation.to_dict()
                await recorder.wait_for(
                    SessionEventType.SESSION_SCHEDULE_CREATED,
                    min(wait_seconds, 30),
                )
                detail["schedule_before_disconnect"] = {
                    "entries": await bridge.get_native_schedules(session)
                }
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
                sessions_after_reconnect = set(await resumed_bridge.list_sessions())
                detail["sessions_after_reconnect_count"] = len(sessions_after_reconnect)
                detail["target_listed_after_reconnect"] = session_id in sessions_after_reconnect
                if not detail["target_listed_after_reconnect"]:
                    raise RuntimeError(
                        "sidecar did not retain the session after the client transport closed"
                    )
                resume_recorder = EventRecorder(asyncio.get_running_loop())
                resumed = await resumed_bridge.resume_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=resume_recorder.callback,
                    continue_pending_work=True,
                    permission_handler=_disposable_permission_handler(),
                )
                history = await resumed_bridge.get_events(resumed)
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
                detail["schedule_after_reconnect"] = {
                    "entries": await resumed_bridge.get_native_schedules(resumed)
                }
                await resumed_bridge.disconnect(resumed)
                resumed = None
                await resumed_bridge.delete_session(session_id)
                await resumed_bridge.stop()
                resumed_bridge = None
        except Exception as error:
            detail["error"] = _error_detail(error)
        finally:
            if resumed is not None and resumed_bridge is not None:
                await resumed_bridge.disconnect(resumed)
            if resumed_bridge is not None:
                try:
                    await resumed_bridge.delete_session(session_id)
                except Exception as error:
                    detail["cleanup_delete_error"] = _error_detail(error)
                await resumed_bridge.stop()
            if session is not None and bridge is not None:
                await bridge.disconnect(session)
            if bridge is not None:
                try:
                    await bridge.delete_session(session_id)
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
            detail["stdout_line_count"] = len(stdout_tail)
            detail["stderr_line_count"] = len(stderr_tail)

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


def _response_matches(
    events: list[dict[str, Any]],
    expected: str,
) -> bool:
    for event in events:
        if event.get("type") != SessionEventType.ASSISTANT_MESSAGE.value:
            continue
        data = event.get("data")
        if isinstance(data, dict) and str(data.get("content", "")).strip() == expected:
            return True
    return False


def _supported(value: Any) -> bool:
    if isinstance(value, CapabilityResult):
        return value.supported
    if isinstance(value, dict):
        return value.get("supported") is True
    return False


def _require_supported_probe(result: CapabilityResult, name: str) -> None:
    if not result.supported:
        raise RuntimeError(f"{name} failed")


def _evidence(
    supported: bool | None,
    evidence_kind: str,
    detail: Any,
) -> dict[str, Any]:
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
