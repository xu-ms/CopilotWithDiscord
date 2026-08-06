from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from copilot.session import CopilotSession
from copilot.session_events import SessionEvent, SessionEventType

from copilotd.config import Settings
from copilotd.sdk.bridge import CopilotBridge, ManagedAwarePermissionHandler
from copilotd.sdk.native import NativeCommandResult, NativeCommandResultKind

REAL_ACCEPTANCE_ENV = "COPILOTD_REAL_ACCEPTANCE"
REAL_ACCEPTANCE_CONFIRMATION = "I_UNDERSTAND_THIS_USES_REAL_COPILOT"
ACCEPTANCE_SCHEMA_VERSION = 2
ACCEPTANCE_SUITES = frozenset(
    {
        "agents",
        "commands",
        "compact",
        "ephemeral",
        "fleet-tasks",
        "model",
        "remote",
        "schedules",
    }
)

_URL = re.compile(r"https?://[^\s\"']+")
_TEMP_PATH = re.compile(r"/(?:private/)?var/folders/[^\s\"']+")
_METHOD_MISSING = re.compile(
    r"(method not found|unknown method|not implemented|unsupported method)",
    re.IGNORECASE,
)


class RealAcceptanceError(RuntimeError):
    pass


class RealAcceptanceOptInError(RealAcceptanceError):
    pass


class RealAcceptanceAuthError(RealAcceptanceError):
    pass


@dataclass(frozen=True, slots=True)
class AcceptanceCapability:
    supported: bool
    executed: bool
    status: str
    evidence_kind: str
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AcceptanceRecorder:
    def __init__(self) -> None:
        self.events: list[SessionEvent] = []
        self.queue: asyncio.Queue[SessionEvent] = asyncio.Queue()

    def callback(self, event: SessionEvent) -> None:
        self.events.append(event)
        self.queue.put_nowait(event)

    def drain(self) -> None:
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()

    async def wait_for(
        self,
        event_type: SessionEventType,
        *,
        timeout_seconds: float,
    ) -> SessionEvent:
        async with asyncio.timeout(timeout_seconds):
            while True:
                event = await self.queue.get()
                self.queue.task_done()
                if event.type == event_type:
                    return event


async def _disposable_owner_is_valid() -> bool:
    return True


class RealNativeAcceptance:
    def __init__(
        self,
        settings: Settings,
        *,
        evidence_path: Path,
        timeout_seconds: float = 180,
        environ: dict[str, str] | None = None,
        suites: set[str] | None = None,
        resume_evidence: tuple[Path, ...] = (),
    ) -> None:
        self._settings = settings
        self._evidence_path = evidence_path.expanduser().resolve()
        self._timeout_seconds = timeout_seconds
        self._environ = os.environ if environ is None else environ
        self._suites = set(ACCEPTANCE_SUITES if suites is None else suites)
        unknown_suites = self._suites.difference(ACCEPTANCE_SUITES)
        if unknown_suites:
            raise ValueError("unknown acceptance suites: " + ", ".join(sorted(unknown_suites)))
        if not self._suites:
            raise ValueError("at least one acceptance suite is required")
        self._resume_evidence = tuple(path.expanduser().resolve() for path in resume_evidence)
        self._capabilities: dict[str, AcceptanceCapability] = {}
        self._observed_command_kinds: set[str] = set()
        self._prior_identity: dict[str, Any] | None = None
        self._resumed_runs: list[dict[str, Any]] = []

    def require_opt_in(self) -> None:
        if self._environ.get(REAL_ACCEPTANCE_ENV) != REAL_ACCEPTANCE_CONFIRMATION:
            raise RealAcceptanceOptInError(
                f"set {REAL_ACCEPTANCE_ENV}={REAL_ACCEPTANCE_CONFIRMATION!r} "
                "and pass --real to run live Copilot mutations"
            )
        if self._timeout_seconds <= 0:
            raise ValueError("acceptance timeout must be positive")

    async def run(self) -> dict[str, Any]:
        self.require_opt_in()
        self._load_resume_evidence()
        started_at = datetime.now(UTC)
        report: dict[str, Any] = {
            "schema_version": ACCEPTANCE_SCHEMA_VERSION,
            "generated_at": started_at.isoformat(),
            "status": "running",
            "environment": {
                "platform": platform.system(),
                "python": platform.python_version(),
            },
            "identity": {},
            "suites": sorted(self._suites),
            "resumed_runs": self._resumed_runs,
            "capabilities": {},
            "cleanup": {},
        }
        error: BaseException | None = None
        try:
            await self._run_disposable(report)
            report["status"] = "passed"
        except BaseException as caught:
            error = caught
            report["status"] = "failed"
            report["error"] = {
                "type": type(caught).__name__,
                "message": _sanitize_string(str(caught)),
            }
        finally:
            report["cleanup"].setdefault("temporary_workspace_removed", True)
            report["capabilities"] = {
                name: capability.to_dict()
                for name, capability in sorted(self._capabilities.items())
            }
            report["duration_seconds"] = round(
                (datetime.now(UTC) - started_at).total_seconds(),
                3,
            )
            sanitized = sanitize_evidence(report)
            self._write_evidence(sanitized)
        if error is not None:
            raise error
        return sanitized

    async def _run_disposable(self, report: dict[str, Any]) -> None:
        bridge = CopilotBridge(self._settings)
        session: CopilotSession | None = None
        session_id = f"copilotd-real-acceptance-{uuid.uuid4()}"
        bridge_started = False
        deleted = False
        with tempfile.TemporaryDirectory(prefix="copilotd-real-acceptance-") as workspace:
            await _git(workspace, "init", "--quiet")
            await _git(workspace, "config", "user.email", "copilotd-acceptance@invalid")
            await _git(workspace, "config", "user.name", "copilotD acceptance")
            await _git(
                workspace,
                "remote",
                "add",
                "origin",
                "https://github.com/octocat/Hello-World.git",
            )
            readme = Path(workspace) / "README.md"
            readme.write_text(
                "# Disposable acceptance repository\n\nInitial state.\n",
                encoding="utf-8",
            )
            await _git(workspace, "add", "README.md")
            await _git(workspace, "commit", "--quiet", "-m", "Initial")
            readme.write_text(
                "# Disposable acceptance repository\n\nReview this harmless change.\n",
                encoding="utf-8",
            )
            recorder = AcceptanceRecorder()
            execution_error: BaseException | None = None
            try:
                await bridge.start()
                bridge_started = True
                identity = await bridge.runtime_identity()
                report["identity"] = {
                    "sdk_version": _package_version(),
                    "runtime_version": identity["runtime_version"],
                    "protocol_version": identity["protocol_version"],
                    "ping_protocol_version": identity["ping_protocol_version"],
                    "authenticated": bool(identity["authenticated"]),
                    "auth_type": identity.get("auth_type"),
                    "auth_host": identity.get("auth_host"),
                }
                if not identity["authenticated"]:
                    raise RealAcceptanceAuthError(
                        "real Copilot acceptance requires an authenticated runtime"
                    )
                self._assert_prior_identity(report["identity"])
                session = await bridge.create_session(
                    session_id=session_id,
                    working_directory=workspace,
                    on_event=recorder.callback,
                    permission_handler=ManagedAwarePermissionHandler(
                        approval_validator=_disposable_owner_is_valid
                    ),
                )
                await bridge.ensure_allow_all(session)
                if "commands" in self._suites:
                    await self._exercise_commands(bridge, session, recorder)
                if "ephemeral" in self._suites:
                    await self._exercise_ephemeral_query(bridge, session, recorder)
                if "compact" in self._suites:
                    await self._exercise_compaction(bridge, session, recorder)
                if "fleet-tasks" in self._suites:
                    await self._exercise_fleet_and_tasks(bridge, session, recorder)
                if "model" in self._suites:
                    await self._exercise_model_config(bridge, session)
                if "schedules" in self._suites:
                    await self._exercise_schedules(bridge, session, recorder)
                if self._observed_command_kinds:
                    self._record_command_result_kinds()
                if "agents" in self._suites:
                    await self._exercise_agents(bridge, session)
                if "remote" in self._suites:
                    await self._exercise_remote(bridge, session)
                self._record_aggregate_capabilities()
            except BaseException as caught:
                execution_error = caught
            finally:
                cleanup_errors: list[Exception] = []
                if session is not None:
                    try:
                        await bridge.disable_remote(session)
                    except Exception as cleanup_error:
                        cleanup_errors.append(cleanup_error)
                        report["cleanup"]["remote_disable_error"] = {
                            "type": type(cleanup_error).__name__
                        }
                    try:
                        entries = await bridge.get_native_schedules(session)
                        for entry in entries:
                            try:
                                stopped = await bridge.stop_native_schedule(
                                    session,
                                    int(entry["id"]),
                                )
                                if stopped is None:
                                    raise RealAcceptanceError(
                                        "schedule cleanup returned no stopped entry"
                                    )
                            except Exception as cleanup_error:
                                cleanup_errors.append(cleanup_error)
                        remaining = await bridge.get_native_schedules(session)
                        if remaining:
                            raise RealAcceptanceError(
                                "runtime schedules remain after acceptance cleanup"
                            )
                    except Exception as cleanup_error:
                        cleanup_errors.append(cleanup_error)
                        report["cleanup"]["schedule_stop_error"] = {
                            "type": type(cleanup_error).__name__
                        }
                    try:
                        await bridge.disconnect(session)
                    except Exception as cleanup_error:
                        cleanup_errors.append(cleanup_error)
                        report["cleanup"]["disconnect_error"] = {
                            "type": type(cleanup_error).__name__
                        }
                    finally:
                        session = None
                if bridge_started:
                    try:
                        await bridge.delete_session(session_id)
                        deleted = True
                    except Exception as cleanup_error:
                        report["cleanup"]["delete_error"] = {"type": type(cleanup_error).__name__}
                        try:
                            deleted = not await bridge.session_exists(session_id)
                            report["cleanup"]["session_absence_confirmed"] = deleted
                        except Exception as reconcile_error:
                            cleanup_errors.extend((cleanup_error, reconcile_error))
                            report["cleanup"]["delete_reconcile_error"] = {
                                "type": type(reconcile_error).__name__
                            }
                        else:
                            if not deleted:
                                cleanup_errors.append(cleanup_error)
                    try:
                        await bridge.stop()
                    except Exception as cleanup_error:
                        cleanup_errors.append(cleanup_error)
                        report["cleanup"]["bridge_stop_error"] = {
                            "type": type(cleanup_error).__name__
                        }
                report["cleanup"]["session_deleted"] = deleted
                if not deleted:
                    cleanup_errors.append(
                        RealAcceptanceError("disposable session deletion was not confirmed")
                    )
                if cleanup_errors:
                    names = ", ".join(type(error).__name__ for error in cleanup_errors)
                    if execution_error is not None:
                        report["execution_error"] = {
                            "type": type(execution_error).__name__,
                            "message": _sanitize_string(str(execution_error)),
                        }
                        raise RealAcceptanceError(
                            "real acceptance failed with "
                            f"{type(execution_error).__name__}; cleanup failed: {names}"
                        ) from execution_error
                    raise RealAcceptanceError(f"real acceptance cleanup failed: {names}")
            if execution_error is not None:
                raise execution_error
        report["cleanup"]["temporary_workspace_removed"] = True

    async def _exercise_commands(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
    ) -> None:
        commands = await bridge.list_commands(session, include_builtins=True)
        builtins = {command.name: command for command in commands if command.kind == "builtin"}
        self._record(
            "commands_list",
            supported=True,
            status="passed",
            detail={
                "include_builtins": True,
                "builtin_count": len(builtins),
                "required_builtins": sorted(
                    name
                    for name in (
                        "after",
                        "every",
                        "research",
                        "review",
                        "rubber-duck",
                        "security-review",
                    )
                    if name in builtins
                ),
            },
        )
        for name in (
            "review",
            "security-review",
            "research",
            "rubber-duck",
        ):
            capability = f"builtin_{name.replace('-', '_')}"
            if name not in builtins:
                self._record(
                    capability,
                    supported=False,
                    status="unregistered",
                    detail={"discovery": "absent from commands.list builtin inventory"},
                )
                self._record(
                    f"{capability}_result_agent_prompt",
                    supported=False,
                    status="builtin-unregistered",
                    detail={},
                )
                continue
            input_text = (
                "Investigate this disposable repository and return a concise result."
                if name == "research"
                else "Use only this disposable repository. Do not modify files."
            )
            try:
                result = await bridge.invoke_command(
                    session,
                    name=name,
                    input_text=input_text,
                )
                final, turn_completed = await self._complete_command_result(
                    bridge,
                    session,
                    recorder,
                    result,
                )
            except Exception as invoke_error:
                if _is_method_missing(invoke_error):
                    self._record(
                        capability,
                        supported=False,
                        status="rpc-unavailable",
                        detail={"error_type": type(invoke_error).__name__},
                    )
                    continue
                raise RealAcceptanceError(
                    f"verified builtin {name} failed real execution: {invoke_error}"
                ) from invoke_error
            expected_variant = final.kind == NativeCommandResultKind.AGENT_PROMPT
            self._record(
                capability,
                supported=expected_variant,
                status=(
                    "passed"
                    if expected_variant and turn_completed
                    else "started-and-aborted-at-timeout"
                    if expected_variant
                    else "unexpected-result-variant"
                ),
                detail={
                    "discovered_kind": builtins[name].kind,
                    "result_kind": final.kind.value,
                    "agent_turn_completed": turn_completed,
                },
            )
            self._record(
                f"{capability}_result_agent_prompt",
                supported=expected_variant,
                status=("observed" if expected_variant else "unexpected-result-variant"),
                detail={"observed_result_kind": final.kind.value},
            )

    def _record_command_result_kinds(self) -> None:
        self._record(
            "commands_invoke",
            supported=bool(self._observed_command_kinds),
            status="passed" if self._observed_command_kinds else "no-result",
            detail={"observed_result_kinds": sorted(self._observed_command_kinds)},
        )
        for kind in NativeCommandResultKind:
            observed = kind.value in self._observed_command_kinds
            self._record(
                f"commands_result_{kind.value.replace('-', '_')}",
                supported=observed,
                status="observed" if observed else "not-observed-in-real-invocations",
                detail={"observed_result_kinds": sorted(self._observed_command_kinds)},
            )

    async def _complete_command_result(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
        result: NativeCommandResult,
    ) -> tuple[NativeCommandResult, bool]:
        self._observed_command_kinds.add(result.kind.value)
        if result.kind == NativeCommandResultKind.AGENT_PROMPT:
            if not result.prompt:
                raise RealAcceptanceError("agent-prompt result omitted its runtime prompt")
            recorder.drain()
            await bridge.send(
                session,
                result.prompt,
                agent_mode=result.mode or "interactive",
            )
            try:
                await recorder.wait_for(
                    SessionEventType.SESSION_IDLE,
                    timeout_seconds=self._timeout_seconds,
                )
            except TimeoutError:
                await bridge.abort(session)
                try:
                    await recorder.wait_for(
                        SessionEventType.SESSION_IDLE,
                        timeout_seconds=min(self._timeout_seconds, 30),
                    )
                except TimeoutError:
                    pass
                return result, False
            return result, True
        if result.kind == NativeCommandResultKind.SELECT_SUBCOMMAND:
            if not result.command or not result.options:
                raise RealAcceptanceError("select-subcommand result omitted command or options")
            selected = await bridge.invoke_command(
                session,
                name=result.command,
                input_text=result.options[0].name,
            )
            return await self._complete_command_result(
                bridge,
                session,
                recorder,
                selected,
            )
        return result, True

    async def _exercise_ephemeral_query(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
    ) -> None:
        history_before = len(await bridge.get_events(session))
        event_start = len(recorder.events)
        try:
            answer = await bridge.ephemeral_query(
                session,
                "Reply with the word TWO for one plus one. Do not use tools.",
            )
        except Exception as query_error:
            if _is_method_missing(query_error):
                self._record(
                    "ephemeral_query",
                    supported=False,
                    status="rpc-unavailable",
                    detail={"error_type": type(query_error).__name__},
                )
                return
            raise
        history_after = len(await bridge.get_events(session))
        observed = recorder.events[event_start:]
        tool_events = [
            event.raw_type or event.type.value
            for event in observed
            if (event.raw_type or event.type.value).startswith("tool.")
        ]
        if history_after != history_before or tool_events:
            raise RealAcceptanceError("ephemeral query changed history or emitted tool execution")
        self._record(
            "ephemeral_query",
            supported=True,
            status="passed",
            detail={
                "answer_present": bool(answer),
                "history_count_before": history_before,
                "history_count_after": history_after,
                "tool_event_count": len(tool_events),
            },
        )

    async def _exercise_compaction(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
    ) -> None:
        recorder.drain()
        await bridge.send(
            session,
            "Remember the marker COPILOTD_ACCEPTANCE_MARKER and reply briefly.",
            agent_mode="interactive",
        )
        await recorder.wait_for(
            SessionEventType.SESSION_IDLE,
            timeout_seconds=self._timeout_seconds,
        )
        try:
            result = await bridge.compact_history(
                session,
                focus="Retain the acceptance marker.",
                timeout_seconds=self._timeout_seconds,
            )
        except Exception as compact_error:
            if _is_method_missing(compact_error):
                self._record(
                    "history_compact",
                    supported=False,
                    status="rpc-unavailable",
                    detail={"error_type": type(compact_error).__name__},
                )
                return
            raise
        if not result.get("success"):
            raise RealAcceptanceError("history.compact returned success=false")
        batch = await bridge.read_event_log(session, cursor=None)
        self._record(
            "history_compact",
            supported=True,
            status="passed",
            detail={
                "messages_removed": int(result.get("messagesRemoved", 0)),
                "tokens_removed": int(result.get("tokensRemoved", 0)),
                "durable_backfill_event_count": len(batch.events),
                "cursor_status": batch.cursor_status,
            },
        )

    async def _exercise_fleet_and_tasks(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
    ) -> None:
        recorder.drain()
        promotion_result: tuple[bool, str, dict[str, Any]] = (
            False,
            "fleet-not-started",
            {"promotable_observed": False, "promoted": False},
        )
        try:
            started = await bridge.start_fleet(
                session,
                "Analyze README.md with two independent workers. Do not modify files.",
                timeout_seconds=self._timeout_seconds,
            )
        except TimeoutError:
            self._record(
                "fleet_start",
                supported=False,
                status="transport-outcome-unknown",
                detail={"deadline_seconds": self._timeout_seconds},
            )
        except Exception as fleet_error:
            if _is_method_missing(fleet_error):
                self._record(
                    "fleet_start",
                    supported=False,
                    status="rpc-unavailable",
                    detail={"error_type": type(fleet_error).__name__},
                )
            else:
                raise
        else:
            if not started:
                raise RealAcceptanceError("fleet.start returned started=false")
            promotion_result = await self._probe_task_promotion(
                bridge,
                session,
                wait_seconds=min(self._timeout_seconds, 30),
            )
            promotion_result[2]["fixture"] = "fleet-sync-wait"
            await recorder.wait_for(
                SessionEventType.SESSION_IDLE,
                timeout_seconds=self._timeout_seconds,
            )
            self._record(
                "fleet_start",
                supported=True,
                status="passed",
                detail={"started": True},
            )

        if not promotion_result[0] and not promotion_result[1].startswith("error:"):
            recorder.drain()
            try:
                await bridge.send(
                    session,
                    (
                        "Use the task tool exactly once with agent_type `general-purpose`. "
                        "Keep the task call synchronous: do not run it in the background. "
                        "Tell the child agent to run `sleep 30`, inspect README.md, and return "
                        "one sentence. Wait for the child, then reply with exactly `done`. "
                        "Do not inspect the file or run the shell command yourself."
                    ),
                    mode="enqueue",
                    agent_mode="interactive",
                )
                promotion_result = await self._probe_task_promotion(
                    bridge,
                    session,
                    wait_seconds=min(self._timeout_seconds, 30),
                )
                promotion_result[2]["fixture"] = "foreground-agent-sync-wait"
                await recorder.wait_for(
                    SessionEventType.SESSION_IDLE,
                    timeout_seconds=self._timeout_seconds,
                )
            except Exception as promotion_error:
                promotion_result = (
                    False,
                    f"error:{type(promotion_error).__name__}",
                    {
                        "fixture": "foreground-agent-sync-wait",
                        "promotable_observed": False,
                        "promoted": False,
                    },
                )
        await bridge.refresh_tasks(session)
        tasks = await bridge.list_tasks(session)
        self._record(
            "tasks_list",
            supported=True,
            status="passed",
            detail={"task_count_after_fleet": len(tasks)},
        )
        active = next(
            (task for task in tasks if str(task.get("status", "")).lower() in {"running", "idle"}),
            None,
        )
        task_id = None if active is None else str(active["id"])
        if task_id is None:
            try:
                task_id = await bridge.start_agent_task(
                    session,
                    agent_type="general-purpose",
                    name="acceptance-worker",
                    description="Disposable native task acceptance worker",
                    prompt=(
                        "Run `sleep 20`, then inspect README.md and report one sentence. "
                        "Do not modify files."
                    ),
                )
            except Exception as setup_error:
                self._record_task_gates(
                    status="setup-unavailable",
                    detail={"error_type": type(setup_error).__name__},
                )
                await self._exercise_task_wait(bridge, session)
                return
        await bridge.refresh_tasks(session)
        tasks = await bridge.list_tasks(session)
        if not any(str(task.get("id")) == task_id for task in tasks):
            raise RealAcceptanceError("tasks.startAgent ID was absent from tasks.list")
        try:
            progress = await bridge.get_task_progress(session, task_id)
        except Exception as progress_error:
            progress = None
            progress_status = f"error:{type(progress_error).__name__}"
        else:
            progress_status = "passed" if progress is not None else "no-progress"
        self._record(
            "tasks_progress",
            supported=progress is not None,
            status=progress_status,
            detail={"typed_progress": progress is not None},
        )
        try:
            message = await bridge.send_task_message(
                session,
                task_id,
                "Please keep the final report concise.",
            )
        except Exception as message_error:
            message = {"sent": False}
            message_status = f"error:{type(message_error).__name__}"
        else:
            message_status = "passed" if message.get("sent") else "rejected"
        self._record(
            "tasks_message",
            supported=bool(message.get("sent")),
            status=message_status,
            detail={"sent": bool(message.get("sent"))},
        )
        promoted, promote_status, promote_detail = promotion_result
        self._record(
            "tasks_promote",
            supported=promoted,
            status=promote_status,
            detail=promote_detail,
        )
        try:
            cancelled = await bridge.cancel_task(session, task_id)
        except Exception as cancel_error:
            cancelled = False
            cancel_status = f"error:{type(cancel_error).__name__}"
        else:
            cancel_status = "passed" if cancelled else "rejected"
        self._record(
            "tasks_cancel",
            supported=cancelled,
            status=cancel_status,
            detail={"cancelled": cancelled},
        )
        await self._exercise_task_wait(bridge, session)
        await bridge.refresh_tasks(session)
        tasks = await bridge.list_tasks(session)
        terminal = next(
            (task for task in tasks if str(task.get("id")) == task_id),
            None,
        )
        try:
            removed = False if terminal is None else await bridge.remove_task(session, task_id)
        except Exception as remove_error:
            removed = False
            remove_status = f"error:{type(remove_error).__name__}"
        else:
            remove_status = "passed" if removed else "task-already-absent-or-not-removable"
        self._record(
            "tasks_remove",
            supported=removed,
            status=remove_status,
            detail={
                "terminal_observed": terminal is not None,
                "removed": removed,
            },
        )

    async def _exercise_task_wait(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
    ) -> None:
        try:
            await bridge.wait_for_tasks(
                session,
                wait_seconds=min(self._timeout_seconds, 60),
            )
        except TimeoutError:
            self._record(
                "tasks_wait",
                supported=False,
                status="transport-outcome-unknown",
                detail={"deadline_seconds": min(self._timeout_seconds, 60)},
            )
            return
        except Exception as wait_error:
            if _is_method_missing(wait_error):
                self._record(
                    "tasks_wait",
                    supported=False,
                    status="rpc-unavailable",
                    detail={"error_type": type(wait_error).__name__},
                )
                return
            raise
        self._record(
            "tasks_wait",
            supported=True,
            status="passed",
            detail={"wait_completed": True},
        )

    async def _probe_task_promotion(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        *,
        wait_seconds: float,
    ) -> tuple[bool, str, dict[str, Any]]:
        deadline = asyncio.get_running_loop().time() + wait_seconds
        promotable_observed = False
        attempts = 0
        while True:
            attempts += 1
            try:
                promotable = await bridge.get_current_promotable_task(session)
                if promotable is not None:
                    promotable_observed = True
                    task_id = str(promotable.get("id", ""))
                    if task_id and await bridge.promote_task(session, task_id):
                        return (
                            True,
                            "passed",
                            {
                                "promotable_observed": True,
                                "promoted": True,
                                "poll_attempts": attempts,
                            },
                        )
            except Exception as error:
                return (
                    False,
                    f"error:{type(error).__name__}",
                    {
                        "promotable_observed": promotable_observed,
                        "promoted": False,
                        "poll_attempts": attempts,
                    },
                )
            if asyncio.get_running_loop().time() >= deadline:
                return (
                    False,
                    "gated-not-promotable",
                    {
                        "promotable_observed": promotable_observed,
                        "promoted": False,
                        "poll_attempts": attempts,
                    },
                )
            await asyncio.sleep(0.2)

    def _record_task_gates(self, *, status: str, detail: dict[str, Any]) -> None:
        for name in (
            "tasks_cancel",
            "tasks_message",
            "tasks_progress",
            "tasks_promote",
            "tasks_remove",
        ):
            self._record(
                name,
                supported=False,
                status=status,
                detail=detail,
            )

    async def _exercise_schedules(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
        recorder: AcceptanceRecorder,
    ) -> None:
        before = await bridge.get_native_schedules(session)
        self._record(
            "schedules_list",
            supported=True,
            status="passed",
            detail={"initial_count": len(before)},
        )
        created_entries = 0
        stopped_entries = 0
        for name, expression in (("after", "30m"), ("every", "30m")):
            recorder.drain()
            try:
                result = await bridge.invoke_command(
                    session,
                    name=name,
                    input_text=(f"{expression} Reply with COPILOTD_{name.upper()}_ACCEPTANCE"),
                )
            except Exception as schedule_error:
                self._record(
                    f"builtin_{name}",
                    supported=False,
                    status=(
                        "rpc-unavailable"
                        if _is_method_missing(schedule_error)
                        else "invocation-failed"
                    ),
                    detail={"error_type": type(schedule_error).__name__},
                )
                continue
            self._observed_command_kinds.add(result.kind.value)
            before_ids = {int(item["id"]) for item in before}
            created: list[dict[str, Any]] = []
            deadline = asyncio.get_running_loop().time() + 15
            while not created:
                after = await bridge.get_native_schedules(session)
                created = [item for item in after if int(item["id"]) not in before_ids]
                if created or asyncio.get_running_loop().time() >= deadline:
                    break
                await asyncio.sleep(0.25)
            created_entries += len(created)
            direct = result.kind == NativeCommandResultKind.COMPLETED and len(created) == 1
            self._record(
                f"builtin_{name}_result_completed",
                supported=result.kind == NativeCommandResultKind.COMPLETED,
                status=(
                    "observed"
                    if result.kind == NativeCommandResultKind.COMPLETED
                    else "unexpected-result-variant"
                ),
                detail={"observed_result_kind": result.kind.value},
            )
            stopped = None
            if len(created) == 1:
                try:
                    stopped = await bridge.stop_native_schedule(
                        session,
                        int(created[0]["id"]),
                    )
                except Exception as stop_error:
                    self._record(
                        f"builtin_{name}",
                        supported=False,
                        status="stop-failed",
                        detail={"error_type": type(stop_error).__name__},
                    )
                    continue
                if stopped is not None:
                    stopped_entries += 1
            if not direct:
                self._record(
                    f"builtin_{name}",
                    supported=False,
                    status="direct-create-gate-failed",
                    detail={
                        "result_kind": result.kind.value,
                        "created_entry_count": len(created),
                        "stopped": stopped is not None,
                    },
                )
                before = await bridge.get_native_schedules(session)
                continue
            if stopped is None:
                raise RealAcceptanceError(f"schedule.stop did not return the {name} entry")
            self._record(
                f"builtin_{name}",
                supported=True,
                status="passed",
                detail={
                    "result_kind": result.kind.value,
                    "created_entry": True,
                    "stopped": True,
                    "manage_schedule_enabled": False,
                },
            )
            before = await bridge.get_native_schedules(session)
        stops_verified = created_entries == 2 and stopped_entries == 2
        self._record(
            "schedules_stop",
            supported=stops_verified,
            status="passed" if stops_verified else "exact-stop-gate-failed",
            detail={"verified_via": ["after", "every"]},
        )

    async def _exercise_agents(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
    ) -> None:
        agents = await bridge.list_agents(session)
        current = await bridge.get_current_agent_info(session)
        self._record(
            "agents_list",
            supported=True,
            status="passed",
            detail={"agent_count": len(agents)},
        )
        self._record(
            "agents_current",
            supported=True,
            status="passed",
            detail={"has_selected_agent": current is not None},
        )
        candidate = next(
            (
                agent
                for agent in agents
                if agent.get("userInvocable") is not False and agent.get("name")
            ),
            None,
        )
        if candidate is None:
            self._record(
                "agents_select",
                supported=False,
                status="no-user-invocable-agent",
                detail={"agent_count": len(agents)},
            )
            self._record(
                "agents_deselect",
                supported=False,
                status="select-prerequisite-unavailable",
                detail={},
            )
            return
        try:
            selected = await bridge.select_agent(session, str(candidate["name"]))
            observed = await bridge.get_current_agent(session)
        except Exception as select_error:
            self._record(
                "agents_select",
                supported=False,
                status="selection-failed",
                detail={"error_type": type(select_error).__name__},
            )
            self._record(
                "agents_deselect",
                supported=False,
                status="select-prerequisite-failed",
                detail={},
            )
            return
        if observed != str(candidate["name"]):
            self._record(
                "agents_select",
                supported=False,
                status="get-current-mismatch",
                detail={},
            )
            return
        self._record(
            "agents_select",
            supported=True,
            status="passed",
            detail={"selected_id_matches": selected.get("id") == candidate.get("id")},
        )
        try:
            await bridge.deselect_agent(session)
            deselected = await bridge.get_current_agent(session) == "default"
        except Exception as deselect_error:
            self._record(
                "agents_deselect",
                supported=False,
                status="deselect-failed",
                detail={"error_type": type(deselect_error).__name__},
            )
            return
        self._record(
            "agents_deselect",
            supported=deselected,
            status="passed" if deselected else "get-current-mismatch",
            detail={"restored_default": deselected},
        )

    async def _exercise_remote(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
    ) -> None:
        auth = await bridge.get_session_auth(session)
        snapshot = await bridge.get_remote_state(session)
        self._record(
            "remote_status",
            supported=bool(auth.get("isAuthenticated")),
            status="passed",
            detail={
                "authenticated": bool(auth.get("isAuthenticated")),
                "snapshot_typed": isinstance(snapshot.get("metadata"), dict),
                "repository_gate": "github-origin",
            },
        )
        try:
            enabled = await bridge.enable_remote(session, "on")
        except Exception as remote_error:
            self._record(
                "remote_enable",
                supported=False,
                status="on-failed",
                detail={"error_type": type(remote_error).__name__},
            )
            self._record(
                "remote_disable",
                supported=False,
                status="on-prerequisite-failed",
                detail={},
            )
            self._record(
                "remote_export_detach_safe",
                supported=False,
                status="export-not-executed",
                detail={},
            )
            return
        if not enabled.get("remoteSteerable"):
            try:
                await bridge.disable_remote(session)
            except Exception as disable_error:
                self._record(
                    "remote_disable",
                    supported=False,
                    status="off-failed",
                    detail={"error_type": type(disable_error).__name__},
                )
                self._record(
                    "remote_export_detach_safe",
                    supported=False,
                    status="export-not-confirmed",
                    detail={},
                )
            else:
                self._record(
                    "remote_disable",
                    supported=True,
                    status="passed",
                    detail={"off_after_on": True},
                )
                try:
                    exported = await bridge.enable_remote(session, "export")
                    await bridge.disable_remote(session)
                except Exception as export_error:
                    self._record(
                        "remote_export_detach_safe",
                        supported=False,
                        status="export-failed",
                        detail={"error_type": type(export_error).__name__},
                    )
                else:
                    self._record(
                        "remote_export_detach_safe",
                        supported=False,
                        status="detach-reconnect-unprobed",
                        detail={
                            "export_steerable": bool(exported.get("remoteSteerable")),
                            "detach_exercised": False,
                            "reconnect_exercised": False,
                            "continued_execution_verified": False,
                        },
                    )
            self._record(
                "remote_enable",
                supported=False,
                status="on-steerability-gate-failed",
                detail={"on_steerable": False},
            )
            return
        try:
            await bridge.disable_remote(session)
            exported = await bridge.enable_remote(session, "export")
            await bridge.disable_remote(session)
        except Exception as remote_error:
            self._record(
                "remote_enable",
                supported=False,
                status="export-or-off-failed",
                detail={"error_type": type(remote_error).__name__},
            )
            self._record(
                "remote_disable",
                supported=False,
                status="transition-failed",
                detail={"error_type": type(remote_error).__name__},
            )
            self._record(
                "remote_export_detach_safe",
                supported=False,
                status="export-not-confirmed",
                detail={},
            )
            return
        if exported.get("remoteSteerable"):
            raise RealAcceptanceError("remote export unexpectedly became steerable")
        self._record(
            "remote_enable",
            supported=True,
            status="passed",
            detail={"on": True, "export": True},
        )
        self._record(
            "remote_disable",
            supported=True,
            status="passed",
            detail={"off_after_on": True, "off_after_export": True},
        )
        self._record(
            "remote_export_detach_safe",
            supported=False,
            status="detach-reconnect-unprobed",
            detail={
                "export_steerable": False,
                "detach_exercised": False,
                "reconnect_exercised": False,
                "continued_execution_verified": False,
            },
        )

    async def _exercise_model_config(
        self,
        bridge: CopilotBridge,
        session: CopilotSession,
    ) -> None:
        models = await bridge.list_models()
        current = await bridge.get_current_model(session)
        model_ids = {str(model.get("id") or "") for model in models}
        explicit_original = current.get("modelId")
        original_model = str(explicit_original or ("auto" if "auto" in model_ids else ""))
        self._record(
            "models",
            supported=bool(models),
            status="passed" if models else "empty-model-list",
            detail={"model_count": len(models)},
        )
        candidate = next(
            (
                model
                for model in models
                if str(model.get("id") or "") != original_model
                and (model.get("policy") or {}).get("state") != "disabled"
                and model.get("supportedReasoningEfforts")
            ),
            None,
        )
        if not original_model or candidate is None:
            self._record(
                "model_config",
                supported=False,
                status="round-trip-prerequisite-unavailable",
                detail={
                    "original_model_present": bool(original_model),
                    "alternate_model_present": candidate is not None,
                    "implicit_auto_original": (
                        explicit_original is None and original_model == "auto"
                    ),
                },
            )
            return
        candidate_id = str(candidate["id"])
        supported_efforts = [
            str(effort) for effort in candidate.get("supportedReasoningEfforts") or []
        ]
        target_effort = next(
            (
                effort
                for effort in supported_efforts
                if effort not in {"none", current.get("reasoningEffort")}
            ),
            None,
        )
        limits = (candidate.get("capabilities") or {}).get("limits") or {}
        target_context_tier = (
            "long_context"
            if int(limits.get("max_context_window_tokens") or 0) >= 1_000_000
            else "default"
        )
        if target_effort is None:
            self._record(
                "model_config",
                supported=False,
                status="option-mutation-prerequisite-unavailable",
                detail={"supported_reasoning_efforts": supported_efforts},
            )
            return
        restored = False
        options_restored = False
        options_changed = False
        changed: dict[str, Any] = {}
        restored_state: dict[str, Any] = {}
        restore_via_auto = False
        try:
            await bridge.set_model(
                session,
                model=candidate_id,
                reasoning_effort=target_effort,
                context_tier=target_context_tier,
            )
            changed = await bridge.get_current_model(session)
            options_changed = (
                changed.get("reasoningEffort") == target_effort
                and changed.get("contextTier") == target_context_tier
            )
            changed_confirmed = changed.get("modelId") == candidate_id and options_changed
        except Exception as error:
            changed_confirmed = False
            change_error = type(error).__name__
        else:
            change_error = None
        finally:
            try:
                optional_field_needs_clear = (
                    current.get("reasoningEffort") is None
                    and changed.get("reasoningEffort") is not None
                ) or (current.get("contextTier") is None and changed.get("contextTier") is not None)
                if optional_field_needs_clear and original_model != "auto" and "auto" in model_ids:
                    await bridge.set_model(session, model="auto")
                    restore_via_auto = True
                await bridge.set_model(
                    session,
                    model=original_model,
                    reasoning_effort=current.get("reasoningEffort"),
                    context_tier=current.get("contextTier"),
                )
                restored_state = await bridge.get_current_model(session)
                restored_model = restored_state.get("modelId")
                restored = restored_model == original_model or (
                    explicit_original is None
                    and original_model == "auto"
                    and restored_model is None
                )
                options_restored = restored_state.get("reasoningEffort") == current.get(
                    "reasoningEffort"
                ) and restored_state.get("contextTier") == current.get("contextTier")
            except Exception as restore_error:
                raise RealAcceptanceError(
                    "model acceptance could not restore the original model: "
                    f"{type(restore_error).__name__}"
                ) from restore_error
        supported = changed_confirmed and restored and options_restored
        self._record(
            "model_config",
            supported=supported,
            status="passed" if supported else "set-readback-restore-failed",
            detail={
                "changed_confirmed": changed_confirmed,
                "options_changed": options_changed,
                "restored": restored,
                "options_restored": options_restored,
                "change_error_type": change_error,
                "original_config": _model_config_detail(current),
                "changed_config": _model_config_detail(changed),
                "restored_config": _model_config_detail(restored_state),
                "restore_via_auto": restore_via_auto,
            },
        )

    def _record(
        self,
        name: str,
        *,
        supported: bool,
        status: str,
        detail: dict[str, Any],
    ) -> None:
        self._capabilities[name] = AcceptanceCapability(
            supported=supported,
            executed=status not in {"unregistered", "rpc-unavailable"},
            status=status,
            evidence_kind="real-disposable-runtime",
            detail=detail,
        )

    def _record_aggregate_capabilities(self) -> None:
        groups = {
            "builtin_commands": ("commands_list", "commands_invoke"),
            "native_schedule": (
                "builtin_after",
                "builtin_every",
                "schedules_list",
                "schedules_stop",
            ),
            "remote": ("remote_disable", "remote_enable", "remote_status"),
            "selected_agent": ("agents_current", "agents_list"),
            "task_snapshot": ("tasks_list",),
        }
        for name, members in groups.items():
            if not all(member in self._capabilities for member in members):
                continue
            supported = all(
                self._capabilities.get(
                    member,
                    AcceptanceCapability(False, False, "missing", "", {}),
                ).supported
                for member in members
            )
            self._record(
                name,
                supported=supported,
                status="passed" if supported else "exact-capability-gate-failed",
                detail={"members": list(members)},
            )

    def _load_resume_evidence(self) -> None:
        for path in self._resume_evidence:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if int(payload.get("schema_version", 0)) != ACCEPTANCE_SCHEMA_VERSION:
                raise RealAcceptanceError(f"resume evidence has unsupported schema: {path.name}")
            cleanup = payload.get("cleanup")
            if payload.get("status") != "passed" or not isinstance(cleanup, dict):
                raise RealAcceptanceError(
                    f"resume evidence did not pass with confirmed cleanup: {path.name}"
                )
            if (
                cleanup.get("session_deleted") is not True
                or cleanup.get("temporary_workspace_removed") is not True
                or any(str(key).endswith("_error") for key in cleanup)
            ):
                raise RealAcceptanceError(f"resume evidence cleanup is incomplete: {path.name}")
            identity = payload.get("identity")
            if not isinstance(identity, dict):
                raise RealAcceptanceError(f"resume evidence has no runtime identity: {path.name}")
            if self._prior_identity is None:
                self._prior_identity = identity
            else:
                self._assert_identity_match(self._prior_identity, identity)
            capabilities = payload.get("capabilities")
            if not isinstance(capabilities, dict):
                raise RealAcceptanceError(f"resume evidence has no capability map: {path.name}")
            for name, raw in capabilities.items():
                if not isinstance(raw, dict):
                    continue
                evidence_kind = str(raw.get("evidence_kind", ""))
                if "real-disposable-runtime" not in evidence_kind:
                    continue
                capability = AcceptanceCapability(
                    supported=bool(raw.get("supported")),
                    executed=bool(raw.get("executed")),
                    status=str(raw.get("status", "unknown")),
                    evidence_kind=f"resumed:{evidence_kind}",
                    detail=(dict(raw["detail"]) if isinstance(raw.get("detail"), dict) else {}),
                )
                self._capabilities[str(name)] = capability
                if name == "commands_invoke":
                    kinds = capability.detail.get("observed_result_kinds", [])
                    self._observed_command_kinds.update(str(kind) for kind in kinds)
                elif str(name).startswith("builtin_"):
                    result_kind = capability.detail.get("result_kind")
                    if result_kind is not None:
                        self._observed_command_kinds.add(str(result_kind))
            self._resumed_runs.append(
                {
                    "sha256": _sha256(path),
                    "source_status": str(payload.get("status", "unknown")),
                }
            )

    def _assert_prior_identity(self, identity: dict[str, Any]) -> None:
        if self._prior_identity is not None:
            self._assert_identity_match(self._prior_identity, identity)

    @staticmethod
    def _assert_identity_match(
        expected: dict[str, Any],
        actual: dict[str, Any],
    ) -> None:
        fields = (
            "sdk_version",
            "runtime_version",
            "protocol_version",
            "ping_protocol_version",
        )
        mismatch = [field for field in fields if expected.get(field) != actual.get(field)]
        if mismatch:
            raise RealAcceptanceError(
                "resume evidence runtime identity mismatch: " + ", ".join(mismatch)
            )

    def _write_evidence(self, report: dict[str, Any]) -> None:
        self._evidence_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._evidence_path.with_suffix(
            f"{self._evidence_path.suffix}.{uuid.uuid4().hex}.tmp"
        )
        temporary.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self._evidence_path)


def _model_config_detail(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "modelId": config.get("modelId"),
        "reasoningEffort": config.get("reasoningEffort"),
        "contextTier": config.get("contextTier"),
    }


def sanitize_evidence(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if isinstance(value, dict):
        return {
            str(item_key): sanitize_evidence(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_evidence(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [sanitize_evidence(item, key=key) for item in value]
    if isinstance(value, str):
        if any(
            marker in lowered
            for marker in (
                "answer",
                "content",
                "connection_token",
                "login",
                "path",
                "prompt",
                "session_id",
                "url",
            )
        ):
            return f"sha256:{hashlib.sha256(value.encode()).hexdigest()[:16]}"
        return _sanitize_string(value)
    return value


def _sanitize_string(value: str) -> str:
    sanitized = _URL.sub("<redacted-url>", value)
    return _TEMP_PATH.sub("<redacted-temp-path>", sanitized)


def _is_method_missing(error: Exception) -> bool:
    return bool(_METHOD_MISSING.search(str(error)))


async def _git(cwd: str, *arguments: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "git",
        *arguments,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RealAcceptanceError(
            f"git {' '.join(arguments)} failed: "
            f"{_sanitize_string(stderr.decode(errors='replace').strip())}"
        )


def _package_version() -> str:
    from importlib.metadata import version

    return version("github-copilot-sdk")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
