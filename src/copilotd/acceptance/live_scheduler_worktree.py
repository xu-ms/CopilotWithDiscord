from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import re
import signal
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from copilotd.config import Settings
from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.lifecycle_commands import SchedulerCommandService
from copilotd.core.projects import ProjectRegistry
from copilotd.core.recovery import StartupRecoveryInventory
from copilotd.core.scheduler import (
    DeterministicSchedulerAdapter,
    SchedulerRepository,
    ScheduleRun,
    ScheduleRunState,
    SchedulerWorker,
)
from copilotd.core.scheduler_adapter import ApplicationSchedulerAdapter
from copilotd.core.session_runtime import RuntimeState, SessionRuntime
from copilotd.core.sessions import (
    CreationIntentRepository,
    SessionCreationService,
    SessionRegistry,
    ThreadGateway,
    ThreadReference,
)
from copilotd.core.worktrees import (
    HistoryForkAdapter,
    SessionCreationWorktreeAdapter,
    WorktreeCapabilityError,
    WorktreeHistoryMode,
    WorktreeManager,
)
from copilotd.sdk.bridge import CopilotBridge
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore

_FEATURES = (
    "scheduled_message",
    "resume",
    "crash_recovery",
    "scheduled_new_session",
    "blank_worktree",
    "history_fork_gate",
)
_SECRET_KEY = re.compile(r"(token|secret|authorization|password|credential)", re.I)
_LONG_SECRET = re.compile(r"\b[A-Za-z0-9_./+=-]{32,}\b")


class LiveAcceptanceError(RuntimeError):
    pass


class LiveAuthenticationError(LiveAcceptanceError):
    pass


@dataclass(frozen=True, slots=True)
class FeatureResult:
    schema_version: int
    namespace: str
    feature: str
    outcome: str
    started_at: str
    finished_at: str
    detail: dict[str, Any]


class ResultArchive:
    def __init__(self, root: Path, namespace: str) -> None:
        self.root = root.expanduser().resolve() / namespace
        self.namespace = namespace
        self.root.mkdir(parents=True, exist_ok=False)
        self._results: list[FeatureResult] = []

    def record(
        self,
        feature: str,
        *,
        outcome: str,
        started_at: datetime,
        detail: dict[str, Any],
    ) -> FeatureResult:
        result = FeatureResult(
            schema_version=1,
            namespace=self.namespace,
            feature=feature,
            outcome=outcome,
            started_at=started_at.isoformat(),
            finished_at=datetime.now(UTC).isoformat(),
            detail=_sanitize(detail),
        )
        self._results.append(result)
        _atomic_json_write(self.root / f"{feature}.json", asdict(result))
        return result

    def finalize(self) -> dict[str, Any]:
        summary = {
            "schema_version": 1,
            "namespace": self.namespace,
            "generated_at": datetime.now(UTC).isoformat(),
            "outcome": (
                "passed"
                if self._results
                and all(result.outcome in {"passed", "gated"} for result in self._results)
                else "failed"
            ),
            "features": [
                {
                    "feature": result.feature,
                    "outcome": result.outcome,
                    "result_file": f"{result.feature}.json",
                }
                for result in self._results
            ],
        }
        _atomic_json_write(self.root / "summary.json", summary)
        return summary


class DisposableThreadGateway:
    """Deterministic gateway used when a real Discord gateway is not injected."""

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self._threads: dict[tuple[str, str], ThreadReference] = {}
        self.create_calls = 0

    async def find_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        creation_token: str,
    ) -> ThreadReference | None:
        del creation_token
        return self._threads.get((channel_id, source_id))

    async def create_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        name: str,
        creation_token: str,
    ) -> ThreadReference:
        del name
        self.create_calls += 1
        reference = ThreadReference(
            thread_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd-live:{self.namespace}:{channel_id}:{source_id}:{creation_token}",
                )
            )
        )
        self._threads[(channel_id, source_id)] = reference
        return reference


class LiveSchedulerWorktreeHarness:
    def __init__(
        self,
        *,
        output_dir: Path,
        timeout_seconds: float = 180,
        namespace: str | None = None,
        thread_gateway: ThreadGateway | None = None,
        fork_adapter: HistoryForkAdapter | None = None,
        features: tuple[str, ...] = _FEATURES,
    ) -> None:
        self.namespace = namespace or f"live-{uuid.uuid4().hex[:12]}"
        self.archive = ResultArchive(output_dir, self.namespace)
        self.timeout_seconds = timeout_seconds
        self.thread_gateway = thread_gateway or DisposableThreadGateway(self.namespace)
        self.fork_adapter = fork_adapter
        unknown = sorted(set(features) - set(_FEATURES))
        if unknown:
            raise ValueError("unknown live acceptance features: " + ", ".join(unknown))
        if not features:
            raise ValueError("at least one non-auth live acceptance feature is required")
        self.features = features
        self._schedule_ids: list[str] = []

    async def run(self) -> dict[str, Any]:
        failures: list[str] = []
        with tempfile.TemporaryDirectory(prefix=f"copilotd-{self.namespace}-") as temporary:
            root = Path(temporary)
            repo = root / "repo"
            await _init_git_repo(repo)
            settings = Settings(
                data_dir=root / "data",
                cache_dir=root / "cache",
                log_dir=root / "logs",
                resolved_home=repo,
            )
            settings.ensure_directories()
            database = Database(settings.database_path)
            bridge = CopilotBridge(settings)
            sessions: SessionRegistry | None = None
            worker: SchedulerWorker | None = None
            try:
                await database.open()
                await self._authenticate(bridge)
                (
                    projects,
                    bindings,
                    sessions,
                    creation,
                    repository,
                    worker,
                    commands,
                ) = await self._build_app(settings, database, bridge, repo)
                project = await projects.resolve("live-channel")
                base_runtime = await creation.create_from_source(
                    channel_id="live-channel",
                    source_kind="live-acceptance",
                    source_id=f"{self.namespace}:base",
                    prompt="",
                    thread_name="live acceptance",
                    send_initial_prompt=False,
                    project_snapshot=project,
                    config_snapshot=await projects.config_snapshot(project),
                )
                context = {
                    "settings": settings,
                    "database": database,
                    "bridge": bridge,
                    "projects": projects,
                    "bindings": bindings,
                    "sessions": sessions,
                    "creation": creation,
                    "repository": repository,
                    "worker": worker,
                    "commands": commands,
                    "project": project,
                    "base_runtime": base_runtime,
                    "repo": repo,
                }
                for feature in self.features:
                    started = datetime.now(UTC)
                    try:
                        detail = await getattr(self, f"_feature_{feature}")(context)
                    except Exception as error:
                        failures.append(feature)
                        self.archive.record(
                            feature,
                            outcome="failed",
                            started_at=started,
                            detail={
                                "error_type": type(error).__name__,
                                "stage": getattr(error, "stage", feature),
                                "error_message": str(error),
                            },
                        )
                    else:
                        outcome = str(detail.pop("outcome", "passed"))
                        self.archive.record(
                            feature,
                            outcome=outcome,
                            started_at=started,
                            detail=detail,
                        )
            finally:
                if worker is not None:
                    await worker.stop()
                if sessions is not None:
                    await sessions.shutdown()
                await self._cleanup_sessions(database, bridge)
                try:
                    await bridge.stop()
                finally:
                    await database.close()
        summary = self.archive.finalize()
        if failures:
            raise LiveAcceptanceError("live acceptance failed: " + ", ".join(failures))
        return summary

    async def _authenticate(self, bridge: CopilotBridge) -> None:
        started = datetime.now(UTC)
        try:
            await bridge.start()
            models = await bridge.list_models()
            if not models:
                raise LiveAuthenticationError("Copilot returned no available models")
            identity = await bridge.runtime_identity()
        except Exception as error:
            self.archive.record(
                "authentication",
                outcome="failed",
                started_at=started,
                detail={"error_type": type(error).__name__, "authenticated": False},
            )
            self.archive.finalize()
            raise LiveAuthenticationError(
                "real Copilot authentication/runtime startup failed"
            ) from error
        self.archive.record(
            "authentication",
            outcome="passed",
            started_at=started,
            detail={
                "authenticated": True,
                "model_count": len(models),
                "runtime_version": identity["runtime_version"],
                "protocol_version": identity["protocol_version"],
            },
        )

    async def _build_app(
        self,
        settings: Settings,
        database: Database,
        bridge: CopilotBridge,
        repo: Path,
    ) -> tuple[
        ProjectRegistry,
        SessionBindingRepository,
        SessionRegistry,
        SessionCreationService,
        SchedulerRepository,
        SchedulerWorker,
        SchedulerCommandService,
    ]:
        projects = ProjectRegistry(database, resolved_home=repo)
        await projects.initialize()
        await projects.bind("live-channel", repo)
        bindings = SessionBindingRepository(database)
        leases = OwnerLeaseStore(database)
        capabilities = CapabilityRegistry(settings).load_checked()

        def runtime_factory(binding: SessionBinding) -> SessionRuntime:
            return SessionRuntime(
                database=database,
                bridge=bridge,
                bindings=bindings,
                owner_leases=leases,
                owner_id=f"live:{self.namespace}",
                binding=binding,
                queue_poll_seconds=0.1,
                capabilities=capabilities,
            )

        sessions = SessionRegistry(bindings, runtime_factory)
        creation = SessionCreationService(
            projects=projects,
            intents=CreationIntentRepository(database),
            bindings=bindings,
            sessions=sessions,
            threads=self.thread_gateway,
        )
        await StartupRecoveryInventory(database).run()
        repository = SchedulerRepository(database)
        await repository.recover()
        worker = SchedulerWorker(
            repository,
            ApplicationSchedulerAdapter(
                database,
                bindings,
                sessions,
                creation,
                capabilities,
            ),
            owner_id=f"live-scheduler:{self.namespace}",
            poll_seconds=0.1,
        )
        commands = SchedulerCommandService(database, projects, repository)
        return projects, bindings, sessions, creation, repository, worker, commands

    async def _feature_scheduled_message(self, context: dict[str, Any]) -> dict[str, Any]:
        token = f"LIVE_MESSAGE_{uuid.uuid4().hex[:10].upper()}"
        runtime: SessionRuntime = context["base_runtime"]
        definition = await context["commands"].create_message(
            thread_id=runtime.binding.thread_id,
            expression="at:2099-01-01T00:00:00Z",
            text=f"Reply with exactly {token} and no other text.",
            timezone="UTC",
            created_by=self.namespace,
        )
        self._schedule_ids.append(definition.id)
        run = await context["repository"].run_now(definition.id)
        terminal, content = await self._wait_for_run(context, run.run_id, token=token)
        return await self._run_detail(context, terminal, content, token)

    async def _feature_resume(self, context: dict[str, Any]) -> dict[str, Any]:
        runtime: SessionRuntime = context["base_runtime"]
        if runtime.state == RuntimeState.READY:
            await runtime.close(idempotency_key=f"{self.namespace}:resume-close")
        token = f"LIVE_RESUME_{uuid.uuid4().hex[:10].upper()}"
        definition = await context["commands"].create_message(
            thread_id=runtime.binding.thread_id,
            expression="at:2099-01-01T00:00:00Z",
            text=f"Reply with exactly {token} and no other text.",
            timezone="UTC",
            created_by=self.namespace,
        )
        self._schedule_ids.append(definition.id)
        run = await context["repository"].run_now(definition.id)
        terminal, content = await self._wait_for_run(context, run.run_id, token=token)
        binding = await context["bindings"].by_thread(runtime.binding.thread_id)
        detail = await self._run_detail(context, terminal, content, token)
        detail.update(
            {
                "binding_intent": binding.binding_intent.value,
                "attachment_state": binding.attachment_state.value,
                "temporary_attachment_released": (
                    binding.binding_intent.value == "closed"
                    and binding.attachment_state.value == "absent"
                ),
            }
        )
        if not detail["temporary_attachment_released"]:
            raise LiveAcceptanceError("temporary scheduler attachment was not released")
        return detail

    async def _feature_crash_recovery(self, context: dict[str, Any]) -> dict[str, Any]:
        crash_root = context["settings"].data_dir.parent / "crash-process"
        await asyncio.to_thread(crash_root.mkdir)
        marker = crash_root / "accepted.json"
        config_path = crash_root / "config.json"
        parent_nonce = uuid.uuid4().hex
        _atomic_json_write(
            config_path,
            {
                "root": str(crash_root),
                "marker": str(marker),
                "namespace": f"{self.namespace}-crash",
                "parent_nonce": parent_nonce,
                "timeout": self.timeout_seconds,
            },
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "copilotd.acceptance.live_scheduler_worktree",
            "--live",
            "--crash-child-config",
            str(config_path),
            "--parent-nonce",
            parent_nonce,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            deadline = asyncio.get_running_loop().time() + self.timeout_seconds
            while not await asyncio.to_thread(marker.exists):
                if process.returncode is not None:
                    raise LiveAcceptanceError(
                        f"crash child exited before acceptance with code {process.returncode}"
                    )
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("crash child did not persist acceptance")
                await asyncio.sleep(0.1)
            child = json.loads(await asyncio.to_thread(marker.read_text, encoding="utf-8"))
            if process.returncode is not None:
                raise LiveAcceptanceError("crash child exited after marker before parent SIGKILL")
            returncode = await _kill_and_reap(process)
            if returncode != -signal.SIGKILL:
                raise LiveAcceptanceError(f"crash child exit was {returncode}, expected SIGKILL")
        finally:
            if process.returncode is None:
                await _kill_and_reap(process)
        await asyncio.sleep(0.5)

        crash_database = Database(Path(str(child["database_path"])))
        await crash_database.open()
        recovery_now = time.time() + 120
        try:
            await StartupRecoveryInventory(crash_database).run(now=recovery_now)
            repository = SchedulerRepository(crash_database)
            await repository.recover(now=recovery_now)
            await repository.reconcile_submissions(now=recovery_now)
            recovered = await repository.get_run(str(child["run_id"]))
            queue_rows = await crash_database.fetchall(
                "SELECT id, state FROM message_queue WHERE schedule_run_id = ?",
                (child["run_id"],),
            )
            submission_rows = await crash_database.fetchall(
                "SELECT submission_id FROM submissions WHERE schedule_run_id = ?",
                (child["run_id"],),
            )
            probe_adapter = DeterministicSchedulerAdapter()
            recovered_worker = SchedulerWorker(
                repository,
                probe_adapter,
                owner_id=f"recovery-probe:{self.namespace}",
            )
            await recovered_worker.tick()
            queue_after = await crash_database.fetchone(
                "SELECT COUNT(*) FROM message_queue WHERE schedule_run_id = ?",
                (child["run_id"],),
            )
            submissions_after = await crash_database.fetchone(
                "SELECT COUNT(*) FROM submissions WHERE schedule_run_id = ?",
                (child["run_id"],),
            )
        finally:
            await crash_database.close()
        try:
            await context["bridge"].client.delete_session(str(child["session_id"]))
        except Exception:
            pass
        if recovered.status not in {
            ScheduleRunState.OUTCOME_UNKNOWN,
            ScheduleRunState.DISPATCH_UNKNOWN,
        }:
            raise LiveAcceptanceError(
                f"crash boundary settled as {recovered.status.value}, expected unknown"
            )
        if child["accepted"] is not True:
            raise LiveAcceptanceError("crash child did not persist SDK acceptance")
        if int(child["queue_row_count"]) != 1:
            raise LiveAcceptanceError("crash child persisted duplicate queue rows")
        if len(queue_rows) != 1 or len(submission_rows) != 1:
            raise LiveAcceptanceError("crash recovery did not retain exactly one queue/submission")
        automatic_resubmits = (
            int(queue_after[0])
            - len(queue_rows)
            + int(submissions_after[0])
            - len(submission_rows)
            + len(probe_adapter.prepare_calls)
            + len(probe_adapter.queue_notifications)
        )
        if automatic_resubmits != 0:
            raise LiveAcceptanceError("recovered worker attempted automatic redispatch")
        if recovered.render_intent_id is None:
            raise LiveAcceptanceError("crash recovery has no final render intent")
        return {
            "accepted_before_shutdown": True,
            "child_exit_code": process.returncode,
            "fresh_recovery_resources": True,
            "recovered_status": recovered.status.value,
            "queue_row_count": len(queue_rows),
            "submission_row_count": len(submission_rows),
            "automatic_resubmit_count": automatic_resubmits,
            "render_intent_persisted": True,
        }

    async def _feature_scheduled_new_session(
        self,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        token = f"LIVE_NEW_SESSION_{uuid.uuid4().hex[:10].upper()}"
        before = getattr(self.thread_gateway, "create_calls", None)
        definition = await context["commands"].create_new_session(
            channel_id="live-channel",
            expression="at:2099-01-01T00:00:00Z",
            text=f"Reply with exactly {token} and no other text.",
            timezone="UTC",
            created_by=self.namespace,
            thread_name="live scheduled new session",
        )
        self._schedule_ids.append(definition.id)
        run = await context["repository"].run_now(definition.id)
        terminal, content = await self._wait_for_run(context, run.run_id, token=token)
        after = getattr(self.thread_gateway, "create_calls", None)
        detail = await self._run_detail(context, terminal, content, token)
        detail.update(
            {
                "target_thread_created_once": (
                    None if before is None or after is None else after - before == 1
                ),
                "result_thread_present": terminal.result_thread_id is not None,
                "result_session_present": terminal.result_session_id is not None,
            }
        )
        if detail["target_thread_created_once"] is not True:
            raise LiveAcceptanceError("new-session target was not created exactly once")
        if not detail["result_thread_present"] or not detail["result_session_present"]:
            raise LiveAcceptanceError("new-session target identifiers are incomplete")
        return detail

    async def _feature_blank_worktree(self, context: dict[str, Any]) -> dict[str, Any]:
        project_id = context["project"].project_id
        if project_id is None:
            raise LiveAcceptanceError("blank worktree requires explicit project")
        manager = WorktreeManager(
            context["database"],
            context["projects"],
            worktrees_root=context["settings"].data_dir / "worktrees",
            adapter=SessionCreationWorktreeAdapter(
                context["creation"],
                context["database"],
                fork_adapter=self.fork_adapter,
            ),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name=f"blank-{self.namespace}",
        )
        runtime = context["sessions"].for_thread(str(created.thread_id))
        if runtime is None:
            raise LiveAcceptanceError("blank worktree session runtime was not created")
        await runtime.close(idempotency_key=f"{self.namespace}:worktree-close")
        binding = await context["bindings"].by_thread(str(created.thread_id))
        if binding is None or binding.runtime_remote_mode != "off":
            raise LiveAcceptanceError("blank worktree session did not confirm remote exposure off")
        closed = await manager.close(created.name, parent_project_id=project_id)
        branch = await _git(
            context["repo"],
            "show-ref",
            "--verify",
            f"refs/heads/{created.branch_name}",
            check=False,
        )
        detail = {
            "created_state": created.state,
            "closed_state": closed.state,
            "worktree_removed": not created.path.exists(),
            "branch_preserved": branch[0] == 0,
            "history_mode": created.history_mode.value,
        }
        if not (
            detail["created_state"] == "ready"
            and detail["closed_state"] == "closed"
            and detail["worktree_removed"]
            and detail["branch_preserved"]
            and detail["history_mode"] == "none"
        ):
            raise LiveAcceptanceError("blank worktree lifecycle invariants failed")
        return detail

    async def _feature_history_fork_gate(
        self,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        project_id = context["project"].project_id
        if project_id is None:
            raise LiveAcceptanceError("history fork gate requires explicit project")
        manager = WorktreeManager(
            context["database"],
            context["projects"],
            worktrees_root=context["settings"].data_dir / "worktrees",
            adapter=SessionCreationWorktreeAdapter(
                context["creation"],
                context["database"],
                fork_adapter=self.fork_adapter,
            ),
        )
        if self.fork_adapter is None or not self.fork_adapter.available:
            before_refs = await _git(context["repo"], "show-ref", check=False)
            before_worktrees = await _git(
                context["repo"],
                "worktree",
                "list",
                "--porcelain",
            )
            before_intents = await context["database"].fetchone(
                "SELECT COUNT(*) FROM worktree_intents"
            )
            try:
                await manager.create(
                    parent_project_id=project_id,
                    name=f"fork-gate-{self.namespace}",
                    history_mode=WorktreeHistoryMode.FORK,
                    source_session_id=context["base_runtime"].binding.sdk_session_id,
                )
            except WorktreeCapabilityError:
                after_refs = await _git(context["repo"], "show-ref", check=False)
                after_worktrees = await _git(
                    context["repo"],
                    "worktree",
                    "list",
                    "--porcelain",
                )
                after_intents = await context["database"].fetchone(
                    "SELECT COUNT(*) FROM worktree_intents"
                )
                side_effects = sum(
                    (
                        before_refs[1] != after_refs[1],
                        before_worktrees[1] != after_worktrees[1],
                        int(before_intents[0]) != int(after_intents[0]),
                    )
                )
                if side_effects:
                    raise LiveAcceptanceError(
                        "history fork gate produced durable or Git side effects"
                    ) from None
                return {
                    "outcome": "gated",
                    "capability_available": False,
                    "prompt_emulation_used": False,
                    "git_side_effects": side_effects,
                }
            raise LiveAcceptanceError("history fork did not fail closed")
        created = await manager.create(
            parent_project_id=project_id,
            name=f"fork-live-{self.namespace}",
            history_mode=WorktreeHistoryMode.FORK,
            source_session_id=context["base_runtime"].binding.sdk_session_id,
        )
        runtime = (
            None if created.thread_id is None else context["sessions"].for_thread(created.thread_id)
        )
        if runtime is not None and runtime.state == RuntimeState.READY:
            await runtime.close(idempotency_key=f"{self.namespace}:fork-close")
        closed = await manager.close(created.name, parent_project_id=project_id)
        detail = {
            "capability_available": True,
            "history_mode": created.history_mode.value,
            "target_session_present": created.sdk_session_id is not None,
            "prompt_emulation_used": False,
            "closed_state": closed.state,
        }
        if not (
            detail["history_mode"] == WorktreeHistoryMode.FORK.value
            and detail["target_session_present"]
            and detail["closed_state"] == "closed"
            and not detail["prompt_emulation_used"]
        ):
            raise LiveAcceptanceError("history fork lifecycle evidence is incomplete")
        return detail

    async def _wait_for_run(
        self,
        context: dict[str, Any],
        run_id: str,
        *,
        token: str,
    ) -> tuple[ScheduleRun, str]:
        deadline = asyncio.get_running_loop().time() + self.timeout_seconds
        while True:
            await context["worker"].tick()
            run = await context["repository"].get_run(run_id)
            content = await _assistant_content(
                context["database"],
                run.result_session_id,
                after=run.created_at,
            )
            if run.status.terminal:
                if run.status != ScheduleRunState.SEMANTIC_COMPLETE:
                    raise LiveAcceptanceError(
                        f"run {run_id} terminated as {run.status.value}; "
                        f"error_code={run.error_code or 'none'}"
                    )
                if content.strip() != token:
                    raise LiveAcceptanceError(
                        "finalized assistant response was not the exact acceptance token"
                    )
                return run, content
            if asyncio.get_running_loop().time() >= deadline:
                queue = await context["database"].fetchone(
                    """
                    SELECT state FROM message_queue
                    WHERE schedule_run_id = ? ORDER BY created_at DESC LIMIT 1
                    """,
                    (run_id,),
                )
                raise TimeoutError(
                    f"schedule run {run_id} did not settle; "
                    f"status={run.status.value}; attempt={run.attempt}; "
                    f"error_code={run.error_code or 'none'}; "
                    f"error_detail={run.error_detail or 'none'}; "
                    f"queue_state={None if queue is None else queue['state']}"
                )
            await asyncio.sleep(0.25)

    async def _run_detail(
        self,
        context: dict[str, Any],
        run: ScheduleRun,
        content: str,
        token: str,
    ) -> dict[str, Any]:
        queue = await context["database"].fetchone(
            """
            SELECT state FROM message_queue
            WHERE schedule_run_id = ? ORDER BY created_at DESC LIMIT 1
            """,
            (run.run_id,),
        )
        detail = {
            "run_status": run.status.value,
            "completion_basis": run.completion_basis,
            "attempt": run.attempt,
            "queue_state": None if queue is None else str(queue["state"]),
            "accepted": run.accepted_message_id is not None,
            "semantic_token_matched": content.strip() == token,
            "response_sha256": hashlib.sha256(content.encode()).hexdigest(),
            "render_intent_persisted": run.render_intent_id is not None,
        }
        if not (
            detail["run_status"] == ScheduleRunState.SEMANTIC_COMPLETE.value
            and detail["queue_state"] == "submitted"
            and detail["accepted"]
            and detail["semantic_token_matched"]
            and detail["render_intent_persisted"]
        ):
            raise LiveAcceptanceError("scheduled run evidence is incomplete")
        return detail

    async def _cleanup_sessions(
        self,
        database: Database,
        bridge: CopilotBridge,
    ) -> None:
        try:
            client = bridge.client
            rows = await database.fetchall("SELECT sdk_session_id FROM session_bindings")
        except RuntimeError:
            return
        for row in rows:
            try:
                await client.delete_session(str(row["sdk_session_id"]))
            except Exception:
                pass


async def _run_crash_child(config_path: Path, *, parent_nonce: str) -> None:
    config = json.loads(await asyncio.to_thread(config_path.read_text, encoding="utf-8"))
    expected_nonce = str(config.get("parent_nonce", ""))
    if not parent_nonce or not hmac.compare_digest(parent_nonce, expected_nonce):
        raise LiveAcceptanceError("crash child parent nonce is missing or invalid")
    root = Path(str(config["root"]))
    marker = Path(str(config["marker"]))
    namespace = str(config["namespace"])
    timeout = float(config["timeout"])
    repo = root / "repo"
    await _init_git_repo(repo)
    settings = Settings(
        data_dir=root / "data",
        cache_dir=root / "cache",
        log_dir=root / "logs",
        resolved_home=repo,
    )
    settings.ensure_directories()
    database = Database(settings.database_path)
    bridge = CopilotBridge(settings)
    sessions: SessionRegistry | None = None
    worker: SchedulerWorker | None = None
    harness = LiveSchedulerWorktreeHarness(
        output_dir=root / "child-results",
        namespace=f"{namespace}-results",
        timeout_seconds=timeout,
        features=("crash_recovery",),
    )
    try:
        await database.open()
        await bridge.start()
        if not await bridge.list_models():
            raise LiveAuthenticationError("crash child has no authenticated models")
        (
            projects,
            _bindings,
            sessions,
            creation,
            repository,
            worker,
            commands,
        ) = await harness._build_app(settings, database, bridge, repo)
        project = await projects.resolve("live-channel")
        runtime = await creation.create_from_source(
            channel_id="live-channel",
            source_kind="live-crash",
            source_id=f"{namespace}:base",
            prompt="",
            thread_name="live crash acceptance",
            send_initial_prompt=False,
            project_snapshot=project,
            config_snapshot=await projects.config_snapshot(project),
        )
        token = f"LIVE_CRASH_{uuid.uuid4().hex[:10].upper()}"
        definition = await commands.create_message(
            thread_id=runtime.binding.thread_id,
            expression="at:2099-01-01T00:00:00Z",
            text=(f"Use a shell tool to run `sleep 20`, then reply with exactly {token}."),
            timezone="UTC",
            created_by=namespace,
        )
        run = await repository.run_now(definition.id)
        await worker.tick()
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            accepted = await repository.get_run(run.run_id)
            if accepted.accepted_message_id is not None:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("crash child did not observe SDK acceptance")
            await asyncio.sleep(0.05)
        queue = await database.fetchone(
            "SELECT COUNT(*) FROM message_queue WHERE schedule_run_id = ?",
            (run.run_id,),
        )
        await asyncio.to_thread(
            _atomic_json_write,
            marker,
            {
                "accepted": True,
                "database_path": str(settings.database_path),
                "run_id": run.run_id,
                "session_id": runtime.binding.sdk_session_id,
                "queue_row_count": int(queue[0]),
            },
        )
        await asyncio.Event().wait()
    finally:
        if worker is not None:
            await worker.stop()
        if sessions is not None:
            await sessions.shutdown()
        try:
            await bridge.stop()
        finally:
            await database.close()


async def _assistant_content(
    database: Database,
    session_id: str | None,
    *,
    after: float,
) -> str:
    if session_id is None:
        return ""
    rows = await database.fetchall(
        """
        SELECT payload FROM render_outbox
        WHERE session_id = ? AND lane = 'assistant_final' AND created_at >= ?
        ORDER BY logical_seq DESC, created_at DESC
        """,
        (session_id, after),
    )
    for row in rows:
        payload = json.loads(str(row["payload"]))
        if payload.get("finalized") is True:
            return str(payload.get("content", ""))
    return ""


async def _init_git_repo(repo: Path) -> None:
    await asyncio.to_thread(repo.mkdir, parents=True)
    await _git(repo, "init")
    await _git(repo, "config", "user.name", "copilotD live acceptance")
    await _git(repo, "config", "user.email", "copilotd-live@example.invalid")
    (repo / "README.txt").write_text("disposable live acceptance\n", encoding="utf-8")
    await _git(repo, "add", "README.txt")
    await _git(repo, "commit", "-m", "disposable acceptance root")


async def _git(
    cwd: Path,
    *arguments: str,
    check: bool = True,
) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        "git",
        *arguments,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    decoded_stdout = stdout.decode(errors="replace")
    decoded_stderr = stderr.decode(errors="replace")
    if check and process.returncode != 0:
        raise LiveAcceptanceError(f"disposable Git command failed: git {arguments[0]}")
    return int(process.returncode or 0), decoded_stdout, decoded_stderr


async def _kill_and_reap(process: asyncio.subprocess.Process) -> int:
    if process.returncode is None:
        process.send_signal(signal.SIGKILL)
    waiter = asyncio.create_task(process.wait())
    try:
        return await asyncio.shield(waiter)
    except asyncio.CancelledError:
        await waiter
        raise


def _sanitize(value: Any, *, key: str = "") -> Any:
    if _SECRET_KEY.search(key) and isinstance(value, str):
        return "<redacted>"
    if isinstance(value, dict):
        return {str(name): _sanitize(item, key=str(name)) for name, item in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_sanitize(item, key=key) for item in value]
    if isinstance(value, Path):
        return "<disposable-path>"
    if isinstance(value, str):
        if key.endswith("sha256") and re.fullmatch(r"[0-9a-f]{64}", value):
            return value
        scrubbed = _LONG_SECRET.sub("<redacted>", value)
        return scrubbed[:500]
    if value is None or isinstance(value, bool | int | float):
        return value
    return type(value).__name__


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run disposable real Copilot scheduler/worktree acceptance cases."
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="required secure opt-in; missing auth is a failure, never a skip",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--features",
        default=",".join(_FEATURES),
        help="comma-separated feature names",
    )
    parser.add_argument("--namespace")
    parser.add_argument(
        "--crash-child-config",
        type=Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--parent-nonce", help=argparse.SUPPRESS)
    return parser


async def run_from_args(args: argparse.Namespace) -> dict[str, Any]:
    if not args.live:
        raise ValueError("--live is required to contact real Copilot services")
    if args.output is None:
        raise ValueError("--output is required in live acceptance mode")
    features = tuple(item.strip() for item in args.features.split(",") if item.strip())
    harness = LiveSchedulerWorktreeHarness(
        output_dir=args.output,
        timeout_seconds=args.timeout,
        namespace=args.namespace,
        features=features,
    )
    return await harness.run()


def main() -> None:
    args = build_parser().parse_args()
    if args.crash_child_config is not None:
        if not args.live:
            raise SystemExit("--live is required for crash child mode")
        if args.parent_nonce is None:
            raise SystemExit("--parent-nonce is required for crash child mode")
        asyncio.run(
            _run_crash_child(
                args.crash_child_config,
                parent_nonce=args.parent_nonce,
            )
        )
        return
    try:
        summary = asyncio.run(run_from_args(args))
    except Exception as error:
        print(
            json.dumps(
                {
                    "outcome": "failed",
                    "error_type": type(error).__name__,
                    "results_root": (
                        None if args.namespace is None else str(args.output / args.namespace)
                    ),
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1) from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
