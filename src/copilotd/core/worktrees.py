from __future__ import annotations

import asyncio
import os
import re
import signal
import subprocess
import sys
import time
import unicodedata
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from aiosqlite import Connection, Row

from copilotd.core.projects import (
    ProjectConfigError,
    ProjectConfigSnapshot,
    ProjectPathError,
    ProjectRegistry,
    ProjectSnapshot,
)
from copilotd.core.sessions import SessionCreationService, SessionCreationUnknown
from copilotd.core.task_registry import TaskRegistry
from copilotd.storage.database import Database
from copilotd.storage.state_only import state_only_json

try:
    import fcntl as _fcntl
except ImportError:  # Windows
    _fcntl = None

try:
    import msvcrt as _msvcrt
except ImportError:  # POSIX
    _msvcrt = None

WORKTREE_GIT_CREATE_LEASE_SECONDS = 300.0
_GIT_CHILD_STOP_SECONDS = 5.0
_GIT_EXEC_WRAPPER = """
import os
import sys

gate_fd = int(sys.argv[1])
lock_fd = int(sys.argv[2])
if os.read(gate_fd, 1) != b"1":
    os._exit(125)
os.close(gate_fd)
os.set_inheritable(lock_fd, True)
os.execvp(sys.argv[3], sys.argv[3:])
"""
_WINDOWS_GIT_EXEC_WRAPPER = r"""
import msvcrt
import os
import pathlib
import subprocess
import sys
import time

gate_path = pathlib.Path(sys.argv[1])
ack_path = pathlib.Path(sys.argv[2])
lock_path = pathlib.Path(sys.argv[3])
parent_pid = int(sys.argv[4])
argv = sys.argv[5:]
deadline = time.monotonic() + 30.0
while not gate_path.exists():
    try:
        os.kill(parent_pid, 0)
    except OSError:
        os._exit(125)
    if time.monotonic() >= deadline:
        os._exit(125)
    time.sleep(0.01)

lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
if os.fstat(lock_fd).st_size == 0:
    os.write(lock_fd, b"\0")
os.lseek(lock_fd, 0, os.SEEK_SET)
try:
    msvcrt.locking(lock_fd, msvcrt.LK_NBLCK, 1)
except OSError:
    sys.stderr.write("copilotd-owned-git-lock-busy\n")
    os._exit(75)

temporary_ack = ack_path.with_suffix(ack_path.suffix + ".tmp")
temporary_ack.write_text(str(os.getpid()), encoding="ascii")
os.replace(temporary_ack, ack_path)
creation_flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
process = subprocess.Popen(argv, creationflags=creation_flags)
try:
    return_code = process.wait()
finally:
    os.lseek(lock_fd, 0, os.SEEK_SET)
    msvcrt.locking(lock_fd, msvcrt.LK_UNLCK, 1)
    os.close(lock_fd)
raise SystemExit(return_code)
"""
_RETRYABLE_GIT_PREFLIGHT_ERRORS = {
    "worktree_branch_conflict",
    "worktree_path_conflict",
}


class WorktreeHistoryMode(StrEnum):
    NONE = "none"
    FORK = "fork"


class WorktreeIntentState(StrEnum):
    RESERVED = "reserved"
    GIT_CREATING = "git_creating"
    GIT_CREATED = "git_created"
    PROJECT_REGISTERED = "project_registered"
    TARGET_CREATING = "target_creating"
    TARGET_UNKNOWN = "target_unknown"
    READY = "ready"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"
    CLOSING = "closing"
    CLOSE_UNKNOWN = "close_unknown"
    CLOSED = "closed"


_INTENT_PREDECESSORS: dict[WorktreeIntentState, tuple[WorktreeIntentState, ...]] = {
    WorktreeIntentState.GIT_CREATING: (
        WorktreeIntentState.RESERVED,
        WorktreeIntentState.GIT_CREATING,
    ),
    WorktreeIntentState.GIT_CREATED: (
        WorktreeIntentState.GIT_CREATING,
        WorktreeIntentState.GIT_CREATED,
    ),
    WorktreeIntentState.PROJECT_REGISTERED: (
        WorktreeIntentState.GIT_CREATED,
        WorktreeIntentState.PROJECT_REGISTERED,
    ),
    WorktreeIntentState.TARGET_CREATING: (WorktreeIntentState.PROJECT_REGISTERED,),
    WorktreeIntentState.TARGET_UNKNOWN: (
        WorktreeIntentState.TARGET_CREATING,
        WorktreeIntentState.TARGET_UNKNOWN,
    ),
    WorktreeIntentState.COMPENSATING: (
        WorktreeIntentState.GIT_CREATED,
        WorktreeIntentState.PROJECT_REGISTERED,
        WorktreeIntentState.TARGET_CREATING,
        WorktreeIntentState.COMPENSATING,
    ),
    WorktreeIntentState.FAILED: (
        WorktreeIntentState.RESERVED,
        WorktreeIntentState.GIT_CREATING,
        WorktreeIntentState.FAILED,
    ),
    WorktreeIntentState.CLOSING: (WorktreeIntentState.READY,),
    WorktreeIntentState.CLOSE_UNKNOWN: (
        WorktreeIntentState.CLOSING,
        WorktreeIntentState.COMPENSATING,
        WorktreeIntentState.CLOSE_UNKNOWN,
    ),
}


class WorktreeError(RuntimeError):
    code = "CD-WORKTREE-001"


class WorktreeInputError(WorktreeError):
    code = "CD-INPUT-001"


class WorktreeConflict(WorktreeError):
    code = "CD-WORKTREE-CONFLICT"

    def __init__(self, message: str, *, blockers: tuple[str, ...] = ()) -> None:
        self.blockers = blockers
        super().__init__(message)


class WorktreeRetryPending(WorktreeConflict):
    def __init__(self, intent_id: str, retry_at: float) -> None:
        self.intent_id = intent_id
        self.retry_at = retry_at
        super().__init__(f"worktree Git creation retry is scheduled at {retry_at}")


class _OwnedGitMutationBusy(WorktreeConflict):
    pass


class WorktreeCapabilityError(WorktreeError):
    code = "CD-CAP-001"


class WorktreeOperationError(WorktreeError):
    def __init__(self, message: str, *, outcome_unknown: bool) -> None:
        self.outcome_unknown = outcome_unknown
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class GitCommandResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class GitWorktreeMetadata:
    path: Path
    branch_ref: str | None
    head: str | None


class GitRunner(Protocol):
    async def run(self, argv: list[str], *, cwd: Path) -> GitCommandResult: ...


class SubprocessGitRunner:
    async def run(self, argv: list[str], *, cwd: Path) -> GitCommandResult:
        _validate_git_argv(argv)
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **_subprocess_group_kwargs(),
        )
        return await _communicate_git_process(process)

    async def run_owned(
        self,
        argv: list[str],
        *,
        cwd: Path,
        lock_path: Path,
        on_started: Callable[[int], Awaitable[None]],
    ) -> GitCommandResult:
        _validate_git_argv(argv)
        if sys.platform == "win32":
            return await self._run_owned_windows(
                argv,
                cwd=cwd,
                lock_path=lock_path,
                on_started=on_started,
            )
        if _fcntl is None:
            raise WorktreeCapabilityError(
                "owned Git mutations require POSIX flock or Windows msvcrt locking"
            )
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        gate_read, gate_write = os.pipe()
        process: asyncio.subprocess.Process | None = None
        try:
            try:
                _fcntl.flock(lock_fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise _OwnedGitMutationBusy(
                    "a prior owned Git mutation is still running"
                ) from error
            os.set_inheritable(lock_fd, True)
            os.set_inheritable(gate_read, True)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                _GIT_EXEC_WRAPPER,
                str(gate_read),
                str(lock_fd),
                *argv,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                pass_fds=(gate_read, lock_fd),
                start_new_session=True,
            )
            os.close(gate_read)
            gate_read = -1
            os.close(lock_fd)
            lock_fd = -1
            try:
                await on_started(process.pid)
            except BaseException:
                os.close(gate_write)
                gate_write = -1
                await _terminate_git_process(process)
                raise
            os.write(gate_write, b"1")
            os.close(gate_write)
            gate_write = -1
            return await _communicate_git_process(process)
        except asyncio.CancelledError:
            if process is not None:
                await asyncio.shield(_terminate_git_process(process))
            raise
        finally:
            for descriptor in (gate_read, gate_write, lock_fd):
                if descriptor >= 0:
                    os.close(descriptor)

    async def _run_owned_windows(
        self,
        argv: list[str],
        *,
        cwd: Path,
        lock_path: Path,
        on_started: Callable[[int], Awaitable[None]],
    ) -> GitCommandResult:
        if _msvcrt is None:
            raise WorktreeCapabilityError("Windows owned Git mutations require msvcrt")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        token = uuid.uuid4().hex
        gate_path = lock_path.with_name(f"{lock_path.name}.{token}.gate")
        ack_path = lock_path.with_name(f"{lock_path.name}.{token}.ack")
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                _WINDOWS_GIT_EXEC_WRAPPER,
                str(gate_path),
                str(ack_path),
                str(lock_path),
                str(os.getpid()),
                *argv,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **_subprocess_group_kwargs(),
            )
            try:
                await on_started(process.pid)
            except BaseException:
                await _terminate_git_process(process)
                raise
            await asyncio.to_thread(gate_path.write_bytes, b"1")
            result = await _communicate_git_process(process)
            if result.returncode == 75 and "copilotd-owned-git-lock-busy" in result.stderr:
                raise _OwnedGitMutationBusy("a prior owned Git mutation is still running")
            return result
        except asyncio.CancelledError:
            if process is not None:
                await asyncio.shield(_terminate_git_process(process))
            raise
        finally:
            await asyncio.to_thread(gate_path.unlink, missing_ok=True)
            await asyncio.to_thread(ack_path.unlink, missing_ok=True)


def _validate_git_argv(argv: list[str]) -> None:
    if not argv or argv[0] != "git":
        raise ValueError("Git runner only accepts a literal git argv")


def _subprocess_group_kwargs() -> dict[str, object]:
    if sys.platform == "win32":
        return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}
    return {"start_new_session": True}


async def _communicate_git_process(
    process: asyncio.subprocess.Process,
) -> GitCommandResult:
    communication = asyncio.create_task(process.communicate())
    try:
        stdout, stderr = await asyncio.shield(communication)
    except asyncio.CancelledError:
        await asyncio.shield(_terminate_git_process(process, communication))
        raise
    return GitCommandResult(
        returncode=int(process.returncode or 0),
        stdout=stdout.decode(errors="replace"),
        stderr=stderr.decode(errors="replace"),
    )


async def _terminate_git_process(
    process: asyncio.subprocess.Process,
    communication: asyncio.Task[tuple[bytes, bytes]] | None = None,
) -> None:
    if process.returncode is None:
        if sys.platform == "win32":
            await _terminate_windows_process_tree(process)
        else:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    waiter: Awaitable[object] = process.wait() if communication is None else communication
    try:
        async with asyncio.timeout(_GIT_CHILD_STOP_SECONDS):
            await asyncio.shield(waiter)
    except TimeoutError:
        if process.returncode is None:
            if sys.platform == "win32":
                await _terminate_windows_process_tree(process)
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if communication is None:
            await process.wait()
        else:
            await communication


async def _terminate_windows_process_tree(process: asyncio.subprocess.Process) -> None:
    try:
        terminator = await asyncio.create_subprocess_exec(
            "taskkill.exe",
            "/PID",
            str(process.pid),
            "/T",
            "/F",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        process.kill()
        return
    await terminator.wait()
    if process.returncode is None and terminator.returncode:
        process.kill()


def _git_lock_is_held(path: Path) -> bool:
    if not path.exists():
        return False
    descriptor = os.open(path, os.O_RDWR)
    try:
        if sys.platform == "win32":
            if _msvcrt is None:
                raise WorktreeCapabilityError("Windows Git lock inspection requires msvcrt")
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
            os.lseek(descriptor, 0, os.SEEK_SET)
            try:
                _msvcrt.locking(descriptor, _msvcrt.LK_NBLCK, 1)
            except OSError:
                return True
            os.lseek(descriptor, 0, os.SEEK_SET)
            _msvcrt.locking(descriptor, _msvcrt.LK_UNLCK, 1)
            return False
        if _fcntl is None:
            raise WorktreeCapabilityError("Git lock inspection requires POSIX flock")
        try:
            _fcntl.flock(descriptor, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        _fcntl.flock(descriptor, _fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class WorktreeTarget:
    thread_id: str
    sdk_session_id: str


@dataclass(frozen=True, slots=True)
class WorktreeIntent:
    intent_id: str
    parent_project_id: str
    source_session_id: str | None
    name: str
    branch_name: str
    base_ref: str
    history_mode: WorktreeHistoryMode
    target_path: Path
    project_id: str | None
    thread_id: str | None
    sdk_session_id: str | None
    created_branch: bool
    state: WorktreeIntentState
    error_code: str | None
    error_detail: str | None
    git_create_holder: str | None
    git_create_fence_token: int
    git_create_lease_expires_at: float | None
    git_create_process_generation: int | None
    git_create_retry_at: float | None
    git_child_pid: int | None
    git_child_token: str | None
    git_child_process_generation: int | None
    git_child_started_at: float | None
    created_at: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class WorktreeProjection:
    worktree_id: str
    intent_id: str
    parent_project_id: str
    project_id: str
    name: str
    path: Path
    branch_name: str
    base_ref: str
    history_mode: WorktreeHistoryMode
    thread_id: str | None
    sdk_session_id: str | None
    state: str
    session_count: int
    active_submission_count: int
    active_lease_count: int
    schedule_count: int
    remote_reference_count: int


@dataclass(frozen=True, slots=True)
class WorktreeRecoveryReport:
    recovery_id: str
    examined_intents: int
    recovered_intents: int
    orphaned_intents: int


class HistoryForkAdapter(Protocol):
    @property
    def available(self) -> bool: ...

    async def fork(
        self,
        *,
        source_session_id: str,
        source_id: str,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
        thread_name: str,
    ) -> WorktreeTarget: ...

    async def reconcile(self, *, source_id: str) -> WorktreeTarget | None: ...


class WorktreeSessionAdapter(Protocol):
    @property
    def history_fork_available(self) -> bool: ...

    async def create_blank(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
        preallocated_session_id: str,
    ) -> WorktreeTarget: ...

    async def create_history_fork(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
    ) -> WorktreeTarget: ...

    async def reconcile_target(self, intent: WorktreeIntent) -> WorktreeTarget | None: ...


class SessionCreationWorktreeAdapter:
    def __init__(
        self,
        creation: SessionCreationService,
        database: Database,
        *,
        fork_adapter: HistoryForkAdapter | None = None,
    ) -> None:
        self._creation = creation
        self._database = database
        self._fork_adapter = fork_adapter

    @property
    def history_fork_available(self) -> bool:
        return self._fork_adapter is not None and self._fork_adapter.available

    async def create_blank(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
        preallocated_session_id: str,
    ) -> WorktreeTarget:
        try:
            runtime = await self._creation.create_from_source(
                channel_id=project.channel_id,
                source_kind="worktree",
                source_id=intent.intent_id,
                prompt="",
                thread_name=intent.name,
                send_initial_prompt=False,
                project_snapshot=project,
                config_snapshot=config_snapshot,
                preallocated_session_id=preallocated_session_id,
                worktree_intent_id=intent.intent_id,
            )
        except SessionCreationUnknown as error:
            raise WorktreeOperationError(str(error), outcome_unknown=True) from error
        return WorktreeTarget(
            thread_id=runtime.binding.thread_id,
            sdk_session_id=runtime.binding.sdk_session_id,
        )

    async def create_history_fork(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
    ) -> WorktreeTarget:
        if self._fork_adapter is None or not self._fork_adapter.available:
            raise WorktreeCapabilityError("native sessions.fork capability is unavailable")
        if intent.source_session_id is None:
            raise WorktreeInputError("history=fork requires a source session")
        return await self._fork_adapter.fork(
            source_session_id=intent.source_session_id,
            source_id=intent.intent_id,
            project=project,
            config_snapshot=config_snapshot,
            thread_name=intent.name,
        )

    async def reconcile_target(self, intent: WorktreeIntent) -> WorktreeTarget | None:
        if intent.history_mode == WorktreeHistoryMode.FORK:
            if self._fork_adapter is None or not self._fork_adapter.available:
                return None
            return await self._fork_adapter.reconcile(source_id=intent.intent_id)
        row = await self._database.fetchone(
            """
            SELECT thread_id, sdk_session_id
            FROM session_creation_intents
            WHERE source_kind = 'worktree' AND source_id = ?
              AND thread_id IS NOT NULL
            """,
            (intent.intent_id,),
        )
        if row is None:
            if intent.project_id is None:
                return None
            project = await self._creation.projects.project_by_id(intent.project_id)
            config = await self._creation.projects.config_snapshot(project)
            try:
                runtime = await self._creation.create_from_source(
                    channel_id=project.channel_id,
                    source_kind="worktree",
                    source_id=intent.intent_id,
                    prompt="",
                    thread_name=intent.name,
                    send_initial_prompt=False,
                    project_snapshot=project,
                    config_snapshot=config,
                    worktree_intent_id=intent.intent_id,
                )
            except SessionCreationUnknown:
                return None
            return WorktreeTarget(
                thread_id=runtime.binding.thread_id,
                sdk_session_id=runtime.binding.sdk_session_id,
            )
        return WorktreeTarget(
            thread_id=str(row["thread_id"]),
            sdk_session_id=str(row["sdk_session_id"]),
        )


class DeterministicWorktreeAdapter:
    def __init__(self, *, history_fork_available: bool = False) -> None:
        self._history_fork_available = history_fork_available
        self.targets: dict[str, WorktreeTarget] = {}
        self.create_calls: list[str] = []
        self.failure: WorktreeOperationError | None = None

    @property
    def history_fork_available(self) -> bool:
        return self._history_fork_available

    async def create_blank(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
        preallocated_session_id: str,
    ) -> WorktreeTarget:
        del project, config_snapshot
        return await self._create(intent, preallocated_session_id)

    async def create_history_fork(
        self,
        *,
        intent: WorktreeIntent,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot,
    ) -> WorktreeTarget:
        del project, config_snapshot
        if not self.history_fork_available:
            raise WorktreeCapabilityError("native sessions.fork capability is unavailable")
        session_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:worktree:{intent.intent_id}:fork")
        )
        return await self._create(intent, session_id)

    async def _create(
        self,
        intent: WorktreeIntent,
        session_id: str,
    ) -> WorktreeTarget:
        self.create_calls.append(intent.intent_id)
        existing = self.targets.get(intent.intent_id)
        if existing is not None:
            return existing
        if self.failure is not None:
            raise self.failure
        target = WorktreeTarget(
            thread_id=f"thread-{intent.intent_id}",
            sdk_session_id=session_id,
        )
        self.targets[intent.intent_id] = target
        return target

    async def reconcile_target(self, intent: WorktreeIntent) -> WorktreeTarget | None:
        return self.targets.get(intent.intent_id)


class WorktreeManager:
    def __init__(
        self,
        database: Database,
        projects: ProjectRegistry,
        *,
        worktrees_root: Path,
        adapter: WorktreeSessionAdapter,
        git: GitRunner | None = None,
        process_owner_id: str | None = None,
        git_create_lease_seconds: float = WORKTREE_GIT_CREATE_LEASE_SECONDS,
        task_registry: TaskRegistry | None = None,
    ) -> None:
        if git_create_lease_seconds <= 0:
            raise ValueError("worktree Git creation lease must be positive")
        self._database = database
        self._projects = projects
        self._worktrees_root = worktrees_root.expanduser().resolve()
        self._adapter = adapter
        self._git = SubprocessGitRunner() if git is None else git
        self._process_owner_id = process_owner_id or f"worktree:{uuid.uuid4()}"
        self._process_generation: int | None = None
        self._process_generation_lock = asyncio.Lock()
        self._git_create_lease_seconds = git_create_lease_seconds
        self._task_registry = task_registry
        self._recovery_retry_tasks: dict[str, asyncio.Task[None]] = {}
        self._active_git_holders: set[str] = set()

    @property
    def history_fork_available(self) -> bool:
        return self._adapter.history_fork_available

    async def create(
        self,
        *,
        parent_project_id: str,
        name: str,
        base_ref: str = "HEAD",
        history_mode: WorktreeHistoryMode = WorktreeHistoryMode.NONE,
        source_session_id: str | None = None,
        now: float | None = None,
    ) -> WorktreeProjection:
        timestamp = time.time() if now is None else now
        await self._ensure_process_generation(now=timestamp)
        normalized_name = _validate_worktree_name(name)
        _validate_base_ref(base_ref)
        if history_mode == WorktreeHistoryMode.FORK:
            if not self._adapter.history_fork_available:
                raise WorktreeCapabilityError(
                    "history=fork requires the native sessions.fork capability"
                )
            if source_session_id is None:
                raise WorktreeInputError("history=fork requires source_session_id")
        parent = await self._projects.project_by_id(parent_project_id)
        repo_root = await self._repo_root(parent.root_path)
        await self._require_commit(repo_root, base_ref)
        intent_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:worktree:{parent_project_id}:{normalized_name}",
            )
        )
        branch_name = _branch_name(normalized_name, intent_id)
        await self._validate_branch(repo_root, branch_name)
        target_path = _safe_target_path(
            self._worktrees_root / parent_project_id,
            normalized_name,
        )
        intent = await self._reserve_intent(
            intent_id=intent_id,
            parent_project_id=parent_project_id,
            source_session_id=source_session_id,
            name=normalized_name,
            branch_name=branch_name,
            base_ref=base_ref,
            history_mode=history_mode,
            target_path=target_path,
            now=timestamp,
        )
        if intent.state == WorktreeIntentState.READY:
            try:
                await self._validate_registered_intent(intent, repo_root)
            except (OSError, WorktreeConflict, WorktreeOperationError) as error:
                await self._record_recovery_intervention(
                    intent,
                    error,
                    now=timestamp,
                )
                raise
            if (
                intent.project_id is None
                or intent.thread_id is None
                or intent.sdk_session_id is None
            ):
                raise WorktreeConflict("ready worktree intent is incomplete")
            await self._ensure_worktree_row(intent, repo_root=repo_root, now=timestamp)
            await self._mark_ready(
                intent,
                target=WorktreeTarget(
                    thread_id=intent.thread_id,
                    sdk_session_id=intent.sdk_session_id,
                ),
                now=timestamp,
            )
            return await self._projection_for_intent(intent.intent_id)
        return await self._resume_create(intent, repo_root=repo_root, now=timestamp)

    async def list(self, *, parent_project_id: str) -> list[WorktreeProjection]:
        rows = await self._database.fetchall(
            """
            SELECT intent_id FROM project_worktrees
            WHERE parent_project_id = ?
            ORDER BY created_at, name
            """,
            (parent_project_id,),
        )
        return [await self._projection_for_intent(str(row["intent_id"])) for row in rows]

    async def blockers(self, name: str, *, parent_project_id: str) -> tuple[str, ...]:
        record = await self._worktree_row(name, parent_project_id=parent_project_id)
        async with self._database.transaction() as connection:
            return await _worktree_blockers(
                connection,
                str(record["project_id"]),
                now=time.time(),
            )

    async def close(
        self,
        name: str,
        *,
        parent_project_id: str,
        now: float | None = None,
    ) -> WorktreeProjection:
        timestamp = time.time() if now is None else now
        row = await self._worktree_row(name, parent_project_id=parent_project_id)
        if row["state"] == WorktreeIntentState.CLOSED.value:
            return await self._projection_for_intent(str(row["intent_id"]))
        if row["state"] != WorktreeIntentState.READY.value:
            raise WorktreeConflict(f"worktree can only close from ready state, not {row['state']}")
        intent_id = str(row["intent_id"])
        await self._begin_close(row, now=timestamp)
        repo_root = Path(str(row["repo_root"]))
        target_path = Path(str(row["path"]))
        registration = await self._worktree_metadata(repo_root, target_path)
        if registration is not None:
            _require_owned_worktree(registration, str(row["branch_name"]))
        else:
            await self._finish_close(
                intent_id,
                project_id=str(row["project_id"]),
                now=timestamp,
            )
            return await self._projection_for_intent(intent_id)
        intent = await self._intent(intent_id)
        result = await self._run_owned_git_mutation(
            intent,
            ["git", "worktree", "remove", "--", str(target_path)],
            cwd=repo_root,
            now=timestamp,
            expected_states=(WorktreeIntentState.CLOSING,),
        )
        if (
            result.returncode != 0
            and await self._worktree_metadata(
                repo_root,
                target_path,
            )
            is not None
        ):
            await self._set_close_state(
                intent_id,
                WorktreeIntentState.CLOSE_UNKNOWN,
                now=timestamp,
                error_code="git_worktree_remove_failed",
                error_detail=result.stderr.strip(),
            )
            raise WorktreeOperationError(
                result.stderr.strip() or "git worktree remove failed",
                outcome_unknown=True,
            )
        await self._finish_close(intent_id, project_id=str(row["project_id"]), now=timestamp)
        return await self._projection_for_intent(intent_id)

    async def recover(self, *, now: float | None = None) -> WorktreeRecoveryReport:
        schedule_retries = now is None
        timestamp = time.time() if now is None else now
        await self._ensure_process_generation(now=timestamp)
        recovery_id = str(uuid.uuid4())
        rows = await self._database.fetchall(
            """
            SELECT * FROM worktree_intents
            WHERE state NOT IN ('closed', 'failed', 'compensated')
            ORDER BY created_at, intent_id
            """
        )
        recovered = 0
        orphaned = 0
        for row in rows:
            intent = _row_to_intent(row)
            try:
                await self._ensure_no_live_git_child(intent, now=timestamp)
                parent = await self._projects.project_by_id(intent.parent_project_id)
                repo_root = await self._repo_root(parent.root_path)
                if (
                    intent.state == WorktreeIntentState.CLOSE_UNKNOWN
                    and intent.error_code == "compensation_remove_unknown"
                ):
                    await self._resume_compensation(
                        intent,
                        repo_root=repo_root,
                        now=timestamp,
                    )
                    recovered += 1
                elif intent.state in {
                    WorktreeIntentState.CLOSING,
                    WorktreeIntentState.CLOSE_UNKNOWN,
                }:
                    registration = await self._worktree_metadata(
                        repo_root,
                        intent.target_path,
                    )
                    if registration is not None:
                        _require_owned_worktree(registration, intent.branch_name)
                        result = await self._run_owned_git_mutation(
                            intent,
                            [
                                "git",
                                "worktree",
                                "remove",
                                "--",
                                str(intent.target_path),
                            ],
                            cwd=repo_root,
                            now=timestamp,
                            expected_states=(
                                WorktreeIntentState.CLOSING,
                                WorktreeIntentState.CLOSE_UNKNOWN,
                            ),
                        )
                        if (
                            result.returncode != 0
                            and await self._worktree_metadata(
                                repo_root,
                                intent.target_path,
                            )
                            is not None
                        ):
                            orphaned += 1
                            continue
                    if intent.project_id is not None:
                        await self._finish_close(
                            intent.intent_id,
                            project_id=intent.project_id,
                            now=timestamp,
                        )
                        recovered += 1
                elif intent.state == WorktreeIntentState.COMPENSATING:
                    await self._resume_compensation(
                        intent,
                        repo_root=repo_root,
                        now=timestamp,
                    )
                    recovered += 1
                elif intent.state == WorktreeIntentState.TARGET_UNKNOWN:
                    await self._validate_registered_intent(intent, repo_root)
                    target = await self._reconcile_target(intent)
                    if target is None or intent.project_id is None:
                        orphaned += 1
                        continue
                    await self._ensure_worktree_row(intent, repo_root=repo_root, now=timestamp)
                    await self._mark_ready(intent, target=target, now=timestamp)
                    recovered += 1
                elif intent.state == WorktreeIntentState.READY:
                    await self._validate_registered_intent(intent, repo_root)
                    if (
                        intent.project_id is None
                        or intent.thread_id is None
                        or intent.sdk_session_id is None
                    ):
                        orphaned += 1
                        continue
                    await self._ensure_worktree_row(intent, repo_root=repo_root, now=timestamp)
                    await self._mark_ready(
                        intent,
                        target=WorktreeTarget(
                            thread_id=intent.thread_id,
                            sdk_session_id=intent.sdk_session_id,
                        ),
                        now=timestamp,
                    )
                    recovered += 1
                elif intent.state == WorktreeIntentState.GIT_CREATING:
                    await self._resume_create(
                        intent,
                        repo_root=repo_root,
                        now=timestamp,
                    )
                    recovered += 1
                else:
                    await self._resume_create(intent, repo_root=repo_root, now=timestamp)
                    recovered += 1
            except WorktreeOperationError as error:
                if error.outcome_unknown:
                    await self._record_recovery_intervention(
                        intent,
                        error,
                        now=timestamp,
                    )
                    orphaned += 1
                else:
                    latest = await self._intent(intent.intent_id)
                    if latest.state in {
                        WorktreeIntentState.FAILED,
                        WorktreeIntentState.COMPENSATED,
                        WorktreeIntentState.CLOSED,
                    }:
                        recovered += 1
                    else:
                        await self._record_recovery_intervention(
                            intent,
                            error,
                            now=timestamp,
                        )
                        orphaned += 1
            except (
                OSError,
                ProjectConfigError,
                ProjectPathError,
                WorktreeInputError,
            ) as error:
                await self._record_recovery_intervention(
                    intent,
                    error,
                    now=timestamp,
                )
                orphaned += 1
            except WorktreeRetryPending as pending:
                if schedule_retries:
                    self._schedule_recovery_retry(pending)
                orphaned += 1
            except WorktreeConflict as error:
                await self._record_recovery_intervention(
                    intent,
                    error,
                    now=timestamp,
                )
                orphaned += 1
        await self._database.execute(
            """
            INSERT INTO worktree_recovery_runs(
                recovery_id, started_at, completed_at, examined_intents,
                recovered_intents, orphaned_intents, detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recovery_id,
                timestamp,
                timestamp,
                len(rows),
                recovered,
                orphaned,
                state_only_json(
                    {
                        "examined": len(rows),
                        "recovered": recovered,
                        "orphaned": orphaned,
                    }
                ),
            ),
        )
        return WorktreeRecoveryReport(
            recovery_id=recovery_id,
            examined_intents=len(rows),
            recovered_intents=recovered,
            orphaned_intents=orphaned,
        )

    def _schedule_recovery_retry(self, pending: WorktreeRetryPending) -> None:
        existing = self._recovery_retry_tasks.get(pending.intent_id)
        if existing is not None and not existing.done():
            return

        async def retry() -> None:
            await asyncio.sleep(max(0, pending.retry_at - time.time()))
            if self._recovery_retry_tasks.get(pending.intent_id) is asyncio.current_task():
                self._recovery_retry_tasks.pop(pending.intent_id, None)
            await self.recover(now=max(time.time(), pending.retry_at))

        if self._task_registry is None:
            task = asyncio.create_task(
                retry(),
                name=f"worktree-recovery-retry:{pending.intent_id}",
            )
        else:
            task = self._task_registry.create(
                retry(),
                name=f"worktree-recovery-retry:{pending.intent_id}",
                source="worktree-recovery",
            )
        self._recovery_retry_tasks[pending.intent_id] = task

        def clear(completed: asyncio.Task[None]) -> None:
            if self._recovery_retry_tasks.get(pending.intent_id) is completed:
                self._recovery_retry_tasks.pop(pending.intent_id, None)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(clear)

    async def _resume_create(
        self,
        intent: WorktreeIntent,
        *,
        repo_root: Path,
        now: float,
    ) -> WorktreeProjection:
        if intent.state not in {
            WorktreeIntentState.RESERVED,
            WorktreeIntentState.GIT_CREATING,
        }:
            registration = await self._worktree_metadata(
                repo_root,
                intent.target_path,
            )
            if registration is None:
                raise WorktreeOperationError(
                    "durable worktree path is no longer registered in its repository",
                    outcome_unknown=True,
                )
            _require_owned_worktree(registration, intent.branch_name)
        if intent.state in {
            WorktreeIntentState.RESERVED,
            WorktreeIntentState.GIT_CREATING,
        }:
            intent = await self._create_git_worktree(intent, repo_root=repo_root, now=now)
        project_id = intent.project_id or str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:worktree-project:{intent.intent_id}")
        )
        if intent.state == WorktreeIntentState.GIT_CREATED:
            try:
                project = await self._projects.register_worktree_project(
                    parent_project_id=intent.parent_project_id,
                    project_id=project_id,
                    path=intent.target_path,
                    now=now,
                )
            except ProjectConfigError as error:
                intent = await self._mark_intent(
                    intent.intent_id,
                    WorktreeIntentState.COMPENSATING,
                    now=now,
                    error_code="parent_lifecycle_race",
                    error_detail=str(error),
                )
                await self._resume_compensation(intent, repo_root=repo_root, now=now)
                raise WorktreeConflict(
                    "parent project changed while registering worktree"
                ) from error
            intent = await self._mark_intent(
                intent.intent_id,
                WorktreeIntentState.PROJECT_REGISTERED,
                project_id=project_id,
                now=now,
            )
            await self._ensure_worktree_row(intent, repo_root=repo_root, now=now)
        else:
            project = await self._projects.project_by_id(project_id)
        if intent.state in {
            WorktreeIntentState.PROJECT_REGISTERED,
            WorktreeIntentState.TARGET_CREATING,
        }:
            await self._ensure_worktree_row(intent, repo_root=repo_root, now=now)
            recovered_target_creation = intent.state == WorktreeIntentState.TARGET_CREATING
            if recovered_target_creation and intent.history_mode == WorktreeHistoryMode.FORK:
                target = await self._reconcile_target(intent)
                if target is None:
                    await self._mark_intent(
                        intent.intent_id,
                        WorktreeIntentState.TARGET_UNKNOWN,
                        now=now,
                        error_code="fork_outcome_unknown",
                    )
                    await self._set_worktree_state(
                        intent.intent_id,
                        WorktreeIntentState.TARGET_UNKNOWN,
                        now=now,
                    )
                    raise WorktreeOperationError(
                        "history fork outcome is unknown; automatic retry is forbidden",
                        outcome_unknown=True,
                    )
                await self._mark_ready(intent, target=target, now=now)
                return await self._projection_for_intent(intent.intent_id)

            try:
                intent = await self._mark_intent(
                    intent.intent_id,
                    WorktreeIntentState.TARGET_CREATING,
                    now=now,
                )
            except WorktreeConflict:
                latest = await self._intent(intent.intent_id)
                if latest.state == WorktreeIntentState.READY:
                    return await self._projection_for_intent(latest.intent_id)
                if latest.state == WorktreeIntentState.TARGET_CREATING:
                    target = await self._reconcile_target(latest)
                    if target is not None:
                        await self._mark_ready(latest, target=target, now=now)
                        return await self._projection_for_intent(latest.intent_id)
                    raise WorktreeOperationError(
                        "another owner is creating the worktree target; "
                        "automatic duplicate creation is forbidden",
                        outcome_unknown=True,
                    ) from None
                raise
            config = await self._projects.config_snapshot(project)
            preallocated = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:worktree:{intent.intent_id}:session",
                )
            )
            try:
                if intent.history_mode == WorktreeHistoryMode.NONE:
                    target = await self._adapter.create_blank(
                        intent=intent,
                        project=project,
                        config_snapshot=config,
                        preallocated_session_id=preallocated,
                    )
                else:
                    target = await self._adapter.create_history_fork(
                        intent=intent,
                        project=project,
                        config_snapshot=config,
                    )
            except WorktreeOperationError as error:
                if error.outcome_unknown:
                    await self._mark_intent(
                        intent.intent_id,
                        WorktreeIntentState.TARGET_UNKNOWN,
                        now=now,
                        error_code="target_creation_unknown",
                        error_detail=str(error),
                    )
                    await self._set_worktree_state(
                        intent.intent_id,
                        WorktreeIntentState.TARGET_UNKNOWN,
                        now=now,
                    )
                    raise
                await self._compensate(intent, repo_root=repo_root, now=now, detail=str(error))
                raise
            except Exception as error:
                await self._mark_intent(
                    intent.intent_id,
                    WorktreeIntentState.TARGET_UNKNOWN,
                    now=now,
                    error_code=type(error).__name__,
                    error_detail=str(error),
                )
                await self._set_worktree_state(
                    intent.intent_id,
                    WorktreeIntentState.TARGET_UNKNOWN,
                    now=now,
                )
                raise WorktreeOperationError(str(error), outcome_unknown=True) from error
            intent = await self._mark_ready(intent, target=target, now=now)
        return await self._projection_for_intent(intent.intent_id)

    async def _validate_registered_intent(
        self,
        intent: WorktreeIntent,
        repo_root: Path,
    ) -> GitWorktreeMetadata:
        registration = await self._worktree_metadata(
            repo_root,
            intent.target_path,
        )
        if registration is None:
            raise WorktreeOperationError(
                "durable worktree path is no longer registered in its repository",
                outcome_unknown=True,
            )
        _require_owned_worktree(registration, intent.branch_name)
        return registration

    async def _reconcile_target(
        self,
        intent: WorktreeIntent,
    ) -> WorktreeTarget | None:
        try:
            return await self._adapter.reconcile_target(intent)
        except Exception as error:
            raise WorktreeOperationError(
                "worktree target reconciliation failed",
                outcome_unknown=True,
            ) from error

    async def _create_git_worktree(
        self,
        intent: WorktreeIntent,
        *,
        repo_root: Path,
        now: float,
    ) -> WorktreeIntent:
        intent = await self._claim_git_creation(intent.intent_id, now=now)
        holder = intent.git_create_holder
        if holder is None:
            raise WorktreeConflict("worktree Git creation claim has no holder")
        fence_token = intent.git_create_fence_token
        registration = await self._worktree_metadata(repo_root, intent.target_path)
        if registration is not None:
            try:
                _require_owned_worktree(registration, intent.branch_name)
            except WorktreeConflict as error:
                await self._settle_git_creation(
                    intent.intent_id,
                    holder=holder,
                    fence_token=fence_token,
                    state=WorktreeIntentState.FAILED,
                    now=now,
                    error_code="worktree_path_conflict",
                    error_detail=str(error),
                )
                raise
            return await self._settle_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                state=WorktreeIntentState.GIT_CREATED,
                now=now,
                created_branch=True,
            )
        path_collision = await asyncio.to_thread(
            _find_casefold_path_collision,
            intent.target_path,
        )
        if path_collision is not None or intent.target_path.exists():
            detail = (
                f"worktree path conflicts with existing path: "
                f"{path_collision or intent.target_path}"
            )
            await self._settle_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                state=WorktreeIntentState.FAILED,
                now=now,
                error_code="worktree_path_conflict",
                error_detail=detail,
            )
            raise WorktreeConflict(detail)
        branch = await self._git.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{intent.branch_name}"],
            cwd=repo_root,
        )
        if branch.returncode == 0:
            detail = (
                f"worktree branch already exists and is not owned by this intent: "
                f"{intent.branch_name}"
            )
            await self._settle_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                state=WorktreeIntentState.FAILED,
                now=now,
                error_code="worktree_branch_conflict",
                error_detail=detail,
            )
            raise WorktreeConflict(detail)
        intent.target_path.parent.mkdir(parents=True, exist_ok=True)
        result, side_effect_now = await self._run_git_creation_side_effect(
            intent,
            holder=holder,
            fence_token=fence_token,
            now=now,
            argv=[
                "git",
                "worktree",
                "add",
                "-b",
                intent.branch_name,
                "--",
                str(intent.target_path),
                intent.base_ref,
            ],
            cwd=repo_root,
        )
        registration = await self._worktree_metadata(repo_root, intent.target_path)
        if result.returncode != 0 and registration is None:
            command_path_collision = await asyncio.to_thread(
                _find_casefold_path_collision,
                intent.target_path,
            )
            command_branch = await self._git.run(
                [
                    "git",
                    "show-ref",
                    "--verify",
                    "--quiet",
                    f"refs/heads/{intent.branch_name}",
                ],
                cwd=repo_root,
            )
            if command_path_collision is not None or intent.target_path.exists():
                detail = (
                    "worktree path conflicted while Git creation was in progress: "
                    f"{command_path_collision or intent.target_path}"
                )
                await self._settle_git_creation(
                    intent.intent_id,
                    holder=holder,
                    fence_token=fence_token,
                    state=WorktreeIntentState.FAILED,
                    now=side_effect_now,
                    error_code="worktree_path_conflict",
                    error_detail=detail,
                )
                raise WorktreeConflict(detail)
            if command_branch.returncode == 0:
                detail = (
                    "worktree branch appeared while Git creation was in progress: "
                    f"{intent.branch_name}"
                )
                await self._settle_git_creation(
                    intent.intent_id,
                    holder=holder,
                    fence_token=fence_token,
                    state=WorktreeIntentState.FAILED,
                    now=side_effect_now,
                    error_code="worktree_branch_conflict",
                    error_detail=detail,
                )
                raise WorktreeConflict(detail)
            await self._settle_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                state=WorktreeIntentState.FAILED,
                now=side_effect_now,
                error_code="git_worktree_add_failed",
                error_detail=result.stderr.strip(),
            )
            raise WorktreeOperationError(
                result.stderr.strip() or "git worktree add failed",
                outcome_unknown=False,
            )
        if registration is not None:
            try:
                _require_owned_worktree(registration, intent.branch_name)
            except WorktreeConflict as error:
                await self._settle_git_creation(
                    intent.intent_id,
                    holder=holder,
                    fence_token=fence_token,
                    state=WorktreeIntentState.FAILED,
                    now=side_effect_now,
                    error_code="worktree_path_conflict",
                    error_detail=str(error),
                )
                raise
        return await self._settle_git_creation(
            intent.intent_id,
            holder=holder,
            fence_token=fence_token,
            state=WorktreeIntentState.GIT_CREATED,
            now=side_effect_now,
            created_branch=True,
        )

    async def _ensure_process_generation(self, *, now: float) -> int:
        if self._process_generation is not None:
            return self._process_generation
        async with self._process_generation_lock:
            if self._process_generation is not None:
                return self._process_generation
            async with self._database.transaction() as connection:
                state = await _fetchone(
                    connection,
                    """
                    SELECT process_generation FROM worktree_process_state
                    WHERE singleton = 1
                    """,
                    (),
                )
                if state is None:
                    raise RuntimeError("worktree process state is not initialized")
                generation = int(state["process_generation"]) + 1
                await connection.execute(
                    """
                    UPDATE worktree_process_state
                    SET process_owner_id = ?, process_generation = ?, started_at = ?
                    WHERE singleton = 1
                    """,
                    (self._process_owner_id, generation, now),
                )
                stale = await _fetchall(
                    connection,
                    """
                    SELECT intent_id, git_create_lease_expires_at
                    FROM worktree_intents
                    WHERE state = 'git_creating'
                      AND (
                          git_create_process_generation IS NULL
                          OR git_create_process_generation != ?
                      )
                    """,
                    (generation,),
                )
                for row in stale:
                    retry_at = (
                        now
                        if row["git_create_lease_expires_at"] is None
                        else max(now, float(row["git_create_lease_expires_at"]))
                    )
                    await connection.execute(
                        """
                        UPDATE worktree_intents
                        SET git_create_holder = NULL, git_create_retry_at = ?,
                            error_code = 'git_owner_generation_stale',
                            error_detail = NULL, updated_at = ?
                        WHERE intent_id = ? AND state = 'git_creating'
                        """,
                        (
                            retry_at,
                            now,
                            row["intent_id"],
                        ),
                    )
                    await connection.execute(
                        """
                        INSERT INTO worktree_events(
                            event_id, intent_id, state, detail, created_at
                        ) VALUES (?, ?, 'git_creating', ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            row["intent_id"],
                            state_only_json(
                                {
                                    "event": "git_owner_generation_invalidated",
                                    "process_generation": generation,
                                    "retry_at": retry_at,
                                }
                            ),
                            now,
                        ),
                    )
            self._process_generation = generation
            return generation

    async def _claim_git_creation(
        self,
        intent_id: str,
        *,
        now: float,
    ) -> WorktreeIntent:
        process_generation = await self._ensure_process_generation(now=now)
        current = await self._intent(intent_id)
        await self._ensure_no_live_git_child(current, now=now)
        holder = uuid.uuid4().hex
        async with self._database.transaction() as connection:
            draining = await _fetchone(
                connection,
                "SELECT value FROM global_config WHERE key = 'restart_draining'",
                (),
            )
            if draining is not None and draining["value"] == "1":
                raise WorktreeConflict("copilotD is draining for restart")
            process = await _fetchone(
                connection,
                """
                SELECT 1 FROM worktree_process_state
                WHERE singleton = 1 AND process_owner_id = ?
                  AND process_generation = ?
                """,
                (self._process_owner_id, process_generation),
            )
            if process is None:
                raise WorktreeConflict("worktree process generation is no longer current")
            row = await _fetchone(
                connection,
                "SELECT * FROM worktree_intents WHERE intent_id = ?",
                (intent_id,),
            )
            if row is None:
                raise WorktreeConflict("worktree intent disappeared before Git creation")
            state = WorktreeIntentState(str(row["state"]))
            lease_expires_at = row["git_create_lease_expires_at"]
            retry_at = row["git_create_retry_at"]
            retry_boundary = (
                float(retry_at)
                if retry_at is not None
                else (now if lease_expires_at is None else float(lease_expires_at))
            )
            active_holder = (
                row["git_create_process_generation"] == process_generation
                and row["git_create_holder"] in self._active_git_holders
            )
            claimable = state == WorktreeIntentState.RESERVED or (
                state == WorktreeIntentState.GIT_CREATING
                and retry_boundary <= now
                and not active_holder
            )
            if not claimable:
                if active_holder and retry_boundary <= now:
                    retry_boundary = now + self._git_create_lease_seconds / 3
                raise WorktreeRetryPending(intent_id, retry_boundary)
            fence_token = int(row["git_create_fence_token"]) + 1
            updated = await connection.execute(
                """
                UPDATE worktree_intents
                SET state = 'git_creating', git_create_holder = ?,
                    git_create_fence_token = ?,
                    git_create_lease_expires_at = ?,
                    git_create_process_generation = ?,
                    git_create_retry_at = NULL,
                    error_code = NULL, error_detail = NULL, updated_at = ?
                WHERE intent_id = ? AND state = ?
                  AND git_create_fence_token = ?
                """,
                (
                    holder,
                    fence_token,
                    now + self._git_create_lease_seconds,
                    process_generation,
                    now,
                    intent_id,
                    state.value,
                    int(row["git_create_fence_token"]),
                ),
            )
            if updated.rowcount != 1:
                await updated.close()
                raise WorktreeConflict("worktree Git creation lease was claimed concurrently")
            await updated.close()
            await connection.execute(
                """
                INSERT INTO worktree_events(event_id, intent_id, state, detail, created_at)
                VALUES (?, ?, 'git_creating', ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    intent_id,
                    state_only_json(
                        {
                            "git_create_holder": holder,
                            "git_create_fence_token": fence_token,
                            "git_create_process_generation": process_generation,
                        }
                    ),
                    now,
                ),
            )
            claimed = await _fetchone(
                connection,
                "SELECT * FROM worktree_intents WHERE intent_id = ?",
                (intent_id,),
            )
            assert claimed is not None
            return _row_to_intent(claimed)

    async def _renew_git_creation(
        self,
        intent_id: str,
        *,
        holder: str,
        fence_token: int,
        now: float,
    ) -> None:
        process_generation = self._process_generation
        if process_generation is None:
            raise WorktreeConflict("worktree process generation is not initialized")
        changed = await self._database.execute_count(
            """
            UPDATE worktree_intents
            SET git_create_lease_expires_at = ?, updated_at = ?
            WHERE intent_id = ? AND state = 'git_creating'
              AND git_create_holder = ? AND git_create_fence_token = ?
              AND git_create_process_generation = ?
              AND git_create_lease_expires_at > ?
              AND EXISTS (
                  SELECT 1 FROM worktree_process_state p
                  WHERE p.singleton = 1
                    AND p.process_owner_id = ?
                    AND p.process_generation = ?
              )
            """,
            (
                now + self._git_create_lease_seconds,
                now,
                intent_id,
                holder,
                fence_token,
                process_generation,
                now,
                self._process_owner_id,
                process_generation,
            ),
        )
        if changed != 1:
            raise WorktreeConflict("worktree Git creation lease is no longer current")

    async def _run_git_creation_side_effect(
        self,
        intent: WorktreeIntent,
        *,
        holder: str,
        fence_token: int,
        now: float,
        argv: list[str],
        cwd: Path,
    ) -> tuple[GitCommandResult, float]:
        loop = asyncio.get_running_loop()
        monotonic_started = loop.time()

        def current_time() -> float:
            return now + (loop.time() - monotonic_started)

        self._active_git_holders.add(holder)
        try:
            await self._renew_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                now=current_time(),
            )
            renewal_interval = max(
                0.01,
                min(30.0, self._git_create_lease_seconds / 3),
            )

            async def renew() -> None:
                while True:
                    await asyncio.sleep(renewal_interval)
                    try:
                        await self._renew_git_creation(
                            intent.intent_id,
                            holder=holder,
                            fence_token=fence_token,
                            now=current_time(),
                        )
                    except WorktreeConflict:
                        return

            renewal = asyncio.create_task(
                renew(),
                name=f"worktree-git-renew:{intent.intent_id}",
            )
            try:
                result = await self._run_owned_git_mutation(
                    intent,
                    argv,
                    cwd=cwd,
                    now=current_time(),
                    expected_states=(WorktreeIntentState.GIT_CREATING,),
                    holder=holder,
                    fence_token=fence_token,
                )
            finally:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            completed_at = current_time()
            await self._renew_git_creation(
                intent.intent_id,
                holder=holder,
                fence_token=fence_token,
                now=completed_at,
            )
            return result, completed_at
        finally:
            self._active_git_holders.discard(holder)

    async def _run_owned_git_mutation(
        self,
        intent: WorktreeIntent,
        argv: list[str],
        *,
        cwd: Path,
        now: float,
        expected_states: tuple[WorktreeIntentState, ...],
        holder: str | None = None,
        fence_token: int | None = None,
    ) -> GitCommandResult:
        if not isinstance(self._git, SubprocessGitRunner):
            return await self._git.run(argv, cwd=cwd)
        process_generation = await self._ensure_process_generation(now=now)
        latest = await self._intent(intent.intent_id)
        await self._ensure_no_live_git_child(latest, now=now)
        child_token = uuid.uuid4().hex

        async def record_started(pid: int) -> None:
            async with self._database.transaction() as connection:
                draining = await _fetchone(
                    connection,
                    "SELECT value FROM global_config WHERE key = 'restart_draining'",
                    (),
                )
                if draining is not None and draining["value"] == "1":
                    raise WorktreeConflict("copilotD is draining for restart")
                process = await _fetchone(
                    connection,
                    """
                    SELECT 1 FROM worktree_process_state
                    WHERE singleton = 1 AND process_owner_id = ?
                      AND process_generation = ?
                    """,
                    (self._process_owner_id, process_generation),
                )
                if process is None:
                    raise WorktreeConflict("worktree process generation is no longer current")
                placeholders = ", ".join("?" for _ in expected_states)
                parameters: list[object] = [
                    pid,
                    child_token,
                    process_generation,
                    now,
                    now,
                    intent.intent_id,
                    *(state.value for state in expected_states),
                ]
                holder_clause = ""
                if holder is not None:
                    if fence_token is None:
                        raise ValueError("owned Git creation requires its fence token")
                    holder_clause = (
                        " AND git_create_holder = ?"
                        " AND git_create_fence_token = ?"
                        " AND git_create_process_generation = ?"
                    )
                    parameters.extend((holder, fence_token, process_generation))
                updated = await connection.execute(
                    f"""
                    UPDATE worktree_intents
                    SET git_child_pid = ?, git_child_token = ?,
                        git_child_process_generation = ?,
                        git_child_started_at = ?, updated_at = ?
                    WHERE intent_id = ? AND state IN ({placeholders})
                      AND git_child_token IS NULL
                      {holder_clause}
                    """,
                    tuple(parameters),
                )
                if updated.rowcount != 1:
                    await updated.close()
                    raise WorktreeConflict("worktree mutation lost its durable child-process fence")
                await updated.close()

        try:
            return await self._git.run_owned(
                argv,
                cwd=cwd,
                lock_path=self._git_child_lock_path(intent.intent_id),
                on_started=record_started,
            )
        except _OwnedGitMutationBusy:
            retry_at = time.time() + min(1.0, self._git_create_lease_seconds / 3)
            if holder is not None and fence_token is not None:
                await asyncio.shield(
                    self._abandon_git_creation(
                        intent.intent_id,
                        holder=holder,
                        fence_token=fence_token,
                        process_generation=process_generation,
                        now=time.time(),
                    )
                )
            await self._database.execute(
                """
                UPDATE worktree_intents
                SET git_create_retry_at = CASE
                        WHEN state = 'git_creating' THEN ?
                        ELSE git_create_retry_at
                    END,
                    updated_at = ?
                WHERE intent_id = ?
                """,
                (retry_at, time.time(), intent.intent_id),
            )
            pending = WorktreeRetryPending(intent.intent_id, retry_at)
            self._schedule_recovery_retry(pending)
            raise pending from None
        except BaseException:
            if holder is not None and fence_token is not None:
                await asyncio.shield(
                    self._abandon_git_creation(
                        intent.intent_id,
                        holder=holder,
                        fence_token=fence_token,
                        process_generation=process_generation,
                        now=time.time(),
                    )
                )
            raise
        finally:
            await asyncio.shield(
                self._clear_git_child(
                    intent.intent_id,
                    child_token=child_token,
                    now=time.time(),
                )
            )

    async def _ensure_no_live_git_child(
        self,
        intent: WorktreeIntent,
        *,
        now: float,
    ) -> None:
        lock_path = self._git_child_lock_path(intent.intent_id)
        if _git_lock_is_held(lock_path):
            retry_at = now + min(1.0, self._git_create_lease_seconds / 3)
            await self._database.execute(
                """
                UPDATE worktree_intents
                SET git_create_retry_at = CASE
                        WHEN state = 'git_creating' THEN ?
                        ELSE git_create_retry_at
                    END,
                    updated_at = ?
                WHERE intent_id = ?
                """,
                (retry_at, now, intent.intent_id),
            )
            raise WorktreeRetryPending(intent.intent_id, retry_at)
        if intent.git_child_token is not None:
            await self._database.execute(
                """
                UPDATE worktree_intents
                SET git_child_pid = NULL, git_child_token = NULL,
                    git_child_process_generation = NULL,
                    git_child_started_at = NULL,
                    git_create_retry_at = CASE
                        WHEN state = 'git_creating' AND git_create_holder IS NULL
                        THEN ?
                        ELSE git_create_retry_at
                    END,
                    updated_at = ?
                WHERE intent_id = ? AND git_child_token = ?
                """,
                (now, now, intent.intent_id, intent.git_child_token),
            )

    async def _clear_git_child(
        self,
        intent_id: str,
        *,
        child_token: str,
        now: float,
    ) -> None:
        await self._database.execute(
            """
            UPDATE worktree_intents
            SET git_child_pid = NULL, git_child_token = NULL,
                git_child_process_generation = NULL,
                git_child_started_at = NULL, updated_at = ?
            WHERE intent_id = ? AND git_child_token = ?
            """,
            (now, intent_id, child_token),
        )

    async def _abandon_git_creation(
        self,
        intent_id: str,
        *,
        holder: str,
        fence_token: int,
        process_generation: int,
        now: float,
    ) -> None:
        await self._database.execute(
            """
            UPDATE worktree_intents
            SET git_create_holder = NULL, git_create_lease_expires_at = NULL,
                git_create_retry_at = ?, updated_at = ?
            WHERE intent_id = ? AND state = 'git_creating'
              AND git_create_holder = ? AND git_create_fence_token = ?
              AND git_create_process_generation = ?
            """,
            (now, now, intent_id, holder, fence_token, process_generation),
        )

    def _git_child_lock_path(self, intent_id: str) -> Path:
        return self._worktrees_root / ".git-child-locks" / f"{intent_id}.lock"

    async def _settle_git_creation(
        self,
        intent_id: str,
        *,
        holder: str,
        fence_token: int,
        state: WorktreeIntentState,
        now: float,
        created_branch: bool | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> WorktreeIntent:
        if state not in {WorktreeIntentState.GIT_CREATED, WorktreeIntentState.FAILED}:
            raise ValueError(f"invalid Git creation settlement state: {state}")
        process_generation = self._process_generation
        if process_generation is None:
            raise WorktreeConflict("worktree process generation is not initialized")
        async with self._database.transaction() as connection:
            updated = await connection.execute(
                """
                UPDATE worktree_intents
                SET state = ?, created_branch = COALESCE(?, created_branch),
                    git_create_holder = NULL, git_create_lease_expires_at = NULL,
                    git_create_retry_at = NULL,
                    error_code = ?, error_detail = NULL, updated_at = ?
                WHERE intent_id = ? AND state = 'git_creating'
                  AND git_create_holder = ? AND git_create_fence_token = ?
                  AND git_create_process_generation = ?
                  AND EXISTS (
                      SELECT 1 FROM worktree_process_state p
                      WHERE p.singleton = 1
                        AND p.process_owner_id = ?
                        AND p.process_generation = ?
                  )
                """,
                (
                    state.value,
                    None if created_branch is None else int(created_branch),
                    error_code,
                    now,
                    intent_id,
                    holder,
                    fence_token,
                    process_generation,
                    self._process_owner_id,
                    process_generation,
                ),
            )
            if updated.rowcount != 1:
                await updated.close()
                raise WorktreeConflict("worktree Git creation lease was lost before settlement")
            await updated.close()
            await connection.execute(
                """
                INSERT INTO worktree_events(event_id, intent_id, state, detail, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    intent_id,
                    state.value,
                    state_only_json(
                        {
                            "git_create_holder": holder,
                            "git_create_fence_token": fence_token,
                            "error_code": error_code,
                            "error_detail": error_detail,
                        }
                    ),
                    now,
                ),
            )
            settled = await _fetchone(
                connection,
                "SELECT * FROM worktree_intents WHERE intent_id = ?",
                (intent_id,),
            )
            assert settled is not None
            return _row_to_intent(settled)

    async def _compensate(
        self,
        intent: WorktreeIntent,
        *,
        repo_root: Path,
        now: float,
        detail: str,
    ) -> None:
        await self._mark_intent(
            intent.intent_id,
            WorktreeIntentState.COMPENSATING,
            now=now,
            error_code="target_creation_failed",
            error_detail=detail,
        )
        await self._resume_compensation(intent, repo_root=repo_root, now=now)

    async def _resume_compensation(
        self,
        intent: WorktreeIntent,
        *,
        repo_root: Path,
        now: float,
    ) -> None:
        registration = await self._worktree_metadata(repo_root, intent.target_path)
        if registration is not None:
            _require_owned_worktree(registration, intent.branch_name)
            latest = await self._intent(intent.intent_id)
            result = await self._run_owned_git_mutation(
                latest,
                ["git", "worktree", "remove", "--", str(intent.target_path)],
                cwd=repo_root,
                now=now,
                expected_states=(
                    WorktreeIntentState.COMPENSATING,
                    WorktreeIntentState.CLOSE_UNKNOWN,
                ),
            )
            if (
                result.returncode != 0
                and await self._worktree_metadata(
                    repo_root,
                    intent.target_path,
                )
                is not None
            ):
                await self._mark_intent(
                    intent.intent_id,
                    WorktreeIntentState.CLOSE_UNKNOWN,
                    now=now,
                    error_code="compensation_remove_unknown",
                    error_detail=result.stderr.strip(),
                )
                raise WorktreeOperationError(
                    result.stderr.strip() or "worktree compensation is unknown",
                    outcome_unknown=True,
                )
        await self._finish_compensation(intent, now=now)

    async def _reserve_intent(
        self,
        *,
        intent_id: str,
        parent_project_id: str,
        source_session_id: str | None,
        name: str,
        branch_name: str,
        base_ref: str,
        history_mode: WorktreeHistoryMode,
        target_path: Path,
        now: float,
    ) -> WorktreeIntent:
        async with self._database.transaction() as connection:
            draining = await _fetchone(
                connection,
                "SELECT value FROM global_config WHERE key = 'restart_draining'",
                (),
            )
            if draining is not None and draining["value"] == "1":
                raise WorktreeConflict("copilotD is draining for restart")
            row = await _fetchone(
                connection,
                """
                SELECT * FROM worktree_intents
                WHERE parent_project_id = ? AND name = ?
                """,
                (parent_project_id, name),
            )
            if row is not None:
                intent = _row_to_intent(row)
                if (
                    intent.branch_name != branch_name
                    or intent.base_ref != base_ref
                    or intent.history_mode != history_mode
                    or intent.source_session_id != source_session_id
                ):
                    raise WorktreeConflict(
                        "worktree name was reused with different immutable parameters"
                    )
                if (
                    intent.state == WorktreeIntentState.FAILED
                    and intent.error_code in _RETRYABLE_GIT_PREFLIGHT_ERRORS
                ):
                    await connection.execute(
                        """
                        UPDATE worktree_intents
                        SET state = 'reserved', git_create_holder = NULL,
                            git_create_lease_expires_at = NULL,
                            git_create_process_generation = NULL,
                            git_create_retry_at = NULL,
                            error_code = NULL, error_detail = NULL, updated_at = ?
                        WHERE intent_id = ? AND state = 'failed'
                          AND error_code = ?
                        """,
                        (now, intent.intent_id, intent.error_code),
                    )
                    await connection.execute(
                        """
                        INSERT INTO worktree_events(
                            event_id, intent_id, state, detail, created_at
                        ) VALUES (?, ?, 'reserved', ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            intent.intent_id,
                            state_only_json(
                                {
                                    "event": "retry_known_git_preflight_conflict",
                                    "previous_error_code": intent.error_code,
                                }
                            ),
                            now,
                        ),
                    )
                    reset = await _fetchone(
                        connection,
                        "SELECT * FROM worktree_intents WHERE intent_id = ?",
                        (intent.intent_id,),
                    )
                    assert reset is not None
                    return _row_to_intent(reset)
                return intent
            parent = await _fetchone(
                connection,
                """
                SELECT state FROM projects
                WHERE id = ? AND state IN ('active', 'worktree')
                """,
                (parent_project_id,),
            )
            if parent is None:
                raise WorktreeConflict("parent project is closing or retired")
            await connection.execute(
                """
                INSERT INTO worktree_intents(
                    intent_id, parent_project_id, source_session_id, name,
                    branch_name, base_ref, history_mode, target_path,
                    state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?)
                """,
                (
                    intent_id,
                    parent_project_id,
                    source_session_id,
                    name,
                    branch_name,
                    base_ref,
                    history_mode.value,
                    str(target_path),
                    now,
                    now,
                ),
            )
        return await self._intent(intent_id)

    async def _mark_intent(
        self,
        intent_id: str,
        state: WorktreeIntentState,
        *,
        now: float,
        project_id: str | None = None,
        thread_id: str | None = None,
        sdk_session_id: str | None = None,
        created_branch: bool | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> WorktreeIntent:
        predecessors = _INTENT_PREDECESSORS.get(state)
        if predecessors is None:
            raise ValueError(f"intent state requires a dedicated atomic transition: {state}")
        placeholders = ", ".join("?" for _ in predecessors)
        async with self._database.transaction() as connection:
            updated = await connection.execute(
                f"""
                UPDATE worktree_intents
                SET state = ?, project_id = COALESCE(?, project_id),
                    thread_id = COALESCE(?, thread_id),
                    sdk_session_id = COALESCE(?, sdk_session_id),
                    created_branch = COALESCE(?, created_branch),
                    error_code = ?, error_detail = NULL, updated_at = ?
                WHERE intent_id = ? AND state IN ({placeholders})
                """,
                (
                    state.value,
                    project_id,
                    thread_id,
                    sdk_session_id,
                    None if created_branch is None else int(created_branch),
                    error_code,
                    now,
                    intent_id,
                    *(item.value for item in predecessors),
                ),
            )
            if updated.rowcount != 1:
                await updated.close()
                raise WorktreeConflict(
                    f"worktree intent changed before transition to {state.value}"
                )
            await updated.close()
            await connection.execute(
                """
                INSERT INTO worktree_events(event_id, intent_id, state, detail, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    intent_id,
                    state.value,
                    state_only_json(
                        {
                            "project_id": project_id,
                            "thread_id": thread_id,
                            "sdk_session_id": sdk_session_id,
                            "error_code": error_code,
                            "error_detail": error_detail,
                        }
                    ),
                    now,
                ),
            )
        return await self._intent(intent_id)

    async def _record_recovery_intervention(
        self,
        intent: WorktreeIntent,
        error: Exception,
        *,
        now: float,
    ) -> None:
        async with self._database.transaction() as connection:
            updated = await connection.execute(
                """
                UPDATE worktree_intents
                SET error_code = COALESCE(error_code, 'recovery_intervention'),
                    error_detail = NULL, updated_at = ?
                WHERE intent_id = ? AND state = ?
                """,
                (
                    now,
                    intent.intent_id,
                    intent.state.value,
                ),
            )
            changed = updated.rowcount == 1
            await updated.close()
            if not changed:
                return
            if intent.state in {
                WorktreeIntentState.PROJECT_REGISTERED,
                WorktreeIntentState.TARGET_CREATING,
                WorktreeIntentState.READY,
                WorktreeIntentState.TARGET_UNKNOWN,
            }:
                await connection.execute(
                    """
                    UPDATE project_worktrees
                    SET state = 'intervention', updated_at = ?
                    WHERE intent_id = ? AND state NOT IN ('closed', 'compensated')
                    """,
                    (now, intent.intent_id),
                )
                if intent.project_id is not None:
                    await connection.execute(
                        """
                        UPDATE projects SET state = 'closing', updated_at = ?
                        WHERE id = ? AND state = 'worktree'
                        """,
                        (now, intent.project_id),
                    )
            await connection.execute(
                """
                INSERT INTO worktree_events(
                    event_id, intent_id, state, detail, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    intent.intent_id,
                    intent.state.value,
                    state_only_json(
                        {
                            "event": "recovery_intervention",
                            "error_type": type(error).__name__,
                        }
                    ),
                    now,
                ),
            )

    async def _ensure_worktree_row(
        self,
        intent: WorktreeIntent,
        *,
        repo_root: Path,
        now: float,
    ) -> None:
        assert intent.project_id is not None
        worktree_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:worktree-record:{intent.intent_id}")
        )
        await self._database.execute(
            """
            INSERT INTO project_worktrees(
                worktree_id, intent_id, parent_project_id, project_id, name,
                repo_root, path, branch_name, base_ref, history_mode,
                created_branch, state, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'project_registered', ?, ?)
            ON CONFLICT(intent_id) DO NOTHING
            """,
            (
                worktree_id,
                intent.intent_id,
                intent.parent_project_id,
                intent.project_id,
                intent.name,
                str(repo_root),
                str(intent.target_path),
                intent.branch_name,
                intent.base_ref,
                intent.history_mode.value,
                int(intent.created_branch),
                now,
                now,
            ),
        )

    async def _mark_ready(
        self,
        intent: WorktreeIntent,
        *,
        target: WorktreeTarget,
        now: float,
    ) -> WorktreeIntent:
        if intent.project_id is None:
            raise WorktreeConflict("ready worktree intent has no project")
        async with self._database.transaction() as connection:
            project = await _fetchone(
                connection,
                "SELECT state FROM projects WHERE id = ?",
                (intent.project_id,),
            )
            if project is None or project["state"] != "worktree":
                raise WorktreeConflict("worktree project is closing or retired")
            updated = await connection.execute(
                """
                UPDATE worktree_intents
                SET state = 'ready', thread_id = ?, sdk_session_id = ?,
                    error_code = NULL, error_detail = NULL, updated_at = ?
                WHERE intent_id = ? AND state IN (
                    'project_registered', 'target_creating', 'target_unknown', 'ready'
                )
                """,
                (
                    target.thread_id,
                    target.sdk_session_id,
                    now,
                    intent.intent_id,
                ),
            )
            if updated.rowcount != 1:
                await updated.close()
                raise WorktreeConflict("worktree intent changed before ready commit")
            await updated.close()
            projection = await connection.execute(
                """
                UPDATE project_worktrees
                SET thread_id = ?, sdk_session_id = ?, state = 'ready', updated_at = ?
                WHERE intent_id = ? AND project_id = ?
                """,
                (
                    target.thread_id,
                    target.sdk_session_id,
                    now,
                    intent.intent_id,
                    intent.project_id,
                ),
            )
            if projection.rowcount != 1:
                await projection.close()
                raise WorktreeConflict("worktree projection is missing before ready commit")
            await projection.close()
        return await self._intent(intent.intent_id)

    async def _finish_compensation(self, intent: WorktreeIntent, *, now: float) -> None:
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE worktree_intents
                SET state = 'compensated', error_code = 'target_creation_failed',
                    updated_at = ?
                WHERE intent_id = ? AND state IN ('compensating', 'close_unknown')
                """,
                (now, intent.intent_id),
            )
            await connection.execute(
                """
                UPDATE project_worktrees
                SET state = 'compensated', updated_at = ?
                WHERE intent_id = ?
                """,
                (now, intent.intent_id),
            )
            if intent.project_id is not None:
                await connection.execute(
                    """
                    UPDATE projects SET state = 'retired', updated_at = ?
                    WHERE id = ?
                    """,
                    (now, intent.project_id),
                )

    async def _projection_for_intent(self, intent_id: str) -> WorktreeProjection:
        row = await self._database.fetchone(
            """
            SELECT w.*,
              (SELECT COUNT(*) FROM session_bindings b
               WHERE b.project_id = w.project_id) AS session_count,
              (SELECT COUNT(*) FROM submissions s
               JOIN session_bindings b ON b.sdk_session_id = s.sdk_session_id
               WHERE b.project_id = w.project_id
                 AND s.state IN (
                     'local_queued', 'submitting', 'submitted', 'submitted_unknown',
                     'observed_active', 'loop_idle', 'continuation_expected'
                 )) AS active_submission_count,
              (SELECT COUNT(*) FROM liveness_leases l
               JOIN session_bindings b ON b.sdk_session_id = l.sdk_session_id
               WHERE b.project_id = w.project_id AND l.state = 'active')
                AS active_lease_count,
              (SELECT COUNT(*) FROM schedules sc
               WHERE sc.project_id = w.project_id AND sc.state != 'deleted')
                AS schedule_count,
              (SELECT COUNT(*) FROM session_bindings b
               WHERE b.project_id = w.project_id
                 AND b.runtime_remote_mode IN ('on', 'unknown'))
                AS remote_reference_count
            FROM project_worktrees w WHERE w.intent_id = ?
            """,
            (intent_id,),
        )
        if row is None:
            raise WorktreeConflict(f"worktree record does not exist for intent {intent_id}")
        return WorktreeProjection(
            worktree_id=str(row["worktree_id"]),
            intent_id=str(row["intent_id"]),
            parent_project_id=str(row["parent_project_id"]),
            project_id=str(row["project_id"]),
            name=str(row["name"]),
            path=Path(str(row["path"])),
            branch_name=str(row["branch_name"]),
            base_ref=str(row["base_ref"]),
            history_mode=WorktreeHistoryMode(str(row["history_mode"])),
            thread_id=None if row["thread_id"] is None else str(row["thread_id"]),
            sdk_session_id=(None if row["sdk_session_id"] is None else str(row["sdk_session_id"])),
            state=str(row["state"]),
            session_count=int(row["session_count"]),
            active_submission_count=int(row["active_submission_count"]),
            active_lease_count=int(row["active_lease_count"]),
            schedule_count=int(row["schedule_count"]),
            remote_reference_count=int(row["remote_reference_count"]),
        )

    async def _worktree_row(self, name: str, *, parent_project_id: str) -> Row:
        row = await self._database.fetchone(
            """
            SELECT * FROM project_worktrees
            WHERE parent_project_id = ? AND name = ?
            """,
            (parent_project_id, name),
        )
        if row is None:
            raise WorktreeConflict(f"worktree does not exist: {name}")
        return row

    async def _intent(self, intent_id: str) -> WorktreeIntent:
        row = await self._database.fetchone(
            "SELECT * FROM worktree_intents WHERE intent_id = ?",
            (intent_id,),
        )
        if row is None:
            raise WorktreeConflict(f"worktree intent does not exist: {intent_id}")
        return _row_to_intent(row)

    async def _repo_root(self, cwd: Path) -> Path:
        result = await self._git.run(["git", "rev-parse", "--show-toplevel"], cwd=cwd)
        if result.returncode != 0:
            raise WorktreeInputError(f"project is not a Git repository: {cwd}")
        root = await asyncio.to_thread(_resolve_path, Path(result.stdout.strip()))
        if not await asyncio.to_thread(root.is_dir):
            raise WorktreeInputError(f"Git repository root is unavailable: {root}")
        return root

    async def _require_commit(self, repo_root: Path, base_ref: str) -> None:
        result = await self._git.run(
            ["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"],
            cwd=repo_root,
        )
        if result.returncode != 0:
            raise WorktreeInputError(f"Git base is not a commit: {base_ref}")

    async def _validate_branch(self, repo_root: Path, branch_name: str) -> None:
        result = await self._git.run(
            ["git", "check-ref-format", "--branch", branch_name],
            cwd=repo_root,
        )
        if result.returncode != 0:
            raise WorktreeInputError(f"invalid generated Git branch: {branch_name}")

    async def _worktree_metadata(
        self,
        repo_root: Path,
        target_path: Path,
    ) -> GitWorktreeMetadata | None:
        result = await self._git.run(
            ["git", "worktree", "list", "--porcelain"],
            cwd=repo_root,
        )
        if result.returncode != 0:
            raise WorktreeOperationError(
                result.stderr.strip() or "git worktree inventory failed",
                outcome_unknown=True,
            )
        target = await asyncio.to_thread(_resolve_path, target_path)
        for block in result.stdout.strip().split("\n\n"):
            fields: dict[str, str] = {}
            for line in block.splitlines():
                name, separator, value = line.partition(" ")
                if separator:
                    fields[name] = value
            if "worktree" not in fields:
                continue
            candidate = await asyncio.to_thread(
                _resolve_path,
                Path(fields["worktree"]),
            )
            if candidate == target:
                return GitWorktreeMetadata(
                    path=candidate,
                    branch_ref=fields.get("branch"),
                    head=fields.get("HEAD"),
                )
        return None

    async def _set_close_state(
        self,
        intent_id: str,
        state: WorktreeIntentState,
        *,
        now: float,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> None:
        await self._mark_intent(
            intent_id,
            state,
            now=now,
            error_code=error_code,
            error_detail=error_detail,
        )
        await self._set_worktree_state(intent_id, state, now=now)

    async def _begin_close(self, worktree: Row, *, now: float) -> None:
        intent_id = str(worktree["intent_id"])
        project_id = str(worktree["project_id"])
        async with self._database.transaction() as connection:
            draining = await _fetchone(
                connection,
                "SELECT value FROM global_config WHERE key = 'restart_draining'",
                (),
            )
            if draining is not None and draining["value"] == "1":
                raise WorktreeConflict("copilotD is draining for restart")
            current = await _fetchone(
                connection,
                """
                SELECT w.state AS worktree_state, i.state AS intent_state,
                       p.state AS project_state
                FROM project_worktrees w
                JOIN worktree_intents i ON i.intent_id = w.intent_id
                JOIN projects p ON p.id = w.project_id
                WHERE w.intent_id = ?
                """,
                (intent_id,),
            )
            if current is None or (
                current["worktree_state"] != "ready"
                or current["intent_state"] != "ready"
                or current["project_state"] != "worktree"
            ):
                raise WorktreeConflict("worktree changed before close fencing")
            blockers = await _worktree_blockers(connection, project_id, now=now)
            if blockers:
                raise WorktreeConflict(
                    "worktree has active references",
                    blockers=blockers,
                )
            project = await connection.execute(
                """
                UPDATE projects SET state = 'closing', updated_at = ?
                WHERE id = ? AND state = 'worktree'
                """,
                (now, project_id),
            )
            if project.rowcount != 1:
                await project.close()
                raise WorktreeConflict("worktree project closure fence was lost")
            await project.close()
            await connection.execute(
                """
                UPDATE worktree_intents SET state = 'closing', updated_at = ?
                WHERE intent_id = ? AND state = 'ready'
                """,
                (now, intent_id),
            )
            await connection.execute(
                """
                UPDATE project_worktrees SET state = 'closing', updated_at = ?
                WHERE intent_id = ? AND state = 'ready'
                """,
                (now, intent_id),
            )

    async def _set_worktree_state(
        self,
        intent_id: str,
        state: WorktreeIntentState,
        *,
        now: float,
    ) -> None:
        await self._database.execute(
            """
            UPDATE project_worktrees SET state = ?, updated_at = ?
            WHERE intent_id = ?
            """,
            (state.value, now, intent_id),
        )

    async def _finish_close(self, intent_id: str, *, project_id: str, now: float) -> None:
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE worktree_intents
                SET state = 'closed', updated_at = ?
                WHERE intent_id = ?
                """,
                (now, intent_id),
            )
            await connection.execute(
                """
                UPDATE project_worktrees
                SET state = 'closed', closed_at = COALESCE(closed_at, ?), updated_at = ?
                WHERE intent_id = ?
                """,
                (now, now, intent_id),
            )
            await connection.execute(
                "UPDATE projects SET state = 'retired', updated_at = ? WHERE id = ?",
                (now, project_id),
            )


def _validate_worktree_name(name: str) -> str:
    normalized = " ".join(name.split())
    if not normalized or normalized in {".", ".."}:
        raise WorktreeInputError("worktree name cannot be empty or dot-relative")
    if len(normalized) > 80:
        raise WorktreeInputError("worktree name must be at most 80 characters")
    if any(character in normalized for character in ("/", "\\", "\0")):
        raise WorktreeInputError("worktree name cannot contain path separators")
    if any(ord(character) < 32 for character in normalized):
        raise WorktreeInputError("worktree name cannot contain control characters")
    return normalized


def _validate_base_ref(base_ref: str) -> None:
    if not base_ref or base_ref.startswith("-") or "\0" in base_ref:
        raise WorktreeInputError("Git base ref is invalid")


def _safe_target_path(root: Path, name: str) -> Path:
    resolved_root = _resolve_path(root)
    target = (resolved_root / name).resolve()
    try:
        target.relative_to(resolved_root)
    except ValueError as error:
        raise WorktreeInputError("worktree path escapes the managed root") from error
    return target


def _find_casefold_path_collision(target: Path) -> Path | None:
    parent = target.parent
    if not parent.exists():
        return None
    expected = unicodedata.normalize("NFC", target.name).casefold()
    for child in parent.iterdir():
        if unicodedata.normalize("NFC", child.name).casefold() == expected:
            return child
    return None


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve()


def _branch_name(name: str, intent_id: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-") or "worktree"
    return f"copilotd/{slug[:40]}-{intent_id[:8]}"


def _require_owned_worktree(
    metadata: GitWorktreeMetadata,
    branch_name: str,
) -> None:
    expected = f"refs/heads/{branch_name}"
    if metadata.branch_ref != expected:
        raise WorktreeConflict(
            f"worktree path is registered to foreign branch "
            f"{metadata.branch_ref or 'detached'}, expected {expected}"
        )


def _row_to_intent(row: Row) -> WorktreeIntent:
    return WorktreeIntent(
        intent_id=str(row["intent_id"]),
        parent_project_id=str(row["parent_project_id"]),
        source_session_id=(
            None if row["source_session_id"] is None else str(row["source_session_id"])
        ),
        name=str(row["name"]),
        branch_name=str(row["branch_name"]),
        base_ref=str(row["base_ref"]),
        history_mode=WorktreeHistoryMode(str(row["history_mode"])),
        target_path=Path(str(row["target_path"])),
        project_id=None if row["project_id"] is None else str(row["project_id"]),
        thread_id=None if row["thread_id"] is None else str(row["thread_id"]),
        sdk_session_id=(None if row["sdk_session_id"] is None else str(row["sdk_session_id"])),
        created_branch=bool(row["created_branch"]),
        state=WorktreeIntentState(str(row["state"])),
        error_code=None if row["error_code"] is None else str(row["error_code"]),
        error_detail=None if row["error_detail"] is None else str(row["error_detail"]),
        git_create_holder=(
            None if row["git_create_holder"] is None else str(row["git_create_holder"])
        ),
        git_create_fence_token=int(row["git_create_fence_token"]),
        git_create_lease_expires_at=(
            None
            if row["git_create_lease_expires_at"] is None
            else float(row["git_create_lease_expires_at"])
        ),
        git_create_process_generation=(
            None
            if row["git_create_process_generation"] is None
            else int(row["git_create_process_generation"])
        ),
        git_create_retry_at=(
            None if row["git_create_retry_at"] is None else float(row["git_create_retry_at"])
        ),
        git_child_pid=(None if row["git_child_pid"] is None else int(row["git_child_pid"])),
        git_child_token=(None if row["git_child_token"] is None else str(row["git_child_token"])),
        git_child_process_generation=(
            None
            if row["git_child_process_generation"] is None
            else int(row["git_child_process_generation"])
        ),
        git_child_started_at=(
            None if row["git_child_started_at"] is None else float(row["git_child_started_at"])
        ),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
    )


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


async def _worktree_blockers(
    connection: Connection,
    project_id: str,
    *,
    now: float,
) -> tuple[str, ...]:
    blockers: list[str] = []
    sessions = await _fetchall(
        connection,
        """
        SELECT thread_id, sdk_session_id, binding_intent, attachment_state,
               runtime_remote_mode
        FROM session_bindings WHERE project_id = ?
        ORDER BY thread_id
        """,
        (project_id,),
    )
    for row in sessions:
        if row["binding_intent"] != "closed" or row["attachment_state"] != "absent":
            blockers.append(
                f"session:{row['thread_id']}:{row['binding_intent']}/{row['attachment_state']}"
            )
        if row["runtime_remote_mode"] in {"on", "unknown"}:
            blockers.append(f"remote:{row['thread_id']}:{row['runtime_remote_mode']}")
    leases = await _fetchall(
        connection,
        """
        SELECT l.sdk_session_id, l.owner_id
        FROM session_owner_leases l
        JOIN session_bindings b ON b.sdk_session_id = l.sdk_session_id
        WHERE b.project_id = ? AND l.expires_at > ?
        """,
        (project_id, now),
    )
    blockers.extend(f"owner_lease:{row['sdk_session_id']}:{row['owner_id']}" for row in leases)
    queued = await _fetchall(
        connection,
        """
        SELECT q.id, q.state
        FROM message_queue q
        JOIN session_bindings b ON b.thread_id = q.thread_id
        WHERE b.project_id = ?
          AND q.state NOT IN ('cancelled', 'submitted', 'failed')
        ORDER BY q.position, q.id
        """,
        (project_id,),
    )
    blockers.extend(f"queue:{row['id']}:{row['state']}" for row in queued)
    live = await _fetchall(
        connection,
        """
        SELECT l.sdk_session_id, l.kind, l.source_id
        FROM liveness_leases l
        JOIN session_bindings b ON b.sdk_session_id = l.sdk_session_id
        WHERE b.project_id = ? AND l.state = 'active'
        """,
        (project_id,),
    )
    blockers.extend(
        f"liveness:{row['sdk_session_id']}:{row['kind']}:{row['source_id']}" for row in live
    )
    native = await _fetchall(
        connection,
        """
        SELECT n.sdk_session_id, n.runtime_schedule_id, n.state
        FROM runtime_schedules n
        JOIN session_bindings b ON b.sdk_session_id = n.sdk_session_id
        WHERE b.project_id = ? AND n.state IN ('active', 'unknown')
        """,
        (project_id,),
    )
    blockers.extend(
        f"native_schedule:{row['sdk_session_id']}:{row['runtime_schedule_id']}:{row['state']}"
        for row in native
    )
    schedules = await _fetchall(
        connection,
        """
        SELECT DISTINCT s.id, s.state
        FROM schedules s
        LEFT JOIN session_bindings b ON b.thread_id = s.thread_id
        WHERE s.state != 'deleted'
          AND (s.project_id = ? OR b.project_id = ?)
        """,
        (project_id, project_id),
    )
    blockers.extend(f"schedule:{row['id']}:{row['state']}" for row in schedules)
    creations = await _fetchall(
        connection,
        """
        SELECT creation_token, state FROM session_creation_intents
        WHERE project_id = ?
          AND state NOT IN ('attached', 'failed')
        """,
        (project_id,),
    )
    blockers.extend(f"creation_intent:{row['creation_token']}:{row['state']}" for row in creations)
    children = await _fetchall(
        connection,
        """
        SELECT intent_id, state FROM worktree_intents
        WHERE parent_project_id = ?
          AND state NOT IN ('closed', 'failed', 'compensated')
        """,
        (project_id,),
    )
    blockers.extend(f"child_worktree:{row['intent_id']}:{row['state']}" for row in children)
    return tuple(blockers)
