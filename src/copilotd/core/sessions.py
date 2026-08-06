from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Protocol

from aiosqlite import Row

from copilotd.core.bindings import (
    BindingIntent,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.extensions import (
    ExtensionConfigFileSource,
    ExtensionConfigRepository,
    ExtensionConfigSnapshot,
)
from copilotd.core.projects import (
    ProjectConfigSnapshot,
    ProjectRegistry,
    ProjectSessionConfigSnapshot,
    ProjectSnapshot,
)
from copilotd.core.session_config import SessionConfigSnapshotError, SessionLaunchOptions
from copilotd.core.session_runtime import (
    ClosedSessionRequiresReactivation,
    RuntimeState,
    SessionAttachRejected,
    SessionAttachUnknown,
    SessionRuntime,
)
from copilotd.storage.database import Database

RuntimeFactory = Callable[[SessionBinding], SessionRuntime]
_creation_admitted: ContextVar[bool] = ContextVar(
    "copilotd_creation_admitted",
    default=False,
)


class SessionRegistryNotAccepting(RuntimeError):
    pass


class CreationState(StrEnum):
    RESERVED = "reserved"
    THREAD_CREATED = "thread_created"
    CREATING = "creating"
    ATTACHED = "attached"
    UNKNOWN = "unknown"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class CreationIntent:
    creation_token: str
    source_kind: str
    source_id: str
    project_source: str
    project_id: str | None
    cwd_snapshot: Path
    sdk_session_id: str
    thread_id: str | None
    project_config_snapshot: dict[str, object]
    channel_config_snapshot: dict[str, object]
    layout: str
    project_config_version: int
    channel_config_version: int
    config_snapshot_state: str
    desired_session_config_version: int
    desired_session_config_hash: str | None
    state: CreationState
    project_snapshot_json: str | None
    session_config_snapshot_json: str | None
    worktree_intent_id: str | None


@dataclass(frozen=True, slots=True)
class ThreadReference:
    thread_id: str


class ThreadGateway(Protocol):
    async def find_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        creation_token: str,
    ) -> ThreadReference | None: ...

    async def create_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        name: str,
        creation_token: str,
        layout: str,
    ) -> ThreadReference: ...


class SessionCreationUnknown(RuntimeError):
    pass


@dataclass(slots=True)
class _SourceCreationLock:
    lock: asyncio.Lock
    users: int = 0


@dataclass(slots=True)
class _AttachmentTransition:
    reactivate_requested: bool
    task: asyncio.Task[SessionRuntime] | None = None


class CreationIntentRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def by_source(
        self,
        *,
        source_kind: str,
        source_id: str,
    ) -> CreationIntent | None:
        row = await self._database.fetchone(
            """
            SELECT * FROM session_creation_intents
            WHERE source_kind = ? AND source_id = ?
            """,
            (source_kind, source_id),
        )
        return None if row is None else _row_to_intent(row)

    @property
    def database(self) -> Database:
        return self._database

    async def reserve(
        self,
        *,
        source_kind: str,
        source_id: str,
        project: ProjectSnapshot,
        config: ProjectSessionConfigSnapshot | None = None,
        config_snapshot: ProjectConfigSnapshot | None = None,
        sdk_session_id: str | None = None,
        worktree_intent_id: str | None = None,
        extension_config: ExtensionConfigSnapshot | None = None,
        now: float | None = None,
    ) -> tuple[CreationIntent, bool]:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM session_creation_intents
                WHERE source_kind = ? AND source_id = ?
                """,
                (source_kind, source_id),
            )
            row = await cursor.fetchone()
            await cursor.close()
            draining = await connection.execute(
                "SELECT value FROM global_config WHERE key = 'restart_draining'"
            )
            draining_row = await draining.fetchone()
            await draining.close()
            if draining_row is not None and draining_row["value"] == "1":
                raise RuntimeError("copilotD is draining for restart")
            if project.project_id is not None:
                project_cursor = await connection.execute(
                    "SELECT state, project_kind FROM projects WHERE id = ?",
                    (project.project_id,),
                )
                project_row = await project_cursor.fetchone()
                await project_cursor.close()
                if (
                    project_row is None
                    or project_row["state"] == "closing"
                    or (
                        project_row["project_kind"] == "worktree"
                        and project_row["state"] == "retired"
                    )
                ):
                    raise RuntimeError("session project is closing or retired")
            if row is not None:
                intent = _row_to_intent(row)
                if (
                    intent.project_source != project.source.value
                    or intent.project_id != project.project_id
                    or intent.cwd_snapshot != project.cwd
                ):
                    raise ValueError("creation source was reused with a different project snapshot")
                if sdk_session_id is not None and intent.sdk_session_id != sdk_session_id:
                    raise ValueError("creation source was reused with a different session id")
                return intent, False

            project_json = _project_snapshot_json(project)
            config_json = None if config_snapshot is None else config_snapshot.canonical_json()
            intent = CreationIntent(
                creation_token=uuid.uuid4().hex,
                source_kind=source_kind,
                source_id=source_id,
                project_source=project.source.value,
                project_id=project.project_id,
                cwd_snapshot=project.cwd,
                sdk_session_id=str(uuid.uuid4()) if sdk_session_id is None else sdk_session_id,
                thread_id=None,
                project_config_snapshot={} if config is None else config.project_payload(),
                channel_config_snapshot={} if config is None else config.channel_payload(),
                layout="text" if config is None else config.layout,
                project_config_version=(
                    project.config_version if config is None else config.project_config_version
                ),
                channel_config_version=(1 if config is None else config.channel_config_version),
                config_snapshot_state="verified",
                desired_session_config_version=(
                    1 if extension_config is None else extension_config.version
                ),
                desired_session_config_hash=(
                    None if extension_config is None else extension_config.config_hash
                ),
                state=CreationState.RESERVED,
                project_snapshot_json=project_json,
                session_config_snapshot_json=config_json,
                worktree_intent_id=worktree_intent_id,
            )
            await connection.execute(
                """
                INSERT INTO session_creation_intents(
                    creation_token, source_kind, source_id, project_source,
                    project_id, cwd_snapshot, sdk_session_id,
                    project_config_snapshot, channel_config_snapshot, layout,
                    project_config_version, channel_config_version,
                    config_snapshot_state, state,
                    project_snapshot_json, session_config_snapshot_json,
                    worktree_intent_id,
                    desired_session_config_version, desired_session_config_hash,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.creation_token,
                    intent.source_kind,
                    intent.source_id,
                    intent.project_source,
                    intent.project_id,
                    str(intent.cwd_snapshot),
                    intent.sdk_session_id,
                    json.dumps(intent.project_config_snapshot, sort_keys=True),
                    json.dumps(intent.channel_config_snapshot, sort_keys=True),
                    intent.layout,
                    intent.project_config_version,
                    intent.channel_config_version,
                    intent.config_snapshot_state,
                    intent.state.value,
                    intent.project_snapshot_json,
                    intent.session_config_snapshot_json,
                    intent.worktree_intent_id,
                    intent.desired_session_config_version,
                    intent.desired_session_config_hash,
                    timestamp,
                    timestamp,
                ),
            )
            return intent, True

    async def assert_side_effect_admitted(self, intent: CreationIntent) -> None:
        async with self._database.transaction() as connection:
            draining = await connection.execute(
                "SELECT value FROM global_config WHERE key = 'restart_draining'"
            )
            draining_row = await draining.fetchone()
            await draining.close()
            if draining_row is not None and draining_row["value"] == "1":
                raise RuntimeError("copilotD is draining for restart")
            if intent.project_id is None:
                return
            project = await connection.execute(
                "SELECT state, project_kind FROM projects WHERE id = ?",
                (intent.project_id,),
            )
            project_row = await project.fetchone()
            await project.close()
            if (
                project_row is None
                or project_row["state"] == "closing"
                or (project_row["project_kind"] == "worktree" and project_row["state"] == "retired")
            ):
                raise RuntimeError("session project is closing or retired")

    async def set_thread(
        self,
        intent: CreationIntent,
        *,
        thread_id: str,
        now: float | None = None,
    ) -> CreationIntent:
        if intent.thread_id is not None and intent.thread_id != thread_id:
            raise ValueError("creation intent is already bound to another thread")
        return await self._transition(
            intent,
            state=CreationState.THREAD_CREATED,
            thread_id=thread_id,
            now=now,
        )

    async def mark(
        self,
        intent: CreationIntent,
        state: CreationState,
        *,
        now: float | None = None,
    ) -> CreationIntent:
        return await self._transition(intent, state=state, thread_id=intent.thread_id, now=now)

    async def _transition(
        self,
        intent: CreationIntent,
        *,
        state: CreationState,
        thread_id: str | None,
        now: float | None,
    ) -> CreationIntent:
        timestamp = time.time() if now is None else now
        await self._database.execute(
            """
            UPDATE session_creation_intents
            SET thread_id = ?, state = ?, updated_at = ?
            WHERE creation_token = ?
            """,
            (thread_id, state.value, timestamp, intent.creation_token),
        )
        row = await self._database.fetchone(
            "SELECT * FROM session_creation_intents WHERE creation_token = ?",
            (intent.creation_token,),
        )
        if row is None:
            raise RuntimeError("creation intent disappeared")
        return _row_to_intent(row)


class SessionRegistry:
    def __init__(
        self,
        bindings: SessionBindingRepository,
        runtime_factory: RuntimeFactory,
    ) -> None:
        self._bindings = bindings
        self._runtime_factory = runtime_factory
        self._runtimes: dict[str, SessionRuntime] = {}
        self._accepting = True
        self._admission = asyncio.Condition()
        self._admitted_operations = 0
        self._mutation_lock = asyncio.Lock()
        self._service_quiesced = False
        self._service_quiesce_violations = 0
        self._service_violation_callback: Callable[[str], None] | None = None
        self._active_creations = 0
        self._admission_condition = asyncio.Condition()
        self._transition_lock = asyncio.Lock()
        self._transitions: dict[str, _AttachmentTransition] = {}
        self._transition_cleanup_tasks: set[asyncio.Task[None]] = set()

    def for_thread(self, thread_id: str) -> SessionRuntime | None:
        runtime = self._runtimes.get(thread_id)
        if runtime is not None and (
            getattr(runtime, "state", None) == RuntimeState.TERMINAL
            or getattr(runtime, "_handle_terminal", False)
        ):
            return None
        return runtime

    def register(self, runtime: SessionRuntime) -> None:
        if not self._accepting:
            raise SessionRegistryNotAccepting("session registry is shutting down")
        self._assert_registry_admission()
        self._register_admitted(runtime)

    def _register_admitted(self, runtime: SessionRuntime) -> None:
        thread_id = runtime.binding.thread_id
        existing = self._runtimes.get(thread_id)
        if existing is not None and existing is not runtime:
            raise RuntimeError(f"thread already has a SessionRuntime: {thread_id}")
        self._runtimes[thread_id] = runtime

    def heartbeat_metrics(self) -> tuple[int, int, float | None]:
        depth = 0
        max_lag_ms = 0
        last_callback_at: float | None = None
        for runtime in self._runtimes.values():
            inbox = runtime.inbox
            if inbox is None:
                continue
            depth += inbox.size
            max_lag_ms = max(max_lag_ms, inbox.lag_ms)
            received_at = inbox.last_received_at
            if received_at is not None:
                last_callback_at = (
                    received_at if last_callback_at is None else max(last_callback_at, received_at)
                )
        return depth, max_lag_ms, last_callback_at

    async def replace(self, binding: SessionBinding) -> SessionRuntime:
        async with self._admit():
            async with self._mutation_lock:
                self._assert_registry_admission()
                existing = self._runtimes.pop(binding.thread_id, None)
                if existing is not None:
                    await existing.shutdown()
                runtime = self._runtime_factory(binding)
                self._register_admitted(runtime)
                return runtime

    async def retire(self, thread_id: str) -> None:
        """Remove a terminal runtime so a deleted binding can never be reused."""
        async with self._mutation_lock:
            runtime = self._runtimes.pop(thread_id, None)
        if runtime is not None and runtime.state not in {
            RuntimeState.CLOSED,
            RuntimeState.RECOVERY_UNKNOWN,
            RuntimeState.TERMINAL,
        }:
            await runtime.shutdown()

    async def ensure_attached(
        self,
        binding: SessionBinding,
        *,
        reactivate: bool = False,
    ) -> SessionRuntime:
        current = await self._bindings.by_thread(binding.thread_id)
        if current is None:
            raise RuntimeError("session binding disappeared before attach")
        binding = current
        if binding.binding_intent == BindingIntent.CLOSED and not reactivate:
            raise ClosedSessionRequiresReactivation("closed session requires an explicit resume")
        runtime = self.for_thread(binding.thread_id)
        if (
            runtime is not None
            and runtime.state == RuntimeState.READY
            and binding.binding_intent == BindingIntent.ACTIVE
        ):
            return runtime
        for attempt in range(2):
            async with self._transition_lock:
                transition = self._transitions.get(binding.thread_id)
                if (
                    transition is not None
                    and transition.task is not None
                    and transition.task.done()
                ):
                    self._transitions.pop(binding.thread_id, None)
                    transition = None
                if transition is None:
                    transition = _AttachmentTransition(
                        reactivate_requested=reactivate,
                    )
                    transition.task = asyncio.create_task(
                        self._ensure_attached_transition(
                            binding,
                            transition=transition,
                        ),
                        name=f"session-attach:{binding.thread_id}",
                    )
                    self._transitions[binding.thread_id] = transition
                    transition.task.add_done_callback(
                        partial(
                            self._schedule_transition_cleanup,
                            binding.thread_id,
                            transition,
                        )
                    )
                elif reactivate:
                    transition.reactivate_requested = True
                task = transition.task
                assert task is not None
            try:
                return await asyncio.shield(task)
            except ClosedSessionRequiresReactivation:
                if not reactivate or attempt == 1:
                    raise
            finally:
                if task.done():
                    async with self._transition_lock:
                        if self._transitions.get(binding.thread_id) is transition:
                            self._transitions.pop(binding.thread_id, None)
            binding = await self._bindings.by_thread(binding.thread_id) or binding
        raise AssertionError("explicit reactivation retry did not settle")

    def _schedule_transition_cleanup(
        self,
        thread_id: str,
        transition: _AttachmentTransition,
        task: asyncio.Task[SessionRuntime],
    ) -> None:
        if not task.cancelled():
            task.exception()
        cleanup = asyncio.create_task(
            self._discard_completed_transition(
                thread_id,
                transition,
                task,
            ),
            name=f"session-attach-cleanup:{thread_id}",
        )
        self._transition_cleanup_tasks.add(cleanup)
        cleanup.add_done_callback(self._transition_cleanup_tasks.discard)

    async def _discard_completed_transition(
        self,
        thread_id: str,
        transition: _AttachmentTransition,
        task: asyncio.Task[SessionRuntime],
    ) -> None:
        async with self._transition_lock:
            if (
                task.done()
                and transition.task is task
                and self._transitions.get(thread_id) is transition
            ):
                self._transitions.pop(thread_id, None)

    async def _ensure_attached_transition(
        self,
        binding: SessionBinding,
        *,
        transition: _AttachmentTransition,
    ) -> SessionRuntime:
        async with self.creation_admission():
            current = await self._bindings.by_thread(binding.thread_id)
            if current is None:
                raise RuntimeError("session binding disappeared during attach transition")
            binding = current
            if (
                binding.binding_intent == BindingIntent.CLOSED
                and not transition.reactivate_requested
            ):
                raise ClosedSessionRequiresReactivation(
                    "closed session requires an explicit resume"
                )
            runtime = self.for_thread(binding.thread_id)
            if runtime is None or runtime.state in {
                RuntimeState.CLOSED,
                RuntimeState.FENCED,
                RuntimeState.RECOVERY_UNKNOWN,
                RuntimeState.TERMINAL,
            }:
                runtime = await self.replace(binding)
            if runtime.state == RuntimeState.DETACHED:
                await runtime.attach_resume(reactivate=transition.reactivate_requested)
            if runtime.state != RuntimeState.READY:
                raise RuntimeError(f"session attach settled in {runtime.state}")
            return runtime

    async def eager_resume(self) -> dict[str, str]:
        failures: dict[str, str] = {}
        for binding in await self._bindings.eager_bindings():
            try:
                async with self._admit():
                    runtime = self._runtime_factory(binding)
                    self._register_admitted(runtime)
                    try:
                        await runtime.attach_resume()
                    except Exception as error:
                        failures[binding.thread_id] = str(error)
                        self._runtimes.pop(binding.thread_id, None)
                        try:
                            await runtime.shutdown()
                        except Exception as cleanup_error:
                            failures[binding.thread_id] = (
                                f"{error}; cleanup failed: {cleanup_error}"
                            )
            except SessionRegistryNotAccepting:
                break
        return failures

    async def begin_service_quiesce(
        self,
        on_violation: Callable[[str], None],
        on_loss: Callable[[str], None],
    ) -> None:
        async with self._admission_condition:
            self._service_quiesced = True
            self._service_quiesce_violations = 0
            self._service_violation_callback = on_violation
            while self._active_creations:
                await self._admission_condition.wait()
        begun: list[SessionRuntime] = []
        try:
            for runtime in self._runtimes.values():
                await runtime.begin_service_quiesce(
                    on_violation,
                    on_loss,
                )
                begun.append(runtime)
        except BaseException:
            for runtime in reversed(begun):
                await runtime.end_service_quiesce()
            raise

    async def end_service_quiesce(self) -> None:
        for runtime in self._runtimes.values():
            await runtime.end_service_quiesce()
        async with self._admission_condition:
            self._service_quiesced = False
            self._service_quiesce_violations = 0
            self._service_violation_callback = None
            self._admission_condition.notify_all()

    async def drain_service_quiesce(self) -> None:
        for runtime in self._runtimes.values():
            await runtime.drain_service_quiesce()

    def service_quiesce_metrics(self) -> tuple[int, int]:
        depth = 0
        violations = self._service_quiesce_violations
        for runtime in self._runtimes.values():
            runtime_depth, runtime_violations = runtime.service_quiesce_metrics()
            depth += runtime_depth
            violations += runtime_violations
        return depth, violations

    @asynccontextmanager
    async def creation_admission(self):
        async with self._admission_condition:
            if self._service_quiesced:
                self._record_registry_violation("session_creation")
                raise RuntimeError("session creation is quiesced for service restart")
            self._active_creations += 1
        token = _creation_admitted.set(True)
        try:
            yield
        finally:
            _creation_admitted.reset(token)
            async with self._admission_condition:
                self._active_creations -= 1
                self._admission_condition.notify_all()

    def _assert_registry_admission(self) -> None:
        if self._service_quiesced and not _creation_admitted.get():
            self._record_registry_violation("runtime_registration")
            raise RuntimeError("session runtime admission is quiesced for service restart")

    def _record_registry_violation(self, source: str) -> None:
        self._service_quiesce_violations += 1
        if self._service_violation_callback is not None:
            self._service_violation_callback(source)

    async def close_admission(self) -> None:
        async with self._admission:
            self._accepting = False
            await self._admission.wait_for(lambda: self._admitted_operations == 0)

    async def shutdown(
        self,
        *,
        emergency_session_id: str | None = None,
    ) -> None:
        await self.close_admission()
        async with self._mutation_lock:
            runtimes = list(self._runtimes.values())
            self._runtimes.clear()
        errors: list[Exception] = []
        for runtime in runtimes:
            try:
                await runtime.shutdown(
                    emergency=(emergency_session_id == runtime.binding.sdk_session_id)
                )
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("one or more session runtimes failed to shut down", errors)

    @asynccontextmanager
    async def _admit(self) -> AsyncIterator[None]:
        async with self._admission:
            if not self._accepting:
                raise SessionRegistryNotAccepting("session registry is shutting down")
            self._admitted_operations += 1
        try:
            yield
        finally:
            async with self._admission:
                self._admitted_operations -= 1
                if self._admitted_operations == 0:
                    self._admission.notify_all()


class SessionCreationService:
    def __init__(
        self,
        *,
        projects: ProjectRegistry,
        intents: CreationIntentRepository,
        bindings: SessionBindingRepository,
        sessions: SessionRegistry,
        threads: ThreadGateway,
        extension_configs: ExtensionConfigRepository | None = None,
        extension_config_source: ExtensionConfigFileSource | None = None,
    ) -> None:
        self._projects = projects
        self._intents = intents
        self._bindings = bindings
        self._sessions = sessions
        self._threads = threads
        self._extension_configs = extension_configs
        self._extension_config_source = extension_config_source
        self._source_locks: dict[tuple[str, str], _SourceCreationLock] = {}
        self._source_locks_guard = asyncio.Lock()

    @property
    def projects(self) -> ProjectRegistry:
        return self._projects

    async def create_from_source(
        self,
        *,
        channel_id: str,
        source_kind: str,
        source_id: str,
        prompt: str,
        thread_name: str,
        send_initial_prompt: bool = True,
        project_snapshot: ProjectSnapshot | None = None,
        config_snapshot: ProjectConfigSnapshot | None = None,
        preallocated_session_id: str | None = None,
        worktree_intent_id: str | None = None,
    ) -> SessionRuntime:
        async with self._sessions.creation_admission():
            draining = await self._intents.database.fetchone(
                "SELECT value FROM global_config WHERE key = 'restart_draining'"
            )
            if draining is not None and draining["value"] == "1":
                raise RuntimeError("copilotD is draining for restart")
            source_key = (source_kind, source_id)
            entry = await self._acquire_source_lock(source_key)
            try:
                return await self._create_from_source_locked(
                    channel_id=channel_id,
                    source_kind=source_kind,
                    source_id=source_id,
                    prompt=prompt,
                    thread_name=thread_name,
                    send_initial_prompt=send_initial_prompt,
                    project_snapshot=project_snapshot,
                    config_snapshot=config_snapshot,
                    preallocated_session_id=preallocated_session_id,
                    worktree_intent_id=worktree_intent_id,
                )
            finally:
                await self._release_source_lock(source_key, entry)

    async def _create_from_source_locked(
        self,
        *,
        channel_id: str,
        source_kind: str,
        source_id: str,
        prompt: str,
        thread_name: str,
        send_initial_prompt: bool,
        project_snapshot: ProjectSnapshot | None,
        config_snapshot: ProjectConfigSnapshot | None,
        preallocated_session_id: str | None,
        worktree_intent_id: str | None,
    ) -> SessionRuntime:
        intent = await self._intents.by_source(
            source_kind=source_kind,
            source_id=source_id,
        )
        if intent is None:
            config = await self._projects.session_config_snapshot(channel_id)
            project = (
                ProjectSnapshot(
                    project_id=config.project_id,
                    channel_id=channel_id,
                    source=config.source,
                    root_path=config.root_path,
                    cwd=config.cwd,
                    config_version=config.project_config_version,
                )
                if project_snapshot is None
                else project_snapshot
            )
            frozen_config = (
                await self._projects.config_snapshot(project)
                if config_snapshot is None
                else config_snapshot
            )
            extension_config = None
            if self._extension_configs is not None:
                extension_config = (
                    await self._extension_configs.latest(project)
                    if self._extension_config_source is None
                    else await self._extension_configs.ingest(
                        project,
                        self._extension_config_source,
                    )
                )
            intent, _ = await self._intents.reserve(
                source_kind=source_kind,
                source_id=source_id,
                project=project,
                config=config,
                config_snapshot=frozen_config,
                sdk_session_id=preallocated_session_id,
                worktree_intent_id=worktree_intent_id,
                extension_config=extension_config,
            )
        elif intent.session_config_snapshot_json is not None:
            frozen_config = ProjectConfigSnapshot.from_dict(
                json.loads(intent.session_config_snapshot_json)
            )
        else:
            frozen_config = None
        if intent.config_snapshot_state != "verified":
            raise SessionCreationUnknown(
                "legacy creation intent has no verified project configuration snapshot"
            )
        await self._intents.assert_side_effect_admitted(intent)
        try:
            SessionLaunchOptions.from_json(intent.session_config_snapshot_json)
        except SessionConfigSnapshotError:
            await self._intents.mark(intent, CreationState.FAILED)
            raise
        if intent.thread_id is None:
            reference = await self._threads.find_thread(
                channel_id=channel_id,
                source_id=source_id,
                creation_token=intent.creation_token,
            )
            if reference is None:
                if intent.state == CreationState.UNKNOWN:
                    raise SessionCreationUnknown(
                        "Discord thread creation remains unknown; "
                        "the original token did not reconcile"
                    )
                try:
                    reference = await self._threads.create_thread(
                        channel_id=channel_id,
                        source_id=source_id,
                        name=thread_name,
                        creation_token=intent.creation_token,
                        layout=intent.layout,
                    )
                except Exception as error:
                    await self._intents.mark(intent, CreationState.UNKNOWN)
                    raise SessionCreationUnknown("Discord thread creation is unknown") from error
            intent = await self._intents.set_thread(intent, thread_id=reference.thread_id)

        binding = await self._bindings.by_thread(intent.thread_id)
        if binding is None:
            binding = await self._bindings.create(
                thread_id=intent.thread_id,
                sdk_session_id=intent.sdk_session_id,
                cwd_snapshot=intent.cwd_snapshot,
                project_source=intent.project_source,
                project_id=intent.project_id,
                session_config_snapshot=intent.project_config_snapshot,
                channel_config_snapshot=intent.channel_config_snapshot,
                project_snapshot_json=intent.project_snapshot_json,
                session_config_snapshot_json=intent.session_config_snapshot_json,
                session_config_version=(
                    intent.project_config_version
                    if frozen_config is None
                    else frozen_config.config_version
                ),
                desired_session_config_version=intent.desired_session_config_version,
                desired_session_config_hash=intent.desired_session_config_hash,
            )

        runtime = self._sessions.for_thread(intent.thread_id)
        if runtime is None:
            runtime = await self._sessions.replace(binding)
        elif runtime.state in {RuntimeState.RECOVERY_UNKNOWN, RuntimeState.FENCED}:
            runtime = await self._sessions.replace(binding)

        if runtime.state == RuntimeState.DETACHED:
            uncertain_create = intent.state in {
                CreationState.CREATING,
                CreationState.ATTACHED,
                CreationState.UNKNOWN,
            }
            intent = await self._intents.mark(intent, CreationState.CREATING)
            try:
                if uncertain_create:
                    await runtime.attach_resume()
                else:
                    await runtime.attach_create()
            except SessionAttachRejected:
                await self._intents.mark(intent, CreationState.FAILED)
                raise
            except SessionAttachUnknown as error:
                await self._intents.mark(intent, CreationState.UNKNOWN)
                raise SessionCreationUnknown("SDK session creation is unknown") from error
            intent = await self._intents.mark(intent, CreationState.ATTACHED)
        elif intent.state != CreationState.ATTACHED:
            intent = await self._intents.mark(intent, CreationState.ATTACHED)

        if send_initial_prompt:
            await runtime.send(
                prompt,
                idempotency_key=f"{source_kind}:{source_id}",
            )
        return runtime

    async def _acquire_source_lock(
        self,
        source_key: tuple[str, str],
    ) -> _SourceCreationLock:
        async with self._source_locks_guard:
            entry = self._source_locks.get(source_key)
            if entry is None:
                entry = _SourceCreationLock(lock=asyncio.Lock())
                self._source_locks[source_key] = entry
            entry.users += 1
        try:
            await entry.lock.acquire()
        except BaseException:
            async with self._source_locks_guard:
                entry.users -= 1
                if entry.users == 0:
                    self._source_locks.pop(source_key, None)
            raise
        return entry

    async def _release_source_lock(
        self,
        source_key: tuple[str, str],
        entry: _SourceCreationLock,
    ) -> None:
        entry.lock.release()
        async with self._source_locks_guard:
            entry.users -= 1
            if entry.users == 0 and self._source_locks.get(source_key) is entry:
                self._source_locks.pop(source_key, None)


def _row_to_intent(row: Row) -> CreationIntent:
    return CreationIntent(
        creation_token=row["creation_token"],
        source_kind=row["source_kind"],
        source_id=row["source_id"],
        project_source=row["project_source"],
        project_id=row["project_id"],
        cwd_snapshot=Path(row["cwd_snapshot"]),
        sdk_session_id=row["sdk_session_id"],
        thread_id=row["thread_id"],
        project_config_snapshot=json.loads(row["project_config_snapshot"]),
        channel_config_snapshot=json.loads(row["channel_config_snapshot"]),
        layout=row["layout"],
        project_config_version=int(row["project_config_version"]),
        channel_config_version=int(row["channel_config_version"]),
        config_snapshot_state=str(row["config_snapshot_state"]),
        desired_session_config_version=row["desired_session_config_version"],
        desired_session_config_hash=row["desired_session_config_hash"],
        state=CreationState(row["state"]),
        project_snapshot_json=row["project_snapshot_json"],
        session_config_snapshot_json=row["session_config_snapshot_json"],
        worktree_intent_id=row["worktree_intent_id"],
    )


def _project_snapshot_json(project: ProjectSnapshot) -> str:
    return json.dumps(
        {
            "project_id": project.project_id,
            "channel_id": project.channel_id,
            "source": project.source.value,
            "root_path": str(project.root_path),
            "cwd": str(project.cwd),
            "config_version": project.config_version,
            "timezone": project.timezone,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
