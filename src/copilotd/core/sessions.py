from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from aiosqlite import Row

from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.projects import (
    ProjectConfigSnapshot,
    ProjectRegistry,
    ProjectSnapshot,
)
from copilotd.core.session_config import SessionConfigSnapshotError, SessionLaunchOptions
from copilotd.core.session_runtime import RuntimeState, SessionAttachUnknown, SessionRuntime
from copilotd.storage.database import Database

RuntimeFactory = Callable[[SessionBinding], SessionRuntime]


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
    ) -> ThreadReference: ...


class SessionCreationUnknown(RuntimeError):
    pass


@dataclass(slots=True)
class _SourceCreationLock:
    lock: asyncio.Lock
    users: int = 0


class CreationIntentRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    @property
    def database(self) -> Database:
        return self._database

    async def reserve(
        self,
        *,
        source_kind: str,
        source_id: str,
        project: ProjectSnapshot,
        config_snapshot: ProjectConfigSnapshot | None = None,
        sdk_session_id: str | None = None,
        worktree_intent_id: str | None = None,
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
                if project_row is None or project_row["state"] == "closing" or (
                    project_row["project_kind"] == "worktree"
                    and project_row["state"] == "retired"
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
            config_json = (
                None if config_snapshot is None else config_snapshot.canonical_json()
            )
            intent = CreationIntent(
                creation_token=uuid.uuid4().hex,
                source_kind=source_kind,
                source_id=source_id,
                project_source=project.source.value,
                project_id=project.project_id,
                cwd_snapshot=project.cwd,
                sdk_session_id=str(uuid.uuid4()) if sdk_session_id is None else sdk_session_id,
                thread_id=None,
                state=CreationState.RESERVED,
                project_snapshot_json=project_json,
                session_config_snapshot_json=config_json,
                worktree_intent_id=worktree_intent_id,
            )
            await connection.execute(
                """
                INSERT INTO session_creation_intents(
                    creation_token, source_kind, source_id, project_source,
                    project_id, cwd_snapshot, sdk_session_id, state,
                    project_snapshot_json, session_config_snapshot_json,
                    worktree_intent_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.creation_token,
                    intent.source_kind,
                    intent.source_id,
                    intent.project_source,
                    intent.project_id,
                    str(intent.cwd_snapshot),
                    intent.sdk_session_id,
                    intent.state.value,
                    intent.project_snapshot_json,
                    intent.session_config_snapshot_json,
                    intent.worktree_intent_id,
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
            if project_row is None or project_row["state"] == "closing" or (
                project_row["project_kind"] == "worktree"
                and project_row["state"] == "retired"
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

    def for_thread(self, thread_id: str) -> SessionRuntime | None:
        return self._runtimes.get(thread_id)

    def register(self, runtime: SessionRuntime) -> None:
        thread_id = runtime.binding.thread_id
        existing = self._runtimes.get(thread_id)
        if existing is not None and existing is not runtime:
            raise RuntimeError(f"thread already has a SessionRuntime: {thread_id}")
        self._runtimes[thread_id] = runtime

    async def replace(self, binding: SessionBinding) -> SessionRuntime:
        existing = self._runtimes.pop(binding.thread_id, None)
        if existing is not None:
            await existing.shutdown()
        runtime = self._runtime_factory(binding)
        self.register(runtime)
        return runtime

    async def eager_resume(self) -> dict[str, str]:
        failures: dict[str, str] = {}
        for binding in await self._bindings.eager_bindings():
            runtime = self._runtime_factory(binding)
            self.register(runtime)
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
        return failures

    async def shutdown(self) -> None:
        runtimes = list(self._runtimes.values())
        self._runtimes.clear()
        errors: list[Exception] = []
        for runtime in runtimes:
            try:
                await runtime.shutdown()
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("one or more session runtimes failed to shut down", errors)


class SessionCreationService:
    def __init__(
        self,
        *,
        projects: ProjectRegistry,
        intents: CreationIntentRepository,
        bindings: SessionBindingRepository,
        sessions: SessionRegistry,
        threads: ThreadGateway,
    ) -> None:
        self._projects = projects
        self._intents = intents
        self._bindings = bindings
        self._sessions = sessions
        self._threads = threads
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
        project = (
            await self._projects.resolve(channel_id)
            if project_snapshot is None
            else project_snapshot
        )
        frozen_config = (
            await self._projects.config_snapshot(project)
            if config_snapshot is None
            else config_snapshot
        )
        intent, _ = await self._intents.reserve(
            source_kind=source_kind,
            source_id=source_id,
            project=project,
            config_snapshot=frozen_config,
            sdk_session_id=preallocated_session_id,
            worktree_intent_id=worktree_intent_id,
        )
        if intent.session_config_snapshot_json is not None:
            frozen_config = ProjectConfigSnapshot.from_dict(
                json.loads(intent.session_config_snapshot_json)
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
                project_snapshot_json=intent.project_snapshot_json,
                session_config_snapshot_json=intent.session_config_snapshot_json,
                session_config_version=frozen_config.config_version,
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
