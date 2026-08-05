from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from aiosqlite import Row

from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.projects import ProjectRegistry, ProjectSnapshot
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

    async def reserve(
        self,
        *,
        source_kind: str,
        source_id: str,
        project: ProjectSnapshot,
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
            if row is not None:
                intent = _row_to_intent(row)
                if (
                    intent.project_source != project.source.value
                    or intent.project_id != project.project_id
                    or intent.cwd_snapshot != project.cwd
                ):
                    raise ValueError("creation source was reused with a different project snapshot")
                return intent, False

            intent = CreationIntent(
                creation_token=uuid.uuid4().hex,
                source_kind=source_kind,
                source_id=source_id,
                project_source=project.source.value,
                project_id=project.project_id,
                cwd_snapshot=project.cwd,
                sdk_session_id=str(uuid.uuid4()),
                thread_id=None,
                state=CreationState.RESERVED,
            )
            await connection.execute(
                """
                INSERT INTO session_creation_intents(
                    creation_token, source_kind, source_id, project_source,
                    project_id, cwd_snapshot, sdk_session_id, state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    timestamp,
                    timestamp,
                ),
            )
            return intent, True

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
                    received_at
                    if last_callback_at is None
                    else max(last_callback_at, received_at)
                )
        return depth, max_lag_ms, last_callback_at

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
        return failures

    async def shutdown(self) -> None:
        runtimes = list(self._runtimes.values())
        self._runtimes.clear()
        for runtime in runtimes:
            await runtime.shutdown()


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

    async def create_from_source(
        self,
        *,
        channel_id: str,
        source_kind: str,
        source_id: str,
        prompt: str,
        thread_name: str,
        send_initial_prompt: bool = True,
    ) -> SessionRuntime:
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
    ) -> SessionRuntime:
        project = await self._projects.resolve(channel_id)
        intent, _ = await self._intents.reserve(
            source_kind=source_kind,
            source_id=source_id,
            project=project,
        )
        if intent.thread_id is None:
            reference = await self._threads.find_thread(
                channel_id=channel_id,
                source_id=source_id,
                creation_token=intent.creation_token,
            )
            if reference is None:
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
    )
