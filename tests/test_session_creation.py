import asyncio
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.projects import ProjectRegistry
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.sessions import (
    CreationIntentRepository,
    SessionCreationService,
    SessionCreationUnknown,
    SessionRegistry,
    ThreadReference,
)
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


class FakeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.send_calls = 0

    async def send(self, _prompt: str, **_kwargs: Any) -> str:
        self.send_calls += 1
        return str(uuid4())

    async def abort(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass


class FakeBridge:
    def __init__(self, *, fail_first_create: bool = False) -> None:
        self.fail_first_create = fail_first_create
        self.create_calls = 0
        self.resume_calls = 0
        self.handle: FakeHandle | None = None
        self.mode = "interactive"

    async def create_session(self, **kwargs: Any) -> FakeHandle:
        self.create_calls += 1
        if self.fail_first_create and self.create_calls == 1:
            raise ConnectionError("create response lost")
        self.handle = FakeHandle(kwargs["session_id"])
        return self.handle

    async def resume_session(self, session_id: str, **_kwargs: Any) -> FakeHandle:
        self.resume_calls += 1
        self.handle = FakeHandle(session_id)
        return self.handle

    async def ensure_allow_all(self, _session: FakeHandle) -> object:
        return object()

    async def get_mode(self, _session: FakeHandle) -> str:
        return self.mode

    async def set_mode(self, _session: FakeHandle, mode: str) -> None:
        self.mode = mode

    async def get_readiness(self, _session: FakeHandle) -> dict[str, Any]:
        return {
            "processing": False,
            "hasActiveWork": False,
            "abortable": False,
            "pendingItems": [],
            "steeringMessages": [],
        }


class FakeThreads:
    def __init__(self, *, ambiguous_first_create: bool = False) -> None:
        self.ambiguous_first_create = ambiguous_first_create
        self.create_calls = 0
        self.reference: ThreadReference | None = None

    async def find_thread(self, **_kwargs: Any) -> ThreadReference | None:
        return self.reference

    async def create_thread(self, **_kwargs: Any) -> ThreadReference:
        self.create_calls += 1
        await asyncio.sleep(0)
        self.reference = ThreadReference(thread_id="thread-1")
        if self.ambiguous_first_create and self.create_calls == 1:
            raise ConnectionError("Discord response lost")
        return self.reference


async def _build_service(
    database: Database,
    home: Path,
    bridge: FakeBridge,
    threads: FakeThreads,
) -> tuple[SessionCreationService, SessionRegistry]:
    projects = ProjectRegistry(database, resolved_home=home)
    await projects.initialize()
    bindings = SessionBindingRepository(database)
    leases = OwnerLeaseStore(database)

    def runtime_factory(binding: Any) -> SessionRuntime:
        return SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=leases,
            owner_id="process-1",
            binding=binding,
        )

    sessions = SessionRegistry(bindings, runtime_factory)
    service = SessionCreationService(
        projects=projects,
        intents=CreationIntentRepository(database),
        bindings=bindings,
        sessions=sessions,
        threads=threads,
    )
    return service, sessions


@pytest.mark.asyncio
async def test_duplicate_gateway_delivery_creates_one_thread_session_and_send(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "creation.sqlite3") as database:
        bridge = FakeBridge()
        threads = FakeThreads()
        service, sessions = await _build_service(database, home, bridge, threads)

        first = await service.create_from_source(
            channel_id="channel-1",
            source_kind="message",
            source_id="message-1",
            prompt="hello",
            thread_name="hello",
        )
        second = await service.create_from_source(
            channel_id="channel-1",
            source_kind="message",
            source_id="message-1",
            prompt="hello",
            thread_name="hello",
        )
        intent = await database.fetchone("SELECT * FROM session_creation_intents")

        assert first is second
        assert threads.create_calls == 1
        assert bridge.create_calls == 1
        assert bridge.handle is not None
        assert bridge.handle.send_calls == 1
        assert intent["state"] == "attached"
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_concurrent_duplicate_delivery_creates_exactly_one_thread(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "concurrent-creation.sqlite3") as database:
        bridge = FakeBridge()
        threads = FakeThreads()
        service, sessions = await _build_service(database, home, bridge, threads)

        first, second = await asyncio.gather(
            service.create_from_source(
                channel_id="channel-1",
                source_kind="message",
                source_id="message-1",
                prompt="hello",
                thread_name="hello",
            ),
            service.create_from_source(
                channel_id="channel-1",
                source_kind="message",
                source_id="message-1",
                prompt="hello",
                thread_name="hello",
            ),
        )

        assert first is second
        assert threads.create_calls == 1
        assert bridge.create_calls == 1
        assert bridge.handle is not None and bridge.handle.send_calls == 1
        assert service._source_locks == {}
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_ambiguous_discord_response_reconciles_same_thread_token(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "discord-unknown.sqlite3") as database:
        bridge = FakeBridge()
        threads = FakeThreads(ambiguous_first_create=True)
        service, sessions = await _build_service(database, home, bridge, threads)

        with pytest.raises(SessionCreationUnknown, match="Discord"):
            await service.create_from_source(
                channel_id="channel-1",
                source_kind="message",
                source_id="message-1",
                prompt="hello",
                thread_name="hello",
            )

        runtime = await service.create_from_source(
            channel_id="channel-1",
            source_kind="message",
            source_id="message-1",
            prompt="hello",
            thread_name="hello",
        )
        intent = await database.fetchone(
            "SELECT thread_id, sdk_session_id, state FROM session_creation_intents"
        )

        assert threads.create_calls == 1
        assert runtime.binding.thread_id == intent["thread_id"] == "thread-1"
        assert runtime.binding.sdk_session_id == intent["sdk_session_id"]
        assert intent["state"] == "attached"
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_ambiguous_sdk_create_is_reconciled_by_resume_without_second_create(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "sdk-unknown.sqlite3") as database:
        bridge = FakeBridge(fail_first_create=True)
        threads = FakeThreads()
        service, sessions = await _build_service(database, home, bridge, threads)

        with pytest.raises(SessionCreationUnknown, match="SDK"):
            await service.create_from_source(
                channel_id="channel-1",
                source_kind="message",
                source_id="message-1",
                prompt="hello",
                thread_name="hello",
            )

        runtime = await service.create_from_source(
            channel_id="channel-1",
            source_kind="message",
            source_id="message-1",
            prompt="hello",
            thread_name="hello",
        )

        assert runtime.handle is bridge.handle
        assert bridge.create_calls == 1
        assert bridge.resume_calls == 1
        await sessions.shutdown()
