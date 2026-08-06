import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.extensions import (
    ExtensionConfigFileSource,
    ExtensionConfigRepository,
)
from copilotd.core.projects import ProjectRegistry
from copilotd.core.session_runtime import SessionAttachRejected, SessionRuntime
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
        self.create_kwargs: dict[str, Any] = {}
        self.managed_settings_enabled = True

    def managed_settings_available(self) -> bool:
        return self.managed_settings_enabled

    def require_managed_settings_available(self) -> None:
        if not self.managed_settings_enabled:
            raise SessionAttachRejected("managed settings require explicit session credentials")

    async def create_session(self, **kwargs: Any) -> FakeHandle:
        self.create_calls += 1
        self.create_kwargs = kwargs
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

    async def get_tasks(self, _session: FakeHandle) -> list[dict[str, Any]]:
        return []


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
    extension_configs = ExtensionConfigRepository(database)
    extension_source = ExtensionConfigFileSource()

    def runtime_factory(binding: Any) -> SessionRuntime:
        return SessionRuntime(
            database=database,
            bridge=bridge,
            bindings=bindings,
            owner_leases=leases,
            owner_id="process-1",
            binding=binding,
            extension_configs=extension_configs,
        )

    sessions = SessionRegistry(bindings, runtime_factory)
    service = SessionCreationService(
        projects=projects,
        intents=CreationIntentRepository(database),
        bindings=bindings,
        sessions=sessions,
        threads=threads,
        extension_configs=extension_configs,
        extension_config_source=extension_source,
        attachment_preflight=bridge.require_managed_settings_available,
    )
    return service, sessions


@pytest.mark.asyncio
async def test_new_session_ingests_production_extension_config(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    config_dir = home / ".copilotd"
    await asyncio.to_thread(config_dir.mkdir)
    await asyncio.to_thread(
        (config_dir / "extensions.json").write_text,
        json.dumps({"disabled_skills": ["production-disabled-skill"]}),
        "utf-8",
    )
    async with Database(tmp_path / "production-config.sqlite3") as database:
        bridge = FakeBridge()
        service, sessions = await _build_service(
            database,
            home,
            bridge,
            FakeThreads(),
        )

        runtime = await service.create_from_source(
            channel_id="channel-production",
            source_kind="message",
            source_id="message-production",
            prompt="hello",
            thread_name="production",
        )
        generation = await database.fetchone(
            """
            SELECT version, config_hash
            FROM project_extension_config_generations
            WHERE scope_key = 'implicit-home'
            """
        )

        assert generation is not None
        assert runtime.binding.desired_session_config_hash == generation["config_hash"]
        assert bridge.create_kwargs["session_options"]["disabled_skills"] == [
            "production-disabled-skill"
        ]
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_creation_intent_fails_closed_without_managed_credentials(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "creation-managed-auth.sqlite3") as database:
        bridge = FakeBridge()
        bridge.managed_settings_enabled = False
        threads = FakeThreads()
        service, sessions = await _build_service(
            database,
            home,
            bridge,
            threads,
        )

        with pytest.raises(SessionAttachRejected, match="managed settings"):
            await service.create_from_source(
                channel_id="channel-managed-auth",
                source_kind="message",
                source_id="message-managed-auth",
                prompt="hello",
                thread_name="managed auth",
            )
        intent = await database.fetchone("SELECT state FROM session_creation_intents")

        assert intent is None
        assert threads.create_calls == 0
        assert bridge.create_calls == 0
        assert sessions._runtimes == {}
        await sessions.shutdown()


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
