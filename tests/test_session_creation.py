import asyncio
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.projects import (
    McpServerSnapshot,
    ProjectConfigSnapshot,
    ProjectRegistry,
    ProjectSource,
    ProjectValidationError,
)
from copilotd.core.session_config import SessionConfigSnapshotError
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.sessions import (
    CreationIntentRepository,
    SessionCreationService,
    SessionCreationUnknown,
    SessionRegistry,
    SessionRegistryNotAccepting,
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
        self.resume_kwargs: dict[str, Any] = {}

    async def create_session(self, **kwargs: Any) -> FakeHandle:
        self.create_calls += 1
        self.create_kwargs = kwargs
        if self.fail_first_create and self.create_calls == 1:
            raise ConnectionError("create response lost")
        self.handle = FakeHandle(kwargs["session_id"])
        return self.handle

    async def resume_session(self, session_id: str, **kwargs: Any) -> FakeHandle:
        self.resume_calls += 1
        self.resume_kwargs = kwargs
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
        self.create_kwargs: dict[str, Any] = {}

    async def find_thread(self, **_kwargs: Any) -> ThreadReference | None:
        return self.reference

    async def create_thread(self, **_kwargs: Any) -> ThreadReference:
        self.create_calls += 1
        self.create_kwargs = _kwargs
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
async def test_future_session_snapshots_and_applies_project_configuration(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    skill = tmp_path / "skills"
    plugin = tmp_path / "plugins"
    for path in (home, repo, skill, plugin):
        path.mkdir()
    bridge = FakeBridge()
    threads = FakeThreads()
    async with Database(tmp_path / "project-session.sqlite3") as database:
        service, sessions = await _build_service(database, home, bridge, threads)
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.bind("channel-project", repo)
        await projects.set_project_env("channel-project", "TOKEN", "snapshot-secret")
        await projects.set_mcp_server(
            "channel-project",
            name="local",
            transport="stdio",
            config={
                "command": "node",
                "args": ["server.js"],
                "project_env_refs": ["TOKEN"],
            },
        )
        await projects.set_skill_dir("channel-project", path=str(skill))
        await projects.set_plugin_dir("channel-project", path=str(plugin))
        await projects.set_custom_agent(
            "channel-project",
            name="reviewer",
            description="Review code",
            prompt="Review the current changes.",
            tools=["tool.review"],
        )
        await projects.set_layout("channel-project", "text")

        runtime = await service.create_from_source(
            channel_id="channel-project",
            source_kind="message",
            source_id="source-project",
            prompt="hello",
            thread_name="Configured session",
            send_initial_prompt=False,
        )
        binding = await SessionBindingRepository(database).by_thread("thread-1")
        await projects.set_project_env(
            "channel-project",
            "TOKEN",
            "new-secret-for-future-session",
        )
        unchanged = await SessionBindingRepository(database).by_thread("thread-1")

        options = bridge.create_kwargs["session_config"]
        assert options["mcp_servers"]["local"]["env"]["TOKEN"] == "snapshot-secret"
        assert options["skill_directories"] == [str(skill)]
        assert options["plugin_directories"] == [str(plugin)]
        assert options["custom_agents"][0]["name"] == "reviewer"
        assert threads.create_kwargs["layout"] == "text"
        assert binding is not None
        assert binding.desired_session_config_version == 6
        assert binding.channel_config_snapshot["layout"] == "text"
        assert unchanged is not None
        assert unchanged.session_config_snapshot == binding.session_config_snapshot
        assert (
            unchanged.session_config_snapshot["session_options"]["mcp_servers"]["local"]["env"][
                "TOKEN"
            ]
            == "snapshot-secret"
        )
        assert runtime.binding.sdk_session_id == binding.sdk_session_id
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_unapplied_project_environment_fails_before_thread_creation(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    home.mkdir()
    repo.mkdir()
    bridge = FakeBridge()
    threads = FakeThreads()
    async with Database(tmp_path / "unapplied-env.sqlite3") as database:
        service, sessions = await _build_service(database, home, bridge, threads)
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.bind("channel-env", repo)
        await projects.set_project_env("channel-env", "UNUSED_TOKEN", "secret")

        with pytest.raises(ProjectValidationError, match="cannot be applied"):
            await service.create_from_source(
                channel_id="channel-env",
                source_kind="message",
                source_id="source-env",
                prompt="hello",
                thread_name="Should not exist",
            )

        assert threads.create_calls == 0
        assert bridge.create_calls == 0
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_legacy_creation_intent_fails_closed_without_new_thread(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    bridge = FakeBridge()
    threads = FakeThreads()
    async with Database(tmp_path / "legacy-intent.sqlite3") as database:
        service, sessions = await _build_service(database, home, bridge, threads)
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-legacy")
        config = await projects.session_config_snapshot("channel-legacy")
        intents = CreationIntentRepository(database)
        await intents.reserve(
            source_kind="message",
            source_id="source-legacy",
            project=project,
            config=config,
        )
        await database.execute(
            """
            UPDATE session_creation_intents
            SET config_snapshot_state = 'legacy_unverified'
            WHERE source_kind = 'message' AND source_id = 'source-legacy'
            """
        )

        with pytest.raises(SessionCreationUnknown, match="legacy creation intent"):
            await service.create_from_source(
                channel_id="channel-legacy",
                source_kind="message",
                source_id="source-legacy",
                prompt="hello",
                thread_name="Legacy intent",
            )

        assert threads.create_calls == 0
        assert bridge.create_calls == 0
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_v11_backfill_verifies_bindings_and_only_blocks_ambiguous_intents(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    database_path = tmp_path / "upgrade-backfill.sqlite3"
    async with Database(database_path) as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-upgrade",
            sdk_session_id="session-upgrade",
            cwd_snapshot=home,
            project_source="implicit-home",
        )
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-upgrade")
        config = await projects.session_config_snapshot("channel-upgrade")
        intents = CreationIntentRepository(database)
        await intents.reserve(
            source_kind="message",
            source_id="attached-source",
            project=project,
            config=config,
        )
        await intents.reserve(
            source_kind="message",
            source_id="ambiguous-source",
            project=project,
            config=config,
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET config_snapshot_state = 'legacy_unverified',
                session_config_snapshot = '{}',
                channel_config_snapshot = '{}'
            WHERE thread_id = 'thread-upgrade'
            """
        )
        await database.execute(
            """
            UPDATE session_creation_intents
            SET config_snapshot_state = 'legacy_unverified',
                project_config_snapshot = '{}',
                channel_config_snapshot = '{}',
                state = CASE source_id
                    WHEN 'attached-source' THEN 'attached'
                    ELSE 'creating'
                END
            """
        )

    bridge = FakeBridge()
    threads = FakeThreads()
    async with Database(database_path) as database:
        binding = await SessionBindingRepository(database).by_thread("thread-upgrade")
        attached = await CreationIntentRepository(database).by_source(
            source_kind="message",
            source_id="attached-source",
        )
        ambiguous = await CreationIntentRepository(database).by_source(
            source_kind="message",
            source_id="ambiguous-source",
        )
        _service, sessions = await _build_service(database, home, bridge, threads)
        failures = await sessions.eager_resume()

        assert binding is not None
        assert binding.config_snapshot_state == "verified"
        assert binding.session_config_snapshot == {"session_options": {}}
        assert attached is not None and attached.config_snapshot_state == "verified"
        assert ambiguous is not None
        assert ambiguous.config_snapshot_state == "legacy_unverified"
        assert failures == {}
        assert bridge.resume_calls == 1
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

        rebound = tmp_path / "rebound"
        rebound.mkdir()
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.bind("channel-1", rebound)
        await projects.set_project_env("channel-1", "UNAPPLIED", "new-value")

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
        assert runtime.binding.project_source == "implicit-home"
        assert runtime.binding.cwd_snapshot == home
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_schedule_creation_reuses_preallocated_session_and_thread_after_retry(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "schedule-creation.sqlite3") as database:
        bridge = FakeBridge()
        threads = FakeThreads(ambiguous_first_create=True)
        service, sessions = await _build_service(database, home, bridge, threads)
        project = await service._projects.resolve("channel-1")
        config = await service._projects.config_snapshot(project)
        preallocated = str(uuid4())

        with pytest.raises(SessionCreationUnknown, match="Discord"):
            await service.create_from_source(
                channel_id="channel-1",
                source_kind="schedule",
                source_id="run-1",
                prompt="",
                thread_name="scheduled",
                send_initial_prompt=False,
                project_snapshot=project,
                config_snapshot=config,
                preallocated_session_id=preallocated,
            )
        runtime = await service.create_from_source(
            channel_id="channel-1",
            source_kind="schedule",
            source_id="run-1",
            prompt="",
            thread_name="scheduled",
            send_initial_prompt=False,
            project_snapshot=project,
            config_snapshot=config,
            preallocated_session_id=preallocated,
        )

        assert threads.create_calls == 1
        assert bridge.create_calls == 1
        assert runtime.binding.sdk_session_id == preallocated
        assert runtime.binding.thread_id == "thread-1"
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_invalid_snapshot_fails_before_discord_or_sdk_creation(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "invalid-snapshot.sqlite3") as database:
        bridge = FakeBridge()
        threads = FakeThreads()
        service, sessions = await _build_service(database, home, bridge, threads)
        project = await service._projects.resolve("channel-1")
        invalid = ProjectConfigSnapshot(
            project_id=None,
            source=ProjectSource.IMPLICIT_HOME,
            cwd=home,
            timezone="UTC",
            config_version=1,
            mcp_servers=(
                McpServerSnapshot(
                    name="broken",
                    transport="stdio",
                    command="server",
                    url=None,
                    args=(),
                    headers=(),
                    env_refs=("MISSING",),
                    enabled=True,
                    version=1,
                ),
            ),
        )

        with pytest.raises(SessionConfigSnapshotError):
            await service.create_from_source(
                channel_id="channel-1",
                source_kind="message",
                source_id="invalid",
                prompt="hello",
                thread_name="invalid",
                project_snapshot=project,
                config_snapshot=invalid,
            )
        intent = await database.fetchone(
            "SELECT state FROM session_creation_intents WHERE source_id = 'invalid'"
        )

        assert intent["state"] == "failed"
        assert threads.create_calls == 0
        assert bridge.create_calls == 0
        await sessions.shutdown()


@pytest.mark.asyncio
async def test_existing_creation_intent_cannot_bypass_restart_drain(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "intent-drain.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-1")
        intents = CreationIntentRepository(database)
        await intents.reserve(
            source_kind="message",
            source_id="message-1",
            project=project,
        )
        await database.execute(
            """
            INSERT INTO global_config(key, value, updated_at)
            VALUES ('restart_draining', '1', 1)
            """
        )

        with pytest.raises(RuntimeError, match="draining"):
            await intents.reserve(
                source_kind="message",
                source_id="message-1",
                project=project,
            )


@pytest.mark.asyncio
async def test_registry_closes_admission_and_drains_replace_before_snapshot(
    tmp_path: Path,
) -> None:
    entered_shutdown = asyncio.Event()
    release_shutdown = asyncio.Event()

    class FakeRuntime:
        def __init__(self, binding: Any, *, block_shutdown: bool = False) -> None:
            self.binding = binding
            self.block_shutdown = block_shutdown
            self.shutdown_calls = 0

        async def shutdown(self, *, emergency: bool = False) -> None:
            del emergency
            self.shutdown_calls += 1
            if self.block_shutdown:
                entered_shutdown.set()
                await release_shutdown.wait()

    async with Database(tmp_path / "registry-admission.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-1",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="explicit",
        )
        created: list[FakeRuntime] = []

        def factory(item: Any) -> Any:
            runtime = FakeRuntime(item)
            created.append(runtime)
            return runtime

        registry = SessionRegistry(bindings, factory)
        initial = FakeRuntime(binding, block_shutdown=True)
        registry.register(initial)
        replacing = asyncio.create_task(registry.replace(binding))
        await entered_shutdown.wait()
        closing = asyncio.create_task(registry.close_admission())
        await asyncio.sleep(0)

        with pytest.raises(SessionRegistryNotAccepting):
            registry.register(FakeRuntime(binding))
        with pytest.raises(SessionRegistryNotAccepting):
            await registry.replace(binding)
        assert not closing.done()

        release_shutdown.set()
        replacement = await replacing
        await closing
        assert registry.for_thread("thread-1") is replacement
        await registry.shutdown()

    assert initial.shutdown_calls == 1
    assert len(created) == 1
    assert created[0].shutdown_calls == 1
