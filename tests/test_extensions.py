import json
from pathlib import Path

import pytest

from copilotd.core import extensions as extension_module
from copilotd.core.bindings import AttachmentState, SessionBindingRepository
from copilotd.core.extensions import (
    ConfigReloadClaimStore,
    ConfigReloadState,
    CustomAgent,
    EnvironmentBinding,
    EnvironmentReference,
    ExtensionConfigConflict,
    ExtensionConfigError,
    ExtensionConfigFileSource,
    ExtensionConfigRepository,
    HeaderBinding,
    McpHttpServer,
    McpStdioServer,
    MissingEnvironmentReference,
    ProjectExtensionConfig,
)
from copilotd.core.projects import ProjectRegistry
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


@pytest.mark.asyncio
async def test_extension_config_generations_are_immutable_and_idempotent(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    project_path = tmp_path / "project"
    skills = project_path / ".skills"
    plugins = project_path / ".plugins"
    for path in (home, project_path, skills, plugins):
        path.mkdir()

    async with Database(tmp_path / "extensions.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.bind("channel-1", project_path)
        repository = ExtensionConfigRepository(database)
        config = ProjectExtensionConfig(
            environment_references=(EnvironmentReference("mcp_token", "TEST_MCP_TOKEN"),),
            mcp_servers=(
                McpStdioServer(
                    name="local",
                    command="python",
                    args=("-m", "test_server"),
                    environment=(EnvironmentBinding("TOKEN", "mcp_token"),),
                    working_directory=".",
                ),
                McpHttpServer(
                    name="remote",
                    url="https://mcp.example.test/rpc",
                    headers=(HeaderBinding("Authorization", "mcp_token"),),
                ),
            ),
            skill_directories=(".skills",),
            disabled_skills=("unsafe-skill",),
            plugin_directories=(".plugins",),
            custom_agents=(
                CustomAgent(
                    name="reviewer",
                    prompt="Review the supplied change.",
                    tools=("read",),
                    skills=("review",),
                    mcp_server_names=("local",),
                ),
            ),
        )

        first = await repository.publish(project, config, expected_current_version=0)
        same = await repository.publish(project, config, expected_current_version=1)
        second = await repository.publish(
            project,
            ProjectExtensionConfig(
                environment_references=config.environment_references,
                mcp_servers=config.mcp_servers,
                skill_directories=config.skill_directories,
                disabled_skills=("unsafe-skill", "network-skill"),
                plugin_directories=config.plugin_directories,
                custom_agents=config.custom_agents,
            ),
            expected_current_version=1,
        )
        reverted = await repository.publish(
            project,
            config,
            expected_current_version=2,
        )
        rows = await database.fetchall(
            """
            SELECT version, config_hash
            FROM project_extension_config_generations
            ORDER BY version
            """
        )
        children = await database.fetchone(
            """
            SELECT
                (SELECT COUNT(*) FROM project_extension_mcp_servers) AS mcp_count,
                (SELECT COUNT(*) FROM project_extension_custom_agents) AS agent_count,
                (SELECT COUNT(*) FROM project_extension_disabled_skills) AS disabled_count
            """
        )

    assert first.version == same.version == 1
    assert second.version == 2
    assert reverted.version == 3
    assert reverted.config_hash == first.config_hash
    assert first.config.skill_directories == (str(skills.resolve()),)
    assert first.config.plugin_directories == (str(plugins.resolve()),)
    assert first.config_hash != second.config_hash
    assert [dict(row) for row in rows] == [
        {"version": 1, "config_hash": first.config_hash},
        {"version": 2, "config_hash": second.config_hash},
        {"version": 3, "config_hash": first.config_hash},
    ]
    assert dict(children) == {
        "mcp_count": 6,
        "agent_count": 3,
        "disabled_count": 4,
    }


@pytest.mark.asyncio
async def test_extension_snapshot_resolves_env_only_at_sdk_boundary(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    secret = "not-persisted-secret"

    async with Database(tmp_path / "env-refs.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-1")
        repository = ExtensionConfigRepository(database)
        snapshot = await repository.publish(
            project,
            ProjectExtensionConfig(
                environment_references=(EnvironmentReference("token", "LIVE_TOKEN"),),
                mcp_servers=(
                    McpHttpServer(
                        name="remote",
                        url="https://mcp.example.test",
                        headers=(HeaderBinding("Authorization", "token"),),
                    ),
                ),
            ),
        )
        persisted = await database.fetchone(
            """
            SELECT config_json FROM project_extension_config_generations
            WHERE scope_key = ? AND version = ?
            """,
            (snapshot.scope_key, snapshot.version),
        )

        options = snapshot.sdk_session_options({"LIVE_TOKEN": secret})
        with pytest.raises(MissingEnvironmentReference, match="LIVE_TOKEN"):
            snapshot.sdk_session_options({})

    assert secret not in persisted["config_json"]
    assert options["mcp_servers"]["remote"]["headers"] == {"Authorization": secret}
    assert snapshot.dynamic_headers("remote", {"LIVE_TOKEN": secret}) == {"Authorization": secret}
    assert options["enable_config_discovery"] is False
    assert options["mcp_oauth_token_storage"] == "in-memory"


@pytest.mark.asyncio
async def test_extension_config_optimistic_version_and_session_pin(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()

    async with Database(tmp_path / "extension-conflict.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-1")
        repository = ExtensionConfigRepository(database)
        first = await repository.publish(project, ProjectExtensionConfig())
        pinned = await repository.for_session(
            project_source=project.source.value,
            project_id=project.project_id,
            cwd_snapshot=project.cwd,
            version=first.version,
        )

        with pytest.raises(ExtensionConfigConflict, match="expected 0"):
            await repository.publish(
                project,
                ProjectExtensionConfig(disabled_skills=("new",)),
                expected_current_version=0,
            )

    assert pinned == first


def test_extension_config_rejects_secret_bearing_or_ambiguous_shapes() -> None:
    with pytest.raises(ExtensionConfigError, match="embed credentials"):
        McpHttpServer(
            name="remote",
            url="https://user:secret@mcp.example.test",
        )


@pytest.mark.asyncio
async def test_extension_file_source_ingests_strict_project_config(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    config_dir = home / ".copilotd"
    config_dir.mkdir()
    config_path = config_dir / "extensions.json"
    config_path.write_text(
        json.dumps(
            {
                "environment_references": [{"name": "token", "source_env": "PRODUCTION_TOKEN"}],
                "disabled_skills": ["disabled-production-skill"],
            }
        ),
        encoding="utf-8",
    )

    async with Database(tmp_path / "file-source.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-file-source")
        repository = ExtensionConfigRepository(database)
        snapshot = await repository.ingest(
            project,
            ExtensionConfigFileSource(),
        )

    assert snapshot.version == 1
    assert snapshot.config.disabled_skills == ("disabled-production-skill",)
    assert snapshot.config.environment_references[0].source_env == "PRODUCTION_TOKEN"
    assert "PRODUCTION_TOKEN" in snapshot.config.canonical_json()


@pytest.mark.asyncio
async def test_extension_file_source_rejects_symlink_and_oversize(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    projects_database = Database(tmp_path / "file-source-errors.sqlite3")
    await projects_database.open()
    try:
        projects = ProjectRegistry(projects_database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-file-source-errors")
        config_dir = home / ".copilotd"
        config_dir.mkdir()
        outside = tmp_path / "outside.json"
        outside.write_text("{}", encoding="utf-8")
        config_path = config_dir / "extensions.json"
        config_path.symlink_to(outside)

        with pytest.raises(ExtensionConfigError, match=r"outside|symlink"):
            await ExtensionConfigFileSource().load(project)

        config_path.unlink()
        outside.unlink()
        config_path.symlink_to(config_dir / "missing.json")
        with pytest.raises(ExtensionConfigError, match="symlink"):
            await ExtensionConfigFileSource().load(project)

        config_path.unlink()
        config_path.write_text('{"disabled_skills":["too-large"]}', encoding="utf-8")
        with pytest.raises(ExtensionConfigError, match="exceeds"):
            await ExtensionConfigFileSource(max_bytes=4).load(project)
    finally:
        await projects_database.close()


@pytest.mark.asyncio
async def test_atomic_reload_publication_rejects_stale_owner_takeover(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    async with Database(tmp_path / "atomic-reload.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-atomic-reload")
        leases = OwnerLeaseStore(database)
        stale_lease = await leases.acquire(
            "session-atomic",
            "owner-stale",
            now=100,
        )
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-atomic",
            sdk_session_id="session-atomic",
            cwd_snapshot=project.cwd,
            project_source=project.source.value,
        )
        binding = await bindings.begin_attachment(
            thread_id=binding.thread_id,
            lease=stale_lease,
            state=AttachmentState.RESUMING,
            now=100,
        )
        await bindings.mark_attached(binding, permission_verified_at=100)
        claims = ConfigReloadClaimStore(database)
        config = ProjectExtensionConfig(disabled_skills=("atomic-skill",))
        first_claim, first_snapshot, created = await claims.claim_and_publish(
            sdk_session_id="session-atomic",
            idempotency_key="reload-atomic",
            project=project,
            config=config,
            owner_id=stale_lease.owner_id,
            runtime_generation=1,
            owner_fence_token=stale_lease.fence_token,
            transition_id="transition-atomic",
            minimum_headroom_seconds=40,
            now=101,
        )
        current_lease = await leases.acquire(
            "session-atomic",
            "owner-current",
            now=200,
        )
        with pytest.raises(ExtensionConfigConflict, match="owner fence"):
            await claims.claim_and_publish(
                sdk_session_id="session-atomic",
                idempotency_key="stale-publication",
                project=project,
                config=ProjectExtensionConfig(disabled_skills=("stale-owner-skill",)),
                owner_id=stale_lease.owner_id,
                runtime_generation=2,
                owner_fence_token=stale_lease.fence_token,
                transition_id="transition-stale",
                minimum_headroom_seconds=40,
                now=201,
            )
        binding = await bindings.by_thread("thread-atomic")
        assert binding is not None
        binding = await bindings.begin_attachment(
            thread_id=binding.thread_id,
            lease=current_lease,
            state=AttachmentState.RESUMING,
            now=201,
        )
        await bindings.mark_attached(binding, permission_verified_at=201)
        takeover_claim, takeover_snapshot, takeover_created = await claims.claim_and_publish(
            sdk_session_id="session-atomic",
            idempotency_key="reload-atomic",
            project=project,
            config=config,
            owner_id=current_lease.owner_id,
            runtime_generation=2,
            owner_fence_token=current_lease.fence_token,
            transition_id="transition-atomic",
            minimum_headroom_seconds=40,
            now=201,
        )
        with pytest.raises(ExtensionConfigConflict, match="changed concurrently"):
            await claims.transition(
                first_claim,
                ConfigReloadState.UNKNOWN,
                now=202,
            )
        generations = await database.fetchone(
            "SELECT COUNT(*) AS count FROM project_extension_config_generations"
        )

    assert created
    assert first_claim.config_version == first_snapshot.version == 1
    assert takeover_created is False
    assert takeover_snapshot == first_snapshot
    assert takeover_claim.owner_fence_token == current_lease.fence_token
    assert takeover_claim.claimed_generation == 2
    assert generations["count"] == 1


@pytest.mark.asyncio
async def test_reload_publication_fault_rolls_back_claim_generation_and_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    async with Database(tmp_path / "atomic-reload-fault.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-atomic-reload-fault")
        leases = OwnerLeaseStore(database)
        lease = await leases.acquire("session-atomic-fault", "owner", now=100)
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-atomic-fault",
            sdk_session_id=lease.sdk_session_id,
            cwd_snapshot=project.cwd,
            project_source=project.source.value,
        )
        binding = await bindings.begin_attachment(
            thread_id=binding.thread_id,
            lease=lease,
            state=AttachmentState.RESUMING,
            now=100,
        )
        await bindings.mark_attached(binding, permission_verified_at=100)

        async def fail_children(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("simulated publication crash")

        monkeypatch.setattr(extension_module, "_insert_config_children", fail_children)
        with pytest.raises(RuntimeError, match="simulated publication crash"):
            await ConfigReloadClaimStore(database).claim_and_publish(
                sdk_session_id=lease.sdk_session_id,
                idempotency_key="reload-fault",
                project=project,
                config=ProjectExtensionConfig(disabled_skills=("never-published",)),
                owner_id=lease.owner_id,
                runtime_generation=1,
                owner_fence_token=lease.fence_token,
                transition_id="transition-fault",
                minimum_headroom_seconds=40,
                now=101,
            )
        claim_count = await database.fetchone("SELECT COUNT(*) AS count FROM config_reload_claims")
        generation_count = await database.fetchone(
            "SELECT COUNT(*) AS count FROM project_extension_config_generations"
        )
        recovered = await bindings.by_thread(binding.thread_id)

    assert claim_count["count"] == 0
    assert generation_count["count"] == 0
    assert recovered is not None
    assert recovered.pending_session_config_version is None


@pytest.mark.asyncio
async def test_unknown_config_key_does_not_replace_existing_generation(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    config_dir = home / ".copilotd"
    config_dir.mkdir()
    async with Database(tmp_path / "unknown-config-key.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.resolve("channel-unknown-key")
        repository = ExtensionConfigRepository(database)
        existing = await repository.publish(
            project,
            ProjectExtensionConfig(disabled_skills=("existing-skill",)),
        )
        (config_dir / "extensions.json").write_text(
            json.dumps({"disabled_skill": ["typo"]}),
            encoding="utf-8",
        )

        with pytest.raises(ExtensionConfigError, match="unknown keys"):
            await repository.ingest(
                project,
                ExtensionConfigFileSource(),
            )
        latest = await repository.latest(project)
        count = await database.fetchone(
            "SELECT COUNT(*) AS count FROM project_extension_config_generations"
        )

    assert latest == existing
    assert count["count"] == 1


@pytest.mark.parametrize(
    "payload",
    [
        {
            "environment_references": [
                {
                    "name": "token",
                    "source_env": "TOKEN",
                    "source_enb": "typo",
                }
            ]
        },
        {
            "mcp_servers": [
                {
                    "name": "local",
                    "transport": "stdio",
                    "command": "server",
                    "argz": [],
                }
            ]
        },
        {
            "environment_references": [{"name": "token", "source_env": "TOKEN"}],
            "mcp_servers": [
                {
                    "name": "local",
                    "transport": "stdio",
                    "command": "server",
                    "environment": [
                        {
                            "name": "TOKEN",
                            "reference": "token",
                            "referance": "typo",
                        }
                    ],
                }
            ],
        },
        {
            "mcp_servers": [
                {
                    "name": "remote",
                    "transport": "http",
                    "url": "https://mcp.example.test",
                    "timeout": 1000,
                }
            ]
        },
        {
            "environment_references": [{"name": "token", "source_env": "TOKEN"}],
            "mcp_servers": [
                {
                    "name": "remote",
                    "transport": "http",
                    "url": "https://mcp.example.test",
                    "headers": [
                        {
                            "name": "Authorization",
                            "reference": "token",
                            "referance": "typo",
                        }
                    ],
                }
            ],
        },
        {
            "custom_agents": [
                {
                    "name": "agent",
                    "prompt": "Review.",
                    "toolz": ["read"],
                }
            ]
        },
    ],
)
def test_nested_extension_records_reject_unknown_keys(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ExtensionConfigError, match="unknown keys"):
        ProjectExtensionConfig.from_dict(payload)
    with pytest.raises(ExtensionConfigError, match="unknown environment"):
        ProjectExtensionConfig(
            mcp_servers=(
                McpStdioServer(
                    name="local",
                    command="server",
                    environment=(EnvironmentBinding("TOKEN", "missing"),),
                ),
            )
        )
    with pytest.raises(ExtensionConfigError, match="unknown MCP servers"):
        ProjectExtensionConfig(
            custom_agents=(
                CustomAgent(
                    name="agent",
                    prompt="Use MCP.",
                    mcp_server_names=("missing",),
                ),
            )
        )
