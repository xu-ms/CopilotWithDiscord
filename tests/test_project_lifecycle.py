import asyncio
import json
from pathlib import Path

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.lifecycle_commands import (
    DiscordParentType,
    LifecycleCommandError,
    ProjectLifecycleService,
    SchedulerCommandService,
)
from copilotd.core.projects import ProjectConfigError, ProjectRegistry
from copilotd.core.scheduler import SchedulerRepository
from copilotd.core.sessions import CreationIntentRepository
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_old_session_keeps_typed_config_snapshot_after_project_mutations(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    skills = tmp_path / "skills"
    plugins = tmp_path / "plugins"
    for directory in (home, repo, skills, plugins):
        await asyncio.to_thread(directory.mkdir)

    async with Database(tmp_path / "immutable-config.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.bind("channel-1", repo)
        assert project.project_id is not None
        await projects.set_variable(project.project_id, "TOKEN_NAME", "first")
        await projects.add_mcp_server(
            project.project_id,
            name="tools",
            transport="stdio",
            command="tool-server",
            args=("--stdio",),
            env_refs=("TOKEN_NAME",),
        )
        await projects.add_directory(project.project_id, kind="skill", path=skills)
        await projects.add_directory(project.project_id, kind="plugin", path=plugins)
        await projects.add_custom_agent(
            project.project_id,
            name="reviewer",
            description="Reviews changes",
            prompt="Review this change.",
            tools=("read_file", "search"),
        )
        before = await projects.config_snapshot_by_id(project.project_id)
        intent, _created = await CreationIntentRepository(database).reserve(
            source_kind="message",
            source_id="message-1",
            project=await projects.project_by_id(project.project_id),
            config_snapshot=before,
        )
        binding = await SessionBindingRepository(database).create(
            thread_id="thread-1",
            sdk_session_id=intent.sdk_session_id,
            cwd_snapshot=intent.cwd_snapshot,
            project_source=intent.project_source,
            project_id=intent.project_id,
            project_snapshot_json=intent.project_snapshot_json,
            session_config_snapshot_json=intent.session_config_snapshot_json,
            session_config_version=before.config_version,
        )

        await projects.set_variable(project.project_id, "TOKEN_NAME", "second")
        await projects.toggle_mcp_server(project.project_id, "tools", False)
        await projects.remove_directory(
            project.project_id,
            kind="plugin",
            path=plugins,
        )
        await projects.toggle_custom_agent(project.project_id, "reviewer", False)
        after = await projects.config_snapshot_by_id(project.project_id)
        persisted_intent = await database.fetchone(
            """
            SELECT session_config_snapshot_json
            FROM session_creation_intents WHERE creation_token = ?
            """,
            (intent.creation_token,),
        )
        persisted_binding = await SessionBindingRepository(database).by_thread("thread-1")

    assert before.variables == (("TOKEN_NAME", "first"),)
    assert before.mcp_servers[0].enabled
    assert before.plugin_dirs[0].enabled
    assert before.custom_agents[0].enabled
    assert after.variables == (("TOKEN_NAME", "second"),)
    assert not after.mcp_servers[0].enabled
    assert after.plugin_dirs == ()
    assert not after.custom_agents[0].enabled
    assert before.snapshot_hash != after.snapshot_hash
    assert json.loads(persisted_intent["session_config_snapshot_json"]) == before.to_dict()
    assert persisted_binding is not None
    assert persisted_binding.session_config_snapshot_json == before.canonical_json()
    assert binding.cwd_snapshot == repo


@pytest.mark.asyncio
async def test_project_lifecycle_validates_parent_layout_timezone_and_typed_mcp(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(repo.mkdir)

    async with Database(tmp_path / "project-service.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.bind("channel-1", repo)
        service = ProjectLifecycleService(database, projects)

        await service.set_layout(
            "channel-1",
            layout="forum",
            parent_type=DiscordParentType.FORUM,
        )
        with pytest.raises(Exception, match="incompatible"):
            await service.set_layout(
                "channel-1",
                layout="text",
                parent_type=DiscordParentType.FORUM,
            )
        await service.set_timezone("channel-1", "Asia/Shanghai")
        with pytest.raises(ProjectConfigError):
            await service.mcp_add(
                project.project_id,
                name="invalid-http",
                transport="http",
                command="forbidden",
                url="https://example.invalid/mcp",
            )
        with pytest.raises(ProjectConfigError, match="do not exist"):
            await service.mcp_add(
                project.project_id,
                name="missing-env",
                transport="stdio",
                command="server",
                env_refs=("MISSING",),
            )
        await service.variable_set(project.project_id, "TOKEN", "value")
        await service.mcp_add(
            project.project_id,
            name="local",
            transport="stdio",
            command="server",
            env_refs=("TOKEN",),
        )
        with pytest.raises(ProjectConfigError, match="referenced"):
            await service.variable_remove(project.project_id, "TOKEN")
        settings = await projects.channel_settings("channel-1")
        timezone = await projects.channel_timezone("channel-1")
        resolved = await projects.resolve("channel-1")

    assert settings[:2] == ("forum", False)
    assert timezone == "Asia/Shanghai"
    assert resolved.timezone == "Asia/Shanghai"
    assert resolved.config_version > project.config_version


@pytest.mark.asyncio
async def test_new_session_schedule_keeps_project_snapshot_after_future_changes(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(repo.mkdir)
    async with Database(tmp_path / "schedule-snapshot.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.bind("channel-1", repo)
        assert project.project_id is not None
        await projects.set_variable(project.project_id, "VALUE", "before")
        commands = SchedulerCommandService(
            database,
            projects,
            SchedulerRepository(database),
        )
        definition = await commands.create_new_session(
            channel_id="channel-1",
            expression="at:2030-01-01T00:00:00Z",
            text="scheduled",
            timezone=None,
            created_by="user",
            now=0,
        )
        await projects.set_variable(project.project_id, "VALUE", "after")
        current = await projects.config_snapshot_by_id(project.project_id)

    frozen = definition.target_snapshot["project_config"]
    assert frozen["variables"] == [{"name": "VALUE", "value": "before"}]
    assert current.variables == (("VALUE", "after"),)


@pytest.mark.asyncio
async def test_legacy_message_schedule_requires_real_channel_timezone_and_valid_intent(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    async with Database(tmp_path / "legacy-schedule.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.set_channel_timezone("channel-1", "Asia/Shanghai")
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="legacy-thread",
            sdk_session_id="legacy-session",
            cwd_snapshot=home,
            project_source="implicit-home",
        )
        commands = SchedulerCommandService(
            database,
            projects,
            SchedulerRepository(database),
        )

        with pytest.raises(LifecycleCommandError, match="timezone or channel_id"):
            await commands.create_message(
                thread_id="legacy-thread",
                expression="cron:0 9 * * *",
                text="scheduled",
                timezone=None,
                created_by="user",
            )
        definition = await commands.create_message(
            thread_id="legacy-thread",
            expression="cron:0 9 * * *",
            text="scheduled",
            timezone=None,
            created_by="user",
            channel_id="channel-1",
        )
        await database.execute(
            """
            UPDATE session_bindings SET binding_intent = 'deleting'
            WHERE thread_id = 'legacy-thread'
            """
        )
        with pytest.raises(LifecycleCommandError, match="existing session thread"):
            await commands.create_message(
                thread_id="legacy-thread",
                expression="cron:0 10 * * *",
                text="scheduled",
                timezone="UTC",
                created_by="user",
                channel_id="channel-1",
            )

    assert definition.timezone == "Asia/Shanghai"
