from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from copilotd.core.projects import (
    ProjectBindingError,
    ProjectConflictError,
    ProjectCustomAgentEntry,
    ProjectDirectoryEntry,
    ProjectEnvEntry,
    ProjectPathError,
    ProjectRegistry,
    ProjectValidationError,
)
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_project_config_crud_is_versioned_and_secret_safe(tmp_path: Path) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    skill_dir = tmp_path / "skills"
    plugin_dir = tmp_path / "plugins"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(repo.mkdir)
    await asyncio.to_thread(skill_dir.mkdir)
    await asyncio.to_thread(plugin_dir.mkdir)

    async with Database(tmp_path / "project-config.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.bind("channel-1", repo)

        env = await projects.set_project_env("channel-1", "TOKEN", "secret")
        mcp = await projects.set_mcp_server(
            "channel-1",
            name="local-mcp",
            transport="stdio",
            config={
                "command": "node",
                "args": ["server.js"],
                "headers": {"Authorization": "Bearer secret"},
                "env": {"TOKEN": "secret"},
                "project_env_refs": ["TOKEN"],
            },
        )
        skill = await projects.set_skill_dir("channel-1", path=str(skill_dir))
        plugin = await projects.set_plugin_dir("channel-1", path=str(plugin_dir), enabled=False)
        agent = await projects.set_custom_agent(
            "channel-1",
            name="reviewer",
            description="Code reviewer",
            prompt="Review the patch",
            tools=["tool.review", "tool.diff"],
        )

        envs = await projects.list_project_env("channel-1")
        envs_revealed = await projects.list_project_env("channel-1", reveal=True)
        mcps = await projects.list_mcp_servers("channel-1")
        mcps_revealed = await projects.list_mcp_servers("channel-1", reveal=True)
        skills = await projects.list_skill_dirs("channel-1")
        plugins = await projects.list_plugin_dirs("channel-1")
        agents = await projects.list_custom_agents("channel-1")
        snapshot = await projects.resolve("channel-1")

    assert env.project_config_version == 2
    assert mcp.project_config_version == 3
    assert skill.project_config_version == 4
    assert plugin.project_config_version == 5
    assert agent.project_config_version == 6
    assert snapshot.config_version == 6
    assert envs == [ProjectEnvEntry(env.project_id, "channel-1", "TOKEN", "[redacted]", 6)]
    assert envs_revealed == [ProjectEnvEntry(env.project_id, "channel-1", "TOKEN", "secret", 6)]
    assert mcps[0].config["headers"]["Authorization"] == "[redacted]"
    assert mcps[0].config["env"]["TOKEN"] == "[redacted]"
    assert mcps_revealed[0].config["headers"]["Authorization"] == "Bearer secret"
    assert mcps_revealed[0].config["env"]["TOKEN"] == "secret"
    assert skills == [ProjectDirectoryEntry(skill.project_id, "channel-1", skill_dir, True, 6)]
    assert plugins == [ProjectDirectoryEntry(plugin.project_id, "channel-1", plugin_dir, False, 6)]
    assert agents == [
        ProjectCustomAgentEntry(
            agent.project_id,
            "channel-1",
            "reviewer",
            "Code reviewer",
            "Review the patch",
            ("tool.review", "tool.diff"),
            True,
            6,
        )
    ]


@pytest.mark.asyncio
async def test_project_config_requires_explicit_binding(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)

    async with Database(tmp_path / "project-gate.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()

        with pytest.raises(ProjectBindingError):
            await projects.set_project_env("channel-1", "TOKEN", "secret")
        with pytest.raises(ProjectBindingError):
            await projects.set_mcp_server(
                "channel-1",
                name="local-mcp",
                transport="stdio",
                config={"command": "node"},
            )
        with pytest.raises(ProjectBindingError):
            await projects.set_skill_dir("channel-1", path=str(tmp_path / "missing"))


@pytest.mark.asyncio
async def test_project_config_validation_and_conflicts(tmp_path: Path) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    skill_dir = tmp_path / "skills"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(repo.mkdir)
    await asyncio.to_thread(skill_dir.mkdir)

    async with Database(tmp_path / "project-validation.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.bind("channel-1", repo)

        with pytest.raises(ProjectValidationError):
            await projects.set_project_env("channel-1", "not-valid", "x")
        with pytest.raises(ProjectValidationError):
            await projects.set_mcp_server(
                "channel-1",
                name="local-mcp",
                transport="stdio",
                config={"args": ["ok"], "headers": {"Bad Header": "x"}},
            )
        with pytest.raises(ProjectValidationError):
            await projects.set_custom_agent(
                "channel-1",
                name="reviewer",
                description="Code reviewer",
                prompt="Review",
                tools=["bad tool name"],
            )
        with pytest.raises(ProjectConflictError):
            await projects.set_project_env(
                "channel-1",
                "TOKEN",
                "secret",
                expected_version=0,
            )
        with pytest.raises(ProjectPathError):
            await projects.set_skill_dir("channel-1", path=str(tmp_path / "missing"))

        changed = await projects.set_skill_dir("channel-1", path=str(skill_dir))
        assert changed.project_config_version == 2
        await asyncio.to_thread(skill_dir.rmdir)
        toggled = await projects.toggle_skill_dir(
            "channel-1",
            path=str(skill_dir),
            enabled=False,
        )
        assert toggled.enabled is False
        assert await projects.remove_skill_dir("channel-1", path=str(skill_dir)) is True
        assert await projects.remove_skill_dir("channel-1", path=str(skill_dir)) is False
