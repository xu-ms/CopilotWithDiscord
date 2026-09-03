import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.projects import (
    ProjectConflictError,
    ProjectPathError,
    ProjectRegistry,
    ProjectSource,
)
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_explicit_project_overrides_home_without_mutating_existing_session(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    first_repo = tmp_path / "first"
    second_repo = tmp_path / "second"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(first_repo.mkdir)
    await asyncio.to_thread(second_repo.mkdir)

    async with Database(tmp_path / "projects.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()

        implicit = await projects.resolve("channel-1")
        assert implicit.source == ProjectSource.IMPLICIT_HOME
        assert implicit.cwd == home

        explicit = await projects.bind("channel-1", first_repo)
        assert explicit.source == ProjectSource.EXPLICIT
        assert explicit.cwd == first_repo

        bindings = SessionBindingRepository(database)
        session = await bindings.create(
            thread_id="thread-1",
            sdk_session_id=str(uuid4()),
            cwd_snapshot=explicit.cwd,
            project_source=explicit.source.value,
            project_id=explicit.project_id,
        )

        unbound = await projects.unbind("channel-1")
        rebound = await projects.bind("channel-1", second_repo)
        unchanged = await bindings.by_thread("thread-1")
        rows = await database.fetchall(
            "SELECT cwd, state, config_version FROM projects ORDER BY config_version"
        )

    assert unbound.source == ProjectSource.IMPLICIT_HOME
    assert unbound.cwd == home
    assert rebound.cwd == second_repo
    assert rebound.config_version == 2
    assert unchanged is not None
    assert unchanged.cwd_snapshot == session.cwd_snapshot == first_repo
    assert [dict(row) for row in rows] == [
        {"cwd": str(first_repo), "state": "retired", "config_version": 1},
        {"cwd": str(second_repo), "state": "active", "config_version": 2},
    ]


@pytest.mark.asyncio
async def test_home_is_persisted_and_cannot_silently_drift(tmp_path: Path) -> None:
    first_home = tmp_path / "home-a"
    second_home = tmp_path / "home-b"
    await asyncio.to_thread(first_home.mkdir)
    await asyncio.to_thread(second_home.mkdir)

    async with Database(tmp_path / "home.sqlite3") as database:
        first = ProjectRegistry(database, resolved_home=first_home)
        await first.initialize()
        second = ProjectRegistry(database, resolved_home=second_home)

        with pytest.raises(ProjectPathError, match="differs"):
            await second.initialize()


@pytest.mark.asyncio
async def test_project_path_must_be_an_existing_directory(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)
    file_path = tmp_path / "file"
    await asyncio.to_thread(file_path.write_text, "not a directory")

    async with Database(tmp_path / "invalid.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()

        with pytest.raises(ProjectPathError, match="does not exist"):
            await projects.bind("channel-1", tmp_path / "missing")
        with pytest.raises(ProjectPathError, match="not a directory"):
            await projects.bind("channel-1", file_path)


@pytest.mark.asyncio
async def test_channel_settings_are_independent_from_project_binding(tmp_path: Path) -> None:
    home = tmp_path / "home"
    await asyncio.to_thread(home.mkdir)

    async with Database(tmp_path / "settings.sqlite3") as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        await projects.set_layout("channel-1", "forum")
        await projects.set_mention_required("channel-1", True)
        await projects.unbind("channel-1")
        settings = await projects.channel_settings("channel-1")

    assert settings == ("forum", True, 3)


@pytest.mark.asyncio
async def test_stale_custom_agent_update_cannot_change_persisted_configuration(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    repo = tmp_path / "repo"
    await asyncio.to_thread(home.mkdir)
    await asyncio.to_thread(repo.mkdir)
    path = tmp_path / "agent-version.sqlite3"

    async with Database(path) as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        project = await projects.bind("channel-agent", repo)
        accepted = await projects.set_custom_agent(
            "channel-agent",
            name="reviewer",
            description="accepted description",
            prompt="accepted prompt",
            tools=("read",),
            expected_version=project.config_version,
        )
        with pytest.raises(ProjectConflictError, match="expected"):
            await projects.set_custom_agent(
                "channel-agent",
                name="reviewer",
                description="stale description",
                prompt="stale prompt",
                tools=("write",),
                expected_version=project.config_version,
            )
        assert accepted.project_config_version == project.config_version + 1

    async with Database(path) as database:
        projects = ProjectRegistry(database, resolved_home=home)
        await projects.initialize()
        agents = await projects.list_custom_agents("channel-agent")

    assert len(agents) == 1
    assert agents[0].description == "accepted description"
    assert agents[0].prompt == "accepted prompt"
    assert agents[0].tools == ("read",)
