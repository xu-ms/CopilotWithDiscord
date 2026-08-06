import argparse
import asyncio
import json
import sqlite3
from pathlib import Path

import pytest
from pydantic import SecretStr

from copilotd import cli
from copilotd.cli import CLI_SCHEMA_VERSION, build_parser, main, run_command
from copilotd.config import Settings
from copilotd.ops.contracts import EXPECTED_MIGRATION_VERSIONS
from copilotd.ops.service import ServiceManager
from copilotd.storage.database import Database


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        resolved_home=tmp_path,
    )


@pytest.mark.asyncio
async def test_foreground_run_returns_restart_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def restart_requested(_settings: object) -> bool:
        return True

    settings = _settings(tmp_path).model_copy(update={"discord_token": SecretStr("discord-token")})
    monkeypatch.setattr(cli, "run_discord_bot", restart_requested)
    args = argparse.Namespace(command="run", foreground=True)

    assert await cli.run_command(args, settings=settings) == 75


def test_parser_failure_is_json_with_stable_exit_code(
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()

    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["service"])

    assert exit_info.value.code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["schema_version"] == CLI_SCHEMA_VERSION
    assert payload["ok"] is False
    assert payload["error"]["code"] == "usage_error"
    assert payload["error"]["exit_code"] == 2


def test_configuration_failure_has_no_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(tmp_path / "logs"))

    with pytest.raises(SystemExit) as exit_info:
        main(["run"])

    assert exit_info.value.code == 2
    error = capsys.readouterr().err
    payload = json.loads(error)
    assert payload["error"]["code"] == "configuration_error"
    assert "Traceback" not in error


def test_service_status_output_uses_versioned_json_envelope(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(tmp_path)
    manager = ServiceManager(settings, platform="unsupported")
    args = argparse.Namespace(command="service", service_command="status")

    assert asyncio.run(run_command(args, settings=settings, manager=manager)) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == CLI_SCHEMA_VERSION
    assert payload["ok"] is True
    assert payload["command"] == "service.status"
    assert payload["result"]["schema_version"] == 1
    assert payload["result"]["effective_state"] == "not-installed"


@pytest.mark.asyncio
async def test_schema_dependent_service_command_applies_forward_operations_migrations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute("DROP TABLE service_admission_fences")
        await database.execute("DROP TABLE service_restart_intents")
        await database.execute("DELETE FROM schema_migrations WHERE version BETWEEN 40 AND 44")
    manager = ServiceManager(settings, platform="unsupported")
    args = argparse.Namespace(command="service", service_command="status")

    assert await run_command(args, settings=settings, manager=manager) == 0
    capsys.readouterr()

    async with Database(settings.database_path) as database:
        versions = await database.fetchall("SELECT version FROM schema_migrations ORDER BY version")
        tables = await database.fetchall(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name IN ('service_restart_intents', 'service_admission_fences')
            ORDER BY name
            """
        )
    assert [row["version"] for row in versions] == list(EXPECTED_MIGRATION_VERSIONS)
    assert [row["name"] for row in tables] == [
        "service_admission_fences",
        "service_restart_intents",
    ]


@pytest.mark.asyncio
async def test_force_restart_applies_operations_migrations_before_coordination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute("DROP TABLE service_admission_fences")
        await database.execute("DROP TABLE service_restart_intents")
        await database.execute("DELETE FROM schema_migrations WHERE version BETWEEN 40 AND 44")
    manager = ServiceManager(settings, platform="unsupported")
    observed = False

    class CoordinationReached(Exception):
        pass

    def restart(*, force: bool) -> None:
        nonlocal observed
        assert force is True
        connection = sqlite3.connect(settings.database_path)
        try:
            names = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'table'
                    """
                )
            }
        finally:
            connection.close()
        observed = {
            "service_restart_intents",
            "service_admission_fences",
        } <= names
        raise CoordinationReached

    monkeypatch.setattr(manager, "restart", restart)
    args = argparse.Namespace(
        command="service",
        service_command="restart",
        force=True,
    )

    with pytest.raises(CoordinationReached):
        await run_command(args, settings=settings, manager=manager)
    assert observed is True


@pytest.mark.asyncio
async def test_unmanaged_command_refuses_pending_windows_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    manager = ServiceManager(settings, platform="win32")
    monkeypatch.setattr(
        Settings,
        "windows_legacy_layout_pending",
        lambda _self: True,
    )
    args = argparse.Namespace(command="service", service_command="status")

    with pytest.raises(ValueError, match=r"setup.*service install"):
        await run_command(args, settings=settings, manager=manager)
    assert not settings.database_path.exists()


@pytest.mark.asyncio
async def test_authenticated_managed_replacement_can_start_during_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path).model_copy(update={"discord_token": SecretStr("test-token")})
    started = False

    async def run_replacement(selected: Settings) -> None:
        nonlocal started
        assert selected is settings
        started = True

    monkeypatch.setattr(
        Settings,
        "windows_legacy_layout_pending",
        lambda _self: True,
    )
    monkeypatch.setattr(
        Settings,
        "windows_migration_replacement_authorized",
        lambda _self: True,
    )
    monkeypatch.setattr(
        "copilotd.cli.run_discord_bot",
        run_replacement,
    )
    args = argparse.Namespace(command="run", foreground=True)

    assert await run_command(args, settings=settings) == 0
    assert started is True
    with pytest.raises(ValueError, match=r"setup.*service install"):
        await run_command(
            argparse.Namespace(
                command="service",
                service_command="status",
            ),
            settings=settings,
            manager=ServiceManager(settings, platform="win32"),
        )


@pytest.mark.asyncio
async def test_install_command_is_only_cli_path_that_adopts_legacy_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path).model_copy(update={"discord_token": SecretStr("discord-token")})

    class AdoptionReached(Exception):
        pass

    class SuccessfulReport:
        def require_success(self) -> None:
            return

        def as_dict(self) -> dict[str, object]:
            return {}

    class SuccessfulPreflight:
        async def run(self) -> SuccessfulReport:
            return SuccessfulReport()

    manager = ServiceManager(settings, platform="win32")

    def adopt(_self: Settings, **kwargs: object) -> bool:
        assert kwargs["platform_name"] == "win32"
        assert kwargs["service_quiescer"] == (manager.quiesce_windows_legacy_layout)
        assert kwargs["managed_replacement_token_hash"] == (manager.handoff_token_hash)
        raise AdoptionReached

    monkeypatch.setattr(Settings, "adopt_legacy_windows_layout", adopt)
    args = argparse.Namespace(
        command="service",
        service_command="install",
    )

    with pytest.raises(AdoptionReached):
        await run_command(
            args,
            settings=settings,
            manager=manager,
            preflight=SuccessfulPreflight(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_managed_service_logging_disables_stderr_duplication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(tmp_path)
    manager = ServiceManager(settings, platform="unsupported")
    calls: list[tuple[str, Path | None, bool]] = []
    monkeypatch.setenv("COPILOTD_MANAGED_SERVICE", "1")
    monkeypatch.setattr(
        "copilotd.cli.configure_logging",
        lambda level, log_dir, *, stderr: calls.append((level, log_dir, stderr)),
    )
    args = argparse.Namespace(command="service", service_command="status")

    assert await run_command(args, settings=settings, manager=manager) == 0
    capsys.readouterr()
    assert calls == [(settings.log_level, settings.log_dir, False)]
