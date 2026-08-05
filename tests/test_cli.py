import argparse
import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from copilotd.cli import CLI_SCHEMA_VERSION, build_parser, main, run_command
from copilotd.config import Settings
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
async def test_schema_dependent_service_command_upgrades_schema_seven(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute("DROP TABLE service_admission_fences")
        await database.execute("DROP TABLE service_restart_intents")
        await database.execute(
            "DELETE FROM schema_migrations WHERE version IN (8, 9, 10, 11)"
        )
    manager = ServiceManager(settings, platform="unsupported")
    args = argparse.Namespace(command="service", service_command="status")

    assert await run_command(args, settings=settings, manager=manager) == 0
    capsys.readouterr()

    async with Database(settings.database_path) as database:
        versions = await database.fetchall(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
        tables = await database.fetchall(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name IN ('service_restart_intents', 'service_admission_fences')
            ORDER BY name
            """
        )
    assert [row["version"] for row in versions] == list(range(1, 12))
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
        await database.execute(
            "DELETE FROM schema_migrations WHERE version IN (8, 9, 10, 11)"
        )
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
        lambda level, log_dir, *, stderr: calls.append(
            (level, log_dir, stderr)
        ),
    )
    args = argparse.Namespace(command="service", service_command="status")

    assert await run_command(args, settings=settings, manager=manager) == 0
    capsys.readouterr()
    assert calls == [(settings.log_level, settings.log_dir, False)]
