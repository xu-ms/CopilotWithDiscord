import argparse
import asyncio
import json
from pathlib import Path

import pytest

from copilotd.cli import CLI_SCHEMA_VERSION, build_parser, main, run_command
from copilotd.config import Settings
from copilotd.ops.service import ServiceManager


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
