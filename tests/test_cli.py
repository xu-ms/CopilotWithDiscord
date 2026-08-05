import argparse
from pathlib import Path

import pytest

from copilotd import cli


@pytest.mark.asyncio
async def test_foreground_run_returns_restart_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def restart_requested(_settings: object) -> bool:
        return True

    monkeypatch.setenv("COPILOTD_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(cli, "run_discord_bot", restart_requested)
    args = argparse.Namespace(command="run", foreground=True)

    assert await cli.run_command(args) == 75
