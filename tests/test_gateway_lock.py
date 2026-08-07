import errno
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from copilotd import discord_app
from copilotd.config import Settings
from copilotd.ops import gateway_lock
from copilotd.ops.gateway_lock import GatewayAlreadyRunning, GatewayInstanceLock


def test_gateway_lock_rejects_competing_owner_and_contains_no_identity(tmp_path: Path) -> None:
    path = tmp_path / "fixed-cache" / "gateway.lock"
    first = GatewayInstanceLock(path=path)
    second = GatewayInstanceLock(path=path)

    with first:
        assert first.acquired
        with pytest.raises(GatewayAlreadyRunning, match="already running") as conflict:
            second.acquire()
        assert str(conflict.value) == (
            "another copilotD gateway is already running for this OS user"
        )
        assert path.read_bytes() == b""
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.parent.stat().st_mode & 0o777 == 0o700

    assert not first.acquired


def test_gateway_lock_can_be_acquired_again_after_release(tmp_path: Path) -> None:
    path = tmp_path / "fixed-cache" / "gateway.lock"

    with GatewayInstanceLock(path=path):
        pass
    with GatewayInstanceLock(path=path) as restarted:
        assert restarted.acquired


def test_gateway_lock_is_process_wide_and_exit_releases_it(tmp_path: Path) -> None:
    path = tmp_path / "fixed-cache" / "gateway.lock"
    script = """
import sys
from pathlib import Path
from copilotd.ops.gateway_lock import GatewayInstanceLock

with GatewayInstanceLock(path=Path(sys.argv[1])):
    print("locked", flush=True)
    sys.stdin.readline()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(GatewayAlreadyRunning):
            GatewayInstanceLock(path=path).acquire()
        assert process.stdin is not None
        process.stdin.write("\n")
        process.stdin.flush()
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)

    with GatewayInstanceLock(path=path) as restarted:
        assert restarted.acquired


def test_settings_directory_overrides_cannot_change_gateway_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_home = tmp_path / "authoritative-home"
    monkeypatch.setattr(
        gateway_lock,
        "_authoritative_user_home",
        lambda _platform: fixed_home,
    )
    first_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "first-data",
        cache_dir=tmp_path / "first-cache",
        log_dir=tmp_path / "first-logs",
    )
    second_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "second-data",
        cache_dir=tmp_path / "second-cache",
        log_dir=tmp_path / "second-logs",
    )
    first = GatewayInstanceLock(platform_name="darwin")
    second = GatewayInstanceLock(platform_name="darwin")

    assert first_settings.cache_dir != second_settings.cache_dir
    expected = fixed_home / "Library" / "Caches" / "copilotd" / "gateway.lock"
    assert first.path == second.path == expected
    with first:
        with pytest.raises(GatewayAlreadyRunning):
            second.acquire()


@pytest.mark.parametrize(
    ("platform_name", "relative_path"),
    [
        ("darwin", Path("Library/Caches/copilotd/gateway.lock")),
        ("linux", Path(".cache/copilotd/gateway.lock")),
        ("win32", Path("AppData/Local/copilotd/cache/gateway.lock")),
    ],
)
def test_gateway_lock_path_ignores_caller_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    platform_name: str,
    relative_path: Path,
) -> None:
    fixed_home = tmp_path / "os-account-home"
    monkeypatch.setattr(
        gateway_lock,
        "_authoritative_user_home",
        lambda _platform: fixed_home,
    )
    monkeypatch.setenv("HOME", str(tmp_path / "spoofed-home"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "spoofed-xdg-cache"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "spoofed-local-app-data"))

    first = gateway_lock.gateway_lock_path(platform_name)
    monkeypatch.setenv("HOME", str(tmp_path / "different-home"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "different-xdg-cache"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "different-local-app-data"))
    second = gateway_lock.gateway_lock_path(platform_name)

    assert first == second == fixed_home / relative_path


def test_windows_locking_is_nonblocking_and_released(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = SimpleNamespace(locked=False)

    def locking(_descriptor: int, mode: int, _size: int) -> None:
        if mode == 1:
            if state.locked:
                raise OSError(errno.EACCES, "locked")
            state.locked = True
        else:
            state.locked = False

    monkeypatch.setitem(
        sys.modules,
        "msvcrt",
        SimpleNamespace(LK_NBLCK=1, LK_UNLCK=2, locking=locking),
    )
    path = tmp_path / "windows-cache" / "gateway.lock"
    first = GatewayInstanceLock(path=path, platform_name="win32")
    second = GatewayInstanceLock(path=path, platform_name="win32")

    with first:
        with pytest.raises(GatewayAlreadyRunning):
            second.acquire()
    with second:
        assert second.acquired


@pytest.mark.asyncio
async def test_run_discord_bot_holds_lock_for_full_gateway_lifetime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeLock:
        def __enter__(self) -> None:
            events.append("lock-acquired")

        def __exit__(self, *_args: object) -> None:
            events.append("lock-released")

    class FakeBot:
        restart_requested = False
        _fatal_worker_error = None

        def __init__(self, _settings: Settings) -> None:
            events.append("bot-created")
            self.closed = False

        async def start(self, token: str, *, reconnect: bool) -> None:
            assert token == "discord-token"
            assert reconnect is True
            events.append("bot-started")

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True
            events.append("bot-closed")

    monkeypatch.setattr(discord_app, "GatewayInstanceLock", FakeLock)
    monkeypatch.setattr(discord_app, "CopilotDiscordBot", FakeBot)
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "override-cache",
        log_dir=tmp_path / "logs",
        discord_token=SecretStr("discord-token"),
    )

    assert await discord_app.run_discord_bot(settings) is False
    assert events == [
        "lock-acquired",
        "bot-created",
        "bot-started",
        "bot-closed",
        "lock-released",
    ]
