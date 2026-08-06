import asyncio
import json
import os
import plistlib
import stat
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import SecretStr

from copilotd.cli import build_parser, run_command
from copilotd.config import Settings, load_settings
from copilotd.ops.heartbeat import HeartbeatSnapshot, HeartbeatWriter, read_heartbeat
from copilotd.ops.service import ServiceManager
from copilotd.storage.database import Database


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        resolved_home=tmp_path,
        discord_token=SecretStr("test-token"),
        github_token=SecretStr("github-test-token"),
    )


def test_macos_service_templates_use_bundled_topology_without_background_process_type(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    manager = ServiceManager(
        settings,
        entrypoint=tmp_path / "bin" / "copilotd",
        platform="darwin",
        launch_agents_dir=tmp_path / "LaunchAgents",
    )

    definitions = {
        name: plistlib.loads(content) for name, content in manager.macos_plists().items()
    }

    assert set(definitions) == {
        "com.github.copilotd.bot.plist",
        "com.github.copilotd.watchdog.plist",
    }
    bot = definitions["com.github.copilotd.bot.plist"]
    watchdog = definitions["com.github.copilotd.watchdog.plist"]
    assert bot["KeepAlive"] is True
    assert bot["RunAtLoad"] is True
    assert bot["ThrottleInterval"] == 30
    assert "ProcessType" not in bot
    assert watchdog["StartInterval"] == 300
    assert watchdog["RunAtLoad"] is True
    assert "ProcessType" not in watchdog
    serialized = b"".join(manager.macos_plists().values())
    assert b"test-token" not in serialized
    assert b"github-test-token" not in serialized
    assert bot["EnvironmentVariables"]["COPILOTD_SERVICE_SECRETS"] == str(
        settings.service_secrets_path
    )


def test_windows_tasks_have_logon_restart_and_five_minute_watchdog(
    tmp_path: Path,
) -> None:
    manager = ServiceManager(
        _settings(tmp_path),
        entrypoint=tmp_path / "copilotd.exe",
        platform="win32",
    )

    tasks = manager.windows_task_xml()
    bot = ET.fromstring(tasks["copilotD Bot"])
    watchdog = ET.fromstring(tasks["copilotD Watchdog"])
    namespace = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}

    assert bot.findtext(".//t:ExecutionTimeLimit", namespaces=namespace) == "PT0S"
    assert bot.findtext(".//t:RestartOnFailure/t:Interval", namespaces=namespace) == "PT30S"
    assert bot.findtext(".//t:RestartOnFailure/t:Count", namespaces=namespace) == "999"
    assert bot.find(".//t:LogonTrigger", namespace) is not None
    assert watchdog.findtext(".//t:Repetition/t:Interval", namespaces=namespace) == "PT5M"
    assert watchdog.findtext(".//t:StartWhenAvailable", namespaces=namespace) == "true"
    runner = manager.windows_runner()
    assert "test-token" not in runner
    assert "github-test-token" not in runner
    assert str(manager.settings.service_secrets_path) in runner
    assert "$env:COPILOTD_SERVICE_SECRETS" in runner


def test_service_install_fails_before_side_effects_without_managed_token(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(update={"github_token": None})
    launch_agents = tmp_path / "LaunchAgents"
    manager = ServiceManager(
        settings,
        entrypoint=tmp_path / "bin" / "copilotd",
        platform="darwin",
        launch_agents_dir=launch_agents,
    )

    with pytest.raises(RuntimeError, match="COPILOTD_GITHUB_TOKEN"):
        manager.install()

    assert not launch_agents.exists()
    assert not settings.data_dir.exists()


@pytest.mark.asyncio
async def test_setup_missing_managed_token_fails_before_filesystem_or_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(data_dir))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("COPILOTD_DISCORD_TOKEN", "discord-token")
    monkeypatch.delenv("COPILOTD_SERVICE_SECRETS", raising=False)
    monkeypatch.delenv("COPILOTD_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("COPILOT_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(ValueError, match="COPILOTD_GITHUB_TOKEN"):
        await run_command(build_parser().parse_args(["setup"]))

    assert not data_dir.exists()


def test_service_install_persists_tokens_only_in_private_credential_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    manager = ServiceManager(
        settings,
        entrypoint=tmp_path / "bin" / "copilotd",
        platform="darwin",
        launch_agents_dir=tmp_path / "LaunchAgents",
    )
    monkeypatch.setattr(manager, "_install_macos", lambda: None)

    manager.install()

    payload = json.loads(settings.service_secrets_path.read_text(encoding="utf-8"))
    assert payload["discord_token"] == "test-token"
    assert payload["github_token"] == "github-test-token"
    if os.name == "posix":
        assert stat.S_IMODE(settings.service_secrets_path.stat().st_mode) == 0o600

    monkeypatch.setenv("COPILOTD_SERVICE_SECRETS", str(settings.service_secrets_path))
    monkeypatch.delenv("COPILOTD_DISCORD_TOKEN", raising=False)
    monkeypatch.delenv("COPILOTD_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("COPILOT_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    loaded = load_settings()
    assert loaded.discord_token is not None
    assert loaded.github_token is not None
    assert loaded.discord_token.get_secret_value() == "test-token"
    assert loaded.github_token.get_secret_value() == "github-test-token"


@pytest.mark.asyncio
async def test_heartbeat_is_structured_and_reflects_durable_work(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_remote_mode, created_at, updated_at
            ) VALUES (
                'thread-1', 'home', '/tmp', 'session-1',
                'attached', 'off', 0, 0
            )
            """
        )
        writer = HeartbeatWriter(database, settings.heartbeat_path, interval_seconds=0.01)
        writer.runtime_state = "ready"
        writer.set_gateway("ready")
        task = asyncio.create_task(writer.run())
        try:
            for _ in range(50):
                if await asyncio.to_thread(settings.heartbeat_path.exists):
                    break
                await asyncio.sleep(0.01)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    snapshot = read_heartbeat(settings.heartbeat_path)
    assert snapshot.schema_version == 1
    assert snapshot.gateway_state == "ready"
    assert snapshot.runtime_state == "ready"
    assert snapshot.attached_sessions == 1
    assert snapshot.remote_steerable_or_unknown_sessions == 0
    assert snapshot.durable_replay_capable is False
    assert snapshot.protected_work is False


class RecordingServiceManager(ServiceManager):
    def __init__(self, settings: Settings) -> None:
        super().__init__(settings, entrypoint=Path("/tmp/copilotd"), platform="darwin")
        self.restarts = 0

    def _restart_bot(self) -> None:
        self.restarts += 1


def test_watchdog_refuses_protected_restart_and_suppresses_restart_storm(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    settings.ensure_directories()
    manager = RecordingServiceManager(settings)
    now = time.time()
    stale = HeartbeatSnapshot(
        schema_version=1,
        pid=1,
        process_generation="generation",
        written_at=datetime.fromtimestamp(now - 180, UTC).isoformat().replace("+00:00", "Z"),
        gateway_state="ready",
        gateway_down_since=None,
        runtime_state="ready",
        attached_sessions=1,
        active_submissions=1,
        observed_background_tasks=0,
        active_or_unknown_native_schedules=0,
        remote_steerable_or_unknown_sessions=0,
        pending_interactions=0,
        ingress_queue_depth=0,
        max_reducer_lag_ms=0,
        last_callback_at=None,
        last_reducer_progress_at=None,
        durable_replay_capable=False,
    )
    settings.heartbeat_path.write_text(json.dumps(asdict(stale)), encoding="utf-8")

    assert manager.watchdog(now=now) == "protected-no-restart"
    assert manager.restarts == 0

    unprotected = HeartbeatSnapshot(
        **{**asdict(stale), "active_submissions": 0, "attached_sessions": 0}
    )
    settings.heartbeat_path.write_text(json.dumps(asdict(unprotected)), encoding="utf-8")
    assert [manager.watchdog(now=now + offset) for offset in range(3)] == [
        "restarted",
        "restarted",
        "restarted",
    ]
    assert manager.watchdog(now=now + 3) == "restart-storm"
    assert manager.restarts == 3


def test_watchdog_treats_fresh_runtime_down_as_unhealthy(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.ensure_directories()
    manager = RecordingServiceManager(settings)
    now = time.time()
    protected = HeartbeatSnapshot(
        schema_version=1,
        pid=1,
        process_generation="generation",
        written_at=datetime.fromtimestamp(now, UTC).isoformat().replace("+00:00", "Z"),
        gateway_state="down",
        gateway_down_since=None,
        runtime_state="down",
        attached_sessions=1,
        active_submissions=1,
        observed_background_tasks=0,
        active_or_unknown_native_schedules=0,
        remote_steerable_or_unknown_sessions=0,
        pending_interactions=0,
        ingress_queue_depth=0,
        max_reducer_lag_ms=0,
        last_callback_at=None,
        last_reducer_progress_at=None,
        durable_replay_capable=True,
    )
    settings.heartbeat_path.write_text(json.dumps(asdict(protected)), encoding="utf-8")

    assert manager.watchdog(now=now) == "runtime-down-protected"
    assert manager.restarts == 0

    unprotected = HeartbeatSnapshot(
        **{
            **asdict(protected),
            "active_submissions": 0,
            "attached_sessions": 0,
        }
    )
    settings.heartbeat_path.write_text(
        json.dumps(asdict(unprotected)),
        encoding="utf-8",
    )
    assert manager.watchdog(now=now + 1) == "restarted-runtime-down"
    assert manager.restarts == 1
