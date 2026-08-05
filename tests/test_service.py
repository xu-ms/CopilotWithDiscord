import asyncio
import json
import plistlib
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from copilotd.config import Settings
from copilotd.ops.heartbeat import HeartbeatSnapshot, HeartbeatWriter, read_heartbeat
from copilotd.ops.service import (
    CommandResult,
    ForceRestartOutcome,
    QuiesceFence,
    RestartBlocked,
    RestartSafetySnapshot,
    RestartStormStore,
    ServiceManager,
    SqliteRestartCoordinator,
    _windows_task_contract_errors,
)
from copilotd.storage.database import Database


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        resolved_home=tmp_path / "home",
        discord_token=SecretStr("test-token"),
        setup_verify_timeout_seconds=0.5,
    )


def _heartbeat(now: float, **overrides: Any) -> HeartbeatSnapshot:
    snapshot = HeartbeatSnapshot(
        schema_version=1,
        pid=4321,
        process_generation="generation-1",
        written_at=datetime.fromtimestamp(now, UTC).isoformat().replace("+00:00", "Z"),
        gateway_state="ready",
        gateway_down_since=None,
        runtime_state="ready",
        attached_sessions=0,
        active_submissions=0,
        observed_background_tasks=0,
        active_or_unknown_native_schedules=0,
        remote_steerable_or_unknown_sessions=0,
        pending_interactions=0,
        ingress_queue_depth=0,
        max_reducer_lag_ms=0,
        last_callback_at=None,
        last_reducer_progress_at=None,
        durable_replay_capable=False,
        process_started_at=datetime.fromtimestamp(now - 10, UTC).isoformat().replace("+00:00", "Z"),
    )
    return replace(snapshot, **overrides)


def _write_heartbeat(settings: Settings, snapshot: HeartbeatSnapshot) -> None:
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    settings.heartbeat_path.write_text(
        json.dumps(asdict(snapshot)),
        encoding="utf-8",
    )


def test_ingress_depth_is_restart_protected_work() -> None:
    assert _heartbeat(time.time(), ingress_queue_depth=1).protected_work is True


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.manager: ServiceManager | None = None
        self.mac_pid = 4321
        self.missing_labels: set[str] = set()
        self.launchctl_overrides: dict[str, str] = {}

    def run(self, command: Any, *, check: bool = False) -> CommandResult:
        del check
        call = tuple(str(value) for value in command)
        self.calls.append(call)
        if call[0] == "plutil" and "-extract" in call:
            return self._result(call, stdout="300\n")
        if call[:2] == ("launchctl", "print"):
            label = call[-1].rsplit("/", 1)[-1]
            if label in self.missing_labels:
                return self._result(call, returncode=113, stderr="Could not find service")
            return self._result(
                call,
                stdout=self.launchctl_overrides.get(label, self._launchctl_output(label)),
            )
        if call[0] == "powershell.exe" and "-Action" in call:
            action = call[call.index("-Action") + 1]
            if action in {"Install", "Status"}:
                return self._result(call, stdout=self._windows_status())
        return self._result(call)

    def _launchctl_output(self, label: str) -> str:
        assert self.manager is not None
        definition = plistlib.loads(self.manager.macos_plists()[f"{label}.plist"])
        state = "waiting" if label.endswith("watchdog") else "running"
        lines = [
            f"gui/501/{label} = {{",
            f"  state = {state}",
            "  arguments = {",
            *(f"    {argument}" for argument in definition["ProgramArguments"]),
            "  }",
        ]
        if not label.endswith("watchdog"):
            lines.append(f"  pid = {self.mac_pid}")
        if label.endswith("watchdog"):
            lines.append("  run interval = 300 seconds")
        lines.append("}")
        return "\n".join(lines)

    def _windows_status(self) -> str:
        assert self.manager is not None
        rows = []
        for task, xml in self.manager.windows_task_xml().items():
            rows.append(
                {
                    "name": task,
                    "state": "Ready" if task.endswith("Watchdog") else "Running",
                    "pid": None if task.endswith("Watchdog") else self.mac_pid,
                    "xml": xml,
                }
            )
        return json.dumps(rows, ensure_ascii=False)

    @staticmethod
    def _result(
        command: tuple[str, ...],
        *,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> CommandResult:
        return CommandResult(command, returncode, stdout, stderr)


class AutoAckRestartCoordinator(SqliteRestartCoordinator):
    def request_quiesce(
        self,
        *,
        expected_pid: int,
        expected_generation: str,
        now: float,
    ) -> QuiesceFence:
        fence = super().request_quiesce(
            expected_pid=expected_pid,
            expected_generation=expected_generation,
            now=now,
        )
        self.acknowledge_quiesce(fence, now=now)
        return fence

    def commit_quiesce(self, fence: QuiesceFence, *, now: float) -> None:
        super().commit_quiesce(fence, now=now)
        self.release_quiesce(fence, now=now, reason="test_process_replaced")


class ViolatingRestartCoordinator(AutoAckRestartCoordinator):
    def snapshot_under_fence(
        self,
        fence: QuiesceFence,
        *,
        now: float,
    ) -> RestartSafetySnapshot:
        snapshot = super().snapshot_under_fence(fence, now=now)
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE service_admission_fences
                SET state = 'violated', violation_count = violation_count + 1
                WHERE fence_id = ?
                """,
                (fence.fence_id,),
            )
            connection.commit()
        finally:
            connection.close()
        return snapshot


def _manager(
    settings: Settings,
    runner: FakeRunner,
    *,
    platform_name: str = "darwin",
    topology: str = "bundled-runtime",
) -> ServiceManager:
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "工具" / "copilotd",
        working_directory=settings.resolved_home / "项目",
        platform=platform_name,
        launch_agents_dir=settings.resolved_home / "Library" / "LaunchAgents",
        topology=topology,  # type: ignore[arg-type]
        runtime_argv=[str(settings.resolved_home / "工具" / "runtime"), "--headless"],
        command_runner=runner,
        restart_coordinator=AutoAckRestartCoordinator(settings.database_path),
        uid=501,
        windows_user_id="DOMAIN\\测试用户",
        resume_provider=lambda: None,
        sleep=lambda _: None,
    )
    runner.manager = manager
    return manager


def test_macos_plists_are_deterministic_secure_and_route_logs(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)

    first = manager.macos_plists()
    second = manager.macos_plists()
    definitions = {name: plistlib.loads(content) for name, content in first.items()}

    assert first == second
    assert set(definitions) == {
        "com.github.copilotd.bot.plist",
        "com.github.copilotd.watchdog.plist",
    }
    bot = definitions["com.github.copilotd.bot.plist"]
    watchdog = definitions["com.github.copilotd.watchdog.plist"]
    assert bot["KeepAlive"] is True
    assert bot["RunAtLoad"] is True
    assert bot["ThrottleInterval"] == 30
    assert bot["LowPriorityBackgroundIO"] is False
    assert "ProcessType" not in bot
    assert bot["StandardOutPath"].endswith("boot.log")
    assert bot["StandardErrorPath"].endswith("boot.log")
    assert watchdog["StartInterval"] == 300
    assert watchdog["StandardOutPath"].endswith("watchdog.log")
    serialized = b"".join(first.values())
    assert b"test-token" not in serialized
    assert bot["EnvironmentVariables"]["COPILOTD_SERVICE_SECRETS"] == str(
        settings.service_secrets_path
    )
    assert bot["EnvironmentVariables"]["COPILOTD_MANAGED_SERVICE"] == "1"


def test_sidecar_topology_adds_runtime_definitions(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, topology="sidecar")

    assert set(manager.macos_plists()) == {
        "com.github.copilotd.runtime.plist",
        "com.github.copilotd.bot.plist",
        "com.github.copilotd.watchdog.plist",
    }
    assert set(manager.windows_task_xml()) == {
        "copilotD Runtime",
        "copilotD Bot",
        "copilotD Watchdog",
    }


def test_macos_install_boots_out_every_definition_before_bootstrap(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.launch_agents_dir.mkdir(parents=True)
    stale_runtime = (
        manager.launch_agents_dir / "com.github.copilotd.runtime.plist"
    )
    stale_runtime.write_text("stale", encoding="utf-8")

    manager.install()

    calls = runner.calls
    bootout_indexes = [
        index for index, call in enumerate(calls) if call[:2] == ("launchctl", "bootout")
    ]
    bootstrap_indexes = [
        index for index, call in enumerate(calls) if call[:2] == ("launchctl", "bootstrap")
    ]
    assert len(bootout_indexes) == 3
    assert len(bootstrap_indexes) == 2
    assert max(bootout_indexes) < min(bootstrap_indexes)
    assert not stale_runtime.exists()
    assert any(
        call[:2] == ("launchctl", "bootout")
        and call[-1].endswith("com.github.copilotd.runtime")
        for call in calls
    )
    for label in ("com.github.copilotd.bot", "com.github.copilotd.watchdog"):
        bootstrap = next(
            index
            for index, call in enumerate(calls)
            if call[:2] == ("launchctl", "bootstrap") and call[-1].endswith(f"{label}.plist")
        )
        enable = next(
            index
            for index, call in enumerate(calls)
            if call[:2] == ("launchctl", "enable") and call[-1].endswith(label)
        )
        kickstart = next(
            index
            for index, call in enumerate(calls)
            if call[:2] == ("launchctl", "kickstart") and call[-1].endswith(label)
        )
        assert bootstrap < enable < kickstart
    assert any(call[:2] == ("plutil", "-lint") for call in calls)
    assert settings.service_secrets_path.stat().st_mode & 0o077 == 0


def test_reinstall_uses_current_entrypoint_instead_of_stale_persisted_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    old_runner = FakeRunner()
    old_manager = _manager(settings, old_runner)
    old_manager.install()
    new_entrypoint = settings.resolved_home / "new-version" / "copilotd"
    monkeypatch.setattr(sys, "argv", [str(new_entrypoint), "service", "install"])
    new_runner = FakeRunner()
    new_manager = ServiceManager(
        settings,
        platform="darwin",
        launch_agents_dir=old_manager.launch_agents_dir,
        command_runner=new_runner,
        uid=501,
        resume_provider=lambda: None,
        sleep=lambda _: None,
    )
    new_runner.manager = new_manager

    assert new_manager.entrypoint == new_entrypoint.resolve()
    assert new_manager.status().definition_drift == ("bot", "watchdog")
    new_manager.install()
    definitions = {
        name: plistlib.loads(content)
        for name, content in new_manager.macos_plists().items()
    }
    assert definitions["com.github.copilotd.bot.plist"]["ProgramArguments"][0] == str(
        new_entrypoint.resolve()
    )


def test_effective_launchctl_definition_drift_is_reported(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()
    _write_heartbeat(settings, _heartbeat(time.time()))

    status = manager.status()

    assert status.ready is True
    assert status.process_identity_matches is True
    assert status.definition_drift == ()
    bot_path = manager.launch_agents_dir / "com.github.copilotd.bot.plist"
    bot_path.write_bytes(bot_path.read_bytes() + b"\n")
    drifted = manager.status()
    assert drifted.ready is False
    assert drifted.definition_drift == ("bot",)


def test_windows_tasks_and_powershell_cover_full_current_user_contract(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")
    namespace = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}

    tasks = manager.windows_task_xml()
    bot = ET.fromstring(tasks["copilotD Bot"])
    watchdog = ET.fromstring(tasks["copilotD Watchdog"])

    assert bot.findtext(".//t:ExecutionTimeLimit", namespaces=namespace) == "PT0S"
    assert bot.findtext(".//t:RestartOnFailure/t:Interval", namespaces=namespace) == "PT1M"
    assert bot.findtext(".//t:RestartOnFailure/t:Count", namespaces=namespace) == "255"
    assert bot.findtext(".//t:WakeToRun", namespaces=namespace) == "false"
    assert bot.findtext(".//t:Principal/t:LogonType", namespaces=namespace) == "InteractiveToken"
    assert bot.findtext(".//t:Principal/t:UserId", namespaces=namespace) == "DOMAIN\\测试用户"
    assert bot.findtext(".//t:WorkingDirectory", namespaces=namespace) == str(
        settings.resolved_home / "项目"
    )
    assert watchdog.findtext(".//t:Repetition/t:Interval", namespaces=namespace) == "PT5M"
    assert (
        watchdog.findtext(
            ".//t:RegistrationTrigger/t:Repetition/t:Interval",
            namespaces=namespace,
        )
        == "PT5M"
    )
    assert watchdog.findtext(".//t:StartWhenAvailable", namespaces=namespace) == "true"
    logon_children = [child.tag.rsplit("}", 1)[-1] for child in watchdog.find(
        ".//t:LogonTrigger",
        namespace,
    )]
    registration_children = [
        child.tag.rsplit("}", 1)[-1]
        for child in watchdog.find(".//t:RegistrationTrigger", namespace)
    ]
    assert logon_children == ["Repetition", "Enabled", "UserId"]
    assert registration_children == ["Repetition", "Enabled"]
    assert manager.validate_windows_task_xml() == {
        "copilotD Bot": (),
        "copilotD Watchdog": (),
    }
    invalid = tasks["copilotD Bot"].replace("PT1M", "PT30S").replace(
        ">255<",
        ">999<",
    )
    assert _windows_task_contract_errors(invalid) == (
        "RestartOnFailure Interval must be at least PT1M",
        "RestartOnFailure Count must be between 1 and 255",
    )
    invalid_order = ET.fromstring(tasks["copilotD Watchdog"])
    invalid_logon = invalid_order.find(".//t:LogonTrigger", namespace)
    assert invalid_logon is not None
    invalid_repetition = invalid_logon.find("t:Repetition", namespace)
    assert invalid_repetition is not None
    invalid_logon.remove(invalid_repetition)
    invalid_logon.append(invalid_repetition)
    assert _windows_task_contract_errors(
        ET.tostring(invalid_order, encoding="unicode")
    )[0].startswith("LogonTrigger child order is schema-invalid")
    installer = manager.windows_installer()
    assert "Unregister-ScheduledTask" in installer
    assert "Register-ScheduledTask" in manager.windows_installer()
    assert "Export-ScheduledTask" in manager.windows_installer()
    assert "Start-ScheduledTask" in manager.windows_installer()
    assert "Stop-ScheduledTask" in manager.windows_installer()
    assert "icacls.exe" in manager.windows_installer()
    assert "Get-CimInstance Win32_Process" in manager.windows_installer()
    assert "function Test-CopilotDAction" in installer
    assert "Test-CopilotDAction $line $actionName" in installer
    assert "[regex]::Escape($ActionName)" in installer
    assert "taskkill.exe /PID $hostProcess.ProcessId /T /F" in installer
    assert "process tree did not exit before task unregister" in installer
    assert "$knownTaskNames" in installer
    assert installer.index("Stop-CopilotDTasks $knownTaskNames") < installer.index(
        "Unregister-ScheduledTask"
    )
    uninstall = installer.split(
        "} elseif ($Action -eq 'Uninstall') {",
        maxsplit=1,
    )[1]
    assert uninstall.index("Stop-CopilotDTasks $knownTaskNames") < uninstall.index(
        "Unregister-ScheduledTask"
    )
    assert "test-token" not in manager.windows_installer()
    assert "test-token" not in manager.windows_runner()
    assert "ConvertFrom-Json" in manager.windows_runner()
    assert "2>>&1" not in manager.windows_runner()
    assert "2>&1" in manager.windows_runner()
    assert manager.validate_windows_powershell() == {
        "runner": (),
        "installer": (),
    }


def test_windows_install_is_idempotent_and_verifies_exported_xml(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")

    manager.install()
    manager.install()

    install_calls = [
        call
        for call in runner.calls
        if call[0] == "powershell.exe"
        and "-Action" in call
        and call[call.index("-Action") + 1] == "Install"
    ]
    assert len(install_calls) == 2
    assert (
        settings.data_dir / "runtime" / "copilotd-service.ps1"
    ).read_bytes().startswith(b"\xef\xbb\xbf")
    assert (
        settings.data_dir / "runtime" / "install-service.ps1"
    ).read_bytes().startswith(b"\xef\xbb\xbf")
    status = manager.status()
    assert status.installed is True
    assert status.definition_drift == ()


def test_windows_uninstall_fails_closed_without_safe_process_tree_script(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")
    settings.ensure_directories()

    with pytest.raises(
        RuntimeError,
        match="refusing unsafe Windows uninstall",
    ):
        manager.uninstall()


@pytest.mark.asyncio
async def test_heartbeat_uses_only_current_generation_fence_and_real_metrics(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                runtime_remote_mode, created_at, updated_at
            ) VALUES (
                'thread-1', 'home', '/tmp', 'session-1',
                'attached', 2, 9, 'unknown', 0, 0
            )
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES
              ('session-1', 'stale', 'submission', 'old', 1, 8, 'active', 0, 0),
              ('session-1', 'current', 'observed_background', 'task', 2, 9, 'active', 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO pending_interactions(
                interaction_id, sdk_session_id, runtime_generation, owner_fence_token,
                thread_id, kind, response_plane, expires_at, state, payload,
                created_at, updated_at
            ) VALUES (
                'interaction-1', 'session-1', 2, 9, 'thread-1', 'user_input',
                'direct', 9999999999, 'pending', '{}', 0, 0
            )
            """
        )
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, state, updated_at
            ) VALUES ('session-1', 'schedule-1', 'after', '{}', 'unknown', 0)
            """
        )
        writer = HeartbeatWriter(
            database,
            settings.heartbeat_path,
            metrics_provider=lambda: (3, 27, 123.5),
            resume_provider=lambda: 100.0,
        )
        writer.runtime_state = "ready"
        writer.set_gateway("ready")

        snapshot = await writer.snapshot(now=200.0, last_resume_at=100.0)

    assert snapshot.active_submissions == 0
    assert snapshot.observed_background_tasks == 1
    assert snapshot.pending_interactions == 1
    assert snapshot.remote_steerable_or_unknown_sessions == 1
    assert snapshot.active_or_unknown_native_schedules == 1
    assert snapshot.ingress_queue_depth == 3
    assert snapshot.max_reducer_lag_ms == 27
    assert snapshot.last_callback_at == "1970-01-01T00:02:03.500000Z"
    assert snapshot.last_resume_at == "1970-01-01T00:01:40Z"
    assert snapshot.wake_suppression_until == "1970-01-01T00:02:40Z"


@pytest.mark.asyncio
async def test_gateway_down_freezes_unprotected_heartbeat_after_600_policy(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        writer = HeartbeatWriter(
            database,
            settings.heartbeat_path,
            interval_seconds=0.01,
            gateway_down_seconds=0.01,
            resume_provider=lambda: None,
        )
        writer.runtime_state = "ready"
        writer.set_gateway("reconnecting")
        writer.gateway_down_since = time.time() - 1
        task = asyncio.create_task(writer.run())
        try:
            for _ in range(100):
                if settings.heartbeat_path.exists():
                    break
                await asyncio.sleep(0.005)
            snapshot = read_heartbeat(settings.heartbeat_path)
            first_mtime = settings.heartbeat_path.stat().st_mtime_ns
            await asyncio.sleep(0.04)
            second_mtime = settings.heartbeat_path.stat().st_mtime_ns
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    assert snapshot.gateway_state == "down"
    assert snapshot.heartbeat_frozen is True
    assert snapshot.frozen_reason == "gateway_down_unprotected"
    assert first_mtime == second_mtime


@pytest.mark.asyncio
async def test_heartbeat_reconnecting_before_threshold_keeps_advancing(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        writer = HeartbeatWriter(
            database,
            settings.heartbeat_path,
            gateway_down_seconds=600,
            resume_provider=lambda: None,
        )
        writer.runtime_state = "ready"
        writer.set_gateway("reconnecting")
        writer.gateway_down_since = time.time() - 599
        snapshot = await writer.snapshot()
    assert snapshot.gateway_state == "reconnecting"
    assert snapshot.heartbeat_frozen is False


class RecordingServiceManager(ServiceManager):
    def __init__(
        self,
        settings: Settings,
        *,
        resume_at: float | None = None,
        notifier: Any = None,
        topology: str = "bundled-runtime",
    ) -> None:
        runner = FakeRunner()
        super().__init__(
            settings,
            entrypoint=Path("/tmp/copilotd"),
            platform="darwin",
            launch_agents_dir=settings.resolved_home / "LaunchAgents",
            command_runner=runner,
            restart_coordinator=AutoAckRestartCoordinator(settings.database_path),
            topology=topology,  # type: ignore[arg-type]
            resume_provider=lambda: resume_at,
            notifier=notifier,
        )
        runner.manager = self
        self.runner = runner
        self.restarts = 0

    def _restart_bot(self) -> None:
        self.restarts += 1

    def _heartbeat_generation_matches_verified(
        self,
        snapshot: HeartbeatSnapshot,
    ) -> bool:
        del snapshot
        return True


class RecordingNotifier:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str]] = []

    def notify(self, title: str, message: str) -> None:
        self.notifications.append((title, message))


@pytest.mark.asyncio
async def test_watchdog_sleep_guard_protected_work_gateway_policy_and_storm(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    settings.ensure_directories()
    now = time.time()
    wake_manager = RecordingServiceManager(settings, resume_at=now - 30)
    _write_heartbeat(settings, _heartbeat(now - 300))
    assert wake_manager.watchdog(now=now) == "recent-wake"
    assert wake_manager.restarts == 0

    protected = _heartbeat(
        now,
        gateway_state="down",
        gateway_down_since=datetime.fromtimestamp(now - 601, UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        active_submissions=1,
    )
    _write_heartbeat(settings, protected)
    manager = RecordingServiceManager(settings)
    assert manager.watchdog(now=now) == "protected-gateway-down"
    assert manager.restarts == 0

    unprotected = replace(protected, active_submissions=0)
    _write_heartbeat(settings, unprotected)
    notifier = RecordingNotifier()
    manager = RecordingServiceManager(settings, notifier=notifier)
    assert [manager.watchdog(now=now + offset) for offset in range(3)] == [
        "restarted",
        "restarted",
        "restarted",
    ]
    assert manager.watchdog(now=now + 3) == "restart-storm"
    assert manager.restarts == 3
    assert len(notifier.notifications) == 1
    alert = json.loads(settings.log_paths["alerts"].read_text(encoding="utf-8").splitlines()[-1])
    assert alert["event"] == "watchdog_restart_storm"


@pytest.mark.asyncio
async def test_watchdog_corrupt_state_and_malformed_heartbeat_fail_closed(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    settings.ensure_directories()
    manager = RecordingServiceManager(settings)
    settings.heartbeat_path.write_text("{broken", encoding="utf-8")
    assert manager.watchdog(now=time.time()) == "heartbeat-invalid"
    assert manager.restarts == 0

    _write_heartbeat(settings, _heartbeat(time.time() - 180))
    settings.watchdog_state_path.write_text("{broken", encoding="utf-8")
    assert manager.watchdog(now=time.time()) == "restart-storm"
    assert manager.restarts == 0


@pytest.mark.asyncio
async def test_watchdog_uses_full_durable_restart_blocker_snapshot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                runtime_remote_mode, created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 2, 'off', 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, prompt, requested_mode_snapshot,
                requested_model_config_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES ('queued-1', 'thread-1', 'pending', 'interactive',
                      '{}', 1, 1, 'local_queued', 0, 0)
            """
        )
    now = time.time()
    _write_heartbeat(settings, _heartbeat(now - 180))
    manager = RecordingServiceManager(settings)

    assert manager.watchdog(now=now) == "protected-no-restart"
    assert manager.restarts == 0
    alert = json.loads(settings.log_paths["alerts"].read_text(encoding="utf-8").splitlines()[-1])
    assert alert["durable_blockers"] == ["local_queue:1"]


@pytest.mark.asyncio
async def test_sidecar_watchdog_checkpoints_before_replay_safe_restart(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 2, 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'lease-1', 'submission', 'submission-1',
                      1, 2, 'active', 0, 0)
            """
        )
    now = time.time()
    _write_heartbeat(
        settings,
        _heartbeat(
            now - 180,
            active_submissions=1,
            durable_replay_capable=True,
        ),
    )
    manager = RecordingServiceManager(settings, topology="sidecar")

    assert manager.watchdog(now=now) == "restarted"
    assert manager.restarts == 1
    async with Database(settings.database_path) as database:
        intent = await database.fetchone(
            """
            SELECT kind, state, outcome
            FROM service_restart_intents
            WHERE sdk_session_id = 'session-1'
            """
        )
    assert tuple(intent) == ("checkpoint_replay", "requested", "replay_required")


@pytest.mark.asyncio
async def test_sidecar_runtime_loss_marks_inflight_outcome_unknown_without_bot_kill(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 2, 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'lease-1', 'submission', 'submission-1',
                      1, 2, 'active', 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES
              ('submission-1', 'session-1', 'app_message', 'submitted', 0),
              ('queued-1', 'session-1', 'app_message', 'local_queued', 0),
              ('cancelled-1', 'session-1', 'app_message', 'cancelled', 0),
              ('complete-1', 'session-1', 'app_message', 'semantic_complete', 0)
            """
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, prompt, requested_mode_snapshot,
                requested_model_config_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES
              ('submission-1', 'thread-1', 'in flight', 'interactive',
               '{}', 1, 1, 'submitting', 0, 0),
              ('queued-1', 'thread-1', 'queued', 'interactive',
               '{}', 1, 2, 'local_queued', 0, 0)
            """
        )
    now = time.time()
    _write_heartbeat(
        settings,
        _heartbeat(now, runtime_state="down", active_submissions=1),
    )
    manager = RecordingServiceManager(settings, topology="sidecar")

    assert manager.watchdog(now=now) == "runtime-loss-restarted"
    assert manager.restarts == 1
    async with Database(settings.database_path) as database:
        submissions = await database.fetchall(
            """
            SELECT submission_id, state FROM submissions
            ORDER BY submission_id
            """
        )
        queue_rows = await database.fetchall(
            "SELECT id, state FROM message_queue ORDER BY id"
        )
    assert [tuple(row) for row in submissions] == [
        ("cancelled-1", "cancelled"),
        ("complete-1", "semantic_complete"),
        ("queued-1", "local_queued"),
        ("submission-1", "outcome_unknown"),
    ]
    assert [tuple(row) for row in queue_rows] == [
        ("queued-1", "local_queued"),
        ("submission-1", "submitted"),
    ]


def test_restart_storm_state_is_concurrency_safe(tmp_path: Path) -> None:
    store = RestartStormStore(tmp_path / "watchdog-state.json")
    barrier = threading.Barrier(4)

    def record(index: int) -> bool:
        barrier.wait()
        return store.check_and_record(1000 + index / 1000).suppress

    with ThreadPoolExecutor(max_workers=4) as executor:
        decisions = list(executor.map(record, range(4)))

    assert decisions.count(False) == 3
    assert decisions.count(True) == 1
    payload = json.loads((tmp_path / "watchdog-state.json").read_text(encoding="utf-8"))
    assert len(payload["restarts"]) == 3


def test_restart_storm_policy_spans_real_watchdog_cadence(tmp_path: Path) -> None:
    store = RestartStormStore(tmp_path / "watchdog-state.json")

    assert store.check_and_record(1000).suppress is False
    assert store.check_and_record(1300).suppress is False
    assert store.check_and_record(1600).suppress is False
    decision = store.check_and_record(1900)

    assert decision.suppress is True
    assert decision.count == 3


@pytest.mark.asyncio
async def test_watchdog_never_kills_replacement_or_process_in_startup_grace(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    settings.ensure_directories()
    now = time.time()
    replacement = RecordingServiceManager(settings)
    replacement.runner.mac_pid = 9001
    _write_heartbeat(settings, _heartbeat(now - 180, pid=4321))

    assert replacement.watchdog(now=now) == "replacement-starting"
    assert replacement.restarts == 0

    replacement.runner.mac_pid = 4321
    settings.service_state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "installed_at": now - 30,
            }
        ),
        encoding="utf-8",
    )
    assert replacement.watchdog(now=now) == "startup-grace"
    assert replacement.restarts == 0


@pytest.mark.asyncio
async def test_restart_fails_closed_and_force_marks_ambiguous_work_unknown(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                runtime_remote_mode, created_at, updated_at
            ) VALUES (
                'thread-1', 'home', '/tmp', 'session-1',
                'attached', 1, 3, 'on', 0, 0
            )
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'lease-1', 'submission', 'submission-1',
                      1, 3, 'active', 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message', 'submitted', 0)
            """
        )
        await database.execute(
            """
            INSERT INTO session_operations(
                operation_id, sdk_session_id, runtime_generation, owner_fence_token,
                kind, idempotency_key, input_hash, state, created_at
            ) VALUES ('operation-1', 'session-1', 1, 3, 'send', 'send:1',
                      'hash', 'started', 0)
            """
        )
        await database.execute(
            """
            INSERT INTO pending_interactions(
                interaction_id, sdk_session_id, runtime_generation, owner_fence_token,
                thread_id, kind, response_plane, expires_at, state, payload,
                created_at, updated_at
            ) VALUES ('interaction-1', 'session-1', 1, 3, 'thread-1', 'user_input',
                      'direct', 9999999999, 'pending', '{}', 0, 0)
            """
        )
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, state, updated_at
            ) VALUES ('session-1', 'schedule-1', 'after', '{}', 'active', 0)
            """
        )
    manager.install()
    now = time.time()
    _write_heartbeat(
        settings,
        _heartbeat(
            now,
            active_submissions=1,
            pending_interactions=1,
            remote_steerable_or_unknown_sessions=1,
            active_or_unknown_native_schedules=1,
        ),
    )

    status = manager.status()
    assert status.active_leases.active_submissions == 1
    assert status.active_leases.total == 1
    assert status.exposure.remote_steerable_or_unknown_sessions == 1
    assert status.exposure.active_or_unknown_native_schedules == 1
    assert status.protected_work is True
    with pytest.raises(RestartBlocked) as blocked:
        manager.restart()
    assert "active_liveness:1" in blocked.value.blockers
    receipt = manager.restart(force=True)

    assert receipt.force_outcome is not None
    assert receipt.force_outcome.submissions_unknown == 1
    assert receipt.force_outcome.operations_unknown == 1
    assert receipt.force_outcome.interactions_cancelled == 1
    assert receipt.force_outcome.intents_recorded == 3
    async with Database(settings.database_path) as database:
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = 'submission-1'"
        )
        operation = await database.fetchone(
            "SELECT state FROM session_operations WHERE operation_id = 'operation-1'"
        )
        interaction = await database.fetchone(
            "SELECT state FROM pending_interactions WHERE interaction_id = 'interaction-1'"
        )
        binding = await database.fetchone(
            """
            SELECT runtime_remote_mode, pending_remote_target
            FROM session_bindings WHERE thread_id = 'thread-1'
            """
        )
        schedule = await database.fetchone(
            """
            SELECT state FROM runtime_schedules
            WHERE runtime_schedule_id = 'schedule-1'
            """
        )
        intents = await database.fetchall(
            """
            SELECT kind, state, outcome
            FROM service_restart_intents
            ORDER BY kind
            """
        )
    assert submission["state"] == "outcome_unknown"
    assert operation["state"] == "unknown"
    assert interaction["state"] == "expired"
    assert binding["runtime_remote_mode"] == "unknown"
    assert binding["pending_remote_target"] == "off"
    assert schedule["state"] == "unknown"
    assert [tuple(row) for row in intents] == [
        ("disable_remote", "requested", "unknown"),
        ("drain_session", "requested", "unknown"),
        ("stop_native_schedule", "requested", "unknown"),
    ]


class SlowRestartCoordinator(AutoAckRestartCoordinator):
    def prepare_force(
        self,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> ForceRestartOutcome:
        del snapshot, deadline
        time.sleep(1.5)
        return ForceRestartOutcome(0, 0, 0, 0, 0, 0, 0, False, "slow")


@pytest.mark.asyncio
async def test_force_restart_coordinator_is_bounded_with_durable_fallback(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(update={"restart_drain_timeout_seconds": 0.5})
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "copilotd",
        platform="darwin",
        launch_agents_dir=settings.resolved_home / "LaunchAgents",
        command_runner=runner,
        restart_coordinator=SlowRestartCoordinator(settings.database_path),
        resume_provider=lambda: None,
        uid=501,
        sleep=lambda _: None,
    )
    runner.manager = manager
    manager.install()
    _write_heartbeat(settings, _heartbeat(time.time()))

    started = time.monotonic()
    receipt = manager.restart(force=True)
    elapsed = time.monotonic() - started

    assert elapsed < 1.2
    assert receipt.force_outcome is not None
    assert receipt.force_outcome.bounded is False
    assert "timed out" in receipt.force_outcome.detail


def test_restart_missing_or_malformed_heartbeat_fails_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()

    with pytest.raises(RestartBlocked, match="heartbeat_unavailable"):
        manager.restart(force=True)


@pytest.mark.asyncio
async def test_restart_aborts_if_ingress_arrives_after_fenced_snapshot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "copilotd",
        platform="darwin",
        launch_agents_dir=settings.resolved_home / "LaunchAgents",
        command_runner=runner,
        restart_coordinator=ViolatingRestartCoordinator(settings.database_path),
        resume_provider=lambda: None,
        uid=501,
        sleep=lambda _: None,
    )
    runner.manager = manager
    manager.install()
    _write_heartbeat(settings, _heartbeat(time.time()))
    restart_calls_before = len(
        [call for call in runner.calls if call[:3] == ("launchctl", "kickstart", "-k")]
    )

    with pytest.raises(RestartBlocked, match="admission_fence_violated"):
        manager.restart()

    restart_calls_after = len(
        [call for call in runner.calls if call[:3] == ("launchctl", "kickstart", "-k")]
    )
    assert restart_calls_after == restart_calls_before
    settings.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    settings.heartbeat_path.write_text("not-json", encoding="utf-8")
    with pytest.raises(RestartBlocked, match="heartbeat_unavailable"):
        manager.restart(force=True)


def test_uninstall_retains_durable_state_logs_and_secrets(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()
    settings.database_path.write_bytes(b"durable")
    settings.log_paths["alerts"].write_text("alert\n", encoding="utf-8")

    manager.uninstall()

    assert settings.database_path.read_bytes() == b"durable"
    assert settings.log_paths["alerts"].read_text(encoding="utf-8") == "alert\n"
    assert settings.service_secrets_path.exists()
    assert not any(manager.launch_agents_dir.glob("com.github.copilotd.*.plist"))
