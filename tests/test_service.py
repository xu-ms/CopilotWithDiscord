import asyncio
import json
import plistlib
import sqlite3
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
    ForcePreparationUncertain,
    ForceRestartOutcome,
    QuiesceFence,
    RestartBlocked,
    RestartSafetySnapshot,
    RestartStormStore,
    ServiceError,
    ServiceManager,
    SqliteRestartCoordinator,
    _windows_task_contract_errors,
)
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


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
        service_control_protocol=2,
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
        self.process_started_at = time.time() - 10
        self.process_running = True
        self.missing_labels: set[str] = set()
        self.missing_tasks: set[str] = set()
        self.launchctl_overrides: dict[str, str] = {}

    def run(self, command: Any, *, check: bool = False) -> CommandResult:
        del check
        call = tuple(str(value) for value in command)
        self.calls.append(call)
        if call[:2] == ("launchctl", "bootout") and call[-1].endswith("com.github.copilotd.bot"):
            self.process_running = False
        if call[:2] == ("launchctl", "bootstrap") and call[-1].endswith(
            "com.github.copilotd.bot.plist"
        ):
            self.process_running = True
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
        if call[0] == "powershell.exe" and "-Command" in call and "ProcessId =" in call[-1]:
            return self._result(
                call,
                stdout=(
                    datetime.fromtimestamp(
                        self.process_started_at,
                        UTC,
                    ).isoformat()
                    + "\n"
                    if self.process_running
                    else "__COPILOTD_PROCESS_ABSENT__\n"
                ),
            )
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
                    "state": (
                        "Missing"
                        if task in self.missing_tasks
                        else "Ready"
                        if task.endswith("Watchdog")
                        else "Running"
                    ),
                    "pid": (
                        None
                        if task in self.missing_tasks or task.endswith("Watchdog")
                        else self.mac_pid
                    ),
                    "process_started_at": (
                        None
                        if task in self.missing_tasks or task.endswith("Watchdog")
                        else datetime.fromtimestamp(
                            self.process_started_at,
                            UTC,
                        ).isoformat()
                    ),
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
        expected_process_started_at: float,
        handoff_token: str = "",
        now: float,
    ) -> QuiesceFence:
        fence = super().request_quiesce(
            expected_pid=expected_pid,
            expected_generation=expected_generation,
            expected_process_started_at=expected_process_started_at,
            handoff_token=handoff_token,
            now=now,
        )
        self.acknowledge_quiesce(fence, now=now)
        return fence


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


class LateProducerAfterPrepareCoordinator(AutoAckRestartCoordinator):
    def prepare_force(
        self,
        fence: QuiesceFence,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> ForceRestartOutcome:
        outcome = super().prepare_force(
            fence,
            snapshot,
            deadline=deadline,
        )
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE service_admission_fences
                SET producer_count = producer_count + 1,
                    violation_count = violation_count + 1
                WHERE fence_id = ? AND state = 'prepared'
                """,
                (fence.fence_id,),
            )
            connection.commit()
        finally:
            connection.close()
        return outcome


class LegacyAdmissionRaceCoordinator(AutoAckRestartCoordinator):
    def __init__(self, database_path: Path) -> None:
        super().__init__(database_path)
        self.snapshot_calls = 0

    def snapshot(self, *, now: float) -> RestartSafetySnapshot:
        snapshot = super().snapshot(now=now)
        self.snapshot_calls += 1
        if self.snapshot_calls == 1:
            connection = self._connect()
            try:
                connection.execute(
                    """
                    INSERT INTO session_bindings(
                        thread_id, project_source, cwd_snapshot,
                        sdk_session_id, attachment_state,
                        runtime_generation, owner_fence_token,
                        created_at, updated_at
                    ) VALUES (
                        'legacy-race-thread', 'home', '/tmp',
                        'legacy-race-session', 'attached', 1, 3, ?, ?
                    )
                    """,
                    (now, now),
                )
                connection.execute(
                    """
                    INSERT INTO session_operations(
                        operation_id, sdk_session_id, runtime_generation,
                        owner_fence_token, kind, idempotency_key,
                        input_hash, state, created_at
                    ) VALUES (
                        'legacy-race-operation', 'legacy-race-session', 1, 3,
                        'send', 'legacy-race-send', 'hash', 'started', ?
                    )
                    """,
                    (now,),
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
        process_start_provider=lambda _: (
            runner.process_started_at if runner.process_running else None
        ),
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
    stale_runtime = manager.launch_agents_dir / "com.github.copilotd.runtime.plist"
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
        call[:2] == ("launchctl", "bootout") and call[-1].endswith("com.github.copilotd.runtime")
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
        process_start_provider=lambda _: new_runner.process_started_at,
        uid=501,
        resume_provider=lambda: None,
        sleep=lambda _: None,
    )
    new_runner.manager = new_manager

    assert new_manager.entrypoint == new_entrypoint.resolve()
    assert new_manager.status().definition_drift == ("bot", "watchdog")
    new_manager.install()
    definitions = {
        name: plistlib.loads(content) for name, content in new_manager.macos_plists().items()
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


def test_post_install_accepts_new_identity_after_legacy_heartbeat(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    old = _heartbeat(
        time.time() - 30,
        process_generation="legacy-generation",
        process_started_at=None,
    )
    _write_heartbeat(settings, old)

    receipt = manager.install()
    replacement_started_at = time.time()
    runner.process_started_at = replacement_started_at
    _write_heartbeat(
        settings,
        _heartbeat(
            time.time(),
            process_generation="replacement-generation",
            process_started_at=datetime.fromtimestamp(
                replacement_started_at,
                UTC,
            )
            .isoformat()
            .replace("+00:00", "Z"),
        ),
    )

    status = manager.verify_post_install(receipt, timeout_seconds=0.1)
    assert status.ready is True
    assert status.process_generation == "replacement-generation"


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
    logon_children = [
        child.tag.rsplit("}", 1)[-1]
        for child in watchdog.find(
            ".//t:LogonTrigger",
            namespace,
        )
    ]
    registration_children = [
        child.tag.rsplit("}", 1)[-1]
        for child in watchdog.find(".//t:RegistrationTrigger", namespace)
    ]
    assert logon_children == ["Enabled", "Repetition", "UserId"]
    assert registration_children == ["Enabled", "Repetition"]
    assert manager.validate_windows_task_xml() == {
        "copilotD Bot": (),
        "copilotD Watchdog": (),
    }
    invalid = (
        tasks["copilotD Bot"]
        .replace("PT1M", "PT30S")
        .replace(
            ">255<",
            ">999<",
        )
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
    assert _windows_task_contract_errors(ET.tostring(invalid_order, encoding="unicode"))[
        0
    ].startswith("LogonTrigger child order is schema-invalid")
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
    assert "RestartRuntime" in installer
    assert "Start-ScheduledTask -TaskName 'copilotD Runtime'" in installer
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
        (settings.data_dir / "runtime" / "copilotd-service.ps1")
        .read_bytes()
        .startswith(b"\xef\xbb\xbf")
    )
    assert (
        (settings.data_dir / "runtime" / "install-service.ps1")
        .read_bytes()
        .startswith(b"\xef\xbb\xbf")
    )
    status = manager.status()
    assert status.installed is True
    assert status.definition_drift == ()


def test_windows_sidecar_runtime_restart_uses_runtime_task(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(
        settings,
        runner,
        platform_name="win32",
        topology="sidecar",
    )

    manager._restart_runtime()

    assert any(
        call[0] == "powershell.exe"
        and "-Action" in call
        and call[call.index("-Action") + 1] == "RestartRuntime"
        for call in runner.calls
    )


def test_windows_process_identity_probe_distinguishes_alive_and_dead(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "copilotd.exe",
        platform="win32",
        command_runner=runner,
        windows_user_id="DOMAIN\\test",
    )

    assert manager.process_identity_alive(
        pid=runner.mac_pid,
        process_started_at=runner.process_started_at,
    )
    runner.process_running = False
    assert not manager.process_identity_alive(
        pid=runner.mac_pid,
        process_started_at=runner.process_started_at,
    )

    runner.process_running = True
    assert manager.process_identity_alive(
        pid=runner.mac_pid,
        process_started_at=None,
    )
    runner.process_running = False
    assert not manager.process_identity_alive(
        pid=runner.mac_pid,
        process_started_at=None,
    )


def test_windows_process_identity_probe_fails_closed_on_probe_ambiguity(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "copilotd.exe",
        platform="win32",
        command_runner=runner,
        windows_user_id="DOMAIN\\test",
    )

    runner.run = lambda command, check=False: CommandResult(  # type: ignore[method-assign]
        tuple(command),
        1,
        "",
        "access denied",
    )
    with pytest.raises(ServiceError, match="could not query Windows"):
        manager.process_identity_alive(
            pid=12345,
            process_started_at=None,
        )

    runner.run = lambda command, check=False: CommandResult(  # type: ignore[method-assign]
        tuple(command),
        0,
        "",
        "",
    )
    with pytest.raises(ServiceError, match="without a start identity"):
        manager.process_identity_alive(
            pid=12345,
            process_started_at=None,
        )


def test_windows_fail_closed_termination_runs_verified_stop_bot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")

    manager._terminate_bot_fail_closed()

    actions = [
        call[call.index("-Action") + 1]
        for call in runner.calls
        if call[0] == "powershell.exe" and "-Action" in call
    ]
    assert actions[-2:] == ["DisableBot", "StopBot"]
    installer = manager.windows_installer()
    stop_bot = installer.split(
        "} elseif ($Action -eq 'StopBot') {",
        maxsplit=1,
    )[1].split("} elseif", maxsplit=1)[0]
    assert "Disable-ScheduledTask -TaskName 'copilotD Bot'" in stop_bot
    assert "Stop-CopilotDTasks @('copilotD Bot')" in stop_bot
    assert "Get-CopilotDProcessTreeIds" in installer
    assert "copilotD process tree did not exit before task unregister" in installer

    manager._start_bot_after_legacy_upgrade()
    enable = next(
        index
        for index, call in enumerate(runner.calls)
        if "-Action" in call and call[call.index("-Action") + 1] == "EnableBot"
    )
    start = next(
        index for index, call in enumerate(runner.calls) if call[:2] == ("schtasks.exe", "/Run")
    )
    assert enable < start


def test_windows_fail_closed_attempts_verified_stop_when_disable_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    manager = _manager(settings, FakeRunner(), platform_name="win32")
    actions: list[str] = []

    def action(name: str, *, check: bool) -> None:
        assert check is True
        actions.append(name)
        if name == "DisableBot":
            raise ServiceError("disable failed")

    monkeypatch.setattr(manager, "_run_windows_installer_action", action)

    with pytest.raises(ServiceError, match="disable failed"):
        manager._terminate_bot_fail_closed()
    assert actions == ["DisableBot", "StopBot"]


def test_macos_process_probe_only_treats_clean_esrch_as_absent(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = ServiceManager(
        settings,
        platform="darwin",
        command_runner=runner,
    )

    runner.run = lambda command, check=False: CommandResult(  # type: ignore[method-assign]
        tuple(command),
        1,
        "",
        "",
    )
    assert not manager.process_identity_alive(
        pid=12345,
        process_started_at=None,
    )

    runner.run = lambda command, check=False: CommandResult(  # type: ignore[method-assign]
        tuple(command),
        1,
        "",
        "operation not permitted",
    )
    with pytest.raises(ServiceError, match="could not query macOS"):
        manager.process_identity_alive(
            pid=12345,
            process_started_at=None,
        )


def test_windows_legacy_migration_guard_holds_lock_and_records_handoff(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)

    async def initialize() -> None:
        async with Database(settings.database_path):
            pass

    asyncio.run(initialize())
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")

    guard = manager.quiesce_windows_legacy_layout((settings.database_path,))
    competing = sqlite3.connect(settings.database_path, timeout=0)
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            competing.execute("BEGIN IMMEDIATE")
    finally:
        competing.close()
    staged = tmp_path / "staged" / "copilotd.sqlite3"
    guard.stage_database(settings.database_path, staged)
    guard.finalize_database(staged)
    guard.release()

    connection = sqlite3.connect(staged)
    try:
        versions = {
            int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations")
        }
        handoffs = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM service_restart_intents
                WHERE kind = 'legacy_worker_replacement'
                """
            ).fetchone()[0]
        )
    finally:
        connection.close()
    assert max(versions) == 12
    assert handoffs == 1
    script = next(
        call[-1]
        for call in runner.calls
        if call[:4]
        == (
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
        )
    )
    assert script.index("Disable-ScheduledTask") < script.index("taskkill.exe")
    assert "HashSet[int]" in script
    assert "Add-CopilotDProcessTree $processes" in script
    assert "$remainingTracked.Count -eq 0" in script


def test_windows_partial_install_reports_missing_task_without_crashing(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner, platform_name="win32")
    settings.ensure_directories()
    runtime_dir = settings.data_dir / "runtime"
    (runtime_dir / "install-service.ps1").write_text(
        manager.windows_installer(),
        encoding="utf-8",
    )
    for task, xml in manager.windows_task_xml().items():
        path = manager._windows_task_paths()[task]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(xml, encoding="utf-8")
    runner.missing_tasks.add("copilotD Bot")

    status = manager.status()

    assert status.ready is False
    bot = next(unit for unit in status.units if unit.name == "bot")
    assert bot.effective_state == "missing"
    assert bot.process_started_at is None


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
        self.runner = runner
        self._provider_settings = settings
        self.use_heartbeat_process_start = True
        super().__init__(
            settings,
            entrypoint=Path("/tmp/copilotd"),
            platform="darwin",
            launch_agents_dir=settings.resolved_home / "LaunchAgents",
            command_runner=runner,
            restart_coordinator=AutoAckRestartCoordinator(settings.database_path),
            topology=topology,  # type: ignore[arg-type]
            resume_provider=lambda: resume_at,
            process_start_provider=self._test_process_started_at,
            notifier=notifier,
        )
        runner.manager = self
        self.restarts = 0

    def _restart_bot(self) -> None:
        self.restarts += 1
        connection = sqlite3.connect(self.settings.database_path)
        try:
            connection.execute(
                """
                UPDATE service_admission_fences
                SET state = 'released', released_at = ?
                WHERE state = 'committed'
                """,
                (time.time(),),
            )
            connection.commit()
        finally:
            connection.close()

    def _heartbeat_generation_matches_verified(
        self,
        snapshot: HeartbeatSnapshot,
        managed_bot,
        *,
        age: float,
    ) -> bool:
        del snapshot, managed_bot, age
        return True

    def _test_process_started_at(self, _pid: int) -> float:
        if self.use_heartbeat_process_start and self._provider_settings.heartbeat_path.exists():
            try:
                snapshot = read_heartbeat(self._provider_settings.heartbeat_path)
            except (ValueError, json.JSONDecodeError):
                pass
            else:
                assert snapshot.process_started_at is not None
                return datetime.fromisoformat(
                    snapshot.process_started_at.replace("Z", "+00:00")
                ).timestamp()
        return self.runner.process_started_at


class RecordingNotifier:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str]] = []

    def notify(self, title: str, message: str) -> None:
        self.notifications.append((title, message))


class FailingWatchdogRestartManager(RecordingServiceManager):
    def _restart_bot(self) -> None:
        raise ServiceError("simulated manager restart failure")


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
async def test_watchdog_restart_failure_after_commit_terminates_fail_closed(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    settings.ensure_directories()
    now = time.time()
    _write_heartbeat(settings, _heartbeat(now - 180))
    manager = FailingWatchdogRestartManager(settings)

    assert manager.watchdog(now=now) == "restart-failed-closed"
    assert any(call[:3] == ("launchctl", "kill", "SIGTERM") for call in manager.runner.calls)
    connection = sqlite3.connect(settings.database_path)
    try:
        state = connection.execute(
            """
            SELECT state FROM service_admission_fences
            ORDER BY requested_at DESC LIMIT 1
            """
        ).fetchone()[0]
    finally:
        connection.close()
    assert state == "committed"


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
              ('ambiguous-1', 'session-1', 'app_message',
               'submitted_unknown', 0),
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
               '{}', 1, 2, 'local_queued', 0, 0),
              ('ambiguous-1', 'thread-1', 'ambiguous', 'interactive',
               '{}', 1, 3, 'local_queued', 0, 0)
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
    assert any(
        call[:3] == ("launchctl", "kickstart", "-k")
        and call[-1].endswith("com.github.copilotd.runtime")
        for call in manager.runner.calls
    )
    async with Database(settings.database_path) as database:
        submissions = await database.fetchall(
            """
            SELECT submission_id, state FROM submissions
            ORDER BY submission_id
            """
        )
        queue_rows = await database.fetchall("SELECT id, state FROM message_queue ORDER BY id")
    assert [tuple(row) for row in submissions] == [
        ("ambiguous-1", "outcome_unknown"),
        ("cancelled-1", "cancelled"),
        ("complete-1", "semantic_complete"),
        ("queued-1", "local_queued"),
        ("submission-1", "outcome_unknown"),
    ]
    assert [tuple(row) for row in queue_rows] == [
        ("ambiguous-1", "submitted_unknown"),
        ("queued-1", "local_queued"),
        ("submission-1", "submitted_unknown"),
    ]
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            UPDATE message_queue SET state = 'cancelled'
            WHERE id = 'queued-1'
            """
        )
    snapshot = SqliteRestartCoordinator(settings.database_path).snapshot(now=time.time())
    assert snapshot.local_pending == 0


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
async def test_committed_restart_hands_off_owner_and_recovers_attachment(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 7, ?, ?)
            """,
            (now, now),
        )
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-1', 'old-owner', 7, ?, ?, ?)
            """,
            (now, now, now + 60),
        )
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                binding_intent, attachment_state,
                runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-closing', 'home', '/tmp', 'session-closing',
                      'closed', 'disconnecting', 1, 9, ?, ?)
            """,
            (now, now),
        )
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-closing', 'closing-owner', 9, ?, ?, ?)
            """,
            (now, now, now + 60),
        )
    coordinator = SqliteRestartCoordinator(settings.database_path)
    fence = coordinator.request_quiesce(
        expected_pid=4321,
        expected_generation="old-generation",
        expected_process_started_at=now - 10,
        now=now,
    )
    coordinator.acknowledge_quiesce(fence, now=now)
    coordinator.commit_quiesce(fence, now=now + 1)

    async with Database(settings.database_path) as database:
        binding = await database.fetchone(
            """
            SELECT attachment_state, attachment_reason
            FROM session_bindings WHERE sdk_session_id = 'session-1'
            """
        )
        old_owner = await database.fetchone(
            """
            SELECT expires_at FROM session_owner_leases
            WHERE sdk_session_id = 'session-1'
            """
        )
        closing = await database.fetchone(
            """
            SELECT binding_intent, attachment_state, attachment_reason
            FROM session_bindings
            WHERE sdk_session_id = 'session-closing'
            """
        )
        closing_owner = await database.fetchone(
            """
            SELECT expires_at FROM session_owner_leases
            WHERE sdk_session_id = 'session-closing'
            """
        )
        replacement = await OwnerLeaseStore(database).acquire(
            "session-1",
            "replacement-owner",
            now=now + 2,
        )
    assert tuple(binding) == (
        "recovery_unknown",
        "restart_owner_handoff",
    )
    assert old_owner["expires_at"] == pytest.approx(now + 1)
    assert tuple(closing) == (
        "closed",
        "recovery_unknown",
        "restart_owner_handoff",
    )
    assert closing_owner["expires_at"] == pytest.approx(now + 1)
    assert replacement.fence_token == 8

    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="new-generation",
            replacement_process_started_at=now + 2,
            manager_handoff_token="",
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now + 2,
        )
        == "committed"
    )
    row = coordinator._fence_row(fence.fence_id)
    assert row is not None and row["state"] == "released"


@pytest.mark.asyncio
async def test_replacement_completes_crashed_irreversible_prepare(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 2, ?, ?)
            """,
            (now, now),
        )
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-1', 'old-owner', 2, ?, ?, ?)
            """,
            (now, now, now + 60),
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'submitted', ?)
            """,
            (now,),
        )
    coordinator = SqliteRestartCoordinator(settings.database_path)
    fence = coordinator.request_quiesce(
        expected_pid=4321,
        expected_generation="old-generation",
        expected_process_started_at=now - 10,
        now=now,
    )
    coordinator.acknowledge_quiesce(fence, now=now)
    snapshot = coordinator.snapshot_under_fence(fence, now=now)
    coordinator.prepare_force(fence, snapshot, deadline=now + 10)
    assert coordinator._fence_row(fence.fence_id)["state"] == "prepared"

    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="replacement-generation",
            replacement_process_started_at=now + 1,
            manager_handoff_token="",
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now + 1,
        )
        == "committed"
    )

    connection = coordinator._connect()
    try:
        row = connection.execute(
            """
            SELECT state, owner_handoff_at
            FROM service_admission_fences WHERE fence_id = ?
            """,
            (fence.fence_id,),
        ).fetchone()
        owner = connection.execute(
            """
            SELECT expires_at FROM session_owner_leases
            WHERE sdk_session_id = 'session-1'
            """
        ).fetchone()
    finally:
        connection.close()
    assert row["state"] == "released"
    assert row["owner_handoff_at"] is not None
    assert owner["expires_at"] == pytest.approx(now + 1)


@pytest.mark.asyncio
async def test_replacement_adopts_reversible_crash_owner_without_ttl_wait(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 4, ?, ?)
            """,
            (now, now),
        )
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-1', 'crashed-owner', 4, ?, ?, ?)
            """,
            (now, now, now + 60),
        )
    coordinator = SqliteRestartCoordinator(settings.database_path)
    coordinator.request_quiesce(
        expected_pid=4321,
        expected_generation="crashed-generation",
        expected_process_started_at=now - 10,
        now=now,
    )

    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="replacement-generation",
            replacement_process_started_at=now + 1,
            manager_handoff_token="",
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now + 1,
        )
        == "requested"
    )

    async with Database(settings.database_path) as database:
        binding = await database.fetchone(
            """
            SELECT attachment_state, attachment_reason
            FROM session_bindings WHERE sdk_session_id = 'session-1'
            """
        )
        replacement = await OwnerLeaseStore(database).acquire(
            "session-1",
            "replacement-owner",
            now=now + 2,
        )
    assert tuple(binding) == (
        "recovery_unknown",
        "replacement_adopted_reversible_crash",
    )
    assert replacement.fence_token == 5


@pytest.mark.asyncio
async def test_replacement_adoption_requires_manager_token_and_old_process_death(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    coordinator = SqliteRestartCoordinator(settings.database_path)
    now = time.time()
    coordinator.request_quiesce(
        expected_pid=4321,
        expected_generation="old-generation",
        expected_process_started_at=now - 10,
        handoff_token="manager-token",
        now=now,
    )
    common = {
        "replacement_pid": 5000,
        "replacement_generation": "new-generation",
        "replacement_process_started_at": now + 1,
        "now": now + 1,
    }

    with pytest.raises(ServiceError, match="not the effective"):
        coordinator.recover_for_replacement(
            manager_handoff_token="manager-token",
            replacement_is_managed=False,
            old_process_identity_alive=False,
            **common,
        )
    with pytest.raises(ServiceError, match="old process is alive"):
        coordinator.recover_for_replacement(
            manager_handoff_token="manager-token",
            replacement_is_managed=True,
            old_process_identity_alive=True,
            **common,
        )
    with pytest.raises(ServiceError, match="handoff token is invalid"):
        coordinator.recover_for_replacement(
            manager_handoff_token="wrong-token",
            replacement_is_managed=True,
            old_process_identity_alive=False,
            **common,
        )


@pytest.mark.asyncio
async def test_replacement_recovers_legacy_committed_fence_with_null_epochs(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO service_admission_fences(
                fence_id, expected_pid, expected_generation,
                expected_process_started_at, state, requested_at,
                acknowledged_at, committed_at, owner_handoff_at,
                ingress_depth, producer_count,
                acknowledged_producer_count,
                acknowledged_journal_id, violation_count, detail
            ) VALUES (
                'legacy-fence', 4321, 'legacy-generation', NULL,
                'committed', ?, ?, ?, ?, 0, 0, NULL, NULL, 0, '{}'
            )
            """,
            (now - 10, now - 9, now - 8, now - 8),
        )
    coordinator = SqliteRestartCoordinator(settings.database_path)

    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="replacement-generation",
            replacement_process_started_at=now,
            manager_handoff_token=None,
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now,
        )
        == "committed"
    )

    row = coordinator._fence_row("legacy-fence")
    assert row is not None and row["state"] == "released"
    connection = coordinator._connect()
    try:
        incident = connection.execute(
            """
            SELECT kind FROM service_restart_intents
            WHERE restart_id = 'legacy-fence'
            """
        ).fetchone()
    finally:
        connection.close()
    assert incident["kind"] == "post_commit_producer_violation"


@pytest.mark.asyncio
async def test_replacement_recovers_legacy_prepared_fence_with_null_identity(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO service_admission_fences(
                fence_id, expected_pid, expected_generation,
                expected_process_started_at, state, requested_at,
                acknowledged_at, force_prepared_at, ingress_depth,
                producer_count, acknowledged_producer_count,
                acknowledged_journal_id, violation_count,
                protocol_version, handoff_token_hash, detail
            ) VALUES (
                'legacy-prepared', 4321, 'legacy-generation', NULL,
                'prepared', ?, ?, ?, 0, 0, NULL, NULL, 0, 1, NULL, '{}'
            )
            """,
            (now - 10, now - 9, now - 8),
        )
    coordinator = SqliteRestartCoordinator(settings.database_path)

    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="replacement-generation",
            replacement_process_started_at=now,
            manager_handoff_token=None,
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now,
        )
        == "committed"
    )

    row = coordinator._fence_row("legacy-prepared")
    assert row is not None
    assert row["state"] == "released"
    assert row["owner_handoff_at"] is not None


@pytest.mark.asyncio
async def test_schema12_restores_unknown_legacy_process_identity_to_null(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO service_admission_fences(
                fence_id, expected_pid, expected_generation,
                expected_process_started_at, state, requested_at,
                protocol_version, handoff_token_hash, detail
            ) VALUES (
                'legacy-unknown-start', 4321, 'legacy-generation',
                0, 'released', ?, 1, '', '{}'
            )
            """,
            (now,),
        )
        await database.execute("DELETE FROM schema_migrations WHERE version = 12")

    async with Database(settings.database_path) as database:
        row = await database.fetchone(
            """
            SELECT expected_process_started_at
            FROM service_admission_fences
            WHERE fence_id = 'legacy-unknown-start'
            """
        )
    assert row is not None and row[0] is None


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
    replacement.use_heartbeat_process_start = False
    replacement.runner.mac_pid = 9001
    _write_heartbeat(settings, _heartbeat(now - 180, pid=4321))

    assert replacement.watchdog(now=now) == "replacement-starting"
    assert replacement.restarts == 0

    replacement.runner.mac_pid = 4321
    replacement.runner.process_started_at = now - 10
    _write_heartbeat(
        settings,
        _heartbeat(
            now - 180,
            pid=4321,
            process_started_at=datetime.fromtimestamp(
                now - 190,
                UTC,
            )
            .isoformat()
            .replace("+00:00", "Z"),
        ),
    )
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
async def test_watchdog_explicitly_adopts_ready_replacement_generation(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    settings.ensure_directories()
    now = time.time()
    runner = FakeRunner()
    runner.process_started_at = now - 10
    manager = _manager(settings, runner)
    settings.service_state_path.parent.mkdir(parents=True, exist_ok=True)
    settings.service_state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "last_verified_pid": 4321,
                "last_verified_generation": "old-generation",
                "last_verified_process_started_at": (
                    datetime.fromtimestamp(now - 100, UTC).isoformat().replace("+00:00", "Z")
                ),
                "last_verified_at": now - 90,
            }
        ),
        encoding="utf-8",
    )
    _write_heartbeat(
        settings,
        _heartbeat(
            now,
            process_generation="replacement-generation",
            process_started_at=(
                datetime.fromtimestamp(now - 10, UTC).isoformat().replace("+00:00", "Z")
            ),
        ),
    )

    assert manager.watchdog(now=now) == "healthy"
    state = json.loads(settings.service_state_path.read_text(encoding="utf-8"))
    assert state["last_verified_generation"] == "replacement-generation"
    assert state["replacement_adopted_at"] == pytest.approx(now)


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
        fence: QuiesceFence,
        snapshot: RestartSafetySnapshot,
        *,
        deadline: float,
    ) -> ForceRestartOutcome:
        del fence, snapshot, deadline
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            time.sleep(1.5)
            connection.rollback()
        finally:
            connection.close()
        return ForceRestartOutcome(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            False,
            "slow locked writer",
        )


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
        process_start_provider=lambda _: runner.process_started_at,
        uid=501,
        sleep=lambda _: None,
    )
    runner.manager = manager
    manager.install()
    _write_heartbeat(settings, _heartbeat(time.time()))

    started = time.monotonic()
    with pytest.raises(ForcePreparationUncertain):
        manager.restart(force=True)
    elapsed = time.monotonic() - started

    assert elapsed < 1.2
    assert any(call[:3] == ("launchctl", "kill", "SIGTERM") for call in runner.calls)


def test_restart_missing_or_malformed_heartbeat_fails_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()

    with pytest.raises(RestartBlocked, match="heartbeat_unavailable"):
        manager.restart(force=True)


@pytest.mark.asyncio
async def test_schema11_manager_replaces_legacy_protocol_worker_before_fence(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()
    _write_heartbeat(
        settings,
        _heartbeat(
            time.time(),
            service_control_protocol=1,
        ),
    )

    receipt = manager.restart()

    assert receipt.admission_fence_id == "legacy-worker-protocol-upgrade"
    assert any(
        call[:2] == ("launchctl", "bootout") and call[-1].endswith("com.github.copilotd.bot")
        for call in runner.calls
    )
    assert any(
        call[:2] == ("launchctl", "bootstrap")
        and call[-1].endswith("com.github.copilotd.bot.plist")
        for call in runner.calls
    )
    secrets = json.loads(settings.service_secrets_path.read_text(encoding="utf-8"))
    assert secrets["service_handoff_token"] == manager._handoff_token
    connection = sqlite3.connect(settings.database_path)
    try:
        count = connection.execute("SELECT COUNT(*) FROM service_admission_fences").fetchone()[0]
    finally:
        connection.close()
    assert count == 0


@pytest.mark.asyncio
async def test_legacy_protocol_replacement_snapshots_after_death_and_recovers_race(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager._coordinator = LegacyAdmissionRaceCoordinator(settings.database_path)
    manager.install()
    _write_heartbeat(
        settings,
        _heartbeat(time.time(), service_control_protocol=1),
    )

    receipt = manager.restart(force=False)

    assert receipt.safety_snapshot.pending_operations == 1
    assert receipt.force_outcome is not None
    assert "conservative ambiguity recovery" in receipt.force_outcome.detail
    assert manager._coordinator.snapshot_calls == 2
    connection = sqlite3.connect(settings.database_path)
    try:
        state = connection.execute(
            """
            SELECT state FROM session_operations
            WHERE operation_id = 'legacy-race-operation'
            """
        ).fetchone()[0]
        incident = connection.execute(
            """
            SELECT kind FROM service_restart_intents
            WHERE kind = 'legacy_worker_replacement'
            """
        ).fetchone()[0]
    finally:
        connection.close()
    assert state == "unknown"
    assert incident == "legacy_worker_replacement"


@pytest.mark.asyncio
async def test_legacy_protocol_replacement_requires_fresh_unprotected_heartbeat(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = _manager(settings, runner)
    manager.install()
    now = time.time()
    _write_heartbeat(
        settings,
        _heartbeat(
            now,
            service_control_protocol=1,
            ingress_queue_depth=1,
        ),
    )

    with pytest.raises(
        RestartBlocked,
        match="legacy_worker_heartbeat_not_detach_safe",
    ):
        manager.restart()
    _write_heartbeat(
        settings,
        _heartbeat(
            now - 180,
            service_control_protocol=1,
            process_started_at=(
                datetime.fromtimestamp(
                    runner.process_started_at,
                    UTC,
                )
                .isoformat()
                .replace("+00:00", "Z")
            ),
        ),
    )
    service_state = json.loads(settings.service_state_path.read_text(encoding="utf-8"))
    service_state["installed_at"] = now - 300
    settings.service_state_path.write_text(
        json.dumps(service_state),
        encoding="utf-8",
    )
    assert manager._watchdog_restart(now) == "legacy-worker-manual-upgrade-required"

    receipt = manager.restart(force=True)
    assert receipt.admission_fence_id == "legacy-worker-protocol-upgrade"


@pytest.mark.asyncio
async def test_legacy_sidecar_runtime_loss_restarts_runtime_before_bot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    async with Database(settings.database_path):
        pass
    runner = FakeRunner()
    manager = _manager(settings, runner, topology="sidecar")
    _write_heartbeat(
        settings,
        _heartbeat(
            time.time(),
            service_control_protocol=1,
            runtime_state="down",
        ),
    )

    receipt = manager.restart(force=True, restart_runtime=True)

    assert receipt.admission_fence_id == "legacy-worker-protocol-upgrade"
    runtime_restart = next(
        index
        for index, call in enumerate(runner.calls)
        if call[:3] == ("launchctl", "kickstart", "-k")
        and call[-1].endswith("com.github.copilotd.runtime")
    )
    bot_start = next(
        index
        for index, call in enumerate(runner.calls)
        if call[:2] == ("launchctl", "bootstrap")
        and call[-1].endswith("com.github.copilotd.bot.plist")
    )
    assert runtime_restart < bot_start


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
        process_start_provider=lambda _: runner.process_started_at,
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


@pytest.mark.asyncio
async def test_force_prepare_failure_stays_irreversible_and_terminates_bot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    now = time.time()
    async with Database(settings.database_path) as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                attachment_state, runtime_generation, owner_fence_token,
                created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1',
                      'attached', 1, 5, ?, ?)
            """,
            (now, now),
        )
        await database.execute(
            """
            INSERT INTO session_owner_leases(
                sdk_session_id, owner_id, fence_token,
                acquired_at, renewed_at, expires_at
            ) VALUES ('session-1', 'old-owner', 5, ?, ?, ?)
            """,
            (now, now, now + 60),
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'submitted', ?)
            """,
            (now,),
        )
    runner = FakeRunner()
    coordinator = LateProducerAfterPrepareCoordinator(settings.database_path)
    manager = ServiceManager(
        settings,
        entrypoint=settings.resolved_home / "copilotd",
        platform="darwin",
        launch_agents_dir=settings.resolved_home / "LaunchAgents",
        command_runner=runner,
        restart_coordinator=coordinator,
        resume_provider=lambda: None,
        process_start_provider=lambda _: runner.process_started_at,
        uid=501,
        sleep=lambda _: None,
    )
    runner.manager = manager
    manager.install()
    _write_heartbeat(settings, _heartbeat(now, active_submissions=1))

    with pytest.raises(
        RestartBlocked,
        match="admission_fence_producer_changed",
    ):
        manager.restart(force=True)

    connection = coordinator._connect()
    try:
        fence = connection.execute(
            """
            SELECT state, owner_handoff_at, violation_count
            FROM service_admission_fences
            ORDER BY requested_at DESC LIMIT 1
            """
        ).fetchone()
        owner = connection.execute(
            """
            SELECT expires_at FROM session_owner_leases
            WHERE sdk_session_id = 'session-1'
            """
        ).fetchone()
    finally:
        connection.close()
    assert fence["state"] == "committed"
    assert fence["owner_handoff_at"] is not None
    assert fence["violation_count"] == 1
    assert owner["expires_at"] <= time.time()
    assert any(call[:3] == ("launchctl", "kill", "SIGTERM") for call in runner.calls)
    assert (
        coordinator.recover_for_replacement(
            replacement_pid=5000,
            replacement_generation="replacement-generation",
            replacement_process_started_at=now + 2,
            manager_handoff_token=manager._handoff_token,
            replacement_is_managed=True,
            old_process_identity_alive=False,
            now=now + 2,
        )
        == "committed"
    )
    connection = coordinator._connect()
    try:
        incident = connection.execute(
            """
            SELECT kind, outcome FROM service_restart_intents
            WHERE kind = 'post_commit_producer_violation'
            """
        ).fetchone()
    finally:
        connection.close()
    assert tuple(incident) == (
        "post_commit_producer_violation",
        "unknown",
    )
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
