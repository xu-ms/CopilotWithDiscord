from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from copilotd.config import Settings
from copilotd.ops.heartbeat import heartbeat_age_seconds, read_heartbeat

Topology = Literal["bundled-runtime"]

_MAC_BOT_LABEL = "com.github.copilotd.bot"
_MAC_WATCHDOG_LABEL = "com.github.copilotd.watchdog"
_WINDOWS_BOT_TASK = "copilotD Bot"
_WINDOWS_WATCHDOG_TASK = "copilotD Watchdog"
_TASK_NAMESPACE = "http://schemas.microsoft.com/windows/2004/02/mit/task"


@dataclass(frozen=True, slots=True)
class ServiceStatus:
    platform: str
    topology: Topology
    installed: bool
    bot_loaded: bool
    watchdog_loaded: bool
    heartbeat_age_seconds: float | None
    gateway_state: str | None
    runtime_state: str | None
    protected_work: bool | None


class ServiceManager:
    """Installs the conservative bundled-runtime bot and watchdog topology."""

    def __init__(
        self,
        settings: Settings,
        *,
        entrypoint: Path | None = None,
        platform: str | None = None,
        launch_agents_dir: Path | None = None,
    ) -> None:
        self.settings = settings
        self.platform = sys.platform if platform is None else platform
        self.entrypoint = (
            Path(sys.argv[0]).expanduser().resolve()
            if entrypoint is None
            else entrypoint.expanduser().resolve()
        )
        self.launch_agents_dir = (
            Path.home() / "Library" / "LaunchAgents"
            if launch_agents_dir is None
            else launch_agents_dir
        )
        self.topology: Topology = "bundled-runtime"

    def macos_plists(self) -> dict[str, bytes]:
        environment = self._service_environment()
        base = {
            "WorkingDirectory": str(Path.cwd().resolve()),
            "EnvironmentVariables": environment,
            "ThrottleInterval": 30,
            "LowPriorityBackgroundIO": False,
        }
        bot = {
            **base,
            "Label": _MAC_BOT_LABEL,
            "ProgramArguments": [str(self.entrypoint), "run", "--foreground"],
            "RunAtLoad": True,
            "KeepAlive": True,
            "StandardOutPath": str(self.settings.log_dir / "copilotd.log"),
            "StandardErrorPath": str(self.settings.log_dir / "boot.log"),
        }
        watchdog = {
            **base,
            "Label": _MAC_WATCHDOG_LABEL,
            "ProgramArguments": [str(self.entrypoint), "service", "watchdog"],
            "RunAtLoad": True,
            "StartInterval": 300,
            "StandardOutPath": str(self.settings.log_dir / "watchdog.log"),
            "StandardErrorPath": str(self.settings.log_dir / "watchdog.log"),
        }
        return {
            f"{_MAC_BOT_LABEL}.plist": plistlib.dumps(bot, sort_keys=True),
            f"{_MAC_WATCHDOG_LABEL}.plist": plistlib.dumps(
                watchdog,
                sort_keys=True,
            ),
        }

    def windows_task_xml(self) -> dict[str, str]:
        runner = self.settings.data_dir / "runtime" / "copilotd-service.ps1"
        return {
            _WINDOWS_BOT_TASK: _windows_task_xml(
                command="powershell.exe",
                arguments=f'-NoProfile -ExecutionPolicy Bypass -File "{runner}" run',
                watchdog=False,
            ),
            _WINDOWS_WATCHDOG_TASK: _windows_task_xml(
                command="powershell.exe",
                arguments=f'-NoProfile -ExecutionPolicy Bypass -File "{runner}" watchdog',
                watchdog=True,
            ),
        }

    def windows_runner(self) -> str:
        secret_path = str(self.settings.service_secrets_path)
        lines = [
            "$ErrorActionPreference = 'Stop'",
            *[
                f"$env:{key} = '{_powershell_quote(value)}'"
                for key, value in sorted(self._service_environment().items())
            ],
            f"$secretPath = '{_powershell_quote(secret_path)}'",
            "if (-not (Test-Path -LiteralPath $secretPath -PathType Leaf)) {",
            '  throw "copilotD service secret file is missing: $secretPath"',
            "}",
            "$action = $args[0]",
            "if ($action -eq 'run') {",
            f"  & '{_powershell_quote(str(self.entrypoint))}' run --foreground",
            "} elseif ($action -eq 'watchdog') {",
            f"  & '{_powershell_quote(str(self.entrypoint))}' service watchdog",
            '} else { throw "Unknown copilotD service action: $action" }',
            "exit $LASTEXITCODE",
            "",
        ]
        return "\n".join(lines)

    def install(self) -> None:
        self._require_install_credentials()
        self.settings.ensure_directories()
        self.settings.write_service_secrets()
        if self.platform == "darwin":
            self._install_macos()
        elif self.platform == "win32":
            self._install_windows()
        else:
            raise RuntimeError(f"service installation is unsupported on {self.platform}")

    def uninstall(self) -> None:
        if self.platform == "darwin":
            domain = f"gui/{os.getuid()}"
            for label in (_MAC_BOT_LABEL, _MAC_WATCHDOG_LABEL):
                _run_optional(["launchctl", "bootout", f"{domain}/{label}"])
                (self.launch_agents_dir / f"{label}.plist").unlink(missing_ok=True)
        elif self.platform == "win32":
            for task in (_WINDOWS_BOT_TASK, _WINDOWS_WATCHDOG_TASK):
                _run_optional(["schtasks.exe", "/Delete", "/TN", task, "/F"])
        else:
            raise RuntimeError(f"service uninstallation is unsupported on {self.platform}")

    def status(self) -> ServiceStatus:
        heartbeat_age = None
        gateway = None
        runtime = None
        protected = None
        try:
            snapshot = read_heartbeat(self.settings.heartbeat_path)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        else:
            heartbeat_age = heartbeat_age_seconds(snapshot)
            gateway = snapshot.gateway_state
            runtime = snapshot.runtime_state
            protected = snapshot.protected_work

        if self.platform == "darwin":
            domain = f"gui/{os.getuid()}"
            bot_loaded = _command_succeeds(["launchctl", "print", f"{domain}/{_MAC_BOT_LABEL}"])
            watchdog_loaded = _command_succeeds(
                ["launchctl", "print", f"{domain}/{_MAC_WATCHDOG_LABEL}"]
            )
            installed = all(
                (self.launch_agents_dir / f"{label}.plist").exists()
                for label in (_MAC_BOT_LABEL, _MAC_WATCHDOG_LABEL)
            )
        elif self.platform == "win32":
            bot_loaded = _command_succeeds(["schtasks.exe", "/Query", "/TN", _WINDOWS_BOT_TASK])
            watchdog_loaded = _command_succeeds(
                ["schtasks.exe", "/Query", "/TN", _WINDOWS_WATCHDOG_TASK]
            )
            installed = bot_loaded and watchdog_loaded
        else:
            installed = bot_loaded = watchdog_loaded = False

        return ServiceStatus(
            platform=self.platform,
            topology=self.topology,
            installed=installed,
            bot_loaded=bot_loaded,
            watchdog_loaded=watchdog_loaded,
            heartbeat_age_seconds=heartbeat_age,
            gateway_state=gateway,
            runtime_state=runtime,
            protected_work=protected,
        )

    def restart(self, *, force: bool = False) -> None:
        snapshot = None
        try:
            snapshot = read_heartbeat(self.settings.heartbeat_path)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        if snapshot is not None and snapshot.protected_work and not force:
            raise RuntimeError("protected work is active; use --force to restart")
        self._restart_bot()

    def watchdog(self, *, now: float | None = None) -> str:
        current = time.time() if now is None else now
        try:
            snapshot = read_heartbeat(self.settings.heartbeat_path)
            age = heartbeat_age_seconds(snapshot, now=current)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            snapshot = None
            age = float("inf")

        if snapshot is not None and snapshot.runtime_state == "down":
            if snapshot.protected_work:
                self._write_alert(
                    "runtime reported down with protected work; waiting for process exit "
                    "instead of guessing recovery"
                )
                return "runtime-down-protected"
            if self._restart_storm(current):
                self._write_alert("runtime-down restart suppressed after 3 attempts in 5 minutes")
                return "restart-storm"
            self._record_restart(current)
            self._restart_bot()
            return "restarted-runtime-down"
        if age <= 120:
            return "healthy"
        if snapshot is not None and snapshot.protected_work:
            self._write_alert(
                "heartbeat stale with protected work; bundled runtime cannot be replayed safely"
            )
            return "protected-no-restart"
        if self._restart_storm(current):
            self._write_alert("watchdog restart suppressed after 3 attempts in 5 minutes")
            return "restart-storm"
        self._record_restart(current)
        self._restart_bot()
        return "restarted"

    def _install_macos(self) -> None:
        self.launch_agents_dir.mkdir(parents=True, exist_ok=True)
        definitions = self.macos_plists()
        for filename, content in definitions.items():
            _atomic_write_bytes(self.launch_agents_dir / filename, content)
        domain = f"gui/{os.getuid()}"
        for label in (_MAC_BOT_LABEL, _MAC_WATCHDOG_LABEL):
            _run_optional(["launchctl", "bootout", f"{domain}/{label}"])
        for label in (_MAC_BOT_LABEL, _MAC_WATCHDOG_LABEL):
            path = self.launch_agents_dir / f"{label}.plist"
            _run_required(["launchctl", "bootstrap", domain, str(path)])
            _run_required(["launchctl", "enable", f"{domain}/{label}"])
            _run_required(["launchctl", "kickstart", f"{domain}/{label}"])

    def _install_windows(self) -> None:
        _run_required(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                (
                    "$path = $args[0]; $sid = [System.Security.Principal.WindowsIdentity]"
                    "::GetCurrent().User.Value; & icacls.exe $path '/inheritance:r' "
                    "'/grant:r' \"*$sid`:(R,W)\" | Out-Null; "
                    "if ($LASTEXITCODE -ne 0) { throw 'Could not restrict service secret ACL' }"
                ),
                str(self.settings.service_secrets_path),
            ]
        )
        runner = self.settings.data_dir / "runtime" / "copilotd-service.ps1"
        _atomic_write_text(runner, self.windows_runner())
        task_directory = self.settings.data_dir / "runtime" / "tasks"
        task_directory.mkdir(parents=True, exist_ok=True)
        for task, xml in self.windows_task_xml().items():
            path = task_directory / f"{task.replace(' ', '-')}.xml"
            _atomic_write_text(path, xml)
            _run_required(["schtasks.exe", "/Create", "/TN", task, "/XML", str(path), "/F"])
            _run_required(["schtasks.exe", "/Run", "/TN", task])

    def _restart_bot(self) -> None:
        if self.platform == "darwin":
            _run_required(
                [
                    "launchctl",
                    "kickstart",
                    "-k",
                    f"gui/{os.getuid()}/{_MAC_BOT_LABEL}",
                ]
            )
        elif self.platform == "win32":
            _run_required(["schtasks.exe", "/Run", "/TN", _WINDOWS_BOT_TASK])
        else:
            raise RuntimeError(f"service restart is unsupported on {self.platform}")

    def _service_environment(self) -> dict[str, str]:
        environment = {
            "HOME": str(self.settings.resolved_home),
            "PATH": os.environ.get("PATH", ""),
            "COPILOTD_DATA_DIR": str(self.settings.data_dir),
            "COPILOTD_CACHE_DIR": str(self.settings.cache_dir),
            "COPILOTD_LOG_DIR": str(self.settings.log_dir),
            "COPILOTD_RESOLVED_HOME": str(self.settings.resolved_home),
            "COPILOTD_LOG_LEVEL": self.settings.log_level,
            "COPILOTD_SDK_LOG_LEVEL": self.settings.sdk_log_level,
            "COPILOTD_SERVICE_SECRETS": str(self.settings.service_secrets_path),
        }
        if self.settings.discord_guild_id is not None:
            environment["COPILOTD_DISCORD_GUILD_ID"] = str(self.settings.discord_guild_id)
        if self.settings.runtime_uri is not None:
            environment["COPILOTD_RUNTIME_URI"] = self.settings.runtime_uri
        if self.settings.runtime_connection_token is not None:
            environment["COPILOTD_RUNTIME_CONNECTION_TOKEN"] = (
                self.settings.runtime_connection_token.get_secret_value()
            )
        environment["COPILOTD_MENTION_REQUIRED"] = str(self.settings.mention_required).lower()
        return environment

    def _require_install_credentials(self) -> None:
        missing: list[str] = []
        if self.settings.discord_token is None:
            missing.append("COPILOTD_DISCORD_TOKEN")
        if self.settings.github_token is None:
            missing.append("COPILOTD_GITHUB_TOKEN")
        if missing:
            raise RuntimeError(
                "service installation requires protected credentials: " + ", ".join(missing)
            )

    def _restart_storm(self, now: float) -> bool:
        history = self._restart_history()
        recent = [value for value in history if now - value <= 300]
        return len(recent) >= 3

    def _record_restart(self, now: float) -> None:
        history = [value for value in self._restart_history() if now - value <= 300]
        history.append(now)
        _atomic_write_text(
            self.settings.cache_dir / "watchdog-state.json",
            json.dumps({"restarts": history}, separators=(",", ":")) + "\n",
        )

    def _restart_history(self) -> list[float]:
        try:
            payload = json.loads(
                (self.settings.cache_dir / "watchdog-state.json").read_text(encoding="utf-8")
            )
            return [float(value) for value in payload.get("restarts", [])]
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return []

    def _write_alert(self, message: str) -> None:
        timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        self.settings.log_dir.mkdir(parents=True, exist_ok=True)
        with (self.settings.log_dir / "alerts.log").open("a", encoding="utf-8") as stream:
            stream.write(f"{timestamp} {message}\n")


def _windows_task_xml(*, command: str, arguments: str, watchdog: bool) -> str:
    ET.register_namespace("", _TASK_NAMESPACE)
    task = ET.Element(f"{{{_TASK_NAMESPACE}}}Task", {"version": "1.4"})
    triggers = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}Triggers")
    logon = ET.SubElement(triggers, f"{{{_TASK_NAMESPACE}}}LogonTrigger")
    ET.SubElement(logon, f"{{{_TASK_NAMESPACE}}}Enabled").text = "true"
    if watchdog:
        repetition = ET.SubElement(logon, f"{{{_TASK_NAMESPACE}}}Repetition")
        ET.SubElement(repetition, f"{{{_TASK_NAMESPACE}}}Interval").text = "PT5M"
        ET.SubElement(repetition, f"{{{_TASK_NAMESPACE}}}StopAtDurationEnd").text = "false"

    settings = ET.SubElement(task, f"{{{_TASK_NAMESPACE}}}Settings")
    values = {
        "MultipleInstancesPolicy": "IgnoreNew",
        "DisallowStartIfOnBatteries": "false",
        "StopIfGoingOnBatteries": "false",
        "StartWhenAvailable": "true" if watchdog else "false",
        "ExecutionTimeLimit": "PT0S",
        "Enabled": "true",
    }
    for name, value in values.items():
        ET.SubElement(settings, f"{{{_TASK_NAMESPACE}}}{name}").text = value
    restart = ET.SubElement(settings, f"{{{_TASK_NAMESPACE}}}RestartOnFailure")
    ET.SubElement(restart, f"{{{_TASK_NAMESPACE}}}Interval").text = "PT30S"
    ET.SubElement(restart, f"{{{_TASK_NAMESPACE}}}Count").text = "999"

    actions = ET.SubElement(
        task,
        f"{{{_TASK_NAMESPACE}}}Actions",
        {"Context": "Author"},
    )
    execute = ET.SubElement(actions, f"{{{_TASK_NAMESPACE}}}Exec")
    ET.SubElement(execute, f"{{{_TASK_NAMESPACE}}}Command").text = command
    ET.SubElement(execute, f"{{{_TASK_NAMESPACE}}}Arguments").text = arguments
    return ET.tostring(task, encoding="unicode", xml_declaration=True)


def _run_required(command: list[str]) -> None:
    subprocess.run(command, check=True, capture_output=True, text=True)


def _run_optional(command: list[str]) -> None:
    subprocess.run(command, check=False, capture_output=True, text=True)


def _command_succeeds(command: list[str]) -> bool:
    return subprocess.run(command, check=False, capture_output=True).returncode == 0


def _atomic_write_text(path: Path, content: str) -> None:
    _atomic_write_bytes(path, content.encode("utf-8"))


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _powershell_quote(value: str) -> str:
    return value.replace("'", "''")


def status_dict(status: ServiceStatus) -> dict[str, Any]:
    return asdict(status)
