from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from datetime import datetime

ResumeTimestampProvider = Callable[[], float | None]

_MAC_WAKE_TIMESTAMP = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4})\s+"
    r"(?:DarkWake|Wake)\s+.*\bfrom\b"
)


def macos_last_resume_timestamp() -> float | None:
    result = subprocess.run(
        ["pmset", "-g", "log"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    latest: float | None = None
    for line in result.stdout.splitlines():
        match = _MAC_WAKE_TIMESTAMP.search(line)
        if match is None:
            continue
        timestamp = datetime.strptime(
            match.group("timestamp"),
            "%Y-%m-%d %H:%M:%S %z",
        ).timestamp()
        latest = timestamp if latest is None else max(latest, timestamp)
    return latest


def windows_last_resume_timestamp() -> float | None:
    script = (
        "$event = Get-WinEvent -FilterHashtable "
        "@{LogName='Microsoft-Windows-Power-Troubleshooter/Operational'; Id=1} "
        "-MaxEvents 1 -ErrorAction SilentlyContinue; "
        "if ($null -eq $event) { $event = Get-WinEvent -FilterHashtable "
        "@{LogName='System'; ProviderName='Microsoft-Windows-Power-Troubleshooter'; Id=1} "
        "-MaxEvents 1 -ErrorAction SilentlyContinue }; "
        "if ($null -ne $event) { $event.TimeCreated.ToUniversalTime().ToString('o') }"
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return datetime.fromisoformat(result.stdout.strip().replace("Z", "+00:00")).timestamp()


def resume_timestamp_provider(platform_name: str) -> ResumeTimestampProvider:
    if platform_name == "darwin":
        return macos_last_resume_timestamp
    if platform_name == "win32":
        return windows_last_resume_timestamp
    return lambda: None
