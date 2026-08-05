from __future__ import annotations

import asyncio
import json
import os
import platform
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from copilotd.config import Settings
from copilotd.ops.contracts import (
    EXPECTED_RUNTIME_VERSION,
    EXPECTED_SDK_VERSION,
    PREFLIGHT_SCHEMA_VERSION,
)
from copilotd.sdk.bridge import CopilotBridge

DiscordProbe = Callable[[str], dict[str, Any] | Awaitable[dict[str, Any]]]
CopilotProbe = Callable[[], dict[str, Any] | Awaitable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class PreflightReport:
    schema_version: int
    checks: tuple[PreflightCheck, ...]
    discord_identity: dict[str, Any] | None
    copilot_runtime: dict[str, Any] | None

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "ok": self.ok,
            "checks": [asdict(check) for check in self.checks],
            "discord_identity": self.discord_identity,
            "copilot_runtime": self.copilot_runtime,
        }

    def require_success(self) -> None:
        failures = [f"{check.name}: {check.detail}" for check in self.checks if not check.ok]
        if failures:
            raise PreflightFailed(failures)


class PreflightFailed(RuntimeError):
    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__("setup preflight failed: " + "; ".join(failures))


class SetupPreflight:
    def __init__(
        self,
        settings: Settings,
        *,
        discord_probe: DiscordProbe | None = None,
        copilot_probe: CopilotProbe | None = None,
    ) -> None:
        self._settings = settings
        self._discord_probe = discord_probe or self._default_discord_probe
        self._copilot_probe = copilot_probe or self._default_copilot_probe

    async def run(self) -> PreflightReport:
        checks: list[PreflightCheck] = []
        discord_identity: dict[str, Any] | None = None
        copilot_runtime: dict[str, Any] | None = None

        token = (
            None
            if self._settings.discord_token is None
            else self._settings.discord_token.get_secret_value()
        )
        checks.append(
            PreflightCheck(
                "discord_token",
                bool(token),
                "present" if token else "COPILOTD_DISCORD_TOKEN is required",
            )
        )

        try:
            self._settings.ensure_directories()
            directory_errors = self._settings.validate_directory_security()
        except OSError as error:
            directory_errors = [str(error)]
        checks.append(
            PreflightCheck(
                "directories",
                not directory_errors,
                "secure and writable" if not directory_errors else "; ".join(directory_errors),
            )
        )

        try:
            ZoneInfo("America/Los_Angeles")
        except ZoneInfoNotFoundError as error:
            checks.append(PreflightCheck("tzdata", False, str(error)))
        else:
            checks.append(PreflightCheck("tzdata", True, "IANA timezone database available"))

        sdk_version = _package_version("github-copilot-sdk")
        checks.append(
            PreflightCheck(
                "sdk_version",
                sdk_version == EXPECTED_SDK_VERSION,
                f"expected {EXPECTED_SDK_VERSION}, found {sdk_version}",
            )
        )
        checks.append(
            PreflightCheck(
                "python",
                tuple(map(int, platform.python_version_tuple()[:2])) >= (3, 11),
                platform.python_version(),
            )
        )

        if token:
            try:
                discord_identity = await _call_probe(self._discord_probe, token)
            except Exception as error:
                checks.append(PreflightCheck("discord_connectivity", False, _safe_error(error)))
            else:
                discord_name = discord_identity.get(
                    "username",
                    discord_identity.get("id"),
                )
                checks.append(
                    PreflightCheck(
                        "discord_connectivity",
                        bool(discord_identity.get("id")),
                        f"authenticated bot {discord_name}",
                    )
                )

        try:
            copilot_runtime = await _call_probe(self._copilot_probe)
        except Exception as error:
            checks.append(PreflightCheck("copilot_runtime", False, _safe_error(error)))
        else:
            runtime_version = str(copilot_runtime.get("runtime_version", "unknown"))
            authenticated = copilot_runtime.get("authenticated") is True
            models = int(copilot_runtime.get("model_count", 0))
            runtime_ok = (
                authenticated and runtime_version == EXPECTED_RUNTIME_VERSION and models > 0
            )
            checks.append(
                PreflightCheck(
                    "copilot_runtime",
                    runtime_ok,
                    (
                        f"authenticated={authenticated}, runtime={runtime_version}, "
                        f"models={models}, expected_runtime={EXPECTED_RUNTIME_VERSION}"
                    ),
                )
            )

        return PreflightReport(
            schema_version=PREFLIGHT_SCHEMA_VERSION,
            checks=tuple(checks),
            discord_identity=discord_identity,
            copilot_runtime=copilot_runtime,
        )

    @staticmethod
    def _default_discord_probe(token: str) -> dict[str, Any]:
        request = urllib.request.Request(
            "https://discord.com/api/v10/users/@me",
            headers={
                "Authorization": f"Bot {token}",
                "User-Agent": "copilotd-setup/0.1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"Discord returned HTTP {response.status}")
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Discord token rejected with HTTP {error.code}") from error
        return {
            "id": str(payload["id"]),
            "username": str(payload.get("username", payload["id"])),
        }

    async def _default_copilot_probe(self) -> dict[str, Any]:
        bridge = CopilotBridge(self._settings)
        try:
            async with asyncio.timeout(30):
                await bridge.start()
                identity = await bridge.runtime_identity()
                models = await bridge.list_models()
        finally:
            await bridge.stop()
        return {**identity, "model_count": len(models)}


async def _call_probe(probe: Callable[..., Any], *arguments: object) -> Any:
    if asyncio.iscoroutinefunction(probe):
        return await probe(*arguments)
    return await asyncio.to_thread(probe, *arguments)


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not-installed"


def _safe_error(error: Exception) -> str:
    text = str(error).replace(os.linesep, " ").strip()
    return f"{type(error).__name__}: {text}" if text else type(error).__name__
