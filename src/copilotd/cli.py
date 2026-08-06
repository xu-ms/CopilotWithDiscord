from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
from importlib.metadata import version
from typing import Any

from copilotd.config import load_settings
from copilotd.discord_app import run_discord_bot
from copilotd.logging import configure_logging
from copilotd.ops.service import ServiceManager, status_dict
from copilotd.sdk.bridge import CopilotBridge
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.sdk.extension_probe import ExtensionAcceptanceProbe
from copilotd.sdk.probe import SdkProbe, _to_jsonable
from copilotd.storage.database import Database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="copilotd")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("db-init", help="create or migrate the durable SQLite database")
    subparsers.add_parser("doctor", help="check local configuration and static SDK contracts")
    subparsers.add_parser("setup", help="install, start, and verify the always-on service")
    run = subparsers.add_parser("run", help="run the Discord gateway in the foreground")
    run.add_argument(
        "--foreground",
        action="store_true",
        help="confirm foreground development mode",
    )
    service = subparsers.add_parser("service", help="manage the OS service")
    service_commands = service.add_subparsers(dest="service_command", required=True)
    service_commands.add_parser("install", help="install and immediately start services")
    service_commands.add_parser("status", help="show service and heartbeat status")
    restart = service_commands.add_parser("restart", help="restart the bot service")
    restart.add_argument("--force", action="store_true")
    service_commands.add_parser("uninstall", help="unregister services but retain state")
    service_commands.add_parser("watchdog", help="run one protected-work-aware health check")
    service_commands.add_parser("logs", help="show service log paths")

    probe = subparsers.add_parser("sdk-probe", help="record the Copilot SDK capability matrix")
    probe.add_argument("--live", action="store_true", help="start the runtime and run a live turn")
    probe.add_argument(
        "--live-extensions",
        action="store_true",
        help="run secure disposable protocol/MCP/config acceptance",
    )
    probe.add_argument(
        "--prompt",
        default="Reply with exactly COPILOTD_PROBE_OK and do not use tools.",
    )
    probe.add_argument("--timeout", type=float, default=120)
    probe.add_argument("--keep-session", action="store_true")
    probe.add_argument(
        "--probe-native-schedule",
        action="store_true",
        help="invoke /after and wait for a post-idle scheduled turn",
    )
    probe.add_argument(
        "--probe-sidecar",
        action="store_true",
        help="test detached execution and replay against a TCP sidecar",
    )
    return parser


async def run_command(args: argparse.Namespace) -> int:
    settings = load_settings()
    installing = args.command == "setup" or (
        args.command == "service" and args.service_command == "install"
    )
    if installing and settings.discord_token is None:
        raise ValueError("COPILOTD_DISCORD_TOKEN is required for service installation")
    if installing and settings.github_token is None:
        raise ValueError("COPILOTD_GITHUB_TOKEN is required for service installation")
    settings.ensure_directories()
    configure_logging(settings.log_level)

    if args.command == "db-init":
        async with Database(settings.database_path):
            pass
        _print_json({"database": str(settings.database_path), "migrated": True})
        return 0

    if args.command == "doctor":
        bridge = CopilotBridge(settings)
        async with Database(settings.database_path) as database:
            await bridge.start()
            try:
                runtime_identity = await bridge.runtime_identity()
                capabilities = await CapabilityRegistry(settings).activate(
                    database,
                    runtime_identity,
                )
            finally:
                await bridge.stop()
            migrations = await database.fetchall(
                "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
            )
        result = {
            "copilotd_version": version("copilotd"),
            "sdk_version": version("github-copilot-sdk"),
            "python": platform.python_version(),
            "data_dir": str(settings.data_dir),
            "resolved_home": str(settings.resolved_home),
            "database": str(settings.database_path),
            "migrations": [dict(row) for row in migrations],
            "runtime": runtime_identity,
            "sdk": capabilities.to_dict(),
        }
        _print_json(result)
        return 0

    if args.command == "run":
        if not args.foreground:
            raise ValueError("use `copilotd run --foreground`; service mode is installed by setup")
        await run_discord_bot(settings)
        return 0

    if args.command == "setup":
        manager = ServiceManager(settings)
        await asyncio.to_thread(manager.install)
        deadline = asyncio.get_running_loop().time() + 45
        while True:
            status = await asyncio.to_thread(manager.status)
            if (
                status.bot_loaded
                and status.watchdog_loaded
                and status.heartbeat_age_seconds is not None
                and status.heartbeat_age_seconds <= 45
            ):
                _print_json(status_dict(status))
                return 0
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(
                    "service installed but did not produce a healthy heartbeat within 45 seconds"
                )
            await asyncio.sleep(1)

    if args.command == "service":
        manager = ServiceManager(settings)
        if args.service_command == "install":
            await asyncio.to_thread(manager.install)
            _print_json(status_dict(await asyncio.to_thread(manager.status)))
        elif args.service_command == "status":
            _print_json(status_dict(await asyncio.to_thread(manager.status)))
        elif args.service_command == "restart":
            await asyncio.to_thread(manager.restart, force=args.force)
            _print_json({"restarted": True, "force": args.force})
        elif args.service_command == "uninstall":
            await asyncio.to_thread(manager.uninstall)
            _print_json({"uninstalled": True, "state_retained": str(settings.data_dir)})
        elif args.service_command == "watchdog":
            outcome = await asyncio.to_thread(manager.watchdog)
            _print_json({"watchdog": outcome})
        elif args.service_command == "logs":
            _print_json(
                {
                    "app": str(settings.log_dir / "copilotd.log"),
                    "boot": str(settings.log_dir / "boot.log"),
                    "watchdog": str(settings.log_dir / "watchdog.log"),
                    "alerts": str(settings.log_dir / "alerts.log"),
                }
            )
        else:
            raise AssertionError(f"unhandled service command: {args.service_command}")
        return 0

    if args.command == "sdk-probe":
        if args.live_extensions:
            extension_probe = ExtensionAcceptanceProbe(
                settings.model_copy(update={"sdk_no_auto_login": True})
            )
            result = await extension_probe.run_live(wait_seconds=args.timeout)
            evidence_path, evidence_sha256 = extension_probe.write_evidence(result)
            result = {
                **result,
                "evidence": {
                    "path": str(evidence_path),
                    "sha256": evidence_sha256,
                },
            }
        elif args.live:
            probe = SdkProbe(settings)
            result = await probe.run_live(
                prompt=args.prompt,
                wait_seconds=args.timeout,
                keep_session=args.keep_session,
                probe_native_schedule=args.probe_native_schedule,
                probe_sidecar=args.probe_sidecar,
            )
        else:
            result = SdkProbe(settings).static_matrix()
        _print_json(result)
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        exit_code = asyncio.run(run_command(args))
    except KeyboardInterrupt:
        exit_code = 130
    raise SystemExit(exit_code)


def _print_json(value: Any) -> None:
    json.dump(value, sys.stdout, default=_to_jsonable, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")
