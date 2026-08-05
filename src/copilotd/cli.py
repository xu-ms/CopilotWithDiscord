from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from pydantic import ValidationError

from copilotd import __version__
from copilotd.config import Settings, load_settings
from copilotd.discord_app import run_discord_bot
from copilotd.logging import configure_logging
from copilotd.ops.contracts import (
    EXPECTED_RUNTIME_VERSION,
    EXPECTED_SDK_VERSION,
    LATEST_MIGRATION_VERSION,
    SERVICE_STATUS_SCHEMA_VERSION,
)
from copilotd.ops.preflight import PreflightFailed, SetupPreflight
from copilotd.ops.service import (
    RestartBlocked,
    ServiceError,
    ServiceManager,
    status_dict,
)
from copilotd.sdk.probe import SdkProbe, _to_jsonable
from copilotd.storage.database import Database

CLI_SCHEMA_VERSION = 1


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        _print_error(
            code="usage_error",
            message=message,
            exit_code=2,
        )
        raise SystemExit(2)


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(prog="copilotd")
    parser.add_argument("--version", action="version", version=f"copilotd {__version__}")
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
    service_commands.add_parser("runtime", help=argparse.SUPPRESS)

    probe = subparsers.add_parser("sdk-probe", help="record the Copilot SDK capability matrix")
    probe.add_argument("--live", action="store_true", help="start the runtime and run a live turn")
    probe.add_argument(
        "--prompt",
        default="Reply with exactly COPILOTD_PROBE_OK and do not use tools.",
    )
    probe.add_argument("--timeout", type=float, default=120)
    probe.add_argument("--keep-session", action="store_true")
    probe.add_argument(
        "--expect-response",
        help="fail unless a live assistant message exactly matches this text",
    )
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


async def run_command(
    args: argparse.Namespace,
    *,
    settings: Settings | None = None,
    preflight: SetupPreflight | None = None,
    manager: ServiceManager | None = None,
) -> int:
    settings = load_settings() if settings is None else settings
    settings.ensure_directories()
    configure_logging(
        settings.log_level,
        settings.log_dir,
        stderr=os.environ.get("COPILOTD_MANAGED_SERVICE") != "1",
    )

    if args.command == "db-init":
        async with Database(settings.database_path):
            pass
        _print_success(
            "db-init",
            {"database": str(settings.database_path), "migrated": True},
        )
        return 0

    if args.command == "doctor":
        async with Database(settings.database_path) as database:
            migrations = await database.fetchall(
                "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
            )
        latest_migration = max((int(row["version"]) for row in migrations), default=0)
        result = {
            "copilotd_version": _installed_version("copilotd", __version__),
            "sdk_version": version("github-copilot-sdk"),
            "python": platform.python_version(),
            "data_dir": str(settings.data_dir),
            "resolved_home": str(settings.resolved_home),
            "database": str(settings.database_path),
            "migrations": [dict(row) for row in migrations],
            "sdk": SdkProbe(settings).static_matrix(),
            "gates": {
                "sdk_version": EXPECTED_SDK_VERSION,
                "runtime_version": EXPECTED_RUNTIME_VERSION,
                "service_status_schema": SERVICE_STATUS_SCHEMA_VERSION,
                "latest_migration": LATEST_MIGRATION_VERSION,
                "migration_gate_ok": latest_migration == LATEST_MIGRATION_VERSION,
            },
        }
        _print_success("doctor", result)
        return 0

    if args.command == "run":
        if not args.foreground:
            raise ValueError("use `copilotd run --foreground`; service mode is installed by setup")
        if settings.discord_token is None:
            raise ValueError("COPILOTD_DISCORD_TOKEN is required")
        await run_discord_bot(settings)
        return 0

    if args.command == "setup":
        async with Database(settings.database_path):
            pass
        selected_preflight = preflight or SetupPreflight(settings)
        report = await selected_preflight.run()
        report.require_success()
        selected_manager = manager or ServiceManager(settings)
        receipt = await asyncio.to_thread(selected_manager.install)
        status = await asyncio.to_thread(
            selected_manager.verify_post_install,
            receipt,
        )
        _print_success(
            "setup",
            {
                "preflight": report.as_dict(),
                "install": asdict(receipt),
                "status": status_dict(status),
            },
        )
        return 0

    if args.command == "service":
        selected_manager = manager or ServiceManager(settings)
        if args.service_command in {"install", "status", "restart", "watchdog"}:
            async with Database(settings.database_path):
                pass
        if args.service_command == "install":
            selected_preflight = preflight or SetupPreflight(settings)
            report = await selected_preflight.run()
            report.require_success()
            receipt = await asyncio.to_thread(selected_manager.install)
            status = await asyncio.to_thread(
                selected_manager.verify_post_install,
                receipt,
            )
            _print_success(
                "service.install",
                {
                    "preflight": report.as_dict(),
                    "install": asdict(receipt),
                    "status": status_dict(status),
                },
            )
        elif args.service_command == "status":
            _print_success(
                "service.status",
                status_dict(await asyncio.to_thread(selected_manager.status)),
            )
        elif args.service_command == "restart":
            receipt = await asyncio.to_thread(
                selected_manager.restart,
                force=args.force,
            )
            status = await asyncio.to_thread(
                selected_manager.verify_restart,
                receipt,
            )
            _print_success(
                "service.restart",
                {
                    "restarted": True,
                    "force": args.force,
                    "restart": asdict(receipt),
                    "status": status_dict(status),
                },
            )
        elif args.service_command == "uninstall":
            await asyncio.to_thread(selected_manager.uninstall)
            _print_success(
                "service.uninstall",
                {
                    "uninstalled": True,
                    "state_retained": [str(path) for path in settings.durable_directories],
                    "logs_retained": str(settings.log_dir),
                },
            )
        elif args.service_command == "watchdog":
            outcome = await asyncio.to_thread(selected_manager.watchdog)
            _print_success("service.watchdog", {"watchdog": outcome})
        elif args.service_command == "logs":
            _print_success(
                "service.logs",
                {name: str(path) for name, path in settings.log_paths.items()},
            )
        elif args.service_command == "runtime":
            await asyncio.to_thread(selected_manager.run_runtime)
        else:
            raise AssertionError(f"unhandled service command: {args.service_command}")
        return 0

    if args.command == "sdk-probe":
        probe = SdkProbe(settings)
        if args.live:
            result = await probe.run_live(
                prompt=args.prompt,
                wait_seconds=args.timeout,
                keep_session=args.keep_session,
                probe_native_schedule=args.probe_native_schedule,
                probe_sidecar=args.probe_sidecar,
                expected_response=args.expect_response,
            )
        else:
            result = probe.static_matrix()
        _print_success("sdk-probe", result)
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        exit_code = asyncio.run(run_command(args))
    except KeyboardInterrupt:
        exit_code = 130
    except ValidationError as error:
        _print_error("configuration_error", str(error), exit_code=2)
        exit_code = 2
    except ValueError as error:
        _print_error("configuration_error", str(error), exit_code=2)
        exit_code = 2
    except OSError as error:
        _print_error("configuration_error", str(error), exit_code=2)
        exit_code = 2
    except PreflightFailed as error:
        _print_error(
            "preflight_failed",
            str(error),
            exit_code=3,
            detail={"failures": error.failures},
        )
        exit_code = 3
    except RestartBlocked as error:
        _print_error(
            "restart_blocked",
            str(error),
            exit_code=4,
            detail={"blockers": list(error.blockers)},
        )
        exit_code = 4
    except ServiceError as error:
        _print_error("service_error", str(error), exit_code=4)
        exit_code = 4
    except Exception as error:
        _print_error(
            "internal_error",
            f"{type(error).__name__}: {error}",
            exit_code=70,
        )
        exit_code = 70
    raise SystemExit(exit_code)


def _print_json(value: Any) -> None:
    json.dump(value, sys.stdout, default=_to_jsonable, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def _print_success(command: str, result: Any) -> None:
    _print_json(
        {
            "schema_version": CLI_SCHEMA_VERSION,
            "ok": True,
            "command": command,
            "result": result,
        }
    )


def _print_error(
    code: str,
    message: str,
    *,
    exit_code: int,
    detail: dict[str, Any] | None = None,
) -> None:
    payload = {
        "schema_version": CLI_SCHEMA_VERSION,
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "exit_code": exit_code,
            "detail": detail or {},
        },
    }
    json.dump(
        payload,
        sys.stderr,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    sys.stderr.write("\n")


def _installed_version(package: str, fallback: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return fallback
