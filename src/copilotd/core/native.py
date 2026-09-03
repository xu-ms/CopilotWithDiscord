from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from copilotd.sdk.native import NativeCommandDefinition
from copilotd.storage.database import Database

_COMMAND_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")

BUILTIN_CAPABILITIES = {
    "after": "builtin_after",
    "every": "builtin_every",
    "research": "builtin_research",
    "review": "builtin_review",
    "rubber-duck": "builtin_rubber_duck",
    "security-review": "builtin_security_review",
}

TERMINAL_TASK_STATES = frozenset({"cancelled", "completed", "failed"})


class NativeCapabilityError(RuntimeError):
    code = "CD-CAP-001"


class NativeTaskAction(StrEnum):
    LIST = "list"
    SHOW = "show"
    PROGRESS = "progress"
    MESSAGE = "message"
    PROMOTE = "promote"
    CANCEL = "cancel"
    ALL = "all"
    REMOVE = "remove"
    WAIT = "wait"


class NativeAgentAction(StrEnum):
    LIST = "list"
    CURRENT = "current"
    SELECT = "select"
    DESELECT = "deselect"


class NativeRemoteMode(StrEnum):
    OFF = "off"
    EXPORT = "export"
    ON = "on"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class RemotePrerequisites:
    authenticated: bool
    auth_type: str | None
    auth_host: str | None
    repository_root: str
    repository_host: str
    has_origin: bool
    snapshot: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NativeBridge(Protocol):
    async def get_session_auth(self, session: Any) -> dict[str, Any]: ...

    async def get_remote_state(self, session: Any) -> dict[str, Any]: ...


class CapabilityView(Protocol):
    def supports(self, capability: str) -> bool: ...


class NativeManifestController:
    def __init__(self, database: Database, capabilities: CapabilityView | None) -> None:
        self._database = database
        self._capabilities = capabilities

    def validate(
        self,
        commands: tuple[NativeCommandDefinition, ...],
    ) -> tuple[NativeCommandDefinition, ...]:
        seen: set[str] = set()
        builtins: list[NativeCommandDefinition] = []
        for command in commands:
            if not _COMMAND_NAME.fullmatch(command.name):
                raise NativeCapabilityError(
                    f"runtime returned an invalid command name: {command.name!r}"
                )
            if command.name in seen:
                raise NativeCapabilityError(
                    f"runtime returned duplicate command metadata for {command.name}"
                )
            seen.add(command.name)
            if command.kind == "builtin":
                builtins.append(command)
        return tuple(sorted(builtins, key=lambda item: item.name))

    async def require_builtin(self, sdk_session_id: str, command_name: str) -> dict[str, Any]:
        capability = BUILTIN_CAPABILITIES.get(command_name)
        if capability is None or (
            self._capabilities is not None and not self._capabilities.supports(capability)
        ):
            raise NativeCapabilityError(
                f"native builtin {command_name!r} has no verified invocation capability"
            )
        row = await self._database.fetchone(
            """
            SELECT * FROM runtime_command_manifest
            WHERE sdk_session_id = ? AND command_name = ?
              AND kind = 'builtin' AND state = 'available'
            """,
            (sdk_session_id, command_name),
        )
        if row is None:
            raise NativeCapabilityError(
                f"native builtin {command_name!r} is absent from the current runtime manifest"
            )
        return dict(row)

    async def available_builtins(self, sdk_session_id: str) -> tuple[dict[str, Any], ...]:
        rows = await self._database.fetchall(
            """
            SELECT * FROM runtime_command_manifest
            WHERE sdk_session_id = ? AND kind = 'builtin' AND state = 'available'
            ORDER BY command_name
            """,
            (sdk_session_id,),
        )
        return tuple(dict(row) for row in rows)


class RemotePreflightController:
    def __init__(self, bridge: NativeBridge) -> None:
        self._bridge = bridge

    async def inspect(self, session: Any, cwd: str) -> RemotePrerequisites:
        status = await self.status(session, cwd)
        if not status.authenticated:
            raise NativeCapabilityError("Copilot authentication is required for remote exposure")
        if not status.repository_root or status.repository_host not in {
            "github.com",
            "ssh.github.com",
        }:
            raise NativeCapabilityError(
                "remote exposure requires a GitHub origin in the current repository"
            )
        return status

    async def status(self, session: Any, cwd: str) -> RemotePrerequisites:
        auth, snapshot = await asyncio.gather(
            self._bridge.get_session_auth(session),
            self._bridge.get_remote_state(session),
        )
        authenticated = bool(auth.get("isAuthenticated"))
        repository_root = await _git_output(
            cwd,
            "rev-parse",
            "--show-toplevel",
            required=False,
        )
        origin = await _git_output(cwd, "remote", "get-url", "origin", required=False)
        repository_host = _repository_host(origin)
        return RemotePrerequisites(
            authenticated=authenticated,
            auth_type=_optional_string(auth.get("authType")),
            auth_host=_optional_string(auth.get("host")),
            repository_root=repository_root,
            repository_host=repository_host,
            has_origin=bool(origin),
            snapshot=snapshot,
        )


def stable_hash(value: str | None) -> str | None:
    return None if value is None else hashlib.sha256(value.encode()).hexdigest()


def json_payload(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def timestamp_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        numeric = float(value)
        return numeric / 1000 if numeric > 10_000_000_000 else numeric
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    raise ValueError(f"unsupported runtime timestamp: {value!r}")


async def _git_output(
    cwd: str,
    *arguments: str,
    required: bool = True,
) -> str:
    process = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        cwd,
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        if not required:
            return ""
        detail = stderr.decode(errors="replace").strip()
        raise NativeCapabilityError(f"Git repository prerequisite failed: {detail or arguments[0]}")
    return stdout.decode(errors="replace").strip()


def _repository_host(origin: str) -> str:
    if origin.startswith("git@"):
        return origin.split("@", 1)[1].split(":", 1)[0].lower()
    if "://" in origin:
        return origin.split("://", 1)[1].split("/", 1)[0].split("@")[-1].lower()
    return ""


def _optional_string(value: Any) -> str | None:
    return None if value is None else str(value)
