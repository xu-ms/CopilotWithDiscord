from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from copilotd.config import Settings
from copilotd.ops.service import ServiceManager
from copilotd.storage.database import Database

_AUTH_HEADER_RE = re.compile(
    r"(?im)(?P<prefix>\b(?:proxy-)?authorization\b\s*[:=]\s*)"
    r"(?:(?:basic|bearer)\s+)?[^\r\n]*"
)
_COOKIE_HEADER_RE = re.compile(r"(?im)(?P<prefix>\b(?:set-cookie|cookie)\b\s*[:=]\s*)[^\r\n]*")
_TEXT_KEY = r"""(?:"[^"\r\n]{1,128}"|'[^'\r\n]{1,128}'|[A-Za-z0-9][A-Za-z0-9._:/-]{0,127})"""
_GENERIC_DOUBLE_RE = re.compile(
    rf"(?i)(?P<key>{_TEXT_KEY})(?P<sep>\s*[:=]\s*)"
    r'"(?!\[redacted\])(?P<value>(?:\\.|[^"\\])*)"'
)
_GENERIC_SINGLE_RE = re.compile(
    rf"(?i)(?P<key>{_TEXT_KEY})(?P<sep>\s*[:=]\s*)"
    r"'(?!\[redacted\])(?P<value>(?:\\.|[^'\\])*)'"
)
_GENERIC_UNQUOTED_RE = re.compile(
    rf"(?i)(?P<key>{_TEXT_KEY})(?P<sep>\s*[:=]\s*)"
    r"(?P<value>(?!\[redacted\])[^\s,;)}\]\"\']+)"
)
_ASSIGNMENT_PREFIX_RE = re.compile(rf"(?i)(?P<key>{_TEXT_KEY})(?P<sep>\s*[:=]\s*)")


class LocalOpsSurface:
    """Bounded, deterministic diagnostics backed by the local durable state."""

    def __init__(self, database: Database, settings: Settings) -> None:
        self._database = database
        self._settings = settings

    async def health(self) -> dict[str, Any]:
        service = await asyncio.to_thread(ServiceManager(self._settings).status)
        counts: dict[str, int] = {}
        for name, query in (
            (
                "active_sessions",
                "SELECT COUNT(*) FROM session_bindings "
                "WHERE binding_intent = 'active' AND attachment_state != 'terminal'",
            ),
            (
                "active_leases",
                "SELECT COUNT(*) FROM liveness_leases WHERE state = 'active'",
            ),
            (
                "queued_messages",
                "SELECT COUNT(*) FROM message_queue "
                "WHERE state NOT IN ('cancelled', 'submitted', 'failed')",
            ),
            (
                "pending_outbox",
                "SELECT COUNT(*) FROM render_outbox WHERE state = 'pending'",
            ),
            (
                "blocked_outbox",
                "SELECT COUNT(*) FROM render_outbox WHERE state = 'blocked'",
            ),
            (
                "runtime_schedules",
                "SELECT COUNT(*) FROM runtime_schedules WHERE state IN ('active', 'unknown')",
            ),
            (
                "app_schedules",
                "SELECT COUNT(*) FROM schedules WHERE state = 'enabled'",
            ),
        ):
            row = await self._database.fetchone(query)
            counts[name] = 0 if row is None else int(row[0])
        integrity = await self._database.fetchone("PRAGMA quick_check")
        return {
            "database": "ok" if integrity is not None and integrity[0] == "ok" else "degraded",
            "services": asdict(service),
            **counts,
        }

    async def diagnostics(self, *, session_id: str | None = None) -> dict[str, Any]:
        parameters: tuple[Any, ...] = ()
        where = ""
        if session_id is not None:
            where = "WHERE sdk_session_id = ?"
            parameters = (session_id,)
        bindings = await self._database.fetchall(
            f"""
            SELECT sdk_session_id, thread_id, binding_intent, attachment_state,
                   runtime_generation, owner_fence_token, last_inbox_seq,
                   last_sdk_receive_seq, last_event_at, activity_observed_at,
                   runtime_processing, runtime_has_active_work
            FROM session_bindings {where}
            ORDER BY updated_at DESC LIMIT 20
            """,
            parameters,
        )
        capabilities = await self._database.fetchall(
            """
            SELECT runtime_version, sdk_version, capability, supported,
                   probe_detail, probed_at
            FROM capabilities ORDER BY probed_at DESC, capability LIMIT 100
            """
        )
        incidents = await self._database.fetchall(
            """
            SELECT timestamp, runtime_generation, session_id, kind,
                   stderr_tail, last_inbox_seq, last_sdk_receive_seq
            FROM runtime_incidents
            WHERE (? IS NULL OR session_id = ?)
            ORDER BY timestamp DESC LIMIT 20
            """,
            (session_id, session_id),
        )
        return _redact_structure(
            {
                "bindings": [dict(row) for row in bindings],
                "capabilities": [dict(row) for row in capabilities],
                "incidents": [dict(row) for row in incidents],
            }
        )

    async def debug(self, *, level: str, duration_minutes: int) -> dict[str, Any]:
        normalized = level.lower()
        if normalized not in {"info", "debug", "trace"}:
            raise ValueError("debug level must be info, debug, or trace")
        if not 1 <= duration_minutes <= 30:
            raise ValueError("debug duration must be between 1 and 30 minutes")
        expires_at = time.time() + duration_minutes * 60
        await self._database.execute(
            """
            INSERT INTO global_config(key, value, updated_at)
            VALUES ('temporary_debug', ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value, updated_at = excluded.updated_at
            """,
            (
                json.dumps(
                    {"level": normalized, "expires_at": expires_at},
                    sort_keys=True,
                ),
                time.time(),
            ),
        )
        return {
            "level": normalized,
            "duration_minutes": duration_minutes,
            "expires_at": expires_at,
        }

    async def log_tail(self, *, correlation_id: str | None = None) -> dict[str, Any]:
        files = await asyncio.to_thread(
            lambda: sorted(self._settings.log_dir.glob("*.log"), key=lambda path: path.name)
        )
        result: dict[str, str] = {}
        remaining = 64 * 1024
        for path in files[:12]:
            if remaining <= 0:
                break
            text = await asyncio.to_thread(_read_tail, path, min(remaining, 16 * 1024))
            if correlation_id:
                text = "\n".join(line for line in text.splitlines() if correlation_id in line)
            text = _redact_text(text)
            result[path.name] = text
            remaining -= len(text.encode("utf-8"))
        return {
            "correlation_id": correlation_id,
            "files": result,
            "truncated": remaining <= 0,
        }

    async def log_dump(self, *, correlation_id: str | None = None) -> dict[str, Any]:
        logs, timeline = await asyncio.gather(
            self.log_tail(correlation_id=correlation_id),
            self.event_dump(),
        )
        return _redact_structure(
            {
                "correlation_id": correlation_id,
                "logs": logs,
                "event_timeline": timeline,
            }
        )

    async def event_dump(self, *, session_id: str | None = None) -> dict[str, Any]:
        rows = await self._database.fetchall(
            """
            SELECT sdk_session_id, generation, inbox_seq, source, raw_type,
                   parent_id, agent_id, message_id, turn_id, received_at
            FROM event_journal
            WHERE (? IS NULL OR sdk_session_id = ?)
            ORDER BY received_at DESC, inbox_seq DESC LIMIT 250
            """,
            (session_id, session_id),
        )
        return _redact_structure(
            {
                "session_id": session_id,
                "events": [dict(row) for row in reversed(rows)],
                "bounded_to": 250,
            }
        )


def _read_tail(path: Path, limit: int) -> str:
    with path.open("rb") as file:
        file.seek(0, 2)
        size = file.tell()
        file.seek(max(0, size - limit))
        content = file.read(limit)
    return content.decode("utf-8", errors="replace")


def _redact_structure(value: Any, *, sensitive: bool = False) -> Any:
    if sensitive:
        return "[redacted]"
    if isinstance(value, Mapping):
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            redacted[key] = _redact_structure(item, sensitive=_is_sensitive_key(key))
        return redacted
    if isinstance(value, list):
        return [_redact_structure(item, sensitive=False) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_structure(item, sensitive=False) for item in value)
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, bytes):
        return _redact_text(value.decode("utf-8", errors="replace"))
    return value


def redact_sensitive_text(value: str) -> str:
    return _redact_text(value)


def redact_sensitive_value(value: Any) -> Any:
    return _redact_structure(value)


def _is_sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", str(key).lower())
    if not normalized:
        return False
    benign = {
        "secretcount",
        "credentialtype",
        "hassecret",
        "nonsecret",
        "publicmetadata",
        "metadata",
    }
    if normalized in benign:
        return False
    direct_tokens = (
        "authorization",
        "cookie",
        "setcookie",
        "token",
        "password",
        "apikey",
        "secret",
        "secrets",
        "credential",
        "credentials",
        "passphrase",
        "webhooksecret",
        "awssecretaccesskey",
        "accesstoken",
        "accesssecret",
        "accesskey",
        "clienttoken",
        "clientsecret",
        "clientkey",
        "privatetoken",
        "privatesecret",
        "privatekey",
        "sessiontoken",
        "refreshtoken",
    )
    if any(token in normalized for token in direct_tokens):
        return True
    if (
        "aws" in normalized
        and "secret" in normalized
        and "access" in normalized
        and "key" in normalized
    ):
        return True
    if any(
        prefix in normalized for prefix in ("access", "client", "private", "session", "refresh")
    ) and any(suffix in normalized for suffix in ("key", "token", "secret", "credential")):
        return True
    return False


def _redact_text(value: str) -> str:
    parsed = _redact_json_fragment(value)
    if parsed is not None:
        return parsed
    redacted = _redact_json_fragments(value)
    redacted = _AUTH_HEADER_RE.sub(lambda match: f"{match.group('prefix')}[redacted]", redacted)
    redacted = _COOKIE_HEADER_RE.sub(lambda match: f"{match.group('prefix')}[redacted]", redacted)
    redacted = _redact_assignment_values(redacted)
    return redacted


def _redact_json_fragments(text: str) -> str:
    redacted = text
    spans: list[tuple[int, int, str]] = []
    for match in re.finditer(r"[\[{]", redacted):
        end = _consume_balanced_json(redacted, match.start())
        if end is None:
            continue
        replacement = _redact_json_fragment(redacted[match.start() : end])
        if replacement is None:
            continue
        spans.append((match.start(), end, replacement))
    if not spans:
        return redacted
    output: list[str] = []
    cursor = 0
    for start, end, replacement in spans:
        if start < cursor:
            continue
        output.append(redacted[cursor:start])
        output.append(replacement)
        cursor = end
    output.append(redacted[cursor:])
    return "".join(output)


def _redact_json_fragment(fragment: str) -> str | None:
    try:
        parsed = json.loads(fragment)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    redacted = _redact_structure(parsed)
    return json.dumps(redacted, separators=(",", ":"), ensure_ascii=False)


def _redact_assignment_values(text: str) -> str:
    output: list[str] = []
    cursor = 0
    while True:
        match = _ASSIGNMENT_PREFIX_RE.search(text, cursor)
        if match is None:
            output.append(text[cursor:])
            break
        output.append(text[cursor : match.start()])
        output.append(match.group(0))
        value_start = match.end()
        if value_start >= len(text):
            cursor = value_start
            continue
        key = match.group("key")
        char = text[value_start]
        if char in "\"'":
            value_end = _consume_quoted_value(text, value_start)
            if value_end is None:
                if _is_sensitive_key(key):
                    output.append(char + "[redacted]" + char)
                else:
                    output.append(char + _redact_assignment_values(text[value_start + 1 :]))
                break
            value = text[value_start:value_end]
            output.append(_redact_assignment_value(key, value, quoted=True))
            cursor = value_end
            continue
        if char in "[{":
            value_end = _consume_balanced_json(text, value_start)
            if value_end is None:
                if _is_sensitive_key(key):
                    output.append("[redacted]")
                else:
                    output.append(char + _redact_assignment_values(text[value_start + 1 :]))
                break
            value = text[value_start:value_end]
            output.append(_redact_assignment_value(key, value, quoted=False))
            cursor = value_end
            continue
        value_end = _consume_bare_value(text, value_start)
        value = text[value_start:value_end]
        output.append(_redact_assignment_value(key, value, quoted=False))
        cursor = value_end
    return "".join(output)


def _redact_assignment_value(key: str, value: str, *, quoted: bool) -> str:
    if _is_sensitive_key(key):
        if quoted:
            return value[0] + "[redacted]" + value[0]
        return "[redacted]"
    stripped = value.strip()
    if stripped and stripped[0] in "[{":
        redacted = _redact_json_fragment(stripped)
        if redacted is not None:
            return redacted
        return _redact_assignment_values(value)
    if quoted and len(value) >= 2:
        inner = value[1:-1].strip()
        if inner and inner[0] in "[{":
            redacted = _redact_json_fragment(inner)
            if redacted is not None:
                return value[0] + redacted + value[-1]
            return value[0] + _redact_assignment_values(value[1:-1]) + value[-1]
    return value


def _consume_quoted_value(text: str, start: int) -> int | None:
    quote = text[start]
    escaped = False
    for index in range(start + 1, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == quote:
            return index + 1
    return None


def _consume_bare_value(text: str, start: int) -> int:
    end = start
    while end < len(text) and text[end] not in "\r\n\t ,;)}]":
        end += 1
    return end


def _consume_balanced_json(text: str, start: int) -> int | None:
    opening = text[start]
    if opening not in "[{":
        return None
    stack = [opening]
    in_string: str | None = None
    escaped = False
    index = start + 1
    while index < len(text):
        char = text[index]
        if in_string is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == in_string:
                in_string = None
        else:
            if char in "\"'":
                in_string = char
            elif char in "[{":
                stack.append(char)
            elif char in "]}":
                if not stack:
                    return None
                open_char = stack.pop()
                if (open_char == "{" and char != "}") or (open_char == "[" and char != "]"):
                    return None
                if not stack:
                    return index + 1
        index += 1
    return None
