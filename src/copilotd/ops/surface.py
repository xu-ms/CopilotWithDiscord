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

_SECRET_KEY = r"(?:authorization|token|secret|password|cookie|(?:x[-_ ]?)?api[-_ ]?key)"
_REDACTION_SENTINEL = "__CD_REDACTED__"
_JSON_QUOTED_SECRET = re.compile(
    rf'(?i)(?P<prefix>["\']?{_SECRET_KEY}["\']?\s*:\s*)"(?P<value>(?:\\.|[^"\\])*)"'
)
_JSON_SINGLE_SECRET = re.compile(
    rf"(?i)(?P<prefix>[\"']?{_SECRET_KEY}[\"']?\s*:\s*)'(?P<value>(?:\\.|[^'\\])*)'"
)
_QUOTED_GENERIC_SECRET = re.compile(
    rf'(?i)(?P<prefix>["\']?{_SECRET_KEY}["\']?\s*(?:[:=]|\s+)\s*(?:bearer\s+)?)'
    r'"(?!__CD_REDACTED__)(?P<value>(?:\\.|[^"\\])*)"'
)
_QUOTED_GENERIC_SINGLE_SECRET = re.compile(
    rf"(?i)(?P<prefix>[\"']?{_SECRET_KEY}[\"']?\s*(?:[:=]|\s+)\s*(?:bearer\s+)?)"
    r"'(?!__CD_REDACTED__)(?P<value>(?:\\.|[^'\\])*)'"
)
_UNQUOTED_SECRET = re.compile(
    rf'(?i)(?P<prefix>["\']?{_SECRET_KEY}["\']?\s*(?:[:=]|\s+)\s*(?:bearer\s+)?)'
    r"(?P<value>[^\s,;)}\]\"']+)"
)


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
    if isinstance(value, Mapping):
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            redacted[key] = _redact_structure(item, sensitive=sensitive or _is_sensitive_key(key))
        return redacted
    if isinstance(value, list):
        return [_redact_structure(item, sensitive=sensitive) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_structure(item, sensitive=sensitive) for item in value)
    if sensitive:
        return "[redacted]"
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, bytes):
        return _redact_text(value.decode("utf-8", errors="replace"))
    return value


def _is_sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", str(key).lower())
    return any(
        token in normalized
        for token in (
            "authorization",
            "token",
            "secret",
            "password",
            "cookie",
            "apikey",
        )
    )


def _redact_text(value: str) -> str:
    redacted = _JSON_QUOTED_SECRET.sub(
        lambda match: f'{match.group("prefix")}"{_REDACTION_SENTINEL}"',
        value,
    )
    redacted = _JSON_SINGLE_SECRET.sub(
        lambda match: f"{match.group('prefix')}'{_REDACTION_SENTINEL}'",
        redacted,
    )
    redacted = _QUOTED_GENERIC_SECRET.sub(
        lambda match: f"{match.group('prefix')}{_REDACTION_SENTINEL}",
        redacted,
    )
    redacted = _QUOTED_GENERIC_SINGLE_SECRET.sub(
        lambda match: f"{match.group('prefix')}{_REDACTION_SENTINEL}",
        redacted,
    )
    redacted = _UNQUOTED_SECRET.sub(
        lambda match: f"{match.group('prefix')}{_REDACTION_SENTINEL}",
        redacted,
    )
    return redacted.replace(_REDACTION_SENTINEL, "[redacted]")
