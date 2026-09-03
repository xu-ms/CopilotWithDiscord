from __future__ import annotations

import json
import re
import shlex
from typing import Any

_REDACTED = "[REDACTED]"
_SECRET_KEY = (
    r"(?:access[_-]?token|api[_-]?key|authorization|auth|bearer|cookie|"
    r"credential|password|passwd|private[_-]?key|secret|session[_-]?id)"
)
_COMPOUND_SECRET_OPTION = (
    r"(?:[a-z0-9]+[_-])*(?:"
    r"client[_-](?:secret|token)|oauth[_-]token|refresh[_-]token|"
    r"access[_-](?:key|token)|api[_-]key|private[_-]key|"
    r"auth(?:entication)?[_-]token|id[_-]token|session[_-]id"
    r")(?:[_-][a-z0-9]+)*"
)
_NAMED_SECRET_OPTION = r"(?:[a-z0-9]+[_-])*(?:secret|password|passwd|credential)(?:[_-][a-z0-9]+)*"
_SECRET_OPTION_KEY = (
    rf"(?:token|auth|authorization|bearer|cookie|{_COMPOUND_SECRET_OPTION}|"
    rf"{_NAMED_SECRET_OPTION}|{_SECRET_KEY})"
)
_ENV_ASSIGNMENT = re.compile(r"(?<![\w-])([A-Za-z_][A-Za-z0-9_]*)=(\"[^\"]*\"|'[^']*'|[^\s]+)")
_SECRET_ASSIGNMENT = re.compile(
    rf"(?i)(?<![\w-])({_SECRET_KEY})\b(\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
)
_SECRET_FLAG = re.compile(
    rf"(?i)(?<![\w-])(--?{_SECRET_OPTION_KEY})(\s+|=)"
    r"(\"[^\"]*\"|'[^']*'|[^\s]+)"
)
_SECRET_WORD_VALUE = re.compile(
    rf"(?i)(?<![\w-])({_SECRET_KEY})\b(\s+)(\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
)
_AUTH_HEADER = re.compile(
    r"(?i)\b(authorization|proxy-authorization|x-api-key|cookie|set-cookie)"
    r"(\s*:\s*)([^\r\n]+)"
)
_BEARER = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
_URL_SECRET = re.compile(rf"(?i)([?&]{_SECRET_KEY}=)[^&#\s]+")
_KNOWN_TOKEN = re.compile(
    r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"sk-[A-Za-z0-9_-]{20,}|AKIA[A-Z0-9]{16}|xox[baprs]-[A-Za-z0-9-]{10,})\b"
)
_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")
_URL_USERINFO = re.compile(r"(?i)(https?://)([^/@\s:]+):([^/@\s]+)@")


def redact_sensitive_text(value: Any, *, limit: int = 500) -> str:
    """Return bounded text with credentials, headers, and environment values removed."""
    text = _text_value(value)
    text = _AUTH_HEADER.sub(lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}", text)
    text = _SECRET_FLAG.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}",
        text,
    )
    text = _SECRET_WORD_VALUE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}",
        text,
    )
    text = _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}",
        text,
    )
    text = _URL_SECRET.sub(lambda match: f"{match.group(1)}{_REDACTED}", text)
    text = _BEARER.sub(lambda match: f"{match.group(1)} {_REDACTED}", text)
    text = _KNOWN_TOKEN.sub(_REDACTED, text)
    text = _JWT.sub(_REDACTED, text)
    text = _URL_USERINFO.sub(
        lambda match: f"{match.group(1)}{match.group(2)}:{_REDACTED}@",
        text,
    )
    text = _ENV_ASSIGNMENT.sub(lambda match: f"{match.group(1)}={_REDACTED}", text)
    normalized = " ".join(text.replace("\x00", "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 1)].rstrip() + "…"


def sanitize_tool_name(value: Any) -> str:
    return redact_sensitive_text(value or "Copilot tool", limit=120) or "Copilot tool"


def sanitize_tool_command(data: dict[str, Any]) -> str:
    """Extract only a command-shaped field; never serialize arbitrary tool arguments."""
    candidate: Any = None
    for container in (data, data.get("arguments"), data.get("input")):
        if not isinstance(container, dict):
            continue
        for key in ("command", "cmd", "script", "commandLine"):
            if container.get(key) is not None:
                candidate = container[key]
                break
        if candidate is not None:
            break
    if isinstance(candidate, list) and all(
        isinstance(part, (str, int, float)) for part in candidate
    ):
        candidate = " ".join(shlex.quote(str(part)) for part in candidate)
    if not isinstance(candidate, str) or not candidate.strip():
        return "(command unavailable)"
    try:
        structured = json.loads(candidate)
    except (TypeError, ValueError):
        pass
    else:
        if isinstance(structured, (dict, list)):
            return "(structured command omitted)"
    return redact_sensitive_text(candidate, limit=700) or "(command unavailable)"


def sanitize_failure_summary(value: Any, *, limit: int = 300) -> str:
    if isinstance(value, dict):
        for key in ("message", "reason", "code", "type"):
            if value.get(key) is not None:
                value = value[key]
                break
        else:
            return "The operation failed; details remain in the durable journal."
    summary = redact_sensitive_text(value, limit=limit)
    return summary or "The operation failed; details remain in the durable journal."


def discord_inline_code(value: str) -> str:
    return value.replace("`", "'").replace("\r", " ").replace("\n", " ")


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, BaseException):
        return str(value)
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return "Sensitive structured details omitted."
