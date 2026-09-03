from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from pathlib import Path
from typing import Any

from copilotd.core.volatile_content import VolatileContentRef

_IDENTIFIER_KEYS = {
    "action",
    "agent",
    "agent_id",
    "basis",
    "builtin_name",
    "capability",
    "code",
    "completion_basis",
    "correlation_id",
    "cursor",
    "cwd_snapshot",
    "delivery",
    "error_category",
    "error_code",
    "error_type",
    "event_id",
    "event_type",
    "expression",
    "hash",
    "id",
    "idempotency_key",
    "kind",
    "lane",
    "layout",
    "message_id",
    "mime_type",
    "mode",
    "model",
    "model_id",
    "name",
    "operation",
    "origin",
    "outcome",
    "parent_id",
    "path",
    "plane",
    "project_source",
    "reason_code",
    "recurrence",
    "request_id",
    "response_plane",
    "schedule_id",
    "source",
    "source_id",
    "state",
    "status",
    "thread_id",
    "timezone",
    "tool_call_id",
    "transport",
    "turn_id",
    "type",
    "url",
}
_CONTENT_KEYS = {
    "answer",
    "arguments",
    "body",
    "command",
    "content",
    "delta",
    "delta_content",
    "description",
    "detail",
    "display_prompt",
    "error",
    "failure",
    "input",
    "invocation_input",
    "message",
    "output",
    "payload",
    "progress",
    "prompt",
    "question",
    "raw",
    "reasoning",
    "response",
    "result",
    "stderr",
    "summary",
    "text",
    "title",
}
_APPLICATION_CONFIGURATION_KEYS = {
    "channel_config",
    "execution_config",
    "model_config",
    "project",
    "project_config",
    "project_snapshot",
    "session_config",
}
_CODE_PATTERN = re.compile(r"[^A-Za-z0-9_.:-]+")


def payload_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def content_receipt(value: Any) -> dict[str, Any]:
    encoded = _canonical_bytes(value)
    return {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "byte_size": len(encoded),
    }


def event_payload_receipt(value: Any) -> str:
    receipt = content_receipt(value)
    return json.dumps(
        {
            "schema": 1,
            "payload_state": "discarded",
            "payload_sha256": receipt["sha256"],
            "payload_bytes": receipt["byte_size"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def render_payload_receipt(payload: dict[str, Any], ref: VolatileContentRef) -> str:
    receipt: dict[str, Any] = {
        "schema": 1,
        "content_sha256": ref.sha256,
        "content_bytes": ref.byte_size,
        "render_kind": str(payload.get("type", "render")),
        "finalized": bool(payload.get("finalized")),
    }
    for key in (
        "submission_id",
        "message_id",
        "agent_id",
        "tool_call_id",
        "source_channel_id",
        "source_message_id",
        "reaction_revision",
        "generation",
        "fence_token",
        "stable_outbox_key",
        "turn_render_key",
        "segment_index",
        "state",
        "superseded",
        "shutdown_event_id",
        "shutdown_generation",
    ):
        value = payload.get(key)
        if value is not None:
            receipt[key] = value
    schedule = payload.get("schedule_run")
    if isinstance(schedule, dict):
        receipt["schedule_run"] = {
            key: schedule[key]
            for key in (
                "run_id",
                "schedule_id",
                "status",
                "completion_basis",
                "error_code",
            )
            if schedule.get(key) is not None
        }
    return json.dumps(receipt, sort_keys=True, separators=(",", ":"))


def state_only_json(
    value: Any,
    *,
    preserve_application_configuration: bool = False,
) -> str:
    return json.dumps(
        scrub_state(
            value,
            preserve_application_configuration=preserve_application_configuration,
        ),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def scrub_state(
    value: Any,
    *,
    key: str | None = None,
    preserve_application_configuration: bool = False,
) -> Any:
    normalized_key = "" if key is None else _normalize_key(key)
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        for item_key, item in value.items():
            text_key = str(item_key)
            item_normalized = _normalize_key(text_key)
            if (
                preserve_application_configuration
                and item_normalized in _APPLICATION_CONFIGURATION_KEYS
            ):
                output[text_key] = _application_configuration(item)
            elif _is_content_key(item_normalized):
                receipt = content_receipt(item)
                output[f"{text_key}_sha256"] = receipt["sha256"]
                output[f"{text_key}_bytes"] = receipt["byte_size"]
            else:
                output[text_key] = scrub_state(
                    item,
                    key=text_key,
                    preserve_application_configuration=(preserve_application_configuration),
                )
        return output
    if isinstance(value, (list, tuple, set, frozenset)):
        return [
            scrub_state(
                item,
                key=key,
                preserve_application_configuration=(preserve_application_configuration),
            )
            for item in value
        ]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        if _is_identifier_key(normalized_key):
            return value
        return f"sha256:{hashlib.sha256(value.encode()).hexdigest()}"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return content_receipt(repr(value))


def _application_configuration(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _application_configuration(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_application_configuration(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return content_receipt(repr(value))


def fixed_error_code(error: BaseException | str | None) -> str | None:
    if error is None:
        return None
    value = error if isinstance(error, str) else type(error).__name__
    normalized = _CODE_PATTERN.sub("_", value).strip("_").lower()
    return normalized[:96] or "error"


def _normalize_key(key: str) -> str:
    output = []
    for character in key:
        if character.isupper() and output:
            output.append("_")
        output.append(character.lower())
    return "".join(output)


def _is_identifier_key(key: str) -> bool:
    return (
        key in _IDENTIFIER_KEYS
        or key.endswith(("_id", "_ids", "_hash", "_sha256", "_state", "_status"))
        or key.endswith(("_at", "_path", "_count", "_bytes", "_version", "_seq"))
    )


def _is_content_key(key: str) -> bool:
    return key in _CONTENT_KEYS or any(
        token in key
        for token in (
            "prompt",
            "content",
            "command",
            "reasoning",
            "response",
            "payload",
            "description",
            "summary",
            "question",
            "answer",
            "output",
            "stderr",
        )
    )


def _canonical_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return repr(value)
