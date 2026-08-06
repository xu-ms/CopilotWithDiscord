from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from copilotd.core.inbox import ReducerInbox


@dataclass(frozen=True, slots=True)
class HookSessionContext:
    sdk_session_id: str
    runtime_generation: int
    owner_fence_token: int
    thread_id: str
    project_id: str | None
    project_source: str
    cwd_snapshot: str
    config_version: int
    config_hash: str | None

    def prompt_context(self) -> str:
        return "copilotD session context: " + json.dumps(
            {
                "session_id": self.sdk_session_id,
                "thread_id": self.thread_id,
                "project_id": self.project_id,
                "project_source": self.project_source,
                "cwd": self.cwd_snapshot,
                "config_version": self.config_version,
                "config_hash": self.config_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


class SessionHookAudit:
    """Builds non-mutating SDK hooks that emit typed, redacted audit receipts."""

    def __init__(
        self,
        *,
        inbox: ReducerInbox,
        context: HookSessionContext,
    ) -> None:
        self._inbox = inbox
        self._context = context

    def handlers(self) -> dict[str, Any]:
        return {
            "on_pre_tool_use": self.on_pre_tool_use,
            "on_pre_mcp_tool_call": self.on_pre_mcp_tool_call,
            "on_post_tool_use": self.on_post_tool_use,
            "on_post_tool_use_failure": self.on_post_tool_use_failure,
            "on_user_prompt_submitted": self.on_user_prompt_submitted,
            "on_session_start": self.on_session_start,
            "on_session_end": self.on_session_end,
            "on_error_occurred": self.on_error_occurred,
        }

    async def on_pre_tool_use(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        await self._record(
            "pre_tool_use",
            "pre",
            hook_input,
            hook_context,
            payload={
                "tool_name": hook_input.get("toolName"),
                "tool_args_hash": _hash_value(hook_input.get("toolArgs")),
            },
        )

    async def on_pre_mcp_tool_call(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        await self._record(
            "pre_mcp_tool_call",
            "pre",
            hook_input,
            hook_context,
            payload={
                "server_name": hook_input.get("serverName"),
                "tool_name": hook_input.get("toolName"),
                "tool_call_id": hook_input.get("toolCallId"),
                "arguments_hash": _hash_value(hook_input.get("arguments")),
                "meta_keys": sorted(
                    str(key)
                    for key in (
                        hook_input.get("_meta")
                        if isinstance(hook_input.get("_meta"), Mapping)
                        else {}
                    )
                ),
            },
        )

    async def on_post_tool_use(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        result = hook_input.get("toolResult")
        await self._record(
            "post_tool_use",
            "post",
            hook_input,
            hook_context,
            payload={
                "tool_name": hook_input.get("toolName"),
                "tool_args_hash": _hash_value(hook_input.get("toolArgs")),
                "result_hash": _hash_value(result),
                "result_metadata": _tool_result_metadata(result),
            },
        )

    async def on_post_tool_use_failure(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        error = str(hook_input.get("error") or "")
        await self._record(
            "post_tool_use_failure",
            "failure",
            hook_input,
            hook_context,
            classification=_classify_error(error),
            payload={
                "tool_name": hook_input.get("toolName"),
                "tool_args_hash": _hash_value(hook_input.get("toolArgs")),
                "error_hash": _hash_value(error),
                "error_length": len(error),
                "auto_retry": False,
            },
        )

    async def on_user_prompt_submitted(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        prompt = str(hook_input.get("prompt") or "")
        await self._record(
            "user_prompt_submitted",
            "submitted",
            hook_input,
            hook_context,
            payload={
                "prompt_hash": _hash_value(prompt),
                "prompt_length": len(prompt),
                "provenance": "discord",
            },
        )

    async def on_user_prompt_transformed(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        prompt = str(hook_input.get("prompt") or "")
        transformed = str(hook_input.get("transformedPrompt") or "")
        await self._record(
            "user_prompt_transformed",
            "transformed",
            hook_input,
            hook_context,
            payload={
                "prompt_hash": _hash_value(prompt),
                "transformed_hash": _hash_value(transformed),
                "changed": prompt != transformed,
            },
        )

    async def on_session_start(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> dict[str, str]:
        await self._record(
            "session_start",
            "start",
            hook_input,
            hook_context,
            payload={
                "source": hook_input.get("source"),
                "working_directory": hook_input.get("workingDirectory"),
                "has_initial_prompt": hook_input.get("initialPrompt") is not None,
            },
        )
        return {"additionalContext": self._context.prompt_context()}

    async def on_session_end(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        await self._record(
            "session_end",
            "end",
            hook_input,
            hook_context,
            classification=str(hook_input.get("reason") or "unknown"),
            payload={
                "reason": hook_input.get("reason"),
                "has_final_message": hook_input.get("finalMessage") is not None,
                "error_hash": (
                    None
                    if hook_input.get("error") is None
                    else _hash_value(hook_input.get("error"))
                ),
                "semantic_close_authority": False,
            },
        )

    async def on_error_occurred(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        error = str(hook_input.get("error") or "")
        await self._record(
            "error_occurred",
            "error",
            hook_input,
            hook_context,
            classification=str(hook_input.get("errorContext") or _classify_error(error)),
            payload={
                "error_hash": _hash_value(error),
                "error_length": len(error),
                "recoverable": hook_input.get("recoverable"),
                "error_context": hook_input.get("errorContext"),
            },
        )

    async def on_agent_stop(
        self,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
    ) -> None:
        await self._record(
            "agent_stop",
            "stop",
            hook_input,
            hook_context,
            classification=str(hook_input.get("stopReason") or "unknown"),
            payload={
                "stop_reason": hook_input.get("stopReason"),
                "stop_hook_active": hook_input.get("stopHookActive"),
                "transcript_present": hook_input.get("transcriptPath") is not None,
                "background_terminal_authority": False,
            },
        )

    async def _record(
        self,
        hook_name: str,
        phase: str,
        hook_input: Mapping[str, Any],
        hook_context: Mapping[str, str],
        *,
        payload: Mapping[str, Any],
        classification: str | None = None,
    ) -> None:
        invocation_id = _hook_invocation_id(
            hook_name,
            hook_input,
            hook_context,
        )
        audit_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"copilotd:{self._context.sdk_session_id}:"
                    f"{self._context.runtime_generation}:hook:"
                    f"{hook_name}:{invocation_id}:{phase}"
                ),
            )
        )
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        await self._inbox.commit_internal(
            {
                "type": "copilotd.hook.audit",
                "data": {
                    "audit_id": audit_id,
                    "hook_name": hook_name,
                    "hook_invocation_id": invocation_id,
                    "phase": phase,
                    "tool_name": payload.get("tool_name"),
                    "tool_call_id": (
                        hook_input.get("toolCallId")
                        or hook_context.get("tool_call_id")
                        or hook_context.get("toolCallId")
                    ),
                    "correlation_id": (
                        hook_context.get("correlation_id") or hook_context.get("correlationId")
                    ),
                    "classification": classification,
                    "payload_hash": hashlib.sha256(encoded.encode()).hexdigest(),
                    "payload": dict(payload),
                    "observed_at": time.time(),
                },
            },
            internal_event_id=f"hook:{audit_id}",
        )


def _hook_invocation_id(
    hook_name: str,
    hook_input: Mapping[str, Any],
    hook_context: Mapping[str, str],
) -> str:
    explicit = (
        hook_context.get("hook_invocation_id")
        or hook_context.get("hookInvocationId")
        or hook_context.get("invocation_id")
    )
    if explicit:
        return str(explicit)
    identity = {
        "hook": hook_name,
        "session": hook_input.get("sessionId"),
        "timestamp": _jsonable(hook_input.get("timestamp")),
        "tool": hook_input.get("toolName"),
        "tool_call": hook_input.get("toolCallId"),
        "source": hook_input.get("source"),
        "reason": hook_input.get("reason") or hook_input.get("stopReason"),
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]


def _tool_result_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {"kind": type(value).__name__}
    metadata: dict[str, Any] = {
        "keys": sorted(str(key) for key in value),
    }
    for source, target in (
        ("status", "status"),
        ("fileName", "file_name"),
        ("file_name", "file_name"),
        ("mimeType", "mime_type"),
        ("mime_type", "mime_type"),
    ):
        if value.get(source) is not None:
            metadata[target] = str(value[source])
    diff = value.get("diff")
    if diff is not None:
        metadata["diff_hash"] = _hash_value(diff)
        metadata["diff_length"] = len(str(diff))
    return metadata


def _classify_error(value: str) -> str:
    normalized = value.lower()
    if "permission" in normalized or "denied" in normalized:
        return "permission"
    if "timeout" in normalized or "timed out" in normalized:
        return "timeout"
    if "connection" in normalized or "transport" in normalized:
        return "transport"
    return "tool"


def _hash_value(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
