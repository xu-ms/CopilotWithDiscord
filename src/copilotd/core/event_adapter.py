from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from copilot.session_events import SessionEvent

from copilotd.core.models import AdaptedEvent, InboxEnvelope

INTERNAL_EVENT_SCHEMA_VERSION = 1


class InvalidSdkEvent(ValueError):
    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


class EventAdapter:
    def adapt(self, envelope: InboxEnvelope) -> AdaptedEvent:
        if envelope.source == "sdk":
            if not isinstance(envelope.payload, SessionEvent):
                raise TypeError("SDK envelope payload must be SessionEvent")
            event = envelope.payload
            event_id = _strict_uuid(event.id, field="event_id")
            parent_id = (
                None
                if event.parent_id is None
                else _strict_uuid(event.parent_id, field="parent_id")
            )
            raw_payload = event.to_dict()
            raw_type = event.raw_type or event.type.value
            data = raw_payload.get("data", {})
            return AdaptedEvent(
                sdk_session_id=envelope.sdk_session_id,
                generation=envelope.generation,
                fence_token=envelope.fence_token,
                inbox_seq=envelope.inbox_seq,
                source=envelope.source,
                schema_version=INTERNAL_EVENT_SCHEMA_VERSION,
                sdk_receive_seq=envelope.sdk_receive_seq,
                event_id=event_id,
                ephemeral=event.ephemeral,
                persistence_class="ephemeral" if event.ephemeral is True else "durable",
                raw_type=raw_type,
                parent_id=parent_id,
                agent_id=event.agent_id,
                thread_id=envelope.thread_id,
                sdk_timestamp=event.timestamp.timestamp(),
                message_id=_first_identifier(data, "messageId", "message_id"),
                turn_id=_first_identifier(data, "turnId", "turn_id"),
                interaction_id=_first_identifier(data, "interactionId", "interaction_id"),
                task_id=_first_identifier(
                    data,
                    "taskId",
                    "task_id",
                    "parentAgentTaskId",
                    "parent_agent_task_id",
                ),
                tool_call_id=_first_identifier(data, "toolCallId", "tool_call_id"),
                request_id=_first_identifier(data, "requestId", "request_id"),
                correlation_id=_first_identifier(data, "correlationId", "correlation_id"),
                raw_payload=raw_payload,
                reducer_hash=_payload_hash(raw_payload),
                received_at=envelope.received_at,
            )

        raw_payload = _to_jsonable(envelope.payload)
        if not isinstance(raw_payload, dict):
            raw_payload = {"value": raw_payload}
        raw_type = str(raw_payload.get("type", f"copilotd.{envelope.source}"))
        data = raw_payload.get("data", raw_payload)
        return AdaptedEvent(
            sdk_session_id=envelope.sdk_session_id,
            generation=envelope.generation,
            fence_token=envelope.fence_token,
            inbox_seq=envelope.inbox_seq,
            source=envelope.source,
            schema_version=INTERNAL_EVENT_SCHEMA_VERSION,
            internal_event_id=envelope.internal_event_id,
            persistence_class="internal",
            raw_type=raw_type,
            agent_id=_first_identifier(data, "agentId", "agent_id"),
            thread_id=envelope.thread_id or _first_identifier(data, "threadId", "thread_id"),
            message_id=_first_identifier(data, "messageId", "message_id"),
            turn_id=_first_identifier(data, "turnId", "turn_id"),
            interaction_id=_first_identifier(data, "interactionId", "interaction_id"),
            task_id=_first_identifier(
                data,
                "taskId",
                "task_id",
                "parentAgentTaskId",
                "parent_agent_task_id",
            ),
            tool_call_id=_first_identifier(data, "toolCallId", "tool_call_id"),
            request_id=_first_identifier(data, "requestId", "request_id"),
            correlation_id=_first_identifier(data, "correlationId", "correlation_id"),
            raw_payload=raw_payload,
            reducer_hash=_payload_hash(raw_payload),
            received_at=envelope.received_at,
        )


def _strict_uuid(value: Any, *, field: str) -> str:
    if value is None:
        raise InvalidSdkEvent(f"missing_{field}", f"SDK event {field} is missing")
    try:
        parsed = UUID(str(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise InvalidSdkEvent(
            f"invalid_{field}",
            f"SDK event {field} is not a UUID: {value!r}",
        ) from error
    return str(parsed)


def _first_identifier(data: Any, *keys: str) -> str | None:
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if value is not None:
            return str(value)
    return None


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    return value
