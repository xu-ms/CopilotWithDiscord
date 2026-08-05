from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from copilot.session_events import SessionEvent

from copilotd.core.models import AdaptedEvent, InboxEnvelope


class EventAdapter:
    def adapt(self, envelope: InboxEnvelope) -> AdaptedEvent:
        if envelope.source == "sdk":
            if not isinstance(envelope.payload, SessionEvent):
                raise TypeError("SDK envelope payload must be SessionEvent")
            event = envelope.payload
            raw_payload = event.to_dict()
            raw_type = event.raw_type or event.type.value
            data = raw_payload.get("data", {})
            return AdaptedEvent(
                sdk_session_id=envelope.sdk_session_id,
                generation=envelope.generation,
                fence_token=envelope.fence_token,
                inbox_seq=envelope.inbox_seq,
                source=envelope.source,
                sdk_receive_seq=envelope.sdk_receive_seq,
                event_id=str(event.id),
                ephemeral=event.ephemeral,
                persistence_class="ephemeral" if event.ephemeral is True else "durable",
                raw_type=raw_type,
                parent_id=None if event.parent_id is None else str(event.parent_id),
                agent_id=event.agent_id,
                message_id=_first_identifier(data, "messageId", "message_id"),
                turn_id=_first_identifier(data, "turnId", "turn_id"),
                interaction_id=_first_identifier(data, "interactionId", "interaction_id"),
                request_id=_first_identifier(data, "requestId", "request_id"),
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
            internal_event_id=envelope.internal_event_id,
            persistence_class="internal",
            raw_type=raw_type,
            agent_id=_first_identifier(data, "agentId", "agent_id"),
            message_id=_first_identifier(data, "messageId", "message_id"),
            turn_id=_first_identifier(data, "turnId", "turn_id"),
            interaction_id=_first_identifier(data, "interactionId", "interaction_id"),
            request_id=_first_identifier(data, "requestId", "request_id"),
            raw_payload=raw_payload,
            reducer_hash=_payload_hash(raw_payload),
            received_at=envelope.received_at,
        )


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
