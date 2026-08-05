from __future__ import annotations

from asyncio import Future
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class InboxEnvelope:
    sdk_session_id: str
    generation: int
    fence_token: int
    inbox_seq: int
    source: Literal["sdk", "internal", "snapshot"]
    payload: Any
    received_at: float
    thread_id: str | None = None
    sdk_receive_seq: int | None = None
    internal_event_id: str | None = None
    commit_ack: Future[None] | None = None


@dataclass(frozen=True, slots=True)
class OverflowIncident:
    sdk_session_id: str
    generation: int
    fence_token: int
    first_lost_inbox_seq: int
    first_lost_sdk_receive_seq: int | None
    lost_count: int
    observed_at: float


@dataclass(frozen=True, slots=True)
class AdaptedEvent:
    sdk_session_id: str
    generation: int
    fence_token: int
    inbox_seq: int
    source: str
    raw_type: str
    raw_payload: dict[str, Any]
    reducer_hash: str
    persistence_class: Literal["durable", "ephemeral", "internal"]
    received_at: float
    schema_version: int = 1
    thread_id: str | None = None
    sdk_timestamp: float | None = None
    sdk_receive_seq: int | None = None
    event_id: str | None = None
    internal_event_id: str | None = None
    ephemeral: bool | None = None
    parent_id: str | None = None
    agent_id: str | None = None
    message_id: str | None = None
    turn_id: str | None = None
    interaction_id: str | None = None
    task_id: str | None = None
    tool_call_id: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class RenderIntent:
    id: str
    session_id: str
    logical_seq: int
    lane: str
    coalesce_key: str | None
    idempotency_key: str
    payload: dict[str, Any]
    finalized: bool
