from pathlib import Path
from uuid import uuid4

import pytest

from copilotd.core.interactions import (
    DiscordInteractionAdapter,
    InteractionValidationError,
    parse_elicitation_schema,
)
from copilotd.core.models import AdaptedEvent
from copilotd.core.protocol import ProtocolResponseRepository
from copilotd.core.reducer import JournalReducer
from copilotd.storage.database import Database


def _protocol_event(
    raw_type: str,
    request_id: str,
    inbox_seq: int,
    **data: object,
) -> AdaptedEvent:
    event_id = str(uuid4())
    return AdaptedEvent(
        sdk_session_id="session-protocol",
        generation=2,
        fence_token=11,
        inbox_seq=inbox_seq,
        source="sdk",
        raw_type=raw_type,
        raw_payload={
            "type": raw_type,
            "data": {"requestId": request_id, **data},
        },
        reducer_hash=f"hash-{event_id}",
        persistence_class="durable",
        received_at=100 + inbox_seq,
        event_id=event_id,
        request_id=request_id,
    )


@pytest.mark.asyncio
async def test_protocol_completed_before_requested_pairs_without_losing_completion(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "protocol-order.sqlite3") as database:
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _protocol_event(
                    "mcp.oauth_completed",
                    "request-1",
                    1,
                    outcome="cancelled",
                ),
                _protocol_event(
                    "mcp.oauth_required",
                    "request-1",
                    2,
                    serverName="server",
                    serverUrl="https://mcp.example.test",
                    reason="initial",
                ),
            ]
        )
        row = await database.fetchone(
            """
            SELECT requested_type, completed_type, wire_state,
                   response_plane, response_state
            FROM protocol_requests
            WHERE request_id = 'request-1'
            """
        )

    assert dict(row) == {
        "requested_type": "mcp.oauth_required",
        "completed_type": "mcp.oauth_completed",
        "wire_state": "paired",
        "response_plane": "sdk_handler",
        "response_state": "completed",
    }


@pytest.mark.asyncio
async def test_app_response_plane_claims_exactly_once(tmp_path: Path) -> None:
    async with Database(tmp_path / "protocol-claim.sqlite3") as database:
        reducer = JournalReducer(database)
        await reducer.persist(
            [
                _protocol_event(
                    "session_limits_exhausted.requested",
                    "request-limits",
                    1,
                    maxAiCredits=5,
                    usedAiCredits=5,
                )
            ]
        )
        responses = ProtocolResponseRepository(database)
        first = await responses.claim(
            sdk_session_id="session-protocol",
            generation=2,
            owner_fence_token=11,
            request_id="request-limits",
            response_payload={"action": "cancel"},
        )
        duplicate = await responses.claim(
            sdk_session_id="session-protocol",
            generation=2,
            owner_fence_token=11,
            request_id="request-limits",
            response_payload={"action": "cancel"},
        )
        assert first is not None
        await responses.settle(first, state="confirmed")
        request = await database.fetchone(
            """
            SELECT response_state, response_attempt_id
            FROM protocol_requests WHERE request_id = 'request-limits'
            """
        )
        attempts = await database.fetchall("SELECT state FROM protocol_response_attempts")

    assert duplicate is None
    assert request["response_state"] == "confirmed"
    assert request["response_attempt_id"] == first.attempt_id
    assert [row["state"] for row in attempts] == ["confirmed"]


def test_elicitation_schema_validation_and_discord_bounds() -> None:
    form = parse_elicitation_schema(
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 2,
                    "maxLength": 20,
                },
                "count": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3,
                },
                "enabled": {"type": "boolean"},
                "tags": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["a", "b"]},
                    "maxItems": 2,
                },
            },
            "required": ["name", "count"],
        }
    )

    assert form.validate(
        {
            "name": "ok",
            "count": 2,
            "enabled": True,
            "tags": ["a"],
        }
    ) == {
        "name": "ok",
        "count": 2,
        "enabled": True,
        "tags": ["a"],
    }
    with pytest.raises(InteractionValidationError, match="required"):
        form.validate({"name": "ok"})
    with pytest.raises(InteractionValidationError, match="invalid item"):
        form.validate({"name": "ok", "count": 2, "tags": ["c"]})

    plan = DiscordInteractionAdapter.plan(
        {
            "kind": "elicitation",
            "interaction_id": "interaction-1",
            "form": form.to_dict(),
        }
    )
    assert plan.form == form
    assert not plan.form_uses_json_fallback
    assert plan.allow_decline
    assert plan.allow_cancel


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "array", "items": {"type": "string"}},
        {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                }
            },
        },
        {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "array",
                    "items": {"type": "object"},
                }
            },
        },
    ],
)
def test_elicitation_schema_declines_unknown_or_nested_shapes(
    schema: dict[str, object],
) -> None:
    with pytest.raises(InteractionValidationError):
        parse_elicitation_schema(schema)
