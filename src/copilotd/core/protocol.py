from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from copilotd.core.models import AdaptedEvent
from copilotd.storage.database import Database

REQUEST_COMPLETION_TYPES: dict[str, str] = {
    "permission.requested": "permission.completed",
    "user_input.requested": "user_input.completed",
    "elicitation.requested": "elicitation.completed",
    "exit_plan_mode.requested": "exit_plan_mode.completed",
    "session_limits_exhausted.requested": "session_limits_exhausted.completed",
    "sampling.requested": "sampling.completed",
    "mcp.oauth_required": "mcp.oauth_completed",
    "mcp.headers_refresh_required": "mcp.headers_refresh_completed",
    "external_tool.requested": "external_tool.completed",
    "auto_mode_switch.requested": "auto_mode_switch.completed",
}
COMPLETION_REQUEST_TYPES = {
    completed: requested for requested, completed in REQUEST_COMPLETION_TYPES.items()
}

_RESPONSE_PLANES = {
    "permission.requested": "sdk_handler",
    "external_tool.requested": "sdk_handler",
    "elicitation.requested": "sdk_handler",
    "mcp.oauth_required": "sdk_handler",
    "user_input.requested": "direct_handler",
    "exit_plan_mode.requested": "direct_handler",
    "auto_mode_switch.requested": "direct_handler",
    "session_limits_exhausted.requested": "app_rpc",
    "sampling.requested": "app_rpc",
    "mcp.headers_refresh_required": "app_rpc",
}


@dataclass(frozen=True, slots=True)
class ProtocolResponseClaim:
    attempt_id: str
    sdk_session_id: str
    generation: int
    owner_fence_token: int
    request_id: str
    requested_type: str
    response_plane: str


class ProtocolResponseRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def claim(
        self,
        *,
        sdk_session_id: str,
        generation: int,
        owner_fence_token: int,
        request_id: str,
        response_payload: dict[str, Any] | None,
    ) -> ProtocolResponseClaim | None:
        now = time.time()
        attempt_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{sdk_session_id}:{generation}:protocol:{request_id}",
            )
        )
        encoded = json.dumps(
            response_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        response_digest = hashlib.sha256(encoded.encode()).hexdigest()
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT requested_type, response_plane, response_state,
                       requested_event_id, completed_event_id
                FROM protocol_requests
                WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
                """,
                (sdk_session_id, generation, request_id),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if (
                row is None
                or row["requested_event_id"] is None
                or row["completed_event_id"] is not None
                or row["response_plane"] != "app_rpc"
                or row["response_state"] != "pending"
            ):
                return None
            update = await connection.execute(
                """
                UPDATE protocol_requests
                SET response_state = 'responding', response_attempt_id = ?,
                    response_payload = ?, updated_at = ?
                WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
                  AND response_state = 'pending' AND completed_event_id IS NULL
                """,
                (
                    attempt_id,
                    encoded,
                    now,
                    sdk_session_id,
                    generation,
                    request_id,
                ),
            )
            if update.rowcount != 1:
                await update.close()
                return None
            await update.close()
            await connection.execute(
                """
                INSERT INTO protocol_response_attempts(
                    attempt_id, sdk_session_id, generation, owner_fence_token,
                    request_id, response_plane, response_hash, state, started_at
                ) VALUES (?, ?, ?, ?, ?, 'app_rpc', ?, 'started', ?)
                """,
                (
                    attempt_id,
                    sdk_session_id,
                    generation,
                    owner_fence_token,
                    request_id,
                    response_digest,
                    now,
                ),
            )
        return ProtocolResponseClaim(
            attempt_id=attempt_id,
            sdk_session_id=sdk_session_id,
            generation=generation,
            owner_fence_token=owner_fence_token,
            request_id=request_id,
            requested_type=str(row["requested_type"]),
            response_plane="app_rpc",
        )

    async def settle(
        self,
        claim: ProtocolResponseClaim,
        *,
        state: str,
        error_code: str | None = None,
    ) -> None:
        if state not in {"confirmed", "rejected", "unknown", "unsupported"}:
            raise ValueError(f"invalid protocol response state: {state}")
        now = time.time()
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE protocol_response_attempts
                SET state = ?, error_code = ?, settled_at = ?
                WHERE attempt_id = ? AND state = 'started'
                """,
                (state, error_code, now, claim.attempt_id),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise RuntimeError("protocol response attempt was already settled")
            await cursor.close()
            await connection.execute(
                """
                UPDATE protocol_requests
                SET response_state = ?, responded_at = ?, updated_at = ?
                WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
                  AND response_attempt_id = ? AND response_state = 'responding'
                """,
                (
                    state,
                    now,
                    now,
                    claim.sdk_session_id,
                    claim.generation,
                    claim.request_id,
                    claim.attempt_id,
                ),
            )

    async def mark_unsupported(
        self,
        *,
        sdk_session_id: str,
        generation: int,
        request_id: str,
        reason: str,
    ) -> None:
        await self._database.execute(
            """
            UPDATE protocol_requests
            SET response_state = 'unsupported', response_payload = ?, updated_at = ?
            WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
              AND response_state = 'pending'
            """,
            (
                json.dumps({"reason": reason}, sort_keys=True),
                time.time(),
                sdk_session_id,
                generation,
                request_id,
            ),
        )


class ProtocolResponder(Protocol):
    async def respond_session_limits(
        self,
        session: Any,
        request_id: str,
    ) -> bool: ...

    async def respond_sampling(
        self,
        session: Any,
        request_id: str,
        response: dict[str, Any] | None,
    ) -> bool: ...

    async def respond_mcp_headers(
        self,
        session: Any,
        request_id: str,
        headers: dict[str, str] | None,
    ) -> bool: ...


async def apply_protocol_event(
    connection: Any,
    event: AdaptedEvent,
    data: dict[str, Any],
    *,
    now: float,
) -> bool:
    request_id = data.get("requestId")
    if request_id is None:
        return False
    raw_type = event.raw_type
    is_requested = raw_type in REQUEST_COMPLETION_TYPES
    is_completed = raw_type in COMPLETION_REQUEST_TYPES
    if not is_requested and not is_completed:
        return False
    request_id_text = str(request_id)
    cursor = await connection.execute(
        """
        SELECT * FROM protocol_requests
        WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
        """,
        (event.sdk_session_id, event.generation, request_id_text),
    )
    current = await cursor.fetchone()
    await cursor.close()
    encoded = json.dumps(data, ensure_ascii=False, sort_keys=True)
    if is_requested:
        response_plane = _RESPONSE_PLANES.get(raw_type, "journal")
        initial_response_state = (
            "pending"
            if response_plane == "app_rpc"
            else "delegated"
            if response_plane in {"sdk_handler", "direct_handler"}
            else "not_applicable"
        )
        if current is None:
            await connection.execute(
                """
                INSERT INTO protocol_requests(
                    sdk_session_id, generation, request_id,
                    requested_type, requested_event_id, requested_payload,
                    requested_at, wire_state, response_plane,
                    response_state, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?)
                """,
                (
                    event.sdk_session_id,
                    event.generation,
                    request_id_text,
                    raw_type,
                    event.event_id,
                    encoded,
                    now,
                    response_plane,
                    initial_response_state,
                    now,
                ),
            )
        else:
            wire_state = "paired" if current["completed_event_id"] is not None else "requested"
            response_state = (
                "completed"
                if current["completed_event_id"] is not None
                and current["response_state"] in {"pending", "not_applicable", "unsupported"}
                else current["response_state"]
            )
            await connection.execute(
                """
                UPDATE protocol_requests
                SET requested_type = COALESCE(requested_type, ?),
                    requested_event_id = COALESCE(requested_event_id, ?),
                    requested_payload = COALESCE(requested_payload, ?),
                    requested_at = COALESCE(requested_at, ?),
                    wire_state = ?, response_plane = ?,
                    response_state = ?, updated_at = ?
                WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
                """,
                (
                    raw_type,
                    event.event_id,
                    encoded,
                    now,
                    wire_state,
                    response_plane,
                    response_state,
                    now,
                    event.sdk_session_id,
                    event.generation,
                    request_id_text,
                ),
            )
        return True

    requested_type = COMPLETION_REQUEST_TYPES[raw_type]
    response_plane = _RESPONSE_PLANES.get(requested_type, "journal")
    if current is None:
        await connection.execute(
            """
            INSERT INTO protocol_requests(
                sdk_session_id, generation, request_id,
                completed_type, completed_event_id, completed_payload,
                completed_at, wire_state, response_plane,
                response_state, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'completed_before_requested', ?,
                      'completed', ?)
            """,
            (
                event.sdk_session_id,
                event.generation,
                request_id_text,
                raw_type,
                event.event_id,
                encoded,
                now,
                response_plane,
                now,
            ),
        )
    else:
        response_state = (
            "completed"
            if current["response_state"]
            in {
                "pending",
                "not_applicable",
                "unsupported",
            }
            else current["response_state"]
        )
        await connection.execute(
            """
            UPDATE protocol_requests
            SET completed_type = COALESCE(completed_type, ?),
                completed_event_id = COALESCE(completed_event_id, ?),
                completed_payload = COALESCE(completed_payload, ?),
                completed_at = COALESCE(completed_at, ?),
                wire_state = CASE
                    WHEN requested_event_id IS NULL
                    THEN 'completed_before_requested' ELSE 'paired'
                END,
                response_state = ?, updated_at = ?
            WHERE sdk_session_id = ? AND generation = ? AND request_id = ?
            """,
            (
                raw_type,
                event.event_id,
                encoded,
                now,
                response_state,
                now,
                event.sdk_session_id,
                event.generation,
                request_id_text,
            ),
        )
    return True
