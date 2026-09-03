from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from copilotd.core.inbox import ReducerInbox
from copilotd.core.volatile_content import (
    VolatileContentStore,
    opaque_content_key,
)
from copilotd.storage.database import Database

InteractionKind = Literal[
    "user_input",
    "exit_plan_mode",
    "auto_mode_switch",
    "elicitation",
    "mcp_oauth",
]
InteractionResponse = dict[str, Any] | str
InteractionResolution = Literal["resolved", "expired", "invalid"]

MAX_ELICITATION_FIELDS = 25
MAX_ELICITATION_ARRAY_ITEMS = 25
MAX_SCHEMA_DEPTH = 2


class InteractionValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class InteractionScope:
    sdk_session_id: str
    runtime_generation: int
    owner_fence_token: int
    thread_id: str


@dataclass(frozen=True, slots=True)
class ElicitationField:
    name: str
    value_type: Literal["string", "number", "integer", "boolean", "array"]
    required: bool
    title: str
    description: str | None
    enum: tuple[str | float | bool, ...]
    default: str | float | bool | tuple[str, ...] | None
    minimum: float | None
    maximum: float | None
    min_length: int | None
    max_length: int | None
    min_items: int | None
    max_items: int | None
    item_enum: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ElicitationForm:
    fields: tuple[ElicitationField, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "fields": [
                {
                    "name": field.name,
                    "value_type": field.value_type,
                    "required": field.required,
                    "title": field.title,
                    "description": field.description,
                    "enum": list(field.enum),
                    "default": (
                        list(field.default) if isinstance(field.default, tuple) else field.default
                    ),
                    "minimum": field.minimum,
                    "maximum": field.maximum,
                    "min_length": field.min_length,
                    "max_length": field.max_length,
                    "min_items": field.min_items,
                    "max_items": field.max_items,
                    "item_enum": list(field.item_enum),
                }
                for field in self.fields
            ]
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ElicitationForm:
        raw_fields = payload.get("fields")
        if not isinstance(raw_fields, list):
            raise InteractionValidationError("invalid persisted elicitation form")
        fields: list[ElicitationField] = []
        for raw in raw_fields:
            if not isinstance(raw, Mapping):
                raise InteractionValidationError("invalid persisted elicitation field")
            default = raw.get("default")
            if isinstance(default, list):
                default = tuple(str(item) for item in default)
            fields.append(
                ElicitationField(
                    name=str(raw["name"]),
                    value_type=cast(Any, str(raw["value_type"])),
                    required=bool(raw["required"]),
                    title=str(raw["title"]),
                    description=(
                        None if raw.get("description") is None else str(raw["description"])
                    ),
                    enum=tuple(cast(Any, raw.get("enum", []))),
                    default=cast(Any, default),
                    minimum=_optional_float(raw.get("minimum")),
                    maximum=_optional_float(raw.get("maximum")),
                    min_length=_optional_int(raw.get("min_length")),
                    max_length=_optional_int(raw.get("max_length")),
                    min_items=_optional_int(raw.get("min_items")),
                    max_items=_optional_int(raw.get("max_items")),
                    item_enum=tuple(str(item) for item in raw.get("item_enum", [])),
                )
            )
        return cls(tuple(fields))

    def validate(self, content: Mapping[str, Any]) -> dict[str, Any]:
        fields = {field.name: field for field in self.fields}
        unknown = sorted(set(content) - set(fields))
        if unknown:
            raise InteractionValidationError(
                "elicitation response contains unknown fields: " + ", ".join(unknown)
            )
        validated: dict[str, Any] = {}
        for field in self.fields:
            if field.name not in content:
                if field.required:
                    raise InteractionValidationError(f"elicitation field is required: {field.name}")
                continue
            value = content[field.name]
            validated[field.name] = _validate_field_value(field, value)
        return validated


@dataclass(frozen=True, slots=True)
class DiscordInteractionPlan:
    kind: InteractionKind
    interaction_id: str
    choices: tuple[str, ...]
    use_buttons: bool
    use_select: bool
    allow_freeform: bool
    form: ElicitationForm | None
    form_uses_json_fallback: bool
    allow_decline: bool
    allow_cancel: bool


class DiscordInteractionAdapter:
    BUTTON_LIMIT = 5
    SELECT_LIMIT = 25
    MODAL_FIELD_LIMIT = 5

    @classmethod
    def plan(cls, payload: Mapping[str, Any]) -> DiscordInteractionPlan:
        kind = cast(InteractionKind, str(payload["kind"]))
        interaction_id = str(payload["interaction_id"])
        choices_value = payload.get("choices", [])
        choices = (
            tuple(str(item) for item in choices_value) if isinstance(choices_value, list) else ()
        )
        form = None
        if isinstance(payload.get("form"), Mapping):
            form = ElicitationForm.from_dict(cast(Mapping[str, Any], payload["form"]))
        return DiscordInteractionPlan(
            kind=kind,
            interaction_id=interaction_id,
            choices=choices,
            use_buttons=0 < len(choices) <= cls.BUTTON_LIMIT,
            use_select=cls.BUTTON_LIMIT < len(choices) <= cls.SELECT_LIMIT,
            allow_freeform=bool(payload.get("allowFreeform")) or len(choices) > cls.SELECT_LIMIT,
            form=form,
            form_uses_json_fallback=(form is not None and len(form.fields) > cls.MODAL_FIELD_LIMIT),
            allow_decline=kind == "elicitation",
            allow_cancel=kind in {"elicitation", "mcp_oauth"},
        )


class InteractionGateway:
    """Owns durable, fenced, exactly-once settlement for SDK interaction handlers."""

    def __init__(
        self,
        *,
        database: Database,
        inbox: ReducerInbox,
        scope: InteractionScope,
        timeout_seconds: float,
        content_store: VolatileContentStore | None = None,
    ) -> None:
        self._database = database
        self._inbox = inbox
        self._scope = scope
        self._timeout_seconds = timeout_seconds
        self._content_store = content_store or database.content_store
        self._futures: dict[str, asyncio.Future[InteractionResponse]] = {}

    async def request(
        self,
        kind: InteractionKind,
        request: Mapping[str, Any],
        *,
        protocol_request_id: str | None = None,
        response_plane: str = "direct_handler",
        automatic_response: Awaitable[Mapping[str, Any]] | None = None,
    ) -> InteractionResponse:
        interaction_id = str(uuid.uuid4())
        now = time.time()
        expires_at = now + self._timeout_seconds
        form: ElicitationForm | None = None
        validation_error: str | None = None
        if kind == "elicitation":
            schema = request.get("requestedSchema")
            try:
                form = parse_elicitation_schema(schema)
            except InteractionValidationError as error:
                validation_error = str(error)

        payload: dict[str, Any] = {
            "interaction_id": interaction_id,
            "thread_id": self._scope.thread_id,
            "kind": kind,
            "state": "pending",
            "expires_at": expires_at,
            "response_plane": response_plane,
        }
        payload.update(_safe_interaction_payload(kind, request))
        if kind == "exit_plan_mode":
            payload["choices"] = list(request.get("actions", []))
        elif kind == "auto_mode_switch":
            payload["choices"] = ["yes", "yes_always", "no"]
            retry_after = request.get("retryAfterSeconds")
            suffix = "" if retry_after is None else f" Retry after {retry_after} seconds."
            payload["question"] = (
                f"Copilot reached an eligible rate limit. Switch to Auto mode?{suffix}"
            )
        elif kind == "mcp_oauth":
            payload["choices"] = ["cancel"]
            payload["question"] = (
                f"MCP server {request.get('serverName', 'unknown')} requires authorization."
            )
        if form is not None:
            payload["form"] = form.to_dict()
        if validation_error is not None:
            payload["schema_error"] = validation_error
        content_key = opaque_content_key(
            "interaction-request",
            self._scope.sdk_session_id,
            interaction_id,
        )
        content_ref = self._content_store.put(payload, key=content_key)

        future: asyncio.Future[InteractionResponse] = asyncio.get_running_loop().create_future()
        self._futures[interaction_id] = future
        try:
            await self._inbox.commit_internal(
                {
                    "type": "copilotd.interaction.requested",
                    "data": {
                        **payload,
                        "protocol_request_id": protocol_request_id,
                        "content_key": content_ref.key,
                        "request_hash": content_ref.sha256,
                        "sensitive_response": kind == "mcp_oauth",
                    },
                },
                internal_event_id=f"interaction:{interaction_id}:requested",
            )
        except BaseException:
            admitted = await self._database.fetchone(
                """
                SELECT 1 FROM pending_interactions
                WHERE interaction_id = ? AND sdk_session_id = ?
                """,
                (interaction_id, self._scope.sdk_session_id),
            )
            if admitted is None:
                self._content_store.delete(content_key)
            self._futures.pop(interaction_id, None)
            raise
        if validation_error is not None:
            await self._settle(
                interaction_id,
                response={"action": "decline"},
                persisted_response={"action": "decline"},
                display_response="Unsupported elicitation schema was declined.",
                state="resolved",
            )
        elif automatic_response is not None:
            if kind != "mcp_oauth":
                raise ValueError("automatic secure responses are only valid for MCP OAuth")
            try:
                secure_response = await asyncio.wait_for(
                    automatic_response,
                    timeout=self._timeout_seconds,
                )
            except TimeoutError:
                secure_response = {"kind": "cancelled"}
            await self.respond(
                interaction_id,
                secure_response=secure_response,
            )
        try:
            return await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self._timeout_seconds,
            )
        except TimeoutError:
            fallback = interaction_fallback(kind)
            _claimed, settled_response = await self._settle(
                interaction_id,
                response=fallback,
                persisted_response=_safe_response(kind, fallback),
                display_response="Request timed out.",
                state="expired",
            )
            return settled_response
        except asyncio.CancelledError:
            fallback = interaction_fallback(kind)
            await asyncio.shield(
                self._settle(
                    interaction_id,
                    response=fallback,
                    persisted_response=_safe_response(kind, fallback),
                    display_response="Request cancelled because the handler stopped.",
                    state="expired",
                )
            )
            raise
        finally:
            self._futures.pop(interaction_id, None)

    async def respond(
        self,
        interaction_id: str,
        *,
        selection: int | None = None,
        freeform: str | None = None,
        form_content: Mapping[str, Any] | None = None,
        action: Literal["decline", "cancel"] | None = None,
        secure_response: Mapping[str, Any] | None = None,
    ) -> InteractionResolution:
        row = await self._database.fetchone(
            """
            SELECT kind, expires_at, state, content_key, request_hash,
                   runtime_generation, owner_fence_token
            FROM pending_interactions
            WHERE interaction_id = ? AND sdk_session_id = ?
            """,
            (interaction_id, self._scope.sdk_session_id),
        )
        if row is None:
            return "invalid"
        if row["state"] != "pending":
            self._discard_interaction_content(interaction_id)
            return "expired"
        if (
            int(row["runtime_generation"]) != self._scope.runtime_generation
            or int(row["owner_fence_token"]) != self._scope.owner_fence_token
        ):
            return "expired"
        if float(row["expires_at"]) <= time.time():
            kind = cast(InteractionKind, str(row["kind"]))
            fallback = interaction_fallback(kind)
            await self._settle(
                interaction_id,
                response=fallback,
                persisted_response=_safe_response(kind, fallback),
                display_response="Request timed out.",
                state="expired",
            )
            return "expired"
        future = self._futures.get(interaction_id)
        if future is None or future.done():
            return "expired"
        payload = self._content_store.get(
            row["content_key"],
            expected_hash=row["request_hash"],
        )
        if not isinstance(payload, dict):
            await self._expire_content_unavailable(interaction_id)
            return "expired"
        kind = cast(InteractionKind, str(row["kind"]))
        response: InteractionResponse
        persisted_response: InteractionResponse
        display_response: str

        if kind == "elicitation":
            if action is not None:
                response = {"action": action}
                persisted_response = response
                display_response = f"Form {action}led." if action == "cancel" else "Form declined."
            elif form_content is not None and isinstance(payload.get("form"), Mapping):
                form = ElicitationForm.from_dict(cast(Mapping[str, Any], payload["form"]))
                try:
                    validated = form.validate(form_content)
                except InteractionValidationError:
                    return "invalid"
                response = {"action": "accept", "content": validated}
                persisted_response = response
                display_response = "Form submitted."
            else:
                return "invalid"
        elif kind == "mcp_oauth":
            if secure_response is not None:
                try:
                    response = _validate_oauth_response(secure_response)
                except InteractionValidationError:
                    return "invalid"
            elif action == "cancel" or _selected_choice(payload, selection) == "cancel":
                response = {"kind": "cancelled"}
            else:
                return "invalid"
            persisted_response = _safe_response(kind, response)
            display_response = (
                "Authorization supplied securely."
                if cast(dict[str, Any], response).get("kind") == "token"
                else "Authorization cancelled."
            )
        elif freeform is not None:
            answer = freeform.strip()
            choices = [str(choice) for choice in payload.get("choices", [])]
            choice_fallback = len(choices) > 25 and answer in choices
            if (
                not answer
                or (
                    kind == "user_input"
                    and not payload.get("allowFreeform")
                    and not choice_fallback
                )
                or (kind == "exit_plan_mode" and answer not in choices)
                or kind not in {"user_input", "exit_plan_mode"}
            ):
                return "invalid"
            if kind == "user_input":
                response = {
                    "answer": answer,
                    "wasFreeform": not choice_fallback,
                }
            else:
                response = {"approved": True, "selectedAction": answer}
            persisted_response = response
            display_response = answer
        else:
            answer = _selected_choice(payload, selection)
            if answer is None:
                return "invalid"
            if kind == "user_input":
                response = {"answer": answer, "wasFreeform": False}
            elif kind == "exit_plan_mode":
                if answer not in payload.get("actions", []):
                    return "invalid"
                response = {"approved": True, "selectedAction": answer}
            elif kind == "auto_mode_switch":
                if answer not in {"yes", "yes_always", "no"}:
                    return "invalid"
                response = answer
            else:
                return "invalid"
            persisted_response = response
            display_response = answer

        claimed, _settled_response = await self._settle(
            interaction_id,
            response=response,
            persisted_response=persisted_response,
            display_response=display_response,
            state="resolved",
        )
        return "resolved" if claimed else "expired"

    async def _expire_content_unavailable(self, interaction_id: str) -> None:
        now = time.time()
        changed = await self._database.execute_count(
            """
            UPDATE pending_interactions
            SET state = 'content_unavailable', updated_at = ?
            WHERE interaction_id = ? AND sdk_session_id = ?
              AND runtime_generation = ? AND owner_fence_token = ?
              AND state = 'pending'
            """,
            (
                now,
                interaction_id,
                self._scope.sdk_session_id,
                self._scope.runtime_generation,
                self._scope.owner_fence_token,
            ),
        )
        if changed:
            await self._database.execute(
                """
                UPDATE liveness_leases
                SET state = 'released', refreshed_at = ?, released_at = ?
                WHERE sdk_session_id = ? AND lease_id = ? AND state = 'active'
                """,
                (
                    now,
                    now,
                    self._scope.sdk_session_id,
                    f"interaction:{interaction_id}",
                ),
            )
        future = self._futures.get(interaction_id)
        if future is not None and not future.done():
            row = await self._database.fetchone(
                "SELECT kind FROM pending_interactions WHERE interaction_id = ?",
                (interaction_id,),
            )
            kind = "user_input" if row is None else str(row["kind"])
            future.set_result(interaction_fallback(kind))
        self._discard_interaction_content(interaction_id)

    async def cancel_pending(self, *, reason: str) -> int:
        rows = await self._database.fetchall(
            """
            SELECT interaction_id, kind FROM pending_interactions
            WHERE sdk_session_id = ? AND runtime_generation = ?
              AND owner_fence_token = ? AND state = 'pending'
            """,
            (
                self._scope.sdk_session_id,
                self._scope.runtime_generation,
                self._scope.owner_fence_token,
            ),
        )
        resolved = 0
        for row in rows:
            kind = cast(InteractionKind, str(row["kind"]))
            fallback = interaction_fallback(kind)
            claimed, _ = await self._settle(
                str(row["interaction_id"]),
                response=fallback,
                persisted_response=_safe_response(kind, fallback),
                display_response=reason,
                state="expired",
            )
            resolved += int(claimed)
        return resolved

    async def _settle(
        self,
        interaction_id: str,
        *,
        response: InteractionResponse,
        persisted_response: InteractionResponse,
        display_response: str,
        state: Literal["resolved", "expired"],
    ) -> tuple[bool, InteractionResponse]:
        future = self._futures.get(interaction_id)
        response_key = opaque_content_key(
            "interaction-response",
            self._scope.sdk_session_id,
            interaction_id,
        )
        target_mode = interaction_target_mode(response) if state == "resolved" else None
        now = time.time()
        response_attempt_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{self._scope.sdk_session_id}:interaction:{interaction_id}",
            )
        )
        with self._content_store.transaction():
            async with self._database.transaction() as connection:
                current_cursor = await connection.execute(
                    """
                    SELECT kind, state, expires_at, response_hash,
                           runtime_generation, owner_fence_token
                    FROM pending_interactions
                    WHERE interaction_id = ? AND sdk_session_id = ?
                    """,
                    (interaction_id, self._scope.sdk_session_id),
                )
                current = await current_cursor.fetchone()
                await current_cursor.close()
                claimed = bool(
                    current is not None
                    and current["state"] == "pending"
                    and int(current["runtime_generation"]) == self._scope.runtime_generation
                    and int(current["owner_fence_token"]) == self._scope.owner_fence_token
                    and (state != "resolved" or float(current["expires_at"]) > now)
                )
                if claimed:
                    response_ref = self._content_store.put(
                        persisted_response,
                        key=response_key,
                    )
                    cursor = await connection.execute(
                        """
                        UPDATE pending_interactions
                        SET state = ?, response = NULL, response_hash = ?, target_mode = ?,
                            response_attempt_id = ?, updated_at = ?
                        WHERE interaction_id = ? AND sdk_session_id = ?
                          AND runtime_generation = ? AND owner_fence_token = ?
                          AND state = 'pending'
                        """,
                        (
                            state,
                            response_ref.sha256,
                            target_mode,
                            response_attempt_id,
                            now,
                            interaction_id,
                            self._scope.sdk_session_id,
                            self._scope.runtime_generation,
                            self._scope.owner_fence_token,
                        ),
                    )
                    if cursor.rowcount != 1:
                        await cursor.close()
                        raise RuntimeError("interaction settlement claim changed in transaction")
                    await cursor.close()
                    await connection.execute(
                        """
                        UPDATE liveness_leases
                        SET state = 'released', refreshed_at = ?, released_at = ?
                        WHERE sdk_session_id = ? AND lease_id = ?
                          AND runtime_generation = ? AND owner_fence_token = ?
                          AND state = 'active'
                        """,
                        (
                            now,
                            now,
                            self._scope.sdk_session_id,
                            f"interaction:{interaction_id}",
                            self._scope.runtime_generation,
                            self._scope.owner_fence_token,
                        ),
                    )
                row_cursor = await connection.execute(
                    """
                    SELECT kind, response_hash, state FROM pending_interactions
                    WHERE interaction_id = ? AND sdk_session_id = ?
                    """,
                    (interaction_id, self._scope.sdk_session_id),
                )
                row = await row_cursor.fetchone()
                await row_cursor.close()

        settled_response = response
        kind = "interaction"
        if row is not None:
            kind = str(row["kind"])
        if not claimed:
            recovered = self._content_store.get(
                response_key,
                expected_hash=None if row is None else row["response_hash"],
            )
            settled_response = (
                cast(InteractionResponse, recovered)
                if recovered is not None
                else _unclaimed_interaction_fallback(kind)
            )
        if claimed:
            await self._inbox.commit_internal(
                {
                    "type": f"copilotd.interaction.{state}",
                    "data": {
                        "interaction_id": interaction_id,
                        "kind": kind,
                        "thread_id": self._scope.thread_id,
                        "state": state,
                        "response": persisted_response,
                        "target_mode": target_mode,
                        "display_response": display_response,
                        "response_attempt_id": response_attempt_id,
                    },
                },
                internal_event_id=f"interaction:{interaction_id}:{state}",
            )
            if future is not None and not future.done():
                future.set_result(response)
        elif future is not None and not future.done():
            future.set_result(settled_response)
        if claimed or (row is not None and str(row["state"]) != "pending"):
            self._discard_interaction_content(interaction_id)
        return claimed, settled_response

    def _discard_interaction_content(self, interaction_id: str) -> None:
        for scope in ("interaction-request", "interaction-response"):
            self._content_store.delete(
                opaque_content_key(
                    scope,
                    self._scope.sdk_session_id,
                    interaction_id,
                )
            )


def interaction_target_mode(response: Any) -> str | None:
    if not isinstance(response, dict) or response.get("approved") is not True:
        return None
    selected_action = response.get("selectedAction")
    if selected_action in {"interactive", "plan", "autopilot"}:
        return str(selected_action)
    return None


def interaction_fallback(kind: str) -> InteractionResponse:
    if kind == "user_input":
        return {
            "answer": "No response was received.",
            "wasFreeform": True,
        }
    if kind == "exit_plan_mode":
        return {"approved": False}
    if kind == "auto_mode_switch":
        return "no"
    if kind == "elicitation":
        return {"action": "cancel"}
    if kind == "mcp_oauth":
        return {"kind": "cancelled"}
    raise ValueError(f"unsupported interaction kind: {kind}")


def _unclaimed_interaction_fallback(kind: str) -> InteractionResponse:
    if kind in {
        "user_input",
        "exit_plan_mode",
        "auto_mode_switch",
        "elicitation",
        "mcp_oauth",
    }:
        return interaction_fallback(kind)
    return {
        "answer": "No committed response was available.",
        "wasFreeform": True,
    }


def parse_elicitation_schema(value: Any) -> ElicitationForm:
    if not isinstance(value, Mapping):
        raise InteractionValidationError("elicitation schema must be an object")
    if value.get("type") != "object":
        raise InteractionValidationError("elicitation schema root must have type object")
    if value.get("additionalProperties") not in {None, False}:
        raise InteractionValidationError("elicitation schema cannot allow unknown fields")
    properties = value.get("properties")
    if not isinstance(properties, Mapping):
        raise InteractionValidationError("elicitation schema properties must be an object")
    if len(properties) > MAX_ELICITATION_FIELDS:
        raise InteractionValidationError(
            f"elicitation schema exceeds {MAX_ELICITATION_FIELDS} fields"
        )
    required_value = value.get("required", [])
    if not isinstance(required_value, list) or not all(
        isinstance(item, str) for item in required_value
    ):
        raise InteractionValidationError("elicitation schema required must be strings")
    required = set(required_value)
    if not required.issubset(properties):
        raise InteractionValidationError("elicitation schema requires an unknown field")

    fields: list[ElicitationField] = []
    for name, raw_field in properties.items():
        if not isinstance(name, str) or not name or len(name) > 100:
            raise InteractionValidationError("elicitation field names must be 1-100 chars")
        if not isinstance(raw_field, Mapping):
            raise InteractionValidationError(f"elicitation field {name} must be an object")
        fields.append(_parse_elicitation_field(name, raw_field, name in required))
    return ElicitationForm(tuple(fields))


def _parse_elicitation_field(
    name: str,
    payload: Mapping[str, Any],
    required: bool,
) -> ElicitationField:
    field_type = payload.get("type")
    if field_type not in {"string", "number", "integer", "boolean", "array"}:
        raise InteractionValidationError(
            f"elicitation field {name} has unsupported type {field_type!r}"
        )
    enum = _primitive_enum(payload.get("enum"), name)
    item_enum: tuple[str, ...] = ()
    min_items = _nonnegative_int(payload.get("minItems"), f"{name}.minItems")
    max_items = _nonnegative_int(payload.get("maxItems"), f"{name}.maxItems")
    if field_type == "array":
        items = payload.get("items")
        if not isinstance(items, Mapping) or items.get("type") != "string":
            raise InteractionValidationError(f"elicitation array {name} must contain strings")
        if any(key in items for key in ("properties", "items", "oneOf", "anyOf", "allOf", "$ref")):
            raise InteractionValidationError(
                f"elicitation array {name} cannot contain nested schemas"
            )
        item_enum_value = items.get("enum")
        if item_enum_value is not None:
            if not isinstance(item_enum_value, list) or not all(
                isinstance(item, str) for item in item_enum_value
            ):
                raise InteractionValidationError(
                    f"elicitation array {name} enum must contain strings"
                )
            item_enum = tuple(item_enum_value)
        if max_items is None:
            max_items = MAX_ELICITATION_ARRAY_ITEMS
        if max_items > MAX_ELICITATION_ARRAY_ITEMS:
            raise InteractionValidationError(
                f"elicitation array {name} exceeds {MAX_ELICITATION_ARRAY_ITEMS} items"
            )
    elif any(key in payload for key in ("properties", "items", "oneOf", "anyOf", "allOf", "$ref")):
        raise InteractionValidationError(f"elicitation field {name} contains a nested schema")

    minimum = _optional_float(payload.get("minimum"))
    maximum = _optional_float(payload.get("maximum"))
    min_length = _nonnegative_int(payload.get("minLength"), f"{name}.minLength")
    max_length = _nonnegative_int(payload.get("maxLength"), f"{name}.maxLength")
    if minimum is not None and maximum is not None and minimum > maximum:
        raise InteractionValidationError(f"elicitation field {name} has invalid bounds")
    if min_length is not None and max_length is not None and min_length > max_length:
        raise InteractionValidationError(f"elicitation field {name} has invalid length")
    if min_items is not None and max_items is not None and min_items > max_items:
        raise InteractionValidationError(f"elicitation field {name} has invalid item bounds")

    default = payload.get("default")
    field = ElicitationField(
        name=name,
        value_type=cast(Any, field_type),
        required=required,
        title=str(payload.get("title") or name),
        description=(None if payload.get("description") is None else str(payload["description"])),
        enum=enum,
        default=cast(Any, tuple(default) if isinstance(default, list) else default),
        minimum=minimum,
        maximum=maximum,
        min_length=min_length,
        max_length=max_length,
        min_items=min_items,
        max_items=max_items,
        item_enum=item_enum,
    )
    if default is not None:
        _validate_field_value(field, default)
    return field


def _validate_field_value(field: ElicitationField, value: Any) -> Any:
    if field.value_type == "string":
        if not isinstance(value, str):
            raise InteractionValidationError(f"{field.name} must be a string")
        if field.min_length is not None and len(value) < field.min_length:
            raise InteractionValidationError(f"{field.name} is too short")
        if field.max_length is not None and len(value) > field.max_length:
            raise InteractionValidationError(f"{field.name} is too long")
        validated: Any = value
    elif field.value_type in {"number", "integer"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InteractionValidationError(f"{field.name} must be numeric")
        if field.value_type == "integer" and not isinstance(value, int):
            raise InteractionValidationError(f"{field.name} must be an integer")
        numeric = float(value)
        if field.minimum is not None and numeric < field.minimum:
            raise InteractionValidationError(f"{field.name} is below its minimum")
        if field.maximum is not None and numeric > field.maximum:
            raise InteractionValidationError(f"{field.name} is above its maximum")
        validated = value
    elif field.value_type == "boolean":
        if not isinstance(value, bool):
            raise InteractionValidationError(f"{field.name} must be boolean")
        validated = value
    else:
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise InteractionValidationError(f"{field.name} must be an array of strings")
        if field.min_items is not None and len(value) < field.min_items:
            raise InteractionValidationError(f"{field.name} has too few items")
        if field.max_items is not None and len(value) > field.max_items:
            raise InteractionValidationError(f"{field.name} has too many items")
        if field.item_enum and any(item not in field.item_enum for item in value):
            raise InteractionValidationError(f"{field.name} contains an invalid item")
        validated = list(value)
    if field.enum and validated not in field.enum:
        raise InteractionValidationError(f"{field.name} is not an allowed value")
    return validated


def _safe_interaction_payload(
    kind: InteractionKind,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _jsonable(request)
    if not isinstance(payload, dict):
        return {}
    if kind != "mcp_oauth":
        return payload
    safe = dict(payload)
    static = safe.get("staticClientConfig")
    if isinstance(static, dict) and "clientSecret" in static:
        safe["staticClientConfig"] = {
            **static,
            "clientSecret": "[redacted]",
        }
    http_response = safe.get("httpResponse")
    if isinstance(http_response, dict) and "body" in http_response:
        safe["httpResponse"] = {
            **http_response,
            "body": "[redacted]",
        }
    return safe


def _safe_response(
    kind: str,
    response: InteractionResponse,
) -> InteractionResponse:
    if kind != "mcp_oauth" or not isinstance(response, Mapping):
        return response
    if response.get("kind") == "token":
        return {
            "kind": "token",
            "tokenType": response.get("tokenType"),
            "expiresIn": response.get("expiresIn"),
            "accessToken": "[redacted]",
        }
    return {"kind": "cancelled"}


def _validate_oauth_response(value: Mapping[str, Any]) -> dict[str, Any]:
    kind = value.get("kind")
    if kind == "cancelled":
        return {"kind": "cancelled"}
    if kind != "token":
        raise InteractionValidationError("unsupported MCP OAuth response")
    token = value.get("accessToken")
    if not isinstance(token, str) or not token:
        raise InteractionValidationError("MCP OAuth token is missing")
    result: dict[str, Any] = {"kind": "token", "accessToken": token}
    if value.get("tokenType") is not None:
        result["tokenType"] = str(value["tokenType"])
    if value.get("expiresIn") is not None:
        expires = value["expiresIn"]
        if isinstance(expires, bool) or not isinstance(expires, int) or expires <= 0:
            raise InteractionValidationError("MCP OAuth expiry must be positive")
        result["expiresIn"] = expires
    return result


def _selected_choice(payload: Mapping[str, Any], selection: int | None) -> str | None:
    choices = payload.get("choices")
    if (
        not isinstance(choices, list)
        or not isinstance(selection, int)
        or isinstance(selection, bool)
        or not 0 <= selection < len(choices)
    ):
        return None
    return str(choices[selection])


def _primitive_enum(value: Any, name: str) -> tuple[str | float | bool, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(
        isinstance(item, (str, int, float, bool)) for item in value
    ):
        raise InteractionValidationError(
            f"elicitation field {name} enum contains unsupported values"
        )
    return tuple(cast(Any, value))


def _nonnegative_int(value: Any, name: str) -> int | None:
    parsed = _optional_int(value)
    if parsed is not None and parsed < 0:
        raise InteractionValidationError(f"{name} must be non-negative")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise InteractionValidationError("expected an integer")
    return value


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InteractionValidationError("expected a number")
    return float(value)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def response_hash(response: Mapping[str, Any] | str | None) -> str:
    encoded = json.dumps(
        _jsonable(response),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()
