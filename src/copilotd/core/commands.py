from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol


class CDCommandError(Exception):
    code: str = "CD-000-000"

    def __init__(self, message: str = "") -> None:
        super().__init__(message)

    @property
    def message(self) -> str:
        return str(self)


class CDScopeError(CDCommandError):
    code = "CD-SCOPE-001"


class CDProjectError(CDCommandError):
    code = "CD-PROJECT-001"


class CDPathError(CDCommandError):
    code = "CD-PATH-001"


class CDSessionError(CDCommandError):
    pass


class CDSessionNotFoundError(CDSessionError):
    code = "CD-SESSION-001"


class CDSessionStateError(CDSessionError):
    code = "CD-SESSION-002"


class CDConflictError(CDCommandError):
    code = "CD-CONFLICT-001"


class CDCapabilityError(CDCommandError):
    code = "CD-CAP-001"


class CDRuntimeError(CDCommandError):
    code = "CD-RUNTIME-001"


class CDInputError(CDCommandError):
    code = "CD-INPUT-001"


class CDQuotaError(CDCommandError):
    code = "CD-QUOTA-001"


class CDDiscordError(CDCommandError):
    code = "CD-DISCORD-001"


class CDResumeError(CDCommandError):
    code = "CD-RESUME-001"


class CDLiveError(CDCommandError):
    code = "CD-LIVE-001"


_ERROR_TYPES: dict[str, type[CDCommandError]] = {
    cls.code: cls
    for cls in (
        CDScopeError,
        CDProjectError,
        CDPathError,
        CDSessionNotFoundError,
        CDSessionStateError,
        CDConflictError,
        CDCapabilityError,
        CDRuntimeError,
        CDInputError,
        CDQuotaError,
        CDDiscordError,
        CDResumeError,
        CDLiveError,
    )
}


@dataclass(frozen=True, slots=True)
class CommandCapability:
    supported: bool
    reason: str | None = None

    @classmethod
    def supported_(cls) -> CommandCapability:
        return cls(True, None)

    @classmethod
    def unsupported(cls, reason: str) -> CommandCapability:
        return cls(False, reason)


@dataclass(frozen=True, slots=True)
class CommandInvocation:
    name: str
    scope: str | None = None
    thread_id: str | None = None
    session_id: str | None = None
    source: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommandResponse:
    content: str
    ephemeral: bool = True
    followup: bool = False


@dataclass(frozen=True, slots=True)
class CommandDispatchOutcome:
    deferred: bool
    followup_used: bool
    response: CommandResponse | None = None
    error: CDCommandError | None = None
    unknown_interaction: bool = False


class UnknownInteractionError(RuntimeError):
    discord_code = 10062

    def __init__(self, message: str = "Unknown interaction") -> None:
        super().__init__(message)


class CommandResponder(Protocol):
    async def defer(self, *, ephemeral: bool = True) -> None: ...

    async def send_inline(self, content: str, *, ephemeral: bool = True) -> None: ...

    async def send_followup(self, content: str, *, ephemeral: bool = True) -> None: ...

    def warn(self, message: str, **fields: Any) -> None: ...


ResponseLike = CommandResponse | str | None
CommandOperation = Callable[[CommandInvocation], ResponseLike | Awaitable[ResponseLike]]
ErrorMapper = Callable[[BaseException], CDCommandError]


@dataclass(frozen=True, slots=True)
class CommandSurfaceDescriptor:
    name: str
    capability: CommandCapability
    description: str | None = None


class SessionNamingAdapter(Protocol):
    async def rename_app_session(self, *, thread_id: str, name: str) -> bool: ...

    async def rename_native_session(self, *, session_id: str, name: str) -> bool: ...


class ModelReasoningSummaryAdapter(Protocol):
    def supports_reasoning_summary(self, model_id: str) -> bool: ...

    async def read_current_model(self, *, session_id: str) -> Mapping[str, Any] | None: ...


class ScheduleOriginAdapter(Protocol):
    def describe_origin(
        self,
        *,
        origin: str,
        schedule_run_id: str | None = None,
    ) -> str: ...


class ProjectConfigAdapter(Protocol):
    async def list_project_env(
        self,
        channel_id: str,
        *,
        reveal: bool = False,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def set_project_env(
        self,
        channel_id: str,
        *,
        name: str,
        value: str,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def remove_project_env(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool: ...

    async def list_mcp_servers(
        self,
        channel_id: str,
        *,
        reveal: bool = False,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def set_mcp_server(
        self,
        channel_id: str,
        *,
        name: str,
        transport: str,
        config: Mapping[str, Any],
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def toggle_mcp_server(
        self,
        channel_id: str,
        *,
        name: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def remove_mcp_server(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool: ...

    async def list_skill_dirs(
        self,
        channel_id: str,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def set_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def toggle_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def remove_skill_dir(
        self,
        channel_id: str,
        *,
        path: str,
        expected_version: int | None = None,
    ) -> bool: ...

    async def list_plugin_dirs(
        self,
        channel_id: str,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def set_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def toggle_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def remove_plugin_dir(
        self,
        channel_id: str,
        *,
        path: str,
        expected_version: int | None = None,
    ) -> bool: ...

    async def list_custom_agents(
        self,
        channel_id: str,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def set_custom_agent(
        self,
        channel_id: str,
        *,
        name: str,
        description: str,
        prompt: str,
        tools: Sequence[str],
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def toggle_custom_agent(
        self,
        channel_id: str,
        *,
        name: str,
        enabled: bool,
        expected_version: int | None = None,
    ) -> Mapping[str, Any]: ...

    async def remove_custom_agent(
        self,
        channel_id: str,
        *,
        name: str,
        expected_version: int | None = None,
    ) -> bool: ...


class OpsSurfaceAdapter(Protocol):
    async def health(self) -> Mapping[str, Any]: ...

    async def diagnostics(self, *, session_id: str | None = None) -> Mapping[str, Any]: ...

    async def debug(self, *, level: str, duration_minutes: int) -> Mapping[str, Any]: ...

    async def log_tail(self, *, correlation_id: str | None = None) -> Mapping[str, Any]: ...

    async def event_dump(self, *, session_id: str | None = None) -> Mapping[str, Any]: ...


class TaskActionAdapter(Protocol):
    async def list_tasks(
        self,
        *,
        session_id: str,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def show_task(
        self,
        *,
        session_id: str,
        task_id: str,
    ) -> Mapping[str, Any]: ...

    async def cancel_task(
        self,
        *,
        session_id: str,
        task_id: str,
    ) -> Mapping[str, Any]: ...

    async def promote_task(
        self,
        *,
        session_id: str,
        task_id: str,
    ) -> Mapping[str, Any]: ...

    async def message_task(
        self,
        *,
        session_id: str,
        task_id: str,
        message: str,
    ) -> Mapping[str, Any]: ...

    async def remove_task(
        self,
        *,
        session_id: str,
        task_id: str,
    ) -> Mapping[str, Any]: ...


class ElicitationAdapter(Protocol):
    async def ask(
        self,
        *,
        question: str,
        choices: Sequence[str] | None = None,
        allow_freeform: bool = True,
    ) -> Mapping[str, Any] | str: ...

    async def oauth(self, *, form: Mapping[str, Any]) -> Mapping[str, Any] | str: ...


class CommandExecutor:
    def __init__(
        self,
        *,
        error_mapper: ErrorMapper | None = None,
        inline_error_limit: int = 1800,
    ) -> None:
        self._error_mapper = error_mapper or _default_error_mapper
        self._inline_error_limit = inline_error_limit

    async def execute(
        self,
        responder: CommandResponder,
        invocation: CommandInvocation,
        operation: CommandOperation,
        *,
        ephemeral: bool = True,
    ) -> CommandDispatchOutcome:
        deferred = False
        followup_used = False
        unknown_interaction = False
        try:
            await responder.defer(ephemeral=ephemeral)
            deferred = True
        except UnknownInteractionError as error:
            unknown_interaction = True
            responder.warn(
                "discord_unknown_interaction_during_defer",
                discord_code=error.discord_code,
                command=invocation.name,
            )
        try:
            result = operation(invocation)
            if inspect.isawaitable(result):
                result = await result
        except CDCommandError as error:
            response = CommandResponse(
                content=_bounded_error_text(error, self._inline_error_limit),
                ephemeral=True,
                followup=unknown_interaction,
            )
            await self._deliver(responder, response, force_followup=unknown_interaction)
            followup_used = response.followup or unknown_interaction
            return CommandDispatchOutcome(
                deferred=deferred,
                followup_used=followup_used,
                response=response,
                error=error,
                unknown_interaction=unknown_interaction,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            mapped = self._error_mapper(error)
            response = CommandResponse(
                content=_bounded_error_text(mapped, self._inline_error_limit),
                ephemeral=True,
                followup=unknown_interaction,
            )
            await self._deliver(responder, response, force_followup=unknown_interaction)
            followup_used = response.followup or unknown_interaction
            return CommandDispatchOutcome(
                deferred=deferred,
                followup_used=followup_used,
                response=response,
                error=mapped,
                unknown_interaction=unknown_interaction,
            )

        response = _normalize_response(result)
        if response is None:
            return CommandDispatchOutcome(
                deferred=deferred,
                followup_used=False,
                response=None,
                error=None,
                unknown_interaction=unknown_interaction,
            )
        try:
            await self._deliver(responder, response, force_followup=unknown_interaction)
            followup_used = response.followup or unknown_interaction
        except UnknownInteractionError:
            responder.warn(
                "discord_unknown_interaction_during_response",
                discord_code=10062,
                command=invocation.name,
            )
            await responder.send_followup(response.content, ephemeral=response.ephemeral)
            followup_used = True
        return CommandDispatchOutcome(
            deferred=deferred,
            followup_used=followup_used,
            response=response,
            error=None,
            unknown_interaction=unknown_interaction,
        )

    async def _deliver(
        self,
        responder: CommandResponder,
        response: CommandResponse,
        *,
        force_followup: bool = False,
    ) -> None:
        if response.followup or force_followup:
            await responder.send_followup(response.content, ephemeral=response.ephemeral)
            return
        await responder.send_inline(response.content, ephemeral=response.ephemeral)


def command_error_from_code(code: str, message: str = "") -> CDCommandError:
    error_type = _ERROR_TYPES.get(code)
    if error_type is None:
        raise KeyError(code)
    return error_type(message)


def command_error_code(error: CDCommandError) -> str:
    return error.code


def _normalize_response(result: ResponseLike) -> CommandResponse | None:
    if result is None:
        return None
    if isinstance(result, CommandResponse):
        return result
    return CommandResponse(content=str(result))


def _bounded_error_text(error: CDCommandError, limit: int) -> str:
    text = f"[{error.code}] {error.message or error.__class__.__name__}"
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)] + "…"


def _default_error_mapper(error: BaseException) -> CDCommandError:
    if isinstance(error, CDCommandError):
        return error
    message = str(error) or error.__class__.__name__
    return CDLiveError(message)
