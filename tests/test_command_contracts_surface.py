from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from copilotd.core.commands import (
    CDCapabilityError,
    CDCommandError,
    CDConflictError,
    CDDiscordError,
    CDInputError,
    CDLiveError,
    CDPathError,
    CDProjectError,
    CDQuotaError,
    CDResumeError,
    CDRuntimeError,
    CDScopeError,
    CDSessionNotFoundError,
    CDSessionStateError,
    CommandCapability,
    CommandExecutor,
    CommandInvocation,
    CommandResponse,
    UnknownInteractionError,
    command_error_code,
    command_error_from_code,
    fenced_code_block,
)


@dataclass
class FakeResponder:
    defer_error: Exception | None = None
    inline_error: Exception | None = None
    followup_error: Exception | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.warnings: list[dict[str, object]] = []

    async def defer(self, *, ephemeral: bool = True) -> None:
        self.calls.append(("defer", ephemeral))
        if self.defer_error is not None:
            raise self.defer_error

    async def send_inline(self, content: str, *, ephemeral: bool = True) -> None:
        self.calls.append(("inline", content, ephemeral))
        if self.inline_error is not None:
            raise self.inline_error

    async def send_followup(self, content: str, *, ephemeral: bool = True) -> None:
        self.calls.append(("followup", content, ephemeral))
        if self.followup_error is not None:
            raise self.followup_error

    async def send_file(
        self,
        message: str,
        *,
        content: bytes,
        filename: str,
        ephemeral: bool = True,
    ) -> None:
        self.calls.append(("file", message, content, filename, ephemeral))

    def warn(self, message: str, **fields: object) -> None:
        payload = {"message": message, **fields}
        self.warnings.append(payload)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (CDScopeError("scope"), "CD-SCOPE-001"),
        (CDProjectError("project"), "CD-PROJECT-001"),
        (CDPathError("path"), "CD-PATH-001"),
        (CDSessionNotFoundError("session"), "CD-SESSION-001"),
        (CDSessionStateError("state"), "CD-SESSION-002"),
        (CDConflictError("conflict"), "CD-CONFLICT-001"),
        (CDCapabilityError("cap"), "CD-CAP-001"),
        (CDRuntimeError("runtime"), "CD-RUNTIME-001"),
        (CDInputError("input"), "CD-INPUT-001"),
        (CDQuotaError("quota"), "CD-QUOTA-001"),
        (CDDiscordError("discord"), "CD-DISCORD-001"),
        (CDResumeError("resume"), "CD-RESUME-001"),
        (CDLiveError("live"), "CD-LIVE-001"),
    ],
)
def test_cd_error_code_round_trips(error: CDCommandError, code: str) -> None:
    mapped = command_error_from_code(code, error.message)

    assert command_error_code(error) == code
    assert type(mapped) is type(error)
    assert mapped.message == error.message


@pytest.mark.asyncio
async def test_command_executor_defer_then_inline_send() -> None:
    responder = FakeResponder()
    executor = CommandExecutor()

    outcome = await executor.execute(
        responder,
        CommandInvocation(name="/demo", source="discord"),
        lambda _invocation: CommandResponse("done"),
    )

    assert responder.calls == [("defer", True), ("inline", "done", True)]
    assert outcome.deferred is True
    assert outcome.followup_used is False
    assert outcome.response is not None and outcome.response.content == "done"


@pytest.mark.asyncio
async def test_command_executor_always_acknowledges_empty_success_and_supports_attachment() -> None:
    responder = FakeResponder()
    executor = CommandExecutor()

    none_result = await executor.execute(
        responder,
        CommandInvocation(name="/empty", source="discord"),
        lambda _invocation: None,
    )
    empty_text = await executor.execute(
        responder,
        CommandInvocation(name="/empty-text", source="discord"),
        lambda _invocation: "",
    )
    empty_response = await executor.execute(
        responder,
        CommandInvocation(name="/empty-response", source="discord"),
        lambda _invocation: CommandResponse(""),
    )
    attached = await executor.execute(
        responder,
        CommandInvocation(name="/dump", source="discord"),
        lambda _invocation: CommandResponse(
            "attached",
            attachment=b"redacted",
            filename="diagnostics.json",
        ),
    )

    assert none_result.response is not None
    assert empty_text.response is not None
    assert empty_response.response is not None
    assert {
        none_result.response.content,
        empty_text.response.content,
        empty_response.response.content,
    } == {"Command completed."}
    assert responder.calls.count(("inline", "Command completed.", True)) == 3
    assert ("file", "attached", b"redacted", "diagnostics.json", True) in responder.calls
    assert attached.response is not None and attached.response.filename == "diagnostics.json"


@pytest.mark.asyncio
async def test_command_executor_unknown_interaction_falls_back_to_followup() -> None:
    responder = FakeResponder(defer_error=UnknownInteractionError())
    executor = CommandExecutor()

    outcome = await executor.execute(
        responder,
        CommandInvocation(name="/demo", source="discord"),
        lambda _invocation: CommandResponse("done"),
    )

    assert responder.calls == [("defer", True), ("followup", "done", True)]
    assert responder.warnings[0]["message"] == "discord_unknown_interaction_during_defer"
    assert outcome.deferred is False
    assert outcome.followup_used is True
    assert outcome.unknown_interaction is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation_error",
    [
        CDProjectError("project unavailable"),
        RuntimeError("unexpected failure"),
    ],
)
async def test_command_executor_error_response_expiry_falls_back_to_thread(
    operation_error: BaseException,
) -> None:
    responder = FakeResponder(inline_error=UnknownInteractionError())
    executor = CommandExecutor()

    def fail(_invocation: CommandInvocation) -> None:
        raise operation_error

    outcome = await executor.execute(
        responder,
        CommandInvocation(name="/demo", source="discord"),
        fail,
    )

    assert responder.calls[0] == ("defer", True)
    assert responder.calls[1][0] == "inline"
    assert responder.calls[2][0] == "followup"
    assert responder.calls[2][1] == responder.calls[1][1]
    assert responder.warnings == [
        {
            "message": "discord_unknown_interaction_during_response",
            "discord_code": 10062,
            "command": "/demo",
        }
    ]
    assert outcome.followup_used is True
    assert outcome.unknown_interaction is True
    assert outcome.error is not None


@pytest.mark.asyncio
async def test_command_executor_defers_before_a_delayed_callback() -> None:
    responder = FakeResponder()
    executor = CommandExecutor()
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()

    async def delayed(_invocation: CommandInvocation) -> str:
        callback_started.set()
        await release_callback.wait()
        return "delayed result"

    task = asyncio.create_task(
        executor.execute(
            responder,
            CommandInvocation(name="/delayed", source="discord"),
            delayed,
        )
    )
    await callback_started.wait()

    assert responder.calls == [("defer", True)]
    release_callback.set()
    outcome = await task

    assert responder.calls[-1] == ("inline", "delayed result", True)
    assert outcome.deferred is True


@pytest.mark.asyncio
async def test_command_executor_bounds_inline_error_text() -> None:
    responder = FakeResponder()
    executor = CommandExecutor(inline_error_limit=32)

    outcome = await executor.execute(
        responder,
        CommandInvocation(name="/demo", source="discord"),
        lambda _invocation: (_ for _ in ()).throw(CDProjectError("x" * 80)),
    )

    assert responder.calls[0] == ("defer", True)
    assert responder.calls[1][0] == "inline"
    assert responder.calls[1][1].startswith("[CD-PROJECT-001]")
    assert len(responder.calls[1][1]) <= 32
    assert outcome.error is not None and outcome.error.code == "CD-PROJECT-001"


def test_command_capability_helpers_are_explicit() -> None:
    assert CommandCapability.supported_().supported is True
    unsupported = CommandCapability.unsupported("probe missing")
    assert unsupported.supported is False
    assert unsupported.reason == "probe missing"


def test_fenced_code_block_preserves_nested_backticks() -> None:
    rendered = fenced_code_block('{"snippet": "```python\\npass\\n```"}', language="json")

    assert rendered.startswith("````json\n")
    assert rendered.endswith("\n````")
    assert "```python\\npass\\n```" in rendered
