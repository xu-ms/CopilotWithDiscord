import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from copilot.generated.rpc import (
    PermissionsAllowAllMode,
    SessionMode,
    SlashCommandAgentPromptResult,
    SlashCommandCompletedResult,
    SlashCommandSelectSubcommandOption,
    SlashCommandSelectSubcommandResult,
    SlashCommandTextResult,
)

from copilotd.sdk.bridge import CopilotBridge, PermissionPostureError
from copilotd.sdk.native import (
    NativeCapabilityUnavailable,
    NativeCommandResultKind,
    parse_command_result,
)


@dataclass
class FakePermissionState:
    enabled: bool
    mode: PermissionsAllowAllMode


class FakePermissions:
    def __init__(self, states: list[FakePermissionState]) -> None:
        self._states = iter(states)
        self.set_allow_all_calls = 0
        self.set_approve_all_calls = 0

    async def get_allow_all(self, *, timeout: float) -> FakePermissionState:  # noqa: ASYNC109
        assert timeout == 10
        return next(self._states)

    async def set_allow_all(
        self,
        _request: object,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> None:
        assert timeout == 10
        self.set_allow_all_calls += 1

    async def set_approve_all(
        self,
        _request: object,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> None:
        assert timeout == 10
        self.set_approve_all_calls += 1


@dataclass
class FakeRpc:
    permissions: FakePermissions


@dataclass
class FakeSession:
    rpc: FakeRpc


@pytest.mark.asyncio
async def test_allow_all_is_reconciled_and_confirmed() -> None:
    permissions = FakePermissions(
        [
            FakePermissionState(False, PermissionsAllowAllMode.OFF),
            FakePermissionState(True, PermissionsAllowAllMode.ON),
        ]
    )
    bridge = object.__new__(CopilotBridge)

    posture = await bridge.ensure_allow_all(FakeSession(FakeRpc(permissions)))

    assert posture.enabled
    assert posture.mode == "on"
    assert permissions.set_allow_all_calls == 1
    assert permissions.set_approve_all_calls == 1


@pytest.mark.asyncio
async def test_allow_all_failure_blocks_dispatch() -> None:
    permissions = FakePermissions(
        [
            FakePermissionState(False, PermissionsAllowAllMode.OFF),
            FakePermissionState(False, PermissionsAllowAllMode.OFF),
        ]
    )
    bridge = object.__new__(CopilotBridge)

    with pytest.raises(PermissionPostureError):
        await bridge.ensure_allow_all(FakeSession(FakeRpc(permissions)))


class FakeClient:
    def __init__(self) -> None:
        self.create_kwargs: dict[str, object] = {}
        self.resume_id: str | None = None
        self.resume_kwargs: dict[str, object] = {}

    async def create_session(self, **kwargs: object) -> object:
        self.create_kwargs = kwargs
        return object()

    async def resume_session(self, session_id: str, **kwargs: object) -> object:
        self.resume_id = session_id
        self.resume_kwargs = kwargs
        return object()


class HungStopClient:
    def __init__(self) -> None:
        self.stop_started = asyncio.Event()
        self.force_stop_calls = 0

    async def stop(self) -> None:
        self.stop_started.set()
        await asyncio.Event().wait()

    async def force_stop(self) -> None:
        self.force_stop_calls += 1


@pytest.mark.asyncio
async def test_bridge_preregisters_event_handler_and_forces_runtime_options() -> None:
    client = FakeClient()
    bridge = object.__new__(CopilotBridge)
    bridge._client = client

    def callback(_event: object) -> None:
        return None

    session_config = {
        "mcp_servers": {"local": {"type": "stdio", "command": "node"}},
        "custom_agents": [{"name": "reviewer", "prompt": "Review"}],
        "enable_skills": True,
        "skill_directories": ["/tmp/skills"],
        "plugin_directories": ["/tmp/plugins"],
    }
    await bridge.create_session(
        session_id="session-1",
        working_directory="/tmp/project",
        on_event=callback,
        session_config=session_config,
    )
    await bridge.resume_session(
        session_id="session-1",
        working_directory="/tmp/project",
        on_event=callback,
        continue_pending_work=False,
        session_config=session_config,
    )

    assert client.create_kwargs["on_event"] is callback
    assert client.create_kwargs["manage_schedule_enabled"] is False
    assert client.create_kwargs["streaming"] is True
    assert client.create_kwargs["mcp_servers"] == session_config["mcp_servers"]
    assert client.create_kwargs["custom_agents"] == session_config["custom_agents"]
    assert client.create_kwargs["skill_directories"] == ["/tmp/skills"]
    assert client.create_kwargs["plugin_directories"] == ["/tmp/plugins"]
    assert client.resume_id == "session-1"
    assert client.resume_kwargs["on_event"] is callback
    assert client.resume_kwargs["continue_pending_work"] is False


@pytest.mark.asyncio
async def test_bridge_stop_force_stops_after_graceful_timeout() -> None:
    client = HungStopClient()
    bridge = object.__new__(CopilotBridge)
    bridge._client = client
    bridge._settings = SimpleNamespace(sdk_shutdown_timeout_seconds=0.01)

    await bridge.stop()

    assert client.stop_started.is_set()
    assert client.force_stop_calls == 1
    assert bridge._client is None


class FakeEventLog:
    def __init__(self) -> None:
        self.read_request: object | None = None

    async def tail(self, *, timeout: float) -> object:  # noqa: ASYNC109
        assert timeout == 10
        return SimpleNamespace(cursor="tail-cursor")

    async def read(self, request: object, *, timeout: float) -> object:  # noqa: ASYNC109
        assert timeout == 10
        self.read_request = request
        return SimpleNamespace(
            cursor="next-cursor",
            cursor_status=SimpleNamespace(value="expired"),
            events=[
                SimpleNamespace(ephemeral=False, id="durable"),
                SimpleNamespace(ephemeral=None, id="durable-omitted"),
                SimpleNamespace(ephemeral=True, id="ephemeral"),
            ],
            has_more=True,
        )


class FakeServerSessions:
    def __init__(self) -> None:
        self.request: object | None = None

    async def check_in_use(self, request: object, *, timeout: float) -> object:  # noqa: ASYNC109
        assert timeout == 10
        self.request = request
        return SimpleNamespace(in_use=["session-1"])


@pytest.mark.asyncio
async def test_event_log_recovery_filters_ephemeral_and_preserves_cursor_status() -> None:
    event_log = FakeEventLog()
    session = SimpleNamespace(rpc=SimpleNamespace(event_log=event_log))
    bridge = object.__new__(CopilotBridge)

    assert await bridge.tail_event_log(session) == "tail-cursor"
    batch = await bridge.read_event_log(
        session,
        cursor="old-cursor",
        max_events=25,
        include_ephemeral=False,
    )

    assert batch.cursor == "next-cursor"
    assert batch.cursor_status == "expired"
    assert [event.id for event in batch.events] == ["durable", "durable-omitted"]
    assert batch.filtered_ephemeral == 1
    assert batch.has_more
    assert event_log.read_request.cursor == "old-cursor"
    assert event_log.read_request.max == 25

    with pytest.raises(ValueError, match="never requests ephemeral"):
        await bridge.read_event_log(session, cursor=None, include_ephemeral=True)


@pytest.mark.asyncio
async def test_check_session_in_use_uses_generated_server_rpc() -> None:
    sessions = FakeServerSessions()
    bridge = object.__new__(CopilotBridge)
    bridge._client = SimpleNamespace(rpc=SimpleNamespace(sessions=sessions))

    assert await bridge.check_session_in_use("session-1")
    assert sessions.request.session_ids == ["session-1"]


def test_commands_invoke_full_result_union_is_typed_and_unknown_fails_closed() -> None:
    text = parse_command_result(SlashCommandTextResult("answer", markdown=True))
    prompt = parse_command_result(
        SlashCommandAgentPromptResult(
            display_prompt="Display",
            prompt="runtime prompt",
            mode=SessionMode.PLAN,
        )
    )
    completed = parse_command_result(SlashCommandCompletedResult(message="done"))
    selection = parse_command_result(
        SlashCommandSelectSubcommandResult(
            command="research",
            title="Choose",
            options=[
                SlashCommandSelectSubcommandOption(
                    name="repo",
                    description="Repository",
                )
            ],
        )
    )

    assert text.kind == NativeCommandResultKind.TEXT
    assert text.markdown
    assert prompt.kind == NativeCommandResultKind.AGENT_PROMPT
    assert prompt.prompt == "runtime prompt"
    assert prompt.mode == "plan"
    assert completed.kind == NativeCommandResultKind.COMPLETED
    assert completed.message == "done"
    assert selection.kind == NativeCommandResultKind.SELECT_SUBCOMMAND
    assert selection.options[0].name == "repo"
    with pytest.raises(NativeCapabilityUnavailable, match="unsupported"):
        parse_command_result(object())
