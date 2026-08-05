import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from copilot.generated.rpc import PermissionsAllowAllMode

from copilotd.sdk.bridge import CopilotBridge, PermissionPostureError


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
