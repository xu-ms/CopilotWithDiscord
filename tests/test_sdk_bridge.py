import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from copilot.generated.rpc import PermissionsAllowAllMode
from pydantic import SecretStr

from copilotd.sdk.bridge import (
    CopilotBridge,
    ManagedAwarePermissionHandler,
    PermissionPostureError,
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
    ) -> object:
        assert timeout == 10
        self.set_allow_all_calls += 1
        return SimpleNamespace(success=True)

    async def set_approve_all(
        self,
        _request: object,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> object:
        assert timeout == 10
        self.set_approve_all_calls += 1
        return SimpleNamespace(success=True)


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
    assert posture.approve_all_confirmed
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


@pytest.mark.asyncio
async def test_approve_all_is_verified_even_when_allow_all_is_already_on() -> None:
    permissions = FakePermissions(
        [
            FakePermissionState(True, PermissionsAllowAllMode.ON),
            FakePermissionState(True, PermissionsAllowAllMode.ON),
        ]
    )
    bridge = object.__new__(CopilotBridge)

    posture = await bridge.ensure_allow_all(FakeSession(FakeRpc(permissions)))

    assert posture.approve_all_confirmed
    assert permissions.set_allow_all_calls == 0
    assert permissions.set_approve_all_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("permission_kind", ["shell", "write", "mcp"])
async def test_managed_aware_permission_handler_approves_ordinary_once(
    permission_kind: str,
) -> None:
    audits: list[dict[str, object]] = []

    async def audit(payload: dict[str, object]) -> None:
        audits.append(payload)

    request = SimpleNamespace(
        kind=permission_kind,
        managed_approval_required=False,
        to_dict=lambda: {"kind": permission_kind},
    )
    result = await ManagedAwarePermissionHandler(audit)(
        request,
        {"managed_settings_enabled": False},
    )

    assert result.kind == "approve-once"
    assert audits[0]["permission_kind"] == permission_kind
    assert audits[0]["decision"] == "approve-once"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("managed_settings", "managed_request"),
    [(True, False), (False, True)],
)
async def test_managed_aware_permission_handler_returns_user_not_available(
    managed_settings: bool,
    managed_request: bool,
) -> None:
    request = SimpleNamespace(
        kind="write",
        managed_approval_required=managed_request,
        to_dict=lambda: {
            "kind": "write",
            "managedApprovalRequired": managed_request,
        },
    )

    result = await ManagedAwarePermissionHandler()(
        request,
        {"managed_settings_enabled": managed_settings},
    )

    assert result.kind == "user-not-available"


@pytest.mark.asyncio
async def test_managed_permission_state_from_runtime_events_blocks_without_sdk_flags() -> None:
    handler = ManagedAwarePermissionHandler()
    request = SimpleNamespace(
        kind="mcp",
        to_dict=lambda: {"kind": "mcp"},
    )

    handler.set_managed_permissions_blocked(True)
    blocked = await handler(request, {"session_id": "session-1"})
    handler.set_managed_permissions_blocked(False)
    ordinary = await handler(request, {"session_id": "session-1"})

    assert blocked.kind == "user-not-available"
    assert ordinary.kind == "approve-once"


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
    bridge._settings = SimpleNamespace(github_token=None)

    def callback(_event: object) -> None:
        return None

    await bridge.create_session(
        session_id="session-1",
        working_directory="/tmp/project",
        on_event=callback,
    )
    await bridge.resume_session(
        session_id="session-1",
        working_directory="/tmp/project",
        on_event=callback,
        continue_pending_work=False,
    )

    assert client.create_kwargs["on_event"] is callback
    assert client.create_kwargs["enable_managed_settings"] is False
    assert client.create_kwargs["github_token"] is None
    assert client.create_kwargs["manage_schedule_enabled"] is False
    assert client.create_kwargs["streaming"] is True
    assert client.resume_id == "session-1"
    assert client.resume_kwargs["on_event"] is callback
    assert client.resume_kwargs["continue_pending_work"] is False


@pytest.mark.asyncio
async def test_bridge_enables_managed_settings_only_with_session_token() -> None:
    client = FakeClient()
    bridge = object.__new__(CopilotBridge)
    bridge._client = client
    bridge._settings = SimpleNamespace(github_token=SecretStr("managed-token"))

    await bridge.create_session(
        session_id="session-managed",
        working_directory="/tmp/project",
        on_event=lambda _event: None,
    )

    assert client.create_kwargs["enable_managed_settings"] is True
    assert client.create_kwargs["github_token"] == "managed-token"


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
