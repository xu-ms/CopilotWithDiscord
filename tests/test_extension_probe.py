import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.cli import build_parser
from copilotd.config import Settings
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.core.volatile_content import VolatileContentStore
from copilotd.sdk import extension_probe
from copilotd.sdk.extension_probe import (
    ExtensionAcceptanceProbe,
    LiveAcceptanceAuthError,
    _correlate_mcp_tool_evidence,
    _delete_session,
    _load_volatile_tool_evidence,
    _protocol_response_evidence,
    _send_and_wait_for_message,
)
from copilotd.storage.database import Database
from copilotd.storage.state_only import payload_sha256


def _tool_start_evidence(data: dict[str, object]) -> dict[str, object]:
    return {
        "kind": "tool.execution_start",
        "tool_call_id": str(data["toolCallId"]),
        "server_name": str(data.get("mcpServerName") or ""),
        "tool_name": str(data.get("mcpToolName") or ""),
        "arguments_hash": payload_sha256(data.get("arguments")),
    }


def _tool_completion_evidence(
    tool_call_id: str,
    *,
    success: bool,
    result_texts: tuple[str, ...],
) -> dict[str, object]:
    return {
        "kind": "tool.execution_complete",
        "tool_call_id": tool_call_id,
        "success": success,
        "result_text_hashes": tuple(
            hashlib.sha256(value.encode("utf-8")).hexdigest() for value in result_texts
        ),
    }


@pytest.mark.asyncio
async def test_live_extension_probe_fails_closed_without_auth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instances: list[object] = []

    class FakeBridge:
        def __init__(self, _settings: Settings) -> None:
            self.started = False
            self.stopped = False
            self.client = SimpleNamespace(delete_session=self._delete)
            instances.append(self)

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def runtime_identity(self) -> dict[str, object]:
            return {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "authenticated": False,
            }

        async def _delete(self, _session_id: str) -> None:
            return None

    monkeypatch.setattr(extension_probe, "CopilotBridge", FakeBridge)
    probe = ExtensionAcceptanceProbe(Settings(_env_file=None, data_dir=tmp_path / "data"))

    with pytest.raises(LiveAcceptanceAuthError, match="authenticated"):
        await probe.run_live(wait_seconds=1)

    assert len(instances) == 1
    assert instances[0].started
    assert instances[0].stopped


def test_cli_exposes_explicit_live_extension_acceptance_selector() -> None:
    args = build_parser().parse_args(["sdk-probe", "--live-extensions"])

    assert args.command == "sdk-probe"
    assert args.live_extensions is True


@pytest.mark.asyncio
async def test_extension_turn_rejects_session_error_before_idle() -> None:
    event_queue: asyncio.Queue[object] = asyncio.Queue()

    class FakeBridge:
        async def send(self, *_args, **_kwargs) -> str:
            for event_type in ("assistant.message", "session.error", "session.idle"):
                await event_queue.put(SimpleNamespace(type=SimpleNamespace(value=event_type)))
            return "accepted-message"

    with pytest.raises(RuntimeError, match="reported an error"):
        await _send_and_wait_for_message(
            FakeBridge(),
            object(),
            event_queue,
            "probe",
            wait_seconds=1,
        )


@pytest.mark.asyncio
async def test_extension_cleanup_requires_authoritative_session_absence() -> None:
    class PresentBridge:
        async def delete_session(self, _session_id: str) -> None:
            return None

        async def session_exists(self, _session_id: str) -> bool:
            return True

    with pytest.raises(RuntimeError, match="deletion was not confirmed"):
        await _delete_session(PresentBridge(), "session-1")


@pytest.mark.asyncio
async def test_extension_cleanup_accepts_lost_delete_response_after_absence() -> None:
    class AbsentBridge:
        async def delete_session(self, _session_id: str) -> None:
            raise RuntimeError("delete response lost")

        async def session_exists(self, _session_id: str) -> bool:
            return False

    await _delete_session(AbsentBridge(), "session-1")


def test_protocol_response_evidence_is_unprobed_without_real_request() -> None:
    evidence = _protocol_response_evidence(
        requested_type="sampling.requested",
        completed_type="sampling.completed",
        events=[],
        accepted=[],
        missing_request_result=False,
        trigger_attempted=True,
    )

    assert evidence["status"] == "unprobed"


def test_protocol_response_evidence_fails_partial_real_flow() -> None:
    with pytest.raises(RuntimeError, match="without successful settlement"):
        _protocol_response_evidence(
            requested_type="sampling.requested",
            completed_type="sampling.completed",
            events=["sampling.requested"],
            accepted=[False],
            missing_request_result=False,
            trigger_attempted=True,
        )


def test_protocol_response_evidence_passes_only_settled_completion() -> None:
    evidence = _protocol_response_evidence(
        requested_type="sampling.requested",
        completed_type="sampling.completed",
        events=["sampling.requested", "sampling.completed"],
        accepted=[True],
        missing_request_result=False,
        trigger_attempted=True,
    )

    assert evidence["status"] == "passed"


def test_reattach_evidence_rejects_unrelated_tool_completion() -> None:
    rows = [
        _tool_start_evidence(
            {
                "toolCallId": "builtin-1",
                "toolName": "shell",
                "arguments": {"command": "echo REATTACH_TOOL_OK"},
            }
        ),
        _tool_completion_evidence(
            "builtin-1",
            success=True,
            result_texts=("REATTACH_TOOL_OK",),
        ),
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is False


def test_reattach_evidence_rejects_marker_outside_result_and_duplicate_identity() -> None:
    start = _tool_start_evidence(
        {
            "toolCallId": "mcp-1",
            "toolName": "second/echo",
            "mcpServerName": "second",
            "mcpToolName": "echo",
            "arguments": {"text": "REATTACH_TOOL_OK"},
        }
    )
    completion = _tool_completion_evidence(
        "mcp-1",
        success=True,
        result_texts=("WRONG_RESULT",),
    )

    marker_outside_result = _correlate_mcp_tool_evidence(
        [start, completion],
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )
    duplicate_request = _correlate_mcp_tool_evidence(
        [start, start, completion],
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert marker_outside_result["correlated"] is False
    assert duplicate_request["correlated"] is False


def test_reattach_evidence_correlates_exact_mcp_request_and_result() -> None:
    rows = [
        _tool_start_evidence(
            {
                "toolCallId": "mcp-1",
                "toolName": "second/echo",
                "mcpServerName": "second",
                "mcpToolName": "echo",
                "arguments": {"text": "REATTACH_TOOL_OK"},
            }
        ),
        _tool_completion_evidence(
            "mcp-1",
            success=True,
            result_texts=("REATTACH_TOOL_OK",),
        ),
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is True
    assert evidence["request_identity_matched"] is True


@pytest.mark.asyncio
async def test_live_reduction_keeps_tool_acceptance_evidence_only_in_memory(
    tmp_path: Path,
) -> None:
    session_id = "extension-probe-session"
    async with Database(tmp_path / "extension-probe.sqlite3") as database:
        await SessionBindingRepository(database).create(
            thread_id="extension-probe-thread",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_generation = 1, owner_fence_token = 7
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        data = (
            {
                "toolCallId": "mcp-1",
                "toolName": "second/echo",
                "mcpServerName": "second",
                "mcpToolName": "echo",
                "arguments": {"text": "REATTACH_TOOL_OK"},
            },
            {
                "toolCallId": "mcp-1",
                "success": True,
                "result": {"content": "REATTACH_TOOL_OK"},
            },
        )
        events = [
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=1,
                fence_token=7,
                inbox_seq=index,
                source="sdk",
                raw_type=kind,
                raw_payload={"type": kind, "data": payload},
                reducer_hash=f"{index:064x}",
                persistence_class="durable",
                received_at=float(index),
                event_id=f"tool-event-{index}",
                tool_call_id="mcp-1",
            )
            for index, (kind, payload) in enumerate(
                zip(
                    ("tool.execution_start", "tool.execution_complete"),
                    data,
                    strict=True,
                ),
                start=1,
            )
        ]
        assert (
            await JournalReducer(
                database,
                capture_tool_acceptance_evidence=True,
            ).persist(events)
            == 2
        )
        rows = await database.fetchall(
            """
            SELECT raw_type, tool_call_id, raw_payload
            FROM event_journal ORDER BY inbox_seq
            """
        )
        evidence = _load_volatile_tool_evidence(
            database.content_store,
            rows,
            session_id=session_id,
            generation=1,
        )
        assert database.content_store.item_count == 0

    assert all("REATTACH_TOOL_OK" not in str(row["raw_payload"]) for row in rows)
    assert _correlate_mcp_tool_evidence(
        evidence,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )["correlated"]


def test_normal_tool_events_do_not_consume_acceptance_evidence_capacity() -> None:
    database = Database(Path(":memory:"))
    database.content_store = VolatileContentStore(max_items=2, max_bytes=1_024)
    reducer = JournalReducer(database)

    for index in range(3_000):
        for offset, kind in enumerate(
            ("tool.execution_start", "tool.execution_complete"),
        ):
            tool_call_id = f"tool-{index}"
            reducer._capture_tool_event_evidence(
                AdaptedEvent(
                    sdk_session_id="production-session",
                    generation=1,
                    fence_token=1,
                    inbox_seq=index * 2 + offset + 1,
                    source="sdk",
                    raw_type=kind,
                    raw_payload={
                        "type": kind,
                        "data": {
                            "toolCallId": tool_call_id,
                            "arguments": {"index": index},
                            "success": True,
                            "result": {"content": "normal tool result"},
                        },
                    },
                    reducer_hash=f"{index * 2 + offset + 1:064x}",
                    persistence_class="durable",
                    received_at=float(index),
                    event_id=f"event-{index}-{offset}",
                    tool_call_id=tool_call_id,
                )
            )

    assert database.content_store.item_count == 0


@pytest.mark.parametrize(
    ("start_overrides", "completion_id", "completion_marker"),
    [
        ({"mcpServerName": "first"}, "mcp-1", "REATTACH_TOOL_OK"),
        ({"mcpToolName": "other"}, "mcp-1", "REATTACH_TOOL_OK"),
        (
            {"mcpServerName": None, "toolName": "second/echo"},
            "mcp-1",
            "REATTACH_TOOL_OK",
        ),
        ({}, "mcp-other", "REATTACH_TOOL_OK"),
        ({}, "mcp-1", "WRONG_RESULT"),
    ],
)
def test_reattach_evidence_requires_exact_server_tool_request_and_result(
    start_overrides: dict[str, object],
    completion_id: str,
    completion_marker: str,
) -> None:
    start = {
        "toolCallId": "mcp-1",
        "toolName": "second/echo",
        "mcpServerName": "second",
        "mcpToolName": "echo",
        "arguments": {"text": "REATTACH_TOOL_OK"},
        **start_overrides,
    }
    rows = [
        _tool_start_evidence(start),
        _tool_completion_evidence(
            completion_id,
            success=True,
            result_texts=(completion_marker,),
        ),
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is False
