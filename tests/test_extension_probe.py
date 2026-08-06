import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.cli import build_parser
from copilotd.config import Settings
from copilotd.sdk import extension_probe
from copilotd.sdk.extension_probe import (
    ExtensionAcceptanceProbe,
    LiveAcceptanceAuthError,
    _correlate_mcp_tool_evidence,
    _protocol_response_evidence,
)


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
        {
            "raw_type": "tool.execution_start",
            "tool_call_id": "builtin-1",
            "raw_payload": json.dumps(
                {
                    "data": {
                        "toolCallId": "builtin-1",
                        "toolName": "shell",
                        "arguments": {"command": "echo REATTACH_TOOL_OK"},
                    }
                }
            ),
        },
        {
            "raw_type": "tool.execution_complete",
            "tool_call_id": "builtin-1",
            "raw_payload": json.dumps(
                {
                    "data": {
                        "toolCallId": "builtin-1",
                        "success": True,
                        "result": "REATTACH_TOOL_OK",
                    }
                }
            ),
        },
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is False


def test_reattach_evidence_rejects_marker_outside_result_and_duplicate_identity() -> None:
    start = {
        "raw_type": "tool.execution_start",
        "tool_call_id": "mcp-1",
        "raw_payload": json.dumps(
            {
                "data": {
                    "toolCallId": "mcp-1",
                    "toolName": "second/echo",
                    "mcpServerName": "second",
                    "mcpToolName": "echo",
                    "arguments": {"text": "REATTACH_TOOL_OK"},
                }
            }
        ),
    }
    completion = {
        "raw_type": "tool.execution_complete",
        "tool_call_id": "mcp-1",
        "raw_payload": json.dumps(
            {
                "data": {
                    "toolCallId": "mcp-1",
                    "success": True,
                    "result": {"content": "WRONG_RESULT"},
                    "mcpMeta": {"note": "REATTACH_TOOL_OK"},
                }
            }
        ),
    }

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
        {
            "raw_type": "tool.execution_start",
            "tool_call_id": "mcp-1",
            "raw_payload": json.dumps(
                {
                    "data": {
                        "toolCallId": "mcp-1",
                        "toolName": "second/echo",
                        "mcpServerName": "second",
                        "mcpToolName": "echo",
                        "arguments": {"text": "REATTACH_TOOL_OK"},
                    }
                }
            ),
        },
        {
            "raw_type": "tool.execution_complete",
            "tool_call_id": "mcp-1",
            "raw_payload": json.dumps(
                {
                    "data": {
                        "toolCallId": "mcp-1",
                        "success": True,
                        "result": {
                            "content": "REATTACH_TOOL_OK",
                        },
                    }
                }
            ),
        },
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is True
    assert evidence["request_identity_matched"] is True


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
        {
            "raw_type": "tool.execution_start",
            "tool_call_id": "mcp-1",
            "raw_payload": json.dumps({"data": start}),
        },
        {
            "raw_type": "tool.execution_complete",
            "tool_call_id": completion_id,
            "raw_payload": json.dumps(
                {
                    "data": {
                        "toolCallId": completion_id,
                        "success": True,
                        "result": {"content": completion_marker},
                    }
                }
            ),
        },
    ]

    evidence = _correlate_mcp_tool_evidence(
        rows,
        server_name="second",
        tool_name="echo",
        marker="REATTACH_TOOL_OK",
    )

    assert evidence["correlated"] is False
