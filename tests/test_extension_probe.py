from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.cli import build_parser
from copilotd.config import Settings
from copilotd.sdk import extension_probe
from copilotd.sdk.extension_probe import (
    ExtensionAcceptanceProbe,
    LiveAcceptanceAuthError,
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
