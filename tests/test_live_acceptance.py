import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from copilotd.acceptance.live_scheduler_worktree import (
    DisposableThreadGateway,
    LiveAcceptanceError,
    LiveAuthenticationError,
    LiveSchedulerWorktreeHarness,
    ResultArchive,
    _run_crash_child,
    run_from_args,
)


def test_result_archive_sanitizes_secrets_paths_and_keeps_evidence_hashes(
    tmp_path: Path,
) -> None:
    archive = ResultArchive(tmp_path, "namespace")
    archive.record(
        "feature",
        outcome="passed",
        started_at=datetime.now(UTC),
        detail={
            "access_token": "must-not-appear",
            "token_matched": True,
            "path": Path("/private/disposable"),
            "response_sha256": "a" * 64,
            "message": "prefix " + "x" * 40,
        },
    )
    summary = archive.finalize()
    payload = json.loads(
        (tmp_path / "namespace" / "feature.json").read_text(encoding="utf-8")
    )

    assert summary["outcome"] == "passed"
    assert payload["detail"]["access_token"] == "<redacted>"
    assert payload["detail"]["token_matched"] is True
    assert payload["detail"]["path"] == "<disposable-path>"
    assert payload["detail"]["response_sha256"] == "a" * 64
    assert "x" * 40 not in payload["detail"]["message"]


@pytest.mark.asyncio
async def test_live_mode_is_explicitly_required(tmp_path: Path) -> None:
    args = argparse.Namespace(
        live=False,
        output=tmp_path,
        timeout=1,
        features="scheduled_message",
        namespace="no-live",
    )
    with pytest.raises(ValueError, match="--live is required"):
        await run_from_args(args)


@pytest.mark.asyncio
async def test_live_mode_requires_non_auth_feature(tmp_path: Path) -> None:
    args = argparse.Namespace(
        live=True,
        output=tmp_path,
        timeout=1,
        features="",
        namespace="empty-live",
    )
    with pytest.raises(ValueError, match="at least one"):
        await run_from_args(args)


@pytest.mark.asyncio
async def test_crash_child_requires_matching_parent_nonce(tmp_path: Path) -> None:
    config = tmp_path / "crash-config.json"
    config.write_text(
        json.dumps({"parent_nonce": "expected"}),
        encoding="utf-8",
    )

    with pytest.raises(LiveAcceptanceError, match="nonce"):
        await _run_crash_child(config, parent_nonce="wrong")


@pytest.mark.asyncio
async def test_selected_live_auth_failure_is_fatal_and_archived(tmp_path: Path) -> None:
    class MissingAuthBridge:
        async def start(self) -> None:
            raise RuntimeError("not authenticated")

    harness = LiveSchedulerWorktreeHarness(
        output_dir=tmp_path,
        namespace="missing-auth",
        features=("scheduled_message",),
    )
    with pytest.raises(LiveAuthenticationError):
        await harness._authenticate(MissingAuthBridge())
    payload = json.loads(
        (tmp_path / "missing-auth" / "authentication.json").read_text(
            encoding="utf-8"
        )
    )

    assert payload["outcome"] == "failed"
    assert payload["detail"] == {
        "authenticated": False,
        "error_type": "RuntimeError",
    }


@pytest.mark.asyncio
async def test_disposable_gateway_is_idempotent_for_injected_thread_hook() -> None:
    gateway = DisposableThreadGateway("namespace")
    created = await gateway.create_thread(
        channel_id="channel",
        source_id="source",
        name="name",
        creation_token="token",
    )
    found = await gateway.find_thread(
        channel_id="channel",
        source_id="source",
        creation_token="token",
    )

    assert found == created
    assert gateway.create_calls == 1
