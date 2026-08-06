import json
from pathlib import Path

import pytest

import copilotd.sdk.acceptance as acceptance_module
from copilotd.cli import build_parser
from copilotd.config import Settings
from copilotd.sdk.acceptance import (
    REAL_ACCEPTANCE_CONFIRMATION,
    REAL_ACCEPTANCE_ENV,
    RealAcceptanceError,
    RealAcceptanceOptInError,
    RealNativeAcceptance,
    sanitize_evidence,
)


def test_real_acceptance_requires_exact_environment_opt_in(tmp_path: Path) -> None:
    acceptance = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: "1"},
    )

    with pytest.raises(RealAcceptanceOptInError, match=REAL_ACCEPTANCE_ENV):
        acceptance.require_opt_in()

    accepted = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
    )
    accepted.require_opt_in()


def test_acceptance_evidence_sanitizes_content_urls_ids_and_temp_paths() -> None:
    sanitized = sanitize_evidence(
        {
            "session_id": "real-session-id",
            "answer": "private answer",
            "remote_url": "https://example.invalid/secret",
            "detail": "failed at /private/var/folders/a/b/c and https://example.invalid/x",
            "count": 3,
        }
    )

    assert sanitized["session_id"].startswith("sha256:")
    assert sanitized["answer"].startswith("sha256:")
    assert sanitized["remote_url"].startswith("sha256:")
    assert "example.invalid" not in sanitized["detail"]
    assert "/private/var/folders" not in sanitized["detail"]
    assert sanitized["count"] == 3


def test_native_acceptance_cli_requires_explicit_evidence_path(tmp_path: Path) -> None:
    args = build_parser().parse_args(
        [
            "native-acceptance",
            "--real",
            "--evidence",
            str(tmp_path / "evidence.json"),
        ]
    )

    assert args.command == "native-acceptance"
    assert args.real
    assert args.evidence == tmp_path / "evidence.json"


@pytest.mark.asyncio
async def test_real_acceptance_fails_when_disposable_cleanup_is_unconfirmed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSession:
        async def disconnect(self) -> None:
            return None

    class FakeClient:
        async def delete_session(self, _session_id: str) -> None:
            raise RuntimeError("delete failed")

    class FakeBridge:
        instance: "FakeBridge | None" = None

        def __init__(self, _settings: Settings) -> None:
            self.client = FakeClient()
            self.stopped = False
            FakeBridge.instance = self

        async def start(self) -> None:
            return None

        async def runtime_identity(self) -> dict[str, object]:
            return {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
                "authenticated": True,
                "auth_type": "test",
                "auth_host": "github.com",
            }

        async def create_session(self, **_kwargs: object) -> FakeSession:
            return FakeSession()

        async def ensure_allow_all(self, _session: FakeSession) -> None:
            return None

        async def list_agents(self, _session: FakeSession) -> list[dict[str, object]]:
            return []

        async def get_current_agent_info(
            self,
            _session: FakeSession,
        ) -> None:
            return None

        async def disable_remote(self, _session: FakeSession) -> None:
            return None

        async def get_native_schedules(
            self,
            _session: FakeSession,
        ) -> list[dict[str, object]]:
            return []

        async def stop(self) -> None:
            self.stopped = True

    async def fake_git(_cwd: str, *_arguments: str) -> None:
        return None

    monkeypatch.setattr(acceptance_module, "CopilotBridge", FakeBridge)
    monkeypatch.setattr(acceptance_module, "_git", fake_git)
    evidence_path = tmp_path / "cleanup-evidence.json"
    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=evidence_path,
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"agents"},
    )

    with pytest.raises(RealAcceptanceError, match="cleanup failed"):
        await runner.run()

    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "failed"
    assert not evidence["cleanup"]["session_deleted"]
    assert evidence["cleanup"]["temporary_workspace_removed"]
    assert FakeBridge.instance is not None and FakeBridge.instance.stopped


@pytest.mark.asyncio
async def test_resume_rejects_failed_or_incompletely_cleaned_evidence(
    tmp_path: Path,
) -> None:
    previous = tmp_path / "failed-evidence.json"
    previous.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "failed",
                "cleanup": {
                    "session_deleted": True,
                    "temporary_workspace_removed": True,
                },
                "identity": {},
                "capabilities": {},
            }
        ),
        encoding="utf-8",
    )
    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "new-evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"agents"},
        resume_evidence=(previous,),
    )

    with pytest.raises(RealAcceptanceError, match="did not pass"):
        await runner.run()
