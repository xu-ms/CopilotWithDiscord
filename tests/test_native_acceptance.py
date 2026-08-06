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
            self.permission_handler = None
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

        async def create_session(self, **kwargs: object) -> FakeSession:
            self.permission_handler = kwargs.get("permission_handler")
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

        async def delete_session(self, session_id: str) -> None:
            await self.client.delete_session(session_id)

        async def session_exists(self, _session_id: str) -> bool:
            return True

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
    handler = FakeBridge.instance.permission_handler
    assert handler is not None
    assert handler._approval_validator is not None


@pytest.mark.asyncio
async def test_real_acceptance_preserves_primary_error_when_session_is_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeBridge:
        def __init__(self, _settings: Settings) -> None:
            self.client = object()

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

        async def create_session(self, **_kwargs: object) -> object:
            raise ValueError("primary create failure")

        async def delete_session(self, _session_id: str) -> None:
            raise RuntimeError("session file not found")

        async def session_exists(self, _session_id: str) -> bool:
            return False

        async def stop(self) -> None:
            return None

    async def fake_git(_cwd: str, *_arguments: str) -> None:
        return None

    monkeypatch.setattr(acceptance_module, "CopilotBridge", FakeBridge)
    monkeypatch.setattr(acceptance_module, "_git", fake_git)
    evidence_path = tmp_path / "primary-error-evidence.json"
    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=evidence_path,
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"agents"},
    )

    with pytest.raises(ValueError, match="primary create failure"):
        await runner.run()

    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["error"]["type"] == "ValueError"
    assert evidence["cleanup"]["session_deleted"]
    assert evidence["cleanup"]["session_absence_confirmed"]


@pytest.mark.asyncio
async def test_resume_rejects_failed_or_incompletely_cleaned_evidence(
    tmp_path: Path,
) -> None:
    previous = tmp_path / "failed-evidence.json"
    previous.write_text(
        json.dumps(
            {
                "schema_version": acceptance_module.ACCEPTANCE_SCHEMA_VERSION,
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


@pytest.mark.asyncio
async def test_resume_rejects_legacy_remote_detach_evidence(
    tmp_path: Path,
) -> None:
    previous = tmp_path / "legacy-evidence.json"
    previous.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "passed",
                "cleanup": {
                    "session_deleted": True,
                    "temporary_workspace_removed": True,
                },
                "identity": {},
                "capabilities": {
                    "remote_export_detach_safe": {
                        "supported": True,
                        "executed": True,
                        "status": "passed",
                        "evidence_kind": "real-disposable-runtime",
                        "detail": {"export_steerable": False},
                    }
                },
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

    with pytest.raises(RealAcceptanceError, match="unsupported schema"):
        await runner.run()


@pytest.mark.asyncio
async def test_remote_export_stays_unprobed_without_detach_reconnect(
    tmp_path: Path,
) -> None:
    class RemoteBridge:
        async def get_session_auth(self, _session: object) -> dict[str, object]:
            return {"isAuthenticated": True}

        async def get_remote_state(self, _session: object) -> dict[str, object]:
            return {"metadata": {}}

        async def enable_remote(
            self,
            _session: object,
            mode: str,
        ) -> dict[str, object]:
            return {
                "remoteSteerable": mode == "on",
                "url": "https://example.invalid/remote",
            }

        async def disable_remote(self, _session: object) -> None:
            return None

    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"remote"},
    )

    await runner._exercise_remote(RemoteBridge(), object())  # type: ignore[arg-type]

    capability = runner._capabilities["remote_export_detach_safe"]
    assert not capability.supported
    assert capability.status == "detach-reconnect-unprobed"
    assert capability.detail["detach_exercised"] is False
    assert capability.detail["reconnect_exercised"] is False
    assert capability.detail["continued_execution_verified"] is False


@pytest.mark.asyncio
async def test_model_config_acceptance_requires_set_readback_restore(
    tmp_path: Path,
) -> None:
    class ModelBridge:
        def __init__(self) -> None:
            self.current = "model-a"
            self.effort: str | None = "high"
            self.context_tier: str | None = "long_context"
            self.set_calls: list[tuple[str, str | None, str | None]] = []

        async def list_models(self) -> list[dict[str, object]]:
            return [
                {"id": "model-a"},
                {
                    "id": "model-b",
                    "policy": {"state": "enabled"},
                    "supportedReasoningEfforts": ["none", "low", "high"],
                    "capabilities": {"limits": {"max_context_window_tokens": 1_050_000}},
                },
            ]

        async def get_current_model(self, _session: object) -> dict[str, object]:
            return {
                "modelId": self.current,
                "reasoningEffort": self.effort,
                "contextTier": self.context_tier,
            }

        async def set_model(
            self,
            _session: object,
            *,
            model: str,
            reasoning_effort: str | None,
            context_tier: str | None,
        ) -> None:
            self.current = model
            self.effort = reasoning_effort
            self.context_tier = context_tier
            self.set_calls.append((model, reasoning_effort, context_tier))

    bridge = ModelBridge()
    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"model"},
    )

    await runner._exercise_model_config(bridge, object())  # type: ignore[arg-type]

    capability = runner._capabilities["model_config"]
    assert capability.supported
    assert capability.detail == {
        "changed_confirmed": True,
        "options_changed": True,
        "restored": True,
        "options_restored": True,
        "change_error_type": None,
        "original_config": {
            "modelId": "model-a",
            "reasoningEffort": "high",
            "contextTier": "long_context",
        },
        "changed_config": {
            "modelId": "model-b",
            "reasoningEffort": "low",
            "contextTier": "long_context",
        },
        "restored_config": {
            "modelId": "model-a",
            "reasoningEffort": "high",
            "contextTier": "long_context",
        },
        "restore_via_auto": False,
    }
    assert bridge.set_calls == [
        ("model-b", "low", "long_context"),
        ("model-a", "high", "long_context"),
    ]

    class DropsRestoredOptions(ModelBridge):
        async def set_model(
            self,
            _session: object,
            *,
            model: str,
            reasoning_effort: str | None,
            context_tier: str | None,
        ) -> None:
            if model == "model-a":
                reasoning_effort = None
                context_tier = None
            await super().set_model(
                _session,
                model=model,
                reasoning_effort=reasoning_effort,
                context_tier=context_tier,
            )

    drops_options = DropsRestoredOptions()
    negative_runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "negative-data"),
        evidence_path=tmp_path / "negative-evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"model"},
    )
    await negative_runner._exercise_model_config(
        drops_options,
        object(),  # type: ignore[arg-type]
    )
    negative = negative_runner._capabilities["model_config"]
    assert not negative.supported
    assert negative.detail["restored"] is True
    assert negative.detail["options_restored"] is False


@pytest.mark.asyncio
async def test_model_config_acceptance_clears_sticky_optional_fields_via_auto(
    tmp_path: Path,
) -> None:
    class StickyOptionalBridge:
        def __init__(self) -> None:
            self.current = "model-a"
            self.effort: str | None = None
            self.context_tier: str | None = None
            self.set_calls: list[tuple[str, str | None, str | None]] = []

        async def list_models(self) -> list[dict[str, object]]:
            return [
                {"id": "auto"},
                {"id": "model-a"},
                {
                    "id": "model-b",
                    "policy": {"state": "enabled"},
                    "supportedReasoningEfforts": ["none", "low"],
                    "capabilities": {"limits": {"max_context_window_tokens": 1_050_000}},
                },
            ]

        async def get_current_model(self, _session: object) -> dict[str, object]:
            return {
                "modelId": self.current,
                "reasoningEffort": self.effort,
                "contextTier": self.context_tier,
            }

        async def set_model(
            self,
            _session: object,
            *,
            model: str,
            reasoning_effort: str | None = None,
            context_tier: str | None = None,
        ) -> None:
            self.current = model
            if model == "auto":
                self.effort = None
                self.context_tier = None
            else:
                if reasoning_effort is not None:
                    self.effort = reasoning_effort
                if context_tier is not None:
                    self.context_tier = context_tier
            self.set_calls.append((model, reasoning_effort, context_tier))

    bridge = StickyOptionalBridge()
    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"model"},
    )

    await runner._exercise_model_config(bridge, object())  # type: ignore[arg-type]

    capability = runner._capabilities["model_config"]
    assert capability.supported
    assert capability.detail["restore_via_auto"] is True
    assert bridge.set_calls == [
        ("model-b", "low", "long_context"),
        ("auto", None, None),
        ("model-a", None, None),
    ]


@pytest.mark.asyncio
async def test_task_promotion_probe_promotes_observed_sync_waiter(
    tmp_path: Path,
) -> None:
    class PromotionBridge:
        async def get_current_promotable_task(self, _session: object) -> dict[str, str]:
            return {"id": "task-1"}

        async def promote_task(self, _session: object, task_id: str) -> bool:
            assert task_id == "task-1"
            return True

    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"fleet-tasks"},
    )

    supported, status, detail = await runner._probe_task_promotion(
        PromotionBridge(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        wait_seconds=0,
    )

    assert supported
    assert status == "passed"
    assert detail == {
        "promotable_observed": True,
        "promoted": True,
        "poll_attempts": 1,
    }


@pytest.mark.asyncio
async def test_task_promotion_probe_treats_no_waiter_as_gated(
    tmp_path: Path,
) -> None:
    class NoPromotableBridge:
        async def get_current_promotable_task(self, _session: object) -> None:
            return None

    runner = RealNativeAcceptance(
        Settings(_env_file=None, data_dir=tmp_path / "data"),
        evidence_path=tmp_path / "evidence.json",
        environ={REAL_ACCEPTANCE_ENV: REAL_ACCEPTANCE_CONFIRMATION},
        suites={"fleet-tasks"},
    )

    supported, status, detail = await runner._probe_task_promotion(
        NoPromotableBridge(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        wait_seconds=0,
    )

    assert not supported
    assert status == "gated-not-promotable"
    assert detail == {
        "promotable_observed": False,
        "promoted": False,
        "poll_attempts": 1,
    }
