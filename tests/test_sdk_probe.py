import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from copilotd.config import Settings
from copilotd.sdk.capabilities import (
    CHECKED_CAPABILITY_FIXTURE_SHA256,
    CapabilityFixtureError,
    CapabilityRegistry,
    RuntimeIdentityMismatch,
)
from copilotd.sdk.probe import CapabilityResult, SdkProbe, _response_matches
from copilotd.storage.database import Database


def test_static_sdk_matrix_tracks_released_contract(tmp_path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)

    matrix = SdkProbe(settings).static_matrix()

    assert matrix["sdk_version"] == "1.0.8"
    assert matrix["event_count"] == 114
    assert matrix["main_branch_only_events"] == [
        "factory.run_updated",
        "session.context_cleared",
    ]
    assert matrix["capabilities"]["pre_registered_on_event"]["supported"]
    assert "audited_main_event_count" not in matrix
    assert len(matrix["event_types"]) == 114


def test_checked_fixture_hash_and_identity_are_valid(tmp_path: Path) -> None:
    manifest = CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).load_checked()

    assert manifest.fixture_sha256 == CHECKED_CAPABILITY_FIXTURE_SHA256
    assert manifest.identity.sdk_version == "1.0.8"
    assert manifest.identity.runtime_version == "1.0.73"
    assert manifest.identity.protocol_version == 3
    assert manifest.generated_event_count == 114
    assert manifest.supports("event_log")
    assert not manifest.supports("detached_continuation")
    assert not manifest.supports("hook_agent_stop")
    assert not manifest.supports("hook_user_prompt_transformed")
    assert not manifest.supports("protocol_sampling_response")
    assert not manifest.supports("protocol_session_limits_response")
    assert not manifest.supports("protocol_mcp_headers_response")


def test_checked_fixture_rejects_tampering(tmp_path: Path) -> None:
    fixture = tmp_path / "capabilities.json"
    fixture.write_text("{}\n", encoding="utf-8")
    registry = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path),
        checked_fixture_path=fixture,
        checked_fixture_sha256=CHECKED_CAPABILITY_FIXTURE_SHA256,
    )

    with pytest.raises(CapabilityFixtureError, match="hash mismatch"):
        registry.load_checked()


def test_runtime_identity_must_exactly_match_checked_tuple(tmp_path: Path) -> None:
    registry = CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path))

    with pytest.raises(RuntimeIdentityMismatch, match="has no checked capability evidence"):
        registry.resolve(
            {
                "runtime_version": "1.0.74",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            }
        )


def test_local_evidence_requires_valid_referenced_fixture(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    checked = CapabilityRegistry(settings).load_checked().to_dict()
    checked["fixture"] = {
        "path": str(tmp_path / "missing.events.jsonl"),
        "sha256": "0" * 64,
    }
    settings.capability_path.parent.mkdir(parents=True)
    settings.capability_path.write_text(json.dumps(checked), encoding="utf-8")

    with pytest.raises(CapabilityFixtureError, match="is missing"):
        CapabilityRegistry(settings).load_local()


def test_schema_one_local_evidence_is_ignored_for_checked_fallback(
    tmp_path: Path,
) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    settings.capability_path.parent.mkdir(parents=True)
    settings.capability_path.write_text(
        json.dumps({"schema_version": 1}),
        encoding="utf-8",
    )

    registry = CapabilityRegistry(settings)

    assert registry.load_local() is None
    assert registry.resolve(
        {
            "runtime_version": "1.0.73",
            "protocol_version": 3,
            "ping_protocol_version": 3,
        }
    ).supports("commands_list")


def test_discord_manifest_is_derived_from_capability_evidence(tmp_path: Path) -> None:
    manifest = CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).load_checked()
    capabilities = dict(manifest.capabilities)
    capabilities["session_mode"] = replace(
        capabilities["session_mode"],
        supported=False,
    )
    capabilities["usage"] = replace(capabilities["usage"], supported=False)
    gated = replace(manifest, capabilities=capabilities)

    assert "plan" not in gated.discord_command_roots()
    assert "autopilot" not in gated.discord_command_roots()
    assert "usage" not in gated.discord_command_roots()
    assert {"project", "queue", "session", "steer"} <= gated.discord_command_roots()


@pytest.mark.asyncio
async def test_activation_persists_exact_capability_evidence(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    async with Database(settings.database_path) as database:
        manifest = await CapabilityRegistry(settings).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        row = await database.fetchone(
            """
            SELECT protocol_version, supported, evidence_kind, fixture_sha256,
                   generated_event_count
            FROM capabilities
            WHERE capability = 'event_log'
            """
        )

    assert manifest.supports("event_log")
    assert dict(row) == {
        "protocol_version": 3,
        "supported": 1,
        "evidence_kind": "live-rpc-fixture",
        "fixture_sha256": CHECKED_CAPABILITY_FIXTURE_SHA256,
        "generated_event_count": 114,
    }


def test_unprobed_live_capabilities_merge_checked_facts_without_erasing_support(
    tmp_path: Path,
) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    probe = SdkProbe(settings)
    fixture = tmp_path / "normal-live.events.jsonl"
    fixture.write_text("{}\n", encoding="utf-8")
    fixture_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    supported = CapabilityResult(True, {"observed": True})
    live = {
        "runtime": {
            "runtime_version": "1.0.73",
            "protocol_version": 3,
            "ping_protocol_version": 3,
        },
        "accepted_user_event_id_mapping": True,
        "activity": supported,
        "processing": supported,
        "commands": supported,
        "context_info": supported,
        "event_log_read": supported,
        "event_log_tail": supported,
        "models": supported,
        "queue": supported,
        "permission_posture": {"enabled": True, "mode": "on"},
        "durable_history_recovered": True,
        "session_id_matches": True,
        "resume_session_id_matches": True,
        "callback_survived_idle": True,
        "agents": supported,
        "agent_current": supported,
        "mode_initial": supported,
        "mode_autopilot": supported,
        "sessions_check_in_use": supported,
        "tasks": supported,
        "task_list": supported,
        "usage_metrics": supported,
    }
    matrix = probe._live_matrix(live, fixture, fixture_hash)
    probe._write_matrix(matrix)

    local = CapabilityRegistry(settings).load_local()
    assert local is not None
    assert local.capabilities["native_schedule"].supported is None
    assert local.capabilities["model_config"].supported is None
    assert local.capabilities["remote"].supported is None

    merged = CapabilityRegistry(settings).resolve(live["runtime"])
    assert merged.supports("model_config")
    assert not merged.supports("native_schedule")
    assert not merged.supports("remote")
    assert merged.capabilities["native_schedule"].evidence_kind.startswith("checked-fallback:")

    live["native_schedule_direct"] = CapabilityResult(
        False,
        {"reason": "explicit disposable invocation failure"},
    )
    probe._write_matrix(probe._live_matrix(live, fixture, fixture_hash))
    explicit_negative = CapabilityRegistry(settings).resolve(live["runtime"])
    assert not explicit_negative.supports("native_schedule")
    assert explicit_negative.capabilities["native_schedule"].evidence_kind == "live-command-probe"

    live.pop("native_schedule_direct")
    unknown_error = probe._live_matrix(live, fixture, fixture_hash)
    unknown_error["capabilities"]["remote"] = {
        "supported": None,
        "evidence_kind": "live-rpc-error",
        "detail": {"error_type": "ConnectionError"},
    }
    probe._write_matrix(unknown_error)
    preserved_unknown = CapabilityRegistry(settings).resolve(live["runtime"])
    assert not preserved_unknown.supports("remote")
    assert preserved_unknown.capabilities["remote"].evidence_kind == "live-rpc-error"


def test_failed_broad_live_probe_blocks_exact_capability_fallback(
    tmp_path: Path,
) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    probe = SdkProbe(settings)
    fixture = tmp_path / "failed-live.events.jsonl"
    fixture.write_text("{}\n", encoding="utf-8")
    fixture_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    supported = CapabilityResult(True, {})
    failed = CapabilityResult(False, {"error_type": "MethodNotFound"})
    live = {
        "runtime": {
            "runtime_version": "1.0.73",
            "protocol_version": 3,
            "ping_protocol_version": 3,
        },
        "accepted_user_event_id_mapping": True,
        "activity": supported,
        "processing": supported,
        "commands": failed,
        "context_info": supported,
        "event_log_read": supported,
        "event_log_tail": supported,
        "models": supported,
        "queue": supported,
        "permission_posture": {"enabled": True, "mode": "on"},
        "durable_history_recovered": True,
        "session_id_matches": True,
        "resume_session_id_matches": True,
        "callback_survived_idle": True,
        "agents": supported,
        "agent_current": supported,
        "mode_initial": supported,
        "mode_autopilot": supported,
        "sessions_check_in_use": supported,
        "tasks": supported,
        "task_list": supported,
        "schedule": supported,
        "metadata_snapshot": supported,
        "model_current": failed,
        "native_schedule_direct": CapabilityResult(
            True,
            {"invocation": {"kind": "completed"}},
        ),
        "usage_metrics": supported,
    }
    probe._write_matrix(probe._live_matrix(live, fixture, fixture_hash))

    resolved = CapabilityRegistry(settings).resolve(live["runtime"])

    assert not resolved.supports("commands_list")
    assert not resolved.supports("builtin_review")
    assert not resolved.supports("model_config")
    assert resolved.capabilities["builtin_review"].evidence_kind == "live-prerequisite-failed"


def test_builtin_support_requires_invoke_and_exact_result_variant(
    tmp_path: Path,
) -> None:
    manifest = CapabilityRegistry(Settings(_env_file=None, data_dir=tmp_path)).load_checked()
    capabilities = dict(manifest.capabilities)
    capabilities["commands_invoke"] = replace(
        capabilities["commands_invoke"],
        supported=False,
    )
    without_invoke = replace(manifest, capabilities=capabilities)
    assert not without_invoke.supports("builtin_review")
    assert "review" not in without_invoke.discord_command_roots()

    capabilities = dict(manifest.capabilities)
    capabilities["commands_result_agent_prompt"] = replace(
        capabilities["commands_result_agent_prompt"],
        supported=False,
    )
    without_variant = replace(manifest, capabilities=capabilities)
    assert not without_variant.supports("builtin_review")
    assert "review" not in without_variant.discord_command_roots()

    capabilities = dict(manifest.capabilities)
    capabilities["commands_result_completed"] = replace(
        capabilities["commands_result_completed"],
        supported=False,
    )
    without_completed = replace(manifest, capabilities=capabilities)
    assert "create" not in without_completed.schedule_actions("after")


def test_model_mutation_requires_round_trip_not_read_only_snapshot(
    tmp_path: Path,
) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    probe = SdkProbe(settings)
    fixture = tmp_path / "model-live.events.jsonl"
    fixture.write_text("{}\n", encoding="utf-8")
    fixture_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    supported = CapabilityResult(True, {})
    live = {
        "runtime": {
            "runtime_version": "1.0.73",
            "protocol_version": 3,
            "ping_protocol_version": 3,
        },
        "accepted_user_event_id_mapping": True,
        "activity": supported,
        "processing": supported,
        "commands": supported,
        "context_info": supported,
        "event_log_read": supported,
        "event_log_tail": supported,
        "models": supported,
        "model_current": supported,
        "queue": supported,
        "permission_posture": {"enabled": True, "mode": "on"},
        "durable_history_recovered": True,
        "session_id_matches": True,
        "resume_session_id_matches": True,
        "callback_survived_idle": True,
        "agents": supported,
        "agent_current": supported,
        "mode_initial": supported,
        "mode_autopilot": supported,
        "sessions_check_in_use": supported,
        "tasks": supported,
        "task_list": supported,
        "schedule": supported,
        "metadata_snapshot": supported,
        "usage_metrics": supported,
    }

    matrix = probe._live_matrix(live, fixture, fixture_hash)

    assert matrix["capabilities"]["model_config"]["supported"] is None
    assert matrix["capabilities"]["model_config"]["evidence_kind"] == "unprobed"
    live["model_config_round_trip"] = CapabilityResult(
        True,
        {"changed_confirmed": True, "restored": True},
    )
    exercised = probe._live_matrix(live, fixture, fixture_hash)
    assert exercised["capabilities"]["model_config"]["supported"] is True
    assert exercised["capabilities"]["model_config"]["evidence_kind"] == "live-model-mutation-probe"


def test_live_probe_expected_response_requires_exact_assistant_message() -> None:
    events = [
        {
            "type": "assistant.message",
            "data": {"content": "COPILOTD_ACCEPTANCE_AUTH_OK"},
        }
    ]

    assert _response_matches(events, "COPILOTD_ACCEPTANCE_AUTH_OK")
    assert not _response_matches(events, "WRONG_ACCOUNT_SENTINEL")
    assert not _response_matches(
        [{"type": "session.idle", "data": {}}],
        "COPILOTD_ACCEPTANCE_AUTH_OK",
    )
