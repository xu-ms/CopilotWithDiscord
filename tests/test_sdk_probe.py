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
from copilotd.sdk.probe import SdkProbe
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
