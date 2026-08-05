from pathlib import Path

import pytest

from copilotd.e2e.discord_harness import (
    DiscordE2EConfigurationError,
    DiscordRealHarness,
    FeatureEvidence,
    RunEvidence,
    load_required_token,
    sanitize_evidence,
    write_evidence,
)


def test_selected_e2e_fails_when_required_key_is_missing(tmp_path: Path) -> None:
    env_file = tmp_path / "missing.env"
    env_file.write_text("OTHER=value\n", encoding="utf-8")

    with pytest.raises(DiscordE2EConfigurationError, match="testbot"):
        load_required_token(env_file)


def test_env_loader_reads_token_without_shell_expansion(tmp_path: Path) -> None:
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "testbot='literal-$NOT_EXPANDED-token'\n",
        encoding="utf-8",
    )

    assert load_required_token(env_file) == "literal-$NOT_EXPANDED-token"


def test_evidence_is_sanitized_and_atomic(tmp_path: Path) -> None:
    evidence = RunEvidence(
        run_id="run",
        started_at=1,
        features=[
            FeatureEvidence(
                feature="probe",
                status="passed",
                transport="unit",
                detail="safe",
            )
        ],
    )
    raw = {
        "token": "secret-token",
        "nested": {"authorization": "Bearer secret", "safe": "visible"},
    }

    assert sanitize_evidence(raw) == {
        "token": "[redacted]",
        "nested": {"authorization": "[redacted]", "safe": "visible"},
    }
    output = tmp_path / "evidence.json"
    write_evidence(output, evidence)
    assert output.is_file()
    assert not list(tmp_path.glob("*.tmp"))


def test_human_driver_plan_has_actions_assertions_and_stable_ids(
    tmp_path: Path,
) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
    )
    harness.evidence.guild_id = "guild"
    harness.evidence.channel_id = "channel"
    harness.evidence.thread_id = "thread"
    harness._command_paths = [
        "session list",
        "project mcp add",
        "Ask Copilot",
        "Pin message",
    ]
    harness._stable_ids = {
        "thread_name": "e2e-thread",
        "seed_message_id": "seed",
        "taskdeck_message_id": "taskdeck",
    }

    harness._record_interaction_coverage()

    pending = [
        feature for feature in harness.evidence.features if feature.status == "pending_human_driver"
    ]
    assert pending
    assert all(feature.ui_actions for feature in pending)
    assert all(feature.assertions for feature in pending)
    assert all(feature.stable_identifiers["thread_id"] == "thread" for feature in pending)
    slash_paths = {
        feature.stable_identifiers["command_path"]
        for feature in pending
        if "command_path" in feature.stable_identifiers
    }
    assert slash_paths == {"/session list", "/project mcp add"}
    assert all("blocked" not in feature.status for feature in pending)
