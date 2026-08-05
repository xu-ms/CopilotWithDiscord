from pathlib import Path

import pytest

from copilotd.e2e.discord_harness import (
    DiscordE2EConfigurationError,
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
