from pathlib import Path

import pytest
from pydantic import SecretStr

from copilotd.config import Settings
from copilotd.ops.contracts import EXPECTED_RUNTIME_VERSION, EXPECTED_SDK_VERSION
from copilotd.ops.preflight import PreflightFailed, SetupPreflight


def _settings(tmp_path: Path, *, token: str | None = "super-secret-value") -> Settings:
    return Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        resolved_home=tmp_path,
        discord_token=None if token is None else SecretStr(token),
    )


@pytest.mark.asyncio
async def test_preflight_validates_discord_runtime_directories_and_tzdata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "copilotd.ops.preflight._package_version",
        lambda _: EXPECTED_SDK_VERSION,
    )
    seen_tokens: list[str] = []

    def discord_probe(token: str) -> dict[str, str]:
        seen_tokens.append(token)
        return {"id": "123", "username": "copilotd-test"}

    async def copilot_probe() -> dict[str, object]:
        return {
            "authenticated": True,
            "runtime_version": EXPECTED_RUNTIME_VERSION,
            "model_count": 2,
        }

    report = await SetupPreflight(
        _settings(tmp_path),
        discord_probe=discord_probe,
        copilot_probe=copilot_probe,
    ).run()

    assert report.ok is True
    assert seen_tokens == ["super-secret-value"]
    assert report.discord_identity == {"id": "123", "username": "copilotd-test"}
    assert "super-secret-value" not in str(report.as_dict())


@pytest.mark.asyncio
async def test_preflight_failure_is_typed_and_actionable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "copilotd.ops.preflight._package_version",
        lambda _: EXPECTED_SDK_VERSION,
    )

    async def copilot_probe() -> dict[str, object]:
        return {
            "authenticated": False,
            "runtime_version": "wrong",
            "model_count": 0,
        }

    report = await SetupPreflight(
        _settings(tmp_path, token=None),
        discord_probe=lambda _: {"id": "unused"},
        copilot_probe=copilot_probe,
    ).run()

    assert report.ok is False
    with pytest.raises(PreflightFailed) as failure:
        report.require_success()
    assert any("discord_token" in item for item in failure.value.failures)
    assert any("copilot_runtime" in item for item in failure.value.failures)
