from pathlib import Path

import pytest
from pydantic import ValidationError

from copilotd.config import Settings


def test_settings_resolve_paths_and_create_layout(tmp_path: Path) -> None:
    data_dir = tmp_path / "data" / ".." / "copilotd"
    home = tmp_path / "home"
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        resolved_home=home,
    )

    assert settings.data_dir == data_dir.resolve()
    assert settings.resolved_home == home.resolve()
    assert settings.database_path == data_dir.resolve() / "copilotd.sqlite3"

    settings.ensure_directories()

    assert settings.capability_path.parent.is_dir()
    assert (settings.data_dir / "sessions").is_dir()
    assert (settings.data_dir / "runtime").is_dir()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_lease_ttl_seconds", 9),
        ("owner_lease_ttl_seconds", 39),
        ("owner_lease_renew_seconds", 0),
        ("ingress_capacity", 0),
        ("reducer_batch_size", 0),
    ],
)
def test_settings_reject_invalid_runtime_limits(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_owner_lease_timing_preserves_headroom_under_renewal_jitter() -> None:
    settings = Settings(_env_file=None)
    assert settings.owner_lease_ttl_seconds == 60
    assert settings.owner_lease_renew_seconds == 15

    with pytest.raises(ValidationError, match="jitter margin"):
        Settings(
            _env_file=None,
            owner_lease_ttl_seconds=60,
            owner_lease_renew_seconds=16,
        )


def test_github_token_uses_sdk_auth_precedence_without_exposing_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "COPILOTD_GITHUB_TOKEN",
        "COPILOT_GITHUB_TOKEN",
        "GH_TOKEN",
        "GITHUB_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GH_TOKEN", "managed-session-token")

    settings = Settings(_env_file=None)

    assert settings.github_token is not None
    assert settings.github_token.get_secret_value() == "managed-session-token"
    assert "managed-session-token" not in repr(settings)
