import json
import os
import sqlite3
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr, ValidationError

from copilotd.config import (
    Settings,
    _quiesce_windows_legacy_services,
    load_settings,
    platform_default_paths,
)


def _create_sqlite(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE marker(value TEXT NOT NULL)")
        connection.execute("INSERT INTO marker(value) VALUES (?)", (value,))
        connection.commit()
    finally:
        connection.close()


def _sqlite_marker(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("SELECT value FROM marker").fetchone()
        return str(row[0])
    finally:
        connection.close()


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
    if os.name == "posix":
        assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(settings.cache_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(settings.log_dir.stat().st_mode) == 0o700


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_lease_ttl_seconds", 9),
        ("owner_lease_renew_seconds", 0),
        ("ingress_capacity", 0),
        ("reducer_batch_size", 0),
    ],
)
def test_settings_reject_invalid_runtime_limits(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_platform_defaults_match_operations_contract(tmp_path: Path) -> None:
    mac = platform_default_paths("darwin", home=tmp_path)
    windows = platform_default_paths(
        "win32",
        home=tmp_path,
        environ={"LOCALAPPDATA": str(tmp_path / "本地数据")},
    )

    assert mac == (
        tmp_path / "Library" / "Application Support" / "copilotd",
        tmp_path / "Library" / "Caches" / "copilotd",
        tmp_path / "Library" / "Logs" / "copilotd",
    )
    assert windows == (
        tmp_path / "本地数据" / "copilotd" / "state",
        tmp_path / "本地数据" / "copilotd" / "cache",
        tmp_path / "本地数据" / "copilotd" / "logs",
    )


def test_windows_legacy_state_is_atomically_adopted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    _create_sqlite(legacy / "copilotd.sqlite3", "legacy-db")
    (legacy / "sessions").mkdir()
    (legacy / "sessions" / "session.json").write_text("durable", encoding="utf-8")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )
    replacements: list[tuple[Path, Path]] = []
    replace = os.replace

    def recording_replace(source: str | Path, target: str | Path) -> None:
        replacements.append((Path(source), Path(target)))
        replace(source, target)

    monkeypatch.setattr("copilotd.config.os.replace", recording_replace)

    assert settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )

    assert _sqlite_marker(settings.database_path) == "legacy-db"
    assert (settings.data_dir / "sessions" / "session.json").read_text() == "durable"
    assert replacements[-1] == (
        local_app_data / ".copilotd-legacy-state-migration",
        settings.data_dir,
    )
    assert not (local_app_data / ".copilotd-layout-migration.json").exists()


def test_windows_legacy_adoption_recovers_staged_tree(tmp_path: Path) -> None:
    local_app_data = tmp_path / "Local"
    staging = local_app_data / ".copilotd-legacy-state-migration"
    staging.mkdir(parents=True)
    _create_sqlite(staging / "copilotd.sqlite3", "staged-db")
    (local_app_data / ".copilotd-layout-migration.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "phase": "staged",
                "staging": str(staging),
            }
        ),
        encoding="utf-8",
    )
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )

    assert settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    assert _sqlite_marker(settings.database_path) == "staged-db"


def test_windows_split_layout_merges_legacy_and_new_state(tmp_path: Path) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    _create_sqlite(legacy / "copilotd.sqlite3", "legacy-db")
    (legacy / "sessions").mkdir()
    (legacy / "sessions" / "legacy.json").write_text(
        "legacy",
        encoding="utf-8",
    )
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    data_dir.mkdir(parents=True)
    (data_dir / "worktrees").mkdir()
    (data_dir / "worktrees" / "new.json").write_text(
        "new",
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )

    assert settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )

    assert _sqlite_marker(settings.database_path) == "legacy-db"
    assert (data_dir / "sessions" / "legacy.json").read_text() == "legacy"
    assert (data_dir / "worktrees" / "new.json").read_text() == "new"
    assert not (legacy / "copilotd.sqlite3").exists()
    assert not (legacy / "sessions").exists()


def test_windows_split_layout_fails_closed_on_conflicting_databases(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    legacy_db = legacy / "copilotd.sqlite3"
    _create_sqlite(legacy_db, "legacy-db")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    data_dir.mkdir(parents=True)
    target_db = data_dir / "copilotd.sqlite3"
    _create_sqlite(target_db, "different-db")
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )

    with pytest.raises(RuntimeError, match="state conflict"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
        )

    assert _sqlite_marker(legacy_db) == "legacy-db"
    assert _sqlite_marker(target_db) == "different-db"
    assert not (
        local_app_data / ".copilotd-legacy-state-migration"
    ).exists()


def test_windows_migration_quiesces_service_before_first_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    legacy_db = legacy / "copilotd.sqlite3"
    _create_sqlite(legacy_db, "legacy-db")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )
    events: list[str] = []
    replace = os.replace

    def quiesce(databases: tuple[Path, ...]) -> None:
        assert legacy_db in databases
        assert legacy_db.exists()
        events.append("quiesced")

    def observed_replace(source: str | Path, target: str | Path) -> None:
        if Path(source).name == "copilotd.sqlite3":
            events.append("moved")
        replace(source, target)

    monkeypatch.setattr("copilotd.config.os.replace", observed_replace)
    settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=quiesce,
    )

    assert events.index("quiesced") < events.index("moved")


def test_windows_migration_quiesce_failure_moves_nothing(tmp_path: Path) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    legacy_db = legacy / "copilotd.sqlite3"
    _create_sqlite(legacy_db, "legacy-db")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )

    def fail_quiesce(_databases: tuple[Path, ...]) -> None:
        raise RuntimeError("legacy process still writing")

    with pytest.raises(RuntimeError, match="still writing"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=fail_quiesce,
        )

    assert _sqlite_marker(legacy_db) == "legacy-db"
    assert not data_dir.exists()
    assert not (
        local_app_data / ".copilotd-legacy-state-migration"
    ).exists()


def test_windows_migration_rejects_active_sqlite_writer(tmp_path: Path) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy.mkdir(parents=True)
    legacy_db = legacy / "copilotd.sqlite3"
    connection = sqlite3.connect(legacy_db)
    connection.execute("CREATE TABLE durable(value TEXT)")
    connection.commit()
    connection.execute("BEGIN IMMEDIATE")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )
    try:
        with pytest.raises(RuntimeError, match="active writer"):
            settings.adopt_legacy_windows_layout(
                platform_name="win32",
                environ={"LOCALAPPDATA": str(local_app_data)},
                home=tmp_path / "home",
                service_quiescer=lambda _databases: None,
            )
    finally:
        connection.rollback()
        connection.close()
    assert legacy_db.exists()


def test_managed_service_never_runs_layout_migration_in_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    legacy_db = legacy / "copilotd.sqlite3"
    _create_sqlite(legacy_db, "legacy")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )
    monkeypatch.setenv("COPILOTD_MANAGED_SERVICE", "1")

    settings.ensure_directories()

    assert _sqlite_marker(legacy_db) == "legacy"
    assert not (
        local_app_data / ".copilotd-legacy-state-migration"
    ).exists()


def test_windows_quiescer_excludes_helper_and_parent_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("copilotd.config.subprocess.run", run)

    _quiesce_windows_legacy_services(())

    script = commands[0][-1]
    assert "$upgraderProcessIds = @($PID," in script
    assert "$upgraderProcessIds -notcontains $_.ProcessId" in script


def test_windows_split_layout_recovers_after_staging_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    (legacy / "sessions").mkdir(parents=True)
    legacy_session = legacy / "sessions" / "legacy.json"
    legacy_session.write_text("legacy", encoding="utf-8")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    data_dir.mkdir(parents=True)
    _create_sqlite(data_dir / "copilotd.sqlite3", "target-db")
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )
    replace = os.replace
    crashed = False

    def crash_after_staging(source: str | Path, target: str | Path) -> None:
        nonlocal crashed
        source_path = Path(source)
        if source_path == legacy / "sessions" and not crashed:
            crashed = True
            raise OSError("simulated migration crash")
        replace(source, target)

    monkeypatch.setattr("copilotd.config.os.replace", crash_after_staging)
    with pytest.raises(OSError, match="simulated migration crash"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
        )
    staging = local_app_data / ".copilotd-legacy-state-migration"
    journal = local_app_data / ".copilotd-layout-migration.json"
    assert staging.exists() and journal.exists()
    assert not data_dir.exists()

    monkeypatch.setattr("copilotd.config.os.replace", replace)
    assert settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "target-db"
    assert (data_dir / "sessions" / "legacy.json").read_text() == "legacy"
    assert not staging.exists() and not journal.exists()


def test_service_secret_is_private_and_loadable_without_plaintext_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        discord_token=SecretStr("private-token"),
    )
    settings.ensure_directories()
    path = settings.write_service_secrets()
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["discord_token"] == "private-token"
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    monkeypatch.setenv("COPILOTD_SERVICE_SECRETS", str(path))
    monkeypatch.delenv("COPILOTD_DISCORD_TOKEN", raising=False)
    loaded = load_settings()
    assert loaded.discord_token is not None
    assert loaded.discord_token.get_secret_value() == "private-token"
    monkeypatch.delenv("COPILOTD_SERVICE_SECRETS")
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(settings.cache_dir))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(settings.log_dir))
    loaded_from_default = load_settings()
    assert loaded_from_default.discord_token is not None
    assert loaded_from_default.discord_token.get_secret_value() == "private-token"


def test_managed_worker_backfills_shared_handoff_token_for_future_manager(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=tmp_path / "logs",
        discord_token=SecretStr("private-token"),
    )
    settings.ensure_directories()
    settings.write_service_secrets()

    worker_settings = settings.ensure_service_handoff_token()

    assert worker_settings.service_handoff_token is not None
    worker_token = (
        worker_settings.service_handoff_token.get_secret_value()
    )
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(settings.cache_dir))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(settings.log_dir))
    manager_settings = load_settings()
    assert manager_settings.service_handoff_token is not None
    assert (
        manager_settings.service_handoff_token.get_secret_value()
        == worker_token
    )


def test_insecure_service_secret_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name != "posix":
        pytest.skip("POSIX mode bits are not available")
    secret = tmp_path / "secrets.json"
    secret.write_text(
        json.dumps({"schema_version": 1, "discord_token": "secret"}),
        encoding="utf-8",
    )
    secret.chmod(0o644)
    monkeypatch.setenv("COPILOTD_SERVICE_SECRETS", str(secret))
    monkeypatch.delenv("COPILOTD_DISCORD_TOKEN", raising=False)
    with pytest.raises(ValueError, match="must not be group/world accessible"):
        load_settings()
