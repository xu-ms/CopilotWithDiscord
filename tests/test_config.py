import hashlib
import json
import os
import shutil
import sqlite3
import stat
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from copilotd.config import (
    Settings,
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


class _TestMigrationGuard:
    def __init__(self, connections: dict[Path, sqlite3.Connection]) -> None:
        self.connections = connections
        self.finalized: list[Path] = []
        self.swap_prepared = False
        self.released = False

    def stage_database(self, source: Path, target: Path) -> None:
        if target.exists():
            assert source.read_bytes() == target.read_bytes()
            return
        shutil.copy2(source, target)

    def finalize_database(self, database: Path) -> None:
        self.finalized.append(database)

    def prepare_swap(self) -> None:
        if self.swap_prepared:
            return
        self.swap_prepared = True
        for connection in self.connections.values():
            connection.rollback()
            connection.close()
        self.connections.clear()

    def release(self) -> None:
        if self.released:
            return
        self.prepare_swap()
        self.released = True


def _test_migration_guard(
    databases: tuple[Path, ...],
) -> _TestMigrationGuard:
    connections: dict[Path, sqlite3.Connection] = {}
    try:
        for database in databases:
            connection = sqlite3.connect(database, timeout=0)
            connection.execute("BEGIN EXCLUSIVE")
            connections[database.resolve()] = connection
    except BaseException as error:
        for connection in connections.values():
            connection.rollback()
            connection.close()
        raise RuntimeError("legacy SQLite database still has an active writer") from error
    return _TestMigrationGuard(connections)


def _complete_adoption(adoption: object) -> bool:
    assert adoption is not None
    adoption.complete()  # type: ignore[union-attr]
    return True


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
        ("owner_lease_ttl_seconds", 39),
        ("owner_lease_renew_seconds", 0),
        ("ingress_capacity", 0),
        ("reducer_batch_size", 0),
        ("interaction_timeout_seconds", 0),
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


def test_interaction_timeout_defaults_to_design_value_and_is_configurable() -> None:
    assert Settings(_env_file=None).interaction_timeout_seconds == 15 * 60
    assert (
        Settings(
            _env_file=None,
            interaction_timeout_seconds=120,
        ).interaction_timeout_seconds
        == 120
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

    assert _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )
    )

    assert _sqlite_marker(settings.database_path) == "legacy-db"
    assert (settings.data_dir / "sessions" / "session.json").read_text() == "durable"
    assert (
        local_app_data / ".copilotd-legacy-state-migration",
        settings.data_dir,
    ) in replacements
    assert not (local_app_data / ".copilotd-layout-migration.json").exists()


def test_windows_migration_journal_persists_until_install_verification(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy_db = local_app_data / "copilotD" / "copilotd.sqlite3"
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

    adoption = settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=_test_migration_guard,
        managed_replacement_token_hash="expected-manager-hash",
    )

    assert adoption is not None
    journal = local_app_data / ".copilotd-layout-migration.json"
    assert json.loads(journal.read_text())["phase"] == ("swapped_pending_install")
    assert (
        json.loads(journal.read_text())["managed_replacement_token_hash"] == "expected-manager-hash"
    )
    assert settings.windows_legacy_layout_pending(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    adoption.abandon()
    assert journal.exists()
    resumed = settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=_test_migration_guard,
    )
    assert resumed is not None
    resumed.complete()
    assert not journal.exists()
    assert not settings.windows_legacy_layout_pending(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )


def test_swapped_layout_authorizes_only_expected_managed_replacement(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    home = tmp_path / "home"
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=home,
    )
    data_dir.mkdir(parents=True)
    token = "expected-handoff-token"
    journal = local_app_data / ".copilotd-layout-migration.json"
    journal.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "phase": "swapped_pending_install",
                "target": str(data_dir),
                "managed_replacement_token_hash": hashlib.sha256(token.encode()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=home,
        service_handoff_token=SecretStr(token),
    )
    managed_environment = {
        "LOCALAPPDATA": str(local_app_data),
        "COPILOTD_MANAGED_SERVICE": "1",
    }

    assert settings.windows_migration_replacement_authorized(
        platform_name="win32",
        environ=managed_environment,
        home=home,
    )
    assert not settings.model_copy(
        update={"service_handoff_token": SecretStr("wrong-token")}
    ).windows_migration_replacement_authorized(
        platform_name="win32",
        environ=managed_environment,
        home=home,
    )
    assert not settings.windows_migration_replacement_authorized(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=home,
    )


def test_completed_split_layout_is_not_legacy_on_case_insensitive_path(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    target_root = local_app_data / "copilotd"
    for name in ("state", "cache", "logs"):
        (target_root / name).mkdir(parents=True, exist_ok=True)
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

    assert not settings.windows_legacy_layout_pending(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )


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

    assert _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )
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

    assert _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )
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
    assert not (local_app_data / ".copilotd-legacy-state-migration").exists()


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

    def quiesce(databases: tuple[Path, ...]) -> _TestMigrationGuard:
        assert legacy_db in databases
        assert legacy_db.exists()
        events.append("quiesced")
        guard = _test_migration_guard(databases)
        stage_database = guard.stage_database

        def observed_stage(source: Path, target: Path) -> None:
            events.append("moved")
            stage_database(source, target)

        monkeypatch.setattr(guard, "stage_database", observed_stage)
        return guard

    _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=quiesce,
        )
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

    def fail_quiesce(
        _databases: tuple[Path, ...],
    ) -> _TestMigrationGuard:
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
    assert not (local_app_data / ".copilotd-legacy-state-migration").exists()


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
                service_quiescer=_test_migration_guard,
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
    assert not (local_app_data / ".copilotd-legacy-state-migration").exists()


def test_unmanaged_directory_creation_never_runs_layout_migration(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy = local_app_data / "copilotD"
    _create_sqlite(legacy / "copilotd.sqlite3", "legacy")
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

    settings.ensure_directories()

    assert _sqlite_marker(legacy / "copilotd.sqlite3") == "legacy"
    assert settings.windows_legacy_layout_pending(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )


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
            service_quiescer=_test_migration_guard,
        )
    staging = local_app_data / ".copilotd-legacy-state-migration"
    journal = local_app_data / ".copilotd-layout-migration.json"
    assert staging.exists() and journal.exists()
    assert data_dir.exists()

    monkeypatch.setattr("copilotd.config.os.replace", replace)
    assert _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )
    )
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "target-db"
    assert (data_dir / "sessions" / "legacy.json").read_text() == "legacy"
    assert not staging.exists() and not journal.exists()


def test_windows_database_copy_crash_deletes_partial_stage_on_retry(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy_db = local_app_data / "copilotD" / "copilotd.sqlite3"
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
    staging = local_app_data / ".copilotd-legacy-state-migration"
    journal = local_app_data / ".copilotd-layout-migration.json"
    partial_artifact = staging / ".copilotd.sqlite3.interrupted.staging"

    def interrupted_quiescer(
        databases: tuple[Path, ...],
    ) -> _TestMigrationGuard:
        guard = _test_migration_guard(databases)

        def interrupted_stage(_source: Path, target: Path) -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"partial database")
            partial_artifact.write_bytes(b"partial temporary")
            raise OSError("simulated database staging crash")

        guard.stage_database = interrupted_stage  # type: ignore[method-assign]
        return guard

    with pytest.raises(OSError, match="database staging crash"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=interrupted_quiescer,
        )

    assert json.loads(journal.read_text())["phase"] == "database_staging"
    assert (staging / "copilotd.sqlite3").read_bytes() == b"partial database"
    assert partial_artifact.exists()

    adoption = settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=_test_migration_guard,
    )
    assert adoption is not None
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "legacy"
    assert not partial_artifact.exists()
    adoption.complete()


def test_windows_staged_phase_recovers_partial_database_from_old_copy_order(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy_db = local_app_data / "copilotD" / "copilotd.sqlite3"
    _create_sqlite(legacy_db, "legacy")
    data_dir, cache_dir, log_dir = platform_default_paths(
        "win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
    )
    staging = local_app_data / ".copilotd-legacy-state-migration"
    staging.mkdir()
    (staging / "copilotd.sqlite3").write_bytes(b"partial database")
    (local_app_data / ".copilotd-layout-migration.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "phase": "staged",
                "legacy": str(legacy_db.parent),
                "staging": str(staging),
                "target": str(data_dir),
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        data_dir=data_dir,
        cache_dir=cache_dir,
        log_dir=log_dir,
        resolved_home=tmp_path / "home",
    )

    with pytest.raises(RuntimeError, match=r"restricted to.*setup"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
        )
    assert (staging / "copilotd.sqlite3").read_bytes() == (b"partial database")

    adoption = settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=_test_migration_guard,
    )

    assert adoption is not None
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "legacy"
    adoption.complete()


def test_windows_migration_recovers_authoritative_handoff_stage(
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

    def quiesce(databases: tuple[Path, ...]) -> _TestMigrationGuard:
        guard = _test_migration_guard(databases)

        def finalize(database: Path) -> None:
            connection = sqlite3.connect(database)
            try:
                connection.execute("UPDATE marker SET value = 'handoff-staged'")
                connection.commit()
            finally:
                connection.close()

        guard.finalize_database = finalize  # type: ignore[method-assign]
        return guard

    replace = os.replace
    staging = local_app_data / ".copilotd-legacy-state-migration"
    crashed = False

    def crash_before_authoritative_swap(
        source: str | Path,
        target: str | Path,
    ) -> None:
        nonlocal crashed
        if Path(source) == staging and Path(target) == data_dir and not crashed:
            crashed = True
            raise OSError("simulated crash before authoritative swap")
        replace(source, target)

    monkeypatch.setattr(
        "copilotd.config.os.replace",
        crash_before_authoritative_swap,
    )
    with pytest.raises(OSError, match="authoritative swap"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=quiesce,
        )
    journal = local_app_data / ".copilotd-layout-migration.json"
    assert json.loads(journal.read_text())["phase"] == "swap_started"
    assert _sqlite_marker(staging / "copilotd.sqlite3") == "handoff-staged"

    monkeypatch.setattr("copilotd.config.os.replace", replace)
    assert _complete_adoption(
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )
    )
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "handoff-staged"
    assert not staging.exists()
    assert not journal.exists()


def test_windows_migration_recovers_crash_after_atomic_tree_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy_db = local_app_data / "copilotD" / "copilotd.sqlite3"
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
    staging = local_app_data / ".copilotd-legacy-state-migration"
    journal = local_app_data / ".copilotd-layout-migration.json"
    replace = os.replace
    crashed = False

    def crash_after_swap(source: str | Path, target: str | Path) -> None:
        nonlocal crashed
        source_path = Path(source)
        target_path = Path(target)
        if source_path == staging and target_path == data_dir and not crashed:
            crashed = True
            replace(source, target)
            raise OSError("simulated crash after atomic tree swap")
        replace(source, target)

    monkeypatch.setattr("copilotd.config.os.replace", crash_after_swap)
    with pytest.raises(OSError, match="after atomic tree swap"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
            service_quiescer=_test_migration_guard,
        )

    assert not staging.exists()
    assert _sqlite_marker(data_dir / "copilotd.sqlite3") == "legacy"
    assert json.loads(journal.read_text())["phase"] == "swap_started"

    monkeypatch.setattr("copilotd.config.os.replace", replace)
    adoption = settings.adopt_legacy_windows_layout(
        platform_name="win32",
        environ={"LOCALAPPDATA": str(local_app_data)},
        home=tmp_path / "home",
        service_quiescer=_test_migration_guard,
    )
    assert adoption is not None
    assert json.loads(journal.read_text())["phase"] == ("swapped_pending_install")
    adoption.complete()
    assert not journal.exists()


def test_windows_migration_without_setup_guard_fails_before_move(
    tmp_path: Path,
) -> None:
    local_app_data = tmp_path / "Local"
    legacy_db = local_app_data / "copilotD" / "copilotd.sqlite3"
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

    with pytest.raises(RuntimeError, match=r"restricted to.*setup"):
        settings.adopt_legacy_windows_layout(
            platform_name="win32",
            environ={"LOCALAPPDATA": str(local_app_data)},
            home=tmp_path / "home",
        )
    assert _sqlite_marker(legacy_db) == "legacy"
    assert not data_dir.exists()


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
        github_token=SecretStr("github-private-token"),
    )
    settings.ensure_directories()
    path = settings.write_service_secrets()
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["discord_token"] == "private-token"
    assert payload["github_token"] == "github-private-token"
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    monkeypatch.setenv("COPILOTD_SERVICE_SECRETS", str(path))
    monkeypatch.delenv("COPILOTD_DISCORD_TOKEN", raising=False)
    for name in (
        "COPILOTD_GITHUB_TOKEN",
        "COPILOT_GITHUB_TOKEN",
        "GH_TOKEN",
        "GITHUB_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    loaded = load_settings()
    assert loaded.discord_token is not None
    assert loaded.github_token is not None
    assert loaded.discord_token.get_secret_value() == "private-token"
    assert loaded.github_token.get_secret_value() == "github-private-token"
    monkeypatch.delenv("COPILOTD_SERVICE_SECRETS")
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(settings.cache_dir))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(settings.log_dir))
    loaded_from_default = load_settings()
    assert loaded_from_default.discord_token is not None
    assert loaded_from_default.github_token is not None
    assert loaded_from_default.discord_token.get_secret_value() == "private-token"
    assert loaded_from_default.github_token.get_secret_value() == "github-private-token"


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
    worker_token = worker_settings.service_handoff_token.get_secret_value()
    monkeypatch.setenv("COPILOTD_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("COPILOTD_CACHE_DIR", str(settings.cache_dir))
    monkeypatch.setenv("COPILOTD_LOG_DIR", str(settings.log_dir))
    manager_settings = load_settings()
    assert manager_settings.service_handoff_token is not None
    assert manager_settings.service_handoff_token.get_secret_value() == worker_token


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
