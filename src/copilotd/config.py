from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from platformdirs import user_cache_path, user_data_path, user_log_path
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from copilotd.storage.leases import (
    MUTATION_HEADROOM_SECONDS,
    OWNER_LEASE_RENEW_SECONDS,
    OWNER_LEASE_TTL_SECONDS,
    RENEWAL_JITTER_MARGIN_SECONDS,
)


class LegacyLayoutMigrationGuard(Protocol):
    def stage_database(self, source: Path, target: Path) -> None: ...

    def finalize_database(self, database: Path) -> None: ...

    def prepare_swap(self) -> None: ...

    def release(self) -> None: ...


@dataclass(slots=True)
class LegacyLayoutAdoption:
    journal_path: Path
    guard: LegacyLayoutMigrationGuard
    _closed: bool = False

    def complete(self) -> None:
        if self._closed:
            return
        try:
            self.journal_path.unlink(missing_ok=True)
        finally:
            try:
                self.guard.release()
            finally:
                self._closed = True

    def abandon(self) -> None:
        if self._closed:
            return
        try:
            self.guard.release()
        finally:
            self._closed = True


def platform_default_paths(
    platform_name: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    home: Path | None = None,
) -> tuple[Path, Path, Path]:
    """Return the design-fixed per-user state, cache, and log directories."""

    effective_platform = sys.platform if platform_name is None else platform_name
    effective_environ = os.environ if environ is None else environ
    effective_home = Path.home() if home is None else home
    if effective_platform == "darwin":
        return (
            effective_home / "Library" / "Application Support" / "copilotd",
            effective_home / "Library" / "Caches" / "copilotd",
            effective_home / "Library" / "Logs" / "copilotd",
        )
    if effective_platform == "win32":
        local_app_data = Path(
            effective_environ.get(
                "LOCALAPPDATA",
                str(effective_home / "AppData" / "Local"),
            )
        )
        root = local_app_data / "copilotd"
        return root / "state", root / "cache", root / "logs"
    return (
        Path(user_data_path("copilotd", ensure_exists=False)),
        Path(user_cache_path("copilotd", ensure_exists=False)),
        Path(user_log_path("copilotd", ensure_exists=False)),
    )


def _default_data_dir() -> Path:
    return platform_default_paths()[0]


def _default_cache_dir() -> Path:
    return platform_default_paths()[1]


def _default_log_dir() -> Path:
    return platform_default_paths()[2]


class Settings(BaseSettings):
    """Process configuration loaded only from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="COPILOTD_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    data_dir: Path = Field(default_factory=_default_data_dir)
    cache_dir: Path = Field(default_factory=_default_cache_dir)
    log_dir: Path = Field(default_factory=_default_log_dir)
    resolved_home: Path = Field(default_factory=lambda: Path.home().expanduser().resolve())
    discord_token: SecretStr | None = None
    github_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "COPILOTD_GITHUB_TOKEN",
            "COPILOT_GITHUB_TOKEN",
            "GH_TOKEN",
            "GITHUB_TOKEN",
        ),
    )
    discord_guild_id: int | None = None
    mention_required: bool = False
    log_level: str = "INFO"
    sdk_log_level: str = "info"
    sdk_no_auto_login: bool = False
    sdk_shutdown_timeout_seconds: float = 10
    runtime_uri: str | None = None
    runtime_connection_token: SecretStr | None = None
    service_handoff_token: SecretStr | None = None
    owner_lease_ttl_seconds: int = int(OWNER_LEASE_TTL_SECONDS)
    owner_lease_renew_seconds: int = int(OWNER_LEASE_RENEW_SECONDS)
    ingress_capacity: int = 4096
    reducer_batch_size: int = 64
    interaction_timeout_seconds: float = 15 * 60
    attachment_file_max_bytes: int = 25 * 1024 * 1024
    attachment_message_max_bytes: int = 100 * 1024 * 1024
    attachment_blob_max_bytes: int = 7 * 1024 * 1024
    attachment_runtime_frame_max_bytes: int = 7 * 1024 * 1024
    discord_upload_max_bytes: int = 7 * 1024 * 1024
    discord_requests_per_second: float = 20.0
    discord_request_burst: int = 10
    discord_route_requests_per_second: float = 5.0
    discord_route_burst: int = 5
    discord_request_queue_limit: int = 512
    discord_interaction_deadline_seconds: float = 2.5
    discord_stream_edit_interval_seconds: float = 1.0
    discord_taskdeck_edit_interval_seconds: float = 4.0
    discord_reaction_interval_seconds: float = 0.25
    discord_request_transient_attempts: int = 3
    heartbeat_interval_seconds: float = 30
    heartbeat_stale_seconds: float = 120
    gateway_down_restart_seconds: float = 600
    resume_suppression_seconds: float = 60
    setup_verify_timeout_seconds: float = 45
    restart_drain_timeout_seconds: float = 15
    service_startup_grace_seconds: float = 120

    @field_validator("data_dir", "cache_dir", "log_dir", "resolved_home", mode="before")
    @classmethod
    def resolve_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("owner_lease_ttl_seconds")
    @classmethod
    def validate_lease_ttl(cls, value: int) -> int:
        if value < MUTATION_HEADROOM_SECONDS + RENEWAL_JITTER_MARGIN_SECONDS:
            raise ValueError("owner lease TTL must exceed mutation headroom by the jitter margin")
        return value

    @field_validator("owner_lease_renew_seconds")
    @classmethod
    def validate_lease_renew(cls, value: int) -> int:
        if value < 1:
            raise ValueError("owner lease renew interval must be positive")
        return value

    @model_validator(mode="after")
    def validate_lease_timing_policy(self) -> Settings:
        required = MUTATION_HEADROOM_SECONDS + RENEWAL_JITTER_MARGIN_SECONDS
        available = self.owner_lease_ttl_seconds - self.owner_lease_renew_seconds
        if available < required:
            raise ValueError(
                "owner lease TTL minus renewal interval must preserve "
                f"at least {required:g} seconds of mutation headroom and jitter margin"
            )
        return self

    @field_validator("sdk_shutdown_timeout_seconds")
    @classmethod
    def validate_sdk_shutdown_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("SDK shutdown timeout must be positive")
        return value

    @field_validator(
        "heartbeat_interval_seconds",
        "heartbeat_stale_seconds",
        "gateway_down_restart_seconds",
        "resume_suppression_seconds",
        "setup_verify_timeout_seconds",
        "restart_drain_timeout_seconds",
        "service_startup_grace_seconds",
        "interaction_timeout_seconds",
        "discord_requests_per_second",
        "discord_route_requests_per_second",
        "discord_interaction_deadline_seconds",
        "discord_stream_edit_interval_seconds",
        "discord_taskdeck_edit_interval_seconds",
        "discord_reaction_interval_seconds",
    )
    @classmethod
    def validate_positive_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("service timing values must be positive")
        return value

    @field_validator(
        "ingress_capacity",
        "reducer_batch_size",
        "attachment_file_max_bytes",
        "attachment_message_max_bytes",
        "attachment_blob_max_bytes",
        "attachment_runtime_frame_max_bytes",
        "discord_upload_max_bytes",
        "discord_request_burst",
        "discord_route_burst",
        "discord_request_queue_limit",
        "discord_request_transient_attempts",
    )
    @classmethod
    def validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("queue and batch sizes must be positive")
        return value

    @property
    def database_path(self) -> Path:
        return self.data_dir / "copilotd.sqlite3"

    @property
    def capability_path(self) -> Path:
        return self.data_dir / "cache" / "event-fixtures" / "capabilities.json"

    def windows_legacy_layout_pending(
        self,
        *,
        platform_name: str | None = None,
        environ: Mapping[str, str] | None = None,
        home: Path | None = None,
    ) -> bool:
        effective_platform = sys.platform if platform_name is None else platform_name
        if effective_platform != "win32":
            return False
        effective_environ = os.environ if environ is None else environ
        effective_home = self.resolved_home if home is None else home
        expected_data, _, _ = platform_default_paths(
            "win32",
            environ=effective_environ,
            home=effective_home,
        )
        if self.data_dir != expected_data.expanduser().resolve():
            return False
        local_app_data = expected_data.parent.parent
        legacy_root = local_app_data / "copilotD"
        excluded = {
            expected_data,
            self.cache_dir,
            self.log_dir,
            local_app_data / ".copilotd-legacy-state-migration",
            local_app_data / ".copilotd-layout-migration.json",
            local_app_data / ".copilotd-layout-migration.lock",
        }
        return (
            bool(_legacy_state_entries(legacy_root, excluded=excluded))
            or (local_app_data / ".copilotd-legacy-state-migration").exists()
            or (local_app_data / ".copilotd-layout-migration.json").exists()
        )

    def windows_migration_replacement_authorized(
        self,
        *,
        platform_name: str | None = None,
        environ: Mapping[str, str] | None = None,
        home: Path | None = None,
    ) -> bool:
        effective_platform = sys.platform if platform_name is None else platform_name
        effective_environ = os.environ if environ is None else environ
        if (
            effective_platform != "win32"
            or effective_environ.get("COPILOTD_MANAGED_SERVICE") != "1"
            or self.service_handoff_token is None
        ):
            return False
        effective_home = self.resolved_home if home is None else home
        expected_data, _, _ = platform_default_paths(
            "win32",
            environ=effective_environ,
            home=effective_home,
        )
        expected_data = expected_data.expanduser().resolve()
        if self.data_dir != expected_data:
            return False
        journal = expected_data.parent.parent / ".copilotd-layout-migration.json"
        migration = _read_private_json(journal)
        if migration is None or migration.get("phase") != "swapped_pending_install":
            return False
        target = migration.get("target")
        expected_hash = migration.get("managed_replacement_token_hash")
        if not isinstance(target, str) or not isinstance(expected_hash, str):
            return False
        target_key = str(Path(target).expanduser().resolve()).replace("/", "\\").casefold()
        expected_key = str(expected_data).replace("/", "\\").casefold()
        if target_key != expected_key:
            return False
        actual_hash = hashlib.sha256(
            self.service_handoff_token.get_secret_value().encode()
        ).hexdigest()
        return hmac.compare_digest(actual_hash, expected_hash)

    def ensure_directories(self) -> None:
        directories = [
            self.data_dir,
            self.cache_dir,
            self.log_dir,
        ]
        directories.extend(
            self.data_dir / relative
            for relative in (
                Path("config"),
                Path("runtime"),
                Path("sessions"),
                Path("worktrees"),
                Path("cache/event-fixtures"),
            )
        )
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            if os.name == "posix":
                directory.chmod(0o700)

    def adopt_legacy_windows_layout(
        self,
        *,
        platform_name: str | None = None,
        environ: Mapping[str, str] | None = None,
        home: Path | None = None,
        service_quiescer: (Callable[[tuple[Path, ...]], LegacyLayoutMigrationGuard] | None) = None,
        managed_replacement_token_hash: str | None = None,
    ) -> LegacyLayoutAdoption | None:
        effective_platform = sys.platform if platform_name is None else platform_name
        if effective_platform != "win32":
            return None
        effective_environ = os.environ if environ is None else environ
        effective_home = self.resolved_home if home is None else home
        expected_data, _, _ = platform_default_paths(
            "win32",
            environ=effective_environ,
            home=effective_home,
        )
        if self.data_dir != expected_data.expanduser().resolve():
            return None
        local_app_data = expected_data.parent.parent
        legacy_root = local_app_data / "copilotD"
        target_root = expected_data.parent
        staging = local_app_data / ".copilotd-legacy-state-migration"
        journal = local_app_data / ".copilotd-layout-migration.json"
        lock = local_app_data / ".copilotd-layout-migration.lock"
        local_app_data.mkdir(parents=True, exist_ok=True)
        descriptor = _acquire_private_lock(lock)
        guard: LegacyLayoutMigrationGuard | None = None
        try:
            legacy_source = legacy_root if legacy_root.exists() else target_root
            excluded = {
                expected_data,
                self.cache_dir,
                self.log_dir,
                staging,
                journal,
                lock,
            }
            legacy_entries = _legacy_state_entries(
                legacy_source,
                excluded=excluded,
            )
            migration = _read_private_json(journal)
            migration_phase = None if migration is None else str(migration.get("phase"))
            effective_replacement_token_hash = (
                managed_replacement_token_hash
                if managed_replacement_token_hash is not None
                else (
                    str(migration["managed_replacement_token_hash"])
                    if migration is not None
                    and isinstance(
                        migration.get("managed_replacement_token_hash"),
                        str,
                    )
                    else None
                )
            )
            staged_database = staging / "copilotd.sqlite3"
            source_database_exists = any(
                path.exists()
                for path in {
                    legacy_source / "copilotd.sqlite3",
                    expected_data / "copilotd.sqlite3",
                }
            )
            staged_copy_incomplete = (
                migration_phase == "staged" and staged_database.exists() and source_database_exists
            )
            if (
                migration_phase
                in {
                    "swap_started",
                    "swapped_pending_install",
                }
                and expected_data.is_dir()
                and not staging.exists()
            ):
                if service_quiescer is None:
                    raise RuntimeError(
                        "Windows legacy state migration is restricted to "
                        "`copilotd setup` or `copilotd service install`"
                    )
                databases = (
                    (expected_data / "copilotd.sqlite3",)
                    if (expected_data / "copilotd.sqlite3").exists()
                    else ()
                )
                guard = service_quiescer(databases)
                guard.prepare_swap()
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "swapped_pending_install",
                        "target": str(expected_data),
                        "service_quiesced": True,
                        "durable_handoff": True,
                        "swap_recovered": migration_phase == "swap_started",
                        "managed_replacement_token_hash": effective_replacement_token_hash,
                    },
                )
                adoption = LegacyLayoutAdoption(journal, guard)
                guard = None
                return adoption
            if staging.exists() and expected_data.exists():
                if migration_phase not in {
                    "prepared",
                    "staged",
                    "database_staging",
                    "database_finalizing",
                    "handoff_staged",
                    "swap_started",
                }:
                    raise RuntimeError("Windows state migration has both staged and target trees")
            if staging.exists():
                if migration is None or migration_phase not in {
                    "prepared",
                    "staged",
                    "database_staging",
                    "database_finalizing",
                    "handoff_staged",
                    "swap_started",
                }:
                    raise RuntimeError(
                        "Windows state migration staging exists without a recoverable journal"
                    )
                _assert_merge_compatible(
                    legacy_entries,
                    staging,
                    ignore_database=migration_phase
                    in {
                        "database_staging",
                        "database_finalizing",
                        "handoff_staged",
                        "swap_started",
                    }
                    or staged_copy_incomplete,
                )
            elif not legacy_entries:
                if migration is not None:
                    raise RuntimeError("Windows migration journal has no recoverable state")
                return None
            else:
                if expected_data.exists():
                    _assert_merge_compatible(legacy_entries, expected_data)
            if service_quiescer is None:
                raise RuntimeError(
                    "Windows legacy state migration is restricted to "
                    "`copilotd setup` or `copilotd service install`"
                )
            if staging.exists() and (
                migration_phase == "database_staging" or staged_copy_incomplete
            ):
                _remove_incomplete_staged_database(staging / "copilotd.sqlite3")
            databases = tuple(
                path
                for path in {
                    legacy_source / "copilotd.sqlite3",
                    expected_data / "copilotd.sqlite3",
                }
                if path.exists()
            )
            guard = service_quiescer(databases)
            if not staging.exists():
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "prepared",
                        "legacy": str(legacy_source),
                        "target": str(expected_data),
                        "staging": str(staging),
                        "service_quiesced": True,
                    },
                )
                staging.mkdir(parents=True, exist_ok=False)
            if migration_phase not in {
                "database_staging",
                "database_finalizing",
                "handoff_staged",
                "swap_started",
            }:
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "staged",
                        "legacy": str(legacy_source),
                        "staging": str(staging),
                        "target": str(expected_data),
                        "service_quiesced": True,
                    },
                )
            handoff_staged = migration_phase in {
                "handoff_staged",
                "swap_started",
            }
            authoritative_staged = handoff_staged or (migration_phase == "database_finalizing")
            if databases and not authoritative_staged:
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "database_staging",
                        "legacy": str(legacy_source),
                        "staging": str(staging),
                        "target": str(expected_data),
                        "service_quiesced": True,
                    },
                )
            deferred: list[Path] = []
            if expected_data.exists():
                for entry in sorted(
                    list(expected_data.iterdir()),
                    key=lambda path: path.name.casefold(),
                ):
                    _merge_state_entry(
                        entry,
                        staging / entry.name,
                        guard=guard,
                        deferred=deferred,
                        authoritative_staged=authoritative_staged,
                    )
            legacy_entries = _legacy_state_entries(
                legacy_source,
                excluded=excluded,
            )
            _assert_merge_compatible(
                legacy_entries,
                staging,
                ignore_database=authoritative_staged,
            )
            for entry in legacy_entries:
                _merge_state_entry(
                    entry,
                    staging / entry.name,
                    guard=guard,
                    deferred=deferred,
                    authoritative_staged=authoritative_staged,
                )
            if not handoff_staged and staged_database.exists():
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "database_finalizing",
                        "legacy": str(legacy_source),
                        "staging": str(staging),
                        "target": str(expected_data),
                        "service_quiesced": True,
                    },
                )
                guard.finalize_database(staged_database)
                _atomic_private_json(
                    journal,
                    {
                        "schema_version": 1,
                        "phase": "handoff_staged",
                        "legacy": str(legacy_source),
                        "staging": str(staging),
                        "target": str(expected_data),
                        "service_quiesced": True,
                    },
                )
            guard.prepare_swap()
            for path in deferred:
                path.unlink(missing_ok=True)
            _prune_empty_directories(expected_data)
            if legacy_source != target_root:
                _prune_empty_directories(legacy_source)
            target_root.mkdir(parents=True, exist_ok=True)
            _atomic_private_json(
                journal,
                {
                    "schema_version": 1,
                    "phase": "swap_started",
                    "legacy": str(legacy_source),
                    "staging": str(staging),
                    "target": str(expected_data),
                    "service_quiesced": True,
                    "durable_handoff": True,
                    "managed_replacement_token_hash": effective_replacement_token_hash,
                },
            )
            os.replace(staging, expected_data)
            _atomic_private_json(
                journal,
                {
                    "schema_version": 1,
                    "phase": "swapped_pending_install",
                    "target": str(expected_data),
                    "service_quiesced": True,
                    "durable_handoff": True,
                    "managed_replacement_token_hash": effective_replacement_token_hash,
                },
            )
            adoption = LegacyLayoutAdoption(journal, guard)
            guard = None
            return adoption
        finally:
            if guard is not None:
                guard.release()
            os.close(descriptor)
            lock.unlink(missing_ok=True)

    def validate_directory_security(self) -> list[str]:
        errors: list[str] = []
        for directory in (self.data_dir, self.cache_dir, self.log_dir):
            if not directory.is_dir():
                errors.append(f"{directory}: missing")
                continue
            probe = directory / f".copilotd-write-probe-{uuid.uuid4().hex}"
            try:
                probe.write_text("ok", encoding="ascii")
            except OSError as error:
                errors.append(f"{directory}: not writable ({error})")
            finally:
                probe.unlink(missing_ok=True)
            if os.name == "posix":
                mode = stat.S_IMODE(directory.stat().st_mode)
                if mode & 0o077:
                    errors.append(f"{directory}: permissions must be 0700, found {mode:04o}")
        return errors

    def write_service_secrets(
        self,
        *,
        service_handoff_token: SecretStr | None = None,
    ) -> Path:
        payload = {
            "schema_version": 1,
            "discord_token": (
                None if self.discord_token is None else self.discord_token.get_secret_value()
            ),
            "github_token": (
                None if self.github_token is None else self.github_token.get_secret_value()
            ),
            "runtime_connection_token": (
                None
                if self.runtime_connection_token is None
                else self.runtime_connection_token.get_secret_value()
            ),
            "service_handoff_token": (
                None
                if service_handoff_token is None and self.service_handoff_token is None
                else (service_handoff_token or self.service_handoff_token).get_secret_value()
            ),
        }
        _atomic_private_json(self.service_secrets_path, payload)
        return self.service_secrets_path

    def ensure_service_handoff_token(self) -> Settings:
        if self.service_handoff_token is not None:
            return self
        updated = self.model_copy(update={"service_handoff_token": SecretStr(uuid.uuid4().hex)})
        updated.write_service_secrets(service_handoff_token=updated.service_handoff_token)
        return updated

    @property
    def service_secrets_path(self) -> Path:
        return self.data_dir / "config" / "service-secrets.json"

    @property
    def service_state_path(self) -> Path:
        return self.data_dir / "runtime" / "service-state.json"

    @property
    def watchdog_state_path(self) -> Path:
        return self.data_dir / "runtime" / "watchdog-state.json"

    @property
    def log_paths(self) -> dict[str, Path]:
        return {
            "app": self.log_dir / "copilotd.log",
            "boot": self.log_dir / "boot.log",
            "watchdog": self.log_dir / "watchdog.log",
            "alerts": self.log_dir / "alerts.log",
        }

    @property
    def durable_directories(self) -> tuple[Path, ...]:
        return (
            self.data_dir,
            self.data_dir / "runtime",
            self.data_dir / "sessions",
            self.data_dir / "worktrees",
        )

    @property
    def ephemeral_directories(self) -> tuple[Path, ...]:
        return (self.cache_dir,)

    @property
    def managed_directories(self) -> tuple[Path, ...]:
        return (*self.durable_directories, *self.ephemeral_directories, self.log_dir)

    @property
    def heartbeat_path(self) -> Path:
        return self.cache_dir / "heartbeat.json"


def load_settings() -> Settings:
    """Load environment settings plus the private service-only secret file."""

    settings = Settings()
    settings = _apply_persisted_service_settings(settings)
    secret_path_text = os.environ.get("COPILOTD_SERVICE_SECRETS")
    if secret_path_text is None and not settings.service_secrets_path.exists():
        return settings
    secret_path = (
        settings.service_secrets_path
        if secret_path_text is None
        else Path(secret_path_text).expanduser().resolve()
    )
    if os.name == "posix":
        mode = stat.S_IMODE(secret_path.stat().st_mode)
        if mode & 0o077:
            raise ValueError(
                f"service secret file must not be group/world accessible: {secret_path}"
            )
    payload = json.loads(secret_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError(f"unsupported service secret schema: {secret_path}")
    updates: dict[str, SecretStr] = {}
    for field in (
        "discord_token",
        "github_token",
        "runtime_connection_token",
        "service_handoff_token",
    ):
        if getattr(settings, field) is None and payload.get(field):
            updates[field] = SecretStr(str(payload[field]))
    return settings.model_copy(update=updates)


def _apply_persisted_service_settings(settings: Settings) -> Settings:
    try:
        payload = json.loads(settings.service_state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return settings
    persisted = payload.get("settings")
    if not isinstance(persisted, dict):
        return settings
    fields = {
        "discord_guild_id": "COPILOTD_DISCORD_GUILD_ID",
        "mention_required": "COPILOTD_MENTION_REQUIRED",
        "log_level": "COPILOTD_LOG_LEVEL",
        "sdk_log_level": "COPILOTD_SDK_LOG_LEVEL",
        "runtime_uri": "COPILOTD_RUNTIME_URI",
    }
    updates = {
        field: persisted[field]
        for field, environment_name in fields.items()
        if environment_name not in os.environ and field in persisted
    }
    return settings.model_copy(update=updates)


def _atomic_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name == "posix":
            path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def _legacy_state_entries(
    root: Path,
    *,
    excluded: set[Path],
) -> list[Path]:
    if not root.is_dir():
        return []
    excluded_keys = {str(path).replace("/", "\\").casefold() for path in excluded}
    return sorted(
        (
            entry
            for entry in root.iterdir()
            if str(entry).replace("/", "\\").casefold() not in excluded_keys
            and entry.name
            not in {
                ".copilotd-layout-migration.json",
                ".copilotd-layout-migration.lock",
                ".copilotd-legacy-state-migration",
            }
        ),
        key=lambda path: path.name.casefold(),
    )


def _assert_merge_compatible(
    entries: list[Path],
    target: Path,
    *,
    ignore_database: bool = False,
) -> None:
    conflicts: list[str] = []
    for source in entries:
        _collect_merge_conflicts(
            source,
            target / source.name,
            conflicts,
            ignore_database=ignore_database,
        )
    if conflicts:
        raise RuntimeError("Windows legacy and target state conflict: " + ", ".join(conflicts))


def _collect_merge_conflicts(
    source: Path,
    target: Path,
    conflicts: list[str],
    *,
    ignore_database: bool,
) -> None:
    if ignore_database and _is_sqlite_family(source):
        return
    if not target.exists():
        return
    if source.is_dir() and target.is_dir():
        for child in source.iterdir():
            _collect_merge_conflicts(
                child,
                target / child.name,
                conflicts,
                ignore_database=ignore_database,
            )
        return
    if source.is_file() and target.is_file():
        if _file_digest(source) != _file_digest(target):
            conflicts.append(f"{source.name} differs at {source} and {target}")
        return
    conflicts.append(f"type mismatch at {source} and {target}")


def _merge_state_entry(
    source: Path,
    target: Path,
    *,
    guard: LegacyLayoutMigrationGuard,
    deferred: list[Path],
    authoritative_staged: bool,
) -> None:
    if _is_sqlite_family(source):
        deferred.append(source)
        if source.name == "copilotd.sqlite3" and not authoritative_staged:
            guard.stage_database(source, target)
        return
    if not target.exists():
        os.replace(source, target)
        return
    if source.is_dir() and target.is_dir():
        for child in list(source.iterdir()):
            _merge_state_entry(
                child,
                target / child.name,
                guard=guard,
                deferred=deferred,
                authoritative_staged=authoritative_staged,
            )
        if not any(source.iterdir()):
            source.rmdir()
        return
    if source.is_file() and target.is_file():
        if _file_digest(source) != _file_digest(target):
            raise RuntimeError(f"Windows state changed during migration: {source}")
        source.unlink()
        return
    raise RuntimeError(f"Windows state type changed during migration: {source}")


def _is_sqlite_family(path: Path) -> bool:
    return path.name in {
        "copilotd.sqlite3",
        "copilotd.sqlite3-wal",
        "copilotd.sqlite3-shm",
    }


def _sqlite_family(database: Path) -> tuple[Path, Path, Path]:
    return (
        database,
        database.with_name(database.name + "-wal"),
        database.with_name(database.name + "-shm"),
    )


def _remove_incomplete_staged_database(database: Path) -> None:
    for path in _sqlite_family(database):
        path.unlink(missing_ok=True)
    if database.parent.is_dir():
        for path in database.parent.glob(f".{database.name}.*.staging"):
            path.unlink(missing_ok=True)


def _prune_empty_directories(root: Path) -> None:
    if not root.is_dir():
        return
    for directory, _, _ in os.walk(root, topdown=False):
        path = Path(directory)
        if not any(path.iterdir()):
            path.rmdir()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _read_private_json(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _acquire_private_lock(path: Path) -> int:
    deadline = time.monotonic() + 5
    while True:
        try:
            return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            try:
                stale = time.time() - path.stat().st_mtime > 60
            except FileNotFoundError:
                continue
            if stale:
                path.unlink(missing_ok=True)
                continue
            if time.monotonic() >= deadline:
                raise RuntimeError("Windows layout migration lock is busy") from None
            time.sleep(0.02)
