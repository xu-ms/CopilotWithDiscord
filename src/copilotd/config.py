from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

from platformdirs import user_cache_path, user_data_path, user_log_path
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from copilotd.storage.leases import (
    MUTATION_HEADROOM_SECONDS,
    OWNER_LEASE_RENEW_SECONDS,
    OWNER_LEASE_TTL_SECONDS,
    RENEWAL_JITTER_MARGIN_SECONDS,
)


class Settings(BaseSettings):
    """Process configuration loaded only from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="COPILOTD_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    data_dir: Path = Field(default_factory=lambda: user_data_path("copilotD", ensure_exists=False))
    cache_dir: Path = Field(
        default_factory=lambda: user_cache_path("copilotD", ensure_exists=False)
    )
    log_dir: Path = Field(default_factory=lambda: user_log_path("copilotD", ensure_exists=False))
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
    discord_operator_ids: str = ""
    mention_required: bool = False
    log_level: str = "INFO"
    sdk_log_level: str = "info"
    sdk_no_auto_login: bool = False
    sdk_shutdown_timeout_seconds: float = 10
    runtime_uri: str | None = None
    runtime_connection_token: SecretStr | None = None
    owner_lease_ttl_seconds: int = int(OWNER_LEASE_TTL_SECONDS)
    owner_lease_renew_seconds: int = int(OWNER_LEASE_RENEW_SECONDS)
    ingress_capacity: int = 4096
    reducer_batch_size: int = 64
    attachment_file_max_bytes: int = 25 * 1024 * 1024
    attachment_message_max_bytes: int = 100 * 1024 * 1024
    attachment_blob_max_bytes: int = 7 * 1024 * 1024
    attachment_runtime_frame_max_bytes: int = 7 * 1024 * 1024
    discord_upload_max_bytes: int = 7 * 1024 * 1024

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
        "ingress_capacity",
        "reducer_batch_size",
        "attachment_file_max_bytes",
        "attachment_message_max_bytes",
        "attachment_blob_max_bytes",
        "attachment_runtime_frame_max_bytes",
        "discord_upload_max_bytes",
    )
    @classmethod
    def validate_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("queue and batch sizes must be positive")
        return value

    @property
    def service_secrets_path(self) -> Path:
        return self.data_dir / "config" / "service-secrets.json"

    def write_service_secrets(self) -> Path:
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
        }
        _atomic_private_json(self.service_secrets_path, payload)
        return self.service_secrets_path

    @property
    def database_path(self) -> Path:
        return self.data_dir / "copilotd.sqlite3"

    @property
    def capability_path(self) -> Path:
        return self.data_dir / "cache" / "event-fixtures" / "capabilities.json"

    def ensure_directories(self) -> None:
        for relative in (
            Path("runtime"),
            Path("sessions"),
            Path("worktrees"),
            Path("logs"),
            Path("cache/event-fixtures"),
        ):
            (self.data_dir / relative).mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def heartbeat_path(self) -> Path:
        return self.cache_dir / "heartbeat.json"

    @property
    def operator_user_ids(self) -> frozenset[int]:
        try:
            return frozenset(
                int(item.strip()) for item in self.discord_operator_ids.split(",") if item.strip()
            )
        except ValueError as error:
            raise ValueError(
                "COPILOTD_DISCORD_OPERATOR_IDS must contain comma-separated user IDs"
            ) from error


def load_settings() -> Settings:
    """Load environment settings plus the private service credential source."""

    settings = Settings()
    configured_path = os.environ.get("COPILOTD_SERVICE_SECRETS")
    secret_path = (
        settings.service_secrets_path
        if configured_path is None
        else Path(configured_path).expanduser().resolve()
    )
    if configured_path is None and not secret_path.exists():
        return settings
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
    for field in ("discord_token", "github_token", "runtime_connection_token"):
        if getattr(settings, field) is None and payload.get(field):
            updates[field] = SecretStr(str(payload[field]))
    return settings.model_copy(update=updates)


def _atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        path.parent.chmod(0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "posix":
            temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
