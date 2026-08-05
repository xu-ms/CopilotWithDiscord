from __future__ import annotations

from pathlib import Path

from platformdirs import user_cache_path, user_data_path, user_log_path
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process configuration loaded only from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="COPILOTD_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    data_dir: Path = Field(
        default_factory=lambda: user_data_path("copilotD", ensure_exists=False)
    )
    cache_dir: Path = Field(
        default_factory=lambda: user_cache_path("copilotD", ensure_exists=False)
    )
    log_dir: Path = Field(default_factory=lambda: user_log_path("copilotD", ensure_exists=False))
    resolved_home: Path = Field(default_factory=lambda: Path.home().expanduser().resolve())
    discord_token: SecretStr | None = None
    discord_guild_id: int | None = None
    mention_required: bool = False
    log_level: str = "INFO"
    sdk_log_level: str = "info"
    sdk_shutdown_timeout_seconds: float = 10
    runtime_uri: str | None = None
    runtime_connection_token: SecretStr | None = None
    owner_lease_ttl_seconds: int = 60
    owner_lease_renew_seconds: int = 20
    ingress_capacity: int = 4096
    reducer_batch_size: int = 64
    attachment_file_max_bytes: int = 25 * 1024 * 1024
    attachment_message_max_bytes: int = 100 * 1024 * 1024
    attachment_blob_max_bytes: int = 7 * 1024 * 1024
    discord_upload_max_bytes: int = 7 * 1024 * 1024

    @field_validator("data_dir", "cache_dir", "log_dir", "resolved_home", mode="before")
    @classmethod
    def resolve_path(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("owner_lease_ttl_seconds")
    @classmethod
    def validate_lease_ttl(cls, value: int) -> int:
        if value < 40:
            raise ValueError("owner lease TTL must be at least 40 seconds")
        return value

    @field_validator("owner_lease_renew_seconds")
    @classmethod
    def validate_lease_renew(cls, value: int) -> int:
        if value < 1:
            raise ValueError("owner lease renew interval must be positive")
        return value

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
        "discord_upload_max_bytes",
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
