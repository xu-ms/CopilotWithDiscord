from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog

APP_LOG_MAX_BYTES = 10 * 1024 * 1024
APP_LOG_BACKUP_COUNT = 7


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        try:
            parsed = json.loads(message)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = {
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "level": record.levelname.lower(),
                "logger": record.name,
                "event": message,
            }
            if record.exc_info is not None:
                parsed["exception"] = self.formatException(record.exc_info)
        return json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def configure_logging(
    level: str,
    log_dir: Path | None = None,
    *,
    stderr: bool = True,
) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    formatter = _JsonFormatter()
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        app_log = log_dir / "copilotd.log"
        file_handler = RotatingFileHandler(
            app_log,
            maxBytes=APP_LOG_MAX_BYTES,
            backupCount=APP_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(numeric_level)
        root.addHandler(file_handler)
        if os.name == "posix":
            app_log.chmod(0o600)
    if stderr:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        stderr_handler.setLevel(numeric_level)
        root.addHandler(stderr_handler)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
