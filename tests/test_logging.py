import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from copilotd.logging import (
    APP_LOG_BACKUP_COUNT,
    APP_LOG_MAX_BYTES,
    configure_logging,
)


def test_json_app_log_rotates_at_10_mib_with_seven_backups(tmp_path: Path) -> None:
    configure_logging("INFO", tmp_path, stderr=False)
    handlers = [
        handler
        for handler in logging.getLogger().handlers
        if isinstance(handler, RotatingFileHandler)
    ]
    assert len(handlers) == 1
    assert handlers[0].maxBytes == APP_LOG_MAX_BYTES == 10 * 1024 * 1024
    assert handlers[0].backupCount == APP_LOG_BACKUP_COUNT == 7

    logger = logging.getLogger("rotation-contract")
    payload = "x" * (1024 * 1024)
    for _ in range(12):
        logger.info(payload)
    handlers[0].flush()

    app_log = tmp_path / "copilotd.log"
    assert app_log.is_file()
    assert (tmp_path / "copilotd.log.1").is_file()
    assert len(list(tmp_path.glob("copilotd.log.*"))) <= 7
    latest = json.loads(app_log.read_text(encoding="utf-8").splitlines()[-1])
    assert latest["level"] == "info"
    assert latest["logger"] == "rotation-contract"
    configure_logging("CRITICAL", None, stderr=False)
