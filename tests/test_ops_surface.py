from pathlib import Path

import pytest

from copilotd.config import Settings
from copilotd.ops.surface import LocalOpsSurface
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_ops_surface_is_bounded_deterministic_and_redacts_logs(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "copilotd.log").write_text(
        "request corr-1 authorization: super-secret\nunrelated token=also-secret\n",
        encoding="utf-8",
    )
    settings = Settings(
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=log_dir,
    )
    async with Database(tmp_path / "ops.sqlite3") as database:
        service = LocalOpsSurface(database, settings)
        health = await service.health()
        debug = await service.debug(level="trace", duration_minutes=30)
        logs = await service.log_tail(correlation_id="corr-1")
        events = await service.event_dump(session_id="missing")

    assert health["database"] == "ok"
    assert health["pending_outbox"] == 0
    assert debug["level"] == "trace"
    assert debug["duration_minutes"] == 30
    assert "super-secret" not in logs["files"]["copilotd.log"]
    assert "[redacted]" in logs["files"]["copilotd.log"]
    assert events == {
        "session_id": "missing",
        "events": [],
        "bounded_to": 250,
    }
