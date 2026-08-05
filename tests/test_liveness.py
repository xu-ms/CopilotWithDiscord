from pathlib import Path

import pytest

from copilotd.core.liveness import LivenessController, LivenessKind
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_liveness_is_idempotent_and_takeover_orphans_old_generation(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "liveness.sqlite3") as database:
        first_controller = LivenessController(
            database,
            sdk_session_id="session-1",
            runtime_generation=1,
            owner_fence_token=11,
        )
        first = await first_controller.acquire(LivenessKind.SUBMISSION, "submission-1", now=100)
        refreshed = await first_controller.acquire(
            LivenessKind.SUBMISSION,
            "submission-1",
            now=120,
        )

        assert refreshed.lease_id == first.lease_id
        assert refreshed.refreshed_at == 120

        next_controller = LivenessController(
            database,
            sdk_session_id="session-1",
            runtime_generation=2,
            owner_fence_token=12,
        )
        assert await next_controller.orphan_previous_generations(now=130) == 1
        replacement = await next_controller.acquire(
            LivenessKind.SUBMISSION,
            "submission-1",
            now=131,
        )

        with pytest.raises(RuntimeError, match="stale generation"):
            await first_controller.refresh(replacement, now=140)

        await next_controller.release(replacement, now=150)
        active = await next_controller.active()
        rows = await database.fetchall(
            "SELECT state, released_at FROM liveness_leases ORDER BY acquired_at"
        )

    assert active == []
    assert [dict(row) for row in rows] == [
        {"state": "orphaned", "released_at": 130},
        {"state": "released", "released_at": 150},
    ]
