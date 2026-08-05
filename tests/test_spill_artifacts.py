import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.spill_artifacts import (
    confirm_and_collect_tool_spills,
    garbage_collect_tool_spills,
)
from copilotd.storage.database import Database


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


async def _insert_artifact(
    database: Database,
    *,
    session_id: str,
    tool_call_id: str,
    path: Path,
    finalized: bool,
    retention_until: float,
) -> None:
    content = await asyncio.to_thread(path.read_bytes)
    await database.execute(
        """
        INSERT INTO tool_spill_artifacts(
            session_id, tool_call_id, local_path, byte_size,
            sha256, finalized, retention_until, delivery_confirmed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0)
        """,
        (
            session_id,
            tool_call_id,
            str(path),
            len(content),
            hashlib.sha256(content).hexdigest(),
            int(finalized),
            retention_until,
        ),
    )


@pytest.mark.asyncio
async def test_spill_gc_waits_for_confirmation_or_expiry(tmp_path: Path) -> None:
    path = tmp_path / "confirmed.txt"
    await asyncio.to_thread(_write, path, b"confirmed")
    async with Database(tmp_path / "spill-gc.sqlite3") as database:
        await _insert_artifact(
            database,
            session_id="session",
            tool_call_id="confirmed",
            path=path,
            finalized=True,
            retention_until=200,
        )

        assert await garbage_collect_tool_spills(database, now=100) == 0
        assert await asyncio.to_thread(path.exists)
        assert (
            await confirm_and_collect_tool_spills(
                database,
                [str(path)],
                now=101,
            )
            == 1
        )

    assert not await asyncio.to_thread(path.exists)


@pytest.mark.asyncio
async def test_spill_gc_handles_expiry_and_forced_session_delete(
    tmp_path: Path,
) -> None:
    expired = tmp_path / "expired.txt"
    active = tmp_path / "active.txt"
    await asyncio.to_thread(_write, expired, b"expired")
    await asyncio.to_thread(_write, active, b"active")
    async with Database(tmp_path / "spill-expiry.sqlite3") as database:
        await _insert_artifact(
            database,
            session_id="session-expired",
            tool_call_id="expired",
            path=expired,
            finalized=True,
            retention_until=50,
        )
        await _insert_artifact(
            database,
            session_id="session-active",
            tool_call_id="active",
            path=active,
            finalized=False,
            retention_until=200,
        )

        assert await garbage_collect_tool_spills(database, now=100) == 1
        assert await asyncio.to_thread(active.exists)
        assert (
            await garbage_collect_tool_spills(
                database,
                now=100,
                session_id="session-active",
                force_session=True,
            )
            == 1
        )

    assert not await asyncio.to_thread(expired.exists)
    assert not await asyncio.to_thread(active.exists)


@pytest.mark.asyncio
async def test_confirmed_deleted_session_collects_nonfinal_spill(
    tmp_path: Path,
) -> None:
    path = tmp_path / "deleted-session.txt"
    await asyncio.to_thread(_write, path, b"deleted")
    async with Database(tmp_path / "deleted-session.sqlite3") as database:
        await SessionBindingRepository(database).create(
            thread_id="deleted-thread",
            sdk_session_id="deleted-session",
            cwd_snapshot=tmp_path,
            project_source="explicit",
        )
        await _insert_artifact(
            database,
            session_id="deleted-session",
            tool_call_id="nonfinal",
            path=path,
            finalized=False,
            retention_until=999,
        )
        await database.execute(
            """
            UPDATE session_bindings SET binding_intent = 'deleted'
            WHERE sdk_session_id = 'deleted-session'
            """
        )

        assert await garbage_collect_tool_spills(database, now=100) == 1

    assert not await asyncio.to_thread(path.exists)


@pytest.mark.asyncio
async def test_expired_spill_is_retained_while_final_delivery_can_retry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "retry.txt"
    await asyncio.to_thread(_write, path, b"retry")
    async with Database(tmp_path / "spill-retry.sqlite3") as database:
        await _insert_artifact(
            database,
            session_id="session-retry",
            tool_call_id="retry",
            path=path,
            finalized=True,
            retention_until=50,
        )
        await database.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, coalesce_key,
                idempotency_key, payload, state, attempts,
                next_attempt_at, created_at, updated_at
            ) VALUES (
                'retry-outbox', 'session-retry', 1, 'artifact', 'retry',
                'retry-key', ?, 'pending', 1, 200, 0, 0
            )
            """,
            (
                json.dumps(
                    {
                        "finalized": True,
                        "attachments": [{"path": str(path)}],
                    }
                ),
            ),
        )

        assert await garbage_collect_tool_spills(database, now=100) == 0
        assert await asyncio.to_thread(path.exists)
        await database.execute(
            "UPDATE render_outbox SET state = 'blocked' WHERE id = 'retry-outbox'"
        )
        assert await garbage_collect_tool_spills(database, now=100) == 1

    assert not await asyncio.to_thread(path.exists)
