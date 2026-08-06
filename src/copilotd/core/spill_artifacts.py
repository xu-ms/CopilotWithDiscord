from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from copilotd.storage.database import Database


async def confirm_and_collect_tool_spills(
    database: Database,
    paths: list[str],
    *,
    now: float | None = None,
    session_id: str | None = None,
) -> int:
    if not paths:
        return 0
    timestamp = time.time() if now is None else now
    placeholders = ", ".join("?" for _ in paths)
    await database.execute(
        f"""
        UPDATE tool_spill_artifacts
        SET delivery_confirmed_at = ?, updated_at = ?
        WHERE local_path IN ({placeholders}) AND finalized = 1
          AND (? IS NULL OR session_id = ?)
        """,
        (timestamp, timestamp, *paths, session_id, session_id),
    )
    return await garbage_collect_tool_spills(
        database,
        now=timestamp,
        session_id=session_id,
    )


async def garbage_collect_tool_spills(
    database: Database,
    *,
    now: float | None = None,
    session_id: str | None = None,
    force_session: bool = False,
) -> int:
    timestamp = time.time() if now is None else now
    forced = force_session and session_id is not None
    retry_paths = set() if forced else await _active_retry_paths(database, session_id=session_id)
    deleted_rows = await database.fetchall(
        """
        SELECT sdk_session_id FROM session_bindings
        WHERE binding_intent = 'deleted'
          AND (? IS NULL OR sdk_session_id = ?)
        """,
        (session_id, session_id),
    )
    deleted_sessions = {str(row["sdk_session_id"]) for row in deleted_rows}
    rows = await database.fetchall(
        """
        SELECT session_id, tool_call_id, local_path
        FROM tool_spill_artifacts
        WHERE (? IS NULL OR session_id = ?)
          AND (
            delivery_confirmed_at IS NOT NULL
            OR retention_until <= ?
            OR ? = 1
            OR session_id IN (
                SELECT sdk_session_id FROM session_bindings
                WHERE binding_intent = 'deleted'
            )
          )
        ORDER BY session_id, tool_call_id
        """,
        (
            session_id,
            session_id,
            timestamp,
            int(forced),
        ),
    )
    removed = 0
    for row in rows:
        path = Path(str(row["local_path"]))
        if (
            not forced
            and str(row["session_id"]) not in deleted_sessions
            and str(path) in retry_paths
        ):
            continue
        try:
            await asyncio.to_thread(path.unlink, missing_ok=True)
        except OSError:
            continue
        await database.execute(
            """
            DELETE FROM tool_spill_artifacts
            WHERE session_id = ? AND tool_call_id = ? AND local_path = ?
            """,
            (
                str(row["session_id"]),
                str(row["tool_call_id"]),
                str(path),
            ),
        )
        removed += 1
    return removed


async def _active_retry_paths(
    database: Database,
    *,
    session_id: str | None,
) -> set[str]:
    rows = await database.fetchall(
        """
        SELECT payload FROM render_outbox
        WHERE state IN ('pending', 'sending')
          AND (? IS NULL OR session_id = ?)
        """,
        (session_id, session_id),
    )
    paths: set[str] = set()
    for row in rows:
        try:
            payload: Any = json.loads(str(row["payload"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        attachments = payload.get("attachments")
        if not isinstance(attachments, list):
            continue
        for attachment in attachments:
            if isinstance(attachment, dict) and isinstance(attachment.get("path"), str):
                paths.add(str(attachment["path"]))
    return paths
