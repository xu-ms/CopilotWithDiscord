from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from copilotd.storage.database import Database


class ProjectSource(StrEnum):
    EXPLICIT = "explicit"
    IMPLICIT_HOME = "implicit-home"


@dataclass(frozen=True, slots=True)
class ProjectSnapshot:
    project_id: str | None
    channel_id: str
    source: ProjectSource
    root_path: Path
    cwd: Path
    config_version: int


class ProjectPathError(ValueError):
    pass


class ProjectRegistry:
    """Resolves explicit channel projects before the immutable HOME fallback."""

    def __init__(self, database: Database, *, resolved_home: Path) -> None:
        self._database = database
        self._configured_home = resolved_home
        self._resolved_home: Path | None = None

    @property
    def resolved_home(self) -> Path:
        if self._resolved_home is None:
            raise RuntimeError("project registry has not been initialized")
        return self._resolved_home

    async def initialize(self) -> None:
        resolved_home = await asyncio.to_thread(_validate_directory, self._configured_home)
        now = time.time()
        row = await self._database.fetchone(
            "SELECT value FROM global_config WHERE key = 'resolved_home'"
        )
        if row is None:
            await self._database.execute(
                """
                INSERT INTO global_config(key, value, updated_at)
                VALUES ('resolved_home', ?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (str(resolved_home), now),
            )
            row = await self._database.fetchone(
                "SELECT value FROM global_config WHERE key = 'resolved_home'"
            )
        if row is None:
            raise RuntimeError("resolved HOME could not be persisted")
        persisted = await asyncio.to_thread(_validate_directory, Path(row["value"]))
        if persisted != resolved_home:
            raise ProjectPathError(
                "resolved HOME differs from the value persisted by this installation"
            )
        self._resolved_home = resolved_home

    async def resolve(self, channel_id: str) -> ProjectSnapshot:
        self._require_initialized()
        row = await self._database.fetchone(
            """
            SELECT * FROM projects
            WHERE channel_id = ? AND state = 'active'
            """,
            (channel_id,),
        )
        if row is None:
            return ProjectSnapshot(
                project_id=None,
                channel_id=channel_id,
                source=ProjectSource.IMPLICIT_HOME,
                root_path=self.resolved_home,
                cwd=self.resolved_home,
                config_version=1,
            )
        return ProjectSnapshot(
            project_id=row["id"],
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=Path(row["root_path"]),
            cwd=Path(row["cwd"]),
            config_version=row["config_version"],
        )

    async def bind(self, channel_id: str, path: Path) -> ProjectSnapshot:
        self._require_initialized()
        resolved = await asyncio.to_thread(_validate_directory, path)
        now = time.time()
        project_id = str(uuid.uuid4())
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT COALESCE(MAX(config_version), 0)
                FROM projects WHERE channel_id = ?
                """,
                (channel_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            config_version = int(row[0]) + 1
            await connection.execute(
                """
                UPDATE projects SET state = 'retired', updated_at = ?
                WHERE channel_id = ? AND state = 'active'
                """,
                (now, channel_id),
            )
            await connection.execute(
                """
                INSERT INTO projects(
                    id, channel_id, root_path, cwd, config_version,
                    state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    project_id,
                    channel_id,
                    str(resolved),
                    str(resolved),
                    config_version,
                    now,
                    now,
                ),
            )
        return ProjectSnapshot(
            project_id=project_id,
            channel_id=channel_id,
            source=ProjectSource.EXPLICIT,
            root_path=resolved,
            cwd=resolved,
            config_version=config_version,
        )

    async def unbind(self, channel_id: str) -> ProjectSnapshot:
        self._require_initialized()
        now = time.time()
        await self._database.execute(
            """
            UPDATE projects SET state = 'retired', updated_at = ?
            WHERE channel_id = ? AND state = 'active'
            """,
            (now, channel_id),
        )
        return await self.resolve(channel_id)

    async def set_layout(self, channel_id: str, layout: str) -> None:
        if layout not in {"text", "forum"}:
            raise ValueError(f"unsupported channel layout: {layout}")
        await self._upsert_channel_setting(channel_id, layout=layout)

    async def set_mention_required(self, channel_id: str, required: bool) -> None:
        await self._upsert_channel_setting(channel_id, mention_required=required)

    async def channel_settings(self, channel_id: str) -> tuple[str, bool, int]:
        row = await self._database.fetchone(
            "SELECT * FROM channel_settings WHERE channel_id = ?",
            (channel_id,),
        )
        if row is None:
            return "text", False, 1
        return row["layout"], bool(row["mention_required"]), row["config_version"]

    async def _upsert_channel_setting(
        self,
        channel_id: str,
        *,
        layout: str | None = None,
        mention_required: bool | None = None,
    ) -> None:
        now = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO channel_settings(channel_id, updated_at)
                VALUES (?, ?)
                ON CONFLICT(channel_id) DO NOTHING
                """,
                (channel_id, now),
            )
            updates: list[str] = []
            values: list[object] = []
            if layout is not None:
                updates.append("layout = ?")
                values.append(layout)
            if mention_required is not None:
                updates.append("mention_required = ?")
                values.append(int(mention_required))
            updates.extend(("config_version = config_version + 1", "updated_at = ?"))
            values.extend((now, channel_id))
            await connection.execute(
                f"UPDATE channel_settings SET {', '.join(updates)} WHERE channel_id = ?",
                values,
            )

    def _require_initialized(self) -> None:
        if self._resolved_home is None:
            raise RuntimeError("project registry has not been initialized")


def _validate_directory(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise ProjectPathError(f"project path does not exist: {resolved}")
    if not resolved.is_dir():
        raise ProjectPathError(f"project path is not a directory: {resolved}")
    return resolved
