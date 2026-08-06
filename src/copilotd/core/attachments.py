from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import re
import shutil
import time
import uuid
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from PIL import Image, ImageOps

from copilotd.storage.database import Database


class AttachmentError(RuntimeError):
    pass


class DiscordAttachment(Protocol):
    id: int
    filename: str
    size: int
    content_type: str | None

    async def read(self, *, use_cached: bool = ...) -> bytes: ...


@dataclass(frozen=True)
class AttachmentCapabilities:
    discord_file_max_bytes: int | None = None
    discord_message_max_bytes: int | None = None
    runtime_inline_blob_max_bytes: int | None = None
    runtime_serialized_frame_max_bytes: int | None = None


@dataclass(frozen=True)
class PreparedAttachments:
    manifest_id: str
    count: int
    total_bytes: int


@dataclass(frozen=True)
class AttachmentRecovery:
    manifest_id: str
    source_kind: str
    source_id: str
    source_channel_id: str | None
    source_message_id: str | None
    session_id: str | None
    state: str
    prompt: str | None
    idempotency_key: str | None
    origin: str | None
    needs_submission: bool


@dataclass(frozen=True)
class _StoredItem:
    item_index: int
    original_name: str
    mime_type: str
    byte_size: int
    sha256: str
    local_path: Path
    sdk_attachment_kind: str
    inline_path: Path | None
    inline_mime_type: str | None
    inline_byte_size: int | None
    inline_sha256: str | None


@dataclass(frozen=True)
class _ResolvedLimits:
    file_max_bytes: int
    message_max_bytes: int
    inline_blob_max_bytes: int
    serialized_frame_max_bytes: int


class AttachmentService:
    """Durably downloads Discord attachments before they enter an SDK submission."""

    def __init__(
        self,
        database: Database,
        data_dir: Path,
        *,
        file_max_bytes: int = 25 * 1024 * 1024,
        message_max_bytes: int = 100 * 1024 * 1024,
        blob_max_bytes: int = 7 * 1024 * 1024,
        capabilities: AttachmentCapabilities | None = None,
        retention_seconds: float = 7 * 24 * 60 * 60,
    ) -> None:
        self._database = database
        self._data_dir = data_dir
        self._file_max_bytes = file_max_bytes
        self._message_max_bytes = message_max_bytes
        self._blob_max_bytes = blob_max_bytes
        self._capabilities = capabilities
        self._retention_seconds = retention_seconds
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    async def prepare(
        self,
        *,
        source_kind: str,
        source_id: str,
        session_id: str,
        attachments: list[DiscordAttachment],
        source_channel_id: str | None = None,
        source_message_id: str | None = None,
        recovery_prompt: str | None = None,
        recovery_idempotency_key: str | None = None,
        recovery_origin: str | None = None,
    ) -> PreparedAttachments | None:
        if not attachments:
            return None
        manifest_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:attachment:{source_kind}:{source_id}",
            )
        )
        lock = self._locks.setdefault(manifest_id, asyncio.Lock())
        async with lock:
            existing = await self._database.fetchone(
                "SELECT state, total_bytes FROM attachment_manifests WHERE id = ?",
                (manifest_id,),
            )
            if existing is not None and existing["state"] == "ready":
                await self._update_recovery_metadata(
                    manifest_id,
                    source_channel_id=source_channel_id,
                    source_message_id=source_message_id,
                    recovery_prompt=recovery_prompt,
                    recovery_idempotency_key=recovery_idempotency_key,
                    recovery_origin=recovery_origin,
                )
                items = await self._verified_items(manifest_id)
                return PreparedAttachments(
                    manifest_id=manifest_id,
                    count=len(items),
                    total_bytes=int(existing["total_bytes"]),
                )

            await self._begin_manifest(
                manifest_id=manifest_id,
                source_kind=source_kind,
                source_id=source_id,
                session_id=session_id,
                source_channel_id=source_channel_id,
                source_message_id=source_message_id,
                recovery_prompt=recovery_prompt,
                recovery_idempotency_key=recovery_idempotency_key,
                recovery_origin=recovery_origin,
            )
            try:
                self._validate_declared_sizes(
                    attachments,
                    self._resolved_limits(),
                )
                stored = await self._download_all(manifest_id, session_id, attachments)
                total_bytes = sum(item.byte_size for item in stored)
                await self._commit_items(manifest_id, stored, total_bytes)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._database.execute(
                    """
                    UPDATE attachment_manifests
                    SET state = 'failed', error_code = 'prepare_failed',
                        error_detail = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (_bounded_error(error), time.time(), manifest_id),
                )
                raise

        return PreparedAttachments(
            manifest_id=manifest_id,
            count=len(stored),
            total_bytes=total_bytes,
        )

    async def prepared_manifest(self, manifest_id: str) -> PreparedAttachments:
        row = await self._database.fetchone(
            "SELECT state, total_bytes FROM attachment_manifests WHERE id = ?",
            (manifest_id,),
        )
        if row is None or str(row["state"]) != "ready":
            raise AttachmentError(f"attachment manifest is not ready: {manifest_id}")
        items = await self._verified_items(manifest_id)
        return PreparedAttachments(
            manifest_id=manifest_id,
            count=len(items),
            total_bytes=int(row["total_bytes"]),
        )

    async def pending_recoveries(self) -> tuple[AttachmentRecovery, ...]:
        rows = await self._database.fetchall(
            """
            SELECT m.id, m.source_kind, m.source_id, m.source_channel_id,
                   m.source_message_id, m.session_id, m.state,
                   m.recovery_prompt, m.recovery_idempotency_key,
                   m.recovery_origin,
                   EXISTS (
                       SELECT 1 FROM message_queue q
                       WHERE q.attachment_manifest_id = m.id
                   ) OR EXISTS (
                       SELECT 1 FROM submissions s
                       WHERE s.attachment_manifest_id = m.id
                   ) AS has_submission_reference
            FROM attachment_manifests m
            WHERE m.state = 'preparing'
               OR (
                   m.state = 'ready'
                   AND m.recovery_idempotency_key IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM message_queue q
                       WHERE q.attachment_manifest_id = m.id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM submissions s
                       WHERE s.attachment_manifest_id = m.id
                   )
               )
            ORDER BY m.created_at, m.id
            """
        )
        return tuple(
            AttachmentRecovery(
                manifest_id=str(row["id"]),
                source_kind=str(row["source_kind"]),
                source_id=str(row["source_id"]),
                source_channel_id=(
                    None if row["source_channel_id"] is None else str(row["source_channel_id"])
                ),
                source_message_id=(
                    None if row["source_message_id"] is None else str(row["source_message_id"])
                ),
                session_id=None if row["session_id"] is None else str(row["session_id"]),
                state=str(row["state"]),
                prompt=None if row["recovery_prompt"] is None else str(row["recovery_prompt"]),
                idempotency_key=(
                    None
                    if row["recovery_idempotency_key"] is None
                    else str(row["recovery_idempotency_key"])
                ),
                origin=None if row["recovery_origin"] is None else str(row["recovery_origin"]),
                needs_submission=not bool(row["has_submission_reference"]),
            )
            for row in rows
        )

    async def record_recovery_error(
        self,
        manifest_id: str,
        *,
        code: str,
        detail: str,
        terminal: bool,
    ) -> None:
        await self._database.execute(
            """
            UPDATE attachment_manifests
            SET state = CASE WHEN ? THEN 'failed' ELSE state END,
                error_code = ?, error_detail = ?, updated_at = ?
            WHERE id = ? AND state IN ('preparing', 'ready', 'released')
            """,
            (int(terminal), code, detail[:1000], time.time(), manifest_id),
        )

    async def record_recovery_success(self, manifest_id: str) -> None:
        await self._database.execute(
            """
            UPDATE attachment_manifests
            SET error_code = NULL, error_detail = NULL, updated_at = ?
            WHERE id = ? AND state = 'ready'
            """,
            (time.time(), manifest_id),
        )

    async def release_unreferenced(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now
        retention_until = timestamp + self._retention_seconds
        return await self._database.execute_count(
            """
            UPDATE attachment_manifests AS manifest
            SET state = 'released', retention_until = ?, updated_at = ?
            WHERE state IN ('ready', 'failed')
              AND NOT EXISTS (
                  SELECT 1 FROM message_queue queue
                  WHERE queue.attachment_manifest_id = manifest.id
                    AND queue.state NOT IN ('cancelled', 'submitted', 'failed')
              )
              AND NOT EXISTS (
                  SELECT 1 FROM submissions submission
                  WHERE submission.attachment_manifest_id = manifest.id
                    AND submission.state NOT IN (
                        'rejected', 'semantic_complete', 'semantic_blocked',
                        'observed_aborted', 'outcome_unknown', 'cancelled', 'failed'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM session_creation_intents creation
                  WHERE creation.attachment_manifest_id = manifest.id
                    AND creation.state NOT IN ('attached', 'failed')
              )
              AND (
                  state = 'failed'
                  OR recovery_idempotency_key IS NULL
                  OR EXISTS (
                      SELECT 1 FROM message_queue queue
                      WHERE queue.attachment_manifest_id = manifest.id
                  )
                  OR EXISTS (
                      SELECT 1 FROM submissions submission
                      WHERE submission.attachment_manifest_id = manifest.id
                  )
              )
            """,
            (retention_until, timestamp),
        )

    async def garbage_collect(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now
        rows = await self._database.fetchall(
            """
            SELECT manifest.id, manifest.session_id
            FROM attachment_manifests manifest
            WHERE manifest.state = 'released'
              AND manifest.retention_until IS NOT NULL
              AND manifest.retention_until <= ?
              AND NOT EXISTS (
                  SELECT 1 FROM message_queue queue
                  WHERE queue.attachment_manifest_id = manifest.id
                    AND queue.state NOT IN ('cancelled', 'submitted', 'failed')
              )
              AND NOT EXISTS (
                  SELECT 1 FROM submissions submission
                  WHERE submission.attachment_manifest_id = manifest.id
                    AND submission.state NOT IN (
                        'rejected', 'semantic_complete', 'semantic_blocked',
                        'observed_aborted', 'outcome_unknown', 'cancelled', 'failed'
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM session_creation_intents creation
                  WHERE creation.attachment_manifest_id = manifest.id
                    AND creation.state NOT IN ('attached', 'failed')
              )
            ORDER BY manifest.updated_at, manifest.id
            """,
            (timestamp,),
        )
        removed = 0
        for row in rows:
            manifest_id = str(row["id"])
            session_id = None if row["session_id"] is None else str(row["session_id"])
            if session_id is None:
                await self.record_recovery_error(
                    manifest_id,
                    code="cleanup_missing_session",
                    detail="attachment manifest has no session directory owner",
                    terminal=False,
                )
                continue
            directory = self._manifest_directory(session_id, manifest_id)
            try:
                await asyncio.to_thread(
                    _remove_manifest_directory,
                    directory,
                    self._data_dir / "sessions",
                )
            except OSError as error:
                await self.record_recovery_error(
                    manifest_id,
                    code="cleanup_failed",
                    detail=_bounded_error(error),
                    terminal=False,
                )
                continue
            async with self._database.transaction() as connection:
                await connection.execute(
                    "DELETE FROM attachment_items WHERE manifest_id = ?",
                    (manifest_id,),
                )
                await connection.execute(
                    """
                    UPDATE attachment_manifests
                    SET total_bytes = 0, error_code = NULL, error_detail = NULL,
                        retention_until = NULL, updated_at = ?
                    WHERE id = ? AND state = 'released'
                    """,
                    (timestamp, manifest_id),
                )
            removed += 1
        return removed

    async def sdk_attachments(self, manifest_id: str) -> list[dict[str, Any]]:
        items = await self._verified_items(manifest_id)
        limits = self._resolved_limits()
        frame_budget = max(0, limits.serialized_frame_max_bytes)
        result: list[dict[str, Any]] = []
        for item in items:
            if item.sdk_attachment_kind == "blob":
                candidate = await asyncio.to_thread(_load_inline_blob, item)
            else:
                candidate = _load_file_attachment(item)

            proposed = [*result, candidate]
            request_size = await asyncio.to_thread(
                _serialized_request_size,
                proposed,
            )
            proposed, request_size = await _downgrade_blobs_by_savings(
                proposed,
                items,
                frame_budget,
                _serialized_request_size,
            )
            if request_size > frame_budget:
                raise AttachmentError(
                    "serialized SDK attachment request exceeds the runtime frame limit "
                    f"at {item.original_name}"
                )

            result = proposed
        return result

    async def sdk_attachments_for_send(
        self,
        manifest_id: str,
        *,
        session_id: str,
        prompt: str,
        mode: str,
        agent_mode: str,
    ) -> list[dict[str, Any]]:
        items = await self._verified_items(manifest_id)
        limits = self._resolved_limits()
        attachments = [
            (
                await asyncio.to_thread(_load_inline_blob, item)
                if item.sdk_attachment_kind == "blob"
                else _load_file_attachment(item)
            )
            for item in items
        ]
        trace_context = await asyncio.to_thread(sdk_trace_context)
        frame_size = await asyncio.to_thread(
            sdk_send_frame_size,
            session_id=session_id,
            prompt=prompt,
            attachments=attachments,
            mode=mode,
            agent_mode=agent_mode,
            trace_context=trace_context,
        )

        def full_frame_size(values: list[dict[str, Any]]) -> int:
            return sdk_send_frame_size(
                session_id=session_id,
                prompt=prompt,
                attachments=values,
                mode=mode,
                agent_mode=agent_mode,
                trace_context=trace_context,
            )

        attachments, frame_size = await _downgrade_blobs_by_savings(
            attachments,
            items,
            limits.serialized_frame_max_bytes,
            full_frame_size,
        )
        if frame_size > limits.serialized_frame_max_bytes:
            raise AttachmentError(
                "complete serialized session.send frame exceeds the runtime limit"
            )
        return attachments

    async def _begin_manifest(
        self,
        *,
        manifest_id: str,
        source_kind: str,
        source_id: str,
        session_id: str,
        source_channel_id: str | None,
        source_message_id: str | None,
        recovery_prompt: str | None,
        recovery_idempotency_key: str | None,
        recovery_origin: str | None,
    ) -> None:
        await asyncio.to_thread(
            _remove_manifest_directory,
            self._manifest_directory(session_id, manifest_id),
            self._data_dir / "sessions",
        )
        timestamp = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO attachment_manifests(
                    id, source_kind, source_id, session_id, state,
                    total_bytes, created_at, source_channel_id,
                    source_message_id, recovery_prompt,
                    recovery_idempotency_key, recovery_origin, updated_at
                ) VALUES (?, ?, ?, ?, 'preparing', 0, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    session_id = excluded.session_id,
                    state = 'preparing',
                    total_bytes = 0,
                    retention_until = NULL,
                    source_channel_id = COALESCE(
                        excluded.source_channel_id,
                        attachment_manifests.source_channel_id
                    ),
                    source_message_id = COALESCE(
                        excluded.source_message_id,
                        attachment_manifests.source_message_id
                    ),
                    recovery_prompt = COALESCE(
                        excluded.recovery_prompt,
                        attachment_manifests.recovery_prompt
                    ),
                    recovery_idempotency_key = COALESCE(
                        excluded.recovery_idempotency_key,
                        attachment_manifests.recovery_idempotency_key
                    ),
                    recovery_origin = COALESCE(
                        excluded.recovery_origin,
                        attachment_manifests.recovery_origin
                    ),
                    error_code = NULL,
                    error_detail = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    manifest_id,
                    source_kind,
                    source_id,
                    session_id,
                    timestamp,
                    source_channel_id,
                    source_message_id,
                    recovery_prompt,
                    recovery_idempotency_key,
                    recovery_origin,
                    timestamp,
                ),
            )
            await connection.execute(
                "DELETE FROM attachment_items WHERE manifest_id = ?",
                (manifest_id,),
            )

    async def _update_recovery_metadata(
        self,
        manifest_id: str,
        *,
        source_channel_id: str | None,
        source_message_id: str | None,
        recovery_prompt: str | None,
        recovery_idempotency_key: str | None,
        recovery_origin: str | None,
    ) -> None:
        await self._database.execute(
            """
            UPDATE attachment_manifests
            SET source_channel_id = COALESCE(?, source_channel_id),
                source_message_id = COALESCE(?, source_message_id),
                recovery_prompt = COALESCE(?, recovery_prompt),
                recovery_idempotency_key = COALESCE(?, recovery_idempotency_key),
                recovery_origin = COALESCE(?, recovery_origin),
                updated_at = ?
            WHERE id = ?
            """,
            (
                source_channel_id,
                source_message_id,
                recovery_prompt,
                recovery_idempotency_key,
                recovery_origin,
                time.time(),
                manifest_id,
            ),
        )

    def _manifest_directory(self, session_id: str, manifest_id: str) -> Path:
        return self._data_dir / "sessions" / session_id / "attachments" / manifest_id

    def _validate_declared_sizes(
        self,
        attachments: list[DiscordAttachment],
        limits: _ResolvedLimits,
    ) -> None:
        total = 0
        for attachment in attachments:
            if attachment.size > limits.file_max_bytes:
                raise AttachmentError(
                    f"{attachment.filename} exceeds the "
                    f"{limits.file_max_bytes // (1024 * 1024)} MiB file limit"
                )
            total += attachment.size
        if total > limits.message_max_bytes:
            raise AttachmentError(
                f"attachments exceed the "
                f"{limits.message_max_bytes // (1024 * 1024)} MiB message limit"
            )

    async def _download_all(
        self,
        manifest_id: str,
        session_id: str,
        attachments: list[DiscordAttachment],
    ) -> list[_StoredItem]:
        directory = self._manifest_directory(session_id, manifest_id)
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        stored: list[_StoredItem] = []
        limits = self._resolved_limits()
        strict_legacy = self._capabilities is None
        actual_total = 0
        for index, attachment in enumerate(attachments):
            content = await attachment.read(use_cached=True)
            if len(content) > limits.file_max_bytes:
                raise AttachmentError(f"{attachment.filename} exceeds the actual file limit")
            actual_total += len(content)
            if actual_total > limits.message_max_bytes:
                raise AttachmentError("actual attachments exceed the message limit")
            mime_type = attachment.content_type or "application/octet-stream"
            kind = "file"
            inline_bytes: bytes | None = None
            inline_mime: str | None = None

            if mime_type.startswith("image/"):
                if len(content) <= limits.inline_blob_max_bytes:
                    kind = "blob"
                    inline_bytes = content
                    inline_mime = mime_type
                else:
                    try:
                        compressed, compressed_mime = await asyncio.to_thread(
                            _compress_image,
                            content,
                            mime_type,
                            limits.inline_blob_max_bytes,
                        )
                    except AttachmentError:
                        if strict_legacy:
                            raise
                    else:
                        if len(compressed) <= limits.inline_blob_max_bytes:
                            kind = "blob"
                            inline_bytes = compressed
                            inline_mime = compressed_mime
                        elif strict_legacy:
                            raise AttachmentError(
                                "image cannot be reduced below the SDK inline attachment limit"
                            )
            filename = f"{index:03d}-{_safe_filename(attachment.filename)}"
            target = directory / filename
            await asyncio.to_thread(_atomic_write, target, content)
            inline_path: Path | None = None
            inline_size: int | None = None
            inline_sha: str | None = None
            if kind == "blob":
                if inline_bytes is None:
                    raise AttachmentError("inline image bytes are unavailable")
                inline_path = target
                if inline_bytes != content:
                    inline_path = target.with_name(f"{target.name}.inline.jpg")
                    await asyncio.to_thread(_atomic_write, inline_path, inline_bytes)
                inline_size = len(inline_bytes)
                inline_sha = await asyncio.to_thread(_sha256_hex, inline_bytes)
            stored.append(
                _StoredItem(
                    item_index=index,
                    original_name=attachment.filename,
                    mime_type=mime_type,
                    byte_size=len(content),
                    sha256=await asyncio.to_thread(_sha256_hex, content),
                    local_path=target.resolve(),
                    sdk_attachment_kind=kind,
                    inline_path=None if inline_path is None else inline_path.resolve(),
                    inline_mime_type=inline_mime,
                    inline_byte_size=inline_size,
                    inline_sha256=inline_sha,
                )
            )
        return stored

    async def _commit_items(
        self,
        manifest_id: str,
        items: list[_StoredItem],
        total_bytes: int,
    ) -> None:
        async with self._database.transaction() as connection:
            for item in items:
                await connection.execute(
                    """
                    INSERT INTO attachment_items(
                        manifest_id, item_index, original_name, mime_type,
                        byte_size, sha256, local_path, sdk_attachment_kind, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready')
                    """,
                    (
                        manifest_id,
                        item.item_index,
                        item.original_name,
                        item.mime_type,
                        item.byte_size,
                        item.sha256,
                        str(item.local_path),
                        item.sdk_attachment_kind,
                    ),
                )
                if item.inline_path is not None:
                    await connection.execute(
                        """
                        INSERT INTO attachment_inline_variants(
                            manifest_id, item_index, mime_type,
                            byte_size, sha256, local_path
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            manifest_id,
                            item.item_index,
                            item.inline_mime_type or item.mime_type,
                            item.inline_byte_size or item.byte_size,
                            item.inline_sha256 or item.sha256,
                            str(item.inline_path),
                        ),
                    )
            await connection.execute(
                """
                UPDATE attachment_manifests
                SET state = 'ready', total_bytes = ?, retention_until = NULL,
                    error_code = NULL, error_detail = NULL, updated_at = ?
                WHERE id = ? AND state = 'preparing'
                """,
                (total_bytes, time.time(), manifest_id),
            )

    async def _verified_items(self, manifest_id: str) -> list[_StoredItem]:
        manifest = await self._database.fetchone(
            "SELECT state FROM attachment_manifests WHERE id = ?",
            (manifest_id,),
        )
        if manifest is None or manifest["state"] != "ready":
            raise AttachmentError(f"attachment manifest is not ready: {manifest_id}")
        rows = await self._database.fetchall(
            """
            SELECT i.item_index, i.original_name, i.mime_type,
                   i.byte_size, i.sha256, i.local_path,
                   i.sdk_attachment_kind,
                   v.mime_type AS inline_mime_type,
                   v.byte_size AS inline_byte_size,
                   v.sha256 AS inline_sha256,
                   v.local_path AS inline_path
            FROM attachment_items AS i
            LEFT JOIN attachment_inline_variants AS v
              ON v.manifest_id = i.manifest_id
             AND v.item_index = i.item_index
            WHERE i.manifest_id = ? AND i.state = 'ready'
            ORDER BY i.item_index
            """,
            (manifest_id,),
        )
        items = [
            _StoredItem(
                item_index=int(row["item_index"]),
                original_name=str(row["original_name"]),
                mime_type=str(row["mime_type"] or "application/octet-stream"),
                byte_size=int(row["byte_size"]),
                sha256=str(row["sha256"]),
                local_path=Path(str(row["local_path"])),
                sdk_attachment_kind=str(row["sdk_attachment_kind"]),
                inline_path=(
                    Path(str(row["inline_path"]))
                    if row["inline_path"] is not None
                    else Path(str(row["local_path"]))
                    if str(row["sdk_attachment_kind"]) == "blob"
                    else None
                ),
                inline_mime_type=(
                    str(row["inline_mime_type"])
                    if row["inline_mime_type"] is not None
                    else str(row["mime_type"])
                    if str(row["sdk_attachment_kind"]) == "blob"
                    else None
                ),
                inline_byte_size=(
                    int(row["inline_byte_size"])
                    if row["inline_byte_size"] is not None
                    else int(row["byte_size"])
                    if str(row["sdk_attachment_kind"]) == "blob"
                    else None
                ),
                inline_sha256=(
                    str(row["inline_sha256"])
                    if row["inline_sha256"] is not None
                    else str(row["sha256"])
                    if str(row["sdk_attachment_kind"]) == "blob"
                    else None
                ),
            )
            for row in rows
        ]
        for item in items:
            valid = await asyncio.to_thread(_matches_integrity, item)
            if not valid:
                await self._database.execute(
                    """
                    UPDATE attachment_manifests
                    SET state = 'failed', error_code = 'integrity_failed',
                        error_detail = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        f"integrity check failed: {item.original_name}"[:1000],
                        time.time(),
                        manifest_id,
                    ),
                )
                raise AttachmentError(f"attachment integrity check failed: {item.original_name}")
        return items

    def _resolved_limits(self) -> _ResolvedLimits:
        if self._capabilities is None:
            return _ResolvedLimits(
                file_max_bytes=self._file_max_bytes,
                message_max_bytes=self._message_max_bytes,
                inline_blob_max_bytes=self._blob_max_bytes,
                serialized_frame_max_bytes=self._message_max_bytes,
            )
        return _ResolvedLimits(
            file_max_bytes=_min_non_none(
                self._file_max_bytes,
                self._capabilities.discord_file_max_bytes,
            ),
            message_max_bytes=_min_non_none(
                self._message_max_bytes,
                self._capabilities.discord_message_max_bytes,
            ),
            inline_blob_max_bytes=_min_non_none(
                self._blob_max_bytes,
                self._capabilities.runtime_inline_blob_max_bytes,
            ),
            serialized_frame_max_bytes=(
                self._capabilities.runtime_serialized_frame_max_bytes
                if self._capabilities.runtime_serialized_frame_max_bytes is not None
                else self._message_max_bytes
            ),
        )


def _min_non_none(*values: int | None) -> int:
    numeric = [value for value in values if value is not None]
    if not numeric:
        raise ValueError("at least one attachment limit must be provided")
    return min(numeric)


async def _downgrade_blobs_by_savings(
    values: list[dict[str, Any]],
    items: list[_StoredItem],
    budget: int,
    size_function: Callable[[list[dict[str, Any]]], int],
) -> tuple[list[dict[str, Any]], int]:
    current = list(values)
    current_size = await asyncio.to_thread(size_function, current)
    while current_size > budget:
        best_index: int | None = None
        best_size = current_size
        best_saving = 0
        for index, value in enumerate(current):
            if value["type"] != "blob":
                continue
            candidate = list(current)
            candidate[index] = _load_file_attachment(items[index])
            candidate_size = await asyncio.to_thread(size_function, candidate)
            saving = current_size - candidate_size
            if saving > best_saving:
                best_index = index
                best_size = candidate_size
                best_saving = saving
        if best_index is None:
            break
        current[best_index] = _load_file_attachment(items[best_index])
        current_size = best_size
    return current, current_size


def _safe_filename(filename: str) -> str:
    name = Path(filename).name
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return name[:160] or "attachment"


def _atomic_write(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_manifest_directory(directory: Path, sessions_root: Path) -> None:
    resolved_root = sessions_root.resolve()
    resolved_directory = directory.resolve(strict=False)
    if not resolved_directory.is_relative_to(resolved_root):
        raise OSError(f"attachment directory escapes the managed sessions root: {directory}")
    if directory.is_symlink():
        directory.unlink(missing_ok=True)
    elif directory.exists():
        shutil.rmtree(directory)


def _bounded_error(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"[:1000]


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _load_inline_blob(item: _StoredItem) -> dict[str, Any]:
    if item.inline_path is None:
        raise AttachmentError(f"inline variant is missing: {item.original_name}")
    content = item.inline_path.read_bytes()
    data = base64.b64encode(content).decode("ascii")
    return {
        "type": "blob",
        "data": data,
        "mimeType": item.inline_mime_type or item.mime_type,
        "displayName": item.original_name,
    }


def _load_file_attachment(item: _StoredItem) -> dict[str, Any]:
    return {
        "type": "file",
        "path": str(item.local_path),
        "displayName": item.original_name,
    }


def _serialized_request_size(items: list[dict[str, Any]]) -> int:
    payload = json.dumps(
        items,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return len(payload.encode("utf-8"))


def sdk_send_frame_size(
    *,
    session_id: str,
    prompt: str,
    attachments: list[dict[str, Any]] | None,
    mode: str | None,
    agent_mode: str | None,
    trace_context: dict[str, str] | None = None,
) -> int:
    params: dict[str, Any] = {
        "sessionId": session_id,
        "prompt": prompt,
    }
    if attachments is not None:
        params["attachments"] = attachments
    if mode is not None:
        params["mode"] = mode
    if agent_mode is not None:
        params["agentMode"] = agent_mode
    params.update(trace_context or {})
    message = {
        "jsonrpc": "2.0",
        "id": "00000000-0000-0000-0000-000000000000",
        "method": "session.send",
        "params": params,
    }
    content_bytes = json.dumps(message, separators=(",", ":")).encode("utf-8")
    header = f"Content-Length: {len(content_bytes)}\r\n\r\n".encode()
    return len(header) + len(content_bytes)


def sdk_trace_context() -> dict[str, str]:
    try:
        from copilot._telemetry import get_trace_context
    except ImportError:
        return {}
    return get_trace_context()


def _matches_integrity(item: _StoredItem) -> bool:
    try:
        content = item.local_path.read_bytes()
    except OSError:
        return False
    if len(content) != item.byte_size or _sha256_hex(content) != item.sha256:
        return False
    if item.inline_path is None:
        return True
    try:
        inline_content = item.inline_path.read_bytes()
    except OSError:
        return False
    return (
        len(inline_content) == item.inline_byte_size
        and _sha256_hex(inline_content) == item.inline_sha256
    )


def _compress_image(content: bytes, mime_type: str, limit: int) -> tuple[bytes, str]:
    try:
        with Image.open(io.BytesIO(content)) as source:
            image = ImageOps.exif_transpose(source)
            image.thumbnail((4096, 4096), Image.Resampling.LANCZOS)
            if image.mode not in {"RGB", "L"}:
                background = Image.new("RGB", image.size, "white")
                if image.mode == "RGBA":
                    background.paste(image, mask=image.getchannel("A"))
                else:
                    background.paste(image.convert("RGB"))
                image = background
            else:
                image = image.convert("RGB")

            for quality in (88, 80, 72, 64, 56):
                output = io.BytesIO()
                image.save(output, format="JPEG", quality=quality, optimize=True)
                encoded = output.getvalue()
                if len(encoded) <= limit:
                    return encoded, "image/jpeg"
                image.thumbnail(
                    (max(1, int(image.width * 0.8)), max(1, int(image.height * 0.8))),
                    Image.Resampling.LANCZOS,
                )
    except Exception as error:
        raise AttachmentError(f"unable to compress {mime_type} image: {error}") from error
    raise AttachmentError("image cannot be reduced below the SDK inline attachment limit")
