from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import re
import time
import uuid
import weakref
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


@dataclass(frozen=True)
class PreparedAttachments:
    manifest_id: str
    count: int
    total_bytes: int


@dataclass(frozen=True)
class _StoredItem:
    item_index: int
    original_name: str
    mime_type: str
    byte_size: int
    sha256: str
    local_path: Path
    sdk_attachment_kind: str


@dataclass(frozen=True)
class _ResolvedLimits:
    file_max_bytes: int
    message_max_bytes: int
    inline_blob_max_bytes: int


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
    ) -> None:
        self._database = database
        self._data_dir = data_dir
        self._file_max_bytes = file_max_bytes
        self._message_max_bytes = message_max_bytes
        self._blob_max_bytes = blob_max_bytes
        self._capabilities = capabilities
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

    async def prepare(
        self,
        *,
        source_kind: str,
        source_id: str,
        session_id: str,
        attachments: list[DiscordAttachment],
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
            )
            try:
                if self._capabilities is None:
                    self._validate_declared_sizes(attachments)
                stored = await self._download_all(manifest_id, session_id, attachments)
                total_bytes = sum(item.byte_size for item in stored)
                await self._commit_items(manifest_id, stored, total_bytes)
            except BaseException:
                await self._database.execute(
                    "UPDATE attachment_manifests SET state = 'failed' WHERE id = ?",
                    (manifest_id,),
                )
                raise

        return PreparedAttachments(
            manifest_id=manifest_id,
            count=len(stored),
            total_bytes=total_bytes,
        )

    async def sdk_attachments(self, manifest_id: str) -> list[dict[str, Any]]:
        items = await self._verified_items(manifest_id)
        limits = self._resolved_limits()
        budget = max(0, limits.message_max_bytes - 2)
        emitted_items = 0
        result: list[dict[str, Any]] = []
        for item in items:
            if item.sdk_attachment_kind == "blob":
                blob, cost = await asyncio.to_thread(_load_inline_blob, item)
                request_cost = cost + (1 if emitted_items else 0)
                if request_cost <= budget:
                    result.append(blob)
                    budget -= request_cost
                    emitted_items += 1
                    continue
            result.append(
                {
                    "type": "file",
                    "path": str(item.local_path),
                    "displayName": item.original_name,
                }
            )
            emitted_items += 1
        return result

    async def _begin_manifest(
        self,
        *,
        manifest_id: str,
        source_kind: str,
        source_id: str,
        session_id: str,
    ) -> None:
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO attachment_manifests(
                    id, source_kind, source_id, session_id, state,
                    total_bytes, created_at
                ) VALUES (?, ?, ?, ?, 'preparing', 0, ?)
                ON CONFLICT(id) DO UPDATE SET
                    session_id = excluded.session_id,
                    state = 'preparing',
                    total_bytes = 0
                """,
                (manifest_id, source_kind, source_id, session_id, time.time()),
            )
            await connection.execute(
                "DELETE FROM attachment_items WHERE manifest_id = ?",
                (manifest_id,),
            )

    def _validate_declared_sizes(self, attachments: list[DiscordAttachment]) -> None:
        total = 0
        for attachment in attachments:
            if attachment.size > self._file_max_bytes:
                raise AttachmentError(
                    f"{attachment.filename} exceeds the "
                    f"{self._file_max_bytes // (1024 * 1024)} MiB file limit"
                )
            total += attachment.size
        if total > self._message_max_bytes:
            raise AttachmentError(
                f"attachments exceed the "
                f"{self._message_max_bytes // (1024 * 1024)} MiB message limit"
            )

    async def _download_all(
        self,
        manifest_id: str,
        session_id: str,
        attachments: list[DiscordAttachment],
    ) -> list[_StoredItem]:
        directory = self._data_dir / "sessions" / session_id / "attachments" / manifest_id
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        stored: list[_StoredItem] = []
        limits = self._resolved_limits()
        strict_legacy = self._capabilities is None
        for index, attachment in enumerate(attachments):
            content = await attachment.read(use_cached=True)
            mime_type = attachment.content_type or "application/octet-stream"
            stored_bytes = content
            stored_mime = mime_type
            kind = "file"

            if mime_type.startswith("image/"):
                if len(content) <= limits.inline_blob_max_bytes:
                    kind = "blob"
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
                            stored_bytes = compressed
                            stored_mime = compressed_mime
                            kind = "blob"
                        elif strict_legacy:
                            raise AttachmentError(
                                "image cannot be reduced below the SDK inline attachment limit"
                            )
            elif strict_legacy and (
                attachment.size > self._file_max_bytes or len(content) > self._message_max_bytes
            ):
                raise AttachmentError(
                    f"{attachment.filename} exceeds the "
                    f"{self._file_max_bytes // (1024 * 1024)} MiB file limit"
                )

            filename = f"{index:03d}-{_safe_filename(attachment.filename)}"
            target = directory / filename
            await asyncio.to_thread(_atomic_write, target, stored_bytes)
            stored.append(
                _StoredItem(
                    item_index=index,
                    original_name=attachment.filename,
                    mime_type=stored_mime,
                    byte_size=len(stored_bytes),
                    sha256=await asyncio.to_thread(_sha256_hex, stored_bytes),
                    local_path=target.resolve(),
                    sdk_attachment_kind=kind,
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
            await connection.execute(
                """
                UPDATE attachment_manifests
                SET state = 'ready', total_bytes = ?
                WHERE id = ? AND state = 'preparing'
                """,
                (total_bytes, manifest_id),
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
            SELECT item_index, original_name, mime_type, byte_size, sha256,
                   local_path, sdk_attachment_kind
            FROM attachment_items
            WHERE manifest_id = ? AND state = 'ready'
            ORDER BY item_index
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
            )
            for row in rows
        ]
        for item in items:
            valid = await asyncio.to_thread(_matches_integrity, item)
            if not valid:
                await self._database.execute(
                    "UPDATE attachment_manifests SET state = 'failed' WHERE id = ?",
                    (manifest_id,),
                )
                raise AttachmentError(f"attachment integrity check failed: {item.original_name}")
        return items

    def _resolved_limits(self) -> _ResolvedLimits:
        if self._capabilities is None:
            return _ResolvedLimits(
                file_max_bytes=self._file_max_bytes,
                message_max_bytes=self._message_max_bytes,
                inline_blob_max_bytes=self._blob_max_bytes,
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
        )


def _min_non_none(*values: int | None) -> int:
    numeric = [value for value in values if value is not None]
    if not numeric:
        raise ValueError("at least one attachment limit must be provided")
    return min(numeric)


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


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _load_inline_blob(item: _StoredItem) -> tuple[dict[str, Any], int]:
    content = item.local_path.read_bytes()
    data = base64.b64encode(content).decode("ascii")
    payload = {
        "type": "blob",
        "data": data,
        "mimeType": item.mime_type,
        "displayName": item.original_name,
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    cost = len(serialized.encode("utf-8"))
    return payload, cost


def _matches_integrity(item: _StoredItem) -> bool:
    try:
        content = item.local_path.read_bytes()
    except OSError:
        return False
    return len(content) == item.byte_size and _sha256_hex(content) == item.sha256


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
