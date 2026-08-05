from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass
from pathlib import Path

import pytest
from PIL import Image

from copilotd.core.attachments import (
    AttachmentCapabilities,
    AttachmentError,
    AttachmentService,
)
from copilotd.storage.database import Database


@dataclass
class FakeAttachment:
    id: int
    filename: str
    content: bytes
    content_type: str | None
    declared_size: int | None = None

    @property
    def size(self) -> int:
        return len(self.content) if self.declared_size is None else self.declared_size

    async def read(self, *, use_cached: bool = True) -> bytes:
        assert use_cached
        return self.content


def _path_exists(path: Path) -> bool:
    return path.exists()


def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _write_bytes(path: Path, content: bytes) -> None:
    path.write_bytes(content)


@pytest.mark.asyncio
async def test_attachment_work_yields_event_loop_and_falls_back_for_large_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = 0
    stop = asyncio.Event()
    calls: list[str] = []
    real_to_thread = asyncio.to_thread

    async def ticker() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            await asyncio.sleep(0)

    async def tracking_to_thread(func, /, *args, **kwargs):
        calls.append(getattr(func, "__name__", repr(func)))
        await asyncio.sleep(0.01)
        return await real_to_thread(func, *args, **kwargs)

    image_buffer = io.BytesIO()
    Image.new("RGB", (128, 128), "blue").save(image_buffer, format="PNG")
    attachment = FakeAttachment(
        id=1,
        filename="wall.png",
        content=image_buffer.getvalue(),
        content_type="image/png",
    )

    monkeypatch.setattr(asyncio, "to_thread", tracking_to_thread)
    ticker_task = asyncio.create_task(ticker())
    try:
        async with Database(tmp_path / "image.sqlite3") as database:
            service = AttachmentService(
                database,
                tmp_path,
                file_max_bytes=8,
                message_max_bytes=8,
                blob_max_bytes=8,
                capabilities=AttachmentCapabilities(
                    discord_file_max_bytes=8,
                    discord_message_max_bytes=8,
                    runtime_inline_blob_max_bytes=1,
                ),
            )
            prepared = await service.prepare(
                source_kind="discord-message",
                source_id="message-image",
                session_id="session-image",
                attachments=[attachment],
            )
            sdk_attachments = await service.sdk_attachments(prepared.manifest_id)
    finally:
        stop.set()
        await ticker_task

    assert ticks > 0
    assert prepared is not None
    assert sdk_attachments == [
        {
            "type": "file",
            "path": sdk_attachments[0]["path"],
            "displayName": "wall.png",
        }
    ]
    blob_path = Path(sdk_attachments[0]["path"])
    assert await asyncio.to_thread(_path_exists, blob_path)
    assert {"_compress_image", "_atomic_write", "_sha256_hex", "_matches_integrity"} <= set(calls)


@pytest.mark.asyncio
async def test_oversized_regular_file_uses_durable_path_fallback(tmp_path: Path) -> None:
    attachment = FakeAttachment(
        id=2,
        filename="large.bin",
        content=b"x" * 64,
        content_type=None,
        declared_size=64,
    )
    async with Database(tmp_path / "file.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            file_max_bytes=8,
            message_max_bytes=8,
            blob_max_bytes=8,
            capabilities=AttachmentCapabilities(
                discord_file_max_bytes=8,
                discord_message_max_bytes=8,
                runtime_inline_blob_max_bytes=8,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-file",
            session_id="session-file",
            attachments=[attachment],
        )
        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert prepared.total_bytes == len(attachment.content)
    assert sdk_attachments == [
        {
            "type": "file",
            "path": sdk_attachments[0]["path"],
            "displayName": "large.bin",
        }
    ]
    file_path = Path(sdk_attachments[0]["path"])
    stored_bytes = await asyncio.to_thread(_read_bytes, file_path)
    assert stored_bytes == attachment.content


@pytest.mark.asyncio
async def test_attachment_manifest_survives_restart_and_detects_tampering(
    tmp_path: Path,
) -> None:
    attachment = FakeAttachment(
        id=3,
        filename="report.txt",
        content=b"durable input",
        content_type="text/plain",
    )
    database_path = tmp_path / "restart.sqlite3"

    async with Database(database_path) as database:
        service = AttachmentService(database, tmp_path)
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-restart",
            session_id="session-restart",
            attachments=[attachment],
        )
        assert prepared is not None
        manifest_id = prepared.manifest_id
        row = await database.fetchone(
            "SELECT local_path FROM attachment_items WHERE manifest_id = ?",
            (manifest_id,),
        )
        local_path = Path(str(row["local_path"]))

    async with Database(database_path) as reopened:
        service = AttachmentService(reopened, tmp_path)
        sdk_attachments = await service.sdk_attachments(manifest_id)
        assert sdk_attachments == [
            {
                "type": "file",
                "path": str(local_path),
                "displayName": "report.txt",
            }
        ]
        await asyncio.to_thread(_write_bytes, local_path, b"tampered")
        with pytest.raises(AttachmentError, match="integrity check failed"):
            await service.sdk_attachments(manifest_id)
        failed = await reopened.fetchone(
            "SELECT state FROM attachment_manifests WHERE id = ?",
            (manifest_id,),
        )
        assert failed is not None and failed["state"] == "failed"
