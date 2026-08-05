from __future__ import annotations

import asyncio
import base64
import io
import json
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


def _inline_blob_cost(content: bytes, filename: str, mime_type: str) -> int:
    payload = {
        "data": base64.b64encode(content).decode("ascii"),
        "displayName": filename,
        "mimeType": mime_type,
        "type": "blob",
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return len(serialized.encode("utf-8"))


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
async def test_inline_budget_is_cumulative_across_images_and_preserves_order(
    tmp_path: Path,
) -> None:
    image_a = io.BytesIO()
    image_b = io.BytesIO()
    image_c = io.BytesIO()
    Image.new("RGB", (4, 4), "red").save(image_a, format="PNG")
    Image.new("RGB", (4, 4), "green").save(image_b, format="PNG")
    Image.new("RGB", (4, 4), "blue").save(image_c, format="PNG")
    attachments = [
        FakeAttachment(1, "a.png", image_a.getvalue(), "image/png"),
        FakeAttachment(2, "b.png", image_b.getvalue(), "image/png"),
        FakeAttachment(3, "c.png", image_c.getvalue(), "image/png"),
    ]
    first_cost = _inline_blob_cost(attachments[0].content, attachments[0].filename, "image/png")
    budget = 2 + first_cost + 1 + first_cost

    async with Database(tmp_path / "cumulative.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=budget,
            capabilities=AttachmentCapabilities(
                discord_message_max_bytes=budget,
                runtime_inline_blob_max_bytes=budget,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-cumulative",
            session_id="session-cumulative",
            attachments=attachments,
        )
        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert [item["type"] for item in sdk_attachments] == ["blob", "blob", "file"]
    assert base64.b64decode(sdk_attachments[0]["data"]) == attachments[0].content
    assert base64.b64decode(sdk_attachments[1]["data"]) == attachments[1].content
    third_path = Path(sdk_attachments[2]["path"])
    assert await asyncio.to_thread(_read_bytes, third_path) == attachments[2].content


@pytest.mark.asyncio
async def test_exact_inline_budget_boundary_keeps_blob_then_falls_back_one_byte_over(
    tmp_path: Path,
) -> None:
    image_buffer = io.BytesIO()
    Image.new("RGB", (4, 4), "purple").save(image_buffer, format="PNG")
    attachment = FakeAttachment(1, "截图.png", image_buffer.getvalue(), "image/png")
    blob_cost = _inline_blob_cost(attachment.content, attachment.filename, "image/png")

    async with Database(tmp_path / "boundary.sqlite3") as database:
        fit_service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=2 + blob_cost,
            capabilities=AttachmentCapabilities(
                discord_message_max_bytes=2 + blob_cost,
                runtime_inline_blob_max_bytes=blob_cost,
            ),
        )
        prepared = await fit_service.prepare(
            source_kind="discord-message",
            source_id="message-boundary-fit",
            session_id="session-boundary-fit",
            attachments=[attachment],
        )
        sdk_attachments = await fit_service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert sdk_attachments[0]["type"] == "blob"
    assert base64.b64decode(sdk_attachments[0]["data"]) == attachment.content

    async with Database(tmp_path / "boundary-over.sqlite3") as database:
        over_service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=2 + blob_cost - 1,
            capabilities=AttachmentCapabilities(
                discord_message_max_bytes=2 + blob_cost - 1,
                runtime_inline_blob_max_bytes=blob_cost,
            ),
        )
        prepared = await over_service.prepare(
            source_kind="discord-message",
            source_id="message-boundary-over",
            session_id="session-boundary-over",
            attachments=[attachment],
        )
        sdk_attachments = await over_service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert sdk_attachments[0]["type"] == "file"
    over_path = Path(sdk_attachments[0]["path"])
    assert await asyncio.to_thread(_read_bytes, over_path) == attachment.content


@pytest.mark.asyncio
async def test_mixed_image_and_file_order_is_preserved_with_budgeted_fallback(
    tmp_path: Path,
) -> None:
    image_a = io.BytesIO()
    image_b = io.BytesIO()
    Image.new("RGB", (4, 4), "yellow").save(image_a, format="PNG")
    Image.new("RGB", (4, 4), "cyan").save(image_b, format="PNG")
    file_content = b"file-content"
    attachments = [
        FakeAttachment(1, "first.png", image_a.getvalue(), "image/png"),
        FakeAttachment(2, "document.txt", file_content, "text/plain"),
        FakeAttachment(3, "second.png", image_b.getvalue(), "image/png"),
    ]
    first_cost = _inline_blob_cost(attachments[0].content, attachments[0].filename, "image/png")
    budget = 2 + first_cost

    async with Database(tmp_path / "mixed.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=budget,
            capabilities=AttachmentCapabilities(
                discord_message_max_bytes=budget,
                runtime_inline_blob_max_bytes=budget,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-mixed",
            session_id="session-mixed",
            attachments=attachments,
        )
        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert [item["type"] for item in sdk_attachments] == ["blob", "file", "file"]
    assert base64.b64decode(sdk_attachments[0]["data"]) == attachments[0].content
    file_path = Path(sdk_attachments[1]["path"])
    assert await asyncio.to_thread(_read_bytes, file_path) == file_content
    third_path = Path(sdk_attachments[2]["path"])
    assert await asyncio.to_thread(_read_bytes, third_path) == attachments[2].content


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
