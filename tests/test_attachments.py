from __future__ import annotations

import asyncio
import base64
import hashlib
import io
from dataclasses import dataclass
from pathlib import Path

import pytest
from PIL import Image

from copilotd.core.attachments import AttachmentError, AttachmentService
from copilotd.storage.database import Database


@dataclass
class FakeAttachment:
    id: int
    filename: str
    content: bytes
    content_type: str | None
    declared_size: int | None = None
    read_calls: int = 0

    @property
    def size(self) -> int:
        return len(self.content) if self.declared_size is None else self.declared_size

    async def read(self, *, use_cached: bool = True) -> bytes:
        assert use_cached
        self.read_calls += 1
        return self.content


@pytest.mark.asyncio
async def test_attachment_manifest_is_durable_idempotent_and_integrity_checked(
    tmp_path: Path,
) -> None:
    attachment = FakeAttachment(
        id=1,
        filename="../report.txt",
        content=b"durable input",
        content_type="text/plain",
    )
    async with Database(tmp_path / "attachments.sqlite3") as database:
        service = AttachmentService(database, tmp_path)
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-1",
            session_id="session-1",
            attachments=[attachment],
        )
        assert prepared is not None

        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)
        repeated = await service.prepare(
            source_kind="discord-message",
            source_id="message-1",
            session_id="session-1",
            attachments=[attachment],
        )
        row = await database.fetchone(
            """
            SELECT m.state, m.total_bytes, i.local_path, i.sha256
            FROM attachment_manifests m
            JOIN attachment_items i ON i.manifest_id = m.id
            WHERE m.id = ?
            """,
            (prepared.manifest_id,),
        )

        assert repeated == prepared
        assert attachment.read_calls == 1
        assert sdk_attachments == [
            {
                "type": "file",
                "path": row["local_path"],
                "displayName": "../report.txt",
            }
        ]
        assert row["state"] == "ready"
        assert row["total_bytes"] == len(attachment.content)
        persisted_content = await asyncio.to_thread(Path(row["local_path"]).read_bytes)
        assert persisted_content == attachment.content
        assert Path(row["local_path"]).name == (
            "000-" + hashlib.sha256(attachment.content).hexdigest()
        )

        await asyncio.to_thread(Path(row["local_path"]).write_bytes, b"tampered")
        with pytest.raises(AttachmentError, match="integrity check failed"):
            await service.sdk_attachments(prepared.manifest_id)
        failed = await database.fetchone(
            "SELECT state FROM attachment_manifests WHERE id = ?",
            (prepared.manifest_id,),
        )
        assert failed["state"] == "failed"


@pytest.mark.asyncio
async def test_image_attachment_becomes_sdk_blob(tmp_path: Path) -> None:
    image_buffer = io.BytesIO()
    Image.new("RGB", (4, 4), "blue").save(image_buffer, format="PNG")
    attachment = FakeAttachment(
        id=2,
        filename="pixel.png",
        content=image_buffer.getvalue(),
        content_type="image/png",
    )
    async with Database(tmp_path / "image.sqlite3") as database:
        service = AttachmentService(database, tmp_path)
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-2",
            session_id="session-2",
            attachments=[attachment],
        )
        assert prepared is not None

        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert sdk_attachments[0]["type"] == "blob"
    assert sdk_attachments[0]["mimeType"] == "image/png"
    assert base64.b64decode(sdk_attachments[0]["data"]) == attachment.content


@pytest.mark.asyncio
async def test_declared_attachment_limit_fails_before_download(tmp_path: Path) -> None:
    attachment = FakeAttachment(
        id=3,
        filename="large.bin",
        content=b"small fixture",
        content_type=None,
        declared_size=11,
    )
    async with Database(tmp_path / "limit.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            file_max_bytes=10,
            message_max_bytes=20,
        )
        with pytest.raises(AttachmentError, match="file limit"):
            await service.prepare(
                source_kind="discord-message",
                source_id="message-3",
                session_id="session-3",
                attachments=[attachment],
            )
        row = await database.fetchone("SELECT state FROM attachment_manifests")

    assert attachment.read_calls == 0
    assert row["state"] == "failed"
