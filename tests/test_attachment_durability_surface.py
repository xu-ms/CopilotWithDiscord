from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from PIL import Image

from copilotd.core.attachments import (
    AttachmentCapabilities,
    AttachmentError,
    AttachmentService,
    sdk_send_frame_size,
)
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


def _path_exists(path: Path) -> bool:
    return path.exists()


def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _write_bytes(path: Path, content: bytes) -> None:
    path.write_bytes(content)


def _serialized_request_size(items: list[dict[str, object]]) -> int:
    serialized = json.dumps(
        items,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return len(serialized.encode("utf-8"))


def _blob_payload(content: bytes, filename: str, mime_type: str) -> dict[str, str]:
    return {
        "data": base64.b64encode(content).decode("ascii"),
        "displayName": filename,
        "mimeType": mime_type,
        "type": "blob",
    }


def _file_payload(path: str, display_name: str) -> dict[str, str]:
    return {
        "displayName": display_name,
        "path": path,
        "type": "file",
    }


def _safe_filename(filename: str) -> str:
    name = Path(filename).name
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return name[:160] or "attachment"


def _inline_blob_cost(content: bytes, filename: str, mime_type: str) -> int:
    return _serialized_request_size([_blob_payload(content, filename, mime_type)])


def _make_noisy_jpeg_bytes(size: int = 2048) -> bytes:
    image = Image.frombytes("RGB", (size, size), os.urandom(size * size * 3))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=95, optimize=True)
    return buffer.getvalue()


def test_sdk_send_frame_matches_pinned_jsonrpc_serialization() -> None:
    attachments = [
        {
            "type": "file",
            "path": "/tmp/文件.txt",
            "displayName": "文件.txt",
        }
    ]
    message = {
        "jsonrpc": "2.0",
        "id": "00000000-0000-0000-0000-000000000000",
        "method": "session.send",
        "params": {
            "sessionId": "session-1",
            "prompt": "你好",
            "attachments": attachments,
            "mode": "enqueue",
            "agentMode": "interactive",
        },
    }
    content = json.dumps(message, separators=(",", ":")).encode("utf-8")
    expected = len(f"Content-Length: {len(content)}\r\n\r\n".encode()) + len(content)

    assert (
        sdk_send_frame_size(
            session_id="session-1",
            prompt="你好",
            attachments=attachments,
            mode="enqueue",
            agent_mode="interactive",
            trace_context={},
        )
        == expected
    )


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
                file_max_bytes=1024 * 1024,
                message_max_bytes=1024 * 1024,
                blob_max_bytes=8,
                capabilities=AttachmentCapabilities(
                    discord_file_max_bytes=1024 * 1024,
                    discord_message_max_bytes=1024 * 1024,
                    runtime_inline_blob_max_bytes=1,
                    runtime_serialized_frame_max_bytes=1024 * 1024,
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
    assert {
        "_compress_image",
        "_atomic_write",
        "_sha256_hex",
        "_matches_integrity",
        "_serialized_request_size",
    } <= set(calls)


@pytest.mark.asyncio
async def test_runtime_serialized_frame_budget_is_cumulative_across_images_and_preserves_order(
    tmp_path: Path,
) -> None:
    image_a = _make_noisy_jpeg_bytes(2048)
    image_b = _make_noisy_jpeg_bytes(2048)
    image_c = _make_noisy_jpeg_bytes(2048)
    attachments = [
        FakeAttachment(1, "first.jpg", image_a, "image/jpeg"),
        FakeAttachment(2, "second.jpg", image_b, "image/jpeg"),
        FakeAttachment(3, "报告.jpg", image_c, "image/jpeg"),
    ]
    manifest_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "copilotd:attachment:discord-message:message-cumulative",
        )
    )
    expected_path = (
        tmp_path
        / "sessions"
        / "session-cumulative"
        / "attachments"
        / manifest_id
        / f"002-{_safe_filename(attachments[2].filename)}"
    )
    frame_budget = _serialized_request_size(
        [
            _blob_payload(attachments[0].content, attachments[0].filename, "image/jpeg"),
            _blob_payload(attachments[1].content, attachments[1].filename, "image/jpeg"),
            _file_payload(str(expected_path.resolve()), attachments[2].filename),
        ]
    )

    async with Database(tmp_path / "cumulative.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=100 * 1024 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=8 * 1024 * 1024,
                runtime_serialized_frame_max_bytes=frame_budget,
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
    assert sdk_attachments[2]["displayName"] == "报告.jpg"
    assert Path(sdk_attachments[2]["path"]) == expected_path.resolve()
    assert _serialized_request_size(sdk_attachments) == frame_budget


@pytest.mark.asyncio
async def test_exact_serialized_frame_boundary_keeps_blob_then_falls_back_one_byte_over(
    tmp_path: Path,
) -> None:
    attachment = FakeAttachment(
        1,
        "截图.jpg",
        _make_noisy_jpeg_bytes(128),
        "image/jpeg",
    )
    blob_cost = _serialized_request_size(
        [_blob_payload(attachment.content, attachment.filename, "image/jpeg")]
    )

    async with Database(tmp_path / "boundary.sqlite3") as database:
        fit_service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=100 * 1024 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=blob_cost,
                runtime_serialized_frame_max_bytes=blob_cost,
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
    assert _serialized_request_size(sdk_attachments) == blob_cost

    async with Database(tmp_path / "boundary-over.sqlite3") as database:
        over_service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=100 * 1024 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=blob_cost,
                runtime_serialized_frame_max_bytes=blob_cost - 1,
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
    assert sdk_attachments[0]["displayName"] == "截图.jpg"
    over_path = Path(sdk_attachments[0]["path"])
    assert await asyncio.to_thread(_read_bytes, over_path) == attachment.content


@pytest.mark.asyncio
async def test_capability_mode_still_enforces_file_and_message_limits(
    tmp_path: Path,
) -> None:
    attachment = FakeAttachment(
        1,
        "too-large.bin",
        b"01234567890",
        "application/octet-stream",
    )
    async with Database(tmp_path / "capability-limits.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            file_max_bytes=10,
            message_max_bytes=20,
            capabilities=AttachmentCapabilities(
                discord_file_max_bytes=10,
                discord_message_max_bytes=20,
                runtime_serialized_frame_max_bytes=1024,
            ),
        )

        with pytest.raises(AttachmentError, match="file limit"):
            await service.prepare(
                source_kind="discord-message",
                source_id="capability-limit",
                session_id="session-capability-limit",
                attachments=[attachment],
            )

    assert attachment.read_calls == 0


@pytest.mark.asyncio
async def test_file_descriptor_must_fit_runtime_frame_budget(tmp_path: Path) -> None:
    attachment = FakeAttachment(
        1,
        "small.txt",
        b"small",
        "text/plain",
    )
    async with Database(tmp_path / "descriptor-frame.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            capabilities=AttachmentCapabilities(
                runtime_serialized_frame_max_bytes=10,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="descriptor-frame",
            session_id="session-descriptor-frame",
            attachments=[attachment],
        )
        assert prepared is not None

        with pytest.raises(AttachmentError, match="runtime frame"):
            await service.sdk_attachments(prepared.manifest_id)


@pytest.mark.asyncio
async def test_later_file_can_downgrade_an_earlier_blob_to_fit_frame(
    tmp_path: Path,
) -> None:
    image = _make_noisy_jpeg_bytes(128)
    attachments = [
        FakeAttachment(1, "image.jpg", image, "image/jpeg"),
        FakeAttachment(2, "document.txt", b"document", "text/plain"),
    ]
    manifest_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "copilotd:attachment:discord-message:greedy-frame",
        )
    )
    directory = tmp_path / "sessions" / "session-greedy" / "attachments" / manifest_id
    all_files = [
        _file_payload(str((directory / "000-image.jpg").resolve()), "image.jpg"),
        _file_payload(
            str((directory / "001-document.txt").resolve()),
            "document.txt",
        ),
    ]
    frame_budget = _serialized_request_size(all_files)
    blob_then_file = [
        _blob_payload(image, "image.jpg", "image/jpeg"),
        all_files[1],
    ]
    assert _serialized_request_size(blob_then_file) > frame_budget

    async with Database(tmp_path / "greedy-frame.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=1024 * 1024,
                runtime_serialized_frame_max_bytes=frame_budget,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="greedy-frame",
            session_id="session-greedy",
            attachments=attachments,
        )
        assert prepared is not None
        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert sdk_attachments == all_files


@pytest.mark.asyncio
async def test_complete_send_frame_downgrades_blob_for_long_prompt(
    tmp_path: Path,
) -> None:
    content = _make_noisy_jpeg_bytes(256)
    attachment = FakeAttachment(1, "长文件名.jpg", content, "image/jpeg")
    prompt = "很长的提示" * 10_000
    manifest_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "copilotd:attachment:discord-message:full-send-frame",
        )
    )
    path = (
        tmp_path / "sessions" / "session-full-frame" / "attachments" / manifest_id / "000-jpg"
    ).resolve()
    file_payload = _file_payload(str(path), attachment.filename)
    blob_payload = _blob_payload(content, attachment.filename, "image/jpeg")
    file_frame_size = sdk_send_frame_size(
        session_id="session-full-frame",
        prompt=prompt,
        attachments=[file_payload],
        mode="enqueue",
        agent_mode="interactive",
        trace_context={},
    )
    attachment_only_size = _serialized_request_size([blob_payload])
    frame_budget = max(file_frame_size, attachment_only_size) + 1024
    assert (
        sdk_send_frame_size(
            session_id="session-full-frame",
            prompt=prompt,
            attachments=[blob_payload],
            mode="enqueue",
            agent_mode="interactive",
            trace_context={},
        )
        > frame_budget
    )

    async with Database(tmp_path / "full-send-frame.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=8 * 1024 * 1024,
                runtime_serialized_frame_max_bytes=frame_budget,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="full-send-frame",
            session_id="session-full-frame",
            attachments=[attachment],
        )
        assert prepared is not None
        attachment_only = await service.sdk_attachments(prepared.manifest_id)
        send_attachments = await service.sdk_attachments_for_send(
            prepared.manifest_id,
            session_id="session-full-frame",
            prompt=prompt,
            mode="enqueue",
            agent_mode="interactive",
        )

    assert attachment_only[0]["type"] == "blob"
    assert send_attachments[0]["type"] == "file"
    assert (
        sdk_send_frame_size(
            session_id="session-full-frame",
            prompt=prompt,
            attachments=send_attachments,
            mode="enqueue",
            agent_mode="interactive",
            trace_context={},
        )
        <= frame_budget
    )


@pytest.mark.asyncio
async def test_transcoded_inline_variant_keeps_original_file_fallback(
    tmp_path: Path,
) -> None:
    original_buffer = io.BytesIO()
    Image.new("RGB", (256, 256), "orange").save(original_buffer, format="BMP")
    original = original_buffer.getvalue()
    attachment = FakeAttachment(1, "screenshot.png", original, "image/bmp")
    prompt = "inspect"
    manifest_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "copilotd:attachment:discord-message:transcode-original",
        )
    )
    original_path = (
        tmp_path
        / "sessions"
        / "session-transcode"
        / "attachments"
        / manifest_id
        / "000-screenshot.png"
    ).resolve()
    file_payload = _file_payload(str(original_path), "screenshot.png")
    frame_budget = (
        sdk_send_frame_size(
            session_id="session-transcode",
            prompt=prompt,
            attachments=[file_payload],
            mode="enqueue",
            agent_mode="interactive",
            trace_context={},
        )
        + 128
    )
    async with Database(tmp_path / "transcode.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            blob_max_bytes=50 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=50 * 1024,
                runtime_serialized_frame_max_bytes=1024 * 1024,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="transcode-original",
            session_id="session-transcode",
            attachments=[attachment],
        )
        assert prepared is not None
        inline = await service.sdk_attachments(prepared.manifest_id)
        send_service = AttachmentService(
            database,
            tmp_path,
            blob_max_bytes=50 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=50 * 1024,
                runtime_serialized_frame_max_bytes=frame_budget,
            ),
        )
        send_attachments = await send_service.sdk_attachments_for_send(
            prepared.manifest_id,
            session_id="session-transcode",
            prompt=prompt,
            mode="enqueue",
            agent_mode="interactive",
        )

    assert inline[0]["type"] == "blob"
    assert inline[0]["mimeType"] == "image/jpeg"
    assert base64.b64decode(inline[0]["data"]).startswith(b"\xff\xd8")
    assert send_attachments == [file_payload]
    assert await asyncio.to_thread(_read_bytes, original_path) == original


@pytest.mark.asyncio
async def test_runtime_serialized_frame_budget_falls_back_for_two_large_images(
    tmp_path: Path,
) -> None:
    first = _make_noisy_jpeg_bytes(2048)
    second = _make_noisy_jpeg_bytes(2048)
    attachments = [
        FakeAttachment(1, "first.jpg", first, "image/jpeg"),
        FakeAttachment(2, "第二张图.jpg", second, "image/jpeg"),
    ]
    frame_budget = 7 * 1024 * 1024

    async with Database(tmp_path / "large-frame.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=100 * 1024 * 1024,
            capabilities=AttachmentCapabilities(
                runtime_inline_blob_max_bytes=8 * 1024 * 1024,
                runtime_serialized_frame_max_bytes=frame_budget,
            ),
        )
        prepared = await service.prepare(
            source_kind="discord-message",
            source_id="message-large-frame",
            session_id="session-large-frame",
            attachments=attachments,
        )
        sdk_attachments = await service.sdk_attachments(prepared.manifest_id)

    assert prepared is not None
    assert [item["type"] for item in sdk_attachments] == ["blob", "file"]
    assert sdk_attachments[1]["displayName"] == "第二张图.jpg"
    assert _serialized_request_size(sdk_attachments) <= frame_budget


@pytest.mark.asyncio
async def test_mixed_image_and_file_order_is_preserved_with_budgeted_fallback(
    tmp_path: Path,
) -> None:
    image_a = io.BytesIO()
    Image.new("RGB", (4, 4), "yellow").save(image_a, format="PNG")
    second_image = _make_noisy_jpeg_bytes(128)
    file_content = b"file-content"
    attachments = [
        FakeAttachment(1, "first.png", image_a.getvalue(), "image/png"),
        FakeAttachment(2, "document.txt", file_content, "text/plain"),
        FakeAttachment(3, "second.jpg", second_image, "image/jpeg"),
    ]
    manifest_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "copilotd:attachment:discord-message:message-mixed",
        )
    )
    directory = tmp_path / "sessions" / "session-mixed" / "attachments" / manifest_id
    budget = _serialized_request_size(
        [
            _blob_payload(
                attachments[0].content,
                attachments[0].filename,
                "image/png",
            ),
            _file_payload(
                str((directory / "001-document.txt").resolve()),
                "document.txt",
            ),
            _file_payload(
                str((directory / "002-second.jpg").resolve()),
                "second.jpg",
            ),
        ]
    )

    async with Database(tmp_path / "mixed.sqlite3") as database:
        service = AttachmentService(
            database,
            tmp_path,
            message_max_bytes=100 * 1024 * 1024,
            capabilities=AttachmentCapabilities(
                discord_message_max_bytes=100 * 1024 * 1024,
                runtime_inline_blob_max_bytes=8 * 1024 * 1024,
                runtime_serialized_frame_max_bytes=budget,
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
