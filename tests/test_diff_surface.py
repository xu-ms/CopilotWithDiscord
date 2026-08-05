from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from copilotd.render.diffs import render_diff


async def _git(*arguments: str, cwd: Path) -> None:
    process = await asyncio.create_subprocess_exec(
        "git",
        *arguments,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode()


class _FakeStream:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    async def read(self, n: int) -> bytes:
        if self._offset >= len(self._payload):
            return b""
        size = min(n, len(self._payload) - self._offset)
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += size
        await asyncio.sleep(0)
        return chunk


class _FakeProcess:
    def __init__(self, stdout: bytes, stderr: bytes = b"", *, auto_finish: bool) -> None:
        self.stdout = _FakeStream(stdout)
        self.stderr = _FakeStream(stderr)
        self.returncode = None
        self.terminated = False
        self.killed = False
        self._wait_event = asyncio.Event()
        self._auto_finish = auto_finish

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9
        self._wait_event.set()

    async def wait(self) -> int:
        if self._auto_finish and self.returncode is None:
            self.returncode = 0
            self._wait_event.set()
            return self.returncode
        await self._wait_event.wait()
        return self.returncode or 0


@pytest.mark.asyncio
async def test_diff_renderer_prefers_structured_and_safely_falls_back_to_local_git(
    tmp_path: Path,
) -> None:
    structured = await render_diff(structured_result={"patch": "+safe line"})
    fenced = await render_diff(structured_result={"patch": "+```\n+code\n+```"})
    with pytest.raises(RuntimeError, match="safety limit"):
        await render_diff(structured_result={"patch": "+" + "x" * 5000}, output_limit=1024)

    repository = tmp_path / "repo"
    await asyncio.to_thread(repository.mkdir)
    await _git("init", "-q", cwd=repository)
    await _git("config", "user.email", "copilotd@example.invalid", cwd=repository)
    await _git("config", "user.name", "copilotD test", cwd=repository)
    path = repository / "file.txt"
    await asyncio.to_thread(path.write_text, "before\n", encoding="utf-8")
    await _git("add", "file.txt", cwd=repository)
    await _git("commit", "-qm", "baseline", cwd=repository)
    await asyncio.to_thread(path.write_text, "after\n", encoding="utf-8")
    local = await render_diff(cwd=repository)

    assert structured is not None
    assert structured.source == "structured"
    assert "```diff" in structured.content
    assert fenced is not None
    assert fenced.assets[0].content == b"+```\n+code\n+```"
    assert local is not None
    assert local.source == "local-git"
    assert "-before" in local.content
    assert "+after" in local.content


@pytest.mark.asyncio
async def test_non_utf8_git_output_uses_replacement_text_and_raw_bytes_attachment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw = b"diff --git a/file b/file\nindex 1..2\n+\x80\xff\n"

    async def factory(*args: object, **kwargs: object) -> _FakeProcess:
        return _FakeProcess(raw, auto_finish=True)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", factory)

    inline = await render_diff(cwd=tmp_path, inline_limit=10_000, output_limit=10_000)
    assert inline is not None
    assert "\ufffd" in inline.content
    inline.content.encode("utf-8")
    assert inline.byte_count == len(raw)

    attachment = await render_diff(cwd=tmp_path, inline_limit=0, output_limit=10_000)
    assert attachment is not None
    assert attachment.assets[0].content == raw
    assert attachment.content.endswith("changes.diff`.")


@pytest.mark.asyncio
async def test_local_git_diff_streaming_enforces_cap_and_terminates_the_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repo"
    await asyncio.to_thread(repository.mkdir)
    await _git("init", "-q", cwd=repository)
    await _git("config", "user.email", "copilotd@example.invalid", cwd=repository)
    await _git("config", "user.name", "copilotD test", cwd=repository)
    path = repository / "huge.txt"
    before = "\n".join(f"before-{index}" for index in range(1500)) + "\n"
    after = "\n".join(f"after-{index}" for index in range(1500)) + "\n"
    await asyncio.to_thread(path.write_text, before, encoding="utf-8")
    await _git("add", "huge.txt", cwd=repository)
    await _git("commit", "-qm", "baseline", cwd=repository)
    await asyncio.to_thread(path.write_text, after, encoding="utf-8")

    process = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        str(repository),
        "--no-pager",
        "diff",
        "--no-ext-diff",
        "--",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await process.communicate()
    assert process.returncode == 0, stderr_bytes.decode()
    assert len(stdout_bytes) > 1024

    created: list[_FakeProcess] = []

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> _FakeProcess:
        fake = _FakeProcess(stdout_bytes, auto_finish=False)
        created.append(fake)
        return fake

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match="safety limit"):
        await render_diff(cwd=repository, output_limit=1024)

    assert created and created[0].terminated is True
    assert created[0].killed is True
