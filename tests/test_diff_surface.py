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


@pytest.mark.asyncio
async def test_diff_renderer_prefers_structured_and_safely_falls_back_to_local_git(
    tmp_path: Path,
) -> None:
    structured = await render_diff(structured_result={"patch": "+safe line"})
    fenced = await render_diff(structured_result={"patch": "+```\n+code\n+```"})

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
