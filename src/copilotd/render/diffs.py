from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from copilotd.render.tables import TableAsset


@dataclass(frozen=True, slots=True)
class DiffRenderPlan:
    source: str
    content: str
    assets: tuple[TableAsset, ...]
    byte_count: int


async def render_diff(
    *,
    structured_result: Mapping[str, Any] | None = None,
    cwd: Path | None = None,
    inline_limit: int = 1600,
    output_limit: int = 8 * 1024 * 1024,
) -> DiffRenderPlan | None:
    patch = _structured_patch(structured_result)
    source = "structured"
    if patch is None and cwd is not None:
        resolved = await asyncio.to_thread(lambda: cwd.expanduser().resolve())
        if not await asyncio.to_thread(resolved.is_dir):
            raise ValueError(f"diff working directory does not exist: {resolved}")
        process = await asyncio.create_subprocess_exec(
            "git",
            "-C",
            str(resolved),
            "--no-pager",
            "diff",
            "--no-ext-diff",
            "--",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace")[:1000]
            raise RuntimeError(f"git diff failed ({process.returncode}): {detail}")
        if len(stdout) > output_limit:
            raise RuntimeError(f"git diff exceeds the {output_limit}-byte safety limit")
        patch = stdout.decode("utf-8", errors="replace")
        source = "local-git"
    if patch is None or not patch:
        return None
    encoded = patch.encode("utf-8")
    if len(patch) <= inline_limit and "```" not in patch:
        return DiffRenderPlan(
            source=source,
            content=f"**Code changes** · `{source}`\n```diff\n{patch}\n```",
            assets=(),
            byte_count=len(encoded),
        )
    asset = TableAsset(
        filename="changes.diff",
        media_type="text/x-diff",
        content=encoded,
    )
    return DiffRenderPlan(
        source=source,
        content=f"**Code changes** · `{source}` attached as `{asset.filename}`.",
        assets=(asset,),
        byte_count=len(encoded),
    )


def _structured_patch(result: Mapping[str, Any] | None) -> str | None:
    if result is None:
        return None
    for key in ("diff", "patch", "unifiedDiff"):
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
    nested = result.get("structuredContent")
    if isinstance(nested, Mapping):
        return _structured_patch(nested)
    return None
