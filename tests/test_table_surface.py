from __future__ import annotations

import asyncio

import pytest

import copilotd.render.tables as tables
from copilotd.render.markdown import MarkdownSpan
from copilotd.render.tables import (
    ParsedTable,
    TableAlignment,
    TableAsset,
    parse_table,
    render_table_sync,
)


def test_parse_table_preserves_raw_source_ranges_and_alignment() -> None:
    parsed = parse_table(
        """
        | Name | Value |
        | :--- | ---: |
        | left\\|right | `a|b` |
        | plain | 2 |
        """
    )

    assert isinstance(parsed, ParsedTable)
    assert parsed.headers == ("Name", "Value")
    assert parsed.header_raw == ("Name", "Value")
    assert parsed.row_raw == (("left\\|right", "`a|b`"), ("plain", "2"))
    assert parsed.alignments == (TableAlignment.LEFT, TableAlignment.RIGHT)
    assert parsed.source_span == MarkdownSpan(1, 4)
    assert parsed.row_spans == (MarkdownSpan(3, 3), MarkdownSpan(4, 4))
    assert parsed.source_hash == parse_table(parsed.markdown).source_hash


@pytest.mark.asyncio
async def test_render_table_is_cacheable_and_handles_unicode_cells() -> None:
    markdown = """
    | 名称 | 状态 | 备注 |
    | --- | --- | --- |
    | 任务一 | ✅ | `inline code` + emoji 🙂 |
    | 任务二 | 完成 | CJK 文本 |
    """

    plan1 = render_table_sync(markdown)
    plan2 = await asyncio.to_thread(render_table_sync, markdown)

    assert plan1 is plan2
    assert plan1.source_hash == plan2.source_hash
    assert plan1.assets == plan2.assets
    if plan1.carrier == "image":
        assert any(asset.media_type == "image/png" for asset in plan1.assets)
        assert plan1.assets[0].content.startswith(b"\x89PNG\r\n\x1a\n")
    else:
        assert plan1.preview_text is not None or plan1.assets[0].media_type == "text/markdown"


def test_pagination_contract_and_upload_fallback_do_not_chunk_png_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    markdown = "| A | B | C | D | E |\n| --- | --- | --- | --- | --- |\n| 1 | 2 | 3 | 4 | 5 |\n"
    png_pages = tuple(
        TableAsset(
            filename=f"table-fake-{index}.png",
            media_type="image/png",
            content=b"\x89PNG\r\n\x1a\n" + bytes([index]),
        )
        for index in range(11)
    )

    def fake_pages(_table: object, _source_hash: str) -> tuple[tuple[TableAsset, ...], bool]:
        return png_pages, False

    monkeypatch.setattr(tables, "_render_png_pages", fake_pages)
    plan = render_table_sync(markdown)

    assert plan.carrier == "image"
    assert plan.preview_truncated is True
    assert plan.source_contract is not None
    assert plan.source_contract["preview_only"] is True
    assert plan.assets[0].media_type == "image/png"
    assert plan.assets[1].media_type == "text/markdown"
    assert len([asset for asset in plan.assets if asset.media_type == "image/png"]) == 1

    def fake_large_pages(_table: object, _source_hash: str) -> tuple[tuple[TableAsset, ...], bool]:
        return (
            TableAsset(
                filename="table-big.png",
                media_type="image/png",
                content=b"\x89PNG\r\n\x1a\n" + b"x" * 256,
            ),
        ), False

    monkeypatch.setattr(tables, "_render_png_pages", fake_large_pages)
    fallback = render_table_sync(markdown, max_upload_bytes=32)

    assert fallback.carrier == "attachment-fallback"
    assert all(asset.media_type != "image/png" for asset in fallback.assets)
    assert fallback.assets[0].media_type == "text/markdown"
