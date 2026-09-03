from __future__ import annotations

import asyncio
from pathlib import Path

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


def test_installed_unicode_fonts_do_not_render_missing_glyph_boxes() -> None:
    assert not tables._font_supports(tables.ImageFont.load_default(), "中")
    resolver = tables._FontResolver(scale=2)
    try:
        chinese = resolver.font_for("中", code=False)
    except OSError:
        pass
    else:
        assert tables._font_supports(chinese, "中")
        assert bytes(chinese.getmask("中")) != bytes(chinese.getmask("文"))
    try:
        emoji = resolver.font_for("🙂", code=False)
    except OSError:
        pass
    else:
        assert tables._font_supports(emoji, "🙂")
        assert bytes(emoji.getmask("🙂")) != bytes(emoji.getmask("\U0010ffff"))
    assert resolver.glyph_width("\ufe0f", code=False) == 0
    assert resolver.glyph_width("\u200d", code=False) == 0
    assert resolver.text_width("e\u0301", code=False) == resolver.text_width("é", code=False)
    assert tables._text_clusters("e\u0301👨\u200d👩") == ("é", "👨\u200d👩")
    assert resolver.text_width("©︎", code=False) == resolver.text_width("©", code=False)
    if resolver._emoji_font is not None:
        assert resolver.font_for("©️", code=False) is resolver._emoji_font
        assert resolver.text_width("©️", code=False) < resolver.text_width("©", code=False) * 2


def test_font_resolver_accepts_fixed_strike_emoji_font(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_strike = tmp_path / "NotoColorEmoji.ttf"
    fixed_strike.write_bytes(b"font fixture")
    default_font = tables.ImageFont.load_default()
    monkeypatch.setattr(
        tables._FontResolver,
        "_load_font",
        staticmethod(lambda _candidates, _size: default_font),
    )
    monkeypatch.setattr(tables, "_CJK_FONT_CANDIDATES", ())
    monkeypatch.setattr(tables, "_EMOJI_FONT_CANDIDATES", (fixed_strike,))

    def load_fixed_strike(path: str, *, size: int) -> tables.ImageFont.ImageFont:
        assert path == str(fixed_strike)
        if size != 109:
            raise OSError("invalid pixel size")
        return default_font

    monkeypatch.setattr(tables.ImageFont, "truetype", load_fixed_strike)

    resolver = tables._FontResolver(scale=2)

    assert resolver._emoji_font is default_font
    assert resolver._emoji_render_scale == pytest.approx(28 / 109)


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
