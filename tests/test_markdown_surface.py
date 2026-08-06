from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from PIL import Image

from copilotd.render.markdown import (
    MarkdownBlockquote,
    MarkdownFence,
    MarkdownImageExtractionPlan,
    MarkdownListBlock,
    MarkdownMessagePlan,
    MarkdownParagraph,
    MarkdownSpan,
    MarkdownTableCandidate,
    MarkdownThematicBreak,
    extract_local_markdown_images,
    parse_markdown_blocks,
    plan_markdown_messages,
)


def _write_png(path: Path) -> None:
    Image.new("RGB", (1, 1), (255, 0, 0)).save(path)


def test_markdown_ast_parses_structural_blocks_and_spans() -> None:
    source = (
        "Paragraph one\n"
        "\n"
        "- item one\n"
        "  continuation line\n"
        "- item two\n"
        "\n"
        "> quote one\n"
        "> quote two\n"
        "\n"
        "```python\n"
        "print('x')\n"
        "```\n"
        "\n"
        "| A | B |\n"
        "| --- | :---: |\n"
        "| 1 | 2 |\n"
        "\n"
        "---\n"
    )

    blocks = parse_markdown_blocks(source)

    assert [type(block) for block in blocks] == [
        MarkdownParagraph,
        MarkdownListBlock,
        MarkdownBlockquote,
        MarkdownFence,
        MarkdownTableCandidate,
        MarkdownThematicBreak,
    ]
    assert blocks[0].span == MarkdownSpan(1, 1)
    list_block = blocks[1]
    assert list_block.span == MarkdownSpan(3, 6)
    assert len(list_block.items) == 2
    assert list_block.items[0].continuation_lines == ("  continuation line",)
    quote = blocks[2]
    assert quote.lines == ("quote one", "quote two")
    fence = blocks[3]
    assert fence.info == "python"
    assert fence.code == "print('x')"
    table = blocks[4]
    assert table.headers == ("A", "B")
    assert table.alignments == ("left", "center")
    assert table.rows == (("1", "2"),)
    assert table.raw_rows == (("1", "2"),)
    assert table.span == MarkdownSpan(14, 16)
    assert blocks[5].raw == "---"


@pytest.mark.asyncio
async def test_markdown_split_plan_adds_continuation_marker_and_attaches_oversized_block() -> None:
    plan = await asyncio.to_thread(plan_markdown_messages, "x" * 19 + "\n\ny", max_chars=20)
    assert isinstance(plan, MarkdownMessagePlan)
    assert len(plan.segments) == 2
    assert plan.segments[0].content == "x" * 19
    assert plan.segments[1].content.startswith("… continued …")
    assert plan.segments[1].content.endswith("y")
    assert all(len(segment.content) <= 20 for segment in plan.segments)

    oversized = await asyncio.to_thread(plan_markdown_messages, "x" * 2100, max_chars=1850)
    assert len(oversized.segments) == 1
    assert oversized.segments[0].attachments[0].filename.endswith(".md")
    assert oversized.segments[0].attachments[0].content == "x" * 2100
    assert oversized.truncated is True


@pytest.mark.asyncio
async def test_local_markdown_image_extraction_ignores_code_containers_and_batches(
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    images = []
    for index in range(13):
        image = root / f"img{index:02d}.png"
        _write_png(image)
        images.append(image)
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"outside")

    source = (
        f"> quoted ![quote]({images[0].name})\n"
        f"![one]({images[1].name}) and ![two]({images[2].name})\n"
        f"> ```\n> ![blocked-fence]({images[3].name})\n> ```\n"
        f"    ![blocked-indent]({images[4].name})\n"
        f"`![inline]({images[5].name})` and ``![nested]({images[6].name})``\n"
        f"`code ``![mismatched]({images[5].name})`` still code`\n"
        f"\\![escaped]({images[7].name})\n"
        f"![three]({images[8].name}) ![four]({images[9].name})\n"
        f"![five]({images[10].name}) ![six]({images[11].name})\n"
        f"![seven]({images[12].name}) ![outside]({outside})\n"
    )

    plan = extract_local_markdown_images(source, allowed_roots=[root])

    assert isinstance(plan, MarkdownImageExtractionPlan)
    assert len(plan.attachments) == 8
    assert len(plan.batches) == 1
    assert [attachment.filename for attachment in plan.attachments] == [
        f"img{index:02d}.png" for index in [0, 1, 2, 8, 9, 10, 11, 12]
    ]
    assert plan.attachments[0].source.startswith("![quote]")
    assert "![blocked-fence]" in plan.content
    assert "![blocked-indent]" in plan.content
    assert "`![inline]" in plan.content
    assert "``![nested]" in plan.content
    assert "![mismatched]" in plan.content
    assert "\\![escaped]" in plan.content
    assert "![quote]" not in plan.content
    assert "![one]" not in plan.content
    assert "![outside]" in plan.content
    assert {warning.kind for warning in plan.warnings} == {"invalid-root"}


@pytest.mark.asyncio
async def test_local_markdown_image_extraction_ends_container_scoped_fences_at_boundary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    quoted_hidden = root / "quoted-hidden.png"
    quoted_visible = root / "quoted-visible.png"
    list_hidden = root / "list-hidden.png"
    list_visible = root / "list-visible.png"
    for path_item in (quoted_hidden, quoted_visible, list_hidden, list_visible):
        _write_png(path_item)

    source = (
        f"> ````\n> ![quoted-hidden]({quoted_hidden.name})\n"
        f"![quoted-visible]({quoted_visible.name})\n"
        f"- item\n"
        f"    `````\n    ![list-hidden]({list_hidden.name})\n"
        f"![list-visible]({list_visible.name})\n"
    )

    plan = extract_local_markdown_images(source, allowed_roots=[root])

    assert isinstance(plan, MarkdownImageExtractionPlan)
    assert [attachment.filename for attachment in plan.attachments] == [
        quoted_visible.name,
        list_visible.name,
    ]
    assert len(plan.batches) == 1
    assert "![quoted-hidden]" in plan.content
    assert "![list-hidden]" in plan.content
    assert "![quoted-visible]" not in plan.content
    assert "![list-visible]" not in plan.content
    assert plan.warnings == ()


@pytest.mark.asyncio
async def test_local_image_extraction_preserves_container_prefixes_and_nested_boundaries(
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    quoted = root / "quoted.png"
    listed = root / "listed.png"
    visible = root / "visible.png"
    nested_hidden = root / "nested-hidden.png"
    inline_boundary = root / "inline-boundary.png"
    for path_item in (quoted, listed, visible, nested_hidden, inline_boundary):
        _write_png(path_item)
    source = (
        f"> quoted ![quoted]({quoted.name})\r\n"
        f"- listed ![listed]({listed.name})\n"
        "    ```\n"
        f"    ![hidden]({visible.name})\n"
        f"- > ```\n  > ![nested-hidden]({nested_hidden.name})\n"
        f"> `unclosed\n![inline-boundary]({inline_boundary.name})\n"
        f"outside ![visible]({visible.name})\n"
    )

    plan = extract_local_markdown_images(source, allowed_roots=[root])

    assert [item.filename for item in plan.attachments] == [
        quoted.name,
        listed.name,
        inline_boundary.name,
        visible.name,
    ]
    assert plan.content.startswith("> quoted \r\n- listed \n")
    assert f"    ![hidden]({visible.name})" in plan.content
    assert f"  > ![nested-hidden]({nested_hidden.name})" in plan.content
    assert plan.content.endswith("outside \n")


@pytest.mark.asyncio
async def test_local_markdown_image_extraction_handles_multiline_code_spans_and_blockquote_fences(
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    visible = root / "visible.png"
    hidden_single = root / "hidden-single.png"
    hidden_double = root / "hidden-double.png"
    hidden_quote = root / "hidden-quote.png"
    for path_item in (visible, hidden_single, hidden_double, hidden_quote):
        _write_png(path_item)

    source = (
        f"`single start\n![hidden-single]({hidden_single.name})\nsingle end`\n"
        f"``double start\n![hidden-double]({hidden_double.name})\ndouble end``\n"
        f"> ```\n> ![hidden-quote]({hidden_quote.name})\n> ```\n"
        f"![visible]({visible.name})\n"
    )

    plan = extract_local_markdown_images(source, allowed_roots=[root])

    assert len(plan.attachments) == 1
    assert plan.attachments[0].resolved_path == str(visible.resolve())
    assert "![hidden-single]" in plan.content
    assert "![hidden-double]" in plan.content
    assert "![hidden-quote]" in plan.content
    assert "![visible]" not in plan.content
    assert plan.warnings == ()
