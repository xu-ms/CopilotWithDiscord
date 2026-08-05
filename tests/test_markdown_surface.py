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
    paragraph = blocks[0]
    assert paragraph.span == MarkdownSpan(1, 1)
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
async def test_local_markdown_image_extraction_skips_fenced_inline_escaped_literals_and_batches(
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    images = []
    for index in range(11):
        image = root / f"img{index:02d}.png"
        _write_png(image)
        images.append(image)
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"outside")

    source = (
        f"intro ![one]({images[0].name}) and again ![one]({images[0].name})\n"
        f"```text\n![code]({images[2].name})\n```\n"
        f"`![inline]({images[3].name})` and \\![escaped]({images[4].name})\n"
        f"![two]({images[1].name})\n"
        f"![missing](missing.png)\n"
        f"![outside]({outside})\n"
        f"![remote](https://example.com/image.png)\n"
        + " ".join(f"![{index}]({image.name})" for index, image in enumerate(images[2:], start=2))
    )

    plan = extract_local_markdown_images(source, allowed_roots=[root])

    assert isinstance(plan, MarkdownImageExtractionPlan)
    assert len(plan.attachments) == 12
    assert len(plan.batches) == 2
    assert len(plan.batches[0]) == 10
    assert len(plan.batches[1]) == 2
    assert plan.attachments[0].resolved_path == str(images[0].resolve())
    assert plan.attachments[0].batch_index == 1
    assert plan.attachments[-1].batch_index == 2
    assert f"![code]({images[2].name})" in plan.content
    assert f"`![inline]({images[3].name})`" in plan.content
    assert "\\![escaped](" in plan.content
    assert "missing.png" in plan.content
    assert str(outside) in plan.content
    assert "https://example.com/image.png" in plan.content
    assert {warning.kind for warning in plan.warnings} == {
        "missing-image",
        "invalid-root",
        "invalid-target",
    }
    assert "![one]" not in plan.content
    assert "![two]" not in plan.content
