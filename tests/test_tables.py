from collections import Counter

import pytest

from copilotd.render.markdown import MarkdownAssembler, TableBlock, TextBlock, split_table_row
from copilotd.render.tables import parse_table, render_table


def test_streaming_assembler_holds_large_table_and_preserves_block_order() -> None:
    rows = "\n".join(f"| row-{index} | {'数据' * 30} |" for index in range(60))
    content = f"before\n\n| Name | Value |\n| --- | --- |\n{rows}\n\nafter"
    assembler = MarkdownAssembler()
    blocks = []
    for index in range(0, len(content), 137):
        emitted = assembler.append(content[index : index + 137])
        blocks.extend(emitted)
        assert sum(isinstance(block, TableBlock) for block in blocks) <= 1
    blocks.extend(assembler.finalize(content))

    kinds = [type(block) for block in blocks]
    assert kinds.count(TableBlock) == 1
    table_index = kinds.index(TableBlock)
    assert "before" in "".join(
        block.content for block in blocks[:table_index] if isinstance(block, TextBlock)
    )
    assert "after" in "".join(
        block.content for block in blocks[table_index + 1 :] if isinstance(block, TextBlock)
    )
    table = next(block for block in blocks if isinstance(block, TableBlock))
    assert table.markdown.count("\n") == 61


def test_table_splitter_handles_escaped_and_inline_code_pipes() -> None:
    cells = split_table_row(r"| left\|right | `a|b` | plain |")

    assert cells == [r"left\|right", "`a|b`", "plain"]
    parsed = parse_table(
        r"""
| Name | Value |
| :--- | ---: |
| left\|right | `a|b` |
"""
    )
    assert parsed.headers == ("Name", "Value")
    assert parsed.rows == (("left|right", "`a|b`"),)
    assert [alignment.value for alignment in parsed.alignments] == ["left", "right"]


@pytest.mark.asyncio
async def test_small_table_uses_copyable_code_block() -> None:
    plan = await render_table(
        """
| Name | Count |
| --- | ---: |
| alpha | 1 |
| beta | 20 |
"""
    )

    assert plan.carrier == "code"
    assert plan.preview_text is not None
    assert "alpha" in plan.preview_text
    assert plan.assets == ()


@pytest.mark.asyncio
async def test_medium_cjk_table_uses_png_and_markdown_source() -> None:
    headers = "| 名称 | 状态 | 负责人 | 耗时 | 备注 |"
    delimiter = "| --- | --- | --- | ---: | --- |"
    rows = "\n".join(
        f"| 任务{index} | 完成 | 张三 | {index} | 保留完整可复制内容 |"
        for index in range(13)
    )
    plan = await render_table(f"{headers}\n{delimiter}\n{rows}")
    media = Counter(asset.media_type for asset in plan.assets)

    assert plan.carrier == "image"
    assert media["image/png"] >= 1
    assert media["text/markdown"] == 1
    png = next(asset for asset in plan.assets if asset.media_type == "image/png")
    assert png.content.startswith(b"\x89PNG\r\n\x1a\n")


@pytest.mark.asyncio
async def test_large_scalar_table_gets_preview_markdown_and_csv() -> None:
    headers = "| " + " | ".join(f"C{index}" for index in range(9)) + " |"
    delimiter = "|" + "|".join(" --- " for _ in range(9)) + "|"
    rows = "\n".join(
        "| " + " | ".join(str(row * 10 + column) for column in range(9)) + " |"
        for row in range(60)
    )
    plan = await render_table(f"{headers}\n{delimiter}\n{rows}")
    media = {asset.media_type for asset in plan.assets}

    assert plan.preview_truncated
    assert plan.row_count == 60
    assert "text/markdown" in media
    assert "text/csv" in media
    if plan.carrier == "image":
        assert "image/png" in media


def test_invalid_delimiter_is_rejected() -> None:
    with pytest.raises(ValueError, match="delimiter"):
        parse_table(
            """
| A | B |
| -- | --- |
| 1 | 2 |
"""
        )
