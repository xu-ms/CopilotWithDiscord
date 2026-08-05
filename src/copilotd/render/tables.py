from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from copilotd.render.markdown import split_table_row

_DELIMITER = re.compile(r"^(:)?-{3,}(:)?$")
_NON_SCALAR_MARKDOWN = re.compile(r"[*_`<>\n]")


class TableAlignment(StrEnum):
    LEFT = "left"
    CENTER = "center"
    RIGHT = "right"


@dataclass(frozen=True, slots=True)
class ParsedTable:
    headers: tuple[str, ...]
    alignments: tuple[TableAlignment, ...]
    rows: tuple[tuple[str, ...], ...]
    markdown: str


@dataclass(frozen=True, slots=True)
class TableAsset:
    filename: str
    media_type: str
    content: bytes


@dataclass(frozen=True, slots=True)
class TableRenderPlan:
    carrier: str
    preview_text: str | None
    assets: tuple[TableAsset, ...]
    row_count: int
    column_count: int
    source_hash: str
    preview_truncated: bool = False


async def render_table(markdown: str) -> TableRenderPlan:
    return await asyncio.to_thread(_render_table_sync, markdown)


def parse_table(markdown: str) -> ParsedTable:
    lines = [line for line in markdown.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        raise ValueError("table requires a header and delimiter row")
    headers = split_table_row(lines[0])
    delimiter_cells = split_table_row(lines[1])
    if len(headers) < 2 or len(headers) != len(delimiter_cells):
        raise ValueError("table header and delimiter widths differ")
    alignments = tuple(_parse_alignment(cell) for cell in delimiter_cells)
    rows: list[tuple[str, ...]] = []
    for line in lines[2:]:
        cells = split_table_row(line)
        if len(cells) > len(headers):
            raise ValueError("table row has more cells than its header")
        cells.extend("" for _ in range(len(headers) - len(cells)))
        rows.append(tuple(_display_cell(cell) for cell in cells))
    return ParsedTable(
        headers=tuple(_display_cell(cell) for cell in headers),
        alignments=alignments,
        rows=tuple(rows),
        markdown=markdown.strip() + "\n",
    )


def _render_table_sync(markdown: str) -> TableRenderPlan:
    table = parse_table(markdown)
    source_hash = hashlib.sha256(table.markdown.encode()).hexdigest()
    width = _formatted_width(table)
    if len(table.headers) <= 4 and len(table.rows) <= 12 and width <= 88:
        return TableRenderPlan(
            carrier="code",
            preview_text=f"```\n{_format_code_table(table)}\n```",
            assets=(),
            row_count=len(table.rows),
            column_count=len(table.headers),
            source_hash=source_hash,
        )

    markdown_asset = TableAsset(
        filename=f"table-{source_hash[:12]}.md",
        media_type="text/markdown",
        content=table.markdown.encode(),
    )
    preview_rows = table.rows
    large = len(table.headers) > 8 or len(table.rows) > 50
    if large:
        preview_rows = table.rows[:20]
    preview = ParsedTable(
        headers=table.headers,
        alignments=table.alignments,
        rows=preview_rows,
        markdown=table.markdown,
    )

    try:
        png_pages, pagination_truncated = _render_png_pages(preview, source_hash)
    except (OSError, UnicodeError):
        code = _format_code_table(preview)
        preview_text = f"```\n{code}\n```" if len(code) <= 1800 else None
        return TableRenderPlan(
            carrier="code-fallback" if preview_text else "attachment-fallback",
            preview_text=preview_text,
            assets=(markdown_asset,),
            row_count=len(table.rows),
            column_count=len(table.headers),
            source_hash=source_hash,
            preview_truncated=large or preview_text is None,
        )

    assets: list[TableAsset] = list(png_pages)
    assets.append(markdown_asset)
    if large and _is_scalar_table(table):
        assets.append(
            TableAsset(
                filename=f"table-{source_hash[:12]}.csv",
                media_type="text/csv",
                content=_to_csv(table),
            )
        )
    return TableRenderPlan(
        carrier="image",
        preview_text=None,
        assets=tuple(assets),
        row_count=len(table.rows),
        column_count=len(table.headers),
        source_hash=source_hash,
        preview_truncated=large or pagination_truncated,
    )


def _parse_alignment(cell: str) -> TableAlignment:
    value = cell.strip()
    match = _DELIMITER.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid table delimiter cell: {cell}")
    if match.group(1) and match.group(2):
        return TableAlignment.CENTER
    if match.group(2):
        return TableAlignment.RIGHT
    return TableAlignment.LEFT


def _display_cell(cell: str) -> str:
    return cell.replace("\\|", "|").strip()


def _formatted_width(table: ParsedTable) -> int:
    widths = [_display_width(header) for header in table.headers]
    for row in table.rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], _display_width(cell))
    return sum(widths) + 3 * (len(widths) - 1)


def _format_code_table(table: ParsedTable) -> str:
    all_rows = [table.headers, *table.rows]
    widths = [
        max(_display_width(row[index]) for row in all_rows)
        for index in range(len(table.headers))
    ]

    def format_row(row: tuple[str, ...]) -> str:
        cells = [
            _align_display(row[index], widths[index], table.alignments[index])
            for index in range(len(widths))
        ]
        return " | ".join(cells).rstrip()

    separator = "-+-".join("-" * width for width in widths)
    return "\n".join(
        (format_row(table.headers), separator, *(format_row(row) for row in table.rows))
    )


def _align_display(value: str, width: int, alignment: TableAlignment) -> str:
    missing = width - _display_width(value)
    if alignment == TableAlignment.RIGHT:
        return " " * missing + value
    if alignment == TableAlignment.CENTER:
        left = missing // 2
        return " " * left + value + " " * (missing - left)
    return value + " " * missing


def _display_width(value: str) -> int:
    return sum(
        2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1
        for character in value
    )


def _render_png_pages(
    table: ParsedTable,
    source_hash: str,
) -> tuple[tuple[TableAsset, ...], bool]:
    scale = 2
    font = _load_font(14 * scale)
    line_height = 20 * scale
    padding_x = 12 * scale
    padding_y = 8 * scale
    max_width = 4096
    max_height = 4096

    natural_widths = []
    for index, header in enumerate(table.headers):
        values = [header, *(row[index] for row in table.rows)]
        natural = max(_text_width(font, value) for value in values) + padding_x * 2
        natural_widths.append(min(max(natural, 120 * scale), 520 * scale))
    total_width = sum(natural_widths) + 1
    if total_width > max_width:
        ratio = max_width / total_width
        natural_widths = [max(int(width * ratio), 80 * scale) for width in natural_widths]
        total_width = sum(natural_widths) + 1
    if total_width > max_width:
        raise OSError("table is too wide for the PNG renderer")

    wrapped_header = _wrap_row(table.headers, natural_widths, font, padding_x)
    wrapped_rows = [
        _wrap_row(row, natural_widths, font, padding_x)
        for row in table.rows
    ]
    header_height = max(len(lines) for lines in wrapped_header) * line_height + padding_y * 2
    row_heights = [
        max(len(lines) for lines in wrapped) * line_height + padding_y * 2
        for wrapped in wrapped_rows
    ]

    pages: list[tuple[int, int]] = []
    start = 0
    while start < len(wrapped_rows) or (not wrapped_rows and not pages):
        height = header_height + 1
        end = start
        while end < len(wrapped_rows) and height + row_heights[end] <= max_height:
            height += row_heights[end]
            end += 1
        if end == start and end < len(wrapped_rows):
            end += 1
        pages.append((start, end))
        start = end
    truncated = len(pages) > 10
    if truncated:
        pages = pages[:1]

    assets: list[TableAsset] = []
    for page_index, (start, end) in enumerate(pages, start=1):
        page_rows = wrapped_rows[start:end]
        page_heights = row_heights[start:end]
        image_height = header_height + sum(page_heights) + 1
        image = Image.new("RGB", (total_width, image_height), "#ffffff")
        draw = ImageDraw.Draw(image)
        y = 0
        _draw_row(
            draw,
            wrapped_header,
            table.alignments,
            natural_widths,
            y,
            header_height,
            font,
            line_height,
            padding_x,
            padding_y,
            fill="#e8eef8",
        )
        y += header_height
        for row_index, (wrapped, row_height) in enumerate(
            zip(page_rows, page_heights, strict=True),
            start=start,
        ):
            _draw_row(
                draw,
                wrapped,
                table.alignments,
                natural_widths,
                y,
                row_height,
                font,
                line_height,
                padding_x,
                padding_y,
                fill="#f7f9fc" if row_index % 2 else "#ffffff",
            )
            y += row_height
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", optimize=True)
        assets.append(
            TableAsset(
                filename=f"table-{source_hash[:12]}-{page_index}.png",
                media_type="image/png",
                content=buffer.getvalue(),
            )
        )
    return tuple(assets), truncated


def _wrap_row(
    row: tuple[str, ...],
    widths: list[int],
    font: ImageFont.ImageFont,
    padding_x: int,
) -> tuple[tuple[str, ...], ...]:
    return tuple(
        tuple(_wrap_text(cell, font, max(width - padding_x * 2, 1)))
        for cell, width in zip(row, widths, strict=True)
    )


def _wrap_text(value: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    if not value:
        return [""]
    lines: list[str] = []
    for source_line in value.splitlines() or [""]:
        current = ""
        for character in source_line:
            candidate = current + character
            if current and _text_width(font, candidate) > max_width:
                lines.append(current)
                current = character
            else:
                current = candidate
        lines.append(current)
    return lines


def _draw_row(
    draw: ImageDraw.ImageDraw,
    wrapped: tuple[tuple[str, ...], ...],
    alignments: tuple[TableAlignment, ...],
    widths: list[int],
    y: int,
    row_height: int,
    font: ImageFont.ImageFont,
    line_height: int,
    padding_x: int,
    padding_y: int,
    *,
    fill: str,
) -> None:
    x = 0
    for lines, alignment, width in zip(wrapped, alignments, widths, strict=True):
        draw.rectangle((x, y, x + width, y + row_height), fill=fill, outline="#c7d0df")
        for line_index, line in enumerate(lines):
            text_width = _text_width(font, line)
            if alignment == TableAlignment.RIGHT:
                text_x = x + width - padding_x - text_width
            elif alignment == TableAlignment.CENTER:
                text_x = x + (width - text_width) / 2
            else:
                text_x = x + padding_x
            draw.text(
                (text_x, y + padding_y + line_index * line_height),
                line,
                fill="#172033",
                font=font,
            )
        x += width


def _text_width(font: ImageFont.ImageFont, value: str) -> int:
    left, _, right, _ = font.getbbox(value or " ")
    return right - left


def _load_font(size: int) -> ImageFont.ImageFont:
    candidates = (
        Path("/System/Library/Fonts/PingFang.ttc"),
        Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/simsun.ttc"),
        Path("C:/Windows/Fonts/arial.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    )
    for candidate in candidates:
        if candidate.is_file():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def _is_scalar_table(table: ParsedTable) -> bool:
    return all(
        _NON_SCALAR_MARKDOWN.search(cell) is None
        for row in (table.headers, *table.rows)
        for cell in row
    )


def _to_csv(table: ParsedTable) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(table.headers)
    writer.writerows(table.rows)
    return buffer.getvalue().encode("utf-8-sig")
