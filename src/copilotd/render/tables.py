from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from copilotd.render.markdown import MarkdownSpan, split_table_row

_DELIMITER = re.compile(r"^(:)?-{3,}:?$")
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
    source_hash: str
    source_span: MarkdownSpan
    header_raw: tuple[str, ...]
    row_raw: tuple[tuple[str, ...], ...]
    header_span: MarkdownSpan
    row_spans: tuple[MarkdownSpan, ...]


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
    source_contract: dict[str, Any] | None = None


async def render_table(markdown: str, *, max_upload_bytes: int | None = None) -> TableRenderPlan:
    return await asyncio.to_thread(render_table_sync, markdown, max_upload_bytes=max_upload_bytes)


def render_table_sync(markdown: str, *, max_upload_bytes: int | None = None) -> TableRenderPlan:
    return _render_table_cached(markdown, max_upload_bytes)


@lru_cache(maxsize=256)
def _render_table_cached(markdown: str, max_upload_bytes: int | None) -> TableRenderPlan:
    table = parse_table(markdown)
    source_hash = table.source_hash
    width = _formatted_width(table)
    if len(table.headers) <= 4 and len(table.rows) <= 12 and width <= 88:
        code = _format_code_table(table)
        preview_text = f"```\n{code}\n```"
        if max_upload_bytes is None or len(preview_text.encode("utf-8")) <= max_upload_bytes:
            return TableRenderPlan(
                carrier="code",
                preview_text=preview_text,
                assets=(),
                row_count=len(table.rows),
                column_count=len(table.headers),
                source_hash=source_hash,
            )

    markdown_asset = TableAsset(
        filename=f"table-{source_hash[:12]}.md",
        media_type="text/markdown",
        content=table.markdown.encode("utf-8"),
    )
    csv_asset = (
        _csv_asset(table)
        if _is_scalar_table(table) and (len(table.headers) > 8 or len(table.rows) > 50)
        else None
    )

    large = len(table.headers) > 8 or len(table.rows) > 50
    preview_rows = table.rows if not large else table.rows[:20]
    preview_raw_rows = table.row_raw if not large else table.row_raw[:20]
    preview_spans = table.row_spans if not large else table.row_spans[:20]
    preview = ParsedTable(
        headers=table.headers,
        alignments=table.alignments,
        rows=preview_rows,
        markdown=table.markdown,
        source_hash=source_hash,
        source_span=table.source_span,
        header_raw=table.header_raw,
        row_raw=preview_raw_rows,
        header_span=table.header_span,
        row_spans=preview_spans,
    )

    try:
        png_pages, pagination_truncated = _render_png_pages(preview, source_hash)
    except (OSError, UnicodeError):
        code = _format_code_table(preview)
        preview_text = f"```\n{code}\n```" if len(code.encode("utf-8")) <= 1800 else None
        assets = [markdown_asset]
        if csv_asset is not None:
            assets.append(csv_asset)
        if max_upload_bytes is not None and any(
            len(asset.content) > max_upload_bytes for asset in assets
        ):
            assets = [markdown_asset]
        return TableRenderPlan(
            carrier="code-fallback" if preview_text else "attachment-fallback",
            preview_text=preview_text,
            assets=tuple(assets),
            row_count=len(table.rows),
            column_count=len(table.headers),
            source_hash=source_hash,
            preview_truncated=large or preview_text is None,
            source_contract={
                "source_hash": source_hash,
                "page_count": 1,
                "preview_only": True,
                "full_source_filename": markdown_asset.filename,
            },
        )

    if max_upload_bytes is not None and any(
        len(asset.content) > max_upload_bytes for asset in png_pages
    ):
        assets = [markdown_asset]
        if csv_asset is not None:
            assets.append(csv_asset)
        return TableRenderPlan(
            carrier="attachment-fallback",
            preview_text=None,
            assets=tuple(assets),
            row_count=len(table.rows),
            column_count=len(table.headers),
            source_hash=source_hash,
            preview_truncated=True,
            source_contract={
                "source_hash": source_hash,
                "page_count": len(png_pages),
                "preview_only": False,
                "reason": "upload-limit",
                "full_source_filename": markdown_asset.filename,
            },
        )

    assets: list[TableAsset] = list(png_pages)
    assets.append(markdown_asset)
    if csv_asset is not None:
        assets.append(csv_asset)
    page_count = len(png_pages)
    preview_truncated = large or pagination_truncated
    if page_count > 10:
        assets = [png_pages[0], markdown_asset]
        if csv_asset is not None:
            assets.append(csv_asset)
        source_contract = {
            "source_hash": source_hash,
            "page_count": page_count,
            "preview_pages": 1,
            "preview_only": True,
            "full_source_filename": markdown_asset.filename,
        }
        preview_truncated = True
    else:
        source_contract = {
            "source_hash": source_hash,
            "page_count": page_count,
            "preview_only": False,
            "full_source_filename": markdown_asset.filename,
        }
    return TableRenderPlan(
        carrier="image",
        preview_text=None,
        assets=tuple(assets),
        row_count=len(table.rows),
        column_count=len(table.headers),
        source_hash=source_hash,
        preview_truncated=preview_truncated,
        source_contract=source_contract,
    )


@lru_cache(maxsize=256)
def _parse_table_cached(markdown: str, start_line: int) -> ParsedTable:
    lines = [line for line in markdown.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        raise ValueError("table requires a header and delimiter row")
    headers_raw = tuple(split_table_row(lines[0]))
    delimiter_cells = tuple(split_table_row(lines[1]))
    if len(headers_raw) < 2 or len(headers_raw) != len(delimiter_cells):
        raise ValueError("table header and delimiter widths differ")
    alignments = tuple(_parse_alignment(cell) for cell in delimiter_cells)
    rows_raw: list[tuple[str, ...]] = []
    rows_display: list[tuple[str, ...]] = []
    row_spans: list[MarkdownSpan] = []
    for offset, line in enumerate(lines[2:], start=2):
        cells = split_table_row(line)
        if len(cells) > len(headers_raw):
            raise ValueError("table row has more cells than its header")
        padded = cells + ["" for _ in range(len(headers_raw) - len(cells))]
        rows_raw.append(tuple(cells))
        rows_display.append(tuple(_display_cell(cell) for cell in padded))
        row_spans.append(MarkdownSpan(start_line + offset, start_line + offset))
    source = "\n".join(lines) + "\n"
    return ParsedTable(
        headers=tuple(_display_cell(cell) for cell in headers_raw),
        alignments=alignments,
        rows=tuple(rows_display),
        markdown=source,
        source_hash=hashlib.sha256(source.encode("utf-8")).hexdigest(),
        source_span=MarkdownSpan(start_line, start_line + len(lines) - 1),
        header_raw=headers_raw,
        row_raw=tuple(rows_raw),
        header_span=MarkdownSpan(start_line, start_line),
        row_spans=tuple(row_spans),
    )


def parse_table(markdown: str, *, start_line: int = 1) -> ParsedTable:
    return _parse_table_cached(markdown, start_line)


def _parse_alignment(cell: str) -> TableAlignment:
    value = cell.strip()
    match = _DELIMITER.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid table delimiter cell: {cell}")
    if value.startswith(":") and value.endswith(":"):
        return TableAlignment.CENTER
    if value.endswith(":"):
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
        max(_display_width(row[index]) for row in all_rows) for index in range(len(table.headers))
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
        2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1 for character in value
    )


def _render_png_pages(table: ParsedTable, source_hash: str) -> tuple[tuple[TableAsset, ...], bool]:
    scale = 2
    resolver = _FontResolver(scale=scale)
    line_height = resolver.line_height
    padding_x = 12 * scale
    padding_y = 8 * scale
    max_width = 4096
    max_height = 4096

    natural_widths: list[int] = []
    for index, header in enumerate(table.headers):
        values = [header, *(row[index] for row in table.rows)]
        natural = (
            max(_measure_inline_text(value, resolver, code=False) for value in values)
            + padding_x * 2
        )
        natural_widths.append(min(max(natural, 120 * scale), 520 * scale))
    total_width = sum(natural_widths) + 1
    if total_width > max_width:
        ratio = max_width / total_width
        natural_widths = [max(int(width * ratio), 80 * scale) for width in natural_widths]
        total_width = sum(natural_widths) + 1
    if total_width > max_width:
        raise OSError("table is too wide for the PNG renderer")

    wrapped_header = _wrap_row(table.headers, natural_widths, resolver, padding_x)
    wrapped_rows = [_wrap_row(row, natural_widths, resolver, padding_x) for row in table.rows]
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
            resolver,
            line_height,
            padding_x,
            padding_y,
            fill="#e8eef8",
        )
        y += header_height
        for row_index, (wrapped, row_height) in enumerate(
            zip(page_rows, page_heights, strict=True), start=start
        ):
            _draw_row(
                draw,
                wrapped,
                table.alignments,
                natural_widths,
                y,
                row_height,
                resolver,
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
    resolver: _FontResolver,
    padding_x: int,
) -> tuple[tuple[tuple[bool, str], ...], ...]:
    return tuple(
        tuple(_wrap_cell_text(cell, max(width - padding_x * 2, 1), resolver))
        for cell, width in zip(row, widths, strict=True)
    )


def _wrap_cell_text(value: str, max_width: int, resolver: _FontResolver) -> list[tuple[bool, str]]:
    raw_pieces: list[tuple[bool, str]] = []
    current_width = 0
    for is_code, segment in _tokenize_inline(value):
        tokens = [segment] if is_code else re.findall(r"\s+|\S+\s*", segment)
        for token in tokens:
            if not token:
                continue
            token_width = _measure_inline_text(token, resolver, code=is_code)
            if current_width and not token.isspace() and current_width + token_width > max_width:
                raw_pieces.append((False, "\n"))
                current_width = 0
            if token_width <= max_width or token.isspace():
                raw_pieces.append((is_code, token))
                current_width += token_width
                continue
            for char in token:
                char_width = _measure_inline_text(char, resolver, code=is_code)
                if current_width and current_width + char_width > max_width:
                    raw_pieces.append((False, "\n"))
                    current_width = 0
                raw_pieces.append((is_code, char))
                current_width += char_width
    lines: list[list[tuple[bool, str]]] = [[]]
    for is_code, token in raw_pieces:
        if token == "\n":
            if lines[-1]:
                lines.append([])
            continue
        lines[-1].append((is_code, token))
    if lines and not lines[-1]:
        lines.pop()
    return [line for line in lines if line]


def _tokenize_inline(value: str) -> list[tuple[bool, str]]:
    segments: list[tuple[bool, str]] = []
    cursor = 0
    while cursor < len(value):
        if value[cursor] != "`":
            next_tick = value.find("`", cursor)
            if next_tick == -1:
                segments.append((False, value[cursor:]))
                break
            segments.append((False, value[cursor:next_tick]))
            cursor = next_tick
            continue
        end = value.find("`", cursor + 1)
        if end == -1:
            segments.append((False, value[cursor:]))
            break
        segments.append((True, value[cursor + 1 : end]))
        cursor = end + 1
    return [(kind, text) for kind, text in segments if text]


def _draw_row(
    draw: ImageDraw.ImageDraw,
    wrapped: tuple[tuple[tuple[bool, str], ...], ...],
    alignments: tuple[TableAlignment, ...],
    widths: list[int],
    y: int,
    row_height: int,
    resolver: _FontResolver,
    line_height: int,
    padding_x: int,
    padding_y: int,
    *,
    fill: str,
) -> None:
    x = 0
    for cell_lines, alignment, width in zip(wrapped, alignments, widths, strict=True):
        draw.rectangle((x, y, x + width, y + row_height), fill=fill, outline="#c7d0df")
        for line_index, line in enumerate(cell_lines):
            line_width = sum(
                _measure_inline_text(text, resolver, code=is_code) for is_code, text in line
            )
            if alignment == TableAlignment.RIGHT:
                text_x = x + width - padding_x - line_width
            elif alignment == TableAlignment.CENTER:
                text_x = x + (width - line_width) / 2
            else:
                text_x = x + padding_x
            cursor_x = text_x
            for is_code, text in line:
                if is_code:
                    piece_width = _measure_inline_text(text, resolver, code=True)
                    draw.rectangle(
                        (
                            cursor_x - 2,
                            y + padding_y + line_index * line_height - 1,
                            cursor_x + piece_width + 2,
                            y + padding_y + line_index * line_height + line_height - 2,
                        ),
                        fill="#eef2f8",
                        outline="#d7dfeb",
                    )
                _draw_inline_text(
                    draw,
                    cursor_x,
                    y + padding_y + line_index * line_height,
                    text,
                    resolver,
                    code=is_code,
                )
                cursor_x += _measure_inline_text(text, resolver, code=is_code)
        x += width


def _measure_inline_text(value: str, resolver: _FontResolver, *, code: bool) -> int:
    return sum(resolver.glyph_width(character, code=code) for character in value)


def _draw_inline_text(
    draw: ImageDraw.ImageDraw,
    x: float,
    y: float,
    value: str,
    resolver: _FontResolver,
    *,
    code: bool,
) -> None:
    cursor = x
    for character in value:
        font = resolver.font_for(character, code=code)
        try:
            draw.text((cursor, y), character, fill="#172033", font=font)
        except Exception:
            draw.text((cursor, y), "?", fill="#172033", font=resolver._base_font)
        cursor += resolver.glyph_width(character, code=code)


class _FontResolver:
    def __init__(self, *, scale: int) -> None:
        self._base_font = self._load_font(
            (
                Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
                Path("/System/Library/Fonts/PingFang.ttc"),
                Path("C:/Windows/Fonts/arial.ttf"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ),
            14 * scale,
        )
        self._code_font = self._load_font(
            (
                Path("/System/Library/Fonts/Supplemental/Menlo.ttc"),
                Path("C:/Windows/Fonts/consola.ttf"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
            ),
            13 * scale,
        )
        self._cjk_font = self._load_font(
            (
                Path("/System/Library/Fonts/PingFang.ttc"),
                Path("C:/Windows/Fonts/msyh.ttc"),
                Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ),
            14 * scale,
        )
        self._emoji_font = self._load_font(
            (
                Path("/System/Library/Fonts/Apple Color Emoji.ttc"),
                Path("C:/Windows/Fonts/seguiemj.ttf"),
                Path("/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ),
            14 * scale,
        )
        self.line_height = max(
            self._line_height(font)
            for font in (self._base_font, self._code_font, self._cjk_font, self._emoji_font)
        )

    def font_for(self, character: str, *, code: bool) -> ImageFont.ImageFont:
        if code:
            return (
                self._code_font if not _needs_fallback(character) else self._fallback_for(character)
            )
        if _is_emoji(character):
            return self._emoji_font
        if _is_cjk(character):
            return self._cjk_font
        return self._base_font if not _needs_fallback(character) else self._fallback_for(character)

    def glyph_width(self, character: str, *, code: bool) -> int:
        for font in (self.font_for(character, code=code), self._base_font, self._code_font):
            try:
                left, _, right, _ = font.getbbox(character or " ")
            except Exception:
                continue
            width = right - left
            if width > 0:
                return width
        return 1

    def _fallback_for(self, character: str) -> ImageFont.ImageFont:
        for font in (self._emoji_font, self._cjk_font, self._code_font, self._base_font):
            if _font_supports(font, character):
                return font
        return self._base_font

    @staticmethod
    def _load_font(candidates: tuple[Path, ...], size: int) -> ImageFont.ImageFont:
        for candidate in candidates:
            if not candidate.is_file():
                continue
            try:
                return ImageFont.truetype(str(candidate), size=size)
            except Exception:
                continue
        return ImageFont.load_default()

    @staticmethod
    def _line_height(font: ImageFont.ImageFont) -> int:
        _, top, _, bottom = font.getbbox("Ag")
        return (bottom - top) + 4


def _font_supports(font: ImageFont.ImageFont, character: str) -> bool:
    try:
        left, top, right, bottom = font.getbbox(character or " ")
    except Exception:
        return False
    return (right - left) > 0 and (bottom - top) > 0


def _needs_fallback(character: str) -> bool:
    return _is_cjk(character) or _is_emoji(character)


def _is_cjk(character: str) -> bool:
    return unicodedata.east_asian_width(character) in {"W", "F"}


def _is_emoji(character: str) -> bool:
    codepoint = ord(character)
    return 0x1F000 <= codepoint <= 0x1FAFF


def _is_scalar_table(table: ParsedTable) -> bool:
    return all(
        _NON_SCALAR_MARKDOWN.search(cell) is None
        for row in (table.headers, *table.rows)
        for cell in row
    )


def _csv_asset(table: ParsedTable) -> TableAsset:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(table.headers)
    writer.writerows(table.rows)
    return TableAsset(
        filename=f"table-{table.source_hash[:12]}.csv",
        media_type="text/csv",
        content=buffer.getvalue().encode("utf-8-sig"),
    )
