from __future__ import annotations

import re
from dataclasses import dataclass

_DELIMITER_CELL = re.compile(r"^:?-{3,}:?$")


@dataclass(frozen=True, slots=True)
class TextBlock:
    content: str


@dataclass(frozen=True, slots=True)
class TableBlock:
    markdown: str


MarkdownBlock = TextBlock | TableBlock


class MarkdownAssembler:
    """Streams text while holding a possible GFM table until it is complete."""

    def __init__(self) -> None:
        self._line_buffer = ""
        self._pending_line: str | None = None
        self._text_lines: list[str] = []
        self._table_lines: list[str] | None = None
        self._source = ""
        self._finalized = False

    def append(self, delta: str) -> list[MarkdownBlock]:
        if self._finalized:
            raise RuntimeError("markdown assembler is already finalized")
        self._source += delta
        self._line_buffer += delta
        blocks: list[MarkdownBlock] = []
        while "\n" in self._line_buffer:
            line, self._line_buffer = self._line_buffer.split("\n", 1)
            blocks.extend(self._consume_line(line))
        blocks.extend(self._flush_text())
        return blocks

    def finalize(self, canonical_content: str | None = None) -> list[MarkdownBlock]:
        if self._finalized:
            return []
        blocks: list[MarkdownBlock] = []
        if canonical_content is not None:
            if canonical_content.startswith(self._source):
                suffix = canonical_content[len(self._source) :]
                if suffix:
                    blocks.extend(self.append(suffix))
            elif canonical_content != self._source:
                raise ValueError("canonical content diverged from already streamed markdown")

        if self._line_buffer:
            blocks.extend(self._consume_line(self._line_buffer))
            self._line_buffer = ""
        if self._table_lines is not None:
            blocks.append(self._close_table())
        elif self._pending_line is not None:
            self._text_lines.append(self._pending_line)
            self._pending_line = None
        blocks.extend(self._flush_text())
        self._finalized = True
        return blocks

    def _consume_line(self, line: str) -> list[MarkdownBlock]:
        blocks: list[MarkdownBlock] = []
        if self._table_lines is not None:
            if _looks_like_table_row(line):
                self._table_lines.append(line)
                return blocks
            blocks.append(self._close_table())

        if self._pending_line is None:
            self._pending_line = line
            return blocks
        if _looks_like_table_header(self._pending_line, line):
            blocks.extend(self._flush_text())
            self._table_lines = [self._pending_line, line]
            self._pending_line = None
            return blocks

        self._text_lines.append(self._pending_line)
        self._pending_line = line
        return blocks

    def _flush_text(self) -> list[MarkdownBlock]:
        if not self._text_lines:
            return []
        content = "\n".join(self._text_lines) + "\n"
        self._text_lines.clear()
        return [TextBlock(content)]

    def _close_table(self) -> TableBlock:
        if self._table_lines is None:
            raise RuntimeError("no table is being assembled")
        block = TableBlock("\n".join(self._table_lines))
        self._table_lines = None
        return block


def _looks_like_table_header(header: str, delimiter: str) -> bool:
    header_cells = split_table_row(header)
    delimiter_cells = split_table_row(delimiter)
    return (
        len(header_cells) >= 2
        and len(header_cells) == len(delimiter_cells)
        and all(_DELIMITER_CELL.fullmatch(cell.strip()) for cell in delimiter_cells)
    )


def _looks_like_table_row(line: str) -> bool:
    return bool(line.strip()) and len(split_table_row(line)) >= 2


def split_table_row(line: str) -> list[str]:
    value = line.strip()
    if value.startswith("|"):
        value = value[1:]
    if value.endswith("|") and not value.endswith("\\|"):
        value = value[:-1]

    cells: list[str] = []
    current: list[str] = []
    escaped = False
    code_ticks = 0
    index = 0
    while index < len(value):
        character = value[index]
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
            current.append(character)
        elif character == "`":
            run = 1
            while index + run < len(value) and value[index + run] == "`":
                run += 1
            if code_ticks == 0:
                code_ticks = run
            elif code_ticks == run:
                code_ticks = 0
            current.extend("`" * run)
            index += run - 1
        elif character == "|" and code_ticks == 0:
            cells.append("".join(current).strip())
            current.clear()
        else:
            current.append(character)
        index += 1
    cells.append("".join(current).strip())
    return cells
