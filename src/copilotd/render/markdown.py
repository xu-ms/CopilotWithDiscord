from __future__ import annotations

import hashlib
import re
import shlex
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, UnidentifiedImageError

_DELIMITER_CELL = re.compile(r"^:?-{3,}:?$")
_LIST_ITEM_RE = re.compile(r"^(?P<indent>[ \t]*)(?P<marker>(?:[*+-]|\d+[.)]))[ \t]+(?P<body>.*)$")
_FENCE_RE = re.compile(r"^(?P<indent>[ \t]*)(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
_BLOCKQUOTE_RE = re.compile(r"^[ \t]*>[ \t]?(?P<body>.*)$")
_THEMATIC_BREAK_RE = re.compile(r"^[ \t]*(?:\*{3,}|-{3,}|_{3,})[ \t]*$")
_IMAGE_RE = re.compile(r"!\[(?P<alt>[^\]]*)\]\((?P<target>[^)]+)\)")
_CONTINUATION_MARKER = "… continued …\n"
_SUPPORTED_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"}


@dataclass(frozen=True, slots=True)
class MarkdownSpan:
    start_line: int
    end_line: int


@dataclass(frozen=True, slots=True)
class TextBlock:
    content: str
    span: MarkdownSpan | None = None


@dataclass(frozen=True, slots=True)
class TableBlock:
    markdown: str
    span: MarkdownSpan | None = None
    source_hash: str | None = None
    raw_lines: tuple[str, ...] = ()


MarkdownBlock = TextBlock | TableBlock


@dataclass(frozen=True, slots=True)
class MarkdownParagraph:
    text: str
    raw: str
    span: MarkdownSpan


@dataclass(frozen=True, slots=True)
class MarkdownListItem:
    marker: str
    body: str
    raw: str
    span: MarkdownSpan
    continuation_lines: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MarkdownListBlock:
    ordered: bool
    items: tuple[MarkdownListItem, ...]
    raw: str
    span: MarkdownSpan


@dataclass(frozen=True, slots=True)
class MarkdownBlockquote:
    lines: tuple[str, ...]
    raw: str
    span: MarkdownSpan


@dataclass(frozen=True, slots=True)
class MarkdownFence:
    fence: str
    info: str
    code: str
    raw: str
    span: MarkdownSpan


@dataclass(frozen=True, slots=True)
class MarkdownThematicBreak:
    raw: str
    span: MarkdownSpan


@dataclass(frozen=True, slots=True)
class MarkdownTableCandidate:
    markdown: str
    span: MarkdownSpan
    headers: tuple[str, ...]
    alignments: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    raw_headers: tuple[str, ...]
    raw_rows: tuple[tuple[str, ...], ...]
    source_hash: str


MarkdownAstNode = (
    MarkdownParagraph
    | MarkdownListBlock
    | MarkdownBlockquote
    | MarkdownFence
    | MarkdownTableCandidate
    | MarkdownThematicBreak
)


@dataclass(frozen=True, slots=True)
class MarkdownAttachmentPlan:
    filename: str
    media_type: str
    content: str
    block_kind: str
    span: MarkdownSpan | None = None


@dataclass(frozen=True, slots=True)
class MarkdownMessageSegment:
    content: str
    spans: tuple[MarkdownSpan, ...]
    attachments: tuple[MarkdownAttachmentPlan, ...] = ()


@dataclass(frozen=True, slots=True)
class MarkdownMessagePlan:
    segments: tuple[MarkdownMessageSegment, ...]
    source_hash: str
    block_count: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class MarkdownImageWarning:
    kind: str
    message: str
    source: str
    span: MarkdownSpan | None = None


@dataclass(frozen=True, slots=True)
class MarkdownImageAttachmentPlan:
    source: str
    path: str
    resolved_path: str
    alt_text: str
    title: str | None
    filename: str
    batch_index: int = 1
    batch_size: int = 1


@dataclass(frozen=True, slots=True)
class MarkdownImageExtractionPlan:
    content: str
    attachments: tuple[MarkdownImageAttachmentPlan, ...]
    batches: tuple[tuple[MarkdownImageAttachmentPlan, ...], ...]
    warnings: tuple[MarkdownImageWarning, ...]


class MarkdownAssembler:
    """Accumulates markdown and emits block-preserving text/table payloads."""

    def __init__(self) -> None:
        self._source = ""
        self._finalized = False

    def append(self, delta: str) -> list[MarkdownBlock]:
        if self._finalized:
            raise RuntimeError("markdown assembler is already finalized")
        self._source += delta
        return []

    def finalize(self, canonical_content: str | None = None) -> list[MarkdownBlock]:
        if self._finalized:
            return []
        if canonical_content is not None:
            if canonical_content.startswith(self._source):
                suffix = canonical_content[len(self._source) :]
                if suffix:
                    self._source += suffix
            elif canonical_content != self._source:
                raise ValueError("canonical content diverged from already streamed markdown")
        blocks: list[MarkdownBlock] = []
        for node in parse_markdown_blocks(self._source):
            if isinstance(node, MarkdownTableCandidate):
                blocks.append(
                    TableBlock(
                        node.markdown,
                        span=node.span,
                        source_hash=node.source_hash,
                        raw_lines=tuple(node.markdown.splitlines()),
                    )
                )
            else:
                raw = _node_raw(node)
                if raw:
                    blocks.append(TextBlock(raw, span=_node_span(node)))
        self._finalized = True
        return blocks


def parse_markdown_blocks(source: str) -> list[MarkdownAstNode]:
    lines = source.splitlines(keepends=True)
    blocks: list[MarkdownAstNode] = []
    index = 0
    while index < len(lines):
        if not _strip_newline(lines[index]).strip():
            index += 1
            continue
        node, next_index = _consume_markdown_block(lines, index)
        blocks.append(node)
        index = next_index
    return blocks


def plan_markdown_messages(source: str, *, max_chars: int = 1850) -> MarkdownMessagePlan:
    if max_chars < 1:
        raise ValueError("max_chars must be positive")
    blocks = parse_markdown_blocks(source)
    source_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()
    segments: list[MarkdownMessageSegment] = []
    current_content = ""
    current_spans: list[MarkdownSpan] = []
    current_attachments: list[MarkdownAttachmentPlan] = []

    def flush() -> None:
        nonlocal current_content, current_spans, current_attachments
        if current_content or current_attachments:
            segments.append(
                MarkdownMessageSegment(
                    content=current_content.rstrip(),
                    spans=tuple(current_spans),
                    attachments=tuple(current_attachments),
                )
            )
        current_content = ""
        current_spans = []
        current_attachments = []

    for block_index, block in enumerate(blocks, start=1):
        raw = _node_raw(block).rstrip("\n")
        span = _node_span(block)
        if len(raw) > max_chars:
            flush()
            filename = f"markdown-block-{block_index:03d}.md"
            segments.append(
                MarkdownMessageSegment(
                    content=f"{_summarize_block(block)} [{filename}]",
                    spans=(span,),
                    attachments=(
                        MarkdownAttachmentPlan(
                            filename=filename,
                            media_type="text/markdown",
                            content=raw,
                            block_kind=block.__class__.__name__,
                            span=span,
                        ),
                    ),
                )
            )
            continue
        prefix = _CONTINUATION_MARKER if segments or current_content else ""
        candidate = prefix + raw if not current_content else f"{current_content}\n{raw}"
        if len(candidate) > max_chars:
            flush()
            current_content = prefix + raw
            current_spans = [span]
            continue
        if not current_content and prefix:
            current_content = prefix + raw
        else:
            current_content = candidate
        current_spans.append(span)
    flush()
    return MarkdownMessagePlan(
        segments=tuple(segments),
        source_hash=source_hash,
        block_count=len(blocks),
        truncated=any(segment.attachments for segment in segments),
    )


@dataclass(frozen=True, slots=True)
class _ContainerToken:
    kind: str
    indent: int = 0
    marker_id: int = 0


@dataclass(frozen=True, slots=True)
class _LiteralContext:
    container_path: tuple[_ContainerToken, ...]
    fence_char: str = ""
    fence_len: int = 0


def extract_local_markdown_images(
    source: str,
    *,
    allowed_roots: Sequence[Path | str],
    trusted_paths: Sequence[Path | str] | None = None,
    trusted_artifacts: Mapping[Path | str, Path | str] | None = None,
) -> MarkdownImageExtractionPlan:
    roots = tuple(Path(root).resolve() for root in allowed_roots)
    artifact_paths = (
        None
        if trusted_artifacts is None
        else {
            source_candidate: Path(snapshot_path).resolve(strict=False)
            for source_path, snapshot_path in trusted_artifacts.items()
            for source_candidate in _trusted_artifact_candidates(Path(source_path), roots)
        }
    )
    external_trusted_paths = (
        set()
        if trusted_artifacts is None
        else {
            path.resolve(strict=False)
            for value in trusted_paths or ()
            if (path := Path(value)).is_absolute()
        }
    )
    trusted = (
        None
        if trusted_paths is None and artifact_paths is None
        else {
            resolved
            for value in trusted_paths or ()
            for resolved in _trusted_path_candidates(Path(value), roots)
        }
        | set(artifact_paths or ())
        | external_trusted_paths
    )
    redact_local_references = trusted is not None
    warnings: list[MarkdownImageWarning] = []
    attachments: list[MarkdownImageAttachmentPlan] = []
    pieces: list[str] = []
    active_fence: _LiteralContext | None = None
    active_indented_code: _LiteralContext | None = None
    active_list_path: tuple[_ContainerToken, ...] = ()
    inline_code_delim: int | None = None
    inline_container_path: tuple[_ContainerToken, ...] | None = None

    for line_number, line in enumerate(source.splitlines(keepends=True), start=1):
        text, line_ending = _split_line_ending(line)

        if active_fence is not None:
            literal_content = _literal_container_content(text, active_fence)
            if literal_content is None:
                active_fence = None
            elif _is_fence_closer(
                literal_content,
                active_fence.fence_char,
                active_fence.fence_len,
            ):
                pieces.append(line)
                active_fence = None
                continue
            else:
                pieces.append(line)
                continue

        if active_indented_code is not None:
            literal_content = _literal_container_content(text, active_indented_code)
            if literal_content is not None and (
                not literal_content.strip()
                or _indent_width(_leading_whitespace(literal_content)) >= 4
            ):
                pieces.append(line)
                continue
            active_indented_code = None

        container_path, content, active_list_path = _document_container_content(
            text,
            active_list_path,
            marker_id=line_number,
        )
        if inline_code_delim is not None and (
            not text.strip() or container_path != inline_container_path
        ):
            inline_code_delim = None
            inline_container_path = None
        indent = _indent_width(_leading_whitespace(content))
        if inline_code_delim is None:
            if indent >= 4:
                pieces.append(line)
                active_indented_code = _LiteralContext(
                    container_path=container_path,
                )
                continue
            fence = _parse_fence_marker(content)
            if fence is not None:
                pieces.append(line)
                active_fence = _LiteralContext(
                    container_path=container_path,
                    fence_char=fence[0],
                    fence_len=fence[1],
                )
                continue

        previous_inline_delim = inline_code_delim
        rendered, inline_code_delim = _extract_images_from_line(
            text,
            roots,
            warnings,
            attachments,
            inline_code_delim,
            trusted,
            artifact_paths,
            redact_local_references,
            external_trusted_paths,
        )
        if previous_inline_delim is None and inline_code_delim is not None:
            inline_container_path = container_path
        elif inline_code_delim is None:
            inline_container_path = None
        pieces.append(rendered)
        pieces.append(line_ending)

    content = "".join(pieces)
    batches = tuple(
        tuple(attachments[index : index + 10]) for index in range(0, len(attachments), 10)
    )
    rebatches = tuple(
        tuple(
            MarkdownImageAttachmentPlan(
                source=item.source,
                path=item.path,
                resolved_path=item.resolved_path,
                alt_text=item.alt_text,
                title=item.title,
                filename=item.filename,
                batch_index=batch_index,
                batch_size=len(batch),
            )
            for item in batch
        )
        for batch_index, batch in enumerate(batches, start=1)
    )
    flattened = tuple(item for batch in rebatches for item in batch)
    return MarkdownImageExtractionPlan(
        content=content,
        attachments=flattened,
        batches=rebatches,
        warnings=tuple(warnings),
    )


def _extract_images_from_line(
    text: str,
    roots: tuple[Path, ...],
    warnings: list[MarkdownImageWarning],
    attachments: list[MarkdownImageAttachmentPlan],
    inline_code_delim: int | None,
    trusted_paths: set[Path] | None,
    trusted_artifacts: dict[Path, Path] | None,
    redact_local_references: bool,
    external_trusted_paths: set[Path],
) -> tuple[str, int | None]:
    pieces: list[str] = []
    index = 0
    while index < len(text):
        character = text[index]
        if character == "\\":
            if index + 1 < len(text):
                pieces.append(text[index : index + 2])
                index += 2
            else:
                pieces.append(character)
                index += 1
            continue
        if character == "`":
            run = _backtick_run_length(text, index)
            pieces.append("`" * run)
            if inline_code_delim is None:
                inline_code_delim = run
            elif run == inline_code_delim:
                inline_code_delim = None
            index += run
            continue
        if inline_code_delim is not None:
            pieces.append(character)
            index += 1
            continue
        if text.startswith("![", index):
            candidate = _scan_markdown_image_candidate(text, index)
            if candidate is None:
                pieces.append(character)
                index += 1
                continue
            end, source_text, alt_text, target = candidate
            path_text, title = _parse_markdown_image_target(target)
            if path_text is None:
                warnings.append(
                    MarkdownImageWarning(
                        kind="invalid-target",
                        message="image target could not be parsed",
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            resolved, warning = _resolve_markdown_image_path(
                path_text,
                roots,
                exact_paths=external_trusted_paths,
            )
            if warning is not None:
                warnings.append(
                    MarkdownImageWarning(
                        kind=warning,
                        message=(
                            "image path is outside allowed roots"
                            if warning == "invalid-root"
                            else "image path is missing"
                        ),
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            if trusted_paths is not None and resolved not in trusted_paths:
                warnings.append(
                    MarkdownImageWarning(
                        kind="untrusted-image",
                        message="local image path lacks verified host provenance",
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            if Path(path_text).suffix.lower() not in _SUPPORTED_IMAGE_SUFFIXES:
                warnings.append(
                    MarkdownImageWarning(
                        kind="unsupported-image",
                        message="local Markdown image uses an unsupported file type",
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            materialized = (
                resolved if trusted_artifacts is None else trusted_artifacts.get(resolved)
            )
            if materialized is None or not materialized.is_file():
                warnings.append(
                    MarkdownImageWarning(
                        kind="missing-image",
                        message="image path does not point to a file",
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            if not _is_supported_image(materialized):
                warnings.append(
                    MarkdownImageWarning(
                        kind="unsupported-image",
                        message="local Markdown image is not a verified image file",
                        source=source_text,
                    )
                )
                pieces.append(
                    _local_image_caption(alt_text) if redact_local_references else source_text
                )
                index = end
                continue
            attachments.append(
                MarkdownImageAttachmentPlan(
                    source=source_text,
                    path=path_text,
                    resolved_path=str(materialized),
                    alt_text=alt_text,
                    title=title,
                    filename=resolved.name,
                )
            )
            if redact_local_references:
                pieces.append(_local_image_caption(alt_text))
            index = end
            continue
        pieces.append(character)
        index += 1
    return "".join(pieces), inline_code_delim


def _local_image_caption(alt_text: str) -> str:
    caption = alt_text.strip()
    return f"**Image:** {caption}" if caption else "**Image attachment**"


def _backtick_run_length(text: str, index: int) -> int:
    run = 1
    while index + run < len(text) and text[index + run] == "`":
        run += 1
    return run


def _scan_markdown_image_candidate(text: str, index: int) -> tuple[int, str, str, str] | None:
    if not text.startswith("![", index):
        return None
    alt_start = index + 2
    alt_end = _find_matching_markdown_bracket(text, alt_start)
    if alt_end is None:
        return None
    target_start = alt_end + 1
    if target_start >= len(text) or text[target_start] != "(":
        return None
    target_end = _find_matching_markdown_paren(text, target_start + 1)
    if target_end is None:
        return None
    source = text[index : target_end + 1]
    alt_text = text[alt_start:alt_end]
    target = text[target_start + 1 : target_end]
    return target_end + 1, source, alt_text, target


def _find_matching_markdown_bracket(text: str, start: int) -> int | None:
    depth = 0
    escaped = False
    index = start
    while index < len(text):
        character = text[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == "[":
            depth += 1
        elif character == "]":
            if depth == 0:
                return index
            depth -= 1
        index += 1
    return None


def _find_matching_markdown_paren(text: str, start: int) -> int | None:
    depth = 1
    escaped = False
    quote: str | None = None
    index = start
    while index < len(text):
        character = text[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif quote is not None:
            if character == quote:
                quote = None
        elif character in {'"', "'"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _document_container_content(
    text: str,
    active_list_path: tuple[_ContainerToken, ...],
    *,
    marker_id: int,
) -> tuple[tuple[_ContainerToken, ...], str, tuple[_ContainerToken, ...]]:
    container_path: tuple[_ContainerToken, ...] = ()
    content = text
    for end in range(len(active_list_path), -1, -1):
        candidate_path = active_list_path[:end]
        candidate_content = _consume_container_path(text, candidate_path)
        if candidate_content is not None:
            container_path = candidate_path
            content = candidate_content
            break

    while True:
        quoted = _strip_one_blockquote(content)
        if quoted is not None:
            container_path = (*container_path, _ContainerToken("quote"))
            content = quoted
            continue
        list_match = _LIST_ITEM_RE.match(content)
        if list_match is None:
            break
        content_indent = _indent_width(content[: list_match.start("body")])
        container_path = (
            *container_path,
            _ContainerToken(
                "list",
                indent=content_indent,
                marker_id=marker_id,
            ),
        )
        content = list_match.group("body")

    return container_path, content, _active_list_prefix(container_path)


def _literal_container_content(text: str, context: _LiteralContext) -> str | None:
    return _consume_container_path(text, context.container_path)


def _consume_container_path(
    text: str,
    path: tuple[_ContainerToken, ...],
) -> str | None:
    content = text
    for token in path:
        if not content.strip():
            return ""
        if token.kind == "quote":
            content = _strip_one_blockquote(content)
        else:
            content = _remove_indent(content, token.indent)
        if content is None:
            return None
    return content


def _strip_one_blockquote(text: str) -> str | None:
    spaces = len(text) - len(text.lstrip(" "))
    if spaces > 3 or spaces >= len(text) or text[spaces] != ">":
        return None
    cursor = spaces + 1
    if cursor < len(text) and text[cursor] in " \t":
        cursor += 1
    return text[cursor:]


def _active_list_prefix(
    path: tuple[_ContainerToken, ...],
) -> tuple[_ContainerToken, ...]:
    for index in range(len(path) - 1, -1, -1):
        if path[index].kind == "list":
            return path[: index + 1]
    return ()


def _remove_indent(text: str, width: int) -> str | None:
    consumed = 0
    index = 0
    while index < len(text) and consumed < width and text[index] in " \t":
        if text[index] == "\t":
            consumed += 4 - (consumed % 4)
        else:
            consumed += 1
        index += 1
    if consumed < width:
        return None
    return text[index:]


def _split_line_ending(line: str) -> tuple[str, str]:
    text = line.rstrip("\r\n")
    return text, line[len(text) :]


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


def _consume_markdown_block(lines: list[str], index: int) -> tuple[MarkdownAstNode, int]:
    text = _strip_newline(lines[index])
    table = _try_consume_table(lines, index)
    if table is not None:
        return table
    fence = _try_consume_fence(lines, index)
    if fence is not None:
        return fence
    quote = _try_consume_blockquote(lines, index)
    if quote is not None:
        return quote
    list_block = _try_consume_list(lines, index)
    if list_block is not None:
        return list_block
    if _is_thematic_break(text):
        return MarkdownThematicBreak(raw=text, span=MarkdownSpan(index + 1, index + 1)), index + 1
    return _consume_paragraph(lines, index)


def _consume_paragraph(lines: list[str], index: int) -> tuple[MarkdownParagraph, int]:
    start = index
    raw_lines = [lines[index]]
    index += 1
    while index < len(lines):
        text = _strip_newline(lines[index])
        if not text.strip() or _is_block_start(lines, index):
            break
        raw_lines.append(lines[index])
        index += 1
    raw = "".join(raw_lines)
    span = MarkdownSpan(start + 1, index)
    return MarkdownParagraph(text=raw.rstrip("\n"), raw=raw, span=span), index


def _try_consume_table(lines: list[str], index: int) -> tuple[MarkdownTableCandidate, int] | None:
    if index + 1 >= len(lines):
        return None
    header = _strip_newline(lines[index])
    delimiter = _strip_newline(lines[index + 1])
    if not _looks_like_table_header(header, delimiter):
        return None
    end = index + 2
    while end < len(lines):
        text = _strip_newline(lines[end])
        if not text.strip() or not _looks_like_table_row(text):
            break
        end += 1
    raw_lines = tuple(_strip_newline(line) for line in lines[index:end])
    markdown = "\n".join(raw_lines)
    headers = tuple(split_table_row(header))
    delimiter_cells = tuple(split_table_row(delimiter))
    rows_raw: list[tuple[str, ...]] = []
    rows_display: list[tuple[str, ...]] = []
    for line in raw_lines[2:]:
        cells = split_table_row(line)
        if len(cells) > len(headers):
            break
        padded = cells + ["" for _ in range(len(headers) - len(cells))]
        rows_raw.append(tuple(cells))
        rows_display.append(tuple(_display_cell(cell) for cell in padded))
    candidate = MarkdownTableCandidate(
        markdown=markdown,
        span=MarkdownSpan(index + 1, end),
        headers=tuple(_display_cell(cell) for cell in headers),
        alignments=tuple(_parse_table_alignment(cell) for cell in delimiter_cells),
        rows=tuple(rows_display),
        raw_headers=headers,
        raw_rows=tuple(rows_raw),
        source_hash=hashlib.sha256(markdown.encode("utf-8")).hexdigest(),
    )
    return candidate, end


def _try_consume_fence(lines: list[str], index: int) -> tuple[MarkdownFence, int] | None:
    match = _FENCE_RE.match(_strip_newline(lines[index]))
    if match is None:
        return None
    fence = match.group("fence")
    info = match.group("info").strip()
    fence_char = fence[0]
    fence_len = len(fence)
    end = index + 1
    code_lines: list[str] = []
    while end < len(lines):
        text = _strip_newline(lines[end])
        if re.fullmatch(rf"[ \t]*{re.escape(fence_char)}{{{fence_len},}}[ \t]*", text):
            end += 1
            break
        code_lines.append(text)
        end += 1
    raw = "\n".join(_strip_newline(line) for line in lines[index:end])
    return (
        MarkdownFence(
            fence=fence,
            info=info,
            code="\n".join(code_lines),
            raw=raw,
            span=MarkdownSpan(index + 1, end),
        ),
        end,
    )


def _try_consume_blockquote(lines: list[str], index: int) -> tuple[MarkdownBlockquote, int] | None:
    if _BLOCKQUOTE_RE.match(_strip_newline(lines[index])) is None:
        return None
    end = index
    quoted: list[str] = []
    while end < len(lines):
        text = _strip_newline(lines[end])
        if not text.strip():
            next_nonblank = end + 1
            while next_nonblank < len(lines) and not _strip_newline(lines[next_nonblank]).strip():
                next_nonblank += 1
            if next_nonblank < len(lines) and _BLOCKQUOTE_RE.match(
                _strip_newline(lines[next_nonblank])
            ):
                quoted.append(text)
                end += 1
                continue
            break
        match = _BLOCKQUOTE_RE.match(text)
        if match is None:
            break
        quoted.append(match.group("body"))
        end += 1
    raw = "\n".join(_strip_newline(line) for line in lines[index:end])
    return (
        MarkdownBlockquote(
            lines=tuple(quoted),
            raw=raw,
            span=MarkdownSpan(index + 1, end),
        ),
        end,
    )


def _try_consume_list(lines: list[str], index: int) -> tuple[MarkdownListBlock, int] | None:
    first = _LIST_ITEM_RE.match(_strip_newline(lines[index]))
    if first is None:
        return None
    start = index
    end = index
    items: list[MarkdownListItem] = []
    first_indent = _indent_width(first.group("indent"))
    item_start = index
    while end < len(lines):
        text = _strip_newline(lines[end])
        if not text.strip():
            end += 1
            continue
        match = _LIST_ITEM_RE.match(text)
        if match is not None:
            indent = _indent_width(match.group("indent"))
            if indent < first_indent and items:
                break
            if end > item_start:
                items.append(_make_list_item(lines[item_start:end], item_start))
            item_start = end
            end += 1
            continue
        indent = _indent_width(_leading_whitespace(text))
        if indent <= first_indent and not text.startswith((" ", "\t")):
            break
        end += 1
    if item_start < end:
        items.append(_make_list_item(lines[item_start:end], item_start))
    raw = "".join(lines[start:end])
    ordered = bool(_LIST_ITEM_RE.match(_strip_newline(lines[start])).group("marker")[0].isdigit())
    return (
        MarkdownListBlock(
            ordered=ordered,
            items=tuple(items),
            raw=raw,
            span=MarkdownSpan(start + 1, end),
        ),
        end,
    )


def _make_list_item(lines: list[str], start_index: int) -> MarkdownListItem:
    first = _LIST_ITEM_RE.match(_strip_newline(lines[0]))
    if first is None:
        raise ValueError("list item does not start with a list marker")
    marker = first.group("marker")
    body = first.group("body")
    continuation = tuple(_strip_newline(line) for line in lines[1:])
    raw = "".join(lines)
    return MarkdownListItem(
        marker=marker,
        body="\n".join([body, *continuation]).rstrip(),
        raw=raw,
        span=MarkdownSpan(start_index + 1, start_index + len(lines)),
        continuation_lines=continuation,
    )


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


def _parse_table_alignment(cell: str) -> str:
    value = cell.strip()
    match = _DELIMITER_CELL.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid table delimiter cell: {cell}")
    if value.startswith(":") and value.endswith(":"):
        return "center"
    if value.endswith(":"):
        return "right"
    return "left"


def _display_cell(cell: str) -> str:
    return cell.replace("\\|", "|").strip()


def _is_thematic_break(text: str) -> bool:
    return _THEMATIC_BREAK_RE.fullmatch(text) is not None


def _is_block_start(lines: list[str], index: int) -> bool:
    text = _strip_newline(lines[index])
    if not text.strip():
        return True
    return (
        _is_thematic_break(text)
        or _try_peek_table(lines, index)
        or _FENCE_RE.match(text) is not None
        or _BLOCKQUOTE_RE.match(text) is not None
        or _LIST_ITEM_RE.match(text) is not None
    )


def _try_peek_table(lines: list[str], index: int) -> bool:
    if index + 1 >= len(lines):
        return False
    return _looks_like_table_header(_strip_newline(lines[index]), _strip_newline(lines[index + 1]))


def _strip_newline(value: str) -> str:
    return value[:-1] if value.endswith("\n") else value


def _leading_whitespace(value: str) -> str:
    return value[: len(value) - len(value.lstrip(" \t"))]


def _indent_width(value: str) -> int:
    return len(value.expandtabs(4))


def _node_raw(node: MarkdownAstNode) -> str:
    return node.raw


def _node_span(node: MarkdownAstNode) -> MarkdownSpan:
    return node.span


def _summarize_block(node: MarkdownAstNode) -> str:
    if isinstance(node, MarkdownFence):
        return f"[{node.fence[:3]} fence block]"
    if isinstance(node, MarkdownBlockquote):
        return "[blockquote attached]"
    if isinstance(node, MarkdownListBlock):
        return "[list attached]"
    if isinstance(node, MarkdownTableCandidate):
        return "[table attached]"
    if isinstance(node, MarkdownThematicBreak):
        return "[thematic break attached]"
    return "[markdown block attached]"


def _parse_markdown_image_target(target: str) -> tuple[str | None, str | None]:
    target = target.strip()
    if not target:
        return None, None
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1].strip()
    if not target:
        return None, None
    try:
        parts = shlex.split(target)
    except ValueError:
        return None, None
    if not parts:
        return None, None
    path = parts[0]
    title = None if len(parts) == 1 else " ".join(parts[1:])
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", path):
        return None, None
    return path, title


def _scan_markdown_image_candidate(text: str, index: int) -> tuple[int, str, str, str] | None:
    if not text.startswith("![", index):
        return None
    alt_start = index + 2
    alt_end = _find_matching_markdown_bracket(text, alt_start)
    if alt_end is None:
        return None
    target_start = alt_end + 1
    if target_start >= len(text) or text[target_start] != "(":
        return None
    target_end = _find_matching_markdown_paren(text, target_start + 1)
    if target_end is None:
        return None
    source = text[index : target_end + 1]
    alt_text = text[alt_start:alt_end]
    target = text[target_start + 1 : target_end]
    return target_end + 1, source, alt_text, target


def _find_matching_markdown_bracket(text: str, start: int) -> int | None:
    depth = 0
    escaped = False
    index = start
    while index < len(text):
        character = text[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == "[":
            depth += 1
        elif character == "]":
            if depth == 0:
                return index
            depth -= 1
        index += 1
    return None


def _find_matching_markdown_paren(text: str, start: int) -> int | None:
    depth = 1
    escaped = False
    quote: str | None = None
    index = start
    while index < len(text):
        character = text[index]
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif quote is not None:
            if character == quote:
                quote = None
        elif character in {'"', "'"}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _parse_fence_marker(text: str) -> tuple[str, int] | None:
    match = _FENCE_RE.match(text)
    if match is None or _indent_width(match.group("indent")) > 3:
        return None
    fence = match.group("fence")
    return fence[0], len(fence)


def _is_fence_closer(text: str, fence_char: str, fence_len: int) -> bool:
    match = re.fullmatch(
        rf"(?P<indent>[ \t]*){re.escape(fence_char)}{{{fence_len},}}[ \t]*",
        text,
    )
    return match is not None and _indent_width(match.group("indent")) <= 3


def _resolve_markdown_image_path(
    path_text: str,
    roots: tuple[Path, ...],
    *,
    exact_paths: set[Path] | None = None,
) -> tuple[Path | None, str | None]:
    candidate = Path(path_text)
    if candidate.is_absolute():
        resolved = candidate.resolve(strict=False)
        if resolved in (exact_paths or ()) or any(_within_root(resolved, root) for root in roots):
            return resolved, None
        return None, "invalid-root"
    for root in roots:
        resolved = (root / candidate).resolve(strict=False)
        if _within_root(resolved, root):
            return resolved, None
    return None, "invalid-root"


def _trusted_path_candidates(path: Path, roots: tuple[Path, ...]) -> tuple[Path, ...]:
    if path.is_absolute():
        resolved = path.resolve(strict=False)
        return (resolved,) if any(_within_root(resolved, root) for root in roots) else ()
    return tuple(
        resolved
        for root in roots
        if _within_root(resolved := (root / path).resolve(strict=False), root)
    )


def _trusted_artifact_candidates(path: Path, roots: tuple[Path, ...]) -> tuple[Path, ...]:
    if path.is_absolute():
        return (path.resolve(strict=False),)
    return _trusted_path_candidates(path, roots)


def _within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _is_supported_image(path: Path) -> bool:
    if path.suffix.lower() not in _SUPPORTED_IMAGE_SUFFIXES:
        return False
    try:
        with Image.open(path) as image:
            image.verify()
    except (OSError, SyntaxError, UnidentifiedImageError):
        return False
    return True
