from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

_FIXED_STYLE = """
body { max-width: 100rem; margin: 0 auto; padding: 2rem; overflow-wrap: anywhere; }
table { display: block; width: 100%; overflow-x: auto; border-collapse: collapse; }
th { position: sticky; top: 0; background: Canvas; }
th, td { min-width: 9rem; max-width: 36rem; padding: .6rem; border: 1px solid #9ca3af; }
pre { max-width: 100%; overflow: auto; }
@media print { table, pre { break-inside: avoid; } h2, h3, h4 { break-after: avoid; } }
""".strip()


@dataclass(frozen=True, slots=True)
class HtmlAudit:
    ok: bool
    errors: tuple[str, ...]
    heading_count: int
    anchor_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class _DocumentInventory(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: list[str] = []
        self.anchor_targets: list[str] = []
        self.remote_assets: list[str] = []
        self.heading_count = 0
        self.text: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        values = dict(attrs)
        if identifier := values.get("id"):
            self.ids.append(identifier)
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.heading_count += 1
        if tag == "a" and (href := values.get("href")) and href.startswith("#"):
            self.anchor_targets.append(href[1:])
        asset = (
            values.get("src")
            if tag in {"img", "script"}
            else values.get("href")
            if tag == "link" and values.get("rel") == "stylesheet"
            else None
        )
        if asset and re.match(r"^https?://", asset):
            self.remote_assets.append(asset)

    def handle_data(self, data: str) -> None:
        self.text.append(data)


def generate_design_html(
    markdown_path: Path,
    output_path: Path,
    *,
    pandoc: str = "pandoc",
) -> None:
    executable = shutil.which(pandoc)
    if executable is None:
        raise RuntimeError("pandoc is required for design HTML generation; install it explicitly")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="copilotd-design-html-") as directory:
        temporary = Path(directory) / "design.html"
        environment = {**os.environ, "SOURCE_DATE_EPOCH": "0", "TZ": "UTC"}
        subprocess.run(
            [
                executable,
                "--from=gfm",
                "--to=html5",
                "--standalone",
                "--toc",
                "--toc-depth=4",
                "--metadata=lang:zh-CN",
                "--metadata=title:copilotD 详细设计 v2.5",
                "--output",
                str(temporary),
                str(markdown_path),
            ],
            check=True,
            env=environment,
            capture_output=True,
            text=True,
        )
        generated = temporary.read_text(encoding="utf-8")
        generated = re.sub(
            r'<meta name="generator" content="[^"]*"\s*/?>',
            '<meta name="generator" content="copilotd-design-generator-v1" />',
            generated,
            count=1,
        )
        generated = generated.replace(
            "</style>",
            f"\n{_FIXED_STYLE}\n</style>",
            1,
        )
        stylesheet_path = markdown_path.with_suffix(".css")
        if stylesheet_path.is_file():
            stylesheet = stylesheet_path.read_text(encoding="utf-8").strip()
            generated = generated.replace(
                "</head>",
                f'<style type="text/css">\n{stylesheet}\n</style>\n</head>',
                1,
            )
        normalized = (
            "\n".join(
                line.rstrip() for line in generated.replace("\r\n", "\n").splitlines()
            ).rstrip()
            + "\n"
        )
        target = output_path.with_name(f".{output_path.name}.tmp")
        target.write_text(normalized, encoding="utf-8")
        os.replace(target, output_path)


def audit_design_html(markdown_path: Path, html_path: Path) -> HtmlAudit:
    errors: list[str] = []
    try:
        markdown = markdown_path.read_text(encoding="utf-8")
        html = html_path.read_text(encoding="utf-8")
    except OSError as error:
        return HtmlAudit(False, (str(error),), 0, 0)
    inventory = _DocumentInventory()
    try:
        inventory.feed(html)
        inventory.close()
    except Exception as error:
        errors.append(f"HTML parser failure: {error}")
    duplicate_ids = sorted(
        identifier for identifier in set(inventory.ids) if inventory.ids.count(identifier) > 1
    )
    missing_targets = sorted(set(inventory.anchor_targets) - set(inventory.ids))
    if duplicate_ids:
        errors.append("duplicate HTML ids: " + ", ".join(duplicate_ids[:10]))
    if missing_targets:
        errors.append("missing table-of-contents anchors: " + ", ".join(missing_targets[:10]))
    if inventory.remote_assets:
        errors.append("remote assets are forbidden: " + ", ".join(inventory.remote_assets))
    visible_html = " ".join(" ".join(inventory.text).split())
    for required in (
        "macOS / Windows 默认自启动与进程保活",
        "Heartbeat 协议",
        "claudeD issue 回归门禁",
        "single-user --yolo",
    ):
        if required not in markdown:
            errors.append(f"Markdown source is missing required content: {required}")
        if required not in visible_html:
            errors.append(f"HTML output is missing required content: {required}")
    if "max-width: 100rem" not in html and "max-width: 90rem" not in html:
        errors.append("HTML body width contract is missing")
    if "position: sticky" not in html:
        errors.append("HTML sticky table-header contract is missing")
    return HtmlAudit(
        ok=not errors,
        errors=tuple(errors),
        heading_count=inventory.heading_count,
        anchor_count=len(inventory.ids),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="copilotd-design-html")
    parser.add_argument("action", choices=("generate", "audit"))
    parser.add_argument(
        "--markdown",
        type=Path,
        default=Path("docs/copilotD-detailed-design.md"),
    )
    parser.add_argument(
        "--html",
        type=Path,
        default=Path("docs/copilotD-detailed-design.html"),
    )
    arguments = parser.parse_args(argv)
    if arguments.action == "generate":
        generate_design_html(arguments.markdown, arguments.html)
        return
    audit = audit_design_html(arguments.markdown, arguments.html)
    print(json.dumps(audit.as_dict(), ensure_ascii=False, sort_keys=True))
    if not audit.ok:
        raise SystemExit(1)
