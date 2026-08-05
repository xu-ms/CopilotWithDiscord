from importlib import resources
from pathlib import Path
from types import SimpleNamespace

import pytest

from copilotd.ops.audit import run_operations_audit
from copilotd.ops.design_html import audit_design_html, generate_design_html


def test_operations_audit_covers_packaged_migrations_scripts_and_html() -> None:
    repository = Path(__file__).resolve().parents[1]

    report = run_operations_audit(repository)

    assert report.ok is True, report.as_dict()
    checks = {check.name: check for check in report.checks}
    assert checks["packaged-migrations"].ok is True
    assert checks["packaged-operations-scripts"].ok is True
    assert checks["design-html"].ok is True


def test_design_html_has_valid_anchors_and_no_remote_assets() -> None:
    repository = Path(__file__).resolve().parents[1]

    audit = audit_design_html(
        repository / "docs" / "copilotD-detailed-design.md",
        repository / "docs" / "copilotD-detailed-design.html",
    )

    assert audit.ok is True, audit.errors
    assert audit.heading_count > 50
    assert audit.anchor_count > 100


def test_operations_scripts_are_wheel_package_resources() -> None:
    names = {item.name for item in resources.files("copilotd.ops.scripts").iterdir()}
    assert {
        "acceptance-macos.sh",
        "acceptance-windows.ps1",
        "package-smoke.sh",
    } <= names


def test_design_html_generator_is_reproducible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    markdown = tmp_path / "design.md"
    output = tmp_path / "design.html"
    markdown.write_text("# Design\n", encoding="utf-8")
    monkeypatch.setattr("copilotd.ops.design_html.shutil.which", lambda _: "/fake/pandoc")

    def fake_run(command: list[str], **_: object) -> SimpleNamespace:
        target = Path(command[command.index("--output") + 1])
        target.write_text(
            "\n".join(
                [
                    "<!doctype html>",
                    '<meta name="generator" content="pandoc 9.9" />',
                    "<style>base { color: black; }</style>",
                    '<body><h1 id="design">Design</h1></body>',
                ]
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("copilotd.ops.design_html.subprocess.run", fake_run)
    generate_design_html(markdown, output)
    first = output.read_bytes()
    generate_design_html(markdown, output)

    assert output.read_bytes() == first
    assert b"copilotd-design-generator-v1" in first
    assert b"max-width: 100rem" in first


def test_design_html_generator_fails_when_pandoc_is_not_selected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("copilotd.ops.design_html.shutil.which", lambda _: None)
    with pytest.raises(RuntimeError, match="pandoc is required"):
        generate_design_html(tmp_path / "design.md", tmp_path / "design.html")
