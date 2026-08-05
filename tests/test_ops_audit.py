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


def test_selected_hardware_lanes_require_real_resume_and_recheck_health() -> None:
    repository = Path(__file__).resolve().parents[1]
    macos = (
        repository
        / "src"
        / "copilotd"
        / "ops"
        / "scripts"
        / "acceptance-macos.sh"
    ).read_text(encoding="utf-8")
    windows = (
        repository
        / "src"
        / "copilotd"
        / "ops"
        / "scripts"
        / "acceptance-windows.ps1"
    ).read_text(encoding="utf-8")
    workflow = (
        repository / ".github" / "workflows" / "ops-acceptance.yml"
    ).read_text(encoding="utf-8")

    assert "COPILOTD_ACCEPTANCE_ALLOW_SLEEP" in macos
    assert "pmset relative wake" in macos
    assert 'wake_deadline="$((sleep_requested_at + 180))"' in macos
    assert "scheduled wake interval" in macos
    assert '!= "recent-wake"' in macos
    assert "status-after-soak.json" in macos
    assert "bot generation changed during soak" in macos
    assert "if ! log show" in macos
    assert "existing LaunchAgent would be replaced" in macos
    assert "copilotd service uninstall" in macos
    assert "COPILOTD_ACCEPTANCE_EVIDENCE_DIR" in macos
    assert "credential leaked into acceptance artifact" in macos
    assert '"cleanup_verified": True' in macos
    assert "COPILOTD_ACCEPTANCE_ALLOW_SLEEP" in windows
    assert "SetSuspendState" in windows
    assert "$wakeAt = (Get-Date).AddMinutes(3)" in windows
    assert "wake trigger does not have sufficient suspend margin" in windows
    assert "$wakeDeadline = $wakeAt.ToUniversalTime().AddMinutes(2)" in windows
    assert "StartTime=$sleepRequestedAt" in windows
    assert "wake marker is outside the intended wake interval" in windows
    assert "'recent-wake'" in windows
    assert "service is not ready after resume" in windows
    assert "runs-on: [self-hosted, macOS, copilotd-acceptance]" in workflow
    assert "runs-on: [self-hosted, Windows, copilotd-acceptance]" in workflow
    assert "sdk-probe --live" in workflow
    assert "--expect-response COPILOTD_ACCEPTANCE_AUTH_OK" in workflow
    assert "macos-15" not in workflow
    assert "windows-2025" not in workflow


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
