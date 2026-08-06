from __future__ import annotations

import argparse
import json
import platform
from dataclasses import asdict, dataclass
from importlib import resources
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from copilotd.ops.contracts import (
    EXPECTED_MIGRATION_VERSIONS,
    EXPECTED_SDK_VERSION,
    LATEST_MIGRATION_VERSION,
)
from copilotd.ops.design_html import audit_design_html


@dataclass(frozen=True, slots=True)
class AuditCheck:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class OperationsAudit:
    schema_version: int
    ok: bool
    checks: tuple[AuditCheck, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "ok": self.ok,
            "checks": [asdict(check) for check in self.checks],
        }


def run_operations_audit(repository: Path | None = None) -> OperationsAudit:
    checks: list[AuditCheck] = []
    sdk_version = _version("github-copilot-sdk")
    checks.append(
        AuditCheck(
            "sdk-version",
            sdk_version == EXPECTED_SDK_VERSION,
            f"expected {EXPECTED_SDK_VERSION}, found {sdk_version}",
        )
    )
    checks.append(
        AuditCheck(
            "python-version",
            tuple(map(int, platform.python_version_tuple()[:2])) >= (3, 11),
            platform.python_version(),
        )
    )
    migration_root = resources.files("copilotd.storage.migrations")
    migrations = sorted(
        item.name for item in migration_root.iterdir() if item.name.endswith(".sql")
    )
    versions = tuple(int(name.partition("_")[0]) for name in migrations)
    latest = max(versions, default=0)
    checks.append(
        AuditCheck(
            "packaged-migrations",
            versions == EXPECTED_MIGRATION_VERSIONS and latest == LATEST_MIGRATION_VERSION,
            f"versions={list(versions)}, files={len(migrations)}",
        )
    )
    script_root = resources.files("copilotd.ops.scripts")
    scripts = {item.name for item in script_root.iterdir()}
    required_scripts = {
        "acceptance-macos.sh",
        "acceptance-windows.ps1",
        "package-smoke.sh",
    }
    missing_scripts = sorted(required_scripts - scripts)
    checks.append(
        AuditCheck(
            "packaged-operations-scripts",
            not missing_scripts,
            "complete" if not missing_scripts else "missing: " + ", ".join(missing_scripts),
        )
    )
    if repository is not None:
        html_audit = audit_design_html(
            repository / "docs" / "copilotD-detailed-design.md",
            repository / "docs" / "copilotD-detailed-design.html",
        )
        checks.append(
            AuditCheck(
                "design-html",
                html_audit.ok,
                (
                    f"headings={html_audit.heading_count}, anchors={html_audit.anchor_count}"
                    if html_audit.ok
                    else "; ".join(html_audit.errors)
                ),
            )
        )
    return OperationsAudit(
        schema_version=1,
        ok=all(check.ok for check in checks),
        checks=tuple(checks),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="copilotd-ops-audit")
    parser.add_argument("--repository", type=Path)
    arguments = parser.parse_args(argv)
    report = run_operations_audit(arguments.repository)
    print(json.dumps(report.as_dict(), ensure_ascii=False, sort_keys=True))
    if not report.ok:
        raise SystemExit(1)


def _version(package: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return "not-installed"
