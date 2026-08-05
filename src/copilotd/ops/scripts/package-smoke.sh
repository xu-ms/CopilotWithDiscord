#!/bin/sh
set -eu

python="${PYTHON:-python3}"
command -v "$python" >/dev/null 2>&1 || {
  printf '%s\n' "package smoke prerequisite failed: Python is required" >&2
  exit 2
}
"$python" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))' || {
  printf '%s\n' "package smoke prerequisite failed: Python 3.11+ is required" >&2
  exit 2
}

root="${1:-.}"
work="$(mktemp -d "${TMPDIR:-/tmp}/copilotd-package-smoke.XXXXXX")"
trap 'rm -rf "$work"' EXIT HUP INT TERM
"$python" -m build --wheel --sdist --outdir "$work/dist" "$root"

for artifact in "$work"/dist/*.whl "$work"/dist/*.tar.gz; do
  name="$(basename "$artifact" | tr '.-' '__')"
  "$python" -m venv "$work/$name"
  "$work/$name/bin/python" -m pip install --quiet "$artifact"
  "$work/$name/bin/copilotd" --version
  "$work/$name/bin/python" - <<'PY'
from importlib import resources

migrations = resources.files("copilotd.storage.migrations")
scripts = resources.files("copilotd.ops.scripts")
assert any(item.name.endswith(".sql") for item in migrations.iterdir())
assert {"acceptance-macos.sh", "acceptance-windows.ps1", "package-smoke.sh"} <= {
    item.name for item in scripts.iterdir()
}
PY
done
