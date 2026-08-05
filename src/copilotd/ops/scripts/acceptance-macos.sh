#!/bin/sh
set -eu

fail() {
  printf '%s\n' "macOS acceptance prerequisite failed: $*" >&2
  exit 2
}

[ "$(uname -s)" = "Darwin" ] || fail "requires a real macOS host"
[ -n "${COPILOTD_DISCORD_TOKEN:-}" ] || fail "COPILOTD_DISCORD_TOKEN is required"
for command in copilotd launchctl plutil pmset python3; do
  command -v "$command" >/dev/null 2>&1 || fail "missing command: $command"
done

work="$(mktemp -d "${TMPDIR:-/tmp}/copilotd-macos-acceptance.XXXXXX")"
trap 'rm -rf "$work"' EXIT HUP INT TERM
copilotd setup >"$work/setup.json"
copilotd service status >"$work/status.json"
python3 - "$work/status.json" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
if not status["ready"]:
    raise SystemExit("copilotD status is not ready")
if status["definition_drift"]:
    raise SystemExit(f"definition drift: {status['definition_drift']}")
if not status["process_identity_matches"]:
    raise SystemExit("launchd PID does not match heartbeat PID")
PY

pmset -g log | grep -E 'DarkWake|Wake' >/dev/null ||
  fail "no wake history is available for the 60-second guard lane"
sleep "${COPILOTD_ACCEPTANCE_SOAK_SECONDS:-3600}"
if log show --style compact --last 70m --predicate \
  'process == "launchd" AND eventMessage CONTAINS "com.github.copilotd"' |
  grep -q "because inefficient"; then
  fail "launchd reported because inefficient"
fi
