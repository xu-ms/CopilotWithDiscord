#!/bin/sh
set -eu

fail() {
  printf '%s\n' "macOS acceptance prerequisite failed: $*" >&2
  exit 2
}

[ "$(uname -s)" = "Darwin" ] || fail "requires a real macOS host"
[ -n "${COPILOTD_DISCORD_TOKEN:-}" ] || fail "COPILOTD_DISCORD_TOKEN is required"
[ "${COPILOTD_ACCEPTANCE_ALLOW_SLEEP:-}" = "1" ] ||
  fail "COPILOTD_ACCEPTANCE_ALLOW_SLEEP=1 is required"
for command in copilotd launchctl plutil pmset python3 log sudo; do
  command -v "$command" >/dev/null 2>&1 || fail "missing command: $command"
done
sudo -n true >/dev/null 2>&1 ||
  fail "passwordless sudo is required to schedule and enter sleep"
for label in \
  com.github.copilotd.runtime \
  com.github.copilotd.bot \
  com.github.copilotd.watchdog; do
  [ ! -e "$HOME/Library/LaunchAgents/$label.plist" ] ||
    fail "existing LaunchAgent would be replaced: $label"
  if launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1; then
    fail "existing effective LaunchAgent would be replaced: $label"
  fi
done

work="$(mktemp -d "${TMPDIR:-/tmp}/copilotd-macos-acceptance.XXXXXX")"
installed=0
cleanup() {
  if [ "$installed" = 1 ]; then
    copilotd service uninstall >"$work/uninstall-fallback.json" 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM
test_started_at="$(date +%s)"
log_started_at="$(date -r "$test_started_at" '+%Y-%m-%d %H:%M:%S')"

copilotd setup >"$work/setup.json"
installed=1
copilotd service status >"$work/status-before.json"
copilotd service logs >"$work/logs.json"
python3 - "$work/status-before.json" "$work/identity.json" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
if not status["ready"]:
    raise SystemExit("copilotD status is not ready")
if status["definition_drift"]:
    raise SystemExit(f"definition drift: {status['definition_drift']}")
if not status["process_identity_matches"]:
    raise SystemExit("launchd PID does not match heartbeat PID")
json.dump(
    {
        "pid": status["pid"],
        "process_generation": status["process_generation"],
    },
    open(sys.argv[2], "w", encoding="utf-8"),
)
PY

copilotd service restart >"$work/restart.json"
copilotd service status >"$work/status-after-restart.json"
python3 - \
  "$work/status-before.json" \
  "$work/status-after-restart.json" \
  "$work/identity.json" <<'PY'
import json
import sys

before = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
after = json.load(open(sys.argv[2], encoding="utf-8"))["result"]
if not after["ready"] or not after["process_identity_matches"]:
    raise SystemExit("service is not ready after restart")
if before["process_generation"] == after["process_generation"]:
    raise SystemExit("restart did not create a new process generation")
json.dump(
    {
        "pid": after["pid"],
        "process_generation": after["process_generation"],
    },
    open(sys.argv[3], "w", encoding="utf-8"),
)
PY

sleep_requested_at="$(date +%s)"
wake_deadline="$((sleep_requested_at + 180))"
sudo -n pmset relative wake 60
sudo -n pmset sleepnow

python3 - "$sleep_requested_at" "$wake_deadline" <<'PY'
import sys
import time

from copilotd.ops.wake import macos_last_resume_timestamp

started = float(sys.argv[1])
deadline = float(sys.argv[2])
while True:
    resumed = macos_last_resume_timestamp()
    if resumed is not None and started <= resumed <= deadline:
        break
    if time.time() >= deadline:
        raise SystemExit(
            "no macOS wake event occurred in the scheduled wake interval"
        )
    time.sleep(1)
PY

copilotd service watchdog >"$work/watchdog-after-wake.json"
python3 - "$work/watchdog-after-wake.json" <<'PY'
import json
import sys

outcome = json.load(open(sys.argv[1], encoding="utf-8"))
if outcome["result"]["watchdog"] != "recent-wake":
    raise SystemExit(f"wake suppression did not trigger: {outcome}")
PY
copilotd service status >"$work/status-after-wake.json"
python3 - "$work/status-after-wake.json" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
if not status["ready"] or not status["process_identity_matches"]:
    raise SystemExit("service is not ready after wake")
PY

sleep "${COPILOTD_ACCEPTANCE_SOAK_SECONDS:-3600}"
copilotd service status >"$work/status-after-soak.json"
python3 - "$work/status-after-soak.json" "$work/identity.json" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))["result"]
identity = json.load(open(sys.argv[2], encoding="utf-8"))
if not status["ready"] or not status["process_identity_matches"]:
    raise SystemExit("service died or became unready during soak")
if status["heartbeat_age_seconds"] is None or status["heartbeat_age_seconds"] > 45:
    raise SystemExit("heartbeat is not fresh after soak")
if status["pid"] != identity["pid"]:
    raise SystemExit("bot PID changed during soak")
if status["process_generation"] != identity["process_generation"]:
    raise SystemExit("bot generation changed during soak")
PY

if ! log show --style compact --start "$log_started_at" --predicate \
  'process == "launchd" AND eventMessage CONTAINS "com.github.copilotd"' \
  >"$work/launchd.log"; then
  fail "launchd log query failed"
fi
if grep -q "because inefficient" "$work/launchd.log"; then
  fail "launchd reported because inefficient"
fi

copilotd service uninstall >"$work/uninstall.json"
installed=0
for label in \
  com.github.copilotd.runtime \
  com.github.copilotd.bot \
  com.github.copilotd.watchdog; do
  [ ! -e "$HOME/Library/LaunchAgents/$label.plist" ] ||
    fail "LaunchAgent plist remains after cleanup: $label"
  if launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1; then
    fail "effective LaunchAgent remains after cleanup: $label"
  fi
done

python3 - "$work" "${COPILOTD_ACCEPTANCE_EVIDENCE_DIR:-}" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

work = Path(sys.argv[1])
evidence_dir = Path(sys.argv[2]) if sys.argv[2] else None
token = os.environ["COPILOTD_DISCORD_TOKEN"].encode()
for path in work.iterdir():
    if path.is_file() and token in path.read_bytes():
        raise SystemExit(f"credential leaked into acceptance artifact: {path.name}")

def result(name):
    value = json.loads((work / name).read_text(encoding="utf-8"))
    return value.get("result", value)

before = result("status-before.json")
after_restart = result("status-after-restart.json")
after_wake = result("status-after-wake.json")
after_soak = result("status-after-soak.json")
watchdog = result("watchdog-after-wake.json")
summary = {
    "schema_version": 1,
    "setup_ready": before["ready"],
    "restart_ready": after_restart["ready"],
    "restart_generation_changed": (
        before["process_generation"] != after_restart["process_generation"]
    ),
    "wake_watchdog": watchdog["watchdog"],
    "wake_ready": after_wake["ready"],
    "soak_ready": after_soak["ready"],
    "soak_identity_stable": (
        after_restart["pid"] == after_soak["pid"]
        and after_restart["process_generation"]
        == after_soak["process_generation"]
    ),
    "soak_generation_hash": hashlib.sha256(
        after_soak["process_generation"].encode()
    ).hexdigest()[:16],
    "heartbeat_age_seconds": after_soak["heartbeat_age_seconds"],
    "cleanup_verified": True,
    "secret_scan": "clean",
}
if evidence_dir is not None:
    evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    output = evidence_dir / "macos-acceptance.sanitized.json"
    output.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output.chmod(0o600)
print(json.dumps(summary, sort_keys=True))
PY
