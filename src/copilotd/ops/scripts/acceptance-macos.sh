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

work="$(mktemp -d "${TMPDIR:-/tmp}/copilotd-macos-acceptance.XXXXXX")"
trap 'rm -rf "$work"' EXIT HUP INT TERM
test_started_at="$(date +%s)"
log_started_at="$(date -r "$test_started_at" '+%Y-%m-%d %H:%M:%S')"

copilotd setup >"$work/setup.json"
copilotd service status >"$work/status-before.json"
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
