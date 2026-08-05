# copilotD

`copilotD` is an always-on, single-user Discord bridge for the official GitHub
Copilot Python SDK. It keeps one durable Copilot session per Discord thread,
uses the operator's resolved `$HOME` when a channel is not explicitly bound to
a project, and runs the Copilot runtime with `--yolo`.

The implementation follows [`docs/copilotD-detailed-design.md`](docs/copilotD-detailed-design.md).

## Implemented

- Bundled Copilot runtime launched with `--yolo`, with allow-all verified on every
  create/resume and reconciled again after runtime permission-change events.
- One long-lived SDK session per Discord thread, eager resume, owner fencing, and
  conservative unknown outcomes across crash windows.
- Explicit channel project bindings with immutable cwd snapshots; unbound channels
  always resolve to the service user's `$HOME`.
- Durable SQLite event journal, app FIFO, command mailbox, liveness leases, readiness
  snapshots, render outbox, and attachment manifests.
- Streaming replies, file/image attachments, table-aware Markdown rendering, and one
  in-thread TaskDeck for tool/subagent activity with select, expand/collapse, and
  pagination controls.
- Durable Copilot input requests: ask-user choices/freeform, Plan exit actions, and
  auto-mode-switch prompts render in-place and settle exactly once without blocking
  event reduction.
- Background task `refresh/list` reconciliation, disappearance-to-unknown handling,
  usage/status rendering, and lossless attachment delivery for tool output at or above
  8000 characters; oversized Discord uploads are split into ordered lossless parts.
- Core `/session`, `/project`, `/model`, `/autopilot`, `/plan`, `/steer`, `/context`,
  `/usage`, and `/queue` commands.
- Default always-on definitions for macOS LaunchAgents and Windows Scheduled Tasks,
  effective-definition/PID verification, protected-work-aware restart coordination,
  sleep/resume suppression, and durable restart-storm alerts.

Native-gated commands such as Fleet, Tasks, quick ask, runtime schedules, and remote
sessions are intentionally not registered until their pinned-runtime fixtures are
implemented. The verified sidecar does not retain sessions after client transport
disconnect, so the current topology is bundled-runtime rather than detached execution.

## Setup

Python 3.11 or newer and an authenticated GitHub Copilot CLI identity are required.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
export COPILOTD_DISCORD_TOKEN='...'
.venv/bin/copilotd setup
```

`setup` first verifies the Discord token with Discord, starts the pinned Copilot runtime
to validate authentication/version/model access, checks timezone data and private
directories, then installs and starts the current platform definitions. It succeeds only
after a new heartbeat reports `gateway_state=ready`, `runtime_state=ready`, and a PID that
matches the effective OS-managed process. The token is stored in a private per-user
service secret file and is never embedded in a plist, Task XML, or PowerShell script.
For local development, use the explicit foreground entrypoint:

```bash
.venv/bin/copilotd run --foreground
```

Useful operations:

```bash
.venv/bin/copilotd service status
.venv/bin/copilotd service logs
.venv/bin/copilotd service restart
.venv/bin/copilotd doctor
.venv/bin/copilotd-ops-audit --repository .
```

`service restart` fails closed when the heartbeat is missing, malformed, stale, or does
not match the OS PID. A normal restart also refuses active current-generation leases,
queued work, remote exposure, native schedules, and trigger windows. `--force` first
durably quiesces all create/resume/send/callback/internal producers, atomically compares
producer and event-journal epochs, marks only true in-flight outcomes unknown, and commits
owner-lease handoff before replacing the process. Once force preparation is durable, a
later failure terminates fail-closed and cannot reopen the old process.

The control protocol is versioned. Upgrades stop and verify a legacy worker before issuing
a v2 fence; replacement adoption requires the private manager handoff token plus OS
PID/start identity. Inbox overflow and accounting failures leave durable watermark files
that block restart, while rollback is bounded and persistently retried.

## Development

Python 3.11 or newer is required.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/copilotd doctor
.venv/bin/pytest
```

The live SDK probe uses the currently logged-in Copilot account and creates a
disposable persistent session:

```bash
.venv/bin/copilotd sdk-probe --live
```

Runtime paths are fixed by platform:

| Platform | State | Heartbeat | Logs |
|---|---|---|---|
| macOS | `~/Library/Application Support/copilotd/` | `~/Library/Caches/copilotd/heartbeat.json` | `~/Library/Logs/copilotd/` |
| Windows | `%LOCALAPPDATA%\copilotd\state\` | `%LOCALAPPDATA%\copilotd\cache\heartbeat.json` | `%LOCALAPPDATA%\copilotd\logs\` |

On first Windows upgrade, a legacy `%LOCALAPPDATA%\copilotD\` state tree is adopted
through a lock-protected two-rename migration before any new directories are created, so
the existing database and session state remain visible.

`copilotd.log` is rotating JSON (10 MiB with seven backups); `boot.log`,
`watchdog.log`, and `alerts.log` have distinct destinations. Override paths with
`COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and `COPILOTD_LOG_DIR`. A guild-scoped
development command sync can be selected with `COPILOTD_DISCORD_GUILD_ID`.

The opt-in hardware/credential lanes are `scripts/acceptance-macos.sh` and
`scripts/acceptance-windows.ps1`. They intentionally exit with failure when selected on
the wrong OS or without required credentials/system facilities; they never silently
skip. The workflow uses dedicated interactive self-hosted runners whose user profile is
already authenticated to Copilot, runs a live SDK preflight, and requires permission to
schedule a wake and put the machine to sleep. `scripts/package-smoke.sh` builds and
installs both the wheel and sdist in isolated environments.

The macOS lane refuses to overwrite pre-existing copilotD LaunchAgents, validates a real
restart plus post-wake soak, scans its work artifacts for the Discord token, and always
uninstalls its test service. Set `COPILOTD_ACCEPTANCE_EVIDENCE_DIR` to retain its
permission-restricted sanitized JSON summary.
