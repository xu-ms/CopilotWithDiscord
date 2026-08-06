# copilotD

`copilotD` is an always-on, single-user Discord bridge for the official GitHub
Copilot Python SDK. It keeps one durable Copilot session per Discord thread,
uses the operator's resolved `$HOME` when a channel is not explicitly bound to
a project, and runs the Copilot runtime with `--yolo`.

The implementation follows [`docs/copilotD-detailed-design.md`](docs/copilotD-detailed-design.md).

## Implemented

- Exact SDK/runtime/protocol startup assertion (`1.0.8` / `1.0.73` / `3`) backed
  by persisted, hash-checked capability evidence rather than generated method presence.
- Bundled Copilot runtime launched with `--yolo`, with allow-all verified on every
  create/resume and reconciled again after runtime permission-change events.
- One long-lived SDK session per Discord thread, eager resume, owner fencing, and
  conservative unknown outcomes across crash windows.
- Explicit channel project bindings with immutable cwd snapshots; unbound channels
  always resolve to the service user's `$HOME`.
- Durable SQLite event journal with strict UUID SDK IDs, app FIFO, reducer-owned
  operation receipts, liveness leases, epoch/watermark snapshots, render outbox,
  and attachment manifests.
- Durable event-log backfill with cursor rebase/gap diagnostics and ingress-overflow
  freeze/backfill/generation replacement; unrecoverable ephemeral gaps remain
  explicitly outcome-unknown.
- Streaming replies, file/image attachments, table-aware Markdown rendering, and one
  in-thread TaskDeck for tool/subagent activity with select, expand/collapse, and
  pagination controls.
- Durable Copilot input requests: ask-user choices/freeform, Plan exit actions, and
  auto-mode-switch prompts render in-place and settle exactly once without blocking
  event reduction.
- Background task `refresh/list` reconciliation, disappearance-to-unknown handling,
  usage/status rendering, and lossless attachment delivery for tool output at or above
  8000 characters; oversized Discord uploads are split into ordered lossless parts.
- Capability-backed registration for core commands plus exact native `/ask`,
  `/session compact`, `/fleet`, `/tasks`, `/agent`, `/after`, `/every`, `/remote`,
  `/review`, `/security-review`, `/research`, and `/rubber-duck` surfaces. Each native
  root or action remains absent until its exact runtime method or builtin invocation is
  verified.
- Runtime command-manifest refresh through `commands.list(include_builtins=true)` and
  `commands.changed`, full typed `commands.invoke` result-union handling, fenced
  idempotent task/agent/remote transitions, and reducer-owned native schedule/TaskDeck
  projections.
- Default always-on definitions for macOS LaunchAgents and Windows Scheduled Tasks,
  plus a protected-work-aware watchdog, shared task-failure supervision, and a
  non-destructive active-execution SUSPECT monitor.

Unsupported native capabilities fail closed and remain unregistered. The verified
sidecar does not retain sessions after client transport disconnect, so the current
topology is bundled-runtime rather than detached execution.

## Setup

Python 3.11 or newer and an authenticated GitHub Copilot CLI identity are required.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
export COPILOTD_DISCORD_TOKEN='...'
.venv/bin/copilotd setup
```

`setup` installs and starts the current platform's service definitions. For local
development, use the explicit foreground entrypoint:

```bash
.venv/bin/copilotd run --foreground
```

Useful operations:

```bash
.venv/bin/copilotd service status
.venv/bin/copilotd service logs
.venv/bin/copilotd service restart
.venv/bin/copilotd doctor
```

## Development

Python 3.11 or newer is required.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/copilotd doctor
.venv/bin/pytest
```

Deterministic tests consume the bundled hash-checked fixture. Live acceptance is
separate. The broad SDK probe creates a disposable persistent session:

```bash
.venv/bin/copilotd sdk-probe --live
```

The complete native acceptance additionally executes supported RPC mutations and
builtins in a disposable Git repository, cleans up schedules/remote exposure/session
state, and writes sanitized JSON evidence. It requires both the CLI flag and an exact
environment confirmation:

```bash
export COPILOTD_REAL_ACCEPTANCE='I_UNDERSTAND_THIS_USES_REAL_COPILOT'
.venv/bin/copilotd native-acceptance --real --evidence /tmp/copilotd-native-evidence.json
```

Runtime data, cache, and logs use platform-specific user directories. Override them
with `COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and `COPILOTD_LOG_DIR`. A guild-scoped
development command sync can be selected with `COPILOTD_DISCORD_GUILD_ID`.
