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
- Capability-backed registration for core `/session`, `/project`, `/model`,
  `/autopilot`, `/plan`, `/steer`, `/context`, `/usage`, and `/queue` commands.
- Application-owned `/schedule` message/new-session jobs with durable leases, DST-safe
  IANA time handling, FIFO coupling, crash recovery, and semantic completion evidence.
- Intent-first `/project worktree` lifecycle with exact Git ownership checks, durable
  compensation/recovery, reference blockers, and capability-gated history forks.
- Default always-on definitions for macOS LaunchAgents and Windows Scheduled Tasks,
  plus a protected-work-aware watchdog, shared task-failure supervision, and a
  non-destructive active-execution SUSPECT monitor.

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
export COPILOTD_DISCORD_OPERATOR_IDS='123456789012345678'
.venv/bin/copilotd setup
```

`setup` installs and starts the current platform's service definitions. For local
development, use the explicit foreground entrypoint:

`COPILOTD_DISCORD_OPERATOR_IDS` is a comma-separated allowlist. Administrative
`/project` (including MCP, variables, agents, and worktrees) and runtime restart
commands fail closed when the caller is not listed. Discord never reveals stored
project variable values.

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
separate: the SDK probe uses the currently logged-in Copilot account and creates a
disposable persistent session:

```bash
.venv/bin/copilotd sdk-probe --live
```

Scheduler/worktree integration has a stricter opt-in runner. It uses real Copilot
sessions and real temporary Git worktrees, fails when authentication is unavailable,
cleans disposable resources, and writes sanitized per-feature JSON plus `summary.json`:

```bash
.venv/bin/copilotd-live-acceptance \
  --live \
  --output "$HOME/copilotd-live-results"
```

The runner accepts injected `ThreadGateway` and `HistoryForkAdapter` implementations,
so a combined Discord branch can drive real threads without coupling scheduler state to
Discord rendering internals. History fork is fail-closed when no verified adapter exists.

Runtime data, cache, and logs use platform-specific user directories. Override them
with `COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and `COPILOTD_LOG_DIR`. A guild-scoped
development command sync can be selected with `COPILOTD_DISCORD_GUILD_ID`.
