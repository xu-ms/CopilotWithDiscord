# copilotD

`copilotD` is an always-on, single-user Discord bridge for the official GitHub
Copilot Python SDK. It keeps one durable Copilot session per Discord thread,
uses the operator's resolved `$HOME` when a channel is not explicitly bound to
a project, and runs the Copilot runtime with `--yolo`.

The implementation follows [`docs/copilotD-detailed-design.md`](docs/copilotD-detailed-design.md).

## Implemented

- Exact SDK/runtime/protocol startup assertion (`1.0.8` / `1.0.73` / `3`) backed
  by persisted, hash-checked capability evidence rather than generated method presence.
- Bundled Copilot runtime launched with `--yolo`, with managed policy resolved from an
  explicit session credential before allow-all is verified on every create/resume.
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
- Immutable, versioned create/resume configuration for custom agents, skill/plugin
  directories, disabled skills, stdio/HTTP MCP servers, and environment references,
  including same-owner-fence config reattach and restart recovery.
- Supported 1.0.8 callback hooks with redacted audit projections; managed-aware,
  owner-fenced permission handling; JSON-Schema elicitation; MCP OAuth; and exactly-once
  protocol response implementations. Sampling, limits, and dynamic-header responses stay
  capability-gated until a real request/response/completion fixture succeeds.
- Durable mode/model per-field reconciliation (including gated reasoning-summary
  readback), MCP/extension health, and stale-aware usage/context projections.
- Background task `refresh/list` reconciliation, disappearance-to-unknown handling,
  usage/status rendering, and lossless attachment delivery for tool output at or above
  8000 characters; oversized Discord uploads are split into ordered lossless parts.
- Capability-backed registration for core `/session`, `/project`, `/model`,
  `/autopilot`, `/plan`, `/steer`, `/context`, `/usage`, and `/queue` commands.
- Default always-on definitions for macOS LaunchAgents and Windows Scheduled Tasks,
  plus a protected-work-aware watchdog, shared task-failure supervision, and a
  non-destructive active-execution SUSPECT monitor.

Native-gated commands such as Fleet, Tasks, quick ask, runtime schedules, and remote
sessions are intentionally not registered until their pinned-runtime fixtures are
implemented. The verified sidecar does not retain sessions after client transport
disconnect, so the current topology is bundled-runtime rather than detached execution.

## Setup

Python 3.11 or newer, an explicit GitHub token, and a Discord token are required. A
logged-in CLI identity alone is intentionally insufficient because managed organization
policy cannot be resolved for each session without a session credential.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
export COPILOTD_DISCORD_TOKEN='...'
export COPILOTD_GITHUB_TOKEN='...'
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

Project extension configuration is loaded from the immutable project snapshot at
`.copilotd/extensions.json`. The JSON accepts the typed custom-agent, skill directory,
disabled-skill, plugin directory, MCP server, and environment-reference fields documented
by the SDK adapter; store environment variable names, never secret values. New sessions
ingest the file automatically. Inside an idle session thread, `/project config-reload`
publishes a new generation and performs a fenced same-session reattach.

SDK 1.0.8 does not invoke `on_user_prompt_transformed` or `on_agent_stop`; copilotD does
not register those silent callbacks. Durable `session.idle` events provide the supported
agent-loop observation instead.

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

The full protocol/extension acceptance uses an explicit token with auto-login disabled,
disposable local stdio and HTTP MCP servers, sanitizes its evidence, and removes every
temporary session:

```bash
.venv/bin/copilotd sdk-probe --live-extensions
```

Runtime data, cache, and logs use platform-specific user directories. Override them
with `COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and `COPILOTD_LOG_DIR`. A guild-scoped
development command sync can be selected with `COPILOTD_DISCORD_GUILD_ID`.
