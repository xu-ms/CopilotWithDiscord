# copilotD

`copilotD` is an always-on, single-user Discord bridge for the official GitHub
Copilot Python SDK. It keeps one durable Copilot session per Discord thread,
uses the operator's resolved `$HOME` when a channel is not explicitly bound to
a project, and runs the Copilot runtime with `--yolo`.

The implementation follows [`docs/copilotD-detailed-design.md`](docs/copilotD-detailed-design.md).

## Implemented

- Exact SDK/runtime/protocol startup assertion (`1.0.8` / `1.0.73` / `3`) backed
  by persisted, hash-checked capability evidence rather than generated method presence.
- Bundled Copilot runtime launched with `--yolo`; the locally logged-in Copilot CLI
  account is valid authentication. An explicit GitHub token is optional and enables
  managed-settings session options when present.
- One long-lived SDK session per Discord thread, eager resume, owner fencing, and
  conservative unknown outcomes across crash windows.
- Explicit channel project bindings with immutable cwd snapshots; unbound channels
  always resolve to the service user's `$HOME`.
- Durable SQLite event journal with strict UUID SDK IDs, app FIFO, reducer-owned
  operation receipts, liveness leases, epoch/watermark snapshots, render outbox,
  and attachment manifests.
- Forty-four applied migrations use unique reserved namespaces: Foundation
  `0001`-`0009`, Native `0010`-`0014`, Protocol `0015`-`0019`, Scheduler
  `0020`-`0028`, Protocol compatibility `0029`, and Discord `0030`-`0037`;
  `0038`-`0039` are reserved, Operations uses forward-only `0040`-`0044`,
  session deletion and attachment lifecycle recovery are `0045`-`0046`, and
  background-liveness compatibility is `0047`.
- Durable event-log backfill with cursor rebase/gap diagnostics and ingress-overflow
  freeze/backfill/generation replacement; unrecoverable ephemeral gaps remain
  explicitly outcome-unknown.
- Restart-recoverable input attachment preparation, ready-orphan resubmission,
  reference-aware release, and seven-day retained-file garbage collection.
- Streaming replies, file/image attachments, table-aware Markdown rendering, and one
  in-thread, embed-backed TaskDeck for tool/subagent activity with select,
  expand/collapse, and pagination controls.
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
- Capability-backed registration for core commands plus exact native `/ask`,
  `/session compact`, `/fleet`, `/tasks`, `/agent`, `/after`, `/every`, `/remote`,
  `/review`, `/security-review`, `/research`, and `/rubber-duck` surfaces. Each native
  root or action remains absent until its exact runtime method or builtin invocation is
  verified.
- Runtime command-manifest refresh through `commands.list(include_builtins=true)` and
  `commands.changed`, full typed `commands.invoke` result-union handling, fenced
  idempotent task/agent/remote transitions, and reducer-owned native schedule/TaskDeck
  projections.
- Resume reconciles crash-pending agent transitions against current state, conservatively
  forces uncertain remote exposure off, and blocks all ordinary submissions while a
  compaction outcome remains unknown. Every SDK RPC revalidates owner generation, fence,
  and lease headroom before its result can be confirmed.
- Capability-backed registration for core `/session`, `/project`, `/model`,
  `/autopilot`, `/plan`, `/steer`, `/context`, `/usage`, and `/queue` commands.
- Application-owned `/schedule` message/new-session jobs with durable leases, DST-safe
  IANA time handling, FIFO coupling, crash recovery, semantic completion evidence, and
  fatal-loop reporting through the shared task supervisor.
- Intent-first `/project worktree` lifecycle with exact Git ownership checks, durable
  compensation/recovery, reference blockers, and capability-gated history forks.
- Default always-on definitions for macOS LaunchAgents and Windows Scheduled Tasks,
  effective-definition/PID verification, protected-work-aware restart coordination,
  sleep/resume suppression, durable restart-storm alerts, shared task-failure
  supervision, and a non-destructive active-execution SUSPECT monitor.

Unsupported native capabilities fail closed and remain unregistered. The verified
sidecar does not retain sessions after client transport disconnect, so the current
topology is bundled-runtime rather than detached execution.

## Setup

Python 3.11 or newer, a locally logged-in Copilot CLI account, and a Discord token are
required. `COPILOTD_GITHUB_TOKEN` is optional. When present it is passed only through
the protected credential source and enables managed-settings options; when absent,
sessions use normal logged-in runtime authentication.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
export COPILOTD_DISCORD_TOKEN='...'
# Optional: export COPILOTD_GITHUB_TOKEN='...'
.venv/bin/copilotd setup
```

`setup` first verifies the Discord token with Discord, starts the pinned Copilot runtime
to validate authentication/version/model access using either the existing local Copilot
CLI login or an optional `COPILOTD_GITHUB_TOKEN`, checks timezone data and private
directories, then installs and starts the current platform definitions. It succeeds only
after a new heartbeat reports `gateway_state=ready`, `runtime_state=ready`, and a PID that
matches the effective OS-managed process. Configured credentials are stored in a private
per-user service secret file (mode `0600` on macOS; current-user ACL on Windows) and are
never embedded in a plist, Task XML, PowerShell script, log, or acceptance artifact.
A runtime-reported managed policy/request fails deterministically with
`UserNotAvailable` and never creates a Discord permission UI; ordinary typed requests
remain owner-fenced `ApproveOnce`.

copilotD is a private single-user deployment. It has no guild/user/role allowlist and no
owner/admin command distinction. Project variables are resolved only through typed
future-session MCP/environment references; they are never copied wholesale into the
Copilot runtime process environment. For local development, use the explicit foreground
entrypoint:

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

Project extension configuration is loaded from the immutable project snapshot at
`.copilotd/extensions.json`. The JSON accepts the typed custom-agent, skill directory,
disabled-skill, plugin directory, MCP server, and environment-reference fields documented
by the SDK adapter; store environment variable names, never secret values. New sessions
ingest the file automatically. Inside an idle session thread, `/project config-reload`
publishes a new generation and performs a fenced same-session reattach.

SDK 1.0.8 does not invoke `on_user_prompt_transformed` or `on_agent_stop`; copilotD does
not register those silent callbacks. Durable `session.idle` events provide the supported
agent-loop observation instead.

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

Deterministic tests consume the bundled hash-checked fixture. Live acceptance is
separate. The broad SDK probe creates a disposable persistent session:

```bash
.venv/bin/copilotd sdk-probe --live
```

The complete native acceptance additionally executes supported RPC mutations and
builtins in a disposable Git repository, cleans up schedules/remote exposure/session
state, performs a real alternate-model set/readback/restore cycle, and writes sanitized
JSON evidence. It requires both the CLI flag and an exact environment confirmation:

```bash
export COPILOTD_REAL_ACCEPTANCE='I_UNDERSTAND_THIS_USES_REAL_COPILOT'
.venv/bin/copilotd native-acceptance \
  --real --evidence "$HOME/copilotd-native-evidence.json"
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

The full protocol/extension acceptance uses an explicit token with auto-login disabled
for its isolated probe only, disposable local stdio and HTTP MCP servers, sanitizes its
evidence, and removes every temporary session:

```bash
.venv/bin/copilotd sdk-probe --live-extensions
```

Runtime paths are fixed by platform:

| Platform | State | Heartbeat | Logs |
|---|---|---|---|
| macOS | `~/Library/Application Support/copilotd/` | `~/Library/Caches/copilotd/heartbeat.json` | `~/Library/Logs/copilotd/` |
| Windows | `%LOCALAPPDATA%\copilotd\state\` | `%LOCALAPPDATA%\copilotd\cache\heartbeat.json` | `%LOCALAPPDATA%\copilotd\logs\` |

The production Discord gateway also holds a per-OS-user non-blocking lock at the
OS account's fixed cache location for its entire lifetime. It resolves the account home from
the POSIX account database or Windows process token and deliberately ignores `HOME`,
`XDG_CACHE_HOME`, `LOCALAPPDATA`, `COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and
`COPILOTD_LOG_DIR`. A second foreground or managed gateway fails
before connecting to Discord with
`another copilotD gateway is already running for this OS user`; it never kills or replaces
the first process. The empty/sentinel lock file contains no token, token hash, PID, or other
identity. POSIX uses `flock`; Windows uses `msvcrt.locking`, and process exit releases the
kernel lock automatically.

On first Windows upgrade, only `setup` or `service install` may adopt a legacy
`%LOCALAPPDATA%\copilotD\` state tree. The installer disables service triggers, proves the
old process trees exited, holds SQLite exclusion while staging a durable unknown-outcome
handoff, atomically swaps the tree, and verifies the reinstalled tasks. Other commands
refuse to create split state until that migration completes; only the expected OS-managed
bot/runtime carrying the journal-bound handoff token may start for install verification.

`copilotd.log` is rotating JSON (10 MiB with seven backups); `boot.log`,
`watchdog.log`, and `alerts.log` have distinct destinations. Override paths with
`COPILOTD_DATA_DIR`, `COPILOTD_CACHE_DIR`, and `COPILOTD_LOG_DIR`. A guild-scoped
development command sync can be selected with `COPILOTD_DISCORD_GUILD_ID`. Copilot
input and Plan interactions expire after 15 minutes by default; deployments can tune
that wait with `COPILOTD_INTERACTION_TIMEOUT_SECONDS` without imposing a session lifetime.

Authentication follows the SDK token precedence
`COPILOTD_GITHUB_TOKEN` > `COPILOT_GITHUB_TOKEN` > `GH_TOKEN` > `GITHUB_TOKEN`.
Any token found through that precedence is treated as an explicit token and enables
managed-settings session options; unset all four to force normal logged-in-user auth.

Operational bounds are deliberately finite at transport and queue boundaries, not at the
session lifetime:

| Boundary | Default |
|---|---|
| Owner lease / renewal / mutation headroom | 60 s / 15 s / 40 s; renewal starts before create/resume RPCs |
| Reducer ingress / reducer batch / command mailbox | 4096 events / 64 events / 1024 operations |
| Generic SDK operation / compact / abort evidence | 30 s / 360 s / 15 s |
| SDK graceful stop, then forced stop | 10 s + 10 s |
| Input attachment file / message / inline-or-frame | 25 MiB / 100 MiB / 7 MiB |
| Extension configuration file | 1 MiB |
| Durable event-log read page | 500 events |
| SQLite busy wait | 5 s |
| App scheduler | 1 s poll, at most 100 due runs per tick; pre-dispatch retries at 5 s, 30 s, 2 m, 10 m, and 30 m |

Input attachment preparation and eager session resume are intentionally conservative.
Attachment files within one manifest are processed serially to bound memory. Eager resume
retries owner conflicts through the prior 60-second lease window and is cancelled during
shutdown. The app scheduler currently dispatches claimed runs serially; per-session durable
fences preserve correctness, but one slow target can delay unrelated due runs.
Runtime command discovery requests builtins only (`include_client_commands=false`,
`include_skills=false`); configured skills remain available to the Agent rather than being
mirrored as Discord commands. Model option input is limited to runtime-confirmable values:
context tier `default|long_context` and reasoning summary `none|concise|detailed`.

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
