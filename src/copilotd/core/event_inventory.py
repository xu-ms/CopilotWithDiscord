from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from copilot.session_events import SessionEventType

from copilotd.sdk.capabilities import MAIN_BRANCH_ONLY_EVENTS

StateDisposition = Literal["audit", "fallback", "journal", "reconcile", "reduce"]
RenderDisposition = Literal["content", "gated", "none", "status"]
LivenessDisposition = Literal["correlate", "diagnostic", "interaction", "none", "snapshot"]


@dataclass(frozen=True, slots=True)
class EventDisposition:
    state: StateDisposition
    render: RenderDisposition
    liveness: LivenessDisposition
    rationale: str


_SESSION_STATE_RENDER = (
    "session.start",
    "session.resume",
    "session.error",
    "session.idle",
    "session.shutdown",
    "session.title_changed",
    "session.context_changed",
    "session.usage_info",
    "session.usage_checkpoint",
    "session.session_limits_changed",
    "session.compaction_start",
    "session.compaction_complete",
    "session.task_complete",
    "session.info",
    "session.warning",
    "session.model_change",
    "session.mode_changed",
    "session.permissions_changed",
    "session.truncation",
    "session.snapshot_rewind",
    "session.plan_changed",
    "session.todos_changed",
    "session.workspace_file_changed",
    "session.handoff",
    "session.remote_steerable_changed",
    "session.autopilot_objective_changed",
    "session.schedule_created",
    "session.schedule_cancelled",
    "session.schedule_rearmed",
    "session.managed_settings_resolved",
    "session.managed_settings_enforced",
    "session.auto_mode_resolved",
    "session.custom_notification",
    "session.binary_asset",
)

_CONTENT_REDUCE_RENDER = (
    "user.message",
    "assistant.turn_start",
    "assistant.turn_end",
    "assistant.turn_retry",
    "assistant.intent",
    "assistant.reasoning_delta",
    "assistant.reasoning",
    "assistant.message_start",
    "assistant.message_delta",
    "assistant.message",
    "assistant.server_tool_progress",
    "assistant.usage",
    "abort",
    "tool.user_requested",
    "tool.execution_start",
    "tool.execution_partial_result",
    "tool.execution_progress",
    "tool.execution_complete",
    "skill.invoked",
    "subagent.selected",
    "subagent.deselected",
    "subagent.started",
    "subagent.completed",
    "subagent.failed",
    "system.notification",
    "model.call_failure",
    "mcp_app.tool_call_complete",
)

_RECONCILE_TRIGGERS = (
    "pending_messages.modified",
    "commands.changed",
    "capabilities.changed",
    "session.tools_updated",
    "session.skills_loaded",
    "session.custom_agents_updated",
    "session.mcp_servers_loaded",
    "session.mcp_server_status_changed",
    "mcp.tools.list_changed",
    "mcp.resources.list_changed",
    "mcp.prompts.list_changed",
    "session.background_tasks_changed",
    "session.extensions_loaded",
    "session.extensions.attachments_pushed",
)

_PROTOCOL_JOURNAL = (
    "permission.requested",
    "permission.completed",
    "user_input.requested",
    "user_input.completed",
    "elicitation.requested",
    "elicitation.completed",
    "exit_plan_mode.requested",
    "exit_plan_mode.completed",
    "session_limits_exhausted.requested",
    "session_limits_exhausted.completed",
    "sampling.requested",
    "sampling.completed",
    "mcp.oauth_required",
    "mcp.oauth_completed",
    "mcp.headers_refresh_required",
    "mcp.headers_refresh_completed",
    "external_tool.requested",
    "external_tool.completed",
    "auto_mode_switch.requested",
    "auto_mode_switch.completed",
)

_AUDIT_ONLY = (
    "assistant.idle",
    "assistant.streaming_delta",
    "assistant.tool_call_delta",
    "model.call_start",
    "tool_search.activated",
    "hook.start",
    "hook.progress",
    "hook.end",
    "system.message",
    "command.queued",
    "command.execute",
    "command.completed",
    "session.canvas.opened",
    "session.canvas.registry_changed",
    "session.canvas.closed",
    "session.canvas.unavailable",
    "session.canvas.recorded",
    "session.canvas.removed",
)


def _build_inventory() -> dict[str, EventDisposition]:
    inventory: dict[str, EventDisposition] = {}

    def register(raw_types: tuple[str, ...], disposition: EventDisposition) -> None:
        for raw_type in raw_types:
            if raw_type in inventory:
                raise RuntimeError(f"duplicate event disposition: {raw_type}")
            inventory[raw_type] = disposition

    register(
        _SESSION_STATE_RENDER,
        EventDisposition(
            state="reduce",
            render="status",
            liveness="correlate",
            rationale="session state or durable status projection",
        ),
    )
    register(
        _CONTENT_REDUCE_RENDER,
        EventDisposition(
            state="reduce",
            render="content",
            liveness="correlate",
            rationale="submission, turn, content, tool, or worker projection",
        ),
    )
    register(
        _RECONCILE_TRIGGERS,
        EventDisposition(
            state="reconcile",
            render="none",
            liveness="snapshot",
            rationale="change notification requires a fenced snapshot",
        ),
    )
    register(
        _PROTOCOL_JOURNAL,
        EventDisposition(
            state="journal",
            render="gated",
            liveness="interaction",
            rationale="wire protocol lifecycle; response plane is handled separately",
        ),
    )
    register(
        _AUDIT_ONLY,
        EventDisposition(
            state="audit",
            render="none",
            liveness="none",
            rationale="diagnostic or transport telemetry with no terminal authority",
        ),
    )
    register(
        ("unknown",),
        EventDisposition(
            state="fallback",
            render="none",
            liveness="diagnostic",
            rationale="unknown raw event is retained without inferred semantics",
        ),
    )

    generated = {event.value for event in SessionEventType}
    actual = set(inventory)
    if actual != generated:
        missing = sorted(generated - actual)
        extra = sorted(actual - generated)
        raise RuntimeError(
            f"generated event disposition mismatch; missing={missing}, extra={extra}"
        )
    if generated.intersection(MAIN_BRANCH_ONLY_EVENTS):
        raise RuntimeError("main-branch-only events cannot be attributed to SDK 1.0.8")
    return inventory


EVENT_DISPOSITIONS = _build_inventory()
MAIN_BRANCH_ONLY_DISPOSITIONS = {
    "factory.run_updated": EventDisposition(
        state="audit",
        render="none",
        liveness="diagnostic",
        rationale="main-branch-only experimental factory telemetry",
    ),
    "session.context_cleared": EventDisposition(
        state="reduce",
        render="status",
        liveness="correlate",
        rationale="main-branch-only context mutation audit",
    ),
}


def disposition_for(raw_type: str) -> EventDisposition:
    return EVENT_DISPOSITIONS.get(raw_type, EVENT_DISPOSITIONS["unknown"])
