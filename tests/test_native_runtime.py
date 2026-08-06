import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from copilot.session_events import (
    CommandsChangedCommand,
    CommandsChangedData,
    SessionCompactionCompleteData,
    SessionCompactionStartData,
    SessionEvent,
    SessionEventType,
    SessionRemoteSteerableChangedData,
    SessionScheduleCancelledData,
    SessionScheduleCreatedData,
    SessionScheduleRearmedData,
    SubagentDeselectedData,
    SubagentSelectedData,
)

from copilotd.config import Settings
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.event_adapter import InvalidSdkEvent
from copilotd.core.mailbox import OperationAmbiguous
from copilotd.core.models import AdaptedEvent, InboxEnvelope
from copilotd.core.native import NativeCapabilityError, NativeTaskAction
from copilotd.core.reducer import JournalReducer
from copilotd.core.session_runtime import (
    DetachBlocked,
    SessionAttachUnknown,
    SessionNotReady,
    SessionRuntime,
)
from copilotd.sdk.bridge import EventLogBatch
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.sdk.native import (
    NativeCommandDefinition,
    NativeCommandResult,
    NativeCommandResultKind,
    NativeCommandSelection,
)
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


class NativeHandle:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.history: list[Any] = []
        self.disconnect_calls = 0

    async def send(self, prompt: str, **kwargs: Any) -> str:
        self.sent.append((prompt, kwargs))
        return str(uuid4())

    async def abort(self) -> None:
        return None

    async def disconnect(self) -> None:
        self.disconnect_calls += 1

    async def get_events(self) -> list[Any]:
        return list(self.history)


class RuntimeScheduleMessageData:
    def __init__(self, schedule_id: int) -> None:
        self.schedule_id = schedule_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "content": "scheduled root",
            "runtimeScheduleId": self.schedule_id,
        }


class NativeBridge:
    def __init__(self, session_id: str) -> None:
        self.handle = NativeHandle(session_id)
        self.ingress: Any = None
        self.mode = "interactive"
        self.model = {
            "modelId": "gpt-test",
            "reasoningEffort": None,
            "contextTier": None,
        }
        self.commands = [
            _command(name)
            for name in (
                "after",
                "every",
                "research",
                "review",
                "rubber-duck",
                "security-review",
            )
        ]
        self.command_results: dict[str, NativeCommandResult] = {}
        self.invocations: list[tuple[str, str | None]] = []
        self.command_list_hook: Any = None
        self.ephemeral_answer = "side answer"
        self.compactions = 0
        self.compact_error: Exception | None = None
        self.fleet_starts = 0
        self.fleet_hook: Any = None
        self.tasks: list[dict[str, Any]] = []
        self.task_messages: list[tuple[str, str]] = []
        self.task_cancels: list[str] = []
        self.task_cancel_rejections: set[str] = set()
        self.task_promotions: list[str] = []
        self.task_removals: list[str] = []
        self.task_waits = 0
        self.agents = [
            {
                "id": "agent-reviewer",
                "name": "reviewer",
                "displayName": "Reviewer",
                "description": "Reviews changes",
                "source": "builtin",
                "userInvocable": True,
            }
        ]
        self.current_agent_name = "default"
        self.schedules: list[dict[str, Any]] = []
        self.next_schedule_id = 1
        self.schedule_stop_override: dict[str, Any] | None = None
        self.remote_mode = "off"
        self.remote_url: str | None = None
        self.authenticated = True
        self.event_log_reads = 0
        self.event_log_error: Exception | None = None
        self.attach_hook: Any = None

    async def create_session(self, **kwargs: Any) -> NativeHandle:
        self.ingress = kwargs["on_event"]
        if self.attach_hook is not None:
            await self.attach_hook()
        return self.handle

    async def resume_session(self, session_id: str, **kwargs: Any) -> NativeHandle:
        assert session_id == self.handle.session_id
        self.ingress = kwargs["on_event"]
        return self.handle

    async def ensure_allow_all(self, _session: NativeHandle) -> object:
        return object()

    async def get_mode(self, _session: NativeHandle) -> str:
        return self.mode

    async def set_mode(self, _session: NativeHandle, mode: str) -> None:
        self.mode = mode

    async def list_models(self) -> list[dict[str, Any]]:
        return [{"id": "gpt-test", "name": "GPT Test"}]

    async def set_model(
        self,
        _session: NativeHandle,
        *,
        model: str,
        reasoning_effort: str | None,
        context_tier: str | None,
    ) -> None:
        self.model = {
            "modelId": model,
            "reasoningEffort": reasoning_effort,
            "contextTier": context_tier,
        }

    async def get_current_model(self, _session: NativeHandle) -> dict[str, Any]:
        return dict(self.model)

    async def get_context(self, _session: NativeHandle) -> dict[str, Any]:
        return {"totalTokens": 50 - self.compactions * 20, "limit": 100}

    async def get_usage(self, _session: NativeHandle) -> dict[str, Any]:
        return {}

    async def get_mcp_servers(self, _session: NativeHandle) -> dict[str, Any]:
        return {"servers": []}

    async def get_skills(self, _session: NativeHandle) -> dict[str, Any]:
        return {"skills": []}

    async def get_agents(self, _session: NativeHandle) -> dict[str, Any]:
        return {"agents": []}

    async def get_readiness(self, _session: NativeHandle) -> dict[str, Any]:
        return {
            "processing": False,
            "hasActiveWork": False,
            "abortable": False,
            "pendingItems": [],
            "steeringMessages": [],
        }

    async def get_tasks(self, _session: NativeHandle) -> list[dict[str, Any]]:
        return [dict(task) for task in self.tasks]

    async def refresh_tasks(self, _session: NativeHandle) -> None:
        return None

    async def list_tasks(self, _session: NativeHandle) -> list[dict[str, Any]]:
        return [dict(task) for task in self.tasks]

    async def get_task_progress(
        self,
        _session: NativeHandle,
        task_id: str,
    ) -> dict[str, Any]:
        return {"type": "agent", "latestIntent": f"progress:{task_id}"}

    async def send_task_message(
        self,
        _session: NativeHandle,
        task_id: str,
        message: str,
    ) -> dict[str, Any]:
        self.task_messages.append((task_id, message))
        return {"sent": True}

    async def get_current_promotable_task(
        self,
        _session: NativeHandle,
    ) -> dict[str, Any] | None:
        return next(
            (dict(task) for task in self.tasks if task["status"] == "running"),
            None,
        )

    async def promote_task(self, _session: NativeHandle, task_id: str) -> bool:
        self.task_promotions.append(task_id)
        return True

    async def cancel_task(self, _session: NativeHandle, task_id: str) -> bool:
        self.task_cancels.append(task_id)
        if task_id in self.task_cancel_rejections:
            return False
        for task in self.tasks:
            if task["id"] == task_id:
                task["status"] = "cancelled"
                return True
        return False

    async def remove_task(self, _session: NativeHandle, task_id: str) -> bool:
        self.task_removals.append(task_id)
        before = len(self.tasks)
        self.tasks = [task for task in self.tasks if task["id"] != task_id]
        return len(self.tasks) != before

    async def wait_for_tasks(
        self,
        _session: NativeHandle,
        *,
        wait_seconds: float,
    ) -> None:
        assert wait_seconds > 0
        self.task_waits += 1

    async def check_session_in_use(self, _session_id: str) -> bool:
        return False

    async def get_native_schedules(
        self,
        _session: NativeHandle,
    ) -> list[dict[str, Any]]:
        return [dict(schedule) for schedule in self.schedules]

    async def stop_native_schedule(
        self,
        _session: NativeHandle,
        schedule_id: int,
    ) -> dict[str, Any] | None:
        if self.schedule_stop_override is not None:
            return dict(self.schedule_stop_override)
        schedule = next(
            (item for item in self.schedules if item["id"] == schedule_id),
            None,
        )
        if schedule is not None:
            self.schedules.remove(schedule)
            return dict(schedule)
        return None

    async def get_remote_state(self, _session: NativeHandle) -> dict[str, Any]:
        return {
            "is_remote_session": False,
            "metadata": {"sessionId": self.handle.session_id},
        }

    async def get_session_auth(self, _session: NativeHandle) -> dict[str, Any]:
        return {
            "isAuthenticated": self.authenticated,
            "authType": "github",
            "host": "github.com",
        }

    async def enable_remote(
        self,
        _session: NativeHandle,
        mode: str,
    ) -> dict[str, Any]:
        self.remote_mode = mode
        self.remote_url = "https://example.invalid/remote"
        return {
            "remoteSteerable": mode == "on",
            "url": self.remote_url,
        }

    async def disable_remote(self, _session: NativeHandle) -> None:
        self.remote_mode = "off"
        self.remote_url = None

    async def get_current_agent(self, _session: NativeHandle) -> str:
        return self.current_agent_name

    async def list_agents(self, _session: NativeHandle) -> list[dict[str, Any]]:
        return [dict(agent) for agent in self.agents]

    async def get_current_agent_info(
        self,
        _session: NativeHandle,
    ) -> dict[str, Any] | None:
        current = next(
            (dict(agent) for agent in self.agents if agent["name"] == self.current_agent_name),
            None,
        )
        if current is not None or self.current_agent_name == "default":
            return current
        return {
            "id": f"agent-{self.current_agent_name}",
            "name": self.current_agent_name,
            "displayName": self.current_agent_name,
            "description": "external agent",
        }

    async def select_agent(
        self,
        _session: NativeHandle,
        name: str,
    ) -> dict[str, Any]:
        self.current_agent_name = name
        return next(dict(agent) for agent in self.agents if agent["name"] == name)

    async def deselect_agent(self, _session: NativeHandle) -> None:
        self.current_agent_name = "default"

    async def list_commands(
        self,
        _session: NativeHandle,
        *,
        include_builtins: bool,
    ) -> tuple[NativeCommandDefinition, ...]:
        assert include_builtins
        snapshot = tuple(self.commands)
        if self.command_list_hook is not None:
            hook = self.command_list_hook
            self.command_list_hook = None
            await hook()
        return snapshot

    async def invoke_command(
        self,
        _session: NativeHandle,
        *,
        name: str,
        input_text: str | None,
    ) -> NativeCommandResult:
        self.invocations.append((name, input_text))
        if name in {"after", "every"}:
            self.schedules.append(
                {
                    "id": self.next_schedule_id,
                    "nextRunAt": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "prompt": input_text or "",
                    "displayPrompt": input_text or "",
                    "recurring": name == "every",
                    "intervalMs": 3_600_000 if name == "every" else None,
                }
            )
            self.next_schedule_id += 1
            return NativeCommandResult(
                kind=NativeCommandResultKind.COMPLETED,
                runtime_settings_changed=False,
                message="scheduled",
            )
        return self.command_results[name]

    async def ephemeral_query(
        self,
        _session: NativeHandle,
        _question: str,
    ) -> str:
        return self.ephemeral_answer

    async def compact_history(
        self,
        _session: NativeHandle,
        *,
        focus: str | None,
    ) -> dict[str, Any]:
        if self.compact_error is not None:
            raise self.compact_error
        self.compactions += 1
        return {
            "success": True,
            "messagesRemoved": 2,
            "tokensRemoved": 20,
            "summaryContent": focus,
        }

    async def start_fleet(
        self,
        _session: NativeHandle,
        _prompt: str,
        *,
        timeout_seconds: float = 120,
    ) -> bool:
        assert timeout_seconds > 0
        if self.fleet_hook is not None:
            await self.fleet_hook()
        self.fleet_starts += 1
        return True

    async def tail_event_log(self, _session: NativeHandle) -> str:
        return "tail"

    async def read_event_log(
        self,
        _session: NativeHandle,
        *,
        cursor: str | None,
        max_events: int = 500,
        wait_ms: int = 0,
        include_ephemeral: bool = False,
    ) -> EventLogBatch:
        del cursor, max_events, wait_ms
        assert not include_ephemeral
        if self.event_log_error is not None:
            raise self.event_log_error
        self.event_log_reads += 1
        return EventLogBatch(
            cursor=f"cursor-{self.event_log_reads}",
            cursor_status="ok",
            events=(),
            has_more=False,
            filtered_ephemeral=0,
        )


@asynccontextmanager
async def native_runtime(
    tmp_path: Path,
    *,
    capabilities: set[str],
    git_origin: bool = False,
    unsupported: set[str] | None = None,
) -> AsyncIterator[tuple[SessionRuntime, NativeBridge, Database]]:
    if git_origin:
        await _git(tmp_path, "init", "--quiet")
        await _git(
            tmp_path,
            "remote",
            "add",
            "origin",
            "https://github.com/octocat/Hello-World.git",
        )
    session_id = str(uuid4())
    database = Database(tmp_path / "native.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="native-thread",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    evidence = dict(manifest.capabilities)
    for name in capabilities:
        evidence[name] = replace(evidence[name], supported=True)
    for name in unsupported or set():
        evidence[name] = replace(evidence[name], supported=False)
    bridge = NativeBridge(session_id)
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="native-test-owner",
        binding=binding,
        capabilities=replace(manifest, capabilities=evidence),
    )
    await runtime.attach_create()
    try:
        yield runtime, bridge, database
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=5)
        await database.close()


@pytest.mark.asyncio
async def test_command_manifest_refresh_and_agent_prompt_use_runtime_prompt(
    tmp_path: Path,
) -> None:
    supported = {
        "builtin_review",
        "commands_invoke",
        "commands_list",
    }
    async with native_runtime(tmp_path, capabilities=supported) as (
        runtime,
        bridge,
        database,
    ):
        bridge.command_results["review"] = NativeCommandResult(
            kind=NativeCommandResultKind.AGENT_PROMPT,
            runtime_settings_changed=False,
            display_prompt="Review changes",
            prompt="RUNTIME GENERATED REVIEW PROMPT",
        )

        result = await runtime.invoke_native_command(
            "review",
            "user instructions",
            idempotency_key="review-1",
        )

        assert result["kind"] == "agent-prompt"
        assert bridge.invocations == [("review", "user instructions")]
        assert bridge.handle.sent[0][0] == "RUNTIME GENERATED REVIEW PROMPT"
        assert bridge.handle.sent[0][0] != "user instructions"
        invocation = await database.fetchone(
            """
            SELECT result_kind, state, agent_submission_id
            FROM runtime_command_invocations WHERE command_name = 'review'
            """
        )
        assert dict(invocation) == {
            "result_kind": "agent-prompt",
            "state": "confirmed",
            "agent_submission_id": result["agent_submission_id"],
        }


@pytest.mark.asyncio
async def test_command_select_subcommand_is_fenced_by_runtime_options(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={
            "builtin_research",
            "commands_invoke",
            "commands_list",
            "commands_result_select_subcommand",
            "commands_result_text",
        },
    ) as (runtime, bridge, _database):
        bridge.command_results["research"] = NativeCommandResult(
            kind=NativeCommandResultKind.SELECT_SUBCOMMAND,
            runtime_settings_changed=False,
            command="research",
            title="Choose scope",
            options=(
                NativeCommandSelection(
                    name="repo",
                    description="Current repository",
                    group=None,
                ),
            ),
        )
        first = await runtime.invoke_native_command(
            "research",
            "topic",
            idempotency_key="research-1",
        )
        bridge.command_results["research"] = NativeCommandResult(
            kind=NativeCommandResultKind.TEXT,
            runtime_settings_changed=False,
            text="selected",
        )

        with pytest.raises(ValueError, match="runtime-provided"):
            await runtime.continue_native_command(
                first["selection_token"],
                "invalid",
                idempotency_key="research-invalid",
            )
        selected = await runtime.continue_native_command(
            first["selection_token"],
            "repo",
            idempotency_key="research-selected",
        )

        assert selected["kind"] == "text"
        assert bridge.invocations[-1] == ("research", "repo")


@pytest.mark.asyncio
async def test_ephemeral_query_preserves_history_and_records_only_hashes(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"ephemeral_query"},
    ) as (runtime, _bridge, database):
        answer = await runtime.ask_ephemeral(
            "What is one plus one?",
            idempotency_key="ask-1",
        )
        row = await database.fetchone("SELECT * FROM ephemeral_queries")
        operation = await database.fetchone(
            """
            SELECT result_ref FROM session_operations
            WHERE kind = 'ephemeral-query'
            """
        )
        journal = await database.fetchall(
            """
            SELECT raw_payload FROM event_journal
            WHERE raw_type LIKE 'copilotd.%ephemeral_query%'
               OR raw_type = 'copilotd.operation.transition'
            """
        )

        assert answer == "side answer"
        assert row["history_count_before"] == row["history_count_after"] == 0
        assert row["question_hash"] != "What is one plus one?"
        assert row["answer_hash"] != answer
        assert row["state"] == "confirmed"
        assert answer not in str(operation["result_ref"])
        assert all(answer not in str(event["raw_payload"]) for event in journal)
        with pytest.raises(NativeCapabilityError, match="not retained"):
            await runtime.ask_ephemeral(
                "What is one plus one?",
                idempotency_key="ask-1",
            )


@pytest.mark.asyncio
async def test_compact_fleet_and_task_surface_are_durable_and_idempotent(
    tmp_path: Path,
) -> None:
    supported = {
        "fleet_start",
        "history_compact",
        "tasks_cancel",
        "tasks_list",
        "tasks_message",
        "tasks_progress",
        "tasks_promote",
        "tasks_remove",
        "tasks_wait",
    }
    async with native_runtime(tmp_path, capabilities=supported) as (
        runtime,
        bridge,
        database,
    ):
        compact = await runtime.compact("retain decisions", idempotency_key="compact-1")
        repeated = await runtime.compact(
            "retain decisions",
            idempotency_key="compact-1",
        )
        fleet = await runtime.start_fleet("analyze", idempotency_key="fleet-1")
        bridge.tasks = [
            {
                "id": "task-agent",
                "type": "agent",
                "status": "running",
                "description": "Agent task",
            },
            {
                "id": "task-shell",
                "type": "shell",
                "status": "completed",
                "description": "Shell task",
            },
            {
                "id": "task-agent-2",
                "type": "agent",
                "status": "running",
                "description": "Second agent task",
            },
        ]

        listed = await runtime.task_action(
            NativeTaskAction.LIST,
            idempotency_key="task-list",
        )
        shown = await runtime.task_action(
            NativeTaskAction.SHOW,
            task_id="task-agent",
            idempotency_key="task-show",
        )
        progressed = await runtime.task_action(
            NativeTaskAction.PROGRESS,
            task_id="task-agent",
            idempotency_key="task-progress",
        )
        await runtime.task_action(
            NativeTaskAction.MESSAGE,
            task_id="task-agent",
            message="status please",
            idempotency_key="task-message",
        )
        await runtime.task_action(
            NativeTaskAction.MESSAGE,
            task_id="task-agent",
            message="status please",
            idempotency_key="task-message",
        )
        await runtime.task_action(
            NativeTaskAction.PROMOTE,
            task_id="task-agent",
            idempotency_key="task-promote",
        )
        await runtime.task_action(
            NativeTaskAction.CANCEL,
            task_id="task-agent",
            idempotency_key="task-cancel",
        )
        await runtime.task_action(
            NativeTaskAction.ALL,
            idempotency_key="task-cancel-all",
        )
        await runtime.task_action(
            NativeTaskAction.REMOVE,
            task_id="task-shell",
            idempotency_key="task-remove",
        )
        await runtime.task_action(
            NativeTaskAction.WAIT,
            idempotency_key="task-wait",
        )

        assert compact == repeated
        assert bridge.compactions == 1
        assert compact["result"]["success"]
        assert fleet["submission_id"]
        assert bridge.fleet_starts == 1
        assert listed["tasks"]
        assert shown["result"]["progress"]["latestIntent"] == "progress:task-agent"
        assert progressed["result"]["progress"]["latestIntent"] == "progress:task-agent"
        assert shown["taskdeck"]
        assert bridge.task_messages == [("task-agent", "status please")]
        assert bridge.task_promotions == ["task-agent"]
        assert bridge.task_cancels == ["task-agent", "task-agent-2"]
        assert bridge.task_removals == ["task-shell"]
        assert bridge.task_waits == 1
        assert await database.fetchone("SELECT 1 FROM compaction_runs WHERE state = 'confirmed'")
        assert await database.fetchone("SELECT 1 FROM fleet_runs WHERE state = 'confirmed'")


@pytest.mark.asyncio
async def test_agent_schedule_and_remote_transitions_reconcile_projections(
    tmp_path: Path,
) -> None:
    supported = {
        "agents_deselect",
        "agents_select",
        "builtin_after",
        "builtin_after_result_completed",
        "commands_invoke",
        "commands_result_completed",
        "remote_disable",
        "remote_enable",
        "remote_export_detach_safe",
        "remote_status",
        "schedules_list",
        "schedules_stop",
    }
    async with native_runtime(
        tmp_path,
        capabilities=supported,
        git_origin=True,
    ) as (runtime, bridge, database):
        bridge.agents[0]["mcpServers"] = {"headers": {"Authorization": "secret-token"}}
        bridge.agents[0]["path"] = "/private/agent.md"
        listing = await runtime.list_agents()
        assert "mcpServers" not in listing["agents"][0]["metadata"]
        assert "path" not in listing["agents"][0]["metadata"]
        stored_agent = await database.fetchone("SELECT metadata_json FROM runtime_agent_manifest")
        assert "secret-token" not in stored_agent["metadata_json"]
        assert "/private/agent.md" not in stored_agent["metadata_json"]
        assert (
            await runtime.select_agent(
                "reviewer",
                idempotency_key="agent-select",
            )
            == "reviewer"
        )
        assert (
            await runtime.deselect_agent(
                idempotency_key="agent-deselect",
            )
            == "default"
        )

        schedule = await runtime.create_runtime_schedule(
            "after",
            "1h",
            "check status",
            idempotency_key="after-create",
        )
        cancelled = await runtime.cancel_runtime_schedule(
            "after",
            str(schedule["runtime_schedule_id"]),
            idempotency_key="after-cancel",
        )

        on = await runtime.set_remote("on", idempotency_key="remote-on")
        with pytest.raises(ValueError, match="confirmed off"):
            await runtime.set_remote("export", idempotency_key="remote-export-invalid")
        off = await runtime.set_remote("off", idempotency_key="remote-off")
        exported = await runtime.set_remote(
            "export",
            idempotency_key="remote-export",
        )

        assert cancelled["state"] == "cancelled"
        assert on["mode"] == "on"
        assert off["mode"] == "off"
        assert exported["mode"] == "export"
        assert not exported["steerable"]
        assert bridge.remote_mode == "export"
        assert await database.fetchone(
            "SELECT 1 FROM runtime_agent_transitions WHERE state = 'confirmed'"
        )
        assert await database.fetchone(
            "SELECT 1 FROM runtime_remote_transitions WHERE state = 'confirmed'"
        )


@pytest.mark.asyncio
async def test_cancel_all_persists_partial_results_and_refreshes_tasks(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"tasks_cancel", "tasks_list"},
    ) as (runtime, bridge, database):
        bridge.tasks = [
            {
                "id": "cancelled-task",
                "type": "agent",
                "status": "running",
                "description": "Will cancel",
            },
            {
                "id": "rejected-task",
                "type": "agent",
                "status": "running",
                "description": "Will reject",
            },
        ]
        bridge.task_cancel_rejections.add("rejected-task")

        result = await runtime.task_action(
            NativeTaskAction.ALL,
            idempotency_key="partial-cancel-all",
        )

        assert result["result"] == {
            "cancelled": ["cancelled-task"],
            "rejected": ["rejected-task"],
            "partial": True,
        }
        action = await database.fetchone("SELECT state, result_json FROM runtime_task_actions")
        operation = await database.fetchone(
            "SELECT state FROM session_operations WHERE kind = 'task-all'"
        )
        assert action["state"] == "confirmed"
        assert json.loads(action["result_json"])["partial"]
        assert operation["state"] == "confirmed"


@pytest.mark.asyncio
async def test_schedule_stop_requires_matching_id_and_fresh_absence(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"schedules_list", "schedules_stop"},
    ) as (runtime, bridge, database):
        bridge.schedules = [
            {
                "id": 5,
                "nextRunAt": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "prompt": "scheduled",
                "recurring": False,
            }
        ]
        await runtime.runtime_schedules(kind="after")
        bridge.schedule_stop_override = {"id": 99}

        with pytest.raises(OperationAmbiguous):
            await runtime.cancel_runtime_schedule(
                "after",
                "5",
                idempotency_key="mismatched-stop",
            )

        schedule = await database.fetchone(
            """
            SELECT state FROM runtime_schedules
            WHERE runtime_schedule_id = '5'
            """
        )
        assert schedule["state"] != "cancelled"


@pytest.mark.asyncio
async def test_reducer_transaction_rejects_stale_fence_before_journaling(
    tmp_path: Path,
) -> None:
    async with native_runtime(tmp_path, capabilities=set()) as (
        runtime,
        _bridge,
        database,
    ):
        assert runtime.binding.owner_fence_token is not None
        event = AdaptedEvent(
            sdk_session_id=runtime.binding.sdk_session_id,
            generation=runtime.binding.runtime_generation,
            fence_token=runtime.binding.owner_fence_token + 1,
            inbox_seq=999,
            source="internal",
            raw_type="copilotd.stale",
            raw_payload={"type": "copilotd.stale", "data": {}},
            reducer_hash="stale",
            persistence_class="internal",
            received_at=datetime.now(UTC).timestamp(),
            internal_event_id="stale-fence-event",
        )

        reducer = JournalReducer(
            database,
            require_binding_fence=True,
        )
        inserted = await reducer.persist([event])
        await reducer.persist_incident(
            InboxEnvelope(
                sdk_session_id=runtime.binding.sdk_session_id,
                generation=runtime.binding.runtime_generation,
                fence_token=runtime.binding.owner_fence_token + 1,
                inbox_seq=1000,
                source="sdk",
                payload=object(),
                received_at=datetime.now(UTC).timestamp(),
            ),
            InvalidSdkEvent("invalid_event", "stale"),
        )

        assert inserted == 0
        assert (
            await database.fetchone(
                "SELECT 1 FROM event_journal WHERE internal_event_id = ?",
                ("stale-fence-event",),
            )
            is None
        )
        assert (
            await database.fetchone("SELECT 1 FROM runtime_incidents WHERE kind = 'invalid_event'")
            is None
        )


@pytest.mark.asyncio
async def test_commands_changed_refreshes_generation_and_removes_missing_builtin(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"commands_list"},
    ) as (runtime, bridge, database):
        initial = await runtime.refresh_native_commands()
        assert {item["command_name"] for item in initial} >= {"review", "research"}
        bridge.commands = [_command("review")]
        bridge.ingress(
            SessionEvent(
                data=CommandsChangedData(commands=[CommandsChangedCommand(name="review")]),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.COMMANDS_CHANGED,
            )
        )
        await asyncio.sleep(0.05)
        await runtime.inbox.join()
        await asyncio.sleep(0.05)
        await runtime.inbox.join()

        rows = await database.fetchall(
            """
            SELECT command_name, state, manifest_generation
            FROM runtime_command_manifest ORDER BY command_name
            """
        )
        states = {row["command_name"]: row["state"] for row in rows}
        assert states["review"] == "available"
        assert states["research"] == "unavailable"
        assert max(int(row["manifest_generation"]) for row in rows) >= 2


@pytest.mark.asyncio
async def test_crossing_positive_command_and_agent_snapshots_cannot_resurrect_state(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"agents_current", "agents_list", "commands_list"},
    ) as (runtime, bridge, database):
        bridge.commands = []
        await runtime.refresh_native_commands()
        command_epoch = await runtime._request_snapshot("commands")
        command_start = runtime.inbox.last_sdk_receive_seq
        bridge.ingress(
            SessionEvent(
                data=CommandsChangedData(commands=[]),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.COMMANDS_CHANGED,
            )
        )
        await runtime.inbox.join()
        await runtime._commit_snapshot(
            "commands",
            command_epoch,
            command_start,
            command_start,
            {
                "commands": [_command("review").to_dict()],
                "manifest_generation": command_epoch,
            },
        )
        await runtime.inbox.join()
        review = await database.fetchone(
            """
            SELECT state FROM runtime_command_manifest
            WHERE command_name = 'review'
            """
        )
        assert review is None or review["state"] == "unavailable"

        bridge.agents = []
        bridge.current_agent_name = "default"
        await runtime._query_snapshot_topic("agents")
        agent_epoch = await runtime._request_snapshot("agents")
        agent_start = runtime.inbox.last_sdk_receive_seq
        bridge.current_agent_name = "external-agent"
        bridge.ingress(
            SessionEvent(
                data=SubagentSelectedData(
                    agent_display_name="External",
                    agent_name="external-agent",
                    tools=[],
                ),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SUBAGENT_SELECTED,
            )
        )
        await runtime.inbox.join()
        await runtime._commit_snapshot(
            "agents",
            agent_epoch,
            agent_start,
            agent_start,
            {
                "agents": [
                    {
                        "id": "agent-reviewer",
                        "name": "reviewer",
                        "displayName": "Reviewer",
                        "description": "stale",
                    }
                ],
                "current": {
                    "id": "agent-reviewer",
                    "name": "reviewer",
                    "displayName": "Reviewer",
                },
                "manifest_generation": agent_epoch,
            },
        )
        await runtime.inbox.join()
        binding = await database.fetchone("SELECT runtime_agent FROM session_bindings")
        reviewer = await database.fetchone(
            """
            SELECT state FROM runtime_agent_manifest
            WHERE agent_name = 'reviewer'
            """
        )
        assert binding["runtime_agent"] == "external-agent"
        assert reviewer["state"] == "unavailable"
        stale_topics = await database.fetchall(
            """
            SELECT topic, requested_epoch, applied_epoch, status
            FROM reconciliation_state
            WHERE topic IN ('agents', 'commands')
            """
        )
        requested = {str(row["topic"]): int(row["requested_epoch"]) for row in stale_topics}
        assert requested["commands"] > command_epoch
        assert requested["agents"] > agent_epoch


@pytest.mark.asyncio
async def test_initial_crossing_command_snapshot_requeries_before_ready(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "initial-command-crossing.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-initial-command-crossing",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    bridge = NativeBridge(session_id)

    async def remove_commands_during_first_list() -> None:
        bridge.commands = []
        bridge.ingress(
            SessionEvent(
                data=CommandsChangedData(commands=[]),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.COMMANDS_CHANGED,
            )
        )

    bridge.command_list_hook = remove_commands_during_first_list
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="initial-command-crossing",
        binding=binding,
        capabilities=CapabilityRegistry(
            Settings(_env_file=None, data_dir=tmp_path / "data")
        ).load_checked(),
    )
    try:
        await asyncio.wait_for(runtime.attach_create(), timeout=5)
        assert runtime.state.value == "ready"
        review = await database.fetchone(
            """
            SELECT state FROM runtime_command_manifest
            WHERE command_name = 'review'
            """
        )
        reconciliation = await database.fetchone(
            """
            SELECT requested_epoch, applied_epoch, status
            FROM reconciliation_state WHERE topic = 'commands'
            """
        )
        assert review is None or review["state"] == "unavailable"
        assert reconciliation["status"] == "idle"
        assert reconciliation["requested_epoch"] == reconciliation["applied_epoch"]
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_remote_auth_and_unverified_capabilities_fail_closed(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"remote_disable", "remote_status"},
        git_origin=True,
        unsupported={"ephemeral_query"},
    ) as (runtime, bridge, database):
        bridge.authenticated = False
        status = await runtime.remote_status()
        assert not status["auth"]["authenticated"]
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_remote_mode = 'on', remote_steerable = 1
            """
        )
        bridge.remote_mode = "on"
        off = await runtime.set_remote("off", idempotency_key="remote-off-no-auth")
        assert off["mode"] == "off"
        assert bridge.remote_mode == "off"
        await database.execute(
            """
            UPDATE session_bindings
            SET runtime_remote_mode = 'unknown',
                pending_remote_target = 'on',
                pending_remote_transition_id = 'old-unknown-transition'
            """
        )
        bridge.remote_mode = "on"
        recovered = await runtime.set_remote(
            "off",
            idempotency_key="remote-off-from-unknown",
        )
        assert recovered["mode"] == "off"
        pending = await database.fetchone(
            """
            SELECT pending_remote_target, pending_remote_transition_id
            FROM session_bindings
            """
        )
        assert dict(pending) == {
            "pending_remote_target": None,
            "pending_remote_transition_id": None,
        }
        with pytest.raises(NativeCapabilityError, match="not verified"):
            await runtime.ask_ephemeral("question", idempotency_key="unsupported")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed_agent", "expected_state", "expected_desired"),
    [
        ("default", "rejected", "default"),
        ("reviewer", "confirmed", "reviewer"),
        ("external-agent", "unknown", "default"),
    ],
)
async def test_attach_reconciles_crash_pending_agent_transition(
    tmp_path: Path,
    observed_agent: str,
    expected_state: str,
    expected_desired: str,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / f"agent-recovery-{observed_agent}.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id=f"thread-{observed_agent}",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    operation_id = str(uuid4())
    transition_id = str(uuid4())
    now = datetime.now(UTC).timestamp()
    await database.execute(
        """
        INSERT INTO session_operations(
            operation_id, sdk_session_id, runtime_generation,
            owner_fence_token, kind, idempotency_key, input_hash,
            state, created_at
        ) VALUES (?, ?, 0, 0, 'agent', ?, 'hash', 'started', ?)
        """,
        (operation_id, session_id, f"agent-recovery:{observed_agent}", now),
    )
    await database.execute(
        """
        INSERT INTO runtime_agent_transitions(
            transition_id, sdk_session_id, operation_id,
            previous_agent, target_agent, state, created_at
        ) VALUES (?, ?, ?, 'default', 'reviewer', 'pending', ?)
        """,
        (transition_id, session_id, operation_id, now),
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET pending_agent = 'reviewer',
            pending_agent_transition_id = ?,
            runtime_agent = 'unknown'
        WHERE sdk_session_id = ?
        """,
        (transition_id, session_id),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    bridge.current_agent_name = observed_agent
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id=f"agent-recovery:{observed_agent}",
        binding=binding,
        capabilities=manifest,
    )
    try:
        if expected_state == "unknown":
            with pytest.raises(SessionNotReady, match="runtime_agent_drift"):
                await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        else:
            await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        row = await database.fetchone(
            """
            SELECT desired_agent, pending_agent,
                   pending_agent_transition_id, runtime_agent
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        transition = await database.fetchone(
            """
            SELECT state, result_json FROM runtime_agent_transitions
            WHERE transition_id = ?
            """,
            (transition_id,),
        )
        assert row["desired_agent"] == expected_desired
        assert row["pending_agent"] is None
        assert row["pending_agent_transition_id"] is None
        assert row["runtime_agent"] == observed_agent
        assert transition["state"] == expected_state
        assert "after_resume" in transition["result_json"]
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=5)
        await database.close()


@pytest.mark.asyncio
async def test_recovered_agent_event_settles_transition_row_atomically(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "agent-event-recovery.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-agent-event-recovery",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    operation_id = str(uuid4())
    transition_id = str(uuid4())
    now = datetime.now(UTC).timestamp()
    await database.execute(
        """
        INSERT INTO session_operations(
            operation_id, sdk_session_id, runtime_generation,
            owner_fence_token, kind, idempotency_key, input_hash,
            state, created_at
        ) VALUES (?, ?, 0, 0, 'agent', 'agent-event-recovery',
                  'hash', 'started', ?)
        """,
        (operation_id, session_id, now),
    )
    await database.execute(
        """
        INSERT INTO runtime_agent_transitions(
            transition_id, sdk_session_id, operation_id,
            previous_agent, target_agent, state, created_at
        ) VALUES (?, ?, ?, 'default', 'reviewer', 'pending', ?)
        """,
        (transition_id, session_id, operation_id, now),
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET pending_agent = 'reviewer',
            pending_agent_transition_id = ?,
            runtime_agent = 'unknown',
            event_cursor = 'cursor-before'
        WHERE sdk_session_id = ?
        """,
        (transition_id, session_id),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    bridge.current_agent_name = "reviewer"
    recovered = SessionEvent(
        data=SubagentSelectedData(
            agent_display_name="Reviewer",
            agent_name="reviewer",
            tools=[],
        ),
        id=uuid4(),
        timestamp=datetime.now(UTC),
        type=SessionEventType.SUBAGENT_SELECTED,
    )
    bridge.event_log_batches = [
        EventLogBatch(
            cursor="cursor-after",
            cursor_status="ok",
            events=(recovered,),
            has_more=False,
            filtered_ephemeral=0,
        )
    ]
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="agent-event-recovery",
        binding=binding,
        capabilities=CapabilityRegistry(
            Settings(_env_file=None, data_dir=tmp_path / "data")
        ).load_checked(),
    )
    try:
        await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        row = await database.fetchone(
            """
            SELECT desired_agent, pending_agent, runtime_agent
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        transition = await database.fetchone(
            """
            SELECT state, result_json FROM runtime_agent_transitions
            WHERE transition_id = ?
            """,
            (transition_id,),
        )
        assert dict(row) == {
            "desired_agent": "reviewer",
            "pending_agent": None,
            "runtime_agent": "reviewer",
        }
        assert transition["state"] == "confirmed"
        assert any(
            basis in transition["result_json"]
            for basis in (
                "sdk_selected_event",
                "observed_target_after_resume",
            )
        )
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_attach_forces_off_and_abandons_pending_remote_transition(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "remote-recovery.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-remote-recovery",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    operation_id = str(uuid4())
    transition_id = str(uuid4())
    now = datetime.now(UTC).timestamp()
    await database.execute(
        """
        INSERT INTO session_operations(
            operation_id, sdk_session_id, runtime_generation,
            owner_fence_token, kind, idempotency_key, input_hash,
            state, created_at
        ) VALUES (?, ?, 0, 0, 'remote', 'remote-recovery', 'hash', 'started', ?)
        """,
        (operation_id, session_id, now),
    )
    await database.execute(
        """
        INSERT INTO runtime_remote_transitions(
            transition_id, sdk_session_id, operation_id,
            previous_mode, target_mode, state, auth_json,
            repository_json, created_at
        ) VALUES (?, ?, ?, 'off', 'on', 'pending', '{}', '{}', ?)
        """,
        (transition_id, session_id, operation_id, now),
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET pending_remote_target = 'on',
            pending_remote_transition_id = ?,
            runtime_remote_mode = 'unknown'
        WHERE sdk_session_id = ?
        """,
        (transition_id, session_id),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    bridge.remote_mode = "on"
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="remote-recovery",
        binding=binding,
        capabilities=manifest,
    )
    try:
        await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        row = await database.fetchone(
            """
            SELECT pending_remote_target, pending_remote_transition_id,
                   runtime_remote_mode, remote_steerable
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        transition = await database.fetchone(
            """
            SELECT state, snapshot_json FROM runtime_remote_transitions
            WHERE transition_id = ?
            """,
            (transition_id,),
        )
        assert dict(row) == {
            "pending_remote_target": None,
            "pending_remote_transition_id": None,
            "runtime_remote_mode": "off",
            "remote_steerable": 0,
        }
        assert bridge.remote_mode == "off"
        assert transition["state"] == "unknown"
        assert "forced_off_during_attach" in transition["snapshot_json"]
    finally:
        await asyncio.wait_for(runtime.shutdown(), timeout=5)
        await database.close()


@pytest.mark.asyncio
async def test_attach_forces_unverified_export_mode_off(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "remote-export-recovery.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-remote-export-recovery",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET runtime_remote_mode = 'export', remote_steerable = 0
        WHERE sdk_session_id = ?
        """,
        (session_id,),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    bridge.remote_mode = "export"
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    capabilities = dict(manifest.capabilities)
    capabilities["remote_status"] = replace(
        capabilities["remote_status"],
        supported=False,
    )
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="remote-export-recovery",
        binding=binding,
        capabilities=replace(manifest, capabilities=capabilities),
    )
    try:
        await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        row = await database.fetchone(
            """
            SELECT runtime_remote_mode, remote_steerable
            FROM session_bindings WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        assert dict(row) == {
            "runtime_remote_mode": "off",
            "remote_steerable": 0,
        }
        assert bridge.remote_mode == "off"
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted_mode", ["on", "unknown"])
async def test_attach_fails_closed_when_unsafe_remote_cannot_be_disabled(
    tmp_path: Path,
    persisted_mode: str,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "remote-disable-unavailable.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-remote-disable-unavailable",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET runtime_remote_mode = ?, remote_steerable = ?
        WHERE sdk_session_id = ?
        """,
        (
            persisted_mode,
            None if persisted_mode == "unknown" else 1,
            session_id,
        ),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    capabilities = dict(manifest.capabilities)
    for name in ("remote_disable", "remote_status"):
        capabilities[name] = replace(capabilities[name], supported=False)
    runtime = SessionRuntime(
        database=database,
        bridge=NativeBridge(session_id),
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="remote-disable-unavailable",
        binding=binding,
        capabilities=replace(manifest, capabilities=capabilities),
    )
    try:
        with pytest.raises(SessionNotReady, match="cannot be disabled"):
            await asyncio.wait_for(runtime.attach_resume(), timeout=5)
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_attach_marks_no_event_compaction_unknown_and_blocks_ready(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "compaction-recovery.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-compaction-recovery",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    operation_id = str(uuid4())
    compaction_id = str(uuid4())
    now = datetime.now(UTC).timestamp()
    await database.execute(
        """
        INSERT INTO session_operations(
            operation_id, sdk_session_id, runtime_generation,
            owner_fence_token, kind, idempotency_key, input_hash,
            state, created_at
        ) VALUES (?, ?, 0, 0, 'compact', 'compact-recovery', 'hash', 'started', ?)
        """,
        (operation_id, session_id, now),
    )
    await database.execute(
        """
        INSERT INTO compaction_runs(
            compaction_id, sdk_session_id, operation_id,
            sdk_receive_seq_before, state, created_at
        ) VALUES (?, ?, ?, 0, 'pending', ?)
        """,
        (compaction_id, session_id, operation_id, now),
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    manifest = CapabilityRegistry(
        Settings(_env_file=None, data_dir=tmp_path / "data")
    ).load_checked()
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="compaction-recovery",
        binding=binding,
        capabilities=manifest,
    )
    try:
        with pytest.raises(SessionNotReady, match="compaction_outcome_unknown"):
            await asyncio.wait_for(runtime.attach_resume(), timeout=5)
        compaction = await database.fetchone(
            """
            SELECT state, result_json FROM compaction_runs
            WHERE compaction_id = ?
            """,
            (compaction_id,),
        )
        assert compaction["state"] == "unknown"
        assert "completion_event_observed" in compaction["result_json"]
        with pytest.raises(SessionNotReady):
            await runtime.send("must remain blocked", idempotency_key="blocked")
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_fleet_losing_lease_headroom_after_rpc_never_confirms(
    tmp_path: Path,
) -> None:
    async with native_runtime(
        tmp_path,
        capabilities={"fleet_start"},
    ) as (runtime, bridge, database):

        async def lose_headroom() -> None:
            await database.execute(
                """
                UPDATE session_owner_leases
                SET expires_at = ?
                WHERE sdk_session_id = ?
                """,
                (
                    datetime.now(UTC).timestamp() + 10,
                    runtime.binding.sdk_session_id,
                ),
            )

        bridge.fleet_hook = lose_headroom
        with pytest.raises(OperationAmbiguous, match="outcome is unknown"):
            await runtime.start_fleet("analyze", idempotency_key="lost-headroom")
        operation = await database.fetchone(
            """
            SELECT state, error_code FROM session_operations
            WHERE kind = 'fleet'
            """
        )
        fleet = await database.fetchone("SELECT state FROM fleet_runs")
        assert operation["state"] == "unknown"
        assert operation["error_code"] in {
            "FenceLost",
            "owner_fence_lost_after_dispatch",
        }
        assert fleet is not None and fleet["state"] == "unknown"


@pytest.mark.asyncio
async def test_attach_post_rpc_fence_loss_retains_handle_for_cleanup(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "attach-post-fence.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-attach-post-fence",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    bridge = NativeBridge(session_id)
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="attach-post-fence",
        binding=binding,
        capabilities=CapabilityRegistry(
            Settings(_env_file=None, data_dir=tmp_path / "data")
        ).load_checked(),
    )

    async def lose_headroom() -> None:
        await database.execute(
            """
            UPDATE session_owner_leases
            SET expires_at = ?
            WHERE sdk_session_id = ?
            """,
            (datetime.now(UTC).timestamp() + 10, session_id),
        )

    bridge.attach_hook = lose_headroom
    try:
        with pytest.raises(SessionAttachUnknown):
            await runtime.attach_create()
        assert bridge.handle.disconnect_calls == 1
        assert runtime.handle is None
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_event_log_recovery_failure_disconnects_attached_handle(
    tmp_path: Path,
) -> None:
    session_id = str(uuid4())
    database = Database(tmp_path / "event-log-attach-cleanup.sqlite3")
    await database.open()
    bindings = SessionBindingRepository(database)
    binding = await bindings.create(
        thread_id="thread-event-log-attach-cleanup",
        sdk_session_id=session_id,
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    binding = await bindings.by_session(session_id)
    assert binding is not None
    bridge = NativeBridge(session_id)
    bridge.event_log_error = RuntimeError("event log unavailable")
    runtime = SessionRuntime(
        database=database,
        bridge=bridge,
        bindings=bindings,
        owner_leases=OwnerLeaseStore(database),
        owner_id="event-log-attach-cleanup",
        binding=binding,
        capabilities=CapabilityRegistry(
            Settings(_env_file=None, data_dir=tmp_path / "data")
        ).load_checked(),
    )
    try:
        with pytest.raises(SessionAttachUnknown, match="event-log recovery"):
            await runtime.attach_resume()
        assert bridge.handle.disconnect_calls == 1
        assert runtime.handle is None
    finally:
        await runtime.shutdown()
        await database.close()


@pytest.mark.asyncio
async def test_native_sdk_events_reconcile_durable_projections(tmp_path: Path) -> None:
    supported = {
        "agents_deselect",
        "agents_select",
        "history_compact",
        "remote_status",
        "schedules_list",
    }
    async with native_runtime(tmp_path, capabilities=supported) as (
        runtime,
        bridge,
        database,
    ):
        bridge.compact_error = TimeoutError("lost compact response")
        with pytest.raises(OperationAmbiguous):
            await runtime.compact("retain decisions", idempotency_key="compact-event")
        with pytest.raises(DetachBlocked, match="compaction_outcome_unknown"):
            await runtime.compact("new focus", idempotency_key="compact-new")
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionCompactionStartData(),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_COMPACTION_START,
            ),
        )
        completion_id = uuid4()
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionCompactionCompleteData(
                    success=True,
                    messages_removed=1,
                    tokens_removed=10,
                ),
                id=completion_id,
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_COMPACTION_COMPLETE,
            ),
        )
        compaction = await database.fetchone(
            "SELECT state, completion_event_id FROM compaction_runs"
        )
        assert dict(compaction) == {
            "state": "confirmed",
            "completion_event_id": str(completion_id),
        }
        await runtime.inbox.commit_internal(
            {
                "type": "copilotd.compaction.settled",
                "data": {
                    "compaction_id": (
                        await database.fetchone("SELECT compaction_id FROM compaction_runs")
                    )["compaction_id"],
                    "state": "unknown",
                    "settled_at": datetime.now(UTC).timestamp(),
                },
            },
            internal_event_id="compaction:late-unknown",
        )
        monotonic = await database.fetchone("SELECT state FROM compaction_runs")
        assert monotonic["state"] == "confirmed"

        bridge.schedules = [
            {
                "id": 7,
                "nextRunAt": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "prompt": "scheduled prompt",
                "displayPrompt": "scheduled prompt",
                "recurring": False,
            }
        ]
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionScheduleCreatedData(
                    id=7,
                    prompt="scheduled prompt",
                    recurring=False,
                ),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_SCHEDULE_CREATED,
            ),
        )
        next_run_ms = int((datetime.now(UTC) + timedelta(hours=2)).timestamp() * 1000)
        bridge.schedules[0]["nextRunAt"] = datetime.fromtimestamp(
            next_run_ms / 1000,
            tz=UTC,
        ).isoformat()
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionScheduleRearmedData(id=7, next_run_at=next_run_ms),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_SCHEDULE_REARMED,
            ),
        )
        bridge.schedules.clear()
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionScheduleCancelledData(id=7),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_SCHEDULE_CANCELLED,
            ),
        )
        schedule = await database.fetchone(
            """
            SELECT state, schedule_kind, next_run_at, terminal_at
            FROM runtime_schedules WHERE runtime_schedule_id = '7'
            """
        )
        assert schedule["state"] == "cancelled"
        assert schedule["schedule_kind"] == "after"
        assert schedule["next_run_at"] == pytest.approx(next_run_ms / 1000)
        assert schedule["terminal_at"] is not None

        bridge.schedules = [
            {
                "id": 9,
                "nextRunAt": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                "prompt": "one-shot",
                "displayPrompt": "one-shot",
                "recurring": False,
            }
        ]
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionScheduleCreatedData(
                    id=9,
                    prompt="one-shot",
                    recurring=False,
                ),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_SCHEDULE_CREATED,
            ),
        )
        bridge.schedules.clear()
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=RuntimeScheduleMessageData(9),  # type: ignore[arg-type]
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.USER_MESSAGE,
            ),
        )
        triggered = await database.fetchone(
            """
            SELECT state, terminal_at FROM runtime_schedules
            WHERE runtime_schedule_id = '9'
            """
        )
        assert triggered["state"] == "triggered"
        assert triggered["terminal_at"] is not None

        bridge.current_agent_name = "reviewer"
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SubagentSelectedData(
                    agent_display_name="Reviewer",
                    agent_name="reviewer",
                    tools=[],
                ),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SUBAGENT_SELECTED,
            ),
        )
        selected = await database.fetchone("SELECT runtime_agent FROM session_bindings")
        assert selected["runtime_agent"] == "reviewer"
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SubagentSelectedData(
                    agent_display_name="Old agent",
                    agent_name="old-agent",
                    tools=[],
                ),
                id=uuid4(),
                timestamp=datetime.now(UTC) - timedelta(days=1),
                type=SessionEventType.SUBAGENT_SELECTED,
            ),
        )
        ordered_agent = await database.fetchone("SELECT runtime_agent FROM session_bindings")
        assert ordered_agent["runtime_agent"] == "reviewer"
        bridge.current_agent_name = "default"
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SubagentDeselectedData(),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SUBAGENT_DESELECTED,
            ),
        )
        deselected = await database.fetchone("SELECT runtime_agent FROM session_bindings")
        assert deselected["runtime_agent"] == "default"

        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionRemoteSteerableChangedData(remote_steerable=True),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_REMOTE_STEERABLE_CHANGED,
            ),
        )
        remote = await database.fetchone(
            """
            SELECT runtime_remote_mode, remote_steerable
            FROM session_bindings
            """
        )
        assert dict(remote) == {
            "runtime_remote_mode": "on",
            "remote_steerable": 1,
        }
        await runtime.inbox.commit_internal(
            {
                "type": "copilotd.snapshot.requested",
                "data": {"topic": "remote"},
            },
            source="snapshot",
            internal_event_id="remote-crossing:requested",
        )
        reconciliation = await database.fetchone(
            """
            SELECT requested_epoch FROM reconciliation_state
            WHERE topic = 'remote'
            """
        )
        await runtime.inbox.commit_internal(
            {
                "type": "copilotd.snapshot.observed",
                "data": {
                    "topic": "remote",
                    "epoch": int(reconciliation["requested_epoch"]),
                    "snapshot_id": "remote-crossing",
                    "query_start_sdk_receive_seq": 0,
                    "query_end_sdk_receive_seq": runtime.inbox.last_sdk_receive_seq,
                    "payload": {
                        "mode": "off",
                        "steerable": False,
                        "metadata": {},
                    },
                    "observed_at": datetime.now(UTC).timestamp(),
                },
            },
            source="snapshot",
            internal_event_id="remote-crossing:observed",
        )
        crossing = await database.fetchone(
            """
            SELECT runtime_remote_mode FROM session_bindings
            """
        )
        assert crossing["runtime_remote_mode"] == "on"
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionRemoteSteerableChangedData(remote_steerable=False),
                id=uuid4(),
                timestamp=datetime.now(UTC) - timedelta(days=1),
                type=SessionEventType.SESSION_REMOTE_STEERABLE_CHANGED,
            ),
        )
        ordered_remote = await database.fetchone(
            """
            SELECT runtime_remote_mode, remote_steerable
            FROM session_bindings
            """
        )
        assert dict(ordered_remote) == {
            "runtime_remote_mode": "on",
            "remote_steerable": 1,
        }
        await _emit(
            runtime,
            bridge,
            SessionEvent(
                data=SessionRemoteSteerableChangedData(remote_steerable=False),
                id=uuid4(),
                timestamp=datetime.now(UTC),
                type=SessionEventType.SESSION_REMOTE_STEERABLE_CHANGED,
            ),
        )
        ambiguous_remote = await database.fetchone(
            """
            SELECT runtime_remote_mode, remote_steerable
            FROM session_bindings
            """
        )
        assert dict(ambiguous_remote) == {
            "runtime_remote_mode": "unknown",
            "remote_steerable": 0,
        }


def _command(name: str) -> NativeCommandDefinition:
    return NativeCommandDefinition(
        name=name,
        kind="builtin",
        description=f"{name} builtin",
        aliases=(),
        allow_during_agent_execution=False,
        experimental=False,
        schedulable=True,
        input=None,
    )


async def _git(cwd: Path, *arguments: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "git",
        *arguments,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(stderr.decode(errors="replace"))


async def _emit(
    runtime: SessionRuntime,
    bridge: NativeBridge,
    event: SessionEvent,
) -> None:
    bridge.ingress(event)
    await runtime.inbox.join()
    await asyncio.sleep(0.05)
    await runtime.inbox.join()
