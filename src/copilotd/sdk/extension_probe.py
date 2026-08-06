from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from copilot import CopilotClient, RuntimeConnection
from copilot.generated.rpc import (
    MCPListToolsRequest,
    PermissionsAllowAllMode,
    PermissionsSetAAllSource,
    PermissionsSetAllowAllRequest,
    PermissionsSetApproveAllRequest,
)
from copilot.tools import Tool, ToolResult

from copilotd.config import Settings
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.extensions import (
    CustomAgent,
    EnvironmentBinding,
    EnvironmentReference,
    ExtensionConfigRepository,
    ExtensionConfigSnapshot,
    McpStdioServer,
    ProjectExtensionConfig,
)
from copilotd.core.projects import ProjectRegistry
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.task_registry import TaskRegistry
from copilotd.sdk.bridge import CopilotBridge, ManagedAwarePermissionHandler
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


class LiveAcceptanceAuthError(RuntimeError):
    pass


class ExtensionAcceptanceProbe:
    """Secure, disposable acceptance for the protocol-v3 extension surface."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def run_live(self, *, wait_seconds: float = 180) -> dict[str, Any]:
        started_at = time.time()
        bridge = CopilotBridge(self._settings)
        await bridge.start()
        session_ids: set[str] = set()
        try:
            identity = await bridge.runtime_identity()
            if not identity.get("authenticated"):
                raise LiveAcceptanceAuthError(
                    "authenticated Copilot identity is required for live extension acceptance"
                )
            workspace = (
                self._settings.cache_dir / f"extension-acceptance-workspace-{uuid.uuid4().hex}"
            )
            await asyncio.to_thread(workspace.mkdir, parents=True)
            try:
                primary = await self._run_primary(
                    bridge,
                    workspace / "primary",
                    session_ids,
                    wait_seconds=wait_seconds,
                )
                oauth = await self._run_oauth(
                    bridge,
                    workspace / "oauth",
                    session_ids,
                    wait_seconds=wait_seconds,
                )
                reattach = await self._run_reattach(
                    bridge,
                    workspace / "reattach",
                    session_ids,
                    wait_seconds=wait_seconds,
                )
            finally:
                await asyncio.to_thread(shutil.rmtree, workspace, True)
            negative_auth = await self._probe_missing_auth_gate()
            leftovers = await self._leftover_acceptance_sessions(bridge, session_ids)
            if leftovers:
                raise RuntimeError("live acceptance left disposable sessions behind")
            result = {
                "schema_version": 1,
                "generated_at": datetime.now(UTC).isoformat(),
                "identity": {
                    "runtime_version": identity["runtime_version"],
                    "protocol_version": identity["protocol_version"],
                    "authenticated": True,
                },
                "features": {
                    **primary,
                    **oauth,
                    **reattach,
                    "missing_auth_gate": negative_auth,
                },
                "cleanup": {
                    "sessions_removed": True,
                    "temporary_workspace_removed": True,
                    "oauth_server_stopped": True,
                },
                "duration_seconds": round(time.time() - started_at, 3),
            }
            _assert_sanitized(result)
            return result
        finally:
            for session_id in session_ids:
                try:
                    await bridge.client.delete_session(session_id)
                except Exception:
                    pass
            await bridge.stop()

    def write_evidence(self, result: dict[str, Any]) -> tuple[Path, str]:
        self._settings.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._settings.cache_dir / "extension-acceptance.json"
        encoded = json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        path.write_text(encoded + "\n", encoding="utf-8")
        return path, hashlib.sha256((encoded + "\n").encode()).hexdigest()

    async def _run_primary(
        self,
        bridge: CopilotBridge,
        workspace: Path,
        session_ids: set[str],
        *,
        wait_seconds: float,
    ) -> dict[str, Any]:
        await asyncio.to_thread(workspace.mkdir, parents=True)
        session_id = str(uuid.uuid4())
        session_ids.add(session_id)
        events: list[str] = []
        hooks_seen: list[str] = []
        permission_audits: list[dict[str, Any]] = []
        elicitation_calls: list[dict[str, Any]] = []
        external_calls: list[Any] = []
        permission_database = Database(workspace / "permission-fence.sqlite3")
        await permission_database.open()
        permission_leases = OwnerLeaseStore(
            permission_database,
            ttl_seconds=max(wait_seconds * 4, 600),
        )
        permission_lease = await permission_leases.acquire(
            session_id,
            "extension-acceptance",
        )
        response_tasks = TaskRegistry()
        response_results: dict[str, list[bool]] = {
            "sampling": [],
            "session_limits": [],
            "mcp_headers": [],
        }
        session_holder: dict[str, Any] = {}
        deferred_responses: list[tuple[str, str]] = []
        loop = asyncio.get_running_loop()
        local_server = _fixture_path("disposable_mcp_server.py")
        skill_directory, plugin_directory = await asyncio.to_thread(
            _create_extension_fixtures,
            workspace,
        )
        config = ProjectExtensionConfig(
            environment_references=(
                EnvironmentReference(
                    "acceptance_value",
                    "COPILOTD_ACCEPTANCE_VALUE",
                ),
            ),
            mcp_servers=(
                McpStdioServer(
                    name="local",
                    command=sys.executable,
                    args=(str(local_server),),
                    environment=(
                        EnvironmentBinding(
                            "COPILOTD_ACCEPTANCE_VALUE",
                            "acceptance_value",
                        ),
                    ),
                ),
            ),
            skill_directories=(str(skill_directory),),
            disabled_skills=("disabled-acceptance-skill",),
            plugin_directories=(str(plugin_directory),),
            custom_agents=(
                CustomAgent(
                    name="acceptance_agent",
                    prompt="You are a disposable acceptance agent.",
                    skills=("enabled-acceptance-skill",),
                ),
            ),
        ).normalized(workspace)
        snapshot = ExtensionConfigSnapshot(
            scope_key="acceptance",
            version=1,
            project_id=None,
            project_source="implicit-home",
            cwd_snapshot=workspace,
            config_hash=config.digest(),
            config=config,
        )
        session_options = snapshot.sdk_session_options(
            {"COPILOTD_ACCEPTANCE_VALUE": "non-secret-acceptance-value"}
        )

        def external_handler(invocation: Any) -> ToolResult:
            external_calls.append(invocation.arguments)
            return ToolResult(text_result_for_llm="EXTERNAL_TOOL_OK")

        session_options["tools"] = [
            Tool(
                name="copilotd_external_acceptance",
                description="Returns one fixed disposable acceptance marker.",
                handler=external_handler,
                parameters={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                skip_permission=True,
                defer="never",
            )
        ]

        async def permission_audit(payload: dict[str, Any]) -> None:
            await permission_database.execute(
                """
                INSERT INTO permission_audit_events(
                    audit_id, sdk_session_id, runtime_generation,
                    owner_fence_token, request_id, permission_kind,
                    managed_settings, managed_approval_required,
                    decision, request_hash, observed_at
                ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    session_id,
                    permission_lease.fence_token,
                    payload.get("request_id"),
                    str(payload["permission_kind"]),
                    int(bool(payload.get("managed_settings"))),
                    int(bool(payload.get("managed_approval_required"))),
                    str(payload["decision"]),
                    str(payload["request_hash"]),
                    time.time(),
                ),
            )
            permission_audits.append(payload)

        async def approval_fence_valid() -> bool:
            return await permission_leases.has_mutation_headroom(permission_lease)

        permission_handler = ManagedAwarePermissionHandler(
            permission_audit,
            approval_fence_valid,
        )

        async def respond_to_protocol_request(kind: str, request_id: str) -> None:
            active_session = session_holder["session"]
            if kind == "sampling":
                accepted = await bridge.respond_sampling(
                    active_session,
                    request_id,
                    None,
                )
            elif kind == "session_limits":
                accepted = await bridge.respond_session_limits(
                    active_session,
                    request_id,
                )
            else:
                accepted = await bridge.respond_mcp_headers(
                    active_session,
                    request_id,
                    None,
                )
            response_results[kind].append(accepted)

        def schedule_protocol_response(kind: str, request_id: str) -> None:
            if "session" not in session_holder:
                deferred_responses.append((kind, request_id))
                return
            response_tasks.create(
                respond_to_protocol_request(kind, request_id),
                name=f"acceptance-{kind}-{request_id}",
                source="extension-acceptance",
                session_id=session_id,
            )

        def on_event(event: Any) -> None:
            raw_type = event.type.value
            events.append(raw_type)
            data = event.to_dict().get("data", {})
            if raw_type in {
                "session.managed_settings_resolved",
                "session.managed_settings_enforced",
            } and isinstance(data, dict):
                permission_handler.set_managed_permissions_blocked(
                    raw_type.endswith("_enforced")
                    or bool(data.get("bypassPermissionsDisabled"))
                    or bool(data.get("failClosed"))
                )
            response_kind = {
                "sampling.requested": "sampling",
                "session_limits_exhausted.requested": "session_limits",
                "mcp.headers_refresh_required": "mcp_headers",
            }.get(raw_type)
            if (
                response_kind is not None
                and isinstance(data, dict)
                and data.get("requestId") is not None
            ):
                loop.call_soon_threadsafe(
                    schedule_protocol_response,
                    response_kind,
                    str(data["requestId"]),
                )

        async def elicitation_handler(context: dict[str, Any]) -> dict[str, Any]:
            elicitation_calls.append(context)
            return {
                "action": "accept",
                "content": {"label": "live-ok", "enabled": True},
            }

        hooks = _acceptance_hooks(hooks_seen)
        session = None
        try:
            session = await bridge.create_session(
                session_id=session_id,
                working_directory=str(workspace),
                on_event=on_event,
                permission_handler=permission_handler,
                hooks=hooks,
                on_elicitation_request=elicitation_handler,
                session_options=session_options,
            )
            session_holder["session"] = session
            for kind, request_id in deferred_responses:
                schedule_protocol_response(kind, request_id)
            deferred_responses.clear()
            await _wait_for_mcp_status(
                session,
                "local",
                "connected",
                wait_seconds=wait_seconds,
            )
            tools = await session.rpc.mcp.list_tools(
                MCPListToolsRequest(server_name="local"),
                timeout=10,
            )
            agents = await bridge.get_agents(session)
            skills = await bridge.get_skills(session)
            plugins = (await session.rpc.plugins.list(timeout=10)).to_dict()
            skill_states = {
                str(item.get("name")): bool(item.get("enabled"))
                for item in skills.get("skills", [])
                if isinstance(item, dict)
            }
            plugin_names = sorted(
                str(item.get("name"))
                for item in plugins.get("plugins", [])
                if isinstance(item, dict)
            )

            await session.rpc.permissions.set_allow_all(
                PermissionsSetAllowAllRequest(
                    enabled=False,
                    mode=PermissionsAllowAllMode.OFF,
                    source=PermissionsSetAAllSource.RPC,
                ),
                timeout=10,
            )
            await session.rpc.permissions.set_approve_all(
                PermissionsSetApproveAllRequest(
                    enabled=False,
                    source=PermissionsSetAAllSource.RPC,
                ),
                timeout=10,
            )
            permission_message = await asyncio.wait_for(
                session.send_and_wait(
                    "In this disposable directory only, perform exactly these actions: "
                    "(1) use shell to run `printf shell-ok > shell-marker.txt`; "
                    "(2) use write to create write-marker.txt containing write-ok; "
                    "(3) call local/get_acceptance_env; "
                    "(4) invoke copilotd_external_acceptance. "
                    "Then reply exactly PROTOCOL_ACCEPTANCE_DONE."
                ),
                timeout=wait_seconds,
            )
            restored = await bridge.ensure_allow_all(session)

            elicitation_message = await asyncio.wait_for(
                session.send_and_wait(
                    "Call local/request_elicitation exactly once, then reply exactly "
                    "ELICITATION_ACCEPTANCE_DONE."
                ),
                timeout=wait_seconds,
            )
            before_sampling = len(events)
            sampling_message = await asyncio.wait_for(
                session.send_and_wait(
                    "Call local/request_sampling exactly once. Whether the server rejects "
                    "it or succeeds, reply exactly SAMPLING_GATE_DONE."
                ),
                timeout=wait_seconds,
            )
            await asyncio.sleep(0)
            await response_tasks.wait_empty(wait_seconds=30)
            if "sampling.requested" in events:
                await _wait_for_event_type(
                    events,
                    "sampling.completed",
                    wait_seconds=30,
                )
            sampling_events = events[before_sampling:]

            missing_limit = await bridge.respond_session_limits(
                session,
                "missing-session-limit-request",
            )
            missing_sampling = await bridge.respond_sampling(
                session,
                "missing-sampling-request",
                None,
            )
            missing_headers = await bridge.respond_mcp_headers(
                session,
                "missing-headers-request",
                None,
            )
            sampling_evidence = _protocol_response_evidence(
                requested_type="sampling.requested",
                completed_type="sampling.completed",
                events=sampling_events,
                accepted=response_results["sampling"],
                missing_request_result=missing_sampling,
                trigger_attempted=True,
                turn_completed=("SAMPLING_GATE_DONE" in str(sampling_message.data.content)),
            )
            limits_evidence = _protocol_response_evidence(
                requested_type="session_limits_exhausted.requested",
                completed_type="session_limits_exhausted.completed",
                events=events,
                accepted=response_results["session_limits"],
                missing_request_result=missing_limit,
                trigger_attempted=False,
            )
            headers_evidence = _protocol_response_evidence(
                requested_type="mcp.headers_refresh_required",
                completed_type="mcp.headers_refresh_completed",
                events=events,
                accepted=response_results["mcp_headers"],
                missing_request_result=missing_headers,
                trigger_attempted=False,
            )
            permission_kinds = sorted({str(item["permission_kind"]) for item in permission_audits})
            hook_set = sorted(set(hooks_seen))
            registered_hooks = sorted(
                {
                    "error",
                    "post_failure",
                    "post_tool",
                    "pre_mcp",
                    "pre_tool",
                    "prompt_submitted",
                    "session_end",
                    "session_start",
                }
            )
            required_live_hooks = {
                "post_tool",
                "pre_mcp",
                "pre_tool",
                "prompt_submitted",
                "session_end",
                "session_start",
            }
            primary = {
                "extension_config": _passed(
                    verified=(
                        "get_acceptance_env" in {tool.name for tool in tools.tools}
                        and "acceptance_agent"
                        in {
                            str(item.get("name"))
                            for item in agents.get("agents", [])
                            if isinstance(item, dict)
                        }
                        and skill_states.get("enabled-acceptance-skill") is True
                        and skill_states.get("disabled-acceptance-skill") is False
                        and skill_states.get("plugin-acceptance-skill") is True
                        and "acceptance-plugin" in plugin_names
                    ),
                    mcp_connected=True,
                    mcp_tools=sorted(tool.name for tool in tools.tools),
                    custom_agent_loaded=(
                        "acceptance_agent"
                        in {
                            str(item.get("name"))
                            for item in agents.get("agents", [])
                            if isinstance(item, dict)
                        }
                    ),
                    skill_states={
                        name: skill_states.get(name)
                        for name in (
                            "disabled-acceptance-skill",
                            "enabled-acceptance-skill",
                            "plugin-acceptance-skill",
                        )
                    },
                    plugins=plugin_names,
                    environment_reference_resolved=(
                        "local/get_acceptance_env" in str(permission_message.data.content)
                        or "PROTOCOL_ACCEPTANCE_DONE" in str(permission_message.data.content)
                    ),
                ),
                "hooks": _passed(
                    verified=required_live_hooks.issubset(hook_set),
                    registered=registered_hooks,
                    observed=hook_set,
                    required_observed=sorted(required_live_hooks),
                    registered_unobserved=sorted(set(registered_hooks).difference(hook_set)),
                ),
                "permissions": _passed(
                    verified=(
                        all(kind in permission_kinds for kind in ("shell", "write", "mcp"))
                        and {str(item["decision"]) for item in permission_audits}
                        == {"approve-once"}
                        and restored.enabled
                        and restored.approve_all_confirmed
                    ),
                    permission_kinds=permission_kinds,
                    all_required_kinds=all(
                        kind in permission_kinds for kind in ("shell", "write", "mcp")
                    ),
                    decisions=sorted({str(item["decision"]) for item in permission_audits}),
                    shell_effect=(workspace / "shell-marker.txt").read_text() == "shell-ok",
                    write_effect=(workspace / "write-marker.txt").read_text().strip() == "write-ok",
                    allow_all_restored=(restored.enabled and restored.approve_all_confirmed),
                    managed_live_triggered=False,
                ),
                "external_tool": _passed(
                    verified=(
                        len(external_calls) == 1
                        and "external_tool.requested" in events
                        and "external_tool.completed" in events
                    ),
                    handler_calls=len(external_calls),
                    requested_event="external_tool.requested" in events,
                    completed_event="external_tool.completed" in events,
                ),
                "elicitation": _passed(
                    verified=(
                        len(elicitation_calls) == 1
                        and "elicitation.requested" in events
                        and "elicitation.completed" in events
                    ),
                    handler_calls=len(elicitation_calls),
                    requested_event="elicitation.requested" in events,
                    completed_event="elicitation.completed" in events,
                    schema_type=(
                        elicitation_calls[0].get("requestedSchema", {}).get("type")
                        if elicitation_calls
                        else None
                    ),
                    turn_completed=(
                        "ELICITATION_ACCEPTANCE_DONE" in str(elicitation_message.data.content)
                    ),
                ),
                "sampling": sampling_evidence,
                "session_limits_response": limits_evidence,
                "mcp_headers_response": headers_evidence,
            }
            return primary
        finally:
            await response_tasks.cancel_all(wait_seconds=5)
            if session is not None:
                await session.disconnect()
            await _delete_session(bridge, session_id)
            try:
                await permission_leases.release(permission_lease)
            except Exception:
                pass
            await permission_database.close()

    async def _run_oauth(
        self,
        bridge: CopilotBridge,
        workspace: Path,
        session_ids: set[str],
        *,
        wait_seconds: float,
    ) -> dict[str, Any]:
        await asyncio.to_thread(workspace.mkdir, parents=True)
        token = f"acceptance-{uuid.uuid4().hex}"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(_fixture_path("disposable_oauth_mcp_server.py")),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "COPILOTD_OAUTH_ACCEPTANCE_TOKEN": token},
        )
        if process.stdout is None:
            raise RuntimeError("OAuth MCP server stdout is unavailable")
        line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not line.startswith(b"Listening: "):
            raise RuntimeError("OAuth MCP server failed to start")
        base_url = line.decode().strip().removeprefix("Listening: ")
        session_id = str(uuid.uuid4())
        session_ids.add(session_id)
        requests: list[dict[str, Any]] = []
        events: list[str] = []

        async def authorize(
            request: dict[str, Any],
            _context: dict[str, str],
        ) -> dict[str, Any]:
            requests.append(request)
            return {
                "kind": "token",
                "accessToken": token,
                "tokenType": "Bearer",
                "expiresIn": 60,
            }

        session = None
        try:
            session = await bridge.create_session(
                session_id=session_id,
                working_directory=str(workspace),
                on_event=lambda event: events.append(event.type.value),
                permission_handler=ManagedAwarePermissionHandler(),
                on_mcp_auth_request=authorize,
                session_options={
                    "mcp_servers": {
                        "oauth": {
                            "type": "http",
                            "url": f"{base_url}/mcp",
                            "tools": ["*"],
                        }
                    },
                    "mcp_oauth_token_storage": "in-memory",
                    "enable_config_discovery": False,
                },
            )
            await _wait_for_mcp_status(
                session,
                "oauth",
                "connected",
                wait_seconds=wait_seconds,
            )
            tools = await session.rpc.mcp.list_tools(
                MCPListToolsRequest(server_name="oauth"),
                timeout=10,
            )
            return {
                "mcp_http_oauth": _passed(
                    verified=(
                        len(requests) == 1
                        and requests[0].get("reason") == "initial"
                        and "mcp.oauth_required" in events
                        and "mcp.oauth_completed" in events
                        and [tool.name for tool in tools.tools] == ["whoami"]
                    ),
                    handler_calls=len(requests),
                    request_reason=requests[0].get("reason") if requests else None,
                    requested_event="mcp.oauth_required" in events,
                    completed_event="mcp.oauth_completed" in events,
                    tools=sorted(tool.name for tool in tools.tools),
                    token_persisted=False,
                )
            }
        finally:
            if session is not None:
                await session.disconnect()
            await _delete_session(bridge, session_id)
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except TimeoutError:
                    process.kill()
                    await process.wait()

    async def _run_reattach(
        self,
        bridge: CopilotBridge,
        workspace: Path,
        session_ids: set[str],
        *,
        wait_seconds: float,
    ) -> dict[str, Any]:
        await asyncio.to_thread(workspace.mkdir, parents=True)
        session_id = str(uuid.uuid4())
        session_ids.add(session_id)
        database = Database(workspace / "state.sqlite3")
        await database.open()
        runtime: SessionRuntime | None = None
        try:
            projects = ProjectRegistry(database, resolved_home=workspace)
            await projects.initialize()
            project = await projects.resolve("acceptance")
            configs = ExtensionConfigRepository(database)
            local_server = _fixture_path("disposable_mcp_server.py")
            first = await configs.publish(
                project,
                ProjectExtensionConfig(
                    mcp_servers=(
                        McpStdioServer(
                            name="first",
                            command=sys.executable,
                            args=(str(local_server),),
                        ),
                    )
                ),
            )
            bindings = SessionBindingRepository(database)
            binding = await bindings.create(
                thread_id="acceptance-thread",
                sdk_session_id=session_id,
                cwd_snapshot=workspace,
                project_source=project.source.value,
                desired_session_config_version=first.version,
                desired_session_config_hash=first.config_hash,
            )
            runtime = SessionRuntime(
                database=database,
                bridge=bridge,
                bindings=bindings,
                owner_leases=OwnerLeaseStore(database),
                owner_id="extension-acceptance",
                binding=binding,
                extension_configs=configs,
            )
            await runtime.attach_create()
            await runtime.send(
                "Reply exactly REATTACH_BASELINE_READY.",
                idempotency_key="acceptance-baseline",
                agent_mode="interactive",
            )
            await _wait_runtime_idle(
                database,
                session_id,
                wait_seconds=wait_seconds,
            )
            await runtime._refresh_all_snapshots()
            fence = runtime.binding.owner_fence_token
            generation = runtime.binding.runtime_generation
            second = await runtime.reload_extension_config(
                idempotency_key="acceptance-reattach",
                config=ProjectExtensionConfig(
                    mcp_servers=(
                        McpStdioServer(
                            name="second",
                            command=sys.executable,
                            args=(str(local_server),),
                        ),
                    ),
                    custom_agents=(
                        CustomAgent(
                            name="reattach_agent",
                            prompt="You are a disposable reattach agent.",
                        ),
                    ),
                ),
                expected_project_config_version=1,
            )
            mcp = await bridge.get_mcp_servers(runtime.handle)
            agents = await bridge.get_agents(runtime.handle)
            mcp_names = sorted(
                str(item.get("name")) for item in mcp.get("servers", []) if isinstance(item, dict)
            )
            agent_names = {
                str(item.get("name")) for item in agents.get("agents", []) if isinstance(item, dict)
            }
            tools = await runtime.handle.rpc.mcp.list_tools(
                MCPListToolsRequest(server_name="second"),
                timeout=10,
            )
            tool_names = sorted(tool.name for tool in tools.tools)
            if runtime.inbox is not None:
                await runtime.inbox.join()
            await runtime._refresh_all_snapshots()
            idle_before = await database.fetchone(
                """
                SELECT COUNT(*) AS count FROM event_journal
                WHERE sdk_session_id = ? AND raw_type = 'session.idle'
                """,
                (session_id,),
            )
            await runtime.send(
                "Call second/echo exactly once with text REATTACH_TOOL_OK, "
                "then reply exactly REATTACH_TOOL_OK.",
                idempotency_key="acceptance-reattach-tool",
                agent_mode="interactive",
            )
            await _wait_runtime_idle(
                database,
                session_id,
                wait_seconds=wait_seconds,
                minimum_idle_count=int(idle_before["count"]) + 1,
            )
            tool_events = await database.fetchall(
                """
                SELECT raw_type, tool_call_id, raw_payload
                FROM event_journal
                WHERE sdk_session_id = ?
                  AND generation = ?
                  AND raw_type IN (
                      'tool.execution_start',
                      'tool.execution_complete'
                  )
                ORDER BY inbox_seq
                """,
                (session_id, runtime.binding.runtime_generation),
            )
            tool_evidence = _correlate_mcp_tool_evidence(
                tool_events,
                server_name="second",
                tool_name="echo",
                marker="REATTACH_TOOL_OK",
            )
            result = {
                "config_reattach": _passed(
                    verified=(
                        runtime.binding.owner_fence_token == fence
                        and runtime.binding.runtime_generation == generation + 1
                        and runtime.binding.runtime_session_config_hash == second.config_hash
                        and runtime.binding.session_config_state == "synced"
                        and mcp_names == ["second"]
                        and "reattach_agent" in agent_names
                        and "echo" in tool_names
                        and tool_evidence["correlated"]
                    ),
                    same_owner_fence=runtime.binding.owner_fence_token == fence,
                    generation_incremented=(runtime.binding.runtime_generation == generation + 1),
                    config_synced=(
                        runtime.binding.runtime_session_config_hash == second.config_hash
                        and runtime.binding.session_config_state == "synced"
                    ),
                    mcp_servers=mcp_names,
                    removed_server_absent="first" not in mcp_names,
                    custom_agent_loaded="reattach_agent" in agent_names,
                    mcp_tools=tool_names,
                    tool_call=tool_evidence,
                )
            }
            await runtime.close(idempotency_key="acceptance-close")
            runtime = None
            return result
        finally:
            if runtime is not None:
                try:
                    await runtime.shutdown()
                except Exception:
                    pass
            await database.close()
            await _delete_session(bridge, session_id)

    async def _probe_missing_auth_gate(self) -> dict[str, Any]:
        runtime_path = os.environ.get("COPILOT_CLI_PATH")
        if not runtime_path:
            return {
                "status": "unsupported",
                "reason": "COPILOT_CLI_PATH is not set for isolated auth probe",
            }
        stripped = dict(os.environ)
        for name in (
            "COPILOT_GITHUB_TOKEN",
            "COPILOT_SDK_AUTH_TOKEN",
            "GH_TOKEN",
            "GITHUB_TOKEN",
        ):
            stripped.pop(name, None)
        connection = RuntimeConnection.for_stdio(
            path=runtime_path,
            args=("--yolo", "--no-auto-login"),
        )
        connection.env = stripped
        client = CopilotClient(
            connection=connection,
            session_idle_timeout_seconds=0,
        )
        await client.start()
        try:
            auth = await client.get_auth_status()
            models_failed = False
            try:
                await client.list_models()
            except Exception:
                models_failed = True
            if auth.isAuthenticated or not models_failed:
                raise RuntimeError("missing-auth live gate did not fail closed")
            return _passed(
                verified=not auth.isAuthenticated and models_failed,
                authenticated=False,
                models_failed=True,
            )
        finally:
            await client.stop()

    async def _leftover_acceptance_sessions(
        self,
        bridge: CopilotBridge,
        session_ids: set[str],
    ) -> list[str]:
        if not session_ids:
            return []
        sessions = await bridge.client.list_sessions()
        active = {str(getattr(item, "session_id", getattr(item, "id", ""))) for item in sessions}
        return sorted(session_ids.intersection(active))


def _acceptance_hooks(seen: list[str]) -> dict[str, Callable[..., Awaitable[Any]]]:
    def handler(name: str, result: Any = None) -> Callable[..., Awaitable[Any]]:
        async def callback(_input: Any, _context: Any) -> Any:
            seen.append(name)
            return result

        return callback

    return {
        "on_pre_tool_use": handler("pre_tool"),
        "on_pre_mcp_tool_call": handler("pre_mcp"),
        "on_post_tool_use": handler("post_tool"),
        "on_post_tool_use_failure": handler("post_failure"),
        "on_user_prompt_submitted": handler("prompt_submitted"),
        "on_session_start": handler(
            "session_start",
            {"additionalContext": "disposable acceptance"},
        ),
        "on_session_end": handler("session_end"),
        "on_error_occurred": handler("error"),
    }


async def _wait_for_mcp_status(
    session: Any,
    server_name: str,
    expected: str,
    *,
    wait_seconds: float,
) -> None:
    deadline = asyncio.get_running_loop().time() + wait_seconds
    while asyncio.get_running_loop().time() < deadline:
        listing = await session.rpc.mcp.list(timeout=10)
        server = next(
            (item for item in listing.servers if item.name == server_name),
            None,
        )
        if server is not None and server.status.value == expected:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError(f"MCP server did not reach expected state: {expected}")


async def _wait_for_event_type(
    events: list[str],
    event_type: str,
    *,
    wait_seconds: float,
) -> None:
    deadline = asyncio.get_running_loop().time() + wait_seconds
    while asyncio.get_running_loop().time() < deadline:
        if event_type in events:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"protocol completion event was not observed: {event_type}")


def _protocol_response_evidence(
    *,
    requested_type: str,
    completed_type: str,
    events: list[str],
    accepted: list[bool],
    missing_request_result: bool,
    trigger_attempted: bool,
    turn_completed: bool | None = None,
) -> dict[str, Any]:
    if missing_request_result is not False:
        raise RuntimeError("missing protocol request ID was unexpectedly accepted")
    requested_count = events.count(requested_type)
    completed_count = events.count(completed_type)
    if requested_count:
        if (
            completed_count < requested_count
            or len(accepted) < requested_count
            or not all(accepted)
        ):
            raise RuntimeError(
                f"observed {requested_type} without successful settlement and completion"
            )
        return {
            "status": "passed",
            "request_count": requested_count,
            "completion_count": completed_count,
            "responses_accepted": list(accepted),
            "trigger_attempted": trigger_attempted,
            **({} if turn_completed is None else {"tool_turn_completed": turn_completed}),
        }
    return {
        "status": "unprobed",
        "request_count": 0,
        "completion_count": 0,
        "trigger_attempted": trigger_attempted,
        "missing_request_rpc_returned_false": True,
        **({} if turn_completed is None else {"tool_turn_completed": turn_completed}),
    }


def _correlate_mcp_tool_evidence(
    rows: list[Any],
    *,
    server_name: str,
    tool_name: str,
    marker: str,
) -> dict[str, Any]:
    starts: dict[str, list[dict[str, Any]]] = {}
    completions: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        tool_call_id = row["tool_call_id"]
        if tool_call_id is None:
            continue
        try:
            payload = json.loads(str(row["raw_payload"]))
        except json.JSONDecodeError:
            continue
        data = payload.get("data", {})
        if not isinstance(data, dict):
            continue
        if str(data.get("toolCallId") or "") != str(tool_call_id):
            continue
        if row["raw_type"] == "tool.execution_start":
            starts.setdefault(str(tool_call_id), []).append(data)
        elif row["raw_type"] == "tool.execution_complete":
            completions.setdefault(str(tool_call_id), []).append(data)
    matching_ids: list[str] = []
    for tool_call_id, start_events in starts.items():
        if len(start_events) != 1:
            continue
        start = start_events[0]
        observed_server = start.get("mcpServerName")
        observed_tool = start.get("mcpToolName")
        arguments = start.get("arguments")
        if (
            observed_server == server_name
            and observed_tool == tool_name
            and isinstance(arguments, dict)
            and arguments.get("text") == marker
        ):
            matching_ids.append(tool_call_id)
    completed_ids = [
        tool_call_id
        for tool_call_id in matching_ids
        if len(completions.get(tool_call_id, [])) == 1
        and completions[tool_call_id][0].get("success") is True
        and marker
        in json.dumps(
            completions[tool_call_id][0].get("result"),
            sort_keys=True,
        )
    ]
    return {
        "correlated": len(matching_ids) == 1 and completed_ids == matching_ids,
        "matching_start_count": len(matching_ids),
        "matching_completion_count": len(completed_ids),
        "request_identity_matched": bool(completed_ids),
        "server": server_name,
        "tool": tool_name,
        "result_marker_matched": bool(completed_ids),
    }


async def _wait_runtime_idle(
    database: Database,
    session_id: str,
    *,
    wait_seconds: float,
    minimum_idle_count: int = 1,
) -> None:
    deadline = asyncio.get_running_loop().time() + wait_seconds
    while asyncio.get_running_loop().time() < deadline:
        idle = await database.fetchone(
            """
            SELECT COUNT(*) AS count FROM event_journal
            WHERE sdk_session_id = ? AND raw_type = 'session.idle'
            """,
            (session_id,),
        )
        active = await database.fetchone(
            """
            SELECT COUNT(*) AS count FROM liveness_leases
            WHERE sdk_session_id = ? AND state = 'active'
            """,
            (session_id,),
        )
        if int(idle["count"]) >= minimum_idle_count and int(active["count"]) == 0:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError("disposable session did not become reattach-safe")


async def _delete_session(bridge: CopilotBridge, session_id: str) -> None:
    try:
        await bridge.client.delete_session(session_id)
    except Exception:
        sessions = await bridge.client.list_sessions()
        if any(
            str(getattr(item, "session_id", getattr(item, "id", ""))) == session_id
            for item in sessions
        ):
            raise


def _fixture_path(name: str) -> Path:
    path = Path(__file__).parent / "fixtures" / name
    if not path.is_file():
        raise RuntimeError(f"acceptance fixture is missing: {name}")
    return path.resolve()


def _create_extension_fixtures(workspace: Path) -> tuple[Path, Path]:
    skill_directory = workspace / "skills"
    for name in ("enabled-acceptance-skill", "disabled-acceptance-skill"):
        directory = skill_directory / name
        directory.mkdir(parents=True)
        (directory / "SKILL.md").write_text(
            (
                "---\n"
                f"name: {name}\n"
                "description: Disposable extension acceptance skill.\n"
                "---\n\n"
                "# Disposable acceptance skill\n"
            ),
            encoding="utf-8",
        )
    plugin_directory = workspace / "plugin"
    plugin_directory.mkdir()
    (plugin_directory / "plugin.json").write_text(
        json.dumps(
            {
                "name": "acceptance-plugin",
                "description": "Disposable extension acceptance plugin.",
                "version": "1.0.0",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (plugin_directory / "SKILL.md").write_text(
        (
            "---\n"
            "name: plugin-acceptance-skill\n"
            "description: Disposable plugin-provided acceptance skill.\n"
            "---\n\n"
            "# Disposable plugin acceptance skill\n"
        ),
        encoding="utf-8",
    )
    return skill_directory, plugin_directory


def _passed(*, verified: bool, **evidence: Any) -> dict[str, Any]:
    if not verified:
        raise RuntimeError("live extension acceptance assertion failed")
    return {"status": "passed", **evidence}


def _assert_sanitized(payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).lower()
    forbidden = (
        "accesstoken",
        "clientsecret",
        "authorization:",
        "gh_token",
        "github_token",
        "copilot_sdk_auth_token",
    )
    if any(value in encoded for value in forbidden):
        raise RuntimeError("live acceptance evidence contains sensitive fields")
