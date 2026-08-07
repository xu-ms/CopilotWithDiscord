from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import discord
import structlog
from discord import app_commands
from discord.ext import commands

from copilotd.config import Settings
from copilotd.core.attachments import (
    AttachmentCapabilities,
    AttachmentError,
    AttachmentService,
)
from copilotd.core.bindings import (
    BindingIntent,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.commands import (
    CDCommandError,
    CDConflictError,
    CDDiscordError,
    CDInputError,
    CDPathError,
    CDResumeError,
    CDRuntimeError,
    CDScopeError,
    CDSessionNotFoundError,
    CDSessionStateError,
    CommandExecutor,
    CommandInvocation,
    CommandOperation,
    CommandResponder,
    CommandResponse,
    ModelReasoningSummaryAdapter,
    OpsSurfaceAdapter,
    ScheduleOriginAdapter,
    SessionNamingAdapter,
    TaskActionAdapter,
    UnknownInteractionError,
)
from copilotd.core.extensions import (
    ExtensionConfigFileSource,
    ExtensionConfigRepository,
)
from copilotd.core.interactions import (
    DiscordInteractionAdapter,
    ElicitationField,
    ElicitationForm,
)
from copilotd.core.lifecycle_commands import (
    DiscordParentType,
    ProjectLifecycleService,
    SchedulerCommandService,
    WorktreeCommandService,
)
from copilotd.core.projects import ProjectRegistry, ProjectSnapshot, ProjectSource
from copilotd.core.recovery import RecoveryInventoryReport, StartupRecoveryInventory
from copilotd.core.scheduler import SchedulerRepository, SchedulerWorker
from copilotd.core.scheduler_adapter import ApplicationSchedulerAdapter
from copilotd.core.session_deletion import (
    SessionDeletionBlocked,
    SessionDeletionService,
    SessionDeletionUnknown,
)
from copilotd.core.session_runtime import (
    DetachBlocked,
    SessionNotReady,
    SessionRuntime,
)
from copilotd.core.sessions import (
    CreationIntentRepository,
    SessionCreationService,
    SessionCreationUnknown,
    SessionRegistry,
    ThreadReference,
)
from copilotd.core.spill_artifacts import garbage_collect_tool_spills
from copilotd.core.supervisor import ExecutionStallMonitor
from copilotd.core.task_registry import TaskRegistry
from copilotd.core.worktrees import SessionCreationWorktreeAdapter, WorktreeManager
from copilotd.discord_native import NativeDiscordRegistrar
from copilotd.ops.control import ServiceControlWorker
from copilotd.ops.heartbeat import HeartbeatWriter
from copilotd.ops.service import ServiceManager, SqliteRestartCoordinator
from copilotd.ops.surface import LocalOpsSurface
from copilotd.render.diffs import render_diff
from copilotd.render.markdown import (
    MarkdownAssembler,
    TableBlock,
    TextBlock,
    extract_local_markdown_images,
    plan_markdown_messages,
)
from copilotd.render.outbox import (
    RenderDeliveryError,
    RenderOutboxDispatcher,
    RenderPermanentError,
    RenderRateLimited,
    RenderTransientError,
)
from copilotd.render.tables import TableAsset, render_table
from copilotd.sdk.bridge import CopilotBridge
from copilotd.sdk.capabilities import CapabilityManifest, CapabilityRegistry
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore

logger = structlog.get_logger(__name__)
_TABLE_DELIMITER = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$")
_COLOR_BLURPLE = 0x5865F2
_COLOR_GREEN = 0x57F287
_COLOR_YELLOW = 0xFEE75C
_COLOR_RED = 0xED4245
_COLOR_CYAN = 0x5BC0DE
_COLOR_NEUTRAL = 0x747F8D


class CopilotDiscordBot(commands.Bot):
    def __init__(
        self,
        settings: Settings,
        *,
        ops_service: OpsSurfaceAdapter | None = None,
        session_naming_adapter: SessionNamingAdapter | None = None,
        model_summary_adapter: ModelReasoningSummaryAdapter | None = None,
        schedule_origin_adapter: ScheduleOriginAdapter | None = None,
        task_action_adapter: TaskActionAdapter | None = None,
        discord_connector: aiohttp.BaseConnector | None = None,
        discord_http_trace: aiohttp.TraceConfig | None = None,
    ) -> None:
        if os.environ.get("COPILOTD_MANAGED_SERVICE") == "1":
            settings = settings.ensure_service_handoff_token()
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
            connector=discord_connector,
            http_trace=discord_http_trace,
        )
        self.settings = settings
        self.database = Database(settings.database_path)
        self.bridge = CopilotBridge(settings)
        self.capability_registry = CapabilityRegistry(settings)
        self.capabilities: CapabilityManifest = self.capability_registry.load_checked()
        self.recovery_inventory: RecoveryInventoryReport | None = None
        self.stall_monitor: ExecutionStallMonitor | None = None
        self.attachment_service = AttachmentService(
            self.database,
            settings.data_dir,
            file_max_bytes=settings.attachment_file_max_bytes,
            message_max_bytes=settings.attachment_message_max_bytes,
            blob_max_bytes=settings.attachment_blob_max_bytes,
            capabilities=AttachmentCapabilities(
                discord_file_max_bytes=settings.attachment_file_max_bytes,
                discord_message_max_bytes=settings.attachment_message_max_bytes,
                runtime_inline_blob_max_bytes=settings.attachment_blob_max_bytes,
                runtime_serialized_frame_max_bytes=(settings.attachment_runtime_frame_max_bytes),
            ),
        )
        self.heartbeat = HeartbeatWriter(
            self.database,
            settings.heartbeat_path,
            interval_seconds=settings.heartbeat_interval_seconds,
            gateway_down_seconds=settings.gateway_down_restart_seconds,
            resume_suppression_seconds=settings.resume_suppression_seconds,
            metrics_provider=self._heartbeat_metrics,
        )
        self.ops_service = ops_service or LocalOpsSurface(self.database, settings)
        self.session_naming_adapter = session_naming_adapter
        self.model_summary_adapter = model_summary_adapter
        self.schedule_origin_adapter = schedule_origin_adapter
        self.task_action_adapter = task_action_adapter
        self.command_executor = CommandExecutor(error_mapper=_map_command_error)
        self.projects: ProjectRegistry | None = None
        self.bindings: SessionBindingRepository | None = None
        self.extension_configs: ExtensionConfigRepository | None = None
        self.extension_config_source = ExtensionConfigFileSource()
        self.sessions: SessionRegistry | None = None
        self.deletions: SessionDeletionService | None = None
        self.creation: SessionCreationService | None = None
        self.dispatcher: RenderOutboxDispatcher | None = None
        self.scheduler_repository: SchedulerRepository | None = None
        self.scheduler_worker: SchedulerWorker | None = None
        self.scheduler_commands: SchedulerCommandService | None = None
        self.project_commands: ProjectLifecycleService | None = None
        self.worktree_manager: WorktreeManager | None = None
        self.worktree_commands: WorktreeCommandService | None = None
        self._tasks = TaskRegistry()
        self._owner_id = f"discord:{uuid.uuid4()}"
        self._commands_registered = False
        self._fatal_worker_error: BaseException | None = None
        self._render_stop = asyncio.Event()
        self._render_task: asyncio.Task[None] | None = None
        self._after_render_send_hook: Callable[[int, str], Awaitable[None]] | None = None
        self._fatal_diagnostic_error: Exception | None = None
        self._restart_task: asyncio.Task[None] | None = None
        self.restart_requested = False
        self._close_lock = asyncio.Lock()
        self._closed_once = False
        self._shutdown_task: asyncio.Task[None] | None = None
        self._fatal_session_id: str | None = None
        self._shutdown_initiator: asyncio.Task[Any] | None = None
        self._accepting_handlers = True
        self._admitted_handlers: set[asyncio.Task[Any]] = set()
        self._attachment_recovery_task: asyncio.Task[None] | None = None

    def _heartbeat_metrics(self) -> tuple[int, int, float | None]:
        if self.sessions is None:
            return 0, 0, None
        return self.sessions.heartbeat_metrics()

    async def setup_hook(self) -> None:
        await self.database.open()
        coordinator = SqliteRestartCoordinator(self.settings.database_path)
        recovery_fence = await asyncio.to_thread(coordinator.recovery_fence)
        if recovery_fence is not None:
            manager = ServiceManager(self.settings)
            replacement_is_managed = await asyncio.to_thread(
                manager.replacement_is_managed,
                pid=os.getpid(),
                process_started_at=self.heartbeat.process_started_at,
            )
            old_process_alive = await asyncio.to_thread(
                manager.process_identity_alive,
                pid=recovery_fence.expected_pid,
                process_started_at=(recovery_fence.expected_process_started_at),
            )
            await asyncio.to_thread(
                coordinator.recover_for_replacement,
                replacement_pid=os.getpid(),
                replacement_generation=self.heartbeat.process_generation,
                replacement_process_started_at=(self.heartbeat.process_started_at),
                manager_handoff_token=(
                    None
                    if self.settings.service_handoff_token is None
                    else self.settings.service_handoff_token.get_secret_value()
                ),
                replacement_is_managed=replacement_is_managed,
                old_process_identity_alive=old_process_alive,
                now=time.time(),
            )
        await garbage_collect_tool_spills(self.database)
        self.projects = ProjectRegistry(
            self.database,
            resolved_home=self.settings.resolved_home,
        )
        await self.projects.initialize()
        self.bindings = SessionBindingRepository(self.database)
        self.extension_configs = ExtensionConfigRepository(self.database)
        leases = OwnerLeaseStore(
            self.database,
            ttl_seconds=self.settings.owner_lease_ttl_seconds,
        )
        await self.bridge.start()
        try:
            self.capabilities = await self.capability_registry.activate(
                self.database,
                await self.bridge.runtime_identity(),
            )
        except BaseException:
            await self.bridge.stop()
            raise
        self.recovery_inventory = await StartupRecoveryInventory(self.database).run()
        self.heartbeat.durable_replay_capable = self.capabilities.supports("event_log")
        self.stall_monitor = ExecutionStallMonitor(
            self.database,
            self.bridge.transport_ping,
        )
        self.heartbeat.runtime_state = "ready"

        def runtime_factory(binding: SessionBinding) -> SessionRuntime:
            return SessionRuntime(
                database=self.database,
                bridge=self.bridge,
                bindings=self._require_bindings(),
                owner_leases=leases,
                owner_id=self._owner_id,
                binding=binding,
                ingress_capacity=self.settings.ingress_capacity,
                reducer_batch_size=self.settings.reducer_batch_size,
                owner_renew_seconds=self.settings.owner_lease_renew_seconds,
                attachment_resolver=self.attachment_service.sdk_attachments_for_send,
                capabilities=self.capabilities,
                task_registry=self._tasks,
                send_frame_max_bytes=(self.settings.attachment_runtime_frame_max_bytes),
                model_summary_adapter=self.model_summary_adapter,
                task_action_adapter=self.task_action_adapter,
                extension_configs=self.extension_configs,
            )

        self.sessions = SessionRegistry(self.bindings, runtime_factory)
        self.deletions = SessionDeletionService(
            self.database,
            self.bindings,
            self.sessions,
            self.bridge,
            data_dir=self.settings.data_dir,
        )
        self.creation = SessionCreationService(
            projects=self.projects,
            intents=CreationIntentRepository(self.database),
            bindings=self.bindings,
            sessions=self.sessions,
            threads=DiscordThreadGateway(self),
            extension_configs=self.extension_configs,
            extension_config_source=self.extension_config_source,
        )
        self.scheduler_repository = SchedulerRepository(self.database)
        await self.scheduler_repository.recover()
        self.scheduler_commands = SchedulerCommandService(
            self.database,
            self.projects,
            self.scheduler_repository,
        )
        self.project_commands = ProjectLifecycleService(self.database, self.projects)
        self.worktree_manager = WorktreeManager(
            self.database,
            self.projects,
            worktrees_root=self.settings.data_dir / "worktrees",
            adapter=SessionCreationWorktreeAdapter(self.creation, self.database),
            task_registry=self._tasks,
        )
        await self.worktree_manager.recover()
        self.worktree_commands = WorktreeCommandService(self.worktree_manager)
        self._tasks.create(
            self._task_failure_loop(),
            name="task-failure-supervisor",
            source="supervisor",
        )
        self._tasks.create(
            self.stall_monitor.run(),
            name="active-execution-stall-monitor",
            source="stall-monitor",
        )
        failures = await self.sessions.eager_resume()
        for thread_id, error in failures.items():
            await logger.awarning(
                "session_eager_resume_failed",
                thread_id=thread_id,
                error=error,
            )
        self.scheduler_worker = SchedulerWorker(
            self.scheduler_repository,
            ApplicationSchedulerAdapter(
                self.database,
                self.bindings,
                self.sessions,
                self.creation,
                self.capabilities,
            ),
            owner_id=f"scheduler:{self._owner_id}",
            task_registry=self._tasks,
        )
        await self.scheduler_worker.start()
        self.dispatcher = RenderOutboxDispatcher(self.database, self)
        self._render_task = self._tasks.create(
            self._render_loop(),
            name="discord-render-outbox",
            source="render-outbox",
        )
        self._tasks.create(
            self.heartbeat.run(),
            name="copilotd-heartbeat",
            source="heartbeat",
        )
        self._tasks.create(
            self._runtime_health_loop(),
            name="copilotd-runtime-health",
            source="runtime-health",
        )
        self._tasks.create(
            ServiceControlWorker(
                self.database,
                self._require_sessions(),
                process_generation=self.heartbeat.process_generation,
                process_started_at=self.heartbeat.process_started_at,
                handoff_token=(
                    ""
                    if self.settings.service_handoff_token is None
                    else self.settings.service_handoff_token.get_secret_value()
                ),
            ).run(),
            name="copilotd-service-control",
            source="service-control",
        )
        self._tasks.create(
            self._attachment_maintenance_loop(),
            name="attachment-lifecycle-maintenance",
            source="attachments",
        )
        self._register_application_commands()
        if self.settings.discord_guild_id is not None:
            guild = discord.Object(id=self.settings.discord_guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def close(self) -> None:
        async with self._close_lock:
            if self._shutdown_task is None:
                self._shutdown_task = asyncio.create_task(
                    self._close_once(),
                    name="copilotd-shutdown",
                )
            shutdown = self._shutdown_task
        await asyncio.shield(shutdown)

    async def _close_once(self) -> None:
        self.heartbeat.set_gateway("down")
        self.heartbeat.runtime_state = "down"
        self._accepting_handlers = False
        errors: list[Exception] = []
        try:
            await super().close()
        except Exception as error:
            errors.append(error)
        if self.sessions is not None:
            try:
                await self.sessions.close_admission()
            except Exception as error:
                errors.append(error)
        try:
            await self._drain_admitted_handlers()
        except Exception as error:
            errors.append(error)
        if self.scheduler_worker is not None:
            try:
                await self.scheduler_worker.stop()
            except Exception as error:
                errors.append(error)
        try:
            await self._stop_render_consumer()
        except Exception as error:
            errors.append(error)
        if self.sessions is not None:
            try:
                await self.sessions.shutdown(
                    emergency_session_id=self._fatal_session_id,
                )
            except Exception as error:
                errors.append(error)
        if self.dispatcher is not None:
            try:
                await self.dispatcher.drain()
            except Exception as error:
                errors.append(error)
        try:
            excluded = (
                frozenset()
                if self._shutdown_initiator is None
                else frozenset({self._shutdown_initiator})
            )
            await self._tasks.cancel_all(exclude=excluded)
        except Exception as error:
            errors.append(error)
        try:
            await self.bridge.stop()
        except Exception as error:
            errors.append(error)
        try:
            await self.database.close()
        except Exception as error:
            errors.append(error)
        self._closed_once = True
        if errors:
            raise ExceptionGroup("copilotD shutdown failed", errors)

    async def _task_failure_loop(self) -> None:
        while True:
            failure = await self._tasks.errors.get()
            try:
                self._fatal_worker_error = failure.error
                self._fatal_session_id = failure.session_id
                self._shutdown_initiator = asyncio.current_task()
                self.heartbeat.runtime_state = "down"
                self.heartbeat.set_gateway("down")
                try:
                    await logger.aerror(
                        "background_task_failed",
                        task_name=failure.name,
                        source=failure.source,
                        session_id=failure.session_id,
                        runtime_generation=failure.runtime_generation,
                        error_type=type(failure.error).__name__,
                        error=str(failure.error),
                    )
                    if failure.session_id is not None:
                        await self.database.execute(
                            """
                            INSERT INTO runtime_incidents(
                                timestamp, runtime_generation, session_id,
                                kind, detail
                            ) VALUES (?, ?, ?, 'background_task_failed', ?)
                            """,
                            (
                                time.time(),
                                failure.runtime_generation or 0,
                                failure.session_id,
                                json.dumps(
                                    {
                                        "task_name": failure.name,
                                        "source": failure.source,
                                        "error_type": type(failure.error).__name__,
                                        "message": str(failure.error),
                                    },
                                    sort_keys=True,
                                ),
                            ),
                        )
                except Exception as diagnostic_error:
                    self._fatal_diagnostic_error = diagnostic_error
                finally:
                    await self.close()
                return
            finally:
                self._tasks.errors.task_done()

    async def _runtime_health_loop(self) -> None:
        while True:
            try:
                async with asyncio.timeout(10):
                    await self.bridge.healthcheck()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.heartbeat.runtime_state = "down"
                await logger.aerror(
                    "runtime_healthcheck_failed",
                    error_type=type(error).__name__,
                    error=str(error),
                )
            else:
                self.heartbeat.runtime_state = "ready"
            await asyncio.sleep(self.settings.heartbeat_interval_seconds)

    async def on_ready(self) -> None:
        self.heartbeat.set_gateway("ready")
        if self._accepting_handlers and (
            self._attachment_recovery_task is None or self._attachment_recovery_task.done()
        ):
            self._attachment_recovery_task = self._tasks.create(
                self._recover_attachment_manifests(),
                name="attachment-source-recovery",
                source="attachments",
            )
        await logger.ainfo(
            "discord_ready",
            user=None if self.user is None else str(self.user),
            guilds=len(self.guilds),
        )

    async def _recover_attachment_manifests(self) -> None:
        recoveries = await self.attachment_service.pending_recoveries()
        for recovery in recoveries:
            if (
                recovery.session_id is None
                or recovery.prompt is None
                or recovery.idempotency_key is None
                or (
                    recovery.state == "preparing"
                    and (recovery.source_channel_id is None or recovery.source_message_id is None)
                )
            ):
                await self.attachment_service.record_recovery_error(
                    recovery.manifest_id,
                    code="source_locator_unavailable",
                    detail="attachment preparation cannot be resumed without its durable source",
                    terminal=True,
                )
                continue
            try:
                if recovery.state == "preparing":
                    channel_id = int(recovery.source_channel_id or "")
                    message_id = int(recovery.source_message_id or "")
                    channel = self.get_channel(channel_id)
                    if channel is None:
                        channel = await self.fetch_channel(channel_id)
                    fetch_message = getattr(channel, "fetch_message", None)
                    if fetch_message is None:
                        raise AttachmentError(
                            "the durable Discord attachment source is not message-addressable"
                        )
                    message = await fetch_message(message_id)
                    prepared = await self.attachment_service.prepare(
                        source_kind=recovery.source_kind,
                        source_id=recovery.source_id,
                        session_id=recovery.session_id,
                        attachments=list(message.attachments),
                        source_channel_id=recovery.source_channel_id,
                        source_message_id=recovery.source_message_id,
                        recovery_prompt=recovery.prompt,
                        recovery_idempotency_key=recovery.idempotency_key,
                        recovery_origin=recovery.origin,
                    )
                    if prepared is None:
                        raise AttachmentError("the durable Discord source has no attachments")
                else:
                    prepared = await self.attachment_service.prepared_manifest(recovery.manifest_id)
                if not recovery.needs_submission:
                    await self.attachment_service.record_recovery_success(recovery.manifest_id)
                    continue
                binding = await self._require_bindings().by_session(recovery.session_id)
                if binding is None or binding.binding_intent != BindingIntent.ACTIVE:
                    raise AttachmentError("the attachment source session is no longer active")
                runtime = await self._require_sessions().ensure_attached(binding)
                sdk_attachments = await self.attachment_service.sdk_attachments(
                    prepared.manifest_id
                )
                await runtime.send(
                    recovery.prompt,
                    idempotency_key=recovery.idempotency_key,
                    attachments=sdk_attachments,
                    attachment_manifest_id=recovery.manifest_id,
                    origin=recovery.origin or "discord_message",
                )
            except (discord.NotFound, discord.Forbidden, AttachmentError) as error:
                await self.attachment_service.record_recovery_error(
                    recovery.manifest_id,
                    code="source_recovery_failed",
                    detail=f"{type(error).__name__}: {error}",
                    terminal=True,
                )
            except (discord.HTTPException, OSError, TimeoutError) as error:
                await self.attachment_service.record_recovery_error(
                    recovery.manifest_id,
                    code="source_recovery_deferred",
                    detail=f"{type(error).__name__}: {error}",
                    terminal=False,
                )
                await logger.awarning(
                    "attachment_recovery_deferred",
                    manifest_id=recovery.manifest_id,
                    error_type=type(error).__name__,
                    error=str(error),
                )
            except Exception as error:
                await self.attachment_service.record_recovery_error(
                    recovery.manifest_id,
                    code="submission_recovery_deferred",
                    detail=f"{type(error).__name__}: {error}",
                    terminal=False,
                )
                await logger.awarning(
                    "attachment_submission_recovery_deferred",
                    manifest_id=recovery.manifest_id,
                    error_type=type(error).__name__,
                    error=str(error),
                )
            else:
                await self.attachment_service.record_recovery_success(recovery.manifest_id)

    async def _attachment_maintenance_loop(self) -> None:
        while True:
            released = await self.attachment_service.release_unreferenced()
            removed = await self.attachment_service.garbage_collect()
            if released or removed:
                await logger.ainfo(
                    "attachment_lifecycle_maintained",
                    released=released,
                    removed=removed,
                )
            await asyncio.sleep(300)

    async def on_disconnect(self) -> None:
        self.heartbeat.set_gateway("reconnecting")

    async def on_resumed(self) -> None:
        self.heartbeat.set_gateway("ready")

    async def on_interaction(self, interaction: discord.Interaction) -> None:
        task = self._admit_handler()
        if task is None:
            return
        try:
            await self._on_interaction_admitted(interaction)
        finally:
            self._admitted_handlers.discard(task)

    async def _on_interaction_admitted(self, interaction: discord.Interaction) -> None:
        data = interaction.data
        custom_id = str(data.get("custom_id", "")) if isinstance(data, dict) else ""
        if interaction.type == discord.InteractionType.component and custom_id.startswith("cdi:"):
            await self._handle_direct_interaction(interaction, custom_id)
            return
        if (
            interaction.type != discord.InteractionType.component
            or not isinstance(data, dict)
            or not custom_id.startswith("cdtd:")
        ):
            return
        parts = str(data["custom_id"]).split(":")
        if len(parts) not in {4, 5}:
            await self._send_component_text(
                interaction,
                "TaskDeck",
                "This TaskDeck control is invalid.",
            )
            return
        _, panel_id, revision_text, action_text, *token_part = parts
        if (
            len(parts) == 5
            and action_text == "message"
            and revision_text.isdigit()
            and interaction.message is not None
        ):
            await DiscordInteractionResponder(
                self,
                interaction,
                name="TaskDeck message",
            ).send_modal(
                TaskMessageModal(
                    self,
                    panel_id=panel_id,
                    card_token=token_part[0],
                    revision=int(revision_text),
                    message_id=str(interaction.message.id),
                )
            )
            return
        action_map = {
            "select": "select",
            "toggle": "toggle",
            "prev": "previous",
            "next": "next",
        }
        action = action_map.get(action_text)
        if (
            (action is None and len(parts) == 4)
            or (
                len(parts) == 5
                and action_text
                not in {
                    "cancel",
                    "promote",
                    "remove",
                    "download",
                }
            )
            or not revision_text.isdigit()
            or interaction.message is None
        ):
            await self._send_component_text(
                interaction,
                "TaskDeck",
                "This TaskDeck control is invalid.",
            )
            return
        responder = DiscordInteractionResponder(self, interaction, name="TaskDeck")
        try:
            await responder.defer()
        except UnknownInteractionError:
            pass
        try:
            runtime = await self._interaction_runtime(interaction)
            if len(parts) == 5:
                result_data = await runtime.perform_taskdeck_action(
                    panel_id=panel_id,
                    card_token=token_part[0],
                    expected_revision=int(revision_text),
                    action=action_text,
                    message_id=str(interaction.message.id),
                    interaction_id=str(interaction.id),
                )
                result = str(result_data["status"])
                if result == "download":
                    await responder.send_file(
                        "Task detail attached.",
                        content=str(result_data["content"]).encode("utf-8"),
                        filename=str(result_data["filename"]),
                    )
                    return
            else:
                values = data.get("values")
                card_token = (
                    str(values[0])
                    if action == "select" and isinstance(values, list) and values
                    else None
                )
                result = (
                    "invalid"
                    if runtime.inbox is None
                    else await runtime.update_taskdeck_view(
                        panel_id=panel_id,
                        expected_revision=int(revision_text),
                        action=action,
                        card_token=card_token,
                        message_id=str(interaction.message.id),
                        interaction_id=str(interaction.id),
                    )
                )
        except Exception as error:
            mapped = _map_command_error(error)
            await responder.send_followup(f"[{mapped.code}] {mapped.message}")
            return
        if result != "updated":
            await responder.send_followup(
                "TaskDeck changed; use the latest controls."
                if result == "stale"
                else "This TaskDeck control has expired."
            )

    async def _send_component_text(
        self,
        interaction: discord.Interaction,
        name: str,
        content: str,
    ) -> None:
        responder = DiscordInteractionResponder(self, interaction, name=name)
        try:
            await responder.send_inline(content)
        except UnknownInteractionError:
            await responder.send_followup(content)

    async def _handle_direct_interaction(
        self,
        interaction: discord.Interaction,
        custom_id: str,
    ) -> None:
        parts = custom_id.split(":")
        if len(parts) != 3:
            await self._send_component_text(
                interaction,
                "Copilot input",
                "This Copilot input control is invalid.",
            )
            return
        _, interaction_id, action = parts
        if action == "freeform":
            await DiscordInteractionResponder(
                self,
                interaction,
                name="Copilot input",
            ).send_modal(InteractionResponseModal(self, interaction_id))
            return
        if action == "form":
            row = await self.database.fetchone(
                """
                SELECT form_schema FROM pending_interactions
                WHERE interaction_id = ? AND state = 'pending'
                """,
                (interaction_id,),
            )
            if row is None or row["form_schema"] is None:
                await interaction.response.send_message(
                    "This Copilot form has expired.",
                    ephemeral=True,
                )
                return
            form = ElicitationForm.from_dict(json.loads(str(row["form_schema"])))
            await interaction.response.send_modal(
                ElicitationResponseModal(self, interaction_id, form)
            )
            return
        runtime = await self._interaction_runtime(interaction)
        if action in {"decline", "cancel"}:
            result = await runtime.respond_interaction(
                interaction_id,
                action=action,
            )
            await interaction.response.send_message(
                _interaction_result_text(result),
                ephemeral=True,
            )
            return
        if action.startswith("choice-") and action.removeprefix("choice-").isdigit():
            result = await runtime.respond_interaction(
                interaction_id,
                selection=int(action.removeprefix("choice-")),
            )
            await interaction.response.send_message(
                _interaction_result_text(result),
                ephemeral=True,
            )
            return
        data = interaction.data
        values = data.get("values") if isinstance(data, dict) else None
        if (
            action != "select"
            or not isinstance(values, list)
            or not values
            or not str(values[0]).isdigit()
        ):
            await self._send_component_text(
                interaction,
                "Copilot input",
                "This Copilot input control is invalid.",
            )
            return
        responder = DiscordInteractionResponder(self, interaction, name="Copilot input")
        try:
            await responder.defer(ephemeral=True)
        except UnknownInteractionError:
            pass
        try:
            runtime = await self._interaction_runtime(interaction)
            result = await runtime.respond_interaction(
                interaction_id,
                selection=int(values[0]),
            )
        except Exception as error:
            mapped = _map_command_error(error)
            await responder.send_followup(f"[{mapped.code}] {mapped.message}")
            return
        await responder.send_followup(_interaction_result_text(result))

    async def on_message(self, message: discord.Message) -> None:
        task = self._admit_handler()
        if task is None:
            return
        try:
            await self._on_message_admitted(message)
        finally:
            self._admitted_handlers.discard(task)

    async def _on_message_admitted(self, message: discord.Message) -> None:
        if message.type not in {discord.MessageType.default, discord.MessageType.reply}:
            return
        if message.author.bot or message.guild is None:
            return
        if await self._is_restart_draining():
            await message.reply("copilotD is draining for restart; no new work was accepted.")
            return
        prompt = self._clean_prompt(message)
        if isinstance(message.channel, discord.Thread):
            binding = await self._require_bindings().by_thread(str(message.channel.id))
            if binding is None or (not prompt and not message.attachments):
                return
            if binding.binding_intent != BindingIntent.ACTIVE:
                if binding.binding_intent == BindingIntent.CLOSED:
                    await message.reply(
                        "[CD-SESSION-002] This session is closed; use `/session resume` "
                        "in this original thread."
                    )
                return
            sessions = self._require_sessions()
            runtime = await sessions.ensure_attached(binding)
            try:
                prepared = await self.attachment_service.prepare(
                    source_kind="discord-message",
                    source_id=str(message.id),
                    session_id=runtime.binding.sdk_session_id,
                    attachments=message.attachments,
                    source_channel_id=str(message.channel.id),
                    source_message_id=str(message.id),
                    recovery_prompt=prompt or "Please inspect the attached files.",
                    recovery_idempotency_key=f"discord-message:{message.id}",
                    recovery_origin="discord_message",
                )
                sdk_attachments = (
                    None
                    if prepared is None
                    else await self.attachment_service.sdk_attachments(prepared.manifest_id)
                )
                await runtime.send(
                    prompt or "Please inspect the attached files.",
                    idempotency_key=f"discord-message:{message.id}",
                    attachments=sdk_attachments,
                    attachment_manifest_id=(None if prepared is None else prepared.manifest_id),
                )
            except AttachmentError as error:
                await message.reply(f"copilotD could not prepare the attachments: `{error}`")
            except Exception as error:
                await message.reply(f"copilotD could not submit this message: `{error}`")
            return

        settings = await self._require_projects().channel_settings(str(message.channel.id))
        mention_required = settings[1] or self.settings.mention_required
        mentioned = self.user is not None and self.user in message.mentions
        if mention_required and not mentioned:
            return
        if not prompt and not message.attachments:
            return
        try:
            effective_prompt = prompt or "Please inspect the attached files."
            runtime = await self._require_creation().create_from_source(
                channel_id=str(message.channel.id),
                source_kind="message",
                source_id=str(message.id),
                prompt=effective_prompt,
                thread_name=_thread_name(effective_prompt),
                send_initial_prompt=False,
            )
            await self._record_session_ui(
                runtime.binding,
                parent_channel_id=str(message.channel.id),
                display_name=_thread_name(effective_prompt),
            )
            prepared = await self.attachment_service.prepare(
                source_kind="discord-message",
                source_id=str(message.id),
                session_id=runtime.binding.sdk_session_id,
                attachments=message.attachments,
                source_channel_id=str(message.channel.id),
                source_message_id=str(message.id),
                recovery_prompt=effective_prompt,
                recovery_idempotency_key=f"message:{message.id}",
                recovery_origin="discord_message",
            )
            sdk_attachments = (
                None
                if prepared is None
                else await self.attachment_service.sdk_attachments(prepared.manifest_id)
            )
            await runtime.send(
                effective_prompt,
                idempotency_key=f"message:{message.id}",
                attachments=sdk_attachments,
                attachment_manifest_id=None if prepared is None else prepared.manifest_id,
            )
            await logger.ainfo(
                "discord_session_created",
                thread_id=runtime.binding.thread_id,
                sdk_session_id=runtime.binding.sdk_session_id,
            )
        except AttachmentError as error:
            await message.reply(f"copilotD could not prepare the attachments: `{error}`")
        except Exception as error:
            await message.reply(f"copilotD could not create the session: `{error}`")

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        try:
            if session_id.startswith(("thread:", "channel:", "ops:")):
                destination = await self._render_destination(session_id)
                plan = await _discord_render_plan(
                    payload,
                    max_bytes=self.settings.discord_upload_max_bytes,
                )
                message_id = await self._deliver_render_plan(
                    thread=destination,
                    session_id=session_id,
                    payload=payload,
                    plan=plan,
                    delivery_id=idempotency_key,
                )
            else:
                binding = await self._require_bindings().by_session(session_id)
                if binding is None:
                    raise RenderPermanentError(f"no Discord binding for SDK session {session_id}")
                thread = await self._thread_for_session(session_id)
                plan = await _discord_render_plan(
                    payload,
                    allowed_roots=(binding.cwd_snapshot,),
                    max_bytes=self.settings.discord_upload_max_bytes,
                )
                message_id = await self._deliver_render_plan(
                    thread=thread,
                    session_id=session_id,
                    payload=payload,
                    plan=plan,
                    delivery_id=idempotency_key,
                )
        except RenderDeliveryError:
            raise
        except (discord.HTTPException, OSError, TimeoutError) as error:
            raise _render_delivery_error(error) from error
        await logger.adebug(
            "render_sent",
            session_id=session_id,
            lane=lane,
            idempotency_key=idempotency_key,
            discord_message_id=message_id,
        )
        return message_id

    async def edit(
        self,
        *,
        session_id: str,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None:
        try:
            if session_id.startswith(("thread:", "channel:", "ops:")):
                destination = await self._render_destination(session_id)
                message = await destination.fetch_message(int(message_id))
                plan = await _discord_render_plan(
                    payload,
                    max_bytes=self.settings.discord_upload_max_bytes,
                )
                await self._deliver_render_plan(
                    thread=destination,
                    session_id=session_id,
                    payload=payload,
                    plan=plan,
                    delivery_id=idempotency_key,
                    first_message=message,
                )
            else:
                binding = await self._require_bindings().by_session(session_id)
                if binding is None:
                    raise RenderPermanentError(f"no Discord binding for SDK session {session_id}")
                thread = await self._thread_for_session(session_id)
                message = await thread.fetch_message(int(message_id))
                plan = await _discord_render_plan(
                    payload,
                    allowed_roots=(binding.cwd_snapshot,),
                    max_bytes=self.settings.discord_upload_max_bytes,
                )
                await self._deliver_render_plan(
                    thread=thread,
                    session_id=session_id,
                    payload=payload,
                    plan=plan,
                    delivery_id=idempotency_key,
                    first_message=message,
                )
        except RenderDeliveryError:
            raise
        except (discord.HTTPException, OSError, TimeoutError) as error:
            raise _render_delivery_error(error) from error
        await logger.adebug("render_edited", lane=lane, discord_message_id=message_id)

    async def _deliver_render_plan(
        self,
        *,
        thread: discord.Thread,
        session_id: str,
        payload: dict[str, Any],
        plan: DiscordRenderPlan,
        delivery_id: str,
        first_message: discord.Message | None = None,
    ) -> str:
        agent_id = str(payload.get("agent_id") or "")
        delivery_family = _render_delivery_family(delivery_id)
        checkpoint = await self.database.fetchone(
            """
            SELECT first_discord_message_id FROM render_attachment_checkpoints
            WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
            """,
            (session_id, delivery_id, agent_id),
        )
        first_message_id = None if checkpoint is None else checkpoint["first_discord_message_id"]
        if first_message is not None:
            first_message_id = str(first_message.id)
        elif first_message_id is None:
            family_checkpoint = await self.database.fetchone(
                """
                SELECT discord_message_id
                FROM render_batch_intents
                WHERE session_id = ? AND delivery_family = ? AND agent_id = ?
                  AND batch_index = 0 AND discord_message_id IS NOT NULL
                ORDER BY updated_at DESC LIMIT 1
                """,
                (session_id, delivery_family, agent_id),
            )
            if family_checkpoint is not None:
                try:
                    first_message = await thread.fetch_message(
                        int(family_checkpoint["discord_message_id"])
                    )
                except (discord.NotFound, KeyError):
                    first_message = None
                else:
                    first_message_id = str(first_message.id)
        delivered = await self.database.fetchall(
            """
            SELECT batch_index, discord_message_id
            FROM render_attachment_batches
            WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
            """,
            (session_id, delivery_id, agent_id),
        )
        delivered_ids = {
            int(row["batch_index"]): str(row["discord_message_id"]) for row in delivered
        }
        persisted_intents = await self.database.fetchall(
            """
            SELECT batch_index FROM render_batch_intents
            WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
            """,
            (session_id, delivery_id, agent_id),
        )
        unexpected_indices = sorted(
            {
                *delivered_ids,
                *(int(row["batch_index"]) for row in persisted_intents),
            }
            - set(range(len(plan.batches)))
        )
        if unexpected_indices:
            raise RenderPermanentError(
                f"render batch count changed for {delivery_id}:"
                f" unexpected persisted indices {unexpected_indices}"
            )
        now = time.time()
        for index, batch in enumerate(plan.batches):
            if index in delivered_ids:
                nonce = _render_batch_nonce(
                    session_id,
                    delivery_id,
                    agent_id,
                    index,
                )
                payload_hash = _render_batch_hash(batch)
                delivered_intent = await self.database.fetchone(
                    """
                    SELECT nonce, payload_hash, state, discord_message_id
                    FROM render_batch_intents
                    WHERE session_id = ? AND render_message_id = ?
                      AND agent_id = ? AND batch_index = ?
                    """,
                    (session_id, delivery_id, agent_id, index),
                )
                if delivered_intent is None:
                    await self.database.execute(
                        """
                        INSERT OR IGNORE INTO render_batch_intents(
                            session_id, render_message_id, agent_id, batch_index,
                            nonce, payload_hash, state, discord_message_id,
                            delivery_family, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'sent', ?, ?, ?, ?)
                        """,
                        (
                            session_id,
                            delivery_id,
                            agent_id,
                            index,
                            nonce,
                            payload_hash,
                            delivered_ids[index],
                            delivery_family,
                            now,
                            now,
                        ),
                    )
                    delivered_intent = await self.database.fetchone(
                        """
                        SELECT nonce, payload_hash, state, discord_message_id
                        FROM render_batch_intents
                        WHERE session_id = ? AND render_message_id = ?
                          AND agent_id = ? AND batch_index = ?
                        """,
                        (session_id, delivery_id, agent_id, index),
                    )
                if (
                    delivered_intent is None
                    or str(delivered_intent["nonce"]) != nonce
                    or str(delivered_intent["payload_hash"]) != payload_hash
                    or str(delivered_intent["state"]) != "sent"
                    or str(delivered_intent["discord_message_id"]) != delivered_ids[index]
                ):
                    raise RenderPermanentError(
                        f"render batch intent changed for {delivery_id}:{index}"
                    )
                if first_message_id is None and index == 0:
                    first_message_id = delivered_ids[index]
                continue
            nonce = _render_batch_nonce(
                session_id,
                delivery_id,
                agent_id,
                index,
            )
            payload_hash = _render_batch_hash(batch)
            previous_intent = await self.database.fetchone(
                """
                SELECT nonce, payload_hash, discord_message_id, created_at
                FROM render_batch_intents
                WHERE session_id = ? AND delivery_family = ? AND agent_id = ?
                  AND batch_index = ? AND render_message_id != ?
                ORDER BY updated_at DESC LIMIT 1
                """,
                (
                    session_id,
                    delivery_family,
                    agent_id,
                    index,
                    delivery_id,
                ),
            )
            intent = await self.database.fetchone(
                """
                SELECT nonce, payload_hash, state, discord_message_id, created_at
                FROM render_batch_intents
                WHERE session_id = ? AND render_message_id = ?
                  AND agent_id = ? AND batch_index = ?
                """,
                (session_id, delivery_id, agent_id, index),
            )
            if intent is None:
                await self.database.execute(
                    """
                    INSERT INTO render_batch_intents(
                        session_id, render_message_id, agent_id, batch_index,
                        nonce, payload_hash, state, delivery_family,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?)
                    """,
                    (
                        session_id,
                        delivery_id,
                        agent_id,
                        index,
                        nonce,
                        payload_hash,
                        delivery_family,
                        now,
                        now,
                    ),
                )
            elif str(intent["nonce"]) != nonce:
                raise RenderPermanentError(f"render batch intent changed for {delivery_id}:{index}")
            elif str(intent["payload_hash"]) != payload_hash:
                if str(intent["state"]) != "prepared" or intent["discord_message_id"] is not None:
                    raise RenderPermanentError(
                        f"render batch intent changed for {delivery_id}:{index}"
                    )
                await self.database.execute(
                    """
                    UPDATE render_batch_intents
                    SET payload_hash = ?, updated_at = ?
                    WHERE session_id = ? AND render_message_id = ?
                      AND agent_id = ? AND batch_index = ?
                      AND state = 'prepared' AND discord_message_id IS NULL
                    """,
                    (
                        payload_hash,
                        now,
                        session_id,
                        delivery_id,
                        agent_id,
                        index,
                    ),
                )

            if intent is not None and intent["discord_message_id"] is not None:
                reconciled_message_id = str(intent["discord_message_id"])
                if first_message_id is None:
                    first_message_id = reconciled_message_id
                await self._checkpoint_render_batch(
                    session_id=session_id,
                    delivery_id=delivery_id,
                    agent_id=agent_id,
                    index=index,
                    discord_message_id=reconciled_message_id,
                    first_message_id=str(first_message_id),
                    attachment_count=len(batch.assets),
                    now=now,
                )
                continue

            recovery_message: discord.Message | None = None
            if index == 0 and first_message is not None:
                recovery_message = first_message
            elif intent is not None:
                recovery_message = await self._find_message_by_nonce(
                    thread,
                    nonce,
                    created_at=float(intent["created_at"]),
                )
            if recovery_message is None and previous_intent is not None:
                if previous_intent["discord_message_id"] is not None:
                    try:
                        recovery_message = await thread.fetch_message(
                            int(previous_intent["discord_message_id"])
                        )
                    except (discord.NotFound, KeyError):
                        recovery_message = None
                else:
                    recovery_message = await self._find_message_by_nonce(
                        thread,
                        str(previous_intent["nonce"]),
                        created_at=float(previous_intent["created_at"]),
                    )

            if recovery_message is not None:
                await recovery_message.edit(
                    content=batch.content or "\u200b",
                    attachments=_discord_files(list(batch.assets)),
                    embeds=_discord_embeds(batch.embeds),
                    view=(
                        _render_view(
                            payload,
                            enable_task_actions=self.task_action_adapter is not None,
                        )
                        if index == 0
                        else None
                    ),
                )
                discord_message_id = str(recovery_message.id)
                if index == 0:
                    first_message = recovery_message
            else:
                sent = await thread.send(
                    content=batch.content or "\u200b",
                    files=_discord_files(list(batch.assets)),
                    embeds=_discord_embeds(batch.embeds),
                    view=(
                        _render_view(
                            payload,
                            enable_task_actions=self.task_action_adapter is not None,
                        )
                        if index == 0
                        else None
                    ),
                    silent=True,
                    nonce=nonce,
                )
                discord_message_id = str(sent.id)
            if first_message_id is None:
                first_message_id = discord_message_id
            if self._after_render_send_hook is not None:
                await self._after_render_send_hook(index, discord_message_id)
            await self._checkpoint_render_batch(
                session_id=session_id,
                delivery_id=delivery_id,
                agent_id=agent_id,
                index=index,
                discord_message_id=discord_message_id,
                first_message_id=str(first_message_id),
                attachment_count=len(batch.assets),
                now=now,
            )
        if first_message_id is None:
            raise RenderPermanentError("render plan did not produce a Discord message")
        await self.database.execute(
            """
            UPDATE render_attachment_checkpoints
            SET finalized = ?, updated_at = ?
            WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
            """,
            (
                int(bool(payload.get("finalized"))),
                time.time(),
                session_id,
                delivery_id,
                agent_id,
            ),
        )
        if first_message is not None:
            await self._prune_previous_render_batches(
                thread=thread,
                session_id=session_id,
                first_message_id=str(first_message.id),
                current_delivery_id=delivery_id,
            )
        return str(first_message_id)

    async def _checkpoint_render_batch(
        self,
        *,
        session_id: str,
        delivery_id: str,
        agent_id: str,
        index: int,
        discord_message_id: str,
        first_message_id: str,
        attachment_count: int,
        now: float,
    ) -> None:
        async with self.database.transaction() as connection:
            await connection.execute(
                """
                UPDATE render_batch_intents
                SET state = 'sent', discord_message_id = ?, updated_at = ?
                WHERE session_id = ? AND render_message_id = ?
                  AND agent_id = ? AND batch_index = ?
                """,
                (
                    discord_message_id,
                    now,
                    session_id,
                    delivery_id,
                    agent_id,
                    index,
                ),
            )
            await connection.execute(
                """
                INSERT INTO render_attachment_batches(
                    session_id, render_message_id, agent_id, batch_index,
                    discord_message_id, idempotency_key, attachment_count,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, render_message_id, agent_id, batch_index)
                DO UPDATE SET
                    discord_message_id = excluded.discord_message_id,
                    attachment_count = excluded.attachment_count,
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    delivery_id,
                    agent_id,
                    index,
                    discord_message_id,
                    f"{delivery_id}:batch:{index}",
                    attachment_count,
                    now,
                    now,
                ),
            )
            await connection.execute(
                """
                INSERT INTO render_attachment_checkpoints(
                    session_id, render_message_id, agent_id,
                    first_discord_message_id, next_batch_index,
                    finalized, updated_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(session_id, render_message_id, agent_id) DO UPDATE SET
                    first_discord_message_id = COALESCE(
                        render_attachment_checkpoints.first_discord_message_id,
                        excluded.first_discord_message_id
                    ),
                    next_batch_index = MAX(
                        render_attachment_checkpoints.next_batch_index,
                        excluded.next_batch_index
                    ),
                    updated_at = excluded.updated_at
                """,
                (
                    session_id,
                    delivery_id,
                    agent_id,
                    first_message_id,
                    index + 1,
                    now,
                ),
            )

    async def _find_message_by_nonce(
        self,
        thread: discord.Thread,
        nonce: str,
        *,
        created_at: float,
    ) -> discord.Message | None:
        history = getattr(thread, "history", None)
        if not callable(history):
            return None
        try:
            async for message in history(
                limit=None,
                after=datetime.fromtimestamp(max(0, created_at - 1), UTC),
                oldest_first=False,
            ):
                if str(getattr(message, "nonce", "")) == nonce:
                    return message
        except discord.HTTPException as error:
            raise _render_delivery_error(error) from error
        return None

    async def _prune_previous_render_batches(
        self,
        *,
        thread: discord.Thread,
        session_id: str,
        first_message_id: str,
        current_delivery_id: str,
    ) -> None:
        old_checkpoints = await self.database.fetchall(
            """
            SELECT render_message_id, agent_id
            FROM render_attachment_checkpoints
            WHERE session_id = ? AND first_discord_message_id = ?
              AND render_message_id != ?
            """,
            (session_id, first_message_id, current_delivery_id),
        )
        if not old_checkpoints:
            return
        old_keys = [
            (str(row["render_message_id"]), str(row["agent_id"])) for row in old_checkpoints
        ]
        current_batches = await self.database.fetchall(
            """
            SELECT discord_message_id FROM render_attachment_batches
            WHERE session_id = ? AND render_message_id = ?
            """,
            (session_id, current_delivery_id),
        )
        current_message_ids = {str(row["discord_message_id"]) for row in current_batches}
        for render_message_id, agent_id in old_keys:
            batches = await self.database.fetchall(
                """
                SELECT discord_message_id FROM render_attachment_batches
                WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
                  AND batch_index > 0
                ORDER BY batch_index
                """,
                (session_id, render_message_id, agent_id),
            )
            for batch in batches:
                if str(batch["discord_message_id"]) in current_message_ids:
                    continue
                try:
                    message = await thread.fetch_message(int(batch["discord_message_id"]))
                    await message.delete()
                except discord.NotFound:
                    continue
        async with self.database.transaction() as connection:
            for render_message_id, agent_id in old_keys:
                await connection.execute(
                    """
                    DELETE FROM render_attachment_batches
                    WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
                    """,
                    (session_id, render_message_id, agent_id),
                )
                await connection.execute(
                    """
                    DELETE FROM render_attachment_checkpoints
                    WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
                    """,
                    (session_id, render_message_id, agent_id),
                )
                await connection.execute(
                    """
                    DELETE FROM render_batch_intents
                    WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
                    """,
                    (session_id, render_message_id, agent_id),
                )

    async def _render_loop(self) -> None:
        while not self._render_stop.is_set():
            dispatcher = self.dispatcher
            if dispatcher is None:
                return
            delivered = await dispatcher.dispatch_once()
            try:
                await asyncio.wait_for(
                    self._render_stop.wait(),
                    timeout=0.2 if delivered else 1.0,
                )
            except TimeoutError:
                pass

    async def _stop_render_consumer(self) -> None:
        self._render_stop.set()
        task = self._render_task
        if task is None:
            return
        try:
            async with asyncio.timeout(5):
                await asyncio.gather(task, return_exceptions=True)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        finally:
            self._render_task = None

    async def _thread_for_session(self, session_id: str) -> discord.Thread:
        binding = await self._require_bindings().by_session(session_id)
        if binding is None:
            raise RenderPermanentError(f"no Discord binding for SDK session {session_id}")
        try:
            channel = self.get_channel(int(binding.thread_id))
            if channel is None:
                channel = await self.fetch_channel(int(binding.thread_id))
        except discord.NotFound as error:
            await self._parent_diagnostic(binding, "bound thread was deleted")
            raise RenderPermanentError(
                f"bound Discord thread was deleted: {binding.thread_id}"
            ) from error
        except discord.Forbidden as error:
            await self._parent_diagnostic(binding, "thread access is forbidden")
            raise RenderPermanentError(
                f"bound Discord thread is inaccessible: {binding.thread_id}"
            ) from error
        if not isinstance(channel, discord.Thread):
            await self._parent_diagnostic(binding, "bound thread is unavailable")
            raise RenderPermanentError(f"bound Discord thread is unavailable: {binding.thread_id}")
        if channel.locked:
            await self._parent_diagnostic(binding, "bound thread is locked")
            raise RenderPermanentError(f"bound Discord thread is locked: {binding.thread_id}")
        if channel.archived:
            try:
                await channel.edit(archived=False)
            except discord.HTTPException as error:
                await self._parent_diagnostic(binding, "archived thread could not be reopened")
                raise _render_delivery_error(error) from error
        return channel

    async def _render_destination(
        self,
        destination_id: str,
    ) -> discord.Thread | discord.TextChannel:
        if destination_id.startswith("thread:"):
            channel_id = destination_id.removeprefix("thread:")
            channel = self.get_channel(int(channel_id))
            if channel is None:
                channel = await self.fetch_channel(int(channel_id))
            if not isinstance(channel, discord.Thread):
                raise RenderPermanentError(f"schedule render thread is unavailable: {channel_id}")
            if channel.archived and not channel.locked:
                await channel.edit(archived=False)
            return channel
        if destination_id.startswith("channel:"):
            channel_id = destination_id.removeprefix("channel:")
            channel = self.get_channel(int(channel_id))
            if channel is None:
                channel = await self.fetch_channel(int(channel_id))
            if isinstance(channel, discord.TextChannel):
                return channel
            raise RenderPermanentError(
                "schedule channel rendering requires a text channel or a "
                "Discord integration hook that supplies a status thread"
            )
        if destination_id == "ops:scheduler":
            raise RenderPermanentError("schedule run has no durable Discord render destination")
        try:
            return await self._thread_for_session(destination_id)
        except RuntimeError as error:
            raise RenderPermanentError(str(error)) from error

    async def _find_thread_for_message(self, message_id: str) -> discord.Thread:
        mapping = await self.database.fetchone(
            """
            SELECT session_id FROM render_messages
            WHERE discord_message_id = ? LIMIT 1
            """,
            (message_id,),
        )
        if mapping is None:
            raise RenderPermanentError(f"Discord message is not mapped to a session: {message_id}")
        return await self._thread_for_session(str(mapping["session_id"]))

    async def _record_session_ui(
        self,
        binding: SessionBinding,
        *,
        parent_channel_id: str | None,
        display_name: str | None,
    ) -> None:
        now = time.time()
        await self.database.execute(
            """
            INSERT INTO session_ui_metadata(
                session_id, thread_id, parent_channel_id, display_name,
                native_name_state, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'unsupported', ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                parent_channel_id = COALESCE(
                    excluded.parent_channel_id,
                    session_ui_metadata.parent_channel_id
                ),
                display_name = COALESCE(
                    excluded.display_name,
                    session_ui_metadata.display_name
                ),
                updated_at = excluded.updated_at
            """,
            (
                binding.sdk_session_id,
                binding.thread_id,
                parent_channel_id,
                display_name,
                now,
                now,
            ),
        )

    async def _parent_diagnostic(
        self,
        binding: SessionBinding,
        reason: str,
    ) -> None:
        metadata = await self.database.fetchone(
            """
            SELECT parent_channel_id FROM session_ui_metadata
            WHERE session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        parent_channel_id = (
            None
            if metadata is None or metadata["parent_channel_id"] is None
            else str(metadata["parent_channel_id"])
        )
        if parent_channel_id is None and binding.project_id is not None:
            project = await self.database.fetchone(
                "SELECT channel_id FROM projects WHERE id = ?",
                (binding.project_id,),
            )
            if project is not None:
                parent_channel_id = str(project["channel_id"])
        key = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{binding.sdk_session_id}:render-diagnostic:{reason}",
            )
        )
        existing = await self.database.fetchone(
            """
            SELECT state FROM render_parent_diagnostics
            WHERE idempotency_key = ?
            """,
            (key,),
        )
        if existing is not None and existing["state"] == "sent":
            return
        now = time.time()
        await self.database.execute(
            """
            INSERT INTO render_parent_diagnostics(
                idempotency_key, session_id, parent_channel_id, reason,
                state, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (key, binding.sdk_session_id, parent_channel_id, reason, now, now),
        )
        if parent_channel_id is None:
            await self.database.execute(
                """
                UPDATE render_parent_diagnostics
                SET state = 'blocked', updated_at = ?
                WHERE idempotency_key = ?
                """,
                (time.time(), key),
            )
            return
        try:
            channel = self.get_channel(int(parent_channel_id))
            if channel is None:
                channel = await self.fetch_channel(int(parent_channel_id))
            if not hasattr(channel, "send"):
                raise TypeError("parent channel cannot receive messages")
            message = await channel.send(
                _bounded_discord_text(
                    (
                        f"copilotD preserved session `{binding.sdk_session_id}`, but rendering "
                        f"to <#{binding.thread_id}> is blocked: {reason}. "
                        "The SDK session remains durable; restore the original thread and resume."
                    ),
                    1800,
                ),
                silent=True,
            )
        except (discord.HTTPException, OSError, TypeError, ValueError) as error:
            await self.database.execute(
                """
                UPDATE render_parent_diagnostics
                SET state = 'blocked', updated_at = ?
                WHERE idempotency_key = ?
                """,
                (time.time(), key),
            )
            await logger.awarning(
                "render_parent_diagnostic_failed",
                session_id=binding.sdk_session_id,
                reason=reason,
                error=str(error),
            )
            return
        await self.database.execute(
            """
            UPDATE render_parent_diagnostics
            SET state = 'sent', discord_message_id = ?, updated_at = ?
            WHERE idempotency_key = ?
            """,
            (str(message.id), time.time(), key),
        )

    async def _run_command(
        self,
        interaction: discord.Interaction,
        name: str,
        operation: CommandOperation,
    ) -> None:
        responder = DiscordInteractionResponder(self, interaction, name=name)
        outcome = await self.command_executor.execute(
            responder,
            CommandInvocation(
                name=name,
                scope="thread" if isinstance(interaction.channel, discord.Thread) else "channel",
                thread_id=(
                    str(interaction.channel.id)
                    if isinstance(interaction.channel, discord.Thread)
                    else None
                ),
                source="discord",
                metadata={"interaction_id": str(interaction.id)},
            ),
            operation,
        )
        if outcome.error is not None:
            await logger.aerror(
                "discord_application_command_failed",
                command=name,
                code=outcome.error.code,
                error=outcome.error.message,
            )

    async def _session_list_projection(self) -> str:
        rows = await self.database.fetchall(
            """
            SELECT b.*, ui.display_name
            FROM session_bindings AS b
            LEFT JOIN session_ui_metadata AS ui
              ON ui.session_id = b.sdk_session_id
            WHERE b.binding_intent != 'deleted'
            ORDER BY b.updated_at DESC LIMIT 30
            """
        )
        if not rows:
            return "No copilotD sessions."
        lines = ["**copilotD sessions**"]
        for row in rows:
            config = _json_object(row["runtime_model_config"])
            if not config:
                config = _json_object(row["desired_model_config"])
            model = config.get("modelId") or "default"
            display = row["display_name"] or f"Session {str(row['sdk_session_id'])[:8]}"
            last_event = (
                "never"
                if row["last_event_at"] is None
                else f"{max(0, int(time.time() - float(row['last_event_at'])))}s ago"
            )
            lines.append(
                f"- <#{row['thread_id']}> **{_bounded_discord_text(str(display), 70)}** · "
                f"`{row['binding_intent']}/{row['attachment_state']}` · "
                f"model `{model}` · last `{last_event}`\n"
                f"  `{row['sdk_session_id']}` · cwd "
                f"`{_bounded_discord_text(str(row['cwd_snapshot']), 100)}`"
            )
        return "\n".join(lines)

    async def _session_info_projection(self, binding: SessionBinding) -> str:
        row = await self.database.fetchone(
            """
            SELECT b.*, ui.display_name, ui.parent_channel_id, ui.native_name_state
            FROM session_bindings AS b
            LEFT JOIN session_ui_metadata AS ui
              ON ui.session_id = b.sdk_session_id
            WHERE b.sdk_session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        if row is None:
            raise CDSessionNotFoundError("session binding disappeared")

        async def state_counts(
            table: str,
            key: str,
            value: str,
        ) -> str:
            if table not in {
                "submissions",
                "message_queue",
                "task_card_projections",
                "schedules",
                "runtime_schedules",
                "liveness_leases",
            }:
                raise ValueError(f"unsupported session-info table: {table}")
            rows = await self.database.fetchall(
                f"""
                SELECT state, COUNT(*) AS count
                FROM {table}
                WHERE {key} = ?
                GROUP BY state
                ORDER BY state
                LIMIT 20
                """,
                (value,),
            )
            if not rows:
                return "none"
            return ", ".join(f"{item['state']}={int(item['count'])}" for item in rows)

        counts: dict[str, int] = {}
        for name, query in (
            (
                "inbox",
                "SELECT COUNT(*) FROM event_journal WHERE sdk_session_id = ?",
            ),
            (
                "outbox",
                "SELECT COUNT(*) FROM render_outbox "
                "WHERE session_id = ? AND state IN ('pending','sending','blocked')",
            ),
        ):
            count = await self.database.fetchone(query, (binding.sdk_session_id,))
            counts[name] = 0 if count is None else int(count[0])

        owner = await self.database.fetchone(
            """
            SELECT owner_id, fence_token, acquired_at, renewed_at, expires_at
            FROM session_owner_leases
            WHERE sdk_session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        context_projection = await self.database.fetchone(
            """
            SELECT runtime_generation, owner_fence_token, source_type, payload_json,
                   observed_at, reconciled_at, stale, stale_reason
            FROM context_projections
            WHERE sdk_session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        usage_projection = await self.database.fetchone(
            """
            SELECT runtime_generation, owner_fence_token, source_type, payload_json,
                   observed_at, reconciled_at, stale, stale_reason
            FROM usage_projections
            WHERE sdk_session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        submission_states = await state_counts(
            "submissions",
            "sdk_session_id",
            binding.sdk_session_id,
        )
        queue_states = await state_counts("message_queue", "thread_id", binding.thread_id)
        task_states = await state_counts(
            "task_card_projections",
            "sdk_session_id",
            binding.sdk_session_id,
        )
        app_schedule_states = await state_counts("schedules", "thread_id", binding.thread_id)
        runtime_schedule_states = await state_counts(
            "runtime_schedules",
            "sdk_session_id",
            binding.sdk_session_id,
        )
        liveness_states = await state_counts(
            "liveness_leases",
            "sdk_session_id",
            binding.sdk_session_id,
        )

        desired_model = _json_object(row["desired_model_config"])
        pending_model = _json_object(row["pending_model_config"])
        runtime_model = _json_object(row["runtime_model_config"])
        now = time.time()
        owner_text = (
            "unavailable (no durable owner lease)"
            if owner is None
            else (
                f"id `{_bounded_discord_text(str(owner['owner_id']), 100)}` · fence "
                f"`{owner['fence_token']}` · expires `{_age_or_future(owner['expires_at'], now)}` "
                f"· renewed `{_age_label(owner['renewed_at'], now)}`"
            )
        )
        context_text = _session_projection_summary(
            context_projection,
            kind="context",
            now=now,
        )
        usage_text = _session_projection_summary(
            usage_projection,
            kind="usage",
            now=now,
        )
        pending_model_text = (
            "none" if row["pending_model_config"] is None else _compact_json(pending_model)
        )
        runtime_model_text = (
            "unknown" if row["runtime_model_config"] is None else _compact_json(runtime_model)
        )
        pending_project_config = row["pending_project_config_version"]
        runtime_project_config = row["runtime_project_config_version"]
        pending_session_config = row["pending_session_config_version"]
        runtime_session_config = row["runtime_session_config_version"]
        native_queue_count = row["native_queue_count"]
        native_steering_count = row["native_steering_count"]
        last_sdk_receive_seq = binding.last_sdk_receive_seq
        lines = [
            f"**{row['display_name'] or 'Copilot session'}**",
            f"SDK session: `{binding.sdk_session_id}`",
            f"Discord: thread <#{binding.thread_id}> · parent "
            f"`{row['parent_channel_id'] or 'unknown'}`",
            f"cwd snapshot: `{binding.cwd_snapshot}` · source `{binding.project_source}`",
            f"binding/attachment: `{binding.binding_intent}/{binding.attachment_state}` · "
            f"reason `{binding.attachment_reason or 'none'}` · runtime generation "
            f"`{binding.runtime_generation}`",
            f"owner: {owner_text}",
            f"mode desired/pending/runtime: `{binding.desired_mode}` / "
            f"`{binding.pending_mode or 'none'}` / `{binding.runtime_mode}`",
            f"model desired/pending/runtime: `{_compact_json(desired_model)}` / "
            f"`{pending_model_text}` / `{runtime_model_text}`",
            f"agent desired/pending/runtime: `{row['desired_agent']}` / "
            f"`{row['pending_agent'] or 'none'}` / `{row['runtime_agent'] or 'unknown'}`",
            f"project config desired/pending/runtime: "
            f"`{row['desired_project_config_version']}` / "
            f"`{pending_project_config if pending_project_config is not None else 'none'}` / "
            f"`{runtime_project_config if runtime_project_config is not None else 'unknown'}`",
            f"session config desired/pending/runtime: "
            f"`{row['desired_session_config_version']}` / "
            f"`{pending_session_config if pending_session_config is not None else 'none'}` / "
            f"`{runtime_session_config if runtime_session_config is not None else 'unknown'}`",
            f"session config hashes desired/pending/runtime: "
            f"`{_short_hash(row['desired_session_config_hash'])}` / "
            f"`{_short_hash(row['pending_session_config_hash'], missing='none')}` / "
            f"`{_short_hash(row['runtime_session_config_hash'])}`",
            f"remote runtime/pending/steerable: `{row['runtime_remote_mode'] or 'unknown'}` / "
            f"`{row['pending_remote_target'] or 'none'}` / "
            f"`{_bool_unknown(row['remote_steerable'])}` · observed "
            f"`{_age_label(row['remote_observed_at'], now)}`",
            f"permission: `{binding.permission_posture}` · verified "
            f"`{_age_label(row['permission_verified_at'], now)}` · managed blocked "
            f"`{bool(row['managed_permissions_blocked'])}` · managed settings "
            f"`{row['managed_settings_state'] or 'unknown'}`",
            f"activity processing/active/abortable: "
            f"`{_bool_unknown(row['runtime_processing'])}` / "
            f"`{_bool_unknown(row['runtime_has_active_work'])}` / "
            f"`{_bool_unknown(row['runtime_abortable'])}` · observed "
            f"`{_age_label(row['activity_observed_at'], now)}`",
            f"submissions: `{submission_states}`",
            f"queue app/native/steering: `{queue_states}` / "
            f"`{native_queue_count if native_queue_count is not None else 'unknown'}` / "
            f"`{native_steering_count if native_steering_count is not None else 'unknown'}` "
            f"· observed `{_age_label(row['queue_observed_at'], now)}`",
            f"tasks: `{task_states}`",
            f"schedules app/runtime: `{app_schedule_states}` / `{runtime_schedule_states}`",
            f"liveness: `{liveness_states}` · render outbox pending `{counts['outbox']}`",
            f"context: {context_text}",
            f"usage: {usage_text}",
            f"reconciliation mode/model/config: `{row['mode_reconciliation_state']}` "
            f"(drift={bool(row['mode_drift'])}) / "
            f"`{row['model_reconciliation_state']}` (drift={bool(row['model_drift'])}, "
            f"confirmed={_compact_json(_json_list(row['model_confirmation_mask']))}) / "
            f"`{row['session_config_state']}` (drift={bool(row['session_config_drift'])})",
            f"snapshot/cursor diagnostics: config `{row['config_snapshot_state']}` · "
            f"cursor `{row['cursor_status'] or 'unknown'}` · inbox `{binding.last_inbox_seq}` "
            f"· SDK receive "
            f"`{last_sdk_receive_seq if last_sdk_receive_seq is not None else 'none'}`",
            f"freshness: last event `{_age_label(row['last_event_at'], now)}` · "
            f"binding updated `{_age_label(row['updated_at'], now)}` · inbox rows "
            f"`{counts['inbox']}` · native name `{row['native_name_state'] or 'unknown'}`",
        ]
        return _bounded_discord_text("\n".join(lines), 16_000)

    async def _project_info_projection(self, channel_id: str) -> str:
        snapshot = await self._require_projects().resolve(channel_id)
        layout, mention_required, channel_version = await self._require_projects().channel_settings(
            channel_id
        )
        resident = await self.database.fetchall(
            """
            SELECT thread_id, sdk_session_id, binding_intent, attachment_state
            FROM session_bindings
            WHERE project_id IS ? OR (
                ? IS NULL AND project_source = 'implicit-home'
            )
            ORDER BY updated_at DESC LIMIT 20
            """,
            (snapshot.project_id, snapshot.project_id),
        )
        lines = [
            f"source: `{snapshot.source}`",
            f"cwd: `{snapshot.cwd}`",
            f"project config version: `{snapshot.config_version}`",
            f"channel layout: `{layout}` · mention required: `{mention_required}` "
            f"· channel config version `{channel_version}`",
            f"resident sessions: `{len(resident)}`",
        ]
        lines.extend(
            (
                f"- <#{item['thread_id']}> `{item['sdk_session_id']}` · "
                f"`{item['binding_intent']}/{item['attachment_state']}`"
            )
            for item in resident
        )
        return "\n".join(lines)

    def _register_application_commands(self) -> None:
        if self._commands_registered:
            return
        self._commands_registered = True
        session = app_commands.Group(name="session", description="Manage Copilot sessions")
        project = app_commands.Group(
            name="project",
            description="Manage channel projects",
        )
        model = app_commands.Group(name="model", description="Inspect or change Copilot models")
        queue = app_commands.Group(name="queue", description="Manage the durable message queue")
        schedule = app_commands.Group(name="schedule", description="Manage app-owned schedules")
        ops = app_commands.Group(name="ops", description="Inspect copilotD operations")
        worktree = app_commands.Group(
            name="worktree",
            description="Manage project Git worktrees",
        )
        variable = app_commands.Group(
            name="variable",
            description="Manage future-session project variables",
        )
        mcp = app_commands.Group(
            name="mcp",
            description="Manage typed future-session MCP servers",
        )
        skill = app_commands.Group(
            name="skill",
            description="Manage future-session skill directories",
        )
        plugin = app_commands.Group(
            name="plugin",
            description="Manage future-session plugin directories",
        )
        custom_agent = app_commands.Group(
            name="agent",
            description="Manage future-session custom agents",
        )

        @session.command(name="new", description="Create a new Copilot session thread")
        async def session_new(interaction: discord.Interaction, prompt: str = "") -> None:
            async def operation(_: CommandInvocation) -> str:
                channel_id = _parent_channel_id(interaction)
                display_name = _thread_name(prompt or "New Copilot session")
                runtime = await self._require_creation().create_from_source(
                    channel_id=channel_id,
                    source_kind="slash",
                    source_id=str(interaction.id),
                    prompt=prompt or "Start a new interactive Copilot session.",
                    thread_name=display_name,
                    send_initial_prompt=bool(prompt),
                )
                await self._record_session_ui(
                    runtime.binding,
                    parent_channel_id=channel_id,
                    display_name=display_name,
                )
                return f"Session created: <#{runtime.binding.thread_id}>"

            await self._run_command(interaction, "session new", operation)

        @session.command(name="list", description="List copilotD session bindings")
        async def session_list(interaction: discord.Interaction) -> None:
            await self._run_command(
                interaction,
                "session list",
                lambda _: self._session_list_projection(),
            )

        @session.command(name="info", description="Show the current session state")
        async def session_info(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                return await self._session_info_projection(
                    await self._interaction_binding(interaction)
                )

            await self._run_command(interaction, "session info", operation)

        @session.command(name="abort", description="Abort the current Copilot turn")
        async def session_abort(
            interaction: discord.Interaction,
            clear_local_queue: bool = True,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                runtime = await self._interaction_runtime(interaction)
                removed = await runtime.clear_queue() if clear_local_queue else 0
                await runtime.abort(idempotency_key=f"interaction:{interaction.id}")
                return f"Abort requested; cancelled {removed} local queue item(s)."

            await self._run_command(interaction, "session abort", operation)

        @session.command(name="close", description="Close without deleting Copilot history")
        async def session_close(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                runtime = await self._interaction_runtime(interaction)
                await runtime.close(
                    idempotency_key=f"interaction:{interaction.id}",
                    force=force,
                )
                return "Session closed."

            await self._run_command(interaction, "session close", operation)

        @session.command(name="delete", description="Permanently delete a Copilot session")
        async def session_delete(
            interaction: discord.Interaction,
            session_id: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                if isinstance(interaction.channel, discord.Thread):
                    binding = await self._require_bindings().by_thread(str(interaction.channel.id))
                    if binding is None:
                        raise CDSessionNotFoundError(
                            "this thread is not bound to a Copilot session"
                        )
                    if session_id is not None and session_id != binding.sdk_session_id:
                        raise CDConflictError("this thread cannot delete another Copilot session")
                else:
                    if session_id is None:
                        raise CDInputError("session_id is required outside a session thread")
                    binding = await self._require_bindings().by_session(session_id)
                    if binding is None:
                        raise CDSessionNotFoundError("the requested copilotD session is unknown")
                await self._require_deletions().delete(
                    binding,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                return "Session permanently deleted."

            await self._run_command(interaction, "session delete", operation)

        @session.command(name="resume", description="Resume this thread's original session")
        async def session_resume(
            interaction: discord.Interaction,
            session_id: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                if isinstance(interaction.channel, discord.Thread):
                    binding = await self._interaction_binding(
                        interaction,
                        allow_closed=True,
                    )
                    if session_id is not None and session_id != binding.sdk_session_id:
                        raise CDConflictError(
                            "this thread cannot be rebound to another Copilot session"
                        )
                else:
                    if session_id is None:
                        raise CDInputError("session_id is required outside a session thread")
                    binding = await self._require_bindings().by_session(session_id)
                    if binding is None:
                        raise CDSessionNotFoundError("the requested copilotD session is unknown")
                await self._require_sessions().ensure_attached(
                    binding,
                    reactivate=True,
                )
                thread = await self._thread_for_session(binding.sdk_session_id)
                return f"Session resumed in its original thread: {thread.mention}"

            await self._run_command(interaction, "session resume", operation)

        @session.command(name="rename", description="Rename this Discord session thread")
        async def session_rename(interaction: discord.Interaction, name: str) -> None:
            async def operation(_: CommandInvocation) -> str:
                binding = await self._interaction_binding(
                    interaction,
                    allow_closed=True,
                )
                if not isinstance(interaction.channel, discord.Thread):
                    raise CDScopeError("this command must be used inside a session thread")
                normalized = " ".join(name.split())
                if not normalized:
                    raise CDInputError("session name cannot be empty")
                normalized = normalized[:100]
                await self._record_session_ui(
                    binding,
                    parent_channel_id=str(interaction.channel.parent_id),
                    display_name=normalized,
                )
                await interaction.channel.edit(name=normalized)
                native_state = "unsupported"
                adapter = self.session_naming_adapter
                if adapter is not None:
                    try:
                        await adapter.rename_app_session(
                            thread_id=binding.thread_id,
                            name=normalized,
                        )
                        native_state = (
                            "confirmed"
                            if await adapter.rename_native_session(
                                session_id=binding.sdk_session_id,
                                name=normalized,
                            )
                            else "unsupported"
                        )
                    except Exception as error:
                        native_state = f"best-effort-failed:{type(error).__name__}"
                        await logger.awarning(
                            "session_native_rename_failed",
                            session_id=binding.sdk_session_id,
                            error=str(error),
                        )
                await self.database.execute(
                    """
                    UPDATE session_ui_metadata
                    SET native_name_state = ?, updated_at = ?
                    WHERE session_id = ?
                    """,
                    (native_state, time.time(), binding.sdk_session_id),
                )
                return f"Session renamed; native metadata state: `{native_state}`."

            await self._run_command(interaction, "session rename", operation)

        @project.command(name="bind", description="Bind future sessions to a local directory")
        @app_commands.choices(
            layout=[
                app_commands.Choice(name="text", value="text"),
                app_commands.Choice(name="forum", value="forum"),
            ]
        )
        async def project_bind(
            interaction: discord.Interaction,
            path: str,
            layout: app_commands.Choice[str] | None = None,
            mention_required: bool | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                channel_id = _parent_channel_id(interaction)
                snapshot = await self._require_projects().bind(channel_id, Path(path))
                if layout is not None:
                    await self._require_projects().set_layout(channel_id, layout.value)
                if mention_required is not None:
                    await self._require_projects().set_mention_required(
                        channel_id,
                        mention_required,
                    )
                (
                    configured_layout,
                    configured_mention,
                    _,
                ) = await self._require_projects().channel_settings(channel_id)
                return (
                    f"Future sessions use `{snapshot.cwd}` · layout "
                    f"`{configured_layout}` · mention required `{configured_mention}`."
                )

            await self._run_command(interaction, "project bind", operation)

        @project.command(name="unbind", description="Return future sessions to HOME")
        async def project_unbind(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                snapshot = await self._require_projects().unbind(_parent_channel_id(interaction))
                return f"Future sessions use implicit HOME `{snapshot.cwd}`."

            await self._run_command(interaction, "project unbind", operation)

        @project.command(name="info", description="Show the channel project resolution")
        async def project_info(interaction: discord.Interaction) -> None:
            await self._run_command(
                interaction,
                "project info",
                lambda _: self._project_info_projection(_parent_channel_id(interaction)),
            )

        @project.command(name="timezone", description="Set the project IANA timezone")
        async def project_timezone(
            interaction: discord.Interaction,
            value: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                await self._require_project_commands().set_timezone(
                    _parent_channel_id(interaction),
                    value,
                )
                return f"Project timezone is `{value}`."

            await self._run_command(interaction, "project timezone", operation)

        @project.command(
            name="config-reload",
            description="Publish .copilotd/extensions.json and reattach this session",
        )
        async def project_config_reload(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                runtime = await self._interaction_runtime(interaction)
                binding = runtime.binding
                project_snapshot = ProjectSnapshot(
                    project_id=binding.project_id,
                    channel_id=f"session:{binding.thread_id}",
                    source=(
                        ProjectSource.EXPLICIT
                        if binding.project_id is not None
                        else ProjectSource.IMPLICIT_HOME
                    ),
                    root_path=binding.cwd_snapshot,
                    cwd=binding.cwd_snapshot,
                    config_version=binding.desired_project_config_version,
                )
                config = await self.extension_config_source.load(project_snapshot)
                snapshot = await runtime.reload_extension_config(
                    idempotency_key=f"interaction:{interaction.id}",
                    config=config,
                )
                return (
                    f"Extension config `{snapshot.version}` reattached "
                    f"(`{snapshot.config_hash[:12]}`)."
                )

            await self._run_command(interaction, "project config-reload", operation)

        async def remove_project_variable(
            interaction: discord.Interaction,
            name: str,
        ) -> str:
            project_snapshot = await self._require_projects().resolve(
                _parent_channel_id(interaction)
            )
            version = await self._require_project_commands().variable_remove(
                project_snapshot.project_id,
                name,
            )
            return f"Variable `{name}` removed in project config version `{version}`."

        @variable.command(name="remove", description="Remove a future-session variable")
        async def project_variable_remove(
            interaction: discord.Interaction,
            name: str,
        ) -> None:
            await self._run_command(
                interaction,
                "project variable remove",
                lambda _: remove_project_variable(interaction, name),
            )

        worktree_history_choices = [app_commands.Choice(name="none", value="none")]
        if self.worktree_commands is not None and self.worktree_commands.history_fork_available:
            worktree_history_choices.append(app_commands.Choice(name="fork", value="fork"))

        @worktree.command(name="create", description="Create a managed Git worktree")
        @app_commands.choices(history=worktree_history_choices)
        async def project_worktree_create(
            interaction: discord.Interaction,
            name: str,
            base: str = "HEAD",
            history: app_commands.Choice[str] | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                project_snapshot = await self._require_projects().resolve(
                    _parent_channel_id(interaction)
                )
                source_session_id = None
                if isinstance(interaction.channel, discord.Thread):
                    source_session_id = (
                        await self._interaction_binding(interaction)
                    ).sdk_session_id
                projection = await self._require_worktree_commands().create(
                    project_id=project_snapshot.project_id,
                    name=name,
                    base_ref=base,
                    history="none" if history is None else history.value,
                    source_session_id=source_session_id,
                )
                return (
                    f"Worktree `{projection.name}` ready at `{projection.path}` "
                    f"on `{projection.branch_name}`"
                    + ("" if projection.thread_id is None else f" in <#{projection.thread_id}>")
                )

            await self._run_command(interaction, "project worktree create", operation)

        @worktree.command(name="list", description="List managed Git worktrees")
        async def project_worktree_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                project_snapshot = await self._require_projects().resolve(
                    _parent_channel_id(interaction)
                )
                items = await self._require_worktree_commands().list(project_snapshot.project_id)
                if not items:
                    return "No managed worktrees."
                return "\n".join(
                    f"`{item.name}` · `{item.state}` · `{item.branch_name}` · "
                    f"sessions `{item.session_count}` · schedules `{item.schedule_count}` · "
                    f"`{item.path}`"
                    for item in items
                )

            await self._run_command(interaction, "project worktree list", operation)

        @worktree.command(name="close", description="Close a managed Git worktree")
        async def project_worktree_close(
            interaction: discord.Interaction,
            name: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                project_snapshot = await self._require_projects().resolve(
                    _parent_channel_id(interaction)
                )
                projection = await self._require_worktree_commands().close(
                    project_snapshot.project_id,
                    name=name,
                )
                return (
                    f"Worktree `{projection.name}` closed; branch "
                    f"`{projection.branch_name}` was preserved."
                )

            await self._run_command(interaction, "project worktree close", operation)

        @project.command(name="layout", description="Set future Discord thread organization")
        @app_commands.choices(
            value=[
                app_commands.Choice(name="text", value="text"),
                app_commands.Choice(name="forum", value="forum"),
            ]
        )
        async def project_layout(
            interaction: discord.Interaction,
            value: app_commands.Choice[str],
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                await self._require_projects().set_layout(
                    _parent_channel_id(interaction),
                    value.value,
                )
                return f"Future project layout is `{value.value}`."

            await self._run_command(interaction, "project layout", operation)

        @project.command(name="mention", description="Set the channel mention trigger")
        async def project_mention(
            interaction: discord.Interaction,
            required: bool,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                await self._require_projects().set_mention_required(
                    _parent_channel_id(interaction),
                    required,
                )
                return f"Mention required is `{required}`."

            await self._run_command(interaction, "project mention", operation)

        @variable.command(name="set", description="Set a future-session environment variable")
        async def project_variable_set(
            interaction: discord.Interaction,
            name: str,
            value: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                entry = await self._require_projects().set_project_env(
                    _parent_channel_id(interaction),
                    name,
                    value,
                )
                return (
                    f"Variable `{entry.name}` saved at project config version "
                    f"`{entry.project_config_version}`."
                )

            await self._run_command(interaction, "project variable set", operation)

        @variable.command(name="unset", description="Remove a future-session variable")
        async def project_variable_unset(
            interaction: discord.Interaction,
            name: str,
        ) -> None:
            await self._run_command(
                interaction,
                "project variable unset",
                lambda _: remove_project_variable(interaction, name),
            )

        @variable.command(name="list", description="List project environment variables")
        async def project_variable_list(
            interaction: discord.Interaction,
            reveal: bool = False,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                entries = await self._require_projects().list_project_env(
                    _parent_channel_id(interaction),
                    reveal=reveal,
                )
                if not entries:
                    return "No project environment variables."
                return "\n".join(f"`{entry.name}` = `{entry.value}`" for entry in entries)

            await self._run_command(interaction, "project variable list", operation)

        @mcp.command(name="list", description="List future-session MCP servers")
        async def project_mcp_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                entries = await self._require_projects().list_mcp_servers(
                    _parent_channel_id(interaction)
                )
                if not entries:
                    return "No project MCP servers."
                return "\n".join(
                    f"`{entry.name}` · `{entry.transport}` · enabled `{entry.enabled}` · "
                    f"version `{entry.server_version}` · "
                    f"`{json.dumps(dict(entry.config), sort_keys=True)}`"
                    for entry in entries
                )

            await self._run_command(interaction, "project mcp list", operation)

        @mcp.command(name="add", description="Add or replace a future-session MCP server")
        @app_commands.choices(
            transport=[
                app_commands.Choice(name="stdio", value="stdio"),
                app_commands.Choice(name="http", value="http"),
            ]
        )
        async def project_mcp_add(
            interaction: discord.Interaction,
            name: str,
            transport: app_commands.Choice[str],
            command_or_url: str,
            args_json: str = "[]",
            headers_json: str = "{}",
            project_env_refs: str = "",
            enabled: bool = True,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                config: dict[str, Any]
                if transport.value == "stdio":
                    config = {
                        "command": command_or_url,
                        "args": _parse_json_list(args_json, field="args_json"),
                    }
                    references = [
                        value.strip() for value in project_env_refs.split(",") if value.strip()
                    ]
                    if references:
                        config["project_env_refs"] = references
                else:
                    if project_env_refs.strip():
                        raise CDInputError(
                            "project_env_refs are supported only for stdio MCP servers"
                        )
                    config = {
                        "url": command_or_url,
                        "headers": _parse_json_object(
                            headers_json,
                            field="headers_json",
                        ),
                    }
                entry = await self._require_projects().set_mcp_server(
                    _parent_channel_id(interaction),
                    name=name,
                    transport=transport.value,
                    config=config,
                    enabled=enabled,
                )
                return (
                    f"MCP server `{entry.name}` saved for future sessions at config "
                    f"version `{entry.project_config_version}`."
                )

            await self._run_command(interaction, "project mcp add", operation)

        @mcp.command(name="toggle", description="Enable or disable an MCP server")
        async def project_mcp_toggle(
            interaction: discord.Interaction,
            name: str,
            enabled: bool,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                entry = await self._require_projects().toggle_mcp_server(
                    _parent_channel_id(interaction),
                    name=name,
                    enabled=enabled,
                )
                return f"MCP server `{entry.name}` enabled is `{entry.enabled}`."

            await self._run_command(interaction, "project mcp toggle", operation)

        @mcp.command(name="remove", description="Remove an MCP server")
        async def project_mcp_remove(
            interaction: discord.Interaction,
            name: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                removed = await self._require_projects().remove_mcp_server(
                    _parent_channel_id(interaction),
                    name=name,
                )
                if not removed:
                    raise CDInputError(f"MCP server not found: {name}")
                return f"MCP server `{name}` removed."

            await self._run_command(interaction, "project mcp remove", operation)

        def register_directory_commands(
            group: app_commands.Group,
            kind: str,
            list_method: Callable[[str], Awaitable[list[Any]]],
            set_method: Callable[..., Awaitable[Any]],
            toggle_method: Callable[..., Awaitable[Any]],
            remove_method: Callable[..., Awaitable[bool]],
        ) -> None:
            @group.command(name="list", description=f"List future-session {kind} directories")
            async def directory_list(interaction: discord.Interaction) -> None:
                async def operation(_: CommandInvocation) -> str:
                    entries = await list_method(_parent_channel_id(interaction))
                    if not entries:
                        return f"No project {kind} directories."
                    return "\n".join(
                        f"`{entry.path}` · enabled `{entry.enabled}`" for entry in entries
                    )

                await self._run_command(interaction, f"project {kind} list", operation)

            @group.command(name="add", description=f"Add a future-session {kind} directory")
            async def directory_add(
                interaction: discord.Interaction,
                path: str,
                enabled: bool = True,
            ) -> None:
                async def operation(_: CommandInvocation) -> str:
                    entry = await set_method(
                        _parent_channel_id(interaction),
                        path=path,
                        enabled=enabled,
                    )
                    return f"{kind.title()} directory `{entry.path}` saved."

                await self._run_command(interaction, f"project {kind} add", operation)

            @group.command(name="toggle", description=f"Toggle a {kind} directory")
            async def directory_toggle(
                interaction: discord.Interaction,
                path: str,
                enabled: bool,
            ) -> None:
                async def operation(_: CommandInvocation) -> str:
                    entry = await toggle_method(
                        _parent_channel_id(interaction),
                        path=path,
                        enabled=enabled,
                    )
                    return f"{kind.title()} directory `{entry.path}` enabled is `{entry.enabled}`."

                await self._run_command(interaction, f"project {kind} toggle", operation)

            @group.command(name="remove", description=f"Remove a {kind} directory")
            async def directory_remove(
                interaction: discord.Interaction,
                path: str,
            ) -> None:
                async def operation(_: CommandInvocation) -> str:
                    removed = await remove_method(
                        _parent_channel_id(interaction),
                        path=path,
                    )
                    if not removed:
                        raise CDInputError(f"{kind} directory not found: {path}")
                    return f"{kind.title()} directory `{path}` removed."

                await self._run_command(interaction, f"project {kind} remove", operation)

        projects = self._require_projects if self.projects is not None else None
        if projects is not None:
            registry = projects()
            register_directory_commands(
                skill,
                "skill",
                registry.list_skill_dirs,
                registry.set_skill_dir,
                registry.toggle_skill_dir,
                registry.remove_skill_dir,
            )
            register_directory_commands(
                plugin,
                "plugin",
                registry.list_plugin_dirs,
                registry.set_plugin_dir,
                registry.toggle_plugin_dir,
                registry.remove_plugin_dir,
            )
        else:
            register_directory_commands(
                skill,
                "skill",
                lambda channel_id: self._require_projects().list_skill_dirs(channel_id),
                self._deferred_set_skill_dir,
                self._deferred_toggle_skill_dir,
                self._deferred_remove_skill_dir,
            )
            register_directory_commands(
                plugin,
                "plugin",
                lambda channel_id: self._require_projects().list_plugin_dirs(channel_id),
                self._deferred_set_plugin_dir,
                self._deferred_toggle_plugin_dir,
                self._deferred_remove_plugin_dir,
            )

        @custom_agent.command(name="list", description="List future-session custom agents")
        async def project_agent_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                entries = await self._require_projects().list_custom_agents(
                    _parent_channel_id(interaction)
                )
                if not entries:
                    return "No project custom agents."
                return "\n".join(
                    f"`{entry.name}` · enabled `{entry.enabled}` · "
                    f"tools `{', '.join(entry.tools) or 'none'}` · "
                    f"{_bounded_discord_text(entry.description, 120)}"
                    for entry in entries
                )

            await self._run_command(interaction, "project agent list", operation)

        @custom_agent.command(name="add", description="Add a future-session custom agent")
        async def project_agent_add(
            interaction: discord.Interaction,
            name: str,
            description: str,
            prompt: str,
            tools: str = "",
            enabled: bool = True,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                entry = await self._require_projects().set_custom_agent(
                    _parent_channel_id(interaction),
                    name=name,
                    description=description,
                    prompt=prompt,
                    tools=tuple(value.strip() for value in tools.split(",") if value.strip()),
                    enabled=enabled,
                )
                return f"Custom agent `{entry.name}` saved for future sessions."

            await self._run_command(interaction, "project agent add", operation)

        @custom_agent.command(name="toggle", description="Enable or disable a custom agent")
        async def project_agent_toggle(
            interaction: discord.Interaction,
            name: str,
            enabled: bool,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                entry = await self._require_projects().toggle_custom_agent(
                    _parent_channel_id(interaction),
                    name=name,
                    enabled=enabled,
                )
                return f"Custom agent `{entry.name}` enabled is `{entry.enabled}`."

            await self._run_command(interaction, "project agent toggle", operation)

        @custom_agent.command(name="remove", description="Remove a custom agent")
        async def project_agent_remove(
            interaction: discord.Interaction,
            name: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                removed = await self._require_projects().remove_custom_agent(
                    _parent_channel_id(interaction),
                    name=name,
                )
                if not removed:
                    raise CDInputError(f"custom agent not found: {name}")
                return f"Custom agent `{name}` removed."

            await self._run_command(interaction, "project agent remove", operation)

        @model.command(name="list", description="List models available to this Copilot account")
        async def model_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                models = await self.bridge.list_models()
                lines = ["Available Copilot models:"]
                for item in models:
                    supports = (item.get("capabilities") or {}).get("supports") or {}
                    billing = item.get("billing") or {}
                    features = [
                        name
                        for name, enabled in (
                            ("vision", supports.get("vision")),
                            ("reasoning", supports.get("reasoningEffort")),
                            ("long-context", supports.get("longContext")),
                            (
                                "reasoning-summary",
                                self.model_summary_adapter is not None
                                and self.model_summary_adapter.supports_reasoning_summary(
                                    str(item["id"])
                                ),
                            ),
                        )
                        if enabled
                    ]
                    multiplier = billing.get("multiplier")
                    suffix = (
                        f"; multiplier {multiplier:g}"
                        if isinstance(multiplier, int | float)
                        else ""
                    )
                    lines.append(
                        f"- `{item['id']}` — {item['name']}"
                        f" ({', '.join(features) or 'standard'}{suffix})"
                    )
                return "\n".join(lines)

            await self._run_command(interaction, "model list", operation)

        async def set_model_operation(
            interaction: discord.Interaction,
            *,
            model_id: str,
            effort: str | None,
            context_tier: app_commands.Choice[str] | None,
            reasoning_summary: str | None,
        ) -> str:
            runtime = await self._interaction_runtime(interaction)
            observed = await runtime.set_model(
                model_id,
                reasoning_effort=effort,
                reasoning_summary=reasoning_summary,
                context_tier=None if context_tier is None else context_tier.value,
                idempotency_key=f"interaction:{interaction.id}",
            )
            return (
                "Model confirmed: "
                f"`{observed.get('modelId')}`"
                f", effort `{observed.get('reasoningEffort') or 'default'}`"
                f", reasoning summary "
                f"`{observed.get('reasoningSummary') or 'default'}`"
                f", context `{observed.get('contextTier') or 'default'}`."
            )

        context_choices = [
            app_commands.Choice(name="default", value="default"),
            app_commands.Choice(name="long context", value="long_context"),
        ]
        if self.model_summary_adapter is None:

            @model.command(name="set", description="Set the model for future messages")
            @app_commands.choices(context_tier=context_choices)
            async def model_set(
                interaction: discord.Interaction,
                model_id: str,
                effort: str | None = None,
                context_tier: app_commands.Choice[str] | None = None,
            ) -> None:
                async def operation(_: CommandInvocation) -> str:
                    return await set_model_operation(
                        interaction,
                        model_id=model_id,
                        effort=effort,
                        context_tier=context_tier,
                        reasoning_summary=None,
                    )

                await self._run_command(interaction, "model set", operation)
        else:

            @model.command(name="set", description="Set the model for future messages")
            @app_commands.choices(context_tier=context_choices)
            async def model_set_with_summary(
                interaction: discord.Interaction,
                model_id: str,
                effort: str | None = None,
                reasoning_summary: str | None = None,
                context_tier: app_commands.Choice[str] | None = None,
            ) -> None:
                async def operation(_: CommandInvocation) -> str:
                    return await set_model_operation(
                        interaction,
                        model_id=model_id,
                        effort=effort,
                        context_tier=context_tier,
                        reasoning_summary=reasoning_summary,
                    )

                await self._run_command(interaction, "model set", operation)

        @self.tree.command(name="context", description="Show current Copilot context usage")
        async def context(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                snapshot = await (await self._interaction_runtime(interaction)).context_snapshot()
                if snapshot is None:
                    return "Copilot context information is currently unavailable."
                total = int(snapshot.get("totalTokens", 0))
                limit = int(snapshot.get("limit", 0))
                percent = 0 if limit <= 0 else total * 100 / limit
                freshness = _freshness_label(snapshot)
                return (
                    f"status: `{freshness}`\n"
                    f"model: `{snapshot.get('modelName', 'unknown')}`\n"
                    f"context: `{total:,}` / `{limit:,}` tokens ({percent:.1f}%)\n"
                    f"conversation: `{int(snapshot.get('conversationTokens', 0)):,}`\n"
                    f"system: `{int(snapshot.get('systemTokens', 0)):,}`\n"
                    f"tools: `{int(snapshot.get('toolDefinitionsTokens', 0)):,}`\n"
                    f"compaction threshold: "
                    f"`{int(snapshot.get('compactionThreshold', 0)):,}`"
                )

            await self._run_command(interaction, "context", operation)

        @self.tree.command(name="usage", description="Show Copilot session usage")
        async def usage(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                snapshot = await (await self._interaction_runtime(interaction)).usage_snapshot()
                return (
                    f"status: `{_freshness_label(snapshot)}`\n"
                    f"model: `{snapshot.get('currentModel') or 'unknown'}`\n"
                    f"user requests: `{int(snapshot.get('totalUserRequests', 0)):,}`\n"
                    f"last call: `{int(snapshot.get('lastCallInputTokens', 0)):,}` input / "
                    f"`{int(snapshot.get('lastCallOutputTokens', 0)):,}` output tokens\n"
                    f"premium request units: "
                    f"`{float(snapshot.get('totalPremiumRequestCost', 0)):.3f}`\n"
                    f"AI credits: `{float(snapshot.get('aiCredits') or 0):.3f}`\n"
                    f"nano-AIU: `{float(snapshot.get('totalNanoAiu') or 0):.3f}`"
                )

            await self._run_command(interaction, "usage", operation)

        @self.tree.command(name="autopilot", description="Enter or leave Copilot Autopilot mode")
        async def autopilot(
            interaction: discord.Interaction,
            enabled: bool = True,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                runtime = await self._interaction_runtime(interaction)
                mode = "autopilot" if enabled else "interactive"
                await runtime.set_mode(
                    mode,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                return f"Mode is now `{mode}`."

            await self._run_command(interaction, "autopilot", operation)

        @self.tree.command(name="plan", description="Enter, exit, or submit in Plan mode")
        @app_commands.choices(
            action=[
                app_commands.Choice(name="enter", value="enter"),
                app_commands.Choice(name="exit", value="exit"),
            ]
        )
        async def plan(
            interaction: discord.Interaction,
            action: app_commands.Choice[str] | None = None,
            prompt: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                runtime = await self._interaction_runtime(interaction)
                selected = "enter" if action is None else action.value
                mode = "plan" if selected == "enter" else "interactive"
                await runtime.set_mode(
                    mode,
                    idempotency_key=f"interaction:{interaction.id}:mode",
                )
                if selected == "enter" and prompt:
                    await runtime.send(
                        prompt,
                        idempotency_key=f"interaction:{interaction.id}:prompt",
                        agent_mode="plan",
                        origin="plan",
                    )
                return f"Mode is now `{mode}`."

            await self._run_command(interaction, "plan", operation)

        @self.tree.command(name="steer", description="Steer the currently active Copilot turn")
        async def steer(interaction: discord.Interaction, text: str) -> None:
            async def operation(_: CommandInvocation) -> str:
                await (await self._interaction_runtime(interaction)).steer(
                    text,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                return "Steer submitted."

            await self._run_command(interaction, "steer", operation)

        @queue.command(name="add", description="Add a prompt to this session's durable queue")
        async def queue_add(interaction: discord.Interaction, text: str) -> None:
            async def operation(_: CommandInvocation) -> str:
                reference = await (await self._interaction_runtime(interaction)).send(
                    text,
                    idempotency_key=f"interaction:{interaction.id}:queue",
                    origin="queue",
                )
                return f"Prompt persisted as `{reference}`."

            await self._run_command(interaction, "queue add", operation)

        @queue.command(name="list", description="List pending durable prompts")
        async def queue_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                items = await (await self._interaction_runtime(interaction)).queue_items()
                if not items:
                    return "The durable queue is empty."
                lines = []
                for item in items[:30]:
                    origin = str(item["origin"])
                    if self.schedule_origin_adapter is not None:
                        origin = self.schedule_origin_adapter.describe_origin(
                            origin=origin,
                            schedule_run_id=item.get("schedule_run_id"),
                        )
                    elif item.get("schedule_run_id"):
                        origin = f"{origin}:schedule-run:{item['schedule_run_id']}"
                    replacement = (
                        ""
                        if item.get("replaces_id") is None
                        else f" · replaces `{item['replaces_id']}`"
                    )
                    lines.append(
                        f"`{item['id']}` · `{item['state']}` · `{origin}`{replacement}\n"
                        f"{_bounded_discord_text(str(item['prompt']), 140)}"
                    )
                if len(items) > 30:
                    lines.append(f"… and {len(items) - 30} more")
                return "\n".join(lines)

            await self._run_command(interaction, "queue list", operation)

        @queue.command(name="remove", description="Cancel one prompt before SDK submission")
        async def queue_remove(interaction: discord.Interaction, item_id: str) -> None:
            async def operation(_: CommandInvocation) -> str:
                removed = await (await self._interaction_runtime(interaction)).cancel_queue_item(
                    item_id
                )
                if not removed:
                    raise CDSessionStateError("queue item is not cancellable")
                return "Queue item cancelled."

            await self._run_command(interaction, "queue remove", operation)

        @queue.command(
            name="resubmit",
            description="Copy a configuration-drift item to the queue tail",
        )
        async def queue_resubmit(
            interaction: discord.Interaction,
            item_id: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                replacement = await (
                    await self._interaction_runtime(interaction)
                ).resubmit_queue_item(
                    item_id,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                return (
                    f"Queue item `{item_id}` was retained as cancelled; replacement "
                    f"`{replacement}` uses the current confirmed configuration."
                )

            await self._run_command(interaction, "queue resubmit", operation)

        @queue.command(name="clear", description="Cancel all prompts not submitted to the SDK")
        async def queue_clear(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                removed = await (await self._interaction_runtime(interaction)).clear_queue()
                return f"Cancelled {removed} queued prompt(s)."

            await self._run_command(interaction, "queue clear", operation)

        @ops.command(name="health", description="Show bounded copilotD health")
        async def ops_health(interaction: discord.Interaction) -> None:
            await self._run_command(
                interaction,
                "ops health",
                lambda _: _json_text_async(self.ops_service.health()),
            )

        @ops.command(name="diagnostics", description="Show session/runtime diagnostics")
        async def ops_diagnostics(
            interaction: discord.Interaction,
            session_id: str | None = None,
        ) -> None:
            await self._run_command(
                interaction,
                "ops diagnostics",
                lambda _: _json_text_async(self.ops_service.diagnostics(session_id=session_id)),
            )

        @ops.command(name="debug", description="Enable bounded temporary debug metadata")
        @app_commands.choices(
            level=[
                app_commands.Choice(name="info", value="info"),
                app_commands.Choice(name="debug", value="debug"),
                app_commands.Choice(name="trace", value="trace"),
            ]
        )
        async def ops_debug(
            interaction: discord.Interaction,
            level: app_commands.Choice[str],
            duration_minutes: app_commands.Range[int, 1, 30] = 10,
        ) -> None:
            await self._run_command(
                interaction,
                "ops debug",
                lambda _: _json_text_async(
                    self.ops_service.debug(
                        level=level.value,
                        duration_minutes=int(duration_minutes),
                    )
                ),
            )

        @ops.command(name="log-tail", description="Dump a bounded local log tail")
        async def ops_log_tail(
            interaction: discord.Interaction,
            correlation_id: str | None = None,
        ) -> None:
            await self._run_command(
                interaction,
                "ops log-tail",
                lambda _: _json_text_async(
                    self.ops_service.log_tail(correlation_id=correlation_id)
                ),
            )

        @ops.command(
            name="log-dump",
            description="Attach bounded redacted logs and the durable event timeline",
        )
        async def ops_log_dump(
            interaction: discord.Interaction,
            correlation_id: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> CommandResponse:
                text = await _json_text_async(
                    self.ops_service.log_dump(correlation_id=correlation_id)
                )
                return CommandResponse(
                    content="Bounded redacted diagnostic log dump attached.",
                    attachment=text.encode("utf-8"),
                    filename="copilotd-log-dump.json",
                )

            await self._run_command(interaction, "ops log-dump", operation)

        @ops.command(name="event-dump", description="Dump a bounded durable event timeline")
        async def ops_event_dump(
            interaction: discord.Interaction,
            session_id: str | None = None,
        ) -> None:
            await self._run_command(
                interaction,
                "ops event-dump",
                lambda _: _json_text_async(self.ops_service.event_dump(session_id=session_id)),
            )

        async def ask_copilot(
            interaction: discord.Interaction,
            target: discord.Message,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                channel_id = _message_parent_channel_id(target)
                content = target.content.strip() or "(message contains attachments only)"
                provenance = (
                    f"Discord message by {target.author} ({target.author.id})\n"
                    f"Source: {target.jump_url}\n\n{content}"
                )
                display_name = _thread_name(content)
                runtime = await self._require_creation().create_from_source(
                    channel_id=channel_id,
                    source_kind="context-ask",
                    source_id=str(interaction.id),
                    prompt=provenance,
                    thread_name=display_name,
                    send_initial_prompt=False,
                )
                await self._record_session_ui(
                    runtime.binding,
                    parent_channel_id=channel_id,
                    display_name=display_name,
                )
                prepared = await self.attachment_service.prepare(
                    source_kind="context-ask",
                    source_id=f"{target.id}:{interaction.id}",
                    session_id=runtime.binding.sdk_session_id,
                    attachments=target.attachments,
                    source_channel_id=str(target.channel.id),
                    source_message_id=str(target.id),
                    recovery_prompt=provenance,
                    recovery_idempotency_key=f"context-ask:{interaction.id}",
                    recovery_origin="context_menu_ask",
                )
                sdk_attachments = (
                    None
                    if prepared is None
                    else await self.attachment_service.sdk_attachments(prepared.manifest_id)
                )
                await runtime.send(
                    provenance,
                    idempotency_key=f"context-ask:{interaction.id}",
                    attachments=sdk_attachments,
                    attachment_manifest_id=(None if prepared is None else prepared.manifest_id),
                    origin="context_menu_ask",
                )
                return f"Asked Copilot in <#{runtime.binding.thread_id}>."

            await self._run_command(interaction, "Ask Copilot", operation)

        async def pin_message(
            interaction: discord.Interaction,
            target: discord.Message,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                await target.pin(reason=f"copilotD pin by interaction {interaction.id}")
                await self.database.execute(
                    """
                    INSERT INTO pinned_message_provenance(
                        discord_message_id, channel_id, guild_id, author_id,
                        jump_url, attachments_json, pinned_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(discord_message_id) DO UPDATE SET
                        jump_url = excluded.jump_url,
                        attachments_json = excluded.attachments_json,
                        pinned_at = excluded.pinned_at
                    """,
                    (
                        str(target.id),
                        str(target.channel.id),
                        None if target.guild is None else str(target.guild.id),
                        str(target.author.id),
                        target.jump_url,
                        json.dumps(
                            [
                                {
                                    "id": str(item.id),
                                    "filename": item.filename,
                                    "size": item.size,
                                    "content_type": item.content_type,
                                    "url": item.url,
                                }
                                for item in target.attachments
                            ],
                            sort_keys=True,
                        ),
                        time.time(),
                    ),
                )
                return "Message pinned with durable provenance metadata."

            await self._run_command(interaction, "Pin message", operation)

        self.tree.add_command(app_commands.ContextMenu(name="Ask Copilot", callback=ask_copilot))
        self.tree.add_command(app_commands.ContextMenu(name="Pin message", callback=pin_message))

        @schedule.command(
            name="message",
            description="Schedule a prompt for this immutable session target",
        )
        async def schedule_message(
            interaction: discord.Interaction,
            when: str,
            text: str,
            timezone: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                binding = await self._interaction_binding(interaction)
                definition = await self._require_scheduler_commands().create_message(
                    thread_id=binding.thread_id,
                    expression=when,
                    text=text,
                    timezone=timezone,
                    created_by=str(interaction.user.id),
                    channel_id=_parent_channel_id(interaction),
                )
                return (
                    f"Schedule `{definition.id}` enabled; next UTC run "
                    f"`{definition.next_run_at_utc}`."
                )

            await self._run_command(interaction, "schedule message", operation)

        @schedule.command(
            name="new-session",
            description="Schedule a new session from an immutable project snapshot",
        )
        async def schedule_new_session(
            interaction: discord.Interaction,
            when: str,
            text: str,
            timezone: str | None = None,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                definition = await self._require_scheduler_commands().create_new_session(
                    channel_id=_parent_channel_id(interaction),
                    expression=when,
                    text=text,
                    timezone=timezone,
                    created_by=str(interaction.user.id),
                    thread_name=_thread_name(text),
                )
                return (
                    f"New-session schedule `{definition.id}` enabled; next UTC run "
                    f"`{definition.next_run_at_utc}`."
                )

            await self._run_command(interaction, "schedule new-session", operation)

        @schedule.command(name="list", description="List app-owned schedules")
        async def schedule_list(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                thread_id = (
                    str(interaction.channel.id)
                    if isinstance(interaction.channel, discord.Thread)
                    else None
                )
                project_snapshot = await self._require_projects().resolve(
                    _parent_channel_id(interaction)
                )
                definitions = await self._require_scheduler_commands().list(
                    project_id=None if thread_id is not None else project_snapshot.project_id,
                    thread_id=thread_id,
                    channel_id=(
                        None
                        if thread_id is not None or project_snapshot.project_id is not None
                        else _parent_channel_id(interaction)
                    ),
                )
                if not definitions:
                    return "No app-owned schedules."
                return "\n".join(
                    f"`{item.id}` · `{item.kind.value}` · `{item.state.value}` · "
                    f"`{item.expression}` @ `{item.timezone}` · next "
                    f"`{item.next_run_at_utc}`"
                    for item in definitions
                )

            await self._run_command(interaction, "schedule list", operation)

        @schedule.command(name="show", description="Show a schedule and recent runs")
        async def schedule_show(
            interaction: discord.Interaction,
            schedule_id: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                detail = await self._require_scheduler_commands().show(schedule_id)
                lines = [
                    f"`{detail.definition.id}` · `{detail.definition.kind.value}` · "
                    f"`{detail.definition.state.value}`",
                    f"`{detail.definition.expression}` @ `{detail.definition.timezone}`",
                    f"next UTC: `{detail.definition.next_run_at_utc}`",
                ]
                lines.extend(
                    f"`{run.run_id}` · `{run.status.value}` · attempt `{run.attempt}` · "
                    f"fence `{run.fence_token}` · basis `{run.completion_basis or '-'}` · "
                    f"error `{run.error_code or '-'}`"
                    for run in detail.runs[:20]
                )
                return "\n".join(lines)

            await self._run_command(interaction, "schedule show", operation)

        @schedule.command(name="toggle", description="Enable or disable future claims")
        async def schedule_toggle(
            interaction: discord.Interaction,
            schedule_id: str,
            enabled: bool,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                definition = await self._require_scheduler_commands().toggle(
                    schedule_id,
                    enabled=enabled,
                )
                return f"Schedule `{definition.id}` is `{definition.state.value}`."

            await self._run_command(interaction, "schedule toggle", operation)

        @schedule.command(name="delete", description="Soft-delete a terminal schedule")
        async def schedule_delete(
            interaction: discord.Interaction,
            schedule_id: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                await self._require_scheduler_commands().delete(schedule_id)
                return "Schedule deleted."

            await self._run_command(interaction, "schedule delete", operation)

        @schedule.command(name="run-now", description="Create an independent manual run")
        async def schedule_run_now(
            interaction: discord.Interaction,
            schedule_id: str,
        ) -> None:
            async def operation(_: CommandInvocation) -> str:
                run = await self._require_scheduler_commands().run_now(schedule_id)
                return f"Manual run `{run.run_id}` is `{run.status.value}`."

            await self._run_command(interaction, "schedule run-now", operation)

        @ops.command(name="scheduler", description="Show scheduler and runtime health")
        async def ops_scheduler(interaction: discord.Interaction) -> None:
            async def operation(_: CommandInvocation) -> str:
                status = await self._require_scheduler_commands().status()
                heartbeat = await self.heartbeat.snapshot()
                return (
                    f"scheduler: `{status.worker_state}`; enabled "
                    f"`{status.enabled_definitions}`; due `{status.due_definitions}`\n"
                    f"runs: pending `{status.pending_runs}`; claimed "
                    f"`{status.claimed_runs}`; waiting `{status.waiting_runs}`; unknown "
                    f"`{status.unknown_runs}`\n"
                    f"sessions: attached `{heartbeat.attached_sessions}`; protected work "
                    f"`{heartbeat.protected_work}`\n"
                    f"restart blockers: `{len(status.restart_blockers)}`"
                )

            await self._run_command(interaction, "ops scheduler", operation)

        @ops.command(name="restart-runtime", description="Restart with durable outcome semantics")
        async def ops_restart_runtime(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            restart_id: str | None = None

            async def operation(_: CommandInvocation) -> str:
                nonlocal restart_id
                restart_id = await self._require_scheduler_repository().prepare_restart(
                    requested_by=str(interaction.user.id),
                    force=force,
                )
                return f"Runtime restart `{restart_id}` prepared" + (
                    " with unknown outcomes fenced." if force else "."
                )

            try:
                await self._run_command(interaction, "ops restart-runtime", operation)
            finally:
                if restart_id is not None:
                    self._restart_task = asyncio.create_task(
                        self._restart_after_ack(),
                        name="discord-runtime-restart",
                    )

        @self.tree.error
        async def application_command_error(
            interaction: discord.Interaction,
            error: app_commands.AppCommandError,
        ) -> None:
            cause = error.original if isinstance(error, app_commands.CommandInvokeError) else error

            async def operation(_: CommandInvocation) -> None:
                raise _map_command_error(cause)

            await self._run_command(
                interaction,
                (
                    "unknown command"
                    if interaction.command is None
                    else interaction.command.qualified_name
                ),
                operation,
            )

        NativeDiscordRegistrar(self, self.capabilities).register(session)
        manifest = self.command_manifest()
        if not self.capabilities.supports("models"):
            model.remove_command("list")
        if not (
            self.capabilities.supports("models") and self.capabilities.supports("model_config")
        ):
            model.remove_command("set")
        project.add_command(variable)
        project.add_command(mcp)
        project.add_command(skill)
        project.add_command(plugin)
        project.add_command(custom_agent)
        project.add_command(worktree)
        self.tree.add_command(session)
        self.tree.add_command(project)
        if "model" in manifest:
            self.tree.add_command(model)
        self.tree.add_command(queue)
        self.tree.add_command(schedule)
        self.tree.add_command(ops)
        for command_name in ("autopilot", "context", "plan", "usage"):
            if command_name not in manifest:
                self.tree.remove_command(command_name)

    def command_manifest(self) -> frozenset[str]:
        return self.capabilities.discord_command_roots()

    async def _interaction_binding(
        self,
        interaction: discord.Interaction,
        *,
        allow_closed: bool = False,
    ) -> SessionBinding:
        if not isinstance(interaction.channel, discord.Thread):
            raise CDScopeError("this command must be used inside a copilotD session thread")
        binding = await self._require_bindings().by_thread(str(interaction.channel.id))
        if binding is None:
            raise CDSessionNotFoundError("this thread is not bound to a Copilot session")
        if binding.binding_intent == BindingIntent.CLOSED and not allow_closed:
            raise CDSessionStateError(
                "this session is closed; use `/session resume` in the original thread"
            )
        if binding.binding_intent != BindingIntent.ACTIVE and not (
            allow_closed and binding.binding_intent == BindingIntent.CLOSED
        ):
            raise CDSessionStateError(
                f"session binding does not allow this operation: {binding.binding_intent}"
            )
        return binding

    async def _interaction_runtime(self, interaction: discord.Interaction) -> SessionRuntime:
        binding = await self._interaction_binding(interaction)
        if binding.binding_intent != BindingIntent.ACTIVE:
            raise ValueError("this Copilot session is closed; use /session resume first")
        sessions = self._require_sessions()
        return await sessions.ensure_attached(binding)

    async def _deferred_set_skill_dir(self, channel_id: str, **kwargs: Any) -> Any:
        return await self._require_projects().set_skill_dir(channel_id, **kwargs)

    async def _deferred_toggle_skill_dir(self, channel_id: str, **kwargs: Any) -> Any:
        return await self._require_projects().toggle_skill_dir(channel_id, **kwargs)

    async def _deferred_remove_skill_dir(self, channel_id: str, **kwargs: Any) -> bool:
        return await self._require_projects().remove_skill_dir(channel_id, **kwargs)

    async def _deferred_set_plugin_dir(self, channel_id: str, **kwargs: Any) -> Any:
        return await self._require_projects().set_plugin_dir(channel_id, **kwargs)

    async def _deferred_toggle_plugin_dir(self, channel_id: str, **kwargs: Any) -> Any:
        return await self._require_projects().toggle_plugin_dir(channel_id, **kwargs)

    async def _deferred_remove_plugin_dir(self, channel_id: str, **kwargs: Any) -> bool:
        return await self._require_projects().remove_plugin_dir(channel_id, **kwargs)

    def _clean_prompt(self, message: discord.Message) -> str:
        content = message.content
        if self.user is not None:
            content = content.replace(f"<@{self.user.id}>", "")
            content = content.replace(f"<@!{self.user.id}>", "")
        return content.strip()

    def _require_projects(self) -> ProjectRegistry:
        if self.projects is None:
            raise RuntimeError("project registry is not initialized")
        return self.projects

    def _require_bindings(self) -> SessionBindingRepository:
        if self.bindings is None:
            raise RuntimeError("session bindings are not initialized")
        return self.bindings

    def _require_deletions(self) -> SessionDeletionService:
        if self.deletions is None:
            raise RuntimeError("session deletion service is not initialized")
        return self.deletions

    def _require_sessions(self) -> SessionRegistry:
        if self.sessions is None:
            raise RuntimeError("session registry is not initialized")
        return self.sessions

    def _require_creation(self) -> SessionCreationService:
        if self.creation is None:
            raise RuntimeError("session creation service is not initialized")
        return self.creation

    def _require_scheduler_repository(self) -> SchedulerRepository:
        if self.scheduler_repository is None:
            raise RuntimeError("scheduler repository is not initialized")
        return self.scheduler_repository

    def _require_scheduler_commands(self) -> SchedulerCommandService:
        if self.scheduler_commands is None:
            raise RuntimeError("scheduler commands are not initialized")
        return self.scheduler_commands

    def _require_project_commands(self) -> ProjectLifecycleService:
        if self.project_commands is None:
            raise RuntimeError("project lifecycle commands are not initialized")
        return self.project_commands

    def _require_worktree_commands(self) -> WorktreeCommandService:
        if self.worktree_commands is None:
            raise RuntimeError("worktree commands are not initialized")
        return self.worktree_commands

    async def _restart_after_ack(self) -> None:
        await asyncio.sleep(0.5)
        self.restart_requested = True
        await self.close()

    async def _is_restart_draining(self) -> bool:
        row = await self.database.fetchone(
            "SELECT value FROM global_config WHERE key = 'restart_draining'"
        )
        return row is not None and row["value"] == "1"

    def _admit_handler(self) -> asyncio.Task[Any] | None:
        if not self._accepting_handlers:
            return None
        task = asyncio.current_task()
        if task is None:
            return None
        self._admitted_handlers.add(task)
        return task

    async def _drain_admitted_handlers(self) -> None:
        current = asyncio.current_task()
        while admitted := [
            task for task in self._admitted_handlers if task is not current and not task.done()
        ]:
            await asyncio.gather(*admitted, return_exceptions=True)


class DiscordThreadGateway:
    def __init__(self, bot: CopilotDiscordBot) -> None:
        self._bot = bot

    async def find_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        creation_token: str,
    ) -> ThreadReference | None:
        channel = await self._channel(channel_id)
        token = creation_token[:8]
        if isinstance(channel, discord.TextChannel):
            try:
                source = await channel.fetch_message(int(source_id))
            except (discord.NotFound, ValueError):
                source = None
            if source is not None and source.thread is not None:
                return ThreadReference(str(source.thread.id))
            threads = channel.threads
        elif isinstance(channel, discord.ForumChannel):
            threads = channel.threads
        else:
            return None
        match = next((thread for thread in threads if f"[cd:{token}]" in thread.name), None)
        return None if match is None else ThreadReference(str(match.id))

    async def create_thread(
        self,
        *,
        channel_id: str,
        source_id: str,
        name: str,
        creation_token: str,
        layout: str,
    ) -> ThreadReference:
        channel = await self._channel(channel_id)
        thread_name = f"{name[:75]} [cd:{creation_token[:8]}]"
        if layout == "text" and isinstance(channel, discord.TextChannel):
            try:
                source = await channel.fetch_message(int(source_id))
            except (discord.NotFound, ValueError):
                source = await channel.send(f"Starting copilotD session `{creation_token[:8]}`")
            thread = await source.create_thread(name=thread_name, auto_archive_duration=1440)
            return ThreadReference(str(thread.id))
        if layout == "forum" and isinstance(channel, discord.ForumChannel):
            created = await channel.create_thread(
                name=thread_name,
                content=f"Starting copilotD session `{creation_token[:8]}`",
                auto_archive_duration=1440,
            )
            return ThreadReference(str(created.thread.id))
        raise ValueError(
            f"configured `{layout}` layout does not match Discord channel type "
            f"`{type(channel).__name__}`"
        )

    async def _channel(
        self,
        channel_id: str,
    ) -> discord.TextChannel | discord.ForumChannel:
        channel = self._bot.get_channel(int(channel_id))
        if channel is None:
            channel = await self._bot.fetch_channel(int(channel_id))
        if not isinstance(channel, (discord.TextChannel, discord.ForumChannel)):
            raise ValueError("channel does not support session threads")
        return channel


class DiscordInteractionResponder(CommandResponder):
    def __init__(
        self,
        bot: CopilotDiscordBot,
        interaction: discord.Interaction,
        *,
        name: str,
    ) -> None:
        self._bot = bot
        self._interaction = interaction
        self._name = name
        self._unknown_interaction = False

    async def defer(self, *, ephemeral: bool = True) -> None:
        if self._interaction.response.is_done():
            return
        try:
            await self._interaction.response.defer(ephemeral=ephemeral)
        except discord.HTTPException as error:
            if not _is_unknown_interaction(error):
                raise
            self._unknown_interaction = True
            raise UnknownInteractionError() from error

    async def send_inline(self, content: str, *, ephemeral: bool = True) -> None:
        if self._unknown_interaction:
            await self._send_thread_fallback(content)
            return
        if self._interaction.response.is_done():
            await self._send_followup_payload(content, ephemeral=ephemeral)
            return
        try:
            if len(content) <= 1850:
                await self._interaction.response.send_message(
                    content,
                    ephemeral=ephemeral,
                )
            else:
                await self._interaction.response.send_message(
                    "The command result is attached.",
                    file=_text_file(content, self._name),
                    ephemeral=ephemeral,
                )
        except discord.HTTPException as error:
            if not _is_unknown_interaction(error):
                raise
            self._unknown_interaction = True
            raise UnknownInteractionError() from error

    async def send_followup(self, content: str, *, ephemeral: bool = True) -> None:
        if self._unknown_interaction:
            await self._send_thread_fallback(content)
            return
        try:
            await self._send_followup_payload(content, ephemeral=ephemeral)
        except discord.HTTPException as error:
            if not _is_unknown_interaction(error):
                raise
            self._unknown_interaction = True
            await self._send_thread_fallback(content)

    async def send_modal(self, modal: discord.ui.Modal) -> None:
        try:
            await self._interaction.response.send_modal(modal)
        except discord.HTTPException as error:
            if not _is_unknown_interaction(error):
                raise
            self._unknown_interaction = True
            self.warn(
                "discord_unknown_interaction_during_modal",
                discord_code=10062,
                command=self._name,
            )
            await self._send_thread_fallback(
                "The interaction expired before the form opened; use the latest control."
            )

    async def send_file(
        self,
        message: str,
        *,
        content: bytes,
        filename: str,
        ephemeral: bool = True,
    ) -> None:
        file = discord.File(io.BytesIO(content), filename=filename)
        if self._unknown_interaction:
            channel = self._interaction.channel
            if channel is None or not hasattr(channel, "send"):
                raise CDDiscordError(
                    "interaction expired and its Discord thread cannot receive the file"
                )
            await channel.send(
                "⚠️ Discord expired this interaction (`10062`); result attached.",
                file=file,
                silent=True,
            )
            return
        try:
            await self._interaction.followup.send(
                message,
                file=file,
                ephemeral=ephemeral,
            )
        except discord.HTTPException as error:
            if not _is_unknown_interaction(error):
                raise
            self._unknown_interaction = True
            channel = self._interaction.channel
            if channel is None or not hasattr(channel, "send"):
                raise CDDiscordError(
                    "interaction expired and its Discord thread cannot receive the file"
                ) from error
            await channel.send(
                "⚠️ Discord expired this interaction (`10062`); result attached.",
                file=discord.File(io.BytesIO(content), filename=filename),
                silent=True,
            )

    def warn(self, message: str, **fields: Any) -> None:
        logger.warning(message, **fields)

    async def _send_followup_payload(self, content: str, *, ephemeral: bool) -> None:
        if len(content) <= 1850:
            await self._interaction.followup.send(content, ephemeral=ephemeral)
            return
        await self._interaction.followup.send(
            "The command result is attached.",
            file=_text_file(content, self._name),
            ephemeral=ephemeral,
        )

    async def _send_thread_fallback(self, content: str) -> None:
        channel = self._interaction.channel
        if channel is None or not hasattr(channel, "send"):
            raise CDDiscordError(
                "interaction expired and its Discord thread cannot receive the result"
            )
        warning = (
            "⚠️ Discord expired this interaction (`10062`); copilotD completed the "
            "operation and is posting the durable result in-thread."
        )
        if len(content) + len(warning) + 2 <= 1850:
            await channel.send(f"{warning}\n\n{content}", silent=True)
            return
        await channel.send(
            warning,
            file=_text_file(content, self._name),
            silent=True,
        )


class TaskMessageModal(discord.ui.Modal):
    message = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000,
    )

    def __init__(
        self,
        bot: CopilotDiscordBot,
        *,
        panel_id: str,
        card_token: str,
        revision: int,
        message_id: str,
    ) -> None:
        super().__init__(title="Message Copilot task")
        self._bot = bot
        self._panel_id = panel_id
        self._card_token = card_token
        self._revision = revision
        self._message_id = message_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        responder = DiscordInteractionResponder(
            self._bot,
            interaction,
            name="TaskDeck message",
        )
        try:
            await responder.defer(ephemeral=True)
        except UnknownInteractionError:
            pass
        try:
            runtime = await self._bot._interaction_runtime(interaction)
            result = await runtime.perform_taskdeck_action(
                panel_id=self._panel_id,
                card_token=self._card_token,
                expected_revision=self._revision,
                action="message",
                message_id=self._message_id,
                interaction_id=str(interaction.id),
                message=str(self.message.value),
            )
            text = (
                "TaskDeck changed; use the latest controls."
                if result["status"] == "stale"
                else "Message sent to the task."
            )
        except Exception as error:
            mapped = _map_command_error(error)
            text = f"[{mapped.code}] {mapped.message}"
        await responder.send_followup(text, ephemeral=True)


class InteractionResponseModal(discord.ui.Modal):
    response = discord.ui.TextInput(
        label="Response",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000,
    )

    def __init__(self, bot: CopilotDiscordBot, interaction_id: str) -> None:
        super().__init__(title="Respond to Copilot")
        self._bot = bot
        self._interaction_id = interaction_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        responder = DiscordInteractionResponder(
            self._bot,
            interaction,
            name="Copilot input",
        )
        try:
            await responder.defer(ephemeral=True)
        except UnknownInteractionError:
            pass
        try:
            runtime = await self._bot._interaction_runtime(interaction)
            result = await runtime.respond_interaction(
                self._interaction_id,
                freeform=str(self.response.value),
            )
            text = _interaction_result_text(result)
        except Exception as error:
            mapped = _map_command_error(error)
            text = f"[{mapped.code}] {mapped.message}"
        await responder.send_followup(text, ephemeral=True)


class ElicitationResponseModal(discord.ui.Modal):
    def __init__(
        self,
        bot: CopilotDiscordBot,
        interaction_id: str,
        form: ElicitationForm,
    ) -> None:
        super().__init__(title="Copilot form")
        self._bot = bot
        self._interaction_id = interaction_id
        self._form = form
        self._inputs: list[tuple[ElicitationField, discord.ui.TextInput[Any]]] = []
        self._json_input: discord.ui.TextInput[Any] | None = None
        if len(form.fields) > DiscordInteractionAdapter.MODAL_FIELD_LIMIT:
            self._json_input = discord.ui.TextInput(
                label="Form values as JSON",
                style=discord.TextStyle.paragraph,
                required=True,
                max_length=4000,
                placeholder='{"field": "value"}',
            )
            self.add_item(self._json_input)
            return
        for field in form.fields:
            default = field.default
            rendered_default = (
                json.dumps(list(default))
                if isinstance(default, tuple)
                else None
                if default is None
                else str(default).lower()
                if isinstance(default, bool)
                else str(default)
            )
            text_input = discord.ui.TextInput(
                label=_bounded_discord_text(field.title, 45),
                style=(
                    discord.TextStyle.paragraph
                    if field.value_type == "array"
                    else discord.TextStyle.short
                ),
                required=field.required,
                default=rendered_default,
                max_length=min(field.max_length or 4000, 4000),
                placeholder=_elicitation_placeholder(field),
            )
            self._inputs.append((field, text_input))
            self.add_item(text_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            if self._json_input is not None:
                decoded = json.loads(str(self._json_input.value))
                if not isinstance(decoded, dict):
                    raise ValueError("form JSON must be an object")
                content = decoded
            else:
                content = {
                    field.name: _coerce_elicitation_value(field, str(item.value))
                    for field, item in self._inputs
                    if str(item.value) or field.required
                }
        except (ValueError, json.JSONDecodeError) as error:
            await interaction.response.send_message(
                f"Invalid form response: {error}",
                ephemeral=True,
            )
            return
        runtime = await self._bot._interaction_runtime(interaction)
        result = await runtime.respond_interaction(
            self._interaction_id,
            form_content=content,
        )
        await interaction.response.send_message(
            _interaction_result_text(result),
            ephemeral=True,
        )


@dataclass(frozen=True, slots=True)
class DiscordRenderBatch:
    content: str
    assets: tuple[TableAsset, ...] = ()
    embeds: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class DiscordRenderPlan:
    batches: tuple[DiscordRenderBatch, ...]


async def _discord_render(
    payload: dict[str, Any],
) -> tuple[str, list[TableAsset]]:
    plan = await _discord_render_plan(payload)
    first = plan.batches[0]
    assets = list(first.assets)
    combined = "\n\n".join(batch.content for batch in plan.batches if batch.content)
    if len(combined) <= 1850:
        for batch in plan.batches[1:]:
            assets.extend(batch.assets)
        return combined, assets
    for index, batch in enumerate(plan.batches[1:], start=2):
        assets.extend(batch.assets)
        if batch.content:
            assets.append(
                TableAsset(
                    filename=f"response-segment-{index:03d}.md",
                    media_type="text/markdown",
                    content=batch.content.encode("utf-8"),
                )
            )
    return first.content, assets


async def _discord_render_plan(
    payload: dict[str, Any],
    *,
    allowed_roots: tuple[Path, ...] = (),
    max_bytes: int | None = None,
) -> DiscordRenderPlan:
    content = str(payload.get("content", ""))
    taskdeck_embeds = _taskdeck_embed_payloads(payload)
    if not payload.get("finalized") and not taskdeck_embeds:
        batches = [DiscordRenderBatch(_safe_stream_content(content))]
        return DiscordRenderPlan(tuple(_decorate_discord_batches(payload, batches)))

    explicit_assets: list[TableAsset] = []
    attachments = payload.get("attachments")
    if isinstance(attachments, list):
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            body = attachment.get("content")
            if isinstance(body, str):
                encoded = body.encode("utf-8")
            elif isinstance(body, bytes):
                encoded = body
            elif isinstance(attachment.get("path"), str):
                artifact_path = Path(str(attachment["path"]))
                encoded = await asyncio.to_thread(artifact_path.read_bytes)
                expected_size = attachment.get("byte_size")
                if expected_size is not None and len(encoded) != int(expected_size):
                    raise RenderPermanentError(f"render artifact size changed: {artifact_path}")
                expected_sha = attachment.get("sha256")
                if expected_sha is not None and hashlib.sha256(encoded).hexdigest() != str(
                    expected_sha
                ):
                    raise RenderPermanentError(f"render artifact digest changed: {artifact_path}")
            else:
                continue
            explicit_assets.append(
                TableAsset(
                    filename=str(attachment.get("filename", "artifact.txt")),
                    media_type=str(attachment.get("media_type", "text/plain")),
                    content=encoded,
                )
            )
    if payload.get("local_git") and allowed_roots:
        try:
            local_diff = await render_diff(cwd=allowed_roots[0])
        except (OSError, RuntimeError, ValueError) as error:
            content = (
                "**Code changes** · `local-git`\n"
                f"Local diff is unavailable: `{type(error).__name__}`."
            )
        else:
            if local_diff is None:
                content = "**Code changes** · `local-git`\nNo uncommitted diff."
            else:
                content = local_diff.content
                explicit_assets.extend(local_diff.assets)

    local_image_assets: list[TableAsset] = []
    image_warnings: list[str] = []
    trusted_local_artifacts = payload.get("trusted_local_image_artifacts")
    if (
        allowed_roots
        and payload.get("trusted_local_images") is True
        and isinstance(trusted_local_artifacts, list)
    ):
        artifact_paths: dict[str, str] = {}
        artifact_metadata: dict[str, tuple[int, str]] = {}
        for artifact in trusted_local_artifacts:
            if not isinstance(artifact, dict):
                continue
            source_path = artifact.get("source_path")
            snapshot_path = artifact.get("snapshot_path")
            byte_size = artifact.get("byte_size")
            digest = artifact.get("sha256")
            if (
                isinstance(source_path, str)
                and isinstance(snapshot_path, str)
                and isinstance(byte_size, int)
                and isinstance(digest, str)
            ):
                resolved_snapshot = str(
                    await asyncio.to_thread(
                        Path(snapshot_path).resolve,
                        strict=False,
                    )
                )
                artifact_paths[source_path] = resolved_snapshot
                artifact_metadata[resolved_snapshot] = (byte_size, digest)
        extraction = await asyncio.to_thread(
            lambda: extract_local_markdown_images(
                content,
                allowed_roots=allowed_roots,
                trusted_artifacts=artifact_paths,
            )
        )
        content = extraction.content
        image_warnings.extend(warning.message for warning in extraction.warnings)
        for attachment in extraction.attachments:
            snapshot_path = Path(attachment.resolved_path)
            try:
                image_content = await asyncio.to_thread(snapshot_path.read_bytes)
            except OSError as error:
                raise RenderPermanentError(
                    f"trusted local image snapshot disappeared: {snapshot_path}"
                ) from error
            expected_size, expected_digest = artifact_metadata[str(snapshot_path)]
            if len(image_content) != expected_size:
                raise RenderPermanentError(
                    f"trusted local image snapshot size changed: {snapshot_path}"
                )
            if hashlib.sha256(image_content).hexdigest() != expected_digest:
                raise RenderPermanentError(
                    f"trusted local image snapshot digest changed: {snapshot_path}"
                )
            local_image_assets.append(
                TableAsset(
                    filename=attachment.filename,
                    media_type=_image_media_type(attachment.filename),
                    content=image_content,
                )
            )

    assembler = MarkdownAssembler()
    assembler.append(content)
    blocks = assembler.finalize(content)
    batches: list[DiscordRenderBatch] = []
    pending_text: list[str] = []

    def flush_text() -> None:
        if not pending_text:
            return
        message_plan = plan_markdown_messages(
            "\n\n".join(pending_text),
            max_chars=1850,
        )
        for segment in message_plan.segments:
            segment_assets = tuple(
                TableAsset(
                    filename=attachment.filename,
                    media_type=attachment.media_type,
                    content=attachment.content.encode("utf-8"),
                )
                for attachment in segment.attachments
            )
            batches.append(
                DiscordRenderBatch(
                    content=segment.content,
                    assets=segment_assets,
                )
            )
        pending_text.clear()

    for block in blocks:
        if isinstance(block, TextBlock):
            pending_text.append(block.content)
            continue
        if isinstance(block, TableBlock):
            flush_text()
            table_plan = await render_table(
                block.markdown,
                max_upload_bytes=max_bytes,
            )
            table_content = table_plan.preview_text or ""
            if table_plan.assets:
                label = ", ".join(asset.filename for asset in table_plan.assets)
                table_content = (
                    f"{table_content}\n\n" if table_content else ""
                ) + f"Table assets: `{label}`"
            batches.append(
                DiscordRenderBatch(
                    content=table_content,
                    assets=table_plan.assets,
                )
            )
    flush_text()

    if image_warnings:
        warning_text = "\n\n".join(
            f"⚠️ {_bounded_discord_text(warning, 300)}" for warning in image_warnings
        )
        warning_plan = plan_markdown_messages(warning_text, max_chars=1850)
        for segment in warning_plan.segments:
            warning_assets = tuple(
                TableAsset(
                    filename=attachment.filename,
                    media_type=attachment.media_type,
                    content=attachment.content.encode("utf-8"),
                )
                for attachment in segment.attachments
            )
            if (
                not warning_assets
                and batches
                and len(batches[-1].content) + len(segment.content) + 2 <= 1850
            ):
                last = batches[-1]
                batches[-1] = DiscordRenderBatch(
                    content=f"{last.content}\n\n{segment.content}".strip(),
                    assets=last.assets,
                    embeds=last.embeds,
                )
            else:
                batches.append(
                    DiscordRenderBatch(
                        content=segment.content,
                        assets=warning_assets,
                    )
                )
    if not batches:
        batches.append(DiscordRenderBatch(""))

    if taskdeck_embeds:
        first = batches[0]
        batches[0] = DiscordRenderBatch(
            content=first.content,
            assets=first.assets,
            embeds=taskdeck_embeds,
        )

    batches = _append_assets_to_batches(batches, explicit_assets)
    for index in range(0, len(local_image_assets), 10):
        group = local_image_assets[index : index + 10]
        batches.append(
            DiscordRenderBatch(
                content=(
                    f"Local image attachment batch "
                    f"{index // 10 + 1}/{(len(local_image_assets) + 9) // 10}."
                ),
                assets=tuple(group),
            )
        )

    prepared_batches: list[DiscordRenderBatch] = []
    for batch in batches:
        prepared_content, prepared_assets = _prepare_discord_assets(
            batch.content,
            list(batch.assets),
            max_bytes=max_bytes or 2**63 - 1,
        )
        if not prepared_assets:
            prepared_batches.append(
                DiscordRenderBatch(
                    prepared_content,
                    embeds=batch.embeds,
                )
            )
            continue
        for index in range(0, len(prepared_assets), 10):
            batch_assets = _unique_discord_assets(tuple(prepared_assets[index : index + 10]))
            prepared_batches.append(
                DiscordRenderBatch(
                    content=prepared_content if index == 0 else "",
                    assets=batch_assets,
                    embeds=batch.embeds if index == 0 else (),
                )
            )
    return DiscordRenderPlan(tuple(_decorate_discord_batches(payload, prepared_batches)))


def _decorate_discord_batches(
    payload: dict[str, Any],
    batches: list[DiscordRenderBatch],
) -> list[DiscordRenderBatch]:
    decorated: list[DiscordRenderBatch] = []
    total = len(batches)
    for index, batch in enumerate(batches):
        embeds = list(batch.embeds)
        content = batch.content
        primary = _rich_content_embed(
            payload,
            batch,
            index=index,
            total=total,
        )
        if primary is not None and not embeds:
            embeds.append(primary)
            content = ""
        candidates = embeds + _image_attachment_embeds(
            batch.assets,
            payload_type=str(payload.get("type") or ""),
        )
        bounded: list[dict[str, Any]] = []
        character_count = 0
        for candidate in candidates:
            candidate_size = _embed_character_count(candidate)
            if len(bounded) >= 10 or character_count + candidate_size > 6000:
                break
            bounded.append(candidate)
            character_count += candidate_size
        decorated.append(
            DiscordRenderBatch(
                content=content,
                assets=batch.assets,
                embeds=tuple(bounded),
            )
        )
    return decorated


def _rich_content_embed(
    payload: dict[str, Any],
    batch: DiscordRenderBatch,
    *,
    index: int,
    total: int,
) -> dict[str, Any] | None:
    payload_type = str(payload.get("type") or "")
    content = batch.content.strip()
    attachment_field = _attachment_embed_field(batch.assets)
    fields: list[dict[str, Any]] = []
    if attachment_field is not None:
        fields.append(attachment_field)

    if payload_type in {"assistant.message", "assistant.message_delta"}:
        streaming = not bool(payload.get("finalized"))
        title = "✨ Copilot is responding" if streaming else "✨ Copilot response"
        if total > 1:
            title += f" · {index + 1}/{total}"
        if _has_table_preview(batch):
            title = f"📊 Data table · {index + 1}/{total}" if total > 1 else "📊 Data table"
        elif _only_image_assets(batch):
            title = f"🖼️ Image gallery · {index + 1}/{total}" if total > 1 else "🖼️ Image gallery"
        return _embed_payload(
            title=title,
            description=content
            or ("Preparing the response…" if streaming else "Response complete."),
            color=_COLOR_BLURPLE,
            fields=fields,
            footer="Live response" if streaming else "Copilot · response complete",
        )

    if payload_type == "interaction":
        interaction = payload.get("interaction")
        if not isinstance(interaction, dict):
            interaction = {}
        state = str(interaction.get("state") or "pending")
        pending = state == "pending"
        resolved = state == "resolved"
        question = str(
            interaction.get("question")
            or interaction.get("summary")
            or "Copilot is waiting for input."
        )
        response = str(interaction.get("display_response") or "").strip()
        description = question
        if response and resolved:
            description += f"\n\n**Response**\n{response}"
        elif not pending and not resolved:
            description += "\n\nThis request expired before a response was recorded."
        fields = [
            {
                "name": "Request type",
                "value": f"`{_bounded_discord_text(str(interaction.get('kind') or 'input'), 80)}`",
                "inline": True,
            },
            {"name": "State", "value": f"`{_bounded_discord_text(state, 40)}`", "inline": True},
        ]
        if pending:
            title = "📝 Copilot needs input"
            color = _COLOR_YELLOW
            footer = "Choose an option below"
        elif resolved:
            title = "✅ Copilot input recorded"
            color = _COLOR_GREEN
            footer = "The response was sent to Copilot"
        else:
            title = "⏳ Copilot input expired"
            color = _COLOR_NEUTRAL
            footer = "No response was sent"
        return _embed_payload(
            title=title,
            description=description,
            color=color,
            fields=fields,
            footer=footer,
        )

    if payload_type in {
        "assistant.usage",
        "session.usage_checkpoint",
        "session.usage_info",
    }:
        usage = payload.get("usage")
        metrics = usage if isinstance(usage, dict) else {}
        usage_fields = _usage_embed_fields(metrics)
        description = _usage_context_bar(metrics)
        if not usage_fields and not description:
            description = _content_without_heading(content) or (
                "The runtime updated usage without exposing numeric fields."
            )
        return _embed_payload(
            title="📈 Copilot usage",
            description=description,
            color=_COLOR_CYAN,
            fields=usage_fields,
            footer="Live usage snapshot" if not payload.get("finalized") else "Usage checkpoint",
        )

    if payload_type == "idle_footer":
        input_tokens = int(payload.get("input_tokens") or 0)
        output_tokens = int(payload.get("output_tokens") or 0)
        fields = [
            {
                "name": "Model",
                "value": f"`{_bounded_discord_text(str(payload.get('model') or 'unknown'), 100)}`",
                "inline": True,
            },
            {
                "name": "Tokens",
                "value": f"`{input_tokens:,}` in · `{output_tokens:,}` out",
                "inline": True,
            },
            {
                "name": "Duration",
                "value": f"`{_format_render_duration(payload.get('duration_seconds'))}`",
                "inline": True,
            },
            {
                "name": "Context",
                "value": f"`{_bounded_discord_text(str(payload.get('context') or 'unknown'), 80)}`",
                "inline": True,
            },
            {
                "name": "AI credits",
                "value": f"`{_format_metric(payload.get('credits') or 0)}`",
                "inline": True,
            },
        ]
        description = (
            "Background work is still observed; this is a point-in-time summary."
            if payload.get("background_observed")
            else "The current Copilot turn is complete."
        )
        return _embed_payload(
            title="✅ Turn complete",
            description=description,
            color=_COLOR_YELLOW if payload.get("background_observed") else _COLOR_GREEN,
            fields=fields,
            footer="copilotD session summary",
        )

    if payload_type == "diff":
        stats = payload.get("stats")
        stats = stats if isinstance(stats, dict) else {}
        oversized = bool(payload.get("oversized"))
        files_value = (
            f"`{int(stats['files']):,}`" if stats.get("files") is not None else "`unknown`"
        )
        changes_value = (
            (
                f"🟢 `+{int(stats.get('additions') or 0):,}` · "
                f"🔴 `-{int(stats.get('deletions') or 0):,}`"
            )
            if stats.get("additions") is not None or stats.get("deletions") is not None
            else "`unknown`"
        )
        if oversized:
            delivery = "`omitted: render safety limit`"
        elif batch.assets:
            delivery = "`attachment`"
        else:
            delivery = "`inline preview`"
        fields = [
            {
                "name": "Source",
                "value": f"`{_bounded_discord_text(str(payload.get('source') or 'unknown'), 80)}`",
                "inline": True,
            },
            {
                "name": "Files",
                "value": files_value,
                "inline": True,
            },
            {
                "name": "Changes",
                "value": changes_value,
                "inline": True,
            },
            {
                "name": "Size",
                "value": f"`{_format_asset_size(int(payload.get('byte_count') or 0))}`",
                "inline": True,
            },
            {
                "name": "Delivery",
                "value": delivery,
                "inline": True,
            },
        ]
        if attachment_field is not None:
            fields.append(attachment_field)
        return _embed_payload(
            title="🧩 Code changes",
            description=_content_without_heading(content)
            or "Structured code changes are available.",
            color=_COLOR_YELLOW if oversized else _COLOR_GREEN,
            fields=fields,
            footer=(
                "Patch omitted from Discord; exact source remains in the durable event journal"
                if oversized
                else "Exact patch is preserved when attached"
            ),
        )

    if payload_type == "tool_output_artifact":
        status_value = payload.get("status")
        if status_value is None:
            heading = content.splitlines()[0].lower() if content else ""
            status_value = (
                "failed" if "tool failed" in heading or "tool error" in heading else "unknown"
            )
        status_text = str(status_value).lower()
        fields = [
            {
                "name": "Status",
                "value": f"`{_bounded_discord_text(status_text, 40)}`",
                "inline": True,
            },
            {
                "name": "Source",
                "value": (
                    "`"
                    + _bounded_discord_text(
                        str(payload.get("tool_source") or "unknown"),
                        80,
                    )
                    + "`"
                ),
                "inline": True,
            },
            {
                "name": "Fidelity",
                "value": "`verbatim`" if payload.get("verbatim") else "`runtime fallback`",
                "inline": True,
            },
            {
                "name": "Output",
                "value": _tool_output_size_text(payload),
                "inline": False,
            },
        ]
        if attachment_field is not None:
            fields.append(attachment_field)
        failed = status_text == "failed"
        unknown = status_text == "unknown"
        if failed:
            title = "❌ Tool output"
            color = _COLOR_RED
        elif unknown:
            title = "⚠️ Tool output"
            color = _COLOR_NEUTRAL
        else:
            title = "📎 Tool output"
            color = _COLOR_CYAN
        return _embed_payload(
            title=title,
            description=_content_without_heading(content) or "Detailed tool output is attached.",
            color=color,
            fields=fields,
            footer="Durable output artifact",
        )

    status = payload.get("status")
    if isinstance(status, dict):
        event_type = str(status.get("event_type") or payload_type)
        icon, color = _status_embed_style(event_type, status=status)
        title = str(status.get("title") or "Copilot status")
        if event_type == "session.task_complete":
            outcome = str(status.get("outcome") or "unknown")
            title = {
                "completed": "Task complete",
                "continue": "Task continuing",
                "blocked": "Task blocked",
            }.get(outcome, title)
        detail = str(status.get("detail") or _content_without_heading(content))
        return _embed_payload(
            title=f"{icon} {title}",
            description=detail,
            color=color,
            footer=(f"{event_type} · updating" if not payload.get("finalized") else event_type),
        )
    return None


def _embed_payload(
    *,
    title: str,
    description: str,
    color: int,
    fields: list[dict[str, Any]] | None = None,
    footer: str | None = None,
) -> dict[str, Any]:
    embed: dict[str, Any] = {
        "title": _bounded_embed_markdown(title, 256),
        "description": _bounded_embed_markdown(description, 3900),
        "color": color,
    }
    if fields:
        embed["fields"] = fields[:25]
    if footer:
        embed["footer"] = {"text": _bounded_embed_markdown(footer, 2048)}
    return embed


def _status_embed_style(
    event_type: str,
    *,
    status: dict[str, Any] | None = None,
) -> tuple[str, int]:
    if event_type == "session.task_complete":
        outcome = str((status or {}).get("outcome") or "unknown")
        return {
            "completed": ("✅", _COLOR_GREEN),
            "continue": ("▶️", _COLOR_BLURPLE),
            "blocked": ("⚠️", _COLOR_YELLOW),
        }.get(outcome, ("🔹", _COLOR_CYAN))
    if event_type in {"session.error", "model.call_failure"}:
        return "❌", _COLOR_RED
    if event_type in {
        "session.warning",
        "session.truncation",
        "assistant.turn_retry",
        "session.snapshot_rewind",
    }:
        return "⚠️", _COLOR_YELLOW
    if event_type in {"abort", "session.shutdown"}:
        return "⏹️", _COLOR_NEUTRAL
    if event_type in {
        "session.compaction_complete",
        "session.context_cleared",
    }:
        return "✅", _COLOR_GREEN
    if event_type == "session.workspace_file_changed":
        return "📝", _COLOR_CYAN
    if event_type in {"assistant.intent", "assistant.reasoning_delta", "session.compaction_start"}:
        return "⏳", _COLOR_BLURPLE
    if event_type == "assistant.reasoning":
        return "💡", _COLOR_BLURPLE
    return "🔹", _COLOR_CYAN


def _usage_embed_fields(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    labels = (
        ("inputTokens", "Input"),
        ("outputTokens", "Output"),
        ("totalTokens", "Total"),
        ("cacheReadTokens", "Cache read"),
        ("cacheWriteTokens", "Cache write"),
        ("premiumRequests", "Premium requests"),
        ("aiCredits", "AI credits"),
        ("nanoAiu", "nano AIU"),
    )
    return [
        {
            "name": label,
            "value": f"`{_format_metric(metrics[key])}`",
            "inline": True,
        }
        for key, label in labels
        if key in metrics
    ]


def _usage_context_bar(metrics: dict[str, Any]) -> str:
    current = metrics.get("currentTokens")
    limit = metrics.get("tokenLimit")
    try:
        current_value = max(0, float(current))
        limit_value = max(0, float(limit))
    except (TypeError, ValueError):
        return ""
    if limit_value <= 0:
        return ""
    ratio = min(1.0, current_value / limit_value)
    filled = round(ratio * 12)
    bar = "#" * filled + "-" * (12 - filled)
    return (
        f"**Context window**\n`[{bar}]` {ratio:.0%} · "
        f"`{_format_metric(current_value)}` / `{_format_metric(limit_value)}` tokens"
    )


def _attachment_embed_field(assets: tuple[TableAsset, ...]) -> dict[str, Any] | None:
    if not assets:
        return None
    lines = []
    for asset in assets[:8]:
        filename = _bounded_discord_text(asset.filename, 120)
        lines.append(f"📎 `{filename}` · {_format_asset_size(len(asset.content))}")
    if len(assets) > 8:
        lines.append(f"… and {len(assets) - 8} more")
    return {
        "name": "Attachments",
        "value": _bounded_embed_markdown("\n".join(lines), 1024),
        "inline": False,
    }


def _image_attachment_embeds(
    assets: tuple[TableAsset, ...],
    *,
    payload_type: str,
) -> list[dict[str, Any]]:
    images = [asset for asset in assets if asset.media_type.startswith("image/")]
    embeds: list[dict[str, Any]] = []
    for index, asset in enumerate(images):
        is_table = "table" in asset.filename.lower() or payload_type == "table"
        title = "📊 Table preview" if is_table else "🖼️ Image preview"
        if len(images) > 1:
            title += f" · {index + 1}/{len(images)}"
        embeds.append(
            {
                "title": title,
                "description": (
                    f"`{_bounded_discord_text(asset.filename, 140)}` · "
                    f"{_format_asset_size(len(asset.content))}"
                ),
                "color": _COLOR_CYAN,
                "image": {"url": f"attachment://{asset.filename}"},
            }
        )
    return embeds


def _unique_discord_assets(assets: tuple[TableAsset, ...]) -> tuple[TableAsset, ...]:
    used: set[str] = set()
    normalized: list[TableAsset] = []
    for asset in assets:
        original = Path(asset.filename).name or "artifact"
        filename = (
            _discord_safe_image_filename(original)
            if asset.media_type.startswith("image/")
            else original
        )
        candidate = filename
        suffix_index = 2
        while candidate.casefold() in used:
            path = Path(filename)
            stem = path.stem[:160] or "artifact"
            suffix = path.suffix[:20]
            candidate = f"{stem}-{suffix_index}{suffix}"
            suffix_index += 1
        used.add(candidate.casefold())
        normalized.append(
            TableAsset(
                filename=candidate,
                media_type=asset.media_type,
                content=asset.content,
            )
        )
    return tuple(normalized)


def _discord_safe_image_filename(filename: str) -> str:
    path = Path(filename)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", path.stem).strip(".-") or "image"
    suffix = re.sub(r"[^A-Za-z0-9.]+", "", path.suffix.lower())
    return f"{stem[:160]}{suffix[:20]}"


def _has_table_preview(batch: DiscordRenderBatch) -> bool:
    return any(
        asset.media_type.startswith("image/") and "table" in asset.filename.lower()
        for asset in batch.assets
    )


def _only_image_assets(batch: DiscordRenderBatch) -> bool:
    return bool(batch.assets) and all(
        asset.media_type.startswith("image/") for asset in batch.assets
    )


def _embed_character_count(embed: dict[str, Any]) -> int:
    count = len(str(embed.get("title") or "")) + len(str(embed.get("description") or ""))
    footer = embed.get("footer")
    if isinstance(footer, dict):
        count += len(str(footer.get("text") or ""))
    author = embed.get("author")
    if isinstance(author, dict):
        count += len(str(author.get("name") or ""))
    fields = embed.get("fields")
    if isinstance(fields, list):
        for field in fields:
            if isinstance(field, dict):
                count += len(str(field.get("name") or "")) + len(str(field.get("value") or ""))
    return count


def _content_without_heading(content: str) -> str:
    lines = content.strip().splitlines()
    if not lines:
        return ""
    if lines[0].startswith("**") and "**" in lines[0][2:]:
        return "\n".join(lines[1:]).strip() or lines[0].replace("**", "").strip()
    return content.strip()


def _bounded_embed_markdown(content: str, limit: int) -> str:
    value = content.strip()
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _format_metric(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    return _bounded_discord_text(str(value), 80)


def _tool_output_size_text(payload: dict[str, Any]) -> str:
    character_count = payload.get("character_count")
    line_count = payload.get("line_count")
    if character_count is not None or line_count is not None:
        return f"`{int(character_count or 0):,}` chars · `{int(line_count or 0):,}` lines"
    if payload.get("byte_count") is not None:
        return f"`{_format_asset_size(int(payload['byte_count']))}`"
    return "`unknown`"


def _format_asset_size(byte_count: int) -> str:
    value = max(0, int(byte_count))
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value / (1024 * 1024):.1f} MiB"


def _format_render_duration(value: Any) -> str:
    try:
        total = max(0, int(float(value)))
    except (TypeError, ValueError):
        return "unknown"
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _safe_stream_content(content: str) -> str:
    lines = content.splitlines(keepends=True)
    for index in range(1, len(lines)):
        if "|" in lines[index - 1] and _TABLE_DELIMITER.match(lines[index]):
            prefix = "".join(lines[: index - 1]).rstrip()
            marker = "\n\n*(rendering table...)*"
            return (prefix + marker).strip()
    plan = plan_markdown_messages(content, max_chars=1750)
    if not plan.segments:
        return ""
    first = plan.segments[0]
    rendered = first.content
    if len(plan.segments) > 1:
        rendered += "\n\n*(stream continues; complete block-preserving output will follow)*"
    return rendered[:1850]


def _bounded_discord_text(content: str, limit: int) -> str:
    normalized = " ".join(content.split())
    return normalized if len(normalized) <= limit else normalized[: limit - 1] + "…"


def _discord_embeds(payloads: tuple[dict[str, Any], ...]) -> list[discord.Embed]:
    return [discord.Embed.from_dict(payload) for payload in payloads]


def _taskdeck_embed_payloads(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    if payload.get("type") != "taskdeck":
        return ()
    cards = payload.get("cards")
    if not isinstance(cards, list):
        raise RenderPermanentError("TaskDeck render payload has no card list")
    if not cards:
        return (
            {
                "title": "TaskDeck",
                "description": "No observed tasks.",
                "color": 0x747F8D,
            },
        )
    metadata = payload.get("taskdeck")
    if not isinstance(metadata, dict):
        raise RenderPermanentError("TaskDeck render payload has no panel metadata")
    try:
        page = max(0, int(metadata.get("page", 0)))
    except (TypeError, ValueError) as error:
        raise RenderPermanentError("TaskDeck render payload has an invalid page") from error
    visible = cards[page * 8 : (page + 1) * 8]
    selected_token = str(metadata.get("selected_card_token") or "")
    expanded = bool(metadata.get("expanded"))
    embeds: list[dict[str, Any]] = []
    for card in visible:
        if not isinstance(card, dict):
            raise RenderPermanentError("TaskDeck render payload contains an invalid card")
        title = _bounded_discord_text(str(card.get("title") or "Untitled task"), 100)
        state = _bounded_discord_text(str(card.get("state") or "unknown"), 40)
        kind = _bounded_discord_text(str(card.get("kind") or "unknown"), 40)
        token = str(card.get("card_token") or "")
        is_selected = token == selected_token
        is_expanded = is_selected and expanded
        progress = _bounded_discord_text(
            str(card.get("progress_summary") or "No progress reported."),
            360,
        )
        description = progress
        if is_expanded and card.get("detail_artifact"):
            filename = f"task-{token}-detail.md"
            description = (
                f"{progress}\n\nFull detail is attached as `{filename}` and remains "
                "available from Download."
            )
        fields = [
            {"name": "State", "value": f"`{state}`", "inline": True},
            {"name": "Type", "value": f"`{kind}`", "inline": True},
            {
                "name": "Elapsed",
                "value": f"`{_taskdeck_elapsed(card)}`",
                "inline": True,
            },
        ]
        dependencies = card.get("dependencies")
        if is_expanded and isinstance(dependencies, list) and dependencies:
            fields.append(
                {
                    "name": "Dependencies",
                    "value": _bounded_discord_text(
                        ", ".join(f"`{value}`" for value in dependencies[:10]),
                        140,
                    ),
                    "inline": False,
                }
            )
        artifact_links = card.get("artifact_links")
        if is_expanded and isinstance(artifact_links, list) and artifact_links:
            fields.append(
                {
                    "name": "Artifacts",
                    "value": _bounded_discord_text(
                        "\n".join(str(value) for value in artifact_links[:8]),
                        160,
                    ),
                    "inline": False,
                }
            )
        embed: dict[str, Any] = {
            "title": f"{_taskdeck_state_icon(state)} {title}",
            "description": _bounded_discord_text(description, 780),
            "color": _taskdeck_state_color(state),
            "fields": fields,
        }
        if is_selected:
            embed["footer"] = {"text": "Selected · expanded" if expanded else "Selected"}
        embeds.append(embed)
    return tuple(embeds)


def _taskdeck_elapsed(card: dict[str, Any]) -> str:
    elapsed = card.get("elapsed")
    if isinstance(elapsed, str) and elapsed:
        return _bounded_discord_text(elapsed, 32)
    first_seen = card.get("first_seen_at")
    observed_end = card.get("terminal_at") or card.get("last_progress_at")
    if not isinstance(first_seen, (int, float)) or not isinstance(observed_end, (int, float)):
        return "unknown"
    total = max(0, int(observed_end - first_seen))
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _taskdeck_state_icon(state: str) -> str:
    return {
        "running": "▶",
        "idle": "⏸",
        "completed": "✅",
        "failed": "❌",
        "cancelled": "⏹",
        "unknown": "⚠️",
    }.get(state, "•")


def _taskdeck_state_color(state: str) -> int:
    return {
        "running": 0x5865F2,
        "idle": 0xFEE75C,
        "completed": 0x57F287,
        "failed": 0xED4245,
        "cancelled": 0x747F8D,
        "unknown": 0xF0B232,
    }.get(state, 0x747F8D)


def _render_view(
    payload: dict[str, Any],
    *,
    enable_task_actions: bool = False,
) -> discord.ui.View | None:
    interaction = payload.get("interaction")
    if isinstance(interaction, dict) and interaction.get("state") == "pending":
        return _interaction_view(interaction)
    return _taskdeck_view(payload, enable_task_actions=enable_task_actions)


def _interaction_view(metadata: dict[str, Any]) -> discord.ui.View:
    plan = DiscordInteractionAdapter.plan(metadata)
    interaction_id = plan.interaction_id
    view = discord.ui.View(timeout=None)
    if plan.form is not None:
        view.add_item(
            discord.ui.Button(
                label="Fill form",
                style=discord.ButtonStyle.primary,
                custom_id=f"cdi:{interaction_id}:form",
            )
        )
        view.add_item(
            discord.ui.Button(
                label="Decline",
                style=discord.ButtonStyle.secondary,
                custom_id=f"cdi:{interaction_id}:decline",
            )
        )
        view.add_item(
            discord.ui.Button(
                label="Cancel",
                style=discord.ButtonStyle.secondary,
                custom_id=f"cdi:{interaction_id}:cancel",
            )
        )
        return view
    if plan.kind != "mcp_oauth" and plan.use_buttons:
        for index, choice in enumerate(plan.choices):
            view.add_item(
                discord.ui.Button(
                    label=_bounded_discord_text(choice, 80),
                    style=discord.ButtonStyle.primary,
                    custom_id=f"cdi:{interaction_id}:choice-{index}",
                )
            )
    elif plan.kind != "mcp_oauth" and plan.use_select:
        view.add_item(
            discord.ui.Select(
                custom_id=f"cdi:{interaction_id}:select",
                placeholder="Choose a response",
                options=[
                    discord.SelectOption(
                        label=_bounded_discord_text(str(choice), 100),
                        value=str(index),
                    )
                    for index, choice in enumerate(plan.choices)
                ],
            )
        )
    if plan.allow_freeform or (
        plan.kind == "exit_plan_mode" and len(plan.choices) > DiscordInteractionAdapter.SELECT_LIMIT
    ):
        view.add_item(
            discord.ui.Button(
                label=(
                    "Enter a choice"
                    if len(plan.choices) > DiscordInteractionAdapter.SELECT_LIMIT
                    and not metadata.get("allowFreeform")
                    else "Write a response"
                ),
                style=discord.ButtonStyle.secondary,
                custom_id=f"cdi:{interaction_id}:freeform",
            )
        )
    if plan.kind == "mcp_oauth":
        view.add_item(
            discord.ui.Button(
                label="Cancel authorization",
                style=discord.ButtonStyle.danger,
                custom_id=f"cdi:{interaction_id}:cancel",
            )
        )
    return view


def _elicitation_placeholder(field: ElicitationField) -> str | None:
    if field.enum:
        return _bounded_discord_text(
            "Allowed: " + ", ".join(str(item) for item in field.enum),
            100,
        )
    if field.value_type == "boolean":
        return "true or false"
    if field.value_type == "array":
        return 'JSON array, for example ["one", "two"]'
    if field.description:
        return _bounded_discord_text(field.description, 100)
    return None


def _coerce_elicitation_value(field: ElicitationField, value: str) -> Any:
    if field.value_type == "string":
        return value
    if field.value_type == "integer":
        return int(value)
    if field.value_type == "number":
        return float(value)
    if field.value_type == "boolean":
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1"}:
            return True
        if normalized in {"false", "no", "0"}:
            return False
        raise ValueError(f"{field.name} must be true or false")
    decoded = json.loads(value)
    if not isinstance(decoded, list):
        raise ValueError(f"{field.name} must be a JSON array")
    return decoded


def _taskdeck_view(
    payload: dict[str, Any],
    *,
    enable_task_actions: bool = False,
) -> discord.ui.View | None:
    metadata = payload.get("taskdeck")
    if not isinstance(metadata, dict):
        return None
    panel_id = str(metadata["panel_id"])
    revision = int(metadata["revision"])
    options = metadata.get("options")
    if not isinstance(options, list) or not options:
        return None
    selected = str(metadata.get("selected_card_token") or "")
    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Select(
            custom_id=f"cdtd:{panel_id}:{revision}:select",
            placeholder="Select a task",
            options=[
                discord.SelectOption(
                    label=str(option.get("label") or "Untitled task"),
                    value=str(option["value"]),
                    description=str(option.get("state") or "unknown"),
                    default=str(option["value"]) == selected,
                )
                for option in options[:25]
            ],
        )
    )
    view.add_item(
        discord.ui.Button(
            label="Collapse" if metadata.get("expanded") else "Expand",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cdtd:{panel_id}:{revision}:toggle",
        )
    )
    page = int(metadata.get("page", 0))
    page_count = int(metadata.get("page_count", 1))
    view.add_item(
        discord.ui.Button(
            label="Previous",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cdtd:{panel_id}:{revision}:prev",
            disabled=page <= 0,
        )
    )
    view.add_item(
        discord.ui.Button(
            label="Next",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cdtd:{panel_id}:{revision}:next",
            disabled=page + 1 >= page_count,
        )
    )
    actions = metadata.get("actions")
    if isinstance(actions, list):
        action_styles = {
            "cancel": discord.ButtonStyle.danger,
            "promote": discord.ButtonStyle.primary,
            "message": discord.ButtonStyle.primary,
            "remove": discord.ButtonStyle.secondary,
            "download": discord.ButtonStyle.secondary,
        }
        for action in actions:
            action_name = str(action)
            if action_name != "download" and not enable_task_actions:
                continue
            if action_name not in action_styles:
                continue
            view.add_item(
                discord.ui.Button(
                    label=action_name.title(),
                    style=action_styles[action_name],
                    custom_id=(f"cdtd:{panel_id}:{revision}:{action_name}:{selected}"),
                )
            )
    return view


def _interaction_result_text(result: str) -> str:
    if result == "resolved":
        return "Response sent to Copilot."
    if result == "expired":
        return "This Copilot input request has expired or was already answered."
    return "This response is not valid for the current request."


def _discord_files(assets: list[TableAsset]) -> list[discord.File]:
    return [discord.File(io.BytesIO(asset.content), filename=asset.filename) for asset in assets]


def _prepare_discord_assets(
    content: str,
    assets: list[TableAsset],
    *,
    max_bytes: int,
) -> tuple[str, list[TableAsset]]:
    if max_bytes < 1:
        raise ValueError("Discord upload size must be positive")
    prepared: list[TableAsset] = []
    split_files = 0
    split_parts = 0
    omitted_images = 0
    for asset in assets:
        if len(asset.content) <= max_bytes:
            prepared.append(asset)
            continue
        if asset.media_type.startswith("image/"):
            omitted_images += 1
            continue
        split_files += 1
        part_count = (len(asset.content) + max_bytes - 1) // max_bytes
        split_parts += part_count
        path = Path(asset.filename)
        stem = path.stem[:120] or "artifact"
        suffix = path.suffix
        for part_index in range(part_count):
            start = part_index * max_bytes
            prepared.append(
                TableAsset(
                    filename=(f"{stem}.part-{part_index + 1:03d}-of-{part_count:03d}{suffix}"),
                    media_type=asset.media_type,
                    content=asset.content[start : start + max_bytes],
                )
            )
    notes: list[str] = []
    if split_files:
        notes.append(
            f"{split_files} large attachment(s) were split into "
            f"{split_parts} upload-safe file(s); concatenate matching parts in order."
        )
    if omitted_images:
        notes.append(
            f"{omitted_images} oversized PNG/image preview(s) were omitted rather than "
            "splitting invalid image bytes; use the accompanying Markdown/CSV source."
        )
    if notes:
        note = "\n\n" + " ".join(notes)
        available = max(1, 1850 - len(note))
        if len(content) > available:
            content = content[: available - 1] + "…"
        content += note
    return content, prepared


def _render_delivery_error(error: Exception) -> RenderDeliveryError:
    if isinstance(error, discord.HTTPException):
        if error.status == 429:
            headers = getattr(error.response, "headers", {})
            retry_header = headers.get("Retry-After", 1)
            try:
                retry_after = max(0.1, float(retry_header))
            except (TypeError, ValueError):
                retry_after = 1.0
            return RenderRateLimited(retry_after)
        if error.status >= 500 or error.status == 408:
            return RenderTransientError(str(error))
        return RenderPermanentError(str(error))
    return RenderTransientError(str(error))


def _thread_name(prompt: str) -> str:
    value = " ".join(prompt.split())
    return (value[:70] or "New Copilot session").strip()


def _parent_channel_id(interaction: discord.Interaction) -> str:
    channel = interaction.channel
    if isinstance(channel, discord.Thread):
        return str(channel.parent_id)
    if interaction.channel_id is None:
        raise ValueError("interaction has no Discord channel")
    return str(interaction.channel_id)


def _message_parent_channel_id(message: discord.Message) -> str:
    if isinstance(message.channel, discord.Thread):
        if message.channel.parent_id is None:
            raise CDScopeError("the source thread has no parent channel")
        return str(message.channel.parent_id)
    return str(message.channel.id)


def _payload_session_hint(payload: dict[str, Any]) -> str | None:
    value = payload.get("render_destination", payload.get("session_id"))
    return None if value is None else str(value)


def _render_batch_nonce(
    session_id: str,
    delivery_id: str,
    agent_id: str,
    index: int,
) -> str:
    value = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"copilotd:{session_id}:{delivery_id}:{agent_id}:{index}",
    )
    return str(value.int & ((1 << 63) - 1))


def _render_delivery_family(delivery_id: str) -> str:
    match = re.fullmatch(
        r"(?P<family>.+):payload:\d+:[0-9a-f]{16}",
        delivery_id,
    )
    return delivery_id if match is None else match.group("family")


def _render_batch_hash(batch: DiscordRenderBatch) -> str:
    digest = hashlib.sha256()
    digest.update(batch.content.encode("utf-8"))
    digest.update(b"\0embeds\0")
    digest.update(
        json.dumps(
            batch.embeds,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    for asset in batch.assets:
        digest.update(b"\0")
        digest.update(asset.filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update(asset.media_type.encode("utf-8"))
        digest.update(b"\0")
        digest.update(asset.content)
    return digest.hexdigest()


def _append_assets_to_batches(
    batches: list[DiscordRenderBatch],
    assets: list[TableAsset],
) -> list[DiscordRenderBatch]:
    if not assets:
        return batches
    first = batches[0]
    capacity = max(0, 10 - len(first.assets))
    batches[0] = DiscordRenderBatch(
        content=first.content,
        assets=first.assets + tuple(assets[:capacity]),
        embeds=first.embeds,
    )
    for index in range(capacity, len(assets), 10):
        batches.append(
            DiscordRenderBatch(
                content="Attached durable output artifact(s).",
                assets=tuple(assets[index : index + 10]),
            )
        )
    return batches


def _image_media_type(filename: str) -> str:
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }.get(Path(filename).suffix.lower(), "application/octet-stream")


def _parse_json_object(value: str, *, field: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise CDInputError(f"{field} is not valid JSON: {error.msg}") from error
    if not isinstance(parsed, dict):
        raise CDInputError(f"{field} must be a JSON object")
    return parsed


def _parse_json_list(value: str, *, field: str) -> list[Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise CDInputError(f"{field} is not valid JSON: {error.msg}") from error
    if not isinstance(parsed, list):
        raise CDInputError(f"{field} must be a JSON array")
    return parsed


async def _json_text_async(operation: Awaitable[Mapping[str, Any]]) -> str:
    value = await operation
    return json.dumps(dict(value), ensure_ascii=False, indent=2, sort_keys=True)


def _freshness_label(snapshot: Mapping[str, Any]) -> str:
    stale = bool(snapshot.get("_stale"))
    observed = snapshot.get("_observed_at")
    if observed is None:
        return "stale/unknown" if stale else "live"
    age = max(0, int(time.time() - float(observed)))
    return f"last-seen {age}s ago (stale)" if stale else f"live ({age}s old)"


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _compact_json(value: Any, limit: int = 320) -> str:
    rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return _bounded_discord_text(rendered, limit)


def _short_hash(value: Any, *, missing: str = "unknown") -> str:
    if value is None:
        return missing
    return _bounded_discord_text(str(value), 16)


def _age_label(value: Any, now: float) -> str:
    if value is None:
        return "unknown"
    try:
        age = max(0, int(now - float(value)))
    except (TypeError, ValueError):
        return "unknown"
    return f"{age}s ago"


def _age_or_future(value: Any, now: float) -> str:
    if value is None:
        return "unknown"
    try:
        delta = int(float(value) - now)
    except (TypeError, ValueError):
        return "unknown"
    if delta >= 0:
        return f"in {delta}s"
    return f"expired {-delta}s ago"


def _projection_number(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, int | float) or isinstance(value, bool):
        return "unknown"
    return f"{value:,}" if isinstance(value, int) else f"{value:.3f}"


def _session_projection_summary(
    row: Any,
    *,
    kind: str,
    now: float,
) -> str:
    if row is None:
        return "unavailable (no durable projection)"
    payload = _json_object(row["payload_json"])
    stale = bool(row["stale"])
    stale_reason = str(row["stale_reason"] or "unspecified") if stale else "none"
    common = (
        f"source `{row['source_type'] or 'unknown'}` · generation "
        f"`{row['runtime_generation']}` · fence `{row['owner_fence_token']}` · observed "
        f"`{_age_label(row['observed_at'], now)}` · reconciled "
        f"`{_age_label(row['reconciled_at'], now)}` · stale `{stale}` "
        f"(reason `{_bounded_discord_text(stale_reason, 120)}`)"
    )
    if kind == "context":
        return (
            f"tokens `{_projection_number(payload, 'totalTokens')}` / "
            f"`{_projection_number(payload, 'limit')}` · model "
            f"`{_bounded_discord_text(str(payload.get('modelName', 'unknown')), 80)}` · "
            f"{common}"
        )
    if kind == "usage":
        return (
            f"requests `{_projection_number(payload, 'totalUserRequests')}` · last call "
            f"`{_projection_number(payload, 'lastCallInputTokens')}` input / "
            f"`{_projection_number(payload, 'lastCallOutputTokens')}` output · model "
            f"`{_bounded_discord_text(str(payload.get('currentModel', 'unknown')), 80)}` · "
            f"{common}"
        )
    raise ValueError(f"unsupported projection kind: {kind}")


def _bool_unknown(value: Any) -> str:
    return "unknown" if value is None else str(bool(value)).lower()


def _text_file(content: str, name: str) -> discord.File:
    filename = re.sub(r"[^a-z0-9.-]+", "-", name.lower()).strip("-") or "command"
    return discord.File(
        io.BytesIO(content.encode("utf-8")),
        filename=f"{filename[:80]}.txt",
    )


def _is_unknown_interaction(error: discord.HTTPException) -> bool:
    return int(getattr(error, "code", 0) or 0) == 10062


def _map_command_error(error: BaseException) -> CDCommandError:
    if isinstance(error, CDCommandError):
        return error
    if isinstance(
        error,
        DetachBlocked | SessionDeletionBlocked | SessionDeletionUnknown | SessionNotReady,
    ):
        return CDSessionStateError(str(error))
    if isinstance(error, SessionCreationUnknown):
        return CDResumeError(str(error))
    if isinstance(error, AttachmentError):
        return CDInputError(str(error))
    if isinstance(error, PermissionError | FileNotFoundError):
        return CDPathError(str(error))
    if isinstance(error, discord.HTTPException):
        return CDDiscordError(str(error))
    if isinstance(error, app_commands.AppCommandError | ValueError | json.JSONDecodeError):
        return CDInputError(str(error))
    if isinstance(error, RuntimeError):
        return CDRuntimeError(str(error))
    return CDRuntimeError(str(error) or error.__class__.__name__)


def _discord_parent_type(interaction: discord.Interaction) -> DiscordParentType:
    channel = interaction.channel
    parent = channel.parent if isinstance(channel, discord.Thread) else channel
    if isinstance(parent, discord.ForumChannel):
        return DiscordParentType.FORUM
    if isinstance(parent, discord.TextChannel):
        return DiscordParentType.TEXT
    raise ValueError("project layout requires a text or forum Discord parent")


def _csv_options(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _key_value_options(value: str | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in _csv_options(value):
        name, separator, option = item.partition("=")
        if not separator or not name.strip():
            raise ValueError("key/value options must use comma-separated NAME=VALUE pairs")
        result[name.strip()] = option
    return result


async def run_discord_bot(settings: Settings) -> bool:
    if settings.discord_token is None:
        raise RuntimeError("COPILOTD_DISCORD_TOKEN is required")
    bot = CopilotDiscordBot(settings)
    try:
        await bot.start(settings.discord_token.get_secret_value(), reconnect=True)
        if bot._fatal_worker_error is not None:
            raise RuntimeError("critical copilotD worker failed") from bot._fatal_worker_error
    finally:
        if not bot.is_closed():
            await bot.close()
    return bot.restart_requested
