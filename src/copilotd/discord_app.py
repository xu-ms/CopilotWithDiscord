from __future__ import annotations

import asyncio
import io
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any

import discord
import structlog
from discord import app_commands
from discord.ext import commands

from copilotd.config import Settings
from copilotd.core.attachments import AttachmentError, AttachmentService
from copilotd.core.bindings import SessionBinding, SessionBindingRepository
from copilotd.core.extensions import ExtensionConfigRepository
from copilotd.core.interactions import (
    DiscordInteractionAdapter,
    ElicitationField,
    ElicitationForm,
)
from copilotd.core.projects import ProjectRegistry
from copilotd.core.recovery import RecoveryInventoryReport, StartupRecoveryInventory
from copilotd.core.session_runtime import SessionRuntime
from copilotd.core.sessions import (
    CreationIntentRepository,
    SessionCreationService,
    SessionRegistry,
    ThreadReference,
)
from copilotd.core.supervisor import ExecutionStallMonitor
from copilotd.core.task_registry import TaskRegistry
from copilotd.ops.heartbeat import HeartbeatWriter
from copilotd.render.markdown import MarkdownAssembler, TableBlock, TextBlock
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


class CopilotDiscordBot(commands.Bot):
    def __init__(self, settings: Settings) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
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
        )
        self.heartbeat = HeartbeatWriter(self.database, settings.heartbeat_path)
        self.projects: ProjectRegistry | None = None
        self.bindings: SessionBindingRepository | None = None
        self.extension_configs: ExtensionConfigRepository | None = None
        self.sessions: SessionRegistry | None = None
        self.creation: SessionCreationService | None = None
        self.dispatcher: RenderOutboxDispatcher | None = None
        self._tasks = TaskRegistry()
        self._owner_id = f"discord:{uuid.uuid4()}"
        self._commands_registered = False
        self._fatal_worker_error: BaseException | None = None

    async def setup_hook(self) -> None:
        await self.database.open()
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
                attachment_resolver=self.attachment_service.sdk_attachments,
                capabilities=self.capabilities,
                task_registry=self._tasks,
                extension_configs=self.extension_configs,
            )

        self.sessions = SessionRegistry(self.bindings, runtime_factory)
        self.creation = SessionCreationService(
            projects=self.projects,
            intents=CreationIntentRepository(self.database),
            bindings=self.bindings,
            sessions=self.sessions,
            threads=DiscordThreadGateway(self),
            extension_configs=self.extension_configs,
        )
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
        self.dispatcher = RenderOutboxDispatcher(self.database, self)
        self._tasks.create(
            self._render_loop(),
            name="discord-render-outbox",
            source="render-outbox",
        )
        self._tasks.create(
            self.heartbeat.run(),
            name="copilotd-heartbeat",
            source="heartbeat",
        )
        self._register_application_commands()
        if self.settings.discord_guild_id is not None:
            guild = discord.Object(id=self.settings.discord_guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def close(self) -> None:
        self.heartbeat.set_gateway("down")
        self.heartbeat.runtime_state = "down"
        errors: list[Exception] = []
        if self.sessions is not None:
            try:
                await self.sessions.shutdown()
            except Exception as error:
                errors.append(error)
        try:
            await self._tasks.cancel_all()
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
        await super().close()
        if errors:
            raise ExceptionGroup("copilotD shutdown failed", errors)

    async def _task_failure_loop(self) -> None:
        while True:
            failure = await self._tasks.errors.get()
            try:
                self.heartbeat.runtime_state = "down"
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
                self._fatal_worker_error = failure.error
                self.heartbeat.set_gateway("down")
                await super().close()
                return
            finally:
                self._tasks.errors.task_done()

    async def on_ready(self) -> None:
        self.heartbeat.set_gateway("ready")
        await logger.ainfo(
            "discord_ready",
            user=None if self.user is None else str(self.user),
            guilds=len(self.guilds),
        )

    async def on_disconnect(self) -> None:
        self.heartbeat.set_gateway("reconnecting")

    async def on_resumed(self) -> None:
        self.heartbeat.set_gateway("ready")

    async def on_interaction(self, interaction: discord.Interaction) -> None:
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
        if len(parts) != 4:
            await interaction.response.send_message(
                "This TaskDeck control is invalid.",
                ephemeral=True,
            )
            return
        _, panel_id, revision_text, action_text = parts
        action_map = {
            "select": "select",
            "toggle": "toggle",
            "prev": "previous",
            "next": "next",
        }
        action = action_map.get(action_text)
        if action is None or not revision_text.isdigit() or interaction.message is None:
            await interaction.response.send_message(
                "This TaskDeck control is invalid.",
                ephemeral=True,
            )
            return
        values = data.get("values")
        card_token = (
            str(values[0]) if action == "select" and isinstance(values, list) and values else None
        )
        await interaction.response.defer()
        runtime = await self._interaction_runtime(interaction)
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
        if result != "updated":
            await interaction.followup.send(
                (
                    "TaskDeck changed; use the latest controls."
                    if result == "stale"
                    else "This TaskDeck control has expired."
                ),
                ephemeral=True,
            )

    async def _handle_direct_interaction(
        self,
        interaction: discord.Interaction,
        custom_id: str,
    ) -> None:
        parts = custom_id.split(":")
        if len(parts) != 3:
            await interaction.response.send_message(
                "This Copilot input control is invalid.",
                ephemeral=True,
            )
            return
        _, interaction_id, action = parts
        if action == "freeform":
            await interaction.response.send_modal(InteractionResponseModal(self, interaction_id))
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
            await interaction.response.send_message(
                "This Copilot input control is invalid.",
                ephemeral=True,
            )
            return
        result = await runtime.respond_interaction(
            interaction_id,
            selection=int(values[0]),
        )
        await interaction.response.send_message(
            _interaction_result_text(result),
            ephemeral=True,
        )

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None:
            return
        prompt = self._clean_prompt(message)
        if isinstance(message.channel, discord.Thread):
            binding = await self._require_bindings().by_thread(str(message.channel.id))
            if binding is None or (not prompt and not message.attachments):
                return
            runtime = self._require_sessions().for_thread(str(message.channel.id))
            if runtime is None:
                runtime = await self._require_sessions().replace(binding)
                await runtime.attach_resume()
            try:
                prepared = await self.attachment_service.prepare(
                    source_kind="discord-message",
                    source_id=str(message.id),
                    session_id=runtime.binding.sdk_session_id,
                    attachments=message.attachments,
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
            prepared = await self.attachment_service.prepare(
                source_kind="discord-message",
                source_id=str(message.id),
                session_id=runtime.binding.sdk_session_id,
                attachments=message.attachments,
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
            thread = await self._thread_for_session(session_id)
            content, assets = await _discord_render(payload)
            content, assets = _prepare_discord_assets(
                content,
                assets,
                max_bytes=self.settings.discord_upload_max_bytes,
            )
            message = await thread.send(
                content=content or "\u200b",
                files=_discord_files(assets[:10]),
                view=_render_view(payload),
                silent=True,
            )
            for index in range(10, len(assets), 10):
                await thread.send(
                    files=_discord_files(assets[index : index + 10]),
                    silent=True,
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
            discord_message_id=message.id,
        )
        return str(message.id)

    async def edit(
        self,
        *,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
    ) -> None:
        try:
            session_id = _payload_session_hint(payload)
            thread = (
                await self._thread_for_session(session_id)
                if session_id is not None
                else await self._find_thread_for_message(message_id)
            )
            message = await thread.fetch_message(int(message_id))
            content, assets = await _discord_render(payload)
            content, assets = _prepare_discord_assets(
                content,
                assets,
                max_bytes=self.settings.discord_upload_max_bytes,
            )
            await message.edit(
                content=content or "\u200b",
                attachments=_discord_files(assets[:10]),
                view=_render_view(payload),
            )
            for index in range(10, len(assets), 10):
                await thread.send(
                    files=_discord_files(assets[index : index + 10]),
                    silent=True,
                )
        except RenderDeliveryError:
            raise
        except (discord.HTTPException, OSError, TimeoutError) as error:
            raise _render_delivery_error(error) from error
        await logger.adebug("render_edited", lane=lane, discord_message_id=message_id)

    async def _render_loop(self) -> None:
        while True:
            dispatcher = self.dispatcher
            if dispatcher is None:
                return
            delivered = await dispatcher.dispatch_once()
            await asyncio.sleep(0.2 if delivered else 1.0)

    async def _thread_for_session(self, session_id: str) -> discord.Thread:
        binding = await self._require_bindings().by_session(session_id)
        if binding is None:
            raise RuntimeError(f"no Discord binding for SDK session {session_id}")
        channel = self.get_channel(int(binding.thread_id))
        if channel is None:
            channel = await self.fetch_channel(int(binding.thread_id))
        if not isinstance(channel, discord.Thread):
            raise RuntimeError(f"bound Discord thread is unavailable: {binding.thread_id}")
        if channel.archived and not channel.locked:
            await channel.edit(archived=False)
        return channel

    async def _find_thread_for_message(self, message_id: str) -> discord.Thread:
        rows = await self.database.fetchall("SELECT thread_id FROM session_bindings")
        for row in rows:
            channel = self.get_channel(int(row["thread_id"]))
            if not isinstance(channel, discord.Thread):
                continue
            try:
                await channel.fetch_message(int(message_id))
            except discord.NotFound:
                continue
            return channel
        raise RuntimeError(f"Discord message is not mapped to a session: {message_id}")

    def _register_application_commands(self) -> None:
        if self._commands_registered:
            return
        self._commands_registered = True
        session = app_commands.Group(name="session", description="Manage Copilot sessions")
        project = app_commands.Group(name="project", description="Manage channel projects")
        model = app_commands.Group(name="model", description="Inspect or change Copilot models")
        queue = app_commands.Group(name="queue", description="Manage the durable message queue")

        @session.command(name="new", description="Create a new Copilot session thread")
        async def session_new(interaction: discord.Interaction, prompt: str = "") -> None:
            await interaction.response.defer(ephemeral=True)
            channel_id = _parent_channel_id(interaction)
            runtime = await self._require_creation().create_from_source(
                channel_id=channel_id,
                source_kind="slash",
                source_id=str(interaction.id),
                prompt=prompt or "Start a new interactive Copilot session.",
                thread_name=_thread_name(prompt or "New Copilot session"),
                send_initial_prompt=bool(prompt),
            )
            await interaction.followup.send(
                f"Session created: <#{runtime.binding.thread_id}>",
                ephemeral=True,
            )

        @session.command(name="list", description="List copilotD session bindings")
        async def session_list(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            rows = await self.database.fetchall(
                """
                SELECT thread_id, sdk_session_id, binding_intent,
                       attachment_state, cwd_snapshot
                FROM session_bindings
                WHERE binding_intent != 'deleted'
                ORDER BY updated_at DESC LIMIT 30
                """
            )
            if not rows:
                await interaction.followup.send("No copilotD sessions.", ephemeral=True)
                return
            lines = [
                (
                    f"<#{row['thread_id']}> · `{row['sdk_session_id']}` · "
                    f"`{row['binding_intent']}/{row['attachment_state']}` · "
                    f"`{_bounded_discord_text(str(row['cwd_snapshot']), 90)}`"
                )
                for row in rows
            ]
            await _send_ephemeral_text(interaction, "\n".join(lines), "copilotd-sessions.txt")

        @session.command(name="info", description="Show the current session state")
        async def session_info(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            binding = await self._interaction_binding(interaction)
            await interaction.followup.send(
                (
                    f"`{binding.sdk_session_id}`\n"
                    f"cwd: `{binding.cwd_snapshot}`\n"
                    f"attachment: `{binding.attachment_state}`\n"
                    f"mode: desired `{binding.desired_mode}`, runtime `{binding.runtime_mode}`\n"
                    f"permission: `{binding.permission_posture}`"
                ),
                ephemeral=True,
            )

        @session.command(name="abort", description="Abort the current Copilot turn")
        async def session_abort(
            interaction: discord.Interaction,
            clear_local_queue: bool = True,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            removed = await runtime.clear_queue() if clear_local_queue else 0
            await runtime.abort(idempotency_key=f"interaction:{interaction.id}")
            await interaction.followup.send(
                f"Abort requested; cancelled {removed} local queue item(s).",
                ephemeral=True,
            )

        @session.command(name="close", description="Close without deleting Copilot history")
        async def session_close(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            await runtime.close(
                idempotency_key=f"interaction:{interaction.id}",
                force=force,
            )
            await interaction.followup.send("Session closed.", ephemeral=True)

        @session.command(name="resume", description="Resume this thread's original session")
        async def session_resume(
            interaction: discord.Interaction,
            session_id: str | None = None,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            if isinstance(interaction.channel, discord.Thread):
                binding = await self._interaction_binding(interaction)
                if session_id is not None and session_id != binding.sdk_session_id:
                    raise ValueError("this thread cannot be rebound to another Copilot session")
            else:
                if session_id is None:
                    raise ValueError("session_id is required outside a session thread")
                binding = await self._require_bindings().by_session(session_id)
                if binding is None:
                    raise ValueError("the requested copilotD session is unknown")
            runtime = self._require_sessions().for_thread(binding.thread_id)
            if runtime is None or runtime.state.value in {
                "closed",
                "fenced",
                "recovery_unknown",
            }:
                runtime = await self._require_sessions().replace(binding)
            if runtime.state.value == "detached":
                await runtime.attach_resume(reactivate=True)
            thread = await self._thread_for_session(binding.sdk_session_id)
            await interaction.followup.send(
                f"Session resumed: {thread.mention}",
                ephemeral=True,
            )

        @session.command(name="rename", description="Rename this Discord session thread")
        async def session_rename(interaction: discord.Interaction, name: str) -> None:
            await interaction.response.defer(ephemeral=True)
            if not isinstance(interaction.channel, discord.Thread):
                raise ValueError("this command must be used inside a session thread")
            normalized = " ".join(name.split())
            if not normalized:
                raise ValueError("session name cannot be empty")
            await interaction.channel.edit(name=normalized[:100])
            await interaction.followup.send("Session thread renamed.", ephemeral=True)

        @project.command(name="bind", description="Bind future sessions to a local directory")
        async def project_bind(interaction: discord.Interaction, path: str) -> None:
            await interaction.response.defer(ephemeral=True)
            snapshot = await self._require_projects().bind(
                _parent_channel_id(interaction),
                Path(path),
            )
            await interaction.followup.send(
                f"Future sessions use `{snapshot.cwd}`.",
                ephemeral=True,
            )

        @project.command(name="unbind", description="Return future sessions to HOME")
        async def project_unbind(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            snapshot = await self._require_projects().unbind(_parent_channel_id(interaction))
            await interaction.followup.send(
                f"Future sessions use implicit HOME `{snapshot.cwd}`.",
                ephemeral=True,
            )

        @project.command(name="info", description="Show the channel project resolution")
        async def project_info(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            snapshot = await self._require_projects().resolve(_parent_channel_id(interaction))
            await interaction.followup.send(
                f"source: `{snapshot.source}`\ncwd: `{snapshot.cwd}`",
                ephemeral=True,
            )

        @model.command(name="list", description="List models available to this Copilot account")
        async def model_list(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
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
                    )
                    if enabled
                ]
                multiplier = billing.get("multiplier")
                suffix = (
                    f"; multiplier {multiplier:g}" if isinstance(multiplier, int | float) else ""
                )
                lines.append(
                    f"- `{item['id']}` — {item['name']}"
                    f" ({', '.join(features) or 'standard'}{suffix})"
                )
            await _send_ephemeral_text(interaction, "\n".join(lines), "copilot-models.txt")

        @model.command(name="set", description="Set the model for future messages")
        @app_commands.choices(
            context_tier=[
                app_commands.Choice(name="default", value="default"),
                app_commands.Choice(name="long context", value="long_context"),
            ]
        )
        async def model_set(
            interaction: discord.Interaction,
            model_id: str,
            effort: str | None = None,
            context_tier: app_commands.Choice[str] | None = None,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            observed = await runtime.set_model(
                model_id,
                reasoning_effort=effort,
                context_tier=None if context_tier is None else context_tier.value,
                idempotency_key=f"interaction:{interaction.id}",
            )
            await interaction.followup.send(
                "Model confirmed: "
                f"`{observed.get('modelId')}`"
                f", effort `{observed.get('reasoningEffort') or 'default'}`"
                f", context `{observed.get('contextTier') or 'default'}`.",
                ephemeral=True,
            )

        @self.tree.command(name="context", description="Show current Copilot context usage")
        async def context(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            snapshot = await (await self._interaction_runtime(interaction)).context_snapshot()
            if snapshot is None:
                text = "Copilot context information is currently unavailable."
            else:
                total = int(snapshot.get("totalTokens", 0))
                limit = int(snapshot.get("limit", 0))
                percent = 0 if limit <= 0 else total * 100 / limit
                text = (
                    f"model: `{snapshot.get('modelName', 'unknown')}`\n"
                    f"context: `{total:,}` / `{limit:,}` tokens ({percent:.1f}%)\n"
                    f"conversation: `{int(snapshot.get('conversationTokens', 0)):,}`\n"
                    f"system: `{int(snapshot.get('systemTokens', 0)):,}`\n"
                    f"tools: `{int(snapshot.get('toolDefinitionsTokens', 0)):,}`\n"
                    f"compaction threshold: "
                    f"`{int(snapshot.get('compactionThreshold', 0)):,}`"
                )
            await interaction.followup.send(text, ephemeral=True)

        @self.tree.command(name="usage", description="Show Copilot session usage")
        async def usage(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            snapshot = await (await self._interaction_runtime(interaction)).usage_snapshot()
            text = (
                f"model: `{snapshot.get('currentModel') or 'unknown'}`\n"
                f"user requests: `{int(snapshot.get('totalUserRequests', 0)):,}`\n"
                f"last call: `{int(snapshot.get('lastCallInputTokens', 0)):,}` input / "
                f"`{int(snapshot.get('lastCallOutputTokens', 0)):,}` output tokens\n"
                f"premium request units: "
                f"`{float(snapshot.get('totalPremiumRequestCost', 0)):.3f}`\n"
                f"nano-AIU: `{float(snapshot.get('totalNanoAiu') or 0):.3f}`"
            )
            await interaction.followup.send(text, ephemeral=True)

        @self.tree.command(name="autopilot", description="Enter or leave Copilot Autopilot mode")
        async def autopilot(
            interaction: discord.Interaction,
            enabled: bool = True,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            mode = "autopilot" if enabled else "interactive"
            await runtime.set_mode(mode, idempotency_key=f"interaction:{interaction.id}")
            await interaction.followup.send(f"Mode is now `{mode}`.", ephemeral=True)

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
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            selected = "enter" if action is None else action.value
            mode = "plan" if selected == "enter" else "interactive"
            await runtime.set_mode(mode, idempotency_key=f"interaction:{interaction.id}:mode")
            if selected == "enter" and prompt:
                await runtime.send(
                    prompt,
                    idempotency_key=f"interaction:{interaction.id}:prompt",
                    agent_mode="plan",
                )
            await interaction.followup.send(f"Mode is now `{mode}`.", ephemeral=True)

        @self.tree.command(name="steer", description="Steer the currently active Copilot turn")
        async def steer(interaction: discord.Interaction, text: str) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            await runtime.send(
                text,
                idempotency_key=f"interaction:{interaction.id}",
                mode="immediate",
            )
            await interaction.followup.send("Steer submitted.", ephemeral=True)

        @queue.command(name="add", description="Add a prompt to this session's durable queue")
        async def queue_add(interaction: discord.Interaction, text: str) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._interaction_runtime(interaction)
            reference = await runtime.send(
                text,
                idempotency_key=f"interaction:{interaction.id}:queue",
            )
            await interaction.followup.send(
                f"Prompt persisted as `{reference}`.",
                ephemeral=True,
            )

        @queue.command(name="list", description="List pending durable prompts")
        async def queue_list(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            items = await (await self._interaction_runtime(interaction)).queue_items()
            if not items:
                await interaction.followup.send("The durable queue is empty.", ephemeral=True)
                return
            lines = [
                (
                    f"`{item['id']}` · `{item['state']}` · "
                    f"{_bounded_discord_text(str(item['prompt']), 100)}"
                )
                for item in items[:20]
            ]
            if len(items) > 20:
                lines.append(f"… and {len(items) - 20} more")
            await interaction.followup.send("\n".join(lines), ephemeral=True)

        @queue.command(name="remove", description="Cancel one prompt before SDK submission")
        async def queue_remove(interaction: discord.Interaction, item_id: str) -> None:
            await interaction.response.defer(ephemeral=True)
            removed = await (await self._interaction_runtime(interaction)).cancel_queue_item(
                item_id
            )
            message = "Queue item cancelled." if removed else "Queue item is not cancellable."
            await interaction.followup.send(message, ephemeral=True)

        @queue.command(name="clear", description="Cancel all prompts not submitted to the SDK")
        async def queue_clear(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            removed = await (await self._interaction_runtime(interaction)).clear_queue()
            await interaction.followup.send(
                f"Cancelled {removed} queued prompt(s).",
                ephemeral=True,
            )

        @self.tree.error
        async def application_command_error(
            interaction: discord.Interaction,
            error: app_commands.AppCommandError,
        ) -> None:
            cause = error.original if isinstance(error, app_commands.CommandInvokeError) else error
            message = f"copilotD command failed: `{cause}`"
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            await logger.aerror(
                "discord_application_command_failed",
                command=None if interaction.command is None else interaction.command.name,
                error=str(cause),
            )

        manifest = self.command_manifest()
        self.tree.add_command(session)
        self.tree.add_command(project)
        if "model" in manifest:
            self.tree.add_command(model)
        self.tree.add_command(queue)
        for command_name in ("autopilot", "context", "plan", "usage"):
            if command_name not in manifest:
                self.tree.remove_command(command_name)

    def command_manifest(self) -> frozenset[str]:
        return self.capabilities.discord_command_roots()

    async def _interaction_binding(self, interaction: discord.Interaction) -> SessionBinding:
        if not isinstance(interaction.channel, discord.Thread):
            raise ValueError("this command must be used inside a copilotD session thread")
        binding = await self._require_bindings().by_thread(str(interaction.channel.id))
        if binding is None:
            raise ValueError("this thread is not bound to a Copilot session")
        return binding

    async def _interaction_runtime(self, interaction: discord.Interaction) -> SessionRuntime:
        binding = await self._interaction_binding(interaction)
        runtime = self._require_sessions().for_thread(binding.thread_id)
        if runtime is None:
            runtime = await self._require_sessions().replace(binding)
            await runtime.attach_resume()
        return runtime

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

    def _require_sessions(self) -> SessionRegistry:
        if self.sessions is None:
            raise RuntimeError("session registry is not initialized")
        return self.sessions

    def _require_creation(self) -> SessionCreationService:
        if self.creation is None:
            raise RuntimeError("session creation service is not initialized")
        return self.creation


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
    ) -> ThreadReference:
        channel = await self._channel(channel_id)
        thread_name = f"{name[:75]} [cd:{creation_token[:8]}]"
        if isinstance(channel, discord.TextChannel):
            try:
                source = await channel.fetch_message(int(source_id))
            except (discord.NotFound, ValueError):
                source = await channel.send(f"Starting copilotD session `{creation_token[:8]}`")
            thread = await source.create_thread(name=thread_name, auto_archive_duration=1440)
            return ThreadReference(str(thread.id))
        if isinstance(channel, discord.ForumChannel):
            created = await channel.create_thread(
                name=thread_name,
                content=f"Starting copilotD session `{creation_token[:8]}`",
                auto_archive_duration=1440,
            )
            return ThreadReference(str(created.thread.id))
        raise ValueError("sessions can only be created in text or forum channels")

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
        runtime = await self._bot._interaction_runtime(interaction)
        result = await runtime.respond_interaction(
            self._interaction_id,
            freeform=str(self.response.value),
        )
        await interaction.response.send_message(
            _interaction_result_text(result),
            ephemeral=True,
        )


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


async def _discord_render(payload: dict[str, Any]) -> tuple[str, list[TableAsset]]:
    content = str(payload.get("content", ""))
    if not payload.get("finalized"):
        return _safe_stream_content(content), []

    assets: list[TableAsset] = []
    attachments = payload.get("attachments")
    if isinstance(attachments, list):
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            body = attachment.get("content")
            if not isinstance(body, str):
                continue
            assets.append(
                TableAsset(
                    filename=str(attachment.get("filename", "artifact.txt")),
                    media_type=str(attachment.get("media_type", "text/plain")),
                    content=body.encode(),
                )
            )
    assembler = MarkdownAssembler()
    blocks = assembler.append(content)
    blocks.extend(assembler.finalize(content))
    output: list[str] = []
    for block in blocks:
        if isinstance(block, TextBlock):
            output.append(block.content)
        elif isinstance(block, TableBlock):
            plan = await render_table(block.markdown)
            if plan.preview_text is not None:
                output.append(plan.preview_text + "\n")
            if plan.assets:
                output.append(f"\n[Table: {plan.assets[0].filename}]\n")
                assets.extend(plan.assets)
    rendered = "".join(output).strip()
    if len(rendered) > 1900:
        digest = uuid.uuid5(uuid.NAMESPACE_URL, content).hex[:12]
        assets.insert(
            0,
            TableAsset(
                filename=f"response-{digest}.md",
                media_type="text/markdown",
                content=content.encode(),
            ),
        )
        rendered = f"Response attached as `{assets[0].filename}`."
    return rendered, assets


def _safe_stream_content(content: str) -> str:
    lines = content.splitlines(keepends=True)
    for index in range(1, len(lines)):
        if "|" in lines[index - 1] and _TABLE_DELIMITER.match(lines[index]):
            prefix = "".join(lines[: index - 1]).rstrip()
            marker = "\n\n*(rendering table...)*"
            return (prefix + marker).strip()
    return content[-1900:]


def _bounded_discord_text(content: str, limit: int) -> str:
    normalized = " ".join(content.split())
    return normalized if len(normalized) <= limit else normalized[: limit - 1] + "…"


def _render_view(payload: dict[str, Any]) -> discord.ui.View | None:
    interaction = payload.get("interaction")
    if isinstance(interaction, dict) and interaction.get("state") == "pending":
        return _interaction_view(interaction)
    return _taskdeck_view(payload)


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


def _taskdeck_view(payload: dict[str, Any]) -> discord.ui.View | None:
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
                    label=str(option["label"]),
                    value=str(option["value"]),
                    description=str(option["state"]),
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
    for asset in assets:
        if len(asset.content) <= max_bytes:
            prepared.append(asset)
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
    if split_files:
        note = (
            f"\n\n{split_files} large attachment(s) were split into "
            f"{split_parts} upload-safe file(s); concatenate matching parts in order."
        )
        available = max(1, 1900 - len(note))
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


def _payload_session_hint(_payload: dict[str, Any]) -> str | None:
    return None


async def _send_ephemeral_text(
    interaction: discord.Interaction,
    text: str,
    filename: str,
) -> None:
    if len(text) <= 1900:
        await interaction.followup.send(text, ephemeral=True)
        return
    await interaction.followup.send(
        "The result is attached.",
        file=discord.File(io.BytesIO(text.encode("utf-8")), filename=filename),
        ephemeral=True,
    )


async def run_discord_bot(settings: Settings) -> None:
    if settings.discord_token is None:
        raise RuntimeError("COPILOTD_DISCORD_TOKEN is required")
    bot = CopilotDiscordBot(settings)
    try:
        await bot.start(settings.discord_token.get_secret_value(), reconnect=True)
        if bot._fatal_worker_error is not None:
            raise RuntimeError("critical copilotD worker failed") from bot._fatal_worker_error
    finally:
        await bot.close()
