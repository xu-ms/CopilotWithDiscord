from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    CommandResponder,
    ModelReasoningSummaryAdapter,
    OpsSurfaceAdapter,
    ScheduleOriginAdapter,
    SessionNamingAdapter,
    TaskActionAdapter,
    UnknownInteractionError,
)
from copilotd.core.projects import ProjectRegistry
from copilotd.core.session_runtime import (
    DetachBlocked,
    RuntimeState,
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
from copilotd.core.task_registry import TaskRegistry
from copilotd.ops.heartbeat import HeartbeatWriter
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
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore

logger = structlog.get_logger(__name__)
_TABLE_DELIMITER = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$")


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
    ) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)
        self.settings = settings
        self.database = Database(settings.database_path)
        self.bridge = CopilotBridge(settings)
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
        self.heartbeat = HeartbeatWriter(self.database, settings.heartbeat_path)
        self.ops_service = ops_service or LocalOpsSurface(self.database, settings)
        self.session_naming_adapter = session_naming_adapter
        self.model_summary_adapter = model_summary_adapter
        self.schedule_origin_adapter = schedule_origin_adapter
        self.task_action_adapter = task_action_adapter
        self.command_executor = CommandExecutor(error_mapper=_map_command_error)
        self.projects: ProjectRegistry | None = None
        self.bindings: SessionBindingRepository | None = None
        self.sessions: SessionRegistry | None = None
        self.creation: SessionCreationService | None = None
        self.dispatcher: RenderOutboxDispatcher | None = None
        self._tasks = TaskRegistry()
        self._owner_id = f"discord:{uuid.uuid4()}"
        self._commands_registered = False
        self._render_stop = asyncio.Event()
        self._render_task: asyncio.Task[None] | None = None
        self._after_render_send_hook: Callable[[int, str], Awaitable[None]] | None = None

    async def setup_hook(self) -> None:
        await self.database.open()
        self.projects = ProjectRegistry(
            self.database,
            resolved_home=self.settings.resolved_home,
        )
        await self.projects.initialize()
        self.bindings = SessionBindingRepository(self.database)
        leases = OwnerLeaseStore(
            self.database,
            ttl_seconds=self.settings.owner_lease_ttl_seconds,
        )
        await self.bridge.start()
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
                model_summary_adapter=self.model_summary_adapter,
                task_action_adapter=self.task_action_adapter,
            )

        self.sessions = SessionRegistry(self.bindings, runtime_factory)
        self.creation = SessionCreationService(
            projects=self.projects,
            intents=CreationIntentRepository(self.database),
            bindings=self.bindings,
            sessions=self.sessions,
            threads=DiscordThreadGateway(self),
        )
        failures = await self.sessions.eager_resume()
        for thread_id, error in failures.items():
            await logger.awarning(
                "session_eager_resume_failed",
                thread_id=thread_id,
                error=error,
            )
        self.dispatcher = RenderOutboxDispatcher(self.database, self)
        self._render_task = self._tasks.create(
            self._render_loop(),
            name="discord-render-outbox",
        )
        self._tasks.create(self.heartbeat.run(), name="copilotd-heartbeat")
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
        await self._stop_render_consumer()
        if self.dispatcher is not None:
            await self.dispatcher.drain()
        if self.sessions is not None:
            await self.sessions.shutdown()
        if self.dispatcher is not None:
            await self.dispatcher.drain()
        await self._tasks.cancel_all()
        await self.bridge.stop()
        await self.database.close()
        await super().close()

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
        if message.author.bot or message.guild is None:
            return
        prompt = self._clean_prompt(message)
        if isinstance(message.channel, discord.Thread):
            binding = await self._require_bindings().by_thread(str(message.channel.id))
            if binding is None or (not prompt and not message.attachments):
                return
            if binding.binding_intent == BindingIntent.CLOSED:
                await message.reply(
                    "[CD-SESSION-002] This session is closed; use `/session resume` "
                    "in this original thread."
                )
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
        checkpoint = await self.database.fetchone(
            """
            SELECT first_discord_message_id FROM render_attachment_checkpoints
            WHERE session_id = ? AND render_message_id = ? AND agent_id = ?
            """,
            (session_id, delivery_id, agent_id),
        )
        first_message_id = None if checkpoint is None else checkpoint["first_discord_message_id"]
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
        now = time.time()
        for index, batch in enumerate(plan.batches):
            if index in delivered_ids:
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
                        nonce, payload_hash, state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?, ?)
                    """,
                    (
                        session_id,
                        delivery_id,
                        agent_id,
                        index,
                        nonce,
                        payload_hash,
                        now,
                        now,
                    ),
                )
            elif str(intent["nonce"]) != nonce or str(intent["payload_hash"]) != payload_hash:
                raise RenderPermanentError(f"render batch intent changed for {delivery_id}:{index}")

            reconciled_message_id: str | None = None
            if intent is not None and intent["discord_message_id"] is not None:
                reconciled_message_id = str(intent["discord_message_id"])
            elif intent is not None and (first_message is None or index > 0):
                reconciled = await self._find_message_by_nonce(
                    thread,
                    nonce,
                    created_at=float(intent["created_at"]),
                )
                if reconciled is not None:
                    reconciled_message_id = str(reconciled.id)
            if reconciled_message_id is not None:
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

            if index == 0 and first_message is not None:
                await first_message.edit(
                    content=batch.content or "\u200b",
                    attachments=_discord_files(list(batch.assets)),
                    view=_render_view(
                        payload,
                        enable_task_actions=self.task_action_adapter is not None,
                    ),
                )
                discord_message_id = str(first_message.id)
            else:
                sent = await thread.send(
                    content=batch.content or "\u200b",
                    files=_discord_files(list(batch.assets)),
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
        operation: Callable[
            [CommandInvocation],
            str | Awaitable[str | None] | None,
        ],
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
        counts: dict[str, int] = {}
        for name, query, parameters in (
            (
                "inbox",
                "SELECT COUNT(*) FROM event_journal WHERE sdk_session_id = ?",
                (binding.sdk_session_id,),
            ),
            (
                "active_liveness",
                "SELECT COUNT(*) FROM liveness_leases "
                "WHERE sdk_session_id = ? AND state = 'active'",
                (binding.sdk_session_id,),
            ),
            (
                "submissions",
                "SELECT COUNT(*) FROM submissions WHERE sdk_session_id = ?",
                (binding.sdk_session_id,),
            ),
            (
                "tasks",
                "SELECT COUNT(*) FROM task_card_projections WHERE sdk_session_id = ?",
                (binding.sdk_session_id,),
            ),
            (
                "queue",
                "SELECT COUNT(*) FROM message_queue "
                "WHERE thread_id = ? AND state NOT IN ('cancelled','submitted','failed')",
                (binding.thread_id,),
            ),
            (
                "outbox",
                "SELECT COUNT(*) FROM render_outbox "
                "WHERE session_id = ? AND state IN ('pending','sending','blocked')",
                (binding.sdk_session_id,),
            ),
        ):
            count = await self.database.fetchone(query, parameters)
            counts[name] = 0 if count is None else int(count[0])
        desired_model = _json_object(row["desired_model_config"])
        runtime_model = _json_object(row["runtime_model_config"])
        return "\n".join(
            (
                f"**{row['display_name'] or 'Copilot session'}**",
                f"SDK session: `{binding.sdk_session_id}`",
                f"Discord: thread <#{binding.thread_id}> · parent "
                f"`{row['parent_channel_id'] or 'unknown'}`",
                f"binding: `{binding.binding_intent}/{binding.attachment_state}` · "
                f"generation `{binding.runtime_generation}` · fence "
                f"`{binding.owner_fence_token or 'none'}`",
                f"cwd snapshot: `{binding.cwd_snapshot}` · source `{binding.project_source}`",
                f"mode: desired `{binding.desired_mode}` · pending "
                f"`{binding.pending_mode or 'none'}` · runtime `{binding.runtime_mode}`",
                f"model: desired `{json.dumps(desired_model, sort_keys=True)}` · runtime "
                f"`{json.dumps(runtime_model, sort_keys=True) if runtime_model else 'unknown'}`",
                f"agent: desired `{row['desired_agent']}` · pending "
                f"`{row['pending_agent'] or 'none'}` · runtime `{row['runtime_agent']}`",
                f"remote: `{row['runtime_remote_mode']}` · permission "
                f"`{binding.permission_posture}` · native name "
                f"`{row['native_name_state'] or 'unknown'}`",
                f"activity: processing `{_bool_unknown(row['runtime_processing'])}` · active "
                f"`{_bool_unknown(row['runtime_has_active_work'])}` · abortable "
                f"`{_bool_unknown(row['runtime_abortable'])}`",
                f"durable: inbox `{counts['inbox']}` · submissions `{counts['submissions']}` · "
                f"tasks `{counts['tasks']}` · queue `{counts['queue']}` · "
                f"liveness `{counts['active_liveness']}` · outbox `{counts['outbox']}`",
                f"cursor: inbox `{binding.last_inbox_seq}` · SDK receive "
                f"`{binding.last_sdk_receive_seq or 'none'}`",
            )
        )

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
        project = app_commands.Group(name="project", description="Manage channel projects")
        model = app_commands.Group(name="model", description="Inspect or change Copilot models")
        queue = app_commands.Group(name="queue", description="Manage the durable message queue")
        ops = app_commands.Group(name="ops", description="Inspect copilotD operations")
        variable = app_commands.Group(
            name="variable",
            description="Manage future-session project environment variables",
            parent=project,
        )
        mcp = app_commands.Group(
            name="mcp",
            description="Manage future-session MCP configuration",
            parent=project,
        )
        skill = app_commands.Group(
            name="skill",
            description="Manage future-session skill directories",
            parent=project,
        )
        plugin = app_commands.Group(
            name="plugin",
            description="Manage future-session plugin directories",
            parent=project,
        )
        custom_agent = app_commands.Group(
            name="agent",
            description="Manage future-session custom agents",
            parent=project,
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
                runtime = self._require_sessions().for_thread(binding.thread_id)
                if runtime is None or runtime.state in {
                    RuntimeState.CLOSED,
                    RuntimeState.FENCED,
                    RuntimeState.RECOVERY_UNKNOWN,
                }:
                    runtime = await self._require_sessions().replace(binding)
                if runtime.state == RuntimeState.DETACHED:
                    await runtime.attach_resume(
                        reactivate=binding.binding_intent == BindingIntent.CLOSED
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
            async def operation(_: CommandInvocation) -> str:
                removed = await self._require_projects().remove_project_env(
                    _parent_channel_id(interaction),
                    name=name,
                )
                if not removed:
                    raise CDInputError(f"project variable not found: {name}")
                return f"Variable `{name}` removed."

            await self._run_command(interaction, "project variable unset", operation)

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

        self.tree.add_command(session)
        self.tree.add_command(project)
        self.tree.add_command(model)
        self.tree.add_command(queue)
        self.tree.add_command(ops)

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
        runtime = self._require_sessions().for_thread(binding.thread_id)
        if runtime is None:
            runtime = await self._require_sessions().replace(binding)
            await runtime.attach_resume()
        return runtime

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


@dataclass(frozen=True, slots=True)
class DiscordRenderBatch:
    content: str
    assets: tuple[TableAsset, ...] = ()


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
    if not payload.get("finalized"):
        return DiscordRenderPlan((DiscordRenderBatch(_safe_stream_content(content)),))

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
    if allowed_roots:
        extraction = await asyncio.to_thread(
            lambda: extract_local_markdown_images(
                content,
                allowed_roots=allowed_roots,
            )
        )
        content = extraction.content
        image_warnings.extend(warning.message for warning in extraction.warnings)
        for attachment in extraction.attachments:
            try:
                image_content = await asyncio.to_thread(Path(attachment.resolved_path).read_bytes)
            except OSError:
                image_warnings.append(f"local image disappeared before upload: {attachment.path}")
                continue
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
            prepared_batches.append(DiscordRenderBatch(prepared_content))
            continue
        for index in range(0, len(prepared_assets), 10):
            prepared_batches.append(
                DiscordRenderBatch(
                    content=prepared_content if index == 0 else "",
                    assets=tuple(prepared_assets[index : index + 10]),
                )
            )
    return DiscordRenderPlan(tuple(prepared_batches))


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
    interaction_id = str(metadata["interaction_id"])
    view = discord.ui.View(timeout=None)
    choices = metadata.get("choices")
    if isinstance(choices, list) and choices:
        view.add_item(
            discord.ui.Select(
                custom_id=f"cdi:{interaction_id}:select",
                placeholder="Choose a response",
                options=[
                    discord.SelectOption(
                        label=_bounded_discord_text(str(choice), 100),
                        value=str(index),
                    )
                    for index, choice in enumerate(choices[:25])
                ],
            )
        )
    if metadata.get("kind") == "user_input" and (
        metadata.get("allowFreeform") or (isinstance(choices, list) and len(choices) > 25)
    ):
        view.add_item(
            discord.ui.Button(
                label=(
                    "Enter a choice"
                    if isinstance(choices, list)
                    and len(choices) > 25
                    and not metadata.get("allowFreeform")
                    else "Write a response"
                ),
                style=discord.ButtonStyle.secondary,
                custom_id=f"cdi:{interaction_id}:freeform",
            )
        )
    return view


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
    value = payload.get("session_id")
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


def _render_batch_hash(batch: DiscordRenderBatch) -> str:
    digest = hashlib.sha256()
    digest.update(batch.content.encode("utf-8"))
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
    if isinstance(error, DetachBlocked | SessionNotReady):
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
    finally:
        if not bot.is_closed():
            await bot.close()
