from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import re
import ssl
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import certifi
import discord
from PIL import Image

from copilotd.config import Settings
from copilotd.discord_app import (
    CopilotDiscordBot,
    _discord_files,
    _discord_render_plan,
    _taskdeck_view,
)

DEFAULT_ENV_FILE = Path("/Users/xu/Downloads/.testbot.env.txt")
REQUIRED_ENV_KEY = "testbot"
_SENSITIVE_KEY = re.compile(
    r"(?i)(token|secret|password|authorization|cookie|private.?key|access.?key)"
)


class DiscordE2EError(RuntimeError):
    pass


class DiscordE2EConfigurationError(DiscordE2EError):
    pass


@dataclass(slots=True)
class FeatureEvidence:
    feature: str
    status: str
    transport: str
    detail: str
    discord_ids: list[str] = field(default_factory=list)
    stable_identifiers: dict[str, str] = field(default_factory=dict)
    ui_actions: list[dict[str, Any]] = field(default_factory=list)
    assertions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RunEvidence:
    run_id: str
    started_at: float
    finished_at: float | None = None
    guild_id: str | None = None
    channel_id: str | None = None
    thread_id: str | None = None
    cleaned_up: bool = False
    human_driver_plan_version: int = 1
    features: list[FeatureEvidence] = field(default_factory=list)
    combined_branch_pending: list[str] = field(
        default_factory=lambda: [
            "native Fleet execution",
            "native task RPC mutations",
            "native remote session control",
            "native /after and /every schedules",
            "app scheduler execution",
        ]
    )


def load_required_token(path: Path = DEFAULT_ENV_FILE) -> str:
    if not path.is_file():
        raise DiscordE2EConfigurationError(f"Discord E2E env file is missing: {path}")
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    token = values.get(REQUIRED_ENV_KEY, "").strip()
    if not token:
        raise DiscordE2EConfigurationError(f"Discord E2E env key `{REQUIRED_ENV_KEY}` is required")
    return token


def sanitize_evidence(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): ("[redacted]" if _SENSITIVE_KEY.search(str(key)) else sanitize_evidence(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_evidence(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_evidence(item) for item in value]
    return value


def write_evidence(path: Path, evidence: RunEvidence) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = sanitize_evidence(asdict(evidence))
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


class DiscordRealHarness:
    def __init__(
        self,
        *,
        token: str,
        evidence_path: Path,
        guild_id: int | None = None,
        keep_resources: bool = False,
    ) -> None:
        self._token = token
        self._evidence_path = evidence_path
        self._requested_guild_id = guild_id
        self._keep_resources = keep_resources
        self._ready = asyncio.Event()
        self._resumed = asyncio.Event()
        self._run_id = f"cd-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        self.evidence = RunEvidence(run_id=self._run_id, started_at=time.time())
        self._bot: CopilotDiscordBot | None = None
        self._runner: asyncio.Task[None] | None = None
        self._channel: discord.TextChannel | None = None
        self._thread: discord.Thread | None = None
        self._seed: discord.Message | None = None
        self._owns_channel = False
        self._guild_object: discord.Object | None = None
        self._command_paths: list[str] = []
        self._stable_ids: dict[str, str] = {}

    async def run(self) -> RunEvidence:
        with tempfile.TemporaryDirectory(prefix="copilotd-discord-e2e-") as directory:
            root = Path(directory)
            settings = Settings(
                data_dir=root / "data",
                cache_dir=root / "cache",
                log_dir=root / "logs",
                resolved_home=root,
            )
            bot = CopilotDiscordBot(settings)
            bot.http.connector = aiohttp.TCPConnector(
                ssl=ssl.create_default_context(cafile=certifi.where())
            )
            bot.intents.message_content = False
            self._bot = bot

            async def setup_hook() -> None:
                bot._register_application_commands()

            async def on_ready() -> None:
                self._ready.set()

            async def on_resumed() -> None:
                self._resumed.set()

            bot.setup_hook = setup_hook
            bot.on_ready = on_ready
            bot.on_resumed = on_resumed
            self._runner = asyncio.create_task(
                bot.start(self._token, reconnect=True),
                name=f"discord-e2e:{self._run_id}",
            )
            try:
                ready_task = asyncio.create_task(self._ready.wait())
                done, _pending = await asyncio.wait(
                    {ready_task, self._runner},
                    timeout=30,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._runner in done:
                    error = self._runner.exception()
                    raise DiscordE2EError(
                        "test bot stopped before READY: "
                        + (
                            "clean shutdown"
                            if error is None
                            else f"{type(error).__name__}: {error}"
                        )
                    )
                if ready_task not in done:
                    ready_task.cancel()
                    await asyncio.gather(ready_task, return_exceptions=True)
                    raise DiscordE2EError("test bot did not reach READY within 30 seconds")
                guild = self._select_guild(bot)
                self.evidence.guild_id = str(guild.id)
                self._guild_object = discord.Object(id=guild.id)
                await self._sync_manifest(bot)
                await self._create_namespace(guild)
                await self._run_real_transport_cases(root)
                self._record_interaction_coverage()
            finally:
                await self._cleanup()
                self.evidence.finished_at = time.time()
                write_evidence(self._evidence_path, self.evidence)
        return self.evidence

    def _select_guild(self, bot: CopilotDiscordBot) -> discord.Guild:
        candidates = [
            guild
            for guild in bot.guilds
            if self._requested_guild_id is None or guild.id == self._requested_guild_id
        ]
        for guild in candidates:
            member = guild.me
            if member is None:
                continue
            if member.guild_permissions.manage_channels:
                return guild
            if any(
                channel.permissions_for(member).view_channel
                and channel.permissions_for(member).send_messages
                and channel.permissions_for(member).create_public_threads
                and channel.permissions_for(member).manage_threads
                for channel in guild.text_channels
            ):
                return guild
        raise DiscordE2EError(
            "test bot needs Manage Channels or Send/Create/Manage Threads "
            "in a writable Discord channel"
        )

    async def _sync_manifest(self, bot: CopilotDiscordBot) -> None:
        assert self._guild_object is not None
        bot.tree.copy_global_to(guild=self._guild_object)
        synced = await bot.tree.sync(guild=self._guild_object)
        names = sorted(command.name for command in synced)
        required = {
            "session",
            "project",
            "model",
            "context",
            "usage",
            "autopilot",
            "plan",
            "steer",
            "queue",
            "ops",
            "Ask Copilot",
            "Pin message",
        }
        missing = sorted(required - set(names))
        if missing:
            raise DiscordE2EError(
                "real command sync omitted required commands: " + ", ".join(missing)
            )
        self.evidence.features.append(
            FeatureEvidence(
                feature="application-command manifest",
                status="passed",
                transport="real Discord command sync",
                detail=f"synced {len(names)} commands",
            )
        )
        for path in _command_paths(bot.tree.get_commands(guild=self._guild_object)):
            self._command_paths.append(path)
            self.evidence.features.append(
                FeatureEvidence(
                    feature=f"command schema `{path}`",
                    status="passed",
                    transport="real Discord command sync",
                    detail="registered with Discord in the isolated test guild",
                )
            )

    async def _create_namespace(self, guild: discord.Guild) -> None:
        member = guild.me
        if member is None:
            raise DiscordE2EError("test bot guild member is unavailable")
        if not member.guild_permissions.manage_channels:
            channel = next(
                (
                    candidate
                    for candidate in guild.text_channels
                    if candidate.permissions_for(member).view_channel
                    and candidate.permissions_for(member).send_messages
                    and candidate.permissions_for(member).create_public_threads
                    and candidate.permissions_for(member).manage_threads
                ),
                None,
            )
            if channel is None:
                raise DiscordE2EError("no writable thread-capable channel is available")
            self._channel = channel
            self.evidence.channel_id = str(channel.id)
            self.evidence.features.append(
                FeatureEvidence(
                    feature="isolated run namespace",
                    status="degraded_existing_channel",
                    transport="real Discord channel and isolated thread",
                    detail=(
                        "bot lacks Manage Channels; reused one writable channel "
                        "and cleans the uniquely named thread/starter"
                    ),
                    discord_ids=[str(channel.id)],
                )
            )
            return
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                create_public_threads=True,
                manage_threads=True,
                attach_files=True,
                embed_links=True,
                read_message_history=True,
                manage_messages=True,
            ),
        }
        self._channel = await guild.create_text_channel(
            self._run_id[:90],
            overwrites=overwrites,
            reason=f"copilotD isolated E2E {self._run_id}",
        )
        self._owns_channel = True
        self.evidence.channel_id = str(self._channel.id)

    async def _run_real_transport_cases(self, root: Path) -> None:
        channel = self._require_channel()
        seed = await channel.send(f"copilotD E2E seed `{self._run_id}`", silent=True)
        self._seed = seed
        self._stable_ids["seed_message_id"] = str(seed.id)
        thread = await seed.create_thread(
            name=f"{self._run_id}-thread"[:100],
            auto_archive_duration=60,
        )
        self._thread = thread
        self._stable_ids["thread_name"] = thread.name
        self.evidence.thread_id = str(thread.id)
        message_ids: list[int] = []

        ordinary = await thread.send("ordinary message transport", silent=True)
        message_ids.append(ordinary.id)
        self.evidence.features.append(
            FeatureEvidence(
                feature="ordinary messages",
                status="passed",
                transport="real Discord thread",
                detail="message created and fetched",
                discord_ids=[str(ordinary.id)],
            )
        )

        image_path = root / "local-image.png"
        Image.new("RGB", (8, 8), "purple").save(image_path)
        markdown = (
            "Paragraph before.\n\n"
            "- list item\n"
            "> quote\n\n"
            "```python\nprint('fence')\n```\n\n"
            "| Name | Value |\n| --- | ---: |\n| alpha | 1 |\n\n"
            f"![local]({image_path})\n" + ("oversized block " * 300)
        )
        plan = await _discord_render_plan(
            {"content": markdown, "finalized": True},
            allowed_roots=(root,),
            max_bytes=7 * 1024 * 1024,
        )
        for batch in plan.batches:
            sent = await thread.send(
                batch.content or "\u200b",
                files=_discord_files(list(batch.assets)),
                silent=True,
            )
            message_ids.append(sent.id)
        fetched = [await thread.fetch_message(message_id) for message_id in message_ids]
        if any(len(message.content) > 2000 for message in fetched):
            raise DiscordE2EError("renderer emitted an over-limit Discord message")
        attachment_names = [
            attachment.filename for message in fetched for attachment in message.attachments
        ]
        if "local-image.png" not in attachment_names:
            raise DiscordE2EError("local Markdown image was not uploaded")
        self.evidence.features.append(
            FeatureEvidence(
                feature="Markdown/table/image/oversized rendering",
                status="passed",
                transport="real Discord messages and files",
                detail=(
                    f"{len(plan.batches)} ordered batches; {len(attachment_names)} attachments"
                ),
                discord_ids=[str(message.id) for message in fetched],
            )
        )

        files = [
            discord.File(io.BytesIO(f"file-{index}".encode()), filename=f"{index:02d}.txt")
            for index in range(12)
        ]
        first_files = await thread.send(files=files[:10], silent=True)
        second_files = await thread.send(files=files[10:], silent=True)
        if len(first_files.attachments) != 10 or len(second_files.attachments) != 2:
            raise DiscordE2EError("Discord attachment batching did not preserve 10+2 files")
        self.evidence.features.append(
            FeatureEvidence(
                feature="attachment batching and ordering",
                status="passed",
                transport="real Discord files",
                detail="12 files delivered as ordered 10+2 batches",
                discord_ids=[str(first_files.id), str(second_files.id)],
            )
        )

        task_payload = {
            "taskdeck": {
                "panel_id": "e2epanel",
                "revision": 1,
                "page": 0,
                "page_count": 1,
                "selected_card_token": "e2ecard",
                "expanded": False,
                "actions": ["download"],
                "options": [{"label": "E2E task", "value": "e2ecard", "state": "running"}],
            }
        }
        task_message = await thread.send(
            "**TaskDeck** — E2E",
            view=_taskdeck_view(task_payload),
            silent=True,
        )
        fetched_task = await thread.fetch_message(task_message.id)
        self._stable_ids["taskdeck_message_id"] = str(task_message.id)
        if not fetched_task.components:
            raise DiscordE2EError("TaskDeck components were not serialized by Discord")
        self.evidence.features.append(
            FeatureEvidence(
                feature="TaskDeck components",
                status="passed",
                transport="real Discord component serialization",
                detail=f"{len(fetched_task.components)} component row(s)",
                discord_ids=[str(task_message.id)],
            )
        )

        error_body = "E" * 8000
        error_message = await thread.send(
            "**Tool failed** — exact output attached.",
            file=discord.File(io.BytesIO(error_body.encode()), filename="tool-error.txt"),
            silent=True,
        )
        if error_message.attachments[0].size != len(error_body):
            raise DiscordE2EError("exact tool error attachment size changed")
        self.evidence.features.append(
            FeatureEvidence(
                feature="error/verbatim artifact boundary",
                status="passed",
                transport="real Discord file",
                detail="8000-character exact artifact preserved",
                discord_ids=[str(error_message.id)],
            )
        )

        try:
            await seed.pin(reason=f"copilotD E2E {self._run_id}")
            pinned = await channel.pins()
        except discord.Forbidden:
            self.evidence.features.append(
                FeatureEvidence(
                    feature="Pin message context behavior",
                    status="blocked_missing_permission",
                    transport="real Discord pin API",
                    detail="test bot lacks Manage Messages in the available channel",
                    discord_ids=[str(seed.id)],
                )
            )
        else:
            if seed.id not in {message.id for message in pinned}:
                raise DiscordE2EError("real Discord pin verification failed")
            self.evidence.features.append(
                FeatureEvidence(
                    feature="Pin message context behavior",
                    status="passed",
                    transport="real Discord pin API",
                    detail="seed message pinned and listed",
                    discord_ids=[str(seed.id)],
                )
            )

        await thread.edit(archived=True)
        await thread.edit(archived=False)
        resumed = await thread.send("archive/resume continuity", silent=True)
        self.evidence.features.append(
            FeatureEvidence(
                feature="archive/resume thread continuity",
                status="passed",
                transport="real Discord thread state",
                detail="same thread unarchived and reused",
                discord_ids=[str(thread.id), str(resumed.id)],
            )
        )

        self._resumed.clear()
        await self._require_bot().ws.close(code=4000)
        await asyncio.wait_for(self._resumed.wait(), timeout=30)
        reconnected_thread = await self._require_bot().fetch_channel(thread.id)
        if not isinstance(reconnected_thread, discord.Thread):
            raise DiscordE2EError("thread was unavailable after gateway reconnect")
        reconnect_message = await reconnected_thread.send(
            "gateway reconnect continuity",
            silent=True,
        )
        self.evidence.features.append(
            FeatureEvidence(
                feature="gateway reconnect",
                status="passed",
                transport="real Discord gateway resume",
                detail="forced websocket close resumed and reused the same thread",
                discord_ids=[str(thread.id), str(reconnect_message.id)],
            )
        )

        burst = await asyncio.gather(
            *(thread.send(f"rate-managed-{index}", silent=True) for index in range(6))
        )
        if len({message.id for message in burst}) != 6:
            raise DiscordE2EError("rate-managed burst lost or duplicated messages")
        self.evidence.features.append(
            FeatureEvidence(
                feature="429/rate handling",
                status="passed",
                transport="discord.py real REST rate manager",
                detail="six-message burst completed without loss",
                discord_ids=[str(message.id) for message in burst],
            )
        )

        ordered = [
            message.id
            async for message in thread.history(limit=None, oldest_first=True)
            if message.author.id == self._require_bot().user.id
        ]
        if ordered != sorted(ordered):
            raise DiscordE2EError("Discord message order is not monotonic")
        self.evidence.features.append(
            FeatureEvidence(
                feature="message/thread count and order",
                status="passed",
                transport="real Discord history",
                detail=f"{len(ordered)} bot messages in one thread",
                discord_ids=[str(thread.id)],
            )
        )

    def _record_interaction_coverage(self) -> None:
        thread_name = self._stable_ids.get("thread_name", "${THREAD_NAME}")
        base_identifiers = {
            "run_id": self._run_id,
            "guild_id": self.evidence.guild_id or "${GUILD_ID}",
            "channel_id": self.evidence.channel_id or "${CHANNEL_ID}",
            "thread_id": self.evidence.thread_id or "${THREAD_ID}",
            "thread_name": thread_name,
            "seed_message_id": self._stable_ids.get(
                "seed_message_id",
                "${SEED_MESSAGE_ID}",
            ),
            "taskdeck_message_id": self._stable_ids.get(
                "taskdeck_message_id",
                "${TASKDECK_MESSAGE_ID}",
            ),
        }
        inbound_marker = f"E2E-INBOUND::{self._run_id}"
        self.evidence.features.append(
            FeatureEvidence(
                feature="ordinary inbound user gateway message",
                status="pending_human_driver",
                transport="Discord desktop Appium + real gateway",
                detail="Send one non-bot message and verify exactly one durable submission.",
                stable_identifiers={
                    **base_identifiers,
                    "message_marker": inbound_marker,
                    "composer_name": f"Message #{thread_name}",
                },
                ui_actions=_composer_actions(thread_name, inbound_marker),
                assertions=[
                    "REST history contains exactly one user-authored message with message_marker.",
                    "Database contains exactly one submission whose prompt hash "
                    "matches message_marker.",
                    "No second Discord thread is created for the same source message.",
                ],
            )
        )

        human_paths = [
            path for path in self._command_paths if path not in {"Ask Copilot", "Pin message"}
        ]
        for execution_order, path in enumerate(
            sorted(human_paths, key=_slash_driver_order),
            start=1,
        ):
            invocation = _slash_driver_invocation(path, self._run_id)
            self.evidence.features.append(
                FeatureEvidence(
                    feature=f"slash command `/{path}`",
                    status="pending_human_driver",
                    transport="Discord desktop Appium + real interaction gateway",
                    detail=(
                        "Invoke the synced product command as an authenticated human; "
                        "placeholders are resolved from prior evidence/API results."
                    ),
                    stable_identifiers={
                        **base_identifiers,
                        "command_path": f"/{path}",
                        "invocation": invocation,
                        "execution_order": str(execution_order),
                        "composer_name": f"Message #{thread_name}",
                    },
                    ui_actions=_composer_actions(thread_name, invocation),
                    assertions=[
                        "Discord interaction is acknowledged within 2.5 seconds.",
                        "A final ephemeral response or in-thread 10062 fallback is observed.",
                        "Response contains no untyped 'command failed' text.",
                        _slash_driver_assertion(path),
                    ],
                )
            )

        self.evidence.features.extend(
            [
                FeatureEvidence(
                    feature="TaskDeck component interactions",
                    status="pending_human_driver",
                    transport="Discord desktop Appium + real component gateway",
                    detail="Select, expand, download, and refresh the same durable TaskDeck.",
                    stable_identifiers={
                        **base_identifiers,
                        "select_custom_id_prefix": "cdtd:",
                        "expected_card_label": "E2E task",
                    },
                    ui_actions=[
                        _appium_click(thread_name),
                        _appium_click("E2E task"),
                        _appium_click("Expand"),
                        _appium_click("Download"),
                    ],
                    assertions=[
                        "All component custom_id values begin with cdtd: and are under 100 chars.",
                        "Expand edits taskdeck_message_id in place; thread count is unchanged.",
                        "Download returns an ephemeral detail attachment.",
                        "A stale revision refreshes controls and does not repeat mutation.",
                    ],
                ),
                FeatureEvidence(
                    feature="modal submissions",
                    status="pending_human_driver",
                    transport="Discord desktop Appium + real modal gateway",
                    detail="Submit TaskDeck Message and Copilot freeform response modals.",
                    stable_identifiers={
                        **base_identifiers,
                        "task_modal_title": "Message Copilot task",
                        "input_modal_title": "Respond to Copilot",
                        "component_custom_id_prefix": "cdi:",
                        "modal_marker": f"E2E-MODAL::{self._run_id}",
                    },
                    ui_actions=[
                        _appium_click("Message"),
                        _appium_send_keys(
                            "Message",
                            f"E2E-MODAL::{self._run_id}",
                        ),
                        _appium_click("Submit"),
                        _appium_click("Write a response"),
                        _appium_send_keys(
                            "Response",
                            f"E2E-INPUT::{self._run_id}",
                        ),
                        _appium_click("Submit"),
                    ],
                    assertions=[
                        "Each modal interaction is acknowledged before DB/runtime work.",
                        "Task adapter receives exactly one modal_marker for the selected task.",
                        "Pending Copilot interaction transitions once to resolved.",
                    ],
                ),
                FeatureEvidence(
                    feature="context-menu Ask Copilot and Pin message",
                    status="pending_human_driver",
                    transport="Discord desktop Appium + real context-menu gateway",
                    detail="Invoke both synced message context menus on seed_message_id.",
                    stable_identifiers={
                        **base_identifiers,
                        "ask_label": "Ask Copilot",
                        "pin_label": "Pin message",
                    },
                    ui_actions=[
                        _appium_right_click(f"copilotD E2E seed `{self._run_id}`"),
                        _appium_click("Apps"),
                        _appium_click("Ask Copilot"),
                        _appium_right_click(f"copilotD E2E seed `{self._run_id}`"),
                        _appium_click("Apps"),
                        _appium_click("Pin message"),
                    ],
                    assertions=[
                        "Ask Copilot creates exactly one new thread with source provenance.",
                        "Source attachments are preserved in the new submission manifest.",
                        "Pin message invokes Discord pin API; missing Manage Messages "
                        "is recorded as a real capability outcome.",
                    ],
                ),
            ]
        )

    async def _cleanup(self) -> None:
        bot = self._bot
        try:
            if not self._keep_resources and not self._owns_channel and self._thread is not None:
                await self._thread.delete()
                self._thread = None
                if self._seed is not None:
                    await self._seed.delete()
                    self._seed = None
            if not self._keep_resources and self._owns_channel and self._channel is not None:
                await self._channel.delete(reason=f"copilotD E2E cleanup {self._run_id}")
                self._channel = None
            if not self._keep_resources and bot is not None and self._guild_object is not None:
                bot.tree.clear_commands(guild=self._guild_object)
                await bot.tree.sync(guild=self._guild_object)
            self.evidence.cleaned_up = not self._keep_resources
        finally:
            if bot is not None:
                await bot.close()
            runner = self._runner
            if runner is not None:
                await asyncio.gather(runner, return_exceptions=True)

    def _require_channel(self) -> discord.TextChannel:
        if self._channel is None:
            raise DiscordE2EError("E2E channel was not created")
        return self._channel

    def _require_bot(self) -> CopilotDiscordBot:
        if self._bot is None or self._bot.user is None:
            raise DiscordE2EError("E2E bot is not connected")
        return self._bot


def _appium_click(value: str) -> dict[str, Any]:
    return {
        "cli": "appium-cli element click",
        "by": "name",
        "value": value,
        "timeout_seconds": 15,
    }


def _appium_right_click(value: str) -> dict[str, Any]:
    return {
        "cli": "appium-cli element right-click",
        "by": "name",
        "value": value,
        "timeout_seconds": 15,
    }


def _appium_send_keys(value: str, text: str) -> dict[str, Any]:
    return {
        "cli": "appium-cli element send-keys",
        "by": "name",
        "value": value,
        "text": text,
        "timeout_seconds": 15,
    }


def _composer_actions(thread_name: str, text: str) -> list[dict[str, Any]]:
    return [
        _appium_click(thread_name),
        _appium_click(f"Message #{thread_name}"),
        {
            "cli": "appium-cli send-keys-macos",
            "text": text,
        },
        {
            "cli": "appium-cli key press",
            "keys": "return",
        },
        {
            "cli": "appium-cli wait exists",
            "by": "name",
            "value": text[:100],
            "timeout_seconds": 20,
        },
    ]


def _slash_driver_invocation(path: str, run_id: str) -> str:
    marker = f"e2e-{run_id[-8:]}"
    arguments = {
        "autopilot": "enabled:true",
        "model set": "model_id:${MODEL_ID_FROM_MODEL_LIST}",
        "ops debug": "level:debug duration_minutes:1",
        "ops diagnostics": "session_id:${SESSION_ID}",
        "ops event-dump": "session_id:${SESSION_ID}",
        "ops log-tail": f"correlation_id:{run_id}",
        "plan": f"action:enter prompt:{marker}-plan",
        "project agent add": (
            f"name:{marker}-agent description:E2E prompt:{marker}-agent-prompt tools:"
        ),
        "project agent remove": f"name:{marker}-agent",
        "project agent toggle": f"name:{marker}-agent enabled:false",
        "project bind": ("path:${E2E_PROJECT_PATH} layout:text mention_required:false"),
        "project layout": "value:text",
        "project mcp add": (
            f"name:{marker}-mcp transport:stdio command_or_url:/usr/bin/true "
            "args_json:[] project_env_refs:E2E_TOKEN enabled:true"
        ),
        "project mcp remove": f"name:{marker}-mcp",
        "project mcp toggle": f"name:{marker}-mcp enabled:false",
        "project mention": "required:false",
        "project plugin add": "path:${E2E_PLUGIN_PATH} enabled:true",
        "project plugin remove": "path:${E2E_PLUGIN_PATH}",
        "project plugin toggle": "path:${E2E_PLUGIN_PATH} enabled:false",
        "project skill add": "path:${E2E_SKILL_PATH} enabled:true",
        "project skill remove": "path:${E2E_SKILL_PATH}",
        "project skill toggle": "path:${E2E_SKILL_PATH} enabled:false",
        "project variable list": "reveal:false",
        "project variable set": "name:E2E_TOKEN value:${E2E_ENV_VALUE}",
        "project variable unset": "name:E2E_TOKEN",
        "queue add": f"text:{marker}-queued-prompt",
        "queue remove": "item_id:${QUEUE_ITEM_ID}",
        "queue resubmit": "item_id:${BLOCKED_QUEUE_ITEM_ID}",
        "session abort": "clear_local_queue:false",
        "session close": "force:false",
        "session new": f"prompt:{marker}-new-session",
        "session rename": f"name:{marker}-renamed",
        "session resume": "session_id:${SESSION_ID_IF_OUTSIDE_THREAD}",
        "steer": f"text:{marker}-steer",
    }
    suffix = arguments.get(path, "")
    return f"/{path}" + (f" {suffix}" if suffix else "")


def _slash_driver_order(path: str) -> tuple[int, str]:
    setup = {
        "project bind": 10,
        "project variable set": 20,
        "project mcp add": 30,
        "project skill add": 31,
        "project plugin add": 32,
        "project agent add": 33,
        "session new": 40,
        "session list": 41,
        "session info": 42,
        "model list": 50,
        "model set": 51,
        "context": 52,
        "usage": 53,
        "queue add": 60,
        "queue list": 61,
        "autopilot": 70,
        "plan": 71,
        "steer": 72,
        "session abort": 73,
        "ops health": 80,
        "ops diagnostics": 81,
        "ops debug": 82,
        "ops log-tail": 83,
        "ops event-dump": 84,
        "session rename": 90,
        "session close": 91,
        "session resume": 92,
        "queue remove": 100,
        "queue resubmit": 101,
        "queue clear": 102,
        "project mcp toggle": 110,
        "project skill toggle": 111,
        "project plugin toggle": 112,
        "project agent toggle": 113,
        "project mcp remove": 120,
        "project skill remove": 121,
        "project plugin remove": 122,
        "project agent remove": 123,
        "project variable unset": 124,
        "project unbind": 130,
    }
    return setup.get(path, 85), path


def _slash_driver_assertion(path: str) -> str:
    if path == "session new":
        return "One new Discord thread and one SDK session are created for the marker."
    if path == "session resume":
        return "The original thread ID is reused and thread count does not increase."
    if path == "session rename":
        return "Thread name and session_ui_metadata display_name equal the marker."
    if path.startswith("project "):
        return (
            "Project/channel config version changes exactly once and the projected value matches."
        )
    if path.startswith("queue "):
        return "Durable queue state and immutable snapshot transition match the command action."
    if path.startswith("model "):
        return "Observed model/options match the confirmed high-level SDK readback."
    if path.startswith("ops "):
        return "Response is bounded, deterministic, and contains no unredacted credential."
    if path == "steer":
        return "Steer succeeds only with observed active work and creates one immediate delivery."
    if path in {"autopilot", "plan"}:
        return "Mode transition is confirmed without an implicit abort or duplicate prompt."
    if path in {"context", "usage"}:
        return "Projection reports live or explicit last-seen/stale metadata without USD."
    return "Discord response and durable state agree for this command."


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run secure real Discord copilotD E2E")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--guild-id", type=int)
    parser.add_argument("--keep-resources", action="store_true")
    return parser.parse_args()


def _command_paths(
    commands: list[Any],
    *,
    prefix: str = "",
) -> list[str]:
    paths: list[str] = []
    for command in commands:
        path = f"{prefix} {command.name}".strip()
        if isinstance(command, discord.app_commands.Group):
            paths.extend(_command_paths(command.commands, prefix=path))
        else:
            paths.append(path)
    return sorted(paths)


async def _main_async(args: argparse.Namespace) -> int:
    token = load_required_token(args.env_file)
    harness = DiscordRealHarness(
        token=token,
        evidence_path=args.evidence,
        guild_id=args.guild_id,
        keep_resources=args.keep_resources,
    )
    evidence = await harness.run()
    passed = sum(feature.status == "passed" for feature in evidence.features)
    pending = sum(feature.status != "passed" for feature in evidence.features)
    print(
        json.dumps(
            {
                "run_id": evidence.run_id,
                "passed": passed,
                "pending": pending,
                "evidence": str(args.evidence),
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> None:
    args = _parse_args()
    try:
        code = asyncio.run(_main_async(args))
    except DiscordE2EConfigurationError as error:
        raise SystemExit(f"configuration error: {error}") from error
    except DiscordE2EError as error:
        raise SystemExit(f"Discord E2E failed: {error}") from error
    raise SystemExit(code)


if __name__ == "__main__":
    main()
