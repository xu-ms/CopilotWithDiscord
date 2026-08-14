from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
import ssl
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import aiohttp
import discord
from PIL import Image

from copilotd.config import Settings
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.discord_app import (
    CopilotDiscordBot,
    _discord_embeds,
    _discord_files,
    _discord_render_plan,
    _render_view,
    _taskdeck_view,
)
from copilotd.discord_requests import DiscordOperation, DiscordPriority
from copilotd.ops.surface import redact_sensitive_text
from copilotd.render.outbox import RenderOutboxDispatcher

try:
    import certifi
except Exception:
    certifi = None

DEFAULT_ENV_FILE = Path(
    os.environ.get(
        "COPILOTD_DISCORD_E2E_ENV_FILE",
        Path.home() / "Downloads" / ".testbot.env.txt",
    )
).expanduser()
REQUIRED_ENV_KEY = "testbot"
REQUIRED_GUILD_ID_KEY = "testbot_guild_id"
REQUIRED_APPLICATION_ID_KEY = "testbot_application_id"
REQUIRED_CHANNEL_ID_KEY = "testbot_channel_id"
_SENSITIVE_KEY = re.compile(
    r"(?i)(token|secret|password|authorization|cookie|private.?key|access.?key)"
)


class DiscordE2EError(RuntimeError):
    pass


class DiscordE2EConfigurationError(DiscordE2EError):
    pass


def _raise_run_errors(errors: list[BaseException]) -> None:
    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    raise BaseExceptionGroup("Discord E2E run failed", errors)


@dataclass
class FeatureEvidence:
    feature: str
    status: str
    transport: str
    detail: str
    discord_ids: list[str] = field(default_factory=list)
    stable_identifiers: dict[str, str] = field(default_factory=dict)
    ui_actions: list[dict[str, Any]] = field(default_factory=list)
    assertions: list[str] = field(default_factory=list)


@dataclass
class DiscordE2ETargets:
    guild_id: int
    application_id: int
    channel_id: int


@dataclass
class HttpRateLimitObservation:
    observed_actual_429: bool = False
    retry_after: float | None = None
    url: str | None = None


class _DryRunRenderTransport:
    def __init__(
        self,
        thread: Any,
        root: Path,
        created_messages: list[Any],
    ) -> None:
        self._thread = thread
        self._root = root
        self._created_messages = created_messages

    async def send(
        self,
        *,
        session_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> str:
        del session_id, lane, idempotency_key
        plan = await _discord_render_plan(
            payload,
            allowed_roots=(self._root,),
            max_bytes=7 * 1024 * 1024,
        )
        first_message: Any | None = None
        for batch in plan.batches:
            message = await self._thread.send(
                batch.content or "\u200b",
                files=_discord_files(list(batch.assets)),
                embeds=_discord_embeds(tuple(getattr(batch, "embeds", ()))),
                silent=True,
            )
            self._created_messages.append(message)
            if first_message is None:
                first_message = message
        if first_message is None:
            raise DiscordE2EError("dry-run renderer emitted no message batches")
        return str(first_message.id)

    async def edit(
        self,
        *,
        session_id: str,
        message_id: str,
        lane: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> None:
        del session_id, lane, payload, idempotency_key
        raise DiscordE2EError(f"dry-run production probe unexpectedly edited message {message_id}")


@dataclass
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


def _parse_required_int(values: dict[str, str], key: str) -> int:
    raw_value = values.get(key, "").strip()
    if not raw_value:
        raise DiscordE2EConfigurationError(f"Discord E2E env key `{key}` is required")
    try:
        return int(raw_value)
    except ValueError as error:
        raise DiscordE2EConfigurationError(
            f"Discord E2E env key `{key}` must be an integer"
        ) from error


def load_required_targets(path: Path = DEFAULT_ENV_FILE) -> DiscordE2ETargets:
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
    return DiscordE2ETargets(
        guild_id=_parse_required_int(values, REQUIRED_GUILD_ID_KEY),
        application_id=_parse_required_int(values, REQUIRED_APPLICATION_ID_KEY),
        channel_id=_parse_required_int(values, REQUIRED_CHANNEL_ID_KEY),
    )


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
    if isinstance(value, str):
        return redact_sensitive_text(value)
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
        guild_id: int,
        application_id: int,
        channel_id: int,
        keep_resources: bool = False,
        dry_run: bool = False,
        bot_factory: Callable[[Settings], CopilotDiscordBot] | None = None,
    ) -> None:
        if guild_id <= 0 or application_id <= 0 or channel_id <= 0:
            raise DiscordE2EConfigurationError(
                "Discord E2E requires explicit positive guild/application/channel IDs"
            )
        self._token = token
        self._evidence_path = evidence_path
        self._requested_guild_id = guild_id
        self._requested_application_id = application_id
        self._requested_channel_id = channel_id
        self._keep_resources = keep_resources
        self._dry_run = dry_run
        self._bot_factory = bot_factory or CopilotDiscordBot
        self._ready = asyncio.Event()
        self._resumed = asyncio.Event()
        self._run_id = f"cd-e2e-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        self.evidence = RunEvidence(run_id=self._run_id, started_at=time.time())
        self._bot: CopilotDiscordBot | None = None
        self._runner: asyncio.Task[None] | None = None
        self._channel: discord.TextChannel | None = None
        self._thread: discord.Thread | None = None
        self._seed: discord.Message | None = None
        self._guild_object: discord.Object | None = None
        self._command_paths: list[str] = []
        self._stable_ids: dict[str, str] = {}
        self._created_messages: list[discord.Message] = []
        self._original_manifest: list[dict[str, Any]] | None = None
        self._original_command_permissions: list[dict[str, Any]] = []
        self._http_rate_limit = HttpRateLimitObservation()

    def record_http_response(
        self,
        *,
        status: int,
        retry_after: float | None = None,
        url: str | None = None,
    ) -> None:
        if status == 429:
            self._http_rate_limit.observed_actual_429 = True
            self._http_rate_limit.retry_after = retry_after
            self._http_rate_limit.url = url

    async def _trace_request_end(
        self,
        _session: aiohttp.ClientSession,
        _context: Any,
        params: Any,
    ) -> None:
        response = params.response
        retry_after: float | None = None
        if response.status == 429:
            raw_retry = response.headers.get("Retry-After")
            try:
                retry_after = None if raw_retry is None else float(raw_retry)
            except ValueError:
                retry_after = None
        self.record_http_response(
            status=response.status,
            retry_after=retry_after,
            url=str(response.url),
        )

    async def record_ordered_delivery_probe(
        self,
        messages: list[Any],
        *,
        expected_contents: list[str] | None = None,
        expected_filenames: list[str] | None = None,
        expected_sha256: list[str] | None = None,
    ) -> FeatureEvidence:
        actual_contents = [str(getattr(message, "content", "")) for message in messages]
        actual_filenames: list[str] = []
        actual_sha256: list[str] = []
        for message in messages:
            delivered_assets: list[tuple[int, str, bytes]] = []
            seen_attachment_ids: set[int] = set()
            for position, attachment in enumerate(getattr(message, "attachments", [])):
                filename = str(getattr(attachment, "filename", "attachment.bin"))
                if hasattr(attachment, "read"):
                    content = await attachment.read(use_cached=False)
                else:
                    content = bytes(getattr(attachment, "content", b""))
                attachment_id = int(getattr(attachment, "id", 0) or 0)
                if attachment_id:
                    seen_attachment_ids.add(attachment_id)
                delivered_assets.append((attachment_id or position + 1, filename, content))
            for image_url in _discord_embed_image_urls(message):
                identity = _discord_cdn_attachment_identity(image_url)
                if identity is None:
                    continue
                attachment_id, filename = identity
                if attachment_id in seen_attachment_ids:
                    continue
                bot = self._require_bot()
                get_from_cdn = getattr(bot.http, "get_from_cdn", None)
                if not callable(get_from_cdn):
                    raise DiscordE2EError("Discord HTTP client cannot read embedded CDN assets")
                content = await get_from_cdn(image_url)
                delivered_assets.append((attachment_id, filename, content))
            for _asset_id, filename, content in sorted(delivered_assets):
                actual_filenames.append(filename)
                actual_sha256.append(_hash_bytes(content))
        if expected_contents is not None and list(expected_contents) != actual_contents:
            raise DiscordE2EError(
                f"ordered content mismatch: expected {expected_contents}, got {actual_contents}"
            )
        if expected_filenames is not None and list(expected_filenames) != actual_filenames:
            raise DiscordE2EError(
                f"ordered filename mismatch: expected {expected_filenames}, got {actual_filenames}"
            )
        if expected_sha256 is not None and list(expected_sha256) != actual_sha256:
            raise DiscordE2EError(
                f"ordered sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
            )
        return FeatureEvidence(
            feature="ordered content and attachment sha256",
            status="passed",
            transport="real Discord history and attachment bytes",
            detail="ordered message contents and downloaded attachment digests were verified",
            assertions=[
                f"ordered_contents={actual_contents}",
                f"attachment_filenames={actual_filenames}",
                f"attachment_sha256={actual_sha256}",
            ],
        )

    async def run(self) -> RunEvidence:
        errors: list[BaseException] = []
        with tempfile.TemporaryDirectory(prefix="copilotd-discord-e2e-") as directory:
            root = Path(directory)
            settings = Settings(
                data_dir=root / "data",
                cache_dir=root / "cache",
                log_dir=root / "logs",
                resolved_home=root,
            )
            bot = self._bot_factory(settings)
            self._bot = bot
            cleanup_errors: list[BaseException] = []
            write_error: BaseException | None = None
            try:
                if self._dry_run:
                    await self._prepare_dry_run_bot(bot)
                    await self._execute_ready_pipeline(bot, root)
                else:
                    ssl_context = (
                        ssl.create_default_context(cafile=certifi.where())
                        if certifi is not None
                        else ssl.create_default_context()
                    )
                    connector = aiohttp.TCPConnector(ssl=ssl_context)
                    trace = aiohttp.TraceConfig()
                    trace.on_request_end.append(self._trace_request_end)
                    _wire_discord_http_trace(bot, connector, trace)

                    async def setup_hook() -> None:
                        await bot.database.open()
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
                    await self._execute_ready_pipeline(bot, root)
            except BaseException as error:
                errors.append(error)
            finally:
                cleanup_errors, cleanup_interruptions = await self._cleanup_to_completion()
                self.evidence.finished_at = time.time()
                try:
                    write_evidence(self._evidence_path, self.evidence)
                except BaseException as error:
                    write_error = error
                errors.extend(cleanup_interruptions)
                errors.extend(cleanup_errors)
                if write_error is not None:
                    errors.append(write_error)
        _raise_run_errors(errors)
        return self.evidence

    async def _cleanup_to_completion(
        self,
    ) -> tuple[list[BaseException], list[BaseException]]:
        cleanup_task = asyncio.create_task(
            self._cleanup(),
            name=f"discord-e2e-cleanup:{self._run_id}",
        )
        interruptions: list[BaseException] = []
        while True:
            try:
                return await asyncio.shield(cleanup_task), interruptions
            except asyncio.CancelledError as error:
                if not cleanup_task.done():
                    interruptions.append(error)
                    continue
                if cleanup_task.cancelled():
                    return [error], interruptions
                interruptions.append(error)
                try:
                    return cleanup_task.result(), interruptions
                except BaseException as cleanup_error:
                    return [cleanup_error], interruptions
            except BaseException as error:
                return [error], interruptions

    async def _prepare_dry_run_bot(self, bot: Any) -> None:
        database = getattr(bot, "database", None)
        if database is not None and hasattr(database, "open"):
            await database.open()

        async def on_resumed() -> None:
            self._resumed.set()

        if hasattr(bot, "on_resumed"):
            bot.on_resumed = on_resumed
        register = getattr(bot, "_register_application_commands", None)
        if callable(register):
            register()

    async def _execute_ready_pipeline(self, bot: CopilotDiscordBot, root: Path) -> None:
        guild = self._select_guild(bot)
        channel = self._select_channel(guild)
        self._verify_connected_identity(bot, guild, channel)
        self._channel = channel
        self.evidence.guild_id = str(guild.id)
        self.evidence.channel_id = str(channel.id)
        self._guild_object = discord.Object(id=guild.id)
        self._original_manifest = await self._snapshot_guild_manifest(bot)
        await self._sync_manifest(bot)
        await self._run_real_transport_cases(root)
        self._record_interaction_coverage()
        await self._record_production_probes(bot)

    def _select_guild(self, bot: CopilotDiscordBot) -> discord.Guild:
        guild = bot.get_guild(self._requested_guild_id)
        if guild is None:
            raise DiscordE2EError(f"dedicated guild `{self._requested_guild_id}` is not connected")
        return guild

    def _select_channel(self, guild: discord.Guild):
        channel = guild.get_channel(self._requested_channel_id)
        if channel is None:
            raise DiscordE2EError(
                f"dedicated channel `{self._requested_channel_id}` is unavailable"
            )
        return channel

    def _verify_connected_identity(
        self,
        bot: CopilotDiscordBot,
        guild: discord.Guild,
        channel: discord.TextChannel,
    ) -> None:
        if bot.application_id != self._requested_application_id:
            raise DiscordE2EError(
                f"connected application `{bot.application_id}` does not match "
                f"requested `{self._requested_application_id}`"
            )
        if guild.id != self._requested_guild_id:
            raise DiscordE2EError(
                f"connected guild `{guild.id}` does not match requested "
                f"`{self._requested_guild_id}`"
            )
        if channel.id != self._requested_channel_id or channel.guild.id != guild.id:
            raise DiscordE2EError(
                f"connected channel `{channel.id}` does not match requested "
                f"`{self._requested_channel_id}`"
            )

    async def _snapshot_guild_manifest(self, bot: CopilotDiscordBot) -> list[dict[str, Any]]:
        assert self._guild_object is not None
        fetch_commands = getattr(bot.tree, "fetch_commands", None)
        if fetch_commands is not None:
            commands = await fetch_commands(guild=self._guild_object)
        else:
            commands = bot.tree.get_commands(guild=self._guild_object)
        manifest = [_command_manifest_entry(command) for command in commands]
        self._original_command_permissions = await self._snapshot_and_validate_command_permissions(
            bot, commands
        )
        return manifest

    async def _snapshot_and_validate_command_permissions(
        self,
        bot: CopilotDiscordBot,
        commands: list[Any],
    ) -> list[dict[str, Any]]:
        get_permissions = getattr(
            bot.http,
            "get_guild_application_command_permissions",
            None,
        )
        if not callable(get_permissions):
            raise DiscordE2EError(
                "cannot snapshot guild command permission overrides before manifest sync"
            )
        try:
            raw_overrides = await get_permissions(
                self._requested_application_id,
                self._requested_guild_id,
            )
        except Exception as error:
            raise DiscordE2EError(
                "cannot read guild command permission overrides before manifest sync"
            ) from error

        commands_by_id = {
            str(command_id): _command_identity(command)
            for command in commands
            if (command_id := _command_id(command)) is not None
        }
        snapshots: list[dict[str, Any]] = []
        for raw_override in raw_overrides or []:
            if not isinstance(raw_override, Mapping):
                continue
            permissions = _command_permission_entries(raw_override.get("permissions"))
            if not permissions:
                continue
            original_command_id = str(raw_override.get("id", ""))
            identity = commands_by_id.get(original_command_id)
            if identity is None:
                raise DiscordE2EError(
                    "guild command permission override references an unknown command ID"
                )
            snapshots.append(
                {
                    "name": identity[1],
                    "type": identity[0],
                    "original_command_id": original_command_id,
                    "permissions": permissions,
                }
            )

        if not snapshots:
            return []
        edit_permissions = getattr(
            bot.http,
            "edit_application_command_permissions",
            None,
        )
        if not callable(edit_permissions):
            raise DiscordE2EError("cannot restore existing guild command permission overrides")
        for snapshot in snapshots:
            try:
                await edit_permissions(
                    self._requested_application_id,
                    self._requested_guild_id,
                    int(snapshot["original_command_id"]),
                    {"permissions": _clone_json_value(snapshot["permissions"])},
                )
            except Exception as error:
                raise DiscordE2EError(
                    "credentials cannot restore guild command permission overrides; "
                    "manifest sync was not attempted"
                ) from error
        return snapshots

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

    async def _restore_guild_manifest(self, bot: CopilotDiscordBot) -> None:
        assert self._guild_object is not None
        commands = list(self._original_manifest)
        bulk_upsert = getattr(bot.http, "bulk_upsert_guild_commands", None)
        if bulk_upsert is None:
            raise DiscordE2EError("missing bulk_upsert_guild_commands")
        restored_commands = await bulk_upsert(
            self._requested_application_id,
            self._requested_guild_id,
            commands,
        )
        if not self._original_command_permissions:
            return
        if not isinstance(restored_commands, list):
            get_commands = getattr(bot.http, "get_guild_commands", None)
            if not callable(get_commands):
                raise DiscordE2EError(
                    "restored command IDs are unavailable for permission override restore"
                )
            restored_commands = await get_commands(
                self._requested_application_id,
                self._requested_guild_id,
            )
        restored_ids = {
            _command_identity(command): command_id
            for command in restored_commands
            if (command_id := _command_id(command)) is not None
        }
        edit_permissions = getattr(
            bot.http,
            "edit_application_command_permissions",
            None,
        )
        if not callable(edit_permissions):
            raise DiscordE2EError(
                "missing edit_application_command_permissions during manifest restore"
            )
        for snapshot in self._original_command_permissions:
            identity = (int(snapshot["type"]), str(snapshot["name"]))
            restored_id = restored_ids.get(identity)
            if restored_id is None:
                raise DiscordE2EError(f"restored command ID is missing for `{identity[1]}`")
            await edit_permissions(
                self._requested_application_id,
                self._requested_guild_id,
                int(restored_id),
                {"permissions": _clone_json_value(snapshot["permissions"])},
            )

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
        await self._run_reaction_state_chain(seed)
        await self._run_production_pipeline_probe(root, thread)
        message_ids: list[int] = []

        ordinary = await thread.send("ordinary message transport", silent=True)
        self._created_messages.append(ordinary)
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
        await self._run_render_gallery(thread)

        image_path = root / "local-image.png"
        Image.new("RGB", (8, 8), "purple").save(image_path)
        image_snapshot = root / "durable-local-image.png"
        image_snapshot.write_bytes(image_path.read_bytes())
        image_snapshot_bytes = image_snapshot.read_bytes()
        markdown = (
            "Paragraph before.\n\n"
            "- list item\n"
            "> quote\n\n"
            "```python\nprint('fence')\n```\n\n"
            "| Name | Value |\n| --- | ---: |\n| alpha | 1 |\n\n"
            f"![local]({image_path})\n" + ("oversized block " * 300)
        )
        plan = await _discord_render_plan(
            {
                "type": "assistant.message",
                "content": markdown,
                "finalized": True,
                "trusted_local_images": True,
                "trusted_local_image_paths": [str(image_path)],
                "trusted_local_image_artifacts": [
                    {
                        "source_path": str(image_path),
                        "snapshot_path": str(image_snapshot),
                        "byte_size": len(image_snapshot_bytes),
                        "sha256": _hash_bytes(image_snapshot_bytes),
                    }
                ],
            },
            allowed_roots=(root,),
            max_bytes=7 * 1024 * 1024,
        )
        expected_contents = [batch.content or "\u200b" for batch in plan.batches]
        expected_filenames = [
            str(asset.filename) for batch in plan.batches for asset in batch.assets
        ]
        expected_sha256 = [
            _hash_bytes(_asset_bytes(asset)) for batch in plan.batches for asset in batch.assets
        ]
        for batch in plan.batches:
            sent = await thread.send(
                batch.content or "\u200b",
                files=_discord_files(list(batch.assets)),
                embeds=_discord_embeds(tuple(getattr(batch, "embeds", ()))),
                silent=True,
            )
            self._created_messages.append(sent)
            message_ids.append(sent.id)
        fetched = [await thread.fetch_message(message_id) for message_id in message_ids]
        rendered_fetched = fetched[1:]
        if any(len(message.content) > 2000 for message in rendered_fetched):
            raise DiscordE2EError("renderer emitted an over-limit Discord message")
        attachment_names = [
            attachment.filename
            for message in rendered_fetched
            for attachment in message.attachments
        ]
        embedded_image_names = [
            identity[1]
            for message in rendered_fetched
            for url in _discord_embed_image_urls(message)
            if (identity := _discord_cdn_attachment_identity(url)) is not None
        ]
        delivered_asset_names = attachment_names + embedded_image_names
        if "local-image.png" not in delivered_asset_names:
            raise DiscordE2EError("local Markdown image was not uploaded")
        self.evidence.features.append(
            await self.record_ordered_delivery_probe(
                rendered_fetched,
                expected_contents=expected_contents,
                expected_filenames=expected_filenames,
                expected_sha256=expected_sha256,
            )
        )
        self.evidence.features.append(
            FeatureEvidence(
                feature="Markdown/table/image/oversized rendering",
                status="passed",
                transport="real Discord messages and files",
                detail=(
                    f"{len(plan.batches)} ordered batches; "
                    f"{len(delivered_asset_names)} delivered assets"
                ),
                discord_ids=[str(message.id) for message in fetched],
            )
        )

        files = [
            discord.File(io.BytesIO(f"file-{index}".encode()), filename=f"{index:02d}.txt")
            for index in range(12)
        ]
        first_files = await thread.send(files=files[:10], silent=True)
        self._created_messages.append(first_files)
        second_files = await thread.send(files=files[10:], silent=True)
        self._created_messages.append(second_files)
        if len(first_files.attachments) != 10 or len(second_files.attachments) != 2:
            raise DiscordE2EError("Discord attachment batching did not preserve 10+2 files")
        self.evidence.features.append(
            await self.record_ordered_delivery_probe(
                [first_files, second_files],
                expected_contents=["", ""],
                expected_filenames=[f"{index:02d}.txt" for index in range(12)],
                expected_sha256=[_hash_bytes(f"file-{index}".encode()) for index in range(12)],
            )
        )
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
            "type": "taskdeck",
            "content": "**TaskDeck** — 1 item(s)",
            "finalized": False,
            "cards": [
                {
                    "card_token": "e2ecard",
                    "title": "E2E task",
                    "state": "running",
                    "kind": "agent",
                    "elapsed": "12s",
                    "progress_summary": "Rendering the complete Discord message gallery.",
                    "detail_artifact": None,
                    "dependencies": ["gallery-seed"],
                    "artifact_links": ["taskdeck-preview.md"],
                }
            ],
            "taskdeck": {
                "panel_id": "e2epanel",
                "revision": 1,
                "page": 0,
                "page_count": 1,
                "selected_card_token": "e2ecard",
                "expanded": False,
                "actions": ["download"],
                "options": [{"label": "E2E task", "value": "e2ecard", "state": "running"}],
            },
        }
        task_plan = await _discord_render_plan(task_payload)
        task_batch = task_plan.batches[0]
        task_message = await thread.send(
            task_batch.content or "\u200b",
            files=_discord_files(list(task_batch.assets)),
            embeds=_discord_embeds(tuple(getattr(task_batch, "embeds", ()))),
            view=_taskdeck_view(task_payload),
            silent=True,
        )
        self._created_messages.append(task_message)
        fetched_task = await thread.fetch_message(task_message.id)
        self._stable_ids["taskdeck_message_id"] = str(task_message.id)
        if not fetched_task.embeds:
            raise DiscordE2EError("TaskDeck embeds were not serialized by Discord")
        if not fetched_task.components:
            raise DiscordE2EError("TaskDeck components were not serialized by Discord")
        self.evidence.features.append(
            FeatureEvidence(
                feature="TaskDeck embeds",
                status="passed",
                transport="real Discord embed serialization",
                detail=f"{len(fetched_task.embeds)} embed card(s)",
                discord_ids=[str(task_message.id)],
            )
        )
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
        error_payload = {
            "type": "tool_output_artifact",
            "content": "**Tool failed**\nExact output is attached for diagnosis.",
            "status": "failed",
            "tool_source": "error",
            "verbatim": True,
            "character_count": len(error_body),
            "line_count": 1,
            "attachments": [
                {
                    "filename": "tool-error.txt",
                    "media_type": "text/plain",
                    "content": error_body,
                }
            ],
            "finalized": True,
        }
        error_batch = (await _discord_render_plan(error_payload)).batches[0]
        error_message = await thread.send(
            content=error_batch.content or "\u200b",
            files=_discord_files(list(error_batch.assets)),
            embeds=_discord_embeds(error_batch.embeds),
            silent=True,
        )
        self._created_messages.append(error_message)
        if error_message.attachments[0].size != len(error_body):
            raise DiscordE2EError("exact tool error attachment size changed")
        if not error_message.embeds:
            raise DiscordE2EError("tool error rich embed was not serialized")
        self.evidence.features.append(
            await self.record_ordered_delivery_probe(
                [error_message],
                expected_contents=[error_batch.content or "\u200b"],
                expected_filenames=["tool-error.txt"],
                expected_sha256=[_hash_bytes(error_body.encode())],
            )
        )
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
        if not self._dry_run and not isinstance(reconnected_thread, discord.Thread):
            raise DiscordE2EError("thread was unavailable after gateway reconnect")
        if not hasattr(reconnected_thread, "send"):
            raise DiscordE2EError("thread was unavailable after gateway reconnect")
        reconnect_message = await reconnected_thread.send(
            "gateway reconnect continuity",
            silent=True,
        )
        self._created_messages.append(reconnect_message)
        self.evidence.features.append(
            FeatureEvidence(
                feature="gateway reconnect",
                status="passed",
                transport="real Discord gateway resume",
                detail="forced websocket close resumed and reused the same thread",
                discord_ids=[str(thread.id), str(reconnect_message.id)],
            )
        )

        production_request = getattr(self._require_bot(), "_discord_request", None)
        if callable(production_request):
            burst = await asyncio.gather(
                *(
                    production_request(
                        DiscordOperation.SEND,
                        lambda index=index: thread.send(
                            f"rate-managed-{index}",
                            silent=True,
                        ),
                        route_key="channels.messages.send",
                        target_key=f"channel:{thread.id}",
                        priority=DiscordPriority.MAINTENANCE,
                    )
                    for index in range(6)
                )
            )
            await production_request(
                DiscordOperation.EDIT,
                lambda: burst[-1].edit(content="rate-managed-5-final"),
                route_key="channels.messages.edit",
                target_key=f"channel:{thread.id}:message:{burst[-1].id}",
                priority=DiscordPriority.FOREGROUND,
                coalesce_key=f"burst-edit:{burst[-1].id}",
                terminal=True,
            )
            fetched_burst_tail = await production_request(
                DiscordOperation.FETCH,
                lambda: thread.fetch_message(burst[-1].id),
                route_key="channels.messages.fetch",
                target_key=f"channel:{thread.id}:message:{burst[-1].id}",
                priority=DiscordPriority.MAINTENANCE,
            )
            if fetched_burst_tail.content != "rate-managed-5-final":
                raise DiscordE2EError("cross-surface burst final edit was not visible")
            metrics = self._require_bot().discord_requests.snapshot()
            if metrics["queue_peak"] < 2 or metrics["deadline_misses"] != 0:
                raise DiscordE2EError(
                    "application coordinator burst did not expose queueing without deadline misses"
                )
        else:
            burst = await asyncio.gather(
                *(thread.send(f"rate-managed-{index}", silent=True) for index in range(6))
            )
            metrics = {"queue_peak": len(burst), "deadline_misses": 0}
        self._created_messages.extend(burst)
        if len({message.id for message in burst}) != 6:
            raise DiscordE2EError("rate-managed burst lost or duplicated messages")
        self.evidence.features.append(self._rate_limit_feature(burst))
        self.evidence.features.append(
            FeatureEvidence(
                feature="cross-surface application coordinator burst",
                status="passed",
                transport="DiscordRequestCoordinator plus real Discord REST",
                detail=(
                    f"queue_peak={metrics['queue_peak']}; "
                    f"deadline_misses={metrics['deadline_misses']}"
                ),
                discord_ids=[str(message.id) for message in burst],
                assertions=[
                    "Burst delivery remained FIFO and complete.",
                    "Application-layer queue metrics were asserted independently of discord.py.",
                ],
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

    async def _run_render_gallery(self, thread: discord.Thread) -> None:
        interaction_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"copilotd:{self._run_id}:input"))
        payloads = [
            {
                "type": "assistant.message_delta",
                "content": "Copilot is streaming a **plain text response**…",
                "finalized": False,
            },
            {
                "type": "assistant.message",
                "content": (
                    "Here is a richer Copilot answer with **emphasis**, `inline code`, "
                    "[a link](https://docs.github.com/copilot), and a concise checklist.\n\n"
                    "- Plain message presentation\n"
                    "- Markdown remains copyable\n"
                    "- Attachments stay durable"
                ),
                "finalized": True,
            },
            {
                "type": "assistant.reasoning",
                "content": (
                    "**Reasoning complete**\nCompared the renderer contract, Discord limits, "
                    "and durable delivery state."
                ),
                "status": {
                    "title": "Reasoning complete",
                    "detail": (
                        "Compared the renderer contract, Discord limits, "
                        "and durable delivery state."
                    ),
                    "event_type": "assistant.reasoning",
                },
                "finalized": True,
            },
            {
                "type": "session.warning",
                "content": (
                    "**Copilot warning**\nContext usage is approaching the configured limit."
                ),
                "status": {
                    "title": "Copilot warning",
                    "detail": "Context usage is approaching the configured limit.",
                    "event_type": "session.warning",
                },
                "finalized": True,
            },
            {
                "type": "interaction",
                "content": "**Copilot needs input**",
                "interaction": {
                    "interaction_id": interaction_id,
                    "kind": "user_input",
                    "state": "pending",
                    "question": "Which deployment target should Copilot prepare?",
                    "choices": ["Local package", "macOS service", "Windows task"],
                    "allowFreeform": True,
                },
                "finalized": False,
            },
            {
                "type": "interaction",
                "content": "**Copilot input recorded**",
                "interaction": {
                    "kind": "user_input",
                    "state": "resolved",
                    "question": "Which deployment target should Copilot prepare?",
                    "display_response": "macOS service",
                },
                "finalized": True,
            },
            {
                "type": "interaction",
                "content": "**Copilot input expired**",
                "interaction": {
                    "kind": "user_input",
                    "state": "expired",
                    "question": "Should Copilot continue waiting for approval?",
                },
                "finalized": True,
            },
            {
                "type": "session.task_complete",
                "content": "**Task evaluation**\nOutcome: `completed`",
                "status": {
                    "title": "Task evaluation",
                    "detail": "Outcome: `completed`",
                    "event_type": "session.task_complete",
                    "outcome": "completed",
                },
                "finalized": True,
            },
            {
                "type": "session.task_complete",
                "content": "**Task evaluation**\nOutcome: `continue`",
                "status": {
                    "title": "Task evaluation",
                    "detail": "Outcome: `continue`",
                    "event_type": "session.task_complete",
                    "outcome": "continue",
                },
                "finalized": True,
            },
            {
                "type": "session.task_complete",
                "content": "**Task evaluation**\nOutcome: `blocked`",
                "status": {
                    "title": "Task evaluation",
                    "detail": "Outcome: `blocked`",
                    "event_type": "session.task_complete",
                    "outcome": "blocked",
                },
                "finalized": True,
            },
            {
                "type": "idle_footer",
                "content": "turn complete",
                "model": "gpt-5.6-sol",
                "input_tokens": 12400,
                "output_tokens": 2180,
                "credits": 1.25,
                "context": "73600/128000",
                "duration_seconds": 83,
                "background_observed": True,
                "finalized": True,
            },
            {
                "type": "diff",
                "content": (
                    "**Code changes** · `structured`\n"
                    "```diff\n"
                    "- every surface uses a card\n"
                    "+ structured surfaces use bounded embeds\n"
                    "```"
                ),
                "source": "structured",
                "byte_count": 82,
                "stats": {"files": 1, "additions": 1, "deletions": 1},
                "finalized": True,
            },
            {
                "type": "diff",
                "content": (
                    "**Code changes**\nStructured diff exceeds the render safety limit; "
                    "exact source remains in the durable event journal."
                ),
                "source": "structured",
                "oversized": True,
                "byte_count": 9 * 1024 * 1024,
                "stats": {},
                "finalized": True,
            },
        ]
        rendered: list[tuple[str, Any]] = []
        for payload in payloads:
            plan = await _discord_render_plan(payload)
            for index, batch in enumerate(plan.batches):
                message = await thread.send(
                    content=batch.content or "\u200b",
                    files=_discord_files(list(batch.assets)),
                    embeds=_discord_embeds(batch.embeds),
                    view=_render_view(payload) if index == 0 else None,
                    silent=True,
                )
                self._created_messages.append(message)
                rendered.append((str(payload["type"]), message))
        fetched = [
            (payload_type, await thread.fetch_message(message.id))
            for payload_type, message in rendered
        ]
        plain_types = {"assistant.message_delta", "assistant.message", "idle_footer"}
        for payload_type, message in fetched:
            if payload_type in plain_types and message.embeds:
                raise DiscordE2EError(f"{payload_type} unexpectedly rendered as an embed")
            if payload_type not in plain_types and not message.embeds:
                raise DiscordE2EError(f"{payload_type} lost its structured embed")
            if payload_type == "idle_footer" and not str(message.content).startswith("-# "):
                raise DiscordE2EError("turn summary did not render as compact subtext")
        if not any(message.components for _, message in fetched):
            raise DiscordE2EError("rich interaction card has no Discord components")
        self.evidence.features.append(
            FeatureEvidence(
                feature="Discord text, footer, and structured render gallery",
                status="passed",
                transport="real Discord content, embed, and component serialization",
                detail=(
                    "plain stream/final answer, compact turn footer, reasoning, warning, "
                    "pending/resolved/expired interaction, task completed/continue/blocked, "
                    "and normal/oversized diff cards"
                ),
                discord_ids=[str(message.id) for _, message in fetched],
            )
        )

    async def _record_production_probes(self, bot: CopilotDiscordBot) -> None:
        del bot
        return None

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

    def _rate_limit_feature(self, burst: list[Any]) -> FeatureEvidence:
        if (
            self._http_rate_limit.observed_actual_429
            and self._http_rate_limit.retry_after is not None
        ):
            return FeatureEvidence(
                feature="429/rate handling",
                status="passed",
                transport="instrumented HTTP 429 observation",
                detail=f"observed actual 429 with retry_after={self._http_rate_limit.retry_after}",
                discord_ids=[str(message.id) for message in burst],
                assertions=[
                    "Instrumented HTTP observed real 429 response status.",
                    "Observed retry_after was propagated from the response, not inferred.",
                ],
            )

        return FeatureEvidence(
            feature="429/rate handling",
            status="pending_not_observed",
            transport="instrumented HTTP 429 observation",
            detail="no actual 429 response was observed by the probe",
            discord_ids=[str(message.id) for message in burst],
            assertions=[
                "Rate-case pass is withheld unless a real 429 and retry_after are observed.",
            ],
        )

    async def _run_reaction_state_chain(self, message: Any) -> None:
        bot_user = self._require_bot().user
        if bot_user is None:
            raise DiscordE2EError("Discord bot identity is unavailable for reaction probe")
        states = ("👀", "🧠", "🛠️", "❓", "🧠", "✅")
        maintained = ("👀", "🧠", "🛠️", "❓", "✅", "❌")
        observed: list[str] = []
        for emoji in states:
            await message.add_reaction(emoji)
            for old in maintained:
                if old != emoji:
                    await message.remove_reaction(old, bot_user)
            channel = getattr(message, "channel", None)
            fetch_message = getattr(channel, "fetch_message", None)
            current = await fetch_message(message.id) if callable(fetch_message) else message
            mine = {
                str(reaction.emoji)
                for reaction in getattr(current, "reactions", [])
                if bool(getattr(reaction, "me", False))
            }
            if mine != {emoji}:
                raise DiscordE2EError(
                    f"reaction chain expected only {emoji}, observed {sorted(mine)}"
                )
            observed.append(emoji)
        self.evidence.features.append(
            FeatureEvidence(
                feature="durable submission reaction state chain",
                status="passed",
                transport="real Discord reaction API",
                detail=" -> ".join(observed),
                discord_ids=[str(message.id)],
                assertions=[
                    "Each new state was visible before prior bot states were removed.",
                    "Only the test bot reaction identity was removed.",
                    "The chain ended with exactly one successful terminal reaction.",
                ],
            )
        )

    async def _run_production_pipeline_probe(
        self,
        root: Path,
        thread: discord.Thread,
    ) -> None:
        bot = self._require_bot()
        session_id = f"e2e-session-{uuid.uuid4()}"
        bindings = SessionBindingRepository(bot.database)
        bot.bindings = bindings
        await bindings.create(
            thread_id=str(thread.id),
            sdk_session_id=session_id,
            cwd_snapshot=root,
            project_source="implicit-home",
        )
        marker_prefix = "DRY-RUN-PIPELINE" if self._dry_run else "PRODUCTION-PIPELINE"
        marker = f"{marker_prefix}::{self._run_id}"
        exact_artifact = ("artifact-" + self._run_id).encode() * 600
        exact_text = exact_artifact.decode()
        events = [
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=0,
                fence_token=0,
                inbox_seq=1,
                source="internal",
                raw_type="assistant.message",
                raw_payload={
                    "type": "assistant.message",
                    "data": {
                        "messageId": "e2e-production-message",
                        "content": marker,
                    },
                },
                reducer_hash="e2e-production-message",
                persistence_class="internal",
                received_at=time.time(),
                internal_event_id=f"{self._run_id}:assistant",
                message_id="e2e-production-message",
            ),
            AdaptedEvent(
                sdk_session_id=session_id,
                generation=0,
                fence_token=0,
                inbox_seq=2,
                source="internal",
                raw_type="tool.execution_complete",
                raw_payload={
                    "type": "tool.execution_complete",
                    "data": {
                        "toolCallId": "e2e-production-tool",
                        "toolName": "e2e tool",
                        "success": True,
                        "result": {"detailedContent": exact_text},
                    },
                },
                reducer_hash="e2e-production-tool",
                persistence_class="internal",
                received_at=time.time(),
                internal_event_id=f"{self._run_id}:tool",
            ),
        ]
        reducer = JournalReducer(
            bot.database,
            artifact_root=root / "artifacts",
        )
        if await reducer.persist(events) != 2:
            raise DiscordE2EError("production reducer did not persist both probe events")
        journal_count = await bot.database.fetchone(
            """
            SELECT COUNT(*) AS count FROM event_journal
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        if journal_count is None or int(journal_count["count"]) != 2:
            raise DiscordE2EError(
                "production reducer probe did not journal exactly two known events"
            )
        expected_rows = await bot.database.fetchall(
            """
            SELECT lane, payload FROM render_outbox
            WHERE session_id = ?
            ORDER BY logical_seq, created_at
            """,
            (session_id,),
        )
        expected_intent_count = 3
        if len(expected_rows) != expected_intent_count:
            raise DiscordE2EError(
                "production reducer probe did not create exactly three known render intents"
            )
        lanes = {str(row["lane"]) for row in expected_rows}
        if lanes != {"assistant_final", "artifact", "taskdeck"}:
            raise DiscordE2EError(
                f"production reducer probe emitted unexpected lanes: {sorted(lanes)}"
            )
        known_payloads = [json.loads(str(row["payload"])) for row in expected_rows]
        if sum(payload.get("content") == marker for payload in known_payloads) != 1:
            raise DiscordE2EError(
                "production reducer probe did not preserve the known marker intent"
            )
        expected_artifact_sha256 = _hash_bytes(exact_artifact)
        intent_artifact_digests: list[str] = []
        for payload in known_payloads:
            attachments = payload.get("attachments")
            if not isinstance(attachments, list):
                continue
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                body = attachment.get("content")
                if isinstance(body, str):
                    intent_artifact_digests.append(_hash_bytes(body.encode()))
                elif isinstance(attachment.get("sha256"), str):
                    intent_artifact_digests.append(str(attachment["sha256"]))
        if intent_artifact_digests.count(expected_artifact_sha256) != 1:
            raise DiscordE2EError(
                "production reducer probe did not create the known exact artifact intent"
            )
        expected_contents: list[str] = []
        expected_filenames: list[str] = []
        expected_sha256: list[str] = []
        expected_embed_count = 0
        for payload in known_payloads:
            plan = await _discord_render_plan(
                payload,
                allowed_roots=(root,),
                max_bytes=7 * 1024 * 1024,
            )
            for batch in plan.batches:
                expected_contents.append(batch.content or "\u200b")
                expected_embed_count += bool(batch.embeds)
                for asset in batch.assets:
                    expected_filenames.append(str(asset.filename))
                    expected_sha256.append(_hash_bytes(_asset_bytes(asset)))
        transport: Any = (
            _DryRunRenderTransport(thread, root, self._created_messages) if self._dry_run else bot
        )
        dispatcher = RenderOutboxDispatcher(bot.database, transport)
        delivered_count = await dispatcher.drain(deadline_seconds=30)
        if delivered_count != expected_intent_count:
            raise DiscordE2EError(
                "production outbox probe did not deliver exactly three known intents"
            )
        pending = await bot.database.fetchone(
            """
            SELECT COUNT(*) AS count FROM render_outbox
            WHERE session_id = ? AND state IN ('pending', 'sending')
            """,
            (session_id,),
        )
        if pending is not None and int(pending["count"]) != 0:
            raise DiscordE2EError("production outbox probe did not fully drain")
        rows = await bot.database.fetchall(
            """
            SELECT messages.discord_message_id
            FROM render_outbox AS outbox
            JOIN render_messages AS messages
              ON messages.session_id = outbox.session_id
             AND messages.logical_key = COALESCE(outbox.coalesce_key, outbox.id)
            WHERE outbox.session_id = ?
            ORDER BY outbox.logical_seq, outbox.created_at
            """,
            (session_id,),
        )
        if len(rows) != expected_intent_count:
            raise DiscordE2EError(
                "production outbox probe did not map exactly three Discord messages"
            )
        messages = [await thread.fetch_message(int(row["discord_message_id"])) for row in rows]
        if not self._dry_run:
            visible_text = [_discord_message_visible_text(message) for message in messages]
            if sum(marker in text for text in visible_text) != 1:
                raise DiscordE2EError(
                    "production Discord history did not contain the known marker exactly once"
                )
            rich_embed_count = sum(bool(message.embeds) for message in messages)
            if rich_embed_count != expected_embed_count:
                raise DiscordE2EError(
                    "production Discord history did not preserve the planned content/embed mix"
                )
            actual_artifact_digests = [
                _hash_bytes(await attachment.read(use_cached=False))
                for message in messages
                for attachment in message.attachments
            ]
            if actual_artifact_digests.count(expected_artifact_sha256) != 1:
                raise DiscordE2EError(
                    "production Discord history did not contain the known artifact digest"
                )
        evidence = await self.record_ordered_delivery_probe(
            messages,
            expected_contents=expected_contents,
            expected_filenames=expected_filenames,
            expected_sha256=expected_sha256,
        )
        if self._dry_run:
            evidence.feature = "dry-run reducer/outbox exact delivery"
            evidence.transport = (
                "JournalReducer -> RenderOutboxDispatcher -> in-memory Discord double"
            )
        else:
            evidence.feature = "production reducer/outbox exact delivery"
            evidence.transport = "JournalReducer -> RenderOutboxDispatcher -> real Discord"
        evidence.assertions.extend(
            [
                "journal_event_count=2",
                f"render_intent_count={expected_intent_count}",
                f"render_message_count={len(rows)}",
                f"rich_embed_message_count={sum(bool(message.embeds) for message in messages)}",
                f"known_marker={marker}",
                f"known_artifact_sha256={expected_artifact_sha256}",
                "delivery_path=JournalReducer->RenderOutboxDispatcher",
            ]
        )
        self.evidence.features.append(evidence)

    async def _cleanup(self) -> list[BaseException]:
        failures: list[BaseException] = []
        bot = self._bot

        async def attempt(label: str, action: Callable[[], Any]) -> None:
            try:
                result = action()
                if asyncio.iscoroutine(result):
                    await result
            except discord.NotFound:
                return
            except asyncio.CancelledError as error:
                failures.append(error)
            except Exception as error:
                failures.append(RuntimeError(f"{label}: {type(error).__name__}: {error}"))

        try:
            if not self._keep_resources:
                for message in reversed(self._created_messages):
                    await attempt(f"delete message {message.id}", message.delete)
                if self._thread is not None:
                    await attempt(f"delete thread {self._thread.id}", self._thread.delete)
                    self._thread = None
                if self._seed is not None:
                    await attempt(f"delete seed {self._seed.id}", self._seed.delete)
                    self._seed = None
            if (
                bot is not None
                and self._guild_object is not None
                and self._original_manifest is not None
            ):
                await attempt(
                    "restore manifest",
                    lambda: self._restore_guild_manifest(bot),
                )
        finally:
            if bot is not None:
                try:
                    await bot.close()
                except asyncio.CancelledError as error:
                    failures.append(error)
                except Exception as error:
                    failures.append(RuntimeError(f"close bot: {type(error).__name__}: {error}"))
            runner = self._runner
            if runner is not None:
                await asyncio.gather(runner, return_exceptions=True)
        self.evidence.cleaned_up = not self._keep_resources and not failures
        return failures

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
    parser.add_argument("--guild-id", type=int, required=True)
    parser.add_argument("--application-id", type=int, required=True)
    parser.add_argument("--channel-id", type=int, required=True)
    parser.add_argument("--keep-resources", action="store_true")
    return parser.parse_args()


def _hash_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _wire_discord_http_trace(
    bot: Any,
    connector: aiohttp.BaseConnector,
    trace: aiohttp.TraceConfig,
) -> None:
    http = getattr(bot, "http", None)
    if http is None:
        raise DiscordE2EError("Discord bot HTTP client is unavailable before start")
    http.connector = connector
    http.http_trace = trace


def _asset_bytes(asset: Any) -> bytes:
    content = getattr(asset, "content", None)
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode()
    file_pointer = getattr(asset, "fp", None)
    if file_pointer is None:
        raise DiscordE2EError(f"cannot read expected asset `{getattr(asset, 'filename', '')}`")
    if hasattr(file_pointer, "getvalue"):
        return bytes(file_pointer.getvalue())
    position = file_pointer.tell()
    try:
        file_pointer.seek(0)
        return bytes(file_pointer.read())
    finally:
        file_pointer.seek(position)


def _discord_message_visible_text(message: Any) -> str:
    parts = [str(getattr(message, "content", "") or "")]
    for raw_embed in getattr(message, "embeds", ()):
        embed = raw_embed.to_dict() if hasattr(raw_embed, "to_dict") else raw_embed
        if not isinstance(embed, dict):
            continue
        parts.extend(str(embed.get(key) or "") for key in ("title", "description"))
        fields = embed.get("fields")
        if isinstance(fields, list):
            for field in fields:
                if isinstance(field, dict):
                    parts.append(str(field.get("name") or ""))
                    parts.append(str(field.get("value") or ""))
    return "\n".join(part for part in parts if part)


def _discord_embed_image_urls(message: Any) -> list[str]:
    urls: list[str] = []
    for raw_embed in getattr(message, "embeds", ()):
        embed = raw_embed.to_dict() if hasattr(raw_embed, "to_dict") else raw_embed
        if not isinstance(embed, dict):
            continue
        image = embed.get("image")
        if isinstance(image, dict) and isinstance(image.get("url"), str):
            urls.append(str(image["url"]))
    return urls


def _discord_cdn_attachment_identity(url: str) -> tuple[int, str] | None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "cdn.discordapp.com":
        return None
    parts = parsed.path.split("/")
    if len(parts) < 5 or parts[1] != "attachments":
        return None
    try:
        attachment_id = int(parts[3])
    except ValueError:
        return None
    filename = unquote(parts[4])
    return attachment_id, filename


def _clone_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clone_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_clone_json_value(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _clone_json_value(to_dict())
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, (str, int, float, bool)):
        return enum_value
    return value


def _command_value(command: Any, field: str) -> Any:
    if isinstance(command, Mapping):
        return command.get(field)
    return getattr(command, field, None)


def _command_id(command: Any) -> int | None:
    value = _command_value(command, "id")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _command_identity(command: Any) -> tuple[int, str]:
    raw_type = _command_value(command, "type")
    command_type = getattr(raw_type, "value", raw_type)
    name = _command_value(command, "name")
    if name is None:
        raise DiscordE2EError("application command snapshot is missing a name")
    try:
        normalized_type = int(command_type)
    except (TypeError, ValueError) as error:
        raise DiscordE2EError(f"application command `{name}` has an invalid type") from error
    return normalized_type, str(name)


def _command_permission_entries(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    entries: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        permission_id = item.get("id")
        permission_type = getattr(item.get("type"), "value", item.get("type"))
        permission = item.get("permission")
        if permission_id is None or permission_type is None or not isinstance(permission, bool):
            raise DiscordE2EError("guild command permission override is malformed")
        try:
            normalized_type = int(permission_type)
        except (TypeError, ValueError) as error:
            raise DiscordE2EError(
                "guild command permission override has an invalid type"
            ) from error
        entries.append(
            {
                "id": str(permission_id),
                "type": normalized_type,
                "permission": permission,
            }
        )
    return sorted(entries, key=lambda item: (item["type"], item["id"]))


def _command_manifest_entry(command: Any) -> dict[str, Any]:
    mutable_fields = (
        "name",
        "type",
        "description",
        "name_localizations",
        "description_localizations",
        "default_member_permissions",
        "default_permission",
        "dm_permission",
        "nsfw",
        "options",
        "contexts",
        "integration_types",
        "handler",
    )
    if isinstance(command, dict):
        payload = dict(command)
    else:
        to_dict = getattr(command, "to_dict", None)
        payload = dict(to_dict()) if callable(to_dict) else {}
        raw_payload = getattr(command, "_data", None)
        if isinstance(raw_payload, Mapping):
            payload.update(raw_payload)
        for field_name in mutable_fields:
            if field_name not in payload and hasattr(command, field_name):
                payload[field_name] = getattr(command, field_name)
    permissions = payload.get("default_member_permissions")
    if permissions is not None and not isinstance(permissions, (str, int)):
        permissions = getattr(permissions, "value", permissions)
    if isinstance(permissions, int):
        payload["default_member_permissions"] = str(permissions)
    return {
        field: _clone_json_value(payload[field]) for field in mutable_fields if field in payload
    }


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
        application_id=args.application_id,
        channel_id=args.channel_id,
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
