from __future__ import annotations

import asyncio
import hashlib
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import discord
import pytest

import copilotd.e2e.discord_harness as harness_module
from copilotd.e2e.discord_harness import (
    DiscordE2EConfigurationError,
    DiscordE2EError,
    DiscordRealHarness,
    FeatureEvidence,
    load_required_targets,
    load_required_token,
    write_evidence,
)
from copilotd.storage.database import Database


@dataclass
class FakeAttachment:
    content: bytes
    filename: str = "attachment.bin"

    async def read(self, *, use_cached: bool = True) -> bytes:
        assert not use_cached
        return self.content


@dataclass
class FakeMessage:
    id: int
    content: str = ""
    attachments: list[FakeAttachment] = field(default_factory=list)
    embeds: list[object] = field(default_factory=list)
    deleted: bool = False

    async def delete(self) -> None:
        self.deleted = True


@dataclass
class FakeThread:
    id: int
    deleted: bool = False

    async def delete(self) -> None:
        self.deleted = True


@dataclass
class FakeChannel:
    id: int
    guild: FakeGuild | None = None  # type: ignore[name-defined]


@dataclass
class FakeGuild:
    id: int
    channel: FakeChannel

    def __post_init__(self) -> None:
        self.channel.guild = self

    def get_channel(self, channel_id: int):
        return self.channel if channel_id == self.channel.id else None


class FakeTree:
    def __init__(self, snapshot: list[dict[str, object]] | None = None) -> None:
        self.snapshot = snapshot or []
        self.fetch_calls: list[int] = []
        self.copy_calls: list[int] = []
        self.sync_calls: list[int] = []

    async def fetch_commands(self, guild) -> list[dict[str, object]]:
        self.fetch_calls.append(guild.id)
        return list(self.snapshot)

    def copy_global_to(self, *, guild) -> None:
        self.copy_calls.append(guild.id)

    async def sync(self, *, guild) -> list[SimpleNamespace]:
        self.sync_calls.append(guild.id)
        return [SimpleNamespace(name=entry["name"]) for entry in self.snapshot]

    def get_commands(self, *, guild) -> list[SimpleNamespace]:
        return [SimpleNamespace(name=entry["name"]) for entry in self.snapshot]


class FakeHttp:
    def __init__(
        self,
        command_permissions: list[dict[str, object]] | None = None,
    ) -> None:
        self.restore_calls: list[tuple[int, int, list[dict[str, object]]]] = []
        self.command_permissions = command_permissions or []
        self.permission_reads: list[tuple[int, int]] = []
        self.permission_edits: list[tuple[int, int, int, dict[str, object]]] = []

    async def bulk_upsert_guild_commands(
        self,
        application_id: int,
        guild_id: int,
        commands: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        self.restore_calls.append((application_id, guild_id, list(commands)))
        return [{**command, "id": 9000 + index} for index, command in enumerate(commands, start=1)]

    async def get_guild_application_command_permissions(
        self,
        application_id: int,
        guild_id: int,
    ) -> list[dict[str, object]]:
        self.permission_reads.append((application_id, guild_id))
        return list(self.command_permissions)

    async def edit_application_command_permissions(
        self,
        application_id: int,
        guild_id: int,
        command_id: int,
        payload: dict[str, object],
    ) -> None:
        self.permission_edits.append((application_id, guild_id, command_id, dict(payload)))


class FakeBot:
    def __init__(
        self,
        *,
        application_id: int,
        guilds: list[FakeGuild],
        tree: FakeTree | None = None,
        http: FakeHttp | None = None,
    ) -> None:
        self.application_id = application_id
        self.guilds = guilds
        self.tree = tree or FakeTree()
        self.http = http or FakeHttp()
        self.user = SimpleNamespace(id=777)
        self.closed = False

    def get_guild(self, guild_id: int):
        return next((guild for guild in self.guilds if guild.id == guild_id), None)

    async def close(self) -> None:
        self.closed = True


@dataclass
class DryRunAttachment:
    content: bytes
    filename: str

    @property
    def size(self) -> int:
        return len(self.content)

    async def read(self, *, use_cached: bool = True) -> bytes:
        assert not use_cached
        return self.content


@dataclass
class DryRunMessage:
    id: int
    content: str = ""
    attachments: list[DryRunAttachment] = field(default_factory=list)
    embeds: list[object] = field(default_factory=list)
    components: list[object] = field(default_factory=list)
    channel: DryRunChannel | None = None  # type: ignore[name-defined]
    author: object = field(default_factory=lambda: SimpleNamespace(id=777))
    deleted: bool = False
    reactions: list[SimpleNamespace] = field(default_factory=list)

    async def delete(self) -> None:
        self.deleted = True

    async def pin(self, *, reason: str | None = None) -> None:
        if self.channel is not None:
            self.channel.pinned = self

    async def add_reaction(self, emoji: str) -> None:
        if emoji not in {str(reaction.emoji) for reaction in self.reactions}:
            self.reactions.append(SimpleNamespace(emoji=emoji, me=True))

    async def remove_reaction(self, emoji: str, _user: object) -> None:
        self.reactions = [reaction for reaction in self.reactions if str(reaction.emoji) != emoji]

    async def create_thread(self, *, name: str, auto_archive_duration: int):
        if self.channel is None:
            raise RuntimeError("seed message has no channel")
        thread = DryRunThread(id=self.id + 1000, name=name, channel=self.channel)
        self.channel.thread = thread
        return thread


@dataclass
class DryRunThread:
    id: int
    name: str
    channel: DryRunChannel  # type: ignore[name-defined]
    messages: list[DryRunMessage] = field(default_factory=list)
    archived: bool = False
    deleted: bool = False

    async def send(
        self,
        content: str = "",
        *,
        file=None,
        files=None,
        embeds=None,
        view=None,
        silent: bool = True,
    ):
        attachments: list[DryRunAttachment] = []
        payload_files = files if files is not None else file
        if payload_files:
            for file in payload_files if isinstance(payload_files, list) else [payload_files]:
                filename = getattr(file, "filename", "attachment.bin")
                fp = getattr(file, "fp", None)
                if fp is not None and hasattr(fp, "getvalue"):
                    payload = fp.getvalue()
                elif fp is not None:
                    position = fp.tell()
                    fp.seek(0)
                    payload = fp.read()
                    fp.seek(position)
                else:
                    payload = b""
                attachments.append(DryRunAttachment(payload, filename))
        message = DryRunMessage(
            id=self.channel.next_message_id(),
            content=content,
            attachments=attachments,
            embeds=list(embeds or []),
            components=[view] if view is not None else [],
            channel=self.channel,
        )
        self.messages.append(message)
        self.channel.messages.append(message)
        return message

    async def fetch_message(self, message_id: int) -> DryRunMessage:
        for message in self.messages:
            if message.id == message_id:
                return message
        raise RuntimeError(f"message {message_id} not found")

    async def history(self, limit=None, oldest_first: bool = True):
        ordered = sorted(self.messages, key=lambda message: message.id)
        for message in ordered:
            yield message

    async def edit(self, *, archived: bool) -> None:
        self.archived = archived

    async def delete(self) -> None:
        self.deleted = True


@dataclass
class DryRunChannel:
    id: int
    guild: DryRunGuild | None = None  # type: ignore[name-defined]
    messages: list[DryRunMessage] = field(default_factory=list)
    pinned: DryRunMessage | None = None
    thread: DryRunThread | None = None
    _message_seq: int = 100

    def next_message_id(self) -> int:
        self._message_seq += 1
        return self._message_seq

    async def send(self, content: str = "", *, silent: bool = True):
        message = DryRunMessage(
            id=self.next_message_id(),
            content=content,
            channel=self,
        )
        self.messages.append(message)
        return message

    async def pins(self):
        return [self.pinned] if self.pinned is not None else []


@dataclass
class DryRunGuild:
    id: int
    channel: DryRunChannel

    def __post_init__(self) -> None:
        self.channel.guild = self

    def get_channel(self, channel_id: int):
        return self.channel if channel_id == self.channel.id else None


class DryRunTree(FakeTree):
    pass


class DryRunHttp(FakeHttp):
    async def bulk_upsert_guild_commands(
        self,
        application_id: int,
        guild_id: int,
        commands: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        return await super().bulk_upsert_guild_commands(
            application_id,
            guild_id,
            commands,
        )


class DryRunBot:
    def __init__(
        self,
        *,
        application_id: int,
        database: Database,
        guilds: list[DryRunGuild],
        tree: DryRunTree | None = None,
        http: DryRunHttp | None = None,
    ) -> None:
        self.application_id = application_id
        self.database = database
        self.guilds = guilds
        self.tree = tree or DryRunTree()
        self.http = http or DryRunHttp()
        self.user = SimpleNamespace(id=777)
        self.closed = False
        self.registered = False
        self.start_called = False
        self.on_resumed = None

        async def _close_ws(*args, **kwargs):
            if callable(self.on_resumed):
                await self.on_resumed()

        self.ws = SimpleNamespace(close=_close_ws)

    def get_guild(self, guild_id: int):
        return next((guild for guild in self.guilds if guild.id == guild_id), None)

    async def fetch_channel(self, channel_id: int):
        for guild in self.guilds:
            if guild.channel.thread is not None and guild.channel.thread.id == channel_id:
                return guild.channel.thread
            if guild.channel.id == channel_id:
                return guild.channel
        raise RuntimeError(f"channel {channel_id} not found")

    def _register_application_commands(self) -> None:
        self.registered = True

    async def start(self, *args, **kwargs) -> None:
        self.start_called = True
        raise AssertionError("dry-run bot must not connect to Discord")

    async def close(self) -> None:
        self.closed = True
        await self.database.close()


def test_selected_e2e_requires_explicit_ids_in_env(tmp_path: Path) -> None:
    env_file = tmp_path / "missing.env"
    env_file.write_text("testbot=literal\n", encoding="utf-8")

    with pytest.raises(DiscordE2EConfigurationError, match="testbot_guild_id"):
        load_required_targets(env_file)


def test_env_loader_reads_token_without_shell_expansion(tmp_path: Path) -> None:
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "testbot='literal-$NOT_EXPANDED-token'\n"
        "testbot_guild_id=1\n"
        "testbot_application_id=2\n"
        "testbot_channel_id=3\n",
        encoding="utf-8",
    )

    assert load_required_token(env_file) == "literal-$NOT_EXPANDED-token"
    targets = load_required_targets(env_file)
    assert targets.guild_id == 1
    assert targets.application_id == 2
    assert targets.channel_id == 3


@pytest.mark.asyncio
async def test_harness_rejects_app_guild_channel_mismatch(tmp_path: Path) -> None:
    channel = FakeChannel(id=30)
    guild = FakeGuild(id=20, channel=channel)
    bot = FakeBot(application_id=10, guilds=[guild])
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    harness._bot = bot
    resolved_guild = harness._select_guild(bot)
    resolved_channel = harness._select_channel(resolved_guild)
    harness._verify_connected_identity(bot, resolved_guild, resolved_channel)

    bad_bot = FakeBot(application_id=11, guilds=[guild])
    with pytest.raises(DiscordE2EError, match="application"):
        harness._verify_connected_identity(bad_bot, resolved_guild, resolved_channel)

    bad_guild = FakeGuild(id=21, channel=channel)
    with pytest.raises(DiscordE2EError, match="guild"):
        harness._verify_connected_identity(bot, bad_guild, resolved_channel)

    bad_channel = FakeChannel(id=31, guild=guild)
    with pytest.raises(DiscordE2EError, match="channel"):
        harness._verify_connected_identity(bot, resolved_guild, bad_channel)


@pytest.mark.asyncio
async def test_manifest_restoration_uses_exact_snapshot(tmp_path: Path) -> None:
    channel = FakeChannel(id=30)
    guild = FakeGuild(id=20, channel=channel)
    tree = FakeTree(
        snapshot=[
            {
                "name": "existing",
                "type": 1,
                "description": "old",
                "name_localizations": {"en-US": "existing"},
                "description_localizations": {"en-US": "old"},
                "default_member_permissions": None,
                "dm_permission": True,
                "nsfw": False,
                "options": [
                    {
                        "type": 3,
                        "name": "input",
                        "description": "prompt",
                        "required": False,
                        "choices": [],
                    }
                ],
                "contexts": [0, 1],
                "integration_types": [0],
            }
        ]
    )
    http = FakeHttp()
    bot = FakeBot(application_id=10, guilds=[guild], tree=tree, http=http)
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        keep_resources=True,
    )
    harness._bot = bot
    harness._guild_object = discord.Object(id=20)
    snapshot = await harness._snapshot_guild_manifest(bot)
    harness._original_manifest = snapshot

    failures = await harness._cleanup()

    assert failures == []
    assert snapshot == tree.snapshot
    assert http.restore_calls == [(10, 20, tree.snapshot)]
    assert bot.closed is True


@pytest.mark.asyncio
async def test_manifest_restores_role_and_user_overrides_to_new_command_ids(
    tmp_path: Path,
) -> None:
    channel = FakeChannel(id=30)
    guild = FakeGuild(id=20, channel=channel)
    tree = FakeTree(
        snapshot=[
            {
                "id": 101,
                "name": "restricted",
                "type": 1,
                "description": "restricted",
                "options": [],
            }
        ]
    )
    permissions = [
        {
            "id": "101",
            "permissions": [
                {"id": "501", "type": 1, "permission": True},
                {"id": "601", "type": 2, "permission": False},
            ],
        }
    ]
    http = FakeHttp(command_permissions=permissions)
    bot = FakeBot(application_id=10, guilds=[guild], tree=tree, http=http)
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        keep_resources=True,
    )
    harness._bot = bot
    harness._guild_object = discord.Object(id=20)
    harness._original_manifest = await harness._snapshot_guild_manifest(bot)

    failures = await harness._cleanup()

    expected_payload = {
        "permissions": [
            {"id": "501", "type": 1, "permission": True},
            {"id": "601", "type": 2, "permission": False},
        ]
    }
    assert failures == []
    assert harness._original_manifest == [
        {
            "name": "restricted",
            "type": 1,
            "description": "restricted",
            "options": [],
        }
    ]
    assert http.permission_edits == [
        (10, 20, 101, expected_payload),
        (10, 20, 9001, expected_payload),
    ]


@pytest.mark.asyncio
async def test_manifest_override_restore_credentials_fail_before_sync(
    tmp_path: Path,
) -> None:
    channel = FakeChannel(id=30)
    guild = FakeGuild(id=20, channel=channel)
    tree = FakeTree(
        snapshot=[
            {
                "id": 101,
                "name": "restricted",
                "type": 1,
                "description": "restricted",
                "options": [],
            }
        ]
    )
    http = FakeHttp(
        command_permissions=[
            {
                "id": "101",
                "permissions": [
                    {"id": "501", "type": 1, "permission": True},
                ],
            }
        ]
    )

    async def forbidden_restore(*args, **kwargs):
        raise RuntimeError("missing applications.commands.permissions.update")

    http.edit_application_command_permissions = forbidden_restore  # type: ignore[assignment]
    bot = FakeBot(application_id=10, guilds=[guild], tree=tree, http=http)
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )

    with pytest.raises(DiscordE2EError, match="manifest sync was not attempted"):
        await harness._execute_ready_pipeline(bot, tmp_path)

    assert tree.copy_calls == []
    assert tree.sync_calls == []
    assert http.restore_calls == []


def test_manifest_merges_mutable_access_controls_from_attributes() -> None:
    command = SimpleNamespace(
        type=SimpleNamespace(value=1),
        name="restricted",
        description="restricted command",
        default_member_permissions=SimpleNamespace(value=8),
        default_permission=False,
        dm_permission=False,
        nsfw=True,
        contexts=[SimpleNamespace(value=0), SimpleNamespace(value=1)],
        integration_types=[SimpleNamespace(value=0)],
        to_dict=lambda: {
            "name": "restricted",
            "type": 1,
            "description": "restricted command",
            "options": [],
        },
    )

    manifest = harness_module._command_manifest_entry(command)

    assert manifest["default_member_permissions"] == "8"
    assert manifest["default_permission"] is False
    assert manifest["dm_permission"] is False
    assert manifest["nsfw"] is True
    assert manifest["contexts"] == [0, 1]
    assert manifest["integration_types"] == [0]


@pytest.mark.asyncio
async def test_http_trace_is_wired_to_discord_http_client_before_start() -> None:
    connector = aiohttp.TCPConnector()
    trace = aiohttp.TraceConfig()
    bot = SimpleNamespace(http=SimpleNamespace())
    try:
        harness_module._wire_discord_http_trace(bot, connector, trace)

        assert bot.http.connector is connector
        assert bot.http.http_trace is trace
    finally:
        await connector.close()


@pytest.mark.asyncio
async def test_cleanup_aggregates_not_found_and_writes_sanitized_evidence_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeNotFound(Exception):
        pass

    monkeypatch.setattr(harness_module.discord, "NotFound", FakeNotFound)

    async def boom_close() -> None:
        raise RuntimeError("close failed")

    class MessageThatFails(FakeMessage):
        async def delete(self) -> None:
            raise RuntimeError("message delete failed")

    class ThreadThatDisappears(FakeThread):
        async def delete(self) -> None:
            raise FakeNotFound("gone")

    channel = FakeChannel(id=30)
    guild = FakeGuild(id=20, channel=channel)
    http = FakeHttp()
    bot = FakeBot(application_id=10, guilds=[guild], http=http)
    bot.close = boom_close  # type: ignore[assignment]
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    harness._bot = bot
    harness._guild_object = discord.Object(id=20)
    harness._thread = ThreadThatDisappears(id=40)
    harness._seed = FakeMessage(id=41)
    harness._created_messages = [MessageThatFails(id=1, content="[redacted-token]", attachments=[])]
    harness._original_manifest = [{"name": "existing"}]
    harness.evidence.features.append(
        FeatureEvidence(
            feature="cleanup probe",
            status="failed",
            transport="unit",
            detail="authorization=secret",
            stable_identifiers={"token": "secret"},
        )
    )

    failures = await harness._cleanup()
    output = tmp_path / "evidence.json"
    write_evidence(output, harness.evidence)

    assert [str(failure) for failure in failures] == [
        "delete message 1: RuntimeError: message delete failed",
        "close bot: RuntimeError: close failed",
    ]
    assert harness.evidence.cleaned_up is False
    evidence_text = output.read_text(encoding="utf-8")
    assert "[redacted]" in evidence_text
    assert "authorization=secret" not in evidence_text


@pytest.mark.asyncio
async def test_ordered_content_and_attachment_sha256_probe(tmp_path: Path) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    messages = [
        FakeMessage(id=1, content="first", attachments=[FakeAttachment(b"alpha")]),
        FakeMessage(
            id=2,
            content="second",
            attachments=[FakeAttachment(b"beta", filename="二.txt")],
        ),
    ]

    evidence = await harness.record_ordered_delivery_probe(messages)

    assert evidence.status == "passed"
    assert "ordered_contents=['first', 'second']" in evidence.assertions[0]
    assert hashlib.sha256(b"alpha").hexdigest() in evidence.assertions[2]
    assert hashlib.sha256(b"beta").hexdigest() in evidence.assertions[2]


@pytest.mark.asyncio
async def test_ordered_probe_reads_images_folded_into_discord_embed_cdn(tmp_path: Path) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    image_url = (
        "https://cdn.discordapp.com/attachments/30/200/chart.png?ex=example&is=example&hm=example"
    )

    class FakeCdnHttp:
        async def get_from_cdn(self, url: str) -> bytes:
            assert url == image_url
            return b"chart-bytes"

    harness._bot = SimpleNamespace(http=FakeCdnHttp(), user=SimpleNamespace(id=777))
    message = FakeMessage(
        id=1,
        content="\u200b",
        embeds=[{"image": {"url": image_url}}],
    )

    evidence = await harness.record_ordered_delivery_probe(
        [message],
        expected_contents=["\u200b"],
        expected_filenames=["chart.png"],
        expected_sha256=[hashlib.sha256(b"chart-bytes").hexdigest()],
    )

    assert evidence.status == "passed"


@pytest.mark.asyncio
async def test_ordered_delivery_probe_rejects_mismatched_expected_values(tmp_path: Path) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    messages = [
        FakeMessage(
            id=1,
            content="first",
            attachments=[FakeAttachment(b"alpha", filename="one.txt")],
        ),
    ]

    with pytest.raises(DiscordE2EError, match="content mismatch"):
        await harness.record_ordered_delivery_probe(
            messages,
            expected_contents=["wrong"],
            expected_filenames=["one.txt"],
            expected_sha256=[hashlib.sha256(b"alpha").hexdigest()],
        )

    with pytest.raises(DiscordE2EError, match="filename mismatch"):
        await harness.record_ordered_delivery_probe(
            messages,
            expected_contents=["first"],
            expected_filenames=["wrong.txt"],
            expected_sha256=[hashlib.sha256(b"alpha").hexdigest()],
        )

    with pytest.raises(DiscordE2EError, match="sha256 mismatch"):
        await harness.record_ordered_delivery_probe(
            messages,
            expected_contents=["first"],
            expected_filenames=["one.txt"],
            expected_sha256=["deadbeef"],
        )


@pytest.mark.asyncio
async def test_dry_run_traverses_snapshot_sync_channel_probe_cleanup_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "dry.sqlite3")
    channel = DryRunChannel(id=30)
    guild = DryRunGuild(id=20, channel=channel)
    required_names = [
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
    ]
    tree = DryRunTree(
        snapshot=[
            {
                "name": name,
                "type": 1,
                "description": f"{name} description",
                "name_localizations": {"en-US": name},
                "description_localizations": {"en-US": f"{name} description"},
                "default_member_permissions": None,
                "dm_permission": True,
                "nsfw": False,
                "options": [],
                "contexts": [0],
                "integration_types": [0],
            }
            for name in required_names
        ]
    )
    http = DryRunHttp()

    async def fake_render_plan(*args, **kwargs):
        payload = args[0] if args and isinstance(args[0], dict) else {}
        payload_type = payload.get("type")
        plain_types = {"assistant.message_delta", "assistant.message", "idle_footer"}
        embeds = (
            ({"title": str(payload_type)},)
            if payload_type and payload_type not in plain_types
            else ()
        )
        content = "-# ✅ 🧠 test" if payload_type == "idle_footer" else "rendered content"
        attachments = payload.get("attachments")
        inline_assets = (
            [
                SimpleNamespace(
                    filename=str(attachment["filename"]),
                    fp=io.BytesIO(str(attachment["content"]).encode()),
                )
                for attachment in attachments
                if isinstance(attachment, dict) and isinstance(attachment.get("content"), str)
            ]
            if isinstance(attachments, list)
            else []
        )
        return SimpleNamespace(
            batches=[
                SimpleNamespace(
                    content=content,
                    embeds=embeds,
                    assets=inline_assets
                    or [
                        SimpleNamespace(
                            filename="local-image.png",
                            fp=io.BytesIO(b"image-bytes"),
                        )
                    ],
                )
            ]
        )

    def fake_files(assets):
        return list(assets)

    def fake_taskdeck_view(payload):
        return SimpleNamespace(payload=payload)

    monkeypatch.setattr(harness_module, "_discord_render_plan", fake_render_plan)
    monkeypatch.setattr(harness_module, "_discord_files", fake_files)
    monkeypatch.setattr(harness_module, "_taskdeck_view", fake_taskdeck_view)

    def bot_factory(settings):
        return DryRunBot(
            application_id=10,
            database=database,
            guilds=[guild],
            tree=tree,
            http=http,
        )

    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        dry_run=True,
        bot_factory=bot_factory,
    )
    evidence = await harness.run()

    assert harness._bot is not None and harness._bot.start_called is False
    assert harness._channel is channel
    assert evidence.guild_id == "20"
    assert evidence.channel_id == "30"
    assert http.restore_calls == [(10, 20, tree.snapshot)]
    assert any(
        feature.feature == "dry-run reducer/outbox exact delivery" for feature in evidence.features
    )
    assert not any(
        feature.feature == "production reducer/outbox exact delivery"
        for feature in evidence.features
    )
    assert any(feature.status == "pending_human_driver" for feature in evidence.features)
    persisted = json.loads((tmp_path / "evidence.json").read_text(encoding="utf-8"))
    assert persisted["guild_id"] == "20"
    assert persisted["channel_id"] == "30"


@pytest.mark.asyncio
async def test_production_probe_rejects_empty_reducer_outbox_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "empty-production.sqlite3")
    channel = DryRunChannel(id=30)
    guild = DryRunGuild(id=20, channel=channel)
    bot = DryRunBot(
        application_id=10,
        database=database,
        guilds=[guild],
    )
    await database.open()
    thread = DryRunThread(id=40, name="empty", channel=channel)
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        dry_run=True,
    )
    harness._bot = bot

    class EmptyOutboxReducer:
        def __init__(self, database, artifact_root=None) -> None:
            self.database = database

        async def persist(self, events) -> int:
            for index in range(2):
                await self.database.execute(
                    """
                    INSERT INTO event_journal(
                        sdk_session_id, generation, inbox_seq, source,
                        persistence_class, raw_type, reducer_hash,
                        raw_payload, received_at
                    ) VALUES (?, 0, ?, 'internal', 'internal', 'probe', ?, '{}', 0)
                    """,
                    (
                        events[0].sdk_session_id,
                        index + 1,
                        f"empty-{index}",
                    ),
                )
            return 2

    monkeypatch.setattr(harness_module, "JournalReducer", EmptyOutboxReducer)
    try:
        with pytest.raises(DiscordE2EError, match="three known render intents"):
            await harness._run_production_pipeline_probe(tmp_path, thread)
    finally:
        await bot.close()


@pytest.mark.asyncio
async def test_run_aggregates_original_cleanup_restore_and_write_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "agg.sqlite3")
    channel = DryRunChannel(id=30)
    guild = DryRunGuild(id=20, channel=channel)
    tree = DryRunTree(
        snapshot=[
            {
                "name": "existing",
                "type": 1,
                "description": "old",
                "options": [],
                "contexts": [],
                "integration_types": [],
            }
        ]
    )
    http = DryRunHttp()

    async def failing_restore(*args, **kwargs):
        raise RuntimeError("restore failed")

    http.bulk_upsert_guild_commands = failing_restore  # type: ignore[assignment]

    async def fake_render_plan(*args, **kwargs):
        embeds = (
            ({"title": "E2E task"},)
            if args and isinstance(args[0], dict) and args[0].get("type") == "taskdeck"
            else ()
        )
        return SimpleNamespace(
            batches=[
                SimpleNamespace(
                    content="rendered content",
                    embeds=embeds,
                    assets=[
                        SimpleNamespace(
                            filename="local-image.png",
                            fp=io.BytesIO(b"image-bytes"),
                        )
                    ],
                )
            ]
        )

    def fake_files(assets):
        return list(assets)

    def fake_taskdeck_view(payload):
        return SimpleNamespace(payload=payload)

    monkeypatch.setattr(harness_module, "_discord_render_plan", fake_render_plan)
    monkeypatch.setattr(harness_module, "_discord_files", fake_files)
    monkeypatch.setattr(harness_module, "_taskdeck_view", fake_taskdeck_view)

    def bot_factory(settings):
        bot = DryRunBot(
            application_id=10,
            database=database,
            guilds=[guild],
            tree=tree,
            http=http,
        )

        async def close() -> None:
            raise RuntimeError("close failed")

        bot.close = close  # type: ignore[assignment]
        return bot

    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        dry_run=True,
        bot_factory=bot_factory,
    )

    async def execute_ready_pipeline(bot, root):
        harness._guild_object = discord.Object(id=20)
        harness._original_manifest = [
            {
                "name": "existing",
                "type": 1,
                "description": "old",
                "options": [],
                "contexts": [],
                "integration_types": [],
            }
        ]
        raise RuntimeError("original failure")

    monkeypatch.setattr(harness, "_execute_ready_pipeline", execute_ready_pipeline)

    real_write = harness_module.write_evidence

    def flaky_write(path: Path, evidence) -> None:
        real_write(path, evidence)
        raise RuntimeError("write failed")

    monkeypatch.setattr(harness_module, "write_evidence", flaky_write)

    try:
        with pytest.raises(Exception) as exc_info:
            await harness.run()

        error = exc_info.value
        collected = getattr(error, "exceptions", getattr(error, "errors", [error]))
        messages = " | ".join(str(item) for item in collected)
        assert "original failure" in messages
        assert "restore manifest" in messages
        assert "close failed" in messages
        assert "write failed" in messages
        assert (tmp_path / "evidence.json").is_file()
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_run_preserves_cancellation_with_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "cancel.sqlite3")
    channel = DryRunChannel(id=30)
    guild = DryRunGuild(id=20, channel=channel)
    http = DryRunHttp()

    async def failing_restore(*args, **kwargs):
        raise RuntimeError("restore after cancellation failed")

    http.bulk_upsert_guild_commands = failing_restore  # type: ignore[assignment]

    def bot_factory(settings):
        return DryRunBot(
            application_id=10,
            database=database,
            guilds=[guild],
            tree=DryRunTree(),
            http=http,
        )

    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "cancel-evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        dry_run=True,
        bot_factory=bot_factory,
    )

    async def cancel_pipeline(bot, root):
        harness._guild_object = discord.Object(id=20)
        harness._original_manifest = [{"name": "existing", "type": 1}]
        raise asyncio.CancelledError("original cancellation")

    monkeypatch.setattr(harness, "_execute_ready_pipeline", cancel_pipeline)

    with pytest.raises(BaseExceptionGroup) as exc_info:
        await harness.run()

    errors = exc_info.value.exceptions
    assert any(isinstance(error, asyncio.CancelledError) for error in errors)
    assert any("restore after cancellation failed" in str(error) for error in errors)
    assert (tmp_path / "cancel-evidence.json").is_file()


@pytest.mark.asyncio
async def test_cancellation_during_cleanup_waits_for_manifest_and_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    class BlockingDeleteMessage(FakeMessage):
        async def delete(self) -> None:
            delete_started.set()
            await release_delete.wait()
            self.deleted = True

    database = Database(tmp_path / "cleanup-cancel.sqlite3")
    channel = DryRunChannel(id=30)
    guild = DryRunGuild(id=20, channel=channel)
    http = DryRunHttp()

    def bot_factory(settings):
        return DryRunBot(
            application_id=10,
            database=database,
            guilds=[guild],
            tree=DryRunTree(),
            http=http,
        )

    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "cleanup-cancel-evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
        dry_run=True,
        bot_factory=bot_factory,
    )
    message = BlockingDeleteMessage(id=700)

    async def successful_pipeline(bot, root):
        harness._guild_object = discord.Object(id=20)
        harness._original_manifest = [
            {
                "name": "existing",
                "type": 1,
                "description": "old",
                "options": [],
            }
        ]
        harness._created_messages.append(message)

    monkeypatch.setattr(harness, "_execute_ready_pipeline", successful_pipeline)
    run_task = asyncio.create_task(harness.run())
    await delete_started.wait()
    run_task.cancel()
    await asyncio.sleep(0)
    assert not run_task.done()

    release_delete.set()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert message.deleted is True
    assert http.restore_calls == [
        (
            10,
            20,
            [
                {
                    "name": "existing",
                    "type": 1,
                    "description": "old",
                    "options": [],
                }
            ],
        )
    ]
    assert harness._bot is not None and harness._bot.closed is True
    assert harness.evidence.cleaned_up is True
    assert (tmp_path / "cleanup-cancel-evidence.json").is_file()


def test_production_imports_do_not_install_direct_delivery_fallback() -> None:
    assert not hasattr(harness_module, "_PRODUCTION_PIPELINE_FALLBACK")


@pytest.mark.asyncio
async def test_rate_case_pends_without_actual_429_and_passes_only_on_probe(tmp_path: Path) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    burst = [SimpleNamespace(id=1), SimpleNamespace(id=2)]

    pending = harness._rate_limit_feature(burst)
    assert pending.status == "pending_not_observed"

    await harness._trace_request_end(
        None,
        None,
        SimpleNamespace(
            response=SimpleNamespace(
                status=429,
                headers={"Retry-After": "1.25"},
                url="https://discord.test",
            )
        ),
    )
    passed = harness._rate_limit_feature(burst)
    assert passed.status == "passed"
    assert "retry_after=1.25" in passed.detail


def test_human_driver_plan_has_actions_assertions_and_stable_ids(tmp_path: Path) -> None:
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    harness.evidence.guild_id = "guild"
    harness.evidence.channel_id = "channel"
    harness.evidence.thread_id = "thread"
    harness._command_paths = [
        "session list",
        "project mcp add",
        "Ask Copilot",
        "Pin message",
    ]
    harness._stable_ids = {
        "thread_name": "e2e-thread",
        "seed_message_id": "seed",
        "taskdeck_message_id": "taskdeck",
    }

    harness._record_interaction_coverage()

    pending = [
        feature for feature in harness.evidence.features if feature.status == "pending_human_driver"
    ]
    assert pending
    assert all(feature.ui_actions for feature in pending)
    assert all(feature.assertions for feature in pending)
    assert all(feature.stable_identifiers["thread_id"] == "thread" for feature in pending)
    slash_paths = {
        feature.stable_identifiers["command_path"]
        for feature in pending
        if "command_path" in feature.stable_identifiers
    }
    assert slash_paths == {"/session list", "/project mcp add"}
    assert all("blocked" not in feature.status for feature in pending)
