from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

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
        assert use_cached
        return self.content


@dataclass
class FakeMessage:
    id: int
    content: str = ""
    attachments: list[FakeAttachment] = field(default_factory=list)
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
    def __init__(self) -> None:
        self.restore_calls: list[tuple[int, int, list[dict[str, object]]]] = []

    async def bulk_upsert_guild_commands(
        self,
        application_id: int,
        guild_id: int,
        commands: list[dict[str, object]],
    ) -> None:
        self.restore_calls.append((application_id, guild_id, list(commands)))


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
        assert use_cached
        return self.content


@dataclass
class DryRunMessage:
    id: int
    content: str = ""
    attachments: list[DryRunAttachment] = field(default_factory=list)
    components: list[object] = field(default_factory=list)
    channel: DryRunChannel | None = None  # type: ignore[name-defined]
    author: object = field(default_factory=lambda: SimpleNamespace(id=777))
    deleted: bool = False

    async def delete(self) -> None:
        self.deleted = True

    async def pin(self, *, reason: str | None = None) -> None:
        if self.channel is not None:
            self.channel.pinned = self

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
    ) -> None:
        await super().bulk_upsert_guild_commands(application_id, guild_id, commands)


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
        return SimpleNamespace(
            batches=[
                SimpleNamespace(
                    content="rendered content",
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
        feature.feature == "production reducer/outbox exact delivery"
        for feature in evidence.features
    )
    assert any(feature.status == "pending_human_driver" for feature in evidence.features)
    persisted = json.loads((tmp_path / "evidence.json").read_text(encoding="utf-8"))
    assert persisted["guild_id"] == "20"
    assert persisted["channel_id"] == "30"


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
        return SimpleNamespace(
            batches=[
                SimpleNamespace(
                    content="rendered content",
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

    harness.record_http_response(status=429, retry_after=1.25, url="https://discord.test")
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
