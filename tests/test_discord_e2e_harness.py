from __future__ import annotations

import hashlib
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
    tree = FakeTree(snapshot=[{"id": 1, "name": "existing", "description": "old"}])
    http = FakeHttp()
    bot = FakeBot(application_id=10, guilds=[guild], tree=tree, http=http)
    harness = DiscordRealHarness(
        token="not-used",
        evidence_path=tmp_path / "evidence.json",
        guild_id=20,
        application_id=10,
        channel_id=30,
    )
    harness._bot = bot
    harness._guild_object = discord.Object(id=20)
    harness._original_manifest = await harness._snapshot_guild_manifest(bot)

    failures = await harness._cleanup()

    assert failures == []
    assert http.restore_calls == [(10, 20, [{"name": "existing", "description": "old"}])]
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

    assert failures == [
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
    assert hashlib.sha256(b"alpha").hexdigest() in evidence.assertions[1]
    assert hashlib.sha256(b"beta").hexdigest() in evidence.assertions[1]


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
