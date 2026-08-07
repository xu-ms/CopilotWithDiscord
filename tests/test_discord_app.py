import asyncio
import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import discord
import pytest
from discord.ext import commands
from PIL import Image

from copilotd.config import Settings
from copilotd.core.bindings import BindingIntent, SessionBindingRepository
from copilotd.core.commands import CDConflictError, UnknownInteractionError
from copilotd.core.task_registry import TaskFailure
from copilotd.discord_app import (
    CopilotDiscordBot,
    DiscordInteractionResponder,
    _discord_render,
    _discord_render_plan,
    _prepare_discord_assets,
    _render_delivery_error,
    _render_view,
    _safe_stream_content,
    _taskdeck_view,
)
from copilotd.render.outbox import (
    RenderPermanentError,
    RenderRateLimited,
    RenderTransientError,
)
from copilotd.render.tables import TableAsset
from copilotd.sdk.capabilities import CapabilityRegistry
from copilotd.storage.database import Database


class _DiscordAttachmentFixture:
    id = 1
    filename = "recovery.txt"
    size = 9
    content_type = "text/plain"

    async def read(self, *, use_cached: bool = True) -> bytes:
        assert use_cached
        return b"recovered"


def test_discord_command_manifest_has_core_modes_and_no_deleted_roots(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    roots = {command.name for command in bot.tree.get_commands()}
    project = bot.tree.get_command("project")

    assert {
        "agent",
        "after",
        "ask",
        "every",
        "fleet",
        "session",
        "project",
        "model",
        "context",
        "usage",
        "autopilot",
        "plan",
        "steer",
        "remote",
        "research",
        "review",
        "rubber-duck",
        "security-review",
        "tasks",
        "queue",
        "ops",
        "Ask Copilot",
        "Pin message",
    } <= roots
    session = next(command for command in bot.tree.get_commands() if command.name == "session")
    session_commands = {command.name: command for command in session.commands}
    assert {"compact", "delete"} <= session_commands.keys()
    assert "fork" not in session_commands
    assert {parameter.name for parameter in session_commands["delete"].parameters} == {"session_id"}
    assert not session_commands["delete"].parameters[0].required
    expected_actions = {
        "agent": {"current", "list"},
        "after": {"cancel", "list"},
        "every": {"cancel", "list"},
        "remote": {"off", "status"},
        "tasks": {
            "all",
            "cancel",
            "list",
            "message",
            "promote",
            "progress",
            "remove",
            "show",
            "wait",
        },
    }
    for root, actions in expected_actions.items():
        command = next(item for item in bot.tree.get_commands() if item.name == root)
        action = next(parameter for parameter in command.parameters if parameter.name == "action")
        assert {choice.value for choice in action.choices} == actions
    assert (
        not {
            "copilot",
            "workflow",
            "max-turns",
            "mode",
            "goal",
            "bare",
            "tools",
            "cost",
            "budget",
            "limits",
            "pr",
            "delegate",
        }
        & roots
    )
    project = bot.tree.get_command("project")
    ops = bot.tree.get_command("ops")
    assert isinstance(project, discord.app_commands.Group)
    assert isinstance(ops, discord.app_commands.Group)
    assert {"bind", "info", "layout", "mention", "variable", "mcp", "skill", "plugin", "agent"} <= {
        command.name for command in project.commands
    }
    assert {
        "health",
        "scheduler",
        "diagnostics",
        "debug",
        "log-dump",
        "log-tail",
        "event-dump",
        "restart-runtime",
    } == {command.name for command in ops.commands}
    mcp = project.get_command("mcp")
    assert isinstance(mcp, discord.app_commands.Group)
    mcp_add = mcp.get_command("add")
    assert mcp_add is not None
    assert "project_env_refs" in {parameter.name for parameter in mcp_add.parameters}
    assert "config-reload" in {command.name for command in project.commands}
    worktree = project.get_command("worktree")
    assert isinstance(worktree, discord.app_commands.Group)
    worktree_create = worktree.get_command("create")
    assert worktree_create is not None
    history = next(
        parameter for parameter in worktree_create.parameters if parameter.name == "history"
    )
    assert {choice.value for choice in history.choices} == {"none"}


@pytest.mark.asyncio
async def test_every_registered_slash_command_routes_through_its_exact_shared_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    routed: list[str] = []

    async def run_command(
        _interaction: object,
        name: str,
        _operation: object,
    ) -> None:
        routed.append(name)

    monkeypatch.setattr(bot, "_run_command", run_command)
    interaction = SimpleNamespace(
        id=1,
        channel=object(),
        channel_id=1,
        user=SimpleNamespace(id=2),
    )

    def leaf_commands(
        commands_to_walk: list[object],
        *,
        prefix: str = "",
    ):
        for candidate in commands_to_walk:
            path = f"{prefix} {candidate.name}".strip()
            if isinstance(candidate, discord.app_commands.Group):
                yield from leaf_commands(candidate.commands, prefix=path)
            elif isinstance(candidate, discord.app_commands.Command):
                yield path, candidate

    def representative_value(parameter: object) -> object:
        choices = list(parameter.choices)
        if choices:
            return choices[0]
        if parameter.type is discord.AppCommandOptionType.boolean:
            return True
        if parameter.type is discord.AppCommandOptionType.integer:
            return 1
        if parameter.type is discord.AppCommandOptionType.number:
            return 1.0
        return f"{parameter.name}-value"

    commands = list(leaf_commands(bot.tree.get_commands()))
    context_menus = [
        candidate
        for candidate in bot.tree.get_commands()
        if isinstance(candidate, discord.app_commands.ContextMenu)
    ]
    assert len(commands) == 77
    assert {candidate.name for candidate in context_menus} == {
        "Ask Copilot",
        "Pin message",
    }
    for path, command in commands:
        arguments = {
            parameter.name: representative_value(parameter)
            for parameter in command.parameters
            if parameter.required
        }
        before = len(routed)
        await command.callback(interaction, **arguments)
        assert routed[before:] == [path], path

    target = SimpleNamespace()
    for command in context_menus:
        before = len(routed)
        await command.callback(interaction, target)
        assert routed[before:] == [command.name], command.name

    assert routed == [
        *[path for path, _command in commands],
        *[command.name for command in context_menus],
    ]


@pytest.mark.asyncio
async def test_ready_attachment_orphan_is_resubmitted_after_gateway_recovery(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    await bot.database.open()
    bindings = SessionBindingRepository(bot.database)
    binding = await bindings.create(
        thread_id="thread-attachment-recovery",
        sdk_session_id="session-attachment-recovery",
        cwd_snapshot=tmp_path,
        project_source="home",
    )
    bot.bindings = bindings
    sent: list[dict[str, Any]] = []

    class Runtime:
        async def send(self, prompt: str, **kwargs: Any) -> str:
            sent.append({"prompt": prompt, **kwargs})
            await bot.database.execute(
                """
                INSERT INTO submissions(
                    submission_id, sdk_session_id, origin,
                    attachment_manifest_id, state, created_at
                ) VALUES ('submission-attachment-recovery', ?,
                          'discord_message', ?, 'local_queued', 1)
                """,
                (
                    binding.sdk_session_id,
                    kwargs["attachment_manifest_id"],
                ),
            )
            return "accepted"

    runtime = Runtime()

    class Sessions:
        async def ensure_attached(self, candidate):
            assert candidate == binding
            return runtime

    bot.sessions = Sessions()  # type: ignore[assignment]
    prepared = await bot.attachment_service.prepare(
        source_kind="discord-message",
        source_id="message-ready-orphan",
        session_id=binding.sdk_session_id,
        attachments=[_DiscordAttachmentFixture()],
        source_channel_id="100",
        source_message_id="200",
        recovery_prompt="recover this message",
        recovery_idempotency_key="discord-message:200",
        recovery_origin="discord_message",
    )
    assert prepared is not None

    await bot._recover_attachment_manifests()

    assert sent[0]["prompt"] == "recover this message"
    assert sent[0]["idempotency_key"] == "discord-message:200"
    assert sent[0]["attachment_manifest_id"] == prepared.manifest_id
    assert await bot.attachment_service.pending_recoveries() == ()
    await bot.database.close()


class _SummaryCapability:
    def supports_reasoning_summary(self, model_id: str) -> bool:
        return model_id == "gpt-test"

    async def read_current_model(self, *, session_id: str) -> dict[str, Any]:
        del session_id
        return {"reasoningSummary": "concise"}


def test_model_reasoning_summary_option_is_capability_injected(tmp_path: Path) -> None:
    unsupported = CopilotDiscordBot(Settings(data_dir=tmp_path / "unsupported"))
    unsupported._register_application_commands()
    unsupported_model = unsupported.tree.get_command("model")
    assert isinstance(unsupported_model, discord.app_commands.Group)
    unsupported_set = unsupported_model.get_command("set")
    assert unsupported_set is not None
    assert "reasoning_summary" not in {parameter.name for parameter in unsupported_set.parameters}

    supported = CopilotDiscordBot(
        Settings(data_dir=tmp_path / "supported"),
        model_summary_adapter=_SummaryCapability(),
    )
    supported._register_application_commands()
    supported_model = supported.tree.get_command("model")
    assert isinstance(supported_model, discord.app_commands.Group)
    supported_set = supported_model.get_command("set")
    assert supported_set is not None
    assert "reasoning_summary" in {parameter.name for parameter in supported_set.parameters}


@pytest.mark.asyncio
async def test_session_delete_uses_thread_binding_before_optional_session_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeThread:
        def __init__(self, thread_id: int) -> None:
            self.id = thread_id

    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    session = bot.tree.get_command("session")
    assert isinstance(session, discord.app_commands.Group)
    command = session.get_command("delete")
    assert command is not None
    binding = SimpleNamespace(
        thread_id="123",
        sdk_session_id="stable-session-id",
    )
    bindings = SimpleNamespace(
        by_thread=AsyncMock(return_value=binding),
        by_session=AsyncMock(),
    )
    deletions = SimpleNamespace(delete=AsyncMock(return_value=BindingIntent.DELETED))
    bot.bindings = bindings
    bot.deletions = deletions
    results: list[str] = []

    async def run_command(
        _interaction: object,
        _name: str,
        operation: object,
    ) -> None:
        results.append(await operation(SimpleNamespace()))

    monkeypatch.setattr(discord, "Thread", FakeThread)
    monkeypatch.setattr(bot, "_run_command", run_command)
    interaction = SimpleNamespace(channel=FakeThread(123), id=456)

    await command.callback(interaction, None)

    bindings.by_thread.assert_awaited_once_with("123")
    bindings.by_session.assert_not_awaited()
    deletions.delete.assert_awaited_once_with(
        binding,
        idempotency_key="interaction:456",
    )
    assert results == ["Session permanently deleted."]

    with pytest.raises(CDConflictError, match="cannot delete another"):
        await command.callback(interaction, "different-session-id")


def test_discord_registration_omits_commands_without_capability_evidence(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    manifest = CapabilityRegistry(bot.settings).load_checked()
    capabilities = dict(manifest.capabilities)
    for capability in (
        "agents_current",
        "agents_list",
        "builtin_research",
        "builtin_review",
        "builtin_rubber_duck",
        "builtin_security_review",
        "context_info",
        "ephemeral_query",
        "fleet_start",
        "model_config",
        "models",
        "remote_disable",
        "remote_enable",
        "remote_status",
        "schedules_list",
        "schedules_stop",
        "session_mode",
        "tasks_list",
        "usage",
    ):
        capabilities[capability] = replace(
            capabilities[capability],
            supported=False,
        )
    bot.capabilities = replace(manifest, capabilities=capabilities)

    bot._register_application_commands()
    roots = {command.name for command in bot.tree.get_commands()}

    assert roots == {
        "Ask Copilot",
        "Pin message",
        "ops",
        "project",
        "queue",
        "schedule",
        "session",
        "steer",
    }


def test_native_discord_adapter_registers_only_exact_supported_actions(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    manifest = CapabilityRegistry(bot.settings).load_checked()
    capabilities = dict(manifest.capabilities)
    supported = {
        "agents_current",
        "agents_deselect",
        "agents_list",
        "agents_select",
        "builtin_after",
        "builtin_every",
        "builtin_research",
        "builtin_review",
        "builtin_rubber_duck",
        "builtin_security_review",
        "commands_invoke",
        "commands_list",
        "ephemeral_query",
        "fleet_start",
        "history_compact",
        "remote_disable",
        "remote_enable",
        "remote_status",
        "schedules_list",
        "schedules_stop",
        "tasks_cancel",
        "tasks_list",
        "tasks_message",
        "tasks_progress",
        "tasks_promote",
        "tasks_remove",
        "tasks_wait",
    }
    for name in supported:
        capabilities[name] = replace(capabilities[name], supported=True)
    bot.capabilities = replace(manifest, capabilities=capabilities)

    bot._register_application_commands()

    roots = {command.name for command in bot.tree.get_commands()}
    assert {
        "agent",
        "after",
        "ask",
        "every",
        "fleet",
        "remote",
        "research",
        "review",
        "rubber-duck",
        "security-review",
        "tasks",
    } <= roots
    session = next(command for command in bot.tree.get_commands() if command.name == "session")
    assert "compact" in {command.name for command in session.commands}


@pytest.mark.asyncio
async def test_native_and_direct_command_handlers_use_shared_invocation_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    routed: list[str] = []

    async def run_command(
        _interaction: object,
        name: str,
        _operation: object,
    ) -> None:
        routed.append(name)

    monkeypatch.setattr(bot, "_run_command", run_command)
    interaction = SimpleNamespace(id=1, channel=object(), channel_id=1)
    project = bot.tree.get_command("project")
    schedule = bot.tree.get_command("schedule")
    ops = bot.tree.get_command("ops")
    assert isinstance(project, discord.app_commands.Group)
    assert isinstance(schedule, discord.app_commands.Group)
    assert isinstance(ops, discord.app_commands.Group)
    worktree = project.get_command("worktree")
    assert isinstance(worktree, discord.app_commands.Group)
    ask = bot.tree.get_command("ask")
    timezone = project.get_command("timezone")
    config_reload = project.get_command("config-reload")
    worktree_list = worktree.get_command("list")
    schedule_list = schedule.get_command("list")
    scheduler = ops.get_command("scheduler")
    assert all(
        command is not None
        for command in (
            ask,
            timezone,
            config_reload,
            worktree_list,
            schedule_list,
            scheduler,
        )
    )

    await ask.callback(interaction, "question")
    await timezone.callback(interaction, "UTC")
    await config_reload.callback(interaction)
    await worktree_list.callback(interaction)
    await schedule_list.callback(interaction)
    await scheduler.callback(interaction)

    assert routed == [
        "ask",
        "project timezone",
        "project config-reload",
        "project worktree list",
        "schedule list",
        "ops scheduler",
    ]


@pytest.mark.asyncio
async def test_variable_unset_and_remove_share_protected_lifecycle_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    bot.projects = SimpleNamespace(
        resolve=AsyncMock(return_value=SimpleNamespace(project_id="project-1")),
        remove_project_env=AsyncMock(side_effect=AssertionError("weaker path used")),
    )
    bot.project_commands = SimpleNamespace(variable_remove=AsyncMock(return_value=9))
    results: list[str] = []

    async def run_command(
        _interaction: object,
        _name: str,
        operation: object,
    ) -> None:
        results.append(await operation(SimpleNamespace()))

    monkeypatch.setattr(bot, "_run_command", run_command)
    interaction = SimpleNamespace(channel=object(), channel_id=42)
    project = bot.tree.get_command("project")
    assert isinstance(project, discord.app_commands.Group)
    variable = project.get_command("variable")
    assert isinstance(variable, discord.app_commands.Group)
    remove = variable.get_command("remove")
    unset = variable.get_command("unset")
    assert remove is not None and unset is not None

    await remove.callback(interaction, "TOKEN")
    await unset.callback(interaction, "TOKEN")

    assert bot.project_commands.variable_remove.await_args_list == [
        ((("project-1", "TOKEN")),),
        ((("project-1", "TOKEN")),),
    ]
    bot.projects.remove_project_env.assert_not_awaited()
    assert results == [
        "Variable `TOKEN` removed in project config version `9`.",
        "Variable `TOKEN` removed in project config version `9`.",
    ]


def test_model_group_omits_set_when_mutation_is_not_verified(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    manifest = CapabilityRegistry(bot.settings).load_checked()
    capabilities = dict(manifest.capabilities)
    capabilities["model_config"] = replace(
        capabilities["model_config"],
        supported=False,
    )
    bot.capabilities = replace(manifest, capabilities=capabilities)

    bot._register_application_commands()

    model = next(command for command in bot.tree.get_commands() if command.name == "model")
    assert {command.name for command in model.commands} == {"list"}


def test_single_user_commands_have_no_operator_allowlist(
    tmp_path: Path,
) -> None:
    settings = Settings(data_dir=tmp_path)
    bot = CopilotDiscordBot(settings)
    bot._register_application_commands()

    assert "discord_operator_ids" not in type(settings).model_fields
    assert not hasattr(bot, "_require_operator")
    assert type(bot.tree.get_command("project")) is discord.app_commands.Group


@pytest.mark.asyncio
async def test_bot_teardown_is_idempotent(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.bridge.stop = AsyncMock()
    bot.database.close = AsyncMock()

    await bot.close()
    await bot.close()

    bot.bridge.stop.assert_awaited_once()
    bot.database.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_closes_gateway_and_registry_then_drains_admitted_handlers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    admission_closed = asyncio.Event()

    async def gateway_close(_bot: commands.Bot) -> None:
        order.append("gateway_closed")

    class FakeSessions:
        async def close_admission(self) -> None:
            order.append("registry_closed")
            admission_closed.set()

        async def shutdown(self, *, emergency_session_id: str | None = None) -> None:
            del emergency_session_id
            order.append("sessions_snapshotted")

    async def admitted_handler(_message: object) -> None:
        handler_started.set()
        await release_handler.wait()
        order.append("handler_drained")

    monkeypatch.setattr(commands.Bot, "close", gateway_close)
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.sessions = FakeSessions()
    bot.bridge.stop = AsyncMock(side_effect=lambda: order.append("bridge_stopped"))
    bot.database.close = AsyncMock(side_effect=lambda: order.append("database_closed"))
    monkeypatch.setattr(bot, "_on_message_admitted", admitted_handler)
    handler = asyncio.create_task(bot.on_message(object()))
    await handler_started.wait()

    closing = asyncio.create_task(bot.close())
    await admission_closed.wait()
    assert not closing.done()
    assert order[:2] == ["gateway_closed", "registry_closed"]

    release_handler.set()
    await handler
    await closing

    assert order.index("handler_drained") < order.index("sessions_snapshotted")
    assert order.index("sessions_snapshotted") < order.index("bridge_stopped")


@pytest.mark.asyncio
async def test_ordinary_message_does_not_resume_closed_session_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeThread:
        def __init__(self, thread_id: str) -> None:
            self.id = thread_id

    async with Database(tmp_path / "closed-message.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-1",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent'
            WHERE thread_id = 'thread-1'
            """
        )
        bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
        bot.bindings = bindings
        bot.sessions = SimpleNamespace(
            for_thread=lambda _thread_id: pytest.fail(
                "ordinary ingress must not instantiate a closed runtime"
            )
        )
        monkeypatch.setattr(discord, "Thread", FakeThread)
        monkeypatch.setattr(
            bot,
            "_is_restart_draining",
            AsyncMock(return_value=False),
        )
        message = SimpleNamespace(
            author=SimpleNamespace(bot=False),
            guild=object(),
            channel=FakeThread("thread-1"),
            type=discord.MessageType.default,
            content="do not resume implicitly",
            attachments=[],
            mentions=[],
            reply=AsyncMock(),
        )

        await bot.on_message(message)

    message.reply.assert_awaited_once()
    assert "closed" in message.reply.await_args.args[0]
    assert "/session resume" in message.reply.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_type",
    [
        discord.MessageType.thread_starter_message,
        discord.MessageType.pins_add,
        discord.MessageType.recipient_add,
        discord.MessageType.call,
        discord.MessageType.auto_moderation_action,
    ],
)
async def test_system_message_types_are_ignored_before_session_lookup(
    tmp_path: Path,
    message_type: discord.MessageType,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.bindings = SimpleNamespace(
        by_thread=AsyncMock(side_effect=AssertionError("must not look up a session"))
    )
    bot.sessions = SimpleNamespace(
        ensure_attached=AsyncMock(side_effect=AssertionError("must not attach"))
    )
    message = SimpleNamespace(type=message_type)

    await bot.on_message(message)

    bot.bindings.by_thread.assert_not_awaited()
    bot.sessions.ensure_attached.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_type",
    [discord.MessageType.default, discord.MessageType.reply],
)
async def test_real_prompt_types_submit_once_without_thread_starter_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    message_type: discord.MessageType,
) -> None:
    class FakeThread:
        def __init__(self, thread_id: int) -> None:
            self.id = thread_id

    binding = SimpleNamespace(binding_intent=BindingIntent.ACTIVE, sdk_session_id="session-1")
    runtime = SimpleNamespace(binding=binding, send=AsyncMock())
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.bindings = SimpleNamespace(by_thread=AsyncMock(return_value=binding))
    bot.sessions = SimpleNamespace(ensure_attached=AsyncMock(return_value=runtime))
    bot.attachment_service.prepare = AsyncMock(return_value=None)
    monkeypatch.setattr(discord, "Thread", FakeThread)
    monkeypatch.setattr(bot, "_is_restart_draining", AsyncMock(return_value=False))
    starter = SimpleNamespace(type=discord.MessageType.thread_starter_message)
    prompt = SimpleNamespace(
        id=71,
        type=message_type,
        author=SimpleNamespace(bot=False),
        guild=object(),
        channel=FakeThread(10),
        content="hello",
        attachments=[],
        mentions=[],
        reply=AsyncMock(),
    )

    await bot.on_message(starter)
    await bot.on_message(prompt)

    runtime.send.assert_awaited_once_with(
        "hello",
        idempotency_key="discord-message:71",
        attachments=None,
        attachment_manifest_id=None,
    )


@pytest.mark.asyncio
async def test_cancelled_close_caller_does_not_poison_shared_teardown(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    stop_started = asyncio.Event()
    allow_stop = asyncio.Event()

    async def delayed_stop() -> None:
        stop_started.set()
        await allow_stop.wait()

    bot.bridge.stop = AsyncMock(side_effect=delayed_stop)
    bot.database.close = AsyncMock()
    first = asyncio.create_task(bot.close())
    await stop_started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    allow_stop.set()

    await bot.close()

    assert bot._closed_once
    bot.bridge.stop.assert_awaited_once()
    bot.database.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fatal_worker_runs_full_application_teardown(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.bridge.stop = AsyncMock()
    bot.database.close = AsyncMock()
    supervisor = bot._tasks.create(
        bot._task_failure_loop(),
        name="test-failure-supervisor",
    )
    await bot._tasks.errors.put(
        TaskFailure(
            name="failed-worker",
            source="test",
            session_id=None,
            runtime_generation=None,
            error=RuntimeError("worker failed"),
        )
    )

    await asyncio.wait_for(supervisor, timeout=2)

    assert bot._closed_once
    bot.bridge.stop.assert_awaited_once()
    bot.database.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_fatal_diagnostic_failure_still_runs_teardown(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot.bridge.stop = AsyncMock()
    bot.database.execute = AsyncMock(side_effect=OSError("database unavailable"))
    bot.database.close = AsyncMock()
    supervisor = bot._tasks.create(
        bot._task_failure_loop(),
        name="test-diagnostic-failure-supervisor",
    )
    await bot._tasks.errors.put(
        TaskFailure(
            name="failed-reducer",
            source="event-reducer",
            session_id="session-1",
            runtime_generation=1,
            error=RuntimeError("reducer failed"),
        )
    )

    await asyncio.wait_for(supervisor, timeout=2)

    assert isinstance(bot._fatal_diagnostic_error, OSError)
    assert bot._closed_once
    bot.bridge.stop.assert_awaited_once()
    bot.database.close.assert_awaited_once()


def test_streaming_table_is_held_before_discord_edit() -> None:
    content = "before\n\n| A | B |\n| --- | --- |\n| 1 | partial"

    rendered = _safe_stream_content(content)

    assert "before" in rendered
    assert "rendering table" in rendered
    assert "| A | B |" not in rendered
    assert "partial" not in rendered


@pytest.mark.asyncio
async def test_final_discord_render_keeps_table_copyable() -> None:
    content = """
before

| A | B |
| --- | ---: |
| alpha | 1 |

after
""".strip()

    rendered, assets = await _discord_render({"content": content, "finalized": True})

    assert "before" in rendered
    assert "alpha" in rendered
    assert "after" in rendered
    assert assets == []


@pytest.mark.asyncio
async def test_discord_render_preserves_explicit_text_artifact() -> None:
    rendered, assets = await _discord_render(
        {
            "content": "Tool output attached.",
            "finalized": True,
            "attachments": [
                {
                    "filename": "tool-output.txt",
                    "media_type": "text/plain",
                    "content": "verbatim output",
                }
            ],
        }
    )

    assert rendered == "Tool output attached."
    assert len(assets) == 1
    assert assets[0].filename == "tool-output.txt"
    assert assets[0].content == b"verbatim output"


@pytest.mark.asyncio
async def test_discord_render_materializes_verified_artifact_reference(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "spill.txt"
    content = b"append-only spill"
    artifact.write_bytes(content)

    rendered, assets = await _discord_render(
        {
            "content": "Tool spill attached.",
            "finalized": True,
            "attachments": [
                {
                    "filename": "spill.txt",
                    "media_type": "text/plain",
                    "path": str(artifact),
                    "byte_size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            ],
        }
    )

    assert rendered == "Tool spill attached."
    assert assets[0].content == content


@pytest.mark.asyncio
async def test_local_image_warning_flood_stays_within_discord_limit(
    tmp_path: Path,
) -> None:
    content = "\n".join(f"![missing-{index}](missing-{index}.png)" for index in range(80))

    plan = await _discord_render_plan(
        {
            "content": content,
            "finalized": True,
            "trusted_local_images": True,
            "trusted_local_image_paths": [f"missing-{index}.png" for index in range(80)],
            "trusted_local_image_artifacts": [
                {
                    "source_path": f"missing-{index}.png",
                    "snapshot_path": str(tmp_path / f"snapshot-{index}.png"),
                    "byte_size": 0,
                    "sha256": hashlib.sha256(b"").hexdigest(),
                }
                for index in range(80)
            ],
        },
        allowed_roots=(tmp_path,),
    )

    assert len(plan.batches) > 1
    assert all(len(batch.content) <= 1850 for batch in plan.batches)
    assert any("image path" in batch.content for batch in plan.batches)


@pytest.mark.asyncio
async def test_assistant_markdown_cannot_dereference_local_image_without_trust(
    tmp_path: Path,
) -> None:
    local = tmp_path / "private.png"
    await asyncio.to_thread(local.write_bytes, b"local private bytes")
    source = f"Do not upload ![private]({local.name})"

    plan = await _discord_render_plan(
        {"content": source, "finalized": True},
        allowed_roots=(tmp_path,),
    )

    assert all(not batch.assets for batch in plan.batches)
    assert "![private]" in plan.batches[0].content


@pytest.mark.asyncio
async def test_verified_relative_assistant_image_is_uploaded(
    tmp_path: Path,
) -> None:
    local = tmp_path / "artifacts" / "chart.png"
    local.parent.mkdir()
    Image.new("RGB", (4, 4), "green").save(local)
    snapshot = tmp_path / "snapshot.png"
    snapshot.write_bytes(local.read_bytes())
    snapshot_content = snapshot.read_bytes()

    plan = await _discord_render_plan(
        {
            "content": "Result: ![chart](artifacts/chart.png)",
            "finalized": True,
            "trusted_local_images": True,
            "trusted_local_image_paths": ["artifacts/chart.png"],
            "trusted_local_image_artifacts": [
                {
                    "source_path": "artifacts/chart.png",
                    "snapshot_path": str(snapshot),
                    "byte_size": len(snapshot_content),
                    "sha256": hashlib.sha256(snapshot_content).hexdigest(),
                }
            ],
        },
        allowed_roots=(tmp_path,),
    )

    assert [asset.filename for batch in plan.batches for asset in batch.assets] == ["chart.png"]
    assert all("![chart]" not in batch.content for batch in plan.batches)


@pytest.mark.asyncio
async def test_assistant_stream_and_final_render_as_plain_message_content() -> None:
    stream = await _discord_render_plan(
        {
            "type": "assistant.message_delta",
            "content": "Working on **three checks**…",
            "finalized": False,
        }
    )
    final = await _discord_render_plan(
        {
            "type": "assistant.message",
            "content": "All checks passed.\n\n```text\n3/3\n```",
            "finalized": True,
        }
    )

    assert stream.batches[0].content == "Working on **three checks**…"
    assert stream.batches[0].embeds == ()
    assert "All checks passed." in final.batches[0].content
    assert "```text" in final.batches[0].content
    assert final.batches[0].embeds == ()


@pytest.mark.asyncio
async def test_structured_events_render_as_distinct_rich_cards() -> None:
    cases = [
        (
            {
                "type": "session.warning",
                "content": "**Copilot warning**\nContext is nearly full.",
                "status": {
                    "title": "Copilot warning",
                    "detail": "Context is nearly full.",
                    "event_type": "session.warning",
                },
                "finalized": True,
            },
            "⚠️ Copilot warning",
        ),
        (
            {
                "type": "interaction",
                "content": "**Copilot needs input**",
                "interaction": {
                    "kind": "user_input",
                    "state": "pending",
                    "question": "Pick a deployment target.",
                },
                "finalized": False,
            },
            "📝 Copilot needs input",
        ),
        (
            {
                "type": "interaction",
                "content": "**Copilot input expired**",
                "interaction": {
                    "kind": "user_input",
                    "state": "expired",
                    "question": "Pick a deployment target.",
                },
                "finalized": True,
            },
            "⏳ Copilot input expired",
        ),
        (
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
            "⚠️ Task blocked",
        ),
        (
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
            "▶️ Task continuing",
        ),
    ]

    for payload, title in cases:
        plan = await _discord_render_plan(payload)
        assert plan.batches[0].content == ""
        assert plan.batches[0].embeds[0]["title"] == title
        assert len(discord.Embed.from_dict(plan.batches[0].embeds[0])) <= 6000

    expired = (await _discord_render_plan(cases[2][0])).batches[0].embeds[0]
    assert expired["footer"]["text"] == "No response was sent"
    blocked = (await _discord_render_plan(cases[3][0])).batches[0].embeds[0]
    assert blocked["color"] == 0xFEE75C
    continuing = (await _discord_render_plan(cases[4][0])).batches[0].embeds[0]
    assert continuing["color"] == 0x5865F2


@pytest.mark.asyncio
async def test_usage_and_turn_summary_render_as_compact_subtext() -> None:
    usage = await _discord_render_plan(
        {
            "type": "session.usage_info",
            "content": "**Copilot usage**",
            "usage": {
                "inputTokens": 12,
                "outputTokens": 7,
                "totalTokens": 19,
                "currentTokens": 50,
                "tokenLimit": 100,
            },
            "finalized": True,
        }
    )
    footer = await _discord_render_plan(
        {
            "type": "idle_footer",
            "content": "turn complete",
            "model": "gpt-test",
            "input_tokens": 1200,
            "output_tokens": 340,
            "credits": 1.5,
            "context": "50/100",
            "duration_seconds": 65,
            "background_observed": False,
            "finalized": True,
        }
    )

    assert usage.batches[0].content == "-# 📥 12 │ 📤 7 │ Σ 19 │ 🧩 50/100"
    assert usage.batches[0].embeds == ()
    assert footer.batches[0].content == (
        "-# ✅ 🧠 gpt-test │ 📥 1,200 │ 📤 340 │ ⏱ 1m 5s │ 🧩 50/100 │ ✨ 1.5"
    )
    assert footer.batches[0].embeds == ()


@pytest.mark.asyncio
async def test_diff_tool_artifact_and_images_use_metadata_and_attachment_previews() -> None:
    image = TableAsset(
        filename="table-preview.png",
        media_type="image/png",
        content=b"png",
    )
    diff = await _discord_render_plan(
        {
            "type": "diff",
            "content": "**Code changes** · `structured`\n```diff\n+added\n-removed\n```",
            "source": "structured",
            "byte_count": 128,
            "stats": {"files": 1, "additions": 4, "deletions": 2},
            "attachments": [
                {
                    "filename": image.filename,
                    "media_type": image.media_type,
                    "content": image.content,
                }
            ],
            "finalized": True,
        }
    )
    artifact = await _discord_render_plan(
        {
            "type": "tool_output_artifact",
            "content": "**Tool completed** — exact output attached.",
            "status": "completed",
            "tool_source": "detailedContent",
            "verbatim": True,
            "character_count": 9000,
            "line_count": 120,
            "attachments": [
                {
                    "filename": "tool-output.txt",
                    "media_type": "text/plain",
                    "content": "exact",
                }
            ],
            "finalized": True,
        }
    )

    assert diff.batches[0].content == ""
    assert [embed["title"] for embed in diff.batches[0].embeds] == [
        "🧩 Code changes",
        "📊 Table preview",
    ]
    assert diff.batches[0].embeds[1]["image"]["url"] == "attachment://table-preview.png"
    assert {field["name"] for field in diff.batches[0].embeds[0]["fields"]} >= {
        "Files",
        "Changes",
        "Attachments",
    }
    assert artifact.batches[0].content == ""
    assert artifact.batches[0].embeds[0]["title"] == "📎 Tool output"
    assert any(
        field["value"] == "`9,000` chars · `120` lines"
        for field in artifact.batches[0].embeds[0]["fields"]
    )


@pytest.mark.asyncio
async def test_oversized_diff_and_legacy_tool_artifact_do_not_claim_success() -> None:
    oversized = await _discord_render_plan(
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
            "attachments": [],
            "finalized": True,
        }
    )
    legacy_failure = await _discord_render_plan(
        {
            "type": "tool_output_artifact",
            "content": "**Tool failed** — exact output attached.",
            "tool_source": "error",
            "verbatim": True,
            "attachments": [
                {
                    "filename": "legacy-error.txt",
                    "media_type": "text/plain",
                    "content": "failed",
                }
            ],
            "finalized": True,
        }
    )

    diff_embed = oversized.batches[0].embeds[0]
    diff_fields = {field["name"]: field["value"] for field in diff_embed["fields"]}
    assert diff_embed["color"] == 0xFEE75C
    assert diff_fields["Files"] == "`unknown`"
    assert diff_fields["Changes"] == "`unknown`"
    assert diff_fields["Delivery"] == "`omitted: render safety limit`"
    assert "durable event journal" in diff_embed["footer"]["text"]
    legacy_embed = legacy_failure.batches[0].embeds[0]
    assert legacy_embed["title"] == "❌ Tool output"
    assert legacy_embed["color"] == 0xED4245


@pytest.mark.asyncio
async def test_image_preview_gallery_respects_discord_count_and_character_budgets() -> None:
    plan = await _discord_render_plan(
        {
            "type": "assistant.message",
            "content": "Image results",
            "attachments": [
                {
                    "filename": f"image-{index}.png",
                    "media_type": "image/png",
                    "content": b"image",
                }
                for index in range(10)
            ],
            "finalized": True,
        }
    )

    embeds = plan.batches[0].embeds
    assert len(embeds) == 10
    assert embeds[0]["title"] == "🖼️ Image preview · 1/10"
    assert sum(len(discord.Embed.from_dict(item)) for item in embeds) <= 6000


@pytest.mark.asyncio
async def test_image_preview_filenames_are_safe_and_unique_per_discord_message() -> None:
    plan = await _discord_render_plan(
        {
            "type": "assistant.message",
            "content": "Two generated charts",
            "attachments": [
                {
                    "filename": "first/report chart.png",
                    "media_type": "image/png",
                    "content": b"first",
                },
                {
                    "filename": "second/report chart.png",
                    "media_type": "image/png",
                    "content": b"second",
                },
            ],
            "finalized": True,
        }
    )

    batch = plan.batches[0]
    assert [asset.filename for asset in batch.assets] == [
        "report-chart.png",
        "report-chart-2.png",
    ]
    assert [embed["image"]["url"] for embed in batch.embeds] == [
        "attachment://report-chart.png",
        "attachment://report-chart-2.png",
    ]


def test_large_discord_assets_are_split_losslessly_below_upload_limit() -> None:
    content, assets = _prepare_discord_assets(
        "Tool output attached.",
        [
            TableAsset(
                filename="tool-output.txt",
                media_type="text/plain",
                content=b"0123456789abcdef",
            )
        ],
        max_bytes=5,
    )

    assert "split into 4 upload-safe file(s)" in content
    assert [asset.filename for asset in assets] == [
        "tool-output.part-001-of-004.txt",
        "tool-output.part-002-of-004.txt",
        "tool-output.part-003-of-004.txt",
        "tool-output.part-004-of-004.txt",
    ]
    assert all(len(asset.content) <= 5 for asset in assets)
    assert b"".join(asset.content for asset in assets) == b"0123456789abcdef"


class _FakeDiscordResponse:
    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.reason = "test"
        self.headers = headers or {}


class _ExpiredInteractionResponse:
    def is_done(self) -> bool:
        return False

    async def defer(self, **_kwargs: Any) -> None:
        raise discord.NotFound(
            _FakeDiscordResponse(404),
            {"code": 10062, "message": "Unknown interaction"},
        )

    async def send_modal(self, _modal: discord.ui.Modal) -> None:
        raise discord.NotFound(
            _FakeDiscordResponse(404),
            {"code": 10062, "message": "Unknown interaction"},
        )


class _FallbackChannel:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, content: str, **_kwargs: Any) -> None:
        self.messages.append(content)


@pytest.mark.asyncio
async def test_component_and_modal_unknown_interaction_fall_back_in_thread(
    tmp_path: Path,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    channel = _FallbackChannel()
    interaction = SimpleNamespace(
        response=_ExpiredInteractionResponse(),
        followup=SimpleNamespace(),
        channel=channel,
    )
    responder = DiscordInteractionResponder(bot, interaction, name="component")

    with pytest.raises(UnknownInteractionError):
        await responder.defer()
    await responder.send_followup("durable component result")

    modal_responder = DiscordInteractionResponder(bot, interaction, name="modal")
    await modal_responder.send_modal(discord.ui.Modal(title="Test modal"))

    assert "durable component result" in channel.messages[0]
    assert "form opened" in channel.messages[1]


@pytest.mark.asyncio
async def test_direct_interaction_choice_defers_and_reports_runtime_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        def __init__(self) -> None:
            self.deferred = False

        def is_done(self) -> bool:
            return self.deferred

        async def defer(self, **_kwargs: Any) -> None:
            self.deferred = True

    class Followup:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send(self, content: str, **_kwargs: Any) -> None:
            self.messages.append(content)

    class Runtime:
        async def respond_interaction(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("runtime unavailable")

    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    response = Response()
    followup = Followup()
    interaction = SimpleNamespace(
        response=response,
        followup=followup,
        channel=_FallbackChannel(),
        data={"custom_id": "cdi:interaction-1:choice-0"},
    )
    monkeypatch.setattr(bot, "_interaction_runtime", AsyncMock(return_value=Runtime()))

    await bot._handle_direct_interaction(
        interaction,
        "cdi:interaction-1:choice-0",
    )

    assert response.deferred is True
    assert len(followup.messages) == 1
    assert "runtime unavailable" in followup.messages[0]


def test_discord_http_errors_map_to_outbox_delivery_classes() -> None:
    rate_limit = _render_delivery_error(
        discord.HTTPException(
            _FakeDiscordResponse(429, headers={"Retry-After": "2.5"}),
            "slow down",
        )
    )
    transient = _render_delivery_error(
        discord.HTTPException(_FakeDiscordResponse(503), "unavailable")
    )
    permanent = _render_delivery_error(
        discord.HTTPException(_FakeDiscordResponse(413), "too large")
    )

    assert isinstance(rate_limit, RenderRateLimited)
    assert rate_limit.retry_after == 2.5
    assert isinstance(transient, RenderTransientError)
    assert isinstance(permanent, RenderPermanentError)


def test_taskdeck_view_uses_short_in_place_controls() -> None:
    view = _taskdeck_view(
        {
            "taskdeck": {
                "panel_id": "panel-token",
                "revision": 12,
                "page": 0,
                "page_count": 2,
                "selected_card_token": "card-a",
                "expanded": False,
                "options": [{"label": "Worker A", "value": "card-a", "state": "running"}],
            }
        }
    )

    assert view is not None
    custom_ids = [item.custom_id for item in view.children]
    assert custom_ids == [
        "cdtd:panel-token:12:select",
        "cdtd:panel-token:12:toggle",
        "cdtd:panel-token:12:prev",
        "cdtd:panel-token:12:next",
    ]
    assert all(len(custom_id) < 100 for custom_id in custom_ids)


def test_taskdeck_view_normalizes_empty_option_text() -> None:
    view = _taskdeck_view(
        {
            "taskdeck": {
                "panel_id": "panel-token",
                "revision": 1,
                "page": 0,
                "page_count": 1,
                "selected_card_token": "card-a",
                "expanded": False,
                "options": [{"label": "", "value": "card-a", "state": ""}],
            }
        }
    )

    assert view is not None
    select = view.children[0]
    assert select.options[0].label == "Untitled task"
    assert select.options[0].description == "unknown"


@pytest.mark.asyncio
async def test_taskdeck_render_plan_uses_bounded_embeds_and_preserves_them_across_assets() -> None:
    cards = [
        {
            "card_token": f"card-{index}",
            "title": f"Worker {index}",
            "state": "running" if index == 0 else "idle",
            "kind": "agent",
            "elapsed": f"{index + 1}s",
            "progress_summary": f"Progress {index}",
            "detail_artifact": "full detail" if index == 0 else None,
            "dependencies": ["setup"] if index == 0 else [],
            "artifact_links": ["artifact.md"] if index == 0 else [],
        }
        for index in range(10)
    ]
    plan = await _discord_render_plan(
        {
            "type": "taskdeck",
            "content": "**TaskDeck** — 10 item(s)",
            "cards": cards,
            "attachments": [
                {
                    "filename": f"artifact-{index}.txt",
                    "media_type": "text/plain",
                    "content": str(index),
                }
                for index in range(11)
            ],
            "finalized": False,
            "taskdeck": {
                "page": 0,
                "selected_card_token": "card-0",
                "expanded": True,
            },
        }
    )

    assert len(plan.batches) == 2
    assert len(plan.batches[0].embeds) == 8
    assert plan.batches[1].embeds == ()
    assert [field["name"] for field in plan.batches[0].embeds[0]["fields"]] == [
        "State",
        "Type",
        "Elapsed",
        "Dependencies",
        "Artifacts",
    ]
    assert plan.batches[0].embeds[0]["footer"]["text"] == "Selected · expanded"
    assert "Full detail is attached" in plan.batches[0].embeds[0]["description"]
    assert sum(len(discord.Embed.from_dict(item)) for item in plan.batches[0].embeds) <= 6000
    assert len(plan.batches[0].assets) == 10
    assert len(plan.batches[1].assets) == 1


def test_interaction_view_uses_bounded_buttons_and_freeform_modal_button() -> None:
    interaction_id = "4ed74879-92fb-47c5-9ee9-81dde5079ab1"
    view = _render_view(
        {
            "interaction": {
                "interaction_id": interaction_id,
                "kind": "user_input",
                "state": "pending",
                "question": "Choose one",
                "choices": ["First answer", "Second answer"],
                "allowFreeform": True,
            }
        }
    )

    assert view is not None
    first, second, button = view.children
    assert first.custom_id == f"cdi:{interaction_id}:choice-0"
    assert second.custom_id == f"cdi:{interaction_id}:choice-1"
    assert button.custom_id == f"cdi:{interaction_id}:freeform"
    assert all(item.custom_id and len(item.custom_id) < 100 for item in view.children)


def test_resolved_interaction_removes_controls() -> None:
    assert (
        _render_view(
            {
                "interaction": {
                    "interaction_id": "resolved-id",
                    "kind": "user_input",
                    "state": "resolved",
                }
            }
        )
        is None
    )


def test_elicitation_view_exposes_bounded_form_decline_and_cancel_controls() -> None:
    interaction_id = "93f3e059-3ec9-45e6-80a5-2df3ea3ca42f"
    view = _render_view(
        {
            "interaction": {
                "interaction_id": interaction_id,
                "kind": "elicitation",
                "state": "pending",
                "form": {
                    "fields": [
                        {
                            "name": "label",
                            "value_type": "string",
                            "required": True,
                            "title": "Label",
                            "description": None,
                            "enum": [],
                            "default": None,
                            "minimum": None,
                            "maximum": None,
                            "min_length": 1,
                            "max_length": 20,
                            "min_items": None,
                            "max_items": None,
                            "item_enum": [],
                        }
                    ]
                },
            }
        }
    )

    assert view is not None
    assert [item.custom_id for item in view.children] == [
        f"cdi:{interaction_id}:form",
        f"cdi:{interaction_id}:decline",
        f"cdi:{interaction_id}:cancel",
    ]
    assert all(item.custom_id and len(item.custom_id) < 100 for item in view.children)


@pytest.mark.asyncio
async def test_critical_task_failure_closes_gateway_and_persists_incident(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    await bot.database.open()
    try:
        gateway_closed = asyncio.Event()

        async def fake_base_close(_bot: commands.Bot) -> None:
            gateway_closed.set()

        monkeypatch.setattr(commands.Bot, "close", fake_base_close)
        supervisor = asyncio.create_task(bot._task_failure_loop())

        async def fail() -> None:
            raise RuntimeError("reducer stopped")

        bot._tasks.create(
            fail(),
            name="reducer:session-1",
            source="event-reducer",
            session_id="session-1",
            runtime_generation=4,
        )
        await asyncio.wait_for(gateway_closed.wait(), timeout=1)
        await supervisor
        async with Database(bot.settings.database_path) as database:
            incident = await database.fetchone(
                """
                SELECT runtime_generation, kind, detail
                FROM runtime_incidents WHERE session_id = 'session-1'
                """
            )

        assert isinstance(bot._fatal_worker_error, RuntimeError)
        assert bot.heartbeat.runtime_state == "down"
        assert bot.heartbeat.gateway_state == "down"
        assert incident["runtime_generation"] == 4
        assert incident["kind"] == "background_task_failed"
        assert "event-reducer" in incident["detail"]
    finally:
        await bot.database.close()


@pytest.mark.asyncio
async def test_runtime_health_loop_marks_unexpected_runtime_loss_and_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = CopilotDiscordBot(
        Settings(
            _env_file=None,
            data_dir=tmp_path / "data",
            cache_dir=tmp_path / "cache",
            log_dir=tmp_path / "logs",
            heartbeat_interval_seconds=0.01,
        )
    )
    failed = asyncio.Event()
    recover = asyncio.Event()
    calls = 0

    async def healthcheck() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            failed.set()
            raise RuntimeError("sidecar disconnected")
        await recover.wait()

    monkeypatch.setattr(bot.bridge, "healthcheck", healthcheck)
    task = asyncio.create_task(bot._runtime_health_loop())
    try:
        await failed.wait()
        for _ in range(100):
            if calls >= 2:
                break
            await asyncio.sleep(0.005)
        assert calls >= 2
        assert bot.heartbeat.runtime_state == "down"
        recover.set()
        for _ in range(100):
            if bot.heartbeat.runtime_state == "ready":
                break
            await asyncio.sleep(0.005)
        assert bot.heartbeat.runtime_state == "ready"
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
