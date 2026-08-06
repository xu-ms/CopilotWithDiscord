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
from copilotd.core.bindings import SessionBindingRepository
from copilotd.core.commands import UnknownInteractionError
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
    assert "compact" in {command.name for command in session.commands}
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


def test_administrative_commands_require_explicit_operator_allowlist(
    tmp_path: Path,
) -> None:
    denied = CopilotDiscordBot(Settings(data_dir=tmp_path))
    interaction = SimpleNamespace(user=SimpleNamespace(id=42))
    with pytest.raises(discord.app_commands.CheckFailure):
        denied._require_operator(interaction)

    allowed = CopilotDiscordBot(Settings(data_dir=tmp_path, discord_operator_ids="41,42"))
    allowed._require_operator(interaction)


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
