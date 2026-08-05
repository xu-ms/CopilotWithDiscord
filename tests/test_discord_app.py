import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import discord
import pytest
from PIL import Image

from copilotd.config import Settings
from copilotd.core.commands import UnknownInteractionError
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


def test_discord_command_manifest_has_core_modes_and_no_deleted_roots(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))
    bot._register_application_commands()
    roots = {command.name for command in bot.tree.get_commands()}

    assert {
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
    } <= roots
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
    assert {"health", "diagnostics", "debug", "log-tail", "event-dump"} == {
        command.name for command in ops.commands
    }
    mcp = project.get_command("mcp")
    assert isinstance(mcp, discord.app_commands.Group)
    mcp_add = mcp.get_command("add")
    assert mcp_add is not None
    assert "project_env_refs" in {parameter.name for parameter in mcp_add.parameters}


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

    plan = await _discord_render_plan(
        {
            "content": "Result: ![chart](artifacts/chart.png)",
            "finalized": True,
            "trusted_local_images": True,
            "trusted_local_image_paths": ["artifacts/chart.png"],
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


def test_interaction_view_uses_indexed_select_and_freeform_modal_button() -> None:
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
    select, button = view.children
    assert select.custom_id == f"cdi:{interaction_id}:select"
    assert [option.value for option in select.options] == ["0", "1"]
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
