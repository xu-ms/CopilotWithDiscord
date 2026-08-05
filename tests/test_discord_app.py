from pathlib import Path

import discord
import pytest

from copilotd.config import Settings
from copilotd.discord_app import (
    CopilotDiscordBot,
    _discord_render,
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
    } <= roots
    assert not {
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
    } & roots


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

    rendered, assets = await _discord_render(
        {"content": content, "finalized": True}
    )

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
                "options": [
                    {"label": "Worker A", "value": "card-a", "state": "running"}
                ],
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
