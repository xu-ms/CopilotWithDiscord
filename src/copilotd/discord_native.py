from __future__ import annotations

import io
import json
from typing import Any, Protocol

import discord
from discord import app_commands

from copilotd.core.native import NativeTaskAction
from copilotd.core.session_runtime import SessionRuntime
from copilotd.sdk.capabilities import CapabilityManifest


class NativeDiscordHost(Protocol):
    tree: app_commands.CommandTree[Any]

    async def _interaction_runtime(
        self,
        interaction: discord.Interaction,
    ) -> SessionRuntime: ...


class NativeDiscordRegistrar:
    def __init__(
        self,
        host: NativeDiscordHost,
        capabilities: CapabilityManifest,
    ) -> None:
        self._host = host
        self._capabilities = capabilities

    def register(self, session: app_commands.Group) -> None:
        roots = self._capabilities.discord_command_roots()
        if self._capabilities.supports("history_compact"):

            @session.command(
                name="compact",
                description="Compact this Copilot session's history",
            )
            async def session_compact(
                interaction: discord.Interaction,
                focus: str | None = None,
            ) -> None:
                await interaction.response.defer(ephemeral=True)
                result = await (await self._runtime(interaction)).compact(
                    focus,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                compacted = result["result"]
                await interaction.followup.send(
                    "Compaction confirmed: "
                    f"`{int(compacted.get('messagesRemoved', 0))}` messages and "
                    f"`{int(compacted.get('tokensRemoved', 0))}` tokens removed.",
                    ephemeral=True,
                )

        if "ask" in roots:

            @self._host.tree.command(
                name="ask",
                description="Ask a no-tools side question without changing history",
            )
            async def ask(interaction: discord.Interaction, question: str) -> None:
                await interaction.response.defer(ephemeral=True)
                answer = await (await self._runtime(interaction)).ask_ephemeral(
                    question,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                await _send_ephemeral(interaction, answer, "copilot-ask.txt")

        if "fleet" in roots:

            @self._host.tree.command(
                name="fleet",
                description="Start Copilot Fleet in this session",
            )
            async def fleet(interaction: discord.Interaction, prompt: str) -> None:
                await interaction.response.defer(ephemeral=True)
                result = await (await self._runtime(interaction)).start_fleet(
                    prompt,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                await interaction.followup.send(
                    f"Fleet started as `{result['fleet_run_id']}`; "
                    "workers remain in this thread's TaskDeck.",
                    ephemeral=True,
                )

        if "tasks" in roots:
            choices = [
                app_commands.Choice(name=action, value=action)
                for action in sorted(self._capabilities.task_actions())
            ]

            @self._host.tree.command(
                name="tasks",
                description="Inspect or control native Copilot tasks",
            )
            @app_commands.choices(action=choices)
            async def tasks(
                interaction: discord.Interaction,
                action: app_commands.Choice[str],
                task_id: str | None = None,
                message: str | None = None,
                wait_seconds: float = 30.0,
            ) -> None:
                await interaction.response.defer(ephemeral=True)
                result = await (await self._runtime(interaction)).task_action(
                    NativeTaskAction(action.value),
                    task_id=task_id,
                    message=message,
                    wait_seconds=wait_seconds,
                    idempotency_key=f"interaction:{interaction.id}",
                )
                await _send_ephemeral(
                    interaction,
                    _task_result_text(result),
                    "copilot-tasks.json",
                )

        if "agent" in roots:
            choices = [
                app_commands.Choice(name=action, value=action)
                for action in sorted(self._capabilities.agent_actions())
            ]

            @self._host.tree.command(
                name="agent",
                description="Inspect or select the root Copilot agent",
            )
            @app_commands.choices(action=choices)
            async def agent(
                interaction: discord.Interaction,
                action: app_commands.Choice[str],
                name: str | None = None,
            ) -> None:
                await interaction.response.defer(ephemeral=True)
                runtime = await self._runtime(interaction)
                if action.value == "list":
                    result = await runtime.list_agents()
                elif action.value == "current":
                    result = await runtime.current_agent()
                elif action.value == "select":
                    if name is None:
                        raise ValueError("name is required for agent select")
                    result = {
                        "selected": await runtime.select_agent(
                            name,
                            idempotency_key=f"interaction:{interaction.id}",
                        )
                    }
                else:
                    result = {
                        "selected": await runtime.deselect_agent(
                            idempotency_key=f"interaction:{interaction.id}",
                        )
                    }
                await _send_ephemeral(
                    interaction,
                    json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
                    "copilot-agents.json",
                )

        for schedule_kind in ("after", "every"):
            if schedule_kind in roots:
                self._register_schedule(schedule_kind)

        if "remote" in roots:
            choices = [
                app_commands.Choice(name=action, value=action)
                for action in sorted(self._capabilities.remote_actions())
            ]

            @self._host.tree.command(
                name="remote",
                description="Inspect or change native Copilot remote exposure",
            )
            @app_commands.choices(action=choices)
            async def remote(
                interaction: discord.Interaction,
                action: app_commands.Choice[str],
            ) -> None:
                await interaction.response.defer(ephemeral=True)
                runtime = await self._runtime(interaction)
                result = (
                    await runtime.remote_status()
                    if action.value == "status"
                    else await runtime.set_remote(
                        action.value,
                        idempotency_key=f"interaction:{interaction.id}",
                    )
                )
                await _send_ephemeral(
                    interaction,
                    json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
                    "copilot-remote.json",
                )

        if "review" in roots:

            @self._host.tree.command(
                name="review",
                description="Run the native Copilot code review builtin",
            )
            async def review(
                interaction: discord.Interaction,
                instructions: str | None = None,
                selection_token: str | None = None,
                selection: str | None = None,
            ) -> None:
                await self._run_builtin(
                    interaction,
                    "review",
                    instructions,
                    selection_token,
                    selection,
                )

        if "security-review" in roots:

            @self._host.tree.command(
                name="security-review",
                description="Run the native Copilot security review builtin",
            )
            async def security_review(
                interaction: discord.Interaction,
                instructions: str | None = None,
                selection_token: str | None = None,
                selection: str | None = None,
            ) -> None:
                await self._run_builtin(
                    interaction,
                    "security-review",
                    instructions,
                    selection_token,
                    selection,
                )

        if "research" in roots:

            @self._host.tree.command(
                name="research",
                description="Run the native Copilot research builtin",
            )
            async def research(
                interaction: discord.Interaction,
                topic: str,
                selection_token: str | None = None,
                selection: str | None = None,
            ) -> None:
                await self._run_builtin(
                    interaction,
                    "research",
                    topic,
                    selection_token,
                    selection,
                )

        if "rubber-duck" in roots:

            @self._host.tree.command(
                name="rubber-duck",
                description="Ask the native Copilot rubber duck critic",
            )
            async def rubber_duck(
                interaction: discord.Interaction,
                question: str | None = None,
                selection_token: str | None = None,
                selection: str | None = None,
            ) -> None:
                await self._run_builtin(
                    interaction,
                    "rubber-duck",
                    question,
                    selection_token,
                    selection,
                )

    def _register_schedule(self, kind: str) -> None:
        choices = [
            app_commands.Choice(name=action, value=action)
            for action in sorted(self._capabilities.schedule_actions(kind))
        ]

        @self._host.tree.command(
            name=kind,
            description=f"Manage native Copilot {kind} schedules",
        )
        @app_commands.choices(action=choices)
        async def schedule(
            interaction: discord.Interaction,
            action: app_commands.Choice[str],
            expression: str | None = None,
            prompt: str | None = None,
            schedule_id: str | None = None,
        ) -> None:
            await interaction.response.defer(ephemeral=True)
            runtime = await self._runtime(interaction)
            if action.value == "create":
                if expression is None or prompt is None:
                    raise ValueError("expression and prompt are required for create")
                result: Any = await runtime.create_runtime_schedule(
                    kind,
                    expression,
                    prompt,
                    idempotency_key=f"interaction:{interaction.id}",
                )
            elif action.value == "cancel":
                if schedule_id is None:
                    raise ValueError("schedule_id is required for cancel")
                result = await runtime.cancel_runtime_schedule(
                    kind,
                    schedule_id,
                    idempotency_key=f"interaction:{interaction.id}",
                )
            else:
                result = await runtime.runtime_schedules(kind=kind)
            await _send_ephemeral(
                interaction,
                json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
                f"copilot-{kind}.json",
            )

    async def _run_builtin(
        self,
        interaction: discord.Interaction,
        command: str,
        input_text: str | None,
        selection_token: str | None,
        selection: str | None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        runtime = await self._runtime(interaction)
        if selection_token is None:
            if selection is not None:
                raise ValueError("selection_token is required with selection")
            result = await runtime.invoke_native_command(
                command,
                input_text,
                idempotency_key=f"interaction:{interaction.id}",
            )
        else:
            if selection is None:
                raise ValueError("selection is required with selection_token")
            result = await runtime.continue_native_command(
                selection_token,
                selection,
                idempotency_key=f"interaction:{interaction.id}:selection",
            )
        await _send_ephemeral(
            interaction,
            _builtin_result_text(result),
            f"copilot-{command}.txt",
        )

    async def _runtime(self, interaction: discord.Interaction) -> SessionRuntime:
        return await self._host._interaction_runtime(interaction)


def _builtin_result_text(result: dict[str, Any]) -> str:
    kind = result["kind"]
    if kind == "text":
        return str(result.get("text") or "")
    if kind == "completed":
        return str(result.get("message") or "Native command completed.")
    if kind == "agent-prompt":
        notice = str(result.get("notice") or "")
        display = str(result.get("display_prompt") or "Runtime-generated prompt")
        return f"{display}\n{notice}\nSubmitted to this session.".strip()
    if kind == "select-subcommand":
        options = "\n".join(
            f"- `{option['name']}` — {option['description']}"
            for option in result.get("options", [])
        )
        return (
            f"{result.get('title') or 'Choose a native subcommand'}\n{options}\n"
            f"selection token: `{result.get('selection_token')}`"
        )
    raise ValueError(f"unsupported native command result: {kind}")


def _task_result_text(result: dict[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)


async def _send_ephemeral(
    interaction: discord.Interaction,
    text: str,
    filename: str,
) -> None:
    if len(text) <= 1900:
        await interaction.followup.send(text or "No content.", ephemeral=True)
        return
    await interaction.followup.send(
        file=discord.File(io.BytesIO(text.encode()), filename=filename),
        ephemeral=True,
    )
