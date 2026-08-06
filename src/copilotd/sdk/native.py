from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from copilot.generated.rpc import (
    SlashCommandAgentPromptResult,
    SlashCommandCompletedResult,
    SlashCommandInfo,
    SlashCommandSelectSubcommandResult,
    SlashCommandTextResult,
)


class NativeCapabilityUnavailable(RuntimeError):
    pass


class NativeCommandResultKind(StrEnum):
    TEXT = "text"
    AGENT_PROMPT = "agent-prompt"
    COMPLETED = "completed"
    SELECT_SUBCOMMAND = "select-subcommand"


@dataclass(frozen=True, slots=True)
class NativeCommandInput:
    hint: str
    required: bool
    preserve_multiline_input: bool
    completion: str | None
    choices: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class NativeCommandDefinition:
    name: str
    kind: str
    description: str
    aliases: tuple[str, ...]
    allow_during_agent_execution: bool
    experimental: bool
    schedulable: bool
    input: NativeCommandInput | None

    @classmethod
    def from_sdk(cls, command: SlashCommandInfo) -> NativeCommandDefinition:
        input_spec = command.input
        return cls(
            name=command.name,
            kind=command.kind.value,
            description=command.description,
            aliases=tuple(command.aliases or ()),
            allow_during_agent_execution=command.allow_during_agent_execution,
            experimental=bool(command.experimental),
            schedulable=bool(command.schedulable),
            input=(
                None
                if input_spec is None
                else NativeCommandInput(
                    hint=input_spec.hint,
                    required=bool(input_spec.required),
                    preserve_multiline_input=bool(input_spec.preserve_multiline_input),
                    completion=(
                        None if input_spec.completion is None else input_spec.completion.value
                    ),
                    choices=tuple(
                        {
                            "name": choice.name,
                            "description": choice.description,
                        }
                        for choice in input_spec.choices or ()
                    ),
                )
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NativeCommandSelection:
    name: str
    description: str
    group: str | None


@dataclass(frozen=True, slots=True)
class NativeCommandResult:
    kind: NativeCommandResultKind
    runtime_settings_changed: bool
    text: str | None = None
    markdown: bool = False
    preserve_ansi: bool = False
    display_prompt: str | None = None
    prompt: str | None = None
    mode: str | None = None
    notice: str | None = None
    message: str | None = None
    command: str | None = None
    title: str | None = None
    options: tuple[NativeCommandSelection, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["kind"] = self.kind.value
        return payload


def parse_command_result(value: Any) -> NativeCommandResult:
    if isinstance(value, SlashCommandTextResult):
        return NativeCommandResult(
            kind=NativeCommandResultKind.TEXT,
            runtime_settings_changed=bool(value.runtime_settings_changed),
            text=value.text,
            markdown=bool(value.markdown),
            preserve_ansi=bool(value.preserve_ansi),
        )
    if isinstance(value, SlashCommandAgentPromptResult):
        return NativeCommandResult(
            kind=NativeCommandResultKind.AGENT_PROMPT,
            runtime_settings_changed=bool(value.runtime_settings_changed),
            display_prompt=value.display_prompt,
            prompt=value.prompt,
            mode=None if value.mode is None else value.mode.value,
            notice=value.notice,
        )
    if isinstance(value, SlashCommandCompletedResult):
        return NativeCommandResult(
            kind=NativeCommandResultKind.COMPLETED,
            runtime_settings_changed=bool(value.runtime_settings_changed),
            message=value.message,
        )
    if isinstance(value, SlashCommandSelectSubcommandResult):
        return NativeCommandResult(
            kind=NativeCommandResultKind.SELECT_SUBCOMMAND,
            runtime_settings_changed=bool(value.runtime_settings_changed),
            command=value.command,
            title=value.title,
            options=tuple(
                NativeCommandSelection(
                    name=option.name,
                    description=option.description,
                    group=option.group,
                )
                for option in value.options
            ),
        )
    raise NativeCapabilityUnavailable(
        f"unsupported commands.invoke result variant: {type(value).__name__}"
    )
