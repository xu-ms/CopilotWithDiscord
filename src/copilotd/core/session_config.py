from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict


class McpServerOptions(TypedDict, total=False):
    tools: list[str]
    type: str
    command: str
    args: list[str]
    env: dict[str, str]
    working_directory: str
    url: str
    headers: dict[str, str]


class CustomAgentOptions(TypedDict, total=False):
    name: str
    display_name: str
    description: str
    prompt: str
    tools: list[str]


class SessionConfigSnapshotError(ValueError):
    code = "CD-INPUT-001"


@dataclass(frozen=True, slots=True)
class SessionLaunchOptions:
    environment: tuple[tuple[str, str], ...]
    mcp_servers: tuple[tuple[str, McpServerOptions], ...]
    skill_directories: tuple[str, ...]
    plugin_directories: tuple[str, ...]
    custom_agents: tuple[CustomAgentOptions, ...]

    @classmethod
    def from_json(cls, snapshot_json: str | None) -> SessionLaunchOptions | None:
        if snapshot_json is None:
            return None
        payload = json.loads(snapshot_json)
        variables = {str(item["name"]): str(item["value"]) for item in payload.get("variables", [])}
        cwd = str(payload["cwd"])
        mcp_servers: list[tuple[str, McpServerOptions]] = []
        resolved_environment_references: set[str] = set()
        for item in payload.get("mcp_servers", []):
            if not item.get("enabled", True):
                continue
            name = str(item["name"])
            transport = str(item["transport"])
            options: McpServerOptions = {"tools": ["*"]}
            if transport == "stdio":
                command = item.get("command")
                if not command:
                    raise SessionConfigSnapshotError(f"stdio MCP server {name!r} has no command")
                references = [str(value) for value in item.get("env_refs", [])]
                missing = [reference for reference in references if reference not in variables]
                if missing:
                    raise SessionConfigSnapshotError(
                        f"MCP server {name!r} references missing project variables: "
                        + ", ".join(missing)
                    )
                resolved_environment_references.update(references)
                options.update(
                    {
                        "type": "stdio",
                        "command": str(command),
                        "args": [str(value) for value in item.get("args", [])],
                        "env": {reference: variables[reference] for reference in references},
                        "working_directory": cwd,
                    }
                )
            elif transport == "http":
                url = item.get("url")
                if not url:
                    raise SessionConfigSnapshotError(f"http MCP server {name!r} has no URL")
                options.update(
                    {
                        "type": "http",
                        "url": str(url),
                        "headers": {
                            str(key): str(value)
                            for key, value in dict(item.get("headers", {})).items()
                        },
                    }
                )
            else:
                raise SessionConfigSnapshotError(
                    f"MCP server {name!r} has unsupported transport {transport!r}"
                )
            mcp_servers.append((name, options))
        agents = tuple(
            CustomAgentOptions(
                name=str(item["name"]),
                display_name=str(item["name"]),
                description=str(item["description"]),
                prompt=str(item["prompt"]),
                tools=[str(value) for value in item.get("tools", [])],
            )
            for item in payload.get("custom_agents", [])
            if item.get("enabled", True)
        )
        return cls(
            environment=tuple(
                (name, variables[name]) for name in sorted(resolved_environment_references)
            ),
            mcp_servers=tuple(mcp_servers),
            skill_directories=tuple(
                str(Path(str(item["path"])))
                for item in payload.get("skill_dirs", [])
                if item.get("enabled", True)
            ),
            plugin_directories=tuple(
                str(Path(str(item["path"])))
                for item in payload.get("plugin_dirs", [])
                if item.get("enabled", True)
            ),
            custom_agents=agents,
        )

    def sdk_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self.mcp_servers:
            kwargs["mcp_servers"] = {name: dict(options) for name, options in self.mcp_servers}
        if self.skill_directories:
            kwargs["enable_skills"] = True
            kwargs["skill_directories"] = list(self.skill_directories)
        if self.plugin_directories:
            kwargs["plugin_directories"] = list(self.plugin_directories)
        if self.custom_agents:
            kwargs["custom_agents"] = [dict(agent) for agent in self.custom_agents]
        return kwargs
