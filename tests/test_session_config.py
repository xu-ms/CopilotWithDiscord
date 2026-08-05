import json

import pytest

from copilotd.core.session_config import (
    SessionConfigSnapshotError,
    SessionLaunchOptions,
)


def test_typed_snapshot_maps_only_public_sdk_session_options() -> None:
    snapshot = {
        "cwd": "/tmp/project",
        "variables": [{"name": "TOKEN", "value": "secret"}],
        "mcp_servers": [
            {
                "name": "local",
                "transport": "stdio",
                "command": "server",
                "args": ["--stdio"],
                "headers": {},
                "env_refs": ["TOKEN"],
                "enabled": True,
                "version": 1,
            },
            {
                "name": "remote",
                "transport": "http",
                "url": "https://example.invalid/mcp",
                "args": [],
                "headers": {"Authorization": "Bearer value"},
                "env_refs": [],
                "enabled": True,
                "version": 1,
            },
        ],
        "skill_dirs": [{"path": "/tmp/skills", "enabled": True}],
        "plugin_dirs": [{"path": "/tmp/plugins", "enabled": True}],
        "custom_agents": [
            {
                "name": "reviewer",
                "description": "Reviews",
                "prompt": "Review this.",
                "tools": ["read_file"],
                "enabled": True,
            }
        ],
    }

    options = SessionLaunchOptions.from_json(json.dumps(snapshot))
    assert options is not None
    kwargs = options.sdk_kwargs()

    assert kwargs["mcp_servers"]["local"] == {
        "tools": ["*"],
        "type": "stdio",
        "command": "server",
        "args": ["--stdio"],
        "env": {"TOKEN": "secret"},
        "working_directory": "/tmp/project",
    }
    assert kwargs["mcp_servers"]["remote"]["type"] == "http"
    assert kwargs["skill_directories"] == ["/tmp/skills"]
    assert kwargs["plugin_directories"] == ["/tmp/plugins"]
    assert kwargs["custom_agents"][0]["name"] == "reviewer"
    assert not {
        "schedule",
        "fork",
        "native_rpc",
        "environment",
    } & kwargs.keys()


def test_mcp_environment_reference_fails_closed_when_snapshot_is_incomplete() -> None:
    snapshot = {
        "cwd": "/tmp/project",
        "variables": [],
        "mcp_servers": [
            {
                "name": "local",
                "transport": "stdio",
                "command": "server",
                "env_refs": ["MISSING"],
                "enabled": True,
            }
        ],
    }
    with pytest.raises(SessionConfigSnapshotError, match="missing project variables"):
        SessionLaunchOptions.from_json(json.dumps(snapshot))
