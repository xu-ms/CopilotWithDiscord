import ast
from pathlib import Path

from copilotd.config import Settings
from copilotd.discord_app import CopilotDiscordBot


def test_direct_discord_calls_are_covered_by_authoritative_http_trace(tmp_path: Path) -> None:
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path))

    assert bot.http.http_trace is not None
    assert bot.http.http_trace._copilotd_discord_http_limiter is bot.discord_http_limiter
    assert bot.http.http_trace.on_request_start
    assert bot.http.http_trace.on_request_end
    assert bot.http.http_trace.on_request_exception


def test_no_discord_api_urllib_or_unlimited_aiohttp_transport_exists() -> None:
    violations: list[str] = []
    for source_path in Path("src").rglob("*.py"):
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports_urllib_request = any(
            (
                isinstance(node, ast.Import)
                and any(alias.name == "urllib.request" for alias in node.names)
            )
            or (
                isinstance(node, ast.ImportFrom)
                and node.module == "urllib"
                and any(alias.name == "request" for alias in node.names)
            )
            for node in ast.walk(tree)
        )
        has_discord_api_url = "discord.com/api" in source or "discordapp.com/api" in source
        if imports_urllib_request and has_discord_api_url:
            violations.append(f"{source_path}: Discord API urllib transport")

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if ast.unparse(node.func) != "aiohttp.ClientSession":
                continue
            trace_keyword = next(
                (keyword for keyword in node.keywords if keyword.arg == "trace_configs"),
                None,
            )
            if source_path.name != "discord_http_limiter.py" or trace_keyword is None:
                violations.append(f"{source_path}:{node.lineno}: unlimited aiohttp session")

    assert violations == []
