import ast
from pathlib import Path


def test_production_discord_rest_awaits_only_the_request_coordinator() -> None:
    source_path = Path("src/copilotd/discord_app.py")
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    rest_operations = {
        "add_reaction",
        "create_thread",
        "defer",
        "delete",
        "edit",
        "fetch_channel",
        "fetch_message",
        "pin",
        "remove_reaction",
        "reply",
        "send",
        "send_message",
        "send_modal",
        "sync",
    }
    violations: list[tuple[int, str]] = []
    non_discord_receivers = {
        "runtime",
        "responder",
        "self._require_deletions()",
        "self._require_scheduler_commands()",
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.Await) or not isinstance(node.value, ast.Call):
            continue
        function = node.value.func
        if isinstance(function, ast.Attribute) and function.attr in rest_operations:
            receiver = ast.unparse(function.value)
            if (
                receiver in non_discord_receivers
                or "_interaction_runtime" in receiver
                or receiver.startswith("DiscordInteractionResponder(")
            ):
                continue
            violations.append((node.lineno, function.attr))

    assert violations == []
