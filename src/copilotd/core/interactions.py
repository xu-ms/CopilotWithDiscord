from __future__ import annotations

from typing import Any


def interaction_target_mode(response: Any) -> str | None:
    if not isinstance(response, dict) or response.get("approved") is not True:
        return None
    selected_action = response.get("selectedAction")
    if selected_action in {"interactive", "plan", "autopilot"}:
        return str(selected_action)
    return None
