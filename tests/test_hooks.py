from datetime import UTC, datetime
from pathlib import Path

import pytest

from copilotd.core.hooks import HookSessionContext, SessionHookAudit
from copilotd.core.inbox import ReducerInbox
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.storage.database import Database


@pytest.mark.asyncio
async def test_all_registered_hooks_create_typed_redacted_audit_projections(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "hooks.sqlite3") as database:
        inbox = ReducerInbox(
            sdk_session_id="session-hooks",
            generation=3,
            fence_token=17,
            capacity=64,
            thread_id="thread-hooks",
        )

        async def valid_fence(generation: int, fence_token: int) -> bool:
            return generation == 3 and fence_token == 17

        worker = EventReducerWorker(
            inbox=inbox,
            reducer=JournalReducer(database),
            batch_size=16,
            fence_validator=valid_fence,
        )
        worker.start()
        audit = SessionHookAudit(
            inbox=inbox,
            context=HookSessionContext(
                sdk_session_id="session-hooks",
                runtime_generation=3,
                owner_fence_token=17,
                thread_id="thread-hooks",
                project_id="project-1",
                project_source="explicit",
                cwd_snapshot=str(tmp_path),
                config_version=4,
                config_hash="config-hash",
            ),
        )
        handlers = audit.handlers()
        assert "on_user_prompt_transformed" not in handlers
        assert "on_agent_stop" not in handlers
        timestamp = datetime.now(UTC)
        base = {
            "sessionId": "session-hooks",
            "timestamp": timestamp,
            "workingDirectory": str(tmp_path),
        }
        await handlers["on_pre_tool_use"](
            {
                **base,
                "toolName": "shell",
                "toolArgs": {"command": "SECRET_COMMAND"},
            },
            {"hookInvocationId": "pre-tool"},
        )
        await handlers["on_pre_mcp_tool_call"](
            {
                **base,
                "serverName": "local",
                "toolName": "echo",
                "arguments": {"secret": "SECRET_MCP_ARGUMENT"},
                "toolCallId": "tool-1",
            },
            {"hookInvocationId": "pre-mcp"},
        )
        await handlers["on_post_tool_use"](
            {
                **base,
                "toolName": "write",
                "toolArgs": {"path": "file.txt"},
                "toolResult": {
                    "status": "success",
                    "diff": "SECRET_DIFF_CONTENT",
                    "fileName": "file.txt",
                },
            },
            {"hookInvocationId": "post-tool"},
        )
        await handlers["on_post_tool_use_failure"](
            {
                **base,
                "toolName": "shell",
                "toolArgs": {"command": "false"},
                "error": "SECRET_PERMISSION_DENIED",
            },
            {"hookInvocationId": "post-failure"},
        )
        await handlers["on_user_prompt_submitted"](
            {**base, "prompt": "SECRET_USER_PROMPT"},
            {"hookInvocationId": "prompt-submitted"},
        )
        start_result = await handlers["on_session_start"](
            {**base, "source": "resume"},
            {"hookInvocationId": "session-start"},
        )
        await handlers["on_session_end"](
            {
                **base,
                "reason": "error",
                "error": "SECRET_SESSION_ERROR",
            },
            {"hookInvocationId": "session-end"},
        )
        await handlers["on_error_occurred"](
            {
                **base,
                "error": "SECRET_MODEL_ERROR",
                "errorContext": "model_call",
                "recoverable": True,
            },
            {
                "hookInvocationId": "session-error",
                "correlationId": "correlation-1",
            },
        )
        await inbox.join()
        rows = await database.fetchall(
            """
            SELECT hook_name, phase, payload_json
            FROM hook_audit_events ORDER BY observed_at, hook_name
            """
        )
        error = await database.fetchone(
            """
            SELECT classification, recoverable, correlation_id, stale
            FROM session_error_projections
            """
        )
        await worker.stop()

    assert len(rows) == 8
    encoded = "\n".join(str(row["payload_json"]) for row in rows)
    for secret in (
        "SECRET_COMMAND",
        "SECRET_MCP_ARGUMENT",
        "SECRET_DIFF_CONTENT",
        "SECRET_PERMISSION_DENIED",
        "SECRET_USER_PROMPT",
        "SECRET_SESSION_ERROR",
        "SECRET_MODEL_ERROR",
    ):
        assert secret not in encoded
    assert start_result["additionalContext"].startswith("copilotD session context:")
    assert dict(error) == {
        "classification": "model_call",
        "recoverable": 1,
        "correlation_id": "correlation-1",
        "stale": 0,
    }
