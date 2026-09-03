from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from copilotd.core.bindings import (
    AttachmentState,
    BindingIntent,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.session_deletion import (
    SessionDeletionBlocked,
    SessionDeletionService,
    SessionDeletionUnknown,
)
from copilotd.core.session_runtime import RuntimeState
from copilotd.core.volatile_content import opaque_content_key
from copilotd.storage.database import Database


class FakeDeleteBridge:
    def __init__(
        self,
        *,
        delete_outcomes: list[Exception | None] | None = None,
        exists_results: list[bool] | None = None,
    ) -> None:
        self.delete_outcomes = list(delete_outcomes or [None])
        self.exists_results = list(exists_results or [])
        self.delete_calls: list[str] = []
        self.exists_calls: list[str] = []

    async def delete_session(self, session_id: str) -> None:
        self.delete_calls.append(session_id)
        outcome = self.delete_outcomes.pop(0)
        if outcome is not None:
            raise outcome

    async def session_exists(self, session_id: str) -> bool:
        self.exists_calls.append(session_id)
        return self.exists_results.pop(0)


class FakeRuntime:
    def __init__(self, database: Database, binding: SessionBinding) -> None:
        self.database = database
        self.binding = binding
        self.state = RuntimeState.READY
        self.close_calls: list[dict[str, Any]] = []

    async def close(self, *, idempotency_key: str, force: bool = False) -> None:
        self.close_calls.append(
            {
                "idempotency_key": idempotency_key,
                "force": force,
            }
        )
        await self.database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent',
                runtime_remote_mode = 'off', row_version = row_version + 1
            WHERE sdk_session_id = ?
            """,
            (self.binding.sdk_session_id,),
        )
        self.state = RuntimeState.CLOSED


class FakeSessionRegistry:
    def __init__(self, runtime: FakeRuntime | None = None) -> None:
        self.runtime = runtime
        self.ensure_calls: list[str] = []
        self.retired: list[str] = []

    def for_thread(self, thread_id: str) -> FakeRuntime | None:
        if self.runtime is not None and self.runtime.binding.thread_id == thread_id:
            return self.runtime
        return None

    async def ensure_attached(self, binding: SessionBinding) -> FakeRuntime:
        self.ensure_calls.append(binding.sdk_session_id)
        if self.runtime is None:
            raise AssertionError("test registry has no runtime")
        return self.runtime

    async def replace(self, binding: SessionBinding) -> FakeRuntime:
        self.ensure_calls.append(binding.sdk_session_id)
        if self.runtime is None:
            raise AssertionError("test registry has no runtime")
        return self.runtime

    async def retire(self, thread_id: str) -> None:
        self.retired.append(thread_id)


async def _closed_binding(
    database: Database,
    bindings: SessionBindingRepository,
    tmp_path: Path,
    *,
    thread_id: str,
) -> SessionBinding:
    binding = await bindings.create(
        thread_id=thread_id,
        sdk_session_id=str(uuid4()),
        cwd_snapshot=tmp_path,
        project_source="implicit-home",
    )
    await database.execute(
        """
        UPDATE session_bindings
        SET binding_intent = 'closed', attachment_state = 'absent',
            runtime_remote_mode = 'off', row_version = row_version + 1
        WHERE sdk_session_id = ?
        """,
        (binding.sdk_session_id,),
    )
    current = await bindings.by_session(binding.sdk_session_id)
    assert current is not None
    return current


@pytest.mark.asyncio
async def test_delete_rejects_even_disabled_app_schedule_reference(tmp_path: Path) -> None:
    async with Database(tmp_path / "delete-schedule-reference.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await _closed_binding(
            database,
            bindings,
            tmp_path,
            thread_id="thread-delete-schedule",
        )
        await database.execute(
            """
            INSERT INTO schedules(
                id, thread_id, kind, expression, timezone, payload,
                target_snapshot, misfire_policy, state, created_at, updated_at
            ) VALUES (
                'schedule-disabled', ?, 'every', '1h', 'UTC', '{}',
                '{}', 'skip', 'disabled', 1, 1
            )
            """,
            (binding.thread_id,),
        )
        bridge = FakeDeleteBridge()
        registry = FakeSessionRegistry()
        service = SessionDeletionService(
            database,
            bindings,
            registry,  # type: ignore[arg-type]
            bridge,
            data_dir=tmp_path,
        )

        with pytest.raises(SessionDeletionBlocked, match="schedule-disabled"):
            await service.delete(binding, idempotency_key="blocked")

        current = await bindings.by_session(binding.sdk_session_id)
        assert current is not None
        assert current.binding_intent == BindingIntent.CLOSED
        assert bridge.delete_calls == []
        assert registry.retired == []


@pytest.mark.asyncio
async def test_delete_response_loss_retains_mapping_and_retries_same_sdk_id(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "delete-response-loss.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await _closed_binding(
            database,
            bindings,
            tmp_path,
            thread_id="thread-delete-response-loss",
        )
        attachment_root = (
            tmp_path / "sessions" / binding.sdk_session_id / "attachments" / "manifest"
        )
        attachment_root.mkdir(parents=True)
        (attachment_root / "payload.txt").write_text("retain until confirmed")
        bridge = FakeDeleteBridge(
            delete_outcomes=[ConnectionError("response lost"), None],
            exists_results=[True, True],
        )
        registry = FakeSessionRegistry()
        service = SessionDeletionService(
            database,
            bindings,
            registry,  # type: ignore[arg-type]
            bridge,
            data_dir=tmp_path,
        )

        with pytest.raises(SessionDeletionUnknown):
            await service.delete(binding, idempotency_key="first")

        unknown = await bindings.by_session(binding.sdk_session_id)
        assert unknown is not None
        assert unknown.binding_intent == BindingIntent.DELETE_UNKNOWN
        assert attachment_root.exists()
        assert registry.retired == []

        result = await service.delete(unknown, idempotency_key="retry")
        deleted = await bindings.by_session(binding.sdk_session_id)
        operations = await database.fetchall(
            """
            SELECT state FROM session_operations
            WHERE sdk_session_id = ? AND kind = 'delete-session'
            ORDER BY created_at
            """,
            (binding.sdk_session_id,),
        )

        assert result == BindingIntent.DELETED
        assert deleted is not None
        assert deleted.binding_intent == BindingIntent.DELETED
        assert deleted.attachment_state == AttachmentState.ABSENT
        assert not attachment_root.exists()
        assert bridge.delete_calls == [binding.sdk_session_id, binding.sdk_session_id]
        assert bridge.exists_calls == [binding.sdk_session_id, binding.sdk_session_id]
        assert [row["state"] for row in operations] == ["unknown", "confirmed"]
        assert registry.retired == [binding.thread_id]

        await service.delete(deleted, idempotency_key="idempotent")
        assert bridge.delete_calls == [binding.sdk_session_id, binding.sdk_session_id]


@pytest.mark.asyncio
async def test_delete_treats_authoritative_not_found_as_deleted(tmp_path: Path) -> None:
    async with Database(tmp_path / "delete-not-found.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await _closed_binding(
            database,
            bindings,
            tmp_path,
            thread_id="thread-delete-not-found",
        )
        bridge = FakeDeleteBridge(
            delete_outcomes=[ConnectionError("response lost")],
            exists_results=[False],
        )
        registry = FakeSessionRegistry()
        service = SessionDeletionService(
            database,
            bindings,
            registry,  # type: ignore[arg-type]
            bridge,
            data_dir=tmp_path,
        )

        result = await service.delete(binding, idempotency_key="not-found")
        operation = await database.fetchone(
            """
            SELECT state, result_ref FROM session_operations
            WHERE sdk_session_id = ? AND kind = 'delete-session'
            """,
            (binding.sdk_session_id,),
        )

        assert result == BindingIntent.DELETED
        assert operation["state"] == "confirmed"
        assert operation["result_ref"] is None
        assert registry.retired == [binding.thread_id]


@pytest.mark.asyncio
async def test_permanent_delete_reclaims_all_session_owned_volatile_refs(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "delete-volatile-content.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await _closed_binding(
            database,
            bindings,
            tmp_path,
            thread_id="thread-delete-content",
        )
        prompt_key = opaque_content_key(
            "submission-prompt",
            binding.sdk_session_id,
            "submission-delete",
        )
        render_key = opaque_content_key("render-outbox", "render-delete")
        result_key = opaque_content_key(
            "operation-result",
            binding.sdk_session_id,
            "operation-delete",
        )
        interaction_key = opaque_content_key(
            "interaction-request",
            binding.sdk_session_id,
            "interaction-delete",
        )
        attachment_name_key = opaque_content_key(
            "attachment-name",
            "manifest-delete",
            0,
        )
        attachment_recovery_key = opaque_content_key(
            "attachment-recovery-prompt",
            "manifest-delete",
        )
        for key, value in (
            (prompt_key, "prompt"),
            (render_key, {"content": "render"}),
            (result_key, {"result": "value"}),
            (interaction_key, {"question": "value"}),
            (attachment_name_key, "attachment.txt"),
            (attachment_recovery_key, "recovery prompt"),
        ):
            database.content_store.put(value, key=key)
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-delete', ?, 'app_message', 'submitted', 1)
            """,
            (binding.sdk_session_id,),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, prompt, prompt_content_key, requested_mode_snapshot,
                requested_model_config_snapshot, requested_agent_snapshot,
                requested_session_config_version, position, state, created_at, updated_at
            ) VALUES ('submission-delete', ?, '', ?, 'interactive', '{}',
                      'default', 1, 1, 'submitted', 1, 1)
            """,
            (binding.thread_id, prompt_key),
        )
        await database.execute(
            """
            INSERT INTO render_outbox(
                id, session_id, logical_seq, lane, idempotency_key, payload,
                state, attempts, next_attempt_at, created_at, updated_at,
                content_key, content_hash, render_kind, finalized
            ) VALUES ('render-delete', ?, 1, 'status', 'render-delete',
                      '{"schema":1}', 'sent', 1, 1, 1, 1, ?, 'hash', 'status', 1)
            """,
            (binding.sdk_session_id, render_key),
        )
        await database.execute(
            """
            INSERT INTO session_operations(
                operation_id, sdk_session_id, runtime_generation, owner_fence_token,
                kind, idempotency_key, input_hash, state, result_ref, created_at
            ) VALUES ('operation-delete', ?, 1, 1, 'query', 'query-delete',
                      'hash', 'confirmed', ?, 1)
            """,
            (binding.sdk_session_id, result_key),
        )
        await database.execute(
            """
            INSERT INTO pending_interactions(
                interaction_id, sdk_session_id, runtime_generation,
                owner_fence_token, thread_id, kind, response_plane,
                expires_at, state, payload, content_key, request_hash,
                created_at, updated_at
            ) VALUES ('interaction-delete', ?, 1, 1, ?, 'user_input',
                      'direct_handler', 2, 'expired', '{}', ?, 'hash', 1, 1)
            """,
            (binding.sdk_session_id, binding.thread_id, interaction_key),
        )
        await database.execute(
            """
            INSERT INTO attachment_manifests(
                id, source_kind, source_id, session_id, state,
                total_bytes, created_at, updated_at
            ) VALUES ('manifest-delete', 'test', 'source', ?, 'released', 1, 1, 1)
            """,
            (binding.sdk_session_id,),
        )
        await database.execute(
            """
            INSERT INTO attachment_items(
                manifest_id, item_index, original_name, mime_type, byte_size,
                sha256, local_path, sdk_attachment_kind, state
            ) VALUES ('manifest-delete', 0, '', 'text/plain', 1, 'hash',
                      ?, 'file', 'ready')
            """,
            (str(tmp_path / "sessions" / binding.sdk_session_id / "attachments" / "item"),),
        )
        service = SessionDeletionService(
            database,
            bindings,
            FakeSessionRegistry(),  # type: ignore[arg-type]
            FakeDeleteBridge(),
            data_dir=tmp_path,
        )

        assert await service.delete(binding, idempotency_key="delete-content") == (
            BindingIntent.DELETED
        )

        assert all(
            database.content_store.get(key) is None
            for key in (
                prompt_key,
                render_key,
                result_key,
                interaction_key,
                attachment_name_key,
                attachment_recovery_key,
            )
        )


@pytest.mark.asyncio
async def test_active_delete_performs_force_teardown_before_sdk_delete(tmp_path: Path) -> None:
    async with Database(tmp_path / "delete-active.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-delete-active",
            sdk_session_id=str(uuid4()),
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        runtime = FakeRuntime(database, binding)
        registry = FakeSessionRegistry(runtime)
        bridge = FakeDeleteBridge()
        service = SessionDeletionService(
            database,
            bindings,
            registry,  # type: ignore[arg-type]
            bridge,
            data_dir=tmp_path,
        )

        result = await service.delete(binding, idempotency_key="active-delete")

        assert result == BindingIntent.DELETED
        assert registry.ensure_calls == [binding.sdk_session_id]
        assert runtime.close_calls == [
            {
                "idempotency_key": (f"session-delete:{binding.sdk_session_id}:teardown"),
                "force": True,
            }
        ]
        assert bridge.delete_calls == [binding.sdk_session_id]
