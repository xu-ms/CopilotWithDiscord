from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from copilotd.core.bindings import (
    AttachmentState,
    BindingIntent,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.session_runtime import RuntimeState
from copilotd.storage.database import Database

if TYPE_CHECKING:
    from copilotd.core.sessions import SessionRegistry


class SessionDeleteBridge(Protocol):
    async def delete_session(self, session_id: str) -> None: ...

    async def session_exists(self, session_id: str) -> bool: ...


class SessionDeletionBlocked(RuntimeError):
    pass


class SessionDeletionUnknown(RuntimeError):
    pass


class SessionDeletionService:
    """Durable, idempotent orchestration for permanent SDK session deletion."""

    def __init__(
        self,
        database: Database,
        bindings: SessionBindingRepository,
        sessions: SessionRegistry,
        bridge: SessionDeleteBridge,
        *,
        data_dir: Path,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._sessions = sessions
        self._bridge = bridge
        self._data_dir = data_dir
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def delete(
        self,
        binding: SessionBinding,
        *,
        idempotency_key: str,
    ) -> BindingIntent:
        del idempotency_key
        async with self._locks[binding.sdk_session_id]:
            current = await self._bindings.by_session(binding.sdk_session_id)
            if current is None:
                raise SessionDeletionBlocked("the copilotD session mapping no longer exists")
            if current.thread_id != binding.thread_id:
                raise SessionDeletionBlocked("the session mapping changed before deletion")
            if current.binding_intent == BindingIntent.DELETED:
                await self._sessions.retire(current.thread_id)
                await self._finish_cleanup(current)
                return BindingIntent.DELETED

            await self._reject_schedule_references(current)
            if current.binding_intent in {
                BindingIntent.DELETING,
                BindingIntent.DELETE_UNKNOWN,
            }:
                if await self._reconcile_missing(current):
                    return BindingIntent.DELETED
                current = await self._bindings.by_session(current.sdk_session_id)
                if current is None:
                    raise SessionDeletionBlocked("the session mapping disappeared during reconcile")

            if current.binding_intent in {BindingIntent.ACTIVE, BindingIntent.CLOSED}:
                await self._destructive_teardown(current)
                current = await self._bindings.by_session(current.sdk_session_id)
                if current is None:
                    raise SessionDeletionBlocked("the session mapping disappeared during teardown")
            if current.binding_intent not in {
                BindingIntent.CLOSED,
                BindingIntent.DELETING,
                BindingIntent.DELETE_UNKNOWN,
            }:
                raise SessionDeletionBlocked(
                    f"session is not closed after destructive teardown: {current.binding_intent}"
                )
            if current.attachment_state not in {
                AttachmentState.ABSENT,
                AttachmentState.RECOVERY_UNKNOWN,
                AttachmentState.TERMINAL,
            }:
                raise SessionDeletionBlocked(
                    "session attachment did not become terminal before deletion"
                )

            operation_id = await self._begin_attempt(current)
            try:
                await self._bridge.delete_session(current.sdk_session_id)
            except Exception as error:
                try:
                    exists = await self._bridge.session_exists(current.sdk_session_id)
                except Exception as reconcile_error:
                    await self._mark_unknown(operation_id, reconcile_error)
                    raise SessionDeletionUnknown(
                        "SDK deletion outcome is unknown; retry will reconcile the same session ID"
                    ) from error
                if exists:
                    await self._mark_unknown(operation_id, error)
                    raise SessionDeletionUnknown(
                        "SDK deletion was not confirmed; retry will use the same session ID"
                    ) from error
                await self._mark_deleted(operation_id, basis="authoritative_not_found")
            else:
                await self._mark_deleted(operation_id, basis="delete_response")

            deleted = await self._bindings.by_session(current.sdk_session_id)
            if deleted is None:
                raise RuntimeError("deleted session mapping disappeared")
            await self._sessions.retire(deleted.thread_id)
            await self._finish_cleanup(deleted)
            return BindingIntent.DELETED

    async def _reject_schedule_references(self, binding: SessionBinding) -> None:
        rows = await self._database.fetchall(
            """
            SELECT id, state FROM schedules
            WHERE thread_id = ? AND state != 'deleted'
            ORDER BY id
            """,
            (binding.thread_id,),
        )
        if rows:
            references = ", ".join(f"{row['id']} ({row['state']})" for row in rows)
            raise SessionDeletionBlocked(
                "delete or detach all app schedules that reference this session first: "
                + references
            )

    async def _reconcile_missing(self, binding: SessionBinding) -> bool:
        try:
            exists = await self._bridge.session_exists(binding.sdk_session_id)
        except Exception as error:
            await self._mark_current_delete_unknown(binding, error)
            raise SessionDeletionUnknown(
                "could not reconcile the prior delete; the original mapping was retained"
            ) from error
        if exists:
            await self._mark_current_delete_unknown(
                binding,
                RuntimeError("persisted session still exists"),
            )
            return False
        operation_id = await self._latest_delete_operation(binding.sdk_session_id)
        if operation_id is None:
            operation_id = await self._begin_attempt(binding)
        await self._mark_deleted(operation_id, basis="authoritative_not_found")
        deleted = await self._bindings.by_session(binding.sdk_session_id)
        if deleted is not None:
            await self._sessions.retire(deleted.thread_id)
            await self._finish_cleanup(deleted)
        return True

    async def _destructive_teardown(self, binding: SessionBinding) -> None:
        runtime = self._sessions.for_thread(binding.thread_id)
        needs_attachment = (
            binding.binding_intent == BindingIntent.ACTIVE
            or binding.attachment_state != AttachmentState.ABSENT
            or await self._has_live_runtime_projection(binding.sdk_session_id)
        )
        if not needs_attachment:
            return
        if binding.binding_intent == BindingIntent.CLOSED:
            if binding.attachment_reason != "recovery_cleanup":
                binding = await self._bindings.set_attachment_reason(
                    binding,
                    "recovery_cleanup",
                )
            if runtime is None or runtime.state in {
                RuntimeState.CLOSED,
                RuntimeState.FENCED,
                RuntimeState.RECOVERY_UNKNOWN,
                RuntimeState.TERMINAL,
            }:
                runtime = await self._sessions.replace(binding)
            if runtime.state == RuntimeState.DETACHED:
                await runtime.attach_resume()
        else:
            runtime = await self._sessions.ensure_attached(binding)

        try:
            await runtime.close(
                idempotency_key=f"session-delete:{binding.sdk_session_id}:teardown",
                force=True,
            )
        except Exception:
            latest = await self._bindings.by_session(binding.sdk_session_id)
            if latest is None or latest.binding_intent != BindingIntent.CLOSED:
                raise
        finally:
            latest = await self._bindings.by_session(binding.sdk_session_id)
            if latest is not None and latest.attachment_reason == "recovery_cleanup":
                await self._bindings.set_attachment_reason(latest, None)

    async def _has_live_runtime_projection(self, session_id: str) -> bool:
        binding = await self._database.fetchone(
            """
            SELECT runtime_remote_mode FROM session_bindings
            WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        if binding is None or str(binding["runtime_remote_mode"]) != "off":
            return True
        schedule = await self._database.fetchone(
            """
            SELECT 1 FROM runtime_schedules
            WHERE sdk_session_id = ? AND state IN ('active', 'unknown')
            LIMIT 1
            """,
            (session_id,),
        )
        if schedule is not None:
            return True
        task = await self._database.fetchone(
            """
            SELECT 1 FROM task_card_projections
            WHERE sdk_session_id = ? AND state IN ('running', 'idle', 'unknown')
            LIMIT 1
            """,
            (session_id,),
        )
        return task is not None

    async def _begin_attempt(self, binding: SessionBinding) -> str:
        now = time.time()
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT COUNT(*) AS count FROM session_operations
                WHERE sdk_session_id = ? AND kind = 'delete-session'
                """,
                (binding.sdk_session_id,),
            )
            attempt = int((await cursor.fetchone())["count"]) + 1
            await cursor.close()
            idempotency_key = f"session-delete:{binding.sdk_session_id}:attempt:{attempt}"
            operation_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"copilotd:{binding.sdk_session_id}:operation:{idempotency_key}",
                )
            )
            input_hash = hashlib.sha256(
                json.dumps(
                    {"sdk_session_id": binding.sdk_session_id},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            await connection.execute(
                """
                INSERT INTO session_operations(
                    operation_id, sdk_session_id, runtime_generation,
                    owner_fence_token, kind, idempotency_key, input_hash,
                    state, started_at, created_at
                ) VALUES (?, ?, ?, ?, 'delete-session', ?, ?, 'started', ?, ?)
                """,
                (
                    operation_id,
                    binding.sdk_session_id,
                    binding.runtime_generation,
                    binding.owner_fence_token or 0,
                    idempotency_key,
                    input_hash,
                    now,
                    now,
                ),
            )
            updated = await connection.execute(
                """
                UPDATE session_bindings
                SET binding_intent = 'deleting', updated_at = ?,
                    row_version = row_version + 1
                WHERE sdk_session_id = ?
                  AND binding_intent IN ('closed', 'deleting', 'delete_unknown')
                """,
                (now, binding.sdk_session_id),
            )
            if updated.rowcount != 1:
                await updated.close()
                raise SessionDeletionBlocked("session state changed before delete admission")
            await updated.close()
        return operation_id

    async def _latest_delete_operation(self, session_id: str) -> str | None:
        row = await self._database.fetchone(
            """
            SELECT operation_id FROM session_operations
            WHERE sdk_session_id = ? AND kind = 'delete-session'
            ORDER BY created_at DESC LIMIT 1
            """,
            (session_id,),
        )
        return None if row is None else str(row["operation_id"])

    async def _mark_current_delete_unknown(
        self,
        binding: SessionBinding,
        error: Exception,
    ) -> None:
        operation_id = await self._latest_delete_operation(binding.sdk_session_id)
        if operation_id is None:
            operation_id = await self._begin_attempt(binding)
        await self._mark_unknown(operation_id, error)

    async def _mark_unknown(self, operation_id: str, error: Exception) -> None:
        now = time.time()
        async with self._database.transaction() as connection:
            await connection.execute(
                """
                UPDATE session_operations
                SET state = 'unknown', error_code = ?, settled_at = ?
                WHERE operation_id = ? AND state IN ('pending', 'started', 'unknown')
                """,
                (type(error).__name__, now, operation_id),
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET binding_intent = 'delete_unknown', updated_at = ?,
                    row_version = row_version + 1
                WHERE sdk_session_id = ?
                  AND binding_intent IN ('deleting', 'delete_unknown')
                """,
                (now, (await self._operation_session(connection, operation_id))),
            )

    async def _mark_deleted(self, operation_id: str, *, basis: str) -> None:
        now = time.time()
        async with self._database.transaction() as connection:
            session_id = await self._operation_session(connection, operation_id)
            await connection.execute(
                """
                UPDATE session_operations
                SET state = 'confirmed', result_ref = ?, error_code = NULL,
                    settled_at = ?
                WHERE operation_id = ?
                """,
                (json.dumps({"basis": basis}, sort_keys=True), now, operation_id),
            )
            await connection.execute(
                """
                UPDATE session_bindings
                SET binding_intent = 'deleted', attachment_state = 'absent',
                    attachment_reason = NULL, permission_posture = 'unverified',
                    permission_verified_at = NULL, delete_cleanup_state = 'pending',
                    delete_cleanup_error = NULL, deleted_at = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ?
                  AND binding_intent IN ('deleting', 'delete_unknown')
                """,
                (now, now, session_id),
            )

    @staticmethod
    async def _operation_session(connection: object, operation_id: str) -> str:
        cursor = await connection.execute(  # type: ignore[attr-defined]
            "SELECT sdk_session_id FROM session_operations WHERE operation_id = ?",
            (operation_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            raise RuntimeError("delete operation disappeared")
        return str(row["sdk_session_id"])

    async def _finish_cleanup(self, binding: SessionBinding) -> None:
        row = await self._database.fetchone(
            """
            SELECT delete_cleanup_state FROM session_bindings
            WHERE sdk_session_id = ?
            """,
            (binding.sdk_session_id,),
        )
        if row is None or row["delete_cleanup_state"] == "complete":
            return
        attachment_root = self._data_dir / "sessions" / binding.sdk_session_id / "attachments"
        try:
            await asyncio.to_thread(shutil.rmtree, attachment_root, True)
            now = time.time()
            async with self._database.transaction() as connection:
                await connection.execute(
                    """
                    UPDATE attachment_items SET state = 'deleted'
                    WHERE manifest_id IN (
                        SELECT id FROM attachment_manifests WHERE session_id = ?
                    )
                    """,
                    (binding.sdk_session_id,),
                )
                await connection.execute(
                    """
                    UPDATE attachment_manifests SET state = 'deleted'
                    WHERE session_id = ?
                    """,
                    (binding.sdk_session_id,),
                )
                await connection.execute(
                    """
                    UPDATE worktree_intents
                    SET thread_id = NULL, sdk_session_id = NULL, updated_at = ?
                    WHERE sdk_session_id = ?
                    """,
                    (now, binding.sdk_session_id),
                )
                await connection.execute(
                    """
                    UPDATE project_worktrees
                    SET thread_id = NULL, sdk_session_id = NULL, updated_at = ?
                    WHERE sdk_session_id = ?
                    """,
                    (now, binding.sdk_session_id),
                )
                await connection.execute(
                    """
                    UPDATE session_ui_metadata
                    SET native_name_state = 'deleted', updated_at = ?
                    WHERE session_id = ?
                    """,
                    (now, binding.sdk_session_id),
                )
                await connection.execute(
                    "DELETE FROM session_owner_leases WHERE sdk_session_id = ?",
                    (binding.sdk_session_id,),
                )
                await connection.execute(
                    """
                    UPDATE session_bindings
                    SET delete_cleanup_state = 'complete',
                        delete_cleanup_error = NULL, updated_at = ?,
                        row_version = row_version + 1
                    WHERE sdk_session_id = ? AND binding_intent = 'deleted'
                    """,
                    (now, binding.sdk_session_id),
                )
        except Exception as error:
            await self._database.execute(
                """
                UPDATE session_bindings
                SET delete_cleanup_state = 'pending', delete_cleanup_error = ?,
                    updated_at = ?, row_version = row_version + 1
                WHERE sdk_session_id = ? AND binding_intent = 'deleted'
                """,
                (f"{type(error).__name__}: {error}", time.time(), binding.sdk_session_id),
            )
