from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from aiosqlite import Row

from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLease


class BindingIntent(StrEnum):
    ACTIVE = "active"
    CLOSED = "closed"
    DELETING = "deleting"
    DELETE_UNKNOWN = "delete_unknown"
    DELETED = "deleted"


class AttachmentState(StrEnum):
    ABSENT = "absent"
    CREATING = "creating"
    RESUMING = "resuming"
    ATTACHED = "attached"
    DISCONNECTING = "disconnecting"
    RECOVERY_UNKNOWN = "recovery_unknown"
    TERMINAL = "terminal"


class PermissionPosture(StrEnum):
    UNVERIFIED = "unverified"
    VERIFIED_ALLOW_ALL = "verified_allow_all"
    PLATFORM_BLOCKED = "platform_blocked"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SessionBinding:
    thread_id: str
    project_id: str | None
    project_source: str
    cwd_snapshot: Path
    sdk_session_id: str
    binding_intent: BindingIntent
    attachment_state: AttachmentState
    permission_posture: PermissionPosture
    desired_mode: str
    pending_mode: str | None
    pending_mode_transition_id: str | None
    runtime_mode: str
    runtime_generation: int
    owner_fence_token: int | None
    last_inbox_seq: int
    last_sdk_receive_seq: int | None
    row_version: int


class BindingConflict(RuntimeError):
    pass


class SessionBindingRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def create(
        self,
        *,
        thread_id: str,
        sdk_session_id: str,
        cwd_snapshot: Path,
        project_source: str,
        project_id: str | None = None,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        resolved_cwd = await asyncio.to_thread(_resolve_path, cwd_snapshot)
        await self._database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_id, project_source, cwd_snapshot, sdk_session_id,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                project_id,
                project_source,
                str(resolved_cwd),
                sdk_session_id,
                timestamp,
                timestamp,
            ),
        )
        binding = await self.by_thread(thread_id)
        if binding is None:
            raise RuntimeError("created session binding could not be read back")
        return binding

    async def by_thread(self, thread_id: str) -> SessionBinding | None:
        row = await self._database.fetchone(
            "SELECT * FROM session_bindings WHERE thread_id = ?",
            (thread_id,),
        )
        return None if row is None else _row_to_binding(row)

    async def by_session(self, sdk_session_id: str) -> SessionBinding | None:
        row = await self._database.fetchone(
            "SELECT * FROM session_bindings WHERE sdk_session_id = ?",
            (sdk_session_id,),
        )
        return None if row is None else _row_to_binding(row)

    async def eager_bindings(self) -> list[SessionBinding]:
        rows = await self._database.fetchall(
            """
            SELECT * FROM session_bindings
            WHERE binding_intent = 'active'
              AND attachment_state != 'terminal'
            ORDER BY created_at, thread_id
            """
        )
        return [_row_to_binding(row) for row in rows]

    async def begin_attachment(
        self,
        *,
        thread_id: str,
        lease: OwnerLease,
        state: AttachmentState,
        now: float | None = None,
    ) -> SessionBinding:
        if state not in {AttachmentState.CREATING, AttachmentState.RESUMING}:
            raise ValueError(f"invalid beginning attachment state: {state}")
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            owner_cursor = await connection.execute(
                """
                SELECT 1 FROM session_owner_leases
                WHERE sdk_session_id = ? AND owner_id = ? AND fence_token = ?
                  AND expires_at > ?
                """,
                (
                    lease.sdk_session_id,
                    lease.owner_id,
                    lease.fence_token,
                    timestamp,
                ),
            )
            owner = await owner_cursor.fetchone()
            await owner_cursor.close()
            if owner is None:
                raise BindingConflict("owner fence is not current")

            cursor = await connection.execute(
                "SELECT * FROM session_bindings WHERE thread_id = ?",
                (thread_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                raise BindingConflict(f"session binding does not exist: {thread_id}")
            binding = _row_to_binding(row)
            if binding.sdk_session_id != lease.sdk_session_id:
                raise BindingConflict("owner lease does not match session binding")
            if binding.binding_intent != BindingIntent.ACTIVE:
                raise BindingConflict(f"cannot attach binding with intent {binding.binding_intent}")
            attachable_states = {
                AttachmentState.ABSENT,
                AttachmentState.RECOVERY_UNKNOWN,
            }
            stale_attached = (
                binding.attachment_state == AttachmentState.ATTACHED
                and binding.owner_fence_token != lease.fence_token
            )
            if binding.attachment_state not in attachable_states and not stale_attached:
                raise BindingConflict(f"cannot attach binding in state {binding.attachment_state}")

            generation = binding.runtime_generation + 1
            update = await connection.execute(
                """
                UPDATE session_bindings
                SET attachment_state = ?, permission_posture = 'unverified',
                    permission_verified_at = NULL, runtime_generation = ?,
                    owner_fence_token = ?, runtime_mode = 'unknown',
                    runtime_processing = NULL, runtime_has_active_work = NULL,
                    runtime_abortable = NULL, updated_at = ?,
                    row_version = row_version + 1
                WHERE thread_id = ? AND row_version = ?
                """,
                (
                    state.value,
                    generation,
                    lease.fence_token,
                    timestamp,
                    thread_id,
                    binding.row_version,
                ),
            )
            if update.rowcount != 1:
                await update.close()
                raise BindingConflict("session binding changed concurrently")
            await update.close()
            await connection.execute(
                """
                UPDATE liveness_leases
                SET state = 'orphaned', released_at = ?
                WHERE sdk_session_id = ? AND state = 'active'
                  AND (
                    runtime_generation != ?
                    OR owner_fence_token != ?
                  )
                """,
                (timestamp, lease.sdk_session_id, generation, lease.fence_token),
            )
            await connection.execute(
                """
                UPDATE pending_interactions
                SET state = 'expired', updated_at = ?
                WHERE sdk_session_id = ? AND state = 'pending'
                  AND (
                    runtime_generation != ?
                    OR owner_fence_token != ?
                  )
                """,
                (timestamp, lease.sdk_session_id, generation, lease.fence_token),
            )

        result = await self.by_thread(thread_id)
        if result is None:
            raise RuntimeError("attached session binding could not be read back")
        return result

    async def mark_attached(
        self,
        binding: SessionBinding,
        *,
        permission_verified_at: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if permission_verified_at is None else permission_verified_at
        return await self._transition_attachment(
            binding,
            state=AttachmentState.ATTACHED,
            permission_posture=PermissionPosture.VERIFIED_ALLOW_ALL,
            permission_verified_at=timestamp,
            now=timestamp,
            expected_states=(AttachmentState.CREATING, AttachmentState.RESUMING),
        )

    async def mark_attached_blocked(
        self,
        binding: SessionBinding,
        *,
        posture: PermissionPosture,
        now: float | None = None,
    ) -> SessionBinding:
        if posture not in {PermissionPosture.PLATFORM_BLOCKED, PermissionPosture.UNKNOWN}:
            raise ValueError(f"invalid blocked attachment posture: {posture}")
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=AttachmentState.ATTACHED,
            permission_posture=posture,
            permission_verified_at=None,
            now=timestamp,
            expected_states=(AttachmentState.CREATING, AttachmentState.RESUMING),
        )

    async def mark_attach_unknown(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=AttachmentState.RECOVERY_UNKNOWN,
            permission_posture=PermissionPosture.UNKNOWN,
            permission_verified_at=None,
            now=timestamp,
            expected_states=(AttachmentState.CREATING, AttachmentState.RESUMING),
        )

    async def reset_cancelled_attachment(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=AttachmentState.ABSENT,
            permission_posture=PermissionPosture.UNVERIFIED,
            permission_verified_at=None,
            now=timestamp,
            expected_states=(
                AttachmentState.CREATING,
                AttachmentState.RESUMING,
                AttachmentState.ATTACHED,
            ),
        )

    async def mark_recovery_unknown(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=AttachmentState.RECOVERY_UNKNOWN,
            permission_posture=PermissionPosture.UNKNOWN,
            permission_verified_at=None,
            now=timestamp,
            expected_states=(
                AttachmentState.CREATING,
                AttachmentState.RESUMING,
                AttachmentState.ATTACHED,
                AttachmentState.DISCONNECTING,
            ),
        )

    async def invalidate_permissions(
        self,
        binding: SessionBinding,
        *,
        posture: PermissionPosture = PermissionPosture.UNVERIFIED,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=binding.attachment_state,
            permission_posture=posture,
            permission_verified_at=None,
            now=timestamp,
            expected_states=(AttachmentState.ATTACHED,),
        )

    async def mark_permissions_verified(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=AttachmentState.ATTACHED,
            permission_posture=PermissionPosture.VERIFIED_ALLOW_ALL,
            permission_verified_at=timestamp,
            now=timestamp,
            expected_states=(AttachmentState.ATTACHED,),
        )

    async def begin_close(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        if binding.attachment_state != AttachmentState.ATTACHED:
            raise BindingConflict("only an attached session can be closed")
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE session_bindings
                SET binding_intent = 'closed', attachment_state = 'disconnecting',
                    updated_at = ?, row_version = row_version + 1
                WHERE thread_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ? AND attachment_state = 'attached'
                """,
                (
                    timestamp,
                    binding.thread_id,
                    binding.runtime_generation,
                    binding.owner_fence_token,
                ),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise BindingConflict("session binding changed before close")
            await cursor.close()
        result = await self.by_thread(binding.thread_id)
        if result is None:
            raise RuntimeError("closing session binding could not be read back")
        return result

    async def finish_disconnect(
        self,
        binding: SessionBinding,
        *,
        succeeded: bool,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        return await self._transition_attachment(
            binding,
            state=(AttachmentState.ABSENT if succeeded else AttachmentState.RECOVERY_UNKNOWN),
            permission_posture=(
                PermissionPosture.UNVERIFIED if succeeded else PermissionPosture.UNKNOWN
            ),
            permission_verified_at=None,
            now=timestamp,
            expected_states=(AttachmentState.DISCONNECTING,),
        )

    async def activate(
        self,
        binding: SessionBinding,
        *,
        now: float | None = None,
    ) -> SessionBinding:
        timestamp = time.time() if now is None else now
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE session_bindings
                SET binding_intent = 'active', updated_at = ?, row_version = row_version + 1
                WHERE thread_id = ? AND binding_intent = 'closed'
                """,
                (timestamp, binding.thread_id),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise BindingConflict("session binding is not closed")
            await cursor.close()
        result = await self.by_thread(binding.thread_id)
        if result is None:
            raise RuntimeError("activated session binding could not be read back")
        return result

    async def _transition_attachment(
        self,
        binding: SessionBinding,
        *,
        state: AttachmentState,
        permission_posture: PermissionPosture,
        permission_verified_at: float | None,
        now: float,
        expected_states: tuple[AttachmentState, ...],
    ) -> SessionBinding:
        placeholders = ", ".join("?" for _ in expected_states)
        async with self._database.transaction() as connection:
            cursor = await connection.execute(
                f"""
                UPDATE session_bindings
                SET attachment_state = ?, permission_posture = ?,
                    permission_verified_at = ?, updated_at = ?,
                    row_version = row_version + 1
                WHERE thread_id = ? AND runtime_generation = ?
                  AND owner_fence_token = ?
                  AND attachment_state IN ({placeholders})
                """,
                (
                    state.value,
                    permission_posture.value,
                    permission_verified_at,
                    now,
                    binding.thread_id,
                    binding.runtime_generation,
                    binding.owner_fence_token,
                    *(expected.value for expected in expected_states),
                ),
            )
            if cursor.rowcount != 1:
                await cursor.close()
                raise BindingConflict("session binding generation or fence changed")
            await cursor.close()
        result = await self.by_thread(binding.thread_id)
        if result is None:
            raise RuntimeError("transitioned session binding could not be read back")
        return result


def _row_to_binding(row: Row) -> SessionBinding:
    return SessionBinding(
        thread_id=row["thread_id"],
        project_id=row["project_id"],
        project_source=row["project_source"],
        cwd_snapshot=Path(row["cwd_snapshot"]),
        sdk_session_id=row["sdk_session_id"],
        binding_intent=BindingIntent(row["binding_intent"]),
        attachment_state=AttachmentState(row["attachment_state"]),
        permission_posture=PermissionPosture(row["permission_posture"]),
        desired_mode=row["desired_mode"],
        pending_mode=row["pending_mode"],
        pending_mode_transition_id=row["pending_mode_transition_id"],
        runtime_mode=row["runtime_mode"],
        runtime_generation=row["runtime_generation"],
        owner_fence_token=row["owner_fence_token"],
        last_inbox_seq=row["last_inbox_seq"],
        last_sdk_receive_seq=row["last_sdk_receive_seq"],
        row_version=row["row_version"],
    )


def _resolve_path(path: Path) -> Path:
    return path.expanduser().resolve()
