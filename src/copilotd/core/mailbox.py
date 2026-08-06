from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

from copilotd.core.inbox import ReducerInbox
from copilotd.core.task_registry import TaskRegistry
from copilotd.storage.database import Database

OperationCallable = Callable[[], Awaitable[Any]]
FenceValidator = Callable[[], Awaitable[bool]]
ResultPersistence = Callable[[Any], Any]


class OperationState(StrEnum):
    PENDING = "pending"
    STARTED = "started"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class OperationRejected(RuntimeError):
    pass


class OperationAmbiguous(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OperationRecord:
    operation_id: str
    sdk_session_id: str
    runtime_generation: int
    owner_fence_token: int
    kind: str
    idempotency_key: str
    input_hash: str
    state: OperationState
    result_ref: str | None = None
    error_code: str | None = None


@dataclass(slots=True)
class _MailboxItem:
    record: OperationRecord
    operation: OperationCallable
    future: asyncio.Future[Any]
    result_persistence: ResultPersistence | None


class OperationStore:
    def __init__(self, database: Database, inbox: ReducerInbox) -> None:
        self._database = database
        self._inbox = inbox

    async def begin(
        self,
        *,
        sdk_session_id: str,
        runtime_generation: int,
        owner_fence_token: int,
        kind: str,
        idempotency_key: str,
        input_payload: Any,
    ) -> tuple[OperationRecord, bool]:
        input_hash = _input_hash(input_payload)
        row = await self._database.fetchone(
            """
            SELECT * FROM session_operations
            WHERE sdk_session_id = ? AND idempotency_key = ?
            """,
            (sdk_session_id, idempotency_key),
        )
        if row is not None:
            record = _row_to_record(row)
            if record.input_hash != input_hash or record.kind != kind:
                raise ValueError("idempotency key was reused with different input")
            return record, False

        operation_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:{sdk_session_id}:operation:{idempotency_key}",
            )
        )
        await self._inbox.commit_internal(
            {
                "type": "copilotd.operation.pending",
                "data": {
                    "operation_id": operation_id,
                    "runtime_generation": runtime_generation,
                    "owner_fence_token": owner_fence_token,
                    "kind": kind,
                    "idempotency_key": idempotency_key,
                    "input_hash": input_hash,
                    "created_at": time.time(),
                },
            },
            internal_event_id=f"operation:{operation_id}:pending",
        )
        row = await self._database.fetchone(
            "SELECT * FROM session_operations WHERE operation_id = ?",
            (operation_id,),
        )
        if row is None:
            raise RuntimeError(f"reducer did not persist operation {operation_id}")
        record = _row_to_record(row)
        if record.input_hash != input_hash or record.kind != kind:
            raise ValueError("idempotency key was reused with different input")
        return record, True

    async def transition(
        self,
        record: OperationRecord,
        *,
        state: OperationState,
        result: Any = None,
        error_code: str | None = None,
    ) -> OperationRecord:
        allowed = {
            OperationState.PENDING: {OperationState.STARTED, OperationState.UNKNOWN},
            OperationState.STARTED: {
                OperationState.CONFIRMED,
                OperationState.REJECTED,
                OperationState.UNKNOWN,
            },
        }
        if state not in allowed.get(record.state, set()):
            raise ValueError(f"invalid operation transition {record.state} -> {state}")
        result_ref = None if result is None else json.dumps(_jsonable(result), sort_keys=True)
        await self._inbox.commit_internal(
            {
                "type": "copilotd.operation.transition",
                "data": {
                    "operation_id": record.operation_id,
                    "from_state": record.state.value,
                    "to_state": state.value,
                    "result_ref": result_ref,
                    "error_code": error_code,
                    "transitioned_at": time.time(),
                },
            },
            internal_event_id=f"operation:{record.operation_id}:{state.value}",
        )
        row = await self._database.fetchone(
            "SELECT * FROM session_operations WHERE operation_id = ?",
            (record.operation_id,),
        )
        if row is None:
            raise RuntimeError(f"operation disappeared: {record.operation_id}")
        return _row_to_record(row)

    async def mark_unsettled_unknown(
        self,
        *,
        sdk_session_id: str,
        runtime_generation: int,
        owner_fence_token: int,
        error_code: str,
    ) -> None:
        await self._inbox.commit_internal(
            {
                "type": "copilotd.operation.unsettled_unknown",
                "data": {
                    "runtime_generation": runtime_generation,
                    "owner_fence_token": owner_fence_token,
                    "error_code": error_code,
                    "settled_at": time.time(),
                },
            },
            internal_event_id=(
                f"operations:{sdk_session_id}:{runtime_generation}:"
                f"{owner_fence_token}:unknown:{error_code}"
            ),
        )


class CommandMailbox:
    """Serializes every app-owned mutating or exclusive SDK operation."""

    def __init__(
        self,
        *,
        store: OperationStore,
        sdk_session_id: str,
        runtime_generation: int,
        owner_fence_token: int,
        fence_validator: FenceValidator,
        task_registry: TaskRegistry | None = None,
        capacity: int = 1024,
    ) -> None:
        self._store = store
        self._sdk_session_id = sdk_session_id
        self._runtime_generation = runtime_generation
        self._owner_fence_token = owner_fence_token
        self._fence_validator = fence_validator
        self._task_registry = task_registry or TaskRegistry()
        self._queue: asyncio.Queue[_MailboxItem | None] = asyncio.Queue(maxsize=capacity)
        self._futures: dict[str, asyncio.Future[Any]] = {}
        self._future_inputs: dict[str, tuple[str, str]] = {}
        self._submission_lock = asyncio.Lock()
        self._worker: asyncio.Task[None] | None = None
        self._accepting = False

    def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("command mailbox already started")
        self._accepting = True
        self._worker = self._task_registry.create(
            self._run(),
            name=f"mailbox:{self._sdk_session_id}",
            source="command-mailbox",
            session_id=self._sdk_session_id,
            runtime_generation=self._runtime_generation,
        )

    def freeze(self) -> None:
        self._accepting = False

    def thaw(self) -> None:
        if self._worker is None:
            raise RuntimeError("command mailbox is not running")
        self._accepting = True

    async def drain(self) -> None:
        await self._queue.join()

    async def freeze_and_drain(self) -> None:
        async with self._submission_lock:
            self._accepting = False
        await self._queue.join()

    async def stop(self, *, timeout_seconds: float = 5) -> None:
        if self._worker is None:
            return
        async with self._submission_lock:
            self._accepting = False
        worker = self._worker
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._queue.join()
        except TimeoutError:
            pass
        worker.cancel()
        try:
            async with asyncio.timeout(timeout_seconds):
                await asyncio.gather(worker, return_exceptions=True)
        except TimeoutError:
            worker.add_done_callback(_consume_task_result)
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is not None and not item.future.done():
                item.future.set_exception(
                    OperationAmbiguous(f"operation {item.record.operation_id} was interrupted")
                )
            self._queue.task_done()
        await self._store.mark_unsettled_unknown(
            sdk_session_id=self._sdk_session_id,
            runtime_generation=self._runtime_generation,
            owner_fence_token=self._owner_fence_token,
            error_code="mailbox_stopped",
        )
        for future in list(self._futures.values()):
            if not future.done():
                future.set_exception(
                    OperationAmbiguous("operation was interrupted by mailbox shutdown")
                )
        self._futures.clear()
        self._future_inputs.clear()
        self._worker = None

    async def submit(
        self,
        *,
        kind: str,
        idempotency_key: str,
        input_payload: Any,
        operation: OperationCallable,
        result_persistence: ResultPersistence | None = None,
        allow_when_frozen: bool = False,
    ) -> Any:
        if not self._accepting and not allow_when_frozen:
            raise RuntimeError("command mailbox is not accepting operations")
        input_hash = _input_hash(input_payload)
        async with self._submission_lock:
            if not self._accepting and not allow_when_frozen:
                raise RuntimeError("command mailbox is not accepting operations")
            future = self._futures.get(idempotency_key)
            if future is None:
                record, created = await self._store.begin(
                    sdk_session_id=self._sdk_session_id,
                    runtime_generation=self._runtime_generation,
                    owner_fence_token=self._owner_fence_token,
                    kind=kind,
                    idempotency_key=idempotency_key,
                    input_payload=input_payload,
                )
                if not created:
                    return _settled_result(record)

                future = asyncio.get_running_loop().create_future()
                self._futures[idempotency_key] = future
                self._future_inputs[idempotency_key] = (kind, input_hash)
                future.add_done_callback(
                    lambda _future, key=idempotency_key: self._evict_future(key)
                )
                await self._queue.put(_MailboxItem(record, operation, future, result_persistence))
            elif self._future_inputs.get(idempotency_key) != (kind, input_hash):
                raise ValueError("idempotency key was reused with different input")
        try:
            return await asyncio.shield(future)
        finally:
            if future.done():
                self._evict_future(idempotency_key)

    def _evict_future(self, idempotency_key: str) -> None:
        self._futures.pop(idempotency_key, None)
        self._future_inputs.pop(idempotency_key, None)

    async def _run(self) -> None:
        while item := await self._queue.get():
            try:
                await self._execute(item)
            finally:
                self._queue.task_done()
        self._queue.task_done()

    async def _execute(self, item: _MailboxItem) -> None:
        record = item.record
        if not await self._fence_validator():
            record = await self._store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code="owner_fence_lost_before_start",
            )
            item.future.set_exception(
                OperationAmbiguous(f"owner fence lost for operation {record.operation_id}")
            )
            return

        record = await self._store.transition(record, state=OperationState.STARTED)
        if not await self._fence_validator():
            record = await self._store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code="owner_fence_lost_before_dispatch",
            )
            item.future.set_exception(
                OperationAmbiguous(f"owner fence lost for operation {record.operation_id}")
            )
            return
        try:
            result = await item.operation()
        except asyncio.CancelledError:
            await self._store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code="mailbox_cancelled",
            )
            item.future.set_exception(
                OperationAmbiguous(f"operation {record.operation_id} was interrupted")
            )
            raise
        except OperationRejected as error:
            await self._store.transition(
                record,
                state=OperationState.REJECTED,
                error_code=type(error).__name__,
            )
            item.future.set_exception(error)
        except Exception as error:
            await self._store.transition(
                record,
                state=OperationState.UNKNOWN,
                error_code=type(error).__name__,
            )
            item.future.set_exception(
                OperationAmbiguous(f"operation {record.operation_id} outcome is unknown: {error}")
            )
        else:
            if not await self._fence_validator():
                await self._store.transition(
                    record,
                    state=OperationState.UNKNOWN,
                    error_code="owner_fence_lost_after_dispatch",
                )
                item.future.set_exception(
                    OperationAmbiguous(
                        f"owner fence was lost after operation {record.operation_id}"
                    )
                )
                return
            try:
                await self._store.transition(
                    record,
                    state=OperationState.CONFIRMED,
                    result=(
                        result
                        if item.result_persistence is None
                        else item.result_persistence(result)
                    ),
                )
            except Exception as error:
                await self._store.transition(
                    record,
                    state=OperationState.UNKNOWN,
                    error_code=type(error).__name__,
                )
                item.future.set_exception(
                    OperationAmbiguous(
                        f"operation {record.operation_id} result was not durably confirmed"
                    )
                )
            else:
                item.future.set_result(result)


def _settled_result(record: OperationRecord) -> Any:
    if record.state == OperationState.CONFIRMED:
        return None if record.result_ref is None else json.loads(record.result_ref)
    if record.state == OperationState.REJECTED:
        raise OperationRejected(
            f"operation {record.operation_id} was rejected: {record.error_code}"
        )
    raise OperationAmbiguous(
        f"operation {record.operation_id} is {record.state}; automatic replay is forbidden"
    )


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _input_hash(value: Any) -> str:
    canonical = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"operation payload is not JSON serializable: {type(value).__name__}")


def _row_to_record(row: Any) -> OperationRecord:
    return OperationRecord(
        operation_id=row["operation_id"],
        sdk_session_id=row["sdk_session_id"],
        runtime_generation=row["runtime_generation"],
        owner_fence_token=row["owner_fence_token"],
        kind=row["kind"],
        idempotency_key=row["idempotency_key"],
        input_hash=row["input_hash"],
        state=OperationState(row["state"]),
        result_ref=row["result_ref"],
        error_code=row["error_code"],
    )
