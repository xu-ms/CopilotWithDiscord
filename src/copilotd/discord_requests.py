from __future__ import annotations

import asyncio
import inspect
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any

import structlog

from copilotd.discord_http_limiter import (
    DiscordHttpRateLimiterClosed,
    DiscordHttpRedirectBlocked,
    DiscordHttpRouteCapacityExceeded,
)

logger = structlog.get_logger(__name__)


class DiscordOperation(StrEnum):
    SEND = "send"
    EDIT = "edit"
    DELETE = "delete"
    ADD_REACTION = "add_reaction"
    REMOVE_REACTION = "remove_reaction"
    CHANNEL_MUTATION = "channel_mutation"
    THREAD_CREATE = "thread_create"
    FETCH = "fetch"
    HISTORY = "history"
    INTERACTION_DEFER = "interaction_defer"
    INTERACTION_RESPONSE = "interaction_response"
    INTERACTION_MODAL = "interaction_modal"
    INTERACTION_FOLLOWUP = "interaction_followup"
    PIN = "pin"
    COMMAND_SYNC = "command_sync"


class DiscordPriority(IntEnum):
    DEADLINE_CRITICAL = 0
    FOREGROUND = 10
    BACKGROUND = 20
    MAINTENANCE = 30


@dataclass(frozen=True, slots=True)
class DiscordCoordinatorConfig:
    queue_limit: int = 512
    max_concurrency: int = 16
    interaction_deadline_seconds: float = 2.5
    stream_edit_interval_seconds: float = 1.0
    reaction_interval_seconds: float = 0.25

    def __post_init__(self) -> None:
        if (
            self.queue_limit < 1
            or self.max_concurrency < 1
            or self.interaction_deadline_seconds <= 0
            or self.stream_edit_interval_seconds <= 0
            or self.reaction_interval_seconds <= 0
        ):
            raise ValueError("Discord semantic coordinator bounds must be positive")


class DiscordRequestError(RuntimeError):
    pass


class DiscordBackpressure(DiscordRequestError):
    pass


class DiscordRequestDropped(DiscordRequestError):
    pass


class DiscordDeadlineExceeded(DiscordRequestError):
    pass


@dataclass(frozen=True, slots=True)
class DiscordRequest:
    operation: DiscordOperation
    callback: Callable[[], Awaitable[Any]]
    route_key: str
    target_key: str
    priority: DiscordPriority = DiscordPriority.FOREGROUND
    coalesce_key: str | None = None
    terminal: bool = False
    deadline: float | None = None
    min_interval_seconds: float = 0.0
    created_at: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        if not self.route_key or not self.target_key:
            raise ValueError("Discord request route and target keys are required")
        if self.min_interval_seconds < 0:
            raise ValueError("Discord request minimum interval cannot be negative")


@dataclass(slots=True)
class _Entry:
    sequence: int
    request: DiscordRequest
    waiters: list[asyncio.Future[Any]]


class DiscordRequestCoordinator:
    """Orders and coalesces logical Discord operations before discord.py."""

    def __init__(
        self,
        config: DiscordCoordinatorConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config or DiscordCoordinatorConfig()
        self._clock = clock
        self._started_at = clock()
        self._target_last_started: dict[str, float] = {}
        self._queue: list[_Entry] = []
        self._condition = asyncio.Condition()
        self._worker: asyncio.Task[None] | None = None
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._active_targets: set[str] = set()
        self._closing = False
        self._sequence = 0
        self._peak_depth = 0
        self._coalesced = 0
        self._dropped = 0
        self._failed = 0
        self._deadline_misses = 0
        self._completed = 0
        self._total_queue_wait_seconds = 0.0
        self._ack_latency_seconds = 0.0
        self._by_priority: Counter[str] = Counter()
        self._by_operation: Counter[str] = Counter()

    async def execute(self, request: DiscordRequest) -> Any:
        if self._closing:
            raise DiscordBackpressure("Discord request coordinator is shutting down")
        self._ensure_worker()
        waiter: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        async with self._condition:
            if self._closing:
                raise DiscordBackpressure("Discord request coordinator is shutting down")
            existing = self._coalescible_entry(request.coalesce_key)
            if existing is not None:
                if existing.request.terminal and not request.terminal:
                    self._dropped += 1
                else:
                    existing.request = request
                    self._coalesced += 1
                existing.waiters.append(waiter)
                self._condition.notify()
            else:
                if len(self._queue) >= self.config.queue_limit:
                    self._reject_overflow(request)
                self._sequence += 1
                self._queue.append(_Entry(self._sequence, request, [waiter]))
                self._peak_depth = max(self._peak_depth, len(self._queue))
                self._condition.notify()
        try:
            return await waiter
        except asyncio.CancelledError:
            waiter.cancel()
            async with self._condition:
                self._condition.notify()
            raise

    async def close(self) -> None:
        self._closing = True
        async with self._condition:
            for entry in self._queue:
                self._fail_waiters(
                    entry,
                    DiscordBackpressure("Discord request coordinator stopped"),
                )
            self._queue.clear()
            self._condition.notify_all()
        worker = self._worker
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            self._worker = None
        active = tuple(self._active_tasks)
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        self._active_tasks.clear()
        self._active_targets.clear()

    def snapshot(self) -> dict[str, Any]:
        now = self._clock()
        priority_depth: Counter[str] = Counter()
        operation_depth: Counter[str] = Counter()
        for entry in self._queue:
            priority_depth[entry.request.priority.name.lower()] += 1
            operation_depth[entry.request.operation.value] += 1
        return {
            "queue_depth": len(self._queue),
            "active_operations": len(self._active_tasks),
            "active_targets": len(self._active_targets),
            "queue_peak": self._peak_depth,
            "queue_by_priority": dict(priority_depth),
            "queue_by_operation": dict(operation_depth),
            "logical_completed": self._completed,
            "logical_completion_rate": self._completed / max(now - self._started_at, 0.001),
            "average_queue_wait_seconds": (
                0.0 if self._completed == 0 else self._total_queue_wait_seconds / self._completed
            ),
            "logical_failures": self._failed,
            "coalesced": self._coalesced,
            "dropped": self._dropped,
            "deadline_misses": self._deadline_misses,
            "interaction_ack_average_seconds": (
                0.0
                if self._by_priority[DiscordPriority.DEADLINE_CRITICAL.name] == 0
                else self._ack_latency_seconds
                / self._by_priority[DiscordPriority.DEADLINE_CRITICAL.name]
            ),
            "completed_by_priority": dict(self._by_priority),
            "completed_by_operation": dict(self._by_operation),
        }

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._run(),
                name="discord-semantic-request-coordinator",
            )

    def _coalescible_entry(self, key: str | None) -> _Entry | None:
        if key is None:
            return None
        return next(
            (
                entry
                for entry in self._queue
                if entry.request.coalesce_key == key
                and any(not waiter.cancelled() for waiter in entry.waiters)
            ),
            None,
        )

    def _reject_overflow(self, request: DiscordRequest) -> None:
        self._dropped += 1
        if request.priority >= DiscordPriority.BACKGROUND and request.coalesce_key is not None:
            raise DiscordRequestDropped(
                f"Discord {request.operation.value} dropped by bounded coordinator queue"
            )
        raise DiscordBackpressure(
            f"Discord {request.operation.value} rejected by bounded coordinator queue"
        )

    async def _run(self) -> None:
        while True:
            entry: _Entry | None = None
            async with self._condition:
                while entry is None:
                    if self._closing:
                        return
                    self._queue = [
                        candidate
                        for candidate in self._queue
                        if any(not waiter.cancelled() for waiter in candidate.waiters)
                    ]
                    if not self._queue:
                        await self._condition.wait()
                        continue
                    if len(self._active_tasks) >= self.config.max_concurrency:
                        await self._condition.wait()
                        continue
                    now = self._clock()
                    entry, delay = self._select(now)
                    if entry is None:
                        if delay is None:
                            await self._condition.wait()
                            continue
                        try:
                            await asyncio.wait_for(
                                self._condition.wait(),
                                timeout=max(0.001, delay),
                            )
                        except TimeoutError:
                            pass
                        continue
                    request = entry.request
                    if request.deadline is not None and now + delay >= request.deadline:
                        self._queue.remove(entry)
                        self._deadline_misses += 1
                        self._fail_waiters(
                            entry,
                            DiscordDeadlineExceeded(
                                f"Discord {request.operation.value} cannot meet its deadline"
                            ),
                        )
                        entry = None
                        continue
                    if delay > 0:
                        entry = None
                        try:
                            await asyncio.wait_for(self._condition.wait(), timeout=delay)
                        except TimeoutError:
                            pass
                        continue
                    self._queue.remove(entry)
                    self._target_last_started[request.target_key] = now
                    self._active_targets.add(request.target_key)
                    task = asyncio.create_task(
                        self._invoke_active(entry),
                        name=f"discord-semantic:{request.operation.value}:{entry.sequence}",
                    )
                    self._active_tasks.add(task)

    def _select(self, now: float) -> tuple[_Entry | None, float | None]:
        heads = [
            entry
            for entry in self._queue
            if entry.request.target_key not in self._active_targets
            if not any(
                older.sequence < entry.sequence
                and older.request.target_key == entry.request.target_key
                for older in self._queue
            )
        ]
        if not heads:
            return None, None
        heads.sort(key=lambda item: (item.request.priority, item.sequence))
        waits = [(entry, self._request_wait(entry.request, now)) for entry in heads]
        ready = [entry for entry, wait in waits if wait <= 0]
        if ready:
            return ready[0], 0.0
        return None, min(wait for _, wait in waits)

    def _request_wait(self, request: DiscordRequest, now: float) -> float:
        target_ready = (
            self._target_last_started.get(request.target_key, float("-inf"))
            + request.min_interval_seconds
        )
        return max(0.0, target_ready - now)

    async def _invoke_active(self, entry: _Entry) -> None:
        try:
            await self._invoke(entry)
        finally:
            current = asyncio.current_task()
            async with self._condition:
                self._active_targets.discard(entry.request.target_key)
                if current is not None:
                    self._active_tasks.discard(current)
                self._condition.notify_all()

    async def _invoke(self, entry: _Entry) -> None:
        request = entry.request
        started = self._clock()
        try:
            result = request.callback()
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            self._fail_waiters(
                entry,
                DiscordBackpressure("Discord request coordinator operation was cancelled"),
            )
            raise
        except (
            DiscordHttpRedirectBlocked,
            DiscordHttpRouteCapacityExceeded,
            DiscordHttpRateLimiterClosed,
        ) as error:
            self._failed += 1
            self._fail_waiters(entry, DiscordBackpressure(str(error)))
            return
        except Exception as error:
            self._failed += 1
            await logger.awarning(
                "discord_logical_operation_failed",
                operation=request.operation.value,
                priority=request.priority.name.lower(),
                route_key=request.route_key,
                target_key=request.target_key,
                error_type=type(error).__name__,
            )
            self._fail_waiters(entry, error)
            return

        completed = self._clock()
        queue_wait = max(0.0, started - request.created_at)
        self._completed += 1
        self._total_queue_wait_seconds += queue_wait
        self._by_priority[request.priority.name] += 1
        self._by_operation[request.operation.value] += 1
        if request.priority == DiscordPriority.DEADLINE_CRITICAL:
            self._ack_latency_seconds += max(0.0, completed - request.created_at)
        for waiter in entry.waiters:
            if not waiter.done():
                waiter.set_result(result)
        await logger.adebug(
            "discord_logical_operation_completed",
            operation=request.operation.value,
            priority=request.priority.name.lower(),
            route_key=request.route_key,
            target_key=request.target_key,
            queue_wait_seconds=round(queue_wait, 4),
            queue_depth=len(self._queue),
        )

    @staticmethod
    def _fail_waiters(entry: _Entry, error: BaseException) -> None:
        for waiter in entry.waiters:
            if not waiter.done():
                waiter.set_exception(error)
