from __future__ import annotations

import asyncio
import inspect
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any

import structlog

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
    requests_per_second: float = 20.0
    burst: int = 10
    route_requests_per_second: float = 5.0
    route_burst: int = 5
    queue_limit: int = 512
    interaction_deadline_seconds: float = 2.5
    stream_edit_interval_seconds: float = 1.0
    taskdeck_edit_interval_seconds: float = 4.0
    reaction_interval_seconds: float = 0.25
    transient_attempts: int = 3
    retry_backoff_base_seconds: float = 0.25
    default_429_retry_seconds: float = 1.0

    def __post_init__(self) -> None:
        if (
            self.requests_per_second <= 0
            or self.route_requests_per_second <= 0
            or self.burst < 1
            or self.route_burst < 1
            or self.queue_limit < 1
            or self.interaction_deadline_seconds <= 0
            or self.transient_attempts < 1
        ):
            raise ValueError("Discord request coordinator limits must be positive")


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
    bucket_key: str | None = None
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
class _Bucket:
    tokens: float
    updated_at: float


@dataclass(slots=True)
class _Entry:
    sequence: int
    request: DiscordRequest
    waiters: list[asyncio.Future[Any]]
    not_before: float = 0.0
    transient_attempts: int = 0


class DiscordRequestCoordinator:
    """Process-wide application limiter around one logical Discord REST operation."""

    def __init__(
        self,
        config: DiscordCoordinatorConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config or DiscordCoordinatorConfig()
        self._clock = clock
        now = clock()
        self._started_at = now
        self._global_bucket = _Bucket(float(self.config.burst), now)
        self._route_buckets: dict[str, _Bucket] = {}
        self._route_aliases: dict[str, str] = {}
        self._global_cooldown_until = 0.0
        self._route_cooldowns: dict[str, float] = {}
        self._target_last_sent: dict[str, float] = {}
        self._queue: list[_Entry] = []
        self._condition = asyncio.Condition()
        self._worker: asyncio.Task[None] | None = None
        self._closing = False
        self._sequence = 0
        self._peak_depth = 0
        self._coalesced = 0
        self._dropped = 0
        self._rate_limited = 0
        self._permanent_failures = 0
        self._deadline_misses = 0
        self._sent = 0
        self._total_wait_seconds = 0.0
        self._ack_latency_seconds = 0.0
        self._by_priority: Counter[str] = Counter()
        self._by_operation: Counter[str] = Counter()

    async def execute(self, request: DiscordRequest) -> Any:
        if self._closing:
            raise DiscordBackpressure("Discord request coordinator is shutting down")
        self._ensure_worker()
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[Any] = loop.create_future()
        async with self._condition:
            existing = self._coalescible_entry(request.coalesce_key)
            if existing is not None:
                if existing.request.terminal and not request.terminal:
                    self._dropped += 1
                else:
                    existing.request = request
                    existing.not_before = 0.0
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

    def snapshot(self) -> dict[str, Any]:
        now = self._clock()
        priority_depth: Counter[str] = Counter()
        operation_depth: Counter[str] = Counter()
        for entry in self._queue:
            priority_depth[entry.request.priority.name.lower()] += 1
            operation_depth[entry.request.operation.value] += 1
        route_cooldowns = {
            key: round(until - now, 3)
            for key, until in self._route_cooldowns.items()
            if until > now
        }
        return {
            "queue_depth": len(self._queue),
            "queue_peak": self._peak_depth,
            "queue_by_priority": dict(priority_depth),
            "queue_by_operation": dict(operation_depth),
            "sent": self._sent,
            "send_rate": self._sent / max(now - self._started_at, 0.001),
            "average_wait_seconds": (
                0.0 if self._sent == 0 else self._total_wait_seconds / self._sent
            ),
            "coalesced": self._coalesced,
            "dropped": self._dropped,
            "rate_limited_429": self._rate_limited,
            "permanent_failures": self._permanent_failures,
            "global_cooldown_seconds": max(0.0, self._global_cooldown_until - now),
            "route_cooldowns": route_cooldowns,
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
                name="discord-request-coordinator",
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
                    now = self._clock()
                    entry, delay = self._select(now)
                    if entry is None:
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
                    self._consume_tokens(request, now)
                    self._queue.remove(entry)
            await self._invoke(entry)

    def _select(self, now: float) -> tuple[_Entry | None, float]:
        heads = [
            entry
            for entry in self._queue
            if not any(
                older.sequence < entry.sequence
                and older.request.target_key == entry.request.target_key
                for older in self._queue
            )
        ]
        heads.sort(key=lambda item: (item.request.priority, item.sequence))
        global_wait = max(0.0, self._global_cooldown_until - now)
        if global_wait > 0:
            return None, global_wait
        waits = [(entry, self._request_wait(entry, now)) for entry in heads]
        ready = [entry for entry, wait in waits if wait <= 0]
        if ready:
            return ready[0], 0.0
        return None, min(wait for _, wait in waits)

    def _request_wait(self, entry: _Entry, now: float) -> float:
        request = entry.request
        bucket_key = self._effective_bucket_key(request)
        route_cooldown = self._route_cooldowns.get(bucket_key, 0.0)
        target_ready = (
            self._target_last_sent.get(request.target_key, float("-inf"))
            + request.min_interval_seconds
        )
        return max(
            0.0,
            entry.not_before - now,
            route_cooldown - now,
            target_ready - now,
            self._token_wait(
                self._global_bucket,
                self.config.requests_per_second,
                float(self.config.burst),
                now,
            ),
            self._token_wait(
                self._route_bucket(bucket_key, now),
                self.config.route_requests_per_second,
                float(self.config.route_burst),
                now,
            ),
        )

    def _consume_tokens(self, request: DiscordRequest, now: float) -> None:
        self._refill(
            self._global_bucket,
            self.config.requests_per_second,
            float(self.config.burst),
            now,
        )
        route = self._route_bucket(self._effective_bucket_key(request), now)
        self._refill(
            route,
            self.config.route_requests_per_second,
            float(self.config.route_burst),
            now,
        )
        self._global_bucket.tokens -= 1
        route.tokens -= 1

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
        except Exception as error:
            status = _http_status(error)
            if status == 429:
                retry_after = _retry_after(error, self.config.default_429_retry_seconds)
                is_global = _is_global_rate_limit(error)
                await self._reschedule_rate_limit(
                    entry,
                    retry_after=retry_after,
                    is_global=is_global,
                    error=error,
                )
                return
            if (
                status == 408
                or (status is not None and status >= 500)
                or isinstance(error, (OSError, TimeoutError))
            ):
                if entry.transient_attempts + 1 < self.config.transient_attempts:
                    entry.transient_attempts += 1
                    entry.not_before = self._clock() + min(
                        self.config.retry_backoff_base_seconds
                        * (2 ** (entry.transient_attempts - 1)),
                        30.0,
                    )
                    await self._requeue(entry)
                    return
            if status is not None and 400 <= status < 500:
                self._permanent_failures += 1
            await logger.awarning(
                "discord_request_failed",
                operation=request.operation.value,
                priority=request.priority.name.lower(),
                route_key=request.route_key,
                target_key=request.target_key,
                status=status,
                permanent=status is not None and 400 <= status < 500,
                error_type=type(error).__name__,
            )
            self._fail_waiters(entry, error)
            return

        completed = self._clock()
        wait = max(0.0, started - request.created_at)
        self._target_last_sent[request.target_key] = completed
        self._sent += 1
        self._total_wait_seconds += wait
        self._by_priority[request.priority.name] += 1
        self._by_operation[request.operation.value] += 1
        if request.priority == DiscordPriority.DEADLINE_CRITICAL:
            self._ack_latency_seconds += max(0.0, completed - request.created_at)
        for waiter in entry.waiters:
            if not waiter.done():
                waiter.set_result(result)
        await logger.adebug(
            "discord_request_completed",
            operation=request.operation.value,
            priority=request.priority.name.lower(),
            route_key=request.route_key,
            target_key=request.target_key,
            wait_seconds=round(wait, 4),
            queue_depth=len(self._queue),
        )

    async def _reschedule_rate_limit(
        self,
        entry: _Entry,
        *,
        retry_after: float,
        is_global: bool,
        error: Exception,
    ) -> None:
        now = self._clock()
        until = now + max(0.0, retry_after)
        headers = _headers(error)
        bucket_id = headers.get("X-RateLimit-Bucket") or headers.get("x-ratelimit-bucket")
        if bucket_id:
            self._route_aliases[entry.request.route_key] = str(bucket_id)
        if is_global:
            self._global_cooldown_until = max(self._global_cooldown_until, until)
        else:
            bucket_key = self._effective_bucket_key(entry.request)
            self._route_cooldowns[bucket_key] = max(
                self._route_cooldowns.get(bucket_key, 0.0),
                until,
            )
        entry.not_before = until
        self._rate_limited += 1
        await logger.awarning(
            "discord_request_rate_limited",
            operation=entry.request.operation.value,
            priority=entry.request.priority.name.lower(),
            route_key=entry.request.route_key,
            global_cooldown=is_global,
            retry_after=retry_after,
        )
        await self._requeue(entry)

    async def _requeue(self, entry: _Entry) -> None:
        async with self._condition:
            self._queue.append(entry)
            self._peak_depth = max(self._peak_depth, len(self._queue))
            self._condition.notify_all()

    def _effective_bucket_key(self, request: DiscordRequest) -> str:
        return request.bucket_key or self._route_aliases.get(request.route_key) or request.route_key

    def _route_bucket(self, key: str, now: float) -> _Bucket:
        return self._route_buckets.setdefault(
            key,
            _Bucket(float(self.config.route_burst), now),
        )

    @staticmethod
    def _fail_waiters(entry: _Entry, error: BaseException) -> None:
        for waiter in entry.waiters:
            if not waiter.done():
                waiter.set_exception(error)

    @staticmethod
    def _refill(bucket: _Bucket, rate: float, capacity: float, now: float) -> None:
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(capacity, bucket.tokens + elapsed * rate)
        bucket.updated_at = now

    @classmethod
    def _token_wait(
        cls,
        bucket: _Bucket,
        rate: float,
        capacity: float,
        now: float,
    ) -> float:
        cls._refill(bucket, rate, capacity, now)
        return 0.0 if bucket.tokens >= 1 else (1 - bucket.tokens) / rate


def _http_status(error: Exception) -> int | None:
    status = getattr(error, "status", None)
    if status is None:
        response = getattr(error, "response", None)
        status = getattr(response, "status", None)
    try:
        return None if status is None else int(status)
    except (TypeError, ValueError):
        return None


def _headers(error: Exception) -> Mapping[str, Any]:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if isinstance(headers, Mapping):
        return headers
    direct = getattr(error, "headers", None)
    return direct if isinstance(direct, Mapping) else {}


def _retry_after(error: Exception, fallback: float) -> float:
    direct = getattr(error, "retry_after", None)
    headers = _headers(error)
    value = direct
    if value is None:
        value = headers.get("Retry-After") or headers.get("retry-after")
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return fallback


def _is_global_rate_limit(error: Exception) -> bool:
    if bool(getattr(error, "global_", False) or getattr(error, "is_global", False)):
        return True
    headers = _headers(error)
    value = headers.get("X-RateLimit-Global") or headers.get("x-ratelimit-global")
    return str(value).lower() in {"1", "true", "yes"}
