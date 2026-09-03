from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import time
from collections import Counter, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import aiohttp

# Discord's documented limits are 50 requests/s globally and 10,000 invalid
# requests per 10 minutes. This process deliberately admits at most 80%.
# https://discord.com/developers/docs/topics/rate-limits
DISCORD_REST_GLOBAL_LIMIT = 40
DISCORD_REST_GLOBAL_WINDOW_SECONDS = 1.0
DISCORD_INVALID_REQUEST_LIMIT = 8_000
DISCORD_INVALID_REQUEST_WINDOW_SECONDS = 600.0
DISCORD_RATE_LIMIT_SAFETY_RATIO = 0.8

_DISCORD_API_HOSTS = frozenset(
    {
        "discord.com",
        "discordapp.com",
        "ptb.discord.com",
        "ptb.discordapp.com",
        "canary.discord.com",
        "canary.discordapp.com",
    }
)
_TRACE_SIGNALS = (
    "on_connection_create_end",
    "on_connection_create_start",
    "on_connection_queued_end",
    "on_connection_queued_start",
    "on_connection_reuseconn",
    "on_dns_cache_hit",
    "on_dns_cache_miss",
    "on_dns_resolvehost_end",
    "on_dns_resolvehost_start",
    "on_request_chunk_sent",
    "on_request_end",
    "on_request_exception",
    "on_request_headers_sent",
    "on_request_redirect",
    "on_request_start",
    "on_response_chunk_received",
)


@dataclass(frozen=True, slots=True)
class _Route:
    signature: str
    major_parameters: tuple[str, ...]
    global_exempt: bool = False


@dataclass(slots=True)
class _RouteMapping:
    bucket_hash: str
    last_seen: float


@dataclass(slots=True)
class _RouteBucket:
    last_seen: float
    limit: float | None = None
    window: float | None = None
    interval: float | None = None
    reset_at: float = 0.0
    next_at: float = 0.0
    cooldown_until: float = 0.0
    last_admitted: float | None = None
    remaining: float | None = None
    in_flight: int = 0
    admissions: dict[int, float] = field(default_factory=dict)


class DiscordHttpRateLimiterClosed(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Discord HTTP rate limiter is closed")


class DiscordHttpRedirectBlocked(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Discord REST redirects are disabled")


class DiscordHttpRouteCapacityExceeded(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Discord REST route limiter capacity is temporarily exhausted")


class DiscordHttpRateLimiter:
    """Authoritative admission control for physical Discord REST attempts.

    discord.py retains ownership of its explicit retries and server-side bucket
    waits. aiohttp's invisible reused-connection retry is disabled before each
    request reaches the wire so one trace admission is one physical attempt.
    """

    def __init__(
        self,
        *,
        global_limit: int = DISCORD_REST_GLOBAL_LIMIT,
        global_window_seconds: float = DISCORD_REST_GLOBAL_WINDOW_SECONDS,
        invalid_limit: int = DISCORD_INVALID_REQUEST_LIMIT,
        invalid_window_seconds: float = DISCORD_INVALID_REQUEST_WINDOW_SECONDS,
        safety_ratio: float = DISCORD_RATE_LIMIT_SAFETY_RATIO,
        max_route_buckets: int = 1_024,
        route_ttl_seconds: float = 900.0,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if not 0 < global_limit <= DISCORD_REST_GLOBAL_LIMIT:
            raise ValueError("Discord global test limit must be between 1 and 40")
        if not 0 < invalid_limit <= DISCORD_INVALID_REQUEST_LIMIT:
            raise ValueError("Discord invalid-request test limit must be between 1 and 8000")
        if global_window_seconds <= 0 or invalid_window_seconds <= 0:
            raise ValueError("Discord limiter windows must be positive")
        if not 0 < safety_ratio <= DISCORD_RATE_LIMIT_SAFETY_RATIO:
            raise ValueError("Discord route safety ratio cannot exceed 0.8")
        if max_route_buckets < 1 or route_ttl_seconds <= 0:
            raise ValueError("Discord route cache bounds must be positive")

        self._global_limit = global_limit
        self._global_window = global_window_seconds
        self._invalid_limit = invalid_limit
        self._invalid_window = invalid_window_seconds
        self._safety_ratio = safety_ratio
        self._max_route_buckets = max_route_buckets
        self._route_ttl = route_ttl_seconds
        self._clock = clock
        self._wall_clock = wall_clock
        self._condition = asyncio.Condition()
        self._global_admissions: deque[float] = deque()
        self._invalid_responses: deque[float] = deque()
        self._invalid_in_flight = 0
        self._unresolved_429_bodies = 0
        self._global_cooldown_until = 0.0
        self._route_hashes: dict[str, _RouteMapping] = {}
        self._route_buckets: dict[tuple[str, tuple[str, ...]], _RouteBucket] = {}
        self._next_route_admission_id = 0
        self._metrics: Counter[str] = Counter()
        self._status_counts: Counter[int] = Counter()
        self._total_wait_seconds = 0.0
        self._closed = False

    def trace_config(
        self,
        observer: aiohttp.TraceConfig | None = None,
    ) -> aiohttp.TraceConfig:
        trace = aiohttp.TraceConfig()
        trace.on_request_start.append(self.on_request_start)
        trace.on_request_end.append(self.on_request_end)
        trace.on_request_exception.append(self.on_request_exception)
        trace.on_request_redirect.append(self.on_request_redirect)
        trace._copilotd_discord_http_limiter = self
        if observer is not None:
            append_trace_callbacks(trace, observer)
        return trace

    async def close(self) -> None:
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    async def on_request_start(
        self,
        _session: aiohttp.ClientSession,
        context: SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:
        _disable_aiohttp_transparent_retries(_session)
        route = _discord_rest_route(params.method, params.url)
        if route is None:
            context.copilotd_discord_limited = False
            self._metrics["filtered_requests"] += 1
            return

        context.copilotd_discord_limited = True
        context.copilotd_discord_released = False
        context.copilotd_discord_route = route
        context.copilotd_discord_bucket_key = None
        context.copilotd_discord_admitted_at = None
        context.copilotd_discord_route_admission_id = None
        queued_at = self._clock()
        delayed = False

        async with self._condition:
            while True:
                if self._closed:
                    raise DiscordHttpRateLimiterClosed()
                now = self._clock()
                self._cleanup_locked(now)
                bucket_key, bucket = self._bucket_for_route_locked(route, now)
                waits: list[tuple[str, float | None]] = [
                    (
                        "global_cooldown",
                        0.0 if route.global_exempt else self._global_cooldown_until - now,
                    ),
                    (
                        "global_window",
                        0.0 if route.global_exempt else self._global_wait_locked(now),
                    ),
                    ("route", self._route_wait_locked(bucket, now)),
                    ("invalid", self._invalid_wait_locked(now)),
                    (
                        "429_body",
                        None if self._unresolved_429_bodies else 0.0,
                    ),
                ]
                active_waits = [
                    (reason, wait) for reason, wait in waits if wait is None or wait > 0
                ]
                if not active_waits:
                    admitted_at = self._clock()
                    if not route.global_exempt:
                        self._global_admissions.append(admitted_at)
                    self._invalid_in_flight += 1
                    bucket.in_flight += 1
                    self._next_route_admission_id += 1
                    route_admission_id = self._next_route_admission_id
                    bucket.admissions[route_admission_id] = admitted_at
                    bucket.last_seen = admitted_at
                    bucket.last_admitted = admitted_at
                    if bucket.interval is not None:
                        bucket.next_at = admitted_at + bucket.interval
                    context.copilotd_discord_bucket_key = bucket_key
                    context.copilotd_discord_admitted_at = admitted_at
                    context.copilotd_discord_route_admission_id = route_admission_id
                    self._metrics["physical_attempts"] += 1
                    if delayed:
                        self._metrics["delayed_attempts"] += 1
                        self._total_wait_seconds += max(0.0, admitted_at - queued_at)
                    return

                delayed = True
                finite = [(reason, wait) for reason, wait in active_waits if wait is not None]
                reason, delay = (
                    min(finite, key=lambda item: item[1]) if finite else ("invalid", None)
                )
                self._metrics[f"{reason}_waits"] += 1
                if delay is None:
                    await self._condition.wait()
                    continue
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=max(delay, 0.001))
                except TimeoutError:
                    pass

    async def on_request_end(
        self,
        _session: aiohttp.ClientSession,
        context: SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ) -> None:
        if not getattr(context, "copilotd_discord_limited", False):
            return

        response = params.response
        status = int(response.status)
        response_at = self._clock()
        ready_for_body = False
        async with self._condition:
            if status == 429 and not getattr(
                context,
                "copilotd_discord_429_body_pending",
                False,
            ):
                context.copilotd_discord_429_body_pending = True
                self._unresolved_429_bodies += 1
            try:
                route = context.copilotd_discord_route
                bucket_key, bucket = self._response_bucket_locked(
                    route,
                    context,
                    response.headers,
                    response_at,
                    learn=True,
                )
                self._release_reservation_locked(context)
                if status == 429:
                    header_retry_after = _header_retry_after(
                        response.headers,
                        self._wall_clock(),
                    )
                    scope = str(_header(response.headers, "X-RateLimit-Scope") or "").lower()
                    if header_retry_after is not None:
                        self._apply_429_cooldown_locked(
                            bucket_key,
                            bucket,
                            response_at + header_retry_after,
                            is_global=(
                                scope == "global"
                                or _truthy(_header(response.headers, "X-RateLimit-Global"))
                            ),
                        )
                    if scope in {"global", "user"} or _truthy(
                        _header(response.headers, "X-RateLimit-Global")
                    ):
                        self._invalid_responses.append(response_at)
                        context.copilotd_discord_invalid_recorded = True
                elif status in {401, 403}:
                    self._invalid_responses.append(response_at)
                self._status_counts[status] += 1
                self._metrics["responses"] += 1
                self._cleanup_locked(response_at)
                ready_for_body = True
            except Exception:
                self._release_reservation_locked(context)
                self._metrics["trace_processing_errors"] += 1
            finally:
                if status != 429:
                    self._condition.notify_all()

        if status != 429:
            return

        body: Mapping[str, Any] = {}
        body_available = False
        try:
            body = await _cached_json_body(response)
            body_available = True
        finally:
            async with self._condition:
                now = self._clock()
                try:
                    if ready_for_body and body_available:
                        route = context.copilotd_discord_route
                        bucket_key, bucket = self._response_bucket_locked(
                            route,
                            context,
                            response.headers,
                            now,
                            learn=False,
                        )
                        scope = str(_header(response.headers, "X-RateLimit-Scope") or "").lower()
                        is_global = (
                            scope == "global"
                            or _truthy(body.get("global"))
                            or _truthy(_header(response.headers, "X-RateLimit-Global"))
                        )
                        is_shared = not is_global and (
                            scope == "shared" or (not scope and _truthy(body.get("shared")))
                        )
                        self._apply_429_cooldown_locked(
                            bucket_key,
                            bucket,
                            response_at
                            + _retry_after(
                                response.headers,
                                body,
                                self._wall_clock(),
                            ),
                            is_global=is_global,
                        )
                        self._metrics[
                            (
                                "global_429"
                                if is_global
                                else ("shared_429" if is_shared else "route_429")
                            )
                        ] += 1
                        if not is_shared and not getattr(
                            context,
                            "copilotd_discord_invalid_recorded",
                            False,
                        ):
                            self._invalid_responses.append(response_at)
                        self._cleanup_locked(now)
                except Exception:
                    self._metrics["trace_processing_errors"] += 1
                finally:
                    if getattr(
                        context,
                        "copilotd_discord_429_body_pending",
                        False,
                    ):
                        context.copilotd_discord_429_body_pending = False
                        self._unresolved_429_bodies = max(
                            0,
                            self._unresolved_429_bodies - 1,
                        )
                    self._condition.notify_all()

    async def on_request_exception(
        self,
        _session: aiohttp.ClientSession,
        context: SimpleNamespace,
        _params: aiohttp.TraceRequestExceptionParams,
    ) -> None:
        if not getattr(context, "copilotd_discord_limited", False):
            return
        async with self._condition:
            self._release_reservation_locked(context)
            self._metrics["request_exceptions"] += 1
            self._condition.notify_all()

    async def on_request_redirect(
        self,
        _session: aiohttp.ClientSession,
        context: SimpleNamespace,
        _params: aiohttp.TraceRequestRedirectParams,
    ) -> None:
        if not getattr(context, "copilotd_discord_limited", False):
            return
        async with self._condition:
            self._release_reservation_locked(context)
            self._metrics["redirects_blocked"] += 1
            self._condition.notify_all()
        raise DiscordHttpRedirectBlocked()

    def snapshot(self) -> dict[str, Any]:
        now = self._clock()
        return {
            "physical_attempts": self._metrics["physical_attempts"],
            "responses": self._metrics["responses"],
            "request_exceptions": self._metrics["request_exceptions"],
            "redirects_blocked": self._metrics["redirects_blocked"],
            "trace_processing_errors": self._metrics["trace_processing_errors"],
            "filtered_requests": self._metrics["filtered_requests"],
            "delayed_attempts": self._metrics["delayed_attempts"],
            "total_wait_seconds": round(self._total_wait_seconds, 6),
            "global_window_waits": self._metrics["global_window_waits"],
            "global_cooldown_waits": self._metrics["global_cooldown_waits"],
            "route_waits": self._metrics["route_waits"],
            "route_capacity_rejections": self._metrics["route_capacity_rejections"],
            "invalid_waits": self._metrics["invalid_waits"],
            "rolling_global_attempts": sum(
                admitted > now - self._global_window for admitted in self._global_admissions
            ),
            "global_ceiling": self._global_limit,
            "global_cooldown_seconds": round(
                max(0.0, self._global_cooldown_until - now),
                6,
            ),
            "invalid_completed": sum(
                completed > now - self._invalid_window for completed in self._invalid_responses
            ),
            "invalid_in_flight": self._invalid_in_flight,
            "unresolved_429_bodies": self._unresolved_429_bodies,
            "invalid_ceiling": self._invalid_limit,
            "known_route_hashes": len(self._route_hashes),
            "active_route_buckets": len(self._route_buckets),
            "global_429": self._metrics["global_429"],
            "shared_429": self._metrics["shared_429"],
            "route_429": self._metrics["route_429"],
            "learned_route_responses": self._metrics["learned_route_responses"],
            "changed_bucket_hashes": self._metrics["changed_bucket_hashes"],
            "status_counts": {str(key): value for key, value in self._status_counts.items()},
            "closed": self._closed,
        }

    def _cleanup_locked(self, now: float) -> None:
        global_cutoff = now - self._global_window
        while self._global_admissions and self._global_admissions[0] <= global_cutoff:
            self._global_admissions.popleft()
        invalid_cutoff = now - self._invalid_window
        while self._invalid_responses and self._invalid_responses[0] <= invalid_cutoff:
            self._invalid_responses.popleft()
        for bucket in self._route_buckets.values():
            self._cleanup_route_admissions_locked(bucket, now)

        stale_before = now - self._route_ttl
        stale_keys = [
            key
            for key, bucket in self._route_buckets.items()
            if bucket.last_seen < stale_before and not self._bucket_is_protected(bucket, now)
        ]
        for key in stale_keys:
            self._route_buckets.pop(key, None)
        stale_routes = [
            signature
            for signature, mapping in self._route_hashes.items()
            if mapping.last_seen < stale_before
            and not any(
                identity == mapping.bucket_hash and self._bucket_is_protected(bucket, now)
                for (identity, _major), bucket in self._route_buckets.items()
            )
        ]
        for signature in stale_routes:
            self._route_hashes.pop(signature, None)

        overflow = len(self._route_buckets) - self._max_route_buckets
        if overflow > 0:
            oldest = sorted(
                (
                    key
                    for key, bucket in self._route_buckets.items()
                    if not self._bucket_is_protected(bucket, now)
                ),
                key=lambda key: self._route_buckets[key].last_seen,
            )
            for key in oldest[:overflow]:
                self._route_buckets.pop(key, None)
        mapping_limit = max(64, self._max_route_buckets)
        if len(self._route_hashes) > mapping_limit:
            oldest_routes = sorted(
                (
                    signature
                    for signature, mapping in self._route_hashes.items()
                    if not any(
                        identity == mapping.bucket_hash and self._bucket_is_protected(bucket, now)
                        for (identity, _major), bucket in self._route_buckets.items()
                    )
                ),
                key=lambda signature: self._route_hashes[signature].last_seen,
            )
            for signature in oldest_routes[: len(self._route_hashes) - mapping_limit]:
                self._route_hashes.pop(signature, None)

    @staticmethod
    def _bucket_is_protected(bucket: _RouteBucket, now: float) -> bool:
        return (
            bucket.in_flight > 0
            or bucket.cooldown_until > now
            or bucket.next_at > now
            or bucket.reset_at > now
        )

    def _global_wait_locked(self, now: float) -> float:
        if len(self._global_admissions) < self._global_limit:
            return 0.0
        return max(0.0, self._global_admissions[0] + self._global_window - now)

    def _invalid_wait_locked(self, now: float) -> float | None:
        used = len(self._invalid_responses) + self._invalid_in_flight
        if used < self._invalid_limit:
            return 0.0
        if self._invalid_responses:
            return max(0.0, self._invalid_responses[0] + self._invalid_window - now)
        return None

    def _route_wait_locked(self, bucket: _RouteBucket, now: float) -> float:
        self._cleanup_route_admissions_locked(bucket, now)
        wait = max(0.0, bucket.next_at - now, bucket.cooldown_until - now)
        if bucket.limit is None or bucket.window is None:
            return wait
        integer_budget = math.floor(bucket.limit * self._safety_ratio)
        if integer_budget < 1 or len(bucket.admissions) < integer_budget:
            return wait
        oldest = min(bucket.admissions.values())
        return max(wait, oldest + bucket.window - now, 0.0)

    @staticmethod
    def _cleanup_route_admissions_locked(bucket: _RouteBucket, now: float) -> None:
        if bucket.window is None:
            if bucket.in_flight == 0 and (bucket.limit is None or bucket.interval is None):
                bucket.admissions.clear()
            return
        cutoff = now - bucket.window
        expired = [
            admission_id
            for admission_id, admitted_at in bucket.admissions.items()
            if admitted_at <= cutoff
        ]
        for admission_id in expired:
            bucket.admissions.pop(admission_id, None)

    def _bucket_for_route_locked(
        self,
        route: _Route,
        now: float,
    ) -> tuple[tuple[str, tuple[str, ...]], _RouteBucket]:
        mapping = self._route_hashes.get(route.signature)
        identity = route.signature if mapping is None else mapping.bucket_hash
        if mapping is not None:
            mapping.last_seen = now
        key = (identity, route.major_parameters)
        bucket = self._route_buckets.get(key)
        if bucket is None:
            self._make_route_bucket_room_locked(now)
            bucket = _RouteBucket(last_seen=now)
            self._route_buckets[key] = bucket
        return key, bucket

    def _make_route_bucket_room_locked(self, now: float) -> None:
        if len(self._route_buckets) < self._max_route_buckets:
            return
        evictable = [
            key
            for key, bucket in self._route_buckets.items()
            if not self._bucket_is_protected(bucket, now)
        ]
        if evictable:
            oldest = min(evictable, key=lambda key: self._route_buckets[key].last_seen)
            self._route_buckets.pop(oldest, None)
            return
        hard_limit = self._max_route_buckets + self._global_limit
        if len(self._route_buckets) >= hard_limit:
            self._metrics["route_capacity_rejections"] += 1
            raise DiscordHttpRouteCapacityExceeded()

    def _response_bucket_locked(
        self,
        route: _Route,
        context: SimpleNamespace,
        headers: Mapping[str, Any],
        now: float,
        *,
        learn: bool,
    ) -> tuple[tuple[str, tuple[str, ...]], _RouteBucket]:
        bucket_hash = _header(headers, "X-RateLimit-Bucket")
        old_mapping = self._route_hashes.get(route.signature)
        previous_key = getattr(context, "copilotd_discord_bucket_key", None)
        previous_bucket = self._route_buckets.get(previous_key)
        if bucket_hash:
            hash_text = str(bucket_hash)
            if old_mapping is not None and old_mapping.bucket_hash != hash_text:
                self._metrics["changed_bucket_hashes"] += 1
            self._route_hashes[route.signature] = _RouteMapping(hash_text, now)
        key, bucket = self._bucket_for_route_locked(route, now)
        admitted_at = context.copilotd_discord_admitted_at
        admission_id = getattr(context, "copilotd_discord_route_admission_id", None)
        if key != context.copilotd_discord_bucket_key and admitted_at is not None:
            if old_mapping is None and previous_bucket is not None:
                bucket.admissions.update(previous_bucket.admissions)
            elif admission_id is not None:
                bucket.admissions[int(admission_id)] = float(admitted_at)
            admission_times = [admitted_at, *bucket.admissions.values()]
            if bucket.last_admitted is not None:
                admission_times.append(bucket.last_admitted)
            bucket.last_admitted = max(admission_times)
            bucket.last_seen = now

        if learn:
            limit = _positive_float(_header(headers, "X-RateLimit-Limit"))
            remaining = _nonnegative_float(_header(headers, "X-RateLimit-Remaining"))
            reset_after = _reset_after(headers, now, self._wall_clock())
            if limit is not None:
                bucket.limit = limit
            if reset_after is not None:
                new_reset_at = now + reset_after
                reset_tolerance = max(0.01, reset_after * 0.05)
                new_window = bucket.window is None or now >= bucket.reset_at - reset_tolerance
                if (
                    remaining is not None
                    and bucket.remaining is not None
                    and remaining > bucket.remaining
                ):
                    new_window = True
                if remaining is None and new_reset_at > bucket.reset_at + reset_tolerance:
                    new_window = True
                if new_window:
                    bucket.window = reset_after
                    bucket.reset_at = new_reset_at
                else:
                    bucket.reset_at = max(bucket.reset_at, new_reset_at)
            bucket.remaining = remaining
            if bucket.limit is not None and bucket.window is not None:
                fractional_budget = bucket.limit * self._safety_ratio
                integer_budget = math.floor(fractional_budget)
                usable_budget = float(integer_budget) if integer_budget >= 1 else fractional_budget
                bucket.interval = bucket.window / usable_budget
                if bucket.last_admitted is not None:
                    bucket.next_at = bucket.last_admitted + bucket.interval
                if (
                    remaining is not None
                    and bucket.reset_at > now
                    and bucket.limit - remaining >= usable_budget
                ):
                    bucket.cooldown_until = max(bucket.cooldown_until, bucket.reset_at)
                self._metrics["learned_route_responses"] += 1
        return key, bucket

    def _release_reservation_locked(self, context: SimpleNamespace) -> None:
        if getattr(context, "copilotd_discord_released", False):
            return
        context.copilotd_discord_released = True
        if getattr(context, "copilotd_discord_admitted_at", None) is None:
            return
        self._invalid_in_flight = max(0, self._invalid_in_flight - 1)
        bucket_key = getattr(context, "copilotd_discord_bucket_key", None)
        bucket = self._route_buckets.get(bucket_key)
        if bucket is not None:
            bucket.in_flight = max(0, bucket.in_flight - 1)
            if bucket.limit is None or bucket.window is None:
                admission_id = getattr(
                    context,
                    "copilotd_discord_route_admission_id",
                    None,
                )
                if admission_id is not None:
                    bucket.admissions.pop(int(admission_id), None)

    def _apply_429_cooldown_locked(
        self,
        bucket_key: tuple[str, tuple[str, ...]],
        bucket: _RouteBucket,
        cooldown_until: float,
        *,
        is_global: bool,
    ) -> None:
        if is_global:
            self._global_cooldown_until = max(
                self._global_cooldown_until,
                cooldown_until,
            )
            return
        bucket.cooldown_until = max(bucket.cooldown_until, cooldown_until)
        self._route_buckets[bucket_key] = bucket


def append_trace_callbacks(
    mandatory: aiohttp.TraceConfig,
    observer: aiohttp.TraceConfig,
) -> None:
    """Compose observer callbacks without replacing mandatory admission."""

    if getattr(mandatory, "_copilotd_discord_http_limiter", None) is None:
        raise ValueError("target trace is not the mandatory Discord limiter")
    for name in _TRACE_SIGNALS:
        target_signal = getattr(mandatory, name)
        for callback in getattr(observer, name):
            forwarded = _observer_callback(observer, callback)
            if name == "on_request_start":
                limiter = mandatory._copilotd_discord_http_limiter
                target_signal.insert(target_signal.index(limiter.on_request_start), forwarded)
            else:
                target_signal.append(forwarded)


def _observer_callback(
    observer: aiohttp.TraceConfig,
    callback: Callable[..., Any],
) -> Callable[..., Any]:
    async def forward(session: Any, context: SimpleNamespace, params: Any) -> None:
        contexts = getattr(context, "copilotd_observer_contexts", None)
        if contexts is None:
            contexts = {}
            context.copilotd_observer_contexts = contexts
        observer_key = id(observer)
        observer_context = contexts.get(observer_key)
        if observer_context is None:
            observer_context = observer.trace_config_ctx(
                trace_request_ctx=getattr(context, "trace_request_ctx", None)
            )
            contexts[observer_key] = observer_context
        result = callback(session, observer_context, params)
        if inspect.isawaitable(result):
            await result

    return forward


async def probe_discord_identity(
    token: str,
    *,
    limiter: DiscordHttpRateLimiter | None = None,
) -> dict[str, Any]:
    """Probe /users/@me through the same mandatory transport admission layer."""

    owns_limiter = limiter is None
    effective_limiter = limiter or DiscordHttpRateLimiter()
    timeout = aiohttp.ClientTimeout(total=15)
    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            trace_configs=[effective_limiter.trace_config()],
        ) as session:
            async with session.get(
                "https://discord.com/api/v10/users/@me",
                headers={
                    "Authorization": f"Bot {token}",
                    "User-Agent": "copilotd-setup/0.1",
                },
            ) as response:
                payload = await _cached_json_body(response)
                if response.status != 200:
                    raise RuntimeError(f"Discord token rejected with HTTP {response.status}")
    finally:
        if owns_limiter:
            await effective_limiter.close()
    return {
        "id": str(payload["id"]),
        "username": str(payload.get("username", payload["id"])),
    }


def _disable_aiohttp_transparent_retries(session: aiohttp.ClientSession | None) -> None:
    if session is None:
        return
    retry_connection = getattr(session, "_retry_connection", None)
    if not isinstance(retry_connection, bool):
        aiohttp_version = getattr(aiohttp, "__version__", "unknown")
        raise RuntimeError(
            "aiohttp "
            f"{aiohttp_version} cannot disable transparent connection retries; "
            "aiohttp >= 3.11,<4 is required"
        )
    # aiohttp 3.11-3.x computes its reused-connection retry flag after this signal.
    session._retry_connection = False


def _discord_rest_route(method: str, url: Any) -> _Route | None:
    host = str(getattr(url, "host", "") or "").lower()
    path = str(getattr(url, "path", "") or "")
    if host not in _DISCORD_API_HOSTS:
        return None
    pieces = [piece for piece in path.split("/") if piece]
    if not pieces or pieces[0] != "api":
        return None
    pieces = pieces[1:]
    if pieces and len(pieces[0]) > 1 and pieces[0][0] == "v" and pieces[0][1:].isdigit():
        pieces = pieces[1:]

    majors: list[str] = []
    major_positions: dict[int, str] = {}
    for resource, label in (("channels", "channel"), ("guilds", "guild")):
        try:
            index = pieces.index(resource)
        except ValueError:
            continue
        if index + 1 < len(pieces):
            majors.append(f"{label}:{pieces[index + 1]}")
            major_positions[index + 1] = f"{{{label}_id}}"
    for resource, label in (("webhooks", "webhook"), ("interactions", "interaction")):
        try:
            index = pieces.index(resource)
        except ValueError:
            continue
        if index + 1 >= len(pieces):
            continue
        identity = pieces[index + 1]
        major_positions[index + 1] = f"{{{label}_id}}"
        token = pieces[index + 2] if index + 2 < len(pieces) else ""
        if token:
            token_fingerprint = hashlib.sha256(token.encode("utf-8")).hexdigest()[:20]
            majors.append(f"{label}:{identity}:{token_fingerprint}")
            major_positions[index + 2] = "{token}"
        else:
            majors.append(f"{label}:{identity}")

    normalized: list[str] = []
    for index, piece in enumerate(pieces):
        if index in major_positions:
            normalized.append(major_positions[index])
        elif piece.isdigit():
            normalized.append("{id}")
        elif index > 0 and pieces[index - 1] == "reactions" and piece != "@me":
            normalized.append("{emoji}")
        else:
            normalized.append(piece)
    signature = f"{method.upper()} /{'/'.join(normalized)}"
    global_exempt = (
        len(pieces) >= 4 and pieces[0] == "interactions" and pieces[3] == "callback"
    ) or (len(pieces) >= 3 and pieces[0] == "webhooks")
    return _Route(signature, tuple(sorted(majors)), global_exempt)


async def _cached_json_body(response: aiohttp.ClientResponse) -> Mapping[str, Any]:
    try:
        raw = await response.read()
        payload = json.loads(raw.decode(response.charset or "utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _header(headers: Mapping[str, Any], name: str) -> Any:
    direct = headers.get(name)
    if direct is not None:
        return direct
    lower = name.lower()
    direct = headers.get(lower)
    if direct is not None:
        return direct
    return next((value for key, value in headers.items() if str(key).lower() == lower), None)


def _retry_after(
    headers: Mapping[str, Any],
    body: Mapping[str, Any],
    wall_now: float,
) -> float:
    body_retry_after = _nonnegative_float(body.get("retry_after"))
    header_retry_after = _header_retry_after(headers, wall_now)
    candidates = [
        candidate for candidate in (body_retry_after, header_retry_after) if candidate is not None
    ]
    return max(candidates, default=1.0)


def _header_retry_after(
    headers: Mapping[str, Any],
    wall_now: float,
) -> float | None:
    candidates: list[float] = []
    for name in ("Retry-After", "X-RateLimit-Reset-After"):
        parsed = _nonnegative_float(_header(headers, name))
        if parsed is not None:
            candidates.append(parsed)
    reset = _nonnegative_float(_header(headers, "X-RateLimit-Reset"))
    if reset is not None:
        candidates.append(max(0.0, reset - wall_now))
    return max(candidates) if candidates else None


def _reset_after(
    headers: Mapping[str, Any],
    _now: float,
    wall_now: float,
) -> float | None:
    reset_after = _positive_float(_header(headers, "X-RateLimit-Reset-After"))
    if reset_after is not None:
        return reset_after
    reset = _positive_float(_header(headers, "X-RateLimit-Reset"))
    if reset is None:
        return None
    delta = reset - wall_now
    return delta if delta > 0 else None


def _positive_float(value: Any) -> float | None:
    parsed = _nonnegative_float(value)
    return parsed if parsed is not None and parsed > 0 else None


def _nonnegative_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _truthy(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes"}
