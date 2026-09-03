import asyncio
import json
import socket
import time
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from yarl import URL

from copilotd.config import Settings
from copilotd.discord_app import CopilotDiscordBot
from copilotd.discord_http_limiter import (
    DiscordHttpRateLimiter,
    DiscordHttpRateLimiterClosed,
    DiscordHttpRedirectBlocked,
    probe_discord_identity,
)


class _Response:
    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self.charset = "utf-8"
        self.url = URL("https://discord.com/api/v10/test")
        self._body = json.dumps(body or {}).encode()
        self.reads = 0

    async def read(self) -> bytes:
        self.reads += 1
        return self._body


class _BlockedResponse(_Response):
    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str],
        body: dict[str, Any],
        read_started: asyncio.Event,
        allow_read: asyncio.Event,
    ) -> None:
        super().__init__(status, headers=headers, body=body)
        self._read_started = read_started
        self._allow_read = allow_read

    async def read(self) -> bytes:
        self.reads += 1
        self._read_started.set()
        await self._allow_read.wait()
        return self._body


class _LocalResolver:
    def __init__(self, host: str) -> None:
        self._host = host

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = socket.AF_INET,
    ) -> list[dict[str, Any]]:
        return [
            {
                "hostname": host,
                "host": self._host,
                "port": port,
                "family": family,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
        ]

    async def close(self) -> None:
        return None


async def _admit(
    limiter: DiscordHttpRateLimiter,
    url: str = "https://discord.com/api/v10/channels/1/messages",
    *,
    method: str = "POST",
) -> SimpleNamespace:
    context = SimpleNamespace()
    await limiter.on_request_start(
        None,
        context,
        SimpleNamespace(method=method, url=URL(url)),
    )
    return context


async def _respond(
    limiter: DiscordHttpRateLimiter,
    context: SimpleNamespace,
    status: int,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> _Response:
    response = _Response(status, headers=headers, body=body)
    await limiter.on_request_end(
        None,
        context,
        SimpleNamespace(response=response),
    )
    return response


async def _fail(limiter: DiscordHttpRateLimiter, context: SimpleNamespace) -> None:
    await limiter.on_request_exception(None, context, SimpleNamespace(exception=OSError()))


@pytest.mark.asyncio
async def test_global_rolling_ceiling_delays_attempt_41_past_first_second() -> None:
    limiter = DiscordHttpRateLimiter()
    admitted_at: list[float] = []
    contexts: list[SimpleNamespace] = []

    async def attempt(index: int) -> None:
        context = await _admit(
            limiter,
            f"https://discord.com/api/v10/channels/{index + 1}/messages",
        )
        contexts.append(context)
        admitted_at.append(time.monotonic())

    await asyncio.gather(*(attempt(index) for index in range(41)))

    assert len(admitted_at) == 41
    assert sorted(admitted_at)[40] - min(admitted_at) >= 0.98
    for context in contexts:
        await _fail(limiter, context)


@pytest.mark.asyncio
async def test_gateway_websocket_and_cdn_downloads_are_not_rest_admissions() -> None:
    limiter = DiscordHttpRateLimiter()

    gateway = await _admit(limiter, "https://gateway.discord.gg/?v=10")
    cdn = await _admit(limiter, "https://cdn.discordapp.com/attachments/1/2/file.txt")

    assert gateway.copilotd_discord_limited is False
    assert cdn.copilotd_discord_limited is False
    assert limiter.snapshot()["physical_attempts"] == 0


@pytest.mark.asyncio
async def test_learned_bucket_uses_80_percent_pacing_and_isolates_major_parameters() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        200,
        headers={
            "X-RateLimit-Bucket": "messages",
            "X-RateLimit-Limit": "5",
            "X-RateLimit-Remaining": "4",
            "X-RateLimit-Reset-After": "0.08",
        },
    )

    same_started = time.monotonic()
    same_task = asyncio.create_task(_admit(limiter))
    other = await _admit(
        limiter,
        "https://discord.com/api/v10/channels/2/messages",
    )
    other_elapsed = time.monotonic() - same_started
    same = await same_task
    same_elapsed = time.monotonic() - same_started

    assert other_elapsed < 0.015
    assert same_elapsed >= 0.018
    await _fail(limiter, same)
    await _fail(limiter, other)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "concurrent_attempts", "window", "minimum_wait"),
    [
        (5, 4, 0.08, 0.07),
        (2, 1, 0.05, 0.045),
        (1, 1, 0.04, 0.047),
    ],
)
async def test_slow_concurrent_attempts_count_in_learned_local_window(
    limit: int,
    concurrent_attempts: int,
    window: float,
    minimum_wait: float,
) -> None:
    limiter = DiscordHttpRateLimiter()
    admitted = [await _admit(limiter) for _ in range(concurrent_attempts)]
    await _respond(
        limiter,
        admitted[0],
        200,
        headers={
            "X-RateLimit-Bucket": f"slow-{limit}",
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(max(0, limit - 1)),
            "X-RateLimit-Reset-After": str(window),
        },
    )

    started = time.monotonic()
    next_attempt = asyncio.create_task(_admit(limiter))
    await asyncio.sleep(min(0.01, minimum_wait / 2))
    assert not next_attempt.done()
    next_context = await asyncio.wait_for(next_attempt, timeout=window * 3 + 0.1)
    elapsed = time.monotonic() - started

    assert elapsed >= minimum_wait
    for context in admitted[1:]:
        await _fail(limiter, context)
    await _fail(limiter, next_context)


@pytest.mark.asyncio
async def test_limit_one_keeps_fractional_safety_margin() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        200,
        headers={
            "X-RateLimit-Bucket": "single",
            "X-RateLimit-Limit": "1",
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset-After": "0.04",
        },
    )

    started = time.monotonic()
    second = await _admit(limiter)

    assert time.monotonic() - started >= 0.047
    await _fail(limiter, second)


@pytest.mark.asyncio
async def test_changed_bucket_hash_is_learned() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        200,
        headers={
            "X-RateLimit-Bucket": "old",
            "X-RateLimit-Limit": "10",
            "X-RateLimit-Remaining": "9",
            "X-RateLimit-Reset-After": "0.01",
        },
    )
    second = await _admit(limiter)
    await _respond(
        limiter,
        second,
        200,
        headers={
            "X-RateLimit-Bucket": "new",
            "X-RateLimit-Limit": "10",
            "X-RateLimit-Remaining": "8",
            "X-RateLimit-Reset-After": "0.01",
        },
    )

    assert limiter.snapshot()["changed_bucket_hashes"] == 1
    assert limiter.snapshot()["known_route_hashes"] == 1


@pytest.mark.asyncio
async def test_404_response_learns_route_bucket_headers() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        404,
        headers={
            "X-RateLimit-Bucket": "not-found",
            "X-RateLimit-Limit": "5",
            "X-RateLimit-Remaining": "4",
            "X-RateLimit-Reset-After": "0.08",
        },
    )

    started = time.monotonic()
    second = await _admit(limiter)

    assert time.monotonic() - started >= 0.018
    assert limiter.snapshot()["learned_route_responses"] == 1
    await _fail(limiter, second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope", "use_other_route", "metric"),
    [
        ("user", False, "route_429"),
        ("shared", False, "shared_429"),
        ("global", True, "global_429"),
    ],
)
async def test_429_scope_applies_exact_retry_after_cooldown(
    scope: str,
    use_other_route: bool,
    metric: str,
) -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    response = await _respond(
        limiter,
        first,
        429,
        headers={
            "X-RateLimit-Bucket": "limited",
            "X-RateLimit-Scope": scope,
        },
        body={"retry_after": 0.04, "global": scope == "global"},
    )
    url = (
        "https://discord.com/api/v10/gateway/bot"
        if use_other_route
        else "https://discord.com/api/v10/channels/1/messages"
    )

    started = time.monotonic()
    second = await _admit(limiter, url)
    elapsed = time.monotonic() - started

    assert 0.035 <= elapsed < 0.15
    assert response.reads == 1
    assert limiter.snapshot()[metric] == 1
    assert limiter.snapshot()["invalid_completed"] == (0 if scope == "shared" else 1)
    await _fail(limiter, second)


@pytest.mark.asyncio
async def test_429_header_cooldown_blocks_admission_while_body_read_is_blocked() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    read_started = asyncio.Event()
    allow_read = asyncio.Event()
    response = _BlockedResponse(
        429,
        headers={
            "Retry-After": "0.1",
            "X-RateLimit-Scope": "global",
        },
        body={"retry_after": 0.01, "global": True},
        read_started=read_started,
        allow_read=allow_read,
    )
    response_task = asyncio.create_task(
        limiter.on_request_end(None, first, SimpleNamespace(response=response))
    )
    await read_started.wait()

    blocked = asyncio.create_task(_admit(limiter, "https://discord.com/api/v10/gateway/bot"))
    await asyncio.sleep(0.02)
    assert not blocked.done()
    assert limiter.snapshot()["global_cooldown_seconds"] > 0

    allow_read.set()
    await response_task
    await asyncio.sleep(0.03)
    assert not blocked.done()
    second = await asyncio.wait_for(blocked, timeout=0.15)

    assert response.reads == 1
    await _fail(limiter, second)


@pytest.mark.asyncio
async def test_body_only_global_429_blocks_until_scope_and_cooldown_are_installed() -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    read_started = asyncio.Event()
    allow_read = asyncio.Event()
    response = _BlockedResponse(
        429,
        headers={},
        body={"retry_after": 0.08, "global": True},
        read_started=read_started,
        allow_read=allow_read,
    )
    response_task = asyncio.create_task(
        limiter.on_request_end(None, first, SimpleNamespace(response=response))
    )
    await read_started.wait()

    blocked = asyncio.create_task(_admit(limiter, "https://discord.com/api/v10/gateway/bot"))
    await asyncio.sleep(0.01)
    assert not blocked.done()
    assert limiter.snapshot()["unresolved_429_bodies"] == 1
    assert limiter.snapshot()["global_cooldown_seconds"] == 0

    allow_read.set()
    await response_task
    await asyncio.sleep(0.02)
    assert not blocked.done()
    second = await asyncio.wait_for(blocked, timeout=0.15)
    await _fail(limiter, second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://discord.com/api/v10/interactions/2/token/callback",
        "https://discord.com/api/v10/webhooks/3/token",
    ],
)
async def test_interaction_routes_bypass_bot_global_window_and_cooldown(
    url: str,
) -> None:
    limiter = DiscordHttpRateLimiter(global_limit=1, global_window_seconds=0.08)
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        429,
        headers={"X-RateLimit-Scope": "global"},
        body={"retry_after": 0.08, "global": True},
    )
    normal = asyncio.create_task(_admit(limiter, "https://discord.com/api/v10/channels/2/messages"))
    await asyncio.sleep(0.01)
    assert not normal.done()

    started = time.monotonic()
    interaction = await asyncio.wait_for(_admit(limiter, url), timeout=0.03)
    assert time.monotonic() - started < 0.03
    await _fail(limiter, interaction)

    second = await asyncio.wait_for(normal, timeout=0.15)
    await _fail(limiter, second)


@pytest.mark.asyncio
async def test_interaction_route_still_obeys_learned_bucket_and_invalid_guard() -> None:
    url = "https://discord.com/api/v10/interactions/2/token/callback"
    limiter = DiscordHttpRateLimiter(invalid_limit=1)
    first = await _admit(limiter, url)
    invalid_blocked = asyncio.create_task(_admit(limiter, url))
    await asyncio.sleep(0.01)
    assert not invalid_blocked.done()
    await _respond(
        limiter,
        first,
        200,
        headers={
            "X-RateLimit-Bucket": "interaction-callback",
            "X-RateLimit-Limit": "2",
            "X-RateLimit-Remaining": "1",
            "X-RateLimit-Reset-After": "0.05",
        },
    )

    started = time.monotonic()
    second = await asyncio.wait_for(invalid_blocked, timeout=0.12)
    assert time.monotonic() - started >= 0.045
    await _fail(limiter, second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "window", "minimum"),
    [
        (2, 0.05, 0.045),
        (5, 0.08, 0.018),
        (1, 0.04, 0.047),
    ],
)
async def test_learned_route_uses_integer_safe_eighty_percent_budget(
    limit: int,
    window: float,
    minimum: float,
) -> None:
    limiter = DiscordHttpRateLimiter()
    first = await _admit(limiter)
    await _respond(
        limiter,
        first,
        200,
        headers={
            "X-RateLimit-Bucket": f"limit-{limit}",
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(limit - 1),
            "X-RateLimit-Reset-After": str(window),
        },
    )

    started = time.monotonic()
    second = await _admit(limiter)
    assert time.monotonic() - started >= minimum
    await _fail(limiter, second)


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_by", ["cooldown", "invalid"])
async def test_close_wakes_waiters_with_fixed_exception(blocked_by: str) -> None:
    if blocked_by == "invalid":
        limiter = DiscordHttpRateLimiter(invalid_limit=1)
        held = await _admit(limiter)
    else:
        limiter = DiscordHttpRateLimiter()
        first = await _admit(limiter)
        await _respond(
            limiter,
            first,
            429,
            headers={"X-RateLimit-Scope": "global"},
            body={"retry_after": 30, "global": True},
        )
        held = None
    waiter = asyncio.create_task(_admit(limiter, "https://discord.com/api/v10/channels/2/messages"))
    await asyncio.sleep(0.01)
    assert not waiter.done()

    await limiter.close()
    with pytest.raises(
        DiscordHttpRateLimiterClosed,
        match="Discord HTTP rate limiter is closed",
    ):
        await asyncio.wait_for(waiter, timeout=0.05)
    if held is not None:
        await _fail(limiter, held)


@pytest.mark.asyncio
async def test_invalid_guard_counts_responses_and_in_flight_reservations() -> None:
    limiter = DiscordHttpRateLimiter(invalid_limit=2, invalid_window_seconds=0.05)
    first = await _admit(limiter)
    second = await _admit(limiter, "https://discord.com/api/v10/channels/2/messages")
    blocked = asyncio.create_task(
        _admit(limiter, "https://discord.com/api/v10/channels/3/messages")
    )
    await asyncio.sleep(0.005)
    assert not blocked.done()

    await _respond(limiter, first, 200)
    third = await asyncio.wait_for(blocked, timeout=0.1)
    await _respond(limiter, second, 401)
    await _respond(limiter, third, 403)

    started = time.monotonic()
    fourth = await _admit(limiter, "https://discord.com/api/v10/channels/4/messages")
    assert time.monotonic() - started >= 0.045
    await _fail(limiter, fourth)


@pytest.mark.asyncio
async def test_request_exception_releases_invalid_reservation() -> None:
    limiter = DiscordHttpRateLimiter(invalid_limit=1)
    failed = await _admit(limiter)
    await _fail(limiter, failed)

    started = time.monotonic()
    next_attempt = await _admit(limiter)

    assert time.monotonic() - started < 0.02
    assert limiter.snapshot()["invalid_in_flight"] == 1
    await _fail(limiter, next_attempt)


@pytest.mark.asyncio
async def test_route_cache_never_evicts_active_pacing_or_reset_windows() -> None:
    now = [100.0]
    limiter = DiscordHttpRateLimiter(
        max_route_buckets=2,
        clock=lambda: now[0],
        wall_clock=lambda: 1_000.0,
    )
    protected_urls = [
        "https://discord.com/api/v10/channels/1/messages",
        "https://discord.com/api/v10/channels/2/messages",
    ]
    for url in protected_urls:
        context = await _admit(limiter, url)
        await _respond(
            limiter,
            context,
            200,
            headers={
                "X-RateLimit-Bucket": "protected",
                "X-RateLimit-Limit": "1",
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset-After": "60",
            },
        )

    third = await _admit(
        limiter,
        "https://discord.com/api/v10/channels/3/messages",
    )
    assert limiter.snapshot()["active_route_buckets"] == 3
    blocked = asyncio.create_task(_admit(limiter, protected_urls[0]))
    await asyncio.sleep(0)
    assert not blocked.done()

    now[0] = 200.0
    async with limiter._condition:
        limiter._condition.notify_all()
    admitted = await asyncio.wait_for(blocked, timeout=0.1)
    await _fail(limiter, third)
    await _fail(limiter, admitted)


@pytest.mark.asyncio
async def test_optional_trace_observer_keeps_its_own_context() -> None:
    seen: list[str] = []
    observer = aiohttp.TraceConfig(
        trace_config_ctx_factory=lambda **_kwargs: SimpleNamespace(marker="observer")
    )

    async def observe_start(_session, context, _params) -> None:
        seen.append(f"start:{context.marker}")

    async def observe_exception(_session, context, _params) -> None:
        seen.append(f"exception:{context.marker}")

    async def observe_end(_session, context, _params) -> None:
        seen.append(f"end:{context.marker}")

    observer.on_request_start.append(observe_start)
    observer.on_request_exception.append(observe_exception)
    observer.on_request_end.append(observe_end)
    limiter = DiscordHttpRateLimiter()
    trace = limiter.trace_config(observer)
    context = trace.trace_config_ctx(trace_request_ctx={"test": True})
    params = SimpleNamespace(
        method="GET",
        url=URL("https://discord.com/api/v10/gateway/bot"),
    )
    for callback in trace.on_request_start:
        await callback(None, context, params)
    exception = SimpleNamespace(exception=OSError())
    for callback in trace.on_request_exception:
        await callback(None, context, exception)

    completed = trace.trace_config_ctx(trace_request_ctx={"test": True})
    for callback in trace.on_request_start:
        await callback(None, completed, params)
    response = SimpleNamespace(response=_Response(200))
    for callback in trace.on_request_end:
        await callback(None, completed, response)

    assert seen == [
        "start:observer",
        "exception:observer",
        "start:observer",
        "end:observer",
    ]
    assert limiter.snapshot()["invalid_in_flight"] == 0


@pytest.mark.asyncio
async def test_raising_observer_start_does_not_leak_invalid_reservation() -> None:
    starts = 0
    observer = aiohttp.TraceConfig()

    async def observe(_session, _context, _params) -> None:
        nonlocal starts
        starts += 1
        if starts == 1:
            raise RuntimeError("observer failed")

    observer.on_request_start.append(observe)
    limiter = DiscordHttpRateLimiter(invalid_limit=1)
    trace = limiter.trace_config(observer)
    params = SimpleNamespace(
        method="GET",
        url=URL("https://discord.com/api/v10/gateway/bot"),
    )

    first = trace.trace_config_ctx(trace_request_ctx=None)
    with pytest.raises(RuntimeError, match="observer failed"):
        for callback in trace.on_request_start:
            await callback(SimpleNamespace(_retry_connection=True), first, params)

    assert limiter.snapshot()["physical_attempts"] == 0
    assert limiter.snapshot()["invalid_in_flight"] == 0

    session = SimpleNamespace(_retry_connection=True)
    second = trace.trace_config_ctx(trace_request_ctx=None)
    for callback in trace.on_request_start:
        await asyncio.wait_for(callback(session, second, params), timeout=0.02)

    assert session._retry_connection is False
    assert limiter.snapshot()["invalid_in_flight"] == 1
    await _fail(limiter, second)


@pytest.mark.asyncio
async def test_unsupported_aiohttp_session_fails_before_admission() -> None:
    limiter = DiscordHttpRateLimiter()

    with pytest.raises(RuntimeError, match=r"aiohttp >= 3\.11,<4 is required"):
        await limiter.on_request_start(
            SimpleNamespace(),
            SimpleNamespace(),
            SimpleNamespace(
                method="GET",
                url=URL("https://discord.com/api/v10/users/@me"),
            ),
        )

    assert limiter.snapshot()["physical_attempts"] == 0
    assert limiter.snapshot()["invalid_in_flight"] == 0


@pytest.mark.asyncio
async def test_reused_connection_failure_has_one_admission_per_server_request() -> None:
    request_transports: list[asyncio.Transport | None] = []

    async def handle(request: web.Request) -> web.Response:
        request_transports.append(request.transport)
        if len(request_transports) == 2:
            assert request.transport is not None
            request.transport.abort()
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_get("/api/v10/test", handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]

    limiter = DiscordHttpRateLimiter()
    connector = aiohttp.TCPConnector(resolver=_LocalResolver("127.0.0.1"))
    try:
        async with aiohttp.ClientSession(
            connector=connector,
            trace_configs=[limiter.trace_config()],
        ) as session:
            url = f"http://discord.com:{port}/api/v10/test"
            async with session.get(url) as response:
                assert response.status == 200
            assert session._retry_connection is False

            with pytest.raises(aiohttp.ClientConnectionError):
                async with session.get(url):
                    pass
    finally:
        await runner.cleanup()

    assert len(request_transports) == 2
    assert request_transports[0] is request_transports[1]
    assert limiter.snapshot()["physical_attempts"] == len(request_transports)
    assert limiter.snapshot()["request_exceptions"] == 1


@pytest.mark.asyncio
async def test_discord_rest_redirect_fails_closed_before_following() -> None:
    requests: list[str] = []

    async def redirect(request: web.Request) -> web.Response:
        requests.append(request.path)
        raise web.HTTPFound("/api/v10/final")

    async def final(request: web.Request) -> web.Response:
        requests.append(request.path)
        return web.json_response({"unexpected": True})

    app = web.Application()
    app.router.add_get("/api/v10/start", redirect)
    app.router.add_get("/api/v10/final", final)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]

    limiter = DiscordHttpRateLimiter()
    connector = aiohttp.TCPConnector(resolver=_LocalResolver("127.0.0.1"))
    try:
        async with aiohttp.ClientSession(
            connector=connector,
            trace_configs=[limiter.trace_config()],
        ) as session:
            with pytest.raises(
                DiscordHttpRedirectBlocked,
                match="Discord REST redirects are disabled",
            ):
                await session.get(f"http://discord.com:{port}/api/v10/start")
        snapshot = limiter.snapshot()
        assert requests == ["/api/v10/start"]
        assert snapshot["physical_attempts"] == 1
        assert snapshot["redirects_blocked"] == 1
        assert snapshot["invalid_in_flight"] == 0
    finally:
        await limiter.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_preflight_probe_uses_retry_disabling_limiter_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    class FakeRequestContext:
        def __init__(self, session: Any, trace: aiohttp.TraceConfig) -> None:
            self._session = session
            self._trace = trace
            self._context = trace.trace_config_ctx(trace_request_ctx=None)
            self._response = _Response(
                200,
                body={"id": "123", "username": "copilotd-test"},
            )

        async def __aenter__(self) -> _Response:
            start = SimpleNamespace(
                method="GET",
                url=URL("https://discord.com/api/v10/users/@me"),
            )
            for callback in self._trace.on_request_start:
                await callback(self._session, self._context, start)
            observed["retry_connection"] = self._session._retry_connection
            end = SimpleNamespace(response=self._response)
            for callback in self._trace.on_request_end:
                await callback(self._session, self._context, end)
            return self._response

        async def __aexit__(self, *_args: Any) -> None:
            return None

    class FakeSession:
        def __init__(self, **kwargs: Any) -> None:
            self._retry_connection = True
            self._trace = kwargs["trace_configs"][0]

        async def __aenter__(self) -> "FakeSession":
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        def get(self, *_args: Any, **_kwargs: Any) -> FakeRequestContext:
            return FakeRequestContext(self, self._trace)

    monkeypatch.setattr("copilotd.discord_http_limiter.aiohttp.ClientSession", FakeSession)

    identity = await probe_discord_identity("secret")

    assert identity == {"id": "123", "username": "copilotd-test"}
    assert observed["retry_connection"] is False


@pytest.mark.asyncio
async def test_bot_trace_covers_login_gateway_normal_and_interaction_requests(
    tmp_path,
) -> None:
    observed: list[str] = []
    observer = aiohttp.TraceConfig()

    async def record(_session, _context, params) -> None:
        observed.append(str(params.response.url))

    observer.on_request_end.append(record)
    bot = CopilotDiscordBot(Settings(data_dir=tmp_path), discord_http_trace=observer)
    trace = bot.http.http_trace
    assert trace._copilotd_discord_http_limiter is bot.discord_http_limiter
    assert len(trace.on_request_end) == 2

    urls = [
        "https://discord.com/api/v10/users/@me",
        "https://discord.com/api/v10/oauth2/applications/@me",
        "https://discord.com/api/v10/gateway/bot",
        "https://discord.com/api/v10/channels/1/messages",
        "https://discord.com/api/v10/interactions/2/token/callback",
        "https://discord.com/api/v10/webhooks/3/token",
    ]
    session = SimpleNamespace(_retry_connection=True)
    for url in urls:
        context = SimpleNamespace()
        params = SimpleNamespace(method="GET", url=URL(url))
        for callback in trace.on_request_start:
            await callback(session, context, params)
        await _fail(bot.discord_http_limiter, context)

    assert session._retry_connection is False
    assert bot.discord_http_limiter.snapshot()["physical_attempts"] == len(urls)
