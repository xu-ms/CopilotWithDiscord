import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest

from copilotd.discord_requests import (
    DiscordBackpressure,
    DiscordCoordinatorConfig,
    DiscordDeadlineExceeded,
    DiscordOperation,
    DiscordPriority,
    DiscordRequest,
    DiscordRequestCoordinator,
)


def _request(
    callback,
    *,
    operation: DiscordOperation = DiscordOperation.SEND,
    route: str = "messages.send",
    target: str = "channel:1",
    priority: DiscordPriority = DiscordPriority.FOREGROUND,
    coalesce: str | None = None,
    terminal: bool = False,
    deadline: float | None = None,
) -> DiscordRequest:
    return DiscordRequest(
        operation=operation,
        callback=callback,
        route_key=route,
        target_key=target,
        priority=priority,
        coalesce_key=coalesce,
        terminal=terminal,
        deadline=deadline,
    )


@pytest.mark.asyncio
async def test_deadline_priority_overtakes_background_without_reordering_target() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(requests_per_second=1000, route_requests_per_second=1000)
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    seen: list[str] = []

    async def first() -> str:
        seen.append("first")
        entered.set()
        await release.wait()
        return "first"

    async def record(value: str) -> str:
        seen.append(value)
        return value

    first_task = asyncio.create_task(
        coordinator.execute(
            _request(first, target="channel:blocker", priority=DiscordPriority.BACKGROUND)
        )
    )
    await entered.wait()
    earlier_same_target = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("same-target"),
                target="channel:shared",
                priority=DiscordPriority.BACKGROUND,
            )
        )
    )
    later_same_target = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("same-target-critical"),
                target="channel:shared",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 1,
            )
        )
    )
    critical = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("critical"),
                target="interaction:1",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 1,
            )
        )
    )
    release.set()
    await asyncio.gather(first_task, earlier_same_target, later_same_target, critical)
    await coordinator.close()

    assert seen == ["first", "critical", "same-target", "same-target-critical"]


@pytest.mark.asyncio
async def test_coalescing_keeps_latest_terminal_and_never_downgrades() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(requests_per_second=1000, route_requests_per_second=1000)
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    seen: list[str] = []

    async def blocker() -> None:
        entered.set()
        await release.wait()

    blocker_task = asyncio.create_task(
        coordinator.execute(_request(blocker, target="channel:blocker"))
    )
    await entered.wait()

    async def record(value: str) -> str:
        seen.append(value)
        return value

    transient = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("transient"),
                target="message:1",
                priority=DiscordPriority.BACKGROUND,
                coalesce="message:1:state",
            )
        )
    )
    terminal = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("terminal"),
                target="message:1",
                coalesce="message:1:state",
                terminal=True,
            )
        )
    )
    stale = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: record("stale"),
                target="message:1",
                priority=DiscordPriority.BACKGROUND,
                coalesce="message:1:state",
            )
        )
    )
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(blocker_task, transient, terminal, stale)
    snapshot = coordinator.snapshot()
    await coordinator.close()

    assert results[1:] == ["terminal", "terminal", "terminal"]
    assert seen == ["terminal"]
    assert snapshot["coalesced"] == 1
    assert snapshot["dropped"] == 1


class _RateLimited(Exception):
    def __init__(self, retry_after: float, *, global_: bool = False) -> None:
        self.status = 429
        self.retry_after = retry_after
        self.global_ = global_
        self.response = SimpleNamespace(headers={})


class _HttpError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        self.response = SimpleNamespace(headers={})


@pytest.mark.asyncio
async def test_route_429_does_not_block_other_route_or_consume_transient_budget() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(
            requests_per_second=1000,
            route_requests_per_second=1000,
            transient_attempts=1,
        )
    )
    attempts = 0
    order: list[str] = []

    async def limited() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _RateLimited(0.03)
        order.append("limited")
        return "limited"

    limited_task = asyncio.create_task(
        coordinator.execute(_request(limited, route="reactions", target="message:1"))
    )
    await asyncio.sleep(0.005)
    other = await coordinator.execute(
        _request(lambda: _record(order, "other"), route="messages", target="channel:2")
    )
    limited_result = await limited_task
    snapshot = coordinator.snapshot()
    await coordinator.close()

    assert other == "other"
    assert limited_result == "limited"
    assert order == ["other", "limited"]
    assert attempts == 2
    assert snapshot["rate_limited_429"] == 1


@pytest.mark.asyncio
async def test_global_429_blocks_every_priority_until_retry_after() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(requests_per_second=1000, route_requests_per_second=1000)
    )
    attempts = 0
    started = time.monotonic()
    sent_at: list[float] = []

    async def limited() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _RateLimited(0.03, global_=True)
        sent_at.append(time.monotonic())

    limited_task = asyncio.create_task(
        coordinator.execute(_request(limited, route="messages", target="channel:1"))
    )
    await asyncio.sleep(0.005)
    critical_task = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: _timestamp(sent_at),
                route="interactions",
                target="interaction:1",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 1,
            )
        )
    )
    await asyncio.gather(limited_task, critical_task)
    await coordinator.close()

    assert min(sent_at) - started >= 0.025


@pytest.mark.asyncio
async def test_bounded_queue_coalesces_background_and_backpressures_foreground() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(
            requests_per_second=1000,
            route_requests_per_second=1000,
            queue_limit=1,
        )
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocker() -> None:
        entered.set()
        await release.wait()

    active = asyncio.create_task(coordinator.execute(_request(blocker, target="active")))
    await entered.wait()
    queued = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: _value("latest"),
                target="message:1",
                priority=DiscordPriority.BACKGROUND,
                coalesce="message:1",
            )
        )
    )
    replacement = asyncio.create_task(
        coordinator.execute(
            _request(
                lambda: _value("replacement"),
                target="message:1",
                priority=DiscordPriority.BACKGROUND,
                coalesce="message:1",
            )
        )
    )
    await asyncio.sleep(0)
    with pytest.raises(DiscordBackpressure):
        await coordinator.execute(_request(lambda: _value("foreground"), target="channel:2"))
    release.set()
    assert (await asyncio.gather(active, queued, replacement))[1:] == [
        "replacement",
        "replacement",
    ]
    await coordinator.close()


@pytest.mark.asyncio
async def test_process_token_bucket_shapes_burst_rate() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(
            requests_per_second=40,
            burst=1,
            route_requests_per_second=1000,
            route_burst=10,
        )
    )
    sent_at: list[float] = []
    await asyncio.gather(
        coordinator.execute(
            _request(lambda: _timestamp(sent_at), target="channel:1", route="send")
        ),
        coordinator.execute(
            _request(lambda: _timestamp(sent_at), target="channel:2", route="send")
        ),
    )
    await coordinator.close()

    assert sent_at[1] - sent_at[0] >= 0.02


@pytest.mark.asyncio
async def test_5xx_retries_are_bounded_and_permanent_4xx_are_observable() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(
            requests_per_second=1000,
            route_requests_per_second=1000,
            transient_attempts=3,
            retry_backoff_base_seconds=0.001,
        )
    )
    attempts = 0

    async def eventually_succeeds() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _HttpError(503)
        return "ok"

    assert await coordinator.execute(_request(eventually_succeeds)) == "ok"
    with pytest.raises(_HttpError):
        await coordinator.execute(_request(lambda: _raise_http(403), target="channel:forbidden"))
    metrics = coordinator.snapshot()
    await coordinator.close()

    assert attempts == 3
    assert metrics["permanent_failures"] == 1


@pytest.mark.asyncio
async def test_interaction_fails_fast_when_limiter_cannot_meet_deadline() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(
            requests_per_second=1,
            burst=1,
            route_requests_per_second=1000,
            route_burst=10,
        )
    )
    await coordinator.execute(_request(lambda: _value("first"), target="channel:first"))
    with pytest.raises(DiscordDeadlineExceeded):
        await coordinator.execute(
            _request(
                lambda: _value("late"),
                target="interaction:late",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 0.01,
            )
        )
    assert coordinator.snapshot()["deadline_misses"] == 1
    await coordinator.close()


async def _record(values: list[str], value: str) -> str:
    values.append(value)
    return value


async def _timestamp(values: list[float]) -> None:
    values.append(time.monotonic())


async def _value(value: Any) -> Any:
    return value


async def _raise_http(status: int) -> None:
    raise _HttpError(status)
