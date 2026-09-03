import asyncio
import time
from typing import Any

import pytest

from copilotd.discord_http_limiter import DiscordHttpRedirectBlocked
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
    min_interval: float = 0.0,
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
        min_interval_seconds=min_interval,
    )


@pytest.mark.asyncio
async def test_deadline_priority_overtakes_background_without_reordering_target() -> None:
    coordinator = DiscordRequestCoordinator(DiscordCoordinatorConfig(max_concurrency=1))
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
async def test_redirect_blocked_fails_one_command_without_stopping_coordinator() -> None:
    coordinator = DiscordRequestCoordinator()

    async def blocked_command() -> None:
        raise DiscordHttpRedirectBlocked()

    with pytest.raises(DiscordBackpressure):
        await coordinator.execute(
            _request(
                blocked_command,
                operation=DiscordOperation.COMMAND_SYNC,
                route="commands.sync",
                target="application:commands",
            )
        )
    assert (
        await coordinator.execute(
            _request(
                lambda: _value("next"),
                operation=DiscordOperation.COMMAND_SYNC,
                route="commands.sync",
                target="application:commands",
            )
        )
        == "next"
    )
    await coordinator.close()


@pytest.mark.asyncio
async def test_coalescing_keeps_latest_terminal_and_never_downgrades() -> None:
    coordinator = DiscordRequestCoordinator(DiscordCoordinatorConfig(max_concurrency=1))
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


@pytest.mark.asyncio
async def test_logical_callback_is_invoked_once_when_it_raises() -> None:
    coordinator = DiscordRequestCoordinator()
    attempts = 0

    async def failing() -> None:
        nonlocal attempts
        attempts += 1
        raise OSError("discord.py owns transport retries")

    with pytest.raises(OSError):
        await coordinator.execute(_request(failing))
    metrics = coordinator.snapshot()
    await coordinator.close()

    assert attempts == 1
    assert metrics["logical_failures"] == 1


@pytest.mark.asyncio
async def test_bounded_queue_coalesces_background_and_backpressures_foreground() -> None:
    coordinator = DiscordRequestCoordinator(
        DiscordCoordinatorConfig(queue_limit=1, max_concurrency=1)
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
async def test_blocked_route_does_not_head_of_line_block_interaction_ack() -> None:
    coordinator = DiscordRequestCoordinator()
    route_entered = asyncio.Event()
    release_route = asyncio.Event()

    async def blocked_route() -> None:
        route_entered.set()
        await release_route.wait()

    normal = asyncio.create_task(
        coordinator.execute(_request(blocked_route, target="channel:normal"))
    )
    await route_entered.wait()
    started = time.monotonic()
    acknowledged = await asyncio.wait_for(
        coordinator.execute(
            _request(
                lambda: _value("acknowledged"),
                operation=DiscordOperation.INTERACTION_DEFER,
                target="interaction:deadline",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 0.1,
            )
        ),
        timeout=0.1,
    )

    assert acknowledged == "acknowledged"
    assert time.monotonic() - started < 0.1
    release_route.set()
    await normal
    await coordinator.close()


@pytest.mark.asyncio
async def test_close_cancels_and_settles_active_callbacks() -> None:
    coordinator = DiscordRequestCoordinator()
    entered = asyncio.Event()
    settled = asyncio.Event()

    async def blocked() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    request = asyncio.create_task(coordinator.execute(_request(blocked, target="channel:active")))
    await entered.wait()
    await coordinator.close()

    with pytest.raises(DiscordBackpressure):
        await request
    assert settled.is_set()
    assert coordinator.snapshot()["active_operations"] == 0


@pytest.mark.asyncio
async def test_target_minimum_interval_is_preserved() -> None:
    coordinator = DiscordRequestCoordinator()
    sent_at: list[float] = []
    await asyncio.gather(
        coordinator.execute(
            _request(
                lambda: _timestamp(sent_at),
                target="message:1",
                route="edit",
                min_interval=0.03,
            )
        ),
        coordinator.execute(
            _request(
                lambda: _timestamp(sent_at),
                target="message:1",
                route="edit",
                min_interval=0.03,
            )
        ),
    )
    await coordinator.close()

    assert sent_at[1] - sent_at[0] >= 0.025


@pytest.mark.asyncio
async def test_interaction_fails_when_target_cadence_misses_deadline() -> None:
    coordinator = DiscordRequestCoordinator()
    await coordinator.execute(
        _request(
            lambda: _value("first"),
            target="interaction:late",
            min_interval=0.05,
        )
    )
    with pytest.raises(DiscordDeadlineExceeded):
        await coordinator.execute(
            _request(
                lambda: _value("late"),
                target="interaction:late",
                priority=DiscordPriority.DEADLINE_CRITICAL,
                deadline=time.monotonic() + 0.01,
                min_interval=0.05,
            )
        )
    assert coordinator.snapshot()["deadline_misses"] == 1
    await coordinator.close()


async def _timestamp(values: list[float]) -> None:
    values.append(time.monotonic())


async def _value(value: Any) -> Any:
    return value
