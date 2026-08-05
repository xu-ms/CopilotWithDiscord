import asyncio
import gc

import pytest

from copilotd.core.task_registry import TaskRegistry


@pytest.mark.asyncio
async def test_task_registry_keeps_strong_reference_until_completion() -> None:
    registry = TaskRegistry()
    release = asyncio.Event()

    async def wait() -> None:
        await release.wait()

    task = registry.create(wait(), name="background")
    del task
    gc.collect()

    assert registry.active_count == 1
    release.set()
    await registry.wait_empty()
    assert registry.active_count == 0


@pytest.mark.asyncio
async def test_task_registry_surfaces_background_exception() -> None:
    registry = TaskRegistry()

    async def fail() -> None:
        raise RuntimeError("heartbeat failed")

    registry.create(fail(), name="heartbeat")
    error = await registry.errors.get()
    await registry.wait_empty()
    assert isinstance(error, RuntimeError)
    assert str(error) == "heartbeat failed"
    assert str(error) == "heartbeat failed"
