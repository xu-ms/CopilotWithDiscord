from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class TaskFailure:
    name: str
    source: str
    session_id: str | None
    runtime_generation: int | None
    error: BaseException


class TaskRegistry:
    """Owns strong references and makes every background failure observable."""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()
        self._metadata: dict[
            asyncio.Task[Any],
            tuple[str, str, str | None, int | None],
        ] = {}
        self._errors: asyncio.Queue[TaskFailure] = asyncio.Queue()
        self._closing = False
        self._empty = asyncio.Event()
        self._empty.set()

    @property
    def active_count(self) -> int:
        return len(self._tasks)

    @property
    def errors(self) -> asyncio.Queue[TaskFailure]:
        return self._errors

    def create(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        name: str,
        source: str = "app",
        session_id: str | None = None,
        runtime_generation: int | None = None,
    ) -> asyncio.Task[T]:
        if self._closing:
            coroutine.close()
            raise RuntimeError("task registry is closing")
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        self._metadata[task] = (
            name,
            source,
            session_id,
            runtime_generation,
        )
        self._empty.clear()
        task.add_done_callback(self._on_done)
        return task

    async def cancel_all(
        self,
        *,
        wait_seconds: float = 10,
        exclude: frozenset[asyncio.Task[Any]] = frozenset(),
    ) -> None:
        self._closing = True
        current = asyncio.current_task()
        tasks = [task for task in self._tasks if task is not current and task not in exclude]
        for task in tasks:
            task.cancel()
        if tasks:
            async with asyncio.timeout(wait_seconds):
                await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_empty(self, *, wait_seconds: float = 10) -> None:
        async with asyncio.timeout(wait_seconds):
            await self._empty.wait()

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        metadata = self._metadata.pop(
            task,
            (task.get_name(), "app", None, None),
        )
        if not self._tasks:
            self._empty.set()
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._errors.put_nowait(
                TaskFailure(
                    name=metadata[0],
                    source=metadata[1],
                    session_id=metadata[2],
                    runtime_generation=metadata[3],
                    error=error,
                )
            )
