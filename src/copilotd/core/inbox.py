from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from copilot.session_events import SessionEvent

from copilotd.core.models import InboxEnvelope, OverflowIncident


class ReducerInbox:
    """Bounded MPSC queue that reserves capacity before crossing thread boundaries."""

    def __init__(
        self,
        *,
        sdk_session_id: str,
        generation: int,
        fence_token: int,
        capacity: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.sdk_session_id = sdk_session_id
        self.generation = generation
        self.fence_token = fence_token
        self._capacity = capacity
        self._loop = loop or asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self._queue: asyncio.Queue[InboxEnvelope] = asyncio.Queue(maxsize=capacity)
        self._space_available = asyncio.Event()
        self._space_available.set()
        self._progress = asyncio.Event()
        self._lock = threading.Lock()
        self._outstanding = 0
        self._next_inbox_seq = 0
        self._next_sdk_receive_seq = 0
        self._closed = False
        self._sdk_closed = False
        self._overflow: OverflowIncident | None = None
        self.overflow_event = asyncio.Event()

    @property
    def size(self) -> int:
        with self._lock:
            return self._outstanding

    @property
    def overflow(self) -> OverflowIncident | None:
        with self._lock:
            return self._overflow

    def submit_sdk(self, event: SessionEvent) -> bool:
        with self._lock:
            if self._sdk_closed:
                return False
        reservation = self._reserve(source="sdk")
        if reservation is None:
            return False
        inbox_seq, sdk_receive_seq = reservation
        envelope = InboxEnvelope(
            sdk_session_id=self.sdk_session_id,
            generation=self.generation,
            fence_token=self.fence_token,
            inbox_seq=inbox_seq,
            source="sdk",
            payload=event,
            received_at=time.time(),
            sdk_receive_seq=sdk_receive_seq,
        )
        return self._schedule(envelope)

    def submit_internal(
        self,
        payload: Any,
        *,
        source: str = "internal",
        internal_event_id: str | None = None,
    ) -> bool:
        if source not in {"internal", "snapshot"}:
            raise ValueError(f"invalid internal source: {source}")
        reservation = self._reserve(source=source)
        if reservation is None:
            return False
        inbox_seq, _ = reservation
        envelope = InboxEnvelope(
            sdk_session_id=self.sdk_session_id,
            generation=self.generation,
            fence_token=self.fence_token,
            inbox_seq=inbox_seq,
            source=source,
            payload=payload,
            received_at=time.time(),
            internal_event_id=internal_event_id or str(uuid.uuid4()),
        )
        return self._schedule(envelope)

    async def commit_internal(
        self,
        payload: Any,
        *,
        source: str = "internal",
        internal_event_id: str | None = None,
    ) -> None:
        if source not in {"internal", "snapshot"}:
            raise ValueError(f"invalid internal source: {source}")
        reservation = self._reserve(source=source, record_overflow=False)
        while reservation is None:
            with self._lock:
                if self._closed:
                    raise RuntimeError("reducer inbox is closed")
            await self._space_available.wait()
            reservation = self._reserve(source=source, record_overflow=False)
        inbox_seq, _ = reservation
        commit_ack = self._loop.create_future()
        envelope = InboxEnvelope(
            sdk_session_id=self.sdk_session_id,
            generation=self.generation,
            fence_token=self.fence_token,
            inbox_seq=inbox_seq,
            source=source,
            payload=payload,
            received_at=time.time(),
            internal_event_id=internal_event_id or str(uuid.uuid4()),
            commit_ack=commit_ack,
        )
        if not self._schedule(envelope):
            raise RuntimeError("failed to schedule reducer receipt")
        await commit_ack

    async def get(self) -> InboxEnvelope:
        return await self._queue.get()

    def get_nowait(self) -> InboxEnvelope:
        return self._queue.get_nowait()

    def acknowledge(
        self,
        envelope: InboxEnvelope,
        *,
        error: BaseException | None = None,
    ) -> None:
        self._queue.task_done()
        with self._lock:
            if self._outstanding < 1:
                raise RuntimeError("inbox acknowledgement underflow")
            self._outstanding -= 1
            self._set_space_available()
            self._signal_progress()
        if envelope.commit_ack is not None and not envelope.commit_ack.done():
            if error is None:
                envelope.commit_ack.set_result(None)
            else:
                envelope.commit_ack.set_exception(error)

    async def join(self) -> None:
        while True:
            with self._lock:
                if self._outstanding == 0:
                    return
            await self._progress.wait()
            self._progress.clear()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._sdk_closed = True
            self._set_space_available()

    def close_sdk(self) -> None:
        with self._lock:
            self._sdk_closed = True

    def _reserve(
        self,
        *,
        source: str,
        record_overflow: bool = True,
    ) -> tuple[int, int | None] | None:
        with self._lock:
            if self._closed:
                return None
            if self._outstanding >= self._capacity:
                self._clear_space_available()
                if record_overflow:
                    self._next_inbox_seq += 1
                    inbox_seq = self._next_inbox_seq
                    sdk_receive_seq: int | None = None
                    if source == "sdk":
                        self._next_sdk_receive_seq += 1
                        sdk_receive_seq = self._next_sdk_receive_seq
                    self._record_overflow_locked(inbox_seq, sdk_receive_seq)
                    self._loop.call_soon_threadsafe(self.overflow_event.set)
                return None
            self._next_inbox_seq += 1
            inbox_seq = self._next_inbox_seq
            sdk_receive_seq: int | None = None
            if source == "sdk":
                self._next_sdk_receive_seq += 1
                sdk_receive_seq = self._next_sdk_receive_seq

            self._outstanding += 1
            if self._outstanding >= self._capacity:
                self._clear_space_available()
            return inbox_seq, sdk_receive_seq

    def _schedule(self, envelope: InboxEnvelope) -> bool:
        try:
            self._loop.call_soon_threadsafe(self._put_reserved, envelope)
        except RuntimeError:
            with self._lock:
                self._outstanding -= 1
                self._set_space_available()
                self._signal_progress()
                self._record_overflow_locked(envelope.inbox_seq, envelope.sdk_receive_seq)
            return False
        return True

    def _put_reserved(self, envelope: InboxEnvelope) -> None:
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            with self._lock:
                self._outstanding -= 1
                self._set_space_available()
                self._signal_progress()
                self._record_overflow_locked(envelope.inbox_seq, envelope.sdk_receive_seq)
            self.overflow_event.set()

    def _set_space_available(self) -> None:
        if threading.get_ident() == self._loop_thread_id:
            self._space_available.set()
        else:
            self._loop.call_soon_threadsafe(self._space_available.set)

    def _clear_space_available(self) -> None:
        if threading.get_ident() == self._loop_thread_id:
            self._space_available.clear()
        else:
            self._loop.call_soon_threadsafe(self._space_available.clear)

    def _signal_progress(self) -> None:
        if threading.get_ident() == self._loop_thread_id:
            self._progress.set()
        else:
            self._loop.call_soon_threadsafe(self._progress.set)

    def _record_overflow_locked(
        self,
        inbox_seq: int,
        sdk_receive_seq: int | None,
    ) -> None:
        if self._overflow is None:
            self._overflow = OverflowIncident(
                sdk_session_id=self.sdk_session_id,
                generation=self.generation,
                fence_token=self.fence_token,
                first_lost_inbox_seq=inbox_seq,
                first_lost_sdk_receive_seq=sdk_receive_seq,
                lost_count=1,
                observed_at=time.time(),
            )
            return
        self._overflow = OverflowIncident(
            sdk_session_id=self._overflow.sdk_session_id,
            generation=self._overflow.generation,
            fence_token=self._overflow.fence_token,
            first_lost_inbox_seq=self._overflow.first_lost_inbox_seq,
            first_lost_sdk_receive_seq=self._overflow.first_lost_sdk_receive_seq,
            lost_count=self._overflow.lost_count + 1,
            observed_at=self._overflow.observed_at,
        )


class SdkEventIngress:
    def __init__(
        self,
        inbox: ReducerInbox,
        *,
        on_event_accepted: Callable[[SessionEvent], None] | None = None,
    ) -> None:
        self._inbox = inbox
        self._on_event_accepted = on_event_accepted

    def __call__(self, event: SessionEvent) -> None:
        if self._inbox.submit_sdk(event) and self._on_event_accepted is not None:
            self._on_event_accepted(event)
