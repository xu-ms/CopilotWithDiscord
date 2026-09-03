from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class VolatileContentError(RuntimeError):
    code = "volatile_content_error"


class MissingVolatileContentError(VolatileContentError):
    code = "content_unavailable"

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(self.code)


class VolatileContentCapacityError(VolatileContentError):
    code = "content_capacity_exceeded"

    def __init__(self) -> None:
        super().__init__(self.code)


class CommittedCancellation(asyncio.CancelledError):
    """Cancellation observed after a coupled durable transaction committed."""


@dataclass(frozen=True, slots=True)
class VolatileContentRef:
    key: str
    sha256: str
    byte_size: int


@dataclass(slots=True)
class _Entry:
    value: Any
    sha256: str
    byte_size: int


@dataclass(slots=True)
class _MutationFrame:
    prior: dict[str, _Entry | object]


_MISSING = object()


class VolatileContentStore:
    """Bounded process memory for content that must never enter SQLite."""

    def __init__(
        self,
        *,
        max_items: int = 4096,
        max_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_items < 1 or max_bytes < 1:
            raise ValueError("volatile content bounds must be positive")
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._entries: dict[str, _Entry] = {}
        self._bytes = 0
        self._lock = threading.RLock()
        self._mutation_frames: ContextVar[tuple[_MutationFrame, ...]] = ContextVar(
            f"volatile_content_mutations_{id(self)}",
            default=(),
        )

    @property
    def item_count(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def byte_count(self) -> int:
        with self._lock:
            return self._bytes

    def put(
        self,
        value: Any,
        *,
        key: str | None = None,
    ) -> VolatileContentRef:
        encoded = _content_bytes(value)
        byte_size = len(encoded)
        digest = hashlib.sha256(encoded).hexdigest()
        opaque_key = f"vc:{uuid.uuid4().hex}" if key is None else key
        entry = _Entry(
            value=copy.deepcopy(value),
            sha256=digest,
            byte_size=byte_size,
        )
        with self._lock:
            previous = self._entries.get(opaque_key)
            prospective_items = len(self._entries) + (previous is None)
            prospective_bytes = self._bytes + byte_size
            if previous is not None:
                prospective_bytes -= previous.byte_size
            if prospective_items > self._max_items or prospective_bytes > self._max_bytes:
                raise VolatileContentCapacityError()
            self._record_prior_locked(opaque_key, previous)
            self._entries[opaque_key] = entry
            self._bytes = prospective_bytes
        return VolatileContentRef(opaque_key, digest, byte_size)

    def get(self, key: str | None, *, expected_hash: str | None = None) -> Any | None:
        if not key:
            return None
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or (expected_hash is not None and entry.sha256 != expected_hash):
                return None
            return copy.deepcopy(entry.value)

    def require(self, key: str | None, *, expected_hash: str | None = None) -> Any:
        value = self.get(key, expected_hash=expected_hash)
        if value is None:
            raise MissingVolatileContentError(key or "")
        return value

    def delete(self, key: str) -> bool:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return False
            self._record_prior_locked(key, entry)
            self._entries.pop(key)
            self._bytes -= entry.byte_size
            return True

    def clear(self) -> None:
        with self._lock:
            for key, entry in self._entries.items():
                self._record_prior_locked(key, entry)
            self._entries.clear()
            self._bytes = 0

    @contextmanager
    def transaction(self) -> Iterator[None]:
        frame = _MutationFrame(prior={})
        frames = self._mutation_frames.get()
        token = self._mutation_frames.set((*frames, frame))
        try:
            yield
        except CommittedCancellation:
            self._commit_frame(frame, frames)
            raise
        except BaseException:
            with self._lock:
                for key, prior in frame.prior.items():
                    if prior is _MISSING:
                        self._entries.pop(key, None)
                    else:
                        assert isinstance(prior, _Entry)
                        self._entries[key] = copy.deepcopy(prior)
                self._bytes = sum(entry.byte_size for entry in self._entries.values())
            raise
        else:
            self._commit_frame(frame, frames)
        finally:
            self._mutation_frames.reset(token)

    @staticmethod
    def _commit_frame(
        frame: _MutationFrame,
        parent_frames: tuple[_MutationFrame, ...],
    ) -> None:
        if parent_frames:
            parent = parent_frames[-1]
            for key, prior in frame.prior.items():
                parent.prior.setdefault(key, prior)

    def _record_prior_locked(self, key: str, entry: _Entry | None) -> None:
        frames = self._mutation_frames.get()
        if not frames:
            return
        frame = frames[-1]
        if key not in frame.prior:
            frame.prior[key] = _MISSING if entry is None else copy.deepcopy(entry)


def opaque_content_key(scope: str, *identifiers: object) -> str:
    material = "\0".join((scope, *(str(item) for item in identifiers)))
    return f"vc:{hashlib.sha256(material.encode()).hexdigest()}"


def tool_event_evidence_key(
    session_id: str,
    generation: int,
    tool_call_id: str,
    event_kind: str,
) -> str:
    return opaque_content_key(
        "tool-event-evidence",
        session_id,
        generation,
        tool_call_id,
        event_kind,
    )


_PROCESS_CONTENT_STORE = VolatileContentStore()


def process_content_store() -> VolatileContentStore:
    return _PROCESS_CONTENT_STORE


def _content_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"unsupported volatile content value: {type(value).__name__}")
