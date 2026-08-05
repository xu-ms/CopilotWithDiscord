from pathlib import Path

import pytest

from copilotd.storage.database import Database
from copilotd.storage.leases import FenceLost, OwnerConflict, OwnerLeaseStore


@pytest.mark.asyncio
async def test_owner_lease_conflict_takeover_and_fence(tmp_path: Path) -> None:
    async with Database(tmp_path / "lease.sqlite3") as database:
        store = OwnerLeaseStore(database, ttl_seconds=60)
        first = await store.acquire("session-1", "owner-a", now=100)

        with pytest.raises(OwnerConflict):
            await store.acquire("session-1", "owner-b", now=120)

        takeover = await store.acquire("session-1", "owner-b", now=161)

        assert first.fence_token == 1
        assert takeover.fence_token == 2
        assert not await store.is_current(first, now=162)
        assert await store.is_current(takeover, now=162)

        with pytest.raises(FenceLost):
            await store.renew(first, now=162)
        with pytest.raises(FenceLost):
            await store.release(first, now=162)


@pytest.mark.asyncio
async def test_same_owner_active_reacquire_keeps_fence(tmp_path: Path) -> None:
    async with Database(tmp_path / "lease.sqlite3") as database:
        store = OwnerLeaseStore(database, ttl_seconds=60)
        first = await store.acquire("session-1", "owner-a", now=100)
        reacquired = await store.acquire("session-1", "owner-a", now=120)

        assert reacquired.fence_token == first.fence_token
        assert reacquired.acquired_at == first.acquired_at
        assert reacquired.expires_at == 180


@pytest.mark.asyncio
async def test_same_owner_expired_reacquire_advances_fence(tmp_path: Path) -> None:
    async with Database(tmp_path / "lease.sqlite3") as database:
        store = OwnerLeaseStore(database, ttl_seconds=60)
        first = await store.acquire("session-1", "owner-a", now=100)
        reacquired = await store.acquire("session-1", "owner-a", now=200)

        assert reacquired.fence_token == first.fence_token + 1
        assert reacquired.acquired_at == 200
        assert reacquired.expires_at == 260


@pytest.mark.asyncio
async def test_expired_lease_must_be_reacquired_before_renew(tmp_path: Path) -> None:
    async with Database(tmp_path / "lease.sqlite3") as database:
        store = OwnerLeaseStore(database, ttl_seconds=60)
        lease = await store.acquire("session-1", "owner-a", now=100)

        with pytest.raises(FenceLost):
            await store.renew(lease, now=161)
