from pathlib import Path

import pytest

from copilotd.core.bindings import AttachmentState, SessionBindingRepository
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


@pytest.mark.asyncio
async def test_takeover_atomically_orphans_all_old_generation_domain_state(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "takeover.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                runtime_generation, owner_fence_token, created_at, updated_at
            ) VALUES ('thread-1', 'home', '/tmp', 'session-1', 1, 1, 1, 1)
            """
        )
        store = OwnerLeaseStore(database, ttl_seconds=60)
        await store.acquire("session-1", "owner-a", now=100)
        await database.execute(
            """
            INSERT INTO session_operations(
                operation_id, sdk_session_id, runtime_generation,
                owner_fence_token, kind, idempotency_key, input_hash,
                state, created_at
            ) VALUES ('operation-1', 'session-1', 1, 1, 'send', 'send-1',
                      'hash', 'started', 101)
            """
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, state, created_at
            ) VALUES ('submission-1', 'session-1', 'app_message',
                      'observed_active', 101)
            """
        )
        await database.execute(
            """
            INSERT INTO background_observations(
                sdk_session_id, runtime_generation, source_event_id,
                task_id, observed_state, last_progress_at
            ) VALUES ('session-1', 1, 'task:1', 'task-1', 'running', 101)
            """
        )
        await database.execute(
            """
            INSERT INTO pending_interactions(
                interaction_id, sdk_session_id, runtime_generation,
                owner_fence_token, thread_id, kind, response_plane,
                expires_at, state, payload, created_at, updated_at
            ) VALUES ('interaction-1', 'session-1', 1, 1, 'thread-1',
                      'user_input', 'direct_handler', 1000, 'pending',
                      '{}', 101, 101)
            """
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES ('session-1', 'submission:1', 'submission', 'submission-1',
                      1, 1, 'active', 101, 101)
            """
        )
        await database.execute(
            """
            INSERT INTO runtime_schedules(
                sdk_session_id, runtime_schedule_id, builtin_name,
                invocation_input, state, updated_at
            ) VALUES ('session-1', 'schedule-1', 'after', '10s work',
                      'active', 101)
            """
        )

        takeover = await store.acquire("session-1", "owner-b", now=161)
        operation = await database.fetchone(
            "SELECT state, error_code FROM session_operations"
        )
        submission = await database.fetchone(
            "SELECT state FROM submissions WHERE submission_id = 'submission-1'"
        )
        background = await database.fetchone(
            "SELECT observed_state FROM background_observations"
        )
        interaction = await database.fetchone(
            "SELECT state FROM pending_interactions"
        )
        liveness = await database.fetchone("SELECT state FROM liveness_leases")
        schedule = await database.fetchone("SELECT state FROM runtime_schedules")

    assert takeover.fence_token == 2
    assert dict(operation) == {
        "state": "unknown",
        "error_code": "owner_fence_takeover",
    }
    assert submission["state"] == "outcome_unknown"
    assert background["observed_state"] == "unknown"
    assert interaction["state"] == "expired"
    assert liveness["state"] == "orphaned"
    assert schedule["state"] == "unknown"


@pytest.mark.asyncio
async def test_mutation_headroom_requires_at_least_forty_seconds(tmp_path: Path) -> None:
    async with Database(tmp_path / "headroom.sqlite3") as database:
        store = OwnerLeaseStore(database, ttl_seconds=60)
        lease = await store.acquire("session-1", "owner-a", now=100)

        assert await store.has_mutation_headroom(lease, now=120)
        assert not await store.has_mutation_headroom(lease, now=121)


@pytest.mark.asyncio
async def test_new_attachment_generation_resets_reducer_watermarks(tmp_path: Path) -> None:
    async with Database(tmp_path / "generation-watermarks.sqlite3") as database:
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="thread-1",
            sdk_session_id="session-1",
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET last_inbox_seq = 500, last_sdk_receive_seq = 400
            WHERE sdk_session_id = 'session-1'
            """
        )
        lease = await OwnerLeaseStore(database).acquire(
            "session-1",
            "owner-1",
            now=100,
        )

        attached = await bindings.begin_attachment(
            thread_id="thread-1",
            lease=lease,
            state=AttachmentState.RESUMING,
            now=100,
        )

    assert attached.runtime_generation == 1
    assert attached.last_inbox_seq == 0
    assert attached.last_sdk_receive_seq is None
