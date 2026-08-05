import hashlib
from pathlib import Path
from uuid import uuid4

import pytest

from copilotd.config import Settings
from copilotd.core.bindings import AttachmentState, SessionBindingRepository
from copilotd.core.models import AdaptedEvent
from copilotd.core.reducer import JournalReducer
from copilotd.sdk.capabilities import CapabilityRegistry
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
        renewed = await store.renew(lease, now=120)
        assert await store.has_mutation_headroom(renewed, now=140)


def test_owner_lease_store_rejects_ttl_without_jitter_margin(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="jitter margin"):
        OwnerLeaseStore(Database(tmp_path / "unsafe.sqlite3"), ttl_seconds=44)


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


@pytest.mark.asyncio
async def test_takeover_replay_reconciles_persisted_acceptance_without_duplicate(
    tmp_path: Path,
) -> None:
    session_id = "session-replay"
    prompt = "accepted before crash"
    accepted_id = str(uuid4())
    async with Database(tmp_path / "takeover-replay.sqlite3") as database:
        await CapabilityRegistry(
            Settings(_env_file=None, data_dir=tmp_path)
        ).activate(
            database,
            {
                "runtime_version": "1.0.73",
                "protocol_version": 3,
                "ping_protocol_version": 3,
            },
        )
        bindings = SessionBindingRepository(database)
        binding = await bindings.create(
            thread_id="thread-replay",
            sdk_session_id=session_id,
            cwd_snapshot=tmp_path,
            project_source="implicit-home",
            now=100,
        )
        store = OwnerLeaseStore(database, ttl_seconds=60)
        first_lease = await store.acquire(session_id, "owner-a", now=100)
        binding = await bindings.begin_attachment(
            thread_id=binding.thread_id,
            lease=first_lease,
            state=AttachmentState.RESUMING,
            now=100,
        )
        binding = await bindings.mark_attached(
            binding,
            permission_verified_at=100,
        )
        await database.execute(
            """
            INSERT INTO submissions(
                submission_id, sdk_session_id, origin, prompt_hash,
                requested_mode, state, accepted_message_id,
                send_started_at, accepted_at, created_at
            ) VALUES (
                'submission-replay', ?, 'app_message', ?, 'interactive',
                'submitted', ?, 104, 105, 101
            )
            """,
            (
                session_id,
                hashlib.sha256(prompt.encode()).hexdigest(),
                accepted_id,
            ),
        )
        await database.execute(
            """
            INSERT INTO message_queue(
                id, thread_id, prompt, requested_mode_snapshot,
                requested_model_config_snapshot, requested_session_config_version,
                position, state, created_at, updated_at
            ) VALUES (
                'submission-replay', 'thread-replay', ?, 'interactive',
                '{}', 1, 1, 'submitted', 101, 105
            )
            """,
            (prompt,),
        )
        await database.execute(
            """
            INSERT INTO liveness_leases(
                sdk_session_id, lease_id, kind, source_id,
                runtime_generation, owner_fence_token, state,
                acquired_at, refreshed_at
            ) VALUES (
                ?, 'submission:submission-replay', 'submission',
                'submission-replay', ?, ?, 'active', 101, 105
            )
            """,
            (
                session_id,
                binding.runtime_generation,
                binding.owner_fence_token,
            ),
        )

        takeover = await store.acquire(session_id, "owner-b", now=161)
        unknown = await database.fetchone(
            """
            SELECT state, accepted_message_id, accepted_at
            FROM submissions WHERE submission_id = 'submission-replay'
            """
        )
        replacement = await bindings.begin_attachment(
            thread_id=binding.thread_id,
            lease=takeover,
            state=AttachmentState.RESUMING,
            now=161,
        )
        event = AdaptedEvent(
            sdk_session_id=session_id,
            generation=replacement.runtime_generation,
            fence_token=takeover.fence_token,
            inbox_seq=1,
            source="sdk",
            raw_type="user.message",
            raw_payload={
                "type": "user.message",
                "data": {"content": prompt, "agentMode": "interactive"},
            },
            reducer_hash="replayed-user-message",
            persistence_class="durable",
            received_at=162,
            event_id=accepted_id,
        )
        assert await JournalReducer(database).persist([event]) == 1
        submissions = await database.fetchall(
            """
            SELECT origin, state, accepted_message_id, observed_user_event_id,
                   correlation_basis, terminal_at
            FROM submissions WHERE sdk_session_id = ?
            """,
            (session_id,),
        )
        lease = await database.fetchone(
            """
            SELECT state, runtime_generation, owner_fence_token
            FROM liveness_leases
            WHERE sdk_session_id = ? AND source_id = 'submission-replay'
            """,
            (session_id,),
        )

    assert dict(unknown) == {
        "state": "submitted_unknown",
        "accepted_message_id": accepted_id,
        "accepted_at": 105,
    }
    assert [dict(row) for row in submissions] == [
        {
            "origin": "app_message",
            "state": "observed_active",
            "accepted_message_id": accepted_id,
            "observed_user_event_id": accepted_id,
            "correlation_basis": "accepted_event_id_fixture",
            "terminal_at": None,
        }
    ]
    assert dict(lease) == {
        "state": "active",
        "runtime_generation": replacement.runtime_generation,
        "owner_fence_token": takeover.fence_token,
    }
