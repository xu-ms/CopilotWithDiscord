import asyncio
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from copilotd.core.bindings import (
    AttachmentState,
    BindingConflict,
    SessionBindingRepository,
)
from copilotd.core.inbox import ReducerInbox
from copilotd.core.projects import ProjectConfigError, ProjectRegistry
from copilotd.core.reducer import EventReducerWorker, JournalReducer
from copilotd.core.scheduler import (
    ScheduleConflict,
    ScheduleKind,
    SchedulerRepository,
)
from copilotd.core.worktrees import (
    DeterministicWorktreeAdapter,
    GitCommandResult,
    SubprocessGitRunner,
    WorktreeCapabilityError,
    WorktreeConflict,
    WorktreeHistoryMode,
    WorktreeInputError,
    WorktreeIntent,
    WorktreeIntentState,
    WorktreeManager,
    WorktreeOperationError,
    WorktreeTarget,
    _branch_name,
)
from copilotd.storage.database import Database
from copilotd.storage.leases import OwnerLeaseStore


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        shell=False,
    )
    return completed.stdout.strip()


def _create_repo(path: Path) -> None:
    path.mkdir()
    _git(path, "init")
    _git(path, "config", "user.name", "copilotD tests")
    _git(path, "config", "user.email", "copilotd@example.invalid")
    (path / "README.txt").write_text("root\n", encoding="utf-8")
    _git(path, "add", "README.txt")
    _git(path, "commit", "-m", "initial")


async def _manager(
    database: Database,
    tmp_path: Path,
    adapter: DeterministicWorktreeAdapter,
) -> tuple[WorktreeManager, str]:
    home = tmp_path / "home"
    home.mkdir()
    repo = tmp_path / "repo with spaces"
    _create_repo(repo)
    projects = ProjectRegistry(database, resolved_home=home)
    await projects.initialize()
    project = await projects.bind("channel-1", repo)
    assert project.project_id is not None
    manager = WorktreeManager(
        database,
        projects,
        worktrees_root=tmp_path / "managed worktrees",
        adapter=adapter,
    )
    return manager, project.project_id


@pytest.mark.asyncio
async def test_real_git_worktree_handles_cjk_spaces_and_preserves_branch_on_close(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "worktrees.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="功能 分支",
        )
        repo_root = Path(
            (
                await database.fetchone(
                    "SELECT repo_root FROM project_worktrees WHERE worktree_id = ?",
                    (created.worktree_id,),
                )
            )["repo_root"]
        )
        assert created.path.is_dir()
        assert _git(repo_root, "rev-parse", "--show-toplevel") == str(repo_root)
        assert str(created.path) in _git(repo_root, "worktree", "list", "--porcelain")

        closed = await manager.close(
            created.name,
            parent_project_id=project_id,
        )
        branch = await asyncio.to_thread(
            subprocess.run,
            ["git", "show-ref", "--verify", f"refs/heads/{created.branch_name}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            shell=False,
        )

    assert closed.state == "closed"
    assert not created.path.exists()
    assert branch.returncode == 0


@pytest.mark.asyncio
async def test_ready_fast_path_fences_externally_removed_worktree(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "ready-fast-path.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="ready removed",
        )
        row = await database.fetchone(
            "SELECT repo_root FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )
        _git(Path(row["repo_root"]), "worktree", "remove", "--", str(created.path))

        with pytest.raises(WorktreeOperationError):
            await manager.create(
                parent_project_id=project_id,
                name="ready removed",
            )
        projection = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )
        project = await database.fetchone(
            "SELECT state FROM projects WHERE id = ?",
            (created.project_id,),
        )

    assert projection["state"] == "intervention"
    assert project["state"] == "closing"


@pytest.mark.asyncio
async def test_stale_intervention_cannot_overwrite_completed_close(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "stale-intervention.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="stale intervention",
        )
        stale = await manager._intent(created.intent_id)
        await manager.close(created.name, parent_project_id=project_id)

        await manager._record_recovery_intervention(
            stale,
            WorktreeConflict("stale recovery"),
            now=time.time(),
        )
        projection = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )
        project = await database.fetchone(
            "SELECT state FROM projects WHERE id = ?",
            (created.project_id,),
        )

    assert projection["state"] == "closed"
    assert project["state"] == "retired"


@pytest.mark.asyncio
async def test_known_target_failure_compensates_worktree_without_deleting_branch(
    tmp_path: Path,
) -> None:
    adapter = DeterministicWorktreeAdapter()
    adapter.failure = WorktreeOperationError("definite target rejection", outcome_unknown=False)
    async with Database(tmp_path / "compensation.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        with pytest.raises(WorktreeOperationError, match="definite target rejection"):
            await manager.create(
                parent_project_id=project_id,
                name="compensate me",
            )
        intent = await database.fetchone(
            "SELECT * FROM worktree_intents WHERE parent_project_id = ?",
            (project_id,),
        )
        project = await database.fetchone(
            "SELECT state FROM projects WHERE id = ?",
            (intent["project_id"],),
        )
        parent = await database.fetchone(
            "SELECT root_path FROM projects WHERE id = ?",
            (project_id,),
        )
        repo_root = Path(parent["root_path"])
        branch = await asyncio.to_thread(
            subprocess.run,
            ["git", "show-ref", "--verify", f"refs/heads/{intent['branch_name']}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            shell=False,
        )

    assert intent["state"] == "compensated"
    assert project["state"] == "retired"
    assert str(intent["target_path"]) not in _git(
        repo_root,
        "worktree",
        "list",
        "--porcelain",
    )
    assert branch.returncode == 0


@pytest.mark.asyncio
async def test_history_fork_is_fail_closed_without_typed_capability(tmp_path: Path) -> None:
    async with Database(tmp_path / "fork-closed.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(history_fork_available=False),
        )
        with pytest.raises(WorktreeCapabilityError):
            await manager.create(
                parent_project_id=project_id,
                name="forked",
                history_mode=WorktreeHistoryMode.FORK,
                source_session_id="source-session",
            )
        intents = await database.fetchone("SELECT COUNT(*) FROM worktree_intents")

    assert intents[0] == 0


@pytest.mark.asyncio
async def test_history_mode_invokes_only_typed_fork_adapter_when_available(
    tmp_path: Path,
) -> None:
    adapter = DeterministicWorktreeAdapter(history_fork_available=True)
    async with Database(tmp_path / "fork-available.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        created = await manager.create(
            parent_project_id=project_id,
            name="typed fork",
            history_mode=WorktreeHistoryMode.FORK,
            source_session_id="source-session",
        )

    assert created.history_mode == WorktreeHistoryMode.FORK
    assert adapter.create_calls == [created.intent_id]
    assert created.sdk_session_id is not None


@pytest.mark.asyncio
async def test_close_reports_session_lease_remote_and_schedule_references(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "blockers.sqlite3") as database:
        manager, parent_project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        worktree = await manager.create(
            parent_project_id=parent_project_id,
            name="blocked",
        )
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_id, project_source, cwd_snapshot, sdk_session_id,
                binding_intent, attachment_state, runtime_remote_mode,
                created_at, updated_at
            ) VALUES ('thread-blocked', ?, 'explicit', ?, 'session-blocked',
                      'active', 'attached', 'on', ?, ?)
            """,
            (
                worktree.project_id,
                str(worktree.path),
                time.time(),
                time.time(),
            ),
        )
        await OwnerLeaseStore(database).acquire(
            "session-blocked",
            "owner",
            now=time.time(),
        )
        repository = SchedulerRepository(database)
        await repository.create(
            kind=ScheduleKind.NEW_SESSION,
            expression="cron:0 9 * * *",
            timezone="UTC",
            payload={"text": "scheduled"},
            target_snapshot={},
            project_id=worktree.project_id,
            now=time.time(),
        )

        blockers = await manager.blockers(
            worktree.name,
            parent_project_id=parent_project_id,
        )
        with pytest.raises(WorktreeConflict) as error:
            await manager.close(
                worktree.name,
                parent_project_id=parent_project_id,
            )

    assert any(item.startswith("remote:") for item in blockers)
    assert any(item.startswith("session:") for item in blockers)
    assert any(item.startswith("owner_lease:") for item in blockers)
    assert any(item.startswith("schedule:") for item in blockers)
    assert error.value.blockers == blockers


@pytest.mark.asyncio
async def test_recovery_closes_only_known_intent_after_exact_path_is_absent(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "close-recovery.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="recover close",
        )
        row = await database.fetchone(
            "SELECT repo_root FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )
        repo_root = Path(row["repo_root"])
        await database.execute(
            "UPDATE worktree_intents SET state = 'closing' WHERE intent_id = ?",
            (created.intent_id,),
        )
        await database.execute(
            "UPDATE project_worktrees SET state = 'closing' WHERE intent_id = ?",
            (created.intent_id,),
        )
        _git(repo_root, "worktree", "remove", "--", str(created.path))

        report = await manager.recover(now=time.time())
        recovered = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )

    assert report.examined_intents == 1
    assert report.recovered_intents == 1
    assert report.orphaned_intents == 0
    assert recovered["state"] == "closed"


@pytest.mark.asyncio
async def test_worktree_name_rejects_path_escape(tmp_path: Path) -> None:
    async with Database(tmp_path / "invalid-name.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        with pytest.raises(WorktreeInputError):
            await manager.create(
                parent_project_id=project_id,
                name="../outside",
            )


@pytest.mark.asyncio
async def test_unknown_target_reconciles_existing_token_without_second_target(
    tmp_path: Path,
) -> None:
    class AmbiguousAdapter(DeterministicWorktreeAdapter):
        async def create_blank(self, **kwargs: object) -> object:
            await super().create_blank(**kwargs)
            raise WorktreeOperationError("target response lost", outcome_unknown=True)

    adapter = AmbiguousAdapter()
    async with Database(tmp_path / "target-reconcile.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        with pytest.raises(WorktreeOperationError, match="response lost"):
            await manager.create(
                parent_project_id=project_id,
                name="unknown target",
            )
        unknown = await database.fetchone(
            "SELECT state FROM worktree_intents WHERE parent_project_id = ?",
            (project_id,),
        )

        report = await manager.recover()
        ready = (await manager.list(parent_project_id=project_id))[0]

    assert unknown["state"] == "target_unknown"
    assert report.recovered_intents == 1
    assert ready.state == "ready"
    assert len(adapter.create_calls) == 1


@pytest.mark.asyncio
async def test_compensation_recovery_resumes_exact_owned_cleanup(tmp_path: Path) -> None:
    async with Database(tmp_path / "compensation-recovery.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="resume compensation",
        )
        await database.execute(
            "UPDATE worktree_intents SET state = 'compensating' WHERE intent_id = ?",
            (created.intent_id,),
        )
        await database.execute(
            "UPDATE project_worktrees SET state = 'compensating' WHERE intent_id = ?",
            (created.intent_id,),
        )

        report = await manager.recover()
        intent = await database.fetchone(
            "SELECT state FROM worktree_intents WHERE intent_id = ?",
            (created.intent_id,),
        )
        project = await database.fetchone(
            "SELECT state FROM projects WHERE id = ?",
            (created.project_id,),
        )

    assert report.recovered_intents == 1
    assert intent["state"] == "compensated"
    assert project["state"] == "retired"
    assert not created.path.exists()


@pytest.mark.asyncio
async def test_ready_intent_repairs_projection_atomically_on_recovery(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "ready-recovery.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="repair projection",
        )
        await database.execute(
            """
            UPDATE project_worktrees
            SET state = 'project_registered', thread_id = NULL, sdk_session_id = NULL
            WHERE intent_id = ?
            """,
            (created.intent_id,),
        )

        report = await manager.recover()
        repaired = (await manager.list(parent_project_id=project_id))[0]

    assert report.recovered_intents == 1
    assert repaired.state == "ready"
    assert repaired.thread_id == created.thread_id
    assert repaired.sdk_session_id == created.sdk_session_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("intent_state", "expected_state"),
    [
        ("git_creating", "failed"),
        ("git_created", "git_created"),
        ("target_unknown", "target_unknown"),
        ("ready", "ready"),
    ],
)
async def test_recovery_never_adopts_foreign_branch_at_reserved_path(
    tmp_path: Path,
    intent_state: str,
    expected_state: str,
) -> None:
    async with Database(tmp_path / "foreign-worktree.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        parent = await database.fetchone(
            "SELECT root_path FROM projects WHERE id = ?",
            (project_id,),
        )
        repo_root = Path(parent["root_path"])
        target = tmp_path / "managed worktrees" / project_id / "foreign"
        target.parent.mkdir(parents=True)
        _git(repo_root, "worktree", "add", "-b", "foreign-branch", str(target), "HEAD")
        await database.execute(
            """
            INSERT INTO worktree_intents(
                intent_id, parent_project_id, name, branch_name, base_ref,
                history_mode, target_path, state, created_at, updated_at
            ) VALUES ('foreign-intent', ?, 'foreign', 'copilotd/expected',
                      'HEAD', 'none', ?, ?, 1, 1)
            """,
            (project_id, str(target), intent_state),
        )

        report = await manager.recover()
        intent = await database.fetchone(
            "SELECT state FROM worktree_intents WHERE intent_id = 'foreign-intent'"
        )

    assert report.orphaned_intents == 1
    assert intent["state"] == expected_state
    assert target.exists()


@pytest.mark.asyncio
async def test_close_fence_blocks_new_session_and_schedule_references(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "close-fence.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        created = await manager.create(
            parent_project_id=project_id,
            name="closing fence",
        )
        bindings = SessionBindingRepository(database)
        await bindings.create(
            thread_id="stale-thread",
            sdk_session_id="stale-session",
            cwd_snapshot=created.path,
            project_source="explicit",
            project_id=created.project_id,
        )
        await database.execute(
            """
            UPDATE session_bindings
            SET binding_intent = 'closed', attachment_state = 'absent',
                runtime_remote_mode = 'off', runtime_generation = 1,
                owner_fence_token = 7
            WHERE thread_id = 'stale-thread'
            """
        )
        stale_binding = await bindings.by_thread("stale-thread")
        assert stale_binding is not None
        row = await database.fetchone(
            "SELECT * FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )
        await manager._begin_close(row, now=time.time())
        with pytest.raises(BindingConflict, match="closing"):
            await bindings.activate(stale_binding)
        owner_leases = OwnerLeaseStore(database)
        late_lease = await owner_leases.acquire("stale-session", "late-owner")
        with pytest.raises(BindingConflict, match="closing"):
            await bindings.begin_attachment(
                thread_id="stale-thread",
                lease=late_lease,
                state=AttachmentState.RESUMING,
            )
        await owner_leases.release(late_lease)
        await database.execute(
            """
            UPDATE session_bindings SET attachment_state = 'attached'
            WHERE thread_id = 'stale-thread'
            """
        )
        inbox = ReducerInbox(
            sdk_session_id="stale-session",
            generation=1,
            fence_token=7,
            capacity=16,
            thread_id="stale-thread",
        )
        reducer = EventReducerWorker(
            inbox=inbox,
            reducer=JournalReducer(database),
            batch_size=4,
        )
        reducer.start()
        await inbox.commit_internal(
            {
                "type": "copilotd.submission.queued",
                "data": {
                    "submission_id": "stale-submission",
                    "thread_id": "stale-thread",
                    "prompt": "must not enqueue",
                    "requested_mode": "interactive",
                },
            },
            internal_event_id="stale-after-close",
        )
        stale_queue = await database.fetchone(
            """
            SELECT COUNT(*) FROM message_queue
            WHERE id = 'stale-submission'
            """
        )
        await reducer.stop()

        with pytest.raises(BindingConflict, match="closing"):
            await SessionBindingRepository(database).create(
                thread_id="late-thread",
                sdk_session_id="late-session",
                cwd_snapshot=created.path,
                project_source="explicit",
                project_id=created.project_id,
            )
        with pytest.raises(ScheduleConflict, match="closing"):
            await SchedulerRepository(database).create(
                kind=ScheduleKind.NEW_SESSION,
                expression="cron:0 9 * * *",
                timezone="UTC",
                payload={"text": "late"},
                target_snapshot={},
                project_id=created.project_id,
            )
        with pytest.raises(WorktreeConflict, match="changed"):
            await manager._mark_intent(
                created.intent_id,
                WorktreeIntentState.TARGET_UNKNOWN,
                now=time.time(),
            )
        await manager.recover()

    assert not created.path.exists()
    assert stale_queue[0] == 0


@pytest.mark.asyncio
async def test_parent_retirement_is_blocked_after_durable_worktree_reservation(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "parent-fence.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        parent = await database.fetchone(
            "SELECT root_path FROM projects WHERE id = ?",
            (project_id,),
        )
        repo_root = Path(parent["root_path"])
        intent_id = "reserved-parent-fence"
        await manager._reserve_intent(
            intent_id=intent_id,
            parent_project_id=project_id,
            source_session_id=None,
            name="reserved",
            branch_name="copilotd/reserved-parent-fence",
            base_ref="HEAD",
            history_mode=WorktreeHistoryMode.NONE,
            target_path=tmp_path / "managed worktrees" / project_id / "reserved",
            now=time.time(),
        )

        with pytest.raises(ProjectConfigError, match="nonterminal worktree intent"):
            await manager._projects.unbind("channel-1")
        state = await database.fetchone(
            "SELECT state FROM projects WHERE id = ?",
            (project_id,),
        )
        assert await asyncio.to_thread(repo_root.exists)

    assert state["state"] == "active"


@pytest.mark.asyncio
async def test_project_registered_gap_recreates_projection_during_recovery(
    tmp_path: Path,
) -> None:
    adapter = DeterministicWorktreeAdapter()
    async with Database(tmp_path / "projection-gap.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        created = await manager.create(
            parent_project_id=project_id,
            name="projection gap",
        )
        await database.execute(
            """
            UPDATE worktree_intents
            SET state = 'project_registered', thread_id = NULL, sdk_session_id = NULL
            WHERE intent_id = ?
            """,
            (created.intent_id,),
        )
        await database.execute(
            "DELETE FROM project_worktrees WHERE intent_id = ?",
            (created.intent_id,),
        )

        report = await manager.recover()
        repaired = (await manager.list(parent_project_id=project_id))[0]

    assert report.recovered_intents == 1
    assert repaired.state == "ready"
    assert repaired.thread_id == created.thread_id


@pytest.mark.asyncio
async def test_recovered_history_fork_reconciles_without_second_fork(
    tmp_path: Path,
) -> None:
    adapter = DeterministicWorktreeAdapter(history_fork_available=True)
    async with Database(tmp_path / "fork-reconcile.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        created = await manager.create(
            parent_project_id=project_id,
            name="fork reconcile",
            history_mode=WorktreeHistoryMode.FORK,
            source_session_id="source",
        )
        await database.execute(
            "UPDATE worktree_intents SET state = 'target_creating' WHERE intent_id = ?",
            (created.intent_id,),
        )
        await database.execute(
            "UPDATE project_worktrees SET state = 'target_creating' WHERE intent_id = ?",
            (created.intent_id,),
        )

        report = await manager.recover()
        recovered = (await manager.list(parent_project_id=project_id))[0]

    assert report.recovered_intents == 1
    assert recovered.state == "ready"
    assert adapter.create_calls == [created.intent_id]


@pytest.mark.asyncio
async def test_registration_race_compensation_finishes_without_project_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with Database(tmp_path / "registration-race-recovery.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        target = tmp_path / "managed worktrees" / project_id / "gone"
        await database.execute(
            """
            INSERT INTO worktree_intents(
                intent_id, parent_project_id, name, branch_name, base_ref,
                history_mode, target_path, state, error_code, created_at, updated_at
            ) VALUES ('registration-race', ?, 'gone', 'copilotd/gone',
                      'HEAD', 'none', ?, 'close_unknown',
                      'compensation_remove_unknown', 1, 1)
            """,
            (project_id, str(target)),
        )

        original_metadata = manager._worktree_metadata

        async def inaccessible(_repo: Path, _target: Path) -> object:
            raise OSError("repository temporarily inaccessible")

        monkeypatch.setattr(manager, "_worktree_metadata", inaccessible)
        first_report = await manager.recover()
        intervened = await database.fetchone(
            """
            SELECT state, error_code FROM worktree_intents
            WHERE intent_id = 'registration-race'
            """
        )
        monkeypatch.setattr(manager, "_worktree_metadata", original_metadata)
        report = await manager.recover()
        intent = await database.fetchone(
            "SELECT state FROM worktree_intents WHERE intent_id = 'registration-race'"
        )

    assert first_report.orphaned_intents == 1
    assert dict(intervened) == {
        "state": "close_unknown",
        "error_code": "compensation_remove_unknown",
    }
    assert report.recovered_intents == 1
    assert intent["state"] == "compensated"


@pytest.mark.asyncio
async def test_concurrent_history_fork_has_one_exclusive_creation_claim(
    tmp_path: Path,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowForkAdapter(DeterministicWorktreeAdapter):
        async def create_history_fork(self, **kwargs: object) -> object:
            entered.set()
            await release.wait()
            return await super().create_history_fork(**kwargs)

    adapter = SlowForkAdapter(history_fork_available=True)
    async with Database(tmp_path / "fork-exclusive.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        first = asyncio.create_task(
            manager.create(
                parent_project_id=project_id,
                name="exclusive fork",
                history_mode=WorktreeHistoryMode.FORK,
                source_session_id="source",
            )
        )
        await entered.wait()
        second = asyncio.create_task(
            manager.create(
                parent_project_id=project_id,
                name="exclusive fork",
                history_mode=WorktreeHistoryMode.FORK,
                source_session_id="source",
            )
        )
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        projection = (await manager.list(parent_project_id=project_id))[0]

    assert projection.state == "ready"
    assert len(adapter.create_calls) == 1
    assert sum(not isinstance(result, Exception) for result in results) >= 1


@pytest.mark.asyncio
async def test_concurrent_git_creation_has_one_fenced_side_effect_holder(
    tmp_path: Path,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowGitRunner:
        def __init__(self) -> None:
            self.delegate = SubprocessGitRunner()
            self.add_calls = 0

        async def run(self, argv: list[str], *, cwd: Path) -> object:
            if argv[1:3] == ["worktree", "add"]:
                self.add_calls += 1
                entered.set()
                await release.wait()
            return await self.delegate.run(argv, cwd=cwd)

    slow_git = SlowGitRunner()
    async with Database(tmp_path / "git-create-exclusive.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        manager._git = slow_git
        manager._git_create_lease_seconds = 0.06
        first = asyncio.create_task(
            manager.create(parent_project_id=project_id, name="exclusive git")
        )
        await entered.wait()
        initial_lease = await database.fetchone(
            """
            SELECT git_create_lease_expires_at FROM worktree_intents
            WHERE parent_project_id = ? AND name = 'exclusive git'
            """,
            (project_id,),
        )
        await asyncio.sleep(0.15)
        renewed_lease = await database.fetchone(
            """
            SELECT git_create_lease_expires_at FROM worktree_intents
            WHERE parent_project_id = ? AND name = 'exclusive git'
            """,
            (project_id,),
        )
        second = asyncio.create_task(
            manager.create(
                parent_project_id=project_id,
                name="exclusive git",
                now=float(renewed_lease["git_create_lease_expires_at"]) + 1,
            )
        )
        try:
            second_result = (
                await asyncio.wait_for(
                    asyncio.gather(second, return_exceptions=True),
                    timeout=2,
                )
            )[0]
        finally:
            release.set()
        first_result = await first
        results = [first_result, second_result]
        projection = (await manager.list(parent_project_id=project_id))[0]
        intent = await database.fetchone(
            """
            SELECT state, git_create_holder, git_create_fence_token
            FROM worktree_intents WHERE intent_id = ?
            """,
            (projection.intent_id,),
        )
        failed_events = await database.fetchone(
            """
            SELECT COUNT(*) FROM worktree_events
            WHERE intent_id = ? AND state = 'failed'
            """,
            (projection.intent_id,),
        )

    assert slow_git.add_calls == 1
    assert (
        renewed_lease["git_create_lease_expires_at"] > initial_lease["git_create_lease_expires_at"]
    )
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert any(isinstance(result, WorktreeConflict) for result in results)
    assert projection.state == "ready"
    assert dict(intent) == {
        "state": "ready",
        "git_create_holder": None,
        "git_create_fence_token": 1,
    }
    assert failed_events[0] == 0


@pytest.mark.asyncio
async def test_restart_invalidates_crashed_git_holder_and_retries_at_lease_expiry(
    tmp_path: Path,
) -> None:
    entered = asyncio.Event()

    class CrashedGitRunner:
        def __init__(self) -> None:
            self.delegate = SubprocessGitRunner()

        async def run(self, argv: list[str], *, cwd: Path) -> object:
            if argv[1:3] == ["worktree", "add"]:
                entered.set()
                await asyncio.Event().wait()
            return await self.delegate.run(argv, cwd=cwd)

    async with Database(tmp_path / "git-create-restart.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        manager._git = CrashedGitRunner()
        manager._git_create_lease_seconds = 0.5
        crashed = asyncio.create_task(
            manager.create(parent_project_id=project_id, name="restart git")
        )
        await entered.wait()
        crashed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await crashed
        abandoned = await database.fetchone(
            """
            SELECT intent_id, git_create_lease_expires_at,
                   git_create_process_generation
            FROM worktree_intents
            WHERE parent_project_id = ? AND name = 'restart git'
            """,
            (project_id,),
        )
        lease_expires_at = float(abandoned["git_create_lease_expires_at"])

        restarted = WorktreeManager(
            database,
            manager._projects,
            worktrees_root=tmp_path / "managed worktrees",
            adapter=DeterministicWorktreeAdapter(),
            process_owner_id="restarted-process",
        )
        early = await restarted.recover()
        scheduled = await database.fetchone(
            """
            SELECT state, git_create_holder, git_create_retry_at,
                   git_create_process_generation
            FROM worktree_intents WHERE intent_id = ?
            """,
            (abandoned["intent_id"],),
        )
        retry_task = restarted._recovery_retry_tasks[str(abandoned["intent_id"])]
        await asyncio.wait_for(asyncio.shield(retry_task), timeout=2)
        projection = (await restarted.list(parent_project_id=project_id))[0]
        recovered = await database.fetchone(
            """
            SELECT state, git_create_holder, git_create_fence_token,
                   git_create_process_generation
            FROM worktree_intents WHERE intent_id = ?
            """,
            (abandoned["intent_id"],),
        )

    assert early.orphaned_intents == 1
    assert dict(scheduled) == {
        "state": "git_creating",
        "git_create_holder": None,
        "git_create_retry_at": lease_expires_at,
        "git_create_process_generation": abandoned["git_create_process_generation"],
    }
    assert projection.state == "ready"
    assert recovered["state"] == "ready"
    assert recovered["git_create_holder"] is None
    assert recovered["git_create_fence_token"] == 2
    assert recovered["git_create_process_generation"] > abandoned["git_create_process_generation"]


@pytest.mark.asyncio
async def test_casefold_path_conflict_is_terminal_and_retryable_after_cleanup(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "casefold-path-retry.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        conflicting = tmp_path / "managed worktrees" / project_id / "CASEFOLD COLLISION"
        conflicting.parent.mkdir(parents=True)
        conflicting.mkdir()

        with pytest.raises(WorktreeConflict, match="conflicts with existing path"):
            await manager.create(
                parent_project_id=project_id,
                name="casefold collision",
            )
        failed = await database.fetchone(
            """
            SELECT state, error_code, git_create_holder, git_create_fence_token
            FROM worktree_intents
            WHERE parent_project_id = ? AND name = 'casefold collision'
            """,
            (project_id,),
        )
        conflicting.rmdir()

        retried = await manager.create(
            parent_project_id=project_id,
            name="casefold collision",
        )
        recovered = await database.fetchone(
            """
            SELECT state, error_code, git_create_holder, git_create_fence_token
            FROM worktree_intents WHERE intent_id = ?
            """,
            (retried.intent_id,),
        )

    assert dict(failed) == {
        "state": "failed",
        "error_code": "worktree_path_conflict",
        "git_create_holder": None,
        "git_create_fence_token": 1,
    }
    assert retried.state == "ready"
    assert dict(recovered) == {
        "state": "ready",
        "error_code": None,
        "git_create_holder": None,
        "git_create_fence_token": 2,
    }


@pytest.mark.asyncio
async def test_branch_conflict_is_terminal_and_retryable_after_cleanup(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "branch-retry.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        parent = await database.fetchone(
            "SELECT root_path FROM projects WHERE id = ?",
            (project_id,),
        )
        repo_root = Path(str(parent["root_path"]))
        name = "branch collision"
        intent_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"copilotd:worktree:{project_id}:{name}",
            )
        )
        branch_name = _branch_name(name, intent_id)
        _git(repo_root, "branch", branch_name)

        with pytest.raises(WorktreeConflict, match="branch already exists"):
            await manager.create(parent_project_id=project_id, name=name)
        failed = await database.fetchone(
            """
            SELECT state, error_code, git_create_holder
            FROM worktree_intents WHERE intent_id = ?
            """,
            (intent_id,),
        )
        _git(repo_root, "branch", "-D", branch_name)

        retried = await manager.create(parent_project_id=project_id, name=name)

    assert dict(failed) == {
        "state": "failed",
        "error_code": "worktree_branch_conflict",
        "git_create_holder": None,
    }
    assert retried.state == "ready"


@pytest.mark.asyncio
async def test_branch_appearing_during_git_command_is_retryable_after_cleanup(
    tmp_path: Path,
) -> None:
    class CommandRaceGitRunner:
        def __init__(self) -> None:
            self.delegate = SubprocessGitRunner()
            self.inject_conflict = True

        async def run(self, argv: list[str], *, cwd: Path) -> GitCommandResult:
            if argv[1:3] == ["worktree", "add"] and self.inject_conflict:
                self.inject_conflict = False
                await self.delegate.run(
                    ["git", "branch", argv[4]],
                    cwd=cwd,
                )
                return GitCommandResult(
                    returncode=128,
                    stdout="",
                    stderr="branch appeared during worktree add",
                )
            return await self.delegate.run(argv, cwd=cwd)

    async with Database(tmp_path / "command-branch-race.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        parent = await database.fetchone(
            "SELECT root_path FROM projects WHERE id = ?",
            (project_id,),
        )
        repo_root = Path(str(parent["root_path"]))
        racing_git = CommandRaceGitRunner()
        manager._git = racing_git

        with pytest.raises(WorktreeConflict, match="appeared"):
            await manager.create(parent_project_id=project_id, name="command race")
        failed = await database.fetchone(
            """
            SELECT intent_id, branch_name, state, error_code
            FROM worktree_intents
            WHERE parent_project_id = ? AND name = 'command race'
            """,
            (project_id,),
        )
        _git(repo_root, "branch", "-D", str(failed["branch_name"]))

        retried = await manager.create(
            parent_project_id=project_id,
            name="command race",
        )

    assert failed["state"] == "failed"
    assert failed["error_code"] == "worktree_branch_conflict"
    assert retried.state == "ready"


@pytest.mark.asyncio
async def test_missing_repository_marks_intervention_and_recovery_continues(
    tmp_path: Path,
) -> None:
    async with Database(tmp_path / "recovery-isolation.sqlite3") as database:
        manager, project_id = await _manager(
            database,
            tmp_path,
            DeterministicWorktreeAdapter(),
        )
        valid = await manager.create(
            parent_project_id=project_id,
            name="valid recovery",
        )
        await database.execute(
            """
            UPDATE project_worktrees SET state = 'project_registered'
            WHERE intent_id = ?
            """,
            (valid.intent_id,),
        )
        missing_project = "missing-project"
        missing_path = tmp_path / "repository-was-removed"
        await database.execute(
            """
            INSERT INTO projects(
                id, channel_id, root_path, cwd, config_version, state,
                project_kind, timezone, created_at, updated_at
            ) VALUES (?, 'missing-channel', ?, ?, 1, 'active',
                      'binding', 'UTC', 0, 0)
            """,
            (missing_project, str(missing_path), str(missing_path)),
        )
        await database.execute(
            """
            INSERT INTO worktree_intents(
                intent_id, parent_project_id, name, branch_name, base_ref,
                history_mode, target_path, state, created_at, updated_at
            ) VALUES ('missing-repo-intent', ?, 'missing', 'copilotd/missing',
                      'HEAD', 'none', ?, 'git_creating', 0, 0)
            """,
            (missing_project, str(tmp_path / "missing-worktree")),
        )
        await database.execute(
            """
            INSERT INTO worktree_intents(
                intent_id, parent_project_id, name, branch_name, base_ref,
                history_mode, target_path, state, created_at, updated_at
            ) VALUES ('invalid-base-intent', ?, 'invalid-base',
                      'copilotd/invalid-base', 'missing-ref', 'none', ?,
                      'reserved', 0.5, 0.5)
            """,
            (
                project_id,
                str(tmp_path / "managed worktrees" / project_id / "invalid-base"),
            ),
        )

        report = await manager.recover()
        missing = await database.fetchone(
            """
            SELECT error_code FROM worktree_intents
            WHERE intent_id = 'missing-repo-intent'
            """
        )
        repaired = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (valid.intent_id,),
        )
        invalid_base = await database.fetchone(
            """
            SELECT state FROM worktree_intents
            WHERE intent_id = 'invalid-base-intent'
            """
        )

    assert report.examined_intents == 3
    assert report.orphaned_intents == 1
    assert report.recovered_intents == 2
    assert missing["error_code"] == "recovery_intervention"
    assert repaired["state"] == "ready"
    assert invalid_base["state"] == "failed"


@pytest.mark.asyncio
async def test_target_reconcile_exception_is_isolated_per_intent(tmp_path: Path) -> None:
    class FailingReconcileAdapter(DeterministicWorktreeAdapter):
        async def reconcile_target(
            self,
            intent: WorktreeIntent,
        ) -> WorktreeTarget | None:
            if intent.name == "failing reconcile":
                raise RuntimeError("adapter unavailable")
            return await super().reconcile_target(intent)

    adapter = FailingReconcileAdapter()
    async with Database(tmp_path / "reconcile-isolation.sqlite3") as database:
        manager, project_id = await _manager(database, tmp_path, adapter)
        failing = await manager.create(
            parent_project_id=project_id,
            name="failing reconcile",
        )
        healthy = await manager.create(
            parent_project_id=project_id,
            name="healthy reconcile",
        )
        await database.execute(
            "UPDATE worktree_intents SET state = 'target_unknown' WHERE intent_id = ?",
            (failing.intent_id,),
        )
        await database.execute(
            "UPDATE project_worktrees SET state = 'target_unknown' WHERE intent_id = ?",
            (failing.intent_id,),
        )

        report = await manager.recover()
        failed_projection = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (failing.intent_id,),
        )
        healthy_projection = await database.fetchone(
            "SELECT state FROM project_worktrees WHERE intent_id = ?",
            (healthy.intent_id,),
        )

    assert report.orphaned_intents == 1
    assert report.recovered_intents >= 1
    assert failed_projection["state"] == "intervention"
    assert healthy_projection["state"] == "ready"
