from __future__ import annotations

from pathlib import Path
from typing import Any

from copilotd.core.bindings import (
    AttachmentState,
    BindingIntent,
    SessionBinding,
    SessionBindingRepository,
)
from copilotd.core.projects import ProjectConfigSnapshot, ProjectSnapshot, ProjectSource
from copilotd.core.scheduler import (
    ScheduleDefinition,
    ScheduledTarget,
    SchedulerDispatchError,
    SchedulerErrorCategory,
    ScheduleRun,
)
from copilotd.core.session_runtime import RuntimeState
from copilotd.core.sessions import (
    SessionCreationService,
    SessionCreationUnknown,
    SessionRegistry,
)
from copilotd.sdk.capabilities import CapabilityManifest
from copilotd.storage.database import Database


class ApplicationSchedulerAdapter:
    """Bridges app schedule runs to durable sessions without exposing SDK internals."""

    def __init__(
        self,
        database: Database,
        bindings: SessionBindingRepository,
        sessions: SessionRegistry,
        creation: SessionCreationService | None,
        capabilities: CapabilityManifest | None = None,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._sessions = sessions
        self._creation = creation
        self._capabilities = capabilities

    async def prepare_message_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget:
        snapshot = definition.target_snapshot
        thread_id = str(snapshot.get("thread_id", ""))
        session_id = str(snapshot.get("sdk_session_id", ""))
        if not thread_id or not session_id:
            raise SchedulerDispatchError(
                "message schedule has an incomplete immutable target",
                category=SchedulerErrorCategory.INPUT,
                code="message_target_incomplete",
            )
        binding = await self._bindings.by_thread(thread_id)
        if binding is None or binding.sdk_session_id != session_id:
            raise SchedulerDispatchError(
                "message schedule target no longer matches its session binding",
                category=SchedulerErrorCategory.TARGET,
                code="message_target_mismatch",
                target_unknown=True,
            )
        temporary = binding.binding_intent == BindingIntent.CLOSED
        binding = await self._prepare_binding(binding, temporary=temporary)
        return ScheduledTarget(
            project_id=binding.project_id,
            thread_id=binding.thread_id,
            sdk_session_id=binding.sdk_session_id,
            temporary_attachment=temporary,
        )

    async def prepare_new_session_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget:
        project_payload = definition.target_snapshot.get("project")
        config_payload = definition.target_snapshot.get("project_config")
        if not isinstance(project_payload, dict) or not isinstance(config_payload, dict):
            raise SchedulerDispatchError(
                "new-session schedule is missing its immutable project snapshot",
                category=SchedulerErrorCategory.INPUT,
                code="new_session_snapshot_incomplete",
            )
        project = _project_snapshot(project_payload)
        config = ProjectConfigSnapshot.from_dict(config_payload)
        if run.result_session_id is None:
            raise SchedulerDispatchError(
                "new-session run did not preallocate a session id",
                category=SchedulerErrorCategory.INTERNAL,
                code="session_id_not_preallocated",
            )
        if self._creation is None:
            raise SchedulerDispatchError(
                "new-session schedule adapter is unavailable",
                category=SchedulerErrorCategory.CAPABILITY,
                code="new_session_adapter_unavailable",
                blocked=True,
            )
        try:
            runtime = await self._creation.create_from_source(
                channel_id=project.channel_id,
                source_kind="schedule",
                source_id=run.run_id,
                prompt="",
                thread_name=str(definition.payload.get("thread_name", "Scheduled session")),
                send_initial_prompt=False,
                project_snapshot=project,
                config_snapshot=config,
                preallocated_session_id=run.result_session_id,
            )
        except SessionCreationUnknown as error:
            raise SchedulerDispatchError(
                str(error),
                category=SchedulerErrorCategory.TARGET,
                code=type(error).__name__,
                target_unknown=True,
            ) from error
        except Exception as error:
            raise SchedulerDispatchError(
                str(error),
                category=SchedulerErrorCategory.TARGET,
                code=type(error).__name__,
                retryable=True,
            ) from error
        return ScheduledTarget(
            project_id=runtime.binding.project_id,
            thread_id=runtime.binding.thread_id,
            sdk_session_id=runtime.binding.sdk_session_id,
        )

    async def reconcile_target(
        self,
        definition: ScheduleDefinition,
        run: ScheduleRun,
    ) -> ScheduledTarget | None:
        if run.result_thread_id is not None and run.result_session_id is not None:
            binding = await self._bindings.by_thread(run.result_thread_id)
            if binding is not None and binding.sdk_session_id == run.result_session_id:
                binding = await self._prepare_binding(
                    binding,
                    temporary=binding.binding_intent == BindingIntent.CLOSED,
                )
                return ScheduledTarget(
                    project_id=binding.project_id,
                    thread_id=binding.thread_id,
                    sdk_session_id=binding.sdk_session_id,
                    temporary_attachment=binding.binding_intent == BindingIntent.CLOSED,
                )
        if definition.kind.value == "new_session":
            return await self.prepare_new_session_target(definition, run)
        return None

    async def queue_ready(self, target: ScheduledTarget, run_id: str) -> None:
        del run_id
        runtime = self._sessions.for_thread(target.thread_id)
        if runtime is None:
            raise SchedulerDispatchError(
                "scheduled target runtime disappeared after queue insertion",
                category=SchedulerErrorCategory.RUNTIME,
                code="runtime_disappeared_after_queue",
                dispatch_unknown=False,
                retryable=False,
            )
        await runtime.dispatch_queued_once()

    async def release_temporary_target(
        self,
        target: ScheduledTarget,
        run: ScheduleRun,
    ) -> None:
        runtime = self._sessions.for_thread(target.thread_id)
        if runtime is None or runtime.state == RuntimeState.CLOSED:
            binding = await self._bindings.by_thread(target.thread_id)
            if binding is not None and binding.attachment_reason == "scheduler_run":
                await self._bindings.set_attachment_reason(binding, None)
            return
        await runtime.close(
            idempotency_key=f"schedule:{run.run_id}:temporary-detach",
            force=False,
        )
        binding = await self._bindings.by_thread(target.thread_id)
        if binding is not None and binding.attachment_reason == "scheduler_run":
            await self._bindings.set_attachment_reason(binding, None)

    async def _prepare_binding(
        self,
        binding: SessionBinding,
        *,
        temporary: bool,
    ) -> SessionBinding:
        if binding.binding_intent not in {BindingIntent.ACTIVE, BindingIntent.CLOSED}:
            raise SchedulerDispatchError(
                f"session binding is not schedulable: {binding.binding_intent}",
                category=SchedulerErrorCategory.TARGET,
                code="binding_not_schedulable",
                blocked=True,
            )
        if temporary and binding.attachment_reason != "scheduler_run":
            binding = await self._bindings.set_attachment_reason(binding, "scheduler_run")
        runtime = self._sessions.for_thread(binding.thread_id)
        if runtime is None or runtime.state in {
            RuntimeState.CLOSED,
            RuntimeState.FENCED,
            RuntimeState.RECOVERY_UNKNOWN,
        }:
            runtime = await self._sessions.replace(binding)
        if runtime.state == RuntimeState.DETACHED:
            try:
                await runtime.attach_resume()
            except Exception as error:
                raise SchedulerDispatchError(
                    str(error),
                    category=SchedulerErrorCategory.RUNTIME,
                    code=type(error).__name__,
                    retryable=True,
                ) from error
        latest = await self._bindings.by_thread(binding.thread_id)
        if latest is None or latest.attachment_state != AttachmentState.ATTACHED:
            raise SchedulerDispatchError(
                "scheduled session did not reach attached state",
                category=SchedulerErrorCategory.RUNTIME,
                code="target_not_attached",
                retryable=True,
            )
        return latest


def _project_snapshot(payload: dict[str, Any]) -> ProjectSnapshot:
    return ProjectSnapshot(
        project_id=(None if payload.get("project_id") is None else str(payload["project_id"])),
        channel_id=str(payload["channel_id"]),
        source=ProjectSource(str(payload["source"])),
        root_path=Path(str(payload["root_path"])),
        cwd=Path(str(payload["cwd"])),
        config_version=int(payload.get("config_version", 1)),
        timezone=str(payload.get("timezone", "UTC")),
    )
