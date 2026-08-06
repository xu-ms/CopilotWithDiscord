from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

from copilot.session_events import SessionEventType

from copilotd.config import Settings
from copilotd.storage.database import Database

CAPABILITY_SCHEMA_VERSION = 1
PINNED_SDK_VERSION = "1.0.8"
PINNED_RUNTIME_VERSION = "1.0.73"
PINNED_PROTOCOL_VERSION = 3
PINNED_GENERATED_EVENT_COUNT = 114
PINNED_GENERATED_EVENT_SHA256 = "b7aed29d812cf032a5f68343b95b15c5f1c6735bde5140670692e6fa5fd0d1a2"
MAIN_BRANCH_ONLY_EVENTS = (
    "factory.run_updated",
    "session.context_cleared",
)
CHECKED_CAPABILITY_FIXTURE_SHA256 = (
    "19d1e08091c55b0fc06ee22172f5ebf2f5c79407c780d6f191613db76df72b12"
)

_REQUIRED_CAPABILITIES = frozenset(
    {
        "accepted_user_event_id_mapping",
        "activity_snapshot",
        "builtin_commands",
        "context_info",
        "detached_continuation",
        "event_log",
        "hook_agent_stop",
        "hook_user_prompt_transformed",
        "config_reattach",
        "managed_permission_handler",
        "mcp_http",
        "mcp_stdio",
        "model_config",
        "models",
        "native_queue_snapshot",
        "native_schedule",
        "permission_allow_all",
        "persistent_history",
        "pre_registered_on_event",
        "protocol_elicitation",
        "protocol_external_tool",
        "protocol_mcp_headers_response",
        "protocol_mcp_oauth",
        "protocol_sampling_response",
        "protocol_session_limits_response",
        "reasoning_summary_readback",
        "remote",
        "selected_agent",
        "session_extension_config",
        "session_hooks",
        "session_mode",
        "sessions_check_in_use",
        "task_snapshot",
        "usage",
    }
)
_REQUIRED_STARTUP_CAPABILITIES = frozenset(
    {
        "activity_snapshot",
        "native_queue_snapshot",
        "permission_allow_all",
        "persistent_history",
        "pre_registered_on_event",
        "managed_permission_handler",
        "session_extension_config",
        "session_hooks",
        "session_mode",
    }
)
_CORE_DISCORD_ROOTS = frozenset({"project", "queue", "session", "steer"})
_GATED_DISCORD_ROOTS: dict[str, frozenset[str]] = {
    "autopilot": frozenset({"session_mode"}),
    "context": frozenset({"context_info"}),
    "model": frozenset({"model_config", "models"}),
    "plan": frozenset({"session_mode"}),
    "usage": frozenset({"usage"}),
}


class CapabilityError(RuntimeError):
    pass


class CapabilityFixtureError(CapabilityError):
    pass


class RuntimeIdentityMismatch(CapabilityError):
    pass


class RequiredCapabilityMissing(CapabilityError):
    pass


@dataclass(frozen=True, slots=True)
class RuntimeIdentity:
    sdk_version: str
    runtime_version: str
    protocol_version: int
    ping_protocol_version: int

    @classmethod
    def from_runtime_payload(cls, payload: dict[str, Any]) -> RuntimeIdentity:
        identity = cls(
            sdk_version=version("github-copilot-sdk"),
            runtime_version=str(payload["runtime_version"]),
            protocol_version=int(payload["protocol_version"]),
            ping_protocol_version=int(payload["ping_protocol_version"]),
        )
        if identity.protocol_version != identity.ping_protocol_version:
            raise RuntimeIdentityMismatch(
                "runtime status and ping protocol versions disagree: "
                f"{identity.protocol_version} != {identity.ping_protocol_version}"
            )
        return identity


@dataclass(frozen=True, slots=True)
class CapabilityEvidence:
    name: str
    supported: bool | None
    evidence_kind: str
    detail: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "supported": self.supported,
            "evidence_kind": self.evidence_kind,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class CapabilityManifest:
    schema_version: int
    source: str
    generated_at: str
    identity: RuntimeIdentity
    generated_event_count: int
    generated_event_sha256: str
    main_branch_only_events: tuple[str, ...]
    capabilities: dict[str, CapabilityEvidence]
    fixture_path: Path
    fixture_sha256: str

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        fixture_path: Path,
        fixture_sha256: str,
    ) -> CapabilityManifest:
        try:
            identity_payload = payload["identity"]
            event_payload = payload["generated_events"]
            capability_payload = payload["capabilities"]
            manifest = cls(
                schema_version=int(payload["schema_version"]),
                source=str(payload["source"]),
                generated_at=str(payload["generated_at"]),
                identity=RuntimeIdentity(
                    sdk_version=str(identity_payload["sdk_version"]),
                    runtime_version=str(identity_payload["runtime_version"]),
                    protocol_version=int(identity_payload["protocol_version"]),
                    ping_protocol_version=int(identity_payload["ping_protocol_version"]),
                ),
                generated_event_count=int(event_payload["count"]),
                generated_event_sha256=str(event_payload["sha256"]),
                main_branch_only_events=tuple(event_payload["main_branch_only"]),
                capabilities={
                    str(name): CapabilityEvidence(
                        name=str(name),
                        supported=(
                            None if evidence["supported"] is None else bool(evidence["supported"])
                        ),
                        evidence_kind=str(evidence["evidence_kind"]),
                        detail=evidence["detail"],
                    )
                    for name, evidence in capability_payload.items()
                },
                fixture_path=fixture_path,
                fixture_sha256=fixture_sha256,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CapabilityFixtureError(
                f"invalid capability fixture structure: {fixture_path}"
            ) from error
        manifest.validate()
        return manifest

    def validate(self) -> None:
        if self.schema_version != CAPABILITY_SCHEMA_VERSION:
            raise CapabilityFixtureError(f"unsupported capability schema {self.schema_version}")
        if self.identity.protocol_version != self.identity.ping_protocol_version:
            raise CapabilityFixtureError("fixture protocol versions disagree")
        installed_events = sorted(item.value for item in SessionEventType)
        installed_hash = hashlib.sha256("\n".join(installed_events).encode()).hexdigest()
        if self.identity.sdk_version == PINNED_SDK_VERSION:
            if self.generated_event_count != PINNED_GENERATED_EVENT_COUNT:
                raise CapabilityFixtureError(
                    "SDK 1.0.8 fixture must record exactly 114 generated events"
                )
            if self.generated_event_sha256 != PINNED_GENERATED_EVENT_SHA256:
                raise CapabilityFixtureError("SDK 1.0.8 event inventory hash is invalid")
        if self.identity.sdk_version == version("github-copilot-sdk") and (
            self.generated_event_count != len(installed_events)
            or self.generated_event_sha256 != installed_hash
        ):
            raise CapabilityFixtureError(
                "fixture generated-event inventory does not match the installed SDK"
            )
        if tuple(self.main_branch_only_events) != MAIN_BRANCH_ONLY_EVENTS:
            raise CapabilityFixtureError("main-branch-only event inventory is invalid")
        installed = set(installed_events)
        if installed.intersection(self.main_branch_only_events):
            raise CapabilityFixtureError(
                "main-branch-only events must not be claimed by the pinned SDK"
            )
        missing = _REQUIRED_CAPABILITIES.difference(self.capabilities)
        if missing:
            raise CapabilityFixtureError(
                f"capability fixture is incomplete: {', '.join(sorted(missing))}"
            )

    def matches(self, identity: RuntimeIdentity) -> bool:
        return self.identity == identity

    def supports(self, capability: str) -> bool:
        evidence = self.capabilities.get(capability)
        return evidence is not None and evidence.supported is True

    def with_checked_fallback(
        self,
        checked: CapabilityManifest,
    ) -> CapabilityManifest:
        if self.identity != checked.identity:
            raise RuntimeIdentityMismatch(
                "live and checked capability evidence identities do not match"
            )
        merged: dict[str, CapabilityEvidence] = {}
        for name in sorted(set(self.capabilities) | set(checked.capabilities)):
            live = self.capabilities.get(name)
            fallback = checked.capabilities.get(name)
            if live is not None and (
                live.supported is not None or live.evidence_kind != "unprobed"
            ):
                merged[name] = live
                continue
            if fallback is None:
                if live is not None:
                    merged[name] = live
                continue
            merged[name] = CapabilityEvidence(
                name=name,
                supported=fallback.supported,
                evidence_kind=f"checked-fallback:{fallback.evidence_kind}",
                detail={
                    "live": None if live is None else live.detail,
                    "checked": fallback.detail,
                },
            )
        manifest = CapabilityManifest(
            schema_version=self.schema_version,
            source=f"{self.source}+checked-fallback",
            generated_at=self.generated_at,
            identity=self.identity,
            generated_event_count=self.generated_event_count,
            generated_event_sha256=self.generated_event_sha256,
            main_branch_only_events=self.main_branch_only_events,
            capabilities=merged,
            fixture_path=self.fixture_path,
            fixture_sha256=self.fixture_sha256,
        )
        manifest.validate()
        return manifest

    def require_startup_capabilities(self) -> None:
        missing = sorted(name for name in _REQUIRED_STARTUP_CAPABILITIES if not self.supports(name))
        if missing:
            raise RequiredCapabilityMissing(
                f"required runtime capabilities are not evidenced: {', '.join(missing)}"
            )

    def discord_command_roots(self) -> frozenset[str]:
        roots = set(_CORE_DISCORD_ROOTS)
        for root, requirements in _GATED_DISCORD_ROOTS.items():
            if all(self.supports(capability) for capability in requirements):
                roots.add(root)
        return frozenset(roots)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source": self.source,
            "generated_at": self.generated_at,
            "identity": {
                "sdk_version": self.identity.sdk_version,
                "runtime_version": self.identity.runtime_version,
                "protocol_version": self.identity.protocol_version,
                "ping_protocol_version": self.identity.ping_protocol_version,
            },
            "generated_events": {
                "count": self.generated_event_count,
                "sha256": self.generated_event_sha256,
                "main_branch_only": list(self.main_branch_only_events),
            },
            "capabilities": {
                name: evidence.to_dict() for name, evidence in sorted(self.capabilities.items())
            },
            "fixture": {
                "path": str(self.fixture_path),
                "sha256": self.fixture_sha256,
            },
        }


class CapabilityRegistry:
    def __init__(
        self,
        settings: Settings,
        *,
        checked_fixture_path: Path | None = None,
        checked_fixture_sha256: str = CHECKED_CAPABILITY_FIXTURE_SHA256,
    ) -> None:
        self._settings = settings
        self._checked_fixture_path = checked_fixture_path or (
            Path(__file__).parent / "fixtures" / "capabilities-sdk-1.0.8-runtime-1.0.73.json"
        )
        self._checked_fixture_sha256 = checked_fixture_sha256

    def load_checked(self) -> CapabilityManifest:
        return self._load_hashed_fixture(
            self._checked_fixture_path,
            expected_sha256=self._checked_fixture_sha256,
        )

    def load_local(self) -> CapabilityManifest | None:
        path = self._settings.capability_path
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            fixture = payload["fixture"]
            fixture_path = Path(str(fixture["path"]))
            if not fixture_path.is_absolute():
                fixture_path = path.parent / fixture_path
            expected_hash = str(fixture["sha256"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise CapabilityFixtureError(f"invalid local capability evidence: {path}") from error
        self._assert_file_hash(fixture_path, expected_hash)
        return CapabilityManifest.from_dict(
            payload,
            fixture_path=fixture_path,
            fixture_sha256=expected_hash,
        )

    def resolve(self, runtime_payload: dict[str, Any]) -> CapabilityManifest:
        identity = RuntimeIdentity.from_runtime_payload(runtime_payload)
        local = self.load_local()
        checked = self.load_checked()
        candidates = [manifest for manifest in (local, checked) if manifest is not None]
        if local is not None and local.matches(identity):
            manifest = local.with_checked_fallback(checked) if checked.matches(identity) else local
            manifest.require_startup_capabilities()
            return manifest
        if checked.matches(identity):
            checked.require_startup_capabilities()
            return checked
        expected = ", ".join(
            (
                f"{item.identity.sdk_version}/"
                f"{item.identity.runtime_version}/"
                f"{item.identity.protocol_version}"
            )
            for item in candidates
        )
        actual = f"{identity.sdk_version}/{identity.runtime_version}/{identity.protocol_version}"
        raise RuntimeIdentityMismatch(
            f"runtime identity {actual} has no checked capability evidence; expected {expected}"
        )

    async def activate(
        self,
        database: Database,
        runtime_payload: dict[str, Any],
    ) -> CapabilityManifest:
        manifest = self.resolve(runtime_payload)
        now = time.time()
        async with database.transaction() as connection:
            for evidence in manifest.capabilities.values():
                await connection.execute(
                    """
                    INSERT INTO capabilities(
                        runtime_version, sdk_version, protocol_version,
                        ping_protocol_version, capability, supported,
                        evidence_status, evidence_kind, probe_detail,
                        fixture_path, fixture_sha256,
                        generated_event_count, event_types_sha256, source, probed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(runtime_version, sdk_version, protocol_version, capability)
                    DO UPDATE SET
                        ping_protocol_version = excluded.ping_protocol_version,
                        supported = excluded.supported,
                        evidence_status = excluded.evidence_status,
                        evidence_kind = excluded.evidence_kind,
                        probe_detail = excluded.probe_detail,
                        fixture_path = excluded.fixture_path,
                        fixture_sha256 = excluded.fixture_sha256,
                        generated_event_count = excluded.generated_event_count,
                        event_types_sha256 = excluded.event_types_sha256,
                        source = excluded.source,
                        probed_at = excluded.probed_at
                    """,
                    (
                        manifest.identity.runtime_version,
                        manifest.identity.sdk_version,
                        manifest.identity.protocol_version,
                        manifest.identity.ping_protocol_version,
                        evidence.name,
                        -1 if evidence.supported is None else int(evidence.supported),
                        (
                            "unknown"
                            if evidence.supported is None
                            else "supported"
                            if evidence.supported
                            else "unsupported"
                        ),
                        evidence.evidence_kind,
                        json.dumps(evidence.detail, ensure_ascii=False, sort_keys=True),
                        str(manifest.fixture_path),
                        manifest.fixture_sha256,
                        manifest.generated_event_count,
                        manifest.generated_event_sha256,
                        manifest.source,
                        now,
                    ),
                )
        return manifest

    def _load_hashed_fixture(
        self,
        path: Path,
        *,
        expected_sha256: str,
    ) -> CapabilityManifest:
        self._assert_file_hash(path, expected_sha256)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise CapabilityFixtureError(f"invalid capability fixture JSON: {path}") from error
        return CapabilityManifest.from_dict(
            payload,
            fixture_path=path,
            fixture_sha256=expected_sha256,
        )

    @classmethod
    def _assert_file_hash(cls, path: Path, expected_sha256: str) -> None:
        if not path.is_file():
            raise CapabilityFixtureError(f"capability fixture is missing: {path}")
        actual = cls._sha256(path)
        if actual != expected_sha256:
            raise CapabilityFixtureError(
                f"capability fixture hash mismatch for {path}: {actual} != {expected_sha256}"
            )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
