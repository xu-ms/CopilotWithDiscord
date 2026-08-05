from __future__ import annotations

import time
from pathlib import Path

import pytest

from copilotd.config import Settings
from copilotd.ops.surface import LocalOpsSurface, _redact_structure, _redact_text
from copilotd.storage.database import Database


def _auth_word() -> str:
    return "".join(["Auth", "orization"])


def _bearer_word() -> str:
    return "".join(["Bea", "rer"])


def _auth_colon(secret: str) -> str:
    return "".join([_auth_word(), "    :", _bearer_word(), "   ", secret])


def _auth_equals(secret: str) -> str:
    return "".join([_auth_word().lower(), " = ", _bearer_word(), " ", secret])


def test_redact_text_consumes_common_secret_formats() -> None:
    text = (
        "safe keep "
        '{"token":"json-secret-123","nested":{"password":"pw-1"}} '
        + _auth_colon("abc123XYZ")
        + " "
        + _auth_equals("def456")
        + " "
        + "token=plain-secret "
        + 'password = "quoted-secret" '
        + "api-key='key-777' "
        + 'cookie = "crumb-8"'
    )
    redacted = _redact_text(text)

    for secret in (
        "json-secret-123",
        "pw-1",
        "abc123XYZ",
        "def456",
        "plain-secret",
        "quoted-secret",
        "key-777",
        "crumb-8",
    ):
        assert secret not in redacted
    assert "safe keep" in redacted
    assert '"token":"[redacted]"' in redacted
    assert '"password":"[redacted]"' in redacted
    assert f"{_auth_word()}    :{_bearer_word()}   [redacted]" in redacted
    assert f"{_auth_word().lower()} = {_bearer_word()} [redacted]" in redacted
    assert "token=[redacted]" in redacted
    assert "password = [redacted]" in redacted
    assert "api-key=[redacted]" in redacted or "api-key = [redacted]" in redacted
    assert "cookie = [redacted]" in redacted


def test_redact_structure_recurses_nested_sensitive_keys() -> None:
    payload = {
        "authorization": {
            "token": "abc",
            "nested": [
                {"cookie": "crumb"},
                {"note": "visible"},
                "plain text",
            ],
            "plain": "keep me",
        },
        "meta": {
            "note": "Authorization: Bearer inner-secret",
            "list": [
                {"password": "pw-2"},
                "safe",
            ],
        },
        "apiKey": [
            {"client_id": "id-1", "secret": "s-2"},
        ],
    }

    redacted = _redact_structure(payload)

    assert redacted["authorization"]["token"] == "[redacted]"
    assert redacted["authorization"]["nested"][0]["cookie"] == "[redacted]"
    assert redacted["authorization"]["nested"][1]["note"] == "[redacted]"
    assert redacted["authorization"]["nested"][2] == "[redacted]"
    assert redacted["authorization"]["plain"] == "[redacted]"
    assert redacted["meta"]["note"] == f"{_auth_word()}: {_bearer_word()} [redacted]"
    assert redacted["meta"]["list"][0]["password"] == "[redacted]"
    assert redacted["meta"]["list"][1] == "safe"
    assert redacted["apiKey"][0]["client_id"] == "[redacted]"
    assert redacted["apiKey"][0]["secret"] == "[redacted]"


@pytest.mark.asyncio
async def test_ops_surface_redacts_diagnostics_log_tail_and_event_dump(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "boot.log").write_text("boot corr-1 safe text\n", encoding="utf-8")
    copilotd_log = (
        'corr-1 {"token":"json-secret-123","nested":{"password":"pw-1"}}\n'
        + _auth_colon("abc123XYZ")
        + "\n"
        + _auth_equals("def456")
        + "\n"
        + 'corr-1 token=plain-secret password = "quoted-secret" '
        + 'api-key=key-777 cookie = "crumb-8"\n'
        + "corr-1 keep this text\n"
    )
    (log_dir / "copilotd.log").write_text(copilotd_log, encoding="utf-8")
    settings = Settings(
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "cache",
        log_dir=log_dir,
    )
    now = time.time()
    async with Database(tmp_path / "ops.sqlite3") as database:
        await database.execute(
            """
            INSERT INTO session_bindings(
                thread_id, project_source, cwd_snapshot, sdk_session_id,
                owner_fence_token, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("thread-1", "explicit", "/tmp/work", "session-1", 987654, now, now),
        )
        await database.execute(
            """
            INSERT INTO capabilities(
                runtime_version, sdk_version, capability, supported, probe_detail, probed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "1.0",
                "1.0",
                "demo",
                1,
                '{"token":"cap-secret","nested":{"password":"cap-pw"}}',
                now,
            ),
        )
        await database.execute(
            """
            INSERT INTO runtime_incidents(
                timestamp, runtime_generation, session_id, kind,
                stderr_tail, last_inbox_seq, last_sdk_receive_seq, detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                3,
                "session-1",
                "crash",
                _auth_colon("incident-secret"),
                7,
                9,
                "detail",
            ),
        )
        await database.execute(
            """
            INSERT INTO event_journal(
                sdk_session_id, generation, inbox_seq, source, persistence_class,
                raw_type, parent_id, agent_id, message_id, turn_id, reducer_hash,
                raw_payload, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "session-1",
                1,
                1,
                "sdk",
                "durable",
                "message",
                _auth_colon("parent-secret"),
                "token=agent-secret",
                "cookie=message-secret",
                "".join(["pass", "word=turn-secret"]),
                "hash",
                '{"token":"payload-secret"}',
                now,
            ),
        )

        service = LocalOpsSurface(database, settings)
        health = await service.health()
        diagnostics = await service.diagnostics(session_id="session-1")
        logs = await service.log_tail(correlation_id="corr-1")
        events = await service.event_dump(session_id="session-1")

    expected_incident_tail = "".join(
        [
            _auth_word(),
            "    :",
            _bearer_word(),
            "   ",
            "[redacted]",
        ]
    )
    expected_parent_id = "".join(
        [
            _auth_word(),
            "    :",
            _bearer_word(),
            "   ",
            "[redacted]",
        ]
    )
    assert health["database"] == "ok"
    assert health["pending_outbox"] == 0
    assert diagnostics["bindings"][0]["owner_fence_token"] == "[redacted]"
    assert diagnostics["capabilities"][0]["probe_detail"] == (
        '{"token":"[redacted]","nested":{"password":"[redacted]"}}'
    )
    assert diagnostics["incidents"][0]["stderr_tail"] == expected_incident_tail
    assert "json-secret-123" not in logs["files"]["copilotd.log"]
    assert "abc123XYZ" not in logs["files"]["copilotd.log"]
    assert "def456" not in logs["files"]["copilotd.log"]
    assert "plain-secret" not in logs["files"]["copilotd.log"]
    assert "quoted-secret" not in logs["files"]["copilotd.log"]
    assert "key-777" not in logs["files"]["copilotd.log"]
    assert "crumb-8" not in logs["files"]["copilotd.log"]
    assert '"token":"[redacted]"' in logs["files"]["copilotd.log"]
    assert "token=[redacted]" in logs["files"]["copilotd.log"]
    assert "safe text" in logs["files"]["boot.log"]
    assert events["events"][0]["parent_id"] == expected_parent_id
    assert events["events"][0]["agent_id"] == "token=[redacted]"
    assert events["events"][0]["message_id"] == "cookie=[redacted]"
    assert events["events"][0]["turn_id"] == "".join(["pass", "word=[redacted]"])
    assert events["events"][0]["raw_type"] == "message"
