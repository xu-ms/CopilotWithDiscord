from __future__ import annotations

import time
from pathlib import Path

import pytest

from copilotd.config import Settings
from copilotd.ops.surface import LocalOpsSurface, _redact_structure, _redact_text
from copilotd.storage.database import Database

NL = chr(10)


def _authorization() -> str:
    return "".join(["Auth", "orization"])


def _cookie() -> str:
    return "".join(["Co", "okie"])


def _set_cookie() -> str:
    return "".join(["Set-", "Cookie"])


def _aws_secret() -> str:
    return "".join(["AWS_", "SECRET_", "ACCESS_", "KEY"])


def test_redact_text_consumes_sensitive_header_json_and_preserves_benign_keys() -> None:
    text = (
        "safe keep "
        + "probe_detail={"
        + '"credentials":{"value":{"nested":"cred-secret"}},'
        + '"secret":{"nested":{"value":"secret-secret"}},'
        + '"secret_count":7,'
        + '"credential_type":"basic",'
        + '"has_secret":false,'
        + '"nonsecret":"keep",'
        + '"public_metadata":"show"}'
        + NL
        + "Authorization    :   Basic abc def ghi"
        + NL
        + f"{_cookie()}   : session=abc; token=xyz; Path=/; HttpOnly"
        + NL
        + f"{_set_cookie()} : sid=abc; Path=/; Secure"
        + NL
        + f"{_aws_secret()} = very-secret"
        + NL
        + 'credentials = {"value":{"nested":"cred-secret"}} '
        + 'secret = {"nested":{"value":"secret-secret"}} '
        + 'webhook_secret = {"nested":{"token":"hook-secret"}} '
        + 'passphrase = {"nested":{"token":"phrase-secret"}} '
        + "payload = {'meta': {'secret': {'nested': 'python-secret'}}} "
        + "nonsecret=keep-me"
    )
    redacted = _redact_text(text)

    for secret in (
        "cred-secret",
        "secret-secret",
        "abc def ghi",
        "session=abc",
        "token=xyz",
        "sid=abc",
        "very-secret",
        "hook-secret",
        "phrase-secret",
        "python-secret",
    ):
        assert secret not in redacted
    assert (
        'probe_detail={"credentials":"[redacted]","secret":"[redacted]",'
        '"secret_count":7,"credential_type":"basic","has_secret":false,'
        '"nonsecret":"keep","public_metadata":"show"}' in redacted
    )
    assert "Authorization    :   [redacted]" in redacted
    assert f"{_cookie()}   : [redacted]" in redacted
    assert f"{_set_cookie()} : [redacted]" in redacted
    assert f"{_aws_secret()} = [redacted]" in redacted
    assert "credentials = [redacted]" in redacted
    assert "secret = [redacted]" in redacted
    assert "webhook_secret = [redacted]" in redacted
    assert "passphrase = [redacted]" in redacted
    assert "'secret': [redacted]" in redacted
    assert "nonsecret=keep-me" in redacted


def test_redact_structure_redacts_entire_sensitive_containers() -> None:
    payload = {
        "authorization": {"value": {"nested": "x"}},
        "credentials": {"value": {"nested": "y"}},
        "secret": {"nested": [{"token": "t"}]},
        "webhook_secret": {"nested": "hook"},
        "passphrase": {"nested": "phrase"},
        "meta": {
            "note": "Authorization: Basic structured-secret",
            "list": [
                {"password": "pw-2"},
                {"private_key": "priv"},
                {"client_secret": "client"},
            ],
        },
        "secret_count": 9,
        "credential_type": "oauth",
        "has_secret": False,
        "nonsecret": "keep",
        "public_metadata": "visible",
    }

    redacted = _redact_structure(payload)

    assert redacted["authorization"] == "[redacted]"
    assert redacted["credentials"] == "[redacted]"
    assert redacted["secret"] == "[redacted]"
    assert redacted["webhook_secret"] == "[redacted]"
    assert redacted["passphrase"] == "[redacted]"
    assert redacted["meta"]["note"] == "Authorization: [redacted]"
    assert redacted["meta"]["list"][0]["password"] == "[redacted]"
    assert redacted["meta"]["list"][1]["private_key"] == "[redacted]"
    assert redacted["meta"]["list"][2]["client_secret"] == "[redacted]"
    assert redacted["secret_count"] == 9
    assert redacted["credential_type"] == "oauth"
    assert redacted["has_secret"] is False
    assert redacted["nonsecret"] == "keep"
    assert redacted["public_metadata"] == "visible"


@pytest.mark.parametrize(
    "value",
    [
        'payload={"nested":{"password":"truncated-secret',
        'payload={"nested":{"password":{"value":"truncated-secret',
        'password="truncated-secret',
    ],
)
def test_redact_text_redacts_unterminated_sensitive_fragments(value: str) -> None:
    redacted = _redact_text(value)

    assert "truncated-secret" not in redacted
    assert "[redacted]" in redacted


@pytest.mark.asyncio
async def test_ops_surface_redacts_diagnostics_log_tail_and_event_dump(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "boot.log").write_text("boot corr-1 safe text" + NL, encoding="utf-8")
    json_blob = (
        '{"credentials":{"value":{"nested":"cred-secret"}},'
        '"secret":{"nested":{"value":"secret-secret"}},'
        '"secret_count":7,"credential_type":"basic",'
        '"has_secret":false,"nonsecret":"keep",'
        '"public_metadata":"show"}'
    )
    copilotd_log = (
        "corr-1 "
        + json_blob
        + NL
        + "corr-1 Authorization    :   Basic abc def ghi"
        + NL
        + "corr-1 Cookie   = session=abc; token=xyz; Path=/; HttpOnly"
        + NL
        + "corr-1 Set-Cookie : sid=abc; Path=/; Secure"
        + NL
        + f"corr-1 {_aws_secret()} = very-secret"
        + NL
        + 'corr-1 credentials = {"value":{"nested":"cred-secret"}}'
        + NL
        + 'corr-1 secret = {"nested":{"value":"secret-secret"}}'
        + NL
        + 'corr-1 webhook_secret = {"nested":{"token":"hook-secret"}}'
        + NL
        + 'corr-1 passphrase = {"nested":{"token":"phrase-secret"}}'
        + NL
        + "corr-1 secret_count=7 credential_type=basic has_secret=true "
        + 'nonsecret=keep-me public_metadata="visible"'
        + NL
        + "corr-1 keep this text"
        + NL
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
                runtime_version, sdk_version, protocol_version,
                ping_protocol_version, capability, supported, evidence_kind,
                probe_detail, fixture_path, fixture_sha256,
                generated_event_count, event_types_sha256, source, probed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1.0",
                "1.0",
                1,
                1,
                "demo",
                1,
                "test",
                "{}",
                "",
                "",
                0,
                "",
                "test",
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
                None,
                7,
                9,
                "{}",
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
                "parent-id",
                "agent-id",
                "message-id",
                "turn-id",
                "hash",
                '{"payload_state":"discarded","schema":1}',
                now,
            ),
        )

        service = LocalOpsSurface(database, settings)
        health = await service.health()
        diagnostics = await service.diagnostics(session_id="session-1")
        logs = await service.log_tail(correlation_id="corr-1")
        events = await service.event_dump(session_id="session-1")

    assert health["database"] == "ok"
    assert health["pending_outbox"] == 0
    assert diagnostics["bindings"][0]["owner_fence_token"] == "[redacted]"
    assert diagnostics["capabilities"][0]["probe_detail"] == "{}"
    assert diagnostics["incidents"][0]["stderr_tail"] is None
    assert "cred-secret" not in logs["files"]["copilotd.log"]
    assert "secret-secret" not in logs["files"]["copilotd.log"]
    assert "hook-secret" not in logs["files"]["copilotd.log"]
    assert "phrase-secret" not in logs["files"]["copilotd.log"]
    assert "abc def ghi" not in logs["files"]["copilotd.log"]
    assert "session=abc" not in logs["files"]["copilotd.log"]
    assert "token=xyz" not in logs["files"]["copilotd.log"]
    assert "sid=abc" not in logs["files"]["copilotd.log"]
    assert "very-secret" not in logs["files"]["copilotd.log"]
    assert '"credentials":"[redacted]"' in logs["files"]["copilotd.log"]
    assert '"secret":"[redacted]"' in logs["files"]["copilotd.log"]
    assert "Authorization    :   [redacted]" in logs["files"]["copilotd.log"]
    assert "Cookie   = [redacted]" in logs["files"]["copilotd.log"]
    assert "Set-Cookie : [redacted]" in logs["files"]["copilotd.log"]
    assert f"corr-1 {_aws_secret()} = [redacted]" in logs["files"]["copilotd.log"]
    assert "credentials = [redacted]" in logs["files"]["copilotd.log"]
    assert "secret = [redacted]" in logs["files"]["copilotd.log"]
    assert "webhook_secret = [redacted]" in logs["files"]["copilotd.log"]
    assert "passphrase = [redacted]" in logs["files"]["copilotd.log"]
    assert "secret_count=7" in logs["files"]["copilotd.log"]
    assert "credential_type=basic" in logs["files"]["copilotd.log"]
    assert "has_secret=true" in logs["files"]["copilotd.log"]
    assert "nonsecret=keep-me" in logs["files"]["copilotd.log"]
    assert 'public_metadata="visible"' in logs["files"]["copilotd.log"]
    assert "safe text" in logs["files"]["boot.log"]
    assert events["events"][0]["parent_id"] == "parent-id"
    assert events["events"][0]["agent_id"] == "agent-id"
    assert events["events"][0]["message_id"] == "message-id"
    assert events["events"][0]["turn_id"] == "turn-id"
    assert events["events"][0]["raw_type"] == "message"
