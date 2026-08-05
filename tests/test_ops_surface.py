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


def test_redact_text_consumes_sensitive_header_keyvalue_and_ignores_benign_names() -> None:
    text = (
        "safe keep "
        '{"token":"json-secret-123","aws_secret_access_key":"aws-secret-1"} '
        + "Authorization    :   Basic abc def ghi"
        + NL
        + "authorization =   Bearer def 456"
        + NL
        + f"{_cookie()}   : session=abc; token=xyz; Path=/; HttpOnly"
        + NL
        + f"{_set_cookie()} : sid=abc; Path=/; Secure"
        + NL
        + f"{_aws_secret()} = very-secret"
        + NL
        + 'access_token="access-secret" '
        + "client_secret='client-secret' "
        + "private_key=private-secret "
        + 'client_access_token = "client-access-secret" '
        + 'client_private_token = "client-private-token" '
        + 'webhook_secret = "webhook-secret" '
        + 'credentials = "cred-secret" '
        + "passphrase=pass-secret "
        + "secret=direct-secret "
        + "secret_count=7 "
        + "credential_type=basic "
        + "has_secret=true "
        + "nonsecret=keep-me "
        + 'public_metadata="visible"'
    )
    redacted = _redact_text(text)

    for secret in (
        "json-secret-123",
        "aws-secret-1",
        "abc def ghi",
        "def 456",
        "session=abc",
        "token=xyz",
        "sid=abc",
        "very-secret",
        "access-secret",
        "client-secret",
        "private-secret",
        "client-access-secret",
        "client-private-token",
        "webhook-secret",
        "cred-secret",
        "pass-secret",
        "direct-secret",
    ):
        assert secret not in redacted

    assert "safe keep" in redacted
    assert '"token":"[redacted]"' in redacted
    assert '"aws_secret_access_key":"[redacted]"' in redacted
    assert "Authorization    :   [redacted]" in redacted
    assert "authorization =   [redacted]" in redacted
    assert f"{_cookie()}   : [redacted]" in redacted
    assert f"{_set_cookie()} : [redacted]" in redacted
    assert f"{_aws_secret()} = [redacted]" in redacted
    assert 'access_token="[redacted]"' in redacted
    assert "client_secret='[redacted]'" in redacted
    assert "private_key=[redacted]" in redacted
    assert 'client_access_token = "[redacted]"' in redacted
    assert 'client_private_token = "[redacted]"' in redacted
    assert 'webhook_secret = "[redacted]"' in redacted
    assert 'credentials = "[redacted]"' in redacted
    assert "passphrase=[redacted]" in redacted
    assert "secret=[redacted]" in redacted
    assert "secret_count=7" in redacted
    assert "credential_type=basic" in redacted
    assert "has_secret=true" in redacted
    assert "nonsecret=keep-me" in redacted
    assert 'public_metadata="visible"' in redacted


def test_redact_structure_recurses_nested_sensitive_keys_and_preserves_benign_keys() -> None:
    payload = {
        "authorization": {
            "token": "abc",
            "aws_secret_access_key": "aws-secret-2",
            "webhook_secret": "webhook-2",
            "credentials": "cred-2",
            "passphrase": "phrase-2",
            "nested": [
                {"cookie": "crumb"},
                {"client_access_token": "client-access"},
                {"note": "visible"},
                {"secret_count": 4},
                {"credential_type": "basic"},
                {"has_secret": True},
                {"nonsecret": "keep"},
                {"public_metadata": "show"},
            ],
            "plain": "keep me",
        },
        "meta": {
            "note": "Authorization: Basic structured-secret",
            "list": [
                {"password": "pw-2"},
                {"private_key": "priv"},
                {"client_secret": "client"},
            ],
        },
        "aws_secret_access_key": "top-secret",
        "client_key": "client-key-secret",
        "client_id": "id-1",
        "secret_count": 9,
        "credential_type": "oauth",
        "has_secret": False,
        "nonsecret": "keep",
        "public_metadata": "visible",
    }

    redacted = _redact_structure(payload)

    assert redacted["authorization"]["token"] == "[redacted]"
    assert redacted["authorization"]["aws_secret_access_key"] == "[redacted]"
    assert redacted["authorization"]["webhook_secret"] == "[redacted]"
    assert redacted["authorization"]["credentials"] == "[redacted]"
    assert redacted["authorization"]["passphrase"] == "[redacted]"
    assert redacted["authorization"]["nested"][0]["cookie"] == "[redacted]"
    assert redacted["authorization"]["nested"][1]["client_access_token"] == "[redacted]"
    assert redacted["authorization"]["nested"][2]["note"] == "visible"
    assert redacted["authorization"]["nested"][3]["secret_count"] == 4
    assert redacted["authorization"]["nested"][4]["credential_type"] == "basic"
    assert redacted["authorization"]["nested"][5]["has_secret"] is True
    assert redacted["authorization"]["nested"][6]["nonsecret"] == "keep"
    assert redacted["authorization"]["nested"][7]["public_metadata"] == "show"
    assert redacted["authorization"]["plain"] == "keep me"
    assert redacted["meta"]["note"] == "Authorization: [redacted]"
    assert redacted["meta"]["list"][0]["password"] == "[redacted]"
    assert redacted["meta"]["list"][1]["private_key"] == "[redacted]"
    assert redacted["meta"]["list"][2]["client_secret"] == "[redacted]"
    assert redacted["aws_secret_access_key"] == "[redacted]"
    assert redacted["client_key"] == "[redacted]"
    assert redacted["client_id"] == "id-1"
    assert redacted["secret_count"] == 9
    assert redacted["credential_type"] == "oauth"
    assert redacted["has_secret"] is False
    assert redacted["nonsecret"] == "keep"
    assert redacted["public_metadata"] == "visible"


@pytest.mark.asyncio
async def test_ops_surface_redacts_diagnostics_log_tail_and_event_dump(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "boot.log").write_text("boot corr-1 safe text" + NL, encoding="utf-8")
    copilotd_log = (
        'corr-1 {"token":"json-secret-123","aws_secret_access_key":"aws-secret-1"}'
        + NL
        + "corr-1 Authorization    :   Basic abc def ghi"
        + NL
        + "corr-1 Cookie   = session=abc; token=xyz; Path=/; HttpOnly"
        + NL
        + "corr-1 Set-Cookie : sid=abc; Path=/; Secure"
        + NL
        + f"corr-1 {_aws_secret()} = very-secret"
        + NL
        + 'corr-1 client_access_token = "client-access-secret"'
        + NL
        + 'corr-1 private_key = "private-secret"'
        + NL
        + 'corr-1 webhook_secret = "webhook-secret"'
        + NL
        + 'corr-1 credentials = "cred-secret"'
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
                runtime_version, sdk_version, capability, supported, probe_detail, probed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "1.0",
                "1.0",
                "demo",
                1,
                '{"aws_secret_access_key":"cap-secret","client_private_token":"cap-private","secret_count":3,"credential_type":"basic","has_secret":false,"nonsecret":"ok","public_metadata":"yes"}',
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
                "Authorization = Digest incident-secret",
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
                "Authorization    : Bearer parent-secret",
                'client_secret="agent-secret"',
                f"{_cookie()} = message-secret",
                f"{_aws_secret()} = turn-secret",
                "hash",
                '{"client_access_token":"payload-secret","secret_count":1,"credential_type":"kind","has_secret":false,"nonsecret":"meta","public_metadata":"pub"}',
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
    assert diagnostics["capabilities"][0]["probe_detail"] == (
        '{"aws_secret_access_key":"[redacted]","client_private_token":"[redacted]","secret_count":3,"credential_type":"basic","has_secret":false,"nonsecret":"ok","public_metadata":"yes"}'
    )
    assert diagnostics["incidents"][0]["stderr_tail"] == "Authorization = [redacted]"
    assert "incident-secret" not in diagnostics["incidents"][0]["stderr_tail"]
    assert "json-secret-123" not in logs["files"]["copilotd.log"]
    assert "aws-secret-1" not in logs["files"]["copilotd.log"]
    assert "abc def ghi" not in logs["files"]["copilotd.log"]
    assert "session=abc" not in logs["files"]["copilotd.log"]
    assert "token=xyz" not in logs["files"]["copilotd.log"]
    assert "sid=abc" not in logs["files"]["copilotd.log"]
    assert "very-secret" not in logs["files"]["copilotd.log"]
    assert "client-access-secret" not in logs["files"]["copilotd.log"]
    assert "private-secret" not in logs["files"]["copilotd.log"]
    assert "webhook-secret" not in logs["files"]["copilotd.log"]
    assert "cred-secret" not in logs["files"]["copilotd.log"]
    assert '"token":"[redacted]"' in logs["files"]["copilotd.log"]
    assert '"aws_secret_access_key":"[redacted]"' in logs["files"]["copilotd.log"]
    assert "Authorization    :   [redacted]" in logs["files"]["copilotd.log"]
    assert "Cookie   = [redacted]" in logs["files"]["copilotd.log"]
    assert "Set-Cookie : [redacted]" in logs["files"]["copilotd.log"]
    assert f"corr-1 {_aws_secret()} = [redacted]" in logs["files"]["copilotd.log"]
    assert 'client_access_token = "[redacted]"' in logs["files"]["copilotd.log"]
    assert 'private_key = "[redacted]"' in logs["files"]["copilotd.log"]
    assert 'webhook_secret = "[redacted]"' in logs["files"]["copilotd.log"]
    assert 'credentials = "[redacted]"' in logs["files"]["copilotd.log"]
    assert "secret_count=7" in logs["files"]["copilotd.log"]
    assert "credential_type=basic" in logs["files"]["copilotd.log"]
    assert "has_secret=true" in logs["files"]["copilotd.log"]
    assert "nonsecret=keep-me" in logs["files"]["copilotd.log"]
    assert 'public_metadata="visible"' in logs["files"]["copilotd.log"]
    assert "safe text" in logs["files"]["boot.log"]
    assert events["events"][0]["parent_id"] == "Authorization    : [redacted]"
    assert events["events"][0]["agent_id"] == 'client_secret="[redacted]"'
    assert events["events"][0]["message_id"] == "Cookie = [redacted]"
    assert events["events"][0]["turn_id"] == f"{_aws_secret()} = [redacted]"
    assert events["events"][0]["raw_type"] == "message"
