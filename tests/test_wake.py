from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from copilotd.ops.wake import (
    macos_last_resume_timestamp,
    windows_last_resume_timestamp,
)


def test_macos_resume_provider_uses_latest_darkwake_or_wake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = "\n".join(
        [
            "2026-08-05 10:00:00 +0000 DarkWake DarkWake from Normal Sleep",
            "2026-08-05 11:00:00 +0000 Wake Wake from Normal Sleep",
            "2026-08-05 12:00:00 +0000 Wake Requests [process=backupd]",
            "2026-08-05 13:00:00 +0000 Video Wake Lock active",
        ]
    )
    monkeypatch.setattr(
        "copilotd.ops.wake.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=output),
    )

    assert (
        macos_last_resume_timestamp()
        == datetime(
            2026,
            8,
            5,
            11,
            tzinfo=UTC,
        ).timestamp()
    )


def test_windows_resume_provider_parses_utc_event_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "copilotd.ops.wake.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="2026-08-05T11:00:00.0000000Z\n",
        ),
    )

    assert (
        windows_last_resume_timestamp()
        == datetime(
            2026,
            8,
            5,
            11,
            tzinfo=UTC,
        ).timestamp()
    )
