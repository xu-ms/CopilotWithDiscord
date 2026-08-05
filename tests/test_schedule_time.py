from datetime import UTC, datetime

import pytest

from copilotd.core import schedule_time
from copilotd.core.schedule_time import (
    AmbiguousLocalTime,
    NonexistentLocalTime,
    TimezoneUnavailable,
    parse_schedule,
)


def _timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).timestamp()


def test_cron_skips_spring_gap_and_emits_both_fall_fold_instants() -> None:
    spring = parse_schedule(
        "cron:30 2 * * *",
        "America/New_York",
        anchor_utc=_timestamp("2026-03-07T00:00:00Z"),
    )
    assert spring.next_after(_timestamp("2026-03-07T07:31:00Z")) == _timestamp(
        "2026-03-09T06:30:00Z"
    )

    fall = parse_schedule(
        "cron:30 1 * * *",
        "America/New_York",
        anchor_utc=_timestamp("2026-10-31T00:00:00Z"),
    )
    first = fall.next_after(_timestamp("2026-11-01T05:00:00Z"))
    second = fall.next_after(first)
    assert first == _timestamp("2026-11-01T05:30:00Z")
    assert second == _timestamp("2026-11-01T06:30:00Z")
    assert fall.latest_due(first, _timestamp("2026-11-01T06:00:00Z")) == first
    every_minute = parse_schedule(
        "cron:* * * * *",
        "America/New_York",
        anchor_utc=_timestamp("2026-10-31T00:00:00Z"),
    )
    assert every_minute.next_after(_timestamp("2026-11-01T05:59:00Z")) == _timestamp(
        "2026-11-01T06:00:00Z"
    )


def test_at_requires_explicit_policy_for_gap_and_offset_for_fold() -> None:
    anchor = _timestamp("2026-01-01T00:00:00Z")
    with pytest.raises(NonexistentLocalTime):
        parse_schedule(
            "at:2026-03-08T02:30:00",
            "America/New_York",
            anchor_utc=anchor,
        )
    with pytest.raises(AmbiguousLocalTime):
        parse_schedule(
            "at:2026-11-01T01:30:00",
            "America/New_York",
            anchor_utc=anchor,
        )
    explicit = parse_schedule(
        "at:2026-11-01T01:30:00-05:00",
        "America/New_York",
        anchor_utc=anchor,
    )
    assert explicit.at_utc == _timestamp("2026-11-01T06:30:00Z")


@pytest.mark.parametrize(
    ("source", "seconds"),
    [
        ("interval:90s", 90.0),
        ("interval:1h30m", 5_400.0),
        ("interval:PT2H", 7_200.0),
        ("interval:P1DT30M", 88_200.0),
    ],
)
def test_interval_parsing_and_next_run_are_deterministic(
    source: str,
    seconds: float,
) -> None:
    parsed = parse_schedule(source, "UTC", anchor_utc=1_000.0)
    assert parsed.interval_seconds == seconds
    previous = 1_000.0
    seen: set[float] = set()
    for index in range(1, 101):
        following = parsed.next_after(previous)
        assert following == 1_000.0 + index * seconds
        assert following not in seen
        seen.add(following)
        previous = following
    assert parsed.latest_due(1_000.0 + seconds, 1_000.0 + 99.5 * seconds) == (
        1_000.0 + 99 * seconds
    )


def test_timezone_error_explains_windows_tzdata_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_name: str) -> None:
        raise schedule_time.ZoneInfoNotFoundError("Missing/Zone")

    monkeypatch.setattr(schedule_time, "ZoneInfo", missing)
    with pytest.raises(TimezoneUnavailable, match="install the tzdata package"):
        schedule_time.load_timezone("Missing/Zone")
