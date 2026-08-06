from datetime import UTC, datetime

import pytest

from copilotd.core import schedule_time
from copilotd.core.schedule_time import (
    AmbiguousLocalTime,
    NonexistentLocalTime,
    ScheduleTimeError,
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
    fold_hour = parse_schedule(
        "cron:* 1 * * *",
        "America/New_York",
        anchor_utc=_timestamp("2026-10-31T00:00:00Z"),
    )
    assert fold_hour.next_after(_timestamp("2026-11-01T05:00:00Z")) == _timestamp(
        "2026-11-01T05:01:00Z"
    )
    assert fold_hour.latest_due(
        _timestamp("2026-11-01T05:01:00Z"),
        _timestamp("2026-11-01T06:00:00Z"),
    ) == _timestamp("2026-11-01T06:00:00Z")


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


def test_cron_applies_gap_and_fold_policies_explicitly() -> None:
    anchor = _timestamp("2026-01-01T00:00:00Z")
    shifted = parse_schedule(
        "cron:30 2 * * *",
        "America/New_York",
        anchor_utc=anchor,
        gap_policy="shift_forward",
    )
    assert shifted.next_after(_timestamp("2026-03-08T06:00:00Z")) == _timestamp(
        "2026-03-08T07:00:00Z"
    )
    rejected_gap = parse_schedule(
        "cron:30 2 * * *",
        "America/New_York",
        anchor_utc=anchor,
        gap_policy="reject",
    )
    with pytest.raises(NonexistentLocalTime):
        rejected_gap.next_after(_timestamp("2026-03-08T06:00:00Z"))

    first = parse_schedule(
        "cron:30 1 * * *",
        "America/New_York",
        anchor_utc=anchor,
        fold_policy="first",
    )
    second = parse_schedule(
        "cron:30 1 * * *",
        "America/New_York",
        anchor_utc=anchor,
        fold_policy="second",
    )
    rejected_fold = parse_schedule(
        "cron:30 1 * * *",
        "America/New_York",
        anchor_utc=anchor,
        fold_policy="reject",
    )
    before_fold = _timestamp("2026-11-01T05:00:00Z")
    assert first.next_after(before_fold) == _timestamp("2026-11-01T05:30:00Z")
    assert second.next_after(before_fold) == _timestamp("2026-11-01T06:30:00Z")
    with pytest.raises(AmbiguousLocalTime):
        rejected_fold.next_after(before_fold)
    assert rejected_fold.next_after(_timestamp("2026-11-01T07:00:00Z")) == _timestamp(
        "2026-11-02T06:30:00Z"
    )
    assert rejected_fold.latest_due(
        _timestamp("2026-10-31T05:30:00Z"),
        _timestamp("2026-11-01T04:00:00Z"),
    ) == _timestamp("2026-10-31T05:30:00Z")


def test_sparse_cron_jumps_directly_to_local_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_croniter = schedule_time.croniter
    calls = 0
    candidates = 0

    class CountingCroniter:
        is_valid = staticmethod(real_croniter.is_valid)

        def __init__(self, *args: object, **kwargs: object) -> None:
            nonlocal calls
            calls += 1
            self.inner = real_croniter(*args, **kwargs)

        def get_next(self, result_type: type[datetime]) -> datetime:
            nonlocal candidates
            candidates += 1
            return self.inner.get_next(result_type)

        def get_prev(self, result_type: type[datetime]) -> datetime:
            nonlocal candidates
            candidates += 1
            return self.inner.get_prev(result_type)

    monkeypatch.setattr(schedule_time, "croniter", CountingCroniter)
    parsed = parse_schedule(
        "cron:0 0 1 1 *",
        "UTC",
        anchor_utc=_timestamp("2026-01-02T00:00:00Z"),
    )

    assert parsed.next_after(_timestamp("2026-01-02T00:00:00Z")) == _timestamp(
        "2027-01-01T00:00:00Z"
    )
    assert calls <= 2
    before_dense = calls
    before_candidates = candidates
    dense = parse_schedule(
        "cron:* * * * *",
        "UTC",
        anchor_utc=_timestamp("2026-01-02T00:00:00Z"),
    )
    assert dense.next_after(_timestamp("2026-01-02T00:00:00Z")) == _timestamp(
        "2026-01-02T00:01:00Z"
    )
    assert calls - before_dense == 1
    assert candidates - before_candidates == 1

    before_spring = candidates
    spring_dense = parse_schedule(
        "cron:* * * * *",
        "America/New_York",
        anchor_utc=_timestamp("2026-03-08T07:00:00Z"),
    )
    assert spring_dense.next_after(_timestamp("2026-03-08T07:00:00Z")) == _timestamp(
        "2026-03-08T07:01:00Z"
    )
    assert candidates - before_spring == 1


def test_shift_forward_handles_date_line_sized_gap() -> None:
    parsed = parse_schedule(
        "cron:0 12 * * *",
        "Pacific/Apia",
        anchor_utc=_timestamp("2011-12-28T00:00:00Z"),
        gap_policy="shift_forward",
    )

    assert parsed.next_after(_timestamp("2011-12-29T23:00:00Z")) == _timestamp(
        "2011-12-30T10:00:00Z"
    )


@pytest.mark.parametrize(
    ("expression", "timezone", "gap_policy"),
    [
        ("cron:0 0 31 2 *", "UTC", "skip"),
        ("cron:30 2 * 3 sun#2", "America/New_York", "skip"),
    ],
)
def test_unresolvable_cron_exhaustion_is_domain_error(
    expression: str,
    timezone: str,
    gap_policy: str,
) -> None:
    parsed = parse_schedule(
        expression,
        timezone,
        anchor_utc=_timestamp("2026-01-01T00:00:00Z"),
        gap_policy=gap_policy,
    )

    with pytest.raises(ScheduleTimeError, match="resolvable future"):
        parsed.next_after(_timestamp("2026-01-01T00:00:00Z"))


def test_previous_search_uses_calendar_horizon_not_candidate_cap() -> None:
    parsed = parse_schedule(
        "cron:* 1,2 * 3 L0",
        "Antarctica/Troll",
        anchor_utc=_timestamp("2000-01-01T00:00:00Z"),
    )

    latest = parsed.latest_due(
        _timestamp("2000-01-01T00:00:00Z"),
        _timestamp("2026-04-01T00:00:00Z"),
    )

    assert latest is not None
    assert datetime.fromtimestamp(latest, UTC).year == 2004


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


def test_fractional_interval_exact_boundary_strictly_advances() -> None:
    anchor = 1_000.123456
    parsed = parse_schedule("interval:1.3s", "UTC", anchor_utc=anchor)
    third = anchor + 3 * 1.3

    assert parsed.latest_due(anchor + 1.3, third) == third
    assert parsed.next_after(third) == anchor + 4 * 1.3
    assert parsed.next_after(third) > third


def test_cron_latest_due_includes_current_floored_minute() -> None:
    parsed = parse_schedule("cron:* * * * *", "UTC", anchor_utc=0)

    assert parsed.latest_due(60, 90) == 60
    assert parsed.latest_due(60, 60) == 60


def test_timezone_error_explains_windows_tzdata_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_name: str) -> None:
        raise schedule_time.ZoneInfoNotFoundError("Missing/Zone")

    monkeypatch.setattr(schedule_time, "ZoneInfo", missing)
    with pytest.raises(TimezoneUnavailable, match="install the tzdata package"):
        schedule_time.load_timezone("Missing/Zone")
