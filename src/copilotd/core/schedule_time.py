from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

GapPolicy = Literal["skip", "shift_forward", "reject"]
FoldPolicy = Literal["both", "first", "second", "reject"]

_INTERVAL_PART = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhdw])", re.IGNORECASE)
_ISO_INTERVAL = re.compile(
    r"^P"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$",
    re.IGNORECASE,
)


class ScheduleExpressionKind(StrEnum):
    AT = "at"
    CRON = "cron"
    INTERVAL = "interval"


class ScheduleTimeError(ValueError):
    code = "CD-SCHEDULE-TIME-001"


class TimezoneUnavailable(ScheduleTimeError):
    code = "CD-SCHEDULE-TZ-001"


class NonexistentLocalTime(ScheduleTimeError):
    code = "CD-SCHEDULE-DST-GAP"


class AmbiguousLocalTime(ScheduleTimeError):
    code = "CD-SCHEDULE-DST-FOLD"


@dataclass(frozen=True, slots=True)
class ParsedSchedule:
    kind: ScheduleExpressionKind
    source: str
    normalized: str
    timezone: str
    zone: ZoneInfo
    anchor_utc: float
    at_utc: float | None = None
    interval_seconds: float | None = None
    gap_policy: GapPolicy = "skip"
    fold_policy: FoldPolicy = "both"

    def next_after(self, after_utc: float) -> float | None:
        if self.kind == ScheduleExpressionKind.AT:
            return self.at_utc if self.at_utc is not None and self.at_utc > after_utc else None
        if self.kind == ScheduleExpressionKind.INTERVAL:
            assert self.interval_seconds is not None
            elapsed = after_utc - self.anchor_utc
            multiplier = max(0, math.floor(elapsed / self.interval_seconds) + 1)
            return self.anchor_utc + multiplier * self.interval_seconds
        return _cron_next(
            self.normalized,
            self.zone,
            after_utc,
            gap_policy=self.gap_policy,
            fold_policy=self.fold_policy,
        )

    def latest_due(self, first_due_utc: float, through_utc: float) -> float | None:
        if first_due_utc > through_utc:
            return None
        if self.kind == ScheduleExpressionKind.AT:
            return (
                self.at_utc
                if self.at_utc is not None
                and first_due_utc <= self.at_utc <= through_utc
                else None
            )
        if self.kind == ScheduleExpressionKind.INTERVAL:
            assert self.interval_seconds is not None
            multiplier = math.floor((through_utc - self.anchor_utc) / self.interval_seconds)
            latest = self.anchor_utc + multiplier * self.interval_seconds
            return latest if latest >= first_due_utc else None
        latest = _cron_previous_or_at(
            self.normalized,
            self.zone,
            through_utc,
            gap_policy=self.gap_policy,
            fold_policy=self.fold_policy,
        )
        return latest if latest is not None and latest >= first_due_utc else None


def parse_schedule(
    expression: str,
    timezone: str,
    *,
    anchor_utc: float,
    gap_policy: GapPolicy | None = None,
    fold_policy: FoldPolicy | None = None,
) -> ParsedSchedule:
    source = expression.strip()
    prefix, separator, body = source.partition(":")
    if not separator or not body.strip():
        raise ScheduleTimeError("schedule must use at:, cron:, or interval:")
    try:
        kind = ScheduleExpressionKind(prefix.lower())
    except ValueError as error:
        raise ScheduleTimeError(f"unsupported schedule expression: {prefix}") from error
    zone = load_timezone(timezone)
    body = body.strip()

    if kind == ScheduleExpressionKind.AT:
        selected_gap = "reject" if gap_policy is None else gap_policy
        selected_fold = "reject" if fold_policy is None else fold_policy
        at_utc = _parse_at(
            body,
            zone,
            gap_policy=selected_gap,
            fold_policy=selected_fold,
        )
        return ParsedSchedule(
            kind=kind,
            source=source,
            normalized=_rfc3339(at_utc),
            timezone=zone.key,
            zone=zone,
            anchor_utc=anchor_utc,
            at_utc=at_utc,
            gap_policy=selected_gap,
            fold_policy=selected_fold,
        )

    if kind == ScheduleExpressionKind.CRON:
        fields = " ".join(body.split())
        if len(fields.split()) != 5 or not croniter.is_valid(fields):
            raise ScheduleTimeError("cron schedules require a valid five-field expression")
        selected_gap = "skip" if gap_policy is None else gap_policy
        selected_fold = "both" if fold_policy is None else fold_policy
        return ParsedSchedule(
            kind=kind,
            source=source,
            normalized=fields,
            timezone=zone.key,
            zone=zone,
            anchor_utc=anchor_utc,
            gap_policy=selected_gap,
            fold_policy=selected_fold,
        )

    seconds = _parse_interval(body)
    return ParsedSchedule(
        kind=kind,
        source=source,
        normalized=_format_interval(seconds),
        timezone=zone.key,
        zone=zone,
        anchor_utc=anchor_utc,
        interval_seconds=seconds,
        gap_policy="skip" if gap_policy is None else gap_policy,
        fold_policy="both" if fold_policy is None else fold_policy,
    )


def load_timezone(name: str) -> ZoneInfo:
    normalized = name.strip()
    if not normalized:
        raise TimezoneUnavailable("an IANA timezone is required")
    try:
        return ZoneInfo(normalized)
    except ZoneInfoNotFoundError as error:
        raise TimezoneUnavailable(
            f"IANA timezone data is unavailable for {normalized!r}; "
            "install the tzdata package on systems without an OS timezone database"
        ) from error


def planned_key(planned_at_utc: float) -> str:
    return _rfc3339(planned_at_utc)


def _parse_at(
    value: str,
    zone: ZoneInfo,
    *,
    gap_policy: GapPolicy,
    fold_policy: FoldPolicy,
) -> float:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ScheduleTimeError("at schedules require an RFC3339 timestamp") from error
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).timestamp()
    candidates = _resolve_wall_time(
        parsed,
        zone,
        gap_policy=gap_policy,
        fold_policy=fold_policy,
    )
    if not candidates:
        raise NonexistentLocalTime(f"{value!r} does not exist in timezone {zone.key}")
    if len(candidates) > 1:
        raise AmbiguousLocalTime(
            f"{value!r} occurs twice in timezone {zone.key}; include an explicit UTC offset"
        )
    return candidates[0]


def _parse_interval(value: str) -> float:
    compact = value.strip().replace(" ", "")
    iso_match = _ISO_INTERVAL.fullmatch(compact)
    if iso_match is not None:
        units = {
            "days": 86_400,
            "hours": 3_600,
            "minutes": 60,
            "seconds": 1,
        }
        seconds = sum(
            float(iso_match.group(name) or 0) * multiplier
            for name, multiplier in units.items()
        )
    else:
        position = 0
        seconds = 0.0
        multipliers = {"s": 1, "m": 60, "h": 3_600, "d": 86_400, "w": 604_800}
        for match in _INTERVAL_PART.finditer(compact):
            if match.start() != position:
                raise ScheduleTimeError(f"invalid interval duration: {value!r}")
            seconds += float(match.group("value")) * multipliers[match.group("unit").lower()]
            position = match.end()
        if position != len(compact):
            raise ScheduleTimeError(f"invalid interval duration: {value!r}")
    if not math.isfinite(seconds) or seconds <= 0:
        raise ScheduleTimeError("interval duration must be positive")
    if seconds < 1:
        raise ScheduleTimeError("interval duration must be at least one second")
    return seconds


def _format_interval(seconds: float) -> str:
    return f"{seconds:g}s"


def _cron_next(
    expression: str,
    zone: ZoneInfo,
    after_utc: float,
    *,
    gap_policy: GapPolicy,
    fold_policy: FoldPolicy,
) -> float:
    instant = math.floor(after_utc / 60) * 60.0 + 60
    for _ in range(5 * 366 * 24 * 60):
        local = datetime.fromtimestamp(instant, zone).replace(
            tzinfo=None,
            second=0,
            microsecond=0,
        )
        if croniter.match(expression, local):
            candidates = _resolve_wall_time(
                local,
                zone,
                gap_policy=gap_policy,
                fold_policy=fold_policy,
            )
            if instant in candidates:
                return instant
        instant += 60
    raise ScheduleTimeError("cron expression did not produce a future occurrence")


def _cron_previous_or_at(
    expression: str,
    zone: ZoneInfo,
    through_utc: float,
    *,
    gap_policy: GapPolicy,
    fold_policy: FoldPolicy,
) -> float | None:
    instant = math.floor(through_utc / 60) * 60.0
    for _ in range(5 * 366 * 24 * 60):
        local = datetime.fromtimestamp(instant, zone).replace(
            tzinfo=None,
            second=0,
            microsecond=0,
        )
        if croniter.match(expression, local):
            candidates = _resolve_wall_time(
                local,
                zone,
                gap_policy=gap_policy,
                fold_policy=fold_policy,
            )
            if instant in candidates:
                return instant
        instant -= 60
    return None


def _resolve_wall_time(
    wall: datetime,
    zone: ZoneInfo,
    *,
    gap_policy: GapPolicy,
    fold_policy: FoldPolicy,
) -> list[float]:
    if wall.tzinfo is not None:
        raise ValueError("wall time must be naive")
    candidates: list[float] = []
    for fold in (0, 1):
        aware = wall.replace(tzinfo=zone, fold=fold)
        utc = aware.astimezone(UTC)
        round_trip = utc.astimezone(zone)
        if round_trip.replace(tzinfo=None) == wall:
            timestamp = utc.timestamp()
            if timestamp not in candidates:
                candidates.append(timestamp)
    candidates.sort()
    if candidates:
        if len(candidates) == 1:
            return candidates
        if fold_policy == "both":
            return candidates
        if fold_policy == "first":
            return candidates[:1]
        if fold_policy == "second":
            return candidates[1:]
        if fold_policy == "reject":
            return candidates
        raise ScheduleTimeError(f"unsupported DST fold policy: {fold_policy}")
    if gap_policy == "skip":
        return []
    if gap_policy == "reject":
        return []
    if gap_policy != "shift_forward":
        raise ScheduleTimeError(f"unsupported DST gap policy: {gap_policy}")
    probe = wall
    for _ in range(180):
        probe += timedelta(minutes=1)
        shifted = _resolve_wall_time(
            probe,
            zone,
            gap_policy="skip",
            fold_policy=fold_policy,
        )
        if shifted:
            return shifted
    raise NonexistentLocalTime(f"could not shift nonexistent local time {wall!s} forward")


def _rfc3339(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
