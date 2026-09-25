from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from data_pipeline.core.schedule import OpeningHours

# Weekdays 10:45 to 17:30 in Buenos Aires (UTC-3): 13:45 to 20:30 UTC. 2026-09-25 is a Friday.
HOURS = OpeningHours(
    frozenset(range(5)), time(10, 45), time(17, 30), ZoneInfo("America/Argentina/Buenos_Aires")
)


def utc(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, day, hour, minute, tzinfo=UTC)


def test_open_includes_both_ends_in_the_zone() -> None:
    assert not HOURS.is_open(utc(25, 13, 44))
    assert HOURS.is_open(utc(25, 13, 45))
    assert HOURS.is_open(utc(25, 20, 30))
    assert not HOURS.is_open(utc(25, 20, 31))
    assert not HOURS.is_open(utc(26, 15))  # Saturday


def test_open_time_counts_only_opening_hours() -> None:
    # From Friday 17:00 to Monday 11:00 in Buenos Aires: 30 minutes on Friday, 15 on Monday.
    assert HOURS.open_time(utc(25, 20), utc(28, 14)) == timedelta(minutes=45)
    # Within one open stretch it is the plain difference.
    assert HOURS.open_time(utc(25, 15), utc(25, 16)) == timedelta(hours=1)
    # A whole weekday is 6 h 45 min.
    assert HOURS.open_time(utc(24, 0), utc(25, 0)) == timedelta(hours=6, minutes=45)


def test_open_time_of_an_empty_or_reversed_span_is_zero() -> None:
    assert HOURS.open_time(utc(25, 15), utc(25, 15)) == timedelta(0)
    assert HOURS.open_time(utc(25, 16), utc(25, 15)) == timedelta(0)


def test_hours_refuse_bad_days_and_times() -> None:
    zone = ZoneInfo("UTC")
    with pytest.raises(ValueError, match="weekdays"):
        OpeningHours(frozenset({7}), time(9), time(10), zone)
    with pytest.raises(ValueError, match="before"):
        OpeningHours(frozenset({0}), time(10), time(10), zone)
