from __future__ import annotations

from datetime import date

import pytest
from fastapi import HTTPException

from data_pipeline.api.schemas import history_range

TODAY = date(2026, 9, 25)


def test_the_default_is_the_last_30_days() -> None:
    days = history_range(None, None, TODAY)
    assert (days.first, days.last) == (date(2026, 8, 27), TODAY)


def test_a_given_end_moves_the_default_start() -> None:
    days = history_range(None, date(2026, 1, 31), TODAY)
    assert (days.first, days.last) == (date(2026, 1, 2), date(2026, 1, 31))


def test_400_days_is_the_limit() -> None:
    assert history_range(date(2025, 8, 22), TODAY, TODAY).first == date(2025, 8, 22)
    with pytest.raises(HTTPException) as error:
        history_range(date(2025, 8, 21), TODAY, TODAY)
    assert error.value.status_code == 422
    assert error.value.detail == "range_too_long"


@pytest.mark.parametrize(
    ("first", "last"),
    [
        (date(9999, 12, 1), date(9999, 12, 31)),
        (None, date(1, 1, 5)),
        (date(1999, 12, 31), None),
        (None, date(2026, 9, 27)),
    ],
)
def test_dates_outside_the_history_are_refused(first: date | None, last: date | None) -> None:
    with pytest.raises(HTTPException) as error:
        history_range(first, last, TODAY)
    assert error.value.detail == "date_out_of_range"


def test_the_first_day_and_tomorrow_are_inside_the_history() -> None:
    # Up to tomorrow is fine: it may already be tomorrow somewhere.
    assert history_range(date(2000, 1, 1), date(2000, 1, 1), TODAY).first == date(2000, 1, 1)
    assert history_range(None, date(2026, 9, 26), TODAY).last == date(2026, 9, 26)
    # The default start never goes before the first day.
    assert history_range(None, date(2000, 1, 5), TODAY).first == date(2000, 1, 1)


def test_from_after_to_is_refused() -> None:
    with pytest.raises(HTTPException) as error:
        history_range(date(2026, 9, 26), TODAY, TODAY)
    assert error.value.detail == "from_after_to"
    assert history_range(TODAY, TODAY, TODAY).first == TODAY
