from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from data_pipeline.core.checks import (
    Range,
    Rules,
    canonical,
    reading_problem,
    value_problem,
)
from data_pipeline.core.readings import Reading, Rejection

NOW = datetime(2026, 9, 25, 18, 0, tzinfo=UTC)
RULES = Rules(
    plausible=Range(Decimal(500), Decimal(50_000)),
    max_age=timedelta(minutes=30),
)
AGE = timedelta(0)


def reading(value: str, as_of: datetime = NOW) -> Reading:
    return Reading(Decimal(value), as_of)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1615.300000000000", "1615.3"),
        ("1E+3", "1000"),
        ("1000.000", "1000"),
        ("0.00000001", "0.00000001"),
        ("1613.34504", "1613.34504"),
        ("0.000", "0"),
        # More digits than Decimal's default precision (28): nothing may be rounded.
        ("999999.99999999999999999999999", "999999.99999999999999999999999"),
        ("1615.30000000000000000000000000000000010", "1615.3000000000000000000000000000000001"),
    ],
)
def test_canonical_drops_trailing_zeros_in_plain_notation(value: str, expected: str) -> None:
    assert format(canonical(Decimal(value)), "f") == expected


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_canonical_refuses_what_is_not_a_number(value: str) -> None:
    with pytest.raises(ValueError, match="not a finite number"):
        canonical(Decimal(value))


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("NaN", Rejection.NOT_FINITE),
        ("Infinity", Rejection.NOT_FINITE),
        ("0", Rejection.NOT_POSITIVE),
        ("-1", Rejection.NOT_POSITIVE),
        ("1000000.01", Rejection.TOO_LARGE),
        ("1.123456789", Rejection.TOO_MANY_DECIMALS),
        ("1615.3000000000000000000000000000000001", Rejection.TOO_MANY_DECIMALS),
        ("999999.99999999999999999999999", Rejection.TOO_MANY_DECIMALS),
    ],
)
def test_value_problem_rejects_what_the_calculator_would(value: str, reason: Rejection) -> None:
    problem = value_problem(Decimal(value))
    assert problem is not None
    assert problem[0] is reason


@pytest.mark.parametrize("value", ["1000000", "0.00000001", "1615.300000000000"])
def test_value_problem_accepts_the_bounds_and_trailing_zeros(value: str) -> None:
    assert value_problem(Decimal(value)) is None


def test_reading_problem_rejects_values_outside_the_plausible_range() -> None:
    for value in ["499.99", "50000.01"]:
        problem = reading_problem(reading(value), RULES, NOW, AGE)
        assert problem is not None
        assert problem[0] is Rejection.IMPLAUSIBLE
    for value in ["500", "50000"]:
        assert reading_problem(reading(value), RULES, NOW, AGE) is None


def test_reading_problem_rejects_readings_older_than_max_age() -> None:
    # The age is given: the series counts it, with or without opening hours.
    old = reading("1600", NOW - timedelta(days=3))
    assert reading_problem(old, RULES, NOW, timedelta(minutes=30)) is None
    problem = reading_problem(old, RULES, NOW, timedelta(minutes=31))
    assert problem is not None
    assert problem[0] is Rejection.STALE


def test_reading_problem_rejects_readings_from_the_future() -> None:
    soon = reading("1600", NOW + timedelta(minutes=5))
    assert reading_problem(soon, RULES, NOW, AGE) is None
    problem = reading_problem(reading("1600", NOW + timedelta(minutes=6)), RULES, NOW, AGE)
    assert problem is not None
    assert problem[0] is Rejection.FROM_THE_FUTURE


def test_reading_requires_an_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Reading(Decimal(1), datetime(2026, 9, 25))  # noqa: DTZ001


def test_rules_refuse_a_max_age_that_is_not_positive() -> None:
    with pytest.raises(ValueError, match="max_age"):
        Rules(RULES.plausible, timedelta(0))
