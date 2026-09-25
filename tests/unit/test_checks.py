from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from data_pipeline.core.checks import (
    Range,
    Rules,
    canonical,
    decide,
    reading_problem,
    value_problem,
)
from data_pipeline.core.readings import Accepted, Reading, Rejection, Suspect

NOW = datetime(2026, 9, 25, 18, 0, tzinfo=UTC)
RULES = Rules(
    plausible=Range(Decimal(500), Decimal(50_000)),
    max_age=timedelta(minutes=30),
    max_jump=Decimal("0.05"),
    confirm_within=Decimal("0.005"),
)


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
        problem = reading_problem(reading(value), RULES, NOW)
        assert problem is not None
        assert problem[0] is Rejection.IMPLAUSIBLE
    for value in ["500", "50000"]:
        assert reading_problem(reading(value), RULES, NOW) is None


def test_reading_problem_rejects_old_readings() -> None:
    assert reading_problem(reading("1600", NOW - timedelta(minutes=30)), RULES, NOW) is None
    problem = reading_problem(reading("1600", NOW - timedelta(minutes=31)), RULES, NOW)
    assert problem is not None
    assert problem[0] is Rejection.STALE


def test_reading_problem_rejects_readings_from_the_future() -> None:
    assert reading_problem(reading("1600", NOW + timedelta(minutes=5)), RULES, NOW) is None
    problem = reading_problem(reading("1600", NOW + timedelta(minutes=6)), RULES, NOW)
    assert problem is not None
    assert problem[0] is Rejection.FROM_THE_FUTURE


def test_reading_requires_an_aware_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Reading(Decimal(1), datetime(2026, 9, 25))  # noqa: DTZ001


def test_rules_refuse_a_confirmation_band_wider_than_the_jump() -> None:
    with pytest.raises(ValueError, match="confirm_within"):
        Rules(RULES.plausible, RULES.max_age, Decimal("0.01"), Decimal("0.02"))


class TestDecide:
    # Last accepted 1600, max jump 5 % (80), confirmation band 0.5 % of the first suspect.

    def test_the_first_reading_of_a_series_is_accepted(self) -> None:
        assert decide(reading("1600"), None, [], RULES) == Accepted(reading("1600"))

    def test_a_move_up_to_the_max_jump_is_accepted(self) -> None:
        assert decide(reading("1680"), Decimal(1600), [], RULES) == Accepted(reading("1680"))
        assert decide(reading("1520"), Decimal(1600), [], RULES) == Accepted(reading("1520"))

    def test_a_bigger_move_is_a_suspect(self) -> None:
        assert decide(reading("1680.01"), Decimal(1600), [], RULES) == Suspect(reading("1680.01"))
        assert decide(reading("1519.99"), Decimal(1600), [], RULES) == Suspect(reading("1519.99"))

    def test_one_consistent_follower_is_not_enough(self) -> None:
        outcome = decide(reading("1800"), Decimal(1600), [Decimal(1800)], RULES)
        assert outcome == Suspect(reading("1800"))

    def test_the_second_consistent_follower_confirms(self) -> None:
        # Band around the first suspect: 1800 * 0.005 = 9, so 1791 to 1809.
        suspects = [Decimal(1800), Decimal(1809)]
        outcome = decide(reading("1791"), Decimal(1600), suspects, RULES)
        assert outcome == Accepted(reading("1791"), confirmed=True)

    def test_the_band_is_closed_on_both_sides_of_the_first_suspect(self) -> None:
        for value in ["1790.99", "1809.01"]:
            outcome = decide(reading(value), Decimal(1600), [Decimal(1800), Decimal(1800)], RULES)
            assert outcome == Suspect(reading(value)), value

    def test_a_follower_outside_the_band_does_not_count(self) -> None:
        suspects = [Decimal(1800), Decimal("1809.01")]
        outcome = decide(reading("1800"), Decimal(1600), suspects, RULES)
        assert outcome == Suspect(reading("1800"))

    def test_an_inconsistent_suspect_starts_a_new_run(self) -> None:
        # 1900 starts a new run; 1901 is its first follower, so 1902 confirms it.
        suspects = [Decimal(1800), Decimal(1900), Decimal(1901)]
        outcome = decide(reading("1902"), Decimal(1600), suspects, RULES)
        assert outcome == Accepted(reading("1902"), confirmed=True)

    def test_a_reading_cannot_rejoin_an_abandoned_run(self) -> None:
        outcome = decide(reading("1801"), Decimal(1600), [Decimal(1800), Decimal(1900)], RULES)
        assert outcome == Suspect(reading("1801"))

    def test_followers_do_not_carry_over_a_broken_run(self) -> None:
        suspects = [Decimal(1800), Decimal(1801), Decimal(1900)]
        outcome = decide(reading("1802"), Decimal(1600), suspects, RULES)
        assert outcome == Suspect(reading("1802"))

    def test_a_reading_back_near_the_last_accepted_is_accepted(self) -> None:
        outcome = decide(reading("1610"), Decimal(1600), [Decimal(1800)], RULES)
        assert outcome == Accepted(reading("1610"))
