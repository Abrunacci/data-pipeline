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
    control_within=Decimal("0.015"),
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


def test_rules_refuse_fractions_outside_zero_to_one() -> None:
    with pytest.raises(ValueError, match="max_jump"):
        Rules(RULES.plausible, RULES.max_age, Decimal(1))
    with pytest.raises(ValueError, match="control_within"):
        Rules(RULES.plausible, RULES.max_age, Decimal("0.05"), Decimal(0))


def decide_(value: str, suspects: list[int | str] = [], control: str | None = None) -> object:  # noqa: B006
    """``decide`` with the last accepted value at 1600 and a 5 % jump (1520 to 1680)."""
    return decide(
        reading(value),
        Decimal(1600),
        [Decimal(s) for s in suspects],
        None if control is None else Decimal(control),
        RULES,
    )


class TestDecide:
    def test_the_first_reading_of_a_series_is_accepted(self) -> None:
        assert decide(reading("1600"), None, [], None, RULES) == Accepted(reading("1600"))
        # Even if the control disagrees: there is nothing to hold it against.
        assert decide(reading("1600"), None, [], Decimal(1800), RULES) == Accepted(reading("1600"))

    def test_a_move_up_to_the_max_jump_is_accepted(self) -> None:
        assert decide_("1680") == Accepted(reading("1680"))
        assert decide_("1520") == Accepted(reading("1520"))

    def test_a_bigger_move_is_a_suspect(self) -> None:
        for value in ["1680.01", "1519.99"]:
            outcome = decide_(value)
            assert isinstance(outcome, Suspect), value
            assert outcome.detail == "jumped from 1600"


class TestJumps:
    """A jump confirms itself when the 2 suspects before it jumped the same way."""

    def test_two_suspects_the_same_way_confirm_the_third(self) -> None:
        outcome = decide_("1800", [1700, 1750])
        assert outcome == Accepted(
            reading("1800"),
            confirmed=True,
            detail="the market kept moving the same way for 3 readings",
        )

    def test_a_steady_trend_confirms_even_if_every_reading_moves(self) -> None:
        # Each reading 3 % above the one before: no two are close, the market is moving.
        assert isinstance(decide_("1854", [1700, 1751, 1803]), Accepted)

    def test_a_fall_confirms_like_a_rise(self) -> None:
        assert isinstance(decide_("1300", [1400, 1350]), Accepted)

    def test_one_suspect_before_is_not_enough(self) -> None:
        assert isinstance(decide_("1800", [1800]), Suspect)

    def test_a_suspect_the_other_way_breaks_the_run(self) -> None:
        assert isinstance(decide_("1800", [1800, 1400, 1800]), Suspect)

    def test_a_suspect_that_did_not_jump_breaks_the_run(self) -> None:
        # 1650 was held back because the control disagreed, not because it jumped.
        assert isinstance(decide_("1800", [1800, 1650]), Suspect)

    def test_the_control_confirms_a_jump_at_once(self) -> None:
        # Within 1.5 % of the control: 1774 * 0.015 = 26.61 >= 26; 1773 * 0.015 = 26.595 < 27.
        outcome = decide_("1800", control="1774")
        assert outcome == Accepted(
            reading("1800"), confirmed=True, detail="the control source agrees (1774)"
        )
        assert isinstance(decide_("1800", control="1773"), Suspect)


class TestControl:
    """A reading the control disagrees with is held back, and confirms itself when the 2
    suspects before it are within 5 % of it: the value persists."""

    def test_agreement_and_no_control_accept(self) -> None:
        assert decide_("1610", control="1600") == Accepted(reading("1610"))
        assert decide_("1610") == Accepted(reading("1610"))

    def test_a_disagreement_is_a_suspect(self) -> None:
        # 1650 is within 5 % of 1600, but 1.5 % of the control is 24.15: up to 1634.15.
        outcome = decide_("1650", control="1610")
        assert outcome == Suspect(reading("1650"), detail="the control source says 1610")

    def test_a_persistent_disagreement_confirms_itself(self) -> None:
        outcome = decide_("1650", [1640, 1655], control="1610")
        assert outcome == Accepted(
            reading("1650"), confirmed=True, detail="the value persisted for 3 readings"
        )

    def test_persistence_needs_the_suspects_within_max_jump_of_the_reading(self) -> None:
        # 1650 * 1.05 = 1732.5
        assert isinstance(decide_("1650", [1733, 1655], control="1610"), Suspect)
        assert isinstance(decide_("1650", ["1732.5", 1655], control="1610"), Accepted)

    def test_a_reading_back_near_the_last_accepted_is_accepted(self) -> None:
        assert decide_("1610", [1800, 1800]) == Accepted(reading("1610"))
