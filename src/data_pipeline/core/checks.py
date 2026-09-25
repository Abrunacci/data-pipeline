"""The checks a reading goes through before it is published.

They run in order: the value itself (``value_problem``), then whether it is plausible and
fresh (``reading_problem``), and last whether it follows from the values before it
(``decide``). A malformed response never gets here: sources refuse it when they parse it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from data_pipeline.core.readings import Accepted, Reading, Rejection, Suspect

# The calculator in cuanto-cuesta accepts prices greater than zero, up to 1,000,000, with at most
# 8 decimals (frontend/src/calculator/inputs.ts). A published value must be one it accepts.
MAX_VALUE = Decimal(1_000_000)
MAX_DECIMALS = 8

# Clocks differ a little between a source and this server; more than this is a wrong timestamp.
CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class Range:
    """The closed range a value can plausibly be in. It catches wrong fields and units."""

    min: Decimal
    max: Decimal

    def __post_init__(self) -> None:
        if not self.min.is_finite() or not self.max.is_finite() or not 0 < self.min < self.max:
            raise ValueError(f"need 0 < min < max, got {self.min} and {self.max}")

    def __contains__(self, value: Decimal) -> bool:
        return self.min <= value <= self.max


@dataclass(frozen=True, slots=True)
class Rules:
    """How a series is checked.

    ``max_jump`` and ``confirm_within`` are fractions (0.05 is 5 %). A reading that moves more
    than ``max_jump`` from the last accepted value is a suspect; it is confirmed when the
    ``confirmations`` readings after it stay within ``confirm_within`` of it.
    """

    plausible: Range
    max_age: timedelta
    max_jump: Decimal
    confirm_within: Decimal
    confirmations: int = 2

    def __post_init__(self) -> None:
        if self.max_age <= timedelta(0):
            raise ValueError(f"max_age must be positive, got {self.max_age}")
        if not 0 < self.max_jump < 1:
            raise ValueError(f"max_jump must be between 0 and 1, got {self.max_jump}")
        if not 0 < self.confirm_within < self.max_jump:
            raise ValueError(
                f"confirm_within must be between 0 and max_jump, got {self.confirm_within}"
            )
        if self.confirmations < 1:
            raise ValueError(f"confirmations must be at least 1, got {self.confirmations}")


def canonical(value: Decimal) -> Decimal:
    """``value`` without trailing zeros, and with no positive exponent: 1615.300 → 1615.3,
    1E+3 → 1000. Write it with ``format(value, "f")``: ``str`` uses exponents for tiny values."""
    if value == value.to_integral_value():
        return value.quantize(Decimal(1))
    return value.normalize()


def decimals(value: Decimal) -> int:
    """How many decimals ``value`` has once trailing zeros are dropped."""
    exponent = canonical(value).as_tuple().exponent
    assert isinstance(exponent, int)  # only NaN and infinities have a str exponent
    return max(0, -exponent)


def value_problem(value: Decimal) -> tuple[Rejection, str] | None:
    """Why ``value`` cannot be published as a price, or None if it can."""
    if not value.is_finite():
        return Rejection.NOT_FINITE, f"got {value}"
    if value <= 0:
        return Rejection.NOT_POSITIVE, f"got {value}"
    if value > MAX_VALUE:
        return Rejection.TOO_LARGE, f"must be at most {MAX_VALUE}, got {value}"
    if decimals(value) > MAX_DECIMALS:
        return Rejection.TOO_MANY_DECIMALS, f"must have at most {MAX_DECIMALS}, got {value}"
    return None


def reading_problem(
    reading: Reading, rules: Rules, fetched_at: datetime
) -> tuple[Rejection, str] | None:
    """Why ``reading`` cannot be used, or None if it can."""
    if (problem := value_problem(reading.value)) is not None:
        return problem
    if reading.value not in rules.plausible:
        return (
            Rejection.IMPLAUSIBLE,
            f"expected {rules.plausible.min} to {rules.plausible.max}, got {reading.value}",
        )
    if reading.as_of > fetched_at + CLOCK_SKEW:
        return Rejection.FROM_THE_FUTURE, f"as_of {reading.as_of}, fetched at {fetched_at}"
    if fetched_at - reading.as_of > rules.max_age:
        return Rejection.STALE, f"as_of {reading.as_of} is older than {rules.max_age}"
    return None


def decide(
    reading: Reading,
    last_accepted: Decimal | None,
    suspects: Sequence[Decimal],
    rules: Rules,
) -> Accepted | Suspect:
    """Whether a valid reading is published now or held back as a suspect.

    ``suspects`` are the values held back since ``last_accepted``, oldest first. They form runs:
    a suspect within ``confirm_within`` of the first one in the current run joins it, and any
    other starts a new run. A reading that joins a run which already has
    ``confirmations - 1`` followers confirms it: the market really moved.
    """
    if last_accepted is None or _within(reading.value, last_accepted, rules.max_jump):
        return Accepted(reading)

    anchor: Decimal | None = None
    followers = 0
    for value in suspects:
        if anchor is not None and _within(value, anchor, rules.confirm_within):
            followers += 1
        else:
            anchor, followers = value, 0

    joins = anchor is not None and _within(reading.value, anchor, rules.confirm_within)
    if joins and followers + 1 >= rules.confirmations:
        return Accepted(reading, confirmed=True)
    return Suspect(reading)


def _within(value: Decimal, reference: Decimal, fraction: Decimal) -> bool:
    return abs(value - reference) <= reference * fraction
