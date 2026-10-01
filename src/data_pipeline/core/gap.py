"""How much less a trade gets than an indicative price promises, from observed pairs.

Binance lists a card price before the purchase and shows the final one only on the last screen,
where it also charges a fee. Each observed pair gives one gap in the price alone: the final
screen's price per unit paid after the fee, against the listed price. The fee is left out
because the calculator charges it on its own (``binance_card_purchase``). The published gap is
the median, with how many observations there are and their dates.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from statistics import median

from data_pipeline.core.checks import SMALLEST_STEP


@dataclass(frozen=True, slots=True)
class GapSample:
    """One observation: for ``amount`` paid, the list promised ``listed`` and the final screen
    gave ``final`` after charging ``fee`` (in the currency paid)."""

    observed_at: datetime
    amount: Decimal
    fee: Decimal
    listed: Decimal
    final: Decimal

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        amounts = (self.amount, self.listed, self.final)
        if not all(amount.is_finite() and amount > 0 for amount in amounts):
            raise ValueError(f"amounts must be positive and finite: {self}")
        if not (self.fee.is_finite() and 0 <= self.fee < self.amount):
            raise ValueError(f"fee must be at least 0 and less than the amount: {self}")
        if self.kept > 1:
            raise ValueError(f"the final price is better than the listed one: {self}")

    @property
    def kept(self) -> Fraction:
        """The share of the listed price the final screen gave, fee aside, exactly: with 10 USD
        paid, a 0.20 fee, 9.7763382 listed and 9.35393217 final, it is
        (9.35393217 / 9.80) / (9.7763382 / 10) = 0.97632..., a 2.37 % gap."""
        final_price = Fraction(self.final) / (Fraction(self.amount) - Fraction(self.fee))
        listed_price = Fraction(self.listed) / Fraction(self.amount)
        return final_price / listed_price

    @property
    def gap(self) -> Decimal:
        """``1 - kept``, for display: rounded to the default 28 significant digits."""
        return _decimal(1 - self.kept)


@dataclass(frozen=True, slots=True)
class Gap:
    """The median of the samples' ``kept`` share, exact (a ``Fraction``), so estimates built on
    it round only once. ``fraction`` is the gap for display."""

    kept: Fraction
    samples: int
    first: datetime
    last: datetime

    def __post_init__(self) -> None:
        if not 0 < self.kept <= 1:
            raise ValueError(f"the kept share must be above 0 and at most 1, got {self.kept}")

    @property
    def fraction(self) -> Decimal:
        return _decimal(1 - self.kept)


def estimate_final(listed: Decimal, gap: Gap) -> Decimal:
    """The final price the gap predicts for a ``listed`` one, ``listed * kept``, computed
    exactly and rounded down once to the smallest step a value can have, the project's rule for
    what a person gets. 0.97768597 listed, with the gap of the purchase observed on 2026-09-25,
    gives 0.95453378. It may round to 0: callers check it like any value."""
    steps = math.floor(Fraction(listed) * gap.kept / Fraction(SMALLEST_STEP))
    return Decimal(steps) * SMALLEST_STEP


def summarize(samples: Sequence[GapSample]) -> Gap | None:
    """The median of the samples' kept share, or None when there are none."""
    if not samples:
        return None
    moments = [sample.observed_at for sample in samples]
    return Gap(
        kept=median(sample.kept for sample in samples),
        samples=len(samples),
        first=min(moments),
        last=max(moments),
    )


def _decimal(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)
