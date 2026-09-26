from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal

import pytest

from data_pipeline.core.gap import GapSample, summarize

WHEN = datetime(2026, 9, 25, 18, 18, tzinfo=UTC)


def sample(amount: str, fee: str, listed: str, final: str) -> GapSample:
    return GapSample(WHEN, Decimal(amount), Decimal(fee), Decimal(listed), Decimal(final))


def test_the_observed_purchase_gives_a_2_37_percent_price_gap() -> None:
    # 2026-09-25: 10 USD, 0.20 fee, 9.7763382 USDT listed, 9.35393217 received.
    # Final price 9.35393217 / 9.80 = 0.954482874...; listed 0.97763382; 1 - ratio = 0.023676...
    # With the fee, the total shortfall would be 4.32 %: the fee is not in the price gap.
    gap = sample("10", "0.20", "9.7763382", "9.35393217").gap
    assert gap.quantize(Decimal("0.0001"), ROUND_HALF_EVEN) == Decimal("0.0237")


def test_a_final_price_equal_to_the_list_is_no_gap() -> None:
    # 98 USD of price after a 2 USD fee, at the listed 1 USDT per USD.
    assert sample("100", "2", "100", "98").gap == 0


def test_the_median_is_published_with_the_dates() -> None:
    days = [datetime(2026, 9, day, 15, tzinfo=UTC) for day in (29, 25, 27)]
    samples = [
        GapSample(day, Decimal(100), Decimal(2), Decimal(100), Decimal(final))
        for day, final in zip(days, ("97", "96", "90"), strict=True)
    ]
    summary = summarize(samples)
    assert summary is not None
    assert summary.fraction == 1 - Decimal(96) / 98
    assert summary.samples == 3
    assert (summary.first, summary.last) == (days[1], days[0])


@pytest.mark.parametrize(
    ("amount", "fee", "listed", "final", "message"),
    [
        ("10", "0.2", "9.77", "0", "positive"),
        ("10", "10", "9.77", "9.35", "less than the amount"),
        ("10", "-0.2", "9.77", "9.35", "at least 0"),
        ("100", "2", "100", "99", "better than"),
    ],
)
def test_impossible_observations_are_refused(
    amount: str, fee: str, listed: str, final: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        sample(amount, fee, listed, final)
