from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from data_pipeline.api.schemas import latest_rate
from data_pipeline.config import DEFAULT_SERIES_FILE, load_series
from data_pipeline.core.gap import Gap
from data_pipeline.runner.store import Latest, Published
from data_pipeline.sources import available_sources

SERIES = {s.id: s for s in load_series(DEFAULT_SERIES_FILE, available_sources())}
# Friday 2026-09-25, 17:00 in Buenos Aires.
FRIDAY_CLOSE = datetime(2026, 9, 25, 20, 0, tzinfo=UTC)


def published(as_of: datetime) -> Latest:
    return Latest(Published(Decimal(1550), as_of, as_of, "s"), None, as_of)


def test_the_mep_ages_only_while_the_market_is_open() -> None:
    mep = SERIES["mep"]
    latest = published(FRIDAY_CLOSE)
    # Monday 10:30 in Buenos Aires: 30 open minutes on Friday, none yet on Monday.
    assert not latest_rate(latest, mep, datetime(2026, 9, 28, 13, 30, tzinfo=UTC)).stale
    # Monday 11:15: 30 + 30 = 60 open minutes, the limit.
    assert not latest_rate(latest, mep, datetime(2026, 9, 28, 14, 15, tzinfo=UTC)).stale
    # Monday 11:16: 61.
    assert latest_rate(latest, mep, datetime(2026, 9, 28, 14, 16, tzinfo=UTC)).stale


def test_a_series_without_hours_ages_all_the_time() -> None:
    bitso = SERIES["bitso_usdt_ars"]
    latest = published(FRIDAY_CLOSE)
    assert not latest_rate(latest, bitso, FRIDAY_CLOSE + timedelta(minutes=30)).stale
    assert latest_rate(latest, bitso, FRIDAY_CLOSE + timedelta(minutes=31)).stale


def test_a_suspect_is_pending_while_it_is_at_most_three_intervals_old() -> None:
    bitso = SERIES["bitso_usdt_ars"]
    value = Published(Decimal(1600), FRIDAY_CLOSE, FRIDAY_CLOSE, "s")
    latest = Latest(value, FRIDAY_CLOSE, FRIDAY_CLOSE)
    edge = FRIDAY_CLOSE + timedelta(minutes=30)
    assert latest_rate(latest, bitso, edge).pending_confirmation
    assert not latest_rate(latest, bitso, edge + timedelta(seconds=1)).pending_confirmation


def test_the_gap_is_published_in_percent() -> None:
    card = SERIES["p2p_usdt_usd"]
    gap = Gap(Decimal("0.04320459"), 3, FRIDAY_CLOSE, FRIDAY_CLOSE + timedelta(days=4))
    indicative = replace(card, indicative=True, gap=gap)
    rate = latest_rate(published(FRIDAY_CLOSE), indicative, FRIDAY_CLOSE)
    assert rate.indicative
    assert rate.final_price_gap is not None
    assert rate.final_price_gap.percent == "4.32"
    assert rate.final_price_gap.samples == 3
