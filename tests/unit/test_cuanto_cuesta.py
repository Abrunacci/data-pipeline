from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from uuid import UUID

import pytest

from data_pipeline.config import DEFAULT_SERIES_FILE, load_series
from data_pipeline.core.gap import Gap
from data_pipeline.core.readings import Accepted, Observation, Reading, Rejected, Rejection
from data_pipeline.core.sources import Request
from data_pipeline.destinations.cuanto_cuesta import RATES, CuantoCuestaLog, batch, rate_item
from data_pipeline.sources import available_sources

pytestmark = pytest.mark.anyio

SOURCES = available_sources()
SERIES = {s.id: s for s in load_series(DEFAULT_SERIES_FILE, SOURCES)}
ID = UUID("0b8e6a3c-5d1f-4c1e-9a77-2f0c8f3e1b20")
# 12:00 in Buenos Aires.
AS_OF = datetime(2026, 10, 1, 12, 0, tzinfo=timezone(timedelta(hours=-3)))


def accepted(series_id: str, source: str, value: str) -> Observation:
    reading = Reading(Decimal(value), AS_OF)
    return Observation(series_id, source, AS_OF, Accepted(reading))


def destination() -> CuantoCuestaLog:
    return CuantoCuestaLog(SERIES.values(), SOURCES, new_id=lambda: ID)


def test_an_item_carries_its_key_pair_source_and_time_in_utc() -> None:
    payload = destination().payload(
        SERIES["bitso_usdt_ars"], accepted("bitso_usdt_ars", "bitso_usdt_ars_bid", "1452.3")
    )
    assert payload == {
        "batch_id": "0b8e6a3c-5d1f-4c1e-9a77-2f0c8f3e1b20",
        "rates": [
            {
                "key": "bitso_usdt_ars",
                "base": "USDT",
                "quote": "ARS",
                "price": "1452.3",
                "source": "bitso_usdt_ars_bid",
                "source_url": SOURCES["bitso_usdt_ars_bid"].request().url,
                "observed_at": "2026-10-01T15:00:00Z",
            }
        ],
    }


def test_every_series_is_a_rate_with_the_pair_cuanto_cuesta_expects() -> None:
    assert set(SERIES) == set(RATES)
    pairs = {key: (rate.base, rate.quote) for key, rate in RATES.items()}
    assert pairs == {
        "mep": ("USD", "ARS"),
        "binance_p2p_usdt_usd": ("USDT", "USD"),
        "bitso_usdt_ars": ("USDT", "ARS"),
        "arq_usd_ars": ("USD", "ARS"),
        "binance_card_usd_usdt": ("USD", "USDT"),
    }


def test_only_the_card_carries_estimated_final() -> None:
    for series in SERIES.values():
        source = series.sources[0]
        item = rate_item(series, source, None, Reading(Decimal("1"), AS_OF))
        assert ("estimated_final" in item) == (series.id == "binance_card_usd_usdt"), series.id


def test_the_card_sends_the_gap_estimate() -> None:
    card = SERIES["binance_card_usd_usdt"]
    assert card.gap is not None
    item = rate_item(
        card, "binance_card_usd_usdt_list", None, Reading(Decimal("0.97768597"), AS_OF)
    )
    # The repo's observed pair, as in core.gap.estimate_final's docstring.
    assert item["price"] == "0.97768597"
    assert item["estimated_final"] == "0.95453378"


def test_the_card_sends_a_null_estimate_without_a_gap() -> None:
    card = replace(SERIES["binance_card_usd_usdt"], gap=None)
    item = rate_item(card, "binance_card_usd_usdt_list", None, Reading(Decimal("0.98"), AS_OF))
    assert "estimated_final" in item
    assert item["estimated_final"] is None


def test_an_estimate_cuanto_cuesta_would_refuse_is_sent_as_null() -> None:
    # A 60 % gap puts the estimate under the 0.5 floor: the whole item would be rejected.
    gap = Gap(Fraction(4, 10), 1, AS_OF, AS_OF)
    card = replace(SERIES["binance_card_usd_usdt"], gap=gap)
    item = rate_item(card, "binance_card_usd_usdt_list", None, Reading(Decimal("0.98"), AS_OF))
    assert item["estimated_final"] is None


def test_a_source_url_that_is_not_https_is_sent_as_null() -> None:
    class Plain:
        name = "plain"

        def request(self) -> Request:
            return Request("GET", "http://plain.example/")

        def parse(self, body: bytes, fetched_at: datetime) -> Reading:
            raise NotImplementedError

    mep = SERIES["mep"]
    log = CuantoCuestaLog([mep], {"plain": Plain()}, new_id=lambda: ID)
    payload = log.payload(mep, accepted("mep", "plain", "1547.2"))
    rates = payload["rates"]
    assert isinstance(rates, list)
    assert rates[0]["source_url"] is None


def test_a_series_cuanto_cuesta_does_not_take_fails_at_startup() -> None:
    other = replace(SERIES["mep"], id="race_results")
    with pytest.raises(ValueError, match="race_results"):
        CuantoCuestaLog([*SERIES.values(), other], SOURCES)


def test_only_accepted_readings_are_sent() -> None:
    failed = Observation("mep", "dolarapi_mep_compra", AS_OF, Rejected(Rejection.STALE, "old"))
    with pytest.raises(ValueError, match="only accepted"):
        destination().payload(SERIES["mep"], failed)


def test_a_batch_holds_1_to_20_items() -> None:
    item: dict[str, object] = {"key": "mep"}
    rates = batch([item] * 20, ID)["rates"]
    assert isinstance(rates, list)
    assert len(rates) == 20
    for size in (0, 21):
        with pytest.raises(ValueError, match="1 to 20"):
            batch([item] * size, ID)


async def test_send_logs_the_batch_as_one_json_line(caplog: pytest.LogCaptureFixture) -> None:
    observation = accepted("mep", "dolarapi_mep_compra", "1547.2")
    with caplog.at_level("INFO", logger="data_pipeline.destinations.cuanto_cuesta"):
        await destination().send(SERIES["mep"], observation)
    (message,) = caplog.messages
    prefix = "cuanto-cuesta batch, not sent: "
    assert message.startswith(prefix)
    assert "\n" not in message
    sent = json.loads(message.removeprefix(prefix))
    assert sent["rates"][0]["price"] == "1547.2"
    assert sent["rates"][0]["observed_at"] == "2026-10-01T15:00:00Z"
