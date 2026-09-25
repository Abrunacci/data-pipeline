from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from data_pipeline.core.sources import MalformedResponseError
from data_pipeline.sources.bitso import BitsoBid

FIXTURES = Path(__file__).parent / "fixtures"
SOURCE = BitsoBid("usdt_ars")


def ticker(**payload: object) -> bytes:
    base = json.loads((FIXTURES / "bitso_ticker_usdt_ars.json").read_bytes())
    base["payload"].update(payload)
    return json.dumps(base).encode()


def test_asks_for_the_ticker_of_its_book() -> None:
    request = SOURCE.request()
    assert request.method == "GET"
    assert request.url == "https://api.bitso.com/v3/ticker/?book=usdt_ars"


def test_reads_the_bid_and_its_time_from_a_recorded_response() -> None:
    # Recorded on 2026-09-25: bid 1615.300000000000, ask 1615.97, last 1615.97.
    reading = SOURCE.parse((FIXTURES / "bitso_ticker_usdt_ars.json").read_bytes())
    assert reading.value == Decimal("1615.3")
    assert reading.as_of == datetime(2026, 9, 25, 18, 3, 30, tzinfo=UTC)


def test_a_recorded_error_is_malformed() -> None:
    body = (FIXTURES / "bitso_ticker_error.json").read_bytes()
    with pytest.raises(MalformedResponseError, match="Unknown OrderBook"):
        SOURCE.parse(body)


@pytest.mark.parametrize(
    "body",
    [
        b"<!DOCTYPE html><title>Just a moment...</title>",
        b"",
        b'{"success": false, "payload": {}}',
        ticker(bid=None),
        ticker(bid="n/a"),
        ticker(created_at="2026-09-25T18:03:30"),
        ticker(created_at=1790359410),
        ticker(created_at="yesterday"),
        ticker(bid=1615.3),
        ticker(bid=" 1615.3"),
        ticker(bid="1_615.3"),
        ticker(bid="-1615.3"),
    ],
    ids=[
        "html",
        "empty",
        "not-success",
        "no-bid",
        "bid-not-a-number",
        "naive-time",
        "epoch-time",
        "not-a-time",
        "bid-as-json-number",
        "bid-with-spaces",
        "bid-with-underscore",
        "negative-bid",
    ],
)
def test_anything_else_is_malformed(body: bytes) -> None:
    with pytest.raises(MalformedResponseError):
        SOURCE.parse(body)


def test_another_book_is_malformed() -> None:
    with pytest.raises(MalformedResponseError, match="expected book usdt_ars"):
        SOURCE.parse(ticker(book="usdc_ars"))
