from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_pipeline.config import DEFAULT_SERIES_FILE, load_series
from data_pipeline.core.readings import Reading
from data_pipeline.core.schedule import BUENOS_AIRES
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.runner.backfill import backfill
from data_pipeline.runner.scheduler import run_series
from data_pipeline.sources import available_history_sources, available_sources
from data_pipeline.sources.argentinadatos import ArgentinaDatosDaily
from tests.fakes import MemoryStore

pytestmark = pytest.mark.anyio

FIXTURE = Path(__file__).parents[1] / "sources" / "fixtures" / "argentinadatos_bolsa_trimmed.json"
SOURCE = ArgentinaDatosDaily("bolsa", "compra", time(17, 0), BUENOS_AIRES)
MEP = next(
    s
    for s in load_series(DEFAULT_SERIES_FILE, available_sources(), available_history_sources())
    if s.id == "mep"
)
# Friday 2026-09-25, 12:00 in Buenos Aires.
NOW = datetime(2026, 9, 25, 15, 0, tzinfo=UTC)


def client(body: bytes, status: int = 200) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status, content=body))
    )


class TestArgentinaDatos:
    def test_the_mep_has_this_history_source(self) -> None:
        assert MEP.history == SOURCE.name == "argentinadatos_bolsa_compra_daily"
        assert available_history_sources()[SOURCE.name] == SOURCE

    def test_reads_the_buy_side_as_of_the_close(self) -> None:
        # Recorded 2026-09-25 (8 of 2889 rows): the first two of 2018 and the last six.
        readings = SOURCE.parse(FIXTURE.read_bytes(), NOW)
        assert len(readings) == 8
        assert readings[0].value == Decimal("36.97")
        assert readings[0].as_of == datetime(2018, 10, 29, 20, 0, tzinfo=UTC)
        assert readings[-1].value == Decimal(1539)
        assert readings[-2].value == Decimal("1537.6")  # venta was 1542.9

    @pytest.mark.parametrize(
        "row",
        [
            {"casa": "oficial", "compra": 1, "venta": 1, "fecha": "2026-09-24"},
            {"casa": "bolsa", "compra": "1537.6", "venta": 1, "fecha": "2026-09-24"},
            {"casa": "bolsa", "compra": 1537.6, "venta": 1, "fecha": "24/09/2026"},
        ],
        ids=["other-casa", "text-price", "other-date-format"],
    )
    def test_a_changed_format_is_malformed(self, row: dict[str, object]) -> None:
        with pytest.raises(MalformedResponseError):
            SOURCE.parse(json.dumps([row]).encode(), NOW)


async def test_loads_past_open_days_once() -> None:
    store = MemoryStore()
    loaded = await backfill(MEP, SOURCE, client(FIXTURE.read_bytes()), store, lambda: NOW)
    # Of the 8 rows: 2026-09-20 is a Sunday and 2026-09-25 is today; 6 are kept, including the
    # 2018 ones, which the plausible range for today's prices would refuse.
    assert loaded == 6
    days = await store.daily("mep", date(2018, 1, 1), date(2026, 9, 25), BUENOS_AIRES)
    assert [day.date for day in days] == [
        date(2018, 10, 29),
        date(2018, 10, 30),
        date(2026, 9, 21),
        date(2026, 9, 22),
        date(2026, 9, 23),
        date(2026, 9, 24),
    ]
    assert await backfill(MEP, SOURCE, client(b"unused"), store, lambda: NOW) is None
    assert len(store.history) == 6


async def test_a_failed_load_stores_nothing_and_is_tried_again() -> None:
    store = MemoryStore()
    assert await backfill(MEP, SOURCE, client(b"<html>", 200), store, lambda: NOW) is None
    assert await backfill(MEP, SOURCE, client(b"", 404), store, lambda: NOW) is None
    assert store.history == []
    assert await backfill(MEP, SOURCE, client(FIXTURE.read_bytes()), store, lambda: NOW) == 6


async def test_another_process_loading_it_is_left_alone() -> None:
    store = MemoryStore()
    store.busy.add("mep")
    assert await backfill(MEP, SOURCE, client(FIXTURE.read_bytes()), store, lambda: NOW) is None
    assert store.history == []


async def test_past_values_are_not_attempts_or_published_values() -> None:
    store = MemoryStore()
    await backfill(MEP, SOURCE, client(FIXTURE.read_bytes()), store, lambda: NOW)
    latest = await store.latest("mep")
    assert latest.published is None
    assert latest.last_attempt_at is None
    assert not await store.attempted_in("mep", NOW, NOW.replace(hour=16))


async def test_a_broken_history_source_never_stops_the_schedule() -> None:
    @dataclass(frozen=True)
    class Broken:
        name: str = SOURCE.name

        def request(self) -> Request:
            return SOURCE.request()

        def parse(self, body: bytes, fetched_at: datetime) -> list[Reading]:
            raise RuntimeError("a bug")

    bitso = next(
        s
        for s in load_series(DEFAULT_SERIES_FILE, available_sources(), available_history_sources())
        if s.id == "bitso_usdt_ars"
    )
    series = replace(bitso, history=SOURCE.name, hours=MEP.hours)
    answers = {
        "api.argentinadatos.com": b"[]",
        "api.bitso.com": (
            Path(__file__).parents[1] / "sources" / "fixtures" / "bitso_ticker_usdt_ars.json"
        ).read_bytes(),
        "criptoya.com": b"Invalid pair",
    }
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=answers[r.url.host]))
    )

    async def stop(seconds: float) -> None:
        raise asyncio.CancelledError

    store = MemoryStore()
    fetched = datetime(2026, 9, 25, 18, 3, 31, tzinfo=UTC)
    with pytest.raises(asyncio.CancelledError):
        await run_series(
            series, available_sources(), {SOURCE.name: Broken()}, http, store, lambda: fetched, stop
        )
    assert store.history == []
    # The slot ran anyway: the control failed, Bitso's reading was accepted.
    assert [o.source for o in store.observations] == [
        "criptoya_bitso_usdt_ars_bid",
        "bitso_usdt_ars_bid",
    ]
