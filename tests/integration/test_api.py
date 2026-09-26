from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from data_pipeline.api.app import HEALTH_TIMEOUT_SECONDS, create_app
from data_pipeline.config import Settings
from data_pipeline.core.readings import (
    Accepted,
    HeldBack,
    Observation,
    Reading,
    Rejected,
    Rejection,
    Suspect,
)
from data_pipeline.storage.postgres import PostgresStore
from tests.integration.conftest import Urls

pytestmark = pytest.mark.anyio


@pytest.fixture
async def client(engine: AsyncEngine, database_url: str) -> AsyncIterator[httpx.AsyncClient]:
    settings = Settings(
        database_url=database_url,
        run_scheduler=False,
        cors_origins=("https://cuanto-cuesta.abrunacci.dev",),
    )
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as http,
    ):
        yield http


async def test_health_checks_the_database(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_every_series_is_listed_even_without_a_value(
    client: httpx.AsyncClient, store: PostgresStore
) -> None:
    failed_at = datetime.now(UTC)
    await store.record(
        Observation("mep", "dolarapi_mep_compra", failed_at, Rejected(Rejection.MALFORMED, "html"))
    )
    response = await client.get("/v1/rates/latest")
    assert response.status_code == 200
    rates = response.json()["rates"]
    assert list(rates) == [
        "mep",
        "binance_p2p_usdt_usd",
        "bitso_usdt_ars",
        "arq_usd_ars",
        "binance_card_usd_usdt",
    ]
    assert rates["binance_card_usd_usdt"]["indicative"] is True
    assert rates["binance_card_usd_usdt"]["final_price_gap"]["samples"] >= 1
    # No value yet, so no estimate either.
    assert rates["binance_card_usd_usdt"]["estimated_final"] is None
    # No value yet, but the failed attempt shows: it is failing, not waiting to start.
    assert rates["mep"] == {
        "value": None,
        "as_of": None,
        "fetched_at": None,
        "source": None,
        "stale": True,
        "pending_confirmation": False,
        "last_attempt_at": failed_at.isoformat().replace("+00:00", "Z"),
        "official_source": True,
        "indicative": False,
        "final_price_gap": None,
        "estimated_final": None,
    }
    assert rates["bitso_usdt_ars"]["last_attempt_at"] is None
    assert rates["arq_usd_ars"]["official_source"] is False


async def test_the_latest_rate_says_when_it_is_from(
    client: httpx.AsyncClient, store: PostgresStore
) -> None:
    now = datetime.now(UTC)
    as_of = now - timedelta(minutes=5)
    reading = Reading(Decimal("0.00000001"), as_of)  # plain notation, never 1E-8
    await store.record(Observation("bitso_usdt_ars", "bitso_usdt_ars_bid", now, Accepted(reading)))

    rate = (await client.get("/v1/rates/latest")).json()["rates"]["bitso_usdt_ars"]
    assert rate["value"] == "0.00000001"
    assert datetime.fromisoformat(rate["as_of"]) == as_of
    assert rate["as_of"].endswith("Z")  # UTC, whatever the database's time zone
    assert datetime.fromisoformat(rate["fetched_at"]) == now
    assert rate["source"] == "bitso_usdt_ars_bid"
    assert rate["stale"] is False
    assert rate["pending_confirmation"] is False


async def test_an_old_rate_is_stale_and_a_suspect_is_pending(
    client: httpx.AsyncClient, store: PostgresStore
) -> None:
    now = datetime.now(UTC)
    old = Reading(Decimal(1600), now - timedelta(minutes=31))
    await store.record(Observation("bitso_usdt_ars", "s", old.as_of, Accepted(old)))
    jump = Reading(Decimal(1800), now)
    await store.record(
        Observation("bitso_usdt_ars", "s", now, Suspect(jump, HeldBack.JUMP, "jumped"))
    )

    rate = (await client.get("/v1/rates/latest")).json()["rates"]["bitso_usdt_ars"]
    assert rate["value"] == "1600"
    assert rate["stale"] is True
    assert rate["pending_confirmation"] is True


async def test_an_expired_suspect_is_no_longer_pending(
    client: httpx.AsyncClient, store: PostgresStore
) -> None:
    # Bitso runs every 10 minutes, so a suspect expires after 30.
    now = datetime.now(UTC)
    value = Reading(Decimal(1600), now - timedelta(minutes=5))
    await store.record(Observation("bitso_usdt_ars", "s", value.as_of, Accepted(value)))
    old = now - timedelta(minutes=31)
    await store.record(
        Observation(
            "bitso_usdt_ars",
            "s",
            old,
            Suspect(Reading(Decimal(1800), old), HeldBack.JUMP, "jumped"),
        )
    )
    rate = (await client.get("/v1/rates/latest")).json()["rates"]["bitso_usdt_ars"]
    assert rate["pending_confirmation"] is False


async def test_the_calculator_origin_may_read_it(client: httpx.AsyncClient) -> None:
    origin = "https://cuanto-cuesta.abrunacci.dev"
    response = await client.get("/v1/rates/latest", headers={"Origin": origin})
    assert response.headers["access-control-allow-origin"] == origin
    other = await client.get("/v1/rates/latest", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in other.headers


@pytest.fixture
async def client_without_database() -> AsyncIterator[httpx.AsyncClient]:
    # Nothing listens on port 1: the connection is refused at once.
    app = create_app(
        Settings(database_url="postgresql+psycopg://u:p@127.0.0.1:1/db", run_scheduler=False)
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as http,
    ):
        yield http


async def test_a_database_that_does_not_answer_fails_the_health_check_in_time() -> None:
    # Accepts connections and never answers, like a database behind a network partition.
    held: list[asyncio.StreamWriter] = []
    silent = await asyncio.start_server(lambda _, writer: held.append(writer), "127.0.0.1", 0)
    port = silent.sockets[0].getsockname()[1]
    app = create_app(
        Settings(database_url=f"postgresql+psycopg://u:p@127.0.0.1:{port}/db", run_scheduler=False)
    )
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as http,
        ):
            started = asyncio.get_running_loop().time()
            response = await http.get("/health")
            elapsed = asyncio.get_running_loop().time() - started
    finally:
        for writer in held:
            writer.close()
        silent.close()
        await silent.wait_closed()
    assert response.status_code == 503
    assert elapsed < HEALTH_TIMEOUT_SECONDS + 1


async def test_without_a_database_health_and_rates_answer_503(
    client_without_database: httpx.AsyncClient,
) -> None:
    health = await client_without_database.get("/health")
    assert health.status_code == 503
    assert health.json() == {"status": "database_unavailable"}
    rates = await client_without_database.get("/v1/rates/latest")
    assert rates.status_code == 503
    assert rates.json() == {"detail": "database_unavailable"}


async def test_a_database_error_that_is_not_an_outage_is_a_500(urls: Urls) -> None:
    # A role that can connect but may not read the table: a bug to fix, not a database that is
    # down, so it must not hide behind "database_unavailable".
    app = create_app(Settings(database_url=urls.no_grants, run_scheduler=False))
    transport = httpx.ASGITransport(app, raise_app_exceptions=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        response = await http.get("/v1/rates/latest")
    assert response.status_code == 500


async def test_history_gives_one_value_a_day(
    client: httpx.AsyncClient, store: PostgresStore
) -> None:
    yesterday = datetime(2026, 9, 24, 20, 0, tzinfo=UTC)  # 17:00 in Buenos Aires
    await store.record_history(
        "mep",
        "argentinadatos_bolsa_compra_daily",
        yesterday,
        [Reading(Decimal("1537.6"), yesterday)],
    )
    today = datetime(2026, 9, 25, 18, 0, tzinfo=UTC)
    await store.record(
        Observation("mep", "dolarapi_mep_compra", today, Accepted(Reading(Decimal(1539), today)))
    )
    response = await client.get("/v1/rates/mep/history?from=2026-09-24&to=2026-09-25")
    assert response.status_code == 200
    assert response.json() == {
        "series": "mep",
        "first": "2026-09-24",
        "last": "2026-09-25",
        "days": [
            {
                "date": "2026-09-24",
                "value": "1537.6",
                "as_of": "2026-09-24T20:00:00Z",
                "source": "argentinadatos_bolsa_compra_daily",
            },
            {
                "date": "2026-09-25",
                "value": "1539",
                "as_of": "2026-09-25T18:00:00Z",
                "source": "dolarapi_mep_compra",
            },
        ],
    }


async def test_history_refuses_unknown_series_and_bad_ranges(client: httpx.AsyncClient) -> None:
    assert (await client.get("/v1/rates/nope/history")).status_code == 404
    assert (await client.get("/v1/rates/mep/history?from=2026-09-25&to=2026-09-24")).json() == {
        "detail": "from_after_to"
    }
    too_long = await client.get("/v1/rates/mep/history?from=2025-01-01&to=2026-09-25")
    assert too_long.status_code == 422
    assert (await client.get("/v1/rates/mep/history?from=yesterday")).status_code == 422
    default = (await client.get("/v1/rates/mep/history")).json()
    assert (date.fromisoformat(default["last"]) - date.fromisoformat(default["first"])).days == 29
