from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from data_pipeline.api.app import create_app
from data_pipeline.config import Settings
from data_pipeline.core.readings import Accepted, Observation, Reading, Suspect
from data_pipeline.storage.postgres import PostgresStore

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


async def test_every_series_is_listed_even_without_a_value(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/rates/latest")
    assert response.status_code == 200
    assert response.json() == {"rates": {"bitso_usdt_ars": None}}


async def test_the_latest_rate_says_when_it_is_from(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    now = datetime.now(UTC)
    store = PostgresStore(engine)
    as_of = now - timedelta(minutes=5)
    reading = Reading(Decimal("0.00000001"), as_of)  # plain notation, never 1E-8
    await store.record(Observation("bitso_usdt_ars", "bitso_usdt_ars_bid", now, Accepted(reading)))

    rate = (await client.get("/v1/rates/latest")).json()["rates"]["bitso_usdt_ars"]
    assert rate["value"] == "0.00000001"
    assert datetime.fromisoformat(rate["as_of"]) == as_of
    assert datetime.fromisoformat(rate["fetched_at"]) == now
    assert rate["source"] == "bitso_usdt_ars_bid"
    assert rate["stale"] is False
    assert rate["pending_confirmation"] is False


async def test_an_old_rate_is_stale_and_a_suspect_is_pending(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    now = datetime.now(UTC)
    store = PostgresStore(engine)
    old = Reading(Decimal(1600), now - timedelta(minutes=31))
    await store.record(Observation("bitso_usdt_ars", "s", old.as_of, Accepted(old)))
    jump = Reading(Decimal(1800), now)
    await store.record(Observation("bitso_usdt_ars", "s", now, Suspect(jump)))

    rate = (await client.get("/v1/rates/latest")).json()["rates"]["bitso_usdt_ars"]
    assert rate["value"] == "1600"
    assert rate["stale"] is True
    assert rate["pending_confirmation"] is True


async def test_the_calculator_origin_may_read_it(client: httpx.AsyncClient) -> None:
    origin = "https://cuanto-cuesta.abrunacci.dev"
    response = await client.get("/v1/rates/latest", headers={"Origin": origin})
    assert response.headers["access-control-allow-origin"] == origin
    other = await client.get("/v1/rates/latest", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in other.headers
