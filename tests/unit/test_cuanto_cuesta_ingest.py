"""Posting to cuanto-cuesta's ``POST /api/ingest``, against a fake of its answers as its
``backend/README.md`` ("API") describes them."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from data_pipeline.config import DEFAULT_SERIES_FILE, load_series
from data_pipeline.core.readings import Accepted, Observation, Reading
from data_pipeline.destinations.cuanto_cuesta import CuantoCuestaIngest
from data_pipeline.sources import available_sources

pytestmark = pytest.mark.anyio

SOURCES = available_sources()
SERIES = {s.id: s for s in load_series(DEFAULT_SERIES_FILE, SOURCES)}
ID = UUID("0b8e6a3c-5d1f-4c1e-9a77-2f0c8f3e1b20")
URL = "http://cuanto-cuesta-backend:8000/api/ingest"
TOKEN = "not-a-real-token"
AS_OF = datetime(2026, 10, 1, 12, 0, tzinfo=timezone(timedelta(hours=-3)))
WHAT = f"cuanto-cuesta batch {ID} (bitso_usdt_ars)"

type Handler = Callable[[httpx.Request], httpx.Response]


def bitso(value: str = "1452.3") -> Observation:
    reading = Reading(Decimal(value), AS_OF)
    return Observation("bitso_usdt_ars", "bitso_usdt_ars_bid", AS_OF, Accepted(reading))


def answer(status: int, body: object) -> Handler:
    return lambda request: httpx.Response(status, json=body)


def result(status: str, error: str | None = None) -> dict[str, object]:
    item: dict[str, object] = {"index": 0, "key": "bitso_usdt_ars", "status": status}
    if error is not None:
        item["error"] = error
    return {"batch_id": str(ID), "results": [item]}


async def send(handler: Handler, caplog: pytest.LogCaptureFixture) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        ingest = CuantoCuestaIngest(SERIES.values(), SOURCES, client, URL, TOKEN, new_id=lambda: ID)
        with caplog.at_level("INFO", logger="data_pipeline.destinations.cuanto_cuesta"):
            await ingest.send(SERIES["bitso_usdt_ars"], bitso())


def records(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    return [(r.levelname, r.getMessage()) for r in caplog.records]


async def test_it_posts_the_batch_with_the_token(caplog: pytest.LogCaptureFixture) -> None:
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json=result("stored"))

    await send(handler, caplog)
    (request,) = sent
    assert request.method == "POST"
    assert str(request.url) == URL
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.content) == {
        "batch_id": str(ID),
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
    assert records(caplog) == [("INFO", f"{WHAT}: bitso_usdt_ars stored")]
    assert TOKEN not in caplog.text


@pytest.mark.parametrize(
    ("body", "logged"),
    [
        (result("stored"), ("INFO", f"{WHAT}: bitso_usdt_ars stored")),
        (result("unchanged"), ("INFO", f"{WHAT}: bitso_usdt_ars unchanged")),
        (
            result("older"),
            ("WARNING", f"{WHAT}: bitso_usdt_ars older than the current quote, ignored"),
        ),
        (
            result("rejected", "out_of_range"),
            ("ERROR", f"{WHAT}: bitso_usdt_ars rejected, out_of_range"),
        ),
    ],
)
async def test_each_item_result_is_logged(
    caplog: pytest.LogCaptureFixture, body: object, logged: tuple[str, str]
) -> None:
    await send(answer(200, body), caplog)
    assert records(caplog) == [logged]


async def test_a_401_reads_as_a_configuration_problem(caplog: pytest.LogCaptureFixture) -> None:
    await send(answer(401, {"error": "unauthorized"}), caplog)
    assert records(caplog) == [
        (
            "ERROR",
            f"{WHAT} dropped: CONFIGURATION: the token was refused (HTTP 401 unauthorized);"
            " check that CUANTO_CUESTA_INGEST_TOKEN holds cuanto-cuesta's INGEST_TOKEN",
        )
    ]


@pytest.mark.parametrize(
    ("status", "code"), [(400, "malformed_json"), (413, "too_large"), (422, "invalid_envelope")]
)
async def test_a_refused_batch_is_logged_with_its_status_and_code(
    caplog: pytest.LogCaptureFixture, status: int, code: str
) -> None:
    await send(answer(status, {"error": code}), caplog)
    assert records(caplog) == [
        ("ERROR", f"{WHAT} dropped: the batch was refused (HTTP {status} {code})")
    ]
    assert "CONFIGURATION" not in caplog.text


@pytest.mark.parametrize(
    ("handler", "logged"),
    [
        (
            lambda request: httpx.Response(503, text="<html>bad gateway</html>"),
            "unexpected answer (HTTP 503 without an error code)",
        ),
        # What the public proxy answers: a URL pointing outside the internal network.
        (
            answer(404, {"detail": "Not Found"}),
            "unexpected answer (HTTP 404 without an error code)",
        ),
    ],
)
async def test_an_answer_outside_the_contract_is_logged(
    caplog: pytest.LogCaptureFixture, handler: Handler, logged: str
) -> None:
    await send(handler, caplog)
    assert records(caplog) == [("ERROR", f"{WHAT} dropped: {logged}")]


async def test_a_200_with_an_unexpected_body_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    await send(answer(200, {"ok": True}), caplog)
    assert records(caplog) == [
        ("ERROR", f'{WHAT}: answered 200 with an unexpected body: {{"ok":true}}')
    ]


async def test_cuanto_cuesta_down_drops_the_value_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno 111] Connection refused", request=request)

    await send(refuse, caplog)
    assert records(caplog) == [
        ("ERROR", f"{WHAT} dropped: no answer: ConnectError: [Errno 111] Connection refused")
    ]


async def test_cuanto_cuesta_hanging_drops_the_value(caplog: pytest.LogCaptureFixture) -> None:
    calls = 0

    async def hang(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(10)
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(hang)) as client:
        ingest = CuantoCuestaIngest(
            SERIES.values(), SOURCES, client, URL, TOKEN, new_id=lambda: ID, send_timeout=0.01
        )
        with caplog.at_level("INFO", logger="data_pipeline.destinations.cuanto_cuesta"):
            await ingest.send(SERIES["bitso_usdt_ars"], bitso())
    assert calls == 1  # not retried
    assert records(caplog) == [("ERROR", f"{WHAT} dropped: no answer in 0.01 s")]
