from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from data_pipeline.core.sources import Request
from data_pipeline.runner.fetch import FetchError, fetch

pytestmark = pytest.mark.anyio

REQUEST = Request("POST", "https://source.example/rate", json={"asset": "USDT"})


def client(answers: list[httpx.Response | Exception]) -> tuple[httpx.AsyncClient, list[bytes]]:
    sent: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request.content)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), sent


def no_sleep() -> tuple[Callable[[float], object], list[float]]:
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    return sleep, waits


async def test_returns_the_body_of_a_200_and_sends_the_json() -> None:
    http, sent = client([httpx.Response(200, content=b"ok")])
    assert await fetch(http, REQUEST) == b"ok"
    assert sent == [b'{"asset":"USDT"}']


async def test_retries_network_errors_and_5xx_with_the_given_delays() -> None:
    http, sent = client(
        [httpx.ConnectError("down"), httpx.Response(503), httpx.Response(200, content=b"ok")]
    )
    sleep, waits = no_sleep()
    assert await fetch(http, REQUEST, delays=(1, 3), sleep=sleep) == b"ok"  # type: ignore[arg-type]
    assert waits == [1, 3]
    assert len(sent) == 3


async def test_gives_up_after_the_last_retry() -> None:
    http, sent = client([httpx.Response(500), httpx.Response(502), httpx.Response(504)])
    sleep, _ = no_sleep()
    with pytest.raises(FetchError, match="HTTP 504"):
        await fetch(http, REQUEST, delays=(1, 3), sleep=sleep)  # type: ignore[arg-type]
    assert len(sent) == 3


@pytest.mark.parametrize("status", [404, 429])
async def test_does_not_retry_client_errors(status: int) -> None:
    http, sent = client([httpx.Response(status)])
    sleep, waits = no_sleep()
    with pytest.raises(FetchError, match=f"HTTP {status}"):
        await fetch(http, REQUEST, delays=(1, 3), sleep=sleep)  # type: ignore[arg-type]
    assert waits == []
    assert len(sent) == 1
