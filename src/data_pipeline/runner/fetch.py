"""Sending a source's request, with a timeout and a couple of retries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence

import httpx

from data_pipeline.core.sources import Request

# Waits before each retry. Two retries keep a run well inside its interval and far from any
# source's rate limit.
RETRY_DELAYS: Sequence[float] = (1.0, 3.0)


class FetchError(Exception):
    """No usable answer: the network failed, or the source answered with an HTTP error."""


async def fetch(
    client: httpx.AsyncClient,
    request: Request,
    *,
    delays: Sequence[float] = RETRY_DELAYS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> bytes:
    """The body of a 200 answer to ``request``.

    Network errors and 5xx answers are retried. A 4xx is not: asking again gets the same
    answer, and a 429 means the source wants fewer requests, not more.
    """
    for delay in (*delays, None):
        try:
            response = await client.request(
                request.method,
                request.url,
                json=request.json,
                headers=dict(request.headers),
            )
        except httpx.HTTPError as error:
            problem = f"{type(error).__name__}: {error}"
        else:
            if response.status_code == httpx.codes.OK:
                return response.content
            problem = f"HTTP {response.status_code}"
            if response.is_client_error:
                raise FetchError(problem)
        if delay is None:
            raise FetchError(problem)
        await sleep(delay)
    raise AssertionError("unreachable")
