"""Sending a source's request, with a time limit, a size limit and a couple of retries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence

import httpx

from data_pipeline.core.sources import Request

# Waits before each retry. Two retries keep a run well inside its interval and far from any
# source's rate limit.
RETRY_DELAYS: Sequence[float] = (1.0, 3.0)
# The client's timeout applies to each read; this bounds a whole attempt, so a source that
# trickles bytes cannot hold a run.
ATTEMPT_TIMEOUT_SECONDS = 15.0
# Every answer we read is a few kilobytes. Anything this big is not one of them.
MAX_BODY_BYTES = 1_000_000


class FetchError(Exception):
    """No usable answer: the network failed, or the source answered with an HTTP error."""


class _RetryableError(Exception):
    """A failure worth asking again for: the network, or a 5xx."""


async def fetch(
    client: httpx.AsyncClient,
    request: Request,
    *,
    delays: Sequence[float] = RETRY_DELAYS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> bytes:
    """The body of a 200 answer to ``request``.

    Network errors, timeouts and 5xx answers are retried. Any other answer is not: asking
    again gets the same answer, and a 429 means the source wants fewer requests, not more.
    """
    for delay in (*delays, None):
        try:
            async with asyncio.timeout(ATTEMPT_TIMEOUT_SECONDS):
                return await _attempt(client, request)
        except _RetryableError as error:
            problem = str(error)
        except TimeoutError:
            problem = f"no answer in {ATTEMPT_TIMEOUT_SECONDS:g} s"
        if delay is None:
            raise FetchError(problem)
        await sleep(delay)
    raise AssertionError("unreachable")


async def _attempt(client: httpx.AsyncClient, request: Request) -> bytes:
    try:
        async with client.stream(
            request.method, request.url, json=request.json, headers=dict(request.headers)
        ) as response:
            if response.is_server_error:
                raise _RetryableError(f"HTTP {response.status_code}")
            if response.status_code != httpx.codes.OK:
                raise FetchError(f"HTTP {response.status_code}")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_BODY_BYTES:
                    raise FetchError(f"answer larger than {MAX_BODY_BYTES} bytes")
            return bytes(body)
    except httpx.HTTPError as error:
        raise _RetryableError(f"{type(error).__name__}: {error}") from error
