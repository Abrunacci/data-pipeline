"""The runner: ``python -m data_pipeline.runner``. It runs every series on its schedule, keeps
what the checks need in memory, and hands each accepted value to cuanto-cuesta's destination. It
has no database and serves no HTTP; Docker's ``HEALTHCHECK`` reads its heartbeat.

The destination posts to cuanto-cuesta when ``CUANTO_CUESTA_INGEST_URL`` and
``CUANTO_CUESTA_INGEST_TOKEN`` are both set, and only logs each batch otherwise; the start says
which, once.

``--check`` loads the series file and builds the destination, then exits: a configuration that
would not start fails there, without network.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Mapping, Sequence

import httpx

from data_pipeline.config import RunnerSettings, load_series
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source
from data_pipeline.destinations.cuanto_cuesta import CuantoCuestaIngest, CuantoCuestaLog
from data_pipeline.runner.destination import Destination
from data_pipeline.runner.fetch import CLIENT_TIMEOUT
from data_pipeline.runner.heartbeat import beat_forever
from data_pipeline.runner.memory import MemoryState
from data_pipeline.runner.scheduler import run_forever
from data_pipeline.sources import available_history_sources, available_sources

logger = logging.getLogger("data_pipeline.runner")


def configure(
    settings: RunnerSettings, client: httpx.AsyncClient
) -> tuple[tuple[Series, ...], Mapping[str, Source], Destination]:
    sources = available_sources()
    # The history sources only load past values for the API; the runner keeps no history, but
    # the series file still names them.
    series = load_series(settings.series_file, sources, available_history_sources())
    return series, sources, pick_destination(settings, series, sources, client)


def pick_destination(
    settings: RunnerSettings,
    series: Sequence[Series],
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
) -> Destination:
    """Posting to cuanto-cuesta when its URL and token are both set; logging otherwise. Either
    way the start says which, so the log shows whether values are being sent."""
    url, token = settings.cuanto_cuesta_ingest_url, settings.cuanto_cuesta_ingest_token
    if url is None or token is None:
        missing = [
            name
            for name, value in (
                ("CUANTO_CUESTA_INGEST_URL", url),
                ("CUANTO_CUESTA_INGEST_TOKEN", token),
            )
            if value is None
        ]
        logger.warning(
            "%s not set: cuanto-cuesta batches are logged, not sent", " and ".join(missing)
        )
        return CuantoCuestaLog(series, sources)
    # The URL only: the token never goes to the log.
    logger.info("sending cuanto-cuesta batches to %s", url)
    return CuantoCuestaIngest(series, sources, client, url, token.get_secret_value())


def http_client(settings: RunnerSettings) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=CLIENT_TIMEOUT, headers={"User-Agent": settings.user_agent})


async def run(settings: RunnerSettings) -> None:
    """Run until SIGTERM or SIGINT. Stopping drops nothing: there is no queue to flush."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    state = MemoryState()
    async with http_client(settings) as client:
        series, sources, destination = configure(settings, client)
        tasks = [asyncio.create_task(beat_forever(), name="heartbeat")]
        tasks += [
            asyncio.create_task(
                run_forever(s, sources, client, state, destination=destination),
                name=f"series:{s.id}",
            )
            for s in series
        ]
        logger.info("running %s", ", ".join(s.id for s in series))
        await stop.wait()
        logger.info("stopping")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m data_pipeline.runner")
    parser.add_argument("--check", action="store_true", help="check the configuration and exit")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = RunnerSettings()
    if args.check:
        asyncio.run(check(settings))
        return 0
    asyncio.run(run(settings))
    return 0


async def check(settings: RunnerSettings) -> None:
    async with http_client(settings) as client:
        series, _, _ = configure(settings, client)
    logger.info("configuration ok: %s", ", ".join(s.id for s in series))


if __name__ == "__main__":
    raise SystemExit(main())
