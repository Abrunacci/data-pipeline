"""One run of a series: ask its sources in order, check the answer against the control source
and the values before it, and record every attempt."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import datetime

import httpx

from data_pipeline.core.checks import canonical, decide, reading_problem
from data_pipeline.core.readings import Control, Observation, Reading, Rejected, Rejection
from data_pipeline.core.series import Series
from data_pipeline.core.sources import MalformedResponseError, NoQuoteError, Source
from data_pipeline.runner.fetch import FetchError, fetch
from data_pipeline.runner.store import Store

logger = logging.getLogger(__name__)

type Clock = Callable[[], datetime]


async def collect(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    store: Store,
    now: Clock,
) -> list[Observation]:
    """Try the sources of ``series`` in order and stop at the first valid reading, then read
    the control source and decide whether the reading is published or held back.

    Every attempt is recorded, including the failed ones and why they failed, and the control
    reading before the decision it informs. A suspect does not try the fallbacks: the source
    did answer.
    """
    observations: list[Observation] = []

    async def record(observation: Observation) -> None:
        await store.record(observation)
        observations.append(observation)

    for name in series.sources:
        fetched_at, result = await _read(series, sources[name], client, now)
        if isinstance(result, Rejected):
            await record(Observation(series.id, name, fetched_at, result))
            continue

        control: Reading | None = None
        if series.control is not None and series.control != name:
            control_at, checked = await _read(series, sources[series.control], client, now)
            outcome = checked if isinstance(checked, Rejected) else Control(checked)
            await record(Observation(series.id, series.control, control_at, outcome))
            control = None if isinstance(checked, Rejected) else checked

        state = await store.state(series.id, since=fetched_at - series.suspects_expire_after)
        decision = decide(
            result,
            state.last_accepted,
            state.suspects,
            None if control is None else control.value,
            series.rules,
        )
        await record(Observation(series.id, name, fetched_at, decision))
        break
    return observations


async def _read(
    series: Series, source: Source, client: httpx.AsyncClient, now: Clock
) -> tuple[datetime, Reading | Rejected]:
    """Fetch, parse and check one reading. The time is when the answer arrived, or when the
    fetch gave up.

    A bug in the source's own code, building its request or parsing an answer it did not
    foresee, is logged with its traceback and recorded as ``source_bug``: the run goes on, so a
    broken control never stops the reading it checks from being decided and recorded. Anything
    else fetch raises (a closed client) is a bug of the runner: it aborts this series' run,
    which the scheduler logs before the next slot.
    """
    try:
        request = source.request()
    except Exception as error:
        return now(), _source_bug(source, "build its request", error)
    try:
        body = await fetch(client, request)
    except FetchError as error:
        return now(), Rejected(Rejection.FETCH_FAILED, str(error))

    fetched_at = now()
    try:
        reading = source.parse(body, fetched_at)
    except MalformedResponseError as error:
        return fetched_at, Rejected(Rejection.MALFORMED, str(error))
    except NoQuoteError as error:
        return fetched_at, Rejected(Rejection.NO_QUOTE, str(error))
    except Exception as error:
        return fetched_at, _source_bug(source, "parse its answer", error)

    age = series.age(reading.as_of, fetched_at)
    if (problem := reading_problem(reading, series.rules, fetched_at, age)) is not None:
        reason, detail = problem
        return fetched_at, Rejected(reason, detail, reading)
    # Stored and published without trailing zeros: Bitso sends 1615.300000000000.
    return fetched_at, Reading(canonical(reading.value), reading.as_of)


def _source_bug(source: Source, doing: str, error: Exception) -> Rejected:
    logger.exception("%s failed to %s", source.name, doing)
    return Rejected(Rejection.SOURCE_BUG, f"{doing}: {type(error).__name__}: {error}")
