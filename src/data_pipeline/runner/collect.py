"""One run of a series: ask its sources in order, check what they say and record every attempt."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime

import httpx

from data_pipeline.core.checks import canonical, decide, reading_problem
from data_pipeline.core.readings import Observation, Reading, Rejected, Rejection
from data_pipeline.core.series import Series
from data_pipeline.core.sources import MalformedResponseError, Source
from data_pipeline.runner.fetch import FetchError, fetch
from data_pipeline.runner.store import Store

type Clock = Callable[[], datetime]


async def collect(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    store: Store,
    now: Clock,
) -> list[Observation]:
    """Try the sources of ``series`` in order and stop at the first valid reading.

    Every attempt is recorded, including the failed ones and why they failed. A valid reading
    is recorded as accepted or as a suspect (see ``core.checks.decide``); either way the
    fallbacks are not asked, because the source did answer.
    """
    observations: list[Observation] = []
    for name in series.sources:
        observation = await _attempt(series, sources[name], client, store, now)
        await store.record(observation)
        observations.append(observation)
        if not isinstance(observation.outcome, Rejected):
            break
    return observations


async def _attempt(
    series: Series, source: Source, client: httpx.AsyncClient, store: Store, now: Clock
) -> Observation:
    def observed(outcome: Rejected) -> Observation:
        return Observation(series.id, source.name, fetched_at, outcome)

    fetched_at = now()
    try:
        body = await fetch(client, source.request())
        fetched_at = now()
        reading = source.parse(body)
    except FetchError as error:
        return observed(Rejected(Rejection.FETCH_FAILED, str(error)))
    except MalformedResponseError as error:
        return observed(Rejected(Rejection.MALFORMED, str(error)))

    if (problem := reading_problem(reading, series.rules, fetched_at)) is not None:
        reason, detail = problem
        return observed(Rejected(reason, detail, reading))

    # Stored and published without trailing zeros: Bitso sends 1615.300000000000.
    reading = Reading(canonical(reading.value), reading.as_of)
    state = await store.state(series.id)
    outcome = decide(reading, state.last_accepted, state.suspects, series.rules)
    return Observation(series.id, source.name, fetched_at, outcome)
