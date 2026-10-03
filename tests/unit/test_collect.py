from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest

from data_pipeline.core.checks import Range, Rules
from data_pipeline.core.readings import Accepted, Observation, Reading, Rejected, Rejection
from data_pipeline.core.schedule import OpeningHours
from data_pipeline.core.series import Series
from data_pipeline.core.sources import MalformedResponseError, Request, Source
from data_pipeline.runner.collect import collect
from data_pipeline.runner.scheduler import run_forever, run_slot, slot_start

pytestmark = pytest.mark.anyio

NOW = datetime(2026, 9, 25, 18, 3, tzinfo=UTC)
SERIES = Series(
    id="rate",
    description="test",
    sources=("primary", "fallback"),
    every=timedelta(minutes=10),
    rules=Rules(plausible=Range(Decimal(500), Decimal(50_000)), max_age=timedelta(minutes=30)),
)


@dataclass(frozen=True)
class FakeSource:
    """Answers at its own URL with a body that is the value, or "bad" for a malformed one."""

    name: str

    def request(self) -> Request:
        return Request("GET", f"https://{self.name}.example/")

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        if body == b"bad":
            raise MalformedResponseError("bad body")
        return Reading(Decimal(body.decode()), fetched_at)


SOURCES = {name: FakeSource(name) for name in ("primary", "fallback")}


def http(answers: dict[str, list[httpx.Response]]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return answers[request.url.host.split(".")[0]].pop(0)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# Only 200 and 404 answers: a 404 is not retried, so no test waits between retries.
def ok(value: str) -> httpx.Response:
    return httpx.Response(200, content=value.encode())


async def run(
    answers: dict[str, list[httpx.Response]], series: Series = SERIES, now: datetime = NOW
) -> list[object]:
    observations = await collect(series, SOURCES, http(answers), lambda: now)
    return [o.outcome for o in observations]


async def test_the_primary_answer_is_used_and_the_fallback_not_asked() -> None:
    outcomes = await run({"primary": [ok("1600.500")], "fallback": []})
    # Sent without trailing zeros.
    assert outcomes == [Accepted(Reading(Decimal("1600.5"), NOW))]


async def test_a_failing_source_is_rejected_and_the_fallback_used() -> None:
    outcomes = await run({"primary": [httpx.Response(404)], "fallback": [ok("1600")]})
    assert outcomes == [
        Rejected(Rejection.FETCH_FAILED, "HTTP 404"),
        Accepted(Reading(Decimal(1600), NOW)),
    ]


async def test_malformed_and_implausible_answers_are_rejected_with_the_reason() -> None:
    outcomes = await run({"primary": [ok("bad")], "fallback": [ok("16.00")]})
    assert outcomes[0] == Rejected(Rejection.MALFORMED, "bad body")
    assert isinstance(outcomes[1], Rejected)
    assert outcomes[1].reason is Rejection.IMPLAUSIBLE
    assert outcomes[1].reading == Reading(Decimal("16.00"), NOW)


async def test_a_reading_is_not_compared_with_the_one_before() -> None:
    # No state between runs: a 20 % move inside the plausible range is accepted at once.
    assert await run({"primary": [ok("1600")]}) == [Accepted(Reading(Decimal(1600), NOW))]
    assert await run({"primary": [ok("1920")]}) == [Accepted(Reading(Decimal(1920), NOW))]


async def test_every_attempt_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO", logger="data_pipeline.runner.log"):
        await run({"primary": [httpx.Response(404)], "fallback": [ok("1600.50")]})
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("WARNING", "rate primary rejected, fetch_failed: HTTP 404"),
        ("INFO", "rate fallback accepted 1600.5"),
    ]


MEP_HOURS = OpeningHours(
    frozenset(range(5)), time(10, 45), time(17, 30), ZoneInfo("America/Argentina/Buenos_Aires")
)
FRIDAY_1729 = datetime(2026, 9, 25, 20, 29, tzinfo=UTC)  # 17:29 in Buenos Aires


@dataclass(frozen=True)
class OldSource:
    """Always answers with a value from Friday 17:29, like a MEP source over a weekend."""

    name: str = "primary"

    def request(self) -> Request:
        return Request("GET", "https://primary.example/")

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        return Reading(Decimal(body.decode()), FRIDAY_1729)


@pytest.mark.parametrize(
    ("now", "fresh"),
    [
        # Monday 10:45: one open minute since Friday 17:29.
        (datetime(2026, 9, 28, 13, 45, tzinfo=UTC), True),
        # Monday 11:44: 1 + 59 = 60 open minutes, the limit.
        (datetime(2026, 9, 28, 14, 44, tzinfo=UTC), True),
        # Monday 11:45: 61.
        (datetime(2026, 9, 28, 14, 45, tzinfo=UTC), False),
        # Tuesday 10:45: 1 + 405 minutes of Monday.
        (datetime(2026, 9, 29, 13, 45, tzinfo=UTC), False),
    ],
)
async def test_a_value_ages_only_while_its_market_is_open(now: datetime, fresh: bool) -> None:
    rules = replace(SERIES.rules, max_age=timedelta(minutes=60))
    series = replace(SERIES, sources=("primary",), hours=MEP_HOURS, rules=rules)
    observations = await collect(
        series, {"primary": OldSource()}, http({"primary": [ok("1550")]}), lambda: now
    )
    [outcome] = [o.outcome for o in observations]
    if fresh:
        assert outcome == Accepted(Reading(Decimal(1550), FRIDAY_1729))
    else:
        assert isinstance(outcome, Rejected)
        assert outcome.reason is Rejection.STALE


class Recorder:
    def __init__(self) -> None:
        self.sent: list[Observation] = []

    async def send(self, series: Series, observation: Observation) -> None:
        self.sent.append(observation)


class TestSlots:
    def test_slots_are_aligned_to_the_clock(self) -> None:
        assert slot_start(NOW, timedelta(minutes=10)) == datetime(2026, 9, 25, 18, 0, tzinfo=UTC)
        start = datetime(2026, 9, 25, 18, 10, tzinfo=UTC)
        assert slot_start(start, timedelta(minutes=10)) == start

    async def test_a_series_does_not_run_outside_its_hours(self) -> None:
        series = replace(SERIES, hours=MEP_HOURS)
        saturday = datetime(2026, 9, 26, 15, 0, tzinfo=UTC)
        assert not await run_slot(series, SOURCES, http({}), lambda: saturday)
        friday_close = datetime(2026, 9, 25, 20, 30, tzinfo=UTC)  # 17:30 in Buenos Aires
        client = http({"primary": [ok("1600")]})
        assert await run_slot(series, SOURCES, client, lambda: friday_close)

    async def test_only_an_accepted_reading_is_sent(self) -> None:
        destination = Recorder()
        client = http({"primary": [httpx.Response(404)], "fallback": [ok("1600")]})
        assert await run_slot(SERIES, SOURCES, client, lambda: NOW, destination)
        assert [(o.source, o.outcome) for o in destination.sent] == [
            ("fallback", Accepted(Reading(Decimal(1600), NOW)))
        ]

    async def test_nothing_is_sent_when_every_source_fails(self) -> None:
        destination = Recorder()
        client = http({"primary": [httpx.Response(404)], "fallback": [ok("bad")]})
        assert await run_slot(SERIES, SOURCES, client, lambda: NOW, destination)
        assert destination.sent == []

    async def test_a_failing_destination_drops_the_value_and_keeps_the_run(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        class Broken:
            async def send(self, series: Series, observation: Observation) -> None:
                raise RuntimeError("unreachable")

        client = http({"primary": [ok("1600")]})
        assert await run_slot(SERIES, SOURCES, client, lambda: NOW, Broken())
        assert "sending rate failed; the value is dropped" in caplog.messages

    async def test_every_slot_runs_and_a_failing_run_does_not_stop_the_schedule(self) -> None:
        waits: list[float] = []

        async def sleep(seconds: float) -> None:
            waits.append(seconds)
            if len(waits) == 2:
                raise asyncio.CancelledError

        # A closed client makes every run fail outside the sources.
        client = http({})
        await client.aclose()
        with pytest.raises(asyncio.CancelledError):
            await run_forever(SERIES, SOURCES, client, lambda: NOW, sleep)
        # Both runs failed and each one waited for the next slot, at 18:10.
        assert waits == [420.0, 420.0]


async def test_a_bug_outside_the_parser_stops_the_run() -> None:
    # A closed client is not a source failing: it must not be logged as a bad answer.
    client = http({})
    await client.aclose()
    with pytest.raises(RuntimeError):
        await collect(SERIES, SOURCES, client, lambda: NOW)


async def test_a_source_that_cannot_build_its_request_is_a_source_bug() -> None:
    @dataclass(frozen=True)
    class NoRequest:
        name: str = "primary"

        def request(self) -> Request:
            raise KeyError("missing setting")

        def parse(self, body: bytes, fetched_at: datetime) -> Reading:
            raise AssertionError("never called")

    sources: dict[str, Source] = {**SOURCES, "primary": NoRequest()}
    observations = await collect(SERIES, sources, http({"fallback": [ok("1600")]}), lambda: NOW)
    assert [o.outcome for o in observations] == [
        Rejected(Rejection.SOURCE_BUG, "build its request: KeyError: 'missing setting'"),
        Accepted(Reading(Decimal(1600), NOW)),
    ]


async def test_a_failed_fetch_is_stamped_when_it_gave_up() -> None:
    # Each fetch takes a minute: the stamp must be taken after it, not before.
    clock = [NOW]

    def slow_404(request: httpx.Request) -> httpx.Response:
        clock[0] += timedelta(minutes=1)
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(slow_404))
    observations = await collect(SERIES, SOURCES, client, lambda: clock[0])
    assert [o.fetched_at for o in observations] == [
        NOW + timedelta(minutes=1),
        NOW + timedelta(minutes=2),
    ]
