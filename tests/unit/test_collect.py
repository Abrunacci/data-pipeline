from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest

from data_pipeline.core.checks import Range, Rules
from data_pipeline.core.readings import Accepted, Control, Reading, Rejected, Rejection, Suspect
from data_pipeline.core.schedule import OpeningHours
from data_pipeline.core.series import Series
from data_pipeline.core.sources import MalformedResponseError, Request, Source
from data_pipeline.runner.collect import collect
from data_pipeline.runner.scheduler import run_forever, run_slot, slot_start
from tests.fakes import MemoryStore

pytestmark = pytest.mark.anyio

NOW = datetime(2026, 9, 25, 18, 3, tzinfo=UTC)
SERIES = Series(
    id="rate",
    description="test",
    sources=("primary", "fallback"),
    every=timedelta(minutes=10),
    rules=Rules(
        plausible=Range(Decimal(500), Decimal(50_000)),
        max_age=timedelta(minutes=30),
        max_jump=Decimal("0.05"),
    ),
)
CONTROLLED = replace(SERIES, control="control")


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


SOURCES = {name: FakeSource(name) for name in ("primary", "fallback", "control")}


def http(answers: dict[str, list[httpx.Response]]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return answers[request.url.host.split(".")[0]].pop(0)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# Only 200 and 404 answers: a 404 is not retried, so no test waits between retries.
def ok(value: str) -> httpx.Response:
    return httpx.Response(200, content=value.encode())


async def run(
    store: MemoryStore,
    answers: dict[str, list[httpx.Response]],
    series: Series = SERIES,
    now: datetime = NOW,
) -> list[object]:
    observations = await collect(series, SOURCES, http(answers), store, lambda: now)
    return [o.outcome for o in observations]


async def test_the_primary_answer_is_used_and_the_fallback_not_asked() -> None:
    store = MemoryStore()
    outcomes = await run(store, {"primary": [ok("1600.500")], "fallback": []})
    # Stored without trailing zeros.
    assert outcomes == [Accepted(Reading(Decimal("1600.5"), NOW))]
    assert [o.source for o in store.observations] == ["primary"]


async def test_a_failing_source_is_recorded_and_the_fallback_used() -> None:
    store = MemoryStore()
    outcomes = await run(store, {"primary": [httpx.Response(404)], "fallback": [ok("1600")]})
    assert outcomes == [
        Rejected(Rejection.FETCH_FAILED, "HTTP 404"),
        Accepted(Reading(Decimal(1600), NOW)),
    ]
    assert [o.source for o in store.observations] == ["primary", "fallback"]


async def test_malformed_and_implausible_answers_are_rejected_with_the_reason() -> None:
    store = MemoryStore()
    outcomes = await run(store, {"primary": [ok("bad")], "fallback": [ok("16.00")]})
    assert outcomes[0] == Rejected(Rejection.MALFORMED, "bad body")
    assert isinstance(outcomes[1], Rejected)
    assert outcomes[1].reason is Rejection.IMPLAUSIBLE
    assert outcomes[1].reading == Reading(Decimal("16.00"), NOW)
    assert (await store.latest("rate")).published is None


async def test_a_jump_is_held_back_until_two_more_readings_move_the_same_way() -> None:
    store = MemoryStore()
    await run(store, {"primary": [ok("1600")]})
    for value in ("1800", "1850"):
        [outcome] = await run(store, {"primary": [ok(value)]})
        assert isinstance(outcome, Suspect)
        latest = await store.latest("rate")
        assert latest.published is not None
        assert latest.published.value == Decimal(1600)
        assert latest.suspect_at == NOW

    [outcome] = await run(store, {"primary": [ok("1900")]})
    assert isinstance(outcome, Accepted)
    assert outcome.confirmed
    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal(1900)
    assert latest.suspect_at is None


@pytest.mark.parametrize(
    ("oldest_age", "confirmed"),
    [(timedelta(minutes=30), True), (timedelta(minutes=30, seconds=1), False)],
)
async def test_suspects_expire_after_three_intervals(
    oldest_age: timedelta, confirmed: bool
) -> None:
    # Two suspects, 10 minutes and oldest_age old: the reading confirms only if both are alive,
    # and a suspect is alive while it is at most 3 intervals (30 minutes) old.
    store = MemoryStore()
    await run(store, {"primary": [ok("1600")]}, now=NOW - timedelta(hours=1))
    await run(store, {"primary": [ok("1800")]}, now=NOW - oldest_age)
    await run(store, {"primary": [ok("1800")]}, now=NOW - timedelta(minutes=10))
    [outcome] = await run(store, {"primary": [ok("1800")]})
    assert isinstance(outcome, Accepted) is confirmed


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
    store = MemoryStore()
    observations = await collect(
        series, {"primary": OldSource()}, http({"primary": [ok("1550")]}), store, lambda: now
    )
    [outcome] = [o.outcome for o in observations]
    if fresh:
        assert outcome == Accepted(Reading(Decimal(1550), FRIDAY_1729))
    else:
        assert isinstance(outcome, Rejected)
        assert outcome.reason is Rejection.STALE


class TestControl:
    async def test_the_control_is_read_and_recorded_before_the_decision(self) -> None:
        store = MemoryStore()
        outcomes = await run(store, {"primary": [ok("1600")], "control": [ok("1601")]}, CONTROLLED)
        assert outcomes == [
            Control(Reading(Decimal(1601), NOW)),
            Accepted(Reading(Decimal(1600), NOW)),
        ]
        assert [o.source for o in store.observations] == ["control", "primary"]

    async def test_an_agreeing_control_confirms_a_jump_at_once(self) -> None:
        store = MemoryStore()
        await run(store, {"primary": [ok("1600")], "control": [ok("1600")]}, CONTROLLED)
        outcomes = await run(store, {"primary": [ok("1800")], "control": [ok("1790")]}, CONTROLLED)
        assert isinstance(outcomes[-1], Accepted)
        assert outcomes[-1].confirmed

    async def test_a_disagreeing_control_holds_the_value_back(self) -> None:
        store = MemoryStore()
        await run(store, {"primary": [ok("1600")], "control": [ok("1600")]}, CONTROLLED)
        outcomes = await run(store, {"primary": [ok("1650")], "control": [ok("1600")]}, CONTROLLED)
        assert isinstance(outcomes[-1], Suspect)

    async def test_a_failing_control_holds_nothing_back(self) -> None:
        store = MemoryStore()
        await run(store, {"primary": [ok("1600")], "control": [ok("1600")]}, CONTROLLED)
        outcomes = await run(
            store, {"primary": [ok("1650")], "control": [httpx.Response(404)]}, CONTROLLED
        )
        assert outcomes == [
            Rejected(Rejection.FETCH_FAILED, "HTTP 404"),
            Accepted(Reading(Decimal(1650), NOW)),
        ]

    async def test_a_control_that_breaks_its_parser_holds_nothing_back(self) -> None:
        @dataclass(frozen=True)
        class Broken:
            name: str = "control"

            def request(self) -> Request:
                return Request("GET", "https://control.example/")

            def parse(self, body: bytes, fetched_at: datetime) -> Reading:
                raise OverflowError("a bug")

        store = MemoryStore()
        sources: dict[str, Source] = {**SOURCES, "control": Broken()}
        answers = http({"primary": [ok("1600")], "control": [ok("x")]})
        observations = await collect(CONTROLLED, sources, answers, store, lambda: NOW)
        assert [o.outcome for o in observations] == [
            Rejected(Rejection.SOURCE_BUG, "parse its answer: OverflowError: a bug"),
            Accepted(Reading(Decimal(1600), NOW)),
        ]

    async def test_a_fallback_that_is_the_control_is_not_checked_against_itself(self) -> None:
        series = replace(SERIES, control="fallback")
        store = MemoryStore()
        outcomes = await run(
            store, {"primary": [httpx.Response(404)], "fallback": [ok("1600")]}, series
        )
        assert [type(o) for o in outcomes] == [Rejected, Accepted]


async def test_a_suspect_does_not_try_the_fallback() -> None:
    store = MemoryStore()
    await run(store, {"primary": [ok("1600")]})
    outcomes = await run(store, {"primary": [ok("1800")], "fallback": []})
    assert len(outcomes) == 1


class TestSlots:
    def test_slots_are_aligned_to_the_clock(self) -> None:
        assert slot_start(NOW, timedelta(minutes=10)) == datetime(2026, 9, 25, 18, 0, tzinfo=UTC)
        start = datetime(2026, 9, 25, 18, 10, tzinfo=UTC)
        assert slot_start(start, timedelta(minutes=10)) == start

    async def test_a_slot_runs_once(self) -> None:
        store = MemoryStore()
        client = http({"primary": [ok("1600")]})
        assert await run_slot(SERIES, SOURCES, client, store, lambda: NOW)
        assert not await run_slot(SERIES, SOURCES, client, store, lambda: NOW)
        assert len(store.observations) == 1

    async def test_a_failed_slot_is_not_retried_until_the_next_one(self) -> None:
        store = MemoryStore()
        client = http({"primary": [httpx.Response(404)], "fallback": [httpx.Response(404)]})
        assert await run_slot(SERIES, SOURCES, client, store, lambda: NOW)
        assert not await run_slot(SERIES, SOURCES, client, store, lambda: NOW)

    async def test_a_series_does_not_run_outside_its_hours(self) -> None:
        hours = OpeningHours(
            frozenset(range(5)),
            time(10, 45),
            time(17, 30),
            ZoneInfo("America/Argentina/Buenos_Aires"),
        )
        series = replace(SERIES, hours=hours)
        store = MemoryStore()
        saturday = datetime(2026, 9, 26, 15, 0, tzinfo=UTC)
        assert not await run_slot(series, SOURCES, http({}), store, lambda: saturday)
        friday_close = datetime(2026, 9, 25, 20, 30, tzinfo=UTC)  # 17:30 in Buenos Aires
        client = http({"primary": [ok("1600")]})
        assert await run_slot(series, SOURCES, client, store, lambda: friday_close)

    async def test_a_series_another_process_is_running_is_skipped(self) -> None:
        store = MemoryStore()
        store.busy.add("rate")
        assert not await run_slot(SERIES, SOURCES, http({}), store, lambda: NOW)
        assert store.observations == []

    async def test_a_failing_run_does_not_stop_the_schedule(self) -> None:
        class BrokenStore(MemoryStore):
            async def record(self, observation: object) -> None:
                raise RuntimeError("database down")

        waits: list[float] = []

        async def sleep(seconds: float) -> None:
            waits.append(seconds)
            if len(waits) == 2:
                raise asyncio.CancelledError

        client = http({"primary": [ok("1600"), ok("1600")]})
        with pytest.raises(asyncio.CancelledError):
            await run_forever(SERIES, SOURCES, client, BrokenStore(), lambda: NOW, sleep)
        # Both runs failed and each one waited for the next slot, at 18:10.
        assert waits == [420.0, 420.0]


async def test_a_bug_outside_the_parser_stops_the_run() -> None:
    # A closed client is not a source failing: it must not be recorded as a bad answer.
    client = http({})
    await client.aclose()
    with pytest.raises(RuntimeError):
        await collect(SERIES, SOURCES, client, MemoryStore(), lambda: NOW)


async def test_a_source_that_cannot_build_its_request_is_a_source_bug() -> None:
    @dataclass(frozen=True)
    class NoRequest:
        name: str = "control"

        def request(self) -> Request:
            raise KeyError("missing setting")

        def parse(self, body: bytes, fetched_at: datetime) -> Reading:
            raise AssertionError("never called")

    sources: dict[str, Source] = {**SOURCES, "control": NoRequest()}
    observations = await collect(
        CONTROLLED, sources, http({"primary": [ok("1600")]}), MemoryStore(), lambda: NOW
    )
    assert [o.outcome for o in observations] == [
        Rejected(Rejection.SOURCE_BUG, "build its request: KeyError: 'missing setting'"),
        Accepted(Reading(Decimal(1600), NOW)),
    ]


async def test_a_failed_fetch_is_stamped_when_it_gave_up() -> None:
    # A clock that moves a minute each time it is read: the stamp must be taken after the
    # fetch, not before it.
    ticks = iter(NOW + timedelta(minutes=n) for n in range(10))
    store = MemoryStore()
    client = http({"primary": [httpx.Response(404)], "fallback": [httpx.Response(404)]})
    observations = await collect(SERIES, SOURCES, client, store, lambda: next(ticks))
    assert [o.fetched_at for o in observations] == [NOW, NOW + timedelta(minutes=1)]
