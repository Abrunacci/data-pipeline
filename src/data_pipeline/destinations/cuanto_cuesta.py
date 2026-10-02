"""cuanto-cuesta, the calculator: the rates it takes and the batch its ingest API accepts.

The contract is cuanto-cuesta's (``backend/README.md``, "API", ``POST /api/ingest``):

- one item per rate, with the rate's fixed ``base`` and ``quote``: an inverted pair is rejected;
- amounts as decimal strings, ``observed_at`` in RFC 3339 with a zone, ``source`` as
  ``[a-z0-9_]``, ``source_url`` as ``https`` or null, and no other field;
- the card rate always carries ``estimated_final``, null when there is no estimate (an item
  without it is rejected); no other rate carries it;
- at most 20 items and 64 KB per batch.

``CuantoCuestaIngest`` posts each batch to that endpoint and logs what cuanto-cuesta did with
every item. ``CuantoCuestaLog`` builds the same batch and only logs it: the runner uses it while
the endpoint's URL or token is not configured.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx

from data_pipeline.core.gap import publishable_estimate
from data_pipeline.core.readings import Accepted, Observation, Reading
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source

logger = logging.getLogger(__name__)

MAX_ITEMS = 20
MAX_BYTES = 64 * 1024
# The client's timeout applies to each read; this bounds the whole send. cuanto-cuesta is on the
# same host, so a send that takes this long means it is down, and the value is dropped.
SEND_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class Rate:
    """One unit of ``base`` is ``price`` of ``quote``."""

    base: str
    quote: str
    estimated_final: bool = False


# cuanto-cuesta's backend/config/rates.yaml. The keys are the series ids.
RATES: Mapping[str, Rate] = {
    "mep": Rate("USD", "ARS"),
    "binance_p2p_usdt_usd": Rate("USDT", "USD"),
    "bitso_usdt_ars": Rate("USDT", "ARS"),
    "arq_usd_ars": Rate("USD", "ARS"),
    "binance_card_usd_usdt": Rate("USD", "USDT", estimated_final=True),
}


def rate_item(
    series: Series, source: str, source_url: str | None, reading: Reading
) -> dict[str, object]:
    """The item for an accepted ``reading`` of ``series`` from ``source``."""
    rate = RATES[series.id]
    item: dict[str, object] = {
        "key": series.id,
        "base": rate.base,
        "quote": rate.quote,
        "price": format(reading.value, "f"),
    }
    if rate.estimated_final:
        item["estimated_final"] = _estimate(series, reading)
    item["source"] = source
    item["source_url"] = source_url
    item["observed_at"] = rfc3339(reading.as_of)
    return item


def batch(items: Sequence[dict[str, object]], batch_id: UUID) -> dict[str, object]:
    if not 0 < len(items) <= MAX_ITEMS:
        raise ValueError(f"a batch has 1 to {MAX_ITEMS} items, got {len(items)}")
    return {"batch_id": str(batch_id), "rates": list(items)}


def rfc3339(moment: datetime) -> str:
    """In UTC, with a Z: 2026-10-01T15:00:00Z."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _estimate(series: Series, reading: Reading) -> str | None:
    """The gap's estimate, or None without a gap or when cuanto-cuesta would refuse it: it
    checks the estimate against the same range as the price, and an item with an estimate out of
    range is rejected whole."""
    if series.gap is None:
        return None
    estimate = publishable_estimate(reading.value, series.gap)
    if estimate is None or estimate not in series.rules.plausible:
        return None
    return format(estimate, "f")


def _source_url(source: Source) -> str | None:
    url = source.request().url
    return url if url.startswith("https://") else None


class _Batches:
    """Builds the batch for each accepted reading, one item per batch: a series run gives one
    value, and sending it at once keeps its ``observed_at`` fresh in cuanto-cuesta."""

    def __init__(
        self,
        series: Iterable[Series],
        sources: Mapping[str, Source],
        new_id: Callable[[], UUID] = uuid4,
    ) -> None:
        # At startup, not at the first value: a series cuanto-cuesta does not know is a bug.
        if unknown := sorted(s.id for s in series if s.id not in RATES):
            raise ValueError(f"cuanto-cuesta takes no rate for the series {unknown}")
        self._sources = sources
        self._new_id = new_id

    def payload(self, series: Series, observation: Observation) -> dict[str, object]:
        if not isinstance(observation.outcome, Accepted):
            raise ValueError(f"only accepted readings are sent, got {observation.outcome}")
        source_url = _source_url(self._sources[observation.source])
        item = rate_item(series, observation.source, source_url, observation.outcome.reading)
        return batch([item], self._new_id())


class CuantoCuestaLog(_Batches):
    """Logs the batch each accepted reading would be sent in, as one JSON line. It sends
    nothing."""

    async def send(self, series: Series, observation: Observation) -> None:
        payload = self.payload(series, observation)
        logger.info("cuanto-cuesta batch, not sent: %s", _json(payload).decode())


class CuantoCuestaIngest(_Batches):
    """Posts each batch to cuanto-cuesta's ``POST /api/ingest`` and logs the outcome.

    Nothing is retried and nothing raises: a value that does not get through is logged and
    dropped, and the next run sends a newer one. The log tells three cases apart:

    - cuanto-cuesta answered 200: one line per item with its status, ``stored``, ``unchanged``,
      ``older``, or ``rejected`` with its error code;
    - it refused the whole batch: ``401`` is a wrong or missing token, a configuration problem
      no later run fixes, so its line says ``CONFIGURATION``; ``400``, ``413`` and ``422`` are a
      batch this runner built wrong;
    - it did not answer, or answered outside its contract (a 5xx, a 404 from a wrong URL).
    """

    def __init__(
        self,
        series: Iterable[Series],
        sources: Mapping[str, Source],
        client: httpx.AsyncClient,
        url: str,
        token: str,
        new_id: Callable[[], UUID] = uuid4,
        send_timeout: float = SEND_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(series, sources, new_id)
        self._client = client
        self._url = url
        self._headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self._send_timeout = send_timeout

    async def send(self, series: Series, observation: Observation) -> None:
        payload = self.payload(series, observation)
        body = _json(payload)
        what = f"cuanto-cuesta batch {payload['batch_id']} ({series.id})"
        if len(body) > MAX_BYTES:
            # cuanto-cuesta would answer 413. One item is far under it, so this is a bug here.
            logger.error("%s not sent: %d bytes, over %d", what, len(body), MAX_BYTES)
            return
        try:
            async with asyncio.timeout(self._send_timeout):
                response = await self._client.post(self._url, content=body, headers=self._headers)
        except TimeoutError:
            logger.error("%s dropped: no answer in %g s", what, self._send_timeout)
            return
        except httpx.HTTPError as error:
            logger.error("%s dropped: no answer: %s: %s", what, type(error).__name__, error)
            return
        _log_response(what, response)


def _log_response(what: str, response: httpx.Response) -> None:
    status = response.status_code
    if status == httpx.codes.OK:
        if (results := _results(response)) is None:
            logger.error("%s: answered 200 with an unexpected body: %.200s", what, response.text)
            return
        for result in results:
            _log_result(what, result)
        return
    code = _error_code(response)
    if status == httpx.codes.UNAUTHORIZED:
        logger.error(
            "%s dropped: CONFIGURATION: the token was refused (HTTP 401 %s); check that"
            " CUANTO_CUESTA_INGEST_TOKEN holds cuanto-cuesta's INGEST_TOKEN",
            what,
            code,
        )
    elif status in (
        httpx.codes.BAD_REQUEST,
        httpx.codes.REQUEST_ENTITY_TOO_LARGE,
        httpx.codes.UNPROCESSABLE_ENTITY,
    ):
        logger.error("%s dropped: the batch was refused (HTTP %d %s)", what, status, code)
    else:
        logger.error("%s dropped: unexpected answer (HTTP %d %s)", what, status, code)


def _results(response: httpx.Response) -> list[dict[str, object]] | None:
    """The per-item results of a 200, or None when the body is not what the contract says."""
    try:
        body = response.json()
    except ValueError:
        return None
    results = body.get("results") if isinstance(body, dict) else None
    if not isinstance(results, list) or not all(isinstance(r, dict) for r in results):
        return None
    return results


def _log_result(what: str, result: dict[str, object]) -> None:
    key = result.get("key")
    match result.get("status"):
        case "stored" | "unchanged" as status:
            logger.info("%s: %s %s", what, key, status)
        case "older":
            # cuanto-cuesta already holds a quote observed after this one, and keeps it.
            logger.warning("%s: %s older than the current quote, ignored", what, key)
        case "rejected":
            logger.error("%s: %s rejected, %s", what, key, result.get("error"))
        case _:
            logger.error("%s: %s unexpected result %s", what, key, result)


def _error_code(response: httpx.Response) -> str:
    """The ``error`` code of a refused request, as cuanto-cuesta sends it in ``{"error": code}``."""
    try:
        body = response.json()
    except ValueError:
        return "without an error code"
    if isinstance(body, dict) and isinstance(code := body.get("error"), str):
        return code
    return "without an error code"


def _json(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()
