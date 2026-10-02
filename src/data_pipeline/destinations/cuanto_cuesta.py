"""cuanto-cuesta, the calculator: the rates it takes and the batch its ingest API accepts.

The contract is cuanto-cuesta's (``backend/README.md``, "API", ``POST /api/ingest``):

- one item per rate, with the rate's fixed ``base`` and ``quote``: an inverted pair is rejected;
- amounts as decimal strings, ``observed_at`` in RFC 3339 with a zone, ``source`` as
  ``[a-z0-9_]``, ``source_url`` as ``https`` or null, and no other field;
- the card rate always carries ``estimated_final``, null when there is no estimate (an item
  without it is rejected); no other rate carries it;
- at most 20 items and 64 KB per batch.

Until its ingest endpoint is reachable, ``CuantoCuestaLog`` builds the batch and logs it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from data_pipeline.core.gap import publishable_estimate
from data_pipeline.core.readings import Accepted, Observation, Reading
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source

logger = logging.getLogger(__name__)

MAX_ITEMS = 20


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


class CuantoCuestaLog:
    """Builds the batch each accepted reading would be sent in, one item per batch, and logs
    it as one JSON line. It sends nothing."""

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

    async def send(self, series: Series, observation: Observation) -> None:
        payload = self.payload(series, observation)
        logger.info("cuanto-cuesta batch, not sent: %s", json.dumps(payload, separators=(",", ":")))
