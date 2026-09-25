"""CriptoYa's quotes from exchanges and wallets, the bid of one of them.

Docs: https://docs.criptoya.com/argentina/. Limit: 120 requests per minute; quotes refresh
every 60 s. It publishes no terms of use. Its ``bid`` is the raw price, before the exchange's
fees (``totalBid`` includes them).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import EpochSeconds, JsonNumber, describe, load_json

API_URL = "https://criptoya.com/api"


class _Quote(BaseModel):
    model_config = ConfigDict(strict=True)

    bid: JsonNumber
    time: EpochSeconds


_BY_EXCHANGE = TypeAdapter(dict[str, object])


@dataclass(frozen=True, slots=True)
class CriptoYaBid:
    """``path`` is the quote's path under /api, e.g. ``bitsoalpha/USDT/ARS/1`` (one exchange)
    or ``usdc/ARS/1`` (every exchange, picked by ``exchange``)."""

    name: str
    path: str
    exchange: str | None = None

    def request(self) -> Request:
        return Request("GET", f"{API_URL}/{self.path}")

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        del fetched_at  # the source says when its value is from
        data = load_json(body)
        try:
            if self.exchange is not None:
                by_exchange = _BY_EXCHANGE.validate_python(data)
                if self.exchange not in by_exchange:
                    raise MalformedResponseError(f"no quote for {self.exchange}")
                data = by_exchange[self.exchange]
            quote = _Quote.model_validate(data)
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        return Reading(quote.bid, quote.time)
