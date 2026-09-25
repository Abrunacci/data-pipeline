"""ARQ's (formerly DolarApp) own ticker: what ARQ pays in ARS for each USDc, the bid.

Not documented and no published terms: it is the endpoint ARQ's web calculator uses, and it can
change without notice, so series that use it are marked as not from an official source. Its
``date`` has no time zone; it is UTC (a call at 18:03:40 UTC on 2026-09-25 said 18:03:40).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import DecimalText, describe, load_json

TICKERS_URL = "https://api.arqfinance.com/v1/tickers?currencies=ARS"


class _Ticker(BaseModel):
    model_config = ConfigDict(strict=True)

    book: str
    bid: DecimalText
    date: str


_TICKERS = TypeAdapter(list[_Ticker])


@dataclass(frozen=True, slots=True)
class ArqBid:
    book: str = "usdc_ars"

    @property
    def name(self) -> str:
        return f"arq_{self.book}_bid"

    def request(self) -> Request:
        return Request("GET", TICKERS_URL)

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        del fetched_at  # the source says when its value is from
        try:
            tickers = _TICKERS.validate_python(load_json(body))
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        matching = [ticker for ticker in tickers if ticker.book == self.book]
        if len(matching) != 1:
            raise MalformedResponseError(f"expected one {self.book} ticker, got {len(matching)}")
        (ticker,) = matching
        try:
            # Nanoseconds: fromisoformat keeps the first six digits.
            moment = datetime.fromisoformat(ticker.date)
        except ValueError as error:
            raise MalformedResponseError(f"date is not ISO 8601: {ticker.date!r}") from error
        if moment.tzinfo is not None:
            raise MalformedResponseError(f"date now has a time zone, check it: {ticker.date!r}")
        return Reading(ticker.bid, moment.replace(tzinfo=UTC))
