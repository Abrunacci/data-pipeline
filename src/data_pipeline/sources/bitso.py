"""Bitso's public ticker, the best bid of a book.

Docs: https://docs.bitso.com/bitso-api/docs/ticker. The public API allows 60 requests per minute
per IP (https://docs.bitso.com/bitso-api/docs/general-concepts).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import DecimalText, aware_iso, describe, load_json

TICKER_URL = "https://api.bitso.com/v3/ticker/"


class _Payload(BaseModel):
    # Bitso sends prices as plain decimal strings and times as ISO 8601 strings. Anything else
    # (a JSON number, an epoch) means the format changed, so it is refused, not coerced.
    model_config = ConfigDict(strict=True)

    book: str
    bid: DecimalText
    created_at: str


class _Ticker(BaseModel):
    model_config = ConfigDict(strict=True)

    success: Literal[True]
    payload: _Payload


@dataclass(frozen=True, slots=True)
class BitsoBid:
    """What Bitso pays for each unit of the book's first currency sold as a taker: the best bid.

    ``book`` is Bitso's book id, e.g. ``usdt_ars``.
    """

    book: str

    @property
    def name(self) -> str:
        return f"bitso_{self.book}_bid"

    def request(self) -> Request:
        return Request("GET", f"{TICKER_URL}?book={self.book}")

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        del fetched_at  # the source says when its value is from
        data = load_json(body)
        try:
            ticker = _Ticker.model_validate(data)
        except ValidationError as error:
            if isinstance(data, dict) and isinstance(bitso_error := data.get("error"), dict):
                message = f"Bitso error: {bitso_error.get('message')}"
                raise MalformedResponseError(message) from error
            raise MalformedResponseError(describe(error)) from error
        if ticker.payload.book != self.book:
            raise MalformedResponseError(f"expected book {self.book}, got {ticker.payload.book}")
        return Reading(ticker.payload.bid, aware_iso(ticker.payload.created_at, "created_at"))
