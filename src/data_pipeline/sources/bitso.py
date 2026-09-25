"""Bitso's public ticker, the best bid of a book.

Docs: https://docs.bitso.com/bitso-api/docs/ticker. The public API allows 60 requests per minute
per IP (https://docs.bitso.com/bitso-api/docs/general-concepts).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import load_json

TICKER_URL = "https://api.bitso.com/v3/ticker/"


class _Payload(BaseModel):
    book: str
    bid: Decimal
    created_at: AwareDatetime


class _Ticker(BaseModel):
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

    def parse(self, body: bytes) -> Reading:
        data = load_json(body)
        try:
            ticker = _Ticker.model_validate(data)
        except ValidationError as error:
            raise MalformedResponseError(_describe(data, error)) from error
        if ticker.payload.book != self.book:
            raise MalformedResponseError(f"expected book {self.book}, got {ticker.payload.book}")
        return Reading(ticker.payload.bid, ticker.payload.created_at)


def _describe(data: object, error: ValidationError) -> str:
    if isinstance(data, dict) and isinstance(body := data.get("error"), dict):
        return f"Bitso error: {body.get('message')}"
    return f"unexpected shape: {error.errors(include_url=False, include_input=False)}"
