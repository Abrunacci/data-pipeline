"""Bitso's public ticker, the best bid of a book.

Docs: https://docs.bitso.com/bitso-api/docs/ticker. The public API allows 60 requests per minute
per IP (https://docs.bitso.com/bitso-api/docs/general-concepts).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import load_json

TICKER_URL = "https://api.bitso.com/v3/ticker/"


class _Payload(BaseModel):
    # Bitso sends prices as plain decimal strings and times as ISO 8601 strings. Anything else
    # (a JSON number, an epoch) means the format changed, so it is refused, not coerced.
    model_config = ConfigDict(strict=True)

    book: str
    bid: Annotated[str, StringConstraints(pattern=r"^[0-9]+(\.[0-9]+)?$")]
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

    def parse(self, body: bytes) -> Reading:
        data = load_json(body)
        try:
            ticker = _Ticker.model_validate(data)
        except ValidationError as error:
            raise MalformedResponseError(_describe(data, error)) from error
        if ticker.payload.book != self.book:
            raise MalformedResponseError(f"expected book {self.book}, got {ticker.payload.book}")
        try:
            as_of = datetime.fromisoformat(ticker.payload.created_at)
        except ValueError as error:
            raise MalformedResponseError(f"created_at is not ISO 8601: {error}") from error
        if as_of.tzinfo is None:
            raise MalformedResponseError(f"created_at has no time zone: {as_of}")
        return Reading(Decimal(ticker.payload.bid), as_of)


def _describe(data: object, error: ValidationError) -> str:
    if isinstance(data, dict) and isinstance(body := data.get("error"), dict):
        return f"Bitso error: {body.get('message')}"
    return f"unexpected shape: {error.errors(include_url=False, include_input=False)}"
