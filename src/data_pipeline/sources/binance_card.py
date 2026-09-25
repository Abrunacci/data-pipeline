"""Binance "Buy crypto" with a card: the price Binance lists, which is indicative only.

It uses Binance's public fiat agent API, which needs no key and says "Prices are indicative
reference rates; execution prices may differ" (https://www.binance.com/en/skills/detail/binance/
fiat). On 2026-09-25 a real purchase of 10 USD got about 4.3 % less USDT than this price
promised: the final price is only shown to a logged-in user. Series built on it are marked
indicative, and the gap is estimated from observed pairs (``core.gap``). The answer has no
timestamp: it is as of the answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, NoQuoteError, Request
from data_pipeline.sources.parsing import DecimalText, describe, load_json

PAYMENT_METHODS_URL = (
    "https://www.binance.com/bapi/fiat/v1/public/fiatpayment/agent/get-buy-and-sell-payment-methods"
)
CARD = "BUY_PAYMONADE_CARD"


class _Method(BaseModel):
    model_config = ConfigDict(strict=True)

    code: str
    quotation: DecimalText
    suspended: bool


class _Data(BaseModel):
    model_config = ConfigDict(strict=True)

    paymentMethods: list[_Method]  # noqa: N815  # the source's field name


class _Answer(BaseModel):
    model_config = ConfigDict(strict=True)

    code: Literal["000000"]
    data: _Data


@dataclass(frozen=True, slots=True)
class BinanceCardPrice:
    """How many ``fiat`` Binance lists for each ``crypto`` bought with a card in ``country``:
    ``quotation``, e.g. 1.0228 USD per USDT."""

    fiat: str = "USD"
    crypto: str = "USDT"
    country: str = "AR"

    @property
    def name(self) -> str:
        return f"binance_card_{self.crypto.lower()}_{self.fiat.lower()}_list"

    def request(self) -> Request:
        return Request(
            "GET",
            f"{PAYMENT_METHODS_URL}?businessType=BUY&fiatCurrency={self.fiat}"
            f"&cryptoCurrency={self.crypto}&country={self.country}",
        )

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        try:
            methods = _Answer.model_validate(load_json(body)).data.paymentMethods
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        cards = [method for method in methods if method.code == CARD]
        if not cards:
            raise NoQuoteError(f"no {CARD} among {[method.code for method in methods]}")
        if cards[0].suspended:
            raise NoQuoteError(f"{CARD} is suspended")
        return Reading(cards[0].quotation, fetched_at)
