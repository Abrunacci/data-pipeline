"""Binance "Buy crypto" with a card: the price Binance lists, which is indicative only.

It uses Binance's public fiat agent API, which needs no key and says "Prices are indicative
reference rates; execution prices may differ"
(https://www.binance.com/en/skills/detail/binance/fiat). It states no rate limit; Binance's
terms are summarized in the P2P source.

On 2026-09-25 a real purchase of 10 USD got 4.3 % less USDT than this price promised: a 2 %
fee, which the calculator charges on its own, and a price 2.37 % worse than listed. The final
price is only shown to a logged-in user. Series built on it are marked indicative, and the
price gap is estimated from observed pairs (``core.gap``). The answer has no timestamp: it is
as of the answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import (
    ROUND_DOWN,
    Context,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)
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


# The calculator accepts prices with at most 8 decimals (core.checks.MAX_DECIMALS).
EIGHT_DECIMALS = Decimal("0.00000001")
_INVERSE = Context(prec=40, rounding=ROUND_DOWN, traps=[InvalidOperation, DivisionByZero, Overflow])


@dataclass(frozen=True, slots=True)
class BinanceCardPrice:
    """How much ``crypto`` Binance lists for each ``fiat`` paid with a card in ``country``.

    Binance quotes the other way round (``quotation``: 1.02282331 USD per USDT). The inverse
    has endless decimals, so it is rounded down to 8, the project's rule for what a person
    gets: 0.97768597 USDT per USD, as Binance itself shows for 10 USD (9.7768597 USDT).
    """

    fiat: str = "USD"
    crypto: str = "USDT"
    country: str = "AR"

    @property
    def name(self) -> str:
        return f"binance_card_{self.fiat.lower()}_{self.crypto.lower()}_list"

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
        quotation = cards[0].quotation
        if quotation == 0:
            raise MalformedResponseError(f"{CARD} quotation is zero")
        # Its own context, so the result does not depend on the thread's. Truncating the
        # quotient at 40 significant digits and then at 8 decimals gives the same result as
        # truncating the exact quotient once, whenever the result fits in 40 digits. One that
        # does not (a quotation at or below 1e-32) is malformed; a merely absurd one (1e-28)
        # inverts, and the checks reject it. The quotation has no sign: DecimalText refuses one.
        try:
            with localcontext(_INVERSE):
                per_fiat = (1 / quotation).quantize(EIGHT_DECIMALS)
        except ArithmeticError as error:
            raise MalformedResponseError(
                f"{CARD} quotation {quotation} cannot be inverted"
            ) from error
        return Reading(per_fiat, fetched_at)
