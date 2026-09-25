"""Binance P2P: what a USDT costs in a fiat currency, from the ads of trustworthy merchants.

It uses Binance's public P2P agent API, which needs no key and is published for automated use
(https://www.binance.com/en/skills/detail/binance/p2p). It states no rate limit. Binance's terms
forbid automated access "not purposely provided through Binance Services", and commercial use of
its market data without consent; this API is provided for it, and the calculator is free.
Binance blocks some countries, possibly including the server's: check from the server. ``ad-list``
returns at most 20 ads, cheapest first, and has no filter by amount or by kind of advertiser,
so both filters are applied here. The ads have no timestamp: they are as of the answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, NoQuoteError, Request
from data_pipeline.sources.parsing import JsonNumber, describe, load_json

AD_LIST_URL = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
MAX_ADS = 20


class _Advertiser(BaseModel):
    model_config = ConfigDict(strict=True)

    userType: str  # noqa: N815  # the source's field names
    monthFinishRate: JsonNumber  # noqa: N815


class _Ad(BaseModel):
    model_config = ConfigDict(strict=True)

    price: JsonNumber
    fiat: str
    asset: str
    minTransAmount: JsonNumber  # noqa: N815
    maxTransAmount: JsonNumber  # noqa: N815
    tradableAmount: JsonNumber  # noqa: N815
    advertiser: _Advertiser


class _Data(BaseModel):
    model_config = ConfigDict(strict=True)

    items: list[_Ad]


class _AdList(BaseModel):
    model_config = ConfigDict(strict=True)

    code: Literal["000000"]
    data: _Data


@dataclass(frozen=True, slots=True)
class BinanceP2PMedian:
    """The median price of the ``top`` cheapest ads to buy ``asset`` with ``fiat`` whose
    advertiser is a merchant with at least ``min_finish_rate`` of orders completed last month,
    and that take an order of ``amount`` (in ``fiat``), with enough ``asset`` left to fill it.
    """

    fiat: str
    asset: str
    amount: Decimal
    top: int = 5
    min_finish_rate: Decimal = Decimal("0.95")

    @property
    def name(self) -> str:
        return f"binance_p2p_{self.asset.lower()}_{self.fiat.lower()}_buy_median"

    def request(self) -> Request:
        return Request(
            "GET",
            f"{AD_LIST_URL}?fiat={self.fiat}&asset={self.asset}&tradeType=BUY&limit={MAX_ADS}",
        )

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        try:
            ads = _AdList.model_validate(load_json(body)).data.items
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        if other := [ad for ad in ads if (ad.fiat, ad.asset) != (self.fiat, self.asset)]:
            raise MalformedResponseError(f"got ads for {other[0].asset}/{other[0].fiat}")
        usable = sorted(ad.price for ad in ads if self._usable(ad))
        if len(usable) < self.top:
            raise NoQuoteError(f"only {len(usable)} usable ads of {len(ads)}, need {self.top}")
        return Reading(Decimal(median(usable[: self.top])), fetched_at)

    def _usable(self, ad: _Ad) -> bool:
        return (
            ad.advertiser.userType == "merchant"
            and ad.advertiser.monthFinishRate >= self.min_finish_rate
            and ad.minTransAmount <= self.amount <= ad.maxTransAmount
            and ad.tradableAmount * ad.price >= self.amount
        )
