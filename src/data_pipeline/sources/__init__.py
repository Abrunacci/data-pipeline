"""The sources the pipeline can read, by name. Series in ``config/series.yaml`` refer to them."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from data_pipeline.core.sources import Source
from data_pipeline.sources.ambito import AmbitoMep
from data_pipeline.sources.arq import ArqBid
from data_pipeline.sources.binance_card import BinanceCardPrice
from data_pipeline.sources.binance_p2p import BinanceP2PMedian
from data_pipeline.sources.bitso import BitsoBid
from data_pipeline.sources.criptoya import CriptoYaBid
from data_pipeline.sources.dolarapi import DolarApiMep


def available_sources() -> Mapping[str, Source]:
    sources: list[Source] = [
        BitsoBid("usdt_ars"),
        CriptoYaBid("criptoya_bitso_usdt_ars_bid", "bitsoalpha/USDT/ARS/1"),
        DolarApiMep(),
        AmbitoMep(),
        # Decision D3: merchants' ads that take a 500 USD order, median of the 5 cheapest.
        BinanceP2PMedian(fiat="USD", asset="USDT", amount=Decimal(500)),
        ArqBid(),
        CriptoYaBid("criptoya_arq_usdc_ars_bid", "usdc/ARS/1", exchange="dolarapp"),
        BinanceCardPrice(),
    ]
    by_name = {source.name: source for source in sources}
    assert len(by_name) == len(sources), "two sources share a name"
    return by_name
