"""The sources the pipeline can read, by name. Series in ``config/series.yaml`` refer to them."""

from __future__ import annotations

from collections.abc import Mapping

from data_pipeline.core.sources import Source
from data_pipeline.sources.bitso import BitsoBid


def available_sources() -> Mapping[str, Source]:
    sources: list[Source] = [BitsoBid("usdt_ars")]
    return {source.name: source for source in sources}
