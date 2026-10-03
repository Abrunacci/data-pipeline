"""The values the pipeline publishes must be ones the cuanto-cuesta calculator accepts, the
plausible ranges must match the ones it warns with, and each rate must go with the pair its
ingest API expects.

Set ``CUANTO_CUESTA_DIR`` to a checkout of https://github.com/Abrunacci/cuanto-cuesta to run
these; CI checks it out. Without it they are skipped.
"""

from __future__ import annotations

import os
import re
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from data_pipeline.config import DEFAULT_SERIES_FILE, load_series
from data_pipeline.core.checks import MAX_DECIMALS, MAX_VALUE
from data_pipeline.destinations.cuanto_cuesta import RATES
from data_pipeline.sources import available_sources

CALCULATOR = os.environ.get("CUANTO_CUESTA_DIR")
pytestmark = pytest.mark.skipif(CALCULATOR is None, reason="CUANTO_CUESTA_DIR is not set")


def frontend(relative: str) -> str:
    assert CALCULATOR is not None
    return (Path(CALCULATOR) / "frontend" / "src" / relative).read_text(encoding="utf-8")


def number(text: str) -> Decimal:
    """A TypeScript number literal or ``new Decimal(...)`` argument: 50_000, "0.5", 2."""
    return Decimal(text.strip().strip('"').replace("_", ""))


def test_the_calculator_accepts_every_value_we_publish() -> None:
    inputs = frontend("calculator/inputs.ts")
    max_price = re.search(r"MAX_PRICE = new Decimal\(([^)]+)\)", inputs)
    max_decimals = re.search(r"MAX_PRICE_DECIMALS = (\d+)", inputs)
    assert max_price is not None
    assert max_decimals is not None
    assert number(max_price.group(1)) == MAX_VALUE
    assert int(max_decimals.group(1)) == MAX_DECIMALS


def test_plausible_ranges_match_the_calculator_warnings() -> None:
    checks = {
        key: (number(low), number(high))
        for key, low, high in re.findall(
            r"(\w+): \{ min: new Decimal\(([^)]+)\), max: new Decimal\(([^)]+)\)",
            frontend("form/plausible.ts"),
        )
    }
    assert checks, "could not read PRICE_CHECKS from plausible.ts"
    for series in load_series(DEFAULT_SERIES_FILE, available_sources()):
        assert series.id in checks, f"{series.id} is not a calculator rate"
        plausible = series.rules.plausible
        assert (plausible.min, plausible.max) == checks[series.id], series.id


def test_each_rate_is_sent_with_the_pair_cuanto_cuesta_ingests() -> None:
    assert CALCULATOR is not None
    path = Path(CALCULATOR) / "backend" / "config" / "rates.yaml"
    if not path.exists():
        pytest.skip("cuanto-cuesta's checkout has no backend/config/rates.yaml yet")
    rates = yaml.safe_load(path.read_text(encoding="utf-8"))["rates"]
    theirs = {
        rate["key"]: (rate["base"], rate["quote"], rate.get("estimated_final", False))
        for rate in rates
    }
    ours = {key: (r.base, r.quote, r.estimated_final) for key, r in RATES.items()}
    assert ours == theirs
