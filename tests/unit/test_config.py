from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from data_pipeline.config import DEFAULT_SERIES_FILE, ConfigError, Settings, load_series
from data_pipeline.core.series import Series
from data_pipeline.sources import available_sources

VALID = """
series:
  - id: bitso_usdt_ars
    description: test
    sources: [bitso_usdt_ars_bid]
    every_minutes: 10
    plausible: {min: 500, max: 50000}
    max_age_minutes: 30
    max_jump_percent: 5
    confirm_within_percent: 0.1
"""


def load(tmp_path: Path, text: str) -> tuple[Series, ...]:
    path = tmp_path / "series.yaml"
    path.write_text(text)
    return load_series(path, available_sources())


def test_the_repo_series_file_loads() -> None:
    series = load_series(DEFAULT_SERIES_FILE, available_sources())
    assert [s.id for s in series] == ["bitso_usdt_ars"]
    (bitso,) = series
    assert bitso.every == timedelta(minutes=10)
    assert bitso.rules.max_jump == Decimal("0.05")
    assert bitso.rules.confirm_within == Decimal("0.005")


def test_decimals_are_read_from_their_text(tmp_path: Path) -> None:
    (series,) = load(tmp_path, VALID)
    # 0.1 as a float would be 0.1000000000000000055…; divided by 100 it must be exact.
    assert series.rules.confirm_within == Decimal("0.001")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("bitso_usdt_ars_bid]", "nope]"), "unknown sources"),
        (("every_minutes: 10", "every_minutes: 10\n    extra: 1"), "extra"),
        (("every_minutes: 10", "every_minutes: true"), "every_minutes"),
        (("every_minutes: 10", "every_minutes: 10\n    every_minutes: 5"), "repeated keys"),
        (("max_jump_percent: 5", "max_jump_percent: .inf"), "not a decimal: .inf"),
        (("max_jump_percent: 5", "max_jump_percent: 0.05"), "confirm_within"),
        (("id: bitso_usdt_ars", "id: Bitso-USDT"), "id"),
        (("{min: 500, max: 50000}", "{min: 50000, max: 500}"), "min < max"),
        (("max_age_minutes: 30", "max_age_minutes: 5"), "shorter than every"),
    ],
)
def test_mistakes_are_config_errors_naming_the_file(
    tmp_path: Path, change: tuple[str, str], message: str
) -> None:
    with pytest.raises(ConfigError, match=message) as error:
        load(tmp_path, VALID.replace(*change))
    assert "series.yaml" in str(error.value)


def test_repeated_series_ids_are_refused(tmp_path: Path) -> None:
    entry = VALID.split("series:\n", 1)[1]
    with pytest.raises(ConfigError, match="repeated series ids"):
        load(tmp_path, VALID + entry)


def test_cors_origins_are_read_as_a_comma_separated_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://localhost/test")
    monkeypatch.setenv("CORS_ORIGINS", "https://a.example, https://b.example,")
    assert Settings().cors_origins == ("https://a.example", "https://b.example")
