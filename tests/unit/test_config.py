from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from data_pipeline.config import (
    DEFAULT_SERIES_FILE,
    ConfigError,
    Settings,
    load_gap_samples,
    load_series,
)
from data_pipeline.core.series import Series
from data_pipeline.sources import available_history_sources, available_sources

VALID = """
series:
  - id: bitso_usdt_ars
    description: test
    sources: [bitso_usdt_ars_bid]
    every_minutes: 10
    plausible: {min: 500, max: 50000}
    max_age_minutes: 30
    max_jump_percent: 5
    control_within_percent: 0.1
"""


def load(tmp_path: Path, text: str) -> tuple[Series, ...]:
    path = tmp_path / "series.yaml"
    path.write_text(text)
    return load_series(path, available_sources(), available_history_sources())


def test_the_repo_series_file_loads() -> None:
    series = {
        s.id: s
        for s in load_series(DEFAULT_SERIES_FILE, available_sources(), available_history_sources())
    }
    assert list(series) == ["mep", "binance_p2p_usdt_usd", "bitso_usdt_ars", "arq_usd_ars"]
    mep = series["mep"]
    assert mep.control == "ambito_mep"
    assert mep.hours is not None
    # Friday 17:30 and Saturday noon, Buenos Aires.
    assert mep.runs_at(datetime(2026, 9, 25, 20, 30, tzinfo=UTC))
    assert not mep.runs_at(datetime(2026, 9, 26, 15, 0, tzinfo=UTC))
    assert series["bitso_usdt_ars"].rules.max_jump == Decimal("0.05")
    assert series["bitso_usdt_ars"].rules.control_within == Decimal("0.015")
    assert series["binance_p2p_usdt_usd"].every == timedelta(minutes=10)
    assert not series["arq_usd_ars"].official_source
    assert all(s.official_source for s in series.values() if s.id != "arq_usd_ars")
    assert not any(s.indicative for s in series.values())


def test_decimals_are_read_from_their_text(tmp_path: Path) -> None:
    (series,) = load(tmp_path, VALID)
    # 0.1 as a float would be 0.1000000000000000055…; divided by 100 it must be exact.
    assert series.rules.control_within == Decimal("0.001")


HOURS = """
    hours:
      weekdays: [0, 4]
      opens: "10:45"
      closes: "17:30"
      zone: America/Argentina/Buenos_Aires"""


def test_opening_hours_are_read_in_their_zone(tmp_path: Path) -> None:
    (series,) = load(tmp_path, VALID + HOURS)
    assert series.hours is not None
    assert series.hours.weekdays == frozenset({0, 4})
    assert series.hours.opens == time(10, 45)
    assert str(series.hours.zone) == "America/Argentina/Buenos_Aires"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (('"10:45"', "10:45"), "write times quoted"),
        (("America/Argentina/Buenos_Aires", "Mars/Olympus"), "unknown time zone"),
        (("[0, 4]", "[0, 7]"), "weekdays"),
        (('closes: "17:30"', 'closes: "10:00"'), "must be before"),
        (('opens: "10:45"', 'opens: "10:45-03:00"'), "the zone goes in zone"),
    ],
)
def test_opening_hours_mistakes_are_config_errors(
    tmp_path: Path, change: tuple[str, str], message: str
) -> None:
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, VALID + HOURS.replace(*change))


def test_a_history_source_must_be_known_and_need_opening_hours(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown history source 'nope'"):
        load(tmp_path, VALID + HOURS + "\n    history: nope")
    with pytest.raises(ConfigError, match="needs opening hours"):
        load(tmp_path, VALID + "\n    history: argentinadatos_bolsa_compra_daily")
    (series,) = load(tmp_path, VALID + HOURS + "\n    history: argentinadatos_bolsa_compra_daily")
    assert series.history == "argentinadatos_bolsa_compra_daily"


def test_a_control_must_be_a_known_source_other_than_the_primary(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"unknown sources \['nope'\]"):
        load(tmp_path, VALID + "\n    control: nope")
    with pytest.raises(ConfigError, match="its own control"):
        load(tmp_path, VALID + "\n    control: bitso_usdt_ars_bid")
    (series,) = load(tmp_path, VALID + "\n    control: criptoya_bitso_usdt_ars_bid")
    assert series.control == "criptoya_bitso_usdt_ars_bid"


CARD = """
series:
  - id: card_test
    description: test
    sources: [binance_card_usdt_usd_list]
    every_minutes: 10
    plausible: {min: 0.5, max: 2}
    max_age_minutes: 30
    max_jump_percent: 2
    indicative: true
    gap_samples: samples.csv
"""
SAMPLES_HEADER = "observed_at,fiat_amount_usd,list_usdt,final_usdt,fee_usd,note\n"


def test_an_indicative_series_reads_its_gap_from_observed_pairs(tmp_path: Path) -> None:
    (tmp_path / "samples.csv").write_text(
        SAMPLES_HEADER
        # 1 - 9.35393217 / 9.7763 = 0.04320...; 1 - 95 / 100 = 0.05; 1 - 97 / 100 = 0.03.
        + "2026-09-25T15:10:00-03:00,10,9.7763,9.35393217,0.20,\n"
        + "2026-09-28T12:00:00-03:00,100,100,95,2,made up for the test\n"
        + "2026-09-29T12:00:00-03:00,100,100,97,2,\n"
    )
    (series,) = load(tmp_path, CARD)
    assert series.indicative
    assert series.gap is not None
    assert series.gap.samples == 3
    assert series.gap.fraction == 1 - Decimal("9.35393217") / Decimal("9.7763")
    assert series.gap.first == datetime(2026, 9, 25, 18, 10, tzinfo=UTC)
    assert series.gap.last == datetime(2026, 9, 29, 15, 0, tzinfo=UTC)


def test_no_observed_pairs_means_no_gap(tmp_path: Path) -> None:
    (tmp_path / "samples.csv").write_text(SAMPLES_HEADER)
    (series,) = load(tmp_path, CARD)
    assert series.gap is None


@pytest.mark.parametrize(
    ("samples", "message"),
    [
        ("observed_at,amount\n", "expected columns"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00,10,9.77,9.35,0.2,\n", "timezone-aware"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,9.35,9.77,0.2,\n", "more than listed"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,abc,9.35,0.2,\n", r"samples.csv:2"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,9.77,9.35,0.2,a, b\n", "6 columns"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,9.77,9.35\n", "6 columns"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,Infinity,9.35,0.2,\n", "finite"),
        (SAMPLES_HEADER + "2026-09-25T15:10:00-03:00,10,9.77,9.35,-1,\n", "fee_usd"),
    ],
)
def test_observed_pairs_mistakes_are_config_errors(
    tmp_path: Path, samples: str, message: str
) -> None:
    (tmp_path / "samples.csv").write_text(samples)
    with pytest.raises(ConfigError, match=message):
        load(tmp_path, CARD)


def test_the_repo_card_samples_file_is_well_formed() -> None:
    path = DEFAULT_SERIES_FILE.parent.parent / "data" / "binance_card_quotes.csv"
    load_gap_samples(path)


def test_only_an_indicative_series_has_a_gap(tmp_path: Path) -> None:
    (tmp_path / "samples.csv").write_text(SAMPLES_HEADER)
    with pytest.raises(ConfigError, match="only for an indicative series"):
        load(tmp_path, CARD.replace("indicative: true", "indicative: false"))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("bitso_usdt_ars_bid]", "nope]"), "unknown sources"),
        (("every_minutes: 10", "every_minutes: 10\n    extra: 1"), "extra"),
        (("every_minutes: 10", "every_minutes: true"), "every_minutes"),
        (("every_minutes: 10", "every_minutes: 10\n    every_minutes: 5"), "repeated keys"),
        (("max_jump_percent: 5", "max_jump_percent: .inf"), "not a decimal: .inf"),
        (("max_jump_percent: 5", "max_jump_percent: 100"), "max_jump must be between"),
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
