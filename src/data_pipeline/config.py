"""Settings from the environment, and the series from ``config/series.yaml``."""

from __future__ import annotations

import csv
from collections.abc import Mapping
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from data_pipeline.core.checks import Range, Rules
from data_pipeline.core.gap import GapSample, summarize
from data_pipeline.core.schedule import OpeningHours
from data_pipeline.core.series import Series
from data_pipeline.core.sources import HistorySource, Source

DEFAULT_SERIES_FILE = Path(__file__).resolve().parents[2] / "config" / "series.yaml"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    # SQLAlchemy URL with the psycopg driver: postgresql+psycopg://user:password@host/db
    database_url: str
    series_file: Path = DEFAULT_SERIES_FILE
    run_scheduler: bool = True
    # Origins allowed to read the API from a browser, comma separated.
    cors_origins: Annotated[tuple[str, ...], NoDecode] = ()
    contact_url: str = "https://github.com/Abrunacci/data-pipeline"

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(origin.strip() for origin in value.split(",") if origin.strip())
        return value

    @property
    def user_agent(self) -> str:
        return f"data-pipeline (+{self.contact_url})"


class ConfigError(Exception):
    """The series file is wrong. The message names the file and the field."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


type _Positive = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class _Range(_Strict):
    min: _Positive
    max: _Positive


def _time_text(value: object) -> object:
    # Unquoted, YAML 1.1 reads 10:45 as the number 645 (base 60). Only a quoted "10:45" is a time.
    if not isinstance(value, str):
        raise ValueError(f'write times quoted, like "10:45"; got {value!r}')
    return value


type _Time = Annotated[time, BeforeValidator(_time_text)]


class _Hours(_Strict):
    weekdays: tuple[Annotated[int, Field(ge=0, le=6, strict=True)], ...]
    opens: _Time
    closes: _Time
    zone: str


class _Series(_Strict):
    id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(_[a-z0-9]+)*$")]
    description: Annotated[str, StringConstraints(min_length=1)]
    sources: tuple[str, ...]
    control: str | None = None
    every_minutes: Annotated[int, Field(gt=0, strict=True)]
    hours: _Hours | None = None
    plausible: _Range
    max_age_minutes: Annotated[int, Field(gt=0, strict=True)]
    max_jump_percent: _Positive
    control_within_percent: _Positive = Decimal("1.5")
    official_source: Annotated[bool, Field(strict=True)] = True
    indicative: Annotated[bool, Field(strict=True)] = False
    history: str | None = None
    # CSV of observed list/final pairs (see data/binance_card_quotes.csv), relative to the
    # series file. Only for an indicative series.
    gap_samples: str | None = None


class _File(_Strict):
    series: tuple[_Series, ...]


def load_series(
    path: Path,
    sources: Mapping[str, Source],
    histories: Mapping[str, HistorySource],
) -> tuple[Series, ...]:
    """Read and check the series file. Every problem is a ``ConfigError`` naming the file."""
    try:
        raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_DecimalLoader)
        parsed = _File.model_validate(raw)
        series = tuple(_build(entry, sources, histories, path.parent) for entry in parsed.series)
    except (OSError, yaml.YAMLError, ValidationError, ValueError, GapSamplesError) as error:
        raise ConfigError(f"{path}: {error}") from error
    ids = [s.id for s in series]
    if duplicated := sorted({i for i in ids if ids.count(i) > 1}):
        raise ConfigError(f"{path}: repeated series ids {duplicated}")
    return series


def _build(
    entry: _Series,
    sources: Mapping[str, Source],
    histories: Mapping[str, HistorySource],
    directory: Path,
) -> Series:
    named = [*entry.sources, *([] if entry.control is None else [entry.control])]
    if unknown := [name for name in named if name not in sources]:
        raise ValueError(f"series {entry.id}: unknown sources {unknown}")
    if entry.history is not None and entry.history not in histories:
        raise ValueError(f"series {entry.id}: unknown history source {entry.history!r}")
    if entry.gap_samples is not None and not entry.indicative:
        raise ValueError(f"series {entry.id}: gap_samples is only for an indicative series")
    hundred = Decimal(100)
    hours = entry.hours
    return Series(
        id=entry.id,
        description=entry.description,
        sources=tuple(entry.sources),
        control=entry.control,
        every=timedelta(minutes=entry.every_minutes),
        hours=None
        if hours is None
        else OpeningHours(frozenset(hours.weekdays), hours.opens, hours.closes, _zone(hours.zone)),
        rules=Rules(
            plausible=Range(entry.plausible.min, entry.plausible.max),
            max_age=timedelta(minutes=entry.max_age_minutes),
            max_jump=entry.max_jump_percent / hundred,
            control_within=entry.control_within_percent / hundred,
        ),
        official_source=entry.official_source,
        history=entry.history,
        indicative=entry.indicative,
        gap=None
        if entry.gap_samples is None
        else summarize(load_gap_samples(directory / entry.gap_samples)),
    )


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise ValueError(f"unknown time zone {name!r}") from error


class GapSamplesError(Exception):
    """The observed pairs file is wrong. The message names the line."""


GAP_COLUMNS = ("observed_at", "fiat_amount_usd", "list_usdt", "final_usdt", "fee_usd", "note")


def load_gap_samples(path: Path) -> list[GapSample]:
    """Read observed list/final pairs. ``note`` is kept in the file for the record."""
    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file, strict=True)
        if tuple(reader.fieldnames or ()) != GAP_COLUMNS:
            raise GapSamplesError(
                f"{path}: expected columns {GAP_COLUMNS}, got {reader.fieldnames}"
            )
        samples: list[GapSample] = []
        for row in reader:
            where = f"{path}:{reader.line_num}"
            if None in row or None in row.values():
                raise GapSamplesError(f"{where}: expected {len(GAP_COLUMNS)} columns")
            try:
                observed_at = datetime.fromisoformat(row["observed_at"])
                samples.append(
                    GapSample(
                        observed_at=observed_at,
                        amount=Decimal(row["fiat_amount_usd"]),
                        fee=Decimal(row["fee_usd"]),
                        listed=Decimal(row["list_usdt"]),
                        final=Decimal(row["final_usdt"]),
                    )
                )
            except (ValueError, ArithmeticError, TypeError) as error:
                raise GapSamplesError(f"{where}: {error}") from error
    return samples


class _DecimalLoader(yaml.SafeLoader):
    """A safe loader that reads YAML floats as ``Decimal`` from their text and refuses
    repeated keys, which plain YAML silently collapses to the last one."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        keys = [self.construct_object(key, deep=deep) for key, _ in node.value]
        if duplicated := sorted({str(k) for k in keys if keys.count(k) > 1}):
            raise yaml.constructor.ConstructorError(
                None, None, f"repeated keys {duplicated}", node.start_mark
            )
        return super().construct_mapping(node, deep=deep)


def _construct_decimal(loader: yaml.SafeLoader, node: yaml.Node) -> Decimal:
    assert isinstance(node, yaml.ScalarNode)
    text = loader.construct_scalar(node)
    try:
        return Decimal(text.replace("_", ""))
    except ArithmeticError as error:  # decimal.InvalidOperation: .inf, .nan
        raise yaml.constructor.ConstructorError(
            None, None, f"not a decimal: {text}", node.start_mark
        ) from error


_DecimalLoader.add_constructor("tag:yaml.org,2002:float", _construct_decimal)
