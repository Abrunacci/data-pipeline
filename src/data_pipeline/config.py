"""Settings from the environment, and the series from ``config/series.yaml``."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from data_pipeline.core.checks import Range, Rules
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source

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


class _Series(_Strict):
    id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(_[a-z0-9]+)*$")]
    description: Annotated[str, StringConstraints(min_length=1)]
    sources: tuple[str, ...]
    every_minutes: Annotated[int, Field(gt=0, strict=True)]
    plausible: _Range
    max_age_minutes: Annotated[int, Field(gt=0, strict=True)]
    max_jump_percent: _Positive
    confirm_within_percent: _Positive


class _File(_Strict):
    series: tuple[_Series, ...]


def load_series(path: Path, sources: Mapping[str, Source]) -> tuple[Series, ...]:
    """Read and check the series file. Every problem is a ``ConfigError`` naming the file."""
    try:
        raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_DecimalLoader)
        parsed = _File.model_validate(raw)
        series = tuple(_build(entry, sources) for entry in parsed.series)
    except (OSError, yaml.YAMLError, ValidationError, ValueError) as error:
        raise ConfigError(f"{path}: {error}") from error
    ids = [s.id for s in series]
    if duplicated := sorted({i for i in ids if ids.count(i) > 1}):
        raise ConfigError(f"{path}: repeated series ids {duplicated}")
    return series


def _build(entry: _Series, sources: Mapping[str, Source]) -> Series:
    if unknown := [name for name in entry.sources if name not in sources]:
        raise ValueError(f"series {entry.id}: unknown sources {unknown}")
    hundred = Decimal(100)
    return Series(
        id=entry.id,
        description=entry.description,
        sources=tuple(entry.sources),
        every=timedelta(minutes=entry.every_minutes),
        rules=Rules(
            plausible=Range(entry.plausible.min, entry.plausible.max),
            max_age=timedelta(minutes=entry.max_age_minutes),
            max_jump=entry.max_jump_percent / hundred,
            confirm_within=entry.confirm_within_percent / hundred,
        ),
    )


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
