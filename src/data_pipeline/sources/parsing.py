"""Parsing shared by sources: exact decimals, strict types, and non-JSON answers refused."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator, ValidationError

from data_pipeline.core.sources import MalformedResponseError

_DECIMAL_TEXT = re.compile(r"^[0-9]+(\.[0-9]+)?$")


def load_json(body: bytes) -> object:
    """Parse ``body`` with every JSON number with a fraction read as ``Decimal``, never float.

    A Cloudflare challenge or an error page arrives as HTML with a 200: it is malformed.
    """
    try:
        return json.loads(body, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MalformedResponseError(f"not JSON: {body[:80]!r}") from error


def _decimal_text(value: object) -> Decimal:
    if not isinstance(value, str) or not _DECIMAL_TEXT.match(value):
        raise ValueError(f"expected a plain decimal string, got {value!r}")
    return Decimal(value)


def _json_number(value: object) -> Decimal:
    # load_json turns numbers with a fraction into Decimal and whole ones into int. Anything
    # else, a string or a bool (an int in Python), is a change of format.
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"expected a JSON number, got {value!r}")
    return Decimal(value)


def _epoch_seconds(value: object) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"expected whole epoch seconds, got {value!r}")
    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except (OverflowError, OSError) as error:
        raise ValueError(f"epoch seconds out of range: {value!r}") from error


type DecimalText = Annotated[Decimal, BeforeValidator(_decimal_text)]
"""A price sent as a string of digits with an optional fraction: no sign, spaces or exponent."""

type JsonNumber = Annotated[Decimal, BeforeValidator(_json_number)]
"""A price sent as a JSON number, read exactly."""

type EpochSeconds = Annotated[datetime, BeforeValidator(_epoch_seconds)]
"""A time sent as whole seconds since the epoch, in UTC."""


def aware_iso(value: str, what: str) -> datetime:
    """An ISO 8601 time with a time zone, or ``MalformedResponseError`` naming ``what``."""
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise MalformedResponseError(f"{what} is not ISO 8601: {value!r}") from error
    if moment.tzinfo is None:
        raise MalformedResponseError(f"{what} has no time zone: {value!r}")
    return moment


def describe(error: ValidationError) -> str:
    return f"unexpected shape: {error.errors(include_url=False, include_input=False)}"
