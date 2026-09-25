"""ArgentinaDatos' daily closing quotes of a dollar ("casa"), for a series' past values.

Docs: https://argentinadatos.com. Open source (MIT, https://github.com/enzonotario/
argentinadatos.com), from the author of DolarApi; it states no rate limit. It is read once per
series. Each row has a date and no time: the value is taken as of the market's close that day.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import JsonNumber, describe, load_json

QUOTES_URL = "https://api.argentinadatos.com/v1/cotizaciones/dolares"


class _Row(BaseModel):
    model_config = ConfigDict(strict=True)

    casa: str
    compra: JsonNumber
    venta: JsonNumber
    fecha: str


_ROWS = TypeAdapter(list[_Row])


@dataclass(frozen=True, slots=True)
class ArgentinaDatosDaily:
    """``side`` of ``casa`` (e.g. the ``compra`` of ``bolsa``, the MEP), one value a day, as of
    ``close`` in ``zone``."""

    casa: str
    side: Literal["compra", "venta"]
    close: dt.time
    zone: ZoneInfo

    @property
    def name(self) -> str:
        return f"argentinadatos_{self.casa}_{self.side}_daily"

    def request(self) -> Request:
        return Request("GET", f"{QUOTES_URL}/{self.casa}")

    def parse(self, body: bytes, fetched_at: dt.datetime) -> list[Reading]:
        del fetched_at  # every row has its date
        try:
            rows = _ROWS.validate_python(load_json(body))
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        readings = []
        for row in rows:
            if row.casa != self.casa:
                raise MalformedResponseError(f"expected casa {self.casa}, got {row.casa}")
            try:
                day = dt.date.fromisoformat(row.fecha)
            except ValueError as error:
                raise MalformedResponseError(f"fecha is not a date: {row.fecha!r}") from error
            value = row.compra if self.side == "compra" else row.venta
            readings.append(Reading(value, dt.datetime.combine(day, self.close, self.zone)))
        return sorted(readings, key=lambda reading: reading.as_of)
