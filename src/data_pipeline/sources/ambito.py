"""Ámbito's MEP, from the endpoint behind its quote widgets. Not documented, and ambito.com
publishes no terms for it; it answers with ``x-ratelimit-limit: 100`` and no window.

It gives a single MEP price (``compra`` equals ``venta``), with a comma as decimal separator and
a local time to the minute ("25/09/2026 - 15:00", Buenos Aires). It is used as a control only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.schedule import BUENOS_AIRES
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import describe, load_json

MEP_URL = "https://mercados.ambito.com//dolarrava/mep/variacion"
_PRICE = re.compile(r"^[0-9]{1,3}(\.[0-9]{3})*,[0-9]+$|^[0-9]+,[0-9]+$")
_DATE_FORMAT = "%d/%m/%Y - %H:%M"


class _Variacion(BaseModel):
    model_config = ConfigDict(strict=True)

    compra: str
    fecha: str


@dataclass(frozen=True, slots=True)
class AmbitoMep:
    @property
    def name(self) -> str:
        return "ambito_mep"

    def request(self) -> Request:
        return Request("GET", MEP_URL)

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        del fetched_at  # the source says when its value is from
        try:
            quote = _Variacion.model_validate(load_json(body))
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        if not _PRICE.match(quote.compra):
            raise MalformedResponseError(f"compra is not an Argentine decimal: {quote.compra!r}")
        value = Decimal(quote.compra.replace(".", "").replace(",", "."))
        try:
            local = datetime.strptime(quote.fecha, _DATE_FORMAT)  # noqa: DTZ007  # zone set below
        except ValueError as error:
            raise MalformedResponseError(f"fecha is not {_DATE_FORMAT}: {quote.fecha!r}") from error
        return Reading(value, local.replace(tzinfo=BUENOS_AIRES))
