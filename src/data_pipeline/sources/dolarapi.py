"""DolarApi's MEP ("bolsa"): what you get for each dollar sold, the ``compra`` side.

Docs: https://dolarapi.com/docs/argentina/operations/get-dolar-bolsa.html. Open source (MIT,
https://github.com/enzonotario/esjs-dolar-api); it states no rate limit. Its legal notice says
the data is informative and may not match real quotes (https://dolarapi.com/docs/legal).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from data_pipeline.core.readings import Reading
from data_pipeline.core.sources import MalformedResponseError, Request
from data_pipeline.sources.parsing import JsonNumber, aware_iso, describe, load_json

BOLSA_URL = "https://dolarapi.com/v1/dolares/bolsa"


class _Bolsa(BaseModel):
    model_config = ConfigDict(strict=True)

    moneda: Literal["USD"]
    casa: Literal["bolsa"]
    compra: JsonNumber
    fechaActualizacion: str  # noqa: N815  # the source's field name


@dataclass(frozen=True, slots=True)
class DolarApiMep:
    @property
    def name(self) -> str:
        return "dolarapi_mep_compra"

    def request(self) -> Request:
        return Request("GET", BOLSA_URL)

    def parse(self, body: bytes, fetched_at: datetime) -> Reading:
        del fetched_at  # the source says when its value is from
        try:
            bolsa = _Bolsa.model_validate(load_json(body))
        except ValidationError as error:
            raise MalformedResponseError(describe(error)) from error
        return Reading(bolsa.compra, aware_iso(bolsa.fechaActualizacion, "fechaActualizacion"))
