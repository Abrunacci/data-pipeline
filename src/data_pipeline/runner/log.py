"""The record of every attempt: one log line each. The runner keeps no state, so this is the only
trace of what a source answered and why a reading was or was not sent."""

from __future__ import annotations

import logging
from decimal import Decimal

from data_pipeline.core.readings import Accepted, Observation, Rejected

logger = logging.getLogger(__name__)


def log_attempt(observation: Observation) -> None:
    head = f"{observation.series_id} {observation.source}"
    match observation.outcome:
        case Accepted(reading=reading):
            logger.info("%s accepted %s", head, _value(reading.value))
        case Rejected(reason=reason, detail=detail):
            logger.warning("%s rejected, %s: %s", head, reason, detail)


def _value(value: Decimal) -> str:
    return format(value, "f")
