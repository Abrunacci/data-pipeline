"""JSON parsing shared by sources: decimals stay exact and non-JSON answers are malformed."""

from __future__ import annotations

import json
from decimal import Decimal

from data_pipeline.core.sources import MalformedResponseError


def load_json(body: bytes) -> object:
    """Parse ``body`` with every JSON number with a fraction read as ``Decimal``, never float.

    A Cloudflare challenge or an error page arrives as HTML with a 200: it is malformed.
    """
    try:
        return json.loads(body, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MalformedResponseError(f"not JSON: {body[:80]!r}") from error
