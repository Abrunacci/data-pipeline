"""What a source is: how to ask for a value and how to read the answer.

Sources do no I/O. The runner sends the request, so every source is tested by parsing recorded
responses, and retries and timeouts live in one place.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, Protocol

from data_pipeline.core.readings import Reading

type Json = bool | int | float | str | list[Json] | dict[str, Json] | None


@dataclass(frozen=True, slots=True)
class Request:
    method: Literal["GET", "POST"]
    url: str
    json: Json = None
    headers: Mapping[str, str] = field(default_factory=dict)


class MalformedResponseError(Exception):
    """The response does not have the shape the source expects: HTML, an error, a new format."""


class Source(Protocol):
    @property
    def name(self) -> str:
        """Stable id, stored with every observation. Renaming it splits the history."""
        ...

    def request(self) -> Request: ...

    def parse(self, body: bytes) -> Reading:
        """Read the value from a response body, or raise ``MalformedResponseError``."""
        ...
