"""A series: one value tracked over time, where it comes from and how it is checked."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from data_pipeline.core.checks import Rules


@dataclass(frozen=True, slots=True)
class Series:
    """``id`` is the public name of the value: consumers look it up by it, so it never changes.

    ``sources`` are tried in order until one gives a valid reading.
    """

    id: str
    description: str
    sources: tuple[str, ...]
    every: timedelta
    rules: Rules

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError(f"series {self.id} has no sources")
        if len(set(self.sources)) != len(self.sources):
            raise ValueError(f"series {self.id} lists a source twice")
        if self.every <= timedelta(0):
            raise ValueError(f"series {self.id}: every must be positive, got {self.every}")
