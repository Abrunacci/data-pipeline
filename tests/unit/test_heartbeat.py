from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from data_pipeline.runner.heartbeat import (
    MAX_AGE_SECONDS,
    beat,
    beat_forever,
    is_fresh,
    main,
)


def age(path: Path, seconds: float) -> None:
    """Set the file's modification time to ``seconds`` ago."""
    then = time.time() - seconds
    os.utime(path, (then, then))


def test_a_beat_creates_the_file(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    beat(alive)
    assert is_fresh(alive)


def test_a_beat_makes_an_old_file_fresh(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    alive.touch()
    age(alive, 600)
    assert not is_fresh(alive)
    beat(alive)
    assert is_fresh(alive)


def test_the_check_fails_without_the_file(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    assert not is_fresh(alive)
    assert main(alive) == 1


def test_the_check_fails_with_a_file_older_than_two_minutes(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    alive.touch()
    age(alive, MAX_AGE_SECONDS + 1)
    assert main(alive) == 1


def test_the_check_passes_up_to_two_minutes(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    alive.touch()
    touched = alive.stat().st_mtime
    assert is_fresh(alive, now=lambda: touched + MAX_AGE_SECONDS)
    assert not is_fresh(alive, now=lambda: touched + MAX_AGE_SECONDS + 0.001)
    assert main(alive) == 0


@pytest.mark.anyio
async def test_every_tick_touches_the_file(tmp_path: Path) -> None:
    alive = tmp_path / "alive"
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        # Each tick starts with the file fresh; make it old before the next one.
        assert is_fresh(alive)
        age(alive, 600)
        waits.append(seconds)
        if len(waits) == 3:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await beat_forever(alive, every=30, sleep=sleep)
    assert waits == [30, 30, 30]


@pytest.mark.anyio
async def test_a_failed_beat_does_not_stop_the_heartbeat(tmp_path: Path) -> None:
    alive = tmp_path / "missing-directory" / "alive"
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)
        if len(waits) == 2:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await beat_forever(alive, every=30, sleep=sleep)
    assert waits == [30, 30]
    assert not is_fresh(alive)
