"""The container's liveness signal, for Docker's ``HEALTHCHECK``: the process touches a file
every few seconds, and the check fails when the file is missing or old.

It proves the event loop is running, not that the sources answer or the database is up: every
attempt already records that, and ``/health`` checks the database. It beats whether or not the
scheduler runs, so a container that only serves the API is healthy too.

The check is ``python -m data_pipeline.runner.heartbeat``. It imports only the standard library,
so it starts fast, and it exits 0 when the file is fresh and 1 otherwise.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

logger = logging.getLogger(__name__)

# A tmpfs in the container, the only place it can write.
HEARTBEAT_FILE = Path("/tmp/alive")
BEAT_EVERY_SECONDS = 30.0
# Four missed beats: a slow loop is not a dead one.
MAX_AGE_SECONDS = 120.0


def beat(path: Path = HEARTBEAT_FILE) -> None:
    """Create ``path`` or set its modification time to now."""
    path.touch()


async def beat_forever(
    path: Path = HEARTBEAT_FILE,
    every: float = BEAT_EVERY_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Beat now, then every ``every`` seconds. A failed beat is logged and the next one tries
    again; if they keep failing, the file gets old and the check says so."""
    while True:
        try:
            beat(path)
        except OSError as error:
            logger.error("heartbeat failed: %s", error)
        await sleep(every)


def is_fresh(
    path: Path = HEARTBEAT_FILE,
    max_age: float = MAX_AGE_SECONDS,
    now: Callable[[], float] = time.time,
) -> bool:
    """Whether ``path`` exists and was touched at most ``max_age`` seconds ago."""
    try:
        touched = path.stat().st_mtime
    except OSError:
        return False
    return now() - touched <= max_age


def main(path: Path = HEARTBEAT_FILE) -> int:
    if is_fresh(path):
        return 0
    # Docker keeps the check's output in `docker inspect`, so say why.
    print(f"{path} is missing or older than {MAX_AGE_SECONDS:.0f} s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
