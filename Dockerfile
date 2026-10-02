ARG PYTHON_IMAGE=python:3.13.15-slim-trixie@sha256:8d9d0b8bcf6506481eae4907c18f5e3e7902e629f5f6d684f9e7c32e85e3ddf0

FROM ${PYTHON_IMAGE} AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
COPY config ./config
COPY data ./data
COPY migrations ./migrations
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}
# The uid infra runs backends with.
RUN useradd --system --uid 10005 --no-create-home app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/config /app/config
COPY --from=build /app/data /app/data
COPY alembic.ini /app/alembic.ini
COPY --from=build /app/migrations /app/migrations
# SERIES_FILE is required here: the installed package cannot find config/ relative to itself.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SERIES_FILE=/app/config/series.yaml
USER app
EXPOSE 8000
# The process touches /tmp/alive every 30 s; the check fails when it is missing or older than
# 2 minutes (src/data_pipeline/runner/heartbeat.py). It needs a writable /tmp, a tmpfs on the server.
# - interval 30s: one check per beat; with retries 3, a hung process is unhealthy within about
#   3.5 minutes (2 for the file to age, then 3 failed checks).
# - timeout 5s: the check is a stat from a bare Python start, well under a second even on a busy
#   server; slower than 5 s counts as a failure.
# - start-period 60s: failures while the app starts do not count, and the first success ends it.
# - start-interval 2s: checks every 2 s during the start period, so the deploy sees "healthy"
#   seconds after the first beat instead of waiting a full interval. Docker Engine 25+; older
#   engines ignore it and check every 30 s from the start.
# - retries 3: one slow check under load does not flip it; three in a row do.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --start-interval=2s --retries=3 \
    CMD ["python", "-m", "data_pipeline.runner.heartbeat"]
# The runner: no database and no HTTP of its own (docs/plan.md, step 1). The API still runs from
# this image with an explicit command, as compose.yml does, until step 3 removes it.
CMD ["python", "-m", "data_pipeline.runner"]
