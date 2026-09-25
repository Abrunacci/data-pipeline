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
COPY migrations ./migrations
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}
# The uid infra runs backends with.
RUN useradd --system --uid 10005 --no-create-home app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/config /app/config
COPY alembic.ini /app/alembic.ini
COPY --from=build /app/migrations /app/migrations
# SERIES_FILE is required here: the installed package cannot find config/ relative to itself.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SERIES_FILE=/app/config/series.yaml
USER app
EXPOSE 8000
# One worker: the scheduler runs inside the process, and one is plenty for a few reads a minute.
CMD ["uvicorn", "--factory", "data_pipeline.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", \
     "--timeout-graceful-shutdown", "10"]
