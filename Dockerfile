# syntax=docker/dockerfile:1
FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md alembic.ini ./
COPY src ./src
COPY config ./config
COPY migrations ./migrations
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/config /app/config
COPY --from=build /app/alembic.ini /app/alembic.ini
COPY --from=build /app/migrations /app/migrations
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SERIES_FILE=/app/config/series.yaml
USER app
EXPOSE 8000
# One worker: the scheduler runs inside the process, and one is plenty for a few reads a minute.
CMD ["uvicorn", "--factory", "data_pipeline.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
