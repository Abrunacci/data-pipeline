# data-pipeline

Scheduled collection of market data, with validation and history. It collects the exchange rates
the [cuanto-cuesta](https://cuanto-cuesta.abrunacci.dev) calculator needs today, and it is meant
to take other datasets later.

Each series is read on a schedule from its sources, checked, and stored with every attempt,
failed or not. A small API publishes the last accepted value of each series, always with when the
source says it is from.

## What it collects

| Series | Value | Source |
|---|---|---|
| `bitso_usdt_ars` | ARS paid for each USDT sold on Bitso (best bid) | [Bitso public API](https://docs.bitso.com/bitso-api/docs/ticker), every 10 minutes |

More rates (MEP, Binance P2P, ARQ) come next.

## How a value gets published

1. **Fetch** from the series' first source, with a 10 s timeout and two retries on network
   errors and 5xx. If the source fails, the next one in the list is tried.
2. **Parse** strictly: an HTML page, an error body or a missing field is rejected.
3. **Check** the value: one the calculator accepts, in a plausible range, and not stale.
4. **Hold back jumps**: a value more than 5 % away from the last accepted one is a suspect. The
   previous value stays published, marked `pending_confirmation`, until the next two readings
   agree with it; then it is published. A real move takes two more runs (20 minutes) to show;
   a one-off glitch never does.

Every attempt is stored with its outcome and reason, so the history shows the failures too.

## Why not Airflow

This started as an Airflow and Spark sandbox. For a handful of values every few minutes on a
2 GB server, both are the wrong tool: Airflow's scheduler, API server and metadata database
need more memory than the server has to spare, and Spark has nothing to distribute. Here the
scheduler is an asyncio task inside the API process, and the whole app runs in about 70 MB.
What Airflow would give is built in:

- runs are aligned to the clock, and a slot that already has an attempt is skipped;
- a Postgres advisory lock keeps two processes (say, during a deploy) from running the same
  series at once;
- retries are per request, and a failed run is recorded and the next slot runs anyway.

## API

`GET /v1/rates/latest`

```json
{
  "rates": {
    "bitso_usdt_ars": {
      "value": "1613.79",
      "as_of": "2026-09-25T18:55:25Z",
      "fetched_at": "2026-09-25T18:55:25.636472Z",
      "source": "bitso_usdt_ars_bid",
      "stale": false,
      "pending_confirmation": false,
      "last_attempt_at": "2026-09-25T18:55:25.636472Z"
    }
  }
}
```

- `value` is a decimal string.
- A series with no accepted value yet is `null`.
- `stale` means `as_of` is older than the series allows.

`GET /health` returns 200 when the database answers, and 503 when it does not.

## Running it

With Docker:

```sh
docker compose up --build        # Postgres, migrations, then the app on http://localhost:8000
```

`APP_PORT` and `DB_PORT` change the host ports.

For development, with [uv](https://docs.astral.sh/uv/):

```sh
uv sync
export DATABASE_URL=postgresql+psycopg://pipeline:pipeline@localhost:5432/pipeline
uv run alembic upgrade head
uv run uvicorn --factory data_pipeline.api.main:app --reload
```

Settings come from the environment:

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | `postgresql+psycopg://…` |
| `MIGRATION_DATABASE_URL` | `DATABASE_URL` | used by Alembic only |
| `RUN_SCHEDULER` | `true` | `false` serves the API without collecting |
| `CORS_ORIGINS` | none | comma-separated origins allowed to read the API from a browser |
| `SERIES_FILE` | `config/series.yaml` | |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the conventions and how to run the checks.
