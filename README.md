# data-pipeline

Scheduled collection of market data, with validation and history. It collects the exchange rates
the [cuanto-cuesta](https://cuanto-cuesta.abrunacci.dev) calculator needs today, and it is meant
to take other datasets later.

Each series is read on a schedule from its sources, checked, and stored with every attempt,
failed or not. A small API publishes the last accepted value of each series, always with when the
source says it is from.

## What it collects

| Series | Value | Source → control | Every |
|---|---|---|---|
| `mep` | ARS paid for each USD sold through the MEP (buy side) | [DolarApi](https://dolarapi.com/docs/argentina/operations/get-dolar-bolsa.html) → Ámbito | 15 min, weekdays 10:45–17:30 Buenos Aires |
| `p2p_usdt_usd` | USD paid for each USDT on Binance P2P: median of the 5 cheapest merchant ads that take 500 USD, from merchants with 95 % of orders completed | [Binance P2P public API](https://www.binance.com/en/skills/detail/binance/p2p) | 10 min |
| `bitso_usdt_ars` | ARS paid for each USDT sold on Bitso (best bid) | [Bitso public API](https://docs.bitso.com/bitso-api/docs/ticker) → CriptoYa | 10 min |
| `arq_usd_ars` | ARS ARQ pays for each USDc, at par with USD (its bid) | ARQ's ticker (undocumented, so `official_source: false`), CriptoYa as fallback → CriptoYa | 10 min |

The series ids are the ones the calculator uses. A Binance card-purchase price is ready to add
(`binance_card_usdt_usd_list`) once the calculator defines its id; see
[Card price gap](#card-price-gap).

## How a value gets published

1. **Fetch** from the series' first source, with a 10 s timeout and two retries on network
   errors and 5xx. If the source fails, the next one in the list is tried.
2. **Parse** strictly: an HTML page, an error body, a missing field or a field in another
   format is rejected.
3. **Check** the value: one the calculator accepts, in a plausible range, and not stale. A
   market's value only ages while it is open: Friday's closing MEP is not stale on Saturday.
4. **Cross-check** it with the series' control source, a second source read every run and
   never published.
5. **Hold back** a value that jumped more than the series allows (5 %, 2 % for P2P) from the
   last published one, or that the control disagrees with by more than 1.5 %. The previous
   value stays published, marked `pending_confirmation`, until the value confirms itself:
   - a jump, at once if the control agrees, or when the next two readings jumped the same
     way (the market moved, even if it keeps moving);
   - a disagreement, when the next two readings also disagree with the control and stay
     within the series' jump limit of it (the value persists).

   A real move shows within two more runs; a one-off glitch never does. Held-back values
   expire after three intervals, so an old one cannot confirm a new jump after an outage.

Every attempt is stored with its outcome and reason, so the history shows the failures too.

## Card price gap

Binance lists a price for buying USDT with a card, but the final screen gives less: on
2026-09-25, 10 USD bought 9.35393217 USDT against 9.7763 listed, 4.3 % less. The final price
is only shown to a logged-in user, so the gap is estimated from observed pairs, written down by
hand in [`data/binance_card_quotes.csv`](data/binance_card_quotes.csv):

| Column | Meaning |
|---|---|
| `observed_at` | when, ISO 8601 with its time zone, e.g. `2026-09-25T15:10:00-03:00` |
| `fiat_amount_usd` | the amount paid, in USD |
| `list_usdt` | the USDT the payment-method list promised for that amount |
| `final_usdt` | the USDT the final screen gave, after every fee |
| `fee_usd` | the fee the final screen showed, for the record |
| `note` | optional |

The published gap is the median of `1 - final_usdt / list_usdt` over every row, with how many
rows there are and their first and last dates. When a series uses the file (`gap_samples`), it
is checked when the app starts: a row with a naive time, a wrong number of columns or a final
above the list stops it with the line number. The file ships inside the image, so new rows take
a new deploy.

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
    "arq_usd_ars": {
      "value": "1609.79859",
      "as_of": "2026-09-25T21:04:24.971394Z",
      "fetched_at": "2026-09-25T21:04:24.919489Z",
      "source": "arq_usdc_ars_bid",
      "stale": false,
      "pending_confirmation": false,
      "last_attempt_at": "2026-09-25T21:04:24.919489Z",
      "official_source": false,
      "indicative": false,
      "final_price_gap": null
    }
  }
}
```

- Every configured series is listed. `value` is a decimal string; it and `as_of`,
  `fetched_at` and `source` are `null` until a first value is accepted, while
  `last_attempt_at` still shows whether the sources are being asked.
- `stale`: no value, or `as_of` is older than the series allows (open-market time for the MEP).
- `pending_confirmation`: a newer reading is held back and may replace this value soon.
- `official_source: false`: the value comes from an undocumented source.
- `indicative`: a reference price, not what a trade gets; `final_price_gap` (`percent`,
  `samples`, `first`, `last`) says how much less a trade got, when there are observations.

`GET /health` returns 200 when the app can read its table, and 503 when it cannot. Both
endpoints answer 503 when the database is down.

## Running it

With Docker:

```sh
docker compose up --build        # Postgres, migrations, then the app on http://localhost:8000
```

It runs like the server: migrations as the database owner, the app as a role with only the
privileges they grant, from a read-only container limited to 256 MB. `APP_PORT` and `DB_PORT`
change the host ports.

For development, with [uv](https://docs.astral.sh/uv/):

```sh
uv sync
docker compose up -d db          # the database, with the owner and app roles
export MIGRATION_DATABASE_URL=postgresql+psycopg://pipeline:pipeline@localhost:5432/pipeline
export APP_DB_USER=pipeline_app
export DATABASE_URL=postgresql+psycopg://pipeline_app:pipeline_app@localhost:5432/pipeline
uv run alembic upgrade head
uv run uvicorn --factory data_pipeline.api.main:app --reload
```

The roles are created when the `db` volume is first created. After pulling a change to them,
recreate it with `docker compose down -v`.

Settings come from the environment:

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | `postgresql+psycopg://…` |
| `MIGRATION_DATABASE_URL` | `DATABASE_URL` | the database owner, used by Alembic only |
| `APP_DB_USER` | required by Alembic | the role the migrations grant access to (the one in `DATABASE_URL`) |
| `RUN_SCHEDULER` | `true` | `false` serves the API without collecting |
| `CORS_ORIGINS` | none | comma-separated origins allowed to read the API from a browser |
| `SERIES_FILE` | `config/series.yaml` in a checkout | set in the image |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the conventions and how to run the checks.
