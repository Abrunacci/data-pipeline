# Plan: from API with a database to a runner that pushes

Status: approved 2026-10-02, with cuanto-cuesta's contract closed the same day.

## Where it goes

Today the image runs a FastAPI app with its own Postgres: it stores every attempt and serves
`/v1/rates/latest`. Nobody reads that API: the cuanto-cuesta calculator has the user type the five
rates on each visit. So there is nothing to keep in sync and nothing to cut over.

The target is a runner with no database and no HTTP of its own:

- It is stateless: it captures each reading, checks it on its own (accepted by the calculator,
  plausible range, not stale), formats it and delivers it. It keeps nothing between runs, so it
  does not compare a reading with earlier ones or with a second source (decided 2026-10-03: the
  jump rule and the control check were removed). Checking a value against what the app already
  holds is the app's job, in its own backend, which has the history.
- Each **accepted** reading goes to cuanto-cuesta's ingest API (`Authorization: Bearer`, over
  the server's internal network, not the public proxy), every run, even when the value did not
  change: its `observed_at` keeps the rate fresh in cuanto-cuesta, which otherwise would flag a
  stable value as possibly old. cuanto-cuesta keeps one field per rate, overwritten by each new
  value, with no history.
- Rejected readings go to the runner's log only. It reads nothing back from cuanto-cuesta.
- If cuanto-cuesta does not answer, the value is logged and dropped. No queue: the next run sends
  a newer one.
- For `binance_card_usd_usdt` it sends the listed `price` and always `estimated_final`, which is
  `null` when there is no estimate (cuanto-cuesta rejects a card item without it). No other key
  carries `estimated_final`. The gap and its CSV stay here.
- Fees stay manual in cuanto-cuesta; this runner sends none.

The contract is cuanto-cuesta's, closed in its PR 1 (`backend/README.md`, "API", on its branch
`feat/rates-backend`). What the runner has to respect:

```json
{"batch_id": "<UUID>", "rates": [
  {"key": "bitso_usdt_ars", "base": "USDT", "quote": "ARS", "price": "1452.30",
   "source": "bitso_usdt_ars_bid", "source_url": "https://...", "observed_at": "2026-10-01T15:00:00Z"},
  {"key": "binance_card_usd_usdt", "base": "USD", "quote": "USDT", "price": "0.9850",
   "estimated_final": null, "source": "...", "source_url": "https://...", "observed_at": "..."}]}
```

- `POST /api/ingest`, with `Authorization: Bearer`, on the internal network only.
- `base` and `quote` are fixed per key (`mep` USD/ARS, `binance_p2p_usdt_usd` USDT/USD,
  `bitso_usdt_ars` USDT/ARS, `arq_usd_ars` USD/ARS, `binance_card_usd_usdt` USD/USDT); an
  inverted pair rejects the item.
- `price` and `estimated_final` are positive decimal strings with up to 10 places, no exponent;
  `source` is `[a-z0-9_]{1,64}`; `source_url` is `https` or `null`; `observed_at` is RFC 3339
  with a zone. Any other field rejects the item.
- At most 20 items and 64 KB per request. One batch per series run is one item (two values for
  the card, in the same item), far under both.
- `200` with one result per item: `stored`, `unchanged`, `older`, or `rejected` with an `error`
  code. The whole request is refused with `401`, `400`, `422` or `413`.
- `GET /api/rates` is public (no token) and returns the current quote of each rate that has one,
  and `server_time`.

## Steps

Each step is merged and deployed on its own, works end to end, and is undone by reverting it.

### 1. Runner with a log destination

A new entry point (`python -m data_pipeline.runner`) runs the scheduler with an in-memory store
and a pluggable **destination**. The first destination builds the payload above, one batch per
series run, and logs it as one JSON line. `estimated_final` moves from the API's response to the
payload builder. The image's `CMD` switches to the runner; the API, Postgres and migrations stay in
the code, unused.

- Usable: the first version that can run on the server, which provides no database for this
  project. Its log shows exactly what would be sent.
- Undo: revert, and the `CMD` goes back to the API.
- State: starts empty on every restart, so the first reading of each series after
  a start is accepted with no reference, as above. Today `decide` (`core/checks.py`) accepts it
  the same way but records nothing that says so; this step adds that to the log.
- cuanto-cuesta: nothing.
- infra: nothing new. The entry in `projects.yml` already has no database, port or health path,
  and health comes from the image's `HEALTHCHECK`. The first deploy needs infra's deploy key
  (Abrunacci/infra#40) merged and the playbook run, and this repo's deploy job (#9), which is
  merged right after this step.
- GitHub: after the first publish, the package `ghcr.io/abrunacci/data-pipeline` is made public,
  so the server pulls it with no credentials.

### 2. HTTP destination to cuanto-cuesta

A second destination posts each batch to cuanto-cuesta's ingest endpoint. It runs when both
`CUANTO_CUESTA_INGEST_URL` and `CUANTO_CUESTA_INGEST_TOKEN` are set; with either missing, the
runner logs the batches as in step 1 and says so once at start. So the deploy does not depend on
the order infra applies its part in, and going back to logging is an environment change.

- Usable: cuanto-cuesta gets live values.
- Undo: unset either variable, or revert.
- Logs: each item's result; a `rejected` item with its `error` code, and a refused request with
  its status and code. A `401` is a wrong token, a configuration problem, and its line says
  `CONFIGURATION`. Nothing is retried; a value cuanto-cuesta does not answer for is dropped.
- cuanto-cuesta: PR 1 merged and its backend deployed. Today it is a static site with no backend.
- infra:
  - a network path from this container to cuanto-cuesta's backend. Today an internal backend
    is only on its own `outbound` network, and nothing else can reach it or be reached from it;
  - a manual secret for this project, `CUANTO_CUESTA_INGEST_TOKEN`. It holds the same value as
    cuanto-cuesta's `INGEST_TOKEN`, which infra generates; it is named after its destination
    because this runner will feed other apps;
  - the endpoint's internal URL as a non-secret `env` value, `CUANTO_CUESTA_INGEST_URL`.

### 3. Remove the database and the API

Delete the API (`api/`), `/health`, `EXPOSE 8000`, `storage/`, the migrations and `alembic.ini`,
Postgres in `compose.yml`, the `DATABASE_URL` setting, the MEP history loader and its source, the
Postgres integration tests, and the dependencies only they use (FastAPI, uvicorn, SQLAlchemy,
psycopg, Alembic, testcontainers). README and CONTRIBUTING describe the runner.

- No data is kept: the server never had a database for this project, and cuanto-cuesta keeps no
  history. No history is copied anywhere and the MEP is not backfilled.
- Usable: a smaller image with a single job.
- Undo: revert the commit.
- cuanto-cuesta: nothing.
- infra: nothing. The server never had a database for this project.

## Not in this plan

- Reading cuanto-cuesta's `GET /api/rates` on start to seed the jump check (dropped 2026-10-02):
  the pipeline does not validate against the app's data; if a value needs checking against the
  current one, cuanto-cuesta's backend does it.
- CriptoYa's `totalBid` and `/api/fees` for the fees it covers: an idea for later.
- Other processes (race results for a game): each will be a new set of sources and a
  destination, added in this repo.
