# Plan: from API with a database to a runner that pushes

Status: proposed, 2026-10-01. No code for these steps until it is approved.

## Where it goes

Today the image runs a FastAPI app with its own Postgres: it stores every attempt and serves
`/v1/rates/latest`. Nobody reads that API: the cuanto-cuesta calculator has the user type the five
rates on each visit. So there is nothing to keep in sync and nothing to cut over.

The target is a runner with no database and no HTTP of its own:

- It reads and checks each series as it does today, with the same rules (`core/`), and keeps what
  the checks need (last accepted value, held-back suspects) in memory.
- Each **accepted** value goes to cuanto-cuesta's ingest API (`Authorization: Bearer`, over the
  server's internal network, not the public proxy). cuanto-cuesta keeps one field per rate,
  overwritten by each new value, with no history.
- Failed, suspect and control readings go to the runner's log only.
- On start, it asks cuanto-cuesta for the current value of each rate, so the jump check has a
  previous value. A restart loses the suspects being held; that is accepted.
- If cuanto-cuesta does not answer, the value is logged and dropped. No queue: the next run sends
  a newer one.
- For `binance_card_usd_usdt` it sends the listed `price` and `estimated_final`, or only `price`
  when there is no estimate. The gap and its CSV stay here.
- Fees stay manual in cuanto-cuesta; this runner sends none.

The contract is cuanto-cuesta's (its PR 1). The starting point is:

```json
{"batch_id": "...", "rates": [{"key": "bitso_usdt_ars", "base": "USDT", "quote": "ARS",
  "price": "1452.30", "source": "bitso_api", "source_url": "https://...",
  "observed_at": "2026-09-30T15:00:00Z"}]}
```

with an `estimated_final` next to `price` for the card. Amounts are decimal strings, errors are
codes.

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
- State: starts empty on every restart until step 3. With no previous value, the first valid
  reading of each series is accepted as it is (`decide` in `core/checks.py`): only the
  per-reading checks apply (accepted by the calculator, plausible range, not stale).
- cuanto-cuesta: nothing.
- infra: nothing new. The entry in `projects.yml` already has no database, port or health path,
  and health comes from the image's `HEALTHCHECK`. The first deploy needs infra's deploy key
  (Abrunacci/infra#40) merged and the playbook run, and this repo's deploy job.

### 2. HTTP destination to cuanto-cuesta

A second destination posts each batch to cuanto-cuesta's ingest endpoint. Which one runs is
configuration (`DESTINATION=log|http`), so going back to logging is an environment change.

- Usable: cuanto-cuesta gets live values.
- Undo: `DESTINATION=log`, or revert.
- cuanto-cuesta: PR 1 merged (endpoint, contract, error codes, token check) and its backend
  deployed. Today it is a static site with no backend.
- infra:
  - a network path from this container to cuanto-cuesta's backend. Today an internal backend
    is only on its own `outbound` network, and nothing else can reach it or be reached from it;
  - a manual secret for this project (the token, for example `CUANTO_CUESTA_TOKEN`) and its
    counterpart on cuanto-cuesta's side;
  - the endpoint's internal URL as a non-secret `env` value.

### 3. State read from cuanto-cuesta on start

Before the first run, the runner asks cuanto-cuesta for the current value of each rate and uses
them as the last accepted values.

- Usable: a restart no longer accepts a first reading that jumped.
- Undo: revert; the runner starts empty again, as in steps 1 and 2.
- cuanto-cuesta: a read endpoint for the current values, with the same token. It is not in the
  starting contract, so it has to be added to its PR 1 or a later one.
- infra: nothing beyond step 2.

### 4. Remove the database and the API

Delete the API (`api/`), `/health`, `EXPOSE 8000`, `storage/`, the migrations and `alembic.ini`,
Postgres in `compose.yml`, the `DATABASE_URL` setting, the MEP history loader and its source, the
Postgres integration tests, and the dependencies only they use (FastAPI, uvicorn, SQLAlchemy,
psycopg, Alembic, testcontainers). README and CONTRIBUTING describe the runner.

- Before: a final `pg_dump` of any database this app has written to, kept outside the repo. No
  history is copied anywhere and the MEP is not backfilled.
- Usable: a smaller image with a single job.
- Undo: revert the commit. The data is in the dump.
- cuanto-cuesta: nothing.
- infra: nothing. The server never had a database for this project.

## Open questions

1. **What gets sent.** Every accepted reading (each run, even when the value did not change,
   which also tells cuanto-cuesta the value is fresh), or only readings whose value changed.
2. **cuanto-cuesta down when the runner starts (step 3).** Start empty and log it, retry for a
   while and then start empty, or run no series until it answers.
3. **First deploy.** The current image cannot start on the server: it requires `DATABASE_URL`,
   and the project has no database there. The deploy job waits for step 1, is merged now but run
   by hand only, or is merged now and its first runs fail until step 1.
4. **Final dump.** Which database, if any, holds data worth the `pg_dump` (a local Compose
   volume, another server).

## Not in this plan

- CriptoYa's `totalBid` and `/api/fees` for the fees it covers: an idea for later.
- Other processes (race results for a game): each will be a new set of sources and a
  destination, added in this repo.
