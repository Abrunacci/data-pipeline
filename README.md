# data-pipeline

A runner that reads data on a schedule, checks it, and sends each accepted value to the app that
uses it. Today it collects the exchange rates the
[cuanto-cuesta](https://cuanto-cuesta.abrunacci.dev) calculator needs, and it is meant to take
other datasets later.

It has no database and serves no HTTP: it logs every attempt, and the app it feeds stores the
values. It runs in one container, whose Docker `HEALTHCHECK` says whether it is alive.

## The runner

```sh
uv run python -m data_pipeline.runner           # until SIGTERM or Ctrl-C
uv run python -m data_pipeline.runner --check   # load the configuration and exit
docker compose up --build                       # the image, as the server runs it
```

For development, install [uv](https://docs.astral.sh/uv/) and run `uv sync` first. Settings come
from the environment, and none is required:

| Variable | Default | |
|---|---|---|
| `CUANTO_CUESTA_INGEST_URL` | none | cuanto-cuesta's `POST /api/ingest`, on the internal network |
| `CUANTO_CUESTA_INGEST_TOKEN` | none | its bearer token, cuanto-cuesta's `INGEST_TOKEN`. Without it or the URL, batches are only logged |
| `SERIES_FILE` | `config/series.yaml` in a checkout | set in the image |
| `CONTACT_URL` | this repository | sent in the `User-Agent` |

One line per attempt, and one per item cuanto-cuesta answers for, for example:

```
INFO data_pipeline.runner.memory: bitso_usdt_ars bitso_usdt_ars_bid accepted 1615.3 (checked)
INFO data_pipeline.destinations.cuanto_cuesta: cuanto-cuesta batch 90a44048-… (bitso_usdt_ars): bitso_usdt_ars stored
```

- It starts with no state, so the first accepted value of each series after a start has nothing
  to check a jump against: it still passes the per-reading checks (plausible range, not stale),
  and its line says `no reference`.
- Held-back, control and rejected readings are logged, never sent.
- `binance_card_usd_usdt` always carries `estimated_final`, `null` when there is no estimate or
  cuanto-cuesta would refuse it; no other rate carries it.

## What it collects

| Series | Value | Source → control | Every |
|---|---|---|---|
| `mep` | ARS paid for each USD sold through the MEP (buy side) | [DolarApi](https://dolarapi.com/docs/argentina/operations/get-dolar-bolsa.html) → Ámbito | 15 min, weekdays 10:45–17:30 Buenos Aires |
| `binance_p2p_usdt_usd` | USD paid for each USDT on Binance P2P: median of the 5 cheapest merchant ads that take 500 USD, from merchants with 95 % of orders completed | [Binance P2P public API](https://www.binance.com/en/skills/detail/binance/p2p) | 10 min |
| `bitso_usdt_ars` | ARS paid for each USDT sold on Bitso (best bid) | [Bitso public API](https://docs.bitso.com/bitso-api/docs/ticker) → CriptoYa | 10 min |
| `binance_card_usd_usdt` | USDT Binance lists for each USD paid with a card: **indicative**, sent with `estimated_final` | [Binance fiat public API](https://www.binance.com/en/skills/detail/binance/fiat) | 10 min |
| `arq_usd_ars` | ARS ARQ pays for each USDc, at par with USD (its bid) | ARQ's ticker (undocumented, so `official_source: false`), CriptoYa as fallback → CriptoYa | 10 min |

The series ids are the ones the calculator uses. For the card price, see
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
   last published one, or that the control disagrees with by more than 1.5 %. Nothing is sent
   until the value confirms itself:
   - a jump, at once if the control agrees, or when the next two readings jumped the same
     way (the market moved, even if it keeps moving);
   - a disagreement, when the next two readings also disagree with the control and stay
     within the series' jump limit of it (the value persists).

   A real move shows within two more runs; a one-off glitch never does. Held-back values
   expire after three intervals, so an old one cannot confirm a new jump after an outage.

Every attempt is logged with its outcome and reason, failures included.

## Card price gap

Binance lists a price for buying USDT with a card, but the final screen gives less: on
2026-09-25, 10 USD bought 9.35393217 USDT against 9.7763382 listed. Part of that is the fee
(0.20 USD, which the calculator charges on its own as `binance_card_purchase`); the rest, 2.37 %,
is a worse price. The final price is only shown to a logged-in user, so that price gap is
estimated from observed pairs, written down by hand in
[`data/binance_card_quotes.csv`](data/binance_card_quotes.csv):

| Column | Meaning |
|---|---|
| `observed_at` | when, ISO 8601 with its time zone, e.g. `2026-09-25T15:10:00-03:00` |
| `fiat_amount_usd` | the amount paid, in USD |
| `list_usdt` | the USDT the payment-method list promised for that amount |
| `final_usdt` | the USDT the final screen gave, after every fee |
| `fee_usd` | the fee the final screen showed, in USD |
| `note` | optional |

The published gap is the median over every row of the price gap, fee aside:
`1 - (final_usdt / (fiat_amount_usd - fee_usd)) / (list_usdt / fiat_amount_usd)`, with how many
rows there are and their first and last dates. The runner sends `estimated_final`, the final
price, fee aside, that the gap predicts for the current listed price: `value * (1 - gap)`,
computed exactly and rounded down to 8 decimals. It is what the calculator's card price field
asks for. When a series uses the file (`gap_samples`), it is checked when the runner starts: a row with a naive time, a wrong number of columns, a fee
below 0 or not below the amount, or a final price (fee aside) better than the listed one stops
it with the line number. The file ships inside the image, so new rows take
a new deploy.

## Why not Airflow

This started as an Airflow and Spark sandbox. For a handful of values every few minutes on a
2 GB server, both are the wrong tool: Airflow's scheduler, API server and metadata database
need more memory than the server has to spare, and Spark has nothing to distribute. Here the
scheduler is one asyncio task per series in a single process. What Airflow would give is built
in:

- runs are aligned to the clock, and a slot that already has an attempt is skipped;
- a slow run and the next slot never overlap;
- retries are per request, and a failed run is logged and the next slot runs anyway.

## Health

The image has a Docker `HEALTHCHECK` that does not go through HTTP: the process touches
`/tmp/alive` every 30 seconds, and the check fails when the file is missing or older than 2
minutes. See `docker ps` or `docker inspect --format '{{json .State.Health}}' <container>`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the conventions and how to run the checks.

## Deploying

CI deploys what lands on `main`: it pushes the image to `ghcr.io/abrunacci/data-pipeline`, then
asks the server, over SSH, to switch to it by digest. The server keeps the new container only if
its `HEALTHCHECK` turns healthy, and otherwise puts back the previous release. The server side
(the project's entry, the deploy key, rollbacks) lives in the infra repository.

The `production` environment, limited to `main`, holds everything the deploy job reads. None of it
is in the repository:

| Name | Kind | |
|---|---|---|
| `DEPLOY_SSH_KEY` | secret | the private deploy key |
| `DEPLOY_KNOWN_HOSTS` | secret | the server's host key line; `[host]:port` when the port is not 22 |
| `DEPLOY_HOST` | variable | the server's name |
| `DEPLOY_PORT` | variable | its SSH port |
| `DEPLOY_USER` | variable | the user the deploy key is restricted to |
