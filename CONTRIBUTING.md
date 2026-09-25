# Contributing

These are the conventions for code and reviews in this repo. They are settled; a PR that changes
one should say so in its description.

## Before a PR

```sh
uv run pytest        # needs Docker: integration tests start a Postgres container
uv run mypy
uv run ruff check . && uv run ruff format --check .
```

All of them must pass; CI runs the same commands and builds the image. The parity tests
(`tests/unit/test_parity.py`) need `CUANTO_CUESTA_DIR` pointing at a checkout of
[cuanto-cuesta](https://github.com/Abrunacci/cuanto-cuesta); without it they are skipped, and CI
always runs them.

## Architecture

Packages under `src/data_pipeline/`:

- `core/`: the pipeline's rules. Readings and their outcomes, the checks, the rule that holds back
  and confirms jumps, what a series and a source are. Standard library only (an import test
  enforces it) and no I/O.
- `sources/`: one adapter per source. A source builds its request and parses the response; it
  never does I/O itself, so every source is tested against recorded responses.
- `runner/`: sends requests (timeout and retries live here only), runs a series through its
  sources, and schedules runs. It defines the `Store` protocol it needs.
- `storage/`: Postgres. The schema and the `Store` implementation.
- `api/`: FastAPI, and the composition root. Its `lifespan` creates the engine, the HTTP client,
  the store and the scheduler tasks; there are no module-level instances.
- `config.py`: settings from the environment and the series from `config/series.yaml`.

Dependencies point inwards: everything may import `core`, `core` imports nothing else, and
nothing imports `api`.

## Series and sources

- A series is one value tracked over time. Its `id` is its public name: consumers look it up by
  it, so an id never changes. The cuanto-cuesta rates use the calculator's ids (`RATE_FIELDS` in
  its `frontend/src/calculator/data/routes.ts`); a new one takes the id the calculator defines,
  never one made up here.
- A source's `name` is stored with every observation, so renaming one splits its history.
- Series are declared in `config/series.yaml`: sources in order (the first is the primary, the
  rest are fallbacks), an optional control source, interval, opening hours and checks. Numbers
  there are read as exact decimals, and times are quoted.
- Prefer official, documented endpoints, called with the project's `User-Agent` and well inside
  their published rate limits. A series whose source is undocumented says so with
  `official_source: false`, which the API publishes. Note the docs, the limit and the terms, or
  their absence, in the source's module docstring.
- A source never does I/O. When its answer carries no time, the reading is as of `fetched_at`,
  which `parse` receives.

## Checks

Every attempt is recorded with its outcome; nothing is updated or deleted.

1. The source refuses a response with the wrong shape (`MalformedResponseError`): HTML, an error
   body, a missing field, a field in another format, a naive timestamp. A well-formed answer
   with nothing to price (too few P2P ads) is `NoQuoteError`.
2. The value must be one the calculator accepts: positive, at most 1,000,000, at most 8 decimals.
3. It must be in the series' plausible range, and its timestamp neither older than `max_age` nor
   in the future. For a series with opening hours, only open time counts towards the age.
4. The control source, if any, is read and recorded (status `control`). If it fails, nothing is
   held back.
5. A value more than `max_jump` away from the last accepted one, or more than `control_within`
   (1.5 %) away from the control, is a **suspect**: the last accepted value stays published,
   marked as pending confirmation. It is **confirmed**:
   - a jump: at once when the control agrees, or when the two suspects before it jumped the
     same way;
   - a disagreement with the control: when the two suspects before it were also held back
     for disagreeing and are within `max_jump` of it. Each suspect keeps why it was held back
     (`reason`: `jump` or `disagreement`).

   Suspects older than three intervals expire. A reading back near the last accepted value is
   accepted. These rules are the product owner's; changing them is a product decision.

A failure in 1–3 tries the next source. A suspect does not: the source did answer.

## Money and time

- `Decimal` everywhere, never floats. JSON is parsed with `parse_float=Decimal`, and the API sends
  values as decimal strings in plain notation.
- Values are stored exactly (`NUMERIC` with no scale) and published without trailing zeros.
- Every timestamp is timezone-aware and stored as `timestamptz`. `as_of` is when the source says
  the value is from; `fetched_at` is when we read it. Show `as_of` to people.

## Types

- Make invalid states unrepresentable: an outcome is `Accepted | Suspect | Rejected`, not a status
  string with optional fields. The database mirrors that with check constraints.
- Branch on a union or an enum with `match`. mypy runs with `exhaustive-match`, so a missed case
  is a type error; do not add a catch-all `case _` to silence it.
- `mypy --strict` and ruff must pass. A `type: ignore` or `noqa` always names the error code, and
  needs a comment unless the reason is evident.

## Database

- Two roles, as on the server: the owner runs the migrations (`MIGRATION_DATABASE_URL`), and the
  app connects as a role that owns nothing (`DATABASE_URL`). Every migration that creates a
  table or sequence grants the app role (`APP_DB_USER`) exactly what it needs; today that is
  `SELECT` and `INSERT`, so the database itself keeps the history append-only.
- Alembic migrations in `migrations/`, run with `alembic upgrade head`. `storage/tables.py` must
  match them; an integration test compares the two.
- Rows of a series are ordered by `id`, the order they were recorded in, never by a timestamp: a
  clock correction must not reorder the history.
- `/health` reads the app's own table, so a missing schema or grant fails the deploy's health
  check and triggers the rollback.
- A deploy can roll back the code but never a migration, so every migration must work with the
  code of the release before it: add columns and tables, do not rename or drop in the same
  release. Migrations run with a 5 s `lock_timeout`, so one that would block the running app
  fails instead.
- All pending migrations run in one transaction. To add or change a constraint on a large
  table, add it `NOT VALID` in one release and `VALIDATE` it in a migration of the next one, so
  the table is never scanned under an exclusive lock.

## Tests

- New behaviour ships with tests. Cover the edges: bounds, rounding, invalid input, failures.
- Sources are tested against recorded responses in `tests/sources/fixtures`, never the network.
  Record a fixture with one real call, and say when it was recorded.
- Integration tests run against a real Postgres in Docker (testcontainers), the same image as
  the server, with the app connecting as the limited role.
- Test behaviour, not implementation. A test that would still pass with the code wrong is a bug.

## Language

Code, comments, commits, PR descriptions and the README are in English.

## Git

- One branch and one PR per change (`feat/…`, `fix/…`, `refactor/…`, `chore/…`), from `main`.
- Small commits with an imperative subject that says what changed.
- Review your own diff against `main` (`git diff main...HEAD`) before opening a PR.
- Commits have a single author and no `Co-Authored-By` trailers.
