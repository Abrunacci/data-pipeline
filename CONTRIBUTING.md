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
  rest are fallbacks), interval and checks. Numbers there are read as exact decimals.
- Only official or documented endpoints unless the plan says otherwise, called with the
  project's `User-Agent` and well inside their published rate limits. Note the docs and the limit
  in the source's module docstring.

## Checks

Every attempt is recorded with its outcome; nothing is updated or deleted.

1. The source refuses a response with the wrong shape (`MalformedResponseError`): HTML, an error
   body, a missing field, a naive timestamp.
2. The value must be one the calculator accepts: positive, at most 1,000,000, at most 8 decimals.
3. It must be in the series' plausible range, and its timestamp neither older than `max_age` nor
   in the future.
4. A value more than `max_jump` away from the last accepted one is a **suspect**: the last
   accepted value stays published, marked as pending confirmation. It is **confirmed** when the
   next two readings stay within `confirm_within` of it. A reading back near the last accepted
   value is accepted.

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

- Alembic migrations in `migrations/`, run with `alembic upgrade head` (`MIGRATION_DATABASE_URL`,
  or `DATABASE_URL`). `storage/tables.py` must match them.
- A deploy can roll back the code but never a migration, so every migration must work with the
  code of the release before it: add columns and tables, do not rename or drop in the same
  release.

## Tests

- New behaviour ships with tests. Cover the edges: bounds, rounding, invalid input, failures.
- Sources are tested against recorded responses in `tests/sources/fixtures`, never the network.
  Record a fixture with one real call, and say when it was recorded.
- Integration tests run against a real Postgres in Docker (testcontainers).
- Test behaviour, not implementation. A test that would still pass with the code wrong is a bug.

## Language

Code, comments, commits, PR descriptions and the README are in English.

## Git

- One branch and one PR per change (`feat/…`, `fix/…`, `refactor/…`, `chore/…`), from `main`.
- Small commits with an imperative subject that says what changed.
- Review your own diff against `main` (`git diff main...HEAD`) before opening a PR.
- Commits have a single author and no `Co-Authored-By` trailers.
