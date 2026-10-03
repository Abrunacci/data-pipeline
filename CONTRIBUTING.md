# Contributing

These are the conventions for code and reviews in this repo. They are settled; a PR that changes
one should say so in its description.

## Before a PR

```sh
uv run pytest
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
  sources, and schedules runs. It defines the `SeriesStore` and `Destination` protocols it needs,
  keeps the state in memory (`memory.py`), and is the entry point of the image
  (`python -m data_pipeline.runner`).
- `destinations/`: one module per app the runner feeds, with the contract that app owns. It
  imports only `core` (a test enforces it).
- `config.py`: settings from the environment and the series from `config/series.yaml`.

Dependencies point inwards: everything may import `core`, and `core` imports nothing else.
`runner/__main__.py` is the composition root: it creates the HTTP client, the state, the
destination and the scheduler tasks; there are no module-level instances.

## Series and sources

- A series is one value tracked over time. Its `id` is its public name: consumers look it up by
  it, so an id never changes. The cuanto-cuesta rates use the calculator's ids (`RATE_FIELDS` in
  its `frontend/src/calculator/data/routes.ts`); a new one takes the id the calculator defines,
  never one made up here.
- A source's `name` is logged with every attempt and sent as the value's `source`, so renaming
  one changes what the app shows.
- Series are declared in `config/series.yaml`: sources in order (the first is the primary, the
  rest are fallbacks), an optional control source, interval, opening hours and checks. Numbers
  there are read as exact decimals, and times are quoted.
- Prefer official, documented endpoints, called with the project's `User-Agent` and well inside
  their published rate limits. A series whose source is undocumented says so with
  `official_source: false`. Note the docs, the limit and the terms, or
  their absence, in the source's module docstring.
- A source never does I/O. When its answer carries no time, the reading is as of `fetched_at`,
  which `parse` receives.

## Checks

Every attempt is logged with its outcome.

1. The source refuses a response with the wrong shape (`MalformedResponseError`): HTML, an error
   body, a missing field, a field in another format, a naive timestamp. A well-formed answer
   with nothing to price (too few P2P ads) is `NoQuoteError`.
2. The value must be one the calculator accepts: positive, at most 1,000,000, at most 8 decimals.
3. It must be in the series' plausible range, and its timestamp neither older than `max_age` nor
   in the future. For a series with opening hours, only open time counts towards the age.
4. The control source, if any, is read and logged. If it fails, nothing is
   held back.
5. A value more than `max_jump` away from the last accepted one, or more than `control_within`
   (1.5 %) away from the control, is a **suspect**: it is not sent. It is **confirmed**:
   - a jump: at once when the control agrees, or when the two suspects before it jumped the
     same way;
   - a disagreement with the control: when the two suspects before it were also held back
     for disagreeing and are within `max_jump` of it. Each suspect keeps why it was held back
     (`jump` or `disagreement`).

   Suspects older than three intervals expire. A reading back near the last accepted value is
   accepted. These rules are the product owner's; changing them is a product decision.

A failure in 1–3 tries the next source. A suspect does not: the source did answer.

## Money and time

- `Decimal` everywhere, never floats. JSON is parsed with `parse_float=Decimal`, and values are
  sent as decimal strings in plain notation, without trailing zeros.
- Every timestamp is timezone-aware. `as_of` is when the source says the value is from (sent as
  `observed_at`); `fetched_at` is when we read it.

## Types

- Make invalid states unrepresentable: an outcome is `Accepted | Suspect | Rejected`, not a status
  string with optional fields.
- Branch on a union or an enum with `match`. mypy runs with `exhaustive-match`, so a missed case
  is a type error; do not add a catch-all `case _` to silence it.
- `mypy --strict` and ruff must pass. A `type: ignore` or `noqa` always names the error code, and
  needs a comment unless the reason is evident.

## Health

- The image's `HEALTHCHECK` runs `python -m data_pipeline.runner.heartbeat`, which checks the
  file the runner touches every 30 s. Keep that module standard-library only: it starts on every
  check, and a slow import is a failed check.
- The runner has no database and serves no HTTP. A deploy is kept only if the container turns
  healthy.

## Tests

- New behaviour ships with tests. Cover the edges: bounds, rounding, invalid input, failures.
- Sources are tested against recorded responses in `tests/sources/fixtures`, never the network.
  Record a fixture with one real call, and say when it was recorded.
- Test behaviour, not implementation. A test that would still pass with the code wrong is a bug.

## Language

Code, comments, commits, PR descriptions and the README are in English.

## Git

- One branch and one PR per change (`feat/…`, `fix/…`, `refactor/…`, `chore/…`), from `main`.
- Small commits with an imperative subject that says what changed.
- Review your own diff against `main` (`git diff main...HEAD`) before opening a PR.
- Commits have a single author and no `Co-Authored-By` trailers.
- CI enforces it on every PR with the **Check commit metadata** step, a shared action from
  [infra](https://github.com/Abrunacci/infra/tree/main/.github/actions/check-commit-metadata):
  each commit is authored by the owner and committed by the owner or GitHub, and no commit message
  carries a `Co-Authored-By` trailer, a "Generated with/by" line, a "Requested by … thread" line
  or a blocked link. It checks commits only; the PR description is reviewed by hand.
