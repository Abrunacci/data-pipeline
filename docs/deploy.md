# Deploying to the server

How the runner gets to production, what the server gives it and expects from it, and what to
look at when something fails. The server side is managed in the
[infra repository](https://github.com/Abrunacci/infra/blob/main/ansible/README.md) ("Internal
backends", "Links between backends", "Deploying a backend", "Backend secrets", "Backend status
and rollbacks", "Backend logs", "Status page and alerts"); this page covers what a reader of this
repository needs, and links there for the rest.

## What runs

| | |
|---|---|
| Kind | internal backend: one container with its scheduler; no domain, no site, no database |
| Image | `ghcr.io/abrunacci/data-pipeline` (public package) |
| Reachable from outside | no: no DNS name, no proxy route, no published port |
| Health | the image's Docker `HEALTHCHECK` |
| Memory | 256 MB, no swap |
| Calls | cuanto-cuesta's `POST /api/ingest`, with a bearer token, over an internal network |
| Entry in infra | `projects.yml`, project `data-pipeline` |

Series are defined here, in `config/series.yaml` (see [CONTRIBUTING.md](../CONTRIBUTING.md)).
Adding one needs no change in infra, unless it needs a new variable or has to reach another
project's backend (see [Variables](#variables-the-container-receives)).

## How a deploy works

One workflow, `CI` (`.github/workflows/ci.yml`), does everything:

| Job | Runs on | What it does |
|---|---|---|
| `check` | every push, PR and the weekly run | commit metadata, tests, mypy, ruff |
| `image` | push and PR | builds the image and runs it the way the server does (read-only, uid 10005, no capabilities, no network), until Docker reports it healthy |
| `publish` | push to `main` only | builds the image again and pushes it to GHCR |
| `deploy` | push to `main` only, after `publish` | asks the server to switch to that image |

`check` and `image` are the required checks of `main`'s ruleset. `image` never writes to the
registry: only `publish`, which only exists on `main`, gets `packages: write`, and it logs in to
`ghcr.io` with the job's own token.

**The image.** `publish` tags the image with the commit SHA (there is no `latest`), pushes it,
and reads its digest back from the registry (`docker inspect` on `RepoDigests`). It checks the
digest is `sha256:` and 64 hex characters and passes it on as the job's `digest` output. The
server deploys by digest, so moving a tag later changes nothing.

**The deploy.** `deploy` connects over SSH and runs a single command:

```sh
ssh -i <key> -p "$DEPLOY_PORT" -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes \
  -o BatchMode=yes -o ConnectTimeout=15 "$DEPLOY_USER@$DEPLOY_HOST" \
  deploy-backend "$GITHUB_SHA" "$DIGEST" "$GITHUB_RUN_ID" </dev/null
```

On the server the deploy key is tied to a forced command: it can only deploy this project, with
no shell and no forwarding. CI only chooses which commit and which digest. The server then:

1. takes the image repository from its own `projects.yml` (never from the client) and pulls
   `ghcr.io/abrunacci/data-pipeline@<digest>`;
2. recreates the container with it;
3. waits up to 60 seconds for Docker to report it `healthy`; an image without a `HEALTHCHECK`, or
   one already `unhealthy`, fails at once;
4. if it does not turn healthy (or stops, or restarts), puts back the previous release and fails
   the job; on the very first deploy there is nothing to go back to, so the backend is stopped;
5. prints `Deployed backend data-pipeline release <id> (<digest>)`.

Only one deploy runs at a time (the job's `deploy-production` concurrency group), and the
`production` environment needs the owner's approval.

### GitHub settings

The `production` environment, limited to `main`, holds everything the deploy job reads. None of
it is in the repository, which is public:

| Name | Kind | |
|---|---|---|
| `DEPLOY_SSH_KEY` | secret | the private deploy key |
| `DEPLOY_KNOWN_HOSTS` | secret | the server's host key line; `[host]:port` when the port is not 22 |
| `DEPLOY_HOST` | variable | the server's name |
| `DEPLOY_PORT` | variable | its SSH port |
| `DEPLOY_USER` | variable | the user the deploy key is restricted to |

The job stops with `<NAME> is empty: set it in the production environment` when one is missing.
`StrictHostKeyChecking=yes` means only the server in `DEPLOY_KNOWN_HOSTS` is trusted; it is never
turned off.

### Old images in GHCR

Nothing deletes old versions of the package. The server keeps the images of its last 5 releases
locally, plus the one that ran before the last deploy, and a rollback or a rebuilt server pulls
them from GHCR again, so none of them may be missing there. The package is public, so its storage
costs nothing, and keeping every version is the simplest way to honour that. If a cleanup is ever
added, it must keep at least the 10 newest versions. On the server,
`sudo backend-rollback data-pipeline --list` shows which ones it may need.

## The health check

It is the only signal of whether the runner is alive, for the deploy and for the status page.
The runner touches `/tmp/alive` every 30 seconds, and `python -m data_pipeline.runner.heartbeat`
fails when the file is missing or older than 2 minutes (`src/data_pipeline/runner/heartbeat.py`).
It proves the event loop runs, not that sources or cuanto-cuesta answer: those failures are in
the log, and a failing series never makes the container unhealthy.

```
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --start-interval=2s --retries=3
```

`--start-interval=2s` checks every 2 seconds while the container starts, so it turns healthy a
few seconds after the first beat, well inside the deploy's 60 seconds. Docker does not restart an
unhealthy container: it only marks it, and the status page alerts. The `Dockerfile` explains each
option.

## Variables the container receives

None is configured in this repository: the server provides them, and the list of names lives in
infra's `projects.yml`. Adding, removing or renaming one is a PR in infra.

| Name | Kind | Value |
|---|---|---|
| `CUANTO_CUESTA_INGEST_URL` | public | `http://cuanto-cuesta-backend:8000/api/ingest` |
| `CUANTO_CUESTA_INGEST_TOKEN` | secret | a copy of cuanto-cuesta's `INGEST_TOKEN`, written by infra |

With both set, every accepted value is posted there with `Authorization: Bearer <token>`. With
either missing, the runner only logs each batch, and says so once when it starts. The URL is
logged at start; the token never is. `SERIES_FILE` is set in the image and `CONTACT_URL` has a
default (see the [README](../README.md#the-runner)), so the server sets neither.

- **The token is not changed here.** Its only source is cuanto-cuesta. Rotating it
  (`sudo project-secret cuanto-cuesta rotate INGEST_TOKEN`) updates the copy and restarts both
  backends together. A deploy refuses to start when the copy is missing or out of date.
- **Reaching another project** (a destination other than cuanto-cuesta) needs a PR in infra first:
  it declares the secret on the receiving side and its copy here, and that creates the
  `link-<project>` network. Without it the runner cannot see the other backend. It goes in before
  the deploy of the image that uses it.

## What the server expects from the container

- **Fixed user:** uid/gid 10005, whatever the image says; never root. The image's own user has the
  same uid.
- **Read-only file system** except `/tmp`, a tmpfs that is lost on every restart or deploy. The
  runner keeps no state, so it loses nothing: the heartbeat file is all it writes.
- **No capabilities**, `no-new-privileges`, at most 256 processes.
- **Networks:** `backend-data-pipeline_outbound`, its own, to reach the sources on the internet;
  and `link-cuanto-cuesta`, internal (no internet), where cuanto-cuesta's backend answers as
  `cuanto-cuesta-backend:8000`. It sees nothing else: not the proxy, not Postgres, not other
  backends. From the internet `/api/ingest` answers 404; it only exists through the link.
- **Sources it reaches:** `dolarapi.com`, `www.binance.com`, `api.bitso.com`,
  `api.arqfinance.com` and `criptoya.com`; the series and their schedule are in the
  [README](../README.md#what-it-collects).
- **Shutdown:** SIGTERM, then 20 seconds before it is killed. The runner stops at once: a run in
  progress is cancelled and its value is lost. On start every series runs immediately (when inside
  its hours), so a deploy leaves a gap of about the restart's length.
- **Restarts:** `restart: unless-stopped` when the process exits; a hung process is not restarted,
  only reported unhealthy.
- **Logs:** stdout and stderr, into the server's journal tagged `backend.data-pipeline` (see
  [Logs on the server](#logs-on-the-server)). The journal takes at most 10,000 lines every 30
  seconds across all containers, so never log per row: the runner logs one line per attempt and
  one per item cuanto-cuesta answers for.
- **Memory:** 256 MB, no swap; over it, the kernel kills the process.

## When cuanto-cuesta does not take a value

Nothing is retried and nothing is queued: a value that does not get through is logged and
dropped, and the series' next run (10 or 15 minutes later) sends a newer one. This is a decision,
not a gap: the runner keeps no state, and an old value resent later is worth less than the next
reading. The cost is bounded: during a token rotation, when both backends restart, at most one
value per series is lost.

What the log says (`data_pipeline.destinations.cuanto_cuesta`):

| Line | Meaning |
|---|---|
| `… stored` / `… unchanged` | taken |
| `… older than the current quote, ignored` | cuanto-cuesta already holds a newer one |
| `… rejected, <code>` | cuanto-cuesta refused that item; the code is its own (for example `jump`, a reading it holds back because it moved too much). Any code is logged as it comes |
| `… dropped: CONFIGURATION: the token was refused (HTTP 401 …)` | the token copy is wrong or missing; no later run fixes it |
| `… dropped: the batch was refused (HTTP 400/413/422 …)` | a batch this runner built wrong: a bug here |
| `… dropped: no answer …` / `unexpected answer …` | cuanto-cuesta is down, or the URL is wrong |

Fetching from the sources is different: each request is retried twice (after 1 and 3 seconds) on
network errors and 5xx, and then the series' next source is tried.

## When something fails

**In the deploy job** (the message appears as is in the job's log):

| Message | What happened | What to do |
|---|---|---|
| `<NAME> is empty: set it in the production environment` | a secret or variable is missing | set it in the `production` environment |
| `Permission denied (publickey)` | the CI key is not the project's deploy key in infra | check the secret; a new key is a PR in infra |
| `Host key verification failed` | `DEPLOY_KNOWN_HOSTS` does not match the server | regenerate it (infra's guide) |
| `deploy-backend: the backend's secrets are not ready: …` | the token copy is missing or out of date | on the server: `sudo project-secret data-pipeline list`, then `sudo project-secret data-pipeline sync` |
| `deploy-backend: cannot pull …: unauthorized` | the server cannot pull the image | check the package is still public |
| `deploy-backend: release … not healthy within 60s (…); back to release …, which is healthy` | the new image did not turn healthy | read the failed release's last lines under `backend-log` (see [Logs on the server](#logs-on-the-server)); they never go to CI |
| `… there is no earlier release, so the backend was stopped` | the first deploy failed | the same |
| `another deploy, rollback or secret change of data-pipeline is running` | two deploys at once, or a token rotation in progress | run the job again |

**Outside CI:**

- The status page cannot query the runner over HTTP. Every 5 minutes the server sends it a beat,
  "data-pipeline / Health", which fails when the container is not running or not healthy; two
  failures in a row (10 minutes) send an email. If the beats stop, it fails after 15 minutes.
- A series that fails (a source down, cuanto-cuesta down, a 401) does not change the health
  check: it only shows in the log.
- On the server: `sudo backend-status data-pipeline`, `sudo journalctl -t deploy-backend` for
  what each deploy did, and `sudo backend-rollback data-pipeline` to go back a release.

### Logs on the server

Two journal tags carry this project's logs, and only the owner can read them on the server:

| Tag | What it holds | When it appears | Command |
|---|---|---|---|
| `backend.data-pipeline` | everything the running container writes to stdout and stderr | always, for the release that is running | `sudo journalctl -t backend.data-pipeline --since -1h` |
| `backend-log` | the last 30 lines of a release that did not turn healthy | only when a deploy fails and `deploy-backend` puts back the previous release (or stops the backend on a first deploy) | `sudo journalctl -t backend-log --since -1h` |

They are not two names for the same thing: the runner writes `backend.data-pipeline`, and
`deploy-backend` writes `backend-log`, only for a release it rejected. After a failed deploy, look
at `backend-log` for why the new release failed, and at `backend.data-pipeline` for the release
that runs again. `sudo docker logs -f backend-data-pipeline-backend-1` follows the current
container only.
