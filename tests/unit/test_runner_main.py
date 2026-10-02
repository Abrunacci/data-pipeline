from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from data_pipeline.config import ConfigError, RunnerSettings
from data_pipeline.destinations.cuanto_cuesta import CuantoCuestaIngest, CuantoCuestaLog
from data_pipeline.runner.__main__ import configure, main

pytestmark = pytest.mark.anyio

URL = "http://cuanto-cuesta-backend:8000/api/ingest"
TOKEN = "not-a-real-token"
SERIES_IDS = "mep, binance_p2p_usdt_usd, bitso_usdt_ars, arq_usd_ars, binance_card_usd_usdt"


@pytest.fixture(autouse=True)
def _no_cuanto_cuesta(monkeypatch: pytest.MonkeyPatch) -> None:
    # Whatever the shell running the tests has set.
    monkeypatch.delenv("CUANTO_CUESTA_INGEST_URL", raising=False)
    monkeypatch.delenv("CUANTO_CUESTA_INGEST_TOKEN", raising=False)


def test_check_loads_the_repo_configuration(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO", logger="data_pipeline.runner"):
        assert main(["--check"]) == 0
    assert caplog.messages[-1] == f"configuration ok: {SERIES_IDS}"


def test_check_fails_on_a_broken_series_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broken = tmp_path / "series.yaml"
    broken.write_text("series: [{id: mep}]\n")
    monkeypatch.setenv("SERIES_FILE", str(broken))
    with pytest.raises(ConfigError):
        main(["--check"])


async def test_without_url_and_token_it_logs_and_says_so_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with httpx.AsyncClient() as client:
        with caplog.at_level("INFO", logger="data_pipeline.runner"):
            _, _, destination = configure(RunnerSettings(), client)
    assert isinstance(destination, CuantoCuestaLog)
    assert caplog.messages == [
        "CUANTO_CUESTA_INGEST_URL and CUANTO_CUESTA_INGEST_TOKEN not set:"
        " cuanto-cuesta batches are logged, not sent"
    ]


@pytest.mark.parametrize(
    ("variable", "value", "missing"),
    [
        ("CUANTO_CUESTA_INGEST_URL", URL, "CUANTO_CUESTA_INGEST_TOKEN"),
        ("CUANTO_CUESTA_INGEST_TOKEN", TOKEN, "CUANTO_CUESTA_INGEST_URL"),
    ],
)
async def test_with_only_one_of_them_it_still_only_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    variable: str,
    value: str,
    missing: str,
) -> None:
    monkeypatch.setenv(variable, value)
    async with httpx.AsyncClient() as client:
        with caplog.at_level("INFO", logger="data_pipeline.runner"):
            _, _, destination = configure(RunnerSettings(), client)
    assert isinstance(destination, CuantoCuestaLog)
    assert caplog.messages == [f"{missing} not set: cuanto-cuesta batches are logged, not sent"]


async def test_an_empty_variable_counts_as_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_URL", URL)
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_TOKEN", " ")
    async with httpx.AsyncClient() as client:
        _, _, destination = configure(RunnerSettings(), client)
    assert isinstance(destination, CuantoCuestaLog)


async def test_with_both_it_sends_and_logs_the_url_but_never_the_token(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_URL", URL)
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_TOKEN", TOKEN)
    async with httpx.AsyncClient() as client:
        with caplog.at_level("INFO", logger="data_pipeline.runner"):
            _, _, destination = configure(RunnerSettings(), client)
    assert isinstance(destination, CuantoCuestaIngest)
    assert caplog.messages == [f"sending cuanto-cuesta batches to {URL}"]
    assert TOKEN not in caplog.text


def test_a_url_that_is_not_http_fails_at_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_URL", "cuanto-cuesta-backend:8000/api/ingest")
    monkeypatch.setenv("CUANTO_CUESTA_INGEST_TOKEN", TOKEN)
    with pytest.raises(ValidationError, match="http"):
        main(["--check"])
