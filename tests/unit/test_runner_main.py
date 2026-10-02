from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline.config import ConfigError
from data_pipeline.runner.__main__ import main


def test_check_loads_the_repo_configuration(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO", logger="data_pipeline.runner"):
        assert main(["--check"]) == 0
    assert caplog.messages == [
        "configuration ok: mep, binance_p2p_usdt_usd, bitso_usdt_ars, arq_usd_ars,"
        " binance_card_usd_usdt"
    ]


def test_check_fails_on_a_broken_series_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broken = tmp_path / "series.yaml"
    broken.write_text("series: [{id: mep}]\n")
    monkeypatch.setenv("SERIES_FILE", str(broken))
    with pytest.raises(ConfigError):
        main(["--check"])
