"""Every source against a response recorded on 2026-09-25, and against what a changed format
looks like. The recorded values are in each test, so a fixture edit shows up here."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from data_pipeline.core.checks import value_problem
from data_pipeline.core.readings import Rejection
from data_pipeline.core.sources import MalformedResponseError, NoQuoteError
from data_pipeline.sources import available_sources
from data_pipeline.sources.ambito import AmbitoMep
from data_pipeline.sources.arq import ArqBid
from data_pipeline.sources.binance_card import BinanceCardPrice
from data_pipeline.sources.binance_p2p import BinanceP2PMedian
from data_pipeline.sources.criptoya import CriptoYaBid
from data_pipeline.sources.dolarapi import DolarApiMep

FIXTURES = Path(__file__).parent / "fixtures"
FETCHED_AT = datetime(2026, 9, 25, 20, 50, tzinfo=UTC)


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def edited(name: str, change: dict[str, object], at: list[str | int] = []) -> bytes:  # noqa: B006
    data = json.loads(fixture(name))
    target = data
    for key in at:
        target = target[key]
    target.update(change)
    return json.dumps(data).encode()


def test_every_source_has_a_unique_name() -> None:
    assert len(available_sources()) == 8


class TestDolarApi:
    source = DolarApiMep()

    def test_reads_the_compra_side_and_its_time(self) -> None:
        # Recorded: compra 1544.3, venta 1557.3, fechaActualizacion 2026-09-25T19:58:00.000Z.
        reading = self.source.parse(fixture("dolarapi_bolsa.json"), FETCHED_AT)
        assert reading.value == Decimal("1544.3")
        assert reading.as_of == datetime(2026, 9, 25, 19, 58, tzinfo=UTC)

    def test_a_whole_number_is_read_too(self) -> None:
        body = edited("dolarapi_bolsa.json", {"compra": 1549})
        assert self.source.parse(body, FETCHED_AT).value == Decimal(1549)

    @pytest.mark.parametrize(
        "change",
        [{"compra": "1544.3"}, {"compra": None}, {"casa": "oficial"}, {"fechaActualizacion": "x"}],
    )
    def test_a_changed_format_is_malformed(self, change: dict[str, object]) -> None:
        with pytest.raises(MalformedResponseError):
            self.source.parse(edited("dolarapi_bolsa.json", change), FETCHED_AT)


class TestAmbito:
    source = AmbitoMep()

    def test_reads_the_comma_decimal_and_the_local_time(self) -> None:
        # Recorded: compra "1550,60", fecha "25/09/2026 - 15:00" (Buenos Aires, UTC-3).
        reading = self.source.parse(fixture("ambito_mep_variacion.json"), FETCHED_AT)
        assert reading.value == Decimal("1550.60")
        assert reading.as_of == datetime(2026, 9, 25, 18, 0, tzinfo=UTC)

    def test_reads_thousands_separators(self) -> None:
        body = edited("ambito_mep_variacion.json", {"compra": "1.550,60"})
        assert self.source.parse(body, FETCHED_AT).value == Decimal("1550.60")

    @pytest.mark.parametrize(
        "change",
        [
            {"compra": "1550.60"},
            {"compra": "1,550.60"},
            {"compra": "-1550,60"},
            {"fecha": "2026-09-25 15:00"},
            {"compra": 1550.6},
        ],
    )
    def test_a_changed_format_is_malformed(self, change: dict[str, object]) -> None:
        with pytest.raises(MalformedResponseError):
            self.source.parse(edited("ambito_mep_variacion.json", change), FETCHED_AT)


class TestBinanceP2P:
    source = BinanceP2PMedian(fiat="USD", asset="USDT", amount=Decimal(500))

    def test_asks_for_the_twenty_cheapest_buy_ads(self) -> None:
        assert self.source.request().url == (
            "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
            "?fiat=USD&asset=USDT&tradeType=BUY&limit=20"
        )

    def test_takes_the_median_of_the_five_cheapest_usable_ads(self) -> None:
        # Recorded: 20 ads, 8 from merchants with >= 95 % completed that take 500 USD, priced
        # 1.0, 1.0, 1.0, 1.001, 1.001, 1.001, 1.002, 1.002. The five cheapest: median 1.0.
        reading = self.source.parse(fixture("binance_p2p_ad_list_usd_usdt_buy.json"), FETCHED_AT)
        assert reading.value == Decimal("1.0")
        assert reading.as_of == FETCHED_AT

    def test_filters_advertisers_rates_and_amounts(self) -> None:
        # JSON text: the prices are written as JSON numbers, like Binance writes them.
        def ad(price: str, user: str = "merchant", rate: str = "0.99", **amounts: str) -> str:
            limits = {"minTransAmount": "10", "maxTransAmount": "1000", "tradableAmount": "1000"}
            limits.update(amounts)
            return (
                f'{{"price": {price}, "fiat": "USD", "asset": "USDT",'
                + "".join(f' "{key}": {value},' for key, value in limits.items())
                + f' "advertiser": {{"userType": "{user}", "monthFinishRate": {rate}}}}}'
            )

        unusable = [
            ad("0.90", user="user"),
            ad("0.91", rate="0.949"),
            ad("0.92", minTransAmount="501"),
            ad("0.93", maxTransAmount="499"),
            ad("0.94", tradableAmount="400"),  # 400 USDT at 0.94 is 376 USD, less than 500
        ]
        usable = [ad(price) for price in ("1.30", "1.01", "1.20", "1.02", "1.04", "1.03")]
        body = f'{{"code": "000000", "data": {{"items": [{", ".join(unusable + usable)}]}}}}'
        # The five cheapest usable: 1.01, 1.02, 1.03, 1.04, 1.20. Median 1.03 (the mean is
        # 1.06, and with the unusable ads the median would be 0.92).
        assert self.source.parse(body.encode(), FETCHED_AT).value == Decimal("1.03")

    def test_too_few_usable_ads_is_no_quote(self) -> None:
        strict = BinanceP2PMedian(fiat="USD", asset="USDT", amount=Decimal(500), top=9)
        with pytest.raises(NoQuoteError, match="only 8 usable ads of 20, need 9"):
            strict.parse(fixture("binance_p2p_ad_list_usd_usdt_buy.json"), FETCHED_AT)

    def test_ads_for_another_pair_are_malformed(self) -> None:
        other = BinanceP2PMedian(fiat="ARS", asset="USDT", amount=Decimal(500))
        with pytest.raises(MalformedResponseError, match="USDT/USD"):
            other.parse(fixture("binance_p2p_ad_list_usd_usdt_buy.json"), FETCHED_AT)

    def test_an_error_code_is_malformed(self) -> None:
        with pytest.raises(MalformedResponseError):
            self.source.parse(b'{"code": "000002", "data": null}', FETCHED_AT)


class TestArq:
    source = ArqBid()

    def test_reads_the_bid_and_takes_the_time_as_utc(self) -> None:
        # Recorded at 18:03 UTC: bid "1613.3450400000", date "2026-09-25T18:03:40.049545995".
        reading = self.source.parse(fixture("arq_tickers_ars.json"), FETCHED_AT)
        assert reading.value == Decimal("1613.34504")
        assert reading.as_of == datetime(2026, 9, 25, 18, 3, 40, 49545, tzinfo=UTC)

    def test_a_date_with_a_zone_is_a_change_to_check(self) -> None:
        body = json.dumps(
            [{"ask": "1", "bid": "1613.3", "book": "usdc_ars", "date": "2026-09-25T18:03:40Z"}]
        )
        with pytest.raises(MalformedResponseError, match="now has a time zone"):
            self.source.parse(body.encode(), FETCHED_AT)

    @pytest.mark.parametrize(
        "body",
        [
            b"[]",
            b'[{"bid": "1613.3", "book": "usdt_ars", "date": "2026-09-25T18:03:40"}]',
            b'[{"bid": 1613.3, "book": "usdc_ars", "date": "2026-09-25T18:03:40"}]',
            b'{"bid": "1613.3", "book": "usdc_ars", "date": "2026-09-25T18:03:40"}',
        ],
        ids=["empty", "other-book", "number-bid", "not-a-list"],
    )
    def test_a_changed_format_is_malformed(self, body: bytes) -> None:
        with pytest.raises(MalformedResponseError):
            self.source.parse(body, FETCHED_AT)


class TestCriptoYa:
    def test_reads_one_exchange(self) -> None:
        # Recorded: bid 1611.19, time 1790369425 (20:50:25 UTC).
        source = CriptoYaBid("criptoya_bitso_usdt_ars_bid", "bitsoalpha/USDT/ARS/1")
        assert source.request().url == "https://criptoya.com/api/bitsoalpha/USDT/ARS/1"
        reading = source.parse(fixture("criptoya_bitsoalpha_usdt_ars.json"), FETCHED_AT)
        assert reading.value == Decimal("1611.19")
        assert reading.as_of == datetime(2026, 9, 25, 20, 50, 25, tzinfo=UTC)

    def test_picks_an_exchange_from_a_list(self) -> None:
        # Recorded: dolarapp bid 1613.3151, time 1790359371 (18:02:51 UTC).
        source = CriptoYaBid("criptoya_arq_usdc_ars_bid", "usdc/ARS/1", exchange="dolarapp")
        reading = source.parse(fixture("criptoya_usdc_ars.json"), FETCHED_AT)
        assert reading.value == Decimal("1613.3151")
        assert reading.as_of == datetime(2026, 9, 25, 18, 2, 51, tzinfo=UTC)

    def test_a_missing_exchange_and_invalid_pair_are_malformed(self) -> None:
        source = CriptoYaBid("x", "usdc/ARS/1", exchange="gone")
        with pytest.raises(MalformedResponseError, match="no quote for gone"):
            source.parse(fixture("criptoya_usdc_ars.json"), FETCHED_AT)
        with pytest.raises(MalformedResponseError, match="not JSON"):
            source.parse(b"Invalid pair", FETCHED_AT)

    @pytest.mark.parametrize(
        "body",
        [
            b'{"bid": "1611.19", "time": 1790369425}',
            b'{"bid": 1611.19, "time": 1790369425.5}',
            b'{"bid": 1611.19, "time": 100000000000000000000}',
        ],
        ids=["text-bid", "fractional-time", "time-out-of-range"],
    )
    def test_a_changed_format_is_malformed(self, body: bytes) -> None:
        with pytest.raises(MalformedResponseError):
            CriptoYaBid("x", "bitsoalpha/USDT/ARS/1").parse(body, FETCHED_AT)


class TestBinanceCard:
    source = BinanceCardPrice()

    def test_reads_the_card_price_in_usdt_per_usd_as_of_the_answer(self) -> None:
        # Recorded: BUY_PAYMONADE_CARD quotation "1.02282331" USD per USDT, and for the minimum
        # of 10 USD, cryptoMinLimit "9.7768597" USDT. 1 / 1.02282331 = 0.977685978..., rounded
        # down to 8 decimals: 0.97768597, what Binance shows.
        body = fixture("binance_fiat_payment_methods_buy_usd_usdt_ar.json")
        reading = self.source.parse(body, FETCHED_AT)
        assert self.source.name == "binance_card_usd_usdt_list"
        assert reading.value == Decimal("0.97768597")
        assert reading.as_of == FETCHED_AT

    def with_quotation(self, quotation: str) -> bytes:
        data = json.loads(fixture("binance_fiat_payment_methods_buy_usd_usdt_ar.json"))
        for method in data["data"]["paymentMethods"]:
            if method["code"] == "BUY_PAYMONADE_CARD":
                method["quotation"] = quotation
        return json.dumps(data).encode()

    @pytest.mark.parametrize("quotation", ["0", "0.000"])
    def test_a_zero_quotation_is_malformed(self, quotation: str) -> None:
        with pytest.raises(MalformedResponseError, match="zero"):
            self.source.parse(self.with_quotation(quotation), FETCHED_AT)

    def test_a_quotation_too_small_to_invert_is_malformed(self) -> None:
        # 1 / 1e-33 to 8 decimals needs 42 digits, more than the 40 the inversion keeps.
        with pytest.raises(MalformedResponseError, match="cannot be inverted"):
            self.source.parse(self.with_quotation("0." + "0" * 32 + "1"), FETCHED_AT)

    def test_an_absurd_quotation_inverts_exactly_and_the_checks_refuse_it(self) -> None:
        # 1 / 1e-28 = 1e28: no crash; value_problem then rejects it as too large.
        reading = self.source.parse(self.with_quotation("0." + "0" * 27 + "1"), FETCHED_AT)
        assert reading.value == Decimal(10) ** 28
        problem = value_problem(reading.value)
        assert problem is not None
        assert problem[0] is Rejection.TOO_LARGE

    def test_the_inverse_is_rounded_down(self) -> None:
        # 1 / 3 = 0.333333333...; 1 / 1.5 = 0.666666666... rounds down, not up to ...67.
        for quotation, expected in (("3", "0.33333333"), ("1.5", "0.66666666")):
            name = "binance_fiat_payment_methods_buy_usd_usdt_ar.json"
            data = json.loads(fixture(name))
            for method in data["data"]["paymentMethods"]:
                if method["code"] == "BUY_PAYMONADE_CARD":
                    method["quotation"] = quotation
            reading = self.source.parse(json.dumps(data).encode(), FETCHED_AT)
            assert reading.value == Decimal(expected)

    def test_a_missing_or_suspended_card_method_is_no_quote(self) -> None:
        name = "binance_fiat_payment_methods_buy_usd_usdt_ar.json"
        data = json.loads(fixture(name))
        methods = data["data"]["paymentMethods"]
        card = next(method for method in methods if method["code"] == "BUY_PAYMONADE_CARD")
        card["suspended"] = True
        with pytest.raises(NoQuoteError, match="suspended"):
            self.source.parse(json.dumps(data).encode(), FETCHED_AT)
        methods.remove(card)
        with pytest.raises(NoQuoteError, match="no BUY_PAYMONADE_CARD"):
            self.source.parse(json.dumps(data).encode(), FETCHED_AT)
