import unittest

from betbot.kalshi_fees import (
    estimate_trade_fee,
    fee_adjusted_edge_per_contract,
    raw_trade_fee_dollars,
    rounded_fee_dollars,
)


class KalshiFeesTests(unittest.TestCase):
    def test_raw_trade_fee_scales_with_price_curve(self) -> None:
        fee_mid = raw_trade_fee_dollars(price_dollars=0.5, contract_count=10, fee_multiplier=0.035)
        fee_tail = raw_trade_fee_dollars(price_dollars=0.95, contract_count=10, fee_multiplier=0.035)
        self.assertGreater(fee_mid, fee_tail)

    def test_rounded_fee_uses_conservative_cent_rounding(self) -> None:
        self.assertEqual(rounded_fee_dollars(0.0001), 0.01)
        self.assertEqual(rounded_fee_dollars(0.0100), 0.01)
        self.assertEqual(rounded_fee_dollars(0.0101), 0.02)

    def test_estimate_trade_fee_returns_per_contract(self) -> None:
        estimate = estimate_trade_fee(
            price_dollars=0.2,
            contract_count=2,
            is_maker=True,
        )
        self.assertGreaterEqual(estimate.rounded_fee_dollars, 0.0)
        self.assertAlmostEqual(estimate.rounded_fee_dollars / 2.0, estimate.fee_per_contract_dollars, places=8)

    def test_fee_adjusted_edge_subtracts_fee(self) -> None:
        edge = fee_adjusted_edge_per_contract(
            fair_probability=0.60,
            entry_price_dollars=0.58,
            contract_count=1,
            is_maker=False,
        )
        self.assertLess(edge, 0.02)

    def test_index_ticker_uses_reduced_taker_fee_multiplier(self) -> None:
        generic = estimate_trade_fee(
            price_dollars=0.5,
            contract_count=1,
            is_maker=False,
            market_ticker="KXSTATE51-29-DC",
        )
        index = estimate_trade_fee(
            price_dollars=0.5,
            contract_count=1,
            is_maker=False,
            market_ticker="KXINXD-26MAR30-B5000",
        )
        self.assertGreater(generic.rounded_fee_dollars, index.rounded_fee_dollars)
        self.assertEqual(generic.rounded_fee_dollars, 0.02)
        self.assertEqual(index.rounded_fee_dollars, 0.01)

    def test_maker_fee_is_unchanged_for_index_tickers(self) -> None:
        generic = estimate_trade_fee(
            price_dollars=0.5,
            contract_count=1,
            is_maker=True,
            market_ticker="KXSTATE51-29-DC",
        )
        index = estimate_trade_fee(
            price_dollars=0.5,
            contract_count=1,
            is_maker=True,
            market_ticker="KXNASDAQ100D-26MAR30-B20000",
        )
        self.assertEqual(generic.rounded_fee_dollars, index.rounded_fee_dollars)


if __name__ == "__main__":
    unittest.main()
