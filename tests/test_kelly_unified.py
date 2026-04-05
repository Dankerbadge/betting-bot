import unittest

from betbot.kelly_unified import (
    KellyConfig,
    binary_kelly_fee_adjusted,
    kelly_dollar_stake,
    score_candidate,
)


class KellyUnifiedTests(unittest.TestCase):
    def test_binary_kelly_reduces_to_no_fee_identity(self) -> None:
        price = 0.18
        fair_prob = 0.25
        f_star, _ = binary_kelly_fee_adjusted(price=price, fair_prob=fair_prob, fee_rate=0.0)
        expected = (fair_prob - price) / (1.0 - price)
        self.assertAlmostEqual(f_star, expected, places=10)

    def test_binary_kelly_fee_adjusted_matches_expected_examples(self) -> None:
        f_a, _ = binary_kelly_fee_adjusted(price=0.18, fair_prob=0.25, fee_rate=0.07)
        f_b, _ = binary_kelly_fee_adjusted(price=0.94, fair_prob=0.97, fee_rate=0.07)
        self.assertAlmostEqual(f_a, 0.0729740, places=4)
        self.assertAlmostEqual(f_b, 0.4646237, places=4)

    def test_score_candidate_sets_reject_reason_for_small_kelly(self) -> None:
        cfg = KellyConfig(
            fee_rate=0.07,
            kelly_fraction=0.25,
            min_kelly_used=0.002,
            min_edge_net_fees=0.01,
            min_fair_prob=0.0,
        )
        scored = score_candidate(
            price=0.50,
            fair_prob=0.519,
            edge_net_fees=0.02,
            bankroll=100.0,
            config=cfg,
            ticker="KXTEST",
            side="yes",
        )
        self.assertIsNotNone(scored.kelly_reject_reason)
        self.assertIn("kelly_used_too_small", str(scored.kelly_reject_reason))

    def test_kelly_dollar_stake_respects_caps(self) -> None:
        stake = kelly_dollar_stake(
            f_used=0.20,
            bankroll=100.0,
            max_bet_fraction=0.10,
            min_stake=1.0,
            max_stake=500.0,
        )
        self.assertEqual(stake, 10.0)


if __name__ == "__main__":
    unittest.main()
