import unittest

from betbot.edge import (
    confidence_adjusted_edge,
    consensus_stats,
    consensus_confidence,
    edge_roi_on_cost,
    expected_value_decimal,
    full_kelly_fraction,
    normalize_implied_probabilities,
    robust_consensus_probability,
    stale_quote_penalty,
)


class EdgeTests(unittest.TestCase):
    def test_expected_value(self) -> None:
        ev = expected_value_decimal(0.55, 1.95)
        self.assertAlmostEqual(ev, 0.0725, places=6)

    def test_full_kelly_positive_edge(self) -> None:
        frac = full_kelly_fraction(0.55, 1.95)
        self.assertGreater(frac, 0.0)

    def test_normalize_implied_probabilities(self) -> None:
        probs = normalize_implied_probabilities([1.90, 1.95])
        self.assertAlmostEqual(sum(probs), 1.0, places=6)

    def test_normalize_implied_probabilities_uses_power_devig_for_skewed_books(self) -> None:
        probs = normalize_implied_probabilities([1.4, 3.2])
        self.assertAlmostEqual(probs[0], 0.703505, places=6)
        self.assertAlmostEqual(probs[1], 0.296495, places=6)

    def test_stale_quote_penalty_caps_at_max_penalty(self) -> None:
        self.assertEqual(stale_quote_penalty(0), 0.0)
        self.assertAlmostEqual(stale_quote_penalty(900), 0.01, places=6)
        self.assertAlmostEqual(stale_quote_penalty(3600), 0.02, places=6)

    def test_consensus_confidence_combines_stability_and_depth(self) -> None:
        shallow = consensus_confidence(stability=0.9, books_used=2, min_books=2)
        deeper = consensus_confidence(stability=0.9, books_used=3, min_books=2)
        self.assertAlmostEqual(shallow, 0.6, places=6)
        self.assertAlmostEqual(deeper, 0.9, places=6)
        self.assertAlmostEqual(confidence_adjusted_edge(0.05, shallow), 0.03, places=6)

    def test_edge_roi_on_cost_uses_entry_price(self) -> None:
        self.assertAlmostEqual(edge_roi_on_cost(0.03, 0.75), 0.04, places=6)
        self.assertAlmostEqual(edge_roi_on_cost(-0.01, 0.2), -0.05, places=6)

    def test_robust_consensus_probability_rejects_out_of_range_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 0 and 1"):
            robust_consensus_probability([0.48, 55, 0.51])

    def test_consensus_stats_rejects_non_finite_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 0 and 1"):
            consensus_stats([0.49, float("nan"), 0.5])


if __name__ == "__main__":
    unittest.main()
