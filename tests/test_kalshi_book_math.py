import unittest

from betbot.kalshi_book_math import derive_top_of_book


class KalshiBookMathTests(unittest.TestCase):
    def test_derive_top_of_book_builds_reciprocal_asks_and_spread(self) -> None:
        snapshot = derive_top_of_book(
            {
                "yes_dollars": [["0.4200", "120.00"], ["0.4100", "5.00"]],
                "no_dollars": [["0.5600", "80.00"], ["0.5500", "10.00"]],
            }
        )
        self.assertEqual(snapshot["best_yes_bid_dollars"], 0.42)
        self.assertEqual(snapshot["best_no_bid_dollars"], 0.56)
        self.assertEqual(snapshot["best_yes_ask_dollars"], 0.44)
        self.assertEqual(snapshot["best_no_ask_dollars"], 0.58)
        self.assertEqual(snapshot["yes_spread_dollars"], 0.02)
        self.assertAlmostEqual(float(snapshot["yes_midpoint_dollars"]), 0.43, places=6)

    def test_derive_top_of_book_uses_max_bid_levels_when_unsorted(self) -> None:
        snapshot = derive_top_of_book(
            {
                "yes_dollars": [["0.1000", "1.00"], ["0.2000", "2.00"]],
                "no_dollars": [["0.7000", "1.00"], ["0.8000", "2.00"]],
            }
        )
        self.assertEqual(snapshot["best_yes_bid_dollars"], 0.2)
        self.assertEqual(snapshot["best_no_bid_dollars"], 0.8)
        self.assertEqual(snapshot["best_yes_ask_dollars"], 0.2)
        self.assertEqual(snapshot["best_no_ask_dollars"], 0.8)


if __name__ == "__main__":
    unittest.main()
