import unittest

from betbot.probability_path import (
    eventual_success_probability,
    hitting_probability,
    required_starting_units,
)


class ProbabilityPathTests(unittest.TestCase):
    def test_hitting_probability_fair_walk(self) -> None:
        # From 1 to 10 in fair walk => 0.1
        value = hitting_probability(start_units=1, target_units=10, p=0.5)
        self.assertAlmostEqual(value, 0.1, places=6)

    def test_hitting_probability_biased_walk(self) -> None:
        # From document's approximate table: p=0.55 from 1 to 100 => ~0.181818
        value = hitting_probability(start_units=1, target_units=100, p=0.55)
        self.assertAlmostEqual(value, 0.181818, places=4)

    def test_eventual_success_probability(self) -> None:
        value = eventual_success_probability(start_units=1, p=0.55)
        self.assertAlmostEqual(value, 1.0 - (0.45 / 0.55), places=6)

    def test_required_units(self) -> None:
        units = required_starting_units(target_success_prob=0.90, p=0.55)
        self.assertEqual(units, 12)


if __name__ == "__main__":
    unittest.main()

