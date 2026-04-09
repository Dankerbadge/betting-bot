from __future__ import annotations

import unittest

from betbot.kalshi_temperature_constraints import (
    evaluate_temperature_constraint,
    infer_settlement_unit,
)


class KalshiTemperatureConstraintsTests(unittest.TestCase):
    def test_evaluate_yes_impossible_for_at_most(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_most:72",
            observed_value=74.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("exceeds", reason)

    def test_evaluate_yes_likely_locked_for_at_least(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_least:75",
            observed_value=75.0,
        )
        self.assertEqual(status, "yes_likely_locked")
        self.assertIn("meets", reason)

    def test_evaluate_no_signal_for_between_in_range(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="between:70:74",
            observed_value=72.0,
        )
        self.assertEqual(status, "no_signal")
        self.assertIn("currently satisfied", reason)

    def test_infer_settlement_unit_defaults_to_fahrenheit(self) -> None:
        unit = infer_settlement_unit("Highest temperature in NYC", "local day")
        self.assertEqual(unit, "fahrenheit")

    def test_infer_settlement_unit_detects_celsius(self) -> None:
        unit = infer_settlement_unit("Highest temperature in Paris", "Resolve in Celsius")
        self.assertEqual(unit, "celsius")


if __name__ == "__main__":
    unittest.main()
