from __future__ import annotations

import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from betbot.kalshi_temperature_constraints import (
    evaluate_temperature_constraint,
    infer_settlement_unit,
    run_kalshi_temperature_constraint_scan,
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

    def test_scan_handles_snapshot_errors_per_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-T84",
                        "market_title": "Will high temp in Austin be >84°?",
                        "rules_primary": "If highest temperature is greater than 84, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "America/Chicago",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "above:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            with patch(
                "betbot.kalshi_temperature_constraints.build_intraday_temperature_snapshot",
                side_effect=TimeoutError("read timed out"),
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_processed"], 1)
            self.assertEqual(summary["markets_emitted"], 1)
            self.assertEqual(summary["snapshot_unavailable_count"], 1)


if __name__ == "__main__":
    unittest.main()
