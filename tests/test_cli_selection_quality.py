from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliSelectionQualityTests(unittest.TestCase):
    def test_cli_temperature_selection_quality_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_selection_quality",
            return_value={"status": "ready", "output_file": "outputs/selection_quality.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-selection-quality",
                "--output-dir",
                "outputs",
                "--lookback-hours",
                "480",
                "--intent-hours",
                "36",
                "--min-resolved-market-sides",
                "18",
                "--min-bucket-samples",
                "6",
                "--preferred-attribution-model",
                "fixed_fraction_per_unique_market_side",
                "--max-profile-age-hours",
                "72",
                "--probability-penalty-max",
                "0.08",
                "--expected-edge-penalty-max",
                "0.01",
                "--score-adjust-scale",
                "0.5",
                "--top-n",
                "7",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(kwargs["lookback_hours"], 480.0, places=6)
        self.assertAlmostEqual(kwargs["intent_hours"], 36.0, places=6)
        self.assertEqual(kwargs["min_resolved_market_sides"], 18)
        self.assertEqual(kwargs["min_bucket_samples"], 6)
        self.assertEqual(
            kwargs["preferred_attribution_model"],
            "fixed_fraction_per_unique_market_side",
        )
        self.assertAlmostEqual(kwargs["max_profile_age_hours"], 72.0, places=6)
        self.assertAlmostEqual(kwargs["probability_penalty_max"], 0.08, places=6)
        self.assertAlmostEqual(kwargs["expected_edge_penalty_max"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["score_adjust_scale"], 0.5, places=6)
        self.assertEqual(kwargs["top_n"], 7)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_growth_optimizer_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_temperature_growth_optimizer",
            return_value={
                "status": "ready",
                "profile_json": "outputs/kalshi_temperature_growth_optimizer_profile.json",
                "profile_application_status": "applied",
            },
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-growth-optimizer",
                "--output-dir",
                "outputs",
                "--intent-files",
                "intents/a.csv",
                "intents/b.csv",
                "--search-bounds-json",
                "bounds.json",
                "--lookback-hours-min",
                "168",
                "--lookback-hours-max",
                "336",
                "--intent-hours-min",
                "12",
                "--intent-hours-max",
                "48",
                "--min-resolved-market-sides-min",
                "12",
                "--min-resolved-market-sides-max",
                "18",
                "--min-bucket-samples-min",
                "4",
                "--min-bucket-samples-max",
                "6",
                "--score-adjust-scale-min",
                "0.25",
                "--score-adjust-scale-max",
                "0.45",
                "--top-n",
                "5",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertEqual(kwargs["intent_files"], ["intents/a.csv", "intents/b.csv"])
        self.assertAlmostEqual(kwargs["lookback_hours_min"], 168.0, places=6)
        self.assertAlmostEqual(kwargs["lookback_hours_max"], 336.0, places=6)
        self.assertAlmostEqual(kwargs["intent_hours_min"], 12.0, places=6)
        self.assertAlmostEqual(kwargs["intent_hours_max"], 48.0, places=6)
        self.assertEqual(kwargs["min_resolved_market_sides_min"], 12)
        self.assertEqual(kwargs["min_resolved_market_sides_max"], 18)
        self.assertEqual(kwargs["min_bucket_samples_min"], 4)
        self.assertEqual(kwargs["min_bucket_samples_max"], 6)
        self.assertAlmostEqual(kwargs["score_adjust_scale_min"], 0.25, places=6)
        self.assertAlmostEqual(kwargs["score_adjust_scale_max"], 0.45, places=6)
        self.assertEqual(kwargs["top_n"], 5)
        self.assertEqual(kwargs["search_bounds_json"], "bounds.json")
        self.assertIn('"status": "ready"', stdout.getvalue())
        self.assertIn('"profile_application_status": "applied"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
