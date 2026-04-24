from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliExecutionCostTapeTests(unittest.TestCase):
    def test_cli_temperature_execution_cost_tape_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_execution_cost_tape",
            return_value={"status": "ready", "calibration_readiness": {"status": "green"}},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-execution-cost-tape",
                "--output-dir",
                "outputs",
                "--window-hours",
                "96",
                "--min-candidate-samples",
                "300",
                "--min-quote-coverage-ratio",
                "0.7",
                "--journal-db-path",
                "outputs/kalshi_execution_journal.sqlite3",
                "--max-tickers",
                "12",
                "--min-global-expected-edge-share-for-exclusion",
                "0.5",
                "--min-ticker-rows-for-exclusion",
                "220",
                "--exclusion-max-quote-coverage-ratio",
                "0.18",
                "--max-ticker-mean-spread-for-exclusion",
                "0.09",
                "--max-excluded-tickers",
                "9",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(float(kwargs["window_hours"]), 96.0, places=9)
        self.assertEqual(int(kwargs["min_candidate_samples"]), 300)
        self.assertAlmostEqual(float(kwargs["min_quote_coverage_ratio"]), 0.7, places=9)
        self.assertEqual(kwargs["journal_db_path"], "outputs/kalshi_execution_journal.sqlite3")
        self.assertEqual(int(kwargs["max_tickers"]), 12)
        self.assertAlmostEqual(float(kwargs["min_global_expected_edge_share_for_exclusion"]), 0.5, places=9)
        self.assertEqual(int(kwargs["min_ticker_rows_for_exclusion"]), 220)
        self.assertAlmostEqual(float(kwargs["exclusion_max_quote_coverage_ratio"]), 0.18, places=9)
        self.assertAlmostEqual(float(kwargs["max_ticker_mean_spread_for_exclusion"]), 0.09, places=9)
        self.assertEqual(int(kwargs["max_excluded_tickers"]), 9)
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
