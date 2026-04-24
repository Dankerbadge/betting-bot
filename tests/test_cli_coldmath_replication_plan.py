from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliColdMathReplicationPlanTests(unittest.TestCase):
    def test_cli_coldmath_replication_plan_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_coldmath_replication_plan",
            return_value={"status": "ready", "candidate_count": 3},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "coldmath-replication-plan",
                "--output-dir",
                "outputs",
                "--top-n",
                "7",
                "--market-tickers",
                "MKT-1,MKT-2",
                "--excluded-market-tickers",
                "MKT-2,MKT-9",
                "--excluded-market-tickers-file",
                "outputs/health/execution_cost_tape_latest.json",
                "--max-spread-dollars",
                "0.12",
                "--min-liquidity-score",
                "0.6",
                "--max-family-candidates",
                "2",
                "--max-family-share",
                "0.5",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertEqual(kwargs["top_n"], 7)
        self.assertEqual(kwargs["market_tickers"], ["MKT-1", "MKT-2"])
        self.assertEqual(kwargs["excluded_market_tickers"], ["MKT-2", "MKT-9"])
        self.assertEqual(kwargs["excluded_market_tickers_file"], "outputs/health/execution_cost_tape_latest.json")
        self.assertAlmostEqual(float(kwargs["max_spread_dollars"]), 0.12, places=9)
        self.assertAlmostEqual(float(kwargs["min_liquidity_score"]), 0.6, places=9)
        self.assertEqual(int(kwargs["max_family_candidates"]), 2)
        self.assertAlmostEqual(float(kwargs["max_family_share"]), 0.5, places=9)
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
