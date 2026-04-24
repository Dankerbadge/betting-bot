from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliDecisionMatrixHardeningTests(unittest.TestCase):
    def test_cli_decision_matrix_hardening_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_decision_matrix_hardening",
            return_value={"status": "ready", "matrix_health_status": "red"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "decision-matrix-hardening",
                "--output-dir",
                "outputs",
                "--window-hours",
                "240",
                "--min-settled-outcomes",
                "30",
                "--max-top-blocker-share",
                "0.52",
                "--min-approval-rate",
                "0.04",
                "--min-intents-sample",
                "1500",
                "--max-sparse-edge-block-share",
                "0.75",
                "--min-execution-cost-candidate-samples",
                "260",
                "--min-execution-cost-quote-coverage-ratio",
                "0.66",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(float(kwargs["window_hours"]), 240.0, places=9)
        self.assertEqual(int(kwargs["min_settled_outcomes"]), 30)
        self.assertAlmostEqual(float(kwargs["max_top_blocker_share"]), 0.52, places=9)
        self.assertAlmostEqual(float(kwargs["min_approval_rate"]), 0.04, places=9)
        self.assertEqual(int(kwargs["min_intents_sample"]), 1500)
        self.assertAlmostEqual(float(kwargs["max_sparse_edge_block_share"]), 0.75, places=9)
        self.assertEqual(int(kwargs["min_execution_cost_candidate_samples"]), 260)
        self.assertAlmostEqual(float(kwargs["min_execution_cost_quote_coverage_ratio"]), 0.66, places=9)
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
