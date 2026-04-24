from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureCoverageVelocityReportTests(unittest.TestCase):
    def test_cli_temperature_coverage_velocity_report_dispatches_run_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_coverage_velocity_report",
            return_value={
                "status": "ready",
                "output_file": "outputs/health/kalshi_temperature_coverage_velocity_report_latest.json",
            },
        ) as mock_run, patch(
            "betbot.cli.summarize_kalshi_temperature_coverage_velocity_report",
            return_value=json.dumps({"status": "ready"}),
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-coverage-velocity-report",
                "--output-dir",
                "outputs/live",
                "--history-limit",
                "12",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_run.call_count, 1)
        self.assertEqual(mock_run.call_args.kwargs["output_dir"], "outputs/live")
        self.assertEqual(mock_run.call_args.kwargs["history_limit"], 12)
        self.assertEqual(mock_summarize.call_count, 0)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "run")

    def test_cli_temperature_coverage_velocity_report_summarize_only_dispatches_summarizer(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_coverage_velocity_report",
            return_value={"status": "ready"},
        ) as mock_run, patch(
            "betbot.cli.summarize_kalshi_temperature_coverage_velocity_report",
            return_value=json.dumps({"status": "ready", "report_summary": "guardrail=active"}),
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-coverage-velocity-report",
                "--summarize-only",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_summarize.call_count, 1)
        self.assertEqual(mock_summarize.call_args.kwargs["output_dir"], "outputs")
        self.assertEqual(mock_summarize.call_args.kwargs["history_limit"], 24)
        self.assertEqual(mock_run.call_count, 0)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "summarize_only")


if __name__ == "__main__":
    unittest.main()
