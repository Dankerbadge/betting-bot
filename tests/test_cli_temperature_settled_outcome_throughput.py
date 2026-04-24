from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureSettledOutcomeThroughputTests(unittest.TestCase):
    def test_cli_temperature_settled_outcome_throughput_dispatches_run_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_settled_outcome_throughput",
            return_value={
                "status": "ready",
                "output_file": "outputs/kalshi_temperature_settled_outcome_throughput_latest.json",
            },
        ) as mock_run, patch(
            "betbot.cli.summarize_kalshi_temperature_settled_outcome_throughput",
            return_value={"status": "ready"},
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-settled-outcome-throughput",
                "--output-dir",
                "outputs/live",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_run.call_count, 1)
        self.assertEqual(mock_run.call_args.kwargs["output_dir"], "outputs/live")
        self.assertEqual(mock_summarize.call_count, 0)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "run")

    def test_cli_temperature_settled_outcome_throughput_summarize_only_dispatches_summarizer(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_settled_outcome_throughput",
            return_value={"status": "ready"},
        ) as mock_run, patch(
            "betbot.cli.summarize_kalshi_temperature_settled_outcome_throughput",
            return_value={"status": "ready", "summary_file": "outputs/settled_outcome_throughput_summary_latest.json"},
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-settled-outcome-throughput",
                "--output-dir",
                "outputs/live",
                "--summarize-only",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_summarize.call_count, 1)
        self.assertEqual(mock_summarize.call_args.kwargs["output_dir"], "outputs/live")
        self.assertEqual(mock_run.call_count, 0)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "summarize_only")

    def test_cli_temperature_settled_outcome_throughput_summarize_only_accepts_json_string_payload(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_settled_outcome_throughput",
            return_value={"status": "ready"},
        ) as mock_run, patch(
            "betbot.cli.summarize_kalshi_temperature_settled_outcome_throughput",
            return_value=json.dumps({"status": "ready", "summary_file": "outputs/settled_outcome_throughput_summary_latest.json"}),
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-settled-outcome-throughput",
                "--summarize-only",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_summarize.call_count, 1)
        self.assertEqual(mock_run.call_count, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "summarize_only")

    def test_cli_temperature_settled_outcome_throughput_alias_falls_back_to_summary_when_run_runner_unavailable(
        self,
    ) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_settled_outcome_throughput",
            None,
        ), patch(
            "betbot.cli.summarize_kalshi_temperature_settled_outcome_throughput",
            return_value={"status": "ready"},
        ) as mock_summarize, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-settled-outcome-throughput",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        self.assertEqual(mock_summarize.call_count, 1)
        self.assertEqual(mock_summarize.call_args.kwargs["output_dir"], "outputs")

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["mode"], "run_fallback_summary")


if __name__ == "__main__":
    unittest.main()
