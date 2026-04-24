from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliMicroGateTests(unittest.TestCase):
    def test_cli_micro_gate_invokes_runner_without_duplicate_janitor_kwarg(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_micro_gate",
            return_value={"status": "ready", "output_file": "outputs/kalshi_micro_gate_summary.json"},
        ) as mock_gate, patch.object(
            sys,
            "argv",
            ["betbot", "kalshi-micro-gate", "--env-file", "dummy.env", "--output-dir", "outputs"],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_gate.call_args.kwargs
        self.assertEqual(kwargs["env_file"], "dummy.env")
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertNotIn("auto_cancel_duplicate_open_orders", kwargs)
        self.assertIsNone(kwargs["history_csv"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_micro_execute_history_csv_defaults_to_none(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_micro_execute",
            return_value={"status": "ready", "output_file": "outputs/kalshi_micro_execute_summary.json"},
        ) as mock_execute, patch.object(
            sys,
            "argv",
            ["betbot", "kalshi-micro-execute", "--env-file", "dummy.env", "--output-dir", "outputs"],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_execute.call_args.kwargs
        self.assertEqual(kwargs["env_file"], "dummy.env")
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertIsNone(kwargs["history_csv"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_micro_trader_history_csv_defaults_to_none(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_micro_trader",
            return_value={"status": "hold", "output_file": "outputs/kalshi_micro_trader_summary.json"},
        ) as mock_trader, patch.object(
            sys,
            "argv",
            ["betbot", "kalshi-micro-trader", "--env-file", "dummy.env", "--output-dir", "outputs"],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_trader.call_args.kwargs
        self.assertEqual(kwargs["env_file"], "dummy.env")
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertIsNone(kwargs["history_csv"])
        self.assertIn('"status": "hold"', stdout.getvalue())

    def test_cli_micro_watch_history_csv_defaults_to_none(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_micro_watch",
            return_value={"status": "ready", "output_file": "outputs/kalshi_micro_watch_summary.json"},
        ) as mock_watch, patch.object(
            sys,
            "argv",
            ["betbot", "kalshi-micro-watch", "--env-file", "dummy.env", "--output-dir", "outputs"],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_watch.call_args.kwargs
        self.assertEqual(kwargs["env_file"], "dummy.env")
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertIsNone(kwargs["history_csv"])
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
