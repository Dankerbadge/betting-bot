from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliWeatherPatternTests(unittest.TestCase):
    def test_cli_temperature_weather_pattern_dispatches_canonical_command(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_weather_pattern",
            return_value={"status": "ready", "output_file": "outputs/weather_pattern_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-weather-pattern",
                "--output-dir",
                "outputs",
                "--window-hours",
                "36",
                "--min-samples",
                "18",
                "--max-age-hours",
                "48",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(kwargs["window_hours"], 36.0, places=6)
        self.assertEqual(kwargs["min_bucket_samples"], 18)
        self.assertAlmostEqual(kwargs["max_profile_age_hours"], 48.0, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_weather_pattern_accepts_alias_command(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_weather_pattern",
            return_value={"status": "ready", "output_file": "outputs/weather_pattern_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-weather-pattern",
                "--output-dir",
                "outputs",
                "--window-hours",
                "24",
                "--min-samples",
                "12",
                "--max-age-hours",
                "72",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(kwargs["window_hours"], 24.0, places=6)
        self.assertEqual(kwargs["min_bucket_samples"], 12)
        self.assertAlmostEqual(kwargs["max_profile_age_hours"], 72.0, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_weather_pattern_accepts_runner_named_arg_aliases(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_weather_pattern",
            return_value={"status": "ready", "output_file": "outputs/weather_pattern_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-weather-pattern",
                "--output-dir",
                "outputs",
                "--window-hours",
                "30",
                "--min-bucket-samples",
                "14",
                "--max-profile-age-hours",
                "40",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(kwargs["window_hours"], 30.0, places=6)
        self.assertEqual(kwargs["min_bucket_samples"], 14)
        self.assertAlmostEqual(kwargs["max_profile_age_hours"], 40.0, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
