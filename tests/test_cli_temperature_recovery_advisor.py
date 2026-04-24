from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureRecoveryAdvisorTests(unittest.TestCase):
    def test_cli_temperature_recovery_advisor_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_recovery_advisor",
            return_value={"status": "ready", "output_file": "outputs/kalshi_temperature_recovery_advisor_latest.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-recovery-advisor",
                "--output-dir",
                "outputs/live",
                "--weather-window-hours",
                "744",
                "--weather-min-bucket-samples",
                "14",
                "--weather-max-profile-age-hours",
                "312",
                "--weather-negative-expectancy-attempt-share-target",
                "0.45",
                "--weather-stale-metar-negative-attempt-share-target",
                "0.53",
                "--weather-stale-metar-attempt-share-target",
                "0.58",
                "--weather-min-attempts-target",
                "250",
                "--optimizer-top-n",
                "7",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs/live")
        self.assertAlmostEqual(kwargs["weather_window_hours"], 744.0, places=6)
        self.assertEqual(kwargs["weather_min_bucket_samples"], 14)
        self.assertAlmostEqual(kwargs["weather_max_profile_age_hours"], 312.0, places=6)
        self.assertAlmostEqual(kwargs["weather_negative_expectancy_attempt_share_target"], 0.45, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_negative_attempt_share_target"], 0.53, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_attempt_share_target"], 0.58, places=6)
        self.assertEqual(kwargs["weather_min_attempts_target"], 250)
        self.assertEqual(kwargs["optimizer_top_n"], 7)
        self.assertNotIn("summarize_only", kwargs)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")

    def test_cli_temperature_recovery_advisor_alias_uses_defaults(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_recovery_advisor",
            return_value={"status": "ready"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-recovery-advisor",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertAlmostEqual(kwargs["weather_window_hours"], 720.0, places=6)
        self.assertEqual(kwargs["weather_min_bucket_samples"], 10)
        self.assertAlmostEqual(kwargs["weather_max_profile_age_hours"], 336.0, places=6)
        self.assertAlmostEqual(kwargs["weather_negative_expectancy_attempt_share_target"], 0.50, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_negative_attempt_share_target"], 0.60, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_attempt_share_target"], 0.65, places=6)
        self.assertEqual(kwargs["weather_min_attempts_target"], 200)
        self.assertEqual(kwargs["optimizer_top_n"], 5)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")


if __name__ == "__main__":
    unittest.main()
