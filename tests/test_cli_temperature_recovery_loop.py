from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureRecoveryLoopTests(unittest.TestCase):
    def test_cli_temperature_recovery_loop_dispatches_runner(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_recovery_loop",
            return_value={"status": "running", "iteration": 1},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-recovery-loop",
                "--output-dir",
                "outputs/live",
                "--trader-env-file",
                "/tmp/runtime.env",
                "--max-iterations",
                "6",
                "--stall-iterations",
                "3",
                "--min-gap-improvement",
                "0.025",
                "--weather-window-hours",
                "744",
                "--weather-min-bucket-samples",
                "16",
                "--weather-max-profile-age-hours",
                "300",
                "--weather-negative-expectancy-attempt-share-target",
                "0.42",
                "--weather-stale-metar-negative-attempt-share-target",
                "0.51",
                "--weather-stale-metar-attempt-share-target",
                "0.56",
                "--weather-min-attempts-target",
                "260",
                "--optimizer-top-n",
                "8",
                "--no-plateau-negative-regime-suppression-enabled",
                "--plateau-negative-regime-suppression-min-bucket-samples",
                "24",
                "--plateau-negative-regime-suppression-expectancy-threshold",
                "-0.08",
                "--plateau-negative-regime-suppression-top-n",
                "14",
                "--retune-weather-window-hours-cap",
                "240",
                "--retune-overblocking-blocked-share-threshold",
                "0.30",
                "--retune-underblocking-min-top-n",
                "18",
                "--retune-overblocking-max-top-n",
                "5",
                "--retune-min-bucket-samples-target",
                "15",
                "--retune-expectancy-threshold-target",
                "-0.05",
                "--execute-actions",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs/live")
        self.assertEqual(kwargs["trader_env_file"], "/tmp/runtime.env")
        self.assertEqual(kwargs["max_iterations"], 6)
        self.assertEqual(kwargs["stall_iterations"], 3)
        self.assertAlmostEqual(kwargs["min_gap_improvement"], 0.025, places=6)
        self.assertAlmostEqual(kwargs["weather_window_hours"], 744.0, places=6)
        self.assertEqual(kwargs["weather_min_bucket_samples"], 16)
        self.assertAlmostEqual(kwargs["weather_max_profile_age_hours"], 300.0, places=6)
        self.assertAlmostEqual(kwargs["weather_negative_expectancy_attempt_share_target"], 0.42, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_negative_attempt_share_target"], 0.51, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_attempt_share_target"], 0.56, places=6)
        self.assertEqual(kwargs["weather_min_attempts_target"], 260)
        self.assertEqual(kwargs["optimizer_top_n"], 8)
        self.assertIs(kwargs["plateau_negative_regime_suppression_enabled"], False)
        self.assertEqual(kwargs["plateau_negative_regime_suppression_min_bucket_samples"], 24)
        self.assertAlmostEqual(kwargs["plateau_negative_regime_suppression_expectancy_threshold"], -0.08, places=6)
        self.assertEqual(kwargs["plateau_negative_regime_suppression_top_n"], 14)
        self.assertAlmostEqual(kwargs["retune_weather_window_hours_cap"], 240.0, places=6)
        self.assertAlmostEqual(kwargs["retune_overblocking_blocked_share_threshold"], 0.30, places=6)
        self.assertEqual(kwargs["retune_underblocking_min_top_n"], 18)
        self.assertEqual(kwargs["retune_overblocking_max_top_n"], 5)
        self.assertEqual(kwargs["retune_min_bucket_samples_target"], 15)
        self.assertAlmostEqual(kwargs["retune_expectancy_threshold_target"], -0.05, places=6)
        self.assertIs(kwargs["execute_actions"], True)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "running")

    def test_cli_temperature_recovery_loop_alias_defaults_no_execute(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_recovery_loop",
            return_value={"status": "ready"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-recovery-loop",
                "--no-execute-actions",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertEqual(kwargs["trader_env_file"], "data/research/account_onboarding.env.template")
        self.assertEqual(kwargs["max_iterations"], 4)
        self.assertEqual(kwargs["stall_iterations"], 2)
        self.assertAlmostEqual(kwargs["min_gap_improvement"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["weather_window_hours"], 720.0, places=6)
        self.assertEqual(kwargs["weather_min_bucket_samples"], 10)
        self.assertAlmostEqual(kwargs["weather_max_profile_age_hours"], 336.0, places=6)
        self.assertAlmostEqual(kwargs["weather_negative_expectancy_attempt_share_target"], 0.50, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_negative_attempt_share_target"], 0.60, places=6)
        self.assertAlmostEqual(kwargs["weather_stale_metar_attempt_share_target"], 0.65, places=6)
        self.assertEqual(kwargs["weather_min_attempts_target"], 200)
        self.assertEqual(kwargs["optimizer_top_n"], 5)
        self.assertIs(kwargs["plateau_negative_regime_suppression_enabled"], True)
        self.assertEqual(kwargs["plateau_negative_regime_suppression_min_bucket_samples"], 18)
        self.assertAlmostEqual(kwargs["plateau_negative_regime_suppression_expectancy_threshold"], -0.06, places=6)
        self.assertEqual(kwargs["plateau_negative_regime_suppression_top_n"], 10)
        self.assertAlmostEqual(kwargs["retune_weather_window_hours_cap"], 336.0, places=6)
        self.assertAlmostEqual(kwargs["retune_overblocking_blocked_share_threshold"], 0.25, places=6)
        self.assertEqual(kwargs["retune_underblocking_min_top_n"], 16)
        self.assertEqual(kwargs["retune_overblocking_max_top_n"], 4)
        self.assertEqual(kwargs["retune_min_bucket_samples_target"], 14)
        self.assertAlmostEqual(kwargs["retune_expectancy_threshold_target"], -0.045, places=6)
        self.assertIs(kwargs["execute_actions"], False)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")


if __name__ == "__main__":
    unittest.main()
