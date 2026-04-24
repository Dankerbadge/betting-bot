from __future__ import annotations

import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureRecoveryCampaignTests(unittest.TestCase):
    def test_cli_temperature_recovery_campaign_alias_uses_defaults(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_recovery_campaign",
            return_value={"status": "ready"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "temperature-recovery-campaign",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertEqual(kwargs["trader_env_file"], "data/research/account_onboarding.env.template")
        self.assertIs(kwargs["execute_actions"], True)
        self.assertIsNone(kwargs["profiles"])
        self.assertEqual(
            kwargs["advisor_targets"],
            {
                "weather_window_hours": 720.0,
                "weather_min_bucket_samples": 10,
                "weather_max_profile_age_hours": 336.0,
                "weather_negative_expectancy_attempt_share_target": 0.50,
                "weather_stale_metar_negative_attempt_share_target": 0.60,
                "weather_stale_metar_attempt_share_target": 0.65,
                "weather_min_attempts_target": 200,
                "optimizer_top_n": 5,
                "plateau_negative_regime_suppression_enabled": True,
                "plateau_negative_regime_suppression_min_bucket_samples": 18,
                "plateau_negative_regime_suppression_expectancy_threshold": -0.06,
                "plateau_negative_regime_suppression_top_n": 10,
                "retune_weather_window_hours_cap": 336.0,
                "retune_overblocking_blocked_share_threshold": 0.25,
                "retune_underblocking_min_top_n": 16,
                "retune_overblocking_max_top_n": 4,
                "retune_min_bucket_samples_target": 14,
                "retune_expectancy_threshold_target": -0.045,
            },
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")

    def test_cli_temperature_recovery_campaign_forwards_explicit_args_and_profiles_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profiles_path = Path(temp_dir) / "profiles.json"
            profiles_payload = [
                {
                    "name": "wide_window",
                    "weather_window_hours": 960.0,
                    "weather_min_bucket_samples": 12,
                    "weather_max_profile_age_hours": 240.0,
                    "optimizer_top_n": 7,
                },
                {"name": "tight_window", "weather_window_hours": 336.0},
            ]
            profiles_path.write_text(json.dumps(profiles_payload), encoding="utf-8")

            with patch(
                "betbot.cli.run_kalshi_temperature_recovery_campaign",
                return_value={"status": "running"},
            ) as mock_runner, patch.object(
                sys,
                "argv",
                [
                    "betbot",
                    "kalshi-temperature-recovery-campaign",
                    "--output-dir",
                    "outputs/campaign",
                    "--trader-env-file",
                    "/tmp/runtime.env",
                    "--no-execute-actions",
                    "--profiles-json",
                    str(profiles_path),
                    "--weather-window-hours",
                    "744",
                    "--weather-min-bucket-samples",
                    "16",
                    "--weather-max-profile-age-hours",
                    "288",
                    "--weather-negative-expectancy-attempt-share-target",
                    "0.44",
                    "--weather-stale-metar-negative-attempt-share-target",
                    "0.52",
                    "--weather-stale-metar-attempt-share-target",
                    "0.57",
                    "--weather-min-attempts-target",
                    "260",
                    "--optimizer-top-n",
                    "8",
                    "--no-plateau-negative-regime-suppression-enabled",
                    "--plateau-negative-regime-suppression-min-bucket-samples",
                    "22",
                    "--plateau-negative-regime-suppression-expectancy-threshold",
                    "-0.09",
                    "--plateau-negative-regime-suppression-top-n",
                    "12",
                    "--retune-weather-window-hours-cap",
                    "288",
                    "--retune-overblocking-blocked-share-threshold",
                    "0.28",
                    "--retune-underblocking-min-top-n",
                    "20",
                    "--retune-overblocking-max-top-n",
                    "6",
                    "--retune-min-bucket-samples-target",
                    "17",
                    "--retune-expectancy-threshold-target",
                    "-0.055",
                ],
            ):
                stdout = StringIO()
                with redirect_stdout(stdout):
                    cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs/campaign")
        self.assertEqual(kwargs["trader_env_file"], "/tmp/runtime.env")
        self.assertIs(kwargs["execute_actions"], False)
        self.assertEqual(kwargs["profiles"], profiles_payload)
        self.assertEqual(
            kwargs["advisor_targets"],
            {
                "weather_window_hours": 744.0,
                "weather_min_bucket_samples": 16,
                "weather_max_profile_age_hours": 288.0,
                "weather_negative_expectancy_attempt_share_target": 0.44,
                "weather_stale_metar_negative_attempt_share_target": 0.52,
                "weather_stale_metar_attempt_share_target": 0.57,
                "weather_min_attempts_target": 260,
                "optimizer_top_n": 8,
                "plateau_negative_regime_suppression_enabled": False,
                "plateau_negative_regime_suppression_min_bucket_samples": 22,
                "plateau_negative_regime_suppression_expectancy_threshold": -0.09,
                "plateau_negative_regime_suppression_top_n": 12,
                "retune_weather_window_hours_cap": 288.0,
                "retune_overblocking_blocked_share_threshold": 0.28,
                "retune_underblocking_min_top_n": 20,
                "retune_overblocking_max_top_n": 6,
                "retune_min_bucket_samples_target": 17,
                "retune_expectancy_threshold_target": -0.055,
            },
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "running")

    def test_cli_temperature_recovery_campaign_selects_best_non_cleared_profile_with_better_settled_blocker_state(
        self,
    ) -> None:
        def _loop_payload(
            *,
            output_dir: str,
            profile_name: str,
            max_iterations: int,
            intents_approved: int,
            settled_blocker_active: bool,
        ) -> dict[str, object]:
            return {
                "termination_reason": "stalled",
                "iterations_executed": max_iterations,
                "initial_gap_score": 1.0,
                "final_gap_score": 0.50,
                "final_advisor_status": "risk_off_active",
                "output_file": str(Path(output_dir) / f"loop_{profile_name}.json"),
                "final_advisor": {
                    "remediation_plan": {
                        "status": "risk_off_active",
                        "prioritized_actions": (
                            [{"key": "increase_settled_outcome_coverage"}]
                            if settled_blocker_active
                            else [{"key": "clear_weather_risk_off_state"}]
                        ),
                    },
                    "metrics": {
                        "weather": {
                            "negative_expectancy_attempt_share": 0.61,
                            "stale_metar_negative_attempt_share": 0.66,
                            "stale_metar_attempt_share": 0.67,
                        },
                        "trade_plan_blockers": {
                            "intents_total": 40,
                            "intents_approved": intents_approved,
                            "policy_reason_counts": {
                                "expected_edge_below_min": 2,
                            },
                        },
                        "decision_matrix": {
                            "settled_outcomes_insufficient": settled_blocker_active,
                            "blockers": (
                                [{"key": "settled_outcome_growth_stalled", "severity": "high"}]
                                if settled_blocker_active
                                else []
                            ),
                        },
                    },
                },
            }

        def fake_loop(**kwargs):
            output_dir = str(kwargs["output_dir"])
            max_iterations = int(kwargs["max_iterations"])
            if max_iterations == 4:
                return _loop_payload(
                    output_dir=output_dir,
                    profile_name="steady_4x2",
                    max_iterations=max_iterations,
                    intents_approved=4,
                    settled_blocker_active=True,
                )
            if max_iterations == 6:
                return _loop_payload(
                    output_dir=output_dir,
                    profile_name="extended_6x3",
                    max_iterations=max_iterations,
                    intents_approved=12,
                    settled_blocker_active=False,
                )
            return _loop_payload(
                output_dir=output_dir,
                profile_name="focused_3x2",
                max_iterations=max_iterations,
                intents_approved=7,
                settled_blocker_active=True,
            )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "betbot.kalshi_temperature_recovery_campaign.run_kalshi_temperature_recovery_loop",
            side_effect=fake_loop,
        ), patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-recovery-campaign",
                "--output-dir",
                temp_dir,
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(int(payload["profiles_evaluated"]), 3)
        run_rows = payload["runs"]
        self.assertEqual(len(run_rows), 3)
        self.assertEqual(
            {str(row["final_advisor_status"]) for row in run_rows},
            {"risk_off_active"},
        )
        best_profile = payload["best_profile"]
        self.assertEqual(best_profile["name"], "extended_6x3")
        self.assertEqual(int(best_profile["final_intents_approved"]), 12)
        self.assertEqual(best_profile["final_advisor_status"], "risk_off_active")
        overrides = payload["recommended_env_overrides"]
        self.assertEqual(overrides["profile_name"], "extended_6x3")
        self.assertEqual(overrides["env"]["COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS"], "6")


if __name__ == "__main__":
    unittest.main()
