from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliTemperatureTraderTests(unittest.TestCase):
    def test_cli_temperature_trader_probability_edge_enforcement_defaults(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["enforce_probability_edge_thresholds"])
        self.assertIsNone(kwargs["fallback_min_probability_confidence"])
        self.assertEqual(kwargs["fallback_min_expected_edge_net"], 0.005)
        self.assertEqual(kwargs["fallback_min_edge_to_risk_ratio"], 0.02)
        self.assertEqual(kwargs["min_base_edge_net"], 0.0)
        self.assertEqual(kwargs["min_probability_breakeven_gap"], 0.0)
        self.assertEqual(kwargs["max_orders_per_station"], 2)
        self.assertEqual(kwargs["max_orders_per_underlying"], 2)
        self.assertEqual(kwargs["min_unique_stations_per_loop"], 3)
        self.assertEqual(kwargs["min_unique_underlyings_per_loop"], 4)
        self.assertEqual(kwargs["min_unique_local_hours_per_loop"], 2)
        self.assertEqual(kwargs["replan_market_side_repeat_window_minutes"], 1440.0)
        self.assertEqual(kwargs["replan_market_side_max_plans_per_window"], 8)
        self.assertTrue(kwargs["historical_selection_quality_enabled"])
        self.assertEqual(kwargs["historical_selection_quality_lookback_hours"], 336.0)
        self.assertEqual(kwargs["historical_selection_quality_min_resolved_market_sides"], 12)
        self.assertEqual(kwargs["historical_selection_quality_min_bucket_samples"], 4)
        self.assertEqual(kwargs["historical_selection_quality_probability_penalty_max"], 0.05)
        self.assertEqual(kwargs["historical_selection_quality_expected_edge_penalty_max"], 0.006)
        self.assertEqual(kwargs["historical_selection_quality_score_adjust_scale"], 0.35)
        self.assertEqual(kwargs["historical_selection_quality_profile_max_age_hours"], 96.0)
        self.assertEqual(
            kwargs["historical_selection_quality_preferred_model"],
            "fixed_fraction_per_underlying_family",
        )
        self.assertIsNone(kwargs["metar_ingest_min_quality_score"])
        self.assertIsNone(kwargs["metar_ingest_min_fresh_station_coverage_ratio"])
        self.assertFalse(kwargs["metar_ingest_require_ready_status"])
        self.assertFalse(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_metar_ingest_gate_flags_forwarded(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--min-metar-ingest-quality-score",
                "0.91",
                "--min-metar-fresh-station-coverage-ratio",
                "0.82",
                "--require-metar-ingest-status-ready",
                "--high-price-edge-guard-enabled",
                "--high-price-edge-guard-min-entry-price-dollars",
                "0.84",
                "--high-price-edge-guard-min-expected-edge-net",
                "0.01",
                "--high-price-edge-guard-min-edge-to-risk-ratio",
                "0.03",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertAlmostEqual(kwargs["metar_ingest_min_quality_score"], 0.91, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_fresh_station_coverage_ratio"], 0.82, places=6)
        self.assertTrue(kwargs["metar_ingest_require_ready_status"])
        self.assertTrue(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.84, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.03, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_micro_live_50_profile_caps_risk(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--allow-live-orders",
                "--micro-live-50",
                "--planning-bankroll",
                "200",
                "--daily-risk-cap",
                "9",
                "--max-total-deployed-pct",
                "0.5",
                "--max-live-cost-per-day-dollars",
                "12",
                "--max-live-submissions-per-day",
                "9",
                "--max-orders",
                "10",
                "--contracts-per-order",
                "3",
                "--min-metar-ingest-quality-score",
                "0.2",
                "--min-metar-fresh-station-coverage-ratio",
                "0.1",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertAlmostEqual(kwargs["planning_bankroll_dollars"], 50.0, places=6)
        self.assertAlmostEqual(kwargs["daily_risk_cap_dollars"], 3.0, places=6)
        self.assertAlmostEqual(kwargs["max_total_deployed_pct"], 0.2, places=6)
        self.assertAlmostEqual(kwargs["max_live_cost_per_day_dollars"], 3.0, places=6)
        self.assertEqual(kwargs["max_live_submissions_per_day"], 3)
        self.assertEqual(kwargs["max_orders"], 3)
        self.assertEqual(kwargs["contracts_per_order"], 1)
        self.assertAlmostEqual(kwargs["yes_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["no_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_quality_score"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_fresh_station_coverage_ratio"], 0.75, places=6)
        self.assertTrue(kwargs["metar_ingest_require_ready_status"])
        self.assertTrue(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["risk_profile"], "micro_live_50")
        self.assertTrue(payload["risk_profile_applied"])
        self.assertAlmostEqual(payload["risk_profile_caps"]["max_total_open_exposure_dollars"], 10.0, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["yes_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["no_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["min_metar_ingest_quality_score"], 0.85, places=6)
        self.assertAlmostEqual(
            payload["risk_profile_caps"]["min_metar_fresh_station_coverage_ratio"],
            0.75,
            places=6,
        )
        self.assertTrue(payload["risk_profile_caps"]["require_metar_ingest_status_ready"])
        self.assertTrue(payload["risk_profile_caps"]["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)

    def test_cli_temperature_trader_consumes_optimizer_profile_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "optimizer_profile.json"
            profile_path.write_text(
                json.dumps(
                    {
                        "optimizer_profile": {
                            "historical_selection_quality_enabled": True,
                            "historical_selection_quality_lookback_hours": 240,
                            "historical_selection_quality_min_resolved_market_sides": 18,
                            "historical_selection_quality_min_bucket_samples": 6,
                            "historical_selection_quality_probability_penalty_max": 0.07,
                            "historical_selection_quality_expected_edge_penalty_max": 0.009,
                            "historical_selection_quality_score_adjust_scale": 0.42,
                            "historical_selection_quality_profile_max_age_hours": 72,
                            "historical_selection_quality_preferred_model": "fixed_unit_risk_budget",
                        }
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "betbot.cli.run_kalshi_temperature_trader",
                return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
            ) as mock_runner, patch.object(
                sys,
                "argv",
                [
                    "betbot",
                    "kalshi-temperature-trader",
                    "--env-file",
                    "dummy.env",
                    "--output-dir",
                    "outputs",
                    "--optimizer-profile-json",
                    str(profile_path),
                ],
            ):
                stdout = StringIO()
                with redirect_stdout(stdout):
                    cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["historical_selection_quality_enabled"])
        self.assertAlmostEqual(kwargs["historical_selection_quality_lookback_hours"], 240.0, places=6)
        self.assertEqual(kwargs["historical_selection_quality_min_resolved_market_sides"], 18)
        self.assertEqual(kwargs["historical_selection_quality_min_bucket_samples"], 6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_probability_penalty_max"], 0.07, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_expected_edge_penalty_max"], 0.009, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_score_adjust_scale"], 0.42, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_profile_max_age_hours"], 72.0, places=6)
        self.assertEqual(kwargs["historical_selection_quality_preferred_model"], "fixed_unit_risk_budget")
        self.assertIn('"optimizer_profile_application_status": "applied"', stdout.getvalue())

    def test_cli_temperature_trader_consumes_weather_pattern_profile_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "weather_pattern_profile.json"
            profile_payload = {
                "station_biases": {"KMIA": 0.35, "KJFK": 0.2},
                "recent_window_hours": 48,
                "min_pattern_samples": 12,
            }
            profile_path.write_text(json.dumps(profile_payload), encoding="utf-8")

            with patch(
                "betbot.cli.run_kalshi_temperature_trader",
                return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
            ) as mock_runner, patch.object(
                sys,
                "argv",
                [
                    "betbot",
                    "kalshi-temperature-trader",
                    "--env-file",
                    "dummy.env",
                    "--output-dir",
                    "outputs",
                    "--weather-pattern-profile-json",
                    str(profile_path),
                ],
            ):
                stdout = StringIO()
                with redirect_stdout(stdout):
                    cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertEqual(kwargs["weather_pattern_profile"], profile_payload)
        self.assertIn('"weather_pattern_profile_application_status": "applied"', stdout.getvalue())
        self.assertIn(str(profile_path), stdout.getvalue())
        self.assertIn('"weather_pattern_profile_source_file":', stdout.getvalue())
        self.assertIn('"weather_pattern_profile_applied": true', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_risk_off_flags_and_summary(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={
                "status": "ready",
                "output_file": "outputs/trader_summary.json",
                "plan_summary": {
                    "weather_pattern_risk_off": {
                        "enabled": True,
                        "applied": True,
                        "application_status": "applied",
                        "status": "triggered",
                        "concentration_threshold": 0.61,
                        "min_attempts": 27,
                        "stale_metar_share_threshold": 0.35,
                    }
                },
            },
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--weather-pattern-risk-off-enabled",
                "--weather-pattern-risk-off-concentration-threshold",
                "0.52",
                "--weather-pattern-risk-off-min-attempts",
                "19",
                "--weather-pattern-risk-off-stale-metar-share-threshold",
                "0.3",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["weather_pattern_risk_off_enabled"])
        self.assertAlmostEqual(kwargs["weather_pattern_risk_off_concentration_threshold"], 0.52, places=6)
        self.assertEqual(kwargs["weather_pattern_risk_off_min_attempts"], 19)
        self.assertAlmostEqual(kwargs["weather_pattern_risk_off_stale_metar_share_threshold"], 0.3, places=6)

        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["weather_pattern_risk_off_enabled"])
        self.assertTrue(payload["weather_pattern_risk_off_applied"])
        self.assertEqual(payload["weather_pattern_risk_off_application_status"], "applied")
        self.assertEqual(payload["weather_pattern_risk_off_status"], "triggered")
        self.assertAlmostEqual(payload["weather_pattern_risk_off_concentration_threshold"], 0.61, places=6)
        self.assertEqual(payload["weather_pattern_risk_off_min_attempts"], 27)
        self.assertAlmostEqual(payload["weather_pattern_risk_off_stale_metar_share_threshold"], 0.35, places=6)

    def test_cli_temperature_trader_weather_pattern_risk_off_disable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--no-weather-pattern-risk-off-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["weather_pattern_risk_off_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_hardening_enable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--weather-pattern-hardening-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["weather_pattern_hardening_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_hardening_disable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--no-weather-pattern-hardening-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["weather_pattern_hardening_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_hardening_disable_alias_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--disable-weather-pattern-hardening",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["weather_pattern_hardening_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_negative_bucket_suppression_flags(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--weather-pattern-negative-bucket-suppression-enabled",
                "--weather-pattern-negative-bucket-suppression-top-n",
                "7",
                "--weather-pattern-negative-bucket-suppression-min-samples",
                "15",
                "--weather-pattern-negative-bucket-suppression-negative-expectancy-threshold",
                "-0.07",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["weather_pattern_negative_regime_suppression_enabled"])
        self.assertEqual(kwargs["weather_pattern_negative_regime_suppression_top_n"], 7)
        self.assertEqual(kwargs["weather_pattern_negative_regime_suppression_min_bucket_samples"], 15)
        self.assertAlmostEqual(
            kwargs["weather_pattern_negative_regime_suppression_expectancy_threshold"],
            -0.07,
            places=6,
        )
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_weather_pattern_negative_bucket_suppression_disable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--no-weather-pattern-negative-bucket-suppression-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["weather_pattern_negative_regime_suppression_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_trader_probability_edge_enforcement_overrides(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_trader",
            return_value={"status": "ready", "output_file": "outputs/trader_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-trader",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--disable-enforce-probability-edge-thresholds",
                "--fallback-min-probability-confidence",
                "0.72",
                "--fallback-min-expected-edge-net",
                "0.03",
                "--fallback-min-edge-to-risk-ratio",
                "0.04",
                "--min-base-edge-net",
                "0.01",
                "--min-probability-breakeven-gap",
                "0.02",
                "--max-orders-per-station",
                "3",
                "--max-orders-per-underlying",
                "4",
                "--min-unique-stations-per-loop",
                "2",
                "--min-unique-underlyings-per-loop",
                "3",
                "--min-unique-local-hours-per-loop",
                "2",
                "--replan-market-side-repeat-window-minutes",
                "720",
                "--replan-market-side-max-plans-per-window",
                "5",
                "--disable-historical-selection-quality",
                "--historical-selection-quality-lookback-hours",
                "672",
                "--historical-selection-quality-min-resolved-market-sides",
                "24",
                "--historical-selection-quality-min-bucket-samples",
                "6",
                "--historical-selection-quality-probability-penalty-max",
                "0.08",
                "--historical-selection-quality-expected-edge-penalty-max",
                "0.01",
                "--historical-selection-quality-score-adjust-scale",
                "0.5",
                "--historical-selection-quality-profile-max-age-hours",
                "48",
                "--historical-selection-quality-preferred-model",
                "fixed_fraction_per_unique_market_side",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["enforce_probability_edge_thresholds"])
        self.assertAlmostEqual(kwargs["fallback_min_probability_confidence"], 0.72, places=6)
        self.assertAlmostEqual(kwargs["fallback_min_expected_edge_net"], 0.03, places=6)
        self.assertAlmostEqual(kwargs["fallback_min_edge_to_risk_ratio"], 0.04, places=6)
        self.assertAlmostEqual(kwargs["min_base_edge_net"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["min_probability_breakeven_gap"], 0.02, places=6)
        self.assertEqual(kwargs["max_orders_per_station"], 3)
        self.assertEqual(kwargs["max_orders_per_underlying"], 4)
        self.assertEqual(kwargs["min_unique_stations_per_loop"], 2)
        self.assertEqual(kwargs["min_unique_underlyings_per_loop"], 3)
        self.assertEqual(kwargs["min_unique_local_hours_per_loop"], 2)
        self.assertAlmostEqual(kwargs["replan_market_side_repeat_window_minutes"], 720.0, places=6)
        self.assertEqual(kwargs["replan_market_side_max_plans_per_window"], 5)
        self.assertFalse(kwargs["historical_selection_quality_enabled"])
        self.assertAlmostEqual(kwargs["historical_selection_quality_lookback_hours"], 672.0, places=6)
        self.assertEqual(kwargs["historical_selection_quality_min_resolved_market_sides"], 24)
        self.assertEqual(kwargs["historical_selection_quality_min_bucket_samples"], 6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_probability_penalty_max"], 0.08, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_expected_edge_penalty_max"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_score_adjust_scale"], 0.5, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_profile_max_age_hours"], 48.0, places=6)
        self.assertEqual(
            kwargs["historical_selection_quality_preferred_model"],
            "fixed_fraction_per_unique_market_side",
        )
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_shadow_watch_metar_ingest_gate_defaults(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertIsNone(kwargs["metar_ingest_min_quality_score"])
        self.assertIsNone(kwargs["metar_ingest_min_fresh_station_coverage_ratio"])
        self.assertFalse(kwargs["metar_ingest_require_ready_status"])
        self.assertFalse(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_shadow_watch_metar_ingest_gate_flags_forwarded(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--min-metar-ingest-quality-score",
                "0.9",
                "--min-metar-fresh-station-coverage-ratio",
                "0.81",
                "--require-metar-ingest-status-ready",
                "--high-price-edge-guard-enabled",
                "--high-price-edge-guard-min-entry-price-dollars",
                "0.83",
                "--high-price-edge-guard-min-expected-edge-net",
                "0.01",
                "--high-price-edge-guard-min-edge-to-risk-ratio",
                "0.04",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertAlmostEqual(kwargs["metar_ingest_min_quality_score"], 0.9, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_fresh_station_coverage_ratio"], 0.81, places=6)
        self.assertTrue(kwargs["metar_ingest_require_ready_status"])
        self.assertTrue(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.83, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.01, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.04, places=6)
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_shadow_watch_probability_edge_overrides(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--disable-enforce-probability-edge-thresholds",
                "--fallback-min-probability-confidence",
                "0.68",
                "--fallback-min-expected-edge-net",
                "0.04",
                "--fallback-min-edge-to-risk-ratio",
                "0.05",
                "--min-base-edge-net",
                "0.02",
                "--min-probability-breakeven-gap",
                "0.03",
                "--max-orders-per-station",
                "3",
                "--max-orders-per-underlying",
                "3",
                "--min-unique-stations-per-loop",
                "2",
                "--min-unique-underlyings-per-loop",
                "2",
                "--min-unique-local-hours-per-loop",
                "1",
                "--replan-market-side-repeat-window-minutes",
                "1080",
                "--replan-market-side-max-plans-per-window",
                "4",
                "--disable-historical-selection-quality",
                "--historical-selection-quality-lookback-hours",
                "240",
                "--historical-selection-quality-min-resolved-market-sides",
                "18",
                "--historical-selection-quality-min-bucket-samples",
                "5",
                "--historical-selection-quality-probability-penalty-max",
                "0.06",
                "--historical-selection-quality-expected-edge-penalty-max",
                "0.009",
                "--historical-selection-quality-score-adjust-scale",
                "0.42",
                "--historical-selection-quality-profile-max-age-hours",
                "36",
                "--historical-selection-quality-preferred-model",
                "fixed_unit_risk_budget",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["enforce_probability_edge_thresholds"])
        self.assertAlmostEqual(kwargs["fallback_min_probability_confidence"], 0.68, places=6)
        self.assertAlmostEqual(kwargs["fallback_min_expected_edge_net"], 0.04, places=6)
        self.assertAlmostEqual(kwargs["fallback_min_edge_to_risk_ratio"], 0.05, places=6)
        self.assertAlmostEqual(kwargs["min_base_edge_net"], 0.02, places=6)
        self.assertAlmostEqual(kwargs["min_probability_breakeven_gap"], 0.03, places=6)
        self.assertEqual(kwargs["max_orders_per_station"], 3)
        self.assertEqual(kwargs["max_orders_per_underlying"], 3)
        self.assertEqual(kwargs["min_unique_stations_per_loop"], 2)
        self.assertEqual(kwargs["min_unique_underlyings_per_loop"], 2)
        self.assertEqual(kwargs["min_unique_local_hours_per_loop"], 1)
        self.assertAlmostEqual(kwargs["replan_market_side_repeat_window_minutes"], 1080.0, places=6)
        self.assertEqual(kwargs["replan_market_side_max_plans_per_window"], 4)
        self.assertFalse(kwargs["historical_selection_quality_enabled"])
        self.assertAlmostEqual(kwargs["historical_selection_quality_lookback_hours"], 240.0, places=6)
        self.assertEqual(kwargs["historical_selection_quality_min_resolved_market_sides"], 18)
        self.assertEqual(kwargs["historical_selection_quality_min_bucket_samples"], 5)
        self.assertAlmostEqual(kwargs["historical_selection_quality_probability_penalty_max"], 0.06, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_expected_edge_penalty_max"], 0.009, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_score_adjust_scale"], 0.42, places=6)
        self.assertAlmostEqual(kwargs["historical_selection_quality_profile_max_age_hours"], 36.0, places=6)
        self.assertEqual(
            kwargs["historical_selection_quality_preferred_model"],
            "fixed_unit_risk_budget",
        )
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_shadow_watch_micro_live_50_profile_caps_risk(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--allow-live-orders",
                "--micro-live-50",
                "--planning-bankroll",
                "150",
                "--daily-risk-cap",
                "8",
                "--max-total-deployed-pct",
                "0.7",
                "--max-live-cost-per-day-dollars",
                "8",
                "--max-live-submissions-per-day",
                "6",
                "--max-orders",
                "12",
                "--contracts-per-order",
                "5",
                "--min-metar-ingest-quality-score",
                "0.3",
                "--min-metar-fresh-station-coverage-ratio",
                "0.2",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertAlmostEqual(kwargs["planning_bankroll_dollars"], 50.0, places=6)
        self.assertAlmostEqual(kwargs["daily_risk_cap_dollars"], 3.0, places=6)
        self.assertAlmostEqual(kwargs["max_total_deployed_pct"], 0.2, places=6)
        self.assertAlmostEqual(kwargs["max_live_cost_per_day_dollars"], 3.0, places=6)
        self.assertEqual(kwargs["max_live_submissions_per_day"], 3)
        self.assertEqual(kwargs["max_orders"], 3)
        self.assertEqual(kwargs["contracts_per_order"], 1)
        self.assertAlmostEqual(kwargs["yes_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["no_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_quality_score"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["metar_ingest_min_fresh_station_coverage_ratio"], 0.75, places=6)
        self.assertTrue(kwargs["metar_ingest_require_ready_status"])
        self.assertTrue(kwargs["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(kwargs["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["risk_profile"], "micro_live_50")
        self.assertTrue(payload["risk_profile_applied"])
        self.assertAlmostEqual(payload["risk_profile_caps"]["max_total_open_exposure_dollars"], 10.0, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["yes_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["no_max_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["min_metar_ingest_quality_score"], 0.85, places=6)
        self.assertAlmostEqual(
            payload["risk_profile_caps"]["min_metar_fresh_station_coverage_ratio"],
            0.75,
            places=6,
        )
        self.assertTrue(payload["risk_profile_caps"]["require_metar_ingest_status_ready"])
        self.assertTrue(payload["risk_profile_caps"]["high_price_edge_guard_enabled"])
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_entry_price_dollars"], 0.85, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_expected_edge_net"], 0.0, places=6)
        self.assertAlmostEqual(payload["risk_profile_caps"]["high_price_edge_guard_min_edge_to_risk_ratio"], 0.02, places=6)

    def test_cli_temperature_shadow_watch_weather_pattern_hardening_enable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--weather-pattern-hardening-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertTrue(kwargs["weather_pattern_hardening_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())

    def test_cli_temperature_shadow_watch_weather_pattern_hardening_disable_flag(self) -> None:
        with patch(
            "betbot.cli.run_kalshi_temperature_shadow_watch",
            return_value={"status": "ready", "output_file": "outputs/shadow_summary.json"},
        ) as mock_runner, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "kalshi-temperature-shadow-watch",
                "--env-file",
                "dummy.env",
                "--output-dir",
                "outputs",
                "--no-weather-pattern-hardening-enabled",
            ],
        ):
            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(kwargs["weather_pattern_hardening_enabled"])
        self.assertIn('"status": "ready"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
