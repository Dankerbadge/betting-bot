from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from betbot.kalshi_temperature_constraints import (
    _alpha_feature_pack,
    _load_speci_calibration,
    _exact_strike_chain_summary,
    _range_family_consistency_summary,
    evaluate_temperature_constraint,
    infer_settlement_unit,
    run_kalshi_temperature_constraint_scan,
)


class KalshiTemperatureConstraintsTests(unittest.TestCase):
    def test_evaluate_yes_impossible_for_at_most(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_most:72",
            observed_value=74.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("exceeds", reason)

    def test_evaluate_yes_likely_locked_for_at_least(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_least:75",
            observed_value=75.0,
        )
        self.assertEqual(status, "yes_likely_locked")
        self.assertIn("meets", reason)

    def test_evaluate_no_signal_for_between_in_range(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="between:70:74",
            observed_value=72.0,
        )
        self.assertEqual(status, "no_signal")
        self.assertIn("currently satisfied", reason)

    def test_evaluate_between_in_range_ignores_stale_forecast_impossible(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="between:89:90",
            observed_value=89.0,
            temperature_metric="daily_high",
            forecast_upper_bound=76.0,
        )
        self.assertEqual(status, "no_signal")
        self.assertIn("currently satisfied", reason)

    def test_evaluate_high_market_uses_forecast_upper_bound_for_impossible(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="above:84",
            observed_value=79.0,
            temperature_metric="daily_high",
            forecast_upper_bound=83.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("does not exceed", reason)

    def test_evaluate_high_market_uses_forecast_upper_bound_even_when_below_observed(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="above:84",
            observed_value=79.0,
            temperature_metric="daily_high",
            forecast_upper_bound=70.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("does not exceed", reason)

    def test_evaluate_low_market_uses_min_metric_and_forecast_lower_bound(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_most:35",
            observed_value=41.0,
            temperature_metric="daily_low",
            forecast_lower_bound=38.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("stays above", reason)

    def test_evaluate_low_market_uses_forecast_lower_bound_even_when_above_observed(self) -> None:
        status, reason = evaluate_temperature_constraint(
            threshold_expression="at_most:35",
            observed_value=41.0,
            temperature_metric="daily_low",
            forecast_lower_bound=45.0,
        )
        self.assertEqual(status, "yes_impossible")
        self.assertIn("stays above", reason)

    def test_alpha_feature_pack_daily_high_clamps_possible_upper_to_observed_when_forecast_below_observed(self) -> None:
        features = _alpha_feature_pack(
            threshold_expression="above:84",
            threshold_kind="above",
            threshold_lower=84.0,
            threshold_upper=None,
            temperature_metric="daily_high",
            observed_value=79.0,
            forecast_upper_bound=70.0,
            forecast_lower_bound=65.0,
        )
        self.assertEqual(features["yes_possible_overlap"], 0)
        self.assertAlmostEqual(float(features["yes_possible_gap"] or 0.0), 5.000000001, places=6)
        self.assertEqual(features["possible_final_upper_bound"], 79.0)
        self.assertEqual(features["forecast_feasibility_margin"], -14.0)

    def test_alpha_feature_pack_daily_low_clamps_possible_lower_to_observed_when_forecast_above_observed(self) -> None:
        features = _alpha_feature_pack(
            threshold_expression="at_most:35",
            threshold_kind="at_most",
            threshold_lower=None,
            threshold_upper=35.0,
            temperature_metric="daily_low",
            observed_value=41.0,
            forecast_upper_bound=50.0,
            forecast_lower_bound=45.0,
        )
        self.assertEqual(features["yes_possible_overlap"], 0)
        self.assertEqual(float(features["yes_possible_gap"] or 0.0), 6.0)
        self.assertEqual(features["possible_final_lower_bound"], 41.0)
        self.assertEqual(features["forecast_feasibility_margin"], -10.0)

    def test_infer_settlement_unit_defaults_to_fahrenheit(self) -> None:
        unit = infer_settlement_unit("Highest temperature in NYC", "local day")
        self.assertEqual(unit, "fahrenheit")

    def test_infer_settlement_unit_detects_celsius(self) -> None:
        unit = infer_settlement_unit("Highest temperature in Paris", "Resolve in Celsius")
        self.assertEqual(unit, "celsius")

    def test_infer_settlement_unit_does_not_false_positive_city_letter_c(self) -> None:
        unit = infer_settlement_unit(
            "Highest temperature in Chicago",
            "Local day settlement window",
            threshold_expression="between:50:51",
        )
        self.assertEqual(unit, "fahrenheit")

    def test_load_speci_calibration_reads_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calibration_path = Path(tmp) / "speci_calibration.json"
            calibration_path.write_text(
                json.dumps(
                    {
                        "version": "speci_calibration_v2",
                        "confidence_threshold_active": 0.6,
                        "severity_multiplier": 1.3,
                    }
                ),
                encoding="utf-8",
            )
            calibration = _load_speci_calibration(str(calibration_path))
        self.assertTrue(calibration["loaded"])
        self.assertEqual(calibration["error"], "")
        self.assertEqual(calibration["version"], "speci_calibration_v2")
        self.assertEqual(calibration["confidence_threshold_active"], 0.6)
        self.assertEqual(calibration["severity_multiplier"], 1.3)

    def test_load_speci_calibration_marks_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            calibration_path = Path(tmp) / "speci_calibration.json"
            calibration_path.write_text("{not valid json", encoding="utf-8")
            calibration = _load_speci_calibration(str(calibration_path))
        self.assertFalse(calibration["loaded"])
        self.assertEqual(calibration["error"], "invalid_speci_calibration_payload")

    def test_scan_handles_snapshot_errors_per_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-T84",
                        "market_title": "Will high temp in Austin be >84°?",
                        "rules_primary": "If highest temperature is greater than 84, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "America/Chicago",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "above:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            with patch(
                "betbot.kalshi_temperature_constraints.build_intraday_temperature_snapshot",
                side_effect=TimeoutError("read timed out"),
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={"status": "forecast_unavailable"},
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_processed"], 1)
            self.assertEqual(summary["markets_emitted"], 1)
            self.assertEqual(summary["snapshot_unavailable_count"], 1)

    def test_scan_prefers_metar_state_before_nws_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-B84.5",
                        "market_title": "Will high temp in Austin be at most 84?",
                        "rules_primary": "If highest temperature is 84 or less, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "America/Chicago",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "at_most:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            state_path = output_dir / "kalshi_temperature_metar_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KAUS": {"observation_time_utc": "2026-04-10T19:00:00+00:00", "temp_c": 30.0}
                        },
                        "max_temp_c_by_station_local_day": {
                            "KAUS|2026-04-10": 30.0,
                        },
                        "min_temp_c_by_station_local_day": {
                            "KAUS|2026-04-10": 24.0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "betbot.kalshi_weather_intraday.fetch_nws_station_recent_observations",
                side_effect=AssertionError("NWS fetch should be skipped when METAR state already has observation"),
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={
                    "status": "ready",
                    "periods": [
                        {"startTime": "2026-04-10T18:00:00+00:00", "temperature": 89},
                    ],
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["yes_impossible_count"], 1)
            self.assertEqual(summary["snapshot_unavailable_count"], 0)
            self.assertGreaterEqual(summary["forecast_modeled_count"], 0)
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["threshold_kind"], "at_most")
            self.assertEqual(rows[0]["yes_possible_overlap"], "0")
            self.assertGreater(float(rows[0]["yes_possible_gap"]), 0.0)

    def test_scan_falls_back_timezone_from_station_when_blank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-B84.5",
                        "market_title": "Will high temp in Austin be at most 84?",
                        "rules_primary": "If highest temperature is 84 or less, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "at_most:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            state_path = output_dir / "kalshi_temperature_metar_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KAUS": {"observation_time_utc": "2026-04-10T19:00:00+00:00", "temp_c": 30.0}
                        },
                        "max_temp_c_by_station_local_day": {
                            "KAUS|2026-04-10": 30.0,
                        },
                        "min_temp_c_by_station_local_day": {
                            "KAUS|2026-04-10": 24.0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "betbot.kalshi_weather_intraday.fetch_nws_station_recent_observations",
                side_effect=AssertionError("NWS fetch should be skipped when METAR state already has observation"),
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={
                    "status": "ready",
                    "periods": [
                        {"startTime": "2026-04-10T18:00:00+00:00", "temperature": 89},
                    ],
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_processed"], 1)
            self.assertEqual(summary["yes_impossible_count"], 1)

            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["settlement_timezone"], "America/Chicago")
            self.assertEqual(rows[0]["temperature_metric"], "daily_high")

    def test_scan_fails_closed_for_stale_active_day_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            now_utc = datetime.now(timezone.utc)
            target_date_local = now_utc.astimezone(ZoneInfo("America/Chicago")).date().isoformat()
            stale_observation_utc = (now_utc - timedelta(hours=6)).isoformat()
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-T84",
                        "market_title": "Will high temp in Austin be >84°?",
                        "rules_primary": "If highest temperature is greater than 84, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "America/Chicago",
                        "target_date_local": target_date_local,
                        "threshold_expression": "above:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            with patch(
                "betbot.kalshi_temperature_constraints.build_intraday_temperature_snapshot",
                return_value={
                    "status": "ready",
                    "max_temperature_settlement_raw": 78.4,
                    "max_temperature_settlement_quantized": 78.0,
                    "min_temperature_settlement_raw": 66.2,
                    "min_temperature_settlement_quantized": 66.0,
                    "observations_for_date": 1,
                    "latest_observation_time_utc": stale_observation_utc,
                    "observations": [{"timestamp": stale_observation_utc}],
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={"status": "forecast_unavailable"},
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["snapshot_unavailable_count"], 1)
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["constraint_status"], "snapshot_unavailable")
            self.assertIn("snapshot_stale_for_active_day_age_minutes", rows[0]["constraint_reason"])

    def test_scan_fails_closed_for_degraded_snapshot_status_with_clear_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHAUS",
                        "event_ticker": "KXHIGHAUS-26APR10",
                        "market_ticker": "KXHIGHAUS-26APR10-T84",
                        "market_title": "Will high temp in Austin be >84°?",
                        "rules_primary": "If highest temperature is greater than 84, resolve Yes.",
                        "settlement_station": "KAUS",
                        "settlement_timezone": "America/Chicago",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "above:84",
                        "settlement_confidence_score": "0.8",
                    }
                )

            with patch(
                "betbot.kalshi_temperature_constraints.build_intraday_temperature_snapshot",
                return_value={
                    "status": "stale_snapshot",
                    "error": "freshness_threshold_exceeded",
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={"status": "forecast_unavailable"},
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["snapshot_unavailable_count"], 1)
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["constraint_status"], "snapshot_unavailable")
            self.assertEqual(
                rows[0]["constraint_reason"],
                "snapshot_status_stale_snapshot:freshness_threshold_exceeded",
            )

    def test_scan_daily_low_market_uses_min_temperature_metric(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            now_utc = datetime.now(timezone.utc)
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXLOWTNYC",
                        "event_ticker": "KXLOWTNYC-26APR10",
                        "market_ticker": "KXLOWTNYC-26APR10-B40.5",
                        "market_title": "Will low temp in NYC be at most 40?",
                        "rules_primary": "If the lowest temperature is 40 or less, resolve Yes.",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "at_most:40",
                        "settlement_confidence_score": "0.9",
                    }
                )

            state_path = output_dir / "kalshi_temperature_metar_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {
                                "observation_time_utc": now_utc.isoformat(),
                                "temp_c": 5.0,
                                "report_type": "SPECI",
                            }
                        },
                        "max_temp_c_by_station_local_day": {
                            "KNYC|2026-04-10": 22.0,
                        },
                        "min_temp_c_by_station_local_day": {
                            "KNYC|2026-04-10": 4.0,
                        },
                        "station_interval_stats_by_station": {
                            "KNYC": {
                                "latest_interval_minutes": "4.5",
                                "interval_median_minutes": "7.0",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "betbot.kalshi_weather_intraday.fetch_nws_station_recent_observations",
                side_effect=AssertionError("NWS observation fetch should be skipped when METAR state has the day"),
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={
                    "status": "ready",
                    "periods": [
                        {"startTime": "2026-04-10T09:00:00+00:00", "temperature": 46},
                    ],
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["yes_likely_locked_count"], 1)
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["temperature_metric"], "daily_low")
            self.assertEqual(rows[0]["constraint_status"], "yes_likely_locked")
            self.assertEqual(rows[0]["speci_recent"], "1")
            self.assertEqual(rows[0]["speci_shock_active"], "1")
            self.assertGreaterEqual(float(rows[0]["speci_shock_confidence"]), 0.45)
            self.assertGreater(float(rows[0]["speci_shock_weight"]), 0.0)
            self.assertEqual(rows[0]["speci_shock_mode"], "operational")
            self.assertIn("explicit_speci", rows[0]["speci_shock_trigger_families"])
            self.assertEqual(rows[0]["threshold_kind"], "at_most")
            self.assertIn("primary_signal_margin", rows[0])

    def test_scan_daily_low_above_boundary_does_not_mark_yes_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            now_utc = datetime.now(timezone.utc)
            output_dir = Path(tmp)
            specs_csv = output_dir / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "rules_primary",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "threshold_expression",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXLOWTLAX",
                        "event_ticker": "KXLOWTLAX-26APR10",
                        "market_ticker": "KXLOWTLAX-26APR10-T57",
                        "market_title": "Will low temp in Los Angeles be above 57?",
                        "rules_primary": "If the lowest temperature is greater than 57, resolve Yes.",
                        "settlement_station": "KLAX",
                        "settlement_timezone": "America/Los_Angeles",
                        "target_date_local": "2026-04-10",
                        "threshold_expression": "above:57",
                        "settlement_confidence_score": "0.9",
                    }
                )

            state_path = output_dir / "kalshi_temperature_metar_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KLAX": {
                                "observation_time_utc": now_utc.isoformat(),
                                "temp_c": 14.0,
                                "report_type": "METAR",
                            }
                        },
                        "max_temp_c_by_station_local_day": {
                            "KLAX|2026-04-10": 23.0,
                        },
                        "min_temp_c_by_station_local_day": {
                            "KLAX|2026-04-10": 14.0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch(
                "betbot.kalshi_weather_intraday.fetch_nws_station_recent_observations",
                side_effect=AssertionError("NWS observation fetch should be skipped when METAR state has the day"),
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_nws_station_hourly_forecast",
                return_value={
                    "status": "ready",
                    "periods": [
                        {"startTime": "2026-04-10T09:00:00+00:00", "temperature": 72},
                    ],
                },
            ), patch(
                "betbot.kalshi_temperature_constraints.fetch_aviationweather_taf_temperature_envelopes",
                return_value={"status": "ready", "station_envelopes": {}},
            ):
                summary = run_kalshi_temperature_constraint_scan(
                    specs_csv=str(specs_csv),
                    output_dir=str(output_dir),
                    max_markets=1,
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["yes_impossible_count"], 1)
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["temperature_metric"], "daily_low")
            self.assertEqual(rows[0]["constraint_status"], "yes_impossible")
            # For strict "above", observed == threshold is not in YES interval.
            self.assertEqual(rows[0]["yes_possible_overlap"], "0")
            self.assertGreater(float(rows[0]["yes_possible_gap"]), 0.0)

    def test_exact_strike_chain_summary_flags_missing_impossible_propagation(self) -> None:
        rows = [
            {
                "series_ticker": "KXHIGHAUS",
                "settlement_station": "KAUS",
                "target_date_local": "2026-04-10",
                "temperature_metric": "daily_high",
                "market_ticker": "KXHIGHAUS-26APR10-B80.5",
                "threshold_expression": "at_most:80",
                "constraint_status": "yes_impossible",
            },
            {
                "series_ticker": "KXHIGHAUS",
                "settlement_station": "KAUS",
                "target_date_local": "2026-04-10",
                "temperature_metric": "daily_high",
                "market_ticker": "KXHIGHAUS-26APR10-E79",
                "threshold_expression": "equal:79",
                "constraint_status": "no_signal",
            },
        ]
        summary = _exact_strike_chain_summary(rows)
        self.assertEqual(summary["checked_groups"], 1)
        self.assertEqual(summary["violations_count"], 1)
        self.assertIn("upper_chain_anchor", summary["violations"][0]["reason"])

    def test_range_family_consistency_flags_empty_locked_intersection(self) -> None:
        rows = [
            {
                "series_ticker": "KXHIGHAUS",
                "settlement_station": "KAUS",
                "target_date_local": "2026-04-10",
                "temperature_metric": "daily_high",
                "market_ticker": "KXHIGHAUS-26APR10-T80",
                "threshold_expression": "above:80",
                "constraint_status": "yes_likely_locked",
                "observed_metric_settlement_quantized": "81",
            },
            {
                "series_ticker": "KXHIGHAUS",
                "settlement_station": "KAUS",
                "target_date_local": "2026-04-10",
                "temperature_metric": "daily_high",
                "market_ticker": "KXHIGHAUS-26APR10-B70.5",
                "threshold_expression": "at_most:70",
                "constraint_status": "yes_likely_locked",
                "observed_metric_settlement_quantized": "81",
            },
        ]
        summary = _range_family_consistency_summary(rows)
        self.assertEqual(summary["checked_groups"], 1)
        self.assertEqual(summary["locked_interval_conflicts_count"], 1)
        self.assertEqual(summary["violations_count"], 1)


if __name__ == "__main__":
    unittest.main()
