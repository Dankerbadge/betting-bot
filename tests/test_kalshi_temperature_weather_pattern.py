from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
import tempfile
import unittest
from pathlib import Path

from betbot.kalshi_temperature_weather_pattern import (
    run_kalshi_temperature_weather_pattern,
    summarize_kalshi_temperature_weather_pattern,
)


class KalshiTemperatureWeatherPatternTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict[str, object]]) -> Path:
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def _write_json(self, path: Path, payload: dict[str, object]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _stamp(self, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def test_weather_pattern_aggregates_station_hour_and_weather_tiers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            later = now + timedelta(minutes=30)
            later_stamp = self._stamp(later)
            intents_path = self._write_csv(
                base / f"kalshi_temperature_trade_intents_{stamp}.csv",
                [
                    {
                        "captured_at_utc": now.isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-a",
                        "settlement_station": "KJFK",
                        "policy_metar_local_hour": "12",
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.91",
                        "policy_expected_edge_net": "0.08",
                        "policy_edge_to_risk_ratio": "0.12",
                        "metar_observation_age_minutes": "20",
                        "forecast_model_status": "ready",
                    },
                    {
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-b",
                        "settlement_station": "KPHL",
                        "policy_metar_local_hour": "13",
                        "constraint_status": "blocked",
                        "signal_type": "shock",
                        "side": "yes",
                        "policy_approved": "false",
                        "policy_probability_confidence": "0.77",
                        "policy_expected_edge_net": "0.03",
                        "policy_edge_to_risk_ratio": "0.08",
                        "metar_observation_age_minutes": "95",
                        "forecast_model_status": "stale",
                    },
                ],
            )
            settled_csv = self._write_csv(
                base / f"kalshi_temperature_profitability_settled_{later_stamp}.csv",
                [
                    {
                        "order_key": "client:temp-a",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": "KXHIGHLAX-26APR22-B61.5",
                        "client_order_id": "temp-a",
                        "realized_pnl_dollars": "0.06",
                        "expected_edge_dollars": "0.08",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.02",
                        "outcome": "win",
                    }
                ],
            )
            self._write_json(
                base / f"kalshi_temperature_profitability_summary_{later_stamp}.json",
                {
                    "status": "ready",
                    "captured_at": later.isoformat().replace("+00:00", "Z"),
                    "output_csv": str(settled_csv),
                    "expected_vs_realized": {
                        "matched_settled_orders": 1,
                        "matched_expected_edge_total_dollars": 0.08,
                        "matched_realized_pnl_total_dollars": 0.06,
                    },
                },
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=1,
                max_profile_age_hours=48.0,
            )
            summary_text = summarize_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=1,
                max_profile_age_hours=48.0,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(json.loads(summary_text)["status"], "ready")
            self.assertEqual(summary["overall"]["attempts_total"], 2)
            self.assertEqual(summary["overall"]["approved_total"], 1)
            self.assertEqual(summary["overall"]["realized_trade_count"], 1)
            self.assertAlmostEqual(summary["overall"]["realized_pnl_sum"], 0.06, places=6)

            station_bucket = summary["profile"]["bucket_dimensions"]["settlement_station"]["KJFK"]
            self.assertEqual(station_bucket["attempts"], 1)
            self.assertEqual(station_bucket["approved"], 1)
            self.assertAlmostEqual(station_bucket["expected_edge_sum"], 0.08, places=6)
            self.assertAlmostEqual(station_bucket["realized_pnl_sum"], 0.06, places=6)
            self.assertAlmostEqual(station_bucket["realized_per_trade"], 0.06, places=6)
            self.assertAlmostEqual(station_bucket["edge_realization_ratio"], 0.75, places=6)

            weather_bucket = summary["profile"]["bucket_dimensions"]["weather_evidence_tier"]["0-30m|ready"]
            self.assertEqual(weather_bucket["attempts"], 1)
            self.assertEqual(weather_bucket["forecast_status"], "ready")
            weather_profile = summary["weather_pattern_profile"]
            self.assertEqual(weather_profile["source_age_hours"], 0.0)
            bucket_profiles = weather_profile["bucket_profiles"]
            required_dimensions = (
                "station",
                "local_hour",
                "signal_type",
                "side",
                "weather_evidence_tier",
                "metar_age_bucket",
            )
            for dimension in required_dimensions:
                self.assertIn(dimension, bucket_profiles)
                self.assertTrue(bucket_profiles[dimension])
                sample_entry = next(iter(bucket_profiles[dimension].values()))
                self.assertIn("samples", sample_entry)
                self.assertIn("expectancy_per_trade", sample_entry)
                self.assertIn("probability_raise", sample_entry)
                self.assertIn("expected_edge_raise", sample_entry)
            station_profile = bucket_profiles["station"]["KJFK"]
            self.assertEqual(station_profile["samples"], 1)
            self.assertAlmostEqual(station_profile["expectancy_per_trade"], 0.06, places=6)
            self.assertEqual(station_profile["realized_trade_count"], 1)
            self.assertAlmostEqual(station_profile["realized_coverage"], 1.0, places=6)
            self.assertAlmostEqual(station_profile["realized_per_trade"], 0.06, places=6)
            self.assertAlmostEqual(station_profile["edge_realization_ratio"], 0.75, places=6)
            self.assertEqual(station_profile["sample_ok"], True)

            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["latest_file"]).exists())

    def test_weather_pattern_handles_missing_profitability_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            self._write_csv(
                base / f"kalshi_temperature_trade_intents_{stamp}.csv",
                [
                    {
                        "captured_at_utc": now.isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-a",
                        "settlement_station": "KJFK",
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.91",
                        "policy_expected_edge_net": "0.08",
                        "policy_edge_to_risk_ratio": "0.12",
                        "metar_observation_age_minutes": "20",
                        "forecast_model_status": "ready",
                    }
                ],
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=1,
                max_profile_age_hours=24.0,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["sources"]["profitability"]["summary_files_count"], 0)
            self.assertEqual(summary["sources"]["profitability"]["realized_files_count"], 0)
            self.assertEqual(summary["overall"]["realized_trade_count"], 0)
            self.assertIsNone(summary["overall"]["realized_pnl_sum"])
            station_bucket = summary["profile"]["bucket_dimensions"]["settlement_station"]["KJFK"]
            self.assertIsNone(station_bucket["realized_pnl_sum"])
            self.assertIsNone(station_bucket["realized_per_trade"])
            self.assertIsNone(station_bucket["edge_realization_ratio"])
            risk_off = summary["profile"]["risk_off_recommendation"]
            self.assertEqual(risk_off["status"], "monitor_only")
            self.assertFalse(risk_off["active"])
            self.assertEqual(risk_off["reason"], "insufficient_attempts")

    def test_weather_pattern_detects_negative_expectancy_block_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            later = now + timedelta(minutes=30)
            later_stamp = self._stamp(later)
            self._write_csv(
                base / f"kalshi_temperature_trade_intents_{stamp}.csv",
                [
                    {
                        "captured_at_utc": now.isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-a",
                        "settlement_station": "KJFK",
                        "constraint_status": "yes_possible",
                        "signal_type": "regime_weak",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.79",
                        "policy_expected_edge_net": "0.03",
                        "policy_edge_to_risk_ratio": "0.05",
                        "metar_observation_age_minutes": "150",
                        "forecast_model_status": "ready",
                    },
                    {
                        "captured_at_utc": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-b",
                        "settlement_station": "KJFK",
                        "constraint_status": "yes_possible",
                        "signal_type": "regime_weak",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.80",
                        "policy_expected_edge_net": "0.03",
                        "policy_edge_to_risk_ratio": "0.05",
                        "metar_observation_age_minutes": "150",
                        "forecast_model_status": "ready",
                    },
                    {
                        "captured_at_utc": (now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-c",
                        "settlement_station": "KJFK",
                        "constraint_status": "yes_possible",
                        "signal_type": "regime_weak",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.81",
                        "policy_expected_edge_net": "0.03",
                        "policy_edge_to_risk_ratio": "0.05",
                        "metar_observation_age_minutes": "150",
                        "forecast_model_status": "ready",
                    },
                ],
            )
            settled_csv = self._write_csv(
                base / f"kalshi_temperature_profitability_settled_{later_stamp}.csv",
                [
                    {
                        "order_key": "client:temp-a",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": "KXHIGHLAX-26APR22-B61.5",
                        "client_order_id": "temp-a",
                        "realized_pnl_dollars": "-0.12",
                        "expected_edge_dollars": "0.03",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.15",
                        "outcome": "loss",
                    },
                    {
                        "order_key": "client:temp-b",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": "KXHIGHLAX-26APR22-B61.6",
                        "client_order_id": "temp-b",
                        "realized_pnl_dollars": "-0.11",
                        "expected_edge_dollars": "0.03",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.14",
                        "outcome": "loss",
                    },
                    {
                        "order_key": "client:temp-c",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": "KXHIGHLAX-26APR22-B61.7",
                        "client_order_id": "temp-c",
                        "realized_pnl_dollars": "-0.13",
                        "expected_edge_dollars": "0.03",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.16",
                        "outcome": "loss",
                    },
                ],
            )
            self._write_json(
                base / f"kalshi_temperature_profitability_summary_{later_stamp}.json",
                {
                    "status": "ready",
                    "captured_at": later.isoformat().replace("+00:00", "Z"),
                    "output_csv": str(settled_csv),
                },
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=2,
                max_profile_age_hours=48.0,
            )

            negative = summary["profile"]["negative_expectancy_buckets"]
            hard_blocks = summary["profile"]["recommendations"]["hard_block_candidates"]
            threshold_raises = summary["profile"]["recommendations"]["threshold_raise_candidates"]

            self.assertGreaterEqual(summary["profile"]["regime_risk"]["negative_expectancy_bucket_count"], 1)
            self.assertGreaterEqual(summary["profile"]["regime_risk"]["hard_block_candidate_count"], 1)
            self.assertAlmostEqual(summary["profile"]["regime_risk"]["negative_expectancy_attempt_share"], 1.0, places=6)
            self.assertAlmostEqual(summary["profile"]["regime_risk"]["stale_metar_attempt_share"], 1.0, places=6)
            self.assertAlmostEqual(
                summary["profile"]["regime_risk"]["stale_metar_negative_attempt_share"],
                1.0,
                places=6,
            )
            self.assertEqual(summary["profile"]["regime_risk"]["stale_negative_station_attempts"], 3)
            self.assertAlmostEqual(
                summary["profile"]["regime_risk"]["stale_negative_station_max_share"],
                1.0,
                places=6,
            )
            self.assertAlmostEqual(
                summary["profile"]["regime_risk"]["stale_negative_station_hhi"],
                1.0,
                places=6,
            )
            stale_station_top = summary["profile"]["regime_risk"]["stale_negative_station_top"]
            self.assertTrue(stale_station_top)
            self.assertEqual(stale_station_top[0]["station"], "KJFK")
            self.assertEqual(stale_station_top[0]["attempts"], 3)
            self.assertAlmostEqual(float(stale_station_top[0]["share"]), 1.0, places=6)
            self.assertAlmostEqual(summary["overall"]["stale_negative_station_max_share"], 1.0, places=6)
            self.assertAlmostEqual(summary["overall"]["stale_negative_station_hhi"], 1.0, places=6)
            self.assertTrue(any(bucket["dimension"] == "settlement_station" and bucket["bucket"] == "KJFK" for bucket in negative))
            self.assertTrue(any(candidate["dimension"] == "settlement_station" and candidate["bucket"] == "KJFK" for candidate in hard_blocks))
            hard_block = next(
                candidate
                for candidate in hard_blocks
                if candidate["dimension"] == "settlement_station" and candidate["bucket"] == "KJFK"
            )
            self.assertGreaterEqual(hard_block["realized_trade_count"], 3)
            self.assertGreaterEqual(float(hard_block["realized_coverage"]), 0.5)
            self.assertGreaterEqual(float(hard_block["realized_coverage_confidence"]), 0.3)
            self.assertTrue(any(candidate["threshold"] == "min_expected_edge_net" for candidate in threshold_raises))
            self.assertLess(summary["overall"]["edge_realization_ratio"], 0.0)

    def test_weather_pattern_expected_edge_only_pressure_still_yields_recommendations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            rows: list[dict[str, object]] = []
            for idx in range(24):
                rows.append(
                    {
                        "captured_at_utc": (now + timedelta(minutes=idx)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": f"temp-{idx}",
                        "settlement_station": "KBOS",
                        "constraint_status": "yes_impossible",
                        "signal_type": "metar_observation_stale",
                        "side": "no",
                        "policy_approved": "true" if idx % 2 == 0 else "false",
                        "policy_probability_confidence": "0.89",
                        "policy_expected_edge_net": "-0.12",
                        "policy_edge_to_risk_ratio": "-0.061",
                        "metar_observation_age_minutes": "1450",
                        "forecast_model_status": "ready",
                    }
                )
            self._write_csv(base / f"kalshi_temperature_trade_intents_{stamp}.csv", rows)

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=10,
                max_profile_age_hours=24.0,
            )

            threshold_raises = summary["profile"]["recommendations"]["threshold_raise_candidates"]
            hard_blocks = summary["profile"]["recommendations"]["hard_block_candidates"]

            self.assertTrue(any(candidate["bucket"] == "KBOS" for candidate in threshold_raises))
            self.assertFalse(any(candidate["bucket"] == "KBOS" for candidate in hard_blocks))
            risk_off = summary["profile"]["risk_off_recommendation"]
            self.assertEqual(risk_off["status"], "risk_off_soft")
            self.assertTrue(risk_off["active"])
            self.assertFalse(risk_off["hard_block"])
            self.assertAlmostEqual(risk_off["probability_raise"], 0.015, places=6)
            self.assertAlmostEqual(risk_off["expected_edge_raise"], 0.003, places=6)

    def test_weather_pattern_weak_realized_coverage_avoids_hard_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            later = now + timedelta(minutes=20)
            later_stamp = self._stamp(later)
            rows: list[dict[str, object]] = []
            for idx in range(10):
                rows.append(
                    {
                        "captured_at_utc": (now + timedelta(minutes=idx)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": f"temp-weak-{idx}",
                        "settlement_station": "KDEN",
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.84",
                        "policy_expected_edge_net": "0.02",
                        "policy_edge_to_risk_ratio": "0.08",
                        "metar_observation_age_minutes": "75",
                        "forecast_model_status": "ready",
                    }
                )
            self._write_csv(base / f"kalshi_temperature_trade_intents_{stamp}.csv", rows)
            settled_csv = self._write_csv(
                base / f"kalshi_temperature_profitability_settled_{later_stamp}.csv",
                [
                    {
                        "order_key": "client:temp-weak-0",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": "KXHIGHSEA-26APR22-B61.5",
                        "client_order_id": "temp-weak-0",
                        "realized_pnl_dollars": "-0.13",
                        "expected_edge_dollars": "0.02",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.15",
                        "outcome": "loss",
                    }
                ],
            )
            self._write_json(
                base / f"kalshi_temperature_profitability_summary_{later_stamp}.json",
                {
                    "status": "ready",
                    "captured_at": later.isoformat().replace("+00:00", "Z"),
                    "output_csv": str(settled_csv),
                },
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=3,
                max_profile_age_hours=48.0,
            )

            hard_blocks = summary["profile"]["recommendations"]["hard_block_candidates"]
            threshold_raises = summary["profile"]["recommendations"]["threshold_raise_candidates"]
            self.assertFalse(any(candidate["bucket"] == "KDEN" for candidate in hard_blocks))
            self.assertTrue(any(candidate["bucket"] == "KDEN" for candidate in threshold_raises))

    def test_weather_pattern_station_stale_negative_concentration_triggers_soft_risk_off(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            rows: list[dict[str, object]] = []
            diversified_stations = ("KATL", "KBOS", "KPHL", "KJFK")
            for idx in range(60):
                is_concentrated_stale_negative = idx < 8
                rows.append(
                    {
                        "captured_at_utc": (now + timedelta(minutes=idx)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": f"temp-station-soft-{idx}",
                        "settlement_station": "KORD" if is_concentrated_stale_negative else diversified_stations[idx % 4],
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "yes",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.84",
                        "policy_expected_edge_net": "-0.06" if is_concentrated_stale_negative else "0.05",
                        "policy_edge_to_risk_ratio": "0.10",
                        "metar_observation_age_minutes": "150" if is_concentrated_stale_negative else "20",
                        "forecast_model_status": "ready",
                    }
                )
            self._write_csv(base / f"kalshi_temperature_trade_intents_{stamp}.csv", rows)

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=6,
                max_profile_age_hours=24.0,
            )

            regime_risk = summary["profile"]["regime_risk"]
            risk_off = summary["profile"]["risk_off_recommendation"]

            self.assertAlmostEqual(regime_risk["stale_metar_negative_attempt_share"], 8.0 / 60.0, places=6)
            self.assertAlmostEqual(regime_risk["negative_expectancy_attempt_share"], 8.0 / 60.0, places=6)
            self.assertEqual(regime_risk["stale_negative_station_attempts"], 8)
            self.assertAlmostEqual(regime_risk["stale_negative_station_max_share"], 1.0, places=6)
            self.assertAlmostEqual(regime_risk["stale_negative_station_hhi"], 1.0, places=6)
            self.assertEqual(regime_risk["stale_negative_station_top"][0]["station"], "KORD")
            self.assertEqual(regime_risk["stale_negative_station_top"][0]["attempts"], 8)

            self.assertLess(float(risk_off["stale_metar_negative_attempt_share_effective"]), 0.18)
            self.assertLess(float(risk_off["negative_expectancy_attempt_share_effective"]), 0.35)
            self.assertEqual(risk_off["status"], "risk_off_soft")
            self.assertTrue(risk_off["active"])
            self.assertFalse(risk_off["hard_block"])
            self.assertEqual(risk_off["reason"], "stale_negative_station_concentration_emerging")
            self.assertAlmostEqual(float(risk_off["stale_negative_station_max_share_effective"]), 1.0, places=6)
            self.assertAlmostEqual(float(risk_off["stale_negative_station_hhi_effective"]), 1.0, places=6)

    def test_weather_pattern_station_stale_negative_concentration_can_escalate_hard_risk_off(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            later = now + timedelta(minutes=90)
            later_stamp = self._stamp(later)
            rows: list[dict[str, object]] = []
            diversified_stations = ("KATL", "KBOS", "KPHL", "KJFK")
            for idx in range(48):
                is_concentrated_stale_negative = idx < 12
                rows.append(
                    {
                        "captured_at_utc": (now + timedelta(minutes=idx)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": f"temp-station-hard-{idx}",
                        "settlement_station": "KSEA" if is_concentrated_stale_negative else diversified_stations[idx % 4],
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "no",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.86",
                        "policy_expected_edge_net": "-0.04" if is_concentrated_stale_negative else "0.06",
                        "policy_edge_to_risk_ratio": "0.11",
                        "metar_observation_age_minutes": "170" if is_concentrated_stale_negative else "20",
                        "forecast_model_status": "ready",
                    }
                )
            self._write_csv(base / f"kalshi_temperature_trade_intents_{stamp}.csv", rows)

            settled_rows: list[dict[str, object]] = []
            for idx in range(10):
                settled_rows.append(
                    {
                        "order_key": f"client:temp-station-hard-{idx}",
                        "captured_at_utc": later.isoformat().replace("+00:00", "Z"),
                        "market_ticker": f"KXHIGHSEA-26APR22-B6{idx}",
                        "client_order_id": f"temp-station-hard-{idx}",
                        "realized_pnl_dollars": "-0.08",
                        "expected_edge_dollars": "0.02",
                        "expected_cost_dollars": "0.95",
                        "realized_minus_expected_dollars": "-0.10",
                        "outcome": "loss",
                    }
                )
            settled_csv = self._write_csv(
                base / f"kalshi_temperature_profitability_settled_{later_stamp}.csv",
                settled_rows,
            )
            self._write_json(
                base / f"kalshi_temperature_profitability_summary_{later_stamp}.json",
                {
                    "status": "ready",
                    "captured_at": later.isoformat().replace("+00:00", "Z"),
                    "output_csv": str(settled_csv),
                },
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=5,
                max_profile_age_hours=48.0,
            )

            regime_risk = summary["profile"]["regime_risk"]
            risk_off = summary["profile"]["risk_off_recommendation"]
            self.assertGreaterEqual(int(regime_risk["hard_block_candidate_count"]), 1)
            self.assertAlmostEqual(regime_risk["stale_metar_negative_attempt_share"], 12.0 / 48.0, places=6)
            self.assertLess(float(risk_off["stale_metar_negative_attempt_share_effective"]), 0.30)
            self.assertEqual(regime_risk["stale_negative_station_attempts"], 12)
            self.assertAlmostEqual(regime_risk["stale_negative_station_max_share"], 1.0, places=6)
            self.assertAlmostEqual(regime_risk["stale_negative_station_hhi"], 1.0, places=6)

            self.assertEqual(risk_off["status"], "risk_off_hard")
            self.assertTrue(risk_off["active"])
            self.assertTrue(risk_off["hard_block"])
            self.assertEqual(risk_off["reason"], "stale_negative_station_concentration")
            self.assertAlmostEqual(float(risk_off["probability_raise"]), 0.03, places=6)
            self.assertAlmostEqual(float(risk_off["expected_edge_raise"]), 0.006, places=6)

    def test_weather_pattern_confidence_adjusted_concentration_suppresses_noise_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            stamp = self._stamp(now)
            rows: list[dict[str, object]] = []
            for idx in range(12):
                rows.append(
                    {
                        "captured_at_utc": (now + timedelta(minutes=idx)).isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": f"temp-noise-{idx}",
                        "settlement_station": "KATL",
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "yes",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.82",
                        "policy_expected_edge_net": "-0.08" if idx < 6 else "0.09",
                        "policy_edge_to_risk_ratio": "0.11",
                        "metar_observation_age_minutes": "20",
                        "forecast_model_status": "ready" if idx < 6 else "stale",
                    }
                )
            self._write_csv(base / f"kalshi_temperature_trade_intents_{stamp}.csv", rows)

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=4,
                max_profile_age_hours=24.0,
            )

            regime_risk = summary["profile"]["regime_risk"]
            risk_off = summary["profile"]["risk_off_recommendation"]
            self.assertAlmostEqual(regime_risk["negative_expectancy_attempt_share"], 0.5, places=6)
            self.assertLess(regime_risk["negative_expectancy_attempt_share_confidence_adjusted"], 0.35)
            self.assertEqual(
                regime_risk["concentration_confidence_adjustment_method"],
                "wilson_lower_bound",
            )
            self.assertEqual(risk_off["status"], "normal")
            self.assertFalse(risk_off["active"])
            self.assertTrue(risk_off["confidence_adjustment_applied"])
            self.assertAlmostEqual(risk_off["negative_expectancy_attempt_share_observed"], 0.5, places=6)
            self.assertLess(risk_off["negative_expectancy_attempt_share_effective"], 0.35)

    def test_weather_pattern_filters_stale_settled_rows_without_row_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            recent_stamp = self._stamp(now)
            stale_dt = now - timedelta(hours=72)
            stale_stamp = self._stamp(stale_dt)

            self._write_csv(
                base / f"kalshi_temperature_trade_intents_{recent_stamp}.csv",
                [
                    {
                        "captured_at_utc": now.isoformat().replace("+00:00", "Z"),
                        "temperature_client_order_id": "temp-stale",
                        "settlement_station": "KJFK",
                        "constraint_status": "yes_possible",
                        "signal_type": "range",
                        "side": "yes",
                        "policy_approved": "true",
                        "policy_probability_confidence": "0.9",
                        "policy_expected_edge_net": "0.04",
                    }
                ],
            )
            stale_settled_path = self._write_csv(
                base / f"kalshi_temperature_profitability_settled_{stale_stamp}.csv",
                [
                    {
                        "order_key": "client:temp-stale",
                        "client_order_id": "temp-stale",
                        "market_ticker": "KXHIGHLAX-26APR22-B61.5",
                        "realized_pnl_dollars": "-0.25",
                        "expected_edge_dollars": "0.04",
                    }
                ],
            )
            self._write_json(
                base / f"kalshi_temperature_profitability_summary_{recent_stamp}.json",
                {
                    "status": "ready",
                    "captured_at": now.isoformat().replace("+00:00", "Z"),
                    "output_csv": str(stale_settled_path),
                },
            )

            summary = run_kalshi_temperature_weather_pattern(
                output_dir=str(base),
                window_hours=24.0,
                min_bucket_samples=1,
                max_profile_age_hours=96.0,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["overall"]["attempts_total"], 1)
            self.assertEqual(summary["overall"]["realized_trade_count"], 0)
            self.assertIsNone(summary["overall"]["realized_pnl_sum"])
            self.assertGreaterEqual(
                int(summary["sources"]["profitability"]["raw_row_status_counts"].get("outside_window") or 0),
                1,
            )
            station_bucket = summary["profile"]["bucket_dimensions"]["settlement_station"]["KJFK"]
            self.assertEqual(station_bucket["realized_trade_count"], 0)
            self.assertIsNone(station_bucket["realized_per_trade"])


if __name__ == "__main__":
    unittest.main()
