from __future__ import annotations

import csv
import json
import math
import tempfile
import unittest
from pathlib import Path

from betbot.kalshi_temperature_growth_optimizer import run_kalshi_temperature_growth_optimizer


class KalshiTemperatureGrowthOptimizerTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict[str, object]]) -> Path:
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    def _write_weather_artifact(self, path: Path, payload: dict[str, object]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _write_execution_artifact(self, path: Path, payload: dict[str, object]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _find_candidate(
        self,
        summary: dict[str, object],
        *,
        min_probability_confidence: float,
        min_expected_edge_net: float,
        min_edge_to_risk_ratio: float,
    ) -> dict[str, object]:
        for candidate in summary.get("top_candidates", []):
            if not isinstance(candidate, dict):
                continue
            if (
                float(candidate.get("min_probability_confidence") or 0.0) == float(min_probability_confidence)
                and float(candidate.get("min_expected_edge_net") or 0.0) == float(min_expected_edge_net)
                and float(candidate.get("min_edge_to_risk_ratio") or 0.0) == float(min_edge_to_risk_ratio)
            ):
                return candidate
        raise AssertionError("candidate not found")

    def test_optimizer_prefers_quality_plus_throughput_over_pure_throughput(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "hq-1",
                    "policy_approved": "true",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.90",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.11",
                },
                {
                    "intent_id": "hq-2",
                    "policy_approved": "true",
                    "underlying_key": "U2",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.91",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.10",
                },
                {
                    "intent_id": "hq-3",
                    "policy_approved": "true",
                    "underlying_key": "U3",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.11",
                },
                {
                    "intent_id": "lq-1",
                    "policy_approved": "false",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.60",
                    "policy_expected_edge_net": "0.01",
                    "policy_edge_to_risk_ratio": "0.01",
                },
                {
                    "intent_id": "lq-2",
                    "policy_approved": "false",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.62",
                    "policy_expected_edge_net": "0.01",
                    "policy_edge_to_risk_ratio": "0.01",
                },
                {
                    "intent_id": "lq-3",
                    "policy_approved": "false",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.63",
                    "policy_expected_edge_net": "0.01",
                    "policy_edge_to_risk_ratio": "0.01",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["inputs"]["rows_valid"], 6)
            recommended = summary["recommended_configuration"]
            self.assertIsInstance(recommended, dict)
            self.assertGreaterEqual(float(recommended["min_probability_confidence"]), 0.9)
            self.assertGreaterEqual(float(recommended["min_edge_to_risk_ratio"]), 0.10)
            self.assertEqual(int(recommended["intents_selected"]), 3)
            self.assertAlmostEqual(float(recommended["selected_expected_edge_sum"]), 0.36, places=6)
            self.assertLess(float(recommended["selected_rate"]), 1.0)
            self.assertGreater(float(recommended["score"]), 0.0)

            scores = [float(candidate["score"]) for candidate in summary["top_candidates"]]
            self.assertEqual(scores, sorted(scores, reverse=True))

    def test_optimizer_applies_robustness_penalties_to_fragile_configs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "fragile-1",
                    "policy_approved": "true",
                    "underlying_key": "UF",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.28",
                    "prelive_fill_ratio": "0.33",
                    "prelive_settlement_ratio": "0.35",
                },
                {
                    "intent_id": "fragile-2",
                    "policy_approved": "true",
                    "underlying_key": "UF",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.27",
                    "prelive_fill_ratio": "0.31",
                    "prelive_settlement_ratio": "0.34",
                },
                {
                    "intent_id": "fragile-3",
                    "policy_approved": "true",
                    "underlying_key": "UF",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.29",
                    "prelive_fill_ratio": "0.32",
                    "prelive_settlement_ratio": "0.33",
                },
                {
                    "intent_id": "robust-1",
                    "policy_approved": "true",
                    "underlying_key": "UR1",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.11",
                    "policy_edge_to_risk_ratio": "0.11",
                    "prelive_submission_ratio": "0.96",
                    "prelive_fill_ratio": "0.97",
                    "prelive_settlement_ratio": "0.95",
                },
                {
                    "intent_id": "robust-2",
                    "policy_approved": "true",
                    "underlying_key": "UR2",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.11",
                    "policy_edge_to_risk_ratio": "0.11",
                    "prelive_submission_ratio": "0.95",
                    "prelive_fill_ratio": "0.96",
                    "prelive_settlement_ratio": "0.94",
                },
                {
                    "intent_id": "robust-3",
                    "policy_approved": "true",
                    "underlying_key": "UR3",
                    "settlement_station": "KALB",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.11",
                    "policy_edge_to_risk_ratio": "0.11",
                    "prelive_submission_ratio": "0.97",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.96",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=30)

            fragile = self._find_candidate(
                summary,
                min_probability_confidence=0.95,
                min_expected_edge_net=0.05,
                min_edge_to_risk_ratio=0.12,
            )
            diversified = self._find_candidate(
                summary,
                min_probability_confidence=0.92,
                min_expected_edge_net=0.11,
                min_edge_to_risk_ratio=0.11,
            )

            self.assertTrue(bool(summary["search"]["robustness"]["calibration_available"]))
            self.assertTrue(bool(fragile["robustness"]["enabled"]))
            self.assertGreater(float(diversified["score"]), float(fragile["score"]))
            self.assertLess(float(fragile["robustness"]["score_multiplier"]), float(diversified["robustness"]["score_multiplier"]))
            self.assertGreater(float(diversified["robustness"]["score_bonus"]), 0.0)
            self.assertGreater(float(fragile["robustness"]["conversion"]["penalty"]), 0.0)
            self.assertGreater(float(fragile["robustness"]["concentration"]["penalty"]), float(diversified["robustness"]["concentration"]["penalty"]))
            self.assertGreater(float(fragile["robustness"]["edge_guardrail"]["penalty"]), 0.0)
            self.assertGreater(float(fragile["siphon_markers"]["high_entry_low_edge_share"]), 0.9)
            self.assertGreater(float(fragile["score_components"]["siphon_penalty"]), 0.4)

    def test_optimizer_blocks_fragile_profiles_and_recommends_robust_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "fragile-regime-1",
                    "underlying_key": "UF1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.96",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.40",
                    "prelive_fill_ratio": "0.45",
                    "prelive_settlement_ratio": "0.43",
                },
                {
                    "intent_id": "fragile-regime-2",
                    "underlying_key": "UF2",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.96",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.42",
                    "prelive_fill_ratio": "0.41",
                    "prelive_settlement_ratio": "0.40",
                },
                {
                    "intent_id": "fragile-regime-3",
                    "underlying_key": "UF3",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.96",
                    "policy_expected_edge_net": "0.05",
                    "policy_edge_to_risk_ratio": "0.12",
                    "prelive_submission_ratio": "0.39",
                    "prelive_fill_ratio": "0.44",
                    "prelive_settlement_ratio": "0.42",
                },
                {
                    "intent_id": "robust-regime-1",
                    "underlying_key": "UR1",
                    "settlement_station": "KALB",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.13",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.95",
                    "prelive_fill_ratio": "0.96",
                    "prelive_settlement_ratio": "0.94",
                },
                {
                    "intent_id": "robust-regime-2",
                    "underlying_key": "UR2",
                    "settlement_station": "KBWI",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.13",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.96",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.97",
                },
                {
                    "intent_id": "robust-regime-3",
                    "underlying_key": "UR3",
                    "settlement_station": "KDCA",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.13",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.94",
                    "prelive_fill_ratio": "0.97",
                    "prelive_settlement_ratio": "0.95",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=40)

            fragile_candidate = self._find_candidate(
                summary,
                min_probability_confidence=0.96,
                min_expected_edge_net=0.05,
                min_edge_to_risk_ratio=0.12,
            )
            robust_candidate = self._find_candidate(
                summary,
                min_probability_confidence=0.92,
                min_expected_edge_net=0.13,
                min_edge_to_risk_ratio=0.16,
            )

            self.assertFalse(bool(fragile_candidate["viable"]))
            self.assertIn("high_entry_low_edge_regime", fragile_candidate["blockers"])
            self.assertGreater(float(fragile_candidate["siphon_markers"]["score_penalty"]), 0.45)
            self.assertTrue(bool(robust_candidate["viable"]))
            self.assertGreater(float(robust_candidate["score"]), float(fragile_candidate["score"]))

            recommended = summary["recommended_configuration"]
            self.assertIsInstance(recommended, dict)
            self.assertAlmostEqual(float(recommended["min_probability_confidence"]), 0.92, places=6)
            self.assertAlmostEqual(float(recommended["min_expected_edge_net"]), 0.13, places=6)
            self.assertAlmostEqual(float(recommended["min_edge_to_risk_ratio"]), 0.16, places=6)

    def test_optimizer_blocks_thin_sample_profiles_and_keeps_robust_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "thin-1",
                    "underlying_key": "UT",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.99",
                    "policy_expected_edge_net": "0.28",
                    "policy_edge_to_risk_ratio": "0.24",
                    "prelive_submission_ratio": "0.90",
                    "prelive_fill_ratio": "0.88",
                    "prelive_settlement_ratio": "0.92",
                },
                {
                    "intent_id": "robust-1",
                    "underlying_key": "UR1",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.13",
                    "prelive_submission_ratio": "0.95",
                    "prelive_fill_ratio": "0.94",
                    "prelive_settlement_ratio": "0.96",
                },
                {
                    "intent_id": "robust-2",
                    "underlying_key": "UR2",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.13",
                    "prelive_submission_ratio": "0.94",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.95",
                },
                {
                    "intent_id": "robust-3",
                    "underlying_key": "UR3",
                    "settlement_station": "KALB",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.13",
                    "prelive_submission_ratio": "0.96",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.97",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=40)

            thin_candidate = self._find_candidate(
                summary,
                min_probability_confidence=0.99,
                min_expected_edge_net=0.28,
                min_edge_to_risk_ratio=0.24,
            )
            self.assertFalse(bool(thin_candidate["viable"]))
            self.assertIn("thin_sample_support_severe", thin_candidate["blockers"])
            self.assertGreaterEqual(float(thin_candidate["selected_thin_sample_support_penalty"]), 0.55)

            recommended = summary["recommended_configuration"]
            self.assertIsInstance(recommended, dict)
            self.assertGreaterEqual(int(recommended["intents_selected"]), 3)
            self.assertGreater(float(recommended["score"]), 0.0)

    def test_optimizer_hardens_repeat_pressure_regimes_with_marker_penalty_and_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "repeat-1",
                    "underlying_key": "URP",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.07",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.61",
                    "prelive_fill_ratio": "0.59",
                    "prelive_settlement_ratio": "0.60",
                },
                {
                    "intent_id": "repeat-2",
                    "underlying_key": "URP",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.07",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.60",
                    "prelive_fill_ratio": "0.58",
                    "prelive_settlement_ratio": "0.59",
                },
                {
                    "intent_id": "repeat-3",
                    "underlying_key": "URP",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.95",
                    "policy_expected_edge_net": "0.07",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.62",
                    "prelive_fill_ratio": "0.60",
                    "prelive_settlement_ratio": "0.61",
                },
                {
                    "intent_id": "robust-repeat-1",
                    "underlying_key": "UR1",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.95",
                    "prelive_fill_ratio": "0.94",
                    "prelive_settlement_ratio": "0.96",
                },
                {
                    "intent_id": "robust-repeat-2",
                    "underlying_key": "UR2",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.94",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.95",
                },
                {
                    "intent_id": "robust-repeat-3",
                    "underlying_key": "UR3",
                    "settlement_station": "KALB",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.16",
                    "prelive_submission_ratio": "0.96",
                    "prelive_fill_ratio": "0.95",
                    "prelive_settlement_ratio": "0.97",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=50)

            repeat_candidate = self._find_candidate(
                summary,
                min_probability_confidence=0.95,
                min_expected_edge_net=0.07,
                min_edge_to_risk_ratio=0.16,
            )
            robust_candidate = self._find_candidate(
                summary,
                min_probability_confidence=0.92,
                min_expected_edge_net=0.12,
                min_edge_to_risk_ratio=0.16,
            )

            self.assertFalse(bool(repeat_candidate["viable"]))
            self.assertIn("repeat_pressure_concentration", repeat_candidate["blockers"])
            self.assertGreater(float(repeat_candidate["siphon_markers"]["repeat_pressure_share"]), 0.95)
            self.assertGreater(float(repeat_candidate["siphon_markers"]["repeat_pressure_penalty"]), 0.95)
            self.assertGreater(float(repeat_candidate["score_components"]["siphon_repeat_pressure_penalty"]), 0.95)
            self.assertGreater(float(repeat_candidate["score_components"]["siphon_penalty"]), 0.15)

            self.assertTrue(bool(robust_candidate["viable"]))
            self.assertLess(float(robust_candidate["siphon_markers"]["repeat_pressure_penalty"]), 0.10)
            self.assertGreater(float(robust_candidate["score"]), float(repeat_candidate["score"]))

            recommended = summary["recommended_configuration"]
            self.assertIsInstance(recommended, dict)
            self.assertNotIn("repeat_pressure_concentration", recommended["blockers"])

    def test_optimizer_merges_multiple_files_and_reports_concentration_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            first = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "dup-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.10",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                    {
                        "intent_id": "dup-2",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.10",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                ],
            )
            second = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_121500.csv",
                [
                    {
                        "intent_id": "dup-2",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "0.10",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                    {
                        "intent_id": "unique-3",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.10",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                ],
            )

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[first, second], top_n=3)

            self.assertEqual(summary["inputs"]["input_files_count"], 2)
            self.assertEqual(summary["inputs"]["rows_valid"], 3)
            self.assertEqual(summary["inputs"]["rows_deduplicated"], 1)
            recommended = summary["recommended_configuration"]
            self.assertIsInstance(recommended, dict)
            self.assertIn("selected_underlying_max_share", recommended)
            self.assertIn("selected_station_max_share", recommended)
            self.assertIn("selected_underlying_hhi", recommended)
            self.assertIn("selected_station_hhi", recommended)
            self.assertAlmostEqual(float(recommended["selected_underlying_max_share"]), 2.0 / 3.0, places=6)
            self.assertAlmostEqual(float(recommended["selected_station_max_share"]), 2.0 / 3.0, places=6)
            self.assertAlmostEqual(float(recommended["selected_underlying_hhi"]), 5.0 / 9.0, places=6)
            self.assertAlmostEqual(float(recommended["selected_station_hhi"]), 5.0 / 9.0, places=6)
            self.assertGreaterEqual(int(recommended["selected_underlying_count"]), 2)
            self.assertGreaterEqual(int(recommended["selected_station_count"]), 2)
            self.assertEqual(int(recommended["intents_selected"]), 3)

    def test_optimizer_reports_blockers_when_no_viable_config_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "bad-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.95",
                        "policy_expected_edge_net": "-0.03",
                        "policy_edge_to_risk_ratio": "0.05",
                    },
                    {
                        "intent_id": "bad-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "-0.01",
                        "policy_edge_to_risk_ratio": "0.06",
                    },
                ],
            )

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=4)

            self.assertEqual(summary["status"], "no_viable_config")
            self.assertIsNone(summary["recommended_configuration"])
            reasons = {blocker["reason"] for blocker in summary["blockers"]}
            self.assertIn("selected_expected_edge_sum_non_positive", reasons)
            self.assertIn("selected_zero_intents", reasons)
            self.assertGreaterEqual(len(summary["top_candidates"]), 1)
            top_candidate = summary["top_candidates"][0]
            self.assertFalse(bool(top_candidate["viable"]))
            self.assertIn("robustness", top_candidate)
            self.assertIn("robustness", summary["search"])
            self.assertFalse(bool(summary["search"]["robustness"]["calibration_available"]))
            self.assertTrue(all(math.isfinite(float(candidate["score"])) for candidate in summary["top_candidates"]))

    def test_optimizer_prefers_confidence_adjusted_weather_pattern_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "wx-pref-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "wx-pref-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                    {
                        "intent_id": "wx-pref-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.91",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                ],
            )
            self._write_weather_artifact(
                base / "health" / "kalshi_temperature_weather_pattern_latest.json",
                {
                    "status": "ready",
                    "negative_expectancy_attempt_share": 0.10,
                    "negative_expectancy_attempt_share_confidence_adjusted": 0.72,
                    "stale_metar_negative_attempt_share": 0.06,
                    "stale_metar_attempt_share_confidence_adjusted": 0.44,
                    "recommendations": {
                        "risk_off": {
                            "recommended": False,
                            "score": 0.10,
                        }
                    },
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(
                input_paths=[intents_path],
                top_n=5,
                weather_risk_off_hard_threshold=0.80,
            )

            weather_inputs = summary["inputs"]["weather_pattern_artifact"]
            self.assertAlmostEqual(float(weather_inputs["negative_expectancy_attempt_share"]), 0.72, places=6)
            self.assertEqual(str(weather_inputs["negative_expectancy_attempt_share_source"]), "confidence_adjusted")
            self.assertAlmostEqual(
                float(weather_inputs["negative_expectancy_attempt_share_confidence_adjusted_observed"]),
                0.72,
                places=6,
            )
            self.assertAlmostEqual(
                float(weather_inputs["negative_expectancy_attempt_share_raw_observed"]),
                0.10,
                places=6,
            )
            self.assertAlmostEqual(float(weather_inputs["stale_metar_negative_attempt_share"]), 0.44, places=6)
            self.assertEqual(str(weather_inputs["stale_metar_negative_attempt_share_source"]), "confidence_adjusted")
            self.assertAlmostEqual(
                float(weather_inputs["stale_metar_negative_attempt_share_confidence_adjusted_observed"]),
                0.44,
                places=6,
            )
            self.assertAlmostEqual(
                float(weather_inputs["stale_metar_negative_attempt_share_raw_observed"]),
                0.06,
                places=6,
            )

            weather_robustness = summary["top_candidates"][0]["robustness"]["weather_risk"]
            self.assertGreater(float(weather_robustness["negative_expectancy_penalty"]), 0.0)
            self.assertGreater(float(weather_robustness["stale_metar_penalty"]), 0.0)
            self.assertEqual(str(weather_robustness["negative_expectancy_attempt_share_source"]), "confidence_adjusted")
            self.assertEqual(str(weather_robustness["stale_metar_negative_attempt_share_source"]), "confidence_adjusted")

    def test_optimizer_applies_weather_risk_penalties_and_surfaces_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "wx-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "wx-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                    {
                        "intent_id": "wx-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.91",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                ],
            )
            self._write_weather_artifact(
                base / "health" / "kalshi_temperature_weather_pattern_latest.json",
                {
                    "status": "ready",
                    "negative_expectancy_attempt_share": 0.72,
                    "stale_metar_negative_attempt_share": 0.44,
                    "recommendations": {
                        "risk_off": {
                            "recommended": True,
                            "score": 0.55,
                        }
                    },
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(
                input_paths=[intents_path],
                top_n=5,
                weather_risk_off_hard_threshold=0.80,
            )

            self.assertEqual(summary["status"], "ready")
            weather_inputs = summary["inputs"]["weather_pattern_artifact"]
            self.assertTrue(bool(weather_inputs["available"]))
            self.assertTrue(str(weather_inputs["source_file"]).endswith("kalshi_temperature_weather_pattern_latest.json"))
            self.assertEqual(str(weather_inputs["negative_expectancy_attempt_share_source"]), "raw")
            self.assertEqual(str(weather_inputs["stale_metar_negative_attempt_share_source"]), "raw")
            self.assertIsNone(weather_inputs["negative_expectancy_attempt_share_confidence_adjusted_observed"])
            self.assertIsNone(weather_inputs["stale_metar_negative_attempt_share_confidence_adjusted_observed"])
            self.assertAlmostEqual(float(weather_inputs["negative_expectancy_attempt_share_raw_observed"]), 0.72, places=6)
            self.assertAlmostEqual(float(weather_inputs["stale_metar_negative_attempt_share_raw_observed"]), 0.44, places=6)

            top_candidate = summary["top_candidates"][0]
            weather_robustness = top_candidate["robustness"]["weather_risk"]
            self.assertTrue(bool(weather_robustness["available"]))
            self.assertAlmostEqual(float(weather_robustness["negative_expectancy_attempt_share"]), 0.72, places=6)
            self.assertAlmostEqual(float(weather_robustness["stale_metar_negative_attempt_share"]), 0.44, places=6)
            self.assertEqual(str(weather_robustness["negative_expectancy_attempt_share_source"]), "raw")
            self.assertEqual(str(weather_robustness["stale_metar_negative_attempt_share_source"]), "raw")
            self.assertTrue(bool(weather_robustness["risk_off_recommended"]))
            self.assertFalse(bool(weather_robustness["hard_block_active"]))
            self.assertGreater(float(weather_robustness["weighted_penalty"]), 0.0)
            self.assertGreater(float(top_candidate["robustness"]["score_penalty"]), 0.0)

            weather_search = summary["search"]["robustness"]["weather_risk"]
            self.assertEqual(str(weather_search["negative_expectancy_attempt_share_source"]), "raw")
            self.assertEqual(str(weather_search["stale_metar_negative_attempt_share_source"]), "raw")
            self.assertGreater(float(weather_search["candidate_weighted_penalty_avg"]), 0.0)
            self.assertEqual(int(weather_search["hard_block_candidate_count"]), 0)

    def test_optimizer_blocks_viability_when_weather_risk_off_above_hard_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "riskoff-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "riskoff-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "riskoff-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                ],
            )
            self._write_weather_artifact(
                base / "health" / "kalshi_temperature_weather_pattern_latest.json",
                {
                    "status": "ready",
                    "negative_expectancy_attempt_share": 0.48,
                    "stale_metar_negative_attempt_share": 0.28,
                    "recommendations": {
                        "risk_off": {
                            "recommended": True,
                            "score": 0.95,
                        }
                    },
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(
                input_paths=[intents_path],
                top_n=5,
                weather_risk_off_hard_threshold=0.80,
            )

            self.assertEqual(summary["status"], "no_viable_config")
            self.assertIsNone(summary["recommended_configuration"])
            reasons = {blocker["reason"] for blocker in summary["blockers"]}
            self.assertIn("weather_risk_off_recommended", reasons)
            top_candidate = summary["top_candidates"][0]
            self.assertFalse(bool(top_candidate["viable"]))
            self.assertIn("weather_risk_off_recommended", top_candidate["blockers"])
            self.assertTrue(bool(top_candidate["robustness"]["weather_risk"]["hard_block_active"]))
            self.assertGreater(
                int(summary["search"]["robustness"]["weather_risk"]["hard_block_candidate_count"]),
                0,
            )

    def test_optimizer_loads_latest_execution_cost_tape_artifact_from_nested_health_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "exec-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                ],
            )
            self._write_execution_artifact(
                base / "health" / "kalshi_temperature_execution_cost_tape_20260422_120000.json",
                {
                    "status": "ready",
                    "spread_median_dollars": 0.02,
                    "spread_p90_dollars": 0.06,
                    "quote_two_sided_ratio": 0.88,
                },
            )
            nested_latest = self._write_execution_artifact(
                base
                / "nested"
                / "ops"
                / "health"
                / "kalshi_temperature_execution_cost_tape_latest.json",
                {
                    "status": "ready",
                    "summary": {
                        "spread_median_dollars": 0.055,
                        "spread_p90_dollars": 0.120,
                        "quote_two_sided_ratio": 0.52,
                        "expected_edge_below_min_share": 0.48,
                        "top_tickers": [
                            {"ticker": "KXTEST1", "share": 0.63},
                            {"ticker": "KXTEST2", "share": 0.21},
                        ],
                    },
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            execution_inputs = summary["inputs"]["execution_cost_tape_artifact"]
            self.assertTrue(bool(execution_inputs["available"]))
            self.assertTrue(str(execution_inputs["source_file"]).endswith(str(nested_latest.name)))
            self.assertAlmostEqual(float(execution_inputs["spread_median_dollars"]), 0.055, places=6)
            self.assertAlmostEqual(float(execution_inputs["spread_p90_dollars"]), 0.12, places=6)
            self.assertAlmostEqual(float(execution_inputs["quote_two_sided_ratio"]), 0.52, places=6)
            self.assertAlmostEqual(float(execution_inputs["expected_edge_below_min_share"]), 0.48, places=6)
            self.assertAlmostEqual(float(execution_inputs["top_ticker_max_share"]), 0.63, places=6)
            self.assertGreater(float(execution_inputs["penalty"]), 0.0)
            self.assertGreater(float(execution_inputs["evidence_coverage"]), 0.5)

    def test_optimizer_loads_execution_cost_tape_latest_default_filename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_130000.csv",
                [
                    {
                        "intent_id": "exec-default-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-default-2",
                        "underlying_key": "U2",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                ],
            )
            latest_path = self._write_execution_artifact(
                base / "health" / "execution_cost_tape_latest.json",
                {
                    "status": "ready",
                    "spread_median_dollars": 0.033,
                    "spread_p90_dollars": 0.091,
                    "quote_two_sided_ratio": 0.71,
                    "expected_edge_below_min_share": 0.42,
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            execution_inputs = summary["inputs"]["execution_cost_tape_artifact"]
            self.assertTrue(bool(execution_inputs["available"]))
            self.assertEqual(str(execution_inputs["source_file"]), str(latest_path))
            self.assertAlmostEqual(float(execution_inputs["spread_median_dollars"]), 0.033, places=6)
            self.assertAlmostEqual(float(execution_inputs["spread_p90_dollars"]), 0.091, places=6)
            self.assertAlmostEqual(float(execution_inputs["quote_two_sided_ratio"]), 0.71, places=6)
            self.assertAlmostEqual(float(execution_inputs["expected_edge_below_min_share"]), 0.42, places=6)

    def test_optimizer_parses_expected_edge_share_from_execution_cost_tape_cli_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_131500.csv",
                [
                    {
                        "intent_id": "exec-cli-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-cli-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                ],
            )
            self._write_execution_artifact(
                base / "health" / "kalshi_temperature_execution_cost_tape_latest.json",
                {
                    "status": "ready",
                    "headline_metrics": {
                        "spread_median_dollars": 0.037,
                        "spread_p90_dollars": 0.106,
                        "quote_two_sided_ratio": 0.66,
                    },
                    "expected_edge_blocking": {
                        "latest_expected_edge_pressure_share_of_blocked": 0.57,
                    },
                },
            )

            summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            execution_inputs = summary["inputs"]["execution_cost_tape_artifact"]
            self.assertTrue(bool(execution_inputs["available"]))
            self.assertAlmostEqual(float(execution_inputs["expected_edge_below_min_share"]), 0.57, places=6)
            self.assertEqual(int(execution_inputs["core_metric_count"]), 4)
            self.assertAlmostEqual(float(execution_inputs["evidence_coverage"]), 1.0, places=6)
            self.assertGreater(float(execution_inputs["penalty"]), 0.0)

    def test_optimizer_execution_friction_penalty_lowers_score_multiplier(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "exec-score-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-score-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                    {
                        "intent_id": "exec-score-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.91",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.10",
                    },
                ],
            )

            baseline = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            self._write_execution_artifact(
                base / "health" / "kalshi_temperature_execution_cost_tape_latest.json",
                {
                    "status": "ready",
                    "spread_median_dollars": 0.11,
                    "spread_p90_dollars": 0.18,
                    "quote_two_sided_ratio": 0.30,
                    "expected_edge_below_min_share": 0.72,
                    "top_ticker_max_share": 0.68,
                },
            )
            with_friction = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            baseline_top = baseline["top_candidates"][0]
            friction_top = with_friction["top_candidates"][0]
            self.assertEqual(float(baseline_top["robustness"]["score_multiplier"]), 1.0)
            self.assertLess(
                float(friction_top["robustness"]["score_multiplier"]),
                float(baseline_top["robustness"]["score_multiplier"]),
            )
            self.assertGreater(float(friction_top["robustness"]["execution_friction"]["penalty"]), 0.75)
            self.assertGreater(
                float(friction_top["robustness"]["execution_friction"]["weighted_penalty"]),
                0.0,
            )
            self.assertGreater(
                float(friction_top["score_components"]["robustness_execution_friction_penalty"]),
                0.0,
            )
            self.assertLess(float(friction_top["score"]), float(baseline_top["score"]))

    def test_optimizer_blocks_on_severe_execution_friction_only_when_weather_elevated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            intents_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                [
                    {
                        "intent_id": "exec-block-1",
                        "underlying_key": "U1",
                        "settlement_station": "KJFK",
                        "policy_probability_confidence": "0.93",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.12",
                    },
                    {
                        "intent_id": "exec-block-2",
                        "underlying_key": "U2",
                        "settlement_station": "KPHL",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                    {
                        "intent_id": "exec-block-3",
                        "underlying_key": "U3",
                        "settlement_station": "KBOS",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.12",
                        "policy_edge_to_risk_ratio": "0.11",
                    },
                ],
            )
            self._write_execution_artifact(
                base / "health" / "kalshi_temperature_execution_cost_tape_latest.json",
                {
                    "status": "ready",
                    "spread_median_dollars": 0.12,
                    "spread_p90_dollars": 0.22,
                    "quote_two_sided_ratio": 0.20,
                    "expected_edge_below_min_share": 0.80,
                },
            )

            self._write_weather_artifact(
                base / "health" / "kalshi_temperature_weather_pattern_latest.json",
                {
                    "status": "ready",
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.35,
                    "recommendations": {
                        "risk_off": {
                            "recommended": True,
                            "score": 0.60,
                        }
                    },
                },
            )
            blocked_summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)

            self.assertEqual(blocked_summary["status"], "no_viable_config")
            blocked_top = blocked_summary["top_candidates"][0]
            self.assertFalse(bool(blocked_top["viable"]))
            self.assertIn("execution_friction_weather_elevated", blocked_top["blockers"])
            self.assertTrue(bool(blocked_top["robustness"]["execution_friction"]["severe"]))
            self.assertGreater(
                float(blocked_top["robustness"]["weather_risk"]["weighted_penalty"]),
                0.30,
            )

            # Remove weather elevation; same severe execution friction should no longer hard-block viability.
            (base / "health" / "kalshi_temperature_weather_pattern_latest.json").unlink()
            unblocked_summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)
            self.assertEqual(unblocked_summary["status"], "ready")
            unblocked_top = unblocked_summary["top_candidates"][0]
            self.assertNotIn("execution_friction_weather_elevated", unblocked_top["blockers"])
            self.assertTrue(bool(unblocked_top["viable"]))

    def test_optimizer_surfaces_taf_missing_station_siphon_marker_and_penalty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            baseline_rows = [
                {
                    "intent_id": "taf-base-1",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
                {
                    "intent_id": "taf-base-2",
                    "underlying_key": "U2",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
                {
                    "intent_id": "taf-base-3",
                    "underlying_key": "U3",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
                {
                    "intent_id": "taf-base-4",
                    "underlying_key": "U4",
                    "settlement_station": "KALB",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
                {
                    "intent_id": "taf-base-5",
                    "underlying_key": "U5",
                    "settlement_station": "KDCA",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
                {
                    "intent_id": "taf-base-6",
                    "underlying_key": "U6",
                    "settlement_station": "KBWI",
                    "policy_probability_confidence": "0.93",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.14",
                    "taf_status": "ready",
                },
            ]
            baseline_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                baseline_rows,
            )
            baseline_summary = run_kalshi_temperature_growth_optimizer(input_paths=[baseline_path], top_n=10)
            baseline_candidate = self._find_candidate(
                baseline_summary,
                min_probability_confidence=0.93,
                min_expected_edge_net=0.12,
                min_edge_to_risk_ratio=0.14,
            )

            stressed_rows = list(baseline_rows)
            for index in (0, 1, 2, 3):
                stressed_rows[index]["forecast_status"] = "missing_station"
                stressed_rows[index].pop("taf_status", None)
            stressed_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_121000.csv",
                stressed_rows,
            )
            stressed_summary = run_kalshi_temperature_growth_optimizer(input_paths=[stressed_path], top_n=10)
            stressed_candidate = self._find_candidate(
                stressed_summary,
                min_probability_confidence=0.93,
                min_expected_edge_net=0.12,
                min_edge_to_risk_ratio=0.14,
            )

            self.assertEqual(float(baseline_candidate["siphon_markers"]["taf_missing_station_share"]), 0.0)
            self.assertEqual(float(baseline_candidate["siphon_markers"]["taf_missing_station_penalty"]), 0.0)
            self.assertAlmostEqual(float(stressed_candidate["selected_taf_missing_station_share"]), 4.0 / 6.0, places=6)
            self.assertGreater(float(stressed_candidate["siphon_markers"]["taf_missing_station_penalty"]), 0.0)
            self.assertGreater(
                float(stressed_candidate["score_components"]["siphon_taf_missing_station_penalty"]),
                0.0,
            )
            self.assertGreater(
                float(stressed_candidate["score_components"]["siphon_penalty"]),
                float(baseline_candidate["score_components"]["siphon_penalty"]),
            )

    def test_optimizer_blocks_taf_missing_station_only_with_severe_share_and_enough_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            severe_rows: list[dict[str, object]] = []
            for idx in range(8):
                severe_rows.append(
                    {
                        "intent_id": f"taf-severe-{idx}",
                        "underlying_key": f"US{idx}",
                        "settlement_station": f"K{idx:03d}",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.13",
                        "taf_status": "missing_station",
                    }
                )
            severe_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_120000.csv",
                severe_rows,
            )
            severe_summary = run_kalshi_temperature_growth_optimizer(input_paths=[severe_path], top_n=10)
            severe_candidate = self._find_candidate(
                severe_summary,
                min_probability_confidence=0.92,
                min_expected_edge_net=0.11,
                min_edge_to_risk_ratio=0.13,
            )
            self.assertFalse(bool(severe_candidate["viable"]))
            self.assertIn("taf_missing_station_concentration", severe_candidate["blockers"])
            self.assertAlmostEqual(float(severe_candidate["selected_taf_missing_station_share"]), 1.0, places=6)

            small_rows: list[dict[str, object]] = []
            for idx in range(4):
                small_rows.append(
                    {
                        "intent_id": f"taf-small-{idx}",
                        "underlying_key": f"UM{idx}",
                        "settlement_station": f"M{idx:03d}",
                        "policy_probability_confidence": "0.92",
                        "policy_expected_edge_net": "0.11",
                        "policy_edge_to_risk_ratio": "0.13",
                        "taf_status": "missing_station",
                    }
                )
            small_path = self._write_csv(
                base / "kalshi_temperature_trade_intents_20260422_121500.csv",
                small_rows,
            )
            small_summary = run_kalshi_temperature_growth_optimizer(input_paths=[small_path], top_n=10)
            small_candidate = self._find_candidate(
                small_summary,
                min_probability_confidence=0.92,
                min_expected_edge_net=0.11,
                min_edge_to_risk_ratio=0.13,
            )
            self.assertNotIn("taf_missing_station_concentration", small_candidate["blockers"])
            self.assertTrue(bool(small_candidate["viable"]))

    def test_optimizer_keeps_backward_compatibility_without_calibration_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            rows = [
                {
                    "intent_id": "base-1",
                    "policy_approved": "true",
                    "underlying_key": "U1",
                    "settlement_station": "KJFK",
                    "policy_probability_confidence": "0.90",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.11",
                },
                {
                    "intent_id": "base-2",
                    "policy_approved": "true",
                    "underlying_key": "U2",
                    "settlement_station": "KPHL",
                    "policy_probability_confidence": "0.91",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.10",
                },
                {
                    "intent_id": "base-3",
                    "policy_approved": "true",
                    "underlying_key": "U3",
                    "settlement_station": "KBOS",
                    "policy_probability_confidence": "0.92",
                    "policy_expected_edge_net": "0.12",
                    "policy_edge_to_risk_ratio": "0.11",
                },
            ]
            intents_path = self._write_csv(base / "kalshi_temperature_trade_intents_20260422_120000.csv", rows)

            default_summary = run_kalshi_temperature_growth_optimizer(input_paths=[intents_path], top_n=5)
            tuned_summary = run_kalshi_temperature_growth_optimizer(
                input_paths=[intents_path],
                top_n=5,
                robustness_conversion_weight=0.9,
                robustness_concentration_weight=0.9,
                robustness_edge_guardrail_weight=0.9,
                robustness_bonus_cap=0.5,
                robustness_edge_median_floor=0.01,
                robustness_edge_median_target=0.02,
                robustness_tail_ratio_floor=0.2,
            )

            self.assertFalse(bool(default_summary["search"]["robustness"]["calibration_available"]))
            self.assertFalse(bool(tuned_summary["search"]["robustness"]["calibration_available"]))
            self.assertEqual(
                float(default_summary["recommended_configuration"]["score"]),
                float(tuned_summary["recommended_configuration"]["score"]),
            )
            self.assertEqual(
                float(default_summary["top_candidates"][0]["score"]),
                float(tuned_summary["top_candidates"][0]["score"]),
            )
            self.assertEqual(
                float(default_summary["top_candidates"][0]["robustness"]["score_multiplier"]),
                1.0,
            )


if __name__ == "__main__":
    unittest.main()
