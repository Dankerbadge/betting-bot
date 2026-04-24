from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from betbot.kalshi_temperature_selection_quality import (
    load_temperature_selection_quality_profile,
    run_kalshi_temperature_selection_quality,
    selection_quality_adjustment_for_intent,
)


class KalshiTemperatureSelectionQualityTests(unittest.TestCase):
    def _write_bankroll_validation(
        self,
        *,
        output_dir: Path,
        captured_at: datetime,
        resolved_unique_market_sides: int = 24,
        concentration_warning: bool = False,
    ) -> Path:
        payload = {
            "captured_at": captured_at.isoformat(),
            "opportunity_breadth": {
                "resolved_planned_rows": 96,
                "resolved_unique_market_sides": resolved_unique_market_sides,
                "repeated_entry_multiplier": 4.0,
            },
            "hit_rate_quality": {
                "unique_market_side": {
                    "wins": 17,
                    "losses": 7,
                    "pushes": 0,
                    "win_rate": 0.708333,
                    "expectancy_per_trade": 0.031,
                }
            },
            "expected_vs_shadow_settled": {
                "calibration_ratio": 0.82,
            },
            "concentration_checks": {
                "concentration_warning": concentration_warning,
            },
            "attribution": {
                "fixed_fraction_per_underlying_family": {
                    "by_station": {
                        "worst": [
                            {"key": "KNYC", "trade_count": 16, "win_rate": 0.40, "pnl_total": -0.48},
                        ],
                        "best": [
                            {"key": "KPHL", "trade_count": 14, "win_rate": 0.86, "pnl_total": 0.56},
                        ],
                    },
                    "by_local_hour": {
                        "worst": [
                            {"key": "8", "trade_count": 12, "win_rate": 0.42, "pnl_total": -0.36},
                        ],
                        "best": [],
                    },
                    "by_signal_type": {
                        "worst": [
                            {
                                "key": "yes_impossible",
                                "trade_count": 10,
                                "win_rate": 0.45,
                                "pnl_total": -0.24,
                            },
                        ],
                        "best": [
                            {"key": "no", "trade_count": 10, "win_rate": 0.83, "pnl_total": 0.35},
                        ],
                    },
                    "by_policy_reason": {
                        "worst": [],
                        "best": [
                            {"key": "range_gap_large", "trade_count": 8, "win_rate": 0.80, "pnl_total": 0.24},
                        ],
                    },
                }
            },
        }
        path = output_dir / f"kalshi_temperature_bankroll_validation_{captured_at:%Y%m%d_%H%M%S}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _write_profitability_checkpoint(
        self,
        *,
        output_dir: Path,
        captured_at: datetime,
        expected_edge_total: float,
        counterfactual_unique_shadow_orders: float,
        resolved_unique_market_sides: int,
    ) -> Path:
        checkpoints = output_dir / "checkpoints"
        checkpoints.mkdir(parents=True, exist_ok=True)
        payload = {
            "captured_at": captured_at.isoformat(),
            "expected_shadow": {
                "planned_orders": 100,
                "expected_edge_total": expected_edge_total,
                "estimated_entry_cost_total": 500.0,
            },
            "shadow_settled_reference": {
                "resolved_unique_market_sides": resolved_unique_market_sides,
                "counterfactual_pnl_total_unique_shadow_orders_dollars_if_live": counterfactual_unique_shadow_orders,
            },
            "attribution": {
                "by_station": {
                    "KAAA": {
                        "planned_orders": 20,
                        "expected_edge_total": 16.0,
                        "approval_rate": 0.08,
                    },
                    "KBBB": {
                        "planned_orders": 5,
                        "expected_edge_total": 2.0,
                        "approval_rate": 0.20,
                    },
                },
                "by_local_hour": {
                    "22": {
                        "planned_orders": 14,
                        "expected_edge_total": 10.0,
                        "approval_rate": 0.10,
                    },
                    "unknown": {
                        "planned_orders": 25,
                        "expected_edge_total": 12.0,
                        "approval_rate": 0.25,
                    },
                },
            },
        }
        path = checkpoints / f"profitability_168h_{captured_at:%Y%m%d_%H%M%S}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def _write_selection_quality_snapshot(
        self,
        *,
        output_dir: Path,
        captured_at: datetime,
        rows_total: int,
        rows_adjusted: int,
        rows_adjusted_global_only: int,
    ) -> Path:
        payload = {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "intent_window": {
                "rows_total": int(rows_total),
                "rows_adjusted": int(rows_adjusted),
                "rows_adjusted_global_only": int(rows_adjusted_global_only),
            },
        }
        path = output_dir / f"kalshi_temperature_selection_quality_{captured_at:%Y%m%d_%H%M%S}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_load_profile_returns_ready_with_bucket_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=2),
                resolved_unique_market_sides=30,
            )
            profile = load_temperature_selection_quality_profile(
                output_dir=str(base),
                now_utc=now,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                preferred_attribution_model="fixed_fraction_per_underlying_family",
                max_profile_age_hours=96.0,
            )
            self.assertEqual(profile["status"], "ready")
            self.assertEqual(profile["attribution_model_used"], "fixed_fraction_per_underlying_family")
            self.assertEqual(profile["resolved_unique_market_sides"], 30)
            self.assertGreater(float(profile["evidence_confidence"]), 0.0)
            station_bucket = profile["bucket_profiles"]["station"]
            self.assertIn("KNYC", station_bucket)
            self.assertGreater(float(station_bucket["KNYC"]["penalty_ratio"]), 0.0)
            self.assertIn("KPHL", station_bucket)
            self.assertGreater(float(station_bucket["KPHL"]["boost_ratio"]), 0.0)
            self.assertIn("no", profile["bucket_profiles"]["side"])

    def test_load_profile_reports_insufficient_and_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=1),
                resolved_unique_market_sides=4,
            )
            insufficient = load_temperature_selection_quality_profile(
                output_dir=str(base),
                now_utc=now,
                min_resolved_market_sides=12,
                max_profile_age_hours=96.0,
            )
            self.assertEqual(insufficient["status"], "insufficient_resolved_market_sides")

        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=140),
                resolved_unique_market_sides=30,
            )
            stale = load_temperature_selection_quality_profile(
                output_dir=str(base),
                now_utc=now,
                min_resolved_market_sides=12,
                max_profile_age_hours=24.0,
            )
            self.assertEqual(stale["status"], "stale_profile")

    def test_load_profile_includes_global_adjustment_profile_from_selection_quality(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=2),
                resolved_unique_market_sides=30,
            )
            self._write_selection_quality_snapshot(
                output_dir=base,
                captured_at=now - timedelta(minutes=20),
                rows_total=2200,
                rows_adjusted=900,
                rows_adjusted_global_only=240,
            )
            profile = load_temperature_selection_quality_profile(
                output_dir=str(base),
                now_utc=now,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                preferred_attribution_model="fixed_fraction_per_underlying_family",
                max_profile_age_hours=96.0,
            )
            global_adjustment = profile.get("global_adjustment_profile")
            self.assertIsInstance(global_adjustment, dict)
            self.assertEqual(global_adjustment.get("status"), "ready")
            self.assertEqual(int(global_adjustment.get("rows_adjusted") or 0), 900)
            self.assertEqual(int(global_adjustment.get("rows_adjusted_global_only") or 0), 240)
            self.assertAlmostEqual(
                float(global_adjustment.get("global_only_adjusted_share") or 0.0),
                240.0 / 900.0,
                places=6,
            )
            self.assertTrue(bool(global_adjustment.get("pressure_active")))

    def test_selection_adjustment_uses_historical_penalty_and_sources(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            constraint_status="yes_impossible",
            side="no",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        profile = {
            "enabled": True,
            "status": "ready",
            "global_penalty_ratio": 0.6,
            "global_boost_ratio": 0.2,
            "bucket_profiles": {
                "station": {
                    "KNYC": {"penalty_ratio": 0.8, "boost_ratio": 0.0, "samples": 24},
                },
                "local_hour": {
                    "8": {"penalty_ratio": 0.5, "boost_ratio": 0.0, "samples": 18},
                },
                "signal_type": {
                    "yes_impossible": {"penalty_ratio": 0.6, "boost_ratio": 0.0, "samples": 16},
                },
                "side": {
                    "no": {"penalty_ratio": 0.0, "boost_ratio": 0.3, "samples": 16},
                },
            },
        }
        adjustment = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=profile,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertEqual(adjustment["status"], "ready")
        self.assertGreater(float(adjustment["penalty_ratio"]), 0.0)
        self.assertGreater(float(adjustment["probability_raise"]), 0.0)
        self.assertGreater(float(adjustment["expected_edge_raise"]), 0.0)
        self.assertGreater(int(adjustment["sample_size"]), 0)
        self.assertTrue(any(str(item).startswith("station:KNYC:") for item in adjustment["sources"]))

    def test_selection_adjustment_hardens_under_global_only_pressure(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KXXX",
            settlement_timezone="America/New_York",
            constraint_status="unknown_signal",
            side="buy",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        base_profile = {
            "enabled": True,
            "status": "ready",
            "global_penalty_ratio": 0.55,
            "global_boost_ratio": 0.25,
            "repeated_entry_multiplier_penalty_ratio": 0.35,
            "fallback_profile_applied": True,
            "evidence_confidence": 0.42,
            "source_age_hours": 2.0,
            "max_profile_age_hours": 96.0,
            "bucket_profiles": {
                "station": {},
                "local_hour": {},
                "signal_type": {},
                "side": {},
            },
        }
        calm_profile = dict(base_profile)
        calm_profile["global_adjustment_profile"] = {
            "pressure_active": False,
            "global_only_adjusted_share": 0.08,
            "target_share": 0.10,
            "rows_adjusted": 800,
            "min_rows_for_pressure": 100,
        }
        pressure_profile = dict(base_profile)
        pressure_profile["global_adjustment_profile"] = {
            "pressure_active": True,
            "global_only_adjusted_share": 0.35,
            "target_share": 0.10,
            "rows_adjusted": 800,
            "min_rows_for_pressure": 100,
        }
        calm = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=calm_profile,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        pressure = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=pressure_profile,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertFalse(bool(calm.get("global_only_pressure_active")))
        self.assertTrue(bool(pressure.get("global_only_pressure_active")))
        self.assertGreater(float(pressure.get("penalty_ratio") or 0.0), float(calm.get("penalty_ratio") or 0.0))
        self.assertGreater(float(pressure.get("probability_raise") or 0.0), float(calm.get("probability_raise") or 0.0))
        self.assertGreater(
            float(pressure.get("expected_edge_raise") or 0.0),
            float(calm.get("expected_edge_raise") or 0.0),
        )
        self.assertTrue(
            any(str(item).startswith("global_only_share:") for item in (pressure.get("sources") or []))
        )

    def test_load_profile_merges_profitability_gap_bucket_penalties(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=2),
                resolved_unique_market_sides=30,
            )
            self._write_profitability_checkpoint(
                output_dir=base,
                captured_at=now - timedelta(minutes=30),
                expected_edge_total=40.0,
                counterfactual_unique_shadow_orders=20.0,
                resolved_unique_market_sides=12,
            )
            profile = load_temperature_selection_quality_profile(
                output_dir=str(base),
                now_utc=now,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                preferred_attribution_model="fixed_fraction_per_underlying_family",
                max_profile_age_hours=96.0,
            )
            gap_meta = profile.get("profitability_calibration_gap")
            self.assertIsInstance(gap_meta, dict)
            self.assertTrue(bool(gap_meta.get("enabled")))
            self.assertEqual(gap_meta.get("status"), "ready")
            self.assertAlmostEqual(float(gap_meta.get("calibration_ratio") or 0.0), 0.5, places=6)

            station_bucket = profile["bucket_profiles"]["station"]
            self.assertIn("KAAA", station_bucket)
            self.assertIn("KBBB", station_bucket)
            self.assertGreater(
                float(station_bucket["KAAA"]["penalty_ratio"]),
                float(station_bucket["KBBB"]["penalty_ratio"]),
            )
            self.assertIn("22", profile["bucket_profiles"]["local_hour"])
            self.assertNotIn("unknown", profile["bucket_profiles"]["local_hour"])

    def test_load_profile_uses_sibling_fallback_when_local_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            active = root / "active"
            sibling = root / "sibling"
            active.mkdir(parents=True, exist_ok=True)
            sibling.mkdir(parents=True, exist_ok=True)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)

            local_path = self._write_bankroll_validation(
                output_dir=active,
                captured_at=now - timedelta(minutes=20),
                resolved_unique_market_sides=1,
            )
            sibling_path = self._write_bankroll_validation(
                output_dir=sibling,
                captured_at=now - timedelta(hours=2),
                resolved_unique_market_sides=36,
            )

            profile = load_temperature_selection_quality_profile(
                output_dir=str(active),
                now_utc=now,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                preferred_attribution_model="fixed_fraction_per_underlying_family",
                max_profile_age_hours=96.0,
            )

            self.assertEqual(profile["status"], "ready")
            self.assertTrue(bool(profile.get("fallback_profile_applied")))
            self.assertEqual(str(profile.get("source_file")), str(sibling_path))
            self.assertEqual(
                str(profile.get("fallback_profile_source_file")),
                str(sibling_path),
            )
            self.assertNotEqual(str(profile.get("source_file")), str(local_path))
            self.assertEqual(int(profile.get("resolved_unique_market_sides") or 0), 36)

    def test_load_profile_uses_sibling_profitability_gap_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            active = root / "active"
            sibling = root / "sibling"
            active.mkdir(parents=True, exist_ok=True)
            sibling.mkdir(parents=True, exist_ok=True)
            now = datetime(2026, 4, 20, 16, 0, tzinfo=timezone.utc)

            self._write_bankroll_validation(
                output_dir=active,
                captured_at=now - timedelta(minutes=30),
                resolved_unique_market_sides=30,
            )
            self._write_profitability_checkpoint(
                output_dir=active,
                captured_at=now - timedelta(minutes=20),
                expected_edge_total=30.0,
                counterfactual_unique_shadow_orders=15.0,
                resolved_unique_market_sides=2,
            )
            sibling_profitability = self._write_profitability_checkpoint(
                output_dir=sibling,
                captured_at=now - timedelta(hours=1),
                expected_edge_total=50.0,
                counterfactual_unique_shadow_orders=20.0,
                resolved_unique_market_sides=14,
            )

            profile = load_temperature_selection_quality_profile(
                output_dir=str(active),
                now_utc=now,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                preferred_attribution_model="fixed_fraction_per_underlying_family",
                max_profile_age_hours=96.0,
            )
            gap_meta = profile.get("profitability_calibration_gap")
            self.assertIsInstance(gap_meta, dict)
            self.assertTrue(bool(gap_meta.get("enabled")))
            self.assertTrue(bool(profile.get("profitability_calibration_gap_fallback_applied")))
            self.assertEqual(
                str(profile.get("profitability_calibration_gap_fallback_source_file")),
                str(sibling_profitability),
            )
            self.assertEqual(str(gap_meta.get("source_file")), str(sibling_profitability))

    def test_selection_adjustment_returns_zero_when_disabled(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            constraint_status="yes_impossible",
            side="no",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        adjustment = selection_quality_adjustment_for_intent(
            intent=intent,
            profile={"enabled": False, "status": "disabled"},
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertEqual(adjustment["status"], "disabled")
        self.assertEqual(float(adjustment["probability_raise"]), 0.0)
        self.assertEqual(float(adjustment["expected_edge_raise"]), 0.0)
        self.assertEqual(float(adjustment["score_adjustment"]), 0.0)
        self.assertEqual(float(adjustment["evidence_quality_scale"]), 0.0)
        self.assertFalse(bool(adjustment["evidence_quality_weak_profile"]))

    def test_selection_adjustment_preserves_strong_penalties_for_strong_evidence(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            constraint_status="yes_impossible",
            side="no",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        profile = {
            "enabled": True,
            "status": "ready",
            "global_penalty_ratio": 0.6,
            "global_boost_ratio": 0.2,
            "evidence_confidence": 0.95,
            "resolved_unique_market_sides": 36,
            "min_resolved_market_sides_required": 12,
            "source_age_hours": 1.5,
            "max_profile_age_hours": 96.0,
            "bucket_profiles": {
                "station": {
                    "KNYC": {"penalty_ratio": 0.8, "boost_ratio": 0.0, "samples": 24},
                },
                "local_hour": {
                    "8": {"penalty_ratio": 0.5, "boost_ratio": 0.0, "samples": 18},
                },
                "signal_type": {
                    "yes_impossible": {"penalty_ratio": 0.6, "boost_ratio": 0.0, "samples": 16},
                },
                "side": {
                    "no": {"penalty_ratio": 0.0, "boost_ratio": 0.3, "samples": 16},
                },
            },
        }
        adjustment = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=profile,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertEqual(adjustment["status"], "ready")
        self.assertFalse(bool(adjustment["evidence_quality_weak_profile"]))
        self.assertGreater(float(adjustment["evidence_quality_scale"]), 0.9)
        self.assertGreater(float(adjustment["penalty_ratio"]), 0.45)
        self.assertGreater(float(adjustment["probability_raise"]), 0.02)
        self.assertGreater(float(adjustment["expected_edge_raise"]), 0.0025)
        self.assertGreater(int(adjustment["sample_size"]), 0)
        self.assertTrue(any(str(item).startswith("station:KNYC:") for item in adjustment["sources"]))

    def test_selection_adjustment_caps_weak_or_stale_evidence(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            constraint_status="yes_impossible",
            side="no",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        profile = {
            "enabled": True,
            "status": "stale_profile",
            "global_penalty_ratio": 0.6,
            "global_boost_ratio": 0.2,
            "evidence_confidence": 0.38,
            "resolved_unique_market_sides": 4,
            "min_resolved_market_sides_required": 12,
            "source_age_hours": 120.0,
            "max_profile_age_hours": 24.0,
            "fallback_profile_applied": True,
            "bucket_profiles": {
                "station": {
                    "KNYC": {"penalty_ratio": 0.8, "boost_ratio": 0.0, "samples": 6},
                },
                "local_hour": {
                    "8": {"penalty_ratio": 0.5, "boost_ratio": 0.0, "samples": 4},
                },
                "signal_type": {
                    "yes_impossible": {"penalty_ratio": 0.6, "boost_ratio": 0.0, "samples": 3},
                },
                "side": {
                    "no": {"penalty_ratio": 0.0, "boost_ratio": 0.3, "samples": 3},
                },
            },
        }
        adjustment = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=profile,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertEqual(adjustment["status"], "stale_profile")
        self.assertTrue(bool(adjustment["evidence_quality_weak_profile"]))
        self.assertLess(float(adjustment["evidence_quality_scale"]), 0.75)
        self.assertGreater(float(adjustment["penalty_ratio"]), 0.0)
        self.assertLessEqual(
            float(adjustment["penalty_ratio"]),
            float(adjustment["evidence_quality_penalty_cap_ratio"]),
        )
        self.assertGreaterEqual(
            float(adjustment["probability_raise"]),
            float(adjustment["evidence_quality_probability_raise_floor"]),
        )
        self.assertLessEqual(
            float(adjustment["probability_raise"]),
            float(adjustment["evidence_quality_probability_raise_cap"]),
        )
        self.assertLessEqual(
            abs(float(adjustment["score_adjustment"])),
            float(adjustment["evidence_quality_score_adjustment_cap"]),
        )

    def test_selection_adjustment_no_profile_is_deterministic_and_safe(self) -> None:
        intent = SimpleNamespace(
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            constraint_status="yes_impossible",
            side="no",
            metar_observation_time_utc="2026-04-20T12:30:00Z",
            captured_at="2026-04-20T12:31:00Z",
        )
        first = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=None,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        second = selection_quality_adjustment_for_intent(
            intent=intent,
            profile=None,
            probability_penalty_max=0.05,
            expected_edge_penalty_max=0.006,
            score_adjust_scale=0.35,
        )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "disabled")
        self.assertEqual(float(first["penalty_ratio"]), 0.0)
        self.assertEqual(float(first["probability_raise"]), 0.0)
        self.assertEqual(float(first["expected_edge_raise"]), 0.0)
        self.assertEqual(float(first["score_adjustment"]), 0.0)
        self.assertEqual(float(first["evidence_quality_scale"]), 0.0)
        self.assertEqual(float(first["evidence_quality_penalty_cap_ratio"]), 0.0)

    def test_run_selection_quality_writes_output_and_latest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            now = datetime.now(timezone.utc)
            self._write_bankroll_validation(
                output_dir=base,
                captured_at=now - timedelta(hours=2),
                resolved_unique_market_sides=30,
            )
            intent_file = base / f"kalshi_temperature_trade_intents_{now:%Y%m%d_%H%M%S}.csv"
            intent_file.write_text(
                "\n".join(
                    [
                        "captured_at_utc,policy_approved,settlement_station,settlement_timezone,constraint_status,side,metar_observation_time_utc",
                        (
                            f"{(now - timedelta(minutes=30)).isoformat()},true,"
                            "KSEA,,unknown_signal,buy,"
                            f"{(now - timedelta(minutes=31)).isoformat()}"
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            summary = run_kalshi_temperature_selection_quality(
                output_dir=str(base),
                lookback_hours=336.0,
                intent_hours=24.0,
                min_resolved_market_sides=12,
                min_bucket_samples=4,
                top_n=5,
            )
            self.assertEqual(summary.get("status"), "ready")
            self.assertTrue(Path(str(summary.get("output_file"))).exists())
            intent_window = summary.get("intent_window", {})
            self.assertEqual(intent_window.get("rows_total"), 1)
            self.assertEqual(intent_window.get("rows_approved"), 1)
            self.assertEqual(intent_window.get("rows_adjusted"), 1)
            self.assertEqual(intent_window.get("rows_adjusted_global_only"), 1)
            self.assertEqual(intent_window.get("rows_adjusted_bucket_backed"), 0)
            latest = base / "kalshi_temperature_selection_quality_latest.json"
            self.assertTrue(latest.exists())
            latest_payload = json.loads(latest.read_text(encoding="utf-8"))
            self.assertEqual(latest_payload.get("status"), "ready")
            self.assertEqual((latest_payload.get("intent_window") or {}).get("rows_total"), 1)


if __name__ == "__main__":
    unittest.main()
