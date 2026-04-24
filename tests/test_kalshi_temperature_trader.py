from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from betbot.kalshi_temperature_trader import (
    _apply_market_side_replan_cooldown,
    _load_alpha_consensus,
    TemperaturePolicyGate,
    TemperaturePortfolioPlanner,
    TemperatureTradeIntent,
    build_temperature_trade_intents,
    revalidate_temperature_trade_intents,
    run_kalshi_temperature_trader,
    run_kalshi_temperature_shadow_watch,
)


class KalshiTemperatureTraderTests(unittest.TestCase):
    def _make_intent(
        self,
        *,
        intent_id: str,
        market_ticker: str,
        underlying_key: str,
        settlement_station: str,
        settlement_timezone: str = "America/New_York",
        side: str = "no",
        max_entry_price_dollars: float = 0.95,
        constraint_status: str = "yes_impossible",
        metar_observation_age_minutes: float = 5.0,
        settlement_confidence_score: float = 0.95,
        metar_observation_time_utc: str = "2026-04-08T11:55:00Z",
        forecast_model_status: str = "",
        taf_status: str = "",
    ) -> TemperatureTradeIntent:
        return TemperatureTradeIntent(
            intent_id=intent_id,
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key=underlying_key,
            series_ticker=underlying_key.split("|")[0],
            event_ticker=f"{underlying_key}-event",
            market_ticker=market_ticker,
            market_title=market_ticker,
            settlement_station=settlement_station,
            settlement_timezone=settlement_timezone,
            target_date_local="2026-04-08",
            constraint_status=constraint_status,
            constraint_reason="test",
            side=side,
            max_entry_price_dollars=max_entry_price_dollars,
            intended_contracts=1,
            settlement_confidence_score=settlement_confidence_score,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash=f"spec-{intent_id}",
            metar_snapshot_sha=f"sha-{intent_id}",
            metar_observation_time_utc=metar_observation_time_utc,
            metar_observation_age_minutes=metar_observation_age_minutes,
            market_snapshot_seq=22,
            forecast_model_status=forecast_model_status,
            taf_status=taf_status,
        )

    def _write_basic_temperature_inputs(
        self,
        *,
        base: Path,
        now: datetime,
        include_ws_sequence: bool,
    ) -> dict[str, str]:
        env_file = base / "env.txt"
        env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

        specs_csv = base / "specs.csv"
        with specs_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "series_ticker",
                    "event_ticker",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "settlement_station",
                    "settlement_timezone",
                    "target_date_local",
                    "rules_primary",
                    "rules_secondary",
                    "local_day_boundary",
                    "observation_window_local_start",
                    "observation_window_local_end",
                    "threshold_expression",
                    "contract_terms_url",
                    "settlement_confidence_score",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "market_title": "72F or above",
                    "close_time": "2026-04-09T00:00:00Z",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "rules_primary": "Highest temperature in local day at KNYC.",
                    "rules_secondary": "",
                    "local_day_boundary": "local_day",
                    "observation_window_local_start": "00:00",
                    "observation_window_local_end": "23:59",
                    "threshold_expression": "at_most:72",
                    "contract_terms_url": "https://example.test/terms",
                    "settlement_confidence_score": "0.92",
                }
            )

        constraint_csv = base / "constraints.csv"
        with constraint_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "scanned_at",
                    "source_specs_csv",
                    "series_ticker",
                    "event_ticker",
                    "market_ticker",
                    "market_title",
                    "settlement_station",
                    "settlement_timezone",
                    "target_date_local",
                    "settlement_unit",
                    "settlement_precision",
                    "threshold_expression",
                    "constraint_status",
                    "constraint_reason",
                    "observed_max_settlement_raw",
                    "observed_max_settlement_quantized",
                    "observations_for_date",
                    "snapshot_status",
                    "settlement_confidence_score",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "scanned_at": now.isoformat(),
                    "source_specs_csv": str(specs_csv),
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "market_title": "72F or above",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "settlement_unit": "fahrenheit",
                    "settlement_precision": "whole_degree",
                    "threshold_expression": "at_most:72",
                    "constraint_status": "yes_impossible",
                    "constraint_reason": "Observed max 74 exceeds at_most threshold 72.",
                    "observed_max_settlement_raw": "74",
                    "observed_max_settlement_quantized": "74",
                    "observations_for_date": "12",
                    "snapshot_status": "ready",
                    "settlement_confidence_score": "0.92",
                }
            )

        metar_state = base / "metar_state.json"
        metar_state.write_text(
            json.dumps(
                {
                    "latest_observation_by_station": {
                        "KNYC": {
                            "observation_time_utc": "2026-04-08T11:55:00Z",
                            "temp_c": 24.5,
                        }
                    },
                    "max_temp_c_by_station_local_day": {},
                }
            ),
            encoding="utf-8",
        )
        metar_summary = base / "metar_summary.json"
        metar_summary.write_text(
            json.dumps(
                {
                    "status": "ready",
                    "captured_at": now.isoformat(),
                    "raw_sha256": "feedbead1234",
                    "state_file": str(metar_state),
                }
            ),
            encoding="utf-8",
        )

        ws_state = base / "ws_state.json"
        ws_markets = (
            {
                "KXHIGHNY-26APR08-B72": {
                    "sequence": 22,
                    "updated_at_utc": now.isoformat(),
                }
            }
            if include_ws_sequence
            else {}
        )
        ws_state.write_text(
            json.dumps(
                {
                    "summary": {
                        "status": "ready",
                        "market_count": 1 if include_ws_sequence else 0,
                        "desynced_market_count": 0,
                        "last_event_at": now.isoformat(),
                    },
                    "markets": ws_markets,
                }
            ),
            encoding="utf-8",
        )

        return {
            "env_file": str(env_file),
            "specs_csv": str(specs_csv),
            "constraint_csv": str(constraint_csv),
            "metar_summary": str(metar_summary),
            "ws_state": str(ws_state),
        }

    def _append_temperature_market_inputs(
        self,
        *,
        specs_csv: Path,
        constraint_csv: Path,
        now: datetime,
        market_ticker: str,
        market_title: str,
        constraint_status: str,
        threshold_expression: str,
        observed_max_settlement_raw: str,
        observed_max_settlement_quantized: str,
        settlement_station: str = "KNYC",
        settlement_timezone: str = "America/New_York",
        target_date_local: str = "2026-04-08",
    ) -> None:
        with specs_csv.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "series_ticker",
                    "event_ticker",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "settlement_station",
                    "settlement_timezone",
                    "target_date_local",
                    "rules_primary",
                    "rules_secondary",
                    "local_day_boundary",
                    "observation_window_local_start",
                    "observation_window_local_end",
                    "threshold_expression",
                    "contract_terms_url",
                    "settlement_confidence_score",
                ],
            )
            writer.writerow(
                {
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": market_ticker,
                    "market_title": market_title,
                    "close_time": "2026-04-09T00:00:00Z",
                    "settlement_station": settlement_station,
                    "settlement_timezone": settlement_timezone,
                    "target_date_local": target_date_local,
                    "rules_primary": f"Highest temperature in local day at {settlement_station}.",
                    "rules_secondary": "",
                    "local_day_boundary": "local_day",
                    "observation_window_local_start": "00:00",
                    "observation_window_local_end": "23:59",
                    "threshold_expression": threshold_expression,
                    "contract_terms_url": "https://example.test/terms",
                    "settlement_confidence_score": "0.92",
                }
            )

        with constraint_csv.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "scanned_at",
                    "source_specs_csv",
                    "series_ticker",
                    "event_ticker",
                    "market_ticker",
                    "market_title",
                    "settlement_station",
                    "settlement_timezone",
                    "target_date_local",
                    "settlement_unit",
                    "settlement_precision",
                    "threshold_expression",
                    "constraint_status",
                    "constraint_reason",
                    "observed_max_settlement_raw",
                    "observed_max_settlement_quantized",
                    "observations_for_date",
                    "snapshot_status",
                    "settlement_confidence_score",
                ],
            )
            writer.writerow(
                {
                    "scanned_at": now.isoformat(),
                    "source_specs_csv": str(specs_csv),
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": market_ticker,
                    "market_title": market_title,
                    "settlement_station": settlement_station,
                    "settlement_timezone": settlement_timezone,
                    "target_date_local": target_date_local,
                    "settlement_unit": "fahrenheit",
                    "settlement_precision": "whole_degree",
                    "threshold_expression": threshold_expression,
                    "constraint_status": constraint_status,
                    "constraint_reason": f"Fixture row for {market_ticker}.",
                    "observed_max_settlement_raw": observed_max_settlement_raw,
                    "observed_max_settlement_quantized": observed_max_settlement_quantized,
                    "observations_for_date": "12",
                    "snapshot_status": "ready",
                    "settlement_confidence_score": "0.92",
                }
            )

    def _run_basic_trader_summary(
        self,
        *,
        adaptive_policy_profile: dict[str, object] | None = None,
        weather_pattern_profile: dict[str, object] | None = None,
        weather_pattern_hardening_enabled: bool = True,
        weather_pattern_profile_max_age_hours: float = 72.0,
        weather_pattern_min_bucket_samples: int = 12,
        weather_pattern_negative_expectancy_threshold: float = -0.05,
        weather_pattern_negative_regime_suppression_enabled: bool = False,
        weather_pattern_negative_regime_suppression_min_bucket_samples: int = 24,
        weather_pattern_negative_regime_suppression_expectancy_threshold: float = -0.08,
        weather_pattern_negative_regime_suppression_top_n: int = 8,
        weather_pattern_profile_artifact: dict[str, object] | None = None,
        weather_pattern_profile_artifact_age_hours: float | None = None,
        weather_pattern_profile_artifact_relative_path: str = "weather_pattern/weather_pattern_profile_20260408_115500.json",
        weather_pattern_risk_off_enabled: bool = True,
        weather_pattern_risk_off_concentration_threshold: float = 0.75,
        weather_pattern_risk_off_min_attempts: int = 24,
        weather_pattern_risk_off_stale_metar_share_threshold: float = 0.50,
    ) -> dict[str, object]:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            if weather_pattern_profile_artifact is not None:
                artifact_path = base / weather_pattern_profile_artifact_relative_path
                artifact_path.parent.mkdir(parents=True, exist_ok=True)
                artifact_path.write_text(json.dumps(weather_pattern_profile_artifact), encoding="utf-8")
                if weather_pattern_profile_artifact_age_hours is not None:
                    age_seconds = max(0.0, float(weather_pattern_profile_artifact_age_hours)) * 3600.0
                    past_ts = (now.timestamp() - age_seconds)
                    os.utime(artifact_path, (past_ts, past_ts))
            return run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                adaptive_policy_profile=adaptive_policy_profile,
                weather_pattern_profile=weather_pattern_profile,
                weather_pattern_hardening_enabled=weather_pattern_hardening_enabled,
                weather_pattern_profile_max_age_hours=weather_pattern_profile_max_age_hours,
                weather_pattern_min_bucket_samples=weather_pattern_min_bucket_samples,
                weather_pattern_negative_expectancy_threshold=weather_pattern_negative_expectancy_threshold,
                weather_pattern_negative_regime_suppression_enabled=(
                    weather_pattern_negative_regime_suppression_enabled
                ),
                weather_pattern_negative_regime_suppression_min_bucket_samples=(
                    weather_pattern_negative_regime_suppression_min_bucket_samples
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=(
                    weather_pattern_negative_regime_suppression_expectancy_threshold
                ),
                weather_pattern_negative_regime_suppression_top_n=(
                    weather_pattern_negative_regime_suppression_top_n
                ),
                weather_pattern_risk_off_enabled=weather_pattern_risk_off_enabled,
                weather_pattern_risk_off_concentration_threshold=weather_pattern_risk_off_concentration_threshold,
                weather_pattern_risk_off_min_attempts=weather_pattern_risk_off_min_attempts,
                weather_pattern_risk_off_stale_metar_share_threshold=weather_pattern_risk_off_stale_metar_share_threshold,
                now=now,
            )

    def test_build_temperature_trade_intents_carries_alpha_feature_columns(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B74",
                "market_title": "74F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "75",
                "forecast_upper_bound_settlement_raw": "77",
                "forecast_lower_bound_settlement_raw": "63",
                "threshold_kind": "between",
                "threshold_lower_bound": "73",
                "threshold_upper_bound": "74",
                "yes_possible_overlap": "0",
                "yes_possible_gap": "1.0",
                "primary_signal_margin": "-1.0",
                "forecast_feasibility_margin": "4.0",
                "forecast_range_width": "14.0",
                "observed_distance_to_lower_bound": "2.0",
                "observed_distance_to_upper_bound": "-1.0",
                "cross_market_family_score": "1.35",
                "cross_market_family_zscore": "1.6",
                "cross_market_family_candidate_rank": "2",
                "cross_market_family_bucket_size": "12",
                "cross_market_family_signal": "relative_outlier_high",
                "speci_recent": "1",
                "settlement_confidence_score": "0.92",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B74": {"market_ticker": "KXHIGHNY-26APR08-B74", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 1)
        intent = intents[0]
        self.assertEqual(intent.threshold_kind, "between")
        self.assertEqual(intent.threshold_lower_bound, 73.0)
        self.assertEqual(intent.threshold_upper_bound, 74.0)
        self.assertEqual(intent.yes_possible_overlap, False)
        self.assertEqual(intent.yes_possible_gap, 1.0)
        self.assertEqual(intent.primary_signal_margin, -1.0)
        self.assertEqual(intent.forecast_feasibility_margin, 4.0)
        self.assertEqual(intent.forecast_range_width, 14.0)
        self.assertEqual(intent.cross_market_family_score, 1.35)
        self.assertEqual(intent.cross_market_family_zscore, 1.6)
        self.assertEqual(intent.cross_market_family_candidate_rank, 2)
        self.assertEqual(intent.cross_market_family_bucket_size, 12)
        self.assertEqual(intent.cross_market_family_signal, "relative_outlier_high")

    def test_build_temperature_trade_intents_prioritizes_recent_speci(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "74",
                "speci_recent": "0",
                "settlement_confidence_score": "0.92",
            },
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B74",
                "market_title": "74F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "75",
                "speci_recent": "1",
                "settlement_confidence_score": "0.92",
            },
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {"market_ticker": "KXHIGHNY-26APR08-B72", "close_time": "2026-04-09T00:00:00Z"},
            "KXHIGHNY-26APR08-B74": {"market_ticker": "KXHIGHNY-26APR08-B74", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 2)
        self.assertTrue(intents[0].speci_recent)
        self.assertEqual(intents[0].market_ticker, "KXHIGHNY-26APR08-B74")

    def test_build_temperature_trade_intents_prioritizes_active_speci_shock(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "74",
                "speci_recent": "1",
                "speci_shock_active": "0",
                "speci_shock_confidence": "0.20",
                "speci_shock_weight": "0.01",
                "settlement_confidence_score": "0.92",
            },
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B74",
                "market_title": "74F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "75",
                "speci_recent": "0",
                "speci_shock_active": "1",
                "speci_shock_confidence": "0.91",
                "speci_shock_weight": "0.84",
                "speci_shock_mode": "operational",
                "speci_shock_trigger_count": "3",
                "speci_shock_trigger_families": "explicit_speci,temperature_jump,cadence_discontinuity",
                "speci_shock_persistence_ok": "1",
                "speci_shock_cooldown_blocked": "0",
                "speci_shock_improvement_hold_active": "0",
                "speci_shock_delta_temp_c": "2.3",
                "speci_shock_delta_minutes": "8.0",
                "speci_shock_decay_tau_minutes": "45.0",
                "settlement_confidence_score": "0.92",
            },
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {"market_ticker": "KXHIGHNY-26APR08-B72", "close_time": "2026-04-09T00:00:00Z"},
            "KXHIGHNY-26APR08-B74": {"market_ticker": "KXHIGHNY-26APR08-B74", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 2)
        self.assertEqual(intents[0].market_ticker, "KXHIGHNY-26APR08-B74")
        self.assertTrue(intents[0].speci_shock_active)
        self.assertGreater(float(intents[0].speci_shock_confidence or 0.0), 0.8)
        self.assertGreater(float(intents[0].speci_shock_weight or 0.0), 0.5)

    def test_build_temperature_trade_intents_uses_consensus_fusion_fields(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "74",
                "speci_recent": "0",
                "speci_shock_active": "0",
                "settlement_confidence_score": "0.91",
            },
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B74",
                "market_title": "74F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_metric_settlement_quantized": "75",
                "speci_recent": "0",
                "speci_shock_active": "0",
                "settlement_confidence_score": "0.91",
            },
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {"market_ticker": "KXHIGHNY-26APR08-B72", "close_time": "2026-04-09T00:00:00Z"},
            "KXHIGHNY-26APR08-B74": {"market_ticker": "KXHIGHNY-26APR08-B74", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            alpha_consensus_by_market_side={
                "KXHIGHNY-26APR08-B74|no": {
                    "profile_support_count": 3,
                    "profile_support_ratio": 1.0,
                    "weighted_support_score": 2.6,
                    "weighted_support_ratio": 0.95,
                    "consensus_alpha_score": 1.4,
                    "consensus_rank": 1,
                    "profile_names": "strict_baseline,relaxed_interval,relaxed_age",
                }
            },
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 2)
        self.assertEqual(intents[0].market_ticker, "KXHIGHNY-26APR08-B74")
        self.assertEqual(intents[0].consensus_profile_support_count, 3)
        self.assertAlmostEqual(float(intents[0].consensus_alpha_score or 0.0), 1.4, places=6)
        self.assertEqual(intents[0].consensus_rank, 1)
        self.assertEqual(intents[1].consensus_profile_support_count, 0)

    def test_load_alpha_consensus_keeps_best_duplicate_candidate_per_market_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            consensus_path = base / "consensus.json"
            consensus_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "market_ticker": "KXHIGHNY-26APR08-B74",
                                "side": "no",
                                "market_side_key": "KXHIGHNY-26APR08-B74|no",
                                "profile_support_count": 3,
                                "profile_support_ratio": 1.0,
                                "weighted_support_score": 2.6,
                                "weighted_support_ratio": 0.95,
                                "consensus_alpha_score": 1.4,
                                "consensus_rank": 1,
                                "profile_names": ["strict_baseline", "relaxed_interval"],
                            },
                            {
                                "market_ticker": "KXHIGHNY-26APR08-B74",
                                "side": "no",
                                "market_side_key": "KXHIGHNY-26APR08-B74|no",
                                "profile_support_count": 1,
                                "profile_support_ratio": 0.33,
                                "weighted_support_score": 0.2,
                                "weighted_support_ratio": 0.1,
                                "consensus_alpha_score": 0.05,
                                "consensus_rank": 9,
                                "profile_names": ["weak_fallback"],
                            },
                            {
                                "market_ticker": "KXHIGHMIA-26APR08-B77",
                                "side": "no",
                                "market_side_key": "KXHIGHMIA-26APR08-B77|no",
                                "profile_support_count": 2,
                                "profile_support_ratio": 0.67,
                                "weighted_support_score": 1.2,
                                "weighted_support_ratio": 0.55,
                                "consensus_alpha_score": 0.5,
                                "consensus_rank": 2,
                                "profile_names": ["balanced"],
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            by_market_side, meta = _load_alpha_consensus(
                output_dir=str(base),
                alpha_consensus_json=str(consensus_path),
            )

            self.assertTrue(meta["loaded"])
            self.assertEqual(meta["candidate_count"], 3)
            self.assertEqual(meta["usable_candidate_count"], 2)
            ny_entry = by_market_side["KXHIGHNY-26APR08-B74|no"]
            self.assertEqual(int(ny_entry["consensus_rank"]), 1)
            self.assertAlmostEqual(float(ny_entry["consensus_alpha_score"] or 0.0), 1.4, places=6)
            self.assertEqual(int(ny_entry["profile_support_count"]), 3)
            self.assertIn("strict_baseline", str(ny_entry["profile_names"]))

    def test_build_temperature_trade_intents_derives_interval_no_edge(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "no_signal",
                "constraint_reason": "At-most threshold still feasible.",
                "threshold_kind": "at_most",
                "threshold_lower_bound": "",
                "threshold_upper_bound": "72",
                "possible_final_lower_bound": "74",
                "possible_final_upper_bound": "79",
                "yes_interval_lower_bound": "-inf",
                "yes_interval_upper_bound": "72",
                "yes_possible_overlap": "0",
                "yes_possible_gap": "2.0",
                "primary_signal_margin": "-2.0",
                "settlement_confidence_score": "0.92",
                "snapshot_status": "ready",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {"market_ticker": "KXHIGHNY-26APR08-B72", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].constraint_status, "no_interval_infeasible")
        self.assertEqual(intents[0].side, "no")

    def test_build_temperature_trade_intents_derives_interval_no_edge_on_collapsed_bounds(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHAUS",
                "event_ticker": "KXHIGHAUS-26APR08",
                "market_ticker": "KXHIGHAUS-26APR08-B91.5",
                "market_title": "Between 91 and 92",
                "settlement_station": "KAUS",
                "settlement_timezone": "America/Chicago",
                "target_date_local": "2026-04-08",
                "constraint_status": "no_signal",
                "constraint_reason": "Between range still feasible.",
                "threshold_kind": "between",
                "threshold_lower_bound": "91",
                "threshold_upper_bound": "92",
                "observed_metric_settlement_quantized": "89",
                "possible_final_lower_bound": "89",
                "possible_final_upper_bound": "89",
                "yes_interval_lower_bound": "91",
                "yes_interval_upper_bound": "92",
                "yes_possible_overlap": "0",
                "yes_possible_gap": "2.0",
                "forecast_feasibility_margin": "-15.0",
                "settlement_confidence_score": "0.92",
                "snapshot_status": "ready",
            }
        ]
        specs_by_ticker = {
            "KXHIGHAUS-26APR08-B91.5": {
                "market_ticker": "KXHIGHAUS-26APR08-B91.5",
                "close_time": "2026-04-09T00:00:00Z",
            },
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KAUS": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 31.0,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].constraint_status, "no_interval_infeasible")
        self.assertEqual(intents[0].side, "no")

    def test_build_temperature_trade_intents_derives_monotonic_chain(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-T70",
                "market_title": "70F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "threshold_kind": "at_least",
                "threshold_lower_bound": "70",
                "threshold_upper_bound": "",
                "settlement_confidence_score": "0.92",
                "snapshot_status": "ready",
            },
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-T72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "no_signal",
                "constraint_reason": "At-least threshold not reached yet.",
                "threshold_kind": "at_least",
                "threshold_lower_bound": "72",
                "threshold_upper_bound": "",
                "settlement_confidence_score": "0.92",
                "snapshot_status": "ready",
            },
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-T70": {"market_ticker": "KXHIGHNY-26APR08-T70", "close_time": "2026-04-09T00:00:00Z"},
            "KXHIGHNY-26APR08-T72": {"market_ticker": "KXHIGHNY-26APR08-T72", "close_time": "2026-04-09T00:00:00Z"},
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        market_status = {intent.market_ticker: intent.constraint_status for intent in intents}
        self.assertEqual(market_status.get("KXHIGHNY-26APR08-T70"), "yes_impossible")
        self.assertEqual(market_status.get("KXHIGHNY-26APR08-T72"), "no_monotonic_chain")

    def test_build_temperature_trade_intents_does_not_derive_interval_certain_on_incoherent_forecast(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHAUS",
                "event_ticker": "KXHIGHAUS-26APR08",
                "market_ticker": "KXHIGHAUS-26APR08-B89.5",
                "market_title": "Between 89 and 90",
                "settlement_station": "KAUS",
                "settlement_timezone": "America/Chicago",
                "target_date_local": "2026-04-08",
                "constraint_status": "no_signal",
                "constraint_reason": "Between range currently satisfied but can still break later.",
                "threshold_kind": "between",
                "threshold_lower_bound": "89",
                "threshold_upper_bound": "90",
                "possible_final_lower_bound": "89",
                "possible_final_upper_bound": "89",
                "yes_interval_lower_bound": "89",
                "yes_interval_upper_bound": "90",
                "yes_possible_overlap": "1",
                "yes_possible_gap": "0.0",
                "forecast_feasibility_margin": "-13.0",
                "settlement_confidence_score": "0.92",
                "snapshot_status": "ready",
            }
        ]
        specs_by_ticker = {
            "KXHIGHAUS-26APR08-B89.5": {
                "market_ticker": "KXHIGHAUS-26APR08-B89.5",
                "close_time": "2026-04-09T00:00:00Z",
            },
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KAUS": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 31.0,
                }
            },
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(intents, [])

    def test_build_temperature_trade_intents_and_gate_approve(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_max_settlement_quantized": "74",
                "settlement_confidence_score": "0.92",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        market_sequences = {"KXHIGHNY-26APR08-B72": 41}

        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences=market_sequences,
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 1)
        intent = intents[0]
        self.assertEqual(intent.side, "no")
        self.assertEqual(intent.market_snapshot_seq, 41)
        self.assertEqual(intent.metar_snapshot_sha, "abc123def456")
        self.assertEqual(intent.max_entry_price_dollars, 0.96)

        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.7,
            max_metar_age_minutes=20.0,
            max_intents_per_underlying=1,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=intents)
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "approved")

    def test_policy_gate_blocks_stale_metar(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_likely_locked",
                "constraint_reason": "Observed max already meets floor",
                "observed_max_settlement_quantized": "72",
                "settlement_confidence_score": "0.95",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T09:30:00Z",
                    "temp_c": 22.0,
                }
            },
        }

        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.9,
            no_max_entry_price_dollars=0.9,
            now=now,
        )
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=intents)
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "metar_observation_stale")
        self.assertIn("metar_observation_stale", decisions[0].decision_notes)

    def test_policy_gate_station_metar_age_override(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-station-override",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHCHI|KMDW|2026-04-08",
            series_ticker="KXHIGHCHI",
            event_ticker="KXHIGHCHI-26APR08",
            market_ticker="KXHIGHCHI-26APR08-B72",
            market_title="72F or above",
            settlement_station="KMDW",
            settlement_timezone="America/Chicago",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max above bracket",
            side="no",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=23.0,
            market_snapshot_seq=10,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            station_max_metar_age_minutes={"KMDW": 25.0},
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertEqual(decision.decision_reason, "approved")
        self.assertAlmostEqual(float(decision.metar_max_age_minutes_applied or 0.0), 25.0, places=6)

    def test_policy_gate_station_hour_override_precedence(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-station-hour-override",
            captured_at="2026-04-08T05:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHCHI|KMDW|2026-04-08",
            series_ticker="KXHIGHCHI",
            event_ticker="KXHIGHCHI-26APR08",
            market_ticker="KXHIGHCHI-26APR08-B72",
            market_title="72F or above",
            settlement_station="KMDW",
            settlement_timezone="America/Chicago",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max above bracket",
            side="no",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T04:30:00+00:00",
            metar_observation_age_minutes=26.0,
            market_snapshot_seq=10,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            station_max_metar_age_minutes={"KMDW": 22.5},
            station_local_hour_max_metar_age_minutes={"KMDW": {23: 30.0}},
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertEqual(decision.decision_reason, "approved")
        self.assertEqual(decision.metar_local_hour, 23)
        self.assertAlmostEqual(float(decision.metar_max_age_minutes_applied or 0.0), 30.0, places=6)

    def test_policy_gate_prioritizes_settlement_block_over_stale_when_multiple_blockers(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-priority-settlement-over-stale",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHCHI|KMDW|2026-04-08",
            series_ticker="KXHIGHCHI",
            event_ticker="KXHIGHCHI-26APR08",
            market_ticker="KXHIGHCHI-26APR08-B72",
            market_title="72F or above",
            settlement_station="KMDW",
            settlement_timezone="America/Chicago",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max above bracket",
            side="no",
            max_entry_price_dollars=0.45,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T10:37:00+00:00",
            metar_observation_age_minutes=83.0,
            market_snapshot_seq=10,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_probability_confidence=0.5,
            min_expected_edge_net=0.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(
            intents=[intent],
            settlement_state_by_underlying={
                intent.underlying_key: {
                    "state": "pending_final_report",
                    "finalization_status": "post_close_unfinalized",
                    "allow_new_orders": False,
                    "reason": "target_date_elapsed_waiting_finalization",
                }
            },
        )[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "settlement_finalization_blocked")
        self.assertIn("settlement_finalization_blocked", decision.decision_notes)
        self.assertIn("metar_observation_stale", decision.decision_notes)

    def test_policy_gate_blocks_near_stale_boundary_when_quality_margin_not_met(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-near-stale-quality",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max above bracket",
            side="no",
            max_entry_price_dollars=0.45,
            intended_contracts=1,
            settlement_confidence_score=0.52,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:41:00+00:00",
            metar_observation_age_minutes=19.0,
            market_snapshot_seq=10,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.5,
            min_expected_edge_net=0.0,
            metar_freshness_quality_boundary_ratio=0.9,
            metar_freshness_quality_probability_margin=0.12,
            metar_freshness_quality_expected_edge_margin=0.0,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "metar_freshness_boundary_quality_insufficient")
        self.assertIn("metar_boundary_failures=probability_confidence", decision.decision_notes)

    def test_policy_gate_blocks_when_metar_ingest_quality_below_thresholds(self) -> None:
        intent = self._make_intent(
            intent_id="intent-ingest-quality-blocked",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            metar_ingest_quality_gate_enabled=True,
            metar_ingest_min_quality_score=0.80,
            metar_ingest_min_fresh_station_coverage_ratio=0.75,
            metar_ingest_require_ready_status=True,
            metar_ingest_quality_score=0.42,
            metar_ingest_quality_grade="degraded",
            metar_ingest_quality_status="degraded",
            metar_ingest_quality_signal_count=2,
            metar_ingest_quality_signals=["fresh_station_coverage_low", "parse_error_rate_elevated"],
            metar_ingest_fresh_station_coverage_ratio=0.38,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "metar_ingest_quality_insufficient")
        self.assertIn("metar_ingest_quality_gate_failures=", decision.decision_notes)
        self.assertIn("quality_score_below_min", decision.decision_notes)
        self.assertIn("fresh_station_coverage_ratio_below_min", decision.decision_notes)

    def test_policy_gate_allows_when_metar_ingest_quality_meets_thresholds(self) -> None:
        intent = self._make_intent(
            intent_id="intent-ingest-quality-approved",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            metar_ingest_quality_gate_enabled=True,
            metar_ingest_min_quality_score=0.70,
            metar_ingest_min_fresh_station_coverage_ratio=0.55,
            metar_ingest_require_ready_status=True,
            metar_ingest_quality_score=0.96,
            metar_ingest_quality_grade="excellent",
            metar_ingest_quality_status="ready",
            metar_ingest_quality_signal_count=0,
            metar_ingest_quality_signals=[],
            metar_ingest_fresh_station_coverage_ratio=0.91,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertEqual(decision.decision_reason, "approved")

    def test_policy_gate_blocks_when_taf_station_missing(self) -> None:
        intent = self._make_intent(
            intent_id="intent-taf-station-missing",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
            forecast_model_status="ready",
            taf_status="missing_station",
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            enforce_probability_edge_thresholds=False,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "taf_station_missing")
        self.assertIn("taf_station_missing=true", decision.decision_notes)

    def test_policy_gate_high_price_edge_guard_blocks_negative_edge_when_enabled(self) -> None:
        intent = self._make_intent(
            intent_id="intent-high-price-guard",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
            side="yes",
            max_entry_price_dollars=0.95,
            settlement_confidence_score=0.50,
        )
        baseline = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            high_price_edge_guard_enabled=False,
        ).evaluate(intents=[intent])[0]
        self.assertTrue(baseline.approved)

        hardened = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            high_price_edge_guard_enabled=True,
            high_price_edge_guard_min_entry_price_dollars=0.85,
            high_price_edge_guard_min_expected_edge_net=0.0,
            high_price_edge_guard_min_edge_to_risk_ratio=0.02,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(hardened.approved)
        self.assertEqual(hardened.decision_reason, "high_price_expected_edge_nonpositive")
        self.assertIn("high_price_expected_edge_nonpositive", hardened.decision_notes)
        self.assertIn("high_price_edge_to_risk_ratio_below_min", hardened.decision_notes)

    def test_portfolio_planner_limits_underlying_and_preserves_breadth(self) -> None:
        intents = [
            self._make_intent(
                intent_id="a1",
                market_ticker="KXHIGHNY-26APR08-T70",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
            ),
            self._make_intent(
                intent_id="a2",
                market_ticker="KXHIGHNY-26APR08-T72",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
            ),
            self._make_intent(
                intent_id="b1",
                market_ticker="KXHIGHDAL-26APR08-T80",
                underlying_key="KXHIGHDAL|KDAL|2026-04-08",
                settlement_station="KDAL",
            ),
            self._make_intent(
                intent_id="c1",
                market_ticker="KXHIGHMIA-26APR08-T82",
                underlying_key="KXHIGHMIA|KMIA|2026-04-08",
                settlement_station="KMIA",
            ),
        ]
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=1.0,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=0.34,
        )
        selected, allocation = planner.select_intents(
            intents=intents,
            decisions_by_id={},
            max_orders=4,
            planning_bankroll_dollars=100.0,
            daily_risk_cap_dollars=3.0,
        )
        selected_tickers = [intent.market_ticker for intent in selected]
        self.assertEqual(len(selected), 3)
        self.assertIn("KXHIGHNY-26APR08-T70", selected_tickers)
        self.assertNotIn("KXHIGHNY-26APR08-T72", selected_tickers)
        self.assertIn("KXHIGHDAL-26APR08-T80", selected_tickers)
        self.assertIn("KXHIGHMIA-26APR08-T82", selected_tickers)
        reason_counts = allocation.get("reason_counts", {})
        self.assertTrue(
            (
                "underlying_exposure_cap_reached" in reason_counts
                or "total_budget_exceeded" in reason_counts
            )
        )

    def test_portfolio_planner_limits_station_cluster(self) -> None:
        intents = [
            self._make_intent(
                intent_id="n1",
                market_ticker="KXHIGHNY-26APR08-T70",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
            ),
            self._make_intent(
                intent_id="n2",
                market_ticker="KXHIGHNY-26APR08-T72",
                underlying_key="KXHIGHNY|KNYC|2026-04-09",
                settlement_station="KNYC",
            ),
            self._make_intent(
                intent_id="d1",
                market_ticker="KXHIGHDAL-26APR08-T80",
                underlying_key="KXHIGHDAL|KDAL|2026-04-08",
                settlement_station="KDAL",
            ),
        ]
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=0.5,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=1.0,
        )
        selected, allocation = planner.select_intents(
            intents=intents,
            decisions_by_id={},
            max_orders=3,
            planning_bankroll_dollars=100.0,
            daily_risk_cap_dollars=3.0,
        )
        selected_tickers = [intent.market_ticker for intent in selected]
        self.assertEqual(len(selected), 2)
        self.assertIn("KXHIGHNY-26APR08-T70", selected_tickers)
        self.assertNotIn("KXHIGHNY-26APR08-T72", selected_tickers)
        self.assertIn("KXHIGHDAL-26APR08-T80", selected_tickers)
        self.assertIn("station_exposure_cap_reached", allocation.get("reason_counts", {}))

    def test_portfolio_planner_prefers_fresher_candidate_when_quality_equal(self) -> None:
        near_stale = self._make_intent(
            intent_id="near-stale",
            market_ticker="KXHIGHNY-26APR08-A70",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
            max_entry_price_dollars=0.45,
            metar_observation_age_minutes=19.3,
            settlement_confidence_score=0.95,
        )
        fresh = self._make_intent(
            intent_id="fresh",
            market_ticker="KXHIGHNY-26APR08-Z70",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            settlement_station="KNYC",
            max_entry_price_dollars=0.45,
            metar_observation_age_minutes=3.2,
            settlement_confidence_score=0.95,
        )
        intents = [near_stale, fresh]
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_probability_confidence=0.5,
            min_expected_edge_net=0.0,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=intents)
        decisions_by_id = {decision.intent_id: decision for decision in decisions if decision.approved}
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=1.0,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=1.0,
        )
        selected, allocation = planner.select_intents(
            intents=[intent for intent in intents if intent.intent_id in decisions_by_id],
            decisions_by_id=decisions_by_id,
            max_orders=1,
            planning_bankroll_dollars=1000.0,
            daily_risk_cap_dollars=100.0,
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].intent_id, "fresh")
        scored = allocation.get("top_candidate_scores", [])
        near_stale_row = next((row for row in scored if row.get("intent_id") == "near-stale"), None)
        self.assertIsNotNone(near_stale_row)
        self.assertGreater(
            float((near_stale_row or {}).get("score_breakdown", {}).get("freshness_boundary_penalty") or 0.0),
            0.0,
        )

    def test_portfolio_planner_enforces_pending_breadth_quota(self) -> None:
        intents = [
            self._make_intent(
                intent_id="n1",
                market_ticker="KXHIGHNY-26APR08-T70",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
                max_entry_price_dollars=0.35,
            ),
            self._make_intent(
                intent_id="n2",
                market_ticker="KXHIGHNY-26APR08-T72",
                underlying_key="KXHIGHNY|KNYC|2026-04-09",
                settlement_station="KNYC",
                max_entry_price_dollars=0.36,
            ),
            self._make_intent(
                intent_id="n3",
                market_ticker="KXHIGHNY-26APR08-T74",
                underlying_key="KXHIGHNY|KNYC|2026-04-10",
                settlement_station="KNYC",
                max_entry_price_dollars=0.37,
            ),
            self._make_intent(
                intent_id="d1",
                market_ticker="KXHIGHDAL-26APR08-T80",
                underlying_key="KXHIGHDAL|KDAL|2026-04-08",
                settlement_station="KDAL",
                max_entry_price_dollars=0.35,
            ),
        ]
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=1.0,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=1.0,
            max_orders_per_station=4,
            max_orders_per_underlying=4,
            min_unique_stations_per_loop=2,
            min_unique_underlyings_per_loop=2,
            min_unique_local_hours_per_loop=0,
        )
        selected, allocation = planner.select_intents(
            intents=intents,
            decisions_by_id={},
            max_orders=2,
            planning_bankroll_dollars=200.0,
            daily_risk_cap_dollars=50.0,
        )
        selected_stations = {str(intent.settlement_station or "").upper() for intent in selected}
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected_stations, {"KNYC", "KDAL"})
        self.assertEqual(int(allocation.get("selected_unique_station_count") or 0), 2)

    def test_portfolio_planner_deprioritizes_global_profitability_guardrail(self) -> None:
        weak_intent = self._make_intent(
            intent_id="weak-global",
            market_ticker="KXHIGHAUS-26APR08-A70",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            max_entry_price_dollars=0.45,
            settlement_confidence_score=0.95,
        )
        strong_intent = self._make_intent(
            intent_id="strong-global",
            market_ticker="KXHIGHDAL-26APR08-Z70",
            underlying_key="KXHIGHDAL|KDAL|2026-04-08",
            settlement_station="KDAL",
            settlement_timezone="America/Chicago",
            max_entry_price_dollars=0.45,
            settlement_confidence_score=0.95,
        )
        weak_decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_probability_confidence=0.5,
            min_expected_edge_net=-1.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "insufficient_resolved_market_sides",
                "resolved_unique_market_sides": 1,
                "repeated_entry_multiplier": 48.0,
                "evidence_confidence": 0.08,
                "global": {
                    "calibration_ratio": 0.12,
                    "concentration_warning": True,
                },
                "bucket_profiles": {
                    "station": {},
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[weak_intent])[0]
        strong_decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_probability_confidence=0.5,
            min_expected_edge_net=-1.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "resolved_unique_market_sides": 34,
                "repeated_entry_multiplier": 4.0,
                "evidence_confidence": 0.95,
                "global": {
                    "calibration_ratio": 0.94,
                    "concentration_warning": False,
                },
                "bucket_profiles": {
                    "station": {},
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[strong_intent])[0]
        self.assertTrue(weak_decision.approved)
        self.assertTrue(strong_decision.approved)
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=1.0,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=1.0,
        )
        selected, allocation = planner.select_intents(
            intents=[weak_intent, strong_intent],
            decisions_by_id={
                weak_decision.intent_id: weak_decision,
                strong_decision.intent_id: strong_decision,
            },
            max_orders=1,
            planning_bankroll_dollars=1000.0,
            daily_risk_cap_dollars=100.0,
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].intent_id, "strong-global")
        scored = allocation.get("top_candidate_scores", [])
        weak_row = next((row for row in scored if row.get("intent_id") == "weak-global"), None)
        strong_row = next((row for row in scored if row.get("intent_id") == "strong-global"), None)
        self.assertIsNotNone(weak_row)
        self.assertIsNotNone(strong_row)
        self.assertGreater(
            float((weak_row or {}).get("score_breakdown", {}).get("historical_profitability_guardrail_penalty_term") or 0.0),
            0.0,
        )
        self.assertAlmostEqual(
            float(
                (strong_row or {}).get("score_breakdown", {}).get(
                    "historical_profitability_guardrail_penalty_term"
                )
                or 0.0
            ),
            0.0,
            places=6,
        )

    def test_portfolio_planner_deprioritizes_profitability_bucket_guardrail(self) -> None:
        penalized_bucket_intent = self._make_intent(
            intent_id="bucket-penalized",
            market_ticker="KXHIGHAUS-26APR08-A71",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            max_entry_price_dollars=0.45,
            settlement_confidence_score=0.95,
        )
        neutral_bucket_intent = self._make_intent(
            intent_id="bucket-neutral",
            market_ticker="KXHIGHDAL-26APR08-Z71",
            underlying_key="KXHIGHDAL|KDAL|2026-04-08",
            settlement_station="KDAL",
            settlement_timezone="America/Chicago",
            max_entry_price_dollars=0.45,
            settlement_confidence_score=0.95,
        )
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_probability_confidence=0.5,
            min_expected_edge_net=-1.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "resolved_unique_market_sides": 34,
                "repeated_entry_multiplier": 4.0,
                "evidence_confidence": 0.95,
                "global": {
                    "calibration_ratio": 0.94,
                    "concentration_warning": False,
                },
                "bucket_profiles": {
                    "station": {
                        "KAUS": {
                            "penalty_ratio": 1.0,
                            "boost_ratio": 0.0,
                            "samples": 24,
                            "source_labels": ["profitability_gap"],
                        }
                    },
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[penalized_bucket_intent, neutral_bucket_intent])
        decisions_by_id = {decision.intent_id: decision for decision in decisions if decision.approved}
        self.assertIn("bucket-penalized", decisions_by_id)
        self.assertIn("bucket-neutral", decisions_by_id)
        planner = TemperaturePortfolioPlanner(
            max_total_deployed_pct=1.0,
            max_same_station_exposure_pct=1.0,
            max_same_hour_cluster_exposure_pct=1.0,
            max_same_underlying_exposure_pct=1.0,
        )
        selected, allocation = planner.select_intents(
            intents=[penalized_bucket_intent, neutral_bucket_intent],
            decisions_by_id=decisions_by_id,
            max_orders=1,
            planning_bankroll_dollars=1000.0,
            daily_risk_cap_dollars=100.0,
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].intent_id, "bucket-neutral")
        scored = allocation.get("top_candidate_scores", [])
        penalized_row = next((row for row in scored if row.get("intent_id") == "bucket-penalized"), None)
        neutral_row = next((row for row in scored if row.get("intent_id") == "bucket-neutral"), None)
        self.assertIsNotNone(penalized_row)
        self.assertIsNotNone(neutral_row)
        self.assertGreater(
            float(
                (penalized_row or {}).get("score_breakdown", {}).get(
                    "historical_profitability_bucket_guardrail_penalty_term"
                )
                or 0.0
            ),
            0.0,
        )
        self.assertAlmostEqual(
            float(
                (neutral_row or {}).get("score_breakdown", {}).get(
                    "historical_profitability_bucket_guardrail_penalty_term"
                )
                or 0.0
            ),
            0.0,
            places=6,
        )

    def test_policy_gate_blocks_no_side_when_yes_interval_still_overlaps(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-no-overlap",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max above bracket",
            side="no",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "no_side_interval_overlap_still_possible")

    def test_policy_gate_blocks_yes_side_when_interval_infeasible(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-yes-infeasible",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=False,
            yes_possible_gap=2.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "yes_side_interval_infeasible")

    def test_policy_gate_blocks_alpha_strength_below_min(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-low-alpha",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=3.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=0.0,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "alpha_strength_below_min")

    def test_policy_gate_blocks_probability_confidence_below_min(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-low-probability-confidence",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.55,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.8,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "probability_confidence_below_min")
        self.assertIn("probability_confidence=", decision.decision_notes)

    def test_policy_gate_blocks_expected_edge_below_min(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-low-expected-edge",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.55,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.07,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        self.assertIn("expected_edge_net=", decision.decision_notes)

    def test_policy_gate_historical_quality_raise_blocks_expected_edge(self) -> None:
        intent = self._make_intent(
            intent_id="intent-historical-quality-edge-raise",
            market_ticker="KXHIGHAUS-26APR08-B75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="no",
            constraint_status="yes_impossible",
            settlement_confidence_score=0.9,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "global_penalty_ratio": 1.0,
                "global_boost_ratio": 0.0,
                "bucket_profiles": {
                    "station": {
                        "KAUS": {
                            "penalty_ratio": 1.0,
                            "boost_ratio": 0.0,
                            "samples": 21,
                        }
                    },
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
            historical_quality_expected_edge_penalty_max=1.0,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        # Evidence-quality scaling now caps historical penalty raises when profile
        # support/freshness is not perfect; assert positive but bounded pressure.
        self.assertGreater(float(decision.historical_quality_expected_edge_raise or 0.0), 0.1)
        self.assertLess(float(decision.historical_quality_expected_edge_raise or 0.0), 0.5)
        self.assertGreaterEqual(
            float(decision.min_expected_edge_net_required or 0.0),
            float(decision.historical_quality_expected_edge_raise or 0.0),
        )
        self.assertIn("historical_quality_expected_edge_raise=", decision.decision_notes)

    def test_policy_gate_blocks_when_global_only_pressure_is_extreme(self) -> None:
        intent = self._make_intent(
            intent_id="intent-historical-quality-global-only-pressure",
            market_ticker="KXHIGHAUS-26APR08-B75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="no",
            constraint_status="yes_impossible",
            settlement_confidence_score=0.9,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=-1.0,
            enforce_probability_edge_thresholds=True,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "insufficient_resolved_market_sides",
                "resolved_unique_market_sides": 0,
                "repeated_entry_multiplier": 42.0,
                "evidence_confidence": 0.1,
                "global_adjustment_profile": {
                    "target_share": 0.1,
                    "global_only_adjusted_share": 0.85,
                    "rows_adjusted_global_only": 850,
                    "rows_adjusted": 1000,
                    "pressure_active": True,
                },
                "bucket_profiles": {
                    "station": {},
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "historical_quality_global_only_pressure")
        self.assertTrue(bool(decision.historical_quality_global_only_pressure_active))
        self.assertGreater(float(decision.historical_quality_global_only_excess_ratio or 0.0), 0.75)
        self.assertIn("historical_quality_global_only_pressure_triggered=true", decision.decision_notes)

    def test_policy_gate_historical_profitability_guardrail_blocks_expected_edge(self) -> None:
        intent = self._make_intent(
            intent_id="intent-profitability-guardrail-edge-block",
            market_ticker="KXHIGHAUS-26APR08-T75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="yes",
            max_entry_price_dollars=0.90,
            constraint_status="yes_likely_locked",
            settlement_confidence_score=0.9,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "insufficient_resolved_market_sides",
                "resolved_unique_market_sides": 2,
                "repeated_entry_multiplier": 34.0,
                "evidence_confidence": 0.10,
                "global": {
                    "calibration_ratio": 0.10,
                    "concentration_warning": True,
                },
                "bucket_profiles": {
                    "station": {},
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        self.assertGreater(float(decision.historical_profitability_guardrail_penalty_ratio or 0.0), 0.0)
        self.assertGreater(float(decision.historical_profitability_guardrail_expected_edge_raise or 0.0), 0.0)
        self.assertIn("historical_profitability_guardrail_status=", decision.decision_notes)
        self.assertIn("historical_profitability_expected_edge_below_min", decision.decision_notes)

    def test_policy_gate_historical_profitability_guardrail_neutral_for_strong_profile(self) -> None:
        intent = self._make_intent(
            intent_id="intent-profitability-guardrail-neutral",
            market_ticker="KXHIGHAUS-26APR08-T75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="yes",
            max_entry_price_dollars=0.90,
            constraint_status="yes_likely_locked",
            settlement_confidence_score=0.9,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "resolved_unique_market_sides": 28,
                "repeated_entry_multiplier": 4.0,
                "evidence_confidence": 0.94,
                "global": {
                    "calibration_ratio": 0.92,
                    "concentration_warning": False,
                },
                "bucket_profiles": {
                    "station": {},
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertAlmostEqual(
            float(decision.historical_profitability_guardrail_penalty_ratio or 0.0),
            0.0,
            places=6,
        )
        self.assertAlmostEqual(
            float(decision.historical_profitability_guardrail_expected_edge_raise or 0.0),
            0.0,
            places=6,
        )

    def test_policy_gate_historical_profitability_bucket_guardrail_blocks_station_bucket(self) -> None:
        intent = self._make_intent(
            intent_id="intent-profitability-bucket-guardrail-block",
            market_ticker="KXHIGHAUS-26APR08-T75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="yes",
            max_entry_price_dollars=0.90,
            constraint_status="yes_likely_locked",
            settlement_confidence_score=0.9,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "resolved_unique_market_sides": 30,
                "repeated_entry_multiplier": 6.0,
                "evidence_confidence": 0.9,
                "global": {
                    "calibration_ratio": 0.95,
                    "concentration_warning": False,
                },
                "bucket_profiles": {
                    "station": {
                        "KAUS": {
                            "penalty_ratio": 1.0,
                            "boost_ratio": 0.0,
                            "samples": 24,
                            "source_labels": ["profitability_gap"],
                        }
                    },
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        self.assertGreater(
            float(decision.historical_profitability_bucket_guardrail_penalty_ratio or 0.0),
            0.0,
        )
        self.assertGreater(
            float(decision.historical_profitability_bucket_guardrail_expected_edge_raise or 0.0),
            0.0,
        )
        self.assertIn("historical_profitability_bucket_guardrail_status=", decision.decision_notes)
        self.assertIn("historical_profitability_expected_edge_below_min", decision.decision_notes)

    def test_policy_gate_historical_profitability_bucket_guardrail_ignores_non_profitability_labels(self) -> None:
        intent = self._make_intent(
            intent_id="intent-profitability-bucket-guardrail-neutral",
            market_ticker="KXHIGHAUS-26APR08-T75",
            underlying_key="KXHIGHAUS|KAUS|2026-04-08",
            settlement_station="KAUS",
            settlement_timezone="America/Chicago",
            side="yes",
            max_entry_price_dollars=0.90,
            constraint_status="yes_likely_locked",
            settlement_confidence_score=0.95,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
            historical_selection_quality_profile={
                "enabled": True,
                "status": "ready",
                "resolved_unique_market_sides": 30,
                "repeated_entry_multiplier": 6.0,
                "evidence_confidence": 0.9,
                "global": {
                    "calibration_ratio": 0.95,
                    "concentration_warning": False,
                },
                "bucket_profiles": {
                    "station": {
                        "KAUS": {
                            "penalty_ratio": 1.0,
                            "boost_ratio": 0.0,
                            "samples": 24,
                            "source_labels": ["historical_quality"],
                        }
                    },
                    "local_hour": {},
                    "signal_type": {},
                    "side": {},
                },
            },
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertAlmostEqual(
            float(decision.historical_profitability_bucket_guardrail_penalty_ratio or 0.0),
            0.0,
            places=6,
        )

    def test_policy_gate_blocks_edge_to_risk_ratio_below_min(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-low-edge-to-risk",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max already above threshold",
            side="no",
            max_entry_price_dollars=0.75,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=False,
            yes_possible_gap=1.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            min_expected_edge_net=0.0,
            min_edge_to_risk_ratio=0.5,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "edge_to_risk_ratio_below_min")
        self.assertAlmostEqual(float(decision.min_edge_to_risk_ratio_required or 0.0), 0.5, places=6)
        self.assertIn("edge_to_risk_ratio=", decision.decision_notes)

    def test_policy_gate_applies_probability_fallback_when_min_threshold_omitted(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-probability-fallback",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.7,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=None,
            fallback_min_probability_confidence=0.99,
            enforce_probability_edge_thresholds=True,
            enforce_sparse_evidence_hardening=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "probability_confidence_below_min")
        self.assertAlmostEqual(float(decision.min_probability_confidence_required or 0.0), 0.99, places=6)

    def test_policy_gate_applies_probability_fallback_when_min_threshold_is_zero(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-probability-fallback-zero",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max already above threshold",
            side="no",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.65,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=False,
            yes_possible_gap=1.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            fallback_min_probability_confidence=0.99,
            enforce_probability_edge_thresholds=True,
            enforce_sparse_evidence_hardening=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "probability_confidence_below_min")
        self.assertAlmostEqual(float(decision.min_probability_confidence_required or 0.0), 0.99, places=6)
        self.assertIn("min_probability_confidence_defaulted=0.990000", decision.decision_notes)

    def test_policy_gate_applies_expected_edge_fallback_when_min_threshold_omitted(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-edge-fallback",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.7,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            min_expected_edge_net=None,
            fallback_min_expected_edge_net=0.5,
            enforce_probability_edge_thresholds=True,
            enforce_sparse_evidence_hardening=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        self.assertGreaterEqual(float(decision.min_expected_edge_net_required or 0.0), 0.5)

    def test_policy_gate_applies_expected_edge_fallback_when_min_threshold_is_zero(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-edge-fallback-zero",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.7,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            min_expected_edge_net=0.0,
            fallback_min_expected_edge_net=0.5,
            enforce_probability_edge_thresholds=True,
            enforce_sparse_evidence_hardening=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "expected_edge_below_min")
        self.assertGreaterEqual(float(decision.min_expected_edge_net_required or 0.0), 0.5)
        self.assertIn("min_expected_edge_net_defaulted=0.500000", decision.decision_notes)

    def test_policy_gate_blocks_when_probability_breakeven_gap_below_min(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-probability-gap-min",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.90,
            intended_contracts=1,
            settlement_confidence_score=0.91,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.50,
            min_expected_edge_net=0.001,
            min_edge_to_risk_ratio=0.001,
            min_base_edge_net=0.0,
            min_probability_breakeven_gap=0.2,
            enforce_probability_edge_thresholds=True,
            enforce_sparse_evidence_hardening=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "probability_breakeven_gap_below_min")
        self.assertIn("min_probability_breakeven_gap_required=0.200000", decision.decision_notes)

    def test_policy_gate_applies_edge_to_risk_fallback_when_min_threshold_omitted(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-edge-to-risk-fallback",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max already above threshold",
            side="no",
            max_entry_price_dollars=0.75,
            intended_contracts=1,
            settlement_confidence_score=0.95,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=False,
            yes_possible_gap=1.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            min_expected_edge_net=0.0,
            min_edge_to_risk_ratio=None,
            fallback_min_edge_to_risk_ratio=0.5,
            enforce_probability_edge_thresholds=True,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "edge_to_risk_ratio_below_min")
        self.assertAlmostEqual(float(decision.min_edge_to_risk_ratio_required or 0.0), 0.5, places=6)
        self.assertIn("min_edge_to_risk_ratio_defaulted=0.500000", decision.decision_notes)

    def test_policy_gate_entry_price_floor_raises_expected_edge_requirement(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-entry-price-floor",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-B72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_impossible",
            constraint_reason="Observed max already above threshold",
            side="no",
            max_entry_price_dollars=0.99,
            intended_contracts=1,
            settlement_confidence_score=0.91,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=False,
            yes_possible_gap=1.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=0.0,
            min_expected_edge_net=0.0,
            enforce_probability_edge_thresholds=True,
            enforce_entry_price_probability_floor=True,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertIn(
            decision.decision_reason,
            {"probability_confidence_below_min", "expected_edge_below_min"},
        )
        self.assertGreater(float(decision.min_expected_edge_net_required or 0.0), 0.01)
        self.assertIn("min_expected_edge_net_raised_for_entry_price=", decision.decision_notes)

    def test_policy_gate_disable_probability_edge_enforcement_preserves_legacy_behavior(self) -> None:
        intent = TemperatureTradeIntent(
            intent_id="intent-disable-prob-edge-enforcement",
            captured_at="2026-04-08T12:00:00+00:00",
            policy_version="temperature_policy_v1",
            underlying_key="KXHIGHNY|KNYC|2026-04-08",
            series_ticker="KXHIGHNY",
            event_ticker="KXHIGHNY-26APR08",
            market_ticker="KXHIGHNY-26APR08-T72",
            market_title="72F or above",
            settlement_station="KNYC",
            settlement_timezone="America/New_York",
            target_date_local="2026-04-08",
            constraint_status="yes_likely_locked",
            constraint_reason="Observed max supports floor",
            side="yes",
            max_entry_price_dollars=0.95,
            intended_contracts=1,
            settlement_confidence_score=0.7,
            observed_max_settlement_quantized=74.0,
            close_time="2026-04-09T00:00:00Z",
            hours_to_close=12.0,
            spec_hash="abc123",
            metar_snapshot_sha="sha123",
            metar_observation_time_utc="2026-04-08T11:37:00+00:00",
            metar_observation_age_minutes=5.0,
            market_snapshot_seq=10,
            yes_possible_overlap=True,
            yes_possible_gap=0.0,
        )
        decision = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=20.0,
            min_alpha_strength=None,
            min_probability_confidence=None,
            min_expected_edge_net=None,
            enforce_probability_edge_thresholds=False,
            enforce_interval_consistency=False,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=[intent])[0]
        self.assertTrue(decision.approved)
        self.assertIsNone(decision.min_probability_confidence_required)
        self.assertIsNone(decision.min_expected_edge_net_required)

    def test_policy_gate_blocks_settlement_review_hold(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        intents = build_temperature_trade_intents(
            constraint_rows=[
                {
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "market_title": "72F or above",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "constraint_status": "yes_impossible",
                    "constraint_reason": "Observed max already above threshold",
                    "observed_max_settlement_quantized": "74",
                    "settlement_confidence_score": "0.92",
                }
            ],
            specs_by_ticker={
                "KXHIGHNY-26APR08-B72": {
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "close_time": "2026-04-09T00:00:00Z",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "rules_primary": "Highest temperature in local day at KNYC.",
                }
            },
            metar_context={
                "raw_sha256": "sha1",
                "latest_by_station": {"KNYC": {"observation_time_utc": "2026-04-08T11:50:00Z", "temp_c": 24.2}},
            },
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.95,
            now=now,
        )
        intent = intents[0]
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(
            intents=intents,
            settlement_state_by_underlying={
                intent.underlying_key: {
                    "state": "review_hold",
                    "finalization_status": "pending_review",
                    "allow_new_orders": False,
                    "reason": "Waiting for settlement-source final report.",
                }
            },
        )
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "settlement_review_hold")
        self.assertIn("settlement_review_hold", decisions[0].decision_notes)

    def test_policy_gate_blocks_pending_final_report_without_explicit_allow_new_orders(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        intents = build_temperature_trade_intents(
            constraint_rows=[
                {
                    "series_ticker": "KXHIGHNY",
                    "event_ticker": "KXHIGHNY-26APR08",
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "market_title": "72F or above",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "constraint_status": "yes_impossible",
                    "constraint_reason": "Observed max already above threshold",
                    "observed_max_settlement_quantized": "74",
                    "settlement_confidence_score": "0.92",
                }
            ],
            specs_by_ticker={
                "KXHIGHNY-26APR08-B72": {
                    "market_ticker": "KXHIGHNY-26APR08-B72",
                    "close_time": "2026-04-09T00:00:00Z",
                    "settlement_station": "KNYC",
                    "settlement_timezone": "America/New_York",
                    "target_date_local": "2026-04-08",
                    "rules_primary": "Highest temperature in local day at KNYC.",
                }
            },
            metar_context={
                "raw_sha256": "sha1",
                "latest_by_station": {"KNYC": {"observation_time_utc": "2026-04-08T11:50:00Z", "temp_c": 24.2}},
            },
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.95,
            now=now,
        )
        intent = intents[0]
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(
            intents=intents,
            settlement_state_by_underlying={
                intent.underlying_key: {
                    "state": "pending_final_report",
                    "finalization_status": "post_close_unfinalized",
                    "reason": "target_date_elapsed_waiting_finalization",
                }
            },
        )
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "settlement_finalization_blocked")
        self.assertIn("settlement_finalization_blocked", decisions[0].decision_notes)

    def test_run_kalshi_temperature_trader_builds_plan_and_calls_execute(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            specs_csv = base / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "close_time",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "rules_primary",
                        "rules_secondary",
                        "local_day_boundary",
                        "observation_window_local_start",
                        "observation_window_local_end",
                        "threshold_expression",
                        "contract_terms_url",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "close_time": "2026-04-09T00:00:00Z",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "rules_primary": "Highest temperature in local day at KNYC.",
                        "rules_secondary": "",
                        "local_day_boundary": "local_day",
                        "observation_window_local_start": "00:00",
                        "observation_window_local_end": "23:59",
                        "threshold_expression": "at_most:72",
                        "contract_terms_url": "https://example.test/terms",
                        "settlement_confidence_score": "0.92",
                    }
                )

            constraint_csv = base / "constraints.csv"
            with constraint_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "scanned_at",
                        "source_specs_csv",
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "settlement_unit",
                        "settlement_precision",
                        "threshold_expression",
                        "constraint_status",
                        "constraint_reason",
                        "observed_max_settlement_raw",
                        "observed_max_settlement_quantized",
                        "observations_for_date",
                        "snapshot_status",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "scanned_at": now.isoformat(),
                        "source_specs_csv": str(specs_csv),
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "settlement_unit": "fahrenheit",
                        "settlement_precision": "whole_degree",
                        "threshold_expression": "at_most:72",
                        "constraint_status": "yes_impossible",
                        "constraint_reason": "Observed max 74 exceeds at_most threshold 72.",
                        "observed_max_settlement_raw": "74",
                        "observed_max_settlement_quantized": "74",
                        "observations_for_date": "12",
                        "snapshot_status": "ready",
                        "settlement_confidence_score": "0.92",
                    }
                )

            metar_state = base / "metar_state.json"
            metar_state.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {
                                "observation_time_utc": "2026-04-08T11:55:00Z",
                                "temp_c": 24.5,
                            }
                        },
                        "max_temp_c_by_station_local_day": {},
                    }
                ),
                encoding="utf-8",
            )
            metar_summary = base / "metar_summary.json"
            metar_summary.write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "captured_at": now.isoformat(),
                        "raw_sha256": "feedbead1234",
                        "state_file": str(metar_state),
                    }
                ),
                encoding="utf-8",
            )

            ws_state = base / "ws_state.json"
            ws_state.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "ready",
                            "market_count": 1,
                            "desynced_market_count": 0,
                            "last_event_at": now.isoformat(),
                        },
                        "markets": {
                            "KXHIGHNY-26APR08-B72": {
                                "sequence": 22,
                                "updated_at_utc": now.isoformat(),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=str(env_file),
                output_dir=str(base),
                specs_csv=str(specs_csv),
                constraint_csv=str(constraint_csv),
                metar_summary_json=str(metar_summary),
                ws_state_json=str(ws_state),
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["intent_summary"]["intents_total"], 1)
            self.assertEqual(summary["intent_summary"]["intents_approved"], 1)
            self.assertEqual(summary["intent_summary"]["approved_probability_below_min_count"], 0)
            self.assertEqual(summary["intent_summary"]["approved_expected_edge_below_min_count"], 0)
            self.assertIn(
                "historical_selection_quality_global_only_pressure_active_count",
                summary["intent_summary"],
            )
            self.assertIn(
                "historical_selection_quality_global_only_pressure_blocked_count",
                summary["intent_summary"],
            )
            self.assertEqual(
                int(summary["intent_summary"]["historical_selection_quality_global_only_pressure_active_count"] or 0),
                0,
            )
            self.assertEqual(
                int(summary["intent_summary"]["historical_selection_quality_global_only_pressure_blocked_count"] or 0),
                0,
            )
            self.assertTrue(Path(summary["intent_summary"]["output_csv"]).exists())
            self.assertTrue(Path(summary["plan_summary"]["output_csv"]).exists())
            with Path(summary["intent_summary"]["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            first = rows[0]
            self.assertIn("policy_historical_quality_global_only_pressure_active", first)
            self.assertIn("policy_historical_quality_global_only_adjusted_share", first)
            self.assertIn("policy_historical_quality_global_only_excess_ratio", first)
            self.assertEqual(first["policy_historical_quality_global_only_pressure_active"], "False")

            plan_summary = execute_capture["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            allocation_summary = summary["plan_summary"]["allocation_summary"]
            self.assertEqual(allocation_summary["optimization_mode"], "score_aware_greedy_v1")
            self.assertGreaterEqual(int(allocation_summary["candidate_count"]), 1)
            order = plan_summary["orders"][0]
            self.assertEqual(order["side"], "no")
            self.assertEqual(order["temperature_expected_edge_model_version"], "temp_edge_v3_price_aware")
            payload = order["order_payload_preview"]
            self.assertEqual(payload["side"], "no")
            self.assertIn("order_group_id", payload)
            self.assertTrue(str(payload.get("client_order_id", "")).startswith("temp-"))

    def test_run_trader_shadow_mode_does_not_require_market_snapshot_sequence(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 1)
            self.assertFalse(intent_summary["require_market_snapshot_seq_applied"])

    def test_run_trader_live_mode_requires_market_snapshot_sequence(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                allow_live_orders=True,
                intents_only=True,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 0)
            self.assertTrue(intent_summary["require_market_snapshot_seq_applied"])
            self.assertEqual(intent_summary["policy_reason_counts"].get("missing_market_snapshot_seq"), 1)

    def test_run_trader_shadow_quote_probe_activates_on_no_candidates(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                execute_capture["allow_live_orders"] = kwargs.get("allow_live_orders")
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                min_settlement_confidence=0.99,
                shadow_quote_probe_on_no_candidates=True,
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["intent_summary"]["intents_total"], 1)
            self.assertEqual(summary["intent_summary"]["intents_approved"], 0)
            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["status"], "ready")
            self.assertTrue(plan_summary["shadow_quote_probe_requested"])
            self.assertTrue(plan_summary["shadow_quote_probe_applied"])
            self.assertEqual(plan_summary["shadow_quote_probe_reason"], "activated_no_candidates")
            self.assertEqual(plan_summary["shadow_quote_probe_source"], "all_intents")
            self.assertEqual(plan_summary["shadow_quote_probe_candidate_intents"], 1)
            self.assertEqual(plan_summary["shadow_quote_probe_planned_orders"], 1)
            self.assertEqual(plan_summary["shadow_quote_probe_market_tickers"], ["KXHIGHNY-26APR08-B72"])
            self.assertIs(execute_capture.get("allow_live_orders"), False)

            synthetic_plan_summary = execute_capture.get("plan_summary")
            self.assertIsInstance(synthetic_plan_summary, dict)
            synthetic_plan_summary = dict(synthetic_plan_summary or {})
            self.assertEqual(synthetic_plan_summary.get("planned_orders"), 1)
            orders = synthetic_plan_summary.get("orders")
            self.assertIsInstance(orders, list)
            self.assertEqual(len(orders), 1)
            first_order = dict(orders[0])
            self.assertIs(first_order.get("shadow_quote_probe"), True)
            self.assertEqual(first_order.get("shadow_quote_probe_source"), "all_intents")

    def test_run_trader_shadow_quote_probe_prioritizes_targeted_market_sides(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )
            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                execute_capture["allow_live_orders"] = kwargs.get("allow_live_orders")
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                min_settlement_confidence=0.99,
                shadow_quote_probe_on_no_candidates=True,
                shadow_quote_probe_market_side_targets=[
                    "KXHIGHNY-26APR08-B73|yes",
                    "KXHIGHNY-26APR08-B72|no",
                ],
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["status"], "ready")
            self.assertTrue(plan_summary["shadow_quote_probe_requested"])
            self.assertTrue(plan_summary["shadow_quote_probe_applied"])
            self.assertTrue(plan_summary["shadow_quote_probe_targeted_requested"])
            self.assertTrue(plan_summary["shadow_quote_probe_targeted_applied"])
            self.assertEqual(
                plan_summary["shadow_quote_probe_targeted_keys"],
                ["KXHIGHNY-26APR08-B73|yes", "KXHIGHNY-26APR08-B72|no"],
            )
            self.assertEqual(plan_summary["shadow_quote_probe_targeted_match_count"], 2)
            self.assertEqual(plan_summary["shadow_quote_probe_reason"], "activated_no_candidates")
            self.assertEqual(plan_summary["shadow_quote_probe_candidate_intents"], 2)
            self.assertEqual(plan_summary["shadow_quote_probe_planned_orders"], 2)
            self.assertEqual(
                set(plan_summary["shadow_quote_probe_market_tickers"]),
                {"KXHIGHNY-26APR08-B72", "KXHIGHNY-26APR08-B73"},
            )
            self.assertIs(execute_capture.get("allow_live_orders"), False)

            synthetic_plan_summary = execute_capture.get("plan_summary")
            self.assertIsInstance(synthetic_plan_summary, dict)
            synthetic_plan_summary = dict(synthetic_plan_summary or {})
            self.assertEqual(synthetic_plan_summary.get("planned_orders"), 2)
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_requested"), True)
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_applied"), True)
            self.assertEqual(
                synthetic_plan_summary.get("shadow_quote_probe_targeted_keys"),
                ["KXHIGHNY-26APR08-B73|yes", "KXHIGHNY-26APR08-B72|no"],
            )
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_match_count"), 2)
            orders = synthetic_plan_summary.get("orders")
            self.assertIsInstance(orders, list)
            self.assertEqual(len(orders), 2)
            first_order = dict(orders[0])
            second_order = dict(orders[1])
            self.assertEqual(first_order.get("market_ticker"), "KXHIGHNY-26APR08-B73")
            self.assertEqual(first_order.get("side"), "yes")
            self.assertEqual(second_order.get("market_ticker"), "KXHIGHNY-26APR08-B72")
            self.assertEqual(second_order.get("side"), "no")

    def test_run_trader_shadow_quote_probe_falls_back_when_targets_miss(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )
            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                execute_capture["allow_live_orders"] = kwargs.get("allow_live_orders")
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                min_settlement_confidence=0.99,
                shadow_quote_probe_on_no_candidates=True,
                shadow_quote_probe_market_side_targets=["NO_MATCH_MARKET"],
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["status"], "ready")
            self.assertTrue(plan_summary["shadow_quote_probe_requested"])
            self.assertTrue(plan_summary["shadow_quote_probe_applied"])
            self.assertTrue(plan_summary["shadow_quote_probe_targeted_requested"])
            self.assertFalse(plan_summary["shadow_quote_probe_targeted_applied"])
            self.assertEqual(plan_summary["shadow_quote_probe_targeted_keys"], ["NO_MATCH_MARKET"])
            self.assertEqual(plan_summary["shadow_quote_probe_targeted_match_count"], 0)
            self.assertEqual(plan_summary["shadow_quote_probe_reason"], "activated_no_candidates")
            self.assertEqual(plan_summary["shadow_quote_probe_candidate_intents"], 2)
            self.assertEqual(plan_summary["shadow_quote_probe_planned_orders"], 2)
            self.assertIs(execute_capture.get("allow_live_orders"), False)

            synthetic_plan_summary = execute_capture.get("plan_summary")
            self.assertIsInstance(synthetic_plan_summary, dict)
            synthetic_plan_summary = dict(synthetic_plan_summary or {})
            self.assertEqual(synthetic_plan_summary.get("planned_orders"), 2)
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_requested"), True)
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_applied"), False)
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_keys"), ["NO_MATCH_MARKET"])
            self.assertEqual(synthetic_plan_summary.get("shadow_quote_probe_targeted_match_count"), 0)
            orders = synthetic_plan_summary.get("orders")
            self.assertIsInstance(orders, list)
            self.assertEqual(len(orders), 2)
            first_order = dict(orders[0])
            self.assertEqual(first_order.get("market_ticker"), "KXHIGHNY-26APR08-B72")
            self.assertEqual(first_order.get("side"), "no")

    def test_run_trader_shadow_quote_probe_blocked_in_live_mode(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                execute_capture["allow_live_orders"] = kwargs.get("allow_live_orders")
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                allow_live_orders=True,
                min_settlement_confidence=0.99,
                shadow_quote_probe_on_no_candidates=True,
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["status"], "no_candidates")
            self.assertTrue(plan_summary["shadow_quote_probe_requested"])
            self.assertFalse(plan_summary["shadow_quote_probe_applied"])
            self.assertEqual(plan_summary["shadow_quote_probe_reason"], "blocked_live_mode")
            self.assertEqual(plan_summary["shadow_quote_probe_planned_orders"], 0)
            self.assertEqual(plan_summary["shadow_quote_probe_market_tickers"], [])
            self.assertIs(execute_capture.get("allow_live_orders"), True)

            synthetic_plan_summary = execute_capture.get("plan_summary")
            self.assertIsInstance(synthetic_plan_summary, dict)
            self.assertEqual(dict(synthetic_plan_summary or {}).get("planned_orders"), 0)

    def test_run_trader_excludes_one_of_multiple_markets_and_reports_diagnostics(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                exclude_market_tickers=["KXHIGHNY-26APR08-B73"],
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            plan_summary = summary["plan_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B73"])
            self.assertEqual(intent_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B73"])
            self.assertEqual(intent_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B73"])
            self.assertEqual(plan_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B73"])
            self.assertEqual(plan_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_ticker_count"], 1)
            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXHIGHNY-26APR08-B72")

    def test_run_trader_exclusions_are_case_insensitive_and_ignore_unknown_keys(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                exclude_market_tickers=["kxhighny-26apr08-b72", "NO_MATCH_MARKET", "KXHIGHNY-26APR08-B72"],
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            plan_summary = summary["plan_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72", "NO_MATCH_MARKET"])
            self.assertEqual(intent_summary["exclude_market_tickers_requested_count"], 2)
            self.assertEqual(intent_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(intent_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72", "NO_MATCH_MARKET"])
            self.assertEqual(plan_summary["exclude_market_tickers_requested_count"], 2)
            self.assertEqual(plan_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(plan_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_ticker_count"], 1)
            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXHIGHNY-26APR08-B73")

    def test_run_trader_market_side_exclusion_removes_only_matching_side(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                exclude_market_tickers=["KXHIGHNY-26APR08-B72|no"],
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            plan_summary = summary["plan_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_requested"], [])
            self.assertEqual(intent_summary["exclude_market_tickers_requested_count"], 0)
            self.assertEqual(intent_summary["exclude_market_tickers_applied"], [])
            self.assertEqual(intent_summary["exclude_market_tickers_applied_count"], 0)
            self.assertEqual(intent_summary["exclude_market_side_targets_requested"], ["KXHIGHNY-26APR08-B72|no"])
            self.assertEqual(intent_summary["exclude_market_side_targets_requested_count"], 1)
            self.assertEqual(intent_summary["exclude_market_side_targets_applied"], ["KXHIGHNY-26APR08-B72|no"])
            self.assertEqual(intent_summary["exclude_market_side_targets_applied_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_ticker_count"], 0)
            self.assertEqual(intent_summary["excluded_intents_by_market_side_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_target_count"], 1)
            self.assertEqual(plan_summary["exclude_market_side_targets_requested"], ["KXHIGHNY-26APR08-B72|no"])
            self.assertEqual(plan_summary["exclude_market_side_targets_requested_count"], 1)
            self.assertEqual(plan_summary["exclude_market_side_targets_applied"], ["KXHIGHNY-26APR08-B72|no"])
            self.assertEqual(plan_summary["exclude_market_side_targets_applied_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_ticker_count"], 0)
            self.assertEqual(plan_summary["excluded_intents_by_market_side_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_target_count"], 1)
            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXHIGHNY-26APR08-B73")

    def test_run_trader_mixed_ticker_and_side_exclusion_filters_both(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B74",
                market_title="74F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:74",
                observed_max_settlement_raw="74",
                observed_max_settlement_quantized="74",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                exclude_market_tickers=["KXHIGHNY-26APR08-B72", "KXHIGHNY-26APR08-B73|yes"],
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            plan_summary = summary["plan_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(intent_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(intent_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(
                intent_summary["exclude_market_side_targets_requested"],
                ["KXHIGHNY-26APR08-B73|yes"],
            )
            self.assertEqual(intent_summary["exclude_market_side_targets_requested_count"], 1)
            self.assertEqual(intent_summary["exclude_market_side_targets_applied"], ["KXHIGHNY-26APR08-B73|yes"])
            self.assertEqual(intent_summary["exclude_market_side_targets_applied_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_side_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_target_count"], 2)
            self.assertEqual(plan_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(plan_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(plan_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(plan_summary["exclude_market_side_targets_requested"], ["KXHIGHNY-26APR08-B73|yes"])
            self.assertEqual(plan_summary["exclude_market_side_targets_requested_count"], 1)
            self.assertEqual(plan_summary["exclude_market_side_targets_applied"], ["KXHIGHNY-26APR08-B73|yes"])
            self.assertEqual(plan_summary["exclude_market_side_targets_applied_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_side_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_target_count"], 2)
            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXHIGHNY-26APR08-B74")

    def test_run_trader_malformed_side_exclusion_falls_back_to_ticker_matching(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            self._append_temperature_market_inputs(
                specs_csv=Path(paths["specs_csv"]),
                constraint_csv=Path(paths["constraint_csv"]),
                now=now,
                market_ticker="KXHIGHNY-26APR08-B73",
                market_title="73F or above",
                constraint_status="yes_likely_locked",
                threshold_expression="at_least:73",
                observed_max_settlement_raw="73",
                observed_max_settlement_quantized="73",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                exclude_market_tickers=["KXHIGHNY-26APR08-B72|MAYBE"],
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            plan_summary = summary["plan_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(intent_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(intent_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(intent_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(intent_summary["exclude_market_side_targets_requested"], [])
            self.assertEqual(intent_summary["exclude_market_side_targets_requested_count"], 0)
            self.assertEqual(intent_summary["exclude_market_side_targets_applied"], [])
            self.assertEqual(intent_summary["exclude_market_side_targets_applied_count"], 0)
            self.assertEqual(intent_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(intent_summary["excluded_intents_by_market_side_count"], 0)
            self.assertEqual(intent_summary["excluded_intents_by_market_target_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_requested"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(plan_summary["exclude_market_tickers_requested_count"], 1)
            self.assertEqual(plan_summary["exclude_market_tickers_applied"], ["KXHIGHNY-26APR08-B72"])
            self.assertEqual(plan_summary["exclude_market_tickers_applied_count"], 1)
            self.assertEqual(plan_summary["exclude_market_side_targets_requested"], [])
            self.assertEqual(plan_summary["exclude_market_side_targets_requested_count"], 0)
            self.assertEqual(plan_summary["exclude_market_side_targets_applied"], [])
            self.assertEqual(plan_summary["exclude_market_side_targets_applied_count"], 0)
            self.assertEqual(plan_summary["excluded_intents_by_market_ticker_count"], 1)
            self.assertEqual(plan_summary["excluded_intents_by_market_side_count"], 0)
            self.assertEqual(plan_summary["excluded_intents_by_market_target_count"], 1)
            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_ticker"], "KXHIGHNY-26APR08-B73")

    def test_run_trader_same_second_runs_write_unique_artifacts(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            first = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )
            second = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )

            first_plan_csv = Path(str(first["plan_summary"]["output_csv"]))
            second_plan_csv = Path(str(second["plan_summary"]["output_csv"]))
            first_plan_summary = Path(str(first["plan_summary"]["output_file"]))
            second_plan_summary = Path(str(second["plan_summary"]["output_file"]))
            first_intents_csv = Path(str(first["intent_summary"]["output_csv"]))
            second_intents_csv = Path(str(second["intent_summary"]["output_csv"]))
            first_intents_summary = Path(str(first["intent_summary"]["output_file"]))
            second_intents_summary = Path(str(second["intent_summary"]["output_file"]))
            first_finalization = Path(str(first["intent_summary"]["finalization_snapshot_file"]))
            second_finalization = Path(str(second["intent_summary"]["finalization_snapshot_file"]))

            self.assertTrue(first_plan_csv.exists())
            self.assertTrue(second_plan_csv.exists())
            self.assertTrue(first_plan_summary.exists())
            self.assertTrue(second_plan_summary.exists())
            self.assertTrue(first_intents_csv.exists())
            self.assertTrue(second_intents_csv.exists())
            self.assertTrue(first_intents_summary.exists())
            self.assertTrue(second_intents_summary.exists())
            self.assertTrue(first_finalization.exists())
            self.assertTrue(second_finalization.exists())

            self.assertNotEqual(first_plan_csv, second_plan_csv)
            self.assertNotEqual(first_plan_summary, second_plan_summary)
            self.assertNotEqual(first_intents_csv, second_intents_csv)
            self.assertNotEqual(first_intents_summary, second_intents_summary)
            self.assertNotEqual(first_finalization, second_finalization)

            self.assertTrue(first_plan_csv.name.startswith("kalshi_temperature_trade_plan_"))
            self.assertTrue(second_plan_csv.name.startswith("kalshi_temperature_trade_plan_"))
            self.assertTrue(first_intents_csv.name.startswith("kalshi_temperature_trade_intents_"))
            self.assertTrue(second_intents_csv.name.startswith("kalshi_temperature_trade_intents_"))
            self.assertTrue(
                first_intents_summary.name.startswith("kalshi_temperature_trade_intents_summary_")
            )
            self.assertTrue(
                second_intents_summary.name.startswith("kalshi_temperature_trade_intents_summary_")
            )

            base_stamp = now.strftime("%Y%m%d_%H%M%S")
            self.assertEqual(first_plan_csv.stem, f"kalshi_temperature_trade_plan_{base_stamp}")
            self.assertEqual(second_plan_csv.stem, f"kalshi_temperature_trade_plan_{base_stamp}_01")

    def test_run_trader_canary_expansion_readiness_tracks_ingest_quality(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            metar_summary_path = Path(paths["metar_summary"])
            low_quality_payload = json.loads(metar_summary_path.read_text(encoding="utf-8"))
            low_quality_payload.update(
                {
                    "quality_score": 0.31,
                    "quality_grade": "critical",
                    "quality_status": "degraded",
                    "quality_signal_count": 2,
                    "quality_signals": ["fresh_station_coverage_critical", "parse_error_rate_critical"],
                    "fresh_station_coverage_ratio": 0.21,
                    "usable_latest_station_count": 1,
                    "stale_or_future_row_ratio": 0.62,
                }
            )
            metar_summary_path.write_text(json.dumps(low_quality_payload), encoding="utf-8")

            blocked_summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )
            blocked_plan = blocked_summary["plan_summary"]
            blocked_intents = blocked_summary["intent_summary"]
            self.assertFalse(blocked_plan["metar_ingest_quality_gate_passed"])
            self.assertEqual(blocked_plan["metar_ingest_quality_status"], "degraded")
            self.assertFalse(blocked_plan["canary_expansion_ready"])
            self.assertIn("metar_ingest_quality_gate_not_passing", blocked_plan["canary_expansion_reasons"])
            self.assertEqual(int(blocked_plan["metar_ingest_quality_blocked_count"]), 1)
            self.assertFalse(blocked_intents["canary_expansion_ready"])

            high_quality_payload = dict(low_quality_payload)
            high_quality_payload.update(
                {
                    "quality_score": 0.95,
                    "quality_grade": "excellent",
                    "quality_status": "ready",
                    "quality_signal_count": 0,
                    "quality_signals": [],
                    "fresh_station_coverage_ratio": 0.94,
                    "stale_or_future_row_ratio": 0.02,
                }
            )
            metar_summary_path.write_text(json.dumps(high_quality_payload), encoding="utf-8")

            ready_summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )
            ready_plan = ready_summary["plan_summary"]
            ready_intents = ready_summary["intent_summary"]
            self.assertTrue(ready_plan["metar_ingest_quality_gate_passed"])
            self.assertEqual(ready_plan["metar_ingest_quality_score"], 0.95)
            self.assertEqual(ready_plan["metar_ingest_quality_status"], "ready")
            self.assertTrue(ready_plan["canary_expansion_ready"])
            self.assertEqual(ready_plan["canary_expansion_reasons"], [])
            self.assertEqual(int(ready_plan["metar_ingest_quality_blocked_count"]), 0)
            self.assertTrue(ready_intents["canary_expansion_ready"])
            self.assertEqual(int(ready_intents["intents_approved"]), 1)

    def test_run_trader_applies_adaptive_policy_profile_overrides(self) -> None:
        summary = self._run_basic_trader_summary(
            adaptive_policy_profile={
                "min_probability_confidence": 0.87,
                "min_expected_edge_net": 0.04,
                "min_edge_to_risk_ratio": 0.75,
                "max_intents_per_underlying": 2,
            }
        )

        profile = summary["adaptive_policy_profile"]
        self.assertTrue(profile["adaptive_policy_profile_present"])
        self.assertTrue(profile["adaptive_policy_profile_valid"])
        self.assertTrue(profile["adaptive_policy_profile_applied"])
        self.assertEqual(
            profile["adaptive_policy_profile_effective_overrides"],
            {
                "min_probability_confidence": 0.87,
                "min_expected_edge_net": 0.04,
                "min_edge_to_risk_ratio": 0.75,
                "max_intents_per_underlying": 2,
            },
        )
        self.assertEqual(summary["intent_summary"]["adaptive_policy_profile_effective_overrides"], profile["adaptive_policy_profile_effective_overrides"])
        self.assertEqual(summary["plan_summary"]["adaptive_policy_profile_effective_overrides"], profile["adaptive_policy_profile_effective_overrides"])

    def test_run_trader_clamps_invalid_adaptive_policy_profile_values(self) -> None:
        summary = self._run_basic_trader_summary(
            adaptive_policy_profile={
                "min_probability_confidence": 0.1,
                "min_expected_edge_net": "bad",
                "min_edge_to_risk_ratio": 999.0,
                "max_intents_per_underlying": 0,
            }
        )

        profile = summary["adaptive_policy_profile"]
        self.assertTrue(profile["adaptive_policy_profile_present"])
        self.assertTrue(profile["adaptive_policy_profile_valid"])
        self.assertTrue(profile["adaptive_policy_profile_applied"])
        self.assertAlmostEqual(
            float(profile["adaptive_policy_profile_effective_overrides"]["min_probability_confidence"]),
            0.6,
            places=6,
        )
        self.assertAlmostEqual(
            float(profile["adaptive_policy_profile_effective_overrides"]["min_edge_to_risk_ratio"]),
            5.0,
            places=6,
        )
        self.assertIn("min_probability_confidence", profile["adaptive_policy_profile_clamped_overrides"])
        self.assertIn("min_edge_to_risk_ratio", profile["adaptive_policy_profile_clamped_overrides"])
        self.assertIn("min_expected_edge_net", profile["adaptive_policy_profile_ignored_overrides"])
        self.assertIn("max_intents_per_underlying", profile["adaptive_policy_profile_ignored_overrides"])
        self.assertNotIn("min_expected_edge_net", profile["adaptive_policy_profile_effective_overrides"])
        self.assertNotIn("max_intents_per_underlying", profile["adaptive_policy_profile_effective_overrides"])

    def test_run_trader_without_adaptive_profile_keeps_default_behavior(self) -> None:
        summary = self._run_basic_trader_summary()

        profile = summary["adaptive_policy_profile"]
        self.assertFalse(profile["adaptive_policy_profile_present"])
        self.assertFalse(profile["adaptive_policy_profile_applied"])
        self.assertEqual(profile["adaptive_policy_profile_effective_overrides"], {})
        self.assertEqual(summary["intent_summary"]["adaptive_policy_profile_effective_overrides"], {})
        self.assertEqual(summary["plan_summary"]["adaptive_policy_profile_effective_overrides"], {})
        self.assertEqual(summary["intent_summary"]["intents_total"], 1)
        self.assertEqual(summary["intent_summary"]["intents_approved"], 1)

    def test_run_trader_weather_pattern_profile_falls_back_without_profile(self) -> None:
        summary = self._run_basic_trader_summary()

        intent_summary = summary["intent_summary"]
        self.assertFalse(intent_summary["weather_pattern_profile_loaded"])
        self.assertFalse(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["weather_pattern_hard_block_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_probability_raise_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_expected_edge_raise_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_matched_bucket_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_profile_status"], "missing")
        self.assertEqual(intent_summary["intents_approved"], 1)

    def test_run_trader_weather_pattern_profile_raises_on_risky_bucket(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 24,
                            "expectancy_per_trade": -0.14,
                        }
                    }
                },
            },
            weather_pattern_negative_expectancy_threshold=-0.139,
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_profile_loaded"])
        self.assertTrue(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["weather_pattern_hard_block_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_probability_raise_count"], 1)
        self.assertEqual(intent_summary["weather_pattern_expected_edge_raise_count"], 1)
        self.assertGreaterEqual(intent_summary["weather_pattern_matched_bucket_count"], 1)
        top_approved = intent_summary.get("top_approved")
        self.assertTrue(isinstance(top_approved, list) and top_approved)
        self.assertIn("weather_pattern_probability_raise=", str(top_approved[0].get("policy_notes") or ""))
        self.assertIn("weather_pattern_bucket_match=", str(top_approved[0].get("policy_notes") or ""))

    def test_run_trader_weather_pattern_multi_bucket_hard_blocks(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 32,
                            "expectancy_per_trade": -0.18,
                            "realized_trade_count": 8,
                            "realized_per_trade": -0.026,
                            "edge_realization_ratio": 0.72,
                            "probability_raise": 0.015,
                            "expected_edge_raise": 0.004,
                        }
                    },
                    "signal_type": {
                        "yes_impossible": {
                            "samples": 30,
                            "expectancy_per_trade": -0.16,
                            "realized_trade_count": 7,
                            "realized_per_trade": -0.018,
                            "edge_realization_ratio": 0.79,
                            "probability_raise": 0.014,
                            "expected_edge_raise": 0.003,
                        }
                    },
                },
            }
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_profile_loaded"])
        self.assertTrue(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["intents_approved"], 0)
        self.assertEqual(intent_summary["weather_pattern_hard_block_count"], 1)
        self.assertGreaterEqual(intent_summary["weather_pattern_hard_block_evidence_count"], 2)
        self.assertEqual(intent_summary["policy_reason_counts"].get("weather_pattern_multi_bucket_hard_block"), 1)
        self.assertEqual(intent_summary["weather_pattern_matched_bucket_count"], 2)
        hard_block_evidence = intent_summary.get("weather_pattern_hard_block_evidence_top") or []
        self.assertTrue(isinstance(hard_block_evidence, list) and hard_block_evidence)
        self.assertTrue(any("station:KNYC" in item for item in hard_block_evidence))
        self.assertTrue(any("signal_type:yes_impossible" in item for item in hard_block_evidence))
        self.assertTrue(any("reason=realized_negative" in item for item in hard_block_evidence))

    def test_run_trader_weather_pattern_multi_bucket_model_only_does_not_hard_block(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 64,
                            "expectancy_per_trade": -0.17,
                            "probability_confidence_mean": 0.89,
                            "probability_raise": 0.015,
                            "expected_edge_raise": 0.004,
                        }
                    },
                    "signal_type": {
                        "yes_impossible": {
                            "samples": 62,
                            "expectancy_per_trade": -0.16,
                            "probability_confidence_mean": 0.88,
                            "probability_raise": 0.014,
                            "expected_edge_raise": 0.003,
                        }
                    },
                },
            }
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_profile_loaded"])
        self.assertTrue(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["weather_pattern_hard_block_count"], 0)
        self.assertIsNone(intent_summary["policy_reason_counts"].get("weather_pattern_multi_bucket_hard_block"))
        self.assertGreaterEqual(intent_summary["weather_pattern_matched_bucket_count"], 2)

    def test_run_trader_weather_pattern_stale_profile_noop(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile_max_age_hours=1.0,
            weather_pattern_profile_artifact={
                "captured_at": "2026-04-01T12:00:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 30,
                            "expectancy_per_trade": -0.2,
                            "probability_raise": 0.02,
                            "expected_edge_raise": 0.005,
                        }
                    }
                },
            },
            weather_pattern_profile_artifact_age_hours=168.0,
        )

        intent_summary = summary["intent_summary"]
        self.assertFalse(intent_summary["weather_pattern_profile_loaded"])
        self.assertFalse(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["weather_pattern_profile_status"], "stale")
        self.assertEqual(intent_summary["weather_pattern_hard_block_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_probability_raise_count"], 0)
        self.assertEqual(intent_summary["weather_pattern_expected_edge_raise_count"], 0)
        self.assertEqual(intent_summary["intents_approved"], 1)

    def test_run_trader_weather_pattern_profile_loads_health_artifact_shape(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile_artifact_relative_path=(
                "outputs/health/kalshi_temperature_weather_pattern_latest.json"
            ),
            weather_pattern_profile_artifact={
                "status": "ready",
                "captured_at": "2026-04-08T11:45:00+00:00",
                "profile": {
                    "bucket_dimensions": {
                        "settlement_station": {
                            "KNYC": {
                                "attempts": 30,
                                "expected_edge_mean": -0.065,
                            }
                        }
                    },
                    "regime_risk": {
                        "negative_expectancy_regime_concentration": 0.12,
                    },
                },
            },
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_profile_loaded"])
        self.assertTrue(intent_summary["weather_pattern_profile_applied"])
        self.assertEqual(intent_summary["weather_pattern_profile_source_origin"], "latest_artifact")
        self.assertTrue(
            str(intent_summary["weather_pattern_profile_json_used"]).endswith(
                "outputs/health/kalshi_temperature_weather_pattern_latest.json"
            )
        )
        self.assertEqual(intent_summary["weather_pattern_probability_raise_count"], 1)
        top_approved = intent_summary.get("top_approved")
        self.assertTrue(isinstance(top_approved, list) and top_approved)
        self.assertIn("weather_pattern_bucket_match=station:KNYC", str(top_approved[0].get("policy_notes") or ""))

    def test_policy_gate_weather_pattern_stale_metar_bucket_raises_more(self) -> None:
        def _extract_float_note(note_text: str, key: str) -> float:
            for token in str(note_text).split(","):
                token = token.strip()
                if token.startswith(f"{key}="):
                    try:
                        return float(token.split("=", 1)[1])
                    except ValueError:
                        return 0.0
            return 0.0

        profile = {
            "bucket_profiles": {
                "metar_age_bucket": {
                    "0-30m": {
                        "samples": 30,
                        "expectancy_per_trade": -0.065,
                    },
                    "61-120m": {
                        "samples": 30,
                        "expectancy_per_trade": -0.065,
                    },
                }
            }
        }
        gate = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=120.0,
            weather_pattern_hardening_enabled=True,
            weather_pattern_profile=profile,
            weather_pattern_min_bucket_samples=12,
            weather_pattern_negative_expectancy_threshold=-0.05,
        )
        fresh = self._make_intent(
            intent_id="weather-fresh",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|2026-04-08",
            settlement_station="KNYC",
            metar_observation_age_minutes=20.0,
            metar_observation_time_utc="2026-04-08T11:40:00Z",
        )
        stale = self._make_intent(
            intent_id="weather-stale",
            market_ticker="KXHIGHNY-26APR08-B73",
            underlying_key="KXHIGHNY|2026-04-08",
            settlement_station="KNYC",
            metar_observation_age_minutes=90.0,
            metar_observation_time_utc="2026-04-08T10:30:00Z",
        )

        fresh_decision = gate.evaluate(intents=[fresh])[0]
        stale_decision = gate.evaluate(intents=[stale])[0]
        fresh_prob_raise = _extract_float_note(fresh_decision.decision_notes, "weather_pattern_probability_raise")
        stale_prob_raise = _extract_float_note(stale_decision.decision_notes, "weather_pattern_probability_raise")
        fresh_edge_raise = _extract_float_note(fresh_decision.decision_notes, "weather_pattern_expected_edge_raise")
        stale_edge_raise = _extract_float_note(stale_decision.decision_notes, "weather_pattern_expected_edge_raise")

        self.assertGreater(stale_prob_raise, fresh_prob_raise)
        self.assertGreater(stale_edge_raise, fresh_edge_raise)
        self.assertIn("stale_metar_pressure=true", stale_decision.decision_notes)

    def test_run_trader_weather_pattern_global_risk_off_blocks(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 30,
                            "expectancy_per_trade": -0.06,
                        }
                    }
                },
                "regime_risk": {
                    "negative_expectancy_regime_concentration": 0.93,
                    "attempts_total": 120,
                    "stale_metar_share": 0.88,
                },
            },
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_risk_off_enabled"])
        self.assertTrue(intent_summary["weather_pattern_risk_off_active"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_blocked_count"], 1)
        self.assertEqual(intent_summary["policy_reason_counts"].get("weather_pattern_global_risk_off"), 1)
        self.assertEqual(intent_summary["intents_approved"], 0)

    def test_run_trader_weather_pattern_global_risk_off_prefers_confidence_adjusted_metrics(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "regime_risk": {
                    "negative_expectancy_regime_concentration": 0.93,
                    "negative_expectancy_attempt_share_confidence_adjusted": 0.61,
                    "attempts_total": 120,
                    "stale_metar_share": 0.88,
                    "stale_metar_negative_attempt_share_confidence_adjusted": 0.41,
                },
            },
        )

        intent_summary = summary["intent_summary"]
        self.assertFalse(intent_summary["weather_pattern_risk_off_active"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_blocked_count"], 0)
        self.assertIsNone(intent_summary["policy_reason_counts"].get("weather_pattern_global_risk_off"))
        self.assertEqual(intent_summary["intents_approved"], 1)

    def test_run_trader_weather_pattern_global_risk_off_bucket_fallback_requires_cross_dimension_confirmation(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "metar_age_bucket": {
                        "121-240m": {
                            "samples": 220,
                            "expectancy_per_trade": -0.08,
                        }
                    }
                },
            },
            weather_pattern_risk_off_concentration_threshold=0.65,
            weather_pattern_risk_off_min_attempts=24,
            weather_pattern_risk_off_stale_metar_share_threshold=0.30,
        )

        intent_summary = summary["intent_summary"]
        self.assertFalse(intent_summary["weather_pattern_risk_off_active"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_metrics_source"], "bucket_fallback")
        self.assertEqual(intent_summary["weather_pattern_risk_off_negative_signal_dimensions"], 1)
        self.assertFalse(intent_summary["weather_pattern_risk_off_fallback_signal_confirmed"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_blocked_count"], 0)
        self.assertIsNone(intent_summary["policy_reason_counts"].get("weather_pattern_global_risk_off"))

    def test_run_trader_weather_pattern_global_risk_off_bucket_fallback_can_activate_with_multi_dimension_signal(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "metar_age_bucket": {
                        "121-240m": {
                            "samples": 220,
                            "expectancy_per_trade": -0.08,
                        }
                    },
                    "station": {
                        "KNYC": {
                            "samples": 220,
                            "expectancy_per_trade": -0.04,
                        }
                    },
                },
            },
            weather_pattern_risk_off_concentration_threshold=0.65,
            weather_pattern_risk_off_min_attempts=24,
            weather_pattern_risk_off_stale_metar_share_threshold=0.30,
        )

        intent_summary = summary["intent_summary"]
        self.assertTrue(intent_summary["weather_pattern_risk_off_active"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_metrics_source"], "bucket_fallback")
        self.assertEqual(intent_summary["weather_pattern_risk_off_negative_signal_dimensions"], 2)
        self.assertTrue(intent_summary["weather_pattern_risk_off_fallback_signal_confirmed"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_blocked_count"], 1)
        self.assertEqual(intent_summary["policy_reason_counts"].get("weather_pattern_global_risk_off"), 1)

    def test_run_trader_weather_pattern_global_risk_off_disabled_no_change(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "bucket_profiles": {
                    "station": {
                        "KNYC": {
                            "samples": 30,
                            "expectancy_per_trade": -0.06,
                        }
                    }
                },
                "regime_risk": {
                    "negative_expectancy_regime_concentration": 0.93,
                    "attempts_total": 120,
                    "stale_metar_share": 0.88,
                },
            },
            weather_pattern_risk_off_enabled=False,
        )

        intent_summary = summary["intent_summary"]
        self.assertFalse(intent_summary["weather_pattern_risk_off_enabled"])
        self.assertFalse(intent_summary["weather_pattern_risk_off_active"])
        self.assertEqual(intent_summary["weather_pattern_risk_off_blocked_count"], 0)
        self.assertIsNone(intent_summary["policy_reason_counts"].get("weather_pattern_global_risk_off"))
        self.assertEqual(intent_summary["intents_approved"], 1)

    def test_run_trader_weather_pattern_negative_regime_suppression_disabled_by_default(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "negative_expectancy_buckets": [
                    {
                        "dimension": "station",
                        "bucket": "KNYC",
                        "samples": 40,
                        "expectancy_per_trade": -0.12,
                    }
                ],
            },
        )

        intent_summary = summary["intent_summary"]
        self.assertEqual(intent_summary["intents_approved"], 1)
        self.assertFalse(intent_summary["weather_pattern_negative_regime_suppression_enabled"])
        self.assertFalse(intent_summary["weather_pattern_negative_regime_suppression_active"])
        self.assertEqual(intent_summary["weather_pattern_negative_regime_suppression_candidate_count"], 0)
        self.assertIsNone(
            intent_summary["policy_reason_counts"].get("weather_pattern_negative_regime_bucket_suppressed")
        )

    def test_run_trader_weather_pattern_negative_regime_suppression_blocks_matching_intent(self) -> None:
        summary = self._run_basic_trader_summary(
            weather_pattern_profile={
                "captured_at": "2026-04-08T11:45:00+00:00",
                "negative_expectancy_buckets": [
                    {
                        "dimension": "station",
                        "bucket": "KNYC",
                        "samples": 48,
                        "expectancy_per_trade": -0.13,
                    }
                ],
            },
            weather_pattern_negative_regime_suppression_enabled=True,
            weather_pattern_negative_regime_suppression_min_bucket_samples=20,
            weather_pattern_negative_regime_suppression_expectancy_threshold=-0.10,
            weather_pattern_negative_regime_suppression_top_n=4,
        )

        intent_summary = summary["intent_summary"]
        self.assertEqual(intent_summary["intents_approved"], 0)
        self.assertTrue(intent_summary["weather_pattern_negative_regime_suppression_enabled"])
        self.assertTrue(intent_summary["weather_pattern_negative_regime_suppression_active"])
        self.assertGreaterEqual(
            int(intent_summary["weather_pattern_negative_regime_suppression_candidate_count"] or 0),
            1,
        )
        self.assertEqual(intent_summary["weather_pattern_negative_regime_suppression_blocked_count"], 1)
        self.assertEqual(
            intent_summary["policy_reason_counts"].get("weather_pattern_negative_regime_bucket_suppressed"),
            1,
        )

    def test_policy_gate_weather_pattern_negative_regime_suppression_notes_include_evidence(self) -> None:
        intent = self._make_intent(
            intent_id="weather-negative-regime-suppression",
            market_ticker="KXHIGHNY-26APR08-B72",
            underlying_key="KXHIGHNY|2026-04-08",
            settlement_station="KNYC",
        )
        gate = TemperaturePolicyGate(
            min_settlement_confidence=0.6,
            max_metar_age_minutes=120.0,
            weather_pattern_negative_regime_suppression_enabled=True,
            weather_pattern_negative_regime_suppression_min_bucket_samples=20,
            weather_pattern_negative_regime_suppression_expectancy_threshold=-0.10,
            weather_pattern_negative_regime_suppression_top_n=4,
            weather_pattern_profile={
                "negative_expectancy_buckets": [
                    {
                        "dimension": "station",
                        "bucket": "KNYC",
                        "samples": 36,
                        "expectancy_per_trade": -0.14,
                    }
                ]
            },
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        )
        decision = gate.evaluate(intents=[intent])[0]
        self.assertFalse(decision.approved)
        self.assertEqual(decision.decision_reason, "weather_pattern_negative_regime_bucket_suppressed")
        self.assertIn("weather_pattern_negative_regime_suppressed=true", decision.decision_notes)
        self.assertIn(
            "weather_pattern_negative_regime_suppression_match=station:KNYC",
            decision.decision_notes,
        )

    def test_run_trader_blocks_recent_market_side_replans_without_material_change(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                enforce_probability_edge_thresholds=False,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.1,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 0)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 1)
            self.assertEqual(cooldown["override_count"], 0)
            self.assertEqual(cooldown["backstop_released_count"], 0)
            self.assertEqual(cooldown["blocked_reason_counts"].get("market_side_replan_cooldown"), 1)

    def test_run_trader_allows_recent_market_side_replan_when_price_moves(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "maker_entry_price_dollars": "0.90",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                enforce_probability_edge_thresholds=False,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.02,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 0)
            self.assertEqual(cooldown["override_count"], 1)
            self.assertEqual(cooldown["backstop_released_count"], 0)
            self.assertEqual(cooldown["override_reason_counts"].get("price_changed"), 1)

    def test_run_trader_repeat_cap_blocks_recycled_market_side_even_when_cooldown_disabled(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_114500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "planned_at_utc",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:45:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:45:00Z",
                        "temperature_client_order_id": "temp-prior-1",
                        "temperature_intent_id": "prior-intent-1",
                    }
                )
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:30:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.88",
                        "confidence": "0.94",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:30:00Z",
                        "temperature_client_order_id": "temp-prior-2",
                        "temperature_intent_id": "prior-intent-2",
                    }
                )

            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=0.0,
                replan_market_side_repeat_window_minutes=1440.0,
                replan_market_side_max_plans_per_window=2,
                replan_market_side_price_change_override_dollars=1.0,
                replan_market_side_alpha_change_override=10.0,
                replan_market_side_confidence_change_override=1.0,
                replan_market_side_min_observation_advance_minutes=999.0,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 0)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 1)
            self.assertEqual(cooldown["repeat_cap_blocked_count"], 1)
            self.assertEqual(cooldown["blocked_reason_counts"].get("market_side_repeat_cap"), 1)

    def test_run_trader_repeat_cap_can_be_disabled(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_114500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "planned_at_utc",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:45:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:45:00Z",
                        "temperature_client_order_id": "temp-prior-1",
                        "temperature_intent_id": "prior-intent-1",
                    }
                )
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:30:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.88",
                        "confidence": "0.94",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:30:00Z",
                        "temperature_client_order_id": "temp-prior-2",
                        "temperature_intent_id": "prior-intent-2",
                    }
                )

            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=0.0,
                replan_market_side_repeat_window_minutes=1440.0,
                replan_market_side_max_plans_per_window=0,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 0)
            self.assertEqual(cooldown["repeat_cap_blocked_count"], 0)

    def test_run_trader_repeat_cap_allows_material_price_override(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_114500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "planned_at_utc",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:55:00Z",
                        "maker_entry_price_dollars": "0.90",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior-1",
                        "temperature_intent_id": "prior-intent-1",
                    }
                )
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:50:00Z",
                        "maker_entry_price_dollars": "0.90",
                        "temperature_alpha_strength": "0.88",
                        "confidence": "0.94",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:50:00Z",
                        "temperature_client_order_id": "temp-prior-2",
                        "temperature_intent_id": "prior-intent-2",
                    }
                )

            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_repeat_window_minutes=1440.0,
                replan_market_side_max_plans_per_window=2,
                replan_market_side_price_change_override_dollars=0.02,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            self.assertEqual(plan_summary["replan_market_side_repeat_cap_override_count"], 1)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 0)
            self.assertEqual(cooldown["repeat_cap_blocked_count"], 0)
            self.assertEqual(cooldown["repeat_cap_override_count"], 1)
            self.assertEqual(cooldown["override_reason_counts"].get("repeat_cap_override_price_changed"), 1)

    def test_run_trader_suppresses_backstop_release_when_independent_breadth_is_thin(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.1,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
                replan_market_side_min_orders_backstop=1,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 0)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["backstop_released_count"], 0)
            self.assertTrue(cooldown["backstop_release_suppressed"])
            self.assertEqual(cooldown["backstop_release_suppressed_reason"], "thin_independent_breadth")
            self.assertEqual(cooldown["blocked_count"], 1)

    def test_market_side_replan_backstop_requires_meaningful_override_trigger(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "market_ticker": "KXHIGHNY-26APR08-B72",
                            "side": "no",
                            "maker_entry_price_dollars": "0.95",
                            "temperature_alpha_strength": "0.90",
                            "confidence": "0.95",
                            "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                            "temperature_client_order_id": "temp-prior-1",
                            "temperature_intent_id": "prior-intent-1",
                        },
                        {
                            "market_ticker": "KXHIGHDAL-26APR08-B90",
                            "side": "no",
                            "maker_entry_price_dollars": "0.95",
                            "temperature_alpha_strength": "0.88",
                            "confidence": "0.94",
                            "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                            "temperature_client_order_id": "temp-prior-2",
                            "temperature_intent_id": "prior-intent-2",
                        },
                        {
                            "market_ticker": "KXHIGHMIA-26APR08-B88",
                            "side": "no",
                            "maker_entry_price_dollars": "0.95",
                            "temperature_alpha_strength": "0.87",
                            "confidence": "0.93",
                            "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                            "temperature_client_order_id": "temp-prior-3",
                            "temperature_intent_id": "prior-intent-3",
                        },
                        {
                            "market_ticker": "KXHIGHPHX-26APR08-B96",
                            "side": "no",
                            "maker_entry_price_dollars": "0.95",
                            "temperature_alpha_strength": "0.86",
                            "confidence": "0.92",
                            "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                            "temperature_client_order_id": "temp-prior-4",
                            "temperature_intent_id": "prior-intent-4",
                        },
                    ]
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            intents = [
                self._make_intent(
                    intent_id="repeat-1",
                    market_ticker="KXHIGHNY-26APR08-B72",
                    underlying_key="KXHIGHNY|KNYC|2026-04-08",
                    settlement_station="KNYC",
                    metar_observation_time_utc="2026-04-08T11:55:00Z",
                ),
                self._make_intent(
                    intent_id="repeat-2",
                    market_ticker="KXHIGHDAL-26APR08-B90",
                    underlying_key="KXHIGHDAL|KDAL|2026-04-08",
                    settlement_station="KDAL",
                    metar_observation_time_utc="2026-04-08T11:55:00Z",
                ),
                self._make_intent(
                    intent_id="repeat-3",
                    market_ticker="KXHIGHMIA-26APR08-B88",
                    underlying_key="KXHIGHMIA|KMIA|2026-04-08",
                    settlement_station="KMIA",
                    metar_observation_time_utc="2026-04-08T11:55:00Z",
                ),
                self._make_intent(
                    intent_id="repeat-4",
                    market_ticker="KXHIGHPHX-26APR08-B96",
                    underlying_key="KXHIGHPHX|KPHX|2026-04-08",
                    settlement_station="KPHX",
                    metar_observation_time_utc="2026-04-08T11:55:00Z",
                ),
            ]

            kept, cooldown_meta = _apply_market_side_replan_cooldown(
                intents=intents,
                output_dir=str(base),
                now_utc=now,
                cooldown_minutes=20.0,
                repeat_window_minutes=1440.0,
                max_plans_per_window=0,
                price_change_override_dollars=0.05,
                alpha_change_override=0.15,
                confidence_change_override=0.10,
                min_observation_advance_minutes=8.0,
                max_history_files=8,
                min_orders_backstop=2,
            )

            self.assertEqual(len(kept), 0)
            self.assertEqual(int(cooldown_meta.get("backstop_released_count") or 0), 0)
            self.assertTrue(bool(cooldown_meta.get("backstop_release_suppressed")))
            self.assertEqual(cooldown_meta.get("backstop_release_suppressed_reason"), "no_override_trigger")
            self.assertEqual(int(cooldown_meta.get("backstop_release_qualified_count") or 0), 0)
            self.assertEqual(int(cooldown_meta.get("backstop_release_unqualified_count") or 0), 4)
            self.assertGreaterEqual(
                float(cooldown_meta.get("backstop_release_override_trigger_min_elapsed_minutes") or 0.0),
                2.0,
            )

    def test_run_trader_allows_recent_market_side_replan_when_observation_advances(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:40:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                enforce_probability_edge_thresholds=False,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.1,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
                replan_market_side_min_observation_advance_minutes=2.0,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 0)
            self.assertEqual(cooldown["override_count"], 1)
            self.assertEqual(cooldown["override_reason_counts"].get("metar_observation_advanced"), 1)

    def test_market_side_replan_repeat_pressure_raises_observation_threshold(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            intent_a = self._make_intent(
                intent_id="repeat-1",
                market_ticker="KXHIGHNY-26APR08-B72",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
                metar_observation_time_utc="2026-04-08T11:58:00Z",
            )
            intent_b = self._make_intent(
                intent_id="repeat-2",
                market_ticker="KXHIGHNY-26APR08-B72",
                underlying_key="KXHIGHNY|KNYC|2026-04-08",
                settlement_station="KNYC",
                metar_observation_time_utc="2026-04-08T11:58:00Z",
            )

            kept, cooldown_meta = _apply_market_side_replan_cooldown(
                intents=[intent_a, intent_b],
                output_dir=str(base),
                now_utc=now,
                cooldown_minutes=20.0,
                repeat_window_minutes=1440.0,
                max_plans_per_window=5,
                price_change_override_dollars=1.0,
                alpha_change_override=10.0,
                confidence_change_override=1.0,
                min_observation_advance_minutes=2.0,
                max_history_files=8,
                min_orders_backstop=0,
            )

            self.assertTrue(bool(cooldown_meta.get("repeat_pressure_mode")))
            self.assertEqual(float(cooldown_meta.get("effective_min_observation_advance_minutes") or 0.0), 8.0)
            self.assertEqual(float(cooldown_meta.get("effective_price_change_override_dollars") or 0.0), 1.0)
            self.assertEqual(len(kept), 0)
            self.assertEqual(int(cooldown_meta.get("blocked_count") or 0), 1)
            self.assertEqual(
                int((cooldown_meta.get("blocked_reason_counts") or {}).get("market_side_replan_cooldown") or 0),
                1,
            )

    def test_run_trader_cooldown_uses_row_planned_time_over_file_mtime(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_115500.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "planned_at_utc",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:55:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:55:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            old_epoch = datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (old_epoch, old_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.1,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 0)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 1)
            self.assertEqual(cooldown["override_count"], 0)

    def test_run_trader_cooldown_ignores_stale_row_even_if_file_mtime_recent(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)

            prior_plan = base / "kalshi_temperature_trade_plan_20260408_112000.csv"
            with prior_plan.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "side",
                        "planned_at_utc",
                        "maker_entry_price_dollars",
                        "temperature_alpha_strength",
                        "confidence",
                        "temperature_metar_observation_time_utc",
                        "temperature_client_order_id",
                        "temperature_intent_id",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "side": "no",
                        "planned_at_utc": "2026-04-08T11:20:00Z",
                        "maker_entry_price_dollars": "0.95",
                        "temperature_alpha_strength": "0.9",
                        "confidence": "0.95",
                        "temperature_metar_observation_time_utc": "2026-04-08T11:20:00Z",
                        "temperature_client_order_id": "temp-prior",
                        "temperature_intent_id": "prior-intent",
                    }
                )
            recent_epoch = datetime(2026, 4, 8, 11, 59, tzinfo=timezone.utc).timestamp()
            os.utime(prior_plan, (recent_epoch, recent_epoch))

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
                replan_market_side_cooldown_minutes=20.0,
                replan_market_side_price_change_override_dollars=0.1,
                replan_market_side_alpha_change_override=0.5,
                replan_market_side_confidence_change_override=0.1,
                replan_market_side_min_orders_backstop=0,
            )

            plan_summary = summary["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            cooldown = plan_summary["replan_market_side_cooldown"]
            self.assertEqual(cooldown["blocked_count"], 0)
            self.assertEqual(cooldown["override_count"], 0)

    def test_run_trader_applies_adaptive_station_metar_age_overrides(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            metar_state_path = Path(paths["metar_summary"]).with_name("metar_state.json")
            metar_state_payload = json.loads(metar_state_path.read_text(encoding="utf-8"))
            metar_state_payload["latest_observation_by_station"]["KNYC"]["observation_time_utc"] = "2026-04-08T11:20:00Z"
            metar_state_payload["station_observation_interval_stats"] = {
                "KNYC": {
                    "sample_count": 3,
                    "latest_interval_minutes": 60.0,
                    "interval_median_minutes": 60.0,
                    "interval_p90_minutes": 60.0,
                    "recent_interval_minutes": [60.0, 60.0, 60.0],
                }
            }
            metar_state_path.write_text(json.dumps(metar_state_payload), encoding="utf-8")

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                max_metar_age_minutes=22.5,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 1)
            self.assertEqual(intent_summary["metar_age_adaptive_station_override_count"], 1)
            self.assertEqual(intent_summary["metar_age_effective_station_override_count"], 1)
            self.assertEqual(intent_summary["metar_age_adaptive_overrides_top"][0]["station"], "KNYC")

    def test_run_trader_bootstraps_station_metar_age_without_interval_history(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            metar_state_path = Path(paths["metar_summary"]).with_name("metar_state.json")
            metar_state_payload = json.loads(metar_state_path.read_text(encoding="utf-8"))
            metar_state_payload["latest_observation_by_station"]["KNYC"]["observation_time_utc"] = "2026-04-08T11:04:00Z"
            metar_state_payload["station_observation_interval_stats"] = {}
            metar_state_path.write_text(json.dumps(metar_state_payload), encoding="utf-8")

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                max_metar_age_minutes=22.5,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 1)
            self.assertGreaterEqual(intent_summary["metar_age_adaptive_station_override_count"], 1)
            self.assertEqual(
                intent_summary["metar_age_adaptive_overrides_top"][0]["mode"],
                "bootstrap_hourly_fallback",
            )
            self.assertGreaterEqual(
                float(intent_summary["metar_age_adaptive_overrides_top"][0]["adaptive_max_age_minutes"]),
                60.0,
            )

    def test_run_trader_bootstrap_override_tracks_observed_age(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            metar_state_path = Path(paths["metar_summary"]).with_name("metar_state.json")
            metar_state_payload = json.loads(metar_state_path.read_text(encoding="utf-8"))
            metar_state_payload["latest_observation_by_station"]["KNYC"]["observation_time_utc"] = "2026-04-08T10:58:00Z"
            metar_state_payload["station_observation_interval_stats"] = {}
            metar_state_path.write_text(json.dumps(metar_state_payload), encoding="utf-8")

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                max_metar_age_minutes=22.5,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 1)
            adaptive = intent_summary["metar_age_adaptive_overrides_top"][0]
            self.assertEqual(adaptive["mode"], "bootstrap_hourly_fallback")
            self.assertGreater(float(adaptive["adaptive_max_age_minutes"]), 60.0)
            self.assertLessEqual(float(adaptive["adaptive_max_age_minutes"]), 75.0)

    def test_run_trader_cadence_warmup_with_single_interval_sample(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            metar_state_path = Path(paths["metar_summary"]).with_name("metar_state.json")
            metar_state_payload = json.loads(metar_state_path.read_text(encoding="utf-8"))
            metar_state_payload["latest_observation_by_station"]["KNYC"]["observation_time_utc"] = "2026-04-08T10:58:00Z"
            metar_state_payload["station_observation_interval_stats"] = {
                "KNYC": {
                    "sample_count": 1,
                    "latest_interval_minutes": 60.0,
                    "interval_median_minutes": 60.0,
                    "interval_p90_minutes": 60.0,
                    "recent_interval_minutes": [60.0],
                }
            }
            metar_state_path.write_text(json.dumps(metar_state_payload), encoding="utf-8")

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                max_metar_age_minutes=22.5,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 1)
            adaptive = intent_summary["metar_age_adaptive_overrides_top"][0]
            self.assertEqual(adaptive["mode"], "cadence_warmup")
            self.assertGreater(float(adaptive["adaptive_max_age_minutes"]), 60.0)
            self.assertLessEqual(float(adaptive["adaptive_max_age_minutes"]), 75.0)

    def test_run_trader_adaptive_overrides_only_active_stations(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)

            metar_state_path = Path(paths["metar_summary"]).with_name("metar_state.json")
            metar_state_payload = json.loads(metar_state_path.read_text(encoding="utf-8"))
            metar_state_payload["station_observation_interval_stats"] = {
                "KNYC": {
                    "sample_count": 3,
                    "latest_interval_minutes": 60.0,
                    "interval_median_minutes": 60.0,
                    "interval_p90_minutes": 60.0,
                    "recent_interval_minutes": [60.0, 60.0, 60.0],
                },
                "KDEN": {
                    "sample_count": 4,
                    "latest_interval_minutes": 70.0,
                    "interval_median_minutes": 70.0,
                    "interval_p90_minutes": 70.0,
                    "recent_interval_minutes": [70.0, 70.0, 70.0, 70.0],
                },
            }
            metar_state_payload["latest_observation_by_station"]["KDEN"] = {
                "observation_time_utc": "2026-04-08T11:00:00Z",
                "temp_c": 20.0,
                "report_type": "METAR",
            }
            metar_state_path.write_text(json.dumps(metar_state_payload), encoding="utf-8")

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                max_metar_age_minutes=22.5,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertEqual(intent_summary["active_settlement_station_count"], 1)
            self.assertEqual(intent_summary["metar_age_adaptive_station_override_count"], 1)
            self.assertEqual(intent_summary["metar_age_adaptive_overrides_top"][0]["station"], "KNYC")

    def test_run_trader_loads_default_alpha_consensus_artifact(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=False)
            consensus_dir = base / "breadth_worker"
            consensus_dir.mkdir(parents=True, exist_ok=True)
            consensus_path = consensus_dir / "breadth_worker_consensus_latest.json"
            consensus_path.write_text(
                json.dumps(
                    {
                        "captured_at": now.isoformat(),
                        "status": "ready",
                        "candidates": [
                            {
                                "market_side_key": "KXHIGHNY-26APR08-B72|no",
                                "market_ticker": "KXHIGHNY-26APR08-B72",
                                "side": "no",
                                "profile_support_count": 2,
                                "profile_support_ratio": 0.67,
                                "weighted_support_score": 1.75,
                                "weighted_support_ratio": 0.7,
                                "consensus_alpha_score": 0.82,
                                "consensus_rank": 1,
                                "profile_names": ["strict_baseline", "relaxed_interval"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                intents_only=True,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            self.assertTrue(intent_summary["alpha_consensus_loaded"])
            self.assertEqual(intent_summary["alpha_consensus_candidate_count"], 1)
            self.assertEqual(intent_summary["alpha_consensus_usable_candidate_count"], 1)
            top_approved = intent_summary.get("top_approved")
            self.assertTrue(isinstance(top_approved, list) and top_approved)
            self.assertEqual(int(top_approved[0].get("consensus_profile_support_count") or 0), 2)
            self.assertAlmostEqual(float(top_approved[0].get("consensus_alpha_score") or 0.0), 0.82, places=6)
            self.assertTrue(bool(top_approved[0].get("policy_approved")))
            self.assertEqual(str(top_approved[0].get("policy_reason") or ""), "approved")
            self.assertIsInstance(top_approved[0].get("policy_alpha_strength"), (int, float))
            self.assertIsInstance(top_approved[0].get("policy_probability_confidence"), (int, float))
            self.assertIsInstance(top_approved[0].get("policy_expected_edge_net"), (int, float))
            self.assertIn(
                type(top_approved[0].get("policy_min_probability_confidence_required")),
                (int, float, type(None)),
            )
            self.assertIn(
                type(top_approved[0].get("policy_min_expected_edge_net_required")),
                (int, float, type(None)),
            )
            self.assertIn(
                type(top_approved[0].get("policy_min_alpha_strength_required")),
                (int, float, type(None)),
            )

    def test_run_kalshi_temperature_trader_blocks_existing_underlying_inventory(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            specs_csv = base / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "close_time",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "rules_primary",
                        "rules_secondary",
                        "local_day_boundary",
                        "observation_window_local_start",
                        "observation_window_local_end",
                        "threshold_expression",
                        "contract_terms_url",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "close_time": "2026-04-09T00:00:00Z",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "rules_primary": "Highest temperature in local day at KNYC.",
                        "rules_secondary": "",
                        "local_day_boundary": "local_day",
                        "observation_window_local_start": "00:00",
                        "observation_window_local_end": "23:59",
                        "threshold_expression": "at_most:72",
                        "contract_terms_url": "https://example.test/terms",
                        "settlement_confidence_score": "0.92",
                    }
                )

            constraint_csv = base / "constraints.csv"
            with constraint_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "scanned_at",
                        "source_specs_csv",
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "settlement_unit",
                        "settlement_precision",
                        "threshold_expression",
                        "constraint_status",
                        "constraint_reason",
                        "observed_max_settlement_raw",
                        "observed_max_settlement_quantized",
                        "observations_for_date",
                        "snapshot_status",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "scanned_at": now.isoformat(),
                        "source_specs_csv": str(specs_csv),
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "settlement_unit": "fahrenheit",
                        "settlement_precision": "whole_degree",
                        "threshold_expression": "at_most:72",
                        "constraint_status": "yes_impossible",
                        "constraint_reason": "Observed max 74 exceeds at_most threshold 72.",
                        "observed_max_settlement_raw": "74",
                        "observed_max_settlement_quantized": "74",
                        "observations_for_date": "12",
                        "snapshot_status": "ready",
                        "settlement_confidence_score": "0.92",
                    }
                )

            metar_state = base / "metar_state.json"
            metar_state.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {
                                "observation_time_utc": "2026-04-08T11:55:00Z",
                                "temp_c": 24.5,
                            }
                        },
                        "max_temp_c_by_station_local_day": {},
                    }
                ),
                encoding="utf-8",
            )
            metar_summary = base / "metar_summary.json"
            metar_summary.write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "captured_at": now.isoformat(),
                        "raw_sha256": "feedbead1234",
                        "state_file": str(metar_state),
                    }
                ),
                encoding="utf-8",
            )

            ws_state = base / "ws_state.json"
            ws_state.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "ready",
                            "market_count": 1,
                            "desynced_market_count": 0,
                            "last_event_at": now.isoformat(),
                        },
                        "markets": {
                            "KXHIGHNY-26APR08-B72": {
                                "sequence": 22,
                                "updated_at_utc": now.isoformat(),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            book_db = base / "kalshi_portfolio_book.sqlite3"
            connection = sqlite3.connect(book_db)
            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS positions (
                        ticker TEXT PRIMARY KEY,
                        position_fp REAL,
                        market_exposure_dollars REAL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO positions (ticker, position_fp, market_exposure_dollars)
                    VALUES (?, ?, ?)
                    """,
                    ("KXHIGHNY-26APR08-B72", 1.0, 0.56),
                )
                connection.commit()
            finally:
                connection.close()

            summary = run_kalshi_temperature_trader(
                env_file=str(env_file),
                output_dir=str(base),
                specs_csv=str(specs_csv),
                constraint_csv=str(constraint_csv),
                metar_summary_json=str(metar_summary),
                ws_state_json=str(ws_state),
                intents_only=True,
                max_intents_per_underlying=1,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            self.assertEqual(summary["intent_summary"]["intents_total"], 1)
            self.assertEqual(summary["intent_summary"]["intents_approved"], 0)
            self.assertEqual(
                summary["intent_summary"]["policy_reason_counts"].get("underlying_exposure_cap_reached"),
                1,
            )
            self.assertTrue(summary["intent_summary"]["underlying_netting_loaded"])
            self.assertEqual(summary["intent_summary"]["existing_underlying_slots_count"], 1)

    def test_run_kalshi_temperature_trader_prefilters_finalization_blocked_underlying(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            settlement_state_json = base / "settlement_state.json"
            settlement_state_json.write_text(
                json.dumps(
                    {
                        "underlyings": {
                            "KXHIGHNY|KNYC|2026-04-08": {
                                "state": "pending_final_report",
                                "finalization_status": "post_close_unfinalized",
                                "allow_new_orders": False,
                                "reason": "waiting_final_report",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                settlement_state_json=str(settlement_state_json),
                intents_only=True,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            policy_reason_counts = intent_summary["policy_reason_counts"]
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 0)
            self.assertEqual(policy_reason_counts.get("settlement_finalization_blocked"), 1)
            self.assertNotIn("missing_decision", policy_reason_counts)
            self.assertTrue(intent_summary["settlement_state_loaded"])
            self.assertEqual(intent_summary["settlement_state_entries"], 1)
            self.assertEqual(int(intent_summary["finalization_blocked_underlyings"] or 0), 1)

            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["policy_reason"], "settlement_finalization_blocked")
            self.assertNotEqual(rows[0]["policy_reason"], "missing_decision")

    def test_run_kalshi_temperature_trader_prefilter_metrics_include_total_and_reason_map(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            paths = self._write_basic_temperature_inputs(base=base, now=now, include_ws_sequence=True)
            settlement_state_json = base / "settlement_state.json"
            settlement_state_json.write_text(
                json.dumps(
                    {
                        "underlyings": {
                            "KXHIGHNY|KNYC|2026-04-08": {
                                "state": "review_hold",
                                "finalization_status": "pending_review",
                                "allow_new_orders": False,
                                "reason": "manual_review_pending",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_temperature_trader(
                env_file=paths["env_file"],
                output_dir=str(base),
                specs_csv=paths["specs_csv"],
                constraint_csv=paths["constraint_csv"],
                metar_summary_json=paths["metar_summary"],
                ws_state_json=paths["ws_state"],
                settlement_state_json=str(settlement_state_json),
                intents_only=True,
                now=now,
            )

            self.assertEqual(summary["status"], "intents_only")
            intent_summary = summary["intent_summary"]
            policy_reason_counts = intent_summary.get("policy_reason_counts") or {}
            settlement_reason_total = sum(
                int(value or 0)
                for key, value in policy_reason_counts.items()
                if str(key).startswith("settlement_")
            )
            self.assertEqual(intent_summary["intents_total"], 1)
            self.assertEqual(intent_summary["intents_approved"], 0)
            self.assertEqual(policy_reason_counts.get("settlement_review_hold"), 1)
            self.assertGreaterEqual(settlement_reason_total, 1)
            self.assertEqual(int(intent_summary.get("finalization_blocked_underlyings") or 0), 1)

            if "settlement_prefilter_blocked_count" in intent_summary:
                self.assertEqual(int(intent_summary["settlement_prefilter_blocked_count"] or 0), 1)
            if isinstance(intent_summary.get("settlement_prefilter_reason_counts"), dict):
                prefilter_reason_counts = intent_summary["settlement_prefilter_reason_counts"]
                self.assertEqual(int(prefilter_reason_counts.get("settlement_review_hold") or 0), 1)

            with Path(intent_summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["policy_reason"], "settlement_review_hold")
            self.assertNotEqual(rows[0]["policy_reason"], "missing_decision")

    def test_revalidate_temperature_trade_intents_detects_sequence_and_metar_changes(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_max_settlement_quantized": "74",
                "settlement_confidence_score": "0.92",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context={
                "raw_sha256": "sha-old",
                "latest_by_station": {"KNYC": {"observation_time_utc": "2026-04-08T11:50:00Z", "temp_c": 24.2}},
            },
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.95,
            now=now,
        )
        self.assertEqual(len(intents), 1)

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            specs_csv = base / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "rules_primary",
                        "rules_secondary",
                        "settlement_station",
                        "settlement_timezone",
                        "local_day_boundary",
                        "observation_window_local_start",
                        "observation_window_local_end",
                        "threshold_expression",
                        "contract_terms_url",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "rules_primary": "Updated rule text",
                        "rules_secondary": "",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "local_day_boundary": "local_day",
                        "observation_window_local_start": "00:00",
                        "observation_window_local_end": "23:59",
                        "threshold_expression": "at_most:72",
                        "contract_terms_url": "https://example.test/terms",
                    }
                )
            metar_state = base / "metar_state.json"
            metar_state.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {"observation_time_utc": "2026-04-08T11:58:00Z", "temp_c": 24.5}
                        }
                    }
                ),
                encoding="utf-8",
            )
            metar_summary = base / "metar_summary.json"
            metar_summary.write_text(
                json.dumps({"raw_sha256": "sha-new", "state_file": str(metar_state)}),
                encoding="utf-8",
            )
            ws_state = base / "ws_state.json"
            ws_state.write_text(
                json.dumps({"markets": {"KXHIGHNY-26APR08-B72": {"sequence": 11}}}),
                encoding="utf-8",
            )

            valid, invalid, meta = revalidate_temperature_trade_intents(
                intents=intents,
                output_dir=str(base),
                specs_csv=str(specs_csv),
                metar_summary_json=str(metar_summary),
                metar_state_json=str(metar_state),
                ws_state_json=str(ws_state),
                require_market_snapshot_seq=True,
                require_metar_snapshot_sha=False,
            )

            self.assertEqual(len(valid), 0)
            self.assertEqual(len(invalid), 1)
            reasons = set(invalid[0]["reasons"])
            self.assertIn("market_snapshot_seq_changed", reasons)
            self.assertIn("metar_observation_advanced", reasons)
            self.assertIn("metar_snapshot_sha_changed", reasons)
            self.assertEqual(meta["metar_snapshot_sha"], "sha-new")

    def test_run_kalshi_temperature_shadow_watch_runs_multiple_cycles(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_trader_runner(**kwargs: object) -> dict[str, object]:
            calls.append(dict(kwargs))
            loop_index = len(calls)
            return {
                "status": "dry_run",
                "intent_summary": {
                    "intents_total": 2,
                    "intents_approved": 1,
                    "intents_revalidated": 1,
                    "revalidation_invalidated": 0,
                },
                "plan_summary": {"planned_orders": 1},
                "execute_summary": {"status": "dry_run", "output_file": f"/tmp/execute_{loop_index}.json"},
            }

        sleeps: list[float] = []
        summary = run_kalshi_temperature_shadow_watch(
            env_file=".env",
            output_dir="outputs",
            loops=2,
            sleep_between_loops_seconds=3.5,
            trader_runner=fake_trader_runner,
            sleep_fn=lambda seconds: sleeps.append(seconds),
            now=datetime(2026, 4, 9, 13, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["loops_run"], 2)
        self.assertEqual(summary["mode"], "shadow")
        self.assertEqual(len(summary["cycle_summaries"]), 2)
        self.assertEqual(summary["cycle_status_counts"].get("dry_run"), 2)
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [3.5])


if __name__ == "__main__":
    unittest.main()
