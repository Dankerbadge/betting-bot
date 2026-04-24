from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from betbot.decision_matrix_hardening import run_decision_matrix_hardening


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _set_file_time(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def _write_weather_pattern_json(
    path: Path,
    payload: dict[str, object],
    *,
    ts: float | None = None,
) -> None:
    _write_json(path, payload)
    if ts is not None:
        _set_file_time(path, ts)


def _write_healthy_weather_pattern_artifact(
    out_dir: Path,
    now: datetime,
    *,
    filename: str = "kalshi_temperature_weather_pattern_latest.json",
    negative_expectancy_regime_concentration: float = 0.18,
    weather_bucket_coverage_ratio: float = 0.92,
    metar_observation_stale_share: float | None = None,
    weather_risk_off_sample_count: int | None = None,
    age_hours: float = 1.0,
) -> Path:
    path = out_dir / "health" / filename
    payload: dict[str, object] = {
        "status": "ready",
        "captured_at_utc": now.isoformat(),
        "negative_expectancy_regime_concentration": negative_expectancy_regime_concentration,
        "negative_expectancy_regime_count": 9,
        "negative_expectancy_regime_total": 50,
        "weather_bucket_coverage_ratio": weather_bucket_coverage_ratio,
        "weather_bucket_coverage_count": 46,
        "weather_bucket_total": 50,
    }
    if metar_observation_stale_share is not None:
        payload["metar_observation_stale_share"] = float(metar_observation_stale_share)
    if weather_risk_off_sample_count is not None:
        payload["weather_risk_off_sample_count"] = int(weather_risk_off_sample_count)
    if (
        metar_observation_stale_share is not None
        and weather_risk_off_sample_count is not None
        and "metar_observation_stale_count" not in payload
    ):
        payload["metar_observation_stale_count"] = int(
            round(float(metar_observation_stale_share) * float(weather_risk_off_sample_count))
        )
    _write_weather_pattern_json(
        path,
        payload,
        ts=(now - timedelta(hours=age_hours)).timestamp(),
    )
    return path


def _write_low_pressure_non_weather_inputs(out_dir: Path) -> None:
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 100,
                "approval_rate": 0.50,
                "settled_unique_market_side_resolved_predictions": 30,
                "projected_pnl_on_reference_bankroll_dollars": 12.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )


def _write_profitability_summary_artifact(
    out_dir: Path,
    *,
    filename: str,
    captured_at: datetime,
    resolved_unique_market_sides: int,
) -> Path:
    path = out_dir / filename
    _write_json(
        path,
        {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "shadow_settled_reference": {
                "resolved_unique_market_sides": int(resolved_unique_market_sides),
                "headline": {
                    "resolved_predictions": int(resolved_unique_market_sides),
                },
            },
        },
    )
    _set_file_time(path, captured_at.timestamp())
    return path


def _write_settled_outcome_throughput_artifact(
    out_dir: Path,
    *,
    filename: str,
    captured_at: datetime,
    coverage_settled_outcomes: int,
    settled_outcomes_delta_24h: int | None,
    settled_outcomes_delta_7d: int | None,
    combined_bucket_count_delta_24h: int | None,
    combined_bucket_count_delta_7d: int | None,
    targeted_constraint_rows: int,
    top_bottlenecks_count: int,
    bottleneck_source: str,
) -> Path:
    path = out_dir / "health" / filename
    _write_json(
        path,
        {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "coverage": {
                "settled_outcomes": int(coverage_settled_outcomes),
            },
            "growth_deltas": {
                "settled_outcomes_delta_24h": settled_outcomes_delta_24h,
                "settled_outcomes_delta_7d": settled_outcomes_delta_7d,
                "combined_bucket_count_delta_24h": combined_bucket_count_delta_24h,
                "combined_bucket_count_delta_7d": combined_bucket_count_delta_7d,
            },
            "targeting": {
                "targeted_constraint_rows": int(targeted_constraint_rows),
            },
            "top_bottlenecks": [
                {"key": f"bottleneck_{idx}", "count": 1}
                for idx in range(int(top_bottlenecks_count))
            ],
            "bottleneck_source": bottleneck_source,
        },
    )
    _set_file_time(path, captured_at.timestamp())
    return path


def test_decision_matrix_hardening_flags_consistency_and_profitability_blockers(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)

    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "historical quality signal type hard block",
                "largest_blocker_share_of_blocked_raw": 0.760501,
                "largest_blocker_count_raw": 4500,
                "blocked_total": 5918,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 191658,
                "approval_rate": 0.0193,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": True,
                "sparse_hardening_expected_edge_block_share": 0.999927,
                "top_blocker_reason": "historical quality signal type hard block",
                "top_blocker_share_of_blocked": 0.760501,
            },
        },
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)

    assert summary["status"] == "ready"
    assert summary["matrix_health_status"] == "red"
    assert summary["supports_consistency_and_profitability"] is False
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    assert "blocker_concentration_high" in blocker_keys
    assert "historical_quality_hard_block_dominance" in blocker_keys
    assert "approval_rate_below_floor" in blocker_keys
    assert "insufficient_settled_outcomes" in blocker_keys
    assert "projected_pnl_non_positive" in blocker_keys
    assert "quality_drift_alert_active" in blocker_keys
    assert "sparse_edge_block_share_high" in blocker_keys

    backlog_ids = {row["id"] for row in summary["pipeline_backlog"]}
    assert "execution_cost_tape" in backlog_ids
    assert "bucket_settlement_attribution" in backlog_ids
    assert "independent_breadth_sampler" in backlog_ids
    assert "gate_false_negative_review" in backlog_ids
    assert "profitability_calibration_loop" in backlog_ids

    assert Path(summary["output_file"]).exists()
    assert Path(summary["latest_file"]).exists()


def test_decision_matrix_hardening_marks_missing_inputs_as_data_gaps(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    summary = run_decision_matrix_hardening(output_dir=str(out_dir))

    assert summary["status"] == "ready"
    assert summary["matrix_health_status"] == "red"
    assert summary["supports_consistency_and_profitability"] is False
    assert "missing_blocker_audit_artifact" in summary["data_pipeline_gaps"]
    assert "missing_alpha_summary_artifact" in summary["data_pipeline_gaps"]
    assert "missing_weather_pattern_artifact" in summary["data_pipeline_gaps"]
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    assert "missing_weather_pattern_artifact" in blocker_keys
    assert summary["data_sources"]["weather_pattern"]["status"] == "missing"
    assert summary["observed_metrics"]["weather_pattern_status"] == "missing"
    assert summary["observed_metrics"]["weather_risk_off_recommended"] is False
    assert summary["bootstrap_signal"]["observed"]["weather_risk_off_recommended"] is False


def test_decision_matrix_hardening_prefers_health_alpha_summary_over_newer_checkpoints(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    blocker_file = out_dir / "checkpoints" / "blocker_audit_168h_latest.json"
    _write_json(
        blocker_file,
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.60,
                "blocked_total": 100,
                "largest_blocker_count_raw": 60,
            },
        },
    )
    health_alpha = out_dir / "health" / "alpha_summary_latest.json"
    checkpoint_alpha = out_dir / "checkpoints" / "alpha_summary_12h_latest.json"
    _write_json(
        health_alpha,
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 2000,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": True,
                "sparse_hardening_expected_edge_block_share": 0.95,
            },
        },
    )
    _write_json(
        checkpoint_alpha,
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 100,
                "approval_rate": 0.50,
                "settled_unique_market_side_resolved_predictions": 30,
                "projected_pnl_on_reference_bankroll_dollars": 20.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    # Ensure checkpoint is newer than health alpha to verify source-priority behavior.
    os.utime(checkpoint_alpha, None)

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)

    assert summary["status"] == "ready"
    assert summary["observed_metrics"]["intents_total"] == 2000
    assert summary["observed_metrics"]["approval_rate"] == 0.02
    assert summary["source_files"]["alpha_summary"].endswith("health/alpha_summary_latest.json")


def test_decision_matrix_hardening_backfills_settled_outcomes_from_profitability_artifact(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 1000,
                "approval_rate": 0.40,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 8.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260413_050000.json",
        captured_at=now - timedelta(hours=1),
        resolved_unique_market_sides=32,
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}

    assert summary["observed_metrics"]["settled_outcomes"] == 32
    assert summary["observed_metrics"]["settled_outcomes_source"] == "profitability_fallback"
    assert summary["observed_metrics"]["settled_outcomes_source_file"].endswith(
        "kalshi_temperature_profitability_summary_20260413_050000.json"
    )
    assert "insufficient_settled_outcomes" not in blocker_keys


def test_decision_matrix_hardening_flags_stalled_settled_outcome_growth(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 1200,
                "approval_rate": 0.40,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 4.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260405_060000.json",
        captured_at=now - timedelta(days=8),
        resolved_unique_market_sides=7,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260412_000000.json",
        captured_at=now - timedelta(hours=30),
        resolved_unique_market_sides=6,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260413_050000.json",
        captured_at=now - timedelta(hours=1),
        resolved_unique_market_sides=6,
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    backlog_ids = {row["id"] for row in summary["pipeline_backlog"]}

    assert "insufficient_settled_outcomes" in blocker_keys
    assert "settled_outcome_growth_stalled" in blocker_keys
    assert "settled_outcome_growth_recovery" in backlog_ids
    assert summary["observed_metrics"]["settled_outcomes_fallback_trend_delta_24h"] == 0
    assert summary["observed_metrics"]["settled_outcomes_fallback_trend_delta_7d"] == -1


def test_decision_matrix_hardening_does_not_flag_stalled_growth_when_trend_improves(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 1200,
                "approval_rate": 0.40,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 4.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260405_060000.json",
        captured_at=now - timedelta(days=8),
        resolved_unique_market_sides=1,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260412_000000.json",
        captured_at=now - timedelta(hours=30),
        resolved_unique_market_sides=2,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260413_050000.json",
        captured_at=now - timedelta(hours=1),
        resolved_unique_market_sides=6,
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    backlog_ids = {row["id"] for row in summary["pipeline_backlog"]}

    assert "insufficient_settled_outcomes" in blocker_keys
    assert "settled_outcome_growth_stalled" not in blocker_keys
    assert "settled_outcome_growth_recovery" not in backlog_ids
    assert summary["observed_metrics"]["settled_outcomes_fallback_trend_delta_24h"] == 4
    assert summary["observed_metrics"]["settled_outcomes_fallback_trend_delta_7d"] == 5


def test_decision_matrix_hardening_prefers_throughput_growth_evidence_over_negative_fallback(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 1200,
                "approval_rate": 0.40,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 4.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260405_060000.json",
        captured_at=now - timedelta(days=8),
        resolved_unique_market_sides=7,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260412_000000.json",
        captured_at=now - timedelta(hours=30),
        resolved_unique_market_sides=6,
    )
    _write_profitability_summary_artifact(
        out_dir,
        filename="kalshi_temperature_profitability_summary_20260413_050000.json",
        captured_at=now - timedelta(hours=1),
        resolved_unique_market_sides=6,
    )
    _write_settled_outcome_throughput_artifact(
        out_dir,
        filename="kalshi_temperature_settled_outcome_throughput_20260413_050000.json",
        captured_at=now - timedelta(hours=1),
        coverage_settled_outcomes=14,
        settled_outcomes_delta_24h=4,
        settled_outcomes_delta_7d=2,
        combined_bucket_count_delta_24h=3,
        combined_bucket_count_delta_7d=1,
        targeted_constraint_rows=88,
        top_bottlenecks_count=3,
        bottleneck_source="constraint_station_bootstrap",
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}

    assert "settled_outcome_growth_stalled" not in blocker_keys
    assert summary["observed_metrics"]["settled_outcome_growth_source"] == "settled_outcome_throughput"
    assert summary["observed_metrics"]["settled_outcome_growth_source_file"].endswith(
        "kalshi_temperature_settled_outcome_throughput_20260413_050000.json"
    )
    assert summary["observed_metrics"]["settled_outcome_growth_delta_24h"] == 4
    assert summary["observed_metrics"]["settled_outcome_growth_delta_7d"] == 2
    assert summary["observed_metrics"]["settled_outcome_growth_combined_bucket_count_delta_24h"] == 3
    assert summary["observed_metrics"]["settled_outcome_growth_combined_bucket_count_delta_7d"] == 1
    assert summary["observed_metrics"]["settled_outcome_throughput_growth_delta_24h"] == 4
    assert summary["observed_metrics"]["settled_outcome_throughput_growth_delta_7d"] == 2


def test_decision_matrix_hardening_surfaces_throughput_metrics_in_data_sources_and_observed_metrics(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 1000,
                "approval_rate": 0.40,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 8.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    throughput_path = _write_settled_outcome_throughput_artifact(
        out_dir,
        filename="kalshi_temperature_settled_outcome_throughput_latest.json",
        captured_at=now - timedelta(hours=2),
        coverage_settled_outcomes=19,
        settled_outcomes_delta_24h=5,
        settled_outcomes_delta_7d=9,
        combined_bucket_count_delta_24h=4,
        combined_bucket_count_delta_7d=8,
        targeted_constraint_rows=144,
        top_bottlenecks_count=2,
        bottleneck_source="constraint_station_bootstrap",
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)

    assert summary["data_sources"]["settled_outcome_throughput"]["status"] == "ready"
    assert summary["data_sources"]["settled_outcome_throughput"]["source"].endswith(
        throughput_path.name
    )
    assert summary["data_sources"]["settled_outcome_throughput"]["coverage_settled_outcomes"] == 19
    assert summary["data_sources"]["settled_outcome_throughput"]["growth_deltas_settled_outcomes_delta_24h"] == 5
    assert summary["data_sources"]["settled_outcome_throughput"]["growth_deltas_combined_bucket_count_delta_24h"] == 4
    assert summary["data_sources"]["settled_outcome_throughput"]["targeting_targeted_constraint_rows"] == 144
    assert summary["data_sources"]["settled_outcome_throughput"]["top_bottlenecks_count"] == 2
    assert summary["data_sources"]["settled_outcome_throughput"]["bottleneck_source"] == "constraint_station_bootstrap"
    assert summary["observed_metrics"]["settled_outcome_throughput_status"] == "ready"
    assert summary["observed_metrics"]["settled_outcome_throughput_coverage_settled_outcomes"] == 19
    assert summary["observed_metrics"]["settled_outcome_throughput_targeted_constraint_rows"] == 144
    assert summary["observed_metrics"]["settled_outcome_throughput_top_bottlenecks_count"] == 2
    assert summary["observed_metrics"]["settled_outcome_throughput_bottleneck_source"] == "constraint_station_bootstrap"


def test_decision_matrix_hardening_tracks_coverage_velocity_guardrail_across_runs(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_low_pressure_non_weather_inputs(out_dir)
    throughput_path = _write_settled_outcome_throughput_artifact(
        out_dir,
        filename="kalshi_temperature_settled_outcome_throughput_latest.json",
        captured_at=now - timedelta(hours=1),
        coverage_settled_outcomes=30,
        settled_outcomes_delta_24h=-1,
        settled_outcomes_delta_7d=0,
        combined_bucket_count_delta_24h=-2,
        combined_bucket_count_delta_7d=0,
        targeted_constraint_rows=88,
        top_bottlenecks_count=2,
        bottleneck_source="constraint_station_bootstrap",
    )

    first_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    first_blocker_keys = {row["key"] for row in first_summary["blocking_factors"]}
    assert "coverage_velocity_guardrail_not_cleared" in first_blocker_keys
    assert first_summary["supports_consistency_and_profitability"] is False
    assert first_summary["observed_metrics"]["coverage_velocity_guardrail_active"] is True
    assert first_summary["observed_metrics"]["coverage_velocity_guardrail_cleared"] is False
    assert first_summary["observed_metrics"]["coverage_velocity_positive_streak"] == 0
    assert first_summary["observed_metrics"]["coverage_velocity_non_positive_streak"] == 1
    assert first_summary["observed_metrics"]["coverage_velocity_required_positive_streak"] == 2
    assert first_summary["observed_metrics"]["coverage_velocity_selected_growth_delta_24h"] == -1
    assert first_summary["observed_metrics"]["coverage_velocity_selected_combined_bucket_count_delta_24h"] == -2
    first_state = json.loads(
        (out_dir / "health" / "decision_matrix_coverage_velocity_state_latest.json").read_text(encoding="utf-8")
    )
    assert first_state["source"].endswith("decision_matrix_coverage_velocity_state_latest.json")
    assert first_state["positive_streak"] == 0
    assert first_state["non_positive_streak"] == 1
    assert first_state["required_positive_streak"] == 2
    assert first_state["guardrail_active"] is True
    assert first_state["guardrail_cleared"] is False

    second_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    second_blocker_keys = {row["key"] for row in second_summary["blocking_factors"]}
    assert "coverage_velocity_guardrail_not_cleared" in second_blocker_keys
    second_state = json.loads(
        (out_dir / "health" / "decision_matrix_coverage_velocity_state_latest.json").read_text(encoding="utf-8")
    )
    assert second_state["positive_streak"] == 0
    assert second_state["non_positive_streak"] == 2
    assert second_state["guardrail_active"] is True
    assert second_state["guardrail_cleared"] is False

    _write_settled_outcome_throughput_artifact(
        out_dir,
        filename="kalshi_temperature_settled_outcome_throughput_latest.json",
        captured_at=now - timedelta(minutes=30),
        coverage_settled_outcomes=32,
        settled_outcomes_delta_24h=3,
        settled_outcomes_delta_7d=5,
        combined_bucket_count_delta_24h=2,
        combined_bucket_count_delta_7d=4,
        targeted_constraint_rows=96,
        top_bottlenecks_count=1,
        bottleneck_source="constraint_station_bootstrap",
    )

    third_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    third_blocker_keys = {row["key"] for row in third_summary["blocking_factors"]}
    assert "coverage_velocity_guardrail_not_cleared" in third_blocker_keys
    assert third_summary["observed_metrics"]["coverage_velocity_guardrail_active"] is True
    assert third_summary["observed_metrics"]["coverage_velocity_guardrail_cleared"] is False
    assert third_summary["observed_metrics"]["coverage_velocity_positive_streak"] == 1
    assert third_summary["observed_metrics"]["coverage_velocity_non_positive_streak"] == 0
    third_state = json.loads(
        (out_dir / "health" / "decision_matrix_coverage_velocity_state_latest.json").read_text(encoding="utf-8")
    )
    assert third_state["positive_streak"] == 1
    assert third_state["non_positive_streak"] == 0
    assert third_state["guardrail_active"] is True
    assert third_state["guardrail_cleared"] is False

    fourth_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    fourth_blocker_keys = {row["key"] for row in fourth_summary["blocking_factors"]}
    assert "coverage_velocity_guardrail_not_cleared" not in fourth_blocker_keys
    assert fourth_summary["supports_consistency_and_profitability"] is True
    assert fourth_summary["observed_metrics"]["coverage_velocity_guardrail_active"] is False
    assert fourth_summary["observed_metrics"]["coverage_velocity_guardrail_cleared"] is True
    assert fourth_summary["observed_metrics"]["coverage_velocity_positive_streak"] == 2
    assert fourth_summary["observed_metrics"]["coverage_velocity_non_positive_streak"] == 0
    fourth_state = json.loads(
        (out_dir / "health" / "decision_matrix_coverage_velocity_state_latest.json").read_text(encoding="utf-8")
    )
    assert fourth_state["positive_streak"] == 2
    assert fourth_state["non_positive_streak"] == 0
    assert fourth_state["guardrail_active"] is False
    assert fourth_state["guardrail_cleared"] is True

    backlog_ids = {row["id"] for row in first_summary["pipeline_backlog"]}
    assert "coverage_velocity_recovery" in backlog_ids


def test_decision_matrix_hardening_flags_missing_execution_cost_tape_for_expected_edge_blocker(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.61,
                "blocked_total": 4000,
                "largest_blocker_count_raw": 2440,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 4000,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.95,
                "top_blocker_reason": "expected_edge_below_min",
                "top_blocker_share_of_blocked": 0.61,
            },
        },
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    assert summary["status"] == "ready"
    assert "missing_execution_cost_tape_artifact" in summary["data_pipeline_gaps"]


def test_decision_matrix_hardening_emits_bootstrap_progression_for_cold_start_expected_edge(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.60,
                "blocked_total": 10000,
                "largest_blocker_count_raw": 6000,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 3000,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": True,
                "sparse_hardening_expected_edge_block_share": 0.99,
                "top_blocker_reason": "expected_edge_below_min",
                "top_blocker_share_of_blocked": 0.60,
            },
        },
    )
    _write_json(
        out_dir / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "calibration_readiness": {
                "status": "red",
                "candidate_rows": 10,
                "quote_coverage_ratio": 0.10,
                "meets_candidate_samples": False,
                "meets_quote_coverage": False,
            },
        },
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    bootstrap_signal = summary["bootstrap_signal"]
    assert summary["supports_bootstrap_progression"] is True
    assert bootstrap_signal["status"] == "ready"
    assert bootstrap_signal["supports_bootstrap_progression"] is True
    assert bootstrap_signal["disallowed_blocker_keys"] == []


def test_decision_matrix_hardening_blocks_bootstrap_on_disallowed_blocker(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_healthy_weather_pattern_artifact(out_dir, now)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "historical quality signal type hard block",
                "largest_blocker_share_of_blocked_raw": 0.76,
                "blocked_total": 5000,
                "largest_blocker_count_raw": 3800,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 3500,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.95,
                "top_blocker_reason": "historical quality signal type hard block",
                "top_blocker_share_of_blocked": 0.76,
            },
        },
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    bootstrap_signal = summary["bootstrap_signal"]
    assert summary["supports_bootstrap_progression"] is False
    assert bootstrap_signal["status"] == "blocked"
    assert "historical_quality_hard_block_dominance" in bootstrap_signal["disallowed_blocker_keys"]


def test_decision_matrix_hardening_flags_stale_weather_pattern_artifact(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)

    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 100,
                "approval_rate": 0.50,
                "settled_unique_market_side_resolved_predictions": 30,
                "projected_pnl_on_reference_bankroll_dollars": 12.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_weather_pattern_json(
        out_dir / "health" / "kalshi_temperature_weather_pattern_20260410_000000.json",
        {
            "status": "ready",
            "captured_at_utc": "2026-04-10T00:00:00+00:00",
            "negative_expectancy_regime_concentration": 0.12,
            "negative_expectancy_regime_count": 6,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
        },
        ts=(now - timedelta(hours=72)).timestamp(),
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    assert "stale_weather_pattern_artifact" in blocker_keys
    assert summary["data_sources"]["weather_pattern"]["status"] == "ready"
    assert summary["data_sources"]["weather_pattern"]["age_hours"] >= 72.0
    assert summary["observed_metrics"]["weather_pattern_status"] == "ready"
    assert summary["observed_metrics"]["weather_pattern_age_hours"] >= 72.0
    backlog_ids = {row["id"] for row in summary["pipeline_backlog"]}
    assert "weather_pattern_refresh" in backlog_ids


def test_decision_matrix_hardening_surfaces_weather_blockers_only_when_regime_is_poor(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)

    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "other_pressure",
                "largest_blocker_share_of_blocked_raw": 0.10,
                "blocked_total": 20,
                "largest_blocker_count_raw": 2,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 100,
                "approval_rate": 0.50,
                "settled_unique_market_side_resolved_predictions": 30,
                "projected_pnl_on_reference_bankroll_dollars": 12.0,
                "quality_drift_alert_active": False,
                "sparse_hardening_expected_edge_block_share": 0.10,
            },
        },
    )
    _write_healthy_weather_pattern_artifact(
        out_dir,
        now,
        negative_expectancy_regime_concentration=0.12,
        weather_bucket_coverage_ratio=0.91,
        age_hours=1.0,
    )

    healthy_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    healthy_blocker_keys = {row["key"] for row in healthy_summary["blocking_factors"]}
    assert "weather_negative_expectancy_regime_concentration_high" not in healthy_blocker_keys
    assert "weather_bucket_coverage_insufficient" not in healthy_blocker_keys
    assert healthy_summary["data_sources"]["weather_pattern"]["status"] == "ready"
    assert healthy_summary["data_sources"]["weather_pattern"]["age_hours"] < 2.0

    _write_healthy_weather_pattern_artifact(
        out_dir,
        now,
        negative_expectancy_regime_concentration=0.82,
        weather_bucket_coverage_ratio=0.31,
        age_hours=1.0,
    )
    poor_summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    poor_blocker_keys = {row["key"] for row in poor_summary["blocking_factors"]}
    assert "weather_negative_expectancy_regime_concentration_high" in poor_blocker_keys
    assert "weather_bucket_coverage_insufficient" in poor_blocker_keys
    backlog_ids = {row["id"] for row in poor_summary["pipeline_backlog"]}
    assert "weather_regime_deconcentration" in backlog_ids
    assert "weather_bucket_coverage_expansion" in backlog_ids


def test_decision_matrix_hardening_flags_weather_global_risk_off_recommended(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.60,
                "blocked_total": 10000,
                "largest_blocker_count_raw": 6000,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 3000,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": True,
                "sparse_hardening_expected_edge_block_share": 0.99,
                "top_blocker_reason": "expected_edge_below_min",
                "top_blocker_share_of_blocked": 0.60,
            },
        },
    )
    _write_json(
        out_dir / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "calibration_readiness": {
                "status": "red",
                "candidate_rows": 10,
                "quote_coverage_ratio": 0.10,
                "meets_candidate_samples": False,
                "meets_quote_coverage": False,
            },
        },
    )
    _write_healthy_weather_pattern_artifact(
        out_dir,
        now,
        negative_expectancy_regime_concentration=0.82,
        weather_bucket_coverage_ratio=0.91,
        metar_observation_stale_share=0.84,
        weather_risk_off_sample_count=640,
        age_hours=1.0,
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    bootstrap_signal = summary["bootstrap_signal"]

    assert "weather_global_risk_off_recommended" in blocker_keys
    assert summary["observed_metrics"]["weather_risk_off_recommended"] is True
    assert summary["observed_metrics"]["weather_metar_observation_stale_share"] == 0.84
    assert summary["observed_metrics"]["weather_risk_off_sample_count"] == 640
    assert summary["supports_bootstrap_progression"] is False
    assert "weather_risk_off_recommended" in bootstrap_signal["reasons"]
    assert bootstrap_signal["observed"]["weather_risk_off_recommended"] is True


def test_decision_matrix_hardening_does_not_flag_weather_global_risk_off_without_sample_floor(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.60,
                "blocked_total": 10000,
                "largest_blocker_count_raw": 6000,
            },
        },
    )
    _write_json(
        out_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "headline_metrics": {
                "intents_total": 3000,
                "approval_rate": 0.02,
                "settled_unique_market_side_resolved_predictions": 0,
                "projected_pnl_on_reference_bankroll_dollars": 0.0,
                "quality_drift_alert_active": True,
                "sparse_hardening_expected_edge_block_share": 0.99,
                "top_blocker_reason": "expected_edge_below_min",
                "top_blocker_share_of_blocked": 0.60,
            },
        },
    )
    _write_json(
        out_dir / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "calibration_readiness": {
                "status": "red",
                "candidate_rows": 10,
                "quote_coverage_ratio": 0.10,
                "meets_candidate_samples": False,
                "meets_quote_coverage": False,
            },
        },
    )
    _write_healthy_weather_pattern_artifact(
        out_dir,
        now,
        negative_expectancy_regime_concentration=0.82,
        weather_bucket_coverage_ratio=0.91,
        metar_observation_stale_share=0.84,
        weather_risk_off_sample_count=80,
        age_hours=1.0,
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}
    bootstrap_signal = summary["bootstrap_signal"]

    assert "weather_global_risk_off_recommended" not in blocker_keys
    assert summary["observed_metrics"]["weather_risk_off_recommended"] is False
    assert summary["observed_metrics"]["weather_metar_observation_stale_share"] == 0.84
    assert summary["observed_metrics"]["weather_risk_off_sample_count"] == 80
    assert "weather_risk_off_recommended" not in bootstrap_signal["reasons"]
    assert bootstrap_signal["observed"]["weather_risk_off_recommended"] is False


def test_decision_matrix_hardening_prioritizes_confidence_adjusted_weather_metrics(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_low_pressure_non_weather_inputs(out_dir)
    _write_weather_pattern_json(
        out_dir / "health" / "kalshi_temperature_weather_pattern_latest.json",
        {
            "status": "ready",
            "captured_at_utc": now.isoformat(),
            "negative_expectancy_regime_concentration": 0.82,
            "negative_expectancy_regime_count": 41,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
            "metar_observation_stale_share": 0.84,
            "metar_observation_stale_count": 538,
            "weather_risk_off_sample_count": 640,
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.82,
                    "negative_expectancy_attempt_share_confidence_adjusted": 0.24,
                    "stale_metar_attempt_share": 0.84,
                    "stale_metar_attempt_share_confidence_adjusted": 0.28,
                }
            },
        },
        ts=(now - timedelta(hours=1)).timestamp(),
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}

    assert "weather_negative_expectancy_regime_concentration_high" not in blocker_keys
    assert "weather_global_risk_off_recommended" not in blocker_keys
    assert summary["observed_metrics"]["weather_negative_expectancy_regime_concentration"] == 0.24
    assert summary["observed_metrics"]["weather_metar_observation_stale_share"] == 0.28
    assert summary["observed_metrics"]["weather_risk_off_sample_count"] == 640
    assert summary["observed_metrics"]["weather_risk_off_recommended"] is False


def test_decision_matrix_hardening_falls_back_to_legacy_weather_metrics(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_low_pressure_non_weather_inputs(out_dir)
    _write_weather_pattern_json(
        out_dir / "health" / "kalshi_temperature_weather_pattern_latest.json",
        {
            "status": "ready",
            "captured_at_utc": now.isoformat(),
            "negative_expectancy_regime_concentration": 0.82,
            "negative_expectancy_regime_count": 41,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
            "metar_observation_stale_share": 0.84,
            "metar_observation_stale_count": 538,
            "weather_risk_off_sample_count": 640,
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_attempt_share": 0.28,
                }
            },
        },
        ts=(now - timedelta(hours=1)).timestamp(),
    )

    summary = run_decision_matrix_hardening(output_dir=str(out_dir), now=now)
    blocker_keys = {row["key"] for row in summary["blocking_factors"]}

    assert "weather_negative_expectancy_regime_concentration_high" in blocker_keys
    assert "weather_global_risk_off_recommended" in blocker_keys
    assert summary["observed_metrics"]["weather_negative_expectancy_regime_concentration"] == 0.82
    assert summary["observed_metrics"]["weather_metar_observation_stale_share"] == 0.84
    assert summary["observed_metrics"]["weather_risk_off_sample_count"] == 640
    assert summary["observed_metrics"]["weather_risk_off_recommended"] is True


def test_decision_matrix_hardening_flags_persistent_raw_confidence_adjusted_fallback(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_low_pressure_non_weather_inputs(out_dir)
    _write_weather_pattern_json(
        out_dir / "health" / "kalshi_temperature_weather_pattern_latest.json",
        {
            "status": "ready",
            "captured_at_utc": now.isoformat(),
            "negative_expectancy_regime_concentration": 0.24,
            "negative_expectancy_regime_count": 12,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
            "metar_observation_stale_share": 0.28,
            "metar_observation_stale_count": 179,
            "weather_risk_off_sample_count": 640,
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_attempt_share": 0.28,
                }
            },
        },
        ts=(now - timedelta(hours=1)).timestamp(),
    )

    first_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=3,
    )
    first_blocker_keys = {row["key"] for row in first_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" not in first_blocker_keys
    first_state = json.loads(
        (out_dir / "health" / "decision_matrix_weather_confidence_state_latest.json").read_text(encoding="utf-8")
    )
    assert first_state["raw_fallback_active"] is True
    assert first_state["raw_fallback_consecutive_count"] == 1
    assert first_state["raw_fallback_persistent"] is False
    assert first_state["raw_fallback_consecutive_threshold"] == 3

    second_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=3,
    )
    second_blocker_keys = {row["key"] for row in second_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" not in second_blocker_keys
    second_state = json.loads(
        (out_dir / "health" / "decision_matrix_weather_confidence_state_latest.json").read_text(encoding="utf-8")
    )
    assert second_state["raw_fallback_active"] is True
    assert second_state["raw_fallback_consecutive_count"] == 2
    assert second_state["raw_fallback_persistent"] is False

    third_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=3,
    )
    blocker_keys = {row["key"] for row in third_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" in blocker_keys
    backlog_ids = {row["id"] for row in third_summary["pipeline_backlog"]}
    assert "weather_confidence_adjusted_signal_repair" in backlog_ids

    fallback_blocker = next(
        row
        for row in third_summary["blocking_factors"]
        if row.get("key") == "weather_confidence_adjusted_signal_fallback_persistent"
    )
    observed = fallback_blocker["observed_value"]
    threshold = fallback_blocker["threshold"]
    assert observed["negative_expectancy_regime_concentration_source"] == "raw"
    assert observed["metar_observation_stale_share_source"] == "raw"
    assert observed["raw_fallback_consecutive_count"] == 3
    assert threshold["raw_fallback_consecutive_threshold"] == 3
    assert threshold["requires_confidence_adjusted_source"] is True

    third_state = json.loads(
        (out_dir / "health" / "decision_matrix_weather_confidence_state_latest.json").read_text(encoding="utf-8")
    )
    assert third_state["raw_fallback_active"] is True
    assert third_state["raw_fallback_consecutive_count"] == 3
    assert third_state["raw_fallback_persistent"] is True
    assert third_state["raw_fallback_consecutive_threshold"] == 3


def test_decision_matrix_hardening_resets_raw_fallback_streak_when_confidence_adjusted_metrics_return(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 13, 6, 0, tzinfo=timezone.utc)
    _write_low_pressure_non_weather_inputs(out_dir)
    weather_artifact_path = out_dir / "health" / "kalshi_temperature_weather_pattern_latest.json"
    _write_weather_pattern_json(
        weather_artifact_path,
        {
            "status": "ready",
            "captured_at_utc": now.isoformat(),
            "negative_expectancy_regime_concentration": 0.24,
            "negative_expectancy_regime_count": 12,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
            "metar_observation_stale_share": 0.28,
            "metar_observation_stale_count": 179,
            "weather_risk_off_sample_count": 640,
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_attempt_share": 0.28,
                }
            },
        },
        ts=(now - timedelta(hours=1)).timestamp(),
    )

    first_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=2,
    )
    first_blocker_keys = {row["key"] for row in first_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" not in first_blocker_keys

    second_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=2,
    )
    second_blocker_keys = {row["key"] for row in second_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" in second_blocker_keys
    second_backlog_ids = {row["id"] for row in second_summary["pipeline_backlog"]}
    assert "weather_confidence_adjusted_signal_repair" in second_backlog_ids

    second_state = json.loads(
        (out_dir / "health" / "decision_matrix_weather_confidence_state_latest.json").read_text(encoding="utf-8")
    )
    assert second_state["raw_fallback_active"] is True
    assert second_state["raw_fallback_consecutive_count"] == 2
    assert second_state["raw_fallback_persistent"] is True
    assert second_state["raw_fallback_consecutive_threshold"] == 2

    _write_weather_pattern_json(
        weather_artifact_path,
        {
            "status": "ready",
            "captured_at_utc": now.isoformat(),
            "negative_expectancy_regime_concentration": 0.82,
            "negative_expectancy_regime_count": 41,
            "negative_expectancy_regime_total": 50,
            "weather_bucket_coverage_ratio": 0.92,
            "weather_bucket_coverage_count": 46,
            "weather_bucket_total": 50,
            "metar_observation_stale_share": 0.84,
            "metar_observation_stale_count": 538,
            "weather_risk_off_sample_count": 640,
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.82,
                    "negative_expectancy_attempt_share_confidence_adjusted": 0.24,
                    "stale_metar_attempt_share": 0.84,
                    "stale_metar_attempt_share_confidence_adjusted": 0.28,
                }
            },
        },
        ts=(now - timedelta(hours=1)).timestamp(),
    )

    reset_summary = run_decision_matrix_hardening(
        output_dir=str(out_dir),
        now=now,
        weather_confidence_adjusted_fallback_consecutive_threshold=2,
    )
    reset_blocker_keys = {row["key"] for row in reset_summary["blocking_factors"]}
    assert "weather_confidence_adjusted_signal_fallback_persistent" not in reset_blocker_keys
    reset_backlog_ids = {row["id"] for row in reset_summary["pipeline_backlog"]}
    assert "weather_confidence_adjusted_signal_repair" not in reset_backlog_ids
    assert reset_summary["observed_metrics"]["weather_negative_expectancy_regime_concentration"] == 0.24
    assert reset_summary["observed_metrics"]["weather_metar_observation_stale_share"] == 0.28

    reset_state = json.loads(
        (out_dir / "health" / "decision_matrix_weather_confidence_state_latest.json").read_text(encoding="utf-8")
    )
    assert reset_state["raw_fallback_active"] is False
    assert reset_state["raw_fallback_consecutive_count"] == 0
    assert reset_state["raw_fallback_persistent"] is False
    assert reset_state["raw_fallback_consecutive_threshold"] == 2
    assert reset_state["negative_expectancy_regime_concentration_source"] == "confidence_adjusted"
    assert reset_state["metar_observation_stale_share_source"] == "confidence_adjusted"
