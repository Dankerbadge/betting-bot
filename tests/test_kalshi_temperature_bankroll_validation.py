from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from unittest.mock import patch

import betbot.kalshi_temperature_bankroll_validation as bankroll_validation_module
from betbot.kalshi_temperature_bankroll_validation import (
    TemperatureOpportunity,
    _build_growth_readiness_block,
    _dedupe_opportunities,
    _derive_threshold_yes_outcome,
    _infer_main_limiting_factor,
    _latest_file_preferring_window,
    _load_json_input,
    _resolve_slippage_bps_list,
    _simulate_bankroll_for_layer,
    _build_overall_live_decision,
    run_kalshi_temperature_alpha_gap_report,
    run_kalshi_temperature_bankroll_validation,
    run_kalshi_temperature_go_live_gate,
    run_kalshi_temperature_live_readiness,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _set_file_time(path: Path, ts: float) -> None:
    os.utime(path, (ts, ts))


def _growth_readiness_payload(
    *,
    resolved_rows: int,
    resolved_market_sides: int,
    resolved_families: int,
    pipeline_status: str,
    main_limiting_factor: str,
    roi_on_reference_bankroll: float,
    excess_return_over_hysa: float,
    equivalent_hysa_return: float,
    calibration_ratio: float | None,
    calibration_trade_count: int,
    concentration_warning: bool,
    unresolved_market_sides: int = 0,
    repeated_entry_multiplier: float = 1.0,
    stale_feeds: list[str] | None = None,
    missing_feeds: list[str] | None = None,
    out_of_window_feeds: list[str] | None = None,
) -> dict[str, object]:
    stale_feeds = stale_feeds or []
    missing_feeds = missing_feeds or []
    out_of_window_feeds = out_of_window_feeds or []
    return {
        "viability_summary": {
            "main_limiting_factor": main_limiting_factor,
            "what_return_would_have_been_produced_on_bankroll": roi_on_reference_bankroll,
            "excess_return_over_hysa_for_window": excess_return_over_hysa,
            "equivalent_window_hysa_return_on_reference_bankroll": equivalent_hysa_return,
        },
        "opportunity_breadth": {
            "resolved_planned_rows": resolved_rows,
            "resolved_unique_market_sides": resolved_market_sides,
            "resolved_unique_underlying_families": resolved_families,
            "unresolved_unique_market_sides": unresolved_market_sides,
            "repeated_entry_multiplier": repeated_entry_multiplier,
        },
        "concentration_checks": {
            "concentration_warning": concentration_warning,
        },
        "data_quality": {
            "pipeline_status": pipeline_status,
            "pipeline_health": {
                "status": pipeline_status,
                "missing_feeds": missing_feeds,
                "stale_feeds": stale_feeds,
                "out_of_window_feeds": out_of_window_feeds,
            },
        },
        "expected_vs_shadow_settled": {
            "calibration_ratio": calibration_ratio,
            "trade_count": calibration_trade_count,
        },
        "hit_rate_quality": {
            "wins": calibration_trade_count,
            "losses": 0,
            "pushes": 0,
            "expectancy_per_trade": 0.1 if resolved_rows > 0 else 0.0,
        },
        "signal_evidence": {
            "pipeline_health": {
                "status": pipeline_status,
                "missing_feeds": missing_feeds,
                "stale_feeds": stale_feeds,
                "out_of_window_feeds": out_of_window_feeds,
            }
        },
    }


def _seed_temperature_artifacts(out_dir: Path, now: datetime) -> None:
    artifact_stamp = (now - timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")
    specs_csv = out_dir / f"kalshi_temperature_contract_specs_{artifact_stamp}.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [
            {"market_ticker": "KXHIGHMIA-26APR13-B77.5", "threshold_expression": "above:77.5"},
            {"market_ticker": "KXHIGHNY-26APR13-B74.5", "threshold_expression": "above:74.5"},
        ],
    )

    intents_csv = out_dir / f"kalshi_temperature_trade_intents_{artifact_stamp}.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-1",
                "underlying_key": "KXHIGHMIA|KMIA|2026-04-13",
                "series_ticker": "KXHIGHMIA",
                "settlement_station": "KMIA",
                "target_date_local": "2026-04-13",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "13",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-13T18:00:00+00:00",
                "hours_to_close": "2",
            },
            {
                "intent_id": "intent-2",
                "underlying_key": "KXHIGHNY|KNYC|2026-04-13",
                "series_ticker": "KXHIGHNY",
                "settlement_station": "KNYC",
                "target_date_local": "2026-04-13",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-13T19:00:00+00:00",
                "hours_to_close": "3",
            },
            {
                "intent_id": "intent-3",
                "underlying_key": "KXHIGHPHIL|KPHL|2026-04-13",
                "series_ticker": "KXHIGHPHIL",
                "settlement_station": "KPHL",
                "target_date_local": "2026-04-13",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "15",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "metar_observation_stale",
                "close_time": "2026-04-13T20:00:00+00:00",
                "hours_to_close": "4",
            },
        ],
    )

    plan_csv = out_dir / f"kalshi_temperature_trade_plan_{artifact_stamp}.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
            "hours_to_close",
        ],
        [
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-1",
                "temperature_client_order_id": "temp-dup-1",
                "market_ticker": "KXHIGHMIA-26APR13-B77.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
                "hours_to_close": "2",
            },
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-1",
                "temperature_client_order_id": "temp-dup-1",
                "market_ticker": "KXHIGHMIA-26APR13-B77.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
                "hours_to_close": "2",
            },
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-2",
                "temperature_client_order_id": "temp-ny-1",
                "market_ticker": "KXHIGHNY-26APR13-B74.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
                "hours_to_close": "3",
            },
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-3",
                "temperature_client_order_id": "temp-phil-1",
                "market_ticker": "KXHIGHPHIL-26APR13-B78.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
                "hours_to_close": "4",
            },
        ],
    )

    settlement_json = out_dir / f"kalshi_temperature_settlement_state_{artifact_stamp}.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHMIA|KMIA|2026-04-13": {"final_truth_value": 75.0},
                    "KXHIGHNY|KNYC|2026-04-13": {"final_truth_value": 70.0},
                }
            }
        ),
        encoding="utf-8",
    )

    ts = (now - timedelta(hours=1)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, ts)


def test_bankroll_validation_enforces_truth_first_layers(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 3
    assert summary["opportunity_breadth"]["resolved_unique_shadow_orders"] == 2
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 2
    assert summary["opportunity_breadth"]["unresolved_unique_market_sides"] == 1
    assert summary["concentration_checks"]["concentration_warning"] is True
    assert summary["concentration_checks"]["duplicate_count"] == 1

    anti = summary["anti_misleading_guards"]
    assert anti["default_prediction_quality_basis"] == "unique_market_side"
    assert anti["default_deployment_quality_basis"] == "underlying_family_aggregated"
    assert anti["shadow_settled_is_not_live"] is True

    data_quality = summary["data_quality"]
    assert data_quality["pipeline_status"] in {"green", "yellow", "red", "unknown"}
    assert isinstance(data_quality["pipeline_health"], dict)
    assert data_quality["settlement_backlog_now"]["current_settlement_unresolved"] == 0
    assert data_quality["settlement_backlog_now"]["settlement_backlog_clear"] is True
    assert data_quality["settlement_state_file_used"]
    assert data_quality["constraint_scan_summary_file_used"] == ""
    assert data_quality["constraint_scan_summary_in_window"] is False
    assert data_quality["trade_plan_summary_file_used"] == ""
    assert data_quality["trade_plan_summary_in_window"] is False

    viability = summary["viability_summary"]
    assert viability["hysa_comparison_assumption_annual_rate"] is not None
    assert viability["equivalent_daily_hysa_return_on_reference_bankroll"] is not None
    assert viability["equivalent_window_hysa_return_on_reference_bankroll"] is not None
    assert viability["excess_return_over_hysa_for_window"] is not None
    assert viability["deployment_headline_basis"]["slippage_bps"] == 10.0

    simulation = summary["bankroll_simulation"]["by_model"]["fixed_fraction_per_underlying_family"]["by_slippage_bps"]["0.0"]
    family_metrics = simulation["underlying_family_aggregated"]
    assert family_metrics["roi_on_deployed_capital"] is not None
    assert family_metrics["roi_on_reference_bankroll"] is not None
    assert family_metrics["trade_count"] >= 1
    headline = summary["bankroll_simulation"]["by_model"]["fixed_fraction_per_underlying_family"][
        "headline_deployment_quality"
    ]
    assert headline["slippage_bps"] == 10.0
    assert summary["attribution"]["fixed_fraction_per_underlying_family"]["slippage_bps"] == 10.0
    assert "alpha_feature_density" in summary
    assert "unique_market_side" in summary["alpha_feature_density"]

    assert Path(summary["output_file"]).exists()


def test_growth_readiness_scores_improve_for_fresher_broader_positive_edge(tmp_path: Path) -> None:
    healthy = _build_growth_readiness_block(
        _growth_readiness_payload(
            resolved_rows=12,
            resolved_market_sides=18,
            resolved_families=5,
            pipeline_status="green",
            main_limiting_factor="insufficient_breadth",
            roi_on_reference_bankroll=0.08,
            excess_return_over_hysa=24.0,
            equivalent_hysa_return=2.0,
            calibration_ratio=1.02,
            calibration_trade_count=12,
            concentration_warning=False,
            repeated_entry_multiplier=1.0,
        )
    )
    stale = _build_growth_readiness_block(
        _growth_readiness_payload(
            resolved_rows=0,
            resolved_market_sides=0,
            resolved_families=0,
            pipeline_status="red",
            main_limiting_factor="stale_suppression",
            roi_on_reference_bankroll=-0.04,
            excess_return_over_hysa=-12.0,
            equivalent_hysa_return=1.0,
            calibration_ratio=None,
            calibration_trade_count=0,
            concentration_warning=False,
            stale_feeds=["metar_summary"],
        )
    )

    assert healthy["readiness_score"] > stale["readiness_score"]
    assert healthy["throughput_score"] > stale["throughput_score"]
    assert healthy["edge_quality_score"] > stale["edge_quality_score"]
    assert healthy["data_freshness_score"] > stale["data_freshness_score"]
    assert healthy["calibration_score"] > stale["calibration_score"]
    assert healthy["top_blockers"] == []
    assert stale["top_blockers"][0]["reason"] == "stale_suppression"


def test_growth_readiness_orders_blockers_by_operational_severity(tmp_path: Path) -> None:
    narrow = _build_growth_readiness_block(
        _growth_readiness_payload(
            resolved_rows=2,
            resolved_market_sides=2,
            resolved_families=1,
            pipeline_status="green",
            main_limiting_factor="insufficient_breadth",
            roi_on_reference_bankroll=0.05,
            excess_return_over_hysa=6.0,
            equivalent_hysa_return=1.0,
            calibration_ratio=1.0,
            calibration_trade_count=4,
            concentration_warning=False,
            repeated_entry_multiplier=1.0,
        )
    )
    stale_no_outcomes = _build_growth_readiness_block(
        _growth_readiness_payload(
            resolved_rows=0,
            resolved_market_sides=0,
            resolved_families=0,
            pipeline_status="red",
            main_limiting_factor="stale_suppression",
            roi_on_reference_bankroll=-0.02,
            excess_return_over_hysa=-3.0,
            equivalent_hysa_return=1.0,
            calibration_ratio=None,
            calibration_trade_count=0,
            concentration_warning=False,
        )
    )

    assert [entry["reason"] for entry in narrow["top_blockers"][:2]] == [
        "insufficient_independent_market_side_breadth",
        "insufficient_underlying_family_breadth",
    ]
    assert [entry["reason"] for entry in stale_no_outcomes["top_blockers"][:2]] == [
        "stale_suppression",
        "no_resolved_outcomes",
    ]
    assert narrow["top_blockers"][0]["score"] >= narrow["top_blockers"][1]["score"]
    assert stale_no_outcomes["top_blockers"][0]["score"] >= stale_no_outcomes["top_blockers"][1]["score"]


def test_bankroll_validation_uses_artifact_timestamp_when_mtime_is_stale(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260414_115500.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [{"market_ticker": "KXHIGHMIA-26APR14-B77.5", "threshold_expression": "above:77.5"}],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260414_115500.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-ts-1",
                "underlying_key": "KXHIGHMIA|KMIA|2026-04-14",
                "series_ticker": "KXHIGHMIA",
                "settlement_station": "KMIA",
                "target_date_local": "2026-04-14",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "11",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-14T18:00:00+00:00",
                "hours_to_close": "2",
            }
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260414_115500.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
        ],
        [
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-ts-1",
                "temperature_client_order_id": "temp-ts-1",
                "market_ticker": "KXHIGHMIA-26APR14-B77.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
            }
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260414_115500.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHMIA|KMIA|2026-04-14": {"final_truth_value": 75.0},
                }
            }
        ),
        encoding="utf-8",
    )

    stale_ts = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, stale_ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 1
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 1
    assert summary["opportunity_breadth"]["resolved_unique_underlying_families"] == 1


def test_bankroll_validation_excludes_stale_artifact_timestamp_even_if_mtime_recent(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260401_010000.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [{"market_ticker": "KXHIGHMIA-26APR14-B77.5", "threshold_expression": "above:77.5"}],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260401_010000.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-old-1",
                "underlying_key": "KXHIGHMIA|KMIA|2026-04-14",
                "series_ticker": "KXHIGHMIA",
                "settlement_station": "KMIA",
                "target_date_local": "2026-04-14",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "11",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-14T18:00:00+00:00",
                "hours_to_close": "2",
            }
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260401_010000.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
        ],
        [
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-old-1",
                "temperature_client_order_id": "temp-old-1",
                "market_ticker": "KXHIGHMIA-26APR14-B77.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
            }
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260401_010000.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHMIA|KMIA|2026-04-14": {"final_truth_value": 75.0},
                }
            }
        ),
        encoding="utf-8",
    )

    recent_ts = (now - timedelta(hours=1)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, recent_ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 0
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 0
    assert float(summary["window"]["effective_window_hours_for_metrics"]) == 0.0
    assert float(summary["window"]["data_coverage_ratio"]) == 0.0


def test_latest_file_preferring_window_is_deterministic_for_same_stamp(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    file_a = out_dir / "kalshi_temperature_constraint_scan_summary_alpha_20260414_110000.json"
    file_b = out_dir / "kalshi_temperature_constraint_scan_summary_bravo_20260414_110000.json"
    file_a.write_text("{}", encoding="utf-8")
    file_b.write_text("{}", encoding="utf-8")

    # For stamped artifacts we intentionally use embedded stamp, not mtime.
    stale_ts = (now - timedelta(days=5)).timestamp()
    _set_file_time(file_a, stale_ts)
    _set_file_time(file_b, stale_ts)

    picked, in_window = _latest_file_preferring_window(
        out_dir,
        "kalshi_temperature_constraint_scan_summary_*.json",
        start_epoch=(now - timedelta(hours=2)).timestamp(),
        end_epoch=now.timestamp(),
    )
    assert in_window is True
    assert picked == file_b


def test_bankroll_validation_window_includes_same_second_fractional_mtime_for_unstamped_artifacts(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    start_epoch = 1000.0
    end_epoch = 2000.0

    unstamped = out_dir / "artifact_unstamped.json"
    unstamped.write_text("{}", encoding="utf-8")
    os.utime(unstamped, (end_epoch + 0.4, end_epoch + 0.4))

    matched = bankroll_validation_module._files_in_window(
        out_dir,
        "*.json",
        start_epoch,
        end_epoch,
    )
    assert unstamped in matched
    assert bankroll_validation_module._artifact_in_window(
        unstamped,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    ) is True


def test_bankroll_validation_includes_legacy_plan_rows_without_source_strategy(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260414_115500.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [{"market_ticker": "KXHIGHMIA-26APR14-B77.5", "threshold_expression": "above:77.5"}],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260414_115500.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-legacy-1",
                "underlying_key": "KXHIGHMIA|KMIA|2026-04-14",
                "series_ticker": "KXHIGHMIA",
                "settlement_station": "KMIA",
                "target_date_local": "2026-04-14",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "11",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-14T18:00:00+00:00",
                "hours_to_close": "2",
            }
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260414_115500.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
        ],
        [
            {
                "source_strategy": "",
                "temperature_intent_id": "intent-legacy-1",
                "temperature_client_order_id": "temp-legacy-1",
                "market_ticker": "KXHIGHMIA-26APR14-B77.5",
                "side": "no",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
            }
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260414_115500.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHMIA|KMIA|2026-04-14": {"final_truth_value": 75.0},
                }
            }
        ),
        encoding="utf-8",
    )

    ts = (now - timedelta(minutes=30)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 1
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 1


def test_bankroll_validation_excludes_non_temperature_kx_rows_without_strategy_or_temp_id(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260414_115500.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [{"market_ticker": "KXRAINNY-26APR14-B0.5", "threshold_expression": "above:0.5"}],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260414_115500.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-rain-1",
                "underlying_key": "KXRAINNY|KNYC|2026-04-14",
                "series_ticker": "KXRAINNY",
                "settlement_station": "KNYC",
                "target_date_local": "2026-04-14",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "11",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-14T18:00:00+00:00",
                "hours_to_close": "2",
            }
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260414_115500.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
        ],
        [
            {
                "source_strategy": "",
                "temperature_intent_id": "intent-rain-1",
                "temperature_client_order_id": "rain-1",
                "market_ticker": "KXRAINNY-26APR14-B0.5",
                "side": "yes",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
            }
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260414_115500.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXRAINNY|KNYC|2026-04-14": {"final_truth_value": 1.0},
                }
            }
        ),
        encoding="utf-8",
    )

    ts = (now - timedelta(minutes=30)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 0
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 0


def test_bankroll_validation_ticker_fallback_keeps_hourly_temp_and_rejects_macro(tmp_path: Path) -> None:
    now = datetime(2026, 4, 15, 20, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260415_195500.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [
            {"market_ticker": "KXTEMPNYCH-26APR1514-T80.99", "threshold_expression": "above:80.99"},
            {"market_ticker": "KXHIGHINFLATION-26DEC-T3.0", "threshold_expression": "above:3.0"},
        ],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260415_195500.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-hourly-1",
                "underlying_key": "KXTEMPNYCH|KNYC|2026-04-15",
                "series_ticker": "KXTEMPNYCH",
                "settlement_station": "KNYC",
                "target_date_local": "2026-04-15",
                "settlement_timezone": "America/New_York",
                "policy_metar_local_hour": "14",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-04-15T20:00:00+00:00",
                "hours_to_close": "1",
            },
            {
                "intent_id": "intent-macro-1",
                "underlying_key": "KXHIGHINFLATION|NONE|2026-12-01",
                "series_ticker": "KXHIGHINFLATION",
                "settlement_station": "",
                "target_date_local": "2026-12-01",
                "settlement_timezone": "",
                "policy_metar_local_hour": "",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-12-31T23:00:00+00:00",
                "hours_to_close": "24",
            },
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260415_195500.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
            "captured_at_utc",
        ],
        [
            {
                "source_strategy": "",
                "temperature_intent_id": "intent-hourly-1",
                "temperature_client_order_id": "",
                "market_ticker": "KXTEMPNYCH-26APR1514-T80.99",
                "side": "yes",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.08",
                "estimated_entry_cost_dollars": "0.50",
                "captured_at_utc": "2026-04-15T19:55:00+00:00",
            },
            {
                "source_strategy": "",
                "temperature_intent_id": "intent-macro-1",
                "temperature_client_order_id": "",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "side": "yes",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.50",
                "captured_at_utc": "2026-04-15T19:55:10+00:00",
            },
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260415_195500.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXTEMPNYCH|KNYC|2026-04-15": {"final_truth_value": 82.0},
                    "KXHIGHINFLATION|NONE|2026-12-01": {"final_truth_value": 3.4},
                }
            }
        ),
        encoding="utf-8",
    )

    ts = (now - timedelta(minutes=10)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 1
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 1


def test_bankroll_validation_excludes_strategy_tagged_macro_tickers(tmp_path: Path) -> None:
    now = datetime(2026, 4, 15, 20, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"

    specs_csv = out_dir / "kalshi_temperature_contract_specs_20260415_195500.csv"
    _write_csv(
        specs_csv,
        ["market_ticker", "threshold_expression"],
        [{"market_ticker": "KXHIGHINFLATION-26DEC-T3.0", "threshold_expression": "above:3.0"}],
    )
    intents_csv = out_dir / "kalshi_temperature_trade_intents_20260415_195500.csv"
    _write_csv(
        intents_csv,
        [
            "intent_id",
            "underlying_key",
            "series_ticker",
            "settlement_station",
            "target_date_local",
            "settlement_timezone",
            "policy_metar_local_hour",
            "constraint_status",
            "policy_reason",
            "close_time",
            "hours_to_close",
        ],
        [
            {
                "intent_id": "intent-macro-1",
                "underlying_key": "KXHIGHINFLATION|NONE|2026-12-01",
                "series_ticker": "KXHIGHINFLATION",
                "settlement_station": "",
                "target_date_local": "2026-12-01",
                "settlement_timezone": "",
                "policy_metar_local_hour": "",
                "constraint_status": "yes_likely_locked",
                "policy_reason": "approved",
                "close_time": "2026-12-31T23:00:00+00:00",
                "hours_to_close": "24",
            }
        ],
    )
    plan_csv = out_dir / "kalshi_temperature_trade_plan_20260415_195500.csv"
    _write_csv(
        plan_csv,
        [
            "source_strategy",
            "temperature_intent_id",
            "temperature_client_order_id",
            "market_ticker",
            "side",
            "maker_entry_price_dollars",
            "contracts_per_order",
            "maker_entry_edge_conservative_net_total",
            "estimated_entry_cost_dollars",
            "captured_at_utc",
        ],
        [
            {
                "source_strategy": "temperature_constraints",
                "temperature_intent_id": "intent-macro-1",
                "temperature_client_order_id": "temp-macro-1",
                "market_ticker": "KXHIGHINFLATION-26DEC-T3.0",
                "side": "yes",
                "maker_entry_price_dollars": "0.5",
                "contracts_per_order": "1",
                "maker_entry_edge_conservative_net_total": "0.10",
                "estimated_entry_cost_dollars": "0.50",
                "captured_at_utc": "2026-04-15T19:55:30+00:00",
            }
        ],
    )
    settlement_json = out_dir / "kalshi_temperature_settlement_state_20260415_195500.json"
    settlement_json.write_text(
        json.dumps(
            {
                "underlyings": {
                    "KXHIGHINFLATION|NONE|2026-12-01": {"final_truth_value": 3.4},
                }
            }
        ),
        encoding="utf-8",
    )

    ts = (now - timedelta(minutes=10)).timestamp()
    for path in (specs_csv, intents_csv, plan_csv, settlement_json):
        _set_file_time(path, ts)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    assert summary["opportunity_breadth"]["resolved_planned_rows"] == 0
    assert summary["opportunity_breadth"]["resolved_unique_market_sides"] == 0


def test_bankroll_validation_empty_window_has_zero_data_coverage(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=14.0,
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert summary["status"] == "ready"
    window = summary["window"]
    assert float(window["observed_span_hours"]) == 0.0
    assert float(window["effective_window_hours_for_metrics"]) == 0.0
    assert float(window["effective_window_days_for_metrics"]) == 0.0
    assert float(window["data_coverage_ratio"]) == 0.0
    viability = summary["viability_summary"]
    assert float(viability["effective_window_days_for_hysa_comparison"]) == 0.0
    assert float(viability["data_coverage_ratio"]) == 0.0


def test_alpha_gap_report_contains_required_signal_map(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    report = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )

    assert report["status"] == "ready"
    names = {entry["name"] for entry in report["missing_or_partial_signals"]}
    assert "bracket_range_consistency" in names
    assert "neighboring_strike_monotonicity" in names
    assert "taf_remainder_of_day_path_modeling" in names
    assert "cross_market_family_mispricing" in names
    assert report["likely_next_highest_impact_signal_expansion"]["name"]
    assert "signal_progress" in report
    assert "validation_context" in report
    context = report["validation_context"]
    assert "expected_vs_shadow_settled" in context
    assert "alpha_feature_density" in context
    assert "signal_evidence" in context
    assert "settlement_state" in context["signal_evidence"]
    assert "data_quality" in context
    assert "anti_misleading_guards" in context
    assert Path(report["output_file"]).exists()


def test_alpha_gap_report_reuses_supplied_validation_file(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    validation = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )
    validation_file = validation["output_file"]

    with patch(
        "betbot.kalshi_temperature_bankroll_validation._build_validation_payload",
        side_effect=AssertionError("alpha gap should reuse supplied validation file"),
    ):
        report = run_kalshi_temperature_alpha_gap_report(
            output_dir=str(out_dir),
            hours=24.0,
            reference_bankroll_dollars=1000.0,
            top_n=3,
            source_bankroll_validation_file=validation_file,
            now=now,
        )

    assert report["source_bankroll_validation_supplied"] is True
    assert report["source_bankroll_validation_reused"] is True
    assert report["source_bankroll_validation_recompute_reason"] == ""
    assert report["source_bankroll_validation_file"] == validation_file


def test_alpha_gap_report_recomputes_when_supplied_validation_invalid(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    invalid_validation = out_dir / "invalid_bankroll_validation.json"
    invalid_validation.write_text(json.dumps({"status": "ready"}), encoding="utf-8")

    with patch(
        "betbot.kalshi_temperature_bankroll_validation._build_validation_payload",
        wraps=bankroll_validation_module._build_validation_payload,
    ) as mocked_build_validation:
        report = run_kalshi_temperature_alpha_gap_report(
            output_dir=str(out_dir),
            hours=24.0,
            reference_bankroll_dollars=1000.0,
            top_n=3,
            source_bankroll_validation_file=str(invalid_validation),
            now=now,
        )

    assert mocked_build_validation.called
    assert report["source_bankroll_validation_supplied"] is True
    assert report["source_bankroll_validation_reused"] is False
    assert report["source_bankroll_validation_recompute_reason"] == "invalid_or_missing_source_validation_payload"


def test_alpha_gap_report_no_resolved_outcomes_uses_explicit_insufficient_data_summary(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    report = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=14.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )

    estimate = report["opportunity_ceiling_estimate"]
    assert estimate["resolved_unique_market_sides"] == 0
    assert estimate["resolved_unique_underlying_families"] == 0
    assert estimate["repeated_entry_multiplier"] is None
    assert "No resolved unique market-side outcomes" in estimate["ceiling_summary"]
    assert report["main_limiting_factor"] == "insufficient_breadth"
    by_name = {entry["name"]: entry for entry in report["missing_or_partial_signals"]}
    assert by_name["neighboring_strike_monotonicity"]["status"] == "missing"
    assert by_name["exact_strike_impossibility_chains"]["status"] == "missing"
    assert by_name["bracket_range_consistency"]["status"] == "missing"
    assert by_name["speci_triggered_intraday_jumps"]["status"] == "missing"


def test_alpha_gap_report_marks_stale_signal_evidence_as_partial(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    stale_constraint = out_dir / "kalshi_temperature_constraint_scan_summary_20260410_010000.json"
    stale_constraint.write_text(
        json.dumps(
            {
                "forecast_modeled_count": 0,
                "taf_ready_count": 0,
                "speci_recent_count": 0,
                "speci_shock_active_count": 0,
                "consistency_checks": {
                    "neighboring_strike_monotonicity": {"checked_groups": 4, "violations_count": 1},
                    "exact_strike_impossibility_chains": {"checked_groups": 3, "violations_count": 0},
                    "range_family_consistency": {"checked_groups": 2, "violations_count": 0},
                    "cross_market_family_mispricing": {"checked_buckets": 5, "candidate_count": 0},
                },
            }
        ),
        encoding="utf-8",
    )
    _set_file_time(stale_constraint, (now - timedelta(days=3)).timestamp())

    stale_plan_summary = out_dir / "kalshi_temperature_trade_plan_summary_20260410_010000.json"
    stale_plan_summary.write_text(
        json.dumps(
            {
                "allocation_summary": {
                    "optimization_mode": "score_aware_greedy_v1",
                    "candidate_count": 12,
                    "selected_count": 4,
                    "selected_score_avg": 1.8,
                    "selected_score_total": 7.2,
                }
            }
        ),
        encoding="utf-8",
    )
    _set_file_time(stale_plan_summary, (now - timedelta(days=3)).timestamp())

    report = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )

    by_name = {entry["name"]: entry for entry in report["missing_or_partial_signals"]}
    assert by_name["neighboring_strike_monotonicity"]["status"] == "partial"
    assert by_name["exact_strike_impossibility_chains"]["status"] == "partial"
    assert by_name["bracket_range_consistency"]["status"] == "partial"
    assert by_name["cross_market_family_mispricing"]["status"] == "partial"
    assert by_name["execution_aware_portfolio_optimization"]["status"] == "partial"
    assert by_name["taf_remainder_of_day_path_modeling"]["status"] == "partial"
    assert by_name["speci_triggered_intraday_jumps"]["status"] == "partial"

    evidence = report["signal_evidence"]
    assert "settlement_state" in evidence
    assert evidence["constraint_scan_summary"]["available"] is True
    assert evidence["constraint_scan_summary"]["in_window"] is False
    assert evidence["trade_plan_summary"]["available"] is True
    assert evidence["trade_plan_summary"]["in_window"] is False
    assert report["validation_context"]["signal_evidence"] == evidence


def test_alpha_gap_report_uses_constraint_signal_progress_for_statuses(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    in_window_stamp = (now - timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")
    constraint_summary = out_dir / f"kalshi_temperature_constraint_scan_summary_{in_window_stamp}.json"
    constraint_summary.write_text(
        json.dumps(
            {
                "forecast_modeled_count": 25,
                "taf_ready_count": 20,
                "speci_recent_count": 3,
                "speci_shock_active_count": 3,
                "speci_shock_confidence_avg": 0.81,
                    "consistency_checks": {
                        "neighboring_strike_monotonicity": {"checked_groups": 5, "violations_count": 0},
                        "exact_strike_impossibility_chains": {"checked_groups": 4, "violations_count": 0},
                        "range_family_consistency": {"checked_groups": 3, "violations_count": 0},
                        "cross_market_family_mispricing": {
                            "checked_buckets": 6,
                            "checked_families": 18,
                            "candidate_count": 2,
                            "high_outlier_count": 2,
                            "low_outlier_count": 0,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
    _set_file_time(constraint_summary, (now - timedelta(hours=1)).timestamp())

    plan_summary_json = out_dir / f"kalshi_temperature_trade_plan_summary_{in_window_stamp}.json"
    plan_summary_json.write_text(
        json.dumps(
            {
                "allocation_summary": {
                    "optimization_mode": "score_aware_greedy_v1",
                    "candidate_count": 12,
                    "selected_count": 5,
                    "selected_score_avg": 2.4,
                    "selected_score_total": 12.0,
                }
            }
        ),
        encoding="utf-8",
    )
    _set_file_time(plan_summary_json, (now - timedelta(hours=1)).timestamp())

    report = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )

    by_name = {entry["name"]: entry for entry in report["missing_or_partial_signals"]}
    assert by_name["neighboring_strike_monotonicity"]["status"] == "implemented"
    assert by_name["exact_strike_impossibility_chains"]["status"] == "implemented"
    assert by_name["bracket_range_consistency"]["status"] == "implemented"
    assert by_name["taf_remainder_of_day_path_modeling"]["status"] == "implemented"
    assert by_name["speci_triggered_intraday_jumps"]["status"] == "implemented"
    assert by_name["cross_market_family_mispricing"]["status"] == "implemented"
    assert by_name["execution_aware_portfolio_optimization"]["status"] == "implemented"
    evidence = report["signal_evidence"]
    assert "settlement_state" in evidence
    assert evidence["constraint_scan_summary"]["available"] is True
    assert evidence["constraint_scan_summary"]["in_window"] is True
    assert evidence["trade_plan_summary"]["available"] is True
    assert evidence["trade_plan_summary"]["in_window"] is True
    assert report["validation_context"]["signal_evidence"] == evidence


def test_alpha_gap_report_marks_in_window_consistency_violations_as_partial(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    in_window_stamp = (now - timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")
    constraint_summary = out_dir / f"kalshi_temperature_constraint_scan_summary_{in_window_stamp}.json"
    constraint_summary.write_text(
        json.dumps(
            {
                "forecast_modeled_count": 5,
                "taf_ready_count": 2,
                "speci_recent_count": 1,
                "speci_shock_active_count": 1,
                "speci_shock_confidence_avg": 0.6,
                "consistency_checks": {
                    "neighboring_strike_monotonicity": {"checked_groups": 5, "violations_count": 2},
                    "exact_strike_impossibility_chains": {"checked_groups": 4, "violations_count": 0},
                    "range_family_consistency": {"checked_groups": 3, "violations_count": 0},
                    "cross_market_family_mispricing": {
                        "checked_buckets": 4,
                        "checked_families": 10,
                        "candidate_count": 1,
                        "high_outlier_count": 1,
                        "low_outlier_count": 0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    _set_file_time(constraint_summary, (now - timedelta(hours=1)).timestamp())

    plan_summary_json = out_dir / f"kalshi_temperature_trade_plan_summary_{in_window_stamp}.json"
    plan_summary_json.write_text(
        json.dumps(
            {
                "allocation_summary": {
                    "optimization_mode": "score_aware_greedy_v1",
                    "candidate_count": 12,
                    "selected_count": 5,
                    "selected_score_avg": 2.4,
                    "selected_score_total": 12.0,
                }
            }
        ),
        encoding="utf-8",
    )
    _set_file_time(plan_summary_json, (now - timedelta(hours=1)).timestamp())

    report = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        top_n=3,
        now=now,
    )

    by_name = {entry["name"]: entry for entry in report["missing_or_partial_signals"]}
    assert by_name["neighboring_strike_monotonicity"]["status"] == "partial"
    assert by_name["exact_strike_impossibility_chains"]["status"] == "implemented"
    assert by_name["bracket_range_consistency"]["status"] == "implemented"


def test_live_readiness_emits_multi_horizon_gates(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    report = run_kalshi_temperature_live_readiness(
        output_dir=str(out_dir),
        horizons="1d,7d,14d",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert report["status"] == "ready"
    readiness = report["readiness_by_horizon"]
    assert [entry["horizon"] for entry in readiness] == ["1d", "7d", "14d"]
    for entry in readiness:
        assert "ready_for_real_money" in entry
        assert "performance" in entry
        assert "gates" in entry
        assert entry["window_semantics"]["type"] == "rolling"
        assert entry["window_semantics"]["is_calendar_day"] is False
        assert "parity_context" in entry
        assert "expected_vs_shadow_settled_full" in entry["parity_context"]
        assert "alpha_feature_density_full" in entry["parity_context"]
        assert "signal_progress" in entry["parity_context"]
        assert "signal_evidence" in entry["parity_context"]
        assert "settlement_state" in entry["parity_context"]["signal_evidence"]
        assert "data_quality" in entry["parity_context"]

    overall = report["overall_live_readiness"]
    assert overall["recommendation"]
    assert overall["ready_for_small_live_pilot"] is False
    assert "validation_parity_by_horizon" in report
    assert set(report["validation_parity_by_horizon"].keys()) == {"1d", "7d", "14d"}
    assert Path(report["output_file"]).exists()


def test_live_readiness_uses_headline_only_simulation_scope(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    with patch(
        "betbot.kalshi_temperature_bankroll_validation._simulate_bankroll_for_layer",
        wraps=bankroll_validation_module._simulate_bankroll_for_layer,
    ) as mocked_simulation:
        report = run_kalshi_temperature_live_readiness(
            output_dir=str(out_dir),
            horizons="1d",
            reference_bankroll_dollars=1000.0,
            slippage_bps_list="0,5,10",
            top_n=3,
            now=now,
        )

    assert report["status"] == "ready"
    # Readiness uses the deployment headline basis only:
    # one model x one layer x one conservative-slippage scenario per horizon.
    assert mocked_simulation.call_count == 1


def test_live_readiness_uses_observed_window_for_hysa_when_horizon_exceeds_data_coverage(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    report = run_kalshi_temperature_live_readiness(
        output_dir=str(out_dir),
        horizons="1yr",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert report["status"] == "ready"
    entry = report["readiness_by_horizon"][0]
    assert entry["horizon"] == "1yr"

    performance = entry["performance"]
    assert float(performance["observed_window_days_for_metrics"]) < 2.0
    assert float(performance["data_coverage_ratio"]) < 0.01
    assert float(performance["equivalent_hysa_return_for_horizon"]) < 1.0
    gates = entry["gates"]
    assert "insufficient_history_coverage_for_horizon" in gates["failed_reasons"]
    assert "pipeline_data_stale_or_missing" in gates["failed_reasons"]
    assert float(gates["minimum_data_coverage_ratio"]) >= 0.35
    assert float(gates["threshold_basis_days"]) >= 1.0

    parity = entry["parity_context"]
    viability = parity["viability_summary_full"]
    assert float(viability["effective_window_days_for_hysa_comparison"]) < 2.0
    assert float(viability["data_coverage_ratio"]) < 0.01
    assert float(viability["equivalent_window_hysa_return_on_reference_bankroll"]) < 1.0


def test_live_readiness_empty_window_reports_zero_coverage(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    report = run_kalshi_temperature_live_readiness(
        output_dir=str(out_dir),
        horizons="1d",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert report["status"] == "ready"
    entry = report["readiness_by_horizon"][0]
    assert entry["horizon"] == "1d"

    performance = entry["performance"]
    assert float(performance["observed_window_days_for_metrics"]) == 0.0
    assert float(performance["data_coverage_ratio"]) == 0.0
    assert float(performance["equivalent_hysa_return_for_horizon"]) == 0.0
    gates = entry["gates"]
    assert "insufficient_history_coverage_for_horizon" in gates["failed_reasons"]

    parity = entry["parity_context"]
    viability = parity["viability_summary_full"]
    assert float(viability["effective_window_days_for_hysa_comparison"]) == 0.0
    assert float(viability["data_coverage_ratio"]) == 0.0
    assert float(viability["equivalent_window_hysa_return_on_reference_bankroll"]) == 0.0


def test_live_readiness_pipeline_gate_passes_when_core_feeds_are_fresh(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    fresh_stamp = (now - timedelta(minutes=5)).strftime("%Y%m%d_%H%M%S")
    shadow_summary = out_dir / f"kalshi_temperature_shadow_watch_summary_{fresh_stamp}.json"
    shadow_summary.write_text(
        json.dumps(
            {
                "status": "ready",
                    "captured_at": (now - timedelta(minutes=5)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    intents_summary = out_dir / f"kalshi_temperature_trade_intents_summary_{fresh_stamp}.json"
    intents_summary.write_text(
        json.dumps(
            {
                "status": "ready",
                    "captured_at": (now - timedelta(minutes=5)).isoformat(),
                "intents_total": 2,
                "intents_approved": 1,
            }
        ),
        encoding="utf-8",
    )
    metar_summary = out_dir / f"kalshi_temperature_metar_summary_{fresh_stamp}.json"
    metar_summary.write_text(
        json.dumps(
            {
                "status": "ready",
                    "captured_at": (now - timedelta(minutes=5)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    settlement_summary = out_dir / f"kalshi_temperature_settlement_state_{fresh_stamp}.json"
    settlement_summary.write_text(
        json.dumps(
            {
                "status": "ready",
                    "captured_at": (now - timedelta(minutes=5)).isoformat(),
                "underlyings": {},
            }
        ),
        encoding="utf-8",
    )
    ts = (now - timedelta(minutes=5)).timestamp()
    for path in (shadow_summary, intents_summary, metar_summary, settlement_summary):
        _set_file_time(path, ts)

    report = run_kalshi_temperature_live_readiness(
        output_dir=str(out_dir),
        horizons="1d",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert report["status"] == "ready"
    entry = report["readiness_by_horizon"][0]
    gates = entry["gates"]
    assert "pipeline_data_stale_or_missing" not in gates["failed_reasons"]
    parity = entry["parity_context"]
    assert parity["data_quality"]["pipeline_status"] == "green"


def test_go_live_gate_emits_pass_fail_snapshot(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    gate = run_kalshi_temperature_go_live_gate(
        output_dir=str(out_dir),
        horizons="1d,7d,14d",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )

    assert gate["status"] == "ready"
    assert gate["gate_status"] in {"pass", "fail"}
    assert gate["recommendation"]
    assert "failed_horizons" in gate
    assert isinstance(gate["failed_horizons"], list)
    assert Path(gate["source_live_readiness_file"]).exists()
    assert Path(gate["output_file"]).exists()


def test_go_live_gate_reuses_supplied_live_readiness_file(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    readiness = run_kalshi_temperature_live_readiness(
        output_dir=str(out_dir),
        horizons="1d,7d,14d",
        reference_bankroll_dollars=1000.0,
        slippage_bps_list="0,5,10",
        top_n=3,
        now=now,
    )
    readiness_file = readiness["output_file"]

    with patch(
        "betbot.kalshi_temperature_bankroll_validation.run_kalshi_temperature_live_readiness",
        side_effect=AssertionError("live readiness should be reused, not recomputed"),
    ):
        gate = run_kalshi_temperature_go_live_gate(
            output_dir=str(out_dir),
            horizons="1d,7d,14d",
            reference_bankroll_dollars=1000.0,
            slippage_bps_list="0,5,10",
            top_n=3,
            source_live_readiness_file=readiness_file,
            now=now,
        )

    assert gate["source_live_readiness_supplied"] is True
    assert gate["source_live_readiness_reused"] is True
    assert gate["source_live_readiness_recompute_reason"] == ""
    assert gate["source_live_readiness_file"] == readiness_file


def test_go_live_gate_recomputes_when_supplied_live_readiness_invalid(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    invalid_readiness = out_dir / "invalid_live_readiness.json"
    invalid_readiness.write_text(json.dumps({"status": "ready"}), encoding="utf-8")

    with patch(
        "betbot.kalshi_temperature_bankroll_validation.run_kalshi_temperature_live_readiness",
        wraps=run_kalshi_temperature_live_readiness,
    ) as mocked_live_readiness:
        gate = run_kalshi_temperature_go_live_gate(
            output_dir=str(out_dir),
            horizons="1d,7d,14d",
            reference_bankroll_dollars=1000.0,
            slippage_bps_list="0,5,10",
            top_n=3,
            source_live_readiness_file=str(invalid_readiness),
            now=now,
        )

    assert mocked_live_readiness.called
    assert gate["source_live_readiness_supplied"] is True
    assert gate["source_live_readiness_reused"] is False
    assert gate["source_live_readiness_recompute_reason"] == "invalid_or_missing_source_readiness_payload"
    assert Path(gate["source_live_readiness_file"]).exists()


def test_report_output_filenames_use_captured_timestamp_when_now_provided(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 34, 56, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)
    expected_stamp = now.strftime("%Y%m%d_%H%M%S")

    validation = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        now=now,
    )
    alpha_gap = run_kalshi_temperature_alpha_gap_report(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        now=now,
    )
    gate = run_kalshi_temperature_go_live_gate(
        output_dir=str(out_dir),
        horizons="1d,7d,14d",
        reference_bankroll_dollars=1000.0,
        now=now,
    )

    assert Path(validation["output_file"]).name == f"kalshi_temperature_bankroll_validation_{expected_stamp}.json"
    assert Path(alpha_gap["output_file"]).name == f"kalshi_temperature_alpha_gap_report_{expected_stamp}.json"
    assert Path(gate["output_file"]).name == f"kalshi_temperature_go_live_gate_{expected_stamp}.json"
    assert _normalize_iso(gate["captured_at"]) == now.isoformat()


def test_viability_summary_uses_deployed_unique_counts_for_return_breadth(tmp_path: Path) -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    out_dir = tmp_path / "outputs"
    _seed_temperature_artifacts(out_dir, now)

    sizing_models = {
        "fixed_fraction_per_underlying_family": {
            "max_simultaneous_market_sides": 1,
            "max_pct_total_deployed": 1.0,
            "max_pct_per_underlying_family": 1.0,
            "max_pct_per_city_day": 1.0,
            "max_same_station_exposure_pct": 1.0,
            "max_same_hour_cluster_pct": 1.0,
            "risk_per_trade_pct": 0.5,
        }
    }
    summary = run_kalshi_temperature_bankroll_validation(
        output_dir=str(out_dir),
        hours=24.0,
        reference_bankroll_dollars=1000.0,
        sizing_models_json=json.dumps(sizing_models),
        slippage_bps_list="0",
        top_n=3,
        now=now,
    )

    viability = summary["viability_summary"]
    deployment_metrics = (
        summary["bankroll_simulation"]["by_model"]["fixed_fraction_per_underlying_family"]["by_slippage_bps"]["0.0"][
            "underlying_family_aggregated"
        ]
    )
    assert viability["how_many_independent_market_side_calls_generated_that_return"] == int(
        deployment_metrics["unique_market_side_count"]
    )
    assert viability["deployed_unique_market_side_calls"] == int(deployment_metrics["unique_market_side_count"])
    assert viability["resolved_unique_market_side_opportunities"] == int(
        summary["opportunity_breadth"]["resolved_unique_market_sides"]
    )


def _normalize_iso(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def test_equal_threshold_uses_numerical_tolerance() -> None:
    assert _derive_threshold_yes_outcome("equal:77.0", 77.0 + 5e-7) is True
    assert _derive_threshold_yes_outcome("equal:77.0", 77.0 + 2e-5) is False


def test_simulation_daily_window_pnl_includes_entry_fee() -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    trade = TemperatureOpportunity(
        row_id="row-1",
        planned_at=now,
        close_time=now + timedelta(hours=1),
        intent_id="intent-1",
        shadow_order_id="shadow-1",
        market_ticker="KXHIGHMIA-26APR13-B77.5",
        side="no",
        market_side_key="KXHIGHMIA-26APR13-B77.5|no",
        underlying_key="KXHIGHMIA|KMIA|2026-04-13",
        underlying_family_key="KXHIGHMIA|KMIA|2026-04-13",
        city_day_key="KMIA|2026-04-13",
        settlement_station="KMIA",
        local_hour_key="13",
        signal_type="yes_likely_locked",
        policy_reason="approved",
        contracts=1.0,
        entry_price_dollars=0.5,
        expected_edge_dollars=0.08,
        estimated_entry_cost_dollars=0.5,
        resolved=True,
        win=True,
        push=False,
        outcome="win",
        base_pnl_dollars=0.5,
        threshold_expression="above:77.5",
        final_truth_value=75.0,
        resolution_reason="resolved",
    )
    metrics, executed_rows = _simulate_bankroll_for_layer(
        trades=[trade],
        model_name="fixed_unit_risk_budget",
        model_config={
            "risk_per_trade_pct": 0.02,
            "unit_risk_dollars": 100.0,
            "max_pct_total_deployed": 1.0,
            "max_pct_per_underlying_family": 1.0,
            "max_pct_per_city_day": 1.0,
            "max_simultaneous_market_sides": 10,
            "max_same_station_exposure_pct": 1.0,
            "max_same_hour_cluster_pct": 1.0,
        },
        reference_bankroll_dollars=1000.0,
        slippage_bps=0.0,
        fee_model={
            "entry_fee_rate": 0.01,
            "exit_fee_rate": 0.01,
            "fixed_fee_per_trade": 0.5,
        },
    )

    assert len(executed_rows) == 1
    assert abs(float(executed_rows[0]["net_pnl"]) - 97.5) < 1e-9
    assert abs(float(metrics["pnl_total"]) - 97.5) < 1e-9
    assert abs(float(metrics["best_window_pnl"]) - 97.5) < 1e-9
    assert abs(float(metrics["worst_window_pnl"]) - 97.5) < 1e-9


def test_simulation_clears_market_side_lock_when_multiple_positions_settle_together() -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    base_kwargs = dict(
        intent_id="intent",
        underlying_key="KXHIGHMIA|KMIA|2026-04-13",
        underlying_family_key="KXHIGHMIA|KMIA|2026-04-13",
        city_day_key="KMIA|2026-04-13",
        settlement_station="KMIA",
        local_hour_key="13",
        signal_type="yes_likely_locked",
        policy_reason="approved",
        contracts=1.0,
        entry_price_dollars=0.5,
        expected_edge_dollars=0.08,
        estimated_entry_cost_dollars=0.5,
        resolved=True,
        win=True,
        push=False,
        outcome="win",
        base_pnl_dollars=0.5,
        threshold_expression="above:77.5",
        final_truth_value=75.0,
        resolution_reason="resolved",
    )

    trade_a = TemperatureOpportunity(
        row_id="row-a",
        planned_at=now,
        close_time=now + timedelta(hours=1),
        shadow_order_id="shadow-a",
        market_ticker="KXHIGHMIA-26APR13-B77.5",
        side="no",
        market_side_key="KXHIGHMIA-26APR13-B77.5|no",
        **base_kwargs,
    )
    trade_b = TemperatureOpportunity(
        row_id="row-b",
        planned_at=now + timedelta(minutes=10),
        close_time=now + timedelta(hours=1),
        shadow_order_id="shadow-b",
        market_ticker="KXHIGHMIA-26APR13-B77.5",
        side="no",
        market_side_key="KXHIGHMIA-26APR13-B77.5|no",
        **base_kwargs,
    )
    trade_c = TemperatureOpportunity(
        row_id="row-c",
        planned_at=now + timedelta(hours=1, minutes=5),
        close_time=now + timedelta(hours=2),
        shadow_order_id="shadow-c",
        market_ticker="KXHIGHNY-26APR13-B74.5",
        side="no",
        market_side_key="KXHIGHNY-26APR13-B74.5|no",
        **base_kwargs,
    )

    metrics, executed_rows = _simulate_bankroll_for_layer(
        trades=[trade_a, trade_b, trade_c],
        model_name="fixed_unit_risk_budget",
        model_config={
            "risk_per_trade_pct": 0.02,
            "unit_risk_dollars": 20.0,
            "max_pct_total_deployed": 1.0,
            "max_pct_per_underlying_family": 1.0,
            "max_pct_per_city_day": 1.0,
            "max_simultaneous_market_sides": 1,
            "max_same_station_exposure_pct": 1.0,
            "max_same_hour_cluster_pct": 1.0,
        },
        reference_bankroll_dollars=1000.0,
        slippage_bps=0.0,
        fee_model={
            "entry_fee_rate": 0.0,
            "exit_fee_rate": 0.0,
            "fixed_fee_per_trade": 0.0,
        },
    )

    assert len(executed_rows) == 3
    executed_ids = {str(row["trade_id"]) for row in executed_rows}
    assert executed_ids == {"row-a", "row-b", "row-c"}
    assert int(metrics["trade_count"]) == 3


def test_dedupe_opportunities_prefers_resolved_duplicate_when_same_key() -> None:
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    unresolved = TemperatureOpportunity(
        row_id="row-unresolved",
        planned_at=now,
        close_time=now + timedelta(hours=1),
        intent_id="intent-dup",
        shadow_order_id="shadow-dup",
        market_ticker="KXHIGHMIA-26APR14-B77.5",
        side="no",
        market_side_key="KXHIGHMIA-26APR14-B77.5|no",
        underlying_key="KXHIGHMIA|KMIA|2026-04-14",
        underlying_family_key="KXHIGHMIA|KMIA|2026-04-14",
        city_day_key="KMIA|2026-04-14",
        settlement_station="KMIA",
        local_hour_key="12",
        signal_type="yes_likely_locked",
        policy_reason="approved",
        contracts=1.0,
        entry_price_dollars=0.5,
        expected_edge_dollars=0.08,
        estimated_entry_cost_dollars=0.5,
        resolved=False,
        win=None,
        push=False,
        outcome="unresolved",
        base_pnl_dollars=0.0,
        threshold_expression="",
        final_truth_value=None,
        resolution_reason="missing_threshold_or_final_truth_or_side",
    )
    resolved = TemperatureOpportunity(
        row_id="row-resolved",
        planned_at=now + timedelta(minutes=5),
        close_time=now + timedelta(hours=1),
        intent_id="intent-dup",
        shadow_order_id="shadow-dup",
        market_ticker="KXHIGHMIA-26APR14-B77.5",
        side="no",
        market_side_key="KXHIGHMIA-26APR14-B77.5|no",
        underlying_key="KXHIGHMIA|KMIA|2026-04-14",
        underlying_family_key="KXHIGHMIA|KMIA|2026-04-14",
        city_day_key="KMIA|2026-04-14",
        settlement_station="KMIA",
        local_hour_key="12",
        signal_type="yes_likely_locked",
        policy_reason="approved",
        contracts=1.0,
        entry_price_dollars=0.5,
        expected_edge_dollars=0.08,
        estimated_entry_cost_dollars=0.5,
        resolved=True,
        win=True,
        push=False,
        outcome="win",
        base_pnl_dollars=0.5,
        threshold_expression="above:77.5",
        final_truth_value=75.0,
        resolution_reason="resolved",
    )

    canonical, duplicates, warnings = _dedupe_opportunities(
        rows=[unresolved, resolved],
        key_fn=lambda row: row.shadow_order_id,
    )

    assert len(canonical) == 1
    assert canonical[0].row_id == "row-resolved"
    assert canonical[0].resolved is True
    assert duplicates["shadow-dup"] == 2
    assert any("replaced_existing_with_higher_quality" in warning for warning in warnings)


def test_main_limiting_factor_prefers_insufficient_breadth_when_overlap_dominates() -> None:
    limiting_factor = _infer_main_limiting_factor(
        policy_reason_counts={
            "no_side_interval_overlap_still_possible": 1200,
            "metar_observation_stale": 95,
            "inside_cutoff_window": 20,
            "settlement_finalization_blocked": 14,
        },
        concentration_warning=False,
        opportunity_breadth={
            "resolved_unique_market_sides": 24,
            "unresolved_unique_market_sides": 130,
            "repeated_entry_multiplier": 12.5,
        },
    )
    assert limiting_factor == "insufficient_breadth"


def test_main_limiting_factor_prefers_stale_when_stale_clearly_dominates() -> None:
    limiting_factor = _infer_main_limiting_factor(
        policy_reason_counts={
            "metar_observation_stale": 950,
            "inside_cutoff_window": 75,
            "no_side_interval_overlap_still_possible": 40,
        },
        concentration_warning=False,
        opportunity_breadth={
            "resolved_unique_market_sides": 40,
            "unresolved_unique_market_sides": 10,
            "repeated_entry_multiplier": 1.2,
        },
    )
    assert limiting_factor == "stale_suppression"


def test_main_limiting_factor_ignores_settlement_when_current_backlog_clear() -> None:
    limiting_factor = _infer_main_limiting_factor(
        policy_reason_counts={
            "settlement_finalization_blocked": 900,
            "metar_observation_stale": 120,
            "inside_cutoff_window": 40,
        },
        concentration_warning=False,
        opportunity_breadth={
            "resolved_unique_market_sides": 40,
            "unresolved_unique_market_sides": 20,
            "repeated_entry_multiplier": 1.1,
        },
        current_settlement_unresolved=0,
    )
    assert limiting_factor == "stale_suppression"


def test_main_limiting_factor_allows_settlement_when_backlog_active() -> None:
    limiting_factor = _infer_main_limiting_factor(
        policy_reason_counts={
            "settlement_finalization_blocked": 900,
            "metar_observation_stale": 120,
            "inside_cutoff_window": 40,
        },
        concentration_warning=False,
        opportunity_breadth={
            "resolved_unique_market_sides": 40,
            "unresolved_unique_market_sides": 20,
            "repeated_entry_multiplier": 1.1,
        },
        current_settlement_unresolved=6,
    )
    assert limiting_factor == "settlement_finalization"


def test_path_probe_permission_error_falls_back_to_inline_parsing() -> None:
    with patch("betbot.kalshi_temperature_bankroll_validation.Path.exists", side_effect=PermissionError("denied")):
        assert _resolve_slippage_bps_list("0,5,10") == [0.0, 5.0, 10.0]
        assert _load_json_input('{"a": 1}') == {"a": 1}


def test_overall_live_decision_handles_custom_horizons_and_earliest_by_duration() -> None:
    decision = _build_overall_live_decision(
        [
            {
                "horizon": "1yr",
                "hours": 365.0 * 24.0,
                "ready_for_real_money": True,
                "gates": {"failed_reasons": []},
            },
            {
                "horizon": "14d",
                "hours": 14.0 * 24.0,
                "ready_for_real_money": True,
                "gates": {"failed_reasons": []},
            },
            {
                "horizon": "60d",
                "hours": 60.0 * 24.0,
                "ready_for_real_money": True,
                "gates": {"failed_reasons": []},
            },
        ]
    )
    assert decision["earliest_passing_horizon"] == "14d"
    assert decision["ready_for_small_live_pilot"] is True
    assert decision["ready_for_scaled_live"] is True
