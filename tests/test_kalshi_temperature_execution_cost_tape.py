from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from betbot.kalshi_execution_journal import append_execution_events, ensure_execution_journal_schema
from betbot.kalshi_temperature_execution_cost_tape import run_kalshi_temperature_execution_cost_tape


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_execution_cost_tape_builds_calibration_metrics_from_artifacts(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)

    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.62,
                "blocked_total": 1000,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "captured_at": now.isoformat(),
            "intents_blocked": 250,
            "policy_reason_counts": {"expected_edge_below_min": 210},
            "sparse_evidence_hardening_blocked_expected_edge_below_min_count": 210,
            "historical_profitability_guardrail_blocked_expected_edge_below_min_count": 220,
            "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": 225,
            "effective_min_expected_edge_net_values": [0.06, 0.08, 0.1],
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {
                "captured_at": now.isoformat(),
                "status": "ready",
                "market_count": 2,
                "desynced_market_count": 0,
                "events_processed": 12,
            },
            "markets": {
                "KXHIGHCHI-26APR22-T73": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.41,
                        "best_yes_ask_dollars": 0.43,
                        "best_no_bid_dollars": 0.57,
                        "best_no_ask_dollars": 0.59,
                        "yes_spread_dollars": 0.02,
                    }
                },
                "KXHIGHNYC-26APR22-T72": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.48,
                        "best_yes_ask_dollars": 0.50,
                        "best_no_bid_dollars": 0.50,
                        "best_no_ask_dollars": 0.52,
                        "yes_spread_dollars": 0.02,
                    }
                },
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(hours=1)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="test-run",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHCHI-26APR22-T73",
                "side": "no",
                "spread_dollars": 0.02,
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": 0.43,
                "best_no_bid_dollars": 0.57,
                "best_no_ask_dollars": 0.59,
                "visible_depth_contracts": 1400.0,
                "result": "dry_run_ready",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHNYC-26APR22-T72",
                "side": "yes",
                "spread_dollars": 0.03,
                "best_yes_bid_dollars": 0.48,
                "best_yes_ask_dollars": 0.50,
                "best_no_bid_dollars": 0.50,
                "best_no_ask_dollars": 0.52,
                "visible_depth_contracts": 1200.0,
                "result": "dry_run_ready",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHCHI-26APR22-T73",
                "side": "no",
                "spread_dollars": 0.01,
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": 0.43,
                "best_no_bid_dollars": 0.57,
                "best_no_ask_dollars": 0.59,
                "visible_depth_contracts": 1300.0,
                "result": "dry_run_ready",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        window_hours=168.0,
        min_candidate_samples=1,
        min_quote_coverage_ratio=0.5,
        max_tickers=5,
    )

    assert summary["status"] == "ready"
    assert summary["calibration_readiness"]["status"] == "green"
    assert summary["expected_edge_blocking"]["largest_blocker_reason"] == "expected_edge_below_min"
    assert summary["execution_cost_observations"]["candidate_rows"] == 1
    assert summary["execution_cost_observations"]["event_rows_scanned"] == 3
    assert summary["execution_cost_observations"]["spread_median_dollars"] is not None
    assert summary["execution_cost_observations"]["quote_two_sided_ratio"] is not None
    decomposition = dict(summary["execution_cost_observations"].get("quote_coverage_decomposition") or {})
    assert decomposition.get("rows_total") == 3
    assert decomposition.get("rows_with_any_two_sided_quote") == 3
    assert decomposition.get("rows_without_two_sided_quote") == 0
    assert decomposition.get("rows_with_both_yes_no_two_sided") == 3
    assert decomposition.get("rows_with_neither_yes_no_two_sided") == 0
    assert decomposition.get("rows_with_partial_yes_quote") == 0
    assert decomposition.get("rows_with_partial_no_quote") == 0
    missing_buckets = dict(summary["execution_cost_observations"].get("top_missing_coverage_buckets") or {})
    assert list(missing_buckets.get("by_market") or []) == []
    assert list(missing_buckets.get("by_side") or []) == []
    assert list(missing_buckets.get("by_market_side") or []) == []
    assert summary["source_files"]["execution_journal"].endswith("kalshi_execution_journal.sqlite3")
    assert isinstance(summary["recommended_exclusions"], dict)
    assert "market_tickers" in summary["recommended_exclusions"]
    recommended_targets = dict(summary.get("recommended_shadow_quote_probe_targets") or {})
    assert recommended_targets.get("status") == "empty"
    assert int(recommended_targets.get("target_count") or 0) == 0
    assert list(recommended_targets.get("target_keys") or []) == []
    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("status") == "ready"
    assert pressure.get("quote_coverage_shortfall") == 0.0
    assert pressure.get("uncovered_market_top5_share") is None
    assert int(pressure.get("low_coverage_wide_spread_ticker_count") or 0) == 0
    assert list(pressure.get("low_coverage_wide_spread_tickers") or []) == []
    assert pressure.get("dominant_uncovered_side") == "unknown"
    assert pressure.get("dominant_uncovered_side_share") is None
    assert pressure.get("yes_uncovered_share") == 0.0
    assert pressure.get("no_uncovered_share") == 0.0
    assert pressure.get("side_imbalance_magnitude") is None
    assert pressure.get("side_pressure_score_contribution") == 0.0
    assert pressure.get("pressure_score") == 0.0
    assert Path(summary["output_file"]).exists()
    assert Path(summary["latest_file"]).exists()


def test_execution_cost_tape_reports_missing_journal_gap(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.58,
                "blocked_total": 100,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 50,
            "policy_reason_counts": {"expected_edge_below_min": 30},
        },
    )
    summary = run_kalshi_temperature_execution_cost_tape(output_dir=str(out_dir))
    assert summary["status"] == "ready"
    assert "missing_execution_journal_data" in summary["data_pipeline_gaps"]
    assert summary["calibration_readiness"]["status"] == "red"


def test_execution_cost_tape_uses_event_weighted_quote_coverage_when_order_terminal_dominates(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.62,
                "blocked_total": 400,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 120,
            "policy_reason_counts": {"expected_edge_below_min": 90},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"captured_at": now.isoformat(), "status": "ready", "market_count": 1},
            "markets": {},
        },
    )
    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=20)
    events: list[dict[str, object]] = []
    for idx in range(2):
        events.append(
            {
                "captured_at_utc": (captured_at + timedelta(seconds=idx)).isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": f"KXHIGHMIA-26APR22-T8{idx}",
                "side": "yes",
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": 0.43,
                "best_no_bid_dollars": 0.57,
                "best_no_ask_dollars": 0.59,
                "spread_dollars": 0.02,
                "result": "expected_edge_below_min",
            }
        )
    for idx in range(2):
        events.append(
            {
                "captured_at_utc": (captured_at + timedelta(seconds=10 + idx)).isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": f"KXHIGHATL-26APR22-T7{idx}",
                "side": "yes",
                "best_yes_bid_dollars": 0.33,
                "best_yes_ask_dollars": 0.35,
                "best_no_bid_dollars": 0.65,
                "best_no_ask_dollars": 0.67,
                "spread_dollars": 0.02,
                "result": "expected_edge_below_min",
            }
        )
    for idx in range(10):
        events.append(
            {
                "captured_at_utc": (captured_at + timedelta(seconds=30 + idx)).isoformat(),
                "event_type": "order_terminal",
                "market_ticker": f"KXHIGHBOS-26APR22-T6{idx}",
                "side": "yes",
                "best_yes_bid_dollars": 0.29,
                "best_yes_ask_dollars": None,
                "best_no_bid_dollars": None,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.05,
                "result": "expected_edge_below_min",
            }
        )
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="event-weighted-gating",
        events=events,
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        min_quote_coverage_ratio=0.5,
        max_tickers=20,
    )

    execution = dict(summary.get("execution_cost_observations") or {})
    assert execution.get("quote_two_sided_ratio") == 0.285714
    assert execution.get("quote_two_sided_ratio_event_weighted") == 0.590164
    assert execution.get("quote_two_sided_ratio_for_gating") == 0.590164
    assert execution.get("quote_coverage_gating_mode") == "event_weighted_order_terminal_debiased"
    by_event_type = list(execution.get("quote_coverage_by_event_type") or [])
    assert by_event_type
    by_event_type_map = {str(row.get("event_type")): row for row in by_event_type}
    assert by_event_type_map["order_terminal"]["event_weight"] == 0.25

    readiness = dict(summary.get("calibration_readiness") or {})
    assert readiness.get("quote_coverage_ratio_raw") == 0.285714
    assert readiness.get("quote_coverage_ratio_event_weighted") == 0.590164
    assert readiness.get("quote_coverage_ratio") == 0.590164
    assert readiness.get("quote_coverage_gating_mode") == "event_weighted_order_terminal_debiased"
    assert readiness.get("meets_quote_coverage") is True

    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("quote_coverage_ratio_raw") == 0.285714
    assert pressure.get("quote_coverage_ratio_for_gating") == 0.590164
    assert pressure.get("quote_coverage_gating_mode") == "event_weighted_order_terminal_debiased"
    assert pressure.get("quote_coverage_shortfall") == 0.0


def test_execution_cost_tape_reports_empty_journal_gap_when_db_exists_without_events(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.58,
                "blocked_total": 100,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 50,
            "policy_reason_counts": {"expected_edge_below_min": 30},
        },
    )
    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    ensure_execution_journal_schema(journal_path)

    summary = run_kalshi_temperature_execution_cost_tape(output_dir=str(out_dir))
    assert summary["status"] == "ready"
    assert summary["execution_cost_observations"]["status"] == "empty_journal"
    assert summary["execution_cost_observations"]["event_rows_scanned"] == 0
    assert "empty_execution_journal_data" in summary["data_pipeline_gaps"]
    assert "missing_execution_journal_data" in summary["data_pipeline_gaps"]
    assert summary["calibration_readiness"]["status"] == "red"


def test_execution_cost_tape_recommends_exclusions_for_low_coverage_tickers(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.66,
                "blocked_total": 2000,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 800,
            "policy_reason_counts": {"expected_edge_below_min": 700},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 1},
            "markets": {
                "KXHIGHBOS-26APR22-B75": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.31,
                        "best_yes_ask_dollars": 0.49,
                        "yes_spread_dollars": 0.18,
                    }
                }
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=30)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="test-repair",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.31,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.19,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.3,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.18,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.32,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.2,
                "result": "expected_edge_below_min",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_ticker_rows_for_exclusion=2,
        exclusion_max_quote_coverage_ratio=0.25,
        max_ticker_mean_spread_for_exclusion=0.12,
        max_excluded_tickers=5,
        max_tickers=10,
    )

    assert summary["status"] == "ready"
    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("status") == "active"
    assert "KXHIGHBOS-26APR22-B75" in list(exclusions.get("market_tickers") or [])
    decomposition = dict(summary["execution_cost_observations"].get("quote_coverage_decomposition") or {})
    assert decomposition.get("rows_total") == 3
    assert decomposition.get("rows_with_any_two_sided_quote") == 0
    assert decomposition.get("rows_without_two_sided_quote") == 3
    assert decomposition.get("rows_with_neither_yes_no_two_sided") == 3
    assert decomposition.get("rows_with_partial_yes_quote") == 3
    assert decomposition.get("rows_with_partial_no_quote") == 0
    assert decomposition.get("rows_missing_all_quote_fields") == 0
    by_event_type = list(summary["execution_cost_observations"].get("quote_coverage_by_event_type") or [])
    assert len(by_event_type) == 3
    by_event_type_map = {str(row.get("event_type")): row for row in by_event_type}
    assert by_event_type_map["candidate_seen"]["rows_without_two_sided_quote"] == 1
    assert by_event_type_map["book_snapshot"]["rows_without_two_sided_quote"] == 1
    assert by_event_type_map["order_terminal"]["rows_without_two_sided_quote"] == 1
    missing_buckets = dict(summary["execution_cost_observations"].get("top_missing_coverage_buckets") or {})
    by_market = list(missing_buckets.get("by_market") or [])
    assert by_market
    assert by_market[0]["bucket"] == "KXHIGHBOS-26APR22-B75"
    assert by_market[0]["rows_without_two_sided_quote"] == 3
    by_side = list(missing_buckets.get("by_side") or [])
    assert by_side
    assert by_side[0]["bucket"] == "unknown"
    assert by_side[0]["rows_without_two_sided_quote"] == 3
    by_market_side = list(missing_buckets.get("by_market_side") or [])
    assert by_market_side
    assert by_market_side[0]["bucket"] == "KXHIGHBOS-26APR22-B75|unknown"
    recommended_targets = dict(summary.get("recommended_shadow_quote_probe_targets") or {})
    assert recommended_targets.get("status") == "ready"
    assert int(recommended_targets.get("target_count") or 0) >= 1
    target_keys = list(recommended_targets.get("target_keys") or [])
    assert target_keys
    assert target_keys[0] == "KXHIGHBOS-26APR22-B75"
    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("status") == "ready"
    assert pressure.get("quote_coverage_shortfall") == 0.6
    assert pressure.get("uncovered_market_top5_share") == 1.0
    assert int(pressure.get("low_coverage_wide_spread_ticker_count") or 0) == 1
    assert list(pressure.get("low_coverage_wide_spread_tickers") or []) == ["KXHIGHBOS-26APR22-B75"]
    assert pressure.get("pressure_score") is not None
    assert float(pressure["pressure_score"]) >= 0.8


def test_execution_cost_tape_side_imbalance_markers_heavy_single_side_coverage(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.61,
                "blocked_total": 500,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 200,
            "policy_reason_counts": {"expected_edge_below_min": 150},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 1},
            "markets": {
                "KXHIGHMIA-26APR22-T89": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.24,
                        "best_yes_ask_dollars": 0.26,
                        "best_no_bid_dollars": 0.74,
                        "best_no_ask_dollars": 0.76,
                        "yes_spread_dollars": 0.02,
                    }
                }
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=10)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="side-heavy",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHMIA-26APR22-T89",
                "side": "yes",
                "best_yes_bid_dollars": 0.24,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.03,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHDEN-26APR22-T71",
                "side": "yes",
                "best_yes_bid_dollars": 0.36,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.03,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHSEA-26APR22-T66",
                "side": "yes",
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.03,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHPHX-26APR22-T95",
                "side": "no",
                "best_no_bid_dollars": 0.64,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.03,
                "result": "expected_edge_below_min",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=10,
    )

    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("dominant_uncovered_side") == "yes"
    assert pressure.get("dominant_uncovered_side_share") == 0.75
    assert pressure.get("yes_uncovered_share") == 0.75
    assert pressure.get("no_uncovered_share") == 0.25
    assert pressure.get("side_imbalance_magnitude") == 0.5
    assert pressure.get("side_pressure_score_contribution") == 0.375
    assert pressure.get("pressure_score") is not None
    assert float(pressure["pressure_score"]) > 0.75
    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("market_side_target_selection_active") is True
    assert exclusions.get("excluded_market_side_target_count") == 3
    assert exclusions.get("market_side_targets") == [
        "KXHIGHDEN-26APR22-T71|yes",
        "KXHIGHMIA-26APR22-T89|yes",
        "KXHIGHSEA-26APR22-T66|yes",
    ]
    side_thresholds = dict(dict(exclusions.get("thresholds") or {}).get("market_side_target_selection") or {})
    assert side_thresholds.get("require_dominant_side") is True
    assert side_thresholds.get("max_excluded_market_side_targets") == 12
    market_side_diagnostics = list(exclusions.get("market_side_diagnostics") or [])
    assert market_side_diagnostics
    assert int(exclusions.get("market_side_diagnostics_count") or 0) == len(market_side_diagnostics)

    probe_targets = dict(summary.get("recommended_shadow_quote_probe_targets") or {})
    side_breakdown = dict(probe_targets.get("side_breakdown") or {})
    counts_by_side = dict(side_breakdown.get("counts_by_side") or {})
    assert counts_by_side.get("yes") == 3
    assert counts_by_side.get("no") == 1
    assert counts_by_side.get("unknown") == 0
    assert side_breakdown.get("dominant_side") == "yes"
    assert side_breakdown.get("dominant_side_target_share") == 0.75
    assert side_breakdown.get("side_imbalance_magnitude") == 0.5


def test_execution_cost_tape_side_imbalance_markers_balanced_coverage(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.61,
                "blocked_total": 500,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 200,
            "policy_reason_counts": {"expected_edge_below_min": 150},
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=10)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="side-balanced",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHATL-26APR22-T82",
                "side": "yes",
                "best_yes_bid_dollars": 0.28,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.04,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHDAL-26APR22-T84",
                "side": "yes",
                "best_yes_bid_dollars": 0.38,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.04,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHDET-26APR22-T67",
                "side": "no",
                "best_no_bid_dollars": 0.62,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.04,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHSTL-26APR22-T76",
                "side": "no",
                "best_no_bid_dollars": 0.58,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.04,
                "result": "expected_edge_below_min",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=10,
    )

    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("dominant_uncovered_side") == "mixed"
    assert pressure.get("dominant_uncovered_side_share") == 0.5
    assert pressure.get("yes_uncovered_share") == 0.5
    assert pressure.get("no_uncovered_share") == 0.5
    assert pressure.get("side_imbalance_magnitude") == 0.0
    assert pressure.get("side_pressure_score_contribution") == 0.0
    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("market_side_target_selection_active") is False
    assert exclusions.get("excluded_market_side_target_count") == 0
    assert list(exclusions.get("market_side_targets") or []) == []

    probe_targets = dict(summary.get("recommended_shadow_quote_probe_targets") or {})
    side_breakdown = dict(probe_targets.get("side_breakdown") or {})
    counts_by_side = dict(side_breakdown.get("counts_by_side") or {})
    assert counts_by_side.get("yes") == 2
    assert counts_by_side.get("no") == 2
    assert counts_by_side.get("unknown") == 0
    assert side_breakdown.get("dominant_side") == "mixed"
    assert side_breakdown.get("dominant_side_target_share") == 0.5
    assert side_breakdown.get("side_imbalance_magnitude") == 0.0


def test_execution_cost_tape_market_side_exclusion_targets_are_capped_and_deterministic(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.72,
                "blocked_total": 900,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 350,
            "policy_reason_counts": {"expected_edge_below_min": 300},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 6},
            "markets": {},
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    base_captured_at = now - timedelta(minutes=25)
    ticker_counts = [
        ("KXHIGHAUS-26APR22-T97", 6),
        ("KXHIGHBNA-26APR22-T78", 5),
        ("KXHIGHCMH-26APR22-T69", 4),
        ("KXHIGHELP-26APR22-T91", 3),
        ("KXHIGHFAT-26APR22-T88", 2),
    ]
    events: list[dict[str, object]] = []
    seconds_offset = 0
    for ticker, count in ticker_counts:
        for _ in range(count):
            events.append(
                {
                    "captured_at_utc": (base_captured_at + timedelta(seconds=seconds_offset)).isoformat(),
                    "event_type": "candidate_seen",
                    "market_ticker": ticker,
                    "side": "yes",
                    "best_yes_bid_dollars": 0.28,
                    "best_yes_ask_dollars": None,
                    "spread_dollars": 0.06,
                    "result": "expected_edge_below_min",
                }
            )
            seconds_offset += 1
    events.append(
        {
            "captured_at_utc": (base_captured_at + timedelta(seconds=seconds_offset)).isoformat(),
            "event_type": "candidate_seen",
            "market_ticker": "KXHIGHGRR-26APR22-T74",
            "side": "no",
            "best_no_bid_dollars": 0.61,
            "best_no_ask_dollars": None,
            "spread_dollars": 0.06,
            "result": "expected_edge_below_min",
        }
    )
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="side-target-cap",
        events=events,
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        min_ticker_rows_for_exclusion=1,
        max_excluded_tickers=3,
        max_tickers=20,
    )

    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("market_side_target_selection_active") is True
    assert exclusions.get("excluded_market_side_target_count") == 3
    assert exclusions.get("market_side_targets") == [
        "KXHIGHAUS-26APR22-T97|yes",
        "KXHIGHBNA-26APR22-T78|yes",
        "KXHIGHCMH-26APR22-T69|yes",
    ]
    thresholds = dict(dict(exclusions.get("thresholds") or {}).get("market_side_target_selection") or {})
    assert thresholds.get("max_excluded_market_side_targets") == 3
    diagnostics = list(exclusions.get("market_side_diagnostics") or [])
    assert diagnostics
    assert int(exclusions.get("market_side_diagnostics_count") or 0) == len(diagnostics)
    assert int(exclusions.get("market_side_diagnostics_total_count") or 0) >= len(diagnostics)
    assert len(diagnostics) <= int(thresholds.get("max_market_side_diagnostics") or 0)


def test_execution_cost_tape_market_side_targets_activate_via_execution_pressure_override(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "max_open_positions_reached",
                "largest_blocker_share_of_blocked_raw": 0.74,
                "blocked_total": 1200,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 450,
            "policy_reason_counts": {
                "max_open_positions_reached": 320,
                "expected_edge_below_min": 30,
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=15)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="secondary-side-activation",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHAUS-26APR22-T97",
                "side": "yes",
                "best_yes_bid_dollars": 0.24,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.06,
                "result": "max_open_positions_reached",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHBNA-26APR22-T78",
                "side": "yes",
                "best_yes_bid_dollars": 0.28,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.06,
                "result": "max_open_positions_reached",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHCMH-26APR22-T69",
                "side": "yes",
                "best_yes_bid_dollars": 0.31,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.06,
                "result": "max_open_positions_reached",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHDAL-26APR22-T84",
                "side": "no",
                "best_no_bid_dollars": 0.71,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.06,
                "result": "max_open_positions_reached",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=12,
    )

    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("expected_edge_dominance_active") is False
    assert exclusions.get("market_side_target_selection_active") is True
    assert exclusions.get("market_side_target_selection_activation_mode") == "execution_pressure_override"
    activation_reasons = list(exclusions.get("market_side_target_selection_activation_reasons") or [])
    assert "material_side_pressure" in activation_reasons
    assert "severe_quote_coverage_shortfall" in activation_reasons
    assert exclusions.get("market_side_target_selection_execution_pressure_route_active") is True
    assert exclusions.get("status") == "active"
    assert exclusions.get("excluded_market_side_target_count") == 3
    assert exclusions.get("market_side_targets") == [
        "KXHIGHAUS-26APR22-T97|yes",
        "KXHIGHBNA-26APR22-T78|yes",
        "KXHIGHCMH-26APR22-T69|yes",
    ]


def test_execution_cost_tape_market_side_targets_do_not_activate_when_routes_not_met(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "max_open_positions_reached",
                "largest_blocker_share_of_blocked_raw": 0.71,
                "blocked_total": 800,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 240,
            "policy_reason_counts": {
                "max_open_positions_reached": 170,
                "expected_edge_below_min": 20,
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=15)
    events: list[dict[str, object]] = []
    for _ in range(3):
        events.append(
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHEWR-26APR22-T83",
                "side": "yes",
                "best_yes_bid_dollars": 0.33,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.03,
                "result": "max_open_positions_reached",
            }
        )
    covered_rows = [
        "KXHIGHMIA-26APR22-T89",
        "KXHIGHORF-26APR22-T88",
        "KXHIGHSAN-26APR22-T94",
    ]
    for ticker in covered_rows:
        for _ in range(3):
            events.append(
                {
                    "captured_at_utc": captured_at.isoformat(),
                    "event_type": "book_snapshot",
                    "market_ticker": ticker,
                    "side": "no",
                    "best_yes_bid_dollars": 0.42,
                    "best_yes_ask_dollars": 0.44,
                    "best_no_bid_dollars": 0.56,
                    "best_no_ask_dollars": 0.58,
                    "spread_dollars": 0.02,
                    "result": "max_open_positions_reached",
                }
            )
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="secondary-side-inactive",
        events=events,
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=12,
    )

    exclusions = dict(summary.get("recommended_exclusions") or {})
    assert exclusions.get("expected_edge_dominance_active") is False
    assert exclusions.get("market_side_target_selection_active") is False
    assert exclusions.get("market_side_target_selection_activation_mode") == "inactive"
    activation_reasons = list(exclusions.get("market_side_target_selection_activation_reasons") or [])
    assert "expected_edge_dominance_inactive" in activation_reasons
    assert "execution_pressure_context_not_severe" in activation_reasons
    assert exclusions.get("market_side_target_selection_execution_pressure_route_active") is False
    assert exclusions.get("status") == "inactive"
    assert exclusions.get("excluded_market_side_target_count") == 0
    assert list(exclusions.get("market_side_targets") or []) == []


def test_execution_cost_tape_side_imbalance_markers_missing_side_fallback(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.61,
                "blocked_total": 500,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 200,
            "policy_reason_counts": {"expected_edge_below_min": 150},
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=10)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="side-unknown",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHCLT-26APR22-T85",
                "best_yes_bid_dollars": 0.31,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.05,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHORL-26APR22-T92",
                "best_yes_bid_dollars": 0.35,
                "best_yes_ask_dollars": None,
                "spread_dollars": 0.05,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHSLC-26APR22-T64",
                "best_no_bid_dollars": 0.67,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.05,
                "result": "expected_edge_below_min",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=10,
    )

    pressure = dict(summary.get("execution_siphon_pressure") or {})
    assert pressure.get("dominant_uncovered_side") == "unknown"
    assert pressure.get("dominant_uncovered_side_share") is None
    assert pressure.get("yes_uncovered_share") == 0.0
    assert pressure.get("no_uncovered_share") == 0.0
    assert pressure.get("side_imbalance_magnitude") is None
    assert pressure.get("side_pressure_score_contribution") == 0.0

    probe_targets = dict(summary.get("recommended_shadow_quote_probe_targets") or {})
    side_breakdown = dict(probe_targets.get("side_breakdown") or {})
    counts_by_side = dict(side_breakdown.get("counts_by_side") or {})
    assert counts_by_side.get("yes") == 0
    assert counts_by_side.get("no") == 0
    assert int(counts_by_side.get("unknown") or 0) >= 1
    assert side_breakdown.get("dominant_side") == "unknown"
    assert side_breakdown.get("dominant_side_target_share") is None
    assert side_breakdown.get("side_imbalance_magnitude") is None


def test_execution_cost_tape_reports_missing_baseline_for_trend_markers(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.58,
                "blocked_total": 100,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 25,
            "policy_reason_counts": {"expected_edge_below_min": 20},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 1, "events_processed": 3},
            "markets": {
                "KXHIGHBOS-26APR22-B75": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.31,
                        "best_yes_ask_dollars": 0.49,
                        "best_no_bid_dollars": 0.51,
                        "best_no_ask_dollars": 0.69,
                        "yes_spread_dollars": 0.18,
                    }
                }
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=30)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="trend-missing-baseline",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.31,
                "best_yes_ask_dollars": 0.49,
                "best_no_bid_dollars": 0.51,
                "best_no_ask_dollars": 0.69,
                "spread_dollars": 0.18,
                "result": "expected_edge_below_min",
            }
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=5,
    )

    trend = dict(summary.get("execution_siphon_trend") or {})
    assert trend.get("status") == "missing_baseline"
    assert trend.get("baseline_file") == str(out_dir / "health" / "execution_cost_tape_latest.json")
    assert trend.get("quote_coverage_ratio_delta") is None
    assert trend.get("candidate_rows_delta") is None
    assert trend.get("siphon_pressure_score_delta") is None
    assert trend.get("quote_coverage_shortfall_delta") is None
    assert trend.get("uncovered_market_top5_share_delta") is None
    assert trend.get("low_coverage_wide_spread_ticker_count_delta") is None
    assert trend.get("worsening_component_count") == 0
    assert trend.get("improving_component_count") == 0
    assert trend.get("trend_direction") == 0
    assert trend.get("trend_label") == "unknown"
    assert trend.get("improving") is False
    assert trend.get("worsening") is False


def test_execution_cost_tape_reports_trend_deltas_and_worsening(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    health_dir = out_dir / "health"
    _write_json(
        health_dir / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "calibration_readiness": {
                "status": "green",
                "candidate_rows": 1,
                "quote_coverage_ratio": 1.0,
            },
            "execution_siphon_pressure": {
                "status": "ready",
                "quote_coverage_shortfall": 0.0,
                "uncovered_market_top5_share": 0.0,
                "low_coverage_wide_spread_ticker_count": 0,
                "pressure_score": 0.0,
            },
        },
    )
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.58,
                "blocked_total": 100,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 25,
            "policy_reason_counts": {"expected_edge_below_min": 20},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 1, "events_processed": 3},
            "markets": {
                "KXHIGHBOS-26APR22-B75": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.31,
                        "best_yes_ask_dollars": 0.49,
                        "best_no_bid_dollars": 0.51,
                        "best_no_ask_dollars": 0.69,
                        "yes_spread_dollars": 0.18,
                    }
                }
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc) - timedelta(minutes=30)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="trend-ready",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.31,
                "best_yes_ask_dollars": None,
                "best_no_bid_dollars": None,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.18,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.3,
                "best_yes_ask_dollars": None,
                "best_no_bid_dollars": None,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.2,
                "result": "expected_edge_below_min",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHBOS-26APR22-B75",
                "best_yes_bid_dollars": 0.29,
                "best_yes_ask_dollars": None,
                "best_no_bid_dollars": None,
                "best_no_ask_dollars": None,
                "spread_dollars": 0.21,
                "result": "expected_edge_below_min",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=5,
    )

    trend = dict(summary.get("execution_siphon_trend") or {})
    assert trend.get("status") == "ready"
    assert trend.get("baseline_file") == str(health_dir / "execution_cost_tape_latest.json")
    assert trend.get("quote_coverage_ratio_delta") == -1.0
    assert trend.get("candidate_rows_delta") == 2
    assert trend.get("siphon_pressure_score_delta") is not None
    assert float(trend["siphon_pressure_score_delta"]) > 0
    assert trend.get("quote_coverage_shortfall_delta") is not None
    assert float(trend["quote_coverage_shortfall_delta"]) > 0
    assert trend.get("uncovered_market_top5_share_delta") is not None
    assert float(trend["uncovered_market_top5_share_delta"]) > 0
    assert trend.get("low_coverage_wide_spread_ticker_count_delta") == 1
    assert int(trend.get("worsening_component_count") or 0) >= 2
    assert "siphon_pressure_score" in list(trend.get("worsening_components") or [])
    assert trend.get("trend_direction") == 1
    assert trend.get("trend_label") == "worsening"
    assert trend.get("improving") is False
    assert trend.get("worsening") is True


def test_execution_cost_tape_reports_trend_improving_markers(tmp_path: Path) -> None:
    out_dir = tmp_path / "outputs"
    health_dir = out_dir / "health"
    _write_json(
        health_dir / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "calibration_readiness": {
                "status": "yellow",
                "candidate_rows": 5,
                "quote_coverage_ratio": 0.2,
            },
            "execution_siphon_pressure": {
                "status": "ready",
                "quote_coverage_shortfall": 0.4,
                "uncovered_market_top5_share": 0.9,
                "low_coverage_wide_spread_ticker_count": 5,
                "pressure_score": 0.85,
            },
        },
    )
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    _write_json(
        out_dir / "checkpoints" / "blocker_audit_168h_latest.json",
        {
            "status": "ready",
            "headline": {
                "largest_blocker_reason_raw": "expected_edge_below_min",
                "largest_blocker_share_of_blocked_raw": 0.58,
                "blocked_total": 100,
            },
        },
    )
    _write_json(
        out_dir / "kalshi_temperature_trade_intents_summary_20260422_115900.json",
        {
            "status": "ready",
            "intents_blocked": 25,
            "policy_reason_counts": {"expected_edge_below_min": 20},
        },
    )
    _write_json(
        out_dir / "kalshi_ws_state_latest.json",
        {
            "summary": {"status": "ready", "market_count": 2, "events_processed": 8},
            "markets": {
                "KXHIGHCHI-26APR22-T73": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.41,
                        "best_yes_ask_dollars": 0.43,
                        "best_no_bid_dollars": 0.57,
                        "best_no_ask_dollars": 0.59,
                        "yes_spread_dollars": 0.02,
                    }
                },
                "KXHIGHNYC-26APR22-T72": {
                    "top_of_book": {
                        "best_yes_bid_dollars": 0.48,
                        "best_yes_ask_dollars": 0.50,
                        "best_no_bid_dollars": 0.50,
                        "best_no_ask_dollars": 0.52,
                        "yes_spread_dollars": 0.02,
                    }
                },
            },
        },
    )

    journal_path = out_dir / "kalshi_execution_journal.sqlite3"
    captured_at = now - timedelta(minutes=20)
    append_execution_events(
        journal_db_path=journal_path,
        default_run_id="trend-improving",
        events=[
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "candidate_seen",
                "market_ticker": "KXHIGHCHI-26APR22-T73",
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": 0.43,
                "best_no_bid_dollars": 0.57,
                "best_no_ask_dollars": 0.59,
                "spread_dollars": 0.02,
                "result": "dry_run_ready",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "book_snapshot",
                "market_ticker": "KXHIGHNYC-26APR22-T72",
                "best_yes_bid_dollars": 0.48,
                "best_yes_ask_dollars": 0.50,
                "best_no_bid_dollars": 0.50,
                "best_no_ask_dollars": 0.52,
                "spread_dollars": 0.03,
                "result": "dry_run_ready",
            },
            {
                "captured_at_utc": captured_at.isoformat(),
                "event_type": "order_terminal",
                "market_ticker": "KXHIGHCHI-26APR22-T73",
                "best_yes_bid_dollars": 0.41,
                "best_yes_ask_dollars": 0.43,
                "best_no_bid_dollars": 0.57,
                "best_no_ask_dollars": 0.59,
                "spread_dollars": 0.01,
                "result": "dry_run_ready",
            },
        ],
    )

    summary = run_kalshi_temperature_execution_cost_tape(
        output_dir=str(out_dir),
        min_candidate_samples=1,
        max_tickers=5,
    )

    trend = dict(summary.get("execution_siphon_trend") or {})
    assert trend.get("status") == "ready"
    assert trend.get("quote_coverage_ratio_delta") is not None
    assert float(trend["quote_coverage_ratio_delta"]) > 0
    assert trend.get("siphon_pressure_score_delta") is not None
    assert float(trend["siphon_pressure_score_delta"]) < 0
    assert trend.get("quote_coverage_shortfall_delta") is not None
    assert float(trend["quote_coverage_shortfall_delta"]) < 0
    assert trend.get("uncovered_market_top5_share_delta") is not None
    assert float(trend["uncovered_market_top5_share_delta"]) < 0
    assert trend.get("low_coverage_wide_spread_ticker_count_delta") is not None
    assert int(trend["low_coverage_wide_spread_ticker_count_delta"]) < 0
    assert int(trend.get("improving_component_count") or 0) >= 2
    assert "siphon_pressure_score" in list(trend.get("improving_components") or [])
    assert trend.get("trend_direction") == -1
    assert trend.get("trend_label") == "improving"
    assert trend.get("improving") is True
    assert trend.get("worsening") is False
