#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import time
from pathlib import Path
from typing import Any


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _safe_load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _latest_path(pattern: str) -> Path | None:
    matches = [Path(p) for p in glob.glob(pattern)]
    if not matches:
        return None
    matches.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0.0)
    return matches[-1]


def _age_seconds(path: Path | None, now_epoch: float) -> int | None:
    if path is None or not path.exists():
        return None
    try:
        return int(max(0, round(now_epoch - path.stat().st_mtime)))
    except OSError:
        return None


def _count_uniques_from_intents_csv(path_text: str) -> dict[str, int]:
    path = Path(_normalize_text(path_text))
    if not path.exists():
        return {"rows": 0, "unique_market_tickers": 0, "unique_underlyings": 0, "unique_stations": 0}
    rows = 0
    market_tickers: set[str] = set()
    underlyings: set[str] = set()
    stations: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows += 1
                ticker = _normalize_text(row.get("market_ticker"))
                underlying = _normalize_text(row.get("underlying_key")) or _normalize_text(
                    row.get("temperature_underlying_key")
                )
                station = _normalize_text(row.get("settlement_station")).upper()
                if ticker:
                    market_tickers.add(ticker)
                if underlying:
                    underlyings.add(underlying)
                if station:
                    stations.add(station)
    except OSError:
        return {"rows": 0, "unique_market_tickers": 0, "unique_underlyings": 0, "unique_stations": 0}
    return {
        "rows": int(rows),
        "unique_market_tickers": int(len(market_tickers)),
        "unique_underlyings": int(len(underlyings)),
        "unique_stations": int(len(stations)),
    }


def _extract_shadow_block(out_dir: Path, now_epoch: float) -> dict[str, Any]:
    shadow_path = _latest_path(str(out_dir / "kalshi_temperature_shadow_watch_summary_*.json"))
    payload = _safe_load_json(shadow_path)
    cycle = {}
    cycle_rows = payload.get("cycle_summaries")
    if isinstance(cycle_rows, list) and cycle_rows:
        last = cycle_rows[-1]
        if isinstance(last, dict):
            cycle = last
    return {
        "file": shadow_path.name if shadow_path else "",
        "age_seconds": _age_seconds(shadow_path, now_epoch),
        "status": _normalize_text(cycle.get("status")) or _normalize_text(payload.get("status")),
        "intents_total": _parse_int(cycle.get("intents_total")) or 0,
        "intents_approved": _parse_int(cycle.get("intents_approved")) or 0,
        "planned_orders": _parse_int(cycle.get("planned_orders")) or 0,
    }


def _extract_profile_block(profile_dir: Path, now_epoch: float) -> dict[str, Any]:
    intents_path = _latest_path(str(profile_dir / "kalshi_temperature_trade_intents_summary_*.json"))
    plans_path = _latest_path(str(profile_dir / "kalshi_temperature_trade_plan_summary_*.json"))
    intents = _safe_load_json(intents_path)
    plans = _safe_load_json(plans_path)
    reason_counts = intents.get("policy_reason_counts") if isinstance(intents.get("policy_reason_counts"), dict) else {}
    ints_csv = _normalize_text(intents.get("output_csv"))
    uniques = _count_uniques_from_intents_csv(ints_csv)

    return {
        "profile_name": profile_dir.name.removeprefix("explore_"),
        "profile_dir": str(profile_dir),
        "intents_file": intents_path.name if intents_path else "",
        "intents_age_seconds": _age_seconds(intents_path, now_epoch),
        "plans_file": plans_path.name if plans_path else "",
        "plans_age_seconds": _age_seconds(plans_path, now_epoch),
        "actionable_constraint_rows": _parse_int(intents.get("actionable_constraint_rows")) or 0,
        "expanded_actionable_intents": _parse_int(intents.get("expanded_actionable_intents")) or 0,
        "approved_count": _parse_int(reason_counts.get("approved")) or 0,
        "stale_count": _parse_int(reason_counts.get("metar_observation_stale")) or 0,
        "underlying_cap_blocked_count": _parse_int(reason_counts.get("underlying_exposure_cap_reached")) or 0,
        "interval_overlap_blocked_count": _parse_int(reason_counts.get("no_side_interval_overlap_still_possible")) or 0,
        "planned_orders": _parse_int(plans.get("planned_orders")) or 0,
        "intent_uniques": uniques,
    }


def _discover_profile_blocks(alpha_dir: Path, now_epoch: float) -> dict[str, dict[str, Any]]:
    profile_dirs = (
        sorted([path for path in alpha_dir.iterdir() if path.is_dir() and path.name.startswith("explore_")])
        if alpha_dir.exists()
        else []
    )
    blocks: dict[str, dict[str, Any]] = {}
    for profile_dir in profile_dirs:
        block = _extract_profile_block(profile_dir, now_epoch)
        profile_name = _normalize_text(block.get("profile_name")) or profile_dir.name.removeprefix("explore_")
        blocks[profile_name] = block
    # Backward-compatible fallback if alpha worker directories are missing.
    if not blocks:
        for profile_name in ("default", "relaxed_interval"):
            blocks[profile_name] = _extract_profile_block(alpha_dir / f"explore_{profile_name}", now_epoch)
    return dict(sorted(blocks.items(), key=lambda item: item[0]))


def _summarize_profile_ranking(all_profiles: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rows = list(all_profiles.values())
    ranked_by_approved = sorted(
        rows,
        key=lambda row: (
            -(int(row.get("approved_count") or 0)),
            -(int(row.get("planned_orders") or 0)),
            _normalize_text(row.get("profile_name")),
        ),
    )
    ranked_by_uniques = sorted(
        rows,
        key=lambda row: (
            -(int((row.get("intent_uniques") or {}).get("unique_market_tickers") or 0)),
            -(int((row.get("intent_uniques") or {}).get("unique_underlyings") or 0)),
            _normalize_text(row.get("profile_name")),
        ),
    )
    return {
        "profile_count": int(len(rows)),
        "active_profile_count": int(
            len(
                [
                    row
                    for row in rows
                    if _normalize_text(row.get("intents_file")) or _normalize_text(row.get("plans_file"))
                ]
            )
        ),
        "by_approved_count": [
            {
                "profile_name": _normalize_text(row.get("profile_name")),
                "approved_count": int(row.get("approved_count") or 0),
                "planned_orders": int(row.get("planned_orders") or 0),
                "unique_market_tickers": int((row.get("intent_uniques") or {}).get("unique_market_tickers") or 0),
            }
            for row in ranked_by_approved[:8]
        ],
        "by_unique_market_tickers": [
            {
                "profile_name": _normalize_text(row.get("profile_name")),
                "unique_market_tickers": int((row.get("intent_uniques") or {}).get("unique_market_tickers") or 0),
                "unique_underlyings": int((row.get("intent_uniques") or {}).get("unique_underlyings") or 0),
                "approved_count": int(row.get("approved_count") or 0),
            }
            for row in ranked_by_uniques[:8]
        ],
    }


def _extract_live_status(out_dir: Path, now_epoch: float) -> dict[str, Any]:
    status_path = out_dir / "health" / "live_status_latest.json"
    payload = _safe_load_json(status_path)
    scan_budget = payload.get("scan_budget") if isinstance(payload.get("scan_budget"), dict) else {}
    trigger_flags = payload.get("trigger_flags") if isinstance(payload.get("trigger_flags"), dict) else {}
    replan = payload.get("replan_cooldown") if isinstance(payload.get("replan_cooldown"), dict) else {}
    load_milli = _parse_int(scan_budget.get("load_per_vcpu_milli"))
    if load_milli is None:
        load_ratio = _parse_float(scan_budget.get("load_per_vcpu"))
        if isinstance(load_ratio, float):
            load_milli = int(round(load_ratio * 1000.0))
    return {
        "file": status_path.name if status_path.exists() else "",
        "age_seconds": _age_seconds(status_path if status_path.exists() else None, now_epoch),
        "status": _normalize_text(payload.get("status")) or "unknown",
        "effective_max_markets": _parse_int(scan_budget.get("effective_max_markets")) or 0,
        "next_max_markets": _parse_int(scan_budget.get("next_max_markets")) or 0,
        "load_per_vcpu_milli": int(load_milli) if isinstance(load_milli, int) else 0,
        "intents_total_hint": _parse_int(scan_budget.get("intents_total_hint")) or -1,
        "replan_cooldown_effective_minutes": _parse_float(replan.get("effective_minutes")) or 0.0,
        "replan_cooldown_next_minutes": _parse_float(replan.get("next_minutes")) or 0.0,
        "replan_cooldown_input_count": _parse_int(replan.get("input_count")) or 0,
        "replan_cooldown_blocked_count": _parse_int(replan.get("blocked_count")) or 0,
        "replan_cooldown_backstop_released_count": _parse_int(replan.get("backstop_released_count")) or 0,
        "replan_cooldown_min_orders_backstop_effective": _parse_int(replan.get("min_orders_backstop_effective")) or 0,
        "replan_cooldown_blocked_ratio": _parse_float(replan.get("blocked_ratio")) or 0.0,
        "shadow_resolved_first": bool(trigger_flags.get("shadow_resolved_first")),
    }


def _extract_policy_block(path: Path, now_epoch: float) -> dict[str, Any]:
    payload = _safe_load_json(path)
    station_overrides = payload.get("station_max_age_minutes") if isinstance(payload.get("station_max_age_minutes"), dict) else {}
    station_hour_overrides = (
        payload.get("station_local_hour_max_age_minutes")
        if isinstance(payload.get("station_local_hour_max_age_minutes"), dict)
        else {}
    )
    flat_hour_count = sum(len(v) for v in station_hour_overrides.values() if isinstance(v, dict))
    top_overrides = sorted(
        ((str(k), float(v)) for k, v in station_overrides.items() if isinstance(v, (int, float))),
        key=lambda item: abs(item[1]),
        reverse=True,
    )[:10]
    return {
        "file": path.name if path.exists() else "",
        "age_seconds": _age_seconds(path if path.exists() else None, now_epoch),
        "generated_at": _normalize_text(payload.get("generated_at")),
        "rows_evaluated": _parse_int(payload.get("rows_evaluated")) or 0,
        "station_override_count": int(len(station_overrides)),
        "station_hour_override_count": int(flat_hour_count),
        "top_station_overrides": [{"station": k, "max_age_minutes": v} for k, v in top_overrides],
    }


def _extract_latest_report_block(out_dir: Path, now_epoch: float, stem: str) -> dict[str, Any]:
    path = _latest_path(str(out_dir / f"{stem}_*.json"))
    payload = _safe_load_json(path)
    block: dict[str, Any] = {
        "file": path.name if path else "",
        "age_seconds": _age_seconds(path, now_epoch),
    }
    if stem == "kalshi_temperature_bankroll_validation":
        viability = payload.get("viability_summary") if isinstance(payload.get("viability_summary"), dict) else {}
        opportunity = payload.get("opportunity_breadth") if isinstance(payload.get("opportunity_breadth"), dict) else {}
        concentration = payload.get("concentration_checks") if isinstance(payload.get("concentration_checks"), dict) else {}
        anti_misleading = (
            payload.get("anti_misleading_guards") if isinstance(payload.get("anti_misleading_guards"), dict) else {}
        )
        deployment_basis = viability.get("deployment_headline_basis") if isinstance(viability.get("deployment_headline_basis"), dict) else {}
        block.update(
            {
                "meaningful_deploy": bool(viability.get("could_reference_bankroll_have_been_deployed_meaningfully")),
                "roi_on_reference_bankroll": viability.get("what_return_would_have_been_produced_on_bankroll"),
                "main_limiting_factor": _normalize_text(viability.get("main_limiting_factor")),
                "next_missing_alpha_layer": _normalize_text(viability.get("next_missing_alpha_layer_preventing_profit_machine")),
                "resolved_unique_market_sides": _parse_int(opportunity.get("resolved_unique_market_sides")) or 0,
                "resolved_unique_shadow_orders": _parse_int(opportunity.get("resolved_unique_shadow_orders")) or 0,
                "resolved_unique_underlying_families": _parse_int(opportunity.get("resolved_unique_underlying_families")) or 0,
                "resolved_planned_rows": _parse_int(opportunity.get("resolved_planned_rows")) or 0,
                "repeated_entry_multiplier": _parse_float(opportunity.get("repeated_entry_multiplier")),
                "concentration_warning": bool(concentration.get("concentration_warning")),
                "default_prediction_quality_basis": _normalize_text(anti_misleading.get("default_prediction_quality_basis")),
                "default_deployment_quality_basis": _normalize_text(anti_misleading.get("default_deployment_quality_basis")),
                "deployment_headline_basis": {
                    "sizing_model": _normalize_text(deployment_basis.get("sizing_model")),
                    "aggregation_layer": _normalize_text(deployment_basis.get("aggregation_layer")),
                    "slippage_bps": _parse_float(deployment_basis.get("slippage_bps")),
                },
            }
        )
    elif stem == "kalshi_temperature_alpha_gap_report":
        gap = payload.get("alpha_gap_report") if isinstance(payload.get("alpha_gap_report"), dict) else payload
        likely = (
            gap.get("likely_next_highest_impact_signal_expansion")
            if isinstance(gap.get("likely_next_highest_impact_signal_expansion"), dict)
            else {}
        )
        ceiling = gap.get("opportunity_ceiling_estimate") if isinstance(gap.get("opportunity_ceiling_estimate"), dict) else {}
        block.update(
            {
                "next_signal_expansion": _normalize_text(likely.get("name")),
                "next_signal_expansion_impact": _normalize_text(likely.get("expected_impact")),
                "ceiling_summary": _normalize_text(ceiling.get("ceiling_summary")),
            }
        )
    elif stem == "kalshi_temperature_live_readiness":
        executive = payload.get("executive_summary") if isinstance(payload.get("executive_summary"), dict) else {}
        overall = payload.get("overall_live_readiness") if isinstance(payload.get("overall_live_readiness"), dict) else {}
        failing_horizons = overall.get("failing_horizons") if isinstance(overall.get("failing_horizons"), list) else []
        block.update(
            {
                "recommendation": _normalize_text(executive.get("recommendation") or overall.get("recommendation")),
                "ready_for_small_live_pilot": bool(
                    executive.get("ready_for_small_live_pilot")
                    if "ready_for_small_live_pilot" in executive
                    else overall.get("ready_for_small_live_pilot")
                ),
                "ready_for_scaled_live": bool(
                    executive.get("ready_for_scaled_live")
                    if "ready_for_scaled_live" in executive
                    else overall.get("ready_for_scaled_live")
                ),
                "earliest_passing_horizon": _normalize_text(overall.get("earliest_passing_horizon")),
                "shortest_horizon_ready": _normalize_text(executive.get("shortest_horizon_ready")),
                "shortest_horizon_status": _normalize_text(executive.get("shortest_horizon_status")),
                "failing_horizon_count": int(len(failing_horizons)),
            }
        )
    elif stem == "kalshi_temperature_go_live_gate":
        failed_horizons = payload.get("failed_horizons") if isinstance(payload.get("failed_horizons"), list) else []
        block.update(
            {
                "gate_status": _normalize_text(payload.get("gate_status")),
                "recommendation": _normalize_text(payload.get("recommendation")),
                "ready_for_small_live_pilot": bool(payload.get("ready_for_small_live_pilot")),
                "ready_for_scaled_live": bool(payload.get("ready_for_scaled_live")),
                "failed_horizon_count": _parse_int(payload.get("failed_horizon_count")) or 0,
                "failed_horizons": failed_horizons[:8],
            }
        )
    return block


def _extract_constraint_summary_block(out_dir: Path, now_epoch: float) -> dict[str, Any]:
    path = _latest_path(str(out_dir / "kalshi_temperature_constraint_scan_summary_*.json"))
    payload = _safe_load_json(path)
    return {
        "file": path.name if path else "",
        "age_seconds": _age_seconds(path, now_epoch),
        "markets_processed": _parse_int(payload.get("markets_processed")) or 0,
        "markets_emitted": _parse_int(payload.get("markets_emitted")) or 0,
        "forecast_modeled_count": _parse_int(payload.get("forecast_modeled_count")) or 0,
        "taf_ready_count": _parse_int(payload.get("taf_ready_count")) or 0,
        "speci_recent_count": _parse_int(payload.get("speci_recent_count")) or 0,
        "speci_shock_active_count": _parse_int(payload.get("speci_shock_active_count")) or 0,
        "monotonic_violation_count": _parse_int(payload.get("monotonic_violation_count")) or 0,
        "exact_chain_violation_count": _parse_int(payload.get("exact_chain_violation_count")) or 0,
        "range_family_conflict_count": _parse_int(payload.get("range_family_conflict_count")) or 0,
    }


def _extract_ws_state_block(out_dir: Path, now_epoch: float) -> dict[str, Any]:
    collect_path = _latest_path(str(out_dir / "kalshi_ws_state_collect_summary_*.json"))
    replay_path = _latest_path(str(out_dir / "kalshi_ws_state_summary_*.json"))
    summary_path = collect_path or replay_path
    summary_payload = _safe_load_json(summary_path)

    ws_state_path_text = _normalize_text(summary_payload.get("ws_state_json"))
    ws_state_path = Path(ws_state_path_text) if ws_state_path_text else (out_dir / "kalshi_ws_state_latest.json")
    ws_state_payload = _safe_load_json(ws_state_path if ws_state_path.exists() else None)
    ws_state_summary = ws_state_payload.get("summary") if isinstance(ws_state_payload.get("summary"), dict) else {}

    market_count = _parse_int(summary_payload.get("market_count"))
    if market_count is None:
        market_count = _parse_int(ws_state_summary.get("market_count")) or 0
    status = _normalize_text(summary_payload.get("status"))
    if not status:
        status = _normalize_text(ws_state_summary.get("status")) or "unknown"

    return {
        "summary_file": summary_path.name if summary_path else "",
        "summary_age_seconds": _age_seconds(summary_path, now_epoch),
        "ws_state_json": str(ws_state_path) if ws_state_path else "",
        "ws_state_age_seconds": _age_seconds(ws_state_path if ws_state_path.exists() else None, now_epoch),
        "status": status,
        "market_count": int(market_count or 0),
        "desynced_market_count": _parse_int(summary_payload.get("desynced_market_count"))
        or _parse_int(ws_state_summary.get("desynced_market_count"))
        or 0,
        "events_logged": _parse_int(summary_payload.get("events_logged")) or 0,
        "reconnects": _parse_int(summary_payload.get("reconnects")) or 0,
        "last_event_age_seconds": _parse_float(summary_payload.get("last_event_age_seconds"))
        if _parse_float(summary_payload.get("last_event_age_seconds")) is not None
        else _parse_float(ws_state_summary.get("last_event_age_seconds")),
        "last_error_kind": _normalize_text(summary_payload.get("last_error_kind")),
        "fallback_state_used": bool(summary_payload.get("fallback_state_used")),
        "market_tickers_count": len(summary_payload.get("market_tickers"))
        if isinstance(summary_payload.get("market_tickers"), list)
        else 0,
    }


def _build_conservative_headline(
    *,
    bankroll_block: dict[str, Any],
    alpha_gap_block: dict[str, Any],
    live_readiness_block: dict[str, Any],
    go_live_gate_block: dict[str, Any],
) -> dict[str, Any]:
    prediction_quality_basis = (
        _normalize_text(bankroll_block.get("default_prediction_quality_basis")) or "unique_market_side"
    )
    deployment_quality_basis = (
        _normalize_text(bankroll_block.get("default_deployment_quality_basis")) or "underlying_family_aggregated"
    )
    return {
        "window_semantics": "rolling",
        "prediction_quality_basis": prediction_quality_basis,
        "deployment_quality_basis": deployment_quality_basis,
        "shadow_settled_is_not_live": True,
        "metrics": {
            "resolved_unique_market_sides": int(bankroll_block.get("resolved_unique_market_sides") or 0),
            "resolved_unique_underlying_families": int(bankroll_block.get("resolved_unique_underlying_families") or 0),
            "resolved_unique_shadow_orders": int(bankroll_block.get("resolved_unique_shadow_orders") or 0),
            "resolved_planned_rows": int(bankroll_block.get("resolved_planned_rows") or 0),
            "repeated_entry_multiplier": bankroll_block.get("repeated_entry_multiplier"),
            "concentration_warning": bool(bankroll_block.get("concentration_warning")),
            "meaningful_deploy": bool(bankroll_block.get("meaningful_deploy")),
            "roi_on_reference_bankroll": bankroll_block.get("roi_on_reference_bankroll"),
            "main_limiting_factor": _normalize_text(bankroll_block.get("main_limiting_factor")),
            "next_missing_alpha_layer": _normalize_text(
                bankroll_block.get("next_missing_alpha_layer") or alpha_gap_block.get("next_signal_expansion")
            ),
        },
        "go_live_snapshot": {
            "gate_status": _normalize_text(go_live_gate_block.get("gate_status")),
            "gate_recommendation": _normalize_text(go_live_gate_block.get("recommendation")),
            "ready_for_small_live_pilot": bool(go_live_gate_block.get("ready_for_small_live_pilot")),
            "ready_for_scaled_live": bool(go_live_gate_block.get("ready_for_scaled_live")),
            "live_readiness_recommendation": _normalize_text(live_readiness_block.get("recommendation")),
            "earliest_passing_horizon": _normalize_text(live_readiness_block.get("earliest_passing_horizon")),
        },
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    alpha_dir = Path(args.alpha_worker_dir)
    now_epoch = time.time()

    live_status = _extract_live_status(out_dir, now_epoch)
    shadow = _extract_shadow_block(out_dir, now_epoch)
    all_profiles = _discover_profile_blocks(alpha_dir, now_epoch)
    default_profile = all_profiles.get("default", _extract_profile_block(alpha_dir / "explore_default", now_epoch))
    relaxed_profile = all_profiles.get(
        "relaxed_interval", _extract_profile_block(alpha_dir / "explore_relaxed_interval", now_epoch)
    )
    profile_rankings = _summarize_profile_ranking(all_profiles)
    policy_block = _extract_policy_block(Path(args.policy_json), now_epoch)
    constraint_summary = _extract_constraint_summary_block(out_dir, now_epoch)
    ws_state = _extract_ws_state_block(out_dir, now_epoch)
    bankroll_block = _extract_latest_report_block(out_dir, now_epoch, "kalshi_temperature_bankroll_validation")
    alpha_gap_block = _extract_latest_report_block(out_dir, now_epoch, "kalshi_temperature_alpha_gap_report")
    live_readiness_block = _extract_latest_report_block(out_dir, now_epoch, "kalshi_temperature_live_readiness")
    go_live_gate_block = _extract_latest_report_block(out_dir, now_epoch, "kalshi_temperature_go_live_gate")
    conservative_headline = _build_conservative_headline(
        bankroll_block=bankroll_block,
        alpha_gap_block=alpha_gap_block,
        live_readiness_block=live_readiness_block,
        go_live_gate_block=go_live_gate_block,
    )

    profile_delta = {
        "approved_delta_relaxed_minus_default": int(relaxed_profile["approved_count"] - default_profile["approved_count"]),
        "planned_orders_delta_relaxed_minus_default": int(relaxed_profile["planned_orders"] - default_profile["planned_orders"]),
        "unique_market_tickers_delta_relaxed_minus_default": int(
            relaxed_profile["intent_uniques"]["unique_market_tickers"] - default_profile["intent_uniques"]["unique_market_tickers"]
        ),
        "underlying_cap_blocked_delta_relaxed_minus_default": int(
            relaxed_profile["underlying_cap_blocked_count"] - default_profile["underlying_cap_blocked_count"]
        ),
        "interval_overlap_blocked_delta_relaxed_minus_default": int(
            relaxed_profile["interval_overlap_blocked_count"] - default_profile["interval_overlap_blocked_count"]
        ),
    }

    payload = {
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch)),
        "status": "ready",
        "out_dir": str(out_dir),
        "alpha_worker_dir": str(alpha_dir),
        "live_status": live_status,
        "shadow_core": shadow,
        "ws_state": ws_state,
        "metar_policy_autotune": policy_block,
        "constraint_scan_summary": constraint_summary,
        "explorer_profiles": {
            "default": default_profile,
            "relaxed_interval": relaxed_profile,
            "delta_relaxed_minus_default": profile_delta,
            "all_profiles": all_profiles,
            "rankings": profile_rankings,
        },
        "latest_reports": {
            "bankroll_validation": bankroll_block,
            "alpha_gap_report": alpha_gap_block,
            "live_readiness": live_readiness_block,
            "go_live_gate": go_live_gate_block,
        },
        "conservative_headline": conservative_headline,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "status": "ready",
        "output_file": str(output_path),
        "captured_at": payload["captured_at"],
        "approved_default": default_profile["approved_count"],
        "approved_relaxed": relaxed_profile["approved_count"],
        "approved_delta": profile_delta["approved_delta_relaxed_minus_default"],
        "profile_count": profile_rankings["profile_count"],
        "resolved_unique_market_sides": int(bankroll_block.get("resolved_unique_market_sides") or 0),
        "go_live_gate_status": _normalize_text(go_live_gate_block.get("gate_status")),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build compact alpha-worker dashboard JSON.")
    parser.add_argument("--out-dir", required=True, help="Main output directory")
    parser.add_argument("--alpha-worker-dir", required=True, help="Alpha worker output directory")
    parser.add_argument("--policy-json", required=True, help="Auto METAR-age policy JSON path")
    parser.add_argument("--output", required=True, help="Dashboard JSON output path")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    summary = run(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
