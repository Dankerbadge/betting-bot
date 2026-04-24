#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
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
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


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


def _read_intents_csv(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "rows": 0,
            "approved_rows": 0,
            "unique_market_tickers": 0,
            "unique_underlyings": 0,
            "unique_market_sides_all": 0,
            "unique_market_sides_approved": 0,
            "market_sides_all": set(),
            "market_sides_approved": set(),
            "market_tickers": set(),
            "underlyings": set(),
        }
    rows = 0
    approved_rows = 0
    market_sides_all: set[str] = set()
    market_sides_approved: set[str] = set()
    market_tickers: set[str] = set()
    underlyings: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows += 1
                ticker = _normalize_text(row.get("market_ticker"))
                side = _normalize_text(row.get("side")).lower()
                policy_reason = _normalize_text(row.get("policy_reason")).lower()
                underlying = _normalize_text(row.get("underlying_key")) or _normalize_text(
                    row.get("temperature_underlying_key")
                )
                if ticker:
                    market_tickers.add(ticker)
                if underlying:
                    underlyings.add(underlying)
                if ticker and side:
                    key = f"{ticker}|{side}"
                    market_sides_all.add(key)
                    if policy_reason == "approved":
                        approved_rows += 1
                        market_sides_approved.add(key)
    except OSError:
        pass
    return {
        "rows": int(rows),
        "approved_rows": int(approved_rows),
        "unique_market_tickers": int(len(market_tickers)),
        "unique_underlyings": int(len(underlyings)),
        "unique_market_sides_all": int(len(market_sides_all)),
        "unique_market_sides_approved": int(len(market_sides_approved)),
        "market_sides_all": market_sides_all,
        "market_sides_approved": market_sides_approved,
        "market_tickers": market_tickers,
        "underlyings": underlyings,
    }


def _read_approved_intents_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    approved_rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                policy_reason = _normalize_text(row.get("policy_reason")).lower()
                if policy_reason != "approved":
                    continue
                revalidation_status = _normalize_text(row.get("revalidation_status")).lower()
                if revalidation_status and revalidation_status != "approved":
                    continue
                market_ticker = _normalize_text(row.get("market_ticker"))
                side = _normalize_text(row.get("side")).lower()
                if not market_ticker or side not in {"yes", "no"}:
                    continue
                approved_rows.append(
                    {
                        "market_ticker": market_ticker,
                        "side": side,
                        "underlying_key": _normalize_text(row.get("underlying_key"))
                        or _normalize_text(row.get("temperature_underlying_key")),
                        "settlement_station": _normalize_text(row.get("settlement_station")).upper(),
                        "target_date_local": _normalize_text(row.get("target_date_local")),
                        "constraint_status": _normalize_text(row.get("constraint_status")).lower(),
                        "settlement_confidence_score": _parse_float(row.get("settlement_confidence_score")),
                        "cross_market_family_score": _parse_float(row.get("cross_market_family_score")),
                        "cross_market_family_zscore": _parse_float(row.get("cross_market_family_zscore")),
                        "speci_shock_confidence": _parse_float(row.get("speci_shock_confidence")),
                        "speci_shock_weight": _parse_float(row.get("speci_shock_weight")),
                        "yes_possible_gap": _parse_float(row.get("yes_possible_gap")),
                        "primary_signal_margin": _parse_float(row.get("primary_signal_margin")),
                        "forecast_feasibility_margin": _parse_float(row.get("forecast_feasibility_margin")),
                    }
                )
    except OSError:
        return []
    return approved_rows


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _profile_weight(profile_name: str) -> float:
    name = _normalize_text(profile_name).lower()
    if "strict" in name:
        return 1.0
    if "wide" in name:
        return 0.65
    if "relaxed_age" in name:
        return 0.75
    if "relaxed" in name:
        return 0.85
    return 0.8


def _build_consensus_payload(
    *,
    profiles: list[dict[str, Any]],
    captured_at: str,
    min_profile_support: int,
    min_support_ratio: float,
    top_n: int,
) -> dict[str, Any]:
    profile_names = [row.get("profile_name", "") for row in profiles if _normalize_text(row.get("profile_name"))]
    profile_weights = {name: _profile_weight(name) for name in profile_names}
    total_weight = sum(profile_weights.values()) or 1.0
    total_profiles = max(1, len(profile_names))

    aggregated: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        profile_name = _normalize_text(profile.get("profile_name"))
        if not profile_name:
            continue
        approved_rows = profile.get("_approved_rows")
        if not isinstance(approved_rows, list) or not approved_rows:
            continue
        dedup_by_market_side: dict[str, dict[str, Any]] = {}
        for row in approved_rows:
            if not isinstance(row, dict):
                continue
            market_ticker = _normalize_text(row.get("market_ticker"))
            side = _normalize_text(row.get("side")).lower()
            if not market_ticker or side not in {"yes", "no"}:
                continue
            market_side_key = f"{market_ticker}|{side}"
            score_hint = _parse_float(row.get("settlement_confidence_score")) or 0.0
            existing = dedup_by_market_side.get(market_side_key)
            existing_hint = _parse_float(existing.get("settlement_confidence_score")) if isinstance(existing, dict) else None
            if existing is None or score_hint > float(existing_hint or 0.0):
                dedup_by_market_side[market_side_key] = row

        for market_side_key, row in dedup_by_market_side.items():
            bucket = aggregated.setdefault(
                market_side_key,
                {
                    "market_side_key": market_side_key,
                    "market_ticker": _normalize_text(row.get("market_ticker")),
                    "side": _normalize_text(row.get("side")).lower(),
                    "underlying_key": _normalize_text(row.get("underlying_key")),
                    "settlement_station": _normalize_text(row.get("settlement_station")).upper(),
                    "target_date_local": _normalize_text(row.get("target_date_local")),
                    "profile_names": [],
                    "_support_profiles": set(),
                    "_settlement_confidence_values": [],
                    "_cross_market_family_score_values": [],
                    "_cross_market_family_zscore_values": [],
                    "_speci_shock_confidence_values": [],
                    "_speci_shock_weight_values": [],
                    "_yes_possible_gap_values": [],
                    "_primary_signal_margin_values": [],
                    "_forecast_feasibility_margin_values": [],
                },
            )
            support_profiles = bucket["_support_profiles"]
            if profile_name not in support_profiles:
                support_profiles.add(profile_name)
                bucket["profile_names"].append(profile_name)
            for source_key, target_key in (
                ("settlement_confidence_score", "_settlement_confidence_values"),
                ("cross_market_family_score", "_cross_market_family_score_values"),
                ("cross_market_family_zscore", "_cross_market_family_zscore_values"),
                ("speci_shock_confidence", "_speci_shock_confidence_values"),
                ("speci_shock_weight", "_speci_shock_weight_values"),
                ("yes_possible_gap", "_yes_possible_gap_values"),
                ("primary_signal_margin", "_primary_signal_margin_values"),
                ("forecast_feasibility_margin", "_forecast_feasibility_margin_values"),
            ):
                parsed = _parse_float(row.get(source_key))
                if parsed is not None:
                    bucket[target_key].append(float(parsed))

    candidates: list[dict[str, Any]] = []
    support_bucket_counts: dict[str, int] = defaultdict(int)
    for market_side_key, bucket in aggregated.items():
        support_profiles: set[str] = bucket["_support_profiles"]
        support_count = len(support_profiles)
        support_ratio = float(support_count) / float(total_profiles)
        if support_count < max(1, int(min_profile_support)):
            continue
        if support_ratio < float(min_support_ratio):
            continue

        weighted_support = sum(profile_weights.get(name, 0.8) for name in support_profiles)
        weighted_support_ratio = weighted_support / total_weight
        avg_confidence = _average(bucket["_settlement_confidence_values"])
        avg_family_score = _average(bucket["_cross_market_family_score_values"])
        avg_family_zscore = _average(bucket["_cross_market_family_zscore_values"])
        avg_speci_confidence = _average(bucket["_speci_shock_confidence_values"])
        avg_speci_weight = _average(bucket["_speci_shock_weight_values"])
        avg_yes_gap = _average(bucket["_yes_possible_gap_values"])
        avg_primary_margin = _average(bucket["_primary_signal_margin_values"])
        avg_forecast_margin = _average(bucket["_forecast_feasibility_margin_values"])

        # Consensus score intentionally combines independent profile support with
        # row-level alpha hints. This gives CPU-heavy profile scans a direct,
        # reproducible contribution to downstream ranking.
        consensus_alpha_score = 0.0
        consensus_alpha_score += 0.52 * weighted_support_ratio
        consensus_alpha_score += 0.23 * support_ratio
        if avg_confidence is not None:
            consensus_alpha_score += (avg_confidence - 0.5) * 0.8
        if avg_family_zscore is not None:
            consensus_alpha_score += _clamp(avg_family_zscore, -2.5, 2.5) * 0.12
        if avg_family_score is not None:
            consensus_alpha_score += _clamp(avg_family_score, -2.5, 2.5) * 0.08
        if avg_speci_confidence is not None:
            consensus_alpha_score += _clamp(avg_speci_confidence, 0.0, 1.0) * 0.22
        if avg_speci_weight is not None:
            consensus_alpha_score += _clamp(avg_speci_weight, 0.0, 1.0) * 0.2
        if avg_yes_gap is not None:
            consensus_alpha_score -= _clamp(avg_yes_gap, 0.0, 6.0) * 0.09
        if avg_primary_margin is not None:
            consensus_alpha_score += _clamp(abs(avg_primary_margin), 0.0, 6.0) * 0.03
        if avg_forecast_margin is not None:
            consensus_alpha_score += _clamp(abs(avg_forecast_margin), 0.0, 8.0) * 0.025

        support_bucket_counts[str(support_count)] += 1
        candidates.append(
            {
                "market_side_key": market_side_key,
                "market_ticker": bucket.get("market_ticker"),
                "side": bucket.get("side"),
                "underlying_key": bucket.get("underlying_key"),
                "settlement_station": bucket.get("settlement_station"),
                "target_date_local": bucket.get("target_date_local"),
                "profile_support_count": int(support_count),
                "profile_support_ratio": round(float(support_ratio), 6),
                "weighted_support_score": round(float(weighted_support), 6),
                "weighted_support_ratio": round(float(weighted_support_ratio), 6),
                "profile_names": sorted([_normalize_text(name) for name in support_profiles if _normalize_text(name)]),
                "avg_settlement_confidence_score": round(float(avg_confidence), 6)
                if avg_confidence is not None
                else None,
                "avg_cross_market_family_score": round(float(avg_family_score), 6)
                if avg_family_score is not None
                else None,
                "avg_cross_market_family_zscore": round(float(avg_family_zscore), 6)
                if avg_family_zscore is not None
                else None,
                "avg_speci_shock_confidence": round(float(avg_speci_confidence), 6)
                if avg_speci_confidence is not None
                else None,
                "avg_speci_shock_weight": round(float(avg_speci_weight), 6)
                if avg_speci_weight is not None
                else None,
                "avg_yes_possible_gap": round(float(avg_yes_gap), 6)
                if avg_yes_gap is not None
                else None,
                "avg_primary_signal_margin": round(float(avg_primary_margin), 6)
                if avg_primary_margin is not None
                else None,
                "avg_forecast_feasibility_margin": round(float(avg_forecast_margin), 6)
                if avg_forecast_margin is not None
                else None,
                "consensus_alpha_score": round(float(consensus_alpha_score), 6),
            }
        )

    candidates.sort(
        key=lambda row: (
            -float(row.get("consensus_alpha_score") or 0.0),
            -int(row.get("profile_support_count") or 0),
            -float(row.get("weighted_support_ratio") or 0.0),
            -float(row.get("avg_settlement_confidence_score") or 0.0),
            _normalize_text(row.get("market_side_key")),
        )
    )
    if top_n > 0:
        candidates = candidates[: int(top_n)]
    for index, row in enumerate(candidates):
        row["consensus_rank"] = int(index + 1)

    return {
        "captured_at": captured_at,
        "status": "ready",
        "profile_count": int(total_profiles),
        "profiles_considered": sorted(profile_names),
        "profile_weights": dict(sorted(profile_weights.items())),
        "thresholds": {
            "min_profile_support_count": int(max(1, int(min_profile_support))),
            "min_support_ratio": round(float(max(0.0, min(1.0, min_support_ratio))), 6),
            "top_n": int(top_n),
        },
        "opportunity_counts": {
            "total_market_sides_seen": int(len(aggregated)),
            "eligible_market_sides": int(len(candidates)),
            "support_bucket_counts": dict(
                sorted(
                    ((int(key), int(value)) for key, value in support_bucket_counts.items()),
                    key=lambda item: item[0],
                )
            ),
        },
        "candidates": candidates,
    }


def _profile_block(profile_dir: Path, now_epoch: float) -> dict[str, Any]:
    intents_summary_path = _latest_path(str(profile_dir / "kalshi_temperature_trade_intents_summary_*.json"))
    plan_summary_path = _latest_path(str(profile_dir / "kalshi_temperature_trade_plan_summary_*.json"))
    intents_summary = _safe_load_json(intents_summary_path)
    plan_summary = _safe_load_json(plan_summary_path)
    reasons = (
        intents_summary.get("policy_reason_counts")
        if isinstance(intents_summary.get("policy_reason_counts"), dict)
        else {}
    )
    intents_csv = Path(_normalize_text(intents_summary.get("output_csv")))
    csv_stats = _read_intents_csv(intents_csv)
    approved_rows = _read_approved_intents_rows(intents_csv)
    return {
        "profile_name": profile_dir.name,
        "intents_file": intents_summary_path.name if intents_summary_path else "",
        "intents_age_seconds": _age_seconds(intents_summary_path, now_epoch),
        "plans_file": plan_summary_path.name if plan_summary_path else "",
        "plans_age_seconds": _age_seconds(plan_summary_path, now_epoch),
        "actionable_constraint_rows": _parse_int(intents_summary.get("actionable_constraint_rows")) or 0,
        "expanded_actionable_intents": _parse_int(intents_summary.get("expanded_actionable_intents")) or 0,
        "approved_count": _parse_int(reasons.get("approved")) or 0,
        "stale_count": _parse_int(reasons.get("metar_observation_stale")) or 0,
        "underlying_cap_blocked_count": _parse_int(reasons.get("underlying_exposure_cap_reached")) or 0,
        "interval_overlap_blocked_count": _parse_int(reasons.get("no_side_interval_overlap_still_possible")) or 0,
        "planned_orders": _parse_int(plan_summary.get("planned_orders")) or 0,
        "csv_stats": {
            k: v
            for k, v in csv_stats.items()
            if k
            in {
                "rows",
                "approved_rows",
                "unique_market_tickers",
                "unique_underlyings",
                "unique_market_sides_all",
                "unique_market_sides_approved",
            }
        },
        "approved_market_side_rows_count": int(len(approved_rows)),
        "_set_market_sides_all": csv_stats["market_sides_all"],
        "_set_market_sides_approved": csv_stats["market_sides_approved"],
        "_set_market_tickers": csv_stats["market_tickers"],
        "_set_underlyings": csv_stats["underlyings"],
        "_approved_rows": approved_rows,
    }


def _build_adaptive_guidance(
    *,
    adaptive_parallelism: dict[str, Any],
    adaptive_max_markets: dict[str, Any],
) -> dict[str, Any]:
    p_enabled = bool(adaptive_parallelism.get("adaptive_enabled"))
    p_current = int(adaptive_parallelism.get("current_parallelism") or 0)
    p_min = int(adaptive_parallelism.get("min_parallelism") or 0)
    p_max = int(adaptive_parallelism.get("max_parallelism") or 0)
    p_load = int(adaptive_parallelism.get("load_per_vcpu_milli") or 0)
    p_low = int(adaptive_parallelism.get("load_low_milli") or 0)
    p_high = int(adaptive_parallelism.get("load_high_milli") or 0)

    m_enabled = bool(adaptive_max_markets.get("adaptive_enabled"))
    m_current = int(adaptive_max_markets.get("current_max_markets") or 0)
    m_min = int(adaptive_max_markets.get("min_max_markets") or 0)
    m_max = int(adaptive_max_markets.get("max_max_markets") or 0)
    m_load = int(adaptive_max_markets.get("load_per_vcpu_milli") or 0)
    target_pressure = adaptive_max_markets.get("target_pressure")
    if not isinstance(target_pressure, dict):
        target_pressure = {}
    target_pressure_level = int(target_pressure.get("level") or 0)
    target_pressure_reason = _normalize_text(target_pressure.get("reason"))

    parallelism_action = "hold"
    parallelism_reason = "adaptive parallelism is stable"
    if p_enabled and p_current < p_max and p_load > 0 and p_load <= max(1, p_low):
        parallelism_action = "increase_parallelism"
        parallelism_reason = "load is below low watermark with available parallelism headroom"
    elif p_enabled and p_current > p_min and p_high > 0 and p_load >= p_high:
        parallelism_action = "decrease_parallelism"
        parallelism_reason = "load is above high watermark and adaptive downshift should protect latency"
    elif not p_enabled:
        parallelism_action = "manual_mode"
        parallelism_reason = "adaptive parallelism is disabled"

    max_markets_action = "hold"
    max_markets_reason = "adaptive max-markets is stable"
    if (
        m_enabled
        and target_pressure_level > 0
        and m_current < m_max
        and p_high > 0
        and m_load <= p_high
    ):
        max_markets_action = "increase_max_markets_target_pressure"
        max_markets_reason = (
            "breadth targets are below threshold and load headroom allows more scan scope"
        )
    elif m_enabled and m_current < m_max and m_load > 0 and m_load <= max(1, p_low):
        max_markets_action = "increase_max_markets"
        max_markets_reason = "load is light and max-markets headroom is available"
    elif m_enabled and m_current > m_min and p_high > 0 and m_load >= p_high:
        max_markets_action = "decrease_max_markets"
        max_markets_reason = "load is elevated and reducing scan scope protects cycle time"
    elif not m_enabled:
        max_markets_action = "manual_mode"
        max_markets_reason = "adaptive max-markets is disabled"

    load_headroom_ratio = max(0.0, min(1.0, 1.0 - (float(max(p_load, m_load)) / 1000.0)))
    return {
        "parallelism": {
            "action": parallelism_action,
            "reason": parallelism_reason,
            "current": p_current,
            "min": p_min,
            "max": p_max,
            "load_per_vcpu_milli": p_load,
        },
        "max_markets": {
            "action": max_markets_action,
            "reason": max_markets_reason,
            "current": m_current,
            "min": m_min,
            "max": m_max,
            "load_per_vcpu_milli": m_load,
            "target_pressure_level": target_pressure_level,
            "target_pressure_reason": target_pressure_reason,
        },
        "cpu_headroom_ratio_estimate": round(float(load_headroom_ratio), 6),
    }


def _build_breadth_headline(
    *,
    profile_count: int,
    union_metrics: dict[str, Any],
    consensus_payload: dict[str, Any],
    top_profiles: dict[str, Any],
    adaptive_guidance: dict[str, Any],
) -> dict[str, Any]:
    top_consensus = (consensus_payload.get("candidates") or [{}])[0] if (consensus_payload.get("candidates") or []) else {}
    top_profile = (top_profiles.get("by_approved_count") or [{}])[0] if (top_profiles.get("by_approved_count") or []) else {}
    return {
        "prediction_quality_basis": "unique_market_side",
        "deployment_quality_basis": "underlying_family_aggregated",
        "profile_count": int(profile_count),
        "breadth_snapshot": {
            "unique_market_sides_approved_rows": int(union_metrics.get("unique_market_sides_approved_rows") or 0),
            "unique_market_tickers_all_rows": int(union_metrics.get("unique_market_tickers_all_rows") or 0),
            "unique_underlyings_all_rows": int(union_metrics.get("unique_underlyings_all_rows") or 0),
            "consensus_candidate_count": int(len(consensus_payload.get("candidates") or [])),
        },
        "leaders": {
            "top_profile_by_approved": {
                "profile_name": _normalize_text(top_profile.get("profile_name")),
                "approved_count": int(top_profile.get("approved_count") or 0),
                "planned_orders": int(top_profile.get("planned_orders") or 0),
            },
            "top_consensus_market_side": {
                "market_side_key": _normalize_text(top_consensus.get("market_side_key")),
                "consensus_alpha_score": _parse_float(top_consensus.get("consensus_alpha_score")),
                "profile_support_count": int(top_consensus.get("profile_support_count") or 0),
            },
        },
        "throughput_guidance": adaptive_guidance,
        "notes": [
            "Row-based approved counts are throughput diagnostics, not independent-alpha headlines.",
            "Use consensus + unique-market-side breadth to judge expansion quality.",
        ],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    breadth_dir = Path(args.breadth_worker_dir)
    profiles_dir = breadth_dir / "profiles"
    now_epoch = time.time()
    profile_dirs = sorted([p for p in profiles_dir.iterdir() if p.is_dir()]) if profiles_dir.exists() else []

    profiles = [_profile_block(profile_dir, now_epoch) for profile_dir in profile_dirs]
    union_market_sides_all: set[str] = set()
    union_market_sides_approved: set[str] = set()
    union_market_tickers: set[str] = set()
    union_underlyings: set[str] = set()
    for row in profiles:
        union_market_sides_all |= row["_set_market_sides_all"]
        union_market_sides_approved |= row["_set_market_sides_approved"]
        union_market_tickers |= row["_set_market_tickers"]
        union_underlyings |= row["_set_underlyings"]
        row.pop("_set_market_sides_all", None)
        row.pop("_set_market_sides_approved", None)
        row.pop("_set_market_tickers", None)
        row.pop("_set_underlyings", None)

    consensus_payload = _build_consensus_payload(
        profiles=profiles,
        captured_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch)),
        min_profile_support=max(1, int(args.consensus_min_profile_support)),
        min_support_ratio=max(0.0, min(1.0, float(args.consensus_min_support_ratio))),
        top_n=max(0, int(args.consensus_top_n)),
    )
    consensus_output_path = Path(args.consensus_output) if _normalize_text(args.consensus_output) else None
    if consensus_output_path is not None:
        consensus_output_path.parent.mkdir(parents=True, exist_ok=True)
        consensus_output_path.write_text(json.dumps(consensus_payload, indent=2, sort_keys=True), encoding="utf-8")

    for row in profiles:
        row.pop("_approved_rows", None)

    ranked_by_approved = sorted(profiles, key=lambda x: x.get("approved_count", 0), reverse=True)
    ranked_by_market_sides = sorted(
        profiles, key=lambda x: (x.get("csv_stats", {}).get("unique_market_sides_approved", 0)), reverse=True
    )
    adaptive_state_path = breadth_dir / ".adaptive_parallelism.json"
    adaptive_state_payload = _safe_load_json(adaptive_state_path)
    adaptive_state = {
        "file": adaptive_state_path.name if adaptive_state_path.exists() else "",
        "age_seconds": _age_seconds(adaptive_state_path if adaptive_state_path.exists() else None, now_epoch),
        "adaptive_enabled": bool(adaptive_state_payload.get("adaptive_enabled")),
        "configured_parallelism": _parse_int(adaptive_state_payload.get("configured_parallelism")) or 0,
        "current_parallelism": _parse_int(adaptive_state_payload.get("current_parallelism")) or 0,
        "load_per_vcpu_milli": _parse_int(adaptive_state_payload.get("load_per_vcpu_milli")) or 0,
        "min_parallelism": _parse_int(adaptive_state_payload.get("min_parallelism")) or 0,
        "max_parallelism": _parse_int(adaptive_state_payload.get("max_parallelism")) or 0,
        "load_low_milli": _parse_int(adaptive_state_payload.get("load_low_milli")) or 0,
        "load_high_milli": _parse_int(adaptive_state_payload.get("load_high_milli")) or 0,
    }
    adaptive_markets_state_path = breadth_dir / ".adaptive_max_markets.json"
    adaptive_markets_payload = _safe_load_json(adaptive_markets_state_path)
    adaptive_max_markets = {
        "file": adaptive_markets_state_path.name if adaptive_markets_state_path.exists() else "",
        "age_seconds": _age_seconds(adaptive_markets_state_path if adaptive_markets_state_path.exists() else None, now_epoch),
        "adaptive_enabled": bool(adaptive_markets_payload.get("adaptive_enabled")),
        "configured_max_markets": _parse_int(adaptive_markets_payload.get("configured_max_markets")) or 0,
        "current_max_markets": _parse_int(adaptive_markets_payload.get("current_max_markets")) or 0,
        "load_per_vcpu_milli": _parse_int(adaptive_markets_payload.get("load_per_vcpu_milli")) or 0,
        "min_max_markets": _parse_int(adaptive_markets_payload.get("min_max_markets")) or 0,
        "max_max_markets": _parse_int(adaptive_markets_payload.get("max_max_markets")) or 0,
        "last_constraint_scan_duration_seconds": _parse_int(
            adaptive_markets_payload.get("last_constraint_scan_duration_seconds")
        )
        or 0,
        "target_constraint_scan_seconds": _parse_int(adaptive_markets_payload.get("target_constraint_scan_seconds")) or 0,
        "target_pressure": adaptive_markets_payload.get("target_pressure")
        if isinstance(adaptive_markets_payload.get("target_pressure"), dict)
        else {},
    }
    adaptive_guidance = _build_adaptive_guidance(
        adaptive_parallelism=adaptive_state,
        adaptive_max_markets=adaptive_max_markets,
    )
    top_profiles_block = {
        "by_approved_count": [
            {
                "profile_name": row.get("profile_name"),
                "approved_count": row.get("approved_count", 0),
                "planned_orders": row.get("planned_orders", 0),
            }
            for row in ranked_by_approved[:5]
        ],
        "by_unique_market_sides_approved": [
            {
                "profile_name": row.get("profile_name"),
                "unique_market_sides_approved": row.get("csv_stats", {}).get("unique_market_sides_approved", 0),
                "unique_market_tickers": row.get("csv_stats", {}).get("unique_market_tickers", 0),
            }
            for row in ranked_by_market_sides[:5]
        ],
    }
    union_metrics = {
        "unique_market_sides_all_rows": int(len(union_market_sides_all)),
        "unique_market_sides_approved_rows": int(len(union_market_sides_approved)),
        "unique_market_tickers_all_rows": int(len(union_market_tickers)),
        "unique_underlyings_all_rows": int(len(union_underlyings)),
    }
    breadth_headline = _build_breadth_headline(
        profile_count=len(profiles),
        union_metrics=union_metrics,
        consensus_payload=consensus_payload,
        top_profiles=top_profiles_block,
        adaptive_guidance=adaptive_guidance,
    )

    payload = {
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch)),
        "status": "ready",
        "out_dir": str(out_dir),
        "breadth_worker_dir": str(breadth_dir),
        "profile_count": int(len(profiles)),
        "profiles": profiles,
        "union_metrics": union_metrics,
        "consensus": {
            "consensus_output_file": str(consensus_output_path) if consensus_output_path is not None else "",
            "consensus_candidate_count": int(len(consensus_payload.get("candidates") or [])),
            "consensus_top_market_side": (
                (consensus_payload.get("candidates") or [{}])[0].get("market_side_key")
                if (consensus_payload.get("candidates") or [])
                else ""
            ),
            "consensus_thresholds": consensus_payload.get("thresholds"),
        },
        "top_profiles": top_profiles_block,
        "adaptive_parallelism": adaptive_state,
        "adaptive_max_markets": adaptive_max_markets,
        "adaptive_guidance": adaptive_guidance,
        "breadth_headline": breadth_headline,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "status": "ready",
        "output_file": str(output_path),
        "profile_count": payload["profile_count"],
        "union_unique_market_sides_approved": payload["union_metrics"]["unique_market_sides_approved_rows"],
        "consensus_output_file": str(consensus_output_path) if consensus_output_path is not None else "",
        "consensus_candidate_count": int(len(consensus_payload.get("candidates") or [])),
        "parallelism_action": _normalize_text((adaptive_guidance.get("parallelism") or {}).get("action")),
        "max_markets_action": _normalize_text((adaptive_guidance.get("max_markets") or {}).get("action")),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build breadth-worker dashboard JSON.")
    parser.add_argument("--out-dir", required=True, help="Main output directory")
    parser.add_argument("--breadth-worker-dir", required=True, help="Breadth worker output directory")
    parser.add_argument("--output", required=True, help="Dashboard JSON output path")
    parser.add_argument(
        "--consensus-output",
        default="",
        help="Optional output path for fused cross-profile consensus opportunities JSON",
    )
    parser.add_argument(
        "--consensus-top-n",
        type=int,
        default=250,
        help="Maximum consensus market-side rows retained in consensus-output",
    )
    parser.add_argument(
        "--consensus-min-profile-support",
        type=int,
        default=1,
        help="Minimum profile support count required for a market-side to enter consensus-output",
    )
    parser.add_argument(
        "--consensus-min-support-ratio",
        type=float,
        default=0.0,
        help="Minimum support ratio (support_count/profile_count) required for consensus-output rows",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    summary = run(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
