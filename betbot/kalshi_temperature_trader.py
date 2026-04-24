from __future__ import annotations

from collections import Counter
import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from betbot.kalshi_book import default_book_db_path
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_temperature_constraints import run_kalshi_temperature_constraint_scan
from betbot.kalshi_temperature_selection_quality import (
    load_temperature_selection_quality_profile,
    selection_quality_adjustment_for_intent,
)
from betbot.kalshi_ws_state import default_ws_state_path


ConstraintScanRunner = Callable[..., dict[str, Any]]
MicroExecuteRunner = Callable[..., dict[str, Any]]
SleepFn = Callable[[float], None]

_ACTIONABLE_CONSTRAINTS = {
    "yes_impossible",
    "yes_likely_locked",
    "no_interval_infeasible",
    "yes_interval_certain",
    "no_monotonic_chain",
    "yes_monotonic_chain",
}
_CONSTRAINT_PRIORITY = {
    "yes_impossible": 0,
    "no_interval_infeasible": 1,
    "yes_likely_locked": 2,
    "yes_interval_certain": 3,
    "no_monotonic_chain": 4,
    "yes_monotonic_chain": 5,
}
_LOWER_BOUNDED_KINDS = {"above", "at_least"}
_UPPER_BOUNDED_KINDS = {"below", "at_most"}
_POLICY_BLOCK_REASON_PRIORITY = {
    # Hard safety / source integrity
    "settlement_review_hold": 10,
    "settlement_finalization_blocked": 11,
    "missing_spec_hash": 20,
    "missing_market_snapshot_seq": 21,
    "missing_metar_snapshot_sha": 22,
    "metar_ingest_quality_insufficient": 29,
    # Freshness / temporal validity
    "metar_observation_age_unknown": 30,
    "metar_observation_stale": 31,
    "taf_station_missing": 32,
    "metar_freshness_boundary_quality_insufficient": 32,
    "inside_cutoff_window": 33,
    "outside_active_horizon": 34,
    # Portfolio risk controls
    "weather_pattern_global_risk_off": 39,
    "underlying_exposure_cap_reached": 40,
    # Structural feasibility / consistency
    "constraint_not_actionable": 50,
    "range_family_consistency_conflict": 51,
    "yes_side_interval_infeasible": 52,
    "no_side_interval_overlap_still_possible": 53,
    "yes_side_gap_above_max": 54,
    "no_side_gap_nonpositive": 55,
    # Weak-evidence / global-only pressure hard block
    "historical_quality_global_only_pressure": 59,
    "historical_quality_station_hour_hard_block": 58,
    "historical_quality_signal_type_hard_block": 58,
    "historical_expectancy_hard_block": 58,
    "weather_pattern_negative_regime_bucket_suppressed": 56,
    "weather_pattern_multi_bucket_hard_block": 57,
    # Score/model thresholds
    "settlement_confidence_below_min": 60,
    "alpha_strength_below_min": 61,
    "probability_confidence_below_min": 62,
    "high_price_expected_edge_nonpositive": 63,
    "high_price_edge_to_risk_ratio_below_min": 64,
    "expected_edge_below_min": 63,
    "edge_to_risk_ratio_below_min": 64,
    "base_edge_below_min": 65,
    "probability_breakeven_gap_below_min": 66,
    "historical_profitability_probability_below_min": 67,
    "historical_profitability_expected_edge_below_min": 68,
}
_WEATHER_PATTERN_HARD_BLOCK_MIN_REALIZED_TRADES = 4
_WEATHER_PATTERN_HARD_BLOCK_MAX_EDGE_REALIZATION_RATIO = 0.85
_WEATHER_PATTERN_HARD_BLOCK_REALIZED_PER_TRADE_THRESHOLD = -0.01
_WEATHER_PATTERN_HARD_BLOCK_EXPECTED_ONLY_MIN_SAMPLES = 180
_WEATHER_PATTERN_HARD_BLOCK_EXPECTED_ONLY_MIN_CONFIDENCE = 0.94
_WEATHER_PATTERN_RISK_OFF_FALLBACK_CONCENTRATION_FLOOR = 0.85
_WEATHER_PATTERN_RISK_OFF_FALLBACK_STALE_SHARE_FLOOR = 0.55
_WEATHER_PATTERN_RISK_OFF_FALLBACK_MIN_ATTEMPTS_FLOOR = 120
_WEATHER_PATTERN_RISK_OFF_FALLBACK_MIN_NEGATIVE_DIMENSIONS = 2


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_boolish(value: Any) -> bool | None:
    text = _normalize_text(value).lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _clamp_fraction(value: Any, default: float) -> float:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return float(default)
    return max(0.0, min(1.0, float(parsed)))


def _unique_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = _normalize_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _parse_shadow_quote_probe_target_keys(
    targets: list[str] | None,
) -> tuple[list[str], list[tuple[str, str | None]]]:
    if targets is None:
        return [], []

    parsed_keys: list[str] = []
    parsed_targets: list[tuple[str, str | None]] = []
    seen_keys: set[str] = set()
    for raw_target in targets:
        normalized_target = _normalize_text(raw_target)
        if not normalized_target:
            continue
        market_ticker, separator, side_text = normalized_target.partition("|")
        normalized_market_ticker = _normalize_text(market_ticker).upper()
        if not normalized_market_ticker:
            continue
        normalized_side = _normalize_text(side_text).lower() if separator else ""
        if normalized_side in {"yes", "no"}:
            normalized_key = f"{normalized_market_ticker}|{normalized_side}"
            target_side: str | None = normalized_side
        else:
            normalized_key = normalized_market_ticker
            target_side = None
        if normalized_key in seen_keys:
            continue
        seen_keys.add(normalized_key)
        parsed_keys.append(normalized_key)
        parsed_targets.append((normalized_market_ticker, target_side))

    return parsed_keys[:20], parsed_targets[:20]


def _parse_market_ticker_exclusions(
    exclusions: list[str] | None,
) -> tuple[list[str], set[str], list[str], set[str]]:
    if exclusions is None:
        return [], set(), [], set()

    requested_tickers: list[str] = []
    excluded_tickers: set[str] = set()
    requested_market_sides: list[str] = []
    excluded_market_sides: set[str] = set()
    for raw_exclusion in exclusions:
        normalized_exclusion = _normalize_text(raw_exclusion)
        if not normalized_exclusion:
            continue
        market_ticker_text, separator, side_text = normalized_exclusion.partition("|")
        normalized_ticker = _normalize_text(market_ticker_text).upper()
        if not normalized_ticker:
            continue
        normalized_side = _normalize_text(side_text).lower() if separator else ""
        if separator and normalized_side in {"yes", "no"}:
            normalized_market_side = f"{normalized_ticker}|{normalized_side}"
            if normalized_market_side in excluded_market_sides:
                continue
            excluded_market_sides.add(normalized_market_side)
            requested_market_sides.append(normalized_market_side)
            continue
        if normalized_ticker in excluded_tickers:
            continue
        excluded_tickers.add(normalized_ticker)
        requested_tickers.append(normalized_ticker)

    return requested_tickers[:50], excluded_tickers, requested_market_sides[:50], excluded_market_sides


def _primary_policy_block_reason(reasons: list[str]) -> str:
    if not reasons:
        return "unknown_block_reason"
    indexed = list(enumerate(reasons))
    indexed.sort(
        key=lambda item: (
            int(_POLICY_BLOCK_REASON_PRIORITY.get(item[1], 999)),
            int(item[0]),
        )
    )
    return indexed[0][1]


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _clamp_unit(value: Any, default: float = 0.0) -> float:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return max(0.0, min(1.0, float(default)))
    return max(0.0, min(1.0, float(parsed)))


def _historical_quality_bucket_snapshot(
    *,
    intent: "TemperatureTradeIntent",
    profile: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(profile, dict):
        return {}
    bucket_profiles = profile.get("bucket_profiles")
    if not isinstance(bucket_profiles, dict):
        return {}
    hour_key = "unknown"
    observed_ts = _parse_ts(getattr(intent, "metar_observation_time_utc", None)) or _parse_ts(
        getattr(intent, "captured_at", None)
    )
    if isinstance(observed_ts, datetime):
        timezone_name = _normalize_text(getattr(intent, "settlement_timezone", ""))
        if timezone_name:
            try:
                hour_key = str(int(observed_ts.astimezone(ZoneInfo(timezone_name)).hour))
            except Exception:
                hour_key = str(int(observed_ts.hour))
        else:
            hour_key = str(int(observed_ts.hour))
    keys_by_dimension = {
        "station": _normalize_text(getattr(intent, "settlement_station", "")).upper(),
        "local_hour": hour_key,
        "signal_type": _normalize_text(getattr(intent, "constraint_status", "")).lower(),
        "side": _normalize_text(getattr(intent, "side", "")).lower(),
    }
    snapshot: dict[str, dict[str, Any]] = {}
    for dimension, key_text in keys_by_dimension.items():
        if not key_text:
            continue
        dimension_bucket = bucket_profiles.get(dimension)
        if not isinstance(dimension_bucket, dict):
            continue
        entry = dimension_bucket.get(key_text)
        if not isinstance(entry, dict):
            continue
        win_rate = _parse_float(entry.get("win_rate"))
        expectancy_per_trade = _parse_float(entry.get("expectancy_per_trade"))
        snapshot[dimension] = {
            "key": key_text,
            "samples": max(0, int(_parse_float(entry.get("samples")) or 0)),
            "penalty_ratio": _clamp_unit(entry.get("penalty_ratio"), 0.0),
            "boost_ratio": _clamp_unit(entry.get("boost_ratio"), 0.0),
            "win_rate": _clamp_unit(win_rate, 0.0) if isinstance(win_rate, float) else None,
            "expectancy_per_trade": (
                float(expectancy_per_trade)
                if isinstance(expectancy_per_trade, float)
                else None
            ),
        }
    return snapshot


def _historical_profitability_guardrail(profile: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(profile, dict) or not bool(profile.get("enabled")):
        return {
            "status": "disabled",
            "penalty_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "calibration_ratio": None,
            "evidence_confidence": None,
            "resolved_unique_market_sides": 0,
            "repeated_entry_multiplier": None,
            "concentration_warning": None,
            "signals": [],
        }
    profile_status = _normalize_text(profile.get("status")).lower() or "unknown"
    evidence_confidence = _parse_float(profile.get("evidence_confidence"))
    resolved_unique_market_sides = int(_parse_float(profile.get("resolved_unique_market_sides")) or 0)
    repeated_entry_multiplier = _parse_float(profile.get("repeated_entry_multiplier"))
    global_meta = profile.get("global") if isinstance(profile.get("global"), dict) else {}
    concentration_warning = (
        bool(global_meta.get("concentration_warning"))
        if isinstance(global_meta.get("concentration_warning"), bool)
        else None
    )
    calibration_ratio = _parse_float(global_meta.get("calibration_ratio"))
    profit_gap_meta = (
        profile.get("profitability_calibration_gap")
        if isinstance(profile.get("profitability_calibration_gap"), dict)
        else {}
    )
    if calibration_ratio is None:
        calibration_ratio = _parse_float(profit_gap_meta.get("calibration_ratio"))

    if profile_status in {"no_bankroll_validation_artifact", "bankroll_validation_parse_failed"}:
        return {
            "status": "no_profile_evidence",
            "penalty_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "calibration_ratio": calibration_ratio,
            "evidence_confidence": evidence_confidence,
            "resolved_unique_market_sides": int(resolved_unique_market_sides),
            "repeated_entry_multiplier": repeated_entry_multiplier,
            "concentration_warning": concentration_warning,
            "signals": ["profile_unavailable"],
        }

    min_evidence_confidence = 0.55
    min_calibration_ratio = 0.55
    min_resolved_market_sides = 10.0
    max_repeated_entry_multiplier = 16.0
    probability_raise_cap = 0.03
    expected_edge_raise_cap = 0.05

    status_pressure = 0.0
    if profile_status in {"stale_profile"}:
        status_pressure = 0.65
    elif profile_status in {"insufficient_resolved_market_sides"}:
        status_pressure = 0.55
    elif profile_status in {"no_bankroll_validation_artifact", "bankroll_validation_parse_failed", "unknown"}:
        status_pressure = 0.75
    elif profile_status != "ready":
        status_pressure = 0.40

    resolved_pressure = max(
        0.0,
        min(
            1.0,
            (min_resolved_market_sides - float(resolved_unique_market_sides)) / max(1.0, min_resolved_market_sides),
        ),
    )
    evidence_pressure = (
        max(
            0.0,
            min(
                1.0,
                (min_evidence_confidence - float(evidence_confidence)) / max(1e-9, min_evidence_confidence),
            ),
        )
        if isinstance(evidence_confidence, float)
        else 0.50
    )
    calibration_pressure = (
        max(
            0.0,
            min(
                1.0,
                (min_calibration_ratio - float(calibration_ratio)) / max(1e-9, min_calibration_ratio),
            ),
        )
        if isinstance(calibration_ratio, float)
        else 0.40
    )
    repeat_pressure = (
        max(
            0.0,
            min(
                1.0,
                (float(repeated_entry_multiplier) - max_repeated_entry_multiplier)
                / max(1e-9, max_repeated_entry_multiplier),
            ),
        )
        if isinstance(repeated_entry_multiplier, float)
        else 0.0
    )
    concentration_pressure = 1.0 if concentration_warning is True else 0.0

    penalty_ratio = max(
        0.0,
        min(
            1.0,
            0.30 * status_pressure
            + 0.20 * resolved_pressure
            + 0.20 * evidence_pressure
            + 0.15 * calibration_pressure
            + 0.10 * repeat_pressure
            + 0.05 * concentration_pressure,
        ),
    )
    probability_raise = probability_raise_cap * penalty_ratio
    expected_edge_raise = expected_edge_raise_cap * penalty_ratio

    signals: list[str] = []
    if status_pressure > 0.0:
        signals.append(f"status:{profile_status}")
    if resolved_pressure > 0.0:
        signals.append(f"resolved_pressure:{resolved_pressure:.3f}")
    if evidence_pressure > 0.0:
        signals.append(f"evidence_pressure:{evidence_pressure:.3f}")
    if calibration_pressure > 0.0:
        signals.append(f"calibration_pressure:{calibration_pressure:.3f}")
    if repeat_pressure > 0.0:
        signals.append(f"repeat_pressure:{repeat_pressure:.3f}")
    if concentration_pressure > 0.0:
        signals.append("concentration_warning")

    return {
        "status": "ready" if penalty_ratio > 0.0 else "neutral",
        "penalty_ratio": round(float(penalty_ratio), 6),
        "probability_raise": round(float(probability_raise), 6),
        "expected_edge_raise": round(float(expected_edge_raise), 6),
        "calibration_ratio": round(float(calibration_ratio), 6) if isinstance(calibration_ratio, float) else None,
        "evidence_confidence": round(float(evidence_confidence), 6) if isinstance(evidence_confidence, float) else None,
        "resolved_unique_market_sides": int(resolved_unique_market_sides),
        "repeated_entry_multiplier": (
            round(float(repeated_entry_multiplier), 6) if isinstance(repeated_entry_multiplier, float) else None
        ),
        "concentration_warning": concentration_warning,
        "signals": signals,
    }


def _historical_profitability_bucket_guardrail(
    *,
    intent: "TemperatureTradeIntent",
    profile: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(profile, dict) or not bool(profile.get("enabled")):
        return {
            "status": "disabled",
            "penalty_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "sources": [],
        }
    bucket_profiles = profile.get("bucket_profiles")
    if not isinstance(bucket_profiles, dict):
        return {
            "status": "no_bucket_profiles",
            "penalty_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "sources": [],
        }
    station = _normalize_text(getattr(intent, "settlement_station", "")).upper()
    local_hour = "unknown"
    observed_ts = _parse_ts(getattr(intent, "metar_observation_time_utc", None)) or _parse_ts(
        getattr(intent, "captured_at", None)
    )
    if isinstance(observed_ts, datetime):
        timezone_name = _normalize_text(getattr(intent, "settlement_timezone", ""))
        if timezone_name:
            try:
                local_hour = str(int(observed_ts.astimezone(ZoneInfo(timezone_name)).hour))
            except Exception:
                local_hour = str(int(observed_ts.hour))
        else:
            local_hour = str(int(observed_ts.hour))

    dimension_keys = {
        "station": station,
        "local_hour": local_hour,
    }
    dimension_weights = {
        "station": 0.6,
        "local_hour": 0.4,
    }
    weighted_penalty = 0.0
    total_weight = 0.0
    sources: list[str] = []
    for dim_name, key in dimension_keys.items():
        key_text = _normalize_text(key)
        if not key_text:
            continue
        dim_bucket = bucket_profiles.get(dim_name)
        if not isinstance(dim_bucket, dict):
            continue
        entry = dim_bucket.get(key_text)
        if not isinstance(entry, dict):
            continue
        source_labels = entry.get("source_labels") if isinstance(entry.get("source_labels"), list) else []
        has_profitability_source = any(
            _normalize_text(label).startswith("profitability_gap")
            for label in source_labels
        )
        if not has_profitability_source:
            continue
        penalty_ratio = max(
            0.0,
            min(
                1.0,
                float(_parse_float(entry.get("penalty_ratio")) or 0.0),
            ),
        )
        sample_count = max(0, int(_parse_float(entry.get("samples")) or 0))
        reliability = max(0.0, min(1.0, float(sample_count) / 12.0))
        weighted_penalty += float(dimension_weights.get(dim_name, 0.0)) * penalty_ratio * reliability
        total_weight += float(dimension_weights.get(dim_name, 0.0))
        sources.append(f"{dim_name}:{key_text}:samples={sample_count}:penalty={penalty_ratio:.6f}")

    if total_weight <= 0.0:
        return {
            "status": "no_profitability_bucket_match",
            "penalty_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "sources": [],
        }
    penalty_ratio = max(0.0, min(1.0, weighted_penalty / total_weight))
    probability_raise = min(0.015, 0.015 * penalty_ratio)
    expected_edge_raise = min(0.04, 0.04 * penalty_ratio)
    return {
        "status": "ready" if penalty_ratio > 0.0 else "neutral",
        "penalty_ratio": round(float(penalty_ratio), 6),
        "probability_raise": round(float(probability_raise), 6),
        "expected_edge_raise": round(float(expected_edge_raise), 6),
        "sources": sources,
    }


def _parse_ts(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _sanitize_metar_age_minutes(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    if parsed < 0.0:
        return None
    return float(parsed)


def _sanitize_probability_threshold(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    if parsed < 0.0:
        return None
    return max(0.0, min(0.999, float(parsed)))


def _sanitize_edge_threshold(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    if parsed < 0.0:
        return None
    return max(0.0, float(parsed))


def _sanitize_unit_threshold(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    return max(0.0, min(1.0, float(parsed)))


def _resolve_metar_ingest_quality_payload(
    *,
    summary_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = summary_payload if isinstance(summary_payload, dict) else {}
    quality_status = _normalize_text(payload.get("quality_status")).lower()
    if not quality_status:
        quality_status = _normalize_text(payload.get("status")).lower()
    if quality_status not in {"ready", "degraded", "blocked"}:
        quality_status = quality_status or "unknown"

    quality_score = _sanitize_unit_threshold(payload.get("quality_score"))
    if quality_score is None:
        if quality_status == "ready":
            quality_score = 1.0
        elif quality_status == "blocked":
            quality_score = 0.0
        elif quality_status == "degraded":
            quality_score = 0.5

    fresh_station_coverage_ratio = _sanitize_unit_threshold(
        payload.get("fresh_station_coverage_ratio")
    )
    if fresh_station_coverage_ratio is None and quality_status == "ready":
        fresh_station_coverage_ratio = 1.0

    quality_grade = _normalize_text(payload.get("quality_grade")).lower()
    if not quality_grade:
        if quality_status == "blocked":
            quality_grade = "critical"
        elif quality_status == "ready":
            if isinstance(quality_score, float) and quality_score >= 0.90:
                quality_grade = "excellent"
            elif isinstance(quality_score, float) and quality_score >= 0.75:
                quality_grade = "good"
            else:
                quality_grade = "ready"
        elif quality_status == "degraded":
            quality_grade = "degraded"
        else:
            quality_grade = "unknown"

    quality_signals_raw = payload.get("quality_signals")
    quality_signals = (
        [
            _normalize_text(item)
            for item in quality_signals_raw
            if _normalize_text(item)
        ]
        if isinstance(quality_signals_raw, list)
        else []
    )
    quality_signal_count = max(
        len(quality_signals),
        int(_parse_float(payload.get("quality_signal_count")) or 0),
    )

    return {
        "quality_score": quality_score,
        "quality_grade": quality_grade,
        "quality_status": quality_status,
        "quality_signal_count": quality_signal_count,
        "quality_signals": quality_signals,
        "fresh_station_coverage_ratio": fresh_station_coverage_ratio,
        "usable_latest_station_count": _parse_int(payload.get("usable_latest_station_count")),
        "stale_or_future_row_ratio": _sanitize_unit_threshold(payload.get("stale_or_future_row_ratio")),
        "parse_error_rate": _sanitize_unit_threshold(payload.get("parse_error_rate")),
    }


def _evaluate_metar_ingest_quality_gate(
    *,
    gate_enabled: Any,
    min_quality_score: Any,
    min_fresh_station_coverage_ratio: Any,
    require_ready_status: Any,
    quality_status: Any,
    quality_score: Any,
    fresh_station_coverage_ratio: Any,
) -> dict[str, Any]:
    enabled = bool(gate_enabled)
    min_quality_score_threshold = _sanitize_unit_threshold(min_quality_score)
    min_fresh_coverage_threshold = _sanitize_unit_threshold(min_fresh_station_coverage_ratio)
    require_ready = bool(require_ready_status)

    resolved_quality_status = _normalize_text(quality_status).lower() or "unknown"
    resolved_quality_score = _sanitize_unit_threshold(quality_score)
    resolved_fresh_coverage = _sanitize_unit_threshold(fresh_station_coverage_ratio)

    failure_reasons: list[str] = []
    if enabled:
        if require_ready and resolved_quality_status != "ready":
            failure_reasons.append("quality_status_not_ready")
        if min_quality_score_threshold is not None:
            if resolved_quality_score is None:
                failure_reasons.append("quality_score_missing")
            elif resolved_quality_score < min_quality_score_threshold:
                failure_reasons.append("quality_score_below_min")
        if min_fresh_coverage_threshold is not None:
            if resolved_fresh_coverage is None:
                failure_reasons.append("fresh_station_coverage_ratio_missing")
            elif resolved_fresh_coverage < min_fresh_coverage_threshold:
                failure_reasons.append("fresh_station_coverage_ratio_below_min")

    return {
        "enabled": enabled,
        "passed": bool(enabled and not failure_reasons),
        "failure_reasons": failure_reasons,
        "min_quality_score": min_quality_score_threshold,
        "min_fresh_station_coverage_ratio": min_fresh_coverage_threshold,
        "require_ready_status": require_ready,
        "quality_status": resolved_quality_status,
        "quality_score": resolved_quality_score,
        "fresh_station_coverage_ratio": resolved_fresh_coverage,
    }


def _json_safe_profile_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return _normalize_text(value)


def _resolve_adaptive_policy_profile(
    *,
    adaptive_policy_profile: Any,
    min_settlement_confidence: Any,
    min_probability_confidence: Any,
    min_expected_edge_net: Any,
    min_edge_to_risk_ratio: Any,
    max_intents_per_underlying: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_min_probability_confidence = _sanitize_probability_threshold(min_probability_confidence)
    resolved_min_expected_edge_net = _sanitize_edge_threshold(min_expected_edge_net)
    resolved_min_edge_to_risk_ratio = _sanitize_edge_threshold(min_edge_to_risk_ratio)
    resolved_max_intents_per_underlying = max(1, int(_parse_float(max_intents_per_underlying) or 6))

    profile_summary = {
        "adaptive_policy_profile_present": adaptive_policy_profile is not None,
        "adaptive_policy_profile_valid": isinstance(adaptive_policy_profile, dict),
        "adaptive_policy_profile_applied": False,
        "adaptive_policy_profile_requested_overrides": {},
        "adaptive_policy_profile_effective_overrides": {},
        "adaptive_policy_profile_clamped_overrides": {},
        "adaptive_policy_profile_ignored_overrides": [],
    }

    resolved = {
        "min_probability_confidence": resolved_min_probability_confidence,
        "min_expected_edge_net": resolved_min_expected_edge_net,
        "min_edge_to_risk_ratio": resolved_min_edge_to_risk_ratio,
        "max_intents_per_underlying": resolved_max_intents_per_underlying,
    }
    if not isinstance(adaptive_policy_profile, dict):
        return resolved, profile_summary

    base_probability_floor = max(
        0.6,
        _sanitize_probability_threshold(min_settlement_confidence) or 0.6,
    )

    profile_specs: dict[str, tuple[str, float | int | None, float | int | None, float | int | None]] = {
        "min_probability_confidence": (
            "min_probability_confidence",
            0.6,
            0.995,
            base_probability_floor,
        ),
        "min_expected_edge_net": (
            "min_expected_edge_net",
            0.005,
            0.25,
            0.005,
        ),
        "min_edge_to_risk_ratio": (
            "min_edge_to_risk_ratio",
            0.02,
            5.0,
            0.02,
        ),
        "max_intents_per_underlying": (
            "max_intents_per_underlying",
            1,
            6,
            max(1, resolved_max_intents_per_underlying),
        ),
    }

    applied_any = False
    for profile_key, (resolved_key, floor, ceiling, base_floor) in profile_specs.items():
        if profile_key not in adaptive_policy_profile:
            continue
        profile_value = adaptive_policy_profile.get(profile_key)
        profile_summary["adaptive_policy_profile_requested_overrides"][profile_key] = _json_safe_profile_value(
            profile_value
        )

        if profile_key == "max_intents_per_underlying":
            parsed_value = _parse_int(profile_value)
            if parsed_value is None or parsed_value < 1:
                profile_summary["adaptive_policy_profile_ignored_overrides"].append(profile_key)
                continue
            clamped_value = max(int(floor), min(int(ceiling), int(parsed_value)))
            effective_value = min(int(base_floor), clamped_value)
        else:
            if resolved_key == "min_probability_confidence":
                parsed_value = _sanitize_probability_threshold(profile_value)
            else:
                parsed_value = _sanitize_edge_threshold(profile_value)
            if parsed_value is None:
                profile_summary["adaptive_policy_profile_ignored_overrides"].append(profile_key)
                continue
            clamped_value = max(float(floor), min(float(ceiling), float(parsed_value)))
            base_value = resolved[resolved_key]
            if isinstance(base_value, (int, float)) and math.isfinite(float(base_value)):
                effective_value = max(float(base_value), clamped_value)
            else:
                effective_value = max(float(base_floor), clamped_value)
            if resolved_key == "min_probability_confidence":
                effective_value = max(float(base_floor), effective_value)

        if resolved_key == "max_intents_per_underlying":
            resolved[resolved_key] = int(effective_value)
            profile_summary["adaptive_policy_profile_effective_overrides"][profile_key] = int(effective_value)
            if int(parsed_value) != int(effective_value):
                profile_summary["adaptive_policy_profile_clamped_overrides"][profile_key] = {
                    "requested": _json_safe_profile_value(profile_value),
                    "effective": int(effective_value),
                }
        else:
            resolved[resolved_key] = float(effective_value)
            profile_summary["adaptive_policy_profile_effective_overrides"][profile_key] = round(
                float(effective_value),
                6,
            )
            requested_value = parsed_value
            if float(requested_value) != float(effective_value):
                profile_summary["adaptive_policy_profile_clamped_overrides"][profile_key] = {
                    "requested": round(float(requested_value), 6),
                    "effective": round(float(effective_value), 6),
                }
        applied_any = True

    profile_summary["adaptive_policy_profile_applied"] = applied_any

    return resolved, profile_summary


def _entry_price_min_expected_edge_floor(max_entry_price_dollars: Any) -> float:
    entry_price = _parse_float(max_entry_price_dollars)
    if entry_price is None or not math.isfinite(entry_price):
        return 0.0
    clipped_entry = max(0.01, min(0.99, float(entry_price)))
    # High entry prices require stronger net edge to preserve payout asymmetry.
    base_floor = 0.003
    premium_pressure = max(0.0, clipped_entry - 0.55) * 0.02
    very_high_price_pressure = max(0.0, clipped_entry - 0.80) * 0.03
    floor = min(0.03, base_floor + premium_pressure + very_high_price_pressure)
    return round(float(max(0.0, floor)), 6)


def _entry_price_min_probability_floor(max_entry_price_dollars: Any) -> float | None:
    entry_price = _parse_float(max_entry_price_dollars)
    if entry_price is None or not math.isfinite(entry_price):
        return None
    clipped_entry = max(0.01, min(0.99, float(entry_price)))
    # Only tighten for extreme-priced entries where payout asymmetry is sharp.
    if clipped_entry < 0.97:
        return None
    base_margin = 0.004
    premium_margin = max(0.0, clipped_entry - 0.97) * 0.20
    margin = min(0.02, base_margin + premium_margin)
    floor = min(0.995, clipped_entry + margin)
    return round(max(0.0, float(floor)), 6)


def _load_metar_age_policy(
    *,
    metar_age_policy_json: str | None,
) -> dict[str, Any]:
    path_text = _normalize_text(metar_age_policy_json)
    result: dict[str, Any] = {
        "path": path_text,
        "loaded": False,
        "error": "",
        "station_overrides": {},
        "station_hour_overrides": {},
        "warnings": [],
    }
    if not path_text:
        return result
    path = Path(path_text)
    if not path.exists():
        result["error"] = "missing_policy_file"
        return result
    payload = _read_json(path)
    if not payload:
        result["error"] = "invalid_policy_payload"
        return result

    station_overrides: dict[str, float] = {}
    station_payload = payload.get("station_max_age_minutes")
    if isinstance(station_payload, dict):
        for key, raw_value in station_payload.items():
            station = _normalize_text(key).upper()
            if not station:
                continue
            minutes = _sanitize_metar_age_minutes(raw_value)
            if minutes is None:
                result["warnings"].append(f"ignored_station_override:{station}")
                continue
            station_overrides[station] = minutes

    station_hour_overrides: dict[str, dict[int, float]] = {}
    station_hour_payload = payload.get("station_local_hour_max_age_minutes")
    if isinstance(station_hour_payload, dict):
        for key, per_hour in station_hour_payload.items():
            station = _normalize_text(key).upper()
            if not station or not isinstance(per_hour, dict):
                if station:
                    result["warnings"].append(f"ignored_station_hour_override:{station}")
                continue
            normalized_per_hour: dict[int, float] = {}
            for hour_key, raw_value in per_hour.items():
                hour = _parse_int(hour_key)
                minutes = _sanitize_metar_age_minutes(raw_value)
                if hour is None or minutes is None or hour < 0 or hour > 23:
                    result["warnings"].append(f"ignored_station_hour_value:{station}:{hour_key}")
                    continue
                normalized_per_hour[int(hour)] = minutes
            if normalized_per_hour:
                station_hour_overrides[station] = normalized_per_hour

    result["loaded"] = True
    result["station_overrides"] = station_overrides
    result["station_hour_overrides"] = station_hour_overrides
    return result


def _default_alpha_consensus_path(output_dir: str) -> Path:
    return Path(output_dir) / "breadth_worker" / "breadth_worker_consensus_latest.json"


def _load_alpha_consensus(
    *,
    output_dir: str,
    alpha_consensus_json: str | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    explicit_path_text = _normalize_text(alpha_consensus_json)
    resolved_path_text = explicit_path_text
    if not resolved_path_text:
        resolved_path_text = str(_default_alpha_consensus_path(output_dir))
    resolved_path = Path(resolved_path_text)
    meta: dict[str, Any] = {
        "alpha_consensus_json_used": str(resolved_path),
        "loaded": False,
        "error": "",
        "candidate_count": 0,
        "usable_candidate_count": 0,
        "top_market_side": "",
    }
    if not resolved_path.exists():
        if explicit_path_text:
            meta["error"] = "alpha_consensus_missing"
        return {}, meta
    payload = _read_json(resolved_path)
    if not payload:
        meta["error"] = "alpha_consensus_unreadable"
        return {}, meta
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        meta["error"] = "alpha_consensus_missing_candidates"
        return {}, meta
    by_market_side: dict[str, dict[str, Any]] = {}
    candidate_count = 0

    def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, float, float, float, float]:
        rank = candidate.get("consensus_rank")
        rank_sort = float(rank) if isinstance(rank, int) else 999999.0
        alpha_sort = -float(candidate.get("consensus_alpha_score") or 0.0)
        weighted_ratio_sort = -float(candidate.get("weighted_support_ratio") or 0.0)
        support_count_sort = -float(candidate.get("profile_support_count") or 0.0)
        weighted_score_sort = -float(candidate.get("weighted_support_score") or 0.0)
        return (rank_sort, alpha_sort, weighted_ratio_sort, support_count_sort, weighted_score_sort)

    for row in candidates:
        if not isinstance(row, dict):
            continue
        market_ticker = _normalize_text(row.get("market_ticker"))
        side = _normalize_text(row.get("side")).lower()
        market_side_key = _normalize_text(row.get("market_side_key"))
        if not market_side_key and market_ticker and side:
            market_side_key = f"{market_ticker}|{side}"
        if not market_side_key or side not in {"yes", "no"}:
            continue
        profile_support_count = _parse_int(row.get("profile_support_count")) or 0
        profile_support_ratio = _parse_float(row.get("profile_support_ratio"))
        weighted_support_score = _parse_float(row.get("weighted_support_score"))
        weighted_support_ratio = _parse_float(row.get("weighted_support_ratio"))
        consensus_alpha_score = _parse_float(row.get("consensus_alpha_score"))
        consensus_rank = _parse_int(row.get("consensus_rank"))
        profile_names = row.get("profile_names")
        profile_names_joined = (
            ",".join([_normalize_text(item) for item in profile_names if _normalize_text(item)])
            if isinstance(profile_names, list)
            else _normalize_text(profile_names)
        )
        candidate_entry = {
            "profile_support_count": profile_support_count,
            "profile_support_ratio": profile_support_ratio,
            "weighted_support_score": weighted_support_score,
            "weighted_support_ratio": weighted_support_ratio,
            "consensus_alpha_score": consensus_alpha_score,
            "consensus_rank": consensus_rank,
            "profile_names": profile_names_joined,
        }
        existing_entry = by_market_side.get(market_side_key)
        if not isinstance(existing_entry, dict) or _candidate_sort_key(candidate_entry) < _candidate_sort_key(existing_entry):
            by_market_side[market_side_key] = candidate_entry
        candidate_count += 1
    if not by_market_side:
        meta["error"] = "alpha_consensus_no_usable_candidates"
        return {}, meta
    meta["loaded"] = True
    meta["candidate_count"] = candidate_count
    meta["usable_candidate_count"] = len(by_market_side)
    ranked_rows = sorted(
        by_market_side.items(),
        key=lambda item: (
            float(item[1].get("consensus_rank")) if isinstance(item[1].get("consensus_rank"), int) else 999999,
            -float(item[1].get("consensus_alpha_score") or 0.0),
            item[0],
        ),
    )
    if ranked_rows:
        meta["top_market_side"] = ranked_rows[0][0]
    return by_market_side, meta


def _default_weather_pattern_profile_path(output_dir: str) -> Path:
    return Path(output_dir) / "weather_pattern" / "weather_pattern_profile_latest.json"


def _find_latest_weather_pattern_profile_json(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates: list[Path] = []
    for pattern in (
        "weather_pattern_profile_*.json",
        "weather_pattern/weather_pattern_profile_*.json",
        "health/kalshi_temperature_weather_pattern_latest.json",
        "health/kalshi_temperature_weather_pattern_*.json",
        "outputs/health/kalshi_temperature_weather_pattern_latest.json",
        "outputs/health/kalshi_temperature_weather_pattern_*.json",
    ):
        for candidate in directory.glob(pattern):
            if candidate.exists():
                candidates.append(candidate)
    if not candidates:
        default_path = _default_weather_pattern_profile_path(output_dir)
        if default_path.exists():
            return str(default_path)
        return ""
    ranked = sorted(
        candidates,
        key=lambda path: (
            float(path.stat().st_mtime) if path.exists() else 0.0,
            str(path),
        ),
        reverse=True,
    )
    return str(ranked[0]) if ranked else ""


def _normalize_weather_pattern_dimension_name(value: Any) -> str:
    normalized = _normalize_text(value).lower()
    aliases = {
        "settlement_station": "station",
        "station": "station",
        "local_hour": "local_hour",
        "constraint_status": "signal_type",
        "signal_type": "signal_type",
        "side": "side",
        "metar_age_bucket": "metar_age_bucket",
        "weather_evidence_tier": "weather_evidence_tier",
    }
    return aliases.get(normalized, normalized)


def _normalize_weather_pattern_dimension_key(*, dimension: str, key: Any) -> str:
    text = _normalize_text(key)
    if not text:
        return ""
    if dimension == "station":
        return text.upper()
    if dimension in {"signal_type", "side", "weather_evidence_tier", "metar_age_bucket"}:
        return text.lower()
    if dimension == "local_hour":
        local_hour = _parse_int(text)
        return str(local_hour) if isinstance(local_hour, int) else text
    return text


def _coerce_weather_pattern_bucket_profiles(
    *,
    payload: dict[str, Any] | None,
    profile_payload: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    normalized_bucket_profiles: dict[str, dict[str, dict[str, Any]]] = {}

    def _ingest(raw_dimensions: dict[str, Any]) -> None:
        for raw_dimension, raw_bucket_rows in raw_dimensions.items():
            dimension = _normalize_weather_pattern_dimension_name(raw_dimension)
            if not dimension or not isinstance(raw_bucket_rows, dict):
                continue
            dimension_entries = normalized_bucket_profiles.setdefault(dimension, {})
            for raw_key, raw_entry in raw_bucket_rows.items():
                key_text = _normalize_weather_pattern_dimension_key(dimension=dimension, key=raw_key)
                if not key_text or not isinstance(raw_entry, dict):
                    continue
                entry = dict(raw_entry)
                samples = max(
                    0,
                    int(
                        _parse_float(entry.get("samples"))
                        or _parse_float(entry.get("attempts"))
                        or 0
                    ),
                )
                entry["samples"] = samples

                expectancy = _parse_float(entry.get("expectancy_per_trade"))
                if expectancy is None:
                    expectancy = _parse_float(entry.get("realized_per_trade"))
                if expectancy is None:
                    expectancy = _parse_float(entry.get("expected_edge_mean"))
                if expectancy is None and samples > 0:
                    expected_edge_sum = _parse_float(entry.get("expected_edge_sum"))
                    if isinstance(expected_edge_sum, float):
                        expectancy = expected_edge_sum / float(samples)
                if isinstance(expectancy, float):
                    entry["expectancy_per_trade"] = round(float(expectancy), 6)

                existing = dimension_entries.get(key_text)
                existing_samples = (
                    int(_parse_float(existing.get("samples")) or 0)
                    if isinstance(existing, dict)
                    else -1
                )
                if existing_samples > samples:
                    continue
                dimension_entries[key_text] = entry

    raw_bucket_profiles = profile_payload.get("bucket_profiles")
    if isinstance(raw_bucket_profiles, dict):
        _ingest(raw_bucket_profiles)
    if normalized_bucket_profiles:
        return normalized_bucket_profiles

    bucket_dimensions = profile_payload.get("bucket_dimensions")
    if not isinstance(bucket_dimensions, dict):
        profile_section = payload.get("profile") if isinstance(payload, dict) else None
        if isinstance(profile_section, dict) and isinstance(profile_section.get("bucket_dimensions"), dict):
            bucket_dimensions = profile_section.get("bucket_dimensions")
    if not isinstance(bucket_dimensions, dict) and isinstance(payload, dict):
        if isinstance(payload.get("bucket_dimensions"), dict):
            bucket_dimensions = payload.get("bucket_dimensions")

    if isinstance(bucket_dimensions, dict):
        _ingest(bucket_dimensions)
    return normalized_bucket_profiles


def _weather_pattern_metar_age_bucket(age_minutes: Any) -> str:
    age = _parse_float(age_minutes)
    if not isinstance(age, float):
        return "unknown"
    if age <= 30.0:
        return "0-30m"
    if age <= 60.0:
        return "31-60m"
    if age <= 120.0:
        return "61-120m"
    if age <= 240.0:
        return "121-240m"
    return "240m+"


def _weather_pattern_is_stale_metar_bucket(bucket_text: Any) -> bool:
    normalized = _normalize_text(bucket_text).lower()
    if not normalized:
        return False
    if "|" in normalized:
        normalized = normalized.split("|", 1)[0]
    normalized = normalized.strip()
    if normalized in {"unknown", "0-30m", "31-60m"}:
        return False
    if normalized in {"61-120m", "121-240m", "240m+", "240+"}:
        return True
    if "stale" in normalized:
        return True

    range_match = re.match(r"^(?P<low>\d+)\s*-\s*(?P<high>\d+)", normalized)
    if range_match:
        return int(range_match.group("low")) >= 61
    plus_match = re.match(r"^(?P<low>\d+)\s*m?\+$", normalized)
    if plus_match:
        return int(plus_match.group("low")) >= 61
    return False


def _weather_pattern_bucket_indicates_stale_metar_pressure(
    *,
    dimension: str,
    bucket_key: str,
    bucket_entry: dict[str, Any],
) -> bool:
    stale_flag = _parse_boolish(bucket_entry.get("stale_metar_pressure"))
    if stale_flag is None:
        stale_flag = _parse_boolish(bucket_entry.get("stale_metar"))
    if stale_flag is True:
        return True
    if dimension in {"metar_age_bucket", "weather_evidence_tier"}:
        if _weather_pattern_is_stale_metar_bucket(bucket_key):
            return True
    metar_age_bucket_hint = _normalize_text(bucket_entry.get("metar_age_bucket"))
    if metar_age_bucket_hint and _weather_pattern_is_stale_metar_bucket(metar_age_bucket_hint):
        return True
    weather_tier_hint = _normalize_text(bucket_entry.get("weather_evidence_tier"))
    if weather_tier_hint and _weather_pattern_is_stale_metar_bucket(weather_tier_hint):
        return True
    return False


def _weather_pattern_dimension_keys_for_intent(intent: "TemperatureTradeIntent") -> dict[str, str]:
    observed_ts = _parse_ts(getattr(intent, "metar_observation_time_utc", None)) or _parse_ts(
        getattr(intent, "captured_at", None)
    )
    local_hour = "unknown"
    if isinstance(observed_ts, datetime):
        timezone_name = _normalize_text(getattr(intent, "settlement_timezone", ""))
        if timezone_name:
            try:
                local_hour = str(int(observed_ts.astimezone(ZoneInfo(timezone_name)).hour))
            except Exception:
                local_hour = str(int(observed_ts.hour))
        else:
            local_hour = str(int(observed_ts.hour))

    metar_age_bucket = _weather_pattern_metar_age_bucket(
        getattr(intent, "metar_observation_age_minutes", None)
    )
    forecast_model_status = _normalize_text(getattr(intent, "forecast_model_status", "")).lower()
    taf_status = _normalize_text(getattr(intent, "taf_status", "")).lower()
    return {
        "station": _normalize_text(getattr(intent, "settlement_station", "")).upper(),
        "local_hour": local_hour,
        "signal_type": _normalize_text(getattr(intent, "constraint_status", "")).lower(),
        "side": _normalize_text(getattr(intent, "side", "")).lower(),
        "metar_age_bucket": metar_age_bucket,
        "weather_evidence_tier": (
            f"{metar_age_bucket}|{forecast_model_status or taf_status or 'unknown'}"
        ),
    }


def _weather_pattern_profile_sections(profile: dict[str, Any]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = [profile]
    for key in (
        "headline_metrics",
        "summary",
        "metrics",
        "overall",
        "regime_risk",
        "recommendations",
        "recommendation",
        "weather_regime",
        "pattern_summary",
    ):
        section = profile.get(key)
        if isinstance(section, dict):
            sections.append(section)
    nested_profile = profile.get("profile")
    if isinstance(nested_profile, dict):
        sections.append(nested_profile)
        for key in ("regime_risk", "recommendations", "summary", "metrics"):
            section = nested_profile.get(key)
            if isinstance(section, dict):
                sections.append(section)
    return sections


def _weather_pattern_first_float(
    sections: list[dict[str, Any]],
    *,
    names: tuple[str, ...],
) -> float | None:
    for section in sections:
        for name in names:
            parsed = _parse_float(section.get(name))
            if isinstance(parsed, float):
                return float(parsed)
    return None


def _weather_pattern_first_int(
    sections: list[dict[str, Any]],
    *,
    names: tuple[str, ...],
) -> int:
    for section in sections:
        for name in names:
            parsed = _parse_int(section.get(name))
            if isinstance(parsed, int):
                return parsed
    return 0


def _weather_pattern_global_risk_off_state(
    *,
    profile: dict[str, Any] | None,
    enabled: bool,
    concentration_threshold: float,
    min_attempts: int,
    stale_metar_share_threshold: float,
) -> dict[str, Any]:
    safe_enabled = bool(enabled)
    safe_concentration_threshold = _clamp_unit(concentration_threshold, 0.75)
    safe_min_attempts = max(1, int(_parse_float(min_attempts) or 24))
    safe_stale_metar_share_threshold = _clamp_unit(stale_metar_share_threshold, 0.50)
    snapshot: dict[str, Any] = {
        "enabled": safe_enabled,
        "active": False,
        "status": "disabled" if not safe_enabled else "profile_unavailable",
        "recommendation_active": False,
        "metrics_triggered": False,
        "concentration_threshold": round(float(safe_concentration_threshold), 6),
        "min_attempts": int(safe_min_attempts),
        "stale_metar_share_threshold": round(float(safe_stale_metar_share_threshold), 6),
        "observed_concentration": None,
        "observed_attempts": 0,
        "observed_stale_metar_share": None,
        "activation_reasons": [],
    }
    if not safe_enabled:
        return snapshot
    if not isinstance(profile, dict):
        return snapshot

    sections = _weather_pattern_profile_sections(profile)
    recommendation_active = False
    recommendation_keys = (
        "weather_pattern_global_risk_off",
        "global_risk_off",
        "risk_off",
        "risk_off_active",
        "risk_off_recommended",
        "hard_risk_off",
        "recommend_global_risk_off",
    )
    recommendation_text_keys = (
        "action",
        "recommendation",
        "risk_posture",
        "posture",
        "mode",
        "global_gate",
    )
    for section in sections:
        for key in recommendation_keys:
            if _parse_boolish(section.get(key)) is True:
                recommendation_active = True
                break
        if recommendation_active:
            break
        for key in recommendation_text_keys:
            text = _normalize_text(section.get(key)).lower()
            if not text:
                continue
            if any(marker in text for marker in ("risk_off", "risk-off", "de-risk", "derisk", "halt")):
                recommendation_active = True
                break
        if recommendation_active:
            break

    observed_concentration = _weather_pattern_first_float(
        sections,
        names=(
            "negative_expectancy_attempt_share_confidence_adjusted",
            "negative_expectancy_regime_concentration",
            "negative_expectancy_regime_share",
            "negative_expectancy_share",
            "negative_expectancy_attempt_share",
            "weather_pattern_negative_expectancy_concentration",
            "risk_off_concentration",
        ),
    )
    observed_attempts = max(
        0,
        _weather_pattern_first_int(
            sections,
            names=(
                "attempts_total",
                "total_attempts",
                "sample_count",
                "samples_total",
                "resolved_attempts_total",
                "weather_pattern_attempts_total",
                "regime_attempts_total",
            ),
        ),
    )
    observed_stale_metar_share = _weather_pattern_first_float(
        sections,
        names=(
            "stale_metar_negative_attempt_share_confidence_adjusted",
            "stale_metar_attempt_share_confidence_adjusted",
            "stale_metar_share_confidence_adjusted",
            "stale_metar_share",
            "stale_metar_pressure_share",
            "stale_metar_attempt_share",
            "negative_expectancy_stale_metar_share",
            "weather_pattern_stale_metar_share",
        ),
    )
    observed_concentration_from_profile = isinstance(observed_concentration, float)
    observed_stale_metar_share_from_profile = isinstance(observed_stale_metar_share, float)

    bucket_profiles = profile.get("bucket_profiles")
    if not isinstance(bucket_profiles, dict):
        bucket_profiles = {}

    attempts_by_dimension: dict[str, int] = {}
    negative_attempts_by_dimension: dict[str, int] = {}
    stale_negative_attempts_by_dimension: dict[str, int] = {}
    for dimension_name, bucket_rows in bucket_profiles.items():
        if not isinstance(bucket_rows, dict):
            continue
        dimension = _normalize_weather_pattern_dimension_name(dimension_name)
        attempts_total = 0
        negative_attempts = 0
        stale_negative_attempts = 0
        for bucket_key, bucket_entry in bucket_rows.items():
            if not isinstance(bucket_entry, dict):
                continue
            samples = max(
                0,
                int(
                    _parse_float(bucket_entry.get("samples"))
                    or _parse_float(bucket_entry.get("attempts"))
                    or 0
                ),
            )
            attempts_total += samples
            expectancy = _parse_float(bucket_entry.get("expectancy_per_trade"))
            is_negative = isinstance(expectancy, float) and expectancy < 0.0
            if is_negative:
                negative_attempts += samples
                if _weather_pattern_bucket_indicates_stale_metar_pressure(
                    dimension=dimension,
                    bucket_key=_normalize_text(bucket_key),
                    bucket_entry=bucket_entry,
                ):
                    stale_negative_attempts += samples
        attempts_by_dimension[dimension] = attempts_total
        negative_attempts_by_dimension[dimension] = negative_attempts
        stale_negative_attempts_by_dimension[dimension] = stale_negative_attempts
    negative_signal_dimensions = sum(
        1 for attempts in negative_attempts_by_dimension.values() if int(attempts) > 0
    )

    if observed_attempts <= 0:
        for preferred_dimension in ("weather_evidence_tier", "metar_age_bucket", "station", "signal_type"):
            if attempts_by_dimension.get(preferred_dimension, 0) > 0:
                observed_attempts = int(attempts_by_dimension[preferred_dimension])
                break
    if observed_attempts <= 0 and attempts_by_dimension:
        observed_attempts = max(0, max(attempts_by_dimension.values()))

    negative_expectancy_buckets = (
        profile.get("negative_expectancy_buckets")
        if isinstance(profile.get("negative_expectancy_buckets"), list)
        else []
    )
    negative_attempts = sum(
        max(
            0,
            int(_parse_float(row.get("attempts")) or _parse_float(row.get("samples")) or 0),
        )
        for row in negative_expectancy_buckets
        if isinstance(row, dict)
    )
    stale_negative_attempts = sum(
        max(
            0,
            int(_parse_float(row.get("attempts")) or _parse_float(row.get("samples")) or 0),
        )
        for row in negative_expectancy_buckets
        if isinstance(row, dict)
        and _weather_pattern_bucket_indicates_stale_metar_pressure(
            dimension=_normalize_weather_pattern_dimension_name(row.get("dimension")),
            bucket_key=(
                _normalize_text(row.get("metar_age_bucket"))
                or _normalize_text(row.get("bucket"))
            ),
            bucket_entry=row,
        )
    )

    if negative_attempts <= 0:
        for preferred_dimension in ("weather_evidence_tier", "metar_age_bucket", "station", "signal_type"):
            if negative_attempts_by_dimension.get(preferred_dimension, 0) > 0:
                negative_attempts = int(negative_attempts_by_dimension[preferred_dimension])
                stale_negative_attempts = int(stale_negative_attempts_by_dimension.get(preferred_dimension, 0))
                break

    if not isinstance(observed_concentration, float) and observed_attempts > 0 and negative_attempts > 0:
        observed_concentration = negative_attempts / float(observed_attempts)
    if (
        not isinstance(observed_stale_metar_share, float)
        and negative_attempts > 0
        and stale_negative_attempts >= 0
    ):
        observed_stale_metar_share = stale_negative_attempts / float(max(1, negative_attempts))

    profile_metrics_available = bool(
        observed_concentration_from_profile and observed_stale_metar_share_from_profile
    )
    effective_concentration_threshold = float(safe_concentration_threshold)
    effective_stale_metar_share_threshold = float(safe_stale_metar_share_threshold)
    effective_min_attempts = int(safe_min_attempts)
    fallback_signal_confirmed = True
    if not profile_metrics_available:
        effective_concentration_threshold = max(
            effective_concentration_threshold,
            float(_WEATHER_PATTERN_RISK_OFF_FALLBACK_CONCENTRATION_FLOOR),
        )
        effective_stale_metar_share_threshold = max(
            effective_stale_metar_share_threshold,
            float(_WEATHER_PATTERN_RISK_OFF_FALLBACK_STALE_SHARE_FLOOR),
        )
        effective_min_attempts = max(
            effective_min_attempts,
            int(_WEATHER_PATTERN_RISK_OFF_FALLBACK_MIN_ATTEMPTS_FLOOR),
        )
        fallback_signal_confirmed = bool(
            negative_signal_dimensions >= int(_WEATHER_PATTERN_RISK_OFF_FALLBACK_MIN_NEGATIVE_DIMENSIONS)
        )

    metrics_triggered = bool(
        observed_attempts >= effective_min_attempts
        and isinstance(observed_concentration, float)
        and float(observed_concentration) >= effective_concentration_threshold
        and isinstance(observed_stale_metar_share, float)
        and float(observed_stale_metar_share) >= effective_stale_metar_share_threshold
        and fallback_signal_confirmed
    )
    active = bool(recommendation_active or metrics_triggered)
    activation_reasons: list[str] = []
    if recommendation_active:
        activation_reasons.append("recommendation")
    if metrics_triggered:
        activation_reasons.append("metrics_threshold")

    snapshot["status"] = "active" if active else "inactive"
    snapshot["active"] = active
    snapshot["recommendation_active"] = bool(recommendation_active)
    snapshot["metrics_triggered"] = bool(metrics_triggered)
    snapshot["observed_attempts"] = int(max(0, observed_attempts))
    snapshot["observed_concentration"] = (
        round(float(observed_concentration), 6)
        if isinstance(observed_concentration, float)
        else None
    )
    snapshot["observed_stale_metar_share"] = (
        round(float(observed_stale_metar_share), 6)
        if isinstance(observed_stale_metar_share, float)
        else None
    )
    snapshot["activation_reasons"] = activation_reasons
    snapshot["metrics_source"] = "profile" if profile_metrics_available else "bucket_fallback"
    snapshot["negative_signal_dimensions"] = int(max(0, negative_signal_dimensions))
    snapshot["effective_concentration_threshold"] = round(float(effective_concentration_threshold), 6)
    snapshot["effective_min_attempts"] = int(max(1, effective_min_attempts))
    snapshot["effective_stale_metar_share_threshold"] = round(float(effective_stale_metar_share_threshold), 6)
    snapshot["fallback_signal_confirmed"] = bool(fallback_signal_confirmed)
    return snapshot


def _load_weather_pattern_profile(
    *,
    output_dir: str,
    now_utc: datetime,
    enabled: bool,
    weather_pattern_profile: dict[str, Any] | None,
    max_age_hours: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    meta: dict[str, Any] = {
        "weather_pattern_profile_json_used": "",
        "loaded": False,
        "enabled": bool(enabled),
        "status": "disabled" if not enabled else "missing",
        "error": "",
        "source_origin": "",
        "source_age_hours": None,
        "bucket_profile_count": 0,
    }
    if not enabled:
        return {}, meta

    payload: dict[str, Any] | None = None
    source_origin = ""
    source_file = ""
    if isinstance(weather_pattern_profile, dict):
        payload = weather_pattern_profile
        source_origin = "injected"
        source_file = "injected"
    else:
        resolved_path_text = _find_latest_weather_pattern_profile_json(output_dir)
        if not resolved_path_text:
            return {}, meta
        source_file = resolved_path_text
        source_origin = "latest_artifact"
        payload = _read_json(Path(resolved_path_text))
        if not payload:
            meta["weather_pattern_profile_json_used"] = resolved_path_text
            meta["error"] = "weather_pattern_profile_unreadable"
            meta["status"] = "unreadable"
            return {}, meta

    profile_payload = payload.get("weather_pattern_profile") if isinstance(payload, dict) else None
    if not isinstance(profile_payload, dict) and isinstance(payload, dict):
        profile_payload = payload.get("profile")
    if not isinstance(profile_payload, dict):
        profile_payload = payload if isinstance(payload, dict) else None
    if not isinstance(profile_payload, dict):
        meta["weather_pattern_profile_json_used"] = source_file
        meta["error"] = "weather_pattern_profile_invalid"
        meta["status"] = "invalid"
        return {}, meta

    profile_payload = dict(profile_payload)
    for key in ("overall", "summary", "metrics", "headline_metrics"):
        if key not in profile_payload and isinstance(payload, dict) and isinstance(payload.get(key), dict):
            profile_payload[key] = dict(payload.get(key))
    bucket_profiles = _coerce_weather_pattern_bucket_profiles(
        payload=payload,
        profile_payload=profile_payload,
    )
    profile_payload["bucket_profiles"] = bucket_profiles

    source_age_hours = _parse_float(profile_payload.get("source_age_hours"))
    if source_age_hours is None:
        captured_at = _parse_ts(profile_payload.get("captured_at"))
        if not isinstance(captured_at, datetime) and isinstance(payload, dict):
            captured_at = _parse_ts(payload.get("captured_at"))
        if not isinstance(captured_at, datetime) and isinstance(payload, dict):
            captured_at = _parse_ts(payload.get("captured_at_utc"))
        if isinstance(captured_at, datetime):
            source_age_hours = max(0.0, (now_utc - captured_at).total_seconds() / 3600.0)
    if source_age_hours is None and source_file and source_origin == "latest_artifact":
        try:
            source_mtime = datetime.fromtimestamp(Path(source_file).stat().st_mtime, tz=timezone.utc)
            source_age_hours = max(0.0, (now_utc - source_mtime).total_seconds() / 3600.0)
        except OSError:
            source_age_hours = None

    if source_age_hours is not None and float(source_age_hours) > max(0.0, float(max_age_hours)):
        meta["weather_pattern_profile_json_used"] = source_file
        meta["source_origin"] = source_origin
        meta["source_age_hours"] = round(float(source_age_hours), 6)
        meta["status"] = "stale"
        meta["error"] = "weather_pattern_profile_stale"
        return {}, meta

    bucket_count = sum(len(value) for value in bucket_profiles.values() if isinstance(value, dict))
    meta["loaded"] = True
    meta["status"] = "ready" if bucket_count > 0 else "no_bucket_profiles"
    meta["weather_pattern_profile_json_used"] = source_file
    meta["source_origin"] = source_origin
    meta["source_age_hours"] = (
        round(float(source_age_hours), 6) if isinstance(source_age_hours, (int, float)) else None
    )
    meta["bucket_profile_count"] = int(bucket_count)
    if bucket_count == 0:
        meta["error"] = "weather_pattern_profile_no_bucket_profiles"
    return profile_payload, meta


def _weather_pattern_hardening_for_intent(
    *,
    intent: "TemperatureTradeIntent",
    profile: dict[str, Any] | None,
    min_bucket_samples: int,
    negative_expectancy_threshold: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "disabled",
        "matched_bucket_count": 0,
        "matched_bucket_evidence": [],
        "probability_raise": 0.0,
        "expected_edge_raise": 0.0,
        "hard_block_active": False,
        "hard_block_hits": [],
        "strong_negative_match_count": 0,
        "stale_metar_pressure_match_count": 0,
    }
    if not isinstance(profile, dict) or not bool(profile.get("bucket_profiles")):
        result["status"] = "no_bucket_profiles" if isinstance(profile, dict) else "disabled"
        return result

    bucket_profiles = profile.get("bucket_profiles")
    if not isinstance(bucket_profiles, dict):
        result["status"] = "no_bucket_profiles"
        return result

    sampled_min_bucket_samples = max(1, int(_parse_float(min_bucket_samples) or 1))
    negative_threshold = min(
        -1e-9,
        float(_parse_float(negative_expectancy_threshold) or -0.05),
    )
    strong_negative_cutoff = min(negative_threshold - 0.02, negative_threshold * 1.5)
    dimension_keys = _weather_pattern_dimension_keys_for_intent(intent)

    total_probability_raise = 0.0
    total_expected_edge_raise = 0.0
    matched_evidence: list[str] = []
    hard_block_hits: list[str] = []
    strong_negative_match_count = 0
    stale_metar_pressure_match_count = 0
    for dimension, key_text in dimension_keys.items():
        if not key_text:
            continue
        dimension_bucket = bucket_profiles.get(dimension)
        if not isinstance(dimension_bucket, dict):
            continue
        entry = dimension_bucket.get(key_text)
        if not isinstance(entry, dict):
            continue
        samples = max(0, int(_parse_float(entry.get("samples")) or 0))
        expectancy = _parse_float(entry.get("expectancy_per_trade"))
        if not isinstance(expectancy, float):
            continue
        if samples < sampled_min_bucket_samples:
            continue
        if expectancy > negative_threshold:
            continue

        probability_raise_raw = _parse_float(entry.get("probability_raise"))
        expected_edge_raise_raw = _parse_float(entry.get("expected_edge_raise"))
        if probability_raise_raw is None:
            probability_raise = min(0.04, max(0.0, (negative_threshold - expectancy) * 0.30))
        else:
            probability_raise = min(0.04, max(0.0, float(probability_raise_raw)))
        if expected_edge_raise_raw is None:
            expected_edge_raise = min(0.008, max(0.0, (negative_threshold - expectancy) * 0.06))
        else:
            expected_edge_raise = min(0.008, max(0.0, float(expected_edge_raise_raw)))

        stale_metar_pressure_hit = _weather_pattern_bucket_indicates_stale_metar_pressure(
            dimension=dimension,
            bucket_key=key_text,
            bucket_entry=entry,
        )
        if stale_metar_pressure_hit:
            stale_metar_pressure_match_count += 1
            probability_raise = min(
                0.06,
                max(
                    float(probability_raise) * 1.50,
                    float(probability_raise) + 0.006,
                    0.012,
                ),
            )
            expected_edge_raise = min(
                0.012,
                max(
                    float(expected_edge_raise) * 1.50,
                    float(expected_edge_raise) + 0.0015,
                    0.0025,
                ),
            )

        total_probability_raise += float(probability_raise)
        total_expected_edge_raise += float(expected_edge_raise)
        matched_evidence.append(
            f"{dimension}:{key_text}:samples={samples}:expectancy={float(expectancy):.6f}:"
            f"prob_raise={float(probability_raise):.6f}:edge_raise={float(expected_edge_raise):.6f}:"
            f"stale_metar_pressure={str(stale_metar_pressure_hit).lower()}"
        )
        realized_trade_count = max(0, int(_parse_float(entry.get("realized_trade_count")) or 0))
        realized_per_trade = _parse_float(entry.get("realized_per_trade"))
        edge_realization_ratio = _parse_float(entry.get("edge_realization_ratio"))
        probability_confidence_mean = _parse_float(entry.get("probability_confidence_mean"))
        realized_negative_signal = bool(
            expectancy <= strong_negative_cutoff
            and realized_trade_count >= int(_WEATHER_PATTERN_HARD_BLOCK_MIN_REALIZED_TRADES)
            and (
                (
                    isinstance(realized_per_trade, float)
                    and float(realized_per_trade) <= float(_WEATHER_PATTERN_HARD_BLOCK_REALIZED_PER_TRADE_THRESHOLD)
                )
                or (
                    isinstance(edge_realization_ratio, float)
                    and float(edge_realization_ratio) <= float(_WEATHER_PATTERN_HARD_BLOCK_MAX_EDGE_REALIZATION_RATIO)
                )
            )
        )
        strong_expected_only_signal = bool(
            expectancy <= strong_negative_cutoff
            and realized_trade_count <= 0
            and samples >= int(_WEATHER_PATTERN_HARD_BLOCK_EXPECTED_ONLY_MIN_SAMPLES)
            and isinstance(probability_confidence_mean, float)
            and float(probability_confidence_mean) >= float(_WEATHER_PATTERN_HARD_BLOCK_EXPECTED_ONLY_MIN_CONFIDENCE)
        )
        if realized_negative_signal or strong_expected_only_signal:
            strong_negative_match_count += 1
            hard_block_reason = "realized_negative" if realized_negative_signal else "expected_only_high_confidence"
            realized_per_trade_text = (
                f"{float(realized_per_trade):.6f}" if isinstance(realized_per_trade, float) else ""
            )
            edge_realization_ratio_text = (
                f"{float(edge_realization_ratio):.6f}" if isinstance(edge_realization_ratio, float) else ""
            )
            confidence_mean_text = (
                f"{float(probability_confidence_mean):.6f}" if isinstance(probability_confidence_mean, float) else ""
            )
            hard_block_hits.append(
                f"{dimension}:{key_text}:samples={samples}:expectancy={float(expectancy):.6f}:"
                f"realized_trade_count={realized_trade_count}:realized_per_trade={realized_per_trade_text}:"
                f"edge_realization_ratio={edge_realization_ratio_text}:confidence_mean={confidence_mean_text}:"
                f"reason={hard_block_reason}"
            )

    result["status"] = "ready" if matched_evidence else "no_match"
    result["matched_bucket_count"] = len(matched_evidence)
    result["matched_bucket_evidence"] = matched_evidence
    probability_cap = 0.06 if stale_metar_pressure_match_count > 0 else 0.04
    expected_edge_cap = 0.012 if stale_metar_pressure_match_count > 0 else 0.008
    result["probability_raise"] = round(min(probability_cap, total_probability_raise), 6)
    result["expected_edge_raise"] = round(min(expected_edge_cap, total_expected_edge_raise), 6)
    result["strong_negative_match_count"] = int(strong_negative_match_count)
    result["stale_metar_pressure_match_count"] = int(stale_metar_pressure_match_count)
    result["hard_block_hits"] = hard_block_hits
    result["hard_block_active"] = bool(strong_negative_match_count >= 2)
    return result


def _weather_pattern_negative_regime_suppression_candidates(
    *,
    profile: dict[str, Any] | None,
    enabled: bool,
    min_bucket_samples: int,
    negative_expectancy_threshold: float,
    top_n: int,
) -> dict[str, Any]:
    safe_enabled = bool(enabled)
    safe_min_bucket_samples = max(1, int(_parse_float(min_bucket_samples) or 1))
    safe_negative_threshold = max(
        -0.25,
        min(
            -0.005,
            float(_parse_float(negative_expectancy_threshold) or -0.08),
        ),
    )
    safe_top_n = max(1, min(64, int(_parse_float(top_n) or 8)))
    state: dict[str, Any] = {
        "enabled": safe_enabled,
        "active": False,
        "status": "disabled" if not safe_enabled else "profile_unavailable",
        "min_bucket_samples": int(safe_min_bucket_samples),
        "negative_expectancy_threshold": round(float(safe_negative_threshold), 6),
        "top_n": int(safe_top_n),
        "candidate_source": "",
        "candidate_count": 0,
        "suppression_candidates": [],
    }
    if not safe_enabled:
        return state
    if not isinstance(profile, dict):
        return state

    suppression_candidates_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    def _upsert_candidate(
        *,
        dimension: str,
        bucket: str,
        samples: int,
        expectancy_per_trade: float,
        source: str,
    ) -> None:
        if samples < safe_min_bucket_samples:
            return
        if expectancy_per_trade > safe_negative_threshold:
            return
        dim_key = _normalize_weather_pattern_dimension_name(dimension)
        bucket_key = _normalize_weather_pattern_dimension_key(dimension=dim_key, key=bucket)
        if not dim_key or not bucket_key:
            return
        key = (dim_key, bucket_key)
        score = (
            abs(float(expectancy_per_trade)),
            int(samples),
        )
        candidate = {
            "dimension": dim_key,
            "bucket": bucket_key,
            "samples": int(samples),
            "expectancy_per_trade": round(float(expectancy_per_trade), 6),
            "source": source,
        }
        existing = suppression_candidates_by_key.get(key)
        if not isinstance(existing, dict):
            suppression_candidates_by_key[key] = candidate
            return
        existing_score = (
            abs(float(_parse_float(existing.get("expectancy_per_trade")) or 0.0)),
            int(_parse_float(existing.get("samples")) or 0),
        )
        if score > existing_score:
            suppression_candidates_by_key[key] = candidate

    preferred_rows = (
        profile.get("negative_expectancy_buckets")
        if isinstance(profile.get("negative_expectancy_buckets"), list)
        else []
    )
    preferred_rows_seen = False
    for raw_row in preferred_rows:
        if not isinstance(raw_row, dict):
            continue
        preferred_rows_seen = True
        raw_dimension = (
            _normalize_text(raw_row.get("dimension"))
            or _normalize_text(raw_row.get("bucket_dimension"))
            or _normalize_text(raw_row.get("dimension_name"))
        )
        dimension = _normalize_weather_pattern_dimension_name(raw_dimension)
        if not dimension:
            continue
        bucket_value = (
            raw_row.get("bucket")
            if _normalize_text(raw_row.get("bucket"))
            else raw_row.get("key")
        )
        if dimension == "station":
            bucket_value = (
                raw_row.get("station")
                if _normalize_text(raw_row.get("station"))
                else bucket_value
            )
        elif dimension == "local_hour":
            bucket_value = (
                raw_row.get("local_hour")
                if _normalize_text(raw_row.get("local_hour"))
                else bucket_value
            )
        elif dimension == "signal_type":
            bucket_value = (
                raw_row.get("signal_type")
                if _normalize_text(raw_row.get("signal_type"))
                else bucket_value
            )
        elif dimension == "side":
            bucket_value = raw_row.get("side") if _normalize_text(raw_row.get("side")) else bucket_value
        elif dimension == "metar_age_bucket":
            bucket_value = (
                raw_row.get("metar_age_bucket")
                if _normalize_text(raw_row.get("metar_age_bucket"))
                else bucket_value
            )
        elif dimension == "weather_evidence_tier":
            bucket_value = (
                raw_row.get("weather_evidence_tier")
                if _normalize_text(raw_row.get("weather_evidence_tier"))
                else bucket_value
            )
        samples = max(
            0,
            int(
                _parse_float(raw_row.get("samples"))
                or _parse_float(raw_row.get("attempts"))
                or 0
            ),
        )
        expectancy_per_trade = _parse_float(raw_row.get("expectancy_per_trade"))
        if expectancy_per_trade is None:
            expectancy_per_trade = _parse_float(raw_row.get("realized_per_trade"))
        if expectancy_per_trade is None:
            expectancy_per_trade = _parse_float(raw_row.get("expected_edge_mean"))
        if expectancy_per_trade is None and samples > 0:
            expected_edge_sum = _parse_float(raw_row.get("expected_edge_sum"))
            if isinstance(expected_edge_sum, float):
                expectancy_per_trade = expected_edge_sum / float(samples)
        if not isinstance(expectancy_per_trade, float):
            continue
        _upsert_candidate(
            dimension=dimension,
            bucket=bucket_value,
            samples=samples,
            expectancy_per_trade=expectancy_per_trade,
            source="negative_expectancy_buckets",
        )

    if not suppression_candidates_by_key:
        bucket_profiles = profile.get("bucket_profiles")
        if isinstance(bucket_profiles, dict):
            for raw_dimension, bucket_rows in bucket_profiles.items():
                dimension = _normalize_weather_pattern_dimension_name(raw_dimension)
                if not dimension or not isinstance(bucket_rows, dict):
                    continue
                for bucket_key, bucket_entry in bucket_rows.items():
                    if not isinstance(bucket_entry, dict):
                        continue
                    samples = max(
                        0,
                        int(
                            _parse_float(bucket_entry.get("samples"))
                            or _parse_float(bucket_entry.get("attempts"))
                            or 0
                        ),
                    )
                    expectancy_per_trade = _parse_float(bucket_entry.get("expectancy_per_trade"))
                    if expectancy_per_trade is None:
                        expectancy_per_trade = _parse_float(bucket_entry.get("realized_per_trade"))
                    if expectancy_per_trade is None:
                        expectancy_per_trade = _parse_float(bucket_entry.get("expected_edge_mean"))
                    if expectancy_per_trade is None and samples > 0:
                        expected_edge_sum = _parse_float(bucket_entry.get("expected_edge_sum"))
                        if isinstance(expected_edge_sum, float):
                            expectancy_per_trade = expected_edge_sum / float(samples)
                    if not isinstance(expectancy_per_trade, float):
                        continue
                    _upsert_candidate(
                        dimension=dimension,
                        bucket=bucket_key,
                        samples=samples,
                        expectancy_per_trade=expectancy_per_trade,
                        source="bucket_profiles",
                    )

    candidates = sorted(
        suppression_candidates_by_key.values(),
        key=lambda row: (
            float(row.get("expectancy_per_trade") or 0.0),
            -int(row.get("samples") or 0),
            _normalize_text(row.get("dimension")),
            _normalize_text(row.get("bucket")),
        ),
    )[:safe_top_n]
    if candidates:
        source_counts = Counter(_normalize_text(row.get("source")) for row in candidates if _normalize_text(row.get("source")))
        state["status"] = "ready"
        state["active"] = True
        state["candidate_source"] = (
            max(source_counts.items(), key=lambda item: (item[1], item[0]))[0]
            if source_counts
            else ("negative_expectancy_buckets" if preferred_rows_seen else "bucket_profiles")
        )
    else:
        state["status"] = "no_candidates"
        state["candidate_source"] = "negative_expectancy_buckets" if preferred_rows_seen else "bucket_profiles"
    state["candidate_count"] = int(len(candidates))
    state["suppression_candidates"] = candidates
    return state


def _build_adaptive_station_metar_age_overrides(
    *,
    base_max_metar_age_minutes: float | None,
    station_interval_stats: dict[str, Any],
    latest_by_station: dict[str, Any],
    now_utc: datetime,
    active_stations: set[str] | None = None,
) -> tuple[dict[str, float], dict[str, dict[str, Any]]]:
    base_limit = _sanitize_metar_age_minutes(base_max_metar_age_minutes)
    if base_limit is None:
        return {}, {}

    normalized_active_stations: set[str] = set()
    if isinstance(active_stations, set):
        normalized_active_stations = {
            _normalize_text(station).upper()
            for station in active_stations
            if _normalize_text(station)
        }

    candidate_stations = set(normalized_active_stations)
    if not candidate_stations:
        candidate_stations.update(
            _normalize_text(station_key).upper()
            for station_key in station_interval_stats.keys()
            if _normalize_text(station_key)
        )
        candidate_stations.update(
            _normalize_text(station_key).upper()
            for station_key in latest_by_station.keys()
            if _normalize_text(station_key)
        )

    adaptive_overrides: dict[str, float] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for station in sorted(candidate_stations):
        payload = station_interval_stats.get(station)
        if not isinstance(payload, dict):
            continue

        latest_interval = _sanitize_metar_age_minutes(payload.get("latest_interval_minutes"))
        median_interval = _sanitize_metar_age_minutes(payload.get("interval_median_minutes"))
        p90_interval = _sanitize_metar_age_minutes(payload.get("interval_p90_minutes"))
        sample_count = _parse_int(payload.get("sample_count")) or 0

        cadence_minutes = p90_interval or median_interval or latest_interval
        if cadence_minutes is None or cadence_minutes <= 0 or sample_count <= 0:
            continue

        # Keep adaptive limits bounded close to known station cadence so the
        # gate is resilient but does not become uninformative. Single-sample
        # stations use a tighter warmup multiplier until more history lands.
        if sample_count >= 2:
            adaptive_limit = max(base_limit, float(cadence_minutes) * 1.15 + 2.0)
            adaptive_mode = "cadence_adaptive"
        else:
            adaptive_limit = max(base_limit, float(cadence_minutes) * 1.05 + 3.0)
            adaptive_mode = "cadence_warmup"
        adaptive_limit = min(adaptive_limit, 75.0)
        if adaptive_limit <= base_limit + 0.5:
            continue

        adaptive_overrides[station] = round(float(adaptive_limit), 3)
        diagnostics[station] = {
            "mode": adaptive_mode,
            "sample_count": sample_count,
            "latest_interval_minutes": latest_interval,
            "interval_median_minutes": median_interval,
            "interval_p90_minutes": p90_interval,
            "cadence_minutes": round(float(cadence_minutes), 3),
            "adaptive_max_age_minutes": adaptive_overrides[station],
        }

    # Bootstrap fallback for stations without interval history yet. Most METAR
    # stations publish around hourly; this avoids false stale suppression while
    # cadence samples warm up after a restart/migration.
    for station in sorted(candidate_stations):
        payload = latest_by_station.get(station)
        if not station or station in adaptive_overrides or not isinstance(payload, dict):
            continue
        report_type = _normalize_text(payload.get("report_type")).upper() or "METAR"
        if report_type not in {"METAR", "SPECI"}:
            continue
        obs_age = _metar_observation_age_minutes(
            observation_time_utc=payload.get("observation_time_utc"),
            now=now_utc,
        )
        if obs_age is None or obs_age > 95.0:
            continue
        # Dynamic bootstrap headroom tracks the currently observed age with a
        # tight bound so hourly stations do not get repeatedly blocked at ~60m
        # before interval cadence samples are available.
        if report_type == "SPECI":
            bootstrap_floor = 45.0
            bootstrap_cap = 65.0
        else:
            bootstrap_floor = 60.0
            bootstrap_cap = 75.0
        bootstrap_limit = min(
            bootstrap_cap,
            max(base_limit, bootstrap_floor, float(obs_age) + 6.0),
        )
        if bootstrap_limit <= base_limit + 0.5:
            continue
        adaptive_overrides[station] = round(float(bootstrap_limit), 3)
        diagnostics[station] = {
            "mode": "bootstrap_hourly_fallback",
            "sample_count": 0,
            "latest_interval_minutes": None,
            "interval_median_minutes": None,
            "interval_p90_minutes": None,
            "cadence_minutes": None,
            "metar_observation_age_minutes": round(float(obs_age), 3),
            "bootstrap_floor_minutes": bootstrap_floor,
            "bootstrap_cap_minutes": bootstrap_cap,
            "adaptive_max_age_minutes": adaptive_overrides[station],
        }

    return adaptive_overrides, diagnostics


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _find_latest_csv(output_dir: str, pattern: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return ""
    return str(candidates[-1])


def _find_latest_json(output_dir: str, pattern: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return ""
    return str(candidates[-1])


def _build_spec_hash(spec_row: dict[str, Any]) -> str:
    digest_payload = {
        "market_ticker": _normalize_text(spec_row.get("market_ticker")),
        "rules_primary": _normalize_text(spec_row.get("rules_primary")),
        "rules_secondary": _normalize_text(spec_row.get("rules_secondary")),
        "settlement_station": _normalize_text(spec_row.get("settlement_station")),
        "settlement_timezone": _normalize_text(spec_row.get("settlement_timezone")),
        "local_day_boundary": _normalize_text(spec_row.get("local_day_boundary")),
        "observation_window_local_start": _normalize_text(spec_row.get("observation_window_local_start")),
        "observation_window_local_end": _normalize_text(spec_row.get("observation_window_local_end")),
        "threshold_expression": _normalize_text(spec_row.get("threshold_expression")),
        "contract_terms_url": _normalize_text(spec_row.get("contract_terms_url")),
    }
    encoded = json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_underlying_key(
    *,
    series_ticker: Any,
    settlement_station: Any,
    target_date_local: Any,
) -> str:
    return "|".join(
        (
            _normalize_text(series_ticker) or "series_unknown",
            _normalize_text(settlement_station) or "station_unknown",
            _normalize_text(target_date_local) or "date_unknown",
        )
    )


def _resolve_specs_csv(
    *,
    explicit_specs_csv: str | None,
    constraint_rows: list[dict[str, str]],
    output_dir: str,
) -> str:
    if _normalize_text(explicit_specs_csv):
        return _normalize_text(explicit_specs_csv)
    for row in constraint_rows:
        source_specs_csv = _normalize_text(row.get("source_specs_csv"))
        if source_specs_csv:
            return source_specs_csv
    return _find_latest_csv(output_dir, "kalshi_temperature_contract_specs_*.csv")


def _build_specs_by_ticker(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        _normalize_text(row.get("market_ticker")): row
        for row in rows
        if _normalize_text(row.get("market_ticker"))
    }


def _ticker_underlying_keys(specs_by_ticker: dict[str, dict[str, str]]) -> dict[str, str]:
    underlying_by_ticker: dict[str, str] = {}
    for ticker, row in specs_by_ticker.items():
        normalized_ticker = _normalize_text(ticker)
        if not normalized_ticker:
            continue
        underlying_by_ticker[normalized_ticker] = _build_underlying_key(
            series_ticker=row.get("series_ticker"),
            settlement_station=row.get("settlement_station"),
            target_date_local=row.get("target_date_local"),
        )
    return underlying_by_ticker


def _load_existing_underlying_netting_snapshot(
    *,
    output_dir: str,
    book_db_path: str | None,
    specs_by_ticker: dict[str, dict[str, str]],
    contracts_per_order: int,
) -> dict[str, Any]:
    resolved_book_path = Path(_normalize_text(book_db_path)) if _normalize_text(book_db_path) else default_book_db_path(output_dir)
    snapshot: dict[str, Any] = {
        "book_db_path": str(resolved_book_path),
        "loaded": False,
        "error": "",
        "position_rows": 0,
        "open_order_rows": 0,
        "unknown_ticker_rows": 0,
        "underlying_slots": {},
        "underlying_position_abs_contracts": {},
        "underlying_open_order_slots": {},
        "underlying_exposure_abs_dollars": {},
    }
    if not resolved_book_path.exists():
        return snapshot

    ticker_to_underlying = _ticker_underlying_keys(specs_by_ticker)
    if not ticker_to_underlying:
        snapshot["loaded"] = True
        return snapshot

    safe_contracts_per_order = max(1, int(contracts_per_order))
    underlying_slots: dict[str, int] = {}
    underlying_position_abs_contracts: dict[str, float] = {}
    underlying_open_order_slots: dict[str, int] = {}
    underlying_exposure_abs_dollars: dict[str, float] = {}

    try:
        connection = sqlite3.connect(str(resolved_book_path))
        connection.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        snapshot["error"] = f"book_connect_failed:{exc}"
        return snapshot

    try:
        try:
            position_rows = connection.execute(
                """
                SELECT ticker, COALESCE(position_fp, 0.0) AS position_fp, COALESCE(market_exposure_dollars, 0.0) AS market_exposure_dollars
                FROM positions
                WHERE ABS(COALESCE(position_fp, 0.0)) > 1e-9
                   OR ABS(COALESCE(market_exposure_dollars, 0.0)) > 1e-9
                """
            ).fetchall()
        except sqlite3.Error:
            position_rows = []
        snapshot["position_rows"] = len(position_rows)
        for row in position_rows:
            ticker = _normalize_text(row["ticker"])
            underlying_key = ticker_to_underlying.get(ticker)
            if not underlying_key:
                snapshot["unknown_ticker_rows"] = int(snapshot.get("unknown_ticker_rows") or 0) + 1
                continue
            position_abs = abs(float(row["position_fp"] or 0.0))
            exposure_abs = abs(float(row["market_exposure_dollars"] or 0.0))
            slots = 0
            if position_abs > 1e-9:
                slots = max(1, int(math.ceil(position_abs / float(safe_contracts_per_order))))
            elif exposure_abs > 1e-9:
                slots = 1
            if slots > 0:
                underlying_slots[underlying_key] = underlying_slots.get(underlying_key, 0) + slots
            if position_abs > 1e-9:
                underlying_position_abs_contracts[underlying_key] = round(
                    underlying_position_abs_contracts.get(underlying_key, 0.0) + position_abs,
                    6,
                )
            if exposure_abs > 1e-9:
                underlying_exposure_abs_dollars[underlying_key] = round(
                    underlying_exposure_abs_dollars.get(underlying_key, 0.0) + exposure_abs,
                    6,
                )

        try:
            open_order_rows = connection.execute(
                """
                SELECT ticker, COUNT(*) AS open_count
                FROM orders
                WHERE LOWER(COALESCE(status, '')) IN ('resting', 'open', 'pending')
                GROUP BY ticker
                """
            ).fetchall()
        except sqlite3.Error:
            open_order_rows = []
        snapshot["open_order_rows"] = len(open_order_rows)
        for row in open_order_rows:
            ticker = _normalize_text(row["ticker"])
            underlying_key = ticker_to_underlying.get(ticker)
            if not underlying_key:
                snapshot["unknown_ticker_rows"] = int(snapshot.get("unknown_ticker_rows") or 0) + 1
                continue
            open_count = max(0, int(row["open_count"] or 0))
            if open_count <= 0:
                continue
            underlying_slots[underlying_key] = underlying_slots.get(underlying_key, 0) + open_count
            underlying_open_order_slots[underlying_key] = underlying_open_order_slots.get(underlying_key, 0) + open_count
    finally:
        connection.close()

    snapshot["loaded"] = True
    snapshot["underlying_slots"] = dict(sorted(underlying_slots.items()))
    snapshot["underlying_position_abs_contracts"] = dict(sorted(underlying_position_abs_contracts.items()))
    snapshot["underlying_open_order_slots"] = dict(sorted(underlying_open_order_slots.items()))
    snapshot["underlying_exposure_abs_dollars"] = dict(sorted(underlying_exposure_abs_dollars.items()))
    snapshot["underlying_count"] = len(underlying_slots)
    return snapshot


def _load_market_sequences(
    *,
    ws_state_json: str | None,
    output_dir: str,
) -> tuple[str, dict[str, int | None], dict[str, Any]]:
    ws_path = Path(ws_state_json) if _normalize_text(ws_state_json) else default_ws_state_path(output_dir)
    payload = _read_json(ws_path)
    market_sequences: dict[str, int | None] = {}
    markets = payload.get("markets")
    if isinstance(markets, dict):
        for market_ticker, market_payload in markets.items():
            if not isinstance(market_payload, dict):
                continue
            market_sequences[str(market_ticker)] = _parse_int(market_payload.get("sequence"))
    return str(ws_path), market_sequences, payload


def _load_metar_context(
    *,
    output_dir: str,
    metar_summary_json: str | None,
    metar_state_json: str | None,
) -> dict[str, Any]:
    summary_path = Path(_normalize_text(metar_summary_json)) if _normalize_text(metar_summary_json) else None
    if summary_path is None:
        latest_summary = _find_latest_csv(output_dir, "kalshi_temperature_metar_summary_*.json")
        if latest_summary:
            summary_path = Path(latest_summary)

    summary_payload: dict[str, Any] = {}
    if summary_path is not None and summary_path.exists():
        summary_payload = _read_json(summary_path)

    state_path_text = _normalize_text(metar_state_json)
    if not state_path_text:
        state_path_text = _normalize_text(summary_payload.get("state_file"))
    if not state_path_text:
        state_path_text = str(Path(output_dir) / "kalshi_temperature_metar_state.json")
    state_path = Path(state_path_text)
    state_payload = _read_json(state_path)
    latest_by_station = state_payload.get("latest_observation_by_station")
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}
    station_interval_stats = state_payload.get("station_observation_interval_stats")
    if not isinstance(station_interval_stats, dict):
        station_interval_stats = {}

    return {
        "summary_path": str(summary_path) if summary_path is not None else "",
        "summary_payload": summary_payload,
        "state_path": str(state_path),
        "state_payload": state_payload,
        "raw_sha256": _normalize_text(summary_payload.get("raw_sha256")),
        "captured_at": _normalize_text(summary_payload.get("captured_at")),
        "latest_by_station": latest_by_station,
        "station_interval_stats": station_interval_stats,
    }


def _hours_to_close(*, close_time: Any, now: datetime) -> float | None:
    close_ts = _parse_ts(close_time)
    if close_ts is None:
        return None
    return round((close_ts - now).total_seconds() / 3600.0, 6)


def _metar_observation_age_minutes(*, observation_time_utc: Any, now: datetime) -> float | None:
    observation_ts = _parse_ts(observation_time_utc)
    if observation_ts is None:
        return None
    return round(max(0.0, (now - observation_ts).total_seconds()) / 60.0, 6)


def _side_from_constraint(constraint_status: str) -> str:
    status = _normalize_text(constraint_status).lower()
    if status == "yes_impossible":
        return "no"
    if status.startswith("no_"):
        return "no"
    if status.startswith("yes_"):
        return "yes"
    return "yes"


def _intent_alpha_strength(intent: TemperatureTradeIntent) -> float:
    def _taf_path_signal_components() -> tuple[float, str]:
        forecast_status = _normalize_text(intent.forecast_model_status).lower()
        taf_status = _normalize_text(intent.taf_status).lower()
        volatility = float(intent.taf_volatility_score) if isinstance(intent.taf_volatility_score, (int, float)) else 0.0
        forecast_range = float(intent.forecast_range_width) if isinstance(intent.forecast_range_width, (int, float)) else None

        score = 0.0
        regime = "unmodeled"
        if forecast_status != "ready":
            return (-0.2, "forecast_unavailable")
        if taf_status == "ready":
            score += 0.18
            regime = "taf_ready"
        elif taf_status in {"partial", "degraded"}:
            score += 0.05
            regime = "taf_partial"
        elif taf_status == "missing_station":
            score -= 0.10
            regime = "taf_missing_station"
        else:
            score -= 0.04
            regime = "taf_unavailable"

        if isinstance(forecast_range, float):
            # Narrow forecast spread generally means higher path confidence.
            score += max(-0.1, min(0.22, (8.0 - max(0.0, forecast_range)) * 0.03))
        if math.isfinite(volatility):
            score -= min(0.24, max(0.0, volatility) * 0.08)
        return (round(score, 6), regime)

    strength = 0.0
    status = _normalize_text(intent.constraint_status).lower()
    if status in {"yes_impossible", "no_interval_infeasible", "no_monotonic_chain"}:
        strength += 0.5
    elif status in {"yes_likely_locked", "yes_interval_certain", "yes_monotonic_chain"}:
        strength += 0.3
    if isinstance(intent.primary_signal_margin, (int, float)):
        strength += abs(float(intent.primary_signal_margin))
    if isinstance(intent.forecast_feasibility_margin, (int, float)):
        strength += 0.6 * abs(float(intent.forecast_feasibility_margin))
    taf_signal_score, _ = _taf_path_signal_components()
    strength += taf_signal_score
    if isinstance(intent.yes_possible_gap, (int, float)):
        strength -= 0.8 * float(intent.yes_possible_gap)
    if intent.speci_recent:
        strength += 0.25
    if intent.speci_shock_active:
        strength += 0.2
    if isinstance(intent.speci_shock_confidence, (int, float)):
        strength += 0.35 * float(intent.speci_shock_confidence)
    if isinstance(intent.speci_shock_weight, (int, float)):
        strength += 0.45 * float(intent.speci_shock_weight)
    if intent.speci_shock_improvement_hold_active:
        strength -= 0.2
    if intent.speci_shock_cooldown_blocked:
        strength -= 0.15
    if isinstance(intent.cross_market_family_score, (int, float)):
        strength += 0.2 * max(-2.0, min(2.0, float(intent.cross_market_family_score)))
    if isinstance(intent.cross_market_family_zscore, (int, float)):
        strength += 0.18 * max(-2.0, min(2.0, float(intent.cross_market_family_zscore)))
    signal = _normalize_text(intent.cross_market_family_signal).lower()
    if signal == "relative_outlier_high":
        strength += 0.25
    elif signal == "relative_outlier_low":
        strength -= 0.15
    if isinstance(intent.consensus_alpha_score, (int, float)):
        strength += 0.35 * max(-2.5, min(2.5, float(intent.consensus_alpha_score)))
    if isinstance(intent.consensus_profile_support_ratio, (int, float)):
        strength += 0.45 * max(0.0, min(1.0, float(intent.consensus_profile_support_ratio)))
    if isinstance(intent.consensus_profile_support_count, int) and intent.consensus_profile_support_count > 0:
        strength += min(0.35, float(intent.consensus_profile_support_count) * 0.07)
    if intent.range_family_consistency_conflict:
        strength -= 0.35
    return round(strength, 6)


def _estimate_temperature_edge_profile(
    *,
    intent: TemperatureTradeIntent,
    alpha_strength: float,
    probability_confidence: float | None = None,
) -> dict[str, float]:
    side_win_probability = (
        float(probability_confidence)
        if isinstance(probability_confidence, (int, float))
        else float(intent.settlement_confidence_score)
    )
    side_win_probability = max(0.0, min(0.999, side_win_probability))
    entry_price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))

    # Binary contract EV per contract for chosen side: EV = P(win) - price.
    # This anchors edge to payout asymmetry so high-price entries require high confidence.
    base_edge = side_win_probability - entry_price

    # Keep alpha/urgency/signal terms as bounded secondary adjustments around
    # the price-implied EV anchor (do not dominate payout math).
    alpha_bonus = max(-0.01, min(0.02, alpha_strength * 0.0025))
    confidence_bonus = max(-0.008, min(0.012, (side_win_probability - 0.5) * 0.02))
    urgency_bonus = 0.0
    if isinstance(intent.hours_to_close, (int, float)):
        hours_to_close = float(intent.hours_to_close)
        if hours_to_close <= 2.0:
            urgency_bonus = 0.004
        elif hours_to_close <= 6.0:
            urgency_bonus = 0.003
        elif hours_to_close <= 12.0:
            urgency_bonus = 0.0015

    speci_bonus = 0.0
    if intent.speci_shock_active:
        speci_bonus += 0.003
    if isinstance(intent.speci_shock_confidence, (int, float)):
        speci_bonus += min(0.003, max(0.0, float(intent.speci_shock_confidence)) * 0.004)
    if isinstance(intent.speci_shock_weight, (int, float)):
        speci_bonus += min(0.003, max(0.0, float(intent.speci_shock_weight)) * 0.0045)

    cross_market_bonus = 0.0
    if isinstance(intent.cross_market_family_zscore, (int, float)):
        zscore = float(intent.cross_market_family_zscore)
        if zscore > 0.0:
            cross_market_bonus += min(0.006, zscore * 0.002)
    if _normalize_text(intent.cross_market_family_signal).lower() == "relative_outlier_high":
        cross_market_bonus += 0.0025
    consensus_bonus = 0.0
    if isinstance(intent.consensus_alpha_score, (int, float)):
        consensus_bonus += min(0.009, max(0.0, float(intent.consensus_alpha_score)) * 0.0045)
    if isinstance(intent.consensus_weighted_support_ratio, (int, float)):
        consensus_bonus += min(0.0075, max(0.0, float(intent.consensus_weighted_support_ratio)) * 0.01)
    if isinstance(intent.consensus_profile_support_count, int):
        consensus_bonus += min(0.004, max(0.0, float(intent.consensus_profile_support_count)) * 0.001)

    taf_bonus = 0.0
    forecast_status = _normalize_text(intent.forecast_model_status).lower()
    taf_status = _normalize_text(intent.taf_status).lower()
    if forecast_status == "ready":
        taf_bonus += 0.001
    if taf_status == "ready":
        taf_bonus += 0.002
    elif taf_status in {"partial", "degraded"}:
        taf_bonus += 0.0005
    elif taf_status == "missing_station":
        taf_bonus -= 0.0015
    if isinstance(intent.taf_volatility_score, (int, float)):
        taf_bonus -= min(0.004, max(0.0, float(intent.taf_volatility_score)) * 0.002)
    if isinstance(intent.forecast_range_width, (int, float)):
        taf_bonus += max(-0.0025, min(0.0035, (6.0 - max(0.0, float(intent.forecast_range_width))) * 0.0008))

    gap_penalty = 0.0
    if isinstance(intent.yes_possible_gap, (int, float)):
        gap_penalty += min(0.04, max(0.0, float(intent.yes_possible_gap)) * 0.012)
    if intent.speci_shock_cooldown_blocked:
        gap_penalty += 0.006
    if intent.speci_shock_improvement_hold_active:
        gap_penalty += 0.005

    # Conservative friction to reduce overestimation under maker queue latency.
    friction_penalty = 0.005

    edge_net = max(
        -0.2,
        min(
            0.2,
            base_edge
            + alpha_bonus
            + confidence_bonus
            + urgency_bonus
            + speci_bonus
            + cross_market_bonus
            + consensus_bonus
            + taf_bonus
            - gap_penalty
            - friction_penalty,
        ),
    )
    upside_per_contract = max(0.0, 1.0 - entry_price)
    downside_per_contract = max(0.0, entry_price)
    risk_reward_ratio = (
        upside_per_contract / downside_per_contract if downside_per_contract > 0.0 else 0.0
    )
    return {
        "base_edge": round(base_edge, 6),
        "alpha_bonus": round(alpha_bonus, 6),
        "confidence_bonus": round(confidence_bonus, 6),
        "urgency_bonus": round(urgency_bonus, 6),
        "speci_bonus": round(speci_bonus, 6),
        "cross_market_bonus": round(cross_market_bonus, 6),
        "consensus_bonus": round(consensus_bonus, 6),
        "taf_bonus": round(taf_bonus, 6),
        "gap_penalty": round(gap_penalty, 6),
        "friction_penalty": round(friction_penalty, 6),
        "entry_price": round(entry_price, 6),
        "side_win_probability": round(side_win_probability, 6),
        "upside_per_contract": round(upside_per_contract, 6),
        "downside_per_contract": round(downside_per_contract, 6),
        "risk_reward_ratio": round(risk_reward_ratio, 6),
        "edge_net": round(edge_net, 6),
    }


def _estimate_temperature_probability_confidence(
    *,
    intent: TemperatureTradeIntent,
    alpha_strength: float,
) -> tuple[float, dict[str, float]]:
    status = _normalize_text(intent.constraint_status).lower()
    # Start from settlement confidence and add only bounded signal adjustments.
    base_confidence = max(0.0, min(1.0, float(intent.settlement_confidence_score)))
    status_prior = {
        "yes_impossible": 0.08,
        "no_interval_infeasible": 0.07,
        "no_monotonic_chain": 0.06,
        "yes_likely_locked": 0.04,
        "yes_interval_certain": 0.035,
        "yes_monotonic_chain": 0.03,
    }.get(status, 0.0)
    alpha_term = max(-0.08, min(0.08, float(alpha_strength) * 0.015))
    speci_term = 0.0
    if intent.speci_shock_active:
        speci_term += 0.03
    if isinstance(intent.speci_shock_confidence, (int, float)):
        speci_term += min(0.03, max(0.0, float(intent.speci_shock_confidence)) * 0.04)
    if isinstance(intent.speci_shock_weight, (int, float)):
        speci_term += min(0.025, max(0.0, float(intent.speci_shock_weight)) * 0.03)

    cross_market_term = 0.0
    if isinstance(intent.cross_market_family_zscore, (int, float)):
        cross_market_term += max(-0.03, min(0.03, float(intent.cross_market_family_zscore) * 0.008))
    if _normalize_text(intent.cross_market_family_signal).lower() == "relative_outlier_high":
        cross_market_term += 0.015

    consensus_term = 0.0
    if isinstance(intent.consensus_alpha_score, (int, float)):
        consensus_term += max(-0.04, min(0.04, float(intent.consensus_alpha_score) * 0.02))
    if isinstance(intent.consensus_weighted_support_ratio, (int, float)):
        consensus_term += min(0.04, max(0.0, float(intent.consensus_weighted_support_ratio)) * 0.05)
    if isinstance(intent.consensus_profile_support_count, int):
        consensus_term += min(0.02, max(0.0, float(intent.consensus_profile_support_count)) * 0.005)

    taf_term = 0.0
    taf_status = _normalize_text(intent.taf_status).lower()
    forecast_status = _normalize_text(intent.forecast_model_status).lower()
    if taf_status == "ready":
        taf_term += 0.015
    elif taf_status in {"partial", "degraded"}:
        taf_term += 0.004
    elif taf_status == "missing_station":
        taf_term -= 0.02
    elif forecast_status != "ready":
        taf_term -= 0.015
    if isinstance(intent.taf_volatility_score, (int, float)):
        taf_term -= min(0.03, max(0.0, float(intent.taf_volatility_score)) * 0.01)

    gap_penalty = 0.0
    if isinstance(intent.yes_possible_gap, (int, float)):
        gap_penalty += min(0.08, max(0.0, float(intent.yes_possible_gap)) * 0.025)
    if intent.speci_shock_cooldown_blocked:
        gap_penalty += 0.02
    if intent.speci_shock_improvement_hold_active:
        gap_penalty += 0.015

    raw_confidence = (
        base_confidence
        + status_prior
        + alpha_term
        + speci_term
        + cross_market_term
        + consensus_term
        + taf_term
        - gap_penalty
    )

    # Evidence-weighted shrinkage: keep confidence anchored near settlement
    # confidence when corroborating signals are weak/noisy.
    evidence_support_score = 0.0
    if intent.speci_shock_active or isinstance(intent.speci_shock_confidence, (int, float)):
        evidence_support_score += 0.25
    if (
        isinstance(intent.consensus_weighted_support_ratio, (int, float))
        and float(intent.consensus_weighted_support_ratio) >= 0.35
    ) or (
        isinstance(intent.consensus_profile_support_count, int)
        and int(intent.consensus_profile_support_count) >= 2
    ):
        evidence_support_score += 0.25
    if _normalize_text(intent.taf_status).lower() == "ready":
        evidence_support_score += 0.25
    if (
        isinstance(intent.cross_market_family_score, (int, float))
        or isinstance(intent.cross_market_family_zscore, (int, float))
        or _normalize_text(intent.cross_market_family_signal)
    ):
        evidence_support_score += 0.25
    evidence_support_score = max(0.0, min(1.0, evidence_support_score))

    volatility_penalty = 0.0
    if isinstance(intent.taf_volatility_score, (int, float)):
        volatility_penalty += min(0.20, max(0.0, float(intent.taf_volatility_score)) * 0.08)
    if isinstance(intent.yes_possible_gap, (int, float)):
        volatility_penalty += min(0.12, max(0.0, float(intent.yes_possible_gap)) * 0.04)
    if intent.speci_shock_cooldown_blocked:
        volatility_penalty += 0.04
    if intent.speci_shock_improvement_hold_active:
        volatility_penalty += 0.03
    volatility_penalty = max(0.0, min(0.35, volatility_penalty))

    evidence_blend = max(
        0.25,
        min(
            0.95,
            0.35 + 0.45 * evidence_support_score + 0.20 * base_confidence - volatility_penalty,
        ),
    )
    shrunk_confidence = base_confidence + (raw_confidence - base_confidence) * evidence_blend
    bounded_confidence = max(0.01, min(0.999, shrunk_confidence))
    # Keep confidence uplift bounded when corroborating evidence is sparse.
    uplift_cap = 0.03 + (0.06 * evidence_support_score)
    uplift_cap -= min(0.03, volatility_penalty * 0.15)
    uplift_cap = max(0.01, min(0.12, uplift_cap))
    max_allowed_confidence = min(0.995, base_confidence + uplift_cap)
    bounded_confidence = min(float(bounded_confidence), float(max_allowed_confidence))
    breakdown = {
        "base_confidence": round(base_confidence, 6),
        "status_prior": round(status_prior, 6),
        "alpha_term": round(alpha_term, 6),
        "speci_term": round(speci_term, 6),
        "cross_market_term": round(cross_market_term, 6),
        "consensus_term": round(consensus_term, 6),
        "taf_term": round(taf_term, 6),
        "gap_penalty": round(gap_penalty, 6),
        "confidence_raw": round(raw_confidence, 6),
        "evidence_support_score": round(evidence_support_score, 6),
        "volatility_penalty": round(volatility_penalty, 6),
        "evidence_blend": round(evidence_blend, 6),
        "confidence_shrunk": round(shrunk_confidence, 6),
        "uplift_cap": round(uplift_cap, 6),
        "max_allowed_confidence": round(max_allowed_confidence, 6),
        "confidence_bounded": round(bounded_confidence, 6),
    }
    return round(bounded_confidence, 6), breakdown


def _find_latest_settlement_state_json(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob("kalshi_temperature_settlement_state_*.json"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _derive_settlement_allow_new_orders(*, state: str, finalization_status: str, allow_new_orders: Any) -> bool:
    if isinstance(allow_new_orders, bool):
        return allow_new_orders
    state_text = _normalize_text(state).lower()
    finalization_text = _normalize_text(finalization_status).lower()
    if state_text.startswith("pending_final") or finalization_text.startswith("pending_final"):
        return False
    if "post_close_unfinalized" in state_text or "post_close_unfinalized" in finalization_text:
        return False
    if "target_date_elapsed_waiting_finalization" in state_text or "target_date_elapsed_waiting_finalization" in finalization_text:
        return False
    if "review" in state_text or "review" in finalization_text:
        return False
    if state_text in {"final", "final_locked", "settled", "closed_final"}:
        return False
    if finalization_text in {"final", "final_locked", "settled"}:
        return False
    return True


def _load_settlement_state_by_underlying(
    *,
    output_dir: str,
    settlement_state_json: str | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    resolved_path = _normalize_text(settlement_state_json)
    if not resolved_path:
        resolved_path = _find_latest_settlement_state_json(output_dir)
    meta: dict[str, Any] = {
        "settlement_state_json_used": resolved_path,
        "loaded": False,
        "entry_count": 0,
        "error": "",
    }
    if not resolved_path:
        return {}, meta
    payload = _read_json(Path(resolved_path))
    if not payload:
        meta["error"] = "settlement_state_json_unreadable"
        return {}, meta

    raw_underlyings: dict[str, Any] = {}
    for key in ("underlyings", "state_by_underlying", "underlying_states"):
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            raw_underlyings = candidate
            break
    if not raw_underlyings and all(isinstance(key, str) for key in payload.keys()):
        raw_underlyings = payload

    normalized: dict[str, dict[str, Any]] = {}
    for underlying_key, entry in raw_underlyings.items():
        if not isinstance(entry, dict):
            continue
        key = _normalize_text(underlying_key)
        if not key:
            continue
        state = _normalize_text(entry.get("state") or entry.get("finalization_status"))
        finalization_status = _normalize_text(entry.get("finalization_status") or state)
        allow_new_orders = _derive_settlement_allow_new_orders(
            state=state,
            finalization_status=finalization_status,
            allow_new_orders=entry.get("allow_new_orders"),
        )
        normalized[key] = {
            "state": state,
            "finalization_status": finalization_status,
            "allow_new_orders": allow_new_orders,
            "reason": _normalize_text(entry.get("reason")),
            "review_flag": bool(entry.get("review_flag")),
            "updated_at": _normalize_text(entry.get("updated_at")),
            "final_truth_value": entry.get("final_truth_value"),
            "fast_truth_value": entry.get("fast_truth_value"),
            "revision_id": _normalize_text(entry.get("revision_id")),
            "source": _normalize_text(entry.get("source")),
        }

    meta["loaded"] = True
    meta["entry_count"] = len(normalized)
    return normalized, meta


def _settlement_block_reason(entry: dict[str, Any] | None) -> str | None:
    if not isinstance(entry, dict):
        return None
    allow_new_orders = entry.get("allow_new_orders")
    if not isinstance(allow_new_orders, bool):
        allow_new_orders = _derive_settlement_allow_new_orders(
            state=_normalize_text(entry.get("state")),
            finalization_status=_normalize_text(entry.get("finalization_status")),
            allow_new_orders=entry.get("allow_new_orders"),
        )
    if bool(allow_new_orders):
        return None
    state = _normalize_text(entry.get("state") or entry.get("finalization_status")).lower()
    if "review" in state:
        return "settlement_review_hold"
    if state in {"final", "final_locked", "settled", "closed_final"}:
        return "settlement_final_locked"
    return "settlement_finalization_blocked"


def _build_settlement_prefilter_decision(
    *,
    intent: TemperatureTradeIntent,
    settlement_reason: str,
) -> TemperaturePolicyDecision:
    resolved_reason = _normalize_text(settlement_reason) or "settlement_finalization_blocked"
    return TemperaturePolicyDecision(
        intent_id=intent.intent_id,
        approved=False,
        decision_reason=resolved_reason,
        decision_notes=f"{resolved_reason},prefiltered_settlement_horizon=true",
    )


def _prefilter_settlement_blocked_intents(
    *,
    intents: list[TemperatureTradeIntent],
    settlement_state_by_underlying: dict[str, dict[str, Any]] | None,
) -> tuple[list[TemperatureTradeIntent], list[TemperaturePolicyDecision], dict[str, int]]:
    if not intents:
        return [], [], {}
    if not isinstance(settlement_state_by_underlying, dict) or not settlement_state_by_underlying:
        return list(intents), [], {}

    prefilter_reason_counts: Counter[str] = Counter()
    gate_candidate_intents: list[TemperatureTradeIntent] = []
    prefilter_decisions: list[TemperaturePolicyDecision] = []
    for intent in intents:
        settlement_entry = settlement_state_by_underlying.get(intent.underlying_key)
        settlement_reason = _settlement_block_reason(settlement_entry)
        if not settlement_reason:
            gate_candidate_intents.append(intent)
            continue
        prefilter_reason = _normalize_text(settlement_reason) or "settlement_finalization_blocked"
        prefilter_reason_counts[prefilter_reason] += 1
        prefilter_decisions.append(
            _build_settlement_prefilter_decision(
                intent=intent,
                settlement_reason=prefilter_reason,
            )
        )
    return (
        gate_candidate_intents,
        prefilter_decisions,
        dict(sorted(prefilter_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
    )


def _build_intent_id(
    *,
    market_ticker: str,
    constraint_status: str,
    spec_hash: str,
    metar_snapshot_sha: str,
    market_snapshot_seq: int | None,
    policy_version: str,
) -> str:
    raw = "|".join(
        (
            market_ticker,
            constraint_status,
            spec_hash,
            metar_snapshot_sha,
            str(market_snapshot_seq if market_snapshot_seq is not None else ""),
            policy_version,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _directional_threshold_value(row: dict[str, Any]) -> tuple[str, float | None]:
    kind = _normalize_text(row.get("threshold_kind")).lower()
    lower = _parse_float(row.get("threshold_lower_bound"))
    upper = _parse_float(row.get("threshold_upper_bound"))
    if kind in _LOWER_BOUNDED_KINDS:
        return kind, lower
    if kind in _UPPER_BOUNDED_KINDS:
        return kind, upper
    return kind, None


def _family_key(row: dict[str, Any]) -> str:
    metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
    return "|".join(
        (
            _normalize_text(row.get("series_ticker")),
            _normalize_text(row.get("settlement_station")),
            _normalize_text(row.get("target_date_local")),
            metric,
        )
    )


def _derive_breadth_constraint_rows(
    *,
    constraint_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    # Keep raw rows untouched and append deterministic derived rows that
    # encode family-level interval/monotonic signals.
    expanded_rows: list[dict[str, Any]] = [dict(row) for row in constraint_rows]
    seen_keys: set[tuple[str, str]] = {
        (_normalize_text(row.get("market_ticker")), _normalize_text(row.get("constraint_status")).lower())
        for row in expanded_rows
        if _normalize_text(row.get("market_ticker"))
    }

    def append_derived(
        *,
        base_row: dict[str, Any],
        status: str,
        reason: str,
        yes_possible_overlap: bool | None = None,
        yes_possible_gap: float | None = None,
        primary_signal_margin: float | None = None,
    ) -> None:
        market_ticker = _normalize_text(base_row.get("market_ticker"))
        dedupe_key = (market_ticker, status)
        if not market_ticker or dedupe_key in seen_keys:
            return
        derived = dict(base_row)
        derived["constraint_status"] = status
        derived["constraint_reason"] = reason
        if isinstance(yes_possible_overlap, bool):
            derived["yes_possible_overlap"] = "1" if yes_possible_overlap else "0"
        if isinstance(yes_possible_gap, (int, float)):
            derived["yes_possible_gap"] = round(float(max(0.0, yes_possible_gap)), 6)
        if isinstance(primary_signal_margin, (int, float)):
            derived["primary_signal_margin"] = round(float(primary_signal_margin), 6)
        expanded_rows.append(derived)
        seen_keys.add(dedupe_key)

    # Pass 1: interval-driven derived signals for no-signal rows.
    for row in list(expanded_rows):
        status = _normalize_text(row.get("constraint_status")).lower()
        if status != "no_signal":
            continue
        snapshot_status = _normalize_text(row.get("snapshot_status")).lower()
        if snapshot_status and snapshot_status != "ready":
            continue

        yes_overlap = _parse_boolish(row.get("yes_possible_overlap"))
        yes_gap = _parse_float(row.get("yes_possible_gap")) or 0.0
        possible_low = _parse_float(row.get("possible_final_lower_bound"))
        possible_high = _parse_float(row.get("possible_final_upper_bound"))
        yes_low = _parse_float(row.get("yes_interval_lower_bound"))
        yes_high = _parse_float(row.get("yes_interval_upper_bound"))
        forecast_feasibility_margin = _parse_float(row.get("forecast_feasibility_margin"))

        if yes_overlap is False and yes_gap >= 0.5:
            append_derived(
                base_row=row,
                status="no_interval_infeasible",
                reason=f"Derived from interval gap: yes interval infeasible (gap={yes_gap:.3f}).",
                yes_possible_overlap=False,
                yes_possible_gap=yes_gap,
                primary_signal_margin=abs(yes_gap),
            )
            continue

        if (
            yes_overlap is True
            and isinstance(possible_low, float)
            and isinstance(possible_high, float)
            and isinstance(yes_low, float)
            and isinstance(yes_high, float)
            and yes_low <= possible_low <= possible_high <= yes_high
        ):
            width = max(0.0, possible_high - possible_low)
            # Keep interval-certain conservative: if forecast feasibility is
            # already negative, do not promote containment into certainty.
            coherent_forecast = (
                not isinstance(forecast_feasibility_margin, float)
                or float(forecast_feasibility_margin) >= -1e-6
            )
            if width <= 4.0 and coherent_forecast:
                append_derived(
                    base_row=row,
                    status="yes_interval_certain",
                    reason="Derived from interval containment: possible final interval fully inside YES interval.",
                    yes_possible_overlap=True,
                    yes_possible_gap=0.0,
                    primary_signal_margin=abs(yes_high - possible_high) + abs(possible_low - yes_low),
                )

    # Pass 2: monotonic chain propagation on directional thresholds.
    grouped_directional: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in list(expanded_rows):
        kind, threshold_value = _directional_threshold_value(row)
        if not isinstance(threshold_value, float):
            continue
        family_key = _family_key(row)
        bucket = "lower" if kind in _LOWER_BOUNDED_KINDS else ("upper" if kind in _UPPER_BOUNDED_KINDS else "")
        if not bucket:
            continue
        grouped_directional.setdefault((family_key, bucket), []).append(row)

    for (_, bucket), rows in grouped_directional.items():
        impossible_thresholds: list[float] = []
        locked_thresholds: list[float] = []
        for row in rows:
            status = _normalize_text(row.get("constraint_status")).lower()
            _, threshold_value = _directional_threshold_value(row)
            if not isinstance(threshold_value, float):
                continue
            if status in {"yes_impossible", "no_interval_infeasible", "no_monotonic_chain"}:
                impossible_thresholds.append(float(threshold_value))
            if status in {"yes_likely_locked", "yes_interval_certain", "yes_monotonic_chain"}:
                locked_thresholds.append(float(threshold_value))

        impossible_anchor = None
        locked_anchor = None
        if impossible_thresholds:
            impossible_anchor = min(impossible_thresholds) if bucket == "lower" else max(impossible_thresholds)
        if locked_thresholds:
            locked_anchor = max(locked_thresholds) if bucket == "lower" else min(locked_thresholds)

        for row in rows:
            status = _normalize_text(row.get("constraint_status")).lower()
            if status != "no_signal":
                continue
            _, threshold_value = _directional_threshold_value(row)
            if not isinstance(threshold_value, float):
                continue

            if isinstance(impossible_anchor, float):
                impossible_condition = (
                    threshold_value >= impossible_anchor if bucket == "lower" else threshold_value <= impossible_anchor
                )
                if impossible_condition:
                    append_derived(
                        base_row=row,
                        status="no_monotonic_chain",
                        reason=(
                            f"Derived from monotonic chain: threshold {threshold_value:g} is downstream of impossible "
                            f"anchor {impossible_anchor:g}."
                        ),
                        yes_possible_overlap=False,
                        yes_possible_gap=max(0.25, abs(threshold_value - impossible_anchor)),
                        primary_signal_margin=abs(threshold_value - impossible_anchor),
                    )
                    continue

            if isinstance(locked_anchor, float):
                locked_condition = (
                    threshold_value <= locked_anchor if bucket == "lower" else threshold_value >= locked_anchor
                )
                if locked_condition:
                    append_derived(
                        base_row=row,
                        status="yes_monotonic_chain",
                        reason=(
                            f"Derived from monotonic chain: threshold {threshold_value:g} is implied by locked "
                            f"anchor {locked_anchor:g}."
                        ),
                        yes_possible_overlap=True,
                        yes_possible_gap=0.0,
                        primary_signal_margin=abs(threshold_value - locked_anchor),
                    )

    return expanded_rows


@dataclass(frozen=True)
class TemperatureTradeIntent:
    intent_id: str
    captured_at: str
    policy_version: str
    underlying_key: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    market_title: str
    settlement_station: str
    settlement_timezone: str
    target_date_local: str
    constraint_status: str
    constraint_reason: str
    side: str
    max_entry_price_dollars: float
    intended_contracts: int
    settlement_confidence_score: float
    observed_max_settlement_quantized: float | None
    close_time: str
    hours_to_close: float | None
    spec_hash: str
    metar_snapshot_sha: str
    metar_observation_time_utc: str
    metar_observation_age_minutes: float | None
    market_snapshot_seq: int | None
    temperature_metric: str = "daily_high"
    observed_metric_settlement_quantized: float | None = None
    forecast_upper_bound_settlement_raw: float | None = None
    forecast_lower_bound_settlement_raw: float | None = None
    threshold_kind: str = ""
    threshold_lower_bound: float | None = None
    threshold_upper_bound: float | None = None
    yes_possible_overlap: bool | None = None
    yes_possible_gap: float | None = None
    primary_signal_margin: float | None = None
    forecast_feasibility_margin: float | None = None
    forecast_model_status: str = ""
    taf_status: str = ""
    taf_volatility_score: float | None = None
    forecast_range_width: float | None = None
    observed_distance_to_lower_bound: float | None = None
    observed_distance_to_upper_bound: float | None = None
    cross_market_family_score: float | None = None
    cross_market_family_zscore: float | None = None
    cross_market_family_candidate_rank: int | None = None
    cross_market_family_bucket_size: int | None = None
    cross_market_family_signal: str = ""
    consensus_profile_support_count: int = 0
    consensus_profile_support_ratio: float | None = None
    consensus_weighted_support_score: float | None = None
    consensus_weighted_support_ratio: float | None = None
    consensus_alpha_score: float | None = None
    consensus_rank: int | None = None
    consensus_profile_names: str = ""
    range_family_consistency_conflict: bool = False
    range_family_consistency_conflict_scope: str = ""
    range_family_consistency_conflict_reason: str = ""
    speci_recent: bool = False
    speci_shock_active: bool = False
    speci_shock_confidence: float | None = None
    speci_shock_weight: float | None = None
    speci_shock_mode: str = ""
    speci_shock_trigger_count: int = 0
    speci_shock_trigger_families: str = ""
    speci_shock_persistence_ok: bool = False
    speci_shock_cooldown_blocked: bool = False
    speci_shock_improvement_hold_active: bool = False
    speci_shock_delta_temp_c: float | None = None
    speci_shock_delta_minutes: float | None = None
    speci_shock_decay_tau_minutes: float | None = None

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemperaturePolicyDecision:
    intent_id: str
    approved: bool
    decision_reason: str
    decision_notes: str
    alpha_strength: float | None = None
    probability_confidence: float | None = None
    expected_edge_net: float | None = None
    base_edge_net: float | None = None
    edge_to_risk_ratio: float | None = None
    min_alpha_strength_required: float | None = None
    min_probability_confidence_required: float | None = None
    min_expected_edge_net_required: float | None = None
    min_edge_to_risk_ratio_required: float | None = None
    min_base_edge_net_required: float | None = None
    min_probability_breakeven_gap_required: float | None = None
    metar_max_age_minutes_applied: float | None = None
    metar_local_hour: int | None = None
    sparse_evidence_hardening_applied: bool = False
    sparse_evidence_probability_raise: float | None = None
    sparse_evidence_expected_edge_raise: float | None = None
    sparse_evidence_support_score: float | None = None
    sparse_evidence_volatility_penalty: float | None = None
    historical_quality_penalty_ratio: float | None = None
    historical_quality_boost_ratio: float | None = None
    historical_quality_probability_raise: float | None = None
    historical_quality_expected_edge_raise: float | None = None
    historical_quality_score_adjustment: float | None = None
    historical_quality_sample_size: int | None = None
    historical_quality_sources: str = ""
    historical_quality_signal_bucket_penalty_ratio: float | None = None
    historical_quality_signal_bucket_samples: int | None = None
    historical_quality_station_bucket_penalty_ratio: float | None = None
    historical_quality_station_bucket_samples: int | None = None
    historical_quality_local_hour_bucket_penalty_ratio: float | None = None
    historical_quality_local_hour_bucket_samples: int | None = None
    historical_quality_signal_hard_block_active: bool = False
    historical_quality_station_hour_hard_block_active: bool = False
    historical_expectancy_hard_block_active: bool = False
    historical_expectancy_pressure_score: float | None = None
    historical_expectancy_edge_raise: float | None = None
    historical_expectancy_probability_raise: float | None = None
    historical_quality_global_only_pressure_active: bool = False
    historical_quality_global_only_adjusted_share: float | None = None
    historical_quality_global_only_excess_ratio: float | None = None
    historical_profitability_guardrail_penalty_ratio: float | None = None
    historical_profitability_guardrail_probability_raise: float | None = None
    historical_profitability_guardrail_expected_edge_raise: float | None = None
    historical_profitability_guardrail_calibration_ratio: float | None = None
    historical_profitability_guardrail_evidence_confidence: float | None = None
    historical_profitability_guardrail_resolved_unique_market_sides: int | None = None
    historical_profitability_guardrail_repeated_entry_multiplier: float | None = None
    historical_profitability_guardrail_concentration_warning: bool | None = None
    historical_profitability_guardrail_status: str = ""
    historical_profitability_guardrail_signals: str = ""
    historical_profitability_bucket_guardrail_penalty_ratio: float | None = None
    historical_profitability_bucket_guardrail_probability_raise: float | None = None
    historical_profitability_bucket_guardrail_expected_edge_raise: float | None = None
    historical_profitability_bucket_guardrail_status: str = ""
    historical_profitability_bucket_guardrail_sources: str = ""

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TemperaturePolicyGate:
    min_settlement_confidence: float = 0.6
    max_metar_age_minutes: float | None = 20.0
    station_max_metar_age_minutes: dict[str, float] = field(default_factory=dict)
    station_local_hour_max_metar_age_minutes: dict[str, dict[int, float]] = field(default_factory=dict)
    min_alpha_strength: float | None = 0.0
    min_probability_confidence: float | None = None
    min_expected_edge_net: float | None = None
    min_edge_to_risk_ratio: float | None = None
    min_base_edge_net: float | None = 0.0
    min_probability_breakeven_gap: float | None = 0.0
    enforce_probability_edge_thresholds: bool = False
    fallback_min_probability_confidence: float | None = None
    fallback_min_expected_edge_net: float | None = 0.005
    fallback_min_edge_to_risk_ratio: float | None = 0.02
    enforce_entry_price_probability_floor: bool = False
    enforce_sparse_evidence_hardening: bool = True
    sparse_evidence_support_floor: float = 0.45
    sparse_evidence_probability_step_per_support_gap: float = 0.06
    sparse_evidence_volatility_gate: float = 0.12
    sparse_evidence_probability_step_per_volatility_gap: float = 0.04
    sparse_evidence_probability_raise_max: float = 0.04
    sparse_evidence_expected_edge_raise_per_prob_point: float = 0.15
    sparse_evidence_expected_edge_raise_max: float = 0.006
    enforce_interval_consistency: bool = True
    max_yes_possible_gap_for_yes_side: float = 0.0
    min_hours_to_close: float | None = 0.0
    max_hours_to_close: float | None = 48.0
    max_intents_per_underlying: int = 6
    taf_stale_grace_minutes: float = 0.0
    taf_stale_grace_max_volatility_score: float | None = 1.0
    taf_stale_grace_max_range_width: float | None = 10.0
    metar_freshness_quality_boundary_ratio: float | None = 0.92
    metar_freshness_quality_probability_margin: float = 0.03
    metar_freshness_quality_expected_edge_margin: float = 0.005
    metar_ingest_quality_gate_enabled: bool = False
    metar_ingest_min_quality_score: float | None = None
    metar_ingest_min_fresh_station_coverage_ratio: float | None = None
    metar_ingest_require_ready_status: bool = False
    metar_ingest_quality_score: float | None = None
    metar_ingest_quality_grade: str = ""
    metar_ingest_quality_status: str = ""
    metar_ingest_quality_signal_count: int | None = None
    metar_ingest_quality_signals: list[str] = field(default_factory=list)
    metar_ingest_fresh_station_coverage_ratio: float | None = None
    high_price_edge_guard_enabled: bool = False
    high_price_edge_guard_min_entry_price_dollars: float = 0.85
    high_price_edge_guard_min_expected_edge_net: float = 0.0
    high_price_edge_guard_min_edge_to_risk_ratio: float = 0.02
    historical_selection_quality_profile: dict[str, Any] | None = None
    historical_quality_probability_penalty_max: float = 0.05
    historical_quality_expected_edge_penalty_max: float = 0.006
    historical_quality_score_adjust_scale: float = 0.35
    enforce_historical_bucket_hard_blocks: bool = True
    historical_bucket_station_hour_hard_block_penalty_ratio: float = 0.72
    historical_bucket_station_hour_hard_block_min_samples: int = 14
    historical_bucket_signal_hard_block_penalty_ratio: float = 0.78
    historical_bucket_signal_hard_block_min_samples: int = 18
    enforce_historical_expectancy_edge_hardening: bool = True
    historical_expectancy_negative_threshold: float = -0.05
    historical_expectancy_win_rate_floor: float = 0.55
    historical_expectancy_min_samples: int = 12
    historical_expectancy_edge_raise_max: float = 0.02
    historical_expectancy_probability_raise_max: float = 0.03
    enforce_historical_expectancy_hard_blocks: bool = True
    historical_expectancy_hard_block_negative_threshold: float = -0.10
    historical_expectancy_hard_block_win_rate_floor: float = 0.50
    historical_expectancy_hard_block_min_samples: int = 24
    historical_expectancy_hard_block_min_bucket_matches: int = 2
    weather_pattern_hardening_enabled: bool = True
    weather_pattern_profile: dict[str, Any] | None = None
    weather_pattern_profile_max_age_hours: float = 72.0
    weather_pattern_min_bucket_samples: int = 12
    weather_pattern_negative_expectancy_threshold: float = -0.05
    weather_pattern_negative_regime_suppression_enabled: bool = False
    weather_pattern_negative_regime_suppression_min_bucket_samples: int = 24
    weather_pattern_negative_regime_suppression_expectancy_threshold: float = -0.08
    weather_pattern_negative_regime_suppression_top_n: int = 8
    weather_pattern_risk_off_enabled: bool = True
    weather_pattern_risk_off_concentration_threshold: float = 0.75
    weather_pattern_risk_off_min_attempts: int = 24
    weather_pattern_risk_off_stale_metar_share_threshold: float = 0.50
    weather_pattern_risk_off_state: dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    weather_pattern_negative_regime_suppression_state: dict[str, Any] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    require_market_snapshot_seq: bool = True
    require_metar_snapshot_sha: bool = False

    def _effective_metar_age_limit(self, intent: TemperatureTradeIntent) -> tuple[float | None, int | None]:
        station = _normalize_text(intent.settlement_station).upper()
        local_hour: int | None = None
        observed_ts = _parse_ts(intent.metar_observation_time_utc) or _parse_ts(intent.captured_at)
        if isinstance(observed_ts, datetime):
            timezone_name = _normalize_text(intent.settlement_timezone)
            if timezone_name:
                try:
                    local_hour = int(observed_ts.astimezone(ZoneInfo(timezone_name)).hour)
                except Exception:
                    local_hour = int(observed_ts.hour)
            else:
                local_hour = int(observed_ts.hour)

        station_hour_overrides = self.station_local_hour_max_metar_age_minutes.get(station, {})
        if isinstance(station_hour_overrides, dict) and local_hour is not None and local_hour in station_hour_overrides:
            return _sanitize_metar_age_minutes(station_hour_overrides.get(local_hour)), local_hour

        if station in self.station_max_metar_age_minutes:
            return _sanitize_metar_age_minutes(self.station_max_metar_age_minutes.get(station)), local_hour

        return _sanitize_metar_age_minutes(self.max_metar_age_minutes), local_hour

    def evaluate(
        self,
        *,
        intents: list[TemperatureTradeIntent],
        existing_underlying_slots: dict[str, int] | None = None,
        settlement_state_by_underlying: dict[str, dict[str, Any]] | None = None,
    ) -> list[TemperaturePolicyDecision]:
        decisions: list[TemperaturePolicyDecision] = []
        approved_by_underlying: dict[str, int] = {}
        min_alpha_strength_required = (
            float(self.min_alpha_strength)
            if isinstance(self.min_alpha_strength, (int, float)) and math.isfinite(float(self.min_alpha_strength))
            else None
        )
        requested_min_probability_confidence = _sanitize_probability_threshold(self.min_probability_confidence)
        min_probability_confidence = requested_min_probability_confidence
        probability_threshold_defaulted = False
        requested_min_expected_edge_net = _sanitize_edge_threshold(self.min_expected_edge_net)
        base_min_expected_edge_net = requested_min_expected_edge_net
        requested_min_edge_to_risk_ratio = _sanitize_edge_threshold(self.min_edge_to_risk_ratio)
        min_edge_to_risk_ratio = requested_min_edge_to_risk_ratio
        requested_min_base_edge_net = _sanitize_edge_threshold(self.min_base_edge_net)
        min_base_edge_net = requested_min_base_edge_net
        requested_probability_breakeven_gap = _sanitize_edge_threshold(self.min_probability_breakeven_gap)
        min_probability_breakeven_gap = requested_probability_breakeven_gap
        if self.enforce_probability_edge_thresholds:
            if (
                min_probability_confidence is None
                or float(min_probability_confidence) <= 0.0
            ):
                probability_threshold_defaulted = True
                fallback_probability = _sanitize_probability_threshold(self.fallback_min_probability_confidence)
                if fallback_probability is None:
                    fallback_probability = _sanitize_probability_threshold(self.min_settlement_confidence)
                min_probability_confidence = fallback_probability
            if (
                base_min_expected_edge_net is None
                or float(base_min_expected_edge_net) <= 0.0
            ):
                fallback_edge = _sanitize_edge_threshold(self.fallback_min_expected_edge_net)
                if fallback_edge is None:
                    fallback_edge = 0.005
                base_min_expected_edge_net = fallback_edge
            if (
                min_edge_to_risk_ratio is None
                or float(min_edge_to_risk_ratio) <= 0.0
            ):
                fallback_edge_to_risk_ratio = _sanitize_edge_threshold(self.fallback_min_edge_to_risk_ratio)
                if fallback_edge_to_risk_ratio is None:
                    fallback_edge_to_risk_ratio = 0.02
                min_edge_to_risk_ratio = fallback_edge_to_risk_ratio
            if min_base_edge_net is None or float(min_base_edge_net) < 0.0:
                min_base_edge_net = 0.0
            if min_probability_breakeven_gap is None or float(min_probability_breakeven_gap) < 0.0:
                min_probability_breakeven_gap = 0.0
        freshness_boundary_ratio_raw = _parse_float(self.metar_freshness_quality_boundary_ratio)
        freshness_boundary_ratio = (
            max(0.0, min(1.0, float(freshness_boundary_ratio_raw)))
            if isinstance(freshness_boundary_ratio_raw, float) and math.isfinite(freshness_boundary_ratio_raw)
            else None
        )
        freshness_probability_margin = max(
            0.0,
            float(_parse_float(self.metar_freshness_quality_probability_margin) or 0.0),
        )
        freshness_expected_edge_margin = max(
            0.0,
            float(_parse_float(self.metar_freshness_quality_expected_edge_margin) or 0.0),
        )
        metar_ingest_quality_gate = _evaluate_metar_ingest_quality_gate(
            gate_enabled=self.metar_ingest_quality_gate_enabled,
            min_quality_score=self.metar_ingest_min_quality_score,
            min_fresh_station_coverage_ratio=self.metar_ingest_min_fresh_station_coverage_ratio,
            require_ready_status=self.metar_ingest_require_ready_status,
            quality_status=self.metar_ingest_quality_status,
            quality_score=self.metar_ingest_quality_score,
            fresh_station_coverage_ratio=self.metar_ingest_fresh_station_coverage_ratio,
        )
        metar_ingest_quality_failures = list(
            metar_ingest_quality_gate.get("failure_reasons")
            if isinstance(metar_ingest_quality_gate.get("failure_reasons"), list)
            else []
        )
        metar_ingest_quality_status = _normalize_text(
            metar_ingest_quality_gate.get("quality_status")
        ) or "unknown"
        metar_ingest_quality_score = _sanitize_unit_threshold(
            metar_ingest_quality_gate.get("quality_score")
        )
        metar_ingest_fresh_station_coverage_ratio = _sanitize_unit_threshold(
            metar_ingest_quality_gate.get("fresh_station_coverage_ratio")
        )
        metar_ingest_quality_grade = _normalize_text(self.metar_ingest_quality_grade).lower() or "unknown"
        metar_ingest_quality_signals = (
            [
                _normalize_text(item)
                for item in self.metar_ingest_quality_signals
                if _normalize_text(item)
            ]
            if isinstance(self.metar_ingest_quality_signals, list)
            else []
        )
        metar_ingest_quality_signal_count = max(
            int(_parse_float(self.metar_ingest_quality_signal_count) or 0),
            len(metar_ingest_quality_signals),
        )
        high_price_edge_guard_enabled = bool(self.high_price_edge_guard_enabled)
        high_price_edge_guard_min_entry_price_dollars = max(
            0.0,
            min(
                1.0,
                float(_parse_float(self.high_price_edge_guard_min_entry_price_dollars) or 0.85),
            ),
        )
        high_price_edge_guard_min_expected_edge_net = max(
            0.0,
            float(_parse_float(self.high_price_edge_guard_min_expected_edge_net) or 0.0),
        )
        high_price_edge_guard_min_edge_to_risk_ratio = max(
            0.0,
            float(_parse_float(self.high_price_edge_guard_min_edge_to_risk_ratio) or 0.02),
        )
        historical_profitability_guardrail = _historical_profitability_guardrail(
            self.historical_selection_quality_profile
        )
        guardrail_penalty_ratio = max(
            0.0,
            min(
                1.0,
                float(_parse_float(historical_profitability_guardrail.get("penalty_ratio")) or 0.0),
            ),
        )
        guardrail_probability_raise = max(
            0.0,
            float(_parse_float(historical_profitability_guardrail.get("probability_raise")) or 0.0),
        )
        guardrail_expected_edge_raise = max(
            0.0,
            float(_parse_float(historical_profitability_guardrail.get("expected_edge_raise")) or 0.0),
        )
        guardrail_status = _normalize_text(historical_profitability_guardrail.get("status")).lower() or "disabled"
        guardrail_signals = (
            historical_profitability_guardrail.get("signals")
            if isinstance(historical_profitability_guardrail.get("signals"), list)
            else []
        )
        guardrail_calibration_ratio = _parse_float(historical_profitability_guardrail.get("calibration_ratio"))
        guardrail_evidence_confidence = _parse_float(historical_profitability_guardrail.get("evidence_confidence"))
        guardrail_resolved_unique_market_sides = int(
            _parse_float(historical_profitability_guardrail.get("resolved_unique_market_sides")) or 0
        )
        guardrail_repeated_entry_multiplier = _parse_float(
            historical_profitability_guardrail.get("repeated_entry_multiplier")
        )
        guardrail_concentration_warning_raw = historical_profitability_guardrail.get("concentration_warning")
        guardrail_concentration_warning = (
            bool(guardrail_concentration_warning_raw)
            if isinstance(guardrail_concentration_warning_raw, bool)
            else None
        )
        enforce_historical_bucket_hard_blocks = bool(
            self.enforce_probability_edge_thresholds and self.enforce_historical_bucket_hard_blocks
        )
        station_hour_hard_block_penalty_ratio = _clamp_unit(
            self.historical_bucket_station_hour_hard_block_penalty_ratio,
            0.72,
        )
        station_hour_hard_block_min_samples = max(
            1,
            int(_parse_float(self.historical_bucket_station_hour_hard_block_min_samples) or 14),
        )
        signal_hard_block_penalty_ratio = _clamp_unit(
            self.historical_bucket_signal_hard_block_penalty_ratio,
            0.78,
        )
        signal_hard_block_min_samples = max(
            1,
            int(_parse_float(self.historical_bucket_signal_hard_block_min_samples) or 18),
        )
        enforce_historical_expectancy_edge_hardening = bool(
            self.enforce_probability_edge_thresholds and self.enforce_historical_expectancy_edge_hardening
        )
        historical_expectancy_negative_threshold = min(
            -1e-9,
            float(_parse_float(self.historical_expectancy_negative_threshold) or -0.05),
        )
        historical_expectancy_win_rate_floor = _clamp_unit(
            self.historical_expectancy_win_rate_floor,
            0.55,
        )
        historical_expectancy_min_samples = max(
            1,
            int(_parse_float(self.historical_expectancy_min_samples) or 12),
        )
        historical_expectancy_edge_raise_max = max(
            0.0,
            min(
                0.08,
                float(_parse_float(self.historical_expectancy_edge_raise_max) or 0.02),
            ),
        )
        historical_expectancy_probability_raise_max = max(
            0.0,
            min(
                0.10,
                float(_parse_float(self.historical_expectancy_probability_raise_max) or 0.03),
            ),
        )
        enforce_historical_expectancy_hard_blocks = bool(
            self.enforce_probability_edge_thresholds and self.enforce_historical_expectancy_hard_blocks
        )
        historical_expectancy_hard_block_negative_threshold = min(
            -1e-9,
            float(_parse_float(self.historical_expectancy_hard_block_negative_threshold) or -0.10),
        )
        historical_expectancy_hard_block_win_rate_floor = _clamp_unit(
            self.historical_expectancy_hard_block_win_rate_floor,
            0.50,
        )
        historical_expectancy_hard_block_min_samples = max(
            1,
            int(_parse_float(self.historical_expectancy_hard_block_min_samples) or 24),
        )
        historical_expectancy_hard_block_min_bucket_matches = max(
            1,
            min(
                3,
                int(_parse_float(self.historical_expectancy_hard_block_min_bucket_matches) or 2),
            ),
        )
        weather_pattern_hardening_enabled = bool(self.weather_pattern_hardening_enabled)
        weather_pattern_min_bucket_samples = max(1, int(_parse_float(self.weather_pattern_min_bucket_samples) or 1))
        weather_pattern_negative_expectancy_threshold = max(
            -0.25,
            min(
                -0.005,
                float(_parse_float(self.weather_pattern_negative_expectancy_threshold) or -0.05),
            ),
        )
        weather_pattern_negative_regime_suppression_enabled = bool(
            self.weather_pattern_negative_regime_suppression_enabled
        )
        weather_pattern_negative_regime_suppression_min_bucket_samples = max(
            1,
            int(
                _parse_float(self.weather_pattern_negative_regime_suppression_min_bucket_samples)
                or 24
            ),
        )
        weather_pattern_negative_regime_suppression_expectancy_threshold = max(
            -0.25,
            min(
                -0.005,
                float(
                    _parse_float(
                        self.weather_pattern_negative_regime_suppression_expectancy_threshold
                    )
                    or -0.08
                ),
            ),
        )
        weather_pattern_negative_regime_suppression_top_n = max(
            1,
            min(
                64,
                int(_parse_float(self.weather_pattern_negative_regime_suppression_top_n) or 8),
            ),
        )
        weather_pattern_negative_regime_suppression_state = (
            _weather_pattern_negative_regime_suppression_candidates(
                profile=self.weather_pattern_profile,
                enabled=weather_pattern_negative_regime_suppression_enabled,
                min_bucket_samples=weather_pattern_negative_regime_suppression_min_bucket_samples,
                negative_expectancy_threshold=weather_pattern_negative_regime_suppression_expectancy_threshold,
                top_n=weather_pattern_negative_regime_suppression_top_n,
            )
        )
        suppression_candidates_by_bucket: dict[tuple[str, str], dict[str, Any]] = {}
        for row in (
            weather_pattern_negative_regime_suppression_state.get("suppression_candidates")
            if isinstance(weather_pattern_negative_regime_suppression_state.get("suppression_candidates"), list)
            else []
        ):
            if not isinstance(row, dict):
                continue
            dimension = _normalize_weather_pattern_dimension_name(row.get("dimension"))
            bucket = _normalize_weather_pattern_dimension_key(
                dimension=dimension,
                key=row.get("bucket"),
            )
            if not dimension or not bucket:
                continue
            suppression_candidates_by_bucket[(dimension, bucket)] = row
        self.weather_pattern_negative_regime_suppression_state = dict(
            weather_pattern_negative_regime_suppression_state
        )
        weather_pattern_negative_regime_suppression_active = bool(
            weather_pattern_negative_regime_suppression_state.get("active")
        )
        weather_pattern_risk_off_state = _weather_pattern_global_risk_off_state(
            profile=self.weather_pattern_profile,
            enabled=bool(self.weather_pattern_risk_off_enabled),
            concentration_threshold=float(
                _parse_float(self.weather_pattern_risk_off_concentration_threshold) or 0.75
            ),
            min_attempts=int(_parse_float(self.weather_pattern_risk_off_min_attempts) or 24),
            stale_metar_share_threshold=float(
                _parse_float(self.weather_pattern_risk_off_stale_metar_share_threshold) or 0.50
            ),
        )
        self.weather_pattern_risk_off_state = dict(weather_pattern_risk_off_state)
        weather_pattern_global_risk_off_active = bool(weather_pattern_risk_off_state.get("active"))
        weather_pattern_probability_raise_count = 0
        weather_pattern_expected_edge_raise_count = 0
        weather_pattern_hard_block_count = 0
        weather_pattern_matched_bucket_evidence: list[str] = []
        weather_pattern_hard_block_evidence: list[str] = []
        if isinstance(existing_underlying_slots, dict):
            for underlying_key, slots in existing_underlying_slots.items():
                key = _normalize_text(underlying_key)
                if not key:
                    continue
                approved_by_underlying[key] = max(0, int(slots))

        for intent in intents:
            blocked: list[str] = []
            decision_notes_extra: list[str] = []
            if bool(metar_ingest_quality_gate.get("enabled")):
                decision_notes_extra.append("metar_ingest_quality_gate_enabled=true")
                decision_notes_extra.append(
                    "metar_ingest_quality_gate_require_ready_status="
                    f"{str(bool(metar_ingest_quality_gate.get('require_ready_status'))).lower()}"
                )
                if isinstance(metar_ingest_quality_gate.get("min_quality_score"), float):
                    decision_notes_extra.append(
                        "metar_ingest_quality_min_score="
                        f"{float(metar_ingest_quality_gate.get('min_quality_score')):.6f}"
                    )
                if isinstance(
                    metar_ingest_quality_gate.get("min_fresh_station_coverage_ratio"),
                    float,
                ):
                    decision_notes_extra.append(
                        "metar_ingest_quality_min_fresh_station_coverage_ratio="
                        f"{float(metar_ingest_quality_gate.get('min_fresh_station_coverage_ratio')):.6f}"
                    )
                decision_notes_extra.append(
                    f"metar_ingest_quality_status={metar_ingest_quality_status}"
                )
                decision_notes_extra.append(
                    f"metar_ingest_quality_grade={metar_ingest_quality_grade}"
                )
                if isinstance(metar_ingest_quality_score, float):
                    decision_notes_extra.append(
                        f"metar_ingest_quality_score={metar_ingest_quality_score:.6f}"
                    )
                if isinstance(metar_ingest_fresh_station_coverage_ratio, float):
                    decision_notes_extra.append(
                        "metar_ingest_fresh_station_coverage_ratio="
                        f"{metar_ingest_fresh_station_coverage_ratio:.6f}"
                    )
                decision_notes_extra.append(
                    "metar_ingest_quality_signal_count="
                    f"{int(metar_ingest_quality_signal_count)}"
                )
                if metar_ingest_quality_signals:
                    decision_notes_extra.append(
                        "metar_ingest_quality_signals="
                        + ";".join(metar_ingest_quality_signals)
                    )
                if metar_ingest_quality_failures:
                    blocked.append("metar_ingest_quality_insufficient")
                    decision_notes_extra.append(
                        "metar_ingest_quality_gate_failures="
                        + "+".join(sorted(metar_ingest_quality_failures))
                    )
            alpha_strength = _intent_alpha_strength(intent)
            probability_confidence, probability_breakdown = _estimate_temperature_probability_confidence(
                intent=intent,
                alpha_strength=alpha_strength,
            )
            effective_min_probability_confidence = min_probability_confidence
            entry_price_probability_floor: float | None = None
            if (
                self.enforce_probability_edge_thresholds
                and probability_threshold_defaulted
                and bool(self.enforce_entry_price_probability_floor)
            ):
                entry_price_probability_floor = _entry_price_min_probability_floor(
                    intent.max_entry_price_dollars
                )
                if (
                    entry_price_probability_floor is not None
                    and (
                        effective_min_probability_confidence is None
                        or entry_price_probability_floor > float(effective_min_probability_confidence)
                    )
                ):
                    effective_min_probability_confidence = float(entry_price_probability_floor)
            expected_edge_profile = _estimate_temperature_edge_profile(
                intent=intent,
                alpha_strength=alpha_strength,
                probability_confidence=probability_confidence,
            )
            expected_edge_net = float(expected_edge_profile.get("edge_net") or 0.0)
            base_edge_net = float(expected_edge_profile.get("base_edge") or 0.0)
            side_win_probability = float(expected_edge_profile.get("side_win_probability") or probability_confidence)
            breakeven_probability = max(0.0, min(0.999, float(intent.max_entry_price_dollars)))
            probability_breakeven_gap = float(side_win_probability) - float(breakeven_probability)
            downside_per_contract = max(
                0.0,
                float(expected_edge_profile.get("downside_per_contract") or 0.0),
            )
            edge_to_risk_ratio = (
                float(base_edge_net) / float(downside_per_contract)
                if downside_per_contract > 0.0
                else None
            )
            entry_price_edge_floor = 0.0
            effective_min_expected_edge_net = base_min_expected_edge_net
            if self.enforce_probability_edge_thresholds and bool(self.enforce_entry_price_probability_floor):
                entry_price_edge_floor = _entry_price_min_expected_edge_floor(intent.max_entry_price_dollars)
            if entry_price_edge_floor > 0.0:
                if (
                    effective_min_expected_edge_net is None
                    or entry_price_edge_floor > float(effective_min_expected_edge_net)
                ):
                    effective_min_expected_edge_net = float(entry_price_edge_floor)
            evidence_support_score = max(
                0.0,
                min(
                    1.0,
                    float(probability_breakdown.get("evidence_support_score") or 0.0),
                ),
            )
            volatility_penalty_score = max(
                0.0,
                min(
                    1.0,
                    float(probability_breakdown.get("volatility_penalty") or 0.0),
                ),
            )
            sparse_evidence_probability_raise = 0.0
            sparse_evidence_expected_edge_raise = 0.0
            if (
                self.enforce_probability_edge_thresholds
                and bool(self.enforce_sparse_evidence_hardening)
            ):
                support_floor = max(
                    0.0,
                    min(
                        1.0,
                        float(
                            _parse_float(self.sparse_evidence_support_floor)
                            if _parse_float(self.sparse_evidence_support_floor) is not None
                            else 0.45
                        ),
                    ),
                )
                support_step = max(
                    0.0,
                    float(
                        _parse_float(self.sparse_evidence_probability_step_per_support_gap)
                        if _parse_float(self.sparse_evidence_probability_step_per_support_gap) is not None
                        else 0.06
                    ),
                )
                volatility_gate = max(
                    0.0,
                    min(
                        1.0,
                        float(
                            _parse_float(self.sparse_evidence_volatility_gate)
                            if _parse_float(self.sparse_evidence_volatility_gate) is not None
                            else 0.12
                        ),
                    ),
                )
                volatility_step = max(
                    0.0,
                    float(
                        _parse_float(self.sparse_evidence_probability_step_per_volatility_gap)
                        if _parse_float(self.sparse_evidence_probability_step_per_volatility_gap) is not None
                        else 0.04
                    ),
                )
                probability_raise_cap = max(
                    0.0,
                    min(
                        0.30,
                        float(
                            _parse_float(self.sparse_evidence_probability_raise_max)
                            if _parse_float(self.sparse_evidence_probability_raise_max) is not None
                            else 0.04
                        ),
                    ),
                )
                expected_edge_raise_per_prob = max(
                    0.0,
                    float(
                        _parse_float(self.sparse_evidence_expected_edge_raise_per_prob_point)
                        if _parse_float(self.sparse_evidence_expected_edge_raise_per_prob_point) is not None
                        else 0.15
                    ),
                )
                expected_edge_raise_cap = max(
                    0.0,
                    float(
                        _parse_float(self.sparse_evidence_expected_edge_raise_max)
                        if _parse_float(self.sparse_evidence_expected_edge_raise_max) is not None
                        else 0.006
                    ),
                )
                if evidence_support_score < support_floor:
                    sparse_evidence_probability_raise += (support_floor - evidence_support_score) * support_step
                if volatility_penalty_score > volatility_gate:
                    sparse_evidence_probability_raise += (volatility_penalty_score - volatility_gate) * volatility_step
                sparse_evidence_probability_raise = max(
                    0.0,
                    min(probability_raise_cap, float(sparse_evidence_probability_raise)),
                )
                if sparse_evidence_probability_raise > 0.0:
                    if effective_min_probability_confidence is None:
                        effective_min_probability_confidence = _sanitize_probability_threshold(
                            self.min_settlement_confidence
                        )
                    if effective_min_probability_confidence is not None:
                        effective_min_probability_confidence = min(
                            0.995,
                            float(effective_min_probability_confidence) + float(sparse_evidence_probability_raise),
                        )
                    if effective_min_expected_edge_net is not None:
                        sparse_evidence_expected_edge_raise = min(
                            expected_edge_raise_cap,
                            float(sparse_evidence_probability_raise) * expected_edge_raise_per_prob,
                        )
                        effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                            sparse_evidence_expected_edge_raise
                        )

            historical_quality_adjustment = selection_quality_adjustment_for_intent(
                intent=intent,
                profile=self.historical_selection_quality_profile,
                probability_penalty_max=max(0.0, float(self.historical_quality_probability_penalty_max)),
                expected_edge_penalty_max=max(0.0, float(self.historical_quality_expected_edge_penalty_max)),
                score_adjust_scale=max(0.0, float(self.historical_quality_score_adjust_scale)),
            )
            historical_bucket_guardrail = _historical_profitability_bucket_guardrail(
                intent=intent,
                profile=self.historical_selection_quality_profile,
            )
            historical_penalty_ratio = max(
                0.0,
                min(1.0, float(_parse_float(historical_quality_adjustment.get("penalty_ratio")) or 0.0)),
            )
            historical_boost_ratio = max(
                0.0,
                min(1.0, float(_parse_float(historical_quality_adjustment.get("boost_ratio")) or 0.0)),
            )
            historical_probability_raise = max(
                0.0,
                float(_parse_float(historical_quality_adjustment.get("probability_raise")) or 0.0),
            )
            historical_expected_edge_raise = max(
                0.0,
                float(_parse_float(historical_quality_adjustment.get("expected_edge_raise")) or 0.0),
            )
            historical_score_adjustment = float(
                _parse_float(historical_quality_adjustment.get("score_adjustment")) or 0.0
            )
            historical_sample_size = max(
                0,
                int(_parse_float(historical_quality_adjustment.get("sample_size")) or 0),
            )
            historical_global_only_pressure_active = bool(
                historical_quality_adjustment.get("global_only_pressure_active")
            )
            historical_global_only_adjusted_share = _parse_float(
                historical_quality_adjustment.get("global_only_adjusted_share")
            )
            historical_global_only_excess_ratio = max(
                0.0,
                min(
                    1.0,
                    float(_parse_float(historical_quality_adjustment.get("global_only_excess_ratio")) or 0.0),
                ),
            )
            historical_sources = (
                historical_quality_adjustment.get("sources")
                if isinstance(historical_quality_adjustment.get("sources"), list)
                else []
            )
            historical_status = _normalize_text(historical_quality_adjustment.get("status")).lower() or "unknown"
            bucket_guardrail_penalty_ratio = max(
                0.0,
                min(
                    1.0,
                    float(_parse_float(historical_bucket_guardrail.get("penalty_ratio")) or 0.0),
                ),
            )
            bucket_guardrail_probability_raise = max(
                0.0,
                float(_parse_float(historical_bucket_guardrail.get("probability_raise")) or 0.0),
            )
            bucket_guardrail_expected_edge_raise = max(
                0.0,
                float(_parse_float(historical_bucket_guardrail.get("expected_edge_raise")) or 0.0),
            )
            bucket_guardrail_status = _normalize_text(historical_bucket_guardrail.get("status")).lower() or "unknown"
            bucket_guardrail_sources = (
                historical_bucket_guardrail.get("sources")
                if isinstance(historical_bucket_guardrail.get("sources"), list)
                else []
            )
            historical_bucket_snapshot = _historical_quality_bucket_snapshot(
                intent=intent,
                profile=self.historical_selection_quality_profile,
            )
            station_bucket_entry = (
                historical_bucket_snapshot.get("station")
                if isinstance(historical_bucket_snapshot.get("station"), dict)
                else {}
            )
            local_hour_bucket_entry = (
                historical_bucket_snapshot.get("local_hour")
                if isinstance(historical_bucket_snapshot.get("local_hour"), dict)
                else {}
            )
            signal_bucket_entry = (
                historical_bucket_snapshot.get("signal_type")
                if isinstance(historical_bucket_snapshot.get("signal_type"), dict)
                else {}
            )
            station_bucket_penalty_ratio = _clamp_unit(station_bucket_entry.get("penalty_ratio"), 0.0)
            station_bucket_samples = max(0, int(_parse_float(station_bucket_entry.get("samples")) or 0))
            local_hour_bucket_penalty_ratio = _clamp_unit(local_hour_bucket_entry.get("penalty_ratio"), 0.0)
            local_hour_bucket_samples = max(0, int(_parse_float(local_hour_bucket_entry.get("samples")) or 0))
            signal_bucket_penalty_ratio = _clamp_unit(signal_bucket_entry.get("penalty_ratio"), 0.0)
            signal_bucket_samples = max(0, int(_parse_float(signal_bucket_entry.get("samples")) or 0))
            signal_hard_block_active = False
            station_hour_hard_block_active = False
            historical_expectancy_hard_block_active = False
            historical_expectancy_pressure_score = 0.0
            historical_expectancy_edge_raise = 0.0
            historical_expectancy_probability_raise = 0.0
            station_bucket_expectancy = _parse_float(station_bucket_entry.get("expectancy_per_trade"))
            local_hour_bucket_expectancy = _parse_float(local_hour_bucket_entry.get("expectancy_per_trade"))
            signal_bucket_expectancy = _parse_float(signal_bucket_entry.get("expectancy_per_trade"))
            station_bucket_win_rate = _parse_float(station_bucket_entry.get("win_rate"))
            local_hour_bucket_win_rate = _parse_float(local_hour_bucket_entry.get("win_rate"))
            signal_bucket_win_rate = _parse_float(signal_bucket_entry.get("win_rate"))

            def _bucket_expectancy_pressure(
                *,
                expectancy_per_trade: float | None,
                win_rate: float | None,
                samples: int,
            ) -> float:
                if samples < historical_expectancy_min_samples:
                    return 0.0
                if not isinstance(expectancy_per_trade, float):
                    return 0.0
                if float(expectancy_per_trade) >= float(historical_expectancy_negative_threshold):
                    return 0.0
                reliability = max(
                    0.0,
                    min(1.0, float(samples) / max(1.0, float(historical_expectancy_min_samples) * 3.0)),
                )
                expectancy_span = max(0.02, abs(float(historical_expectancy_negative_threshold)) * 3.0)
                expectancy_pressure = max(
                    0.0,
                    min(
                        1.0,
                        (float(historical_expectancy_negative_threshold) - float(expectancy_per_trade))
                        / expectancy_span,
                    ),
                )
                if isinstance(win_rate, float):
                    win_rate_pressure = max(
                        0.0,
                        min(
                            1.0,
                            (float(historical_expectancy_win_rate_floor) - float(win_rate))
                            / max(0.05, float(historical_expectancy_win_rate_floor)),
                        ),
                    )
                else:
                    win_rate_pressure = 0.0
                return max(
                    0.0,
                    min(
                        1.0,
                        reliability * (0.70 * expectancy_pressure + 0.30 * win_rate_pressure),
                    ),
                )

            def _bucket_expectancy_hard_block(
                *,
                expectancy_per_trade: float | None,
                win_rate: float | None,
                samples: int,
            ) -> bool:
                if samples < historical_expectancy_hard_block_min_samples:
                    return False
                expectancy_bad = bool(
                    isinstance(expectancy_per_trade, float)
                    and float(expectancy_per_trade)
                    <= float(historical_expectancy_hard_block_negative_threshold)
                )
                win_rate_bad = bool(
                    isinstance(win_rate, float)
                    and float(win_rate) <= float(historical_expectancy_hard_block_win_rate_floor)
                )
                return bool(expectancy_bad or win_rate_bad)

            if enforce_historical_expectancy_edge_hardening:
                signal_expectancy_pressure = _bucket_expectancy_pressure(
                    expectancy_per_trade=signal_bucket_expectancy,
                    win_rate=signal_bucket_win_rate,
                    samples=signal_bucket_samples,
                )
                station_expectancy_pressure = _bucket_expectancy_pressure(
                    expectancy_per_trade=station_bucket_expectancy,
                    win_rate=station_bucket_win_rate,
                    samples=station_bucket_samples,
                )
                local_hour_expectancy_pressure = _bucket_expectancy_pressure(
                    expectancy_per_trade=local_hour_bucket_expectancy,
                    win_rate=local_hour_bucket_win_rate,
                    samples=local_hour_bucket_samples,
                )
                historical_expectancy_pressure_score = max(
                    0.0,
                    min(
                        1.0,
                        0.45 * signal_expectancy_pressure
                        + 0.30 * station_expectancy_pressure
                        + 0.25 * local_hour_expectancy_pressure,
                    ),
                )
                if historical_expectancy_pressure_score > 0.0:
                    historical_expectancy_probability_raise = min(
                        float(historical_expectancy_probability_raise_max),
                        float(historical_expectancy_probability_raise_max)
                        * float(historical_expectancy_pressure_score),
                    )
                    historical_expectancy_edge_raise = min(
                        float(historical_expectancy_edge_raise_max),
                        float(historical_expectancy_edge_raise_max)
                        * float(historical_expectancy_pressure_score),
                    )
                    if effective_min_probability_confidence is None:
                        effective_min_probability_confidence = _sanitize_probability_threshold(
                            self.min_settlement_confidence
                        )
                    if effective_min_probability_confidence is not None:
                        effective_min_probability_confidence = min(
                            0.995,
                            float(effective_min_probability_confidence)
                            + float(historical_expectancy_probability_raise),
                        )
                    if effective_min_expected_edge_net is None:
                        effective_min_expected_edge_net = _sanitize_edge_threshold(self.fallback_min_expected_edge_net)
                        if effective_min_expected_edge_net is None:
                            effective_min_expected_edge_net = 0.005
                    effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                        historical_expectancy_edge_raise
                    )
            if historical_probability_raise > 0.0:
                if effective_min_probability_confidence is None:
                    effective_min_probability_confidence = _sanitize_probability_threshold(self.min_settlement_confidence)
                if effective_min_probability_confidence is not None:
                    effective_min_probability_confidence = min(
                        0.995,
                        float(effective_min_probability_confidence) + float(historical_probability_raise),
                    )
            if historical_expected_edge_raise > 0.0:
                if effective_min_expected_edge_net is None:
                    effective_min_expected_edge_net = _sanitize_edge_threshold(self.fallback_min_expected_edge_net)
                    if effective_min_expected_edge_net is None:
                        effective_min_expected_edge_net = 0.005
                effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                    historical_expected_edge_raise
                )
            if bucket_guardrail_probability_raise > 0.0:
                if effective_min_probability_confidence is None:
                    effective_min_probability_confidence = _sanitize_probability_threshold(
                        self.min_settlement_confidence
                    )
                if effective_min_probability_confidence is not None:
                    effective_min_probability_confidence = min(
                        0.995,
                        float(effective_min_probability_confidence) + float(bucket_guardrail_probability_raise),
                    )
            if bucket_guardrail_expected_edge_raise > 0.0:
                if effective_min_expected_edge_net is None:
                    effective_min_expected_edge_net = _sanitize_edge_threshold(self.fallback_min_expected_edge_net)
                    if effective_min_expected_edge_net is None:
                        effective_min_expected_edge_net = 0.005
                effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                    bucket_guardrail_expected_edge_raise
                )
            if guardrail_probability_raise > 0.0:
                if effective_min_probability_confidence is None:
                    effective_min_probability_confidence = _sanitize_probability_threshold(
                        self.min_settlement_confidence
                    )
                if effective_min_probability_confidence is not None:
                    effective_min_probability_confidence = min(
                        0.995,
                        float(effective_min_probability_confidence) + float(guardrail_probability_raise),
                    )
            if guardrail_expected_edge_raise > 0.0:
                if effective_min_expected_edge_net is None:
                    effective_min_expected_edge_net = _sanitize_edge_threshold(self.fallback_min_expected_edge_net)
                    if effective_min_expected_edge_net is None:
                        effective_min_expected_edge_net = 0.005
                effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                    guardrail_expected_edge_raise
                )

            if intent.constraint_status not in _ACTIONABLE_CONSTRAINTS:
                blocked.append("constraint_not_actionable")
            if intent.settlement_confidence_score < float(self.min_settlement_confidence):
                blocked.append("settlement_confidence_below_min")
            if min_alpha_strength_required is not None and alpha_strength < float(min_alpha_strength_required):
                blocked.append("alpha_strength_below_min")
            if self.require_metar_snapshot_sha and not intent.metar_snapshot_sha:
                blocked.append("missing_metar_snapshot_sha")
            if self.require_market_snapshot_seq and intent.market_snapshot_seq is None:
                blocked.append("missing_market_snapshot_seq")
            if not intent.spec_hash:
                blocked.append("missing_spec_hash")
            if weather_pattern_global_risk_off_active:
                blocked.append("weather_pattern_global_risk_off")
                decision_notes_extra.append("weather_pattern_global_risk_off_triggered=true")
                decision_notes_extra.append(
                    "weather_pattern_global_risk_off_status="
                    f"{_normalize_text(weather_pattern_risk_off_state.get('status')) or 'active'}"
                )
                if isinstance(weather_pattern_risk_off_state.get("observed_concentration"), (int, float)):
                    decision_notes_extra.append(
                        "weather_pattern_global_risk_off_observed_concentration="
                        f"{float(weather_pattern_risk_off_state.get('observed_concentration')):.6f}"
                    )
                if isinstance(weather_pattern_risk_off_state.get("observed_stale_metar_share"), (int, float)):
                    decision_notes_extra.append(
                        "weather_pattern_global_risk_off_observed_stale_metar_share="
                        f"{float(weather_pattern_risk_off_state.get('observed_stale_metar_share')):.6f}"
                    )
                decision_notes_extra.append(
                    "weather_pattern_global_risk_off_observed_attempts="
                    f"{int(_parse_float(weather_pattern_risk_off_state.get('observed_attempts')) or 0)}"
                )
                activation_reasons = (
                    weather_pattern_risk_off_state.get("activation_reasons")
                    if isinstance(weather_pattern_risk_off_state.get("activation_reasons"), list)
                    else []
                )
                if activation_reasons:
                    decision_notes_extra.append(
                        "weather_pattern_global_risk_off_activation_reasons="
                        + ";".join(_normalize_text(reason) for reason in activation_reasons if _normalize_text(reason))
                    )
            decision_notes_extra.append(f"alpha_strength={alpha_strength:.6f}")
            decision_notes_extra.append(f"probability_confidence={probability_confidence:.6f}")
            decision_notes_extra.append(f"expected_edge_net={expected_edge_net:.6f}")
            decision_notes_extra.append(f"base_edge_net={base_edge_net:.6f}")
            decision_notes_extra.append(f"probability_breakeven_gap={probability_breakeven_gap:.6f}")
            decision_notes_extra.append(f"breakeven_probability={breakeven_probability:.6f}")
            if edge_to_risk_ratio is not None:
                decision_notes_extra.append(f"edge_to_risk_ratio={float(edge_to_risk_ratio):.6f}")
            if entry_price_edge_floor > 0.0:
                decision_notes_extra.append(f"entry_price_edge_floor={entry_price_edge_floor:.6f}")
            if min_alpha_strength_required is not None:
                decision_notes_extra.append(f"min_alpha_strength_required={float(min_alpha_strength_required):.6f}")
            if effective_min_probability_confidence is not None:
                decision_notes_extra.append(
                    f"min_probability_confidence_required={float(effective_min_probability_confidence):.6f}"
                )
            if effective_min_expected_edge_net is not None:
                decision_notes_extra.append(f"min_expected_edge_net_required={float(effective_min_expected_edge_net):.6f}")
            if min_edge_to_risk_ratio is not None:
                decision_notes_extra.append(f"min_edge_to_risk_ratio_required={float(min_edge_to_risk_ratio):.6f}")
            if min_base_edge_net is not None:
                decision_notes_extra.append(f"min_base_edge_net_required={float(min_base_edge_net):.6f}")
            if min_probability_breakeven_gap is not None:
                decision_notes_extra.append(
                    f"min_probability_breakeven_gap_required={float(min_probability_breakeven_gap):.6f}"
                )
            if (
                base_min_expected_edge_net is not None
                and effective_min_expected_edge_net is not None
                and float(effective_min_expected_edge_net) > float(base_min_expected_edge_net)
            ):
                decision_notes_extra.append(
                    f"min_expected_edge_net_raised_for_entry_price={float(effective_min_expected_edge_net):.6f}"
                )
            if self.enforce_probability_edge_thresholds:
                if (
                    (
                        requested_min_probability_confidence is None
                        or float(requested_min_probability_confidence) <= 0.0
                    )
                    and effective_min_probability_confidence is not None
                ):
                    decision_notes_extra.append(
                        f"min_probability_confidence_defaulted={float(effective_min_probability_confidence):.6f}"
                    )
                if (
                    isinstance(entry_price_probability_floor, float)
                    and effective_min_probability_confidence is not None
                    and float(entry_price_probability_floor)
                    >= float(effective_min_probability_confidence) - 1e-9
                ):
                    decision_notes_extra.append(
                        f"min_probability_confidence_raised_for_entry_price={float(entry_price_probability_floor):.6f}"
                    )
                if (
                    (
                        requested_min_expected_edge_net is None
                        or float(requested_min_expected_edge_net) <= 0.0
                    )
                    and base_min_expected_edge_net is not None
                ):
                    decision_notes_extra.append(
                        f"min_expected_edge_net_defaulted={float(base_min_expected_edge_net):.6f}"
                    )
                if (
                    (
                        requested_min_edge_to_risk_ratio is None
                        or float(requested_min_edge_to_risk_ratio) <= 0.0
                    )
                    and min_edge_to_risk_ratio is not None
                ):
                    decision_notes_extra.append(
                        f"min_edge_to_risk_ratio_defaulted={float(min_edge_to_risk_ratio):.6f}"
                    )
            decision_notes_extra.append(
                f"probability_confidence_base={float(probability_breakdown.get('base_confidence') or 0.0):.6f}"
            )
            decision_notes_extra.append(
                f"probability_evidence_support={float(probability_breakdown.get('evidence_support_score') or 0.0):.6f}"
            )
            decision_notes_extra.append(
                f"probability_confidence_shrunk={float(probability_breakdown.get('confidence_shrunk') or 0.0):.6f}"
            )
            decision_notes_extra.append(
                f"probability_uplift_cap={float(probability_breakdown.get('uplift_cap') or 0.0):.6f}"
            )
            if sparse_evidence_probability_raise > 0.0:
                decision_notes_extra.append(
                    f"sparse_evidence_probability_raise={float(sparse_evidence_probability_raise):.6f}"
                )
                decision_notes_extra.append(
                    f"sparse_evidence_expected_edge_raise={float(sparse_evidence_expected_edge_raise):.6f}"
                )
                decision_notes_extra.append(
                    f"sparse_evidence_support_score={evidence_support_score:.6f}"
                )
                decision_notes_extra.append(
                    f"sparse_evidence_volatility_penalty={volatility_penalty_score:.6f}"
                )
            if historical_penalty_ratio > 0.0 or historical_boost_ratio > 0.0:
                decision_notes_extra.append(f"historical_quality_status={historical_status}")
                decision_notes_extra.append(f"historical_quality_penalty_ratio={historical_penalty_ratio:.6f}")
                decision_notes_extra.append(f"historical_quality_boost_ratio={historical_boost_ratio:.6f}")
                decision_notes_extra.append(f"historical_quality_score_adjustment={historical_score_adjustment:.6f}")
                decision_notes_extra.append(
                    f"historical_quality_sample_size={int(historical_sample_size)}"
                )
                decision_notes_extra.append(
                    "historical_quality_global_only_pressure_active="
                    f"{str(bool(historical_global_only_pressure_active)).lower()}"
                )
                if historical_global_only_adjusted_share is not None:
                    decision_notes_extra.append(
                        "historical_quality_global_only_adjusted_share="
                        f"{float(historical_global_only_adjusted_share):.6f}"
                    )
                decision_notes_extra.append(
                    "historical_quality_global_only_excess_ratio="
                    f"{float(historical_global_only_excess_ratio):.6f}"
                )
                if historical_probability_raise > 0.0:
                    decision_notes_extra.append(
                        f"historical_quality_probability_raise={historical_probability_raise:.6f}"
                    )
                if historical_expected_edge_raise > 0.0:
                    decision_notes_extra.append(
                        f"historical_quality_expected_edge_raise={historical_expected_edge_raise:.6f}"
                    )
                if historical_sources:
                    decision_notes_extra.append(
                        f"historical_quality_sources={';'.join(_normalize_text(item) for item in historical_sources if _normalize_text(item))}"
                    )
            if historical_expectancy_pressure_score > 0.0:
                decision_notes_extra.append(
                    f"historical_expectancy_pressure_score={float(historical_expectancy_pressure_score):.6f}"
                )
                decision_notes_extra.append(
                    f"historical_expectancy_probability_raise={float(historical_expectancy_probability_raise):.6f}"
                )
                decision_notes_extra.append(
                    f"historical_expectancy_edge_raise={float(historical_expectancy_edge_raise):.6f}"
                )
                if isinstance(signal_bucket_expectancy, float):
                    decision_notes_extra.append(
                        f"historical_expectancy_signal_type={float(signal_bucket_expectancy):.6f}:samples={signal_bucket_samples}"
                    )
                if isinstance(station_bucket_expectancy, float):
                    decision_notes_extra.append(
                        f"historical_expectancy_station={float(station_bucket_expectancy):.6f}:samples={station_bucket_samples}"
                    )
                if isinstance(local_hour_bucket_expectancy, float):
                    decision_notes_extra.append(
                        f"historical_expectancy_local_hour={float(local_hour_bucket_expectancy):.6f}:samples={local_hour_bucket_samples}"
                    )
            if bucket_guardrail_penalty_ratio > 0.0:
                decision_notes_extra.append(
                    f"historical_profitability_bucket_guardrail_status={bucket_guardrail_status}"
                )
                decision_notes_extra.append(
                    "historical_profitability_bucket_guardrail_penalty_ratio="
                    f"{bucket_guardrail_penalty_ratio:.6f}"
                )
                if bucket_guardrail_probability_raise > 0.0:
                    decision_notes_extra.append(
                        "historical_profitability_bucket_guardrail_probability_raise="
                        f"{bucket_guardrail_probability_raise:.6f}"
                    )
                if bucket_guardrail_expected_edge_raise > 0.0:
                    decision_notes_extra.append(
                        "historical_profitability_bucket_guardrail_expected_edge_raise="
                        f"{bucket_guardrail_expected_edge_raise:.6f}"
                    )
                if bucket_guardrail_sources:
                    decision_notes_extra.append(
                        "historical_profitability_bucket_guardrail_sources="
                        + ";".join(_normalize_text(item) for item in bucket_guardrail_sources if _normalize_text(item))
                    )
            if guardrail_penalty_ratio > 0.0:
                decision_notes_extra.append(f"historical_profitability_guardrail_status={guardrail_status}")
                decision_notes_extra.append(
                    f"historical_profitability_guardrail_penalty_ratio={guardrail_penalty_ratio:.6f}"
                )
                if guardrail_probability_raise > 0.0:
                    decision_notes_extra.append(
                        f"historical_profitability_guardrail_probability_raise={guardrail_probability_raise:.6f}"
                    )
                if guardrail_expected_edge_raise > 0.0:
                    decision_notes_extra.append(
                        f"historical_profitability_guardrail_expected_edge_raise={guardrail_expected_edge_raise:.6f}"
                    )
                if guardrail_evidence_confidence is not None:
                    decision_notes_extra.append(
                        f"historical_profitability_guardrail_evidence_confidence={guardrail_evidence_confidence:.6f}"
                    )
                if guardrail_calibration_ratio is not None:
                    decision_notes_extra.append(
                        f"historical_profitability_guardrail_calibration_ratio={guardrail_calibration_ratio:.6f}"
                    )
                decision_notes_extra.append(
                    f"historical_profitability_guardrail_resolved_unique_market_sides={guardrail_resolved_unique_market_sides}"
                )
                if guardrail_repeated_entry_multiplier is not None:
                    decision_notes_extra.append(
                        "historical_profitability_guardrail_repeated_entry_multiplier="
                        f"{guardrail_repeated_entry_multiplier:.6f}"
                    )
                if guardrail_concentration_warning is not None:
                    decision_notes_extra.append(
                        "historical_profitability_guardrail_concentration_warning="
                        f"{str(guardrail_concentration_warning).lower()}"
                    )
                if guardrail_signals:
                    decision_notes_extra.append(
                        "historical_profitability_guardrail_signals="
                        + ";".join(_normalize_text(item) for item in guardrail_signals if _normalize_text(item))
                    )

            if self.enforce_interval_consistency:
                if intent.range_family_consistency_conflict:
                    blocked.append("range_family_consistency_conflict")
                if intent.side == "yes":
                    if isinstance(intent.yes_possible_overlap, bool) and not intent.yes_possible_overlap:
                        blocked.append("yes_side_interval_infeasible")
                    if (
                        isinstance(intent.yes_possible_gap, (int, float))
                        and float(intent.yes_possible_gap) > float(self.max_yes_possible_gap_for_yes_side)
                    ):
                        blocked.append("yes_side_gap_above_max")
                elif intent.side == "no":
                    if isinstance(intent.yes_possible_overlap, bool) and intent.yes_possible_overlap:
                        blocked.append("no_side_interval_overlap_still_possible")
                    if isinstance(intent.yes_possible_gap, (int, float)) and float(intent.yes_possible_gap) <= 0.0:
                        blocked.append("no_side_gap_nonpositive")
            global_only_pressure_hard_block = bool(
                self.enforce_probability_edge_thresholds
                and historical_global_only_pressure_active
                and (
                    (
                        historical_sample_size <= 0
                        and historical_global_only_excess_ratio >= 0.50
                    )
                    or historical_global_only_excess_ratio >= 0.75
                )
            )
            if global_only_pressure_hard_block:
                blocked.append("historical_quality_global_only_pressure")
                decision_notes_extra.append(
                    "historical_quality_global_only_pressure_triggered=true"
                )
            if enforce_historical_bucket_hard_blocks:
                signal_hard_block_gate_probability = max(
                    0.72,
                    float(effective_min_probability_confidence or 0.0) + 0.02,
                )
                signal_hard_block_gate_edge = max(
                    0.018,
                    float(effective_min_expected_edge_net or 0.0) + 0.005,
                )
                if (
                    signal_bucket_samples >= signal_hard_block_min_samples
                    and signal_bucket_penalty_ratio >= signal_hard_block_penalty_ratio
                    and (
                        probability_confidence < signal_hard_block_gate_probability
                        or expected_edge_net < signal_hard_block_gate_edge
                    )
                ):
                    signal_hard_block_active = True
                    blocked.append("historical_quality_signal_type_hard_block")
                    decision_notes_extra.append(
                        "historical_quality_signal_hard_block="
                        f"{_normalize_text(signal_bucket_entry.get('key')) or _normalize_text(intent.constraint_status).lower()}:"
                        f"penalty={signal_bucket_penalty_ratio:.6f}:samples={signal_bucket_samples}"
                    )
                station_hour_hard_block_gate_probability = max(
                    0.70,
                    float(effective_min_probability_confidence or 0.0) + 0.015,
                )
                station_hour_hard_block_gate_edge = max(
                    0.016,
                    float(effective_min_expected_edge_net or 0.0) + 0.004,
                )
                station_hour_pair_penalty = (
                    station_bucket_penalty_ratio + local_hour_bucket_penalty_ratio
                ) / 2.0
                if (
                    station_bucket_samples >= station_hour_hard_block_min_samples
                    and local_hour_bucket_samples >= station_hour_hard_block_min_samples
                    and station_bucket_penalty_ratio >= station_hour_hard_block_penalty_ratio
                    and local_hour_bucket_penalty_ratio >= station_hour_hard_block_penalty_ratio
                    and station_hour_pair_penalty >= station_hour_hard_block_penalty_ratio
                    and (
                        probability_confidence < station_hour_hard_block_gate_probability
                        or expected_edge_net < station_hour_hard_block_gate_edge
                    )
                ):
                    station_hour_hard_block_active = True
                    blocked.append("historical_quality_station_hour_hard_block")
                    decision_notes_extra.append(
                        "historical_quality_station_hour_hard_block="
                        f"station:{_normalize_text(station_bucket_entry.get('key')) or _normalize_text(intent.settlement_station).upper()}:"
                        f"local_hour:{_normalize_text(local_hour_bucket_entry.get('key'))}:"
                        f"penalty={station_hour_pair_penalty:.6f}:"
                        f"samples={min(station_bucket_samples, local_hour_bucket_samples)}"
                    )
            if enforce_historical_expectancy_hard_blocks:
                hard_block_hits: list[str] = []
                if _bucket_expectancy_hard_block(
                    expectancy_per_trade=signal_bucket_expectancy,
                    win_rate=signal_bucket_win_rate,
                    samples=signal_bucket_samples,
                ):
                    signal_key = _normalize_text(signal_bucket_entry.get("key")) or _normalize_text(intent.constraint_status).lower()
                    signal_win_rate_text = (
                        f"{float(signal_bucket_win_rate):.4f}"
                        if isinstance(signal_bucket_win_rate, float)
                        else "n/a"
                    )
                    hard_block_hits.append(
                        "signal_type:"
                        f"{signal_key}:"
                        f"exp={float(signal_bucket_expectancy):.4f}:"
                        f"win={signal_win_rate_text}:"
                        f"samples={signal_bucket_samples}"
                    )
                if _bucket_expectancy_hard_block(
                    expectancy_per_trade=station_bucket_expectancy,
                    win_rate=station_bucket_win_rate,
                    samples=station_bucket_samples,
                ):
                    station_key = _normalize_text(station_bucket_entry.get("key")) or _normalize_text(intent.settlement_station).upper()
                    station_win_rate_text = (
                        f"{float(station_bucket_win_rate):.4f}"
                        if isinstance(station_bucket_win_rate, float)
                        else "n/a"
                    )
                    hard_block_hits.append(
                        "station:"
                        f"{station_key}:"
                        f"exp={float(station_bucket_expectancy):.4f}:"
                        f"win={station_win_rate_text}:"
                        f"samples={station_bucket_samples}"
                    )
                if _bucket_expectancy_hard_block(
                    expectancy_per_trade=local_hour_bucket_expectancy,
                    win_rate=local_hour_bucket_win_rate,
                    samples=local_hour_bucket_samples,
                ):
                    hour_key = _normalize_text(local_hour_bucket_entry.get("key"))
                    local_hour_win_rate_text = (
                        f"{float(local_hour_bucket_win_rate):.4f}"
                        if isinstance(local_hour_bucket_win_rate, float)
                        else "n/a"
                    )
                    hard_block_hits.append(
                        "local_hour:"
                        f"{hour_key}:"
                        f"exp={float(local_hour_bucket_expectancy):.4f}:"
                        f"win={local_hour_win_rate_text}:"
                        f"samples={local_hour_bucket_samples}"
                    )
                if len(hard_block_hits) >= historical_expectancy_hard_block_min_bucket_matches:
                    historical_expectancy_hard_block_active = True
                    blocked.append("historical_expectancy_hard_block")
                    decision_notes_extra.append("historical_expectancy_hard_block_triggered=true")
                    decision_notes_extra.append(
                        "historical_expectancy_hard_block_hits="
                        + ";".join(hard_block_hits)
                    )
            if weather_pattern_negative_regime_suppression_enabled:
                decision_notes_extra.append("weather_pattern_negative_regime_suppression_enabled=true")
                decision_notes_extra.append(
                    "weather_pattern_negative_regime_suppression_candidate_count="
                    f"{int(_parse_float(weather_pattern_negative_regime_suppression_state.get('candidate_count')) or 0)}"
                )
                if weather_pattern_negative_regime_suppression_active:
                    suppression_matches: list[dict[str, Any]] = []
                    for dimension, key_text in _weather_pattern_dimension_keys_for_intent(intent).items():
                        if not key_text:
                            continue
                        candidate = suppression_candidates_by_bucket.get((dimension, key_text))
                        if isinstance(candidate, dict):
                            suppression_matches.append(candidate)
                    if suppression_matches:
                        blocked.append("weather_pattern_negative_regime_bucket_suppressed")
                        decision_notes_extra.append("weather_pattern_negative_regime_suppressed=true")
                        for row in suppression_matches[:8]:
                            decision_notes_extra.append(
                                "weather_pattern_negative_regime_suppression_match="
                                f"{_normalize_text(row.get('dimension'))}:{_normalize_text(row.get('bucket'))}:"
                                f"samples={int(_parse_float(row.get('samples')) or 0)}:"
                                f"expectancy={float(_parse_float(row.get('expectancy_per_trade')) or 0.0):.6f}:"
                                f"source={_normalize_text(row.get('source')) or 'unknown'}"
                            )
            if weather_pattern_hardening_enabled:
                weather_pattern_hardening = _weather_pattern_hardening_for_intent(
                    intent=intent,
                    profile=self.weather_pattern_profile,
                    min_bucket_samples=weather_pattern_min_bucket_samples,
                    negative_expectancy_threshold=weather_pattern_negative_expectancy_threshold,
                )
                if weather_pattern_hardening["matched_bucket_count"] > 0:
                    decision_notes_extra.append(
                        f"weather_pattern_hardening_status={_normalize_text(weather_pattern_hardening.get('status'))}"
                    )
                    for evidence in weather_pattern_hardening["matched_bucket_evidence"]:
                        decision_notes_extra.append(f"weather_pattern_bucket_match={evidence}")
                    weather_pattern_matched_bucket_evidence.extend(
                        list(weather_pattern_hardening["matched_bucket_evidence"])
                    )
                    if weather_pattern_hardening["probability_raise"] > 0.0:
                        weather_pattern_probability_raise_count += 1
                        if effective_min_probability_confidence is None:
                            effective_min_probability_confidence = _sanitize_probability_threshold(
                                self.min_settlement_confidence
                            )
                        if effective_min_probability_confidence is None:
                            effective_min_probability_confidence = 0.6
                        effective_min_probability_confidence = min(
                            0.995,
                            float(effective_min_probability_confidence)
                            + float(weather_pattern_hardening["probability_raise"]),
                        )
                        decision_notes_extra.append(
                            f"weather_pattern_probability_raise={float(weather_pattern_hardening['probability_raise']):.6f}"
                        )
                    if weather_pattern_hardening["expected_edge_raise"] > 0.0:
                        weather_pattern_expected_edge_raise_count += 1
                        if effective_min_expected_edge_net is None:
                            effective_min_expected_edge_net = _sanitize_edge_threshold(
                                self.fallback_min_expected_edge_net
                            )
                            if effective_min_expected_edge_net is None:
                                effective_min_expected_edge_net = 0.005
                        effective_min_expected_edge_net = float(effective_min_expected_edge_net) + float(
                            weather_pattern_hardening["expected_edge_raise"]
                        )
                        decision_notes_extra.append(
                            f"weather_pattern_expected_edge_raise={float(weather_pattern_hardening['expected_edge_raise']):.6f}"
                        )
                    if weather_pattern_hardening["hard_block_active"]:
                        weather_pattern_hard_block_count += 1
                        weather_pattern_hard_block_evidence.extend(
                            list(weather_pattern_hardening["hard_block_hits"])
                        )
                        blocked.append("weather_pattern_multi_bucket_hard_block")
                        decision_notes_extra.append("weather_pattern_hard_block_triggered=true")
                        decision_notes_extra.append(
                            "weather_pattern_hard_block_hits="
                            + ";".join(weather_pattern_hardening["hard_block_hits"])
                        )
            if (
                high_price_edge_guard_enabled
                and float(intent.max_entry_price_dollars) >= float(high_price_edge_guard_min_entry_price_dollars)
            ):
                decision_notes_extra.append(
                    "high_price_edge_guard_entry_floor="
                    f"{high_price_edge_guard_min_entry_price_dollars:.6f}"
                )
                decision_notes_extra.append(
                    "high_price_edge_guard_min_expected_edge_net="
                    f"{high_price_edge_guard_min_expected_edge_net:.6f}"
                )
                decision_notes_extra.append(
                    "high_price_edge_guard_min_edge_to_risk_ratio="
                    f"{high_price_edge_guard_min_edge_to_risk_ratio:.6f}"
                )
                if expected_edge_net <= float(high_price_edge_guard_min_expected_edge_net):
                    blocked.append("high_price_expected_edge_nonpositive")
                if (
                    edge_to_risk_ratio is None
                    or edge_to_risk_ratio < float(high_price_edge_guard_min_edge_to_risk_ratio)
                ):
                    blocked.append("high_price_edge_to_risk_ratio_below_min")
            taf_status = _normalize_text(intent.taf_status).lower()
            if taf_status == "missing_station":
                blocked.append("taf_station_missing")
                decision_notes_extra.append("taf_station_missing=true")
            if (
                effective_min_probability_confidence is not None
                and probability_confidence < float(effective_min_probability_confidence)
            ):
                blocked.append("probability_confidence_below_min")
                if guardrail_probability_raise > 0.0 or bucket_guardrail_probability_raise > 0.0:
                    blocked.append("historical_profitability_probability_below_min")
            if (
                effective_min_expected_edge_net is not None
                and expected_edge_net < float(effective_min_expected_edge_net)
            ):
                blocked.append("expected_edge_below_min")
                if guardrail_expected_edge_raise > 0.0 or bucket_guardrail_expected_edge_raise > 0.0:
                    blocked.append("historical_profitability_expected_edge_below_min")
            if (
                min_edge_to_risk_ratio is not None
                and edge_to_risk_ratio is not None
                and edge_to_risk_ratio < float(min_edge_to_risk_ratio)
            ):
                blocked.append("edge_to_risk_ratio_below_min")
            if self.enforce_probability_edge_thresholds:
                if min_base_edge_net is not None and base_edge_net < float(min_base_edge_net):
                    blocked.append("base_edge_below_min")
                if (
                    min_probability_breakeven_gap is not None
                    and probability_breakeven_gap < float(min_probability_breakeven_gap)
                ):
                    blocked.append("probability_breakeven_gap_below_min")

            effective_metar_age_limit, metar_local_hour = self._effective_metar_age_limit(intent)
            if effective_metar_age_limit is not None:
                decision_notes_extra.append(f"metar_max_age_minutes_applied={effective_metar_age_limit:.3f}")
                if metar_local_hour is not None:
                    decision_notes_extra.append(f"metar_local_hour={metar_local_hour}")
                effective_limit_for_age_check = float(effective_metar_age_limit)
                taf_grace_applied = 0.0
                if (
                    isinstance(self.taf_stale_grace_minutes, (int, float))
                    and float(self.taf_stale_grace_minutes) > 0.0
                    and _normalize_text(intent.forecast_model_status).lower() == "ready"
                ):
                    taf_status = _normalize_text(intent.taf_status).lower()
                    if taf_status in {"ready", "partial", "degraded"}:
                        taf_grace_applied = float(self.taf_stale_grace_minutes)
                        if taf_status in {"partial", "degraded"}:
                            taf_grace_applied *= 0.5
                        if (
                            isinstance(self.taf_stale_grace_max_volatility_score, (int, float))
                            and isinstance(intent.taf_volatility_score, (int, float))
                            and float(intent.taf_volatility_score) > float(self.taf_stale_grace_max_volatility_score)
                        ):
                            taf_grace_applied = 0.0
                        if (
                            isinstance(self.taf_stale_grace_max_range_width, (int, float))
                            and isinstance(intent.forecast_range_width, (int, float))
                            and float(intent.forecast_range_width) > float(self.taf_stale_grace_max_range_width)
                        ):
                            taf_grace_applied = 0.0
                if taf_grace_applied > 0.0:
                    effective_limit_for_age_check = round(
                        float(effective_metar_age_limit) + max(0.0, float(taf_grace_applied)),
                        6,
                    )
                    decision_notes_extra.append(f"taf_stale_grace_minutes_applied={float(taf_grace_applied):.3f}")
                    decision_notes_extra.append(f"metar_max_age_minutes_effective={effective_limit_for_age_check:.3f}")
                if intent.metar_observation_age_minutes is None:
                    blocked.append("metar_observation_age_unknown")
                else:
                    metar_observation_age_minutes = float(intent.metar_observation_age_minutes)
                    decision_notes_extra.append(f"metar_observation_age_minutes={metar_observation_age_minutes:.3f}")
                    if metar_observation_age_minutes > float(effective_limit_for_age_check):
                        blocked.append("metar_observation_stale")
                    else:
                        age_ratio = (
                            metar_observation_age_minutes / max(1e-9, float(effective_limit_for_age_check))
                        )
                        decision_notes_extra.append(f"metar_observation_age_ratio={age_ratio:.6f}")
                        if (
                            isinstance(freshness_boundary_ratio, float)
                            and freshness_boundary_ratio > 0.0
                            and age_ratio >= float(freshness_boundary_ratio)
                        ):
                            boundary_failures: list[str] = []
                            if (
                                effective_min_probability_confidence is not None
                                and freshness_probability_margin > 0.0
                            ):
                                boundary_probability_min = _sanitize_probability_threshold(
                                    float(effective_min_probability_confidence) + float(freshness_probability_margin)
                                )
                                if boundary_probability_min is not None:
                                    decision_notes_extra.append(
                                        f"metar_boundary_min_probability_confidence={float(boundary_probability_min):.6f}"
                                    )
                                    if probability_confidence < float(boundary_probability_min):
                                        boundary_failures.append("probability_confidence")
                            if (
                                effective_min_expected_edge_net is not None
                                and freshness_expected_edge_margin > 0.0
                            ):
                                boundary_expected_edge_min = _sanitize_edge_threshold(
                                    float(effective_min_expected_edge_net) + float(freshness_expected_edge_margin)
                                )
                                if boundary_expected_edge_min is not None:
                                    decision_notes_extra.append(
                                        f"metar_boundary_min_expected_edge_net={float(boundary_expected_edge_min):.6f}"
                                    )
                                    if expected_edge_net < float(boundary_expected_edge_min):
                                        boundary_failures.append("expected_edge")
                            if boundary_failures:
                                blocked.append("metar_freshness_boundary_quality_insufficient")
                                decision_notes_extra.append(
                                    f"metar_boundary_failures={'+'.join(sorted(boundary_failures))}"
                                )

            if self.min_hours_to_close is not None and intent.hours_to_close is not None:
                if float(intent.hours_to_close) < float(self.min_hours_to_close):
                    blocked.append("inside_cutoff_window")
            if self.max_hours_to_close is not None and intent.hours_to_close is not None:
                if float(intent.hours_to_close) > float(self.max_hours_to_close):
                    blocked.append("outside_active_horizon")

            if isinstance(settlement_state_by_underlying, dict):
                settlement_entry = settlement_state_by_underlying.get(intent.underlying_key)
                settlement_block_reason = _settlement_block_reason(settlement_entry)
                if settlement_block_reason:
                    blocked.append(settlement_block_reason)

            current_underlying = approved_by_underlying.get(intent.underlying_key, 0)
            if current_underlying >= max(1, int(self.max_intents_per_underlying)):
                blocked.append("underlying_exposure_cap_reached")

            if blocked:
                deduped_blocked = _unique_preserve_order(blocked)
                primary_block_reason = _primary_policy_block_reason(deduped_blocked)
                decision_notes = [primary_block_reason] + [
                    reason for reason in deduped_blocked if reason != primary_block_reason
                ]
                decision_notes.extend(decision_notes_extra)
                decisions.append(
                    TemperaturePolicyDecision(
                        intent_id=intent.intent_id,
                        approved=False,
                        decision_reason=primary_block_reason,
                        decision_notes=",".join(decision_notes),
                        alpha_strength=round(alpha_strength, 6),
                        probability_confidence=round(probability_confidence, 6),
                        expected_edge_net=round(expected_edge_net, 6),
                        base_edge_net=round(base_edge_net, 6),
                        edge_to_risk_ratio=(round(edge_to_risk_ratio, 6) if edge_to_risk_ratio is not None else None),
                        min_alpha_strength_required=min_alpha_strength_required,
                        min_probability_confidence_required=effective_min_probability_confidence,
                        min_expected_edge_net_required=effective_min_expected_edge_net,
                        min_edge_to_risk_ratio_required=min_edge_to_risk_ratio,
                        min_base_edge_net_required=min_base_edge_net,
                        min_probability_breakeven_gap_required=min_probability_breakeven_gap,
                        metar_max_age_minutes_applied=effective_metar_age_limit,
                        metar_local_hour=metar_local_hour,
                        sparse_evidence_hardening_applied=bool(sparse_evidence_probability_raise > 0.0),
                        sparse_evidence_probability_raise=round(float(sparse_evidence_probability_raise), 6),
                        sparse_evidence_expected_edge_raise=round(float(sparse_evidence_expected_edge_raise), 6),
                        sparse_evidence_support_score=round(float(evidence_support_score), 6),
                        sparse_evidence_volatility_penalty=round(float(volatility_penalty_score), 6),
                        historical_quality_penalty_ratio=round(float(historical_penalty_ratio), 6),
                        historical_quality_boost_ratio=round(float(historical_boost_ratio), 6),
                        historical_quality_probability_raise=round(float(historical_probability_raise), 6),
                        historical_quality_expected_edge_raise=round(float(historical_expected_edge_raise), 6),
                        historical_quality_score_adjustment=round(float(historical_score_adjustment), 6),
                        historical_quality_sample_size=int(historical_sample_size),
                        historical_quality_sources=";".join(
                            _normalize_text(item) for item in historical_sources if _normalize_text(item)
                        ),
                        historical_quality_signal_bucket_penalty_ratio=round(
                            float(signal_bucket_penalty_ratio),
                            6,
                        ),
                        historical_quality_signal_bucket_samples=int(signal_bucket_samples),
                        historical_quality_station_bucket_penalty_ratio=round(
                            float(station_bucket_penalty_ratio),
                            6,
                        ),
                        historical_quality_station_bucket_samples=int(station_bucket_samples),
                        historical_quality_local_hour_bucket_penalty_ratio=round(
                            float(local_hour_bucket_penalty_ratio),
                            6,
                        ),
                        historical_quality_local_hour_bucket_samples=int(local_hour_bucket_samples),
                        historical_quality_signal_hard_block_active=bool(signal_hard_block_active),
                        historical_quality_station_hour_hard_block_active=bool(
                            station_hour_hard_block_active
                        ),
                        historical_expectancy_hard_block_active=bool(
                            historical_expectancy_hard_block_active
                        ),
                        historical_expectancy_pressure_score=round(
                            float(historical_expectancy_pressure_score),
                            6,
                        ),
                        historical_expectancy_edge_raise=round(
                            float(historical_expectancy_edge_raise),
                            6,
                        ),
                        historical_expectancy_probability_raise=round(
                            float(historical_expectancy_probability_raise),
                            6,
                        ),
                        historical_quality_global_only_pressure_active=bool(
                            historical_global_only_pressure_active
                        ),
                        historical_quality_global_only_adjusted_share=(
                            round(float(historical_global_only_adjusted_share), 6)
                            if isinstance(historical_global_only_adjusted_share, float)
                            else None
                        ),
                        historical_quality_global_only_excess_ratio=round(
                            float(historical_global_only_excess_ratio), 6
                        ),
                        historical_profitability_guardrail_penalty_ratio=round(float(guardrail_penalty_ratio), 6),
                        historical_profitability_guardrail_probability_raise=round(
                            float(guardrail_probability_raise), 6
                        ),
                        historical_profitability_guardrail_expected_edge_raise=round(
                            float(guardrail_expected_edge_raise), 6
                        ),
                        historical_profitability_guardrail_calibration_ratio=(
                            round(float(guardrail_calibration_ratio), 6)
                            if isinstance(guardrail_calibration_ratio, float)
                            else None
                        ),
                        historical_profitability_guardrail_evidence_confidence=(
                            round(float(guardrail_evidence_confidence), 6)
                            if isinstance(guardrail_evidence_confidence, float)
                            else None
                        ),
                        historical_profitability_guardrail_resolved_unique_market_sides=int(
                            guardrail_resolved_unique_market_sides
                        ),
                        historical_profitability_guardrail_repeated_entry_multiplier=(
                            round(float(guardrail_repeated_entry_multiplier), 6)
                            if isinstance(guardrail_repeated_entry_multiplier, float)
                            else None
                        ),
                        historical_profitability_guardrail_concentration_warning=guardrail_concentration_warning,
                        historical_profitability_guardrail_status=guardrail_status,
                        historical_profitability_guardrail_signals=";".join(
                            _normalize_text(item) for item in guardrail_signals if _normalize_text(item)
                        ),
                        historical_profitability_bucket_guardrail_penalty_ratio=round(
                            float(bucket_guardrail_penalty_ratio), 6
                        ),
                        historical_profitability_bucket_guardrail_probability_raise=round(
                            float(bucket_guardrail_probability_raise), 6
                        ),
                        historical_profitability_bucket_guardrail_expected_edge_raise=round(
                            float(bucket_guardrail_expected_edge_raise), 6
                        ),
                        historical_profitability_bucket_guardrail_status=bucket_guardrail_status,
                        historical_profitability_bucket_guardrail_sources=";".join(
                            _normalize_text(item) for item in bucket_guardrail_sources if _normalize_text(item)
                        ),
                    )
                )
                continue

            approved_by_underlying[intent.underlying_key] = current_underlying + 1
            decisions.append(
                TemperaturePolicyDecision(
                    intent_id=intent.intent_id,
                    approved=True,
                    decision_reason="approved",
                    decision_notes=",".join(decision_notes_extra),
                    alpha_strength=round(alpha_strength, 6),
                    probability_confidence=round(probability_confidence, 6),
                    expected_edge_net=round(expected_edge_net, 6),
                    base_edge_net=round(base_edge_net, 6),
                    edge_to_risk_ratio=(round(edge_to_risk_ratio, 6) if edge_to_risk_ratio is not None else None),
                    min_alpha_strength_required=min_alpha_strength_required,
                    min_probability_confidence_required=effective_min_probability_confidence,
                    min_expected_edge_net_required=effective_min_expected_edge_net,
                    min_edge_to_risk_ratio_required=min_edge_to_risk_ratio,
                    min_base_edge_net_required=min_base_edge_net,
                    min_probability_breakeven_gap_required=min_probability_breakeven_gap,
                    metar_max_age_minutes_applied=effective_metar_age_limit,
                    metar_local_hour=metar_local_hour,
                    sparse_evidence_hardening_applied=bool(sparse_evidence_probability_raise > 0.0),
                    sparse_evidence_probability_raise=round(float(sparse_evidence_probability_raise), 6),
                    sparse_evidence_expected_edge_raise=round(float(sparse_evidence_expected_edge_raise), 6),
                    sparse_evidence_support_score=round(float(evidence_support_score), 6),
                    sparse_evidence_volatility_penalty=round(float(volatility_penalty_score), 6),
                    historical_quality_penalty_ratio=round(float(historical_penalty_ratio), 6),
                    historical_quality_boost_ratio=round(float(historical_boost_ratio), 6),
                    historical_quality_probability_raise=round(float(historical_probability_raise), 6),
                    historical_quality_expected_edge_raise=round(float(historical_expected_edge_raise), 6),
                    historical_quality_score_adjustment=round(float(historical_score_adjustment), 6),
                    historical_quality_sample_size=int(historical_sample_size),
                    historical_quality_sources=";".join(
                        _normalize_text(item) for item in historical_sources if _normalize_text(item)
                    ),
                    historical_quality_signal_bucket_penalty_ratio=round(
                        float(signal_bucket_penalty_ratio),
                        6,
                    ),
                    historical_quality_signal_bucket_samples=int(signal_bucket_samples),
                    historical_quality_station_bucket_penalty_ratio=round(
                        float(station_bucket_penalty_ratio),
                        6,
                    ),
                    historical_quality_station_bucket_samples=int(station_bucket_samples),
                    historical_quality_local_hour_bucket_penalty_ratio=round(
                        float(local_hour_bucket_penalty_ratio),
                        6,
                    ),
                    historical_quality_local_hour_bucket_samples=int(local_hour_bucket_samples),
                    historical_quality_signal_hard_block_active=bool(signal_hard_block_active),
                    historical_quality_station_hour_hard_block_active=bool(station_hour_hard_block_active),
                    historical_expectancy_hard_block_active=bool(
                        historical_expectancy_hard_block_active
                    ),
                    historical_expectancy_pressure_score=round(
                        float(historical_expectancy_pressure_score),
                        6,
                    ),
                    historical_expectancy_edge_raise=round(
                        float(historical_expectancy_edge_raise),
                        6,
                    ),
                    historical_expectancy_probability_raise=round(
                        float(historical_expectancy_probability_raise),
                        6,
                    ),
                    historical_quality_global_only_pressure_active=bool(
                        historical_global_only_pressure_active
                    ),
                    historical_quality_global_only_adjusted_share=(
                        round(float(historical_global_only_adjusted_share), 6)
                        if isinstance(historical_global_only_adjusted_share, float)
                        else None
                    ),
                    historical_quality_global_only_excess_ratio=round(
                        float(historical_global_only_excess_ratio), 6
                    ),
                    historical_profitability_guardrail_penalty_ratio=round(float(guardrail_penalty_ratio), 6),
                    historical_profitability_guardrail_probability_raise=round(
                        float(guardrail_probability_raise), 6
                    ),
                    historical_profitability_guardrail_expected_edge_raise=round(
                        float(guardrail_expected_edge_raise), 6
                    ),
                    historical_profitability_guardrail_calibration_ratio=(
                        round(float(guardrail_calibration_ratio), 6)
                        if isinstance(guardrail_calibration_ratio, float)
                        else None
                    ),
                    historical_profitability_guardrail_evidence_confidence=(
                        round(float(guardrail_evidence_confidence), 6)
                        if isinstance(guardrail_evidence_confidence, float)
                        else None
                    ),
                    historical_profitability_guardrail_resolved_unique_market_sides=int(
                        guardrail_resolved_unique_market_sides
                    ),
                    historical_profitability_guardrail_repeated_entry_multiplier=(
                        round(float(guardrail_repeated_entry_multiplier), 6)
                        if isinstance(guardrail_repeated_entry_multiplier, float)
                        else None
                    ),
                    historical_profitability_guardrail_concentration_warning=guardrail_concentration_warning,
                    historical_profitability_guardrail_status=guardrail_status,
                    historical_profitability_guardrail_signals=";".join(
                        _normalize_text(item) for item in guardrail_signals if _normalize_text(item)
                    ),
                    historical_profitability_bucket_guardrail_penalty_ratio=round(
                        float(bucket_guardrail_penalty_ratio), 6
                    ),
                    historical_profitability_bucket_guardrail_probability_raise=round(
                        float(bucket_guardrail_probability_raise), 6
                    ),
                    historical_profitability_bucket_guardrail_expected_edge_raise=round(
                        float(bucket_guardrail_expected_edge_raise), 6
                    ),
                    historical_profitability_bucket_guardrail_status=bucket_guardrail_status,
                    historical_profitability_bucket_guardrail_sources=";".join(
                        _normalize_text(item) for item in bucket_guardrail_sources if _normalize_text(item)
                    ),
                )
            )

        return decisions


@dataclass
class TemperatureExecutionBridge:
    contracts_per_order: int = 1

    def _payload(
        self,
        *,
        intent: TemperatureTradeIntent,
        order_group_id: str,
        client_order_id: str,
    ) -> dict[str, Any]:
        contracts = max(1, int(self.contracts_per_order))
        price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))
        payload: dict[str, Any] = {
            "ticker": intent.market_ticker,
            "side": intent.side,
            "action": "buy",
            "count": contracts,
            "client_order_id": _normalize_text(client_order_id),
            "time_in_force": "good_till_canceled",
            "post_only": True,
            "cancel_order_on_pause": True,
            "self_trade_prevention_type": "maker",
        }
        if intent.side == "no":
            payload["no_price_dollars"] = f"{price:.4f}"
        else:
            payload["yes_price_dollars"] = f"{price:.4f}"
        if order_group_id:
            payload["order_group_id"] = order_group_id
        return payload

    def _edge_profile(
        self,
        *,
        intent: TemperatureTradeIntent,
        alpha_strength: float,
        probability_confidence: float | None = None,
    ) -> dict[str, float]:
        return _estimate_temperature_edge_profile(
            intent=intent,
            alpha_strength=alpha_strength,
            probability_confidence=probability_confidence,
        )

    def to_plan(
        self,
        *,
        intent: TemperatureTradeIntent,
        rank: int,
        order_group_id: str,
    ) -> dict[str, Any]:
        contracts = max(1, int(self.contracts_per_order))
        price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))
        client_order_id = _build_deterministic_client_order_id(
            intent=intent,
            order_group_id=order_group_id,
        )
        estimated_entry_cost = round(price * contracts, 4)
        alpha_strength = _intent_alpha_strength(intent)
        confidence_raw, confidence_breakdown = _estimate_temperature_probability_confidence(
            intent=intent,
            alpha_strength=alpha_strength,
        )
        edge_profile = self._edge_profile(
            intent=intent,
            alpha_strength=alpha_strength,
            probability_confidence=confidence_raw,
        )
        edge_net = float(edge_profile["edge_net"])
        confidence_adjusted = round(float(confidence_raw), 3)
        return {
            "planned_at_utc": _normalize_text(intent.captured_at),
            "plan_rank": rank,
            "category": "Climate and Weather",
            "market_ticker": intent.market_ticker,
            "canonical_ticker": intent.underlying_key,
            "canonical_niche": "weather_climate",
            "contract_family": "daily_temperature",
            "source_strategy": "temperature_constraints",
            "side": intent.side,
            "contracts_per_order": contracts,
            "hours_to_close": intent.hours_to_close if intent.hours_to_close is not None else "",
            "confidence": min(0.999, confidence_adjusted),
            "temperature_probability_confidence_raw": round(float(confidence_raw), 6),
            "temperature_probability_confidence_base": confidence_breakdown["base_confidence"],
            "temperature_probability_confidence_status_prior": confidence_breakdown["status_prior"],
            "temperature_probability_confidence_alpha_term": confidence_breakdown["alpha_term"],
            "temperature_probability_confidence_speci_term": confidence_breakdown["speci_term"],
            "temperature_probability_confidence_cross_market_term": confidence_breakdown["cross_market_term"],
            "temperature_probability_confidence_consensus_term": confidence_breakdown["consensus_term"],
            "temperature_probability_confidence_taf_term": confidence_breakdown["taf_term"],
            "temperature_probability_confidence_gap_penalty": confidence_breakdown["gap_penalty"],
            "temperature_alpha_strength": alpha_strength,
            "effective_min_evidence_count": 3,
            "maker_entry_price_dollars": round(price, 4),
            "maker_yes_price_dollars": round(price, 4) if intent.side == "yes" else "",
            "yes_ask_dollars": "",
            "maker_entry_edge_conservative_net_total": round(edge_net, 6),
            "temperature_expected_edge_model_version": "temp_edge_v3_price_aware",
            "temperature_expected_edge_base": edge_profile["base_edge"],
            "temperature_expected_edge_alpha_bonus": edge_profile["alpha_bonus"],
            "temperature_expected_edge_confidence_bonus": edge_profile["confidence_bonus"],
            "temperature_expected_edge_urgency_bonus": edge_profile["urgency_bonus"],
            "temperature_expected_edge_speci_bonus": edge_profile["speci_bonus"],
            "temperature_expected_edge_cross_market_bonus": edge_profile["cross_market_bonus"],
            "temperature_expected_edge_consensus_bonus": edge_profile["consensus_bonus"],
            "temperature_expected_edge_taf_bonus": edge_profile["taf_bonus"],
            "temperature_expected_edge_gap_penalty": edge_profile["gap_penalty"],
            "temperature_expected_edge_friction_penalty": edge_profile["friction_penalty"],
            "temperature_expected_edge_entry_price": edge_profile["entry_price"],
            "temperature_expected_edge_side_win_probability": edge_profile["side_win_probability"],
            "temperature_expected_edge_upside_per_contract": edge_profile["upside_per_contract"],
            "temperature_expected_edge_downside_per_contract": edge_profile["downside_per_contract"],
            "temperature_expected_edge_risk_reward_ratio": edge_profile["risk_reward_ratio"],
            "estimated_entry_cost_dollars": estimated_entry_cost,
            "estimated_entry_fee_dollars": 0.0,
            "temperature_intent_id": intent.intent_id,
            "temperature_underlying_key": intent.underlying_key,
            "temperature_policy_version": intent.policy_version,
            "temperature_spec_hash": intent.spec_hash,
            "temperature_metar_snapshot_sha": intent.metar_snapshot_sha,
            "temperature_metar_observation_time_utc": intent.metar_observation_time_utc,
            "temperature_market_snapshot_seq": intent.market_snapshot_seq if intent.market_snapshot_seq is not None else "",
            "temperature_metric": intent.temperature_metric,
            "temperature_observed_metric_settlement_quantized": (
                intent.observed_metric_settlement_quantized
                if isinstance(intent.observed_metric_settlement_quantized, (int, float))
                else ""
            ),
            "temperature_forecast_upper_bound_settlement_raw": (
                intent.forecast_upper_bound_settlement_raw
                if isinstance(intent.forecast_upper_bound_settlement_raw, (int, float))
                else ""
            ),
            "temperature_forecast_lower_bound_settlement_raw": (
                intent.forecast_lower_bound_settlement_raw
                if isinstance(intent.forecast_lower_bound_settlement_raw, (int, float))
                else ""
            ),
            "temperature_threshold_kind": intent.threshold_kind,
            "temperature_threshold_lower_bound": (
                intent.threshold_lower_bound if isinstance(intent.threshold_lower_bound, (int, float)) else ""
            ),
            "temperature_threshold_upper_bound": (
                intent.threshold_upper_bound if isinstance(intent.threshold_upper_bound, (int, float)) else ""
            ),
            "temperature_yes_possible_overlap": (
                bool(intent.yes_possible_overlap) if isinstance(intent.yes_possible_overlap, bool) else ""
            ),
            "temperature_yes_possible_gap": (
                intent.yes_possible_gap if isinstance(intent.yes_possible_gap, (int, float)) else ""
            ),
            "temperature_primary_signal_margin": (
                intent.primary_signal_margin if isinstance(intent.primary_signal_margin, (int, float)) else ""
            ),
            "temperature_forecast_feasibility_margin": (
                intent.forecast_feasibility_margin
                if isinstance(intent.forecast_feasibility_margin, (int, float))
                else ""
            ),
            "temperature_forecast_model_status": intent.forecast_model_status,
            "temperature_taf_status": intent.taf_status,
            "temperature_taf_volatility_score": (
                intent.taf_volatility_score if isinstance(intent.taf_volatility_score, (int, float)) else ""
            ),
            "temperature_forecast_range_width": (
                intent.forecast_range_width if isinstance(intent.forecast_range_width, (int, float)) else ""
            ),
            "temperature_observed_distance_to_lower_bound": (
                intent.observed_distance_to_lower_bound
                if isinstance(intent.observed_distance_to_lower_bound, (int, float))
                else ""
            ),
            "temperature_observed_distance_to_upper_bound": (
                intent.observed_distance_to_upper_bound
                if isinstance(intent.observed_distance_to_upper_bound, (int, float))
                else ""
            ),
            "temperature_cross_market_family_score": (
                intent.cross_market_family_score if isinstance(intent.cross_market_family_score, (int, float)) else ""
            ),
            "temperature_cross_market_family_zscore": (
                intent.cross_market_family_zscore if isinstance(intent.cross_market_family_zscore, (int, float)) else ""
            ),
            "temperature_cross_market_family_candidate_rank": (
                intent.cross_market_family_candidate_rank
                if isinstance(intent.cross_market_family_candidate_rank, int)
                else ""
            ),
            "temperature_cross_market_family_bucket_size": (
                intent.cross_market_family_bucket_size
                if isinstance(intent.cross_market_family_bucket_size, int)
                else ""
            ),
            "temperature_cross_market_family_signal": intent.cross_market_family_signal,
            "temperature_consensus_profile_support_count": int(intent.consensus_profile_support_count or 0),
            "temperature_consensus_profile_support_ratio": (
                intent.consensus_profile_support_ratio
                if isinstance(intent.consensus_profile_support_ratio, (int, float))
                else ""
            ),
            "temperature_consensus_weighted_support_score": (
                intent.consensus_weighted_support_score
                if isinstance(intent.consensus_weighted_support_score, (int, float))
                else ""
            ),
            "temperature_consensus_weighted_support_ratio": (
                intent.consensus_weighted_support_ratio
                if isinstance(intent.consensus_weighted_support_ratio, (int, float))
                else ""
            ),
            "temperature_consensus_alpha_score": (
                intent.consensus_alpha_score
                if isinstance(intent.consensus_alpha_score, (int, float))
                else ""
            ),
            "temperature_consensus_rank": (
                intent.consensus_rank
                if isinstance(intent.consensus_rank, int)
                else ""
            ),
            "temperature_consensus_profile_names": intent.consensus_profile_names,
            "temperature_range_family_consistency_conflict": bool(intent.range_family_consistency_conflict),
            "temperature_range_family_consistency_conflict_scope": intent.range_family_consistency_conflict_scope,
            "temperature_range_family_consistency_conflict_reason": intent.range_family_consistency_conflict_reason,
            "temperature_speci_recent": bool(intent.speci_recent),
            "temperature_speci_shock_active": bool(intent.speci_shock_active),
            "temperature_speci_shock_confidence": (
                intent.speci_shock_confidence if isinstance(intent.speci_shock_confidence, (int, float)) else ""
            ),
            "temperature_speci_shock_weight": (
                intent.speci_shock_weight if isinstance(intent.speci_shock_weight, (int, float)) else ""
            ),
            "temperature_speci_shock_mode": intent.speci_shock_mode,
            "temperature_speci_shock_trigger_count": int(intent.speci_shock_trigger_count or 0),
            "temperature_speci_shock_trigger_families": intent.speci_shock_trigger_families,
            "temperature_speci_shock_persistence_ok": bool(intent.speci_shock_persistence_ok),
            "temperature_speci_shock_cooldown_blocked": bool(intent.speci_shock_cooldown_blocked),
            "temperature_speci_shock_improvement_hold_active": bool(intent.speci_shock_improvement_hold_active),
            "temperature_speci_shock_delta_temp_c": (
                intent.speci_shock_delta_temp_c if isinstance(intent.speci_shock_delta_temp_c, (int, float)) else ""
            ),
            "temperature_speci_shock_delta_minutes": (
                intent.speci_shock_delta_minutes if isinstance(intent.speci_shock_delta_minutes, (int, float)) else ""
            ),
            "temperature_speci_shock_decay_tau_minutes": (
                intent.speci_shock_decay_tau_minutes
                if isinstance(intent.speci_shock_decay_tau_minutes, (int, float))
                else ""
            ),
            "temperature_client_order_id": client_order_id,
            "order_payload_preview": self._payload(
                intent=intent,
                order_group_id=order_group_id,
                client_order_id=client_order_id,
            ),
        }


@dataclass
class TemperaturePortfolioPlanner:
    max_total_deployed_pct: float = 0.35
    max_same_station_exposure_pct: float = 0.6
    max_same_hour_cluster_exposure_pct: float = 0.6
    max_same_underlying_exposure_pct: float = 0.5
    min_bucket_budget_dollars: float = 0.5
    max_orders_per_station: int = 0
    max_orders_per_underlying: int = 0
    min_unique_stations_per_loop: int = 0
    min_unique_underlyings_per_loop: int = 0
    min_unique_local_hours_per_loop: int = 0
    historical_quality_score_adjust_scale: float = 0.35
    historical_profitability_guardrail_score_penalty_scale: float = 0.55
    historical_profitability_bucket_guardrail_score_penalty_scale: float = 0.4

    def _constraint_priority_weight(self, intent: TemperatureTradeIntent) -> float:
        status = _normalize_text(intent.constraint_status).lower()
        return {
            "yes_impossible": 1.0,
            "no_interval_infeasible": 0.9,
            "no_monotonic_chain": 0.86,
            "yes_likely_locked": 0.7,
            "yes_interval_certain": 0.62,
            "yes_monotonic_chain": 0.58,
        }.get(status, 0.35)

    def _urgency_bonus(self, intent: TemperatureTradeIntent) -> float:
        if not isinstance(intent.hours_to_close, (int, float)):
            return 0.0
        hours = float(intent.hours_to_close)
        if hours <= 1.0:
            return 0.35
        if hours <= 3.0:
            return 0.28
        if hours <= 6.0:
            return 0.2
        if hours <= 12.0:
            return 0.12
        if hours <= 24.0:
            return 0.05
        return -0.02

    def _execution_score(
        self,
        *,
        intent: TemperatureTradeIntent,
        decision: TemperaturePolicyDecision | None,
        order_cost: float,
        total_budget: float,
        min_bucket: float,
    ) -> dict[str, float]:
        alpha_strength = _intent_alpha_strength(intent)
        priority_weight = self._constraint_priority_weight(intent)
        urgency = self._urgency_bonus(intent)
        confidence = max(0.0, min(1.0, float(intent.settlement_confidence_score)))
        confidence_term = (confidence - 0.5) * 1.1
        alpha_term = max(-0.4, min(1.8, alpha_strength * 0.22))
        speci_term = 0.0
        if intent.speci_shock_active:
            speci_term += 0.1
        if isinstance(intent.speci_shock_confidence, (int, float)):
            speci_term += 0.12 * max(0.0, min(1.0, float(intent.speci_shock_confidence)))
        if isinstance(intent.speci_shock_weight, (int, float)):
            speci_term += 0.16 * max(0.0, min(1.0, float(intent.speci_shock_weight)))
        cross_market_term = 0.0
        if isinstance(intent.cross_market_family_zscore, (int, float)):
            cross_market_term += 0.08 * max(-2.0, min(2.0, float(intent.cross_market_family_zscore)))
        if _normalize_text(intent.cross_market_family_signal).lower() == "relative_outlier_high":
            cross_market_term += 0.08
        consensus_term = 0.0
        if isinstance(intent.consensus_alpha_score, (int, float)):
            consensus_term += 0.2 * max(-2.0, min(2.0, float(intent.consensus_alpha_score)))
        if isinstance(intent.consensus_weighted_support_ratio, (int, float)):
            consensus_term += 0.35 * max(0.0, min(1.0, float(intent.consensus_weighted_support_ratio)))
        if isinstance(intent.consensus_profile_support_count, int):
            consensus_term += min(0.18, max(0.0, float(intent.consensus_profile_support_count)) * 0.04)
        gap_penalty = 0.0
        if isinstance(intent.yes_possible_gap, (int, float)):
            gap_penalty += min(0.4, max(0.0, float(intent.yes_possible_gap)) * 0.12)
        if intent.speci_shock_cooldown_blocked:
            gap_penalty += 0.08
        if intent.speci_shock_improvement_hold_active:
            gap_penalty += 0.05
        stale_penalty = 0.0
        freshness_boundary_penalty = 0.0
        if isinstance(intent.metar_observation_age_minutes, (int, float)):
            effective_limit = (
                float(decision.metar_max_age_minutes_applied)
                if isinstance(decision, TemperaturePolicyDecision)
                and isinstance(decision.metar_max_age_minutes_applied, (int, float))
                and float(decision.metar_max_age_minutes_applied) > 0.0
                else None
            )
            if isinstance(effective_limit, float):
                age_ratio = float(intent.metar_observation_age_minutes) / max(1e-9, effective_limit)
                if age_ratio > 0.75:
                    stale_penalty = min(0.24, (age_ratio - 0.75) * 0.55)
                if age_ratio > 0.92:
                    freshness_boundary_penalty = min(0.14, (age_ratio - 0.92) * 1.5)
        budget_scale = max(min_bucket, total_budget * 0.2, 1e-9)
        cost_penalty = min(0.18, max(0.0, order_cost / budget_scale) * 0.04)
        historical_quality_penalty_term = 0.0
        historical_quality_boost_term = 0.0
        historical_profitability_guardrail_penalty_term = 0.0
        historical_profitability_bucket_guardrail_penalty_term = 0.0
        if isinstance(decision, TemperaturePolicyDecision):
            raw_penalty = _parse_float(decision.historical_quality_penalty_ratio)
            raw_boost = _parse_float(decision.historical_quality_boost_ratio)
            score_scale = max(0.0, float(self.historical_quality_score_adjust_scale))
            if isinstance(raw_penalty, float) and raw_penalty > 0.0:
                historical_quality_penalty_term = min(0.55, score_scale * float(raw_penalty))
            if isinstance(raw_boost, float) and raw_boost > 0.0:
                historical_quality_boost_term = min(0.25, score_scale * float(raw_boost) * 0.6)
            raw_score_adjustment = _parse_float(decision.historical_quality_score_adjustment)
            if isinstance(raw_score_adjustment, float):
                if raw_score_adjustment < 0.0:
                    historical_quality_penalty_term = max(
                        historical_quality_penalty_term,
                        min(0.55, abs(raw_score_adjustment)),
                    )
                elif raw_score_adjustment > 0.0:
                    historical_quality_boost_term = max(
                        historical_quality_boost_term,
                        min(0.25, raw_score_adjustment),
                    )
            raw_profitability_guardrail_penalty = _parse_float(
                decision.historical_profitability_guardrail_penalty_ratio
            )
            raw_profitability_bucket_guardrail_penalty = _parse_float(
                decision.historical_profitability_bucket_guardrail_penalty_ratio
            )
            profitability_guardrail_scale = max(
                0.0,
                float(self.historical_profitability_guardrail_score_penalty_scale),
            )
            profitability_bucket_guardrail_scale = max(
                0.0,
                float(self.historical_profitability_bucket_guardrail_score_penalty_scale),
            )
            if (
                isinstance(raw_profitability_guardrail_penalty, float)
                and raw_profitability_guardrail_penalty > 0.0
            ):
                historical_profitability_guardrail_penalty_term = min(
                    0.65,
                    profitability_guardrail_scale * float(raw_profitability_guardrail_penalty),
                )
            if (
                isinstance(raw_profitability_bucket_guardrail_penalty, float)
                and raw_profitability_bucket_guardrail_penalty > 0.0
            ):
                historical_profitability_bucket_guardrail_penalty_term = min(
                    0.45,
                    profitability_bucket_guardrail_scale
                    * float(raw_profitability_bucket_guardrail_penalty),
                )
        score = priority_weight + urgency + confidence_term + alpha_term + speci_term + cross_market_term + consensus_term
        score += historical_quality_boost_term
        score -= (
            gap_penalty
            + stale_penalty
            + freshness_boundary_penalty
            + cost_penalty
            + historical_quality_penalty_term
            + historical_profitability_guardrail_penalty_term
            + historical_profitability_bucket_guardrail_penalty_term
        )
        return {
            "score": round(score, 6),
            "priority_weight": round(priority_weight, 6),
            "urgency": round(urgency, 6),
            "confidence_term": round(confidence_term, 6),
            "alpha_term": round(alpha_term, 6),
            "speci_term": round(speci_term, 6),
            "cross_market_term": round(cross_market_term, 6),
            "consensus_term": round(consensus_term, 6),
            "gap_penalty": round(gap_penalty, 6),
            "stale_penalty": round(stale_penalty, 6),
            "freshness_boundary_penalty": round(freshness_boundary_penalty, 6),
            "cost_penalty": round(cost_penalty, 6),
            "historical_quality_penalty_term": round(historical_quality_penalty_term, 6),
            "historical_quality_boost_term": round(historical_quality_boost_term, 6),
            "historical_profitability_guardrail_penalty_term": round(
                historical_profitability_guardrail_penalty_term,
                6,
            ),
            "historical_profitability_bucket_guardrail_penalty_term": round(
                historical_profitability_bucket_guardrail_penalty_term,
                6,
            ),
        }

    def _intent_local_hour(
        self,
        *,
        intent: TemperatureTradeIntent,
        decision: TemperaturePolicyDecision | None,
    ) -> int | None:
        if isinstance(decision, TemperaturePolicyDecision) and isinstance(decision.metar_local_hour, int):
            if 0 <= int(decision.metar_local_hour) <= 23:
                return int(decision.metar_local_hour)
        observed_ts = _parse_ts(intent.metar_observation_time_utc) or _parse_ts(intent.captured_at)
        if not isinstance(observed_ts, datetime):
            return None
        timezone_name = _normalize_text(intent.settlement_timezone)
        if timezone_name:
            try:
                return int(observed_ts.astimezone(ZoneInfo(timezone_name)).hour)
            except Exception:
                return int(observed_ts.hour)
        return int(observed_ts.hour)

    def select_intents(
        self,
        *,
        intents: list[TemperatureTradeIntent],
        decisions_by_id: dict[str, TemperaturePolicyDecision],
        max_orders: int,
        planning_bankroll_dollars: float,
        daily_risk_cap_dollars: float,
    ) -> tuple[list[TemperatureTradeIntent], dict[str, Any]]:
        safe_max_orders = max(0, int(max_orders))
        if safe_max_orders <= 0:
            return [], {
                "status": "no_capacity",
                "selected_count": 0,
                "dropped_count": len(intents),
                "reason_counts": {"max_orders_zero_or_no_intents": len(intents)},
            }
        if not intents:
            return [], {
                "status": "no_candidates",
                "selected_count": 0,
                "dropped_count": 0,
                "reason_counts": {},
            }

        bankroll = max(0.0, float(planning_bankroll_dollars))
        daily_cap = max(0.0, float(daily_risk_cap_dollars))
        deploy_cap = bankroll * _clamp_fraction(self.max_total_deployed_pct, 0.35)
        if daily_cap > 0.0 and deploy_cap > 0.0:
            total_budget = min(daily_cap, deploy_cap)
        elif daily_cap > 0.0:
            total_budget = daily_cap
        else:
            total_budget = deploy_cap

        if total_budget <= 0.0:
            return [], {
                "status": "budget_zero",
                "reference_bankroll_dollars": bankroll,
                "daily_risk_cap_dollars": daily_cap,
                "max_total_deployed_pct": _clamp_fraction(self.max_total_deployed_pct, 0.35),
                "total_budget_dollars": 0.0,
                "selected_count": 0,
                "dropped_count": len(intents),
                "reason_counts": {"budget_zero": len(intents)},
            }

        station_pct = _clamp_fraction(self.max_same_station_exposure_pct, 0.6)
        hour_pct = _clamp_fraction(self.max_same_hour_cluster_exposure_pct, 0.6)
        underlying_pct = _clamp_fraction(self.max_same_underlying_exposure_pct, 0.5)
        min_bucket = max(0.0, float(self.min_bucket_budget_dollars))
        max_orders_per_station = max(0, int(self.max_orders_per_station))
        max_orders_per_underlying = max(0, int(self.max_orders_per_underlying))
        min_unique_stations_target = min(
            safe_max_orders,
            max(0, int(self.min_unique_stations_per_loop)),
        )
        min_unique_underlyings_target = min(
            safe_max_orders,
            max(0, int(self.min_unique_underlyings_per_loop)),
        )
        min_unique_local_hours_target = min(
            safe_max_orders,
            max(0, int(self.min_unique_local_hours_per_loop)),
        )
        station_cap = max(min_bucket, total_budget * station_pct)
        hour_cap = max(min_bucket, total_budget * hour_pct)
        underlying_cap = max(min_bucket, total_budget * underlying_pct)

        selected: list[TemperatureTradeIntent] = []
        deployed_total = 0.0
        deployed_by_station: dict[str, float] = {}
        deployed_by_hour: dict[int, float] = {}
        deployed_by_underlying: dict[str, float] = {}
        selected_station_order_counts: dict[str, int] = {}
        selected_underlying_order_counts: dict[str, int] = {}
        selected_unique_stations: set[str] = set()
        selected_unique_underlyings: set[str] = set()
        selected_unique_local_hours: set[int] = set()
        reason_counts: Counter[str] = Counter()
        scored_candidates: list[dict[str, Any]] = []
        for intent in intents:
            order_cost = round(
                max(0.0, float(intent.max_entry_price_dollars)) * max(1, int(intent.intended_contracts)),
                6,
            )
            decision = decisions_by_id.get(intent.intent_id)
            hour = self._intent_local_hour(intent=intent, decision=decision)
            station_key = _normalize_text(intent.settlement_station).upper() or "station_unknown"
            underlying_key = _normalize_text(intent.underlying_key) or "underlying_unknown"
            score_breakdown = self._execution_score(
                intent=intent,
                decision=decision,
                order_cost=order_cost,
                total_budget=total_budget,
                min_bucket=min_bucket,
            )
            scored_candidates.append(
                {
                    "intent": intent,
                    "order_cost": order_cost,
                    "station_key": station_key,
                    "underlying_key": underlying_key,
                    "hour": hour,
                    "score_breakdown": score_breakdown,
                }
            )
        scored_candidates.sort(
            key=lambda item: (
                -float(item.get("score_breakdown", {}).get("score") or 0.0),
                float(item.get("order_cost") or 0.0),
                _normalize_text(item.get("intent").market_ticker if isinstance(item.get("intent"), TemperatureTradeIntent) else ""),
            )
        )

        selected_scores: list[float] = []
        for candidate in scored_candidates:
            intent = candidate["intent"]
            order_cost = float(candidate["order_cost"])
            station_key = candidate["station_key"]
            underlying_key = candidate["underlying_key"]
            hour = candidate["hour"]
            if len(selected) >= safe_max_orders:
                reason_counts["max_orders_reached"] += 1
                continue
            if order_cost <= 0.0:
                reason_counts["non_positive_order_cost"] += 1
                continue
            station_introduces_breadth = station_key not in selected_unique_stations
            underlying_introduces_breadth = underlying_key not in selected_unique_underlyings
            hour_introduces_breadth = isinstance(hour, int) and hour not in selected_unique_local_hours
            remaining_slots = safe_max_orders - len(selected)
            pending_station_quota = max(0, min_unique_stations_target - len(selected_unique_stations))
            pending_underlying_quota = max(0, min_unique_underlyings_target - len(selected_unique_underlyings))
            pending_hour_quota = max(0, min_unique_local_hours_target - len(selected_unique_local_hours))
            if (
                pending_station_quota > 0
                and remaining_slots <= pending_station_quota
                and not station_introduces_breadth
            ):
                reason_counts["breadth_station_quota_pending"] += 1
                continue
            if (
                pending_underlying_quota > 0
                and remaining_slots <= pending_underlying_quota
                and not underlying_introduces_breadth
            ):
                reason_counts["breadth_underlying_quota_pending"] += 1
                continue
            if (
                pending_hour_quota > 0
                and remaining_slots <= pending_hour_quota
                and not hour_introduces_breadth
            ):
                reason_counts["breadth_local_hour_quota_pending"] += 1
                continue
            if deployed_total + order_cost > total_budget + 1e-9:
                reason_counts["total_budget_exceeded"] += 1
                continue
            if (
                max_orders_per_station > 0
                and selected_station_order_counts.get(station_key, 0) >= max_orders_per_station
            ):
                reason_counts["station_order_count_cap_reached"] += 1
                continue
            if (
                max_orders_per_underlying > 0
                and selected_underlying_order_counts.get(underlying_key, 0) >= max_orders_per_underlying
            ):
                reason_counts["underlying_order_count_cap_reached"] += 1
                continue
            if deployed_by_station.get(station_key, 0.0) + order_cost > station_cap + 1e-9:
                reason_counts["station_exposure_cap_reached"] += 1
                continue
            if deployed_by_underlying.get(underlying_key, 0.0) + order_cost > underlying_cap + 1e-9:
                reason_counts["underlying_exposure_cap_reached"] += 1
                continue
            if isinstance(hour, int):
                if deployed_by_hour.get(hour, 0.0) + order_cost > hour_cap + 1e-9:
                    reason_counts["hour_cluster_cap_reached"] += 1
                    continue

            selected.append(intent)
            selected_scores.append(float(candidate.get("score_breakdown", {}).get("score") or 0.0))
            deployed_total = round(deployed_total + order_cost, 6)
            deployed_by_station[station_key] = round(deployed_by_station.get(station_key, 0.0) + order_cost, 6)
            deployed_by_underlying[underlying_key] = round(
                deployed_by_underlying.get(underlying_key, 0.0) + order_cost,
                6,
            )
            selected_station_order_counts[station_key] = int(selected_station_order_counts.get(station_key, 0) + 1)
            selected_underlying_order_counts[underlying_key] = int(
                selected_underlying_order_counts.get(underlying_key, 0) + 1
            )
            selected_unique_stations.add(station_key)
            selected_unique_underlyings.add(underlying_key)
            if isinstance(hour, int):
                deployed_by_hour[hour] = round(deployed_by_hour.get(hour, 0.0) + order_cost, 6)
                selected_unique_local_hours.add(hour)

        top_candidate_scores = [
            {
                "intent_id": _normalize_text(candidate.get("intent").intent_id if isinstance(candidate.get("intent"), TemperatureTradeIntent) else ""),
                "market_ticker": _normalize_text(candidate.get("intent").market_ticker if isinstance(candidate.get("intent"), TemperatureTradeIntent) else ""),
                "score": round(float(candidate.get("score_breakdown", {}).get("score") or 0.0), 6),
                "order_cost_dollars": round(float(candidate.get("order_cost") or 0.0), 6),
                "score_breakdown": candidate.get("score_breakdown", {}),
            }
            for candidate in scored_candidates[:20]
        ]

        return selected, {
            "status": "ready" if selected else "no_candidates",
            "optimization_mode": "score_aware_greedy_v1",
            "reference_bankroll_dollars": round(bankroll, 6),
            "daily_risk_cap_dollars": round(daily_cap, 6),
            "max_total_deployed_pct": round(_clamp_fraction(self.max_total_deployed_pct, 0.35), 6),
            "max_same_station_exposure_pct": round(station_pct, 6),
            "max_same_hour_cluster_exposure_pct": round(hour_pct, 6),
            "max_same_underlying_exposure_pct": round(underlying_pct, 6),
            "total_budget_dollars": round(total_budget, 6),
            "station_cap_dollars": round(station_cap, 6),
            "hour_cap_dollars": round(hour_cap, 6),
            "underlying_cap_dollars": round(underlying_cap, 6),
            "max_orders": safe_max_orders,
            "candidate_count": len(scored_candidates),
            "selected_count": len(selected),
            "dropped_count": max(0, len(intents) - len(selected)),
            "deployed_total_dollars": round(deployed_total, 6),
            "selected_score_total": round(sum(selected_scores), 6),
            "selected_score_avg": round(sum(selected_scores) / len(selected_scores), 6) if selected_scores else None,
            "top_candidate_scores": top_candidate_scores,
            "deployed_by_station": dict(sorted(deployed_by_station.items())),
            "deployed_by_hour": dict(sorted(deployed_by_hour.items())),
            "deployed_by_underlying": dict(sorted(deployed_by_underlying.items())),
            "selected_station_order_counts": dict(sorted(selected_station_order_counts.items())),
            "selected_underlying_order_counts": dict(sorted(selected_underlying_order_counts.items())),
            "selected_unique_station_count": len(selected_unique_stations),
            "selected_unique_underlying_count": len(selected_unique_underlyings),
            "selected_unique_local_hour_count": len(selected_unique_local_hours),
            "min_unique_stations_target": int(min_unique_stations_target),
            "min_unique_underlyings_target": int(min_unique_underlyings_target),
            "min_unique_local_hours_target": int(min_unique_local_hours_target),
            "max_orders_per_station": int(max_orders_per_station),
            "max_orders_per_underlying": int(max_orders_per_underlying),
            "reason_counts": dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        }


def build_temperature_trade_intents(
    *,
    constraint_rows: list[dict[str, str]],
    specs_by_ticker: dict[str, dict[str, str]],
    metar_context: dict[str, Any],
    market_sequences: dict[str, int | None],
    alpha_consensus_by_market_side: dict[str, dict[str, Any]] | None = None,
    policy_version: str,
    contracts_per_order: int,
    yes_max_entry_price_dollars: float,
    no_max_entry_price_dollars: float,
    now: datetime,
) -> list[TemperatureTradeIntent]:
    expanded_constraint_rows = _derive_breadth_constraint_rows(constraint_rows=constraint_rows)
    latest_by_station = (
        metar_context.get("latest_by_station") if isinstance(metar_context.get("latest_by_station"), dict) else {}
    )
    metar_snapshot_sha = _normalize_text(metar_context.get("raw_sha256"))

    intents: list[TemperatureTradeIntent] = []
    for row in expanded_constraint_rows:
        constraint_status = _normalize_text(row.get("constraint_status")).lower()
        if constraint_status not in _ACTIONABLE_CONSTRAINTS:
            continue

        market_ticker = _normalize_text(row.get("market_ticker"))
        if not market_ticker:
            continue
        spec_row = specs_by_ticker.get(market_ticker, {})
        settlement_station = _normalize_text(row.get("settlement_station")) or _normalize_text(
            spec_row.get("settlement_station")
        )
        target_date_local = _normalize_text(row.get("target_date_local")) or _normalize_text(
            spec_row.get("target_date_local")
        )
        series_ticker = _normalize_text(row.get("series_ticker")) or _normalize_text(spec_row.get("series_ticker"))
        event_ticker = _normalize_text(row.get("event_ticker")) or _normalize_text(spec_row.get("event_ticker"))
        underlying_key = _build_underlying_key(
            series_ticker=series_ticker,
            settlement_station=settlement_station,
            target_date_local=target_date_local,
        )
        side = _side_from_constraint(constraint_status)
        max_entry_price = float(yes_max_entry_price_dollars) if side == "yes" else float(no_max_entry_price_dollars)
        market_side_key = f"{market_ticker}|{side}"
        consensus_entry = (
            alpha_consensus_by_market_side.get(market_side_key, {})
            if isinstance(alpha_consensus_by_market_side, dict)
            else {}
        )
        if not isinstance(consensus_entry, dict):
            consensus_entry = {}

        latest_station = latest_by_station.get(settlement_station) if settlement_station else None
        if not isinstance(latest_station, dict):
            latest_station = {}
        metar_observation_time = _normalize_text(latest_station.get("observation_time_utc"))
        metar_age = _metar_observation_age_minutes(observation_time_utc=metar_observation_time, now=now)

        close_time = _normalize_text(spec_row.get("close_time"))
        confidence = _parse_float(row.get("settlement_confidence_score"))
        if confidence is None:
            confidence = _parse_float(spec_row.get("settlement_confidence_score"))
        if confidence is None:
            confidence = 0.0

        observed_metric = _parse_float(row.get("observed_metric_settlement_quantized"))
        if observed_metric is None:
            observed_metric = _parse_float(row.get("observed_max_settlement_quantized"))
        forecast_upper = _parse_float(row.get("forecast_upper_bound_settlement_raw"))
        forecast_lower = _parse_float(row.get("forecast_lower_bound_settlement_raw"))
        threshold_kind = _normalize_text(row.get("threshold_kind")).lower()
        threshold_lower_bound = _parse_float(row.get("threshold_lower_bound"))
        threshold_upper_bound = _parse_float(row.get("threshold_upper_bound"))
        yes_possible_overlap_raw = _normalize_text(row.get("yes_possible_overlap")).lower()
        yes_possible_overlap: bool | None = None
        if yes_possible_overlap_raw in {"1", "true", "yes"}:
            yes_possible_overlap = True
        elif yes_possible_overlap_raw in {"0", "false", "no"}:
            yes_possible_overlap = False
        yes_possible_gap = _parse_float(row.get("yes_possible_gap"))
        primary_signal_margin = _parse_float(row.get("primary_signal_margin"))
        forecast_feasibility_margin = _parse_float(row.get("forecast_feasibility_margin"))
        forecast_model_status = _normalize_text(row.get("forecast_model_status")).lower()
        taf_status = _normalize_text(row.get("taf_status")).lower()
        taf_volatility_score = _parse_float(row.get("taf_volatility_score"))
        forecast_range_width = _parse_float(row.get("forecast_range_width"))
        observed_distance_to_lower_bound = _parse_float(row.get("observed_distance_to_lower_bound"))
        observed_distance_to_upper_bound = _parse_float(row.get("observed_distance_to_upper_bound"))
        cross_market_family_score = _parse_float(row.get("cross_market_family_score"))
        cross_market_family_zscore = _parse_float(row.get("cross_market_family_zscore"))
        cross_market_family_candidate_rank = _parse_int(row.get("cross_market_family_candidate_rank"))
        cross_market_family_bucket_size = _parse_int(row.get("cross_market_family_bucket_size"))
        cross_market_family_signal = _normalize_text(row.get("cross_market_family_signal")).lower()
        temperature_metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
        speci_recent = _normalize_text(row.get("speci_recent")).lower() in {"1", "true", "yes"}
        speci_shock_active = _normalize_text(row.get("speci_shock_active")).lower() in {"1", "true", "yes"}
        speci_shock_confidence = _parse_float(row.get("speci_shock_confidence"))
        speci_shock_weight = _parse_float(row.get("speci_shock_weight"))
        speci_shock_mode = _normalize_text(row.get("speci_shock_mode")).lower()
        speci_shock_trigger_count = _parse_int(row.get("speci_shock_trigger_count")) or 0
        speci_shock_trigger_families = _normalize_text(row.get("speci_shock_trigger_families"))
        speci_shock_persistence_ok = (
            _normalize_text(row.get("speci_shock_persistence_ok")).lower() in {"1", "true", "yes"}
        )
        speci_shock_cooldown_blocked = (
            _normalize_text(row.get("speci_shock_cooldown_blocked")).lower() in {"1", "true", "yes"}
        )
        speci_shock_improvement_hold_active = (
            _normalize_text(row.get("speci_shock_improvement_hold_active")).lower() in {"1", "true", "yes"}
        )
        speci_shock_delta_temp_c = _parse_float(row.get("speci_shock_delta_temp_c"))
        speci_shock_delta_minutes = _parse_float(row.get("speci_shock_delta_minutes"))
        speci_shock_decay_tau_minutes = _parse_float(row.get("speci_shock_decay_tau_minutes"))
        market_seq = market_sequences.get(market_ticker)
        spec_hash = _build_spec_hash(spec_row if spec_row else row)
        intent_id = _build_intent_id(
            market_ticker=market_ticker,
            constraint_status=constraint_status,
            spec_hash=spec_hash,
            metar_snapshot_sha=metar_snapshot_sha,
            market_snapshot_seq=market_seq,
            policy_version=policy_version,
        )

        intents.append(
            TemperatureTradeIntent(
                intent_id=intent_id,
                captured_at=now.isoformat(),
                policy_version=policy_version,
                underlying_key=underlying_key,
                series_ticker=series_ticker,
                event_ticker=event_ticker,
                market_ticker=market_ticker,
                market_title=_normalize_text(row.get("market_title")) or _normalize_text(spec_row.get("market_title")),
                settlement_station=settlement_station,
                settlement_timezone=_normalize_text(row.get("settlement_timezone"))
                or _normalize_text(spec_row.get("settlement_timezone")),
                target_date_local=target_date_local,
                constraint_status=constraint_status,
                constraint_reason=_normalize_text(row.get("constraint_reason")),
                side=side,
                max_entry_price_dollars=round(max(0.01, min(0.99, max_entry_price)), 4),
                intended_contracts=max(1, int(contracts_per_order)),
                settlement_confidence_score=round(max(0.0, min(1.0, confidence)), 6),
                observed_max_settlement_quantized=observed_metric,
                close_time=close_time,
                hours_to_close=_hours_to_close(close_time=close_time, now=now),
                spec_hash=spec_hash,
                metar_snapshot_sha=metar_snapshot_sha,
                metar_observation_time_utc=metar_observation_time,
                metar_observation_age_minutes=metar_age,
                market_snapshot_seq=market_seq,
                temperature_metric=temperature_metric,
                observed_metric_settlement_quantized=observed_metric,
                forecast_upper_bound_settlement_raw=forecast_upper,
                forecast_lower_bound_settlement_raw=forecast_lower,
                threshold_kind=threshold_kind,
                threshold_lower_bound=threshold_lower_bound,
                threshold_upper_bound=threshold_upper_bound,
                yes_possible_overlap=yes_possible_overlap,
                yes_possible_gap=yes_possible_gap,
                primary_signal_margin=primary_signal_margin,
                forecast_feasibility_margin=forecast_feasibility_margin,
                forecast_model_status=forecast_model_status,
                taf_status=taf_status,
                taf_volatility_score=taf_volatility_score,
                forecast_range_width=forecast_range_width,
                observed_distance_to_lower_bound=observed_distance_to_lower_bound,
                observed_distance_to_upper_bound=observed_distance_to_upper_bound,
                cross_market_family_score=cross_market_family_score,
                cross_market_family_zscore=cross_market_family_zscore,
                cross_market_family_candidate_rank=cross_market_family_candidate_rank,
                cross_market_family_bucket_size=cross_market_family_bucket_size,
                cross_market_family_signal=cross_market_family_signal,
                consensus_profile_support_count=max(0, _parse_int(consensus_entry.get("profile_support_count")) or 0),
                consensus_profile_support_ratio=_parse_float(consensus_entry.get("profile_support_ratio")),
                consensus_weighted_support_score=_parse_float(consensus_entry.get("weighted_support_score")),
                consensus_weighted_support_ratio=_parse_float(consensus_entry.get("weighted_support_ratio")),
                consensus_alpha_score=_parse_float(consensus_entry.get("consensus_alpha_score")),
                consensus_rank=_parse_int(consensus_entry.get("consensus_rank")),
                consensus_profile_names=_normalize_text(consensus_entry.get("profile_names")),
                speci_recent=speci_recent,
                speci_shock_active=speci_shock_active,
                speci_shock_confidence=speci_shock_confidence,
                speci_shock_weight=speci_shock_weight,
                speci_shock_mode=speci_shock_mode,
                speci_shock_trigger_count=max(0, int(speci_shock_trigger_count)),
                speci_shock_trigger_families=speci_shock_trigger_families,
                speci_shock_persistence_ok=speci_shock_persistence_ok,
                speci_shock_cooldown_blocked=speci_shock_cooldown_blocked,
                speci_shock_improvement_hold_active=speci_shock_improvement_hold_active,
                speci_shock_delta_temp_c=speci_shock_delta_temp_c,
                speci_shock_delta_minutes=speci_shock_delta_minutes,
                speci_shock_decay_tau_minutes=speci_shock_decay_tau_minutes,
            )
        )

    intents.sort(
        key=lambda intent: (
            _CONSTRAINT_PRIORITY.get(intent.constraint_status, 99),
            0 if intent.speci_shock_active else 1,
            0 if intent.speci_recent else 1,
            -(float(intent.speci_shock_confidence) if isinstance(intent.speci_shock_confidence, (int, float)) else 0.0),
            -(float(intent.speci_shock_weight) if isinstance(intent.speci_shock_weight, (int, float)) else 0.0),
            -(float(intent.consensus_alpha_score) if isinstance(intent.consensus_alpha_score, (int, float)) else 0.0),
            -(float(intent.consensus_weighted_support_ratio) if isinstance(intent.consensus_weighted_support_ratio, (int, float)) else 0.0),
            -int(intent.consensus_profile_support_count or 0),
            -_intent_alpha_strength(intent),
            intent.hours_to_close if intent.hours_to_close is not None else 9999.0,
            intent.market_ticker,
        )
    )
    deduped: list[TemperatureTradeIntent] = []
    seen_market_side: set[tuple[str, str]] = set()
    for intent in intents:
        key = (_normalize_text(intent.market_ticker), _normalize_text(intent.side))
        if key in seen_market_side:
            continue
        seen_market_side.add(key)
        deduped.append(intent)
    return deduped


def revalidate_temperature_trade_intents(
    *,
    intents: list[TemperatureTradeIntent],
    output_dir: str,
    specs_csv: str | None,
    metar_summary_json: str | None,
    metar_state_json: str | None,
    ws_state_json: str | None,
    require_market_snapshot_seq: bool,
    require_metar_snapshot_sha: bool,
) -> tuple[list[TemperatureTradeIntent], list[dict[str, Any]], dict[str, Any]]:
    if not intents:
        return [], [], {
            "specs_csv_used": _normalize_text(specs_csv),
            "metar_summary_json_used": _normalize_text(metar_summary_json),
            "metar_state_json_used": _normalize_text(metar_state_json),
            "ws_state_json_used": _normalize_text(ws_state_json),
            "market_count": 0,
        }

    specs_path = Path(_normalize_text(specs_csv)) if _normalize_text(specs_csv) else None
    specs_rows = _read_csv_rows(specs_path) if specs_path is not None else []
    specs_by_ticker = _build_specs_by_ticker(specs_rows)

    metar_context = _load_metar_context(
        output_dir=output_dir,
        metar_summary_json=metar_summary_json,
        metar_state_json=metar_state_json,
    )
    current_metar_snapshot_sha = _normalize_text(metar_context.get("raw_sha256"))
    latest_by_station = metar_context.get("latest_by_station")
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}

    ws_path, market_sequences, _ = _load_market_sequences(
        ws_state_json=ws_state_json,
        output_dir=output_dir,
    )

    valid: list[TemperatureTradeIntent] = []
    invalid: list[dict[str, Any]] = []

    for intent in intents:
        reasons: list[str] = []

        current_spec_row = specs_by_ticker.get(intent.market_ticker, {})
        current_spec_hash = _build_spec_hash(current_spec_row) if current_spec_row else ""
        if not current_spec_hash:
            reasons.append("spec_missing_on_revalidate")
        elif current_spec_hash != intent.spec_hash:
            reasons.append("spec_hash_changed")

        current_seq = market_sequences.get(intent.market_ticker)
        if require_market_snapshot_seq:
            if current_seq is None:
                reasons.append("market_snapshot_seq_missing_on_revalidate")
            elif intent.market_snapshot_seq is None:
                reasons.append("intent_missing_market_snapshot_seq")
            elif int(current_seq) != int(intent.market_snapshot_seq):
                reasons.append("market_snapshot_seq_changed")

        if require_metar_snapshot_sha:
            if not current_metar_snapshot_sha:
                reasons.append("metar_snapshot_sha_missing_on_revalidate")
            elif not intent.metar_snapshot_sha:
                reasons.append("intent_missing_metar_snapshot_sha")
            elif current_metar_snapshot_sha != intent.metar_snapshot_sha:
                reasons.append("metar_snapshot_sha_changed")
        elif current_metar_snapshot_sha and intent.metar_snapshot_sha and current_metar_snapshot_sha != intent.metar_snapshot_sha:
            reasons.append("metar_snapshot_sha_changed")

        current_station_payload = latest_by_station.get(intent.settlement_station)
        if not isinstance(current_station_payload, dict):
            current_station_payload = {}
        current_obs_time = _parse_ts(current_station_payload.get("observation_time_utc"))
        intent_obs_time = _parse_ts(intent.metar_observation_time_utc)
        if current_obs_time is not None and intent_obs_time is not None and current_obs_time > intent_obs_time:
            reasons.append("metar_observation_advanced")

        if reasons:
            invalid.append(
                {
                    "intent_id": intent.intent_id,
                    "market_ticker": intent.market_ticker,
                    "underlying_key": intent.underlying_key,
                    "reason": reasons[0],
                    "reasons": reasons,
                    "intent_market_snapshot_seq": intent.market_snapshot_seq,
                    "current_market_snapshot_seq": current_seq,
                    "intent_metar_snapshot_sha": intent.metar_snapshot_sha,
                    "current_metar_snapshot_sha": current_metar_snapshot_sha,
                    "intent_spec_hash": intent.spec_hash,
                    "current_spec_hash": current_spec_hash,
                }
            )
            continue

        valid.append(intent)

    revalidation_meta = {
        "specs_csv_used": str(specs_path) if specs_path is not None else "",
        "metar_summary_json_used": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json_used": _normalize_text(metar_context.get("state_path")),
        "ws_state_json_used": ws_path,
        "market_count": len(market_sequences),
        "metar_snapshot_sha": current_metar_snapshot_sha,
    }
    return valid, invalid, revalidation_meta


def _build_order_group_id(*, metar_snapshot_sha: str, captured_at: datetime) -> str:
    normalized = _normalize_text(metar_snapshot_sha)
    if normalized:
        return f"temp-{normalized[:20]}"
    return f"temp-{captured_at.strftime('%Y%m%d%H%M%S')}"


def _resolve_trader_output_stamp(*, output_dir: Path, captured_at: datetime) -> str:
    base_stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = 0
    while True:
        stamp = base_stamp if suffix <= 0 else f"{base_stamp}_{suffix:02d}"
        artifact_names = (
            f"kalshi_temperature_trade_intents_{stamp}.csv",
            f"kalshi_temperature_trade_plan_{stamp}.csv",
            f"kalshi_temperature_finalization_snapshot_{stamp}.json",
            f"kalshi_temperature_trade_plan_summary_{stamp}.json",
            f"kalshi_temperature_trade_intents_summary_{stamp}.json",
        )
        if not any((output_dir / name).exists() for name in artifact_names):
            return stamp
        suffix += 1


def _plan_file_epoch_hint(path: Path) -> tuple[float | None, str]:
    stem = _normalize_text(path.stem)
    match = re.fullmatch(r"kalshi_temperature_trade_plan_(\d{8}_\d{6})(?:_\d+)?", stem)
    if not match:
        return None, ""
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None, ""
    return float(parsed.timestamp()), parsed.isoformat()


def _plan_row_epoch_hint(
    row: dict[str, Any],
    *,
    fallback_epoch: float,
    fallback_iso: str,
) -> tuple[float, str]:
    for key in ("planned_at_utc", "captured_at", "planned_at"):
        parsed = _parse_ts(row.get(key))
        if isinstance(parsed, datetime):
            normalized = parsed.astimezone(timezone.utc)
            return float(normalized.timestamp()), normalized.isoformat()
    return float(fallback_epoch), fallback_iso


def _build_deterministic_client_order_id(
    *,
    intent: TemperatureTradeIntent,
    order_group_id: str,
) -> str:
    seq = intent.market_snapshot_seq if intent.market_snapshot_seq is not None else 0
    raw = "|".join(
        (
            intent.intent_id,
            intent.policy_version,
            intent.market_ticker,
            intent.side,
            str(seq),
            _normalize_text(order_group_id),
        )
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"temp-{intent.intent_id}-{digest}"


def _load_recent_market_side_plan_index(
    *,
    output_dir: str,
    now_utc: datetime,
    cooldown_minutes: float,
    max_history_files: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, int], dict[str, Any]]:
    safe_window_minutes = max(0.0, float(cooldown_minutes))
    safe_max_history_files = max(1, int(max_history_files))
    if safe_window_minutes <= 0.0:
        return {}, {}, {
            "enabled": False,
            "cooldown_minutes": safe_window_minutes,
            "history_window_minutes": safe_window_minutes,
            "history_files_considered": 0,
            "history_files_scanned": 0,
            "history_rows_scanned": 0,
            "history_index_count": 0,
            "history_plan_count_index_size": 0,
            "status": "disabled",
        }

    cutoff_epoch = (now_utc - timedelta(minutes=safe_window_minutes)).timestamp()
    # Filenames are timestamped (YYYYMMDD_HHMMSS, optionally with a uniqueness
    # suffix), so lexical ordering remains chronological and cheaper than
    # stat-sorting every historical file.
    candidates = sorted(
        Path(output_dir).glob("kalshi_temperature_trade_plan_*.csv"),
        reverse=True,
    )
    considered_paths = candidates[:safe_max_history_files]
    latest_by_market_side: dict[str, dict[str, Any]] = {}
    plan_count_by_market_side: dict[str, int] = {}
    files_scanned = 0
    rows_scanned = 0

    for plan_path in considered_paths:
        fallback_epoch: float | None = None
        fallback_iso = ""
        try:
            fallback_epoch = float(plan_path.stat().st_mtime)
            fallback_iso = datetime.fromtimestamp(fallback_epoch, tz=timezone.utc).isoformat()
        except OSError:
            fallback_epoch = None

        file_hint_epoch, file_hint_iso = _plan_file_epoch_hint(plan_path)
        effective_file_epoch = (
            float(file_hint_epoch)
            if isinstance(file_hint_epoch, (int, float)) and math.isfinite(float(file_hint_epoch))
            else (float(fallback_epoch) if isinstance(fallback_epoch, (int, float)) else None)
        )
        effective_file_iso = file_hint_iso or fallback_iso
        if not isinstance(effective_file_epoch, (int, float)):
            continue
        # Candidate paths are lexically ordered by timestamp in filename, so once
        # file-level time hints are older than cutoff, we can stop scanning.
        if float(effective_file_epoch) < cutoff_epoch:
            break
        rows = _read_csv_rows(plan_path)
        files_scanned += 1
        if not rows:
            continue
        rows_scanned += len(rows)
        for row in rows:
            market_ticker = _normalize_text(row.get("market_ticker"))
            side = _normalize_text(row.get("side")).lower()
            if not market_ticker or side not in {"yes", "no"}:
                continue
            row_epoch, row_planned_at_utc = _plan_row_epoch_hint(
                row,
                fallback_epoch=float(effective_file_epoch),
                fallback_iso=effective_file_iso,
            )
            if row_epoch < cutoff_epoch:
                continue
            market_side_key = f"{market_ticker}|{side}"
            plan_count_by_market_side[market_side_key] = plan_count_by_market_side.get(market_side_key, 0) + 1
            existing = latest_by_market_side.get(market_side_key)
            existing_epoch = float(existing.get("planned_at_epoch") or 0.0) if isinstance(existing, dict) else 0.0
            if existing is not None and existing_epoch >= row_epoch:
                continue
            latest_by_market_side[market_side_key] = {
                "market_side_key": market_side_key,
                "market_ticker": market_ticker,
                "side": side,
                "planned_at_epoch": float(row_epoch),
                "planned_at_utc": row_planned_at_utc,
                "maker_entry_price_dollars": _parse_float(row.get("maker_entry_price_dollars")),
                "temperature_alpha_strength": _parse_float(row.get("temperature_alpha_strength")),
                "confidence": _parse_float(row.get("confidence")),
                "temperature_observed_metric_settlement_quantized": _parse_float(
                    row.get("temperature_observed_metric_settlement_quantized")
                ),
                "temperature_metar_observation_time_utc": _normalize_text(
                    row.get("temperature_metar_observation_time_utc")
                ),
                "temperature_client_order_id": _normalize_text(row.get("temperature_client_order_id")),
                "temperature_intent_id": _normalize_text(row.get("temperature_intent_id")),
            }

    return latest_by_market_side, plan_count_by_market_side, {
        "enabled": True,
        "cooldown_minutes": round(safe_window_minutes, 6),
        "history_window_minutes": round(safe_window_minutes, 6),
        "history_files_considered": len(considered_paths),
        "history_files_scanned": files_scanned,
        "history_rows_scanned": rows_scanned,
        "history_index_count": len(latest_by_market_side),
        "history_plan_count_index_size": len(plan_count_by_market_side),
        "status": "ready" if latest_by_market_side else "no_recent_history",
    }


def _apply_market_side_replan_cooldown(
    *,
    intents: list[TemperatureTradeIntent],
    output_dir: str,
    now_utc: datetime,
    cooldown_minutes: float,
    repeat_window_minutes: float,
    max_plans_per_window: int,
    price_change_override_dollars: float,
    alpha_change_override: float,
    confidence_change_override: float,
    min_observation_advance_minutes: float,
    max_history_files: int,
    min_orders_backstop: int,
) -> tuple[list[TemperatureTradeIntent], dict[str, Any]]:
    safe_cooldown_minutes = max(0.0, float(cooldown_minutes))
    safe_repeat_window_minutes = max(0.0, float(repeat_window_minutes))
    safe_max_plans_per_window = max(0, int(max_plans_per_window))
    safe_price_delta = max(0.0, float(price_change_override_dollars))
    safe_alpha_delta = max(0.0, float(alpha_change_override))
    safe_conf_delta = max(0.0, float(confidence_change_override))
    safe_min_observation_advance = max(0.0, float(min_observation_advance_minutes))
    safe_max_history_files = max(1, int(max_history_files))
    safe_min_orders_backstop = max(0, int(min_orders_backstop))
    history_window_minutes = max(safe_cooldown_minutes, safe_repeat_window_minutes)
    recent_by_market_side, recent_plan_counts_by_market_side, history_meta = _load_recent_market_side_plan_index(
        output_dir=output_dir,
        now_utc=now_utc,
        cooldown_minutes=history_window_minutes,
        max_history_files=safe_max_history_files,
    )
    if safe_cooldown_minutes <= 0.0 and safe_max_plans_per_window <= 0:
        unique_market_side_count = len(
            {
                f"{_normalize_text(intent.market_ticker)}|{_normalize_text(intent.side).lower()}"
                for intent in intents
                if _normalize_text(intent.market_ticker) and _normalize_text(intent.side)
            }
        )
        unique_underlying_count = len(
            {_normalize_text(intent.underlying_key) for intent in intents if _normalize_text(intent.underlying_key)}
        )
        repeat_multiplier = (
            float(len(intents)) / float(unique_market_side_count)
            if unique_market_side_count > 0
            else 0.0
        )
        return intents, {
            **history_meta,
            "input_count": len(intents),
            "deduped_input_count": len(intents),
            "kept_count": len(intents),
            "blocked_count": 0,
            "override_count": 0,
            "backstop_released_count": 0,
            "backstop_release_suppressed": False,
            "backstop_release_suppressed_reason": "",
            "backstop_release_qualified_count": 0,
            "backstop_release_unqualified_count": 0,
            "backstop_release_override_trigger_min_elapsed_minutes": 0.0,
            "min_orders_backstop": safe_min_orders_backstop,
            "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
            "max_plans_per_window": safe_max_plans_per_window,
            "repeat_cap_blocked_count": 0,
            "min_observation_advance_minutes": safe_min_observation_advance,
            "input_unique_market_sides": unique_market_side_count,
            "input_unique_underlyings": unique_underlying_count,
            "input_repeat_multiplier": round(repeat_multiplier, 6),
            "same_cycle_duplicate_count": 0,
            "same_cycle_duplicate_market_side_count": 0,
            "blocked_reason_counts": {},
            "override_reason_counts": {},
            "blocked_top": [],
            "same_cycle_duplicate_top": [],
        }

    if not intents:
        return [], {
            **history_meta,
            "input_count": 0,
            "deduped_input_count": 0,
            "kept_count": 0,
            "blocked_count": 0,
            "override_count": 0,
            "backstop_released_count": 0,
            "backstop_release_suppressed": False,
            "backstop_release_suppressed_reason": "",
            "backstop_release_qualified_count": 0,
            "backstop_release_unqualified_count": 0,
            "backstop_release_override_trigger_min_elapsed_minutes": 0.0,
            "min_orders_backstop": safe_min_orders_backstop,
            "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
            "max_plans_per_window": safe_max_plans_per_window,
            "repeat_cap_blocked_count": 0,
            "min_observation_advance_minutes": safe_min_observation_advance,
            "input_unique_market_sides": 0,
            "input_unique_underlyings": 0,
            "input_repeat_multiplier": 0.0,
            "same_cycle_duplicate_count": 0,
            "same_cycle_duplicate_market_side_count": 0,
            "blocked_reason_counts": {},
            "override_reason_counts": {},
            "blocked_top": [],
            "same_cycle_duplicate_top": [],
        }

    input_unique_market_side_keys = {
        f"{_normalize_text(intent.market_ticker)}|{_normalize_text(intent.side).lower()}"
        for intent in intents
        if _normalize_text(intent.market_ticker) and _normalize_text(intent.side)
    }
    input_unique_underlying_keys = {
        _normalize_text(intent.underlying_key) for intent in intents if _normalize_text(intent.underlying_key)
    }
    input_unique_market_side_count = len(input_unique_market_side_keys)
    input_unique_underlying_count = len(input_unique_underlying_keys)
    input_repeat_multiplier = (
        float(len(intents)) / float(input_unique_market_side_count) if input_unique_market_side_count > 0 else 0.0
    )

    deduped_intents: list[TemperatureTradeIntent] = []
    same_cycle_duplicate_rows: list[dict[str, Any]] = []
    same_cycle_duplicate_reason_counts: Counter[str] = Counter()
    seen_market_sides: set[str] = set()
    for intent in intents:
        market_side_key = f"{_normalize_text(intent.market_ticker)}|{_normalize_text(intent.side).lower()}"
        if market_side_key in seen_market_sides:
            same_cycle_duplicate_reason_counts["duplicate_market_side_same_cycle"] += 1
            same_cycle_duplicate_rows.append(
                {
                    "market_ticker": intent.market_ticker,
                    "side": intent.side,
                    "underlying_key": intent.underlying_key,
                    "intent_id": intent.intent_id,
                    "constraint_status": intent.constraint_status,
                    "settlement_station": intent.settlement_station,
                    "reason": "duplicate_market_side_same_cycle",
                }
            )
            continue
        seen_market_sides.add(market_side_key)
        deduped_intents.append(intent)

    kept: list[TemperatureTradeIntent] = []
    blocked: list[dict[str, Any]] = []
    blocked_intents: list[TemperatureTradeIntent] = []
    blocked_intent_rows: list[dict[str, Any]] = []
    blocked_reasons: Counter[str] = Counter()
    override_reasons: Counter[str] = Counter()
    override_suppressed_reasons: Counter[str] = Counter()
    override_count = 0
    repeat_cap_override_count = 0
    backstop_released_count = 0
    repeat_pressure_mode = (
        input_unique_market_side_count > 0
        and input_repeat_multiplier >= 1.6
        and input_unique_market_side_count <= 8
        and input_unique_underlying_count <= 5
    )
    effective_price_delta = float(safe_price_delta)
    effective_alpha_delta = float(safe_alpha_delta)
    effective_conf_delta = float(safe_conf_delta)
    effective_min_observation_advance = float(safe_min_observation_advance)
    if repeat_pressure_mode:
        # Under repeated-entry pressure require materially stronger evidence
        # before allowing another order on the same market-side.
        effective_price_delta = max(effective_price_delta, 0.02)
        effective_alpha_delta = max(effective_alpha_delta, 0.08)
        effective_conf_delta = max(effective_conf_delta, 0.05)
        effective_min_observation_advance = max(effective_min_observation_advance, 8.0)
    global_weak_override_min_elapsed_minutes = max(1.0, min(4.0, safe_cooldown_minutes * 0.5))
    weak_override_min_elapsed_minutes = max(3.0, min(12.0, safe_cooldown_minutes * 0.5))
    for intent in deduped_intents:
        market_side_key = f"{_normalize_text(intent.market_ticker)}|{_normalize_text(intent.side).lower()}"
        prior = recent_by_market_side.get(market_side_key)
        recent_plan_count = int(recent_plan_counts_by_market_side.get(market_side_key) or 0)
        repeat_cap_hit = safe_max_plans_per_window > 0 and recent_plan_count >= safe_max_plans_per_window

        if not isinstance(prior, dict):
            if repeat_cap_hit:
                blocked_reasons["market_side_repeat_cap"] += 1
                blocked.append(
                    {
                        "market_ticker": intent.market_ticker,
                        "side": intent.side,
                        "underlying_key": intent.underlying_key,
                        "intent_id": intent.intent_id,
                        "constraint_status": intent.constraint_status,
                        "settlement_station": intent.settlement_station,
                        "reason": "market_side_repeat_cap",
                        "recent_plan_count_window": recent_plan_count,
                        "max_plans_per_window": safe_max_plans_per_window,
                        "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
                        "prior_planned_at_utc": None,
                        "prior_temperature_client_order_id": None,
                    }
                )
                continue
            kept.append(intent)
            continue

        prior_epoch = _parse_float(prior.get("planned_at_epoch"))
        if prior_epoch is None:
            if repeat_cap_hit:
                blocked_reasons["market_side_repeat_cap"] += 1
                blocked.append(
                    {
                        "market_ticker": intent.market_ticker,
                        "side": intent.side,
                        "underlying_key": intent.underlying_key,
                        "intent_id": intent.intent_id,
                        "constraint_status": intent.constraint_status,
                        "settlement_station": intent.settlement_station,
                        "reason": "market_side_repeat_cap",
                        "recent_plan_count_window": recent_plan_count,
                        "max_plans_per_window": safe_max_plans_per_window,
                        "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
                        "prior_planned_at_utc": _normalize_text(prior.get("planned_at_utc")) or None,
                        "prior_temperature_client_order_id": _normalize_text(
                            prior.get("temperature_client_order_id")
                        )
                        or None,
                    }
                )
                continue
            kept.append(intent)
            continue
        elapsed_minutes = max(0.0, (now_utc.timestamp() - prior_epoch) / 60.0)
        if not repeat_cap_hit and (safe_cooldown_minutes <= 0.0 or elapsed_minutes >= safe_cooldown_minutes):
            kept.append(intent)
            continue

        current_price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))
        prior_price = _parse_float(prior.get("maker_entry_price_dollars"))
        current_alpha = _intent_alpha_strength(intent)
        prior_alpha = _parse_float(prior.get("temperature_alpha_strength"))
        current_confidence = max(0.0, min(1.0, float(intent.settlement_confidence_score)))
        prior_confidence = _parse_float(prior.get("confidence"))
        current_observed = _parse_float(intent.observed_metric_settlement_quantized)
        prior_observed = _parse_float(prior.get("temperature_observed_metric_settlement_quantized"))
        current_obs_ts = _parse_ts(intent.metar_observation_time_utc)
        prior_obs_ts = _parse_ts(prior.get("temperature_metar_observation_time_utc"))

        price_delta = (
            abs(current_price - prior_price)
            if isinstance(prior_price, (int, float)) and math.isfinite(float(prior_price))
            else None
        )
        alpha_delta = (
            current_alpha - float(prior_alpha)
            if isinstance(prior_alpha, (int, float)) and math.isfinite(float(prior_alpha))
            else None
        )
        confidence_delta = (
            current_confidence - float(prior_confidence)
            if isinstance(prior_confidence, (int, float)) and math.isfinite(float(prior_confidence))
            else None
        )
        observation_advance_minutes = (
            max(0.0, (current_obs_ts - prior_obs_ts).total_seconds() / 60.0)
            if isinstance(current_obs_ts, datetime)
            and isinstance(prior_obs_ts, datetime)
            and current_obs_ts > prior_obs_ts
            else 0.0
        )
        observed_metric_changed = (
            isinstance(current_observed, (int, float))
            and isinstance(prior_observed, (int, float))
            and abs(float(current_observed) - float(prior_observed)) > 1e-9
        )

        override_reason = ""
        if (
            isinstance(price_delta, (int, float))
            and float(price_delta) >= effective_price_delta
            and effective_price_delta > 0.0
        ):
            override_reason = "price_changed"
        elif (
            isinstance(alpha_delta, (int, float))
            and float(alpha_delta) >= effective_alpha_delta
            and effective_alpha_delta > 0.0
        ):
            override_reason = "alpha_strength_improved"
        elif (
            isinstance(confidence_delta, (int, float))
            and float(confidence_delta) >= effective_conf_delta
            and effective_conf_delta > 0.0
        ):
            override_reason = "confidence_improved"
        elif observed_metric_changed:
            override_reason = "observed_metric_changed"
        elif (
            observation_advance_minutes >= effective_min_observation_advance
            and effective_min_observation_advance > 0.0
        ):
            override_reason = "metar_observation_advanced"

        override_suppressed_reason = ""
        if override_reason:
            if (
                override_reason in {"metar_observation_advanced", "observed_metric_changed"}
                and elapsed_minutes < global_weak_override_min_elapsed_minutes
            ):
                override_suppressed_reason = "weak_override_elapsed_too_short"
            elif (
                override_reason == "metar_observation_advanced"
                and repeat_pressure_mode
                and observation_advance_minutes < max(4.0, effective_min_observation_advance)
            ):
                override_suppressed_reason = "repeat_pressure_insufficient_observation_advance"
            elif (
                override_reason in {"metar_observation_advanced", "observed_metric_changed"}
                and repeat_pressure_mode
                and elapsed_minutes < weak_override_min_elapsed_minutes
            ):
                override_suppressed_reason = "repeat_pressure_weak_override"

        if override_suppressed_reason:
            override_suppressed_reasons[override_suppressed_reason] += 1
            override_reason = ""

        if repeat_cap_hit and not override_reason:
            blocked_reasons["market_side_repeat_cap"] += 1
            blocked.append(
                {
                    "market_ticker": intent.market_ticker,
                    "side": intent.side,
                    "underlying_key": intent.underlying_key,
                    "intent_id": intent.intent_id,
                    "constraint_status": intent.constraint_status,
                    "settlement_station": intent.settlement_station,
                    "reason": "market_side_repeat_cap",
                    "recent_plan_count_window": recent_plan_count,
                    "max_plans_per_window": safe_max_plans_per_window,
                    "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
                    "minutes_since_last_plan": round(elapsed_minutes, 3),
                    "cooldown_minutes": round(safe_cooldown_minutes, 3),
                    "override_suppressed_reason": override_suppressed_reason or None,
                    "prior_planned_at_utc": _normalize_text(prior.get("planned_at_utc")) or None,
                    "prior_temperature_client_order_id": _normalize_text(
                        prior.get("temperature_client_order_id")
                    )
                    or None,
                }
            )
            continue

        if override_reason:
            kept.append(intent)
            override_count += 1
            if repeat_cap_hit:
                repeat_cap_override_count += 1
                override_reasons[f"repeat_cap_override_{override_reason}"] += 1
            else:
                override_reasons[override_reason] += 1
            continue

        blocked_reasons["market_side_replan_cooldown"] += 1
        blocked_row = {
            "market_ticker": intent.market_ticker,
            "side": intent.side,
            "underlying_key": intent.underlying_key,
            "intent_id": intent.intent_id,
            "constraint_status": intent.constraint_status,
            "settlement_station": intent.settlement_station,
            "minutes_since_last_plan": round(elapsed_minutes, 3),
            "cooldown_minutes": round(safe_cooldown_minutes, 3),
            "current_price_dollars": round(current_price, 4),
            "prior_price_dollars": round(float(prior_price), 4) if isinstance(prior_price, (int, float)) else None,
            "price_delta_dollars": round(float(price_delta), 4) if isinstance(price_delta, (int, float)) else None,
            "current_alpha_strength": round(float(current_alpha), 6),
            "prior_alpha_strength": (
                round(float(prior_alpha), 6) if isinstance(prior_alpha, (int, float)) else None
            ),
            "alpha_delta": round(float(alpha_delta), 6) if isinstance(alpha_delta, (int, float)) else None,
            "current_confidence": round(float(current_confidence), 6),
            "prior_confidence": (
                round(float(prior_confidence), 6) if isinstance(prior_confidence, (int, float)) else None
            ),
            "confidence_delta": (
                round(float(confidence_delta), 6) if isinstance(confidence_delta, (int, float)) else None
            ),
            "current_observed_metric_settlement_quantized": (
                round(float(current_observed), 6) if isinstance(current_observed, (int, float)) else None
            ),
            "prior_observed_metric_settlement_quantized": (
                round(float(prior_observed), 6) if isinstance(prior_observed, (int, float)) else None
            ),
            "observed_metric_changed": bool(observed_metric_changed),
            "current_metar_observation_time_utc": (
                current_obs_ts.isoformat() if isinstance(current_obs_ts, datetime) else None
            ),
            "prior_metar_observation_time_utc": (
                prior_obs_ts.isoformat() if isinstance(prior_obs_ts, datetime) else None
            ),
            "observation_advance_minutes": round(float(observation_advance_minutes), 6),
            "prior_planned_at_utc": _normalize_text(prior.get("planned_at_utc")) or None,
            "prior_temperature_client_order_id": _normalize_text(prior.get("temperature_client_order_id")) or None,
            "override_suppressed_reason": override_suppressed_reason or None,
            "effective_price_change_override_dollars": round(float(effective_price_delta), 6),
            "effective_alpha_change_override": round(float(effective_alpha_delta), 6),
            "effective_confidence_change_override": round(float(effective_conf_delta), 6),
            "effective_min_observation_advance_minutes": round(float(effective_min_observation_advance), 6),
        }
        blocked.append(blocked_row)
        blocked_intents.append(intent)
        blocked_intent_rows.append(blocked_row)

    # Throughput backstop: keep at least the top-ranked intents from the
    # pre-filter list so cooldown cannot starve planning completely.
    backstop_release_suppressed = False
    backstop_release_suppressed_reason = ""
    backstop_release_qualified_count = 0
    backstop_release_unqualified_count = 0
    backstop_release_override_trigger_min_elapsed_minutes = 0.0
    if safe_min_orders_backstop > 0 and len(kept) < safe_min_orders_backstop and blocked_intents:
        minimum_unique_market_sides = max(3, safe_min_orders_backstop * 2)
        minimum_unique_underlyings = max(2, min(4, safe_min_orders_backstop + 1))
        thin_independent_breadth = (
            input_unique_market_side_count < minimum_unique_market_sides
            or input_unique_underlying_count < minimum_unique_underlyings
        )
        repeat_pressure = (
            input_unique_market_side_count > 0
            and input_repeat_multiplier >= 1.6
            and input_unique_market_side_count <= 8
            and input_unique_underlying_count <= 5
        )

        if thin_independent_breadth:
            backstop_release_suppressed = True
            backstop_release_suppressed_reason = "thin_independent_breadth"
        elif repeat_pressure:
            backstop_release_suppressed = True
            backstop_release_suppressed_reason = "repeat_pressure"
        else:
            backstop_release_candidate_indices: list[int] = []
            backstop_release_override_trigger_min_elapsed_minutes = max(
                2.0,
                min(12.0, safe_cooldown_minutes * 0.75),
            )
            min_override_price_delta = max(0.01, float(effective_price_delta) * 0.75)
            min_override_alpha_delta = max(0.03, float(effective_alpha_delta) * 0.60)
            min_override_conf_delta = max(0.02, float(effective_conf_delta) * 0.60)
            min_override_obs_advance = max(3.0, float(effective_min_observation_advance) * 0.75)
            for idx, blocked_row in enumerate(blocked_intent_rows):
                minutes_since_last_plan = float(_parse_float(blocked_row.get("minutes_since_last_plan")) or 0.0)
                price_delta = _parse_float(blocked_row.get("price_delta_dollars"))
                alpha_delta = _parse_float(blocked_row.get("alpha_delta"))
                confidence_delta = _parse_float(blocked_row.get("confidence_delta"))
                observation_advance_minutes = _parse_float(blocked_row.get("observation_advance_minutes"))
                observed_metric_changed = bool(blocked_row.get("observed_metric_changed"))
                meaningful_override_trigger = (
                    observed_metric_changed
                    or (
                        isinstance(price_delta, float)
                        and price_delta >= float(min_override_price_delta)
                    )
                    or (
                        isinstance(alpha_delta, float)
                        and alpha_delta >= float(min_override_alpha_delta)
                    )
                    or (
                        isinstance(confidence_delta, float)
                        and confidence_delta >= float(min_override_conf_delta)
                    )
                    or (
                        isinstance(observation_advance_minutes, float)
                        and observation_advance_minutes >= float(min_override_obs_advance)
                    )
                    or (
                        safe_cooldown_minutes > 0.0
                        and minutes_since_last_plan >= float(backstop_release_override_trigger_min_elapsed_minutes)
                    )
                )
                if meaningful_override_trigger:
                    backstop_release_candidate_indices.append(idx)
            backstop_release_qualified_count = int(len(backstop_release_candidate_indices))
            backstop_release_unqualified_count = max(0, len(blocked_intents) - backstop_release_qualified_count)
            if not backstop_release_candidate_indices:
                backstop_release_suppressed = True
                backstop_release_suppressed_reason = "no_override_trigger"
            release_count = min(
                safe_min_orders_backstop - len(kept),
                len(backstop_release_candidate_indices),
            )
            for idx in backstop_release_candidate_indices[:release_count]:
                kept.append(blocked_intents[idx])
                blocked_intent_rows[idx]["released_by_backstop"] = True
                blocked_intent_rows[idx]["backstop_release_reason"] = "qualified_override_trigger"
                backstop_released_count += 1
            if backstop_released_count > 0:
                override_reasons["cooldown_backstop_release"] += backstop_released_count
                blocked = [row for row in blocked if not bool(row.get("released_by_backstop"))]

    blocked.sort(
        key=lambda row: (
            float(row.get("minutes_since_last_plan") or 0.0),
            -float(row.get("current_alpha_strength") or 0.0),
            _normalize_text(row.get("market_ticker")),
        )
    )
    return kept, {
        **history_meta,
        "input_count": len(intents),
        "deduped_input_count": len(deduped_intents),
        "kept_count": len(kept),
        "blocked_count": len(blocked),
        "override_count": override_count,
        "backstop_released_count": backstop_released_count,
        "backstop_release_suppressed": bool(backstop_release_suppressed),
        "backstop_release_suppressed_reason": backstop_release_suppressed_reason,
        "backstop_release_qualified_count": int(backstop_release_qualified_count),
        "backstop_release_unqualified_count": int(backstop_release_unqualified_count),
        "backstop_release_override_trigger_min_elapsed_minutes": round(
            float(backstop_release_override_trigger_min_elapsed_minutes),
            6,
        ),
        "min_orders_backstop": safe_min_orders_backstop,
        "repeat_window_minutes": round(safe_repeat_window_minutes, 6),
        "max_plans_per_window": int(safe_max_plans_per_window),
        "repeat_cap_blocked_count": int(blocked_reasons.get("market_side_repeat_cap") or 0),
        "repeat_cap_override_count": int(repeat_cap_override_count),
        "min_observation_advance_minutes": round(safe_min_observation_advance, 6),
        "effective_min_observation_advance_minutes": round(float(effective_min_observation_advance), 6),
        "price_change_override_dollars": round(safe_price_delta, 6),
        "effective_price_change_override_dollars": round(float(effective_price_delta), 6),
        "alpha_change_override": round(safe_alpha_delta, 6),
        "effective_alpha_change_override": round(float(effective_alpha_delta), 6),
        "confidence_change_override": round(safe_conf_delta, 6),
        "effective_confidence_change_override": round(float(effective_conf_delta), 6),
        "input_unique_market_sides": int(input_unique_market_side_count),
        "input_unique_underlyings": int(input_unique_underlying_count),
        "input_repeat_multiplier": round(float(input_repeat_multiplier), 6),
        "repeat_pressure_mode": bool(repeat_pressure_mode),
        "global_weak_override_min_elapsed_minutes": round(float(global_weak_override_min_elapsed_minutes), 6),
        "weak_override_min_elapsed_minutes": round(float(weak_override_min_elapsed_minutes), 6),
        "same_cycle_duplicate_count": int(len(same_cycle_duplicate_rows)),
        "same_cycle_duplicate_market_side_count": int(
            len(
                {
                    f"{_normalize_text(row.get('market_ticker'))}|{_normalize_text(row.get('side')).lower()}"
                    for row in same_cycle_duplicate_rows
                }
            )
        ),
        "blocked_reason_counts": dict(
            sorted(
                {
                    **{key: int(value) for key, value in blocked_reasons.items()},
                    **{key: int(value) for key, value in same_cycle_duplicate_reason_counts.items()},
                }.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        "override_reason_counts": dict(sorted(override_reasons.items(), key=lambda item: (-item[1], item[0]))),
        "override_suppressed_reason_counts": dict(
            sorted(override_suppressed_reasons.items(), key=lambda item: (-item[1], item[0]))
        ),
        "blocked_top": blocked[:40],
        "same_cycle_duplicate_top": same_cycle_duplicate_rows[:40],
    }

def _intent_with_policy_row(
    *,
    intent: TemperatureTradeIntent,
    decision: TemperaturePolicyDecision | None,
    revalidation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = intent.to_row()
    row["policy_approved"] = bool(decision.approved) if decision else False
    row["policy_reason"] = decision.decision_reason if decision else "missing_decision"
    row["policy_notes"] = decision.decision_notes if decision else ""
    row["policy_alpha_strength"] = decision.alpha_strength if decision else ""
    row["policy_probability_confidence"] = decision.probability_confidence if decision else ""
    row["policy_expected_edge_net"] = decision.expected_edge_net if decision else ""
    row["policy_base_edge_net"] = decision.base_edge_net if decision else ""
    row["policy_edge_to_risk_ratio"] = decision.edge_to_risk_ratio if decision else ""
    row["policy_min_alpha_strength_required"] = (
        decision.min_alpha_strength_required if decision else ""
    )
    row["policy_min_probability_confidence_required"] = (
        decision.min_probability_confidence_required if decision else ""
    )
    row["policy_min_expected_edge_net_required"] = (
        decision.min_expected_edge_net_required if decision else ""
    )
    row["policy_min_edge_to_risk_ratio_required"] = (
        decision.min_edge_to_risk_ratio_required if decision else ""
    )
    row["policy_min_base_edge_net_required"] = (
        decision.min_base_edge_net_required if decision else ""
    )
    row["policy_min_probability_breakeven_gap_required"] = (
        decision.min_probability_breakeven_gap_required if decision else ""
    )
    row["policy_metar_max_age_minutes_applied"] = (
        decision.metar_max_age_minutes_applied if decision else ""
    )
    row["policy_metar_local_hour"] = decision.metar_local_hour if decision else ""
    row["policy_sparse_evidence_hardening_applied"] = (
        decision.sparse_evidence_hardening_applied if decision else ""
    )
    row["policy_sparse_evidence_probability_raise"] = (
        decision.sparse_evidence_probability_raise if decision else ""
    )
    row["policy_sparse_evidence_expected_edge_raise"] = (
        decision.sparse_evidence_expected_edge_raise if decision else ""
    )
    row["policy_sparse_evidence_support_score"] = (
        decision.sparse_evidence_support_score if decision else ""
    )
    row["policy_sparse_evidence_volatility_penalty"] = (
        decision.sparse_evidence_volatility_penalty if decision else ""
    )
    row["policy_historical_quality_penalty_ratio"] = (
        decision.historical_quality_penalty_ratio if decision else ""
    )
    row["policy_historical_quality_boost_ratio"] = (
        decision.historical_quality_boost_ratio if decision else ""
    )
    row["policy_historical_quality_probability_raise"] = (
        decision.historical_quality_probability_raise if decision else ""
    )
    row["policy_historical_quality_expected_edge_raise"] = (
        decision.historical_quality_expected_edge_raise if decision else ""
    )
    row["policy_historical_quality_score_adjustment"] = (
        decision.historical_quality_score_adjustment if decision else ""
    )
    row["policy_historical_quality_sample_size"] = (
        decision.historical_quality_sample_size if decision else ""
    )
    row["policy_historical_quality_sources"] = (
        decision.historical_quality_sources if decision else ""
    )
    row["policy_historical_quality_signal_bucket_penalty_ratio"] = (
        decision.historical_quality_signal_bucket_penalty_ratio if decision else ""
    )
    row["policy_historical_quality_signal_bucket_samples"] = (
        decision.historical_quality_signal_bucket_samples if decision else ""
    )
    row["policy_historical_quality_station_bucket_penalty_ratio"] = (
        decision.historical_quality_station_bucket_penalty_ratio if decision else ""
    )
    row["policy_historical_quality_station_bucket_samples"] = (
        decision.historical_quality_station_bucket_samples if decision else ""
    )
    row["policy_historical_quality_local_hour_bucket_penalty_ratio"] = (
        decision.historical_quality_local_hour_bucket_penalty_ratio if decision else ""
    )
    row["policy_historical_quality_local_hour_bucket_samples"] = (
        decision.historical_quality_local_hour_bucket_samples if decision else ""
    )
    row["policy_historical_quality_signal_hard_block_active"] = (
        decision.historical_quality_signal_hard_block_active if decision else ""
    )
    row["policy_historical_quality_station_hour_hard_block_active"] = (
        decision.historical_quality_station_hour_hard_block_active if decision else ""
    )
    row["policy_historical_expectancy_hard_block_active"] = (
        decision.historical_expectancy_hard_block_active if decision else ""
    )
    row["policy_historical_expectancy_pressure_score"] = (
        decision.historical_expectancy_pressure_score if decision else ""
    )
    row["policy_historical_expectancy_edge_raise"] = (
        decision.historical_expectancy_edge_raise if decision else ""
    )
    row["policy_historical_expectancy_probability_raise"] = (
        decision.historical_expectancy_probability_raise if decision else ""
    )
    row["policy_historical_quality_global_only_pressure_active"] = (
        decision.historical_quality_global_only_pressure_active if decision else ""
    )
    row["policy_historical_quality_global_only_adjusted_share"] = (
        decision.historical_quality_global_only_adjusted_share if decision else ""
    )
    row["policy_historical_quality_global_only_excess_ratio"] = (
        decision.historical_quality_global_only_excess_ratio if decision else ""
    )
    row["policy_historical_profitability_guardrail_penalty_ratio"] = (
        decision.historical_profitability_guardrail_penalty_ratio if decision else ""
    )
    row["policy_historical_profitability_guardrail_probability_raise"] = (
        decision.historical_profitability_guardrail_probability_raise if decision else ""
    )
    row["policy_historical_profitability_guardrail_expected_edge_raise"] = (
        decision.historical_profitability_guardrail_expected_edge_raise if decision else ""
    )
    row["policy_historical_profitability_guardrail_calibration_ratio"] = (
        decision.historical_profitability_guardrail_calibration_ratio if decision else ""
    )
    row["policy_historical_profitability_guardrail_evidence_confidence"] = (
        decision.historical_profitability_guardrail_evidence_confidence if decision else ""
    )
    row["policy_historical_profitability_guardrail_resolved_unique_market_sides"] = (
        decision.historical_profitability_guardrail_resolved_unique_market_sides if decision else ""
    )
    row["policy_historical_profitability_guardrail_repeated_entry_multiplier"] = (
        decision.historical_profitability_guardrail_repeated_entry_multiplier if decision else ""
    )
    row["policy_historical_profitability_guardrail_concentration_warning"] = (
        decision.historical_profitability_guardrail_concentration_warning if decision else ""
    )
    row["policy_historical_profitability_guardrail_status"] = (
        decision.historical_profitability_guardrail_status if decision else ""
    )
    row["policy_historical_profitability_guardrail_signals"] = (
        decision.historical_profitability_guardrail_signals if decision else ""
    )
    row["policy_historical_profitability_bucket_guardrail_penalty_ratio"] = (
        decision.historical_profitability_bucket_guardrail_penalty_ratio if decision else ""
    )
    row["policy_historical_profitability_bucket_guardrail_probability_raise"] = (
        decision.historical_profitability_bucket_guardrail_probability_raise if decision else ""
    )
    row["policy_historical_profitability_bucket_guardrail_expected_edge_raise"] = (
        decision.historical_profitability_bucket_guardrail_expected_edge_raise if decision else ""
    )
    row["policy_historical_profitability_bucket_guardrail_status"] = (
        decision.historical_profitability_bucket_guardrail_status if decision else ""
    )
    row["policy_historical_profitability_bucket_guardrail_sources"] = (
        decision.historical_profitability_bucket_guardrail_sources if decision else ""
    )
    if isinstance(revalidation, dict):
        row["revalidation_status"] = "invalidated"
        row["revalidation_reason"] = _normalize_text(revalidation.get("reason"))
        reasons = revalidation.get("reasons")
        row["revalidation_reasons"] = ",".join([str(item) for item in reasons]) if isinstance(reasons, list) else ""
    else:
        row["revalidation_status"] = "approved"
        row["revalidation_reason"] = ""
        row["revalidation_reasons"] = ""
    return row


def _write_intents_csv(
    *,
    path: Path,
    intents: list[TemperatureTradeIntent],
    decisions_by_id: dict[str, TemperaturePolicyDecision],
    revalidation_by_id: dict[str, dict[str, Any]] | None = None,
) -> None:
    fieldnames = [
        "intent_id",
        "captured_at",
        "policy_version",
        "underlying_key",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "market_title",
        "settlement_station",
        "settlement_timezone",
        "target_date_local",
        "constraint_status",
        "constraint_reason",
        "side",
        "max_entry_price_dollars",
        "intended_contracts",
        "settlement_confidence_score",
        "observed_max_settlement_quantized",
        "temperature_metric",
        "observed_metric_settlement_quantized",
        "forecast_upper_bound_settlement_raw",
        "forecast_lower_bound_settlement_raw",
        "threshold_kind",
        "threshold_lower_bound",
        "threshold_upper_bound",
        "yes_possible_overlap",
        "yes_possible_gap",
        "primary_signal_margin",
        "forecast_feasibility_margin",
        "forecast_model_status",
        "taf_status",
        "taf_volatility_score",
        "forecast_range_width",
        "observed_distance_to_lower_bound",
        "observed_distance_to_upper_bound",
        "cross_market_family_score",
        "cross_market_family_zscore",
        "cross_market_family_candidate_rank",
        "cross_market_family_bucket_size",
        "cross_market_family_signal",
        "consensus_profile_support_count",
        "consensus_profile_support_ratio",
        "consensus_weighted_support_score",
        "consensus_weighted_support_ratio",
        "consensus_alpha_score",
        "consensus_rank",
        "consensus_profile_names",
        "range_family_consistency_conflict",
        "range_family_consistency_conflict_scope",
        "range_family_consistency_conflict_reason",
        "speci_recent",
        "speci_shock_active",
        "speci_shock_confidence",
        "speci_shock_weight",
        "speci_shock_mode",
        "speci_shock_trigger_count",
        "speci_shock_trigger_families",
        "speci_shock_persistence_ok",
        "speci_shock_cooldown_blocked",
        "speci_shock_improvement_hold_active",
        "speci_shock_delta_temp_c",
        "speci_shock_delta_minutes",
        "speci_shock_decay_tau_minutes",
        "close_time",
        "hours_to_close",
        "spec_hash",
        "metar_snapshot_sha",
        "metar_observation_time_utc",
        "metar_observation_age_minutes",
        "market_snapshot_seq",
        "policy_approved",
        "policy_reason",
        "policy_notes",
        "policy_alpha_strength",
        "policy_probability_confidence",
        "policy_expected_edge_net",
        "policy_base_edge_net",
        "policy_edge_to_risk_ratio",
        "policy_min_alpha_strength_required",
        "policy_min_probability_confidence_required",
        "policy_min_expected_edge_net_required",
        "policy_min_edge_to_risk_ratio_required",
        "policy_min_base_edge_net_required",
        "policy_min_probability_breakeven_gap_required",
        "policy_metar_max_age_minutes_applied",
        "policy_metar_local_hour",
        "policy_sparse_evidence_hardening_applied",
        "policy_sparse_evidence_probability_raise",
        "policy_sparse_evidence_expected_edge_raise",
        "policy_sparse_evidence_support_score",
        "policy_sparse_evidence_volatility_penalty",
        "policy_historical_quality_penalty_ratio",
        "policy_historical_quality_boost_ratio",
        "policy_historical_quality_probability_raise",
        "policy_historical_quality_expected_edge_raise",
        "policy_historical_quality_score_adjustment",
        "policy_historical_quality_sample_size",
        "policy_historical_quality_sources",
        "policy_historical_quality_signal_bucket_penalty_ratio",
        "policy_historical_quality_signal_bucket_samples",
        "policy_historical_quality_station_bucket_penalty_ratio",
        "policy_historical_quality_station_bucket_samples",
        "policy_historical_quality_local_hour_bucket_penalty_ratio",
        "policy_historical_quality_local_hour_bucket_samples",
        "policy_historical_quality_signal_hard_block_active",
        "policy_historical_quality_station_hour_hard_block_active",
        "policy_historical_expectancy_hard_block_active",
        "policy_historical_expectancy_pressure_score",
        "policy_historical_expectancy_edge_raise",
        "policy_historical_expectancy_probability_raise",
        "policy_historical_quality_global_only_pressure_active",
        "policy_historical_quality_global_only_adjusted_share",
        "policy_historical_quality_global_only_excess_ratio",
        "policy_historical_profitability_guardrail_penalty_ratio",
        "policy_historical_profitability_guardrail_probability_raise",
        "policy_historical_profitability_guardrail_expected_edge_raise",
        "policy_historical_profitability_guardrail_calibration_ratio",
        "policy_historical_profitability_guardrail_evidence_confidence",
        "policy_historical_profitability_guardrail_resolved_unique_market_sides",
        "policy_historical_profitability_guardrail_repeated_entry_multiplier",
        "policy_historical_profitability_guardrail_concentration_warning",
        "policy_historical_profitability_guardrail_status",
        "policy_historical_profitability_guardrail_signals",
        "policy_historical_profitability_bucket_guardrail_penalty_ratio",
        "policy_historical_profitability_bucket_guardrail_probability_raise",
        "policy_historical_profitability_bucket_guardrail_expected_edge_raise",
        "policy_historical_profitability_bucket_guardrail_status",
        "policy_historical_profitability_bucket_guardrail_sources",
        "revalidation_status",
        "revalidation_reason",
        "revalidation_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for intent in intents:
            decision = decisions_by_id.get(intent.intent_id)
            revalidation = revalidation_by_id.get(intent.intent_id) if isinstance(revalidation_by_id, dict) else None
            row = _intent_with_policy_row(
                intent=intent,
                decision=decision,
                revalidation=revalidation,
            )
            writer.writerow(row)


def _write_plan_csv(path: Path, plans: list[dict[str, Any]]) -> None:
    fieldnames = [
        "planned_at_utc",
        "plan_rank",
        "category",
        "market_ticker",
        "canonical_ticker",
        "canonical_niche",
        "contract_family",
        "source_strategy",
        "side",
        "contracts_per_order",
        "hours_to_close",
        "confidence",
        "temperature_alpha_strength",
        "temperature_probability_confidence_raw",
        "temperature_probability_confidence_base",
        "temperature_probability_confidence_status_prior",
        "temperature_probability_confidence_alpha_term",
        "temperature_probability_confidence_speci_term",
        "temperature_probability_confidence_cross_market_term",
        "temperature_probability_confidence_consensus_term",
        "temperature_probability_confidence_taf_term",
        "temperature_probability_confidence_gap_penalty",
        "maker_entry_price_dollars",
        "maker_yes_price_dollars",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
        "estimated_entry_fee_dollars",
        "temperature_intent_id",
        "temperature_underlying_key",
        "temperature_policy_version",
        "temperature_spec_hash",
        "temperature_metar_snapshot_sha",
        "temperature_metar_observation_time_utc",
        "temperature_market_snapshot_seq",
        "temperature_metric",
        "temperature_observed_metric_settlement_quantized",
        "temperature_forecast_upper_bound_settlement_raw",
        "temperature_forecast_lower_bound_settlement_raw",
        "temperature_threshold_kind",
        "temperature_threshold_lower_bound",
        "temperature_threshold_upper_bound",
        "temperature_yes_possible_overlap",
        "temperature_yes_possible_gap",
        "temperature_primary_signal_margin",
        "temperature_forecast_feasibility_margin",
        "temperature_forecast_model_status",
        "temperature_taf_status",
        "temperature_taf_volatility_score",
        "temperature_forecast_range_width",
        "temperature_observed_distance_to_lower_bound",
        "temperature_observed_distance_to_upper_bound",
        "temperature_cross_market_family_score",
        "temperature_cross_market_family_zscore",
        "temperature_cross_market_family_candidate_rank",
        "temperature_cross_market_family_bucket_size",
        "temperature_cross_market_family_signal",
        "temperature_speci_recent",
        "temperature_speci_shock_active",
        "temperature_speci_shock_confidence",
        "temperature_speci_shock_weight",
        "temperature_speci_shock_mode",
        "temperature_speci_shock_trigger_count",
        "temperature_speci_shock_trigger_families",
        "temperature_speci_shock_persistence_ok",
        "temperature_speci_shock_cooldown_blocked",
        "temperature_speci_shock_improvement_hold_active",
        "temperature_speci_shock_delta_temp_c",
        "temperature_speci_shock_delta_minutes",
        "temperature_speci_shock_decay_tau_minutes",
        "temperature_expected_edge_model_version",
        "temperature_expected_edge_base",
        "temperature_expected_edge_alpha_bonus",
        "temperature_expected_edge_confidence_bonus",
        "temperature_expected_edge_urgency_bonus",
        "temperature_expected_edge_speci_bonus",
        "temperature_expected_edge_cross_market_bonus",
        "temperature_expected_edge_consensus_bonus",
        "temperature_expected_edge_taf_bonus",
        "temperature_expected_edge_gap_penalty",
        "temperature_expected_edge_friction_penalty",
        "temperature_expected_edge_entry_price",
        "temperature_expected_edge_side_win_probability",
        "temperature_expected_edge_upside_per_contract",
        "temperature_expected_edge_downside_per_contract",
        "temperature_expected_edge_risk_reward_ratio",
        "temperature_client_order_id",
        "temperature_consensus_profile_support_count",
        "temperature_consensus_profile_support_ratio",
        "temperature_consensus_weighted_support_score",
        "temperature_consensus_weighted_support_ratio",
        "temperature_consensus_alpha_score",
        "temperature_consensus_rank",
        "temperature_consensus_profile_names",
        "order_payload_preview",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for plan in plans:
            row = dict(plan)
            row["order_payload_preview"] = json.dumps(plan.get("order_payload_preview", {}), separators=(",", ":"))
            writer.writerow(row)


def _build_settlement_finalization_snapshot(
    *,
    intents: list[TemperatureTradeIntent],
    settlement_state_by_underlying: dict[str, dict[str, Any]],
    captured_at: datetime,
) -> dict[str, Any]:
    by_underlying: dict[str, dict[str, Any]] = {}
    for intent in intents:
        row = by_underlying.setdefault(
            intent.underlying_key,
            {
                "underlying_key": intent.underlying_key,
                "series_ticker": intent.series_ticker,
                "settlement_station": intent.settlement_station,
                "target_date_local": intent.target_date_local,
                "intent_count": 0,
                "market_tickers": [],
                "fast_truth_max_settlement_quantized": None,
                "state": "fast_truth_only",
                "finalization_status": "fast_truth_only",
                "allow_new_orders": True,
                "reason": "",
                "final_truth_value": "",
                "revision_id": "",
                "source": "",
                "updated_at": "",
            },
        )
        row["intent_count"] = int(row.get("intent_count") or 0) + 1
        market_tickers = row.get("market_tickers")
        if isinstance(market_tickers, list) and intent.market_ticker not in market_tickers:
            market_tickers.append(intent.market_ticker)
        observed = intent.observed_max_settlement_quantized
        current_max = _parse_float(row.get("fast_truth_max_settlement_quantized"))
        observed_val = _parse_float(observed)
        if isinstance(observed_val, float) and (current_max is None or observed_val > current_max):
            row["fast_truth_max_settlement_quantized"] = observed_val

    for underlying_key, row in by_underlying.items():
        settlement_entry = settlement_state_by_underlying.get(underlying_key, {})
        if not isinstance(settlement_entry, dict):
            settlement_entry = {}
        if settlement_entry:
            row["state"] = _normalize_text(settlement_entry.get("state")) or "fast_truth_only"
            row["finalization_status"] = _normalize_text(settlement_entry.get("finalization_status")) or row["state"]
            row["allow_new_orders"] = bool(settlement_entry.get("allow_new_orders", True))
            row["reason"] = _normalize_text(settlement_entry.get("reason"))
            final_truth_value = settlement_entry.get("final_truth_value")
            row["final_truth_value"] = final_truth_value if final_truth_value not in (None, "") else ""
            row["revision_id"] = _normalize_text(settlement_entry.get("revision_id"))
            row["source"] = _normalize_text(settlement_entry.get("source"))
            row["updated_at"] = _normalize_text(settlement_entry.get("updated_at"))
        elif isinstance(_parse_float(row.get("fast_truth_max_settlement_quantized")), float):
            row["state"] = "fast_truth_only"
            row["finalization_status"] = "intraday_unfinalized"

    state_counts: dict[str, int] = {}
    blocked_underlyings = 0
    for row in by_underlying.values():
        state = _normalize_text(row.get("state")) or "unknown"
        state_counts[state] = state_counts.get(state, 0) + 1
        if not bool(row.get("allow_new_orders", True)):
            blocked_underlyings += 1

    return {
        "captured_at": captured_at.isoformat(),
        "underlying_count": len(by_underlying),
        "blocked_underlyings": blocked_underlyings,
        "state_counts": dict(sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))),
        "underlyings": list(by_underlying.values()),
    }


def run_kalshi_temperature_trader(
    *,
    env_file: str,
    output_dir: str = "outputs",
    specs_csv: str | None = None,
    constraint_csv: str | None = None,
    metar_summary_json: str | None = None,
    metar_state_json: str | None = None,
    ws_state_json: str | None = None,
    alpha_consensus_json: str | None = None,
    settlement_state_json: str | None = None,
    book_db_path: str | None = None,
    policy_version: str = "temperature_policy_v1",
    contracts_per_order: int = 1,
    max_orders: int = 8,
    max_markets: int = 100,
    timeout_seconds: float = 12.0,
    allow_live_orders: bool = False,
    intents_only: bool = False,
    shadow_quote_probe_on_no_candidates: bool = False,
    shadow_quote_probe_market_side_targets: list[str] | None = None,
    exclude_market_tickers: list[str] | None = None,
    min_settlement_confidence: float = 0.6,
    max_metar_age_minutes: float | None = 20.0,
    metar_age_policy_json: str | None = None,
    speci_calibration_json: str | None = None,
    min_alpha_strength: float | None = 0.0,
    min_probability_confidence: float | None = None,
    min_expected_edge_net: float | None = None,
    min_edge_to_risk_ratio: float | None = None,
    min_base_edge_net: float | None = 0.0,
    min_probability_breakeven_gap: float | None = 0.0,
    enforce_probability_edge_thresholds: bool = False,
    enforce_entry_price_probability_floor: bool = False,
    fallback_min_probability_confidence: float | None = None,
    fallback_min_expected_edge_net: float | None = 0.005,
    fallback_min_edge_to_risk_ratio: float | None = 0.02,
    enforce_interval_consistency: bool = True,
    max_yes_possible_gap_for_yes_side: float = 0.0,
    min_hours_to_close: float | None = 0.0,
    max_hours_to_close: float | None = 48.0,
    max_intents_per_underlying: int = 6,
    adaptive_policy_profile: dict[str, Any] | None = None,
    taf_stale_grace_minutes: float = 0.0,
    taf_stale_grace_max_volatility_score: float | None = 1.0,
    taf_stale_grace_max_range_width: float | None = 10.0,
    metar_freshness_quality_boundary_ratio: float | None = 0.92,
    metar_freshness_quality_probability_margin: float = 0.03,
    metar_freshness_quality_expected_edge_margin: float = 0.005,
    metar_ingest_quality_gate_enabled: bool = True,
    metar_ingest_min_quality_score: float | None = 0.70,
    metar_ingest_min_fresh_station_coverage_ratio: float | None = 0.55,
    metar_ingest_require_ready_status: bool = False,
    high_price_edge_guard_enabled: bool = False,
    high_price_edge_guard_min_entry_price_dollars: float = 0.85,
    high_price_edge_guard_min_expected_edge_net: float = 0.0,
    high_price_edge_guard_min_edge_to_risk_ratio: float = 0.02,
    yes_max_entry_price_dollars: float = 0.95,
    no_max_entry_price_dollars: float = 0.95,
    require_market_snapshot_seq: bool = True,
    require_metar_snapshot_sha: bool = False,
    enforce_underlying_netting: bool = True,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    enforce_trade_gate: bool = False,
    enforce_ws_state_authority: bool = False,
    ws_state_max_age_seconds: float = 30.0,
    max_total_deployed_pct: float = 0.35,
    max_same_station_exposure_pct: float = 0.6,
    max_same_hour_cluster_exposure_pct: float = 0.6,
    max_same_underlying_exposure_pct: float = 0.5,
    max_orders_per_station: int = 0,
    max_orders_per_underlying: int = 0,
    min_unique_stations_per_loop: int = 0,
    min_unique_underlyings_per_loop: int = 0,
    min_unique_local_hours_per_loop: int = 0,
    replan_market_side_cooldown_minutes: float = 20.0,
    replan_market_side_price_change_override_dollars: float = 0.02,
    replan_market_side_alpha_change_override: float = 0.2,
    replan_market_side_confidence_change_override: float = 0.03,
    replan_market_side_min_observation_advance_minutes: float = 2.0,
    replan_market_side_repeat_window_minutes: float = 1440.0,
    replan_market_side_max_plans_per_window: int = 8,
    replan_market_side_history_files: int = 180,
    replan_market_side_min_orders_backstop: int = 1,
    historical_selection_quality_enabled: bool = True,
    historical_selection_quality_lookback_hours: float = 14.0 * 24.0,
    historical_selection_quality_min_resolved_market_sides: int = 12,
    historical_selection_quality_min_bucket_samples: int = 4,
    historical_selection_quality_probability_penalty_max: float = 0.05,
    historical_selection_quality_expected_edge_penalty_max: float = 0.006,
    historical_selection_quality_score_adjust_scale: float = 0.35,
    historical_selection_quality_profile_max_age_hours: float = 96.0,
    historical_selection_quality_preferred_model: str = "fixed_fraction_per_underlying_family",
    weather_pattern_hardening_enabled: bool = True,
    weather_pattern_profile: dict[str, Any] | None = None,
    weather_pattern_profile_max_age_hours: float = 72.0,
    weather_pattern_min_bucket_samples: int = 12,
    weather_pattern_negative_expectancy_threshold: float = -0.05,
    weather_pattern_negative_regime_suppression_enabled: bool = False,
    weather_pattern_negative_regime_suppression_min_bucket_samples: int = 24,
    weather_pattern_negative_regime_suppression_expectancy_threshold: float = -0.08,
    weather_pattern_negative_regime_suppression_top_n: int = 8,
    weather_pattern_risk_off_enabled: bool = True,
    weather_pattern_risk_off_concentration_threshold: float = 0.75,
    weather_pattern_risk_off_min_attempts: int = 24,
    weather_pattern_risk_off_stale_metar_share_threshold: float = 0.50,
    constraint_scan_runner: ConstraintScanRunner = run_kalshi_temperature_constraint_scan,
    micro_execute_runner: MicroExecuteRunner = run_kalshi_micro_execute,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    resolved_constraint_csv = _normalize_text(constraint_csv)
    constraint_scan_summary: dict[str, Any] = {}
    if not resolved_constraint_csv:
        constraint_scan_summary = constraint_scan_runner(
            specs_csv=specs_csv,
            output_dir=output_dir,
            timeout_seconds=timeout_seconds,
            max_markets=max_markets,
            speci_calibration_json=speci_calibration_json,
        )
        resolved_constraint_csv = _normalize_text(constraint_scan_summary.get("output_csv"))
    speci_calibration_file_used = _normalize_text(constraint_scan_summary.get("speci_calibration_file_used"))
    if not speci_calibration_file_used:
        speci_calibration_file_used = _normalize_text(speci_calibration_json)

    if not resolved_constraint_csv:
        return {
            "status": "missing_constraint_csv",
            "captured_at": captured_at.isoformat(),
            "constraint_scan_summary": constraint_scan_summary,
            "error": "Constraint scan output CSV unavailable.",
        }

    constraint_path = Path(resolved_constraint_csv)
    constraint_rows = _read_csv_rows(constraint_path)
    if not constraint_rows:
        return {
            "status": "no_constraint_rows",
            "captured_at": captured_at.isoformat(),
            "constraint_csv": str(constraint_path),
            "constraint_scan_summary": constraint_scan_summary,
            "error": "Constraint CSV is empty or missing.",
        }

    resolved_specs_csv = _resolve_specs_csv(
        explicit_specs_csv=specs_csv,
        constraint_rows=constraint_rows,
        output_dir=output_dir,
    )
    specs_path = Path(resolved_specs_csv) if resolved_specs_csv else None
    specs_rows = _read_csv_rows(specs_path) if specs_path is not None else []
    specs_by_ticker = _build_specs_by_ticker(specs_rows)
    netting_snapshot = _load_existing_underlying_netting_snapshot(
        output_dir=output_dir,
        book_db_path=book_db_path,
        specs_by_ticker=specs_by_ticker,
        contracts_per_order=max(1, int(contracts_per_order)),
    )
    existing_underlying_slots = (
        netting_snapshot.get("underlying_slots")
        if enforce_underlying_netting and isinstance(netting_snapshot.get("underlying_slots"), dict)
        else {}
    )
    settlement_state_by_underlying, settlement_state_meta = _load_settlement_state_by_underlying(
        output_dir=output_dir,
        settlement_state_json=settlement_state_json,
    )

    metar_context = _load_metar_context(
        output_dir=output_dir,
        metar_summary_json=metar_summary_json,
        metar_state_json=metar_state_json,
    )
    metar_ingest_quality_context = _resolve_metar_ingest_quality_payload(
        summary_payload=(
            metar_context.get("summary_payload")
            if isinstance(metar_context.get("summary_payload"), dict)
            else {}
        ),
    )
    metar_ingest_quality_gate_state = _evaluate_metar_ingest_quality_gate(
        gate_enabled=metar_ingest_quality_gate_enabled,
        min_quality_score=metar_ingest_min_quality_score,
        min_fresh_station_coverage_ratio=metar_ingest_min_fresh_station_coverage_ratio,
        require_ready_status=metar_ingest_require_ready_status,
        quality_status=metar_ingest_quality_context.get("quality_status"),
        quality_score=metar_ingest_quality_context.get("quality_score"),
        fresh_station_coverage_ratio=metar_ingest_quality_context.get("fresh_station_coverage_ratio"),
    )
    alpha_consensus_by_market_side, alpha_consensus_meta = _load_alpha_consensus(
        output_dir=output_dir,
        alpha_consensus_json=alpha_consensus_json,
    )
    ws_path, market_sequences, ws_payload = _load_market_sequences(
        ws_state_json=ws_state_json,
        output_dir=output_dir,
    )
    # Market-sequence matching is a live-execution safety check. For shadow and
    # intents-only discovery we keep it permissive to avoid suppressing valid
    # opportunities due to transient websocket coverage gaps.
    effective_require_market_snapshot_seq = bool(require_market_snapshot_seq and allow_live_orders)
    constraint_status_counts = Counter(_normalize_text(row.get("constraint_status")).lower() or "unknown" for row in constraint_rows)
    constraint_status_counts_sorted = dict(sorted(constraint_status_counts.items(), key=lambda item: (-item[1], item[0])))
    actionable_constraint_rows = sum(
        1 for row in constraint_rows if (_normalize_text(row.get("constraint_status")).lower() in _ACTIONABLE_CONSTRAINTS)
    )
    constraint_market_tickers = {
        _normalize_text(row.get("market_ticker")) for row in constraint_rows if _normalize_text(row.get("market_ticker"))
    }
    active_settlement_stations = {
        _normalize_text(row.get("settlement_station")).upper()
        for row in constraint_rows
        if _normalize_text(row.get("settlement_station"))
    }
    ws_tickers_with_sequence = {
        _normalize_text(ticker)
        for ticker, sequence in market_sequences.items()
        if _normalize_text(ticker) and sequence is not None
    }
    ws_constraint_ticker_overlap = len(constraint_market_tickers & ws_tickers_with_sequence)

    intents = build_temperature_trade_intents(
        constraint_rows=constraint_rows,
        specs_by_ticker=specs_by_ticker,
        metar_context=metar_context,
        market_sequences=market_sequences,
        alpha_consensus_by_market_side=alpha_consensus_by_market_side,
        policy_version=policy_version,
        contracts_per_order=contracts_per_order,
        yes_max_entry_price_dollars=yes_max_entry_price_dollars,
        no_max_entry_price_dollars=no_max_entry_price_dollars,
        now=captured_at,
    )
    (
        exclude_market_tickers_requested,
        excluded_market_tickers,
        exclude_market_side_targets_requested,
        excluded_market_side_targets,
    ) = _parse_market_ticker_exclusions(exclude_market_tickers)
    excluded_intents_by_market_ticker_count = 0
    excluded_intents_by_market_side_count = 0
    excluded_intents_by_market_target_count = 0
    exclude_market_tickers_applied: list[str] = []
    exclude_market_side_targets_applied: list[str] = []
    if excluded_market_tickers or excluded_market_side_targets:
        filtered_intents: list[TemperatureTradeIntent] = []
        excluded_market_tickers_applied_set: set[str] = set()
        excluded_market_side_targets_applied_set: set[str] = set()
        for intent in intents:
            intent_market_ticker = _normalize_text(intent.market_ticker).upper()
            if intent_market_ticker and intent_market_ticker in excluded_market_tickers:
                excluded_intents_by_market_ticker_count += 1
                excluded_intents_by_market_target_count += 1
                excluded_market_tickers_applied_set.add(intent_market_ticker)
                continue
            intent_side = _normalize_text(intent.side).lower()
            intent_market_side_key = (
                f"{intent_market_ticker}|{intent_side}"
                if intent_market_ticker and intent_side in {"yes", "no"}
                else ""
            )
            if intent_market_side_key and intent_market_side_key in excluded_market_side_targets:
                excluded_intents_by_market_side_count += 1
                excluded_intents_by_market_target_count += 1
                excluded_market_side_targets_applied_set.add(intent_market_side_key)
                continue
            filtered_intents.append(intent)
        intents = filtered_intents
        exclude_market_tickers_applied = [
            ticker
            for ticker in exclude_market_tickers_requested
            if ticker in excluded_market_tickers_applied_set
        ]
        exclude_market_side_targets_applied = [
            market_side
            for market_side in exclude_market_side_targets_requested
            if market_side in excluded_market_side_targets_applied_set
        ]
    intent_status_counts = Counter(_normalize_text(intent.constraint_status).lower() or "unknown" for intent in intents)
    intent_status_counts_sorted = dict(sorted(intent_status_counts.items(), key=lambda item: (-item[1], item[0])))
    expanded_actionable_intents = sum(
        1 for intent in intents if (_normalize_text(intent.constraint_status).lower() in _ACTIONABLE_CONSTRAINTS)
    )

    metar_age_policy = _load_metar_age_policy(metar_age_policy_json=metar_age_policy_json)
    station_metar_age_overrides = metar_age_policy.get("station_overrides")
    if not isinstance(station_metar_age_overrides, dict):
        station_metar_age_overrides = {}
    station_hour_metar_age_overrides = metar_age_policy.get("station_hour_overrides")
    if not isinstance(station_hour_metar_age_overrides, dict):
        station_hour_metar_age_overrides = {}
    station_interval_stats = metar_context.get("station_interval_stats")
    if not isinstance(station_interval_stats, dict):
        station_interval_stats = {}
    adaptive_station_overrides, adaptive_station_diagnostics = _build_adaptive_station_metar_age_overrides(
        base_max_metar_age_minutes=max_metar_age_minutes,
        station_interval_stats=station_interval_stats,
        latest_by_station=metar_context.get("latest_by_station") if isinstance(metar_context.get("latest_by_station"), dict) else {},
        now_utc=captured_at,
        active_stations=active_settlement_stations,
    )
    effective_station_metar_age_overrides = dict(adaptive_station_overrides)
    # Explicit policy JSON overrides always take precedence over adaptive defaults.
    effective_station_metar_age_overrides.update(station_metar_age_overrides)
    weather_pattern_profile_max_age_hours = max(0.0, float(weather_pattern_profile_max_age_hours))
    weather_pattern_min_bucket_samples = max(1, int(_parse_float(weather_pattern_min_bucket_samples) or 1))
    weather_pattern_negative_expectancy_threshold = max(
        -0.25,
        min(
            -0.005,
            float(_parse_float(weather_pattern_negative_expectancy_threshold) or -0.05),
        ),
    )
    weather_pattern_negative_regime_suppression_min_bucket_samples = max(
        1,
        int(_parse_float(weather_pattern_negative_regime_suppression_min_bucket_samples) or 24),
    )
    weather_pattern_negative_regime_suppression_expectancy_threshold = max(
        -0.25,
        min(
            -0.005,
            float(
                _parse_float(weather_pattern_negative_regime_suppression_expectancy_threshold)
                or -0.08
            ),
        ),
    )
    weather_pattern_negative_regime_suppression_top_n = max(
        1,
        min(
            64,
            int(_parse_float(weather_pattern_negative_regime_suppression_top_n) or 8),
        ),
    )
    weather_pattern_risk_off_concentration_threshold = _clamp_unit(
        weather_pattern_risk_off_concentration_threshold,
        0.75,
    )
    weather_pattern_risk_off_min_attempts = max(
        1,
        int(_parse_float(weather_pattern_risk_off_min_attempts) or 24),
    )
    weather_pattern_risk_off_stale_metar_share_threshold = _clamp_unit(
        weather_pattern_risk_off_stale_metar_share_threshold,
        0.50,
    )
    weather_pattern_profile_effective, weather_pattern_profile_meta = _load_weather_pattern_profile(
        output_dir=output_dir,
        now_utc=captured_at,
        enabled=bool(weather_pattern_hardening_enabled),
        weather_pattern_profile=weather_pattern_profile,
        max_age_hours=weather_pattern_profile_max_age_hours,
    )
    adaptive_policy_resolved, adaptive_policy_profile_summary = _resolve_adaptive_policy_profile(
        adaptive_policy_profile=adaptive_policy_profile,
        min_settlement_confidence=min_settlement_confidence,
        min_probability_confidence=min_probability_confidence,
        min_expected_edge_net=min_expected_edge_net,
        min_edge_to_risk_ratio=min_edge_to_risk_ratio,
        max_intents_per_underlying=max_intents_per_underlying,
    )
    historical_selection_quality_profile = load_temperature_selection_quality_profile(
        output_dir=output_dir,
        now_utc=captured_at,
        enabled=bool(historical_selection_quality_enabled),
        lookback_hours=max(1.0, float(historical_selection_quality_lookback_hours)),
        min_resolved_market_sides=max(1, int(historical_selection_quality_min_resolved_market_sides)),
        min_bucket_samples=max(1, int(historical_selection_quality_min_bucket_samples)),
        preferred_attribution_model=_normalize_text(historical_selection_quality_preferred_model)
        or "fixed_fraction_per_underlying_family",
        max_profile_age_hours=max(0.0, float(historical_selection_quality_profile_max_age_hours)),
    )

    gate = TemperaturePolicyGate(
        min_settlement_confidence=float(min_settlement_confidence),
        max_metar_age_minutes=max_metar_age_minutes,
        station_max_metar_age_minutes=effective_station_metar_age_overrides,
        station_local_hour_max_metar_age_minutes=station_hour_metar_age_overrides,
        min_alpha_strength=min_alpha_strength,
        min_probability_confidence=adaptive_policy_resolved["min_probability_confidence"],
        min_expected_edge_net=adaptive_policy_resolved["min_expected_edge_net"],
        min_edge_to_risk_ratio=adaptive_policy_resolved["min_edge_to_risk_ratio"],
        min_base_edge_net=min_base_edge_net,
        min_probability_breakeven_gap=min_probability_breakeven_gap,
        enforce_probability_edge_thresholds=bool(enforce_probability_edge_thresholds),
        enforce_entry_price_probability_floor=bool(enforce_entry_price_probability_floor),
        fallback_min_probability_confidence=fallback_min_probability_confidence,
        fallback_min_expected_edge_net=fallback_min_expected_edge_net,
        fallback_min_edge_to_risk_ratio=fallback_min_edge_to_risk_ratio,
        enforce_interval_consistency=bool(enforce_interval_consistency),
        max_yes_possible_gap_for_yes_side=float(max_yes_possible_gap_for_yes_side),
        min_hours_to_close=min_hours_to_close,
        max_hours_to_close=max_hours_to_close,
        max_intents_per_underlying=int(adaptive_policy_resolved["max_intents_per_underlying"]),
        taf_stale_grace_minutes=max(0.0, float(taf_stale_grace_minutes)),
        taf_stale_grace_max_volatility_score=(
            float(taf_stale_grace_max_volatility_score)
            if isinstance(taf_stale_grace_max_volatility_score, (int, float))
            else None
        ),
        taf_stale_grace_max_range_width=(
            float(taf_stale_grace_max_range_width)
            if isinstance(taf_stale_grace_max_range_width, (int, float))
            else None
        ),
        metar_freshness_quality_boundary_ratio=(
            max(0.0, min(1.0, float(metar_freshness_quality_boundary_ratio)))
            if isinstance(metar_freshness_quality_boundary_ratio, (int, float))
            else None
        ),
        metar_freshness_quality_probability_margin=max(
            0.0,
            float(metar_freshness_quality_probability_margin),
        ),
        metar_freshness_quality_expected_edge_margin=max(
            0.0,
            float(metar_freshness_quality_expected_edge_margin),
        ),
        metar_ingest_quality_gate_enabled=bool(metar_ingest_quality_gate_state.get("enabled")),
        metar_ingest_min_quality_score=metar_ingest_quality_gate_state.get("min_quality_score"),
        metar_ingest_min_fresh_station_coverage_ratio=metar_ingest_quality_gate_state.get(
            "min_fresh_station_coverage_ratio"
        ),
        metar_ingest_require_ready_status=bool(metar_ingest_quality_gate_state.get("require_ready_status")),
        metar_ingest_quality_score=metar_ingest_quality_context.get("quality_score"),
        metar_ingest_quality_grade=_normalize_text(metar_ingest_quality_context.get("quality_grade")),
        metar_ingest_quality_status=_normalize_text(metar_ingest_quality_context.get("quality_status")),
        metar_ingest_quality_signal_count=int(
            _parse_float(metar_ingest_quality_context.get("quality_signal_count")) or 0
        ),
        metar_ingest_quality_signals=(
            list(metar_ingest_quality_context.get("quality_signals"))
            if isinstance(metar_ingest_quality_context.get("quality_signals"), list)
            else []
        ),
        metar_ingest_fresh_station_coverage_ratio=metar_ingest_quality_context.get(
            "fresh_station_coverage_ratio"
        ),
        high_price_edge_guard_enabled=bool(high_price_edge_guard_enabled),
        high_price_edge_guard_min_entry_price_dollars=float(
            _parse_float(high_price_edge_guard_min_entry_price_dollars) or 0.85
        ),
        high_price_edge_guard_min_expected_edge_net=max(
            0.0,
            float(_parse_float(high_price_edge_guard_min_expected_edge_net) or 0.0),
        ),
        high_price_edge_guard_min_edge_to_risk_ratio=max(
            0.0,
            float(_parse_float(high_price_edge_guard_min_edge_to_risk_ratio) or 0.02),
        ),
        historical_selection_quality_profile=historical_selection_quality_profile,
        historical_quality_probability_penalty_max=max(
            0.0,
            float(historical_selection_quality_probability_penalty_max),
        ),
        historical_quality_expected_edge_penalty_max=max(
            0.0,
            float(historical_selection_quality_expected_edge_penalty_max),
        ),
        historical_quality_score_adjust_scale=max(
            0.0,
            float(historical_selection_quality_score_adjust_scale),
        ),
        weather_pattern_hardening_enabled=bool(weather_pattern_hardening_enabled),
        weather_pattern_profile=weather_pattern_profile_effective,
        weather_pattern_profile_max_age_hours=weather_pattern_profile_max_age_hours,
        weather_pattern_min_bucket_samples=weather_pattern_min_bucket_samples,
        weather_pattern_negative_expectancy_threshold=weather_pattern_negative_expectancy_threshold,
        weather_pattern_negative_regime_suppression_enabled=bool(
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
        weather_pattern_risk_off_enabled=bool(weather_pattern_risk_off_enabled),
        weather_pattern_risk_off_concentration_threshold=weather_pattern_risk_off_concentration_threshold,
        weather_pattern_risk_off_min_attempts=weather_pattern_risk_off_min_attempts,
        weather_pattern_risk_off_stale_metar_share_threshold=weather_pattern_risk_off_stale_metar_share_threshold,
        require_market_snapshot_seq=effective_require_market_snapshot_seq,
        require_metar_snapshot_sha=bool(require_metar_snapshot_sha),
    )
    gate_candidate_intents, settlement_prefilter_decisions, settlement_prefilter_reason_counts = (
        _prefilter_settlement_blocked_intents(
            intents=intents,
            settlement_state_by_underlying=settlement_state_by_underlying,
        )
    )
    evaluated_decisions = gate.evaluate(
        intents=gate_candidate_intents,
        existing_underlying_slots=existing_underlying_slots,
        settlement_state_by_underlying=settlement_state_by_underlying,
    )
    weather_pattern_risk_off_state = (
        dict(gate.weather_pattern_risk_off_state)
        if isinstance(gate.weather_pattern_risk_off_state, dict)
        else {}
    )
    weather_pattern_negative_regime_suppression_state = (
        dict(gate.weather_pattern_negative_regime_suppression_state)
        if isinstance(gate.weather_pattern_negative_regime_suppression_state, dict)
        else {}
    )
    decisions_by_id: dict[str, TemperaturePolicyDecision] = {
        decision.intent_id: decision for decision in evaluated_decisions
    }
    for decision in settlement_prefilter_decisions:
        decisions_by_id[decision.intent_id] = decision
    decisions = [
        decisions_by_id[intent.intent_id]
        for intent in intents
        if _normalize_text(intent.intent_id) and intent.intent_id in decisions_by_id
    ]
    approved_intents = [
        intent for intent in intents if decisions_by_id.get(intent.intent_id, None) and decisions_by_id[intent.intent_id].approved
    ]
    shadow_quote_probe_targeted_requested = shadow_quote_probe_market_side_targets is not None
    shadow_quote_probe_targeted_keys, shadow_quote_probe_target_specs = _parse_shadow_quote_probe_target_keys(
        shadow_quote_probe_market_side_targets
    )
    shadow_quote_probe_targeted_applied = False
    shadow_quote_probe_targeted_match_count = 0

    revalidated_intents, revalidation_invalidations, revalidation_meta = revalidate_temperature_trade_intents(
        intents=approved_intents,
        output_dir=output_dir,
        specs_csv=str(specs_path) if specs_path is not None else None,
        metar_summary_json=_normalize_text(metar_context.get("summary_path")) or metar_summary_json,
        metar_state_json=_normalize_text(metar_context.get("state_path")) or metar_state_json,
        ws_state_json=ws_path or ws_state_json,
        require_market_snapshot_seq=effective_require_market_snapshot_seq,
        require_metar_snapshot_sha=bool(require_metar_snapshot_sha),
    )
    revalidation_by_id = {
        _normalize_text(row.get("intent_id")): row
        for row in revalidation_invalidations
        if _normalize_text(row.get("intent_id"))
    }

    order_group_id = _build_order_group_id(
        metar_snapshot_sha=_normalize_text(metar_context.get("raw_sha256")),
        captured_at=captured_at,
    )
    bridge = TemperatureExecutionBridge(contracts_per_order=max(1, int(contracts_per_order)))
    portfolio_planner = TemperaturePortfolioPlanner(
        max_total_deployed_pct=float(max_total_deployed_pct),
        max_same_station_exposure_pct=float(max_same_station_exposure_pct),
        max_same_hour_cluster_exposure_pct=float(max_same_hour_cluster_exposure_pct),
        max_same_underlying_exposure_pct=float(max_same_underlying_exposure_pct),
        max_orders_per_station=max(0, int(max_orders_per_station)),
        max_orders_per_underlying=max(0, int(max_orders_per_underlying)),
        min_unique_stations_per_loop=max(0, int(min_unique_stations_per_loop)),
        min_unique_underlyings_per_loop=max(0, int(min_unique_underlyings_per_loop)),
        min_unique_local_hours_per_loop=max(0, int(min_unique_local_hours_per_loop)),
        historical_quality_score_adjust_scale=max(
            0.0,
            float(historical_selection_quality_score_adjust_scale),
        ),
    )
    capped_intents, allocation_summary = portfolio_planner.select_intents(
        intents=revalidated_intents,
        decisions_by_id=decisions_by_id,
        max_orders=max_orders,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
    )
    capped_intents, replan_cooldown_meta = _apply_market_side_replan_cooldown(
        intents=capped_intents,
        output_dir=output_dir,
        now_utc=captured_at,
        cooldown_minutes=replan_market_side_cooldown_minutes,
        repeat_window_minutes=replan_market_side_repeat_window_minutes,
        max_plans_per_window=replan_market_side_max_plans_per_window,
        price_change_override_dollars=replan_market_side_price_change_override_dollars,
        alpha_change_override=replan_market_side_alpha_change_override,
        confidence_change_override=replan_market_side_confidence_change_override,
        min_observation_advance_minutes=replan_market_side_min_observation_advance_minutes,
        max_history_files=replan_market_side_history_files,
        min_orders_backstop=replan_market_side_min_orders_backstop,
    )
    plans = [
        bridge.to_plan(intent=intent, rank=index + 1, order_group_id=order_group_id)
        for index, intent in enumerate(capped_intents)
    ]
    shadow_quote_probe_requested = bool(shadow_quote_probe_on_no_candidates)
    shadow_quote_probe_applied = False
    shadow_quote_probe_reason = "disabled"
    shadow_quote_probe_source = "none"
    shadow_quote_probe_candidate_intents = 0
    shadow_quote_probe_planned_orders = 0
    shadow_quote_probe_market_tickers: list[str] = []

    if shadow_quote_probe_requested:
        if intents_only:
            shadow_quote_probe_reason = "intents_only_mode"
        elif allow_live_orders:
            shadow_quote_probe_reason = "blocked_live_mode"
        elif plans:
            shadow_quote_probe_reason = "not_needed_candidates_available"
        else:
            probe_source_intents: list[TemperatureTradeIntent]
            if revalidated_intents:
                probe_source_intents = list(revalidated_intents)
                shadow_quote_probe_source = "revalidated_intents"
            elif approved_intents:
                probe_source_intents = list(approved_intents)
                shadow_quote_probe_source = "approved_intents"
            else:
                probe_source_intents = list(intents)
                shadow_quote_probe_source = "all_intents"

            shadow_quote_probe_candidate_intents = len(probe_source_intents)
            safe_probe_limit = max(0, int(max_orders))
            if safe_probe_limit <= 0:
                shadow_quote_probe_reason = "max_orders_nonpositive"
            else:
                probe_intents: list[TemperatureTradeIntent] = []
                seen_market_side_keys: set[str] = set()

                def _append_probe_intent(intent: TemperatureTradeIntent) -> bool:
                    market_ticker = _normalize_text(getattr(intent, "market_ticker", ""))
                    side = _normalize_text(getattr(intent, "side", "")).lower()
                    if not market_ticker or side not in {"yes", "no"}:
                        return False
                    market_side_key = f"{market_ticker}|{side}"
                    if market_side_key in seen_market_side_keys:
                        return False
                    seen_market_side_keys.add(market_side_key)
                    probe_intents.append(intent)
                    return True

                if shadow_quote_probe_target_specs:
                    for target_market_ticker, target_side in shadow_quote_probe_target_specs:
                        matched_intent: TemperatureTradeIntent | None = None
                        for intent in probe_source_intents:
                            intent_market_ticker = _normalize_text(getattr(intent, "market_ticker", ""))
                            intent_side = _normalize_text(getattr(intent, "side", "")).lower()
                            if not intent_market_ticker or intent_market_ticker != target_market_ticker:
                                continue
                            if target_side is not None and intent_side != target_side:
                                continue
                            if intent_side not in {"yes", "no"}:
                                continue
                            market_side_key = f"{intent_market_ticker}|{intent_side}"
                            if market_side_key in seen_market_side_keys:
                                continue
                            matched_intent = intent
                            break
                        if matched_intent is None:
                            continue
                        if _append_probe_intent(matched_intent):
                            shadow_quote_probe_targeted_match_count += 1
                            if len(probe_intents) >= safe_probe_limit:
                                break

                for intent in probe_source_intents:
                    if len(probe_intents) >= safe_probe_limit:
                        break
                    _append_probe_intent(intent)
                    if len(probe_intents) >= safe_probe_limit:
                        break

                if not probe_intents:
                    shadow_quote_probe_reason = "no_probe_intents"
                else:
                    shadow_quote_probe_targeted_applied = shadow_quote_probe_targeted_match_count > 0
                    probe_plans: list[dict[str, Any]] = []
                    for index, intent in enumerate(probe_intents):
                        probe_plan = bridge.to_plan(
                            intent=intent,
                            rank=index + 1,
                            order_group_id=order_group_id,
                        )
                        probe_plan["shadow_quote_probe"] = True
                        probe_plan["shadow_quote_probe_source"] = shadow_quote_probe_source
                        probe_plans.append(probe_plan)
                    plans = probe_plans
                    shadow_quote_probe_applied = bool(plans)
                    shadow_quote_probe_planned_orders = len(plans)
                    shadow_quote_probe_market_tickers = sorted(
                        {
                            _normalize_text(plan.get("market_ticker"))
                            for plan in plans
                            if _normalize_text(plan.get("market_ticker"))
                        }
                    )
                    shadow_quote_probe_reason = (
                        "activated_no_candidates" if shadow_quote_probe_applied else "probe_plan_build_failed"
                    )

    stamp = _resolve_trader_output_stamp(output_dir=out_dir, captured_at=captured_at)
    intents_csv_path = out_dir / f"kalshi_temperature_trade_intents_{stamp}.csv"
    _write_intents_csv(
        path=intents_csv_path,
        intents=intents,
        decisions_by_id=decisions_by_id,
        revalidation_by_id=revalidation_by_id,
    )

    plan_csv_path = out_dir / f"kalshi_temperature_trade_plan_{stamp}.csv"
    _write_plan_csv(plan_csv_path, plans)
    finalization_snapshot = _build_settlement_finalization_snapshot(
        intents=intents,
        settlement_state_by_underlying=settlement_state_by_underlying,
        captured_at=captured_at,
    )
    finalization_snapshot_path = out_dir / f"kalshi_temperature_finalization_snapshot_{stamp}.json"
    finalization_snapshot_path.write_text(json.dumps(finalization_snapshot, indent=2), encoding="utf-8")

    policy_reason_counts: dict[str, int] = {}
    for decision in decisions:
        key = decision.decision_reason
        policy_reason_counts[key] = policy_reason_counts.get(key, 0) + 1

    intent_alpha_strengths = [_intent_alpha_strength(intent) for intent in intents]
    approved_alpha_strengths = [_intent_alpha_strength(intent) for intent in revalidated_intents]
    intent_yes_gap_values = [
        float(intent.yes_possible_gap)
        for intent in intents
        if isinstance(intent.yes_possible_gap, (int, float))
    ]
    approved_yes_gap_values = [
        float(intent.yes_possible_gap)
        for intent in revalidated_intents
        if isinstance(intent.yes_possible_gap, (int, float))
    ]
    intents_with_overlap = [
        intent
        for intent in intents
        if isinstance(intent.yes_possible_overlap, bool)
    ]
    approved_with_overlap = [
        intent
        for intent in revalidated_intents
        if isinstance(intent.yes_possible_overlap, bool)
    ]
    planned_alpha_strength_values = [
        float(plan.get("temperature_alpha_strength"))
        for plan in plans
        if isinstance(plan.get("temperature_alpha_strength"), (int, float))
    ]
    intent_shock_confidence_values = [
        float(intent.speci_shock_confidence)
        for intent in intents
        if isinstance(intent.speci_shock_confidence, (int, float))
    ]
    approved_shock_confidence_values = [
        float(intent.speci_shock_confidence)
        for intent in revalidated_intents
        if isinstance(intent.speci_shock_confidence, (int, float))
    ]
    intent_shock_weight_values = [
        float(intent.speci_shock_weight)
        for intent in intents
        if isinstance(intent.speci_shock_weight, (int, float))
    ]
    approved_shock_weight_values = [
        float(intent.speci_shock_weight)
        for intent in revalidated_intents
        if isinstance(intent.speci_shock_weight, (int, float))
    ]
    planned_shock_confidence_values = [
        float(plan.get("temperature_speci_shock_confidence"))
        for plan in plans
        if isinstance(plan.get("temperature_speci_shock_confidence"), (int, float))
    ]
    planned_shock_weight_values = [
        float(plan.get("temperature_speci_shock_weight"))
        for plan in plans
        if isinstance(plan.get("temperature_speci_shock_weight"), (int, float))
    ]
    intents_consensus_alpha_values = [
        float(intent.consensus_alpha_score)
        for intent in intents
        if isinstance(intent.consensus_alpha_score, (int, float))
    ]
    approved_consensus_alpha_values = [
        float(intent.consensus_alpha_score)
        for intent in revalidated_intents
        if isinstance(intent.consensus_alpha_score, (int, float))
    ]
    intents_consensus_support_ratio_values = [
        float(intent.consensus_profile_support_ratio)
        for intent in intents
        if isinstance(intent.consensus_profile_support_ratio, (int, float))
    ]
    approved_consensus_support_ratio_values = [
        float(intent.consensus_profile_support_ratio)
        for intent in revalidated_intents
        if isinstance(intent.consensus_profile_support_ratio, (int, float))
    ]
    intents_alpha_strength_avg = _average(intent_alpha_strengths)
    approved_alpha_strength_avg = _average(approved_alpha_strengths)
    intents_yes_gap_avg = _average(intent_yes_gap_values)
    approved_yes_gap_avg = _average(approved_yes_gap_values)
    planned_alpha_strength_avg = _average(planned_alpha_strength_values)
    intents_speci_shock_confidence_avg = _average(intent_shock_confidence_values)
    approved_speci_shock_confidence_avg = _average(approved_shock_confidence_values)
    planned_speci_shock_confidence_avg = _average(planned_shock_confidence_values)
    intents_speci_shock_weight_avg = _average(intent_shock_weight_values)
    approved_speci_shock_weight_avg = _average(approved_shock_weight_values)
    planned_speci_shock_weight_avg = _average(planned_shock_weight_values)
    intents_consensus_alpha_avg = _average(intents_consensus_alpha_values)
    approved_consensus_alpha_avg = _average(approved_consensus_alpha_values)
    intents_consensus_support_ratio_avg = _average(intents_consensus_support_ratio_values)
    approved_consensus_support_ratio_avg = _average(approved_consensus_support_ratio_values)
    decision_probability_values = [
        float(decision.probability_confidence)
        for decision in decisions
        if isinstance(decision.probability_confidence, (int, float))
    ]
    approved_decision_probability_values = [
        float(decision.probability_confidence)
        for decision in decisions
        if decision.approved and isinstance(decision.probability_confidence, (int, float))
    ]
    decision_expected_edge_values = [
        float(decision.expected_edge_net)
        for decision in decisions
        if isinstance(decision.expected_edge_net, (int, float))
    ]
    approved_decision_expected_edge_values = [
        float(decision.expected_edge_net)
        for decision in decisions
        if decision.approved and isinstance(decision.expected_edge_net, (int, float))
    ]
    decision_base_edge_values = [
        float(decision.base_edge_net)
        for decision in decisions
        if isinstance(decision.base_edge_net, (int, float))
    ]
    approved_decision_base_edge_values = [
        float(decision.base_edge_net)
        for decision in decisions
        if decision.approved and isinstance(decision.base_edge_net, (int, float))
    ]
    decision_edge_to_risk_values = [
        float(decision.edge_to_risk_ratio)
        for decision in decisions
        if isinstance(decision.edge_to_risk_ratio, (int, float))
    ]
    approved_decision_edge_to_risk_values = [
        float(decision.edge_to_risk_ratio)
        for decision in decisions
        if decision.approved and isinstance(decision.edge_to_risk_ratio, (int, float))
    ]
    intent_by_id = {intent.intent_id: intent for intent in intents}
    approved_probability_below_min_count = sum(
        1
        for decision in decisions
        if decision.approved
        and isinstance(decision.probability_confidence, (int, float))
        and isinstance(decision.min_probability_confidence_required, (int, float))
        and float(decision.probability_confidence) < float(decision.min_probability_confidence_required)
    )
    approved_expected_edge_below_min_count = sum(
        1
        for decision in decisions
        if decision.approved
        and isinstance(decision.expected_edge_net, (int, float))
        and isinstance(decision.min_expected_edge_net_required, (int, float))
        and float(decision.expected_edge_net) < float(decision.min_expected_edge_net_required)
    )
    approved_edge_to_risk_below_min_count = sum(
        1
        for decision in decisions
        if decision.approved
        and isinstance(decision.edge_to_risk_ratio, (int, float))
        and isinstance(decision.min_edge_to_risk_ratio_required, (int, float))
        and float(decision.edge_to_risk_ratio) < float(decision.min_edge_to_risk_ratio_required)
    )
    approved_base_edge_below_min_count = sum(
        1
        for decision in decisions
        if decision.approved
        and isinstance(decision.base_edge_net, (int, float))
        and isinstance(decision.min_base_edge_net_required, (int, float))
        and float(decision.base_edge_net) < float(decision.min_base_edge_net_required)
    )
    approved_probability_breakeven_gap_below_min_count = 0
    for decision in decisions:
        if not decision.approved:
            continue
        if not isinstance(decision.probability_confidence, (int, float)):
            continue
        if not isinstance(decision.min_probability_breakeven_gap_required, (int, float)):
            continue
        source_intent = intent_by_id.get(decision.intent_id)
        if not isinstance(source_intent, TemperatureTradeIntent):
            continue
        breakeven_probability = max(0.0, min(0.999, float(source_intent.max_entry_price_dollars)))
        probability_breakeven_gap = float(decision.probability_confidence) - breakeven_probability
        if probability_breakeven_gap < float(decision.min_probability_breakeven_gap_required):
            approved_probability_breakeven_gap_below_min_count += 1
    effective_probability_threshold_values = sorted(
        {
            round(float(decision.min_probability_confidence_required), 6)
            for decision in decisions
            if isinstance(decision.min_probability_confidence_required, (int, float))
        }
    )
    effective_expected_edge_threshold_values = sorted(
        {
            round(float(decision.min_expected_edge_net_required), 6)
            for decision in decisions
            if isinstance(decision.min_expected_edge_net_required, (int, float))
        }
    )
    effective_alpha_strength_threshold_values = sorted(
        {
            round(float(decision.min_alpha_strength_required), 6)
            for decision in decisions
            if isinstance(decision.min_alpha_strength_required, (int, float))
        }
    )
    effective_edge_to_risk_threshold_values = sorted(
        {
            round(float(decision.min_edge_to_risk_ratio_required), 6)
            for decision in decisions
            if isinstance(decision.min_edge_to_risk_ratio_required, (int, float))
        }
    )
    effective_base_edge_threshold_values = sorted(
        {
            round(float(decision.min_base_edge_net_required), 6)
            for decision in decisions
            if isinstance(decision.min_base_edge_net_required, (int, float))
        }
    )
    effective_probability_breakeven_gap_threshold_values = sorted(
        {
            round(float(decision.min_probability_breakeven_gap_required), 6)
            for decision in decisions
            if isinstance(decision.min_probability_breakeven_gap_required, (int, float))
        }
    )
    intents_probability_confidence_avg = _average(decision_probability_values)
    approved_probability_confidence_avg = _average(approved_decision_probability_values)
    intents_expected_edge_net_avg = _average(decision_expected_edge_values)
    approved_expected_edge_net_avg = _average(approved_decision_expected_edge_values)
    intents_base_edge_net_avg = _average(decision_base_edge_values)
    approved_base_edge_net_avg = _average(approved_decision_base_edge_values)
    intents_edge_to_risk_ratio_avg = _average(decision_edge_to_risk_values)
    approved_edge_to_risk_ratio_avg = _average(approved_decision_edge_to_risk_values)
    sparse_hardening_decisions = [
        decision
        for decision in decisions
        if bool(decision.sparse_evidence_hardening_applied)
    ]
    sparse_hardening_probability_raise_values = [
        float(decision.sparse_evidence_probability_raise)
        for decision in sparse_hardening_decisions
        if isinstance(decision.sparse_evidence_probability_raise, (int, float))
    ]
    sparse_hardening_expected_edge_raise_values = [
        float(decision.sparse_evidence_expected_edge_raise)
        for decision in sparse_hardening_decisions
        if isinstance(decision.sparse_evidence_expected_edge_raise, (int, float))
    ]
    sparse_hardening_support_score_values = [
        float(decision.sparse_evidence_support_score)
        for decision in sparse_hardening_decisions
        if isinstance(decision.sparse_evidence_support_score, (int, float))
    ]
    sparse_hardening_volatility_penalty_values = [
        float(decision.sparse_evidence_volatility_penalty)
        for decision in sparse_hardening_decisions
        if isinstance(decision.sparse_evidence_volatility_penalty, (int, float))
    ]
    sparse_hardening_applied_count = len(sparse_hardening_decisions)
    sparse_hardening_approved_count = sum(
        1 for decision in sparse_hardening_decisions if decision.approved
    )
    sparse_hardening_blocked_count = sparse_hardening_applied_count - sparse_hardening_approved_count
    sparse_hardening_blocked_probability_below_min_count = 0
    sparse_hardening_blocked_expected_edge_below_min_count = 0
    sparse_hardening_blocked_edge_to_risk_below_min_count = 0
    for decision in sparse_hardening_decisions:
        if decision.approved:
            continue
        note_tokens = {
            token.strip()
            for token in str(decision.decision_notes).split(",")
            if token.strip()
        }
        if "probability_confidence_below_min" in note_tokens:
            sparse_hardening_blocked_probability_below_min_count += 1
        if "expected_edge_below_min" in note_tokens:
            sparse_hardening_blocked_expected_edge_below_min_count += 1
        if "edge_to_risk_ratio_below_min" in note_tokens:
            sparse_hardening_blocked_edge_to_risk_below_min_count += 1

    historical_quality_probability_raise_values = [
        float(decision.historical_quality_probability_raise)
        for decision in decisions
        if isinstance(decision.historical_quality_probability_raise, (int, float))
        and float(decision.historical_quality_probability_raise) > 0.0
    ]
    historical_quality_expected_edge_raise_values = [
        float(decision.historical_quality_expected_edge_raise)
        for decision in decisions
        if isinstance(decision.historical_quality_expected_edge_raise, (int, float))
        and float(decision.historical_quality_expected_edge_raise) > 0.0
    ]
    historical_quality_penalty_ratio_values = [
        float(decision.historical_quality_penalty_ratio)
        for decision in decisions
        if isinstance(decision.historical_quality_penalty_ratio, (int, float))
        and float(decision.historical_quality_penalty_ratio) > 0.0
    ]
    historical_quality_boost_ratio_values = [
        float(decision.historical_quality_boost_ratio)
        for decision in decisions
        if isinstance(decision.historical_quality_boost_ratio, (int, float))
        and float(decision.historical_quality_boost_ratio) > 0.0
    ]
    historical_quality_score_adjustment_values = [
        float(decision.historical_quality_score_adjustment)
        for decision in decisions
        if isinstance(decision.historical_quality_score_adjustment, (int, float))
    ]
    historical_quality_global_only_adjusted_share_values = [
        float(decision.historical_quality_global_only_adjusted_share)
        for decision in decisions
        if isinstance(decision.historical_quality_global_only_adjusted_share, (int, float))
    ]
    historical_quality_global_only_excess_ratio_values = [
        float(decision.historical_quality_global_only_excess_ratio)
        for decision in decisions
        if isinstance(decision.historical_quality_global_only_excess_ratio, (int, float))
    ]
    historical_quality_global_only_pressure_active_count = sum(
        1 for decision in decisions if bool(decision.historical_quality_global_only_pressure_active)
    )
    historical_quality_adjusted_count = sum(
        1
        for decision in decisions
        if (
            isinstance(decision.historical_quality_probability_raise, (int, float))
            and float(decision.historical_quality_probability_raise) > 0.0
        )
        or (
            isinstance(decision.historical_quality_expected_edge_raise, (int, float))
            and float(decision.historical_quality_expected_edge_raise) > 0.0
        )
        or (
            isinstance(decision.historical_quality_penalty_ratio, (int, float))
            and float(decision.historical_quality_penalty_ratio) > 0.0
        )
        or (
            isinstance(decision.historical_quality_boost_ratio, (int, float))
            and float(decision.historical_quality_boost_ratio) > 0.0
        )
    )
    historical_profitability_guardrail_penalty_values = [
        float(decision.historical_profitability_guardrail_penalty_ratio)
        for decision in decisions
        if isinstance(decision.historical_profitability_guardrail_penalty_ratio, (int, float))
        and float(decision.historical_profitability_guardrail_penalty_ratio) > 0.0
    ]
    historical_profitability_guardrail_probability_raise_values = [
        float(decision.historical_profitability_guardrail_probability_raise)
        for decision in decisions
        if isinstance(decision.historical_profitability_guardrail_probability_raise, (int, float))
        and float(decision.historical_profitability_guardrail_probability_raise) > 0.0
    ]
    historical_profitability_guardrail_expected_edge_raise_values = [
        float(decision.historical_profitability_guardrail_expected_edge_raise)
        for decision in decisions
        if isinstance(decision.historical_profitability_guardrail_expected_edge_raise, (int, float))
        and float(decision.historical_profitability_guardrail_expected_edge_raise) > 0.0
    ]
    historical_profitability_bucket_guardrail_penalty_values = [
        float(decision.historical_profitability_bucket_guardrail_penalty_ratio)
        for decision in decisions
        if isinstance(decision.historical_profitability_bucket_guardrail_penalty_ratio, (int, float))
        and float(decision.historical_profitability_bucket_guardrail_penalty_ratio) > 0.0
    ]
    historical_profitability_bucket_guardrail_probability_raise_values = [
        float(decision.historical_profitability_bucket_guardrail_probability_raise)
        for decision in decisions
        if isinstance(decision.historical_profitability_bucket_guardrail_probability_raise, (int, float))
        and float(decision.historical_profitability_bucket_guardrail_probability_raise) > 0.0
    ]
    historical_profitability_bucket_guardrail_expected_edge_raise_values = [
        float(decision.historical_profitability_bucket_guardrail_expected_edge_raise)
        for decision in decisions
        if isinstance(decision.historical_profitability_bucket_guardrail_expected_edge_raise, (int, float))
        and float(decision.historical_profitability_bucket_guardrail_expected_edge_raise) > 0.0
    ]
    historical_profitability_guardrail_applied_count = len(historical_profitability_guardrail_penalty_values)
    historical_profitability_bucket_guardrail_applied_count = len(
        historical_profitability_bucket_guardrail_penalty_values
    )
    historical_profitability_guardrail_blocked_probability_count = 0
    historical_profitability_guardrail_blocked_expected_edge_count = 0
    historical_profitability_bucket_guardrail_blocked_probability_count = 0
    historical_profitability_bucket_guardrail_blocked_expected_edge_count = 0
    historical_quality_global_only_pressure_blocked_count = 0
    weather_pattern_probability_raise_count = 0
    weather_pattern_expected_edge_raise_count = 0
    weather_pattern_blocked_count = 0
    weather_pattern_risk_off_blocked_count = 0
    weather_pattern_negative_regime_suppression_blocked_count = 0
    metar_ingest_quality_blocked_count = 0
    high_price_edge_guard_blocked_count = 0
    weather_pattern_matched_bucket_evidence: list[str] = []
    weather_pattern_hard_block_evidence: list[str] = []
    for decision in decisions:
        weather_note_text = str(decision.decision_notes)
        if "weather_pattern_probability_raise=" in weather_note_text:
            weather_pattern_probability_raise_count += 1
        if "weather_pattern_expected_edge_raise=" in weather_note_text:
            weather_pattern_expected_edge_raise_count += 1
        if "weather_pattern_bucket_match=" in weather_note_text:
            for token in weather_note_text.split(","):
                token = token.strip()
                if token.startswith("weather_pattern_bucket_match="):
                    weather_pattern_matched_bucket_evidence.append(token.split("=", 1)[1])
        if "weather_pattern_hard_block_hits=" in weather_note_text:
            for token in weather_note_text.split(","):
                token = token.strip()
                if token.startswith("weather_pattern_hard_block_hits="):
                    evidence_blob = token.split("=", 1)[1]
                    weather_pattern_hard_block_evidence.extend(
                        [part for part in evidence_blob.split(";") if part]
                    )
        if decision.approved:
            continue
        note_tokens = {
            token.strip()
            for token in str(decision.decision_notes).split(",")
            if token.strip()
        }
        if "historical_profitability_probability_below_min" in note_tokens:
            historical_profitability_guardrail_blocked_probability_count += 1
        if "historical_profitability_expected_edge_below_min" in note_tokens:
            historical_profitability_guardrail_blocked_expected_edge_count += 1
        if "historical_profitability_bucket_guardrail_probability_raise=" in str(decision.decision_notes):
            if "probability_confidence_below_min" in note_tokens:
                historical_profitability_bucket_guardrail_blocked_probability_count += 1
        if "historical_profitability_bucket_guardrail_expected_edge_raise=" in str(decision.decision_notes):
            if "expected_edge_below_min" in note_tokens:
                historical_profitability_bucket_guardrail_blocked_expected_edge_count += 1
        if (
            decision.decision_reason == "historical_quality_global_only_pressure"
            or "historical_quality_global_only_pressure" in note_tokens
        ):
            historical_quality_global_only_pressure_blocked_count += 1
        if (
            decision.decision_reason == "weather_pattern_multi_bucket_hard_block"
            or "weather_pattern_multi_bucket_hard_block" in note_tokens
        ):
            weather_pattern_blocked_count += 1
        if (
            decision.decision_reason == "weather_pattern_global_risk_off"
            or "weather_pattern_global_risk_off" in note_tokens
        ):
            weather_pattern_risk_off_blocked_count += 1
        if (
            decision.decision_reason == "weather_pattern_negative_regime_bucket_suppressed"
            or "weather_pattern_negative_regime_bucket_suppressed" in note_tokens
        ):
            weather_pattern_negative_regime_suppression_blocked_count += 1
        if (
            decision.decision_reason == "metar_ingest_quality_insufficient"
            or "metar_ingest_quality_insufficient" in note_tokens
        ):
            metar_ingest_quality_blocked_count += 1
        if (
            decision.decision_reason in {
                "high_price_expected_edge_nonpositive",
                "high_price_edge_to_risk_ratio_below_min",
            }
            or "high_price_expected_edge_nonpositive" in note_tokens
            or "high_price_edge_to_risk_ratio_below_min" in note_tokens
        ):
            high_price_edge_guard_blocked_count += 1

    metar_ingest_gate_failure_reasons = (
        list(metar_ingest_quality_gate_state.get("failure_reasons"))
        if isinstance(metar_ingest_quality_gate_state.get("failure_reasons"), list)
        else []
    )
    metar_ingest_gate_enabled = bool(metar_ingest_quality_gate_state.get("enabled"))
    metar_ingest_gate_passed = bool(metar_ingest_quality_gate_state.get("passed"))
    canary_expansion_reasons: list[str] = []
    if not metar_ingest_gate_enabled:
        canary_expansion_reasons.append("metar_ingest_quality_gate_disabled")
    elif not metar_ingest_gate_passed:
        canary_expansion_reasons.append("metar_ingest_quality_gate_not_passing")
        canary_expansion_reasons.extend(
            [f"metar_ingest_{reason}" for reason in metar_ingest_gate_failure_reasons]
        )
    if bool(weather_pattern_risk_off_state.get("active")):
        canary_expansion_reasons.append("weather_pattern_global_risk_off_active")
    if not (revalidated_intents or approved_intents):
        canary_expansion_reasons.append("no_approved_or_revalidated_intents")
    canary_expansion_ready = not canary_expansion_reasons

    bridge_plan_summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready" if plans else "no_candidates",
        "policy_version": policy_version,
        "shadow_quote_probe_requested": bool(shadow_quote_probe_requested),
        "shadow_quote_probe_applied": bool(shadow_quote_probe_applied),
        "shadow_quote_probe_reason": shadow_quote_probe_reason,
        "shadow_quote_probe_source": shadow_quote_probe_source,
        "shadow_quote_probe_targeted_requested": bool(shadow_quote_probe_targeted_requested),
        "shadow_quote_probe_targeted_applied": bool(shadow_quote_probe_targeted_applied),
        "shadow_quote_probe_targeted_keys": shadow_quote_probe_targeted_keys[:20],
        "shadow_quote_probe_targeted_match_count": int(shadow_quote_probe_targeted_match_count),
        "shadow_quote_probe_candidate_intents": int(shadow_quote_probe_candidate_intents),
        "shadow_quote_probe_planned_orders": int(shadow_quote_probe_planned_orders),
        "shadow_quote_probe_market_tickers": shadow_quote_probe_market_tickers[:20],
        "exclude_market_tickers_requested": exclude_market_tickers_requested[:50],
        "exclude_market_tickers_requested_count": len(exclude_market_tickers_requested),
        "exclude_market_tickers_applied": exclude_market_tickers_applied[:50],
        "exclude_market_tickers_applied_count": len(exclude_market_tickers_applied),
        "exclude_market_side_targets_requested": exclude_market_side_targets_requested[:50],
        "exclude_market_side_targets_requested_count": len(exclude_market_side_targets_requested),
        "exclude_market_side_targets_applied": exclude_market_side_targets_applied[:50],
        "exclude_market_side_targets_applied_count": len(exclude_market_side_targets_applied),
        "excluded_intents_by_market_ticker_count": int(excluded_intents_by_market_ticker_count),
        "excluded_intents_by_market_side_count": int(excluded_intents_by_market_side_count),
        "excluded_intents_by_market_target_count": int(excluded_intents_by_market_target_count),
        "adaptive_policy_profile_present": adaptive_policy_profile_summary["adaptive_policy_profile_present"],
        "adaptive_policy_profile_valid": adaptive_policy_profile_summary["adaptive_policy_profile_valid"],
        "adaptive_policy_profile_applied": adaptive_policy_profile_summary["adaptive_policy_profile_applied"],
        "adaptive_policy_profile_requested_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_requested_overrides"
        ],
        "adaptive_policy_profile_effective_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_effective_overrides"
        ],
        "adaptive_policy_profile_clamped_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_clamped_overrides"
        ],
        "adaptive_policy_profile_ignored_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_ignored_overrides"
        ],
        "constraint_csv": str(constraint_path),
        "specs_csv": str(specs_path) if specs_path is not None else "",
        "weather_pattern_hardening_enabled": bool(weather_pattern_hardening_enabled),
        "weather_pattern_profile_loaded": bool(weather_pattern_profile_meta.get("loaded")),
        "weather_pattern_profile_status": _normalize_text(weather_pattern_profile_meta.get("status")),
        "weather_pattern_profile_error": _normalize_text(weather_pattern_profile_meta.get("error")),
        "weather_pattern_profile_json_used": _normalize_text(
            weather_pattern_profile_meta.get("weather_pattern_profile_json_used")
        ),
        "weather_pattern_profile_source_origin": _normalize_text(
            weather_pattern_profile_meta.get("source_origin")
        ),
        "weather_pattern_profile_source_age_hours": _parse_float(
            weather_pattern_profile_meta.get("source_age_hours")
        ),
        "weather_pattern_profile_bucket_profile_count": int(
            weather_pattern_profile_meta.get("bucket_profile_count") or 0
        ),
        "weather_pattern_profile_applied": bool(
            weather_pattern_profile_meta.get("loaded")
            and (
                weather_pattern_probability_raise_count > 0
                or weather_pattern_expected_edge_raise_count > 0
                or weather_pattern_blocked_count > 0
                or weather_pattern_negative_regime_suppression_blocked_count > 0
            )
        ),
        "weather_pattern_probability_raise_count": int(weather_pattern_probability_raise_count),
        "weather_pattern_expected_edge_raise_count": int(weather_pattern_expected_edge_raise_count),
        "weather_pattern_hard_block_count": int(weather_pattern_blocked_count),
        "weather_pattern_negative_regime_suppression_enabled": bool(
            weather_pattern_negative_regime_suppression_state.get("enabled")
        ),
        "weather_pattern_negative_regime_suppression_active": bool(
            weather_pattern_negative_regime_suppression_state.get("active")
        ),
        "weather_pattern_negative_regime_suppression_status": _normalize_text(
            weather_pattern_negative_regime_suppression_state.get("status")
        ),
        "weather_pattern_negative_regime_suppression_min_bucket_samples": int(
            _parse_float(weather_pattern_negative_regime_suppression_state.get("min_bucket_samples"))
            or weather_pattern_negative_regime_suppression_min_bucket_samples
        ),
        "weather_pattern_negative_regime_suppression_expectancy_threshold": (
            _parse_float(weather_pattern_negative_regime_suppression_state.get("negative_expectancy_threshold"))
            if isinstance(weather_pattern_negative_regime_suppression_state, dict)
            else weather_pattern_negative_regime_suppression_expectancy_threshold
        ),
        "weather_pattern_negative_regime_suppression_top_n": int(
            _parse_float(weather_pattern_negative_regime_suppression_state.get("top_n"))
            or weather_pattern_negative_regime_suppression_top_n
        ),
        "weather_pattern_negative_regime_suppression_candidate_source": _normalize_text(
            weather_pattern_negative_regime_suppression_state.get("candidate_source")
        ),
        "weather_pattern_negative_regime_suppression_candidate_count": int(
            _parse_float(weather_pattern_negative_regime_suppression_state.get("candidate_count")) or 0
        ),
        "weather_pattern_negative_regime_suppression_blocked_count": int(
            weather_pattern_negative_regime_suppression_blocked_count
        ),
        "weather_pattern_negative_regime_suppression_candidates_top": (
            list(weather_pattern_negative_regime_suppression_state.get("suppression_candidates"))[:20]
            if isinstance(weather_pattern_negative_regime_suppression_state.get("suppression_candidates"), list)
            else []
        ),
        "weather_pattern_risk_off_enabled": bool(weather_pattern_risk_off_state.get("enabled")),
        "weather_pattern_risk_off_active": bool(weather_pattern_risk_off_state.get("active")),
        "weather_pattern_risk_off_status": _normalize_text(weather_pattern_risk_off_state.get("status")),
        "weather_pattern_risk_off_recommendation_active": bool(
            weather_pattern_risk_off_state.get("recommendation_active")
        ),
        "weather_pattern_risk_off_metrics_triggered": bool(
            weather_pattern_risk_off_state.get("metrics_triggered")
        ),
        "weather_pattern_risk_off_concentration_threshold": (
            _parse_float(weather_pattern_risk_off_state.get("concentration_threshold"))
            if isinstance(weather_pattern_risk_off_state, dict)
            else weather_pattern_risk_off_concentration_threshold
        ),
        "weather_pattern_risk_off_min_attempts": int(
            _parse_float(weather_pattern_risk_off_state.get("min_attempts"))
            or weather_pattern_risk_off_min_attempts
        ),
        "weather_pattern_risk_off_stale_metar_share_threshold": (
            _parse_float(weather_pattern_risk_off_state.get("stale_metar_share_threshold"))
            if isinstance(weather_pattern_risk_off_state, dict)
            else weather_pattern_risk_off_stale_metar_share_threshold
        ),
        "weather_pattern_risk_off_observed_concentration": _parse_float(
            weather_pattern_risk_off_state.get("observed_concentration")
        ),
        "weather_pattern_risk_off_observed_attempts": int(
            _parse_float(weather_pattern_risk_off_state.get("observed_attempts")) or 0
        ),
        "weather_pattern_risk_off_observed_stale_metar_share": _parse_float(
            weather_pattern_risk_off_state.get("observed_stale_metar_share")
        ),
        "weather_pattern_risk_off_metrics_source": _normalize_text(
            weather_pattern_risk_off_state.get("metrics_source")
        ),
        "weather_pattern_risk_off_negative_signal_dimensions": int(
            _parse_float(weather_pattern_risk_off_state.get("negative_signal_dimensions")) or 0
        ),
        "weather_pattern_risk_off_effective_concentration_threshold": _parse_float(
            weather_pattern_risk_off_state.get("effective_concentration_threshold")
        ),
        "weather_pattern_risk_off_effective_min_attempts": int(
            _parse_float(weather_pattern_risk_off_state.get("effective_min_attempts")) or 0
        ),
        "weather_pattern_risk_off_effective_stale_metar_share_threshold": _parse_float(
            weather_pattern_risk_off_state.get("effective_stale_metar_share_threshold")
        ),
        "weather_pattern_risk_off_fallback_signal_confirmed": bool(
            weather_pattern_risk_off_state.get("fallback_signal_confirmed")
        ),
        "weather_pattern_risk_off_activation_reasons": (
            list(weather_pattern_risk_off_state.get("activation_reasons"))
            if isinstance(weather_pattern_risk_off_state.get("activation_reasons"), list)
            else []
        ),
        "weather_pattern_risk_off_blocked_count": int(weather_pattern_risk_off_blocked_count),
        "weather_pattern_matched_bucket_count": int(len(weather_pattern_matched_bucket_evidence)),
        "weather_pattern_hard_block_evidence_count": int(len(weather_pattern_hard_block_evidence)),
        "weather_pattern_matched_bucket_evidence_top": weather_pattern_matched_bucket_evidence[:20],
        "weather_pattern_hard_block_evidence_top": weather_pattern_hard_block_evidence[:20],
        "metar_summary_json": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json": _normalize_text(metar_context.get("state_path")),
        "metar_ingest_quality_score": metar_ingest_quality_context.get("quality_score"),
        "metar_ingest_quality_grade": _normalize_text(metar_ingest_quality_context.get("quality_grade")),
        "metar_ingest_quality_status": _normalize_text(metar_ingest_quality_context.get("quality_status")),
        "metar_ingest_quality_signal_count": int(
            _parse_float(metar_ingest_quality_context.get("quality_signal_count")) or 0
        ),
        "metar_ingest_quality_signals": (
            list(metar_ingest_quality_context.get("quality_signals"))
            if isinstance(metar_ingest_quality_context.get("quality_signals"), list)
            else []
        ),
        "metar_ingest_fresh_station_coverage_ratio": metar_ingest_quality_context.get(
            "fresh_station_coverage_ratio"
        ),
        "metar_ingest_usable_latest_station_count": metar_ingest_quality_context.get(
            "usable_latest_station_count"
        ),
        "metar_ingest_stale_or_future_row_ratio": metar_ingest_quality_context.get(
            "stale_or_future_row_ratio"
        ),
        "metar_ingest_parse_error_rate": metar_ingest_quality_context.get("parse_error_rate"),
        "metar_ingest_quality_gate_enabled": metar_ingest_gate_enabled,
        "metar_ingest_quality_gate_passed": metar_ingest_gate_passed,
        "metar_ingest_quality_gate_failure_reasons": metar_ingest_gate_failure_reasons,
        "metar_ingest_min_quality_score": metar_ingest_quality_gate_state.get("min_quality_score"),
        "metar_ingest_min_fresh_station_coverage_ratio": metar_ingest_quality_gate_state.get(
            "min_fresh_station_coverage_ratio"
        ),
        "metar_ingest_require_ready_status": bool(
            metar_ingest_quality_gate_state.get("require_ready_status")
        ),
        "metar_ingest_quality_blocked_count": int(metar_ingest_quality_blocked_count),
        "high_price_edge_guard_enabled": bool(gate.high_price_edge_guard_enabled),
        "high_price_edge_guard_min_entry_price_dollars": round(
            float(gate.high_price_edge_guard_min_entry_price_dollars),
            6,
        ),
        "high_price_edge_guard_min_expected_edge_net": round(
            float(gate.high_price_edge_guard_min_expected_edge_net),
            6,
        ),
        "high_price_edge_guard_min_edge_to_risk_ratio": round(
            float(gate.high_price_edge_guard_min_edge_to_risk_ratio),
            6,
        ),
        "high_price_edge_guard_blocked_count": int(high_price_edge_guard_blocked_count),
        "canary_expansion_ready": bool(canary_expansion_ready),
        "canary_expansion_reasons": canary_expansion_reasons,
        "ws_state_json": ws_path,
        "settlement_state_json": _normalize_text(settlement_state_meta.get("settlement_state_json_used")),
        "settlement_state_loaded": bool(settlement_state_meta.get("loaded")),
        "settlement_state_entries": int(settlement_state_meta.get("entry_count") or 0),
        "settlement_prefilter_blocked_count": int(len(settlement_prefilter_decisions)),
        "settlement_prefilter_blocked_by_reason": settlement_prefilter_reason_counts,
        "enforce_underlying_netting": bool(enforce_underlying_netting),
        "underlying_netting_book_db_path": _normalize_text(netting_snapshot.get("book_db_path")),
        "underlying_netting_loaded": bool(netting_snapshot.get("loaded")),
        "underlying_netting_error": _normalize_text(netting_snapshot.get("error")),
        "existing_underlying_slots_count": len(existing_underlying_slots) if isinstance(existing_underlying_slots, dict) else 0,
        "existing_underlying_slots": existing_underlying_slots if isinstance(existing_underlying_slots, dict) else {},
        "constraint_status_counts": constraint_status_counts_sorted,
        "actionable_constraint_rows": actionable_constraint_rows,
        "expanded_intent_status_counts": intent_status_counts_sorted,
        "expanded_actionable_intents": expanded_actionable_intents,
        "constraint_market_tickers_count": len(constraint_market_tickers),
        "active_settlement_station_count": len(active_settlement_stations),
        "ws_tickers_with_sequence_count": len(ws_tickers_with_sequence),
        "ws_constraint_ticker_overlap": ws_constraint_ticker_overlap,
        "metar_age_station_override_count": len(station_metar_age_overrides),
        "metar_age_adaptive_station_override_count": len(adaptive_station_overrides),
        "metar_age_effective_station_override_count": len(effective_station_metar_age_overrides),
        "alpha_consensus_json": _normalize_text(alpha_consensus_meta.get("alpha_consensus_json_used")),
        "alpha_consensus_loaded": bool(alpha_consensus_meta.get("loaded")),
        "alpha_consensus_error": _normalize_text(alpha_consensus_meta.get("error")),
        "alpha_consensus_candidate_count": int(alpha_consensus_meta.get("candidate_count") or 0),
        "alpha_consensus_usable_candidate_count": int(alpha_consensus_meta.get("usable_candidate_count") or 0),
        "alpha_consensus_top_market_side": _normalize_text(alpha_consensus_meta.get("top_market_side")),
        "speci_calibration_json": speci_calibration_file_used,
        "require_market_snapshot_seq_configured": bool(require_market_snapshot_seq),
        "require_market_snapshot_seq_applied": bool(effective_require_market_snapshot_seq),
        "finalization_snapshot_file": str(finalization_snapshot_path),
        "finalization_state_counts": finalization_snapshot.get("state_counts"),
        "finalization_blocked_underlyings": finalization_snapshot.get("blocked_underlyings"),
        "order_group_id": order_group_id,
        "intents_total": len(intents),
        "intents_approved": len(approved_intents),
        "intents_revalidated": len(revalidated_intents),
        "revalidation_invalidated": len(revalidation_invalidations),
        "revalidation_invalidations": revalidation_invalidations[:100],
        "revalidation_meta": revalidation_meta,
        "allocation_summary": allocation_summary,
        "intents_selected_for_plan": len(capped_intents),
        "replan_market_side_cooldown": replan_cooldown_meta,
        "replan_market_side_cooldown_minutes": round(float(max(0.0, float(replan_market_side_cooldown_minutes))), 6),
        "replan_market_side_repeat_window_minutes": round(
            float(max(0.0, float(replan_market_side_repeat_window_minutes))),
            6,
        ),
        "replan_market_side_max_plans_per_window": int(max(0, int(replan_market_side_max_plans_per_window))),
        "replan_market_side_min_observation_advance_minutes": round(
            float(max(0.0, float(replan_market_side_min_observation_advance_minutes))),
            6,
        ),
        "replan_market_side_min_orders_backstop": int(max(0, int(replan_market_side_min_orders_backstop))),
        "replan_market_side_cooldown_blocked_count": int(replan_cooldown_meta.get("blocked_count") or 0),
        "replan_market_side_repeat_cap_blocked_count": int(replan_cooldown_meta.get("repeat_cap_blocked_count") or 0),
        "replan_market_side_repeat_cap_override_count": int(
            replan_cooldown_meta.get("repeat_cap_override_count") or 0
        ),
        "replan_market_side_cooldown_override_count": int(replan_cooldown_meta.get("override_count") or 0),
        "replan_market_side_cooldown_backstop_released_count": int(
            replan_cooldown_meta.get("backstop_released_count") or 0
        ),
        "historical_selection_quality_enabled": bool(historical_selection_quality_enabled),
        "historical_selection_quality_status": _normalize_text(historical_selection_quality_profile.get("status")),
        "historical_selection_quality_source_file": _normalize_text(
            historical_selection_quality_profile.get("source_file")
        ),
        "historical_selection_quality_source_age_hours": _parse_float(
            historical_selection_quality_profile.get("source_age_hours")
        ),
        "historical_selection_quality_fallback_profile_applied": bool(
            historical_selection_quality_profile.get("fallback_profile_applied")
        ),
        "historical_selection_quality_fallback_profile_source_file": _normalize_text(
            historical_selection_quality_profile.get("fallback_profile_source_file")
        ),
        "historical_selection_quality_resolved_unique_market_sides": int(
            _parse_float(historical_selection_quality_profile.get("resolved_unique_market_sides")) or 0
        ),
        "historical_selection_quality_repeated_entry_multiplier": _parse_float(
            historical_selection_quality_profile.get("repeated_entry_multiplier")
        ),
        "historical_selection_quality_evidence_confidence": _parse_float(
            historical_selection_quality_profile.get("evidence_confidence")
        ),
        "historical_selection_quality_global_penalty_ratio": _parse_float(
            historical_selection_quality_profile.get("global_penalty_ratio")
        ),
        "historical_selection_quality_global_boost_ratio": _parse_float(
            historical_selection_quality_profile.get("global_boost_ratio")
        ),
        "historical_selection_quality_profitability_gap_enabled": bool(
            (
                historical_selection_quality_profile.get("profitability_calibration_gap")
                if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                else {}
            ).get("enabled")
        ),
        "historical_selection_quality_profitability_gap_status": _normalize_text(
            (
                historical_selection_quality_profile.get("profitability_calibration_gap")
                if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                else {}
            ).get("status")
        ),
        "historical_selection_quality_profitability_gap_source_file": _normalize_text(
            (
                historical_selection_quality_profile.get("profitability_calibration_gap")
                if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                else {}
            ).get("source_file")
        ),
        "historical_selection_quality_profitability_gap_source_age_hours": _parse_float(
            (
                historical_selection_quality_profile.get("profitability_calibration_gap")
                if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                else {}
            ).get("source_age_hours")
        ),
        "historical_selection_quality_profitability_gap_calibration_ratio": _parse_float(
            (
                historical_selection_quality_profile.get("profitability_calibration_gap")
                if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                else {}
            ).get("calibration_ratio")
        ),
        "historical_selection_quality_profitability_gap_station_bucket_count": int(
            _parse_float(
                (
                    (
                        historical_selection_quality_profile.get("profitability_calibration_gap")
                        if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                        else {}
                    ).get("applied_bucket_counts")
                    if isinstance(
                        (
                            historical_selection_quality_profile.get("profitability_calibration_gap")
                            if isinstance(
                                historical_selection_quality_profile.get("profitability_calibration_gap"), dict
                            )
                            else {}
                        ).get("applied_bucket_counts"),
                        dict,
                    )
                    else {}
                ).get("station")
            )
            or 0
        ),
        "historical_selection_quality_profitability_gap_local_hour_bucket_count": int(
            _parse_float(
                (
                    (
                        historical_selection_quality_profile.get("profitability_calibration_gap")
                        if isinstance(historical_selection_quality_profile.get("profitability_calibration_gap"), dict)
                        else {}
                    ).get("applied_bucket_counts")
                    if isinstance(
                        (
                            historical_selection_quality_profile.get("profitability_calibration_gap")
                            if isinstance(
                                historical_selection_quality_profile.get("profitability_calibration_gap"), dict
                            )
                            else {}
                        ).get("applied_bucket_counts"),
                        dict,
                    )
                    else {}
                ).get("local_hour")
            )
            or 0
        ),
        "historical_selection_quality_profitability_gap_fallback_applied": bool(
            historical_selection_quality_profile.get("profitability_calibration_gap_fallback_applied")
        ),
        "historical_selection_quality_profitability_gap_fallback_source_file": _normalize_text(
            historical_selection_quality_profile.get("profitability_calibration_gap_fallback_source_file")
        ),
        "historical_selection_quality_probability_penalty_max": round(
            max(0.0, float(historical_selection_quality_probability_penalty_max)),
            6,
        ),
        "historical_selection_quality_expected_edge_penalty_max": round(
            max(0.0, float(historical_selection_quality_expected_edge_penalty_max)),
            6,
        ),
        "historical_selection_quality_score_adjust_scale": round(
            max(0.0, float(historical_selection_quality_score_adjust_scale)),
            6,
        ),
        "min_probability_confidence": (
            effective_probability_threshold_values[0]
            if len(effective_probability_threshold_values) == 1
            else min_probability_confidence
        ),
        "min_expected_edge_net": (
            effective_expected_edge_threshold_values[0]
            if len(effective_expected_edge_threshold_values) == 1
            else min_expected_edge_net
        ),
        "min_edge_to_risk_ratio": (
            effective_edge_to_risk_threshold_values[0]
            if len(effective_edge_to_risk_threshold_values) == 1
            else min_edge_to_risk_ratio
        ),
        "min_base_edge_net": (
            effective_base_edge_threshold_values[0]
            if len(effective_base_edge_threshold_values) == 1
            else gate.min_base_edge_net
        ),
        "min_probability_breakeven_gap": (
            effective_probability_breakeven_gap_threshold_values[0]
            if len(effective_probability_breakeven_gap_threshold_values) == 1
            else gate.min_probability_breakeven_gap
        ),
        "enforce_probability_edge_thresholds": bool(enforce_probability_edge_thresholds),
        "enforce_entry_price_probability_floor": bool(enforce_entry_price_probability_floor),
        "fallback_min_probability_confidence": fallback_min_probability_confidence,
        "fallback_min_expected_edge_net": fallback_min_expected_edge_net,
        "fallback_min_edge_to_risk_ratio": fallback_min_edge_to_risk_ratio,
        "effective_min_probability_confidence_values": effective_probability_threshold_values,
        "effective_min_expected_edge_net_values": effective_expected_edge_threshold_values,
        "effective_min_alpha_strength_values": effective_alpha_strength_threshold_values,
        "effective_min_edge_to_risk_ratio_values": effective_edge_to_risk_threshold_values,
        "effective_min_base_edge_net_values": effective_base_edge_threshold_values,
        "effective_min_probability_breakeven_gap_values": effective_probability_breakeven_gap_threshold_values,
        "approved_probability_below_min_count": int(approved_probability_below_min_count),
        "approved_expected_edge_below_min_count": int(approved_expected_edge_below_min_count),
        "approved_edge_to_risk_below_min_count": int(approved_edge_to_risk_below_min_count),
        "approved_base_edge_below_min_count": int(approved_base_edge_below_min_count),
        "approved_probability_breakeven_gap_below_min_count": int(
            approved_probability_breakeven_gap_below_min_count
        ),
        "sparse_evidence_hardening_applied_count": int(sparse_hardening_applied_count),
        "sparse_evidence_hardening_applied_rate": (
            round(float(sparse_hardening_applied_count) / float(len(decisions)), 6)
            if decisions
            else None
        ),
        "sparse_evidence_hardening_approved_count": int(sparse_hardening_approved_count),
        "sparse_evidence_hardening_blocked_count": int(sparse_hardening_blocked_count),
        "sparse_evidence_hardening_blocked_probability_below_min_count": int(
            sparse_hardening_blocked_probability_below_min_count
        ),
        "sparse_evidence_hardening_blocked_expected_edge_below_min_count": int(
            sparse_hardening_blocked_expected_edge_below_min_count
        ),
        "sparse_evidence_hardening_blocked_edge_to_risk_below_min_count": int(
            sparse_hardening_blocked_edge_to_risk_below_min_count
        ),
        "sparse_evidence_probability_raise_avg": (
            round(_average(sparse_hardening_probability_raise_values), 6)
            if _average(sparse_hardening_probability_raise_values) is not None
            else None
        ),
        "sparse_evidence_probability_raise_max": (
            round(max(sparse_hardening_probability_raise_values), 6)
            if sparse_hardening_probability_raise_values
            else None
        ),
        "sparse_evidence_expected_edge_raise_avg": (
            round(_average(sparse_hardening_expected_edge_raise_values), 6)
            if _average(sparse_hardening_expected_edge_raise_values) is not None
            else None
        ),
        "sparse_evidence_expected_edge_raise_max": (
            round(max(sparse_hardening_expected_edge_raise_values), 6)
            if sparse_hardening_expected_edge_raise_values
            else None
        ),
        "historical_selection_quality_adjusted_count": int(historical_quality_adjusted_count),
        "historical_selection_quality_adjusted_rate": (
            round(float(historical_quality_adjusted_count) / float(len(decisions)), 6)
            if decisions
            else None
        ),
        "historical_selection_quality_probability_raise_avg": (
            round(_average(historical_quality_probability_raise_values), 6)
            if _average(historical_quality_probability_raise_values) is not None
            else None
        ),
        "historical_selection_quality_probability_raise_max": (
            round(max(historical_quality_probability_raise_values), 6)
            if historical_quality_probability_raise_values
            else None
        ),
        "historical_selection_quality_expected_edge_raise_avg": (
            round(_average(historical_quality_expected_edge_raise_values), 6)
            if _average(historical_quality_expected_edge_raise_values) is not None
            else None
        ),
        "historical_selection_quality_expected_edge_raise_max": (
            round(max(historical_quality_expected_edge_raise_values), 6)
            if historical_quality_expected_edge_raise_values
            else None
        ),
        "historical_selection_quality_penalty_ratio_avg": (
            round(_average(historical_quality_penalty_ratio_values), 6)
            if _average(historical_quality_penalty_ratio_values) is not None
            else None
        ),
        "historical_selection_quality_penalty_ratio_max": (
            round(max(historical_quality_penalty_ratio_values), 6)
            if historical_quality_penalty_ratio_values
            else None
        ),
        "historical_selection_quality_boost_ratio_avg": (
            round(_average(historical_quality_boost_ratio_values), 6)
            if _average(historical_quality_boost_ratio_values) is not None
            else None
        ),
        "historical_selection_quality_boost_ratio_max": (
            round(max(historical_quality_boost_ratio_values), 6)
            if historical_quality_boost_ratio_values
            else None
        ),
        "historical_selection_quality_score_adjustment_avg": (
            round(_average(historical_quality_score_adjustment_values), 6)
            if _average(historical_quality_score_adjustment_values) is not None
            else None
        ),
        "historical_selection_quality_score_adjustment_min": (
            round(min(historical_quality_score_adjustment_values), 6)
            if historical_quality_score_adjustment_values
            else None
        ),
        "historical_selection_quality_score_adjustment_max": (
            round(max(historical_quality_score_adjustment_values), 6)
            if historical_quality_score_adjustment_values
            else None
        ),
        "historical_selection_quality_global_only_pressure_active_count": int(
            historical_quality_global_only_pressure_active_count
        ),
        "historical_selection_quality_global_only_pressure_active_rate": (
            round(float(historical_quality_global_only_pressure_active_count) / float(len(decisions)), 6)
            if decisions
            else None
        ),
        "historical_selection_quality_global_only_adjusted_share_avg": (
            round(_average(historical_quality_global_only_adjusted_share_values), 6)
            if _average(historical_quality_global_only_adjusted_share_values) is not None
            else None
        ),
        "historical_selection_quality_global_only_adjusted_share_max": (
            round(max(historical_quality_global_only_adjusted_share_values), 6)
            if historical_quality_global_only_adjusted_share_values
            else None
        ),
        "historical_selection_quality_global_only_excess_ratio_avg": (
            round(_average(historical_quality_global_only_excess_ratio_values), 6)
            if _average(historical_quality_global_only_excess_ratio_values) is not None
            else None
        ),
        "historical_selection_quality_global_only_excess_ratio_max": (
            round(max(historical_quality_global_only_excess_ratio_values), 6)
            if historical_quality_global_only_excess_ratio_values
            else None
        ),
        "historical_selection_quality_global_only_pressure_blocked_count": int(
            historical_quality_global_only_pressure_blocked_count
        ),
        "historical_profitability_guardrail_applied_count": int(
            historical_profitability_guardrail_applied_count
        ),
        "historical_profitability_guardrail_applied_rate": (
            round(float(historical_profitability_guardrail_applied_count) / float(len(decisions)), 6)
            if decisions
            else None
        ),
        "historical_profitability_guardrail_penalty_ratio_avg": (
            round(_average(historical_profitability_guardrail_penalty_values), 6)
            if _average(historical_profitability_guardrail_penalty_values) is not None
            else None
        ),
        "historical_profitability_guardrail_penalty_ratio_max": (
            round(max(historical_profitability_guardrail_penalty_values), 6)
            if historical_profitability_guardrail_penalty_values
            else None
        ),
        "historical_profitability_guardrail_probability_raise_avg": (
            round(_average(historical_profitability_guardrail_probability_raise_values), 6)
            if _average(historical_profitability_guardrail_probability_raise_values) is not None
            else None
        ),
        "historical_profitability_guardrail_probability_raise_max": (
            round(max(historical_profitability_guardrail_probability_raise_values), 6)
            if historical_profitability_guardrail_probability_raise_values
            else None
        ),
        "historical_profitability_guardrail_expected_edge_raise_avg": (
            round(_average(historical_profitability_guardrail_expected_edge_raise_values), 6)
            if _average(historical_profitability_guardrail_expected_edge_raise_values) is not None
            else None
        ),
        "historical_profitability_guardrail_expected_edge_raise_max": (
            round(max(historical_profitability_guardrail_expected_edge_raise_values), 6)
            if historical_profitability_guardrail_expected_edge_raise_values
            else None
        ),
        "historical_profitability_guardrail_blocked_probability_below_min_count": int(
            historical_profitability_guardrail_blocked_probability_count
        ),
        "historical_profitability_guardrail_blocked_expected_edge_below_min_count": int(
            historical_profitability_guardrail_blocked_expected_edge_count
        ),
        "historical_profitability_bucket_guardrail_applied_count": int(
            historical_profitability_bucket_guardrail_applied_count
        ),
        "historical_profitability_bucket_guardrail_applied_rate": (
            round(float(historical_profitability_bucket_guardrail_applied_count) / float(len(decisions)), 6)
            if decisions
            else None
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_avg": (
            round(_average(historical_profitability_bucket_guardrail_penalty_values), 6)
            if _average(historical_profitability_bucket_guardrail_penalty_values) is not None
            else None
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_max": (
            round(max(historical_profitability_bucket_guardrail_penalty_values), 6)
            if historical_profitability_bucket_guardrail_penalty_values
            else None
        ),
        "historical_profitability_bucket_guardrail_probability_raise_avg": (
            round(_average(historical_profitability_bucket_guardrail_probability_raise_values), 6)
            if _average(historical_profitability_bucket_guardrail_probability_raise_values) is not None
            else None
        ),
        "historical_profitability_bucket_guardrail_probability_raise_max": (
            round(max(historical_profitability_bucket_guardrail_probability_raise_values), 6)
            if historical_profitability_bucket_guardrail_probability_raise_values
            else None
        ),
        "historical_profitability_bucket_guardrail_expected_edge_raise_avg": (
            round(_average(historical_profitability_bucket_guardrail_expected_edge_raise_values), 6)
            if _average(historical_profitability_bucket_guardrail_expected_edge_raise_values) is not None
            else None
        ),
        "historical_profitability_bucket_guardrail_expected_edge_raise_max": (
            round(max(historical_profitability_bucket_guardrail_expected_edge_raise_values), 6)
            if historical_profitability_bucket_guardrail_expected_edge_raise_values
            else None
        ),
        "historical_profitability_bucket_guardrail_blocked_probability_below_min_count": int(
            historical_profitability_bucket_guardrail_blocked_probability_count
        ),
        "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": int(
            historical_profitability_bucket_guardrail_blocked_expected_edge_count
        ),
        "sparse_evidence_support_score_avg": (
            round(_average(sparse_hardening_support_score_values), 6)
            if _average(sparse_hardening_support_score_values) is not None
            else None
        ),
        "sparse_evidence_volatility_penalty_avg": (
            round(_average(sparse_hardening_volatility_penalty_values), 6)
            if _average(sparse_hardening_volatility_penalty_values) is not None
            else None
        ),
        "policy_reason_counts": dict(sorted(policy_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "alpha_feature_summary": {
            "intents_alpha_strength_avg": (
                round(intents_alpha_strength_avg, 6)
                if intents_alpha_strength_avg is not None
                else None
            ),
            "approved_alpha_strength_avg": (
                round(approved_alpha_strength_avg, 6)
                if approved_alpha_strength_avg is not None
                else None
            ),
            "intents_yes_possible_gap_avg": (
                round(intents_yes_gap_avg, 6)
                if intents_yes_gap_avg is not None
                else None
            ),
            "approved_yes_possible_gap_avg": (
                round(approved_yes_gap_avg, 6)
                if approved_yes_gap_avg is not None
                else None
            ),
            "intents_yes_possible_overlap_rate": (
                round(
                    sum(1 for intent in intents_with_overlap if intent.yes_possible_overlap) / len(intents_with_overlap),
                    6,
                )
                if intents_with_overlap
                else None
            ),
            "approved_yes_possible_overlap_rate": (
                round(
                    sum(1 for intent in approved_with_overlap if intent.yes_possible_overlap)
                    / len(approved_with_overlap),
                    6,
                )
                if approved_with_overlap
                else None
            ),
            "planned_alpha_strength_avg": (
                round(planned_alpha_strength_avg, 6)
                if planned_alpha_strength_avg is not None
                else None
            ),
            "intents_speci_shock_confidence_avg": (
                round(intents_speci_shock_confidence_avg, 6)
                if intents_speci_shock_confidence_avg is not None
                else None
            ),
            "approved_speci_shock_confidence_avg": (
                round(approved_speci_shock_confidence_avg, 6)
                if approved_speci_shock_confidence_avg is not None
                else None
            ),
            "planned_speci_shock_confidence_avg": (
                round(planned_speci_shock_confidence_avg, 6)
                if planned_speci_shock_confidence_avg is not None
                else None
            ),
            "intents_speci_shock_weight_avg": (
                round(intents_speci_shock_weight_avg, 6)
                if intents_speci_shock_weight_avg is not None
                else None
            ),
            "approved_speci_shock_weight_avg": (
                round(approved_speci_shock_weight_avg, 6)
                if approved_speci_shock_weight_avg is not None
                else None
            ),
            "planned_speci_shock_weight_avg": (
                round(planned_speci_shock_weight_avg, 6)
                if planned_speci_shock_weight_avg is not None
                else None
            ),
            "intents_speci_shock_active_rate": (
                round(sum(1 for intent in intents if intent.speci_shock_active) / len(intents), 6)
                if intents
                else None
            ),
            "approved_speci_shock_active_rate": (
                round(
                    sum(1 for intent in revalidated_intents if intent.speci_shock_active) / len(revalidated_intents),
                    6,
                )
                if revalidated_intents
                else None
            ),
            "planned_speci_shock_active_rate": (
                round(
                    sum(1 for plan in plans if _normalize_text(plan.get("temperature_speci_shock_active")).lower() in {"1", "true", "yes"}) / len(plans),
                    6,
                )
                if plans
                else None
            ),
            "intents_consensus_alpha_score_avg": (
                round(intents_consensus_alpha_avg, 6)
                if intents_consensus_alpha_avg is not None
                else None
            ),
            "approved_consensus_alpha_score_avg": (
                round(approved_consensus_alpha_avg, 6)
                if approved_consensus_alpha_avg is not None
                else None
            ),
            "intents_consensus_support_ratio_avg": (
                round(intents_consensus_support_ratio_avg, 6)
                if intents_consensus_support_ratio_avg is not None
                else None
            ),
            "approved_consensus_support_ratio_avg": (
                round(approved_consensus_support_ratio_avg, 6)
                if approved_consensus_support_ratio_avg is not None
                else None
            ),
            "intents_probability_confidence_avg": (
                round(intents_probability_confidence_avg, 6)
                if intents_probability_confidence_avg is not None
                else None
            ),
            "approved_probability_confidence_avg": (
                round(approved_probability_confidence_avg, 6)
                if approved_probability_confidence_avg is not None
                else None
            ),
            "intents_expected_edge_net_avg": (
                round(intents_expected_edge_net_avg, 6)
                if intents_expected_edge_net_avg is not None
                else None
            ),
            "approved_expected_edge_net_avg": (
                round(approved_expected_edge_net_avg, 6)
                if approved_expected_edge_net_avg is not None
                else None
            ),
            "intents_base_edge_net_avg": (
                round(intents_base_edge_net_avg, 6)
                if intents_base_edge_net_avg is not None
                else None
            ),
            "approved_base_edge_net_avg": (
                round(approved_base_edge_net_avg, 6)
                if approved_base_edge_net_avg is not None
                else None
            ),
            "intents_edge_to_risk_ratio_avg": (
                round(intents_edge_to_risk_ratio_avg, 6)
                if intents_edge_to_risk_ratio_avg is not None
                else None
            ),
            "approved_edge_to_risk_ratio_avg": (
                round(approved_edge_to_risk_ratio_avg, 6)
                if approved_edge_to_risk_ratio_avg is not None
                else None
            ),
            "sparse_evidence_hardening_applied_rate": (
                round(float(sparse_hardening_applied_count) / float(len(decisions)), 6)
                if decisions
                else None
            ),
            "sparse_evidence_probability_raise_avg": (
                round(_average(sparse_hardening_probability_raise_values), 6)
                if _average(sparse_hardening_probability_raise_values) is not None
                else None
            ),
            "sparse_evidence_expected_edge_raise_avg": (
                round(_average(sparse_hardening_expected_edge_raise_values), 6)
                if _average(sparse_hardening_expected_edge_raise_values) is not None
                else None
            ),
            "sparse_evidence_support_score_avg": (
                round(_average(sparse_hardening_support_score_values), 6)
                if _average(sparse_hardening_support_score_values) is not None
                else None
            ),
            "sparse_evidence_volatility_penalty_avg": (
                round(_average(sparse_hardening_volatility_penalty_values), 6)
                if _average(sparse_hardening_volatility_penalty_values) is not None
                else None
            ),
            "historical_selection_quality_adjusted_rate": (
                round(float(historical_quality_adjusted_count) / float(len(decisions)), 6)
                if decisions
                else None
            ),
            "historical_selection_quality_penalty_ratio_avg": (
                round(_average(historical_quality_penalty_ratio_values), 6)
                if _average(historical_quality_penalty_ratio_values) is not None
                else None
            ),
            "historical_selection_quality_boost_ratio_avg": (
                round(_average(historical_quality_boost_ratio_values), 6)
                if _average(historical_quality_boost_ratio_values) is not None
                else None
            ),
            "historical_selection_quality_probability_raise_avg": (
                round(_average(historical_quality_probability_raise_values), 6)
                if _average(historical_quality_probability_raise_values) is not None
                else None
            ),
            "historical_selection_quality_expected_edge_raise_avg": (
                round(_average(historical_quality_expected_edge_raise_values), 6)
                if _average(historical_quality_expected_edge_raise_values) is not None
                else None
            ),
            "historical_profitability_guardrail_applied_rate": (
                round(float(historical_profitability_guardrail_applied_count) / float(len(decisions)), 6)
                if decisions
                else None
            ),
            "historical_profitability_guardrail_penalty_ratio_avg": (
                round(_average(historical_profitability_guardrail_penalty_values), 6)
                if _average(historical_profitability_guardrail_penalty_values) is not None
                else None
            ),
            "historical_profitability_guardrail_probability_raise_avg": (
                round(_average(historical_profitability_guardrail_probability_raise_values), 6)
                if _average(historical_profitability_guardrail_probability_raise_values) is not None
                else None
            ),
            "historical_profitability_guardrail_expected_edge_raise_avg": (
                round(_average(historical_profitability_guardrail_expected_edge_raise_values), 6)
                if _average(historical_profitability_guardrail_expected_edge_raise_values) is not None
                else None
            ),
            "historical_profitability_bucket_guardrail_applied_rate": (
                round(float(historical_profitability_bucket_guardrail_applied_count) / float(len(decisions)), 6)
                if decisions
                else None
            ),
            "historical_profitability_bucket_guardrail_penalty_ratio_avg": (
                round(_average(historical_profitability_bucket_guardrail_penalty_values), 6)
                if _average(historical_profitability_bucket_guardrail_penalty_values) is not None
                else None
            ),
            "historical_profitability_bucket_guardrail_probability_raise_avg": (
                round(_average(historical_profitability_bucket_guardrail_probability_raise_values), 6)
                if _average(historical_profitability_bucket_guardrail_probability_raise_values) is not None
                else None
            ),
            "historical_profitability_bucket_guardrail_expected_edge_raise_avg": (
                round(_average(historical_profitability_bucket_guardrail_expected_edge_raise_values), 6)
                if _average(historical_profitability_bucket_guardrail_expected_edge_raise_values) is not None
                else None
            ),
        },
        "planned_orders": len(plans),
        "total_planned_cost_dollars": round(
            sum(_parse_float(plan.get("estimated_entry_cost_dollars")) or 0.0 for plan in plans),
            4,
        ),
        "orders": plans,
        "output_csv": str(plan_csv_path),
    }
    bridge_plan_summary_path = out_dir / f"kalshi_temperature_trade_plan_summary_{stamp}.json"
    bridge_plan_summary_path.write_text(json.dumps(bridge_plan_summary, indent=2), encoding="utf-8")
    bridge_plan_summary["output_file"] = str(bridge_plan_summary_path)

    intents_summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready",
        "adaptive_policy_profile_present": adaptive_policy_profile_summary["adaptive_policy_profile_present"],
        "adaptive_policy_profile_valid": adaptive_policy_profile_summary["adaptive_policy_profile_valid"],
        "adaptive_policy_profile_applied": adaptive_policy_profile_summary["adaptive_policy_profile_applied"],
        "adaptive_policy_profile_requested_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_requested_overrides"
        ],
        "adaptive_policy_profile_effective_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_effective_overrides"
        ],
        "adaptive_policy_profile_clamped_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_clamped_overrides"
        ],
        "adaptive_policy_profile_ignored_overrides": adaptive_policy_profile_summary[
            "adaptive_policy_profile_ignored_overrides"
        ],
        "weather_pattern_hardening_enabled": bridge_plan_summary.get("weather_pattern_hardening_enabled"),
        "weather_pattern_profile_loaded": bridge_plan_summary.get("weather_pattern_profile_loaded"),
        "weather_pattern_profile_status": bridge_plan_summary.get("weather_pattern_profile_status"),
        "weather_pattern_profile_error": bridge_plan_summary.get("weather_pattern_profile_error"),
        "weather_pattern_profile_json_used": bridge_plan_summary.get("weather_pattern_profile_json_used"),
        "weather_pattern_profile_source_origin": bridge_plan_summary.get("weather_pattern_profile_source_origin"),
        "weather_pattern_profile_source_age_hours": bridge_plan_summary.get(
            "weather_pattern_profile_source_age_hours"
        ),
        "weather_pattern_profile_bucket_profile_count": bridge_plan_summary.get(
            "weather_pattern_profile_bucket_profile_count"
        ),
        "weather_pattern_profile_applied": bridge_plan_summary.get("weather_pattern_profile_applied"),
        "weather_pattern_probability_raise_count": bridge_plan_summary.get(
            "weather_pattern_probability_raise_count"
        ),
        "weather_pattern_expected_edge_raise_count": bridge_plan_summary.get(
            "weather_pattern_expected_edge_raise_count"
        ),
        "weather_pattern_hard_block_count": bridge_plan_summary.get("weather_pattern_hard_block_count"),
        "weather_pattern_negative_regime_suppression_enabled": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_enabled"
        ),
        "weather_pattern_negative_regime_suppression_active": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_active"
        ),
        "weather_pattern_negative_regime_suppression_status": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_status"
        ),
        "weather_pattern_negative_regime_suppression_min_bucket_samples": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_min_bucket_samples"
        ),
        "weather_pattern_negative_regime_suppression_expectancy_threshold": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_expectancy_threshold"
        ),
        "weather_pattern_negative_regime_suppression_top_n": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_top_n"
        ),
        "weather_pattern_negative_regime_suppression_candidate_source": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_candidate_source"
        ),
        "weather_pattern_negative_regime_suppression_candidate_count": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_candidate_count"
        ),
        "weather_pattern_negative_regime_suppression_blocked_count": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_blocked_count"
        ),
        "weather_pattern_negative_regime_suppression_candidates_top": bridge_plan_summary.get(
            "weather_pattern_negative_regime_suppression_candidates_top"
        ),
        "weather_pattern_risk_off_enabled": bridge_plan_summary.get("weather_pattern_risk_off_enabled"),
        "weather_pattern_risk_off_active": bridge_plan_summary.get("weather_pattern_risk_off_active"),
        "weather_pattern_risk_off_status": bridge_plan_summary.get("weather_pattern_risk_off_status"),
        "weather_pattern_risk_off_recommendation_active": bridge_plan_summary.get(
            "weather_pattern_risk_off_recommendation_active"
        ),
        "weather_pattern_risk_off_metrics_triggered": bridge_plan_summary.get(
            "weather_pattern_risk_off_metrics_triggered"
        ),
        "weather_pattern_risk_off_concentration_threshold": bridge_plan_summary.get(
            "weather_pattern_risk_off_concentration_threshold"
        ),
        "weather_pattern_risk_off_min_attempts": bridge_plan_summary.get("weather_pattern_risk_off_min_attempts"),
        "weather_pattern_risk_off_stale_metar_share_threshold": bridge_plan_summary.get(
            "weather_pattern_risk_off_stale_metar_share_threshold"
        ),
        "weather_pattern_risk_off_observed_concentration": bridge_plan_summary.get(
            "weather_pattern_risk_off_observed_concentration"
        ),
        "weather_pattern_risk_off_observed_attempts": bridge_plan_summary.get(
            "weather_pattern_risk_off_observed_attempts"
        ),
        "weather_pattern_risk_off_observed_stale_metar_share": bridge_plan_summary.get(
            "weather_pattern_risk_off_observed_stale_metar_share"
        ),
        "weather_pattern_risk_off_metrics_source": bridge_plan_summary.get(
            "weather_pattern_risk_off_metrics_source"
        ),
        "weather_pattern_risk_off_negative_signal_dimensions": bridge_plan_summary.get(
            "weather_pattern_risk_off_negative_signal_dimensions"
        ),
        "weather_pattern_risk_off_effective_concentration_threshold": bridge_plan_summary.get(
            "weather_pattern_risk_off_effective_concentration_threshold"
        ),
        "weather_pattern_risk_off_effective_min_attempts": bridge_plan_summary.get(
            "weather_pattern_risk_off_effective_min_attempts"
        ),
        "weather_pattern_risk_off_effective_stale_metar_share_threshold": bridge_plan_summary.get(
            "weather_pattern_risk_off_effective_stale_metar_share_threshold"
        ),
        "weather_pattern_risk_off_fallback_signal_confirmed": bridge_plan_summary.get(
            "weather_pattern_risk_off_fallback_signal_confirmed"
        ),
        "weather_pattern_risk_off_activation_reasons": bridge_plan_summary.get(
            "weather_pattern_risk_off_activation_reasons"
        ),
        "weather_pattern_risk_off_blocked_count": bridge_plan_summary.get("weather_pattern_risk_off_blocked_count"),
        "weather_pattern_matched_bucket_count": bridge_plan_summary.get("weather_pattern_matched_bucket_count"),
        "weather_pattern_hard_block_evidence_count": bridge_plan_summary.get(
            "weather_pattern_hard_block_evidence_count"
        ),
        "weather_pattern_matched_bucket_evidence_top": bridge_plan_summary.get(
            "weather_pattern_matched_bucket_evidence_top"
        ),
        "weather_pattern_hard_block_evidence_top": bridge_plan_summary.get(
            "weather_pattern_hard_block_evidence_top"
        ),
        "exclude_market_tickers_requested": bridge_plan_summary.get("exclude_market_tickers_requested"),
        "exclude_market_tickers_requested_count": bridge_plan_summary.get(
            "exclude_market_tickers_requested_count"
        ),
        "exclude_market_tickers_applied": bridge_plan_summary.get("exclude_market_tickers_applied"),
        "exclude_market_tickers_applied_count": bridge_plan_summary.get("exclude_market_tickers_applied_count"),
        "exclude_market_side_targets_requested": bridge_plan_summary.get("exclude_market_side_targets_requested"),
        "exclude_market_side_targets_requested_count": bridge_plan_summary.get(
            "exclude_market_side_targets_requested_count"
        ),
        "exclude_market_side_targets_applied": bridge_plan_summary.get("exclude_market_side_targets_applied"),
        "exclude_market_side_targets_applied_count": bridge_plan_summary.get(
            "exclude_market_side_targets_applied_count"
        ),
        "excluded_intents_by_market_ticker_count": bridge_plan_summary.get(
            "excluded_intents_by_market_ticker_count"
        ),
        "excluded_intents_by_market_side_count": bridge_plan_summary.get(
            "excluded_intents_by_market_side_count"
        ),
        "excluded_intents_by_market_target_count": bridge_plan_summary.get(
            "excluded_intents_by_market_target_count"
        ),
        "constraint_csv": str(constraint_path),
        "specs_csv": str(specs_path) if specs_path is not None else "",
        "metar_summary_json": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json": _normalize_text(metar_context.get("state_path")),
        "metar_ingest_quality_score": bridge_plan_summary.get("metar_ingest_quality_score"),
        "metar_ingest_quality_grade": bridge_plan_summary.get("metar_ingest_quality_grade"),
        "metar_ingest_quality_status": bridge_plan_summary.get("metar_ingest_quality_status"),
        "metar_ingest_quality_signal_count": bridge_plan_summary.get("metar_ingest_quality_signal_count"),
        "metar_ingest_quality_signals": bridge_plan_summary.get("metar_ingest_quality_signals"),
        "metar_ingest_fresh_station_coverage_ratio": bridge_plan_summary.get(
            "metar_ingest_fresh_station_coverage_ratio"
        ),
        "metar_ingest_usable_latest_station_count": bridge_plan_summary.get(
            "metar_ingest_usable_latest_station_count"
        ),
        "metar_ingest_stale_or_future_row_ratio": bridge_plan_summary.get(
            "metar_ingest_stale_or_future_row_ratio"
        ),
        "metar_ingest_parse_error_rate": bridge_plan_summary.get("metar_ingest_parse_error_rate"),
        "metar_ingest_quality_gate_enabled": bridge_plan_summary.get("metar_ingest_quality_gate_enabled"),
        "metar_ingest_quality_gate_passed": bridge_plan_summary.get("metar_ingest_quality_gate_passed"),
        "metar_ingest_quality_gate_failure_reasons": bridge_plan_summary.get(
            "metar_ingest_quality_gate_failure_reasons"
        ),
        "metar_ingest_min_quality_score": bridge_plan_summary.get("metar_ingest_min_quality_score"),
        "metar_ingest_min_fresh_station_coverage_ratio": bridge_plan_summary.get(
            "metar_ingest_min_fresh_station_coverage_ratio"
        ),
        "metar_ingest_require_ready_status": bridge_plan_summary.get("metar_ingest_require_ready_status"),
        "metar_ingest_quality_blocked_count": bridge_plan_summary.get("metar_ingest_quality_blocked_count"),
        "high_price_edge_guard_enabled": bridge_plan_summary.get("high_price_edge_guard_enabled"),
        "high_price_edge_guard_min_entry_price_dollars": bridge_plan_summary.get(
            "high_price_edge_guard_min_entry_price_dollars"
        ),
        "high_price_edge_guard_min_expected_edge_net": bridge_plan_summary.get(
            "high_price_edge_guard_min_expected_edge_net"
        ),
        "high_price_edge_guard_min_edge_to_risk_ratio": bridge_plan_summary.get(
            "high_price_edge_guard_min_edge_to_risk_ratio"
        ),
        "high_price_edge_guard_blocked_count": bridge_plan_summary.get("high_price_edge_guard_blocked_count"),
        "metar_snapshot_sha": _normalize_text(metar_context.get("raw_sha256")),
        "metar_age_policy_json": _normalize_text(metar_age_policy.get("path")),
        "metar_age_policy_loaded": bool(metar_age_policy.get("loaded")),
        "metar_age_policy_error": _normalize_text(metar_age_policy.get("error")),
        "alpha_consensus_json": _normalize_text(alpha_consensus_meta.get("alpha_consensus_json_used")),
        "alpha_consensus_loaded": bool(alpha_consensus_meta.get("loaded")),
        "alpha_consensus_error": _normalize_text(alpha_consensus_meta.get("error")),
        "alpha_consensus_candidate_count": int(alpha_consensus_meta.get("candidate_count") or 0),
        "alpha_consensus_usable_candidate_count": int(alpha_consensus_meta.get("usable_candidate_count") or 0),
        "alpha_consensus_top_market_side": _normalize_text(alpha_consensus_meta.get("top_market_side")),
        "speci_calibration_json": speci_calibration_file_used,
        "metar_age_station_override_count": len(station_metar_age_overrides),
        "metar_age_adaptive_station_override_count": len(adaptive_station_overrides),
        "metar_age_effective_station_override_count": len(effective_station_metar_age_overrides),
        "metar_age_station_hour_override_count": sum(
            len(value) for value in station_hour_metar_age_overrides.values() if isinstance(value, dict)
        ),
        "metar_age_policy_warnings": metar_age_policy.get("warnings") if isinstance(metar_age_policy.get("warnings"), list) else [],
        "metar_age_adaptive_overrides_top": sorted(
            (
                {
                    "station": station,
                    **details,
                }
                for station, details in adaptive_station_diagnostics.items()
            ),
            key=lambda row: (
                -float(_parse_float(row.get("adaptive_max_age_minutes")) or 0.0),
                _normalize_text(row.get("station")),
            ),
        )[:20],
        "min_alpha_strength": min_alpha_strength,
        "min_probability_confidence": min_probability_confidence,
        "min_expected_edge_net": min_expected_edge_net,
        "min_edge_to_risk_ratio": min_edge_to_risk_ratio,
        "enforce_probability_edge_thresholds": bool(enforce_probability_edge_thresholds),
        "enforce_entry_price_probability_floor": bool(enforce_entry_price_probability_floor),
        "fallback_min_probability_confidence": fallback_min_probability_confidence,
        "fallback_min_expected_edge_net": fallback_min_expected_edge_net,
        "fallback_min_edge_to_risk_ratio": fallback_min_edge_to_risk_ratio,
        "effective_min_probability_confidence_values": bridge_plan_summary.get(
            "effective_min_probability_confidence_values"
        ),
        "effective_min_expected_edge_net_values": bridge_plan_summary.get(
            "effective_min_expected_edge_net_values"
        ),
        "effective_min_alpha_strength_values": bridge_plan_summary.get(
            "effective_min_alpha_strength_values"
        ),
        "effective_min_edge_to_risk_ratio_values": bridge_plan_summary.get(
            "effective_min_edge_to_risk_ratio_values"
        ),
        "approved_probability_below_min_count": bridge_plan_summary.get("approved_probability_below_min_count"),
        "approved_expected_edge_below_min_count": bridge_plan_summary.get("approved_expected_edge_below_min_count"),
        "approved_edge_to_risk_below_min_count": bridge_plan_summary.get("approved_edge_to_risk_below_min_count"),
        "sparse_evidence_hardening_applied_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_applied_count"
        ),
        "sparse_evidence_hardening_applied_rate": bridge_plan_summary.get(
            "sparse_evidence_hardening_applied_rate"
        ),
        "sparse_evidence_hardening_approved_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_approved_count"
        ),
        "sparse_evidence_hardening_blocked_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_blocked_count"
        ),
        "sparse_evidence_hardening_blocked_probability_below_min_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_blocked_probability_below_min_count"
        ),
        "sparse_evidence_hardening_blocked_expected_edge_below_min_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_blocked_expected_edge_below_min_count"
        ),
        "sparse_evidence_hardening_blocked_edge_to_risk_below_min_count": bridge_plan_summary.get(
            "sparse_evidence_hardening_blocked_edge_to_risk_below_min_count"
        ),
        "sparse_evidence_probability_raise_avg": bridge_plan_summary.get("sparse_evidence_probability_raise_avg"),
        "sparse_evidence_probability_raise_max": bridge_plan_summary.get("sparse_evidence_probability_raise_max"),
        "sparse_evidence_expected_edge_raise_avg": bridge_plan_summary.get(
            "sparse_evidence_expected_edge_raise_avg"
        ),
        "sparse_evidence_expected_edge_raise_max": bridge_plan_summary.get(
            "sparse_evidence_expected_edge_raise_max"
        ),
        "sparse_evidence_support_score_avg": bridge_plan_summary.get("sparse_evidence_support_score_avg"),
        "sparse_evidence_volatility_penalty_avg": bridge_plan_summary.get(
            "sparse_evidence_volatility_penalty_avg"
        ),
        "historical_selection_quality_enabled": bridge_plan_summary.get(
            "historical_selection_quality_enabled"
        ),
        "historical_selection_quality_status": bridge_plan_summary.get(
            "historical_selection_quality_status"
        ),
        "historical_selection_quality_source_file": bridge_plan_summary.get(
            "historical_selection_quality_source_file"
        ),
        "historical_selection_quality_source_age_hours": bridge_plan_summary.get(
            "historical_selection_quality_source_age_hours"
        ),
        "historical_selection_quality_fallback_profile_applied": bridge_plan_summary.get(
            "historical_selection_quality_fallback_profile_applied"
        ),
        "historical_selection_quality_fallback_profile_source_file": bridge_plan_summary.get(
            "historical_selection_quality_fallback_profile_source_file"
        ),
        "historical_selection_quality_resolved_unique_market_sides": bridge_plan_summary.get(
            "historical_selection_quality_resolved_unique_market_sides"
        ),
        "historical_selection_quality_repeated_entry_multiplier": bridge_plan_summary.get(
            "historical_selection_quality_repeated_entry_multiplier"
        ),
        "historical_selection_quality_evidence_confidence": bridge_plan_summary.get(
            "historical_selection_quality_evidence_confidence"
        ),
        "historical_selection_quality_global_penalty_ratio": bridge_plan_summary.get(
            "historical_selection_quality_global_penalty_ratio"
        ),
        "historical_selection_quality_global_boost_ratio": bridge_plan_summary.get(
            "historical_selection_quality_global_boost_ratio"
        ),
        "historical_selection_quality_profitability_gap_enabled": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_enabled"
        ),
        "historical_selection_quality_profitability_gap_status": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_status"
        ),
        "historical_selection_quality_profitability_gap_source_file": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_source_file"
        ),
        "historical_selection_quality_profitability_gap_source_age_hours": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_source_age_hours"
        ),
        "historical_selection_quality_profitability_gap_calibration_ratio": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_calibration_ratio"
        ),
        "historical_selection_quality_profitability_gap_station_bucket_count": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_station_bucket_count"
        ),
        "historical_selection_quality_profitability_gap_local_hour_bucket_count": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_local_hour_bucket_count"
        ),
        "historical_selection_quality_profitability_gap_fallback_applied": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_fallback_applied"
        ),
        "historical_selection_quality_profitability_gap_fallback_source_file": bridge_plan_summary.get(
            "historical_selection_quality_profitability_gap_fallback_source_file"
        ),
        "historical_selection_quality_adjusted_count": bridge_plan_summary.get(
            "historical_selection_quality_adjusted_count"
        ),
        "historical_selection_quality_adjusted_rate": bridge_plan_summary.get(
            "historical_selection_quality_adjusted_rate"
        ),
        "historical_selection_quality_probability_raise_avg": bridge_plan_summary.get(
            "historical_selection_quality_probability_raise_avg"
        ),
        "historical_selection_quality_probability_raise_max": bridge_plan_summary.get(
            "historical_selection_quality_probability_raise_max"
        ),
        "historical_selection_quality_expected_edge_raise_avg": bridge_plan_summary.get(
            "historical_selection_quality_expected_edge_raise_avg"
        ),
        "historical_selection_quality_expected_edge_raise_max": bridge_plan_summary.get(
            "historical_selection_quality_expected_edge_raise_max"
        ),
        "historical_selection_quality_penalty_ratio_avg": bridge_plan_summary.get(
            "historical_selection_quality_penalty_ratio_avg"
        ),
        "historical_selection_quality_penalty_ratio_max": bridge_plan_summary.get(
            "historical_selection_quality_penalty_ratio_max"
        ),
        "historical_selection_quality_boost_ratio_avg": bridge_plan_summary.get(
            "historical_selection_quality_boost_ratio_avg"
        ),
        "historical_selection_quality_boost_ratio_max": bridge_plan_summary.get(
            "historical_selection_quality_boost_ratio_max"
        ),
        "historical_selection_quality_score_adjustment_avg": bridge_plan_summary.get(
            "historical_selection_quality_score_adjustment_avg"
        ),
        "historical_selection_quality_score_adjustment_min": bridge_plan_summary.get(
            "historical_selection_quality_score_adjustment_min"
        ),
        "historical_selection_quality_score_adjustment_max": bridge_plan_summary.get(
            "historical_selection_quality_score_adjustment_max"
        ),
        "historical_selection_quality_global_only_pressure_active_count": bridge_plan_summary.get(
            "historical_selection_quality_global_only_pressure_active_count"
        ),
        "historical_selection_quality_global_only_pressure_active_rate": bridge_plan_summary.get(
            "historical_selection_quality_global_only_pressure_active_rate"
        ),
        "historical_selection_quality_global_only_adjusted_share_avg": bridge_plan_summary.get(
            "historical_selection_quality_global_only_adjusted_share_avg"
        ),
        "historical_selection_quality_global_only_adjusted_share_max": bridge_plan_summary.get(
            "historical_selection_quality_global_only_adjusted_share_max"
        ),
        "historical_selection_quality_global_only_excess_ratio_avg": bridge_plan_summary.get(
            "historical_selection_quality_global_only_excess_ratio_avg"
        ),
        "historical_selection_quality_global_only_excess_ratio_max": bridge_plan_summary.get(
            "historical_selection_quality_global_only_excess_ratio_max"
        ),
        "historical_selection_quality_global_only_pressure_blocked_count": bridge_plan_summary.get(
            "historical_selection_quality_global_only_pressure_blocked_count"
        ),
        "historical_profitability_guardrail_applied_count": bridge_plan_summary.get(
            "historical_profitability_guardrail_applied_count"
        ),
        "historical_profitability_guardrail_applied_rate": bridge_plan_summary.get(
            "historical_profitability_guardrail_applied_rate"
        ),
        "historical_profitability_guardrail_penalty_ratio_avg": bridge_plan_summary.get(
            "historical_profitability_guardrail_penalty_ratio_avg"
        ),
        "historical_profitability_guardrail_penalty_ratio_max": bridge_plan_summary.get(
            "historical_profitability_guardrail_penalty_ratio_max"
        ),
        "historical_profitability_guardrail_probability_raise_avg": bridge_plan_summary.get(
            "historical_profitability_guardrail_probability_raise_avg"
        ),
        "historical_profitability_guardrail_probability_raise_max": bridge_plan_summary.get(
            "historical_profitability_guardrail_probability_raise_max"
        ),
        "historical_profitability_guardrail_expected_edge_raise_avg": bridge_plan_summary.get(
            "historical_profitability_guardrail_expected_edge_raise_avg"
        ),
        "historical_profitability_guardrail_expected_edge_raise_max": bridge_plan_summary.get(
            "historical_profitability_guardrail_expected_edge_raise_max"
        ),
        "historical_profitability_guardrail_blocked_probability_below_min_count": bridge_plan_summary.get(
            "historical_profitability_guardrail_blocked_probability_below_min_count"
        ),
        "historical_profitability_guardrail_blocked_expected_edge_below_min_count": bridge_plan_summary.get(
            "historical_profitability_guardrail_blocked_expected_edge_below_min_count"
        ),
        "historical_profitability_bucket_guardrail_applied_count": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_applied_count"
        ),
        "historical_profitability_bucket_guardrail_applied_rate": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_applied_rate"
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_avg": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_penalty_ratio_avg"
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_max": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_penalty_ratio_max"
        ),
        "historical_profitability_bucket_guardrail_probability_raise_avg": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_probability_raise_avg"
        ),
        "historical_profitability_bucket_guardrail_probability_raise_max": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_probability_raise_max"
        ),
        "historical_profitability_bucket_guardrail_expected_edge_raise_avg": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_expected_edge_raise_avg"
        ),
        "historical_profitability_bucket_guardrail_expected_edge_raise_max": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_expected_edge_raise_max"
        ),
        "historical_profitability_bucket_guardrail_blocked_probability_below_min_count": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_blocked_probability_below_min_count"
        ),
        "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": bridge_plan_summary.get(
            "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count"
        ),
        "enforce_interval_consistency": bool(enforce_interval_consistency),
        "max_yes_possible_gap_for_yes_side": float(max_yes_possible_gap_for_yes_side),
        "taf_stale_grace_minutes": round(float(max(0.0, float(taf_stale_grace_minutes))), 6),
        "taf_stale_grace_max_volatility_score": (
            round(float(taf_stale_grace_max_volatility_score), 6)
            if isinstance(taf_stale_grace_max_volatility_score, (int, float))
            else None
        ),
        "taf_stale_grace_max_range_width": (
            round(float(taf_stale_grace_max_range_width), 6)
            if isinstance(taf_stale_grace_max_range_width, (int, float))
            else None
        ),
        "metar_freshness_quality_boundary_ratio": (
            round(
                max(0.0, min(1.0, float(metar_freshness_quality_boundary_ratio))),
                6,
            )
            if isinstance(metar_freshness_quality_boundary_ratio, (int, float))
            else None
        ),
        "metar_freshness_quality_probability_margin": round(
            max(0.0, float(metar_freshness_quality_probability_margin)),
            6,
        ),
        "metar_freshness_quality_expected_edge_margin": round(
            max(0.0, float(metar_freshness_quality_expected_edge_margin)),
            6,
        ),
        "ws_state_json": ws_path,
        "settlement_state_json": _normalize_text(settlement_state_meta.get("settlement_state_json_used")),
        "settlement_state_loaded": bool(settlement_state_meta.get("loaded")),
        "settlement_state_entries": int(settlement_state_meta.get("entry_count") or 0),
        "settlement_prefilter_blocked_count": bridge_plan_summary.get("settlement_prefilter_blocked_count"),
        "settlement_prefilter_blocked_by_reason": bridge_plan_summary.get("settlement_prefilter_blocked_by_reason"),
        "enforce_underlying_netting": bool(enforce_underlying_netting),
        "underlying_netting_book_db_path": _normalize_text(netting_snapshot.get("book_db_path")),
        "underlying_netting_loaded": bool(netting_snapshot.get("loaded")),
        "underlying_netting_error": _normalize_text(netting_snapshot.get("error")),
        "existing_underlying_slots_count": len(existing_underlying_slots) if isinstance(existing_underlying_slots, dict) else 0,
        "existing_underlying_slots": existing_underlying_slots if isinstance(existing_underlying_slots, dict) else {},
        "ws_market_count": len(market_sequences),
        "constraint_status_counts": constraint_status_counts_sorted,
        "actionable_constraint_rows": actionable_constraint_rows,
        "expanded_intent_status_counts": intent_status_counts_sorted,
        "expanded_actionable_intents": expanded_actionable_intents,
        "constraint_market_tickers_count": len(constraint_market_tickers),
        "active_settlement_station_count": len(active_settlement_stations),
        "ws_tickers_with_sequence_count": len(ws_tickers_with_sequence),
        "ws_constraint_ticker_overlap": ws_constraint_ticker_overlap,
        "require_market_snapshot_seq_configured": bool(require_market_snapshot_seq),
        "require_market_snapshot_seq_applied": bool(effective_require_market_snapshot_seq),
        "intents_total": len(intents),
        "intents_approved": len(approved_intents),
        "intents_revalidated": len(revalidated_intents),
        "revalidation_invalidated": len(revalidation_invalidations),
        "intents_blocked": max(0, len(intents) - len(approved_intents)),
        "canary_expansion_ready": bridge_plan_summary.get("canary_expansion_ready"),
        "canary_expansion_reasons": bridge_plan_summary.get("canary_expansion_reasons"),
        "policy_reason_counts": bridge_plan_summary["policy_reason_counts"],
        "replan_market_side_cooldown": replan_cooldown_meta,
        "replan_market_side_cooldown_minutes": bridge_plan_summary.get("replan_market_side_cooldown_minutes"),
        "replan_market_side_repeat_window_minutes": bridge_plan_summary.get(
            "replan_market_side_repeat_window_minutes"
        ),
        "replan_market_side_max_plans_per_window": bridge_plan_summary.get(
            "replan_market_side_max_plans_per_window"
        ),
        "replan_market_side_min_observation_advance_minutes": bridge_plan_summary.get(
            "replan_market_side_min_observation_advance_minutes"
        ),
        "replan_market_side_min_orders_backstop": bridge_plan_summary.get("replan_market_side_min_orders_backstop"),
        "replan_market_side_cooldown_blocked_count": bridge_plan_summary.get(
            "replan_market_side_cooldown_blocked_count"
        ),
        "replan_market_side_repeat_cap_blocked_count": bridge_plan_summary.get(
            "replan_market_side_repeat_cap_blocked_count"
        ),
        "replan_market_side_repeat_cap_override_count": bridge_plan_summary.get(
            "replan_market_side_repeat_cap_override_count"
        ),
        "replan_market_side_cooldown_override_count": bridge_plan_summary.get(
            "replan_market_side_cooldown_override_count"
        ),
        "replan_market_side_cooldown_backstop_released_count": bridge_plan_summary.get(
            "replan_market_side_cooldown_backstop_released_count"
        ),
        "allocation_summary": bridge_plan_summary.get("allocation_summary"),
        "revalidation_meta": revalidation_meta,
        "revalidation_invalidations": revalidation_invalidations[:100],
        "alpha_feature_summary": bridge_plan_summary.get("alpha_feature_summary"),
        "output_csv": str(intents_csv_path),
        "finalization_snapshot_file": str(finalization_snapshot_path),
        "finalization_state_counts": finalization_snapshot.get("state_counts"),
        "finalization_blocked_underlyings": finalization_snapshot.get("blocked_underlyings"),
        "top_approved": [
            _intent_with_policy_row(
                intent=intent,
                decision=decisions_by_id.get(intent.intent_id),
                revalidation=(
                    revalidation_by_id.get(intent.intent_id)
                    if isinstance(revalidation_by_id, dict)
                    else None
                ),
            )
            for intent in revalidated_intents[:20]
        ],
    }
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{stamp}.json"
    intents_summary_path.write_text(json.dumps(intents_summary, indent=2), encoding="utf-8")
    intents_summary["output_file"] = str(intents_summary_path)

    if intents_only:
        return {
            "status": "intents_only",
            "captured_at": captured_at.isoformat(),
            "adaptive_policy_profile": adaptive_policy_profile_summary,
            "weather_pattern_profile": weather_pattern_profile_meta,
            "constraint_scan_summary": constraint_scan_summary,
            "intent_summary": intents_summary,
            "plan_summary": bridge_plan_summary,
            "ws_state_status": _normalize_text((ws_payload.get("summary") or {}).get("status")),
        }

    synthetic_plan_summary = {
        "status": "ready" if plans else "no_candidates",
        "planned_orders": len(plans),
        "total_planned_cost_dollars": bridge_plan_summary["total_planned_cost_dollars"],
        "shadow_quote_probe_requested": bridge_plan_summary.get("shadow_quote_probe_requested"),
        "shadow_quote_probe_applied": bridge_plan_summary.get("shadow_quote_probe_applied"),
        "shadow_quote_probe_reason": bridge_plan_summary.get("shadow_quote_probe_reason"),
        "shadow_quote_probe_source": bridge_plan_summary.get("shadow_quote_probe_source"),
        "shadow_quote_probe_targeted_requested": bridge_plan_summary.get("shadow_quote_probe_targeted_requested"),
        "shadow_quote_probe_targeted_applied": bridge_plan_summary.get("shadow_quote_probe_targeted_applied"),
        "shadow_quote_probe_targeted_keys": bridge_plan_summary.get("shadow_quote_probe_targeted_keys"),
        "shadow_quote_probe_targeted_match_count": bridge_plan_summary.get("shadow_quote_probe_targeted_match_count"),
        "shadow_quote_probe_candidate_intents": bridge_plan_summary.get("shadow_quote_probe_candidate_intents"),
        "shadow_quote_probe_planned_orders": bridge_plan_summary.get("shadow_quote_probe_planned_orders"),
        "shadow_quote_probe_market_tickers": bridge_plan_summary.get("shadow_quote_probe_market_tickers"),
        "exclude_market_tickers_requested": bridge_plan_summary.get("exclude_market_tickers_requested"),
        "exclude_market_tickers_requested_count": bridge_plan_summary.get(
            "exclude_market_tickers_requested_count"
        ),
        "exclude_market_tickers_applied": bridge_plan_summary.get("exclude_market_tickers_applied"),
        "exclude_market_tickers_applied_count": bridge_plan_summary.get("exclude_market_tickers_applied_count"),
        "exclude_market_side_targets_requested": bridge_plan_summary.get("exclude_market_side_targets_requested"),
        "exclude_market_side_targets_requested_count": bridge_plan_summary.get(
            "exclude_market_side_targets_requested_count"
        ),
        "exclude_market_side_targets_applied": bridge_plan_summary.get("exclude_market_side_targets_applied"),
        "exclude_market_side_targets_applied_count": bridge_plan_summary.get(
            "exclude_market_side_targets_applied_count"
        ),
        "excluded_intents_by_market_ticker_count": bridge_plan_summary.get(
            "excluded_intents_by_market_ticker_count"
        ),
        "excluded_intents_by_market_side_count": bridge_plan_summary.get(
            "excluded_intents_by_market_side_count"
        ),
        "excluded_intents_by_market_target_count": bridge_plan_summary.get(
            "excluded_intents_by_market_target_count"
        ),
        "actual_live_balance_dollars": None,
        "actual_live_balance_source": "unknown",
        "funding_gap_dollars": None,
        "board_warning": None,
        "output_file": str(bridge_plan_summary_path),
        "output_csv": str(plan_csv_path),
        "orders": plans,
    }

    def _synthetic_plan_runner(**_: Any) -> dict[str, Any]:
        return dict(synthetic_plan_summary)

    execute_summary = micro_execute_runner(
        env_file=env_file,
        output_dir=output_dir,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=max(1, int(contracts_per_order)),
        max_orders=max(1, int(max_orders)),
        timeout_seconds=timeout_seconds,
        allow_live_orders=allow_live_orders,
        cancel_resting_immediately=cancel_resting_immediately,
        resting_hold_seconds=resting_hold_seconds,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        enforce_trade_gate=enforce_trade_gate,
        enforce_ws_state_authority=enforce_ws_state_authority,
        ws_state_json=ws_path,
        ws_state_max_age_seconds=ws_state_max_age_seconds,
        order_group_auto_create=bool(allow_live_orders),
        plan_runner=_synthetic_plan_runner,
        now=captured_at,
    )

    return {
        "status": _normalize_text(execute_summary.get("status")) or ("ready" if plans else "no_candidates"),
        "captured_at": captured_at.isoformat(),
        "adaptive_policy_profile": adaptive_policy_profile_summary,
        "weather_pattern_profile": weather_pattern_profile_meta,
        "constraint_scan_summary": constraint_scan_summary,
        "intent_summary": intents_summary,
        "plan_summary": bridge_plan_summary,
        "execute_summary": execute_summary,
    }


def run_kalshi_temperature_shadow_watch(
    *,
    env_file: str,
    output_dir: str = "outputs",
    loops: int = 1,
    sleep_between_loops_seconds: float = 60.0,
    allow_live_orders: bool = False,
    shadow_quote_probe_on_no_candidates: bool = False,
    specs_csv: str | None = None,
    constraint_csv: str | None = None,
    metar_summary_json: str | None = None,
    metar_state_json: str | None = None,
    ws_state_json: str | None = None,
    alpha_consensus_json: str | None = None,
    settlement_state_json: str | None = None,
    book_db_path: str | None = None,
    policy_version: str = "temperature_policy_v1",
    contracts_per_order: int = 1,
    max_orders: int = 8,
    max_markets: int = 100,
    timeout_seconds: float = 12.0,
    min_settlement_confidence: float = 0.6,
    max_metar_age_minutes: float | None = 20.0,
    metar_age_policy_json: str | None = None,
    speci_calibration_json: str | None = None,
    min_alpha_strength: float | None = 0.0,
    min_probability_confidence: float | None = None,
    min_expected_edge_net: float | None = None,
    min_edge_to_risk_ratio: float | None = None,
    min_base_edge_net: float | None = 0.0,
    min_probability_breakeven_gap: float | None = 0.0,
    enforce_probability_edge_thresholds: bool = False,
    enforce_entry_price_probability_floor: bool = False,
    fallback_min_probability_confidence: float | None = None,
    fallback_min_expected_edge_net: float | None = 0.005,
    fallback_min_edge_to_risk_ratio: float | None = 0.02,
    enforce_interval_consistency: bool = True,
    max_yes_possible_gap_for_yes_side: float = 0.0,
    min_hours_to_close: float | None = 0.0,
    max_hours_to_close: float | None = 48.0,
    max_intents_per_underlying: int = 6,
    adaptive_policy_profile: dict[str, Any] | None = None,
    taf_stale_grace_minutes: float = 0.0,
    taf_stale_grace_max_volatility_score: float | None = 1.0,
    taf_stale_grace_max_range_width: float | None = 10.0,
    metar_freshness_quality_boundary_ratio: float | None = 0.92,
    metar_freshness_quality_probability_margin: float = 0.03,
    metar_freshness_quality_expected_edge_margin: float = 0.005,
    metar_ingest_quality_gate_enabled: bool = True,
    metar_ingest_min_quality_score: float | None = 0.70,
    metar_ingest_min_fresh_station_coverage_ratio: float | None = 0.55,
    metar_ingest_require_ready_status: bool = False,
    high_price_edge_guard_enabled: bool = False,
    high_price_edge_guard_min_entry_price_dollars: float = 0.85,
    high_price_edge_guard_min_expected_edge_net: float = 0.0,
    high_price_edge_guard_min_edge_to_risk_ratio: float = 0.02,
    yes_max_entry_price_dollars: float = 0.95,
    no_max_entry_price_dollars: float = 0.95,
    require_market_snapshot_seq: bool = True,
    require_metar_snapshot_sha: bool = False,
    enforce_underlying_netting: bool = True,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    enforce_trade_gate: bool = False,
    enforce_ws_state_authority: bool = False,
    ws_state_max_age_seconds: float = 30.0,
    max_total_deployed_pct: float = 0.35,
    max_same_station_exposure_pct: float = 0.6,
    max_same_hour_cluster_exposure_pct: float = 0.6,
    max_same_underlying_exposure_pct: float = 0.5,
    max_orders_per_station: int = 0,
    max_orders_per_underlying: int = 0,
    min_unique_stations_per_loop: int = 0,
    min_unique_underlyings_per_loop: int = 0,
    min_unique_local_hours_per_loop: int = 0,
    replan_market_side_cooldown_minutes: float = 20.0,
    replan_market_side_price_change_override_dollars: float = 0.02,
    replan_market_side_alpha_change_override: float = 0.2,
    replan_market_side_confidence_change_override: float = 0.03,
    replan_market_side_min_observation_advance_minutes: float = 2.0,
    replan_market_side_repeat_window_minutes: float = 1440.0,
    replan_market_side_max_plans_per_window: int = 8,
    replan_market_side_history_files: int = 180,
    replan_market_side_min_orders_backstop: int = 1,
    historical_selection_quality_enabled: bool = True,
    historical_selection_quality_lookback_hours: float = 14.0 * 24.0,
    historical_selection_quality_min_resolved_market_sides: int = 12,
    historical_selection_quality_min_bucket_samples: int = 4,
    historical_selection_quality_probability_penalty_max: float = 0.05,
    historical_selection_quality_expected_edge_penalty_max: float = 0.006,
    historical_selection_quality_score_adjust_scale: float = 0.35,
    historical_selection_quality_profile_max_age_hours: float = 96.0,
    historical_selection_quality_preferred_model: str = "fixed_fraction_per_underlying_family",
    weather_pattern_hardening_enabled: bool = True,
    weather_pattern_profile: dict[str, Any] | None = None,
    weather_pattern_profile_max_age_hours: float = 72.0,
    weather_pattern_min_bucket_samples: int = 12,
    weather_pattern_negative_expectancy_threshold: float = -0.05,
    weather_pattern_risk_off_enabled: bool = True,
    weather_pattern_risk_off_concentration_threshold: float = 0.75,
    weather_pattern_risk_off_min_attempts: int = 24,
    weather_pattern_risk_off_stale_metar_share_threshold: float = 0.50,
    trader_runner: Callable[..., dict[str, Any]] = run_kalshi_temperature_trader,
    sleep_fn: SleepFn = time.sleep,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_loops = int(loops)
    run_forever = safe_loops == 0
    if safe_loops < 0:
        safe_loops = 1
    if not run_forever:
        safe_loops = max(1, safe_loops)

    cycle_summaries: list[dict[str, Any]] = []
    run_index = 0
    while run_forever or run_index < safe_loops:
        cycle_now = captured_at if run_index == 0 and now is not None else datetime.now(timezone.utc)
        trader_kwargs = dict(
            env_file=env_file,
            output_dir=output_dir,
            specs_csv=specs_csv,
            constraint_csv=constraint_csv,
            metar_summary_json=metar_summary_json,
            metar_state_json=metar_state_json,
            ws_state_json=ws_state_json,
            alpha_consensus_json=alpha_consensus_json,
            settlement_state_json=settlement_state_json,
            book_db_path=book_db_path,
            policy_version=policy_version,
            contracts_per_order=contracts_per_order,
            max_orders=max_orders,
            max_markets=max_markets,
            timeout_seconds=timeout_seconds,
            allow_live_orders=allow_live_orders,
            shadow_quote_probe_on_no_candidates=shadow_quote_probe_on_no_candidates,
            intents_only=False,
            min_settlement_confidence=min_settlement_confidence,
            max_metar_age_minutes=max_metar_age_minutes,
            metar_age_policy_json=metar_age_policy_json,
            speci_calibration_json=speci_calibration_json,
            min_alpha_strength=min_alpha_strength,
            min_probability_confidence=min_probability_confidence,
            min_expected_edge_net=min_expected_edge_net,
            min_edge_to_risk_ratio=min_edge_to_risk_ratio,
            min_base_edge_net=min_base_edge_net,
            min_probability_breakeven_gap=min_probability_breakeven_gap,
            enforce_probability_edge_thresholds=enforce_probability_edge_thresholds,
            enforce_entry_price_probability_floor=enforce_entry_price_probability_floor,
            fallback_min_probability_confidence=fallback_min_probability_confidence,
            fallback_min_expected_edge_net=fallback_min_expected_edge_net,
            fallback_min_edge_to_risk_ratio=fallback_min_edge_to_risk_ratio,
            enforce_interval_consistency=enforce_interval_consistency,
            max_yes_possible_gap_for_yes_side=max_yes_possible_gap_for_yes_side,
            min_hours_to_close=min_hours_to_close,
            max_hours_to_close=max_hours_to_close,
            max_intents_per_underlying=max_intents_per_underlying,
            adaptive_policy_profile=adaptive_policy_profile,
            taf_stale_grace_minutes=taf_stale_grace_minutes,
            taf_stale_grace_max_volatility_score=taf_stale_grace_max_volatility_score,
            taf_stale_grace_max_range_width=taf_stale_grace_max_range_width,
            metar_freshness_quality_boundary_ratio=metar_freshness_quality_boundary_ratio,
            metar_freshness_quality_probability_margin=metar_freshness_quality_probability_margin,
            metar_freshness_quality_expected_edge_margin=metar_freshness_quality_expected_edge_margin,
            metar_ingest_quality_gate_enabled=metar_ingest_quality_gate_enabled,
            metar_ingest_min_quality_score=metar_ingest_min_quality_score,
            metar_ingest_min_fresh_station_coverage_ratio=metar_ingest_min_fresh_station_coverage_ratio,
            metar_ingest_require_ready_status=metar_ingest_require_ready_status,
            high_price_edge_guard_enabled=high_price_edge_guard_enabled,
            high_price_edge_guard_min_entry_price_dollars=high_price_edge_guard_min_entry_price_dollars,
            high_price_edge_guard_min_expected_edge_net=high_price_edge_guard_min_expected_edge_net,
            high_price_edge_guard_min_edge_to_risk_ratio=high_price_edge_guard_min_edge_to_risk_ratio,
            yes_max_entry_price_dollars=yes_max_entry_price_dollars,
            no_max_entry_price_dollars=no_max_entry_price_dollars,
            require_market_snapshot_seq=require_market_snapshot_seq,
            require_metar_snapshot_sha=require_metar_snapshot_sha,
            enforce_underlying_netting=enforce_underlying_netting,
            planning_bankroll_dollars=planning_bankroll_dollars,
            daily_risk_cap_dollars=daily_risk_cap_dollars,
            cancel_resting_immediately=cancel_resting_immediately,
            resting_hold_seconds=resting_hold_seconds,
            max_live_submissions_per_day=max_live_submissions_per_day,
            max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
            enforce_trade_gate=enforce_trade_gate,
            enforce_ws_state_authority=enforce_ws_state_authority,
            ws_state_max_age_seconds=ws_state_max_age_seconds,
            max_total_deployed_pct=max_total_deployed_pct,
            max_same_station_exposure_pct=max_same_station_exposure_pct,
            max_same_hour_cluster_exposure_pct=max_same_hour_cluster_exposure_pct,
            max_same_underlying_exposure_pct=max_same_underlying_exposure_pct,
            max_orders_per_station=max_orders_per_station,
            max_orders_per_underlying=max_orders_per_underlying,
            min_unique_stations_per_loop=min_unique_stations_per_loop,
            min_unique_underlyings_per_loop=min_unique_underlyings_per_loop,
            min_unique_local_hours_per_loop=min_unique_local_hours_per_loop,
            replan_market_side_cooldown_minutes=replan_market_side_cooldown_minutes,
            replan_market_side_price_change_override_dollars=replan_market_side_price_change_override_dollars,
            replan_market_side_alpha_change_override=replan_market_side_alpha_change_override,
            replan_market_side_confidence_change_override=replan_market_side_confidence_change_override,
            replan_market_side_min_observation_advance_minutes=replan_market_side_min_observation_advance_minutes,
            replan_market_side_repeat_window_minutes=replan_market_side_repeat_window_minutes,
            replan_market_side_max_plans_per_window=replan_market_side_max_plans_per_window,
            replan_market_side_history_files=replan_market_side_history_files,
            replan_market_side_min_orders_backstop=replan_market_side_min_orders_backstop,
            historical_selection_quality_enabled=historical_selection_quality_enabled,
            historical_selection_quality_lookback_hours=historical_selection_quality_lookback_hours,
            historical_selection_quality_min_resolved_market_sides=historical_selection_quality_min_resolved_market_sides,
            historical_selection_quality_min_bucket_samples=historical_selection_quality_min_bucket_samples,
            historical_selection_quality_probability_penalty_max=historical_selection_quality_probability_penalty_max,
            historical_selection_quality_expected_edge_penalty_max=historical_selection_quality_expected_edge_penalty_max,
            historical_selection_quality_score_adjust_scale=historical_selection_quality_score_adjust_scale,
            historical_selection_quality_profile_max_age_hours=historical_selection_quality_profile_max_age_hours,
            historical_selection_quality_preferred_model=historical_selection_quality_preferred_model,
            weather_pattern_hardening_enabled=weather_pattern_hardening_enabled,
            weather_pattern_profile=weather_pattern_profile,
            weather_pattern_profile_max_age_hours=weather_pattern_profile_max_age_hours,
            weather_pattern_min_bucket_samples=weather_pattern_min_bucket_samples,
            weather_pattern_negative_expectancy_threshold=weather_pattern_negative_expectancy_threshold,
            weather_pattern_risk_off_enabled=weather_pattern_risk_off_enabled,
            weather_pattern_risk_off_concentration_threshold=weather_pattern_risk_off_concentration_threshold,
            weather_pattern_risk_off_min_attempts=weather_pattern_risk_off_min_attempts,
            weather_pattern_risk_off_stale_metar_share_threshold=weather_pattern_risk_off_stale_metar_share_threshold,
            now=cycle_now,
        )
        if adaptive_policy_profile is not None:
            trader_kwargs["adaptive_policy_profile"] = adaptive_policy_profile
        cycle_summary = trader_runner(**trader_kwargs)
        cycle_profile_summary = (
            cycle_summary.get("adaptive_policy_profile")
            if isinstance(cycle_summary.get("adaptive_policy_profile"), dict)
            else {}
        )
        cycle_summaries.append(
            {
                "cycle_index": run_index + 1,
                "captured_at": cycle_now.isoformat(),
                "status": _normalize_text(cycle_summary.get("status")),
                "execute_status": _normalize_text(
                    (cycle_summary.get("execute_summary") if isinstance(cycle_summary.get("execute_summary"), dict) else {}).get("status")
                ),
                "intents_total": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_total")
                ),
                "intents_approved": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_approved")
                ),
                "intents_revalidated": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_revalidated")
                ),
                "revalidation_invalidated": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("revalidation_invalidated")
                ),
                "planned_orders": _parse_int(
                    (cycle_summary.get("plan_summary") if isinstance(cycle_summary.get("plan_summary"), dict) else {}).get("planned_orders")
                ),
                "summary_file": _normalize_text(
                    (cycle_summary.get("execute_summary") if isinstance(cycle_summary.get("execute_summary"), dict) else {}).get("output_file")
                ),
                "adaptive_policy_profile_applied": bool(cycle_profile_summary.get("adaptive_policy_profile_applied")),
                "adaptive_policy_profile_effective_overrides": (
                    cycle_profile_summary.get("adaptive_policy_profile_effective_overrides")
                    if isinstance(cycle_profile_summary.get("adaptive_policy_profile_effective_overrides"), dict)
                    else {}
                ),
            }
        )

        run_index += 1
        if run_forever or run_index < safe_loops:
            sleep_fn(max(0.0, float(sleep_between_loops_seconds)))

    status_counts: dict[str, int] = {}
    for cycle in cycle_summaries:
        key = _normalize_text(cycle.get("status")) or "unknown"
        status_counts[key] = status_counts.get(key, 0) + 1

    summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready",
        "mode": "live" if allow_live_orders else "shadow",
        "loops_requested": loops,
        "loops_run": run_index,
        "sleep_between_loops_seconds": float(sleep_between_loops_seconds),
        "cycle_status_counts": dict(sorted(status_counts.items(), key=lambda item: (-item[1], item[0]))),
        "cycle_summaries": cycle_summaries,
    }
    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"kalshi_temperature_shadow_watch_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
