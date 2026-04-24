from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


_DIMENSION_WEIGHTS = {
    "station": 0.35,
    "local_hour": 0.25,
    "signal_type": 0.25,
    "side": 0.15,
}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _latest_bankroll_validation_file(output_dir: str) -> Path | None:
    out_dir = Path(output_dir)
    candidates = list(out_dir.glob("kalshi_temperature_bankroll_validation_*.json"))
    if not candidates:
        return None
    try:
        return max(candidates, key=lambda path: path.stat().st_mtime)
    except OSError:
        # Fall back to deterministic filename ordering if stat access fails.
        return sorted(candidates, reverse=True)[0]


def _best_bankroll_validation_fallback(
    *,
    output_dir: str,
    captured_at: datetime,
    lookback_hours: float,
    max_profile_age_hours: float,
    min_resolved_market_sides: int,
    exclude_paths: set[str],
) -> tuple[Path, dict[str, Any], float] | None:
    out_dir = Path(output_dir)
    parent = out_dir.parent
    if not parent.exists():
        return None
    age_limit_hours = max(float(max_profile_age_hours) * 2.0, float(lookback_hours) * 1.5)
    candidate_paths = list(parent.glob("*/kalshi_temperature_bankroll_validation_*.json"))
    best: tuple[tuple[int, float], Path, dict[str, Any], float] | None = None
    for path in candidate_paths:
        if str(path) in exclude_paths:
            continue
        payload = _load_json_dict(path)
        if payload is None:
            continue
        age_hours = _payload_age_hours(payload, path=path, captured_at=captured_at)
        if age_hours is None or age_hours > age_limit_hours:
            continue
        opportunity = (
            payload.get("opportunity_breadth")
            if isinstance(payload.get("opportunity_breadth"), dict)
            else {}
        )
        resolved_market_sides = int(_parse_float(opportunity.get("resolved_unique_market_sides")) or 0)
        # Require at least some resolved outcomes and strongly prefer profiles
        # that satisfy the configured minimum resolved target.
        if resolved_market_sides <= 0:
            continue
        readiness_flag = 1 if resolved_market_sides >= int(min_resolved_market_sides) else 0
        score = (int(readiness_flag), float(resolved_market_sides) - float(age_hours) / 24.0)
        if best is None or score > best[0]:
            best = (score, path, payload, float(age_hours))
    if best is None:
        return None
    return best[1], best[2], best[3]


def _latest_profitability_file(output_dir: str) -> Path | None:
    out_dir = Path(output_dir)
    patterns = [
        "checkpoints/profitability_168h_*.json",
        "checkpoints/profitability_14h_*.json",
        "checkpoints/profitability_12h_*.json",
        "checkpoints/profitability_4h_*.json",
        "kalshi_temperature_profitability_summary_*.json",
    ]
    ranked: list[tuple[int, Path]] = []
    for rank, pattern in enumerate(patterns):
        for path in out_dir.glob(pattern):
            ranked.append((rank, path))
    if not ranked:
        return None
    try:
        ranked.sort(key=lambda item: (int(item[0]), -float(item[1].stat().st_mtime)))
    except OSError:
        ranked.sort(key=lambda item: (int(item[0]), _normalize_text(item[1].name)))
    return ranked[0][1]


def _latest_selection_quality_file(output_dir: str) -> Path | None:
    out_dir = Path(output_dir)
    candidates = list(out_dir.glob("kalshi_temperature_selection_quality_*.json"))
    if not candidates:
        return None
    try:
        return max(candidates, key=lambda path: path.stat().st_mtime)
    except OSError:
        return sorted(candidates, reverse=True)[0]


def _load_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _payload_age_hours(payload: dict[str, Any], *, path: Path, captured_at: datetime) -> float | None:
    payload_captured_at = _parse_ts(payload.get("captured_at"))
    if payload_captured_at is None:
        try:
            payload_captured_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return None
    return max(0.0, (captured_at - payload_captured_at).total_seconds() / 3600.0)


def _merge_bucket_entry(
    *,
    target_bucket: dict[str, dict[str, Any]],
    key: str,
    entry: dict[str, Any],
    source_label: str,
) -> None:
    key_text = _normalize_text(key)
    if not key_text:
        return
    samples = int(_parse_float(entry.get("samples")) or 0)
    penalty_ratio = _clamp(float(_parse_float(entry.get("penalty_ratio")) or 0.0), 0.0, 1.0)
    boost_ratio = _clamp(float(_parse_float(entry.get("boost_ratio")) or 0.0), 0.0, 1.0)
    if key_text not in target_bucket:
        merged = dict(entry)
        merged["samples"] = int(samples)
        merged["penalty_ratio"] = round(float(penalty_ratio), 6)
        merged["boost_ratio"] = round(float(boost_ratio), 6)
        merged["source_labels"] = [source_label] if source_label else []
        target_bucket[key_text] = merged
        return
    current = target_bucket[key_text]
    current["samples"] = max(int(_parse_float(current.get("samples")) or 0), int(samples))
    current["penalty_ratio"] = round(
        max(float(_parse_float(current.get("penalty_ratio")) or 0.0), float(penalty_ratio)),
        6,
    )
    current["boost_ratio"] = round(
        max(float(_parse_float(current.get("boost_ratio")) or 0.0), float(boost_ratio)),
        6,
    )
    labels = current.get("source_labels") if isinstance(current.get("source_labels"), list) else []
    if source_label and source_label not in labels:
        labels.append(source_label)
    current["source_labels"] = labels


def _profitability_bucket_penalties(
    *,
    profitability_payload: dict[str, Any],
    min_bucket_samples: int,
) -> dict[str, Any]:
    shadow_ref = (
        profitability_payload.get("shadow_settled_reference")
        if isinstance(profitability_payload.get("shadow_settled_reference"), dict)
        else {}
    )
    expected_shadow = (
        profitability_payload.get("expected_shadow")
        if isinstance(profitability_payload.get("expected_shadow"), dict)
        else {}
    )
    expected_vs_realized = (
        profitability_payload.get("expected_vs_realized")
        if isinstance(profitability_payload.get("expected_vs_realized"), dict)
        else {}
    )
    attribution = (
        profitability_payload.get("attribution")
        if isinstance(profitability_payload.get("attribution"), dict)
        else {}
    )
    expected_edge_total = _parse_float(expected_shadow.get("expected_edge_total"))
    counterfactual_candidates = [
        _parse_float(shadow_ref.get("counterfactual_pnl_total_unique_shadow_orders_dollars_if_live")),
        _parse_float(shadow_ref.get("counterfactual_pnl_total_unique_market_sides_dollars_if_live")),
        _parse_float(shadow_ref.get("counterfactual_pnl_total_rows_dollars_if_live")),
        _parse_float(expected_vs_realized.get("realized_pnl_total")),
    ]
    counterfactual_candidates = [
        float(value)
        for value in counterfactual_candidates
        if isinstance(value, float)
    ]
    counterfactual_total = counterfactual_candidates[0] if counterfactual_candidates else None
    calibration_ratio = _parse_float(expected_vs_realized.get("selection_calibration_ratio"))
    if not isinstance(calibration_ratio, float):
        calibration_ratio = _parse_float(expected_vs_realized.get("calibration_ratio"))
    if isinstance(expected_edge_total, float) and expected_edge_total > 0.0 and isinstance(counterfactual_total, float):
        calibration_ratio = counterfactual_total / expected_edge_total
    if calibration_ratio is None:
        return {
            "enabled": False,
            "status": "missing_calibration_ratio",
            "calibration_ratio": None,
            "bucket_profiles": {"station": {}, "local_hour": {}},
            "resolved_unique_market_sides": int(_parse_float(shadow_ref.get("resolved_unique_market_sides")) or 0),
        }
    calibration_ratio = _clamp(float(calibration_ratio), 0.0, 2.0)
    calibration_gap_ratio = _clamp(1.0 - calibration_ratio, 0.0, 1.0)
    resolved_unique_market_sides = int(_parse_float(shadow_ref.get("resolved_unique_market_sides")) or 0)
    if calibration_gap_ratio <= 0.0:
        return {
            "enabled": False,
            "status": "no_overestimation_gap",
            "calibration_ratio": round(float(calibration_ratio), 6),
            "bucket_profiles": {"station": {}, "local_hour": {}},
            "resolved_unique_market_sides": int(resolved_unique_market_sides),
        }
    if resolved_unique_market_sides < max(3, int(min_bucket_samples)):
        return {
            "enabled": False,
            "status": "insufficient_resolved_market_sides",
            "calibration_ratio": round(float(calibration_ratio), 6),
            "bucket_profiles": {"station": {}, "local_hour": {}},
            "resolved_unique_market_sides": int(resolved_unique_market_sides),
        }

    dimension_map = {
        "station": "by_station",
        "local_hour": "by_local_hour",
    }
    bucket_profiles: dict[str, dict[str, dict[str, Any]]] = {
        "station": {},
        "local_hour": {},
    }
    applied_bucket_counts: dict[str, int] = {"station": 0, "local_hour": 0}

    for dim_name, source_key in dimension_map.items():
        dim_payload = attribution.get(source_key)
        if not isinstance(dim_payload, dict):
            continue
        valid_rows: list[tuple[str, dict[str, Any], float, int, float | None]] = []
        for key, row in dim_payload.items():
            if not isinstance(row, dict):
                continue
            key_text = _normalize_text(key)
            if not key_text:
                continue
            if dim_name == "local_hour" and key_text.lower() == "unknown":
                continue
            planned_orders = int(_parse_float(row.get("planned_orders")) or 0)
            expected_edge = float(_parse_float(row.get("expected_edge_total")) or 0.0)
            approval_rate = _parse_float(row.get("approval_rate"))
            if planned_orders < max(1, int(min_bucket_samples)) or expected_edge <= 0.0:
                continue
            valid_rows.append((key_text, row, expected_edge, planned_orders, approval_rate))
        total_expected_edge = sum(item[2] for item in valid_rows)
        if total_expected_edge <= 0.0:
            continue
        for key_text, _row, expected_edge, planned_orders, approval_rate in valid_rows:
            expected_edge_share = _clamp(expected_edge / total_expected_edge, 0.0, 1.0)
            concentration_pressure = _clamp((expected_edge_share - 0.08) / 0.32, 0.0, 1.0)
            throughput_pressure = _clamp(
                float(planned_orders) / max(1.0, float(min_bucket_samples) * 8.0),
                0.0,
                1.0,
            )
            approval_pressure = (
                _clamp((0.10 - float(approval_rate)) / 0.10, 0.0, 1.0)
                if isinstance(approval_rate, float)
                else 0.0
            )
            penalty_ratio = calibration_gap_ratio * _clamp(
                0.35 + 0.40 * concentration_pressure + 0.20 * throughput_pressure + 0.05 * approval_pressure,
                0.0,
                1.0,
            )
            penalty_ratio = _clamp(float(penalty_ratio), 0.0, 1.0)
            if penalty_ratio <= 0.0:
                continue
            bucket_profiles[dim_name][key_text] = {
                "samples": int(planned_orders),
                "penalty_ratio": round(float(penalty_ratio), 6),
                "boost_ratio": 0.0,
                "calibration_gap_ratio": round(float(calibration_gap_ratio), 6),
                "expected_edge_share": round(float(expected_edge_share), 6),
                "expected_edge_total": round(float(expected_edge), 6),
            }
        applied_bucket_counts[dim_name] = int(len(bucket_profiles[dim_name]))

    return {
        "enabled": bool(bucket_profiles["station"] or bucket_profiles["local_hour"]),
        "status": "ready" if (bucket_profiles["station"] or bucket_profiles["local_hour"]) else "no_bucket_penalties",
        "calibration_ratio": round(float(calibration_ratio), 6),
        "calibration_gap_ratio": round(float(calibration_gap_ratio), 6),
        "resolved_unique_market_sides": int(resolved_unique_market_sides),
        "applied_bucket_counts": applied_bucket_counts,
        "bucket_profiles": bucket_profiles,
    }


def _best_profitability_fallback(
    *,
    output_dir: str,
    captured_at: datetime,
    lookback_hours: float,
    max_profile_age_hours: float,
    min_bucket_samples: int,
    exclude_paths: set[str],
) -> tuple[Path, dict[str, Any], dict[str, Any], float] | None:
    out_dir = Path(output_dir)
    parent = out_dir.parent
    if not parent.exists():
        return None
    age_limit_hours = max(float(max_profile_age_hours) * 2.0, float(lookback_hours) * 1.5)
    candidate_paths: list[Path] = []
    for pattern in (
        "*/checkpoints/profitability_168h_*.json",
        "*/checkpoints/profitability_14h_*.json",
        "*/checkpoints/profitability_12h_*.json",
        "*/checkpoints/profitability_4h_*.json",
        "*/kalshi_temperature_profitability_summary_*.json",
    ):
        candidate_paths.extend(parent.glob(pattern))
    best: tuple[tuple[int, int, float], Path, dict[str, Any], dict[str, Any], float] | None = None
    for path in candidate_paths:
        if str(path) in exclude_paths:
            continue
        payload = _load_json_dict(path)
        if payload is None:
            continue
        age_hours = _payload_age_hours(payload, path=path, captured_at=captured_at)
        if age_hours is None or age_hours > age_limit_hours:
            continue
        gap_payload = _profitability_bucket_penalties(
            profitability_payload=payload,
            min_bucket_samples=min_bucket_samples,
        )
        if not bool(gap_payload.get("enabled")):
            continue
        bucket_counts = (
            gap_payload.get("applied_bucket_counts")
            if isinstance(gap_payload.get("applied_bucket_counts"), dict)
            else {}
        )
        score = (
            int(_parse_float(gap_payload.get("resolved_unique_market_sides")) or 0),
            int(_parse_float(bucket_counts.get("station")) or 0)
            + int(_parse_float(bucket_counts.get("local_hour")) or 0),
            -float(age_hours),
        )
        if best is None or score > best[0]:
            best = (score, path, payload, gap_payload, float(age_hours))
    if best is None:
        return None
    return best[1], best[2], best[3], best[4]


def _resolve_attribution_model(payload: dict[str, Any], preferred_model: str) -> tuple[str, dict[str, Any]]:
    attribution = payload.get("attribution")
    if not isinstance(attribution, dict) or not attribution:
        return "", {}
    if preferred_model in attribution and isinstance(attribution.get(preferred_model), dict):
        return preferred_model, dict(attribution[preferred_model])
    for key, value in attribution.items():
        if isinstance(value, dict):
            return _normalize_text(key), dict(value)
    return "", {}


def _build_bucket_overrides(
    *,
    source: dict[str, Any],
    dimension: str,
    global_win_rate: float | None,
    global_expectancy: float | None,
    min_bucket_samples: int,
) -> dict[str, dict[str, Any]]:
    payload = source.get(dimension)
    if not isinstance(payload, dict):
        return {}
    best_rows = payload.get("best") if isinstance(payload.get("best"), list) else []
    worst_rows = payload.get("worst") if isinstance(payload.get("worst"), list) else []
    bucket: dict[str, dict[str, Any]] = {}
    baseline_expectancy_scale = max(0.02, abs(float(global_expectancy or 0.0)) * 1.5)
    reliability_den = max(1.0, float(min_bucket_samples) * 2.0)

    def _upsert(
        key: str,
        *,
        trade_count: int,
        win_rate: float | None,
        expectancy: float,
        penalty: float,
        boost: float,
    ) -> None:
        if key not in bucket:
            bucket[key] = {
                "samples": int(trade_count),
                "win_rate": round(float(win_rate), 6) if isinstance(win_rate, float) else None,
                "expectancy_per_trade": round(float(expectancy), 6),
                "penalty_ratio": round(float(max(0.0, penalty)), 6),
                "boost_ratio": round(float(max(0.0, boost)), 6),
            }
            return
        current = bucket[key]
        current["samples"] = max(int(current.get("samples") or 0), int(trade_count))
        if isinstance(win_rate, float):
            current["win_rate"] = round(float(win_rate), 6)
        current["expectancy_per_trade"] = round(float(expectancy), 6)
        current["penalty_ratio"] = round(
            max(float(current.get("penalty_ratio") or 0.0), float(max(0.0, penalty))),
            6,
        )
        current["boost_ratio"] = round(
            max(float(current.get("boost_ratio") or 0.0), float(max(0.0, boost))),
            6,
        )

    for row in worst_rows:
        key = _normalize_text(row.get("key"))
        trade_count = int(_parse_float(row.get("trade_count")) or 0)
        if not key or trade_count < max(1, int(min_bucket_samples)):
            continue
        win_rate = _parse_float(row.get("win_rate"))
        pnl_total = _parse_float(row.get("pnl_total")) or 0.0
        expectancy = pnl_total / max(1.0, float(trade_count))
        reliability = _clamp(float(trade_count) / reliability_den, 0.0, 1.0)
        win_gap = (
            max(0.0, float(global_win_rate) - float(win_rate))
            if isinstance(global_win_rate, float) and isinstance(win_rate, float)
            else 0.0
        )
        expectancy_gap = (
            max(0.0, float(global_expectancy) - float(expectancy))
            if isinstance(global_expectancy, float)
            else max(0.0, -float(expectancy))
        )
        penalty = reliability * _clamp((win_gap / 0.25) * 0.7 + (expectancy_gap / baseline_expectancy_scale) * 0.3, 0.0, 1.0)
        _upsert(
            key,
            trade_count=trade_count,
            win_rate=win_rate,
            expectancy=expectancy,
            penalty=penalty,
            boost=0.0,
        )

    for row in best_rows:
        key = _normalize_text(row.get("key"))
        trade_count = int(_parse_float(row.get("trade_count")) or 0)
        if not key or trade_count < max(1, int(min_bucket_samples)):
            continue
        win_rate = _parse_float(row.get("win_rate"))
        pnl_total = _parse_float(row.get("pnl_total")) or 0.0
        expectancy = pnl_total / max(1.0, float(trade_count))
        reliability = _clamp(float(trade_count) / reliability_den, 0.0, 1.0)
        win_gap = (
            max(0.0, float(win_rate) - float(global_win_rate))
            if isinstance(global_win_rate, float) and isinstance(win_rate, float)
            else 0.0
        )
        expectancy_gap = (
            max(0.0, float(expectancy) - float(global_expectancy))
            if isinstance(global_expectancy, float)
            else max(0.0, float(expectancy))
        )
        boost = reliability * _clamp((win_gap / 0.25) * 0.6 + (expectancy_gap / baseline_expectancy_scale) * 0.4, 0.0, 1.0)
        _upsert(
            key,
            trade_count=trade_count,
            win_rate=win_rate,
            expectancy=expectancy,
            penalty=0.0,
            boost=boost,
        )

    return dict(sorted(bucket.items(), key=lambda item: item[0]))


def load_temperature_selection_quality_profile(
    *,
    output_dir: str,
    now_utc: datetime | None = None,
    enabled: bool = True,
    lookback_hours: float = 14.0 * 24.0,
    min_resolved_market_sides: int = 12,
    min_bucket_samples: int = 4,
    preferred_attribution_model: str = "fixed_fraction_per_underlying_family",
    max_profile_age_hours: float = 96.0,
) -> dict[str, Any]:
    captured_at = now_utc or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)

    if not enabled:
        return {
            "enabled": False,
            "status": "disabled",
            "captured_at": captured_at.isoformat(),
            "lookback_hours": round(max(1.0, float(lookback_hours)), 3),
        }

    latest_path = _latest_bankroll_validation_file(output_dir)
    if latest_path is None:
        return {
            "enabled": True,
            "status": "no_bankroll_validation_artifact",
            "captured_at": captured_at.isoformat(),
            "lookback_hours": round(max(1.0, float(lookback_hours)), 3),
            "source_file": "",
        }
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "enabled": True,
            "status": "bankroll_validation_parse_failed",
            "captured_at": captured_at.isoformat(),
            "lookback_hours": round(max(1.0, float(lookback_hours)), 3),
            "source_file": str(latest_path),
        }

    payload_captured_at = _parse_ts(payload.get("captured_at")) if isinstance(payload, dict) else None
    if payload_captured_at is None:
        try:
            payload_captured_at = datetime.fromtimestamp(latest_path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            payload_captured_at = captured_at
    age_hours = max(0.0, (captured_at - payload_captured_at).total_seconds() / 3600.0)

    opportunity = payload.get("opportunity_breadth") if isinstance(payload.get("opportunity_breadth"), dict) else {}
    resolved_unique_market_sides = int(_parse_float(opportunity.get("resolved_unique_market_sides")) or 0)
    resolved_planned_rows = int(_parse_float(opportunity.get("resolved_planned_rows")) or 0)
    repeated_entry_multiplier = _parse_float(opportunity.get("repeated_entry_multiplier"))

    hit_quality = payload.get("hit_rate_quality") if isinstance(payload.get("hit_rate_quality"), dict) else {}
    unique_market_side_hit = (
        hit_quality.get("unique_market_side") if isinstance(hit_quality.get("unique_market_side"), dict) else {}
    )
    global_win_rate = _parse_float(unique_market_side_hit.get("win_rate"))
    global_expectancy = _parse_float(unique_market_side_hit.get("expectancy_per_trade"))
    global_wins = int(_parse_float(unique_market_side_hit.get("wins")) or 0)
    global_losses = int(_parse_float(unique_market_side_hit.get("losses")) or 0)
    global_pushes = int(_parse_float(unique_market_side_hit.get("pushes")) or 0)

    expected_vs_shadow = (
        payload.get("expected_vs_shadow_settled")
        if isinstance(payload.get("expected_vs_shadow_settled"), dict)
        else {}
    )
    calibration_ratio = _parse_float(expected_vs_shadow.get("calibration_ratio"))

    concentration_checks = (
        payload.get("concentration_checks") if isinstance(payload.get("concentration_checks"), dict) else {}
    )
    concentration_warning = bool(concentration_checks.get("concentration_warning"))
    global_adjustment_target_share = 0.10
    global_adjustment_min_rows = 100
    global_adjustment_profile: dict[str, Any] = {
        "status": "unavailable",
        "source_file": "",
        "source_captured_at": None,
        "source_age_hours": None,
        "rows_total": 0,
        "rows_adjusted": 0,
        "rows_adjusted_global_only": 0,
        "global_only_adjusted_share": None,
        "global_only_total_share": None,
        "target_share": round(float(global_adjustment_target_share), 6),
        "min_rows_for_pressure": int(global_adjustment_min_rows),
        "pressure_active": False,
    }
    latest_selection_quality_path = _latest_selection_quality_file(output_dir)
    if latest_selection_quality_path is not None:
        selection_quality_payload = _load_json_dict(latest_selection_quality_path)
        if selection_quality_payload is None:
            global_adjustment_profile["status"] = "selection_quality_parse_failed"
            global_adjustment_profile["source_file"] = str(latest_selection_quality_path)
        else:
            intent_window = (
                selection_quality_payload.get("intent_window")
                if isinstance(selection_quality_payload.get("intent_window"), dict)
                else {}
            )
            rows_total = int(_parse_float(intent_window.get("rows_total")) or 0)
            rows_adjusted = int(_parse_float(intent_window.get("rows_adjusted")) or 0)
            rows_adjusted_global_only = int(
                _parse_float(intent_window.get("rows_adjusted_global_only")) or 0
            )
            global_only_adjusted_share = (
                _clamp(float(rows_adjusted_global_only) / float(rows_adjusted), 0.0, 1.0)
                if rows_adjusted > 0
                else None
            )
            global_only_total_share = (
                _clamp(float(rows_adjusted_global_only) / float(rows_total), 0.0, 1.0)
                if rows_total > 0
                else None
            )
            selection_quality_captured_at = _parse_ts(selection_quality_payload.get("captured_at"))
            if selection_quality_captured_at is None:
                try:
                    selection_quality_captured_at = datetime.fromtimestamp(
                        latest_selection_quality_path.stat().st_mtime, tz=timezone.utc
                    )
                except OSError:
                    selection_quality_captured_at = captured_at
            selection_quality_age_hours = max(
                0.0,
                (captured_at - selection_quality_captured_at).total_seconds() / 3600.0,
            )
            pressure_active = bool(
                rows_adjusted >= int(global_adjustment_min_rows)
                and isinstance(global_only_adjusted_share, float)
                and global_only_adjusted_share >= float(global_adjustment_target_share)
                and selection_quality_age_hours <= max(0.0, float(max_profile_age_hours))
            )
            global_adjustment_profile.update(
                {
                    "status": (
                        "stale_selection_quality"
                        if selection_quality_age_hours > max(0.0, float(max_profile_age_hours))
                        else ("ready" if rows_adjusted > 0 else "no_adjusted_rows")
                    ),
                    "source_file": str(latest_selection_quality_path),
                    "source_captured_at": (
                        selection_quality_captured_at.isoformat()
                        if isinstance(selection_quality_captured_at, datetime)
                        else None
                    ),
                    "source_age_hours": round(float(selection_quality_age_hours), 6),
                    "rows_total": int(rows_total),
                    "rows_adjusted": int(rows_adjusted),
                    "rows_adjusted_global_only": int(rows_adjusted_global_only),
                    "global_only_adjusted_share": (
                        round(float(global_only_adjusted_share), 6)
                        if isinstance(global_only_adjusted_share, float)
                        else None
                    ),
                    "global_only_total_share": (
                        round(float(global_only_total_share), 6)
                        if isinstance(global_only_total_share, float)
                        else None
                    ),
                    "pressure_active": bool(pressure_active),
                }
            )

    resolved_target = max(1, int(min_resolved_market_sides))
    fallback_profile_applied = False
    fallback_profile_source = ""
    stale_profile = age_hours > max(0.0, float(max_profile_age_hours))
    insufficient_resolved = resolved_unique_market_sides < resolved_target
    if stale_profile or insufficient_resolved:
        fallback_profile = _best_bankroll_validation_fallback(
            output_dir=output_dir,
            captured_at=captured_at,
            lookback_hours=lookback_hours,
            max_profile_age_hours=max_profile_age_hours,
            min_resolved_market_sides=resolved_target,
            exclude_paths={str(latest_path)},
        )
        if fallback_profile is not None:
            fallback_path, fallback_payload, fallback_age_hours = fallback_profile
            fallback_opportunity = (
                fallback_payload.get("opportunity_breadth")
                if isinstance(fallback_payload.get("opportunity_breadth"), dict)
                else {}
            )
            fallback_resolved = int(_parse_float(fallback_opportunity.get("resolved_unique_market_sides")) or 0)
            if fallback_resolved > resolved_unique_market_sides:
                payload = fallback_payload
                latest_path = fallback_path
                age_hours = float(fallback_age_hours)
                payload_captured_at = _parse_ts(payload.get("captured_at"))
                if payload_captured_at is None:
                    try:
                        payload_captured_at = datetime.fromtimestamp(latest_path.stat().st_mtime, tz=timezone.utc)
                    except OSError:
                        payload_captured_at = captured_at
                opportunity = (
                    payload.get("opportunity_breadth")
                    if isinstance(payload.get("opportunity_breadth"), dict)
                    else {}
                )
                resolved_unique_market_sides = int(
                    _parse_float(opportunity.get("resolved_unique_market_sides")) or 0
                )
                resolved_planned_rows = int(_parse_float(opportunity.get("resolved_planned_rows")) or 0)
                repeated_entry_multiplier = _parse_float(opportunity.get("repeated_entry_multiplier"))
                hit_quality = (
                    payload.get("hit_rate_quality")
                    if isinstance(payload.get("hit_rate_quality"), dict)
                    else {}
                )
                unique_market_side_hit = (
                    hit_quality.get("unique_market_side")
                    if isinstance(hit_quality.get("unique_market_side"), dict)
                    else {}
                )
                global_win_rate = _parse_float(unique_market_side_hit.get("win_rate"))
                global_expectancy = _parse_float(unique_market_side_hit.get("expectancy_per_trade"))
                global_wins = int(_parse_float(unique_market_side_hit.get("wins")) or 0)
                global_losses = int(_parse_float(unique_market_side_hit.get("losses")) or 0)
                global_pushes = int(_parse_float(unique_market_side_hit.get("pushes")) or 0)
                expected_vs_shadow = (
                    payload.get("expected_vs_shadow_settled")
                    if isinstance(payload.get("expected_vs_shadow_settled"), dict)
                    else {}
                )
                calibration_ratio = _parse_float(expected_vs_shadow.get("calibration_ratio"))
                concentration_checks = (
                    payload.get("concentration_checks")
                    if isinstance(payload.get("concentration_checks"), dict)
                    else {}
                )
                concentration_warning = bool(concentration_checks.get("concentration_warning"))
                fallback_profile_applied = True
                fallback_profile_source = str(fallback_path)

    coverage_score = _clamp(float(resolved_unique_market_sides) / float(resolved_target), 0.0, 1.0)
    win_score = (
        _clamp((float(global_win_rate) - 0.5) / 0.3, 0.0, 1.0)
        if isinstance(global_win_rate, float)
        else 0.0
    )
    calibration_score = (
        _clamp(float(calibration_ratio), 0.0, 1.0)
        if isinstance(calibration_ratio, float)
        else 0.4
    )
    concentration_score = 0.0 if concentration_warning else 1.0
    evidence_confidence_raw = _clamp(
        0.45 * coverage_score + 0.30 * win_score + 0.20 * calibration_score + 0.05 * concentration_score,
        0.0,
        1.0,
    )
    repeated_entry_multiplier_penalty_ratio = (
        _clamp((float(repeated_entry_multiplier) - 6.0) / 28.0, 0.0, 0.40)
        if isinstance(repeated_entry_multiplier, float)
        else 0.0
    )
    source_age_penalty_ratio = _clamp(
        max(0.0, float(age_hours) - (float(max_profile_age_hours) * 0.5))
        / max(1.0, float(max_profile_age_hours) * 1.5),
        0.0,
        0.20,
    )
    fallback_profile_penalty_ratio = 0.06 if fallback_profile_applied else 0.0
    evidence_confidence = _clamp(
        (
            float(evidence_confidence_raw)
            * (1.0 - (0.75 * float(repeated_entry_multiplier_penalty_ratio)))
        )
        - float(source_age_penalty_ratio)
        - float(fallback_profile_penalty_ratio),
        0.0,
        1.0,
    )
    global_penalty_ratio = _clamp(
        ((1.0 - float(evidence_confidence)) * 0.75)
        + (0.15 * float(repeated_entry_multiplier_penalty_ratio)),
        0.0,
        1.0,
    )
    global_boost_ratio = _clamp((float(evidence_confidence) - 0.68) / 0.32, 0.0, 1.0)
    if fallback_profile_applied and repeated_entry_multiplier_penalty_ratio >= 0.30:
        global_boost_ratio = min(float(global_boost_ratio), 0.15)
    if age_hours >= (float(max_profile_age_hours) * 0.75):
        global_boost_ratio = min(float(global_boost_ratio), 0.20)

    selected_model_name, selected_model = _resolve_attribution_model(payload, preferred_attribution_model)
    bucket_profiles = {
        "station": _build_bucket_overrides(
            source=selected_model,
            dimension="by_station",
            global_win_rate=global_win_rate,
            global_expectancy=global_expectancy,
            min_bucket_samples=min_bucket_samples,
        ),
        "local_hour": _build_bucket_overrides(
            source=selected_model,
            dimension="by_local_hour",
            global_win_rate=global_win_rate,
            global_expectancy=global_expectancy,
            min_bucket_samples=min_bucket_samples,
        ),
        "signal_type": _build_bucket_overrides(
            source=selected_model,
            dimension="by_signal_type",
            global_win_rate=global_win_rate,
            global_expectancy=global_expectancy,
            min_bucket_samples=min_bucket_samples,
        ),
        "policy_reason": _build_bucket_overrides(
            source=selected_model,
            dimension="by_policy_reason",
            global_win_rate=global_win_rate,
            global_expectancy=global_expectancy,
            min_bucket_samples=min_bucket_samples,
        ),
        "side": {},
    }
    # Side-level attribution is not emitted directly; synthesize from signal-type
    # buckets when common side labels appear there.
    for key, value in bucket_profiles["signal_type"].items():
        key_text = _normalize_text(key).lower()
        if key_text in {"yes", "no"}:
            bucket_profiles["side"][key_text] = dict(value)

    profitability_path = _latest_profitability_file(output_dir)
    profitability_fallback_applied = False
    profitability_fallback_source = ""
    profitability_gap: dict[str, Any] = {
        "enabled": False,
        "status": "no_profitability_artifact",
        "source_file": "",
        "source_age_hours": None,
        "calibration_ratio": None,
        "calibration_gap_ratio": None,
        "resolved_unique_market_sides": None,
        "applied_bucket_counts": {"station": 0, "local_hour": 0},
    }
    if profitability_path is not None:
        profitability_gap["source_file"] = str(profitability_path)
        try:
            profitability_payload = json.loads(profitability_path.read_text(encoding="utf-8"))
            profitability_captured_at = _parse_ts(profitability_payload.get("captured_at"))
            if profitability_captured_at is None:
                profitability_captured_at = datetime.fromtimestamp(profitability_path.stat().st_mtime, tz=timezone.utc)
            profitability_age_hours = max(0.0, (captured_at - profitability_captured_at).total_seconds() / 3600.0)
            profitability_gap["source_age_hours"] = round(float(profitability_age_hours), 6)
            if profitability_age_hours <= max(0.0, float(max_profile_age_hours)):
                gap_payload = _profitability_bucket_penalties(
                    profitability_payload=profitability_payload,
                    min_bucket_samples=min_bucket_samples,
                )
                profitability_gap.update(
                    {
                        "enabled": bool(gap_payload.get("enabled")),
                        "status": _normalize_text(gap_payload.get("status")) or "unknown",
                        "calibration_ratio": gap_payload.get("calibration_ratio"),
                        "calibration_gap_ratio": gap_payload.get("calibration_gap_ratio"),
                        "resolved_unique_market_sides": gap_payload.get("resolved_unique_market_sides"),
                        "applied_bucket_counts": gap_payload.get("applied_bucket_counts")
                        if isinstance(gap_payload.get("applied_bucket_counts"), dict)
                        else {"station": 0, "local_hour": 0},
                    }
                )
                source_bucket_profiles = (
                    gap_payload.get("bucket_profiles")
                    if isinstance(gap_payload.get("bucket_profiles"), dict)
                    else {}
                )
                for dim_name in ("station", "local_hour"):
                    source_bucket = source_bucket_profiles.get(dim_name)
                    if not isinstance(source_bucket, dict):
                        continue
                    target_bucket = bucket_profiles.get(dim_name)
                    if not isinstance(target_bucket, dict):
                        target_bucket = {}
                        bucket_profiles[dim_name] = target_bucket
                    for key, entry in source_bucket.items():
                        if isinstance(entry, dict):
                            _merge_bucket_entry(
                                target_bucket=target_bucket,
                                key=key,
                                entry=entry,
                                source_label="profitability_gap",
                            )
            else:
                profitability_gap["status"] = "profitability_profile_stale"
        except (OSError, json.JSONDecodeError, ValueError):
            profitability_gap["status"] = "profitability_profile_parse_failed"
    if not bool(profitability_gap.get("enabled")):
        fallback_profitability = _best_profitability_fallback(
            output_dir=output_dir,
            captured_at=captured_at,
            lookback_hours=lookback_hours,
            max_profile_age_hours=max_profile_age_hours,
            min_bucket_samples=min_bucket_samples,
            exclude_paths={str(path) for path in (profitability_path,) if path is not None},
        )
        if fallback_profitability is not None:
            fallback_path, _fallback_payload, gap_payload, fallback_age_hours = fallback_profitability
            profitability_fallback_applied = True
            profitability_fallback_source = str(fallback_path)
            profitability_gap.update(
                {
                    "enabled": bool(gap_payload.get("enabled")),
                    "status": _normalize_text(gap_payload.get("status")) or "unknown",
                    "source_file": str(fallback_path),
                    "source_age_hours": round(float(fallback_age_hours), 6),
                    "calibration_ratio": gap_payload.get("calibration_ratio"),
                    "calibration_gap_ratio": gap_payload.get("calibration_gap_ratio"),
                    "resolved_unique_market_sides": gap_payload.get("resolved_unique_market_sides"),
                    "applied_bucket_counts": gap_payload.get("applied_bucket_counts")
                    if isinstance(gap_payload.get("applied_bucket_counts"), dict)
                    else {"station": 0, "local_hour": 0},
                }
            )
            source_bucket_profiles = (
                gap_payload.get("bucket_profiles")
                if isinstance(gap_payload.get("bucket_profiles"), dict)
                else {}
            )
            for dim_name in ("station", "local_hour"):
                source_bucket = source_bucket_profiles.get(dim_name)
                if not isinstance(source_bucket, dict):
                    continue
                target_bucket = bucket_profiles.get(dim_name)
                if not isinstance(target_bucket, dict):
                    target_bucket = {}
                    bucket_profiles[dim_name] = target_bucket
                for key, entry in source_bucket.items():
                    if isinstance(entry, dict):
                        _merge_bucket_entry(
                            target_bucket=target_bucket,
                            key=key,
                            entry=entry,
                            source_label="profitability_gap_fallback",
                        )

    stale_profile = age_hours > max(0.0, float(max_profile_age_hours))
    insufficient_resolved = resolved_unique_market_sides < resolved_target
    if stale_profile:
        status = "stale_profile"
    elif insufficient_resolved:
        status = "insufficient_resolved_market_sides"
    else:
        status = "ready"

    return {
        "enabled": True,
        "status": status,
        "captured_at": captured_at.isoformat(),
        "lookback_hours": round(max(1.0, float(lookback_hours)), 3),
        "max_profile_age_hours": round(max(0.0, float(max_profile_age_hours)), 3),
        "source_file": str(latest_path),
        "source_captured_at": payload_captured_at.isoformat(),
        "source_age_hours": round(age_hours, 6),
        "fallback_profile_applied": bool(fallback_profile_applied),
        "fallback_profile_source_file": fallback_profile_source,
        "resolved_unique_market_sides": int(resolved_unique_market_sides),
        "resolved_planned_rows": int(resolved_planned_rows),
        "repeated_entry_multiplier": round(float(repeated_entry_multiplier), 6)
        if isinstance(repeated_entry_multiplier, float)
        else None,
        "global": {
            "wins": int(global_wins),
            "losses": int(global_losses),
            "pushes": int(global_pushes),
            "win_rate": round(float(global_win_rate), 6) if isinstance(global_win_rate, float) else None,
            "expectancy_per_trade": (
                round(float(global_expectancy), 6) if isinstance(global_expectancy, float) else None
            ),
            "calibration_ratio": (
                round(float(calibration_ratio), 6) if isinstance(calibration_ratio, float) else None
            ),
            "concentration_warning": bool(concentration_warning),
        },
        "evidence_confidence": round(float(evidence_confidence), 6),
        "evidence_confidence_raw": round(float(evidence_confidence_raw), 6),
        "repeated_entry_multiplier_penalty_ratio": round(float(repeated_entry_multiplier_penalty_ratio), 6),
        "source_age_penalty_ratio": round(float(source_age_penalty_ratio), 6),
        "fallback_profile_penalty_ratio": round(float(fallback_profile_penalty_ratio), 6),
        "global_penalty_ratio": round(float(global_penalty_ratio), 6),
        "global_boost_ratio": round(float(global_boost_ratio), 6),
        "min_resolved_market_sides_required": int(resolved_target),
        "min_bucket_samples_required": int(max(1, int(min_bucket_samples))),
        "attribution_model_used": selected_model_name,
        "profitability_calibration_gap": profitability_gap,
        "profitability_calibration_gap_fallback_applied": bool(profitability_fallback_applied),
        "profitability_calibration_gap_fallback_source_file": profitability_fallback_source,
        "global_adjustment_profile": global_adjustment_profile,
        "bucket_profiles": bucket_profiles,
    }


def _intent_local_hour(intent: Any) -> str:
    observation_ts = _parse_ts(getattr(intent, "metar_observation_time_utc", None)) or _parse_ts(
        getattr(intent, "captured_at", None)
    )
    if observation_ts is None:
        return "unknown"
    timezone_name = _normalize_text(getattr(intent, "settlement_timezone", ""))
    if timezone_name:
        try:
            return str(int(observation_ts.astimezone(ZoneInfo(timezone_name)).hour))
        except Exception:
            return str(int(observation_ts.hour))
    return str(int(observation_ts.hour))


def selection_quality_adjustment_for_intent(
    *,
    intent: Any,
    profile: dict[str, Any] | None,
    probability_penalty_max: float,
    expected_edge_penalty_max: float,
    score_adjust_scale: float,
) -> dict[str, Any]:
    safe_probability_penalty_max = max(0.0, float(probability_penalty_max))
    safe_expected_edge_penalty_max = max(0.0, float(expected_edge_penalty_max))
    safe_score_adjust_scale = max(0.0, float(score_adjust_scale))
    if not isinstance(profile, dict) or not bool(profile.get("enabled")):
        return {
            "status": "disabled",
            "penalty_ratio": 0.0,
            "boost_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "score_adjustment": 0.0,
            "sample_size": 0,
            "sources": [],
            "evidence_quality_raw": 0.0,
            "evidence_quality_scale": 0.0,
            "evidence_quality_support_ratio": 0.0,
            "evidence_quality_freshness_ratio": 0.0,
            "evidence_quality_weak_profile": False,
            "evidence_quality_penalty_floor_ratio": 0.0,
            "evidence_quality_penalty_cap_ratio": 0.0,
            "evidence_quality_boost_cap_ratio": 0.0,
            "evidence_quality_probability_raise_floor": 0.0,
            "evidence_quality_probability_raise_cap": 0.0,
            "evidence_quality_expected_edge_raise_floor": 0.0,
            "evidence_quality_expected_edge_raise_cap": 0.0,
            "evidence_quality_score_adjustment_cap": 0.0,
        }

    profile_status = _normalize_text(profile.get("status")).lower() or "unknown"
    if profile_status in {"no_bankroll_validation_artifact", "bankroll_validation_parse_failed"}:
        return {
            "status": profile_status,
            "penalty_ratio": 0.0,
            "boost_ratio": 0.0,
            "probability_raise": 0.0,
            "expected_edge_raise": 0.0,
            "score_adjustment": 0.0,
            "sample_size": 0,
            "sources": [],
            "evidence_quality_raw": 0.0,
            "evidence_quality_scale": 0.0,
            "evidence_quality_support_ratio": 0.0,
            "evidence_quality_freshness_ratio": 0.0,
            "evidence_quality_weak_profile": False,
            "evidence_quality_penalty_floor_ratio": 0.0,
            "evidence_quality_penalty_cap_ratio": 0.0,
            "evidence_quality_boost_cap_ratio": 0.0,
            "evidence_quality_probability_raise_floor": 0.0,
            "evidence_quality_probability_raise_cap": 0.0,
            "evidence_quality_expected_edge_raise_floor": 0.0,
            "evidence_quality_expected_edge_raise_cap": 0.0,
            "evidence_quality_score_adjustment_cap": 0.0,
        }

    bucket_profiles = profile.get("bucket_profiles") if isinstance(profile.get("bucket_profiles"), dict) else {}
    keys_by_dimension = {
        "station": _normalize_text(getattr(intent, "settlement_station", "")).upper(),
        "local_hour": _intent_local_hour(intent),
        "signal_type": _normalize_text(getattr(intent, "constraint_status", "")).lower(),
        "side": _normalize_text(getattr(intent, "side", "")).lower(),
    }
    weighted_penalty = 0.0
    weighted_boost = 0.0
    total_weight = 0.0
    samples: list[int] = []
    sources: list[str] = []
    for dimension, weight in _DIMENSION_WEIGHTS.items():
        key = _normalize_text(keys_by_dimension.get(dimension))
        if not key:
            continue
        dim_bucket = bucket_profiles.get(dimension)
        if not isinstance(dim_bucket, dict):
            continue
        entry = dim_bucket.get(key)
        if not isinstance(entry, dict):
            continue
        penalty_ratio = _clamp(float(_parse_float(entry.get("penalty_ratio")) or 0.0), 0.0, 1.0)
        boost_ratio = _clamp(float(_parse_float(entry.get("boost_ratio")) or 0.0), 0.0, 1.0)
        sample_count = int(_parse_float(entry.get("samples")) or 0)
        weighted_penalty += float(weight) * penalty_ratio
        weighted_boost += float(weight) * boost_ratio
        total_weight += float(weight)
        samples.append(sample_count)
        sources.append(f"{dimension}:{key}:{sample_count}")

    if total_weight > 0:
        bucket_penalty_ratio = _clamp(weighted_penalty / total_weight, 0.0, 1.0)
        bucket_boost_ratio = _clamp(weighted_boost / total_weight, 0.0, 1.0)
    else:
        bucket_penalty_ratio = 0.0
        bucket_boost_ratio = 0.0

    global_penalty_ratio = _clamp(float(_parse_float(profile.get("global_penalty_ratio")) or 0.0), 0.0, 1.0)
    global_boost_ratio = _clamp(float(_parse_float(profile.get("global_boost_ratio")) or 0.0), 0.0, 1.0)
    repeated_multiplier_penalty_ratio = _clamp(
        float(_parse_float(profile.get("repeated_entry_multiplier_penalty_ratio")) or 0.0),
        0.0,
        1.0,
    )
    fallback_profile_applied = bool(profile.get("fallback_profile_applied"))
    evidence_confidence = _clamp(float(_parse_float(profile.get("evidence_confidence")) or 0.0), 0.0, 1.0)
    source_age_hours = _parse_float(profile.get("source_age_hours"))
    max_profile_age_hours = max(
        1.0,
        float(_parse_float(profile.get("max_profile_age_hours")) or 96.0),
    )
    source_age_ratio = (
        _clamp(float(source_age_hours) / float(max_profile_age_hours), 0.0, 2.0)
        if isinstance(source_age_hours, float)
        else 0.0
    )
    global_adjustment_meta = (
        profile.get("global_adjustment_profile")
        if isinstance(profile.get("global_adjustment_profile"), dict)
        else {}
    )
    global_only_adjusted_share = _parse_float(global_adjustment_meta.get("global_only_adjusted_share"))
    global_only_target_share = _clamp(
        float(_parse_float(global_adjustment_meta.get("target_share")) or 0.10),
        0.01,
        0.95,
    )
    global_only_rows_adjusted = int(_parse_float(global_adjustment_meta.get("rows_adjusted")) or 0)
    global_only_min_rows_for_pressure = max(
        1,
        int(_parse_float(global_adjustment_meta.get("min_rows_for_pressure")) or 100),
    )
    global_only_pressure_active = bool(global_adjustment_meta.get("pressure_active"))
    if not global_only_pressure_active:
        global_only_pressure_active = bool(
            isinstance(global_only_adjusted_share, float)
            and global_only_rows_adjusted >= global_only_min_rows_for_pressure
            and global_only_adjusted_share >= global_only_target_share
        )
    global_only_excess_ratio = (
        _clamp(
            (float(global_only_adjusted_share) - float(global_only_target_share))
            / max(0.01, (0.50 - float(global_only_target_share))),
            0.0,
            1.0,
        )
        if isinstance(global_only_adjusted_share, float)
        else 0.0
    )

    resolved_support_required = max(
        1,
        int(_parse_float(profile.get("min_resolved_market_sides_required")) or 0),
    )
    resolved_support = max(0, int(_parse_float(profile.get("resolved_unique_market_sides")) or 0))
    resolved_support_ratio = _clamp(
        float(resolved_support) / float(resolved_support_required),
        0.0,
        2.0,
    )
    resolved_support_quality = _clamp(float(resolved_support_ratio), 0.0, 1.0)
    freshness_quality = _clamp(1.0 - float(source_age_ratio), 0.0, 1.0)
    weak_evidence_profile = bool(
        fallback_profile_applied
        or profile_status in {"stale_profile", "insufficient_resolved_market_sides"}
        or resolved_support < resolved_support_required
        or (isinstance(source_age_hours, float) and source_age_ratio >= 1.0)
    )
    evidence_quality_raw = _clamp(
        (0.52 * float(evidence_confidence))
        + (0.28 * float(resolved_support_quality))
        + (0.20 * float(freshness_quality)),
        0.0,
        1.0,
    )
    evidence_quality_scale = _clamp(0.35 + (0.65 * float(evidence_quality_raw)), 0.35, 1.0)
    evidence_quality_penalty_floor_ratio = 0.0
    evidence_quality_penalty_cap_ratio = 1.0
    evidence_quality_boost_cap_ratio = 1.0
    evidence_quality_score_adjustment_cap = 1.0
    if weak_evidence_profile:
        evidence_quality_penalty_floor_ratio = _clamp(
            0.03 + (0.05 * float(evidence_quality_scale)),
            0.03,
            0.08,
        )
        evidence_quality_penalty_cap_ratio = _clamp(
            0.16 + (0.16 * float(evidence_quality_scale)),
            0.18,
            0.36,
        )
        evidence_quality_boost_cap_ratio = _clamp(
            0.10 + (0.08 * float(evidence_quality_scale)),
            0.10,
            0.22,
        )
        evidence_quality_score_adjustment_cap = _clamp(
            0.05 + (0.04 * float(evidence_quality_scale)),
            0.05,
            0.09,
        )

    global_penalty_weight = 0.28 + (0.18 * repeated_multiplier_penalty_ratio)
    if fallback_profile_applied:
        global_penalty_weight += 0.06
    if source_age_ratio >= 0.75:
        global_penalty_weight += 0.06
    global_penalty_weight = _clamp(global_penalty_weight, 0.20, 0.70)

    global_boost_weight = 0.16 * (1.0 - repeated_multiplier_penalty_ratio)
    if fallback_profile_applied:
        global_boost_weight *= 0.70
    if evidence_confidence < 0.60:
        global_boost_weight *= 0.70
    if source_age_ratio >= 0.75:
        global_boost_weight *= 0.65
    global_boost_weight = _clamp(global_boost_weight, 0.0, 0.25)

    final_penalty_ratio = _clamp(
        (global_penalty_weight * global_penalty_ratio) + (0.80 * bucket_penalty_ratio),
        0.0,
        1.0,
    )
    final_boost_ratio = _clamp(
        (global_boost_weight * global_boost_ratio) + (0.90 * bucket_boost_ratio),
        0.0,
        1.0,
    )
    if not sources and fallback_profile_applied and repeated_multiplier_penalty_ratio > 0.20:
        global_only_penalty_cap = _clamp(
            0.03 + (0.10 * (1.0 - evidence_confidence)),
            0.03,
            0.16,
        )
        if global_only_pressure_active:
            global_only_penalty_cap = max(
                float(global_only_penalty_cap),
                _clamp(
                    0.12 + (0.22 * float(global_only_excess_ratio)),
                    0.12,
                    0.40,
                ),
            )
        final_penalty_ratio = min(float(final_penalty_ratio), float(global_only_penalty_cap))
        final_boost_ratio = min(float(final_boost_ratio), 0.10)
    if global_only_pressure_active and isinstance(global_only_adjusted_share, float):
        final_penalty_ratio = _clamp(
            float(final_penalty_ratio) + (0.22 * float(global_only_excess_ratio)),
            0.0,
            1.0,
        )
        final_boost_ratio = _clamp(
            float(final_boost_ratio) * (1.0 - (0.70 * float(global_only_excess_ratio))),
            0.0,
            1.0,
        )
        sources.append(
            f"global_only_share:{float(global_only_adjusted_share):.3f}:{int(global_only_rows_adjusted)}"
        )
    if weak_evidence_profile:
        final_penalty_ratio = _clamp(
            float(final_penalty_ratio) * float(evidence_quality_scale),
            float(evidence_quality_penalty_floor_ratio),
            float(evidence_quality_penalty_cap_ratio),
        )
        final_boost_ratio = _clamp(
            float(final_boost_ratio) * (0.70 + (0.30 * float(evidence_quality_scale))),
            0.0,
            float(evidence_quality_boost_cap_ratio),
        )
    probability_raise = safe_probability_penalty_max * final_penalty_ratio
    expected_edge_raise = safe_expected_edge_penalty_max * final_penalty_ratio
    score_adjustment = safe_score_adjust_scale * (final_boost_ratio - final_penalty_ratio)
    if weak_evidence_profile:
        score_adjustment = _clamp(
            float(score_adjustment),
            -float(evidence_quality_score_adjustment_cap),
            float(evidence_quality_score_adjustment_cap),
        )

    return {
        "status": profile_status,
        "penalty_ratio": round(float(final_penalty_ratio), 6),
        "boost_ratio": round(float(final_boost_ratio), 6),
        "probability_raise": round(float(probability_raise), 6),
        "expected_edge_raise": round(float(expected_edge_raise), 6),
        "score_adjustment": round(float(score_adjustment), 6),
        "sample_size": max(samples) if samples else 0,
        "global_only_pressure_active": bool(global_only_pressure_active),
        "global_only_adjusted_share": (
            round(float(global_only_adjusted_share), 6)
            if isinstance(global_only_adjusted_share, float)
            else None
        ),
        "global_only_excess_ratio": round(float(global_only_excess_ratio), 6),
        "evidence_quality_raw": round(float(evidence_quality_raw), 6),
        "evidence_quality_scale": round(float(evidence_quality_scale), 6),
        "evidence_quality_support_ratio": round(float(resolved_support_ratio), 6),
        "evidence_quality_freshness_ratio": round(float(freshness_quality), 6),
        "evidence_quality_weak_profile": bool(weak_evidence_profile),
        "evidence_quality_penalty_floor_ratio": round(float(evidence_quality_penalty_floor_ratio), 6),
        "evidence_quality_penalty_cap_ratio": round(float(evidence_quality_penalty_cap_ratio), 6),
        "evidence_quality_boost_cap_ratio": round(float(evidence_quality_boost_cap_ratio), 6),
        "evidence_quality_probability_raise_floor": round(
            float(safe_probability_penalty_max * evidence_quality_penalty_floor_ratio),
            6,
        ),
        "evidence_quality_probability_raise_cap": round(
            float(safe_probability_penalty_max * evidence_quality_penalty_cap_ratio),
            6,
        ),
        "evidence_quality_expected_edge_raise_floor": round(
            float(safe_expected_edge_penalty_max * evidence_quality_penalty_floor_ratio),
            6,
        ),
        "evidence_quality_expected_edge_raise_cap": round(
            float(safe_expected_edge_penalty_max * evidence_quality_penalty_cap_ratio),
            6,
        ),
        "evidence_quality_score_adjustment_cap": round(float(evidence_quality_score_adjustment_cap), 6),
        "sources": sources,
    }


def _parse_bool_text(value: Any) -> bool | None:
    text = _normalize_text(value).lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def _artifact_timestamp_token(path: Path) -> str:
    stem = _normalize_text(path.stem)
    if not stem:
        return ""
    parts = stem.split("_")
    if len(parts) >= 2 and len(parts[-1]) == 6 and len(parts[-2]) == 8:
        return f"{parts[-2]}_{parts[-1]}"
    return ""


def _artifact_epoch(path: Path) -> float:
    token = _artifact_timestamp_token(path)
    if token:
        try:
            parsed = datetime.strptime(token, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            pass
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _iter_recent_intent_rows(
    *,
    output_dir: Path,
    now_utc: datetime,
    lookback_hours: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    window_seconds = max(0.0, float(lookback_hours)) * 3600.0
    window_start_epoch = now_utc.timestamp() - window_seconds
    candidates = sorted(
        output_dir.glob("kalshi_temperature_trade_intents_*.csv"),
        key=_artifact_epoch,
        reverse=True,
    )
    rows: list[dict[str, Any]] = []
    source_files: list[str] = []
    for path in candidates:
        artifact_epoch = _artifact_epoch(path)
        if artifact_epoch < window_start_epoch:
            continue
        source_files.append(str(path))
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not isinstance(row, dict):
                        continue
                    captured_at = (
                        _parse_ts(row.get("captured_at_utc"))
                        or _parse_ts(row.get("captured_at"))
                        or datetime.fromtimestamp(artifact_epoch, tz=timezone.utc)
                    )
                    if captured_at.timestamp() < window_start_epoch:
                        continue
                    rows.append(
                        {
                            "settlement_station": _normalize_text(row.get("settlement_station")).upper(),
                            "settlement_timezone": _normalize_text(row.get("settlement_timezone")),
                            "constraint_status": _normalize_text(row.get("constraint_status")).lower(),
                            "side": _normalize_text(row.get("side")).lower(),
                            "metar_observation_time_utc": _normalize_text(
                                row.get("metar_observation_time_utc")
                            ),
                            "captured_at": captured_at.isoformat(),
                            "policy_approved": _parse_bool_text(row.get("policy_approved")),
                        }
                    )
        except OSError:
            continue
    return rows, source_files


def run_kalshi_temperature_selection_quality(
    *,
    output_dir: str,
    lookback_hours: float = 14.0 * 24.0,
    min_resolved_market_sides: int = 12,
    min_bucket_samples: int = 4,
    preferred_attribution_model: str = "fixed_fraction_per_underlying_family",
    max_profile_age_hours: float = 96.0,
    probability_penalty_max: float = 0.05,
    expected_edge_penalty_max: float = 0.006,
    score_adjust_scale: float = 0.35,
    intent_hours: float = 24.0,
    top_n: int = 10,
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    top_limit = max(1, int(top_n))
    safe_lookback_hours = max(1.0, float(lookback_hours))
    safe_intent_hours = max(0.0, float(intent_hours))
    profile = load_temperature_selection_quality_profile(
        output_dir=str(out_dir),
        now_utc=now_utc,
        enabled=True,
        lookback_hours=safe_lookback_hours,
        min_resolved_market_sides=max(1, int(min_resolved_market_sides)),
        min_bucket_samples=max(1, int(min_bucket_samples)),
        preferred_attribution_model=_normalize_text(preferred_attribution_model)
        or "fixed_fraction_per_underlying_family",
        max_profile_age_hours=max(0.0, float(max_profile_age_hours)),
    )

    intents_rows, intents_source_files = _iter_recent_intent_rows(
        output_dir=out_dir,
        now_utc=now_utc,
        lookback_hours=safe_intent_hours,
    )
    adjusted_rows = 0
    approved_rows = 0
    approved_adjusted_rows = 0
    adjusted_rows_bucket_backed = 0
    adjusted_rows_global_only = 0
    approved_adjusted_rows_bucket_backed = 0
    approved_adjusted_rows_global_only = 0
    penalty_values: list[float] = []
    probability_raise_values: list[float] = []
    edge_raise_values: list[float] = []
    score_adjust_values: list[float] = []
    sample_sizes: list[int] = []
    source_counter: dict[str, int] = {}

    for row in intents_rows:
        adjustment = selection_quality_adjustment_for_intent(
            intent=type("IntentProxy", (), row),
            profile=profile,
            probability_penalty_max=max(0.0, float(probability_penalty_max)),
            expected_edge_penalty_max=max(0.0, float(expected_edge_penalty_max)),
            score_adjust_scale=max(0.0, float(score_adjust_scale)),
        )
        approved = row.get("policy_approved") is True
        if approved:
            approved_rows += 1
        penalty_ratio = float(_parse_float(adjustment.get("penalty_ratio")) or 0.0)
        prob_raise = float(_parse_float(adjustment.get("probability_raise")) or 0.0)
        edge_raise = float(_parse_float(adjustment.get("expected_edge_raise")) or 0.0)
        score_adj = float(_parse_float(adjustment.get("score_adjustment")) or 0.0)
        sample_size = int(_parse_float(adjustment.get("sample_size")) or 0)
        source_items = (
            adjustment.get("sources") if isinstance(adjustment.get("sources"), list) else []
        )
        has_bucket_sources = any(_normalize_text(item) for item in source_items)
        is_adjusted = bool(
            abs(prob_raise) > 1e-12 or abs(edge_raise) > 1e-12 or abs(score_adj) > 1e-12
        )
        if is_adjusted:
            adjusted_rows += 1
            if has_bucket_sources:
                adjusted_rows_bucket_backed += 1
            else:
                adjusted_rows_global_only += 1
            if approved:
                approved_adjusted_rows += 1
                if has_bucket_sources:
                    approved_adjusted_rows_bucket_backed += 1
                else:
                    approved_adjusted_rows_global_only += 1
        penalty_values.append(penalty_ratio)
        probability_raise_values.append(prob_raise)
        edge_raise_values.append(edge_raise)
        score_adjust_values.append(score_adj)
        sample_sizes.append(sample_size)
        for source_item in source_items:
            source_key = _normalize_text(source_item)
            if source_key:
                source_counter[source_key] = source_counter.get(source_key, 0) + 1

    def _safe_avg(values: list[float]) -> float | None:
        if not values:
            return None
        return float(sum(values) / float(len(values)))

    top_sources = sorted(
        (
            {"source": key, "count": int(value)}
            for key, value in source_counter.items()
        ),
        key=lambda item: int(item["count"]),
        reverse=True,
    )[:top_limit]

    payload = {
        "status": "ready",
        "captured_at": now_utc.isoformat(),
        "output_dir": str(out_dir),
        "profile": profile,
        "inputs": {
            "lookback_hours": round(float(safe_lookback_hours), 3),
            "intent_hours": round(float(safe_intent_hours), 3),
            "min_resolved_market_sides": int(max(1, int(min_resolved_market_sides))),
            "min_bucket_samples": int(max(1, int(min_bucket_samples))),
            "preferred_attribution_model": _normalize_text(preferred_attribution_model)
            or "fixed_fraction_per_underlying_family",
            "max_profile_age_hours": round(max(0.0, float(max_profile_age_hours)), 3),
            "probability_penalty_max": round(max(0.0, float(probability_penalty_max)), 6),
            "expected_edge_penalty_max": round(max(0.0, float(expected_edge_penalty_max)), 6),
            "score_adjust_scale": round(max(0.0, float(score_adjust_scale)), 6),
            "top_n": int(top_limit),
        },
        "intent_window": {
            "files_count": int(len(intents_source_files)),
            "files": intents_source_files[:top_limit],
            "rows_total": int(len(intents_rows)),
            "rows_approved": int(approved_rows),
            "rows_adjusted": int(adjusted_rows),
            "rows_approved_adjusted": int(approved_adjusted_rows),
            "rows_adjusted_bucket_backed": int(adjusted_rows_bucket_backed),
            "rows_adjusted_global_only": int(adjusted_rows_global_only),
            "rows_approved_adjusted_bucket_backed": int(approved_adjusted_rows_bucket_backed),
            "rows_approved_adjusted_global_only": int(approved_adjusted_rows_global_only),
            "adjusted_rate": round((adjusted_rows / len(intents_rows)), 6) if intents_rows else 0.0,
            "approved_adjusted_rate": round((approved_adjusted_rows / approved_rows), 6)
            if approved_rows > 0
            else 0.0,
            "adjusted_bucket_backed_rate": (
                round((adjusted_rows_bucket_backed / adjusted_rows), 6)
                if adjusted_rows > 0
                else 0.0
            ),
            "approved_adjusted_bucket_backed_rate": (
                round((approved_adjusted_rows_bucket_backed / approved_adjusted_rows), 6)
                if approved_adjusted_rows > 0
                else 0.0
            ),
            "penalty_ratio_avg": round(float(_safe_avg(penalty_values) or 0.0), 6),
            "penalty_ratio_max": round(float(max(penalty_values) if penalty_values else 0.0), 6),
            "probability_raise_avg": round(float(_safe_avg(probability_raise_values) or 0.0), 6),
            "probability_raise_max": round(
                float(max(probability_raise_values) if probability_raise_values else 0.0),
                6,
            ),
            "expected_edge_raise_avg": round(float(_safe_avg(edge_raise_values) or 0.0), 6),
            "expected_edge_raise_max": round(
                float(max(edge_raise_values) if edge_raise_values else 0.0),
                6,
            ),
            "score_adjustment_avg": round(float(_safe_avg(score_adjust_values) or 0.0), 6),
            "score_adjustment_min": round(float(min(score_adjust_values) if score_adjust_values else 0.0), 6),
            "score_adjustment_max": round(float(max(score_adjust_values) if score_adjust_values else 0.0), 6),
            "sample_size_avg": round(float(_safe_avg([float(v) for v in sample_sizes]) or 0.0), 6),
            "sample_size_max": int(max(sample_sizes) if sample_sizes else 0),
            "top_sources": top_sources,
        },
    }

    timestamp = now_utc.strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"kalshi_temperature_selection_quality_{timestamp}.json"
    latest_path = out_dir / "kalshi_temperature_selection_quality_latest.json"
    text = json.dumps(payload, indent=2, sort_keys=True)
    tmp_output = output_path.with_name(f".{output_path.name}.tmp-{os.getpid()}")
    tmp_latest = latest_path.with_name(f".{latest_path.name}.tmp-{os.getpid()}")
    try:
        tmp_output.write_text(text, encoding="utf-8")
        tmp_latest.write_text(text, encoding="utf-8")
        os.replace(tmp_output, output_path)
        os.replace(tmp_latest, latest_path)
    finally:
        for path in (tmp_output, tmp_latest):
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                pass
    payload["output_file"] = str(output_path)
    return payload
