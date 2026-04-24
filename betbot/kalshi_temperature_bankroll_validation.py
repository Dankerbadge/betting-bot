from __future__ import annotations

from bisect import bisect_left, bisect_right
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import json
from pathlib import Path
import re
from time import perf_counter
from typing import Any


_REQUIRED_SIZING_MODELS = (
    "fixed_fraction_per_unique_market_side",
    "fixed_fraction_per_underlying_family",
    "capped_fractional_kelly_shadow",
    "fixed_unit_risk_budget",
)

_REQUIRED_MODEL_FIELDS = (
    "risk_per_trade_pct",
    "max_pct_per_underlying_family",
    "max_pct_per_city_day",
    "max_pct_total_deployed",
    "max_simultaneous_market_sides",
    "max_same_station_exposure_pct",
    "max_same_hour_cluster_pct",
)

_EDGE_FIELD_CANDIDATES = (
    "maker_entry_edge_conservative_net_total",
    "maker_entry_edge_net_total",
    "maker_entry_edge_conservative_net_fees",
    "maker_entry_edge_net_fees",
    "maker_entry_edge_conservative",
    "maker_entry_edge",
)

_COST_FIELD_CANDIDATES = ("estimated_entry_cost_dollars", "cost_dollars")
_POLICY_VERSION_CANDIDATES = (
    "temperature_policy_version",
    "policy_version",
    "temperature_model_version",
    "model_version",
)
_TEMPERATURE_SOURCE_STRATEGY = "temperature_constraints"
_TEMPERATURE_TICKER_PREFIXES = (
    "KXHIGH",
    "KXLOW",
    "KXTEMP",
    "NHIGH",
    "NLOW",
    "NTEMP",
)
_TEMPERATURE_TICKER_DATE_TOKEN_RE = re.compile(r"-\d{2}[A-Z]{3}\d{2,4}(?:-|$)")

_DEFAULT_READINESS_HORIZONS = (
    ("1d", 24.0),
    ("7d", 7.0 * 24.0),
    ("14d", 14.0 * 24.0),
    ("21d", 21.0 * 24.0),
    ("28d", 28.0 * 24.0),
    ("3mo", 90.0 * 24.0),
    ("6mo", 180.0 * 24.0),
    ("1yr", 365.0 * 24.0),
)

_DEPLOYMENT_HEADLINE_MODEL = "fixed_fraction_per_underlying_family"
_DEPLOYMENT_HEADLINE_LAYER = "underlying_family_aggregated"
_STAMP_SUFFIX_RE = re.compile(r"(?:^|_)(\d{8}_\d{6})$")

_READINESS_BLOCKER_DETAILS = {
    "non_positive_roi_on_reference_bankroll": "Projected ROI on full bankroll is not positive.",
    "does_not_exceed_hysa_for_window": "Projected return does not exceed HYSA baseline for the same window.",
    "insufficient_history_coverage_for_horizon": "Observed data coverage is too short for this horizon label.",
    "insufficient_independent_market_side_breadth": "Too few independent unique market-side outcomes resolved.",
    "insufficient_underlying_family_breadth": "Too few unique underlying families resolved.",
    "insufficient_settled_trade_count": "Not enough settled trade samples for decision-grade confidence.",
    "insufficient_peak_bankroll_utilization": "Peak bankroll deployment is too low to be meaningful.",
    "insufficient_average_bankroll_utilization": "Average bankroll deployment is too low to be meaningful.",
    "repeated_entry_multiplier_too_high": "Too much repeated entry concentration versus independent outcomes.",
    "concentration_warning": "Outcomes are concentrated into too few market-side buckets.",
    "stale_suppression": "Freshness constraints are dominating the current window.",
    "no_resolved_outcomes": "No settled outcomes are available yet for deployability assessment.",
    "non_positive_calibration_ratio": "Shadow-settled calibration ratio is non-positive.",
    "pipeline_data_stale_or_missing": "Core weather pipeline artifacts are stale or missing in the requested window.",
}

_READINESS_BLOCKER_ACTIONS = {
    "non_positive_roi_on_reference_bankroll": "Improve selection quality or risk sizing before live deployment.",
    "does_not_exceed_hysa_for_window": "Increase independent edge or reduce friction/slippage impact.",
    "insufficient_history_coverage_for_horizon": "Run longer unchanged soak to build settlement-aged evidence for this horizon.",
    "insufficient_independent_market_side_breadth": "Expand scanning breadth across more independent city/day opportunities.",
    "insufficient_underlying_family_breadth": "Add broader family-level coverage and selection diversity.",
    "insufficient_settled_trade_count": "Continue soak until more shadow picks age into settlement.",
    "insufficient_peak_bankroll_utilization": "Increase actionable breadth to create deployable opportunities.",
    "insufficient_average_bankroll_utilization": "Improve consistency of qualified entries across time buckets.",
    "repeated_entry_multiplier_too_high": "Reduce repeated entries on same market-side outcomes.",
    "concentration_warning": "Prioritize deconcentration across stations/hours/families.",
    "stale_suppression": "Restore fresher artifacts and re-run once the window is no longer freshness-dominated.",
    "no_resolved_outcomes": "Wait for settled outcomes to age in before treating the lane as deployable.",
    "non_positive_calibration_ratio": "Recalibrate expected edge assumptions against settled outcomes.",
    "pipeline_data_stale_or_missing": "Restore shadow/metar/settlement loops and verify fresh artifact heartbeat before trusting readiness.",
}

_PIPELINE_HEALTH_FEEDS: tuple[dict[str, Any], ...] = (
    {
        "name": "shadow_watch_summary",
        "pattern": "kalshi_temperature_shadow_watch_summary_*.json",
        "stale_warn_seconds": 900.0,
    },
    {
        "name": "trade_intents_summary",
        "pattern": "kalshi_temperature_trade_intents_summary_*.json",
        "stale_warn_seconds": 900.0,
    },
    {
        "name": "metar_summary",
        "pattern": "kalshi_temperature_metar_summary_*.json",
        "stale_warn_seconds": 600.0,
    },
    {
        "name": "settlement_state",
        "pattern": "kalshi_temperature_settlement_state_*.json",
        "stale_warn_seconds": 1800.0,
    },
)


@dataclass
class TemperatureOpportunity:
    row_id: str
    planned_at: datetime
    close_time: datetime
    intent_id: str
    shadow_order_id: str
    market_ticker: str
    side: str
    market_side_key: str
    underlying_key: str
    underlying_family_key: str
    city_day_key: str
    settlement_station: str
    local_hour_key: str
    signal_type: str
    policy_reason: str
    contracts: float
    entry_price_dollars: float
    expected_edge_dollars: float
    estimated_entry_cost_dollars: float
    resolved: bool
    win: bool | None
    push: bool
    outcome: str
    base_pnl_dollars: float
    threshold_expression: str
    final_truth_value: float | None
    resolution_reason: str
    alpha_strength: float | None = None
    yes_possible_gap: float | None = None
    yes_possible_overlap: bool | None = None
    primary_signal_margin: float | None = None
    forecast_feasibility_margin: float | None = None
    speci_shock_active: bool | None = None
    speci_shock_confidence: float | None = None
    speci_shock_weight: float | None = None
    speci_shock_mode: str | None = None
    speci_shock_trigger_count: int | None = None
    speci_shock_cooldown_blocked: bool | None = None
    speci_shock_improvement_hold_active: bool | None = None
    policy_version: str = ""


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (TypeError, ValueError):
        return None


def _parse_optional_bool(value: Any) -> bool | None:
    text = _normalize_text(value).lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
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


def _artifact_timestamp_utc(path: Path) -> datetime | None:
    match = _STAMP_SUFFIX_RE.search(path.stem)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _artifact_epoch(path: Path, *, fallback_mtime: float | None = None) -> float:
    inferred = _artifact_timestamp_utc(path)
    if isinstance(inferred, datetime):
        return inferred.timestamp()
    if isinstance(fallback_mtime, float):
        return fallback_mtime
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _observed_span_hours(rows: list[dict[str, Any]]) -> float:
    planned_at_values: list[datetime] = []
    for row in rows:
        planned_at = row.get("planned_at")
        if isinstance(planned_at, datetime):
            planned_at_values.append(planned_at)
    if not planned_at_values:
        return 0.0
    # Deduplicate and sort by epoch to keep behavior stable for any timezone-aware
    # datetime values and avoid relying on arbitrary object ordering.
    unique_planned_at_values = sorted(
        {planned_at.timestamp(): planned_at for planned_at in planned_at_values}.values(),
        key=lambda value: value.timestamp(),
    )
    if len(unique_planned_at_values) == 1:
        return 1.0
    span_seconds = (unique_planned_at_values[-1] - unique_planned_at_values[0]).total_seconds()
    if span_seconds <= 0:
        return 1.0
    return max(1.0, span_seconds / 3600.0)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_row_metric(row: dict[str, str], candidates: tuple[str, ...]) -> float:
    for key in candidates:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return parsed
    return 0.0


def _extract_first_text(row: dict[str, str], candidates: tuple[str, ...]) -> str:
    for key in candidates:
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _is_temperature_ticker(ticker: Any) -> bool:
    normalized = _normalize_text(ticker).upper()
    if not normalized.startswith(_TEMPERATURE_TICKER_PREFIXES):
        return False
    # Require a daily/hourly date token to avoid non-weather macro series that
    # share HIGH/LOW prefixes (for example inflation contracts).
    return bool(_TEMPERATURE_TICKER_DATE_TOKEN_RE.search(normalized))


def _is_temperature_plan_row(row: dict[str, str]) -> bool:
    ticker_is_temperature = _is_temperature_ticker(row.get("market_ticker"))
    strategy = _normalize_text(row.get("source_strategy")).lower()
    if strategy:
        if strategy != _TEMPERATURE_SOURCE_STRATEGY:
            return False
        # Guard against mislabeled strategy rows leaking non-temperature
        # contracts into temperature bankroll validation.
        if ticker_is_temperature:
            return True
        client_order_id = _normalize_text(row.get("temperature_client_order_id") or row.get("client_order_id"))
        return not _normalize_text(row.get("market_ticker")) and client_order_id.startswith("temp-")
    client_order_id = _normalize_text(row.get("temperature_client_order_id") or row.get("client_order_id"))
    if client_order_id.startswith("temp-"):
        return ticker_is_temperature
    return ticker_is_temperature


def _files_in_window(out_dir: Path, pattern: str, start_epoch: float, end_epoch: float) -> list[Path]:
    rows: list[tuple[float, Path]] = []
    end_epoch_inclusive = float(end_epoch) + 1.0
    for path in out_dir.glob(pattern):
        try:
            mtime = float(path.stat().st_mtime)
        except OSError:
            continue
        inferred_timestamp = _artifact_timestamp_utc(path)
        artifact_epoch = _artifact_epoch(path, fallback_mtime=mtime)
        # For stamped artifacts, trust the embedded timestamp to avoid treating
        # copied/synced historical files as fresh just because mtime changed.
        # For unstamped files, fall back to mtime.
        if isinstance(inferred_timestamp, datetime):
            in_window = float(start_epoch) <= artifact_epoch < end_epoch_inclusive
        else:
            in_window = float(start_epoch) <= mtime < end_epoch_inclusive
        if in_window:
            rows.append((artifact_epoch, path))
    # Deterministic tie-break so same-stamp artifacts resolve predictably.
    rows.sort(key=lambda item: (item[0], str(item[1])))
    return [path for _, path in rows]


@lru_cache(maxsize=2048)
def _cached_files_in_window(
    out_dir_text: str,
    pattern: str,
    start_epoch: float,
    end_epoch: float,
) -> tuple[str, ...]:
    out_dir = Path(out_dir_text)
    return tuple(
        str(path)
        for path in _files_in_window(
            out_dir,
            pattern,
            float(start_epoch),
            float(end_epoch),
        )
    )


def _files_in_window_cached(out_dir: Path, pattern: str, start_epoch: float, end_epoch: float) -> list[Path]:
    out_dir_text = str(out_dir.resolve())
    return [
        Path(path_text)
        for path_text in _cached_files_in_window(
            out_dir_text,
            pattern,
            float(start_epoch),
            float(end_epoch),
        )
    ]


def _file_signature(path: Path) -> tuple[str, int, int]:
    resolved = str(path.resolve())
    try:
        stat = path.stat()
        return resolved, int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))), int(stat.st_size)
    except OSError:
        return resolved, 0, 0


def _latest_file_at_or_before(out_dir: Path, pattern: str, end_epoch: float) -> Path | None:
    candidates = _files_in_window_cached(out_dir, pattern, 0.0, end_epoch)
    if not candidates:
        return None
    return candidates[-1]


def _latest_file_preferring_window(
    out_dir: Path,
    pattern: str,
    *,
    start_epoch: float,
    end_epoch: float,
) -> tuple[Path | None, bool]:
    in_window_files = _files_in_window_cached(out_dir, pattern, start_epoch, end_epoch)
    if in_window_files:
        return in_window_files[-1], True
    fallback = _latest_file_at_or_before(out_dir, pattern, end_epoch)
    return fallback, False


def _artifact_age_seconds(path: Path | None, *, now_epoch: float) -> float | None:
    if path is None:
        return None
    try:
        mtime = float(path.stat().st_mtime)
    except OSError:
        mtime = None
    epoch = _artifact_epoch(path, fallback_mtime=mtime)
    age = now_epoch - epoch
    if age < 0:
        age = 0.0
    return float(age)


def _artifact_in_window(path: Path | None, *, start_epoch: float, end_epoch: float) -> bool:
    if path is None:
        return False
    try:
        mtime = float(path.stat().st_mtime)
    except OSError:
        mtime = None
    epoch = _artifact_epoch(path, fallback_mtime=mtime)
    return bool(float(start_epoch) <= epoch < (float(end_epoch) + 1.0))


def _artifact_span_hours(paths: list[Path]) -> float:
    if not paths:
        return 0.0
    epochs = sorted({_artifact_epoch(path) for path in paths})
    if not epochs:
        return 0.0
    if len(epochs) == 1:
        return 1.0
    span_seconds = float(epochs[-1] - epochs[0])
    if span_seconds <= 0:
        return 1.0
    return max(1.0, span_seconds / 3600.0)


def _build_pipeline_health_snapshot(
    *,
    out_dir: Path,
    start_epoch: float,
    end_epoch: float,
    lookback_hours: float,
    now_epoch: float,
) -> dict[str, Any]:
    feed_rows: dict[str, dict[str, Any]] = {}
    missing_feeds: list[str] = []
    stale_feeds: list[str] = []
    out_of_window_feeds: list[str] = []
    coverage_values: list[float] = []

    for spec in _PIPELINE_HEALTH_FEEDS:
        name = _normalize_text(spec.get("name"))
        pattern = _normalize_text(spec.get("pattern"))
        stale_warn_seconds = float(_parse_float(spec.get("stale_warn_seconds")) or 900.0)
        if not name or not pattern:
            continue

        in_window_files = _files_in_window_cached(out_dir, pattern, start_epoch, end_epoch)
        latest_file, latest_in_window = _latest_file_preferring_window(
            out_dir,
            pattern,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
        )
        age_seconds = _artifact_age_seconds(latest_file, now_epoch=now_epoch)
        in_window_span_hours = _artifact_span_hours(in_window_files)
        in_window_count = len(in_window_files)
        in_window_coverage_ratio = (
            min(1.0, in_window_span_hours / lookback_hours) if lookback_hours > 0 else 0.0
        )
        coverage_values.append(in_window_coverage_ratio)

        stale_now = bool(isinstance(age_seconds, float) and age_seconds > stale_warn_seconds)
        if latest_file is None:
            status = "missing"
            missing_feeds.append(name)
        elif stale_now:
            status = "stale"
            stale_feeds.append(name)
        elif latest_in_window:
            status = "fresh"
        else:
            status = "out_of_window"
            out_of_window_feeds.append(name)

        feed_rows[name] = {
            "file_used": str(latest_file) if latest_file else "",
            "available": bool(latest_file),
            "in_window": bool(latest_in_window),
            "in_window_file_count": in_window_count,
            "in_window_span_hours": round(in_window_span_hours, 6),
            "in_window_coverage_ratio": round(in_window_coverage_ratio, 6),
            "age_seconds": round(age_seconds, 3) if isinstance(age_seconds, float) else None,
            "stale_warn_seconds": round(stale_warn_seconds, 3),
            "stale_now": stale_now,
            "status": status,
        }

    if missing_feeds or stale_feeds:
        status = "red"
        reason = "missing_or_stale_core_feeds"
    elif out_of_window_feeds:
        status = "yellow"
        reason = "out_of_window_feed_history"
    else:
        status = "green"
        reason = "fresh_core_feeds"

    min_coverage_ratio = min(coverage_values) if coverage_values else 0.0
    avg_coverage_ratio = _average(coverage_values) or 0.0
    return {
        "status": status,
        "reason": reason,
        "missing_feeds": sorted(missing_feeds),
        "stale_feeds": sorted(stale_feeds),
        "out_of_window_feeds": sorted(out_of_window_feeds),
        "min_in_window_coverage_ratio": round(min_coverage_ratio, 6),
        "avg_in_window_coverage_ratio": round(avg_coverage_ratio, 6),
        "feeds": feed_rows,
    }


def _fraction(value: Any, default: float) -> float:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        parsed = float(default)
    if parsed > 1.0:
        parsed = parsed / 100.0
    if parsed < 0.0:
        return 0.0
    if parsed > 1.0:
        return 1.0
    return parsed


def _safe_positive(value: Any, default: float) -> float:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return float(default)
    return max(0.0, parsed)


def _safe_non_negative_int(value: Any, default: int) -> int:
    parsed = _parse_int(value)
    if not isinstance(parsed, int):
        return int(default)
    return max(0, parsed)


def _load_json_input(value: str | None) -> dict[str, Any]:
    text = _normalize_text(value)
    if not text:
        return {}
    as_path = Path(text)
    try:
        if as_path.exists() and as_path.is_file():
            return _load_json(as_path)
    except OSError:
        pass
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _default_sizing_models(reference_bankroll_dollars: float) -> dict[str, dict[str, Any]]:
    base = {
        "risk_per_trade_pct": 0.02,
        "max_pct_per_underlying_family": 0.15,
        "max_pct_per_city_day": 0.20,
        "max_pct_total_deployed": 0.50,
        "max_simultaneous_market_sides": 10,
        "max_same_station_exposure_pct": 0.25,
        "max_same_hour_cluster_pct": 0.30,
    }
    return {
        "fixed_fraction_per_unique_market_side": dict(base),
        "fixed_fraction_per_underlying_family": dict(base),
        "capped_fractional_kelly_shadow": {
            **base,
            "kelly_fraction_multiplier": 0.5,
        },
        "fixed_unit_risk_budget": {
            **base,
            "unit_risk_dollars": round(reference_bankroll_dollars * base["risk_per_trade_pct"], 6),
        },
    }


def _resolve_sizing_models(
    *,
    sizing_models_json: str | None,
    reference_bankroll_dollars: float,
) -> dict[str, dict[str, Any]]:
    payload = _load_json_input(sizing_models_json)
    defaults = _default_sizing_models(reference_bankroll_dollars)
    models: dict[str, dict[str, Any]] = {}
    for model_name in _REQUIRED_SIZING_MODELS:
        raw = payload.get(model_name) if isinstance(payload, dict) else None
        base = dict(defaults[model_name])
        if isinstance(raw, dict):
            base.update(raw)

        normalized = {
            "risk_per_trade_pct": _fraction(base.get("risk_per_trade_pct"), defaults[model_name]["risk_per_trade_pct"]),
            "max_pct_per_underlying_family": _fraction(
                base.get("max_pct_per_underlying_family"), defaults[model_name]["max_pct_per_underlying_family"]
            ),
            "max_pct_per_city_day": _fraction(base.get("max_pct_per_city_day"), defaults[model_name]["max_pct_per_city_day"]),
            "max_pct_total_deployed": _fraction(
                base.get("max_pct_total_deployed"), defaults[model_name]["max_pct_total_deployed"]
            ),
            "max_simultaneous_market_sides": _safe_non_negative_int(
                base.get("max_simultaneous_market_sides"), defaults[model_name]["max_simultaneous_market_sides"]
            ),
            "max_same_station_exposure_pct": _fraction(
                base.get("max_same_station_exposure_pct"), defaults[model_name]["max_same_station_exposure_pct"]
            ),
            "max_same_hour_cluster_pct": _fraction(
                base.get("max_same_hour_cluster_pct"), defaults[model_name]["max_same_hour_cluster_pct"]
            ),
        }
        if model_name == "capped_fractional_kelly_shadow":
            normalized["kelly_fraction_multiplier"] = _safe_positive(base.get("kelly_fraction_multiplier"), 0.5)
        if model_name == "fixed_unit_risk_budget":
            normalized["unit_risk_dollars"] = _safe_positive(
                base.get("unit_risk_dollars"),
                reference_bankroll_dollars * normalized["risk_per_trade_pct"],
            )
        models[model_name] = normalized
    return models


def _resolve_slippage_bps_list(value: str | None) -> list[float]:
    text = _normalize_text(value)
    if not text:
        return [0.0]
    as_path = Path(text)
    try:
        if as_path.exists() and as_path.is_file():
            payload = _load_json(as_path)
            if isinstance(payload.get("slippage_bps_list"), list):
                output = []
                for item in payload["slippage_bps_list"]:
                    parsed = _parse_float(item)
                    if isinstance(parsed, float):
                        output.append(max(0.0, parsed))
                return sorted(set(output)) or [0.0]
    except OSError:
        pass
    output: list[float] = []
    for token in text.split(","):
        parsed = _parse_float(token)
        if isinstance(parsed, float):
            output.append(max(0.0, parsed))
    return sorted(set(output)) or [0.0]


def _default_readiness_horizon_map() -> dict[str, float]:
    return {label: hours for label, hours in _DEFAULT_READINESS_HORIZONS}


def _resolve_readiness_horizons(value: str | None) -> list[tuple[str, float]]:
    if not _normalize_text(value):
        return list(_DEFAULT_READINESS_HORIZONS)

    default_map = _default_readiness_horizon_map()
    seen: set[str] = set()
    resolved: list[tuple[str, float]] = []
    for token in [item.strip().lower() for item in str(value).split(",") if item.strip()]:
        label = token
        hours: float | None = default_map.get(token)
        if hours is None:
            if token.endswith("h"):
                parsed = _parse_float(token[:-1])
                if isinstance(parsed, float) and parsed > 0:
                    hours = float(parsed)
                    label = f"{int(parsed)}h" if abs(parsed - round(parsed)) < 1e-9 else f"{parsed}h"
            elif token.endswith("d"):
                parsed = _parse_float(token[:-1])
                if isinstance(parsed, float) and parsed > 0:
                    hours = float(parsed) * 24.0
                    label = f"{int(parsed)}d" if abs(parsed - round(parsed)) < 1e-9 else f"{parsed}d"
        if not isinstance(hours, float) or hours <= 0:
            continue
        if label in seen:
            continue
        seen.add(label)
        resolved.append((label, hours))
    if resolved:
        return resolved
    return list(_DEFAULT_READINESS_HORIZONS)


def _readiness_gate_thresholds(days: float) -> dict[str, float]:
    return {
        "min_resolved_unique_market_sides": float(max(3, round(days * 2.0))),
        "min_resolved_unique_underlying_families": float(max(2, round(days * 0.5))),
        "min_exposure_utilization_peak_pct": 8.0,
        "min_exposure_utilization_avg_pct": 2.0,
        "max_repeated_entry_multiplier": 2.5,
        "min_trade_count": float(max(4, round(days * 1.2))),
    }


def _min_data_coverage_ratio_for_horizon(days: float) -> float:
    if days <= 1.0:
        return 1.0
    if days <= 14.0:
        return 0.75
    if days <= 90.0:
        return 0.5
    return 0.35


def _extract_deployment_metrics(
    validation_payload: dict[str, Any],
    *,
    model_name: str,
    aggregation_layer: str,
    slippage_bps: float,
) -> dict[str, Any]:
    by_model = (
        validation_payload.get("bankroll_simulation", {})
        .get("by_model", {})
        .get(model_name, {})
        .get("by_slippage_bps", {})
    )
    slippage_key = str(slippage_bps)
    metrics = by_model.get(slippage_key, {}).get(aggregation_layer)
    if isinstance(metrics, dict):
        return metrics

    fallback_metrics = by_model.get(str(0.0), {}).get(aggregation_layer)
    if isinstance(fallback_metrics, dict):
        return fallback_metrics
    return {}


def _build_horizon_readiness_entry(
    *,
    horizon_label: str,
    horizon_hours: float,
    validation_payload: dict[str, Any],
    reference_bankroll_dollars: float,
    conservative_slippage_bps: float,
    fee_model: dict[str, Any],
) -> dict[str, Any]:
    days = max(1e-9, horizon_hours / 24.0)
    window_payload = validation_payload.get("window") if isinstance(validation_payload.get("window"), dict) else {}
    observed_window_hours = _parse_float(window_payload.get("effective_window_hours_for_metrics"))
    if not isinstance(observed_window_hours, float):
        observed_window_hours = _parse_float(window_payload.get("observed_span_hours"))
    if not isinstance(observed_window_hours, float) or observed_window_hours <= 0.0:
        observed_window_hours = 0.0
    observed_window_hours = min(horizon_hours, max(0.0, observed_window_hours))
    observed_window_days = observed_window_hours / 24.0 if observed_window_hours > 0.0 else 0.0
    data_coverage_ratio = min(1.0, observed_window_hours / max(1e-9, horizon_hours)) if horizon_hours > 0 else 0.0

    breadth = validation_payload.get("opportunity_breadth", {})
    concentration = validation_payload.get("concentration_checks", {})
    expected_vs_shadow = (
        validation_payload.get("expected_vs_shadow_settled", {})
        .get("by_aggregation_layer", {})
        .get("unique_market_side", {})
    )
    expected_vs_shadow_full = (
        validation_payload.get("expected_vs_shadow_settled")
        if isinstance(validation_payload.get("expected_vs_shadow_settled"), dict)
        else {}
    )
    deployment_metrics = _extract_deployment_metrics(
        validation_payload,
        model_name=_DEPLOYMENT_HEADLINE_MODEL,
        aggregation_layer=_DEPLOYMENT_HEADLINE_LAYER,
        slippage_bps=conservative_slippage_bps,
    )

    pnl_total = float(deployment_metrics.get("pnl_total") or 0.0)
    deployed_capital = float(deployment_metrics.get("deployed_capital") or 0.0)
    roi_reference = _parse_float(deployment_metrics.get("roi_on_reference_bankroll")) or 0.0
    roi_deployed = _parse_float(deployment_metrics.get("roi_on_deployed_capital"))
    utilization_avg_pct = float(_parse_float(deployment_metrics.get("exposure_utilization_avg")) or 0.0) * 100.0
    utilization_peak_pct = float(_parse_float(deployment_metrics.get("exposure_utilization_peak")) or 0.0) * 100.0

    projected_daily_pnl = pnl_total / observed_window_days if observed_window_days > 0 else 0.0
    projected_pnl_for_horizon = pnl_total if data_coverage_ratio < 0.999 else projected_daily_pnl * days
    projected_roi_reference = projected_pnl_for_horizon / reference_bankroll_dollars if reference_bankroll_dollars > 0 else 0.0

    hysa_annual_rate = _fraction(fee_model.get("hysa_comparison_assumption_annual_rate"), 0.045)
    equivalent_hysa_return = reference_bankroll_dollars * (hysa_annual_rate * observed_window_days / 365.0)
    projected_excess_over_hysa = projected_pnl_for_horizon - equivalent_hysa_return

    threshold_basis_days = max(1.0, observed_window_days)
    thresholds = _readiness_gate_thresholds(threshold_basis_days)
    min_coverage_ratio = _min_data_coverage_ratio_for_horizon(days)
    resolved_unique_market_sides = int(breadth.get("resolved_unique_market_sides") or 0)
    resolved_unique_families = int(breadth.get("resolved_unique_underlying_families") or 0)
    repeated_multiplier = _parse_float(breadth.get("repeated_entry_multiplier"))
    repeated_multiplier_value = repeated_multiplier if isinstance(repeated_multiplier, float) else 0.0
    trade_count = int(deployment_metrics.get("trade_count") or 0)
    concentration_warning = bool(concentration.get("concentration_warning"))
    calibration_ratio = _parse_float(expected_vs_shadow.get("calibration_ratio"))
    data_quality = (
        validation_payload.get("data_quality")
        if isinstance(validation_payload.get("data_quality"), dict)
        else {}
    )
    pipeline_health = (
        data_quality.get("pipeline_health")
        if isinstance(data_quality.get("pipeline_health"), dict)
        else {}
    )
    pipeline_status = (
        _normalize_text(pipeline_health.get("status"))
        or _normalize_text(data_quality.get("pipeline_status"))
        or "unknown"
    ).lower()

    gate_checks = [
        {
            "reason": "non_positive_roi_on_reference_bankroll",
            "passed": bool(roi_reference > 0),
            "observed": round(roi_reference, 6),
            "threshold": "> 0",
        },
        {
            "reason": "does_not_exceed_hysa_for_window",
            "passed": bool(projected_excess_over_hysa > 0),
            "observed": round(projected_excess_over_hysa, 6),
            "threshold": "> 0",
        },
        {
            "reason": "insufficient_history_coverage_for_horizon",
            "passed": bool(data_coverage_ratio >= min_coverage_ratio),
            "observed": round(data_coverage_ratio, 6),
            "threshold": f">= {round(min_coverage_ratio, 6)}",
        },
        {
            "reason": "insufficient_independent_market_side_breadth",
            "passed": bool(resolved_unique_market_sides >= int(thresholds["min_resolved_unique_market_sides"])),
            "observed": resolved_unique_market_sides,
            "threshold": int(thresholds["min_resolved_unique_market_sides"]),
        },
        {
            "reason": "insufficient_underlying_family_breadth",
            "passed": bool(resolved_unique_families >= int(thresholds["min_resolved_unique_underlying_families"])),
            "observed": resolved_unique_families,
            "threshold": int(thresholds["min_resolved_unique_underlying_families"]),
        },
        {
            "reason": "insufficient_settled_trade_count",
            "passed": bool(trade_count >= int(thresholds["min_trade_count"])),
            "observed": trade_count,
            "threshold": int(thresholds["min_trade_count"]),
        },
        {
            "reason": "insufficient_peak_bankroll_utilization",
            "passed": bool(utilization_peak_pct >= thresholds["min_exposure_utilization_peak_pct"]),
            "observed": round(utilization_peak_pct, 4),
            "threshold": thresholds["min_exposure_utilization_peak_pct"],
        },
        {
            "reason": "insufficient_average_bankroll_utilization",
            "passed": bool(utilization_avg_pct >= thresholds["min_exposure_utilization_avg_pct"]),
            "observed": round(utilization_avg_pct, 4),
            "threshold": thresholds["min_exposure_utilization_avg_pct"],
        },
        {
            "reason": "repeated_entry_multiplier_too_high",
            "passed": bool(repeated_multiplier_value <= thresholds["max_repeated_entry_multiplier"]),
            "observed": round(repeated_multiplier_value, 6),
            "threshold": thresholds["max_repeated_entry_multiplier"],
        },
        {
            "reason": "concentration_warning",
            "passed": not concentration_warning,
            "observed": concentration_warning,
            "threshold": False,
        },
        {
            "reason": "pipeline_data_stale_or_missing",
            "passed": pipeline_status != "red",
            "observed": pipeline_status,
            "threshold": "!= red",
        },
    ]
    if isinstance(calibration_ratio, float):
        gate_checks.append(
            {
                "reason": "non_positive_calibration_ratio",
                "passed": bool(calibration_ratio > 0),
                "observed": round(calibration_ratio, 6),
                "threshold": "> 0",
            }
        )

    failed_reasons = [str(check["reason"]) for check in gate_checks if not bool(check.get("passed"))]
    passed_count = len([check for check in gate_checks if bool(check.get("passed"))])
    total_checks = len(gate_checks)
    gate_score = (passed_count / total_checks) if total_checks > 0 else 0.0

    readiness_passed = len(failed_reasons) == 0
    confidence = "high" if readiness_passed else ("low" if len(failed_reasons) >= 4 else "medium")
    status = "green" if readiness_passed else ("yellow" if gate_score >= 0.6 else "red")
    failed_reason_details = [
        {
            "reason": reason,
            "description": _READINESS_BLOCKER_DETAILS.get(reason, "Readiness gate failed."),
            "recommended_action": _READINESS_BLOCKER_ACTIONS.get(reason, "Continue shadow soak and gather more evidence."),
        }
        for reason in failed_reasons
    ]

    signal_progress = (
        validation_payload.get("signal_progress")
        if isinstance(validation_payload.get("signal_progress"), dict)
        else {}
    )
    signal_evidence_full = (
        validation_payload.get("signal_evidence")
        if isinstance(validation_payload.get("signal_evidence"), dict)
        else {}
    )
    data_quality = (
        validation_payload.get("data_quality")
        if isinstance(validation_payload.get("data_quality"), dict)
        else {}
    )
    anti_misleading_guards = (
        validation_payload.get("anti_misleading_guards")
        if isinstance(validation_payload.get("anti_misleading_guards"), dict)
        else {}
    )
    window_full = (
        validation_payload.get("window")
        if isinstance(validation_payload.get("window"), dict)
        else {}
    )
    viability_full = (
        validation_payload.get("viability_summary")
        if isinstance(validation_payload.get("viability_summary"), dict)
        else {}
    )
    alpha_feature_density_full = (
        validation_payload.get("alpha_feature_density")
        if isinstance(validation_payload.get("alpha_feature_density"), dict)
        else {}
    )
    deployment_headline = (
        validation_payload.get("viability_summary", {}).get("deployment_headline_basis")
        if isinstance(validation_payload.get("viability_summary"), dict)
        else {}
    )
    pipeline_health_summary = (
        data_quality.get("pipeline_health")
        if isinstance(data_quality.get("pipeline_health"), dict)
        else {}
    )

    return {
        "horizon": horizon_label,
        "hours": round(horizon_hours, 6),
        "days": round(days, 6),
        "window_semantics": {
            "type": "rolling",
            "is_calendar_day": False,
            "label": f"rolling_{horizon_label}",
        },
        "deployment_quality_basis": {
            "sizing_model": _DEPLOYMENT_HEADLINE_MODEL,
            "aggregation_layer": _DEPLOYMENT_HEADLINE_LAYER,
            "slippage_bps": conservative_slippage_bps,
        },
        "opportunity_breadth": {
            "resolved_unique_market_sides": resolved_unique_market_sides,
            "resolved_unique_underlying_families": resolved_unique_families,
            "repeated_entry_multiplier": repeated_multiplier,
            "concentration_warning": concentration_warning,
        },
        "performance": {
            "deployed_capital": round(deployed_capital, 6),
            "pnl_total": round(pnl_total, 6),
            "roi_on_deployed_capital": round(roi_deployed, 6) if isinstance(roi_deployed, float) else None,
            "roi_on_reference_bankroll": round(roi_reference, 6),
            "observed_window_days_for_metrics": round(observed_window_days, 6),
            "data_coverage_ratio": round(data_coverage_ratio, 6),
            "projected_daily_pnl_on_reference_bankroll": round(projected_daily_pnl, 6),
            "projected_pnl_for_horizon": round(projected_pnl_for_horizon, 6),
            "projected_roi_on_reference_bankroll": round(projected_roi_reference, 6),
            "equivalent_hysa_return_for_horizon": round(equivalent_hysa_return, 6),
            "projected_excess_return_over_hysa": round(projected_excess_over_hysa, 6),
        },
        "quality": {
            "trade_count": trade_count,
            "calibration_ratio_unique_market_side": round(calibration_ratio, 6)
            if isinstance(calibration_ratio, float)
            else None,
            "exposure_utilization_avg_pct": round(utilization_avg_pct, 4),
            "exposure_utilization_peak_pct": round(utilization_peak_pct, 4),
            "max_drawdown": deployment_metrics.get("max_drawdown"),
            "worst_window_pnl": deployment_metrics.get("worst_window_pnl"),
            "best_window_pnl": deployment_metrics.get("best_window_pnl"),
        },
        "gates": {
            "thresholds": thresholds,
            "threshold_basis_days": round(threshold_basis_days, 6),
            "minimum_data_coverage_ratio": round(min_coverage_ratio, 6),
            "checks": gate_checks,
            "failed_reasons": failed_reasons,
            "failed_reason_details": failed_reason_details,
            "passed_count": passed_count,
            "total_checks": total_checks,
            "gate_score": round(gate_score, 6),
            "passed": readiness_passed,
        },
        "readiness_status": status,
        "ready_for_real_money": readiness_passed,
        "readiness_confidence": confidence,
        "parity_context": {
            "window": {
                "hours": _parse_float(window_full.get("hours")),
                "observed_span_hours": _parse_float(window_full.get("observed_span_hours")),
                "effective_window_hours_for_metrics": _parse_float(
                    window_full.get("effective_window_hours_for_metrics")
                ),
                "effective_window_days_for_metrics": _parse_float(
                    window_full.get("effective_window_days_for_metrics")
                ),
                "data_coverage_ratio": _parse_float(window_full.get("data_coverage_ratio")),
            },
            "deployment_headline_basis": deployment_headline if isinstance(deployment_headline, dict) else {},
            "opportunity_breadth": {
                "resolved_unique_market_sides": resolved_unique_market_sides,
                "resolved_unique_underlying_families": resolved_unique_families,
                "repeated_entry_multiplier": repeated_multiplier,
                "concentration_warning": concentration_warning,
            },
            "quality_summary": {
                "trade_count": trade_count,
                "calibration_ratio_unique_market_side": round(calibration_ratio, 6)
                if isinstance(calibration_ratio, float)
                else None,
            },
            "expected_vs_shadow_settled_full": {
                "expected_shadow_edge_total": _parse_float(expected_vs_shadow_full.get("expected_shadow_edge_total")),
                "shadow_settled_pnl": _parse_float(expected_vs_shadow_full.get("shadow_settled_pnl")),
                "delta_total": _parse_float(expected_vs_shadow_full.get("delta_total")),
                "delta_per_trade": _parse_float(expected_vs_shadow_full.get("delta_per_trade")),
                "calibration_ratio": _parse_float(expected_vs_shadow_full.get("calibration_ratio")),
            },
            "viability_summary_full": {
                "effective_window_days_for_hysa_comparison": _parse_float(
                    viability_full.get("effective_window_days_for_hysa_comparison")
                ),
                "data_coverage_ratio": _parse_float(viability_full.get("data_coverage_ratio")),
                "equivalent_window_hysa_return_on_reference_bankroll": _parse_float(
                    viability_full.get("equivalent_window_hysa_return_on_reference_bankroll")
                ),
                "excess_return_over_hysa_for_window": _parse_float(
                    viability_full.get("excess_return_over_hysa_for_window")
                ),
                "main_limiting_factor": _normalize_text(viability_full.get("main_limiting_factor")),
            },
            "alpha_feature_density_full": alpha_feature_density_full,
            "signal_progress": signal_progress,
            "signal_evidence": signal_evidence_full,
            "data_quality": {
                "pipeline_status": _normalize_text(data_quality.get("pipeline_status")) or pipeline_status,
                "pipeline_reason": _normalize_text(data_quality.get("pipeline_reason")),
                "pipeline_missing_feeds": (
                    pipeline_health_summary.get("missing_feeds")
                    if isinstance(pipeline_health_summary.get("missing_feeds"), list)
                    else []
                ),
                "pipeline_stale_feeds": (
                    pipeline_health_summary.get("stale_feeds")
                    if isinstance(pipeline_health_summary.get("stale_feeds"), list)
                    else []
                ),
                "pipeline_out_of_window_feeds": (
                    pipeline_health_summary.get("out_of_window_feeds")
                    if isinstance(pipeline_health_summary.get("out_of_window_feeds"), list)
                    else []
                ),
                "pipeline_min_in_window_coverage_ratio": _parse_float(
                    pipeline_health_summary.get("min_in_window_coverage_ratio")
                ),
                "pipeline_avg_in_window_coverage_ratio": _parse_float(
                    pipeline_health_summary.get("avg_in_window_coverage_ratio")
                ),
                "current_settlement_unresolved": _parse_int(data_quality.get("settlement_backlog_now", {}).get("current_settlement_unresolved"))
                if isinstance(data_quality.get("settlement_backlog_now"), dict)
                else None,
            },
            "anti_misleading_guards": anti_misleading_guards,
            "full_payload_omitted": True,
        },
    }


def _build_overall_live_decision(horizon_entries: list[dict[str, Any]]) -> dict[str, Any]:
    by_label = {str(entry.get("horizon")): entry for entry in horizon_entries}
    passing = [entry for entry in horizon_entries if bool(entry.get("ready_for_real_money"))]
    passing_sorted = sorted(
        passing,
        key=lambda entry: float(_parse_float(entry.get("hours")) or 1e18),
    )
    earliest = passing_sorted[0]["horizon"] if passing_sorted else None
    pilot_required = ["14d", "21d"]
    scaled_required = ["28d", "3mo", "6mo"]

    available_pilot_required = [label for label in pilot_required if label in by_label]
    available_scaled_required = [label for label in scaled_required if label in by_label]

    if available_pilot_required:
        pilot_ready = all(bool(by_label.get(label, {}).get("ready_for_real_money")) for label in available_pilot_required)
    else:
        # Fallback for custom horizon subsets that omit pilot markers: require all
        # requested horizons >= 14d to pass.
        pilot_candidates = [
            entry
            for entry in horizon_entries
            if float(_parse_float(entry.get("hours")) or 0.0) >= 14.0 * 24.0
        ]
        pilot_ready = bool(pilot_candidates) and all(bool(entry.get("ready_for_real_money")) for entry in pilot_candidates)

    if available_scaled_required:
        scaled_ready = all(bool(by_label.get(label, {}).get("ready_for_real_money")) for label in available_scaled_required)
    else:
        scaled_candidates = [
            entry
            for entry in horizon_entries
            if float(_parse_float(entry.get("hours")) or 0.0) >= 28.0 * 24.0
        ]
        scaled_ready = bool(scaled_candidates) and all(bool(entry.get("ready_for_real_money")) for entry in scaled_candidates)

    if scaled_ready:
        recommendation = "scaled_live_candidate"
        message = "Multi-week/month horizons are passing readiness gates; scaled live trial can be considered."
    elif pilot_ready:
        recommendation = "small_live_pilot_candidate"
        message = "Intermediate horizons are passing; a tightly capped live pilot can be considered."
    else:
        recommendation = "shadow_only_continue"
        message = "Readiness gates are not consistently passing yet; keep shadow soak and broaden independent alpha."

    failed_reason_counts: dict[str, int] = {}
    for entry in horizon_entries:
        gates = entry.get("gates", {})
        for reason in gates.get("failed_reasons", []) if isinstance(gates, dict) else []:
            key = _normalize_text(reason)
            if not key:
                continue
            failed_reason_counts[key] = int(failed_reason_counts.get(key, 0) + 1)
    ranked_failures = sorted(failed_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    blocker_actions = [
        {
            "reason": reason,
            "count": count,
            "description": _READINESS_BLOCKER_DETAILS.get(reason, "Readiness gate failed."),
            "recommended_action": _READINESS_BLOCKER_ACTIONS.get(
                reason, "Continue shadow soak and gather more evidence."
            ),
        }
        for reason, count in ranked_failures[:5]
    ]

    return {
        "ready_for_small_live_pilot": pilot_ready,
        "ready_for_scaled_live": scaled_ready,
        "earliest_passing_horizon": earliest,
        "passing_horizons": [entry.get("horizon") for entry in passing],
        "most_common_blockers": blocker_actions,
        "recommendation": recommendation,
        "recommendation_summary": message,
    }

def _resolve_fee_model(fee_model_json: str | None) -> dict[str, Any]:
    payload = _load_json_input(fee_model_json)
    model = {
        "entry_fee_rate": _fraction(payload.get("entry_fee_rate"), 0.0),
        "exit_fee_rate": _fraction(payload.get("exit_fee_rate"), 0.0),
        "fixed_fee_per_trade": _safe_positive(payload.get("fixed_fee_per_trade"), 0.0),
        "hysa_comparison_assumption_annual_rate": _fraction(payload.get("hysa_comparison_assumption_annual_rate"), 0.045),
    }
    return model


def _derive_threshold_yes_outcome(threshold_expression: str, observed: float) -> bool | None:
    text = _normalize_text(threshold_expression)
    if not text or ":" not in text:
        return None
    parts = [token.strip() for token in text.split(":")]
    if len(parts) < 2:
        return None
    kind = parts[0].lower()
    try:
        values = [float(token) for token in parts[1:] if token]
    except ValueError:
        return None
    if kind == "at_most" and len(values) >= 1:
        return observed <= values[0]
    if kind == "below" and len(values) >= 1:
        return observed < values[0]
    if kind == "at_least" and len(values) >= 1:
        return observed >= values[0]
    if kind == "above" and len(values) >= 1:
        return observed > values[0]
    if kind == "between" and len(values) >= 2:
        lo = min(values[0], values[1])
        hi = max(values[0], values[1])
        return lo <= observed <= hi
    if kind == "equal" and len(values) >= 1:
        return abs(observed - values[0]) <= 1e-6
    return None


@lru_cache(maxsize=512)
def _cached_parse_intent_context_rows(
    path_text: str,
    _mtime_ns: int,
    _size_bytes: int,
) -> tuple[dict[str, Any], ...]:
    path = Path(path_text)
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                intent_id = _normalize_text(row.get("intent_id"))
                if not intent_id:
                    continue
                policy_reason = _normalize_text(row.get("policy_reason")).lower() or "unknown"
                local_hour = _parse_int(row.get("policy_metar_local_hour"))
                rows.append(
                    {
                        "intent_id": intent_id,
                        "policy_reason": policy_reason,
                        "underlying_key": _normalize_text(row.get("underlying_key")),
                        "series_ticker": _normalize_text(row.get("series_ticker")),
                        "settlement_station": _normalize_text(row.get("settlement_station")).upper() or "unknown",
                        "target_date_local": _normalize_text(row.get("target_date_local")),
                        "settlement_timezone": _normalize_text(row.get("settlement_timezone")),
                        "local_hour_key": str(local_hour) if isinstance(local_hour, int) else "unknown",
                        "signal_type": _normalize_text(row.get("constraint_status")).lower() or "unknown",
                        "policy_version": _extract_first_text(row, _POLICY_VERSION_CANDIDATES),
                        "close_time": _normalize_text(row.get("close_time")),
                        "hours_to_close": _parse_float(row.get("hours_to_close")),
                    }
                )
    except OSError:
        return tuple()
    return tuple(rows)


@lru_cache(maxsize=256)
def _cached_parse_temperature_plan_rows(
    path_text: str,
    _mtime_ns: int,
    _size_bytes: int,
) -> tuple[dict[str, Any], ...]:
    path = Path(path_text)
    inferred_ts = _artifact_timestamp_utc(path)
    try:
        mtime_ts = datetime.fromtimestamp(float(path.stat().st_mtime), timezone.utc)
    except OSError:
        mtime_ts = None
    if isinstance(inferred_ts, datetime) and isinstance(mtime_ts, datetime):
        file_planned_at = inferred_ts if inferred_ts >= mtime_ts else mtime_ts
    elif isinstance(inferred_ts, datetime):
        file_planned_at = inferred_ts
    elif isinstance(mtime_ts, datetime):
        file_planned_at = mtime_ts
    else:
        return tuple()

    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for csv_row in reader:
                if not _is_temperature_plan_row(csv_row):
                    continue
                planned_at = (
                    _parse_ts(csv_row.get("captured_at_utc"))
                    or _parse_ts(csv_row.get("captured_at"))
                    or _parse_ts(csv_row.get("planned_at_utc"))
                    or _parse_ts(csv_row.get("planned_at"))
                    or file_planned_at
                )
                entry_price = _parse_float(csv_row.get("maker_entry_price_dollars"))
                if not isinstance(entry_price, float) or entry_price <= 0.0:
                    continue
                contracts = _parse_float(csv_row.get("contracts_per_order"))
                if not isinstance(contracts, float) or contracts <= 0:
                    contracts = 1.0
                yes_possible_overlap_raw = _normalize_text(csv_row.get("temperature_yes_possible_overlap")).lower()
                yes_possible_overlap: bool | None = None
                if yes_possible_overlap_raw in {"1", "true", "yes"}:
                    yes_possible_overlap = True
                elif yes_possible_overlap_raw in {"0", "false", "no"}:
                    yes_possible_overlap = False

                rows.append(
                    {
                        "planned_at": planned_at,
                        "planned_at_epoch": planned_at.timestamp(),
                        "intent_id": _normalize_text(csv_row.get("temperature_intent_id")),
                        "shadow_order_id": _normalize_text(
                            csv_row.get("temperature_client_order_id") or csv_row.get("client_order_id")
                        ),
                        "market_ticker": _normalize_text(csv_row.get("market_ticker")),
                        "side": _normalize_text(csv_row.get("side")).lower(),
                        "entry_price_dollars": max(0.01, min(0.99, float(entry_price))),
                        "contracts": float(contracts),
                        "expected_edge_dollars": float(_extract_row_metric(csv_row, _EDGE_FIELD_CANDIDATES)),
                        "estimated_entry_cost_dollars": float(_extract_row_metric(csv_row, _COST_FIELD_CANDIDATES)),
                        "row_underlying_key": _normalize_text(csv_row.get("temperature_underlying_key")),
                        "row_policy_version": _extract_first_text(csv_row, _POLICY_VERSION_CANDIDATES),
                        "hours_to_close": _parse_float(csv_row.get("hours_to_close")),
                        "alpha_strength": _parse_float(csv_row.get("temperature_alpha_strength")),
                        "yes_possible_gap": _parse_float(csv_row.get("temperature_yes_possible_gap")),
                        "yes_possible_overlap": yes_possible_overlap,
                        "primary_signal_margin": _parse_float(csv_row.get("temperature_primary_signal_margin")),
                        "forecast_feasibility_margin": _parse_float(
                            csv_row.get("temperature_forecast_feasibility_margin")
                        ),
                        "speci_shock_active": _parse_optional_bool(csv_row.get("temperature_speci_shock_active")),
                        "speci_shock_confidence": _parse_float(csv_row.get("temperature_speci_shock_confidence")),
                        "speci_shock_weight": _parse_float(csv_row.get("temperature_speci_shock_weight")),
                        "speci_shock_mode": _normalize_text(csv_row.get("temperature_speci_shock_mode")).lower()
                        or None,
                        "speci_shock_trigger_count": _parse_int(
                            csv_row.get("temperature_speci_shock_trigger_count")
                        ),
                        "speci_shock_cooldown_blocked": _parse_optional_bool(
                            csv_row.get("temperature_speci_shock_cooldown_blocked")
                        ),
                        "speci_shock_improvement_hold_active": _parse_optional_bool(
                            csv_row.get("temperature_speci_shock_improvement_hold_active")
                        ),
                    }
                )
    except OSError:
        return tuple()
    return tuple(rows)


@lru_cache(maxsize=128)
def _cached_parse_threshold_map_by_ticker(
    path_text: str,
    _mtime_ns: int,
    _size_bytes: int,
) -> dict[str, str]:
    path = Path(path_text)
    threshold_map: dict[str, str] = {}
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ticker = _normalize_text(row.get("market_ticker"))
                threshold_expression = _normalize_text(row.get("threshold_expression"))
                if ticker and threshold_expression:
                    threshold_map[ticker] = threshold_expression
    except OSError:
        return {}
    return threshold_map


@lru_cache(maxsize=128)
def _cached_parse_settlement_truth_map(
    path_text: str,
    _mtime_ns: int,
    _size_bytes: int,
) -> dict[str, float]:
    payload = _load_json(Path(path_text))
    underlyings = payload.get("underlyings") if isinstance(payload.get("underlyings"), dict) else {}
    truth_map: dict[str, float] = {}
    for key, entry in underlyings.items():
        if not isinstance(entry, dict):
            continue
        key_text = _normalize_text(key)
        if not key_text:
            continue
        truth = _parse_float(entry.get("final_truth_value"))
        if isinstance(truth, float):
            truth_map[key_text] = float(truth)
    return truth_map


@lru_cache(maxsize=32)
def _cached_intent_context_upto_end(
    out_dir_text: str,
    end_epoch: float,
) -> dict[str, dict[str, Any]]:
    out_dir = Path(out_dir_text)
    intents_files = _files_in_window_cached(out_dir, "kalshi_temperature_trade_intents_*.csv", 0.0, end_epoch)
    by_intent: dict[str, dict[str, Any]] = {}
    for path in intents_files:
        signature = _file_signature(path)
        for row in _cached_parse_intent_context_rows(*signature):
            intent_id = _normalize_text(row.get("intent_id"))
            if not intent_id:
                continue
            by_intent[intent_id] = {
                "underlying_key": _normalize_text(row.get("underlying_key")),
                "series_ticker": _normalize_text(row.get("series_ticker")),
                "settlement_station": _normalize_text(row.get("settlement_station")).upper() or "unknown",
                "target_date_local": _normalize_text(row.get("target_date_local")),
                "settlement_timezone": _normalize_text(row.get("settlement_timezone")),
                "local_hour_key": _normalize_text(row.get("local_hour_key")) or "unknown",
                "signal_type": _normalize_text(row.get("signal_type")).lower() or "unknown",
                "policy_reason": _normalize_text(row.get("policy_reason")).lower() or "unknown",
                "policy_version": _normalize_text(row.get("policy_version")),
                "close_time": _normalize_text(row.get("close_time")),
                "hours_to_close": _parse_float(row.get("hours_to_close")),
            }
    return by_intent


def _intent_context_in_window(out_dir: Path, start_epoch: float, end_epoch: float) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    out_dir_text = str(out_dir.resolve())
    intents_files = _files_in_window_cached(out_dir, "kalshi_temperature_trade_intents_*.csv", start_epoch, end_epoch)
    policy_reason_counts: dict[str, int] = {}
    for path in intents_files:
        signature = _file_signature(path)
        for row in _cached_parse_intent_context_rows(*signature):
            intent_id = _normalize_text(row.get("intent_id"))
            if not intent_id:
                continue
            policy_reason = _normalize_text(row.get("policy_reason")).lower() or "unknown"
            policy_reason_counts[policy_reason] = int(policy_reason_counts.get(policy_reason, 0) + 1)
    return dict(_cached_intent_context_upto_end(out_dir_text, float(end_epoch))), policy_reason_counts


def _build_plan_row_with_context(
    *,
    parsed_row: dict[str, Any],
    context: dict[str, Any],
    row_id: str,
    source_file_epoch: float,
    source_file_has_stamp: bool,
) -> dict[str, Any]:
    planned_at = parsed_row["planned_at"]
    intent_id = _normalize_text(parsed_row.get("intent_id"))
    shadow_order_id = _normalize_text(parsed_row.get("shadow_order_id"))
    market_ticker = _normalize_text(parsed_row.get("market_ticker"))
    side = _normalize_text(parsed_row.get("side")).lower()
    entry_price = float(parsed_row.get("entry_price_dollars") or 0.5)
    contracts = float(parsed_row.get("contracts") or 1.0)
    expected_edge = float(parsed_row.get("expected_edge_dollars") or 0.0)
    estimated_cost = float(parsed_row.get("estimated_entry_cost_dollars") or 0.0)
    policy_version = _normalize_text(parsed_row.get("row_policy_version")) or _normalize_text(context.get("policy_version"))

    underlying_key = _normalize_text(parsed_row.get("row_underlying_key")) or _normalize_text(context.get("underlying_key"))
    settlement_station = _normalize_text(context.get("settlement_station")).upper() or "unknown"
    target_date_local = _normalize_text(context.get("target_date_local"))
    if not target_date_local and underlying_key.count("|") >= 2:
        target_date_local = underlying_key.split("|")[-1]
    series_ticker = _normalize_text(context.get("series_ticker"))
    if not series_ticker and underlying_key.count("|") >= 1:
        series_ticker = underlying_key.split("|")[0]
    local_hour_key = _normalize_text(context.get("local_hour_key")) or "unknown"
    signal_type = _normalize_text(context.get("signal_type")).lower() or "unknown"
    policy_reason = _normalize_text(context.get("policy_reason")).lower() or "unknown"

    hours_to_close = _parse_float(parsed_row.get("hours_to_close"))
    if not isinstance(hours_to_close, float):
        hours_to_close = _parse_float(context.get("hours_to_close"))
    close_time = _parse_ts(context.get("close_time"))
    if close_time is None and isinstance(hours_to_close, float):
        close_time = planned_at + timedelta(hours=max(0.0, hours_to_close))
    if close_time is None:
        close_time = planned_at + timedelta(hours=24)

    city_day_key = f"{settlement_station}|{target_date_local}" if target_date_local else settlement_station
    underlying_family_key = underlying_key or f"{series_ticker}|{city_day_key}"
    market_side_key = f"{market_ticker}|{side}" if market_ticker and side else market_ticker

    return {
        "row_id": row_id,
        "planned_at": planned_at,
        "planned_at_epoch": float(parsed_row.get("planned_at_epoch") or planned_at.timestamp()),
        "source_file_epoch": float(source_file_epoch),
        "source_file_has_stamp": bool(source_file_has_stamp),
        "close_time": close_time,
        "intent_id": intent_id,
        "shadow_order_id": shadow_order_id or row_id,
        "market_ticker": market_ticker,
        "side": side,
        "market_side_key": market_side_key,
        "underlying_key": underlying_key,
        "underlying_family_key": underlying_family_key,
        "city_day_key": city_day_key,
        "settlement_station": settlement_station,
        "local_hour_key": local_hour_key,
        "signal_type": signal_type,
        "policy_reason": policy_reason,
        "policy_version": policy_version,
        "contracts": contracts,
        "entry_price_dollars": max(0.01, min(0.99, float(entry_price))),
        "expected_edge_dollars": expected_edge,
        "estimated_entry_cost_dollars": estimated_cost,
        "alpha_strength": _parse_float(parsed_row.get("alpha_strength")),
        "yes_possible_gap": _parse_float(parsed_row.get("yes_possible_gap")),
        "yes_possible_overlap": parsed_row.get("yes_possible_overlap")
        if isinstance(parsed_row.get("yes_possible_overlap"), bool)
        else None,
        "primary_signal_margin": _parse_float(parsed_row.get("primary_signal_margin")),
        "forecast_feasibility_margin": _parse_float(parsed_row.get("forecast_feasibility_margin")),
        "speci_shock_active": parsed_row.get("speci_shock_active")
        if isinstance(parsed_row.get("speci_shock_active"), bool)
        else None,
        "speci_shock_confidence": _parse_float(parsed_row.get("speci_shock_confidence")),
        "speci_shock_weight": _parse_float(parsed_row.get("speci_shock_weight")),
        "speci_shock_mode": _normalize_text(parsed_row.get("speci_shock_mode")) or None,
        "speci_shock_trigger_count": (
            int(parsed_row.get("speci_shock_trigger_count"))
            if isinstance(parsed_row.get("speci_shock_trigger_count"), int)
            else None
        ),
        "speci_shock_cooldown_blocked": parsed_row.get("speci_shock_cooldown_blocked")
        if isinstance(parsed_row.get("speci_shock_cooldown_blocked"), bool)
        else None,
        "speci_shock_improvement_hold_active": parsed_row.get("speci_shock_improvement_hold_active")
        if isinstance(parsed_row.get("speci_shock_improvement_hold_active"), bool)
        else None,
    }


@lru_cache(maxsize=8)
def _cached_plan_rows_upto_end(
    out_dir_text: str,
    end_epoch: float,
) -> tuple[dict[str, Any], ...]:
    out_dir = Path(out_dir_text)
    intent_context = _cached_intent_context_upto_end(out_dir_text, float(end_epoch))
    plan_files = _files_in_window_cached(out_dir, "kalshi_temperature_trade_plan_*.csv", 0.0, end_epoch)
    rows: list[dict[str, Any]] = []
    row_counter = 0
    for path in plan_files:
        inferred_timestamp = _artifact_timestamp_utc(path)
        source_file_has_stamp = isinstance(inferred_timestamp, datetime)
        source_file_epoch = _artifact_epoch(path)
        signature = _file_signature(path)
        for parsed_row in _cached_parse_temperature_plan_rows(*signature):
            planned_at_epoch = float(parsed_row.get("planned_at_epoch") or 0.0)
            if planned_at_epoch > end_epoch:
                continue
            row_counter += 1
            intent_id = _normalize_text(parsed_row.get("intent_id"))
            context = intent_context.get(intent_id, {})
            rows.append(
                _build_plan_row_with_context(
                    parsed_row=parsed_row,
                    context=context,
                    row_id=f"{path.name}:{row_counter}",
                    source_file_epoch=source_file_epoch,
                    source_file_has_stamp=source_file_has_stamp,
                )
            )
    return tuple(rows)


@lru_cache(maxsize=8)
def _cached_plan_row_epochs_upto_end(
    out_dir_text: str,
    end_epoch: float,
) -> tuple[float, ...]:
    rows = _cached_plan_rows_upto_end(out_dir_text, end_epoch)
    return tuple(float(row.get("planned_at_epoch") or 0.0) for row in rows)


def _plan_rows_in_window(
    *,
    out_dir: Path,
    start_epoch: float,
    end_epoch: float,
    intent_context: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], float, float]:
    del intent_context
    out_dir_text = str(out_dir.resolve())
    all_rows = _cached_plan_rows_upto_end(out_dir_text, float(end_epoch))
    all_epochs = _cached_plan_row_epochs_upto_end(out_dir_text, float(end_epoch))
    if not all_rows or not all_epochs:
        return [], 0.0, 0.0

    lo = bisect_left(all_epochs, float(start_epoch))
    end_epoch_inclusive = float(end_epoch) + 1.0
    hi = bisect_left(all_epochs, end_epoch_inclusive)
    if hi <= lo:
        return [], 0.0, 0.0

    rows: list[dict[str, Any]] = []
    expected_edge_total = 0.0
    expected_cost_total = 0.0
    for row in all_rows[lo:hi]:
        if bool(row.get("source_file_has_stamp")):
            in_window_epoch = float(row.get("source_file_epoch") or float(row.get("planned_at_epoch") or 0.0))
        else:
            in_window_epoch = float(row.get("planned_at_epoch") or 0.0)
        if in_window_epoch < float(start_epoch) or in_window_epoch >= end_epoch_inclusive:
            continue
        rows.append(row)
        expected_edge_total += float(row.get("expected_edge_dollars") or 0.0)
        expected_cost_total += float(row.get("estimated_entry_cost_dollars") or 0.0)
    return rows, round(expected_edge_total, 6), round(expected_cost_total, 6)


@lru_cache(maxsize=32)
def _cached_threshold_map_upto_end(
    out_dir_text: str,
    end_epoch: float,
) -> dict[str, str]:
    out_dir = Path(out_dir_text)
    specs_files = _files_in_window_cached(out_dir, "kalshi_temperature_contract_specs_*.csv", 0.0, end_epoch)
    threshold_by_ticker: dict[str, str] = {}
    for path in reversed(specs_files):
        signature = _file_signature(path)
        threshold_map = _cached_parse_threshold_map_by_ticker(*signature)
        for ticker, threshold_expression in threshold_map.items():
            if ticker not in threshold_by_ticker and threshold_expression:
                threshold_by_ticker[ticker] = threshold_expression
    return threshold_by_ticker


def _threshold_map(out_dir: Path, end_epoch: float, needed_tickers: set[str]) -> dict[str, str]:
    if not needed_tickers:
        return {}
    full_map = _cached_threshold_map_upto_end(str(out_dir.resolve()), float(end_epoch))
    return {
        ticker: threshold_expression
        for ticker, threshold_expression in ((ticker, _normalize_text(full_map.get(ticker))) for ticker in needed_tickers)
        if threshold_expression
    }


@lru_cache(maxsize=32)
def _cached_settlement_truth_upto_end(
    out_dir_text: str,
    end_epoch: float,
) -> tuple[dict[str, float], str, int]:
    out_dir = Path(out_dir_text)
    files = _files_in_window_cached(out_dir, "kalshi_temperature_settlement_state_*.json", 0.0, end_epoch)
    if not files:
        return {}, "", 0
    latest = files[-1]
    truth_map: dict[str, float] = {}
    files_scanned = 0
    for path in reversed(files):
        files_scanned += 1
        signature = _file_signature(path)
        parsed_truth_map = _cached_parse_settlement_truth_map(*signature)
        for key_text, truth in parsed_truth_map.items():
            if key_text not in truth_map:
                truth_map[key_text] = float(truth)
    return truth_map, str(latest), files_scanned


def _latest_settlement_truth_map(
    out_dir: Path,
    end_epoch: float,
    *,
    needed_underlyings: set[str] | None = None,
) -> tuple[dict[str, float], Path | None, int]:
    full_truth_map, latest_text, files_scanned = _cached_settlement_truth_upto_end(str(out_dir.resolve()), float(end_epoch))
    latest_path = Path(latest_text) if latest_text else None
    if needed_underlyings:
        needed = {_normalize_text(key) for key in needed_underlyings if _normalize_text(key)}
        truth_map = {key: value for key, value in full_truth_map.items() if key in needed}
    else:
        truth_map = dict(full_truth_map)
    return truth_map, latest_path, files_scanned


def _current_settlement_backlog_snapshot(settlement_state_file: Path | None) -> dict[str, Any]:
    blocked_underlyings = 0
    pending_final_report = 0
    if settlement_state_file is not None:
        payload = _load_json(settlement_state_file)
        blocked_underlyings = max(0, _parse_int(payload.get("blocked_underlyings")) or 0)
        state_counts = payload.get("state_counts") if isinstance(payload.get("state_counts"), dict) else {}
        pending_from_state = max(0, _parse_int(state_counts.get("pending_final_report")) or 0)
        pending_from_top = max(0, _parse_int(payload.get("final_report_pending_count")) or 0)
        pending_final_report = max(pending_from_state, pending_from_top)
    unresolved = int(blocked_underlyings + pending_final_report)
    return {
        "current_settlement_blocked_underlyings": int(blocked_underlyings),
        "current_settlement_pending_final_report": int(pending_final_report),
        "current_settlement_unresolved": unresolved,
        "settlement_backlog_clear": bool(unresolved <= 0),
    }


def _grade_plan_rows(rows: list[dict[str, Any]], threshold_by_ticker: dict[str, str], truth_by_underlying: dict[str, float]) -> list[TemperatureOpportunity]:
    graded: list[TemperatureOpportunity] = []
    for row in rows:
        market_ticker = _normalize_text(row.get("market_ticker"))
        side = _normalize_text(row.get("side")).lower()
        threshold = _normalize_text(threshold_by_ticker.get(market_ticker))
        truth = truth_by_underlying.get(_normalize_text(row.get("underlying_key")))
        resolved = False
        win: bool | None = None
        push = False
        outcome = "unresolved"
        resolution_reason = ""
        base_pnl = 0.0
        if not threshold or not isinstance(truth, float) or side not in {"yes", "no"}:
            resolution_reason = "missing_threshold_or_final_truth_or_side"
        else:
            yes_outcome = _derive_threshold_yes_outcome(threshold, truth)
            if yes_outcome is None:
                resolution_reason = "unsupported_threshold_expression"
            else:
                resolved = True
                win = (side == "yes" and yes_outcome) or (side == "no" and not yes_outcome)
                if bool(win):
                    outcome = "win"
                else:
                    outcome = "loss"
                contracts = float(row.get("contracts") or 1.0)
                entry_price = float(row.get("entry_price_dollars") or 0.5)
                if bool(win):
                    base_pnl = contracts * (1.0 - entry_price)
                else:
                    base_pnl = -contracts * entry_price
                resolution_reason = "resolved"
        graded.append(
            TemperatureOpportunity(
                row_id=_normalize_text(row.get("row_id")),
                planned_at=row.get("planned_at") if isinstance(row.get("planned_at"), datetime) else datetime.fromtimestamp(0, timezone.utc),
                close_time=row.get("close_time") if isinstance(row.get("close_time"), datetime) else datetime.fromtimestamp(0, timezone.utc),
                intent_id=_normalize_text(row.get("intent_id")),
                shadow_order_id=_normalize_text(row.get("shadow_order_id")),
                market_ticker=market_ticker,
                side=side,
                market_side_key=_normalize_text(row.get("market_side_key")),
                underlying_key=_normalize_text(row.get("underlying_key")),
                underlying_family_key=_normalize_text(row.get("underlying_family_key")),
                city_day_key=_normalize_text(row.get("city_day_key")),
                settlement_station=_normalize_text(row.get("settlement_station")).upper() or "unknown",
                local_hour_key=_normalize_text(row.get("local_hour_key")) or "unknown",
                signal_type=_normalize_text(row.get("signal_type")).lower() or "unknown",
                policy_reason=_normalize_text(row.get("policy_reason")).lower() or "unknown",
                policy_version=_normalize_text(row.get("policy_version")),
                contracts=float(row.get("contracts") or 1.0),
                entry_price_dollars=float(row.get("entry_price_dollars") or 0.5),
                expected_edge_dollars=float(row.get("expected_edge_dollars") or 0.0),
                estimated_entry_cost_dollars=float(row.get("estimated_entry_cost_dollars") or 0.0),
                resolved=resolved,
                win=win,
                push=push,
                outcome=outcome,
                base_pnl_dollars=round(base_pnl, 6),
                threshold_expression=threshold,
                final_truth_value=truth,
                resolution_reason=resolution_reason,
                alpha_strength=_parse_float(row.get("alpha_strength")),
                yes_possible_gap=_parse_float(row.get("yes_possible_gap")),
                yes_possible_overlap=row.get("yes_possible_overlap")
                if isinstance(row.get("yes_possible_overlap"), bool)
                else None,
                primary_signal_margin=_parse_float(row.get("primary_signal_margin")),
                forecast_feasibility_margin=_parse_float(row.get("forecast_feasibility_margin")),
                speci_shock_active=row.get("speci_shock_active")
                if isinstance(row.get("speci_shock_active"), bool)
                else None,
                speci_shock_confidence=_parse_float(row.get("speci_shock_confidence")),
                speci_shock_weight=_parse_float(row.get("speci_shock_weight")),
                speci_shock_mode=_normalize_text(row.get("speci_shock_mode")) or None,
                speci_shock_trigger_count=_parse_int(row.get("speci_shock_trigger_count")),
                speci_shock_cooldown_blocked=row.get("speci_shock_cooldown_blocked")
                if isinstance(row.get("speci_shock_cooldown_blocked"), bool)
                else None,
                speci_shock_improvement_hold_active=row.get("speci_shock_improvement_hold_active")
                if isinstance(row.get("speci_shock_improvement_hold_active"), bool)
                else None,
            )
        )
    return graded


def _dedupe_opportunities(
    *,
    rows: list[TemperatureOpportunity],
    key_fn,
    max_warning_entries: int = 200,
    include_warning_stats: bool = False,
) -> tuple[list[TemperatureOpportunity], dict[str, int], list[str]] | tuple[
    list[TemperatureOpportunity], dict[str, int], list[str], dict[str, int]
]:
    def _quality_score(item: TemperatureOpportunity) -> tuple[int, int, int, int, int]:
        return (
            1 if item.resolved else 0,
            1 if item.win is not None else 0,
            1 if _normalize_text(item.resolution_reason).lower() == "resolved" else 0,
            1 if _normalize_text(item.threshold_expression) else 0,
            1 if isinstance(item.final_truth_value, (int, float)) else 0,
        )

    sorted_rows = sorted(rows, key=lambda item: (item.planned_at, item.row_id))
    canonical: dict[str, TemperatureOpportunity] = {}
    duplicates_counter: dict[str, int] = {}
    warnings: list[str] = []
    warnings_total = 0
    warnings_truncated = 0
    exact_duplicates = 0
    non_exact_reuse = 0
    for row in sorted_rows:
        key = _normalize_text(key_fn(row))
        if not key:
            key = row.row_id
        existing = canonical.get(key)
        if existing is None:
            canonical[key] = row
            continue
        duplicates_counter[key] = int(duplicates_counter.get(key, 1) + 1)
        replace_existing = _quality_score(row) > _quality_score(existing)
        exact_duplicate = (
            existing.market_ticker == row.market_ticker
            and existing.side == row.side
            and existing.underlying_key == row.underlying_key
            and existing.resolved == row.resolved
            and existing.win == row.win
            and abs(existing.entry_price_dollars - row.entry_price_dollars) < 1e-9
            and abs(existing.base_pnl_dollars - row.base_pnl_dollars) < 1e-9
        )
        kept = row if replace_existing else existing
        dropped = existing if replace_existing else row
        if replace_existing:
            canonical[key] = row
        warnings_total += 1
        if exact_duplicate:
            exact_duplicates += 1
        else:
            non_exact_reuse += 1
        warning_text = (
            f"{'exact_duplicate' if exact_duplicate else 'non_exact_reuse'}"
            f"{'_replaced_existing_with_higher_quality' if replace_existing else ''}: key={key} "
            f"kept_row_id={kept.row_id} dropped_row_id={dropped.row_id}"
        )
        if len(warnings) < max(0, int(max_warning_entries)):
            warnings.append(warning_text)
        else:
            warnings_truncated += 1
    if warnings_truncated > 0:
        warnings.append(
            f"duplicate_warning_truncated: omitted={warnings_truncated} total={warnings_total} "
            f"max_warning_entries={max(0, int(max_warning_entries))}"
        )
    duplicates_counter = {key: count for key, count in sorted(duplicates_counter.items())}
    warning_stats = {
        "warnings_total": int(warnings_total),
        "warnings_emitted": int(len(warnings)),
        "warnings_truncated": int(warnings_truncated),
        "exact_duplicate_count": int(exact_duplicates),
        "non_exact_reuse_count": int(non_exact_reuse),
        "max_warning_entries": int(max(0, int(max_warning_entries))),
    }
    if include_warning_stats:
        return list(canonical.values()), duplicates_counter, warnings, warning_stats
    return list(canonical.values()), duplicates_counter, warnings


def _aggregate_underlying_family(rows: list[TemperatureOpportunity]) -> list[TemperatureOpportunity]:
    grouped: dict[str, list[TemperatureOpportunity]] = {}
    for row in rows:
        key = row.underlying_family_key or row.underlying_key or row.market_ticker
        grouped.setdefault(key, []).append(row)
    aggregated: list[TemperatureOpportunity] = []
    for family_key, family_rows in grouped.items():
        sorted_rows = sorted(family_rows, key=lambda item: (item.planned_at, item.row_id))
        first = sorted_rows[0]
        total_pnl = sum(item.base_pnl_dollars for item in sorted_rows)
        total_expected = sum(item.expected_edge_dollars for item in sorted_rows)
        total_cost = sum(item.estimated_entry_cost_dollars for item in sorted_rows)
        total_contracts = sum(item.contracts for item in sorted_rows)
        alpha_strength_avg = _average(
            [float(item.alpha_strength) for item in sorted_rows if isinstance(item.alpha_strength, (int, float))]
        )
        yes_gap_avg = _average(
            [float(item.yes_possible_gap) for item in sorted_rows if isinstance(item.yes_possible_gap, (int, float))]
        )
        yes_overlap_values = [item.yes_possible_overlap for item in sorted_rows if isinstance(item.yes_possible_overlap, bool)]
        yes_overlap_vote: bool | None = None
        if yes_overlap_values:
            yes_overlap_vote = (sum(1 for value in yes_overlap_values if value) / len(yes_overlap_values)) >= 0.5
        primary_margin_avg = _average(
            [
                float(item.primary_signal_margin)
                for item in sorted_rows
                if isinstance(item.primary_signal_margin, (int, float))
            ]
        )
        forecast_margin_avg = _average(
            [
                float(item.forecast_feasibility_margin)
                for item in sorted_rows
                if isinstance(item.forecast_feasibility_margin, (int, float))
            ]
        )
        shock_confidence_avg = _average(
            [
                float(item.speci_shock_confidence)
                for item in sorted_rows
                if isinstance(item.speci_shock_confidence, (int, float))
            ]
        )
        shock_weight_avg = _average(
            [
                float(item.speci_shock_weight)
                for item in sorted_rows
                if isinstance(item.speci_shock_weight, (int, float))
            ]
        )
        shock_active_values = [item.speci_shock_active for item in sorted_rows if isinstance(item.speci_shock_active, bool)]
        shock_active_vote: bool | None = None
        if shock_active_values:
            shock_active_vote = (sum(1 for value in shock_active_values if value) / len(shock_active_values)) >= 0.5
        shock_cooldown_values = [
            item.speci_shock_cooldown_blocked
            for item in sorted_rows
            if isinstance(item.speci_shock_cooldown_blocked, bool)
        ]
        shock_cooldown_vote: bool | None = None
        if shock_cooldown_values:
            shock_cooldown_vote = (sum(1 for value in shock_cooldown_values if value) / len(shock_cooldown_values)) >= 0.5
        shock_improvement_values = [
            item.speci_shock_improvement_hold_active
            for item in sorted_rows
            if isinstance(item.speci_shock_improvement_hold_active, bool)
        ]
        shock_improvement_vote: bool | None = None
        if shock_improvement_values:
            shock_improvement_vote = (
                sum(1 for value in shock_improvement_values if value) / len(shock_improvement_values)
            ) >= 0.5
        shock_trigger_counts = [
            int(item.speci_shock_trigger_count)
            for item in sorted_rows
            if isinstance(item.speci_shock_trigger_count, int)
        ]
        win = total_pnl > 0
        push = abs(total_pnl) < 1e-9
        outcome = "push" if push else ("win" if win else "loss")
        aggregated.append(
            TemperatureOpportunity(
                row_id=f"family:{family_key}",
                planned_at=min(item.planned_at for item in sorted_rows),
                close_time=max(item.close_time for item in sorted_rows),
                intent_id=first.intent_id,
                shadow_order_id=f"family:{family_key}",
                market_ticker=first.market_ticker,
                side=first.side,
                market_side_key=f"family:{family_key}",
                underlying_key=first.underlying_key,
                underlying_family_key=family_key,
                city_day_key=first.city_day_key,
                settlement_station=first.settlement_station,
                local_hour_key=first.local_hour_key,
                signal_type=first.signal_type,
                policy_reason=first.policy_reason,
                policy_version=first.policy_version,
                contracts=total_contracts,
                entry_price_dollars=first.entry_price_dollars,
                expected_edge_dollars=round(total_expected, 6),
                estimated_entry_cost_dollars=round(total_cost, 6),
                resolved=True,
                win=win,
                push=push,
                outcome=outcome,
                base_pnl_dollars=round(total_pnl, 6),
                threshold_expression=first.threshold_expression,
                final_truth_value=first.final_truth_value,
                resolution_reason="resolved",
                alpha_strength=alpha_strength_avg,
                yes_possible_gap=yes_gap_avg,
                yes_possible_overlap=yes_overlap_vote,
                primary_signal_margin=primary_margin_avg,
                forecast_feasibility_margin=forecast_margin_avg,
                speci_shock_active=shock_active_vote,
                speci_shock_confidence=shock_confidence_avg,
                speci_shock_weight=shock_weight_avg,
                speci_shock_mode=first.speci_shock_mode,
                speci_shock_trigger_count=int(round(sum(shock_trigger_counts) / len(shock_trigger_counts)))
                if shock_trigger_counts
                else None,
                speci_shock_cooldown_blocked=shock_cooldown_vote,
                speci_shock_improvement_hold_active=shock_improvement_vote,
            )
        )
    return aggregated


def _average_unique_per_day(rows: list[TemperatureOpportunity], key_fn) -> float | None:
    by_day: dict[str, set[str]] = {}
    for row in rows:
        if not row.resolved:
            continue
        value_key = _normalize_text(key_fn(row))
        if not value_key:
            continue
        day_key = ""
        city_day_key = _normalize_text(row.city_day_key)
        if "|" in city_day_key:
            _, _, maybe_day = city_day_key.rpartition("|")
            day_key = _normalize_text(maybe_day)
        if not day_key and isinstance(row.planned_at, datetime):
            day_key = row.planned_at.astimezone(timezone.utc).date().isoformat()
        if not day_key:
            continue
        by_day.setdefault(day_key, set()).add(value_key)
    if not by_day:
        return None
    counts = [len(values) for values in by_day.values()]
    if not counts:
        return None
    return round(sum(counts) / len(counts), 6)


def _policy_version_profile(rows: list[TemperatureOpportunity]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    missing = 0
    for row in rows:
        version = _normalize_text(getattr(row, "policy_version", ""))
        if version:
            counts[version] = int(counts.get(version, 0) + 1)
        else:
            missing += 1
    sorted_counts = dict(sorted(counts.items(), key=lambda item: (-int(item[1]), str(item[0]))))
    return {
        "counts": sorted_counts,
        "missing": int(missing),
        "total": int(len(rows)),
        "mixed_policy_versions": bool(len(sorted_counts) > 1),
        "dominant_policy_version": next(iter(sorted_counts.keys()), ""),
    }


def _model_lineage_summary(
    *,
    graded_rows: list[TemperatureOpportunity],
    unique_shadow_rows: list[TemperatureOpportunity],
    unique_market_side_rows: list[TemperatureOpportunity],
    underlying_family_rows: list[TemperatureOpportunity],
) -> dict[str, Any]:
    planned_profile = _policy_version_profile(graded_rows)
    resolved_profile = _policy_version_profile([row for row in graded_rows if row.resolved])
    unique_shadow_profile = _policy_version_profile([row for row in unique_shadow_rows if row.resolved])
    unique_market_side_profile = _policy_version_profile([row for row in unique_market_side_rows if row.resolved])
    underlying_family_profile = _policy_version_profile([row for row in underlying_family_rows if row.resolved])
    warnings: list[str] = []
    if bool(planned_profile.get("mixed_policy_versions")):
        warnings.append("mixed_policy_versions_planned_rows")
    if bool(resolved_profile.get("mixed_policy_versions")):
        warnings.append("mixed_policy_versions_resolved_rows")
    if int(planned_profile.get("missing") or 0) > 0:
        warnings.append("planned_rows_missing_policy_version")
    if int(resolved_profile.get("missing") or 0) > 0:
        warnings.append("resolved_rows_missing_policy_version")
    return {
        "planned_rows": planned_profile,
        "resolved_rows": resolved_profile,
        "unique_shadow_order": unique_shadow_profile,
        "unique_market_side": unique_market_side_profile,
        "underlying_family_aggregated": underlying_family_profile,
        "mixed_policy_versions_detected": bool(
            planned_profile.get("mixed_policy_versions")
            or resolved_profile.get("mixed_policy_versions")
            or unique_shadow_profile.get("mixed_policy_versions")
            or unique_market_side_profile.get("mixed_policy_versions")
            or underlying_family_profile.get("mixed_policy_versions")
        ),
        "warnings": warnings,
    }


def _opportunity_breadth(
    *,
    graded_rows: list[TemperatureOpportunity],
    unique_shadow_rows: list[TemperatureOpportunity],
    unique_market_side_rows: list[TemperatureOpportunity],
    underlying_family_rows: list[TemperatureOpportunity],
) -> dict[str, Any]:
    resolved_rows = [row for row in graded_rows if row.resolved]
    unresolved_rows = [row for row in graded_rows if not row.resolved]
    unresolved_market_sides = {
        row.market_side_key for row in unresolved_rows if _normalize_text(row.market_side_key)
    }
    unresolved_families = {
        row.underlying_family_key for row in unresolved_rows if _normalize_text(row.underlying_family_key)
    }
    resolved_market_side_count = len([row for row in unique_market_side_rows if row.resolved])
    repeated_entry_multiplier = (
        round(len(resolved_rows) / resolved_market_side_count, 6)
        if resolved_market_side_count > 0
        else None
    )
    return {
        "resolved_planned_rows": len(resolved_rows),
        "resolved_unique_shadow_orders": len([row for row in unique_shadow_rows if row.resolved]),
        "resolved_unique_market_sides": resolved_market_side_count,
        "resolved_unique_underlying_families": len([row for row in underlying_family_rows if row.resolved]),
        "average_unique_market_sides_per_day": _average_unique_per_day(unique_market_side_rows, lambda row: row.market_side_key),
        "average_unique_underlying_families_per_day": _average_unique_per_day(
            underlying_family_rows,
            lambda row: row.underlying_family_key,
        ),
        "repeated_entry_multiplier": repeated_entry_multiplier,
        "unresolved_unique_market_sides": len(unresolved_market_sides),
        "unresolved_unique_underlying_families": len(unresolved_families),
    }


def _hit_rate_quality(rows: list[TemperatureOpportunity]) -> dict[str, Any]:
    resolved = [row for row in rows if row.resolved]
    wins = [row for row in resolved if row.outcome == "win"]
    losses = [row for row in resolved if row.outcome == "loss"]
    pushes = [row for row in resolved if row.outcome == "push"]
    gross_win = sum(row.base_pnl_dollars for row in wins)
    gross_loss_abs = abs(sum(row.base_pnl_dollars for row in losses))
    total_pnl = sum(row.base_pnl_dollars for row in resolved)
    trade_count = len(resolved)
    return {
        "wins": len(wins),
        "losses": len(losses),
        "pushes": len(pushes),
        "win_rate": round(len(wins) / trade_count, 6) if trade_count > 0 else None,
        "profit_factor": round(gross_win / gross_loss_abs, 6) if gross_loss_abs > 0 else None,
        "avg_win": round(gross_win / len(wins), 6) if wins else None,
        "avg_loss": round(sum(row.base_pnl_dollars for row in losses) / len(losses), 6) if losses else None,
        "expectancy_per_trade": round(total_pnl / trade_count, 6) if trade_count > 0 else None,
    }


def _alpha_feature_summary(rows: list[TemperatureOpportunity]) -> dict[str, Any]:
    resolved = [row for row in rows if row.resolved]
    alpha_strength_values = [
        float(row.alpha_strength) for row in resolved if isinstance(row.alpha_strength, (int, float))
    ]
    yes_gap_values = [
        float(row.yes_possible_gap) for row in resolved if isinstance(row.yes_possible_gap, (int, float))
    ]
    yes_overlap_values = [row.yes_possible_overlap for row in resolved if isinstance(row.yes_possible_overlap, bool)]
    primary_margin_values = [
        float(row.primary_signal_margin)
        for row in resolved
        if isinstance(row.primary_signal_margin, (int, float))
    ]
    forecast_margin_values = [
        float(row.forecast_feasibility_margin)
        for row in resolved
        if isinstance(row.forecast_feasibility_margin, (int, float))
    ]
    shock_confidence_values = [
        float(row.speci_shock_confidence)
        for row in resolved
        if isinstance(row.speci_shock_confidence, (int, float))
    ]
    shock_weight_values = [
        float(row.speci_shock_weight)
        for row in resolved
        if isinstance(row.speci_shock_weight, (int, float))
    ]
    shock_active_values = [row.speci_shock_active for row in resolved if isinstance(row.speci_shock_active, bool)]
    shock_cooldown_values = [
        row.speci_shock_cooldown_blocked
        for row in resolved
        if isinstance(row.speci_shock_cooldown_blocked, bool)
    ]
    shock_improvement_values = [
        row.speci_shock_improvement_hold_active
        for row in resolved
        if isinstance(row.speci_shock_improvement_hold_active, bool)
    ]
    return {
        "alpha_strength_avg": round(_average(alpha_strength_values), 6)
        if _average(alpha_strength_values) is not None
        else None,
        "yes_possible_gap_avg": round(_average(yes_gap_values), 6)
        if _average(yes_gap_values) is not None
        else None,
        "yes_possible_overlap_rate": round(
            sum(1 for value in yes_overlap_values if value) / len(yes_overlap_values),
            6,
        )
        if yes_overlap_values
        else None,
        "primary_signal_margin_avg": round(_average(primary_margin_values), 6)
        if _average(primary_margin_values) is not None
        else None,
        "primary_signal_margin_abs_avg": round(
            _average([abs(value) for value in primary_margin_values]),
            6,
        )
        if primary_margin_values
        else None,
        "forecast_feasibility_margin_avg": round(_average(forecast_margin_values), 6)
        if _average(forecast_margin_values) is not None
        else None,
        "speci_shock_confidence_avg": round(_average(shock_confidence_values), 6)
        if _average(shock_confidence_values) is not None
        else None,
        "speci_shock_weight_avg": round(_average(shock_weight_values), 6)
        if _average(shock_weight_values) is not None
        else None,
        "speci_shock_active_rate": round(
            sum(1 for value in shock_active_values if value) / len(shock_active_values),
            6,
        )
        if shock_active_values
        else None,
        "speci_shock_cooldown_blocked_rate": round(
            sum(1 for value in shock_cooldown_values if value) / len(shock_cooldown_values),
            6,
        )
        if shock_cooldown_values
        else None,
        "speci_shock_improvement_hold_rate": round(
            sum(1 for value in shock_improvement_values if value) / len(shock_improvement_values),
            6,
        )
        if shock_improvement_values
        else None,
    }


def _build_expected_vs_shadow_settled(layer_rows: dict[str, list[TemperatureOpportunity]]) -> dict[str, Any]:
    by_layer: dict[str, dict[str, Any]] = {}
    for layer_name, rows in layer_rows.items():
        resolved = [row for row in rows if row.resolved]
        expected_total = sum(row.expected_edge_dollars for row in resolved)
        pnl_total = sum(row.base_pnl_dollars for row in resolved)
        trade_count = len(resolved)
        by_layer[layer_name] = {
            "expected_shadow_edge_total": round(expected_total, 6),
            "shadow_settled_pnl": round(pnl_total, 6),
            "delta_total": round(pnl_total - expected_total, 6),
            "delta_per_trade": round((pnl_total - expected_total) / trade_count, 6) if trade_count > 0 else None,
            "calibration_ratio": round(pnl_total / expected_total, 6) if expected_total > 0 else None,
            "trade_count": trade_count,
        }

    default_layer = "unique_market_side"
    default_payload = by_layer.get(default_layer, {})
    return {
        "basis": default_layer,
        "expected_shadow_edge_total": default_payload.get("expected_shadow_edge_total", 0.0),
        "shadow_settled_pnl": default_payload.get("shadow_settled_pnl", 0.0),
        "delta_total": default_payload.get("delta_total", 0.0),
        "delta_per_trade": default_payload.get("delta_per_trade"),
        "calibration_ratio": default_payload.get("calibration_ratio"),
        "by_aggregation_layer": by_layer,
    }


def _model_target_stake(
    *,
    model_name: str,
    model_config: dict[str, Any],
    trade: TemperatureOpportunity,
    equity: float,
    reference_bankroll: float,
) -> float:
    risk_per_trade_pct = _fraction(model_config.get("risk_per_trade_pct"), 0.02)
    if model_name in {"fixed_fraction_per_unique_market_side", "fixed_fraction_per_underlying_family"}:
        return equity * risk_per_trade_pct
    if model_name == "fixed_unit_risk_budget":
        configured = _safe_positive(model_config.get("unit_risk_dollars"), reference_bankroll * risk_per_trade_pct)
        return configured
    if model_name == "capped_fractional_kelly_shadow":
        contracts = max(1e-9, float(trade.contracts or 1.0))
        edge_per_contract = float(trade.expected_edge_dollars or 0.0) / contracts
        p_est = max(0.001, min(0.999, trade.entry_price_dollars + edge_per_contract))
        denom = max(1e-6, 1.0 - trade.entry_price_dollars)
        kelly_fraction = max(0.0, min(1.0, (p_est - trade.entry_price_dollars) / denom))
        multiplier = _safe_positive(model_config.get("kelly_fraction_multiplier"), 0.5)
        effective_fraction = min(risk_per_trade_pct, kelly_fraction * multiplier)
        return equity * effective_fraction
    return equity * risk_per_trade_pct


def _simulate_bankroll_for_layer(
    *,
    trades: list[TemperatureOpportunity],
    model_name: str,
    model_config: dict[str, Any],
    reference_bankroll_dollars: float,
    slippage_bps: float,
    fee_model: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sorted_trades = sorted([trade for trade in trades if trade.resolved], key=lambda item: (item.planned_at, item.row_id))

    cash = float(reference_bankroll_dollars)
    open_positions: list[dict[str, Any]] = []
    deployed_by_family: dict[str, float] = {}
    deployed_by_city_day: dict[str, float] = {}
    deployed_by_station: dict[str, float] = {}
    deployed_by_hour: dict[str, float] = {}
    open_market_sides: set[str] = set()

    entry_fee_rate = _fraction(fee_model.get("entry_fee_rate"), 0.0)
    exit_fee_rate = _fraction(fee_model.get("exit_fee_rate"), 0.0)
    fixed_fee_per_trade = _safe_positive(fee_model.get("fixed_fee_per_trade"), 0.0)

    max_pct_total_deployed = _fraction(model_config.get("max_pct_total_deployed"), 0.5)
    max_pct_per_family = _fraction(model_config.get("max_pct_per_underlying_family"), 0.15)
    max_pct_per_city_day = _fraction(model_config.get("max_pct_per_city_day"), 0.2)
    max_pct_same_station = _fraction(model_config.get("max_same_station_exposure_pct"), 0.25)
    max_pct_same_hour = _fraction(model_config.get("max_same_hour_cluster_pct"), 0.3)
    max_simultaneous_market_sides = _safe_non_negative_int(model_config.get("max_simultaneous_market_sides"), 10)

    executed_rows: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    deployed_turnover = 0.0
    utilization_samples: list[float] = []
    equity_samples: list[float] = [cash]

    seen_market_sides_for_model: set[str] = set()
    seen_underlying_families_for_model: set[str] = set()

    def _current_deployed() -> float:
        return sum(float(position.get("stake") or 0.0) for position in open_positions)

    def _remove_position(position: dict[str, Any]) -> None:
        family_key = _normalize_text(position.get("underlying_family_key"))
        city_day_key = _normalize_text(position.get("city_day_key"))
        station_key = _normalize_text(position.get("settlement_station"))
        hour_key = _normalize_text(position.get("local_hour_key"))
        stake = float(position.get("stake") or 0.0)

        if family_key:
            deployed_by_family[family_key] = max(0.0, float(deployed_by_family.get(family_key, 0.0)) - stake)
        if city_day_key:
            deployed_by_city_day[city_day_key] = max(0.0, float(deployed_by_city_day.get(city_day_key, 0.0)) - stake)
        if station_key:
            deployed_by_station[station_key] = max(0.0, float(deployed_by_station.get(station_key, 0.0)) - stake)
        if hour_key:
            deployed_by_hour[hour_key] = max(0.0, float(deployed_by_hour.get(hour_key, 0.0)) - stake)

    def _settle_positions(until_time: datetime) -> None:
        nonlocal cash, open_market_sides
        remaining: list[dict[str, Any]] = []
        for position in open_positions:
            close_time = position.get("close_time")
            if isinstance(close_time, datetime) and close_time <= until_time:
                stake = float(position.get("stake") or 0.0)
                base_pnl = float(position.get("base_pnl") or 0.0)
                entry_fee = float(position.get("entry_fee") or 0.0)
                exit_fee = float(position.get("exit_fee") or 0.0)
                net_pnl = base_pnl - entry_fee - exit_fee
                cash += stake + base_pnl - exit_fee
                close_day = close_time.date().isoformat()
                daily_pnl[close_day] = float(daily_pnl.get(close_day, 0.0) + net_pnl)
                _remove_position(position)
            else:
                remaining.append(position)
        open_positions[:] = remaining
        # Rebuild from remaining positions to avoid stale locks when multiple
        # positions on the same market-side settle in one sweep.
        open_market_sides = {
            _normalize_text(item.get("market_side_key"))
            for item in open_positions
            if _normalize_text(item.get("market_side_key"))
        }
        deployed = _current_deployed()
        utilization_samples.append(deployed / reference_bankroll_dollars if reference_bankroll_dollars > 0 else 0.0)
        equity_samples.append(cash + deployed)

    for trade in sorted_trades:
        _settle_positions(trade.planned_at)
        deployed_before = _current_deployed()
        equity = cash + deployed_before

        if model_name == "fixed_fraction_per_unique_market_side" and trade.market_side_key in seen_market_sides_for_model:
            continue
        if model_name == "fixed_fraction_per_underlying_family" and trade.underlying_family_key in seen_underlying_families_for_model:
            continue

        if max_simultaneous_market_sides > 0 and trade.market_side_key not in open_market_sides:
            if len(open_market_sides) >= max_simultaneous_market_sides:
                continue

        target_stake = _model_target_stake(
            model_name=model_name,
            model_config=model_config,
            trade=trade,
            equity=max(0.0, equity),
            reference_bankroll=reference_bankroll_dollars,
        )
        if target_stake <= 0:
            continue

        max_total = max_pct_total_deployed * max(0.0, equity)
        max_family = max_pct_per_family * max(0.0, equity)
        max_city_day = max_pct_per_city_day * max(0.0, equity)
        max_station = max_pct_same_station * max(0.0, equity)
        max_hour = max_pct_same_hour * max(0.0, equity)

        avail_total = max(0.0, max_total - deployed_before)
        avail_family = max(0.0, max_family - float(deployed_by_family.get(trade.underlying_family_key, 0.0)))
        avail_city_day = max(0.0, max_city_day - float(deployed_by_city_day.get(trade.city_day_key, 0.0)))
        avail_station = max(0.0, max_station - float(deployed_by_station.get(trade.settlement_station, 0.0)))
        avail_hour = max(0.0, max_hour - float(deployed_by_hour.get(trade.local_hour_key, 0.0)))

        candidate_stake = min(target_stake, avail_total, avail_family, avail_city_day, avail_station, avail_hour)
        if candidate_stake <= 0:
            continue

        effective_entry_price = max(0.01, min(0.99, trade.entry_price_dollars * (1.0 + (slippage_bps / 10000.0))))
        entry_fee = (candidate_stake * entry_fee_rate) + fixed_fee_per_trade
        total_open_outflow = candidate_stake + entry_fee
        if total_open_outflow > cash and total_open_outflow > 0:
            scale = max(0.0, cash - fixed_fee_per_trade)
            candidate_stake = scale / max(1e-9, 1.0 + entry_fee_rate)
            if candidate_stake <= 0:
                continue
            entry_fee = (candidate_stake * entry_fee_rate) + fixed_fee_per_trade
            total_open_outflow = candidate_stake + entry_fee
            if total_open_outflow > cash:
                continue

        contracts = candidate_stake / max(1e-9, effective_entry_price)
        if trade.outcome == "win":
            base_pnl = contracts * (1.0 - effective_entry_price)
        elif trade.outcome == "loss":
            base_pnl = -candidate_stake
        else:
            base_pnl = 0.0
        exit_fee = candidate_stake * exit_fee_rate
        net_pnl = base_pnl - entry_fee - exit_fee

        cash -= total_open_outflow
        position = {
            "close_time": trade.close_time,
            "stake": candidate_stake,
            "base_pnl": base_pnl,
            "entry_fee": entry_fee,
            "exit_fee": exit_fee,
            "underlying_family_key": trade.underlying_family_key,
            "city_day_key": trade.city_day_key,
            "settlement_station": trade.settlement_station,
            "local_hour_key": trade.local_hour_key,
            "market_side_key": trade.market_side_key,
        }
        open_positions.append(position)
        deployed_by_family[trade.underlying_family_key] = float(deployed_by_family.get(trade.underlying_family_key, 0.0) + candidate_stake)
        deployed_by_city_day[trade.city_day_key] = float(deployed_by_city_day.get(trade.city_day_key, 0.0) + candidate_stake)
        deployed_by_station[trade.settlement_station] = float(deployed_by_station.get(trade.settlement_station, 0.0) + candidate_stake)
        deployed_by_hour[trade.local_hour_key] = float(deployed_by_hour.get(trade.local_hour_key, 0.0) + candidate_stake)
        open_market_sides.add(trade.market_side_key)

        deployed_turnover += candidate_stake
        seen_market_sides_for_model.add(trade.market_side_key)
        seen_underlying_families_for_model.add(trade.underlying_family_key)

        executed_rows.append(
            {
                "trade_id": trade.row_id,
                "market_side_key": trade.market_side_key,
                "underlying_family_key": trade.underlying_family_key,
                "city_day_key": trade.city_day_key,
                "settlement_station": trade.settlement_station,
                "local_hour_key": trade.local_hour_key,
                "signal_type": trade.signal_type,
                "policy_reason": trade.policy_reason,
                "stake": round(candidate_stake, 6),
                "entry_price": round(effective_entry_price, 6),
                "base_pnl": round(base_pnl, 6),
                "entry_fee": round(entry_fee, 6),
                "exit_fee": round(exit_fee, 6),
                "net_pnl": round(net_pnl, 6),
                "outcome": trade.outcome,
                "planned_at": trade.planned_at.isoformat(),
                "close_time": trade.close_time.isoformat(),
            }
        )

        deployed_after = _current_deployed()
        utilization_samples.append(deployed_after / reference_bankroll_dollars if reference_bankroll_dollars > 0 else 0.0)
        equity_samples.append(cash + deployed_after)

    _settle_positions(datetime.max.replace(tzinfo=timezone.utc))

    ending_balance = cash
    pnl_total = ending_balance - reference_bankroll_dollars
    max_drawdown = 0.0
    running_peak = equity_samples[0] if equity_samples else reference_bankroll_dollars
    for equity in equity_samples:
        running_peak = max(running_peak, equity)
        if running_peak > 0:
            drawdown = (running_peak - equity) / running_peak
            max_drawdown = max(max_drawdown, drawdown)

    best_window_pnl = max(daily_pnl.values()) if daily_pnl else 0.0
    worst_window_pnl = min(daily_pnl.values()) if daily_pnl else 0.0

    unique_market_side_count = len({row["market_side_key"] for row in executed_rows if _normalize_text(row.get("market_side_key"))})
    unique_underlying_family_count = len(
        {row["underlying_family_key"] for row in executed_rows if _normalize_text(row.get("underlying_family_key"))}
    )

    metrics = {
        "reference_bankroll": round(reference_bankroll_dollars, 6),
        "deployed_capital": round(deployed_turnover, 6),
        "ending_balance": round(ending_balance, 6),
        "pnl_total": round(pnl_total, 6),
        "roi_on_deployed_capital": round(pnl_total / deployed_turnover, 6) if deployed_turnover > 0 else None,
        "roi_on_reference_bankroll": round(pnl_total / reference_bankroll_dollars, 6) if reference_bankroll_dollars > 0 else None,
        "max_drawdown": round(max_drawdown, 6),
        "worst_window_pnl": round(worst_window_pnl, 6),
        "best_window_pnl": round(best_window_pnl, 6),
        "trade_count": len(executed_rows),
        "unique_market_side_count": unique_market_side_count,
        "unique_underlying_family_count": unique_underlying_family_count,
        "exposure_utilization_avg": round(sum(utilization_samples) / len(utilization_samples), 6)
        if utilization_samples
        else 0.0,
        "exposure_utilization_peak": round(max(utilization_samples), 6) if utilization_samples else 0.0,
    }
    return metrics, executed_rows


def _build_attribution(executed_rows: list[dict[str, Any]], group_key: str, top_n: int) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in executed_rows:
        key = _normalize_text(row.get(group_key)) or "unknown"
        bucket = grouped.setdefault(
            key,
            {
                "key": key,
                "trade_count": 0,
                "pnl_total": 0.0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"] + 1)
        net_pnl = float(row.get("net_pnl") or 0.0)
        bucket["pnl_total"] = float(bucket["pnl_total"] + net_pnl)
        outcome = _normalize_text(row.get("outcome")).lower()
        if outcome == "win":
            bucket["wins"] = int(bucket["wins"] + 1)
        elif outcome == "loss":
            bucket["losses"] = int(bucket["losses"] + 1)
        else:
            bucket["pushes"] = int(bucket["pushes"] + 1)

    for bucket in grouped.values():
        count = int(bucket.get("trade_count") or 0)
        bucket["win_rate"] = round((int(bucket.get("wins") or 0) / count), 6) if count > 0 else None
        bucket["pnl_total"] = round(float(bucket.get("pnl_total") or 0.0), 6)

    rows = sorted(grouped.values(), key=lambda item: float(item.get("pnl_total") or 0.0), reverse=True)
    worst = list(reversed(rows))
    limit = max(1, int(top_n))
    return {
        "best": rows[:limit],
        "worst": worst[:limit],
    }


def _concentration_summary(resolved_rows: list[TemperatureOpportunity], resolved_unique_market_sides: int) -> tuple[bool, dict[str, Any], str]:
    if not resolved_rows:
        return False, {"resolved_planned_rows": 0, "resolved_unique_market_sides": 0, "top_market_side_share": 0.0}, "no_resolved_rows"

    counts: dict[str, int] = {}
    for row in resolved_rows:
        key = row.market_side_key
        counts[key] = int(counts.get(key, 0) + 1)
    rows_total = len(resolved_rows)
    sorted_counts = sorted(counts.values(), reverse=True)
    top_share = sorted_counts[0] / rows_total if sorted_counts else 0.0
    collapse_share = 1.0 - (resolved_unique_market_sides / rows_total) if rows_total > 0 else 0.0
    warning = bool(resolved_unique_market_sides < 5 and collapse_share > 0.2)
    summary = (
        "resolved rows are heavily concentrated into fewer than 5 unique market-side outcomes; "
        "apparent hit-rate may overstate independent alpha breadth"
        if warning
        else "concentration within acceptable bounds"
    )
    return (
        warning,
        {
            "resolved_planned_rows": rows_total,
            "resolved_unique_market_sides": resolved_unique_market_sides,
            "top_market_side_share": round(top_share, 6),
            "collapse_share": round(collapse_share, 6),
        },
        summary,
    )


def _infer_main_limiting_factor(
    *,
    policy_reason_counts: dict[str, int],
    concentration_warning: bool,
    opportunity_breadth: dict[str, Any],
    current_settlement_unresolved: int | None = None,
    mixed_policy_versions_detected: bool = False,
) -> str:
    stale = 0
    cutoff = 0
    finalization = 0
    breadth_reason_score = 0
    for reason, count in policy_reason_counts.items():
        reason_lower = _normalize_text(reason).lower()
        normalized_count = int(count)
        if "stale" in reason_lower:
            stale += normalized_count
        if "cutoff" in reason_lower:
            cutoff += normalized_count
        if "final" in reason_lower or "pending_final" in reason_lower:
            finalization += normalized_count
        if any(
            token in reason_lower
            for token in (
                "overlap",
                "still_possible",
                "range_possible",
                "range_still_possible",
                "no_side_interval_overlap",
                "family_conflict",
                "monotonic",
                "breadth",
                "concentration",
            )
        ):
            breadth_reason_score += normalized_count

    insufficient_breadth_score = 0
    unique_market_sides = int(opportunity_breadth.get("resolved_unique_market_sides") or 0)
    unresolved_unique_market_sides = int(opportunity_breadth.get("unresolved_unique_market_sides") or 0)
    repeated_multiplier = _parse_float(opportunity_breadth.get("repeated_entry_multiplier")) or 0.0
    insufficient_breadth_score += breadth_reason_score
    if unique_market_sides <= 0 and unresolved_unique_market_sides > 0:
        insufficient_breadth_score += 200
    if unique_market_sides < 5:
        insufficient_breadth_score += 100
    elif unique_market_sides < 10:
        insufficient_breadth_score += 40
    if unresolved_unique_market_sides > unique_market_sides and unresolved_unique_market_sides > 0:
        insufficient_breadth_score += 50
    if repeated_multiplier > 2.0:
        insufficient_breadth_score += 50
    if repeated_multiplier > 10.0:
        insufficient_breadth_score += 100
    if concentration_warning:
        insufficient_breadth_score += 100

    scores = {
        "stale_suppression": stale,
        "cutoff_timing": cutoff,
        "settlement_finalization": finalization,
        "insufficient_breadth": insufficient_breadth_score,
        "model_consistency": 200 if mixed_policy_versions_detected else 0,
    }
    if isinstance(current_settlement_unresolved, int) and current_settlement_unresolved <= 0:
        # Do not headline settlement finalization as the root limiter when
        # current unresolved settlement backlog is clear.
        scores["settlement_finalization"] = 0
    if all(value <= 0 for value in scores.values()):
        return "insufficient_breadth"
    return max(scores.items(), key=lambda item: item[1])[0]


def _next_missing_alpha_layer(limiting_factor: str) -> dict[str, str]:
    if limiting_factor == "model_consistency":
        return {
            "name": "model_version_isolated_calibration",
            "reason": "Mixed policy/model versions in one window reduce comparability; isolate calibration by model version before scaling risk.",
        }
    if limiting_factor == "stale_suppression":
        return {
            "name": "taf_remainder_of_day_path_modeling",
            "reason": "Stale-heavy suppression implies path-aware weather updates are the highest-impact next alpha layer.",
        }
    if limiting_factor == "cutoff_timing":
        return {
            "name": "execution_aware_portfolio_optimization",
            "reason": "Cutoff pressure favors urgency-aware sequencing, not only static per-trade rules.",
        }
    if limiting_factor == "settlement_finalization":
        return {
            "name": "cross_market_family_mispricing",
            "reason": "When finalization blocks dominate, broader family-relative opportunities are the best expansion.",
        }
    return {
        "name": "bracket_range_consistency",
        "reason": "Breadth/concentration constraints indicate missing range/family structure is the bottleneck.",
    }


def _build_growth_readiness_block(validation_payload: dict[str, Any]) -> dict[str, Any]:
    def _clamp01(value: Any) -> float:
        parsed = _parse_float(value)
        if parsed is None:
            return 0.0
        return max(0.0, min(1.0, parsed))

    def _append_blocker(
        blockers: list[dict[str, Any]],
        *,
        reason: str,
        score: float,
    ) -> None:
        if score <= 0.0:
            return
        blockers.append(
            {
                "reason": reason,
                "score": round(_clamp01(score), 6),
                "detail": _READINESS_BLOCKER_DETAILS.get(reason, "Readiness blocker detected."),
                "recommended_action": _READINESS_BLOCKER_ACTIONS.get(reason, "Gather more evidence before deploying."),
            }
        )

    viability = validation_payload.get("viability_summary") if isinstance(validation_payload.get("viability_summary"), dict) else {}
    breadth = validation_payload.get("opportunity_breadth") if isinstance(validation_payload.get("opportunity_breadth"), dict) else {}
    concentration = (
        validation_payload.get("concentration_checks")
        if isinstance(validation_payload.get("concentration_checks"), dict)
        else {}
    )
    data_quality = validation_payload.get("data_quality") if isinstance(validation_payload.get("data_quality"), dict) else {}
    expected_vs_shadow = (
        validation_payload.get("expected_vs_shadow_settled")
        if isinstance(validation_payload.get("expected_vs_shadow_settled"), dict)
        else {}
    )
    hit_rate_quality = (
        validation_payload.get("hit_rate_quality") if isinstance(validation_payload.get("hit_rate_quality"), dict) else {}
    )
    signal_evidence = (
        validation_payload.get("signal_evidence") if isinstance(validation_payload.get("signal_evidence"), dict) else {}
    )
    pipeline_health = (
        data_quality.get("pipeline_health") if isinstance(data_quality.get("pipeline_health"), dict) else {}
    )

    main_limiting_factor = _normalize_text(viability.get("main_limiting_factor")) or "stale_suppression"
    pipeline_status = (
        _normalize_text(pipeline_health.get("status"))
        or _normalize_text(data_quality.get("pipeline_status"))
        or "unknown"
    ).lower()
    missing_feeds = (
        pipeline_health.get("missing_feeds") if isinstance(pipeline_health.get("missing_feeds"), list) else []
    )
    stale_feeds = pipeline_health.get("stale_feeds") if isinstance(pipeline_health.get("stale_feeds"), list) else []
    out_of_window_feeds = (
        pipeline_health.get("out_of_window_feeds")
        if isinstance(pipeline_health.get("out_of_window_feeds"), list)
        else []
    )
    resolved_rows = int(breadth.get("resolved_planned_rows") or 0)
    resolved_unique_market_sides = int(breadth.get("resolved_unique_market_sides") or 0)
    resolved_unique_families = int(breadth.get("resolved_unique_underlying_families") or 0)
    unresolved_unique_market_sides = int(breadth.get("unresolved_unique_market_sides") or 0)
    repeated_entry_multiplier = _parse_float(breadth.get("repeated_entry_multiplier"))
    repeated_entry_multiplier_value = repeated_entry_multiplier if isinstance(repeated_entry_multiplier, float) else 0.0
    concentration_warning = bool(concentration.get("concentration_warning")) or bool(breadth.get("concentration_warning"))
    roi_on_reference_bankroll = _parse_float(viability.get("what_return_would_have_been_produced_on_bankroll")) or 0.0
    excess_return_over_hysa = _parse_float(viability.get("excess_return_over_hysa_for_window")) or 0.0
    equivalent_hysa_return = _parse_float(viability.get("equivalent_window_hysa_return_on_reference_bankroll")) or 0.0
    calibration_ratio = _parse_float(expected_vs_shadow.get("calibration_ratio"))
    calibration_trade_count = int(
        expected_vs_shadow.get("trade_count")
        or hit_rate_quality.get("trade_count")
        or hit_rate_quality.get("wins")
        or 0
    )
    calibration_trade_count = max(calibration_trade_count, resolved_rows)

    if pipeline_status == "green":
        data_freshness_score = 1.0
    elif pipeline_status == "yellow":
        data_freshness_score = 0.7
    elif pipeline_status == "red":
        data_freshness_score = 0.0
    else:
        data_freshness_score = 0.5
    data_freshness_score -= 0.08 * len(missing_feeds)
    data_freshness_score -= 0.07 * len(stale_feeds)
    data_freshness_score -= 0.05 * len(out_of_window_feeds)
    if main_limiting_factor == "stale_suppression":
        data_freshness_score = min(data_freshness_score, 0.35)
    data_freshness_score = _clamp01(data_freshness_score)

    market_side_score = _clamp01(resolved_unique_market_sides / 20.0)
    family_score = _clamp01(resolved_unique_families / 5.0)
    trade_volume_score = _clamp01(resolved_rows / 15.0)
    throughput_score = 0.55 * market_side_score + 0.25 * family_score + 0.20 * trade_volume_score
    if resolved_rows <= 0:
        throughput_score = 0.0
    if concentration_warning:
        throughput_score *= 0.85
    throughput_score = _clamp01(throughput_score)

    if resolved_rows <= 0:
        edge_quality_score = 0.0
    else:
        roi_component = _clamp01(max(0.0, roi_on_reference_bankroll) / 0.05)
        hysa_component = _clamp01(max(0.0, excess_return_over_hysa) / max(1.0, equivalent_hysa_return or 1.0))
        edge_quality_score = _clamp01((0.7 * roi_component) + (0.3 * hysa_component))
        if concentration_warning:
            edge_quality_score *= 0.9
        edge_quality_score = _clamp01(edge_quality_score)

    if calibration_ratio is None or calibration_ratio <= 0.0 or calibration_trade_count <= 0:
        calibration_score = 0.0
    else:
        ratio_fit = 1.0 / (1.0 + abs(calibration_ratio - 1.0))
        sample_fit = _clamp01(calibration_trade_count / 4.0)
        calibration_score = _clamp01(ratio_fit * sample_fit)

    readiness_score = _clamp01(
        (0.30 * throughput_score)
        + (0.30 * edge_quality_score)
        + (0.20 * data_freshness_score)
        + (0.20 * calibration_score)
    )

    blockers: list[dict[str, Any]] = []
    if main_limiting_factor == "stale_suppression":
        _append_blocker(blockers, reason="stale_suppression", score=1.0)
    elif pipeline_status in {"red", "yellow"} or missing_feeds or stale_feeds or out_of_window_feeds:
        _append_blocker(
            blockers,
            reason="pipeline_data_stale_or_missing",
            score=max(0.0, 1.0 - data_freshness_score),
        )

    if resolved_rows <= 0 or resolved_unique_market_sides <= 0:
        _append_blocker(blockers, reason="no_resolved_outcomes", score=0.98)
    else:
        market_side_gap = 1.0 - _clamp01(resolved_unique_market_sides / 5.0)
        if (
            resolved_unique_market_sides < 5
            or unresolved_unique_market_sides > resolved_unique_market_sides
            or repeated_entry_multiplier_value > 2.0
        ):
            market_side_severity = max(
                market_side_gap,
                _clamp01(unresolved_unique_market_sides / 10.0),
                _clamp01((repeated_entry_multiplier_value - 1.0) / 4.0),
            )
            _append_blocker(
                blockers,
                reason="insufficient_independent_market_side_breadth",
                score=market_side_severity,
            )
        if resolved_unique_families < 2:
            family_gap = 1.0 - _clamp01(resolved_unique_families / 2.0)
            _append_blocker(
                blockers,
                reason="insufficient_underlying_family_breadth",
                score=family_gap,
            )
        if concentration_warning:
            concentration_severity = max(
                0.55,
                _clamp01(1.0 - max(market_side_score, family_score)),
            )
            _append_blocker(blockers, reason="concentration_warning", score=concentration_severity)

    if resolved_rows > 0:
        if roi_on_reference_bankroll <= 0.0:
            _append_blocker(
                blockers,
                reason="non_positive_roi_on_reference_bankroll",
                score=0.8,
            )
        elif excess_return_over_hysa <= 0.0:
            _append_blocker(
                blockers,
                reason="does_not_exceed_hysa_for_window",
                score=0.72,
            )
        if calibration_score < 0.45:
            _append_blocker(
                blockers,
                reason="non_positive_calibration_ratio",
                score=max(0.45, 1.0 - calibration_score),
            )

    blockers = sorted(blockers, key=lambda item: (-float(item.get("score") or 0.0), str(item.get("reason") or "")))

    return {
        "readiness_score": round(readiness_score, 6),
        "throughput_score": round(throughput_score, 6),
        "edge_quality_score": round(edge_quality_score, 6),
        "data_freshness_score": round(data_freshness_score, 6),
        "calibration_score": round(calibration_score, 6),
        "top_blockers": blockers[:5],
    }


def _build_validation_payload(
    *,
    output_dir: str,
    hours: float,
    reference_bankroll_dollars: float,
    sizing_models_json: str | None,
    slippage_bps_list: str | None,
    fee_model_json: str | None,
    top_n: int,
    simulation_model_names: set[str] | None = None,
    simulation_layer_names: set[str] | None = None,
    simulation_slippage_bps: list[float] | None = None,
    include_simulation_scenarios: bool = True,
    include_attribution: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)

    lookback_hours = max(0.0, float(hours))
    window_start = captured_at - timedelta(hours=lookback_hours)
    start_epoch = window_start.timestamp()
    end_epoch = captured_at.timestamp()

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sizing_models = _resolve_sizing_models(
        sizing_models_json=sizing_models_json,
        reference_bankroll_dollars=reference_bankroll_dollars,
    )
    slippage_list = _resolve_slippage_bps_list(slippage_bps_list)
    fee_model = _resolve_fee_model(fee_model_json)

    intent_context, policy_reason_counts = _intent_context_in_window(out_dir, start_epoch, end_epoch)
    raw_plan_rows, expected_shadow_edge_total, expected_shadow_cost_total = _plan_rows_in_window(
        out_dir=out_dir,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        intent_context=intent_context,
    )
    observed_span_hours = _observed_span_hours(raw_plan_rows)
    effective_window_hours_for_metrics = min(lookback_hours, observed_span_hours) if observed_span_hours > 0 else 0.0
    effective_window_hours_for_metrics = max(0.0, effective_window_hours_for_metrics)
    effective_window_days_for_metrics = effective_window_hours_for_metrics / 24.0 if effective_window_hours_for_metrics > 0 else 0.0
    data_coverage_ratio = min(1.0, effective_window_hours_for_metrics / lookback_hours) if lookback_hours > 0 else 0.0

    needed_tickers = {
        _normalize_text(row.get("market_ticker"))
        for row in raw_plan_rows
        if _normalize_text(row.get("market_ticker"))
    }
    threshold_by_ticker = _threshold_map(out_dir, end_epoch, needed_tickers)
    needed_underlyings = {
        _normalize_text(row.get("underlying_key"))
        for row in raw_plan_rows
        if _normalize_text(row.get("underlying_key"))
    }
    truth_by_underlying, settlement_state_file, settlement_state_files_scanned = _latest_settlement_truth_map(
        out_dir,
        end_epoch,
        needed_underlyings=needed_underlyings,
    )
    constraint_summary_file, constraint_summary_in_window = _latest_file_preferring_window(
        out_dir,
        "kalshi_temperature_constraint_scan_summary_*.json",
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )
    constraint_summary_payload = _load_json(constraint_summary_file) if constraint_summary_file is not None else {}
    consistency_checks = (
        constraint_summary_payload.get("consistency_checks")
        if isinstance(constraint_summary_payload.get("consistency_checks"), dict)
        else {}
    )
    neighboring_summary = (
        consistency_checks.get("neighboring_strike_monotonicity")
        if isinstance(consistency_checks.get("neighboring_strike_monotonicity"), dict)
        else consistency_checks
    )
    exact_chain_summary = (
        consistency_checks.get("exact_strike_impossibility_chains")
        if isinstance(consistency_checks.get("exact_strike_impossibility_chains"), dict)
        else {}
    )
    range_family_summary = (
        consistency_checks.get("range_family_consistency")
        if isinstance(consistency_checks.get("range_family_consistency"), dict)
        else {}
    )
    cross_market_summary = (
        consistency_checks.get("cross_market_family_mispricing")
        if isinstance(consistency_checks.get("cross_market_family_mispricing"), dict)
        else {}
    )
    trade_plan_summary_file, trade_plan_summary_in_window = _latest_file_preferring_window(
        out_dir,
        "kalshi_temperature_trade_plan_summary_*.json",
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )
    trade_plan_summary_payload = _load_json(trade_plan_summary_file) if trade_plan_summary_file is not None else {}
    allocation_summary = (
        trade_plan_summary_payload.get("allocation_summary")
        if isinstance(trade_plan_summary_payload.get("allocation_summary"), dict)
        else {}
    )
    settlement_backlog_now = _current_settlement_backlog_snapshot(settlement_state_file)

    graded_rows = _grade_plan_rows(raw_plan_rows, threshold_by_ticker, truth_by_underlying)
    resolved_rows = [row for row in graded_rows if row.resolved]

    unique_shadow_rows, duplicate_shadow_ids, duplicate_warnings, duplicate_warning_stats = _dedupe_opportunities(
        rows=graded_rows,
        key_fn=lambda row: row.shadow_order_id,
        include_warning_stats=True,
    )
    duplicate_shadow_ids_top_n = 250
    duplicate_shadow_ids_total_unique = len(duplicate_shadow_ids)
    duplicate_shadow_ids_top = {
        key: int(count)
        for key, count in sorted(
            duplicate_shadow_ids.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:duplicate_shadow_ids_top_n]
    }
    duplicate_shadow_ids_truncated_count = max(0, duplicate_shadow_ids_total_unique - len(duplicate_shadow_ids_top))
    unique_market_side_rows, _, _ = _dedupe_opportunities(
        rows=[row for row in unique_shadow_rows if row.resolved],
        key_fn=lambda row: row.market_side_key,
    )
    underlying_family_rows = _aggregate_underlying_family([row for row in unique_shadow_rows if row.resolved])

    layer_rows: dict[str, list[TemperatureOpportunity]] = {
        "row_based": [row for row in graded_rows if row.resolved],
        "unique_shadow_order": [row for row in unique_shadow_rows if row.resolved],
        "unique_market_side": [row for row in unique_market_side_rows if row.resolved],
        "underlying_family_aggregated": [row for row in underlying_family_rows if row.resolved],
    }

    opportunity_breadth = _opportunity_breadth(
        graded_rows=graded_rows,
        unique_shadow_rows=unique_shadow_rows,
        unique_market_side_rows=unique_market_side_rows,
        underlying_family_rows=underlying_family_rows,
    )
    model_lineage = _model_lineage_summary(
        graded_rows=graded_rows,
        unique_shadow_rows=unique_shadow_rows,
        unique_market_side_rows=unique_market_side_rows,
        underlying_family_rows=underlying_family_rows,
    )

    concentration_warning, concentration_metrics, concentration_summary = _concentration_summary(
        resolved_rows,
        int(opportunity_breadth.get("resolved_unique_market_sides") or 0),
    )

    hit_rate_quality = {
        layer_name: _hit_rate_quality(rows)
        for layer_name, rows in layer_rows.items()
    }
    alpha_feature_density = {
        layer_name: _alpha_feature_summary(rows)
        for layer_name, rows in layer_rows.items()
    }

    expected_vs_shadow_settled = _build_expected_vs_shadow_settled(layer_rows)

    simulation_scenarios: list[dict[str, Any]] = []
    simulation_by_model: dict[str, dict[str, Any]] = {}
    attribution: dict[str, Any] = {}

    conservative_slippage = max(slippage_list) if slippage_list else 0.0
    selected_model_names = {
        _normalize_text(name)
        for name in (simulation_model_names or set())
        if _normalize_text(name)
    }
    selected_models: list[tuple[str, dict[str, Any]]] = [
        (model_name, model_config)
        for model_name, model_config in sizing_models.items()
        if (not selected_model_names) or (model_name in selected_model_names)
    ]
    if not selected_models:
        selected_models = list(sizing_models.items())

    selected_layer_names = {
        _normalize_text(name)
        for name in (simulation_layer_names or set())
        if _normalize_text(name)
    }
    selected_layers = [
        layer_name
        for layer_name in layer_rows.keys()
        if (not selected_layer_names) or (layer_name in selected_layer_names)
    ]
    if not selected_layers:
        selected_layers = list(layer_rows.keys())

    selected_slippage_keys = {
        str(parsed)
        for item in (simulation_slippage_bps or [])
        for parsed in [_parse_float(item)]
        if isinstance(parsed, float)
    }
    selected_slippage_values = [item for item in slippage_list if (not selected_slippage_keys) or (str(item) in selected_slippage_keys)]
    if not selected_slippage_values:
        selected_slippage_values = [conservative_slippage]

    for model_name, model_config in selected_models:
        model_bucket = {
            "model_config": model_config,
            "by_slippage_bps": {},
        }
        for slippage_bps in selected_slippage_values:
            slippage_key = str(slippage_bps)
            layer_metrics: dict[str, Any] = {}
            layer_executed_rows: dict[str, list[dict[str, Any]]] = {}
            for layer_name in selected_layers:
                rows = layer_rows.get(layer_name, [])
                metrics, executed = _simulate_bankroll_for_layer(
                    trades=rows,
                    model_name=model_name,
                    model_config=model_config,
                    reference_bankroll_dollars=reference_bankroll_dollars,
                    slippage_bps=slippage_bps,
                    fee_model=fee_model,
                )
                layer_metrics[layer_name] = metrics
                if include_attribution:
                    layer_executed_rows[layer_name] = executed
                if include_simulation_scenarios:
                    simulation_scenarios.append(
                        {
                            "sizing_model": model_name,
                            "aggregation_layer": layer_name,
                            "slippage_bps": slippage_bps,
                            **metrics,
                        }
                    )

            model_bucket["by_slippage_bps"][slippage_key] = layer_metrics
            if include_attribution:
                model_bucket.setdefault("_executed_rows", {})[slippage_key] = layer_executed_rows

        default_slippage_key = str(conservative_slippage)
        headline_metrics = model_bucket["by_slippage_bps"].get(default_slippage_key, {}).get(
            "underlying_family_aggregated", {}
        )
        model_bucket["headline_deployment_quality"] = {
            "aggregation_layer": "underlying_family_aggregated",
            "slippage_bps": conservative_slippage,
            **headline_metrics,
        }

        if include_attribution:
            executed_for_attribution = model_bucket.get("_executed_rows", {}).get(default_slippage_key, {}).get(
                "underlying_family_aggregated", []
            )
            attribution[model_name] = {
                "aggregation_layer": "underlying_family_aggregated",
                "slippage_bps": conservative_slippage,
                "by_station": _build_attribution(executed_for_attribution, "settlement_station", top_n),
                "by_local_hour": _build_attribution(executed_for_attribution, "local_hour_key", top_n),
                "by_signal_type": _build_attribution(executed_for_attribution, "signal_type", top_n),
                "by_policy_reason": _build_attribution(executed_for_attribution, "policy_reason", top_n),
                "by_underlying_family": _build_attribution(executed_for_attribution, "underlying_family_key", top_n),
            }
            del model_bucket["_executed_rows"]
        simulation_by_model[model_name] = model_bucket

    deployment_model_name = "fixed_fraction_per_underlying_family"
    deployment_slippage = conservative_slippage
    deployment_metrics = (
        simulation_by_model.get(deployment_model_name, {})
        .get("by_slippage_bps", {})
        .get(str(deployment_slippage), {})
        .get("underlying_family_aggregated", {})
    )

    hysa_annual_rate = _fraction(fee_model.get("hysa_comparison_assumption_annual_rate"), 0.045)
    equivalent_daily_hysa_return = reference_bankroll_dollars * (hysa_annual_rate / 365.0)
    equivalent_window_hysa_return = reference_bankroll_dollars * (
        hysa_annual_rate * effective_window_days_for_metrics / 365.0
    )
    excess_return_over_hysa = float(deployment_metrics.get("pnl_total") or 0.0) - equivalent_window_hysa_return

    main_limiting_factor = _infer_main_limiting_factor(
        policy_reason_counts=policy_reason_counts,
        concentration_warning=concentration_warning,
        opportunity_breadth=opportunity_breadth,
        current_settlement_unresolved=_parse_int(settlement_backlog_now.get("current_settlement_unresolved")),
        mixed_policy_versions_detected=bool(model_lineage.get("mixed_policy_versions_detected")),
    )
    next_missing_layer = _next_missing_alpha_layer(main_limiting_factor)

    utilization_avg = _parse_float(deployment_metrics.get("exposure_utilization_avg")) or 0.0
    utilization_peak = _parse_float(deployment_metrics.get("exposure_utilization_peak")) or 0.0
    resolved_unique_market_sides = int(opportunity_breadth.get("resolved_unique_market_sides") or 0)
    resolved_unique_families = int(opportunity_breadth.get("resolved_unique_underlying_families") or 0)
    deployed_unique_market_sides = int(deployment_metrics.get("unique_market_side_count") or 0)
    deployed_unique_families = int(deployment_metrics.get("unique_underlying_family_count") or 0)

    viability_summary = {
        "could_reference_bankroll_have_been_deployed_meaningfully": bool(utilization_peak >= 0.10),
        "deployment_meaningful_threshold_pct": 0.10,
        "what_pct_of_bankroll_would_have_been_utilized_avg": round(utilization_avg * 100.0, 4),
        "what_pct_of_bankroll_would_have_been_utilized_peak": round(utilization_peak * 100.0, 4),
        "what_return_would_have_been_produced_on_bankroll": deployment_metrics.get("roi_on_reference_bankroll"),
        "how_many_independent_market_side_calls_generated_that_return": deployed_unique_market_sides,
        "is_return_breadth_sufficient_to_justify_real_capital": bool(
            deployed_unique_market_sides >= 20 and deployed_unique_families >= 5 and not concentration_warning
        ),
        "resolved_unique_market_side_opportunities": resolved_unique_market_sides,
        "resolved_unique_underlying_family_opportunities": resolved_unique_families,
        "deployed_unique_market_side_calls": deployed_unique_market_sides,
        "deployed_unique_underlying_family_calls": deployed_unique_families,
        "would_plausibly_beat_hysa_after_slippage_fees": bool(excess_return_over_hysa > 0),
        "main_limiting_factor": main_limiting_factor,
        "model_consistency_mixed_policy_versions_detected": bool(model_lineage.get("mixed_policy_versions_detected")),
        "model_consistency_warning_count": int(len(model_lineage.get("warnings") or [])),
        "next_missing_alpha_layer_preventing_profit_machine": next_missing_layer["name"],
        "next_missing_alpha_layer_reason": next_missing_layer["reason"],
        "hysa_comparison_assumption_annual_rate": round(hysa_annual_rate, 6),
        "equivalent_daily_hysa_return_on_reference_bankroll": round(equivalent_daily_hysa_return, 6),
        "effective_window_days_for_hysa_comparison": round(effective_window_days_for_metrics, 6),
        "data_coverage_ratio": round(data_coverage_ratio, 6),
        "equivalent_window_hysa_return_on_reference_bankroll": round(equivalent_window_hysa_return, 6),
        "excess_return_over_hysa_for_window": round(excess_return_over_hysa, 6),
        "deployment_headline_basis": {
            "sizing_model": deployment_model_name,
            "aggregation_layer": "underlying_family_aggregated",
            "slippage_bps": deployment_slippage,
        },
        "notes": [
            "Prediction-quality headline uses unique_market_side by default.",
            "Deployment-quality headline uses underlying_family_aggregated bankroll simulation.",
            "Shadow-settled reference is not live execution performance.",
        ],
    }

    now_epoch = captured_at.timestamp()
    pipeline_health = _build_pipeline_health_snapshot(
        out_dir=out_dir,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        lookback_hours=lookback_hours,
        now_epoch=now_epoch,
    )
    settlement_state_file_in_window = _artifact_in_window(
        settlement_state_file,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )
    settlement_state_file_age_seconds = _artifact_age_seconds(settlement_state_file, now_epoch=now_epoch)
    constraint_summary_age_seconds = _artifact_age_seconds(constraint_summary_file, now_epoch=now_epoch)
    trade_plan_summary_age_seconds = _artifact_age_seconds(trade_plan_summary_file, now_epoch=now_epoch)
    signal_evidence = {
        "settlement_state": {
            "file_used": str(settlement_state_file) if settlement_state_file else "",
            "available": bool(settlement_state_file),
            "in_window": settlement_state_file_in_window,
            "age_seconds": (
                round(settlement_state_file_age_seconds, 3)
                if isinstance(settlement_state_file_age_seconds, float)
                else None
            ),
            **settlement_backlog_now,
        },
        "constraint_scan_summary": {
            "file_used": str(constraint_summary_file) if constraint_summary_file else "",
            "available": bool(constraint_summary_file),
            "in_window": bool(constraint_summary_in_window),
            "age_seconds": (
                round(constraint_summary_age_seconds, 3) if isinstance(constraint_summary_age_seconds, float) else None
            ),
        },
        "trade_plan_summary": {
            "file_used": str(trade_plan_summary_file) if trade_plan_summary_file else "",
            "available": bool(trade_plan_summary_file),
            "in_window": bool(trade_plan_summary_in_window),
            "age_seconds": (
                round(trade_plan_summary_age_seconds, 3) if isinstance(trade_plan_summary_age_seconds, float) else None
            ),
        },
        "pipeline_health": pipeline_health,
    }

    viability_summary["data_pipeline_status"] = _normalize_text(pipeline_health.get("status")) or "unknown"
    viability_summary["data_pipeline_reason"] = _normalize_text(pipeline_health.get("reason"))
    viability_summary["data_pipeline_missing_feeds"] = (
        pipeline_health.get("missing_feeds") if isinstance(pipeline_health.get("missing_feeds"), list) else []
    )
    viability_summary["data_pipeline_stale_feeds"] = (
        pipeline_health.get("stale_feeds") if isinstance(pipeline_health.get("stale_feeds"), list) else []
    )

    payload: dict[str, Any] = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "window": {
            "hours": lookback_hours,
            "window_start_utc": window_start.isoformat(),
            "window_end_utc": captured_at.isoformat(),
            "observed_span_hours": round(observed_span_hours, 6),
            "effective_window_hours_for_metrics": round(effective_window_hours_for_metrics, 6),
            "effective_window_days_for_metrics": round(effective_window_days_for_metrics, 6),
            "data_coverage_ratio": round(data_coverage_ratio, 6),
            "window_semantics": {
                "type": "rolling",
                "is_calendar_day": False,
                "label": f"rolling_{int(lookback_hours)}h" if abs(lookback_hours - round(lookback_hours)) < 1e-9 else f"rolling_{lookback_hours}h",
            },
        },
        "output_dir": str(out_dir),
        "inputs": {
            "reference_bankroll_dollars": reference_bankroll_dollars,
            "sizing_models": sizing_models,
            "slippage_bps_list": slippage_list,
            "fee_model": fee_model,
            "top_n": max(1, int(top_n)),
            "settlement_state_files_scanned_for_truth": settlement_state_files_scanned,
        },
        "aggregation_layers": [
            "row_based",
            "unique_shadow_order",
            "unique_market_side",
            "underlying_family_aggregated",
        ],
        "opportunity_breadth": opportunity_breadth,
        "concentration_checks": {
            "concentration_warning": concentration_warning,
            "concentration_metrics": concentration_metrics,
            "concentration_summary": concentration_summary,
            "duplicate_shadow_order_ids": duplicate_shadow_ids_top,
            "duplicate_shadow_order_ids_total_unique": int(duplicate_shadow_ids_total_unique),
            "duplicate_shadow_order_ids_returned": int(len(duplicate_shadow_ids_top)),
            "duplicate_shadow_order_ids_truncated": bool(duplicate_shadow_ids_truncated_count > 0),
            "duplicate_shadow_order_ids_truncated_count": int(duplicate_shadow_ids_truncated_count),
            "duplicate_shadow_order_ids_top_n_limit": int(duplicate_shadow_ids_top_n),
            "duplicate_count": int(sum(max(0, count - 1) for count in duplicate_shadow_ids.values())),
            "duplicate_warnings": duplicate_warnings,
            "duplicate_warning_stats": duplicate_warning_stats,
        },
        "hit_rate_quality": hit_rate_quality,
        "alpha_feature_density": alpha_feature_density,
        "model_lineage": model_lineage,
        "expected_vs_shadow_settled": expected_vs_shadow_settled,
        "bankroll_simulation": {
            "scenarios": simulation_scenarios,
            "by_model": simulation_by_model,
        },
        "attribution": attribution,
        "viability_summary": viability_summary,
        "anti_misleading_guards": {
            "default_prediction_quality_basis": "unique_market_side",
            "default_deployment_quality_basis": "underlying_family_aggregated",
            "shadow_settled_is_not_live": True,
            "always_show_roi_on_deployed_and_reference": True,
            "never_headline_row_based_wins": True,
        },
        "signal_evidence": signal_evidence,
        "data_quality": {
            "pipeline_health": pipeline_health,
            "pipeline_status": _normalize_text(pipeline_health.get("status")) or "unknown",
            "pipeline_reason": _normalize_text(pipeline_health.get("reason")),
            "pipeline_missing_feeds": (
                pipeline_health.get("missing_feeds") if isinstance(pipeline_health.get("missing_feeds"), list) else []
            ),
            "pipeline_stale_feeds": (
                pipeline_health.get("stale_feeds") if isinstance(pipeline_health.get("stale_feeds"), list) else []
            ),
            "pipeline_out_of_window_feeds": (
                pipeline_health.get("out_of_window_feeds")
                if isinstance(pipeline_health.get("out_of_window_feeds"), list)
                else []
            ),
            "pipeline_min_in_window_coverage_ratio": float(
                _parse_float(pipeline_health.get("min_in_window_coverage_ratio")) or 0.0
            ),
            "pipeline_avg_in_window_coverage_ratio": float(
                _parse_float(pipeline_health.get("avg_in_window_coverage_ratio")) or 0.0
            ),
            "settlement_backlog_now": settlement_backlog_now,
            "settlement_state_file_used": str(settlement_state_file) if settlement_state_file else "",
            "settlement_state_file_in_window": settlement_state_file_in_window,
            "settlement_state_file_age_seconds": (
                round(settlement_state_file_age_seconds, 3)
                if isinstance(settlement_state_file_age_seconds, float)
                else None
            ),
            "constraint_scan_summary_file_used": str(constraint_summary_file) if constraint_summary_file else "",
            "constraint_scan_summary_in_window": bool(constraint_summary_in_window),
            "constraint_scan_summary_age_seconds": (
                round(constraint_summary_age_seconds, 3) if isinstance(constraint_summary_age_seconds, float) else None
            ),
            "trade_plan_summary_file_used": str(trade_plan_summary_file) if trade_plan_summary_file else "",
            "trade_plan_summary_in_window": bool(trade_plan_summary_in_window),
            "trade_plan_summary_age_seconds": (
                round(trade_plan_summary_age_seconds, 3) if isinstance(trade_plan_summary_age_seconds, float) else None
            ),
            "expected_shadow_edge_total_raw_rows": round(expected_shadow_edge_total, 6),
            "expected_shadow_estimated_entry_cost_total_raw_rows": round(expected_shadow_cost_total, 6),
            "model_lineage": model_lineage,
            "policy_reason_counts": policy_reason_counts,
            "alpha_feature_density_unique_market_side": alpha_feature_density.get("unique_market_side", {}),
            "alpha_feature_density_underlying_family_aggregated": alpha_feature_density.get(
                "underlying_family_aggregated", {}
            ),
        },
        "alpha_gap_brief": {
            "current_signals_implemented": [
                "hard_constraints",
                "station_hour_freshness_tuning",
                "settlement_final_report_gating",
            ],
            "missing_or_partial_signals": [
                "bracket_range_consistency",
                "neighboring_strike_monotonicity",
                "exact_strike_impossibility_chains",
                "taf_remainder_of_day_path_modeling",
                "speci_triggered_intraday_jumps",
                "cross_market_family_mispricing",
                "execution_aware_portfolio_optimization",
                "broad_multi_city_scanning_throughput",
            ],
            "likely_next_highest_impact_signal_expansion": next_missing_layer,
        },
        "signal_progress": {
            "neighboring_strike_monotonicity": {
                "checked_groups": int(neighboring_summary.get("checked_groups") or 0),
                "violations_count": int(neighboring_summary.get("violations_count") or 0),
            },
            "exact_strike_impossibility_chains": {
                "checked_groups": int(exact_chain_summary.get("checked_groups") or 0),
                "violations_count": int(exact_chain_summary.get("violations_count") or 0),
            },
            "range_family_consistency": {
                "checked_groups": int(range_family_summary.get("checked_groups") or 0),
                "violations_count": int(range_family_summary.get("violations_count") or 0),
            },
            "cross_market_family_mispricing": {
                "checked_buckets": int(cross_market_summary.get("checked_buckets") or 0),
                "checked_families": int(cross_market_summary.get("checked_families") or 0),
                "candidate_count": int(cross_market_summary.get("candidate_count") or 0),
                "high_outlier_count": int(cross_market_summary.get("high_outlier_count") or 0),
                "low_outlier_count": int(cross_market_summary.get("low_outlier_count") or 0),
            },
            "taf_remainder_of_day_path_modeling": {
                "forecast_modeled_count": int(constraint_summary_payload.get("forecast_modeled_count") or 0),
                "taf_ready_count": int(constraint_summary_payload.get("taf_ready_count") or 0),
            },
            "speci_triggered_intraday_jumps": {
                "speci_recent_count": int(constraint_summary_payload.get("speci_recent_count") or 0),
                "speci_shock_active_count": int(constraint_summary_payload.get("speci_shock_active_count") or 0),
                "speci_shock_confidence_avg": _parse_float(constraint_summary_payload.get("speci_shock_confidence_avg")),
                "speci_shock_weight_avg": _parse_float(constraint_summary_payload.get("speci_shock_weight_avg")),
                "speci_shock_cooldown_blocked_count": int(
                    constraint_summary_payload.get("speci_shock_cooldown_blocked_count") or 0
                ),
                "speci_shock_improvement_hold_count": int(
                    constraint_summary_payload.get("speci_shock_improvement_hold_count") or 0
                ),
            },
            "execution_aware_portfolio_optimization": {
                "optimizer_mode": _normalize_text(allocation_summary.get("optimization_mode")),
                "candidate_count": int(allocation_summary.get("candidate_count") or 0),
                "selected_count": int(allocation_summary.get("selected_count") or 0),
                "selected_score_avg": _parse_float(allocation_summary.get("selected_score_avg")),
                "selected_score_total": _parse_float(allocation_summary.get("selected_score_total")),
            },
        },
    }
    payload["growth_readiness"] = _build_growth_readiness_block(payload)
    return payload


def run_kalshi_temperature_bankroll_validation(
    *,
    output_dir: str = "outputs",
    hours: float = 24.0,
    reference_bankroll_dollars: float = 1000.0,
    sizing_models_json: str | None = None,
    slippage_bps_list: str | None = "0,5,10",
    fee_model_json: str | None = None,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    payload = _build_validation_payload(
        output_dir=output_dir,
        hours=hours,
        reference_bankroll_dollars=max(1.0, float(reference_bankroll_dollars)),
        sizing_models_json=sizing_models_json,
        slippage_bps_list=slippage_bps_list,
        fee_model_json=fee_model_json,
        top_n=top_n,
        now=captured_at,
    )

    out_dir = Path(output_dir)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"kalshi_temperature_bankroll_validation_{stamp}.json"
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    payload["output_file"] = str(summary_path)
    return payload


def run_kalshi_temperature_alpha_gap_report(
    *,
    output_dir: str = "outputs",
    hours: float = 24.0,
    reference_bankroll_dollars: float = 1000.0,
    sizing_models_json: str | None = None,
    slippage_bps_list: str | None = "0,5,10",
    fee_model_json: str | None = None,
    top_n: int = 10,
    source_bankroll_validation_file: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    source_validation_path = Path(source_bankroll_validation_file).expanduser() if source_bankroll_validation_file else None
    source_validation_payload: dict[str, Any] = {}
    if source_validation_path is not None:
        source_validation_payload = _load_json(source_validation_path)
    source_validation_valid = (
        isinstance(source_validation_payload.get("opportunity_breadth"), dict)
        and isinstance(source_validation_payload.get("viability_summary"), dict)
        and isinstance(source_validation_payload.get("concentration_checks"), dict)
    )
    if source_validation_valid:
        validation = source_validation_payload
    else:
        validation = _build_validation_payload(
            output_dir=output_dir,
            hours=hours,
            reference_bankroll_dollars=max(1.0, float(reference_bankroll_dollars)),
            sizing_models_json=sizing_models_json,
            slippage_bps_list=slippage_bps_list,
            fee_model_json=fee_model_json,
            top_n=top_n,
            now=captured_at,
        )

    breadth = validation.get("opportunity_breadth") or {}
    viability = validation.get("viability_summary") or {}
    concentration = validation.get("concentration_checks") or {}
    data_quality = validation.get("data_quality") if isinstance(validation.get("data_quality"), dict) else {}
    validation_signal_evidence = (
        validation.get("signal_evidence")
        if isinstance(validation.get("signal_evidence"), dict)
        else {}
    )
    settlement_signal_evidence = (
        validation_signal_evidence.get("settlement_state")
        if isinstance(validation_signal_evidence.get("settlement_state"), dict)
        else {}
    )
    constraint_summary_file_used = _normalize_text(data_quality.get("constraint_scan_summary_file_used"))
    constraint_summary_available = bool(constraint_summary_file_used)
    constraint_summary_in_window = bool(data_quality.get("constraint_scan_summary_in_window"))
    constraint_summary_age_seconds = _parse_float(data_quality.get("constraint_scan_summary_age_seconds"))
    trade_plan_summary_file_used = _normalize_text(data_quality.get("trade_plan_summary_file_used"))
    trade_plan_summary_available = bool(trade_plan_summary_file_used)
    trade_plan_summary_in_window = bool(data_quality.get("trade_plan_summary_in_window"))
    trade_plan_summary_age_seconds = _parse_float(data_quality.get("trade_plan_summary_age_seconds"))
    policy_reason_counts = (
        data_quality.get("policy_reason_counts") if isinstance(data_quality.get("policy_reason_counts"), dict) else {}
    )
    limiting_factor = _normalize_text(viability.get("main_limiting_factor")) or "stale_suppression"
    signal_progress = validation.get("signal_progress") if isinstance(validation.get("signal_progress"), dict) else {}

    monotonic_progress = (
        signal_progress.get("neighboring_strike_monotonicity")
        if isinstance(signal_progress.get("neighboring_strike_monotonicity"), dict)
        else {}
    )
    exact_chain_progress = (
        signal_progress.get("exact_strike_impossibility_chains")
        if isinstance(signal_progress.get("exact_strike_impossibility_chains"), dict)
        else {}
    )
    range_family_progress = (
        signal_progress.get("range_family_consistency")
        if isinstance(signal_progress.get("range_family_consistency"), dict)
        else {}
    )
    cross_market_progress = (
        signal_progress.get("cross_market_family_mispricing")
        if isinstance(signal_progress.get("cross_market_family_mispricing"), dict)
        else {}
    )
    taf_progress = (
        signal_progress.get("taf_remainder_of_day_path_modeling")
        if isinstance(signal_progress.get("taf_remainder_of_day_path_modeling"), dict)
        else {}
    )
    speci_progress = (
        signal_progress.get("speci_triggered_intraday_jumps")
        if isinstance(signal_progress.get("speci_triggered_intraday_jumps"), dict)
        else {}
    )
    execution_progress = (
        signal_progress.get("execution_aware_portfolio_optimization")
        if isinstance(signal_progress.get("execution_aware_portfolio_optimization"), dict)
        else {}
    )

    def _status_from_consistency(
        progress: dict[str, Any],
        *,
        evidence_available: bool,
        evidence_in_window: bool,
    ) -> str:
        checked = int(progress.get("checked_groups") or 0)
        violations = int(progress.get("violations_count") or 0)
        if checked > 0:
            return "implemented" if (evidence_in_window and violations == 0) else "partial"
        if evidence_available:
            return "partial"
        return "missing"

    monotonic_status = _status_from_consistency(
        monotonic_progress,
        evidence_available=constraint_summary_available,
        evidence_in_window=constraint_summary_in_window,
    )
    exact_chain_status = _status_from_consistency(
        exact_chain_progress,
        evidence_available=constraint_summary_available,
        evidence_in_window=constraint_summary_in_window,
    )
    bracket_status = _status_from_consistency(
        range_family_progress,
        evidence_available=constraint_summary_available,
        evidence_in_window=constraint_summary_in_window,
    )
    cross_market_checked_buckets = int(cross_market_progress.get("checked_buckets") or 0)
    if cross_market_checked_buckets > 0:
        cross_market_status = "implemented" if constraint_summary_in_window else "partial"
    elif constraint_summary_available:
        cross_market_status = "partial"
    else:
        cross_market_status = "missing"

    forecast_modeled_count = int(taf_progress.get("forecast_modeled_count") or 0)
    taf_ready_count = int(taf_progress.get("taf_ready_count") or 0)
    if taf_ready_count > 0:
        taf_status = "implemented" if constraint_summary_in_window else "partial"
    elif forecast_modeled_count > 0:
        taf_status = "implemented" if constraint_summary_in_window else "partial"
    elif constraint_summary_available:
        taf_status = "partial"
    else:
        taf_status = "missing"

    speci_recent_count = int(speci_progress.get("speci_recent_count") or 0)
    speci_shock_active_count = int(speci_progress.get("speci_shock_active_count") or 0)
    speci_shock_confidence_avg = _parse_float(speci_progress.get("speci_shock_confidence_avg")) or 0.0
    speci_shock_weight_avg = _parse_float(speci_progress.get("speci_shock_weight_avg")) or 0.0
    speci_shock_cooldown_blocked_count = int(speci_progress.get("speci_shock_cooldown_blocked_count") or 0)
    speci_shock_improvement_hold_count = int(speci_progress.get("speci_shock_improvement_hold_count") or 0)
    if speci_shock_active_count > 0 and speci_shock_confidence_avg >= 0.45:
        speci_status = "implemented" if constraint_summary_in_window else "partial"
    elif (
        (speci_recent_count > 0)
        or (speci_shock_cooldown_blocked_count > 0)
        or (speci_shock_improvement_hold_count > 0)
        or (speci_shock_confidence_avg > 0.0)
        or (speci_shock_weight_avg > 0.0)
    ):
        speci_status = "implemented" if constraint_summary_in_window else "partial"
    elif constraint_summary_available:
        speci_status = "partial"
    else:
        speci_status = "missing"

    optimizer_mode = _normalize_text(execution_progress.get("optimizer_mode")).lower()
    optimizer_candidate_count = int(execution_progress.get("candidate_count") or 0)
    optimizer_selected_score_avg = _parse_float(execution_progress.get("selected_score_avg"))
    if optimizer_mode.startswith("score_aware_greedy"):
        execution_status = "implemented" if trade_plan_summary_in_window else "partial"
    elif optimizer_candidate_count > 0 or isinstance(optimizer_selected_score_avg, float) or optimizer_mode:
        execution_status = "partial"
    elif trade_plan_summary_available:
        execution_status = "partial"
    else:
        execution_status = "missing"

    resolved_unique_market_sides = int(breadth.get("resolved_unique_market_sides") or 0)
    resolved_unique_families = int(breadth.get("resolved_unique_underlying_families") or 0)
    unresolved_unique_market_sides = int(breadth.get("unresolved_unique_market_sides") or 0)
    unresolved_unique_families = int(breadth.get("unresolved_unique_underlying_families") or 0)
    approved_policy_count = int(policy_reason_counts.get("approved") or 0)
    if resolved_unique_market_sides >= 30 and resolved_unique_families >= 8:
        throughput_status = "implemented"
    elif resolved_unique_market_sides >= 10 and resolved_unique_families >= 3:
        throughput_status = "partial"
    elif (
        optimizer_candidate_count >= 20
        or forecast_modeled_count >= 80
        or unresolved_unique_market_sides >= 10
        or unresolved_unique_families >= 5
        or approved_policy_count > 0
    ):
        throughput_status = "partial"
    else:
        throughput_status = "missing"

    current_signals_implemented = [
        {"name": "hard_constraints", "status": "implemented"},
        {"name": "station_hour_freshness_tuning", "status": "implemented"},
        {"name": "settlement_final_report_gating", "status": "implemented"},
    ]

    missing_or_partial = [
        {
            "name": "bracket_range_consistency",
            "status": bracket_status,
            "impact": "high",
            "why_it_matters": "Prevents incoherent strike-level selections and supports range-book consistency.",
        },
        {
            "name": "neighboring_strike_monotonicity",
            "status": monotonic_status,
            "impact": "high",
            "why_it_matters": "Enforces adjacent-strike coherence and improves confidence scoring in clustered books.",
        },
        {
            "name": "exact_strike_impossibility_chains",
            "status": exact_chain_status,
            "impact": "medium",
            "why_it_matters": "Current hard-constraint lane catches obvious cases but not full chain propagation across bracket families.",
        },
        {
            "name": "taf_remainder_of_day_path_modeling",
            "status": taf_status,
            "impact": "high",
            "why_it_matters": "Adds path-aware weather dynamics beyond static impossibility checks.",
        },
        {
            "name": "speci_triggered_intraday_jumps",
            "status": speci_status,
            "impact": "medium",
            "why_it_matters": "Captures abrupt intraday state changes with persistence/cooldown controls that can dominate same-day temperature outcomes.",
        },
        {
            "name": "cross_market_family_mispricing",
            "status": cross_market_status,
            "impact": "high",
            "why_it_matters": "Supports relative-value selection across correlated city/day families.",
        },
        {
            "name": "execution_aware_portfolio_optimization",
            "status": execution_status,
            "impact": "medium",
            "why_it_matters": "Sizing constraints exist, but full portfolio optimizer and fill-aware routing are not yet implemented.",
        },
        {
            "name": "broad_multi_city_scanning_throughput",
            "status": throughput_status,
            "impact": "medium",
            "why_it_matters": "Throughput is functional but opportunity breadth remains concentrated in a small set of outcomes.",
        },
    ]

    repeated_entry_multiplier_raw = _parse_float(breadth.get("repeated_entry_multiplier"))
    repeated_entry_multiplier = (
        round(repeated_entry_multiplier_raw, 6)
        if isinstance(repeated_entry_multiplier_raw, float)
        else None
    )

    next_signal = dict(_next_missing_alpha_layer(limiting_factor))
    next_signal["expected_impact"] = "high" if next_signal["name"] != "cross_market_family_mispricing" else "medium"

    if resolved_unique_market_sides <= 0:
        ceiling_summary = (
            "No resolved unique market-side outcomes in this window yet; alpha breadth and concentration "
            "cannot be assessed until settlements age in."
        )
    elif concentration.get("concentration_warning"):
        ceiling_summary = (
            "Current lane appears breadth-limited and concentration-prone; bankroll scaling is likely "
            "capped without broader signal layers."
        )
    elif resolved_unique_market_sides < 5 or resolved_unique_families < 2:
        ceiling_summary = (
            "Resolved breadth is still narrow for dependable scaling; expand independent station/day "
            "coverage before increasing bankroll risk."
        )
    else:
        ceiling_summary = (
            "Current lane has workable breadth, but additional signals are still needed for robust "
            "multi-city deployment."
        )

    signal_evidence = {
        "settlement_state": {
            "file_used": _normalize_text(settlement_signal_evidence.get("file_used")),
            "available": bool(settlement_signal_evidence.get("available")),
            "in_window": bool(settlement_signal_evidence.get("in_window")),
            "age_seconds": (
                round(_parse_float(settlement_signal_evidence.get("age_seconds")), 3)
                if isinstance(_parse_float(settlement_signal_evidence.get("age_seconds")), float)
                else None
            ),
        },
        "constraint_scan_summary": {
            "file_used": constraint_summary_file_used,
            "available": constraint_summary_available,
            "in_window": constraint_summary_in_window,
            "age_seconds": (
                round(constraint_summary_age_seconds, 3) if isinstance(constraint_summary_age_seconds, float) else None
            ),
        },
        "trade_plan_summary": {
            "file_used": trade_plan_summary_file_used,
            "available": trade_plan_summary_available,
            "in_window": trade_plan_summary_in_window,
            "age_seconds": (
                round(trade_plan_summary_age_seconds, 3) if isinstance(trade_plan_summary_age_seconds, float) else None
            ),
        },
        "pipeline_health": (
            validation_signal_evidence.get("pipeline_health")
            if isinstance(validation_signal_evidence.get("pipeline_health"), dict)
            else {}
        ),
    }

    payload = {
        "status": "ready",
        "captured_at": validation.get("captured_at"),
        "output_dir": validation.get("output_dir"),
        "window": validation.get("window"),
        "current_signals_implemented": current_signals_implemented,
        "missing_or_partial_signals": missing_or_partial,
        "opportunity_ceiling_estimate": {
            "resolved_unique_market_sides": resolved_unique_market_sides,
            "resolved_unique_underlying_families": resolved_unique_families,
            "repeated_entry_multiplier": repeated_entry_multiplier,
            "peak_bankroll_utilization_pct": viability.get("what_pct_of_bankroll_would_have_been_utilized_peak"),
            "ceiling_summary": ceiling_summary,
        },
        "likely_next_highest_impact_signal_expansion": next_signal,
        "main_limiting_factor": limiting_factor,
        "signal_progress": signal_progress,
        "signal_evidence": signal_evidence,
        "validation_context": {
            "window": validation.get("window"),
            "inputs": validation.get("inputs"),
            "viability_summary": viability,
            "concentration_checks": concentration,
            "opportunity_breadth": breadth,
            "hit_rate_quality": validation.get("hit_rate_quality"),
            "alpha_feature_density": validation.get("alpha_feature_density"),
            "expected_vs_shadow_settled": validation.get("expected_vs_shadow_settled"),
            "signal_evidence": signal_evidence,
            "data_quality": validation.get("data_quality"),
            "anti_misleading_guards": validation.get("anti_misleading_guards"),
            "bankroll_simulation_headline": (
                validation.get("viability_summary", {}).get("deployment_headline_basis")
                if isinstance(validation.get("viability_summary"), dict)
                else {}
            ),
        },
        "source_bankroll_validation_file": _normalize_text(validation.get("output_file"))
        or (_normalize_text(str(source_validation_path)) if source_validation_path else ""),
        "source_bankroll_validation_supplied": bool(source_validation_path),
        "source_bankroll_validation_reused": source_validation_valid,
        "source_bankroll_validation_recompute_reason": (
            ""
            if source_validation_valid
            else ("invalid_or_missing_source_validation_payload" if source_validation_path else "not_supplied")
        ),
    }

    out_dir = Path(output_dir)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"kalshi_temperature_alpha_gap_report_{stamp}.json"
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    payload["output_file"] = str(summary_path)
    return payload


def run_kalshi_temperature_live_readiness(
    *,
    output_dir: str = "outputs",
    horizons: str | None = None,
    reference_bankroll_dollars: float = 1000.0,
    sizing_models_json: str | None = None,
    slippage_bps_list: str | None = "0,5,10",
    fee_model_json: str | None = None,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    run_started_perf = perf_counter()
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)

    reference_bankroll = max(1.0, float(reference_bankroll_dollars))
    resolved_horizons = _resolve_readiness_horizons(horizons)
    slippage_list = _resolve_slippage_bps_list(slippage_bps_list)
    conservative_slippage = max(slippage_list) if slippage_list else 0.0
    fee_model = _resolve_fee_model(fee_model_json)

    horizon_results: list[dict[str, Any]] = []
    validation_by_horizon: dict[str, dict[str, Any]] = {}
    horizon_runtime_profile: list[dict[str, Any]] = []
    for label, hours in resolved_horizons:
        horizon_started_perf = perf_counter()
        validation = _build_validation_payload(
            output_dir=output_dir,
            hours=hours,
            reference_bankroll_dollars=reference_bankroll,
            sizing_models_json=sizing_models_json,
            slippage_bps_list=",".join(str(item) for item in slippage_list),
            fee_model_json=fee_model_json,
            top_n=top_n,
            simulation_model_names={_DEPLOYMENT_HEADLINE_MODEL},
            simulation_layer_names={_DEPLOYMENT_HEADLINE_LAYER},
            simulation_slippage_bps=[conservative_slippage],
            include_simulation_scenarios=False,
            include_attribution=False,
            now=captured_at,
        )
        validation_by_horizon[label] = validation
        horizon_runtime_seconds = max(0.0, perf_counter() - horizon_started_perf)
        horizon_runtime_profile.append(
            {
                "horizon": label,
                "hours": round(float(hours), 6),
                "runtime_seconds": round(float(horizon_runtime_seconds), 6),
            }
        )
        horizon_results.append(
            _build_horizon_readiness_entry(
                horizon_label=label,
                horizon_hours=hours,
                validation_payload=validation,
                reference_bankroll_dollars=reference_bankroll,
                conservative_slippage_bps=conservative_slippage,
                fee_model=fee_model,
            )
        )

    overall_decision = _build_overall_live_decision(horizon_results)
    status_counts = {
        "green": len([entry for entry in horizon_results if entry.get("readiness_status") == "green"]),
        "yellow": len([entry for entry in horizon_results if entry.get("readiness_status") == "yellow"]),
        "red": len([entry for entry in horizon_results if entry.get("readiness_status") == "red"]),
    }
    first_horizon = horizon_results[0] if horizon_results else {}
    latest_window = validation_by_horizon.get(horizon_results[0]["horizon"], {}).get("window") if horizon_results else {}
    total_runtime_seconds = max(0.0, perf_counter() - run_started_perf)
    output_payload = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "output_dir": str(Path(output_dir)),
        "reference_bankroll_dollars": reference_bankroll,
        "horizons_requested": [label for label, _ in resolved_horizons],
        "slippage_bps_list": slippage_list,
        "conservative_slippage_for_readiness": conservative_slippage,
        "window_semantics_note": "All horizon outputs are rolling windows, not calendar-day labels.",
        "executive_summary": {
            "recommendation": overall_decision.get("recommendation"),
            "recommendation_summary": overall_decision.get("recommendation_summary"),
            "ready_for_small_live_pilot": overall_decision.get("ready_for_small_live_pilot"),
            "ready_for_scaled_live": overall_decision.get("ready_for_scaled_live"),
            "horizon_status_counts": status_counts,
            "shortest_horizon_status": first_horizon.get("readiness_status"),
            "shortest_horizon_ready": first_horizon.get("ready_for_real_money"),
            "shortest_horizon_pipeline_status": _normalize_text(
                (
                    first_horizon.get("parity_context", {})
                    .get("data_quality", {})
                    .get("pipeline_status")
                    if isinstance(first_horizon.get("parity_context"), dict)
                    else ""
                )
            ),
            "shortest_horizon_pipeline_reason": _normalize_text(
                (
                    first_horizon.get("parity_context", {})
                    .get("data_quality", {})
                    .get("pipeline_reason")
                    if isinstance(first_horizon.get("parity_context"), dict)
                    else ""
                )
            ),
        },
        "readiness_by_horizon": horizon_results,
        "overall_live_readiness": overall_decision,
        "validation_parity_by_horizon": {
            str(entry.get("horizon")): (
                entry.get("parity_context")
                if isinstance(entry.get("parity_context"), dict)
                else {}
            )
            for entry in horizon_results
        },
        "evaluation_context": {
            "default_prediction_quality_basis": "unique_market_side",
            "default_deployment_quality_basis": "underlying_family_aggregated",
            "shadow_settled_is_not_live": True,
            "latest_window_sample": latest_window,
        },
        "runtime_profile": {
            "total_runtime_seconds": round(float(total_runtime_seconds), 6),
            "horizon_runtime_seconds": horizon_runtime_profile,
            "horizon_count": int(len(resolved_horizons)),
        },
    }

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"kalshi_temperature_live_readiness_{stamp}.json"
    summary_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")
    output_payload["output_file"] = str(summary_path)
    return output_payload


def run_kalshi_temperature_go_live_gate(
    *,
    output_dir: str = "outputs",
    horizons: str | None = None,
    reference_bankroll_dollars: float = 1000.0,
    sizing_models_json: str | None = None,
    slippage_bps_list: str | None = "0,5,10",
    fee_model_json: str | None = None,
    top_n: int = 10,
    source_live_readiness_file: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    source_readiness_path = Path(source_live_readiness_file).expanduser() if source_live_readiness_file else None
    source_readiness_payload: dict[str, Any] = {}
    if source_readiness_path is not None:
        source_readiness_payload = _load_json(source_readiness_path)
    source_readiness_valid = (
        isinstance(source_readiness_payload.get("readiness_by_horizon"), list)
        and isinstance(source_readiness_payload.get("overall_live_readiness"), dict)
    )
    if source_readiness_valid:
        readiness = source_readiness_payload
    else:
        readiness = run_kalshi_temperature_live_readiness(
            output_dir=output_dir,
            horizons=horizons,
            reference_bankroll_dollars=reference_bankroll_dollars,
            sizing_models_json=sizing_models_json,
            slippage_bps_list=slippage_bps_list,
            fee_model_json=fee_model_json,
            top_n=top_n,
            now=captured_at,
        )
    by_horizon = (
        readiness.get("readiness_by_horizon")
        if isinstance(readiness.get("readiness_by_horizon"), list)
        else []
    )
    overall = (
        readiness.get("overall_live_readiness")
        if isinstance(readiness.get("overall_live_readiness"), dict)
        else {}
    )

    failed_horizons: list[dict[str, Any]] = []
    for row in by_horizon:
        if not isinstance(row, dict):
            continue
        gates = row.get("gates") if isinstance(row.get("gates"), dict) else {}
        if bool(row.get("ready_for_real_money")):
            continue
        failed_horizons.append(
            {
                "horizon": _normalize_text(row.get("horizon")),
                "readiness_status": _normalize_text(row.get("readiness_status")),
                "failed_reasons": gates.get("failed_reasons") if isinstance(gates.get("failed_reasons"), list) else [],
                "failed_reason_details": gates.get("failed_reason_details")
                if isinstance(gates.get("failed_reason_details"), list)
                else [],
            }
        )

    ready_for_small_pilot = bool(overall.get("ready_for_small_live_pilot"))
    ready_for_scaled = bool(overall.get("ready_for_scaled_live"))
    gate_status = "pass" if ready_for_small_pilot else "fail"
    recommendation = _normalize_text(overall.get("recommendation")) or "shadow_only_continue"

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    gate_path = out_dir / f"kalshi_temperature_go_live_gate_{stamp}.json"
    gate_payload = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "gate_status": gate_status,
        "ready_for_small_live_pilot": ready_for_small_pilot,
        "ready_for_scaled_live": ready_for_scaled,
        "recommendation": recommendation,
        "recommendation_summary": _normalize_text(overall.get("recommendation_summary")),
        "earliest_passing_horizon": overall.get("earliest_passing_horizon"),
        "passing_horizons": overall.get("passing_horizons") if isinstance(overall.get("passing_horizons"), list) else [],
        "failed_horizons": failed_horizons,
        "failed_horizon_count": len(failed_horizons),
        "source_live_readiness_file": _normalize_text(readiness.get("output_file"))
        or (_normalize_text(str(source_readiness_path)) if source_readiness_path else ""),
        "source_live_readiness_supplied": bool(source_readiness_path),
        "source_live_readiness_reused": source_readiness_valid,
        "source_live_readiness_recompute_reason": (
            ""
            if source_readiness_valid
            else ("invalid_or_missing_source_readiness_payload" if source_readiness_path else "not_supplied")
        ),
    }
    gate_path.write_text(json.dumps(gate_payload, indent=2), encoding="utf-8")
    gate_payload["output_file"] = str(gate_path)
    return gate_payload
