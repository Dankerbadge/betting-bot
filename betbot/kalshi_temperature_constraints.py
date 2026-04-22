from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from betbot.kalshi_weather_intraday import build_intraday_temperature_snapshot
from betbot.kalshi_weather_ingest import (
    fetch_aviationweather_taf_temperature_envelopes,
    fetch_nws_station_hourly_forecast,
)
from betbot.kalshi_weather_settlement import infer_timezone_from_station


CONSTRAINT_SCAN_FIELDNAMES = [
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
    "temperature_metric",
    "threshold_expression",
    "threshold_kind",
    "threshold_lower_bound",
    "threshold_upper_bound",
    "constraint_status",
    "constraint_reason",
    "observed_max_settlement_raw",
    "observed_max_settlement_quantized",
    "observed_metric_settlement_raw",
    "observed_metric_settlement_quantized",
    "forecast_upper_bound_settlement_raw",
    "forecast_lower_bound_settlement_raw",
    "forecast_range_width",
    "possible_final_lower_bound",
    "possible_final_upper_bound",
    "yes_interval_lower_bound",
    "yes_interval_upper_bound",
    "yes_possible_overlap",
    "yes_possible_gap",
    "observed_distance_to_lower_bound",
    "observed_distance_to_upper_bound",
    "primary_signal_margin",
    "forecast_feasibility_margin",
    "forecast_model_status",
    "taf_status",
    "taf_volatility_score",
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
    "range_family_consistency_conflict",
    "range_family_consistency_conflict_scope",
    "range_family_consistency_conflict_reason",
    "cross_market_family_score",
    "cross_market_family_zscore",
    "cross_market_family_candidate_rank",
    "cross_market_family_bucket_size",
    "cross_market_family_signal",
    "observations_for_date",
    "snapshot_status",
    "settlement_confidence_score",
]

_SNAPSHOT_ACTIVE_DAY_MAX_AGE_MINUTES = 180.0
_DEGRADED_SNAPSHOT_STATUS_TOKENS = (
    "stale",
    "freshness",
    "degraded",
    "partial",
    "outdated",
    "lag",
)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_threshold_expression(value: Any) -> tuple[str, list[float]]:
    text = _normalize_text(value)
    if not text or ":" not in text:
        return ("", [])
    parts = [item.strip() for item in text.split(":")]
    if len(parts) < 2:
        return ("", [])
    kind = parts[0].lower()
    values: list[float] = []
    try:
        values = [float(token) for token in parts[1:] if token]
    except ValueError:
        return ("", [])
    return (kind, values)


def infer_settlement_unit(
    market_title: str,
    rules_primary: str,
    *,
    threshold_expression: str | None = None,
) -> str:
    merged = " ".join((_normalize_text(market_title), _normalize_text(rules_primary))).lower()
    if any(token in merged for token in ("fahrenheit", "°f", "deg f", "degrees f")):
        return "fahrenheit"
    if any(token in merged for token in ("celsius", "°c", "deg c", "degrees c")):
        return "celsius"

    # Fallback heuristic: thresholds above 60 are overwhelmingly Fahrenheit in
    # Kalshi temperature markets. Keep default as Fahrenheit to avoid false
    # positive Celsius matches on city names (for example "Chicago").
    _, values = _normalize_threshold_expression(threshold_expression or "")
    if values and max(abs(value) for value in values) >= 60.0:
        return "fahrenheit"

    return "fahrenheit"


def infer_temperature_metric(
    *,
    series_ticker: str,
    market_title: str,
    rules_primary: str,
) -> str:
    normalized_series = _normalize_text(series_ticker).upper()
    if "LOW" in normalized_series:
        return "daily_low"
    if "HIGH" in normalized_series:
        return "daily_high"

    merged = " ".join((_normalize_text(market_title), _normalize_text(rules_primary))).lower()
    if any(token in merged for token in ("daily low", "lowest", "minimum temperature", "overnight low", "low temp")):
        return "daily_low"
    return "daily_high"


def _f_to_c(value_f: float | None) -> float | None:
    if value_f is None:
        return None
    return ((float(value_f) - 32.0) * 5.0) / 9.0


def _local_standard_date(timestamp: datetime, zone: ZoneInfo) -> date:
    local_timestamp = timestamp.astimezone(zone)
    dst_delta = local_timestamp.dst()
    if dst_delta and dst_delta.total_seconds() > 0:
        local_timestamp = local_timestamp - dst_delta
    return local_timestamp.date()


def _parse_iso_datetime(value: Any) -> datetime | None:
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


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _forecast_envelope_for_target_day(
    *,
    station_id: str,
    target_date_local: str,
    timezone_name: str,
    settlement_unit: str,
    timeout_seconds: float,
    cache: dict[str, dict[str, Any]],
    taf_envelopes_by_station: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cache_key = f"{_normalize_text(station_id).upper()}|{target_date_local}|{timezone_name}|{settlement_unit}"
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        return cached

    try:
        payload = fetch_nws_station_hourly_forecast(
            station_id=station_id,
            include_gridpoint_data=False,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        result = {
            "status": "forecast_unavailable",
            "forecast_upper_bound_settlement_raw": None,
            "forecast_lower_bound_settlement_raw": None,
            "periods_for_target_day": 0,
            "upstream_status": f"exception:{exc}",
        }
        cache[cache_key] = result
        return result
    status = _normalize_text(payload.get("status")).lower()
    if status != "ready":
        result = {
            "status": "forecast_unavailable",
            "forecast_upper_bound_settlement_raw": None,
            "forecast_lower_bound_settlement_raw": None,
            "periods_for_target_day": 0,
            "upstream_status": status,
            "taf_status": "unavailable",
            "taf_volatility_score": 0.0,
        }
        cache[cache_key] = result
        return result

    periods = payload.get("periods")
    if not isinstance(periods, list):
        periods = []

    try:
        zone = ZoneInfo(_normalize_text(timezone_name) or "UTC")
    except Exception:
        zone = timezone.utc

    try:
        target_day = datetime.fromisoformat(str(target_date_local)).date()
    except ValueError:
        target_day = None

    day_values_f: list[float] = []
    for period in periods:
        if not isinstance(period, dict):
            continue
        temp_f_raw = period.get("temperature")
        try:
            temp_f = float(temp_f_raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(temp_f):
            continue
        start = _parse_iso_datetime(period.get("startTime"))
        if start is None:
            continue
        if isinstance(target_day, date):
            if _local_standard_date(start, zone) != target_day:
                continue
        day_values_f.append(temp_f)

    if not day_values_f:
        result = {
            "status": "forecast_missing_target_day_periods",
            "forecast_upper_bound_settlement_raw": None,
            "forecast_lower_bound_settlement_raw": None,
            "periods_for_target_day": 0,
            "upstream_status": status,
            "taf_status": "unavailable",
            "taf_volatility_score": 0.0,
        }
        cache[cache_key] = result
        return result

    upper_f = max(day_values_f)
    lower_f = min(day_values_f)
    unit = _normalize_text(settlement_unit).lower()
    if unit == "celsius":
        upper_raw = _f_to_c(upper_f)
        lower_raw = _f_to_c(lower_f)
    else:
        upper_raw = upper_f
        lower_raw = lower_f

    station_taf = None
    if isinstance(taf_envelopes_by_station, dict):
        station_taf = taf_envelopes_by_station.get(_normalize_text(station_id).upper())
    taf_status = _normalize_text((station_taf or {}).get("taf_status")).lower() if isinstance(station_taf, dict) else "unavailable"
    taf_volatility_score = (
        float((station_taf or {}).get("taf_volatility_score"))
        if isinstance(station_taf, dict) and isinstance((station_taf or {}).get("taf_volatility_score"), (int, float))
        else 0.0
    )
    if isinstance(station_taf, dict):
        taf_upper_f = station_taf.get("taf_upper_bound_f")
        taf_lower_f = station_taf.get("taf_lower_bound_f")
        if isinstance(taf_upper_f, (int, float)):
            taf_upper_unit = _f_to_c(float(taf_upper_f)) if unit == "celsius" else float(taf_upper_f)
            upper_raw = max(float(upper_raw), float(taf_upper_unit)) if upper_raw is not None else float(taf_upper_unit)
        if isinstance(taf_lower_f, (int, float)):
            taf_lower_unit = _f_to_c(float(taf_lower_f)) if unit == "celsius" else float(taf_lower_f)
            lower_raw = min(float(lower_raw), float(taf_lower_unit)) if lower_raw is not None else float(taf_lower_unit)

    result = {
        "status": "ready",
        "forecast_upper_bound_settlement_raw": round(float(upper_raw), 6) if upper_raw is not None else None,
        "forecast_lower_bound_settlement_raw": round(float(lower_raw), 6) if lower_raw is not None else None,
        "periods_for_target_day": len(day_values_f),
        "upstream_status": status,
        "taf_status": taf_status or "unavailable",
        "taf_volatility_score": round(float(taf_volatility_score), 6),
    }
    cache[cache_key] = result
    return result


def _latest_station_payload(
    *,
    metar_state: dict[str, Any] | None,
    station_id: str,
) -> dict[str, Any]:
    if not isinstance(metar_state, dict):
        return {}
    latest_by_station = metar_state.get("latest_observation_by_station")
    if not isinstance(latest_by_station, dict):
        return {}
    payload = latest_by_station.get(_normalize_text(station_id).upper())
    if not isinstance(payload, dict):
        return {}
    return payload


def _speci_recent_flag(
    *,
    latest_station_payload: dict[str, Any],
    scanned_at: datetime,
    max_age_minutes: float = 45.0,
) -> bool:
    report_type = _normalize_text(latest_station_payload.get("report_type")).upper()
    if report_type != "SPECI":
        return False
    observed = _parse_iso_datetime(latest_station_payload.get("observation_time_utc"))
    if observed is None:
        return False
    age_minutes = max(0.0, (scanned_at - observed).total_seconds() / 60.0)
    return age_minutes <= float(max_age_minutes)


def _shock_confidence_band(confidence: float) -> str:
    if confidence >= 0.85:
        return "high"
    if confidence >= 0.65:
        return "medium"
    if confidence >= 0.45:
        return "low"
    return "very_low"


def _shock_confidence_scale(confidence: float) -> float:
    band = _shock_confidence_band(confidence)
    if band == "high":
        return 1.0
    if band == "medium":
        return 0.75
    if band == "low":
        return 0.4
    return 0.0


def _load_speci_calibration(path: str | None) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "loaded": False,
        "path": _normalize_text(path),
        "error": "",
        "version": "speci_calibration_v1",
        "confidence_threshold_active": 0.45,
        "min_weight_active": 0.05,
        "explicit_speci_max_age_minutes": 45.0,
        "delta_jump_threshold_c": 2.0,
        "rapid_jump_threshold_c": 1.5,
        "cooldown_min_minutes": 10.0,
        "improvement_hold_minutes": 10.0,
        "severity_multiplier": 1.0,
        "confidence_bias": 0.0,
    }
    path_text = _normalize_text(path)
    if not path_text:
        return defaults
    calibration_path = Path(path_text)
    if not calibration_path.exists():
        defaults["error"] = "missing_speci_calibration_file"
        return defaults
    payload = _read_json(calibration_path)
    if not payload:
        defaults["error"] = "invalid_speci_calibration_payload"
        return defaults

    def _get_positive(name: str, fallback: float) -> float:
        parsed = _parse_float(payload.get(name))
        if not isinstance(parsed, float):
            return fallback
        return max(0.0, float(parsed))

    calibrated = dict(defaults)
    calibrated["loaded"] = True
    calibrated["version"] = _normalize_text(payload.get("version")) or defaults["version"]
    calibrated["confidence_threshold_active"] = min(
        0.95,
        max(0.2, _get_positive("confidence_threshold_active", defaults["confidence_threshold_active"])),
    )
    calibrated["min_weight_active"] = min(1.0, max(0.0, _get_positive("min_weight_active", defaults["min_weight_active"])))
    calibrated["explicit_speci_max_age_minutes"] = min(
        180.0,
        max(5.0, _get_positive("explicit_speci_max_age_minutes", defaults["explicit_speci_max_age_minutes"])),
    )
    calibrated["delta_jump_threshold_c"] = min(
        8.0,
        max(0.25, _get_positive("delta_jump_threshold_c", defaults["delta_jump_threshold_c"])),
    )
    calibrated["rapid_jump_threshold_c"] = min(
        calibrated["delta_jump_threshold_c"],
        max(0.15, _get_positive("rapid_jump_threshold_c", defaults["rapid_jump_threshold_c"])),
    )
    calibrated["cooldown_min_minutes"] = min(
        60.0,
        max(1.0, _get_positive("cooldown_min_minutes", defaults["cooldown_min_minutes"])),
    )
    calibrated["improvement_hold_minutes"] = min(
        60.0,
        max(1.0, _get_positive("improvement_hold_minutes", defaults["improvement_hold_minutes"])),
    )
    calibrated["severity_multiplier"] = min(
        2.0,
        max(0.25, _get_positive("severity_multiplier", defaults["severity_multiplier"])),
    )
    confidence_bias = _parse_float(payload.get("confidence_bias"))
    calibrated["confidence_bias"] = max(-0.35, min(0.35, float(confidence_bias))) if isinstance(confidence_bias, float) else 0.0
    return calibrated


def _speci_shock_profile(
    *,
    latest_station_payload: dict[str, Any],
    station_interval_stats_payload: dict[str, Any] | None,
    scanned_at: datetime,
    temperature_metric: str,
    calibration: dict[str, Any] | None = None,
) -> dict[str, Any]:
    calibration_payload = calibration if isinstance(calibration, dict) else {}
    explicit_speci_max_age_minutes = _parse_float(calibration_payload.get("explicit_speci_max_age_minutes"))
    if not isinstance(explicit_speci_max_age_minutes, float):
        explicit_speci_max_age_minutes = 45.0
    delta_jump_threshold_c = _parse_float(calibration_payload.get("delta_jump_threshold_c"))
    if not isinstance(delta_jump_threshold_c, float):
        delta_jump_threshold_c = 2.0
    rapid_jump_threshold_c = _parse_float(calibration_payload.get("rapid_jump_threshold_c"))
    if not isinstance(rapid_jump_threshold_c, float):
        rapid_jump_threshold_c = 1.5
    rapid_jump_threshold_c = min(delta_jump_threshold_c, rapid_jump_threshold_c)
    cooldown_min_minutes = _parse_float(calibration_payload.get("cooldown_min_minutes"))
    if not isinstance(cooldown_min_minutes, float):
        cooldown_min_minutes = 10.0
    improvement_hold_minutes = _parse_float(calibration_payload.get("improvement_hold_minutes"))
    if not isinstance(improvement_hold_minutes, float):
        improvement_hold_minutes = 10.0
    min_weight_active = _parse_float(calibration_payload.get("min_weight_active"))
    if not isinstance(min_weight_active, float):
        min_weight_active = 0.05
    confidence_threshold_active = _parse_float(calibration_payload.get("confidence_threshold_active"))
    if not isinstance(confidence_threshold_active, float):
        confidence_threshold_active = 0.45
    severity_multiplier = _parse_float(calibration_payload.get("severity_multiplier"))
    if not isinstance(severity_multiplier, float):
        severity_multiplier = 1.0
    confidence_bias = _parse_float(calibration_payload.get("confidence_bias"))
    if not isinstance(confidence_bias, float):
        confidence_bias = 0.0

    report_type = _normalize_text(latest_station_payload.get("report_type")).upper()
    observed = _parse_iso_datetime(latest_station_payload.get("observation_time_utc"))
    if observed is None:
        return {
            "active": False,
            "confidence": 0.0,
            "weight": 0.0,
            "mode": "unavailable",
            "trigger_count": 0,
            "trigger_families": "",
            "persistence_ok": False,
            "cooldown_blocked": False,
            "improvement_hold_active": False,
            "delta_temp_c": None,
            "delta_minutes": None,
            "decay_tau_minutes": 15.0,
        }

    observation_age_minutes = max(0.0, (scanned_at - observed).total_seconds() / 60.0)
    temp_now = latest_station_payload.get("temp_c")
    if not isinstance(temp_now, (int, float)):
        temp_now = None
    temp_prev = latest_station_payload.get("previous_temp_c")
    if not isinstance(temp_prev, (int, float)):
        temp_prev = None
    previous_observed = _parse_iso_datetime(latest_station_payload.get("previous_observation_time_utc"))
    delta_temp_c: float | None = None
    delta_minutes: float | None = None
    if isinstance(temp_now, (int, float)) and isinstance(temp_prev, (int, float)):
        delta_temp_c = round(float(temp_now) - float(temp_prev), 6)
    if previous_observed is not None:
        delta_minutes = max(0.0, (observed - previous_observed).total_seconds() / 60.0)

    abs_delta_temp = abs(float(delta_temp_c)) if isinstance(delta_temp_c, float) else 0.0
    explicit_speci = report_type == "SPECI" and observation_age_minutes <= float(explicit_speci_max_age_minutes)

    trigger_families: list[str] = []
    if explicit_speci:
        trigger_families.append("explicit_speci")
    if isinstance(delta_minutes, float) and delta_minutes <= 30.0 and abs_delta_temp >= float(delta_jump_threshold_c):
        trigger_families.append("temperature_jump")
    if isinstance(delta_minutes, float) and delta_minutes <= 15.0 and abs_delta_temp >= float(rapid_jump_threshold_c):
        trigger_families.append("rapid_temp_jump")

    latest_interval = None
    interval_median = None
    if isinstance(station_interval_stats_payload, dict):
        latest_interval = _parse_float(station_interval_stats_payload.get("latest_interval_minutes"))
        interval_median = _parse_float(station_interval_stats_payload.get("interval_median_minutes"))
    if isinstance(latest_interval, float) and isinstance(interval_median, float):
        if interval_median > 0 and latest_interval <= max(2.0, interval_median * 0.6):
            trigger_families.append("cadence_discontinuity")

    # Deduplicate while preserving order.
    deduped_trigger_families: list[str] = []
    for family in trigger_families:
        if family not in deduped_trigger_families:
            deduped_trigger_families.append(family)

    has_temperature_trigger = any(
        family in {"temperature_jump", "rapid_temp_jump", "explicit_speci"}
        for family in deduped_trigger_families
    )
    persistence_ok = explicit_speci or (isinstance(delta_minutes, float) and delta_minutes >= 2.0)

    metric = _normalize_text(temperature_metric).lower() or "daily_high"
    directional_deterioration: bool | None = None
    if isinstance(delta_temp_c, float):
        if metric == "daily_low":
            directional_deterioration = delta_temp_c < 0.0
        else:
            directional_deterioration = delta_temp_c > 0.0

    improvement_hold_active = bool(
        has_temperature_trigger
        and (directional_deterioration is False)
        and not explicit_speci
        and isinstance(delta_minutes, float)
        and delta_minutes < float(improvement_hold_minutes)
    )

    corroborators = len([family for family in deduped_trigger_families if family != "explicit_speci"])
    severity_floor = 0.5 if explicit_speci else 0.0
    severity_base = max(severity_floor, (abs_delta_temp / 2.0)) + min(0.9, 0.35 * corroborators)
    cooldown_blocked = bool(
        has_temperature_trigger
        and not explicit_speci
        and isinstance(delta_minutes, float)
        and delta_minutes < float(cooldown_min_minutes)
        and severity_base < 1.6
    )

    if explicit_speci:
        confidence = 0.70
    elif any(family == "temperature_jump" for family in deduped_trigger_families):
        confidence = 0.52
    elif has_temperature_trigger:
        confidence = 0.45
    else:
        confidence = 0.20
    confidence += min(0.30, 0.10 * corroborators)
    if persistence_ok:
        confidence += 0.10
    if abs_delta_temp >= 3.0:
        confidence += 0.10
    if not persistence_ok:
        confidence -= 0.15
    if observation_age_minutes > 30.0:
        confidence -= 0.10
    confidence += float(confidence_bias)
    confidence = max(0.0, min(1.0, float(confidence)))
    confidence_scale = _shock_confidence_scale(confidence)
    if confidence < float(confidence_threshold_active):
        confidence_scale = 0.0
    elif confidence_scale <= 0.0:
        confidence_scale = 0.4

    if len(deduped_trigger_families) >= 3:
        decay_tau_minutes = 45.0
    elif any(family == "temperature_jump" for family in deduped_trigger_families):
        decay_tau_minutes = 30.0
    else:
        decay_tau_minutes = 15.0

    accepted = bool(
        has_temperature_trigger
        and persistence_ok
        and not cooldown_blocked
        and not improvement_hold_active
        and confidence_scale > 0.0
    )
    weight_0 = severity_base * confidence_scale * max(0.25, float(severity_multiplier))
    weight = 0.0
    if accepted and weight_0 > 0.0:
        weight = weight_0 * math.exp(-observation_age_minutes / max(1.0, decay_tau_minutes))

    return {
        "active": bool(accepted and confidence >= float(confidence_threshold_active) and weight >= float(min_weight_active)),
        "confidence": round(confidence, 6),
        "weight": round(max(0.0, weight), 6),
        "mode": "operational" if accepted else ("suppressed" if has_temperature_trigger else "inactive"),
        "trigger_count": len(deduped_trigger_families),
        "trigger_families": ",".join(deduped_trigger_families),
        "persistence_ok": bool(persistence_ok),
        "cooldown_blocked": bool(cooldown_blocked),
        "improvement_hold_active": bool(improvement_hold_active),
        "delta_temp_c": round(float(delta_temp_c), 6) if isinstance(delta_temp_c, float) else None,
        "delta_minutes": round(float(delta_minutes), 6) if isinstance(delta_minutes, float) else None,
        "decay_tau_minutes": round(float(decay_tau_minutes), 6),
        "calibration_version": _normalize_text(calibration_payload.get("version")) or "speci_calibration_v1",
    }


def evaluate_temperature_constraint(
    *,
    threshold_expression: str,
    observed_value: float | None,
    temperature_metric: str = "daily_high",
    forecast_upper_bound: float | None = None,
    forecast_lower_bound: float | None = None,
) -> tuple[str, str]:
    if observed_value is None:
        if temperature_metric == "daily_low":
            return ("no_observation", "No observed minimum for target day yet.")
        return ("no_observation", "No observed maximum for target day yet.")

    kind, values = _normalize_threshold_expression(threshold_expression)
    if not kind:
        return ("unsupported_threshold", "Threshold expression unavailable or unparsable.")

    obs = float(observed_value)
    metric = _normalize_text(temperature_metric).lower()
    if metric == "daily_low":
        if kind == "at_most" and len(values) >= 1:
            limit = values[0]
            if obs <= limit:
                return ("yes_likely_locked", f"Observed min {obs:g} already at or below at_most threshold {limit:g}.")
            if (
                isinstance(forecast_lower_bound, (int, float))
                and float(forecast_lower_bound) > limit
            ):
                return (
                    "yes_impossible",
                    f"Forecast lower bound {float(forecast_lower_bound):g} stays above at_most threshold {limit:g}.",
                )
            return ("no_signal", "At-most threshold still feasible if temperatures drop further.")

        if kind == "below" and len(values) >= 1:
            limit = values[0]
            if obs < limit:
                return ("yes_likely_locked", f"Observed min {obs:g} already below {limit:g}.")
            if (
                isinstance(forecast_lower_bound, (int, float))
                and float(forecast_lower_bound) >= limit
            ):
                return (
                    "yes_impossible",
                    f"Forecast lower bound {float(forecast_lower_bound):g} does not break below {limit:g}.",
                )
            return ("no_signal", "Below-threshold condition still feasible if temperatures fall.")

        if kind == "at_least" and len(values) >= 1:
            floor = values[0]
            if obs < floor:
                return ("yes_impossible", f"Observed min {obs:g} already below at_least floor {floor:g}.")
            return ("no_signal", "At-least threshold currently feasible but can fail on future lower prints.")

        if kind == "above" and len(values) >= 1:
            floor = values[0]
            if obs <= floor:
                return ("yes_impossible", f"Observed min {obs:g} is not above {floor:g}.")
            return ("no_signal", "Above-threshold currently feasible but can fail on future lower prints.")

        if kind == "between" and len(values) >= 2:
            low = min(values[0], values[1])
            high = max(values[0], values[1])
            if obs < low:
                return ("yes_impossible", f"Observed min {obs:g} already below between lower bound {low:g}.")
            if (
                obs > high
                and isinstance(forecast_lower_bound, (int, float))
                and float(forecast_lower_bound) > high
            ):
                return (
                    "yes_impossible",
                    f"Forecast lower bound {float(forecast_lower_bound):g} remains above between upper bound {high:g}.",
                )
            return ("no_signal", "Between range remains path-dependent for the final low.")

        if kind == "equal" and len(values) >= 1:
            target = values[0]
            if obs < target:
                return ("yes_impossible", f"Observed min {obs:g} already below equal target {target:g}.")
            if (
                obs > target
                and isinstance(forecast_lower_bound, (int, float))
                and float(forecast_lower_bound) > target
            ):
                return (
                    "yes_impossible",
                    f"Forecast lower bound {float(forecast_lower_bound):g} stays above equal target {target:g}.",
                )
            return ("no_signal", "Equal target still feasible if observed lows move accordingly.")

        return ("unsupported_threshold", "Threshold kind unsupported for daily_low metric.")

    if kind == "at_most" and len(values) >= 1:
        limit = values[0]
        if obs > limit:
            return ("yes_impossible", f"Observed max {obs:g} exceeds at_most threshold {limit:g}.")
        return ("no_signal", "At-most threshold still feasible.")

    if kind == "below" and len(values) >= 1:
        limit = values[0]
        if obs >= limit:
            return ("yes_impossible", f"Observed max {obs:g} is not below {limit:g}.")
        return ("no_signal", "Below-threshold condition still feasible.")

    if kind == "at_least" and len(values) >= 1:
        floor = values[0]
        if obs >= floor:
            return ("yes_likely_locked", f"Observed max {obs:g} already meets at_least {floor:g}.")
        if (
            isinstance(forecast_upper_bound, (int, float))
            and float(forecast_upper_bound) < floor
        ):
            return (
                "yes_impossible",
                f"Forecast upper bound {float(forecast_upper_bound):g} cannot reach at_least floor {floor:g}.",
            )
        return ("no_signal", "At-least threshold not reached yet.")

    if kind == "above" and len(values) >= 1:
        floor = values[0]
        if obs > floor:
            return ("yes_likely_locked", f"Observed max {obs:g} already above {floor:g}.")
        if (
            isinstance(forecast_upper_bound, (int, float))
            and float(forecast_upper_bound) <= floor
        ):
            return (
                "yes_impossible",
                f"Forecast upper bound {float(forecast_upper_bound):g} does not exceed above threshold {floor:g}.",
            )
        return ("no_signal", "Above-threshold condition not reached yet.")

    if kind == "between" and len(values) >= 2:
        low = min(values[0], values[1])
        high = max(values[0], values[1])
        if obs > high:
            return ("yes_impossible", f"Observed max {obs:g} already above between upper bound {high:g}.")
        if low <= obs <= high:
            return ("no_signal", "Between range currently satisfied but can still break later.")
        if (
            obs < low
            and isinstance(forecast_upper_bound, (int, float))
            and float(forecast_upper_bound) < low
        ):
            return (
                "yes_impossible",
                f"Forecast upper bound {float(forecast_upper_bound):g} remains below between lower bound {low:g}.",
            )
        return ("no_signal", "Between range still feasible.")

    if kind == "equal" and len(values) >= 1:
        target = values[0]
        if obs > target:
            return ("yes_impossible", f"Observed max {obs:g} already above equal target {target:g}.")
        if obs == target:
            return ("no_signal", "Equal target currently matched but can still move.")
        if (
            isinstance(forecast_upper_bound, (int, float))
            and float(forecast_upper_bound) < target
        ):
            return (
                "yes_impossible",
                f"Forecast upper bound {float(forecast_upper_bound):g} cannot reach equal target {target:g}.",
            )
        return ("no_signal", "Equal target still feasible.")

    return ("unsupported_threshold", "Threshold kind unsupported by constraint engine.")


def _find_latest_specs_csv(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob("kalshi_temperature_contract_specs_*.csv"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _read_specs_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_constraints_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CONSTRAINT_SCAN_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _load_metar_state(output_dir: str) -> dict[str, Any] | None:
    state_path = Path(output_dir) / "kalshi_temperature_metar_state.json"
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _snapshot_latest_observation_time(snapshot: dict[str, Any]) -> datetime | None:
    latest_observation = _parse_iso_datetime(snapshot.get("latest_observation_time_utc"))
    if latest_observation is not None:
        return latest_observation
    observations = snapshot.get("observations")
    if not isinstance(observations, list):
        return None
    latest_from_rows: datetime | None = None
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        parsed = _parse_iso_datetime(observation.get("timestamp"))
        if parsed is None:
            continue
        if latest_from_rows is None or parsed > latest_from_rows:
            latest_from_rows = parsed
    return latest_from_rows


def _snapshot_quality_gate(
    *,
    snapshot: dict[str, Any],
    scanned_at: datetime,
    target_date_local: str,
    timezone_name: str,
) -> tuple[bool, str]:
    snapshot_status = _normalize_text(snapshot.get("status")).lower()
    error_text = _normalize_text(snapshot.get("error"))
    if not snapshot_status:
        return (False, "snapshot_status_missing")
    if snapshot_status != "ready":
        reason = f"snapshot_status_{snapshot_status}"
        if error_text:
            reason = f"{reason}:{error_text}"
        return (False, reason)
    if any(token in snapshot_status for token in _DEGRADED_SNAPSHOT_STATUS_TOKENS):
        reason = f"snapshot_status_{snapshot_status}"
        if error_text:
            reason = f"{reason}:{error_text}"
        return (False, reason)

    observations_for_date = 0
    try:
        observations_for_date = int(snapshot.get("observations_for_date") or 0)
    except (TypeError, ValueError):
        observations_for_date = 0
    if observations_for_date <= 0:
        return (False, "snapshot_no_observations_for_target_day")

    try:
        zone = ZoneInfo(_normalize_text(timezone_name) or "UTC")
    except Exception:
        zone = timezone.utc
    try:
        target_day = datetime.fromisoformat(_normalize_text(target_date_local)).date()
    except ValueError:
        return (False, "snapshot_invalid_target_date_local")

    if target_day == _local_standard_date(scanned_at, zone):
        latest_observed = _snapshot_latest_observation_time(snapshot)
        if latest_observed is None:
            return (False, "snapshot_missing_latest_observation_time_for_active_day")
        if latest_observed > scanned_at + timedelta(minutes=5.0):
            return (False, "snapshot_future_latest_observation_time")
        age_minutes = max(0.0, (scanned_at - latest_observed).total_seconds() / 60.0)
        if age_minutes > _SNAPSHOT_ACTIVE_DAY_MAX_AGE_MINUTES:
            return (False, f"snapshot_stale_for_active_day_age_minutes:{age_minutes:.1f}")

    return (True, "")


def _parse_threshold_value(threshold_expression: str) -> float | None:
    kind, values = _normalize_threshold_expression(threshold_expression)
    if kind in {"above", "at_least", "at_most", "below", "equal"} and values:
        return float(values[0])
    return None


def _parse_threshold_bounds(
    threshold_expression: str,
) -> tuple[str, float | None, float | None]:
    kind, values = _normalize_threshold_expression(threshold_expression)
    if kind in {"above", "at_least", "at_most", "below", "equal"} and values:
        value = float(values[0])
        return (kind, value, value)
    if kind == "between" and len(values) >= 2:
        low = float(min(values[0], values[1]))
        high = float(max(values[0], values[1]))
        return (kind, low, high)
    return (kind, None, None)


def _finite_or_none(value: float | int | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(parsed):
        return parsed
    return None


def _yes_interval_bounds(
    *,
    threshold_kind: str,
    threshold_lower: float | None,
    threshold_upper: float | None,
) -> tuple[float, float] | None:
    strict_epsilon = 1e-9
    kind = _normalize_text(threshold_kind).lower()
    if kind == "above" and isinstance(threshold_lower, float):
        return (float(threshold_lower) + strict_epsilon, float("inf"))
    if kind == "at_least" and isinstance(threshold_lower, float):
        return (float(threshold_lower), float("inf"))
    if kind == "below" and isinstance(threshold_upper, float):
        return (float("-inf"), float(threshold_upper) - strict_epsilon)
    if kind == "at_most" and isinstance(threshold_upper, float):
        return (float("-inf"), float(threshold_upper))
    if kind == "between" and isinstance(threshold_lower, float) and isinstance(threshold_upper, float):
        return (float(threshold_lower), float(threshold_upper))
    if kind == "equal" and isinstance(threshold_lower, float):
        value = float(threshold_lower)
        return (value, value)
    return None


def _possible_final_interval(
    *,
    temperature_metric: str,
    observed_value: float | None,
    forecast_upper_bound: float | None,
    forecast_lower_bound: float | None,
) -> tuple[float, float] | None:
    observed = _finite_or_none(observed_value)
    if observed is None:
        return None
    metric = _normalize_text(temperature_metric).lower()
    if metric == "daily_low":
        low = _finite_or_none(forecast_lower_bound)
        high = observed
        if low is None:
            low = float("-inf")
        if low > high:
            low = high
        return (low, high)

    low = observed
    high = _finite_or_none(forecast_upper_bound)
    if high is None:
        high = float("inf")
    if high < low:
        high = low
    return (low, high)


def _alpha_feature_pack(
    *,
    threshold_expression: str,
    threshold_kind: str,
    threshold_lower: float | None,
    threshold_upper: float | None,
    temperature_metric: str,
    observed_value: float | None,
    forecast_upper_bound: float | None,
    forecast_lower_bound: float | None,
) -> dict[str, float | int | str | None]:
    observed = _finite_or_none(observed_value)
    forecast_upper = _finite_or_none(forecast_upper_bound)
    forecast_lower = _finite_or_none(forecast_lower_bound)
    metric = _normalize_text(temperature_metric).lower()
    forecast_range_width: float | None = None
    if isinstance(forecast_upper, float) and isinstance(forecast_lower, float):
        forecast_range_width = forecast_upper - forecast_lower

    yes_interval = _yes_interval_bounds(
        threshold_kind=threshold_kind,
        threshold_lower=threshold_lower,
        threshold_upper=threshold_upper,
    )
    possible_interval = _possible_final_interval(
        temperature_metric=temperature_metric,
        observed_value=observed,
        forecast_upper_bound=forecast_upper,
        forecast_lower_bound=forecast_lower,
    )

    yes_overlap_flag = 0
    yes_gap: float | None = None
    if yes_interval is not None and possible_interval is not None:
        yes_low, yes_high = yes_interval
        possible_low, possible_high = possible_interval
        overlap_low = max(yes_low, possible_low)
        overlap_high = min(yes_high, possible_high)
        if overlap_low <= overlap_high:
            yes_overlap_flag = 1
            yes_gap = 0.0
        else:
            yes_overlap_flag = 0
            yes_gap = overlap_low - overlap_high

    observed_to_lower: float | None = None
    if isinstance(observed, float) and isinstance(threshold_lower, float):
        observed_to_lower = observed - threshold_lower
    observed_to_upper: float | None = None
    if isinstance(observed, float) and isinstance(threshold_upper, float):
        observed_to_upper = threshold_upper - observed

    kind = _normalize_text(threshold_kind).lower()
    primary_signal_margin: float | None = None
    if kind in {"above", "at_least"}:
        primary_signal_margin = observed_to_lower
    elif kind in {"below", "at_most"}:
        primary_signal_margin = observed_to_upper
    elif kind == "between" and isinstance(observed_to_lower, float) and isinstance(observed_to_upper, float):
        primary_signal_margin = min(observed_to_lower, observed_to_upper)
    elif kind == "equal" and isinstance(observed, float) and isinstance(threshold_lower, float):
        primary_signal_margin = -abs(observed - threshold_lower)

    forecast_feasibility_margin: float | None = None
    if kind in {"above", "at_least"} and isinstance(forecast_upper, float) and isinstance(threshold_lower, float):
        forecast_feasibility_margin = forecast_upper - threshold_lower
    elif kind in {"below", "at_most"} and isinstance(forecast_lower, float) and isinstance(threshold_upper, float):
        forecast_feasibility_margin = threshold_upper - forecast_lower
    elif kind == "between":
        margins: list[float] = []
        if isinstance(forecast_upper, float) and isinstance(threshold_lower, float):
            margins.append(forecast_upper - threshold_lower)
        if isinstance(forecast_lower, float) and isinstance(threshold_upper, float):
            margins.append(threshold_upper - forecast_lower)
        if margins:
            forecast_feasibility_margin = min(margins)
    elif kind == "equal" and isinstance(threshold_lower, float):
        if metric == "daily_low" and isinstance(forecast_lower, float):
            forecast_feasibility_margin = threshold_lower - forecast_lower
        elif metric != "daily_low" and isinstance(forecast_upper, float):
            forecast_feasibility_margin = forecast_upper - threshold_lower

    possible_low_out = _finite_or_none(possible_interval[0]) if possible_interval is not None else None
    possible_high_out = _finite_or_none(possible_interval[1]) if possible_interval is not None else None
    yes_low_out = _finite_or_none(yes_interval[0]) if yes_interval is not None else None
    yes_high_out = _finite_or_none(yes_interval[1]) if yes_interval is not None else None

    return {
        "threshold_kind": kind,
        "threshold_lower_bound": threshold_lower,
        "threshold_upper_bound": threshold_upper,
        "forecast_range_width": forecast_range_width,
        "possible_final_lower_bound": possible_low_out,
        "possible_final_upper_bound": possible_high_out,
        "yes_interval_lower_bound": yes_low_out,
        "yes_interval_upper_bound": yes_high_out,
        "yes_possible_overlap": yes_overlap_flag,
        "yes_possible_gap": yes_gap,
        "observed_distance_to_lower_bound": observed_to_lower,
        "observed_distance_to_upper_bound": observed_to_upper,
        "primary_signal_margin": primary_signal_margin,
        "forecast_feasibility_margin": forecast_feasibility_margin,
    }


def _monotonic_consistency_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
        kind, _ = _normalize_threshold_expression(_normalize_text(row.get("threshold_expression")))
        if kind not in {"above", "at_least", "below", "at_most"}:
            continue
        threshold = _parse_threshold_value(_normalize_text(row.get("threshold_expression")))
        if threshold is None:
            continue
        group_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
                metric,
            )
        )
        grouped.setdefault(group_key, []).append(
            {
                "market_ticker": _normalize_text(row.get("market_ticker")),
                "kind": kind,
                "threshold": threshold,
                "constraint_status": _normalize_text(row.get("constraint_status")).lower(),
            }
        )

    violations: list[dict[str, Any]] = []
    checked_groups = 0
    for group_key, group_rows in grouped.items():
        if len(group_rows) < 2:
            continue
        checked_groups += 1
        lower_bounded = [
            row for row in group_rows if _normalize_text(row.get("kind")).lower() in {"above", "at_least"}
        ]
        upper_bounded = [
            row for row in group_rows if _normalize_text(row.get("kind")).lower() in {"below", "at_most"}
        ]

        ordered_lower = sorted(
            lower_bounded,
            key=lambda item: (float(item.get("threshold") or 0.0), item.get("market_ticker") or ""),
        )
        saw_lower_impossible = False
        lower_anchor = None
        for row in ordered_lower:
            status = _normalize_text(row.get("constraint_status")).lower()
            if status == "yes_impossible":
                saw_lower_impossible = True
                lower_anchor = row
                continue
            if saw_lower_impossible:
                violations.append(
                    {
                        "group_key": group_key,
                        "lower_market_ticker": _normalize_text((lower_anchor or {}).get("market_ticker")),
                        "lower_threshold": (lower_anchor or {}).get("threshold"),
                        "higher_market_ticker": _normalize_text(row.get("market_ticker")),
                        "higher_threshold": row.get("threshold"),
                        "higher_constraint_status": status,
                        "reason": "higher_threshold_not_impossible_after_lower_marked_impossible",
                    }
                )
                break

        saw_higher_locked = False
        higher_anchor = None
        for row in reversed(ordered_lower):
            status = _normalize_text(row.get("constraint_status")).lower()
            if status == "yes_likely_locked":
                saw_higher_locked = True
                higher_anchor = row
                continue
            if saw_higher_locked:
                violations.append(
                    {
                        "group_key": group_key,
                        "lower_market_ticker": _normalize_text(row.get("market_ticker")),
                        "lower_threshold": row.get("threshold"),
                        "higher_market_ticker": _normalize_text((higher_anchor or {}).get("market_ticker")),
                        "higher_threshold": (higher_anchor or {}).get("threshold"),
                        "higher_constraint_status": _normalize_text((higher_anchor or {}).get("constraint_status")).lower(),
                        "reason": "lower_threshold_not_locked_after_higher_marked_locked",
                    }
                )
                break

        ordered_upper = sorted(
            upper_bounded,
            key=lambda item: (float(item.get("threshold") or 0.0), item.get("market_ticker") or ""),
        )
        saw_higher_impossible = False
        higher_anchor = None
        for row in reversed(ordered_upper):
            status = _normalize_text(row.get("constraint_status")).lower()
            if status == "yes_impossible":
                saw_higher_impossible = True
                higher_anchor = row
                continue
            if saw_higher_impossible:
                violations.append(
                    {
                        "group_key": group_key,
                        "lower_market_ticker": _normalize_text(row.get("market_ticker")),
                        "lower_threshold": row.get("threshold"),
                        "higher_market_ticker": _normalize_text((higher_anchor or {}).get("market_ticker")),
                        "higher_threshold": (higher_anchor or {}).get("threshold"),
                        "higher_constraint_status": _normalize_text((higher_anchor or {}).get("constraint_status")).lower(),
                        "reason": "lower_threshold_not_impossible_after_higher_marked_impossible",
                    }
                )
                break

        saw_lower_locked = False
        lower_anchor = None
        for row in ordered_upper:
            status = _normalize_text(row.get("constraint_status")).lower()
            if status == "yes_likely_locked":
                saw_lower_locked = True
                lower_anchor = row
                continue
            if saw_lower_locked:
                violations.append(
                    {
                        "group_key": group_key,
                        "lower_market_ticker": _normalize_text((lower_anchor or {}).get("market_ticker")),
                        "lower_threshold": (lower_anchor or {}).get("threshold"),
                        "higher_market_ticker": _normalize_text(row.get("market_ticker")),
                        "higher_threshold": row.get("threshold"),
                        "higher_constraint_status": status,
                        "reason": "higher_threshold_not_locked_after_lower_marked_locked",
                    }
                )
                break

    return {
        "checked_groups": checked_groups,
        "violations_count": len(violations),
        "violations": violations[:25],
    }


def _exact_strike_chain_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
        group_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
                metric,
            )
        )
        grouped.setdefault(group_key, []).append(row)

    checked_groups = 0
    violations: list[dict[str, Any]] = []
    for group_key, group_rows in grouped.items():
        upper_impossible_anchor = None
        lower_impossible_anchor = None
        for row in group_rows:
            status = _normalize_text(row.get("constraint_status")).lower()
            if status != "yes_impossible":
                continue
            kind, low, high = _parse_threshold_bounds(_normalize_text(row.get("threshold_expression")))
            if kind in {"at_most", "below"} and isinstance(high, float):
                upper_impossible_anchor = (
                    max(float(upper_impossible_anchor), high)
                    if isinstance(upper_impossible_anchor, (int, float))
                    else high
                )
            if kind in {"at_least", "above"} and isinstance(low, float):
                lower_impossible_anchor = (
                    min(float(lower_impossible_anchor), low)
                    if isinstance(lower_impossible_anchor, (int, float))
                    else low
                )

        if upper_impossible_anchor is None and lower_impossible_anchor is None:
            continue
        checked_groups += 1

        for row in group_rows:
            status = _normalize_text(row.get("constraint_status")).lower()
            if status == "yes_impossible":
                continue
            threshold_expression = _normalize_text(row.get("threshold_expression"))
            kind, low, high = _parse_threshold_bounds(threshold_expression)
            if kind not in {"equal", "between"}:
                continue

            if isinstance(upper_impossible_anchor, (int, float)) and isinstance(high, float) and high <= float(upper_impossible_anchor):
                violations.append(
                    {
                        "group_key": group_key,
                        "market_ticker": _normalize_text(row.get("market_ticker")),
                        "threshold_expression": threshold_expression,
                        "constraint_status": status,
                        "anchor_type": "upper_impossible_chain",
                        "anchor_threshold": float(upper_impossible_anchor),
                        "reason": "exact_or_bracket_threshold_not_marked_impossible_after_upper_chain_anchor",
                    }
                )
                continue

            if isinstance(lower_impossible_anchor, (int, float)) and isinstance(low, float) and low >= float(lower_impossible_anchor):
                violations.append(
                    {
                        "group_key": group_key,
                        "market_ticker": _normalize_text(row.get("market_ticker")),
                        "threshold_expression": threshold_expression,
                        "constraint_status": status,
                        "anchor_type": "lower_impossible_chain",
                        "anchor_threshold": float(lower_impossible_anchor),
                        "reason": "exact_or_bracket_threshold_not_marked_impossible_after_lower_chain_anchor",
                    }
                )

    return {
        "checked_groups": checked_groups,
        "violations_count": len(violations),
        "violations": violations[:25],
    }


def _range_family_consistency_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    cross_metric_grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
        group_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
                metric,
            )
        )
        grouped.setdefault(group_key, []).append(row)

        base_key = "|".join(
            (
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
            )
        )
        observed = row.get("observed_metric_settlement_quantized")
        try:
            observed_value = float(observed)
        except (TypeError, ValueError):
            observed_value = None
        if isinstance(observed_value, float):
            entry = cross_metric_grouped.setdefault(base_key, {})
            if metric == "daily_high":
                entry["daily_high"] = observed_value
            elif metric == "daily_low":
                entry["daily_low"] = observed_value

    interval_conflicts: list[dict[str, Any]] = []
    checked_groups = 0
    for group_key, group_rows in grouped.items():
        locked_rows = [
            row for row in group_rows if _normalize_text(row.get("constraint_status")).lower() == "yes_likely_locked"
        ]
        if len(locked_rows) < 2:
            continue
        checked_groups += 1
        low_bound = float("-inf")
        high_bound = float("inf")
        for row in locked_rows:
            kind, low, high = _parse_threshold_bounds(_normalize_text(row.get("threshold_expression")))
            if kind in {"above", "at_least", "between", "equal"} and isinstance(low, float):
                low_bound = max(low_bound, low)
            if kind in {"below", "at_most", "between", "equal"} and isinstance(high, float):
                high_bound = min(high_bound, high)
        if low_bound > high_bound:
            interval_conflicts.append(
                {
                    "group_key": group_key,
                    "locked_markets": [
                        {
                            "market_ticker": _normalize_text(row.get("market_ticker")),
                            "threshold_expression": _normalize_text(row.get("threshold_expression")),
                            "constraint_status": _normalize_text(row.get("constraint_status")).lower(),
                        }
                        for row in locked_rows[:12]
                    ],
                    "intersection_low_bound": low_bound,
                    "intersection_high_bound": high_bound,
                    "reason": "locked_intervals_have_empty_intersection",
                }
            )

    cross_metric_conflicts: list[dict[str, Any]] = []
    for base_key, values in cross_metric_grouped.items():
        daily_high = values.get("daily_high")
        daily_low = values.get("daily_low")
        if isinstance(daily_high, float) and isinstance(daily_low, float) and daily_low > daily_high:
            cross_metric_conflicts.append(
                {
                    "station_day_key": base_key,
                    "daily_low_observed": daily_low,
                    "daily_high_observed": daily_high,
                    "reason": "daily_low_observed_exceeds_daily_high_observed",
                }
            )

    interval_conflicted_group_keys = sorted(
        {
            _normalize_text(item.get("group_key"))
            for item in interval_conflicts
            if _normalize_text(item.get("group_key"))
        }
    )
    cross_metric_conflicted_station_day_keys = sorted(
        {
            _normalize_text(item.get("station_day_key"))
            for item in cross_metric_conflicts
            if _normalize_text(item.get("station_day_key"))
        }
    )
    violations = interval_conflicts + cross_metric_conflicts
    return {
        "checked_groups": checked_groups,
        "locked_interval_conflicts_count": len(interval_conflicts),
        "cross_metric_conflicts_count": len(cross_metric_conflicts),
        "interval_conflicted_group_keys": interval_conflicted_group_keys,
        "cross_metric_conflicted_station_day_keys": cross_metric_conflicted_station_day_keys,
        "violations_count": len(violations),
        "violations": violations[:25],
    }


def _constraint_signal_strength(row: dict[str, Any]) -> float:
    status = _normalize_text(row.get("constraint_status")).lower()
    status_score = {
        "yes_impossible": 1.2,
        "no_interval_infeasible": 1.0,
        "no_monotonic_chain": 0.95,
        "yes_likely_locked": 0.8,
        "yes_interval_certain": 0.7,
        "yes_monotonic_chain": 0.65,
    }.get(status, 0.0)
    settlement_confidence = _parse_float(row.get("settlement_confidence_score")) or 0.0
    primary_margin = abs(_parse_float(row.get("primary_signal_margin")) or 0.0)
    forecast_margin = abs(_parse_float(row.get("forecast_feasibility_margin")) or 0.0)
    yes_gap = max(0.0, _parse_float(row.get("yes_possible_gap")) or 0.0)
    shock_active = _normalize_text(row.get("speci_shock_active")).lower() in {"1", "true", "yes"}
    shock_confidence = _parse_float(row.get("speci_shock_confidence")) or 0.0
    shock_weight = _parse_float(row.get("speci_shock_weight")) or 0.0
    shock_cooldown_blocked = _normalize_text(row.get("speci_shock_cooldown_blocked")).lower() in {"1", "true", "yes"}
    shock_improvement_hold = _normalize_text(row.get("speci_shock_improvement_hold_active")).lower() in {"1", "true", "yes"}
    score = status_score
    score += 0.35 * max(0.0, min(1.0, settlement_confidence))
    score += 0.18 * min(4.0, primary_margin)
    score += 0.14 * min(4.0, forecast_margin)
    score -= 0.30 * min(3.0, yes_gap)
    if shock_active:
        score += 0.18
    score += 0.14 * max(0.0, min(1.0, shock_confidence))
    score += 0.16 * max(0.0, min(1.0, shock_weight))
    if shock_cooldown_blocked:
        score -= 0.12
    if shock_improvement_hold:
        score -= 0.08
    return round(score, 6)


def _cross_market_family_mispricing_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    family_best: dict[str, dict[str, Any]] = {}
    for row in rows:
        snapshot_status = _normalize_text(row.get("snapshot_status")).lower()
        if snapshot_status and snapshot_status != "ready":
            continue
        family_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")).upper(),
                _normalize_text(row.get("target_date_local")),
                _normalize_text(row.get("temperature_metric")).lower() or "daily_high",
            )
        )
        if not family_key:
            continue
        threshold_kind = _normalize_text(row.get("threshold_kind")).lower() or "unknown"
        bucket_key = "|".join(
            (
                _normalize_text(row.get("target_date_local")),
                _normalize_text(row.get("temperature_metric")).lower() or "daily_high",
                threshold_kind,
            )
        )
        score = _constraint_signal_strength(row)
        existing = family_best.get(family_key)
        if existing is None or float(existing.get("score") or 0.0) < score:
            family_best[family_key] = {
                "family_key": family_key,
                "bucket_key": bucket_key,
                "score": score,
                "market_ticker": _normalize_text(row.get("market_ticker")),
                "constraint_status": _normalize_text(row.get("constraint_status")).lower(),
                "threshold_expression": _normalize_text(row.get("threshold_expression")),
                "settlement_station": _normalize_text(row.get("settlement_station")).upper(),
                "target_date_local": _normalize_text(row.get("target_date_local")),
            }

    buckets: dict[str, list[dict[str, Any]]] = {}
    for payload in family_best.values():
        bucket_key = _normalize_text(payload.get("bucket_key"))
        if not bucket_key:
            continue
        buckets.setdefault(bucket_key, []).append(payload)

    zscore_by_family: dict[str, float] = {}
    score_by_family: dict[str, float] = {}
    bucket_size_by_family: dict[str, int] = {}
    signal_by_family: dict[str, str] = {}
    candidates: list[dict[str, Any]] = []
    checked_buckets = 0
    for bucket_key, members in buckets.items():
        bucket_size = len(members)
        for member in members:
            family_key = _normalize_text(member.get("family_key"))
            score_by_family[family_key] = float(member.get("score") or 0.0)
            bucket_size_by_family[family_key] = bucket_size
        if bucket_size < 3:
            continue
        checked_buckets += 1
        scores = [float(member.get("score") or 0.0) for member in members]
        mean_score = sum(scores) / len(scores) if scores else 0.0
        variance = sum((score - mean_score) ** 2 for score in scores) / len(scores) if scores else 0.0
        std = math.sqrt(max(variance, 0.0))
        for member in members:
            family_key = _normalize_text(member.get("family_key"))
            score = float(member.get("score") or 0.0)
            if std > 1e-9:
                zscore = (score - mean_score) / std
            else:
                zscore = 0.0
            zscore_by_family[family_key] = zscore
            signal = ""
            if zscore >= 1.2:
                signal = "relative_outlier_high"
            elif zscore <= -1.2:
                signal = "relative_outlier_low"
            signal_by_family[family_key] = signal
            if signal:
                candidates.append(
                    {
                        **member,
                        "signal": signal,
                        "bucket_size": bucket_size,
                        "zscore": round(zscore, 6),
                        "bucket_mean_score": round(mean_score, 6),
                        "bucket_std_score": round(std, 6),
                    }
                )

    candidates.sort(
        key=lambda item: (
            -abs(float(item.get("zscore") or 0.0)),
            -float(item.get("score") or 0.0),
            _normalize_text(item.get("market_ticker")),
        )
    )
    rank_by_family: dict[str, int] = {}
    for index, candidate in enumerate(candidates):
        family_key = _normalize_text(candidate.get("family_key"))
        if family_key and family_key not in rank_by_family:
            rank_by_family[family_key] = index + 1

    for row in rows:
        family_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")).upper(),
                _normalize_text(row.get("target_date_local")),
                _normalize_text(row.get("temperature_metric")).lower() or "daily_high",
            )
        )
        score = score_by_family.get(family_key)
        zscore = zscore_by_family.get(family_key)
        bucket_size = bucket_size_by_family.get(family_key)
        signal = signal_by_family.get(family_key, "")
        rank = rank_by_family.get(family_key)
        row["cross_market_family_score"] = round(score, 6) if isinstance(score, float) else ""
        row["cross_market_family_zscore"] = round(zscore, 6) if isinstance(zscore, float) else ""
        row["cross_market_family_candidate_rank"] = int(rank) if isinstance(rank, int) else ""
        row["cross_market_family_bucket_size"] = int(bucket_size) if isinstance(bucket_size, int) else ""
        row["cross_market_family_signal"] = signal

    high_count = sum(1 for candidate in candidates if _normalize_text(candidate.get("signal")) == "relative_outlier_high")
    low_count = sum(1 for candidate in candidates if _normalize_text(candidate.get("signal")) == "relative_outlier_low")
    return {
        "checked_buckets": checked_buckets,
        "checked_families": len(family_best),
        "candidate_count": len(candidates),
        "high_outlier_count": high_count,
        "low_outlier_count": low_count,
        "top_candidates": candidates[:25],
    }


def run_kalshi_temperature_constraint_scan(
    *,
    specs_csv: str | None = None,
    output_dir: str = "outputs",
    timeout_seconds: float = 12.0,
    max_markets: int = 100,
    speci_calibration_json: str | None = None,
) -> dict[str, Any]:
    scanned_at = datetime.now(timezone.utc)
    resolved_specs_csv = _normalize_text(specs_csv) or _find_latest_specs_csv(output_dir)
    if not resolved_specs_csv:
        return {
            "status": "missing_specs_csv",
            "error": "No specs CSV provided and none found in output_dir.",
        }

    specs_path = Path(resolved_specs_csv)
    spec_rows = _read_specs_rows(specs_path)
    if not spec_rows:
        return {
            "status": "no_specs_rows",
            "specs_csv": str(specs_path),
            "error": "Specs CSV missing or empty.",
        }

    result_rows: list[dict[str, Any]] = []
    metar_state = _load_metar_state(output_dir)
    station_interval_stats = (
        metar_state.get("station_observation_interval_stats")
        if isinstance(metar_state, dict) and isinstance(metar_state.get("station_observation_interval_stats"), dict)
        else {}
    )
    speci_calibration = _load_speci_calibration(speci_calibration_json)
    forecast_envelope_cache: dict[str, dict[str, Any]] = {}
    station_ids = sorted(
        {
            _normalize_text(row.get("settlement_station")).upper()
            for row in spec_rows
            if _normalize_text(row.get("settlement_station"))
        }
    )
    try:
        taf_payload = fetch_aviationweather_taf_temperature_envelopes(
            station_ids=station_ids,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        taf_payload = {"status": f"upstream_exception:{exc}", "station_envelopes": {}}
    taf_envelopes_by_station = (
        taf_payload.get("station_envelopes")
        if isinstance(taf_payload.get("station_envelopes"), dict)
        else {}
    )
    processed = 0
    for row in spec_rows:
        if processed >= max(1, int(max_markets)):
            break
        series_ticker = _normalize_text(row.get("series_ticker"))
        station_id = _normalize_text(row.get("settlement_station"))
        timezone_name = _normalize_text(row.get("settlement_timezone")) or infer_timezone_from_station(station_id)
        target_date_local = _normalize_text(row.get("target_date_local"))
        threshold_expression = _normalize_text(row.get("threshold_expression"))
        if not station_id or not target_date_local:
            continue
        if not timezone_name:
            timezone_name = "UTC"
        if not threshold_expression:
            continue
        threshold_kind, threshold_lower_bound, threshold_upper_bound = _parse_threshold_bounds(threshold_expression)

        settlement_unit = infer_settlement_unit(
            _normalize_text(row.get("market_title")),
            _normalize_text(row.get("rules_primary")),
            threshold_expression=threshold_expression,
        )
        temperature_metric = infer_temperature_metric(
            series_ticker=series_ticker,
            market_title=_normalize_text(row.get("market_title")),
            rules_primary=_normalize_text(row.get("rules_primary")),
        )
        settlement_precision = "whole_degree"

        snapshot: dict[str, Any]
        try:
            snapshot = build_intraday_temperature_snapshot(
                station_id=station_id,
                target_date_local=target_date_local,
                timezone_name=timezone_name,
                settlement_unit=settlement_unit,
                settlement_precision=settlement_precision,
                timeout_seconds=timeout_seconds,
                metar_state=metar_state,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            snapshot = {
                "status": "snapshot_unavailable",
                "error": str(exc),
            }

        snapshot_status = _normalize_text(snapshot.get("status"))
        observed_max_raw = snapshot.get("max_temperature_settlement_raw") if isinstance(snapshot, dict) else None
        observed_max_quantized = snapshot.get("max_temperature_settlement_quantized") if isinstance(snapshot, dict) else None
        observed_min_raw = snapshot.get("min_temperature_settlement_raw") if isinstance(snapshot, dict) else None
        observed_min_quantized = snapshot.get("min_temperature_settlement_quantized") if isinstance(snapshot, dict) else None
        if temperature_metric == "daily_low":
            observed_raw = observed_min_raw
            observed_quantized = observed_min_quantized
        else:
            observed_raw = observed_max_raw
            observed_quantized = observed_max_quantized
        observations_for_date = int(snapshot.get("observations_for_date") or 0) if isinstance(snapshot, dict) else 0
        forecast_model = _forecast_envelope_for_target_day(
            station_id=station_id,
            target_date_local=target_date_local,
            timezone_name=timezone_name,
            settlement_unit=settlement_unit,
            timeout_seconds=timeout_seconds,
            cache=forecast_envelope_cache,
            taf_envelopes_by_station=taf_envelopes_by_station,
        )
        forecast_upper = (
            float(forecast_model["forecast_upper_bound_settlement_raw"])
            if isinstance(forecast_model.get("forecast_upper_bound_settlement_raw"), (int, float))
            else None
        )
        forecast_lower = (
            float(forecast_model["forecast_lower_bound_settlement_raw"])
            if isinstance(forecast_model.get("forecast_lower_bound_settlement_raw"), (int, float))
            else None
        )

        latest_station_payload = _latest_station_payload(
            metar_state=metar_state,
            station_id=station_id,
        )
        speci_recent = _speci_recent_flag(
            latest_station_payload=latest_station_payload,
            scanned_at=scanned_at,
        )
        speci_shock_profile = _speci_shock_profile(
            latest_station_payload=latest_station_payload,
            station_interval_stats_payload=station_interval_stats.get(_normalize_text(station_id).upper())
            if isinstance(station_interval_stats, dict)
            else None,
            scanned_at=scanned_at,
            temperature_metric=temperature_metric,
            calibration=speci_calibration,
        )

        snapshot_quality_ok, snapshot_quality_reason = _snapshot_quality_gate(
            snapshot=snapshot if isinstance(snapshot, dict) else {},
            scanned_at=scanned_at,
            target_date_local=target_date_local,
            timezone_name=timezone_name,
        )
        if not snapshot_quality_ok:
            constraint_status = "snapshot_unavailable"
            constraint_reason = snapshot_quality_reason or "Snapshot unavailable."
        else:
            constraint_status, constraint_reason = evaluate_temperature_constraint(
                threshold_expression=threshold_expression,
                observed_value=(float(observed_quantized) if isinstance(observed_quantized, (int, float)) else None),
                temperature_metric=temperature_metric,
                forecast_upper_bound=forecast_upper,
                forecast_lower_bound=forecast_lower,
            )
        alpha_features = _alpha_feature_pack(
            threshold_expression=threshold_expression,
            threshold_kind=threshold_kind,
            threshold_lower=threshold_lower_bound,
            threshold_upper=threshold_upper_bound,
            temperature_metric=temperature_metric,
            observed_value=(float(observed_quantized) if isinstance(observed_quantized, (int, float)) else None),
            forecast_upper_bound=forecast_upper,
            forecast_lower_bound=forecast_lower,
        )

        result_rows.append(
            {
                "scanned_at": scanned_at.isoformat(),
                "source_specs_csv": str(specs_path),
                "series_ticker": series_ticker,
                "event_ticker": _normalize_text(row.get("event_ticker")),
                "market_ticker": _normalize_text(row.get("market_ticker")),
                "market_title": _normalize_text(row.get("market_title")),
                "settlement_station": station_id,
                "settlement_timezone": timezone_name,
                "target_date_local": target_date_local,
                "settlement_unit": settlement_unit,
                "settlement_precision": settlement_precision,
                "temperature_metric": temperature_metric,
                "threshold_expression": threshold_expression,
                "threshold_kind": _normalize_text(alpha_features.get("threshold_kind")),
                "threshold_lower_bound": alpha_features.get("threshold_lower_bound")
                if isinstance(alpha_features.get("threshold_lower_bound"), (int, float))
                else "",
                "threshold_upper_bound": alpha_features.get("threshold_upper_bound")
                if isinstance(alpha_features.get("threshold_upper_bound"), (int, float))
                else "",
                "constraint_status": constraint_status,
                "constraint_reason": constraint_reason,
                "observed_max_settlement_raw": observed_max_raw if observed_max_raw is not None else "",
                "observed_max_settlement_quantized": observed_max_quantized if observed_max_quantized is not None else "",
                "observed_metric_settlement_raw": observed_raw if observed_raw is not None else "",
                "observed_metric_settlement_quantized": observed_quantized if observed_quantized is not None else "",
                "forecast_upper_bound_settlement_raw": forecast_upper if forecast_upper is not None else "",
                "forecast_lower_bound_settlement_raw": forecast_lower if forecast_lower is not None else "",
                "forecast_range_width": alpha_features.get("forecast_range_width")
                if isinstance(alpha_features.get("forecast_range_width"), (int, float))
                else "",
                "possible_final_lower_bound": alpha_features.get("possible_final_lower_bound")
                if isinstance(alpha_features.get("possible_final_lower_bound"), (int, float))
                else "",
                "possible_final_upper_bound": alpha_features.get("possible_final_upper_bound")
                if isinstance(alpha_features.get("possible_final_upper_bound"), (int, float))
                else "",
                "yes_interval_lower_bound": alpha_features.get("yes_interval_lower_bound")
                if isinstance(alpha_features.get("yes_interval_lower_bound"), (int, float))
                else "",
                "yes_interval_upper_bound": alpha_features.get("yes_interval_upper_bound")
                if isinstance(alpha_features.get("yes_interval_upper_bound"), (int, float))
                else "",
                "yes_possible_overlap": alpha_features.get("yes_possible_overlap"),
                "yes_possible_gap": alpha_features.get("yes_possible_gap")
                if isinstance(alpha_features.get("yes_possible_gap"), (int, float))
                else "",
                "observed_distance_to_lower_bound": alpha_features.get("observed_distance_to_lower_bound")
                if isinstance(alpha_features.get("observed_distance_to_lower_bound"), (int, float))
                else "",
                "observed_distance_to_upper_bound": alpha_features.get("observed_distance_to_upper_bound")
                if isinstance(alpha_features.get("observed_distance_to_upper_bound"), (int, float))
                else "",
                "primary_signal_margin": alpha_features.get("primary_signal_margin")
                if isinstance(alpha_features.get("primary_signal_margin"), (int, float))
                else "",
                "forecast_feasibility_margin": alpha_features.get("forecast_feasibility_margin")
                if isinstance(alpha_features.get("forecast_feasibility_margin"), (int, float))
                else "",
                "forecast_model_status": _normalize_text(forecast_model.get("status")),
                "taf_status": _normalize_text(forecast_model.get("taf_status")),
                "taf_volatility_score": (
                    forecast_model.get("taf_volatility_score")
                    if isinstance(forecast_model.get("taf_volatility_score"), (int, float))
                    else ""
                ),
                "speci_recent": "1" if speci_recent else "0",
                "speci_shock_active": "1" if bool(speci_shock_profile.get("active")) else "0",
                "speci_shock_confidence": (
                    speci_shock_profile.get("confidence")
                    if isinstance(speci_shock_profile.get("confidence"), (int, float))
                    else ""
                ),
                "speci_shock_weight": (
                    speci_shock_profile.get("weight")
                    if isinstance(speci_shock_profile.get("weight"), (int, float))
                    else ""
                ),
                "speci_shock_mode": _normalize_text(speci_shock_profile.get("mode")),
                "speci_shock_trigger_count": (
                    int(speci_shock_profile.get("trigger_count") or 0)
                    if isinstance(speci_shock_profile.get("trigger_count"), (int, float))
                    else 0
                ),
                "speci_shock_trigger_families": _normalize_text(speci_shock_profile.get("trigger_families")),
                "speci_shock_persistence_ok": "1" if bool(speci_shock_profile.get("persistence_ok")) else "0",
                "speci_shock_cooldown_blocked": (
                    "1" if bool(speci_shock_profile.get("cooldown_blocked")) else "0"
                ),
                "speci_shock_improvement_hold_active": (
                    "1" if bool(speci_shock_profile.get("improvement_hold_active")) else "0"
                ),
                "speci_shock_delta_temp_c": (
                    speci_shock_profile.get("delta_temp_c")
                    if isinstance(speci_shock_profile.get("delta_temp_c"), (int, float))
                    else ""
                ),
                "speci_shock_delta_minutes": (
                    speci_shock_profile.get("delta_minutes")
                    if isinstance(speci_shock_profile.get("delta_minutes"), (int, float))
                    else ""
                ),
                "speci_shock_decay_tau_minutes": (
                    speci_shock_profile.get("decay_tau_minutes")
                    if isinstance(speci_shock_profile.get("decay_tau_minutes"), (int, float))
                    else ""
                ),
                "range_family_consistency_conflict": "",
                "range_family_consistency_conflict_scope": "",
                "range_family_consistency_conflict_reason": "",
                "observations_for_date": observations_for_date,
                "snapshot_status": snapshot_status,
                "settlement_confidence_score": _normalize_text(row.get("settlement_confidence_score")),
            }
        )
        processed += 1

    monotonic_consistency = _monotonic_consistency_summary(result_rows)
    exact_chain_consistency = _exact_strike_chain_summary(result_rows)
    range_family_consistency = _range_family_consistency_summary(result_rows)
    interval_conflicted_group_keys = {
        _normalize_text(key)
        for key in range_family_consistency.get("interval_conflicted_group_keys", [])
        if _normalize_text(key)
    }
    cross_metric_conflicted_station_day_keys = {
        _normalize_text(key)
        for key in range_family_consistency.get("cross_metric_conflicted_station_day_keys", [])
        if _normalize_text(key)
    }
    range_family_conflicted_rows = 0
    for row in result_rows:
        metric = _normalize_text(row.get("temperature_metric")).lower() or "daily_high"
        group_key = "|".join(
            (
                _normalize_text(row.get("series_ticker")),
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
                metric,
            )
        )
        station_day_key = "|".join(
            (
                _normalize_text(row.get("settlement_station")),
                _normalize_text(row.get("target_date_local")),
            )
        )
        conflict_scope = ""
        conflict_reason = ""
        if group_key in interval_conflicted_group_keys:
            conflict_scope = "family_interval"
            conflict_reason = "locked_intervals_have_empty_intersection"
        elif station_day_key in cross_metric_conflicted_station_day_keys:
            conflict_scope = "station_day_cross_metric"
            conflict_reason = "daily_low_observed_exceeds_daily_high_observed"
        if conflict_scope:
            row["range_family_consistency_conflict"] = "1"
            row["range_family_consistency_conflict_scope"] = conflict_scope
            row["range_family_consistency_conflict_reason"] = conflict_reason
            range_family_conflicted_rows += 1
        else:
            row["range_family_consistency_conflict"] = "0"
            row["range_family_consistency_conflict_scope"] = ""
            row["range_family_consistency_conflict_reason"] = ""
    cross_market_mispricing = _cross_market_family_mispricing_summary(result_rows)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = scanned_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"kalshi_temperature_constraint_scan_{stamp}.csv"
    _write_constraints_csv(csv_path, result_rows)

    yes_possible_gaps = [
        float(row.get("yes_possible_gap"))
        for row in result_rows
        if isinstance(row.get("yes_possible_gap"), (int, float))
    ]
    speci_shock_confidences = [
        float(row.get("speci_shock_confidence"))
        for row in result_rows
        if isinstance(row.get("speci_shock_confidence"), (int, float))
    ]
    speci_shock_weights = [
        float(row.get("speci_shock_weight"))
        for row in result_rows
        if isinstance(row.get("speci_shock_weight"), (int, float))
    ]

    summary = {
        "status": "ready" if result_rows else "no_markets",
        "scanned_at": scanned_at.isoformat(),
        "specs_csv": str(specs_path),
        "markets_processed": processed,
        "markets_emitted": len(result_rows),
        "yes_impossible_count": sum(1 for row in result_rows if row.get("constraint_status") == "yes_impossible"),
        "yes_likely_locked_count": sum(
            1 for row in result_rows if row.get("constraint_status") == "yes_likely_locked"
        ),
        "yes_possible_overlap_count": sum(1 for row in result_rows if int(row.get("yes_possible_overlap") or 0) == 1),
        "yes_possible_gap_avg": round(sum(yes_possible_gaps) / len(yes_possible_gaps), 6) if yes_possible_gaps else None,
        "snapshot_unavailable_count": sum(
            1 for row in result_rows if row.get("constraint_status") == "snapshot_unavailable"
        ),
        "daily_high_markets": sum(1 for row in result_rows if _normalize_text(row.get("temperature_metric")) == "daily_high"),
        "daily_low_markets": sum(1 for row in result_rows if _normalize_text(row.get("temperature_metric")) == "daily_low"),
        "forecast_modeled_count": sum(1 for row in result_rows if _normalize_text(row.get("forecast_model_status")) == "ready"),
        "taf_ready_count": sum(1 for row in result_rows if _normalize_text(row.get("taf_status")) == "ready"),
        "speci_recent_count": sum(1 for row in result_rows if _normalize_text(row.get("speci_recent")) in {"1", "true", "yes"}),
        "speci_shock_active_count": sum(
            1 for row in result_rows if _normalize_text(row.get("speci_shock_active")) in {"1", "true", "yes"}
        ),
        "speci_shock_cooldown_blocked_count": sum(
            1
            for row in result_rows
            if _normalize_text(row.get("speci_shock_cooldown_blocked")) in {"1", "true", "yes"}
        ),
        "speci_shock_improvement_hold_count": sum(
            1
            for row in result_rows
            if _normalize_text(row.get("speci_shock_improvement_hold_active")) in {"1", "true", "yes"}
        ),
        "speci_shock_confidence_avg": (
            round(sum(speci_shock_confidences) / len(speci_shock_confidences), 6)
            if speci_shock_confidences
            else None
        ),
        "speci_shock_weight_avg": (
            round(sum(speci_shock_weights) / len(speci_shock_weights), 6)
            if speci_shock_weights
            else None
        ),
        "speci_calibration_file_used": _normalize_text(speci_calibration.get("path")),
        "speci_calibration_loaded": bool(speci_calibration.get("loaded")),
        "speci_calibration_error": _normalize_text(speci_calibration.get("error")),
        "speci_calibration_version": _normalize_text(speci_calibration.get("version")),
        "taf_payload_status": _normalize_text(taf_payload.get("status")) or "unknown",
        "top_candidates": [
            row
            for row in result_rows
            if row.get("constraint_status") in {"yes_impossible", "yes_likely_locked"}
        ][:20],
        "monotonic_violation_count": monotonic_consistency.get("violations_count", 0),
        "exact_chain_violation_count": exact_chain_consistency.get("violations_count", 0),
        "range_family_conflict_count": range_family_consistency.get("violations_count", 0),
        "range_family_conflicted_rows": range_family_conflicted_rows,
        "cross_market_family_mispricing_candidate_count": int(cross_market_mispricing.get("candidate_count") or 0),
        "cross_market_family_checked_buckets": int(cross_market_mispricing.get("checked_buckets") or 0),
        "consistency_checks": {
            **monotonic_consistency,
            "neighboring_strike_monotonicity": monotonic_consistency,
            "exact_strike_impossibility_chains": exact_chain_consistency,
            "range_family_consistency": range_family_consistency,
            "cross_market_family_mispricing": cross_market_mispricing,
        },
        "output_csv": str(csv_path),
    }
    summary_path = out_dir / f"kalshi_temperature_constraint_scan_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
