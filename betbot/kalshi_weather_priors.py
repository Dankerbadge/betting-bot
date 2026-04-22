from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
import inspect
import json
import math
from pathlib import Path
import re
from statistics import mean, pstdev
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from betbot.kalshi_nonsports_priors import PRIOR_FIELDNAMES, load_prior_rows
from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.runtime_version import (
    build_runtime_version_block,
    detect_weather_model_tags,
    file_mtime_utc,
    weather_priors_version,
)
from betbot.kalshi_weather_ingest import (
    fetch_ncei_cdo_station_daily_history,
    fetch_ncei_normals_station_day,
    fetch_noaa_mrms_qpe_latest_metadata,
    fetch_noaa_nbm_latest_snapshot,
    fetch_noaa_global_land_ocean_anomaly_series,
    fetch_nws_active_alerts_for_point,
    fetch_nws_station_hourly_forecast,
    fetch_nws_station_recent_observations,
)
from betbot.kalshi_weather_settlement import (
    build_weather_settlement_spec,
    infer_settlement_station,
    infer_settlement_timezone,
    infer_timezone_from_station,
)


WeatherStationForecastFetcher = Callable[..., dict[str, Any]]
WeatherStationObservationFetcher = Callable[..., dict[str, Any]]
WeatherPointAlertsFetcher = Callable[..., dict[str, Any]]
NoaaAnomalySeriesFetcher = Callable[..., dict[str, Any]]
StationHistoryFetcher = Callable[..., dict[str, Any]]
StationNormalsFetcher = Callable[..., dict[str, Any]]
MrmsSnapshotFetcher = Callable[..., dict[str, Any]]
NbmSnapshotFetcher = Callable[..., dict[str, Any]]


_DEFAULT_ALLOWED_FAMILIES = (
    "daily_rain",
    "daily_temperature",
    "monthly_climate_anomaly",
)
_DEFAULT_HISTORICAL_LOOKBACK_YEARS = 15
_DEFAULT_STATION_HISTORY_CACHE_MAX_AGE_HOURS = 24.0
_MIN_VALID_TEMPERATURE_F = -120.0
_MAX_VALID_TEMPERATURE_F = 140.0
_MAX_RECENT_OBSERVATION_FUTURE_SKEW = timedelta(minutes=0)
_MIN_VALID_OBSERVATION_TEMPERATURE_C = -100.0
_MAX_VALID_OBSERVATION_TEMPERATURE_C = 70.0
_MIN_VALID_OBSERVATION_DEWPOINT_C = -100.0
_MAX_VALID_OBSERVATION_DEWPOINT_C = 70.0
_MIN_VALID_OBSERVATION_RELATIVE_HUMIDITY_PCT = 0.0
_MAX_VALID_OBSERVATION_RELATIVE_HUMIDITY_PCT = 100.0
_MIN_VALID_OBSERVATION_PRECIP_MM = 0.0
_MAX_VALID_OBSERVATION_PRECIP_MM = 500.0
_MIN_VALID_OBSERVATION_WIND_SPEED_MPS = 0.0
_MAX_VALID_OBSERVATION_WIND_SPEED_MPS = 120.0
_MIN_SAMPLE_YEARS_BY_DAILY_FAMILY = {
    "daily_rain": 8,
    "daily_temperature": 10,
    "daily_snow": 10,
}

_CITY_STATION_FALLBACKS = {
    "new york": "KJFK",
    "nyc": "KJFK",
    "boston": "KBOS",
    "washington": "KDCA",
    "dc": "KDCA",
    "chicago": "KORD",
    "dallas": "KDFW",
    "denver": "KDEN",
    "phoenix": "KPHX",
    "los angeles": "KLAX",
    "la": "KLAX",
    "san francisco": "KSFO",
    "sf": "KSFO",
    "seattle": "KSEA",
    "miami": "KMIA",
    "philadelphia": "KPHL",
    "philly": "KPHL",
    "atlanta": "KATL",
    "houston": "KIAH",
    "minneapolis": "KMSP",
    "detroit": "KDTW",
    "baltimore": "KBWI",
    "las vegas": "KLAS",
    "salt lake city": "KSLC",
    "portland": "KPDX",
    "austin": "KAUS",
    "san antonio": "KSAT",
    "new orleans": "KMSY",
    "nola": "KMSY",
    "oklahoma city": "KOKC",
    "okc": "KOKC",
}

WEATHER_PRIOR_EXTRA_FIELDS = [
    "contract_family",
    "resolution_source_type",
    "model_name",
    "model_probability_raw",
    "execution_probability_guarded",
    "market_midpoint_probability",
    "settlement_source_primary",
    "settlement_source_fallback",
    "settlement_station",
    "settlement_timezone",
    "local_day_boundary",
    "observation_window_local_start",
    "observation_window_local_end",
    "observation_window_local_source",
    "weather_station_history_status",
    "weather_station_history_cache_hit",
    "weather_station_history_cache_fallback_used",
    "weather_station_history_cache_fresh",
    "weather_station_history_cache_age_seconds",
    "weather_station_history_sample_metric",
    "weather_station_history_sample_years",
    "weather_station_history_sample_years_total",
    "weather_station_history_sample_years_precip",
    "weather_station_history_sample_years_tmax",
    "weather_station_history_sample_years_tmin",
    "weather_station_history_sample_years_mean",
    "weather_station_history_min_sample_years_required",
    "weather_station_history_live_ready",
    "weather_station_history_live_ready_reason",
    "threshold_expression",
    "rule_text_hash_sha256",
]

WEATHER_PRIOR_OUTPUT_FIELDNAMES = [
    "market_ticker",
    "event_title",
    "market_title",
    "close_time",
    "hours_to_close",
    "fair_yes_probability",
    "fair_yes_probability_low",
    "fair_yes_probability_high",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
    "evidence_count",
    "evidence_quality",
    "source_type",
    "last_evidence_at",
    "contract_family",
    "resolution_source_type",
    "model_name",
    "model_probability_raw",
    "execution_probability_guarded",
    "market_midpoint_probability",
    "settlement_source_primary",
    "settlement_source_fallback",
    "settlement_station",
    "settlement_timezone",
    "local_day_boundary",
    "observation_window_local_start",
    "observation_window_local_end",
    "observation_window_local_source",
    "weather_station_history_status",
    "weather_station_history_cache_hit",
    "weather_station_history_cache_fallback_used",
    "weather_station_history_cache_fresh",
    "weather_station_history_cache_age_seconds",
    "weather_station_history_sample_metric",
    "weather_station_history_sample_years",
    "weather_station_history_sample_years_total",
    "weather_station_history_sample_years_precip",
    "weather_station_history_sample_years_tmax",
    "weather_station_history_sample_years_tmin",
    "weather_station_history_sample_years_mean",
    "weather_station_history_min_sample_years_required",
    "weather_station_history_live_ready",
    "weather_station_history_live_ready_reason",
    "threshold_expression",
    "rule_text_hash_sha256",
]


def _classify_fetch_error(error: Any) -> str:
    text = str(error or "").strip().lower()
    if not text:
        return "unknown_error"
    if "nodename nor servname" in text or "name or service not known" in text or "temporary failure in name resolution" in text:
        return "dns_resolution_error"
    if "timed out" in text or "timeout" in text:
        return "timeout"
    if "status 429" in text or "rate limit" in text:
        return "rate_limited"
    if "urlopen error" in text or "network" in text:
        return "network_error"
    if "status " in text:
        return "http_error"
    return "upstream_error"


def _parse_float(value: Any) -> float | None:
    numeric: float | None = None
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            return None
    if numeric is None or not math.isfinite(numeric):
        return None
    return numeric


def _parse_recent_observation_float(
    value: Any,
    *,
    min_value: float,
    max_value: float,
) -> float | None:
    if isinstance(value, bool):
        return None
    numeric = _parse_float(value)
    if numeric is None:
        return None
    if numeric < min_value or numeric > max_value:
        return None
    return numeric


def _parse_recent_observation_timestamp(value: Any, *, now: datetime) -> datetime | None:
    observed_at = _parse_datetime(value)
    if observed_at is None:
        return None
    current_time = now.astimezone(timezone.utc)
    if observed_at > current_time + _MAX_RECENT_OBSERVATION_FUTURE_SKEW:
        return None
    return observed_at


def _validate_recent_nws_observation(
    observation: dict[str, Any],
    *,
    now: datetime,
) -> tuple[datetime | None, dict[str, float] | None, str | None]:
    timestamp = _parse_recent_observation_timestamp(observation.get("timestamp"), now=now)
    if timestamp is None:
        return None, None, "invalid_timestamp"

    metric_specs = (
        ("temperature_c", _MIN_VALID_OBSERVATION_TEMPERATURE_C, _MAX_VALID_OBSERVATION_TEMPERATURE_C),
        ("dewpoint_c", _MIN_VALID_OBSERVATION_DEWPOINT_C, _MAX_VALID_OBSERVATION_DEWPOINT_C),
        (
            "relative_humidity_pct",
            _MIN_VALID_OBSERVATION_RELATIVE_HUMIDITY_PCT,
            _MAX_VALID_OBSERVATION_RELATIVE_HUMIDITY_PCT,
        ),
        ("precipitation_last_hour_mm", _MIN_VALID_OBSERVATION_PRECIP_MM, _MAX_VALID_OBSERVATION_PRECIP_MM),
        ("wind_speed_mps", _MIN_VALID_OBSERVATION_WIND_SPEED_MPS, _MAX_VALID_OBSERVATION_WIND_SPEED_MPS),
    )

    parsed_metrics: dict[str, float] = {}
    for field_name, min_value, max_value in metric_specs:
        raw_value = observation.get(field_name)
        if raw_value is None:
            continue
        parsed_value = _parse_recent_observation_float(raw_value, min_value=min_value, max_value=max_value)
        if parsed_value is None:
            return None, None, f"invalid_{field_name}"
        parsed_metrics[field_name] = parsed_value

    return timestamp, parsed_metrics, None


def _is_valid_temperature_f(value: Any) -> bool:
    numeric = _parse_float(value)
    if numeric is None:
        return False
    return _MIN_VALID_TEMPERATURE_F <= numeric <= _MAX_VALID_TEMPERATURE_F


def _normalize_string_set(values: tuple[str, ...] | list[str] | set[str] | None) -> set[str]:
    if not values:
        return set()
    normalized: set[str] = set()
    for value in values:
        text = str(value or "").strip().lower()
        if text:
            normalized.add(text)
    return normalized


def _parse_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clamp_probability(value: float) -> float:
    return round(min(0.999, max(0.001, float(value))), 6)


def _parse_probability_percent(value: Any) -> float | None:
    numeric = _parse_float(value)
    if numeric is None:
        return None
    if numeric < 0.0 or numeric > 100.0:
        return None
    return numeric / 100.0


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def _history_sample_years_snapshot(history_payload: dict[str, Any] | None) -> dict[str, int | None]:
    if not isinstance(history_payload, dict):
        return {
            "sample_years_total": None,
            "sample_years_precip": None,
            "sample_years_tmax": None,
            "sample_years_tmin": None,
            "sample_years_mean": None,
        }

    def _from_value(value: Any) -> int | None:
        numeric = _parse_float(value)
        if not isinstance(numeric, float) or numeric < 0:
            return None
        return int(numeric)

    sample_years_total = _from_value(history_payload.get("sample_years"))
    sample_years_precip = _from_value(history_payload.get("sample_years_precip"))
    sample_years_tmax = _from_value(history_payload.get("sample_years_tmax"))
    sample_years_tmin = _from_value(history_payload.get("sample_years_tmin"))
    sample_years_mean = _from_value(history_payload.get("sample_years_mean"))

    if sample_years_total is None and isinstance(history_payload.get("daily_samples"), list):
        sample_years_total = len([item for item in history_payload.get("daily_samples", []) if isinstance(item, dict)])
    if sample_years_precip is None and isinstance(history_payload.get("prcp_values_in"), list):
        sample_years_precip = len([item for item in history_payload.get("prcp_values_in", []) if isinstance(item, (int, float))])
    if sample_years_tmax is None and isinstance(history_payload.get("tmax_values_f"), list):
        sample_years_tmax = len([item for item in history_payload.get("tmax_values_f", []) if isinstance(item, (int, float))])
    if sample_years_tmin is None and isinstance(history_payload.get("tmin_values_f"), list):
        sample_years_tmin = len([item for item in history_payload.get("tmin_values_f", []) if isinstance(item, (int, float))])
    if sample_years_mean is None and isinstance(history_payload.get("daily_mean_values_f"), list):
        sample_years_mean = len(
            [item for item in history_payload.get("daily_mean_values_f", []) if isinstance(item, (int, float))]
        )

    return {
        "sample_years_total": sample_years_total,
        "sample_years_precip": sample_years_precip,
        "sample_years_tmax": sample_years_tmax,
        "sample_years_tmin": sample_years_tmin,
        "sample_years_mean": sample_years_mean,
    }


def _history_sample_years_for_metric(
    history_payload: dict[str, Any] | None,
    *,
    sample_metric: str | None = None,
) -> int | None:
    metric = str(sample_metric or "").strip().lower()
    snapshot = _history_sample_years_snapshot(history_payload)
    sample_years_total = snapshot.get("sample_years_total")
    if metric in {"precip", "rain"}:
        sample_years_precip = snapshot.get("sample_years_precip")
        return sample_years_total if sample_years_precip is None else sample_years_precip
    if metric in {"tmax", "high"}:
        sample_years_tmax = snapshot.get("sample_years_tmax")
        return sample_years_total if sample_years_tmax is None else sample_years_tmax
    if metric in {"tmin", "low"}:
        sample_years_tmin = snapshot.get("sample_years_tmin")
        return sample_years_total if sample_years_tmin is None else sample_years_tmin
    if metric in {"mean", "daily_mean"}:
        sample_years_mean = snapshot.get("sample_years_mean")
        return sample_years_total if sample_years_mean is None else sample_years_mean
    return sample_years_total


def _history_live_health(
    history_payload: dict[str, Any] | None,
    *,
    contract_family: str | None = None,
    sample_metric: str | None = None,
) -> dict[str, Any]:
    family = str(contract_family or "").strip().lower()
    metric = str(sample_metric or "").strip().lower()
    min_sample_years_required = int(_MIN_SAMPLE_YEARS_BY_DAILY_FAMILY.get(family, 0) or 0)
    sample_snapshot = _history_sample_years_snapshot(history_payload)
    sample_years = _history_sample_years_for_metric(history_payload, sample_metric=metric)
    if not isinstance(history_payload, dict):
        return {
            "status": "missing",
            "cache_hit": False,
            "cache_fallback_used": False,
            "cache_fresh": False,
            "cache_age_seconds": None,
            "sample_metric": metric,
            "sample_years": sample_years,
            "sample_years_total": sample_snapshot.get("sample_years_total"),
            "sample_years_precip": sample_snapshot.get("sample_years_precip"),
            "sample_years_tmax": sample_snapshot.get("sample_years_tmax"),
            "sample_years_tmin": sample_snapshot.get("sample_years_tmin"),
            "sample_years_mean": sample_snapshot.get("sample_years_mean"),
            "min_sample_years_required": min_sample_years_required,
            "live_ready": False,
            "live_ready_reason": "missing_history_payload",
        }
    status = str(history_payload.get("status") or "").strip().lower() or "unknown"
    cache_hit = _as_bool(history_payload.get("cache_hit"))
    cache_fallback_used = _as_bool(history_payload.get("cache_fallback_used"))
    cache_fresh = _as_bool(history_payload.get("cache_fresh"))
    cache_age_seconds_raw = _parse_float(history_payload.get("cache_age_seconds"))
    cache_age_seconds = round(cache_age_seconds_raw, 3) if isinstance(cache_age_seconds_raw, float) else None
    live_ready = status in {"ready", "ready_partial"}
    reason = "ready"
    if not live_ready:
        reason = f"status_{status}"
    elif cache_fallback_used:
        live_ready = False
        reason = "cache_fallback_used"
    elif cache_hit and not cache_fresh:
        live_ready = False
        reason = "stale_cache_entry"
    elif min_sample_years_required > 0 and (sample_years is None or sample_years < min_sample_years_required):
        live_ready = False
        reason = "insufficient_sample_years"
    return {
        "status": status,
        "cache_hit": cache_hit,
        "cache_fallback_used": cache_fallback_used,
        "cache_fresh": cache_fresh,
        "cache_age_seconds": cache_age_seconds,
        "sample_metric": metric,
        "sample_years": sample_years,
        "sample_years_total": sample_snapshot.get("sample_years_total"),
        "sample_years_precip": sample_snapshot.get("sample_years_precip"),
        "sample_years_tmax": sample_snapshot.get("sample_years_tmax"),
        "sample_years_tmin": sample_snapshot.get("sample_years_tmin"),
        "sample_years_mean": sample_snapshot.get("sample_years_mean"),
        "min_sample_years_required": min_sample_years_required,
        "live_ready": live_ready,
        "live_ready_reason": reason,
    }


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    latest: dict[str, dict[str, str]] = {}
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest[ticker] = rows_sorted[-1]
    return latest


def _midpoint_probability_from_market(row: dict[str, Any]) -> float | None:
    yes_bid = _parse_float(row.get("yes_bid_dollars"))
    yes_ask = _parse_float(row.get("yes_ask_dollars"))
    if yes_bid is None and yes_ask is None:
        return None
    if yes_bid is None:
        return _clamp_probability(yes_ask)
    if yes_ask is None:
        return _clamp_probability(yes_bid)
    return _clamp_probability((yes_bid + yes_ask) / 2.0)


def _execution_guard_probability(
    probability: float,
    row: dict[str, Any],
    *,
    max_deviation: float,
    midpoint_blend_weight: float = 0.06,
) -> float:
    midpoint = _midpoint_probability_from_market(row)
    if midpoint is None:
        return _clamp_probability(probability)
    low = midpoint - max_deviation
    high = midpoint + max_deviation
    bounded = min(max(probability, low), high)
    blend_weight = max(0.0, min(0.30, float(midpoint_blend_weight)))
    blended = (1.0 - blend_weight) * bounded + blend_weight * midpoint
    return _clamp_probability(blended)


def _normal_cdf(value: float, mean_value: float, sigma: float) -> float:
    safe_sigma = max(1e-9, float(sigma))
    z_score = (float(value) - float(mean_value)) / (safe_sigma * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z_score))


def _parse_threshold_expression(expression: str) -> tuple[str, float | None, float | None]:
    text = str(expression or "").strip().lower()
    if not text:
        return ("", None, None)
    for prefix in ("between", "above", "below", "at_least", "at_most", "equal"):
        if text.startswith(f"{prefix}:"):
            parts = text.split(":")
            if prefix == "between" and len(parts) == 3:
                low = _parse_float(parts[1])
                high = _parse_float(parts[2])
                if low is not None and high is not None:
                    return ("between", min(low, high), max(low, high))
            if prefix in {"above", "below", "at_least", "at_most", "equal"}:
                return (prefix, _parse_float(text.split(":", 1)[1]), None)
    between_match = re.search(r"between\s+([-\d.]+)\s*(?:-|to|and)\s*([-\d.]+)", text)
    if between_match:
        low = _parse_float(between_match.group(1))
        high = _parse_float(between_match.group(2))
        if low is not None and high is not None:
            return ("between", min(low, high), max(low, high))
    at_least_match = re.search(r"(?:at least|greater than or equal to|>=)\s*([-\d.]+)", text)
    if at_least_match:
        return ("at_least", _parse_float(at_least_match.group(1)), None)
    at_most_match = re.search(r"(?:at most|less than or equal to|<=|no more than)\s*([-\d.]+)", text)
    if at_most_match:
        return ("at_most", _parse_float(at_most_match.group(1)), None)
    above_match = re.search(r"(?:above|greater than|over)\s+([-\d.]+)", text)
    if above_match:
        return ("above", _parse_float(above_match.group(1)), None)
    below_match = re.search(r"(?:below|less than|under)\s+([-\d.]+)", text)
    if below_match:
        return ("below", _parse_float(below_match.group(1)), None)
    equal_match = re.search(r"(?:equal to|equals|exactly)\s+([-\d.]+)", text)
    if equal_match:
        return ("equal", _parse_float(equal_match.group(1)), None)
    return ("", None, None)


def _threshold_probability(kind: str, first: float | None, second: float | None, mean_value: float, sigma: float) -> float | None:
    if kind == "between" and first is not None and second is not None:
        return _normal_cdf(second, mean_value, sigma) - _normal_cdf(first, mean_value, sigma)
    if kind in {"above", "at_least"} and first is not None:
        return 1.0 - _normal_cdf(first, mean_value, sigma)
    if kind in {"below", "at_most"} and first is not None:
        return _normal_cdf(first, mean_value, sigma)
    if kind == "equal" and first is not None:
        half_width = 0.5
        return _normal_cdf(first + half_width, mean_value, sigma) - _normal_cdf(first - half_width, mean_value, sigma)
    return None


def _effective_source_type(row: dict[str, str]) -> str:
    source_type = str(row.get("source_type") or "").strip().lower()
    if source_type:
        return source_type
    if any(str(row.get(field) or "").strip() for field in ("thesis", "source_note", "updated_at")):
        return "manual"
    return ""


def _merge_headers(existing_fieldnames: list[str] | None) -> list[str]:
    merged: list[str] = []
    for field in PRIOR_FIELDNAMES + WEATHER_PRIOR_EXTRA_FIELDS:
        if field not in merged:
            merged.append(field)
    for field in existing_fieldnames or []:
        if field not in merged:
            merged.append(field)
    return merged


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _upsert_priors_csv(
    *,
    priors_path: Path,
    generated_rows: list[dict[str, Any]],
    protect_manual: bool,
) -> dict[str, Any]:
    existing_rows: list[dict[str, str]] = []
    existing_fieldnames: list[str] | None = None
    if priors_path.exists():
        with priors_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_fieldnames = list(reader.fieldnames or [])
            existing_rows = [dict(row) for row in reader]

    index_by_ticker: dict[str, int] = {}
    for index, row in enumerate(existing_rows):
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker and ticker not in index_by_ticker:
            index_by_ticker[ticker] = index

    inserted = 0
    updated = 0
    skipped_manual = 0

    for generated in generated_rows:
        ticker = str(generated.get("market_ticker") or "").strip()
        if not ticker:
            continue
        existing_index = index_by_ticker.get(ticker)
        row_payload = {key: str(value) if not isinstance(value, str) else value for key, value in generated.items()}
        if existing_index is None:
            existing_rows.append(row_payload)
            index_by_ticker[ticker] = len(existing_rows) - 1
            inserted += 1
            continue
        existing = existing_rows[existing_index]
        source_type = _effective_source_type(existing)
        if protect_manual and source_type in {"manual", "manual_override"}:
            skipped_manual += 1
            continue
        for key, value in row_payload.items():
            existing[key] = value
        updated += 1

    merged_fieldnames = _merge_headers(existing_fieldnames)
    _write_csv(priors_path, existing_rows, merged_fieldnames)
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_manual": skipped_manual,
        "rows_total": len(existing_rows),
    }


def _infer_station_id(row: dict[str, str], settlement_station: str) -> str:
    station = str(settlement_station or "").strip().upper()
    if station:
        return station
    inferred_station = infer_settlement_station(
        str(row.get("rules_primary") or ""),
        str(row.get("market_title") or ""),
        str(row.get("event_title") or ""),
    )
    if inferred_station:
        return inferred_station
    merged = " ".join(
        (
            str(row.get("market_ticker") or ""),
            str(row.get("event_title") or ""),
            str(row.get("market_title") or ""),
            str(row.get("rules_primary") or ""),
        )
    ).lower()
    for token, station_id in sorted(_CITY_STATION_FALLBACKS.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"\b{re.escape(token)}\b", merged):
            return station_id
    return ""


def _fetch_station_forecast(
    *,
    station_forecast_fetcher: WeatherStationForecastFetcher,
    station_id: str,
    timeout_seconds: float,
    include_gridpoint_data: bool,
) -> dict[str, Any]:
    fetch_signature = inspect.signature(station_forecast_fetcher)
    fetch_kwargs: dict[str, Any] = {
        "station_id": station_id,
        "timeout_seconds": timeout_seconds,
    }
    if "include_gridpoint_data" in fetch_signature.parameters:
        fetch_kwargs["include_gridpoint_data"] = bool(include_gridpoint_data)
    return station_forecast_fetcher(**fetch_kwargs)


def _parse_valid_time_start(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    if not text:
        return None
    return _parse_datetime(text)


def _observation_window_local_minutes(
    *,
    observation_window_local_start: str | None,
    observation_window_local_end: str | None,
) -> tuple[int | None, int | None, bool]:
    def _minutes_from_clock(value: str | None) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.match(r"^(\d{1,2}):(\d{2})$", text)
        if not match:
            return None
        hours = int(match.group(1))
        minutes = int(match.group(2))
        if hours < 0 or hours > 23 or minutes < 0 or minutes > 59:
            return None
        return (hours * 60) + minutes

    start_minutes = _minutes_from_clock(observation_window_local_start)
    end_minutes = _minutes_from_clock(observation_window_local_end)
    overnight_window = (
        start_minutes is not None
        and end_minutes is not None
        and start_minutes > end_minutes
    )
    return start_minutes, end_minutes, overnight_window


def _gridpoint_numeric_values_for_target_window(
    *,
    values: list[dict[str, Any]],
    settlement_timezone_name: str,
    target_settlement_date: date | None,
    observation_window_local_start: str | None,
    observation_window_local_end: str | None,
) -> list[float]:
    if not values:
        return []

    try:
        settlement_zone = ZoneInfo(settlement_timezone_name)
    except ZoneInfoNotFoundError:
        settlement_zone = timezone.utc

    start_minutes, end_minutes, overnight_window = _observation_window_local_minutes(
        observation_window_local_start=observation_window_local_start,
        observation_window_local_end=observation_window_local_end,
    )
    has_explicit_window = start_minutes is not None and end_minutes is not None
    next_settlement_date = (
        target_settlement_date + timedelta(days=1)
        if isinstance(target_settlement_date, date)
        else None
    )

    selected: list[float] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        raw_value = _parse_float(item.get("value"))
        if raw_value is None:
            continue
        valid_time_start = _parse_valid_time_start(item.get("validTime"))
        if valid_time_start is None:
            continue
        local_start = valid_time_start.astimezone(settlement_zone)
        local_date = local_start.date()
        minute_of_day = (local_start.hour * 60) + local_start.minute

        if isinstance(target_settlement_date, date):
            if not has_explicit_window:
                if local_date != target_settlement_date:
                    continue
            elif not overnight_window:
                if local_date != target_settlement_date:
                    continue
                if minute_of_day < int(start_minutes) or minute_of_day > int(end_minutes):
                    continue
            else:
                in_target_day_segment = (
                    local_date == target_settlement_date
                    and minute_of_day >= int(start_minutes)
                )
                in_next_day_segment = (
                    isinstance(next_settlement_date, date)
                    and local_date == next_settlement_date
                    and minute_of_day <= int(end_minutes)
                )
                if not in_target_day_segment and not in_next_day_segment:
                    continue
        selected.append(float(raw_value))
    return selected


def _period_window(
    periods: list[dict[str, Any]],
    *,
    now: datetime,
    hours_to_close: float | None,
    settlement_timezone_name: str,
    target_settlement_date: date | None,
    observation_window_local_start: str | None = None,
    observation_window_local_end: str | None = None,
) -> list[dict[str, Any]]:
    def _minutes_from_clock(value: str | None) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.match(r"^(\d{1,2}):(\d{2})$", text)
        if not match:
            return None
        hours = int(match.group(1))
        minutes = int(match.group(2))
        if hours < 0 or hours > 23 or minutes < 0 or minutes > 59:
            return None
        return (hours * 60) + minutes

    if isinstance(target_settlement_date, date):
        try:
            settlement_zone = ZoneInfo(settlement_timezone_name)
        except ZoneInfoNotFoundError:
            settlement_zone = timezone.utc
        start_minutes = _minutes_from_clock(observation_window_local_start)
        end_minutes = _minutes_from_clock(observation_window_local_end)
        has_explicit_window = start_minutes is not None and end_minutes is not None
        overnight_window = has_explicit_window and bool(start_minutes > end_minutes)
        next_settlement_date = target_settlement_date + timedelta(days=1)
        settlement_day_periods: list[dict[str, Any]] = []
        for period in periods:
            start_time = _parse_datetime(str(period.get("startTime") or ""))
            if start_time is None:
                continue
            local_start = start_time.astimezone(settlement_zone)
            local_date = local_start.date()
            minute_of_day = (local_start.hour * 60) + local_start.minute

            if not has_explicit_window:
                if local_date != target_settlement_date:
                    continue
            elif not overnight_window:
                if local_date != target_settlement_date:
                    continue
                if minute_of_day < start_minutes or minute_of_day > end_minutes:
                    continue
            else:
                in_target_day_segment = (
                    local_date == target_settlement_date and minute_of_day >= start_minutes
                )
                in_next_day_segment = (
                    local_date == next_settlement_date and minute_of_day <= end_minutes
                )
                if not in_target_day_segment and not in_next_day_segment:
                    continue
            settlement_day_periods.append(period)
        if settlement_day_periods:
            return settlement_day_periods

    horizon_hours = min(72.0, max(6.0, hours_to_close if isinstance(hours_to_close, (int, float)) else 24.0))
    cutoff = now + timedelta(hours=float(horizon_hours))
    selected: list[dict[str, Any]] = []
    for period in periods:
        start_time = _parse_datetime(str(period.get("startTime") or ""))
        if start_time is None:
            selected.append(period)
            continue
        if start_time <= cutoff:
            selected.append(period)
    if selected:
        return selected
    return periods[:24]


def _settlement_timezone_name(settlement: dict[str, Any], row: dict[str, Any]) -> str:
    timezone_name = str(settlement.get("settlement_timezone") or "").strip()
    if timezone_name:
        return timezone_name
    station_id = _infer_station_id(
        row,
        str(settlement.get("settlement_station") or ""),
    )
    timezone_from_station = infer_timezone_from_station(station_id)
    if timezone_from_station:
        return timezone_from_station
    timezone_from_titles = infer_settlement_timezone(
        str(row.get("market_ticker") or ""),
        str(row.get("market_title") or ""),
        str(row.get("event_title") or ""),
    )
    if timezone_from_titles:
        return timezone_from_titles
    return "America/New_York"


def _target_settlement_local_datetime(
    *,
    row: dict[str, Any],
    settlement: dict[str, Any],
    now: datetime,
) -> datetime:
    timezone_name = _settlement_timezone_name(settlement, row)
    close_dt = _parse_datetime(str(row.get("close_time") or ""))
    if close_dt is None:
        local_dt = now
    else:
        local_dt = close_dt
    try:
        zone = ZoneInfo(timezone_name)
        local_dt = local_dt.astimezone(zone)
    except ZoneInfoNotFoundError:
        local_dt = local_dt.astimezone(timezone.utc)

    if str(settlement.get("local_day_boundary") or "").strip().lower() == "local_day":
        if local_dt.hour <= 6:
            local_dt = local_dt - timedelta(days=1)
    return local_dt


def _target_month_day(
    *,
    row: dict[str, Any],
    settlement: dict[str, Any],
    now: datetime,
) -> tuple[int, int]:
    local_dt = _target_settlement_local_datetime(row=row, settlement=settlement, now=now)
    return (local_dt.month, local_dt.day)


def _temperature_expected_label(row: dict[str, Any]) -> str:
    descriptor = " ".join(
        (
            str(row.get("event_title") or ""),
            str(row.get("market_title") or ""),
            str(row.get("rules_primary") or ""),
        )
    ).lower()
    if any(token in descriptor for token in ("daily low", "low temperature", "lowest temperature")):
        return "daily low"
    if any(token in descriptor for token in ("daily high", "high temperature", "highest temperature")):
        return "daily high"
    return "hourly mean"


def _temperature_sample_metric(expected_label: str) -> str:
    normalized = str(expected_label or "").strip().lower()
    if normalized == "daily high":
        return "tmax"
    if normalized == "daily low":
        return "tmin"
    return "mean"


def _climatology_temperature_series(
    history_payload: dict[str, Any],
    *,
    expected_label: str,
) -> list[float]:
    if expected_label == "daily low":
        raw = history_payload.get("tmin_values_f")
    elif expected_label == "daily high":
        raw = history_payload.get("tmax_values_f")
    else:
        raw = history_payload.get("daily_mean_values_f")
    if not isinstance(raw, list):
        return []
    cleaned: list[float] = []
    for value in raw:
        numeric = _parse_float(value)
        if numeric is None or not _is_valid_temperature_f(numeric):
            continue
        cleaned.append(float(numeric))
    return cleaned


def _blend_temperature_model(
    *,
    forecast_expected_temperature: float,
    forecast_sigma: float,
    climatology_values: list[float],
) -> tuple[float, float]:
    if len(climatology_values) < 5:
        return (forecast_expected_temperature, forecast_sigma)
    climatology_mean = mean(climatology_values)
    climatology_sigma = max(2.0, pstdev(climatology_values) if len(climatology_values) > 1 else 3.5)
    forecast_weight = 0.76
    climatology_weight = 1.0 - forecast_weight
    blended_expected = (
        forecast_weight * float(forecast_expected_temperature)
        + climatology_weight * float(climatology_mean)
    )
    blended_sigma = max(
        1.5,
        math.sqrt(
            (forecast_weight * float(forecast_sigma)) ** 2
            + (climatology_weight * float(climatology_sigma)) ** 2
        ),
    )
    return (blended_expected, blended_sigma)


def _lag1_autocorrelation(values: list[float]) -> float:
    if len(values) < 3:
        return 0.0
    x_values = values[:-1]
    y_values = values[1:]
    x_mean = mean(x_values)
    y_mean = mean(y_values)
    cov = 0.0
    x_var = 0.0
    y_var = 0.0
    for x_value, y_value in zip(x_values, y_values, strict=False):
        x_delta = x_value - x_mean
        y_delta = y_value - y_mean
        cov += x_delta * y_delta
        x_var += x_delta * x_delta
        y_var += y_delta * y_delta
    if x_var <= 1e-12 or y_var <= 1e-12:
        return 0.0
    correlation = cov / math.sqrt(x_var * y_var)
    return max(-0.95, min(0.95, correlation))


def _rain_probability_regime_adjusted(pop_values: list[float]) -> dict[str, float]:
    no_rain_probability = 1.0
    for value in pop_values:
        no_rain_probability *= (1.0 - value)
    independent_probability = max(0.0, min(1.0, 1.0 - no_rain_probability))
    if len(pop_values) < 3:
        return {
            "independent_probability": independent_probability,
            "lag1_autocorrelation": 0.0,
            "effective_independence_ratio": 1.0,
            "persistence_adjusted_probability": independent_probability,
            "concentration_ratio": 1.0,
            "burst_blend_weight": 0.0,
            "max_hourly_probability": max(pop_values) if pop_values else 0.0,
            "regime_adjusted_probability": independent_probability,
        }

    lag1_autocorrelation = _lag1_autocorrelation(pop_values)
    effective_independence_ratio = 1.0
    if lag1_autocorrelation > 0.0:
        effective_independence_ratio = max(
            0.35,
            min(1.0, (1.0 - lag1_autocorrelation) / (1.0 + lag1_autocorrelation)),
        )
    no_rain_persistence_adjusted = no_rain_probability**effective_independence_ratio
    persistence_adjusted_probability = max(0.0, min(1.0, 1.0 - no_rain_persistence_adjusted))

    total_pop = sum(pop_values)
    top_hours = max(1, min(3, len(pop_values)))
    top_pop_sum = sum(sorted(pop_values, reverse=True)[:top_hours])
    concentration_ratio = max(0.0, min(1.0, (top_pop_sum / total_pop) if total_pop > 1e-9 else 1.0))
    max_hourly_probability = max(pop_values)
    burst_blend_weight = 0.12 + (0.40 * concentration_ratio) + (0.08 * max(0.0, lag1_autocorrelation))
    burst_blend_weight = max(0.10, min(0.60, burst_blend_weight))

    regime_adjusted_probability = (
        ((1.0 - burst_blend_weight) * persistence_adjusted_probability)
        + (burst_blend_weight * max_hourly_probability)
    )
    regime_adjusted_probability = max(0.0, min(1.0, regime_adjusted_probability))
    return {
        "independent_probability": independent_probability,
        "lag1_autocorrelation": lag1_autocorrelation,
        "effective_independence_ratio": effective_independence_ratio,
        "persistence_adjusted_probability": persistence_adjusted_probability,
        "concentration_ratio": concentration_ratio,
        "burst_blend_weight": burst_blend_weight,
        "max_hourly_probability": max_hourly_probability,
        "regime_adjusted_probability": regime_adjusted_probability,
    }


def _adaptive_rain_climatology_blend_weight(
    *,
    pop_values: list[float],
    rain_regime: dict[str, float],
    climatology_count: int,
    forecast_age_hours: float | None,
    history_live_ready: bool,
) -> float:
    if climatology_count <= 0 or not pop_values:
        return 0.0

    def _unit_clamp(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    # Higher climatology blend when we have deeper station history, stale forecasts,
    # and noisier/dispersed PoP structure; lower blend when short-term rain regime
    # structure is concentrated and persistent.
    depth_score = _unit_clamp((float(climatology_count) - 6.0) / 12.0)
    pop_dispersion = pstdev(pop_values) if len(pop_values) > 1 else 0.0
    dispersion_score = _unit_clamp(pop_dispersion / 0.20)
    horizon_score = _unit_clamp((float(len(pop_values)) - 8.0) / 18.0)
    freshness_score = _unit_clamp((float(forecast_age_hours or 0.0)) / 36.0)

    concentration_ratio = _unit_clamp(float(rain_regime.get("concentration_ratio") or 0.0))
    lag1 = _unit_clamp(max(0.0, float(rain_regime.get("lag1_autocorrelation") or 0.0)))
    burst_weight = _unit_clamp(float(rain_regime.get("burst_blend_weight") or 0.0))
    regime_strength = _unit_clamp(
        (0.40 * concentration_ratio) + (0.25 * lag1) + (0.35 * burst_weight),
    )

    blend_weight = (
        0.08
        + (0.18 * depth_score)
        + (0.14 * dispersion_score)
        + (0.10 * horizon_score)
        + (0.12 * freshness_score)
        - (0.24 * regime_strength)
    )
    if not history_live_ready:
        blend_weight *= 0.75
    return round(max(0.06, min(0.45, blend_weight)), 6)


def _build_daily_rain_prior(
    *,
    row: dict[str, str],
    settlement: dict[str, Any],
    history_payload: dict[str, Any] | None,
    normals_payload: dict[str, Any] | None,
    mrms_payload: dict[str, Any] | None,
    nbm_payload: dict[str, Any] | None,
    now: datetime,
    timeout_seconds: float,
    include_nws_gridpoint_data: bool,
    include_nws_observations: bool,
    include_nws_alerts: bool,
    station_forecast_fetcher: WeatherStationForecastFetcher,
    station_observations_fetcher: WeatherStationObservationFetcher,
    point_alerts_fetcher: WeatherPointAlertsFetcher,
) -> tuple[dict[str, Any] | None, str | None]:
    station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
    if not station_id:
        return None, "missing_settlement_station"

    forecast = _fetch_station_forecast(
        station_forecast_fetcher=station_forecast_fetcher,
        station_id=station_id,
        timeout_seconds=timeout_seconds,
        include_gridpoint_data=include_nws_gridpoint_data,
    )
    if str(forecast.get("status") or "") != "ready":
        return None, f"station_forecast_{forecast.get('status') or 'unavailable'}"

    periods = forecast.get("periods")
    if not isinstance(periods, list) or not periods:
        return None, "station_forecast_missing_periods"

    settlement_timezone_name = _settlement_timezone_name(settlement, row)
    target_local_dt = _target_settlement_local_datetime(row=row, settlement=settlement, now=now)
    scoped_periods = _period_window(
        periods,
        now=now,
        hours_to_close=_parse_float(row.get("hours_to_close")),
        settlement_timezone_name=settlement_timezone_name,
        target_settlement_date=target_local_dt.date(),
        observation_window_local_start=str(settlement.get("observation_window_local_start") or "").strip(),
        observation_window_local_end=str(settlement.get("observation_window_local_end") or "").strip(),
    )
    pop_values: list[float] = []
    for period in scoped_periods:
        probability_payload = period.get("probabilityOfPrecipitation")
        probability = None
        if isinstance(probability_payload, dict):
            probability = _parse_probability_percent(probability_payload.get("value"))
        if probability is None:
            continue
        pop_values.append(probability)
    if not pop_values:
        return None, "station_forecast_missing_precip_probability"

    window_start = str(settlement.get("observation_window_local_start") or "").strip()
    window_end = str(settlement.get("observation_window_local_end") or "").strip()

    gridpoint_status = str(forecast.get("gridpoint_status") or "").strip().lower()
    gridpoint_layers = forecast.get("gridpoint_layers") if isinstance(forecast.get("gridpoint_layers"), dict) else {}
    gridpoint_pop_values: list[float] = []
    gridpoint_qpf_in_values: list[float] = []
    if gridpoint_status == "ready" and isinstance(gridpoint_layers, dict):
        raw_pop_values = _gridpoint_numeric_values_for_target_window(
            values=[
                item
                for item in (gridpoint_layers.get("probabilityOfPrecipitation") or [])
                if isinstance(item, dict)
            ],
            settlement_timezone_name=settlement_timezone_name,
            target_settlement_date=target_local_dt.date(),
            observation_window_local_start=window_start,
            observation_window_local_end=window_end,
        )
        gridpoint_pop_values = [
            probability
            for value in raw_pop_values
            for probability in [_parse_probability_percent(value)]
            if probability is not None
        ]
        raw_qpf_values_mm = _gridpoint_numeric_values_for_target_window(
            values=[
                item
                for item in (gridpoint_layers.get("quantitativePrecipitation") or [])
                if isinstance(item, dict)
            ],
            settlement_timezone_name=settlement_timezone_name,
            target_settlement_date=target_local_dt.date(),
            observation_window_local_start=window_start,
            observation_window_local_end=window_end,
        )
        gridpoint_qpf_in_values = [
            max(0.0, value / 25.4)
            for value in raw_qpf_values_mm
        ]

    combined_pop_values = list(pop_values)
    if gridpoint_pop_values:
        combined_pop_values.extend(gridpoint_pop_values)
    pop_values = combined_pop_values

    rain_regime = _rain_probability_regime_adjusted(pop_values)
    model_probability_raw_independent = float(rain_regime.get("independent_probability") or 0.0)
    model_probability_raw_unclamped = float(rain_regime.get("regime_adjusted_probability") or 0.0)
    history_health = _history_live_health(
        history_payload,
        contract_family="daily_rain",
        sample_metric="precip",
    )
    climatology_count = int(_history_sample_years_for_metric(history_payload, sample_metric="precip") or 0)
    climatology_frequency = None
    if isinstance(history_payload, dict):
        rain_frequency = history_payload.get("rain_day_frequency")
        if isinstance(rain_frequency, (int, float)):
            climatology_frequency = max(0.0, min(1.0, float(rain_frequency)))
    normals_status = str((normals_payload or {}).get("status") or "").strip().lower()
    normals_rain_frequency = _parse_float((normals_payload or {}).get("rain_day_frequency"))
    if normals_status == "ready" and isinstance(normals_rain_frequency, float):
        normals_rain_frequency = max(0.0, min(1.0, float(normals_rain_frequency)))
        if isinstance(climatology_frequency, float):
            climatology_frequency = (0.82 * climatology_frequency) + (0.18 * normals_rain_frequency)
        else:
            climatology_frequency = normals_rain_frequency
            climatology_count = max(climatology_count, 30)
    rain_climatology_blend_weight = 0.0
    updated_at = str(forecast.get("forecast_updated_at") or "").strip()
    age_penalty = 0.0
    age_hours = 0.0
    updated_dt = _parse_datetime(updated_at)
    if isinstance(updated_dt, datetime):
        age_hours = max(0.0, (now - updated_dt).total_seconds() / 3600.0)
        age_penalty = min(0.2, age_hours / 48.0)

    gridpoint_qpf_signal = 0.0
    if gridpoint_qpf_in_values:
        qpf_mean = max(0.0, mean(gridpoint_qpf_in_values))
        qpf_peak = max(0.0, max(gridpoint_qpf_in_values))
        gridpoint_qpf_signal = max(
            0.0,
            min(0.12, (qpf_mean * 0.35) + min(0.08, qpf_peak * 0.15)),
        )
        model_probability_raw_unclamped = min(1.0, model_probability_raw_unclamped + gridpoint_qpf_signal)

    observations_status = "disabled"
    observations_recent_count = 0
    observations_recent_rain_count = 0
    observations_signal = 0.0
    observations_parse_warning_count = 0
    if include_nws_observations:
        try:
            observations_payload = station_observations_fetcher(
                station_id=station_id,
                timeout_seconds=timeout_seconds,
                limit=24,
            )
        except Exception:
            observations_payload = {"status": "upstream_error"}
        observations_status = str(observations_payload.get("status") or "").strip().lower() or "upstream_error"
        if observations_status == "ready":
            observations = observations_payload.get("observations")
            if isinstance(observations, list):
                for observation in observations:
                    if not isinstance(observation, dict):
                        continue
                    observed_dt, parsed_metrics, parse_warning = _validate_recent_nws_observation(
                        observation,
                        now=now,
                    )
                    if parse_warning is not None:
                        observations_parse_warning_count += 1
                        continue
                    if observed_dt is None or parsed_metrics is None:
                        continue
                    age_hours_obs = (now.astimezone(timezone.utc) - observed_dt).total_seconds() / 3600.0
                    if age_hours_obs > 12.0:
                        continue
                    observations_recent_count += 1
                    precip_mm = parsed_metrics.get("precipitation_last_hour_mm")
                    text_description = str(observation.get("text_description") or "").strip().lower()
                    if (isinstance(precip_mm, float) and precip_mm >= 0.2) or ("rain" in text_description):
                        observations_recent_rain_count += 1
                if observations_recent_count > 0:
                    observations_signal = min(
                        0.12,
                        float(observations_recent_rain_count) / float(observations_recent_count) * 0.15,
                    )
                    model_probability_raw_unclamped = min(1.0, model_probability_raw_unclamped + observations_signal)

    alerts_status = "disabled"
    alerts_count = 0
    alerts_signal = 0.0
    if include_nws_alerts:
        latitude = _parse_float(forecast.get("latitude"))
        longitude = _parse_float(forecast.get("longitude"))
        if isinstance(latitude, float) and isinstance(longitude, float):
            try:
                alerts_payload = point_alerts_fetcher(
                    latitude=latitude,
                    longitude=longitude,
                    timeout_seconds=timeout_seconds,
                )
            except Exception:
                alerts_payload = {"status": "upstream_error"}
            alerts_status = str(alerts_payload.get("status") or "").strip().lower() or "upstream_error"
            if alerts_status == "ready":
                alerts_count = max(0, int(_parse_float(alerts_payload.get("alerts_count")) or 0))
                severe_alerts = 0
                for alert in alerts_payload.get("alerts") if isinstance(alerts_payload.get("alerts"), list) else []:
                    if not isinstance(alert, dict):
                        continue
                    event_text = str(alert.get("event") or "").lower()
                    severity_text = str(alert.get("severity") or "").lower()
                    if any(token in event_text for token in ("flood", "storm", "rain", "thunder")):
                        severe_alerts += 1
                    elif severity_text in {"severe", "extreme"}:
                        severe_alerts += 1
                alerts_signal = min(0.05, severe_alerts * 0.015)
                model_probability_raw_unclamped = min(1.0, model_probability_raw_unclamped + alerts_signal)

    mrms_status = str((mrms_payload or {}).get("status") or "").strip().lower()
    mrms_age_seconds = _parse_float((mrms_payload or {}).get("age_seconds"))
    mrms_freshness_boost = 0.0
    if mrms_status == "ready" and isinstance(mrms_age_seconds, float):
        if mrms_age_seconds <= 90.0 * 60.0:
            mrms_freshness_boost = 0.02
        elif mrms_age_seconds >= 6.0 * 3600.0:
            mrms_freshness_boost = -0.03

    nbm_status = str((nbm_payload or {}).get("status") or "").strip().lower()
    nbm_cycle_age_seconds = _parse_float((nbm_payload or {}).get("cycle_age_seconds"))
    nbm_freshness_boost = 0.0
    if nbm_status == "ready" and isinstance(nbm_cycle_age_seconds, float):
        if nbm_cycle_age_seconds <= 3.0 * 3600.0:
            nbm_freshness_boost = 0.02
        elif nbm_cycle_age_seconds >= 12.0 * 3600.0:
            nbm_freshness_boost = -0.02

    if climatology_frequency is not None:
        rain_climatology_blend_weight = _adaptive_rain_climatology_blend_weight(
            pop_values=pop_values,
            rain_regime=rain_regime,
            climatology_count=climatology_count,
            forecast_age_hours=age_hours,
            history_live_ready=bool(history_health.get("live_ready")),
        )
        model_probability_raw_unclamped = (
            (1.0 - rain_climatology_blend_weight) * model_probability_raw_unclamped
            + (rain_climatology_blend_weight * climatology_frequency)
        )
    model_probability = _clamp_probability(model_probability_raw_unclamped)
    execution_probability = _execution_guard_probability(
        model_probability_raw_unclamped,
        row,
        max_deviation=0.50,
        midpoint_blend_weight=0.06,
    )
    market_midpoint = _midpoint_probability_from_market(row)

    confidence = round(
        min(
            0.92,
            max(
                0.25,
                0.40
                + min(0.40, len(pop_values) * 0.02)
                + min(0.12, climatology_count * 0.006)
                + min(0.05, gridpoint_qpf_signal * 0.8)
                + min(0.05, observations_signal * 0.9)
                + alerts_signal
                + mrms_freshness_boost
                + nbm_freshness_boost
                - age_penalty,
            ),
        ),
        6,
    )
    interval_half_width = max(0.04, min(0.30, 0.28 - 0.22 * confidence))
    low = _clamp_probability(execution_probability - interval_half_width)
    high = _clamp_probability(execution_probability + interval_half_width)
    thesis_suffix = ""
    if climatology_frequency is not None:
        thesis_suffix = (
            f" Adaptive climatology blend weight {rain_climatology_blend_weight:.1%} "
            f"with {climatology_count}y station rain frequency {climatology_frequency:.1%}."
        )
    if observations_recent_count > 0:
        thesis_suffix += (
            " Recent station observations were incorporated for short-horizon rain persistence."
        )
    source_parts = [
        f"nws_station_hourly_forecast:{station_id}",
        f"periods_used={len(pop_values)}",
        f"hourly_pop_points={len(scoped_periods)}",
        f"gridpoint_status={gridpoint_status or 'missing'}",
        f"gridpoint_pop_points={len(gridpoint_pop_values)}",
        f"gridpoint_qpf_points={len(gridpoint_qpf_in_values)}",
        f"gridpoint_qpf_signal={gridpoint_qpf_signal:.4f}",
        f"forecast_updated_at={updated_at or 'unknown'}",
        f"rain_forecast_age_hours={age_hours:.2f}",
        f"raw_pop_independent={model_probability_raw_independent:.4f}",
        f"raw_pop_persistence_adjusted={float(rain_regime.get('persistence_adjusted_probability') or 0.0):.4f}",
        f"raw_pop_regime_adjusted={float(rain_regime.get('regime_adjusted_probability') or 0.0):.4f}",
        f"rain_lag1_autocorrelation={float(rain_regime.get('lag1_autocorrelation') or 0.0):.3f}",
        f"rain_effective_independence_ratio={float(rain_regime.get('effective_independence_ratio') or 1.0):.3f}",
        f"rain_hourly_concentration_ratio={float(rain_regime.get('concentration_ratio') or 1.0):.3f}",
        f"rain_burst_blend_weight={float(rain_regime.get('burst_blend_weight') or 0.0):.3f}",
        f"rain_climatology_blend_weight={rain_climatology_blend_weight:.4f}",
        f"nws_observations_status={observations_status}",
        f"nws_observations_recent_count={observations_recent_count}",
        f"nws_observations_parse_warning_count={observations_parse_warning_count}",
        f"nws_observations_recent_rain_count={observations_recent_rain_count}",
        f"nws_observations_signal={observations_signal:.4f}",
        f"nws_alerts_status={alerts_status}",
        f"nws_alerts_count={alerts_count}",
        f"nws_alerts_signal={alerts_signal:.4f}",
        f"noaa_mrms_status={mrms_status or 'missing'}",
        f"noaa_mrms_age_seconds={mrms_age_seconds if isinstance(mrms_age_seconds, float) else ''}",
        f"noaa_nbm_status={nbm_status or 'missing'}",
        f"noaa_nbm_cycle_age_seconds={nbm_cycle_age_seconds if isinstance(nbm_cycle_age_seconds, float) else ''}",
    ]
    if window_start and window_end:
        source_parts.append(f"settlement_window_local={window_start}-{window_end}")
    if normals_status:
        source_parts.append(f"ncei_normals_status={normals_status}")
    if normals_status == "ready":
        source_parts.append(
            f"ncei_normals_rain_freq={normals_rain_frequency:.4f}"
            if isinstance(normals_rain_frequency, float)
            else "ncei_normals_rain_freq="
        )
    if isinstance(history_payload, dict):
        source_parts.append(f"ncei_cdo_status={history_health.get('status')}")
        source_parts.append(f"historical_sample_metric={history_health.get('sample_metric')}")
        source_parts.append(f"historical_sample_years={history_health.get('sample_years')}")
        source_parts.append(f"historical_min_sample_years_required={history_health.get('min_sample_years_required')}")
        source_parts.append(f"historical_years={climatology_count}")
        if climatology_frequency is not None:
            source_parts.append(f"historical_rain_freq={climatology_frequency:.4f}")
        source_parts.append(f"station_history_live_ready={history_health.get('live_ready')}")
        source_parts.append(f"station_history_live_ready_reason={history_health.get('live_ready_reason')}")
    source_parts.append(f"execution_guarded_probability={execution_probability:.4f}")

    return (
        {
            "market_ticker": str(row.get("market_ticker") or "").strip(),
            "event_title": str(row.get("event_title") or "").strip(),
            "market_title": str(row.get("market_title") or "").strip(),
            "close_time": str(row.get("close_time") or "").strip(),
            "hours_to_close": str(row.get("hours_to_close") or "").strip(),
            "fair_yes_probability": execution_probability,
            "fair_yes_probability_low": min(low, execution_probability),
            "fair_yes_probability_high": max(high, execution_probability),
            "confidence": confidence,
            "thesis": (
                f"NWS hourly precipitation probabilities for station {station_id} imply a "
                f"{execution_probability:.1%} execution-guarded chance of measurable rain for "
                f"{target_local_dt.strftime('%Y-%m-%d')} settlement."
                f"{thesis_suffix}"
            ),
            "source_note": "; ".join(source_parts),
            "updated_at": now.isoformat(),
            "evidence_count": len(pop_values) + climatology_count + observations_recent_count + alerts_count,
            "evidence_quality": round(
                min(
                    1.0,
                    0.55
                    + min(0.25, len(pop_values) * 0.01)
                    + min(0.20, climatology_count * 0.008)
                    + min(0.05, observations_recent_count * 0.01)
                    + max(-0.03, min(0.03, mrms_freshness_boost + nbm_freshness_boost)),
                ),
                6,
            ),
            "source_type": "auto_weather",
            "last_evidence_at": updated_at,
            "contract_family": "daily_rain",
            "resolution_source_type": "weather_forecast",
            "model_name": "weather_rain_pop_regime_historical_v4",
            "model_probability_raw": round(float(model_probability_raw_unclamped), 6),
            "execution_probability_guarded": execution_probability,
            "market_midpoint_probability": market_midpoint if market_midpoint is not None else "",
        },
        None,
    )


def _build_daily_temperature_prior(
    *,
    row: dict[str, str],
    settlement: dict[str, Any],
    history_payload: dict[str, Any] | None,
    normals_payload: dict[str, Any] | None,
    mrms_payload: dict[str, Any] | None,
    nbm_payload: dict[str, Any] | None,
    now: datetime,
    timeout_seconds: float,
    include_nws_gridpoint_data: bool,
    include_nws_observations: bool,
    include_nws_alerts: bool,
    station_forecast_fetcher: WeatherStationForecastFetcher,
    station_observations_fetcher: WeatherStationObservationFetcher,
    point_alerts_fetcher: WeatherPointAlertsFetcher,
) -> tuple[dict[str, Any] | None, str | None]:
    station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
    if not station_id:
        return None, "missing_settlement_station"

    threshold_expression = str(settlement.get("threshold_expression") or "").strip()
    threshold_kind, threshold_a, threshold_b = _parse_threshold_expression(threshold_expression)
    if not threshold_kind:
        return None, "missing_threshold_expression"

    forecast = _fetch_station_forecast(
        station_forecast_fetcher=station_forecast_fetcher,
        station_id=station_id,
        timeout_seconds=timeout_seconds,
        include_gridpoint_data=include_nws_gridpoint_data,
    )
    if str(forecast.get("status") or "") != "ready":
        return None, f"station_forecast_{forecast.get('status') or 'unavailable'}"

    periods = forecast.get("periods")
    if not isinstance(periods, list) or not periods:
        return None, "station_forecast_missing_periods"

    settlement_timezone_name = _settlement_timezone_name(settlement, row)
    target_local_dt = _target_settlement_local_datetime(row=row, settlement=settlement, now=now)
    scoped_periods = _period_window(
        periods,
        now=now,
        hours_to_close=_parse_float(row.get("hours_to_close")),
        settlement_timezone_name=settlement_timezone_name,
        target_settlement_date=target_local_dt.date(),
        observation_window_local_start=str(settlement.get("observation_window_local_start") or "").strip(),
        observation_window_local_end=str(settlement.get("observation_window_local_end") or "").strip(),
    )
    window_start = str(settlement.get("observation_window_local_start") or "").strip()
    window_end = str(settlement.get("observation_window_local_end") or "").strip()
    temperatures: list[float] = []
    for period in scoped_periods:
        temperature = _parse_float(period.get("temperature"))
        if temperature is not None:
            temperatures.append(temperature)
    if not temperatures:
        return None, "station_forecast_missing_temperatures"

    gridpoint_status = str(forecast.get("gridpoint_status") or "").strip().lower()
    gridpoint_layers = forecast.get("gridpoint_layers") if isinstance(forecast.get("gridpoint_layers"), dict) else {}
    gridpoint_tmax_values: list[float] = []
    gridpoint_tmin_values: list[float] = []
    if gridpoint_status == "ready" and isinstance(gridpoint_layers, dict):
        gridpoint_tmax_values = _gridpoint_numeric_values_for_target_window(
            values=[
                item
                for item in (gridpoint_layers.get("maxTemperature") or [])
                if isinstance(item, dict)
            ],
            settlement_timezone_name=settlement_timezone_name,
            target_settlement_date=target_local_dt.date(),
            observation_window_local_start=window_start,
            observation_window_local_end=window_end,
        )
        gridpoint_tmin_values = _gridpoint_numeric_values_for_target_window(
            values=[
                item
                for item in (gridpoint_layers.get("minTemperature") or [])
                if isinstance(item, dict)
            ],
            settlement_timezone_name=settlement_timezone_name,
            target_settlement_date=target_local_dt.date(),
            observation_window_local_start=window_start,
            observation_window_local_end=window_end,
        )

    expected_label = _temperature_expected_label(row)
    if expected_label == "daily low":
        expected_temperature = min(temperatures)
    elif expected_label == "daily high":
        expected_temperature = max(temperatures)
    else:
        expected_temperature = mean(temperatures)

    if expected_label == "daily high" and gridpoint_tmax_values:
        expected_temperature = (0.72 * expected_temperature) + (0.28 * max(gridpoint_tmax_values))
    elif expected_label == "daily low" and gridpoint_tmin_values:
        expected_temperature = (0.72 * expected_temperature) + (0.28 * min(gridpoint_tmin_values))
    elif gridpoint_tmax_values and gridpoint_tmin_values:
        gridpoint_mid = (max(gridpoint_tmax_values) + min(gridpoint_tmin_values)) / 2.0
        expected_temperature = (0.80 * expected_temperature) + (0.20 * gridpoint_mid)

    sigma_forecast = max(2.0, pstdev(temperatures) if len(temperatures) > 1 else 3.5)
    climatology_values = (
        _climatology_temperature_series(history_payload, expected_label=expected_label)
        if isinstance(history_payload, dict)
        else []
    )
    expected_temperature, sigma = _blend_temperature_model(
        forecast_expected_temperature=expected_temperature,
        forecast_sigma=sigma_forecast,
        climatology_values=climatology_values,
    )

    normals_status = str((normals_payload or {}).get("status") or "").strip().lower()
    normal_expected = None
    normal_sigma = None
    if normals_status == "ready":
        if expected_label == "daily high":
            normal_expected = _parse_float((normals_payload or {}).get("tmax_normal_f"))
            normal_sigma = _parse_float((normals_payload or {}).get("tmax_stddev_f"))
        elif expected_label == "daily low":
            normal_expected = _parse_float((normals_payload or {}).get("tmin_normal_f"))
            normal_sigma = _parse_float((normals_payload or {}).get("tmin_stddev_f"))
        else:
            normal_tmax = _parse_float((normals_payload or {}).get("tmax_normal_f"))
            normal_tmin = _parse_float((normals_payload or {}).get("tmin_normal_f"))
            if isinstance(normal_tmax, float) and isinstance(normal_tmin, float):
                normal_expected = (normal_tmax + normal_tmin) / 2.0
            normal_sigma = _parse_float((normals_payload or {}).get("tmax_stddev_f"))
        if isinstance(normal_expected, float):
            expected_temperature = (0.86 * expected_temperature) + (0.14 * normal_expected)
        if isinstance(normal_sigma, float):
            sigma = max(1.5, math.sqrt((0.86 * sigma) ** 2 + (0.14 * max(1.5, normal_sigma)) ** 2))

    observations_status = "disabled"
    observations_recent_count = 0
    observations_temp_correction = 0.0
    observations_parse_warning_count = 0
    if include_nws_observations:
        try:
            observations_payload = station_observations_fetcher(
                station_id=station_id,
                timeout_seconds=timeout_seconds,
                limit=24,
            )
        except Exception:
            observations_payload = {"status": "upstream_error"}
        observations_status = str(observations_payload.get("status") or "").strip().lower() or "upstream_error"
        if observations_status == "ready":
            observations = observations_payload.get("observations")
            if isinstance(observations, list):
                recent_temps_f: list[float] = []
                for observation in observations:
                    if not isinstance(observation, dict):
                        continue
                    observed_dt, parsed_metrics, parse_warning = _validate_recent_nws_observation(
                        observation,
                        now=now,
                    )
                    if parse_warning is not None:
                        observations_parse_warning_count += 1
                        continue
                    if observed_dt is None or parsed_metrics is None:
                        continue
                    age_hours_obs = (now.astimezone(timezone.utc) - observed_dt).total_seconds() / 3600.0
                    if age_hours_obs > 6.0:
                        continue
                    temperature_c = parsed_metrics.get("temperature_c")
                    if temperature_c is None:
                        continue
                    recent_temps_f.append((temperature_c * 9.0 / 5.0) + 32.0)
                observations_recent_count = len(recent_temps_f)
                if recent_temps_f and temperatures:
                    latest_obs_f = recent_temps_f[0]
                    nearest_forecast_f = temperatures[0]
                    raw_delta = latest_obs_f - nearest_forecast_f
                    observations_temp_correction = max(-4.0, min(4.0, raw_delta * 0.30))
                    expected_temperature = expected_temperature + observations_temp_correction

    alerts_status = "disabled"
    alerts_count = 0
    alerts_sigma_add = 0.0
    if include_nws_alerts:
        latitude = _parse_float(forecast.get("latitude"))
        longitude = _parse_float(forecast.get("longitude"))
        if isinstance(latitude, float) and isinstance(longitude, float):
            try:
                alerts_payload = point_alerts_fetcher(
                    latitude=latitude,
                    longitude=longitude,
                    timeout_seconds=timeout_seconds,
                )
            except Exception:
                alerts_payload = {"status": "upstream_error"}
            alerts_status = str(alerts_payload.get("status") or "").strip().lower() or "upstream_error"
            if alerts_status == "ready":
                alerts_count = max(0, int(_parse_float(alerts_payload.get("alerts_count")) or 0))
                for alert in alerts_payload.get("alerts") if isinstance(alerts_payload.get("alerts"), list) else []:
                    if not isinstance(alert, dict):
                        continue
                    event_text = str(alert.get("event") or "").lower()
                    severity_text = str(alert.get("severity") or "").lower()
                    if any(token in event_text for token in ("storm", "warning", "advisory")):
                        alerts_sigma_add = max(alerts_sigma_add, 0.35)
                    if severity_text in {"severe", "extreme"}:
                        alerts_sigma_add = max(alerts_sigma_add, 0.55)
                sigma = sigma + alerts_sigma_add
    probability_raw_unclamped = _threshold_probability(
        threshold_kind,
        threshold_a,
        threshold_b,
        expected_temperature,
        sigma,
    )
    if probability_raw_unclamped is None:
        return None, "unsupported_threshold_expression"
    model_probability = _clamp_probability(probability_raw_unclamped)
    execution_probability = _execution_guard_probability(
        probability_raw_unclamped,
        row,
        max_deviation=0.50,
        midpoint_blend_weight=0.06,
    )
    market_midpoint = _midpoint_probability_from_market(row)

    climatology_count = len(climatology_values)
    mrms_status = str((mrms_payload or {}).get("status") or "").strip().lower()
    mrms_age_seconds = _parse_float((mrms_payload or {}).get("age_seconds"))
    mrms_freshness_boost = 0.0
    if mrms_status == "ready" and isinstance(mrms_age_seconds, float):
        if mrms_age_seconds <= 90.0 * 60.0:
            mrms_freshness_boost = 0.015
        elif mrms_age_seconds >= 6.0 * 3600.0:
            mrms_freshness_boost = -0.02

    nbm_status = str((nbm_payload or {}).get("status") or "").strip().lower()
    nbm_cycle_age_seconds = _parse_float((nbm_payload or {}).get("cycle_age_seconds"))
    nbm_freshness_boost = 0.0
    if nbm_status == "ready" and isinstance(nbm_cycle_age_seconds, float):
        if nbm_cycle_age_seconds <= 3.0 * 3600.0:
            nbm_freshness_boost = 0.02
        elif nbm_cycle_age_seconds >= 12.0 * 3600.0:
            nbm_freshness_boost = -0.02

    confidence = round(
        min(
            0.9,
            max(
                0.25,
                0.36
                + min(0.30, len(temperatures) * 0.02)
                + min(0.18, climatology_count * 0.007)
                + min(0.04, len(gridpoint_tmax_values + gridpoint_tmin_values) * 0.006)
                + min(0.03, observations_recent_count * 0.008)
                - min(0.03, alerts_sigma_add * 0.03)
                + mrms_freshness_boost
                + nbm_freshness_boost,
            ),
        ),
        6,
    )
    interval_half_width = max(0.05, min(0.32, 0.30 - 0.21 * confidence))
    low = _clamp_probability(execution_probability - interval_half_width)
    high = _clamp_probability(execution_probability + interval_half_width)
    updated_at = str(forecast.get("forecast_updated_at") or "").strip()
    thesis_suffix = ""
    if climatology_count:
        thesis_suffix = (
            f" Blended with {climatology_count} historical same-day station realizations "
            f"for bias-resistant pricing."
        )
    if normals_status == "ready":
        thesis_suffix += " NCEI daily normals anchor was applied for station/date baseline."
    if observations_recent_count > 0:
        thesis_suffix += " Recent station observations were used for short-horizon bias correction."
    source_parts = [
        f"nws_station_hourly_forecast:{station_id}",
        f"temp_points={len(temperatures)}",
        f"gridpoint_status={gridpoint_status or 'missing'}",
        f"gridpoint_tmax_points={len(gridpoint_tmax_values)}",
        f"gridpoint_tmin_points={len(gridpoint_tmin_values)}",
        f"forecast_updated_at={updated_at or 'unknown'}",
        f"nws_observations_status={observations_status}",
        f"nws_observations_recent_count={observations_recent_count}",
        f"nws_observations_parse_warning_count={observations_parse_warning_count}",
        f"nws_observations_temp_correction_f={observations_temp_correction:.3f}",
        f"nws_alerts_status={alerts_status}",
        f"nws_alerts_count={alerts_count}",
        f"nws_alerts_sigma_add={alerts_sigma_add:.3f}",
        f"noaa_mrms_status={mrms_status or 'missing'}",
        f"noaa_mrms_age_seconds={mrms_age_seconds if isinstance(mrms_age_seconds, float) else ''}",
        f"noaa_nbm_status={nbm_status or 'missing'}",
        f"noaa_nbm_cycle_age_seconds={nbm_cycle_age_seconds if isinstance(nbm_cycle_age_seconds, float) else ''}",
        f"expected_temperature_f={expected_temperature:.3f}",
        f"sigma_f={sigma:.3f}",
    ]
    history_health = _history_live_health(
        history_payload,
        contract_family="daily_temperature",
        sample_metric=_temperature_sample_metric(expected_label),
    )
    if window_start and window_end:
        source_parts.append(f"settlement_window_local={window_start}-{window_end}")
    if normals_status:
        source_parts.append(f"ncei_normals_status={normals_status}")
    if normals_status == "ready":
        if isinstance(normal_expected, float):
            source_parts.append(f"ncei_normals_expected_temperature_f={normal_expected:.3f}")
        if isinstance(normal_sigma, float):
            source_parts.append(f"ncei_normals_sigma_f={normal_sigma:.3f}")
    if isinstance(history_payload, dict):
        source_parts.append(f"ncei_cdo_status={history_health.get('status')}")
        source_parts.append(f"historical_sample_metric={history_health.get('sample_metric')}")
        source_parts.append(f"historical_sample_years={history_health.get('sample_years')}")
        source_parts.append(f"historical_min_sample_years_required={history_health.get('min_sample_years_required')}")
        source_parts.append(f"historical_samples={climatology_count}")
        source_parts.append(f"station_history_live_ready={history_health.get('live_ready')}")
        source_parts.append(f"station_history_live_ready_reason={history_health.get('live_ready_reason')}")
    source_parts.append(f"execution_guarded_probability={execution_probability:.4f}")

    return (
        {
            "market_ticker": str(row.get("market_ticker") or "").strip(),
            "event_title": str(row.get("event_title") or "").strip(),
            "market_title": str(row.get("market_title") or "").strip(),
            "close_time": str(row.get("close_time") or "").strip(),
            "hours_to_close": str(row.get("hours_to_close") or "").strip(),
            "fair_yes_probability": execution_probability,
            "fair_yes_probability_low": min(low, execution_probability),
            "fair_yes_probability_high": max(high, execution_probability),
            "confidence": confidence,
            "thesis": (
                f"NWS hourly temperatures at {station_id} imply about {execution_probability:.1%} "
                "execution-guarded probability for the "
                f"contract threshold ({threshold_expression}) using expected {expected_label} {expected_temperature:.1f}F."
                f" Settlement day: {target_local_dt.strftime('%Y-%m-%d')}."
                f"{thesis_suffix}"
            ),
            "source_note": "; ".join(source_parts),
            "updated_at": now.isoformat(),
            "evidence_count": (
                len(temperatures)
                + climatology_count
                + len(gridpoint_tmax_values)
                + len(gridpoint_tmin_values)
                + observations_recent_count
                + alerts_count
            ),
            "evidence_quality": round(
                min(
                    1.0,
                    0.58
                    + min(0.22, len(temperatures) * 0.008)
                    + min(0.20, climatology_count * 0.007)
                    + min(0.05, len(gridpoint_tmax_values + gridpoint_tmin_values) * 0.005)
                    + min(0.03, observations_recent_count * 0.006)
                    + max(-0.03, min(0.03, mrms_freshness_boost + nbm_freshness_boost)),
                ),
                6,
            ),
            "source_type": "auto_weather",
            "last_evidence_at": updated_at,
            "contract_family": "daily_temperature",
            "resolution_source_type": "weather_forecast",
            "model_name": "weather_temperature_threshold_historical_v2",
            "model_probability_raw": round(float(probability_raw_unclamped), 6),
            "execution_probability_guarded": execution_probability,
            "market_midpoint_probability": market_midpoint if market_midpoint is not None else "",
        },
        None,
    )


def _build_monthly_anomaly_prior(
    *,
    row: dict[str, str],
    settlement: dict[str, Any],
    noaa_series_payload: dict[str, Any],
    now: datetime,
) -> tuple[dict[str, Any] | None, str | None]:
    if str(noaa_series_payload.get("status") or "") != "ready":
        return None, f"noaa_series_{noaa_series_payload.get('status') or 'unavailable'}"

    threshold_expression = str(settlement.get("threshold_expression") or "").strip()
    threshold_kind, threshold_a, threshold_b = _parse_threshold_expression(threshold_expression)
    if not threshold_kind:
        return None, "missing_threshold_expression"

    values_raw = noaa_series_payload.get("values")
    if not isinstance(values_raw, list):
        return None, "noaa_series_missing_values"
    values = [float(item) for item in values_raw if isinstance(item, (int, float))]
    if len(values) < 24:
        return None, "noaa_series_too_short"

    recent_window = values[-120:] if len(values) >= 120 else values[:]
    latest = recent_window[-1]
    if len(recent_window) >= 13:
        trend = (recent_window[-1] - recent_window[-13]) / 12.0
    else:
        trend = 0.0
    projected = latest + trend
    sigma = max(0.05, pstdev(recent_window[-60:]) if len(recent_window) > 3 else 0.09)
    probability_raw_unclamped = _threshold_probability(threshold_kind, threshold_a, threshold_b, projected, sigma)
    if probability_raw_unclamped is None:
        return None, "unsupported_threshold_expression"
    model_probability = _clamp_probability(probability_raw_unclamped)
    execution_probability = _execution_guard_probability(
        probability_raw_unclamped,
        row,
        max_deviation=0.50,
        midpoint_blend_weight=0.08,
    )
    market_midpoint = _midpoint_probability_from_market(row)

    confidence = round(min(0.9, max(0.3, 0.43 + min(0.25, len(recent_window) / 400.0))), 6)
    interval_half_width = max(0.06, min(0.34, 0.31 - 0.20 * confidence))
    low = _clamp_probability(execution_probability - interval_half_width)
    high = _clamp_probability(execution_probability + interval_half_width)

    end_year = noaa_series_payload.get("end_year")
    end_month = noaa_series_payload.get("end_month")
    end_stamp = ""
    if isinstance(end_year, int) and isinstance(end_month, int):
        end_stamp = f"{end_year:04d}-{end_month:02d}-01T00:00:00+00:00"

    return (
        {
            "market_ticker": str(row.get("market_ticker") or "").strip(),
            "event_title": str(row.get("event_title") or "").strip(),
            "market_title": str(row.get("market_title") or "").strip(),
            "close_time": str(row.get("close_time") or "").strip(),
            "hours_to_close": str(row.get("hours_to_close") or "").strip(),
            "fair_yes_probability": execution_probability,
            "fair_yes_probability_low": min(low, execution_probability),
            "fair_yes_probability_high": max(high, execution_probability),
            "confidence": confidence,
            "thesis": (
                "NOAA global land-ocean anomaly trend and variance imply "
                f"{execution_probability:.1%} execution-guarded probability for threshold ({threshold_expression})."
            ),
            "source_note": (
                f"noaa_global_land_ocean_anomaly_series:{noaa_series_payload.get('series_url', '')}; "
                f"series_end={end_year}-{end_month}; projected={projected:.3f}; sigma={sigma:.3f}; "
                f"execution_guarded_probability={execution_probability:.4f}"
            ),
            "updated_at": now.isoformat(),
            "evidence_count": len(recent_window),
            "evidence_quality": 0.82,
            "source_type": "auto_weather",
            "last_evidence_at": end_stamp,
            "contract_family": "monthly_climate_anomaly",
            "resolution_source_type": "climate_archive",
            "model_name": "weather_monthly_anomaly_threshold_v2",
            "model_probability_raw": round(float(probability_raw_unclamped), 6),
            "execution_probability_guarded": execution_probability,
            "market_midpoint_probability": market_midpoint if market_midpoint is not None else "",
        },
        None,
    )


def run_kalshi_weather_priors(
    *,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    allowed_contract_families: tuple[str, ...] | None = _DEFAULT_ALLOWED_FAMILIES,
    top_n: int = 10,
    max_markets: int = 30,
    timeout_seconds: float = 12.0,
    protect_manual: bool = True,
    write_back_to_priors: bool = True,
    station_forecast_fetcher: WeatherStationForecastFetcher = fetch_nws_station_hourly_forecast,
    station_observations_fetcher: WeatherStationObservationFetcher = fetch_nws_station_recent_observations,
    point_alerts_fetcher: WeatherPointAlertsFetcher = fetch_nws_active_alerts_for_point,
    station_history_fetcher: StationHistoryFetcher = fetch_ncei_cdo_station_daily_history,
    station_normals_fetcher: StationNormalsFetcher = fetch_ncei_normals_station_day,
    mrms_snapshot_fetcher: MrmsSnapshotFetcher = fetch_noaa_mrms_qpe_latest_metadata,
    nbm_snapshot_fetcher: NbmSnapshotFetcher = fetch_noaa_nbm_latest_snapshot,
    anomaly_series_fetcher: NoaaAnomalySeriesFetcher = fetch_noaa_global_land_ocean_anomaly_series,
    historical_lookback_years: int = _DEFAULT_HISTORICAL_LOOKBACK_YEARS,
    station_history_cache_max_age_hours: float = _DEFAULT_STATION_HISTORY_CACHE_MAX_AGE_HOURS,
    include_nws_gridpoint_data: bool = False,
    include_nws_observations: bool = False,
    include_nws_alerts: bool = False,
    include_ncei_normals: bool = False,
    include_mrms_qpe: bool = False,
    include_nbm_snapshot: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    run_id = f"kalshi_weather_priors::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
    priors_path = Path(priors_csv)
    history_path = Path(history_csv)
    prior_rows = load_prior_rows(priors_path)
    history_rows = load_history_rows(history_path)
    latest_rows = _latest_market_rows(history_rows)
    allowed_family_set = _normalize_string_set(allowed_contract_families)

    candidate_rows: list[tuple[dict[str, str], dict[str, Any]]] = []
    for row in latest_rows.values():
        settlement = build_weather_settlement_spec(row)
        family = str(settlement.get("contract_family") or "").strip().lower()
        if not family or family in {"non_weather", "weather_other"}:
            continue
        if allowed_family_set and family not in allowed_family_set:
            continue
        candidate_rows.append((row, settlement))

    candidate_rows.sort(
        key=lambda item: (
            _parse_float(item[0].get("hours_to_close")) if _parse_float(item[0].get("hours_to_close")) is not None else 1e9,
            _parse_float(item[0].get("spread_dollars")) if _parse_float(item[0].get("spread_dollars")) is not None else 1.0,
        )
    )
    candidate_rows = candidate_rows[: max(1, int(max_markets))]

    has_monthly_family = any(
        str(settlement.get("contract_family") or "").strip().lower() == "monthly_climate_anomaly"
        for _, settlement in candidate_rows
    )
    has_daily_weather_family = any(
        str(settlement.get("contract_family") or "").strip().lower() in {"daily_rain", "daily_temperature", "daily_snow"}
        for _, settlement in candidate_rows
    )
    noaa_series_payload: dict[str, Any] | None = None
    mrms_snapshot_payload: dict[str, Any] | None = None
    nbm_snapshot_payload: dict[str, Any] | None = None
    fetch_errors: list[dict[str, str]] = []
    fetch_error_kind_counts: dict[str, int] = {}
    if has_monthly_family:
        try:
            noaa_series_payload = anomaly_series_fetcher(timeout_seconds=timeout_seconds)
        except Exception as exc:
            error_text = str(exc)
            error_kind = _classify_fetch_error(exc)
            fetch_errors.append(
                {
                    "source": "noaa_series",
                    "market_ticker": "",
                    "error": error_text,
                    "error_kind": error_kind,
                }
            )
            fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
            noaa_series_payload = {
                "status": "upstream_error",
                "error": error_text,
                "error_kind": error_kind,
            }

    if has_daily_weather_family and include_mrms_qpe:
        try:
            mrms_snapshot_payload = mrms_snapshot_fetcher(
                timeout_seconds=timeout_seconds,
                now=captured_at,
            )
        except Exception as exc:
            error_text = str(exc)
            error_kind = _classify_fetch_error(exc)
            fetch_errors.append(
                {
                    "source": "mrms_snapshot",
                    "market_ticker": "",
                    "error": error_text,
                    "error_kind": error_kind,
                }
            )
            fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
            mrms_snapshot_payload = {
                "status": "upstream_error",
                "error": error_text,
                "error_kind": error_kind,
            }

    if has_daily_weather_family and include_nbm_snapshot:
        try:
            nbm_snapshot_payload = nbm_snapshot_fetcher(
                timeout_seconds=timeout_seconds,
                now=captured_at,
            )
        except Exception as exc:
            error_text = str(exc)
            error_kind = _classify_fetch_error(exc)
            fetch_errors.append(
                {
                    "source": "nbm_snapshot",
                    "market_ticker": "",
                    "error": error_text,
                    "error_kind": error_kind,
                }
            )
            fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
            nbm_snapshot_payload = {
                "status": "upstream_error",
                "error": error_text,
                "error_kind": error_kind,
            }

    generated_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    family_generated_counts: dict[str, int] = {}
    family_skipped_counts: dict[str, int] = {}
    history_fetch_status_counts: dict[str, int] = {}
    station_history_cache: dict[tuple[str, int, int], dict[str, Any]] = {}
    station_normals_cache: dict[tuple[str, int, int], dict[str, Any]] = {}
    station_history_cache_dir = str(Path(output_dir) / "weather_station_history_cache")
    station_history_fetcher_signature = inspect.signature(station_history_fetcher)
    supports_history_cache_dir = "cache_dir" in station_history_fetcher_signature.parameters
    supports_history_cache_age = "cache_max_age_hours" in station_history_fetcher_signature.parameters

    for row, settlement in candidate_rows:
        family = str(settlement.get("contract_family") or "").strip().lower()
        generated: dict[str, Any] | None = None
        skip_reason: str | None = None
        history_payload: dict[str, Any] | None = None
        normals_payload: dict[str, Any] | None = None
        history_health: dict[str, Any] | None = None
        if family in {"daily_rain", "daily_temperature"}:
            station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
            month_value, day_value = _target_month_day(row=row, settlement=settlement, now=captured_at)
            cache_key = (station_id, month_value, day_value)
            if station_id and cache_key not in station_history_cache:
                try:
                    history_fetch_kwargs: dict[str, Any] = {
                        "station_id": station_id,
                        "month": month_value,
                        "day": day_value,
                        "lookback_years": historical_lookback_years,
                        "timeout_seconds": timeout_seconds,
                        "now": captured_at,
                    }
                    if supports_history_cache_dir:
                        history_fetch_kwargs["cache_dir"] = station_history_cache_dir
                    if supports_history_cache_age:
                        history_fetch_kwargs["cache_max_age_hours"] = max(
                            0.0,
                            float(station_history_cache_max_age_hours),
                        )
                    station_history_cache[cache_key] = station_history_fetcher(**history_fetch_kwargs)
                except Exception as exc:
                    error_text = str(exc)
                    error_kind = _classify_fetch_error(exc)
                    fetch_errors.append(
                        {
                            "source": "station_history",
                            "market_ticker": str(row.get("market_ticker") or "").strip(),
                            "error": error_text,
                            "error_kind": error_kind,
                        }
                    )
                    fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
                    station_history_cache[cache_key] = {
                        "status": "upstream_error",
                        "error": error_text,
                        "error_kind": error_kind,
                    }
            if station_id:
                history_payload = station_history_cache.get(cache_key)
                history_status = str((history_payload or {}).get("status") or "").strip().lower() or "unknown"
                history_fetch_status_counts[history_status] = history_fetch_status_counts.get(history_status, 0) + 1
                if include_ncei_normals and cache_key not in station_normals_cache:
                    try:
                        station_normals_cache[cache_key] = station_normals_fetcher(
                            station_id=station_id,
                            month=month_value,
                            day=day_value,
                            timeout_seconds=timeout_seconds,
                        )
                    except Exception as exc:
                        error_text = str(exc)
                        error_kind = _classify_fetch_error(exc)
                        fetch_errors.append(
                            {
                                "source": "station_normals",
                                "market_ticker": str(row.get("market_ticker") or "").strip(),
                                "error": error_text,
                                "error_kind": error_kind,
                            }
                        )
                        fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
                        station_normals_cache[cache_key] = {
                            "status": "upstream_error",
                            "error": error_text,
                            "error_kind": error_kind,
                        }
                if include_ncei_normals:
                    normals_payload = station_normals_cache.get(cache_key)
            history_sample_metric = ""
            if family == "daily_rain":
                history_sample_metric = "precip"
            elif family == "daily_temperature":
                history_sample_metric = _temperature_sample_metric(_temperature_expected_label(row))
            history_health = _history_live_health(
                history_payload,
                contract_family=family,
                sample_metric=history_sample_metric,
            )
        if family == "daily_rain":
            try:
                generated, skip_reason = _build_daily_rain_prior(
                    row=row,
                    settlement=settlement,
                    history_payload=history_payload,
                    normals_payload=normals_payload,
                    mrms_payload=mrms_snapshot_payload,
                    nbm_payload=nbm_snapshot_payload,
                    now=captured_at,
                    timeout_seconds=timeout_seconds,
                    include_nws_gridpoint_data=include_nws_gridpoint_data,
                    include_nws_observations=include_nws_observations,
                    include_nws_alerts=include_nws_alerts,
                    station_forecast_fetcher=station_forecast_fetcher,
                    station_observations_fetcher=station_observations_fetcher,
                    point_alerts_fetcher=point_alerts_fetcher,
                )
            except Exception as exc:
                error_text = str(exc)
                error_kind = _classify_fetch_error(exc)
                fetch_errors.append(
                    {
                        "source": "station_forecast",
                        "market_ticker": str(row.get("market_ticker") or "").strip(),
                        "error": error_text,
                        "error_kind": error_kind,
                    }
                )
                fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
                generated, skip_reason = None, f"station_forecast_exception:{error_kind}"
        elif family == "daily_temperature":
            try:
                generated, skip_reason = _build_daily_temperature_prior(
                    row=row,
                    settlement=settlement,
                    history_payload=history_payload,
                    normals_payload=normals_payload,
                    mrms_payload=mrms_snapshot_payload,
                    nbm_payload=nbm_snapshot_payload,
                    now=captured_at,
                    timeout_seconds=timeout_seconds,
                    include_nws_gridpoint_data=include_nws_gridpoint_data,
                    include_nws_observations=include_nws_observations,
                    include_nws_alerts=include_nws_alerts,
                    station_forecast_fetcher=station_forecast_fetcher,
                    station_observations_fetcher=station_observations_fetcher,
                    point_alerts_fetcher=point_alerts_fetcher,
                )
            except Exception as exc:
                error_text = str(exc)
                error_kind = _classify_fetch_error(exc)
                fetch_errors.append(
                    {
                        "source": "station_forecast",
                        "market_ticker": str(row.get("market_ticker") or "").strip(),
                        "error": error_text,
                        "error_kind": error_kind,
                    }
                )
                fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
                generated, skip_reason = None, f"station_forecast_exception:{error_kind}"
        elif family == "monthly_climate_anomaly":
            generated, skip_reason = _build_monthly_anomaly_prior(
                row=row,
                settlement=settlement,
                noaa_series_payload=noaa_series_payload or {"status": "noaa_series_unavailable"},
                now=captured_at,
            )
        else:
            skip_reason = f"unsupported_contract_family:{family}"

        if generated is None:
            family_skipped_counts[family] = family_skipped_counts.get(family, 0) + 1
            skipped_rows.append(
                {
                    "market_ticker": str(row.get("market_ticker") or "").strip(),
                    "contract_family": family,
                    "skip_reason": skip_reason or "unknown_skip_reason",
                }
            )
            continue

        generated.update(
            {
                "settlement_source_primary": str(settlement.get("settlement_source_primary") or "").strip(),
                "settlement_source_fallback": str(settlement.get("settlement_source_fallback") or "").strip(),
                "settlement_station": (
                    str(settlement.get("settlement_station") or "").strip()
                    or _infer_station_id(row, "")
                ),
                "settlement_timezone": str(settlement.get("settlement_timezone") or "").strip(),
                "local_day_boundary": str(settlement.get("local_day_boundary") or "").strip(),
                "observation_window_local_start": str(settlement.get("observation_window_local_start") or "").strip(),
                "observation_window_local_end": str(settlement.get("observation_window_local_end") or "").strip(),
                "observation_window_local_source": str(settlement.get("observation_window_local_source") or "").strip(),
                "threshold_expression": str(settlement.get("threshold_expression") or "").strip(),
                "rule_text_hash_sha256": str(settlement.get("rule_text_hash_sha256") or "").strip(),
                "weather_station_history_status": (
                    str((history_health or {}).get("status") or "").strip()
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_cache_hit": (
                    bool((history_health or {}).get("cache_hit"))
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_cache_fallback_used": (
                    bool((history_health or {}).get("cache_fallback_used"))
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_cache_fresh": (
                    bool((history_health or {}).get("cache_fresh"))
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_cache_age_seconds": (
                    (history_health or {}).get("cache_age_seconds")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_metric": (
                    str((history_health or {}).get("sample_metric") or "").strip()
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years": (
                    (history_health or {}).get("sample_years")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years_total": (
                    (history_health or {}).get("sample_years_total")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years_precip": (
                    (history_health or {}).get("sample_years_precip")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years_tmax": (
                    (history_health or {}).get("sample_years_tmax")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years_tmin": (
                    (history_health or {}).get("sample_years_tmin")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_sample_years_mean": (
                    (history_health or {}).get("sample_years_mean")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_min_sample_years_required": (
                    (history_health or {}).get("min_sample_years_required")
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_live_ready": (
                    bool((history_health or {}).get("live_ready"))
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
                "weather_station_history_live_ready_reason": (
                    str((history_health or {}).get("live_ready_reason") or "").strip()
                    if family in {"daily_rain", "daily_temperature"}
                    else ""
                ),
            }
        )
        generated_rows.append(generated)
        family_generated_counts[family] = family_generated_counts.get(family, 0) + 1

    generated_rows.sort(
        key=lambda row: (
            _parse_float(row.get("confidence")) or 0.0,
            _parse_float(row.get("evidence_quality")) or 0.0,
            -(_parse_float(row.get("hours_to_close")) or 1e9),
        ),
        reverse=True,
    )

    upsert_summary = {
        "inserted": 0,
        "updated": 0,
        "skipped_manual": 0,
        "rows_total": len(prior_rows),
    }
    if write_back_to_priors and generated_rows:
        upsert_summary = _upsert_priors_csv(
            priors_path=priors_path,
            generated_rows=generated_rows,
            protect_manual=protect_manual,
        )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_csv_path = out_dir / f"kalshi_weather_priors_{stamp}.csv"
    skipped_csv_path = out_dir / f"kalshi_weather_priors_skipped_{stamp}.csv"
    _write_csv(generated_csv_path, generated_rows, WEATHER_PRIOR_OUTPUT_FIELDNAMES)
    _write_csv(skipped_csv_path, skipped_rows, ["market_ticker", "contract_family", "skip_reason"])
    model_tags = detect_weather_model_tags(generated_rows)
    rain_model_tag = model_tags.get("rain_model_tag")
    temperature_model_tag = model_tags.get("temperature_model_tag")
    weather_priors_version_name = weather_priors_version(
        rain_model_tag=rain_model_tag,
        temperature_model_tag=temperature_model_tag,
    )
    weather_station_history_cache_age_seconds: float | None = None
    if supports_history_cache_dir and station_history_cache_dir:
        cache_dir = Path(station_history_cache_dir)
        if cache_dir.exists():
            cache_files = [path for path in cache_dir.glob("*.json") if path.is_file()]
            if cache_files:
                newest_mtime = max(path.stat().st_mtime for path in cache_files)
                newest_dt = datetime.fromtimestamp(newest_mtime, tz=timezone.utc)
                weather_station_history_cache_age_seconds = round(
                    max(0.0, (captured_at - newest_dt).total_seconds()),
                    3,
                )

    summary = {
        "run_id": run_id,
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "history_csv_path": str(history_path),
        "history_csv_mtime_utc": file_mtime_utc(history_path),
        "priors_csv": str(priors_path),
        "write_back_to_priors": write_back_to_priors,
        "protect_manual": protect_manual,
        "allowed_contract_families": sorted(allowed_family_set) if allowed_family_set else list(_DEFAULT_ALLOWED_FAMILIES),
        "historical_lookback_years": int(historical_lookback_years),
        "include_nws_gridpoint_data": bool(include_nws_gridpoint_data),
        "include_nws_observations": bool(include_nws_observations),
        "include_nws_alerts": bool(include_nws_alerts),
        "include_ncei_normals": bool(include_ncei_normals),
        "include_mrms_qpe": bool(include_mrms_qpe),
        "include_nbm_snapshot": bool(include_nbm_snapshot),
        "station_history_cache_dir": station_history_cache_dir if supports_history_cache_dir else None,
        "station_history_cache_max_age_hours": (
            max(0.0, float(station_history_cache_max_age_hours)) if supports_history_cache_age else None
        ),
        "candidate_markets": len(candidate_rows),
        "generated_priors": len(generated_rows),
        "skipped_markets": len(skipped_rows),
        "inserted_rows": upsert_summary.get("inserted", 0),
        "updated_rows": upsert_summary.get("updated", 0),
        "manual_rows_protected": upsert_summary.get("skipped_manual", 0),
        "prior_rows_total_after_upsert": upsert_summary.get("rows_total", len(prior_rows)),
        "contract_family_generated_counts": family_generated_counts,
        "contract_family_skipped_counts": family_skipped_counts,
        "station_history_cache_entries": len(station_history_cache),
        "station_normals_cache_entries": len(station_normals_cache),
        "weather_station_history_cache_age_seconds": weather_station_history_cache_age_seconds,
        "station_history_status_counts": history_fetch_status_counts,
        "mrms_snapshot_status": str((mrms_snapshot_payload or {}).get("status") or "").strip(),
        "mrms_snapshot_age_seconds": (mrms_snapshot_payload or {}).get("age_seconds"),
        "nbm_snapshot_status": str((nbm_snapshot_payload or {}).get("status") or "").strip(),
        "nbm_snapshot_cycle_age_seconds": (nbm_snapshot_payload or {}).get("cycle_age_seconds"),
        "rain_model_tag": rain_model_tag,
        "temperature_model_tag": temperature_model_tag,
        "weather_priors_version": weather_priors_version_name,
        "top_market_ticker": generated_rows[0]["market_ticker"] if generated_rows else None,
        "top_market_confidence": generated_rows[0]["confidence"] if generated_rows else None,
        "top_markets": generated_rows[: max(1, int(top_n))],
        "fetch_errors_count": len(fetch_errors),
        "fetch_error_kind_counts": fetch_error_kind_counts,
        "fetch_errors": fetch_errors,
        "error": fetch_errors[0]["error"] if fetch_errors else None,
        "error_kind": fetch_errors[0]["error_kind"] if fetch_errors else None,
        "status": "ready" if generated_rows else ("upstream_error" if fetch_errors else "no_weather_priors"),
        "output_csv": str(generated_csv_path),
        "skipped_output_csv": str(skipped_csv_path),
    }

    summary_path = out_dir / f"kalshi_weather_priors_summary_{stamp}.json"
    summary["runtime_version"] = build_runtime_version_block(
        run_started_at=captured_at,
        run_id=run_id,
        git_cwd=Path.cwd(),
        rain_model_tag=rain_model_tag,
        temperature_model_tag=temperature_model_tag,
        weather_priors_version_name=weather_priors_version_name,
        as_of=captured_at,
    )
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary


def run_kalshi_weather_station_history_prewarm(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    historical_lookback_years: int = _DEFAULT_HISTORICAL_LOOKBACK_YEARS,
    station_history_cache_max_age_hours: float = _DEFAULT_STATION_HISTORY_CACHE_MAX_AGE_HOURS,
    timeout_seconds: float = 12.0,
    max_station_day_keys: int = 500,
    station_history_fetcher: StationHistoryFetcher = fetch_ncei_cdo_station_daily_history,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    latest_rows = _latest_market_rows(history_rows)

    key_index: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in latest_rows.values():
        settlement = build_weather_settlement_spec(row)
        family = str(settlement.get("contract_family") or "").strip().lower()
        if family not in {"daily_rain", "daily_temperature", "daily_snow"}:
            continue
        station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
        if not station_id:
            continue
        month_value, day_value = _target_month_day(row=row, settlement=settlement, now=captured_at)
        key = (station_id, month_value, day_value)
        hours_to_close = _parse_float(row.get("hours_to_close"))
        entry = key_index.setdefault(
            key,
            {
                "station_id": station_id,
                "month": month_value,
                "day": day_value,
                "markets": 0,
                "min_hours_to_close": hours_to_close if isinstance(hours_to_close, float) else None,
                "sample_tickers": [],
            },
        )
        entry["markets"] = int(entry.get("markets") or 0) + 1
        if isinstance(hours_to_close, float):
            current_min = entry.get("min_hours_to_close")
            if not isinstance(current_min, float) or hours_to_close < current_min:
                entry["min_hours_to_close"] = hours_to_close
        sample_tickers = entry.get("sample_tickers")
        if isinstance(sample_tickers, list):
            ticker = str(row.get("market_ticker") or "").strip()
            if ticker and ticker not in sample_tickers and len(sample_tickers) < 5:
                sample_tickers.append(ticker)

    ranked_keys = sorted(
        key_index.values(),
        key=lambda item: (
            item.get("min_hours_to_close") if isinstance(item.get("min_hours_to_close"), float) else 1e9,
            -int(item.get("markets") or 0),
            str(item.get("station_id") or ""),
            int(item.get("month") or 0),
            int(item.get("day") or 0),
        ),
    )
    if max_station_day_keys > 0:
        ranked_keys = ranked_keys[: int(max_station_day_keys)]

    station_history_cache_dir = str(Path(output_dir) / "weather_station_history_cache")
    fetcher_signature = inspect.signature(station_history_fetcher)
    supports_history_cache_dir = "cache_dir" in fetcher_signature.parameters
    supports_history_cache_age = "cache_max_age_hours" in fetcher_signature.parameters

    status_counts: dict[str, int] = {}
    live_ready_counts: dict[str, int] = {}
    fetch_error_kind_counts: dict[str, int] = {}
    entries: list[dict[str, Any]] = []
    for key in ranked_keys:
        station_id = str(key.get("station_id") or "").strip()
        month_value = int(key.get("month") or 0)
        day_value = int(key.get("day") or 0)
        payload: dict[str, Any]
        try:
            history_fetch_kwargs: dict[str, Any] = {
                "station_id": station_id,
                "month": month_value,
                "day": day_value,
                "lookback_years": int(historical_lookback_years),
                "timeout_seconds": float(timeout_seconds),
                "now": captured_at,
            }
            if supports_history_cache_dir:
                history_fetch_kwargs["cache_dir"] = station_history_cache_dir
            if supports_history_cache_age:
                history_fetch_kwargs["cache_max_age_hours"] = max(0.0, float(station_history_cache_max_age_hours))
            payload = station_history_fetcher(**history_fetch_kwargs)
        except Exception as exc:
            error_kind = _classify_fetch_error(exc)
            fetch_error_kind_counts[error_kind] = fetch_error_kind_counts.get(error_kind, 0) + 1
            payload = {
                "status": "upstream_error",
                "error": str(exc),
                "error_kind": error_kind,
            }
        health = _history_live_health(payload)
        status_value = str(health.get("status") or "").strip().lower() or "unknown"
        status_counts[status_value] = status_counts.get(status_value, 0) + 1
        live_ready_key = "live_ready" if bool(health.get("live_ready")) else "not_live_ready"
        live_ready_counts[live_ready_key] = live_ready_counts.get(live_ready_key, 0) + 1
        entries.append(
            {
                "station_id": station_id,
                "month": month_value,
                "day": day_value,
                "markets": int(key.get("markets") or 0),
                "min_hours_to_close": key.get("min_hours_to_close"),
                "status": status_value,
                "cache_hit": bool(health.get("cache_hit")),
                "cache_fallback_used": bool(health.get("cache_fallback_used")),
                "cache_fresh": bool(health.get("cache_fresh")),
                "cache_age_seconds": health.get("cache_age_seconds"),
                "live_ready": bool(health.get("live_ready")),
                "live_ready_reason": str(health.get("live_ready_reason") or "").strip(),
                "sample_tickers": ",".join(str(value) for value in (key.get("sample_tickers") or [])),
            }
        )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_csv = out_dir / f"kalshi_weather_station_history_prewarm_{stamp}.csv"
    _write_csv(
        output_csv,
        entries,
        [
            "station_id",
            "month",
            "day",
            "markets",
            "min_hours_to_close",
            "status",
            "cache_hit",
            "cache_fallback_used",
            "cache_fresh",
            "cache_age_seconds",
            "live_ready",
            "live_ready_reason",
            "sample_tickers",
        ],
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "candidate_markets": len(latest_rows),
        "daily_weather_station_day_keys_total": len(key_index),
        "prewarm_keys_attempted": len(ranked_keys),
        "max_station_day_keys": int(max_station_day_keys),
        "historical_lookback_years": int(historical_lookback_years),
        "station_history_cache_dir": station_history_cache_dir if supports_history_cache_dir else None,
        "station_history_cache_max_age_hours": (
            max(0.0, float(station_history_cache_max_age_hours)) if supports_history_cache_age else None
        ),
        "status_counts": status_counts,
        "live_ready_counts": live_ready_counts,
        "fetch_error_kind_counts": fetch_error_kind_counts,
        "top_unhealthy_keys": [
            item for item in entries if not bool(item.get("live_ready"))
        ][:25],
        "output_csv": str(output_csv),
        "status": "ready",
    }
    output_file = out_dir / f"kalshi_weather_station_history_prewarm_summary_{stamp}.json"
    output_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_file)
    return summary
