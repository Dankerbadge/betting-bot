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
from betbot.kalshi_weather_ingest import (
    fetch_ncei_cdo_station_daily_history,
    fetch_noaa_global_land_ocean_anomaly_series,
    fetch_nws_station_hourly_forecast,
)
from betbot.kalshi_weather_settlement import build_weather_settlement_spec


WeatherStationForecastFetcher = Callable[..., dict[str, Any]]
NoaaAnomalySeriesFetcher = Callable[..., dict[str, Any]]
StationHistoryFetcher = Callable[..., dict[str, Any]]


_DEFAULT_ALLOWED_FAMILIES = (
    "daily_rain",
    "daily_temperature",
    "monthly_climate_anomaly",
)
_DEFAULT_HISTORICAL_LOOKBACK_YEARS = 15
_DEFAULT_STATION_HISTORY_CACHE_MAX_AGE_HOURS = 24.0

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
    "la ": "KLAX",
    "san francisco": "KSFO",
    "sf ": "KSFO",
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
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


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
    merged = " ".join(
        (
            str(row.get("market_ticker") or ""),
            str(row.get("event_title") or ""),
            str(row.get("market_title") or ""),
            str(row.get("rules_primary") or ""),
        )
    ).lower()
    for token, station_id in _CITY_STATION_FALLBACKS.items():
        if token in merged:
            return station_id
    return ""


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
        settlement_day_periods: list[dict[str, Any]] = []
        for period in periods:
            start_time = _parse_datetime(str(period.get("startTime") or ""))
            if start_time is None:
                continue
            local_start = start_time.astimezone(settlement_zone)
            if local_start.date() != target_settlement_date:
                continue
            if start_minutes is not None and end_minutes is not None:
                minute_of_day = (local_start.hour * 60) + local_start.minute
                if start_minutes <= end_minutes:
                    if minute_of_day < start_minutes or minute_of_day > end_minutes:
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
    merged = " ".join(
        (
            str(row.get("event_title") or ""),
            str(row.get("market_title") or ""),
            str(row.get("market_ticker") or ""),
        )
    ).lower()
    if any(token in merged for token in ("new york", "nyc", "boston", "washington", "dc", "miami", "atlanta", "philly", "philadelphia")):
        return "America/New_York"
    if any(token in merged for token in ("chicago", "dallas", "houston", "minneapolis")):
        return "America/Chicago"
    if any(token in merged for token in ("denver", "salt lake city")):
        return "America/Denver"
    if any(token in merged for token in ("los angeles", "la ", "san francisco", "sf ", "seattle", "portland", "las vegas")):
        return "America/Los_Angeles"
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
    return [float(value) for value in raw if isinstance(value, (int, float))]


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


def _build_daily_rain_prior(
    *,
    row: dict[str, str],
    settlement: dict[str, Any],
    history_payload: dict[str, Any] | None,
    now: datetime,
    timeout_seconds: float,
    station_forecast_fetcher: WeatherStationForecastFetcher,
) -> tuple[dict[str, Any] | None, str | None]:
    station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
    if not station_id:
        return None, "missing_settlement_station"

    forecast = station_forecast_fetcher(station_id=station_id, timeout_seconds=timeout_seconds)
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
        value = None
        if isinstance(probability_payload, dict):
            value = _parse_float(probability_payload.get("value"))
        if value is None:
            continue
        pop_values.append(min(1.0, max(0.0, value / 100.0)))
    if not pop_values:
        return None, "station_forecast_missing_precip_probability"

    no_rain_probability = 1.0
    for value in pop_values:
        no_rain_probability *= (1.0 - value)
    model_probability_raw_unclamped = 1.0 - no_rain_probability
    climatology_frequency = None
    if isinstance(history_payload, dict):
        rain_frequency = history_payload.get("rain_day_frequency")
        if isinstance(rain_frequency, (int, float)):
            climatology_frequency = max(0.0, min(1.0, float(rain_frequency)))
    if climatology_frequency is not None:
        model_probability_raw_unclamped = 0.78 * model_probability_raw_unclamped + 0.22 * climatology_frequency
    model_probability = _clamp_probability(model_probability_raw_unclamped)
    execution_probability = _execution_guard_probability(
        model_probability_raw_unclamped,
        row,
        max_deviation=0.50,
        midpoint_blend_weight=0.06,
    )
    market_midpoint = _midpoint_probability_from_market(row)

    updated_at = str(forecast.get("forecast_updated_at") or "").strip()
    age_penalty = 0.0
    updated_dt = _parse_datetime(updated_at)
    if isinstance(updated_dt, datetime):
        age_hours = max(0.0, (now - updated_dt).total_seconds() / 3600.0)
        age_penalty = min(0.2, age_hours / 48.0)
    climatology_count = 0
    if isinstance(history_payload, dict):
        climatology_count = int(history_payload.get("sample_years") or 0)
    confidence = round(
        min(
            0.92,
            max(
                0.25,
                0.40
                + min(0.40, len(pop_values) * 0.02)
                + min(0.12, climatology_count * 0.006)
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
        thesis_suffix = f" Blended with {climatology_count}y station climatology rain frequency {climatology_frequency:.1%}."
    source_parts = [
        f"nws_station_hourly_forecast:{station_id}",
        f"periods_used={len(pop_values)}",
        f"forecast_updated_at={updated_at or 'unknown'}",
    ]
    window_start = str(settlement.get("observation_window_local_start") or "").strip()
    window_end = str(settlement.get("observation_window_local_end") or "").strip()
    if window_start and window_end:
        source_parts.append(f"settlement_window_local={window_start}-{window_end}")
    if isinstance(history_payload, dict):
        source_parts.append(f"ncei_cdo_status={history_payload.get('status')}")
        source_parts.append(f"historical_years={climatology_count}")
        if climatology_frequency is not None:
            source_parts.append(f"historical_rain_freq={climatology_frequency:.4f}")
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
            "evidence_count": len(pop_values) + climatology_count,
            "evidence_quality": round(
                min(1.0, 0.55 + min(0.25, len(pop_values) * 0.01) + min(0.20, climatology_count * 0.008)),
                6,
            ),
            "source_type": "auto_weather",
            "last_evidence_at": updated_at,
            "contract_family": "daily_rain",
            "resolution_source_type": "weather_forecast",
            "model_name": "weather_rain_pop_historical_v2",
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
    now: datetime,
    timeout_seconds: float,
    station_forecast_fetcher: WeatherStationForecastFetcher,
) -> tuple[dict[str, Any] | None, str | None]:
    station_id = _infer_station_id(row, str(settlement.get("settlement_station") or ""))
    if not station_id:
        return None, "missing_settlement_station"

    threshold_expression = str(settlement.get("threshold_expression") or "").strip()
    threshold_kind, threshold_a, threshold_b = _parse_threshold_expression(threshold_expression)
    if not threshold_kind:
        return None, "missing_threshold_expression"

    forecast = station_forecast_fetcher(station_id=station_id, timeout_seconds=timeout_seconds)
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
    temperatures: list[float] = []
    for period in scoped_periods:
        temperature = _parse_float(period.get("temperature"))
        if temperature is not None:
            temperatures.append(temperature)
    if not temperatures:
        return None, "station_forecast_missing_temperatures"

    descriptor = " ".join(
        (
            str(row.get("event_title") or ""),
            str(row.get("market_title") or ""),
            str(row.get("rules_primary") or ""),
        )
    ).lower()
    if any(token in descriptor for token in ("daily low", "low temperature", "lowest temperature")):
        expected_temperature = min(temperatures)
        expected_label = "daily low"
    elif any(token in descriptor for token in ("daily high", "high temperature", "highest temperature")):
        expected_temperature = max(temperatures)
        expected_label = "daily high"
    else:
        expected_temperature = mean(temperatures)
        expected_label = "hourly mean"

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
    confidence = round(
        min(
            0.9,
            max(
                0.25,
                0.36 + min(0.30, len(temperatures) * 0.02) + min(0.18, climatology_count * 0.007),
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
    source_parts = [
        f"nws_station_hourly_forecast:{station_id}",
        f"temp_points={len(temperatures)}",
        f"forecast_updated_at={updated_at or 'unknown'}",
    ]
    window_start = str(settlement.get("observation_window_local_start") or "").strip()
    window_end = str(settlement.get("observation_window_local_end") or "").strip()
    if window_start and window_end:
        source_parts.append(f"settlement_window_local={window_start}-{window_end}")
    if isinstance(history_payload, dict):
        source_parts.append(f"ncei_cdo_status={history_payload.get('status')}")
        source_parts.append(f"historical_samples={climatology_count}")
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
            "evidence_count": len(temperatures) + climatology_count,
            "evidence_quality": round(
                min(1.0, 0.58 + min(0.22, len(temperatures) * 0.008) + min(0.20, climatology_count * 0.007)),
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
    station_history_fetcher: StationHistoryFetcher = fetch_ncei_cdo_station_daily_history,
    anomaly_series_fetcher: NoaaAnomalySeriesFetcher = fetch_noaa_global_land_ocean_anomaly_series,
    historical_lookback_years: int = _DEFAULT_HISTORICAL_LOOKBACK_YEARS,
    station_history_cache_max_age_hours: float = _DEFAULT_STATION_HISTORY_CACHE_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
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
    noaa_series_payload: dict[str, Any] | None = None
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

    generated_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    family_generated_counts: dict[str, int] = {}
    family_skipped_counts: dict[str, int] = {}
    history_fetch_status_counts: dict[str, int] = {}
    station_history_cache: dict[tuple[str, int, int], dict[str, Any]] = {}
    station_history_cache_dir = str(Path(output_dir) / "weather_station_history_cache")
    station_history_fetcher_signature = inspect.signature(station_history_fetcher)
    supports_history_cache_dir = "cache_dir" in station_history_fetcher_signature.parameters
    supports_history_cache_age = "cache_max_age_hours" in station_history_fetcher_signature.parameters

    for row, settlement in candidate_rows:
        family = str(settlement.get("contract_family") or "").strip().lower()
        generated: dict[str, Any] | None = None
        skip_reason: str | None = None
        history_payload: dict[str, Any] | None = None
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
        if family == "daily_rain":
            try:
                generated, skip_reason = _build_daily_rain_prior(
                    row=row,
                    settlement=settlement,
                    history_payload=history_payload,
                    now=captured_at,
                    timeout_seconds=timeout_seconds,
                    station_forecast_fetcher=station_forecast_fetcher,
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
                    now=captured_at,
                    timeout_seconds=timeout_seconds,
                    station_forecast_fetcher=station_forecast_fetcher,
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

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "priors_csv": str(priors_path),
        "write_back_to_priors": write_back_to_priors,
        "protect_manual": protect_manual,
        "allowed_contract_families": sorted(allowed_family_set) if allowed_family_set else list(_DEFAULT_ALLOWED_FAMILIES),
        "historical_lookback_years": int(historical_lookback_years),
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
        "station_history_status_counts": history_fetch_status_counts,
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
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
