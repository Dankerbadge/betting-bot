from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Callable
from zoneinfo import ZoneInfo

from betbot.kalshi_weather_ingest import fetch_nws_station_recent_observations


JsonGetter = Callable[[str, float], tuple[int, dict[str, Any] | list[Any] | Any]]


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def celsius_to_fahrenheit(value_c: float) -> float:
    return (float(value_c) * 9.0 / 5.0) + 32.0


def quantize_temperature(
    value: float,
    *,
    precision: str = "whole_degree",
    rounding_mode: str = "nearest",
) -> float:
    raw = float(value)
    if not math.isfinite(raw):
        raise ValueError("Temperature must be finite.")

    precision_key = str(precision or "whole_degree").strip().lower()
    rounding_key = str(rounding_mode or "nearest").strip().lower()
    scale = 1.0
    if precision_key == "tenth_degree":
        scale = 10.0

    scaled = raw * scale
    if rounding_key == "nearest":
        if scaled >= 0.0:
            rounded = math.floor(scaled + 0.5)
        else:
            rounded = math.ceil(scaled - 0.5)
    elif rounding_key == "floor":
        rounded = math.floor(scaled)
    elif rounding_key == "ceil":
        rounded = math.ceil(scaled)
    else:
        raise ValueError(f"Unsupported rounding mode: {rounding_mode}")

    return rounded / scale


def build_intraday_temperature_snapshot(
    *,
    station_id: str,
    target_date_local: str,
    timezone_name: str,
    settlement_unit: str = "celsius",
    settlement_precision: str = "whole_degree",
    rounding_mode: str = "nearest",
    limit: int = 96,
    timeout_seconds: float = 12.0,
    http_get_json: JsonGetter | None = None,
) -> dict[str, Any]:
    fetch_kwargs: dict[str, Any] = {
        "station_id": station_id,
        "limit": int(limit),
        "timeout_seconds": float(timeout_seconds),
    }
    if http_get_json is not None:
        fetch_kwargs["http_get_json"] = http_get_json

    observations_payload = fetch_nws_station_recent_observations(**fetch_kwargs)
    if str(observations_payload.get("status") or "") != "ready":
        return {
            "status": "observations_unavailable",
            "station_id": str(station_id or "").strip().upper(),
            "target_date_local": str(target_date_local or ""),
            "timezone_name": str(timezone_name or ""),
            "settlement_unit": str(settlement_unit or ""),
            "settlement_precision": str(settlement_precision or ""),
            "error": str(observations_payload.get("error") or "NWS observations unavailable."),
            "upstream": observations_payload,
        }

    try:
        target_date = datetime.fromisoformat(str(target_date_local)).date()
    except ValueError:
        return {
            "status": "invalid_target_date",
            "station_id": str(station_id or "").strip().upper(),
            "target_date_local": str(target_date_local or ""),
            "error": "target_date_local must be YYYY-MM-DD.",
        }

    try:
        zone = ZoneInfo(str(timezone_name or "UTC"))
    except Exception:
        return {
            "status": "invalid_timezone",
            "station_id": str(station_id or "").strip().upper(),
            "target_date_local": str(target_date_local or ""),
            "timezone_name": str(timezone_name or ""),
            "error": "Invalid timezone name.",
        }

    station_observations = observations_payload.get("observations")
    if not isinstance(station_observations, list):
        station_observations = []

    observations_for_date: list[dict[str, Any]] = []
    all_errors: list[str] = []
    for index, observation in enumerate(station_observations):
        if not isinstance(observation, dict):
            continue
        timestamp = _parse_iso_datetime(str(observation.get("timestamp") or ""))
        if timestamp is None:
            all_errors.append(f"invalid_timestamp_at_index_{index}")
            continue

        local_timestamp = timestamp.astimezone(zone)
        if local_timestamp.date() != target_date:
            continue

        temperature_c = observation.get("temperature_c")
        if not isinstance(temperature_c, (float, int)):
            continue

        temp_c = float(temperature_c)
        temp_f = celsius_to_fahrenheit(temp_c)
        observations_for_date.append(
            {
                "timestamp": str(observation.get("timestamp") or ""),
                "local_timestamp": local_timestamp.isoformat(),
                "temperature_c": round(temp_c, 3),
                "temperature_f": round(temp_f, 3),
            }
        )

    max_temp_c = max((item["temperature_c"] for item in observations_for_date), default=None)
    max_temp_f = max((item["temperature_f"] for item in observations_for_date), default=None)

    settlement_unit_key = str(settlement_unit or "celsius").strip().lower()
    settlement_raw = None
    if settlement_unit_key == "fahrenheit":
        settlement_raw = max_temp_f
    else:
        settlement_unit_key = "celsius"
        settlement_raw = max_temp_c

    settlement_quantized = None
    if isinstance(settlement_raw, (float, int)):
        settlement_quantized = quantize_temperature(
            float(settlement_raw),
            precision=settlement_precision,
            rounding_mode=rounding_mode,
        )

    return {
        "status": "ready",
        "station_id": str(station_id or "").strip().upper(),
        "target_date_local": target_date.isoformat(),
        "timezone_name": timezone_name,
        "settlement_unit": settlement_unit_key,
        "settlement_precision": settlement_precision,
        "rounding_mode": rounding_mode,
        "observations_total": len(station_observations),
        "observations_for_date": len(observations_for_date),
        "observations": observations_for_date,
        "max_temperature_c": max_temp_c,
        "max_temperature_f": max_temp_f,
        "max_temperature_settlement_raw": settlement_raw,
        "max_temperature_settlement_quantized": settlement_quantized,
        "parse_warnings": all_errors,
    }


def classify_temperature_outcomes(
    *,
    candidate_values: list[int],
    observed_max_value: int | float | None,
    forecast_upper_bound: int | float | None = None,
) -> dict[str, Any]:
    deduped_candidates = sorted({int(value) for value in candidate_values})
    if not deduped_candidates:
        return {
            "status": "invalid_candidates",
            "error": "candidate_values must contain at least one value.",
        }

    lower_bound = int(observed_max_value) if observed_max_value is not None else None
    upper_bound = int(forecast_upper_bound) if forecast_upper_bound is not None else None

    impossible: list[int] = []
    feasible: list[int] = []
    for value in deduped_candidates:
        if lower_bound is not None and value < lower_bound:
            impossible.append(value)
            continue
        if upper_bound is not None and value > upper_bound:
            impossible.append(value)
            continue
        feasible.append(value)

    locked: list[int] = []
    if lower_bound is not None and upper_bound is not None and lower_bound == upper_bound:
        if lower_bound in feasible:
            locked = [lower_bound]

    return {
        "status": "ready",
        "observed_lower_bound": lower_bound,
        "forecast_upper_bound": upper_bound,
        "candidate_values": deduped_candidates,
        "impossible_values": impossible,
        "feasible_values": feasible,
        "locked_values": locked,
    }
