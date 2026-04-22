from __future__ import annotations

from datetime import datetime, timezone
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


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _local_standard_date(timestamp: datetime, zone: ZoneInfo) -> str:
    local_timestamp = timestamp.astimezone(zone)
    dst_delta = local_timestamp.dst()
    if dst_delta and dst_delta.total_seconds() > 0:
        local_timestamp = local_timestamp - dst_delta
    return local_timestamp.date().isoformat()


def _build_metar_state_snapshot(
    *,
    station_id: str,
    target_date_local: str,
    timezone_name: str,
    zone: ZoneInfo,
    settlement_unit: str,
    settlement_precision: str,
    rounding_mode: str,
    metar_state: dict[str, Any],
) -> dict[str, Any] | None:
    latest_by_station = metar_state.get("latest_observation_by_station")
    max_by_station_day = metar_state.get("max_temp_c_by_station_local_day")
    min_by_station_day = metar_state.get("min_temp_c_by_station_local_day")
    if not isinstance(latest_by_station, dict) or not isinstance(max_by_station_day, dict):
        return None
    if not isinstance(min_by_station_day, dict):
        min_by_station_day = {}

    station_key = str(station_id or "").strip().upper()
    target_key = f"{station_key}|{target_date_local}"
    max_temp_c = _float_or_none(max_by_station_day.get(target_key))
    max_temp_f = celsius_to_fahrenheit(max_temp_c) if max_temp_c is not None else None
    min_temp_c = _float_or_none(min_by_station_day.get(target_key))
    min_temp_f = celsius_to_fahrenheit(min_temp_c) if min_temp_c is not None else None

    settlement_unit_key = str(settlement_unit or "celsius").strip().lower()
    max_settlement_raw = max_temp_f if settlement_unit_key == "fahrenheit" else max_temp_c
    min_settlement_raw = min_temp_f if settlement_unit_key == "fahrenheit" else min_temp_c
    if settlement_unit_key != "fahrenheit":
        settlement_unit_key = "celsius"

    max_settlement_quantized = None
    if max_settlement_raw is not None:
        max_settlement_quantized = quantize_temperature(
            max_settlement_raw,
            precision=settlement_precision,
            rounding_mode=rounding_mode,
        )
    min_settlement_quantized = None
    if min_settlement_raw is not None:
        min_settlement_quantized = quantize_temperature(
            min_settlement_raw,
            precision=settlement_precision,
            rounding_mode=rounding_mode,
        )

    latest_station_observation = latest_by_station.get(station_key)
    latest_observation_utc = ""
    if isinstance(latest_station_observation, dict):
        latest_observation_utc = str(latest_station_observation.get("observation_time_utc") or "")

    observations: list[dict[str, Any]] = []
    if max_temp_c is not None:
        local_timestamp = None
        parsed = _parse_iso_datetime(latest_observation_utc)
        if parsed is not None:
            local_timestamp = parsed.astimezone(zone).isoformat()
        observations.append(
            {
                "timestamp": latest_observation_utc,
                "local_timestamp": local_timestamp or "",
                "temperature_c": round(max_temp_c, 3),
                "temperature_f": round(max_temp_f or celsius_to_fahrenheit(max_temp_c), 3),
            }
        )

    return {
        "status": "ready",
        "station_id": station_key,
        "target_date_local": target_date_local,
        "timezone_name": timezone_name,
        "settlement_unit": settlement_unit_key,
        "settlement_precision": settlement_precision,
        "rounding_mode": rounding_mode,
        "observations_total": 1 if latest_observation_utc else 0,
        "observations_for_date": 1 if max_temp_c is not None else 0,
        "observations": observations,
        "max_temperature_c": max_temp_c,
        "max_temperature_f": max_temp_f,
        "max_temperature_settlement_raw": max_settlement_raw,
        "max_temperature_settlement_quantized": max_settlement_quantized,
        "min_temperature_c": min_temp_c,
        "min_temperature_f": min_temp_f,
        "min_temperature_settlement_raw": min_settlement_raw,
        "min_temperature_settlement_quantized": min_settlement_quantized,
        "parse_warnings": [],
        "snapshot_source": "metar_state",
        "latest_observation_time_utc": latest_observation_utc,
    }


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
    metar_state: dict[str, Any] | None = None,
    allow_nws_fallback: bool = True,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    station_normalized = str(station_id or "").strip().upper()
    target_date_text = str(target_date_local or "")
    timezone_text = str(timezone_name or "")

    try:
        target_date = datetime.fromisoformat(target_date_text).date()
    except ValueError:
        return {
            "status": "invalid_target_date",
            "station_id": station_normalized,
            "target_date_local": target_date_text,
            "error": "target_date_local must be YYYY-MM-DD.",
        }

    try:
        zone = ZoneInfo(timezone_text or "UTC")
    except Exception:
        return {
            "status": "invalid_timezone",
            "station_id": station_normalized,
            "target_date_local": target_date.isoformat(),
            "timezone_name": timezone_text,
            "error": "Invalid timezone name.",
        }

    reference_now_utc = now_utc
    if reference_now_utc is None:
        reference_now_utc = datetime.now(timezone.utc)
    elif reference_now_utc.tzinfo is None:
        reference_now_utc = reference_now_utc.replace(tzinfo=timezone.utc)
    else:
        reference_now_utc = reference_now_utc.astimezone(timezone.utc)

    if isinstance(metar_state, dict):
        metar_snapshot = _build_metar_state_snapshot(
            station_id=station_normalized,
            target_date_local=target_date.isoformat(),
            timezone_name=timezone_text,
            zone=zone,
            settlement_unit=settlement_unit,
            settlement_precision=settlement_precision,
            rounding_mode=rounding_mode,
            metar_state=metar_state,
        )
        if metar_snapshot is not None:
            has_observations = int(metar_snapshot.get("observations_for_date") or 0) > 0
            if has_observations or not allow_nws_fallback:
                return metar_snapshot

    fetch_kwargs: dict[str, Any] = {
        "station_id": station_normalized,
        "limit": int(limit),
        "timeout_seconds": float(timeout_seconds),
    }
    if http_get_json is not None:
        fetch_kwargs["http_get_json"] = http_get_json

    observations_payload = fetch_nws_station_recent_observations(**fetch_kwargs)
    if str(observations_payload.get("status") or "") != "ready":
        return {
            "status": "observations_unavailable",
            "station_id": station_normalized,
            "target_date_local": target_date.isoformat(),
            "timezone_name": timezone_text,
            "settlement_unit": str(settlement_unit or ""),
            "settlement_precision": str(settlement_precision or ""),
            "error": str(observations_payload.get("error") or "NWS observations unavailable."),
            "upstream": observations_payload,
        }

    station_observations = observations_payload.get("observations")
    if not isinstance(station_observations, list):
        station_observations = []

    observations_for_date_raw: list[tuple[datetime, dict[str, Any]]] = []
    all_errors: list[str] = []
    for index, observation in enumerate(station_observations):
        if not isinstance(observation, dict):
            continue
        timestamp = _parse_iso_datetime(str(observation.get("timestamp") or ""))
        if timestamp is None:
            all_errors.append(f"invalid_timestamp_at_index_{index}")
            continue
        if timestamp.astimezone(timezone.utc) > reference_now_utc:
            all_errors.append(f"future_timestamp_at_index_{index}")
            continue

        local_standard_date = _local_standard_date(timestamp, zone)
        if local_standard_date != target_date.isoformat():
            continue
        local_timestamp = timestamp.astimezone(zone)

        temperature_c = observation.get("temperature_c")
        if isinstance(temperature_c, bool):
            all_errors.append(f"invalid_temperature_c_at_index_{index}")
            continue

        temp_c = _float_or_none(temperature_c)
        if temp_c is None:
            all_errors.append(f"invalid_temperature_c_at_index_{index}")
            continue
        temp_f = celsius_to_fahrenheit(temp_c)
        observations_for_date_raw.append(
            (
                timestamp,
                {
                    "timestamp": str(observation.get("timestamp") or ""),
                    "local_timestamp": local_timestamp.isoformat(),
                    "temperature_c": round(temp_c, 3),
                    "temperature_f": round(temp_f, 3),
                },
            )
        )

    observations_for_date_raw.sort(key=lambda item: item[0])
    observations_for_date = [item[1] for item in observations_for_date_raw]

    max_temp_c = max((item["temperature_c"] for item in observations_for_date), default=None)
    max_temp_f = max((item["temperature_f"] for item in observations_for_date), default=None)
    min_temp_c = min((item["temperature_c"] for item in observations_for_date), default=None)
    min_temp_f = min((item["temperature_f"] for item in observations_for_date), default=None)

    settlement_unit_key = str(settlement_unit or "celsius").strip().lower()
    max_settlement_raw = None
    min_settlement_raw = None
    if settlement_unit_key == "fahrenheit":
        max_settlement_raw = max_temp_f
        min_settlement_raw = min_temp_f
    else:
        settlement_unit_key = "celsius"
        max_settlement_raw = max_temp_c
        min_settlement_raw = min_temp_c

    max_settlement_quantized = None
    if isinstance(max_settlement_raw, (float, int)):
        max_settlement_quantized = quantize_temperature(
            float(max_settlement_raw),
            precision=settlement_precision,
            rounding_mode=rounding_mode,
        )
    min_settlement_quantized = None
    if isinstance(min_settlement_raw, (float, int)):
        min_settlement_quantized = quantize_temperature(
            float(min_settlement_raw),
            precision=settlement_precision,
            rounding_mode=rounding_mode,
        )

    return {
        "status": "ready",
        "station_id": station_normalized,
        "target_date_local": target_date.isoformat(),
        "timezone_name": timezone_text,
        "settlement_unit": settlement_unit_key,
        "settlement_precision": settlement_precision,
        "rounding_mode": rounding_mode,
        "observations_total": len(station_observations),
        "observations_for_date": len(observations_for_date),
        "observations": observations_for_date,
        "max_temperature_c": max_temp_c,
        "max_temperature_f": max_temp_f,
        "max_temperature_settlement_raw": max_settlement_raw,
        "max_temperature_settlement_quantized": max_settlement_quantized,
        "min_temperature_c": min_temp_c,
        "min_temperature_f": min_temp_f,
        "min_temperature_settlement_raw": min_settlement_raw,
        "min_temperature_settlement_quantized": min_settlement_quantized,
        "parse_warnings": all_errors,
        "snapshot_source": "nws_station_observations",
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
