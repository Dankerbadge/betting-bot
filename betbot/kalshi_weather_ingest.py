from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from betbot.dns_guard import urlopen_with_dns_recovery

JsonGetter = Callable[[str, float], tuple[int, dict[str, Any] | list[Any] | Any]]
JsonGetterWithHeaders = Callable[
    [str, float, dict[str, str] | None],
    tuple[int, dict[str, Any] | list[Any] | Any],
]


_NCEI_CDO_STATION_BY_ICAO = {
    "KJFK": "GHCND:USW00094789",
    "KBOS": "GHCND:USW00014739",
    "KDCA": "GHCND:USW00013743",
    "KORD": "GHCND:USW00094846",
    "KDFW": "GHCND:USW00003927",
    "KDEN": "GHCND:USW00003017",
    "KPHX": "GHCND:USW00023183",
    "KLAX": "GHCND:USW00023174",
    "KSFO": "GHCND:USW00023234",
    "KSEA": "GHCND:USW00024233",
    "KMIA": "GHCND:USW00012839",
    "KPHL": "GHCND:USW00013739",
    "KATL": "GHCND:USW00013874",
    "KMSP": "GHCND:USW00014922",
    "KDTW": "GHCND:USW00014822",
    "KIAH": "GHCND:USW00012960",
    "KBWI": "GHCND:USW00093721",
    "KLAS": "GHCND:USW00023169",
    "KSLC": "GHCND:USW00024127",
    "KPDX": "GHCND:USW00024229",
}


def _resolve_cdo_station_id(station_id: str) -> str:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return ""

    override_text = str(os.getenv("BETBOT_WEATHER_CDO_STATION_MAP") or "").strip()
    if override_text:
        for chunk in override_text.replace(";", ",").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            key, value = item.split("=", 1)
            if key.strip().upper() == clean_station_id:
                override_value = value.strip()
                if override_value:
                    return override_value

    return _NCEI_CDO_STATION_BY_ICAO.get(clean_station_id, "")


def _normalize_cdo_temperature_f(value: Any) -> float | None:
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(raw):
        return None
    if raw > 140.0 or raw < -100.0:
        celsius = raw / 10.0
        return round((celsius * 9.0 / 5.0) + 32.0, 3)
    return round(raw, 3)


def _normalize_cdo_precip_in(value: Any) -> float | None:
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(raw):
        return None
    if raw < 0.0:
        return 0.0
    if raw > 10.0:
        return round(raw / 254.0, 4)
    return round(raw, 4)


def _parse_iso_datetime(value: Any) -> datetime | None:
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


def _status_is_cacheable(status: str) -> bool:
    return status in {"ready", "ready_partial", "no_history"}


def _load_cdo_cache_entry(path: Path) -> tuple[dict[str, Any] | None, datetime | None]:
    if not path.exists():
        return (None, None)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (None, None)
    if not isinstance(payload, dict):
        return (None, None)
    cached_at = _parse_iso_datetime(payload.get("cached_at"))
    cached_payload = payload.get("payload")
    if not isinstance(cached_payload, dict):
        return (None, cached_at)
    return (cached_payload, cached_at)


def _write_cdo_cache_entry(path: Path, payload: dict[str, Any], now: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache_wrapper = {
        "cached_at": now.astimezone(timezone.utc).isoformat(),
        "payload": payload,
    }
    path.write_text(json.dumps(cache_wrapper, indent=2), encoding="utf-8")


def _http_get_json(url: str, timeout_seconds: float) -> tuple[int, dict[str, Any] | list[Any] | Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/geo+json, application/json;q=0.9, */*;q=0.8",
            "User-Agent": "betbot-weather-ingest/1.0 (research)",
        },
        method="GET",
    )
    with urlopen_with_dns_recovery(
        request,
        timeout_seconds=max(1.0, float(timeout_seconds)),
        urlopen_fn=urlopen,
    ) as response:
        status = int(getattr(response, "status", 200) or 200)
        payload = response.read().decode("utf-8", errors="replace")
    try:
        return status, json.loads(payload)
    except json.JSONDecodeError:
        return status, {"raw_text": payload}


def _http_get_json_with_headers(
    url: str,
    timeout_seconds: float,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any] | list[Any] | Any]:
    headers = {
        "Accept": "application/json, */*;q=0.8",
        "User-Agent": "betbot-weather-ingest/1.0 (research)",
    }
    for key, value in (extra_headers or {}).items():
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if key_text and value_text:
            headers[key_text] = value_text
    request = Request(url, headers=headers, method="GET")
    with urlopen_with_dns_recovery(
        request,
        timeout_seconds=max(1.0, float(timeout_seconds)),
        urlopen_fn=urlopen,
    ) as response:
        status = int(getattr(response, "status", 200) or 200)
        payload = response.read().decode("utf-8", errors="replace")
    try:
        return status, json.loads(payload)
    except json.JSONDecodeError:
        return status, {"raw_text": payload}


def fetch_nws_station_hourly_forecast(
    *,
    station_id: str,
    timeout_seconds: float = 12.0,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return {
            "status": "invalid_station",
            "station_id": "",
            "error": "Missing station identifier.",
        }

    station_url = f"https://api.weather.gov/stations/{clean_station_id}"
    status_station, payload_station = http_get_json(station_url, timeout_seconds)
    if status_station != 200 or not isinstance(payload_station, dict):
        return {
            "status": "station_unavailable",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "error": f"Station metadata request failed for {clean_station_id}.",
        }

    station_properties = payload_station.get("properties") if isinstance(payload_station, dict) else None
    station_geometry = payload_station.get("geometry") if isinstance(payload_station, dict) else None
    station_timezone = (
        str((station_properties or {}).get("timeZone") or "").strip()
        if isinstance(station_properties, dict)
        else ""
    )
    coordinates = station_geometry.get("coordinates") if isinstance(station_geometry, dict) else None
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return {
            "status": "station_missing_coordinates",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "error": f"Station metadata for {clean_station_id} is missing coordinates.",
        }
    lon = coordinates[0]
    lat = coordinates[1]
    try:
        lat_value = float(lat)
        lon_value = float(lon)
    except (TypeError, ValueError):
        return {
            "status": "station_invalid_coordinates",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "error": f"Station metadata for {clean_station_id} has invalid coordinates.",
        }

    points_url = f"https://api.weather.gov/points/{lat_value:.4f},{lon_value:.4f}"
    status_points, payload_points = http_get_json(points_url, timeout_seconds)
    if status_points != 200 or not isinstance(payload_points, dict):
        return {
            "status": "points_unavailable",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "http_status_points": status_points,
            "station_timezone": station_timezone or "",
            "latitude": lat_value,
            "longitude": lon_value,
            "error": f"NWS points metadata request failed for station {clean_station_id}.",
        }

    points_properties = payload_points.get("properties") if isinstance(payload_points, dict) else None
    forecast_hourly_url = (
        str((points_properties or {}).get("forecastHourly") or "").strip()
        if isinstance(points_properties, dict)
        else ""
    )
    if not forecast_hourly_url:
        return {
            "status": "forecast_hourly_missing",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "http_status_points": status_points,
            "station_timezone": station_timezone or "",
            "latitude": lat_value,
            "longitude": lon_value,
            "error": f"NWS points metadata did not include forecastHourly for station {clean_station_id}.",
        }

    status_forecast, payload_forecast = http_get_json(forecast_hourly_url, timeout_seconds)
    if status_forecast != 200 or not isinstance(payload_forecast, dict):
        return {
            "status": "forecast_unavailable",
            "station_id": clean_station_id,
            "http_status_station": status_station,
            "http_status_points": status_points,
            "http_status_forecast": status_forecast,
            "forecast_hourly_url": forecast_hourly_url,
            "station_timezone": station_timezone or "",
            "latitude": lat_value,
            "longitude": lon_value,
            "error": f"NWS hourly forecast request failed for station {clean_station_id}.",
        }

    forecast_properties = payload_forecast.get("properties") if isinstance(payload_forecast, dict) else None
    periods = forecast_properties.get("periods") if isinstance(forecast_properties, dict) else None
    if not isinstance(periods, list):
        periods = []
    updated = ""
    generated_at = ""
    if isinstance(forecast_properties, dict):
        updated = str(forecast_properties.get("updateTime") or "").strip()
        generated_at = str(forecast_properties.get("generatedAt") or "").strip()

    return {
        "status": "ready",
        "station_id": clean_station_id,
        "station_timezone": station_timezone or "",
        "latitude": lat_value,
        "longitude": lon_value,
        "forecast_hourly_url": forecast_hourly_url,
        "forecast_updated_at": updated or generated_at,
        "periods": [period for period in periods if isinstance(period, dict)],
        "http_status_station": status_station,
        "http_status_points": status_points,
        "http_status_forecast": status_forecast,
    }


def fetch_noaa_global_land_ocean_anomaly_series(
    *,
    timeout_seconds: float = 12.0,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    url = "https://storage.googleapis.com/noaa-ncei-ipg/datasets/cag/data/time-series/global/tavg/anomaly_globe-land_ocean.json"
    status, payload = http_get_json(url, timeout_seconds)
    if status != 200 or not isinstance(payload, list):
        return {
            "status": "noaa_series_unavailable",
            "http_status": status,
            "series_url": url,
            "error": "Failed to load NOAA global land_ocean anomaly series.",
        }

    values: list[float] = []
    for item in payload:
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            continue
    if not values:
        return {
            "status": "noaa_series_empty",
            "http_status": status,
            "series_url": url,
            "error": "NOAA global anomaly series returned no numeric values.",
        }

    start_year = 1850
    start_month = 1
    last_index = len(values) - 1
    end_year = start_year + (last_index // 12)
    end_month = (last_index % 12) + 1
    now_utc = datetime.now(timezone.utc)
    cache_hint = f"{start_year:04d}-{start_month:02d}..{end_year:04d}-{end_month:02d}@{now_utc.isoformat()}"
    return {
        "status": "ready",
        "series_url": url,
        "start_year": start_year,
        "start_month": start_month,
        "end_year": end_year,
        "end_month": end_month,
        "values": values,
        "cache_hint": cache_hint,
        "http_status": status,
    }


def fetch_ncei_cdo_station_daily_history(
    *,
    station_id: str,
    month: int,
    day: int,
    lookback_years: int = 15,
    timeout_seconds: float = 12.0,
    cdo_token: str | None = None,
    cache_dir: str | None = None,
    cache_max_age_hours: float = 24.0,
    now: datetime | None = None,
    http_get_json_with_headers: JsonGetterWithHeaders = _http_get_json_with_headers,
) -> dict[str, Any]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return {
            "status": "invalid_station",
            "station_id": "",
            "error": "Missing station identifier.",
        }

    try:
        month_value = int(month)
        day_value = int(day)
    except (TypeError, ValueError):
        return {
            "status": "invalid_target_day",
            "station_id": clean_station_id,
            "error": "Month/day target is invalid.",
        }
    if month_value < 1 or month_value > 12 or day_value < 1 or day_value > 31:
        return {
            "status": "invalid_target_day",
            "station_id": clean_station_id,
            "error": "Month/day target is out of range.",
        }

    current_time = now or datetime.now(timezone.utc)
    lookback_years_clamped = max(3, min(25, int(lookback_years)))
    end_year = current_time.year - 1
    start_year = end_year - lookback_years_clamped + 1
    cache_ttl_seconds = max(0.0, float(cache_max_age_hours)) * 3600.0

    cache_file: Path | None = None
    cache_payload: dict[str, Any] | None = None
    cache_age_seconds: float | None = None
    if cache_dir:
        cache_key = f"{clean_station_id}_{month_value:02d}_{day_value:02d}_{lookback_years_clamped}_{end_year}.json"
        cache_file = Path(cache_dir) / cache_key
        cache_payload, cached_at = _load_cdo_cache_entry(cache_file)
        if cache_payload is not None and cached_at is not None:
            cache_age_seconds = max(
                0.0,
                (current_time.astimezone(timezone.utc) - cached_at.astimezone(timezone.utc)).total_seconds(),
            )
            cached_status = str(cache_payload.get("status") or "").strip().lower()
            if _status_is_cacheable(cached_status) and cache_age_seconds <= cache_ttl_seconds:
                cached_result = dict(cache_payload)
                cached_result["cache_hit"] = True
                cached_result["cache_fallback_used"] = False
                cached_result["cache_fresh"] = True
                cached_result["cache_age_seconds"] = round(cache_age_seconds, 3)
                return cached_result

    token = str(
        cdo_token
        or os.getenv("BETBOT_NOAA_CDO_TOKEN")
        or os.getenv("NOAA_CDO_TOKEN")
        or os.getenv("NCEI_CDO_TOKEN")
        or ""
    ).strip()
    if not token:
        if cache_payload is not None:
            cached_status = str(cache_payload.get("status") or "").strip().lower()
            if _status_is_cacheable(cached_status):
                stale_result = dict(cache_payload)
                stale_result["cache_hit"] = True
                stale_result["cache_fallback_used"] = True
                stale_result["cache_fresh"] = False
                stale_result["cache_age_seconds"] = round(float(cache_age_seconds or 0.0), 3)
                stale_result["cache_warning"] = "Using cached station history because NOAA CDO token is missing."
                return stale_result
        return {
            "status": "disabled_missing_token",
            "station_id": clean_station_id,
            "error": "Missing NOAA/NCEI CDO API token.",
            "cache_hit": False,
            "cache_fallback_used": False,
        }

    cdo_station_id = _resolve_cdo_station_id(clean_station_id)
    if not cdo_station_id:
        return {
            "status": "station_mapping_missing",
            "station_id": clean_station_id,
            "error": "No CDO station mapping is configured for this settlement station.",
            "cache_hit": False,
            "cache_fallback_used": False,
        }

    source_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    request_headers = {"token": token}

    daily_samples: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    request_count = 0

    for year in range(start_year, end_year + 1):
        date_stamp = f"{year:04d}-{month_value:02d}-{day_value:02d}"
        params = [
            ("datasetid", "GHCND"),
            ("stationid", cdo_station_id),
            ("startdate", date_stamp),
            ("enddate", date_stamp),
            ("units", "standard"),
            ("limit", "1000"),
            ("datatypeid", "TMAX"),
            ("datatypeid", "TMIN"),
            ("datatypeid", "PRCP"),
        ]
        url = f"{source_url}?{urlencode(params, doseq=True)}"
        request_count += 1
        status, payload = http_get_json_with_headers(url, timeout_seconds, request_headers)
        if status == 429:
            if cache_payload is not None:
                cached_status = str(cache_payload.get("status") or "").strip().lower()
                if _status_is_cacheable(cached_status):
                    stale_result = dict(cache_payload)
                    stale_result["cache_hit"] = True
                    stale_result["cache_fallback_used"] = True
                    stale_result["cache_fresh"] = False
                    stale_result["cache_age_seconds"] = round(float(cache_age_seconds or 0.0), 3)
                    stale_result["cache_warning"] = "Using cached station history because CDO returned HTTP 429."
                    return stale_result
            return {
                "status": "rate_limited",
                "station_id": clean_station_id,
                "cdo_station_id": cdo_station_id,
                "month": month_value,
                "day": day_value,
                "lookback_years": lookback_years_clamped,
                "sample_years": len(daily_samples),
                "request_count": request_count,
                "source_url": source_url,
                "error": f"CDO rate-limited request for {date_stamp}.",
                "cache_hit": False,
                "cache_fallback_used": False,
            }
        if status != 200 or not isinstance(payload, dict):
            errors.append(
                {
                    "year": year,
                    "http_status": status,
                    "error": f"Failed to fetch CDO sample for {date_stamp}.",
                }
            )
            continue

        results = payload.get("results")
        if not isinstance(results, list) or not results:
            continue

        sample: dict[str, Any] = {"year": year, "date": date_stamp}
        for item in results:
            if not isinstance(item, dict):
                continue
            datatype = str(item.get("datatype") or "").strip().upper()
            value = item.get("value")
            if datatype == "TMAX":
                normalized = _normalize_cdo_temperature_f(value)
                if normalized is not None:
                    sample["tmax_f"] = normalized
            elif datatype == "TMIN":
                normalized = _normalize_cdo_temperature_f(value)
                if normalized is not None:
                    sample["tmin_f"] = normalized
            elif datatype == "PRCP":
                normalized = _normalize_cdo_precip_in(value)
                if normalized is not None:
                    sample["prcp_in"] = normalized
        if len(sample) > 2:
            daily_samples.append(sample)

    tmax_values = [float(sample["tmax_f"]) for sample in daily_samples if isinstance(sample.get("tmax_f"), (int, float))]
    tmin_values = [float(sample["tmin_f"]) for sample in daily_samples if isinstance(sample.get("tmin_f"), (int, float))]
    prcp_values = [float(sample["prcp_in"]) for sample in daily_samples if isinstance(sample.get("prcp_in"), (int, float))]
    daily_mean_values: list[float] = []
    for sample in daily_samples:
        tmax = sample.get("tmax_f")
        tmin = sample.get("tmin_f")
        if isinstance(tmax, (int, float)) and isinstance(tmin, (int, float)):
            daily_mean_values.append(round((float(tmax) + float(tmin)) / 2.0, 3))

    if not daily_samples:
        result = {
            "status": "no_history",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "month": month_value,
            "day": day_value,
            "lookback_years": lookback_years_clamped,
            "request_count": request_count,
            "source_url": source_url,
            "errors": errors,
            "error": "No historical station samples were returned for this month/day target.",
            "cache_hit": False,
            "cache_fallback_used": False,
        }
        if cache_file is not None:
            try:
                _write_cdo_cache_entry(cache_file, result, current_time)
            except OSError:
                pass
        return result

    rain_day_frequency = None
    if prcp_values:
        rain_day_frequency = round(
            sum(1 for value in prcp_values if value >= 0.01) / float(len(prcp_values)),
            6,
        )

    result = {
        "status": "ready" if not errors else "ready_partial",
        "station_id": clean_station_id,
        "cdo_station_id": cdo_station_id,
        "month": month_value,
        "day": day_value,
        "lookback_years": lookback_years_clamped,
        "sample_years": len(daily_samples),
        "daily_samples": daily_samples,
        "tmax_values_f": tmax_values,
        "tmin_values_f": tmin_values,
        "daily_mean_values_f": daily_mean_values,
        "prcp_values_in": prcp_values,
        "rain_day_frequency": rain_day_frequency,
        "request_count": request_count,
        "source_url": source_url,
        "errors": errors,
        "cache_hit": False,
        "cache_fallback_used": False,
    }
    if cache_file is not None:
        try:
            _write_cdo_cache_entry(cache_file, result, current_time)
        except OSError:
            pass
    return result
