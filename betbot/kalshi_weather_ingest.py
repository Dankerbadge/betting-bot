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


def _extract_station_daily_samples_from_rows(
    *,
    rows: list[Any],
    month: int,
    day: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    samples_by_year: dict[int, dict[str, Any]] = {}
    parse_errors: list[dict[str, Any]] = []

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        date_text = str(row.get("DATE") or row.get("date") or "").strip()
        if not date_text:
            continue
        date_value = _parse_iso_datetime(date_text)
        if date_value is None:
            try:
                date_value = datetime.fromisoformat(date_text[:10]).replace(tzinfo=timezone.utc)
            except ValueError:
                parse_errors.append({"index": index, "error": f"invalid_date:{date_text}"})
                continue
        if date_value.month != int(month) or date_value.day != int(day):
            continue
        year = int(date_value.year)
        sample = samples_by_year.setdefault(
            year,
            {
                "year": year,
                "date": f"{year:04d}-{int(month):02d}-{int(day):02d}",
            },
        )

        tmax_value = row.get("TMAX") if "TMAX" in row else row.get("tmax")
        tmin_value = row.get("TMIN") if "TMIN" in row else row.get("tmin")
        prcp_value = row.get("PRCP") if "PRCP" in row else row.get("prcp")
        tmax_normalized = _normalize_cdo_temperature_f(tmax_value)
        tmin_normalized = _normalize_cdo_temperature_f(tmin_value)
        prcp_normalized = _normalize_cdo_precip_in(prcp_value)
        if tmax_normalized is not None:
            sample["tmax_f"] = tmax_normalized
        if tmin_normalized is not None:
            sample["tmin_f"] = tmin_normalized
        if prcp_normalized is not None:
            sample["prcp_in"] = prcp_normalized

    daily_samples = [
        sample
        for _, sample in sorted(samples_by_year.items(), key=lambda item: item[0])
        if len(sample) > 2
    ]
    return daily_samples, parse_errors


def _build_station_daily_history_result(
    *,
    status: str,
    station_id: str,
    cdo_station_id: str,
    month: int,
    day: int,
    lookback_years: int,
    request_count: int,
    source_url: str,
    errors: list[dict[str, Any]],
    daily_samples: list[dict[str, Any]],
    cache_hit: bool,
    cache_fallback_used: bool,
    error_message: str | None = None,
    data_source: str | None = None,
) -> dict[str, Any]:
    tmax_values = [float(sample["tmax_f"]) for sample in daily_samples if isinstance(sample.get("tmax_f"), (int, float))]
    tmin_values = [float(sample["tmin_f"]) for sample in daily_samples if isinstance(sample.get("tmin_f"), (int, float))]
    prcp_values = [float(sample["prcp_in"]) for sample in daily_samples if isinstance(sample.get("prcp_in"), (int, float))]
    daily_mean_values: list[float] = []
    for sample in daily_samples:
        tmax = sample.get("tmax_f")
        tmin = sample.get("tmin_f")
        if isinstance(tmax, (int, float)) and isinstance(tmin, (int, float)):
            daily_mean_values.append(round((float(tmax) + float(tmin)) / 2.0, 3))

    rain_day_frequency = None
    if prcp_values:
        rain_day_frequency = round(
            sum(1 for value in prcp_values if value >= 0.01) / float(len(prcp_values)),
            6,
        )

    payload = {
        "status": status,
        "station_id": station_id,
        "cdo_station_id": cdo_station_id,
        "month": int(month),
        "day": int(day),
        "lookback_years": int(lookback_years),
        "sample_years": len(daily_samples),
        "sample_years_precip": len(prcp_values),
        "sample_years_tmax": len(tmax_values),
        "sample_years_tmin": len(tmin_values),
        "sample_years_mean": len(daily_mean_values),
        "daily_samples": daily_samples,
        "tmax_values_f": tmax_values,
        "tmin_values_f": tmin_values,
        "daily_mean_values_f": daily_mean_values,
        "prcp_values_in": prcp_values,
        "rain_day_frequency": rain_day_frequency,
        "request_count": int(request_count),
        "source_url": source_url,
        "errors": errors,
        "cache_hit": bool(cache_hit),
        "cache_fallback_used": bool(cache_fallback_used),
    }
    if data_source:
        payload["data_source"] = str(data_source)
    if error_message:
        payload["error"] = error_message
    return payload


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
    enable_access_data_service_fallback: bool = True,
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

    cdo_station_id = _resolve_cdo_station_id(clean_station_id)
    if not cdo_station_id:
        return {
            "status": "station_mapping_missing",
            "station_id": clean_station_id,
            "error": "No CDO station mapping is configured for this settlement station.",
            "cache_hit": False,
            "cache_fallback_used": False,
        }

    token = str(
        cdo_token
        or os.getenv("BETBOT_NOAA_CDO_TOKEN")
        or os.getenv("NOAA_CDO_TOKEN")
        or os.getenv("NCEI_CDO_TOKEN")
        or ""
    ).strip()
    if not token and enable_access_data_service_fallback:
        ads_source_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        station_for_ads = cdo_station_id.split(":", 1)[-1].strip() or cdo_station_id
        ads_params = [
            ("dataset", "daily-summaries"),
            ("stations", station_for_ads),
            ("startDate", f"{start_year:04d}-{month_value:02d}-{day_value:02d}"),
            ("endDate", f"{end_year:04d}-{month_value:02d}-{day_value:02d}"),
            ("dataTypes", "TMAX,TMIN,PRCP"),
            ("format", "json"),
            ("units", "standard"),
            ("includeAttributes", "false"),
            ("includeStationName", "false"),
            ("includeStationLocation", "false"),
        ]
        ads_url = f"{ads_source_url}?{urlencode(ads_params, doseq=True)}"
        ads_status, ads_payload = http_get_json_with_headers(ads_url, timeout_seconds, None)
        if ads_status == 200:
            ads_rows: list[Any] | None = None
            if isinstance(ads_payload, list):
                ads_rows = ads_payload
            elif isinstance(ads_payload, dict):
                results = ads_payload.get("results")
                if isinstance(results, list):
                    ads_rows = results
                else:
                    data_field = ads_payload.get("data")
                    if isinstance(data_field, list):
                        ads_rows = data_field
            if isinstance(ads_rows, list):
                daily_samples, parse_errors = _extract_station_daily_samples_from_rows(
                    rows=ads_rows,
                    month=month_value,
                    day=day_value,
                )
                result = _build_station_daily_history_result(
                    status="ready" if not parse_errors else "ready_partial",
                    station_id=clean_station_id,
                    cdo_station_id=cdo_station_id,
                    month=month_value,
                    day=day_value,
                    lookback_years=lookback_years_clamped,
                    request_count=1,
                    source_url=ads_source_url,
                    errors=parse_errors,
                    daily_samples=daily_samples,
                    cache_hit=False,
                    cache_fallback_used=False,
                    error_message=(
                        "No historical station samples were returned for this month/day target."
                        if not daily_samples
                        else None
                    ),
                    data_source="access_data_service_v1",
                )
                if not daily_samples:
                    result["status"] = "no_history"
                if cache_file is not None:
                    try:
                        _write_cdo_cache_entry(cache_file, result, current_time)
                    except OSError:
                        pass
                return result

        if cache_payload is not None:
            cached_status = str(cache_payload.get("status") or "").strip().lower()
            if _status_is_cacheable(cached_status):
                stale_result = dict(cache_payload)
                stale_result["cache_hit"] = True
                stale_result["cache_fallback_used"] = True
                stale_result["cache_fresh"] = False
                stale_result["cache_age_seconds"] = round(float(cache_age_seconds or 0.0), 3)
                stale_result["cache_warning"] = "Using cached station history because NOAA ADS fallback is unavailable."
                stale_result["fallback_failure_status"] = ads_status
                return stale_result
        return {
            "status": "disabled_missing_token",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "error": "Missing NOAA/NCEI CDO API token and NOAA Access Data Service fallback unavailable.",
            "fallback_source_url": ads_source_url,
            "fallback_http_status": ads_status,
            "cache_hit": False,
            "cache_fallback_used": False,
        }
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
            "cdo_station_id": cdo_station_id,
            "error": "Missing NOAA/NCEI CDO API token.",
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

    if not daily_samples:
        result = _build_station_daily_history_result(
            status="no_history",
            station_id=clean_station_id,
            cdo_station_id=cdo_station_id,
            month=month_value,
            day=day_value,
            lookback_years=lookback_years_clamped,
            request_count=request_count,
            source_url=source_url,
            errors=errors,
            daily_samples=daily_samples,
            cache_hit=False,
            cache_fallback_used=False,
            error_message="No historical station samples were returned for this month/day target.",
            data_source="cdo_api_v2",
        )
        if cache_file is not None:
            try:
                _write_cdo_cache_entry(cache_file, result, current_time)
            except OSError:
                pass
        return result

    result = _build_station_daily_history_result(
        status="ready" if not errors else "ready_partial",
        station_id=clean_station_id,
        cdo_station_id=cdo_station_id,
        month=month_value,
        day=day_value,
        lookback_years=lookback_years_clamped,
        request_count=request_count,
        source_url=source_url,
        errors=errors,
        daily_samples=daily_samples,
        cache_hit=False,
        cache_fallback_used=False,
        data_source="cdo_api_v2",
    )
    if cache_file is not None:
        try:
            _write_cdo_cache_entry(cache_file, result, current_time)
        except OSError:
            pass
    return result
