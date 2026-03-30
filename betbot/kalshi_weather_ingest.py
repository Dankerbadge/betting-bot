from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from betbot.dns_guard import urlopen_with_dns_recovery

JsonGetter = Callable[[str, float], tuple[int, dict[str, Any] | list[Any] | Any]]


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
