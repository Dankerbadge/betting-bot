from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Callable
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from betbot.dns_guard import urlopen_with_dns_recovery

JsonGetter = Callable[[str, float], tuple[int, dict[str, Any] | list[Any] | Any]]
JsonGetterWithHeaders = Callable[
    [str, float, dict[str, str] | None],
    tuple[int, dict[str, Any] | list[Any] | Any],
]
TextGetter = Callable[[str, float], tuple[int, str]]
BytesGetter = Callable[[str, float], tuple[int, bytes, dict[str, str]]]

_NWS_GRIDPOINT_LAYER_KEYS = (
    "maxTemperature",
    "minTemperature",
    "probabilityOfPrecipitation",
    "quantitativePrecipitation",
    "snowfallAmount",
    "hazards",
)

DEFAULT_TAF_CACHE_URL = "https://aviationweather.gov/data/cache/tafs.cache.xml.gz"


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

_NWS_CLI_LOCATION_BY_STATION = {
    # Some settlement stations do not expose CLI products under the raw ICAO/IATA code.
    # Keep this intentionally minimal and override via BETBOT_WEATHER_NWS_CLI_LOCATION_MAP when needed.
    "KDAL": ("DFW",),
}

_HTTP_ERROR_STATUS_RE = re.compile(r"\bhttp error\s+(\d{3})\b", flags=re.IGNORECASE)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _http_status_from_exception(exc: BaseException) -> int | None:
    if isinstance(exc, HTTPError):
        try:
            return int(exc.code)
        except (TypeError, ValueError):
            return None
    raw_code = getattr(exc, "code", None)
    try:
        if raw_code is not None:
            return int(raw_code)
    except (TypeError, ValueError):
        pass
    match = _HTTP_ERROR_STATUS_RE.search(str(exc or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


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


def _resolve_nws_cli_location_candidates(station_id: str) -> tuple[list[str], set[str]]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return ([], set())

    candidates: list[str] = []
    allowed_station_codes: set[str] = set()

    def _add(code: str) -> None:
        normalized = str(code or "").strip().upper()
        if not normalized:
            return
        if normalized not in candidates:
            candidates.append(normalized)
        allowed_station_codes.add(normalized)

    _add(clean_station_id)
    if clean_station_id.startswith("K") and len(clean_station_id) > 1:
        _add(clean_station_id[1:])

    for alias in _NWS_CLI_LOCATION_BY_STATION.get(clean_station_id, ()):
        _add(alias)

    override_text = str(os.getenv("BETBOT_WEATHER_NWS_CLI_LOCATION_MAP") or "").strip()
    if override_text:
        parsed_overrides: dict[str, list[str]] = {}
        for chunk in override_text.replace(";", ",").split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            key, value = item.split("=", 1)
            station_key = key.strip().upper()
            if not station_key:
                continue
            override_codes = [
                code.strip().upper()
                for code in re.split(r"[|/]", value)
                if code.strip()
            ]
            if not override_codes:
                continue
            parsed_overrides.setdefault(station_key, []).extend(override_codes)

        for key in (clean_station_id, clean_station_id[1:] if clean_station_id.startswith("K") else clean_station_id):
            for alias in parsed_overrides.get(key, []):
                _add(alias)

    return candidates, allowed_station_codes


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


def _sanitize_temperature_bounds_for_sample(
    *,
    sample: dict[str, Any],
    errors: list[dict[str, Any]],
    error_context: dict[str, Any],
) -> None:
    tmax_value = sample.get("tmax_f")
    tmin_value = sample.get("tmin_f")
    if not isinstance(tmax_value, (int, float)) or not isinstance(tmin_value, (int, float)):
        return
    tmax_f = float(tmax_value)
    tmin_f = float(tmin_value)
    if not math.isfinite(tmax_f) or not math.isfinite(tmin_f) or tmin_f > tmax_f:
        sample.pop("tmax_f", None)
        sample.pop("tmin_f", None)
        error_payload = dict(error_context)
        error_payload["error"] = "invalid_temperature_range"
        error_payload["tmax_f"] = round(tmax_f, 3) if math.isfinite(tmax_f) else "non_finite"
        error_payload["tmin_f"] = round(tmin_f, 3) if math.isfinite(tmin_f) else "non_finite"
        errors.append(error_payload)


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
        _sanitize_temperature_bounds_for_sample(
            sample=sample,
            errors=parse_errors,
            error_context={"index": index, "year": year, "date": sample["date"]},
        )

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


def _safe_write_cdo_cache_entry(path: Path, payload: dict[str, Any], now: datetime) -> str | None:
    try:
        _write_cdo_cache_entry(path, payload, now)
        return None
    except OSError as exc:
        message = str(exc).strip()
        return message or exc.__class__.__name__


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


def _http_get_text(url: str, timeout_seconds: float) -> tuple[int, str]:
    request = Request(
        url,
        headers={
            "Accept": "application/xml, text/xml, text/plain, application/json;q=0.9, */*;q=0.8",
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
    return (status, payload)


def _http_get_bytes(url: str, timeout_seconds: float) -> tuple[int, bytes, dict[str, str]]:
    request = Request(
        url,
        headers={
            "Accept": "application/gzip, application/xml, text/xml, text/plain, */*;q=0.8",
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
        payload = response.read()
        headers: dict[str, str] = {}
        for key in ("etag", "last-modified", "content-type", "content-length"):
            value = response.headers.get(key)
            if value:
                headers[key] = str(value)
    return (status, payload, headers)


def _xml_local_name(tag: str) -> str:
    text = str(tag or "")
    if "}" in text:
        text = text.split("}", 1)[1]
    return text.lower()


def _parse_taf_temp_c(token: str) -> float | None:
    text = str(token or "").strip().upper()
    if not text:
        return None
    sign = -1.0 if text.startswith("M") else 1.0
    digits = text[1:] if text.startswith("M") else text
    if len(digits) != 2 or not digits.isdigit():
        return None
    return sign * float(int(digits))


def _extract_taf_envelope_from_raw_text(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").upper()
    tx_values_f: list[float] = []
    tn_values_f: list[float] = []
    for match in re.finditer(r"\bTX(M?\d{2})/\d{4}Z\b", text):
        temp_c = _parse_taf_temp_c(match.group(1))
        if isinstance(temp_c, float):
            tx_values_f.append(round((temp_c * 9.0 / 5.0) + 32.0, 3))
    for match in re.finditer(r"\bTN(M?\d{2})/\d{4}Z\b", text):
        temp_c = _parse_taf_temp_c(match.group(1))
        if isinstance(temp_c, float):
            tn_values_f.append(round((temp_c * 9.0 / 5.0) + 32.0, 3))

    marker_counts = {
        "tempo_count": len(re.findall(r"\bTEMPO\b", text)),
        "prob_count": len(re.findall(r"\bPROB(?:30|40)\b", text)),
        "fm_count": len(re.findall(r"\bFM\d{6}\b", text)),
        "becmg_count": len(re.findall(r"\bBECMG\b", text)),
        "thunder_count": len(re.findall(r"\bTS(?:RA|GR|SN)?\b", text)),
        "amended_count": len(re.findall(r"\bAMD\b", text)),
    }
    volatility_score = (
        (1.40 * marker_counts["tempo_count"])
        + (1.20 * marker_counts["prob_count"])
        + (0.75 * marker_counts["fm_count"])
        + (0.50 * marker_counts["becmg_count"])
        + (0.90 * marker_counts["thunder_count"])
        + (1.10 * marker_counts["amended_count"])
    )
    if volatility_score > 10.0:
        volatility_score = 10.0

    return {
        "taf_tx_values_f": tx_values_f,
        "taf_tn_values_f": tn_values_f,
        "taf_upper_bound_f": max(tx_values_f) if tx_values_f else None,
        "taf_lower_bound_f": min(tn_values_f) if tn_values_f else None,
        "taf_volatility_score": round(float(volatility_score), 6),
        "taf_marker_counts": marker_counts,
    }


def fetch_aviationweather_taf_temperature_envelopes(
    *,
    station_ids: list[str],
    cache_url: str = DEFAULT_TAF_CACHE_URL,
    timeout_seconds: float = 20.0,
    http_get_bytes: BytesGetter = _http_get_bytes,
) -> dict[str, Any]:
    requested = sorted({_normalize_text(item).upper() for item in station_ids if _normalize_text(item)})
    if not requested:
        return {
            "status": "no_station_ids",
            "cache_url": cache_url,
            "requested_station_count": 0,
            "station_envelopes": {},
        }

    status_code, payload, _ = http_get_bytes(str(cache_url), float(timeout_seconds))
    if status_code != 200:
        return {
            "status": "request_failed",
            "cache_url": cache_url,
            "http_status": status_code,
            "requested_station_count": len(requested),
            "station_envelopes": {},
        }

    try:
        xml_bytes = gzip.decompress(payload)
    except OSError as exc:
        return {
            "status": "invalid_gzip",
            "cache_url": cache_url,
            "requested_station_count": len(requested),
            "error": str(exc),
            "station_envelopes": {},
        }

    try:
        root = ET.fromstring(xml_bytes.decode("utf-8", errors="replace"))
    except ET.ParseError as exc:
        return {
            "status": "invalid_xml",
            "cache_url": cache_url,
            "requested_station_count": len(requested),
            "error": str(exc),
            "station_envelopes": {},
        }

    latest_by_station: dict[str, dict[str, Any]] = {}
    for element in root.iter():
        if _xml_local_name(element.tag) != "taf":
            continue
        field_map: dict[str, str] = {}
        for child in element.iter():
            local_tag = _xml_local_name(child.tag)
            text = str(child.text or "").strip()
            if text and local_tag not in field_map:
                field_map[local_tag] = text
        station_id = _normalize_text(
            field_map.get("station_id")
            or field_map.get("station")
            or field_map.get("icao_id")
            or field_map.get("icaoid")
        ).upper()
        if not station_id or station_id not in requested:
            continue
        raw_text = _normalize_text(field_map.get("raw_text") or field_map.get("raw") or field_map.get("taf"))
        issue_time_text = _normalize_text(field_map.get("issue_time") or field_map.get("issue") or field_map.get("issuetime"))
        issue_time = _parse_iso_datetime(issue_time_text)
        existing = latest_by_station.get(station_id)
        existing_issue = _parse_iso_datetime(existing.get("taf_issue_time")) if isinstance(existing, dict) else None
        if isinstance(existing_issue, datetime) and isinstance(issue_time, datetime):
            if issue_time <= existing_issue:
                continue
        elif isinstance(existing_issue, datetime) and issue_time is None:
            continue
        latest_by_station[station_id] = {
            "station_id": station_id,
            "taf_issue_time": issue_time.isoformat() if isinstance(issue_time, datetime) else issue_time_text,
            "taf_raw_text": raw_text,
        }

    station_envelopes: dict[str, dict[str, Any]] = {}
    ready_count = 0
    for station in requested:
        latest = latest_by_station.get(station)
        if not isinstance(latest, dict):
            station_envelopes[station] = {
                "taf_status": "missing_station",
                "station_id": station,
                "taf_upper_bound_f": None,
                "taf_lower_bound_f": None,
                "taf_volatility_score": 0.0,
                "taf_marker_counts": {},
            }
            continue
        extracted = _extract_taf_envelope_from_raw_text(_normalize_text(latest.get("taf_raw_text")))
        station_envelopes[station] = {
            "taf_status": "ready",
            "station_id": station,
            "taf_issue_time": _normalize_text(latest.get("taf_issue_time")),
            "taf_upper_bound_f": extracted["taf_upper_bound_f"],
            "taf_lower_bound_f": extracted["taf_lower_bound_f"],
            "taf_volatility_score": extracted["taf_volatility_score"],
            "taf_marker_counts": extracted["taf_marker_counts"],
        }
        ready_count += 1

    return {
        "status": "ready",
        "cache_url": cache_url,
        "http_status": status_code,
        "requested_station_count": len(requested),
        "taf_station_ready_count": ready_count,
        "station_envelopes": station_envelopes,
    }


def _parse_s3_list_bucket_payload(payload_text: str) -> dict[str, Any]:
    text = str(payload_text or "").strip()
    if not text:
        return {
            "keys": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_marker": "",
            "parse_error": "empty_payload",
        }
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        return {
            "keys": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_marker": "",
            "parse_error": str(exc),
        }

    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    keys = [
        str(node.text or "").strip()
        for node in root.findall("s3:Contents/s3:Key", namespace)
        if str(node.text or "").strip()
    ]
    common_prefixes = [
        str(node.text or "").strip()
        for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)
        if str(node.text or "").strip()
    ]
    is_truncated_text = str(root.findtext("s3:IsTruncated", default="", namespaces=namespace) or "").strip().lower()
    next_marker = str(root.findtext("s3:NextMarker", default="", namespaces=namespace) or "").strip()
    return {
        "keys": keys,
        "common_prefixes": common_prefixes,
        "is_truncated": is_truncated_text == "true",
        "next_marker": next_marker,
    }


def fetch_nws_station_hourly_forecast(
    *,
    station_id: str,
    include_gridpoint_data: bool = False,
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
    forecast_grid_data_url = (
        str((points_properties or {}).get("forecastGridData") or "").strip()
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
            "forecast_grid_data_url": forecast_grid_data_url,
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

    payload = {
        "status": "ready",
        "station_id": clean_station_id,
        "station_timezone": station_timezone or "",
        "latitude": lat_value,
        "longitude": lon_value,
        "forecast_hourly_url": forecast_hourly_url,
        "forecast_grid_data_url": forecast_grid_data_url,
        "forecast_updated_at": updated or generated_at,
        "periods": [period for period in periods if isinstance(period, dict)],
        "http_status_station": status_station,
        "http_status_points": status_points,
        "http_status_forecast": status_forecast,
    }
    if include_gridpoint_data:
        payload["gridpoint_status"] = "missing"
        payload["gridpoint_updated_at"] = ""
        payload["gridpoint_layers"] = {}
        if forecast_grid_data_url:
            status_gridpoint, payload_gridpoint = http_get_json(forecast_grid_data_url, timeout_seconds)
            payload["http_status_gridpoint"] = status_gridpoint
            if status_gridpoint == 200 and isinstance(payload_gridpoint, dict):
                gridpoint_properties = payload_gridpoint.get("properties") if isinstance(payload_gridpoint, dict) else None
                if isinstance(gridpoint_properties, dict):
                    layers: dict[str, list[dict[str, Any]]] = {}
                    for layer_key in _NWS_GRIDPOINT_LAYER_KEYS:
                        layer_payload = gridpoint_properties.get(layer_key)
                        if not isinstance(layer_payload, dict):
                            continue
                        values = layer_payload.get("values")
                        if not isinstance(values, list):
                            continue
                        normalized_values = [item for item in values if isinstance(item, dict)]
                        if normalized_values:
                            layers[layer_key] = normalized_values
                    payload["gridpoint_layers"] = layers
                    payload["gridpoint_status"] = "ready"
                    payload["gridpoint_updated_at"] = str(
                        gridpoint_properties.get("updateTime")
                        or gridpoint_properties.get("generatedAt")
                        or ""
                    ).strip()
                else:
                    payload["gridpoint_status"] = "invalid_payload"
                    payload["gridpoint_error"] = "Gridpoint response missing properties object."
            else:
                payload["gridpoint_status"] = "unavailable"
                payload["gridpoint_error"] = "NWS forecastGridData request failed."
        else:
            payload["gridpoint_error"] = "NWS points metadata did not include forecastGridData."
    return payload


def fetch_nws_station_recent_observations(
    *,
    station_id: str,
    limit: int = 24,
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
    limit_value = max(1, min(500, int(limit)))
    observations_url = f"https://api.weather.gov/stations/{clean_station_id}/observations?limit={limit_value}"
    status, payload = http_get_json(observations_url, timeout_seconds)
    if status != 200 or not isinstance(payload, dict):
        return {
            "status": "observations_unavailable",
            "station_id": clean_station_id,
            "observations_url": observations_url,
            "http_status_observations": status,
            "error": f"NWS station observations request failed for station {clean_station_id}.",
        }

    features = payload.get("features")
    if not isinstance(features, list):
        features = []
    observations: list[dict[str, Any]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        observations.append(
            {
                "timestamp": str(properties.get("timestamp") or "").strip(),
                "text_description": str(properties.get("textDescription") or "").strip(),
                "temperature_c": (
                    float(properties.get("temperature", {}).get("value"))
                    if isinstance(properties.get("temperature"), dict)
                    and isinstance(properties.get("temperature", {}).get("value"), (int, float))
                    else None
                ),
                "dewpoint_c": (
                    float(properties.get("dewpoint", {}).get("value"))
                    if isinstance(properties.get("dewpoint"), dict)
                    and isinstance(properties.get("dewpoint", {}).get("value"), (int, float))
                    else None
                ),
                "relative_humidity_pct": (
                    float(properties.get("relativeHumidity", {}).get("value"))
                    if isinstance(properties.get("relativeHumidity"), dict)
                    and isinstance(properties.get("relativeHumidity", {}).get("value"), (int, float))
                    else None
                ),
                "precipitation_last_hour_mm": (
                    float(properties.get("precipitationLastHour", {}).get("value"))
                    if isinstance(properties.get("precipitationLastHour"), dict)
                    and isinstance(properties.get("precipitationLastHour", {}).get("value"), (int, float))
                    else None
                ),
                "wind_speed_mps": (
                    float(properties.get("windSpeed", {}).get("value"))
                    if isinstance(properties.get("windSpeed"), dict)
                    and isinstance(properties.get("windSpeed", {}).get("value"), (int, float))
                    else None
                ),
            }
        )

    return {
        "status": "ready",
        "station_id": clean_station_id,
        "observations_url": observations_url,
        "http_status_observations": status,
        "observations_count": len(observations),
        "observations": observations,
    }


def fetch_nws_active_alerts_for_point(
    *,
    latitude: float,
    longitude: float,
    timeout_seconds: float = 12.0,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    try:
        lat_value = float(latitude)
        lon_value = float(longitude)
    except (TypeError, ValueError):
        return {
            "status": "invalid_coordinates",
            "error": "Latitude/longitude must be numeric.",
        }

    alerts_url = f"https://api.weather.gov/alerts/active?point={lat_value:.4f},{lon_value:.4f}"
    status, payload = http_get_json(alerts_url, timeout_seconds)
    if status != 200 or not isinstance(payload, dict):
        return {
            "status": "alerts_unavailable",
            "alerts_url": alerts_url,
            "http_status_alerts": status,
            "error": "NWS active alerts request failed for point.",
        }

    features = payload.get("features")
    if not isinstance(features, list):
        features = []

    alerts: list[dict[str, Any]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        alerts.append(
            {
                "id": str(feature.get("id") or "").strip(),
                "event": str(properties.get("event") or "").strip(),
                "severity": str(properties.get("severity") or "").strip(),
                "urgency": str(properties.get("urgency") or "").strip(),
                "headline": str(properties.get("headline") or "").strip(),
                "effective": str(properties.get("effective") or "").strip(),
                "expires": str(properties.get("expires") or "").strip(),
                "areas_desc": str(properties.get("areaDesc") or "").strip(),
            }
        )

    return {
        "status": "ready",
        "alerts_url": alerts_url,
        "http_status_alerts": status,
        "alerts_count": len(alerts),
        "alerts": alerts,
    }


def _extract_timestamp_from_key(
    *,
    key: str,
    pattern: str,
) -> datetime | None:
    match = re.search(pattern, str(key or ""))
    if not match:
        return None
    stamp = str(match.group(1) or "").strip()
    if not stamp:
        return None
    for fmt in ("%Y%m%d-%H%M%S", "%Y%m%d%H%M", "%Y%m%d%H"):
        try:
            return datetime.strptime(stamp, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def fetch_noaa_mrms_qpe_latest_metadata(
    *,
    now: datetime | None = None,
    timeout_seconds: float = 12.0,
    lookback_days: int = 2,
    product_prefix: str = "CONUS/MultiSensor_QPE_01H_Pass2_00.00/",
    base_url: str = "https://noaa-mrms-pds.s3.amazonaws.com",
    http_get_text: TextGetter = _http_get_text,
) -> dict[str, Any]:
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    clean_prefix = str(product_prefix or "").strip().lstrip("/")
    if not clean_prefix.endswith("/"):
        clean_prefix = f"{clean_prefix}/"
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        return {
            "status": "invalid_base_url",
            "error": "Missing MRMS base URL.",
        }

    lookback_days_clamped = max(1, min(7, int(lookback_days)))
    request_count = 0
    errors: list[dict[str, Any]] = []
    for offset in range(lookback_days_clamped + 1):
        day_value = current_time.date().fromordinal(current_time.date().toordinal() - offset)
        day_prefix = f"{clean_prefix}{day_value.strftime('%Y%m%d')}/"
        list_url = f"{base}/?{urlencode([('prefix', day_prefix), ('max-keys', '1000')])}"
        request_count += 1
        status, payload_text = http_get_text(list_url, timeout_seconds)
        if status != 200:
            errors.append(
                {
                    "prefix": day_prefix,
                    "http_status": int(status),
                    "error": "mrms_list_request_failed",
                }
            )
            continue
        parsed = _parse_s3_list_bucket_payload(payload_text)
        parse_error = str(parsed.get("parse_error") or "").strip()
        if parse_error:
            errors.append(
                {
                    "prefix": day_prefix,
                    "http_status": int(status),
                    "error": f"mrms_list_parse_error:{parse_error}",
                }
            )
            continue
        keys = [
            str(key or "").strip()
            for key in (parsed.get("keys") or [])
            if str(key or "").strip().endswith(".grib2.gz")
        ]
        if not keys:
            continue
        latest_key = sorted(keys)[-1]
        observed_at = _extract_timestamp_from_key(
            key=latest_key,
            pattern=r"_(\d{8}-\d{6})\.grib2\.gz$",
        )
        age_seconds = None
        if isinstance(observed_at, datetime):
            age_seconds = max(0.0, (current_time - observed_at.astimezone(timezone.utc)).total_seconds())
        return {
            "status": "ready",
            "data_source": "noaa_mrms_s3",
            "product_prefix": clean_prefix,
            "day_prefix": day_prefix,
            "latest_key": latest_key,
            "latest_url": f"{base}/{latest_key}",
            "observed_at_utc": observed_at.isoformat() if isinstance(observed_at, datetime) else "",
            "age_seconds": round(age_seconds, 3) if isinstance(age_seconds, float) else None,
            "request_count": int(request_count),
            "list_url": list_url,
            "lookback_days": int(lookback_days_clamped),
            "errors": errors,
        }

    return {
        "status": "no_data",
        "data_source": "noaa_mrms_s3",
        "product_prefix": clean_prefix,
        "request_count": int(request_count),
        "lookback_days": int(lookback_days_clamped),
        "errors": errors,
        "error": "No MRMS QPE objects were found in the requested lookback window.",
    }


def fetch_noaa_nbm_latest_snapshot(
    *,
    now: datetime | None = None,
    timeout_seconds: float = 12.0,
    lookback_days: int = 2,
    region: str = "co",
    base_url: str = "https://noaa-nbm-grib2-pds.s3.amazonaws.com",
    http_get_text: TextGetter = _http_get_text,
) -> dict[str, Any]:
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    clean_region = str(region or "co").strip().lower() or "co"
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        return {
            "status": "invalid_base_url",
            "error": "Missing NBM base URL.",
        }

    lookback_days_clamped = max(1, min(7, int(lookback_days)))
    request_count = 0
    errors: list[dict[str, Any]] = []
    candidate_days = [
        current_time.date().fromordinal(current_time.date().toordinal() - offset)
        for offset in range(lookback_days_clamped + 1)
    ]
    for day_value in candidate_days:
        day_stamp = day_value.strftime("%Y%m%d")
        for cycle_hour in range(23, -1, -1):
            cycle_prefix = f"blend.{day_stamp}/{cycle_hour:02d}/core/"
            list_url = f"{base}/?{urlencode([('prefix', cycle_prefix), ('max-keys', '2000')])}"
            request_count += 1
            status, payload_text = http_get_text(list_url, timeout_seconds)
            if status != 200:
                errors.append(
                    {
                        "prefix": cycle_prefix,
                        "http_status": int(status),
                        "error": "nbm_list_request_failed",
                    }
                )
                continue
            parsed = _parse_s3_list_bucket_payload(payload_text)
            parse_error = str(parsed.get("parse_error") or "").strip()
            if parse_error:
                errors.append(
                    {
                        "prefix": cycle_prefix,
                        "http_status": int(status),
                        "error": f"nbm_list_parse_error:{parse_error}",
                    }
                )
                continue
            idx_keys = [
                str(key or "").strip()
                for key in (parsed.get("keys") or [])
                if str(key or "").strip().endswith(f".{clean_region}.grib2.idx")
            ]
            if not idx_keys:
                continue

            forecast_hours: list[int] = []
            for key in idx_keys:
                match = re.search(r"\.f(\d{3})\.", key)
                if not match:
                    continue
                try:
                    forecast_hours.append(int(match.group(1)))
                except ValueError:
                    continue
            forecast_hours = sorted(set(forecast_hours))
            representative_idx_key = sorted(idx_keys)[0]
            representative_idx_url = f"{base}/{representative_idx_key}"
            status_idx, idx_text = http_get_text(representative_idx_url, timeout_seconds)
            idx_variable_counts: dict[str, int] = {}
            if status_idx == 200 and idx_text:
                for line in idx_text.splitlines():
                    fields = line.split(":")
                    if len(fields) < 4:
                        continue
                    variable = str(fields[3] or "").strip().upper()
                    if variable:
                        idx_variable_counts[variable] = idx_variable_counts.get(variable, 0) + 1

            cycle_dt = datetime.strptime(f"{day_stamp}{cycle_hour:02d}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
            cycle_age_seconds = max(0.0, (current_time - cycle_dt).total_seconds())
            return {
                "status": "ready",
                "data_source": "noaa_nbm_s3",
                "region": clean_region,
                "cycle_prefix": cycle_prefix,
                "cycle_utc": cycle_dt.isoformat(),
                "cycle_age_seconds": round(cycle_age_seconds, 3),
                "forecast_hours_available": forecast_hours,
                "forecast_hours_count": len(forecast_hours),
                "max_forecast_hour": max(forecast_hours) if forecast_hours else None,
                "representative_idx_key": representative_idx_key,
                "representative_idx_url": representative_idx_url,
                "representative_idx_http_status": int(status_idx),
                "idx_variable_counts": idx_variable_counts,
                "request_count": int(request_count + 1),
                "lookback_days": int(lookback_days_clamped),
                "list_url": list_url,
                "errors": errors,
            }

    return {
        "status": "no_data",
        "data_source": "noaa_nbm_s3",
        "region": clean_region,
        "request_count": int(request_count),
        "lookback_days": int(lookback_days_clamped),
        "errors": errors,
        "error": "No NBM snapshot objects were found in the requested lookback window.",
    }


def fetch_ncei_normals_station_day(
    *,
    station_id: str,
    month: int,
    day: int,
    timeout_seconds: float = 12.0,
    cdo_token: str | None = None,
    rate_limit_retries: int = 3,
    rate_limit_backoff_seconds: float = 1.0,
    rate_limit_backoff_cap_seconds: float = 8.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    http_get_json_with_headers: JsonGetterWithHeaders = _http_get_json_with_headers,
) -> dict[str, Any]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return {
            "status": "invalid_station",
            "station_id": "",
            "error": "Missing station identifier.",
        }
    cdo_station_id = _resolve_cdo_station_id(clean_station_id)
    if not cdo_station_id:
        return {
            "status": "station_mapping_missing",
            "station_id": clean_station_id,
            "error": "No CDO station mapping is configured for normals lookup.",
        }

    try:
        month_value = int(month)
        day_value = int(day)
    except (TypeError, ValueError):
        return {
            "status": "invalid_target_day",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "error": "Month/day target is invalid.",
        }
    if month_value < 1 or month_value > 12 or day_value < 1 or day_value > 31:
        return {
            "status": "invalid_target_day",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "error": "Month/day target is out of range.",
        }

    token = str(
        cdo_token
        or os.getenv("BETBOT_NOAA_CDO_TOKEN")
        or os.getenv("NOAA_CDO_TOKEN")
        or os.getenv("NCEI_CDO_TOKEN")
        or ""
    ).strip()
    if not token:
        return {
            "status": "disabled_missing_token",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "error": "Missing NOAA/NCEI CDO API token for normals lookup.",
        }

    normals_reference_date = f"2010-{month_value:02d}-{day_value:02d}"
    datatypes = (
        "DLY-TMAX-NORMAL",
        "DLY-TMAX-STDDEV",
        "DLY-TMIN-NORMAL",
        "DLY-TMIN-STDDEV",
        "DLY-PRCP-PCTALL-GE001HI",
    )
    params: list[tuple[str, str]] = [
        ("datasetid", "NORMAL_DLY"),
        ("stationid", cdo_station_id),
        ("startdate", normals_reference_date),
        ("enddate", normals_reference_date),
        ("units", "standard"),
        ("limit", "1000"),
    ]
    params.extend(("datatypeid", datatype) for datatype in datatypes)
    source_url = f"https://www.ncei.noaa.gov/cdo-web/api/v2/data?{urlencode(params, doseq=True)}"
    safe_rate_limit_retries = max(0, int(rate_limit_retries))
    safe_rate_limit_backoff_seconds = max(0.0, float(rate_limit_backoff_seconds))
    safe_rate_limit_backoff_cap_seconds = max(
        safe_rate_limit_backoff_seconds,
        float(rate_limit_backoff_cap_seconds),
    )
    rate_limit_retries_used = 0
    status = 0
    payload: dict[str, Any] | list[Any] | Any = {}
    for attempt in range(safe_rate_limit_retries + 1):
        try:
            status, payload = http_get_json_with_headers(
                source_url,
                timeout_seconds,
                {"token": token},
            )
        except Exception as exc:
            inferred_status = _http_status_from_exception(exc)
            if inferred_status is None:
                raise
            status = inferred_status
            payload = {
                "error": str(exc),
            }
        if status != 429:
            break
        if attempt >= safe_rate_limit_retries:
            break
        backoff_seconds = min(
            safe_rate_limit_backoff_cap_seconds,
            safe_rate_limit_backoff_seconds * (2**attempt),
        )
        if backoff_seconds > 0:
            sleep_fn(backoff_seconds)
        rate_limit_retries_used += 1
    if status != 200 or not isinstance(payload, dict):
        failed_status = "rate_limited" if int(status) == 429 else "normals_unavailable"
        failed_error = (
            "NCEI normals request exhausted rate-limit retries."
            if int(status) == 429
            else "NCEI normals request failed."
        )
        return {
            "status": failed_status,
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "month": int(month_value),
            "day": int(day_value),
            "http_status": int(status),
            "source_url": source_url,
            "rate_limit_retries_used": int(rate_limit_retries_used),
            "error": failed_error,
        }

    results = payload.get("results")
    if not isinstance(results, list):
        results = []
    values_by_datatype: dict[str, float] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        datatype = str(item.get("datatype") or "").strip().upper()
        numeric_value = item.get("value")
        try:
            numeric = float(numeric_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric):
            continue
        values_by_datatype[datatype] = numeric

    if not values_by_datatype:
        return {
            "status": "no_history",
            "station_id": clean_station_id,
            "cdo_station_id": cdo_station_id,
            "month": int(month_value),
            "day": int(day_value),
            "source_url": source_url,
            "sample_years": 0,
            "error": "No normals values were returned for the target station/day.",
        }

    rain_frequency = None
    rain_raw = values_by_datatype.get("DLY-PRCP-PCTALL-GE001HI")
    if isinstance(rain_raw, float):
        if rain_raw > 100.0:
            rain_pct = rain_raw / 10.0
        elif rain_raw > 1.0:
            rain_pct = rain_raw
        else:
            rain_pct = rain_raw * 100.0
        rain_frequency = max(0.0, min(1.0, rain_pct / 100.0))

    return {
        "status": "ready",
        "station_id": clean_station_id,
        "cdo_station_id": cdo_station_id,
        "month": int(month_value),
        "day": int(day_value),
        "normals_reference_date": normals_reference_date,
        "source_url": source_url,
        "sample_years": 30,
        "tmax_normal_f": values_by_datatype.get("DLY-TMAX-NORMAL"),
        "tmax_stddev_f": values_by_datatype.get("DLY-TMAX-STDDEV"),
        "tmin_normal_f": values_by_datatype.get("DLY-TMIN-NORMAL"),
        "tmin_stddev_f": values_by_datatype.get("DLY-TMIN-STDDEV"),
        "rain_day_frequency": rain_frequency,
        "raw_values": values_by_datatype,
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
            numeric = float(item)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric):
            continue
        values.append(numeric)
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
    rate_limit_retries: int = 3,
    rate_limit_backoff_seconds: float = 1.0,
    rate_limit_backoff_cap_seconds: float = 8.0,
    sleep_fn: Callable[[float], None] = time.sleep,
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
                    cache_write_error = _safe_write_cdo_cache_entry(cache_file, result, current_time)
                    if cache_write_error:
                        result["cache_write_error"] = cache_write_error
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
    safe_rate_limit_retries = max(0, int(rate_limit_retries))
    safe_rate_limit_backoff_seconds = max(0.0, float(rate_limit_backoff_seconds))
    safe_rate_limit_backoff_cap_seconds = max(
        safe_rate_limit_backoff_seconds,
        float(rate_limit_backoff_cap_seconds),
    )
    rate_limit_retries_used = 0

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
        status = 0
        payload: dict[str, Any] | list[Any] | Any = {}
        for attempt in range(safe_rate_limit_retries + 1):
            request_count += 1
            try:
                status, payload = http_get_json_with_headers(url, timeout_seconds, request_headers)
            except Exception as exc:
                inferred_status = _http_status_from_exception(exc)
                if inferred_status is None:
                    raise
                status = inferred_status
                payload = {"error": str(exc)}
            if status != 429:
                break
            if attempt >= safe_rate_limit_retries:
                break
            backoff_seconds = min(
                safe_rate_limit_backoff_cap_seconds,
                safe_rate_limit_backoff_seconds * (2**attempt),
            )
            if backoff_seconds > 0:
                sleep_fn(backoff_seconds)
            rate_limit_retries_used += 1
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
                    stale_result["rate_limit_retries_used"] = int(rate_limit_retries_used)
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
                "rate_limit_retries_used": int(rate_limit_retries_used),
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
        _sanitize_temperature_bounds_for_sample(
            sample=sample,
            errors=errors,
            error_context={"year": year, "date": date_stamp},
        )
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
            cache_write_error = _safe_write_cdo_cache_entry(cache_file, result, current_time)
            if cache_write_error:
                result["cache_write_error"] = cache_write_error
        result["rate_limit_retries_used"] = int(rate_limit_retries_used)
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
        cache_write_error = _safe_write_cdo_cache_entry(cache_file, result, current_time)
        if cache_write_error:
            result["cache_write_error"] = cache_write_error
    result["rate_limit_retries_used"] = int(rate_limit_retries_used)
    return result


def _parse_cli_report_target_date(product_text: str) -> str:
    text = str(product_text or "")
    match = re.search(
        r"CLIMATE SUMMARY FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})",
        text.upper(),
    )
    if not match:
        return ""
    month_name = str(match.group(1) or "").strip().title()
    day_text = str(match.group(2) or "").strip()
    year_text = str(match.group(3) or "").strip()
    for month_format in ("%B", "%b"):
        try:
            month_value = datetime.strptime(month_name, month_format).month
        except ValueError:
            continue
        try:
            day_value = int(day_text)
            year_value = int(year_text)
            return datetime(year=year_value, month=month_value, day=day_value, tzinfo=timezone.utc).date().isoformat()
        except (TypeError, ValueError):
            return ""
    return ""


def _parse_cli_station_code(product_text: str) -> str:
    text = str(product_text or "")
    match = re.search(r"(?m)^\s*CLI([A-Z0-9]{3,4})\s*$", text.upper())
    if not match:
        return ""
    return str(match.group(1) or "").strip().upper()


def _parse_cli_observed_high_low_f(product_text: str) -> tuple[float | None, float | None]:
    text = str(product_text or "")
    tmax_f: float | None = None
    tmin_f: float | None = None
    for raw_line in text.splitlines():
        line = raw_line.upper()
        if tmax_f is None:
            max_match = re.search(r"^\s*MAXIMUM(?:\s+TEMPERATURE\s*\(F\))?\s+(-?\d{1,3})\b", line)
            if max_match:
                try:
                    tmax_f = float(int(max_match.group(1)))
                except (TypeError, ValueError):
                    tmax_f = None
        if tmin_f is None:
            min_match = re.search(r"^\s*MINIMUM(?:\s+TEMPERATURE\s*\(F\))?\s+(-?\d{1,3})\b", line)
            if min_match:
                try:
                    tmin_f = float(int(min_match.group(1)))
                except (TypeError, ValueError):
                    tmin_f = None
        if tmax_f is not None and tmin_f is not None:
            break
    return tmax_f, tmin_f


def fetch_nws_cli_station_daily_summary_for_date(
    *,
    station_id: str,
    target_date: str,
    timeout_seconds: float = 12.0,
    max_products: int = 20,
    http_get_json: JsonGetter = _http_get_json,
) -> dict[str, Any]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return {
            "status": "invalid_station",
            "station_id": "",
            "target_date": str(target_date or ""),
            "error": "Missing station identifier.",
        }

    target_text = str(target_date or "").strip()
    try:
        target_stamp = datetime.fromisoformat(target_text).date().isoformat()
    except ValueError:
        return {
            "status": "invalid_target_date",
            "station_id": clean_station_id,
            "target_date": target_text,
            "error": "target_date must be YYYY-MM-DD.",
        }

    location_candidates, allowed_station_codes = _resolve_nws_cli_location_candidates(clean_station_id)

    max_products_safe = max(1, int(max_products))
    product_errors: list[str] = []
    scanned_products = 0
    for location in location_candidates:
        list_url = f"https://api.weather.gov/products/types/CLI/locations/{location}"
        list_status, list_payload = http_get_json(list_url, timeout_seconds)
        if list_status != 200 or not isinstance(list_payload, dict):
            product_errors.append(f"location_lookup_failed:{location}:{list_status}")
            continue
        graph = list_payload.get("@graph")
        if not isinstance(graph, list) or not graph:
            continue
        sorted_graph = sorted(
            [item for item in graph if isinstance(item, dict)],
            key=lambda item: str(item.get("issuanceTime") or ""),
            reverse=True,
        )
        for item in sorted_graph[:max_products_safe]:
            product_id = str(item.get("id") or "").strip()
            if not product_id:
                continue
            product_url = f"https://api.weather.gov/products/{product_id}"
            product_status, product_payload = http_get_json(product_url, timeout_seconds)
            scanned_products += 1
            if product_status != 200 or not isinstance(product_payload, dict):
                product_errors.append(f"product_fetch_failed:{product_id}:{product_status}")
                continue
            product_text = str(product_payload.get("productText") or "")
            if not product_text:
                continue
            report_date = _parse_cli_report_target_date(product_text)
            if report_date != target_stamp:
                continue
            cli_station_code = _parse_cli_station_code(product_text)
            if cli_station_code and cli_station_code not in allowed_station_codes:
                product_errors.append(f"station_code_mismatch:{product_id}:{cli_station_code}")
                continue
            tmax_f, tmin_f = _parse_cli_observed_high_low_f(product_text)
            if tmax_f is None and tmin_f is None:
                continue
            sample: dict[str, Any] = {"date": target_stamp}
            if tmax_f is not None:
                sample["tmax_f"] = tmax_f
            if tmin_f is not None:
                sample["tmin_f"] = tmin_f
            return {
                "status": "ready",
                "station_id": clean_station_id,
                "target_date": target_stamp,
                "data_source": "nws_cli_daily_climate_report",
                "source_url": product_url,
                "http_status": product_status,
                "nws_product_id": product_id,
                "nws_cli_station_code": cli_station_code,
                "daily_sample": sample,
                "scanned_products": scanned_products,
                "location_candidates": location_candidates,
            }

    return {
        "status": "no_final_report",
        "station_id": clean_station_id,
        "target_date": target_stamp,
        "data_source": "nws_cli_daily_climate_report",
        "source_url": "https://api.weather.gov/products/types/CLI/locations",
        "http_status": 200,
        "daily_sample": {},
        "scanned_products": scanned_products,
        "location_candidates": location_candidates,
        "error": "No matching CLI daily climate report found for station/date.",
        "errors": product_errors[:20],
    }


def fetch_ncei_station_daily_summary_for_date(
    *,
    station_id: str,
    target_date: str,
    timeout_seconds: float = 12.0,
    cdo_token: str | None = None,
    enable_nws_cli_lookup: bool = True,
    nws_cli_max_products: int = 20,
    http_get_json: JsonGetter = _http_get_json,
    http_get_json_with_headers: JsonGetterWithHeaders = _http_get_json_with_headers,
) -> dict[str, Any]:
    clean_station_id = str(station_id or "").strip().upper()
    if not clean_station_id:
        return {
            "status": "invalid_station",
            "station_id": "",
            "target_date": str(target_date or ""),
            "error": "Missing station identifier.",
        }

    target_text = str(target_date or "").strip()
    try:
        target_dt = datetime.fromisoformat(target_text)
    except ValueError:
        return {
            "status": "invalid_target_date",
            "station_id": clean_station_id,
            "target_date": target_text,
            "error": "target_date must be YYYY-MM-DD.",
        }
    target_stamp = target_dt.date().isoformat()

    nws_lookup_status = "not_attempted"
    nws_lookup_error = ""
    nws_lookup_http_status = 0
    if enable_nws_cli_lookup:
        try:
            nws_payload = fetch_nws_cli_station_daily_summary_for_date(
                station_id=clean_station_id,
                target_date=target_stamp,
                timeout_seconds=timeout_seconds,
                max_products=nws_cli_max_products,
                http_get_json=http_get_json,
            )
        except Exception as exc:
            nws_payload = {
                "status": "request_failed",
                "station_id": clean_station_id,
                "target_date": target_stamp,
                "data_source": "nws_cli_daily_climate_report",
                "source_url": "https://api.weather.gov/products/types/CLI/locations",
                "http_status": _http_status_from_exception(exc) or 0,
                "daily_sample": {},
                "error": str(exc),
            }
        nws_lookup_status = str(nws_payload.get("status") or "unknown")
        nws_lookup_error = str(nws_payload.get("error") or "")
        nws_lookup_http_status = int(nws_payload.get("http_status") or 0)
        if nws_lookup_status == "ready":
            cdo_station_id = _resolve_cdo_station_id(clean_station_id)
            if cdo_station_id:
                nws_payload["cdo_station_id"] = cdo_station_id
            return nws_payload

    cdo_station_id = _resolve_cdo_station_id(clean_station_id)

    token = str(
        cdo_token
        or os.getenv("BETBOT_NOAA_CDO_TOKEN")
        or os.getenv("NOAA_CDO_TOKEN")
        or os.getenv("NCEI_CDO_TOKEN")
        or ""
    ).strip()

    cdo_status = 0
    cdo_error = ""
    source_url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    if token and cdo_station_id:
        cdo_params = [
            ("datasetid", "GHCND"),
            ("stationid", cdo_station_id),
            ("startdate", target_stamp),
            ("enddate", target_stamp),
            ("units", "standard"),
            ("limit", "1000"),
            ("datatypeid", "TMAX"),
            ("datatypeid", "TMIN"),
            ("datatypeid", "PRCP"),
        ]
        cdo_url = f"{source_url}?{urlencode(cdo_params, doseq=True)}"
        try:
            cdo_status, cdo_payload = http_get_json_with_headers(cdo_url, timeout_seconds, {"token": token})
        except Exception as exc:
            inferred_status = _http_status_from_exception(exc)
            cdo_status = inferred_status or 0
            cdo_payload = {"error": str(exc)}

        if cdo_status == 200 and isinstance(cdo_payload, dict):
            results = cdo_payload.get("results")
            sample: dict[str, Any] = {"date": target_stamp}
            sample_errors: list[dict[str, Any]] = []
            if isinstance(results, list):
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
            _sanitize_temperature_bounds_for_sample(
                sample=sample,
                errors=sample_errors,
                error_context={"date": target_stamp},
            )
            if len(sample) > 1:
                return {
                    "status": "ready" if not sample_errors else "ready_partial",
                    "station_id": clean_station_id,
                    "cdo_station_id": cdo_station_id,
                    "target_date": target_stamp,
                    "data_source": "cdo_api_v2",
                    "source_url": source_url,
                    "http_status": cdo_status,
                    "daily_sample": sample,
                    "errors": sample_errors,
                    "nws_cli_lookup_status": nws_lookup_status,
                    "nws_cli_lookup_error": nws_lookup_error,
                    "nws_cli_http_status": nws_lookup_http_status if nws_lookup_http_status > 0 else None,
                }
            return {
                "status": "no_final_report",
                "station_id": clean_station_id,
                "cdo_station_id": cdo_station_id,
                "target_date": target_stamp,
                "data_source": "cdo_api_v2",
                "source_url": source_url,
                "http_status": cdo_status,
                "daily_sample": {},
                "error": (
                    "No daily summary rows returned for station/date."
                    if not sample_errors
                    else "Daily summary rows failed temperature validation."
                ),
                "errors": sample_errors,
                "nws_cli_lookup_status": nws_lookup_status,
                "nws_cli_lookup_error": nws_lookup_error,
                "nws_cli_http_status": nws_lookup_http_status if nws_lookup_http_status > 0 else None,
            }
        cdo_error = f"CDO request failed (http_status={cdo_status})."
    elif token and not cdo_station_id:
        cdo_error = "No CDO station mapping is configured for this settlement station."
    elif not token:
        cdo_error = "NOAA/NCEI CDO token missing."

    ads_source_url = "https://www.ncei.noaa.gov/access/services/data/v1"
    station_for_ads = cdo_station_id.split(":", 1)[-1].strip() if cdo_station_id else clean_station_id
    if not station_for_ads:
        station_for_ads = cdo_station_id
    ads_params = [
        ("dataset", "daily-summaries"),
        ("stations", station_for_ads),
        ("startDate", target_stamp),
        ("endDate", target_stamp),
        ("dataTypes", "TMAX,TMIN,PRCP"),
        ("format", "json"),
        ("units", "standard"),
        ("includeAttributes", "false"),
        ("includeStationName", "false"),
        ("includeStationLocation", "false"),
    ]
    ads_url = f"{ads_source_url}?{urlencode(ads_params, doseq=True)}"
    try:
        ads_status, ads_payload = http_get_json_with_headers(ads_url, timeout_seconds, None)
    except Exception as exc:
        inferred_status = _http_status_from_exception(exc)
        ads_status = inferred_status or 0
        ads_payload = {"error": str(exc)}
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
            month_value = int(target_stamp[5:7])
            day_value = int(target_stamp[8:10])
            daily_samples, _ = _extract_station_daily_samples_from_rows(
                rows=ads_rows,
                month=month_value,
                day=day_value,
            )
            sample = next((row for row in daily_samples if str(row.get("date") or "") == target_stamp), None)
            if isinstance(sample, dict) and sample:
                return {
                    "status": "ready",
                    "station_id": clean_station_id,
                    "cdo_station_id": cdo_station_id,
                    "target_date": target_stamp,
                    "data_source": "access_data_service_v1",
                    "source_url": ads_source_url,
                    "http_status": ads_status,
                    "daily_sample": sample,
                    "ads_station_id": station_for_ads,
                    "nws_cli_lookup_status": nws_lookup_status,
                    "nws_cli_lookup_error": nws_lookup_error,
                    "nws_cli_http_status": nws_lookup_http_status if nws_lookup_http_status > 0 else None,
                }
            return {
                "status": "no_final_report",
                "station_id": clean_station_id,
                "cdo_station_id": cdo_station_id,
                "target_date": target_stamp,
                "data_source": "access_data_service_v1",
                "source_url": ads_source_url,
                "http_status": ads_status,
                "daily_sample": {},
                "error": "No daily summary rows returned for station/date.",
                "ads_station_id": station_for_ads,
                "nws_cli_lookup_status": nws_lookup_status,
                "nws_cli_lookup_error": nws_lookup_error,
                "nws_cli_http_status": nws_lookup_http_status if nws_lookup_http_status > 0 else None,
            }

    return {
        "status": "request_failed",
        "station_id": clean_station_id,
        "cdo_station_id": cdo_station_id,
        "target_date": target_stamp,
        "source_url": ads_source_url,
        "http_status": ads_status,
        "daily_sample": {},
        "error": cdo_error or f"ADS request failed (http_status={ads_status}).",
        "fallback_http_status": ads_status,
        "ads_station_id": station_for_ads,
        "nws_cli_lookup_status": nws_lookup_status,
        "nws_cli_lookup_error": nws_lookup_error,
        "nws_cli_http_status": nws_lookup_http_status if nws_lookup_http_status > 0 else None,
    }
