from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from betbot.dns_guard import urlopen_with_dns_recovery
from betbot.kalshi_weather_settlement import infer_timezone_from_station


BytesGetter = Callable[[str, float], tuple[int, bytes, dict[str, str]]]

DEFAULT_METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"

OBSERVATION_FIELDNAMES = [
    "captured_at",
    "station_id",
    "report_type",
    "observation_time_utc",
    "timezone_name",
    "local_date",
    "temp_c",
    "raw_text",
    "payload_hash",
]

# Guard against unit/parse corruption (e.g., Fahrenheit-in-C fields) from
# poisoning station-day extrema and downstream weather pattern signals.
MIN_VALID_METAR_TEMP_C = -100.0
MAX_VALID_METAR_TEMP_C = 60.0

# Recency sanity bounds for state/extrema updates. Keep generous to remain
# backward-compatible with normal feed latency while blocking stale/future drift.
MAX_FUTURE_OBSERVATION_SKEW_MINUTES = 15.0
MAX_STALE_OBSERVATION_AGE_HOURS = 72.0

QUALITY_READY_MIN_SCORE = 0.8
QUALITY_GOOD_MIN_SCORE = 0.75
QUALITY_EXCELLENT_MIN_SCORE = 0.9
QUALITY_DEGRADED_MIN_SCORE = 0.4

QUALITY_READY_MAX_PARSE_ERROR_RATE = 0.25
QUALITY_READY_MAX_STALE_OR_FUTURE_RATIO = 0.35
QUALITY_READY_MIN_FRESH_COVERAGE_RATIO = 0.65

QUALITY_SIGNAL_PARSE_ERROR_RATE_WARN = 0.1
QUALITY_SIGNAL_PARSE_ERROR_RATE_CRIT = 0.35
QUALITY_SIGNAL_STALE_OR_FUTURE_WARN = 0.2
QUALITY_SIGNAL_STALE_OR_FUTURE_CRIT = 0.5
QUALITY_SIGNAL_FRESH_COVERAGE_WARN = 0.75
QUALITY_SIGNAL_FRESH_COVERAGE_CRIT = 0.4
QUALITY_SIGNAL_MISSING_STATION_OR_TIME_WARN = 0.05
QUALITY_SIGNAL_MISSING_STATION_OR_TIME_CRIT = 0.2

QUALITY_SCORE_PARSE_ERROR_WEIGHT = 0.45
QUALITY_SCORE_STALE_OR_FUTURE_WEIGHT = 0.3
QUALITY_SCORE_FRESH_COVERAGE_WEIGHT = 0.45

QUALITY_SIGNALS_LIMIT = 20


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _append_quality_signal(signals: list[str], value: str) -> None:
    signal = _normalize_text(value)
    if not signal:
        return
    if signal in signals:
        return
    signals.append(signal)


def _latest_station_entry_is_newer(
    *,
    current_time: datetime | None,
    current_has_temp: bool,
    existing_time: datetime | None,
    existing_has_temp: bool,
) -> bool:
    if current_time is None:
        return False
    if existing_time is None:
        return True
    if current_time > existing_time:
        return True
    if current_time == existing_time and current_has_temp and not existing_has_temp:
        return True
    return False


def _compute_ingest_quality_summary(
    *,
    source_row_count: int,
    rows_parsed: int,
    parse_error_count: int,
    stale_or_future_row_count: int,
    stale_observation_row_count: int,
    future_observation_row_count: int,
    missing_station_or_observation_time_count: int,
    stations_with_observations: int,
    usable_latest_station_count: int,
    fresh_usable_station_count: int,
) -> dict[str, Any]:
    resolved_source_row_count = max(
        int(source_row_count),
        int(rows_parsed) + int(max(0, missing_station_or_observation_time_count)),
    )
    parse_error_rate = _safe_ratio(parse_error_count, rows_parsed)
    stale_or_future_row_ratio = _safe_ratio(stale_or_future_row_count, rows_parsed)
    stale_row_ratio = _safe_ratio(stale_observation_row_count, rows_parsed)
    future_row_ratio = _safe_ratio(future_observation_row_count, rows_parsed)
    fresh_station_coverage_ratio = _safe_ratio(fresh_usable_station_count, stations_with_observations)
    missing_station_or_observation_time_ratio = _safe_ratio(
        missing_station_or_observation_time_count,
        resolved_source_row_count,
    )

    score = 1.0
    score -= _clamp01(parse_error_rate) * QUALITY_SCORE_PARSE_ERROR_WEIGHT
    score -= _clamp01(stale_or_future_row_ratio) * QUALITY_SCORE_STALE_OR_FUTURE_WEIGHT
    score -= _clamp01(1.0 - fresh_station_coverage_ratio) * QUALITY_SCORE_FRESH_COVERAGE_WEIGHT
    quality_score = _clamp01(score)

    quality_signals: list[str] = []
    if rows_parsed <= 0:
        _append_quality_signal(quality_signals, "no_parsed_rows")
    if stations_with_observations <= 0:
        _append_quality_signal(quality_signals, "no_station_observations")
    if usable_latest_station_count <= 0:
        _append_quality_signal(quality_signals, "no_usable_latest_stations")
    if fresh_usable_station_count <= 0:
        _append_quality_signal(quality_signals, "no_fresh_usable_stations")

    if parse_error_rate >= QUALITY_SIGNAL_PARSE_ERROR_RATE_CRIT:
        _append_quality_signal(quality_signals, "parse_error_rate_critical")
    elif parse_error_rate >= QUALITY_SIGNAL_PARSE_ERROR_RATE_WARN:
        _append_quality_signal(quality_signals, "parse_error_rate_elevated")

    if missing_station_or_observation_time_ratio >= QUALITY_SIGNAL_MISSING_STATION_OR_TIME_CRIT:
        _append_quality_signal(quality_signals, "missing_station_or_observation_time_ratio_critical")
    elif missing_station_or_observation_time_ratio >= QUALITY_SIGNAL_MISSING_STATION_OR_TIME_WARN:
        _append_quality_signal(quality_signals, "missing_station_or_observation_time_ratio_elevated")

    if stale_or_future_row_ratio >= QUALITY_SIGNAL_STALE_OR_FUTURE_CRIT:
        _append_quality_signal(quality_signals, "stale_or_future_ratio_critical")
    elif stale_or_future_row_ratio >= QUALITY_SIGNAL_STALE_OR_FUTURE_WARN:
        _append_quality_signal(quality_signals, "stale_or_future_ratio_elevated")

    if fresh_station_coverage_ratio <= QUALITY_SIGNAL_FRESH_COVERAGE_CRIT:
        _append_quality_signal(quality_signals, "fresh_station_coverage_critical")
    elif fresh_station_coverage_ratio <= QUALITY_SIGNAL_FRESH_COVERAGE_WARN:
        _append_quality_signal(quality_signals, "fresh_station_coverage_low")

    quality_status = "degraded"
    if rows_parsed <= 0 or usable_latest_station_count <= 0:
        quality_status = "blocked"
    elif (
        quality_score >= QUALITY_READY_MIN_SCORE
        and parse_error_rate <= QUALITY_READY_MAX_PARSE_ERROR_RATE
        and stale_or_future_row_ratio <= QUALITY_READY_MAX_STALE_OR_FUTURE_RATIO
        and fresh_station_coverage_ratio >= QUALITY_READY_MIN_FRESH_COVERAGE_RATIO
    ):
        quality_status = "ready"
    elif quality_score < QUALITY_DEGRADED_MIN_SCORE:
        quality_status = "blocked"

    quality_grade = "degraded"
    if quality_status == "blocked":
        quality_grade = "critical"
    elif quality_status == "ready" and quality_score >= QUALITY_EXCELLENT_MIN_SCORE:
        quality_grade = "excellent"
    elif quality_score >= QUALITY_GOOD_MIN_SCORE:
        quality_grade = "good"

    missing_station_or_observation_time_warn = (
        missing_station_or_observation_time_ratio >= QUALITY_SIGNAL_MISSING_STATION_OR_TIME_WARN
    )
    missing_station_or_observation_time_critical = (
        missing_station_or_observation_time_ratio >= QUALITY_SIGNAL_MISSING_STATION_OR_TIME_CRIT
    )
    timestamp_leakage_warn = stale_or_future_row_ratio >= QUALITY_SIGNAL_STALE_OR_FUTURE_WARN
    timestamp_leakage_critical = stale_or_future_row_ratio >= QUALITY_SIGNAL_STALE_OR_FUTURE_CRIT
    fresh_station_coverage_low = fresh_station_coverage_ratio <= QUALITY_SIGNAL_FRESH_COVERAGE_WARN
    fresh_station_coverage_critical = fresh_station_coverage_ratio <= QUALITY_SIGNAL_FRESH_COVERAGE_CRIT

    missing_station_or_observation_time_status = "healthy"
    if missing_station_or_observation_time_critical:
        missing_station_or_observation_time_status = "critical"
    elif missing_station_or_observation_time_warn:
        missing_station_or_observation_time_status = "elevated"

    timestamp_leakage_status = "healthy"
    if timestamp_leakage_critical:
        timestamp_leakage_status = "critical"
    elif timestamp_leakage_warn:
        timestamp_leakage_status = "elevated"

    fresh_station_coverage_status = "healthy"
    if fresh_station_coverage_critical:
        fresh_station_coverage_status = "critical"
    elif fresh_station_coverage_low:
        fresh_station_coverage_status = "low"

    leakage_status = "healthy"
    if (
        missing_station_or_observation_time_critical
        or timestamp_leakage_critical
        or fresh_station_coverage_critical
    ):
        leakage_status = "critical"
    elif (
        missing_station_or_observation_time_warn
        or timestamp_leakage_warn
        or fresh_station_coverage_low
    ):
        leakage_status = "elevated"

    leakage_flags: list[str] = []
    if missing_station_or_observation_time_warn:
        leakage_flags.append(
            "missing_station_or_observation_time_ratio_critical"
            if missing_station_or_observation_time_critical
            else "missing_station_or_observation_time_ratio_elevated"
        )
    if timestamp_leakage_warn:
        leakage_flags.append(
            "timestamp_leakage_ratio_critical"
            if timestamp_leakage_critical
            else "timestamp_leakage_ratio_elevated"
        )
    if fresh_station_coverage_low:
        leakage_flags.append(
            "fresh_station_coverage_critical"
            if fresh_station_coverage_critical
            else "fresh_station_coverage_low"
        )

    bounded_signals = quality_signals[:QUALITY_SIGNALS_LIMIT]
    return {
        "source_row_count": int(resolved_source_row_count),
        "quality_score": round(float(quality_score), 6),
        "quality_grade": quality_grade,
        "quality_status": quality_status,
        "quality_signal_count": len(bounded_signals),
        "quality_signals": bounded_signals,
        "parse_error_rate": round(float(parse_error_rate), 6),
        "missing_station_or_observation_time_count": int(max(0, missing_station_or_observation_time_count)),
        "missing_station_or_observation_time_ratio": round(
            float(missing_station_or_observation_time_ratio),
            6,
        ),
        "missing_station_or_observation_time_warn_ratio": float(
            QUALITY_SIGNAL_MISSING_STATION_OR_TIME_WARN
        ),
        "missing_station_or_observation_time_critical_ratio": float(
            QUALITY_SIGNAL_MISSING_STATION_OR_TIME_CRIT
        ),
        "missing_station_or_observation_time_warn": bool(missing_station_or_observation_time_warn),
        "missing_station_or_observation_time_critical": bool(
            missing_station_or_observation_time_critical
        ),
        "missing_station_or_observation_time_detected": bool(
            int(max(0, missing_station_or_observation_time_count)) > 0
        ),
        "missing_station_or_observation_time_status": missing_station_or_observation_time_status,
        "stale_or_future_row_count": int(max(0, stale_or_future_row_count)),
        "timestamp_leakage_row_count": int(max(0, stale_or_future_row_count)),
        "timestamp_leakage_detected": bool(int(max(0, stale_or_future_row_count)) > 0),
        "stale_observation_row_count": int(max(0, stale_observation_row_count)),
        "future_observation_row_count": int(max(0, future_observation_row_count)),
        "fresh_station_coverage_ratio": round(float(fresh_station_coverage_ratio), 6),
        "fresh_usable_station_count": int(max(0, fresh_usable_station_count)),
        "fresh_station_coverage_warn_ratio": float(QUALITY_SIGNAL_FRESH_COVERAGE_WARN),
        "fresh_station_coverage_critical_ratio": float(QUALITY_SIGNAL_FRESH_COVERAGE_CRIT),
        "fresh_station_coverage_low": bool(fresh_station_coverage_low),
        "fresh_station_coverage_critical": bool(fresh_station_coverage_critical),
        "fresh_station_coverage_degraded": bool(fresh_station_coverage_low),
        "fresh_station_coverage_status": fresh_station_coverage_status,
        "usable_latest_station_count": int(usable_latest_station_count),
        "stale_or_future_row_ratio": round(float(stale_or_future_row_ratio), 6),
        "stale_row_ratio": round(float(stale_row_ratio), 6),
        "future_row_ratio": round(float(future_row_ratio), 6),
        "timestamp_leakage_warn_ratio": float(QUALITY_SIGNAL_STALE_OR_FUTURE_WARN),
        "timestamp_leakage_critical_ratio": float(QUALITY_SIGNAL_STALE_OR_FUTURE_CRIT),
        "timestamp_leakage_warn": bool(timestamp_leakage_warn),
        "timestamp_leakage_critical": bool(timestamp_leakage_critical),
        "timestamp_leakage_status": timestamp_leakage_status,
        "leakage_status": leakage_status,
        "leakage_flag_count": len(leakage_flags),
        "leakage_flags": leakage_flags,
    }


def _http_get_bytes(url: str, timeout_seconds: float) -> tuple[int, bytes, dict[str, str]]:
    request = Request(
        url,
        headers={
            "Accept": "application/gzip, text/csv, */*;q=0.8",
            "User-Agent": "betbot-temperature-metar-ingest/1.0",
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


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    candidates = [
        text.replace("Z", "+00:00"),
        text.replace(" UTC", "+00:00"),
    ]
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        return parsed
    return None


def _pick_row_value(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key not in row:
            continue
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _infer_report_type(*, row: dict[str, Any], raw_text: str) -> str:
    explicit = _pick_row_value(row, ("report_type", "reportType", "metar_type", "report"))
    if explicit:
        normalized = explicit.upper()
        if normalized in {"METAR", "SPECI"}:
            return normalized
    stripped_raw = _normalize_text(raw_text).upper()
    if stripped_raw.startswith("SPECI "):
        return "SPECI"
    if stripped_raw.startswith("METAR "):
        return "METAR"
    return ""


def parse_metar_cache_csv_gz(blob_gz: bytes) -> dict[str, Any]:
    if not blob_gz:
        return {
            "status": "empty_payload",
            "rows": [],
            "errors": ["empty_payload"],
            "source_row_count": 0,
            "diagnostics": {
                "source_row_count": 0,
                "dropped_row_count": 0,
                "missing_station_count": 0,
                "missing_observation_time_count": 0,
                "missing_station_or_observation_time_count": 0,
                "invalid_temp_count": 0,
            },
        }
    try:
        csv_payload = gzip.decompress(blob_gz)
    except OSError as exc:
        return {
            "status": "invalid_gzip",
            "rows": [],
            "errors": [str(exc)],
            "source_row_count": 0,
            "diagnostics": {
                "source_row_count": 0,
                "dropped_row_count": 0,
                "missing_station_count": 0,
                "missing_observation_time_count": 0,
                "missing_station_or_observation_time_count": 0,
                "invalid_temp_count": 0,
            },
        }

    text = csv_payload.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    diagnostics = {
        "source_row_count": 0,
        "dropped_row_count": 0,
        "missing_station_count": 0,
        "missing_observation_time_count": 0,
        "missing_station_or_observation_time_count": 0,
        "invalid_temp_count": 0,
    }
    for index, row in enumerate(reader):
        if not isinstance(row, dict):
            continue
        diagnostics["source_row_count"] += 1
        station_id = _pick_row_value(
            row,
            (
                "station_id",
                "station",
                "icao_id",
                "icaoId",
                "icao",
            ),
        ).upper()
        observed_text = _pick_row_value(
            row,
            (
                "observation_time",
                "obs_time",
                "valid",
                "observed",
            ),
        )
        observed = _parse_iso_datetime(observed_text)
        if not station_id:
            diagnostics["missing_station_count"] += 1
        if observed is None:
            diagnostics["missing_observation_time_count"] += 1
        if not station_id or observed is None:
            diagnostics["missing_station_or_observation_time_count"] += 1
            diagnostics["dropped_row_count"] += 1
            errors.append(f"row_{index}:missing_station_or_observation_time")
            continue
        temp_text = _pick_row_value(row, ("temp_c", "temperature_c", "temp", "air_temp_c"))
        temp_c = None
        if temp_text:
            try:
                parsed_temp = float(temp_text)
                if not math.isfinite(parsed_temp):
                    raise ValueError("non_finite_temp_c")
                if parsed_temp < MIN_VALID_METAR_TEMP_C or parsed_temp > MAX_VALID_METAR_TEMP_C:
                    raise ValueError("out_of_range_temp_c")
                temp_c = parsed_temp
            except (TypeError, ValueError):
                diagnostics["invalid_temp_count"] += 1
                errors.append(f"row_{index}:invalid_temp_c")
                temp_c = None
        raw_text = _pick_row_value(row, ("raw_text", "raw_ob", "raw", "metar"))
        report_type = _infer_report_type(row=row, raw_text=raw_text)
        canonical = {
            "station_id": station_id,
            "report_type": report_type,
            "observation_time_utc": observed.isoformat(),
            "temp_c": temp_c,
            "raw_text": raw_text,
        }
        payload_hash = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode("utf-8")).hexdigest()
        canonical["payload_hash"] = payload_hash
        rows.append(canonical)

    status = "ready"
    if not rows:
        status = "no_rows"
    elif errors:
        status = "ready_partial"
    return {
        "status": status,
        "rows": rows,
        "errors": errors,
        "source_row_count": int(diagnostics["source_row_count"]),
        "diagnostics": diagnostics,
    }


def _find_latest_specs_csv(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob("kalshi_temperature_contract_specs_*.csv"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _load_station_timezone_map(specs_csv: str | None, output_dir: str) -> dict[str, str]:
    resolved = _normalize_text(specs_csv) or _find_latest_specs_csv(output_dir)
    if not resolved:
        return {}
    path = Path(resolved)
    if not path.exists():
        return {}
    station_timezone: dict[str, str] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            station = _normalize_text(row.get("settlement_station")).upper()
            timezone_name = _normalize_text(row.get("settlement_timezone"))
            if not station:
                continue
            resolved_timezone = timezone_name or infer_timezone_from_station(station)
            if resolved_timezone and station not in station_timezone:
                station_timezone[station] = resolved_timezone
    return station_timezone


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
            "min_temp_c_by_station_local_day": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
            "min_temp_c_by_station_local_day": {},
        }
    if not isinstance(payload, dict):
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
            "min_temp_c_by_station_local_day": {},
        }
    if not isinstance(payload.get("latest_observation_by_station"), dict):
        payload["latest_observation_by_station"] = {}
    if not isinstance(payload.get("max_temp_c_by_station_local_day"), dict):
        payload["max_temp_c_by_station_local_day"] = {}
    if not isinstance(payload.get("min_temp_c_by_station_local_day"), dict):
        payload["min_temp_c_by_station_local_day"] = {}
    if not isinstance(payload.get("station_observation_interval_stats"), dict):
        payload["station_observation_interval_stats"] = {}
    return payload


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    rank = max(0.0, min(1.0, float(p))) * (len(values) - 1)
    lower = int(rank)
    upper = min(len(values) - 1, lower + 1)
    if lower == upper:
        return float(values[lower])
    weight = rank - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def _observation_local_date(observation_time_utc: str, timezone_name: str) -> str:
    observed = _parse_iso_datetime(observation_time_utc)
    if observed is None:
        return ""
    try:
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = timezone.utc
    local_timestamp = observed.astimezone(zone)
    # Kalshi weather resolution references local standard time; when DST is in
    # effect, shift back by the DST delta so day buckets align with settlement.
    dst_delta = local_timestamp.dst()
    if dst_delta and dst_delta.total_seconds() > 0:
        local_timestamp = local_timestamp - dst_delta
    return local_timestamp.date().isoformat()


def _write_observations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OBSERVATION_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_temperature_metar_ingest(
    *,
    output_dir: str = "outputs",
    specs_csv: str | None = None,
    cache_url: str = DEFAULT_METAR_CACHE_URL,
    timeout_seconds: float = 20.0,
    http_get_bytes: BytesGetter = _http_get_bytes,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    status_code, blob_gz, response_headers = http_get_bytes(str(cache_url), float(timeout_seconds))
    if status_code != 200:
        return {
            "status": "request_failed",
            "cache_url": str(cache_url),
            "http_status": status_code,
            "error": "METAR cache request failed.",
        }

    raw_sha256 = hashlib.sha256(blob_gz).hexdigest()
    parsed = parse_metar_cache_csv_gz(blob_gz)
    parsed_rows = parsed.get("rows") if isinstance(parsed.get("rows"), list) else []
    parse_errors = parsed.get("errors") if isinstance(parsed.get("errors"), list) else []
    processing_errors: list[str] = []

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")

    raw_dir = out_dir / "kalshi_temperature_metar_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"metars_cache_{stamp}_{raw_sha256[:10]}.csv.gz"
    raw_path.write_bytes(blob_gz)

    station_timezone_map = _load_station_timezone_map(specs_csv, output_dir)
    state_path = out_dir / "kalshi_temperature_metar_state.json"
    state_payload = _load_state(state_path)
    latest_by_station = state_payload.get("latest_observation_by_station")
    max_by_station_day = state_payload.get("max_temp_c_by_station_local_day")
    min_by_station_day = state_payload.get("min_temp_c_by_station_local_day")
    station_interval_stats = state_payload.get("station_observation_interval_stats")
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}
        state_payload["latest_observation_by_station"] = latest_by_station
    if not isinstance(max_by_station_day, dict):
        max_by_station_day = {}
        state_payload["max_temp_c_by_station_local_day"] = max_by_station_day
    if not isinstance(min_by_station_day, dict):
        min_by_station_day = {}
        state_payload["min_temp_c_by_station_local_day"] = min_by_station_day
    if not isinstance(station_interval_stats, dict):
        station_interval_stats = {}
        state_payload["station_observation_interval_stats"] = station_interval_stats

    observation_rows: list[dict[str, Any]] = []
    new_station_updates = 0
    stale_or_future_row_count = 0
    stale_observation_row_count = 0
    future_observation_row_count = 0
    stations_with_observations: set[str] = set()
    latest_quality_by_station: dict[str, dict[str, Any]] = {}
    for row_index, row in enumerate(parsed_rows):
        if not isinstance(row, dict):
            continue
        station_id = _normalize_text(row.get("station_id")).upper()
        observed_utc = _normalize_text(row.get("observation_time_utc"))
        temp_c = row.get("temp_c")
        current_has_temp = isinstance(temp_c, (int, float))
        if station_id:
            stations_with_observations.add(station_id)
        existing = latest_by_station.get(station_id)
        existing_timezone_name = _normalize_text(existing.get("timezone_name")) if isinstance(existing, dict) else ""
        timezone_name = (
            station_timezone_map.get(station_id)
            or existing_timezone_name
            or infer_timezone_from_station(station_id)
            or "UTC"
        )
        local_date = _observation_local_date(observed_utc, timezone_name)
        current_time = _parse_iso_datetime(observed_utc)

        is_stale = False
        is_future = False
        if current_time is not None:
            max_future_time = captured_at + timedelta(minutes=MAX_FUTURE_OBSERVATION_SKEW_MINUTES)
            min_fresh_time = captured_at - timedelta(hours=MAX_STALE_OBSERVATION_AGE_HOURS)
            if current_time > max_future_time:
                is_future = True
                future_observation_row_count += 1
                processing_errors.append(f"row_{row_index}:future_observation_time")
            elif current_time < min_fresh_time:
                is_stale = True
                stale_observation_row_count += 1
                processing_errors.append(f"row_{row_index}:stale_observation_time")
        if is_stale or is_future:
            stale_or_future_row_count += 1

        latest_quality = latest_quality_by_station.get(station_id)
        latest_quality_time = latest_quality.get("time") if isinstance(latest_quality, dict) else None
        latest_quality_has_temp = bool(latest_quality.get("has_temp")) if isinstance(latest_quality, dict) else False
        if _latest_station_entry_is_newer(
            current_time=current_time,
            current_has_temp=current_has_temp,
            existing_time=latest_quality_time if isinstance(latest_quality_time, datetime) else None,
            existing_has_temp=latest_quality_has_temp,
        ):
            latest_quality_by_station[station_id] = {
                "time": current_time,
                "has_temp": current_has_temp,
                "is_fresh": not (is_stale or is_future),
            }

        observation_rows.append(
            {
                "captured_at": captured_at.isoformat(),
                "station_id": station_id,
                "report_type": _normalize_text(row.get("report_type")),
                "observation_time_utc": observed_utc,
                "timezone_name": timezone_name,
                "local_date": local_date,
                "temp_c": temp_c if isinstance(temp_c, (int, float)) else "",
                "raw_text": _normalize_text(row.get("raw_text")),
                "payload_hash": _normalize_text(row.get("payload_hash")),
            }
        )

        if is_stale or is_future:
            continue

        existing = latest_by_station.get(station_id)
        existing_time = _parse_iso_datetime(existing.get("observation_time_utc")) if isinstance(existing, dict) else None
        existing_has_temp = isinstance(existing.get("temp_c"), (int, float)) if isinstance(existing, dict) else False
        should_update_latest = False
        if current_time is not None:
            if existing_time is None or current_time > existing_time:
                # Do not advance latest observation timestamps when temperature is
                # missing/invalid; freshness must represent latest usable weather.
                if current_has_temp:
                    should_update_latest = True
                elif existing_time is not None and existing_has_temp:
                    processing_errors.append(f"row_{row_index}:latest_update_skipped_missing_temp_c")
            elif existing_time is not None and current_time == existing_time:
                # Fail closed on duplicate timestamp downgrades: do not allow a
                # malformed/no-temp duplicate to clobber a valid latest temp.
                if existing_has_temp and not current_has_temp:
                    should_update_latest = False
                else:
                    should_update_latest = True
        if should_update_latest:
            previous_observation_time = (
                _normalize_text(existing.get("observation_time_utc")) if isinstance(existing, dict) else ""
            )
            previous_temp_c = (
                existing.get("temp_c")
                if isinstance(existing, dict) and isinstance(existing.get("temp_c"), (int, float))
                else None
            )
            previous_report_type = (
                _normalize_text(existing.get("report_type")) if isinstance(existing, dict) else ""
            )
            previous_payload_hash = (
                _normalize_text(existing.get("payload_hash")) if isinstance(existing, dict) else ""
            )
            previous_local_date = (
                _normalize_text(existing.get("local_date")) if isinstance(existing, dict) else ""
            )
            if existing_time is not None and current_time > existing_time:
                interval_minutes = (current_time - existing_time).total_seconds() / 60.0
                # Ignore pathological long gaps so adaptive freshness does not
                # balloon from outages or sparse historical backfills.
                if 1.0 <= interval_minutes <= 180.0:
                    stats_payload = station_interval_stats.get(station_id)
                    if not isinstance(stats_payload, dict):
                        stats_payload = {}
                    raw_recent = stats_payload.get("recent_interval_minutes")
                    recent: list[float] = []
                    if isinstance(raw_recent, list):
                        for value in raw_recent:
                            if isinstance(value, (int, float)):
                                recent.append(float(value))
                    recent.append(float(interval_minutes))
                    recent = recent[-16:]
                    ordered = sorted(recent)
                    median = _percentile(ordered, 0.5)
                    p90 = _percentile(ordered, 0.9)
                    stats_payload.update(
                        {
                            "sample_count": len(recent),
                            "latest_interval_minutes": round(float(interval_minutes), 3),
                            "interval_median_minutes": (
                                round(float(median), 3) if isinstance(median, (int, float)) else None
                            ),
                            "interval_p90_minutes": (
                                round(float(p90), 3) if isinstance(p90, (int, float)) else None
                            ),
                            "recent_interval_minutes": [round(float(value), 3) for value in recent],
                            "updated_at": captured_at.isoformat(),
                        }
                    )
                    station_interval_stats[station_id] = stats_payload
            latest_by_station_entry: dict[str, Any] = {
                "observation_time_utc": observed_utc,
                "temp_c": temp_c if isinstance(temp_c, (int, float)) else None,
                "report_type": _normalize_text(row.get("report_type")),
                "payload_hash": _normalize_text(row.get("payload_hash")),
                "timezone_name": timezone_name,
                "local_date": local_date,
                "captured_at": captured_at.isoformat(),
            }
            if previous_observation_time:
                latest_by_station_entry["previous_observation_time_utc"] = previous_observation_time
            if previous_temp_c is not None:
                latest_by_station_entry["previous_temp_c"] = round(float(previous_temp_c), 3)
            if previous_report_type:
                latest_by_station_entry["previous_report_type"] = previous_report_type
            if previous_payload_hash:
                latest_by_station_entry["previous_payload_hash"] = previous_payload_hash
            if previous_local_date:
                latest_by_station_entry["previous_local_date"] = previous_local_date
            latest_by_station[station_id] = latest_by_station_entry
            new_station_updates += 1

        if isinstance(temp_c, (int, float)) and local_date:
            max_key = f"{station_id}|{local_date}"
            existing_max = max_by_station_day.get(max_key)
            existing_max_value = None
            try:
                if existing_max is not None:
                    existing_max_value = float(existing_max)
            except (TypeError, ValueError):
                existing_max_value = None
            if existing_max_value is None or float(temp_c) > existing_max_value:
                max_by_station_day[max_key] = round(float(temp_c), 3)
            existing_min = min_by_station_day.get(max_key)
            existing_min_value = None
            try:
                if existing_min is not None:
                    existing_min_value = float(existing_min)
            except (TypeError, ValueError):
                existing_min_value = None
            if existing_min_value is None or float(temp_c) < existing_min_value:
                min_by_station_day[max_key] = round(float(temp_c), 3)

    _write_state(state_path, state_payload)

    observations_csv = out_dir / f"kalshi_temperature_metar_observations_{stamp}.csv"
    _write_observations_csv(observations_csv, observation_rows)

    combined_errors = parse_errors + processing_errors

    status = _normalize_text(parsed.get("status")) or "ready"
    if status == "ready" and combined_errors:
        status = "ready_partial"
    if status == "no_rows":
        status = "ready_no_rows"

    usable_latest_station_count = 0
    fresh_usable_station_count = 0
    for latest_station_payload in latest_quality_by_station.values():
        if not isinstance(latest_station_payload, dict):
            continue
        if bool(latest_station_payload.get("has_temp")):
            usable_latest_station_count += 1
            if bool(latest_station_payload.get("is_fresh")):
                fresh_usable_station_count += 1

    parsed_diagnostics = parsed.get("diagnostics")
    if not isinstance(parsed_diagnostics, dict):
        parsed_diagnostics = {}
    source_row_count = int(parsed.get("source_row_count") or 0)
    if source_row_count <= 0:
        source_row_count = len(parsed_rows) + int(
            parsed_diagnostics.get("missing_station_or_observation_time_count") or 0
        )
    quality_summary = _compute_ingest_quality_summary(
        source_row_count=source_row_count,
        rows_parsed=len(parsed_rows),
        parse_error_count=len(parse_errors),
        stale_or_future_row_count=stale_or_future_row_count,
        stale_observation_row_count=stale_observation_row_count,
        future_observation_row_count=future_observation_row_count,
        missing_station_or_observation_time_count=int(
            parsed_diagnostics.get("missing_station_or_observation_time_count") or 0
        ),
        stations_with_observations=len(stations_with_observations),
        usable_latest_station_count=usable_latest_station_count,
        fresh_usable_station_count=fresh_usable_station_count,
    )

    summary = {
        "status": status,
        "captured_at": captured_at.isoformat(),
        "cache_url": str(cache_url),
        "http_status": status_code,
        "response_headers": response_headers,
        "raw_sha256": raw_sha256,
        "raw_snapshot": str(raw_path),
        "specs_csv_used": _normalize_text(specs_csv) or _find_latest_specs_csv(output_dir),
        "station_timezone_mappings": len(station_timezone_map),
        "rows_parsed": len(parsed_rows),
        "rows_emitted": len(observation_rows),
        "parse_errors_count": len(combined_errors),
        "parse_errors": combined_errors[:100],
        "missing_station_count": int(parsed_diagnostics.get("missing_station_count") or 0),
        "missing_observation_time_count": int(parsed_diagnostics.get("missing_observation_time_count") or 0),
        "raw_rows_missing_station_or_observation_time_count": int(
            parsed_diagnostics.get("missing_station_or_observation_time_count") or 0
        ),
        "station_updates": new_station_updates,
        "stations_tracked": len(latest_by_station),
        "station_interval_stats_count": len(station_interval_stats),
        "station_local_day_max_count": len(max_by_station_day),
        "station_local_day_min_count": len(min_by_station_day),
        "state_file": str(state_path),
        "output_csv": str(observations_csv),
        **quality_summary,
    }
    summary_path = out_dir / f"kalshi_temperature_metar_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
