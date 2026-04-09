from __future__ import annotations

import csv
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from betbot.dns_guard import urlopen_with_dns_recovery


BytesGetter = Callable[[str, float], tuple[int, bytes, dict[str, str]]]

DEFAULT_METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"

OBSERVATION_FIELDNAMES = [
    "captured_at",
    "station_id",
    "observation_time_utc",
    "timezone_name",
    "local_date",
    "temp_c",
    "raw_text",
    "payload_hash",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def parse_metar_cache_csv_gz(blob_gz: bytes) -> dict[str, Any]:
    if not blob_gz:
        return {
            "status": "empty_payload",
            "rows": [],
            "errors": ["empty_payload"],
        }
    try:
        csv_payload = gzip.decompress(blob_gz)
    except OSError as exc:
        return {
            "status": "invalid_gzip",
            "rows": [],
            "errors": [str(exc)],
        }

    text = csv_payload.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, row in enumerate(reader):
        if not isinstance(row, dict):
            continue
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
        if not station_id or observed is None:
            errors.append(f"row_{index}:missing_station_or_observation_time")
            continue
        temp_text = _pick_row_value(row, ("temp_c", "temperature_c", "temp", "air_temp_c"))
        temp_c = None
        if temp_text:
            try:
                temp_c = float(temp_text)
            except ValueError:
                errors.append(f"row_{index}:invalid_temp_c")
                temp_c = None
        raw_text = _pick_row_value(row, ("raw_text", "raw_ob", "raw", "metar"))
        canonical = {
            "station_id": station_id,
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
            if station and timezone_name and station not in station_timezone:
                station_timezone[station] = timezone_name
    return station_timezone


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
        }
    if not isinstance(payload, dict):
        return {
            "latest_observation_by_station": {},
            "max_temp_c_by_station_local_day": {},
        }
    if not isinstance(payload.get("latest_observation_by_station"), dict):
        payload["latest_observation_by_station"] = {}
    if not isinstance(payload.get("max_temp_c_by_station_local_day"), dict):
        payload["max_temp_c_by_station_local_day"] = {}
    return payload


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _observation_local_date(observation_time_utc: str, timezone_name: str) -> str:
    observed = _parse_iso_datetime(observation_time_utc)
    if observed is None:
        return ""
    try:
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = timezone.utc
    return observed.astimezone(zone).date().isoformat()


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
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}
        state_payload["latest_observation_by_station"] = latest_by_station
    if not isinstance(max_by_station_day, dict):
        max_by_station_day = {}
        state_payload["max_temp_c_by_station_local_day"] = max_by_station_day

    observation_rows: list[dict[str, Any]] = []
    new_station_updates = 0
    for row in parsed_rows:
        if not isinstance(row, dict):
            continue
        station_id = _normalize_text(row.get("station_id")).upper()
        observed_utc = _normalize_text(row.get("observation_time_utc"))
        temp_c = row.get("temp_c")
        timezone_name = station_timezone_map.get(station_id, "UTC")
        local_date = _observation_local_date(observed_utc, timezone_name)

        observation_rows.append(
            {
                "captured_at": captured_at.isoformat(),
                "station_id": station_id,
                "observation_time_utc": observed_utc,
                "timezone_name": timezone_name,
                "local_date": local_date,
                "temp_c": temp_c if isinstance(temp_c, (int, float)) else "",
                "raw_text": _normalize_text(row.get("raw_text")),
                "payload_hash": _normalize_text(row.get("payload_hash")),
            }
        )

        existing = latest_by_station.get(station_id)
        existing_time = _parse_iso_datetime(existing.get("observation_time_utc")) if isinstance(existing, dict) else None
        current_time = _parse_iso_datetime(observed_utc)
        if current_time is not None and (existing_time is None or current_time >= existing_time):
            latest_by_station[station_id] = {
                "observation_time_utc": observed_utc,
                "temp_c": temp_c if isinstance(temp_c, (int, float)) else None,
                "payload_hash": _normalize_text(row.get("payload_hash")),
                "timezone_name": timezone_name,
                "local_date": local_date,
                "captured_at": captured_at.isoformat(),
            }
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

    _write_state(state_path, state_payload)

    observations_csv = out_dir / f"kalshi_temperature_metar_observations_{stamp}.csv"
    _write_observations_csv(observations_csv, observation_rows)

    status = _normalize_text(parsed.get("status")) or "ready"
    if status == "no_rows":
        status = "ready_no_rows"

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
        "parse_errors_count": len(parse_errors),
        "parse_errors": parse_errors[:100],
        "station_updates": new_station_updates,
        "stations_tracked": len(latest_by_station),
        "station_local_day_max_count": len(max_by_station_day),
        "state_file": str(state_path),
        "output_csv": str(observations_csv),
    }
    summary_path = out_dir / f"kalshi_temperature_metar_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary

