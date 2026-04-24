from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
from typing import Any


_PROFITABILITY_SUMMARY_PATTERNS = (
    "health/kalshi_temperature_profitability_summary_latest.json",
    "health/kalshi_temperature_profitability_summary_*.json",
    "checkpoints/profitability_*_latest.json",
    "checkpoints/profitability_*.json",
    "kalshi_temperature_profitability_summary_latest.json",
    "kalshi_temperature_profitability_summary_*.json",
)
_CONSTRAINT_SCAN_PATTERNS = (
    "kalshi_temperature_constraint_scan_latest.csv",
    "kalshi_temperature_constraint_scan_*.csv",
    "health/kalshi_temperature_constraint_scan_latest.csv",
    "health/kalshi_temperature_constraint_scan_*.csv",
)
_STAMP_SUFFIX_RE = re.compile(r"(?:^|_)(\d{8}_\d{6})$")
_DIMENSION_KEYS = ("settlement_station", "local_hour", "signal_bucket")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not (parsed == parsed and abs(parsed) != float("inf")):
        return None
    return float(parsed)


def _safe_int(value: Any) -> int:
    parsed = _safe_float(value)
    if parsed is None:
        return 0
    return int(round(parsed))


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_station(value: Any) -> str:
    text = _normalize_text(value).upper()
    return text if text else "UNKNOWN"


def _normalize_hour(value: Any) -> str:
    parsed = _safe_int(value)
    if 0 <= parsed <= 23 and _normalize_text(value):
        return str(parsed)
    text = _normalize_text(value).lower()
    return text if text else "unknown"


def _normalize_signal_bucket(value: Any) -> str:
    text = _normalize_text(value).lower()
    return text if text else "unknown"


def _artifact_timestamp_utc(path: Path) -> datetime | None:
    match = _STAMP_SUFFIX_RE.search(path.stem)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _artifact_epoch(path: Path) -> float:
    stamped = _artifact_timestamp_utc(path)
    if isinstance(stamped, datetime):
        return stamped.timestamp()
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    tmp_path = path.with_name(tmp_name)
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _write_csv_atomic(path: Path, *, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    tmp_path = path.with_name(tmp_name)
    try:
        with tmp_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _find_latest_matching_file(output_dir: Path, patterns: tuple[str, ...]) -> Path | None:
    seen: set[str] = set()
    candidates: list[Path] = []
    for pattern in patterns:
        for path in output_dir.glob(pattern):
            if not path.is_file():
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda item: (_artifact_epoch(item), item.name))


def _find_all_matching_files(output_dir: Path, patterns: tuple[str, ...]) -> list[Path]:
    seen: set[str] = set()
    candidates: list[Path] = []
    for pattern in patterns:
        for path in output_dir.glob(pattern):
            if not path.is_file():
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
    candidates.sort(key=lambda item: (_artifact_epoch(item), item.name))
    return candidates


def _resolve_optional_path(raw_path: str, *, output_dir: Path, summary_path: Path) -> str:
    text = _normalize_text(raw_path)
    if not text:
        return ""
    candidate = Path(text)
    if candidate.is_file():
        return str(candidate)
    candidate_from_output = output_dir / text
    if candidate_from_output.is_file():
        return str(candidate_from_output)
    candidate_from_summary = summary_path.parent / text
    if candidate_from_summary.is_file():
        return str(candidate_from_summary)
    return text


def _count_dimension_buckets(regime_payload: dict[str, Any], *, key: str) -> int:
    dimension_regimes = regime_payload.get("dimension_regimes")
    if not isinstance(dimension_regimes, dict):
        return 0
    bucket_map = dimension_regimes.get(key)
    if not isinstance(bucket_map, dict):
        return 0
    return len([name for name in bucket_map if _normalize_text(name)])


def _extract_combined_buckets(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    regime_breakdown = payload.get("regime_breakdown")
    if not isinstance(regime_breakdown, dict):
        return {}
    raw_combined = regime_breakdown.get("combined_regimes")
    if not isinstance(raw_combined, dict):
        return {}

    combined: dict[str, dict[str, Any]] = {}
    for raw_entry in raw_combined.values():
        if not isinstance(raw_entry, dict):
            continue
        station = _normalize_station(raw_entry.get("settlement_station"))
        local_hour = _normalize_hour(raw_entry.get("local_hour"))
        signal_bucket = _normalize_signal_bucket(raw_entry.get("signal_bucket"))
        bucket_key = "|".join((station, local_hour, signal_bucket))
        entry = combined.setdefault(
            bucket_key,
            {
                "bucket_key": bucket_key,
                "settlement_station": station,
                "local_hour": local_hour,
                "signal_bucket": signal_bucket,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "realized_pnl_sum": 0.0,
            },
        )
        entry["trades"] = int(entry["trades"]) + max(0, _safe_int(raw_entry.get("trades")))
        entry["wins"] = int(entry["wins"]) + max(0, _safe_int(raw_entry.get("wins")))
        entry["losses"] = int(entry["losses"]) + max(0, _safe_int(raw_entry.get("losses")))
        entry["pushes"] = int(entry["pushes"]) + max(0, _safe_int(raw_entry.get("pushes")))
        entry["realized_pnl_sum"] = round(
            float(entry["realized_pnl_sum"]) + float(_safe_float(raw_entry.get("realized_pnl_sum")) or 0.0),
            6,
        )
    return combined


def _extract_record_from_profitability_summary(
    *,
    output_dir: Path,
    summary_path: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    captured_at = (
        _parse_iso_datetime(payload.get("captured_at"))
        or _artifact_timestamp_utc(summary_path)
        or datetime.fromtimestamp(_artifact_epoch(summary_path), tz=timezone.utc)
    )
    regime_breakdown = payload.get("regime_breakdown")
    if not isinstance(regime_breakdown, dict):
        regime_breakdown = {}

    combined_buckets = _extract_combined_buckets(payload)
    station_bucket_count = _count_dimension_buckets(regime_breakdown, key="settlement_station")
    local_hour_bucket_count = _count_dimension_buckets(regime_breakdown, key="local_hour")
    signal_bucket_count = _count_dimension_buckets(regime_breakdown, key="signal_bucket")

    if station_bucket_count <= 0 and combined_buckets:
        station_bucket_count = len({str(item["settlement_station"]) for item in combined_buckets.values()})
    if local_hour_bucket_count <= 0 and combined_buckets:
        local_hour_bucket_count = len({str(item["local_hour"]) for item in combined_buckets.values()})
    if signal_bucket_count <= 0 and combined_buckets:
        signal_bucket_count = len({str(item["signal_bucket"]) for item in combined_buckets.values()})

    settled_outcomes = max(
        0,
        max(
            _safe_int(payload.get("orders_settled_with_numeric_pnl")),
            _safe_int(payload.get("orders_settled")),
            _safe_int(payload.get("matched_settled_orders")),
        ),
    )
    return {
        "source": str(summary_path),
        "captured_at": captured_at,
        "captured_at_iso": captured_at.isoformat(),
        "settled_outcomes": int(settled_outcomes),
        "station_bucket_count": int(station_bucket_count),
        "local_hour_bucket_count": int(local_hour_bucket_count),
        "signal_bucket_count": int(signal_bucket_count),
        "combined_bucket_count": int(len(combined_buckets)),
        "combined_buckets": combined_buckets,
        "settled_csv": _resolve_optional_path(
            _normalize_text(payload.get("output_csv")),
            output_dir=output_dir,
            summary_path=summary_path,
        ),
    }


def _build_growth_deltas(records: list[dict[str, Any]], *, latest: dict[str, Any]) -> dict[str, Any]:
    if not records:
        return {}
    latest_captured_at = latest.get("captured_at")
    if not isinstance(latest_captured_at, datetime):
        return {}

    def _select_baseline(hours: int) -> dict[str, Any] | None:
        cutoff = latest_captured_at - timedelta(hours=hours)
        eligible = [
            row
            for row in records
            if isinstance(row.get("captured_at"), datetime) and row["captured_at"] <= cutoff
        ]
        if not eligible:
            return None
        return max(eligible, key=lambda row: row["captured_at"])

    baseline_24h = _select_baseline(24)
    baseline_7d = _select_baseline(7 * 24)

    def _delta(metric: str, baseline: dict[str, Any] | None) -> int | None:
        if not isinstance(baseline, dict):
            return None
        return int(_safe_int(latest.get(metric)) - _safe_int(baseline.get(metric)))

    return {
        "baseline_24h_source": _normalize_text(baseline_24h.get("source")) if isinstance(baseline_24h, dict) else "",
        "baseline_7d_source": _normalize_text(baseline_7d.get("source")) if isinstance(baseline_7d, dict) else "",
        "settled_outcomes_delta_24h": _delta("settled_outcomes", baseline_24h),
        "settled_outcomes_delta_7d": _delta("settled_outcomes", baseline_7d),
        "station_bucket_count_delta_24h": _delta("station_bucket_count", baseline_24h),
        "station_bucket_count_delta_7d": _delta("station_bucket_count", baseline_7d),
        "local_hour_bucket_count_delta_24h": _delta("local_hour_bucket_count", baseline_24h),
        "local_hour_bucket_count_delta_7d": _delta("local_hour_bucket_count", baseline_7d),
        "signal_bucket_count_delta_24h": _delta("signal_bucket_count", baseline_24h),
        "signal_bucket_count_delta_7d": _delta("signal_bucket_count", baseline_7d),
        "combined_bucket_count_delta_24h": _delta("combined_bucket_count", baseline_24h),
        "combined_bucket_count_delta_7d": _delta("combined_bucket_count", baseline_7d),
    }


def _build_top_bottlenecks(
    *,
    latest_record: dict[str, Any],
    min_trades_per_bucket: int,
    top_n: int,
) -> list[dict[str, Any]]:
    combined_buckets = latest_record.get("combined_buckets")
    if not isinstance(combined_buckets, dict):
        return []

    rows: list[dict[str, Any]] = []
    safe_min_trades = max(1, int(min_trades_per_bucket))
    for bucket in combined_buckets.values():
        if not isinstance(bucket, dict):
            continue
        trades = max(0, _safe_int(bucket.get("trades")))
        gap = max(0, safe_min_trades - trades)
        if gap <= 0:
            continue
        win_rate = round(float(max(0, _safe_int(bucket.get("wins"))) / float(trades)), 6) if trades > 0 else None
        rows.append(
            {
                "bucket_key": _normalize_text(bucket.get("bucket_key")),
                "settlement_station": _normalize_station(bucket.get("settlement_station")),
                "local_hour": _normalize_hour(bucket.get("local_hour")),
                "signal_bucket": _normalize_signal_bucket(bucket.get("signal_bucket")),
                "trades": trades,
                "wins": max(0, _safe_int(bucket.get("wins"))),
                "losses": max(0, _safe_int(bucket.get("losses"))),
                "pushes": max(0, _safe_int(bucket.get("pushes"))),
                "realized_pnl_sum": round(float(_safe_float(bucket.get("realized_pnl_sum")) or 0.0), 6),
                "win_rate": win_rate,
                "target_trades_per_bucket": safe_min_trades,
                "coverage_gap_to_target_trades": gap,
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["coverage_gap_to_target_trades"]),
            int(row["trades"]),
            float(row["realized_pnl_sum"]),
            _normalize_text(row["bucket_key"]),
        )
    )
    return rows[: max(0, int(top_n))]


def _build_bootstrap_bottlenecks_from_constraint_scan(
    *,
    output_dir: Path,
    min_trades_per_bucket: int,
    top_n: int,
) -> list[dict[str, Any]]:
    source_csv_path = _find_latest_matching_file(output_dir, _CONSTRAINT_SCAN_PATTERNS)
    if not isinstance(source_csv_path, Path):
        return []
    rows, _fieldnames = _read_csv_rows_with_fieldnames(source_csv_path)
    if not rows:
        return []

    safe_min_trades = max(1, int(min_trades_per_bucket))
    by_station: dict[str, dict[str, Any]] = {}
    for row in rows:
        station = _normalize_station(row.get("settlement_station"))
        if station == "UNKNOWN":
            continue
        signal_bucket = _normalize_signal_bucket(row.get("constraint_status"))
        station_entry = by_station.setdefault(
            station,
            {
                "row_count": 0,
                "signal_counts": {},
            },
        )
        station_entry["row_count"] = int(station_entry["row_count"]) + 1
        signal_counts = station_entry["signal_counts"]
        signal_counts[signal_bucket] = int(signal_counts.get(signal_bucket, 0)) + 1

    if not by_station:
        return []

    candidate_rows: list[dict[str, Any]] = []
    for station, station_entry in by_station.items():
        signal_counts = station_entry.get("signal_counts")
        dominant_signal = "unknown"
        if isinstance(signal_counts, dict) and signal_counts:
            dominant_signal = max(
                signal_counts.items(),
                key=lambda item: (int(item[1]), _normalize_text(item[0])),
            )[0]
        candidate_rows.append(
            {
                "bucket_key": "|".join((station, "unknown", dominant_signal)),
                "settlement_station": station,
                "local_hour": "unknown",
                "signal_bucket": _normalize_signal_bucket(dominant_signal),
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "realized_pnl_sum": 0.0,
                "win_rate": None,
                "target_trades_per_bucket": safe_min_trades,
                "coverage_gap_to_target_trades": safe_min_trades,
                "bootstrap_market_count": int(station_entry.get("row_count") or 0),
                "bootstrap_reason": "constraint_station_without_settled_history",
            }
        )

    candidate_rows.sort(
        key=lambda row: (
            -int(row.get("bootstrap_market_count") or 0),
            _normalize_text(row.get("settlement_station")),
        )
    )
    return candidate_rows[: max(0, int(top_n))]


def _read_csv_rows_with_fieldnames(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.is_file():
        return [], []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return [], []
        rows = [dict(row) for row in reader]
        fieldnames = [str(item) for item in reader.fieldnames]
    return rows, fieldnames


def _filter_constraint_rows_for_bottlenecks(
    *,
    output_dir: Path,
    bottlenecks: list[dict[str, Any]],
    captured_at: datetime,
) -> dict[str, Any]:
    source_csv_path = _find_latest_matching_file(output_dir, _CONSTRAINT_SCAN_PATTERNS)
    if not isinstance(source_csv_path, Path):
        return {
            "source_constraint_csv": "",
            "targeted_constraint_csv": "",
            "targeted_constraint_rows": 0,
            "targeted_filter_mode": "none",
        }

    rows, fieldnames = _read_csv_rows_with_fieldnames(source_csv_path)
    if not rows or not fieldnames:
        return {
            "source_constraint_csv": str(source_csv_path),
            "targeted_constraint_csv": "",
            "targeted_constraint_rows": 0,
            "targeted_filter_mode": "none",
        }

    targeted_stations = {
        _normalize_station(item.get("settlement_station"))
        for item in bottlenecks
        if _normalize_text(item.get("settlement_station"))
    }
    targeted_signals = {
        _normalize_signal_bucket(item.get("signal_bucket"))
        for item in bottlenecks
        if _normalize_text(item.get("signal_bucket"))
    }
    if not targeted_stations and not targeted_signals:
        return {
            "source_constraint_csv": str(source_csv_path),
            "targeted_constraint_csv": "",
            "targeted_constraint_rows": 0,
            "targeted_filter_mode": "none",
        }

    def _strict(row: dict[str, str]) -> bool:
        station = _normalize_station(row.get("settlement_station"))
        signal = _normalize_signal_bucket(row.get("constraint_status"))
        return station in targeted_stations and signal in targeted_signals

    def _station_only(row: dict[str, str]) -> bool:
        return _normalize_station(row.get("settlement_station")) in targeted_stations

    def _signal_only(row: dict[str, str]) -> bool:
        return _normalize_signal_bucket(row.get("constraint_status")) in targeted_signals

    filtered_rows = [row for row in rows if _strict(row)] if targeted_stations and targeted_signals else []
    mode = "station_and_signal" if filtered_rows else "none"
    if not filtered_rows and targeted_stations:
        filtered_rows = [row for row in rows if _station_only(row)]
        mode = "station_only" if filtered_rows else mode
    if not filtered_rows and targeted_signals:
        filtered_rows = [row for row in rows if _signal_only(row)]
        mode = "signal_only" if filtered_rows else mode
    if not filtered_rows:
        return {
            "source_constraint_csv": str(source_csv_path),
            "targeted_constraint_csv": "",
            "targeted_constraint_rows": 0,
            "targeted_filter_mode": "none",
        }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    health_dir = output_dir / "health"
    targeted_path = health_dir / f"kalshi_temperature_settled_outcome_targeted_constraints_{stamp}.csv"
    latest_path = health_dir / "kalshi_temperature_settled_outcome_targeted_constraints_latest.csv"
    _write_csv_atomic(targeted_path, rows=filtered_rows, fieldnames=fieldnames)
    _write_csv_atomic(latest_path, rows=filtered_rows, fieldnames=fieldnames)
    return {
        "source_constraint_csv": str(source_csv_path),
        "targeted_constraint_csv": str(targeted_path),
        "targeted_constraint_latest_csv": str(latest_path),
        "targeted_constraint_rows": int(len(filtered_rows)),
        "targeted_filter_mode": mode,
    }


def run_kalshi_temperature_settled_outcome_throughput(
    *,
    output_dir: str = "outputs",
    top_n_bottlenecks: int = 12,
    min_trades_per_bucket: int = 3,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    summary_paths = _find_all_matching_files(out_dir, _PROFITABILITY_SUMMARY_PATTERNS)
    summary_records: list[dict[str, Any]] = []
    for summary_path in summary_paths:
        payload = _load_json(summary_path)
        if not payload:
            continue
        summary_records.append(
            _extract_record_from_profitability_summary(
                output_dir=out_dir,
                summary_path=summary_path,
                payload=payload,
            )
        )
    summary_records.sort(
        key=lambda row: (
            row["captured_at"].timestamp() if isinstance(row.get("captured_at"), datetime) else 0.0,
            _normalize_text(row.get("source")),
        )
    )

    status = "ready" if summary_records else "missing_profitability_summary"
    latest_record = summary_records[-1] if summary_records else {}
    top_bottlenecks = (
        _build_top_bottlenecks(
            latest_record=latest_record,
            min_trades_per_bucket=min_trades_per_bucket,
            top_n=top_n_bottlenecks,
        )
        if summary_records
        else []
    )
    bottleneck_source = "profitability_combined_regimes"
    if summary_records and not top_bottlenecks:
        bootstrap_rows = _build_bootstrap_bottlenecks_from_constraint_scan(
            output_dir=out_dir,
            min_trades_per_bucket=min_trades_per_bucket,
            top_n=top_n_bottlenecks,
        )
        if bootstrap_rows:
            top_bottlenecks = bootstrap_rows
            bottleneck_source = "constraint_station_bootstrap"
        else:
            bottleneck_source = "none"
    elif not summary_records:
        bottleneck_source = "none"
    growth_deltas = (
        _build_growth_deltas(summary_records, latest=latest_record)
        if summary_records
        else {
            "baseline_24h_source": "",
            "baseline_7d_source": "",
            "settled_outcomes_delta_24h": None,
            "settled_outcomes_delta_7d": None,
            "station_bucket_count_delta_24h": None,
            "station_bucket_count_delta_7d": None,
            "local_hour_bucket_count_delta_24h": None,
            "local_hour_bucket_count_delta_7d": None,
            "signal_bucket_count_delta_24h": None,
            "signal_bucket_count_delta_7d": None,
            "combined_bucket_count_delta_24h": None,
            "combined_bucket_count_delta_7d": None,
        }
    )
    targeting = _filter_constraint_rows_for_bottlenecks(
        output_dir=out_dir,
        bottlenecks=top_bottlenecks,
        captured_at=captured_at,
    )

    payload: dict[str, Any] = {
        "status": status,
        "captured_at": captured_at.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "inputs": {
            "top_n_bottlenecks": max(0, int(top_n_bottlenecks)),
            "min_trades_per_bucket": max(1, int(min_trades_per_bucket)),
        },
        "profitability_summary_files_scanned": int(len(summary_paths)),
        "profitability_summary_records_used": int(len(summary_records)),
        "latest_profitability_summary_source": _normalize_text(latest_record.get("source")),
        "latest_profitability_settled_csv": _normalize_text(latest_record.get("settled_csv")),
        "coverage": {
            "settled_outcomes": max(0, _safe_int(latest_record.get("settled_outcomes"))),
            "station_bucket_count": max(0, _safe_int(latest_record.get("station_bucket_count"))),
            "local_hour_bucket_count": max(0, _safe_int(latest_record.get("local_hour_bucket_count"))),
            "signal_bucket_count": max(0, _safe_int(latest_record.get("signal_bucket_count"))),
            "combined_bucket_count": max(0, _safe_int(latest_record.get("combined_bucket_count"))),
            "dimension_keys": list(_DIMENSION_KEYS),
        },
        "growth_deltas": growth_deltas,
        "bottleneck_source": bottleneck_source,
        "top_bottlenecks": top_bottlenecks,
        "targeting": {
            **targeting,
            "targeted_station_count": len(
                {
                    _normalize_station(row.get("settlement_station"))
                    for row in top_bottlenecks
                    if _normalize_text(row.get("settlement_station"))
                }
            ),
            "targeted_signal_bucket_count": len(
                {
                    _normalize_signal_bucket(row.get("signal_bucket"))
                    for row in top_bottlenecks
                    if _normalize_text(row.get("signal_bucket"))
                }
            ),
            "targeted_local_hour_count": len(
                {
                    _normalize_hour(row.get("local_hour"))
                    for row in top_bottlenecks
                    if _normalize_text(row.get("local_hour"))
                }
            ),
        },
    }
    payload["targeted_constraint_csv"] = _normalize_text(targeting.get("targeted_constraint_csv"))
    payload["targeted_constraint_rows"] = max(0, _safe_int(targeting.get("targeted_constraint_rows")))

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_settled_outcome_throughput_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_settled_outcome_throughput_latest.json"
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)

    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return payload


def summarize_kalshi_temperature_settled_outcome_throughput(
    *,
    output_dir: str = "outputs",
    top_n_bottlenecks: int = 12,
    min_trades_per_bucket: int = 3,
) -> str:
    payload = run_kalshi_temperature_settled_outcome_throughput(
        output_dir=output_dir,
        top_n_bottlenecks=top_n_bottlenecks,
        min_trades_per_bucket=min_trades_per_bucket,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
