from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_local_date(value: Any) -> date | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _find_latest_csv(output_dir: str, pattern: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return ""
    return str(candidates[-1])


def _underlying_key(*, series_ticker: Any, settlement_station: Any, target_date_local: Any) -> str:
    return "|".join(
        (
            _normalize_text(series_ticker) or "series_unknown",
            _normalize_text(settlement_station) or "station_unknown",
            _normalize_text(target_date_local) or "date_unknown",
        )
    )


def _local_today(now_utc: datetime, timezone_name: str) -> date:
    zone_name = _normalize_text(timezone_name) or "UTC"
    try:
        zone = ZoneInfo(zone_name)
    except ZoneInfoNotFoundError:
        zone = timezone.utc
    return now_utc.astimezone(zone).date()


def _resolve_state(
    *,
    target_date_local: str,
    settlement_timezone: str,
    now_utc: datetime,
) -> tuple[str, str, bool, str]:
    target_date = _parse_local_date(target_date_local)
    if target_date is None:
        return ("unknown_window", "unknown_window", False, "target_date_local_missing")

    today_local = _local_today(now_utc, settlement_timezone)
    if target_date < today_local:
        return (
            "pending_final_report",
            "post_close_unfinalized",
            False,
            "target_date_elapsed_waiting_finalization",
        )
    if target_date == today_local:
        return ("intraday_unfinalized", "intraday_unfinalized", True, "")
    return ("pre_target_day", "pre_target_day", True, "")


def run_kalshi_temperature_settlement_state(
    *,
    specs_csv: str | None = None,
    constraint_csv: str | None = None,
    output_dir: str = "outputs",
    top_n: int = 25,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    resolved_specs_csv = _normalize_text(specs_csv) or _find_latest_csv(output_dir, "kalshi_temperature_contract_specs_*.csv")
    if not resolved_specs_csv:
        return {
            "status": "missing_specs_csv",
            "captured_at": captured_at.isoformat(),
            "error": "No specs CSV provided and none found in output_dir.",
        }
    specs_path = Path(resolved_specs_csv)
    specs_rows = _read_csv_rows(specs_path)
    if not specs_rows:
        return {
            "status": "no_specs_rows",
            "captured_at": captured_at.isoformat(),
            "specs_csv": str(specs_path),
            "error": "Specs CSV missing or empty.",
        }

    resolved_constraint_csv = _normalize_text(constraint_csv) or _find_latest_csv(
        output_dir,
        "kalshi_temperature_constraint_scan_*.csv",
    )
    constraint_rows: list[dict[str, str]] = []
    if resolved_constraint_csv:
        constraint_rows = _read_csv_rows(Path(resolved_constraint_csv))

    specs_by_ticker = {
        _normalize_text(row.get("market_ticker")): row
        for row in specs_rows
        if _normalize_text(row.get("market_ticker"))
    }

    by_underlying: dict[str, dict[str, Any]] = {}
    for row in specs_rows:
        if _normalize_text(row.get("contract_family")) != "daily_temperature":
            continue
        settlement_station = _normalize_text(row.get("settlement_station"))
        target_date_local = _normalize_text(row.get("target_date_local"))
        if not settlement_station or not target_date_local:
            continue
        key = _underlying_key(
            series_ticker=row.get("series_ticker"),
            settlement_station=settlement_station,
            target_date_local=target_date_local,
        )
        entry = by_underlying.setdefault(
            key,
            {
                "underlying_key": key,
                "series_ticker": _normalize_text(row.get("series_ticker")),
                "settlement_station": settlement_station,
                "target_date_local": target_date_local,
                "settlement_timezone": _normalize_text(row.get("settlement_timezone")),
                "market_tickers": [],
                "market_count": 0,
                "fast_truth_value": None,
                "final_truth_value": None,
            },
        )
        ticker = _normalize_text(row.get("market_ticker"))
        if ticker and ticker not in entry["market_tickers"]:
            entry["market_tickers"].append(ticker)
            entry["market_count"] = int(entry["market_count"]) + 1

    for row in constraint_rows:
        ticker = _normalize_text(row.get("market_ticker"))
        if not ticker:
            continue
        spec_row = specs_by_ticker.get(ticker)
        if not isinstance(spec_row, dict):
            continue
        key = _underlying_key(
            series_ticker=spec_row.get("series_ticker"),
            settlement_station=spec_row.get("settlement_station"),
            target_date_local=spec_row.get("target_date_local"),
        )
        entry = by_underlying.get(key)
        if not isinstance(entry, dict):
            continue
        observed = _parse_float(row.get("observed_max_settlement_quantized"))
        if not isinstance(observed, float):
            continue
        current = _parse_float(entry.get("fast_truth_value"))
        if current is None or observed > current:
            entry["fast_truth_value"] = observed

    normalized_underlyings: dict[str, dict[str, Any]] = {}
    state_counts: dict[str, int] = {}
    blocked_underlyings = 0
    for key in sorted(by_underlying.keys()):
        entry = by_underlying[key]
        state, finalization_status, allow_new_orders, reason = _resolve_state(
            target_date_local=_normalize_text(entry.get("target_date_local")),
            settlement_timezone=_normalize_text(entry.get("settlement_timezone")),
            now_utc=captured_at,
        )
        if not allow_new_orders:
            blocked_underlyings += 1
        state_counts[state] = state_counts.get(state, 0) + 1

        digest_source = "|".join(
            (
                key,
                state,
                finalization_status,
                str(entry.get("fast_truth_value")),
                captured_at.isoformat(),
            )
        )
        revision_id = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]

        normalized_underlyings[key] = {
            "state": state,
            "finalization_status": finalization_status,
            "allow_new_orders": allow_new_orders,
            "reason": reason,
            "review_flag": not allow_new_orders,
            "updated_at": captured_at.isoformat(),
            "final_truth_value": entry.get("final_truth_value"),
            "fast_truth_value": entry.get("fast_truth_value"),
            "revision_id": revision_id,
            "source": "kalshi_temperature_settlement_state_v1",
            "series_ticker": _normalize_text(entry.get("series_ticker")),
            "settlement_station": _normalize_text(entry.get("settlement_station")),
            "settlement_timezone": _normalize_text(entry.get("settlement_timezone")),
            "target_date_local": _normalize_text(entry.get("target_date_local")),
            "market_count": int(entry.get("market_count") or 0),
            "market_tickers": list(entry.get("market_tickers") or []),
        }

    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"kalshi_temperature_settlement_state_{stamp}.json"

    summary: dict[str, Any] = {
        "status": "ready" if normalized_underlyings else "no_underlyings",
        "captured_at": captured_at.isoformat(),
        "source_specs_csv": str(specs_path),
        "source_constraint_csv": resolved_constraint_csv,
        "underlying_count": len(normalized_underlyings),
        "blocked_underlyings": blocked_underlyings,
        "state_counts": dict(sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))),
        "underlyings": normalized_underlyings,
        "top_underlyings": [
            {"underlying_key": key, **entry}
            for key, entry in list(normalized_underlyings.items())[: max(1, int(top_n))]
        ],
    }
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary

