from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from betbot.kalshi_weather_ingest import fetch_ncei_station_daily_summary_for_date
from betbot.kalshi_weather_settlement import infer_timezone_from_station


FinalReportLookupRunner = Callable[..., dict[str, Any]]


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


def _load_final_report_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, dict):
        return {}
    entries: dict[str, dict[str, Any]] = {}
    for key, value in raw_entries.items():
        if not isinstance(value, dict):
            continue
        normalized_key = _normalize_text(key)
        if not normalized_key:
            continue
        entries[normalized_key] = value
    return entries


def _write_final_report_cache(path: Path, entries: dict[str, dict[str, Any]]) -> None:
    payload = {"entries": entries}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _cache_entry_is_fresh(entry: dict[str, Any], *, now_utc: datetime, ttl_minutes: float) -> bool:
    lookup_payload = entry.get("lookup")
    if isinstance(lookup_payload, dict):
        # Legacy behavior cached station_mapping_missing as a terminal error
        # before ADS fallback logic was added. Force a refresh so those keys
        # can be re-resolved via current lookup ordering.
        lookup_status = _normalize_text(lookup_payload.get("status")).lower()
        if lookup_status == "station_mapping_missing":
            return False
    looked_up_at = _parse_iso_datetime(entry.get("looked_up_at"))
    if looked_up_at is None:
        return False
    age_seconds = (now_utc - looked_up_at).total_seconds()
    return age_seconds >= 0.0 and age_seconds <= max(0.0, float(ttl_minutes)) * 60.0


def _series_final_truth_metric(series_ticker: str) -> str:
    series_text = _normalize_text(series_ticker).upper()
    if "LOW" in series_text:
        return "tmin_f"
    return "tmax_f"


def _resolve_state(
    *,
    target_date_local: str,
    settlement_timezone: str,
    settlement_station: str,
    now_utc: datetime,
) -> tuple[str, str, bool, str]:
    target_date = _parse_local_date(target_date_local)
    if target_date is None:
        return ("unknown_window", "unknown_window", False, "target_date_local_missing")

    effective_timezone = _normalize_text(settlement_timezone) or infer_timezone_from_station(settlement_station) or "UTC"
    today_local = _local_today(now_utc, effective_timezone)
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
    final_report_lookup_enabled: bool = True,
    final_report_cache_ttl_minutes: float = 30.0,
    final_report_timeout_seconds: float = 12.0,
    final_report_lookup_runner: FinalReportLookupRunner = fetch_ncei_station_daily_summary_for_date,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_path = out_dir / "kalshi_temperature_settlement_final_reports_cache.json"
    final_report_cache = _load_final_report_cache(cache_path) if final_report_lookup_enabled else {}
    cache_dirty = False

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
                "settlement_source_primary": _normalize_text(row.get("settlement_source_primary")),
                "settlement_source_fallback": _normalize_text(row.get("settlement_source_fallback")),
                "market_tickers": [],
                "market_count": 0,
                "fast_truth_value": None,
                "final_truth_value": None,
            },
        )
        if not _normalize_text(entry.get("settlement_timezone")):
            inferred_timezone = _normalize_text(row.get("settlement_timezone")) or infer_timezone_from_station(settlement_station)
            if inferred_timezone:
                entry["settlement_timezone"] = inferred_timezone
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
    final_report_lookup_attempted = 0
    final_report_lookup_cache_hit = 0
    final_report_ready_count = 0
    final_report_pending_count = 0
    final_report_error_count = 0
    for key in sorted(by_underlying.keys()):
        entry = by_underlying[key]
        state, finalization_status, allow_new_orders, reason = _resolve_state(
            target_date_local=_normalize_text(entry.get("target_date_local")),
            settlement_timezone=_normalize_text(entry.get("settlement_timezone")),
            settlement_station=_normalize_text(entry.get("settlement_station")),
            now_utc=captured_at,
        )
        final_report_lookup_status = "not_checked"
        final_report_data_source = ""
        final_report_http_status = 0
        if (
            final_report_lookup_enabled
            and state == "pending_final_report"
            and _normalize_text(entry.get("settlement_station"))
            and _normalize_text(entry.get("target_date_local"))
        ):
            final_report_lookup_attempted += 1
            cache_entry = final_report_cache.get(key)
            lookup_payload: dict[str, Any] | None = None
            if isinstance(cache_entry, dict) and _cache_entry_is_fresh(
                cache_entry,
                now_utc=captured_at.astimezone(timezone.utc),
                ttl_minutes=final_report_cache_ttl_minutes,
            ):
                cached_lookup = cache_entry.get("lookup")
                if isinstance(cached_lookup, dict):
                    lookup_payload = cached_lookup
                    final_report_lookup_cache_hit += 1
            if lookup_payload is None:
                try:
                    lookup_payload = final_report_lookup_runner(
                        station_id=_normalize_text(entry.get("settlement_station")),
                        target_date=_normalize_text(entry.get("target_date_local")),
                        timeout_seconds=final_report_timeout_seconds,
                    )
                except Exception as exc:  # pragma: no cover - runtime guard
                    lookup_payload = {
                        "status": "lookup_error",
                        "error": str(exc),
                    }
                final_report_cache[key] = {
                    "looked_up_at": captured_at.astimezone(timezone.utc).isoformat(),
                    "lookup": lookup_payload,
                }
                cache_dirty = True

            if isinstance(lookup_payload, dict):
                final_report_lookup_status = _normalize_text(lookup_payload.get("status")) or "unknown"
                final_report_data_source = _normalize_text(lookup_payload.get("data_source"))
                final_report_http_status = int(lookup_payload.get("http_status") or 0)
                if final_report_lookup_status == "ready":
                    sample = lookup_payload.get("daily_sample")
                    if isinstance(sample, dict):
                        metric_key = _series_final_truth_metric(_normalize_text(entry.get("series_ticker")))
                        final_truth_value = _parse_float(sample.get(metric_key))
                        if final_truth_value is not None:
                            entry["final_truth_value"] = final_truth_value
                            state = "final_report_available"
                            finalization_status = "final_report_available"
                            allow_new_orders = False
                            reason = "final_report_published"
                            final_report_ready_count += 1
                        else:
                            final_report_pending_count += 1
                    else:
                        final_report_pending_count += 1
                elif final_report_lookup_status in {"no_final_report", "no_history"}:
                    final_report_pending_count += 1
                else:
                    final_report_error_count += 1
                    if reason == "target_date_elapsed_waiting_finalization":
                        reason = "target_date_elapsed_final_report_lookup_error"
            else:
                final_report_lookup_status = "lookup_error"
                final_report_error_count += 1
                if reason == "target_date_elapsed_waiting_finalization":
                    reason = "target_date_elapsed_final_report_lookup_error"

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
            "review_flag": state == "pending_final_report",
            "updated_at": captured_at.isoformat(),
            "final_truth_value": entry.get("final_truth_value"),
            "fast_truth_value": entry.get("fast_truth_value"),
            "final_report_lookup_status": final_report_lookup_status,
            "final_report_data_source": final_report_data_source,
            "final_report_http_status": final_report_http_status if final_report_http_status > 0 else None,
            "revision_id": revision_id,
            "source": "kalshi_temperature_settlement_state_v1",
            "series_ticker": _normalize_text(entry.get("series_ticker")),
            "settlement_station": _normalize_text(entry.get("settlement_station")),
            "settlement_timezone": _normalize_text(entry.get("settlement_timezone")),
            "settlement_source_primary": _normalize_text(entry.get("settlement_source_primary")),
            "settlement_source_fallback": _normalize_text(entry.get("settlement_source_fallback")),
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
        "final_report_lookup_enabled": bool(final_report_lookup_enabled),
        "final_report_cache_ttl_minutes": float(final_report_cache_ttl_minutes),
        "final_report_cache_file": str(cache_path),
        "final_report_lookup_attempted": final_report_lookup_attempted,
        "final_report_lookup_cache_hit": final_report_lookup_cache_hit,
        "final_report_ready_count": final_report_ready_count,
        "final_report_pending_count": final_report_pending_count,
        "final_report_error_count": final_report_error_count,
        "underlying_count": len(normalized_underlyings),
        "blocked_underlyings": blocked_underlyings,
        "state_counts": dict(sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))),
        "underlyings": normalized_underlyings,
        "top_underlyings": [
            {"underlying_key": key, **entry}
            for key, entry in list(normalized_underlyings.items())[: max(1, int(top_n))]
        ],
    }
    if final_report_lookup_enabled and cache_dirty:
        _write_final_report_cache(cache_path, final_report_cache)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
