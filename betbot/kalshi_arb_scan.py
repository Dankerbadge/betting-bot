from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from betbot.kalshi_nonsports_scan import _load_open_events, _parse_float
from betbot.live_smoke import HttpGetter, KALSHI_API_ROOTS, _http_get_json, kalshi_api_root_candidates
from betbot.onboarding import _parse_env_file


def _derive_yes_ask(market: dict[str, Any]) -> float | None:
    yes_ask = _parse_float(market.get("yes_ask_dollars"))
    if isinstance(yes_ask, float):
        return yes_ask
    no_bid = _parse_float(market.get("no_bid_dollars"))
    if isinstance(no_bid, float):
        return round(1.0 - no_bid, 6)
    return None


def build_mutually_exclusive_arb_rows(
    *,
    events: list[dict[str, Any]],
    fee_buffer_per_contract_dollars: float,
    min_margin_dollars: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not bool(event.get("mutually_exclusive")):
            continue
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue
        active_markets = [
            market for market in markets
            if isinstance(market, dict) and str(market.get("status") or "").strip().lower() == "active"
        ]
        if len(active_markets) < 2:
            continue
        market_entries: list[dict[str, Any]] = []
        missing_price = False
        bundle_cost = 0.0
        for market in active_markets:
            yes_ask = _derive_yes_ask(market)
            if yes_ask is None:
                missing_price = True
                break
            bundle_cost += yes_ask
            market_entries.append(
                {
                    "ticker": str(market.get("ticker") or ""),
                    "title": str(market.get("title") or ""),
                    "yes_ask_dollars": round(yes_ask, 6),
                }
            )
        if missing_price:
            continue
        fee_buffer_total = fee_buffer_per_contract_dollars * len(market_entries)
        bundle_cost_with_fees = bundle_cost + fee_buffer_total
        expected_margin = 1.0 - bundle_cost_with_fees
        if expected_margin < min_margin_dollars:
            continue
        rows.append(
            {
                "category": str(event.get("category") or ""),
                "series_ticker": str(event.get("series_ticker") or ""),
                "event_ticker": str(event.get("event_ticker") or ""),
                "event_title": str(event.get("title") or ""),
                "markets_count": len(market_entries),
                "bundle_cost_dollars": round(bundle_cost, 6),
                "fee_buffer_total_dollars": round(fee_buffer_total, 6),
                "bundle_cost_with_fees_dollars": round(bundle_cost_with_fees, 6),
                "expected_margin_dollars": round(expected_margin, 6),
                "is_opportunity": expected_margin > 0,
                "market_bundle": market_entries,
            }
        )
    rows.sort(
        key=lambda row: (
            float(row.get("expected_margin_dollars") or -1.0),
            int(row.get("markets_count") or 0),
        ),
        reverse=True,
    )
    return rows


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "category",
        "series_ticker",
        "event_ticker",
        "event_title",
        "markets_count",
        "bundle_cost_dollars",
        "fee_buffer_total_dollars",
        "bundle_cost_with_fees_dollars",
        "expected_margin_dollars",
        "is_opportunity",
        "market_bundle",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serializable = dict(row)
            serializable["market_bundle"] = json.dumps(row.get("market_bundle", []), separators=(",", ":"))
            writer.writerow(serializable)


def run_kalshi_arb_scan(
    *,
    env_file: str,
    output_dir: str = "outputs",
    timeout_seconds: float = 15.0,
    page_limit: int = 200,
    max_pages: int = 5,
    fee_buffer_per_contract_dollars: float = 0.01,
    min_margin_dollars: float = 0.0,
    top_n: int = 10,
    http_get_json: HttpGetter = _http_get_json,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)
    kalshi_env = (env_data.get("KALSHI_ENV") or "prod").strip().lower()
    if kalshi_env not in KALSHI_API_ROOTS:
        raise ValueError(f"Unsupported KALSHI_ENV={kalshi_env!r}")
    api_roots = kalshi_api_root_candidates(kalshi_env)

    status = "ready"
    events_error = None
    events: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    try:
        events = _load_open_events(
            api_roots=api_roots,
            timeout_seconds=timeout_seconds,
            page_limit=page_limit,
            max_pages=max_pages,
            http_get_json=http_get_json,
        )
        rows = build_mutually_exclusive_arb_rows(
            events=events,
            fee_buffer_per_contract_dollars=fee_buffer_per_contract_dollars,
            min_margin_dollars=min_margin_dollars,
        )
    except Exception as exc:  # pragma: no cover - defensive runtime path
        events_error = str(exc)
        status = "rate_limited" if "status 429" in events_error else "upstream_error"

    stamp = captured_at.astimezone(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_arb_scan_{stamp}.csv"
    _write_rows_csv(csv_path, rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": str(env_path),
        "kalshi_env": kalshi_env,
        "status": status,
        "events_error": events_error,
        "events_fetched": len(events),
        "mutually_exclusive_opportunities": len(rows),
        "top_event_ticker": rows[0]["event_ticker"] if rows else None,
        "top_expected_margin_dollars": rows[0]["expected_margin_dollars"] if rows else None,
        "top_opportunities": rows[:top_n],
        "output_csv": str(csv_path),
    }
    output_path = out_dir / f"kalshi_arb_scan_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
