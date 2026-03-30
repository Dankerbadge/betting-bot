from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.kalshi_weather_settlement import build_weather_settlement_spec


WEATHER_CATALOG_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "spread_dollars",
    "contract_family",
    "settlement_source_primary",
    "settlement_source_fallback",
    "settlement_station",
    "settlement_timezone",
    "local_day_boundary",
    "threshold_expression",
    "rule_text_hash_sha256",
    "rules_primary",
]


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    latest: dict[str, dict[str, str]] = {}
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest[ticker] = rows_sorted[-1]
    return latest


def _is_weather_row(row: dict[str, Any]) -> bool:
    category = str(row.get("category") or "").strip().lower()
    if category == "climate and weather":
        return True
    ticker_tokens = " ".join(
        (
            str(row.get("series_ticker") or ""),
            str(row.get("event_ticker") or ""),
            str(row.get("market_ticker") or ""),
        )
    ).upper()
    weather_like_ticker_prefix = (
        ticker_tokens.startswith("KXRAIN")
        or ticker_tokens.startswith("KXSNOW")
        or ticker_tokens.startswith("KXTEMP")
        or ticker_tokens.startswith("KXHMONTH")
        or ticker_tokens.startswith("KXHURR")
    )
    settlement = build_weather_settlement_spec(row)
    family = str(settlement.get("contract_family") or "")
    return weather_like_ticker_prefix and family not in {"", "non_weather"}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=WEATHER_CATALOG_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_weather_catalog(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    top_n: int = 20,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    latest_rows = _latest_market_rows(history_rows)

    catalog_rows: list[dict[str, Any]] = []
    family_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for row in latest_rows.values():
        if not _is_weather_row(row):
            continue
        settlement = build_weather_settlement_spec(row)
        family = str(settlement.get("contract_family") or "")
        primary_source = str(settlement.get("settlement_source_primary") or "")
        family_counts[family] = family_counts.get(family, 0) + 1
        if primary_source:
            source_counts[primary_source] = source_counts.get(primary_source, 0) + 1
        catalog_rows.append(
            {
                "captured_at": row.get("captured_at", ""),
                "category": row.get("category", ""),
                "series_ticker": row.get("series_ticker", ""),
                "event_ticker": row.get("event_ticker", ""),
                "market_ticker": row.get("market_ticker", ""),
                "event_title": row.get("event_title", ""),
                "market_title": row.get("market_title", ""),
                "close_time": row.get("close_time", ""),
                "hours_to_close": row.get("hours_to_close", ""),
                "yes_bid_dollars": row.get("yes_bid_dollars", ""),
                "yes_ask_dollars": row.get("yes_ask_dollars", ""),
                "spread_dollars": row.get("spread_dollars", ""),
                "contract_family": settlement.get("contract_family", ""),
                "settlement_source_primary": settlement.get("settlement_source_primary", ""),
                "settlement_source_fallback": settlement.get("settlement_source_fallback", ""),
                "settlement_station": settlement.get("settlement_station", ""),
                "settlement_timezone": settlement.get("settlement_timezone", ""),
                "local_day_boundary": settlement.get("local_day_boundary", ""),
                "threshold_expression": settlement.get("threshold_expression", ""),
                "rule_text_hash_sha256": settlement.get("rule_text_hash_sha256", ""),
                "rules_primary": row.get("rules_primary", ""),
            }
        )

    def _score(entry: dict[str, Any]) -> float:
        yes_bid = float(entry.get("yes_bid_dollars") or 0.0)
        yes_ask = float(entry.get("yes_ask_dollars") or 0.0)
        spread = float(entry.get("spread_dollars") or 1.0)
        return yes_bid + (1.0 - spread) + (1.0 - abs(0.5 - ((yes_bid + yes_ask) / 2.0 if yes_ask > 0 else yes_bid)))

    top_rows = sorted(catalog_rows, key=_score, reverse=True)[: max(1, int(top_n))]

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_weather_catalog_{stamp}.csv"
    _write_csv(csv_path, catalog_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "weather_markets_total": len(catalog_rows),
        "contract_family_counts": family_counts,
        "settlement_source_primary_counts": source_counts,
        "top_markets": top_rows,
        "status": "ready" if catalog_rows else "no_weather_markets",
        "output_csv": str(csv_path),
    }

    summary_path = out_dir / f"kalshi_weather_catalog_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
