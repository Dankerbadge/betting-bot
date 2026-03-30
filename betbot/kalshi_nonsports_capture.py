from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_nonsports_scan import run_kalshi_nonsports_scan


ScanRunner = Callable[..., dict[str, Any]]


HISTORY_FIELDNAMES = [
    "captured_at",
    "summary_file",
    "scan_csv",
    "category",
    "market_family",
    "resolution_source_type",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "event_sub_title",
    "market_title",
    "yes_sub_title",
    "rules_primary",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_bid_size_contracts",
    "yes_ask_dollars",
    "yes_ask_size_contracts",
    "no_bid_dollars",
    "no_ask_dollars",
    "last_price_dollars",
    "spread_dollars",
    "liquidity_dollars",
    "volume_24h_contracts",
    "open_interest_contracts",
    "ten_dollar_fillable_at_best_ask",
    "two_sided_book",
    "execution_fit_score",
]


def _read_scan_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _infer_market_family(row: dict[str, Any]) -> str:
    merged = " ".join(
        (
            str(row.get("market_ticker") or ""),
            str(row.get("market_title") or ""),
            str(row.get("event_title") or ""),
            str(row.get("event_sub_title") or ""),
        )
    ).lower()
    if any(token in merged for token in ("ipo", "go public", "public offering", "s-1")):
        return "ipo_announcement"
    if any(token in merged for token in ("merger", "acquisition", "acquire", "buyout")):
        return "merger_announcement"
    if any(token in merged for token in ("trailer",)):
        return "trailer_release"
    if any(token in merged for token in ("release", "premiere", "season")):
        return "media_release"
    if any(token in merged for token in ("resign", "leave office", "fired", "out by")):
        return "cabinet_exit"
    if any(token in merged for token in ("appoint", "confirmation", "confirm", "nominated")):
        return "appointment_confirmation"
    return "general_event"


def _infer_resolution_source_type(row: dict[str, Any]) -> str:
    rules = str(row.get("rules_primary") or "").strip().lower()
    if not rules:
        return ""
    if any(token in rules for token in (".gov", "official", "department", "ministry", "white house", "sec filing")):
        return "official_source"
    if any(token in rules for token in ("press release", "company announcement", "company filing")):
        return "company_source"
    if any(token in rules for token in ("reuters", "associated press", "news report")):
        return "news_source"
    return "rules_text"


def _rewrite_history_with_latest_schema(path: Path) -> None:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing_rows = [dict(row) for row in reader]

    temp_path = path.with_name(f"{path.name}.tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDNAMES)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow({field: row.get(field, "") for field in HISTORY_FIELDNAMES})
    temp_path.replace(path)


def _append_history(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        with path.open("r", newline="", encoding="utf-8") as handle:
            existing_fieldnames = list(csv.DictReader(handle).fieldnames or [])
        if existing_fieldnames != HISTORY_FIELDNAMES:
            _rewrite_history_with_latest_schema(path)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDNAMES)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in HISTORY_FIELDNAMES})


def _count_history_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _count_distinct_markets(path: Path) -> int:
    if not path.exists():
        return 0
    tickers: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("market_ticker") or "").strip()
            if ticker:
                tickers.add(ticker)
    return len(tickers)


def run_kalshi_nonsports_capture(
    *,
    env_file: str,
    output_dir: str = "outputs",
    history_csv: str | None = None,
    timeout_seconds: float = 15.0,
    excluded_categories: tuple[str, ...] = ("Sports",),
    max_hours_to_close: float | None = 336.0,
    page_limit: int = 200,
    max_pages: int = 5,
    top_n: int = 10,
    scan_runner: ScanRunner = run_kalshi_nonsports_scan,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    scan_summary = scan_runner(
        env_file=env_file,
        output_dir=output_dir,
        timeout_seconds=timeout_seconds,
        excluded_categories=excluded_categories,
        max_hours_to_close=max_hours_to_close,
        page_limit=page_limit,
        max_pages=max_pages,
        top_n=top_n,
        now=captured_at,
    )
    scan_csv = Path(str(scan_summary["output_csv"]))
    scan_rows = _read_scan_rows(scan_csv)

    history_path = Path(history_csv) if history_csv else Path(output_dir) / "kalshi_nonsports_history.csv"
    appended_rows: list[dict[str, Any]] = []
    for row in scan_rows:
        appended_rows.append(
            {
                "captured_at": captured_at.isoformat(),
                "summary_file": scan_summary.get("output_file", ""),
                "scan_csv": str(scan_csv),
                "category": row.get("category", ""),
                "market_family": row.get("market_family", "") or _infer_market_family(row),
                "resolution_source_type": (
                    row.get("resolution_source_type", "") or _infer_resolution_source_type(row)
                ),
                "series_ticker": row.get("series_ticker", ""),
                "event_ticker": row.get("event_ticker", ""),
                "market_ticker": row.get("market_ticker", ""),
                "event_title": row.get("event_title", ""),
                "event_sub_title": row.get("event_sub_title", ""),
                "market_title": row.get("market_title", ""),
                "yes_sub_title": row.get("yes_sub_title", ""),
                "rules_primary": row.get("rules_primary", ""),
                "close_time": row.get("close_time", ""),
                "hours_to_close": row.get("hours_to_close", ""),
                "yes_bid_dollars": row.get("yes_bid_dollars", ""),
                "yes_bid_size_contracts": row.get("yes_bid_size_contracts", ""),
                "yes_ask_dollars": row.get("yes_ask_dollars", ""),
                "yes_ask_size_contracts": row.get("yes_ask_size_contracts", ""),
                "no_bid_dollars": row.get("no_bid_dollars", ""),
                "no_ask_dollars": row.get("no_ask_dollars", ""),
                "last_price_dollars": row.get("last_price_dollars", ""),
                "spread_dollars": row.get("spread_dollars", ""),
                "liquidity_dollars": row.get("liquidity_dollars", ""),
                "volume_24h_contracts": row.get("volume_24h_contracts", ""),
                "open_interest_contracts": row.get("open_interest_contracts", ""),
                "ten_dollar_fillable_at_best_ask": row.get("ten_dollar_fillable_at_best_ask", ""),
                "two_sided_book": row.get("two_sided_book", ""),
                "execution_fit_score": row.get("execution_fit_score", ""),
            }
        )

    _append_history(history_path, appended_rows)

    return {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "scan_status": scan_summary.get("status"),
        "scan_error": scan_summary.get("events_error"),
        "scan_summary_file": scan_summary.get("output_file"),
        "scan_output_csv": str(scan_csv),
        "scan_page_requests": scan_summary.get("page_requests"),
        "scan_rate_limit_retries_used": scan_summary.get("rate_limit_retries_used"),
        "scan_network_retries_used": scan_summary.get("network_retries_used"),
        "scan_transient_http_retries_used": scan_summary.get("transient_http_retries_used"),
        "scan_search_retries_total": scan_summary.get("search_retries_total"),
        "scan_search_health_status": scan_summary.get("search_health_status"),
        "scan_events_fetched": scan_summary.get("events_fetched"),
        "scan_markets_ranked": scan_summary.get("markets_ranked"),
        "history_csv": str(history_path),
        "rows_appended": len(appended_rows),
        "history_rows_total": _count_history_rows(history_path),
        "distinct_markets_observed": _count_distinct_markets(history_path),
        "two_sided_rows_appended": sum(1 for row in appended_rows if str(row["two_sided_book"]) == "True"),
        "status": scan_summary.get("status", "ready"),
    }
