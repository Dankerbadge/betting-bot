from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo


LEDGER_FIELDNAMES = [
    "recorded_at",
    "trading_day",
    "run_mode",
    "live_write_allowed",
    "market_ticker",
    "plan_rank",
    "planned_yes_bid_dollars",
    "planned_yes_ask_dollars",
    "estimated_entry_cost_dollars",
    "result",
    "submission_http_status",
    "order_id",
    "order_status",
    "queue_position_contracts",
    "cancel_http_status",
    "cancel_reduced_by_contracts",
    "resting_hold_seconds",
    "counts_toward_live_submission",
]

REAL_ACTIVITY_RESULTS = {
    "submit_failed",
    "submitted",
    "submitted_then_canceled",
    "cancel_failed",
}

_RELEASED_TERMINAL_ORDER_STATUSES = {
    "canceled",
    "cancelled",
    "expired",
    "rejected",
    "voided",
    "closed_not_found",
}


def default_ledger_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_micro_trade_ledger.csv"


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def _parse_trading_day(value: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def load_trade_ledger(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def append_trade_ledger(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDNAMES)
        if not exists:
            writer.writeheader()
        for row in rows:
            serialized = {key: row.get(key, "") for key in LEDGER_FIELDNAMES}
            writer.writerow(serialized)


def _load_order_status_map(book_db_path: Path | None) -> dict[str, str]:
    if book_db_path is None or not book_db_path.exists():
        return {}
    try:
        with sqlite3.connect(book_db_path) as conn:
            rows = conn.execute("SELECT order_id, status FROM orders").fetchall()
    except sqlite3.Error:
        return {}
    status_map: dict[str, str] = {}
    for order_id, status in rows:
        key = str(order_id or "").strip()
        if not key:
            continue
        status_map[key] = str(status or "").strip().lower()
    return status_map


def summarize_trade_ledger(
    *,
    path: Path,
    timezone_name: str,
    trading_day: date,
    max_live_submissions_per_day: int | None = None,
    max_live_cost_per_day_dollars: float | None = None,
    book_db_path: Path | None = None,
) -> dict[str, Any]:
    rows = load_trade_ledger(path)
    order_status_map = _load_order_status_map(book_db_path)
    live_submissions_today = 0
    live_submitted_cost_today = 0.0
    live_submitted_cost_to_date = 0.0
    canceled_submissions_today = 0
    latest_live_recorded_at = None
    live_submissions_to_date = 0
    first_counted_live_submission_day: date | None = None
    first_live_activity_day: date | None = None
    released_counted_submission_rows = 0

    for row in rows:
        row_trading_day = _parse_trading_day(str(row.get("trading_day") or ""))
        if row_trading_day == trading_day and str(row.get("result") or "") == "submitted_then_canceled":
            canceled_submissions_today += 1

        if row_trading_day is None or row_trading_day > trading_day:
            continue
        row_result = str(row.get("result") or "").strip().lower()
        row_run_mode = str(row.get("run_mode") or "").strip().lower()
        if row_run_mode == "live" and row_result in REAL_ACTIVITY_RESULTS:
            if first_live_activity_day is None or row_trading_day < first_live_activity_day:
                first_live_activity_day = row_trading_day
        counts_toward_live_submission = _parse_bool(row.get("counts_toward_live_submission", ""))
        if not counts_toward_live_submission:
            continue
        result = row_result
        if result == "submitted_then_canceled":
            released_counted_submission_rows += 1
            continue
        order_id = str(row.get("order_id") or "").strip()
        ledger_status = str(row.get("order_status") or "").strip().lower()
        effective_status = order_status_map.get(order_id) or ledger_status
        if result == "submitted" and effective_status in _RELEASED_TERMINAL_ORDER_STATUSES:
            released_counted_submission_rows += 1
            continue
        live_submissions_to_date += 1
        if first_counted_live_submission_day is None or row_trading_day < first_counted_live_submission_day:
            first_counted_live_submission_day = row_trading_day
        if row_trading_day == trading_day:
            live_submissions_today += 1
            try:
                live_submitted_cost_today += float(row.get("estimated_entry_cost_dollars") or 0.0)
            except ValueError:
                pass
            recorded_at = str(row.get("recorded_at") or "")
            if recorded_at:
                latest_live_recorded_at = recorded_at
        try:
            live_submitted_cost_to_date += float(row.get("estimated_entry_cost_dollars") or 0.0)
        except ValueError:
            pass

    safe_daily_limit = max(0, int(max_live_submissions_per_day or 0))
    baseline_day = first_counted_live_submission_day or first_live_activity_day or trading_day
    if safe_daily_limit > 0:
        live_submission_days_elapsed = max(1, (trading_day - baseline_day).days + 1)
        live_submission_budget_total = live_submission_days_elapsed * safe_daily_limit
        live_submission_budget_remaining = max(0, live_submission_budget_total - live_submissions_to_date)
        live_submissions_remaining_today = max(0, safe_daily_limit - live_submissions_today)
    else:
        live_submission_days_elapsed = 0
        live_submission_budget_total = 0
        live_submission_budget_remaining = 0
        live_submissions_remaining_today = 0

    safe_daily_cost = max(0.0, float(max_live_cost_per_day_dollars or 0.0))
    if safe_daily_cost > 0 and live_submission_days_elapsed > 0:
        live_cost_budget_total = round(live_submission_days_elapsed * safe_daily_cost, 4)
        live_cost_budget_remaining = round(max(0.0, live_cost_budget_total - live_submitted_cost_to_date), 4)
    else:
        live_cost_budget_total = 0.0
        live_cost_budget_remaining = 0.0
    live_cost_remaining_today = round(max(0.0, safe_daily_cost - live_submitted_cost_today), 4)

    return {
        "ledger_path": str(path),
        "timezone": timezone_name,
        "trading_day": trading_day.isoformat(),
        "max_live_submissions_per_day": safe_daily_limit,
        "max_live_cost_per_day_dollars": safe_daily_cost,
        "live_submissions_today": live_submissions_today,
        "live_submissions_remaining_today": live_submissions_remaining_today,
        "live_submitted_cost_today": round(live_submitted_cost_today, 4),
        "live_submitted_cost_to_date": round(live_submitted_cost_to_date, 4),
        "canceled_submissions_today": canceled_submissions_today,
        "live_submissions_to_date": live_submissions_to_date,
        "first_counted_live_submission_day": (
            first_counted_live_submission_day.isoformat() if isinstance(first_counted_live_submission_day, date) else None
        ),
        "first_live_activity_day": first_live_activity_day.isoformat() if isinstance(first_live_activity_day, date) else None,
        "budget_baseline_day": baseline_day.isoformat() if isinstance(baseline_day, date) else None,
        "live_submission_days_elapsed": live_submission_days_elapsed,
        "live_submission_budget_total": live_submission_budget_total,
        "live_submission_budget_remaining": live_submission_budget_remaining,
        "live_cost_budget_total": live_cost_budget_total,
        "live_cost_budget_remaining": live_cost_budget_remaining,
        "live_cost_remaining_today": live_cost_remaining_today,
        "released_counted_submission_rows": released_counted_submission_rows,
        "latest_live_recorded_at": latest_live_recorded_at,
        "ledger_rows_total": len(rows),
    }


def ledger_rows_from_attempts(
    *,
    attempts: list[dict[str, Any]],
    captured_at: datetime,
    trading_day: date,
    run_mode: str,
    resting_hold_seconds: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for attempt in attempts:
        result = str(attempt.get("result") or "")
        if run_mode != "live" or result not in REAL_ACTIVITY_RESULTS:
            continue
        # Do not consume the live-cap budget for maker tests that were submitted
        # and then explicitly canceled.
        counts_toward_live_submission = result == "submitted"
        rows.append(
            {
                "recorded_at": captured_at.isoformat(),
                "trading_day": trading_day.isoformat(),
                "run_mode": run_mode,
                "live_write_allowed": str(bool(attempt.get("live_write_allowed"))).lower(),
                "market_ticker": str(attempt.get("market_ticker") or ""),
                "plan_rank": attempt.get("plan_rank", ""),
                "planned_yes_bid_dollars": attempt.get("planned_yes_bid_dollars", ""),
                "planned_yes_ask_dollars": attempt.get("planned_yes_ask_dollars", ""),
                "estimated_entry_cost_dollars": attempt.get("estimated_entry_cost_dollars", ""),
                "result": result,
                "submission_http_status": attempt.get("submission_http_status", ""),
                "order_id": attempt.get("order_id", ""),
                "order_status": attempt.get("order_status", ""),
                "queue_position_contracts": attempt.get("queue_position_contracts", ""),
                "cancel_http_status": attempt.get("cancel_http_status", ""),
                "cancel_reduced_by_contracts": attempt.get("cancel_reduced_by_contracts", ""),
                "resting_hold_seconds": resting_hold_seconds,
                "counts_toward_live_submission": str(counts_toward_live_submission).lower(),
            }
        )
    return rows


def trading_day_for_timestamp(captured_at: datetime, timezone_name: str) -> date:
    zone = ZoneInfo(timezone_name)
    return captured_at.astimezone(zone).date()
