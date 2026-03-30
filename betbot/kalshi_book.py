from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any


def default_book_db_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_portfolio_book.sqlite3"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def _managed_connection(path: Path):
    conn = _connect(path)
    try:
        yield conn
    finally:
        conn.close()


def ensure_book_schema(path: str | Path) -> Path:
    db_path = Path(path)
    with _managed_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT,
                ticker TEXT NOT NULL,
                side TEXT,
                action TEXT,
                limit_price_dollars REAL,
                post_only INTEGER,
                status TEXT,
                created_time TEXT,
                last_update_time TEXT,
                last_seen_at TEXT NOT NULL,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS fills (
                fill_id TEXT PRIMARY KEY,
                order_id TEXT,
                ticker TEXT,
                fill_ts TEXT,
                contracts_fp REAL,
                price_dollars REAL,
                fee_dollars REAL,
                liquidity_side TEXT,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS positions (
                ticker TEXT PRIMARY KEY,
                position_fp REAL,
                market_exposure_dollars REAL,
                realized_pnl_dollars REAL,
                fees_paid_dollars REAL,
                resting_orders_count INTEGER,
                updated_at TEXT NOT NULL,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS decisions (
                decision_id TEXT PRIMARY KEY,
                captured_at TEXT NOT NULL,
                source TEXT NOT NULL,
                ticker TEXT NOT NULL,
                category TEXT,
                side TEXT,
                planned_size REAL,
                expected_edge_net REAL,
                expected_value_net_dollars REAL,
                expected_roi_on_cost REAL,
                expected_roi_per_day REAL,
                gate_reason TEXT,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS settlements (
                settlement_id TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                settlement_ts TEXT,
                settlement_value REAL,
                realized_pnl REAL,
                fee_cost_dollars REAL,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS signals (
                signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                captured_ts TEXT NOT NULL,
                signal_name TEXT NOT NULL,
                signal_value REAL,
                features_json TEXT
            );

            CREATE TABLE IF NOT EXISTS series_fee_regime (
                series_ticker TEXT PRIMARY KEY,
                fee_type TEXT,
                fee_multiplier REAL,
                scheduled_ts TEXT,
                active_from_ts TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
    return db_path


def record_decisions(
    *,
    book_db_path: str | Path,
    source: str,
    captured_at: datetime,
    plans: list[dict[str, Any]],
) -> None:
    if not plans:
        return
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        for index, plan in enumerate(plans, start=1):
            ticker = str(plan.get("market_ticker") or "").strip()
            if not ticker:
                continue
            decision_id = f"{captured_at.isoformat()}::{source}::{ticker}::{index}"
            conn.execute(
                """
                INSERT OR REPLACE INTO decisions (
                    decision_id, captured_at, source, ticker, category, side, planned_size,
                    expected_edge_net, expected_value_net_dollars, expected_roi_on_cost,
                    expected_roi_per_day, gate_reason, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    captured_at.isoformat(),
                    source,
                    ticker,
                    str(plan.get("category") or ""),
                    str(plan.get("side") or "yes"),
                    _to_float(plan.get("contracts_per_order")) or 0.0,
                    _to_float(plan.get("maker_entry_edge_net_fees") or plan.get("maker_entry_edge")),
                    _to_float(plan.get("expected_value_net_dollars") or plan.get("expected_value_dollars")),
                    _to_float(plan.get("expected_roi_on_cost_net")),
                    _to_float(plan.get("expected_roi_per_day_net")),
                    str(plan.get("skip_reason") or ""),
                    json.dumps(plan, separators=(",", ":"), sort_keys=True),
                ),
            )
        conn.commit()


def record_order_attempts(
    *,
    book_db_path: str | Path,
    captured_at: datetime,
    attempts: list[dict[str, Any]],
) -> None:
    if not attempts:
        return
    db_path = ensure_book_schema(book_db_path)
    seen_at = captured_at.isoformat()
    with _managed_connection(db_path) as conn:
        for attempt in attempts:
            order_id = str(attempt.get("order_id") or "").strip()
            if not order_id:
                continue
            ticker = str(attempt.get("market_ticker") or "").strip()
            planned_side = str(attempt.get("planned_side") or "").strip().lower()
            if planned_side not in {"yes", "no"}:
                planned_side = "yes"
            limit_price = _to_float(
                attempt.get("planned_entry_price_dollars")
                or attempt.get("planned_yes_bid_dollars")
            )
            conn.execute(
                """
                INSERT INTO orders (
                    order_id, client_order_id, ticker, side, action, limit_price_dollars,
                    post_only, status, created_time, last_update_time, last_seen_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    ticker=excluded.ticker,
                    side=excluded.side,
                    action=excluded.action,
                    limit_price_dollars=excluded.limit_price_dollars,
                    post_only=excluded.post_only,
                    status=excluded.status,
                    last_update_time=excluded.last_update_time,
                    last_seen_at=excluded.last_seen_at,
                    raw_json=excluded.raw_json
                """,
                (
                    order_id,
                    str(attempt.get("client_order_id") or ""),
                    ticker,
                    planned_side,
                    "buy",
                    limit_price,
                    1,
                    str(attempt.get("order_status") or attempt.get("result") or ""),
                    str(attempt.get("created_time") or ""),
                    str(attempt.get("last_update_time") or ""),
                    seen_at,
                    json.dumps(attempt, separators=(",", ":"), sort_keys=True),
                ),
            )
        conn.commit()


def record_reconcile_snapshot(
    *,
    book_db_path: str | Path,
    captured_at: datetime,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        for row in rows:
            ticker = str(row.get("ticker") or "").strip()
            order_id = str(row.get("order_id") or "").strip()
            if order_id:
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, ticker, side, action, limit_price_dollars,
                        post_only, status, created_time, last_update_time, last_seen_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        client_order_id=excluded.client_order_id,
                        ticker=excluded.ticker,
                        side=excluded.side,
                        limit_price_dollars=excluded.limit_price_dollars,
                        status=excluded.status,
                        created_time=excluded.created_time,
                        last_update_time=excluded.last_update_time,
                        last_seen_at=excluded.last_seen_at,
                        raw_json=excluded.raw_json
                    """,
                    (
                        order_id,
                        str(row.get("client_order_id") or ""),
                        ticker,
                        str(row.get("planned_side") or ""),
                        "buy",
                        _to_float(row.get("effective_price_dollars")),
                        1,
                        str(row.get("status") or ""),
                        str(row.get("created_time") or ""),
                        str(row.get("last_update_time") or ""),
                        captured_at.isoformat(),
                        json.dumps(row, separators=(",", ":"), sort_keys=True),
                    ),
                )
            if ticker:
                conn.execute(
                    """
                    INSERT INTO positions (
                        ticker, position_fp, market_exposure_dollars, realized_pnl_dollars,
                        fees_paid_dollars, resting_orders_count, updated_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        position_fp=excluded.position_fp,
                        market_exposure_dollars=excluded.market_exposure_dollars,
                        realized_pnl_dollars=excluded.realized_pnl_dollars,
                        fees_paid_dollars=excluded.fees_paid_dollars,
                        resting_orders_count=excluded.resting_orders_count,
                        updated_at=excluded.updated_at,
                        raw_json=excluded.raw_json
                    """,
                    (
                        ticker,
                        _to_float(row.get("position_fp")),
                        _to_float(row.get("market_exposure_dollars")),
                        _to_float(row.get("realized_pnl_dollars")),
                        _to_float(row.get("fees_paid_dollars")),
                        int(_to_float(row.get("resting_orders_count")) or 0),
                        captured_at.isoformat(),
                        json.dumps(row, separators=(",", ":"), sort_keys=True),
                    ),
                )
        conn.commit()


def record_series_fee_regime(
    *,
    book_db_path: str | Path,
    series_ticker: str,
    fee_type: str,
    fee_multiplier: float | None,
    scheduled_ts: str | None = None,
    active_from_ts: str | None = None,
) -> None:
    if not series_ticker:
        return
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO series_fee_regime (
                series_ticker, fee_type, fee_multiplier, scheduled_ts, active_from_ts, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(series_ticker) DO UPDATE SET
                fee_type=excluded.fee_type,
                fee_multiplier=excluded.fee_multiplier,
                scheduled_ts=excluded.scheduled_ts,
                active_from_ts=excluded.active_from_ts,
                updated_at=excluded.updated_at
            """,
            (
                series_ticker,
                fee_type,
                fee_multiplier,
                scheduled_ts or "",
                active_from_ts or "",
                _utc_now_iso(),
            ),
        )
        conn.commit()


def count_open_positions(*, book_db_path: str | Path) -> int:
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM positions
            WHERE ABS(COALESCE(position_fp, 0.0)) > 1e-9
               OR ABS(COALESCE(market_exposure_dollars, 0.0)) > 1e-9
            """
        ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def count_matching_open_orders(
    *,
    book_db_path: str | Path,
    ticker: str,
    side: str,
    limit_price_dollars: float,
    price_tolerance: float = 1e-6,
) -> int:
    normalized_ticker = ticker.strip()
    normalized_side = side.strip().lower()
    if not normalized_ticker or normalized_side not in {"yes", "no"}:
        return 0
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM orders
            WHERE ticker = ?
              AND LOWER(COALESCE(side, '')) = ?
              AND LOWER(COALESCE(status, '')) IN ('resting', 'open', 'pending')
              AND ABS(COALESCE(limit_price_dollars, 0.0) - ?) <= ?
            """,
            (
                normalized_ticker,
                normalized_side,
                float(limit_price_dollars),
                max(0.0, float(price_tolerance)),
            ),
        ).fetchone()
    return int(row[0] or 0) if row is not None else 0


def list_matching_open_orders(
    *,
    book_db_path: str | Path,
    ticker: str,
    side: str,
    limit_price_dollars: float,
    price_tolerance: float = 1e-6,
) -> list[dict[str, Any]]:
    normalized_ticker = ticker.strip()
    normalized_side = side.strip().lower()
    if not normalized_ticker or normalized_side not in {"yes", "no"}:
        return []
    db_path = ensure_book_schema(book_db_path)
    with _managed_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT order_id, ticker, side, limit_price_dollars, status, created_time, last_update_time, last_seen_at
            FROM orders
            WHERE ticker = ?
              AND LOWER(COALESCE(side, '')) = ?
              AND LOWER(COALESCE(status, '')) IN ('resting', 'open', 'pending')
              AND ABS(COALESCE(limit_price_dollars, 0.0) - ?) <= ?
            ORDER BY COALESCE(created_time, ''), COALESCE(last_update_time, ''), order_id
            """,
            (
                normalized_ticker,
                normalized_side,
                float(limit_price_dollars),
                max(0.0, float(price_tolerance)),
            ),
        ).fetchall()
    return [
        {
            "order_id": str(row["order_id"] or ""),
            "ticker": str(row["ticker"] or ""),
            "side": str(row["side"] or ""),
            "limit_price_dollars": row["limit_price_dollars"],
            "status": str(row["status"] or ""),
            "created_time": str(row["created_time"] or ""),
            "last_update_time": str(row["last_update_time"] or ""),
            "last_seen_at": str(row["last_seen_at"] or ""),
        }
        for row in rows
    ]


def update_order_statuses(
    *,
    book_db_path: str | Path,
    order_ids: list[str],
    status: str,
    updated_at: datetime | None = None,
) -> int:
    normalized_ids = [order_id.strip() for order_id in order_ids if order_id and order_id.strip()]
    if not normalized_ids:
        return 0
    db_path = ensure_book_schema(book_db_path)
    when = (updated_at or datetime.now(timezone.utc)).isoformat()
    with _managed_connection(db_path) as conn:
        placeholders = ",".join("?" for _ in normalized_ids)
        cursor = conn.execute(
            f"""
            UPDATE orders
            SET status = ?, last_update_time = ?, last_seen_at = ?
            WHERE order_id IN ({placeholders})
            """,
            [status.strip(), when, when, *normalized_ids],
        )
        conn.commit()
    return int(cursor.rowcount or 0)
