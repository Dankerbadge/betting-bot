from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any


def default_execution_journal_db_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_execution_journal.sqlite3"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_iso(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.astimezone(timezone.utc).isoformat()
    text = str(value or "").strip()
    if text:
        return text
    return _utc_now_iso()


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


def ensure_execution_journal_schema(path: str | Path) -> Path:
    db_path = Path(path)
    with _managed_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS execution_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                captured_at_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                market_ticker TEXT,
                event_family TEXT,
                side TEXT,
                limit_price_dollars REAL,
                contracts_fp REAL,
                client_order_id TEXT,
                exchange_order_id TEXT,
                parent_order_id TEXT,
                best_yes_bid_dollars REAL,
                best_yes_ask_dollars REAL,
                best_no_bid_dollars REAL,
                best_no_ask_dollars REAL,
                spread_dollars REAL,
                visible_depth_contracts REAL,
                queue_position_contracts REAL,
                signal_score REAL,
                signal_age_seconds REAL,
                time_to_close_seconds REAL,
                latency_ms REAL,
                websocket_lag_ms REAL,
                api_latency_ms REAL,
                fee_dollars REAL,
                maker_fee_dollars REAL,
                taker_fee_dollars REAL,
                realized_pnl_dollars REAL,
                markout_10s_dollars REAL,
                markout_60s_dollars REAL,
                markout_300s_dollars REAL,
                result TEXT,
                status TEXT,
                payload_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_execution_events_captured_at
                ON execution_events (captured_at_utc);
            CREATE INDEX IF NOT EXISTS idx_execution_events_event_type
                ON execution_events (event_type);
            CREATE INDEX IF NOT EXISTS idx_execution_events_market_ticker
                ON execution_events (market_ticker, captured_at_utc);
            CREATE INDEX IF NOT EXISTS idx_execution_events_exchange_order_id
                ON execution_events (exchange_order_id, captured_at_utc);
            CREATE INDEX IF NOT EXISTS idx_execution_events_client_order_id
                ON execution_events (client_order_id, captured_at_utc);
            CREATE INDEX IF NOT EXISTS idx_execution_events_run_id
                ON execution_events (run_id, captured_at_utc);
            """
        )
        conn.commit()
    return db_path


def _normalized_event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event)
    for key in (
        "run_id",
        "captured_at_utc",
        "event_type",
        "market_ticker",
        "event_family",
        "side",
        "limit_price_dollars",
        "contracts_fp",
        "client_order_id",
        "exchange_order_id",
        "parent_order_id",
        "best_yes_bid_dollars",
        "best_yes_ask_dollars",
        "best_no_bid_dollars",
        "best_no_ask_dollars",
        "spread_dollars",
        "visible_depth_contracts",
        "queue_position_contracts",
        "signal_score",
        "signal_age_seconds",
        "time_to_close_seconds",
        "latency_ms",
        "websocket_lag_ms",
        "api_latency_ms",
        "fee_dollars",
        "maker_fee_dollars",
        "taker_fee_dollars",
        "realized_pnl_dollars",
        "markout_10s_dollars",
        "markout_60s_dollars",
        "markout_300s_dollars",
        "result",
        "status",
    ):
        payload.pop(key, None)
    return payload


def append_execution_events(
    *,
    journal_db_path: str | Path,
    events: list[dict[str, Any]],
    default_run_id: str | None = None,
    default_captured_at: datetime | str | None = None,
) -> int:
    if not events:
        return 0
    db_path = ensure_execution_journal_schema(journal_db_path)
    written = 0
    with _managed_connection(db_path) as conn:
        for raw_event in events:
            if not isinstance(raw_event, dict):
                continue
            event_type = str(raw_event.get("event_type") or "").strip()
            if not event_type:
                continue
            run_id = str(raw_event.get("run_id") or default_run_id or "").strip() or "unknown_run"
            captured_at = _as_iso(raw_event.get("captured_at_utc") or default_captured_at)
            payload = _normalized_event_payload(raw_event)
            conn.execute(
                """
                INSERT INTO execution_events (
                    run_id, captured_at_utc, event_type, market_ticker, event_family, side,
                    limit_price_dollars, contracts_fp, client_order_id, exchange_order_id, parent_order_id,
                    best_yes_bid_dollars, best_yes_ask_dollars, best_no_bid_dollars, best_no_ask_dollars,
                    spread_dollars, visible_depth_contracts, queue_position_contracts,
                    signal_score, signal_age_seconds, time_to_close_seconds,
                    latency_ms, websocket_lag_ms, api_latency_ms,
                    fee_dollars, maker_fee_dollars, taker_fee_dollars, realized_pnl_dollars,
                    markout_10s_dollars, markout_60s_dollars, markout_300s_dollars,
                    result, status, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    captured_at,
                    event_type,
                    str(raw_event.get("market_ticker") or "").strip(),
                    str(raw_event.get("event_family") or "").strip(),
                    str(raw_event.get("side") or "").strip().lower(),
                    _to_float(raw_event.get("limit_price_dollars")),
                    _to_float(raw_event.get("contracts_fp")),
                    str(raw_event.get("client_order_id") or "").strip(),
                    str(raw_event.get("exchange_order_id") or "").strip(),
                    str(raw_event.get("parent_order_id") or "").strip(),
                    _to_float(raw_event.get("best_yes_bid_dollars")),
                    _to_float(raw_event.get("best_yes_ask_dollars")),
                    _to_float(raw_event.get("best_no_bid_dollars")),
                    _to_float(raw_event.get("best_no_ask_dollars")),
                    _to_float(raw_event.get("spread_dollars")),
                    _to_float(raw_event.get("visible_depth_contracts")),
                    _to_float(raw_event.get("queue_position_contracts")),
                    _to_float(raw_event.get("signal_score")),
                    _to_float(raw_event.get("signal_age_seconds")),
                    _to_float(raw_event.get("time_to_close_seconds")),
                    _to_float(raw_event.get("latency_ms")),
                    _to_float(raw_event.get("websocket_lag_ms")),
                    _to_float(raw_event.get("api_latency_ms")),
                    _to_float(raw_event.get("fee_dollars")),
                    _to_float(raw_event.get("maker_fee_dollars")),
                    _to_float(raw_event.get("taker_fee_dollars")),
                    _to_float(raw_event.get("realized_pnl_dollars")),
                    _to_float(raw_event.get("markout_10s_dollars")),
                    _to_float(raw_event.get("markout_60s_dollars")),
                    _to_float(raw_event.get("markout_300s_dollars")),
                    str(raw_event.get("result") or "").strip(),
                    str(raw_event.get("status") or "").strip(),
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                ),
            )
            written += 1
        conn.commit()
    return written


def append_execution_event(
    *,
    journal_db_path: str | Path,
    event: dict[str, Any],
    default_run_id: str | None = None,
    default_captured_at: datetime | str | None = None,
) -> int:
    return append_execution_events(
        journal_db_path=journal_db_path,
        events=[event],
        default_run_id=default_run_id,
        default_captured_at=default_captured_at,
    )


def load_execution_events(
    *,
    journal_db_path: str | Path,
    event_types: tuple[str, ...] | None = None,
    market_ticker: str | None = None,
    exchange_order_id: str | None = None,
    client_order_id: str | None = None,
    run_id: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    db_path = ensure_execution_journal_schema(journal_db_path)
    where_parts: list[str] = []
    params: list[Any] = []
    if event_types:
        placeholders = ",".join("?" for _ in event_types)
        where_parts.append(f"event_type IN ({placeholders})")
        params.extend(event_types)
    if market_ticker:
        where_parts.append("market_ticker = ?")
        params.append(market_ticker)
    if exchange_order_id:
        where_parts.append("exchange_order_id = ?")
        params.append(exchange_order_id)
    if client_order_id:
        where_parts.append("client_order_id = ?")
        params.append(client_order_id)
    if run_id:
        where_parts.append("run_id = ?")
        params.append(run_id)
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    sql = f"""
        SELECT *
        FROM execution_events
        {where_clause}
        ORDER BY captured_at_utc DESC, event_id DESC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    with _managed_connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        payload_text = str(item.get("payload_json") or "")
        if payload_text:
            try:
                item["payload"] = json.loads(payload_text)
            except json.JSONDecodeError:
                item["payload"] = {}
        else:
            item["payload"] = {}
        results.append(item)
    return results
