from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from betbot.kalshi_book import default_book_db_path, record_reconcile_snapshot
from betbot.kalshi_execution_journal import (
    append_execution_events,
    default_execution_journal_db_path,
    load_execution_events,
)
from betbot.kalshi_micro_execute import _http_request_json, _signed_kalshi_request
from betbot.kalshi_nonsports_scan import _parse_float
from betbot.live_smoke import KALSHI_API_ROOTS, KalshiSigner, _kalshi_sign_request
from betbot.onboarding import _parse_env_file

TERMINAL_ORDER_STATUSES = {
    "canceled",
    "cancelled",
    "executed",
    "filled",
    "completed",
    "expired",
    "rejected",
    "voided",
    "closed",
    "closed_not_found",
}


def _latest_output_file(output_dir: str, prefix: str) -> Path | None:
    paths = sorted(Path(output_dir).glob(f"{prefix}_*.json"), key=lambda path: path.stat().st_mtime)
    return paths[-1] if paths else None


def _load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _historical_order_by_id(
    *,
    env_data: dict[str, str],
    order_id: str,
    timeout_seconds: float,
    max_pages: int,
    http_request_json,
    sign_request: KalshiSigner,
) -> dict[str, Any] | None:
    cursor: str | None = None
    for _ in range(max_pages):
        query = {"limit": "200"}
        if cursor:
            query["cursor"] = cursor
        status_code, payload = _signed_kalshi_request(
            env_data=env_data,
            method="GET",
            path_with_query=f"/historical/orders?{urlencode(query)}",
            body=None,
            timeout_seconds=timeout_seconds,
            http_request_json=http_request_json,
            sign_request=sign_request,
        )
        if status_code != 200 or not isinstance(payload, dict):
            return None
        orders = payload.get("orders")
        if isinstance(orders, list):
            for order in orders:
                if isinstance(order, dict) and str(order.get("order_id") or "") == order_id:
                    return order
        next_cursor = payload.get("cursor")
        if not isinstance(next_cursor, str) or next_cursor.strip() == "":
            break
        cursor = next_cursor
    return None


def _fetch_order(
    *,
    env_data: dict[str, str],
    order_id: str,
    timeout_seconds: float,
    max_historical_pages: int,
    http_request_json,
    sign_request: KalshiSigner,
) -> tuple[dict[str, Any] | None, str]:
    status_code, payload = _signed_kalshi_request(
        env_data=env_data,
        method="GET",
        path_with_query=f"/portfolio/orders/{order_id}",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    if status_code == 200 and isinstance(payload, dict) and isinstance(payload.get("order"), dict):
        return payload["order"], "current"
    historical = _historical_order_by_id(
        env_data=env_data,
        order_id=order_id,
        timeout_seconds=timeout_seconds,
        max_pages=max_historical_pages,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    if historical is not None:
        return historical, "historical"
    return None, "missing"


def _fetch_queue_position(
    *,
    env_data: dict[str, str],
    order_id: str,
    timeout_seconds: float,
    http_request_json,
    sign_request: KalshiSigner,
) -> float | None:
    status_code, payload = _signed_kalshi_request(
        env_data=env_data,
        method="GET",
        path_with_query=f"/portfolio/orders/{order_id}/queue_position",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    if status_code == 200 and isinstance(payload, dict):
        return _parse_float(payload.get("queue_position_fp"))
    return None


def _fetch_position_map(
    *,
    env_data: dict[str, str],
    tickers: set[str],
    timeout_seconds: float,
    http_request_json,
    sign_request: KalshiSigner,
) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    for ticker in sorted(tickers):
        if not ticker:
            continue
        query = urlencode(
            {
                "ticker": ticker,
                "count_filter": "position,total_traded",
                "limit": "100",
            }
        )
        status_code, payload = _signed_kalshi_request(
            env_data=env_data,
            method="GET",
            path_with_query=f"/portfolio/positions?{query}",
            body=None,
            timeout_seconds=timeout_seconds,
            http_request_json=http_request_json,
            sign_request=sign_request,
        )
        if status_code != 200 or not isinstance(payload, dict):
            continue
        market_positions = payload.get("market_positions")
        if not isinstance(market_positions, list):
            continue
        for position in market_positions:
            if isinstance(position, dict) and str(position.get("ticker") or "") == ticker:
                positions[ticker] = position
                break
    return positions


def _write_reconcile_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "order_id",
        "source",
        "ticker",
        "client_order_id",
        "planned_side",
        "status",
        "yes_price_dollars",
        "no_price_dollars",
        "effective_price_dollars",
        "planned_entry_price_dollars",
        "fill_count_fp",
        "remaining_count_fp",
        "initial_count_fp",
        "queue_position_contracts",
        "maker_fill_cost_dollars",
        "maker_fees_dollars",
        "taker_fill_cost_dollars",
        "taker_fees_dollars",
        "created_time",
        "last_update_time",
        "position_fp",
        "market_exposure_dollars",
        "realized_pnl_dollars",
        "fees_paid_dollars",
        "resting_orders_count",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _history_midpoint_index(history_csv: Path) -> dict[str, list[tuple[datetime, float]]]:
    if not history_csv.exists():
        return {}
    index: dict[str, list[tuple[datetime, float]]] = {}
    with history_csv.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("market_ticker") or "").strip()
            captured_at = _parse_ts(row.get("captured_at"))
            if not ticker or captured_at is None:
                continue
            yes_bid = _parse_float(row.get("yes_bid_dollars"))
            yes_ask = _parse_float(row.get("yes_ask_dollars"))
            midpoint = None
            if isinstance(yes_bid, float) and isinstance(yes_ask, float):
                midpoint = (yes_bid + yes_ask) / 2.0
            elif isinstance(yes_bid, float):
                midpoint = yes_bid
            elif isinstance(yes_ask, float):
                midpoint = yes_ask
            if midpoint is None:
                continue
            index.setdefault(ticker, []).append((captured_at, midpoint))
    for ticker in index:
        index[ticker].sort(key=lambda item: item[0])
    return index


def _future_midpoint(
    *,
    observations: list[tuple[datetime, float]],
    target_ts: datetime,
) -> float | None:
    for observed_ts, midpoint in observations:
        if observed_ts >= target_ts:
            return midpoint
    return None


def _side_adjusted_markout(
    *,
    side: str,
    fill_price: float,
    future_mid_yes: float,
) -> float:
    side_normalized = str(side or "").strip().lower()
    if side_normalized == "no":
        return (1.0 - future_mid_yes) - fill_price
    return future_mid_yes - fill_price


def _order_journal_state(
    *,
    journal_db_path: Path,
    order_id: str,
) -> dict[str, Any]:
    events = load_execution_events(
        journal_db_path=journal_db_path,
        exchange_order_id=order_id,
        limit=2000,
    )
    filled_contracts = 0.0
    fill_fees = 0.0
    has_full_fill = False
    terminal_statuses: set[str] = set()
    cancel_confirmed_seen = False
    settlement_pnls: set[float] = set()
    latest_queue_position: float | None = None
    fill_events: list[dict[str, Any]] = []
    markout_horizons_logged: set[int] = set()

    for event in events:
        event_type = str(event.get("event_type") or "").strip()
        if event_type in {"partial_fill", "full_fill"}:
            filled_contracts += max(0.0, _parse_float(event.get("contracts_fp")) or 0.0)
            fill_fees += max(0.0, _parse_float(event.get("fee_dollars")) or 0.0)
            fill_ts = _parse_ts(event.get("captured_at_utc"))
            fill_price = _parse_float(event.get("limit_price_dollars"))
            fill_side = str(event.get("side") or "").strip().lower()
            fill_contracts = max(0.0, _parse_float(event.get("contracts_fp")) or 0.0)
            if fill_ts is not None and isinstance(fill_price, float) and fill_contracts > 0:
                fill_events.append(
                    {
                        "captured_at_utc": fill_ts,
                        "limit_price_dollars": fill_price,
                        "side": fill_side or "yes",
                        "contracts_fp": fill_contracts,
                    }
                )
            if event_type == "full_fill":
                has_full_fill = True
        elif event_type == "order_terminal":
            status = str(event.get("status") or event.get("result") or "").strip().lower()
            if status:
                terminal_statuses.add(status)
        elif event_type == "cancel_confirmed":
            cancel_confirmed_seen = True
        elif event_type == "settlement_outcome":
            pnl = _parse_float(event.get("realized_pnl_dollars"))
            if pnl is not None:
                settlement_pnls.add(round(pnl, 6))
        elif event_type == "queue_snapshot":
            queue_position = _parse_float(event.get("queue_position_contracts"))
            if queue_position is not None:
                latest_queue_position = queue_position
        elif event_type == "markout_snapshot":
            payload = event.get("payload")
            if isinstance(payload, dict):
                payload_inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                horizon_value = payload.get("horizon_seconds")
                if horizon_value in (None, ""):
                    horizon_value = payload_inner.get("horizon_seconds")
                try:
                    horizon_seconds = int(float(horizon_value))
                except (TypeError, ValueError):
                    horizon_seconds = None
                if isinstance(horizon_seconds, int) and horizon_seconds > 0:
                    markout_horizons_logged.add(horizon_seconds)

    return {
        "filled_contracts": filled_contracts,
        "fill_fees": fill_fees,
        "has_full_fill": has_full_fill,
        "terminal_statuses": terminal_statuses,
        "cancel_confirmed_seen": cancel_confirmed_seen,
        "settlement_pnls": settlement_pnls,
        "latest_queue_position": latest_queue_position,
        "fill_events": fill_events,
        "markout_horizons_logged": markout_horizons_logged,
    }


def run_kalshi_micro_reconcile(
    *,
    env_file: str,
    execute_summary_file: str | None = None,
    output_dir: str = "outputs",
    book_db_path: str | None = None,
    execution_journal_db_path: str | None = None,
    timeout_seconds: float = 15.0,
    max_historical_pages: int = 5,
    http_request_json=_http_request_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)

    kalshi_env = (env_data.get("KALSHI_ENV") or "prod").strip().lower()
    if kalshi_env not in KALSHI_API_ROOTS:
        raise ValueError(f"Unsupported KALSHI_ENV={kalshi_env!r}")

    summary_path = Path(execute_summary_file) if execute_summary_file else _latest_output_file(
        output_dir,
        "kalshi_micro_execute_summary",
    )
    if summary_path is None or not summary_path.exists():
        raise ValueError("No kalshi_micro_execute_summary JSON file was found")

    execute_summary = _load_json_file(summary_path)
    execute_history_csv = str(execute_summary.get("history_csv") or "").strip()
    history_path = (
        Path(execute_history_csv)
        if execute_history_csv
        else (Path(output_dir) / "kalshi_nonsports_history.csv")
    )
    history_midpoint_index = _history_midpoint_index(history_path)
    execute_summary_journal = str(execute_summary.get("execution_journal_db_path") or "").strip()
    execute_summary_legacy_log = str(execute_summary.get("execution_event_log_csv") or "").strip()
    if execution_journal_db_path:
        journal_path = Path(execution_journal_db_path)
    elif execute_summary_journal:
        journal_path = Path(execute_summary_journal)
    elif execute_summary_legacy_log:
        legacy_path = Path(execute_summary_legacy_log)
        journal_path = legacy_path.with_suffix(".sqlite3") if legacy_path.suffix.lower() == ".csv" else legacy_path
    else:
        journal_path = default_execution_journal_db_path(output_dir)
    journal_run_id = f"kalshi_micro_reconcile::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"

    attempts = execute_summary.get("attempts")
    attempt_by_order_id: dict[str, dict[str, Any]] = {}
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            order_id = str(attempt.get("order_id") or "").strip()
            if order_id:
                attempt_by_order_id[order_id] = attempt
    order_ids = [
        str(attempt.get("order_id") or "")
        for attempt in attempts
        if isinstance(attempt, dict) and str(attempt.get("order_id") or "").strip()
    ] if isinstance(attempts, list) else []

    if not order_ids:
        stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"kalshi_micro_reconcile_{stamp}.csv"
        _write_reconcile_csv(csv_path, [])
        summary = {
            "env_file": str(env_path),
            "execute_summary_file": str(summary_path),
            "captured_at": captured_at.isoformat(),
            "status": "no_order_ids",
            "orders_requested": 0,
            "orders_found": 0,
            "execution_journal_db_path": str(journal_path),
            "execution_journal_run_id": journal_run_id,
            "execution_journal_rows_written": 0,
            "book_db_path": str(Path(book_db_path) if book_db_path else default_book_db_path(output_dir)),
            "output_csv": str(csv_path),
        }
        output_path = out_dir / f"kalshi_micro_reconcile_summary_{stamp}.json"
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["output_file"] = str(output_path)
        return summary

    details: list[tuple[dict[str, Any], str]] = []
    tickers: set[str] = set()
    missing_order_ids: list[str] = []
    for order_id in order_ids:
        order, source = _fetch_order(
            env_data=env_data,
            order_id=order_id,
            timeout_seconds=timeout_seconds,
            max_historical_pages=max_historical_pages,
            http_request_json=http_request_json,
            sign_request=sign_request,
        )
        if order is None:
            missing_order_ids.append(order_id)
            continue
        details.append((order, source))
        tickers.add(str(order.get("ticker") or ""))

    position_map = _fetch_position_map(
        env_data=env_data,
        tickers=tickers,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )

    rows: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    total_fees_paid = 0.0
    total_realized_pnl = 0.0
    total_market_exposure = 0.0
    total_resting_orders_count = 0
    journal_events: list[dict[str, Any]] = []
    execute_run_id = str(execute_summary.get("execution_journal_run_id") or "").strip()

    for order, source in details:
        order_id = str(order.get("order_id") or "")
        attempt = attempt_by_order_id.get(order_id, {})
        ticker = str(order.get("ticker") or "")
        status = str(order.get("status") or "")
        position = position_map.get(ticker, {})
        queue_position = None
        if status == "resting":
            queue_position = _fetch_queue_position(
                env_data=env_data,
                order_id=order_id,
                timeout_seconds=timeout_seconds,
                http_request_json=http_request_json,
                sign_request=sign_request,
            )

        fees_paid = _parse_float(position.get("fees_paid_dollars")) or 0.0
        realized_pnl = _parse_float(position.get("realized_pnl_dollars")) or 0.0
        market_exposure = _parse_float(position.get("market_exposure_dollars")) or 0.0
        resting_orders_count = int(position.get("resting_orders_count") or 0) if position else 0
        total_fees_paid += fees_paid
        total_realized_pnl += realized_pnl
        total_market_exposure += market_exposure
        total_resting_orders_count += resting_orders_count
        status_counts[status] = status_counts.get(status, 0) + 1

        yes_price = _parse_float(order.get("yes_price_dollars"))
        no_price = round(1.0 - yes_price, 4) if isinstance(yes_price, float) else None
        planned_side = str(attempt.get("planned_side") or attempt.get("side") or "").strip().lower()
        if planned_side not in {"yes", "no"}:
            planned_side = ""
        effective_price = no_price if planned_side == "no" else yes_price
        planned_entry_price = _parse_float(attempt.get("planned_entry_price_dollars"))

        rows.append(
            {
                "order_id": order_id,
                "source": source,
                "ticker": ticker,
                "client_order_id": str(order.get("client_order_id") or ""),
                "planned_side": planned_side,
                "status": status,
                "yes_price_dollars": yes_price if yes_price is not None else "",
                "no_price_dollars": no_price if no_price is not None else "",
                "effective_price_dollars": effective_price if effective_price is not None else "",
                "planned_entry_price_dollars": planned_entry_price if planned_entry_price is not None else "",
                "fill_count_fp": _parse_float(order.get("fill_count_fp")) or "",
                "remaining_count_fp": _parse_float(order.get("remaining_count_fp")) or "",
                "initial_count_fp": _parse_float(order.get("initial_count_fp")) or "",
                "queue_position_contracts": queue_position if queue_position is not None else "",
                "maker_fill_cost_dollars": _parse_float(order.get("maker_fill_cost_dollars")) or "",
                "maker_fees_dollars": _parse_float(order.get("maker_fees_dollars")) or "",
                "taker_fill_cost_dollars": _parse_float(order.get("taker_fill_cost_dollars")) or "",
                "taker_fees_dollars": _parse_float(order.get("taker_fees_dollars")) or "",
                "created_time": str(order.get("created_time") or ""),
                "last_update_time": str(order.get("last_update_time") or ""),
                "position_fp": _parse_float(position.get("position_fp")) if position else "",
                "market_exposure_dollars": market_exposure if position else "",
                "realized_pnl_dollars": realized_pnl if position else "",
                "fees_paid_dollars": fees_paid if position else "",
                "resting_orders_count": resting_orders_count if position else "",
            }
        )

        # Append reconcile backfills into the execution journal without
        # duplicating previously logged fill/terminal state.
        if order_id:
            existing_state = _order_journal_state(
                journal_db_path=journal_path,
                order_id=order_id,
            )
            planned_side = str(attempt.get("planned_side") or "").strip().lower()
            if planned_side not in {"yes", "no"}:
                planned_side = "yes"
            fill_count = max(0.0, _parse_float(order.get("fill_count_fp")) or 0.0)
            remaining_count = max(0.0, _parse_float(order.get("remaining_count_fp")) or 0.0)
            initial_count = _parse_float(order.get("initial_count_fp"))
            if initial_count is None:
                initial_count = _parse_float(attempt.get("planned_contracts"))
            if initial_count is None:
                initial_count = fill_count + remaining_count
            initial_count = max(fill_count, remaining_count, float(initial_count or 0.0))

            yes_price = _parse_float(order.get("yes_price_dollars"))
            no_price = round(1.0 - yes_price, 6) if isinstance(yes_price, float) else None
            effective_fill_price = no_price if planned_side == "no" else yes_price
            if effective_fill_price is None:
                effective_fill_price = _parse_float(attempt.get("planned_entry_price_dollars"))

            maker_fees = max(0.0, _parse_float(order.get("maker_fees_dollars")) or 0.0)
            taker_fees = max(0.0, _parse_float(order.get("taker_fees_dollars")) or 0.0)
            total_order_fees = maker_fees + taker_fees
            fee_delta = max(0.0, total_order_fees - float(existing_state.get("fill_fees") or 0.0))
            filled_delta = max(0.0, fill_count - float(existing_state.get("filled_contracts") or 0.0))
            has_full_fill_logged = bool(existing_state.get("has_full_fill"))
            terminal_statuses_logged = set(existing_state.get("terminal_statuses") or set())
            cancel_confirmed_logged = bool(existing_state.get("cancel_confirmed_seen"))
            settlement_pnls_logged = set(existing_state.get("settlement_pnls") or set())
            latest_queue_position = _parse_float(existing_state.get("latest_queue_position"))
            existing_fill_events = list(existing_state.get("fill_events") or [])
            markout_horizons_logged = set(existing_state.get("markout_horizons_logged") or set())
            new_fill_events_for_markout: list[dict[str, Any]] = []

            order_ts = _parse_ts(order.get("last_update_time")) or _parse_ts(order.get("created_time")) or captured_at
            order_ts_iso = order_ts.astimezone(timezone.utc).isoformat()
            event_base = {
                "run_id": journal_run_id,
                "captured_at_utc": order_ts_iso,
                "market_ticker": ticker,
                "event_family": str(attempt.get("category") or ""),
                "side": planned_side,
                "limit_price_dollars": effective_fill_price,
                "contracts_fp": _parse_float(order.get("initial_count_fp")) or _parse_float(attempt.get("planned_contracts")) or 0.0,
                "client_order_id": str(order.get("client_order_id") or attempt.get("client_order_id") or ""),
                "exchange_order_id": order_id,
                "parent_order_id": str(attempt.get("parent_order_id") or ""),
                "best_yes_bid_dollars": _parse_float(attempt.get("current_best_yes_bid_dollars")),
                "best_yes_ask_dollars": _parse_float(attempt.get("planned_yes_ask_dollars")),
                "best_no_bid_dollars": _parse_float(attempt.get("current_best_no_bid_dollars")),
                "best_no_ask_dollars": _parse_float(attempt.get("current_best_no_ask_dollars")),
                "spread_dollars": _parse_float(attempt.get("market_spread_dollars")),
                "visible_depth_contracts": _parse_float(attempt.get("current_best_same_side_bid_size_contracts")),
                "queue_position_contracts": queue_position,
                "signal_score": _parse_float(attempt.get("execution_forecast_edge_net_per_contract_dollars")),
                "signal_age_seconds": _parse_float(attempt.get("signal_age_seconds")),
                "time_to_close_seconds": (
                    (_parse_float(attempt.get("market_hours_to_close")) or 0.0) * 3600.0
                    if _parse_float(attempt.get("market_hours_to_close")) is not None
                    else None
                ),
                "latency_ms": None,
                "websocket_lag_ms": _parse_float(attempt.get("websocket_lag_ms")),
                "api_latency_ms": _parse_float(attempt.get("api_latency_ms")),
                "payload": {
                    "source": "reconcile",
                    "reconcile_fetch_source": source,
                    "execute_run_id": execute_run_id,
                },
            }

            is_full_fill_now = (
                fill_count > 0.0
                and (
                    (initial_count > 0.0 and fill_count >= initial_count - 1e-9)
                    or status.strip().lower() in {"executed", "filled", "completed"}
                )
            )
            if is_full_fill_now and not has_full_fill_logged:
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "full_fill",
                        "contracts_fp": filled_delta if filled_delta > 1e-9 else 0.0,
                        "fee_dollars": fee_delta,
                        "maker_fee_dollars": maker_fees,
                        "taker_fee_dollars": taker_fees,
                        "status": status,
                        "result": "reconcile_fill_backfill",
                    }
                )
                if filled_delta > 1e-9 and isinstance(effective_fill_price, float):
                    new_fill_events_for_markout.append(
                        {
                            "captured_at_utc": order_ts,
                            "limit_price_dollars": effective_fill_price,
                            "side": planned_side or "yes",
                            "contracts_fp": filled_delta,
                        }
                    )
            elif filled_delta > 1e-9:
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "partial_fill",
                        "contracts_fp": filled_delta,
                        "fee_dollars": fee_delta,
                        "maker_fee_dollars": maker_fees,
                        "taker_fee_dollars": taker_fees,
                        "status": status,
                        "result": "reconcile_fill_backfill",
                    }
                )
                if isinstance(effective_fill_price, float):
                    new_fill_events_for_markout.append(
                        {
                            "captured_at_utc": order_ts,
                            "limit_price_dollars": effective_fill_price,
                            "side": planned_side or "yes",
                            "contracts_fp": filled_delta,
                        }
                    )

            if status.strip().lower() in {"canceled", "cancelled"} and not cancel_confirmed_logged:
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "cancel_confirmed",
                        "contracts_fp": max(0.0, remaining_count),
                        "status": status,
                        "result": "reconcile_cancel_confirmed",
                    }
                )

            if queue_position is not None and (
                latest_queue_position is None or abs(queue_position - latest_queue_position) > 1e-9
            ):
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "queue_snapshot",
                        "contracts_fp": max(0.0, initial_count),
                        "queue_position_contracts": queue_position,
                        "status": status,
                        "result": "reconcile_queue_snapshot",
                    }
                )

            normalized_terminal_status = status.strip().lower()
            if (
                normalized_terminal_status
                and normalized_terminal_status in TERMINAL_ORDER_STATUSES
                and normalized_terminal_status not in terminal_statuses_logged
            ):
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "order_terminal",
                        "status": normalized_terminal_status,
                        "result": f"reconcile_terminal_{normalized_terminal_status}",
                    }
                )

            if (
                normalized_terminal_status in TERMINAL_ORDER_STATUSES
                and round(realized_pnl, 6) not in settlement_pnls_logged
            ):
                journal_events.append(
                    {
                        **event_base,
                        "event_type": "settlement_outcome",
                        "status": normalized_terminal_status or status,
                        "result": "reconcile_settlement_outcome",
                        "realized_pnl_dollars": realized_pnl,
                        "fee_dollars": fees_paid,
                        "payload": {
                            "source": "reconcile",
                            "reconcile_fetch_source": source,
                            "position_fp": _parse_float(position.get("position_fp")) if position else None,
                            "market_exposure_dollars": market_exposure if position else None,
                            "execute_run_id": execute_run_id,
                        },
                    }
                )

            if ticker and ticker in history_midpoint_index:
                fill_events_for_markout = [*existing_fill_events, *new_fill_events_for_markout]
                observations = history_midpoint_index.get(ticker, [])
                for horizon_seconds in (10, 60, 300):
                    if horizon_seconds in markout_horizons_logged:
                        continue
                    weighted_markout = 0.0
                    weighted_contracts = 0.0
                    for fill_event in fill_events_for_markout:
                        fill_ts = fill_event.get("captured_at_utc")
                        fill_price = _parse_float(fill_event.get("limit_price_dollars"))
                        fill_side = str(fill_event.get("side") or planned_side or "yes").strip().lower()
                        fill_contracts = max(0.0, _parse_float(fill_event.get("contracts_fp")) or 0.0)
                        if not isinstance(fill_ts, datetime) or not isinstance(fill_price, float) or fill_contracts <= 0.0:
                            continue
                        future_mid_yes = _future_midpoint(
                            observations=observations,
                            target_ts=fill_ts + timedelta(seconds=horizon_seconds),
                        )
                        if not isinstance(future_mid_yes, float):
                            continue
                        side_adjusted_markout = _side_adjusted_markout(
                            side=fill_side,
                            fill_price=fill_price,
                            future_mid_yes=future_mid_yes,
                        )
                        weighted_markout += side_adjusted_markout * fill_contracts
                        weighted_contracts += fill_contracts
                    if weighted_contracts <= 0.0:
                        continue
                    markout_per_contract = weighted_markout / weighted_contracts
                    journal_events.append(
                        {
                            **event_base,
                            "captured_at_utc": captured_at.astimezone(timezone.utc).isoformat(),
                            "event_type": "markout_snapshot",
                            "contracts_fp": weighted_contracts,
                            "markout_10s_dollars": (
                                markout_per_contract if horizon_seconds == 10 else None
                            ),
                            "markout_60s_dollars": (
                                markout_per_contract if horizon_seconds == 60 else None
                            ),
                            "markout_300s_dollars": (
                                markout_per_contract if horizon_seconds == 300 else None
                            ),
                            "status": normalized_terminal_status or status,
                            "result": f"reconcile_markout_{horizon_seconds}s",
                            "horizon_seconds": horizon_seconds,
                            "markout_per_contract_dollars": markout_per_contract,
                            "markout_contracts": weighted_contracts,
                            "source": "reconcile",
                            "reconcile_fetch_source": source,
                            "execute_run_id": execute_run_id,
                        }
                    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_micro_reconcile_{stamp}.csv"
    _write_reconcile_csv(csv_path, rows)
    effective_book_db_path = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)
    record_reconcile_snapshot(
        book_db_path=effective_book_db_path,
        captured_at=captured_at,
        rows=rows,
    )
    execution_journal_rows_written = append_execution_events(
        journal_db_path=journal_path,
        events=journal_events,
        default_run_id=journal_run_id,
        default_captured_at=captured_at,
    )
    markout_snapshot_events_generated = sum(
        1 for event in journal_events if str(event.get("event_type") or "").strip() == "markout_snapshot"
    )

    summary = {
        "env_file": str(env_path),
        "execute_summary_file": str(summary_path),
        "captured_at": captured_at.isoformat(),
        "status": "ready",
        "orders_requested": len(order_ids),
        "orders_found": len(rows),
        "orders_missing": missing_order_ids,
        "execution_journal_db_path": str(journal_path),
        "execution_journal_run_id": journal_run_id,
        "execution_journal_rows_written": execution_journal_rows_written,
        "markout_snapshot_events_generated": markout_snapshot_events_generated,
        "history_csv": str(history_path),
        "book_db_path": str(effective_book_db_path),
        "status_counts": status_counts,
        "total_fees_paid_dollars": round(total_fees_paid, 4),
        "total_realized_pnl_dollars": round(total_realized_pnl, 4),
        "total_market_exposure_dollars": round(total_market_exposure, 4),
        "total_resting_orders_count": total_resting_orders_count,
        "rows": rows,
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_micro_reconcile_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
