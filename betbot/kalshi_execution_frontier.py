from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

from betbot.kalshi_execution_journal import (
    default_execution_journal_db_path,
    load_execution_events,
)
from betbot.runtime_version import build_runtime_version_block


FRONTIER_BUCKET_FIELDNAMES = [
    "bucket",
    "orders_submitted",
    "fill_rate",
    "full_fill_rate",
    "median_time_to_fill_seconds",
    "p90_time_to_fill_seconds",
    "markout_10s_side_adjusted",
    "markout_60s_side_adjusted",
    "markout_300s_side_adjusted",
    "markout_10s_samples",
    "markout_60s_samples",
    "markout_300s_samples",
    "markout_horizons_trusted",
    "markout_horizons_untrusted_reason",
    "fee_spread_cancel_leakage_dollars_per_order",
    "expected_net_edge_after_costs_per_contract",
    "break_even_edge_per_contract",
]

_DECIMAL_ZERO = Decimal("0")
_DEFAULT_MIN_MARKOUT_SAMPLES_10S = 3
_DEFAULT_MIN_MARKOUT_SAMPLES_60S = 3
_DEFAULT_MIN_MARKOUT_SAMPLES_300S = 2


def _parse_float(value: Any) -> float | None:
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


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        candidate = value
    elif isinstance(value, (int, float)):
        candidate = Decimal(str(value))
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            candidate = Decimal(text)
        except InvalidOperation:
            return None
    if candidate.is_nan() or candidate.is_infinite():
        return None
    return candidate


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


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _p90(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = int(math.ceil(0.9 * len(ordered))) - 1
    idx = max(0, min(idx, len(ordered) - 1))
    return ordered[idx]


def _spread_bucket(spread: float | None) -> str:
    if spread is None:
        return "spread_unknown"
    if spread < 0.01:
        return "spread_tight"
    if spread < 0.03:
        return "spread_mid"
    return "spread_wide"


def _time_bucket(time_to_close_seconds: float | None) -> str:
    if time_to_close_seconds is None:
        return "ttc_unknown"
    hours = time_to_close_seconds / 3600.0
    if hours <= 6.0:
        return "ttc_near"
    if hours <= 24.0:
        return "ttc_short"
    return "ttc_long"


def _aggressiveness_bucket(value: float | None) -> str:
    if value is None:
        return "aggr_unknown"
    if value < 0.34:
        return "aggr_passive"
    if value < 0.67:
        return "aggr_mid"
    return "aggr_aggressive"


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
            last_price = _parse_float(row.get("last_price_dollars"))
            midpoint = None
            if isinstance(yes_bid, float) and isinstance(yes_ask, float):
                midpoint = (yes_bid + yes_ask) / 2.0
            elif isinstance(last_price, float):
                midpoint = last_price
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
    side_normalized = side.strip().lower()
    if side_normalized == "no":
        fill_price_no = fill_price
        future_no_mid = 1.0 - future_mid_yes
        return future_no_mid - fill_price_no
    return future_mid_yes - fill_price


def _order_key(event: dict[str, Any]) -> str:
    exchange_order_id = str(event.get("exchange_order_id") or "").strip()
    if exchange_order_id:
        return f"exchange:{exchange_order_id}"
    client_order_id = str(event.get("client_order_id") or "").strip()
    if client_order_id:
        return f"client:{client_order_id}"
    market_ticker = str(event.get("market_ticker") or "").strip()
    event_id = str(event.get("event_id") or "").strip()
    return f"fallback:{market_ticker}:{event_id}"


def run_kalshi_execution_frontier(
    *,
    output_dir: str = "outputs",
    journal_db_path: str | None = None,
    history_csv: str | None = None,
    recent_events: int = 20000,
    min_markout_samples_10s: int = _DEFAULT_MIN_MARKOUT_SAMPLES_10S,
    min_markout_samples_60s: int = _DEFAULT_MIN_MARKOUT_SAMPLES_60S,
    min_markout_samples_300s: int = _DEFAULT_MIN_MARKOUT_SAMPLES_300S,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    run_id = f"kalshi_execution_frontier::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    journal_path = Path(journal_db_path) if journal_db_path else default_execution_journal_db_path(output_dir)
    history_path = Path(history_csv) if history_csv else (out_dir / "kalshi_nonsports_history.csv")
    safe_min_markout_samples_10s = max(1, int(min_markout_samples_10s))
    safe_min_markout_samples_60s = max(1, int(min_markout_samples_60s))
    safe_min_markout_samples_300s = max(1, int(min_markout_samples_300s))

    events = load_execution_events(
        journal_db_path=journal_path,
        limit=max(1, int(recent_events)),
    )
    events.sort(
        key=lambda event: (
            _parse_ts(event.get("captured_at_utc")) or datetime.min.replace(tzinfo=timezone.utc),
            int(event.get("event_id") or 0),
        )
    )
    history_index = _history_midpoint_index(history_path)

    orders: dict[str, dict[str, Any]] = {}
    fill_samples: list[dict[str, Any]] = []

    for event in events:
        event_type = str(event.get("event_type") or "").strip()
        if not event_type:
            continue
        key = _order_key(event)
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        order = orders.setdefault(
            key,
            {
                "key": key,
                "market_ticker": str(event.get("market_ticker") or "").strip(),
                "side": str(event.get("side") or "").strip().lower(),
                "submitted_at": None,
                "first_fill_at": None,
                "terminal_at": None,
                "terminal_status": "",
                "submitted_contracts": 0.0,
                "filled_contracts": 0.0,
                "has_full_fill": False,
                "fee_dollars": 0.0,
                "spread_dollars": _parse_float(event.get("spread_dollars")),
                "time_to_close_seconds": _parse_float(event.get("time_to_close_seconds")),
                "quote_aggressiveness": _parse_float(payload.get("quote_aggressiveness")),
                "forecast_edge_net_per_contract": _parse_float(payload.get("execution_forecast_edge_net_per_contract_dollars")),
                "cancel_confirmed_count": 0,
                "markout_10s_weighted": 0.0,
                "markout_60s_weighted": 0.0,
                "markout_300s_weighted": 0.0,
                "markout_10s_contracts": 0.0,
                "markout_60s_contracts": 0.0,
                "markout_300s_contracts": 0.0,
                "markout_10s_samples": 0,
                "markout_60s_samples": 0,
                "markout_300s_samples": 0,
                "markout_contracts": 0.0,
            },
        )
        if not order["market_ticker"]:
            order["market_ticker"] = str(event.get("market_ticker") or "").strip()
        if not order["side"]:
            order["side"] = str(event.get("side") or "").strip().lower()
        spread_value = _parse_float(event.get("spread_dollars"))
        if spread_value is not None:
            order["spread_dollars"] = spread_value
        time_to_close = _parse_float(event.get("time_to_close_seconds"))
        if time_to_close is not None:
            order["time_to_close_seconds"] = time_to_close
        aggressiveness_value = _parse_float(payload.get("quote_aggressiveness"))
        if aggressiveness_value is not None:
            order["quote_aggressiveness"] = aggressiveness_value
        forecast_edge = _parse_float(payload.get("execution_forecast_edge_net_per_contract_dollars"))
        if forecast_edge is not None:
            order["forecast_edge_net_per_contract"] = forecast_edge

        event_ts = _parse_ts(event.get("captured_at_utc"))
        if event_type == "order_submitted":
            order["submitted_at"] = event_ts
            order["submitted_contracts"] = max(order["submitted_contracts"], _parse_float(event.get("contracts_fp")) or 0.0)
        elif event_type in {"partial_fill", "full_fill"}:
            fill_contracts = _parse_float(event.get("contracts_fp")) or 0.0
            fill_price = _parse_float(event.get("limit_price_dollars"))
            fee_dollars = _parse_float(event.get("fee_dollars"))
            if fee_dollars is None:
                fee_dollars = (_parse_float(event.get("maker_fee_dollars")) or 0.0) + (_parse_float(event.get("taker_fee_dollars")) or 0.0)
            order["filled_contracts"] += max(0.0, fill_contracts)
            order["fee_dollars"] += max(0.0, fee_dollars or 0.0)
            if event_type == "full_fill":
                order["has_full_fill"] = True
            if order["first_fill_at"] is None and event_ts is not None:
                order["first_fill_at"] = event_ts
            if (
                event_ts is not None
                and isinstance(fill_price, float)
                and fill_contracts > 0
                and order["market_ticker"]
                and order["market_ticker"] in history_index
            ):
                observations = history_index[order["market_ticker"]]
                markouts: dict[int, float] = {}
                for horizon in (10, 60, 300):
                    future_mid_yes = _future_midpoint(
                        observations=observations,
                        target_ts=event_ts + timedelta(seconds=horizon),
                    )
                    if future_mid_yes is None:
                        continue
                    markouts[horizon] = _side_adjusted_markout(
                        side=order["side"] or "yes",
                        fill_price=fill_price,
                        future_mid_yes=future_mid_yes,
                    )
                if 10 in markouts:
                    order["markout_10s_weighted"] += markouts[10] * fill_contracts
                    order["markout_10s_contracts"] += fill_contracts
                    order["markout_10s_samples"] += 1
                if 60 in markouts:
                    order["markout_60s_weighted"] += markouts[60] * fill_contracts
                    order["markout_60s_contracts"] += fill_contracts
                    order["markout_60s_samples"] += 1
                if 300 in markouts:
                    order["markout_300s_weighted"] += markouts[300] * fill_contracts
                    order["markout_300s_contracts"] += fill_contracts
                    order["markout_300s_samples"] += 1
                if markouts:
                    order["markout_contracts"] += fill_contracts
                    fill_samples.append(
                        {
                            "order_key": key,
                            "market_ticker": order["market_ticker"],
                            "side": order["side"],
                            "captured_at_utc": event.get("captured_at_utc"),
                            "fill_contracts": fill_contracts,
                            "fill_price": fill_price,
                            "markout_10s": markouts.get(10),
                            "markout_60s": markouts.get(60),
                            "markout_300s": markouts.get(300),
                        }
                    )
        elif event_type == "cancel_confirmed":
            order["cancel_confirmed_count"] += 1
        elif event_type == "order_terminal":
            order["terminal_at"] = event_ts
            order["terminal_status"] = str(event.get("status") or event.get("result") or "").strip().lower()
        elif event_type == "settlement_outcome":
            pnl = _parse_float(event.get("realized_pnl_dollars"))
            if pnl is not None:
                order["realized_pnl_dollars"] = pnl

    submitted_orders = [order for order in orders.values() if order["submitted_at"] is not None]
    filled_orders = [order for order in submitted_orders if order["filled_contracts"] > 1e-9]
    full_filled_orders = [order for order in submitted_orders if bool(order["has_full_fill"])]

    bucket_map: dict[str, dict[str, Any]] = {}
    for order in submitted_orders:
        bucket = "|".join(
            (
                _aggressiveness_bucket(_parse_float(order.get("quote_aggressiveness"))),
                _spread_bucket(_parse_float(order.get("spread_dollars"))),
                _time_bucket(_parse_float(order.get("time_to_close_seconds"))),
            )
        )
        aggregate = bucket_map.setdefault(
            bucket,
            {
                "bucket": bucket,
                "orders_submitted": 0,
                "filled_orders": 0,
                "full_filled_orders": 0,
                "time_to_fill_seconds": [],
                "markout_10s": [],
                "markout_60s": [],
                "markout_300s": [],
                "markout_10s_samples": 0,
                "markout_60s_samples": 0,
                "markout_300s_samples": 0,
                "leakage_dollars": [],
                "expected_net_edge_after_costs_per_contract": [],
                "break_even_edge_per_contract": [],
            },
        )
        aggregate["orders_submitted"] += 1
        submitted_contracts = max(1.0, _parse_float(order.get("submitted_contracts")) or 1.0)
        filled_contracts = max(0.0, _parse_float(order.get("filled_contracts")) or 0.0)
        if filled_contracts > 1e-9:
            aggregate["filled_orders"] += 1
        if bool(order.get("has_full_fill")):
            aggregate["full_filled_orders"] += 1
        if order.get("submitted_at") and order.get("first_fill_at"):
            delta = (order["first_fill_at"] - order["submitted_at"]).total_seconds()
            if delta >= 0:
                aggregate["time_to_fill_seconds"].append(delta)
        markout_contracts = _parse_float(order.get("markout_contracts")) or 0.0
        markout_10_contracts = _parse_float(order.get("markout_10s_contracts")) or 0.0
        markout_60_contracts = _parse_float(order.get("markout_60s_contracts")) or 0.0
        markout_300_contracts = _parse_float(order.get("markout_300s_contracts")) or 0.0
        markout_10_samples = max(0, int(order.get("markout_10s_samples") or 0))
        markout_60_samples = max(0, int(order.get("markout_60s_samples") or 0))
        markout_300_samples = max(0, int(order.get("markout_300s_samples") or 0))
        aggregate["markout_10s_samples"] += markout_10_samples
        aggregate["markout_60s_samples"] += markout_60_samples
        aggregate["markout_300s_samples"] += markout_300_samples
        if markout_10_contracts > 0:
            markout_10 = (_parse_float(order.get("markout_10s_weighted")) or 0.0) / markout_10_contracts
            aggregate["markout_10s"].append(markout_10)
        if markout_60_contracts > 0:
            markout_60 = (_parse_float(order.get("markout_60s_weighted")) or 0.0) / markout_60_contracts
            aggregate["markout_60s"].append(markout_60)
        if markout_300_contracts > 0:
            markout_300 = (_parse_float(order.get("markout_300s_weighted")) or 0.0) / markout_300_contracts
            aggregate["markout_300s"].append(markout_300)

        spread = max(_DECIMAL_ZERO, _parse_decimal(order.get("spread_dollars")) or _DECIMAL_ZERO)
        fee_dollars = max(_DECIMAL_ZERO, _parse_decimal(order.get("fee_dollars")) or _DECIMAL_ZERO)
        cancel_count = int(order.get("cancel_confirmed_count") or 0)
        canceled_contracts = max(
            _DECIMAL_ZERO,
            Decimal(str(submitted_contracts)) - Decimal(str(filled_contracts)),
        )
        cancel_leakage = (
            canceled_contracts * spread * Decimal("0.25")
            if cancel_count > 0
            else _DECIMAL_ZERO
        )
        spread_leakage = Decimal(str(filled_contracts)) * spread * Decimal("0.5")
        markout_60_per_contract = (
            ((_parse_float(order.get("markout_60s_weighted")) or 0.0) / markout_60_contracts)
            if markout_60_contracts > 0
            else 0.0
        )
        adverse_selection_cost = max(
            _DECIMAL_ZERO,
            Decimal(str(-markout_60_per_contract * filled_contracts)),
        )
        total_leakage = fee_dollars + cancel_leakage + spread_leakage + adverse_selection_cost
        aggregate["leakage_dollars"].append(float(total_leakage))
        break_even_edge_per_contract = (
            total_leakage / Decimal(str(submitted_contracts))
            if submitted_contracts > 0
            else _DECIMAL_ZERO
        )
        aggregate["break_even_edge_per_contract"].append(float(break_even_edge_per_contract))
        forecast_edge = _parse_float(order.get("forecast_edge_net_per_contract"))
        if forecast_edge is not None:
            aggregate["expected_net_edge_after_costs_per_contract"].append(
                forecast_edge - float(break_even_edge_per_contract)
            )

    bucket_rows: list[dict[str, Any]] = []
    break_even_edge_by_bucket: dict[str, float] = {}
    trusted_break_even_edge_by_bucket: dict[str, float] = {}
    bucket_markout_sample_counts_by_horizon: dict[str, dict[str, int]] = {}
    bucket_markout_trust_by_bucket: dict[str, dict[str, Any]] = {}
    for bucket_name in sorted(bucket_map):
        aggregate = bucket_map[bucket_name]
        orders_submitted_count = int(aggregate["orders_submitted"])
        filled_orders_count = int(aggregate["filled_orders"])
        full_filled_count = int(aggregate["full_filled_orders"])
        fill_rate = filled_orders_count / orders_submitted_count if orders_submitted_count > 0 else 0.0
        full_fill_rate = full_filled_count / orders_submitted_count if orders_submitted_count > 0 else 0.0
        median_ttf = median(aggregate["time_to_fill_seconds"]) if aggregate["time_to_fill_seconds"] else None
        p90_ttf = _p90(aggregate["time_to_fill_seconds"])
        break_even_mean = _safe_mean(aggregate["break_even_edge_per_contract"])
        if isinstance(break_even_mean, float):
            break_even_edge_by_bucket[bucket_name] = round(break_even_mean, 6)
        markout_10s_samples = int(aggregate.get("markout_10s_samples") or 0)
        markout_60s_samples = int(aggregate.get("markout_60s_samples") or 0)
        markout_300s_samples = int(aggregate.get("markout_300s_samples") or 0)
        bucket_markout_sample_counts_by_horizon[bucket_name] = {
            "10s": markout_10s_samples,
            "60s": markout_60s_samples,
            "300s": markout_300s_samples,
        }
        untrusted_reasons: list[str] = []
        if markout_10s_samples < safe_min_markout_samples_10s:
            untrusted_reasons.append(
                f"markout_10s_samples_below_min:{markout_10s_samples}<{safe_min_markout_samples_10s}"
            )
        if markout_60s_samples < safe_min_markout_samples_60s:
            untrusted_reasons.append(
                f"markout_60s_samples_below_min:{markout_60s_samples}<{safe_min_markout_samples_60s}"
            )
        if markout_300s_samples < safe_min_markout_samples_300s:
            untrusted_reasons.append(
                f"markout_300s_samples_below_min:{markout_300s_samples}<{safe_min_markout_samples_300s}"
            )
        markout_horizons_trusted = not untrusted_reasons
        bucket_markout_trust_by_bucket[bucket_name] = {
            "trusted": markout_horizons_trusted,
            "reason": "ready" if markout_horizons_trusted else ";".join(untrusted_reasons),
            "markout_10s_samples": markout_10s_samples,
            "markout_60s_samples": markout_60s_samples,
            "markout_300s_samples": markout_300s_samples,
        }
        if isinstance(break_even_mean, float) and markout_horizons_trusted:
            trusted_break_even_edge_by_bucket[bucket_name] = round(break_even_mean, 6)
        bucket_rows.append(
            {
                "bucket": bucket_name,
                "orders_submitted": orders_submitted_count,
                "fill_rate": round(fill_rate, 6),
                "full_fill_rate": round(full_fill_rate, 6),
                "median_time_to_fill_seconds": round(median_ttf, 6) if isinstance(median_ttf, (int, float)) else "",
                "p90_time_to_fill_seconds": round(p90_ttf, 6) if isinstance(p90_ttf, (int, float)) else "",
                "markout_10s_side_adjusted": (
                    round(_safe_mean(aggregate["markout_10s"]), 6)
                    if _safe_mean(aggregate["markout_10s"]) is not None
                    else ""
                ),
                "markout_60s_side_adjusted": (
                    round(_safe_mean(aggregate["markout_60s"]), 6)
                    if _safe_mean(aggregate["markout_60s"]) is not None
                    else ""
                ),
                "markout_300s_side_adjusted": (
                    round(_safe_mean(aggregate["markout_300s"]), 6)
                    if _safe_mean(aggregate["markout_300s"]) is not None
                    else ""
                ),
                "markout_10s_samples": markout_10s_samples,
                "markout_60s_samples": markout_60s_samples,
                "markout_300s_samples": markout_300s_samples,
                "markout_horizons_trusted": markout_horizons_trusted,
                "markout_horizons_untrusted_reason": (
                    "ready" if markout_horizons_trusted else ";".join(untrusted_reasons)
                ),
                "fee_spread_cancel_leakage_dollars_per_order": (
                    round(_safe_mean(aggregate["leakage_dollars"]), 6)
                    if _safe_mean(aggregate["leakage_dollars"]) is not None
                    else ""
                ),
                "expected_net_edge_after_costs_per_contract": (
                    round(_safe_mean(aggregate["expected_net_edge_after_costs_per_contract"]), 6)
                    if _safe_mean(aggregate["expected_net_edge_after_costs_per_contract"]) is not None
                    else ""
                ),
                "break_even_edge_per_contract": (
                    round(break_even_mean, 6) if isinstance(break_even_mean, (int, float)) else ""
                ),
            }
        )

    status = (
        "ready"
        if submitted_orders and fill_samples and trusted_break_even_edge_by_bucket
        else "insufficient_data"
    )
    recommendations: list[str] = []
    if status == "insufficient_data":
        if not submitted_orders or not fill_samples:
            recommendations.append("Need real probe fills before gating live orders by execution frontier.")
        elif not trusted_break_even_edge_by_bucket:
            recommendations.append(
                "Execution frontier has fills but not enough per-horizon markout samples; continue probe fills before bucket gating."
            )
    else:
        overall_fill_rate = len(filled_orders) / max(1, len(submitted_orders))
        overall_full_fill_rate = len(full_filled_orders) / max(1, len(submitted_orders))
        if overall_fill_rate < 0.2:
            recommendations.append("Fill rate is low; tighten queue-depth filters or raise quote aggressiveness in validated buckets.")
        if overall_full_fill_rate < 0.1:
            recommendations.append("Full-fill rate is low; reduce size or split orders by visible depth.")
        markout_60_values = [
            value
            for row in bucket_rows
            for value in [row.get("markout_60s_side_adjusted")]
            if isinstance(value, (int, float))
        ]
        if markout_60_values and (_safe_mean([float(v) for v in markout_60_values]) or 0.0) < 0:
            recommendations.append("60s markout is negative; block toxic buckets until post-fill drift improves.")
        if not recommendations:
            recommendations.append("Execution frontier is stable enough to use as a live trade gate.")

    stamp = captured_at.strftime("%Y%m%d_%H%M%S_%f")[:-3]
    bucket_csv_path = out_dir / f"execution_frontier_report_buckets_{stamp}.csv"
    with bucket_csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FRONTIER_BUCKET_FIELDNAMES)
        writer.writeheader()
        writer.writerows(bucket_rows)

    summary = {
        "run_id": run_id,
        "captured_at": captured_at.isoformat(),
        "status": status,
        "journal_db_path": str(journal_path),
        "history_csv": str(history_path),
        "events_scanned": len(events),
        "submitted_orders": len(submitted_orders),
        "filled_orders": len(filled_orders),
        "full_filled_orders": len(full_filled_orders),
        "fill_samples_with_markout": len(fill_samples),
        "break_even_edge_by_bucket": break_even_edge_by_bucket,
        "trusted_break_even_edge_by_bucket": trusted_break_even_edge_by_bucket,
        "bucket_markout_sample_counts_by_horizon": bucket_markout_sample_counts_by_horizon,
        "bucket_markout_trust_by_bucket": bucket_markout_trust_by_bucket,
        "min_markout_samples_10s": safe_min_markout_samples_10s,
        "min_markout_samples_60s": safe_min_markout_samples_60s,
        "min_markout_samples_300s": safe_min_markout_samples_300s,
        "bucket_rows": bucket_rows,
        "bucket_csv": str(bucket_csv_path),
        "recommendations": recommendations,
    }
    summary_path = out_dir / f"execution_frontier_report_{stamp}.json"
    runtime_version = build_runtime_version_block(
        run_started_at=captured_at,
        run_id=run_id,
        git_cwd=Path.cwd(),
        frontier_artifact_path=summary_path,
        frontier_selection_mode="self_generated",
        frontier_payload=summary,
        as_of=captured_at,
    )
    summary["runtime_version"] = runtime_version
    summary["frontier_artifact_path"] = runtime_version.get("frontier_artifact_path")
    summary["frontier_artifact_sha256"] = runtime_version.get("frontier_artifact_sha256")
    summary["frontier_artifact_as_of_utc"] = runtime_version.get("frontier_artifact_as_of_utc")
    summary["frontier_artifact_age_seconds"] = runtime_version.get("frontier_artifact_age_seconds")
    summary["frontier_selection_mode"] = runtime_version.get("frontier_selection_mode")
    summary["frontier_trusted_bucket_count"] = runtime_version.get("frontier_trusted_bucket_count")
    summary["frontier_untrusted_bucket_count"] = runtime_version.get("frontier_untrusted_bucket_count")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary


def run_execution_frontier_report(
    *,
    output_dir: str = "outputs",
    journal_db_path: str | None = None,
    history_csv: str | None = None,
    recent_events: int = 20000,
    now: datetime | None = None,
) -> dict[str, Any]:
    return run_kalshi_execution_frontier(
        output_dir=output_dir,
        journal_db_path=journal_db_path,
        history_csv=history_csv,
        recent_events=recent_events,
        now=now,
    )
