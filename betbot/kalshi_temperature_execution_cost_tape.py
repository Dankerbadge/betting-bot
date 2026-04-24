from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from betbot.kalshi_execution_journal import default_execution_journal_db_path


_QUOTE_COVERAGE_EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "candidate_seen": 1.0,
    "book_snapshot": 0.8,
    "order_terminal": 0.25,
}
_QUOTE_COVERAGE_DEFAULT_EVENT_WEIGHT = 0.5
_QUOTE_COVERAGE_ORDER_TERMINAL_SHARE_THRESHOLD = 0.45
_QUOTE_COVERAGE_WEIGHTED_IMPROVEMENT_THRESHOLD = 0.08


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int:
    parsed = _parse_float(value)
    if parsed is None:
        return 0
    return int(parsed)


def _parse_ts(value: Any) -> datetime | None:
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


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    q_clamped = max(0.0, min(1.0, float(q)))
    idx = int(round(q_clamped * float(len(ordered) - 1)))
    idx = max(0, min(idx, len(ordered) - 1))
    return ordered[idx]


def _delta_float(current: Any, baseline: Any) -> float | None:
    current_value = _parse_float(current)
    baseline_value = _parse_float(baseline)
    if not isinstance(current_value, float) or not isinstance(baseline_value, float):
        return None
    return round(float(current_value - baseline_value), 6)


def _quote_coverage_event_weight(event_type: str) -> float:
    normalized = _normalize_text(event_type).lower()
    if not normalized:
        return float(_QUOTE_COVERAGE_DEFAULT_EVENT_WEIGHT)
    configured = _QUOTE_COVERAGE_EVENT_TYPE_WEIGHTS.get(normalized)
    if isinstance(configured, (int, float)):
        return max(0.0, float(configured))
    return float(_QUOTE_COVERAGE_DEFAULT_EVENT_WEIGHT)


def _latest_json_payload(
    output_dir: Path,
    patterns: tuple[str, ...],
) -> tuple[dict[str, Any] | None, str]:
    for pattern in patterns:
        candidates = sorted(
            output_dir.glob(pattern),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                return payload, str(path)
    return None, ""


def _load_previous_execution_cost_tape_baseline(latest_path: Path) -> tuple[dict[str, Any] | None, str]:
    if not latest_path.exists():
        return None, str(latest_path)
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, str(latest_path)
    if isinstance(payload, dict):
        return payload, str(latest_path)
    return None, str(latest_path)


def _journal_window_metrics(
    *,
    journal_db_path: Path,
    window_start_utc: datetime,
    max_tickers: int,
) -> dict[str, Any]:
    if not journal_db_path.exists():
        return {
            "status": "missing_journal",
            "journal_db_path": str(journal_db_path),
            "window_start": window_start_utc.isoformat(),
            "candidate_rows": 0,
            "event_rows_scanned": 0,
            "quote_two_sided_ratio": None,
            "quote_two_sided_ratio_raw": None,
            "quote_two_sided_ratio_event_weighted": None,
            "quote_two_sided_ratio_for_gating": None,
            "quote_coverage_gating_mode": "unavailable",
            "quote_coverage_event_type_weights": {},
            "quote_coverage_event_type_shares": {},
            "spread_median_dollars": None,
            "spread_p90_dollars": None,
            "visible_depth_median_contracts": None,
            "event_type_counts": {},
            "result_counts": {},
            "top_tickers": [],
            "quote_coverage_decomposition": {
                "rows_total": 0,
                "rows_with_any_two_sided_quote": 0,
                "rows_without_two_sided_quote": 0,
                "rows_with_both_yes_no_two_sided": 0,
                "rows_with_yes_two_sided_only": 0,
                "rows_with_no_two_sided_only": 0,
                "rows_with_neither_yes_no_two_sided": 0,
                "rows_with_partial_yes_quote": 0,
                "rows_with_partial_no_quote": 0,
                "rows_missing_all_quote_fields": 0,
            },
            "quote_coverage_by_event_type": [],
            "top_missing_coverage_buckets": {
                "by_market": [],
                "by_side": [],
                "by_market_side": [],
            },
        }
    query = """
        SELECT
            captured_at_utc,
            market_ticker,
            side,
            event_type,
            result,
            best_yes_bid_dollars,
            best_yes_ask_dollars,
            best_no_bid_dollars,
            best_no_ask_dollars,
            spread_dollars,
            visible_depth_contracts
        FROM execution_events
        WHERE captured_at_utc >= ?
          AND event_type IN ('candidate_seen', 'book_snapshot', 'order_terminal')
    """
    spreads: list[float] = []
    depths: list[float] = []
    total_rows = 0
    candidate_rows = 0
    two_sided_rows = 0
    quote_yes_two_sided_rows = 0
    quote_no_two_sided_rows = 0
    quote_both_two_sided_rows = 0
    quote_yes_only_two_sided_rows = 0
    quote_no_only_two_sided_rows = 0
    quote_neither_two_sided_rows = 0
    quote_partial_yes_rows = 0
    quote_partial_no_rows = 0
    quote_missing_all_fields_rows = 0
    event_type_counts: Counter[str] = Counter()
    result_counts: Counter[str] = Counter()
    event_type_two_sided_counts: Counter[str] = Counter()
    ticker_counts: Counter[str] = Counter()
    ticker_candidate_counts: Counter[str] = Counter()
    ticker_two_sided_counts: Counter[str] = Counter()
    ticker_expected_edge_counts: Counter[str] = Counter()
    uncovered_by_market: Counter[str] = Counter()
    uncovered_by_side: Counter[str] = Counter()
    uncovered_by_market_side: Counter[str] = Counter()
    ticker_spread_sum: dict[str, float] = {}
    ticker_spread_count: dict[str, int] = {}
    ticker_last_captured_at: dict[str, datetime] = {}
    with sqlite3.connect(journal_db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, (window_start_utc.isoformat(),))
        for row in cursor:
            total_rows += 1
            ticker = _normalize_text(row["market_ticker"])
            side = _normalize_text(row["side"]).lower() or "unknown"
            event_type = _normalize_text(row["event_type"]).lower()
            result = _normalize_text(row["result"]).lower()
            if event_type:
                event_type_counts[event_type] += 1
            if result:
                result_counts[result] += 1
            if ticker:
                ticker_counts[ticker] += 1
            if event_type == "candidate_seen":
                candidate_rows += 1
                if ticker:
                    ticker_candidate_counts[ticker] += 1
            yes_bid = _parse_float(row["best_yes_bid_dollars"])
            yes_ask = _parse_float(row["best_yes_ask_dollars"])
            no_bid = _parse_float(row["best_no_bid_dollars"])
            no_ask = _parse_float(row["best_no_ask_dollars"])
            has_yes_bid = isinstance(yes_bid, float)
            has_yes_ask = isinstance(yes_ask, float)
            has_no_bid = isinstance(no_bid, float)
            has_no_ask = isinstance(no_ask, float)
            has_yes_two_sided = isinstance(yes_bid, float) and isinstance(yes_ask, float)
            has_no_two_sided = isinstance(no_bid, float) and isinstance(no_ask, float)
            if has_yes_two_sided:
                quote_yes_two_sided_rows += 1
            if has_no_two_sided:
                quote_no_two_sided_rows += 1
            if has_yes_two_sided and has_no_two_sided:
                quote_both_two_sided_rows += 1
            elif has_yes_two_sided:
                quote_yes_only_two_sided_rows += 1
            elif has_no_two_sided:
                quote_no_only_two_sided_rows += 1
            else:
                quote_neither_two_sided_rows += 1
            if (has_yes_bid or has_yes_ask) and not has_yes_two_sided:
                quote_partial_yes_rows += 1
            if (has_no_bid or has_no_ask) and not has_no_two_sided:
                quote_partial_no_rows += 1
            if not (has_yes_bid or has_yes_ask or has_no_bid or has_no_ask):
                quote_missing_all_fields_rows += 1
            if has_yes_two_sided or has_no_two_sided:
                two_sided_rows += 1
                if event_type:
                    event_type_two_sided_counts[event_type] += 1
                if ticker:
                    ticker_two_sided_counts[ticker] += 1
            else:
                uncovered_ticker = ticker or "unknown_market"
                uncovered_by_market[uncovered_ticker] += 1
                uncovered_by_side[side] += 1
                uncovered_by_market_side[f"{uncovered_ticker}|{side}"] += 1
            if ticker and "expected_edge_below_min" in result:
                ticker_expected_edge_counts[ticker] += 1
            captured_at = _parse_ts(row["captured_at_utc"])
            if ticker and isinstance(captured_at, datetime):
                prev = ticker_last_captured_at.get(ticker)
                if prev is None or captured_at > prev:
                    ticker_last_captured_at[ticker] = captured_at
            spread = _parse_float(row["spread_dollars"])
            if isinstance(spread, float):
                spreads.append(spread)
                if ticker:
                    ticker_spread_sum[ticker] = float(ticker_spread_sum.get(ticker, 0.0) + spread)
                    ticker_spread_count[ticker] = int(ticker_spread_count.get(ticker, 0) + 1)
            depth = _parse_float(row["visible_depth_contracts"])
            if isinstance(depth, float):
                depths.append(depth)
    if total_rows <= 0:
        return {
            "status": "empty_journal",
            "journal_db_path": str(journal_db_path),
            "window_start": window_start_utc.isoformat(),
            "event_rows_scanned": 0,
            "candidate_rows": 0,
            "quote_two_sided_ratio": None,
            "quote_two_sided_ratio_raw": None,
            "quote_two_sided_ratio_event_weighted": None,
            "quote_two_sided_ratio_for_gating": None,
            "quote_coverage_gating_mode": "unavailable",
            "quote_coverage_event_type_weights": {},
            "quote_coverage_event_type_shares": {},
            "spread_median_dollars": None,
            "spread_p90_dollars": None,
            "visible_depth_median_contracts": None,
            "event_type_counts": {},
            "result_counts": {},
            "ticker_diagnostics_count": 0,
            "top_tickers": [],
            "quote_coverage_decomposition": {
                "rows_total": 0,
                "rows_with_any_two_sided_quote": 0,
                "rows_without_two_sided_quote": 0,
                "rows_with_both_yes_no_two_sided": 0,
                "rows_with_yes_two_sided_only": 0,
                "rows_with_no_two_sided_only": 0,
                "rows_with_neither_yes_no_two_sided": 0,
                "rows_with_partial_yes_quote": 0,
                "rows_with_partial_no_quote": 0,
                "rows_missing_all_quote_fields": 0,
            },
            "quote_coverage_by_event_type": [],
            "top_missing_coverage_buckets": {
                "by_market": [],
                "by_side": [],
                "by_market_side": [],
            },
        }
    top_ticker_rows: list[dict[str, Any]] = []
    for ticker, count in ticker_counts.most_common(max(1, int(max_tickers))):
        spread_count = int(ticker_spread_count.get(ticker, 0))
        mean_spread = None
        if spread_count > 0:
            mean_spread = float(ticker_spread_sum.get(ticker, 0.0)) / float(spread_count)
        two_sided_count = int(ticker_two_sided_counts.get(ticker, 0))
        quote_coverage_ratio = _safe_ratio(float(two_sided_count), float(count))
        expected_edge_count = int(ticker_expected_edge_counts.get(ticker, 0))
        expected_edge_ratio = _safe_ratio(float(expected_edge_count), float(count))
        candidate_count = int(ticker_candidate_counts.get(ticker, 0))
        last_captured_at = ticker_last_captured_at.get(ticker)
        top_ticker_rows.append(
            {
                "ticker": ticker,
                "rows": int(count),
                "candidate_rows": int(candidate_count),
                "quote_two_sided_rows": int(two_sided_count),
                "quote_coverage_ratio": (
                    round(float(quote_coverage_ratio), 6)
                    if isinstance(quote_coverage_ratio, float)
                    else None
                ),
                "expected_edge_rows": int(expected_edge_count),
                "expected_edge_ratio": (
                    round(float(expected_edge_ratio), 6)
                    if isinstance(expected_edge_ratio, float)
                    else None
                ),
                "spread_samples": int(spread_count),
                "mean_spread_dollars": round(float(mean_spread), 6) if isinstance(mean_spread, float) else None,
                "last_captured_at": (
                    last_captured_at.astimezone(timezone.utc).isoformat()
                    if isinstance(last_captured_at, datetime)
                    else ""
                ),
            }
        )
    quote_two_sided_ratio = _safe_ratio(float(two_sided_rows), float(total_rows))
    uncovered_rows = max(0, int(total_rows - two_sided_rows))
    quote_coverage_event_type_shares: dict[str, float | None] = {}
    weighted_two_sided_rows = 0.0
    weighted_total_rows = 0.0
    quote_coverage_event_type_weights: dict[str, float] = {}
    for event_type, row_count in sorted(event_type_counts.items(), key=lambda item: item[0]):
        share = _safe_ratio(float(row_count), float(total_rows))
        quote_coverage_event_type_shares[event_type] = (
            round(float(share), 6) if isinstance(share, float) else None
        )
        covered_count = int(event_type_two_sided_counts.get(event_type, 0))
        event_weight = _quote_coverage_event_weight(event_type)
        quote_coverage_event_type_weights[event_type] = round(float(event_weight), 6)
        weighted_two_sided_rows += float(event_weight) * float(covered_count)
        weighted_total_rows += float(event_weight) * float(row_count)
    quote_two_sided_ratio_event_weighted = _safe_ratio(
        float(weighted_two_sided_rows),
        float(weighted_total_rows),
    )
    quote_coverage_gating_mode = "raw"
    quote_two_sided_ratio_for_gating = quote_two_sided_ratio
    order_terminal_share = _parse_float(quote_coverage_event_type_shares.get("order_terminal"))
    if (
        isinstance(order_terminal_share, float)
        and order_terminal_share >= float(_QUOTE_COVERAGE_ORDER_TERMINAL_SHARE_THRESHOLD)
        and isinstance(quote_two_sided_ratio, float)
        and isinstance(quote_two_sided_ratio_event_weighted, float)
        and float(quote_two_sided_ratio_event_weighted)
        >= float(quote_two_sided_ratio) + float(_QUOTE_COVERAGE_WEIGHTED_IMPROVEMENT_THRESHOLD)
    ):
        quote_two_sided_ratio_for_gating = float(quote_two_sided_ratio_event_weighted)
        quote_coverage_gating_mode = "event_weighted_order_terminal_debiased"
    elif not isinstance(quote_two_sided_ratio_for_gating, float) and isinstance(quote_two_sided_ratio_event_weighted, float):
        quote_two_sided_ratio_for_gating = float(quote_two_sided_ratio_event_weighted)
        quote_coverage_gating_mode = "event_weighted_fallback"
    elif not isinstance(quote_two_sided_ratio_for_gating, float):
        quote_coverage_gating_mode = "unavailable"

    quote_coverage_by_event_type: list[dict[str, Any]] = []
    for event_type, row_count in sorted(event_type_counts.items(), key=lambda item: (-item[1], item[0])):
        covered_count = int(event_type_two_sided_counts.get(event_type, 0))
        uncovered_count = max(0, int(row_count - covered_count))
        coverage_ratio = _safe_ratio(float(covered_count), float(row_count))
        event_weight = _quote_coverage_event_weight(event_type)
        quote_coverage_by_event_type.append(
            {
                "event_type": event_type,
                "rows": int(row_count),
                "rows_with_any_two_sided_quote": int(covered_count),
                "rows_without_two_sided_quote": int(uncovered_count),
                "event_weight": round(float(event_weight), 6),
                "quote_coverage_ratio": round(float(coverage_ratio), 6) if isinstance(coverage_ratio, float) else None,
            }
        )

    top_missing_coverage_by_market: list[dict[str, Any]] = []
    for bucket, count in uncovered_by_market.most_common(max(1, int(max_tickers))):
        share = _safe_ratio(float(count), float(uncovered_rows))
        top_missing_coverage_by_market.append(
            {
                "bucket": bucket,
                "rows_without_two_sided_quote": int(count),
                "share_of_uncovered_rows": round(float(share), 6) if isinstance(share, float) else None,
            }
        )

    top_missing_coverage_by_side: list[dict[str, Any]] = []
    for bucket, count in uncovered_by_side.most_common(8):
        share = _safe_ratio(float(count), float(uncovered_rows))
        top_missing_coverage_by_side.append(
            {
                "bucket": bucket,
                "rows_without_two_sided_quote": int(count),
                "share_of_uncovered_rows": round(float(share), 6) if isinstance(share, float) else None,
            }
        )

    top_missing_coverage_by_market_side: list[dict[str, Any]] = []
    for bucket, count in uncovered_by_market_side.most_common(max(1, int(max_tickers))):
        share = _safe_ratio(float(count), float(uncovered_rows))
        top_missing_coverage_by_market_side.append(
            {
                "bucket": bucket,
                "rows_without_two_sided_quote": int(count),
                "share_of_uncovered_rows": round(float(share), 6) if isinstance(share, float) else None,
            }
        )

    return {
        "status": "ready",
        "journal_db_path": str(journal_db_path),
        "window_start": window_start_utc.isoformat(),
        "event_rows_scanned": int(total_rows),
        "candidate_rows": int(candidate_rows),
        "quote_two_sided_ratio": round(float(quote_two_sided_ratio), 6) if isinstance(quote_two_sided_ratio, float) else None,
        "quote_two_sided_ratio_raw": (
            round(float(quote_two_sided_ratio), 6) if isinstance(quote_two_sided_ratio, float) else None
        ),
        "quote_two_sided_ratio_event_weighted": (
            round(float(quote_two_sided_ratio_event_weighted), 6)
            if isinstance(quote_two_sided_ratio_event_weighted, float)
            else None
        ),
        "quote_two_sided_ratio_for_gating": (
            round(float(quote_two_sided_ratio_for_gating), 6)
            if isinstance(quote_two_sided_ratio_for_gating, float)
            else None
        ),
        "quote_coverage_gating_mode": quote_coverage_gating_mode,
        "quote_coverage_event_type_weights": quote_coverage_event_type_weights,
        "quote_coverage_event_type_shares": quote_coverage_event_type_shares,
        "spread_median_dollars": (
            round(float(_quantile(spreads, 0.50)), 6) if isinstance(_quantile(spreads, 0.50), float) else None
        ),
        "spread_p90_dollars": (
            round(float(_quantile(spreads, 0.90)), 6) if isinstance(_quantile(spreads, 0.90), float) else None
        ),
        "visible_depth_median_contracts": (
            round(float(_quantile(depths, 0.50)), 3) if isinstance(_quantile(depths, 0.50), float) else None
        ),
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "result_counts": dict(sorted(result_counts.items())),
        "ticker_diagnostics_count": int(len(ticker_counts)),
        "top_tickers": top_ticker_rows,
        "quote_coverage_decomposition": {
            "rows_total": int(total_rows),
            "rows_with_any_two_sided_quote": int(two_sided_rows),
            "rows_without_two_sided_quote": int(uncovered_rows),
            "rows_with_both_yes_no_two_sided": int(quote_both_two_sided_rows),
            "rows_with_yes_two_sided_only": int(quote_yes_only_two_sided_rows),
            "rows_with_no_two_sided_only": int(quote_no_only_two_sided_rows),
            "rows_with_neither_yes_no_two_sided": int(quote_neither_two_sided_rows),
            "rows_with_partial_yes_quote": int(quote_partial_yes_rows),
            "rows_with_partial_no_quote": int(quote_partial_no_rows),
            "rows_missing_all_quote_fields": int(quote_missing_all_fields_rows),
            "rows_with_yes_two_sided_quote": int(quote_yes_two_sided_rows),
            "rows_with_no_two_sided_quote": int(quote_no_two_sided_rows),
        },
        "quote_coverage_by_event_type": quote_coverage_by_event_type,
        "top_missing_coverage_buckets": {
            "by_market": top_missing_coverage_by_market,
            "by_side": top_missing_coverage_by_side,
            "by_market_side": top_missing_coverage_by_market_side,
        },
    }


def _ws_state_metrics(ws_payload: dict[str, Any]) -> dict[str, Any]:
    summary = ws_payload.get("summary")
    summary = dict(summary) if isinstance(summary, dict) else {}
    markets_payload = ws_payload.get("markets")
    market_rows: list[tuple[str, dict[str, Any]]] = []
    if isinstance(markets_payload, dict):
        for ticker, row in markets_payload.items():
            if isinstance(row, dict):
                market_rows.append((_normalize_text(ticker), row))
    elif isinstance(markets_payload, list):
        for row in markets_payload:
            if not isinstance(row, dict):
                continue
            ticker = _normalize_text(row.get("market_ticker") or row.get("ticker"))
            market_rows.append((ticker, row))

    market_count = 0
    two_sided_markets = 0
    spread_values: list[float] = []
    sample_rows: list[dict[str, Any]] = []
    for ticker, row in market_rows:
        top = row.get("top_of_book")
        top = dict(top) if isinstance(top, dict) else {}
        if not top:
            continue
        market_count += 1
        yes_bid = _parse_float(top.get("best_yes_bid_dollars"))
        yes_ask = _parse_float(top.get("best_yes_ask_dollars"))
        no_bid = _parse_float(top.get("best_no_bid_dollars"))
        no_ask = _parse_float(top.get("best_no_ask_dollars"))
        has_yes_two_sided = isinstance(yes_bid, float) and isinstance(yes_ask, float)
        has_no_two_sided = isinstance(no_bid, float) and isinstance(no_ask, float)
        if has_yes_two_sided or has_no_two_sided:
            two_sided_markets += 1
        spread = _parse_float(top.get("yes_spread_dollars"))
        if isinstance(spread, float):
            spread_values.append(spread)
        if len(sample_rows) < 12:
            sample_rows.append(
                {
                    "ticker": ticker,
                    "best_yes_bid_dollars": yes_bid,
                    "best_yes_ask_dollars": yes_ask,
                    "best_no_bid_dollars": no_bid,
                    "best_no_ask_dollars": no_ask,
                    "yes_spread_dollars": spread,
                }
            )
    coverage_ratio = _safe_ratio(float(two_sided_markets), float(market_count))
    return {
        "status": _normalize_text(summary.get("status")) or "unknown",
        "captured_at": _normalize_text(summary.get("captured_at")),
        "market_count": int(_parse_int(summary.get("market_count")) or market_count),
        "desynced_market_count": int(_parse_int(summary.get("desynced_market_count"))),
        "events_processed": int(_parse_int(summary.get("events_processed"))),
        "last_event_age_seconds": _parse_float(summary.get("last_event_age_seconds")),
        "two_sided_markets": int(two_sided_markets),
        "quote_two_sided_ratio": round(float(coverage_ratio), 6) if isinstance(coverage_ratio, float) else None,
        "yes_spread_median_dollars": (
            round(float(_quantile(spread_values, 0.50)), 6)
            if isinstance(_quantile(spread_values, 0.50), float)
            else None
        ),
        "sample_markets": sample_rows,
    }


def _build_recommended_exclusions(
    *,
    top_reason: str,
    top_share: float,
    ticker_rows: list[dict[str, Any]],
    missing_market_side_rows: list[dict[str, Any]],
    execution_siphon_pressure: dict[str, Any],
    execution_siphon_trend: dict[str, Any] | None,
    min_global_expected_edge_share_for_exclusion: float,
    min_ticker_rows_for_exclusion: int,
    exclusion_max_quote_coverage_ratio: float,
    max_ticker_mean_spread_for_exclusion: float,
    max_excluded_tickers: int,
) -> dict[str, Any]:
    safe_global_share = max(0.0, min(1.0, float(min_global_expected_edge_share_for_exclusion)))
    safe_min_rows = max(1, int(min_ticker_rows_for_exclusion))
    safe_max_quote_coverage = max(0.0, min(1.0, float(exclusion_max_quote_coverage_ratio)))
    safe_max_spread = max(0.001, float(max_ticker_mean_spread_for_exclusion))
    safe_limit = max(1, int(max_excluded_tickers))
    safe_market_side_target_limit = max(1, int(max_excluded_tickers))
    safe_market_side_diagnostics_limit = max(8, min(64, int(max(1, safe_market_side_target_limit)) * 3))
    safe_side_dominant_share_threshold = 0.60
    safe_side_imbalance_threshold = 0.20
    safe_severe_quote_coverage_shortfall_threshold = 0.20
    safe_severe_pressure_score_threshold = 0.55
    safe_worsening_pressure_delta_threshold = 0.05
    safe_worsening_shortfall_delta_threshold = 0.05
    safe_severe_low_coverage_wide_spread_ticker_count = 3
    safe_market_side_max_quote_coverage = max(0.0, min(1.0, float(safe_max_quote_coverage)))
    safe_market_side_min_mean_spread = max(0.02, min(float(safe_max_spread), 0.08))
    normalized_top_reason = _normalize_text(top_reason).lower()
    normalized_siphon_trend = (
        dict(execution_siphon_trend) if isinstance(execution_siphon_trend, dict) else {}
    )
    expected_edge_dominance_active = (
        normalized_top_reason == "expected_edge_below_min" and float(top_share) >= safe_global_share
    )
    dominant_uncovered_side = _normalize_text(execution_siphon_pressure.get("dominant_uncovered_side")).lower()
    dominant_uncovered_side_share = _parse_float(execution_siphon_pressure.get("dominant_uncovered_side_share"))
    side_imbalance_magnitude = _parse_float(execution_siphon_pressure.get("side_imbalance_magnitude"))
    quote_coverage_shortfall = _parse_float(execution_siphon_pressure.get("quote_coverage_shortfall"))
    pressure_score = _parse_float(execution_siphon_pressure.get("pressure_score"))
    low_coverage_wide_spread_ticker_count = int(
        _parse_int(execution_siphon_pressure.get("low_coverage_wide_spread_ticker_count"))
    )
    trend_status = _normalize_text(normalized_siphon_trend.get("status")).lower()
    trend_worsening = bool(normalized_siphon_trend.get("worsening"))
    trend_label = _normalize_text(normalized_siphon_trend.get("trend_label")).lower()
    trend_pressure_delta = _parse_float(normalized_siphon_trend.get("siphon_pressure_score_delta"))
    trend_shortfall_delta = _parse_float(normalized_siphon_trend.get("quote_coverage_shortfall_delta"))
    material_side_pressure = bool(
        dominant_uncovered_side in {"yes", "no"}
        and (
            (
                isinstance(dominant_uncovered_side_share, float)
                and float(dominant_uncovered_side_share) >= float(safe_side_dominant_share_threshold)
            )
            or (
                isinstance(side_imbalance_magnitude, float)
                and float(side_imbalance_magnitude) >= float(safe_side_imbalance_threshold)
            )
        )
    )
    severe_quote_coverage_shortfall = bool(
        isinstance(quote_coverage_shortfall, float)
        and float(quote_coverage_shortfall) >= float(safe_severe_quote_coverage_shortfall_threshold)
    )
    severe_pressure_score = bool(
        isinstance(pressure_score, float)
        and float(pressure_score) >= float(safe_severe_pressure_score_threshold)
    )
    severe_low_coverage_wide_spread_pressure = bool(
        int(low_coverage_wide_spread_ticker_count) >= int(safe_severe_low_coverage_wide_spread_ticker_count)
    )
    worsening_siphon_trend = bool(
        trend_status == "ready"
        and (
            trend_worsening
            or trend_label == "worsening"
            or (
                isinstance(trend_pressure_delta, float)
                and float(trend_pressure_delta) >= float(safe_worsening_pressure_delta_threshold)
            )
            or (
                isinstance(trend_shortfall_delta, float)
                and float(trend_shortfall_delta) >= float(safe_worsening_shortfall_delta_threshold)
            )
        )
    )
    execution_pressure_override_active = bool(
        material_side_pressure
        and (
            severe_quote_coverage_shortfall
            or severe_pressure_score
            or severe_low_coverage_wide_spread_pressure
            or worsening_siphon_trend
        )
    )
    market_side_target_selection_active = bool(
        (expected_edge_dominance_active and material_side_pressure) or execution_pressure_override_active
    )
    market_side_target_selection_activation_mode = "inactive"
    market_side_target_selection_activation_reasons: list[str] = []
    if expected_edge_dominance_active and material_side_pressure:
        market_side_target_selection_activation_mode = "expected_edge_dominance"
        market_side_target_selection_activation_reasons = [
            "expected_edge_dominance_material_side_pressure"
        ]
    elif execution_pressure_override_active:
        market_side_target_selection_activation_mode = "execution_pressure_override"
        if material_side_pressure:
            market_side_target_selection_activation_reasons.append("material_side_pressure")
        if severe_quote_coverage_shortfall:
            market_side_target_selection_activation_reasons.append("severe_quote_coverage_shortfall")
        if severe_pressure_score:
            market_side_target_selection_activation_reasons.append("severe_pressure_score")
        if severe_low_coverage_wide_spread_pressure:
            market_side_target_selection_activation_reasons.append("severe_low_coverage_wide_spread_pressure")
        if worsening_siphon_trend:
            market_side_target_selection_activation_reasons.append("worsening_siphon_trend")
    else:
        if not expected_edge_dominance_active:
            market_side_target_selection_activation_reasons.append("expected_edge_dominance_inactive")
        if not material_side_pressure:
            market_side_target_selection_activation_reasons.append("side_pressure_not_material")
        if (
            material_side_pressure
            and not (
                severe_quote_coverage_shortfall
                or severe_pressure_score
                or severe_low_coverage_wide_spread_pressure
                or worsening_siphon_trend
            )
        ):
            market_side_target_selection_activation_reasons.append("execution_pressure_context_not_severe")

    candidates: list[dict[str, Any]] = []
    ticker_quality_index: dict[str, dict[str, Any]] = {}
    for row in list(ticker_rows or []):
        if not isinstance(row, dict):
            continue
        ticker = _normalize_text(row.get("ticker")).upper()
        if not ticker:
            continue
        rows = int(_parse_int(row.get("rows")))
        coverage_ratio = _parse_float(row.get("quote_coverage_ratio"))
        mean_spread = _parse_float(row.get("mean_spread_dollars"))
        expected_edge_ratio = _parse_float(row.get("expected_edge_ratio"))
        ticker_quality_index[ticker] = {
            "rows": rows,
            "candidate_rows": int(_parse_int(row.get("candidate_rows"))),
            "quote_coverage_ratio": (
                round(float(coverage_ratio), 6) if isinstance(coverage_ratio, float) else None
            ),
            "mean_spread_dollars": round(float(mean_spread), 6) if isinstance(mean_spread, float) else None,
            "expected_edge_ratio": round(float(expected_edge_ratio), 6) if isinstance(expected_edge_ratio, float) else None,
        }
        if rows < safe_min_rows:
            continue
        reasons: list[str] = []
        if isinstance(coverage_ratio, float) and coverage_ratio <= safe_max_quote_coverage:
            reasons.append("very_low_quote_coverage")
        if isinstance(mean_spread, float) and mean_spread >= safe_max_spread:
            reasons.append("mean_spread_wide")
        if not reasons:
            continue
        if not expected_edge_dominance_active:
            continue
        candidates.append(
            {
                "ticker": ticker,
                "rows": rows,
                "candidate_rows": int(_parse_int(row.get("candidate_rows"))),
                "quote_coverage_ratio": round(float(coverage_ratio), 6) if isinstance(coverage_ratio, float) else None,
                "mean_spread_dollars": round(float(mean_spread), 6) if isinstance(mean_spread, float) else None,
                "expected_edge_ratio": (
                    round(float(expected_edge_ratio), 6)
                    if isinstance(expected_edge_ratio, float)
                    else None
                ),
                "reasons": reasons,
            }
        )
    candidates.sort(
        key=lambda item: (
            float(item.get("quote_coverage_ratio") if item.get("quote_coverage_ratio") is not None else 1.0),
            -int(item.get("rows") or 0),
            -int(item.get("candidate_rows") or 0),
        )
    )
    selected = candidates[:safe_limit]

    market_side_candidates: list[dict[str, Any]] = []
    market_side_diagnostics: list[dict[str, Any]] = []
    seen_market_side_targets: set[str] = set()
    for row in list(missing_market_side_rows or []):
        row_dict = dict(row) if isinstance(row, dict) else {}
        bucket = _normalize_text(row_dict.get("bucket"))
        if "|" not in bucket:
            continue
        ticker_text, side_text = bucket.split("|", 1)
        ticker = _normalize_text(ticker_text).upper()
        side = _normalize_text(side_text).lower()
        if not ticker or side not in {"yes", "no"}:
            continue
        market_side_target = f"{ticker}|{side}"
        if market_side_target in seen_market_side_targets:
            continue
        seen_market_side_targets.add(market_side_target)

        rows_without_two_sided_quote = max(0, int(_parse_int(row_dict.get("rows_without_two_sided_quote"))))
        share_of_uncovered_rows = _parse_float(row_dict.get("share_of_uncovered_rows"))
        ticker_quality = dict(ticker_quality_index.get(ticker) or {})
        ticker_quote_coverage_ratio = _parse_float(ticker_quality.get("quote_coverage_ratio"))
        ticker_mean_spread = _parse_float(ticker_quality.get("mean_spread_dollars"))
        low_quote_coverage = (
            isinstance(ticker_quote_coverage_ratio, float)
            and float(ticker_quote_coverage_ratio) <= float(safe_market_side_max_quote_coverage)
        )
        wide_spread = (
            isinstance(ticker_mean_spread, float)
            and float(ticker_mean_spread) >= float(safe_market_side_min_mean_spread)
        )
        ticker_quality_available = isinstance(ticker_quote_coverage_ratio, float) or isinstance(ticker_mean_spread, float)
        ticker_quality_evidence_ok = (low_quote_coverage or wide_spread) if ticker_quality_available else True
        eligible_for_side_target = bool(
            market_side_target_selection_active
            and side == dominant_uncovered_side
            and rows_without_two_sided_quote > 0
            and ticker_quality_evidence_ok
        )

        gate_reasons: list[str] = []
        if not market_side_target_selection_active:
            gate_reasons.append("market_side_target_selection_inactive")
            if not expected_edge_dominance_active:
                gate_reasons.append("expected_edge_dominance_inactive")
            if not execution_pressure_override_active:
                gate_reasons.append("execution_pressure_override_inactive")
        if side != dominant_uncovered_side:
            gate_reasons.append("non_dominant_side")
        if rows_without_two_sided_quote <= 0:
            gate_reasons.append("empty_market_side_bucket")
        if ticker_quality_available and not ticker_quality_evidence_ok:
            gate_reasons.append("ticker_quality_evidence_missing")

        diagnostic_row = {
            "market_side_target": market_side_target,
            "ticker": ticker,
            "side": side,
            "rows_without_two_sided_quote": int(rows_without_two_sided_quote),
            "share_of_uncovered_rows": (
                round(float(share_of_uncovered_rows), 6)
                if isinstance(share_of_uncovered_rows, float)
                else None
            ),
            "ticker_quote_coverage_ratio": (
                round(float(ticker_quote_coverage_ratio), 6)
                if isinstance(ticker_quote_coverage_ratio, float)
                else None
            ),
            "ticker_mean_spread_dollars": (
                round(float(ticker_mean_spread), 6)
                if isinstance(ticker_mean_spread, float)
                else None
            ),
            "low_quote_coverage_evidence": bool(low_quote_coverage),
            "wide_spread_evidence": bool(wide_spread),
            "ticker_quality_available": bool(ticker_quality_available),
            "eligible_for_side_target": bool(eligible_for_side_target),
            "selection_activation_mode": market_side_target_selection_activation_mode,
            "selection_activation_reasons": market_side_target_selection_activation_reasons[:6],
            "gate_reasons": gate_reasons[:6],
        }
        market_side_diagnostics.append(diagnostic_row)
        if eligible_for_side_target:
            market_side_candidates.append(diagnostic_row)

    market_side_candidates.sort(
        key=lambda item: (
            -float(_parse_float(item.get("share_of_uncovered_rows")) or 0.0),
            -int(_parse_int(item.get("rows_without_two_sided_quote"))),
            float(_parse_float(item.get("ticker_quote_coverage_ratio")) or 1.0),
            -float(_parse_float(item.get("ticker_mean_spread_dollars")) or 0.0),
            str(item.get("market_side_target") or ""),
        )
    )
    market_side_diagnostics.sort(
        key=lambda item: (
            0 if bool(item.get("eligible_for_side_target")) else 1,
            -float(_parse_float(item.get("share_of_uncovered_rows")) or 0.0),
            -int(_parse_int(item.get("rows_without_two_sided_quote"))),
            str(item.get("market_side_target") or ""),
        )
    )
    selected_market_side = market_side_candidates[:safe_market_side_target_limit]
    market_side_targets = [
        str(item.get("market_side_target"))
        for item in selected_market_side
        if str(item.get("market_side_target") or "").strip()
    ]
    market_side_diagnostics_sample = market_side_diagnostics[:safe_market_side_diagnostics_limit]

    return {
        "status": (
            "active"
            if (expected_edge_dominance_active or execution_pressure_override_active)
            else "inactive"
        ),
        "expected_edge_dominance_active": bool(expected_edge_dominance_active),
        "largest_blocker_reason": normalized_top_reason,
        "largest_blocker_share_of_blocked": round(float(top_share), 6),
        "thresholds": {
            "min_global_expected_edge_share_for_exclusion": round(float(safe_global_share), 6),
            "min_ticker_rows_for_exclusion": int(safe_min_rows),
            "exclusion_max_quote_coverage_ratio": round(float(safe_max_quote_coverage), 6),
            "max_ticker_mean_spread_for_exclusion": round(float(safe_max_spread), 6),
            "max_excluded_tickers": int(safe_limit),
            "market_side_target_selection": {
                "require_expected_edge_dominance": True,
                "allow_execution_pressure_override": True,
                "require_dominant_side": True,
                "min_dominant_uncovered_side_share": round(float(safe_side_dominant_share_threshold), 6),
                "min_side_imbalance_magnitude": round(float(safe_side_imbalance_threshold), 6),
                "severe_quote_coverage_shortfall_threshold": round(
                    float(safe_severe_quote_coverage_shortfall_threshold), 6
                ),
                "severe_pressure_score_threshold": round(float(safe_severe_pressure_score_threshold), 6),
                "severe_low_coverage_wide_spread_ticker_count": int(
                    safe_severe_low_coverage_wide_spread_ticker_count
                ),
                "worsening_pressure_score_delta_threshold": round(
                    float(safe_worsening_pressure_delta_threshold), 6
                ),
                "worsening_quote_coverage_shortfall_delta_threshold": round(
                    float(safe_worsening_shortfall_delta_threshold), 6
                ),
                "max_ticker_quote_coverage_ratio": round(float(safe_market_side_max_quote_coverage), 6),
                "min_ticker_mean_spread_dollars": round(float(safe_market_side_min_mean_spread), 6),
                "max_excluded_market_side_targets": int(safe_market_side_target_limit),
                "max_market_side_diagnostics": int(safe_market_side_diagnostics_limit),
            },
        },
        "excluded_ticker_count": int(len(selected)),
        "market_tickers": [str(item.get("ticker")) for item in selected if str(item.get("ticker") or "").strip()],
        "diagnostics": selected,
        "market_side_target_selection_active": bool(market_side_target_selection_active),
        "market_side_target_selection_activation_mode": market_side_target_selection_activation_mode,
        "market_side_target_selection_activation_reasons": market_side_target_selection_activation_reasons[:8],
        "market_side_target_selection_expected_edge_route_active": bool(
            expected_edge_dominance_active and material_side_pressure
        ),
        "market_side_target_selection_execution_pressure_route_active": bool(execution_pressure_override_active),
        "market_side_target_selection_execution_pressure_signals": {
            "material_side_pressure": bool(material_side_pressure),
            "severe_quote_coverage_shortfall": bool(severe_quote_coverage_shortfall),
            "severe_pressure_score": bool(severe_pressure_score),
            "severe_low_coverage_wide_spread_pressure": bool(severe_low_coverage_wide_spread_pressure),
            "worsening_siphon_trend": bool(worsening_siphon_trend),
        },
        "market_side_targets": market_side_targets,
        "excluded_market_side_target_count": int(len(market_side_targets)),
        "market_side_diagnostics_count": int(len(market_side_diagnostics_sample)),
        "market_side_diagnostics_total_count": int(len(market_side_diagnostics)),
        "market_side_diagnostics": market_side_diagnostics_sample,
    }


def _normalize_shadow_quote_probe_target(raw_value: Any) -> str:
    text = _normalize_text(raw_value)
    if not text:
        return ""
    ticker_text = text
    side_text = ""
    if "|" in text:
        ticker_text, side_text = text.split("|", 1)
    ticker = _normalize_text(ticker_text).upper()
    if not ticker:
        return ""
    side = _normalize_text(side_text).lower()
    if side in {"yes", "no"}:
        return f"{ticker}|{side}"
    return ticker


def _build_recommended_shadow_quote_probe_targets(
    *,
    journal_metrics: dict[str, Any],
    max_target_keys: int,
) -> dict[str, Any]:
    safe_limit = max(1, int(max_target_keys))
    missing_buckets = journal_metrics.get("top_missing_coverage_buckets")
    missing_buckets = dict(missing_buckets) if isinstance(missing_buckets, dict) else {}

    by_market_side = missing_buckets.get("by_market_side")
    by_market_side = list(by_market_side) if isinstance(by_market_side, list) else []
    by_market = missing_buckets.get("by_market")
    by_market = list(by_market) if isinstance(by_market, list) else []

    targets: list[str] = []
    seen: set[str] = set()
    source_labels: list[str] = []
    side_target_counts = {"yes": 0, "no": 0, "unknown": 0}

    for row in by_market_side:
        row_dict = dict(row) if isinstance(row, dict) else {}
        normalized = _normalize_shadow_quote_probe_target(row_dict.get("bucket"))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        targets.append(normalized)
        side_bucket = "unknown"
        if "|" in normalized:
            _, side_text = normalized.split("|", 1)
            normalized_side = _normalize_text(side_text).lower()
            if normalized_side in {"yes", "no"}:
                side_bucket = normalized_side
        side_target_counts[side_bucket] = int(side_target_counts.get(side_bucket, 0) + 1)
        if len(targets) >= safe_limit:
            break
    if targets:
        source_labels.append("by_market_side")

    if len(targets) < safe_limit:
        for row in by_market:
            row_dict = dict(row) if isinstance(row, dict) else {}
            normalized = _normalize_shadow_quote_probe_target(row_dict.get("bucket"))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            targets.append(normalized)
            if len(targets) >= safe_limit:
                break
        if len(targets) > 0 and "by_market" not in source_labels:
            source_labels.append("by_market")

    known_side_target_count = int(side_target_counts["yes"] + side_target_counts["no"])
    yes_target_share = None
    no_target_share = None
    side_imbalance_magnitude = None
    dominant_side = "unknown"
    dominant_side_target_share = None
    if known_side_target_count > 0:
        yes_target_share = float(side_target_counts["yes"]) / float(known_side_target_count)
        no_target_share = float(side_target_counts["no"]) / float(known_side_target_count)
        side_imbalance_magnitude = abs(float(yes_target_share) - float(no_target_share))
        if abs(float(yes_target_share) - float(no_target_share)) <= 1e-9:
            dominant_side = "mixed"
        elif float(yes_target_share) > float(no_target_share):
            dominant_side = "yes"
        else:
            dominant_side = "no"
        dominant_side_target_share = max(float(yes_target_share), float(no_target_share))

    return {
        "status": "ready" if targets else "empty",
        "target_count": int(len(targets)),
        "target_keys": targets,
        "sources_used": source_labels,
        "side_breakdown": {
            "counts_by_side": side_target_counts,
            "known_side_target_count": int(known_side_target_count),
            "dominant_side": dominant_side,
            "dominant_side_target_share": (
                round(float(dominant_side_target_share), 6)
                if isinstance(dominant_side_target_share, float)
                else None
            ),
            "yes_target_share": round(float(yes_target_share), 6) if isinstance(yes_target_share, float) else None,
            "no_target_share": round(float(no_target_share), 6) if isinstance(no_target_share, float) else None,
            "side_imbalance_magnitude": (
                round(float(side_imbalance_magnitude), 6)
                if isinstance(side_imbalance_magnitude, float)
                else None
            ),
        },
    }


def _build_execution_siphon_pressure(
    *,
    journal_metrics: dict[str, Any],
    calibration_status: str,
    min_quote_coverage_ratio: float,
) -> dict[str, Any]:
    safe_min_quote_coverage = max(0.0, min(1.0, float(min_quote_coverage_ratio)))
    quote_coverage_ratio_raw = _parse_float(journal_metrics.get("quote_two_sided_ratio_raw"))
    if not isinstance(quote_coverage_ratio_raw, float):
        quote_coverage_ratio_raw = _parse_float(journal_metrics.get("quote_two_sided_ratio"))
    quote_coverage_ratio_event_weighted = _parse_float(journal_metrics.get("quote_two_sided_ratio_event_weighted"))
    quote_coverage_ratio_for_gating = _parse_float(journal_metrics.get("quote_two_sided_ratio_for_gating"))
    quote_coverage_gating_mode = _normalize_text(journal_metrics.get("quote_coverage_gating_mode")) or "raw"
    quote_coverage_ratio = quote_coverage_ratio_for_gating
    if not isinstance(quote_coverage_ratio, float):
        quote_coverage_ratio = quote_coverage_ratio_raw
    if not isinstance(quote_coverage_ratio, float):
        quote_coverage_ratio = quote_coverage_ratio_event_weighted
    quote_coverage_shortfall = None
    if isinstance(quote_coverage_ratio, float):
        quote_coverage_shortfall = max(0.0, float(safe_min_quote_coverage) - float(quote_coverage_ratio))

    missing_buckets = journal_metrics.get("top_missing_coverage_buckets")
    missing_buckets = dict(missing_buckets) if isinstance(missing_buckets, dict) else {}
    by_market = missing_buckets.get("by_market")
    by_market = list(by_market) if isinstance(by_market, list) else []
    by_market_side = missing_buckets.get("by_market_side")
    by_market_side = list(by_market_side) if isinstance(by_market_side, list) else []
    uncovered_market_top5_share = None
    if by_market:
        share_total = 0.0
        share_count = 0
        for row in by_market[:5]:
            row_dict = dict(row) if isinstance(row, dict) else {}
            share = _parse_float(row_dict.get("share_of_uncovered_rows"))
            if isinstance(share, float):
                share_total += float(share)
                share_count += 1
        if share_count > 0:
            uncovered_market_top5_share = min(1.0, max(0.0, float(share_total)))

    side_share_by_side = {"yes": 0.0, "no": 0.0, "unknown": 0.0}
    side_count_by_side = {"yes": 0, "no": 0, "unknown": 0}
    share_inputs_present = False
    total_rows_from_market_side = 0
    for row in by_market_side:
        row_dict = dict(row) if isinstance(row, dict) else {}
        bucket = _normalize_text(row_dict.get("bucket"))
        side = "unknown"
        if "|" in bucket:
            _, side_text = bucket.split("|", 1)
            normalized_side = _normalize_text(side_text).lower()
            if normalized_side in {"yes", "no"}:
                side = normalized_side
        row_count = max(0, int(_parse_int(row_dict.get("rows_without_two_sided_quote"))))
        side_count_by_side[side] = int(side_count_by_side.get(side, 0) + row_count)
        total_rows_from_market_side += row_count
        row_share = _parse_float(row_dict.get("share_of_uncovered_rows"))
        if isinstance(row_share, float):
            share_inputs_present = True
            side_share_by_side[side] = float(side_share_by_side.get(side, 0.0) + max(0.0, float(row_share)))
    if not share_inputs_present and total_rows_from_market_side > 0:
        denominator = float(total_rows_from_market_side)
        side_share_by_side = {
            "yes": float(side_count_by_side["yes"]) / denominator,
            "no": float(side_count_by_side["no"]) / denominator,
            "unknown": float(side_count_by_side["unknown"]) / denominator,
        }
    else:
        side_share_by_side = {
            "yes": max(0.0, min(1.0, float(side_share_by_side["yes"]))),
            "no": max(0.0, min(1.0, float(side_share_by_side["no"]))),
            "unknown": max(0.0, min(1.0, float(side_share_by_side["unknown"]))),
        }

    yes_uncovered_share = float(side_share_by_side["yes"])
    no_uncovered_share = float(side_share_by_side["no"])
    known_side_uncovered_share = float(yes_uncovered_share + no_uncovered_share)
    dominant_side = "unknown"
    dominant_side_uncovered_share = None
    side_imbalance_magnitude = None
    side_pressure_score_contribution = 0.0
    if known_side_uncovered_share > 0.0:
        side_imbalance_magnitude = abs(float(yes_uncovered_share) - float(no_uncovered_share))
        dominant_side_uncovered_share = max(float(yes_uncovered_share), float(no_uncovered_share))
        if side_imbalance_magnitude <= 1e-9:
            dominant_side = "mixed"
        elif yes_uncovered_share > no_uncovered_share:
            dominant_side = "yes"
        else:
            dominant_side = "no"
        side_pressure_score_contribution = max(
            0.0,
            min(
                1.0,
                float(dominant_side_uncovered_share) * float(side_imbalance_magnitude),
            ),
        )

    low_coverage_wide_spread_tickers: list[str] = []
    seen_tickers: set[str] = set()
    for row in list(journal_metrics.get("top_tickers") or []):
        row_dict = dict(row) if isinstance(row, dict) else {}
        ticker = _normalize_text(row_dict.get("ticker")).upper()
        if not ticker or ticker in seen_tickers:
            continue
        coverage_ratio = _parse_float(row_dict.get("quote_coverage_ratio"))
        mean_spread = _parse_float(row_dict.get("mean_spread_dollars"))
        if not isinstance(coverage_ratio, float) or not isinstance(mean_spread, float):
            continue
        if coverage_ratio <= 0.15 and mean_spread >= 0.05:
            seen_tickers.add(ticker)
            low_coverage_wide_spread_tickers.append(ticker)
        if len(low_coverage_wide_spread_tickers) >= 20:
            break

    coverage_pressure = 0.0
    if isinstance(quote_coverage_shortfall, float) and safe_min_quote_coverage > 0:
        coverage_pressure = max(0.0, min(1.0, float(quote_coverage_shortfall) / float(safe_min_quote_coverage)))
    top5_pressure = float(uncovered_market_top5_share or 0.0)
    ticker_pressure = min(1.0, float(len(low_coverage_wide_spread_tickers)) / 20.0)
    calibration_pressure = 0.0
    normalized_calibration_status = _normalize_text(calibration_status).lower()
    if normalized_calibration_status == "red":
        calibration_pressure = 1.0
    elif normalized_calibration_status == "yellow":
        calibration_pressure = 0.5

    pressure_score = (
        0.40 * float(coverage_pressure)
        + 0.30 * float(top5_pressure)
        + 0.20 * float(ticker_pressure)
        + 0.10 * float(calibration_pressure)
        + 0.08 * float(side_pressure_score_contribution)
    )
    pressure_score = max(0.0, min(1.0, float(pressure_score)))
    return {
        "status": "ready",
        "quote_coverage_ratio_raw": (
            round(float(quote_coverage_ratio_raw), 6) if isinstance(quote_coverage_ratio_raw, float) else None
        ),
        "quote_coverage_ratio_event_weighted": (
            round(float(quote_coverage_ratio_event_weighted), 6)
            if isinstance(quote_coverage_ratio_event_weighted, float)
            else None
        ),
        "quote_coverage_ratio_for_gating": (
            round(float(quote_coverage_ratio_for_gating), 6)
            if isinstance(quote_coverage_ratio_for_gating, float)
            else None
        ),
        "quote_coverage_gating_mode": quote_coverage_gating_mode,
        "quote_coverage_shortfall": (
            round(float(quote_coverage_shortfall), 6) if isinstance(quote_coverage_shortfall, float) else None
        ),
        "uncovered_market_top5_share": (
            round(float(uncovered_market_top5_share), 6) if isinstance(uncovered_market_top5_share, float) else None
        ),
        "low_coverage_wide_spread_ticker_count": int(len(low_coverage_wide_spread_tickers)),
        "low_coverage_wide_spread_tickers": low_coverage_wide_spread_tickers,
        "dominant_uncovered_side": dominant_side,
        "dominant_uncovered_side_share": (
            round(float(dominant_side_uncovered_share), 6)
            if isinstance(dominant_side_uncovered_share, float)
            else None
        ),
        "yes_uncovered_share": round(float(yes_uncovered_share), 6),
        "no_uncovered_share": round(float(no_uncovered_share), 6),
        "side_imbalance_magnitude": (
            round(float(side_imbalance_magnitude), 6) if isinstance(side_imbalance_magnitude, float) else None
        ),
        "side_pressure_score_contribution": round(float(side_pressure_score_contribution), 6),
        "pressure_score": round(float(pressure_score), 6),
    }


def run_kalshi_temperature_execution_cost_tape(
    *,
    output_dir: str = "outputs",
    window_hours: float = 168.0,
    min_candidate_samples: int = 200,
    min_quote_coverage_ratio: float = 0.60,
    journal_db_path: str | None = None,
    max_tickers: int = 25,
    min_global_expected_edge_share_for_exclusion: float = 0.45,
    min_ticker_rows_for_exclusion: int = 200,
    exclusion_max_quote_coverage_ratio: float = 0.20,
    max_ticker_mean_spread_for_exclusion: float = 0.10,
    max_excluded_tickers: int = 12,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)
    latest_path = health_dir / "execution_cost_tape_latest.json"
    captured_at = datetime.now(timezone.utc)
    safe_window_hours = max(1.0, float(window_hours))
    window_start = captured_at - timedelta(hours=safe_window_hours)

    blocker_payload, blocker_file = _latest_json_payload(
        out_dir,
        (
            f"checkpoints/blocker_audit_{int(round(safe_window_hours))}h_latest.json",
            f"checkpoints/blocker_audit_{int(round(safe_window_hours))}h_*.json",
            "checkpoints/blocker_audit_168h_latest.json",
            "checkpoints/blocker_audit_168h_*.json",
        ),
    )
    intents_payload, intents_file = _latest_json_payload(
        out_dir,
        ("kalshi_temperature_trade_intents_summary_*.json",),
    )
    ws_payload, ws_file = _latest_json_payload(
        out_dir,
        ("kalshi_ws_state_latest.json", "kalshi_ws_state_*.json"),
    )
    frontier_payload, frontier_file = _latest_json_payload(
        out_dir,
        ("execution_frontier_report_*.json",),
    )

    data_gaps: list[str] = []
    if not isinstance(blocker_payload, dict):
        data_gaps.append("missing_blocker_audit_artifact")
    if not isinstance(intents_payload, dict):
        data_gaps.append("missing_intents_summary_artifact")
    if not isinstance(ws_payload, dict):
        data_gaps.append("missing_ws_state_artifact")

    blocker_headline = blocker_payload.get("headline") if isinstance(blocker_payload, dict) else {}
    blocker_headline = dict(blocker_headline) if isinstance(blocker_headline, dict) else {}
    top_reason = _normalize_text(blocker_headline.get("largest_blocker_reason_raw") or blocker_headline.get("largest_blocker_reason"))
    top_share = float(
        _parse_float(
            blocker_headline.get("largest_blocker_share_of_blocked_raw")
            or blocker_headline.get("largest_blocker_share_of_blocked")
        )
        or 0.0
    )
    blocked_total = int(_parse_int(blocker_headline.get("blocked_total")))

    intents_payload = intents_payload if isinstance(intents_payload, dict) else {}
    policy_reason_counts = intents_payload.get("policy_reason_counts")
    policy_reason_counts = dict(policy_reason_counts) if isinstance(policy_reason_counts, dict) else {}
    intents_blocked = int(_parse_int(intents_payload.get("intents_blocked")))
    expected_edge_direct = int(_parse_int(policy_reason_counts.get("expected_edge_below_min")))
    expected_edge_sparse = int(
        _parse_int(intents_payload.get("sparse_evidence_hardening_blocked_expected_edge_below_min_count"))
    )
    expected_edge_historical = int(
        _parse_int(intents_payload.get("historical_profitability_guardrail_blocked_expected_edge_below_min_count"))
    )
    expected_edge_bucket = int(
        _parse_int(intents_payload.get("historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count"))
    )
    expected_edge_pressure_count = max(expected_edge_direct, expected_edge_sparse, expected_edge_historical, expected_edge_bucket)
    expected_edge_share_latest = _safe_ratio(float(expected_edge_pressure_count), float(intents_blocked))
    expected_edge_floor_values = [
        float(item)
        for item in (intents_payload.get("effective_min_expected_edge_net_values") or [])
        if isinstance(_parse_float(item), float)
    ]
    expected_edge_floor_median = _quantile(expected_edge_floor_values, 0.50)
    expected_edge_floor_p90 = _quantile(expected_edge_floor_values, 0.90)

    journal_path = Path(journal_db_path) if journal_db_path else default_execution_journal_db_path(output_dir)
    journal_metrics = _journal_window_metrics(
        journal_db_path=journal_path,
        window_start_utc=window_start,
        max_tickers=max(1, int(max_tickers)),
    )
    journal_status = _normalize_text(journal_metrics.get("status"))
    if journal_status != "ready":
        data_gaps.append("missing_execution_journal_data")
    if journal_status == "empty_journal":
        data_gaps.append("empty_execution_journal_data")

    ws_metrics = _ws_state_metrics(ws_payload) if isinstance(ws_payload, dict) else {
        "status": "missing",
        "market_count": 0,
        "quote_two_sided_ratio": None,
        "sample_markets": [],
    }

    frontier_payload = frontier_payload if isinstance(frontier_payload, dict) else {}
    frontier_status = _normalize_text(frontier_payload.get("status")) or "missing"
    frontier_submitted = int(_parse_int(frontier_payload.get("submitted_orders")))
    frontier_filled = int(_parse_int(frontier_payload.get("filled_orders")))
    frontier_trusted_bucket_count = len(frontier_payload.get("trusted_break_even_edge_by_bucket") or {})

    quote_coverage_ratio_raw = _parse_float(journal_metrics.get("quote_two_sided_ratio_raw"))
    if not isinstance(quote_coverage_ratio_raw, float):
        quote_coverage_ratio_raw = _parse_float(journal_metrics.get("quote_two_sided_ratio"))
    quote_coverage_ratio_event_weighted = _parse_float(journal_metrics.get("quote_two_sided_ratio_event_weighted"))
    quote_coverage_ratio_for_gating = _parse_float(journal_metrics.get("quote_two_sided_ratio_for_gating"))
    quote_coverage_gating_mode = _normalize_text(journal_metrics.get("quote_coverage_gating_mode")).lower() or "raw"
    quote_coverage_ratio = quote_coverage_ratio_for_gating
    if not isinstance(quote_coverage_ratio, float):
        quote_coverage_ratio = quote_coverage_ratio_raw
    if not isinstance(quote_coverage_ratio, float):
        quote_coverage_ratio = quote_coverage_ratio_event_weighted
        if isinstance(quote_coverage_ratio, float):
            quote_coverage_gating_mode = "event_weighted_fallback"
    if not isinstance(quote_coverage_ratio, float):
        quote_coverage_ratio = _parse_float(ws_metrics.get("quote_two_sided_ratio"))
        if isinstance(quote_coverage_ratio, float):
            quote_coverage_gating_mode = "ws_fallback"

    candidate_rows = int(_parse_int(journal_metrics.get("candidate_rows")))
    meets_candidate_samples = candidate_rows >= int(max(1, int(min_candidate_samples)))
    meets_quote_coverage = (
        isinstance(quote_coverage_ratio, float)
        and quote_coverage_ratio >= float(max(0.0, min(1.0, float(min_quote_coverage_ratio))))
    )
    calibration_status = "green"
    calibration_reason = "execution_cost_calibration_ready"
    if not meets_candidate_samples:
        calibration_status = "red"
        calibration_reason = "insufficient_candidate_samples"
    elif not meets_quote_coverage:
        calibration_status = "yellow"
        calibration_reason = "quote_coverage_below_target"

    recommendations: list[str] = []
    if top_reason == "expected_edge_below_min":
        recommendations.append(
            "Expected-edge blockers dominate; prioritize execution-cost calibration before loosening quality thresholds."
        )
    if not meets_candidate_samples:
        recommendations.append(
            "Collect more candidate_seen/book_snapshot/order_terminal rows in the rolling window to improve cost-floor confidence."
        )
    if not meets_quote_coverage:
        recommendations.append(
            "Increase two-sided quote coverage telemetry (WS state + journal snapshots) before expected-edge floor retuning."
        )
    elif quote_coverage_gating_mode.startswith("event_weighted"):
        recommendations.append(
            "Quote-coverage gating is event-type weighted (order-terminal debiased); prioritize candidate/book quote completeness to keep calibration trustworthy."
        )
    if frontier_status != "ready":
        recommendations.append(
            "Execution frontier is missing or insufficient; keep live sizing constrained until trusted buckets are available."
        )
    if not recommendations:
        recommendations.append("Execution cost tape is healthy enough to support expected-edge floor calibration.")

    execution_siphon_pressure = _build_execution_siphon_pressure(
        journal_metrics=journal_metrics,
        calibration_status=calibration_status,
        min_quote_coverage_ratio=min_quote_coverage_ratio,
    )
    baseline_payload, baseline_file = _load_previous_execution_cost_tape_baseline(latest_path)
    baseline_calibration = dict(baseline_payload.get("calibration_readiness") or {}) if isinstance(baseline_payload, dict) else {}
    baseline_pressure = dict(baseline_payload.get("execution_siphon_pressure") or {}) if isinstance(baseline_payload, dict) else {}
    baseline_quote_coverage_ratio = _parse_float(baseline_calibration.get("quote_coverage_ratio"))
    baseline_candidate_rows = _parse_int(baseline_calibration.get("candidate_rows")) if isinstance(baseline_payload, dict) else 0
    baseline_siphon_pressure_score = _parse_float(baseline_pressure.get("pressure_score"))
    current_quote_coverage_ratio = _parse_float(quote_coverage_ratio)
    current_candidate_rows = int(candidate_rows)
    current_siphon_pressure_score = _parse_float(execution_siphon_pressure.get("pressure_score"))

    execution_siphon_trend = {
        "status": "missing_baseline",
        "baseline_file": str(baseline_file),
        "quote_coverage_ratio_delta": None,
        "candidate_rows_delta": None,
        "siphon_pressure_score_delta": None,
        "quote_coverage_shortfall_delta": None,
        "uncovered_market_top5_share_delta": None,
        "low_coverage_wide_spread_ticker_count_delta": None,
        "worsening_components": [],
        "improving_components": [],
        "worsening_component_count": 0,
        "improving_component_count": 0,
        "improving": False,
        "trend_direction": 0,
        "trend_label": "unknown",
        "worsening": False,
    }
    if isinstance(baseline_payload, dict):
        quote_delta = _delta_float(current_quote_coverage_ratio, baseline_quote_coverage_ratio)
        candidate_delta = None
        if isinstance(current_candidate_rows, int):
            candidate_delta = int(current_candidate_rows - int(baseline_candidate_rows))
        pressure_delta = _delta_float(current_siphon_pressure_score, baseline_siphon_pressure_score)
        shortfall_delta = (
            _delta_float(
                execution_siphon_pressure.get("quote_coverage_shortfall"),
                baseline_pressure.get("quote_coverage_shortfall"),
            )
            if "quote_coverage_shortfall" in baseline_pressure
            else None
        )
        uncovered_top5_delta = (
            _delta_float(
                (
                    execution_siphon_pressure.get("uncovered_market_top5_share")
                    if execution_siphon_pressure.get("uncovered_market_top5_share") is not None
                    else 0.0
                ),
                (
                    baseline_pressure.get("uncovered_market_top5_share")
                    if baseline_pressure.get("uncovered_market_top5_share") is not None
                    else 0.0
                ),
            )
            if "uncovered_market_top5_share" in baseline_pressure
            else None
        )
        wide_spread_ticker_count_delta = (
            int(
                _parse_int(execution_siphon_pressure.get("low_coverage_wide_spread_ticker_count"))
                - _parse_int(baseline_pressure.get("low_coverage_wide_spread_ticker_count"))
            )
            if "low_coverage_wide_spread_ticker_count" in baseline_pressure
            else None
        )
        worsening_components: list[str] = []
        improving_components: list[str] = []
        if isinstance(quote_delta, float):
            if quote_delta < 0.0:
                worsening_components.append("quote_coverage_ratio")
            elif quote_delta > 0.0:
                improving_components.append("quote_coverage_ratio")
        if isinstance(pressure_delta, float):
            if pressure_delta > 0.0:
                worsening_components.append("siphon_pressure_score")
            elif pressure_delta < 0.0:
                improving_components.append("siphon_pressure_score")
        if isinstance(shortfall_delta, float):
            if shortfall_delta > 0.0:
                worsening_components.append("quote_coverage_shortfall")
            elif shortfall_delta < 0.0:
                improving_components.append("quote_coverage_shortfall")
        if isinstance(uncovered_top5_delta, float):
            if uncovered_top5_delta > 0.0:
                worsening_components.append("uncovered_market_top5_share")
            elif uncovered_top5_delta < 0.0:
                improving_components.append("uncovered_market_top5_share")
        if isinstance(wide_spread_ticker_count_delta, int):
            if wide_spread_ticker_count_delta > 0:
                worsening_components.append("low_coverage_wide_spread_ticker_count")
            elif wide_spread_ticker_count_delta < 0:
                improving_components.append("low_coverage_wide_spread_ticker_count")
        worsening_flag = len(worsening_components) > 0
        improving_flag = len(improving_components) > 0 and not worsening_flag
        trend_direction = 0
        trend_label = "flat"
        if worsening_flag and not improving_components:
            trend_direction = 1
            trend_label = "worsening"
        elif improving_flag:
            trend_direction = -1
            trend_label = "improving"
        elif worsening_flag and improving_components:
            trend_direction = 0
            trend_label = "mixed"
        execution_siphon_trend = {
            "status": "ready",
            "baseline_file": str(baseline_file),
            "quote_coverage_ratio_delta": quote_delta,
            "candidate_rows_delta": candidate_delta,
            "siphon_pressure_score_delta": pressure_delta,
            "quote_coverage_shortfall_delta": shortfall_delta,
            "uncovered_market_top5_share_delta": uncovered_top5_delta,
            "low_coverage_wide_spread_ticker_count_delta": wide_spread_ticker_count_delta,
            "worsening_components": worsening_components,
            "improving_components": improving_components,
            "worsening_component_count": int(len(worsening_components)),
            "improving_component_count": int(len(improving_components)),
            "improving": bool(improving_flag),
            "trend_direction": int(trend_direction),
            "trend_label": trend_label,
            "worsening": bool(worsening_flag),
        }

    missing_coverage_buckets = journal_metrics.get("top_missing_coverage_buckets")
    missing_coverage_buckets = dict(missing_coverage_buckets) if isinstance(missing_coverage_buckets, dict) else {}
    recommended_exclusions = _build_recommended_exclusions(
        top_reason=top_reason,
        top_share=top_share,
        ticker_rows=list(journal_metrics.get("top_tickers") or []),
        missing_market_side_rows=list(missing_coverage_buckets.get("by_market_side") or []),
        execution_siphon_pressure=execution_siphon_pressure,
        execution_siphon_trend=execution_siphon_trend,
        min_global_expected_edge_share_for_exclusion=min_global_expected_edge_share_for_exclusion,
        min_ticker_rows_for_exclusion=min_ticker_rows_for_exclusion,
        exclusion_max_quote_coverage_ratio=exclusion_max_quote_coverage_ratio,
        max_ticker_mean_spread_for_exclusion=max_ticker_mean_spread_for_exclusion,
        max_excluded_tickers=max_excluded_tickers,
    )
    recommended_shadow_quote_probe_targets = _build_recommended_shadow_quote_probe_targets(
        journal_metrics=journal_metrics,
        max_target_keys=max_tickers,
    )
    if int(_parse_int(recommended_exclusions.get("excluded_ticker_count"))) > 0:
        recommendations.append(
            "Route coldmath replication through execution-cost-tape market exclusions until quote coverage normalizes."
        )
    if int(_parse_int(recommended_shadow_quote_probe_targets.get("target_count"))) > 0:
        recommendations.append(
            "Use targeted shadow quote probes on top missing market-side coverage buckets before broad quote probing."
        )

    payload: dict[str, Any] = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "window_hours": round(float(safe_window_hours), 3),
        "window_label": f"{int(round(safe_window_hours))}h",
        "expected_edge_blocking": {
            "largest_blocker_reason": top_reason,
            "largest_blocker_share_of_blocked": round(float(top_share), 6),
            "blocked_total": int(blocked_total),
            "latest_intents_blocked": int(intents_blocked),
            "latest_expected_edge_direct_count": int(expected_edge_direct),
            "latest_expected_edge_pressure_count": int(expected_edge_pressure_count),
            "latest_expected_edge_pressure_share_of_blocked": (
                round(float(expected_edge_share_latest), 6) if isinstance(expected_edge_share_latest, float) else None
            ),
            "latest_expected_edge_floor_median": (
                round(float(expected_edge_floor_median), 6) if isinstance(expected_edge_floor_median, float) else None
            ),
            "latest_expected_edge_floor_p90": (
                round(float(expected_edge_floor_p90), 6) if isinstance(expected_edge_floor_p90, float) else None
            ),
        },
        "execution_cost_observations": journal_metrics,
        "ws_state_observations": ws_metrics,
        "frontier_observations": {
            "status": frontier_status,
            "submitted_orders": int(frontier_submitted),
            "filled_orders": int(frontier_filled),
            "trusted_bucket_count": int(frontier_trusted_bucket_count),
        },
        "calibration_readiness": {
            "status": calibration_status,
            "reason": calibration_reason,
            "candidate_rows": int(candidate_rows),
            "min_candidate_samples": int(max(1, int(min_candidate_samples))),
            "meets_candidate_samples": bool(meets_candidate_samples),
            "quote_coverage_ratio": round(float(quote_coverage_ratio), 6) if isinstance(quote_coverage_ratio, float) else None,
            "quote_coverage_ratio_raw": (
                round(float(quote_coverage_ratio_raw), 6) if isinstance(quote_coverage_ratio_raw, float) else None
            ),
            "quote_coverage_ratio_event_weighted": (
                round(float(quote_coverage_ratio_event_weighted), 6)
                if isinstance(quote_coverage_ratio_event_weighted, float)
                else None
            ),
            "quote_coverage_ratio_for_gating": (
                round(float(quote_coverage_ratio_for_gating), 6)
                if isinstance(quote_coverage_ratio_for_gating, float)
                else None
            ),
            "quote_coverage_gating_mode": quote_coverage_gating_mode,
            "min_quote_coverage_ratio": round(float(max(0.0, min(1.0, float(min_quote_coverage_ratio)))), 6),
            "meets_quote_coverage": bool(meets_quote_coverage),
        },
        "recommended_exclusions": recommended_exclusions,
        "recommended_shadow_quote_probe_targets": recommended_shadow_quote_probe_targets,
        "execution_siphon_pressure": execution_siphon_pressure,
        "execution_siphon_trend": execution_siphon_trend,
        "data_pipeline_gaps": sorted(set(data_gaps)),
        "recommendations": recommendations,
        "source_files": {
            "blocker_audit": blocker_file,
            "trade_intents_summary": intents_file,
            "ws_state": ws_file,
            "execution_frontier": frontier_file,
            "execution_journal": str(journal_path),
        },
    }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"execution_cost_tape_{stamp}.json"
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    output_path.write_text(encoded, encoding="utf-8")
    latest_path.write_text(encoded, encoding="utf-8")
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)
    return payload
