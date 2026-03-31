from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from betbot.dns_guard import urlopen_with_dns_recovery
from betbot.kalshi_execution_frontier import run_kalshi_execution_frontier
from betbot.kalshi_execution_journal import (
    append_execution_events,
    default_execution_journal_db_path,
)
from betbot.kalshi_book import (
    count_open_positions,
    default_book_db_path,
    list_matching_open_orders,
    record_decisions,
    record_order_attempts,
    update_order_statuses,
)
from betbot.kalshi_book_math import derive_top_of_book
from betbot.kalshi_micro_ledger import (
    append_trade_ledger,
    default_ledger_path,
    ledger_rows_from_attempts,
    summarize_trade_ledger,
    trading_day_for_timestamp,
)
from betbot.kalshi_micro_gate import build_trade_gate_decision, count_meaningful_candidates
from betbot.kalshi_micro_plan import run_kalshi_micro_plan
from betbot.kalshi_nonsports_categories import run_kalshi_nonsports_categories
from betbot.kalshi_nonsports_deltas import run_kalshi_nonsports_deltas
from betbot.kalshi_nonsports_persistence import run_kalshi_nonsports_persistence
from betbot.kalshi_nonsports_pressure import run_kalshi_nonsports_pressure
from betbot.kalshi_nonsports_quality import run_kalshi_nonsports_quality
from betbot.kalshi_nonsports_signals import run_kalshi_nonsports_signals
from betbot.kalshi_nonsports_scan import _parse_float
from betbot.kalshi_ws_state import default_ws_state_path, load_ws_state_authority
from betbot.live_smoke import (
    HttpGetter,
    KALSHI_API_ROOTS,
    KalshiSigner,
    _http_get_json,
    _kalshi_sign_request,
    kalshi_api_root_candidates,
)
from betbot.onboarding import _parse_env_file

try:  # pragma: no cover - platform specific
    import fcntl  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - defensive fallback
    fcntl = None


AuthenticatedRequester = Callable[
    [str, str, dict[str, str], Any | None, float],
    tuple[int, Any],
]
PlanRunner = Callable[..., dict[str, Any]]
TimeSleeper = Callable[[float], None]
SummaryRunner = Callable[..., dict[str, Any]]
CategoryRunner = Callable[..., dict[str, Any]]
PressureRunner = Callable[..., dict[str, Any]]
KALSHI_HTTP_NETWORK_MAX_RETRIES = 6
KALSHI_HTTP_DNS_MAX_RETRIES = 1
KALSHI_HTTP_NETWORK_BACKOFF_SECONDS = 0.35
KALSHI_DNS_ERROR_MARKERS = (
    "nodename nor servname",
    "name or service not known",
    "temporary failure in name resolution",
    "no address associated with hostname",
)


def _decode_response_body(raw_body: bytes) -> Any:
    text = raw_body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text_excerpt": text[:200]}


def _url_error_reason_text(exc: URLError) -> str:
    reason = exc.reason
    if isinstance(reason, BaseException):
        return str(reason)
    return str(reason or exc)


def _is_dns_resolution_error(exc: URLError) -> bool:
    reason = exc.reason
    if isinstance(reason, socket.gaierror):
        return True
    text = _url_error_reason_text(exc).lower()
    return any(marker in text for marker in KALSHI_DNS_ERROR_MARKERS)


def _is_retryable_network_error(exc: URLError | TimeoutError) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    reason = exc.reason
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return True
    if _is_dns_resolution_error(exc):
        return True
    text = _url_error_reason_text(exc).lower()
    transient_markers = (
        "timed out",
        "temporarily unavailable",
        "network is unreachable",
        "connection reset",
        "connection refused",
    )
    return any(marker in text for marker in transient_markers)


def _is_transient_orderbook_unavailable_attempt(attempt: dict[str, Any]) -> bool:
    if str(attempt.get("result") or "").strip().lower() != "orderbook_unavailable":
        return False
    error_type = str(attempt.get("orderbook_error_type") or "").strip().lower()
    if error_type in {"url_error", "timeout_error", "network_error"}:
        return True
    error_text = str(attempt.get("orderbook_error") or "").strip().lower()
    if any(marker in error_text for marker in KALSHI_DNS_ERROR_MARKERS):
        return True
    transient_markers = (
        "timed out",
        "timeout",
        "temporary failure in name resolution",
        "network is unreachable",
        "connection reset",
        "connection refused",
    )
    return any(marker in error_text for marker in transient_markers)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _coalesce_float(*values: Any) -> float | None:
    for value in values:
        parsed = _parse_float(value)
        if isinstance(parsed, float):
            return parsed
    return None


def _execution_policy_metrics(
    *,
    plan: dict[str, Any],
    attempt: dict[str, Any],
    orderbook: dict[str, Any],
    resting_hold_seconds: float,
) -> dict[str, Any]:
    planned_contracts = _coalesce_float(
        plan.get("contracts_per_order"),
        plan.get("order_payload_preview", {}).get("count") if isinstance(plan.get("order_payload_preview"), dict) else None,
        attempt.get("planned_contracts"),
    )
    if planned_contracts is None or planned_contracts <= 0.0:
        planned_contracts = 1.0

    planned_entry_price = _coalesce_float(
        attempt.get("planned_entry_price_dollars"),
        plan.get("maker_entry_price_dollars"),
        plan.get("maker_yes_price_dollars"),
    )
    side = str(attempt.get("planned_side") or "").strip().lower()
    same_side_best_bid = _coalesce_float(attempt.get("current_best_same_side_bid_dollars"))
    same_side_depth = _coalesce_float(attempt.get("current_best_same_side_bid_size_contracts")) or 0.0
    yes_bid = _coalesce_float(attempt.get("current_best_yes_bid_dollars"))
    yes_ask = _coalesce_float(
        orderbook.get("best_yes_ask_dollars"),
        attempt.get("planned_yes_ask_dollars"),
        plan.get("yes_ask_dollars"),
    )

    spread = None
    if isinstance(yes_bid, float) and isinstance(yes_ask, float):
        spread = round(max(0.0, yes_ask - yes_bid), 6)

    price_for_scale = planned_entry_price if isinstance(planned_entry_price, float) and planned_entry_price > 0 else 0.5
    spread_for_scale = max(float(spread or 0.01), 0.005)

    aggressiveness = 0.5
    if isinstance(planned_entry_price, float) and isinstance(same_side_best_bid, float):
        aggressiveness = _clamp(
            0.5 + ((planned_entry_price - same_side_best_bid) / spread_for_scale),
            0.0,
            1.0,
        )

    queue_ahead_estimate = 0.0
    if isinstance(same_side_best_bid, float) and isinstance(planned_entry_price, float):
        if planned_entry_price <= same_side_best_bid + 1e-6:
            queue_ahead_estimate = same_side_depth
        else:
            queue_ahead_estimate = max(0.0, same_side_depth * (1.0 - aggressiveness))

    order_size_depth_ratio = planned_contracts / max(planned_contracts, same_side_depth, 1.0)
    queue_ratio = queue_ahead_estimate / max(queue_ahead_estimate + 10.0, 1.0)

    hours_to_close = _coalesce_float(plan.get("hours_to_close"), attempt.get("market_hours_to_close"))
    urgency = 0.5
    if isinstance(hours_to_close, float) and hours_to_close > 0:
        urgency = _clamp(1.0 - (hours_to_close / 168.0), 0.0, 1.0)

    spread_penalty = _clamp(spread_for_scale / 0.05, 0.0, 1.0)
    base_fill_60s = _clamp(
        0.22 + (0.45 * aggressiveness) + (0.12 * urgency) - (0.35 * order_size_depth_ratio) - (0.18 * spread_penalty) - (0.1 * queue_ratio),
        0.02,
        0.98,
    )
    fill_prob_10s = round(1.0 - ((1.0 - base_fill_60s) ** (10.0 / 60.0)), 6)
    fill_prob_60s = round(base_fill_60s, 6)
    fill_prob_300s = round(1.0 - ((1.0 - base_fill_60s) ** (300.0 / 60.0)), 6)

    if resting_hold_seconds > 0:
        horizon_seconds = max(10.0, min(300.0, resting_hold_seconds))
    elif isinstance(hours_to_close, float) and hours_to_close > 0:
        horizon_seconds = max(60.0, min(300.0, hours_to_close * 90.0))
    else:
        horizon_seconds = 180.0
    fill_prob_horizon = round(1.0 - ((1.0 - base_fill_60s) ** (horizon_seconds / 60.0)), 6)
    full_fill_prob_horizon = round(fill_prob_horizon * _clamp(1.0 - (0.55 * order_size_depth_ratio), 0.05, 1.0), 6)
    partial_fill_prob_horizon = round(max(0.0, fill_prob_horizon - full_fill_prob_horizon), 6)

    edge_net_per_contract = _coalesce_float(
        plan.get("maker_entry_edge_conservative_net_total"),
        plan.get("maker_entry_edge_net_total"),
        plan.get("maker_entry_edge_conservative_net_fees"),
        plan.get("maker_entry_edge_net_fees"),
        plan.get("maker_entry_edge_conservative"),
        plan.get("maker_entry_edge"),
    )
    if edge_net_per_contract is None:
        return {
            "planned_contracts": round(planned_contracts, 4),
            "market_hours_to_close": hours_to_close if isinstance(hours_to_close, float) else "",
            "market_spread_dollars": spread if isinstance(spread, float) else "",
            "queue_ahead_estimate_contracts": round(queue_ahead_estimate, 4),
            "order_size_depth_ratio": round(order_size_depth_ratio, 6),
            "quote_aggressiveness": round(aggressiveness, 6),
            "signal_confidence": plan.get("confidence", ""),
            "signal_evidence_count": plan.get("effective_min_evidence_count", ""),
            "signal_age_seconds": "",
            "execution_fill_probability_10s": fill_prob_10s,
            "execution_fill_probability_60s": fill_prob_60s,
            "execution_fill_probability_300s": fill_prob_300s,
            "execution_fill_probability_horizon": fill_prob_horizon,
            "execution_full_fill_probability_horizon": full_fill_prob_horizon,
            "execution_partial_fill_probability_horizon": partial_fill_prob_horizon,
            "execution_expected_spread_capture_per_contract_dollars": "",
            "execution_expected_adverse_selection_per_contract_dollars": "",
            "execution_expected_partial_fill_drag_per_contract_dollars": "",
            "execution_expected_cancel_replace_leakage_per_contract_dollars": "",
            "execution_expected_retry_slippage_per_contract_dollars": "",
            "execution_expected_cost_stack_per_contract_dollars": "",
            "execution_break_even_edge_per_contract_dollars": "",
            "execution_forecast_edge_net_per_contract_dollars": "",
            "execution_expected_net_pnl_if_fill_per_contract_dollars": "",
            "execution_ev_submit_dollars": "",
            "execution_policy_active": False,
            "execution_policy_decision": "submit",
            "execution_policy_reason": "edge_inputs_missing_assume_plan_prequalified",
        }

    volatility_proxy = _clamp(spread_for_scale / max(price_for_scale, 0.05), 0.0, 1.0)
    expected_spread_capture = max(0.0, spread_for_scale * (1.0 - aggressiveness) * 0.25)
    expected_adverse_selection = max(
        0.0001,
        spread_for_scale * (0.10 + (0.35 * aggressiveness) + (0.30 * volatility_proxy)),
    )
    expected_partial_fill_drag = abs(edge_net_per_contract) * partial_fill_prob_horizon * 0.2
    expected_cancel_replace_leakage = 0.0002 + (0.0003 if resting_hold_seconds > 0 else 0.0001)
    expected_retry_slippage = 0.00015 if int(orderbook.get("http_status") or 0) not in {0, 200} else 0.0
    expected_cost_stack = max(
        0.0,
        expected_adverse_selection
        + expected_partial_fill_drag
        + expected_cancel_replace_leakage
        + expected_retry_slippage
        - expected_spread_capture,
    )
    uncertainty_buffer = max(0.0005, (spread_for_scale * 0.08) + (abs(edge_net_per_contract) * 0.05))
    break_even_edge = expected_cost_stack + uncertainty_buffer
    expected_net_if_fill = (
        edge_net_per_contract
        - expected_adverse_selection
        - expected_partial_fill_drag
        - expected_cancel_replace_leakage
        - expected_retry_slippage
        + expected_spread_capture
    )
    ev_submit_dollars = fill_prob_horizon * expected_net_if_fill * planned_contracts

    policy_decision = "submit"
    policy_reason = "positive_ev_submit"
    if fill_prob_horizon < 0.05:
        policy_decision = "skip"
        policy_reason = "fill_probability_too_low_before_signal_decay"
    elif expected_net_if_fill <= 0.0:
        policy_decision = "skip"
        policy_reason = "negative_expected_net_after_execution_costs"
    elif ev_submit_dollars <= 0.0:
        policy_decision = "skip"
        policy_reason = "non_positive_ev_submit"
    elif edge_net_per_contract < break_even_edge:
        policy_decision = "skip"
        policy_reason = "forecast_edge_below_break_even_edge"

    return {
        "planned_contracts": round(planned_contracts, 4),
        "market_hours_to_close": hours_to_close if isinstance(hours_to_close, float) else "",
        "market_spread_dollars": round(spread_for_scale, 6),
        "queue_ahead_estimate_contracts": round(queue_ahead_estimate, 4),
        "order_size_depth_ratio": round(order_size_depth_ratio, 6),
        "quote_aggressiveness": round(aggressiveness, 6),
        "signal_confidence": plan.get("confidence", ""),
        "signal_evidence_count": plan.get("effective_min_evidence_count", ""),
        "signal_age_seconds": "",
        "execution_fill_probability_10s": fill_prob_10s,
        "execution_fill_probability_60s": fill_prob_60s,
        "execution_fill_probability_300s": fill_prob_300s,
        "execution_fill_probability_horizon": fill_prob_horizon,
        "execution_full_fill_probability_horizon": full_fill_prob_horizon,
        "execution_partial_fill_probability_horizon": partial_fill_prob_horizon,
        "execution_expected_spread_capture_per_contract_dollars": round(expected_spread_capture, 6),
        "execution_expected_adverse_selection_per_contract_dollars": round(expected_adverse_selection, 6),
        "execution_expected_partial_fill_drag_per_contract_dollars": round(expected_partial_fill_drag, 6),
        "execution_expected_cancel_replace_leakage_per_contract_dollars": round(expected_cancel_replace_leakage, 6),
        "execution_expected_retry_slippage_per_contract_dollars": round(expected_retry_slippage, 6),
        "execution_expected_cost_stack_per_contract_dollars": round(expected_cost_stack, 6),
        "execution_break_even_edge_per_contract_dollars": round(break_even_edge, 6),
        "execution_forecast_edge_net_per_contract_dollars": round(edge_net_per_contract, 6),
        "execution_expected_net_pnl_if_fill_per_contract_dollars": round(expected_net_if_fill, 6),
        "execution_ev_submit_dollars": round(ev_submit_dollars, 6),
        "execution_policy_active": True,
        "execution_policy_decision": policy_decision,
        "execution_policy_reason": policy_reason,
    }


def _http_request_json(
    url: str,
    method: str,
    headers: dict[str, str],
    body: Any | None,
    timeout_seconds: float,
) -> tuple[int, Any]:
    request_headers = dict(headers)
    request_data = None
    if body is not None:
        request_data = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = Request(url=url, headers=request_headers, data=request_data, method=method)
    for attempt in range(KALSHI_HTTP_NETWORK_MAX_RETRIES + 1):
        try:
            with urlopen_with_dns_recovery(
                request,
                timeout_seconds=timeout_seconds,
                urlopen_fn=urlopen,
            ) as response:
                return response.getcode(), _decode_response_body(response.read())
        except HTTPError as exc:
            return exc.code, _decode_response_body(exc.read())
        except (URLError, TimeoutError) as exc:
            if isinstance(exc, URLError) and not _is_retryable_network_error(exc):
                return 599, {"error": _url_error_reason_text(exc), "error_type": "url_error"}
            max_retries = KALSHI_HTTP_NETWORK_MAX_RETRIES
            if isinstance(exc, URLError) and _is_dns_resolution_error(exc):
                max_retries = min(max_retries, KALSHI_HTTP_DNS_MAX_RETRIES)
            if attempt >= max_retries:
                if isinstance(exc, URLError):
                    return 599, {"error": _url_error_reason_text(exc), "error_type": "url_error"}
                return 599, {"error": str(exc), "error_type": "timeout_error"}
            time.sleep(KALSHI_HTTP_NETWORK_BACKOFF_SECONDS * (2**attempt))
    return 599, {"error": "request_retry_budget_exhausted", "error_type": "network_error"}


def _execution_frontier_bucket_for_attempt(attempt: dict[str, Any]) -> str:
    aggressiveness = _parse_float(attempt.get("quote_aggressiveness"))
    spread = _parse_float(attempt.get("market_spread_dollars"))
    hours_to_close = _parse_float(attempt.get("market_hours_to_close"))

    if aggressiveness is None:
        aggr_bucket = "aggr_unknown"
    elif aggressiveness < 0.34:
        aggr_bucket = "aggr_passive"
    elif aggressiveness < 0.67:
        aggr_bucket = "aggr_mid"
    else:
        aggr_bucket = "aggr_aggressive"

    if spread is None:
        spread_bucket = "spread_unknown"
    elif spread < 0.01:
        spread_bucket = "spread_tight"
    elif spread < 0.03:
        spread_bucket = "spread_mid"
    else:
        spread_bucket = "spread_wide"

    if hours_to_close is None:
        ttc_bucket = "ttc_unknown"
    elif hours_to_close <= 6.0:
        ttc_bucket = "ttc_near"
    elif hours_to_close <= 24.0:
        ttc_bucket = "ttc_short"
    else:
        ttc_bucket = "ttc_long"
    return f"{aggr_bucket}|{spread_bucket}|{ttc_bucket}"


def _load_latest_break_even_edges_by_bucket(
    output_dir: str,
) -> tuple[dict[str, float], str | None, dict[str, dict[str, Any]]]:
    out_dir = Path(output_dir)
    candidates = sorted(
        out_dir.glob("execution_frontier_report_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    if not candidates:
        return {}, None, {}
    latest_path = candidates[-1]
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, str(latest_path), {}
    if not isinstance(payload, dict):
        return {}, str(latest_path), {}
    raw = payload.get("trusted_break_even_edge_by_bucket")
    if not isinstance(raw, dict):
        raw = payload.get("break_even_edge_by_bucket")
    if not isinstance(raw, dict):
        return {}, str(latest_path), {}
    edges: dict[str, float] = {}
    for bucket, value in raw.items():
        if not isinstance(bucket, str):
            continue
        parsed = _parse_float(value)
        if isinstance(parsed, float):
            edges[bucket] = float(parsed)
    bucket_trust_raw = payload.get("bucket_markout_trust_by_bucket")
    bucket_trust_by_bucket: dict[str, dict[str, Any]] = {}
    if isinstance(bucket_trust_raw, dict):
        for bucket, trust_payload in bucket_trust_raw.items():
            if not isinstance(bucket, str) or not isinstance(trust_payload, dict):
                continue
            bucket_trust_by_bucket[bucket] = {
                "trusted": bool(trust_payload.get("trusted")),
                "reason": str(trust_payload.get("reason") or "").strip(),
                "markout_10s_samples": int(trust_payload.get("markout_10s_samples") or 0),
                "markout_60s_samples": int(trust_payload.get("markout_60s_samples") or 0),
                "markout_300s_samples": int(trust_payload.get("markout_300s_samples") or 0),
            }
    return edges, str(latest_path), bucket_trust_by_bucket


def _signed_kalshi_request(
    *,
    env_data: dict[str, str],
    method: str,
    path_with_query: str,
    body: Any | None,
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
    sign_request: KalshiSigner,
) -> tuple[int, Any]:
    transient_http_status_codes = {408, 425, 500, 502, 503, 504}
    retryable_error_types = {"url_error", "timeout_error", "network_error"}
    env_name = (env_data.get("KALSHI_ENV") or "").strip().lower()
    if env_name not in KALSHI_API_ROOTS:
        return 599, {
            "error": "missing_or_invalid_kalshi_env",
            "error_type": "config_error",
            "details": {"KALSHI_ENV": env_data.get("KALSHI_ENV")},
        }
    missing: list[str] = []
    for key in ("KALSHI_ACCESS_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        if not str(env_data.get(key) or "").strip():
            missing.append(key)
    if missing:
        return 599, {
            "error": "missing_kalshi_credentials",
            "error_type": "config_error",
            "details": {"missing": missing},
        }
    api_roots = kalshi_api_root_candidates(env_name)
    attempted_roots: list[str] = []
    failover_errors: list[str] = []
    final_status = 599
    final_payload: Any = {"error": "request_not_attempted", "error_type": "network_error"}

    for index, api_root in enumerate(api_roots):
        request_url = f"{api_root}{path_with_query}"
        attempted_roots.append(api_root)
        timestamp_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        try:
            signature = sign_request(
                env_data["KALSHI_PRIVATE_KEY_PATH"],
                timestamp_ms,
                method,
                urlparse(request_url).path,
            )
        except Exception as exc:  # pragma: no cover - defensive runtime path
            return 599, {
                "error": f"signature_generation_failed:{exc}",
                "error_type": "signing_error",
            }
        headers = {
            "Accept": "application/json",
            "KALSHI-ACCESS-KEY": env_data["KALSHI_ACCESS_KEY_ID"],
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "User-Agent": "betbot-kalshi-micro-execute/1.0",
        }

        status_code, payload = http_request_json(request_url, method, headers, body, timeout_seconds)
        final_status = status_code
        final_payload = payload

        payload_error_type = ""
        payload_error_text = ""
        if isinstance(payload, dict):
            payload_error_type = str(payload.get("error_type") or "").strip().lower()
            payload_error_text = str(payload.get("error") or "").strip()
            payload.setdefault("api_root_used", api_root)
            if index > 0:
                payload.setdefault("api_roots_attempted", attempted_roots)
            if failover_errors:
                payload.setdefault("api_root_failover_errors", list(failover_errors))

        should_failover = False
        if status_code == 599 and payload_error_type in retryable_error_types:
            should_failover = True
            if payload_error_text:
                failover_errors.append(f"{api_root}: {payload_error_text}")
        elif status_code in transient_http_status_codes:
            should_failover = True
            failover_errors.append(f"{api_root}: http_{status_code}")

        if should_failover and index < len(api_roots) - 1:
            continue
        return final_status, final_payload

    if isinstance(final_payload, dict):
        final_payload.setdefault("api_roots_attempted", attempted_roots)
        if failover_errors:
            final_payload.setdefault("api_root_failover_errors", list(failover_errors))
    return final_status, final_payload


def _fetch_orderbook_top(
    *,
    env_data: dict[str, str],
    ticker: str,
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
    sign_request: KalshiSigner,
) -> dict[str, Any]:
    status_code, payload = _signed_kalshi_request(
        env_data=env_data,
        method="GET",
        path_with_query=f"/markets/{ticker}/orderbook?depth=1",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    result: dict[str, Any] = {"http_status": status_code}
    if status_code != 200 or not isinstance(payload, dict):
        if isinstance(payload, dict):
            error_type = str(payload.get("error_type") or "").strip()
            error_text = str(payload.get("error") or "").strip()
            raw_text = str(payload.get("raw_text_excerpt") or "").strip()
            if error_type:
                result["error_type"] = error_type
            if error_text:
                result["error"] = error_text
            elif raw_text:
                result["error"] = raw_text
        if "error" not in result:
            result["error"] = f"orderbook request failed with status {status_code}"
        return result

    orderbook = payload.get("orderbook_fp")
    if not isinstance(orderbook, dict):
        result["error"] = "orderbook_fp missing from payload"
        result["error_type"] = "payload_shape_error"
        return result

    result.update(derive_top_of_book(orderbook))
    return result


def _fetch_queue_position(
    *,
    env_data: dict[str, str],
    order_id: str,
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
    sign_request: KalshiSigner,
) -> dict[str, Any]:
    status_code, payload = _signed_kalshi_request(
        env_data=env_data,
        method="GET",
        path_with_query=f"/portfolio/orders/{order_id}/queue_position",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    result = {"http_status": status_code}
    if status_code == 200 and isinstance(payload, dict):
        result["queue_position_contracts"] = _parse_float(payload.get("queue_position_fp"))
    else:
        result["error"] = f"queue position request failed with status {status_code}"
    return result


def _create_order(
    *,
    env_data: dict[str, str],
    payload: dict[str, Any],
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
    sign_request: KalshiSigner,
) -> tuple[int, Any]:
    return _signed_kalshi_request(
        env_data=env_data,
        method="POST",
        path_with_query="/portfolio/orders",
        body=payload,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )


def _cancel_order(
    *,
    env_data: dict[str, str],
    order_id: str,
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
    sign_request: KalshiSigner,
) -> tuple[int, Any]:
    return _signed_kalshi_request(
        env_data=env_data,
        method="DELETE",
        path_with_query=f"/portfolio/orders/{order_id}",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )


def _read_exchange_status(
    *,
    env_data: dict[str, str],
    timeout_seconds: float,
    http_request_json: AuthenticatedRequester,
) -> dict[str, Any]:
    env_name = (env_data.get("KALSHI_ENV") or "").strip().lower()
    if env_name not in KALSHI_API_ROOTS:
        return {
            "checked": True,
            "http_status": None,
            "trading_active": False,
            "exchange_active": False,
            "status_ok": False,
            "error": "missing_or_invalid_kalshi_env",
        }

    api_roots = kalshi_api_root_candidates(env_name)
    retryable_error_types = {"url_error", "timeout_error", "network_error"}
    transient_http_status_codes = {408, 425, 500, 502, 503, 504, 599}
    attempted_roots: list[str] = []
    final_status: int | None = None
    final_payload: Any = None
    final_api_root = ""
    for index, api_root in enumerate(api_roots):
        attempted_roots.append(api_root)
        status_code, payload = http_request_json(
            f"{api_root}/exchange/status",
            "GET",
            {
                "Accept": "application/json",
                "User-Agent": "betbot-kalshi-micro-execute/1.0",
            },
            None,
            timeout_seconds,
        )
        final_status = status_code
        final_payload = payload
        final_api_root = api_root
        if status_code == 200:
            break
        payload_error_type = ""
        if isinstance(payload, dict):
            payload_error_type = str(payload.get("error_type") or "").strip().lower()
        should_failover = status_code in transient_http_status_codes
        if status_code == 599 and payload_error_type in retryable_error_types:
            should_failover = True
        if should_failover and index < len(api_roots) - 1:
            continue
        break

    if final_status is None:
        return {
            "checked": True,
            "http_status": None,
            "trading_active": False,
            "exchange_active": False,
            "status_ok": False,
            "error": "exchange_status_not_attempted",
            "api_roots_attempted": attempted_roots,
        }

    trading_active = False
    exchange_active = False
    if final_status == 200 and isinstance(final_payload, dict):
        if isinstance(final_payload.get("trading_active"), bool):
            trading_active = bool(final_payload.get("trading_active"))
        if isinstance(final_payload.get("exchange_active"), bool):
            exchange_active = bool(final_payload.get("exchange_active"))
        if "trading_active" not in final_payload and "exchange_active" in final_payload:
            trading_active = exchange_active
    result = {
        "checked": True,
        "http_status": final_status,
        "trading_active": trading_active,
        "exchange_active": exchange_active,
        "status_ok": final_status == 200,
        "error": "" if final_status == 200 else f"exchange_status_http_{final_status}",
        "payload_type": type(final_payload).__name__,
        "api_root_used": final_api_root,
    }
    if attempted_roots:
        result["api_roots_attempted"] = attempted_roots
    return result


def _write_attempts_csv(path: Path, attempts: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "plan_rank",
        "category",
        "market_ticker",
        "canonical_ticker",
        "canonical_niche",
        "planned_side",
        "planned_contracts",
        "planned_entry_price_dollars",
        "market_hours_to_close",
        "market_spread_dollars",
        "queue_ahead_estimate_contracts",
        "order_size_depth_ratio",
        "quote_aggressiveness",
        "signal_confidence",
        "signal_evidence_count",
        "signal_age_seconds",
        "execution_fill_probability_10s",
        "execution_fill_probability_60s",
        "execution_fill_probability_300s",
        "execution_fill_probability_horizon",
        "execution_full_fill_probability_horizon",
        "execution_partial_fill_probability_horizon",
        "execution_expected_spread_capture_per_contract_dollars",
        "execution_expected_adverse_selection_per_contract_dollars",
        "execution_expected_partial_fill_drag_per_contract_dollars",
        "execution_expected_cancel_replace_leakage_per_contract_dollars",
        "execution_expected_retry_slippage_per_contract_dollars",
        "execution_expected_cost_stack_per_contract_dollars",
        "execution_break_even_edge_per_contract_dollars",
        "execution_forecast_edge_net_per_contract_dollars",
        "execution_expected_net_pnl_if_fill_per_contract_dollars",
        "execution_ev_submit_dollars",
        "execution_policy_active",
        "execution_policy_decision",
        "execution_policy_reason",
        "execution_frontier_bucket",
        "execution_frontier_break_even_edge_per_contract_dollars",
        "execution_frontier_bucket_markout_trusted",
        "execution_frontier_bucket_markout_trust_reason",
        "execution_frontier_bucket_markout_10s_samples",
        "execution_frontier_bucket_markout_60s_samples",
        "execution_frontier_bucket_markout_300s_samples",
        "planned_yes_bid_dollars",
        "planned_yes_ask_dollars",
        "current_best_yes_bid_dollars",
        "current_best_yes_bid_size_contracts",
        "current_best_no_bid_dollars",
        "current_best_no_bid_size_contracts",
        "current_best_same_side_bid_dollars",
        "current_best_same_side_bid_size_contracts",
        "orderbook_http_status",
        "orderbook_error_type",
        "orderbook_error",
        "live_write_allowed",
        "result",
        "duplicate_open_orders_count",
        "duplicate_open_orders_count_after_janitor",
        "matching_open_order_ids",
        "janitor_attempted",
        "janitor_cancelled_order_ids",
        "janitor_cancel_http_statuses",
        "janitor_cancel_error",
        "submission_http_status",
        "order_id",
        "order_status",
        "queue_position_http_status",
        "queue_position_contracts",
        "queue_position_error",
        "cancel_http_status",
        "cancel_reduced_by_contracts",
        "estimated_entry_cost_dollars",
        "estimated_entry_fee_dollars",
        "client_order_id",
        "api_latency_ms",
        "resting_hold_seconds",
        "order_payload_preview",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for attempt in attempts:
            serializable = dict(attempt)
            serializable["matching_open_order_ids"] = json.dumps(
                attempt.get("matching_open_order_ids", []),
                separators=(",", ":"),
            )
            serializable["janitor_cancelled_order_ids"] = json.dumps(
                attempt.get("janitor_cancelled_order_ids", []),
                separators=(",", ":"),
            )
            serializable["janitor_cancel_http_statuses"] = json.dumps(
                attempt.get("janitor_cancel_http_statuses", {}),
                separators=(",", ":"),
                sort_keys=True,
            )
            serializable["order_payload_preview"] = json.dumps(
                attempt.get("order_payload_preview", {}),
                separators=(",", ":"),
            )
            writer.writerow(serializable)


def _acquire_live_execution_lock(lock_path: Path) -> tuple[Any | None, bool, str | None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if fcntl is None:  # pragma: no cover - platform specific
        return None, False, "fcntl_unavailable"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        return None, False, str(exc)
    handle.seek(0)
    handle.truncate(0)
    handle.write(f"pid={os.getpid()} lock_path={lock_path}\n")
    handle.flush()
    return handle, True, None


def _release_live_execution_lock(handle: Any | None) -> None:
    if handle is None:
        return
    try:
        if fcntl is not None:  # pragma: no cover - platform specific
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def run_kalshi_micro_execute(
    *,
    env_file: str,
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_yes_bid_dollars: float = 0.01,
    max_yes_ask_dollars: float = 0.10,
    max_spread_dollars: float = 0.02,
    max_hours_to_close: float = 336.0,
    excluded_categories: tuple[str, ...] = ("Sports",),
    page_limit: int = 200,
    max_pages: int = 5,
    timeout_seconds: float = 15.0,
    allow_live_orders: bool = False,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    auto_cancel_duplicate_open_orders: bool = True,
    live_execution_lock_path: str | None = None,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    execution_event_log_csv: str | None = None,
    execution_journal_db_path: str | None = None,
    execution_frontier_recent_rows: int = 5000,
    history_csv: str | None = None,
    scan_csv: str | None = None,
    enforce_trade_gate: bool = False,
    enforce_ws_state_authority: bool = False,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    http_request_json: AuthenticatedRequester = _http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    plan_runner: PlanRunner = run_kalshi_micro_plan,
    quality_runner: SummaryRunner = run_kalshi_nonsports_quality,
    signal_runner: SummaryRunner = run_kalshi_nonsports_signals,
    persistence_runner: SummaryRunner = run_kalshi_nonsports_persistence,
    delta_runner: SummaryRunner = run_kalshi_nonsports_deltas,
    category_runner: CategoryRunner = run_kalshi_nonsports_categories,
    pressure_runner: PressureRunner = run_kalshi_nonsports_pressure,
    sleep_fn: TimeSleeper = time.sleep,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)
    timezone_name = str(env_data.get("BETBOT_TIMEZONE") or "America/New_York").strip()
    trading_day = trading_day_for_timestamp(captured_at, timezone_name)
    ledger_path = Path(ledger_csv) if ledger_csv else default_ledger_path(output_dir)
    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")
    effective_book_db_path = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)

    plan_summary = plan_runner(
        env_file=env_file,
        output_dir=output_dir,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_yes_bid_dollars=min_yes_bid_dollars,
        max_yes_ask_dollars=max_yes_ask_dollars,
        max_spread_dollars=max_spread_dollars,
        max_hours_to_close=max_hours_to_close,
        excluded_categories=excluded_categories,
        page_limit=page_limit,
        max_pages=max_pages,
        timeout_seconds=timeout_seconds,
        scan_csv=scan_csv,
        http_get_json=http_get_json,
        sign_request=sign_request,
        now=captured_at,
    )
    plan_orders = plan_summary.get("orders")
    if isinstance(plan_orders, list):
        record_decisions(
            book_db_path=effective_book_db_path,
            source="kalshi_micro_execute_plan",
            captured_at=captured_at,
            plans=[row for row in plan_orders if isinstance(row, dict)],
        )

    sports_excluded = any(category.strip().lower() == "sports" for category in excluded_categories)
    safety_env_enabled = str(env_data.get("BETBOT_ENABLE_LIVE_ORDERS") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    ledger_summary_before = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )
    open_positions_count_before = count_open_positions(book_db_path=effective_book_db_path)
    live_submission_budget_total = int(
        ledger_summary_before.get("live_submission_budget_total") or max_live_submissions_per_day
    )
    live_cost_budget_total = float(
        ledger_summary_before.get("live_cost_budget_total") or max_live_cost_per_day_dollars
    )
    live_submission_budget_remaining = int(ledger_summary_before.get("live_submission_budget_remaining") or 0)
    live_cost_remaining = round(float(ledger_summary_before.get("live_cost_budget_remaining") or 0.0), 4)
    live_submission_budget_remaining_before = live_submission_budget_remaining
    live_cost_remaining_before = live_cost_remaining
    lock_path = Path(live_execution_lock_path) if live_execution_lock_path else (Path(output_dir) / "betbot_live_execution.lock")
    lock_handle: Any | None = None
    live_execution_lock_acquired = True
    live_execution_lock_error: str | None = None
    if allow_live_orders:
        lock_handle, live_execution_lock_acquired, live_execution_lock_error = _acquire_live_execution_lock(lock_path)
    live_write_allowed = (
        allow_live_orders
        and safety_env_enabled
        and sports_excluded
        and live_execution_lock_acquired
        and live_submission_budget_remaining > 0
        and live_cost_remaining > 0
    )
    exchange_status = {
        "checked": False,
        "http_status": None,
        "trading_active": False,
        "exchange_active": False,
        "status_ok": False,
        "error": "",
    }
    ws_state_path = Path(ws_state_json) if ws_state_json else default_ws_state_path(output_dir)
    ws_state_authority = {
        "checked": False,
        "path": str(ws_state_path),
        "status": "disabled",
        "gate_pass": True,
        "reason": "ws_state_authority_disabled",
        "market_count": 0,
        "desynced_market_count": 0,
        "last_event_at": "",
        "last_event_age_seconds": "",
        "websocket_lag_ms": "",
        "max_staleness_seconds": max(1.0, float(ws_state_max_age_seconds)),
    }
    if enforce_ws_state_authority:
        ws_state_authority = load_ws_state_authority(
            ws_state_json=ws_state_path,
            captured_at=captured_at,
            max_staleness_seconds=ws_state_max_age_seconds,
        )
        if allow_live_orders and not bool(ws_state_authority.get("gate_pass")):
            live_write_allowed = False
    trade_gate_summary: dict[str, Any] | None = None
    if allow_live_orders and enforce_trade_gate and plan_summary.get("status") not in {"rate_limited", "upstream_error"}:
        quality_summary = quality_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        signal_summary = signal_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        persistence_summary = persistence_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        delta_summary = delta_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        category_summary = category_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        pressure_summary = pressure_runner(
            history_csv=effective_history_csv,
            output_dir=output_dir,
            now=captured_at,
        )
        trade_gate_summary = build_trade_gate_decision(
            actual_live_balance_dollars=plan_summary.get("actual_live_balance_dollars"),
            funding_gap_dollars=plan_summary.get("funding_gap_dollars"),
            planned_orders=int(plan_summary.get("planned_orders") or 0),
            meaningful_candidates=count_meaningful_candidates(plan_summary.get("orders")),
            ledger_summary=ledger_summary_before,
            max_live_submissions_per_day=max_live_submissions_per_day,
            max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
            open_positions_count=open_positions_count_before,
            quality_summary=quality_summary,
            signal_summary=signal_summary,
            persistence_summary=persistence_summary,
            delta_summary=delta_summary,
            category_summary=category_summary,
            pressure_summary=pressure_summary,
        )
        if not trade_gate_summary.get("gate_pass", False):
            live_write_allowed = False
    if (
        allow_live_orders
        and enforce_trade_gate
        and (trade_gate_summary is None or bool(trade_gate_summary.get("gate_pass", False)))
    ):
        exchange_status = _read_exchange_status(
            env_data=env_data,
            timeout_seconds=timeout_seconds,
            http_request_json=http_request_json,
        )
        if not bool(exchange_status.get("status_ok")):
            live_write_allowed = False
        elif not bool(exchange_status.get("trading_active")):
            live_write_allowed = False

    run_id = f"kalshi_micro_execute::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
    legacy_execution_event_log_path = Path(execution_event_log_csv) if execution_event_log_csv else None
    if execution_journal_db_path:
        journal_path = Path(execution_journal_db_path)
        execution_journal_legacy_alias_used = False
    elif legacy_execution_event_log_path is not None:
        # Backward compatibility: prior versions accepted a CSV path. We now use
        # SQLite and place the DB alongside the legacy path.
        if legacy_execution_event_log_path.suffix.lower() == ".csv":
            journal_path = legacy_execution_event_log_path.with_suffix(".sqlite3")
        else:
            journal_path = legacy_execution_event_log_path
        execution_journal_legacy_alias_used = True
    else:
        journal_path = default_execution_journal_db_path(output_dir)
        execution_journal_legacy_alias_used = False
    journal_events: list[dict[str, Any]] = []
    (
        execution_frontier_break_even_by_bucket,
        execution_frontier_break_even_reference_file,
        execution_frontier_bucket_trust_by_bucket,
    ) = (
        _load_latest_break_even_edges_by_bucket(output_dir)
    )

    def _append_journal_event(event_type: str, attempt: dict[str, Any], **extra: Any) -> None:
        side = str(attempt.get("planned_side") or "").strip().lower()
        best_yes_bid = _parse_float(attempt.get("current_best_yes_bid_dollars"))
        best_no_bid = _parse_float(attempt.get("current_best_no_bid_dollars"))
        spread_dollars = _parse_float(attempt.get("market_spread_dollars"))
        contracts = _parse_float(attempt.get("planned_contracts"))
        time_to_close_hours = _parse_float(attempt.get("market_hours_to_close"))
        signal_score = _parse_float(attempt.get("execution_forecast_edge_net_per_contract_dollars"))
        event_payload: dict[str, Any] = {
            "quote_aggressiveness": attempt.get("quote_aggressiveness"),
            "order_size_depth_ratio": attempt.get("order_size_depth_ratio"),
            "queue_ahead_estimate_contracts": attempt.get("queue_ahead_estimate_contracts"),
            "execution_policy_decision": attempt.get("execution_policy_decision"),
            "execution_policy_reason": attempt.get("execution_policy_reason"),
            "execution_forecast_edge_net_per_contract_dollars": attempt.get(
                "execution_forecast_edge_net_per_contract_dollars"
            ),
            "execution_break_even_edge_per_contract_dollars": attempt.get(
                "execution_break_even_edge_per_contract_dollars"
            ),
            "execution_frontier_bucket": attempt.get("execution_frontier_bucket"),
            "execution_frontier_break_even_edge_per_contract_dollars": attempt.get(
                "execution_frontier_break_even_edge_per_contract_dollars"
            ),
        }
        event_payload.update(extra.pop("payload", {}) if isinstance(extra.get("payload"), dict) else {})
        journal_event = {
            "run_id": run_id,
            "captured_at_utc": captured_at.isoformat(),
            "event_type": event_type,
            "market_ticker": str(attempt.get("market_ticker") or "").strip(),
            "event_family": str(attempt.get("canonical_niche") or attempt.get("category") or "").strip(),
            "side": side,
            "limit_price_dollars": _parse_float(attempt.get("planned_entry_price_dollars")),
            "contracts_fp": contracts,
            "client_order_id": str(attempt.get("client_order_id") or ""),
            "exchange_order_id": str(attempt.get("order_id") or ""),
            "parent_order_id": str(attempt.get("parent_order_id") or ""),
            "best_yes_bid_dollars": best_yes_bid,
            "best_yes_ask_dollars": _parse_float(attempt.get("planned_yes_ask_dollars")),
            "best_no_bid_dollars": best_no_bid,
            "best_no_ask_dollars": _parse_float(attempt.get("current_best_no_ask_dollars")),
            "spread_dollars": spread_dollars,
            "visible_depth_contracts": _parse_float(attempt.get("current_best_same_side_bid_size_contracts")),
            "queue_position_contracts": _parse_float(attempt.get("queue_position_contracts")),
            "signal_score": signal_score,
            "signal_age_seconds": _parse_float(attempt.get("signal_age_seconds")),
            "time_to_close_seconds": (
                float(time_to_close_hours) * 3600.0 if isinstance(time_to_close_hours, float) else None
            ),
            "latency_ms": _parse_float(attempt.get("latency_ms")),
            "websocket_lag_ms": _coalesce_float(
                attempt.get("websocket_lag_ms"),
                ws_state_authority.get("websocket_lag_ms"),
            ),
            "api_latency_ms": _parse_float(attempt.get("api_latency_ms")),
            "fee_dollars": _parse_float(attempt.get("estimated_entry_fee_dollars")),
            "maker_fee_dollars": _parse_float(attempt.get("estimated_entry_fee_dollars")),
            "taker_fee_dollars": None,
            "result": str(attempt.get("result") or ""),
            "status": str(attempt.get("order_status") or ""),
            "payload": event_payload,
        }
        for key, value in extra.items():
            journal_event[key] = value
        journal_events.append(journal_event)

    def _append_book_snapshot_event(
        attempt: dict[str, Any],
        *,
        snapshot_phase: str,
        snapshot: dict[str, Any] | None = None,
    ) -> None:
        ticker = str(attempt.get("market_ticker") or "").strip()
        snapshot_payload = snapshot if isinstance(snapshot, dict) else None
        snapshot_latency_ms: float | None = None
        if snapshot_payload is None and ticker:
            snapshot_started = time.monotonic()
            snapshot_payload = _fetch_orderbook_top(
                env_data=env_data,
                ticker=ticker,
                timeout_seconds=timeout_seconds,
                http_request_json=http_request_json,
                sign_request=sign_request,
            )
            snapshot_latency_ms = round((time.monotonic() - snapshot_started) * 1000.0, 3)
        snapshot_payload = snapshot_payload or {}
        _append_journal_event(
            "book_snapshot",
            attempt,
            best_yes_bid_dollars=_parse_float(snapshot_payload.get("best_yes_bid_dollars")),
            best_yes_ask_dollars=_parse_float(snapshot_payload.get("best_yes_ask_dollars")),
            best_no_bid_dollars=_parse_float(snapshot_payload.get("best_no_bid_dollars")),
            best_no_ask_dollars=_parse_float(snapshot_payload.get("best_no_ask_dollars")),
            spread_dollars=(
                _coalesce_float(
                    _parse_float(snapshot_payload.get("best_yes_ask_dollars"))
                    - _parse_float(snapshot_payload.get("best_yes_bid_dollars"))
                    if isinstance(_parse_float(snapshot_payload.get("best_yes_ask_dollars")), float)
                    and isinstance(_parse_float(snapshot_payload.get("best_yes_bid_dollars")), float)
                    else None,
                    _parse_float(attempt.get("market_spread_dollars")),
                )
            ),
            visible_depth_contracts=_coalesce_float(
                snapshot_payload.get("best_yes_bid_size_contracts")
                if str(attempt.get("planned_side") or "").strip().lower() != "no"
                else snapshot_payload.get("best_no_bid_size_contracts"),
                attempt.get("current_best_same_side_bid_size_contracts"),
            ),
            api_latency_ms=(
                snapshot_latency_ms
                if snapshot_latency_ms is not None
                else _parse_float(attempt.get("api_latency_ms"))
            ),
            payload={
                "snapshot_phase": snapshot_phase,
                "snapshot_http_status": snapshot_payload.get("http_status"),
                "snapshot_error_type": snapshot_payload.get("error_type"),
                "snapshot_error": snapshot_payload.get("error"),
            },
        )

    attempts: list[dict[str, Any]] = []
    orderbook_outage_short_circuit_triggered = False
    orderbook_outage_short_circuit_trigger_market_ticker: str | None = None
    orderbook_outage_short_circuit_skipped_orders = 0
    orders = plan_summary.get("orders")
    if isinstance(orders, list):
        eligible_orders = [plan for plan in orders if isinstance(plan, dict)]
        for plan_index, plan in enumerate(eligible_orders):
            if not isinstance(plan, dict):
                continue
            ticker = str(plan.get("market_ticker") or "")
            orderbook_started = time.monotonic()
            orderbook = _fetch_orderbook_top(
                env_data=env_data,
                ticker=ticker,
                timeout_seconds=timeout_seconds,
                http_request_json=http_request_json,
                sign_request=sign_request,
            )
            orderbook_latency_ms = round((time.monotonic() - orderbook_started) * 1000.0, 3)
            attempt: dict[str, Any] = {
                "plan_rank": plan.get("plan_rank"),
                "category": plan.get("category"),
                "market_ticker": ticker,
                "canonical_ticker": plan.get("canonical_ticker", ""),
                "canonical_niche": plan.get("canonical_niche", ""),
                "planned_side": str(plan.get("side") or plan.get("order_payload_preview", {}).get("side") or "yes"),
                "planned_contracts": (
                    plan.get("contracts_per_order")
                    if plan.get("contracts_per_order") not in (None, "")
                    else plan.get("order_payload_preview", {}).get("count", contracts_per_order)
                ),
                "planned_entry_price_dollars": (
                    plan.get("maker_entry_price_dollars")
                    if plan.get("maker_entry_price_dollars") not in (None, "")
                    else plan.get("maker_yes_price_dollars")
                ),
                "market_hours_to_close": plan.get("hours_to_close", ""),
                "market_spread_dollars": "",
                "queue_ahead_estimate_contracts": "",
                "order_size_depth_ratio": "",
                "quote_aggressiveness": "",
                "signal_confidence": plan.get("confidence", ""),
                "signal_evidence_count": plan.get("effective_min_evidence_count", ""),
                "signal_age_seconds": "",
                "execution_fill_probability_10s": "",
                "execution_fill_probability_60s": "",
                "execution_fill_probability_300s": "",
                "execution_fill_probability_horizon": "",
                "execution_full_fill_probability_horizon": "",
                "execution_partial_fill_probability_horizon": "",
                "execution_expected_spread_capture_per_contract_dollars": "",
                "execution_expected_adverse_selection_per_contract_dollars": "",
                "execution_expected_partial_fill_drag_per_contract_dollars": "",
                "execution_expected_cancel_replace_leakage_per_contract_dollars": "",
                "execution_expected_retry_slippage_per_contract_dollars": "",
                "execution_expected_cost_stack_per_contract_dollars": "",
                "execution_break_even_edge_per_contract_dollars": "",
                "execution_forecast_edge_net_per_contract_dollars": "",
                "execution_expected_net_pnl_if_fill_per_contract_dollars": "",
                "execution_ev_submit_dollars": "",
                "execution_policy_active": False,
                "execution_policy_decision": "submit",
                "execution_policy_reason": "",
                "execution_frontier_bucket": "",
                "execution_frontier_break_even_edge_per_contract_dollars": "",
                "execution_frontier_bucket_markout_trusted": "",
                "execution_frontier_bucket_markout_trust_reason": "",
                "execution_frontier_bucket_markout_10s_samples": "",
                "execution_frontier_bucket_markout_60s_samples": "",
                "execution_frontier_bucket_markout_300s_samples": "",
                "planned_yes_bid_dollars": plan.get("maker_yes_price_dollars"),
                "planned_yes_ask_dollars": plan.get("yes_ask_dollars"),
                "current_best_yes_bid_dollars": orderbook.get("best_yes_bid_dollars", ""),
                "current_best_yes_bid_size_contracts": orderbook.get("best_yes_bid_size_contracts", ""),
                "current_best_no_bid_dollars": orderbook.get("best_no_bid_dollars", ""),
                "current_best_no_bid_size_contracts": orderbook.get("best_no_bid_size_contracts", ""),
                "current_best_same_side_bid_dollars": "",
                "current_best_same_side_bid_size_contracts": "",
                "orderbook_http_status": orderbook.get("http_status"),
                "orderbook_error_type": orderbook.get("error_type", ""),
                "orderbook_error": orderbook.get("error", ""),
                "live_write_allowed": live_write_allowed,
                "result": "dry_run_ready",
                "duplicate_open_orders_count": "",
                "duplicate_open_orders_count_after_janitor": "",
                "matching_open_order_ids": [],
                "janitor_attempted": False,
                "janitor_cancelled_order_ids": [],
                "janitor_cancel_http_statuses": {},
                "janitor_cancel_error": "",
                "submission_http_status": "",
                "client_order_id": "",
                "order_id": "",
                "order_status": "",
                "queue_position_http_status": "",
                "queue_position_contracts": "",
                "queue_position_error": "",
                "cancel_http_status": "",
                "cancel_reduced_by_contracts": "",
                "estimated_entry_cost_dollars": plan.get("estimated_entry_cost_dollars", ""),
                "estimated_entry_fee_dollars": plan.get("estimated_entry_fee_dollars", ""),
                "api_latency_ms": orderbook_latency_ms,
                "resting_hold_seconds": resting_hold_seconds,
                "order_payload_preview": plan.get("order_payload_preview", {}),
            }
            if attempt["planned_side"] == "no":
                attempt["current_best_same_side_bid_dollars"] = orderbook.get("best_no_bid_dollars", "")
                attempt["current_best_same_side_bid_size_contracts"] = orderbook.get("best_no_bid_size_contracts", "")
            else:
                attempt["current_best_same_side_bid_dollars"] = orderbook.get("best_yes_bid_dollars", "")
                attempt["current_best_same_side_bid_size_contracts"] = orderbook.get("best_yes_bid_size_contracts", "")
            attempt.update(
                _execution_policy_metrics(
                    plan=plan,
                    attempt=attempt,
                    orderbook=orderbook,
                    resting_hold_seconds=resting_hold_seconds,
                )
            )
            frontier_bucket = _execution_frontier_bucket_for_attempt(attempt)
            attempt["execution_frontier_bucket"] = frontier_bucket
            bucket_trust = execution_frontier_bucket_trust_by_bucket.get(frontier_bucket, {})
            if bucket_trust:
                attempt["execution_frontier_bucket_markout_trusted"] = bool(bucket_trust.get("trusted"))
                attempt["execution_frontier_bucket_markout_trust_reason"] = str(bucket_trust.get("reason") or "").strip()
                attempt["execution_frontier_bucket_markout_10s_samples"] = int(bucket_trust.get("markout_10s_samples") or 0)
                attempt["execution_frontier_bucket_markout_60s_samples"] = int(bucket_trust.get("markout_60s_samples") or 0)
                attempt["execution_frontier_bucket_markout_300s_samples"] = int(bucket_trust.get("markout_300s_samples") or 0)
            empirical_break_even = execution_frontier_break_even_by_bucket.get(frontier_bucket)
            if isinstance(empirical_break_even, float):
                empirical_break_even = round(empirical_break_even, 6)
                attempt["execution_frontier_break_even_edge_per_contract_dollars"] = empirical_break_even
                forecast_edge = _parse_float(
                    attempt.get("execution_forecast_edge_net_per_contract_dollars")
                )
                if (
                    bool(attempt.get("execution_policy_active"))
                    and str(attempt.get("execution_policy_decision") or "").strip().lower() == "submit"
                    and isinstance(forecast_edge, float)
                    and forecast_edge < empirical_break_even
                ):
                    attempt["execution_policy_decision"] = "skip"
                    attempt["execution_policy_reason"] = "forecast_edge_below_empirical_break_even_bucket"
            if (
                allow_live_orders
                and enforce_trade_gate
                and bool(attempt.get("execution_policy_active"))
                and str(attempt.get("execution_policy_decision") or "").strip().lower() == "submit"
            ):
                if not execution_frontier_break_even_by_bucket:
                    attempt["execution_policy_decision"] = "skip"
                    attempt["execution_policy_reason"] = "execution_frontier_insufficient_data"
                elif empirical_break_even is None:
                    attempt["execution_policy_decision"] = "skip"
                    if bucket_trust and not bool(bucket_trust.get("trusted")):
                        attempt["execution_policy_reason"] = "insufficient_empirical_markout_samples_bucket"
                    else:
                        attempt["execution_policy_reason"] = "missing_empirical_break_even_bucket"
            _append_journal_event("candidate_seen", attempt)
            _append_book_snapshot_event(
                attempt,
                snapshot_phase="pre_submit",
                snapshot=orderbook,
            )

            if not live_write_allowed:
                if (
                    not allow_live_orders
                    and orderbook.get("http_status") == 200
                    and bool(attempt.get("execution_policy_active"))
                    and str(attempt.get("execution_policy_decision") or "") == "skip"
                ):
                    attempt["result"] = "blocked_execution_policy"
                if not allow_live_orders and orderbook.get("http_status") != 200:
                    # Even in dry run we fail closed on unreachable/auth-failed orderbooks so
                    # automation health checks surface real connectivity/config regressions.
                    attempt["result"] = "orderbook_unavailable"
                if allow_live_orders and not sports_excluded:
                    attempt["result"] = "blocked_sports_guardrail"
                elif allow_live_orders and not safety_env_enabled:
                    attempt["result"] = "blocked_by_safety_flag"
                elif allow_live_orders and not live_execution_lock_acquired:
                    attempt["result"] = "blocked_concurrent_live_execution"
                elif allow_live_orders and live_submission_budget_remaining <= 0:
                    attempt["result"] = "blocked_submission_budget"
                elif allow_live_orders and live_cost_remaining <= 0:
                    attempt["result"] = "blocked_live_cost_cap"
                elif allow_live_orders and enforce_ws_state_authority and not bool(ws_state_authority.get("gate_pass")):
                    ws_status = str(ws_state_authority.get("status") or "").strip().lower()
                    if ws_status == "missing":
                        attempt["result"] = "blocked_ws_state_missing"
                    elif ws_status == "upstream_error":
                        attempt["result"] = "blocked_ws_state_upstream_error"
                    elif ws_status == "stale":
                        attempt["result"] = "blocked_ws_state_stale"
                    elif ws_status == "desynced":
                        attempt["result"] = "blocked_ws_state_desynced"
                    elif ws_status == "empty":
                        attempt["result"] = "blocked_ws_state_empty"
                    elif ws_status == "invalid":
                        attempt["result"] = "blocked_ws_state_invalid"
                    else:
                        attempt["result"] = "blocked_ws_state_unhealthy"
                elif (
                    allow_live_orders
                    and enforce_trade_gate
                    and bool(exchange_status.get("checked"))
                    and not bool(exchange_status.get("status_ok"))
                ):
                    attempt["result"] = "blocked_exchange_status_unavailable"
                elif (
                    allow_live_orders
                    and enforce_trade_gate
                    and bool(exchange_status.get("checked"))
                    and not bool(exchange_status.get("trading_active"))
                ):
                    attempt["result"] = "blocked_exchange_inactive"
                elif plan_summary.get("status") in {"rate_limited", "upstream_error"}:
                    attempt["result"] = str(plan_summary.get("status"))
                elif allow_live_orders and enforce_trade_gate and trade_gate_summary is not None:
                    attempt["result"] = "blocked_trade_gate"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                if (
                    str(attempt.get("result") or "").strip().lower() == "orderbook_unavailable"
                    and _is_transient_orderbook_unavailable_attempt(attempt)
                ):
                    orderbook_outage_short_circuit_triggered = True
                    orderbook_outage_short_circuit_trigger_market_ticker = ticker or None
                    orderbook_outage_short_circuit_skipped_orders = max(0, len(eligible_orders) - plan_index - 1)
                    break
                continue

            if orderbook.get("http_status") != 200:
                attempt["result"] = "orderbook_unavailable"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                if _is_transient_orderbook_unavailable_attempt(attempt):
                    orderbook_outage_short_circuit_triggered = True
                    orderbook_outage_short_circuit_trigger_market_ticker = ticker or None
                    orderbook_outage_short_circuit_skipped_orders = max(0, len(eligible_orders) - plan_index - 1)
                    break
                continue

            if (
                bool(attempt.get("execution_policy_active"))
                and str(attempt.get("execution_policy_decision") or "") == "skip"
            ):
                attempt["result"] = "blocked_execution_policy"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                continue

            planned_entry_price = _parse_float(attempt.get("planned_entry_price_dollars"))
            planned_side = str(attempt.get("planned_side") or "").strip().lower()
            if isinstance(planned_entry_price, float) and planned_side in {"yes", "no"}:
                matching_open_orders = list_matching_open_orders(
                    book_db_path=effective_book_db_path,
                    ticker=ticker,
                    side=planned_side,
                    limit_price_dollars=planned_entry_price,
                )
                duplicate_open_orders_count = len(matching_open_orders)
                attempt["duplicate_open_orders_count"] = duplicate_open_orders_count
                attempt["matching_open_order_ids"] = [str(row.get("order_id") or "") for row in matching_open_orders]
                if duplicate_open_orders_count > 0:
                    janitor_enabled = (
                        allow_live_orders
                        and auto_cancel_duplicate_open_orders
                        and duplicate_open_orders_count > 1
                    )
                    if janitor_enabled:
                        attempt["janitor_attempted"] = True
                        canceled_ids: list[str] = []
                        not_found_ids: list[str] = []
                        failed_ids: list[str] = []
                        cancel_http_statuses: dict[str, int] = {}
                        for row in matching_open_orders:
                            order_id = str(row.get("order_id") or "").strip()
                            if not order_id:
                                continue
                            cancel_status, _ = _cancel_order(
                                env_data=env_data,
                                order_id=order_id,
                                timeout_seconds=timeout_seconds,
                                http_request_json=http_request_json,
                                sign_request=sign_request,
                            )
                            cancel_http_statuses[order_id] = int(cancel_status)
                            if cancel_status == 200:
                                canceled_ids.append(order_id)
                            elif cancel_status in {404, 410}:
                                not_found_ids.append(order_id)
                            else:
                                failed_ids.append(order_id)

                        if canceled_ids:
                            update_order_statuses(
                                book_db_path=effective_book_db_path,
                                order_ids=canceled_ids,
                                status="canceled",
                                updated_at=captured_at,
                            )
                        if not_found_ids:
                            update_order_statuses(
                                book_db_path=effective_book_db_path,
                                order_ids=not_found_ids,
                                status="closed_not_found",
                                updated_at=captured_at,
                            )

                        attempt["janitor_cancelled_order_ids"] = canceled_ids + not_found_ids
                        attempt["janitor_cancel_http_statuses"] = cancel_http_statuses
                        attempt["duplicate_open_orders_count_after_janitor"] = len(failed_ids)
                        if failed_ids:
                            attempt["janitor_cancel_error"] = f"failed_order_ids:{','.join(failed_ids)}"
                            attempt["result"] = "janitor_cancel_failed"
                            _append_journal_event(
                                "order_terminal",
                                attempt,
                                result=attempt.get("result"),
                                status=attempt.get("order_status"),
                            )
                            attempts.append(attempt)
                            continue
                    else:
                        attempt["result"] = "blocked_duplicate_open_order"
                        _append_journal_event(
                            "order_terminal",
                            attempt,
                            result=attempt.get("result"),
                            status=attempt.get("order_status"),
                        )
                        attempts.append(attempt)
                        continue

            if live_submission_budget_remaining <= 0:
                attempt["result"] = "blocked_submission_budget"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                continue

            estimated_entry_cost = _parse_float(attempt.get("estimated_entry_cost_dollars"))
            if isinstance(estimated_entry_cost, float) and live_cost_remaining + 1e-9 < estimated_entry_cost:
                attempt["result"] = "blocked_live_cost_cap"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                continue

            payload = dict(plan.get("order_payload_preview", {}))
            payload.setdefault("post_only", True)
            payload.setdefault("cancel_order_on_pause", True)
            payload.setdefault("self_trade_prevention_type", "maker")
            configured_order_group_id = str(env_data.get("BETBOT_ORDER_GROUP_ID") or "").strip()
            if configured_order_group_id and not str(payload.get("order_group_id") or "").strip():
                payload["order_group_id"] = configured_order_group_id
            plan_rank = int(plan.get("plan_rank", 0))
            safe_ticker = "".join(ch for ch in ticker if ch.isalnum())[-12:] or "market"
            payload["client_order_id"] = (
                f"betbot-{captured_at.strftime('%Y%m%d%H%M%S%f')[:-3]}-{plan_rank:02d}-{safe_ticker}"
            )
            attempt["client_order_id"] = payload["client_order_id"]
            _append_journal_event("order_submitted", attempt, status="request_sent")
            submit_started = time.monotonic()
            submission_status, submission_payload = _create_order(
                env_data=env_data,
                payload=payload,
                timeout_seconds=timeout_seconds,
                http_request_json=http_request_json,
                sign_request=sign_request,
            )
            attempt["api_latency_ms"] = round((time.monotonic() - submit_started) * 1000.0, 3)
            attempt["submission_http_status"] = submission_status
            order = submission_payload.get("order") if isinstance(submission_payload, dict) else None
            if submission_status != 201 or not isinstance(order, dict):
                attempt["result"] = "submit_failed"
                _append_journal_event(
                    "order_terminal",
                    attempt,
                    result=attempt.get("result"),
                    status=attempt.get("order_status"),
                )
                attempts.append(attempt)
                continue

            order_id = str(order.get("order_id") or "")
            order_status = str(order.get("status") or "")
            attempt["order_id"] = order_id
            attempt["order_status"] = order_status
            attempt["result"] = "submitted"
            _append_journal_event("order_acknowledged", attempt, status=order_status)
            _append_book_snapshot_event(
                attempt,
                snapshot_phase="at_ack",
                snapshot=orderbook,
            )
            live_submission_budget_remaining = max(0, live_submission_budget_remaining - 1)
            if isinstance(estimated_entry_cost, float):
                live_cost_remaining = round(max(0.0, live_cost_remaining - estimated_entry_cost), 4)

            if order_id and order_status == "resting":
                queue_snapshot = _fetch_queue_position(
                    env_data=env_data,
                    order_id=order_id,
                    timeout_seconds=timeout_seconds,
                    http_request_json=http_request_json,
                    sign_request=sign_request,
                )
                attempt["queue_position_http_status"] = queue_snapshot.get("http_status", "")
                attempt["queue_position_contracts"] = queue_snapshot.get("queue_position_contracts", "")
                attempt["queue_position_error"] = queue_snapshot.get("error", "")
                _append_journal_event("queue_snapshot", attempt, status=attempt.get("order_status"))
                _append_book_snapshot_event(
                    attempt,
                    snapshot_phase="at_queue_snapshot",
                    snapshot=orderbook,
                )

                should_cancel_after_hold = cancel_resting_immediately or resting_hold_seconds > 0
                if should_cancel_after_hold and not cancel_resting_immediately and resting_hold_seconds > 0:
                    sleep_fn(resting_hold_seconds)
                if should_cancel_after_hold:
                    _append_journal_event("cancel_requested", attempt, status=attempt.get("order_status"))
                    cancel_status, cancel_payload = _cancel_order(
                        env_data=env_data,
                        order_id=order_id,
                        timeout_seconds=timeout_seconds,
                        http_request_json=http_request_json,
                        sign_request=sign_request,
                    )
                    attempt["cancel_http_status"] = cancel_status
                    if cancel_status == 200 and isinstance(cancel_payload, dict):
                        attempt["cancel_reduced_by_contracts"] = _parse_float(cancel_payload.get("reduced_by_fp")) or ""
                        attempt["result"] = "submitted_then_canceled"
                        attempt["order_status"] = "canceled"
                        _append_journal_event("cancel_confirmed", attempt, status="canceled")
                        _append_book_snapshot_event(
                            attempt,
                            snapshot_phase="after_cancel_confirmed",
                            snapshot=orderbook,
                        )
                        live_submission_budget_remaining = min(
                            live_submission_budget_total,
                            live_submission_budget_remaining + 1,
                        )
                        if isinstance(estimated_entry_cost, float):
                            live_cost_remaining = round(
                                min(live_cost_budget_total, live_cost_remaining + estimated_entry_cost),
                                4,
                            )
                    else:
                        attempt["result"] = "cancel_failed"
                        _append_book_snapshot_event(
                            attempt,
                            snapshot_phase="after_cancel_failed",
                            snapshot=orderbook,
                        )

            _append_journal_event(
                "order_terminal",
                attempt,
                result=attempt.get("result"),
                status=attempt.get("order_status"),
            )
            attempts.append(attempt)

    run_mode = "live" if allow_live_orders else "dry_run"
    ledger_rows = ledger_rows_from_attempts(
        attempts=attempts,
        captured_at=captured_at,
        trading_day=trading_day,
        run_mode=run_mode,
        resting_hold_seconds=resting_hold_seconds,
    )
    if ledger_rows:
        append_trade_ledger(ledger_path, ledger_rows)
    ledger_summary_after = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_micro_execute_{stamp}.csv"
    _write_attempts_csv(csv_path, attempts)
    execution_journal_rows_written = append_execution_events(
        journal_db_path=journal_path,
        events=journal_events,
        default_run_id=run_id,
        default_captured_at=captured_at,
    )
    execution_frontier_summary = run_kalshi_execution_frontier(
        output_dir=output_dir,
        journal_db_path=str(journal_path),
        history_csv=effective_history_csv,
        recent_events=max(1, int(execution_frontier_recent_rows)),
        now=captured_at,
    )

    status = "dry_run"
    blocked_submission_budget_attempts = sum(1 for attempt in attempts if attempt["result"] == "blocked_submission_budget")
    blocked_live_cost_cap_attempts = sum(1 for attempt in attempts if attempt["result"] == "blocked_live_cost_cap")
    blocked_execution_policy_attempts = sum(1 for attempt in attempts if attempt["result"] == "blocked_execution_policy")
    if plan_summary.get("status") in {"rate_limited", "upstream_error"}:
        status = str(plan_summary.get("status"))
    elif plan_summary.get("status") == "no_candidates":
        status = "no_candidates"
    elif allow_live_orders and not sports_excluded:
        status = "blocked_sports_guardrail"
    elif allow_live_orders and not safety_env_enabled:
        status = "blocked_by_safety_flag"
    elif allow_live_orders and not live_execution_lock_acquired:
        status = "blocked_concurrent_live_execution"
    elif allow_live_orders and live_submission_budget_remaining_before <= 0:
        status = "blocked_submission_budget"
    elif allow_live_orders and live_cost_remaining_before <= 0:
        status = "blocked_live_cost_cap"
    elif allow_live_orders and enforce_ws_state_authority and not bool(ws_state_authority.get("gate_pass")):
        ws_status = str(ws_state_authority.get("status") or "").strip().lower()
        if ws_status == "missing":
            status = "blocked_ws_state_missing"
        elif ws_status == "upstream_error":
            status = "blocked_ws_state_upstream_error"
        elif ws_status == "stale":
            status = "blocked_ws_state_stale"
        elif ws_status == "desynced":
            status = "blocked_ws_state_desynced"
        elif ws_status == "empty":
            status = "blocked_ws_state_empty"
        elif ws_status == "invalid":
            status = "blocked_ws_state_invalid"
        else:
            status = "blocked_ws_state_unhealthy"
    elif (
        allow_live_orders
        and enforce_trade_gate
        and bool(exchange_status.get("checked"))
        and not bool(exchange_status.get("status_ok"))
    ):
        status = "blocked_exchange_status_unavailable"
    elif (
        allow_live_orders
        and enforce_trade_gate
        and bool(exchange_status.get("checked"))
        and not bool(exchange_status.get("trading_active"))
    ):
        status = "blocked_exchange_inactive"
    elif allow_live_orders and enforce_trade_gate and trade_gate_summary is not None and not trade_gate_summary.get("gate_pass", False):
        status = "blocked_trade_gate"
    elif allow_live_orders and plan_summary.get("actual_live_balance_source") != "live":
        status = "blocked_balance_unverified"
    elif allow_live_orders and plan_summary.get("funding_gap_dollars") not in (None, 0, 0.0):
        status = "needs_funding"
    elif allow_live_orders:
        duplicate_block_attempts = sum(1 for attempt in attempts if attempt["result"] == "blocked_duplicate_open_order")
        if attempts and all(attempt["result"] == "submitted_then_canceled" for attempt in attempts):
            status = "live_submitted_and_canceled"
        elif any(attempt["result"] == "submitted" for attempt in attempts):
            status = "live_submitted"
        elif any(attempt["result"] == "submit_failed" for attempt in attempts):
            status = "live_submit_failed"
        elif any(attempt["result"] == "orderbook_unavailable" for attempt in attempts):
            status = "live_orderbook_unavailable"
        elif any(attempt["result"] == "janitor_cancel_failed" for attempt in attempts):
            status = "live_janitor_cancel_failed"
        elif blocked_submission_budget_attempts > 0 and (
            blocked_submission_budget_attempts + duplicate_block_attempts == len(attempts)
        ):
            status = "blocked_submission_budget"
        elif blocked_live_cost_cap_attempts > 0 and (
            blocked_live_cost_cap_attempts + duplicate_block_attempts == len(attempts)
        ):
            status = "blocked_live_cost_cap"
        elif blocked_execution_policy_attempts > 0 and blocked_execution_policy_attempts == len(attempts):
            status = "live_blocked_execution_policy"
        elif blocked_execution_policy_attempts > 0:
            status = "live_partial_execution_policy_blocked"
        elif duplicate_block_attempts > 0 and duplicate_block_attempts == len(attempts):
            status = "live_blocked_duplicate_open_orders"
        elif duplicate_block_attempts > 0:
            status = "live_partial_duplicate_open_orders"
        else:
            status = "live_no_actions"
    elif any(attempt["result"] == "orderbook_unavailable" for attempt in attempts):
        unavailable_attempts = [attempt for attempt in attempts if attempt["result"] == "orderbook_unavailable"]
        if unavailable_attempts and all(_is_transient_orderbook_unavailable_attempt(attempt) for attempt in unavailable_attempts):
            status = "dry_run_orderbook_degraded"
        else:
            status = "upstream_error"
    elif blocked_execution_policy_attempts > 0 and blocked_execution_policy_attempts == len(attempts):
        status = "dry_run_policy_blocked"

    duplicate_open_order_attempts = [
        attempt for attempt in attempts if attempt.get("result") == "blocked_duplicate_open_order"
    ]
    duplicate_open_order_markets: list[dict[str, Any]] = []
    for attempt in duplicate_open_order_attempts:
        duplicate_open_order_markets.append(
            {
                "market_ticker": attempt.get("market_ticker"),
                "planned_side": attempt.get("planned_side"),
                "planned_entry_price_dollars": attempt.get("planned_entry_price_dollars"),
                "duplicate_open_orders_count": attempt.get("duplicate_open_orders_count"),
            }
        )

    summary = {
        "env_file": str(env_path),
        "captured_at": captured_at.isoformat(),
        "jurisdiction": (env_data.get("BETBOT_JURISDICTION") or "").strip(),
        "kalshi_env": (env_data.get("KALSHI_ENV") or "").strip().lower(),
        "allow_live_orders": allow_live_orders,
        "safety_env_enabled": safety_env_enabled,
        "sports_excluded": sports_excluded,
        "cancel_resting_immediately": cancel_resting_immediately,
        "resting_hold_seconds": resting_hold_seconds,
        "max_live_submissions_per_day": max_live_submissions_per_day,
        "max_live_cost_per_day_dollars": max_live_cost_per_day_dollars,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "live_execution_lock_path": str(lock_path),
        "live_execution_lock_acquired": live_execution_lock_acquired,
        "live_execution_lock_error": live_execution_lock_error,
        "open_positions_count_before": open_positions_count_before,
        "live_submissions_to_date_before": int(ledger_summary_before.get("live_submissions_to_date") or 0),
        "live_submissions_remaining_today_before": int(ledger_summary_before.get("live_submissions_remaining_today") or 0),
        "live_submission_days_elapsed_before": int(ledger_summary_before.get("live_submission_days_elapsed") or 0),
        "live_submission_budget_total_before": int(ledger_summary_before.get("live_submission_budget_total") or 0),
        "live_submission_budget_remaining_before": live_submission_budget_remaining_before,
        "live_cost_remaining_before": live_cost_remaining_before,
        "enforce_trade_gate": enforce_trade_gate,
        "enforce_ws_state_authority": enforce_ws_state_authority,
        "ws_state_authority": ws_state_authority,
        "exchange_status": exchange_status,
        "status": status,
        "plan_status": plan_summary.get("status"),
        "plan_events_error": plan_summary.get("events_error"),
        "plan_summary_file": plan_summary.get("output_file"),
        "plan_output_csv": plan_summary.get("output_csv"),
        "planned_orders": plan_summary.get("planned_orders"),
        "total_planned_cost_dollars": plan_summary.get("total_planned_cost_dollars"),
        "actual_live_balance_dollars": plan_summary.get("actual_live_balance_dollars"),
        "actual_live_balance_source": plan_summary.get("actual_live_balance_source"),
        "balance_live_verified": plan_summary.get("balance_live_verified"),
        "balance_check_error": plan_summary.get("balance_check_error"),
        "balance_cache_file": plan_summary.get("balance_cache_file"),
        "balance_cache_age_seconds": plan_summary.get("balance_cache_age_seconds"),
        "funding_gap_dollars": plan_summary.get("funding_gap_dollars"),
        "board_warning": plan_summary.get("board_warning"),
        "ledger_csv": str(ledger_path),
        "book_db_path": str(effective_book_db_path),
        "ledger_summary_before": ledger_summary_before,
        "ledger_summary_after": ledger_summary_after,
        "trade_gate_summary": trade_gate_summary,
        "blocked_duplicate_open_order_attempts": len(duplicate_open_order_attempts),
        "blocked_submission_budget_attempts": blocked_submission_budget_attempts,
        "blocked_live_cost_cap_attempts": blocked_live_cost_cap_attempts,
        "blocked_execution_policy_attempts": blocked_execution_policy_attempts,
        "execution_policy_active_attempts": sum(1 for attempt in attempts if bool(attempt.get("execution_policy_active"))),
        "execution_policy_submit_attempts": sum(
            1 for attempt in attempts if str(attempt.get("execution_policy_decision") or "") == "submit"
        ),
        "execution_policy_skip_attempts": sum(
            1 for attempt in attempts if str(attempt.get("execution_policy_decision") or "") == "skip"
        ),
        "janitor_attempts": sum(1 for attempt in attempts if bool(attempt.get("janitor_attempted"))),
        "janitor_canceled_open_orders_count": sum(
            len(attempt.get("janitor_cancelled_order_ids", []) or [])
            for attempt in attempts
            if isinstance(attempt, dict)
        ),
        "janitor_cancel_failed_attempts": sum(
            1 for attempt in attempts if attempt.get("result") == "janitor_cancel_failed"
        ),
        "execution_journal_db_path": str(journal_path),
        "execution_journal_run_id": run_id,
        "execution_journal_rows_written": execution_journal_rows_written,
        "execution_journal_legacy_alias_used": execution_journal_legacy_alias_used,
        # Compatibility fields for existing dashboards/tests that still use the
        # old CSV naming.
        "execution_event_log_csv": str(journal_path),
        "execution_event_rows_written": execution_journal_rows_written,
        "execution_frontier_status": execution_frontier_summary.get("status"),
        "execution_frontier_summary_file": execution_frontier_summary.get("output_file"),
        "execution_frontier_bucket_csv": execution_frontier_summary.get("bucket_csv"),
        "execution_frontier_break_even_reference_file": execution_frontier_break_even_reference_file,
        "execution_frontier_break_even_buckets_loaded": len(execution_frontier_break_even_by_bucket),
        "execution_frontier_recommendations": execution_frontier_summary.get("recommendations"),
        "orderbook_outage_short_circuit_triggered": orderbook_outage_short_circuit_triggered,
        "orderbook_outage_short_circuit_trigger_market_ticker": orderbook_outage_short_circuit_trigger_market_ticker,
        "orderbook_outage_short_circuit_skipped_orders": orderbook_outage_short_circuit_skipped_orders,
        "duplicate_open_order_markets": duplicate_open_order_markets,
        "attempts": attempts,
        "output_csv": str(csv_path),
    }
    record_order_attempts(
        book_db_path=effective_book_db_path,
        captured_at=captured_at,
        attempts=attempts,
    )

    output_path = out_dir / f"kalshi_micro_execute_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    _release_live_execution_lock(lock_handle)
    return summary
