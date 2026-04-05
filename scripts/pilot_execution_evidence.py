#!/usr/bin/env python3
"""Summarize pilot execution evidence from local outputs artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any) -> float | None:
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


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _latest_file(outputs_dir: Path, pattern: str) -> Path | None:
    files = [item for item in outputs_dir.glob(pattern) if item.is_file()]
    if not files:
        return None
    return max(files, key=lambda item: item.stat().st_mtime)


def _top_reason(reason_counts: dict[str, Any]) -> str:
    best_reason = ""
    best_count = -1
    for key, raw in reason_counts.items():
        count = _as_int(raw, 0)
        if count > best_count:
            best_count = count
            best_reason = _as_str(key)
    return best_reason


def _recommend_action(
    *,
    mode: str,
    attempted_orders: int,
    filled_orders: int,
    would_attempt_live_if_enabled: int,
    frontier_status: str,
    top_blocker_reason: str,
) -> str:
    mode_lower = mode.lower()
    frontier_lower = frontier_status.lower()
    if filled_orders > 0:
        return "collect_markout_and_roll_forward"
    if attempted_orders > 0 and filled_orders == 0:
        return "collect_no_fill_diagnostics_and_retry_single_shot"
    if "dry_run" in mode_lower and would_attempt_live_if_enabled > 0:
        return "enable_single_shot_live_for_1x1_evidence"
    if "insufficient_data" in frontier_lower:
        return "collect_more_frontier_samples_before_scaling"
    if top_blocker_reason:
        return f"resolve_blocker_{top_blocker_reason}"
    return "no_actionable_attempt_evidence"


def build_summary(outputs_dir: Path) -> dict[str, Any]:
    overnight_path = outputs_dir / "overnight_alpha_latest.json"
    overnight = _load_json(overnight_path) if overnight_path.exists() else {}

    micro_summary_path = _latest_file(outputs_dir, "kalshi_micro_execute_summary_*.json")
    micro_summary = _load_json(micro_summary_path) if micro_summary_path else {}

    frontier_report_path = _latest_file(outputs_dir, "execution_frontier_report_*.json")
    frontier_report = _load_json(frontier_report_path) if frontier_report_path else {}

    mode = _as_str(
        micro_summary.get("mode")
        or overnight.get("mode"),
        "unknown",
    )
    live_ready = bool(overnight.get("live_ready"))
    live_blockers = overnight.get("live_blockers")
    if not isinstance(live_blockers, list):
        live_blockers = []

    frontier = overnight.get("execution_frontier")
    frontier_status = ""
    if isinstance(frontier, dict):
        frontier_status = _as_str(frontier.get("status"))
    if not frontier_status:
        frontier_status = _as_str(micro_summary.get("execution_frontier_status")) or _as_str(frontier_report.get("status"))
    if not frontier_status:
        frontier_status = "unknown"

    considered_rows = _as_int(
        micro_summary.get("climate_router_pilot_considered_rows", overnight.get("climate_router_pilot_considered_rows")),
        0,
    )
    promoted_rows = _as_int(
        micro_summary.get("climate_router_pilot_promoted_rows", overnight.get("climate_router_pilot_promoted_rows")),
        0,
    )
    execute_considered_rows = _as_int(
        micro_summary.get("climate_router_pilot_execute_considered_rows", overnight.get("climate_router_pilot_execute_considered_rows")),
        0,
    )
    would_attempt_live_if_enabled = _as_int(
        micro_summary.get(
            "climate_router_pilot_would_attempt_live_if_enabled",
            overnight.get("climate_router_pilot_would_attempt_live_if_enabled"),
        ),
        0,
    )
    attempted_orders = _as_int(
        micro_summary.get("climate_router_pilot_attempted_orders", overnight.get("climate_router_pilot_attempted_orders")),
        0,
    )
    filled_orders = _as_int(
        micro_summary.get("climate_router_pilot_filled_orders", overnight.get("climate_router_pilot_filled_orders")),
        0,
    )

    reason_counts = micro_summary.get("climate_router_pilot_blocked_research_dry_run_only_reason_counts")
    if not isinstance(reason_counts, dict):
        reason_counts = overnight.get("climate_router_pilot_blocked_research_dry_run_only_reason_counts")
    if not isinstance(reason_counts, dict):
        reason_counts = {}
    dominant_blocker_reason = _top_reason(reason_counts)

    selected_ticker = ""
    selected_tickers = micro_summary.get("climate_router_pilot_selected_tickers")
    if not isinstance(selected_tickers, list):
        selected_tickers = overnight.get("climate_router_pilot_selected_tickers")
    if isinstance(selected_tickers, list):
        for raw in selected_tickers:
            ticker = _as_str(raw).strip()
            if ticker:
                selected_ticker = ticker
                break

    attempts = micro_summary.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
    first_attempt = attempts[0] if attempts and isinstance(attempts[0], dict) else {}
    if first_attempt and not selected_ticker:
        selected_ticker = _as_str(first_attempt.get("market_ticker")).strip()

    if filled_orders > 0:
        evidence_status = "filled"
    elif attempted_orders > 0:
        evidence_status = "attempted_no_fill"
    elif would_attempt_live_if_enabled > 0:
        evidence_status = "blocked_before_submit"
    else:
        evidence_status = "no_attempt_signal"

    summary: dict[str, Any] = {
        "generated_at_utc": _utc_now_iso(),
        "source_files": {
            "overnight_alpha_latest_json": str(overnight_path) if overnight_path.exists() else None,
            "latest_micro_execute_summary_json": str(micro_summary_path) if micro_summary_path else None,
            "latest_execution_frontier_report_json": str(frontier_report_path) if frontier_report_path else None,
        },
        "core_state": {
            "mode": mode,
            "live_ready": live_ready,
            "live_blockers": live_blockers,
            "frontier_status": frontier_status,
        },
        "pilot_funnel": {
            "considered_rows": considered_rows,
            "promoted_rows": promoted_rows,
            "execute_considered_rows": execute_considered_rows,
            "would_attempt_live_if_enabled": would_attempt_live_if_enabled,
            "attempted_orders": attempted_orders,
            "filled_orders": filled_orders,
            "selected_ticker": selected_ticker or None,
            "dominant_dry_run_blocker_reason": dominant_blocker_reason or None,
        },
        "first_attempt_evidence": {
            "status": evidence_status,
            "attempt_snapshot": {
                "market_ticker": _as_str(first_attempt.get("market_ticker")) or None,
                "execution_policy_reason": _as_str(first_attempt.get("execution_policy_reason")) or None,
                "order_status": _as_str(first_attempt.get("order_status")) or None,
                "order_id": _as_str(first_attempt.get("order_id")) or None,
                "frontier_bucket": _as_str(first_attempt.get("execution_frontier_bucket")) or None,
                "frontier_trusted": first_attempt.get("execution_frontier_bucket_markout_trusted"),
                "live_write_allowed": first_attempt.get("live_write_allowed"),
            },
        },
        "pass_fail": {
            "has_selected_ticker": bool(selected_ticker),
            "has_attempt": attempted_orders > 0,
            "has_fill": filled_orders > 0,
            "frontier_not_insufficient_data": frontier_status != "insufficient_data",
        },
        "recommended_next_action": _recommend_action(
            mode=mode,
            attempted_orders=attempted_orders,
            filled_orders=filled_orders,
            would_attempt_live_if_enabled=would_attempt_live_if_enabled,
            frontier_status=frontier_status,
            top_blocker_reason=dominant_blocker_reason,
        ),
    }

    selected_family = _as_str(
        first_attempt.get("contract_family")
        or overnight.get("pilot_execution_selected_family")
        or "",
    ).strip()
    sizing_basis = _as_str(overnight.get("sizing_basis")).strip() or None
    if sizing_basis is None:
        shadow_start_dollars = _as_float(overnight.get("shadow_bankroll_start_dollars"))
        if shadow_start_dollars is not None:
            start_text = f"{max(0.0, shadow_start_dollars):.4f}".rstrip("0").rstrip(".") or "0"
            sizing_basis = f"shadow_{start_text}"
    execution_basis = _as_str(overnight.get("execution_basis")).strip() or "live_actual_balance"
    paper_live_execution_basis = _as_str(overnight.get("paper_live_execution_basis")).strip() or "paper_live_balance"
    paper_live_balance_start_dollars = _as_float(overnight.get("paper_live_balance_start_dollars"))
    paper_live_balance_current_dollars = _as_float(overnight.get("paper_live_balance_current_dollars"))
    paper_live_order_attempts = _as_int(
        overnight.get("paper_live_order_attempts", overnight.get("paper_live_attempted_orders")),
        0,
    )
    paper_live_orders_filled = _as_int(
        overnight.get("paper_live_orders_filled", overnight.get("paper_live_filled_orders")),
        0,
    )
    paper_live_orders_partial_filled = _as_int(overnight.get("paper_live_orders_partial_filled"), 0)
    paper_live_orders_canceled = _as_int(overnight.get("paper_live_orders_canceled"), 0)
    paper_live_orders_expired = _as_int(overnight.get("paper_live_orders_expired"), 0)
    paper_live_fill_time_seconds = _as_float(overnight.get("paper_live_fill_time_seconds"))
    paper_live_markout_10s_dollars = _as_float(overnight.get("paper_live_markout_10s_dollars"))
    paper_live_markout_60s_dollars = _as_float(overnight.get("paper_live_markout_60s_dollars"))
    paper_live_markout_300s_dollars = _as_float(overnight.get("paper_live_markout_300s_dollars"))
    paper_live_settlement_pnl_dollars = _as_float(overnight.get("paper_live_settlement_pnl_dollars"))
    paper_live_expected_value_dollars = _as_float(overnight.get("paper_live_expected_value_dollars"))
    paper_live_expected_vs_realized_delta = _as_float(overnight.get("paper_live_expected_vs_realized_delta"))
    blocked_reason_counts = micro_summary.get("climate_router_pilot_blocked_post_promotion_reason_counts")
    if not isinstance(blocked_reason_counts, dict):
        blocked_reason_counts = overnight.get("pilot_execution_blocked_reason_counts")
    if not isinstance(blocked_reason_counts, dict):
        blocked_reason_counts = {}

    # Compatibility top-level fields used by overnight summaries and operator checks.
    summary.update(
        {
            "pilot_execution_evidence_status": evidence_status,
            "pilot_execution_selected_ticker": selected_ticker or None,
            "pilot_execution_selected_family": selected_family or None,
            "pilot_execution_would_attempt_live_if_enabled": would_attempt_live_if_enabled,
            "pilot_execution_attempted_orders": attempted_orders,
            "pilot_execution_filled_orders": filled_orders,
            "pilot_execution_frontier_status": frontier_status,
            "pilot_execution_recommended_next_action": summary.get("recommended_next_action"),
            "pilot_execution_blocked_reason_counts": blocked_reason_counts,
            "sizing_basis": sizing_basis,
            "execution_basis": execution_basis,
            "paper_live_execution_basis": paper_live_execution_basis,
            "paper_live_balance_start_dollars": paper_live_balance_start_dollars,
            "paper_live_balance_current_dollars": paper_live_balance_current_dollars,
            "paper_live_order_attempts": paper_live_order_attempts,
            "paper_live_orders_filled": paper_live_orders_filled,
            "paper_live_attempted_orders": paper_live_order_attempts,
            "paper_live_filled_orders": paper_live_orders_filled,
            "paper_live_orders_partial_filled": paper_live_orders_partial_filled,
            "paper_live_orders_canceled": paper_live_orders_canceled,
            "paper_live_orders_expired": paper_live_orders_expired,
            "paper_live_fill_time_seconds": paper_live_fill_time_seconds,
            "paper_live_markout_10s_dollars": paper_live_markout_10s_dollars,
            "paper_live_markout_10s": paper_live_markout_10s_dollars,
            "paper_live_markout_60s_dollars": paper_live_markout_60s_dollars,
            "paper_live_markout_60s": paper_live_markout_60s_dollars,
            "paper_live_markout_300s_dollars": paper_live_markout_300s_dollars,
            "paper_live_markout_300s": paper_live_markout_300s_dollars,
            "paper_live_settlement_pnl_dollars": paper_live_settlement_pnl_dollars,
            "paper_live_expected_value_dollars": paper_live_expected_value_dollars,
            "paper_live_expected_vs_realized_delta": paper_live_expected_vs_realized_delta,
        }
    )
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize pilot execution evidence from outputs artifacts.")
    parser.add_argument(
        "--outputs-dir",
        default="/Users/dankerbadge/Documents/Betting Bot/outputs",
        help="Path to outputs directory (default: %(default)s)",
    )
    parser.add_argument(
        "--output-json",
        default="/Users/dankerbadge/Documents/Betting Bot/outputs/pilot_execution_evidence_latest.json",
        help="Path to write summary JSON (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    outputs_dir = Path(args.outputs_dir).expanduser().resolve()
    output_json = Path(args.output_json).expanduser().resolve()

    summary = build_summary(outputs_dir)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
