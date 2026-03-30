from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_book import count_open_positions, default_book_db_path
from betbot.kalshi_micro_execute import (
    AuthenticatedRequester,
    TimeSleeper,
    run_kalshi_micro_execute,
    _http_request_json,
)
from betbot.kalshi_micro_ledger import default_ledger_path, summarize_trade_ledger, trading_day_for_timestamp
from betbot.kalshi_micro_prior_plan import LIVE_ALLOWED_CANONICAL_NICHES, run_kalshi_micro_prior_plan
from betbot.live_smoke import HttpGetter, KalshiSigner, _http_get_json, _kalshi_sign_request
from betbot.onboarding import _parse_env_file


def build_prior_trade_gate_decision(
    *,
    plan_summary: dict[str, Any],
    ledger_summary: dict[str, Any],
    max_live_submissions_per_day: int,
    max_live_cost_per_day_dollars: float,
    open_positions_count: int,
    min_live_maker_edge: float,
    min_live_maker_edge_net_fees: float,
    allowed_canonical_niches: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    live_submissions_today = int(ledger_summary.get("live_submissions_today") or 0)
    live_submitted_cost_today = float(ledger_summary.get("live_submitted_cost_today") or 0.0)
    live_submissions_remaining_today = int(
        ledger_summary.get("live_submissions_remaining_today") or max(0, max_live_submissions_per_day - live_submissions_today)
    )
    live_submissions_to_date = int(ledger_summary.get("live_submissions_to_date") or live_submissions_today)
    live_submission_days_elapsed = int(ledger_summary.get("live_submission_days_elapsed") or 1)
    live_submission_budget_total = int(
        ledger_summary.get("live_submission_budget_total") or (live_submission_days_elapsed * max_live_submissions_per_day)
    )
    live_submission_budget_remaining = int(
        ledger_summary.get("live_submission_budget_remaining") or live_submissions_remaining_today
    )
    live_cost_budget_total = float(
        ledger_summary.get("live_cost_budget_total") or (live_submission_days_elapsed * max_live_cost_per_day_dollars)
    )
    live_cost_budget_remaining = float(
        ledger_summary.get("live_cost_budget_remaining") or max(0.0, live_cost_budget_total - live_submitted_cost_today)
    )
    live_cost_remaining_today = float(
        ledger_summary.get("live_cost_remaining_today") or max(0.0, max_live_cost_per_day_dollars - live_submitted_cost_today)
    )

    planned_orders = int(plan_summary.get("planned_orders") or 0)
    positive_maker_entry_markets_total = int(plan_summary.get("positive_maker_entry_markets") or 0)
    positive_maker_entry_markets = int(
        plan_summary.get("positive_maker_entry_markets_with_canonical_policy") or positive_maker_entry_markets_total
    )
    top_market_side = str(plan_summary.get("top_market_side") or "")
    top_market_canonical_ticker = plan_summary.get("top_market_canonical_ticker")
    top_market_canonical_niche = str(plan_summary.get("top_market_canonical_niche") or "").strip().lower()
    top_market_canonical_policy_applied = plan_summary.get("top_market_canonical_policy_applied")
    top_market_maker_entry_edge = plan_summary.get("top_market_maker_entry_edge")
    top_market_maker_entry_edge_net_fees = plan_summary.get("top_market_maker_entry_edge_net_fees")
    funding_gap_dollars = plan_summary.get("funding_gap_dollars")
    actual_live_balance_dollars = plan_summary.get("actual_live_balance_dollars")

    blockers: list[str] = []
    if actual_live_balance_dollars is None:
        blockers.append("Live balance could not be verified.")
    elif actual_live_balance_dollars in (0, 0.0):
        blockers.append("Live balance is not funded.")
    elif isinstance(funding_gap_dollars, (int, float)) and funding_gap_dollars > 0:
        blockers.append("Planned prior-backed workflow still shows a funding gap.")
    if live_submission_budget_remaining <= 0:
        blockers.append("Accumulated live submission budget is exhausted.")
    if live_cost_budget_remaining <= 0:
        blockers.append("Accumulated live cost budget is exhausted.")
    if planned_orders <= 0:
        blockers.append("No prior-backed maker plans are available.")
    if positive_maker_entry_markets <= 0:
        blockers.append("No prior-backed maker edge is currently positive.")
    if planned_orders > 0:
        if top_market_side not in {"yes", "no"}:
            blockers.append("Top prior-backed plan does not specify a tradable side.")
        if not isinstance(top_market_maker_entry_edge, (int, float)) or top_market_maker_entry_edge < min_live_maker_edge:
            blockers.append(f"Top prior-backed maker edge is below the live minimum of {min_live_maker_edge:.3f}.")
        if (
            not isinstance(top_market_maker_entry_edge_net_fees, (int, float))
            or top_market_maker_entry_edge_net_fees < min_live_maker_edge_net_fees
        ):
            blockers.append(
                f"Top prior-backed maker edge net fees is below the live minimum of {min_live_maker_edge_net_fees:.3f}."
            )
    normalized_allowed_niches = (
        {value.strip().lower() for value in allowed_canonical_niches if value.strip()}
        if allowed_canonical_niches
        else set()
    )
    if normalized_allowed_niches and planned_orders > 0:
        if not top_market_canonical_policy_applied:
            blockers.append("Top prior-backed plan is missing canonical policy coverage.")
        elif top_market_canonical_niche not in normalized_allowed_niches:
            blockers.append("Top prior-backed plan is outside allowed live niches.")

    gate_pass = len(blockers) == 0
    gate_status = "pass" if gate_pass else "hold"
    if not gate_pass:
        if actual_live_balance_dollars is None:
            gate_status = "balance_unavailable"
        elif actual_live_balance_dollars in (0, 0.0) or (
            isinstance(funding_gap_dollars, (int, float)) and funding_gap_dollars > 0
        ):
            gate_status = "needs_funding"
        elif live_submission_budget_remaining <= 0 or live_cost_budget_remaining <= 0:
            gate_status = "cap_reached"
        elif planned_orders <= 0:
            gate_status = "no_candidates"
        elif positive_maker_entry_markets <= 0:
            gate_status = "no_edge"
        else:
            gate_status = "edge_too_small"

    gate_score = round(
        min(
            100.0,
            positive_maker_entry_markets * 30.0
            + min(float(top_market_maker_entry_edge or 0.0), 0.05) * 1000.0
            + min(live_submission_budget_remaining, max_live_submissions_per_day * 2) * 3.0,
        ),
        2,
    )
    return {
        "gate_pass": gate_pass,
        "gate_status": gate_status,
        "gate_score": gate_score,
        "gate_blockers": blockers,
        "open_positions_count": int(open_positions_count),
        "live_submissions_to_date": live_submissions_to_date,
        "live_submissions_remaining_today": live_submissions_remaining_today,
        "live_submission_days_elapsed": live_submission_days_elapsed,
        "live_submission_budget_total": live_submission_budget_total,
        "live_submission_budget_remaining": live_submission_budget_remaining,
        "live_cost_budget_total": round(live_cost_budget_total, 4),
        "live_cost_budget_remaining": round(live_cost_budget_remaining, 4),
        "live_cost_remaining_today": round(live_cost_remaining_today, 4),
        "live_cost_remaining_dollars": round(live_cost_budget_remaining, 4),
        "planned_orders": planned_orders,
        "positive_maker_entry_markets_total": positive_maker_entry_markets_total,
        "positive_maker_entry_markets": positive_maker_entry_markets,
        "top_market_ticker": plan_summary.get("top_market_ticker"),
        "top_market_title": plan_summary.get("top_market_title"),
        "top_market_close_time": plan_summary.get("top_market_close_time"),
        "top_market_hours_to_close": plan_summary.get("top_market_hours_to_close"),
        "top_market_side": top_market_side or None,
        "top_market_canonical_ticker": top_market_canonical_ticker,
        "top_market_canonical_niche": top_market_canonical_niche or None,
        "top_market_canonical_policy_applied": top_market_canonical_policy_applied,
        "allowed_canonical_niches": sorted(normalized_allowed_niches) if normalized_allowed_niches else None,
        "top_market_maker_entry_price_dollars": plan_summary.get("top_market_maker_entry_price_dollars"),
        "top_market_maker_entry_edge": top_market_maker_entry_edge,
        "top_market_maker_entry_edge_net_fees": top_market_maker_entry_edge_net_fees,
        "top_market_estimated_entry_cost_dollars": plan_summary.get("top_market_estimated_entry_cost_dollars"),
        "top_market_estimated_entry_fee_dollars": plan_summary.get("top_market_estimated_entry_fee_dollars"),
        "top_market_expected_value_dollars": plan_summary.get("top_market_expected_value_dollars"),
        "top_market_expected_value_net_dollars": plan_summary.get("top_market_expected_value_net_dollars"),
        "top_market_expected_roi_on_cost": plan_summary.get("top_market_expected_roi_on_cost"),
        "top_market_expected_roi_on_cost_net": plan_summary.get("top_market_expected_roi_on_cost_net"),
        "top_market_expected_value_per_day_dollars": plan_summary.get("top_market_expected_value_per_day_dollars"),
        "top_market_expected_value_per_day_net_dollars": plan_summary.get("top_market_expected_value_per_day_net_dollars"),
        "top_market_expected_roi_per_day": plan_summary.get("top_market_expected_roi_per_day"),
        "top_market_expected_roi_per_day_net": plan_summary.get("top_market_expected_roi_per_day_net"),
        "top_market_estimated_max_profit_dollars": plan_summary.get("top_market_estimated_max_profit_dollars"),
        "top_market_estimated_max_loss_dollars": plan_summary.get("top_market_estimated_max_loss_dollars"),
        "top_market_max_profit_roi_on_cost": plan_summary.get("top_market_max_profit_roi_on_cost"),
        "top_market_fair_probability": plan_summary.get("top_market_fair_probability"),
        "top_market_confidence": plan_summary.get("top_market_confidence"),
        "top_market_thesis": plan_summary.get("top_market_thesis"),
    }


def run_kalshi_micro_prior_execute(
    *,
    env_file: str,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    min_live_entry_price_dollars: float = 0.03,
    max_live_entry_price_dollars: float = 0.95,
    routine_live_max_hours_to_close: float = 48.0,
    max_live_hours_to_close_by_niche: dict[str, float] | None = None,
    routine_live_longdated_allowed_niches: tuple[str, ...] = (),
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    canonical_threshold_csv: str | None = "data/research/canonical_threshold_library.csv",
    prefer_canonical_thresholds: bool = True,
    require_canonical_mapping_for_live: bool = True,
    enforce_canonical_dataset: bool = True,
    timeout_seconds: float = 15.0,
    allow_live_orders: bool = False,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    auto_cancel_duplicate_open_orders: bool = True,
    min_live_maker_edge: float = 0.01,
    min_live_maker_edge_net_fees: float = 0.0,
    include_incentives: bool = False,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    execution_event_log_csv: str | None = None,
    execution_journal_db_path: str | None = None,
    execution_frontier_recent_rows: int = 5000,
    enforce_ws_state_authority: bool = False,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    http_request_json: AuthenticatedRequester = _http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    sleep_fn: TimeSleeper | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    env_data = _parse_env_file(Path(env_file))
    timezone_name = str(env_data.get("BETBOT_TIMEZONE") or "America/New_York").strip()
    trading_day = trading_day_for_timestamp(captured_at, timezone_name)
    ledger_path = Path(ledger_csv) if ledger_csv else default_ledger_path(output_dir)
    effective_book_db_path = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)
    enforce_canonical_dataset_effective = bool(enforce_canonical_dataset)
    allowed_live_canonical_niches = (
        LIVE_ALLOWED_CANONICAL_NICHES if (allow_live_orders or enforce_canonical_dataset_effective) else None
    )
    enforce_live_quality_filters = bool(allow_live_orders or enforce_canonical_dataset_effective)
    effective_min_entry_price_dollars = (
        max(0.0, float(min_live_entry_price_dollars)) if enforce_live_quality_filters else 0.0
    )
    effective_max_entry_price_dollars = (
        min(float(max_entry_price_dollars), float(max_live_entry_price_dollars))
        if enforce_live_quality_filters
        else float(max_entry_price_dollars)
    )
    effective_routine_max_hours_to_close = (
        float(routine_live_max_hours_to_close)
        if enforce_live_quality_filters and float(routine_live_max_hours_to_close) > 0
        else None
    )
    default_max_hours_by_niche = {
        "macro_release": 168.0,
        "weather_energy_transmission": 72.0,
    }
    effective_max_hours_to_close_by_niche = dict(default_max_hours_by_niche)
    if isinstance(max_live_hours_to_close_by_niche, dict):
        for key, value in max_live_hours_to_close_by_niche.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, (int, float)):
                continue
            effective_max_hours_to_close_by_niche[key.strip().lower()] = float(value)
    if not enforce_live_quality_filters:
        effective_max_hours_to_close_by_niche = {}
    effective_routine_longdated_allowed_niches = (
        tuple(routine_live_longdated_allowed_niches) if enforce_live_quality_filters else ()
    )
    prefer_canonical_thresholds_effective = (
        True if (allow_live_orders or enforce_canonical_dataset_effective) else prefer_canonical_thresholds
    )
    require_canonical_mapping_for_live_effective = (
        True if (allow_live_orders or enforce_canonical_dataset_effective) else False
    )

    plan_summary = run_kalshi_micro_prior_plan(
        env_file=env_file,
        priors_csv=priors_csv,
        history_csv=history_csv,
        output_dir=output_dir,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_maker_edge=min_maker_edge,
        min_maker_edge_net_fees=min_maker_edge_net_fees,
        min_entry_price_dollars=effective_min_entry_price_dollars,
        max_entry_price_dollars=effective_max_entry_price_dollars,
        routine_max_hours_to_close=effective_routine_max_hours_to_close,
        max_hours_to_close_by_canonical_niche=effective_max_hours_to_close_by_niche,
        routine_longdated_allowed_niches=effective_routine_longdated_allowed_niches,
        canonical_mapping_csv=canonical_mapping_csv,
        canonical_threshold_csv=canonical_threshold_csv,
        prefer_canonical_thresholds=prefer_canonical_thresholds_effective,
        require_canonical_mapping=require_canonical_mapping_for_live_effective,
        allowed_canonical_niches=allowed_live_canonical_niches,
        book_db_path=book_db_path,
        include_incentives=include_incentives,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        http_get_json=http_get_json,
        sign_request=sign_request,
        now=captured_at,
    )
    ledger_summary_before = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )
    open_positions_count = count_open_positions(book_db_path=effective_book_db_path)
    prior_trade_gate_summary = build_prior_trade_gate_decision(
        plan_summary=plan_summary,
        ledger_summary=ledger_summary_before,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        open_positions_count=open_positions_count,
        min_live_maker_edge=min_live_maker_edge,
        min_live_maker_edge_net_fees=min_live_maker_edge_net_fees,
        allowed_canonical_niches=allowed_live_canonical_niches,
    )

    def prior_plan_adapter(**kwargs: Any) -> dict[str, Any]:
        return dict(plan_summary)

    effective_allow_live_orders = allow_live_orders and bool(prior_trade_gate_summary.get("gate_pass"))
    execute_kwargs: dict[str, Any] = {
        "env_file": env_file,
        "output_dir": output_dir,
        "planning_bankroll_dollars": planning_bankroll_dollars,
        "daily_risk_cap_dollars": daily_risk_cap_dollars,
        "contracts_per_order": contracts_per_order,
        "max_orders": max_orders,
        "allow_live_orders": effective_allow_live_orders,
        "cancel_resting_immediately": cancel_resting_immediately,
        "resting_hold_seconds": resting_hold_seconds,
        "max_live_submissions_per_day": max_live_submissions_per_day,
        "max_live_cost_per_day_dollars": max_live_cost_per_day_dollars,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "ledger_csv": str(ledger_path),
        "book_db_path": str(effective_book_db_path),
        "execution_event_log_csv": execution_event_log_csv,
        "execution_journal_db_path": execution_journal_db_path,
        "execution_frontier_recent_rows": execution_frontier_recent_rows,
        "enforce_ws_state_authority": enforce_ws_state_authority,
        "ws_state_json": ws_state_json,
        "ws_state_max_age_seconds": ws_state_max_age_seconds,
        "history_csv": history_csv,
        "timeout_seconds": timeout_seconds,
        "http_request_json": http_request_json,
        "http_get_json": http_get_json,
        "sign_request": sign_request,
        "plan_runner": prior_plan_adapter,
        "now": captured_at,
    }
    if sleep_fn is not None:
        execute_kwargs["sleep_fn"] = sleep_fn
    execute_summary = run_kalshi_micro_execute(**execute_kwargs)

    summary_status = execute_summary.get("status")
    if allow_live_orders and not prior_trade_gate_summary.get("gate_pass", False):
        summary_status = "blocked_prior_trade_gate"

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "allow_live_orders_requested": allow_live_orders,
        "allow_live_orders_effective": effective_allow_live_orders,
        "enforce_live_quality_filters": enforce_live_quality_filters,
        "effective_min_entry_price_dollars": effective_min_entry_price_dollars,
        "effective_max_entry_price_dollars": effective_max_entry_price_dollars,
        "effective_routine_max_hours_to_close": effective_routine_max_hours_to_close,
        "effective_max_hours_to_close_by_niche": (
            dict(effective_max_hours_to_close_by_niche) if effective_max_hours_to_close_by_niche else None
        ),
        "effective_routine_longdated_allowed_niches": (
            list(effective_routine_longdated_allowed_niches)
            if effective_routine_longdated_allowed_niches
            else None
        ),
        "canonical_mapping_csv": canonical_mapping_csv,
        "canonical_threshold_csv": canonical_threshold_csv,
        "prefer_canonical_thresholds": prefer_canonical_thresholds,
        "prefer_canonical_thresholds_effective": prefer_canonical_thresholds_effective,
        "require_canonical_mapping_for_live": require_canonical_mapping_for_live,
        "require_canonical_mapping_for_live_effective": require_canonical_mapping_for_live_effective,
        "enforce_canonical_dataset": enforce_canonical_dataset,
        "enforce_canonical_dataset_effective": enforce_canonical_dataset_effective,
        "allowed_live_canonical_niches": list(allowed_live_canonical_niches) if allowed_live_canonical_niches else None,
        "plan_canonical_policy_enabled": plan_summary.get("canonical_policy_enabled"),
        "plan_canonical_policy_reason": plan_summary.get("canonical_policy_reason"),
        "plan_matched_live_markets_with_canonical_policy": plan_summary.get(
            "matched_live_markets_with_canonical_policy"
        ),
        "plan_top_market_canonical_ticker": plan_summary.get("top_market_canonical_ticker"),
        "plan_top_market_canonical_policy_applied": plan_summary.get("top_market_canonical_policy_applied"),
        "include_incentives": include_incentives,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "prior_trade_gate_summary": prior_trade_gate_summary,
        "plan_summary_file": plan_summary.get("output_file"),
        "plan_output_csv": plan_summary.get("output_csv"),
        "execute_summary_file": execute_summary.get("output_file"),
        "execute_output_csv": execute_summary.get("output_csv"),
        "planned_orders": execute_summary.get("planned_orders"),
        "total_planned_cost_dollars": execute_summary.get("total_planned_cost_dollars"),
        "live_execution_lock_path": execute_summary.get("live_execution_lock_path"),
        "live_execution_lock_acquired": execute_summary.get("live_execution_lock_acquired"),
        "live_execution_lock_error": execute_summary.get("live_execution_lock_error"),
        "actual_live_balance_dollars": execute_summary.get("actual_live_balance_dollars"),
        "actual_live_balance_source": execute_summary.get("actual_live_balance_source"),
        "balance_live_verified": execute_summary.get("balance_live_verified"),
        "funding_gap_dollars": execute_summary.get("funding_gap_dollars"),
        "open_positions_count": open_positions_count,
        "live_submissions_to_date": prior_trade_gate_summary.get("live_submissions_to_date"),
        "live_submissions_remaining_today": prior_trade_gate_summary.get("live_submissions_remaining_today"),
        "live_submission_days_elapsed": prior_trade_gate_summary.get("live_submission_days_elapsed"),
        "live_submission_budget_total": prior_trade_gate_summary.get("live_submission_budget_total"),
        "live_submission_budget_remaining": prior_trade_gate_summary.get("live_submission_budget_remaining"),
        "top_market_ticker": prior_trade_gate_summary.get("top_market_ticker"),
        "top_market_title": prior_trade_gate_summary.get("top_market_title"),
        "top_market_close_time": prior_trade_gate_summary.get("top_market_close_time"),
        "top_market_hours_to_close": prior_trade_gate_summary.get("top_market_hours_to_close"),
        "top_market_side": prior_trade_gate_summary.get("top_market_side"),
        "top_market_canonical_ticker": prior_trade_gate_summary.get("top_market_canonical_ticker"),
        "top_market_canonical_niche": prior_trade_gate_summary.get("top_market_canonical_niche"),
        "top_market_canonical_policy_applied": prior_trade_gate_summary.get("top_market_canonical_policy_applied"),
        "top_market_maker_entry_price_dollars": prior_trade_gate_summary.get("top_market_maker_entry_price_dollars"),
        "top_market_maker_entry_edge": prior_trade_gate_summary.get("top_market_maker_entry_edge"),
        "top_market_maker_entry_edge_net_fees": prior_trade_gate_summary.get("top_market_maker_entry_edge_net_fees"),
        "top_market_estimated_entry_cost_dollars": prior_trade_gate_summary.get("top_market_estimated_entry_cost_dollars"),
        "top_market_estimated_entry_fee_dollars": prior_trade_gate_summary.get("top_market_estimated_entry_fee_dollars"),
        "top_market_expected_value_dollars": prior_trade_gate_summary.get("top_market_expected_value_dollars"),
        "top_market_expected_value_net_dollars": prior_trade_gate_summary.get("top_market_expected_value_net_dollars"),
        "top_market_expected_roi_on_cost": prior_trade_gate_summary.get("top_market_expected_roi_on_cost"),
        "top_market_expected_roi_on_cost_net": prior_trade_gate_summary.get("top_market_expected_roi_on_cost_net"),
        "top_market_expected_value_per_day_dollars": prior_trade_gate_summary.get("top_market_expected_value_per_day_dollars"),
        "top_market_expected_value_per_day_net_dollars": prior_trade_gate_summary.get("top_market_expected_value_per_day_net_dollars"),
        "top_market_expected_roi_per_day": prior_trade_gate_summary.get("top_market_expected_roi_per_day"),
        "top_market_expected_roi_per_day_net": prior_trade_gate_summary.get("top_market_expected_roi_per_day_net"),
        "top_market_estimated_max_profit_dollars": prior_trade_gate_summary.get("top_market_estimated_max_profit_dollars"),
        "top_market_estimated_max_loss_dollars": prior_trade_gate_summary.get("top_market_estimated_max_loss_dollars"),
        "top_market_max_profit_roi_on_cost": prior_trade_gate_summary.get("top_market_max_profit_roi_on_cost"),
        "top_market_fair_probability": prior_trade_gate_summary.get("top_market_fair_probability"),
        "top_market_confidence": prior_trade_gate_summary.get("top_market_confidence"),
        "top_market_thesis": prior_trade_gate_summary.get("top_market_thesis"),
        "status": summary_status,
        "blocked_duplicate_open_order_attempts": execute_summary.get("blocked_duplicate_open_order_attempts"),
        "blocked_submission_budget_attempts": execute_summary.get("blocked_submission_budget_attempts"),
        "blocked_live_cost_cap_attempts": execute_summary.get("blocked_live_cost_cap_attempts"),
        "janitor_attempts": execute_summary.get("janitor_attempts"),
        "janitor_canceled_open_orders_count": execute_summary.get("janitor_canceled_open_orders_count"),
        "janitor_cancel_failed_attempts": execute_summary.get("janitor_cancel_failed_attempts"),
        "blocked_execution_policy_attempts": execute_summary.get("blocked_execution_policy_attempts"),
        "execution_policy_active_attempts": execute_summary.get("execution_policy_active_attempts"),
        "execution_policy_submit_attempts": execute_summary.get("execution_policy_submit_attempts"),
        "execution_policy_skip_attempts": execute_summary.get("execution_policy_skip_attempts"),
        "execution_event_log_csv": execute_summary.get("execution_event_log_csv"),
        "execution_event_rows_written": execute_summary.get("execution_event_rows_written"),
        "execution_journal_db_path": execute_summary.get("execution_journal_db_path"),
        "execution_journal_run_id": execute_summary.get("execution_journal_run_id"),
        "execution_journal_rows_written": execute_summary.get("execution_journal_rows_written"),
        "enforce_ws_state_authority": execute_summary.get("enforce_ws_state_authority"),
        "ws_state_authority": execute_summary.get("ws_state_authority"),
        "execution_frontier_status": execute_summary.get("execution_frontier_status"),
        "execution_frontier_summary_file": execute_summary.get("execution_frontier_summary_file"),
        "execution_frontier_bucket_csv": execute_summary.get("execution_frontier_bucket_csv"),
        "execution_frontier_recommendations": execute_summary.get("execution_frontier_recommendations"),
        "orderbook_outage_short_circuit_triggered": execute_summary.get("orderbook_outage_short_circuit_triggered"),
        "orderbook_outage_short_circuit_trigger_market_ticker": execute_summary.get(
            "orderbook_outage_short_circuit_trigger_market_ticker"
        ),
        "orderbook_outage_short_circuit_skipped_orders": execute_summary.get(
            "orderbook_outage_short_circuit_skipped_orders"
        ),
        "duplicate_open_order_markets": execute_summary.get("duplicate_open_order_markets"),
        "attempts": execute_summary.get("attempts"),
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_prior_execute_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
