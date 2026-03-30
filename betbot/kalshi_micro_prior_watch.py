from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_micro_prior_trader import _classify_capture_error, run_kalshi_micro_prior_trader
from betbot.kalshi_micro_status import run_kalshi_micro_status
from betbot.kalshi_micro_watch_history import default_watch_history_path
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture
from betbot.kalshi_nonsports_priors import run_kalshi_nonsports_priors


CaptureRunner = Callable[..., dict[str, Any]]
StatusRunner = Callable[..., dict[str, Any]]
PriorTraderRunner = Callable[..., dict[str, Any]]
PriorSummaryRunner = Callable[..., dict[str, Any]]


def run_kalshi_micro_prior_watch(
    *,
    env_file: str,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    output_dir: str = "outputs",
    history_csv: str | None = None,
    watch_history_csv: str | None = None,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_yes_bid_dollars: float = 0.01,
    max_yes_ask_dollars: float = 0.10,
    max_spread_dollars: float = 0.02,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    canonical_threshold_csv: str | None = "data/research/canonical_threshold_library.csv",
    prefer_canonical_thresholds: bool = True,
    require_canonical_mapping_for_live: bool = True,
    max_hours_to_close: float = 336.0,
    excluded_categories: tuple[str, ...] = ("Sports",),
    page_limit: int = 200,
    max_pages: int = 5,
    timeout_seconds: float = 15.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    auto_cancel_duplicate_open_orders: bool = True,
    min_live_maker_edge: float = 0.01,
    min_live_maker_edge_net_fees: float = 0.0,
    include_incentives: bool = False,
    auto_refresh_priors: bool = True,
    auto_prior_max_markets: int = 15,
    auto_prior_min_evidence_count: int = 2,
    auto_prior_min_evidence_quality: float = 0.55,
    auto_prior_min_high_trust_sources: int = 1,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    capture_runner: CaptureRunner = run_kalshi_nonsports_capture,
    status_runner: StatusRunner = run_kalshi_micro_status,
    prior_trader_runner: PriorTraderRunner = run_kalshi_micro_prior_trader,
    prior_summary_runner: PriorSummaryRunner = run_kalshi_nonsports_priors,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")
    watch_history_path = Path(watch_history_csv) if watch_history_csv else default_watch_history_path(output_dir)

    capture_summary = capture_runner(
        env_file=env_file,
        output_dir=output_dir,
        history_csv=effective_history_csv,
        timeout_seconds=timeout_seconds,
        excluded_categories=excluded_categories,
        max_hours_to_close=max_hours_to_close,
        page_limit=page_limit,
        max_pages=max_pages,
        now=captured_at,
    )

    effective_scan_csv = capture_summary.get("scan_output_csv") if capture_summary.get("status") == "ready" else None
    status_summary = status_runner(
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
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        ledger_csv=ledger_csv,
        watch_history_csv=str(watch_history_path),
        history_csv=effective_history_csv,
        scan_csv=effective_scan_csv,
        now=captured_at,
    )
    prior_trader_summary = prior_trader_runner(
        env_file=env_file,
        priors_csv=priors_csv,
        output_dir=output_dir,
        history_csv=effective_history_csv,
        watch_history_csv=str(watch_history_path),
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_maker_edge=min_maker_edge,
        min_maker_edge_net_fees=min_maker_edge_net_fees,
        max_entry_price_dollars=max_entry_price_dollars,
        canonical_mapping_csv=canonical_mapping_csv,
        canonical_threshold_csv=canonical_threshold_csv,
        prefer_canonical_thresholds=prefer_canonical_thresholds,
        require_canonical_mapping_for_live=require_canonical_mapping_for_live,
        timeout_seconds=timeout_seconds,
        allow_live_orders=False,
        cancel_resting_immediately=False,
        resting_hold_seconds=0.0,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        auto_cancel_duplicate_open_orders=auto_cancel_duplicate_open_orders,
        min_live_maker_edge=min_live_maker_edge,
        min_live_maker_edge_net_fees=min_live_maker_edge_net_fees,
        include_incentives=include_incentives,
        auto_refresh_priors=auto_refresh_priors,
        auto_prior_max_markets=auto_prior_max_markets,
        auto_prior_min_evidence_count=auto_prior_min_evidence_count,
        auto_prior_min_evidence_quality=auto_prior_min_evidence_quality,
        auto_prior_min_high_trust_sources=auto_prior_min_high_trust_sources,
        ledger_csv=ledger_csv,
        book_db_path=book_db_path,
        capture_before_execute=False,
        use_temporary_live_env=False,
        now=captured_at,
    )
    prior_summary = prior_summary_runner(
        priors_csv=priors_csv,
        history_csv=effective_history_csv,
        output_dir=output_dir,
        contracts_per_order=contracts_per_order,
        now=captured_at,
    )

    capture_status = capture_summary.get("status")
    capture_degraded = capture_status not in {None, "ready"}

    degraded_reason = (
        f"Fresh capture returned {capture_status}; combined prior-watch summary is using existing history."
        if capture_degraded
        else "Fresh capture, status, and prior-backed dry-run all completed."
    )
    if capture_degraded and capture_summary.get("scan_error"):
        degraded_reason = f"{degraded_reason} Capture error: {capture_summary.get('scan_error')}"

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "priors_csv": priors_csv,
        "include_incentives": include_incentives,
        "canonical_mapping_csv": canonical_mapping_csv,
        "canonical_threshold_csv": canonical_threshold_csv,
        "prefer_canonical_thresholds": prefer_canonical_thresholds,
        "require_canonical_mapping_for_live": require_canonical_mapping_for_live,
        "capture_status": capture_status,
        "capture_degraded": capture_degraded,
        "capture_scan_status": capture_summary.get("scan_status"),
        "capture_scan_error": capture_summary.get("scan_error"),
        "capture_error_kind": _classify_capture_error(capture_summary.get("scan_error")),
        "capture_summary_file": capture_summary.get("scan_summary_file"),
        "capture_scan_csv": effective_scan_csv,
        "capture_scan_page_requests": capture_summary.get("scan_page_requests"),
        "capture_scan_rate_limit_retries_used": capture_summary.get("scan_rate_limit_retries_used"),
        "capture_scan_network_retries_used": capture_summary.get("scan_network_retries_used"),
        "capture_scan_transient_http_retries_used": capture_summary.get("scan_transient_http_retries_used"),
        "capture_scan_search_retries_total": capture_summary.get("scan_search_retries_total"),
        "capture_scan_search_health_status": capture_summary.get("scan_search_health_status"),
        "capture_scan_events_fetched": capture_summary.get("scan_events_fetched"),
        "capture_scan_markets_ranked": capture_summary.get("scan_markets_ranked"),
        "history_csv": capture_summary.get("history_csv", effective_history_csv),
        "watch_history_csv": str(watch_history_path),
        "watch_runs_total": prior_trader_summary.get("watch_runs_total"),
        "watch_latest_recorded_at": prior_trader_summary.get("watch_latest_recorded_at"),
        "status_recommendation": status_summary.get("recommendation"),
        "status_trade_gate_status": status_summary.get("trade_gate_status"),
        "status_board_regime": status_summary.get("board_regime"),
        "status_board_regime_reason": status_summary.get("board_regime_reason"),
        "recommendation": status_summary.get("recommendation"),
        "board_regime": status_summary.get("board_regime"),
        "board_regime_reason": status_summary.get("board_regime_reason"),
        "status_summary_file": status_summary.get("output_file"),
        "best_entry_prior_summary_file": prior_summary.get("output_file"),
        "best_entry_positive_markets": prior_summary.get("positive_best_entry_markets"),
        "best_entry_positive_yes_ask_markets": prior_summary.get("positive_edge_yes_ask_markets"),
        "best_entry_positive_no_ask_markets": prior_summary.get("positive_edge_no_ask_markets"),
        "best_entry_top_market_ticker": prior_summary.get("top_market_ticker"),
        "best_entry_top_market_hours_to_close": prior_summary.get("top_market_hours_to_close"),
        "best_entry_top_market_side": prior_summary.get("top_market_best_entry_side"),
        "best_entry_top_market_edge": prior_summary.get("top_market_best_entry_edge"),
        "best_entry_top_market_edge_net_fees": prior_summary.get("top_market_best_entry_edge_net_fees"),
        "best_entry_top_market_maker_side": prior_summary.get("top_market_best_maker_entry_side"),
        "best_entry_top_market_maker_edge": prior_summary.get("top_market_best_maker_entry_edge"),
        "best_entry_top_market_maker_edge_net_fees": prior_summary.get("top_market_best_maker_entry_edge_net_fees"),
        "prior_trader_status": prior_trader_summary.get("status"),
        "prior_trader_error_kind": prior_trader_summary.get("prior_execute_error_kind"),
        "prior_trader_status_reason": prior_trader_summary.get("status_reason"),
        "prior_trader_failure_attempts_count": prior_trader_summary.get("prior_execute_failure_attempts_count"),
        "prior_trader_failure_retryable_attempts_count": prior_trader_summary.get(
            "prior_execute_failure_retryable_attempts_count"
        ),
        "prior_trader_failure_market_tickers": prior_trader_summary.get("prior_execute_failure_market_tickers"),
        "prior_trader_failure_result_counts": prior_trader_summary.get("prior_execute_failure_result_counts"),
        "prior_trader_failure_http_status_counts": prior_trader_summary.get(
            "prior_execute_failure_http_status_counts"
        ),
        "blocked_duplicate_open_order_attempts": prior_trader_summary.get("blocked_duplicate_open_order_attempts"),
        "blocked_submission_budget_attempts": prior_trader_summary.get("blocked_submission_budget_attempts"),
        "blocked_live_cost_cap_attempts": prior_trader_summary.get("blocked_live_cost_cap_attempts"),
        "janitor_attempts": prior_trader_summary.get("janitor_attempts"),
        "janitor_canceled_open_orders_count": prior_trader_summary.get("janitor_canceled_open_orders_count"),
        "janitor_cancel_failed_attempts": prior_trader_summary.get("janitor_cancel_failed_attempts"),
        "live_execution_lock_path": prior_trader_summary.get("live_execution_lock_path"),
        "live_execution_lock_acquired": prior_trader_summary.get("live_execution_lock_acquired"),
        "live_execution_lock_error": prior_trader_summary.get("live_execution_lock_error"),
        "duplicate_open_order_markets": prior_trader_summary.get("duplicate_open_order_markets"),
        "prior_trader_action_taken": prior_trader_summary.get("action_taken"),
        "auto_priors_status": prior_trader_summary.get("auto_priors_status"),
        "auto_priors_generated": prior_trader_summary.get("auto_priors_generated"),
        "auto_priors_inserted_rows": prior_trader_summary.get("auto_priors_inserted_rows"),
        "auto_priors_updated_rows": prior_trader_summary.get("auto_priors_updated_rows"),
        "auto_priors_manual_rows_protected": prior_trader_summary.get("auto_priors_manual_rows_protected"),
        "auto_priors_error": prior_trader_summary.get("auto_priors_error"),
        "auto_priors_summary_file": prior_trader_summary.get("auto_priors_summary_file"),
        "auto_priors_output_csv": prior_trader_summary.get("auto_priors_output_csv"),
        "auto_priors_skipped_output_csv": prior_trader_summary.get("auto_priors_skipped_output_csv"),
        "prior_trade_gate_pass": prior_trader_summary.get("prior_trade_gate_pass"),
        "prior_trade_gate_status": prior_trader_summary.get("prior_trade_gate_status"),
        "prior_trade_gate_score": prior_trader_summary.get("prior_trade_gate_score"),
        "prior_gate_result": prior_trader_summary.get("prior_trade_gate_status"),
        "watch_focus_market_mode": prior_trader_summary.get("watch_focus_market_mode"),
        "watch_focus_market_ticker": prior_trader_summary.get("watch_focus_market_ticker"),
        "watch_focus_market_streak": prior_trader_summary.get("watch_focus_market_streak"),
        "watch_focus_market_state": prior_trader_summary.get("watch_focus_market_state"),
        "watch_focus_market_state_reason": prior_trader_summary.get("watch_focus_market_state_reason"),
        "watch_recent_focus_market_changes": prior_trader_summary.get("watch_recent_focus_market_changes"),
        "watch_recommendation_streak": prior_trader_summary.get("watch_recommendation_streak"),
        "watch_trade_gate_status_streak": prior_trader_summary.get("watch_trade_gate_status_streak"),
        "ready_for_live_order": prior_trader_summary.get("ready_for_live_order"),
        "ready_for_live_order_reason": prior_trader_summary.get("ready_for_live_order_reason"),
        "ready_for_manual_live_order": prior_trader_summary.get("ready_for_live_order"),
        "ready_for_manual_live_order_reason": prior_trader_summary.get("ready_for_live_order_reason"),
        "manual_live_ready": prior_trader_summary.get("ready_for_live_order"),
        "manual_live_ready_reason": prior_trader_summary.get("ready_for_live_order_reason"),
        "ready_for_auto_live_order": prior_trader_summary.get("ready_for_auto_live_order"),
        "ready_for_auto_live_order_reason": prior_trader_summary.get("ready_for_auto_live_order_reason"),
        "auto_live_ready": prior_trader_summary.get("ready_for_auto_live_order"),
        "auto_live_ready_reason": prior_trader_summary.get("ready_for_auto_live_order_reason"),
        "top_market_ticker": prior_trader_summary.get("top_market_ticker"),
        "top_market_title": prior_trader_summary.get("top_market_title"),
        "top_market_close_time": prior_trader_summary.get("top_market_close_time"),
        "top_market_hours_to_close": prior_trader_summary.get("top_market_hours_to_close"),
        "hours_to_close": prior_trader_summary.get("top_market_hours_to_close"),
        "top_market_side": prior_trader_summary.get("top_market_side"),
        "top_market_canonical_ticker": prior_trader_summary.get("top_market_canonical_ticker"),
        "top_market_canonical_policy_applied": prior_trader_summary.get("top_market_canonical_policy_applied"),
        "top_market_maker_entry_price_dollars": prior_trader_summary.get("top_market_maker_entry_price_dollars"),
        "maker_price": prior_trader_summary.get("top_market_maker_entry_price_dollars"),
        "top_market_maker_entry_edge": prior_trader_summary.get("top_market_maker_entry_edge"),
        "top_market_maker_entry_edge_net_fees": prior_trader_summary.get("top_market_maker_entry_edge_net_fees"),
        "maker_edge": prior_trader_summary.get("top_market_maker_entry_edge"),
        "top_market_estimated_entry_cost_dollars": prior_trader_summary.get("top_market_estimated_entry_cost_dollars"),
        "estimated_entry_cost": prior_trader_summary.get("top_market_estimated_entry_cost_dollars"),
        "top_market_expected_value_dollars": prior_trader_summary.get("top_market_expected_value_dollars"),
        "expected_value": prior_trader_summary.get("top_market_expected_value_dollars"),
        "top_market_expected_roi_on_cost": prior_trader_summary.get("top_market_expected_roi_on_cost"),
        "expected_roi_on_cost": prior_trader_summary.get("top_market_expected_roi_on_cost"),
        "top_market_expected_value_per_day_dollars": prior_trader_summary.get("top_market_expected_value_per_day_dollars"),
        "expected_value_per_day": prior_trader_summary.get("top_market_expected_value_per_day_dollars"),
        "top_market_expected_roi_per_day": prior_trader_summary.get("top_market_expected_roi_per_day"),
        "expected_roi_per_day": prior_trader_summary.get("top_market_expected_roi_per_day"),
        "top_market_estimated_max_profit_dollars": prior_trader_summary.get("top_market_estimated_max_profit_dollars"),
        "expected_max_profit": prior_trader_summary.get("top_market_estimated_max_profit_dollars"),
        "top_market_estimated_max_loss_dollars": prior_trader_summary.get("top_market_estimated_max_loss_dollars"),
        "top_market_max_profit_roi_on_cost": prior_trader_summary.get("top_market_max_profit_roi_on_cost"),
        "top_market_fair_probability": prior_trader_summary.get("top_market_fair_probability"),
        "top_market_confidence": prior_trader_summary.get("top_market_confidence"),
        "top_market_thesis": prior_trader_summary.get("top_market_thesis"),
        "prior_trader_summary_file": prior_trader_summary.get("output_file"),
        "reconcile_status": prior_trader_summary.get("reconcile_status"),
        "reconcile_summary_file": prior_trader_summary.get("reconcile_summary_file"),
        "status": "degraded_ready" if capture_degraded else "ready",
        "status_reason": degraded_reason,
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_prior_watch_summary_{stamp}.json"
    summary["output_file"] = str(output_path)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
