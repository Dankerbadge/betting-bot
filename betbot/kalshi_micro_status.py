from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable

from betbot.kalshi_book import count_open_positions, default_book_db_path
from betbot.kalshi_focus_dossier import run_kalshi_focus_dossier
from betbot.kalshi_micro_gate import build_trade_gate_decision, count_meaningful_candidates
from betbot.kalshi_micro_ledger import default_ledger_path, summarize_trade_ledger, trading_day_for_timestamp
from betbot.kalshi_micro_watch_history import (
    append_watch_history,
    default_watch_history_path,
    summarize_watch_history,
)
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_nonsports_categories import run_kalshi_nonsports_categories
from betbot.kalshi_nonsports_deltas import run_kalshi_nonsports_deltas
from betbot.kalshi_nonsports_persistence import run_kalshi_nonsports_persistence
from betbot.kalshi_nonsports_pressure import run_kalshi_nonsports_pressure
from betbot.kalshi_nonsports_priors import run_kalshi_nonsports_priors
from betbot.kalshi_nonsports_quality import run_kalshi_nonsports_quality
from betbot.kalshi_nonsports_signals import run_kalshi_nonsports_signals
from betbot.kalshi_nonsports_thresholds import run_kalshi_nonsports_thresholds
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile


ExecuteRunner = Callable[..., dict[str, Any]]
ReconcileRunner = Callable[..., dict[str, Any]]
QualityRunner = Callable[..., dict[str, Any]]
SignalRunner = Callable[..., dict[str, Any]]
PersistenceRunner = Callable[..., dict[str, Any]]
DeltaRunner = Callable[..., dict[str, Any]]
CategoryRunner = Callable[..., dict[str, Any]]
PressureRunner = Callable[..., dict[str, Any]]
ThresholdRunner = Callable[..., dict[str, Any]]
PriorRunner = Callable[..., dict[str, Any]]


def _find_recent_scan_csv(
    *,
    output_dir: str,
    captured_at: datetime,
    max_age_seconds: float,
) -> str | None:
    out_dir = Path(output_dir)
    candidates = sorted(
        out_dir.glob("kalshi_nonsports_scan_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    captured_ts = captured_at.timestamp()
    for path in candidates:
        age_seconds = max(0.0, captured_ts - path.stat().st_mtime)
        if age_seconds <= max_age_seconds:
            return str(path)
    return None


def _open_order_state_snapshot(*, book_db_path: Path) -> dict[str, Any]:
    snapshot = {
        "positions_with_nonzero_exposure": 0,
        "positions_with_resting_orders_hint": 0,
        "resting_orders_in_orders_table": 0,
        "consistency": "unknown",
        "warning": "",
    }
    if not book_db_path.exists():
        snapshot["warning"] = "portfolio_book_missing"
        return snapshot
    try:
        with sqlite3.connect(book_db_path) as conn:
            positions_with_nonzero_exposure = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM positions
                    WHERE ABS(COALESCE(position_fp, 0.0)) > 0.000001
                    """
                ).fetchone()[0]
            )
            positions_with_resting_orders_hint = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM positions
                    WHERE COALESCE(resting_orders_count, 0) > 0
                    """
                ).fetchone()[0]
            )
            resting_orders_in_orders_table = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM orders
                    WHERE LOWER(COALESCE(status, '')) IN ('resting', 'open', 'pending')
                    """
                ).fetchone()[0]
            )
    except sqlite3.Error:
        snapshot["warning"] = "portfolio_book_unreadable"
        return snapshot

    snapshot.update(
        {
            "positions_with_nonzero_exposure": positions_with_nonzero_exposure,
            "positions_with_resting_orders_hint": positions_with_resting_orders_hint,
            "resting_orders_in_orders_table": resting_orders_in_orders_table,
            "consistency": "consistent",
            "warning": "",
        }
    )
    if positions_with_resting_orders_hint > 0 and resting_orders_in_orders_table <= 0:
        snapshot["consistency"] = "inconsistent"
        snapshot["warning"] = (
            "positions table indicates resting orders but orders table has zero open/resting orders"
        )
    return snapshot


def _recommendation(
    *,
    execute_summary: dict[str, Any],
    reconcile_summary: dict[str, Any],
    quality_summary: dict[str, Any],
    signal_summary: dict[str, Any],
    persistence_summary: dict[str, Any],
    delta_summary: dict[str, Any],
    category_summary: dict[str, Any],
    pressure_summary: dict[str, Any],
    threshold_summary: dict[str, Any],
    prior_summary: dict[str, Any],
    gate_summary: dict[str, Any],
) -> str:
    if execute_summary.get("status") == "rate_limited":
        return "wait_for_rate_limit_reset"
    if execute_summary.get("status") == "upstream_error":
        return "check_upstream_error"
    if execute_summary.get("actual_live_balance_dollars") is None:
        return "check_balance_connection"
    if execute_summary.get("actual_live_balance_dollars") in (0, 0.0):
        return "fund_account"
    if reconcile_summary.get("total_market_exposure_dollars") not in (None, 0, 0.0):
        return "monitor_live_exposure"
    if gate_summary.get("gate_pass"):
        return "review_trade_gate_pass"
    if persistence_summary.get("persistent_tradeable_markets", 0) > 0:
        return "review_persistent_tradeable_markets"
    if signal_summary.get("eligible_markets", 0) > 0:
        return "review_signal_candidates"
    if (
        prior_summary.get("positive_edge_yes_ask_markets", 0) > 0
        or prior_summary.get("positive_edge_no_ask_markets", 0) > 0
    ):
        return "review_prior_edge"
    if threshold_summary.get("approaching_markets", 0) > 0:
        return "review_threshold_approach"
    if pressure_summary.get("build_markets", 0) > 0:
        return "review_pressure_build"
    if category_summary.get("tradeable_categories", 0) > 0:
        return "review_tradeable_categories"
    if quality_summary.get("meaningful_markets", 0) > 0:
        return "review_meaningful_markets"
    if delta_summary.get("board_change_label") == "improving":
        return "review_board_improvement"
    if persistence_summary.get("persistent_watch_markets", 0) > 0:
        return "watch_persistent_markets"
    if gate_summary.get("meaningful_candidates", 0) > 0:
        return "watch_for_better_board"
    if execute_summary.get("planned_orders", 0) > 0:
        return "hold_penny_markets_only"
    return "no_action"


def _with_open_order_state_override(
    *,
    recommendation: str,
    open_order_state: dict[str, Any],
) -> str:
    consistency = str(open_order_state.get("consistency") or "").strip().lower()
    if consistency != "inconsistent":
        return recommendation
    if recommendation in {
        "check_upstream_error",
        "hold_penny_markets_only",
        "no_action",
        "watch_for_better_board",
    }:
        return "restore_connectivity_then_reconcile_open_orders"
    return recommendation


def run_kalshi_micro_status(
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
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    ledger_csv: str | None = None,
    watch_history_csv: str | None = None,
    history_csv: str | None = None,
    scan_csv: str | None = None,
    recent_scan_max_age_seconds: float = 300.0,
    execute_runner: ExecuteRunner = run_kalshi_micro_execute,
    reconcile_runner: ReconcileRunner = run_kalshi_micro_reconcile,
    quality_runner: QualityRunner = run_kalshi_nonsports_quality,
    signal_runner: SignalRunner = run_kalshi_nonsports_signals,
    persistence_runner: PersistenceRunner = run_kalshi_nonsports_persistence,
    delta_runner: DeltaRunner = run_kalshi_nonsports_deltas,
    category_runner: CategoryRunner = run_kalshi_nonsports_categories,
    pressure_runner: PressureRunner = run_kalshi_nonsports_pressure,
    threshold_runner: ThresholdRunner = run_kalshi_nonsports_thresholds,
    prior_runner: PriorRunner = run_kalshi_nonsports_priors,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    timezone_name = "America/New_York"
    trading_day = trading_day_for_timestamp(captured_at, timezone_name)
    ledger_path = Path(ledger_csv) if ledger_csv else default_ledger_path(output_dir)
    watch_history_path = Path(watch_history_csv) if watch_history_csv else default_watch_history_path(output_dir)
    effective_book_db_path = default_book_db_path(output_dir)
    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")
    recent_scan_csv = scan_csv or _find_recent_scan_csv(
        output_dir=output_dir,
        captured_at=captured_at,
        max_age_seconds=recent_scan_max_age_seconds,
    )

    execute_summary = execute_runner(
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
        ledger_csv=str(ledger_path),
        scan_csv=recent_scan_csv,
        allow_live_orders=False,
        cancel_resting_immediately=False,
        now=captured_at,
    )
    reconcile_summary = reconcile_runner(
        env_file=env_file,
        execute_summary_file=execute_summary.get("output_file"),
        output_dir=output_dir,
        timeout_seconds=timeout_seconds,
        now=captured_at,
    )
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
    threshold_summary = threshold_runner(
        history_csv=effective_history_csv,
        output_dir=output_dir,
        now=captured_at,
    )
    prior_summary = prior_runner(
        history_csv=effective_history_csv,
        output_dir=output_dir,
        now=captured_at,
    )

    attempts = [
        attempt for attempt in execute_summary.get("attempts", [])
        if isinstance(attempt, dict)
    ] if isinstance(execute_summary.get("attempts"), list) else []
    meaningful_candidates = count_meaningful_candidates(attempts)
    top_attempt = attempts[0] if attempts else None
    ledger_summary = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )
    open_positions_count = count_open_positions(book_db_path=effective_book_db_path)
    open_order_state = _open_order_state_snapshot(book_db_path=effective_book_db_path)
    gate_summary = build_trade_gate_decision(
        actual_live_balance_dollars=execute_summary.get("actual_live_balance_dollars"),
        funding_gap_dollars=execute_summary.get("funding_gap_dollars"),
        planned_orders=int(execute_summary.get("planned_orders") or 0),
        meaningful_candidates=meaningful_candidates,
        ledger_summary=ledger_summary,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        open_positions_count=open_positions_count,
        quality_summary=quality_summary,
        signal_summary=signal_summary,
        persistence_summary=persistence_summary,
        delta_summary=delta_summary,
        category_summary=category_summary,
        pressure_summary=pressure_summary,
    )
    if execute_summary.get("status") in {"rate_limited", "upstream_error"}:
        blockers = list(gate_summary.get("gate_blockers", []))
        plan_events_error = str(execute_summary.get("plan_events_error") or "").strip()
        if plan_events_error:
            blockers.insert(0, plan_events_error)
        gate_summary.update(
            {
                "gate_pass": False,
                "gate_status": str(execute_summary.get("status")),
                "gate_blockers": blockers,
            }
        )
    if str(open_order_state.get("consistency") or "").strip().lower() == "inconsistent":
        blockers = list(gate_summary.get("gate_blockers", []))
        warning = str(open_order_state.get("warning") or "").strip()
        if warning and warning not in blockers:
            blockers.append(warning)
        gate_summary["gate_blockers"] = blockers
        gate_summary["gate_pass"] = False
        gate_summary["gate_status"] = "open_order_state_inconsistent"

    watch_recommendation = _with_open_order_state_override(
        recommendation=_recommendation(
            execute_summary=execute_summary,
            reconcile_summary=reconcile_summary,
            quality_summary=quality_summary,
            signal_summary=signal_summary,
            persistence_summary=persistence_summary,
            delta_summary=delta_summary,
            category_summary=category_summary,
            pressure_summary=pressure_summary,
            threshold_summary=threshold_summary,
            prior_summary=prior_summary,
            gate_summary=gate_summary,
        ),
        open_order_state=open_order_state,
    )

    append_watch_history(
        watch_history_path,
        {
            "recorded_at": captured_at.isoformat(),
            "capture_status": "status_only",
            "capture_scan_status": execute_summary.get("status"),
            "status_recommendation": watch_recommendation,
            "status_trade_gate_status": gate_summary.get("gate_status"),
            "trade_gate_pass": str(bool(gate_summary.get("gate_pass"))).lower(),
            "meaningful_candidates_yes_bid_ge_0_05": meaningful_candidates,
            "persistent_tradeable_markets": persistence_summary.get("persistent_tradeable_markets"),
            "improved_two_sided_markets": delta_summary.get("improved_two_sided_markets"),
            "pressure_build_markets": pressure_summary.get("build_markets"),
            "threshold_approaching_markets": threshold_summary.get("approaching_markets"),
            "top_pressure_market_ticker": pressure_summary.get("top_build_market_ticker"),
            "top_threshold_market_ticker": threshold_summary.get("top_approaching_market_ticker"),
            "board_change_label": delta_summary.get("board_change_label"),
            "top_category": (
                category_summary.get("top_categories", [{}])[0].get("category")
                if isinstance(category_summary.get("top_categories"), list) and category_summary.get("top_categories")
                else None
            ),
            "top_category_label": (
                category_summary.get("top_categories", [{}])[0].get("category_label")
                if isinstance(category_summary.get("top_categories"), list) and category_summary.get("top_categories")
                else None
            ),
            "category_concentration_warning": category_summary.get("concentration_warning"),
        },
    )
    watch_history_summary = summarize_watch_history(watch_history_path)
    recommendation = _recommendation(
        execute_summary=execute_summary,
        reconcile_summary=reconcile_summary,
        quality_summary=quality_summary,
        signal_summary=signal_summary,
        persistence_summary=persistence_summary,
        delta_summary=delta_summary,
        category_summary=category_summary,
        pressure_summary=pressure_summary,
        threshold_summary=threshold_summary,
        prior_summary=prior_summary,
        gate_summary=gate_summary,
    )
    focus_market_state = str(watch_history_summary.get("focus_market_state") or "")
    if focus_market_state == "sustained_pressure_focus" and recommendation == "review_pressure_build":
        recommendation = "monitor_focus_market"
    elif focus_market_state == "stalled_pressure_focus" and recommendation == "review_pressure_build":
        recommendation = "hold_focus_market_stalled"
    elif focus_market_state in {"new_threshold_focus", "sustained_threshold_focus", "pressure_with_threshold_context"}:
        if recommendation in {"review_pressure_build", "monitor_focus_market"}:
            recommendation = "review_threshold_approach"
    recommendation = _with_open_order_state_override(
        recommendation=recommendation,
        open_order_state=open_order_state,
    )
    focus_dossier = run_kalshi_focus_dossier(
        history_csv=effective_history_csv,
        watch_history_csv=str(watch_history_path),
        output_dir=output_dir,
        watch_history_summary=watch_history_summary,
        now=captured_at,
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "planning_bankroll_dollars": planning_bankroll_dollars,
        "daily_risk_cap_dollars": daily_risk_cap_dollars,
        "max_live_submissions_per_day": max_live_submissions_per_day,
        "max_live_cost_per_day_dollars": max_live_cost_per_day_dollars,
        "actual_live_balance_dollars": execute_summary.get("actual_live_balance_dollars"),
        "actual_live_balance_source": execute_summary.get("actual_live_balance_source"),
        "balance_live_verified": execute_summary.get("balance_live_verified"),
        "balance_check_error": execute_summary.get("balance_check_error"),
        "balance_cache_file": execute_summary.get("balance_cache_file"),
        "balance_cache_age_seconds": execute_summary.get("balance_cache_age_seconds"),
        "planned_orders": execute_summary.get("planned_orders"),
        "meaningful_candidates_yes_bid_ge_0_05": meaningful_candidates,
        "top_market_ticker": top_attempt.get("market_ticker") if isinstance(top_attempt, dict) else None,
        "top_market_yes_bid_dollars": top_attempt.get("planned_yes_bid_dollars") if isinstance(top_attempt, dict) else None,
        "board_warning": execute_summary.get("board_warning"),
        "latest_execute_status": execute_summary.get("status"),
        "reused_scan_csv": recent_scan_csv,
        "latest_reconcile_status": reconcile_summary.get("status"),
        "reconcile_status_counts": reconcile_summary.get("status_counts"),
        "total_market_exposure_dollars": reconcile_summary.get("total_market_exposure_dollars"),
        "total_realized_pnl_dollars": reconcile_summary.get("total_realized_pnl_dollars"),
        "total_fees_paid_dollars": reconcile_summary.get("total_fees_paid_dollars"),
        "ledger_summary": ledger_summary,
        "open_positions_count": open_positions_count,
        "positions_with_nonzero_exposure": open_order_state.get("positions_with_nonzero_exposure"),
        "positions_with_resting_orders_hint": open_order_state.get("positions_with_resting_orders_hint"),
        "resting_orders_in_orders_table": open_order_state.get("resting_orders_in_orders_table"),
        "open_order_state_consistency": open_order_state.get("consistency"),
        "open_order_state_warning": open_order_state.get("warning"),
        "live_submissions_to_date": gate_summary.get("live_submissions_to_date"),
        "live_submissions_remaining_today": gate_summary.get("live_submissions_remaining_today"),
        "live_submission_days_elapsed": gate_summary.get("live_submission_days_elapsed"),
        "live_submission_budget_total": gate_summary.get("live_submission_budget_total"),
        "live_submission_budget_remaining": gate_summary.get("live_submission_budget_remaining"),
        "live_cost_budget_total": gate_summary.get("live_cost_budget_total"),
        "live_cost_budget_remaining": gate_summary.get("live_cost_budget_remaining"),
        "live_cost_remaining_today": gate_summary.get("live_cost_remaining_today"),
        "live_cost_remaining_dollars": gate_summary.get("live_cost_remaining_dollars"),
        "meaningful_markets_observed": quality_summary.get("meaningful_markets"),
        "watchlist_markets_observed": quality_summary.get("watchlist_markets"),
        "top_quality_market_ticker": (
            quality_summary.get("top_markets", [{}])[0].get("market_ticker")
            if isinstance(quality_summary.get("top_markets"), list) and quality_summary.get("top_markets")
            else None
        ),
        "eligible_signal_markets": signal_summary.get("eligible_markets"),
        "watch_signal_markets": signal_summary.get("watch_markets"),
        "top_signal_market_ticker": (
            signal_summary.get("top_markets", [{}])[0].get("market_ticker")
            if isinstance(signal_summary.get("top_markets"), list) and signal_summary.get("top_markets")
            else None
        ),
        "persistent_tradeable_markets": persistence_summary.get("persistent_tradeable_markets"),
        "persistent_watch_markets": persistence_summary.get("persistent_watch_markets"),
        "recurring_markets_observed": persistence_summary.get("recurring_markets"),
        "top_persistence_market_ticker": (
            persistence_summary.get("top_markets", [{}])[0].get("market_ticker")
            if isinstance(persistence_summary.get("top_markets"), list) and persistence_summary.get("top_markets")
            else None
        ),
        "board_change_label": delta_summary.get("board_change_label"),
        "improved_two_sided_markets": delta_summary.get("improved_two_sided_markets"),
        "newly_tradeable_markets": delta_summary.get("newly_tradeable_markets"),
        "top_delta_market_ticker": (
            delta_summary.get("top_markets", [{}])[0].get("market_ticker")
            if isinstance(delta_summary.get("top_markets"), list) and delta_summary.get("top_markets")
            else None
        ),
        "tradeable_categories_observed": category_summary.get("tradeable_categories"),
        "watch_categories_observed": category_summary.get("watch_categories"),
        "thin_categories_observed": category_summary.get("thin_categories"),
        "pressure_build_markets": pressure_summary.get("build_markets"),
        "pressure_watch_markets": pressure_summary.get("watch_markets"),
        "top_pressure_market_ticker": pressure_summary.get("top_build_market_ticker"),
        "top_pressure_category": pressure_summary.get("top_build_category"),
        "threshold_approaching_markets": threshold_summary.get("approaching_markets"),
        "threshold_building_markets": threshold_summary.get("building_markets"),
        "top_threshold_market_ticker": threshold_summary.get("top_approaching_market_ticker"),
        "top_threshold_category": threshold_summary.get("top_approaching_category"),
        "top_threshold_hours_to_tradeable": threshold_summary.get("top_approaching_hours_to_tradeable"),
        "prior_markets_covered": prior_summary.get("matched_live_markets"),
        "prior_positive_yes_bid_markets": prior_summary.get("positive_edge_yes_bid_markets"),
        "prior_positive_yes_ask_markets": prior_summary.get("positive_edge_yes_ask_markets"),
        "prior_positive_no_bid_markets": prior_summary.get("positive_edge_no_bid_markets"),
        "prior_positive_no_ask_markets": prior_summary.get("positive_edge_no_ask_markets"),
        "prior_positive_best_entry_markets": prior_summary.get("positive_best_entry_markets"),
        "top_prior_market_ticker": prior_summary.get("top_market_ticker"),
        "top_prior_edge_to_yes_ask": prior_summary.get("top_market_edge_to_yes_ask"),
        "top_prior_best_entry_side": prior_summary.get("top_market_best_entry_side"),
        "top_prior_best_entry_edge": prior_summary.get("top_market_best_entry_edge"),
        "top_prior_best_maker_entry_side": prior_summary.get("top_market_best_maker_entry_side"),
        "top_prior_best_maker_entry_edge": prior_summary.get("top_market_best_maker_entry_edge"),
        "top_category": (
            category_summary.get("top_categories", [{}])[0].get("category")
            if isinstance(category_summary.get("top_categories"), list) and category_summary.get("top_categories")
            else None
        ),
        "top_category_label": (
            category_summary.get("top_categories", [{}])[0].get("category_label")
            if isinstance(category_summary.get("top_categories"), list) and category_summary.get("top_categories")
            else None
        ),
        "top_category_rank_score": (
            category_summary.get("top_categories", [{}])[0].get("category_rank_score")
            if isinstance(category_summary.get("top_categories"), list) and category_summary.get("top_categories")
            else None
        ),
        "category_concentration_warning": category_summary.get("concentration_warning"),
        "trade_gate_pass": gate_summary.get("gate_pass"),
        "trade_gate_status": gate_summary.get("gate_status"),
        "trade_gate_score": gate_summary.get("gate_score"),
        "trade_gate_blockers": gate_summary.get("gate_blockers"),
        "watch_history_csv": str(watch_history_path),
        "watch_history_summary": watch_history_summary,
        "board_regime": watch_history_summary.get("board_regime"),
        "board_regime_reason": watch_history_summary.get("board_regime_reason"),
        "focus_market_mode": watch_history_summary.get("latest_focus_market_mode"),
        "focus_market_ticker": watch_history_summary.get("latest_focus_market_ticker"),
        "focus_market_streak": watch_history_summary.get("focus_market_streak"),
        "focus_market_state": watch_history_summary.get("focus_market_state"),
        "focus_market_state_reason": watch_history_summary.get("focus_market_state_reason"),
        "focus_dossier_action_hint": focus_dossier.get("action_hint"),
        "focus_dossier_action_reason": focus_dossier.get("action_reason"),
        "focus_dossier_research_prompt": focus_dossier.get("research_prompt"),
        "recommendation": recommendation,
        "execute_summary_file": execute_summary.get("output_file"),
        "reconcile_summary_file": reconcile_summary.get("output_file"),
        "quality_summary_file": quality_summary.get("output_file"),
        "signal_summary_file": signal_summary.get("output_file"),
        "persistence_summary_file": persistence_summary.get("output_file"),
        "delta_summary_file": delta_summary.get("output_file"),
        "category_summary_file": category_summary.get("output_file"),
        "pressure_summary_file": pressure_summary.get("output_file"),
        "threshold_summary_file": threshold_summary.get("output_file"),
        "prior_summary_file": prior_summary.get("output_file"),
        "focus_dossier_file": focus_dossier.get("output_file"),
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_status_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
