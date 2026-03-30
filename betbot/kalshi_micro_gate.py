from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_book import count_open_positions, default_book_db_path
from betbot.kalshi_micro_ledger import default_ledger_path, summarize_trade_ledger, trading_day_for_timestamp
from betbot.kalshi_micro_plan import run_kalshi_micro_plan
from betbot.kalshi_nonsports_categories import run_kalshi_nonsports_categories
from betbot.kalshi_nonsports_deltas import run_kalshi_nonsports_deltas
from betbot.kalshi_nonsports_persistence import run_kalshi_nonsports_persistence
from betbot.kalshi_nonsports_pressure import run_kalshi_nonsports_pressure
from betbot.kalshi_nonsports_quality import run_kalshi_nonsports_quality
from betbot.kalshi_nonsports_signals import run_kalshi_nonsports_signals


PlanRunner = Callable[..., dict[str, Any]]
QualityRunner = Callable[..., dict[str, Any]]
SignalRunner = Callable[..., dict[str, Any]]
PersistenceRunner = Callable[..., dict[str, Any]]
DeltaRunner = Callable[..., dict[str, Any]]
CategoryRunner = Callable[..., dict[str, Any]]
PressureRunner = Callable[..., dict[str, Any]]


def count_meaningful_candidates(
    attempts_or_orders: list[dict[str, Any]] | None,
    *,
    min_meaningful_yes_bid_dollars: float = 0.05,
) -> int:
    if not isinstance(attempts_or_orders, list):
        return 0
    count = 0
    for row in attempts_or_orders:
        if not isinstance(row, dict):
            continue
        for key in ("planned_yes_bid_dollars", "maker_yes_price_dollars", "yes_bid_dollars"):
            value = row.get(key)
            if isinstance(value, int | float) and value >= min_meaningful_yes_bid_dollars:
                count += 1
                break
    return count


def build_trade_gate_decision(
    *,
    actual_live_balance_dollars: float | None,
    funding_gap_dollars: float | None,
    planned_orders: int,
    meaningful_candidates: int,
    ledger_summary: dict[str, Any],
    max_live_submissions_per_day: int,
    max_live_cost_per_day_dollars: float,
    open_positions_count: int = 0,
    quality_summary: dict[str, Any],
    signal_summary: dict[str, Any],
    persistence_summary: dict[str, Any],
    delta_summary: dict[str, Any],
    category_summary: dict[str, Any],
    pressure_summary: dict[str, Any],
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

    meaningful_markets = int(quality_summary.get("meaningful_markets") or 0)
    eligible_markets = int(signal_summary.get("eligible_markets") or 0)
    persistent_tradeable_markets = int(persistence_summary.get("persistent_tradeable_markets") or 0)
    improved_two_sided_markets = int(delta_summary.get("improved_two_sided_markets") or 0)
    newly_tradeable_markets = int(delta_summary.get("newly_tradeable_markets") or 0)
    board_change_label = str(delta_summary.get("board_change_label") or "unknown")
    tradeable_categories = int(category_summary.get("tradeable_categories") or 0)
    watch_categories = int(category_summary.get("watch_categories") or 0)
    pressure_build_markets = int(pressure_summary.get("build_markets") or 0)
    pressure_watch_markets = int(pressure_summary.get("watch_markets") or 0)
    top_categories = category_summary.get("top_categories")
    top_category_row = top_categories[0] if isinstance(top_categories, list) and top_categories else {}
    top_category = str(top_category_row.get("category") or "") if isinstance(top_category_row, dict) else ""
    top_category_label = str(top_category_row.get("category_label") or "") if isinstance(top_category_row, dict) else ""
    concentration_warning = str(category_summary.get("concentration_warning") or "").strip()

    has_signal_edge = eligible_markets > 0
    has_persistent_edge = persistent_tradeable_markets > 0
    has_quality_plus_improvement = meaningful_markets > 0 and board_change_label == "improving"
    edge_ready = has_signal_edge or has_persistent_edge or has_quality_plus_improvement

    blockers: list[str] = []
    if actual_live_balance_dollars is None:
        blockers.append("Live balance could not be verified.")
    elif actual_live_balance_dollars in (0, 0.0):
        blockers.append("Live balance is not funded.")
    elif isinstance(funding_gap_dollars, int | float) and funding_gap_dollars > 0:
        blockers.append("Planned live workflow still shows a funding gap.")
    if live_submission_budget_remaining <= 0:
        blockers.append("Accumulated live submission budget is exhausted.")
    if live_cost_budget_remaining <= 0:
        blockers.append("Accumulated live cost budget is exhausted.")
    if planned_orders <= 0:
        blockers.append("No planned orders are available on the current board.")
    if meaningful_candidates <= 0:
        blockers.append("No candidate clears the $0.05 Yes-bid floor.")
    if not edge_ready:
        blockers.append("No persistent tradeable or signal-backed market is available yet.")
    if board_change_label == "stale" and not has_signal_edge and not has_persistent_edge:
        blockers.append("Board is stale between the latest two snapshots.")
    if board_change_label == "deteriorating":
        blockers.append("Board quality deteriorated between the latest two snapshots.")
    if concentration_warning and not edge_ready:
        blockers.append("Observed two-sided liquidity is concentrated in one category.")

    gate_pass = len(blockers) == 0
    gate_status = "pass" if gate_pass else "hold"
    if not gate_pass:
        if actual_live_balance_dollars is None:
            gate_status = "balance_unavailable"
        elif actual_live_balance_dollars in (0, 0.0) or (
            isinstance(funding_gap_dollars, int | float) and funding_gap_dollars > 0
        ):
            gate_status = "needs_funding"
        elif live_submission_budget_remaining <= 0 or live_cost_budget_remaining <= 0:
            gate_status = "cap_reached"
        elif planned_orders <= 0:
            gate_status = "no_candidates"
        elif meaningful_candidates <= 0:
            gate_status = "no_meaningful_candidates"
        elif board_change_label == "deteriorating":
            gate_status = "deteriorating_board"
        elif board_change_label == "stale":
            gate_status = "stale_board"
        else:
            gate_status = "insufficient_edge"

    gate_score = round(
        min(
            100.0,
            meaningful_candidates * 20.0
            + meaningful_markets * 10.0
            + eligible_markets * 20.0
            + persistent_tradeable_markets * 25.0
            + improved_two_sided_markets * 10.0
            + newly_tradeable_markets * 15.0
            + pressure_build_markets * 5.0
            + min(pressure_watch_markets, 3) * 2.0
            + min(live_submission_budget_remaining, max_live_submissions_per_day * 2) * 2.0,
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
        "meaningful_candidates": meaningful_candidates,
        "meaningful_markets_observed": meaningful_markets,
        "eligible_signal_markets": eligible_markets,
        "persistent_tradeable_markets": persistent_tradeable_markets,
        "improved_two_sided_markets": improved_two_sided_markets,
        "newly_tradeable_markets": newly_tradeable_markets,
        "board_change_label": board_change_label,
        "tradeable_categories_observed": tradeable_categories,
        "watch_categories_observed": watch_categories,
        "pressure_build_markets": pressure_build_markets,
        "pressure_watch_markets": pressure_watch_markets,
        "top_pressure_market_ticker": pressure_summary.get("top_build_market_ticker"),
        "top_pressure_category": pressure_summary.get("top_build_category"),
        "top_category": top_category or None,
        "top_category_label": top_category_label or None,
        "category_concentration_warning": concentration_warning or None,
    }


def run_kalshi_micro_gate(
    *,
    env_file: str,
    output_dir: str = "outputs",
    history_csv: str | None = None,
    scan_csv: str | None = None,
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
    book_db_path: str | None = None,
    plan_runner: PlanRunner = run_kalshi_micro_plan,
    quality_runner: QualityRunner = run_kalshi_nonsports_quality,
    signal_runner: SignalRunner = run_kalshi_nonsports_signals,
    persistence_runner: PersistenceRunner = run_kalshi_nonsports_persistence,
    delta_runner: DeltaRunner = run_kalshi_nonsports_deltas,
    category_runner: CategoryRunner = run_kalshi_nonsports_categories,
    pressure_runner: PressureRunner = run_kalshi_nonsports_pressure,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    timezone_name = "America/New_York"
    trading_day = trading_day_for_timestamp(captured_at, timezone_name)
    ledger_path = Path(ledger_csv) if ledger_csv else default_ledger_path(output_dir)
    effective_book_db_path = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)
    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")

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
        now=captured_at,
    )
    ledger_summary = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )
    open_positions_count = count_open_positions(book_db_path=effective_book_db_path)
    quality_summary = quality_runner(history_csv=effective_history_csv, output_dir=output_dir, now=captured_at)
    signal_summary = signal_runner(history_csv=effective_history_csv, output_dir=output_dir, now=captured_at)
    persistence_summary = persistence_runner(
        history_csv=effective_history_csv,
        output_dir=output_dir,
        now=captured_at,
    )
    delta_summary = delta_runner(history_csv=effective_history_csv, output_dir=output_dir, now=captured_at)
    category_summary = category_runner(history_csv=effective_history_csv, output_dir=output_dir, now=captured_at)
    pressure_summary = pressure_runner(history_csv=effective_history_csv, output_dir=output_dir, now=captured_at)

    decision = build_trade_gate_decision(
        actual_live_balance_dollars=plan_summary.get("actual_live_balance_dollars"),
        funding_gap_dollars=plan_summary.get("funding_gap_dollars"),
        planned_orders=int(plan_summary.get("planned_orders") or 0),
        meaningful_candidates=count_meaningful_candidates(plan_summary.get("orders")),
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
    plan_status = str(plan_summary.get("status") or "")
    if plan_status in {"rate_limited", "upstream_error"}:
        blockers = list(decision.get("gate_blockers", []))
        events_error = str(plan_summary.get("events_error") or "Kalshi board fetch failed.")
        blockers.insert(0, events_error)
        decision.update(
            {
                "gate_pass": False,
                "gate_status": plan_status,
                "gate_blockers": blockers,
            }
        )

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "history_csv": effective_history_csv,
        "ledger_csv": str(ledger_path),
        "book_db_path": str(effective_book_db_path),
        "plan_summary_file": plan_summary.get("output_file"),
        "quality_summary_file": quality_summary.get("output_file"),
        "signal_summary_file": signal_summary.get("output_file"),
        "persistence_summary_file": persistence_summary.get("output_file"),
        "delta_summary_file": delta_summary.get("output_file"),
        "category_summary_file": category_summary.get("output_file"),
        "pressure_summary_file": pressure_summary.get("output_file"),
        **decision,
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_gate_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
