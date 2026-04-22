from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_focus_dossier import run_kalshi_focus_dossier
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_micro_gate import run_kalshi_micro_gate
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile
from betbot.kalshi_micro_watch_history import default_watch_history_path, summarize_watch_history


CaptureRunner = Callable[..., dict[str, Any]]
GateRunner = Callable[..., dict[str, Any]]
ExecuteRunner = Callable[..., dict[str, Any]]
ReconcileRunner = Callable[..., dict[str, Any]]


def _coerce_gate_pass(value: Any) -> tuple[bool, bool]:
    if isinstance(value, bool):
        return value, True
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True, True
        if normalized in {"false", "0", "no", ""}:
            return False, True
        return False, False
    if isinstance(value, int):
        if value in {0, 1}:
            return bool(value), True
        return False, False
    return False, False


def run_kalshi_micro_trader(
    *,
    env_file: str,
    output_dir: str = "outputs",
    history_csv: str | None = None,
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
    watch_history_csv: str | None = None,
    allow_live_orders: bool = False,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    capture_before_gate: bool = True,
    capture_runner: CaptureRunner = run_kalshi_nonsports_capture,
    gate_runner: GateRunner = run_kalshi_micro_gate,
    execute_runner: ExecuteRunner = run_kalshi_micro_execute,
    reconcile_runner: ReconcileRunner = run_kalshi_micro_reconcile,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")
    watch_history_path = Path(watch_history_csv) if watch_history_csv else default_watch_history_path(output_dir)
    watch_history_summary = summarize_watch_history(watch_history_path)
    capture_summary: dict[str, Any] | None = None
    if capture_before_gate:
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
    effective_scan_csv = (
        str(capture_summary.get("scan_output_csv"))
        if isinstance(capture_summary, dict) and capture_summary.get("status") == "ready"
        else None
    )
    gate_summary = gate_runner(
        env_file=env_file,
        output_dir=output_dir,
        history_csv=effective_history_csv,
        scan_csv=effective_scan_csv,
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
        now=captured_at,
    )
    focus_dossier = run_kalshi_focus_dossier(
        history_csv=effective_history_csv,
        watch_history_csv=str(watch_history_path),
        output_dir=output_dir,
        watch_history_summary=watch_history_summary,
        now=captured_at,
    )
    gate_pass, gate_pass_valid = _coerce_gate_pass(gate_summary.get("gate_pass"))
    gate_status = gate_summary.get("gate_status")
    gate_blockers = list(gate_summary.get("gate_blockers") or [])
    if not gate_pass_valid:
        gate_pass = False
        gate_status = "invalid_gate_pass"
        gate_blockers.insert(0, f"Invalid gate_pass value: {gate_summary.get('gate_pass')!r}")

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "capture_before_gate": capture_before_gate,
        "capture_status": capture_summary.get("status") if isinstance(capture_summary, dict) else None,
        "capture_summary_file": capture_summary.get("scan_summary_file") if isinstance(capture_summary, dict) else None,
        "capture_history_csv": capture_summary.get("history_csv") if isinstance(capture_summary, dict) else effective_history_csv,
        "capture_scan_csv": effective_scan_csv,
        "allow_live_orders": allow_live_orders,
        "gate_pass": gate_pass,
        "gate_status": gate_status,
        "gate_score": gate_summary.get("gate_score"),
        "gate_blockers": gate_blockers,
        "gate_top_category": gate_summary.get("top_category"),
        "gate_top_category_label": gate_summary.get("top_category_label"),
        "gate_category_concentration_warning": gate_summary.get("category_concentration_warning"),
        "gate_pressure_build_markets": gate_summary.get("pressure_build_markets"),
        "gate_pressure_watch_markets": gate_summary.get("pressure_watch_markets"),
        "gate_top_pressure_market_ticker": gate_summary.get("top_pressure_market_ticker"),
        "gate_top_pressure_category": gate_summary.get("top_pressure_category"),
        "watch_history_csv": str(watch_history_path),
        "watch_history_summary": watch_history_summary,
        "watch_board_regime": watch_history_summary.get("board_regime"),
        "watch_board_regime_reason": watch_history_summary.get("board_regime_reason"),
        "watch_focus_market_mode": watch_history_summary.get("latest_focus_market_mode"),
        "watch_focus_market_ticker": watch_history_summary.get("latest_focus_market_ticker"),
        "watch_focus_market_streak": watch_history_summary.get("focus_market_streak"),
        "watch_focus_market_state": watch_history_summary.get("focus_market_state"),
        "watch_focus_market_state_reason": watch_history_summary.get("focus_market_state_reason"),
        "focus_dossier_action_hint": focus_dossier.get("action_hint"),
        "focus_dossier_action_reason": focus_dossier.get("action_reason"),
        "focus_dossier_research_prompt": focus_dossier.get("research_prompt"),
        "focus_dossier_file": focus_dossier.get("output_file"),
        "gate_summary_file": gate_summary.get("output_file"),
        "action_taken": "hold",
        "status": "hold",
    }

    if isinstance(capture_summary, dict) and capture_summary.get("status") not in {None, "ready"}:
        blockers = list(summary["gate_blockers"])
        scan_error = str(capture_summary.get("scan_error") or capture_summary.get("status"))
        blockers.insert(0, f"Fresh capture failed: {scan_error}")
        summary["gate_pass"] = False
        summary["gate_status"] = str(capture_summary.get("status"))
        summary["gate_blockers"] = blockers

    if not summary.get("gate_pass", False):
        stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f"kalshi_micro_trader_summary_{stamp}.json"
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["output_file"] = str(output_path)
        return summary

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
        allow_live_orders=allow_live_orders,
        cancel_resting_immediately=cancel_resting_immediately,
        resting_hold_seconds=resting_hold_seconds,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        ledger_csv=ledger_csv,
        book_db_path=book_db_path,
        history_csv=effective_history_csv,
        scan_csv=effective_scan_csv,
        enforce_trade_gate=True,
        now=captured_at,
    )
    reconcile_summary = reconcile_runner(
        env_file=env_file,
        execute_summary_file=execute_summary.get("output_file"),
        output_dir=output_dir,
        book_db_path=book_db_path,
        timeout_seconds=timeout_seconds,
        now=captured_at,
    )

    summary.update(
        {
            "action_taken": "live_execute_reconcile" if allow_live_orders else "dry_run_execute_reconcile",
            "status": "executed",
            "execute_status": execute_summary.get("status"),
            "reconcile_status": reconcile_summary.get("status"),
            "execute_summary_file": execute_summary.get("output_file"),
            "reconcile_summary_file": reconcile_summary.get("output_file"),
        }
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_trader_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
