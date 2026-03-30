from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_micro_watch_history import default_watch_history_path
from betbot.kalshi_micro_status import run_kalshi_micro_status
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture


CaptureRunner = Callable[..., dict[str, Any]]
StatusRunner = Callable[..., dict[str, Any]]


def run_kalshi_micro_watch(
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
    watch_history_csv: str | None = None,
    capture_runner: CaptureRunner = run_kalshi_nonsports_capture,
    status_runner: StatusRunner = run_kalshi_micro_status,
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
        scan_csv=capture_summary.get("scan_output_csv") if capture_summary.get("status") == "ready" else None,
        now=captured_at,
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "capture_status": capture_summary.get("status"),
        "capture_scan_status": capture_summary.get("scan_status"),
        "capture_scan_error": capture_summary.get("scan_error"),
        "capture_summary_file": capture_summary.get("scan_summary_file"),
        "capture_scan_csv": capture_summary.get("scan_output_csv"),
        "history_csv": capture_summary.get("history_csv", effective_history_csv),
        "status_recommendation": status_summary.get("recommendation"),
        "status_trade_gate_status": status_summary.get("trade_gate_status"),
        "status_reused_scan_csv": status_summary.get("reused_scan_csv"),
        "status_top_category": status_summary.get("top_category"),
        "status_top_category_label": status_summary.get("top_category_label"),
        "status_category_concentration_warning": status_summary.get("category_concentration_warning"),
        "status_board_regime": status_summary.get("board_regime"),
        "status_board_regime_reason": status_summary.get("board_regime_reason"),
        "watch_history_csv": status_summary.get("watch_history_csv", str(watch_history_path)),
        "watch_history_summary": status_summary.get("watch_history_summary"),
        "status_summary_file": status_summary.get("output_file"),
        "status": "ready",
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_watch_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
