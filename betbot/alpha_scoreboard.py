from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


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


def _latest_output_file(output_dir: Path, pattern: str) -> Path | None:
    matches = list(output_dir.glob(pattern))
    if not matches:
        return None
    matches.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return matches[0]


def _load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_research_rows(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader):
            if idx >= limit:
                break
            rows.append(
                {
                    "market_ticker": str(row.get("market_ticker") or ""),
                    "category": str(row.get("category") or ""),
                    "research_priority_label": str(row.get("research_priority_label") or ""),
                    "research_priority_score": _parse_float(row.get("research_priority_score")),
                    "research_prompt": str(row.get("research_prompt") or ""),
                }
            )
    return rows


def _annualize_cycle_return(*, cycle_return: float | None, cycle_days: float | None) -> float | None:
    if cycle_return is None or cycle_days is None or cycle_days <= 0.0:
        return None
    if cycle_return <= -1.0:
        return None
    return (1.0 + cycle_return) ** (365.0 / cycle_days) - 1.0


def _required_cycle_return_for_benchmark(*, benchmark_annual_return: float, cycle_days: float | None) -> float | None:
    if cycle_days is None or cycle_days <= 0.0:
        return None
    return (1.0 + benchmark_annual_return) ** (cycle_days / 365.0) - 1.0


def _safe_pct(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value * 100.0, 6)


def _latest_plan_projection(plan_summary: dict[str, Any]) -> dict[str, float | int | None]:
    top_plans = plan_summary.get("top_plans")
    if isinstance(top_plans, list) and top_plans:
        total_cost = 0.0
        total_ev_net = 0.0
        total_ev_day_net = 0.0
        counted_rows = 0
        for row in top_plans:
            if not isinstance(row, dict):
                continue
            cost = _parse_float(row.get("estimated_entry_cost_dollars"))
            ev_net = _parse_float(row.get("expected_value_net_dollars"))
            ev_day_net = _parse_float(row.get("expected_value_per_day_net_dollars"))
            if cost is None or ev_net is None:
                continue
            total_cost += cost
            total_ev_net += ev_net
            if ev_day_net is not None:
                total_ev_day_net += ev_day_net
            counted_rows += 1
        if counted_rows > 0 and total_cost > 0.0:
            roi_cycle_net = total_ev_net / total_cost
            roi_day_net = total_ev_day_net / total_cost
            return {
                "planned_orders_count": counted_rows,
                "total_planned_cost_dollars": round(total_cost, 6),
                "trade_net_roi_per_cycle": round(roi_cycle_net, 9),
                "trade_net_roi_per_day": round(roi_day_net, 9),
            }

    top_market_roi_cycle_net = _parse_float(plan_summary.get("top_market_expected_roi_on_cost_net"))
    top_market_roi_day_net = _parse_float(plan_summary.get("top_market_expected_roi_per_day_net"))
    planned_cost = _parse_float(plan_summary.get("total_planned_cost_dollars"))
    planned_orders = _parse_float(plan_summary.get("planned_orders"))
    return {
        "planned_orders_count": int(planned_orders) if planned_orders is not None else None,
        "total_planned_cost_dollars": planned_cost,
        "trade_net_roi_per_cycle": top_market_roi_cycle_net,
        "trade_net_roi_per_day": top_market_roi_day_net,
    }


def run_alpha_scoreboard(
    *,
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    benchmark_annual_return: float = 0.10,
    plan_summary_file: str | None = None,
    daily_ops_report_file: str | None = None,
    research_queue_csv: str | None = None,
    top_research_targets: int = 5,
    now: datetime | None = None,
) -> dict[str, Any]:
    if planning_bankroll_dollars <= 0.0:
        raise ValueError("planning_bankroll_dollars must be positive")
    if benchmark_annual_return <= -1.0:
        raise ValueError("benchmark_annual_return must be greater than -1.0")
    if top_research_targets <= 0:
        raise ValueError("top_research_targets must be positive")

    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    plan_path = Path(plan_summary_file) if plan_summary_file else _latest_output_file(
        out_dir, "kalshi_micro_prior_plan_summary_*.json"
    )
    daily_ops_path = Path(daily_ops_report_file) if daily_ops_report_file else _latest_output_file(
        out_dir, "daily_ops_report_*.json"
    )
    research_queue_path = Path(research_queue_csv) if research_queue_csv else _latest_output_file(
        out_dir, "kalshi_nonsports_research_queue_*.csv"
    )

    blockers: list[str] = []
    plan_summary: dict[str, Any] | None = None
    if plan_path is None:
        blockers.append("No kalshi_micro_prior_plan_summary file was found.")
    elif not plan_path.exists():
        blockers.append(f"Plan summary file does not exist: {plan_path}")
    else:
        plan_summary = _load_json_file(plan_path)

    daily_ops: dict[str, Any] | None = None
    if daily_ops_path is not None and daily_ops_path.exists():
        daily_ops = _load_json_file(daily_ops_path)

    research_targets: list[dict[str, Any]] = []
    if research_queue_path is not None and research_queue_path.exists():
        research_targets = _load_research_rows(research_queue_path, top_research_targets)

    if plan_summary is None:
        summary = {
            "captured_at": captured_at.isoformat(),
            "status": "blocked",
            "blockers": blockers,
            "benchmark_annual_return_target_pct": _safe_pct(benchmark_annual_return),
            "planning_bankroll_dollars": planning_bankroll_dollars,
            "plan_summary_file": str(plan_path) if plan_path is not None else None,
            "daily_ops_report_file": str(daily_ops_path) if daily_ops_path is not None else None,
            "research_queue_csv": str(research_queue_path) if research_queue_path is not None else None,
            "research_targets": research_targets,
        }
    else:
        projection = _latest_plan_projection(plan_summary)
        cycle_days = None
        hours_to_close = _parse_float(plan_summary.get("top_market_hours_to_close"))
        if hours_to_close is not None and hours_to_close > 0.0:
            cycle_days = hours_to_close / 24.0

        total_planned_cost = _parse_float(projection.get("total_planned_cost_dollars"))
        trade_net_roi_per_cycle = _parse_float(projection.get("trade_net_roi_per_cycle"))
        trade_net_roi_per_day = _parse_float(projection.get("trade_net_roi_per_day"))

        deployed_fraction = None
        if total_planned_cost is not None:
            deployed_fraction = total_planned_cost / planning_bankroll_dollars

        bankroll_net_roi_per_cycle = None
        if deployed_fraction is not None and trade_net_roi_per_cycle is not None:
            bankroll_net_roi_per_cycle = deployed_fraction * trade_net_roi_per_cycle

        bankroll_net_roi_per_day = None
        if bankroll_net_roi_per_cycle is not None and cycle_days is not None and cycle_days > 0.0:
            bankroll_net_roi_per_day = (1.0 + bankroll_net_roi_per_cycle) ** (1.0 / cycle_days) - 1.0

        annualized_net_return = _annualize_cycle_return(
            cycle_return=bankroll_net_roi_per_cycle,
            cycle_days=cycle_days,
        )

        benchmark_gap = None
        if annualized_net_return is not None:
            benchmark_gap = annualized_net_return - benchmark_annual_return

        required_cycle_return = _required_cycle_return_for_benchmark(
            benchmark_annual_return=benchmark_annual_return,
            cycle_days=cycle_days,
        )
        required_deployed_fraction = None
        if (
            required_cycle_return is not None
            and trade_net_roi_per_cycle is not None
            and trade_net_roi_per_cycle > 0.0
        ):
            required_deployed_fraction = required_cycle_return / trade_net_roi_per_cycle

        required_daily_risk_cap = None
        additional_daily_risk_cap = None
        if required_deployed_fraction is not None:
            required_daily_risk_cap = required_deployed_fraction * planning_bankroll_dollars
            if total_planned_cost is not None:
                additional_daily_risk_cap = max(0.0, required_daily_risk_cap - total_planned_cost)

        realized_returns = {}
        if daily_ops is not None:
            realized_returns = daily_ops.get("return_windows") or {}

        status = "ready"
        if annualized_net_return is None:
            status = "insufficient_projection"

        summary = {
            "captured_at": captured_at.isoformat(),
            "status": status,
            "benchmark_annual_return_target_pct": _safe_pct(benchmark_annual_return),
            "planning_bankroll_dollars": planning_bankroll_dollars,
            "plan_summary_file": str(plan_path),
            "daily_ops_report_file": str(daily_ops_path) if daily_ops_path is not None else None,
            "research_queue_csv": str(research_queue_path) if research_queue_path is not None else None,
            "strategy_projection": {
                "planned_orders_count": projection.get("planned_orders_count"),
                "total_planned_cost_dollars": total_planned_cost,
                "deployed_fraction_pct": _safe_pct(deployed_fraction),
                "trade_net_roi_per_cycle_pct": _safe_pct(trade_net_roi_per_cycle),
                "trade_net_roi_per_day_pct": _safe_pct(trade_net_roi_per_day),
                "cycle_days": round(cycle_days, 6) if cycle_days is not None else None,
                "top_market_ticker": plan_summary.get("top_market_ticker"),
                "top_market_side": plan_summary.get("top_market_side"),
            },
            "bankroll_projection": {
                "bankroll_net_roi_per_cycle_pct": _safe_pct(bankroll_net_roi_per_cycle),
                "bankroll_net_roi_per_day_pct": _safe_pct(bankroll_net_roi_per_day),
                "annualized_net_return_pct": _safe_pct(annualized_net_return),
                "beats_benchmark_projection": (
                    annualized_net_return is not None and annualized_net_return >= benchmark_annual_return
                ),
                "benchmark_gap_pct_points": _safe_pct(benchmark_gap),
            },
            "scaling_requirements": {
                "required_deployed_fraction_pct_to_hit_benchmark": _safe_pct(required_deployed_fraction),
                "required_daily_risk_cap_dollars_to_hit_benchmark": (
                    round(required_daily_risk_cap, 6) if required_daily_risk_cap is not None else None
                ),
                "additional_daily_risk_cap_dollars_needed": (
                    round(additional_daily_risk_cap, 6) if additional_daily_risk_cap is not None else None
                ),
            },
            "realized_returns": realized_returns,
            "research_targets": research_targets,
        }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"alpha_scoreboard_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
