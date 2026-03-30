from __future__ import annotations

import csv
from dataclasses import asdict, replace
from datetime import datetime
from itertools import product
import json
from pathlib import Path

from betbot.backtest import run_backtest
from betbot.config import StrategyConfig
from betbot.types import BetCandidate


def parse_float_list(raw: str) -> list[float]:
    values = [x.strip() for x in raw.split(",") if x.strip()]
    if not values:
        raise ValueError("Expected at least one float value")
    return [float(v) for v in values]


def parse_int_list(raw: str) -> list[int]:
    values = [x.strip() for x in raw.split(",") if x.strip()]
    if not values:
        raise ValueError("Expected at least one integer value")
    return [int(v) for v in values]


def build_rungs(
    *,
    starting_bankroll: float,
    first_rung_offset: float,
    rung_step_offset: float,
    rung_count: int,
) -> list[float]:
    if starting_bankroll <= 0:
        raise ValueError("starting_bankroll must be positive")
    if first_rung_offset <= 0:
        raise ValueError("first_rung_offset must be positive")
    if rung_step_offset <= 0:
        raise ValueError("rung_step_offset must be positive")
    if rung_count <= 0:
        raise ValueError("rung_count must be positive")

    return [
        round(starting_bankroll + first_rung_offset + (i * rung_step_offset), 2)
        for i in range(rung_count)
    ]


def score_result(summary: dict, starting_bankroll: float, drawdown_penalty: float) -> float:
    return float(summary["net_profit_total_wealth"]) - (
        drawdown_penalty * float(summary["max_drawdown_total_wealth"]) * starting_bankroll
    )


def pareto_front(rows: list[dict]) -> list[dict]:
    """
    Maximize net profit and minimize drawdown.
    Keep rows not dominated by any other row.
    """
    if not rows:
        return []

    front: list[dict] = []
    for i, row_i in enumerate(rows):
        profit_i = float(row_i["net_profit_total_wealth"])
        drawdown_i = float(row_i["max_drawdown_total_wealth"])
        dominated = False

        for j, row_j in enumerate(rows):
            if i == j:
                continue
            profit_j = float(row_j["net_profit_total_wealth"])
            drawdown_j = float(row_j["max_drawdown_total_wealth"])

            not_worse = (profit_j >= profit_i) and (drawdown_j <= drawdown_i)
            strictly_better = (profit_j > profit_i) or (drawdown_j < drawdown_i)
            if not_worse and strictly_better:
                dominated = True
                break

        if not dominated:
            front.append(row_i)

    front.sort(
        key=lambda r: (float(r["net_profit_total_wealth"]), -float(r["max_drawdown_total_wealth"])),
        reverse=True,
    )

    unique_front: list[dict] = []
    seen: set[tuple[float, float]] = set()
    for row in front:
        key = (
            round(float(row["net_profit_total_wealth"]), 8),
            round(float(row["max_drawdown_total_wealth"]), 8),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_front.append(row)
    return unique_front


def _materialize_best_config(base_cfg: StrategyConfig, best_row: dict) -> dict:
    payload = asdict(base_cfg)
    payload["ladder_enabled"] = True
    payload["ladder_rungs"] = [float(x) for x in str(best_row["ladder_rungs"]).split("|")]
    payload["ladder_min_success_prob"] = float(best_row["min_success_prob"])
    payload["ladder_withdraw_step"] = float(best_row["withdraw_step"])
    payload["ladder_min_risk_wallet"] = float(best_row["min_risk_wallet"])
    payload["ladder_planning_p"] = float(best_row["planning_p"])
    return payload


def run_ladder_grid(
    *,
    candidates: list[BetCandidate],
    base_cfg: StrategyConfig,
    starting_bankroll: float,
    output_dir: str,
    first_rung_offsets: list[float],
    rung_step_offsets: list[float],
    rung_counts: list[int],
    min_success_probs: list[float],
    planning_ps: list[float],
    withdraw_steps: list[float],
    min_risk_wallets: list[float],
    drawdown_penalty: float,
    top_k: int,
    pareto_k: int,
) -> dict:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    if pareto_k <= 0:
        raise ValueError("pareto_k must be positive")
    if drawdown_penalty < 0:
        raise ValueError("drawdown_penalty cannot be negative")

    attempts = 0
    failures = 0
    rows: list[dict] = []

    combos = product(
        first_rung_offsets,
        rung_step_offsets,
        rung_counts,
        min_success_probs,
        planning_ps,
        withdraw_steps,
        min_risk_wallets,
    )
    for (
        first_rung_offset,
        rung_step_offset,
        rung_count,
        min_success_prob,
        planning_p,
        withdraw_step,
        min_risk_wallet,
    ) in combos:
        attempts += 1
        try:
            rungs = build_rungs(
                starting_bankroll=starting_bankroll,
                first_rung_offset=first_rung_offset,
                rung_step_offset=rung_step_offset,
                rung_count=rung_count,
            )
            cfg = replace(
                base_cfg,
                ladder_enabled=True,
                ladder_rungs=rungs,
                ladder_min_success_prob=min_success_prob,
                ladder_withdraw_step=withdraw_step,
                ladder_min_risk_wallet=min_risk_wallet,
                ladder_planning_p=planning_p,
            )
            summary = run_backtest(
                candidates=candidates,
                cfg=cfg,
                starting_bankroll=starting_bankroll,
                output_dir=output_dir,
                persist_outputs=False,
            )
            score = score_result(
                summary=summary,
                starting_bankroll=starting_bankroll,
                drawdown_penalty=drawdown_penalty,
            )
            rows.append(
                {
                    "score": round(score, 6),
                    "first_rung_offset": first_rung_offset,
                    "rung_step_offset": rung_step_offset,
                    "rung_count": rung_count,
                    "min_success_prob": min_success_prob,
                    "planning_p": planning_p,
                    "withdraw_step": withdraw_step,
                    "min_risk_wallet": min_risk_wallet,
                    "ladder_rungs": "|".join(f"{x:.2f}" for x in rungs),
                    "final_bankroll": summary["final_bankroll"],
                    "final_locked_vault": summary["final_locked_vault"],
                    "final_total_wealth": summary["final_total_wealth"],
                    "net_profit_total_wealth": summary["net_profit_total_wealth"],
                    "max_drawdown_total_wealth": summary["max_drawdown_total_wealth"],
                    "bets_accepted": summary["bets_accepted"],
                    "roi_on_staked": summary["roi_on_staked"],
                    "ladder_events_count": summary["ladder_events_count"],
                }
            )
        except Exception:
            failures += 1
            continue

    rows.sort(key=lambda x: x["score"], reverse=True)
    best_rows = rows[:top_k]
    full_front = pareto_front(rows)
    front_rows = full_front[:pareto_k]

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"ladder_grid_results_{stamp}.csv"
    json_path = out_dir / f"ladder_grid_summary_{stamp}.json"
    pareto_csv_path = out_dir / f"ladder_grid_pareto_{stamp}.csv"
    best_cfg_path = out_dir / f"best_ladder_config_{stamp}.json"

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "score",
                "first_rung_offset",
                "rung_step_offset",
                "rung_count",
                "min_success_prob",
                "planning_p",
                "withdraw_step",
                "min_risk_wallet",
                "ladder_rungs",
                "final_bankroll",
                "final_locked_vault",
                "final_total_wealth",
                "net_profit_total_wealth",
                "max_drawdown_total_wealth",
                "bets_accepted",
                "roi_on_staked",
                "ladder_events_count",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["score"],
                    row["first_rung_offset"],
                    row["rung_step_offset"],
                    row["rung_count"],
                    row["min_success_prob"],
                    row["planning_p"],
                    row["withdraw_step"],
                    row["min_risk_wallet"],
                    row["ladder_rungs"],
                    row["final_bankroll"],
                    row["final_locked_vault"],
                    row["final_total_wealth"],
                    row["net_profit_total_wealth"],
                    row["max_drawdown_total_wealth"],
                    row["bets_accepted"],
                    row["roi_on_staked"],
                    row["ladder_events_count"],
                ]
            )

    with pareto_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "score",
                "first_rung_offset",
                "rung_step_offset",
                "rung_count",
                "min_success_prob",
                "planning_p",
                "withdraw_step",
                "min_risk_wallet",
                "ladder_rungs",
                "final_bankroll",
                "final_locked_vault",
                "final_total_wealth",
                "net_profit_total_wealth",
                "max_drawdown_total_wealth",
                "bets_accepted",
                "roi_on_staked",
                "ladder_events_count",
            ]
        )
        for row in front_rows:
            writer.writerow(
                [
                    row["score"],
                    row["first_rung_offset"],
                    row["rung_step_offset"],
                    row["rung_count"],
                    row["min_success_prob"],
                    row["planning_p"],
                    row["withdraw_step"],
                    row["min_risk_wallet"],
                    row["ladder_rungs"],
                    row["final_bankroll"],
                    row["final_locked_vault"],
                    row["final_total_wealth"],
                    row["net_profit_total_wealth"],
                    row["max_drawdown_total_wealth"],
                    row["bets_accepted"],
                    row["roi_on_staked"],
                    row["ladder_events_count"],
                ]
            )

    best_cfg_payload = None
    if best_rows:
        best_cfg_payload = _materialize_best_config(base_cfg, best_rows[0])
        best_cfg_path.write_text(json.dumps(best_cfg_payload, indent=2), encoding="utf-8")

    summary = {
        "analysis_timestamp": datetime.now().isoformat(),
        "starting_bankroll": starting_bankroll,
        "drawdown_penalty": drawdown_penalty,
        "runs_attempted": attempts,
        "runs_completed": len(rows),
        "runs_failed": failures,
        "top_k": top_k,
        "pareto_k": pareto_k,
        "best_result": best_rows[0] if best_rows else None,
        "top_results": best_rows,
        "pareto_front_count": len(full_front),
        "pareto_front": front_rows,
        "best_config": best_cfg_payload,
        "results_csv": str(csv_path),
        "pareto_csv": str(pareto_csv_path),
        "best_config_json": str(best_cfg_path) if best_cfg_payload else None,
        "summary_json": str(json_path),
    }
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
