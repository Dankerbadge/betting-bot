from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from statistics import mean

from betbot.config import StrategyConfig
from betbot.edge import expected_value_decimal
from betbot.guardrails import (
    GuardrailState,
    apply_settlement,
    apply_transfer_out,
    check_pre_trade_limits,
    roll_day_if_needed,
)
from betbot.io import write_decisions, write_ladder_events
from betbot.ladder import LadderEvent, LadderState, build_policy, choose_withdrawal
from betbot.risk import propose_stake, settle_pnl
from betbot.types import BetCandidate, Decision


def run_paper(
    candidates: list[BetCandidate],
    cfg: StrategyConfig,
    starting_bankroll: float,
    output_dir: str = "outputs",
    simulate_with_outcomes: bool = False,
) -> dict:
    if starting_bankroll <= 0:
        raise ValueError("starting_bankroll must be positive")
    if not candidates:
        raise ValueError("No candidates supplied")
    candidates = sorted(
        candidates,
        key=lambda c: (
            c.timestamp,
            -(c.edge_rank_score if c.edge_rank_score is not None else float("-inf")),
            c.event_id,
            c.selection,
        ),
    )

    state = GuardrailState.initialize(candidates[0].timestamp.date(), starting_bankroll)
    decisions: list[Decision] = []
    ladder_events: list[LadderEvent] = []
    accepted_evs: list[float] = []
    accepted_count = 0
    simulated_settled = 0
    simulated_pnl_total = 0.0

    planning_p = (
        cfg.ladder_planning_p
        if cfg.ladder_planning_p is not None
        else (cfg.planning_prob_floor if cfg.planning_prob_floor is not None else 0.5)
    )
    ladder_policy = build_policy(
        enabled=cfg.ladder_enabled,
        rungs=cfg.ladder_rungs,
        min_success_prob=cfg.ladder_min_success_prob,
        withdraw_step=cfg.ladder_withdraw_step,
        min_risk_wallet=cfg.ladder_min_risk_wallet,
        risk_per_effort=cfg.ladder_risk_per_effort,
        planning_p=planning_p,
    )
    ladder_state = (
        LadderState.initialize(starting_bankroll, ladder_policy.rungs)
        if ladder_policy.enabled
        else LadderState(locked_vault=0.0, next_rung_idx=0)
    )

    def apply_ladder(timestamp: datetime) -> None:
        nonlocal state, ladder_state
        if not ladder_policy.enabled:
            return

        while ladder_state.next_rung_idx < len(ladder_policy.rungs):
            rung_reached = ladder_policy.rungs[ladder_state.next_rung_idx]
            total_wealth = state.current_bankroll + ladder_state.locked_vault
            if total_wealth + 1e-9 < rung_reached:
                break

            next_target = (
                ladder_policy.rungs[ladder_state.next_rung_idx + 1]
                if ladder_state.next_rung_idx + 1 < len(ladder_policy.rungs)
                else None
            )

            risk_before = state.current_bankroll
            vault_before = ladder_state.locked_vault
            withdrawn = 0.0
            success_probability_after: float | None = None
            reason = "rung_reached_no_withdrawal"

            if next_target is None:
                max_withdrawable = max(0.0, risk_before - ladder_policy.min_risk_wallet)
                withdrawn = round(max_withdrawable, 2)
                if withdrawn > 0:
                    state = apply_transfer_out(state, withdrawn)
                    ladder_state.locked_vault = round(ladder_state.locked_vault + withdrawn, 2)
                    reason = "final_rung_lock_in"
                else:
                    reason = "final_rung_no_withdrawable_balance"
            else:
                withdrawn, success_probability_after = choose_withdrawal(
                    current_total_wealth=total_wealth,
                    locked_vault=ladder_state.locked_vault,
                    next_target=next_target,
                    risk_wallet=state.current_bankroll,
                    policy=ladder_policy,
                )
                withdrawn = round(withdrawn, 2)
                if withdrawn > 0:
                    state = apply_transfer_out(state, withdrawn)
                    ladder_state.locked_vault = round(ladder_state.locked_vault + withdrawn, 2)
                    reason = "withdrawal_applied"
                else:
                    reason = "no_withdrawal_threshold_or_optimal"

            ladder_events.append(
                LadderEvent(
                    timestamp=timestamp,
                    rung_reached=rung_reached,
                    next_target=next_target,
                    total_wealth=round(total_wealth, 2),
                    risk_wallet_before=round(risk_before, 2),
                    risk_wallet_after=round(state.current_bankroll, 2),
                    locked_vault_before=round(vault_before, 2),
                    locked_vault_after=round(ladder_state.locked_vault, 2),
                    withdrawn=withdrawn,
                    planning_p=ladder_policy.planning_p,
                    success_probability_after=success_probability_after,
                    min_success_required=ladder_policy.min_success_prob,
                    reason=reason,
                )
            )
            ladder_state.next_rung_idx += 1

    apply_ladder(candidates[0].timestamp)

    for c in candidates:
        state = roll_day_if_needed(state, c.timestamp.date())
        bankroll_before = state.current_bankroll
        decision_prob = c.decision_prob if c.decision_prob is not None else c.model_prob
        if cfg.planning_prob_floor is not None:
            decision_prob = min(decision_prob, cfg.planning_prob_floor)
        ev = expected_value_decimal(decision_prob, c.odds)

        if ev < cfg.min_ev:
            decisions.append(
                Decision(
                    timestamp=c.timestamp,
                    event_id=c.event_id,
                    selection=c.selection,
                    odds=c.odds,
                    model_prob=c.model_prob,
                    decision_prob=decision_prob,
                    ev=ev,
                    kelly_full=0.0,
                    kelly_used=0.0,
                    stake=0.0,
                    status="rejected",
                    reason="ev_below_threshold",
                    bankroll_before=bankroll_before,
                    bankroll_after=state.current_bankroll,
                    pnl=0.0,
                    closing_odds=c.closing_odds,
                    outcome=c.outcome,
                )
            )
            continue

        stake_result = propose_stake(state.current_bankroll, decision_prob, c.odds, cfg)
        if stake_result.stake <= 0:
            decisions.append(
                Decision(
                    timestamp=c.timestamp,
                    event_id=c.event_id,
                    selection=c.selection,
                    odds=c.odds,
                    model_prob=c.model_prob,
                    decision_prob=decision_prob,
                    ev=ev,
                    kelly_full=stake_result.full_kelly,
                    kelly_used=stake_result.used_fraction,
                    stake=0.0,
                    status="rejected",
                    reason="stake_below_min",
                    bankroll_before=bankroll_before,
                    bankroll_after=state.current_bankroll,
                    pnl=0.0,
                    closing_odds=c.closing_odds,
                    outcome=c.outcome,
                )
            )
            continue

        allowed, reason = check_pre_trade_limits(state, stake_result.stake, cfg)
        if not allowed:
            decisions.append(
                Decision(
                    timestamp=c.timestamp,
                    event_id=c.event_id,
                    selection=c.selection,
                    odds=c.odds,
                    model_prob=c.model_prob,
                    decision_prob=decision_prob,
                    ev=ev,
                    kelly_full=stake_result.full_kelly,
                    kelly_used=stake_result.used_fraction,
                    stake=stake_result.stake,
                    status="rejected",
                    reason=reason,
                    bankroll_before=bankroll_before,
                    bankroll_after=state.current_bankroll,
                    pnl=0.0,
                    closing_odds=c.closing_odds,
                    outcome=c.outcome,
                )
            )
            continue

        accepted_count += 1
        accepted_evs.append(ev)
        pnl = 0.0
        bankroll_after = state.current_bankroll
        decision_reason = "accepted"

        if simulate_with_outcomes and c.outcome in (0, 1):
            pnl = settle_pnl(stake_result.stake, c.odds, c.outcome)
            state = apply_settlement(state, pnl)
            apply_ladder(c.timestamp)
            bankroll_after = state.current_bankroll
            simulated_settled += 1
            simulated_pnl_total += pnl
            decision_reason = "accepted_simulated_settlement"

        decisions.append(
            Decision(
                timestamp=c.timestamp,
                event_id=c.event_id,
                selection=c.selection,
                odds=c.odds,
                model_prob=c.model_prob,
                decision_prob=decision_prob,
                ev=ev,
                kelly_full=stake_result.full_kelly,
                kelly_used=stake_result.used_fraction,
                stake=stake_result.stake,
                status="accepted",
                reason=decision_reason,
                bankroll_before=bankroll_before,
                bankroll_after=bankroll_after,
                pnl=pnl,
                closing_odds=c.closing_odds,
                outcome=c.outcome,
            )
        )

    final_total_wealth = state.current_bankroll + ladder_state.locked_vault
    summary = {
        "starting_bankroll": round(starting_bankroll, 2),
        "bankroll_end_of_session": round(state.current_bankroll, 2),
        "locked_vault_end_of_session": round(ladder_state.locked_vault, 2),
        "total_wealth_end_of_session": round(final_total_wealth, 2),
        "candidates_seen": len(candidates),
        "accepted": accepted_count,
        "rejected": len(candidates) - accepted_count,
        "avg_ev_accepted": round(mean(accepted_evs), 6) if accepted_evs else 0.0,
        "simulated_settled_count": simulated_settled,
        "simulated_pnl_total": round(simulated_pnl_total, 2),
        "ladder_enabled": ladder_policy.enabled,
        "ladder_planning_p": ladder_policy.planning_p,
        "ladder_events_count": len(ladder_events),
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    decisions_path = out_dir / f"paper_decisions_{stamp}.csv"
    ladder_events_path = out_dir / f"paper_ladder_events_{stamp}.csv"
    summary_path = out_dir / f"paper_summary_{stamp}.json"
    write_decisions(decisions_path, decisions)
    write_ladder_events(ladder_events_path, ladder_events)
    summary["output_decisions_csv"] = str(decisions_path)
    summary["output_ladder_csv"] = str(ladder_events_path)
    summary["output_file"] = str(summary_path)
    summary_path.write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    return summary
