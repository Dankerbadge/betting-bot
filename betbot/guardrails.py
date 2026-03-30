from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from betbot.config import StrategyConfig


@dataclass
class GuardrailState:
    day: date
    day_start_bankroll: float
    day_realized_pnl: float
    peak_bankroll: float
    current_bankroll: float

    @classmethod
    def initialize(cls, trading_day: date, starting_bankroll: float) -> "GuardrailState":
        return cls(
            day=trading_day,
            day_start_bankroll=starting_bankroll,
            day_realized_pnl=0.0,
            peak_bankroll=starting_bankroll,
            current_bankroll=starting_bankroll,
        )


def roll_day_if_needed(state: GuardrailState, new_day: date) -> GuardrailState:
    if state.day == new_day:
        return state
    return GuardrailState(
        day=new_day,
        day_start_bankroll=state.current_bankroll,
        day_realized_pnl=0.0,
        peak_bankroll=state.peak_bankroll,
        current_bankroll=state.current_bankroll,
    )


def check_pre_trade_limits(state: GuardrailState, stake: float, cfg: StrategyConfig) -> tuple[bool, str]:
    if stake <= 0:
        return False, "stake_non_positive"
    if stake > state.current_bankroll:
        return False, "stake_exceeds_bankroll"

    daily_loss_limit = state.day_start_bankroll * cfg.max_daily_loss_fraction
    projected_daily_pnl = state.day_realized_pnl - stake
    if projected_daily_pnl < -daily_loss_limit:
        return False, "daily_loss_limit"

    if state.peak_bankroll > 0:
        drawdown = (state.peak_bankroll - state.current_bankroll) / state.peak_bankroll
        if drawdown >= cfg.max_drawdown_fraction:
            return False, "drawdown_limit"

    return True, "ok"


def apply_settlement(state: GuardrailState, pnl: float) -> GuardrailState:
    new_bankroll = state.current_bankroll + pnl
    return GuardrailState(
        day=state.day,
        day_start_bankroll=state.day_start_bankroll,
        day_realized_pnl=state.day_realized_pnl + pnl,
        peak_bankroll=max(state.peak_bankroll, new_bankroll),
        current_bankroll=new_bankroll,
    )


def apply_transfer_out(state: GuardrailState, amount: float) -> GuardrailState:
    """
    Move capital out of the risk wallet without counting as realized trading PnL.
    Keeps guardrail references aligned to avoid false drawdown/daily-loss triggers.
    """
    if amount < 0:
        raise ValueError("Transfer amount cannot be negative")
    if amount > state.current_bankroll:
        raise ValueError("Transfer amount cannot exceed current bankroll")
    if amount == 0:
        return state

    new_current = state.current_bankroll - amount
    new_day_start = max(0.0, state.day_start_bankroll - amount)
    adjusted_peak = max(new_current, state.peak_bankroll - amount)

    return GuardrailState(
        day=state.day,
        day_start_bankroll=new_day_start,
        day_realized_pnl=state.day_realized_pnl,
        peak_bankroll=adjusted_peak,
        current_bankroll=new_current,
    )
