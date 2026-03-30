from __future__ import annotations

from math import sqrt
from statistics import mean, pstdev

from betbot.types import Decision


def max_drawdown(bankroll_series: list[float]) -> float:
    if not bankroll_series:
        return 0.0
    peak = bankroll_series[0]
    max_dd = 0.0
    for value in bankroll_series:
        peak = max(peak, value)
        if peak > 0:
            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)
    return max_dd


def sharpe_ratio(per_bet_returns: list[float]) -> float:
    if len(per_bet_returns) < 2:
        return 0.0
    sigma = pstdev(per_bet_returns)
    if sigma == 0:
        return 0.0
    return mean(per_bet_returns) / sigma * sqrt(len(per_bet_returns))


def average_clv_bps(decisions: list[Decision]) -> float | None:
    values: list[float] = []
    for d in decisions:
        if d.status != "accepted":
            continue
        if d.closing_odds is None:
            continue
        open_ip = 1.0 / d.odds
        close_ip = 1.0 / d.closing_odds
        values.append((close_ip - open_ip) * 10_000.0)
    if not values:
        return None
    return mean(values)

