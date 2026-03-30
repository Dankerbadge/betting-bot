from __future__ import annotations

from dataclasses import dataclass

from betbot.config import StrategyConfig
from betbot.edge import full_kelly_fraction


@dataclass(frozen=True)
class StakeResult:
    full_kelly: float
    used_fraction: float
    stake: float


def _confidence_scaled_fraction(cfg: StrategyConfig, confidence: float | None) -> float:
    if not cfg.dynamic_kelly_enabled or not isinstance(confidence, (int, float)):
        return cfg.kelly_fraction
    lo = min(cfg.dynamic_kelly_confidence_floor, cfg.dynamic_kelly_confidence_ceiling)
    hi = max(cfg.dynamic_kelly_confidence_floor, cfg.dynamic_kelly_confidence_ceiling)
    bounded_conf = min(max(float(confidence), lo), hi)
    span = hi - lo
    ratio = ((bounded_conf - lo) / span) if span > 0 else 1.0
    min_frac = min(cfg.dynamic_kelly_min_fraction, cfg.dynamic_kelly_max_fraction)
    max_frac = max(cfg.dynamic_kelly_min_fraction, cfg.dynamic_kelly_max_fraction)
    return min_frac + ratio * (max_frac - min_frac)


def propose_stake(
    bankroll: float,
    model_prob: float,
    odds: float,
    cfg: StrategyConfig,
    *,
    confidence: float | None = None,
) -> StakeResult:
    if bankroll <= 0:
        return StakeResult(full_kelly=0.0, used_fraction=0.0, stake=0.0)

    kelly_full = full_kelly_fraction(model_prob, odds)
    bounded_full = max(0.0, min(kelly_full, 1.0))
    effective_fraction = _confidence_scaled_fraction(cfg, confidence)
    fractional = bounded_full * effective_fraction
    used_fraction = min(fractional, cfg.max_bet_fraction)
    stake = bankroll * used_fraction

    if stake < cfg.min_stake:
        return StakeResult(full_kelly=kelly_full, used_fraction=used_fraction, stake=0.0)

    stake = min(stake, bankroll)
    return StakeResult(full_kelly=kelly_full, used_fraction=used_fraction, stake=stake)


def settle_pnl(stake: float, odds: float, outcome: int) -> float:
    if outcome not in (0, 1):
        raise ValueError("Outcome must be 0 or 1")
    if outcome == 1:
        return stake * (odds - 1.0)
    return -stake
