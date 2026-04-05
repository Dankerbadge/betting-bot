from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


DEFAULT_FEE_RATE: float = 0.07
DEFAULT_KELLY_FRACTION: float = 0.25
DEFAULT_MIN_KELLY_USED: float = 0.002
DEFAULT_MIN_EDGE_NET_FEES: float = 0.01


@dataclass
class KellyConfig:
    fee_rate: float = DEFAULT_FEE_RATE
    kelly_fraction: float = DEFAULT_KELLY_FRACTION
    min_kelly_used: float = DEFAULT_MIN_KELLY_USED
    min_edge_net_fees: float = DEFAULT_MIN_EDGE_NET_FEES
    min_fair_prob: float = 0.0
    max_kelly_full: float = 1.0


@dataclass
class KellyCandidate:
    ticker: str
    side: str
    price: float
    fair_prob: float
    lane: str
    edge_gross: float
    edge_net_fees: float
    kelly_full: float = 0.0
    kelly_used: float = 0.0
    kelly_dollar: float = 0.0
    kelly_ev_per_dollar: float = 0.0
    kelly_rank_score: float = 0.0
    kelly_reject_reason: Optional[str] = None


def binary_kelly_fee_adjusted(
    price: float,
    fair_prob: float,
    fee_rate: float = DEFAULT_FEE_RATE,
) -> tuple[float, float]:
    if not (0.0 < price < 1.0):
        raise ValueError(f"price must be in (0,1), got {price}")
    if not (0.0 <= fair_prob <= 1.0):
        raise ValueError(f"fair_prob must be in [0,1], got {fair_prob}")
    if not (0.0 <= fee_rate < 1.0):
        raise ValueError(f"fee_rate must be in [0,1), got {fee_rate}")

    net_win = (1.0 - price) * (1.0 - fee_rate)
    net_loss = price
    f_star = (fair_prob * net_win - (1.0 - fair_prob) * net_loss) / net_win
    contracts_per_dollar = 1.0 / price
    ev_per_dollar = contracts_per_dollar * (
        fair_prob * net_win - (1.0 - fair_prob) * net_loss
    )
    return f_star, ev_per_dollar


def score_candidate(
    *,
    price: float,
    fair_prob: float,
    edge_net_fees: float,
    bankroll: float,
    config: KellyConfig,
    ticker: str = "",
    side: str = "yes",
    lane: str = "kelly_unified",
    edge_gross: float = 0.0,
) -> KellyCandidate:
    f_star, ev_per_dollar = binary_kelly_fee_adjusted(price, fair_prob, config.fee_rate)
    f_clamped = min(f_star, config.max_kelly_full)
    f_used = config.kelly_fraction * max(f_clamped, 0.0)
    kelly_dollar = f_used * bankroll

    candidate = KellyCandidate(
        ticker=ticker,
        side=side,
        price=price,
        fair_prob=fair_prob,
        lane=lane,
        edge_gross=edge_gross,
        edge_net_fees=edge_net_fees,
        kelly_full=f_star,
        kelly_used=f_used,
        kelly_dollar=kelly_dollar,
        kelly_ev_per_dollar=ev_per_dollar,
        kelly_rank_score=f_used,
    )

    if config.min_fair_prob > 0 and fair_prob < config.min_fair_prob:
        candidate.kelly_reject_reason = (
            f"probability_too_low ({fair_prob:.4f} < {config.min_fair_prob:.4f})"
        )
    elif edge_net_fees < config.min_edge_net_fees:
        candidate.kelly_reject_reason = (
            f"edge_net_fees_too_low ({edge_net_fees:.4f} < {config.min_edge_net_fees:.4f})"
        )
    elif f_star <= 0.0:
        candidate.kelly_reject_reason = f"negative_kelly ({f_star:.6f})"
    elif f_used < config.min_kelly_used:
        candidate.kelly_reject_reason = (
            f"kelly_used_too_small ({f_used:.6f} < {config.min_kelly_used:.6f})"
        )

    return candidate


def kelly_dollar_stake(
    *,
    f_used: float,
    bankroll: float,
    max_bet_fraction: float = 0.10,
    min_stake: float = 1.00,
    max_stake: float = 500.0,
) -> float:
    if f_used <= 0.0 or bankroll <= 0.0:
        return 0.0
    raw = f_used * bankroll
    clipped = min(raw, max_bet_fraction * bankroll, max_stake)
    return clipped if clipped >= min_stake else 0.0
