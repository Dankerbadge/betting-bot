from __future__ import annotations

import math


def hitting_probability(start_units: int, target_units: int, p: float) -> float:
    """
    Probability of hitting target_units before 0 from start_units
    in a +/-1 random walk with up-probability p.
    """
    if not (0.0 <= p <= 1.0):
        raise ValueError("p must be in [0,1]")
    if target_units <= 0:
        raise ValueError("target_units must be positive")
    if not (0 <= start_units <= target_units):
        raise ValueError("start_units must be between 0 and target_units")

    if start_units == 0:
        return 0.0
    if start_units == target_units:
        return 1.0

    q = 1.0 - p
    if abs(p - q) < 1e-12:
        return start_units / target_units

    ratio = q / p
    numerator = 1.0 - math.pow(ratio, start_units)
    denominator = 1.0 - math.pow(ratio, target_units)
    return numerator / denominator


def eventual_success_probability(start_units: int, p: float) -> float:
    """
    Unbounded-target success probability:
    P(avoid eventual ruin) for biased walk.
    """
    if start_units < 0:
        raise ValueError("start_units must be non-negative")
    if not (0.0 <= p <= 1.0):
        raise ValueError("p must be in [0,1]")
    if start_units == 0:
        return 0.0
    if p <= 0.5:
        return 0.0

    q = 1.0 - p
    return 1.0 - math.pow(q / p, start_units)


def required_starting_units(target_success_prob: float, p: float) -> int | None:
    """
    Minimum start units to meet an eventual success target in unbounded-goal model.
    Returns None if not achievable (p <= 0.5).
    """
    if not (0.0 < target_success_prob < 1.0):
        raise ValueError("target_success_prob must be in (0,1)")
    if not (0.0 <= p <= 1.0):
        raise ValueError("p must be in [0,1]")
    if p <= 0.5:
        return None

    q = 1.0 - p
    ratio = q / p
    # Need: 1 - ratio^i >= target  => ratio^i <= (1-target)
    min_units = math.log(1.0 - target_success_prob) / math.log(ratio)
    return int(math.ceil(min_units))


def units_from_dollars(amount: float, risk_per_effort: float) -> int:
    if risk_per_effort <= 0:
        raise ValueError("risk_per_effort must be positive")
    if amount < 0:
        raise ValueError("amount cannot be negative")
    return int(round(amount / risk_per_effort))


def ladder_option_probability(
    current_total_wealth: float,
    locked_vault: float,
    withdraw_now: float,
    target_total_wealth: float,
    risk_per_effort: float,
    p: float,
) -> float:
    if current_total_wealth < 0:
        raise ValueError("current_total_wealth cannot be negative")
    if locked_vault < 0:
        raise ValueError("locked_vault cannot be negative")
    if withdraw_now < 0:
        raise ValueError("withdraw_now cannot be negative")
    if target_total_wealth <= current_total_wealth:
        return 1.0

    risk_wallet = current_total_wealth - locked_vault - withdraw_now
    if risk_wallet <= 0:
        return 0.0

    target_wallet = target_total_wealth - (locked_vault + withdraw_now)
    if target_wallet <= 0:
        return 1.0

    start_units = units_from_dollars(risk_wallet, risk_per_effort)
    target_units = units_from_dollars(target_wallet, risk_per_effort)
    if target_units <= 0:
        return 1.0
    start_units = max(0, min(start_units, target_units))
    return hitting_probability(start_units, target_units, p)

