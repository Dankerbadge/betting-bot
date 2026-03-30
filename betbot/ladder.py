from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math

from betbot.probability_path import ladder_option_probability


@dataclass(frozen=True)
class LadderPolicy:
    enabled: bool
    rungs: tuple[float, ...]
    min_success_prob: float
    withdraw_step: float
    min_risk_wallet: float
    risk_per_effort: float
    planning_p: float


@dataclass
class LadderState:
    locked_vault: float
    next_rung_idx: int

    @classmethod
    def initialize(cls, starting_total_wealth: float, rungs: tuple[float, ...]) -> "LadderState":
        idx = 0
        while idx < len(rungs) and starting_total_wealth >= rungs[idx]:
            idx += 1
        return cls(locked_vault=0.0, next_rung_idx=idx)


@dataclass(frozen=True)
class LadderEvent:
    timestamp: datetime
    rung_reached: float
    next_target: float | None
    total_wealth: float
    risk_wallet_before: float
    risk_wallet_after: float
    locked_vault_before: float
    locked_vault_after: float
    withdrawn: float
    planning_p: float
    success_probability_after: float | None
    min_success_required: float
    reason: str


def validate_ladder_policy(policy: LadderPolicy) -> None:
    if not policy.enabled:
        return
    if not policy.rungs:
        raise ValueError("ladder_rungs must be provided when ladder_enabled is true")
    if policy.withdraw_step <= 0:
        raise ValueError("ladder_withdraw_step must be positive")
    if policy.min_risk_wallet < 0:
        raise ValueError("ladder_min_risk_wallet cannot be negative")
    if policy.risk_per_effort <= 0:
        raise ValueError("ladder_risk_per_effort must be positive")
    if not (0.0 <= policy.planning_p <= 1.0):
        raise ValueError("ladder_planning_p must be in [0,1]")
    if not (0.0 <= policy.min_success_prob <= 1.0):
        raise ValueError("ladder_min_success_prob must be in [0,1]")
    prev = -math.inf
    for rung in policy.rungs:
        if rung <= 0:
            raise ValueError("All ladder rungs must be positive")
        if rung <= prev:
            raise ValueError("ladder_rungs must be strictly increasing")
        prev = rung


def build_policy(
    *,
    enabled: bool,
    rungs: list[float] | None,
    min_success_prob: float,
    withdraw_step: float,
    min_risk_wallet: float,
    risk_per_effort: float,
    planning_p: float,
) -> LadderPolicy:
    tuple_rungs = tuple(rungs or [])
    policy = LadderPolicy(
        enabled=enabled,
        rungs=tuple_rungs,
        min_success_prob=min_success_prob,
        withdraw_step=withdraw_step,
        min_risk_wallet=min_risk_wallet,
        risk_per_effort=risk_per_effort,
        planning_p=planning_p,
    )
    validate_ladder_policy(policy)
    return policy


def _withdraw_candidates(max_withdrawable: float, step: float) -> list[float]:
    if max_withdrawable <= 0:
        return [0.0]
    count = int(max_withdrawable // step)
    values = [round(i * step, 2) for i in range(count + 1)]
    if abs(values[-1] - max_withdrawable) > 1e-9:
        values.append(round(max_withdrawable, 2))
    return values


def choose_withdrawal(
    *,
    current_total_wealth: float,
    locked_vault: float,
    next_target: float,
    risk_wallet: float,
    policy: LadderPolicy,
) -> tuple[float, float]:
    max_withdrawable = max(0.0, risk_wallet - policy.min_risk_wallet)
    candidates = _withdraw_candidates(max_withdrawable, policy.withdraw_step)

    best_withdrawal = 0.0
    best_probability = ladder_option_probability(
        current_total_wealth=current_total_wealth,
        locked_vault=locked_vault,
        withdraw_now=0.0,
        target_total_wealth=next_target,
        risk_per_effort=policy.risk_per_effort,
        p=policy.planning_p,
    )

    for amount in candidates:
        probability = ladder_option_probability(
            current_total_wealth=current_total_wealth,
            locked_vault=locked_vault,
            withdraw_now=amount,
            target_total_wealth=next_target,
            risk_per_effort=policy.risk_per_effort,
            p=policy.planning_p,
        )
        if probability >= policy.min_success_prob and amount >= best_withdrawal:
            best_withdrawal = amount
            best_probability = probability

    return best_withdrawal, best_probability
