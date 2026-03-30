from __future__ import annotations

from dataclasses import dataclass
import random
from statistics import mean


@dataclass(frozen=True)
class BetaPosterior:
    alpha: float
    beta: float
    wins: int
    trials: int

    @property
    def posterior_mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)


def build_beta_posterior(
    wins: int,
    trials: int,
    prior_alpha: float = 0.5,
    prior_beta: float = 0.5,
) -> BetaPosterior:
    if trials < 0 or wins < 0:
        raise ValueError("wins and trials must be non-negative")
    if wins > trials:
        raise ValueError("wins cannot exceed trials")
    if prior_alpha <= 0 or prior_beta <= 0:
        raise ValueError("prior_alpha and prior_beta must be positive")

    losses = trials - wins
    return BetaPosterior(
        alpha=prior_alpha + wins,
        beta=prior_beta + losses,
        wins=wins,
        trials=trials,
    )


def beta_credible_bounds_mc(
    posterior: BetaPosterior,
    confidence: float = 0.95,
    samples: int = 200_000,
    seed: int = 7,
) -> tuple[float, float]:
    """
    Monte Carlo equal-tailed credible interval for Beta posterior.
    Uses deterministic seed for reproducibility.
    """
    if not (0.0 < confidence < 1.0):
        raise ValueError("confidence must be in (0,1)")
    if samples < 10_000:
        raise ValueError("samples must be >= 10_000 for stable intervals")

    rng = random.Random(seed)
    draws = [rng.betavariate(posterior.alpha, posterior.beta) for _ in range(samples)]
    draws.sort()

    alpha_tail = (1.0 - confidence) / 2.0
    low_idx = int(alpha_tail * (samples - 1))
    high_idx = int((1.0 - alpha_tail) * (samples - 1))
    return draws[low_idx], draws[high_idx]


def conservative_planning_p(
    wins: int,
    trials: int,
    confidence: float = 0.95,
    prior_alpha: float = 0.5,
    prior_beta: float = 0.5,
) -> dict:
    posterior = build_beta_posterior(
        wins=wins,
        trials=trials,
        prior_alpha=prior_alpha,
        prior_beta=prior_beta,
    )
    low, high = beta_credible_bounds_mc(posterior=posterior, confidence=confidence)
    return {
        "wins": wins,
        "trials": trials,
        "posterior_alpha": posterior.alpha,
        "posterior_beta": posterior.beta,
        "posterior_mean": round(posterior.posterior_mean, 6),
        "credible_low": round(low, 6),
        "credible_high": round(high, 6),
        "recommended_planning_p": round(low, 6),
        "confidence": confidence,
    }


def summarize_outcomes(outcomes: list[int]) -> dict:
    if not outcomes:
        return {"trials": 0, "wins": 0, "win_rate": 0.0}
    for item in outcomes:
        if item not in (0, 1):
            raise ValueError("Outcomes must be 0/1")
    wins = sum(outcomes)
    return {"trials": len(outcomes), "wins": wins, "win_rate": round(mean(outcomes), 6)}
