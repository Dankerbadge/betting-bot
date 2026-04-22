from __future__ import annotations

from math import isfinite
from statistics import median, pstdev


def expected_value_decimal(model_prob: float, odds: float) -> float:
    """Expected profit per $1 staked using decimal odds."""
    return (model_prob * odds) - 1.0


def probability_from_expected_value_decimal(expected_value: float, odds: float) -> float:
    """Invert expected_value_decimal back into an implied win probability."""
    if odds <= 1.0:
        raise ValueError("Decimal odds must be > 1.0")
    probability = (expected_value + 1.0) / odds
    return min(1.0, max(0.0, probability))


def full_kelly_fraction(model_prob: float, odds: float) -> float:
    """Return unconstrained Kelly fraction for decimal odds."""
    if odds <= 1.0:
        raise ValueError("Decimal odds must be > 1.0")
    b = odds - 1.0
    q = 1.0 - model_prob
    return ((b * model_prob) - q) / b


def normalize_implied_probabilities(decimal_odds: list[float]) -> list[float]:
    """Remove vig with a power de-vig that better handles favorite-longshot skew."""
    if not decimal_odds:
        raise ValueError("decimal_odds cannot be empty")

    implied = []
    for odd in decimal_odds:
        try:
            odd_value = float(odd)
        except (TypeError, ValueError):
            raise ValueError("All odds must be finite and > 1.0") from None
        if not isfinite(odd_value) or odd_value <= 1.0:
            raise ValueError("All odds must be finite and > 1.0")
        implied.append(1.0 / odd_value)

    total = sum(implied)
    if not isfinite(total) or total <= 0:
        raise ValueError("Invalid implied probability total")
    if len(implied) == 1:
        return [1.0]
    if abs(total - 1.0) <= 1e-12:
        return implied

    def powered_total(exponent: float) -> float:
        return sum(probability**exponent for probability in implied)

    lower = 0.0
    upper = 1.0
    while powered_total(upper) > 1.0:
        upper *= 2.0
        if upper > 1024.0:
            raise ValueError("Unable to solve power de-vig exponent")

    for _ in range(80):
        midpoint = (lower + upper) / 2.0
        if powered_total(midpoint) > 1.0:
            lower = midpoint
        else:
            upper = midpoint

    exponent = (lower + upper) / 2.0
    normalized = [probability**exponent for probability in implied]
    normalized_total = sum(normalized)
    return [probability / normalized_total for probability in normalized]


def _validated_probabilities(probabilities: list[float]) -> list[float]:
    if not probabilities:
        raise ValueError("probabilities cannot be empty")
    validated: list[float] = []
    for probability in probabilities:
        try:
            value = float(probability)
        except (TypeError, ValueError):
            raise ValueError("probabilities must be finite values between 0 and 1") from None
        if not isfinite(value) or value < 0.0 or value > 1.0:
            raise ValueError("probabilities must be finite values between 0 and 1")
        validated.append(value)
    return validated


def robust_consensus_probability(probabilities: list[float]) -> float:
    """Return a trimmed consensus probability that downweights one-book outliers."""
    ordered = sorted(_validated_probabilities(probabilities))

    if len(ordered) <= 2:
        return sum(ordered) / len(ordered)
    if len(ordered) == 3:
        return float(median(ordered))
    trimmed = ordered[1:-1]
    return sum(trimmed) / len(trimmed)


def consensus_stats(probabilities: list[float]) -> dict[str, float]:
    """Summarize consensus quality for a set of fair probabilities."""
    probabilities = _validated_probabilities(probabilities)

    prob_low = min(probabilities)
    prob_high = max(probabilities)
    prob_range = prob_high - prob_low
    prob_stddev = pstdev(probabilities) if len(probabilities) > 1 else 0.0
    stability = max(0.0, 1.0 - min(1.0, prob_range / 0.1) * 0.5)
    return {
        "mean": sum(probabilities) / len(probabilities),
        "robust": robust_consensus_probability(probabilities),
        "low": prob_low,
        "high": prob_high,
        "range": prob_range,
        "stddev": prob_stddev,
        "stability": stability,
    }


def stability_adjusted_probability(consensus_prob: float, stability: float) -> float:
    adjusted = 0.5 + ((consensus_prob - 0.5) * stability)
    return min(1.0, max(0.0, adjusted))


def consensus_confidence(*, stability: float, books_used: int, min_books: int) -> float:
    """Combine quote agreement and book depth into a bounded confidence factor."""
    if books_used <= 0:
        raise ValueError("books_used must be positive")
    if min_books <= 0:
        raise ValueError("min_books must be positive")

    target_books = max(3, min_books + 1)
    depth_confidence = min(1.0, books_used / target_books)
    bounded_stability = min(1.0, max(0.0, stability))
    return bounded_stability * depth_confidence


def confidence_adjusted_edge(raw_edge: float, confidence: float) -> float:
    """Shrink edge magnitude toward zero when consensus support is weak."""
    bounded_confidence = min(1.0, max(0.0, confidence))
    return raw_edge * bounded_confidence


def edge_roi_on_cost(edge: float, price_dollars: float) -> float:
    """Return expected ROI on deployed capital for a binary contract entry."""
    if price_dollars <= 0.0:
        raise ValueError("price_dollars must be positive")
    return edge / price_dollars


def consensus_rank_score(
    *,
    base_edge: float,
    stability: float,
    books_used: int,
    min_books: int,
    stale_quote_penalty_value: float,
) -> float:
    extra_books = max(0, books_used - min_books)
    extra_book_bonus = extra_books * 0.0025 * stability
    disagreement_penalty = (1.0 - stability) * 0.02
    return round(
        base_edge - disagreement_penalty + extra_book_bonus - stale_quote_penalty_value,
        6,
    )


def stale_quote_penalty(age_seconds: float, *, max_age_seconds: float = 1800.0, max_penalty: float = 0.02) -> float:
    """Return a bounded ranking penalty for stale quotes relative to the freshest paired quote."""
    if age_seconds <= 0:
        return 0.0
    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be positive")
    if max_penalty < 0:
        raise ValueError("max_penalty cannot be negative")
    return min(age_seconds / max_age_seconds, 1.0) * max_penalty
