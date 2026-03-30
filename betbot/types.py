from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class BetCandidate:
    timestamp: datetime
    event_id: str
    selection: str
    odds: float
    model_prob: float
    decision_prob: float | None = None
    edge_rank_score: float | None = None
    closing_odds: float | None = None
    outcome: int | None = None


@dataclass
class Decision:
    timestamp: datetime
    event_id: str
    selection: str
    odds: float
    model_prob: float
    decision_prob: float
    ev: float
    kelly_full: float
    kelly_used: float
    stake: float
    status: str
    reason: str
    bankroll_before: float
    bankroll_after: float
    pnl: float
    closing_odds: float | None = None
    outcome: int | None = None
