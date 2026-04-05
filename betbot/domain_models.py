from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MarketSnapshot:
    market: str
    source: str
    best_bid: float
    best_ask: float
    quote_age_seconds: float
    open_status: str


@dataclass(frozen=True)
class CandidateEvaluation:
    market: str
    side: str
    selected_fair_probability: float
    selected_fair_probability_conservative: float
    expected_value_dollars: float
    expected_value_per_cost: float
    edge_rank_score: float
    stale_quote_penalty: float
    consensus_book_count: int


@dataclass(frozen=True)
class PolicyDecisionRecord:
    status: str
    reason: str
    penalties_applied: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ApprovalRecordModel:
    ticket_hash: str
    approved_by: str
    issued_at: str
    expires_at: str


@dataclass(frozen=True)
class OrderLifecycle:
    order_id: str
    status: str
    market: str
    side: str
    contracts: int


@dataclass(frozen=True)
class PositionLifecycle:
    market: str
    side: str
    open_contracts: int
    settled_contracts: int


@dataclass(frozen=True)
class SettlementRecord:
    market: str
    settled_at: str
    realized_pnl_dollars: float
