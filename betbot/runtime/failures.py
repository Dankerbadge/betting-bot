from __future__ import annotations

from typing import Literal

FailureType = Literal[
    "feed_unavailable",
    "feed_stale",
    "schema_invalid",
    "mapping_missing",
    "news_unavailable",
    "bankroll_block",
    "compliance_block",
    "policy_block",
    "approval_missing",
    "approval_expired",
    "market_closed",
    "submission_rejected",
    "reconcile_mismatch",
    "infra",
]

FAILURE_TYPES: tuple[FailureType, ...] = (
    "feed_unavailable",
    "feed_stale",
    "schema_invalid",
    "mapping_missing",
    "news_unavailable",
    "bankroll_block",
    "compliance_block",
    "policy_block",
    "approval_missing",
    "approval_expired",
    "market_closed",
    "submission_rejected",
    "reconcile_mismatch",
    "infra",
)

FAILURE_DESCRIPTIONS: dict[FailureType, str] = {
    "feed_unavailable": "Required external feed is unavailable.",
    "feed_stale": "Feed is available but stale beyond policy limits.",
    "schema_invalid": "Adapter payload failed schema validation.",
    "mapping_missing": "Required market mapping could not be resolved.",
    "news_unavailable": "Curated news source failed or had no fresh coverage.",
    "bankroll_block": "Bankroll or exposure policy blocked ticketing.",
    "compliance_block": "Compliance policy blocked cycle progression.",
    "policy_block": "Lane policy or action policy rejected operation.",
    "approval_missing": "Live execute request has no approval artifact.",
    "approval_expired": "Approval artifact is stale or expired.",
    "market_closed": "Market state changed to closed/suspended before submit.",
    "submission_rejected": "Venue rejected order submission request.",
    "reconcile_mismatch": "Venue/account reconciliation mismatch detected.",
    "infra": "Infrastructure error outside adapter semantics.",
}


def classify_failure(message: str) -> FailureType:
    low = (message or "").lower()
    if "stale" in low:
        return "feed_stale"
    if "schema" in low:
        return "schema_invalid"
    if "mapping" in low:
        return "mapping_missing"
    if "approval" in low and "expired" in low:
        return "approval_expired"
    if "approval" in low:
        return "approval_missing"
    if "compliance" in low:
        return "compliance_block"
    if "bankroll" in low or "risk" in low:
        return "bankroll_block"
    if "reconcile" in low:
        return "reconcile_mismatch"
    if "market" in low and "closed" in low:
        return "market_closed"
    if "submit" in low or "reject" in low:
        return "submission_rejected"
    if "news" in low:
        return "news_unavailable"
    if "policy" in low or "lane" in low:
        return "policy_block"
    if "feed" in low or "adapter" in low or "provider" in low:
        return "feed_unavailable"
    return "infra"
