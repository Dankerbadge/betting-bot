from __future__ import annotations

from betbot.runtime.failures import FailureType

RECOVERY_RECOMMENDATIONS: dict[FailureType, str] = {
    "feed_unavailable": "Retry source fetch with backoff and fallback provider where available.",
    "feed_stale": "Hold live actions and refresh feed until freshness policy recovers.",
    "schema_invalid": "Pin adapter version and inspect provider schema change before rerun.",
    "mapping_missing": "Block affected markets and rebuild event/selection mapping cache.",
    "news_unavailable": "Apply policy penalty and retry curated sources on next cycle.",
    "bankroll_block": "Reduce open risk or fund account before resuming ticketing.",
    "compliance_block": "Resolve compliance matrix blockers before live lane usage.",
    "policy_block": "Switch to an allowed lane or adjust policy config.",
    "approval_missing": "Create a fresh approval bound to ticket hash and expiry.",
    "approval_expired": "Re-issue approval artifact and retry within validity window.",
    "market_closed": "Drop ticket and wait for next eligible market window.",
    "submission_rejected": "Capture reject reason and downgrade lane until reconciled.",
    "reconcile_mismatch": "Run reconciliation workflow and freeze new submissions.",
    "infra": "Inspect runtime logs and dependency health checks before retry.",
}


def recommendation_for_failure(failure_type: FailureType) -> str:
    return RECOVERY_RECOMMENDATIONS.get(failure_type, "Investigate runtime state before retry.")
