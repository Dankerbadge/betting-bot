from __future__ import annotations

from dataclasses import dataclass

from betbot.policy.degraded_mode import DegradedSummary
from betbot.policy.lanes import LanePolicySet


@dataclass(frozen=True)
class PolicyDecision:
    status: str
    reason: str
    allowed_actions: list[str]



def evaluate_policy_gate(
    *,
    lane: str,
    lane_policy_set: LanePolicySet,
    degraded_summary: DegradedSummary,
    request_live_submit: bool,
) -> PolicyDecision:
    allowed_actions = lane_policy_set.allowed_actions(lane)

    if request_live_submit and not lane_policy_set.is_allowed(lane, "live_submit"):
        return PolicyDecision(
            status="blocked",
            reason="policy_block",
            allowed_actions=allowed_actions,
        )

    if degraded_summary.overall_status == "blocked":
        return PolicyDecision(
            status="blocked",
            reason="required_source_failure",
            allowed_actions=allowed_actions,
        )

    if degraded_summary.overall_status == "degraded":
        return PolicyDecision(
            status="degraded",
            reason="partial_source_coverage",
            allowed_actions=allowed_actions,
        )

    return PolicyDecision(status="ok", reason="policy_pass", allowed_actions=allowed_actions)
