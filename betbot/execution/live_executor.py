from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from betbot.execution.ticket import TicketProposal
from betbot.policy.approvals import ApprovalRecord, verify_approval
from betbot.policy.lanes import LanePolicySet


@dataclass(frozen=True)
class LiveExecutionResult:
    status: str
    reason: str
    submitted_at: str
    market: str
    side: str


class LiveExecutor:
    def __init__(self, lane_policy_set: LanePolicySet) -> None:
        self._lane_policy_set = lane_policy_set

    def submit(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        approval: ApprovalRecord | None,
    ) -> LiveExecutionResult:
        now_iso = datetime.now(timezone.utc).isoformat()
        if not self._lane_policy_set.is_allowed(lane, "live_submit"):
            return LiveExecutionResult(
                status="blocked",
                reason="policy_block",
                submitted_at=now_iso,
                market=ticket.market,
                side=ticket.side,
            )

        ok, reason = verify_approval(ticket=ticket, approval=approval)
        if not ok:
            return LiveExecutionResult(
                status="blocked",
                reason=reason,
                submitted_at=now_iso,
                market=ticket.market,
                side=ticket.side,
            )

        return LiveExecutionResult(
            status="submitted",
            reason="live_submit_allowed",
            submitted_at=now_iso,
            market=ticket.market,
            side=ticket.side,
        )
