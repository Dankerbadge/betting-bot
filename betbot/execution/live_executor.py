from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import uuid
from typing import Protocol

from betbot.execution.ticket import TicketProposal
from betbot.policy.approvals import ApprovalRecord, verify_approval
from betbot.policy.lanes import LanePolicySet


@dataclass(frozen=True)
class LiveVenueAck:
    accepted: bool
    ack_status: str
    external_order_id: str
    reason: str


class LiveVenueAdapter(Protocol):
    def submit_order(self, *, lane: str, ticket: TicketProposal) -> LiveVenueAck:
        ...


class LocalLiveVenueAdapter:
    """Default narrow venue adapter stub used until live venue integration is wired."""

    def submit_order(self, *, lane: str, ticket: TicketProposal) -> LiveVenueAck:
        return LiveVenueAck(
            accepted=True,
            ack_status="accepted",
            external_order_id=f"sim-live::{uuid.uuid4().hex[:16]}",
            reason="accepted",
        )


@dataclass(frozen=True)
class LiveExecutionResult:
    status: str
    reason: str
    submitted_at: str
    market: str
    side: str
    ack_status: str
    external_order_id: str | None


class LiveExecutor:
    def __init__(self, lane_policy_set: LanePolicySet, venue_adapter: LiveVenueAdapter | None = None) -> None:
        self._lane_policy_set = lane_policy_set
        self._venue_adapter = venue_adapter or LocalLiveVenueAdapter()

    def submit(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        approval: ApprovalRecord | None,
        approval_required: bool = True,
    ) -> LiveExecutionResult:
        now_iso = datetime.now(timezone.utc).isoformat()
        if not self._lane_policy_set.is_allowed(lane, "live_submit"):
            return LiveExecutionResult(
                status="blocked",
                reason="policy_block",
                submitted_at=now_iso,
                market=ticket.market,
                side=ticket.side,
                ack_status="not_submitted",
                external_order_id=None,
            )

        if approval_required:
            ok, reason = verify_approval(ticket=ticket, approval=approval)
            if not ok:
                return LiveExecutionResult(
                    status="blocked",
                    reason=reason,
                    submitted_at=now_iso,
                    market=ticket.market,
                    side=ticket.side,
                    ack_status="not_submitted",
                    external_order_id=None,
                )

        venue_ack = self._venue_adapter.submit_order(lane=lane, ticket=ticket)
        if not venue_ack.accepted:
            return LiveExecutionResult(
                status="blocked",
                reason=venue_ack.reason or "submission_rejected",
                submitted_at=now_iso,
                market=ticket.market,
                side=ticket.side,
                ack_status=venue_ack.ack_status or "rejected",
                external_order_id=venue_ack.external_order_id or None,
            )

        return LiveExecutionResult(
            status="submitted",
            reason="live_submit_allowed",
            submitted_at=now_iso,
            market=ticket.market,
            side=ticket.side,
            ack_status=venue_ack.ack_status,
            external_order_id=venue_ack.external_order_id,
        )
