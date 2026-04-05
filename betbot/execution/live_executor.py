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
    external_order_id: str | None
    reason: str


class LiveVenueAdapter(Protocol):
    def submit_order(self, *, lane: str, ticket: TicketProposal) -> LiveVenueAck:
        ...

    def reconcile_order(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        external_order_id: str,
    ) -> "LiveReconciliationResult":
        ...


class LocalLiveVenueAdapter:
    """Default narrow venue adapter used until full venue integration is wired."""

    def __init__(
        self,
        *,
        submit_outcome: str = "accepted",
        reconcile_outcome: str = "resting",
    ) -> None:
        self._submit_outcome = str(submit_outcome or "accepted").strip().lower()
        self._reconcile_outcome = str(reconcile_outcome or "resting").strip().lower()

    def submit_order(self, *, lane: str, ticket: TicketProposal) -> LiveVenueAck:
        if self._submit_outcome == "timeout":
            return LiveVenueAck(
                accepted=False,
                ack_status="timeout",
                external_order_id=None,
                reason="submission_timeout",
            )
        if self._submit_outcome == "rejected":
            return LiveVenueAck(
                accepted=False,
                ack_status="rejected",
                external_order_id=None,
                reason="submission_rejected",
            )
        return LiveVenueAck(
            accepted=True,
            ack_status="accepted",
            external_order_id=f"sim-live::{uuid.uuid4().hex[:16]}",
            reason="accepted",
        )

    def reconcile_order(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        external_order_id: str,
    ) -> "LiveReconciliationResult":
        outcome = self._reconcile_outcome
        if outcome == "partially_filled":
            return LiveReconciliationResult(
                status="partially_filled",
                reason="reconciled_partially_filled",
                filled_quantity=0.5,
                remaining_quantity=0.5,
                mismatches=0,
                position_status="open",
                external_order_id=external_order_id,
            )
        if outcome == "filled":
            return LiveReconciliationResult(
                status="filled",
                reason="reconciled_filled",
                filled_quantity=1.0,
                remaining_quantity=0.0,
                mismatches=0,
                position_status="open",
                external_order_id=external_order_id,
            )
        if outcome == "canceled":
            return LiveReconciliationResult(
                status="canceled",
                reason="reconciled_canceled",
                filled_quantity=0.0,
                remaining_quantity=0.0,
                mismatches=0,
                position_status="none",
                external_order_id=external_order_id,
            )
        if outcome == "mismatch":
            return LiveReconciliationResult(
                status="mismatch",
                reason="reconcile_mismatch",
                filled_quantity=0.0,
                remaining_quantity=0.0,
                mismatches=1,
                position_status="unknown",
                external_order_id=external_order_id,
            )
        return LiveReconciliationResult(
            status="resting",
            reason="reconciled_resting",
            filled_quantity=0.0,
            remaining_quantity=1.0,
            mismatches=0,
            position_status="none",
            external_order_id=external_order_id,
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


@dataclass(frozen=True)
class LiveReconciliationResult:
    status: str
    reason: str
    filled_quantity: float
    remaining_quantity: float
    mismatches: int
    position_status: str
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

    def reconcile(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        external_order_id: str | None,
    ) -> LiveReconciliationResult:
        if external_order_id is None or not str(external_order_id).strip():
            return LiveReconciliationResult(
                status="unknown",
                reason="missing_external_order_id",
                filled_quantity=0.0,
                remaining_quantity=0.0,
                mismatches=0,
                position_status="unknown",
                external_order_id=None,
            )
        return self._venue_adapter.reconcile_order(
            lane=lane,
            ticket=ticket,
            external_order_id=str(external_order_id),
        )
