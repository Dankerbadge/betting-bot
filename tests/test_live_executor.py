from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from betbot.execution.live_executor import LiveExecutor, LocalLiveVenueAdapter
from betbot.execution.ticket import create_ticket_proposal
from betbot.policy.approvals import ApprovalRecord
from betbot.policy.lanes import load_lane_policy_set


class LiveExecutorTests(unittest.TestCase):
    def test_submit_blocked_outside_live_lane(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(lane_policy_set)
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=10.0,
            lane="research",
            source_run_id="run-x",
        )
        result = executor.submit(lane="research", ticket=ticket, approval=None)
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.reason, "policy_block")
        self.assertEqual(result.ack_status, "not_submitted")
        self.assertIsNone(result.external_order_id)

    def test_submit_blocked_without_valid_approval(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(lane_policy_set)
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=10.0,
            lane="live_execute",
            source_run_id="run-y",
        )
        result = executor.submit(lane="live_execute", ticket=ticket, approval=None)
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.reason, "approval_missing")
        self.assertEqual(result.ack_status, "not_submitted")
        self.assertIsNone(result.external_order_id)

    def test_submit_allowed_with_valid_approval(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(lane_policy_set)
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
        ticket = create_ticket_proposal(
            market="MKT",
            side="no",
            max_cost=8.5,
            lane="live_execute",
            source_run_id="run-z",
            expires_at=expires_at,
        )
        approval = ApprovalRecord(
            ticket_hash=ticket.ticket_hash,
            market=ticket.market,
            side=ticket.side,
            max_cost=ticket.max_cost,
            issued_at=datetime.now(timezone.utc).isoformat(),
            expires_at=expires_at,
            approved_by="operator",
        )
        result = executor.submit(lane="live_execute", ticket=ticket, approval=approval)
        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.reason, "live_submit_allowed")
        self.assertEqual(result.ack_status, "accepted")
        self.assertIsNotNone(result.external_order_id)

    def test_submit_allowed_when_approval_optional(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(lane_policy_set)
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=4.0,
            lane="live_execute",
            source_run_id="run-opt",
        )
        result = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.reason, "live_submit_allowed")
        self.assertEqual(result.ack_status, "accepted")

    def test_submit_rejected_ack_surfaces_reject_reason(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(submit_outcome="rejected"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=4.0,
            lane="live_execute",
            source_run_id="run-reject",
        )
        result = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.reason, "submission_rejected")
        self.assertEqual(result.ack_status, "rejected")
        self.assertIsNone(result.external_order_id)

    def test_submit_timeout_ack_surfaces_timeout_reason(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(submit_outcome="timeout"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=4.0,
            lane="live_execute",
            source_run_id="run-timeout",
        )
        result = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.reason, "submission_timeout")
        self.assertEqual(result.ack_status, "timeout")
        self.assertIsNone(result.external_order_id)

    def test_reconcile_default_resting_lifecycle(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(lane_policy_set)
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=1.0,
            lane="live_execute",
            source_run_id="run-reconcile",
        )
        submission = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(submission.status, "submitted")
        reconciliation = executor.reconcile(
            lane="live_execute",
            ticket=ticket,
            external_order_id=submission.external_order_id,
        )
        self.assertEqual(reconciliation.status, "resting")
        self.assertEqual(reconciliation.reason, "reconciled_resting")
        self.assertEqual(reconciliation.remaining_quantity, 1.0)
        self.assertEqual(reconciliation.filled_quantity, 0.0)

    def test_reconcile_mismatch_outcome(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="mismatch"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=1.0,
            lane="live_execute",
            source_run_id="run-reconcile-mismatch",
        )
        submission = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(submission.status, "submitted")
        reconciliation = executor.reconcile(
            lane="live_execute",
            ticket=ticket,
            external_order_id=submission.external_order_id,
        )
        self.assertEqual(reconciliation.status, "mismatch")
        self.assertEqual(reconciliation.reason, "reconcile_mismatch")
        self.assertEqual(reconciliation.mismatches, 1)

    def test_reconcile_filled_outcome(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="filled"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=1.0,
            lane="live_execute",
            source_run_id="run-reconcile-filled",
        )
        submission = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(submission.status, "submitted")
        reconciliation = executor.reconcile(
            lane="live_execute",
            ticket=ticket,
            external_order_id=submission.external_order_id,
        )
        self.assertEqual(reconciliation.status, "filled")
        self.assertEqual(reconciliation.reason, "reconciled_filled")
        self.assertEqual(reconciliation.filled_quantity, 1.0)
        self.assertEqual(reconciliation.remaining_quantity, 0.0)

    def test_reconcile_partially_filled_outcome(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="partially_filled"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=1.0,
            lane="live_execute",
            source_run_id="run-reconcile-partial",
        )
        submission = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(submission.status, "submitted")
        reconciliation = executor.reconcile(
            lane="live_execute",
            ticket=ticket,
            external_order_id=submission.external_order_id,
        )
        self.assertEqual(reconciliation.status, "partially_filled")
        self.assertEqual(reconciliation.reason, "reconciled_partially_filled")
        self.assertEqual(reconciliation.filled_quantity, 0.5)
        self.assertEqual(reconciliation.remaining_quantity, 0.5)

    def test_reconcile_canceled_outcome(self) -> None:
        lane_policy_set = load_lane_policy_set()
        executor = LiveExecutor(
            lane_policy_set,
            venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="canceled"),
        )
        ticket = create_ticket_proposal(
            market="MKT",
            side="yes",
            max_cost=1.0,
            lane="live_execute",
            source_run_id="run-reconcile-canceled",
        )
        submission = executor.submit(
            lane="live_execute",
            ticket=ticket,
            approval=None,
            approval_required=False,
        )
        self.assertEqual(submission.status, "submitted")
        reconciliation = executor.reconcile(
            lane="live_execute",
            ticket=ticket,
            external_order_id=submission.external_order_id,
        )
        self.assertEqual(reconciliation.status, "canceled")
        self.assertEqual(reconciliation.reason, "reconciled_canceled")
        self.assertEqual(reconciliation.filled_quantity, 0.0)
        self.assertEqual(reconciliation.remaining_quantity, 0.0)


if __name__ == "__main__":
    unittest.main()
