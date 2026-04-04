from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from betbot.execution.live_executor import LiveExecutor
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


if __name__ == "__main__":
    unittest.main()
