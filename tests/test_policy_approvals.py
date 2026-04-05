from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from betbot.execution.ticket import create_ticket_proposal
from betbot.policy.approvals import ApprovalRecord, verify_approval


class ApprovalPolicyTests(unittest.TestCase):
    def test_verify_approval_valid(self) -> None:
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        ticket = create_ticket_proposal(
            market="TEST-MARKET",
            side="yes",
            max_cost=12.5,
            lane="live_execute",
            source_run_id="run-1",
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
        ok, reason = verify_approval(ticket=ticket, approval=approval)
        self.assertTrue(ok)
        self.assertEqual(reason, "approval_valid")

    def test_verify_approval_missing(self) -> None:
        ticket = create_ticket_proposal(
            market="TEST-MARKET",
            side="no",
            max_cost=7.0,
            lane="live_execute",
            source_run_id="run-2",
        )
        ok, reason = verify_approval(ticket=ticket, approval=None)
        self.assertFalse(ok)
        self.assertEqual(reason, "approval_missing")

    def test_verify_approval_expired(self) -> None:
        expires_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        ticket = create_ticket_proposal(
            market="TEST-MARKET",
            side="yes",
            max_cost=5.0,
            lane="live_execute",
            source_run_id="run-3",
            expires_at=expires_at,
        )
        approval = ApprovalRecord(
            ticket_hash=ticket.ticket_hash,
            market=ticket.market,
            side=ticket.side,
            max_cost=ticket.max_cost,
            issued_at=(datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat(),
            expires_at=expires_at,
            approved_by="operator",
        )
        ok, reason = verify_approval(ticket=ticket, approval=approval)
        self.assertFalse(ok)
        self.assertEqual(reason, "approval_expired")


if __name__ == "__main__":
    unittest.main()
