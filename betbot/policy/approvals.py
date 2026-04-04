from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from betbot.execution.ticket import TicketProposal


@dataclass(frozen=True)
class ApprovalRecord:
    ticket_hash: str
    market: str
    side: str
    max_cost: float
    issued_at: str
    expires_at: str
    approved_by: str

    def is_fresh(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(timezone.utc)
        expires = datetime.fromisoformat(self.expires_at)
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return current <= expires


def verify_approval(
    *,
    ticket: TicketProposal,
    approval: ApprovalRecord | None,
    now: datetime | None = None,
) -> tuple[bool, str]:
    if approval is None:
        return False, "approval_missing"
    if approval.ticket_hash != ticket.ticket_hash:
        return False, "approval_ticket_hash_mismatch"
    if approval.market != ticket.market or approval.side.lower() != ticket.side.lower():
        return False, "approval_ticket_identity_mismatch"
    if float(approval.max_cost) != float(ticket.max_cost):
        return False, "approval_max_cost_mismatch"
    if not approval.is_fresh(now=now):
        return False, "approval_expired"
    return True, "approval_valid"
