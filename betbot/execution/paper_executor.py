from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from betbot.execution.ticket import TicketProposal


@dataclass(frozen=True)
class PaperExecutionResult:
    status: str
    order_id: str
    submitted_at: str
    market: str
    side: str
    max_cost: float


class PaperExecutor:
    def submit(self, ticket: TicketProposal) -> PaperExecutionResult:
        now_iso = datetime.now(timezone.utc).isoformat()
        order_id = f"paper::{ticket.ticket_hash[:16]}"
        return PaperExecutionResult(
            status="submitted",
            order_id=order_id,
            submitted_at=now_iso,
            market=ticket.market,
            side=ticket.side,
            max_cost=ticket.max_cost,
        )
