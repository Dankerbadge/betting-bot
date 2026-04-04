from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json


@dataclass(frozen=True)
class TicketProposal:
    ticket_hash: str
    market: str
    side: str
    max_cost: float
    expires_at: str
    lane: str
    source_run_id: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)



def build_ticket_hash(
    *,
    market: str,
    side: str,
    max_cost: float,
    expires_at: str,
) -> str:
    payload = {
        "market": market,
        "side": side.lower(),
        "max_cost": round(float(max_cost), 6),
        "expires_at": expires_at,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def create_ticket_proposal(
    *,
    market: str,
    side: str,
    max_cost: float,
    lane: str,
    source_run_id: str,
    expires_at: str | None = None,
) -> TicketProposal:
    ticket_expiry = expires_at or datetime.now(timezone.utc).isoformat()
    ticket_hash = build_ticket_hash(
        market=market,
        side=side,
        max_cost=max_cost,
        expires_at=ticket_expiry,
    )
    return TicketProposal(
        ticket_hash=ticket_hash,
        market=market,
        side=side,
        max_cost=float(max_cost),
        expires_at=ticket_expiry,
        lane=lane,
        source_run_id=source_run_id,
    )
