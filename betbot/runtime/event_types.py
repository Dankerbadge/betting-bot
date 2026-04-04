from __future__ import annotations

from typing import Literal

CycleOverallStatus = Literal["ok", "degraded", "blocked", "failed"]
SourceStatus = Literal["ok", "partial", "degraded", "failed", "blocked"]
EventSeverity = Literal["info", "warn", "error", "block"]

PHASE_CYCLE_STARTED = "cycle.started"
PHASE_SOURCES_FETCHING = "sources.fetching"
PHASE_SOURCES_PARTIAL = "sources.partial"
PHASE_SOURCES_READY = "sources.ready"
PHASE_NEWS_ENRICHING = "news.enriching"
PHASE_SNAPSHOTS_NORMALIZED = "snapshots.normalized"
PHASE_CANDIDATES_SCORED = "candidates.scored"
PHASE_POLICY_CHECKED = "policy.checked"
PHASE_TICKET_READY = "ticket.ready"
PHASE_TICKET_BLOCKED = "ticket.blocked"
PHASE_APPROVAL_WAITING = "approval.waiting"
PHASE_ORDER_SUBMITTED = "order.submitted"
PHASE_ORDER_RESTING = "order.resting"
PHASE_ORDER_PARTIALLY_FILLED = "order.partially_filled"
PHASE_ORDER_FILLED = "order.filled"
PHASE_ORDER_CANCELED = "order.canceled"
PHASE_POSITION_OPEN = "position.open"
PHASE_POSITION_SETTLED = "position.settled"
PHASE_CYCLE_FINISHED = "cycle.finished"
PHASE_CYCLE_FAILED = "cycle.failed"

PHASES: tuple[str, ...] = (
    PHASE_CYCLE_STARTED,
    PHASE_SOURCES_FETCHING,
    PHASE_SOURCES_PARTIAL,
    PHASE_SOURCES_READY,
    PHASE_NEWS_ENRICHING,
    PHASE_SNAPSHOTS_NORMALIZED,
    PHASE_CANDIDATES_SCORED,
    PHASE_POLICY_CHECKED,
    PHASE_TICKET_READY,
    PHASE_TICKET_BLOCKED,
    PHASE_APPROVAL_WAITING,
    PHASE_ORDER_SUBMITTED,
    PHASE_ORDER_RESTING,
    PHASE_ORDER_PARTIALLY_FILLED,
    PHASE_ORDER_FILLED,
    PHASE_ORDER_CANCELED,
    PHASE_POSITION_OPEN,
    PHASE_POSITION_SETTLED,
    PHASE_CYCLE_FINISHED,
    PHASE_CYCLE_FAILED,
)


def is_valid_phase(phase: str) -> bool:
    return phase in PHASES
