from __future__ import annotations

from dataclasses import dataclass

from betbot.runtime.event_types import (
    PHASE_CANDIDATES_SCORED,
    PHASE_CYCLE_FAILED,
    PHASE_CYCLE_FINISHED,
    PHASE_CYCLE_STARTED,
    PHASE_NEWS_ENRICHING,
    PHASE_POLICY_CHECKED,
    PHASE_SNAPSHOTS_NORMALIZED,
    PHASE_SOURCES_FETCHING,
    PHASE_SOURCES_PARTIAL,
    PHASE_SOURCES_READY,
)

_ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    PHASE_CYCLE_STARTED: (PHASE_SOURCES_FETCHING, PHASE_CYCLE_FAILED),
    PHASE_SOURCES_FETCHING: (PHASE_SOURCES_PARTIAL, PHASE_SOURCES_READY, PHASE_CYCLE_FAILED),
    PHASE_SOURCES_PARTIAL: (PHASE_SOURCES_READY, PHASE_CYCLE_FAILED),
    PHASE_SOURCES_READY: (PHASE_NEWS_ENRICHING, PHASE_CYCLE_FAILED),
    PHASE_NEWS_ENRICHING: (PHASE_SNAPSHOTS_NORMALIZED, PHASE_CYCLE_FAILED),
    PHASE_SNAPSHOTS_NORMALIZED: (PHASE_CANDIDATES_SCORED, PHASE_CYCLE_FAILED),
    PHASE_CANDIDATES_SCORED: (PHASE_POLICY_CHECKED, PHASE_CYCLE_FAILED),
    PHASE_POLICY_CHECKED: (PHASE_CYCLE_FINISHED, PHASE_CYCLE_FAILED),
    PHASE_CYCLE_FINISHED: (),
    PHASE_CYCLE_FAILED: (),
}


@dataclass
class RuntimeStateMachine:
    current_phase: str = PHASE_CYCLE_STARTED

    def transition(self, next_phase: str) -> None:
        allowed = _ALLOWED_TRANSITIONS.get(self.current_phase, ())
        if next_phase not in allowed:
            raise ValueError(
                f"Invalid runtime transition from {self.current_phase!r} to {next_phase!r}; "
                f"allowed={list(allowed)}"
            )
        self.current_phase = next_phase
