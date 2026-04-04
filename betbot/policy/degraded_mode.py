from __future__ import annotations

from dataclasses import dataclass

from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class DegradedSummary:
    overall_status: str
    phase: str
    source_statuses: dict[str, str]
    candidate_count_before_penalties: int
    candidate_count_after_penalties: int
    blocked_markets: list[str]
    penalties_applied: dict[str, float]
    blocker_type: str | None
    recovery_recommendation: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "overall_status": self.overall_status,
            "phase": self.phase,
            "source_statuses": dict(self.source_statuses),
            "candidate_count_before_penalties": self.candidate_count_before_penalties,
            "candidate_count_after_penalties": self.candidate_count_after_penalties,
            "blocked_markets": list(self.blocked_markets),
            "penalties_applied": dict(self.penalties_applied),
            "blocker_type": self.blocker_type,
            "recovery_recommendation": self.recovery_recommendation,
        }


def summarize_source_results(
    *,
    source_results: dict[str, SourceResult[object]],
    phase: str,
    hard_required_sources: tuple[str, ...] = (),
    candidate_count_before_penalties: int = 0,
    candidate_count_after_penalties: int = 0,
    blocked_markets: list[str] | None = None,
    penalties_applied: dict[str, float] | None = None,
    blocker_type: str | None = None,
    recovery_recommendation: str | None = None,
) -> DegradedSummary:
    source_statuses = {provider: result.status for provider, result in source_results.items()}
    hard_required = set(hard_required_sources)

    hard_block = any(
        provider in hard_required and status in {"failed", "blocked"}
        for provider, status in source_statuses.items()
    )
    any_problem = any(status in {"partial", "degraded", "failed", "blocked"} for status in source_statuses.values())

    if hard_block:
        overall_status = "blocked"
    elif any_problem:
        overall_status = "degraded"
    else:
        overall_status = "ok"

    if blocker_type is None and hard_block:
        blocker_type = "required_source_failure"

    if recovery_recommendation is None and overall_status == "degraded":
        recovery_recommendation = "Retry degraded sources and continue with policy penalties."
    if recovery_recommendation is None and overall_status == "blocked":
        recovery_recommendation = "Resolve hard-required source failures before next cycle."

    return DegradedSummary(
        overall_status=overall_status,
        phase=phase,
        source_statuses=source_statuses,
        candidate_count_before_penalties=int(candidate_count_before_penalties),
        candidate_count_after_penalties=int(candidate_count_after_penalties),
        blocked_markets=list(blocked_markets or []),
        penalties_applied=dict(penalties_applied or {}),
        blocker_type=blocker_type,
        recovery_recommendation=recovery_recommendation,
    )
