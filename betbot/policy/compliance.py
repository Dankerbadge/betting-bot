from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ComplianceThresholds:
    settlement_min: float = 0.7
    execution_min: float = 0.7
    compliance_min: float = 0.7
    overall_min: float = 0.75


@dataclass(frozen=True)
class ComplianceScorecard:
    settlement: float
    execution: float
    compliance: float
    overall: float
    high_blockers: int = 0
    critical_blockers: int = 0


def evaluate_readiness(
    scorecard: ComplianceScorecard,
    thresholds: ComplianceThresholds,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if scorecard.critical_blockers > 0:
        reasons.append("critical_blockers_present")
    if scorecard.high_blockers > 0:
        reasons.append("high_blockers_present")
    if scorecard.settlement < thresholds.settlement_min:
        reasons.append("settlement_below_threshold")
    if scorecard.execution < thresholds.execution_min:
        reasons.append("execution_below_threshold")
    if scorecard.compliance < thresholds.compliance_min:
        reasons.append("compliance_below_threshold")
    if scorecard.overall < thresholds.overall_min:
        reasons.append("overall_below_threshold")
    return (len(reasons) == 0), reasons
