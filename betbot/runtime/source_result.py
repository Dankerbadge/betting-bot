from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from betbot.runtime.event_types import SourceStatus

PayloadT = TypeVar("PayloadT")


@dataclass(frozen=True)
class SourceResult(Generic[PayloadT]):
    provider: str
    status: SourceStatus
    payload: PayloadT | None = None
    coverage_ratio: float = 0.0
    stale_seconds: float | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    failed_components: list[str] = field(default_factory=list)
    recovery_recommendation: str | None = None


EMPTY_SOURCE_RESULT = SourceResult[dict[str, object]](
    provider="unknown",
    status="failed",
    payload=None,
    coverage_ratio=0.0,
    stale_seconds=None,
    warnings=[],
    errors=["no_data"],
    failed_components=["adapter"],
    recovery_recommendation="Check provider credentials and connectivity.",
)
