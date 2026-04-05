from __future__ import annotations

from dataclasses import dataclass

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class OpticOddsConsensusAdapter:
    provider: str = "opticodds_consensus"

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(
            provider=self.provider,
            status="partial",
            payload={"note": "consensus scaffold"},
            coverage_ratio=0.5,
            stale_seconds=None,
            warnings=["Consensus adapter scaffold returns partial coverage until phase 2 wiring."],
            errors=[],
            failed_components=[],
            recovery_recommendation="Connect to book-depth source and emit per-book health events.",
        )
