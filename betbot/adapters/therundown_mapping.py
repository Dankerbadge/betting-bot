from __future__ import annotations

from dataclasses import dataclass

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class TheRundownMappingAdapter:
    provider: str = "therundown_mapping"

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(
            provider=self.provider,
            status="ok",
            payload={"note": "mapping scaffold"},
            coverage_ratio=1.0,
            stale_seconds=None,
            warnings=[],
            errors=[],
            failed_components=[],
            recovery_recommendation=None,
        )
