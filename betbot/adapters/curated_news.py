from __future__ import annotations

from dataclasses import dataclass

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class CuratedNewsAdapter:
    provider: str = "curated_news"

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(
            provider=self.provider,
            status="degraded",
            payload={"articles": []},
            coverage_ratio=0.0,
            stale_seconds=None,
            warnings=["No news ingested in scaffold mode."],
            errors=["news_unavailable"],
            failed_components=["news_fetch"],
            recovery_recommendation="Use allowlisted domain ingestion and citation checks before live gating.",
        )
