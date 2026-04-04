from __future__ import annotations

from dataclasses import dataclass

from betbot.adapters.base import AdapterContext
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class KalshiMarketDataAdapter:
    provider: str = "kalshi_market_data"

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(
            provider=self.provider,
            status="degraded",
            payload={"note": "adapter scaffold active"},
            coverage_ratio=0.0,
            stale_seconds=None,
            warnings=["Kalshi market data adapter is scaffolded but not bound to live API in runtime phase 1."],
            errors=[],
            failed_components=[],
            recovery_recommendation="Wire this adapter to existing live snapshot modules in phase 2.",
        )
