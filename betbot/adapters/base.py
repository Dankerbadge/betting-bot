from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class AdapterContext:
    run_id: str
    cycle_id: str
    lane: str
    now_iso: str
    output_dir: str


class Adapter(Protocol):
    provider: str

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        ...


def run_adapter(adapter: Adapter, context: AdapterContext) -> SourceResult[dict[str, object]]:
    try:
        return adapter.fetch(context)
    except Exception as exc:
        return SourceResult(
            provider=getattr(adapter, "provider", "unknown"),
            status="failed",
            payload=None,
            coverage_ratio=0.0,
            stale_seconds=None,
            warnings=[],
            errors=[str(exc)],
            failed_components=["adapter"],
            recovery_recommendation="Adapter raised an exception; inspect source integration.",
        )
