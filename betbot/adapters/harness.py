from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import random

from betbot.policy.degraded_mode import summarize_source_results
from betbot.runtime.source_result import SourceResult


@dataclass(frozen=True)
class HarnessScenarioResult:
    scenario_id: str
    overall_status: str
    expected_status: str
    source_statuses: dict[str, str]
    passed: bool


def run_adapter_harness(
    scenarios_file: str | Path,
    *,
    seed: int = 42,
) -> list[HarnessScenarioResult]:
    rng = random.Random(seed)
    payload = json.loads(Path(scenarios_file).read_text(encoding="utf-8"))
    scenarios = list(payload.get("scenarios") or [])
    results: list[HarnessScenarioResult] = []

    for scenario in scenarios:
        scenario_id = str(scenario.get("id") or f"scenario_{len(results)+1}")
        expected = str(scenario.get("expected_status") or "ok")
        source_statuses = {
            str(provider): str(status).strip().lower()
            for provider, status in dict(scenario.get("source_statuses") or {}).items()
            if str(provider).strip()
        }
        hard_required_sources = tuple(
            str(item).strip()
            for item in list(scenario.get("hard_required_sources") or [])
            if str(item).strip()
        )

        if source_statuses:
            source_results = {
                provider: SourceResult(
                    provider=provider,
                    status=status if status in {"ok", "partial", "degraded", "failed", "blocked"} else "failed",
                    payload={},
                )
                for provider, status in source_statuses.items()
            }
            summary = summarize_source_results(
                source_results=source_results,
                phase="sources.ready",
                hard_required_sources=hard_required_sources,
            )
            observed = summary.overall_status
        else:
            observed = str(scenario.get("observed_status") or expected)
        if bool(scenario.get("randomize_observed")):
            observed = rng.choice(["ok", "degraded", "blocked", "failed"])
        results.append(
            HarnessScenarioResult(
                scenario_id=scenario_id,
                overall_status=observed,
                expected_status=expected,
                source_statuses=source_statuses,
                passed=(observed == expected),
            )
        )
    return results
