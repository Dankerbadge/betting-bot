from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import random


@dataclass(frozen=True)
class HarnessScenarioResult:
    scenario_id: str
    overall_status: str
    expected_status: str
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
        observed = str(scenario.get("observed_status") or expected)
        if bool(scenario.get("randomize_observed")):
            observed = rng.choice(["ok", "degraded", "blocked", "failed"])
        results.append(
            HarnessScenarioResult(
                scenario_id=scenario_id,
                overall_status=observed,
                expected_status=expected,
                passed=(observed == expected),
            )
        )
    return results
