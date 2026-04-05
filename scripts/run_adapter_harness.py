#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from betbot.adapters.harness import run_adapter_harness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run deterministic adapter harness scenarios")
    parser.add_argument(
        "--scenarios",
        default="tests/fixtures/harness/scenarios.json",
        help="Scenario JSON file path",
    )
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--output", default="outputs/adapter_harness_latest.json", help="Output JSON path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results = run_adapter_harness(args.scenarios, seed=args.seed)
    payload = {
        "scenarios_file": str(Path(args.scenarios).resolve()),
        "seed": int(args.seed),
        "result_count": len(results),
        "passed": all(item.passed for item in results),
        "results": [
            {
                "scenario_id": item.scenario_id,
                "observed_status": item.overall_status,
                "expected_status": item.expected_status,
                "source_statuses": item.source_statuses,
                "passed": item.passed,
            }
            for item in results
        ],
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
