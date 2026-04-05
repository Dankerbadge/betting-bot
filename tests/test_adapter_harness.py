from __future__ import annotations

from pathlib import Path
import unittest

from betbot.adapters.harness import run_adapter_harness


class AdapterHarnessTests(unittest.TestCase):
    def test_fixture_scenarios_are_deterministic(self) -> None:
        scenarios_file = Path("tests/fixtures/harness/scenarios.json")
        first = run_adapter_harness(scenarios_file, seed=42)
        second = run_adapter_harness(scenarios_file, seed=42)
        self.assertEqual(first, second)
        self.assertTrue(all(item.passed for item in first))
        self.assertEqual(first[0].overall_status, "ok")
        self.assertEqual(first[1].overall_status, "degraded")
        self.assertEqual(first[2].overall_status, "blocked")


if __name__ == "__main__":
    unittest.main()
