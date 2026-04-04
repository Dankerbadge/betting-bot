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


if __name__ == "__main__":
    unittest.main()
