from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from betbot.adapters.base import AdapterContext
from betbot.runtime.cycle_runner import CycleRunner, CycleRunnerConfig
from betbot.runtime.source_result import SourceResult


class _StaticAdapter:
    def __init__(self, provider: str, status: str) -> None:
        self.provider = provider
        self._status = status

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(provider=self.provider, status=self._status, payload={})


class CycleRunnerTests(unittest.TestCase):
    def test_blocked_cycle_stays_blocked_in_report_and_board(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter("kalshi_market_data", "failed"),
                    _StaticAdapter("opticodds_consensus", "ok"),
                ],
                hard_required_sources=("kalshi_market_data",),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="research",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                )
            )

            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["phase"], "cycle.finished")
            self.assertTrue(report["policy_decisions"])
            self.assertEqual(report["policy_decisions"][0]["status"], "blocked")
            self.assertNotEqual(report["config_fingerprint"], "")
            self.assertNotEqual(report["policy_fingerprint"], "")

            board_path = output_dir / "board_latest.json"
            board_payload = json.loads(board_path.read_text(encoding="utf-8"))
            self.assertEqual(board_payload["overall_status"], "blocked")

    def test_failed_source_emits_error_severity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "failed")],
                hard_required_sources=("kalshi_market_data",),
            )
            runner.run(CycleRunnerConfig(lane="research", output_dir=str(output_dir), repo_root=str(Path.cwd())))

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            source_events = [json.loads(line) for line in event_lines if json.loads(line).get("event_type") == "source_result"]
            self.assertTrue(source_events)
            self.assertEqual(source_events[0]["severity"], "error")


if __name__ == "__main__":
    unittest.main()
