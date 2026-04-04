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

    def test_missing_required_source_blocks_and_emits_missing_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[_StaticAdapter("opticodds_consensus", "ok")],
                hard_required_sources=("kalshi_market_data",),
            )
            report = runner.run(CycleRunnerConfig(lane="research", output_dir=str(output_dir), repo_root=str(Path.cwd())))

            self.assertEqual(report["overall_status"], "blocked")
            self.assertIn("kalshi_market_data", report["degraded_summary"]["missing_required_sources"])
            self.assertEqual(report["source_health"]["kalshi_market_data"], "missing")

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            source_events = [json.loads(line) for line in event_lines if json.loads(line).get("event_type") == "source_result"]
            missing_events = [row for row in source_events if row.get("source") == "kalshi_market_data"]
            self.assertTrue(missing_events)
            self.assertEqual(missing_events[0]["data"]["status"], "missing")
            self.assertEqual(missing_events[0]["severity"], "block")

    def test_lane_scoped_required_sources_research_not_blocked_by_live_only_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="research",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["hard_required_sources"], ["kalshi_market_data"])
            self.assertEqual(report["degraded_summary"]["missing_required_sources"], [])

    def test_lane_scoped_required_sources_live_execute_blocks_when_live_dependencies_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertIn("venue_balances", report["degraded_summary"]["missing_required_sources"])
            self.assertIn("order_permissions", report["degraded_summary"]["missing_required_sources"])


if __name__ == "__main__":
    unittest.main()
