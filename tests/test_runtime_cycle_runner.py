from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from betbot.adapters.base import AdapterContext
from betbot.execution.live_executor import LocalLiveVenueAdapter
from betbot.execution.ticket import create_ticket_proposal
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

    def test_explicit_empty_lane_required_sources_does_not_fallback_to_global(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / ".betbot").mkdir(parents=True, exist_ok=True)
            (repo_root / ".betbot.json").write_text(
                json.dumps({"policy": {"hard_required_sources": ["global_source"]}}),
                encoding="utf-8",
            )
            (repo_root / ".betbot" / "settings.json").write_text(
                json.dumps({"policy": {"hard_required_sources_by_lane": {"research": []}}}),
                encoding="utf-8",
            )

            output_dir = repo_root / "outputs"
            runner = CycleRunner(adapters=[_StaticAdapter("opticodds_consensus", "ok")])
            report = runner.run(
                CycleRunnerConfig(
                    lane="research",
                    output_dir=str(output_dir),
                    repo_root=str(repo_root),
                )
            )
            self.assertEqual(report["hard_required_sources"], [])
            self.assertEqual(report["overall_status"], "ok")

    def test_live_execute_with_valid_approval_submits_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-1",
                side="yes",
                max_cost=3.0,
                lane="live_execute",
                source_run_id="ref",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-1",
                        "side": "yes",
                        "max_cost": 3.0,
                        "issued_at": datetime.now(timezone.utc).isoformat(),
                        "expires_at": expires_at,
                        "approved_by": "tester",
                    }
                ),
                encoding="utf-8",
            )

            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                hard_required_sources=("kalshi_market_data",),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-1",
                    ticket_side="yes",
                    ticket_max_cost=3.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["approval_status"], "approved")
            self.assertEqual(report["order_status"], "submitted")
            self.assertEqual(report["execution_reason"], "live_submit_allowed")
            self.assertEqual(report["execution_ack_status"], "accepted")
            self.assertIsNotNone(report["execution_external_order_id"])
            self.assertEqual(report["reconciliation_status"], "resting")
            self.assertEqual(report["reconciliation_reason"], "reconciled_resting")
            self.assertEqual(report["reconciliation_mismatches"], 0)
            self.assertEqual(report["position_status"], "none")

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            event_types = [json.loads(line).get("event_type") for line in event_lines]
            self.assertIn("order_resting", event_types)

    def test_live_execute_missing_approval_blocks_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                hard_required_sources=("kalshi_market_data",),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    ticket_market="MKT-2",
                    ticket_side="no",
                    ticket_max_cost=2.0,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["order_status"], "blocked")
            self.assertIn(report["approval_status"], {"required", "approval_missing"})

    def test_live_execute_optional_approval_submits_when_policy_disables_approval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / ".betbot").mkdir(parents=True, exist_ok=True)
            (repo_root / ".betbot.json").write_text(
                json.dumps({"policy": {"approval_required": True}}),
                encoding="utf-8",
            )
            (repo_root / ".betbot" / "settings.json").write_text(
                json.dumps(
                    {
                        "policy": {
                            "approval_required_by_lane": {"live_execute": False},
                            "hard_required_sources_by_lane": {"live_execute": ["kalshi_market_data"]},
                        }
                    }
                ),
                encoding="utf-8",
            )

            output_dir = repo_root / "outputs"
            runner = CycleRunner(adapters=[_StaticAdapter("kalshi_market_data", "ok")])
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(repo_root),
                    request_live_submit=True,
                    ticket_market="MKT-3",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["approval_status"], "not_required")
            self.assertEqual(report["order_status"], "submitted")

    def test_live_execute_rejected_submit_ack_blocks_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-REJECT",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-reject",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_reject.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-REJECT",
                        "side": "yes",
                        "max_cost": 1.0,
                        "issued_at": datetime.now(timezone.utc).isoformat(),
                        "expires_at": expires_at,
                        "approved_by": "tester",
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                hard_required_sources=("kalshi_market_data",),
                live_venue_adapter=LocalLiveVenueAdapter(submit_outcome="rejected"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-REJECT",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["order_status"], "blocked")
            self.assertEqual(report["execution_ack_status"], "rejected")
            self.assertEqual(report["execution_reason"], "submission_rejected")
            self.assertEqual(report["reconciliation_status"], "not_requested")

    def test_live_execute_timeout_submit_ack_blocks_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-TIMEOUT",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-timeout",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_timeout.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-TIMEOUT",
                        "side": "yes",
                        "max_cost": 1.0,
                        "issued_at": datetime.now(timezone.utc).isoformat(),
                        "expires_at": expires_at,
                        "approved_by": "tester",
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                hard_required_sources=("kalshi_market_data",),
                live_venue_adapter=LocalLiveVenueAdapter(submit_outcome="timeout"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-TIMEOUT",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["order_status"], "blocked")
            self.assertEqual(report["execution_ack_status"], "timeout")
            self.assertEqual(report["execution_reason"], "submission_timeout")
            self.assertEqual(report["reconciliation_status"], "not_requested")

    def test_live_execute_reconcile_mismatch_fails_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-MISMATCH",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-mismatch",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_mismatch.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-MISMATCH",
                        "side": "yes",
                        "max_cost": 1.0,
                        "issued_at": datetime.now(timezone.utc).isoformat(),
                        "expires_at": expires_at,
                        "approved_by": "tester",
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                hard_required_sources=("kalshi_market_data",),
                live_venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="mismatch"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-MISMATCH",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "failed")
            self.assertEqual(report["order_status"], "reconcile_mismatch")
            self.assertEqual(report["execution_reason"], "reconcile_mismatch")
            self.assertEqual(report["reconciliation_status"], "mismatch")
            self.assertEqual(report["reconciliation_mismatches"], 1)

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            event_types = [json.loads(line).get("event_type") for line in event_lines]
            self.assertIn("order_reconcile_mismatch", event_types)


if __name__ == "__main__":
    unittest.main()
