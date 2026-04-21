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
    def __init__(self, provider: str, status: str, payload: dict[str, object] | None = None) -> None:
        self.provider = provider
        self._status = status
        self._payload = payload or {}

    def fetch(self, context: AdapterContext) -> SourceResult[dict[str, object]]:
        return SourceResult(provider=self.provider, status=self._status, payload=dict(self._payload))


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

    def test_default_ticket_selection_uses_scored_curated_news_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter(
                        "kalshi_market_data",
                        "ok",
                        payload={"market_tickers": ["KXRAINNYCM-26JUN-1", "KXRAINNYCM-26JUN-2"]},
                    ),
                    _StaticAdapter(
                        "curated_news",
                        "ok",
                        payload={
                            "top_market_ticker": "KXRAINNYCM-26JUN-1",
                            "top_market_fair_yes_probability": 0.32,
                            "top_market_confidence": 0.82,
                        },
                    ),
                    _StaticAdapter(
                        "opticodds_consensus",
                        "ok",
                        payload={
                            "positive_ev_candidates": 2,
                            "market_pairs_with_consensus": 4,
                        },
                    ),
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

            self.assertEqual(report["overall_status"], "ok")
            ticket = dict(report["ticket_proposal"] or {})
            self.assertEqual(ticket["market"], "KXRAINNYCM-26JUN-1")
            self.assertEqual(ticket["side"], "no")
            self.assertAlmostEqual(float(ticket["max_cost"]), 0.68, places=6)
            self.assertEqual(report["ticket_selection"]["selection_source"], "curated_news")
            self.assertTrue(report["candidate_scores"])

    def test_unmapped_curated_news_ticker_falls_back_to_market_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter(
                        "kalshi_market_data",
                        "ok",
                        payload={"market_tickers": ["MKT-PRIMARY", "MKT-SECONDARY"]},
                    ),
                    _StaticAdapter(
                        "curated_news",
                        "ok",
                        payload={
                            "top_market_ticker": "OUTSIDE-MARKET",
                            "top_market_fair_yes_probability": 0.31,
                            "top_market_confidence": 0.9,
                        },
                    ),
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

            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["ticket_selection"]["selection_source"], "kalshi_market_data")
            ticket = dict(report["ticket_proposal"] or {})
            self.assertEqual(ticket["market"], "MKT-PRIMARY")
            self.assertEqual(ticket["side"], "yes")

    def test_coldmath_replication_signal_biases_fallback_ticket_side_to_no(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["multi_strike_clustering", "high_price_no_inventory"],
                            "no_outcome_ratio": 0.62,
                            "positions_with_high_price_no": 12,
                            "multi_strike_family_count": 42,
                            "families": [
                                {
                                    "family_key": "highest-temperature-in-nyc-on-april-21-2026",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter(
                        "kalshi_market_data",
                        "ok",
                        payload={"market_tickers": ["MKT-A", "MKT-B"]},
                    ),
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

            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["ticket_selection"]["selection_source"], "coldmath_replication")
            ticket = dict(report["ticket_proposal"] or {})
            self.assertEqual(ticket["market"], "MKT-A")
            self.assertEqual(ticket["side"], "no")
            self.assertGreaterEqual(float(ticket["max_cost"]), 0.94)
            normalized_snapshot = dict(report["normalized_snapshot"] or {})
            coldmath_replication = dict(normalized_snapshot.get("coldmath_replication") or {})
            self.assertIn("high_price_no_inventory", coldmath_replication.get("behavior_tags", []))
            self.assertEqual(coldmath_replication.get("status"), "ready")

    def test_coldmath_replication_prefers_largest_market_family_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["multi_strike_clustering", "no_side_bias"],
                            "no_outcome_ratio": 0.58,
                            "positions_with_high_price_no": 2,
                            "multi_strike_family_count": 8,
                            "families": [
                                {
                                    "family_key": "highest-temperature-in-nyc-on-april-21-2026",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter(
                        "kalshi_market_data",
                        "ok",
                        payload={
                            "market_tickers": [
                                "KXHIGHNYC-26APR21-B80",
                                "KXHIGHDAL-26APR21-B80",
                                "KXHIGHNYC-26APR21-B81",
                                "KXHIGHNYC-26APR21-B82",
                            ]
                        },
                    ),
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

            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["ticket_selection"]["selection_source"], "coldmath_replication")
            ticket = dict(report["ticket_proposal"] or {})
            self.assertTrue(str(ticket["market"]).startswith("KXHIGHNYC-26APR21-"))
            self.assertEqual(ticket["side"], "no")
            self.assertGreaterEqual(float(ticket["max_cost"]), 0.8)
            self.assertIn("family_size=3", str(report["ticket_selection"]["selection_rationale"]))

    def test_coldmath_replication_plan_candidates_take_priority_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "coldmath_replication_plan_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "theme": "temperature",
                        "preferred_side": "no",
                        "candidate_count": 2,
                        "candidates": [
                            {
                                "market": "MKT-B",
                                "side": "no",
                                "max_cost": 0.91,
                                "score": 0.88,
                                "family_key": "FAMILY-B",
                                "rationale": "planner pick B",
                            },
                            {
                                "market": "MKT-A",
                                "side": "no",
                                "max_cost": 0.9,
                                "score": 0.55,
                                "family_key": "FAMILY-A",
                                "rationale": "planner pick A",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            runner = CycleRunner(
                adapters=[
                    _StaticAdapter(
                        "kalshi_market_data",
                        "ok",
                        payload={"market_tickers": ["MKT-A", "MKT-B", "MKT-C"]},
                    ),
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

            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["ticket_selection"]["selection_source"], "coldmath_replication_plan")
            ticket = dict(report["ticket_proposal"] or {})
            self.assertEqual(ticket["market"], "MKT-B")
            self.assertEqual(ticket["side"], "no")
            self.assertAlmostEqual(float(ticket["max_cost"]), 0.91, places=6)
            normalized_snapshot = dict(report["normalized_snapshot"] or {})
            plan = dict(normalized_snapshot.get("coldmath_replication_plan") or {})
            self.assertEqual(plan.get("status"), "ready")
            self.assertGreaterEqual(int(plan.get("candidate_count") or 0), 2)

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
                live_venue_adapter=LocalLiveVenueAdapter(),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
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
            self.assertEqual(report["live_adapter_mode"], "simulated_allowed")
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
                live_venue_adapter=LocalLiveVenueAdapter(),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
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
            runner = CycleRunner(
                adapters=[_StaticAdapter("kalshi_market_data", "ok")],
                live_venue_adapter=LocalLiveVenueAdapter(),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(repo_root),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
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
                    allow_simulated_live_adapter=True,
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
                    allow_simulated_live_adapter=True,
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
                    allow_simulated_live_adapter=True,
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

    def test_live_execute_reconcile_filled_emits_position_opened(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-FILLED",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-filled",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_filled.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-FILLED",
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
                live_venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="filled"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-FILLED",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["order_status"], "submitted")
            self.assertEqual(report["reconciliation_status"], "filled")
            self.assertEqual(report["position_status"], "open")
            self.assertEqual(report["reconciliation_filled_quantity"], 1.0)
            self.assertEqual(report["reconciliation_remaining_quantity"], 0.0)

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            event_types = [json.loads(line).get("event_type") for line in event_lines]
            self.assertIn("order_filled", event_types)
            self.assertIn("position_opened", event_types)

    def test_live_execute_reconcile_partially_filled_emits_position_opened(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-PARTIAL",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-partial",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_partial.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-PARTIAL",
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
                live_venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="partially_filled"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-PARTIAL",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["order_status"], "submitted")
            self.assertEqual(report["reconciliation_status"], "partially_filled")
            self.assertEqual(report["position_status"], "open")
            self.assertEqual(report["reconciliation_filled_quantity"], 0.5)
            self.assertEqual(report["reconciliation_remaining_quantity"], 0.5)

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            event_types = [json.loads(line).get("event_type") for line in event_lines]
            self.assertIn("order_partially_filled", event_types)
            self.assertIn("position_opened", event_types)

    def test_live_execute_reconcile_canceled_emits_order_canceled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-CANCELED",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-canceled",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_canceled.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-CANCELED",
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
                live_venue_adapter=LocalLiveVenueAdapter(reconcile_outcome="canceled"),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    allow_simulated_live_adapter=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-CANCELED",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "ok")
            self.assertEqual(report["order_status"], "submitted")
            self.assertEqual(report["reconciliation_status"], "canceled")
            self.assertEqual(report["position_status"], "none")
            self.assertEqual(report["reconciliation_filled_quantity"], 0.0)
            self.assertEqual(report["reconciliation_remaining_quantity"], 0.0)

            event_lines = (output_dir / "runtime_events_latest.jsonl").read_text(encoding="utf-8").splitlines()
            event_types = [json.loads(line).get("event_type") for line in event_lines]
            self.assertIn("order_canceled", event_types)

    def test_live_execute_missing_env_for_real_adapter_blocks_cleanly(self) -> None:
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
                    live_env_file=str(output_dir / "missing-live.env"),
                    ticket_market="MKT-ENV-MISS",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["order_status"], "blocked")
            self.assertEqual(report["execution_ack_status"], "not_submitted")
            self.assertTrue(str(report["execution_reason"]).startswith("live_adapter_init_failed:"))

    def test_live_execute_simulated_adapter_blocked_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
            reference_ticket = create_ticket_proposal(
                market="MKT-SIM-BLOCK",
                side="yes",
                max_cost=1.0,
                lane="live_execute",
                source_run_id="ref-sim-block",
                expires_at=expires_at,
            )
            approval_path = output_dir / "approval_sim_block.json"
            approval_path.write_text(
                json.dumps(
                    {
                        "ticket_hash": reference_ticket.ticket_hash,
                        "market": "MKT-SIM-BLOCK",
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
                live_venue_adapter=LocalLiveVenueAdapter(),
            )
            report = runner.run(
                CycleRunnerConfig(
                    lane="live_execute",
                    output_dir=str(output_dir),
                    repo_root=str(Path.cwd()),
                    request_live_submit=True,
                    approval_json_path=str(approval_path),
                    ticket_market="MKT-SIM-BLOCK",
                    ticket_side="yes",
                    ticket_max_cost=1.0,
                    ticket_expires_at=expires_at,
                )
            )
            self.assertEqual(report["overall_status"], "blocked")
            self.assertEqual(report["order_status"], "blocked")
            self.assertEqual(report["execution_ack_status"], "not_submitted")
            self.assertEqual(report["execution_reason"], "simulated_live_adapter_not_allowed")
            self.assertEqual(report["live_adapter_mode"], "simulated_blocked")


if __name__ == "__main__":
    unittest.main()
