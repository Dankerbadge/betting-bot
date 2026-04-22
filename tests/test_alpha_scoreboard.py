from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from betbot.alpha_scoreboard import run_alpha_scoreboard


class AlphaScoreboardTests(unittest.TestCase):
    def test_alpha_scoreboard_ready_projection_and_research_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            plan = {
                "top_market_ticker": "KXFREDDIE-26APR01",
                "top_market_side": "no",
                "top_market_hours_to_close": 59.8598,
                "total_planned_cost_dollars": 2.91,
                "top_plans": [
                    {
                        "plan_rank": 1,
                        "market_ticker": "KXFREDDIE-26APR01",
                        "side": "no",
                        "estimated_entry_cost_dollars": 0.97,
                        "expected_value_net_dollars": 0.016,
                        "expected_value_per_day_net_dollars": 0.006415,
                    },
                    {
                        "plan_rank": 2,
                        "market_ticker": "KXDEREMEROUT-26-APR01",
                        "side": "no",
                        "estimated_entry_cost_dollars": 0.96,
                        "expected_value_net_dollars": 0.010,
                        "expected_value_per_day_net_dollars": 0.004009,
                    },
                    {
                        "plan_rank": 3,
                        "market_ticker": "KXLUTNICKOUT-26APR01",
                        "side": "no",
                        "estimated_entry_cost_dollars": 0.98,
                        "expected_value_net_dollars": 0.0,
                        "expected_value_per_day_net_dollars": 0.0,
                    },
                ],
            }
            plan_path = base / "kalshi_micro_prior_plan_summary_20260329_120724.json"
            plan_path.write_text(json.dumps(plan), encoding="utf-8")

            daily_ops = {
                "captured_at": "2026-03-29T04:28:39.235525+00:00",
                "return_windows": {
                    "rolling_7d": {"net_return_pct": 0.0, "trade_count": 0},
                    "rolling_30d": {"net_return_pct": 0.0, "trade_count": 0},
                    "since_live_start": {"net_return_pct": 0.0, "trade_count": 0},
                },
            }
            daily_ops_path = base / "daily_ops_report_20260329_002839.json"
            daily_ops_path.write_text(json.dumps(daily_ops), encoding="utf-8")

            research_queue_path = base / "kalshi_nonsports_research_queue_20260329_123534.csv"
            with research_queue_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "category",
                        "research_priority_label",
                        "research_priority_score",
                        "research_prompt",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "market_ticker": "KXIPORIPPLING-26APR01",
                            "category": "Economics",
                            "research_priority_label": "medium",
                            "research_priority_score": "35.313392",
                            "research_prompt": "Estimate fair probability for Rippling IPO timing.",
                        },
                        {
                            "market_ticker": "KXIPORAMP-26APR01",
                            "category": "Economics",
                            "research_priority_label": "medium",
                            "research_priority_score": "35.313392",
                            "research_prompt": "Estimate fair probability for Ramp IPO timing.",
                        },
                    ]
                )

            summary = run_alpha_scoreboard(
                output_dir=str(base),
                planning_bankroll_dollars=40.0,
                benchmark_annual_return=0.10,
                now=datetime(2026, 3, 29, 16, 20, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertFalse(summary["bankroll_projection"]["beats_benchmark_projection"])
            self.assertAlmostEqual(summary["strategy_projection"]["deployed_fraction_pct"], 7.275, places=3)
            self.assertAlmostEqual(summary["strategy_projection"]["trade_net_roi_per_cycle_pct"], 0.893471, places=5)
            self.assertAlmostEqual(summary["bankroll_projection"]["annualized_net_return_pct"], 9.975933, places=4)
            self.assertGreater(
                summary["scaling_requirements"]["required_daily_risk_cap_dollars_to_hit_benchmark"],
                summary["strategy_projection"]["total_planned_cost_dollars"],
            )
            self.assertEqual(len(summary["research_targets"]), 2)
            self.assertEqual(summary["research_targets"][0]["market_ticker"], "KXIPORIPPLING-26APR01")
            self.assertEqual(summary["realized_returns"]["rolling_30d"]["trade_count"], 0)
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_alpha_scoreboard_blocked_without_plan_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_alpha_scoreboard(
                output_dir=str(base),
                planning_bankroll_dollars=40.0,
                benchmark_annual_return=0.10,
                now=datetime(2026, 3, 29, 16, 20, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked")
            self.assertTrue(summary["blockers"])
            self.assertIn("No kalshi_micro_prior_plan_summary file was found.", summary["blockers"][0])
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_alpha_scoreboard_ignores_non_finite_plan_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            plan = {
                "top_market_ticker": "KXTEST-26APR01",
                "top_market_side": "yes",
                "top_market_hours_to_close": 24.0,
                "top_plans": [
                    {
                        "estimated_entry_cost_dollars": "NaN",
                        "expected_value_net_dollars": "Infinity",
                        "expected_value_per_day_net_dollars": "Infinity",
                    },
                    {
                        "estimated_entry_cost_dollars": 2.0,
                        "expected_value_net_dollars": 0.2,
                        "expected_value_per_day_net_dollars": 0.2,
                    },
                ],
            }
            plan_path = base / "kalshi_micro_prior_plan_summary_20260329_120724.json"
            plan_path.write_text(json.dumps(plan), encoding="utf-8")

            summary = run_alpha_scoreboard(
                output_dir=str(base),
                planning_bankroll_dollars=40.0,
                benchmark_annual_return=0.10,
                now=datetime(2026, 3, 29, 16, 20, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["strategy_projection"]["planned_orders_count"], 1)
            self.assertAlmostEqual(summary["strategy_projection"]["total_planned_cost_dollars"], 2.0, places=6)
            self.assertAlmostEqual(summary["strategy_projection"]["trade_net_roi_per_cycle_pct"], 10.0, places=6)
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
