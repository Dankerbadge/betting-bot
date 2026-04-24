from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_gate import build_trade_gate_decision, run_kalshi_micro_gate


class KalshiMicroGateTests(unittest.TestCase):
    def test_build_trade_gate_decision_passes_with_persistent_edge(self) -> None:
        decision = build_trade_gate_decision(
            actual_live_balance_dollars=40.0,
            funding_gap_dollars=0.0,
            planned_orders=2,
            meaningful_candidates=1,
            ledger_summary={
                "live_submissions_today": 0,
                "live_submitted_cost_today": 0.0,
            },
            max_live_submissions_per_day=3,
            max_live_cost_per_day_dollars=3.0,
            quality_summary={"meaningful_markets": 1},
            signal_summary={"eligible_markets": 0},
            persistence_summary={"persistent_tradeable_markets": 1},
            delta_summary={
                "board_change_label": "stale",
                "improved_two_sided_markets": 0,
                "newly_tradeable_markets": 0,
            },
            category_summary={
                "tradeable_categories": 1,
                "watch_categories": 0,
                "top_categories": [{"category": "Economics", "category_label": "tradeable"}],
                "concentration_warning": None,
            },
            pressure_summary={
                "build_markets": 0,
                "watch_markets": 0,
                "top_build_market_ticker": None,
                "top_build_category": None,
            },
        )

        self.assertTrue(decision["gate_pass"])
        self.assertEqual(decision["gate_status"], "pass")

    def test_build_trade_gate_decision_marks_unknown_balance_unavailable(self) -> None:
        decision = build_trade_gate_decision(
            actual_live_balance_dollars=None,
            funding_gap_dollars=None,
            planned_orders=0,
            meaningful_candidates=0,
            ledger_summary={
                "live_submissions_today": 0,
                "live_submitted_cost_today": 0.0,
            },
            max_live_submissions_per_day=3,
            max_live_cost_per_day_dollars=3.0,
            quality_summary={"meaningful_markets": 0},
            signal_summary={"eligible_markets": 0},
            persistence_summary={"persistent_tradeable_markets": 0},
            delta_summary={
                "board_change_label": "stale",
                "improved_two_sided_markets": 0,
                "newly_tradeable_markets": 0,
            },
            category_summary={
                "tradeable_categories": 0,
                "watch_categories": 0,
                "top_categories": [],
                "concentration_warning": None,
            },
            pressure_summary={
                "build_markets": 0,
                "watch_markets": 0,
                "top_build_market_ticker": None,
                "top_build_category": None,
            },
        )

        self.assertFalse(decision["gate_pass"])
        self.assertEqual(decision["gate_status"], "balance_unavailable")
        self.assertIn("Live balance could not be verified.", decision["gate_blockers"])

    def test_run_kalshi_micro_gate_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "planned_orders": 1,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "orders": [{"maker_yes_price_dollars": 0.06}],
                    "output_file": str(base / "plan.json"),
                }

            def fake_quality_runner(**kwargs: object) -> dict[str, object]:
                return {"meaningful_markets": 1, "output_file": str(base / "quality.json")}

            def fake_signal_runner(**kwargs: object) -> dict[str, object]:
                return {"eligible_markets": 1, "output_file": str(base / "signals.json")}

            def fake_persistence_runner(**kwargs: object) -> dict[str, object]:
                return {"persistent_tradeable_markets": 0, "output_file": str(base / "persistence.json")}

            def fake_delta_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "board_change_label": "improving",
                    "improved_two_sided_markets": 1,
                    "newly_tradeable_markets": 0,
                    "output_file": str(base / "deltas.json"),
                }

            def fake_category_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "tradeable_categories": 1,
                    "watch_categories": 0,
                    "top_categories": [{"category": "Economics", "category_label": "tradeable"}],
                    "concentration_warning": None,
                    "output_file": str(base / "categories.json"),
                }

            def fake_pressure_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "build_markets": 1,
                    "watch_markets": 0,
                    "top_build_market_ticker": "KXTEST-1",
                    "top_build_category": "Economics",
                    "output_file": str(base / "pressure.json"),
                }

            summary = run_kalshi_micro_gate(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "history.csv"),
                ledger_csv=str(base / "ledger.csv"),
                plan_runner=fake_plan_runner,
                quality_runner=fake_quality_runner,
                signal_runner=fake_signal_runner,
                persistence_runner=fake_persistence_runner,
                delta_runner=fake_delta_runner,
                category_runner=fake_category_runner,
                pressure_runner=fake_pressure_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(summary["gate_pass"])
            self.assertEqual(summary["gate_status"], "pass")
            self.assertEqual(summary["top_pressure_market_ticker"], "KXTEST-1")
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_gate_returns_structured_history_missing_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "planned_orders": 1,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "orders": [{"maker_yes_price_dollars": 0.06}],
                    "output_file": str(base / "plan.json"),
                    "status": "ready",
                }

            def missing_history_runner(**kwargs: object) -> dict[str, object]:
                raise ValueError(f"History CSV not found: {base / 'missing_history.csv'}")

            summary = run_kalshi_micro_gate(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "missing_history.csv"),
                ledger_csv=str(base / "ledger.csv"),
                plan_runner=fake_plan_runner,
                quality_runner=missing_history_runner,
                signal_runner=missing_history_runner,
                persistence_runner=missing_history_runner,
                delta_runner=missing_history_runner,
                category_runner=missing_history_runner,
                pressure_runner=missing_history_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertFalse(summary["gate_pass"])
            self.assertEqual(summary["gate_status"], "history_missing")
            self.assertIn("History CSV not found", " | ".join(summary["gate_blockers"]))
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
