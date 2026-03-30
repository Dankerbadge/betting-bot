import tempfile
from pathlib import Path
import unittest
from datetime import datetime
import json

from betbot.config import StrategyConfig
from betbot.ladder_grid import (
    build_rungs,
    pareto_front,
    parse_float_list,
    parse_int_list,
    run_ladder_grid,
)
from betbot.types import BetCandidate


class LadderGridTests(unittest.TestCase):
    def test_parse_float_list(self) -> None:
        self.assertEqual(parse_float_list("0.5, 0.6"), [0.5, 0.6])

    def test_parse_int_list(self) -> None:
        self.assertEqual(parse_int_list("3,4"), [3, 4])

    def test_build_rungs(self) -> None:
        rungs = build_rungs(
            starting_bankroll=100.0,
            first_rung_offset=10.0,
            rung_step_offset=20.0,
            rung_count=3,
        )
        self.assertEqual(rungs, [110.0, 130.0, 150.0])

    def test_pareto_front_selection(self) -> None:
        rows = [
            {"net_profit_total_wealth": 5.0, "max_drawdown_total_wealth": 0.20, "scenario": "a"},
            {"net_profit_total_wealth": 4.0, "max_drawdown_total_wealth": 0.10, "scenario": "b"},
            {"net_profit_total_wealth": 3.0, "max_drawdown_total_wealth": 0.30, "scenario": "c"},
        ]
        front = pareto_front(rows)
        scenarios = {row["scenario"] for row in front}
        self.assertIn("a", scenarios)
        self.assertIn("b", scenarios)
        self.assertNotIn("c", scenarios)

    def test_run_ladder_grid_single_combo(self) -> None:
        candidates = [
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-27T10:00:00"),
                event_id="evt_1",
                selection="A",
                odds=2.0,
                model_prob=0.6,
                outcome=1,
            ),
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-27T11:00:00"),
                event_id="evt_2",
                selection="B",
                odds=1.9,
                model_prob=0.58,
                outcome=0,
            ),
        ]
        cfg = StrategyConfig(
            min_ev=0.0,
            kelly_fraction=0.25,
            max_bet_fraction=0.3,
            min_stake=1.0,
            max_daily_loss_fraction=1.0,
            max_drawdown_fraction=1.0,
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = run_ladder_grid(
                candidates=candidates,
                base_cfg=cfg,
                starting_bankroll=100.0,
                output_dir=tmp_dir,
                first_rung_offsets=[5.0],
                rung_step_offsets=[20.0],
                rung_counts=[2],
                min_success_probs=[0.7],
                planning_ps=[0.55],
                withdraw_steps=[10.0],
                min_risk_wallets=[10.0],
                drawdown_penalty=0.0,
                top_k=1,
                pareto_k=5,
            )
            self.assertEqual(summary["runs_attempted"], 1)
            self.assertEqual(summary["runs_completed"], 1)
            self.assertIsNotNone(summary["best_result"])
            self.assertTrue(Path(summary["results_csv"]).exists())
            self.assertTrue(Path(summary["pareto_csv"]).exists())
            self.assertTrue(Path(summary["best_config_json"]).exists())
            self.assertTrue(Path(summary["summary_json"]).exists())
            payload = json.loads(Path(summary["best_config_json"]).read_text(encoding="utf-8"))
            self.assertTrue(payload["ladder_enabled"])


if __name__ == "__main__":
    unittest.main()
