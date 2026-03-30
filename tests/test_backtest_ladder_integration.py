import unittest
from datetime import datetime
import tempfile
import csv
from pathlib import Path

from betbot.backtest import run_backtest
from betbot.config import StrategyConfig
from betbot.types import BetCandidate


class BacktestLadderIntegrationTests(unittest.TestCase):
    def test_ladder_locks_vault_after_milestone(self) -> None:
        candidates = [
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-27T10:00:00"),
                event_id="evt_a",
                selection="Team A ML",
                odds=2.0,
                model_prob=0.8,
                closing_odds=1.95,
                outcome=1,
            )
        ]
        cfg = StrategyConfig(
            min_ev=0.0,
            kelly_fraction=0.25,
            max_bet_fraction=0.5,
            min_stake=1.0,
            max_daily_loss_fraction=1.0,
            ladder_enabled=True,
            ladder_rungs=[105, 120],
            ladder_min_success_prob=0.0,
            ladder_withdraw_step=10.0,
            ladder_min_risk_wallet=10.0,
            ladder_risk_per_effort=10.0,
            ladder_planning_p=0.55,
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = run_backtest(
                candidates=candidates,
                cfg=cfg,
                starting_bankroll=100.0,
                output_dir=tmp_dir,
            )

            self.assertGreaterEqual(summary["ladder_events_count"], 1)
            self.assertGreater(summary["final_locked_vault"], 0.0)

            ladder_files = list(Path(tmp_dir).glob("backtest_ladder_events_*.csv"))
            self.assertEqual(len(ladder_files), 1)

    def test_backtest_reorders_same_timestamp_candidates_by_edge_rank_score(self) -> None:
        candidates = [
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-27T10:00:00"),
                event_id="evt_low",
                selection="Lower edge",
                odds=2.0,
                model_prob=0.6,
                edge_rank_score=0.01,
                outcome=0,
            ),
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-27T10:00:00"),
                event_id="evt_high",
                selection="Higher edge",
                odds=2.0,
                model_prob=0.6,
                edge_rank_score=0.02,
                outcome=1,
            ),
        ]
        cfg = StrategyConfig(
            min_ev=0.0,
            kelly_fraction=0.5,
            max_bet_fraction=0.5,
            min_stake=1.0,
            max_daily_loss_fraction=1.0,
            max_drawdown_fraction=1.0,
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = run_backtest(
                candidates=candidates,
                cfg=cfg,
                starting_bankroll=100.0,
                output_dir=tmp_dir,
            )

            self.assertEqual(summary["wins"], 1)
            decisions_file = next(Path(tmp_dir).glob("backtest_decisions_*.csv"))
            with decisions_file.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["selection"], "Higher edge")


if __name__ == "__main__":
    unittest.main()
