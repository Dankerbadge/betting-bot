import csv
from datetime import datetime
import tempfile
from pathlib import Path
import unittest

from betbot.config import StrategyConfig
from betbot.paper import run_paper
from betbot.types import BetCandidate


class PaperDecisionProbabilityTests(unittest.TestCase):
    def test_run_paper_prefers_candidate_decision_prob_when_present(self) -> None:
        candidates = [
            BetCandidate(
                timestamp=datetime.fromisoformat("2026-03-28T19:30:00"),
                event_id="evt1",
                selection="Confidence-shrunk edge",
                odds=2.0,
                model_prob=0.56,
                decision_prob=0.50,
                edge_rank_score=0.01,
            )
        ]
        cfg = StrategyConfig(
            min_ev=0.05,
            kelly_fraction=0.25,
            max_bet_fraction=0.5,
            min_stake=1.0,
            max_daily_loss_fraction=1.0,
            max_drawdown_fraction=1.0,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            summary = run_paper(
                candidates=candidates,
                cfg=cfg,
                starting_bankroll=100.0,
                output_dir=tmp_dir,
            )

            self.assertEqual(summary["accepted"], 0)
            self.assertEqual(summary["rejected"], 1)

            decisions_file = Path(summary["output_decisions_csv"])
            with decisions_file.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["status"], "rejected")
            self.assertEqual(rows[0]["reason"], "ev_below_threshold")
            self.assertEqual(rows[0]["model_prob"], "0.560000")
            self.assertEqual(rows[0]["decision_prob"], "0.500000")
            self.assertEqual(rows[0]["ev"], "0.000000")


if __name__ == "__main__":
    unittest.main()
