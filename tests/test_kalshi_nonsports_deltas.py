from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_deltas import build_delta_rows, run_kalshi_nonsports_deltas


class KalshiNonSportsDeltasTests(unittest.TestCase):
    def test_build_delta_rows_labels_improved_two_sided_market(self) -> None:
        previous_rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "market_ticker": "KXTEST-1",
                "category": "Economics",
                "event_title": "Event one",
                "market_title": "Market one",
                "yes_bid_dollars": "0.02",
                "spread_dollars": "0.02",
                "two_sided_book": "True",
            }
        ]
        latest_rows = [
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "market_ticker": "KXTEST-1",
                "category": "Economics",
                "event_title": "Event one",
                "market_title": "Market one",
                "yes_bid_dollars": "0.03",
                "spread_dollars": "0.02",
                "two_sided_book": "True",
            }
        ]

        rows = build_delta_rows(
            previous_rows=previous_rows,
            latest_rows=latest_rows,
            min_tradeable_yes_bid=0.05,
            max_tradeable_spread=0.03,
            min_bid_improvement=0.01,
            min_spread_improvement=0.01,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["change_label"], "improved_two_sided")
        self.assertEqual(rows[0]["yes_bid_delta_dollars"], 0.01)

    def test_run_kalshi_nonsports_deltas_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history = base / "history.csv"
            history.write_text(
                (
                    "captured_at,summary_file,scan_csv,category,series_ticker,event_ticker,market_ticker,event_title,market_title,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,spread_dollars,liquidity_dollars,volume_24h_contracts,open_interest_contracts,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score\n"
                    "2026-03-27T20:00:00+00:00,a,b,Economics,s,e,KXTEST-1,Event one,Market one,2026-03-28T12:00:00+00:00,24,0.02,10,0.04,5,0.02,0,100,200,True,True,11\n"
                    "2026-03-27T21:00:00+00:00,a,b,Economics,s,e,KXTEST-1,Event one,Market one,2026-03-28T12:00:00+00:00,23,0.03,10,0.05,5,0.02,0,100,200,True,True,10\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_nonsports_deltas(
                history_csv=str(history),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["board_change_label"], "improving")
            self.assertEqual(summary["improved_two_sided_markets"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
