from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_signals import build_signal_rows, run_kalshi_nonsports_signals


class KalshiNonSportsSignalsTests(unittest.TestCase):
    def test_build_signal_rows_labels_eligible_market(self) -> None:
        history_rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "market_ticker": "KXTEST-1",
                "category": "Economics",
                "event_title": "Event one",
                "market_title": "Market one",
                "yes_bid_dollars": "0.08",
                "spread_dollars": "0.02",
                "two_sided_book": "True",
                "hours_to_close": "24",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "market_ticker": "KXTEST-1",
                "category": "Economics",
                "event_title": "Event one",
                "market_title": "Market one",
                "yes_bid_dollars": "0.07",
                "spread_dollars": "0.02",
                "two_sided_book": "True",
                "hours_to_close": "23",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "market_ticker": "KXTEST-1",
                "category": "Economics",
                "event_title": "Event one",
                "market_title": "Market one",
                "yes_bid_dollars": "0.06",
                "spread_dollars": "0.02",
                "two_sided_book": "True",
                "hours_to_close": "22",
            },
        ]

        rows = build_signal_rows(
            history_rows=history_rows,
            min_observations=3,
            min_stable_ratio=0.5,
            min_latest_yes_bid=0.05,
            min_mean_yes_bid=0.05,
            max_mean_spread=0.03,
            max_yes_bid_stddev=0.03,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["signal_label"], "eligible")
        self.assertEqual(rows[0]["stable_observations"], 3)

    def test_run_kalshi_nonsports_signals_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history = base / "history.csv"
            history.write_text(
                (
                    "captured_at,summary_file,scan_csv,category,series_ticker,event_ticker,market_ticker,event_title,market_title,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,spread_dollars,liquidity_dollars,volume_24h_contracts,open_interest_contracts,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score\n"
                    "2026-03-27T20:00:00+00:00,a,b,Economics,s,e,KXTEST-1,Event one,Market one,2026-03-28T12:00:00+00:00,24,0.08,10,0.10,5,0.02,0,100,200,True,True,11\n"
                    "2026-03-27T21:00:00+00:00,a,b,Economics,s,e,KXTEST-1,Event one,Market one,2026-03-28T12:00:00+00:00,23,0.07,10,0.09,5,0.02,0,100,200,True,True,10\n"
                    "2026-03-27T22:00:00+00:00,a,b,Economics,s,e,KXTEST-1,Event one,Market one,2026-03-28T12:00:00+00:00,22,0.06,10,0.08,5,0.02,0,100,200,True,True,12\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_nonsports_signals(
                history_csv=str(history),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["eligible_markets"], 1)
            self.assertEqual(summary["distinct_markets"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
