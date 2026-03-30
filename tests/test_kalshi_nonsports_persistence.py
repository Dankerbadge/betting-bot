from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_persistence import build_persistence_rows, run_kalshi_nonsports_persistence


class KalshiNonSportsPersistenceTests(unittest.TestCase):
    def test_build_persistence_rows_labels_persistent_tradeable_market(self) -> None:
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

        rows = build_persistence_rows(
            history_rows=history_rows,
            min_tradeable_yes_bid=0.05,
            max_tradeable_spread=0.03,
            min_tradeable_snapshot_count=2,
            min_consecutive_tradeable_snapshots=2,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["persistence_label"], "persistent_tradeable")
        self.assertEqual(rows[0]["tradeable_snapshots"], 3)
        self.assertEqual(rows[0]["consecutive_tradeable_snapshots"], 3)

    def test_run_kalshi_nonsports_persistence_writes_outputs(self) -> None:
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

            summary = run_kalshi_nonsports_persistence(
                history_csv=str(history),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["persistent_tradeable_markets"], 1)
            self.assertEqual(summary["snapshot_count"], 3)
            self.assertEqual(summary["distinct_markets"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_build_persistence_rows_does_not_count_listed_only_market_as_recurring(self) -> None:
        history_rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "market_ticker": "KXTEST-2",
                "category": "Economics",
                "event_title": "Event two",
                "market_title": "Market two",
                "yes_bid_dollars": "0.00",
                "spread_dollars": "0.01",
                "two_sided_book": "False",
                "hours_to_close": "24",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "market_ticker": "KXTEST-2",
                "category": "Economics",
                "event_title": "Event two",
                "market_title": "Market two",
                "yes_bid_dollars": "0.00",
                "spread_dollars": "0.01",
                "two_sided_book": "False",
                "hours_to_close": "23",
            },
        ]

        rows = build_persistence_rows(
            history_rows=history_rows,
            min_tradeable_yes_bid=0.05,
            max_tradeable_spread=0.03,
            min_tradeable_snapshot_count=2,
            min_consecutive_tradeable_snapshots=2,
        )

        self.assertEqual(rows[0]["persistence_label"], "one_off")


if __name__ == "__main__":
    unittest.main()
