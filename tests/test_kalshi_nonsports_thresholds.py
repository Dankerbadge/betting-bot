from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_thresholds import build_threshold_rows, run_kalshi_nonsports_thresholds


FIELDNAMES = [
    "captured_at",
    "category",
    "market_ticker",
    "event_title",
    "market_title",
    "two_sided_book",
    "yes_bid_dollars",
    "spread_dollars",
]


def _write_history(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class KalshiNonsportsThresholdsTests(unittest.TestCase):
    def test_build_threshold_rows_detects_approaching_market(self) -> None:
        rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event",
                "market_title": "Test Market",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.02",
                "spread_dollars": "0.02",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event",
                "market_title": "Test Market",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.03",
                "spread_dollars": "0.02",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event",
                "market_title": "Test Market",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.04",
                "spread_dollars": "0.02",
            },
        ]

        threshold_rows = build_threshold_rows(
            history_rows=rows,
            target_yes_bid=0.05,
            target_spread=0.02,
            recent_window=5,
            max_hours_to_target=6.0,
            min_recent_two_sided_ratio=0.5,
            min_observations=3,
        )

        self.assertEqual(threshold_rows[0]["market_ticker"], "KXTEST-1")
        self.assertEqual(threshold_rows[0]["threshold_label"], "approaching")

    def test_run_kalshi_nonsports_thresholds_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            _write_history(
                history_csv,
                [
                    {
                        "captured_at": "2026-03-27T20:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event",
                        "market_title": "Test Market",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.02",
                        "spread_dollars": "0.02",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event",
                        "market_title": "Test Market",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.03",
                        "spread_dollars": "0.02",
                    },
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event",
                        "market_title": "Test Market",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.04",
                        "spread_dollars": "0.02",
                    },
                ],
            )

            summary = run_kalshi_nonsports_thresholds(
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["approaching_markets"], 1)
            self.assertEqual(summary["top_approaching_market_ticker"], "KXTEST-1")
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
