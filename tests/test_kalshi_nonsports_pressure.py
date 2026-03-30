from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_pressure import build_pressure_rows, run_kalshi_nonsports_pressure


FIELDNAMES = [
    "captured_at",
    "category",
    "market_ticker",
    "event_title",
    "market_title",
    "two_sided_book",
    "yes_bid_dollars",
    "spread_dollars",
    "execution_fit_score",
]


def _write_history(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class KalshiNonsportsPressureTests(unittest.TestCase):
    def test_build_pressure_rows_detects_build_market(self) -> None:
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
                "execution_fit_score": "50",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event",
                "market_title": "Test Market",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.03",
                "spread_dollars": "0.01",
                "execution_fit_score": "60",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event",
                "market_title": "Test Market",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.03",
                "spread_dollars": "0.01",
                "execution_fit_score": "61",
            },
        ]

        pressure_rows = build_pressure_rows(
            history_rows=rows,
            min_observations=3,
            min_latest_yes_bid=0.02,
            max_latest_spread=0.02,
            min_two_sided_ratio=0.5,
            min_recent_bid_change=0.01,
        )

        self.assertEqual(pressure_rows[0]["market_ticker"], "KXTEST-1")
        self.assertEqual(pressure_rows[0]["pressure_label"], "build")

    def test_run_kalshi_nonsports_pressure_writes_summary(self) -> None:
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
                        "execution_fit_score": "50",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event",
                        "market_title": "Test Market",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.03",
                        "spread_dollars": "0.01",
                        "execution_fit_score": "60",
                    },
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event",
                        "market_title": "Test Market",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.03",
                        "spread_dollars": "0.01",
                        "execution_fit_score": "61",
                    },
                ],
            )

            summary = run_kalshi_nonsports_pressure(
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["build_markets"], 1)
            self.assertEqual(summary["top_build_market_ticker"], "KXTEST-1")
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
