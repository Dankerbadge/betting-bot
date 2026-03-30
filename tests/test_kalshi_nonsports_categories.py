from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_categories import build_category_rows, run_kalshi_nonsports_categories


FIELDNAMES = [
    "captured_at",
    "category",
    "market_ticker",
    "two_sided_book",
    "yes_bid_dollars",
    "spread_dollars",
]


def _write_history(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class KalshiNonsportsCategoriesTests(unittest.TestCase):
    def test_build_category_rows_labels_tradeable_and_thin(self) -> None:
        rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "category": "Economics",
                "market_ticker": "KXECON-1",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.08",
                "spread_dollars": "0.02",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Economics",
                "market_ticker": "KXECON-1",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.09",
                "spread_dollars": "0.02",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "market_ticker": "KXPOL-1",
                "two_sided_book": "true",
                "yes_bid_dollars": "0.02",
                "spread_dollars": "0.02",
            },
        ]

        category_rows = build_category_rows(
            history_rows=rows,
            min_tradeable_yes_bid=0.05,
            max_tradeable_spread=0.03,
        )

        self.assertEqual(category_rows[0]["category"], "Economics")
        self.assertEqual(category_rows[0]["category_label"], "tradeable")
        politics = next(row for row in category_rows if row["category"] == "Politics")
        self.assertEqual(politics["category_label"], "thin")

    def test_run_kalshi_nonsports_categories_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            _write_history(
                history_csv,
                [
                    {
                        "captured_at": "2026-03-27T20:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXPOL-1",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.02",
                        "spread_dollars": "0.02",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXPOL-1",
                        "two_sided_book": "true",
                        "yes_bid_dollars": "0.03",
                        "spread_dollars": "0.02",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Economics",
                        "market_ticker": "KXECON-1",
                        "two_sided_book": "false",
                        "yes_bid_dollars": "0.10",
                        "spread_dollars": "0.10",
                    },
                ],
            )

            summary = run_kalshi_nonsports_categories(
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["categories_observed"], 2)
            self.assertEqual(summary["watch_categories"], 1)
            self.assertIsNotNone(summary["concentration_warning"])
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
