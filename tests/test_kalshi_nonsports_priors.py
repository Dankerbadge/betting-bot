from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_priors import build_prior_rows, run_kalshi_nonsports_priors


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "market_ticker",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
]

PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class KalshiNonsportsPriorsTests(unittest.TestCase):
    def test_build_prior_rows_matches_live_market(self) -> None:
        prior_rows = [
            {
                "market_ticker": "KXTEST-1",
                "fair_yes_probability": "0.08",
                "confidence": "0.7",
                "thesis": "Test",
                "source_note": "Note",
                "updated_at": "2026-03-27T21:00:00+00:00",
            }
        ]
        latest_market_rows = {
            "KXTEST-1": {
                "category": "Politics",
                "market_title": "Test Market",
                "close_time": "2026-03-28T12:00:00Z",
                "hours_to_close": "15",
                "yes_bid_dollars": "0.03",
                "yes_ask_dollars": "0.04",
            }
        }

        rows = build_prior_rows(
            prior_rows=prior_rows,
            latest_market_rows=latest_market_rows,
        )

        self.assertEqual(rows[0]["market_ticker"], "KXTEST-1")
        self.assertEqual(rows[0]["edge_to_yes_ask"], 0.04)
        self.assertEqual(rows[0]["edge_to_no_ask"], -0.05)
        self.assertEqual(rows[0]["best_entry_side"], "yes")
        self.assertEqual(rows[0]["best_maker_entry_side"], "yes")
        self.assertEqual(rows[0]["hours_to_close"], 15.0)

    def test_run_kalshi_nonsports_priors_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "market_title": "Test Market",
                        "close_time": "2026-03-28T12:00:00Z",
                        "hours_to_close": "15",
                        "yes_bid_dollars": "0.03",
                        "yes_ask_dollars": "0.04",
                    }
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXTEST-1",
                        "fair_yes_probability": "0.08",
                        "confidence": "0.7",
                        "thesis": "Test",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                ],
            )

            summary = run_kalshi_nonsports_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(summary["prior_file_exists"])
            self.assertEqual(summary["matched_live_markets"], 1)
            self.assertEqual(summary["positive_edge_yes_ask_markets"], 1)
            self.assertEqual(summary["positive_edge_no_ask_markets"], 0)
            self.assertEqual(summary["top_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["top_market_hours_to_close"], 15.0)
            self.assertEqual(summary["top_market_best_entry_side"], "yes")
            self.assertEqual(summary["top_market_best_maker_entry_side"], "yes")
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_build_prior_rows_excludes_endpoint_prices_from_orderable_entries(self) -> None:
        prior_rows = [
            {
                "market_ticker": "KXTEST-EDGE",
                "fair_yes_probability": "0.03",
                "confidence": "0.57",
                "thesis": "Test",
                "source_note": "Note",
                "updated_at": "2026-03-28T01:00:00+00:00",
            }
        ]
        latest_market_rows = {
            "KXTEST-EDGE": {
                "category": "Politics",
                "market_title": "Endpoint Market",
                "close_time": "2026-04-01T03:59:00Z",
                "hours_to_close": "94",
                "yes_bid_dollars": "0.00",
                "yes_ask_dollars": "0.02",
            }
        }

        rows = build_prior_rows(prior_rows=prior_rows, latest_market_rows=latest_market_rows)

        self.assertEqual(rows[0]["best_entry_side"], "yes")
        self.assertEqual(rows[0]["best_entry_price_dollars"], 0.02)
        self.assertEqual(rows[0]["best_maker_entry_side"], "no")
        self.assertEqual(rows[0]["best_maker_entry_price_dollars"], 0.98)
        self.assertNotEqual(rows[0]["best_maker_entry_price_dollars"], 0.0)


if __name__ == "__main__":
    unittest.main()
