from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_research_queue import (
    build_research_queue_rows,
    run_kalshi_nonsports_research_queue,
)


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "event_title",
    "market_ticker",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "spread_dollars",
    "execution_fit_score",
    "two_sided_book",
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


class KalshiNonsportsResearchQueueTests(unittest.TestCase):
    def test_build_research_queue_rows_excludes_prior_covered_markets(self) -> None:
        history_rows = [
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Economics",
                "event_title": "OpenAI IPO?",
                "market_ticker": "KXIPOOPENAI-26APR01",
                "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "96",
                "yes_bid_dollars": "0.08",
                "yes_ask_dollars": "0.10",
                "spread_dollars": "0.02",
                "execution_fit_score": "72",
                "two_sided_book": "True",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Economics",
                "event_title": "OpenAI IPO?",
                "market_ticker": "KXIPOOPENAI-26APR01",
                "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "95",
                "yes_bid_dollars": "0.09",
                "yes_ask_dollars": "0.11",
                "spread_dollars": "0.02",
                "execution_fit_score": "73",
                "two_sided_book": "True",
            },
            {
                "captured_at": "2026-03-27T23:00:00+00:00",
                "category": "Economics",
                "event_title": "OpenAI IPO?",
                "market_ticker": "KXIPOOPENAI-26APR01",
                "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "94",
                "yes_bid_dollars": "0.10",
                "yes_ask_dollars": "0.12",
                "spread_dollars": "0.02",
                "execution_fit_score": "74",
                "two_sided_book": "True",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "event_title": "Lori Chavez-DeRemer out?",
                "market_ticker": "KXDEREMEROUT-26-APR01",
                "market_title": "Will Lori Chavez-DeRemer leaves as Secretary of Labor before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "96",
                "yes_bid_dollars": "0.03",
                "yes_ask_dollars": "0.04",
                "spread_dollars": "0.01",
                "execution_fit_score": "60",
                "two_sided_book": "True",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Politics",
                "event_title": "Lori Chavez-DeRemer out?",
                "market_ticker": "KXDEREMEROUT-26-APR01",
                "market_title": "Will Lori Chavez-DeRemer leaves as Secretary of Labor before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "95",
                "yes_bid_dollars": "0.03",
                "yes_ask_dollars": "0.04",
                "spread_dollars": "0.01",
                "execution_fit_score": "61",
                "two_sided_book": "True",
            },
            {
                "captured_at": "2026-03-27T23:00:00+00:00",
                "category": "Politics",
                "event_title": "Lori Chavez-DeRemer out?",
                "market_ticker": "KXDEREMEROUT-26-APR01",
                "market_title": "Will Lori Chavez-DeRemer leaves as Secretary of Labor before Apr 1, 2026?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "94",
                "yes_bid_dollars": "0.03",
                "yes_ask_dollars": "0.04",
                "spread_dollars": "0.01",
                "execution_fit_score": "62",
                "two_sided_book": "True",
            },
        ]
        prior_rows = [
            {
                "market_ticker": "KXDEREMEROUT-26-APR01",
                "fair_yes_probability": "0.02",
                "confidence": "0.58",
                "thesis": "Test thesis",
                "source_note": "Test source",
                "updated_at": "2026-03-28T00:00:00+00:00",
            }
        ]

        rows = build_research_queue_rows(history_rows=history_rows, prior_rows=prior_rows)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_ticker"], "KXIPOOPENAI-26APR01")
        self.assertEqual(rows[0]["category"], "Economics")
        self.assertEqual(rows[0]["cheapest_side"], "yes")
        self.assertEqual(rows[0]["category_prior_count"], 0)
        self.assertEqual(rows[0]["research_priority_label"], "high")

    def test_run_kalshi_nonsports_research_queue_writes_outputs(self) -> None:
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
                        "category": "Economics",
                        "event_title": "OpenAI IPO?",
                        "market_ticker": "KXIPOOPENAI-26APR01",
                        "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "96",
                        "yes_bid_dollars": "0.08",
                        "yes_ask_dollars": "0.10",
                        "spread_dollars": "0.02",
                        "execution_fit_score": "72",
                        "two_sided_book": "True",
                    },
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Economics",
                        "event_title": "OpenAI IPO?",
                        "market_ticker": "KXIPOOPENAI-26APR01",
                        "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "95",
                        "yes_bid_dollars": "0.09",
                        "yes_ask_dollars": "0.11",
                        "spread_dollars": "0.02",
                        "execution_fit_score": "73",
                        "two_sided_book": "True",
                    },
                    {
                        "captured_at": "2026-03-27T23:00:00+00:00",
                        "category": "Economics",
                        "event_title": "OpenAI IPO?",
                        "market_ticker": "KXIPOOPENAI-26APR01",
                        "market_title": "Will OpenAI IPO before Apr 1, 2026?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "94",
                        "yes_bid_dollars": "0.10",
                        "yes_ask_dollars": "0.12",
                        "spread_dollars": "0.02",
                        "execution_fit_score": "74",
                        "two_sided_book": "True",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            summary = run_kalshi_nonsports_research_queue(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["uncovered_research_markets"], 1)
            self.assertEqual(summary["top_market_ticker"], "KXIPOOPENAI-26APR01")
            self.assertEqual(summary["top_market_category"], "Economics")
            self.assertEqual(summary["top_market_cheapest_side"], "yes")
            self.assertEqual(summary["categories_without_priors"], ["Economics"])
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_build_research_queue_rows_keeps_one_sided_uncovered_markets(self) -> None:
        rows = build_research_queue_rows(
            history_rows=[
                {
                    "captured_at": "2026-03-27T21:00:00+00:00",
                    "category": "Companies",
                    "event_title": "Stripe IPO?",
                    "market_ticker": "KXSTRIPEIPO-26APR01",
                    "market_title": "Will Stripe IPO before Apr 1, 2026?",
                    "close_time": "2026-04-01T03:59:00+00:00",
                    "hours_to_close": "96",
                    "yes_bid_dollars": "0.00",
                    "yes_ask_dollars": "0.01",
                    "spread_dollars": "0.01",
                    "execution_fit_score": "32",
                    "two_sided_book": "False",
                },
                {
                    "captured_at": "2026-03-27T22:00:00+00:00",
                    "category": "Companies",
                    "event_title": "Stripe IPO?",
                    "market_ticker": "KXSTRIPEIPO-26APR01",
                    "market_title": "Will Stripe IPO before Apr 1, 2026?",
                    "close_time": "2026-04-01T03:59:00+00:00",
                    "hours_to_close": "95",
                    "yes_bid_dollars": "0.00",
                    "yes_ask_dollars": "0.01",
                    "spread_dollars": "0.01",
                    "execution_fit_score": "32",
                    "two_sided_book": "False",
                },
                {
                    "captured_at": "2026-03-27T23:00:00+00:00",
                    "category": "Companies",
                    "event_title": "Stripe IPO?",
                    "market_ticker": "KXSTRIPEIPO-26APR01",
                    "market_title": "Will Stripe IPO before Apr 1, 2026?",
                    "close_time": "2026-04-01T03:59:00+00:00",
                    "hours_to_close": "94",
                    "yes_bid_dollars": "0.00",
                    "yes_ask_dollars": "0.01",
                    "spread_dollars": "0.01",
                    "execution_fit_score": "32",
                    "two_sided_book": "False",
                },
            ],
            prior_rows=[],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market_ticker"], "KXSTRIPEIPO-26APR01")
        self.assertEqual(rows[0]["book_state"], "one_sided")


if __name__ == "__main__":
    unittest.main()
