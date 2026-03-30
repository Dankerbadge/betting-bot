from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from betbot.kalshi_weather_catalog import run_kalshi_weather_catalog


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "rules_primary",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "spread_dollars",
]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class KalshiWeatherCatalogTests(unittest.TestCase):
    def test_run_kalshi_weather_catalog_extracts_weather_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            _write_csv(
                history_csv,
                [
                    {
                        "captured_at": "2026-03-29T22:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXHMONTHRANGE",
                        "event_ticker": "KXHMONTHRANGE-26APR",
                        "market_ticker": "KXHMONTHRANGE-26APR-B1.200",
                        "event_title": "Apr 2026 temperature increase?",
                        "market_title": "Apr 2026 temperature increase?",
                        "rules_primary": "If the Land Ocean-Temperature Index for Apr 2026 is between 1.17-1.23, then the market resolves to Yes.",
                        "close_time": "2026-05-01T00:00:00+00:00",
                        "hours_to_close": "800",
                        "yes_bid_dollars": "0.23",
                        "yes_ask_dollars": "0.32",
                        "spread_dollars": "0.09",
                    },
                    {
                        "captured_at": "2026-03-29T22:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXPOL",
                        "event_ticker": "KXPOL-1",
                        "market_ticker": "KXPOL-1-APR",
                        "event_title": "Will policy pass?",
                        "market_title": "Will policy pass?",
                        "rules_primary": "If policy bill passes, market resolves to Yes.",
                        "close_time": "2026-05-01T00:00:00+00:00",
                        "hours_to_close": "800",
                        "yes_bid_dollars": "0.45",
                        "yes_ask_dollars": "0.46",
                        "spread_dollars": "0.01",
                    },
                ],
            )

            summary = run_kalshi_weather_catalog(
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 29, 22, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["weather_markets_total"], 1)
            self.assertEqual(summary["top_markets"][0]["market_ticker"], "KXHMONTHRANGE-26APR-B1.200")
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
