from __future__ import annotations

from datetime import datetime, timezone
import unittest

from betbot.polymarket_market_ingest import (
    fetch_polymarket_markets_page,
    fetch_polymarket_temperature_markets,
)


class PolymarketMarketIngestTests(unittest.TestCase):
    def test_fetch_polymarket_markets_page_accepts_list_payload(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/markets?", url)
            return (200, [{"id": "1"}, {"id": "2"}])

        payload = fetch_polymarket_markets_page(
            offset=0,
            page_size=2,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(len(payload["markets"]), 2)

    def test_fetch_polymarket_temperature_markets_filters_weather_temperature(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            if "offset=0" in url:
                return (
                    200,
                    [
                        {
                            "id": "m1",
                            "slug": "highest-temp-nyc",
                            "question": "Highest temperature in New York on April 8, 2026?",
                            "description": "Uses station KJFK.",
                            "endDate": "2026-04-09T00:00:00Z",
                            "active": True,
                            "closed": False,
                            "acceptingOrders": True,
                            "outcomes": '["Yes","No"]',
                            "clobTokenIds": '["1","2"]',
                            "conditionId": "abc",
                            "event": {"title": "Weather - NYC"},
                        },
                        {
                            "id": "m2",
                            "slug": "btc-100k",
                            "question": "Will BTC close above $100k?",
                            "description": "Crypto market",
                            "active": True,
                            "closed": False,
                        },
                    ],
                )
            return (200, [])

        payload = fetch_polymarket_temperature_markets(
            max_markets=20,
            page_size=2,
            max_pages=2,
            http_get_json=fake_http_get_json,
            now=datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["markets_count"], 1)
        row = payload["markets"][0]
        self.assertEqual(row["market_id"], "m1")
        self.assertEqual(row["event_title"], "Weather - NYC")
        self.assertEqual(row["outcomes"], ["Yes", "No"])
        self.assertEqual(row["clob_token_ids"], ["1", "2"])


if __name__ == "__main__":
    unittest.main()
