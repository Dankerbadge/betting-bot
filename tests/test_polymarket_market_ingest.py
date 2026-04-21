from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from betbot.polymarket_market_ingest import (
    fetch_polymarket_markets_page,
    fetch_polymarket_temperature_markets,
    run_polymarket_market_data_ingest,
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

    def test_run_polymarket_ingest_can_attach_coldmath_snapshot_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "coldmath_snapshot"
            out_dir = Path(tmp) / "out"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            valuation_time = "2026-04-21T11:30:00Z"
            (snapshot_dir / "equity.csv").write_text(
                "cashBalance,positionsValue,equity,valuationTime\n"
                f"100.0,25.0,125.0,{valuation_time}\n",
                encoding="utf-8",
            )
            (snapshot_dir / "positions.csv").write_text(
                "conditionId,asset,size,curPrice,valuationTime\n"
                f"c1,a1,10,0.50,{valuation_time}\n",
                encoding="utf-8",
            )

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                if "offset=0" in url:
                    return (200, [])
                return (200, [])

            summary = run_polymarket_market_data_ingest(
                output_dir=str(out_dir),
                max_markets=10,
                page_size=10,
                max_pages=1,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
                coldmath_snapshot_dir=str(snapshot_dir),
                coldmath_wallet_address="0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
            )
            self.assertIn("coldmath_snapshot", summary)
            coldmath_snapshot = summary["coldmath_snapshot"]
            self.assertEqual(coldmath_snapshot["status"], "ready")
            self.assertEqual(coldmath_snapshot["priced_positions"], 1)
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_polymarket_ingest_builds_coldmath_temperature_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "coldmath_snapshot"
            out_dir = Path(tmp) / "out"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            valuation_time = "2026-04-21T11:30:00Z"
            (snapshot_dir / "equity.csv").write_text(
                "cashBalance,positionsValue,equity,valuationTime\n"
                f"100.0,25.0,125.0,{valuation_time}\n",
                encoding="utf-8",
            )
            (snapshot_dir / "positions.csv").write_text(
                "conditionId,asset,size,curPrice,valuationTime\n"
                f"c1,a1,10,0.50,{valuation_time}\n"
                f"c2,a2,8,0.40,{valuation_time}\n",
                encoding="utf-8",
            )

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                if "offset=0" in url:
                    return (
                        200,
                        [
                            {
                                "id": "m1",
                                "slug": "highest-temp-nyc",
                                "question": "Highest temperature in NYC on April 21, 2026?",
                                "description": "Weather temperature market",
                                "endDate": "2026-04-22T00:00:00Z",
                                "active": True,
                                "closed": False,
                                "acceptingOrders": True,
                                "outcomes": '["Yes","No"]',
                                "clobTokenIds": '["1","2"]',
                                "conditionId": "c1",
                                "event": {"title": "Weather - NYC"},
                            }
                        ],
                    )
                return (200, [])

            summary = run_polymarket_market_data_ingest(
                output_dir=str(out_dir),
                max_markets=10,
                page_size=10,
                max_pages=1,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
                coldmath_snapshot_dir=str(snapshot_dir),
            )
            alignment = summary["coldmath_temperature_alignment"]
            self.assertEqual(alignment["status"], "ready")
            self.assertEqual(alignment["positions_rows"], 2)
            self.assertEqual(alignment["matched_positions"], 1)
            self.assertEqual(alignment["unmatched_positions"], 1)
            self.assertEqual(alignment["top_matched_positions"][0]["condition_id"], "c1")

    def test_run_polymarket_ingest_can_refresh_coldmath_snapshot_from_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_dir = Path(tmp) / "coldmath_snapshot"
            out_dir = Path(tmp) / "out"
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                if "offset=0" in url:
                    return (
                        200,
                        [
                            {
                                "id": "m1",
                                "slug": "highest-temp-nyc",
                                "question": "Highest temperature in NYC on April 21, 2026?",
                                "description": "Weather temperature market",
                                "endDate": "2026-04-22T00:00:00Z",
                                "active": True,
                                "closed": False,
                                "acceptingOrders": True,
                                "outcomes": '["Yes","No"]',
                                "clobTokenIds": '["1","2"]',
                                "conditionId": "c1",
                                "event": {"title": "Weather - NYC"},
                            }
                        ],
                    )
                return (200, [])

            with patch(
                "betbot.polymarket_market_ingest.run_coldmath_snapshot_summary",
                return_value={
                    "status": "ready",
                    "equity_csv": str(snapshot_dir / "equity.csv"),
                    "positions_csv": str(snapshot_dir / "positions.csv"),
                },
            ) as mock_refresh, patch(
                "betbot.polymarket_market_ingest.summarize_coldmath_temperature_alignment",
                return_value={"status": "ready", "positions_rows": 0, "matched_positions": 0},
            ):
                summary = run_polymarket_market_data_ingest(
                    output_dir=str(out_dir),
                    max_markets=10,
                    page_size=10,
                    max_pages=1,
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
                    coldmath_snapshot_dir=str(snapshot_dir),
                    coldmath_wallet_address="0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
                    coldmath_refresh_from_api=True,
                )

            self.assertTrue(mock_refresh.called)
            refresh_kwargs = mock_refresh.call_args.kwargs
            self.assertTrue(refresh_kwargs["refresh_trades_from_api"])
            self.assertTrue(refresh_kwargs["refresh_activity_from_api"])
            self.assertTrue(refresh_kwargs["include_taker_only_trades"])
            self.assertTrue(refresh_kwargs["include_all_trade_roles"])
            self.assertEqual(summary["coldmath_snapshot"]["status"], "ready")


if __name__ == "__main__":
    unittest.main()
