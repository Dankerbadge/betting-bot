from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from betbot.coldmath_snapshot import (
    build_public_observed_ledger_events,
    run_coldmath_snapshot_summary,
    summarize_coldmath_snapshot_files,
)


class ColdmathSnapshotTests(unittest.TestCase):
    def test_summarize_snapshot_files_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            now_utc = datetime(2026, 4, 21, 16, 0, tzinfo=timezone.utc)
            valuation = (now_utc - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
            (base / "equity.csv").write_text(
                "cashBalance,positionsValue,equity,valuationTime\n"
                f"100.0,25.0,125.0,{valuation}\n",
                encoding="utf-8",
            )
            (base / "positions.csv").write_text(
                "conditionId,asset,size,curPrice,valuationTime\n"
                f"c1,a1,10,0.50,{valuation}\n"
                f"c2,a2,12,0.00,{valuation}\n",
                encoding="utf-8",
            )
            summary = summarize_coldmath_snapshot_files(
                equity_csv=base / "equity.csv",
                positions_csv=base / "positions.csv",
                wallet_address="0xabc",
                stale_hours=48.0,
                now=now_utc,
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["priced_positions"], 1)
            self.assertEqual(summary["unpriced_positions"], 1)
            self.assertAlmostEqual(float(summary["equity"] or 0.0), 125.0, places=6)

    def test_summarize_snapshot_files_marks_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            now_utc = datetime(2026, 4, 21, 20, 0, tzinfo=timezone.utc)
            valuation = (now_utc - timedelta(hours=72)).isoformat().replace("+00:00", "Z")
            (base / "equity.csv").write_text(
                "cashBalance,positionsValue,equity,valuationTime\n"
                f"100.0,25.0,125.0,{valuation}\n",
                encoding="utf-8",
            )
            (base / "positions.csv").write_text(
                "conditionId,asset,size,curPrice,valuationTime\n"
                f"c1,a1,10,0.50,{valuation}\n",
                encoding="utf-8",
            )
            summary = summarize_coldmath_snapshot_files(
                equity_csv=base / "equity.csv",
                positions_csv=base / "positions.csv",
                stale_hours=24.0,
                now=now_utc,
            )
            self.assertEqual(summary["status"], "stale")
            self.assertGreater(float(summary["stale_seconds"] or 0.0), 24.0 * 3600.0)

    def test_run_summary_writes_output_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            snapshot_dir = base / "coldmath"
            out_dir = base / "out"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            now_utc = datetime(2026, 4, 21, 21, 0, tzinfo=timezone.utc)
            valuation = (now_utc - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
            (snapshot_dir / "equity.csv").write_text(
                "cashBalance,positionsValue,equity,valuationTime\n"
                f"90.0,10.0,100.0,{valuation}\n",
                encoding="utf-8",
            )
            (snapshot_dir / "positions.csv").write_text(
                "conditionId,asset,size,curPrice,valuationTime\n"
                f"c1,a1,2,0.45,{valuation}\n",
                encoding="utf-8",
            )
            summary = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir),
                output_dir=str(out_dir),
                wallet_address="0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
                now=now_utc,
            )
            self.assertEqual(summary["status"], "ready")
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["latest_file"]).exists())

    def test_run_summary_refreshes_from_polymarket_api_and_extracts_family_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            snapshot_dir = base / "coldmath"
            out_dir = base / "out"
            now_utc = datetime(2026, 4, 21, 22, 0, tzinfo=timezone.utc)

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                if "/value?" in url:
                    return (200, [{"user": "0xabc", "value": 150.0}])
                if "/closed-positions?" in url:
                    return (200, [])
                if "/positions?" in url and "offset=0" in url:
                    return (
                        200,
                        [
                            {
                                "conditionId": "c1",
                                "asset": "a1",
                                "size": 10.0,
                                "curPrice": 0.96,
                                "currentValue": 9.6,
                                "eventSlug": "highest-temperature-in-nyc-on-april-21-2026",
                                "slug": "highest-temperature-in-nyc-on-april-21-2026-60-61f",
                                "title": "Will the highest temperature in NYC be between 60-61F?",
                                "outcome": "No",
                                "endDate": "2026-04-21",
                            },
                            {
                                "conditionId": "c2",
                                "asset": "a2",
                                "size": 8.0,
                                "curPrice": 0.92,
                                "currentValue": 7.36,
                                "eventSlug": "highest-temperature-in-nyc-on-april-21-2026",
                                "slug": "highest-temperature-in-nyc-on-april-21-2026-62-63f",
                                "title": "Will the highest temperature in NYC be between 62-63F?",
                                "outcome": "No",
                                "endDate": "2026-04-21",
                            },
                        ],
                    )
                if "/positions?" in url and "offset=500" in url:
                    return (200, [])
                return (404, {"error": "not found"})

            summary = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir),
                output_dir=str(out_dir),
                wallet_address="0xabc",
                now=now_utc,
                refresh_from_api=True,
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["api_fetch"]["status"], "ready")
            self.assertEqual(summary["positions_rows"], 2)
            self.assertEqual(summary["closed_positions_rows"], 0)
            self.assertEqual(summary["portfolio_reconciliation"]["status"], "drift")
            self.assertTrue((snapshot_dir / "equity.csv").exists())
            self.assertTrue((snapshot_dir / "positions.csv").exists())
            self.assertTrue((snapshot_dir / "closed_positions.csv").exists())
            family_behavior = summary["family_behavior"]
            self.assertEqual(family_behavior["family_count"], 1)
            self.assertEqual(family_behavior["multi_strike_family_count"], 1)
            self.assertEqual(family_behavior["positions_with_no_outcome"], 2)
            self.assertIn("multi_strike_clustering", family_behavior["behavior_tags"])
            self.assertIn("no_side_bias", family_behavior["behavior_tags"])

    def test_run_summary_refreshes_trades_and_activity_with_taker_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            snapshot_dir = base / "coldmath"
            out_dir = base / "out"
            now_utc = datetime(2026, 4, 21, 22, 30, tzinfo=timezone.utc)
            requested_urls: list[str] = []

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                requested_urls.append(url)
                if "/value?" in url:
                    return (200, [{"user": "0xabc", "value": 200.0}])
                if "/closed-positions?" in url:
                    return (200, [])
                if "/positions?" in url and "offset=0" in url:
                    return (200, [])
                if "/trades?" in url and "takerOnly=true" in url:
                    return (
                        200,
                        [
                            {
                                "id": "t1",
                                "timestamp": "2026-04-21T22:00:00Z",
                                "side": "BUY",
                                "size": 10,
                                "price": 0.44,
                                "usdcSize": 4.4,
                                "title": "Weather trade",
                                "slug": "highest-temp-nyc-apr-21-60-61f",
                                "eventSlug": "highest-temp-nyc-apr-21",
                                "outcome": "No",
                                "transactionHash": "0xtx1",
                            }
                        ],
                    )
                if "/trades?" in url and "takerOnly=false" in url:
                    return (
                        200,
                        [
                            {
                                "id": "t1",
                                "timestamp": "2026-04-21T22:00:00Z",
                                "side": "BUY",
                                "size": 10,
                                "price": 0.44,
                                "usdcSize": 4.4,
                                "title": "Weather trade",
                                "slug": "highest-temp-nyc-apr-21-60-61f",
                                "eventSlug": "highest-temp-nyc-apr-21",
                                "outcome": "No",
                                "transactionHash": "0xtx1",
                            },
                            {
                                "id": "t2",
                                "timestamp": "2026-04-21T22:05:00Z",
                                "side": "SELL",
                                "size": 8,
                                "price": 0.57,
                                "usdcSize": 4.56,
                                "title": "Weather trade 2",
                                "slug": "highest-temp-nyc-apr-21-62-63f",
                                "eventSlug": "highest-temp-nyc-apr-21",
                                "outcome": "Yes",
                                "transactionHash": "0xtx2",
                            },
                        ],
                    )
                if "/activity?" in url:
                    return (
                        200,
                        [
                            {
                                "id": "a1",
                                "timestamp": "2026-04-21T22:03:00Z",
                                "type": "TRADE",
                                "title": "Weather trade",
                                "slug": "highest-temp-nyc-apr-21-60-61f",
                                "eventSlug": "highest-temp-nyc-apr-21",
                                "outcome": "No",
                                "side": "BUY",
                                "size": 10,
                                "price": 0.44,
                                "usdcSize": 4.4,
                                "transactionHash": "0xtx1",
                            },
                            {
                                "id": "a2",
                                "timestamp": "2026-04-21T22:10:00Z",
                                "type": "MAKER_REBATE",
                                "title": "Maker rebate",
                                "transactionHash": "0xtx3",
                            },
                        ],
                    )
                return (404, {"error": "not found"})

            summary = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir),
                output_dir=str(out_dir),
                wallet_address="0xabc",
                now=now_utc,
                refresh_from_api=True,
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["ledger"]["status"], "ready")
            self.assertEqual(summary["ledger"]["trade_scope_counts"]["taker_only"], 1)
            self.assertEqual(summary["ledger"]["trade_scope_counts"]["all_roles"], 2)
            self.assertEqual(summary["ledger"]["activity_type_counts"]["TRADE"], 1)
            self.assertEqual(summary["ledger"]["activity_type_counts"]["MAKER_REBATE"], 1)
            self.assertEqual(
                summary["api_fetch"]["ledger_fetch"]["trades"]["non_taker_trade_delta"],
                1,
            )
            self.assertEqual(summary["ledger"]["events_rows_total"], 4)
            self.assertEqual(summary["ledger"]["event_keys_unique"], 4)
            self.assertEqual(summary["ledger"]["event_duplicate_rows_detected"], 0)
            self.assertEqual(
                summary["api_fetch"]["ledger_fetch"]["events"]["duplicates_dropped"],
                1,
            )
            self.assertEqual(
                summary["api_fetch"]["ledger_fetch"]["events"]["canonical_rows_total"],
                4,
            )
            self.assertTrue((snapshot_dir / "trades.csv").exists())
            self.assertTrue((snapshot_dir / "activity.csv").exists())
            self.assertTrue((snapshot_dir / "ledger_events.csv").exists())
            self.assertTrue((snapshot_dir / "closed_positions.csv").exists())
            self.assertTrue(any("takerOnly=true" in url for url in requested_urls))
            self.assertTrue(any("takerOnly=false" in url for url in requested_urls))

    def test_run_summary_resolves_proxy_wallet_and_marks_public_observability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            snapshot_dir = base / "coldmath"
            out_dir = base / "out"
            now_utc = datetime(2026, 4, 21, 22, 45, tzinfo=timezone.utc)
            requested_urls: list[str] = []

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                requested_urls.append(url)
                if "/profile?" in url:
                    return (200, {"proxyWallet": "0xProxy"})
                if "/value?" in url and "user=0xproxy" in url:
                    return (200, [{"user": "0xproxy", "value": 50.0}])
                if "/closed-positions?" in url and "user=0xproxy" in url:
                    return (200, [])
                if "/positions?" in url and "user=0xproxy" in url:
                    return (200, [])
                if "/trades?" in url and "user=0xproxy" in url:
                    return (200, [])
                if "/activity?" in url and "user=0xproxy" in url:
                    return (200, [])
                return (404, {"error": "not found"})

            summary = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir),
                output_dir=str(out_dir),
                wallet_address="0xEOA",
                now=now_utc,
                refresh_from_api=True,
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["requested_wallet_address"], "0xeoa")
            self.assertEqual(summary["normalized_wallet_address"], "0xproxy")
            self.assertEqual(summary["wallet_address"], "0xproxy")
            self.assertEqual(summary["closed_positions_rows"], 0)
            self.assertEqual(summary["observability_mode"], "public_observed_ledger")
            self.assertFalse(summary["private_order_lifecycle_observable"])
            self.assertEqual(summary["profile_wallet_resolution"]["status"], "resolved")
            self.assertTrue(any("/profile?" in url for url in requested_urls))
            self.assertTrue(any("user=0xproxy" in url and "/value?" in url for url in requested_urls))

    def test_closed_positions_uses_timestamp_and_respects_limit_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            snapshot_dir = base / "coldmath"
            out_dir = base / "out"
            now_utc = datetime(2026, 4, 21, 23, 0, tzinfo=timezone.utc)
            requested_urls: list[str] = []

            def fake_http_get_json(url: str, timeout_seconds: float):
                _ = timeout_seconds
                requested_urls.append(url)
                if "/value?" in url:
                    return (200, [{"user": "0xabc", "value": 12.5}])
                if "/positions?" in url:
                    return (200, [])
                if "/closed-positions?" in url:
                    return (
                        200,
                        [
                            {
                                "conditionId": "c1",
                                "asset": "a1",
                                "size": 10.0,
                                "avgPrice": 0.45,
                                "title": "Closed weather position",
                                "slug": "highest-temp-nyc-apr-20-60-61f",
                                "eventSlug": "highest-temp-nyc-apr-20",
                                "outcome": "No",
                                "timestamp": "2026-04-21T18:30:00Z",
                                "realizedPnl": 3.2,
                            }
                        ],
                    )
                if "/trades?" in url:
                    return (200, [])
                if "/activity?" in url:
                    return (200, [])
                return (404, {"error": "not found"})

            summary = run_coldmath_snapshot_summary(
                snapshot_dir=str(snapshot_dir),
                output_dir=str(out_dir),
                wallet_address="0xabc",
                now=now_utc,
                refresh_from_api=True,
                closed_positions_page_size=500,
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["closed_positions_rows"], 1)
            self.assertAlmostEqual(
                float(summary["closed_positions_realized_pnl_sum"] or 0.0),
                3.2,
                places=6,
            )
            self.assertEqual(
                summary["closed_positions_closed_at_min"],
                "2026-04-21T18:30:00+00:00",
            )
            self.assertEqual(summary["api_fetch"]["closed_positions_rows"], 1)
            self.assertEqual(summary["api_fetch"]["closed_positions_endpoint_status"], 200)
            self.assertTrue(
                any("/closed-positions?" in url and "limit=50" in url for url in requested_urls)
            )

    def test_public_ledger_dedupe_uses_event_timestamp(self) -> None:
        payload = build_public_observed_ledger_events(
            trades_rows=[
                {
                    "capturedAt": "2026-04-21T00:00:00Z",
                    "queryScope": "all_roles",
                    "tradeId": "t1",
                    "timestamp": "2026-04-21T10:00:00Z",
                    "marketSlug": "mkt-1",
                    "eventSlug": "ev-1",
                    "title": "Trade A",
                    "outcome": "No",
                    "side": "BUY",
                    "size": "10",
                    "price": "0.5",
                    "usdcSize": "5",
                    "transactionHash": "0xtx",
                    "conditionId": "c1",
                    "asset": "a1",
                },
                {
                    "capturedAt": "2026-04-21T00:00:00Z",
                    "queryScope": "all_roles",
                    "tradeId": "t2",
                    "timestamp": "2026-04-21T10:01:00Z",
                    "marketSlug": "mkt-1",
                    "eventSlug": "ev-1",
                    "title": "Trade B",
                    "outcome": "No",
                    "side": "BUY",
                    "size": "10",
                    "price": "0.5",
                    "usdcSize": "5",
                    "transactionHash": "0xtx",
                    "conditionId": "c1",
                    "asset": "a1",
                },
            ],
            activity_rows=[],
        )
        self.assertEqual(payload["canonical_rows_total"], 2)
        self.assertEqual(payload["duplicates_dropped"], 0)


if __name__ == "__main__":
    unittest.main()
