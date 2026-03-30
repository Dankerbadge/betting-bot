from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture


class KalshiNonSportsCaptureTests(unittest.TestCase):
    def test_run_kalshi_nonsports_capture_appends_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            scan_csv = base / "scan.csv"
            scan_csv.write_text(
                (
                    "category,market_family,resolution_source_type,series_ticker,event_ticker,market_ticker,event_title,event_sub_title,market_title,yes_sub_title,rules_primary,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,no_bid_dollars,no_ask_dollars,last_price_dollars,spread_dollars,liquidity_dollars,volume_24h_contracts,open_interest_contracts,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score\n"
                    "Politics,cabinet_exit,official_source,KXTEST,KXTEST-1,KXTEST-1-Y,Event one,Sub event,Market one,Yes,Resolves to official source,2026-03-28T12:00:00+00:00,12.0,0.02,10,0.03,5,0.97,0.98,0.025,0.01,0,100,200,True,True,9.1\n"
                ),
                encoding="utf-8",
            )

            def fake_scan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "events_error": None,
                    "output_csv": str(scan_csv),
                    "output_file": str(base / "scan_summary.json"),
                    "page_requests": 2,
                    "rate_limit_retries_used": 0,
                    "network_retries_used": 1,
                    "transient_http_retries_used": 0,
                    "search_retries_total": 1,
                    "search_health_status": "degraded_retrying",
                    "events_fetched": 3,
                    "markets_ranked": 1,
                }

            summary = run_kalshi_nonsports_capture(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                scan_runner=fake_scan_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["rows_appended"], 1)
            self.assertEqual(summary["history_rows_total"], 1)
            self.assertEqual(summary["distinct_markets_observed"], 1)
            self.assertEqual(summary["two_sided_rows_appended"], 1)
            self.assertEqual(summary["scan_page_requests"], 2)
            self.assertEqual(summary["scan_network_retries_used"], 1)
            self.assertEqual(summary["scan_search_retries_total"], 1)
            self.assertEqual(summary["scan_search_health_status"], "degraded_retrying")
            self.assertEqual(summary["scan_events_fetched"], 3)
            self.assertEqual(summary["scan_markets_ranked"], 1)
            self.assertTrue(Path(summary["history_csv"]).exists())
            with Path(summary["history_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(rows[0]["event_sub_title"], "Sub event")
            self.assertEqual(rows[0]["yes_sub_title"], "Yes")
            self.assertEqual(rows[0]["rules_primary"], "Resolves to official source")
            self.assertEqual(rows[0]["no_bid_dollars"], "0.97")
            self.assertEqual(rows[0]["no_ask_dollars"], "0.98")
            self.assertEqual(rows[0]["last_price_dollars"], "0.025")
            self.assertEqual(rows[0]["market_family"], "cabinet_exit")
            self.assertEqual(rows[0]["resolution_source_type"], "official_source")

    def test_run_kalshi_nonsports_capture_migrates_legacy_history_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text(
                (
                    "captured_at,summary_file,scan_csv,category,series_ticker,event_ticker,market_ticker,event_title,market_title,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,spread_dollars,liquidity_dollars,volume_24h_contracts,open_interest_contracts,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score\n"
                    "2026-03-27T20:00:00+00:00,old_summary.json,old_scan.csv,Politics,KXOLD,KXOLD-1,KXOLD-1-Y,Old Event,Old Market,2026-03-28T12:00:00+00:00,16,0.02,10,0.03,5,0.01,0,100,200,True,True,7.5\n"
                ),
                encoding="utf-8",
            )
            scan_csv = base / "scan.csv"
            scan_csv.write_text(
                (
                    "category,series_ticker,event_ticker,market_ticker,event_title,event_sub_title,market_title,yes_sub_title,rules_primary,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,no_bid_dollars,no_ask_dollars,last_price_dollars,spread_dollars,liquidity_dollars,volume_24h_contracts,open_interest_contracts,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score\n"
                    "Politics,KXTEST,KXTEST-1,KXTEST-1-Y,Event one,Sub event,Market one,Yes,Resolves to official source,2026-03-28T12:00:00+00:00,12.0,0.02,10,0.03,5,0.97,0.98,0.025,0.01,0,100,200,True,True,9.1\n"
                ),
                encoding="utf-8",
            )

            def fake_scan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "output_csv": str(scan_csv),
                    "output_file": str(base / "scan_summary.json"),
                }

            summary = run_kalshi_nonsports_capture(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                scan_runner=fake_scan_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["rows_appended"], 1)
            self.assertEqual(summary["history_rows_total"], 2)
            header = history_csv.read_text(encoding="utf-8").splitlines()[0]
            self.assertIn("market_family", header)
            self.assertIn("resolution_source_type", header)
            self.assertIn("rules_primary", header)


if __name__ == "__main__":
    unittest.main()
