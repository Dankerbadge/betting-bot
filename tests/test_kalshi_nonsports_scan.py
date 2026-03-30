import tempfile
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import URLError

from betbot.kalshi_nonsports_scan import extract_kalshi_nonsports_rows, run_kalshi_nonsports_scan


class KalshiNonSportsScanTests(unittest.TestCase):
    def test_extract_kalshi_nonsports_rows_filters_sports_and_scores_fillability(self) -> None:
        captured_at = datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc)
        events = [
            {
                "category": "Sports",
                "event_ticker": "KXSPORTS-1",
                "series_ticker": "KXSPORTS",
                "title": "Sports event",
                "sub_title": "Ignore this",
                "markets": [
                    {
                        "ticker": "KXSPORTS-1-Y",
                        "title": "Sports market",
                        "status": "active",
                        "close_time": "2026-03-28T00:00:00Z",
                        "yes_ask_dollars": "0.48",
                        "yes_bid_dollars": "0.47",
                        "yes_ask_size_fp": "100.00",
                    }
                ],
            },
            {
                "category": "Economics",
                "event_ticker": "KXECON-1",
                "series_ticker": "KXECON",
                "title": "Will CPI beat expectations?",
                "sub_title": "March release",
                "markets": [
                    {
                        "ticker": "KXECON-1-Y",
                        "title": "Will CPI beat expectations?",
                        "yes_sub_title": "Yes",
                        "status": "active",
                        "close_time": "2026-03-28T12:00:00Z",
                        "yes_ask_dollars": "0.40",
                        "yes_bid_dollars": "0.38",
                        "no_ask_dollars": "0.62",
                        "no_bid_dollars": "0.60",
                        "last_price_dollars": "0.39",
                        "yes_ask_size_fp": "50.00",
                        "yes_bid_size_fp": "20.00",
                        "liquidity_dollars": "500.00",
                        "volume_fp": "900.00",
                        "volume_24h_fp": "120.00",
                        "open_interest_fp": "700.00",
                        "rules_primary": "Resolves to official CPI print.",
                    }
                ],
            },
        ]

        rows, categories = extract_kalshi_nonsports_rows(
            events=events,
            captured_at=captured_at,
            excluded_categories=("Sports",),
            max_hours_to_close=48.0,
        )

        self.assertEqual(categories, {"Economics": 1})
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["category"], "Economics")
        self.assertEqual(row["market_ticker"], "KXECON-1-Y")
        self.assertEqual(row["contracts_for_ten_dollars"], 25)
        self.assertTrue(row["ten_dollar_fillable_at_best_ask"])
        self.assertGreater(row["execution_fit_score"], 0)

    def test_run_kalshi_nonsports_scan_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.40",
                                    "yes_bid_dollars": "0.38",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            summary = run_kalshi_nonsports_scan(
                env_file=str(env_file),
                output_dir=str(base),
                max_hours_to_close=48.0,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["jurisdiction"], "new_jersey")
            self.assertEqual(summary["markets_ranked"], 1)
            self.assertEqual(summary["ten_dollar_fillable_markets"], 1)
            self.assertEqual(summary["search_health_status"], "ready")
            self.assertEqual(summary["search_retries_total"], 0)
            self.assertEqual(summary["page_requests"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())
            persisted = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(persisted["output_file"], summary["output_file"])

    def test_run_kalshi_nonsports_scan_handles_rate_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 429, {"error": "rate limited"}

            summary = run_kalshi_nonsports_scan(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "rate_limited")
            self.assertEqual(summary["markets_ranked"], 0)
            self.assertEqual(summary["search_health_status"], "error")
            self.assertGreaterEqual(summary["rate_limit_retries_used"], 1)
            self.assertGreaterEqual(summary["search_retries_total"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_nonsports_scan_retries_rate_limit_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            attempts = 0

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    return 429, {"error": "rate limited"}
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.40",
                                    "yes_bid_dollars": "0.38",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            with patch("betbot.kalshi_nonsports_scan.time.sleep") as mock_sleep:
                summary = run_kalshi_nonsports_scan(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_ranked"], 1)
            self.assertEqual(attempts, 3)
            self.assertEqual(summary["search_health_status"], "degraded_retrying")
            self.assertEqual(summary["rate_limit_retries_used"], 2)
            self.assertEqual(summary["search_retries_total"], 2)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_run_kalshi_nonsports_scan_retries_network_error_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            attempts = 0

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise URLError("[Errno 8] nodename nor servname provided, or not known")
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.40",
                                    "yes_bid_dollars": "0.38",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            with patch("betbot.kalshi_nonsports_scan.time.sleep") as mock_sleep:
                summary = run_kalshi_nonsports_scan(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_ranked"], 1)
            self.assertGreaterEqual(attempts, 2)
            self.assertEqual(summary["search_health_status"], "degraded_retrying")
            self.assertGreaterEqual(summary["network_retries_used"], 1)
            self.assertGreaterEqual(summary["search_retries_total"], 1)
            self.assertGreaterEqual(mock_sleep.call_count, 1)

    def test_run_kalshi_nonsports_scan_retries_transient_http_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            attempts = 0

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    return 503, {"error": "service unavailable"}
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.40",
                                    "yes_bid_dollars": "0.38",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            with patch("betbot.kalshi_nonsports_scan.time.sleep") as mock_sleep:
                summary = run_kalshi_nonsports_scan(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_ranked"], 1)
            self.assertEqual(attempts, 3)
            self.assertEqual(summary["search_health_status"], "degraded_retrying")
            self.assertEqual(summary["transient_http_retries_used"], 2)
            self.assertEqual(summary["search_retries_total"], 2)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_run_kalshi_nonsports_scan_fails_over_immediately_on_dns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            requested_urls: list[str] = []

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                requested_urls.append(url)
                if "api.elections.kalshi.com" in url:
                    raise URLError("[Errno 8] nodename nor servname provided, or not known")
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.40",
                                    "yes_bid_dollars": "0.38",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            with patch("betbot.kalshi_nonsports_scan.time.sleep") as mock_sleep:
                summary = run_kalshi_nonsports_scan(
                    env_file=str(env_file),
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markets_ranked"], 1)
            self.assertEqual(summary["search_health_status"], "degraded_retrying")
            self.assertEqual(summary["network_retries_used"], 1)
            self.assertEqual(summary["api_root_failovers_used"], 1)
            self.assertEqual(mock_sleep.call_count, 0)
            self.assertEqual(len(requested_urls), 2)
            self.assertIn("api.elections.kalshi.com", requested_urls[0])
            self.assertIn("trading-api.kalshi.com", requested_urls[1])


if __name__ == "__main__":
    unittest.main()
